"""Explicit CPU dedup selection, guarded ledgers and real C/Python TCP chain."""

import copy
import ctypes as ct
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/validation"))
import eviction_batch as batch
import eviction_client as adapter
import eviction_dedup as dedup
import eviction_smoke as smoke
import eviction_trace as trace

TOKEN = "0123456789abcdef0123456789abcdef"


class CpuAdapter:
    instances = []
    failure = None
    fail_setup = False
    fail_after = False

    def __init__(self, document, lib, members, namespace, *, cpu_dedup_token):
        self.document, self.namespace = document, namespace.hex()
        self.token = cpu_dedup_token
        self.transport, self.closed = "test-only", False
        self.counters = dict.fromkeys(dedup.COUNTERS, 0)
        self.snapshots = 0
        self.instances.append(self)

    def cpu_dedup_metrics(self):
        self.snapshots += 1
        if self.fail_setup or self.fail_after and self.snapshots > 1:
            raise RuntimeError("CPU dedup not observed")
        return dict(self.counters)

    def execute(self, request_id):
        return {"outcome": "put_ack"}

    def execute_batch(self, ids):
        self.counters["fetches"] += len(ids)
        if self.failure == "raise":
            raise RuntimeError("whole C call rejected")
        results = [{"outcome": "get_hit", "status": 0} for _ in ids]
        if self.failure:
            results[-1] = dict(self.failure)
        return results

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True


class CpuDedupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.document = trace.generate(objects=8, rounds=2, length=3)
        self.source = self.base / "trace.json"
        self.source.write_bytes(trace.encode(self.document))
        self.config = smoke._config(__file__, "n1=127.0.0.1:12000", batch_gets=True,
                                    tcp=True, cpu_dedup=True, cpu_dedup_token=TOKEN)
        CpuAdapter.instances, CpuAdapter.failure = [], None
        CpuAdapter.fail_setup = CpuAdapter.fail_after = False

    def run_client(self):
        return smoke.run(self.document, self.config, library_loader=lambda _: object(),
                         client_factory=CpuAdapter)

    def test_mode_requires_batching_bool_and_frozen_token(self):
        for options in ({"cpu_dedup": True}, {"cpu_dedup": 1},
                        {"batch_gets": True, "cpu_dedup": True},
                        {"batch_gets": True, "cpu_dedup": True, "cpu_dedup_token": "../bad"},
                        {"cpu_dedup_token": TOKEN}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                smoke._config(__file__, "n1=127.0.0.1:12000", **options)
        self.assertEqual(self.config["client_environment"]["DFKV_CLIENT_NODE_DEDUP"], "1")
        self.assertEqual(self.config["client_environment"]["DFKV_NODE_DEDUP_ARENA_MB"], "1")
        command = smoke._command(self.source, self.base / "raw", self.config)
        self.assertIn("--cpu-dedup", command)
        self.assertEqual(command[command.index("--cpu-dedup-token") + 1], TOKEN)
        direct = smoke._config(__file__, "n1=127.0.0.1:12000", batch_gets=True)
        self.assertNotIn("cpu_dedup", direct)
        self.assertNotIn("DFKV_CLIENT_NODE_DEDUP", direct["client_environment"])
        self.assertNotIn("--cpu-dedup", smoke._command(self.source, self.base / "raw", direct))

    def test_namespaces_and_shm_names_are_frozen_isolated_and_occupied_is_rejected(self):
        identities = dedup.identities(self.document, TOKEN)
        self.assertEqual(len(identities), 2)
        self.assertNotEqual(identities[0]["shm_name"], identities[1]["shm_name"])
        other = dedup.identities(self.document, "a" * 32)
        self.assertNotEqual(identities, other)
        for item in identities:
            self.assertIn(TOKEN.encode().hex(), item["backend_namespace_hex"])
            self.assertTrue(item["shm_name"].startswith(f"/dfkv-dedup-v4-{os.getuid()}-"))
        with mock.patch.object(dedup.os.path, "lexists", return_value=True):
            with self.assertRaises(ValueError):
                dedup.require_fresh(self.document, TOKEN)
        # A dangling symlink is occupied too; do not weaken to Path.exists().
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "dangling"
            path.symlink_to(path.parent / "absent")
            with mock.patch.object(dedup, "identities", return_value=[{"possible_shm_path": str(path)}]):
                with self.assertRaises(ValueError):
                    dedup.require_fresh(self.document, TOKEN)

    def test_complete_enabled_ledger_and_logical_denominator(self):
        result = self.run_client()
        self.assertTrue(result["passed"])
        self.assertEqual(result["model"], batch.DEDUP_MODEL)
        summary = smoke._verify(self.document, result, self.config)
        self.assertEqual((summary["planned"], summary["completed"]), (20, 20))
        self.assertEqual(summary["cpu_dedup_observations"],
                         {"hits": 0, "fetches": 10, "wait_hits": 0, "wait_timeouts": 0})
        self.assertTrue(all(client.closed for client in CpuAdapter.instances))
        self.assertTrue(all(client.token == TOKEN for client in CpuAdapter.instances))

    def test_missing_enablement_or_final_snapshot_fails_without_fabricated_counters(self):
        CpuAdapter.fail_setup = True
        result = self.run_client()
        self.assertFalse(result["passed"])
        summary = smoke._verify(self.document, result, self.config)
        self.assertEqual(summary["attempted"], 0)
        self.assertIsNone(summary["cpu_dedup_observations"])
        self.assertTrue(all(client.closed for client in CpuAdapter.instances))
        CpuAdapter.fail_setup, CpuAdapter.fail_after = False, True
        result = self.run_client()
        self.assertFalse(result["passed"])
        self.assertEqual(smoke._verify(self.document, result, self.config)["completed"], 20)
        self.assertIsNone(result["summary"]["cpu_dedup_observations"])

    def test_negative_slots_and_whole_call_failure_retain_complete_group_and_stop(self):
        for failure, terminal in (({"outcome": "get_miss", "status": 1}, "get_miss"),
                                  ({"outcome": "client_error", "status": 4, "error": "I/O"}, "client_error"),
                                  ("raise", "get_batch_error")):
            CpuAdapter.failure = failure
            result = self.run_client()
            counts = smoke._verify(self.document, result, self.config)
            self.assertFalse(result["passed"])
            self.assertEqual((counts["attempted"], counts["completed"]), (12, 12))
            self.assertEqual(counts["get_outcomes"][terminal], 4 if terminal == "get_batch_error" else 1)
            self.assertEqual(counts["get_outcomes"]["not_attempted"], 6)

    def test_hostile_identity_config_metrics_and_model_mutations_are_rejected(self):
        original = self.run_client()
        mutations = [lambda r: r.update(model=batch.MODEL),
                     lambda r: r["config"].update(cpu_dedup=False),
                     lambda r: r["cpu_dedup"][0].update(backend_namespace_hex="00"),
                     lambda r: r["cpu_dedup"][0].update(possible_shm_path="/tmp/other"),
                     lambda r: r["cpu_dedup"][0]["before"].update(hits=1),
                     lambda r: r["cpu_dedup"][0]["after"].update(fetches=True),
                     lambda r: r["cpu_dedup"][0]["after"].update(fetches=999),
                     lambda r: r["cpu_dedup"][0].update(before=None),
                     lambda r: r["cpu_dedup"][0].update(after=None),
                     lambda r: r.update(cpu_dedup=r["cpu_dedup"][:1]),
                     lambda r: r["summary"].update(cpu_dedup_observations=None)]
        for mutation in mutations:
            result = copy.deepcopy(original)
            mutation(result)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                smoke._verify(self.document, result, self.config)

    def child_command(self, suffix=""):
        return [sys.executable, "-c", f'''
import sys
sys.path.insert(0, {str(Path(__file__).parent)!r})
from test_eviction_dedup import CpuAdapter
import eviction_smoke as s
import eviction_trace as t
from pathlib import Path
p=Path({str(self.base / "run")!r})
c={self.config!r}
r=s.run(t.load(p/'trace.json'),c,library_loader=lambda _:object(),client_factory=CpuAdapter)
(p/'child-result.json').write_bytes(t.encode(r))
{suffix}
sys.exit(0 if r['passed'] else 1)
''']

    def supervise(self, command, budget=2):
        with mock.patch.object(smoke.uuid, "uuid4", return_value=mock.Mock(hex=TOKEN)), \
                mock.patch.object(smoke, "_command", return_value=command):
            return smoke.supervise(self.source, self.base / "run", lib=__file__,
                                    members="n1=127.0.0.1:12000", batch_gets=True,
                                    tcp=True, cpu_dedup=True, process_timeout_sec=budget)

    def test_guarded_child_enabled_observations_are_verified(self):
        report = self.supervise(self.child_command())
        self.assertTrue(report["passed"], report)
        self.assertTrue(report["reaped"])
        self.assertEqual(report["cpu_dedup_observations"]["fetches"], 10)
        self.assertEqual(report["shm_cleanup"], "not_attempted_no_exclusive_creator_proof")
        self.assertEqual(len(report["possible_retained_shm_paths"]), 2)

    def test_process_timeout_overrides_already_written_pass_and_keeps_counts_unknown(self):
        report = self.supervise(self.child_command("import time;time.sleep(60)"), budget=.3)
        self.assertEqual(report["status"], "process_timeout")
        self.assertTrue(report["reaped"])
        self.assertIsNone(report["cpu_dedup_observations"])
        self.assertIsNone(report["completed_requests"])
        self.assertIsNone(report["get_outcomes"])

    def test_parent_rejects_forged_enabled_counts(self):
        report = self.supervise(self.child_command(
            "r['cpu_dedup'][0]['after']['fetches']=99;(p/'child-result.json').write_bytes(t.encode(r))"))
        self.assertEqual(report["status"], "invalid_result")
        self.assertFalse(report["passed"])
        self.assertIsNone(report["cpu_dedup_observations"])
        self.assertIsNone(report["completed_requests"])

    def test_parent_keeps_known_negative_group_counts_and_enabled_observation(self):
        command = self.child_command()
        command[-1] = command[-1].replace("r=s.run(",
            "CpuAdapter.failure={'outcome':'get_miss','status':1}\nr=s.run(")
        report = self.supervise(command)
        self.assertEqual(report["status"], "workload_failed")
        self.assertFalse(report["passed"])
        self.assertEqual((report["attempted_requests"], report["completed_requests"]), (12, 12))
        self.assertEqual(report["cpu_dedup_observations"]["fetches"], 4)
        self.assertEqual(report["get_outcomes"]["get_miss"], 1)
        self.assertEqual(report["get_outcomes"]["not_attempted"], 6)

    def test_bounded_snapshot_rejects_missing_partial_duplicate_or_disabled_metrics(self):
        class Library:
            pass
        library = Library()
        valid = "".join(f"dfkv_client_dedup_{name}_total 0\n" for name in dedup.COUNTERS)
        for text, size in ((valid, None), ("", None), (valid + valid, None),
                           (valid.replace("hits_total 0", "hits_total -1"), None),
                           (valid, 65536), (valid, 0)):
            def function(handle, buffer, capacity):
                ct.memmove(buffer, text.encode(), len(text))
                return len(text) if size is None else size
            library.dfkv_stats_snapshot = function
            if text == valid and size is None:
                self.assertEqual(dedup.snapshot(library, 7), dict.fromkeys(dedup.COUNTERS, 0))
            else:
                with self.assertRaises(RuntimeError):
                    dedup.snapshot(library, 7)
        with self.assertRaises(RuntimeError):
            dedup.snapshot(object(), 7)


class CpuDedupAdapterTest(unittest.TestCase):
    def test_explicit_adapter_remaps_backend_but_not_trace_ids_and_does_not_use_direct(self):
        document = trace.generate(objects=8, rounds=2, length=3)
        namespace = bytes.fromhex(document["requests"][0]["namespace_hex"])
        owned = mock.Mock(transport="test-only", handle=7)
        owned.read_deduplicated_outcomes.return_value = [{"outcome": "get_hit", "status": 0}] * 2
        factory = mock.Mock(return_value=owned)
        client = adapter.TraceClient(document, object(), "test", namespace,
                                      client_factory=factory, cpu_dedup_token=TOKEN)
        self.addCleanup(client.close)
        self.assertEqual(factory.call_args.args[-1], dedup.namespace(TOKEN, namespace))
        self.assertEqual(len(client.execute_batch([8, 10])), 2)
        owned.read_outcomes.assert_not_called()
        owned.read_deduplicated_outcomes.assert_called_once()
        client.close()
        with self.assertRaises(RuntimeError):
            client.execute_batch([8, 10])
        with self.assertRaises(RuntimeError):
            client.cpu_dedup_metrics()


# Reuse the existing tiny TCP fixture without inheriting its unrelated tests.
import test_eviction_batch as batch_tests


class NativeCpuDedupTest(unittest.TestCase):
    setUp = batch_tests.NativeBatchTest.setUp
    stop_server = batch_tests.NativeBatchTest.stop_server

    def test_actual_guarded_cabi_python_report_records_enabled_cpu_reuse(self):
        document = trace.generate(objects=8, rounds=2, length=17)
        source = self.base / "cpu-trace.json"
        source.write_bytes(trace.encode(document))
        report = smoke.supervise(source, self.base / "cpu-run", lib=str(self.library),
                                 members=self.members, batch_gets=True, tcp=True, cpu_dedup=True)
        self.assertTrue(report["passed"], report)
        self.assertTrue(report["reaped"])
        self.assertEqual((report["attempted_requests"], report["completed_requests"]), (20, 20))
        observed = report["cpu_dedup_observations"]
        self.assertGreater(observed["hits"], 0)
        self.assertEqual(sum(observed.values()), 10)
        self.assertEqual(report["get_outcomes"]["get_hit"], 10)
        self.assertEqual(report["get_adapter_calls"], 4)
        raw = trace.load(self.base / "cpu-run/child-result.json")
        self.assertEqual(smoke._verify(document, raw, report["config"])["cpu_dedup_observations"], observed)
        self.assertTrue(all(row["before"] == dict.fromkeys(dedup.COUNTERS, 0) for row in raw["cpu_dedup"]))
        for entry in raw["cpu_dedup"]:
            self.assertTrue(Path(entry["possible_shm_path"]).is_file())
        self.assertNotEqual(raw["cpu_dedup"][0]["namespace_hex"], raw["cpu_dedup"][0]["backend_namespace_hex"])

    def test_actual_new_binding_mixed_and_stopped_errors_are_not_misses(self):
        from disk_scenarios import Client
        import uuid
        token = uuid.uuid4().hex
        namespace = dedup.namespace(token, b"native\0mixed")
        with mock.patch.dict(os.environ, dict(dedup.ENV, DFKV_RDMA="0")):
            client = Client(self.lib, self.members, namespace)
        self.addCleanup(client.close)
        client.put([b"hit\0", b"wide"], [b"abc", b"123456789"])
        results = client.read_deduplicated_outcomes([b"hit\0", b"absent", b"wide"], [b"abc", b"abc", b"x"])
        self.assertEqual([row["status"] for row in results], [0, 1, 5])
        self.assertEqual([row["outcome"] for row in results], ["get_hit", "get_miss", "client_error"])
        self.stop_server()
        # A never-read key avoids a legitimate TTL-local hit after server stop.
        results = client.read_deduplicated_outcomes([b"never-read"], [b"abc"])
        self.assertEqual(results[0]["status"], 4)
        self.assertEqual(results[0]["outcome"], "client_error")
        identity = dedup.identities({"requests": [{"namespace_hex": b"native\0mixed".hex()}]}, token)[0]
        (self.base / "native-mixed.json").write_bytes(trace.encode(
            {"identity": identity, "mixed_statuses": [0, 1, 5], "stopped_results": results}))
