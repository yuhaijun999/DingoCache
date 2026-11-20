"""Namespace-batched smoke contracts and tiny real TCP/C ABI integration."""

import copy
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/validation"))
sys.path.insert(0, str(ROOT / "integration/common/src"))
import eviction_batch as batch
import eviction_client as adapter
import eviction_smoke as smoke
import eviction_trace as trace


class Adapter:
    instances = []
    calls = []
    outcomes = {}
    raise_get = False

    def __init__(self, document, lib, members, namespace):
        self.document, self.transport = document, "test-only"
        self.closed = False
        self.instances.append(self)

    def execute(self, request_id):
        self.calls.append([request_id])
        return {"outcome": "put_ack" if self.document["requests"][request_id]["op"] == "put" else "get_hit"}

    def execute_batch(self, request_ids):
        self.calls.append(list(request_ids))
        if self.raise_get:
            raise RuntimeError("controlled whole-call failure")
        return [dict(self.outcomes.get(request_id, {"outcome": "get_hit", "status": 0}))
                for request_id in request_ids]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True


class BatchSmokeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.document = trace.generate(objects=8, rounds=2, length=3)
        self.source = self.base / "trace.json"
        self.source.write_bytes(trace.encode(self.document))
        self.config = smoke._config(__file__, "n1=127.0.0.1:12000", batch_gets=True)
        Adapter.instances, Adapter.calls, Adapter.outcomes = [], [], {}
        Adapter.raise_get = False

    def run_client(self):
        return smoke.run(self.document, self.config, library_loader=lambda _: object(),
                         client_factory=Adapter)

    def command(self, suffix=""):
        # Actual child process, but in-memory Adapter: not native/network proof.
        return [sys.executable, "-c", f'''
import sys
sys.path.insert(0, {str(Path(__file__).parent)!r})
from test_eviction_batch import Adapter
import eviction_smoke as s
import eviction_trace as t
from pathlib import Path
p=Path({str(self.base / "run")!r})
c=s._config({str(Path(__file__).resolve())!r}, 'n1=127.0.0.1:12000', batch_gets=True)
r=s.run(t.load(p/'trace.json'), c, library_loader=lambda _: object(), client_factory=Adapter)
(p/'child-result.json').write_bytes(t.encode(r))
{suffix}
sys.exit(0 if r['passed'] else 1)
''']

    def supervise(self, command):
        with mock.patch.object(smoke, "_command", return_value=command):
            return smoke.supervise(self.source, self.base / "run", lib=__file__,
                                   members="n1=127.0.0.1:12000", batch_gets=True,
                                   process_timeout_sec=2)

    def test_plan_groups_real_multi_item_reads_without_crossing_put_or_phase(self):
        expected = [[index] for index in range(8)] + [[8, 10, 12, 14], [9, 11, 13, 15],
                                                     [16], [17], [18], [19]]
        self.assertEqual(batch.plan(self.document), expected)
        self.assertTrue(any(len(group) > 1 for group in expected))
        result = self.run_client()
        self.assertEqual(Adapter.calls, expected)
        self.assertEqual(result["model"], "namespace-batched-real-client-correctness-only")
        self.assertEqual(result["schedule_mode"], "namespace-batched-unpaced")
        self.assertEqual(smoke._verify(self.document, result, self.config)["completed"], 20)
        self.assertEqual(result["summary"]["get_adapter_calls"], 4)
        self.assertEqual(result["summary"]["get_failed_adapter_calls"], 0)
        self.assertEqual(result["summary"]["hit_get_bytes"], 30)
        self.assertEqual(result["summary"]["acked_put_bytes"], 30)
        self.assertTrue(all(client.closed for client in Adapter.instances))
        self.assertEqual(self.source.read_bytes(), trace.encode(self.document))

    def test_mixed_group_keeps_siblings_and_blocks_next_namespace_and_measurement(self):
        Adapter.outcomes = {10: {"outcome": "get_miss", "status": 1},
                            12: {"outcome": "client_error", "status": 999,
                                 "error": "unknown native result"}}
        result = self.run_client()
        self.assertFalse(result["passed"])
        self.assertEqual([result["requests"][i]["terminal"] for i in (8, 10, 12, 14)],
                         ["get_hit", "get_miss", "client_error", "get_hit"])
        self.assertEqual(result["requests"][12]["status"], 999)
        self.assertEqual(Adapter.calls[-1], [8, 10, 12, 14])
        self.assertEqual(len(Adapter.calls), 9)
        summary = smoke._verify(self.document, result, self.config)
        self.assertEqual((summary["planned"], summary["attempted"], summary["completed"]), (20, 12, 12))
        self.assertEqual(summary["get_outcomes"], {
            "planned": 10, "attempted": 4, "completed": 4, "get_hit": 2,
            "get_miss": 1, "get_unresolved": 0, "client_error": 1,
            "get_batch_error": 0, "not_attempted": 6})
        self.assertEqual(summary["get_adapter_calls"], 1)
        self.assertEqual(summary["get_failed_adapter_calls"], 0)
        for index in (8, 10, 12, 14):
            self.assertEqual(result["requests"][index]["started_ns"], result["groups"][8]["started_ns"])
            self.assertEqual(result["requests"][index]["completed_ns"], result["groups"][8]["completed_ns"])

    def test_whole_batch_exception_has_no_partial_success_or_fabricated_miss(self):
        Adapter.raise_get = True
        result = self.run_client()
        summary = smoke._verify(self.document, result, self.config)
        self.assertFalse(result["passed"])
        self.assertEqual(summary["get_outcomes"]["get_batch_error"], 4)
        self.assertEqual(summary["get_outcomes"]["get_hit"], 0)
        self.assertEqual(summary["get_outcomes"]["get_miss"], 0)
        self.assertEqual(summary["get_failed_adapter_calls"], 1)
        self.assertEqual(result["groups"][8]["state"], "call_error")
        self.assertTrue(all("status" not in result["requests"][i] for i in (8, 10, 12, 14)))

    def test_malformed_adapter_group_never_publishes_a_valid_prefix(self):
        for results in ([{"outcome": "get_hit", "status": 0}],
                        [{"outcome": "get_hit", "status": 0}] * 3 +
                        [{"outcome": "get_miss", "status": 0}]):
            with self.subTest(results=results), mock.patch.object(Adapter, "execute_batch", return_value=results):
                result = self.run_client()
            summary = smoke._verify(self.document, result, self.config)
            self.assertEqual(summary["get_outcomes"]["get_batch_error"], 4)
            self.assertEqual(summary["get_outcomes"]["get_hit"], 0)
            self.assertEqual(summary["get_failed_adapter_calls"], 1)
            self.assertEqual(result["groups"][9]["state"], "not_attempted")

    def test_legacy_unresolved_is_known_failure_not_miss(self):
        Adapter.outcomes = {10: {"outcome": "get_unresolved"}}
        result = self.run_client()
        summary = smoke._verify(self.document, result, self.config)
        self.assertEqual(summary["get_outcomes"]["get_unresolved"], 1)
        self.assertEqual(summary["get_outcomes"]["get_miss"], 0)
        self.assertEqual(summary["get_outcomes"]["get_hit"], 3)
        self.assertFalse(result["passed"])

    def test_parent_rejects_forged_group_order_shape_times_and_counts(self):
        result = self.run_client()
        mutations = [lambda r: r["groups"][8]["request_ids"].reverse(),
                     lambda r: r["groups"].pop(),
                     lambda r: r["requests"][8].update(group_id=9),
                     lambda r: r["requests"][10].update(completed_ns=10**12),
                     lambda r: r["requests"][8].update(status=True),
                     lambda r: r["requests"][8].update(terminal="get_miss", status=0),
                     lambda r: r["summary"]["get_outcomes"].update(get_hit=0),
                     lambda r: r["summary"].update(get_adapter_calls=1),
                     lambda r: r.update(model=smoke.MODEL),
                     lambda r: r["requests"][0].update(length=999)]
        for change in mutations:
            with self.subTest(change=change):
                candidate = copy.deepcopy(result)
                change(candidate)
                with self.assertRaises((ValueError, TypeError)):
                    smoke._verify(self.document, candidate, self.config)
        Adapter.outcomes = {10: {"outcome": "get_miss", "status": 1}}
        failed = self.run_client()
        failed["groups"][9] = copy.deepcopy(result["groups"][9])
        for index in (9, 11, 13, 15):
            failed["requests"][index] = copy.deepcopy(result["requests"][index])
        with self.assertRaises(ValueError):
            smoke._verify(self.document, failed, self.config)

    def test_default_scalar_model_and_command_remain_unchanged(self):
        config = smoke._config(__file__, "n1=127.0.0.1:12000")
        self.assertNotIn("batch_gets", config)
        result = smoke.run(self.document, config, library_loader=lambda _: object(), client_factory=Adapter)
        self.assertEqual(result["model"], smoke.MODEL)
        self.assertNotIn("groups", result)
        self.assertNotIn("get_outcomes", result["summary"])
        self.assertNotIn("--batch-gets", smoke._command(self.source, self.base / "raw", config))
        self.assertIn("--batch-gets", smoke._command(self.source, self.base / "raw", self.config))
        self.assertTrue(result["passed"])
        for value in (1, "true", None):
            with self.assertRaises(ValueError):
                smoke._config(__file__, "n1=127.0.0.1:12000", batch_gets=value)

    def test_guarded_child_positive_counts_and_process_timeout_remain_unknown(self):
        report = self.supervise(self.command())
        self.assertTrue(report["passed"])
        self.assertTrue(report["reaped"])
        self.assertEqual(report["get_outcomes"]["get_hit"], 10)
        self.assertEqual(report["get_adapter_calls"], 4)
        self.assertEqual(report["completed_requests"], 20)
        with tempfile.TemporaryDirectory() as other:
            self.base = Path(other)
            report = self.supervise([sys.executable, "-c", "import time;time.sleep(60)"])
        self.assertEqual(report["status"], "process_timeout")
        self.assertTrue(report["reaped"])
        self.assertIsNone(report["get_outcomes"])
        self.assertIsNone(report["get_adapter_calls"])
        self.assertIsNone(report["get_failed_adapter_calls"])
        self.assertIsNone(report["completed_requests"])

    def test_guarded_known_failure_retains_full_group_counts(self):
        command = self.command()
        command[-1] = command[-1].replace("r=s.run(",
            "Adapter.outcomes={10:{'outcome':'get_miss','status':1}}\nr=s.run(")
        report = self.supervise(command)
        self.assertFalse(report["passed"])
        self.assertEqual(report["status"], "workload_failed")
        self.assertEqual(report["request_outcomes"], "complete_child_ledger")
        self.assertEqual((report["attempted_requests"], report["completed_requests"]), (12, 12))
        self.assertEqual(report["get_outcomes"]["get_hit"], 3)
        self.assertEqual(report["get_outcomes"]["get_miss"], 1)
        self.assertEqual(report["get_outcomes"]["not_attempted"], 6)
        self.assertEqual(report["get_failed_adapter_calls"], 0)

    def test_guarded_forged_summary_is_invalid_not_zero_failure_counts(self):
        report = self.supervise(self.command(
            "r['summary']['get_outcomes']['get_miss']=1;(p/'child-result.json').write_bytes(t.encode(r))"))
        self.assertEqual(report["status"], "invalid_result")
        self.assertFalse(report["passed"])
        self.assertIsNone(report["get_outcomes"])
        self.assertIsNone(report["completed_requests"])


class OwnedClient:
    transport = "test-only"

    def __init__(self):
        self.calls = []
        self.results = "default"
        self.closed = 0
        self.entered = self.release = None

    def read_outcomes(self, keys, values):
        self.calls.append((keys, values))
        if self.entered is not None:
            self.entered.set()
            if not self.release.wait(2):
                raise RuntimeError("test release timeout")
        return ([{"outcome": "get_hit", "status": 0} for key in keys]
                if self.results == "default" else self.results)

    def close(self):
        self.closed += 1


class BatchAdapterTest(unittest.TestCase):
    def setUp(self):
        self.document = trace.generate(objects=8, rounds=2, length=3)
        self.owned = OwnedClient()
        namespace = bytes.fromhex(self.document["requests"][0]["namespace_hex"])
        self.client = adapter.TraceClient(self.document, object(), "test", namespace,
                                          client_factory=lambda *args: self.owned)
        self.addCleanup(self.client.close)

    def test_one_call_retains_binary_order_payload_and_rejects_bad_ids_before_io(self):
        ids = [8, 10, 12, 14]
        result = self.client.execute_batch(ids)
        self.assertEqual(len(result), 4)
        self.assertEqual(len(self.owned.calls), 1)
        keys, values = self.owned.calls[0]
        self.assertEqual(keys, [bytes.fromhex(self.document["requests"][i]["key_hex"]) for i in ids])
        self.assertEqual(values, [trace.payload(self.document["requests"][i]) for i in ids])
        for bad in ([0], [9], [8, 8], [True], [1.0], [-1], [999], "8"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.client.execute_batch(bad)
        self.assertEqual(len(self.owned.calls), 1)
        self.assertEqual(self.client.execute_batch([]), [])
        self.client.close()
        with self.assertRaises(RuntimeError):
            self.client.execute_batch(ids)

    def test_complete_result_shape_is_checked_before_any_sibling_is_published(self):
        malformed = [None, [], [{"outcome": "get_hit", "status": 0}],
                     [{"outcome": "get_hit", "status": 0}, {"outcome": "get_miss", "status": 0}],
                     [{"outcome": "get_hit", "status": 0}, {"outcome": "client_error"}],
                     [{"outcome": "get_hit", "status": 0}, {"outcome": "get_hit", "status": True}]]
        for results in malformed:
            self.owned.results = results
            with self.subTest(results=results), self.assertRaises((ValueError, RuntimeError)):
                self.client.execute_batch([8, 10])
        self.owned.results = [{"outcome": "get_hit", "status": 0},
                              {"outcome": "client_error", "status": 999, "error": "unknown"}]
        self.assertEqual(self.client.execute_batch([8, 10]), self.owned.results)

    def test_close_waits_for_inflight_batch_before_releasing_handle(self):
        self.owned.entered, self.owned.release = threading.Event(), threading.Event()
        contended = threading.Event()
        lock = self.client._lock
        class ObservedLock:
            def __enter__(self):
                if lock.locked():
                    contended.set()
                lock.acquire()
            def __exit__(self, *args):
                lock.release()
        self.client._lock = ObservedLock()
        with ThreadPoolExecutor(2) as pool:
            reading = pool.submit(self.client.execute_batch, [8, 10])
            self.assertTrue(self.owned.entered.wait(1))
            closing = pool.submit(self.client.close)
            self.assertTrue(contended.wait(1))
            self.assertFalse(closing.done())
            self.assertEqual(self.owned.closed, 0)
            self.owned.release.set()
            self.assertEqual(len(reading.result(timeout=2)), 2)
            closing.result(timeout=2)
        self.assertEqual(self.owned.closed, 1)


class NativeBatchTest(unittest.TestCase):
    def setUp(self):
        build = Path(os.environ.get("DFKV_BUILD", ROOT / "build-test")).resolve()
        self.library = Path(os.environ.get("DFKV_LIB", build / "libdfkv.so")).resolve()
        server = build / "dfkv_server"
        if not server.is_file() or not self.library.is_file():
            self.skipTest("requires built dfkv_server and libdfkv.so")
        from disk_scenarios import load_library
        self.lib = load_library(str(self.library))
        if not hasattr(self.lib, "dfkv_batch_get_auto_status"):
            self.skipTest("requires new batch GET C status symbol")
        self.temp = tempfile.TemporaryDirectory(prefix="dfkv-batch-python-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        cache = self.base / "cache"
        cache.mkdir()
        log = (self.base / "server.log").open("w")
        self.addCleanup(log.close)
        env = {key: value for key, value in os.environ.items() if not key.startswith("DFKV_")}
        env.update(DFKV_RDMA="0", DFKV_STORE_ENGINE="file", DFKV_SLAB_WRITE="buffered")
        self.server = subprocess.Popen(
            [str(server), "--dir", str(cache), "--port", "0", "--cap", str(1 << 20),
             "--store-engine", "file", "--ram-tier", "off", "--slab-write", "buffered"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log, text=True, env=env)
        self.addCleanup(self.stop_server)
        self.addCleanup(self.server.stdout.close)
        with selectors.DefaultSelector() as selector:
            selector.register(self.server.stdout, selectors.EVENT_READ)
            self.assertTrue(selector.select(timeout=10), "server port greeting timed out")
        greeting = self.server.stdout.readline().strip()
        self.assertTrue(greeting.startswith("PORT "), greeting)
        self.members = f"n1=127.0.0.1:{int(greeting.split()[1])}"

    def stop_server(self):
        if self.server.poll() is None:
            self.server.terminate()
            try:
                self.server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=5)
        self.assertIsNotNone(self.server.poll())

    def test_actual_mixed_native_batch_and_old_library_projection_are_distinct(self):
        from disk_scenarios import Client
        with mock.patch.dict(os.environ, {"DFKV_RDMA": "0"}):
            client = Client(self.lib, self.members, b"batch-native\x00ns")
        self.addCleanup(client.close)
        client.put([b"hit\x00", b"large"], [b"abc", b"0123456789"])
        results = client.read_outcomes([b"hit\x00", b"absent", b"large"], [b"abc", b"abc", b"x"])
        self.assertEqual([row["outcome"] for row in results], ["get_hit", "get_miss", "client_error"])
        self.assertEqual([row["status"] for row in results], [0, 1, 5])
        self.assertIn("error", results[2])
        class LegacyView:
            def __getattr__(self, name):
                if name == "dfkv_batch_get_auto_status":
                    raise AttributeError(name)
                return getattr(self.lib, name)
        legacy = LegacyView()
        legacy.lib = self.lib
        client.lib = legacy
        projected = client.read_outcomes([b"hit\x00", b"absent", b"large"], [b"abc", b"abc", b"x"])
        self.assertEqual([row["outcome"] for row in projected],
                         ["get_hit", "get_unresolved", "get_unresolved"])
        self.assertTrue(all("status" not in row for row in projected))
        client.lib = self.lib
        self.stop_server()
        failed = client.read_outcomes([b"hit\x00", b"absent"], [b"abc", b"abc"])
        self.assertEqual([row["outcome"] for row in failed], ["client_error", "client_error"])
        self.assertEqual([row["status"] for row in failed], [4, 4])

    def test_actual_mixed_native_group_reaches_a_verified_failure_ledger(self):
        document = trace.generate(objects=8, rounds=2, length=3)
        injected = []
        class InjectedClient(adapter.TraceClient):
            def execute_batch(client, request_ids):
                if request_ids == [8, 10, 12, 14]:
                    # Extra fixture actions are not trace PUTs or workload ACKs.
                    missing = bytes.fromhex(document["requests"][10]["key_hex"])
                    oversized = bytes.fromhex(document["requests"][12]["key_hex"])
                    client._client.remove(missing)
                    client._client.remove(oversized)
                    client._client.put([oversized], [b"x" * 5])
                    injected.extend(["remove-missing", "remove-oversized", "put-five-bytes"])
                return super().execute_batch(request_ids)
        with mock.patch.dict(os.environ, {"DFKV_RDMA": "0"}):
            config = smoke._config(str(self.library), self.members, batch_gets=True, tcp=True)
            result = smoke.run(document, config, client_factory=InjectedClient)
        summary = smoke._verify(document, result, config)
        self.assertFalse(result["passed"])
        self.assertEqual(injected, ["remove-missing", "remove-oversized", "put-five-bytes"])
        self.assertEqual([result["requests"][i]["terminal"] for i in (8, 10, 12, 14)],
                         ["get_hit", "get_miss", "client_error", "get_hit"])
        self.assertEqual([result["requests"][i]["status"] for i in (8, 10, 12, 14)], [0, 1, 5, 0])
        self.assertEqual((summary["planned"], summary["attempted"], summary["completed"]), (20, 12, 12))
        self.assertEqual(summary["acked_put_bytes"], 24)
        self.assertEqual(summary["hit_get_bytes"], 6)
        self.assertEqual(summary["get_outcomes"]["get_miss"], 1)
        self.assertEqual(summary["get_outcomes"]["client_error"], 1)
        self.assertEqual(summary["get_outcomes"]["not_attempted"], 6)
        self.assertEqual(summary["get_adapter_calls"], 1)
        self.assertEqual(result["groups"][9]["state"], "not_attempted")
        # Re-parse actual serialized report data before accepting the ledger.
        (self.base / "mixed-trace.json").write_bytes(trace.encode(document))
        (self.base / "mixed-child-result.json").write_bytes(trace.encode(result))
        (self.base / "mixed-injections.json").write_bytes(trace.encode({
            "actions": injected, "extra_remove_calls": 2, "extra_put_calls": 1,
            "extra_put_bytes": 5, "included_in_trace_ack_denominator": False}))
        self.assertEqual(smoke._verify(document, trace.load(self.base / "mixed-child-result.json"), config), summary)

    def test_actual_guarded_batch_trace_reaches_verified_parent_report(self):
        document = trace.generate(objects=8, rounds=2, length=17)
        source = self.base / "trace.json"
        source.write_bytes(trace.encode(document))
        report = smoke.supervise(source, self.base / "run", lib=str(self.library),
                                 members=self.members, batch_gets=True, tcp=True,
                                 process_timeout_sec=10)
        self.assertTrue(report["passed"], (report, trace.load(self.base / "run/child-result.json")))
        self.assertTrue(report["reaped"])
        self.assertEqual((report["attempted_requests"], report["completed_requests"]), (20, 20))
        self.assertEqual(report["get_outcomes"]["get_hit"], 10)
        self.assertEqual(report["get_outcomes"]["get_miss"], 0)
        self.assertEqual(report["get_adapter_calls"], 4)
        self.assertEqual(report["get_failed_adapter_calls"], 0)
        self.assertIn("--tcp", report["command"])
        self.assertEqual(report["config"]["client_environment"]["DFKV_RDMA"], "0")
        child = trace.load(self.base / "run/child-result.json")
        self.assertTrue(all(item["mode"] == "tcp(rdma-not-requested)" for item in child["transports"]))
        self.assertEqual([group["request_ids"] for group in child["groups"] if len(group["request_ids"]) > 1],
                         [[8, 10, 12, 14], [9, 11, 13, 15]])
        self.assertTrue(all(row["status"] == 0 for row in child["requests"] if row["op"] == "get"))
        self.assertEqual((self.base / "run/trace.json").read_bytes(), source.read_bytes())
        self.assertEqual(report["acked_put_bytes"], 170)
        self.assertEqual(json.loads((self.base / "run/report.json").read_text()), report)


if __name__ == "__main__":
    unittest.main()
