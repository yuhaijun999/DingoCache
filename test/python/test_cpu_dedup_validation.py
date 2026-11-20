"""Pure CPU runtime probe contracts; no native build, service or shared-memory I/O."""

import copy
import ctypes as ct
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/validation"))
import cpu_dedup_validation as probe
import eviction_dedup as dedup
import eviction_trace as trace

TOKEN = "0123456789abcdef0123456789abcdef"


class Fake:
    instances = []
    failure = None

    def __init__(self, cfg):
        self.cfg, self.closed = cfg, False
        self.transports = ["tcp(rdma-not-requested)" if cfg["client_environment"]["DFKV_RDMA"] == "0" else "rdma"] * 2
        self.values = [dict.fromkeys(dedup.COUNTERS, 0) for _ in range(2)]
        self.seen = set()
        self.instances.append(self)

    def close(self):
        self.closed = True

    def metrics(self):
        return copy.deepcopy(self.values)

    def put(self, row):
        if self.failure == "put":
            return {"raw": {"rc": 0, "acks": [1, 0]}, "error": "partial PUT"}
        return {"raw": {"rc": 0, "acks": [1] * len(row["items"])}}

    def get(self, row):
        raw = {"rc": 0, "statuses": list(row["expected"]), "lengths": [], "values_hex": []}
        outcomes = []
        for item, status in zip(row["items"], row["expected"]):
            identity = row["namespace"], item["key_hex"]
            self.values[row["namespace"]]["hits" if identity in self.seen else "fetches"] += 1
            if status == 0:
                self.seen.add(identity)
                raw["lengths"].append(item["length"])
                raw["values_hex"].append(probe._payload(row["namespace"], bytes.fromhex(item["key_hex"]), item["length"]).hex())
                outcomes.append({"status": 0, "outcome": "get_hit"})
            else:
                raw["lengths"].append(0)
                raw["values_hex"].append(None)
                outcomes.append({"status": status, "outcome": "get_miss"} if status == 1 else
                                {"status": status, "outcome": "client_error", "error": "native failed"})
        if self.failure == "rc":
            raw["rc"] = -1
            return {"raw": raw, "error": "whole call rejected"}
        if self.failure == "bytes":
            raw["values_hex"][0] = "ff" * row["items"][0]["length"]
            outcomes[0] = {"status": 0, "outcome": "client_error", "error": "wrong bytes"}
        return {"raw": raw, "outcomes": outcomes}


class CpuProbeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.cfg = probe.config(__file__, "n=127.0.0.1:12001", "unit", token=TOKEN)
        Fake.instances, Fake.failure = [], None

    def run_probe(self, cfg=None):
        return probe.run(cfg or self.cfg, client_factory=Fake)

    def test_frozen_live_plan_full_bytes_cpu_counters_and_denominators(self):
        result = self.run_probe()
        self.assertTrue(result["passed"])
        counts = probe.verify(result, self.cfg)
        self.assertEqual((counts["planned_puts"], counts["acked_puts"], counts["acked_put_bytes"]), (3, 3, 192))
        self.assertEqual((counts["planned_gets"], counts["completed_gets"], counts["get_calls"]), (6, 6, 4))
        self.assertEqual(counts["get_outcomes"], {"hit": 4, "miss": 1, "error": 1,
                                                "wrong_bytes": 0, "call_error": 0, "not_attempted": 0})
        self.assertEqual(counts["hit_get_bytes"], 256)
        self.assertEqual(counts["cpu_dedup_observations"],
                         {"hits": 2, "fetches": 4, "wait_hits": 0, "wait_timeouts": 0})
        self.assertTrue(Fake.instances[0].closed)
        self.assertNotEqual(result["calls"][0]["raw"]["values_hex"], result["calls"][2]["raw"]["values_hex"])

    def test_stopped_plan_has_no_put_and_only_explicit_io_not_miss(self):
        cfg = probe.config(__file__, "n=127.0.0.1:12001", "unit", "stopped", token=TOKEN)
        result = self.run_probe(cfg)
        counts = probe.verify(result, cfg)
        self.assertTrue(result["passed"])
        self.assertEqual((counts["planned_puts"], counts["planned_gets"]), (0, 2))
        self.assertEqual(counts["get_outcomes"]["error"], 2)
        self.assertEqual(counts["get_outcomes"]["miss"], 0)

    def test_config_rejects_bad_transport_endpoint_tag_mode_and_token(self):
        for members in ("remote:1", "n=localhost:1", "n=127.0.0.1:0", "n=127.0.0.1:65536"):
            with self.subTest(members=members), self.assertRaises(ValueError):
                probe.config(__file__, members, "tag", token=TOKEN)
        for tag in ("", "../x", "x" * 65, "中文"):
            with self.assertRaises(ValueError):
                probe.config(__file__, "n=127.0.0.1:1", tag, token=TOKEN)
        with self.assertRaises(ValueError):
            probe.config(__file__, "n=127.0.0.1:1", "tag", "invalid", token=TOKEN)
        with self.assertRaises(ValueError):
            probe.config(__file__, "n=127.0.0.1:1", "tag", token=None)
        with self.assertRaises(ValueError):
            probe.config(__file__, "n=127.0.0.1:1", "tag", tcp=1, token=TOKEN)

    def test_whole_call_error_discards_apparent_hits_and_stops_remaining_plan(self):
        Fake.failure = "rc"
        result = self.run_probe()
        counts = probe.verify(result, self.cfg)
        self.assertFalse(result["passed"])
        self.assertEqual(counts["get_outcomes"]["call_error"], 1)
        self.assertEqual(counts["get_outcomes"]["not_attempted"], 5)
        self.assertEqual(counts["get_outcomes"]["hit"], 0)
        self.assertTrue(Fake.instances[0].closed)

    def test_partial_setup_retains_confirmed_ack_but_blocks_remaining_work(self):
        Fake.failure = "put"
        result = self.run_probe()
        counts = probe.verify(result, self.cfg)
        self.assertFalse(result["passed"])
        self.assertEqual(counts["acked_puts"], 1)
        self.assertEqual(counts["acked_put_bytes"], 64)
        self.assertEqual(counts["attempted_gets"], 0)
        self.assertEqual(counts["get_outcomes"]["not_attempted"], 6)
        forged = copy.deepcopy(result)
        forged["setup"][0]["raw"]["acks"][1] = 2
        with self.assertRaises(ValueError):
            probe.verify(forged, self.cfg)

    def test_wrong_full_bytes_fail_diagnostic_not_counted_as_hit(self):
        Fake.failure = "bytes"
        result = self.run_probe()
        counts = probe.verify(result, self.cfg)
        self.assertFalse(result["passed"])
        self.assertEqual(counts["get_outcomes"]["wrong_bytes"], 1)
        self.assertEqual(counts["hit_get_bytes"], 0)

    def test_setup_failure_and_transport_refusal_are_known_failed_ledgers(self):
        def failed(cfg):
            raise RuntimeError("missing CPU symbol")
        result = probe.run(self.cfg, client_factory=failed)
        self.assertFalse(result["passed"])
        self.assertEqual(probe.verify(result, self.cfg)["attempted_gets"], 0)
        self.assertIsNone(result["summary"]["cpu_dedup_observations"])
        class Wrong(Fake):
            def __init__(self, cfg):
                super().__init__(cfg)
                self.transports = ["tcp", "tcp"]
        result = probe.run(self.cfg, client_factory=Wrong)
        self.assertFalse(result["passed"])
        self.assertEqual(probe.verify(result, self.cfg)["attempted_gets"], 0)
        self.assertTrue(Fake.instances[-1].closed)

    def test_hostile_schema_identity_bytes_counters_and_summary_are_rejected(self):
        original = self.run_probe()
        mutations = [lambda r: r.update(extra=True), lambda r: r["config"].update(tag="other"),
                     lambda r: r["calls"][0].update(expected=[1]),
                     lambda r: r["calls"][0]["raw"].update(lengths=[]),
                     lambda r: r["calls"][0]["raw"].update(statuses=[True]),
                     lambda r: r["calls"][0]["raw"].update(values_hex=["00"]),
                     lambda r: r["calls"][3]["raw"]["lengths"].__setitem__(1, 1),
                     lambda r: r["before"][0].update(hits=True),
                     lambda r: r["after"][0].update(fetches=-1),
                     lambda r: r["after"][0].update(fetches=999),
                     lambda r: r["calls"][0].update(started_ns=-1),
                     lambda r: r["summary"].update(acked_puts=99),
                     lambda r: r["setup"][0].update(error="hidden ignored failure")]
        for mutation in mutations:
            result = copy.deepcopy(original)
            mutation(result)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                probe.verify(result, self.cfg)

    def command(self, suffix=""):
        return [sys.executable, "-c", f'''
import sys
sys.path.insert(0, {str(Path(__file__).parent)!r})
from test_cpu_dedup_validation import Fake
import cpu_dedup_validation as p
import eviction_trace as t
from pathlib import Path
root=Path({str(self.base / 'run')!r})
c=t.load(root/'config.json')
r=p.run(c,client_factory=Fake)
(root/'child-result.json').write_bytes(t.encode(r))
{suffix}
sys.exit(0 if r['passed'] else 1)
''']

    def supervise(self, command, budget=2):
        with mock.patch.object(probe, "_command", return_value=command):
            return probe.supervise(self.base / "run", lib=__file__, members="n=127.0.0.1:12001",
                                    tag="unit", process_timeout_sec=budget)

    def test_parent_accepts_only_verified_full_child_ledger(self):
        report = self.supervise(self.command())
        self.assertTrue(report["passed"], report)
        self.assertTrue(report["reaped"])
        self.assertEqual(report["summary"]["planned_gets"], 6)
        self.assertEqual(len(report["possible_retained_shm_paths"]), 2)
        self.assertEqual(report["shm_cleanup"], "not_attempted_no_exclusive_creator_proof")

    def test_timeout_overrides_completed_pass_file_and_keeps_summary_unknown(self):
        report = self.supervise(self.command("import time;time.sleep(60)"), budget=.3)
        self.assertEqual(report["status"], "process_timeout")
        self.assertTrue(report["reaped"])
        self.assertFalse(report["passed"])
        self.assertIsNone(report["summary"])
        self.assertTrue(trace.load(self.base / "run/child-result.json")["passed"])

    def test_forgery_and_crash_cannot_publish_partial_results(self):
        report = self.supervise(self.command(
            "r['summary']['planned_gets']=99;(root/'child-result.json').write_bytes(t.encode(r))"))
        self.assertEqual(report["status"], "invalid_result")
        self.assertIsNone(report["summary"])
        with tempfile.TemporaryDirectory() as temporary:
            self.base = Path(temporary)
            report = self.supervise([sys.executable, "-c", "raise SystemExit(9)"])
        self.assertEqual(report["status"], "child_exit")
        self.assertIsNone(report["summary"])
        self.assertTrue(report["reaped"])

    def test_existing_output_or_shm_is_refused_and_timeout_bounds_are_strict(self):
        (self.base / "run").mkdir()
        with self.assertRaises(FileExistsError):
            self.supervise(self.command())
        for budget in (True, 0, 31, float("nan")):
            with self.assertRaises(ValueError):
                self.supervise(self.command(), budget)
        with mock.patch.object(probe.os.path, "lexists", return_value=True):
            with self.assertRaises(ValueError):
                self.supervise(self.command())

    def test_stopped_boundary_helper_requires_warm_hit_and_cold_io(self):
        client = mock.Mock()
        warm = [{"outcome": "get_hit", "status": 0}]
        cold = [{"outcome": "client_error", "status": 4, "error": "I/O"}]
        client.read_deduplicated_outcomes.side_effect = [warm, cold]
        self.assertEqual(probe.check_stopped_boundary(client, b"warm\0", b"data", b"cold"),
                         {"warm": warm, "cold": cold})
        client.read_deduplicated_outcomes.side_effect = [warm, [{"outcome": "get_miss", "status": 1}]]
        with self.assertRaises(ValueError):
            probe.check_stopped_boundary(client, b"warm", b"data", b"cold")
        with self.assertRaises(ValueError):
            probe.check_stopped_boundary(client, b"same", b"data", b"same")

    def test_capture_reads_raw_bytes_only_inside_live_owners_and_bounds_lengths(self):
        raw = mock.Mock()
        owner = ct.create_string_buffer(b"a\0c", 3)
        data = (ct.c_void_p * 1)(ct.addressof(owner))
        caps = (ct.c_uint64 * 1)(3)
        statuses = (ct.c_int * 1)(0)
        lengths = (ct.c_uint64 * 1)(999)
        raw.dfkv_batch_get_auto_deduplicated_status.return_value = 0
        capture = probe._Capture(raw)
        self.assertEqual(capture.dfkv_batch_get_auto_deduplicated_status(
            7, None, None, data, caps, 1, statuses, lengths), 0)
        self.assertEqual(capture.raw["values_hex"], [b"a\0c".hex()])
        def mutate(*args):
            data[0], caps[0] = 1, 99999
            return 0
        raw.dfkv_batch_get_auto_deduplicated_status.side_effect = mutate
        capture.dfkv_batch_get_auto_deduplicated_status(7, None, None, data, caps, 1, statuses, lengths)
        self.assertEqual(capture.raw["values_hex"], [b"a\0c".hex()])
        raw.dfkv_batch_get_auto_deduplicated_status.side_effect = None
        raw.dfkv_batch_get_auto_deduplicated_status.return_value = -1
        capture.dfkv_batch_get_auto_deduplicated_status(7, None, None, data, caps, 1, statuses, lengths)
        self.assertEqual(capture.raw["values_hex"], [None])
