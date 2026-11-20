"""SG validation plan, hostile ledgers, exact-child guard and tiny TCP checks."""

import copy
import os
from pathlib import Path
import selectors
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/validation"))
import eviction_trace as trace
import sg_validation as sg


class FakeNative:
    instances = []
    fail_call = None
    wrong_bytes = False
    fail_put = False
    error_put = False
    startup_error = False
    width = 29

    def __init__(self, cfg):
        if self.startup_error:
            raise RuntimeError("controlled open failure")
        self.transport = "tcp(rdma-not-requested)" if cfg["client_environment"]["DFKV_RDMA"] == "0" else "rdma"
        self.sg_width = self.width
        self.closed = False
        self.calls = []
        self.instances.append(self)

    def close(self):
        self.closed = True

    def put(self, items):
        self.calls.append("put")
        if self.error_put:
            raise RuntimeError("controlled PUT exception")
        return (0, [0 if self.fail_put else 1] * len(items))

    def get(self, call):
        self.calls.append(call["name"])
        if call["name"] == self.fail_call:
            return {"rc": -1, "statuses": [0] * len(call["items"]),
                    "lengths": [999] * len(call["items"]), "values_hex": [None] * len(call["items"])}
        codes = list(call["expected"])
        lengths, values = [], []
        for item, status in zip(call["items"], codes):
            hit = status == (0 if call["api"] == "status" else 1)
            lengths.append(item["length"] if hit else 0)
            value = sg._payload(bytes.fromhex(item["key_hex"]), item["length"]).hex() if hit else None
            if hit and self.wrong_bytes:
                value = "ff" * item["length"]
            values.append(value)
        return {"rc": 0, "statuses": codes, "lengths": lengths, "values_hex": values}


class PureSgTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.cfg = sg.config(__file__, "n=127.0.0.1:12001", "unit-tag")
        FakeNative.instances = []
        FakeNative.fail_call = None
        FakeNative.wrong_bytes = FakeNative.fail_put = FakeNative.error_put = FakeNative.startup_error = False
        FakeNative.width = 29

    def good(self, cfg=None):
        cfg = cfg or self.cfg
        result = sg.run(cfg, client_factory=FakeNative)
        sg.verify(result, cfg)
        self.assertTrue(result["passed"])
        return result

    def test_fixed_plan_denominators_transport_specific_zero_guard_and_actual_width(self):
        result = self.good()
        self.assertEqual(result["summary"], {
            "planned_puts": 4, "planned_gets": 17, "attempted_puts": 4, "completed_puts": 4,
            "attempted_gets": 17, "completed_gets": 17, "acked_puts": 4, "acked_put_bytes": 321,
            "sg_calls": 6, "sg_failed_calls": 0, "hit_get_bytes": 513,
            "get_outcomes": {"hit": 7, "miss": 1, "unresolved": 2, "error": 7,
                             "wrong_bytes": 0, "call_error": 0, "not_attempted": 0}})
        self.assertEqual(result["calls"][3]["statuses"], [5, 5, 5])
        self.assertEqual(result["calls"][2]["items"][0]["caps"], [1] * 129)
        self.assertEqual(result["calls"][0]["items"][0]["caps"], [3, 0, 61])
        self.assertTrue(all(b"\0" in bytes.fromhex(i["key_hex"]) for i in result["setup"]["items"]))
        self.assertTrue(FakeNative.instances[-1].closed)
        tcp = sg.config(__file__, "n=127.0.0.1:12000", "unit-tag", tcp=True)
        tcp_result = self.good(tcp)
        self.assertEqual(tcp_result["calls"][3]["statuses"], [1, 1, 1])
        self.assertEqual(tcp_result["calls"][-1]["name"], "tcp-zero-nonempty")
        self.assertEqual(tcp_result["calls"][-1]["statuses"], [4, 4])
        self.assertEqual(tcp_result["summary"]["planned_gets"], 19)
        self.assertEqual(tcp_result["summary"]["get_outcomes"]["error"], 6)
        FakeNative.width = 129
        rejected = sg.run(self.cfg, client_factory=FakeNative)
        self.assertFalse(rejected["passed"])
        self.assertEqual(rejected["summary"]["attempted_gets"], 0)

    def test_stopped_is_four_explicit_io_errors_not_miss_or_new_put(self):
        cfg = sg.config(__file__, "n=127.0.0.1:12001", "unit-tag", mode="stopped")
        result = self.good(cfg)
        self.assertEqual(result["calls"][0]["statuses"], [4] * 4)
        self.assertEqual(result["summary"]["planned_puts"], 0)
        self.assertEqual(result["summary"]["get_outcomes"]["error"], 4)
        self.assertEqual(result["summary"]["get_outcomes"]["miss"], 0)
        self.assertEqual(FakeNative.instances[-1].calls, ["stopped"])
        FakeNative.startup_error = True
        failed = sg.run(cfg, client_factory=FakeNative)
        self.assertEqual(sg.verify(failed, cfg)["get_outcomes"]["not_attempted"], 4)
        self.assertFalse(failed["passed"])

    def test_config_rejects_remote_dns_bad_tag_flags_library_and_timeout(self):
        for member in ("n=localhost:1", "n=10.0.0.1:1", "n=127.0.0.1:0", "n=127.0.0.1:65536", "a=127.0.0.1:1,b=127.0.0.1:2"):
            with self.subTest(member=member), self.assertRaises(ValueError):
                sg.config(__file__, member, "tag")
        for tag in ("", "../tag", "x" * 81, "\u4e2d"):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                sg.config(__file__, "n=127.0.0.1:1", tag)
        for tcp in (0, 1, None, "yes"):
            with self.assertRaises(ValueError):
                sg.config(__file__, "n=127.0.0.1:1", "tag", tcp=tcp)
        for timeout in (True, 0, 31, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                sg.supervise(self.base / "out", lib=__file__, members="n=127.0.0.1:1", tag="tag", process_timeout_sec=timeout)
        self.assertFalse((self.base / "out").exists())

    def test_wrong_bytes_failure_stops_later_calls_and_does_not_count_hit(self):
        FakeNative.wrong_bytes = True
        result = sg.run(self.cfg, client_factory=FakeNative)
        summary = sg.verify(result, self.cfg)
        self.assertFalse(result["passed"])
        self.assertEqual(summary["get_outcomes"]["wrong_bytes"], 1)
        self.assertEqual(summary["hit_get_bytes"], 0)
        self.assertEqual(summary["attempted_gets"], 1)
        self.assertEqual(summary["get_outcomes"]["not_attempted"], 16)
        self.assertTrue(FakeNative.instances[-1].closed)

    def test_whole_call_error_discards_output_slots_and_stops_complete_denominator(self):
        FakeNative.fail_call = "mixed"
        result = sg.run(self.cfg, client_factory=FakeNative)
        summary = sg.verify(result, self.cfg)
        self.assertFalse(result["passed"])
        self.assertEqual(summary["attempted_gets"], 5)
        self.assertEqual(summary["completed_gets"], 1)
        self.assertEqual(summary["get_outcomes"]["call_error"], 4)
        self.assertEqual(summary["get_outcomes"]["miss"], 0)
        self.assertEqual(summary["get_outcomes"]["not_attempted"], 12)

    def test_setup_failure_and_exception_never_read_and_close_handle(self):
        for flag in ("fail_put", "error_put", "startup_error"):
            setattr(FakeNative, flag, True)
            result = sg.run(self.cfg, client_factory=FakeNative)
            summary = sg.verify(result, self.cfg)
            self.assertFalse(result["passed"])
            self.assertEqual(summary["attempted_gets"], 0)
            self.assertEqual(summary["get_outcomes"]["not_attempted"], 17)
            if flag != "startup_error":
                self.assertTrue(FakeNative.instances[-1].closed)
            setattr(FakeNative, flag, False)

    def test_verifier_rejects_forged_identity_status_shape_byte_length_summary_transport(self):
        result = self.good()
        edits = [lambda r: r.update(config=dict(r["config"], tag="other")),
                 lambda r: r.update(transport="tcp"), lambda r: r.update(sg_width=129),
                 lambda r: r["calls"].pop(), lambda r: r["calls"][0]["items"][0].update(caps=[64]),
                 lambda r: r["calls"][0].update(expected=[1]),
                 lambda r: r["calls"][1].update(statuses=[0]),
                 lambda r: r["calls"][1]["statuses"].__setitem__(0, True),
                 lambda r: r["calls"][1]["statuses"].__setitem__(0, -999),
                 lambda r: r["calls"][1]["lengths"].__setitem__(1, 1),
                 lambda r: r["calls"][0].update(values_hex=["00"]),
                 lambda r: r["calls"][0].update(lengths=[65]),
                 lambda r: r["summary"].update(acked_put_bytes=999),
                 lambda r: r.update(elapsed_ns=0), lambda r: r.update(passed=False),
                 lambda r: r["calls"][0].update(error="unexpected"), lambda r: r.update(unexpected=True),
                 lambda r: r["setup"].update(acks=[True] * 4),
                 lambda r: r["setup"].update(error="contradictory hidden setup failure"),
                 lambda r: r["setup"].update(unexpected=True)]
        for index, edit in enumerate(edits):
            bad = copy.deepcopy(result)
            edit(bad)
            with self.subTest(index=index), self.assertRaises(ValueError):
                sg.verify(bad, self.cfg)
        bad = copy.deepcopy(result)
        bad["calls"][0]["values_hex"] = ["ff" * 64]
        # Byte substitution with an unchanged claim of success must fail too.
        with self.assertRaises(ValueError):
            sg.verify(bad, self.cfg)

    def test_verifier_rejects_execution_after_failure_skips_and_unattempted_activity(self):
        result = self.good()
        for edit in (lambda r: r["calls"][0].update(state="not_attempted"),
                     lambda r: r["calls"][1]["statuses"].__setitem__(0, 4),
                     lambda r: r["calls"][1].update(started_ns=0)):
            bad = copy.deepcopy(result)
            edit(bad)
            with self.assertRaises(ValueError):
                sg.verify(bad, self.cfg)

    def child_command(self, suffix=""):
        return [sys.executable, "-c", f'''
import sys
sys.path[:0]=[{str(Path(__file__).parent)!r},{str(ROOT / 'deploy/validation')!r}]
from test_sg_validation import FakeNative
import sg_validation as s
import eviction_trace as t
from pathlib import Path
p=Path({str(self.base / 'run')!r})
c=t.load(p/'config.json')
r=s.run(c,client_factory=FakeNative)
(p/'child-result.json').write_bytes(t.encode(r))
{suffix}
sys.exit(0 if r['passed'] else 1)
''']

    def test_supervisor_actual_child_pass_unknown_crash_and_timeout_override(self):
        with mock.patch.object(sg, "_command", return_value=self.child_command()):
            report = sg.supervise(self.base / "run", lib=__file__, members="n=127.0.0.1:12001", tag="unit-tag")
        self.assertTrue(report["passed"])
        self.assertTrue(report["reaped"])
        self.assertEqual(report["summary"]["planned_gets"], 17)
        self.assertEqual(report["outcomes"], "complete_child_ledger")
        with self.assertRaises(FileExistsError):
            sg.supervise(self.base / "run", lib=__file__, members="n=127.0.0.1:12001", tag="unit-tag")
        for name, suffix, status in (("timeout", "import time; time.sleep(5)", "process_timeout"),
                                      ("crash", "import os; os._exit(7)", "child_exit"),
                                      ("forged", "r['summary']['planned_gets']=999; (p/'child-result.json').write_bytes(t.encode(r))", "invalid_result")):
            command = [part.replace(str(self.base / "run"), str(self.base / name)) for part in self.child_command(suffix)]
            with mock.patch.object(sg, "_command", return_value=command):
                report = sg.supervise(self.base / name, lib=__file__, members="n=127.0.0.1:12001", tag="unit-tag", process_timeout_sec=.5 if name == "timeout" else 5)
            self.assertFalse(report["passed"])
            self.assertEqual(report["status"], status)
            self.assertEqual(report["outcomes"], "unknown")
            self.assertIsNone(report["summary"])
            self.assertTrue(report["reaped"])
            self.assertIsInstance(report["child_pid"], int)
            if name == "timeout":
                # Prove the valid-looking file existed before the deadline:
                # the parent must ignore it after timing out the live child.
                self.assertTrue(trace.load(self.base / name / "child-result.json")["passed"])


class NativeSgTest(unittest.TestCase):
    def setUp(self):
        build = Path(os.environ.get("DFKV_BUILD", ROOT / "build-test")).resolve()
        self.library = Path(os.environ.get("DFKV_LIB", build / "libdfkv.so")).resolve()
        binary = build / "dfkv_server"
        if not binary.is_file() or not self.library.is_file():
            self.skipTest("built dfkv_server/libdfkv.so required")
        import ctypes as ct
        if not hasattr(ct.CDLL(str(self.library)), "dfkv_batch_get_auto_sg_status"):
            self.skipTest("new SG status symbol required")
        self.temp = tempfile.TemporaryDirectory(prefix="dfkv-sg-validation-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        cache = self.base / "cache"
        cache.mkdir()
        log = (self.base / "server.log").open("w")
        self.addCleanup(log.close)
        env = {k: v for k, v in os.environ.items() if not k.startswith("DFKV_")}
        env.update(DFKV_RDMA="0", DFKV_STORE_ENGINE="file", DFKV_SLAB_WRITE="buffered")
        self.server = subprocess.Popen([str(binary), "--dir", str(cache), "--port", "0", "--cap", str(1 << 20),
             "--store-engine", "file", "--ram-tier", "off", "--slab-write", "buffered"],
             stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log, text=True, env=env)
        self.addCleanup(self.stop_server)
        self.addCleanup(self.server.stdout.close)
        with selectors.DefaultSelector() as selector:
            selector.register(self.server.stdout, selectors.EVENT_READ)
            self.assertTrue(selector.select(timeout=10), "server port greeting timed out")
            greeting = self.server.stdout.readline().strip()
        self.assertTrue(greeting.startswith("PORT ") and greeting[5:].isdigit(), greeting)
        self.members = "n=127.0.0.1:" + greeting[5:]

    def stop_server(self):
        if self.server.poll() is None:
            self.server.terminate()
            try:
                self.server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=3)

    def test_public_supervisor_real_tcp_all_sg_cases_and_stopped_service(self):
        live = sg.supervise(self.base / "live", lib=self.library, members=self.members, tag="native", tcp=True)
        self.assertTrue(live["passed"], (live, (self.base / "live/stderr.log").read_text()))
        self.assertTrue(live["reaped"])
        raw = trace.load(self.base / "live/child-result.json")
        self.assertEqual(raw["transport"], "tcp(rdma-not-requested)")
        self.assertEqual(raw["summary"]["planned_gets"], 19)
        self.assertEqual(raw["calls"][1]["statuses"], [0, 1, 5, 0])
        self.assertEqual(raw["calls"][3]["statuses"], [1, 1, 1])
        self.assertEqual(raw["calls"][4]["statuses"], [5, 5, 5, 0])
        self.assertEqual(raw["calls"][5]["statuses"], [1, 0, 0, 1])
        self.assertEqual(len(raw["calls"][2]["items"][0]["caps"]), 129)
        self.assertEqual(raw["summary"]["hit_get_bytes"], 513)
        self.assertEqual(raw["calls"][-1]["name"], "tcp-zero-nonempty")
        self.assertEqual(raw["calls"][-1]["statuses"], [4, 4])
        self.stop_server()
        stopped = sg.supervise(self.base / "stopped", lib=self.library, members=self.members, tag="native", mode="stopped", tcp=True)
        self.assertTrue(stopped["passed"], stopped)
        raw = trace.load(self.base / "stopped/child-result.json")
        self.assertEqual(raw["calls"][0]["statuses"], [4] * 4)
        self.assertEqual(raw["summary"]["get_outcomes"]["miss"], 0)


if __name__ == "__main__":
    unittest.main()
