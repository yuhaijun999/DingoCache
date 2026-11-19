"""Bounded real-smoke runner contracts; child fault fixtures never use a service."""

import copy
import json
import subprocess
import time
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/validation"))
import eviction_smoke as smoke
import eviction_trace as trace


class Adapter:
    instances = []
    fail_id = None
    raise_id = None
    close_error = False
    fail_open = None

    def __init__(self, document, lib, members, namespace):
        if namespace == self.fail_open:
            raise RuntimeError("open failure")
        self.document, self.closed, self.transport = document, False, "test-only"
        self.instances.append(self)

    def execute(self, request_id):
        if request_id == self.raise_id:
            raise RuntimeError("call failure")
        if request_id == self.fail_id:
            return {"outcome": "get_unresolved"}
        return {"outcome": "put_ack" if self.document["requests"][request_id]["op"] == "put" else "get_hit"}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True
        if self.close_error:
            raise RuntimeError("close failure")


class SmokeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.document = trace.generate(objects=2, rounds=2, length=3)
        self.source = self.base / "source.json"
        self.source.write_bytes(trace.encode(self.document))
        self.config = smoke._config(__file__, "n1=127.0.0.1:12000")
        Adapter.instances, Adapter.fail_id, Adapter.raise_id = [], None, None
        Adapter.close_error, Adapter.fail_open = False, None

    def run_client(self):
        return smoke.run(self.document, self.config, library_loader=lambda _: object(), client_factory=Adapter)

    def supervise(self, command=None, timeout=2):
        with mock.patch.object(smoke, "_command", return_value=command) if command else mock.patch.object(smoke, "CLIENT_ENV", smoke.CLIENT_ENV):
            return smoke.supervise(self.source, self.base / "run", lib=__file__,
                                   members="n1=127.0.0.1:12000", process_timeout_sec=timeout)

    def command(self, suffix="", rc=None):
        # A controlled child fixture creates a real valid ledger using the worker
        # engine, but its Adapter is in-memory: not a native/service smoke.
        return [sys.executable, "-c", f'''
import sys
sys.path.insert(0, {str(Path(__file__).parent)!r})
from test_eviction_smoke import Adapter
import eviction_smoke as s
import eviction_trace as t
from pathlib import Path
p=Path({str(self.base / 'run')!r})
c=s._config({str(Path(__file__).resolve())!r}, 'n1=127.0.0.1:12000')
r=s.run(t.load(p/'trace.json'), c, library_loader=lambda _: object(), client_factory=Adapter)
(p/'child-result.json').write_bytes(t.encode(r))
{suffix}
sys.exit({rc!r} if {rc!r} is not None else (0 if r['passed'] else 1))
''']

    def test_success_is_byte_immutable_and_closes_all_handles(self):
        original = copy.deepcopy(self.document)
        result = self.run_client()
        self.assertTrue(result["passed"])
        self.assertEqual(smoke._verify(self.document, result, self.config)["completed"], 8)
        self.assertEqual(result["summary"]["acked_put_bytes"], 12)
        self.assertEqual(self.document, original)
        self.assertEqual(len(Adapter.instances), 2)
        self.assertTrue(all(client.closed for client in Adapter.instances))

    def test_unresolved_warmup_blocks_measurement_and_remaining_warmup(self):
        Adapter.fail_id = 2
        result = self.run_client()
        self.assertFalse(result["passed"])
        self.assertEqual([r["terminal"] for r in result["requests"]],
                         ["put_ack", "put_ack", "get_unresolved"] + ["not_attempted"] * 5)
        self.assertEqual(smoke._verify(self.document, result, self.config)["attempted"], 3)

    def test_call_and_close_failures_remain_failures(self):
        Adapter.raise_id = 0
        result = self.run_client()
        self.assertEqual(result["requests"][0]["terminal"], "client_error")
        self.assertEqual(smoke._verify(self.document, result, self.config)["completed"], 1)
        Adapter.raise_id, Adapter.close_error = None, True
        result = self.run_client()
        self.assertFalse(result["passed"])
        self.assertTrue(result["errors"])
        self.assertEqual(smoke._verify(self.document, result, self.config)["completed"], 8)
        self.assertTrue(all(client.closed for client in Adapter.instances))

    def test_setup_failure_closes_prior_handle_and_does_not_offer_requests(self):
        Adapter.fail_open = bytes.fromhex(self.document["requests"][1]["namespace_hex"])
        result = self.run_client()
        self.assertTrue(Adapter.instances[0].closed)
        self.assertEqual(smoke._verify(self.document, result, self.config)["attempted"], 0)
        self.assertFalse(result["passed"])

    def test_bad_bounds_library_change_and_no_nonloopback_targets(self):
        for document in (trace.generate(objects=9), trace.generate(rounds=5), trace.generate(length=4097)):
            with self.assertRaises(ValueError):
                smoke._bounded(document)
        for members in ("n1=10.0.0.1:1", "n1=localhost:1", "n1=127.0.0.1:0", "n1=127.0.0.1:65536", "n1=127.0.0.1:1,n2=127.0.0.1:2"):
            with self.assertRaises(ValueError):
                smoke._config(__file__, members)
        self.config["lib_sha256"] = "changed"
        result = self.run_client()
        self.assertFalse(result["passed"])
        self.assertEqual(Adapter.instances, [])

    def test_forged_success_counts_identity_timing_and_resume_are_rejected(self):
        result = self.run_client()
        mutations = [lambda r: r.update(passed=False),
                     lambda r: r["requests"].pop(),
                     lambda r: r["requests"][0].update(terminal="get_hit"),
                     lambda r: r["requests"][0].update(length=4),
                     lambda r: r["requests"][1].update(started_ns=0),
                     lambda r: r["summary"].update(completed=0)]
        for change in mutations:
            forged = copy.deepcopy(result)
            change(forged)
            with self.assertRaises(ValueError):
                smoke._verify(self.document, forged, self.config)
        Adapter.fail_id = 2
        failed = self.run_client()
        failed["requests"][3].update(terminal="get_hit", started_ns=10**12, completed_ns=10**12)
        with self.assertRaises(ValueError):
            smoke._verify(self.document, failed, self.config)

    def test_actual_child_success_retains_logs_and_trace(self):
        report = self.supervise(self.command())
        self.assertTrue(report["passed"])
        self.assertTrue(report["reaped"])
        self.assertEqual(report["completed_requests"], 8)
        self.assertEqual((self.base/'run/trace.json').read_bytes(), self.source.read_bytes())
        self.assertTrue((self.base/'run/stderr.log').exists())
        with self.assertRaises(FileExistsError):
            smoke.supervise(self.source, self.base/'run', lib=__file__, members='n1=127.0.0.1:12000')

    def test_actual_hang_is_terminated_and_counts_unknown(self):
        report = self.supervise([sys.executable, "-c", "import time;time.sleep(60)"], timeout=.1)
        self.assertEqual(report["status"], "process_timeout")
        self.assertTrue(report["reaped"])
        self.assertIsNone(report["completed_requests"])
        self.assertIsNone(report["acked_put_bytes"])
        self.assertFalse(report["passed"])

    def test_term_ignoring_child_is_killed(self):
        ready = self.base/'ready'
        command = [sys.executable, '-c', f"import signal,time;from pathlib import Path;signal.signal(signal.SIGTERM,signal.SIG_IGN);Path({str(ready)!r}).touch();time.sleep(60)"]
        report = self.supervise(command, timeout=1)
        self.assertTrue(ready.exists())
        self.assertEqual(report["status"], "process_timeout")
        self.assertTrue(report["reaped"])
        self.assertEqual([a["action"] for a in report["actions"]], ["terminate", "kill"])

    def test_success_file_then_hang_or_wrong_exit_never_passes(self):
        report = self.supervise(self.command("import time;time.sleep(60)"), timeout=.5)
        self.assertEqual(report["status"], "process_timeout")
        self.assertIsNone(report["acked_put_bytes"])
        with tempfile.TemporaryDirectory() as other:
            self.base = Path(other)
            report = self.supervise(self.command(rc=1))
            self.assertEqual(report["status"], "invalid_result")

    def test_crash_missing_truncated_and_forged_child_reports_fail(self):
        for suffix, expected in (("raise SystemExit(7)", "child_exit"),
                                 ("(p/'child-result.json').unlink()", "invalid_result"),
                                 ("(p/'child-result.json').write_text('{')", "invalid_result"),
                                 ("r['requests'][0]['terminal']='get_hit';(p/'child-result.json').write_bytes(t.encode(r))", "invalid_result")):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as other:
                self.base = Path(other)
                report = self.supervise(self.command(suffix))
                self.assertEqual(report["status"], expected)
                self.assertFalse(report["passed"])
                self.assertIsNone(report["completed_requests"])

    def test_invalid_budget_and_spawn_failure(self):
        for timeout in (True, 0, float('nan'), float('inf'), 31):
            with self.assertRaises(ValueError):
                self.supervise(timeout=timeout)
        self.assertFalse((self.base/'run').exists())
        with mock.patch.object(smoke.subprocess, 'Popen', side_effect=OSError('spawn failed')):
            report = self.supervise([sys.executable])
        self.assertEqual(report['status'], 'supervisor_error')
        self.assertIsNone(report['child_pid'])
        self.assertTrue((self.base/'run/report.json').exists())

    def test_handle_close_hang_is_inside_child_budget(self):
        command = self.command()
        command[-1] = command[-1].replace("r=s.run(", "def hang(*args):\n import time;time.sleep(60)\nAdapter.__exit__=hang\nr=s.run(")
        command[-1] = command[-1].replace("import time;time.sleep(60)",
                                        f"Path({str(self.base/'close-entered')!r}).touch();import time;time.sleep(60)")
        report = self.supervise(command, timeout=1)
        self.assertTrue((self.base/'close-entered').exists())
        self.assertEqual(report['status'], 'process_timeout')
        self.assertTrue(report['reaped'])
        self.assertIsNone(report['acked_put_bytes'])
        self.assertFalse((self.base/'run/child-result.json').exists())

    def test_parent_interrupt_reaps_child_saves_report_and_reraises(self):
        original_popen = subprocess.Popen
        children = []
        def launch(*args, **kwargs):
            child = original_popen(*args, **kwargs)
            children.append(child)
            original_wait = child.wait
            interrupted = False
            def wait(timeout):
                nonlocal interrupted
                if not interrupted:
                    interrupted = True
                    raise KeyboardInterrupt('controlled interrupt')
                return original_wait(timeout=timeout)
            child.wait = wait
            return child
        with mock.patch.object(smoke.subprocess, 'Popen', side_effect=launch):
            with self.assertRaises(KeyboardInterrupt):
                self.supervise([sys.executable, '-c', 'import time;time.sleep(60)'])
        report = json.loads((self.base/'run/report.json').read_text())
        self.assertEqual(report['status'], 'supervisor_error')
        self.assertTrue(report['reaped'])
        self.assertIsNotNone(children[0].poll())

    def test_startup_cost_is_deducted_from_process_budget(self):
        original_popen = subprocess.Popen
        def launch(*args, **kwargs):
            child = original_popen(*args, **kwargs)
            time.sleep(.15)
            return child
        with mock.patch.object(smoke.subprocess, 'Popen', side_effect=launch):
            report = self.supervise(self.command(), timeout=.1)
        self.assertEqual(report['status'], 'process_timeout')
        self.assertTrue(report['reaped'])
        self.assertIsNone(report['completed_requests'])

    def test_real_entrypoint_bad_library_is_a_complete_setup_failure(self):
        # Real entrypoint/worker; this text file deliberately cannot be dlopened.
        report = self.supervise()
        self.assertEqual(report['status'], 'workload_failed')
        self.assertTrue(report['reaped'])
        self.assertEqual(report['attempted_requests'], 0)
        self.assertEqual(report['acked_put_bytes'], 0)
        self.assertEqual(report['request_outcomes'], 'complete_child_ledger')


if __name__ == '__main__':
    unittest.main()
