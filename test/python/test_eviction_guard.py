"""Real owned-child failure injection; never connects to cache services."""

import copy
import importlib.util
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = ROOT / "deploy/validation"
sys.path.insert(0, str(DIRECTORY))
import eviction_trace as trace
import eviction_realtime as realtime

SPEC = importlib.util.spec_from_file_location("eviction_guard", DIRECTORY / "eviction_guard.py")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class GuardTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.source = self.directory / "source.json"
        self.document = trace.generate(objects=1, rounds=1, length=3,
                                       interval_ns=80000000)
        self.source.write_bytes(trace.encode(self.document))
        self.output = self.directory / "run"

    def injected(self, code, **kwargs):
        command = [sys.executable, "-I", "-c", code]
        with mock.patch.object(guard, "_command", return_value=command):
            return guard.supervise(self.source, self.output, **kwargs)

    def assert_failed_unknown(self, report, status):
        self.assertFalse(report["passed"])
        self.assertEqual(report["status"], status)
        self.assertEqual(report["planned_requests"], 4)
        self.assertEqual(report["request_outcomes"], "unknown")
        self.assertIsNone(report["completed_requests"])
        self.assertIsNone(report["acked_put_bytes"])
        self.assertEqual(json.loads((self.output / "report.json").read_text()), report)

    def test_real_child_success_with_frozen_input_and_complete_report(self):
        report = guard.supervise(self.source, self.output, process_timeout_sec=5)
        self.assertTrue(report["passed"])
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["returncode"], 0)
        self.assertTrue(report["reaped"])
        self.assertEqual(report["actions"], [])
        self.assertEqual(report["completed_requests"], 4)
        self.assertEqual(report["acked_put_bytes"], 6)
        self.assertEqual((self.output / "trace.json").read_bytes(), self.source.read_bytes())
        self.assertTrue((self.output / "stdout.log").exists())
        self.assertTrue((self.output / "stderr.log").exists())

    def test_complete_workload_failure_keeps_known_denominators(self):
        report = guard.supervise(self.source, self.output, timeout_ns=1,
                                 process_timeout_sec=5)
        self.assertFalse(report["passed"])
        self.assertEqual(report["status"], "workload_failed")
        self.assertEqual(report["request_outcomes"], "complete_child_ledger")
        self.assertEqual(report["completed_requests"], 0)
        self.assertEqual(report["acked_put_bytes"], 0)
        self.assertTrue(report["reaped"])

    def test_hung_child_is_terminated_and_reaped(self):
        report = self.injected("import threading; print('ready', flush=True); threading.Event().wait()",
                               process_timeout_sec=.4)
        self.assert_failed_unknown(report, "process_timeout")
        self.assertTrue(report["reaped"])
        self.assertEqual(report["returncode"], -signal.SIGTERM)
        self.assertEqual([a["action"] for a in report["actions"]], ["terminate"])
        self.assertIn("ready", (self.output / "stdout.log").read_text())

    def test_term_ignoring_child_is_killed_without_touching_sibling(self):
        sibling = subprocess.Popen([sys.executable, "-I", "-c",
                                    "import threading; threading.Event().wait()"])
        try:
            with mock.patch.object(guard, "TERM_GRACE_SEC", .1):
                report = self.injected("import signal,threading; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); threading.Event().wait()",
                                       process_timeout_sec=.4)
            self.assert_failed_unknown(report, "process_timeout")
            self.assertEqual(report["returncode"], -signal.SIGKILL)
            self.assertTrue(report["reaped"])
            self.assertEqual([a["action"] for a in report["actions"]], ["terminate", "kill"])
            self.assertIn("ready", (self.output / "stdout.log").read_text())
            self.assertIsNone(sibling.poll())
        finally:
            sibling.terminate()
            sibling.wait(timeout=5)

    def test_child_exception_and_signal_exit_are_not_success(self):
        for index, code in enumerate(("raise RuntimeError('controlled child crash')",
                                      "import os,signal; os.kill(os.getpid(),signal.SIGTERM)")):
            with self.subTest(index=index):
                self.output = self.directory / str(index)
                report = self.injected(code)
                # RuntimeError exits 1 but has no valid result; signal exits negative.
                self.assert_failed_unknown(report, "invalid_result" if index == 0 else "child_exit")
                self.assertTrue(report["reaped"])
                if index == 0:
                    self.assertIn("controlled child crash", (self.output / "stderr.log").read_text())

    def test_zero_exit_missing_truncated_duplicate_result_is_rejected(self):
        for index, raw in enumerate((None, "{", '{"passed":true,"passed":true}')):
            with self.subTest(raw=raw):
                self.output = self.directory / str(index)
                path = self.output / "child-result.json"
                code = "pass" if raw is None else f"from pathlib import Path; Path({str(path)!r}).write_text({raw!r})"
                report = self.injected(code)
                self.assert_failed_unknown(report, "invalid_result")
                self.assertEqual(report["returncode"], 0)
                if raw is not None:
                    self.assertEqual(path.read_text(), raw)

    def test_rehashed_or_claimed_success_cannot_mask_bad_ledger(self):
        baseline = realtime.run(self.document, realtime.SleepingFakeBackend())
        config = dict(workers=1, queue_capacity=16, timeout_ns=1000000000, service_ns=1)
        mutations = [lambda r: r["requests"].pop(),
                     lambda r: r["requests"][0].update(request_id=False),
                     lambda r: r["summary"].update(acked_put_bytes=999),
                     lambda r: r.update(passed=False),
                     lambda r: r.update(trace_sha256="0" * 64),
                     lambda r: r["requests"][0].pop("completed_ns"),
                     lambda r: r["config"].update(workers=2)]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                candidate = copy.deepcopy(baseline)
                mutate(candidate)
                with self.assertRaises(ValueError):
                    guard._verify(self.document, candidate, config)
                self.output = self.directory / str(index)
                raw = json.dumps(candidate)
                path = self.output / "child-result.json"
                report = self.injected(f"from pathlib import Path; Path({str(path)!r}).write_text({raw!r})")
                self.assert_failed_unknown(report, "invalid_result")

    def test_success_terminal_must_match_original_operation(self):
        candidate = realtime.run(self.document, realtime.SleepingFakeBackend())
        candidate["requests"][0].update(terminal="get_hit", backend_outcome="get_hit")
        candidate["summary"]["terminal"] = {"put_ack": 1, "get_hit": 3}
        candidate["summary"].update(acked_put_bytes=3, hit_get_bytes=9)
        config = dict(workers=1, queue_capacity=16, timeout_ns=1000000000, service_ns=1)
        with self.assertRaises(ValueError):
            guard._verify(self.document, candidate, config)

    def test_started_call_requires_submission_provenance(self):
        candidate = realtime.run(self.document, realtime.SleepingFakeBackend())
        candidate["requests"][0].pop("submitted_ns")
        candidate["summary"]["submitted"] = 3
        config = dict(workers=1, queue_capacity=16, timeout_ns=1000000000, service_ns=1)
        with self.assertRaises(ValueError):
            guard._verify(self.document, candidate, config)

    def test_success_terminal_cannot_precede_actual_completion(self):
        candidate = realtime.run(self.document, realtime.SleepingFakeBackend())
        candidate["requests"][0]["terminal_ns"] = 0
        config = dict(workers=1, queue_capacity=16, timeout_ns=1000000000, service_ns=1)
        with self.assertRaises(ValueError):
            guard._verify(self.document, candidate, config)

    def test_valid_success_report_with_wrong_exit_code_still_fails(self):
        candidate = realtime.run(self.document, realtime.SleepingFakeBackend())
        path = self.output / "child-result.json"
        code = f"from pathlib import Path; Path({str(path)!r}).write_text({json.dumps(candidate)!r}); raise SystemExit(1)"
        self.assert_failed_unknown(self.injected(code), "invalid_result")

    def test_timeout_wins_even_if_child_wrote_success_then_hung(self):
        candidate = realtime.run(self.document, realtime.SleepingFakeBackend())
        path = self.output / "child-result.json"
        code = f"from pathlib import Path; import threading; Path({str(path)!r}).write_text({json.dumps(candidate)!r}); threading.Event().wait()"
        report = self.injected(code, process_timeout_sec=.4)
        self.assert_failed_unknown(report, "process_timeout")
        self.assertTrue(path.exists())
        self.assertTrue(report["reaped"])

    def test_spawn_error_is_saved_and_existing_outputs_never_overwritten(self):
        with mock.patch.object(guard, "_command", return_value=["/no/such/dfkv-child"]):
            report = guard.supervise(self.source, self.output)
        self.assert_failed_unknown(report, "supervisor_error")
        self.assertIsNone(report["child_pid"])
        self.assertFalse(report["reaped"])
        before = (self.output / "report.json").read_bytes()
        with self.assertRaises(FileExistsError):
            guard.supervise(self.source, self.output)
        self.assertEqual((self.output / "report.json").read_bytes(), before)
        link = self.directory / "link"
        link.symlink_to(self.output, target_is_directory=True)
        with self.assertRaises(FileExistsError):
            guard.supervise(self.source, link)
        self.assertEqual((self.output / "report.json").read_bytes(), before)

    def test_invalid_input_and_process_timeout_do_not_create_output(self):
        for limit in (True, float("nan"), float("inf"), 0, 61, 10**1000):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                guard.supervise(self.source, self.output, process_timeout_sec=limit)
            self.assertFalse(self.output.exists())
        for document in (trace.generate(objects=4, rounds=1, interval_ns=1000000000),
                         trace.generate(objects=5, rounds=1)):
            self.source.write_bytes(trace.encode(document))
            with self.assertRaises(ValueError):
                guard.supervise(self.source, self.output, service_ns=1000000000)
            self.assertFalse(self.output.exists())
        self.source.write_text("{}")
        with self.assertRaises(ValueError):
            guard.supervise(self.source, self.output)
        self.assertFalse(self.output.exists())

    def test_launch_delay_consumes_budget_even_with_valid_success_file(self):
        candidate = realtime.run(self.document, realtime.SleepingFakeBackend())
        raw = json.dumps(candidate)
        result_path = self.output / "child-result.json"
        command = [sys.executable, "-I", "-c",
                   f"from pathlib import Path; Path({str(result_path)!r}).write_text({raw!r})"]
        original_popen = subprocess.Popen
        def delayed_launch(*args, **kwargs):
            child = original_popen(*args, **kwargs)
            child.wait(timeout=2)
            # Parent does not receive its Popen handle until budget is gone.
            import threading
            threading.Event().wait(.1)
            return child
        with mock.patch.object(guard, "_command", return_value=command):
            with mock.patch.object(guard.subprocess, "Popen", side_effect=delayed_launch):
                report = guard.supervise(self.source, self.output, process_timeout_sec=.05)
        self.assertTrue(json.loads(result_path.read_text())["passed"])
        self.assert_failed_unknown(report, "process_timeout")
        self.assertTrue(report["reaped"])

    def test_unreapable_mock_is_reported_not_claimed_cleaned(self):
        class Unreapable:
            pid = 123
            returncode = None
            def poll(self):
                return None
            def wait(self, timeout):
                raise subprocess.TimeoutExpired("mock-only-no-process", timeout)
            def terminate(self):
                pass
            def kill(self):
                pass
        with mock.patch.object(guard.subprocess, "Popen", return_value=Unreapable()):
            report = guard.supervise(self.source, self.output)
        self.assert_failed_unknown(report, "cleanup_failed")
        self.assertEqual(report["previous_status"], "process_timeout")
        self.assertFalse(report["reaped"])
        self.assertIsNone(report["returncode"])
        self.assertEqual([a["action"] for a in report["actions"]], ["terminate", "kill"])

    def test_parent_interrupt_reaps_owned_child_saves_report_and_reraises(self):
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
                    raise KeyboardInterrupt("controlled parent interruption")
                return original_wait(timeout=timeout)
            child.wait = wait
            return child
        with mock.patch.object(guard.subprocess, "Popen", side_effect=launch):
            with self.assertRaises(KeyboardInterrupt):
                guard.supervise(self.source, self.output)
        report = json.loads((self.output / "report.json").read_text())
        self.assert_failed_unknown(report, "supervisor_error")
        self.assertIn("KeyboardInterrupt", report["error"])
        self.assertTrue(report["reaped"])
        self.assertIsNotNone(children[0].poll())

    def test_cli_no_custom_command_option_and_failure_report(self):
        script = DIRECTORY / "eviction_guard.py"
        command = [sys.executable, "-I", str(script), str(self.source), str(self.output),
                   "--timeout-ns", "1", "--process-timeout-sec", "5"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(json.loads((self.output / "report.json").read_text())["status"], "workload_failed")
        result = subprocess.run(command + ["--command", "anything"],
                                capture_output=True, text=True, timeout=15)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unrecognized arguments", result.stderr)


if __name__ == "__main__":
    unittest.main()
