"""Real threads and monotonic timings, with Event-gated fake calls only."""

from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = ROOT / "deploy/validation"
sys.path.insert(0, str(DIRECTORY))
import eviction_trace as trace

SPEC = importlib.util.spec_from_file_location("eviction_realtime", DIRECTORY / "eviction_realtime.py")
realtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(realtime)


class GatedBackend(realtime.SleepingFakeBackend):
    def __init__(self, ids, **kwargs):
        super().__init__(**kwargs)
        self.entered = {index: threading.Event() for index in ids}
        self.release = threading.Event()

    def wait(self, row):
        if row["request_id"] in self.entered:
            self.entered[row["request_id"]].set()
            if not self.release.wait(2):
                raise TimeoutError("test gate was not released")
        else:
            super().wait(row)


class RealtimeTest(unittest.TestCase):
    def document(self, interval=80000000):
        return trace.generate(objects=1, rounds=1, length=3, interval_ns=interval)

    def check_conservation(self, result):
        rows, summary = result["requests"], result["summary"]
        self.assertEqual(summary["offered"], 4)
        self.assertEqual(sum(summary["terminal"].values()), 4)
        self.assertEqual([row["request_id"] for row in rows], list(range(4)))
        self.assertEqual(summary["started"], summary["completed"])
        self.assertLessEqual(summary["peak_reserved"], result["config"]["workers"])
        self.assertLessEqual(summary["peak_running"], summary["peak_reserved"])
        self.assertLessEqual(summary["peak_queue"], result["config"]["queue_capacity"])
        for row in rows:
            self.assertGreaterEqual(row["settled_ns"], row["terminal_ns"])
            self.assertEqual(row["observed_latency_ns"], row["settled_ns"] - row["scheduled_ns"])
            self.assertGreaterEqual(row["schedule_lag_ns"], 0)
            if "started_ns" in row:
                self.assertLess(row["started_ns"], row["deadline_ns"])
                self.assertGreaterEqual(row["completed_ns"], row["started_ns"])
                self.assertEqual(row["queue_wait_ns"], row["started_ns"] - row["admitted_ns"])

    def test_real_clock_success_and_observed_timestamps(self):
        before = time.monotonic_ns()
        result = realtime.run(self.document(), realtime.SleepingFakeBackend(1000000))
        after = time.monotonic_ns()
        self.assertTrue(result["passed"])
        self.assertEqual(result["model"], "monotonic-threaded-fake-only")
        self.assertEqual(result["summary"]["terminal"], {"put_ack": 2, "get_hit": 2})
        self.assertEqual(result["summary"]["acked_put_bytes"], 6)
        self.assertEqual(result["summary"]["hit_get_bytes"], 6)
        self.assertLessEqual(result["summary"]["drained_ns"], after - before)
        self.assertGreaterEqual(result["summary"]["drained_ns"], 240000000)
        self.check_conservation(result)

    def test_event_handshake_proves_two_real_calls_overlap(self):
        backend = GatedBackend({0, 1})
        with ThreadPoolExecutor(max_workers=1) as controller:
            future = controller.submit(realtime.run, self.document(), backend, workers=2)
            try:
                self.assertTrue(backend.entered[0].wait(2))
                self.assertTrue(backend.entered[1].wait(2))
            finally:
                backend.release.set()
            result = future.result(timeout=5)
        first, second = result["requests"][:2]
        self.assertLess(first["started_ns"], second["completed_ns"])
        self.assertLess(second["started_ns"], first["completed_ns"])
        self.assertEqual(result["summary"]["peak_running"], 2)
        self.assertTrue(result["passed"])
        self.check_conservation(result)

    def test_queue_wait_is_measured_not_replaced_by_service_time(self):
        backend = GatedBackend({0})
        with ThreadPoolExecutor(max_workers=1) as controller:
            future = controller.submit(realtime.run, self.document(), backend)
            try:
                self.assertTrue(backend.entered[0].wait(2))
                # Keep PUT active across the next planned arrival (80ms).
                backend.release.wait(.14)
            finally:
                backend.release.set()
            result = future.result(timeout=5)
        row = result["requests"][1]
        self.assertGreater(row["queue_wait_ns"], 0)
        self.assertGreater(row["latency_ns"], row["service_ns"])
        self.assertEqual(row["latency_ns"], row["dispatch_lag_ns"] + row["service_ns"])
        self.assertTrue(result["passed"])
        self.check_conservation(result)

    def test_active_timeout_keeps_slot_and_never_backfills_ack(self):
        backend = GatedBackend({0})
        with ThreadPoolExecutor(max_workers=1) as controller:
            future = controller.submit(realtime.run, self.document(), backend,
                                       queue_capacity=0, timeout_ns=50000000)
            try:
                self.assertTrue(backend.entered[0].wait(2))
                backend.release.wait(.35)
            finally:
                backend.release.set()
            result = future.result(timeout=5)
        first, second = result["requests"][:2]
        self.assertEqual(first["terminal"], "timeout")
        self.assertEqual(first["terminal_ns"], first["deadline_ns"])
        self.assertGreater(first["completed_ns"], first["deadline_ns"])
        self.assertEqual(first["backend_outcome"], "put_ack")
        self.assertEqual(second["terminal"], "queue_full")
        self.assertNotIn("started_ns", second)
        self.assertEqual(result["summary"]["acked_put_bytes"], 0)
        self.assertEqual(result["summary"]["late_completions"], 1)
        self.assertFalse(result["passed"])
        self.check_conservation(result)

    def test_waiting_timeout_never_starts_a_call(self):
        backend = GatedBackend({0})
        with ThreadPoolExecutor(max_workers=1) as controller:
            future = controller.submit(realtime.run, self.document(), backend,
                                       queue_capacity=8, timeout_ns=50000000)
            try:
                self.assertTrue(backend.entered[0].wait(2))
                backend.release.wait(.35)
            finally:
                backend.release.set()
            result = future.result(timeout=5)
        second = result["requests"][1]
        self.assertEqual(second["terminal"], "timeout")
        self.assertNotIn("submitted_ns", second)
        self.assertNotIn("started_ns", second)
        self.assertNotIn("completed_ns", second)
        self.check_conservation(result)

    def test_submitted_task_expiring_before_worker_entry_does_not_call_backend(self):
        class DelayedExecutor(ThreadPoolExecutor):
            def submit(self, fn, *args, **kwargs):
                def delayed():
                    threading.Event().wait(.2)
                    return fn(*args, **kwargs)
                return super().submit(delayed)
        with mock.patch.object(realtime, "ThreadPoolExecutor", DelayedExecutor):
            result = realtime.run(self.document(), realtime.SleepingFakeBackend(),
                                  queue_capacity=0, timeout_ns=50000000)
        first = result["requests"][0]
        self.assertEqual(first["terminal"], "timeout")
        self.assertIn("submitted_ns", first)
        self.assertIn("worker_expired_ns", first)
        self.assertNotIn("started_ns", first)
        self.assertGreater(result["summary"]["submitted"], result["summary"]["started"])
        self.check_conservation(result)

    def test_scheduler_stall_preserves_all_ids_without_catchup_burst(self):
        original = realtime.ThreadPoolExecutor
        class SlowSubmitExecutor(original):
            def submit(self, fn, *args, **kwargs):
                # Force scheduler lateness, not backend slowness.
                threading.Event().wait(.2)
                return super().submit(fn, *args, **kwargs)
        with mock.patch.object(realtime, "ThreadPoolExecutor", SlowSubmitExecutor):
            result = realtime.run(self.document(), realtime.SleepingFakeBackend())
        self.assertEqual(result["requests"][1]["terminal"], "arrival_late")
        self.assertNotIn("submitted_ns", result["requests"][1])
        self.assertFalse(result["passed"])
        self.check_conservation(result)

    def test_miss_errors_corruption_and_worker_exception_are_preserved(self):
        for field, outcome in (("miss_ids", "get_miss"), ("error_ids", "backend_error"),
                               ("corrupt_ids", "wrong_bytes")):
            with self.subTest(field=field):
                result = realtime.run(self.document(), realtime.SleepingFakeBackend(**{field: {1}}))
                self.assertEqual(result["requests"][1]["terminal"], outcome)
                self.assertEqual(result["passed"], outcome == "get_miss")
                self.check_conservation(result)
        class Raises(realtime.SleepingFakeBackend):
            def wait(self, row):
                raise RuntimeError("controlled wait failure")
        result = realtime.run(self.document(), Raises())
        self.assertEqual(result["requests"][0]["terminal"], "backend_error")
        self.assertIn("controlled wait failure", result["requests"][0]["backend_error"])
        self.assertFalse(result["passed"])
        self.check_conservation(result)

    def test_late_corruption_is_not_hidden_by_timeout(self):
        backend = GatedBackend({1}, corrupt_ids={1})
        with ThreadPoolExecutor(max_workers=1) as controller:
            future = controller.submit(realtime.run, self.document(), backend,
                                       workers=2, timeout_ns=50000000)
            try:
                self.assertTrue(backend.entered[1].wait(2))
                backend.release.wait(.15)
            finally:
                backend.release.set()
            result = future.result(timeout=5)
        row = result["requests"][1]
        self.assertEqual(row["terminal"], "timeout")
        self.assertEqual(row["backend_outcome"], "wrong_bytes")
        self.assertEqual(result["summary"]["wrong_bytes"], 1)
        self.assertFalse(result["passed"])
        self.assertFalse(any(thread.name.startswith("dfkv-fake-replay")
                             for thread in threading.enumerate()))
        self.check_conservation(result)

    def test_completion_exception_and_reuse_do_not_silently_succeed(self):
        class Raises(realtime.SleepingFakeBackend):
            def complete(self, row):
                raise RuntimeError("controlled completion failure")
        backend = Raises()
        result = realtime.run(self.document(), backend)
        self.assertIn("controlled completion failure", result["requests"][0]["backend_error"])
        self.assertEqual(result["requests"][0]["terminal"], "backend_error")
        self.assertFalse(result["passed"])
        with self.assertRaises(ValueError):
            realtime.run(self.document(), backend)
        self.check_conservation(result)
        large = trace.generate(objects=5, rounds=1)
        backend = realtime.SleepingFakeBackend(1000000000)
        with self.assertRaises(ValueError):
            realtime.run(large, backend)
        self.assertFalse(backend.used)

    def test_invalid_inputs_are_rejected_before_backend_use(self):
        for config in ({"workers": True}, {"workers": 17}, {"queue_capacity": -1},
                       {"timeout_ns": 0}, {"timeout_ns": 5000000001}):
            backend = realtime.SleepingFakeBackend()
            with self.subTest(config=config), self.assertRaises(ValueError):
                realtime.run(self.document(), backend, **config)
            self.assertFalse(backend.used)
        for backend in (realtime.SleepingFakeBackend(1000000001),
                        realtime.SleepingFakeBackend(durations={4: 1}),
                        realtime.SleepingFakeBackend(miss_ids={0})):
            with self.assertRaises(ValueError):
                realtime.run(self.document(), backend)
        document = trace.generate(objects=4, rounds=1, interval_ns=1000000000)
        with self.assertRaises(ValueError):
            realtime.run(document, realtime.SleepingFakeBackend())
        document = self.document()
        document["requests"].pop()
        with self.assertRaises(ValueError):
            realtime.run(document, realtime.SleepingFakeBackend())

    def test_cli_success_failed_result_and_no_overwrite(self):
        script = DIRECTORY / "eviction_realtime.py"
        with tempfile.TemporaryDirectory() as directory:
            source, output = [Path(directory) / n for n in ("trace.json", "result.json")]
            source.write_bytes(trace.encode(self.document()))
            command = [sys.executable, "-I", str(script), str(source), str(output)]
            success = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertEqual(success.returncode, 0, success.stderr)
            original = output.read_bytes()
            self.assertTrue(json.loads(original)["passed"])
            again = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertEqual(again.returncode, 1)
            self.assertEqual(output.read_bytes(), original)
            failed = Path(directory) / "failed.json"
            result = subprocess.run(command[:-1] + [str(failed), "--timeout-ns", "1"],
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 1)
            report = json.loads(failed.read_bytes())
            self.assertFalse(report["passed"])
            self.check_conservation(report)


if __name__ == "__main__":
    unittest.main()
