"""Deterministic virtual-time queue/deadline tests; no service or wall sleeps."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = ROOT / "deploy/validation"
sys.path.insert(0, str(DIRECTORY))
import eviction_trace as trace

SPEC = importlib.util.spec_from_file_location("eviction_replay", DIRECTORY / "eviction_replay.py")
replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay)


class ReplayTest(unittest.TestCase):
    def document(self):
        return trace.generate(objects=2, rounds=2, length=3, interval_ns=10)

    def run_model(self, *, service_ns=1, **kwargs):
        return replay.run(self.document(), replay.FakeBackend(service_ns), **kwargs)

    def test_fast_backend_exact_counts_bytes_and_no_input_mutation(self):
        document = self.document()
        original = copy.deepcopy(document)
        result = replay.run(document, replay.FakeBackend(1))
        self.assertEqual(document, original)
        self.assertTrue(result["passed"])
        self.assertEqual(result["model"], "virtual-time-only")
        self.assertEqual(result["summary"]["offered"], 8)
        self.assertEqual(result["summary"]["terminal"], {"put_ack": 4, "get_hit": 4})
        self.assertEqual(result["summary"]["acked_put_bytes"], 12)
        self.assertEqual(result["summary"]["hit_get_bytes"], 12)
        self.assertEqual(result["summary"]["started"], 8)
        self.assertEqual(result["summary"]["completed"], 8)
        self.assertEqual(result["summary"]["late_completions"], 0)
        self.assertEqual([r["scheduled_ns"] for r in result["requests"]],
                         list(range(0, 80, 10)))
        for row in result["requests"]:
            self.assertEqual(row["started_ns"], row["scheduled_ns"])
            self.assertEqual(row["latency_ns"], 1)
            self.assertEqual(row["queue_wait_ns"], 0)
            self.assertEqual(row["service_ns"], 1)

    def test_fifo_queue_delay_is_in_end_to_end_latency(self):
        result = self.run_model(service_ns=15, workers=1, queue_capacity=8,
                                timeout_ns=1000)
        self.assertTrue(result["passed"])
        row = result["requests"][2]
        self.assertEqual((row["scheduled_ns"], row["started_ns"],
                          row["completed_ns"]), (20, 30, 45))
        self.assertEqual(row["queue_wait_ns"], 10)
        self.assertEqual(row["service_ns"], 15)
        self.assertEqual(row["latency_ns"], 25)
        self.assertEqual(result["summary"]["peak_active"], 1)

    def test_queue_full_requests_remain_in_denominator_without_start(self):
        result = self.run_model(service_ns=50, workers=1, queue_capacity=1,
                                timeout_ns=1000)
        rows = result["requests"]
        self.assertFalse(result["passed"])
        self.assertEqual(rows[2]["terminal"], "queue_full")
        self.assertNotIn("started_ns", rows[2])
        self.assertEqual(rows[2]["terminal_ns"], 20)
        self.assertEqual([r["request_id"] for r in rows], list(range(8)))
        self.assertEqual(sum(result["summary"]["terminal"].values()), 8)
        self.assertEqual(result["summary"]["offered"], 8)
        self.assertEqual(result["summary"]["peak_queue"], 1)

    def test_active_timeout_keeps_worker_and_late_put_is_not_ack(self):
        document = trace.generate(objects=1, rounds=1, length=3, interval_ns=10)
        backend = replay.FakeBackend(1, durations={0: 30})
        result = replay.run(document, backend, workers=1, queue_capacity=0,
                            timeout_ns=15)
        first, second, third, fourth = result["requests"]
        self.assertEqual(first["terminal"], "timeout")
        self.assertEqual((first["terminal_ns"], first["completed_ns"]), (15, 30))
        self.assertEqual(first["backend_outcome"], "put_ack")
        self.assertTrue(first["late_completion"])
        self.assertEqual(second["terminal"], "queue_full")
        self.assertEqual(third["terminal"], "queue_full")
        self.assertEqual(fourth["terminal"], "get_hit")
        self.assertEqual(result["summary"]["acked_put_bytes"], 0)
        self.assertEqual(result["summary"]["late_completions"], 1)
        self.assertEqual(result["summary"]["peak_active"], 1)

    def test_waiting_deadline_expires_without_backend_invocation(self):
        result = self.run_model(service_ns=50, workers=1, queue_capacity=8,
                                timeout_ns=15)
        row = result["requests"][1]
        self.assertEqual(row["terminal"], "timeout")
        self.assertEqual(row["terminal_ns"], 25)
        self.assertEqual(row["latency_ns"], 15)
        self.assertNotIn("started_ns", row)
        self.assertNotIn("completed_ns", row)

    def test_completion_at_deadline_wins_but_expired_queue_never_starts(self):
        result = self.run_model(service_ns=15, workers=1, queue_capacity=8,
                                timeout_ns=15)
        self.assertEqual(result["requests"][0]["terminal"], "put_ack")
        # Request 2 deadline=35; worker frees exactly then, so it must not start.
        backend = replay.FakeBackend(1, durations={0: 35})
        result = replay.run(self.document(), backend, workers=1,
                            queue_capacity=8, timeout_ns=15)
        self.assertEqual(result["requests"][2]["terminal_ns"], 35)
        self.assertNotIn("started_ns", result["requests"][2])

    def test_multiple_workers_bounded_and_out_of_order_completion(self):
        backend = replay.FakeBackend(1, durations={0: 40, 1: 5})
        result = replay.run(self.document(), backend, workers=2,
                            queue_capacity=8, timeout_ns=1000)
        self.assertEqual(result["summary"]["peak_active"], 2)
        self.assertEqual(result["requests"][1]["completed_ns"], 15)
        self.assertEqual(result["requests"][0]["completed_ns"], 40)
        # GET 0 completes before delayed PUT 0: model miss is legitimate.
        self.assertEqual(result["requests"][2]["terminal"], "get_miss")
        self.assertTrue(result["passed"])

    def test_miss_corruption_error_and_late_corruption_preserved(self):
        for field, outcome in (("miss_ids", "get_miss"),
                               ("corrupt_ids", "wrong_bytes"),
                               ("error_ids", "backend_error")):
            with self.subTest(field=field):
                backend = replay.FakeBackend(1, **{field: {2}})
                result = replay.run(self.document(), backend)
                self.assertEqual(result["requests"][2]["terminal"], outcome)
                self.assertEqual(result["passed"], outcome == "get_miss")
                self.assertEqual(sum(result["summary"]["terminal"].values()), 8)
        backend = replay.FakeBackend(1, durations={2: 30}, corrupt_ids={2})
        result = replay.run(self.document(), backend, timeout_ns=15, workers=2)
        row = result["requests"][2]
        self.assertEqual(row["terminal"], "timeout")
        self.assertEqual(row["backend_outcome"], "wrong_bytes")
        self.assertEqual(result["summary"]["wrong_bytes"], 1)
        self.assertFalse(result["passed"])

    def test_rejects_invalid_trace_config_and_backend_reuse(self):
        for name, value in (("workers", 0), ("workers", True), ("workers", 65),
                            ("queue_capacity", -1), ("queue_capacity", 1025),
                            ("timeout_ns", 0), ("timeout_ns", 1.5)):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.run_model(**{name: value})
        for value in (0, -1, True, 1.5, 10**12 + 1):
            with self.subTest(service=value), self.assertRaises(ValueError):
                replay.FakeBackend(value)
        with self.assertRaises(ValueError):
            replay.FakeBackend(1, durations={0: 0})
        document = self.document()
        document["requests"].pop()
        with self.assertRaises(ValueError):
            replay.run(document, replay.FakeBackend())
        backend = replay.FakeBackend()
        replay.run(self.document(), backend)
        with self.assertRaises(ValueError):
            replay.run(self.document(), backend)

    def test_queue_timeout_grid_conserves_every_request_and_drains(self):
        for workers in (1, 2, 4):
            for capacity in (0, 1, 8):
                for deadline in (1, 10, 15, 100):
                    with self.subTest(workers=workers, capacity=capacity, deadline=deadline):
                        result = self.run_model(service_ns=15, workers=workers,
                                                queue_capacity=capacity,
                                                timeout_ns=deadline)
                        summary = result["summary"]
                        self.assertEqual(summary["offered"], 8)
                        self.assertEqual(sum(summary["terminal"].values()), 8)
                        self.assertEqual(summary["started"], summary["completed"])
                        self.assertLessEqual(summary["peak_active"], workers)
                        self.assertLessEqual(summary["peak_queue"], capacity)
                        for row in result["requests"]:
                            self.assertGreaterEqual(row["terminal_ns"], row["scheduled_ns"])
                            self.assertLessEqual(row["terminal_ns"], row["deadline_ns"])
                            if "started_ns" in row:
                                self.assertLess(row["started_ns"], row["deadline_ns"])
                                self.assertEqual(row["service_ns"], 15)

    def test_backend_error_put_does_not_create_a_resident(self):
        result = replay.run(self.document(), replay.FakeBackend(error_ids={0}))
        self.assertEqual(result["requests"][0]["terminal"], "backend_error")
        self.assertEqual(result["requests"][2]["terminal"], "get_miss")
        self.assertEqual(result["summary"]["acked_put_bytes"], 9)
        self.assertFalse(result["passed"])

    def test_cli_success_and_bad_input_has_no_result(self):
        script = DIRECTORY / "eviction_replay.py"
        with tempfile.TemporaryDirectory() as directory:
            source, output = [Path(directory) / n for n in ("trace.json", "result.json")]
            command = [sys.executable, "-I", str(script), str(source), str(output)]
            source.write_bytes(trace.encode(self.document()))
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(json.loads(output.read_bytes())["passed"])
            bad_output = Path(directory) / "bad-result.json"
            source.write_text("{}")
            result = subprocess.run(command[:-1] + [str(bad_output)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertFalse(bad_output.exists())
            self.assertNotIn("Traceback", result.stderr)

    def test_cli_no_overwrite_and_failed_report_is_retained(self):
        script = DIRECTORY / "eviction_replay.py"
        with tempfile.TemporaryDirectory() as directory:
            source, output = [Path(directory) / n for n in ("trace.json", "result.json")]
            source.write_bytes(trace.encode(self.document()))
            command = [sys.executable, "-I", str(script), str(source), str(output),
                       "--service-ns", "50", "--workers", "1",
                       "--queue-capacity", "0", "--timeout-ns", "15"]
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1, result.stderr)
            data = output.read_bytes()
            self.assertFalse(json.loads(data)["passed"])
            self.assertEqual(json.loads(data)["summary"]["offered"], 8)
            again = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(again.returncode, 0)
            self.assertEqual(output.read_bytes(), data)
            self.assertNotIn("Traceback", again.stderr)


if __name__ == "__main__":
    unittest.main()
