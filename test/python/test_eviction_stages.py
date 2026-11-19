"""Drained fake phase barriers and exact initial payload-state regressions."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from unittest import mock
import unittest

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = ROOT / "deploy/validation"
sys.path.insert(0, str(DIRECTORY))
import eviction_trace as trace
import eviction_realtime as realtime
import eviction_guard as guard


class StagesTest(unittest.TestCase):
    def document(self):
        return trace.generate(objects=1, rounds=1, length=3, interval_ns=80000000)

    def config(self):
        return dict(workers=1, queue_capacity=16, timeout_ns=1000000000,
                    service_ns=1, staged=True)

    def test_all_success_phases_finalize_and_keep_source_plan_immutable(self):
        document = self.document()
        before = trace.encode(document)
        result = realtime.run(document, realtime.SleepingFakeBackend(), staged=True)
        self.assertTrue(result["passed"])
        self.assertEqual([p["status"] for p in result["phases"]], ["passed"] * 3)
        self.assertEqual(trace.encode(document), before)
        self.assertEqual([r["scheduled_ns"] for r in result["requests"]],
                         [0, 80000000, 160000000, 240000000])
        self.assertEqual(result["summary"]["planned"], 4)
        self.assertEqual(result["summary"]["offered"], 4)
        self.assertEqual(result["phases"][2]["summary"]["offered"], 2)
        self.assertEqual(result["phases"][2]["summary"]["acked_put_bytes"], 3)
        self.assertEqual(result["phases"][0]["start_state"]["residents"], 0)

    def test_init_physical_drain_precedes_warm_and_measure_origins(self):
        result = realtime.run(self.document(), realtime.SleepingFakeBackend(
            durations={0: 150000000}), workers=2, staged=True)
        self.assertTrue(result["passed"])
        phases = result["phases"]
        rows = result["requests"]
        self.assertLessEqual(rows[0]["completed_ns"], phases[0]["finished_ns"])
        self.assertLessEqual(phases[0]["finished_ns"], phases[1]["origin_ns"])
        self.assertLessEqual(phases[1]["finished_ns"], phases[2]["origin_ns"])
        self.assertEqual(rows[1]["terminal"], "get_hit")
        self.assertGreater(rows[1]["effective_scheduled_ns"], rows[1]["scheduled_ns"])
        for row in rows:
            self.assertEqual(row["deadline_ns"], row["effective_scheduled_ns"] + 1000000000)

    def test_failed_init_even_with_late_commit_blocks_unoffered_future_rows(self):
        result = realtime.run(self.document(), realtime.SleepingFakeBackend(
            durations={0: 150000000}), timeout_ns=50000000, staged=True)
        self.assertFalse(result["passed"])
        self.assertEqual(result["requests"][0]["terminal"], "timeout")
        self.assertEqual(result["requests"][0]["backend_outcome"], "put_ack")
        self.assertGreaterEqual(result["phases"][0]["finished_ns"], result["requests"][0]["completed_ns"])
        self.assertEqual([p["status"] for p in result["phases"]], ["failed", "blocked", "blocked"])
        self.assertEqual(result["summary"]["planned"], 4)
        self.assertEqual(result["summary"]["offered"], 1)
        for row in result["requests"][1:]:
            self.assertEqual(row["terminal"], "phase_blocked")
            for field in ("arrival_observed_ns", "effective_scheduled_ns", "deadline_ns",
                          "started_ns", "terminal_ns", "latency_ns"):
                self.assertNotIn(field, row)

    def test_extra_missing_wrong_initial_payload_fail_despite_put_ack(self):
        for corruption in ("extra", "missing", "wrong"):
            class Corrupts(realtime.SleepingFakeBackend):
                def complete(self, row):
                    outcome = super().complete(row)
                    if row["request_id"] == 0:
                        key = (row["namespace_hex"], row["key_hex"])
                        if corruption == "extra":
                            self.store[("00", "00")] = b"x"
                        elif corruption == "missing":
                            self.store.pop(key)
                        else:
                            self.store[key] = b"bad"
                    return outcome
            with self.subTest(corruption=corruption):
                result = realtime.run(self.document(), Corrupts(), staged=True)
                self.assertEqual(result["requests"][0]["terminal"], "put_ack")
                self.assertEqual(result["phases"][0]["status"], "failed")
                self.assertEqual(result["phases"][2]["status"], "blocked")
                self.assertFalse(result["passed"])

    def test_warm_miss_is_not_success_and_measure_is_not_offered(self):
        class Misses(realtime.SleepingFakeBackend):
            def complete(self, row):
                return "get_miss" if row["phase"] == "warmup" else super().complete(row)
        result = realtime.run(self.document(), Misses(), staged=True)
        self.assertEqual([p["status"] for p in result["phases"]], ["passed", "failed", "blocked"])
        self.assertEqual(result["summary"]["offered"], 2)
        self.assertEqual(result["requests"][1]["terminal"], "get_miss")
        self.assertFalse(result["passed"])
        guard._verify(self.document(), result, self.config())

    def test_warm_snapshot_must_still_match_even_after_reported_hit(self):
        class Corrupts(realtime.SleepingFakeBackend):
            def complete(self, row):
                outcome = super().complete(row)
                if row["phase"] == "warmup":
                    self.store[(row["namespace_hex"], row["key_hex"])] = b"bad"
                return outcome
        result = realtime.run(self.document(), Corrupts(), staged=True)
        self.assertEqual(result["requests"][1]["terminal"], "get_hit")
        self.assertEqual(result["phases"][1]["status"], "failed")
        self.assertEqual(result["phases"][2]["status"], "blocked")
        self.assertFalse(result["passed"])

    def test_state_hashing_finishes_before_next_phase_schedule_origin(self):
        snapshot = realtime._snapshot
        def slow_snapshot(store):
            if store:
                threading.Event().wait(.1)
            return snapshot(store)
        with mock.patch.object(realtime, "_snapshot", side_effect=slow_snapshot):
            result = realtime.run(self.document(), realtime.SleepingFakeBackend(), staged=True)
        self.assertTrue(result["passed"])
        self.assertEqual([p["status"] for p in result["phases"]], ["passed"] * 3)

    def test_measure_miss_remains_valid_and_preparation_counts_are_separate(self):
        result = realtime.run(self.document(), realtime.SleepingFakeBackend(miss_ids={3}), staged=True)
        self.assertTrue(result["passed"])
        self.assertEqual(result["phases"][1]["summary"]["terminal"], {"get_hit": 1})
        self.assertEqual(result["phases"][2]["summary"]["terminal"], {"put_ack": 1, "get_miss": 1})

    def test_dirty_or_reused_backend_and_nonboolean_mode_rejected(self):
        backend = realtime.SleepingFakeBackend()
        backend.store[("00", "00")] = b"x"
        with self.assertRaises(ValueError):
            realtime.run(self.document(), backend, staged=True)
        self.assertFalse(backend.used)
        with self.assertRaises(ValueError):
            realtime.run(self.document(), realtime.SleepingFakeBackend(), staged=1)
        backend = realtime.SleepingFakeBackend()
        realtime.run(self.document(), backend, staged=True)
        with self.assertRaises(ValueError):
            realtime.run(self.document(), backend, staged=True)

    def test_guard_rejects_forged_phase_origin_snapshot_status_or_counts(self):
        baseline = realtime.run(self.document(), realtime.SleepingFakeBackend(), staged=True)
        guard._verify(self.document(), baseline, self.config())
        mutations = [lambda r: r["phases"][1].update(origin_ns=0),
                     lambda r: r["phases"][0]["end_state"].update(sha256="0" * 64),
                     lambda r: r["phases"][1].update(status="failed"),
                     lambda r: r["phases"][2]["summary"].update(offered=0),
                     lambda r: r["summary"].update(offered=0),
                     lambda r: r["requests"][1].update(effective_scheduled_ns=0),
                     lambda r: r.update(schedule_mode="flat")]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                result = copy.deepcopy(baseline)
                mutate(result)
                with self.assertRaises(ValueError):
                    guard._verify(self.document(), result, self.config())

    def test_guard_rejects_missing_arrival_even_with_reconciled_offered_counts(self):
        result = realtime.run(self.document(), realtime.SleepingFakeBackend(), staged=True)
        result["requests"][0].pop("arrival_observed_ns")
        result["summary"]["offered"] = 3
        result["phases"][0]["summary"]["offered"] = 0
        with self.assertRaises(ValueError):
            guard._verify(self.document(), result, self.config())

    def test_blocked_phase_cannot_claim_offered_requests(self):
        class Misses(realtime.SleepingFakeBackend):
            def complete(self, row):
                return "get_miss" if row["phase"] == "warmup" else super().complete(row)
        baseline = realtime.run(self.document(), Misses(), staged=True)
        mutations = [lambda r: r["phases"][2].update(summary={"offered": 2}),
                     lambda r: r["requests"][2].update(backend_outcome="get_hit"),
                     lambda r: r["requests"][2].update(dispatch_lag_ns=0),
                     lambda r: r["requests"][2].update(worker_expired_ns=0),
                     lambda r: r["requests"][2].update(late_completion=False)]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                result = copy.deepcopy(baseline)
                mutate(result)
                with self.assertRaises(ValueError):
                    guard._verify(self.document(), result, self.config())

    def test_guard_cli_staged_end_to_end_and_default_flat_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.json"
            source.write_bytes(trace.encode(self.document()))
            command = [sys.executable, "-I", str(DIRECTORY / "eviction_guard.py"),
                       str(source), str(Path(directory) / "run"), "--staged"]
            run = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(run.returncode, 0, run.stderr)
            report = json.loads((Path(directory) / "run/report.json").read_text())
            self.assertTrue(report["passed"])
            self.assertEqual(report["offered_requests"], 4)
            failed_command = command[:-2] + [str(Path(directory) / "failed"), "--staged", "--timeout-ns", "1"]
            failed = subprocess.run(failed_command, capture_output=True, text=True, timeout=10)
            self.assertEqual(failed.returncode, 1, failed.stderr)
            report = json.loads((Path(directory) / "failed/report.json").read_text())
            self.assertEqual(report["status"], "workload_failed")
            self.assertEqual(report["offered_requests"], 1)
            self.assertEqual(report["completed_requests"], 0)
            child = json.loads((Path(directory) / "failed/child-result.json").read_text())
            self.assertEqual(child["phases"][2]["summary"]["offered"], 0)
        flat = realtime.run(self.document(), realtime.SleepingFakeBackend())
        self.assertTrue(flat["passed"])
        self.assertNotIn("phases", flat)
        self.assertNotIn("staged", flat["config"])


if __name__ == "__main__":
    unittest.main()
