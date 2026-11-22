"""Backend identity/state seams; controlled calls, no native service or GPU."""

import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import threading
import unittest

DIRECTORY = Path(__file__).resolve().parents[2] / "deploy/validation"
sys.path.insert(0, str(DIRECTORY))
import eviction_guard as guard
import eviction_realtime as realtime
import eviction_trace as trace


class ControlledBackend(realtime.SleepingFakeBackend):
    """Stand-in external calls with a logical ACK shadow, not actual residents."""

    model = "controlled-external-backend-test-only"
    state_count_name = "acknowledged_objects"

    def __init__(self):
        super().__init__()
        self.snapshots = 0

    def wait(self, row):
        pass  # The test adapter performs no simulated sleep or real I/O.

    def snapshot(self):
        self.snapshots += 1
        return super().snapshot()

    def report_config(self, scheduler_config):
        return dict(scheduler_config, backend="controlled", state_scope="logical-ACK-shadow")


class SchedulerBackendTest(unittest.TestCase):
    def document(self):
        return trace.generate(objects=1, rounds=1, length=3, interval_ns=80000000)

    def scheduler_config(self, *, workers=1):
        return dict(workers=workers, queue_capacity=16, timeout_ns=1000000000, staged=True)

    def verify(self, document, result, *, workers=1, extra_terminals=()):
        config = self.scheduler_config(workers=workers)
        return guard._verify(document, result, config,
                             expected_model=ControlledBackend.model,
                             expected_config=ControlledBackend().report_config(config),
                             state_count_name=ControlledBackend.state_count_name,
                             extra_terminals=extra_terminals)

    def test_default_fake_result_shape_and_config_are_unchanged(self):
        document = self.document()
        result = realtime.run(document, realtime.SleepingFakeBackend(), staged=True)
        self.assertEqual(set(result), {"model", "trace_sha256", "config", "passed", "summary",
                                       "requests", "schedule_mode", "phases"})
        self.assertEqual(result["model"], "monotonic-threaded-fake-only")
        config = dict(self.scheduler_config(), service_ns=1)
        expected = dict(config, durations={}, error_ids=[], miss_ids=[], corrupt_ids=[])
        self.assertEqual(trace.encode(result["config"]), trace.encode(expected))
        self.assertEqual(set(result["phases"][0]["start_state"]),
                         {"residents", "payload_bytes", "sha256"})
        self.assertEqual(result["summary"]["terminal"], {"put_ack": 2, "get_hit": 2})
        self.assertTrue(result["passed"])
        guard._verify(document, result, config)

    def test_custom_model_config_and_shadow_state_are_not_fake_residents(self):
        document = self.document()
        original = trace.encode(document)
        backend = ControlledBackend()
        result = realtime.run(document, backend, staged=True)
        self.assertTrue(result["passed"])
        self.assertEqual(result["model"], ControlledBackend.model)
        self.assertEqual(backend.snapshots, 6)
        for phase in result["phases"]:
            for name in ("start_state", "end_state"):
                self.assertIn("acknowledged_objects", phase[name])
                self.assertNotIn("residents", phase[name])
        self.assertEqual(result["phases"][0]["start_state"]["acknowledged_objects"], 0)
        self.assertEqual(result["phases"][1]["start_state"]["acknowledged_objects"], 1)
        self.assertEqual(result["config"]["state_scope"], "logical-ACK-shadow")
        self.assertNotIn("service_ns", result["config"])
        self.verify(document, result)
        self.assertEqual(trace.encode(document), original)
        with self.assertRaisesRegex(ValueError, "model"):
            guard._verify(document, result, self.scheduler_config())

    def test_foreign_calls_overlap_outside_condition_and_drain_before_next_phase(self):
        class Gated(ControlledBackend):
            def __init__(self):
                super().__init__()
                self.entered = {index: threading.Event() for index in (0, 1)}
                self.release = threading.Event()

            def wait(self, row):
                if row["request_id"] in self.entered:
                    self.entered[row["request_id"]].set()
                    if not self.release.wait(3):
                        raise RuntimeError("test controller did not release foreign call")

        document = trace.generate(objects=2, rounds=1, length=3, interval_ns=80000000)
        backend = Gated()
        with ThreadPoolExecutor(max_workers=1) as controller:
            future = controller.submit(realtime.run, document, backend, workers=2, staged=True)
            try:
                self.assertTrue(backend.entered[0].wait(2))
                self.assertTrue(backend.entered[1].wait(2))
            finally:
                backend.release.set()
            result = future.result(timeout=5)
        self.assertTrue(result["passed"])
        first, second = result["requests"][:2]
        self.assertLess(first["started_ns"], second["completed_ns"])
        self.assertLess(second["started_ns"], first["completed_ns"])
        self.assertEqual(result["summary"]["peak_running"], 2)
        self.assertGreaterEqual(result["phases"][1]["origin_ns"],
                                max(first["completed_ns"], second["completed_ns"]))
        self.verify(document, result, workers=2)

    def test_forged_model_config_count_scope_or_fingerprint_is_rejected(self):
        document = self.document()
        baseline = realtime.run(document, ControlledBackend(), staged=True)
        mutations = [lambda r: r.update(model="monotonic-threaded-fake-only"),
                     lambda r: r["config"].update(backend="forged"),
                     lambda r: r["phases"][1]["start_state"].update(acknowledged_objects=0),
                     lambda r: r["phases"][0]["end_state"].update(sha256="0" * 64),
                     lambda r: r["phases"][0]["start_state"].update(residents=0)]
        for mutate in mutations:
            result = copy.deepcopy(baseline)
            mutate(result)
            with self.assertRaises(ValueError):
                self.verify(document, result)

    def test_extra_unresolved_terminal_never_becomes_success_or_miss(self):
        class Unresolved(ControlledBackend):
            def complete(self, row):
                return "get_unresolved" if row["op"] == "get" else super().complete(row)

        document = self.document()
        result = realtime.run(document, Unresolved(), staged=True)
        self.assertFalse(result["passed"])
        self.assertEqual(result["requests"][1]["terminal"], "get_unresolved")
        self.assertEqual(result["summary"]["terminal"],
                         {"put_ack": 1, "get_unresolved": 1, "phase_blocked": 2})
        self.assertEqual(result["summary"]["offered"], 2)
        self.verify(document, result, extra_terminals=("get_unresolved",))
        with self.assertRaisesRegex(ValueError, "terminal"):
            self.verify(document, result)
        result["passed"] = True
        with self.assertRaises(ValueError):
            self.verify(document, result, extra_terminals=("get_unresolved",))

    def test_invalid_backend_identity_rejected_before_use(self):
        for field, value in (("model", ""), ("model", False),
                             ("state_count_name", ""), ("state_count_name", "sha256"),
                             ("state_count_name", "payload_bytes"), ("state_count_name", False)):
            with self.subTest(field=field, value=value):
                backend = ControlledBackend()
                setattr(backend, field, value)
                with self.assertRaises(ValueError):
                    realtime.run(self.document(), backend, staged=True)
                self.assertFalse(backend.used)

    def test_invalid_verifier_options_are_rejected(self):
        document = self.document()
        result = realtime.run(document, realtime.SleepingFakeBackend(), staged=True)
        config = dict(self.scheduler_config(), service_ns=1)
        for options in ({"expected_model": False}, {"expected_model": ""},
                        {"expected_config": []}, {"state_count_name": "sha256"},
                        {"state_count_name": False}, {"extra_terminals": "get_unresolved"},
                        {"extra_terminals": [False]}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                guard._verify(document, result, config, **options)

    def test_snapshot_count_field_cannot_overwrite_hash_or_bytes(self):
        for name in (False, "", "payload_bytes", "sha256"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                realtime._snapshot({}, name)
        fake = realtime._snapshot({})
        logical = realtime._snapshot({}, "acknowledged_objects")
        self.assertEqual(fake.pop("residents"), logical.pop("acknowledged_objects"))
        self.assertEqual(fake, logical)


if __name__ == "__main__":
    unittest.main()
