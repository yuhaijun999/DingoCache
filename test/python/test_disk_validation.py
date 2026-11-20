"""Hardware-free regression coverage for the external disk diagnostic driver."""

import ctypes as ct
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "integration/common/src"))
SPEC = importlib.util.spec_from_file_location(
    "disk_scenarios", ROOT / "deploy/validation/disk_scenarios.py")
driver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(driver)


class MemoryClient:
    def __init__(self, storage, members, namespace):
        self.storage = storage.setdefault(namespace, {})
        self.transport = "fake-only"

    def close(self):
        pass

    def put(self, keys, values):
        for key, value in zip(keys, values):
            self.storage.setdefault(key, value)

    def read(self, keys, values, *, allow_miss=False):
        return sum(driver.check_read(value, int(key in self.storage),
                                     self.storage.get(key),
                                     len(self.storage.get(key, b"")),
                                     allow_miss=allow_miss)
                   for key, value in zip(keys, values))

    def expect_missing(self, key):
        if key in self.storage:
            raise RuntimeError("expected missing key")

    def remove(self, key):
        del self.storage[key]


class DiskValidationTest(unittest.TestCase):
    def args(self, output, scenario="roundtrip", extra=()):
        return driver.parse_args(["--lib", "/fake/libdfkv.so", "--members",
                                  "n1=127.0.0.1:1", "--output", str(output),
                                  "--scenario", scenario, *extra])

    def test_generator_reproducible_and_domain_separated(self):
        baseline = driver.payload(1, b"ns\x00A", b"key\x00", 0, 128)
        self.assertEqual(baseline, driver.payload(1, b"ns\x00A", b"key\x00", 0, 128))
        for fields in ((2, b"ns\x00A", b"key\x00", 0, 128),
                       (1, b"ns\x00B", b"key\x00", 0, 128),
                       (1, b"ns\x00A", b"key2", 0, 128),
                       (1, b"ns\x00A", b"key\x00", 1, 128),
                       (1, b"ns\x00A", b"key\x00", 0, 129)):
            self.assertNotEqual(baseline, driver.payload(*fields)[:128])
        self.assertNotEqual(baseline[:64], baseline[64:])
        self.assertNotEqual(driver.key_for(1, 0), driver.key_for(2, 0))
        self.assertNotEqual(driver.key_for(1, 0), driver.key_for(1, 1))
        for length in driver.LENGTHS:
            self.assertEqual(len(driver.payload(1, b"ns", b"k", 0, length)), length)

    def test_oracle_rejects_corruption_truncation_and_unexpected_misses(self):
        self.assertTrue(driver.check_read(b"abc", 1, b"abc", 3))
        for hit, actual, length in ((1, b"abd", 3), (1, b"ab", 2),
                                    (1, b"abc", 2), (0, b"", 0), (2, b"abc", 3)):
            with self.subTest(hit=hit, actual=actual, length=length):
                with self.assertRaises(RuntimeError):
                    driver.check_read(b"abc", hit, actual, length)
        self.assertFalse(driver.check_read(b"abc", 0, b"", 0, allow_miss=True))
        with self.assertRaises(RuntimeError):
            driver.check_read(b"abc", 1, b"abd", 3, allow_miss=True)

        class CorruptLib:
            def dfkv_batch_get_auto(fake, handle, keys, lengths, data, caps,
                                    count, hits, sizes):
                ct.memmove(data[0], b"abd", 3)
                hits[0], sizes[0] = 1, 3
                return 0

        client = driver.Client.__new__(driver.Client)
        client.handle, client.lib = 1, CorruptLib()
        with self.assertRaises(RuntimeError) as raised:
            client.read([b"key\x00"], [b"abc"])
        message = str(raised.exception)
        for context in ("wrong bytes/length", "key_hex=6b657900", "batch_index=0",
                        "expected_length=3",
                        "expected_sha256=" + hashlib.sha256(b"abc").hexdigest(),
                        "actual_sha256=" + hashlib.sha256(b"abd").hexdigest()):
            self.assertIn(context, message)

    def test_trace_precedes_client_and_matches_cli_and_result(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "case"
            args = self.args(output, extra=("--seed", "0x123", "--readers", "8"))
            storage = {}

            def load(path):
                self.assertEqual(path, args.lib)
                self.assertTrue((output / "trace.json").exists())
                return storage

            result = driver.run(args, library_loader=load, client_factory=MemoryClient)
            trace = json.loads((output / "trace.json").read_text())
            config = trace["config"]
            digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
            self.assertEqual(result["config_sha256"], digest)
            self.assertEqual(trace["config_sha256"], digest)
            self.assertEqual(config["seed"], 0x123)
            self.assertEqual(config["count"], 9)
            self.assertEqual(config["lengths"], list(driver.LENGTHS))
            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["concurrent_reader_hits"], 72)
            self.assertEqual(result["transport_mode"], ["fake-only"] * 2)
            self.assertEqual(set(storage), set(driver.NAMESPACES))
            self.assertEqual(set(storage[driver.NAMESPACES[0]]),
                             set(storage[driver.NAMESPACES[1]]))
            with self.assertRaises(FileExistsError):
                driver.run(args, library_loader=load, client_factory=MemoryClient)

    def test_write_verify_reuse_dataset_and_fail_missing(self):
        with tempfile.TemporaryDirectory() as temp:
            storage = {}
            for scenario in ("write", "verify"):
                result = driver.run(self.args(Path(temp) / scenario, scenario),
                                    library_loader=lambda _: storage,
                                    client_factory=MemoryClient)
                self.assertEqual(result["status"], "passed")
            storage[driver.NAMESPACES[0]].clear()
            output = Path(temp) / "missing"
            with self.assertRaisesRegex(RuntimeError, "below-capacity miss"):
                driver.run(self.args(output, "verify"), library_loader=lambda _: storage,
                           client_factory=MemoryClient)
            self.assertEqual(json.loads((output / "result.json").read_text())["status"], "failed")

    def test_pressure_explicitly_allows_partial_eviction_not_total_failure(self):
        class EvictingClient(MemoryClient):
            def put(self, keys, values):
                super().put(keys, values)
                self.storage.pop(keys[0], None)

        with tempfile.TemporaryDirectory() as temp:
            args = self.args(Path(temp) / "pressure", "pressure", ("--count", "3"))
            result = driver.run(args, library_loader=lambda _: {}, client_factory=EvictingClient)
            self.assertEqual((result["hits"], result["misses"]), (4, 2))
            self.assertEqual(self.args(Path(temp) / "default", "pressure").count, 26000)
            args.count = 1
            args.output = Path(temp) / "unavailable"
            with self.assertRaisesRegex(RuntimeError, "no successful reads"):
                driver.run(args, library_loader=lambda _: {}, client_factory=EvictingClient)

    def test_quick_pressure_prefills_before_hot_reads_and_checks_evicted_bytes(self):
        events = []

        class EvictHotClient(MemoryClient):
            def put(self, keys, values):
                super().put(keys, values)
                events.append("put")
                if int.from_bytes(keys[0][-8:], "big") >= driver.QUICK_PREFILL:
                    for index in range(driver.QUICK_HOT):
                        self.storage.pop(driver.key_for(0xD1A60001, index), None)

            def read(self, keys, values, *, allow_miss=False):
                events.append("read")
                return super().read(keys, values, allow_miss=allow_miss)

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 16), \
                mock.patch.object(driver, "QUICK_HOT", 2):
            args = self.args(Path(temp) / "quick", "quick-pressure", ("--count", "20"))
            result = driver.run(args, library_loader=lambda _: {},
                                client_factory=EvictHotClient)
            self.assertEqual(events[:3], ["put", "put", "read"])
            self.assertEqual((result["hits"], result["misses"]), (36, 4))
            self.assertEqual((result["hot_hits"], result["hot_misses"]), (8, 4))
            self.assertEqual(result["hot_reads"], 12)
            trace = json.loads((args.output / "trace.json").read_text())["config"]
            self.assertEqual((trace["prefill"], trace["hot"], trace["lengths"]),
                             (16, 2, [4096]))

    def test_quick_pressure_rejects_warmup_miss_and_hot_corruption(self):
        class MissingWarmClient(MemoryClient):
            def put(self, keys, values):
                super().put(keys, values)
                self.storage.pop(keys[0], None)

        class CorruptHotClient(MemoryClient):
            def read(self, keys, values, *, allow_miss=False):
                if allow_miss:
                    self.storage[keys[0]] = b"x" * len(values[0])
                return super().read(keys, values, allow_miss=allow_miss)

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 16), \
                mock.patch.object(driver, "QUICK_HOT", 2):
            for client, message in ((MissingWarmClient, "below-capacity miss"),
                                    (CorruptHotClient, "wrong bytes/length")):
                with self.subTest(client=client.__name__):
                    args = self.args(Path(temp) / client.__name__, "quick-pressure",
                                     ("--count", "20"))
                    with self.assertRaisesRegex(RuntimeError, message):
                        driver.run(args, library_loader=lambda _: {}, client_factory=client)
                    result = json.loads((args.output / "result.json").read_text())
                    self.assertEqual(result["status"], "failed")
                    self.assertEqual(result["wrong_bytes"], int(client is CorruptHotClient))

    def test_quick_pressure_arguments(self):
        self.assertEqual(self.args(Path("unused"), "quick-pressure").count, 6452)
        for extra in (("--count", "4608"), ("--readers", "8")):
            with self.subTest(extra=extra), self.assertRaises(SystemExit):
                self.args(Path("unused"), "quick-pressure", extra)

    def test_quick_pressure_waits_after_warmup_and_captures_phases(self):
        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 16), \
                mock.patch.object(driver, "QUICK_HOT", 2):
            args = self.args(Path(temp) / "aged", "quick-pressure",
                             ("--count", "20", "--pressure-delay-sec", "185",
                              "--metrics-url", "http://127.0.0.1:12010/metrics"))
            def sleep(seconds):
                self.assertEqual(seconds, 185)
                phases = json.loads((args.output / "phases.json").read_text())
                self.assertEqual([row["name"] for row in phases], ["prefill", "warm"])
                self.assertEqual(phases[-1]["hot_hits"], 8)

            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = b"sample 1\n"
            with mock.patch.object(driver.time, "sleep", side_effect=sleep) as wait, \
                    mock.patch.object(driver.urllib.request, "urlopen", return_value=response) as fetch:
                result = driver.run(args, library_loader=lambda _: {}, client_factory=MemoryClient)
            wait.assert_called_once_with(185)
            self.assertEqual(fetch.call_count, 5)
            expected = ["prefill", "warm", "before-pressure", "after-pressure", "after-scan"]
            self.assertEqual([row["name"] for row in result["phases"]], expected)
            for name in expected:
                self.assertEqual((args.output / ("phase-" + name + ".prom")).read_text(), "sample 1\n")
            trace = json.loads((args.output / "trace.json").read_text())
            self.assertEqual(trace["config"]["pressure_delay_sec"], 185)

    def test_phase_capture_failure_is_failed_not_silently_skipped(self):
        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 16), \
                mock.patch.object(driver, "QUICK_HOT", 2):
            args = self.args(Path(temp) / "bad-metrics", "quick-pressure",
                             ("--count", "20", "--metrics-url", "http://127.0.0.1:12010/metrics"))
            with mock.patch.object(driver.urllib.request, "urlopen", side_effect=TimeoutError("metrics timed out")):
                with self.assertRaisesRegex(TimeoutError, "metrics timed out"):
                    driver.run(args, library_loader=lambda _: {}, client_factory=MemoryClient)
            self.assertEqual(json.loads((args.output / "result.json").read_text())["status"], "failed")

    def test_pressure_delay_is_bounded_and_quick_only(self):
        for scenario, extra in (("quick-pressure", ("--pressure-delay-sec", "-1")),
                                ("quick-pressure", ("--pressure-delay-sec", "301")),
                                ("pressure", ("--pressure-delay-sec", "1")),
                                ("roundtrip", ("--metrics-url", "http://127.0.0.1/metrics"))):
            with self.subTest(scenario=scenario, extra=extra), self.assertRaises(SystemExit):
                self.args(Path("unused"), scenario, extra)

    def test_sustained_trace_is_paced_and_does_not_catch_up_after_stalls(self):
        class Clock:
            now = 0.0
            def monotonic(self):
                return self.now
            def sleep(self, seconds):
                self.assert_nonnegative(seconds)
                self.now += seconds
            def assert_nonnegative(self, seconds):
                if seconds < 0:
                    raise RuntimeError("negative sleep")

        for stall in (False, True):
            clock = Clock()
            delayed = []
            class SlowClient(MemoryClient):
                def put(self, keys, values):
                    super().put(keys, values)
                    if stall and not delayed and int.from_bytes(keys[0][-8:], "big") >= 16:
                        clock.now += 3
                        delayed.append(True)

            with self.subTest(stall=stall), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "QUICK_PREFILL", 16), \
                    mock.patch.object(driver, "QUICK_HOT", 2), \
                    mock.patch.object(driver, "SUSTAIN_STEPS", 3), \
                    mock.patch.object(driver.time, "monotonic", side_effect=clock.monotonic), \
                    mock.patch.object(driver.time, "sleep", side_effect=clock.sleep):
                args = self.args(Path(temp) / "sustained", "sustained-pressure")
                result = driver.run(args, library_loader=lambda _: {}, client_factory=SlowClient)
                self.assertEqual(args.count, 64)
                self.assertEqual((result["hits"], result["misses"]), (128, 0))
                self.assertEqual((result["hot_hits"], result["hot_misses"], result["hot_reads"]),
                                 (20, 0, 20))
                rows = result["steps"]
                self.assertEqual([row["step"] for row in rows], [0, 1, 2])
                self.assertEqual([row["start_sec"] for row in rows], [0, 3, 4] if stall else [0, 1, 2])
                self.assertTrue(all(row["cold_written"] == 32 for row in rows))
                self.assertEqual([json.loads(line) for line in (args.output / "steps.jsonl").read_text().splitlines()], rows)
                self.assertEqual(result["pressure_schedule_elapsed_sec"], 4 if stall else 2)
                trace = json.loads((args.output / "trace.json").read_text())["config"]
                self.assertEqual((trace["steps"], trace["step_interval_sec"], trace["refill_on_miss"]),
                                 (3, 1, False))

    def test_sustained_full_schedule_captures_window_checkpoints(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"sample 1\n"
        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 16), \
                mock.patch.object(driver, "QUICK_HOT", 2), \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep), \
                mock.patch.object(driver.urllib.request, "urlopen", return_value=response) as fetch:
            args = self.args(Path(temp) / "checkpoints", "sustained-pressure",
                             ("--metrics-url", "http://127.0.0.1:12010/metrics"))
            result = driver.run(args, library_loader=lambda _: {}, client_factory=MemoryClient)
            self.assertEqual(result["pressure_schedule_elapsed_sec"], 199)
            self.assertEqual(result["hot_reads"], 808)
            self.assertEqual(fetch.call_count, 11)
            self.assertEqual([row["name"] for row in result["phases"]],
                             ["prefill", "warm", "before-pressure", "step-040", "step-080",
                              "step-120", "step-160", "step-180", "step-200",
                              "after-pressure", "after-scan"])

    def test_sustained_hot_corruption_fails_and_preserves_partial_trace(self):
        class CorruptClient(MemoryClient):
            def read(self, keys, values, *, allow_miss=False):
                if allow_miss:
                    self.storage[keys[0]] = b"x" * len(values[0])
                return super().read(keys, values, allow_miss=allow_miss)

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 16), \
                mock.patch.object(driver, "QUICK_HOT", 2), \
                mock.patch.object(driver, "SUSTAIN_STEPS", 3):
            args = self.args(Path(temp) / "bad-hot", "sustained-pressure")
            with self.assertRaisesRegex(RuntimeError, "wrong bytes/length"):
                driver.run(args, library_loader=lambda _: {}, client_factory=CorruptClient)
            result = json.loads((args.output / "result.json").read_text())
            self.assertEqual((result["status"], result["wrong_bytes"]), ("failed", 1))
            self.assertEqual(result["steps"], [])
            self.assertTrue((args.output / "steps.jsonl").exists())

    def test_sustained_arguments_fix_trace_and_disallow_quiet_delay(self):
        args = self.args(Path("unused"), "sustained-pressure")
        self.assertEqual((args.count, args.pressure_delay_sec), (7808, 0))
        for extra in (("--count", "6452"), ("--pressure-delay-sec", "1"), ("--readers", "8")):
            with self.subTest(extra=extra), self.assertRaises(SystemExit):
                self.args(Path("unused"), "sustained-pressure", extra)

    def test_delayed_first_reuse_warms_only_filler_and_records_actual_ages(self):
        for delay in (45, 90, 185):
            now = [0.0]
            events = []
            def sleep(seconds):
                self.assertGreaterEqual(seconds, 0)
                now[0] += seconds

            class TrackingClient(MemoryClient):
                def __init__(self, storage, members, namespace):
                    super().__init__(storage, members, namespace)
                    self.namespace = namespace

                def put(self, keys, values):
                    super().put(keys, values)
                    start = int.from_bytes(keys[0][-8:], "big")
                    if start == 16:
                        now[0] += 0.25
                    if start >= 64:
                        self.storage.pop(driver.key_for(0xD1A60001, 0), None)

                def read(self, keys, values, *, allow_miss=False):
                    events.append((self.namespace, now[0],
                                   [int.from_bytes(key[-8:], "big") for key in keys],
                                   allow_miss))
                    return super().read(keys, values, allow_miss=allow_miss)

            with self.subTest(delay=delay), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "QUICK_PREFILL", 64), \
                    mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(driver.time, "sleep", side_effect=sleep):
                args = self.args(Path(temp) / "delayed", "sustained-pressure",
                                 ("--first-reuse-sec", str(delay)))
                result = driver.run(args, library_loader=lambda _: {}, client_factory=TrackingClient)
                self.assertEqual(result["filler_warmup_reads"], 128)
                warm = [event for event in events if not event[3]]
                self.assertEqual(len(warm), 8)
                self.assertEqual(set(i for event in warm for i in event[2]), set(range(32, 64)))
                probes = [event for event in events if len(event[2]) == 32]
                self.assertEqual(len(probes), 2 * (200 - delay))
                self.assertTrue(all(event[1] >= 0.5 + delay for event in probes))
                self.assertEqual(result["hot_cohort_write_completed_sec"], [0.25, 0.5])
                self.assertEqual(result["first_reuse_sec"], delay)
                self.assertEqual([row["write_to_probe_sec"] for row in result["first_reuse"]],
                                 [delay + 0.25, delay])
                self.assertEqual([row["namespace_hex"] for row in result["first_reuse"]],
                                 [ns.hex() for ns in driver.NAMESPACES])
                self.assertTrue(all((row["hits"], row["misses"]) == (31, 1)
                                    for row in result["first_reuse"]))
                self.assertEqual(result["hot_hits"], 62 * (200 - delay))
                self.assertEqual(result["hot_misses"], 2 * (200 - delay))
                self.assertEqual(result["hot_reads"], 64 * (200 - delay))
                self.assertEqual(result["misses"], 2)
                for row in result["steps"]:
                    due = row["step"] >= delay
                    self.assertEqual(row["hot_probe_started_sec"], row["step"] if due else None)
                    self.assertEqual(row["hot_hits"] + row["hot_misses"], 64 if due else 0)
                for phase in result["phases"][:3]:
                    self.assertEqual((phase["hot_hits"], phase["hot_misses"]), (0, 0))
                self.assertEqual(result["phases"][1]["filler_warmup_reads"], 128)
                config = json.loads((args.output / "trace.json").read_text())["config"]
                self.assertEqual((config["first_reuse_sec"], config["warmup_rounds"],
                                  config["filler_warmup_rounds"]), (delay, 0, 2))

    def test_delayed_probe_uses_actual_elapsed_after_cold_write_stall(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds

        class StallingClient(MemoryClient):
            def put(self, keys, values):
                super().put(keys, values)
                if now[0] == 44:
                    now[0] += 3

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 64), \
                mock.patch.object(driver, "SUSTAIN_STEPS", 47), \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep):
            args = self.args(Path(temp) / "stall", "sustained-pressure",
                             ("--first-reuse-sec", "45"))
            result = driver.run(args, library_loader=lambda _: {}, client_factory=StallingClient)
            self.assertEqual(result["steps"][44]["start_sec"], 44)
            self.assertEqual(result["steps"][44]["hot_probe_started_sec"], 47)
            self.assertEqual(result["steps"][45]["start_sec"], 47)
            self.assertEqual(result["steps"][46]["start_sec"], 48)
            self.assertEqual(result["hot_reads"], 3 * 64)
            self.assertEqual([row["probe_started_sec"] for row in result["first_reuse"]], [47, 47])

    def test_delayed_first_reuse_failures_preserve_failed_result(self):
        for failure in ("filler-missing", "hot-corrupt", "hot-io"):
            now = [0.0]
            def sleep(seconds):
                now[0] += seconds

            class FailingClient(MemoryClient):
                def read(self, keys, values, *, allow_miss=False):
                    if not allow_miss and failure == "filler-missing":
                        self.storage.pop(keys[0], None)
                    if allow_miss and failure == "hot-corrupt":
                        self.storage[keys[0]] = b"x" * len(values[0])
                    if allow_miss and failure == "hot-io":
                        raise RuntimeError("GET failed")
                    return super().read(keys, values, allow_miss=allow_miss)

            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "QUICK_PREFILL", 64), \
                    mock.patch.object(driver, "SUSTAIN_STEPS", 46), \
                    mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(driver.time, "sleep", side_effect=sleep):
                args = self.args(Path(temp) / failure, "sustained-pressure",
                                 ("--first-reuse-sec", "45"))
                with self.assertRaisesRegex(RuntimeError,
                        {"filler-missing": "below-capacity miss", "hot-corrupt": "wrong bytes/length",
                         "hot-io": "GET failed"}[failure]):
                    driver.run(args, library_loader=lambda _: {}, client_factory=FailingClient)
                result = json.loads((args.output / "result.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["wrong_bytes"], int(failure == "hot-corrupt"))
                self.assertEqual(result["hot_hits"], 0)
                self.assertEqual(result["first_reuse"], [])
                if failure != "filler-missing":
                    self.assertEqual(len(result["steps"]), 45)
                    self.assertTrue(all(row["hot_probe_started_sec"] is None for row in result["steps"]))

    def test_first_reuse_arguments_are_bounded_and_sustained_only(self):
        self.assertEqual(self.args(Path("unused"), "sustained-pressure").first_reuse_sec, 0)
        for value in (0, 45, 90, 185):
            self.assertEqual(self.args(Path("unused"), "sustained-pressure",
                                       ("--first-reuse-sec", str(value))).first_reuse_sec, value)
        for scenario, value in (("quick-pressure", "45"), ("roundtrip", "90"),
                                ("pressure", "185"), ("sustained-pressure", "1"),
                                ("sustained-pressure", "-1"), ("sustained-pressure", "186"),
                                ("sustained-pressure", "nan")):
            with self.subTest(scenario=scenario, value=value), self.assertRaises(SystemExit):
                self.args(Path("unused"), scenario, ("--first-reuse-sec", value))

    def test_mixed_pressure_keeps_hot_and_once_only_delayed_cohorts_separate(self):
        now = [0.0]
        events, writes = [], []
        def sleep(seconds):
            self.assertGreaterEqual(seconds, 0)
            now[0] += seconds

        class TrackingClient(MemoryClient):
            def __init__(self, storage, members, namespace):
                super().__init__(storage, members, namespace)
                self.namespace = namespace

            def put(self, keys, values):
                super().put(keys, values)
                indices = [int.from_bytes(key[-8:], "big") for key in keys]
                writes.append((self.namespace, indices))
                if indices[0] in (32, 48, 64):
                    now[0] += .25
                if indices[0] >= 96:
                    # Simulate eviction without reinserting either kind of miss.
                    for index in (0, 32, 48, 64):
                        self.storage.pop(driver.key_for(0xD1A60001, index), None)

            def read(self, keys, values, *, allow_miss=False):
                events.append((self.namespace, now[0],
                               [int.from_bytes(key[-8:], "big") for key in keys], allow_miss))
                return super().read(keys, values, allow_miss=allow_miss)

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 96), \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep):
            args = self.args(Path(temp) / "mixed", "mixed-pressure")
            result = driver.run(args, library_loader=lambda _: {}, client_factory=TrackingClient)
            self.assertEqual(result["filler_warmup_reads"], 64)
            self.assertEqual((result["hot_hits"], result["hot_misses"], result["hot_reads"]),
                             (12528, 400, 12928))
            self.assertEqual((result["reuse_hits"], result["reuse_misses"], result["reuse_reads"]),
                             (90, 6, 96))
            self.assertEqual((result["hits"], result["misses"]), (6584, 8))
            strict = [event for event in events if not event[3]]
            self.assertEqual([event[2] for event in strict[:4]], [list(range(32))] * 4)
            self.assertEqual([event[2] for event in strict[4:]], [list(range(80, 96))] * 4)
            origin = result["pressure_schedule_origin_sec"]
            self.assertEqual(origin, 1.5)
            self.assertEqual(len(result["steps"]), 200)
            for row in result["steps"]:
                self.assertEqual(row["hot_hits"] + row["hot_misses"], 64)
                self.assertEqual(row["cold_written"], 32)
                expected = [row["step"]] if row["step"] in (45, 90, 185) else []
                self.assertEqual([probe["delay_sec"] for probe in row["reuse_probes"]], expected)
                for probe in row["reuse_probes"]:
                    self.assertEqual((probe["hits"], probe["misses"]), (30, 2))
                    self.assertEqual(probe["probe_started_sec"], row["step"])
            for group, starts in zip(result["reuse_cohorts"], ((.25, 1.0), (.5, 1.25), (.75, 1.5))):
                delay, first = group["delay_sec"], group["start_index"]
                self.assertEqual(group["write_completed_sec"], list(starts))
                self.assertEqual(group["count_per_namespace"], 16)
                self.assertEqual([row["namespace_hex"] for row in group["first_reuse"]],
                                 [namespace.hex() for namespace in driver.NAMESPACES])
                pending = [event for event in events if event[2] == list(range(first, first + 16))
                           and event[1] < origin + 199]
                self.assertEqual(len(pending), 2)
                self.assertTrue(all(event[1] == origin + delay and event[3] for event in pending))
                for index, row in enumerate(group["first_reuse"]):
                    self.assertEqual(row["probe_started_sec"], origin + delay)
                    self.assertEqual(row["write_to_probe_sec"], origin + delay - starts[index])
                    self.assertEqual((row["hits"], row["misses"]), (15, 1))
            for namespace in driver.NAMESPACES:
                indices = [index for ns, batch in writes if ns == namespace for index in batch]
                self.assertEqual(indices, list(range(args.count)))
            self.assertEqual([json.loads(line) for line in (args.output / "steps.jsonl").read_text().splitlines()],
                             result["steps"])
            config = json.loads((args.output / "trace.json").read_text())["config"]
            self.assertEqual(config["filler_start"], 80)
            self.assertEqual(config["reuse_cohorts"], [
                {"delay_sec": delay, "start_index": start, "count_per_namespace": 16}
                for delay, start in zip((45, 90, 185), (32, 48, 64))])
            self.assertTrue(config["reuse_once"])
            self.assertFalse(config["refill_on_miss"])
            self.assertEqual([row["name"] for row in result["phases"]],
                             ["prefill", "warm", "before-pressure", "step-040", "step-080",
                              "step-120", "step-160", "step-180", "step-200",
                              "after-pressure", "after-scan"])
            self.assertEqual((result["phases"][1]["hot_hits"],
                              result["phases"][1]["filler_warmup_reads"]), (128, 64))

    def test_mixed_first_reuse_uses_actual_time_after_stall_without_catchup(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds

        class StallingClient(MemoryClient):
            def put(self, keys, values):
                super().put(keys, values)
                if now[0] == 44:
                    now[0] += 3

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 96), \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep):
            args = self.args(Path(temp) / "mixed-stall", "mixed-pressure")
            result = driver.run(args, library_loader=lambda _: {}, client_factory=StallingClient)
            first = result["steps"][44]
            self.assertEqual(first["start_sec"], 44)
            self.assertEqual(first["reuse_probes"], [
                {"delay_sec": 45, "probe_started_sec": 47, "hits": 32, "misses": 0}])
            self.assertEqual([row["probe_started_sec"] for row in result["reuse_cohorts"][0]["first_reuse"]],
                             [47, 47])
            self.assertEqual([row["start_sec"] for row in result["steps"][45:47]], [47, 48])
            self.assertEqual(sum(len(row["reuse_probes"]) for row in result["steps"]), 3)
            self.assertEqual(result["reuse_reads"], 96)

    def test_mixed_failures_preserve_partial_cohorts_and_failed_status(self):
        for failure in ("warm-hot-missing", "filler-missing", "hot-corrupt", "pending-corrupt", "pending-io"):
            now = [0.0]
            def sleep(seconds):
                now[0] += seconds

            class FailingClient(MemoryClient):
                def __init__(self, storage, members, namespace):
                    super().__init__(storage, members, namespace)
                    self.namespace = namespace

                def read(self, keys, values, *, allow_miss=False):
                    first = int.from_bytes(keys[0][-8:], "big")
                    if not allow_miss and ((failure == "warm-hot-missing" and first == 0) or
                                           (failure == "filler-missing" and first == 80)):
                        self.storage.pop(keys[0], None)
                    if allow_miss and ((failure == "hot-corrupt" and first == 0) or
                                       (failure == "pending-corrupt" and first == 32)):
                        self.storage[keys[0]] = b"x" * len(values[0])
                    if (allow_miss and first == 32 and failure == "pending-io" and
                            self.namespace == driver.NAMESPACES[1]):
                        raise RuntimeError("GET failed")
                    return super().read(keys, values, allow_miss=allow_miss)

            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "QUICK_PREFILL", 96), \
                    mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(driver.time, "sleep", side_effect=sleep):
                args = self.args(Path(temp) / failure, "mixed-pressure")
                message = ("below-capacity miss" if failure.endswith("missing") else
                           "GET failed" if failure == "pending-io" else "wrong bytes/length")
                with self.assertRaisesRegex(RuntimeError, message):
                    driver.run(args, library_loader=lambda _: {}, client_factory=FailingClient)
                result = json.loads((args.output / "result.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["wrong_bytes"], int(failure.endswith("corrupt")))
                self.assertEqual(len(result["reuse_cohorts"][0]["first_reuse"]),
                                 1 if failure == "pending-io" else 0)
                if failure.startswith("pending"):
                    self.assertEqual(len(result["steps"]), 45)
                    self.assertTrue(all(not row["reuse_probes"] for row in result["steps"]))

    def test_mixed_incomplete_schedule_is_not_success(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds
        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 96), \
                mock.patch.object(driver, "SUSTAIN_STEPS", 3), \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep):
            args = self.args(Path(temp) / "incomplete", "mixed-pressure")
            with self.assertRaisesRegex(RuntimeError, "did not reach every first reuse cohort"):
                driver.run(args, library_loader=lambda _: {}, client_factory=MemoryClient)
            result = json.loads((args.output / "result.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(len(result["steps"]), 3)
            self.assertTrue(all(not cohort["first_reuse"] for cohort in result["reuse_cohorts"]))

    def test_mixed_arguments_fix_layout_and_disallow_single_delay_options(self):
        args = self.args(Path("unused"), "mixed-pressure",
                         ("--metrics-url", "http://127.0.0.1:12010/metrics"))
        self.assertEqual((args.count, args.first_reuse_sec, args.pressure_delay_sec, args.readers),
                         (7808, 0, 0, 0))
        for extra in (("--count", "6452"), ("--pressure-delay-sec", "1"),
                       ("--first-reuse-sec", "45"), ("--readers", "8")):
            with self.subTest(extra=extra), self.assertRaises(SystemExit):
                self.args(Path("unused"), "mixed-pressure", extra)

    def test_arrivals_sample_new_writes_once_after_both_namespace_acks(self):
        now, writes, reads = [0.0], [], []
        def sleep(seconds):
            self.assertGreaterEqual(seconds, 0)
            now[0] += seconds

        class TrackingClient(MemoryClient):
            def __init__(self, storage, members, namespace):
                super().__init__(storage, members, namespace)
                self.namespace = namespace

            def put(self, keys, values):
                super().put(keys, values)
                indices = [int.from_bytes(key[-8:], "big") for key in keys]
                if indices[0] >= 4608:
                    now[0] += .125
                writes.append((self.namespace, indices, now[0]))

            def read(self, keys, values, *, allow_miss=False):
                reads.append((self.namespace, [int.from_bytes(key[-8:], "big")
                                              for key in keys], now[0], allow_miss))
                return super().read(keys, values, allow_miss=allow_miss)

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep):
            args = self.args(Path(temp) / "arrivals", "arrivals-pressure")
            result = driver.run(args, library_loader=lambda _: {}, client_factory=TrackingClient)
            self.assertEqual(result["status"], "passed")
            self.assertEqual((result["hot_hits"], result["hot_misses"], result["hot_reads"]),
                             (12928, 0, 12928))
            self.assertEqual(result["filler_warmup_reads"], 18304)
            self.assertEqual((result["reuse_hits"], result["reuse_misses"], result["reuse_reads"]),
                             (384, 0, 384))
            self.assertEqual((result["hits"], result["misses"]), (15616, 0))
            self.assertEqual(sum(len(row[1]) for row in reads), 47232)
            self.assertEqual(len(result["steps"]), 200)
            origin = result["pressure_schedule_origin_sec"]
            cohorts = result["reuse_cohorts"]
            self.assertEqual(len(cohorts), 12)
            events = [probe for row in result["steps"] for probe in row["reuse_probes"]]
            self.assertEqual(sorted(probe["admission_step"] for probe in events), list(range(12)))
            for index, cohort in enumerate(cohorts):
                delay = (45, 90, 185)[index % 3]
                first = 4608 + index * 16
                self.assertEqual((cohort["admission_step"], cohort["start_index"],
                                  cohort["count_per_namespace"], cohort["delay_sec"]),
                                 (index, first, 16, delay))
                self.assertEqual(cohort["write_completed_sec"],
                                 [origin + index + .125, origin + index + .25])
                self.assertEqual(cohort["due_sec"], index + .25 + delay)
                pending = [row for row in reads if row[1] == list(range(first, first + 16))]
                # Exactly one first reuse per namespace, then one final scan.
                self.assertEqual(len(pending), 4)
                for ns_index, outcome in enumerate(cohort["first_reuse"]):
                    self.assertEqual(outcome["namespace_hex"], driver.NAMESPACES[ns_index].hex())
                    self.assertEqual(outcome["probe_started_sec"], origin + index + delay + .25)
                    self.assertEqual(outcome["write_to_probe_sec"], delay + (.125 if ns_index == 0 else 0))
                    self.assertEqual((outcome["hits"], outcome["misses"]), (16, 0))
                self.assertTrue(all(row[2] >= origin + cohort["due_sec"] and row[3]
                                    for row in pending))
            strict = [row for row in reads if not row[3]]
            self.assertEqual([row[1] for row in strict[:4]], [list(range(32))] * 4)
            self.assertTrue(all(max(row[1]) < 4608 for row in strict))
            for namespace in driver.NAMESPACES:
                indices = [index for ns, batch, _ in writes if ns == namespace for index in batch]
                self.assertEqual(indices, list(range(args.count)))
            self.assertEqual([json.loads(line) for line in (args.output / "steps.jsonl").read_text().splitlines()],
                             result["steps"])
            config = json.loads((args.output / "trace.json").read_text())["config"]
            self.assertEqual((config["filler_start"], config["sampled_arrival_steps"],
                              config["reuse_delay_origin"]), (32, 12, "later-namespace-put-ack"))
            self.assertFalse(config["refill_on_miss"])
            self.assertFalse(config["independent_read_threads"])
            self.assertEqual(len(result["phases"]), 11)

    def test_arrivals_stall_uses_actual_ack_age_without_catchup(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds

        class StallingClient(MemoryClient):
            def put(self, keys, values):
                super().put(keys, values)
                if now[0] == 44:
                    now[0] += 3

        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 96), \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep):
            args = self.args(Path(temp) / "arrivals-stall", "arrivals-pressure")
            result = driver.run(args, library_loader=lambda _: {}, client_factory=StallingClient)
            self.assertEqual(result["steps"][44]["start_sec"], 44)
            self.assertEqual([row["start_sec"] for row in result["steps"][45:47]], [47, 48])
            first = result["reuse_cohorts"][0]
            self.assertEqual([row["write_to_probe_sec"] for row in first["first_reuse"]], [47, 47])
            self.assertEqual(result["reuse_reads"], 384)

    def test_arrivals_misses_do_not_refill_and_failures_keep_partial_evidence(self):
        for failure in ("pending-missing", "warm-hot-missing", "filler-missing",
                        "pending-corrupt", "pending-io", "put-io"):
            now, writes = [0.0], []
            def sleep(seconds):
                now[0] += seconds

            class FailingClient(MemoryClient):
                def __init__(self, storage, members, namespace):
                    super().__init__(storage, members, namespace)
                    self.namespace = namespace

                def put(self, keys, values):
                    first = int.from_bytes(keys[0][-8:], "big")
                    if failure == "put-io" and first == 96 and self.namespace == driver.NAMESPACES[1]:
                        raise RuntimeError("PUT failed")
                    super().put(keys, values)
                    writes.append((self.namespace, first))
                    if failure == "pending-missing" and 96 <= first < 288:
                        for key in keys:
                            self.storage.pop(key)

                def read(self, keys, values, *, allow_miss=False):
                    first = int.from_bytes(keys[0][-8:], "big")
                    if not allow_miss and ((failure == "warm-hot-missing" and first == 0) or
                                           (failure == "filler-missing" and first == 32)):
                        self.storage.pop(keys[0], None)
                    if allow_miss and first == 96:
                        if failure == "pending-corrupt":
                            self.storage[keys[0]] = b"x" * len(values[0])
                        if failure == "pending-io" and self.namespace == driver.NAMESPACES[1]:
                            raise RuntimeError("GET failed")
                    return super().read(keys, values, allow_miss=allow_miss)

            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "QUICK_PREFILL", 96), \
                    mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(driver.time, "sleep", side_effect=sleep):
                args = self.args(Path(temp) / failure, "arrivals-pressure")
                if failure == "pending-missing":
                    result = driver.run(args, library_loader=lambda _: {}, client_factory=FailingClient)
                    self.assertEqual((result["reuse_hits"], result["reuse_misses"]), (0, 384))
                    self.assertEqual(len(writes), 2 * args.count // 16)
                    self.assertEqual(len(set(writes)), len(writes))
                    continue
                message = ("below-capacity miss" if failure.endswith("missing") else
                           "GET failed" if failure == "pending-io" else
                           "PUT failed" if failure == "put-io" else "wrong bytes/length")
                with self.assertRaisesRegex(RuntimeError, message):
                    driver.run(args, library_loader=lambda _: {}, client_factory=FailingClient)
                result = json.loads((args.output / "result.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["wrong_bytes"], int(failure.endswith("corrupt")))
                if failure == "pending-io":
                    self.assertEqual(len(result["reuse_cohorts"][0]["first_reuse"]), 1)
                    self.assertEqual(len(result["steps"]), 45)
                if failure == "put-io":
                    self.assertEqual(len(result["steps"]), 0)
                    self.assertEqual(result["reuse_cohorts"][0]["write_completed_sec"], [0])

    def test_arrivals_incomplete_schedule_fails_instead_of_dropping_pending(self):
        now = [0.0]
        def sleep(seconds):
            now[0] += seconds
        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(driver, "QUICK_PREFILL", 96), \
                mock.patch.object(driver, "SUSTAIN_STEPS", 12), \
                mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(driver.time, "sleep", side_effect=sleep):
            args = self.args(Path(temp) / "arrivals-incomplete", "arrivals-pressure")
            with self.assertRaisesRegex(RuntimeError, "did not reach every first reuse cohort"):
                driver.run(args, library_loader=lambda _: {}, client_factory=MemoryClient)
            result = json.loads((args.output / "result.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(len(result["reuse_cohorts"]), 12)
            self.assertEqual(len(result["steps"]), 12)
            self.assertTrue(all(not cohort["first_reuse"] for cohort in result["reuse_cohorts"]))

    def test_arrivals_arguments_keep_fixed_layout_and_reject_other_delay_modes(self):
        args = self.args(Path("unused"), "arrivals-pressure",
                         ("--metrics-url", "http://127.0.0.1:12010/metrics"))
        self.assertEqual((args.count, args.first_reuse_sec, args.pressure_delay_sec, args.readers),
                         (7808, 0, 0, 0))
        for extra in (("--count", "6452"), ("--pressure-delay-sec", "1"),
                      ("--first-reuse-sec", "45"), ("--readers", "8")):
            with self.subTest(extra=extra), self.assertRaises(SystemExit):
                self.args(Path("unused"), "arrivals-pressure", extra)

    def test_refill_arguments_are_arrivals_only_and_opt_in(self):
        self.assertFalse(self.args(Path("unused"), "arrivals-pressure").refill_on_miss)
        self.assertTrue(self.args(Path("unused"), "arrivals-pressure",
                                  ("--refill-on-miss",)).refill_on_miss)
        for scenario in ("roundtrip", "write", "verify", "pressure", "quick-pressure",
                         "sustained-pressure", "mixed-pressure"):
            with self.subTest(scenario=scenario), self.assertRaises(SystemExit):
                self.args(Path("unused"), scenario, ("--refill-on-miss",))

    def test_refill_records_first_outcomes_and_separate_writes_and_verification(self):
        # The all-hit run uses the actual 4608/7808 layout. Fault runs shrink only
        # prefill, not the 200-step/12-cohort/384-first-read schedule.
        for mode in ("hit", "partial", "miss", "evicted-again", "hot-loss"):
            now, writes, reads = [0.0], [], []
            def sleep(seconds):
                now[0] += seconds

            class RefillClient(MemoryClient):
                def put(self, keys, values):
                    first = int.from_bytes(keys[0][-8:], "big")
                    writes.append((first, len(keys)))
                    super().put(keys, values)
                    if len(keys) == 16 and prefill <= first < prefill + 192:
                        for index, key in enumerate(keys):
                            if mode != "hit" and (mode != "partial" or index % 2 == 0):
                                self.storage.pop(key)
                    if len(keys) == 1 and mode == "evicted-again":
                        self.storage.pop(keys[0])

                def read(self, keys, values, *, allow_miss=False):
                    reads.append(len(keys))
                    if mode == "hot-loss" and allow_miss and int.from_bytes(keys[0][-8:], "big") == 0:
                        for key in keys:
                            self.storage.pop(key, None)
                    return super().read(keys, values, allow_miss=allow_miss)

            prefill = 4608 if mode == "hit" else 96
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "QUICK_PREFILL", prefill), \
                    mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(driver.time, "sleep", side_effect=sleep):
                args = self.args(Path(temp) / mode, "arrivals-pressure", ("--refill-on-miss",))
                result = driver.run(args, library_loader=lambda _: {}, client_factory=RefillClient)
                refill = result["refill"]
                misses = 0 if mode == "hit" else 192 if mode == "partial" else 384
                self.assertEqual((result["reuse_hits"], result["reuse_misses"]), (384 - misses, misses))
                self.assertEqual((refill["first_hits"], refill["first_misses"]), (384 - misses, misses))
                self.assertEqual((refill["put_attempts"], refill["put_acks"]), (misses, misses))
                self.assertEqual((refill["attempted_payload_bytes"], refill["acked_payload_bytes"]),
                                 (misses * 4096, misses * 4096))
                self.assertEqual((refill["verify_hits"], refill["verify_misses"]),
                                 (0, misses) if mode == "evicted-again" else (misses, 0))
                self.assertEqual(len(refill["objects"]), 384)
                self.assertEqual(len({(row["namespace_hex"], row["index"])
                                      for row in refill["objects"]}), 384)
                self.assertEqual(sum(size == 1 for _, size in writes), misses)
                self.assertEqual(sum(size for _, size in writes), args.count * 2 + misses)
                self.assertEqual(sum(reads), 12928 + (prefill - 32) * 4 + 384 + misses + args.count * 2)
                self.assertEqual(refill["baseline_payload_bytes"], args.count * 2 * 4096)
                self.assertEqual(refill["logical_payload_write_amplification"],
                                 1 + misses / (args.count * 2))
                for row in refill["objects"]:
                    self.assertIn(row["first_hit"], (0, 1))
                    self.assertEqual(row["put_attempted"], not row["first_hit"])
                    self.assertEqual(row["put_acked"], not row["first_hit"])
                    self.assertGreaterEqual(row["first_get_started_sec"], 45)
                    self.assertEqual(row["verify_hit"] is not None, not row["first_hit"])
                self.assertEqual(result["hot_misses"], 12800 if mode == "hot-loss" else 0)
                trace = json.loads((args.output / "trace.json").read_text())
                self.assertTrue(trace["config"]["refill_on_miss"])
                self.assertEqual(trace["config"]["refill_scope"], "sampled-first-reuse-only")

    def test_refill_failures_preserve_partial_first_and_write_evidence(self):
        for failure in ("first-io", "put-io", "verify-io", "verify-corrupt",
                        "later-put-io", "second-ns-put-io"):
            now = [0.0]
            def sleep(seconds):
                now[0] += seconds

            class FailingRefillClient(MemoryClient):
                def __init__(self, storage, members, namespace):
                    super().__init__(storage, members, namespace)
                    self.namespace = namespace

                def put(self, keys, values):
                    first = int.from_bytes(keys[0][-8:], "big")
                    fail_put = (failure == "put-io" or
                                (failure == "later-put-io" and first == 97) or
                                (failure == "second-ns-put-io" and
                                 self.namespace == driver.NAMESPACES[1]))
                    if len(keys) == 1 and fail_put:
                        # A transport failure can hide an already-committed PUT.
                        super().put(keys, values)
                        raise RuntimeError("refill PUT failed")
                    super().put(keys, values)
                    if len(keys) == 16 and 96 <= first < 288:
                        for key in keys:
                            self.storage.pop(key)
                    if len(keys) == 1 and failure == "verify-corrupt":
                        self.storage[keys[0]] = b"x" * len(values[0])

                def read(self, keys, values, *, allow_miss=False):
                    if len(keys) == 1:
                        if failure == "first-io" or (failure == "verify-io" and keys[0] in self.storage):
                            raise RuntimeError("refill GET failed")
                    return super().read(keys, values, allow_miss=allow_miss)

            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "QUICK_PREFILL", 96), \
                    mock.patch.object(driver.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(driver.time, "sleep", side_effect=sleep):
                args = self.args(Path(temp) / failure, "arrivals-pressure", ("--refill-on-miss",))
                with self.assertRaises(RuntimeError):
                    driver.run(args, library_loader=lambda _: {}, client_factory=FailingRefillClient)
                result = json.loads((args.output / "result.json").read_text())
                self.assertEqual(result["status"], "failed")
                refill = result["refill"]
                self.assertNotIn("logical_payload_write_amplification", refill)
                completed = 16 if failure == "second-ns-put-io" else 1 if failure == "later-put-io" else 0
                self.assertEqual(len(refill["objects"]), completed + 1)
                for previous in refill["objects"][:-1]:
                    self.assertEqual((previous["first_hit"], previous["put_acked"],
                                      previous["verify_hit"]), (0, True, 1))
                row = refill["objects"][-1]
                self.assertEqual(refill["first_misses"], completed + int(failure != "first-io"))
                self.assertEqual(row["first_hit"], None if failure == "first-io" else 0)
                self.assertEqual(refill["put_attempts"], completed + int(failure != "first-io"))
                self.assertEqual(refill["put_acks"], completed + int(failure.startswith("verify")))
                self.assertEqual(refill["verify_hits"], completed)
                self.assertIsNone(row["verify_hit"])
                self.assertEqual(result["wrong_bytes"], int(failure == "verify-corrupt"))
                self.assertEqual(len(result["steps"]), 45)

    def test_multisize_arguments_and_real_payload_boundaries(self):
        args = self.args(Path("unused"), "multisize-pressure")
        self.assertEqual(args.count, 6400)
        self.assertFalse(args.refill_on_miss)
        for extra in (("--count", "6399"), ("--pressure-delay-sec", "1"),
                      ("--first-reuse-sec", "45"), ("--refill-on-miss",), ("--readers", "8")):
            with self.subTest(extra=extra), self.assertRaises(SystemExit):
                self.args(Path("unused"), "multisize-pressure", extra)
        args = self.args(Path("unused"), "multisize-pressure",
                         ("--metrics-url", "http://127.0.0.1:12010/metrics"))
        self.assertEqual(args.metrics_url, "http://127.0.0.1:12010/metrics")
        for length in (4096, 1048577):
            value = driver.payload(1, b"ns", b"key", 0, length)
            self.assertEqual(len(value), length)
            self.assertTrue(driver.check_read(value, 1, value, length))
            with self.assertRaisesRegex(RuntimeError, "wrong bytes/length"):
                driver.check_read(value, 1, value[:-1] + bytes([value[-1] ^ 1]), length)

    def test_multisize_fixed_schedule_separates_admission_warmup_hot_and_scan(self):
        # Full 6400-key layout without allocating 6.7GB: tokens retain requested
        # payload length and every generator identity. Actual bytes are tested
        # separately above and in the corruption/failure cases below.
        class PayloadToken:
            def __init__(self, fields):
                self.fields = fields

            def __len__(self):
                return self.fields[-1]

            def __eq__(self, other):
                return isinstance(other, PayloadToken) and self.fields == other.fields

        for mode in ("hit", "warm-partial", "warm-all", "hot-loss", "large-starved"):
            phase, operations = ["admission"], []
            original_phase = driver.record_phase
            def record_phase(args, result, name, started):
                original_phase(args, result, name, started)
                phase[0] = name

            class TrackingClient(MemoryClient):
                def __init__(self, storage, members, namespace):
                    super().__init__(storage, members, namespace)
                    self.namespace = namespace

                def put(self, keys, values):
                    indices = [int.from_bytes(key[-8:], "big") for key in keys]
                    self_lengths = [len(value) for value in values]
                    expected = (4096, 1048577)[(indices[0] // 16) % 2]
                    if self_lengths != [expected] * 16:
                        raise AssertionError("batch is not the frozen homogeneous size")
                    operations.append(("put", self.namespace, indices))
                    super().put(keys, values)
                    if mode == "large-starved" and indices[0] >= 512 and expected == 1048577:
                        for key in keys:
                            self.storage.pop(key)

                def read(self, keys, values, *, allow_miss=False):
                    if not allow_miss:
                        raise AssertionError("compact diagnostic reads must allow eviction misses")
                    indices = [int.from_bytes(key[-8:], "big") for key in keys]
                    operations.append(("read-" + phase[0], self.namespace, indices))
                    if phase[0] == "prefill" and mode in ("warm-partial", "warm-all"):
                        for key in keys[:8 if mode == "warm-partial" else 16]:
                            self.storage.pop(key, None)
                    if phase[0] == "before-pressure" and mode == "hot-loss":
                        for key in keys:
                            self.storage.pop(key, None)
                    if phase[0] == "prefill" and mode == "large-starved":
                        for key in list(self.storage):
                            if (int.from_bytes(key[-8:], "big") // 16) % 2:
                                self.storage.pop(key)
                    return super().read(keys, values, allow_miss=allow_miss)

            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "payload", side_effect=lambda *fields: PayloadToken(fields)), \
                    mock.patch.object(driver, "record_phase", side_effect=record_phase):
                args = self.args(Path(temp) / mode, "multisize-pressure")
                result = driver.run(args, library_loader=lambda _: {}, client_factory=TrackingClient)
                self.assertEqual(result["status"], "passed")
                self.assertEqual((result["admission_hits"], result["admission_misses"]), (64, 0))
                self.assertEqual(result["warmup_hits"] + result["warmup_misses"], 128)
                self.assertEqual(result["hot_reads"], 23552)
                self.assertEqual(result["hits"] + result["misses"], 12800)
                self.assertEqual(sum(len(indices) for kind, _, indices in operations if kind.startswith("read")),
                                 36544)
                self.assertEqual(result["warmup_misses"],
                                 64 if mode in ("warm-partial", "large-starved") else
                                 128 if mode == "warm-all" else 0)
                self.assertEqual(result["hot_misses"],
                                 11776 if mode in ("warm-partial", "large-starved") else
                                 23552 if mode in ("warm-all", "hot-loss") else 0)
                for index, row in enumerate(result["multisize"]):
                    self.assertEqual(row["payload_bytes"], (4096, 1048577)[index])
                    self.assertEqual(row["slot_size"], (index + 1) * 1048576)
                    self.assertEqual((row["put_attempts"], row["put_acks"]), (6400, 6400))
                    for part, count in (("admission", 32), ("warmup", 64), ("hot", 11776), ("scan", 6400)):
                        self.assertEqual(row[part + "_hits"] + row[part + "_misses"], count)
                if mode == "large-starved":
                    self.assertEqual(result["multisize"][1]["scan_hits"], 0)
                    self.assertEqual(result["multisize"][0]["scan_hits"], 6400)
                for namespace in driver.NAMESPACES:
                    writes = [i for kind, ns, indices in operations if kind == "put" and ns == namespace
                              for i in indices]
                    self.assertEqual(writes, list(range(6400)))
                    own = [(kind, indices) for kind, ns, indices in operations if ns == namespace]
                    self.assertEqual(own[:4], [("put", list(range(16))), ("read-admission", list(range(16))),
                                               ("put", list(range(16, 32))), ("read-admission", list(range(16, 32)))])
                self.assertEqual([row["name"] for row in result["phases"]],
                                 ["prefill", "warm", "before-pressure", "after-pressure", "after-scan"])
                trace = json.loads((args.output / "trace.json").read_text())
                config = trace["config"]
                self.assertEqual(config["lengths"], [4096, 1048577])
                self.assertEqual(config["length_layout"], "alternating-homogeneous-16-key-batches")
                self.assertEqual((config["prefill"], config["hot"], config["warmup_rounds"],
                                  config["admission_read_batches_per_namespace"]), (512, 32, 2, 2))
                self.assertTrue(config["warmup_allow_miss"])
                self.assertFalse(config["refill_on_miss"])
                self.assertFalse(config["independent_read_threads"])

    def test_multisize_real_byte_failures_and_partial_acks_remain_failed(self):
        # Small injected layout, real payload generation and byte oracle. The
        # full production-sized count/order is exercised by the token test.
        for failure in ("put-ns2", "admission-io", "admission-corrupt", "warm-io",
                        "hot-io", "scan-io", "scan-corrupt"):
            phase = ["admission"]
            original_phase = driver.record_phase
            def record_phase(args, result, name, started):
                original_phase(args, result, name, started)
                phase[0] = name

            class FailingClient(MemoryClient):
                def __init__(self, storage, members, namespace):
                    super().__init__(storage, members, namespace)
                    self.namespace = namespace

                def put(self, keys, values):
                    if failure == "put-ns2" and self.namespace == driver.NAMESPACES[1]:
                        super().put(keys, values)  # Completion is uncertain on error.
                        raise RuntimeError("PUT failed")
                    super().put(keys, values)

                def read(self, keys, values, *, allow_miss=False):
                    first = int.from_bytes(keys[0][-8:], "big")
                    active = ((failure.startswith("admission") and phase[0] == "admission") or
                              (failure == "warm-io" and phase[0] == "prefill") or
                              (failure == "hot-io" and phase[0] == "before-pressure") or
                              (failure.startswith("scan") and phase[0] == "after-pressure" and first == 32))
                    if active:
                        if failure.endswith("corrupt"):
                            self.storage[keys[0]] = b"x" * len(values[0])
                        else:
                            raise RuntimeError("GET failed")
                    return super().read(keys, values, allow_miss=allow_miss)

            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp, \
                    mock.patch.object(driver, "MULTISIZE_COUNT", 64), \
                    mock.patch.object(driver, "MULTISIZE_PREFILL", 32), \
                    mock.patch.object(driver, "record_phase", side_effect=record_phase):
                args = self.args(Path(temp) / failure, "multisize-pressure")
                with self.assertRaisesRegex(RuntimeError, "wrong bytes/length|PUT failed|GET failed"):
                    driver.run(args, library_loader=lambda _: {}, client_factory=FailingClient)
                result = json.loads((args.output / "result.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["wrong_bytes"], int(failure.endswith("corrupt")))
                rows = result["multisize"]
                if failure == "put-ns2":
                    self.assertEqual((rows[0]["put_attempts"], rows[0]["put_acks"]), (32, 16))
                    self.assertEqual((rows[1]["put_attempts"], rows[1]["put_acks"]), (16, 16))
                    self.assertEqual(result["admission_hits"], 32)
                if failure.startswith("admission"):
                    self.assertEqual((rows[0]["put_attempts"], rows[0]["put_acks"]), (16, 16))
                    self.assertEqual(result["admission_hits"], 0)
                if failure.startswith("scan"):
                    self.assertEqual(sum(row["scan_hits"] for row in rows), 32)
                    self.assertEqual(sum(row["put_acks"] for row in rows), 128)

    def test_missing_key_oracle_rejects_all_false_hits_including_empty(self):
        class FakeLib:
            status = 0
            length = 0
            rc = 0

            def dfkv_exist(fake, handle, key, length):
                return 0

            def dfkv_batch_get_auto(fake, handle, keys, lengths, data, caps,
                                    count, hits, sizes):
                self.assertEqual(count, 1)
                self.assertEqual(caps[0], 1)
                self.assertTrue(data[0])
                hits[0], sizes[0] = fake.status, fake.length
                return fake.rc

        client = driver.Client.__new__(driver.Client)
        client.handle, client.lib = 1, FakeLib()
        client.expect_missing(b"never-written\x00key")
        for status, length, rc in ((1, 0, 0), (1, 1, 0), (2, 0, 0), (0, 0, -1)):
            with self.subTest(status=status, length=length, rc=rc):
                client.lib.status, client.lib.length, client.lib.rc = status, length, rc
                with self.assertRaisesRegex(RuntimeError, "expected missing key"):
                    client.expect_missing(b"never-written\x00key")

    def test_missing_key_oracle_rejects_existing_value_larger_than_get_capacity(self):
        class CapAwareLib:
            value = b"oversized-value"

            def dfkv_batch_get_auto(fake, handle, keys, lengths, data, caps,
                                    count, hits, sizes):
                hits[0] = int(len(fake.value) <= caps[0])
                sizes[0] = len(fake.value) if hits[0] else 0
                self.assertEqual(hits[0], 0)
                return 0

            def dfkv_exist(fake, handle, key, length):
                return 1

        client = driver.Client.__new__(driver.Client)
        client.handle, client.lib = 1, CapAwareLib()
        with self.assertRaisesRegex(RuntimeError, "expected missing key: exists=1"):
            client.expect_missing(b"existing-key")

    def test_guest_xml_checker_rejects_invalid_results_with_python_optimization(self):
        script = (ROOT / "deploy/validation/guest_checks.sh").read_text()
        checker = script.split("<<'CHECK_GTEST_XML'\n", 1)[1].split(
            "\nCHECK_GTEST_XML\n", 1)[0]
        cases = (
            ("valid", '<testcase name="ok" status="run"/>', 1, (), True),
            ("zero", "", 1, (), False),
            ("mismatch", '<testcase name="ok"/>', 2, (), False),
            ("skipped", '<testcase><skipped/></testcase>', 1, (), False),
            ("notrun", '<testcase status="notrun"/>', 1, (), False),
            ("failure", '<testcase><failure/></testcase>', 1, (), False),
            ("error", '<testcase><error/></testcase>', 1, (), False),
            ("allowed-skip", '<testcase status="notrun"><skipped/></testcase>',
             1, ("allow-skip",), True),
            ("bare-notrun", '<testcase status="notrun"/>',
             1, ("allow-skip",), False),
            ("missing-file", None, 1, (), False),
        )
        with tempfile.TemporaryDirectory() as temp:
            for name, body, expected, extra_args, passes in cases:
                with self.subTest(name=name):
                    xml = Path(temp) / f"{name}.xml"
                    if body is not None:
                        xml.write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>")
                    result = subprocess.run(
                        [sys.executable, "-", str(xml), str(expected), *extra_args], input=checker,
                        env={**os.environ, "PYTHONOPTIMIZE": "1"},
                        text=True, capture_output=True, timeout=10, check=False)
                    self.assertEqual(result.returncode == 0, passes,
                                     result.stdout + result.stderr)

    def test_cabi_failures_are_not_successes(self):
        class FakeLib:
            def dfkv_open_v2(self, options):
                return 1

            def dfkv_transport_mode(self, handle):
                return b"fake-only"

            def dfkv_batch_put(self, *args):
                return -1

            def dfkv_batch_get_auto(self, *args):
                return -1

            def dfkv_remove(self, *args):
                return 0

            def dfkv_close(self, handle):
                pass

        client = driver.Client(FakeLib(), "members", b"ns")
        with self.assertRaisesRegex(RuntimeError, "PUT failed"):
            client.put([b"key"], [b"data"])
        with self.assertRaisesRegex(RuntimeError, "GET failed"):
            client.read([b"key"], [b"data"])
        with self.assertRaisesRegex(RuntimeError, "REMOVE failed"):
            client.remove(b"key")
        # rc=0 still requires each PUT's success flag, not merely call success.
        client.lib.dfkv_batch_put = lambda *args: 0
        with self.assertRaisesRegex(RuntimeError, "PUT failed"):
            client.put([b"key"], [b"data"])
        client.close()


class ConcurrentDriverTest(unittest.TestCase):
    def arguments(self, output, extra=()):
        return driver.parse_args(["--lib", "/fake/libdfkv.so", "--members", "n=localhost:1",
                                  "--output", str(output), "--scenario", "concurrent-pressure", *extra])

    def test_concurrent_arguments_are_fixed_and_do_not_reuse_roundtrip_readers(self):
        args = self.arguments(Path("unused"))
        self.assertEqual(args.count, 7808)
        self.assertEqual(args.readers, 0)
        for extra in (("--count", "7807"), ("--pressure-delay-sec", "1"),
                      ("--readers", "8"), ("--first-reuse-sec", "45"), ("--refill-on-miss",)):
            with self.subTest(extra=extra), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                self.arguments(Path("unused"), extra)

    def test_cabi_timing_brackets_only_the_native_get_even_on_failure(self):
        class Lib:
            def dfkv_batch_get_auto(self, handle, keys, lengths, data, caps, count, hits, sizes):
                self.called_at = time.monotonic()
                time.sleep(.001)
                hits[0], sizes[0] = 1, 3
                ct.memmove(data[0], b"abc", 3)
                return self.rc
        lib = Lib()
        client = driver.Client.__new__(driver.Client)
        client.handle, client.lib = 1, lib
        for rc in (0, -1):
            lib.rc = rc
            timing = {}
            if rc:
                with self.assertRaisesRegex(RuntimeError, "GET failed"):
                    client.read([b"key"], [b"abc"], call_timing=timing)
            else:
                self.assertEqual(client.read([b"key"], [b"abc"], call_timing=timing), 1)
            self.assertLessEqual(timing["call_started_sec"], lib.called_at)
            self.assertLess(lib.called_at, timing["call_finished_sec"])
        lib.rc = 0
        with self.assertRaisesRegex(RuntimeError, "wrong bytes/length"):
            client.read([b"key"], [b"xyz"], call_timing=timing)
        self.assertLessEqual(timing["call_started_sec"], lib.called_at)
        self.assertLess(lib.called_at, timing["call_finished_sec"])

    def run_memory(self, output, failure=None, *, startup_probe=None):
        storage, handles = {}, []
        first_reads_ready = (threading.Event() if startup_probe is None
                             else startup_probe["first_reads_ready"])
        first_reads = threading.Barrier(8, action=first_reads_ready.set, timeout=3)
        reader_failed = threading.Event()
        class IndependentClient(MemoryClient):
            def __init__(self, storage, members, namespace):
                if failure == "open" and len(handles) == 5:
                    raise RuntimeError("reader open failed")
                super().__init__(storage, members, namespace)
                self.number = len(handles)
                self.active, self.closed, self.calls = False, False, 0
                handles.append(self)
            def close(self):
                if self.active:
                    raise AssertionError("close while native call active")
                self.closed = True
            def put(self, keys, values):
                cold_write = self.number == 0 and int.from_bytes(keys[0][-8:], "big") >= 4608
                if cold_write and failure in (None, "miss"):
                    # The accelerated fake writer can finish before a released
                    # reader gets scheduled. Hold it until all first fake GETs
                    # have entered; do not change production pacing/deadlines.
                    if not first_reads_ready.wait(3):
                        raise RuntimeError("fixture readers did not enter first GET")
                if failure == "writer-and-corrupt" and int.from_bytes(keys[0][-8:], "big") >= 4608:
                    if not reader_failed.wait(3):
                        raise RuntimeError("fixture reader never reached corruption")
                    super().put(keys, values)
                    raise RuntimeError("writer PUT failed concurrently")
                super().put(keys, values)
                if cold_write and startup_probe is not None:
                    startup_probe["writer_acked"].set()
                if failure == "writer" and self.number == 1 and int.from_bytes(keys[0][-8:], "big") >= 4608:
                    raise RuntimeError("writer PUT failed after uncertain completion")
            def read(self, keys, values, *, allow_miss=False, call_timing=None):
                if call_timing is None:
                    return super().read(keys, values, allow_miss=allow_miss)
                if self.closed or self.active or self.number < 2:
                    raise AssertionError("reader did not own an independent idle handle")
                self.active = True
                call_timing["call_started_sec"] = time.monotonic()
                try:
                    if self.number == 9 and failure == "corrupt-last-start":
                        driver.check_read(values[0], 1, b"x" * len(values[0]), len(values[0]))
                    if self.number == 9 and self.calls == 0 and startup_probe is not None:
                        startup_probe["last_reader_waiting"].set()
                        if not startup_probe["release_last_reader"].wait(3):
                            raise RuntimeError("fixture last reader was not released")
                    if self.calls == 0 and failure not in ("deadline", "submit"):
                        first_reads.wait()
                    self.calls += 1
                    time.sleep(.002)
                    if (self.number == 2 and failure == "io") or (self.number == 9 and failure == "io-last"):
                        raise RuntimeError("reader GET failed")
                    if (self.number == 2 and failure == "corrupt") or (self.number == 9 and failure in ("corrupt-last", "writer-and-corrupt")):
                        reader_failed.set()
                        driver.check_read(values[0], 1, b"x" * len(values[0]), len(values[0]))
                    if failure == "miss":
                        return 0
                    return super().read(keys, values, allow_miss=allow_miss)
                finally:
                    call_timing["call_finished_sec"] = time.monotonic()
                    self.active = False
        args = self.arguments(output)
        original_pool = driver.ThreadPoolExecutor
        class FailingPool(original_pool):
            def submit(self, *args, **kwargs):
                self.submits = getattr(self, "submits", 0) + 1
                if self.submits == 4:
                    raise RuntimeError("reader submit failed")
                return super().submit(*args, **kwargs)
        with mock.patch.object(driver, "CONCURRENT_INTERVAL_SEC", .001), \
                mock.patch.object(driver, "CONCURRENT_MAX_BATCHES", 1 if failure == "limit" else 8192), \
                mock.patch.object(driver, "CONCURRENT_SOFT_DEADLINE_SEC", 0 if failure == "deadline" else 90), \
                mock.patch.object(driver, "ThreadPoolExecutor", FailingPool if failure == "submit" else original_pool):
            try:
                outcome = driver.run(args, library_loader=lambda _: storage, client_factory=IndependentClient)
                error = None
            except Exception as caught:
                outcome = json.loads((output / "result.json").read_text())
                error = caught
        self.assertTrue(all(handle.closed and not handle.active for handle in handles))
        return outcome, handles, error

    def test_fake_writer_waits_for_delayed_last_reader_before_ack(self):
        probe = {name: threading.Event() for name in (
            "writer_waiting", "writer_acked", "last_reader_waiting", "release_last_reader",
            "first_reads_ready")}
        results, failures = [], []
        original_wait = probe["first_reads_ready"].wait
        def observe_writer_wait(timeout):
            probe["writer_waiting"].set()
            return original_wait(timeout)
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(
                probe["first_reads_ready"], "wait", side_effect=observe_writer_wait):
            def run():
                try:
                    results.append(self.run_memory(Path(temp) / "case", startup_probe=probe))
                except BaseException as error:
                    failures.append(error)
            worker = threading.Thread(target=run)
            worker.start()
            try:
                self.assertTrue(probe["last_reader_waiting"].wait(3))
                self.assertTrue(probe["writer_waiting"].wait(3))
                # The last reader is positively held outside the eight-party
                # rendezvous, so this is not a negative sleep-based assertion.
                self.assertFalse(probe["writer_acked"].is_set())
            finally:
                probe["release_last_reader"].set()
                worker.join(10)
            self.assertFalse(worker.is_alive())
            if failures:
                raise failures[0]
            self.assertEqual(len(results), 1)
            result, handles, error = results[0]
            self.assertIsNone(error)
            self.assertEqual(result["status"], "passed")
            self.assertTrue(probe["writer_acked"].is_set())
            self.assertEqual(len(handles), 10)
            self.assertTrue(all(row["batches"] for row in result["concurrency"]["readers"]))
            self.assertEqual(len(result["concurrency"]["writer_steps"]), 200)

    def test_full_fixed_schedule_uses_ten_handles_and_real_thread_overlap(self):
        for mode in (None, "miss"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                output = Path(temp) / "case"
                result, handles, error = self.run_memory(output, mode)
                self.assertIsNone(error)
                self.assertEqual(result["status"], "passed")
                self.assertEqual(len(handles), 10)
                stats = result["concurrency"]
                self.assertEqual(stats["warmup_reads"], 512)
                self.assertEqual(len(stats["writer_steps"]), 200)
                self.assertEqual(sum(r["put_acks"] for r in stats["writer_steps"]), 6400)
                self.assertEqual(result["hits"] + result["misses"], 15616)
                self.assertEqual([r["name"] for r in result["phases"]],
                                 ["prefill", "warm", "before-pressure", "step-100", "after-pressure", "after-scan"])
                events = []
                for index, row in enumerate(stats["readers"]):
                    self.assertEqual((row["reader_id"], row["namespace_hex"], row["start_index"], row["count"]),
                                     (index, driver.NAMESPACES[index % 2].hex(), index // 2 * 32, 32))
                    self.assertEqual(row["status"], "passed")
                    self.assertGreater(len(row["batches"]), 0)
                    saved = [json.loads(line) for line in (output / f"concurrent-reader-{index:02d}.jsonl").read_text().splitlines()]
                    self.assertEqual(saved, row["batches"])
                    for batch, call in enumerate(row["batches"]):
                        self.assertEqual((call["batch"], call["start_index"], call["hits"] + call["misses"]),
                                         (batch, index // 2 * 32 + batch % 2 * 16, 16))
                        events += [(call["call_started_sec"], 1), (call["call_finished_sec"], -1)]
                active = peak = 0
                for _, change in sorted(events):
                    active += change
                    peak = max(peak, active)
                self.assertGreaterEqual(peak, 2)
                hits = sum(r["hits"] for r in stats["readers"])
                misses = sum(r["misses"] for r in stats["readers"])
                self.assertEqual((result["hot_hits"], result["hot_misses"], result["hot_reads"]),
                                 (512 + hits, misses, 512 + hits + misses))
                self.assertEqual(misses > 0, mode == "miss")
                trace = json.loads((output / "trace.json").read_text())
                self.assertEqual((trace["config"]["independent_readers"], trace["config"]["hot"],
                                  trace["config"]["reader_window"], trace["config"]["timing_origin"]),
                                 (8, 128, 32, "driver-start"))

    def test_originating_last_reader_corruption_survives_barrier_and_writer_failures(self):
        for failure in ("corrupt-last-start", "writer-and-corrupt"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                result, handles, error = self.run_memory(Path(temp) / "case", failure)
                self.assertIsNotNone(error)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["wrong_bytes"], 1)
                self.assertIn("wrong bytes/length", result["concurrency"]["readers"][7]["error"])
                if failure == "corrupt-last-start":
                    self.assertIn("wrong bytes/length", str(error))
                else:
                    self.assertIn("writer PUT failed concurrently", str(error))

    def test_reader_and_writer_failures_preserve_partial_evidence_and_join_before_close(self):
        for failure in ("io", "corrupt", "io-last", "corrupt-last", "writer", "open", "submit", "limit", "deadline"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                output = Path(temp) / "case"
                result, handles, error = self.run_memory(output, failure)
                self.assertIsNotNone(error)
                expected = {"io": "reader GET failed", "io-last": "reader GET failed",
                            "corrupt": "wrong bytes/length", "corrupt-last": "wrong bytes/length",
                            "writer": "writer PUT failed", "open": "reader open failed",
                            "submit": "reader submit failed", "limit": "batch limit exhausted",
                            "deadline": "soft deadline exceeded"}
                self.assertIn(expected[failure], str(error))
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["wrong_bytes"], int(failure.startswith("corrupt")))
                if failure not in ("open", "submit"):
                    self.assertIn("concurrency", result)
                if failure in ("io", "corrupt", "io-last", "corrupt-last"):
                    bad_index = 7 if failure.endswith("-last") else 0
                    bad = result["concurrency"]["readers"][bad_index]
                    self.assertEqual(bad["status"], "failed")
                    self.assertEqual(bad["batches"], [])
                    log = (output / f"concurrent-reader-{bad_index:02d}.jsonl").read_text()
                    self.assertIn("error", log)
                if failure == "writer":
                    row = result["concurrency"]["writer_steps"][0]
                    self.assertEqual((row["put_attempts"], row["put_acks"]), (32, 16))


if __name__ == "__main__":
    unittest.main()
