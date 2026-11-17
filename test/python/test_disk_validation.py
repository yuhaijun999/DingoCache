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


if __name__ == "__main__":
    unittest.main()
