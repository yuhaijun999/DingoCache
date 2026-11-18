"""Offline trace contract tests: no client, shared library or server required."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "deploy/validation/eviction_trace.py"
SPEC = importlib.util.spec_from_file_location("eviction_trace", SCRIPT)
trace = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(trace)


class EvictionTraceTest(unittest.TestCase):
    def test_deterministic_bytes_and_pinned_digest(self):
        first = trace.generate(seed=7, objects=2, rounds=2, length=3,
                               interval_ns=100)
        self.assertEqual(trace.encode(first), trace.encode(trace.generate(
            seed=7, objects=2, rounds=2, length=3, interval_ns=100)))
        self.assertEqual(hashlib.sha256(trace.encode(first)).hexdigest(),
                         "f6f17af010fb21f3e06797bdcf3fd64c498b73462f97b6d6ff15588eb714309e")
        self.assertNotEqual(first, trace.generate(seed=8, objects=2,
                                                 rounds=2, length=3,
                                                 interval_ns=100))

    def test_complete_order_identity_and_denominators(self):
        document = trace.generate(objects=2, rounds=2, length=3, interval_ns=100)
        rows = document["requests"]
        self.assertEqual([row["op"] for row in rows],
                         ["put", "put", "get", "get", "put", "get", "put", "get"])
        self.assertEqual([row["phase"] for row in rows],
                         ["init"] * 2 + ["warmup"] * 2 + ["measure"] * 4)
        self.assertEqual([row["request_id"] for row in rows], list(range(8)))
        self.assertEqual([row["scheduled_ns"] for row in rows],
                         list(range(0, 800, 100)))
        self.assertEqual(rows[0]["key_hex"], rows[2]["key_hex"])
        self.assertEqual(rows[0]["payload_sha256"], rows[5]["payload_sha256"])
        self.assertNotEqual(rows[0]["namespace_hex"], rows[1]["namespace_hex"])
        self.assertTrue(all(row["generation"] == 0 for row in rows))
        self.assertEqual(trace.validate(document), {
            "requests": 8, "put_requests": 4, "get_requests": 4,
            "put_payload_bytes": 12, "requested_get_bytes": 12,
            "measurement_requests": 4})

    def test_payload_has_independent_byte_oracle(self):
        row = trace.generate(seed=7, objects=1, rounds=1, length=3)["requests"][0]
        fields = [bytes.fromhex(row["namespace_hex"]),
                  bytes.fromhex(row["key_hex"]), (0).to_bytes(8, "big"),
                  (3).to_bytes(8, "big")]
        raw = b"dfkv-offline-trace-v1" + b"".join(
            len(field).to_bytes(8, "big") + field for field in fields)
        expected = hashlib.shake_256(raw).digest(3)
        self.assertEqual(trace.payload(row), expected)
        self.assertEqual(row["payload_sha256"], hashlib.sha256(expected).hexdigest())

    def test_rejects_bad_config_types_ranges_and_extra_fields(self):
        for name, value in [("seed", -1), ("seed", 2**64), ("objects", 0),
                            ("objects", 257), ("rounds", 0), ("rounds", 257),
                            ("length", 0), ("length", 65537),
                            ("interval_ns", 0), ("interval_ns", 1000000001),
                            ("objects", True), ("length", 3.0), ("seed", "1")]:
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                trace.generate(**{name: value})
        document = trace.generate()
        document["config"]["extra"] = 1
        with self.assertRaises(ValueError):
            trace.validate(document)

    def test_rejects_corruption_even_with_recomputed_checksum(self):
        original = trace.generate(objects=2, rounds=2, length=3)
        mutations = [
            lambda d: d["requests"].pop(),
            lambda d: d["requests"].append(copy.deepcopy(d["requests"][-1])),
            lambda d: d["requests"].reverse(),
            lambda d: d["requests"][0].update(request_id=1),
            lambda d: d["requests"][1].update(scheduled_ns=0),
            lambda d: d["requests"][0].update(op="get"),
            lambda d: d["requests"][2].update(phase="measure"),
            lambda d: d["requests"][2].update(key_hex="00"),
            lambda d: d["requests"][2].update(namespace_hex="00"),
            lambda d: d["requests"][2].update(generation=1),
            lambda d: d["requests"][2].update(length=4),
            lambda d: d["requests"][2].update(payload_sha256="0" * 64),
            lambda d: d["requests"][0].update(request_id=False),
            lambda d: d.update(schema="future-version"),
            lambda d: d.update(summary={"requests": 8}),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                document = copy.deepcopy(original)
                mutate(document)
                document["sha256"] = trace.checksum(document)
                with self.assertRaises(ValueError):
                    trace.validate(document)
        original["sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            trace.validate(original)

    def test_rejects_wrong_shapes_and_boolean_config(self):
        for document in (None, [], {}, {"config": []}):
            with self.subTest(document=document), self.assertRaises(ValueError):
                trace.validate(document)
        document = trace.generate()
        document["config"]["seed"] = True
        with self.assertRaises(ValueError):
            trace.validate(document)

    def test_boundaries_and_measurement_totals(self):
        for size in (1, 256):
            document = trace.generate(seed=2**64-1, objects=size, rounds=size,
                                      length=1, interval_ns=1000000000)
            result = trace.validate(document)
            self.assertEqual(result["requests"], 4 * size)
            self.assertEqual(result["get_requests"], 2 * size)
            self.assertEqual(result["measurement_requests"], 2 * size)
        document = trace.generate(objects=1, rounds=1, length=65536)
        self.assertEqual(trace.validate(document)["put_payload_bytes"], 131072)

    def run_cli(self, *args):
        return subprocess.run([sys.executable, "-I", str(SCRIPT), *args],
                              text=True, capture_output=True, timeout=15)

    def test_cli_roundtrip_reproducible_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = [Path(directory) / name for name in ("a.json", "b.json")]
            for path in (first, second):
                result = self.run_cli("generate", str(path))
                self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            before = first.read_bytes()
            checked = self.run_cli("check", str(first))
            self.assertEqual(checked.returncode, 0, checked.stderr)
            self.assertEqual(json.loads(checked.stdout)["requests"], 40)
            self.assertNotEqual(self.run_cli("generate", str(first)).returncode, 0)
            self.assertEqual(first.read_bytes(), before)
            link = Path(directory) / "link.json"
            link.symlink_to(first)
            self.assertNotEqual(self.run_cli("generate", str(link)).returncode, 0)
            self.assertTrue(link.is_symlink())
            self.assertEqual(first.read_bytes(), before)
            missing_parent = Path(directory) / "missing" / "trace.json"
            self.assertNotEqual(self.run_cli("generate", str(missing_parent)).returncode, 0)
            self.assertFalse(missing_parent.parent.exists())

    def test_cli_rejects_invalid_duplicate_nonfinite_and_oversize_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            for raw in (b"{", b'{"schema":1,"schema":2}', b'{"x":NaN}',
                        b"[1]", b" " * (trace.MAX_FILE_BYTES + 1)):
                with self.subTest(raw=raw[:40]):
                    path.write_bytes(raw)
                    result = self.run_cli("check", str(path))
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
