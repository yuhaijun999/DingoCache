"""Exercise the trace adapter through the existing ctypes client, without services."""

import copy
from concurrent.futures import ThreadPoolExecutor
import ctypes as ct
from pathlib import Path
import sys
import threading
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/validation"))
sys.path.insert(0, str(ROOT / "integration/common/src"))
import disk_scenarios as driver
import eviction_trace as trace
import eviction_client as adapter


class Library:
    def __init__(self):
        self.storage = {}
        self.calls = []
        self.put_rc = self.get_rc = 0
        self.put_slot = 1
        self.hit_override = None
        self.closed = 0
        self.entered = self.release = None

    def dfkv_batch_put(self, handle, keys, lengths, data, sizes, count, ok):
        self.calls.append("put")
        if self.entered is not None:
            self.entered.set()
            if not self.release.wait(2):
                raise RuntimeError("test release timeout")
        key = ct.string_at(keys[0], lengths[0])
        if self.put_rc == 0 and self.put_slot == 1:
            self.storage[key] = ct.string_at(data[0], sizes[0])
        ok[0] = self.put_slot
        return self.put_rc

    def dfkv_batch_get_auto(self, handle, keys, lengths, data, caps, count, hits, sizes):
        self.calls.append("get")
        value = self.storage.get(ct.string_at(keys[0], lengths[0]))
        hit = value is not None and len(value) <= caps[0]
        hits[0] = int(hit) if self.hit_override is None else self.hit_override
        if hit:
            ct.memmove(data[0], value, len(value))
            sizes[0] = len(value)
        return self.get_rc

    def dfkv_exist(self, handle, key, length):
        self.calls.append("exist")
        return int(ct.string_at(key, length) in self.storage)

    def dfkv_close(self, handle):
        self.closed += 1


class TraceClientTest(unittest.TestCase):
    def setUp(self):
        self.document = trace.generate(objects=2, rounds=1, length=3)
        self.namespace = bytes.fromhex(self.document["requests"][0]["namespace_hex"])
        self.lib = Library()
        client_type = driver.Client
        def factory(lib, members, namespace):
            self.assertIs(lib, self.lib)
            self.assertEqual((members, namespace), ("n1=localhost:1", self.namespace))
            client = client_type.__new__(client_type)
            client.lib, client.handle, client.transport = lib, 7, "test-only"
            return client
        self.factory = mock.Mock(side_effect=factory)
        self.client = adapter.TraceClient(self.document, self.lib, "n1=localhost:1",
                                          self.namespace, client_factory=self.factory)
        self.addCleanup(self.client.close)

    def test_put_hit_and_binary_identity_without_mutating_trace(self):
        original = copy.deepcopy(self.document)
        self.assertEqual(self.client.execute(0), {"outcome": "put_ack"})
        self.assertEqual(self.client.execute(2), {"outcome": "get_hit"})
        row = self.document["requests"][0]
        self.assertEqual(self.lib.storage[bytes.fromhex(row["key_hex"])], trace.payload(row))
        self.assertEqual(self.lib.calls, ["put", "get"])
        self.assertEqual(self.document, original)
        self.assertEqual(self.client.transport, "test-only")

    def test_zero_hit_is_unresolved_not_a_proven_miss(self):
        # Actual ABI shape for both absent keys and some network failures:
        # rc=0, hit=0; EXIST=0 would not disambiguate those failures either.
        for storage in ({}, {bytes.fromhex(self.document["requests"][2]["key_hex"]): b"x" * 100}):
            self.lib.storage = storage
            self.assertEqual(self.client.execute(2), {"outcome": "get_unresolved"})
        self.assertEqual(self.lib.calls, ["get", "get"])
        self.client.execute(0)
        self.lib.hit_override = 0  # resident exists but API swallowed a failure
        self.assertEqual(self.client.execute(2), {"outcome": "get_unresolved"})
        self.assertNotIn("exist", self.lib.calls)

    def test_corrupt_and_truncated_hits_raise(self):
        row = self.document["requests"][2]
        for value in (b"bad", b"x"):
            with self.subTest(value=value):
                self.lib.storage[bytes.fromhex(row["key_hex"])] = value
                with self.assertRaises(RuntimeError):
                    self.client.execute(2)

    def test_failed_put_does_not_ack_and_failed_get_does_not_miss(self):
        for rc, slot in ((1, 1), (0, 0), (0, 2)):
            self.lib.put_rc, self.lib.put_slot = rc, slot
            with self.assertRaises(RuntimeError):
                self.client.execute(0)
        self.lib.get_rc = 1
        with self.assertRaises(RuntimeError):
            self.client.execute(2)
        self.lib.get_rc, self.lib.hit_override = 0, 2
        with self.assertRaises(RuntimeError):
            self.client.execute(2)

    def install_status_get(self, status=0, value=None, length=None):
        def get(handle, key, key_len, buffer, cap, out_len):
            self.lib.calls.append("status_get")
            self.assertEqual(ct.string_at(key, key_len), bytes.fromhex(
                self.document["requests"][2]["key_hex"]))
            data = trace.payload(self.document["requests"][2]) if value is None else value
            if status == 0:
                ct.memmove(buffer, data, min(len(data), cap))
            ct.cast(out_len, ct.POINTER(ct.c_uint64))[0] = (
                (len(data) if status == 0 else 0) if length is None else length)
            return status
        self.lib.dfkv_get_auto_status = get

    def test_status_symbol_hit_and_only_explicit_absence_is_miss(self):
        self.install_status_get()
        self.assertEqual(self.client.execute(2), {"outcome": "get_hit"})
        self.install_status_get(1)
        self.assertEqual(self.client.execute(2), {"outcome": "get_miss"})
        for status in (2, 3, 4, 5, 6, 7, 8, -1, 999):
            with self.subTest(status=status):
                self.install_status_get(status)
                with self.assertRaisesRegex(RuntimeError, "scalar GET failed"):
                    self.client.execute(2)
        self.assertEqual(self.lib.calls, ["status_get"] * 11)

    def test_status_hit_validates_bytes_and_full_length(self):
        for value, length in ((b"bad", None), (b"x", None),
                              (b"", None), (b"abcd", None), (b"abc", 999)):
            with self.subTest(value=value, length=length):
                self.install_status_get(value=value, length=length)
                with self.assertRaises(RuntimeError):
                    self.client.execute(2)
        self.install_status_get(1, length=99)
        with self.assertRaisesRegex(RuntimeError, "nonzero length"):
            self.client.execute(2)

    def test_status_exception_does_not_fall_back_to_legacy(self):
        self.lib.dfkv_get_auto_status = mock.Mock(side_effect=RuntimeError("native error"))
        with self.assertRaisesRegex(RuntimeError, "native error"):
            self.client.execute(2)
        self.assertEqual(self.lib.calls, [])

    def test_loader_optionally_binds_status_symbol(self):
        for available in (False, True):
            lib = mock.Mock(spec=["dfkv_open_v2", "dfkv_close", "dfkv_transport_mode",
                                  "dfkv_remove", "dfkv_exist", "dfkv_batch_put",
                                  "dfkv_batch_get_auto"] +
                                 (["dfkv_get_auto_status"] if available else []))
            with mock.patch.object(driver.ct, "CDLL", return_value=lib):
                self.assertIs(driver.load_library("unused-test-path"), lib)
            if available:
                function = lib.dfkv_get_auto_status
                self.assertEqual(function.restype, ct.c_int)
                self.assertEqual(function.argtypes, [ct.c_void_p, ct.c_void_p,
                    ct.c_uint64, ct.c_void_p, ct.c_uint64, ct.POINTER(ct.c_uint64)])

    def test_bad_id_or_other_namespace_makes_no_call(self):
        for request_id in (-1, True, 1.0, 99, 1):
            with self.subTest(request_id=request_id):
                with self.assertRaises(ValueError):
                    self.client.execute(request_id)
        self.assertEqual(self.lib.calls, [])

    def test_validate_before_open_and_copy_source(self):
        bad = copy.deepcopy(self.document)
        bad["requests"][0]["length"] = 4
        for document, namespace in ((bad, self.namespace), (self.document, b"absent"),
                                    (self.document, "not-bytes")):
            factory = mock.Mock()
            with self.assertRaises(ValueError):
                adapter.TraceClient(document, self.lib, "n1", namespace, client_factory=factory)
            factory.assert_not_called()
        self.document["requests"][0]["op"] = "get"
        self.assertEqual(self.client.execute(0)["outcome"], "put_ack")

    def test_close_is_idempotent_and_rejects_reuse(self):
        with self.client:
            self.client.execute(0)
        self.client.close()
        self.assertEqual(self.lib.closed, 1)
        with self.assertRaises(RuntimeError):
            self.client.execute(0)

    def test_close_waits_for_inflight_call(self):
        self.lib.entered, self.lib.release = threading.Event(), threading.Event()
        contended = threading.Event()
        lock = self.client._lock
        class ObservedLock:
            def __enter__(self):
                if lock.locked():
                    contended.set()
                lock.acquire()
            def __exit__(self, *args):
                lock.release()
        self.client._lock = ObservedLock()
        with ThreadPoolExecutor(2) as pool:
            call = pool.submit(self.client.execute, 0)
            self.assertTrue(self.lib.entered.wait(1))
            closing = pool.submit(self.client.close)
            self.assertTrue(contended.wait(1), "close must attempt the occupied lock")
            self.assertFalse(closing.done())
            self.assertEqual(self.lib.closed, 0)
            self.lib.release.set()
            self.assertEqual(call.result(timeout=2)["outcome"], "put_ack")
            closing.result(timeout=2)
        self.assertEqual(self.lib.closed, 1)

    def test_close_exception_does_not_allow_handle_reuse(self):
        with mock.patch.object(self.lib, "dfkv_close", side_effect=RuntimeError("close failed")):
            with self.assertRaises(RuntimeError):
                self.client.close()
        with self.assertRaises(RuntimeError):
            self.client.execute(0)
        self.client.close()

    def test_default_factory_reuses_existing_client(self):
        with mock.patch.object(driver, "Client", self.factory):
            client = adapter.TraceClient(self.document, self.lib, "n1=localhost:1", self.namespace)
            client.close()
        self.assertEqual(self.factory.call_count, 2)


class BatchReadOutcomesTest(unittest.TestCase):
    def setUp(self):
        self.lib = Library()
        self.client = driver.Client.__new__(driver.Client)
        self.client.lib, self.client.handle = self.lib, 7
        self.keys = [b"first\x00", b"second"]
        self.values = [b"abc", b"def"]

    def install_batch(self, statuses, *, values=None, lengths=None, rc=0,
                      legacy=False, deduplicated=False):
        payloads = self.values if values is None else values
        def get(handle, keys, key_lens, data, caps, count, output, sizes):
            self.lib.calls.append("deduplicated_batch" if deduplicated else
                                  "legacy_batch" if legacy else "status_batch")
            self.assertEqual(handle, 7)
            self.assertEqual(count, len(statuses))
            self.assertEqual([ct.string_at(keys[i], key_lens[i]) for i in range(count)],
                             self.keys)
            self.assertEqual(list(output), [-999] * count)
            self.assertEqual(list(sizes), [0] * count)
            for i, status in enumerate(statuses):
                output[i] = status
                hit = status == (1 if legacy else 0)
                sizes[i] = (len(payloads[i]) if hit else 0) if lengths is None else lengths[i]
                if hit:
                    ct.memmove(data[i], payloads[i], min(len(payloads[i]), caps[i]))
            return rc
        symbol = ("dfkv_batch_get_auto_deduplicated_status" if deduplicated else
                  "dfkv_batch_get_auto" if legacy else "dfkv_batch_get_auto_status")
        setattr(self.lib, symbol, get)

    def test_mixed_statuses_keep_order_and_valid_siblings(self):
        statuses = [0, 1, 2, 3, 4, 5, 6, 7, 8, 999, -999]
        self.keys = [i.to_bytes(2, "big") for i in range(len(statuses))]
        self.values = [b"abc"] * len(statuses)
        self.install_batch(statuses)
        results = self.client.read_outcomes(self.keys, self.values)
        self.assertEqual([r["outcome"] for r in results],
                         ["get_hit", "get_miss"] + ["client_error"] * 9)
        self.assertEqual([r["status"] for r in results], statuses)
        self.assertTrue(all(r["error"] for r in results[2:]))
        self.assertEqual(self.lib.calls, ["status_batch"])

    def test_binary_keys_empty_payload_and_empty_batch(self):
        self.values = [b"", b"def"]
        self.install_batch([0, 0])
        self.assertEqual(self.client.read_outcomes(self.keys, self.values),
                         [{"outcome": "get_hit", "status": 0}] * 2)
        self.assertEqual(self.client.read_outcomes([], []), [])
        self.assertEqual(self.lib.calls, ["status_batch"])

    def test_invalid_inputs_fail_before_any_native_call(self):
        for keys, values in ((self.keys, [b"abc"]), ([b""], [b"a"]),
                             (["text"], [b"a"]), ([b"k"], ["text"]),
                             ([None], [b"a"])):
            with self.subTest(keys=keys, values=values):
                with self.assertRaises((TypeError, ValueError)):
                    self.client.read_outcomes(keys, values)
        self.client.handle = None
        with self.assertRaisesRegex(RuntimeError, "closed"):
            self.client.read_outcomes([], [])
        self.assertEqual(self.lib.calls, [])

    def test_bad_lengths_and_payloads_are_slot_errors_not_whole_batch(self):
        cases = [([0, 0], [b"bad", b"def"], [3, 3]),
                 ([0, 0], [b"ab", b"def"], [2, 3]),
                 ([0, 0], [b"abc", b"def"], [999, 3]),
                 ([1, 0], self.values, [1, 3]),
                 ([4, 0], self.values, [999, 3])]
        for statuses, values, lengths in cases:
            with self.subTest(statuses=statuses, lengths=lengths):
                self.install_batch(statuses, values=values, lengths=lengths)
                result = self.client.read_outcomes(self.keys, self.values)
                self.assertEqual(result[0]["outcome"], "client_error")
                self.assertTrue(result[0]["error"])
                self.assertEqual(result[1], {"outcome": "get_hit", "status": 0})
        self.assertEqual(self.lib.calls, ["status_batch"] * len(cases))

    def test_entire_call_failure_and_exception_never_fall_back(self):
        self.install_batch([0, 1], rc=-1)
        with self.assertRaisesRegex(RuntimeError, "rc=-1"):
            self.client.read_outcomes(self.keys, self.values)
        self.lib.dfkv_batch_get_auto_status = mock.Mock(side_effect=RuntimeError("native failed"))
        with self.assertRaisesRegex(RuntimeError, "native failed"):
            self.client.read_outcomes(self.keys, self.values)
        self.assertEqual(self.lib.calls, ["status_batch"])

    def test_unwritten_native_slots_remain_errors_not_default_ok(self):
        def partial(handle, keys, key_lens, data, caps, count, statuses, sizes):
            self.lib.calls.append("partial_status_batch")
            statuses[0], sizes[0] = 0, 3
            ct.memmove(data[0], b"abc", 3)
            return 0
        self.lib.dfkv_batch_get_auto_status = partial
        self.assertEqual(self.client.read_outcomes(self.keys, self.values), [
            {"outcome": "get_hit", "status": 0},
            {"outcome": "client_error", "status": -999,
             "error": "batch_index=1: batch GET failed: status=-999"}])
        self.assertEqual(self.lib.calls, ["partial_status_batch"])

    def test_legacy_no_hit_is_unresolved_not_miss(self):
        self.install_batch([1, 0], legacy=True)
        self.assertEqual(self.client.read_outcomes(self.keys, self.values),
                         [{"outcome": "get_hit"}, {"outcome": "get_unresolved"}])
        self.assertEqual(self.lib.calls, ["legacy_batch"])

    def test_legacy_invalid_results_keep_valid_siblings_and_rc_failure_raises(self):
        for hits, lengths in (([2, 1], [0, 3]), ([0, 1], [99, 3]),
                              ([1, 1], [2, 3])):
            with self.subTest(hits=hits, lengths=lengths):
                self.install_batch(hits, lengths=lengths, legacy=True)
                result = self.client.read_outcomes(self.keys, self.values)
                self.assertEqual(result[0]["outcome"], "client_error")
                self.assertEqual(result[1], {"outcome": "get_hit"})
                self.assertNotIn("status", result[0])
        self.install_batch([1, 0], legacy=True, rc=-1)
        with self.assertRaisesRegex(RuntimeError, "rc=-1"):
            self.client.read_outcomes(self.keys, self.values)
        self.assertNotIn("exist", self.lib.calls)

    def test_explicit_dedup_uses_only_new_symbol_and_preserves_direct_selection(self):
        self.install_batch([0, 1], deduplicated=True)
        self.install_batch([1, 0])
        self.assertEqual(self.client.read_deduplicated_outcomes(self.keys, self.values),
                         [{"outcome": "get_hit", "status": 0},
                          {"outcome": "get_miss", "status": 1}])
        self.assertEqual(self.client.read_outcomes(self.keys, self.values),
                         [{"outcome": "get_miss", "status": 1},
                          {"outcome": "get_hit", "status": 0}])
        self.assertEqual(self.lib.calls, ["deduplicated_batch", "status_batch"])

    def test_explicit_dedup_missing_symbol_never_uses_direct_or_legacy(self):
        self.install_batch([0, 0])
        self.install_batch([1, 1], legacy=True)
        for keys, values in ((self.keys, self.values), ([], [])):
            with self.assertRaisesRegex(RuntimeError, "deduplicated.*unavailable"):
                self.client.read_deduplicated_outcomes(keys, values)
        self.assertEqual(self.lib.calls, [])

    def test_explicit_dedup_mixed_errors_and_unknown_keep_valid_siblings(self):
        statuses = [0, 1, 2, 3, 4, 5, 6, 7, 8, 999, -999]
        self.keys = [i.to_bytes(2, "big") for i in range(len(statuses))]
        self.values = [b"a\x00c"] * len(statuses)
        self.install_batch(statuses, deduplicated=True)
        results = self.client.read_deduplicated_outcomes(self.keys, self.values)
        self.assertEqual([r["outcome"] for r in results],
                         ["get_hit", "get_miss"] + ["client_error"] * 9)
        self.assertEqual([r["status"] for r in results], statuses)
        self.assertTrue(all(r["error"] for r in results[2:]))
        self.assertEqual(self.lib.calls, ["deduplicated_batch"])

    def test_explicit_dedup_bad_lengths_and_bytes_are_not_hits_or_misses(self):
        for statuses, values, lengths in (
                ([0, 0], [b"bad", b"def"], [3, 3]),
                ([0, 0], [b"ab", b"def"], [2, 3]),
                ([0, 0], self.values, [999, 3]),
                ([1, 0], self.values, [1, 3]),
                ([4, 0], self.values, [999, 3])):
            with self.subTest(statuses=statuses, lengths=lengths):
                self.install_batch(statuses, values=values, lengths=lengths,
                                   deduplicated=True)
                result = self.client.read_deduplicated_outcomes(self.keys, self.values)
                self.assertEqual(result[0]["outcome"], "client_error")
                self.assertTrue(result[0]["error"])
                self.assertEqual(result[1], {"outcome": "get_hit", "status": 0})
        self.assertEqual(self.lib.calls, ["deduplicated_batch"] * 5)

    def test_explicit_dedup_rc_failure_and_exception_never_retry_or_fall_back(self):
        self.install_batch([0, 0])
        self.install_batch([1, 1], legacy=True)
        self.install_batch([0, 1], rc=-1, deduplicated=True)
        with self.assertRaisesRegex(RuntimeError, "rc=-1"):
            self.client.read_deduplicated_outcomes(self.keys, self.values)
        self.lib.dfkv_batch_get_auto_deduplicated_status = mock.Mock(
            side_effect=RuntimeError("dedup native failed"))
        with self.assertRaisesRegex(RuntimeError, "dedup native failed"):
            self.client.read_deduplicated_outcomes(self.keys, self.values)
        self.assertEqual(self.lib.calls, ["deduplicated_batch"])

    def test_explicit_dedup_binary_empty_payload_and_input_validation(self):
        self.values = [b"", b"d\x00f"]
        self.install_batch([0, 0], deduplicated=True)
        self.assertEqual(self.client.read_deduplicated_outcomes(iter(self.keys), iter(self.values)),
                         [{"outcome": "get_hit", "status": 0}] * 2)
        self.assertEqual(self.client.read_deduplicated_outcomes([], []), [])
        for keys, values in ((self.keys, [b"abc"]), ([b""], [b"a"]),
                             (["text"], [b"a"]), ([b"k"], ["text"]),
                             ([None], [b"a"])):
            with self.subTest(keys=keys, values=values):
                with self.assertRaises((TypeError, ValueError)):
                    self.client.read_deduplicated_outcomes(keys, values)
        self.client.handle = None
        with self.assertRaisesRegex(RuntimeError, "closed"):
            self.client.read_deduplicated_outcomes([], [])
        self.assertEqual(self.lib.calls, ["deduplicated_batch"])

    def test_optional_dedup_symbol_is_independent_of_direct_and_scalar(self):
        required = ["dfkv_open_v2", "dfkv_close", "dfkv_transport_mode",
                    "dfkv_remove", "dfkv_exist", "dfkv_batch_put", "dfkv_batch_get_auto"]
        for scalar in (False, True):
            for direct in (False, True):
                for dedup in (False, True):
                    with self.subTest(scalar=scalar, direct=direct, dedup=dedup):
                        optional = (["dfkv_get_auto_status"] if scalar else [])
                        optional += (["dfkv_batch_get_auto_status"] if direct else [])
                        optional += (["dfkv_batch_get_auto_deduplicated_status"] if dedup else [])
                        lib = mock.Mock(spec=required + optional)
                        with mock.patch.object(driver.ct, "CDLL", return_value=lib):
                            self.assertIs(driver.load_library("unused-test-path"), lib)
                        if dedup:
                            function = lib.dfkv_batch_get_auto_deduplicated_status
                            self.assertEqual(function.restype, ct.c_int)
                            self.assertEqual(function.argtypes, [ct.c_void_p,
                                ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_uint64),
                                ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_uint64), ct.c_int,
                                ct.POINTER(ct.c_int), ct.POINTER(ct.c_uint64)])

    def test_optional_metrics_snapshot_is_bound_once_at_library_load(self):
        required = ["dfkv_open_v2", "dfkv_close", "dfkv_transport_mode",
                    "dfkv_remove", "dfkv_exist", "dfkv_batch_put", "dfkv_batch_get_auto"]
        for present in (False, True):
            lib = mock.Mock(spec=required + (["dfkv_stats_snapshot"] if present else []))
            with mock.patch.object(driver.ct, "CDLL", return_value=lib):
                self.assertIs(driver.load_library("unused-test-path"), lib)
            if present:
                function = lib.dfkv_stats_snapshot
                self.assertEqual(function.argtypes, [ct.c_void_p, ct.c_void_p, ct.c_uint64])
                self.assertEqual(function.restype, ct.c_uint64)

    def test_optional_batch_symbol_is_bound_independently_of_scalar(self):
        required = ["dfkv_open_v2", "dfkv_close", "dfkv_transport_mode",
                    "dfkv_remove", "dfkv_exist", "dfkv_batch_put", "dfkv_batch_get_auto"]
        for scalar, batch in ((False, False), (False, True), (True, False), (True, True)):
            with self.subTest(scalar=scalar, batch=batch):
                lib = mock.Mock(spec=required + (["dfkv_get_auto_status"] if scalar else []) +
                                (["dfkv_batch_get_auto_status"] if batch else []))
                with mock.patch.object(driver.ct, "CDLL", return_value=lib):
                    self.assertIs(driver.load_library("unused-test-path"), lib)
                if batch:
                    function = lib.dfkv_batch_get_auto_status
                    self.assertEqual(function.restype, ct.c_int)
                    self.assertEqual(function.argtypes, [ct.c_void_p,
                        ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_uint64),
                        ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_uint64), ct.c_int,
                        ct.POINTER(ct.c_int), ct.POINTER(ct.c_uint64)])


if __name__ == "__main__":
    unittest.main()
