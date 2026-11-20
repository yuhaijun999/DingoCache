#!/usr/bin/env python3
"""External-client correctness diagnostics; never starts or resets a server.

Run each case with a fresh --output directory. write/verify share a deterministic
initial dataset for clean-restart checks. pressure permits eviction misses only
AFTER all writes; quick-pressure also probes hot keys during cold writes, after
a strict warmup. sustained-pressure adds paced interleaving without catch-up or
miss refill; delayed first reuse warms only the non-cohort filler. mixed-pressure
combines existing hot reads with three once-only delayed cohorts. arrivals-pressure
samples the first twelve newly arriving batches, delaying reuse from their PUT
ACKs while writes continue. Its optional sampled-miss refill mode records
first outcomes, one refill attempt and one verification GET separately. multisize-pressure
uses two real slot classes, allowing compact-geometry donor losses during warmup.
concurrent-pressure uses eight private reader handles alongside paced writes. These are not
persistence, performance, or transport-backend proof.
Requires the repository's dfkv_common on PYTHONPATH (no third-party dependencies).
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import ctypes as ct
import hashlib
import json
from pathlib import Path
import threading
import time
import urllib.request

from dfkv_common.cabi import (
    DfkvClientOptionsV2, make_client_options_v2, make_key_array, make_key_buffer,
)

NAMESPACES = (b"disk-validation\x00A\xff", b"disk-validation\x00B\xfe")
LENGTHS = (4095, 4096, 4097, 65535, 65536, 65537,
           1048575, 1048576, 1048577)
BATCH_SIZE = 16
QUICK_PREFILL = 4608
QUICK_HOT = 32
SUSTAIN_STEPS = 200
SUSTAIN_INTERVAL_SEC = 1
MIXED_REUSE_DELAYS = (45, 90, 185)
MIXED_COHORT_SIZE = 16
ARRIVAL_SAMPLE_STEPS = 12
MULTISIZE_COUNT = 6400
MULTISIZE_PREFILL = 512
MULTISIZE_LENGTHS = (4096, 1048577)
CONCURRENT_READERS = 8
CONCURRENT_HOT = 128
CONCURRENT_WINDOW = 32
CONCURRENT_INTERVAL_SEC = .1
CONCURRENT_MAX_BATCHES = 32768
CONCURRENT_SOFT_DEADLINE_SEC = 90


def key_for(seed, index):
    return b"disk\x00" + seed.to_bytes(8, "big") + index.to_bytes(8, "big")


def payload(seed, namespace, key, generation, length):
    """SHAKE's stream supplies offset-varying bytes, domain separated by length."""
    fields = (seed.to_bytes(8, "big"), namespace, key,
              generation.to_bytes(8, "big"), length.to_bytes(8, "big"))
    identity = b"disk-synthetic-v1" + b"".join(
        len(field).to_bytes(8, "big") + field for field in fields)
    return hashlib.shake_256(identity).digest(length)


def check_read(expected, hit, actual, length, *, allow_miss=False):
    if hit not in (0, 1):
        raise RuntimeError(f"invalid read status: {hit}")
    if not hit:
        if allow_miss:
            return False
        raise RuntimeError("unexpected below-capacity miss")
    if length != len(expected) or actual != expected:
        raise RuntimeError(
            f"wrong bytes/length: expected {len(expected)}, got {length}; "
            f"expected_sha256={hashlib.sha256(expected).hexdigest()} "
            f"actual_sha256={hashlib.sha256(actual).hexdigest()}")
    return True


class Client:
    """Own one C ABI handle; all ctypes backing storage lives through each call."""

    def __init__(self, lib, members, namespace):
        self.lib = lib
        options = make_client_options_v2(namespace, members=members,
                                         batch_concurrency=8)
        self.handle = lib.dfkv_open_v2(ct.byref(options))
        if not self.handle:
            raise RuntimeError("dfkv_open_v2 failed")
        self.transport = (lib.dfkv_transport_mode(self.handle) or b"").decode()
        if not self.transport:
            self.close()
            raise RuntimeError("empty transport mode")

    def close(self):
        if self.handle:
            self.lib.dfkv_close(self.handle)
            self.handle = None

    def put(self, keys, values):
        pointers, lengths, owners = make_key_array(keys)
        buffers = [ct.create_string_buffer(value, len(value)) for value in values]
        data = (ct.c_void_p * len(keys))(*[ct.addressof(b) for b in buffers])
        sizes = (ct.c_uint64 * len(keys))(*map(len, values))
        ok = (ct.c_int * len(keys))()
        rc = self.lib.dfkv_batch_put(self.handle, pointers, lengths, data,
                                     sizes, len(keys), ok)
        if rc != 0 or any(value != 1 for value in ok):
            raise RuntimeError(f"batch PUT failed: rc={rc}, slots={list(ok)}")

    def read(self, keys, expected, *, allow_miss=False, call_timing=None):
        pointers, lengths, owners = make_key_array(keys)
        # Extra capacity detects incorrect returned lengths without truncating them.
        buffers = [ct.create_string_buffer(len(value) + 1) for value in expected]
        data = (ct.c_void_p * len(keys))(*[ct.addressof(b) for b in buffers])
        caps = (ct.c_uint64 * len(keys))(*[ct.sizeof(b) for b in buffers])
        hits = (ct.c_int * len(keys))()
        sizes = (ct.c_uint64 * len(keys))()
        # Optional evidence brackets the native call, not Python buffer setup
        # or byte validation. ctypes releases the GIL during this CDLL call.
        if call_timing is not None:
            call_timing["call_started_sec"] = time.monotonic()
        try:
            rc = self.lib.dfkv_batch_get_auto(self.handle, pointers, lengths, data,
                                              caps, len(keys), hits, sizes)
        finally:
            if call_timing is not None:
                call_timing["call_finished_sec"] = time.monotonic()
        if rc != 0:
            raise RuntimeError(f"batch GET failed: rc={rc}")
        checked_hits = 0
        for index, (key, value, hit, buffer, size) in enumerate(
                zip(keys, expected, hits, buffers, sizes)):
            try:
                checked_hits += check_read(value, hit, buffer.raw[:size], size,
                                           allow_miss=allow_miss)
            except RuntimeError as error:
                raise RuntimeError(
                    f"key_hex={key.hex()} batch_index={index} "
                    f"expected_length={len(value)}: {error}") from error
        return checked_hits

    def read_outcome(self, key, expected):
        """One checked scalar GET; legacy no-hit remains unresolved.

        This is deliberately separate from the concurrent batch diagnostic path.
        Only the additive C status symbol can prove absence; errors never fall
        back to the bool API or to EXIST probes.
        """
        function = getattr(self.lib, "dfkv_get_auto_status", None)
        if function is None:
            hits = self.read([key], [expected], allow_miss=True)
            return "get_hit" if hits else "get_unresolved"
        pointer, owner = make_key_buffer(key)
        buffer = ct.create_string_buffer(len(expected) + 1)
        size = ct.c_uint64(0)
        status = function(self.handle, pointer, len(key), buffer,
                          ct.sizeof(buffer), ct.byref(size))
        if status != 0:  # DFKV_GET_OK
            if size.value != 0:
                raise RuntimeError("failed scalar GET returned a nonzero length")
            if status == 1:  # DFKV_GET_NOT_FOUND is the only trusted miss.
                return "get_miss"
            raise RuntimeError(f"scalar GET failed: status={status}")
        check_read(expected, 1, buffer.raw[:size.value], size.value)
        return "get_hit"

    def read_outcomes(self, keys, expected):
        """One checked batch, preserving slots and explicit absence when available.

        A failed native call raises for the whole batch: partial outputs must not
        become hits or misses. Per-item errors stay in the returned ledger so a
        valid sibling is not lost. Only a missing symbol selects the old bool
        API; its no-hit result remains unresolved, without retry/EXIST probes.
        The new symbol is direct-only: it bypasses opt-in dedup and may issue
        more I/O than the legacy batch API. No dedup/performance equivalence.
        """
        if not self.handle:
            raise RuntimeError("client is closed")
        keys, expected = list(keys), list(expected)
        if len(keys) != len(expected):
            raise ValueError("keys and expected payloads must have equal length")
        if len(keys) > (1 << 31) - 1:
            raise ValueError("batch count exceeds the C ABI int limit")
        for key, value in zip(keys, expected):
            if not isinstance(key, bytes) or not key:
                raise ValueError("batch keys must be non-empty bytes")
            if not isinstance(value, bytes):
                raise TypeError("expected payloads must be bytes")
        if not keys:
            return []
        pointers, lengths, owners = make_key_array(keys)
        buffers = [ct.create_string_buffer(len(value) + 1) for value in expected]
        data = (ct.c_void_p * len(keys))(*[ct.addressof(b) for b in buffers])
        caps = (ct.c_uint64 * len(keys))(*[ct.sizeof(b) for b in buffers])
        statuses = (ct.c_int * len(keys))(*([-999] * len(keys)))
        sizes = (ct.c_uint64 * len(keys))()
        function = getattr(self.lib, "dfkv_batch_get_auto_status", None)
        legacy = function is None
        if legacy:
            function = self.lib.dfkv_batch_get_auto
        rc = function(self.handle, pointers, lengths, data, caps, len(keys),
                      statuses, sizes)
        if rc != 0:
            raise RuntimeError(f"batch GET failed: rc={rc}")
        results = []
        for index, (value, buffer, status, size) in enumerate(
                zip(expected, buffers, statuses, sizes)):
            result = {} if legacy else {"status": status}
            try:
                hit = status == (1 if legacy else 0)
                if hit:
                    check_read(value, 1, buffer.raw[:size], size)
                    result["outcome"] = "get_hit"
                elif size != 0:
                    raise RuntimeError("failed batch GET returned a nonzero length")
                elif legacy and status == 0:
                    result["outcome"] = "get_unresolved"
                elif not legacy and status == 1:  # DFKV_GET_NOT_FOUND only.
                    result["outcome"] = "get_miss"
                else:
                    raise RuntimeError(f"batch GET failed: status={status}")
            except RuntimeError as error:
                result.update(outcome="client_error", error=f"batch_index={index}: {error}")
            results.append(result)
        return results

    def expect_missing(self, key):
        pointers, lengths, owners = make_key_array([key])
        buffer = ct.create_string_buffer(1)
        data = (ct.c_void_p * 1)(ct.addressof(buffer))
        caps = (ct.c_uint64 * 1)(1)
        hits, sizes = (ct.c_int * 1)(), (ct.c_uint64 * 1)()
        rc = self.lib.dfkv_batch_get_auto(self.handle, pointers, lengths, data,
                                          caps, 1, hits, sizes)
        if rc != 0 or hits[0] != 0:
            raise RuntimeError(f"expected missing key: rc={rc}, hit={hits[0]}")
        # GET_AUTO also reports a miss when a stored value exceeds capacity.
        # The metadata-only check is necessary to prove absence of larger values.
        exists = self.lib.dfkv_exist(self.handle, pointers[0], lengths[0])
        if exists != 0:
            raise RuntimeError(f"expected missing key: exists={exists}")

    def remove(self, key):
        pointer, owner = make_key_buffer(key)
        if self.lib.dfkv_remove(self.handle, pointer, len(key)) != 1:
            raise RuntimeError("REMOVE failed")


def load_library(path):
    lib = ct.CDLL(path)
    pointer_array = ct.POINTER(ct.c_void_p)
    size_array = ct.POINTER(ct.c_uint64)
    status_array = ct.POINTER(ct.c_int)
    signatures = {
        "dfkv_open_v2": ([ct.POINTER(DfkvClientOptionsV2)], ct.c_void_p),
        "dfkv_close": ([ct.c_void_p], None),
        "dfkv_transport_mode": ([ct.c_void_p], ct.c_char_p),
        "dfkv_remove": ([ct.c_void_p, ct.c_void_p, ct.c_uint64], ct.c_int),
        "dfkv_exist": ([ct.c_void_p, ct.c_void_p, ct.c_uint64], ct.c_int),
        "dfkv_batch_put": ([ct.c_void_p, pointer_array, size_array,
                             pointer_array, size_array, ct.c_int, status_array], ct.c_int),
        "dfkv_batch_get_auto": ([ct.c_void_p, pointer_array, size_array,
                                  pointer_array, size_array, ct.c_int,
                                  status_array, size_array], ct.c_int),
    }
    for name, (args, result) in signatures.items():
        function = getattr(lib, name)
        function.argtypes, function.restype = args, result
    status_get = getattr(lib, "dfkv_get_auto_status", None)
    if status_get is not None:
        status_get.argtypes = [ct.c_void_p, ct.c_void_p, ct.c_uint64,
                               ct.c_void_p, ct.c_uint64, size_array]
        status_get.restype = ct.c_int
    batch_status_get = getattr(lib, "dfkv_batch_get_auto_status", None)
    if batch_status_get is not None:
        batch_status_get.argtypes = signatures["dfkv_batch_get_auto"][0]
        batch_status_get.restype = ct.c_int
    return lib


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib", required=True)
    parser.add_argument("--members", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", required=True,
                        choices=("roundtrip", "write", "verify", "pressure", "quick-pressure", "sustained-pressure", "mixed-pressure", "arrivals-pressure", "multisize-pressure", "concurrent-pressure"))
    parser.add_argument("--seed", type=lambda s: int(s, 0), default=0xD1A60001)
    parser.add_argument("--count", type=int, help="objects PER namespace")
    parser.add_argument("--readers", type=int, choices=(0, 8), default=0,
                        help="optional stable roundtrip readers")
    parser.add_argument("--pressure-delay-sec", type=int, default=0,
                        help="quick-pressure: real wait after warmup (0..300 seconds)")
    parser.add_argument("--metrics-url", help="quick-pressure: optional phase metrics endpoint")
    parser.add_argument("--first-reuse-sec", type=int, choices=(0, 45, 90, 185), default=0,
                        help="sustained-pressure: defer the cohort first GET from pressure origin")
    parser.add_argument("--refill-on-miss", action="store_true",
                        help="arrivals-pressure: refill sampled first misses once, then verify once")
    args = parser.parse_args(argv)
    if not 0 <= args.seed < 2**64:
        parser.error("seed must fit uint64")
    if args.count is None:
        args.count = (QUICK_PREFILL + SUSTAIN_STEPS * BATCH_SIZE
                      if args.scenario in ("sustained-pressure", "mixed-pressure", "arrivals-pressure", "concurrent-pressure") else
                      MULTISIZE_COUNT if args.scenario == "multisize-pressure" else
                      6452 if args.scenario == "quick-pressure" else
                      26000 if args.scenario == "pressure" else len(LENGTHS))
    if args.count < (1 if args.scenario == "pressure" else len(LENGTHS)):
        parser.error("count must be positive and cover all nine length boundaries")
    if args.scenario == "quick-pressure" and args.count <= QUICK_PREFILL:
        parser.error("quick-pressure count must exceed prefill (4608 per namespace)")
    if not 0 <= args.pressure_delay_sec <= 300:
        parser.error("pressure-delay-sec must be in 0..300")
    if args.scenario in ("sustained-pressure", "mixed-pressure", "arrivals-pressure", "concurrent-pressure") and (
            args.pressure_delay_sec or args.count != QUICK_PREFILL + SUSTAIN_STEPS * BATCH_SIZE):
        parser.error(args.scenario + " requires fixed count and zero pressure delay")
    if args.scenario == "multisize-pressure" and (
            args.count != MULTISIZE_COUNT or args.pressure_delay_sec):
        parser.error("multisize-pressure requires fixed count and zero pressure delay")
    if (args.pressure_delay_sec or args.metrics_url) and args.scenario not in ("quick-pressure", "sustained-pressure", "mixed-pressure", "arrivals-pressure", "multisize-pressure", "concurrent-pressure"):
        parser.error("delay and phase metrics require quick-pressure")
    if args.first_reuse_sec and args.scenario != "sustained-pressure":
        parser.error("first-reuse-sec requires sustained-pressure")
    if args.refill_on_miss and args.scenario != "arrivals-pressure":
        parser.error("refill-on-miss requires arrivals-pressure")
    if args.readers and args.scenario != "roundtrip":
        parser.error("readers require roundtrip")
    return args


def record_phase(args, result, name, started):
    """Capture phase counters before moving on; HTTP metrics never heat keys."""
    row = {"name": name, "elapsed_sec": time.monotonic() - started,
           "hot_hits": result["hot_hits"], "hot_misses": result["hot_misses"],
           "filler_warmup_reads": result["filler_warmup_reads"]}
    if args.metrics_url:
        with urllib.request.urlopen(args.metrics_url, timeout=3) as response:
            (args.output / ("phase-" + name + ".prom")).write_bytes(response.read())
    result.setdefault("phases", []).append(row)
    (args.output / "phases.json").write_text(json.dumps(result["phases"], indent=2) + "\n")


def sustained_writes(args, clients, dataset, hot_reads, result, started):
    """Fixed-order read/write interleaving, paced without catch-up bursts.

    Missed hot objects are not reinserted. This measures retention, not an
    application's recomputation/refill policy or independently concurrent IO.
    """
    origin = time.monotonic()
    result["pressure_schedule_origin_sec"] = origin - started
    result["steps"] = []
    next_at = origin
    with (args.output / "steps.jsonl").open("x") as log:
        for step in range(SUSTAIN_STEPS):
            time.sleep(max(0, next_at - time.monotonic()))
            step_start = time.monotonic()
            next_at = step_start + SUSTAIN_INTERVAL_SEC
            before_hits, before_misses = result["hot_hits"], result["hot_misses"]
            start = QUICK_PREFILL + step * BATCH_SIZE
            for client, namespace in zip(clients, NAMESPACES):
                client.put(*dataset(namespace, start, start + BATCH_SIZE))
            probe_at = time.monotonic() - origin
            probe_started = probe_at if probe_at >= args.first_reuse_sec else None
            if probe_started is not None:
                hot_reads(True)
            row = {"step": step, "start_sec": step_start - origin,
                   "end_sec": time.monotonic() - origin,
                   "cold_written": len(NAMESPACES) * BATCH_SIZE,
                   "hot_probe_started_sec": probe_started,
                   "hot_hits": result["hot_hits"] - before_hits,
                   "hot_misses": result["hot_misses"] - before_misses}
            result["steps"].append(row)
            log.write(json.dumps(row) + "\n")
            log.flush()
            if (step + 1) % 40 == 0 or step + 1 == 180:
                record_phase(args, result, "step-" + str(step + 1).zfill(3), started)
    result["pressure_schedule_elapsed_sec"] = time.monotonic() - origin


def quick_pressure(args, clients, dataset, result, started):
    """Prefill one size class, warm a small hot set, then interleave cold writes.

    Protection is soft: an early read removes write BOOST, so hot survival is
    measured, not asserted. Every returned value must still match exact bytes.
    """
    def hot_reads(allow_miss):
        for index, (client, namespace) in enumerate(zip(clients, NAMESPACES)):
            keys, values = dataset(namespace, 0, QUICK_HOT)
            probe_started = time.monotonic() - started
            hits = client.read(keys, values, allow_miss=allow_miss)
            if args.first_reuse_sec and len(result["first_reuse"]) == index:
                result["first_reuse"].append({
                    "namespace_hex": namespace.hex(),
                    "probe_started_sec": probe_started,
                    "write_to_probe_sec": probe_started - result["hot_cohort_write_completed_sec"][index],
                    "hits": hits, "misses": QUICK_HOT - hits})
            result["hot_hits"] += hits
            result["hot_misses"] += QUICK_HOT - hits

    result.update(hot_hits=0, hot_misses=0, filler_warmup_reads=0,
                  first_reuse_sec=args.first_reuse_sec, first_reuse=[],
                  hot_cohort_write_completed_sec=[])
    for client, namespace in zip(clients, NAMESPACES):
        for start in range(0, QUICK_PREFILL, BATCH_SIZE):
            client.put(*dataset(namespace, start,
                                min(start + BATCH_SIZE, QUICK_PREFILL)))
            # The cohort spans two production batches; timestamp its last ACK.
            if start < QUICK_HOT <= start + BATCH_SIZE:
                result["hot_cohort_write_completed_sec"].append(time.monotonic() - started)
    record_phase(args, result, "prefill", started)
    if args.first_reuse_sec:
        # Warm competitors, never the pending first-reuse cohort. A missing
        # filler here invalidates the initial state rather than counting as heat.
        for _ in range(2):
            for client, namespace in zip(clients, NAMESPACES):
                for start in range(QUICK_HOT, QUICK_PREFILL, BATCH_SIZE):
                    stop = min(start + BATCH_SIZE, QUICK_PREFILL)
                    result["filler_warmup_reads"] += client.read(
                        *dataset(namespace, start, stop), allow_miss=False)
    else:
        for _ in range(2):
            hot_reads(False)
    record_phase(args, result, "warm", started)
    wait_started = time.monotonic()
    time.sleep(args.pressure_delay_sec)
    result["pressure_wait_elapsed_sec"] = time.monotonic() - wait_started
    record_phase(args, result, "before-pressure", started)
    if args.scenario == "sustained-pressure":
        sustained_writes(args, clients, dataset, hot_reads, result, started)
    else:
        for start in range(QUICK_PREFILL, args.count, BATCH_SIZE):
            for client, namespace in zip(clients, NAMESPACES):
                client.put(*dataset(namespace, start, min(start + BATCH_SIZE, args.count)))
            hot_reads(True)
    result["hot_reads"] = result["hot_hits"] + result["hot_misses"]
    record_phase(args, result, "after-pressure", started)


def multisize_pressure(args, clients, dataset, result, started):
    """Interleave homogeneous batches from two classes in a compact arena.

    Two active classes can steal extents even at low payload occupancy. Warmup
    therefore permits misses and stays separate from pressure hot reads; neither
    misses nor a successful PUT imply the object's later residence. No refill or
    independently concurrent reads are introduced by this diagnostic.
    """
    result.update(hot_hits=0, hot_misses=0, hot_reads=0, admission_hits=0,
                  admission_misses=0, warmup_hits=0,
                  warmup_misses=0, filler_warmup_reads=0,
                  multisize=[{"payload_bytes": length, "slot_size": (index + 1) * 1048576,
                              "put_attempts": 0, "put_acks": 0, "admission_hits": 0,
                              "admission_misses": 0, "warmup_hits": 0,
                              "warmup_misses": 0, "hot_hits": 0, "hot_misses": 0,
                              "scan_hits": 0, "scan_misses": 0}
                             for index, length in enumerate(MULTISIZE_LENGTHS)])

    def write_batch(client, namespace, start):
        row = result["multisize"][(start // BATCH_SIZE) % 2]
        keys, values = dataset(namespace, start, start + BATCH_SIZE)
        row["put_attempts"] += len(keys)
        client.put(keys, values)
        row["put_acks"] += len(keys)
        # Prove both byte paths before the next different-class PUT can steal
        # the newly bound extent. These probes intentionally heat initial keys.
        if start < 2 * BATCH_SIZE:
            read_batch(client, namespace, start, "admission")

    def read_batch(client, namespace, start, phase):
        row = result["multisize"][(start // BATCH_SIZE) % 2]
        hits = client.read(*dataset(namespace, start, start + BATCH_SIZE), allow_miss=True)
        row[phase + "_hits"] += hits
        row[phase + "_misses"] += BATCH_SIZE - hits
        result[phase + "_hits"] += hits
        result[phase + "_misses"] += BATCH_SIZE - hits

    def hot_reads(phase):
        for client, namespace in zip(clients, NAMESPACES):
            for start in range(0, QUICK_HOT, BATCH_SIZE):
                read_batch(client, namespace, start, phase)

    for client, namespace in zip(clients, NAMESPACES):
        for start in range(0, MULTISIZE_PREFILL, BATCH_SIZE):
            write_batch(client, namespace, start)
    record_phase(args, result, "prefill", started)
    for _ in range(2):
        hot_reads("warmup")
    record_phase(args, result, "warm", started)
    record_phase(args, result, "before-pressure", started)
    for start in range(MULTISIZE_PREFILL, args.count, BATCH_SIZE):
        for client, namespace in zip(clients, NAMESPACES):
            write_batch(client, namespace, start)
        hot_reads("hot")
    result["hot_reads"] = result["hot_hits"] + result["hot_misses"]
    record_phase(args, result, "after-pressure", started)


def refill_first_reads(client, namespace, first, keys, values, result, started):
    """Read each sampled key once; refill only a miss, without retrying errors.

    Acknowledged PUT is not proof of continued residence: verification misses are
    valid under pressure. A raised PUT may have committed, so its ACK stays false
    (unknown completion), not a claim that no device write happened. Append before
    GET and update immediately to preserve partial evidence on any exception.
    """
    stats = result["refill"]
    hits = 0
    for offset, (key, value) in enumerate(zip(keys, values)):
        row = {"namespace_hex": namespace.hex(), "index": first + offset,
               "first_get_started_sec": time.monotonic() - started,
               "first_hit": None, "put_attempted": False, "put_acked": False,
               "verify_hit": None}
        stats["objects"].append(row)
        found = client.read([key], [value], allow_miss=True)
        row["first_hit"] = found
        stats["first_hits"] += found
        stats["first_misses"] += 1 - found
        hits += found
        if found:
            continue
        row["put_attempted"] = True
        stats["put_attempts"] += 1
        stats["attempted_payload_bytes"] += len(value)
        client.put([key], [value])
        row["put_acked"] = True
        row["put_ack_sec"] = time.monotonic() - started
        stats["put_acks"] += 1
        stats["acked_payload_bytes"] += len(value)
        row["verify_started_sec"] = time.monotonic() - started
        verified = client.read([key], [value], allow_miss=True)
        row["verify_hit"] = verified
        stats["verify_hits"] += verified
        stats["verify_misses"] += 1 - verified
    return hits


def mixed_pressure(args, clients, dataset, result, started):
    """Probe existing hot objects and once-only delayed cohorts under cold writes.

    Delayed objects are never warmed. Refill is opt-in for arrivals only. First
    outcomes stay separate from repeated hot reads and the final full scan. The arrivals mode
    samples only the first twelve arriving batches, not the full arrival stream;
    their deadlines use the later namespace PUT ACK instead of pressure origin.
    """
    arrivals = args.scenario == "arrivals-pressure"
    if args.refill_on_miss:
        result["refill"] = {"first_hits": 0, "first_misses": 0,
                            "put_attempts": 0, "put_acks": 0,
                            "attempted_payload_bytes": 0, "acked_payload_bytes": 0,
                            "verify_hits": 0, "verify_misses": 0, "objects": []}
    cohorts = [] if arrivals else [{"delay_sec": delay,
                "start_index": QUICK_HOT + index * MIXED_COHORT_SIZE,
                "count_per_namespace": MIXED_COHORT_SIZE,
                "write_completed_sec": [], "first_reuse": []}
               for index, delay in enumerate(MIXED_REUSE_DELAYS)]
    filler_start = QUICK_HOT + len(cohorts) * MIXED_COHORT_SIZE
    result.update(hot_hits=0, hot_misses=0, filler_warmup_reads=0,
                  reuse_hits=0, reuse_misses=0, reuse_reads=0,
                  reuse_cohorts=cohorts)

    def hot_reads(allow_miss):
        for client, namespace in zip(clients, NAMESPACES):
            hits = client.read(*dataset(namespace, 0, QUICK_HOT), allow_miss=allow_miss)
            result["hot_hits"] += hits
            result["hot_misses"] += QUICK_HOT - hits

    for client, namespace in zip(clients, NAMESPACES):
        for start in range(0, QUICK_PREFILL, BATCH_SIZE):
            client.put(*dataset(namespace, start, min(start + BATCH_SIZE, QUICK_PREFILL)))
            for cohort in cohorts:
                # Each cohort occupies one aligned batch in the frozen layout.
                if start == cohort["start_index"]:
                    cohort["write_completed_sec"].append(time.monotonic() - started)
    record_phase(args, result, "prefill", started)
    for _ in range(2):
        hot_reads(False)
    for _ in range(2):
        for client, namespace in zip(clients, NAMESPACES):
            for start in range(filler_start, QUICK_PREFILL, BATCH_SIZE):
                stop = min(start + BATCH_SIZE, QUICK_PREFILL)
                result["filler_warmup_reads"] += client.read(
                    *dataset(namespace, start, stop), allow_miss=False)
    record_phase(args, result, "warm", started)
    record_phase(args, result, "before-pressure", started)

    origin = time.monotonic()
    result["pressure_schedule_origin_sec"] = origin - started
    result["steps"] = []
    next_at = origin
    with (args.output / "steps.jsonl").open("x") as log:
        for step in range(SUSTAIN_STEPS):
            time.sleep(max(0, next_at - time.monotonic()))
            step_start = time.monotonic()
            next_at = step_start + SUSTAIN_INTERVAL_SEC
            before_hits, before_misses = result["hot_hits"], result["hot_misses"]
            start = QUICK_PREFILL + step * BATCH_SIZE
            arrival = None
            if arrivals and step < ARRIVAL_SAMPLE_STEPS:
                delay = MIXED_REUSE_DELAYS[step % len(MIXED_REUSE_DELAYS)]
                arrival = {"admission_step": step, "delay_sec": delay,
                           "start_index": start, "count_per_namespace": BATCH_SIZE,
                           "write_completed_sec": [], "first_reuse": []}
                # Append before PUT so failed admission retains partial ACKs.
                cohorts.append(arrival)
            for client, namespace in zip(clients, NAMESPACES):
                client.put(*dataset(namespace, start, start + BATCH_SIZE))
                if arrival is not None:
                    arrival["write_completed_sec"].append(time.monotonic() - started)
            if arrival is not None:
                # Both namespaces must reach the requested ACK-to-first-GET age.
                arrival["due_sec"] = (max(arrival["write_completed_sec"]) -
                                      result["pressure_schedule_origin_sec"] +
                                      arrival["delay_sec"])
            hot_reads(True)
            probes = []
            for cohort in cohorts:
                probe_at = time.monotonic() - origin
                due = cohort["due_sec"] if arrivals else cohort["delay_sec"]
                if cohort["first_reuse"] or probe_at < due:
                    continue
                hits = 0
                for index, (client, namespace) in enumerate(zip(clients, NAMESPACES)):
                    first = cohort["start_index"]
                    keys, values = dataset(namespace, first, first + MIXED_COHORT_SIZE)
                    probe_started = time.monotonic() - started
                    found = (refill_first_reads(client, namespace, first, keys, values,
                                                result, started) if args.refill_on_miss else
                             client.read(keys, values, allow_miss=True))
                    cohort["first_reuse"].append({
                        "namespace_hex": namespace.hex(), "probe_started_sec": probe_started,
                        "write_to_probe_sec": probe_started - cohort["write_completed_sec"][index],
                        "hits": found, "misses": MIXED_COHORT_SIZE - found})
                    hits += found
                reads = len(NAMESPACES) * MIXED_COHORT_SIZE
                result["reuse_hits"] += hits
                result["reuse_misses"] += reads - hits
                result["reuse_reads"] += reads
                event = {"delay_sec": cohort["delay_sec"], "probe_started_sec": probe_at,
                         "hits": hits, "misses": reads - hits}
                if arrivals:
                    event["admission_step"] = cohort["admission_step"]
                probes.append(event)
            row = {"step": step, "start_sec": step_start - origin,
                   "end_sec": time.monotonic() - origin,
                   "cold_written": len(NAMESPACES) * BATCH_SIZE,
                   "hot_hits": result["hot_hits"] - before_hits,
                   "hot_misses": result["hot_misses"] - before_misses,
                   "reuse_probes": probes}
            result["steps"].append(row)
            log.write(json.dumps(row) + "\n")
            log.flush()
            if (step + 1) % 40 == 0 or step + 1 == 180:
                record_phase(args, result, "step-" + str(step + 1).zfill(3), started)
    result["pressure_schedule_elapsed_sec"] = time.monotonic() - origin
    if ((arrivals and len(cohorts) != ARRIVAL_SAMPLE_STEPS) or
            any(len(cohort["first_reuse"]) != len(NAMESPACES) for cohort in cohorts)):
        mode = "arrivals" if arrivals else "mixed"
        raise RuntimeError(mode + " schedule did not reach every first reuse cohort")
    result["hot_reads"] = result["hot_hits"] + result["hot_misses"]
    if args.refill_on_miss:
        # Only a complete schedule proves all baseline PUTs were acknowledged.
        stats = result["refill"]
        baseline = args.count * len(NAMESPACES) * 4096
        stats["baseline_payload_bytes"] = baseline
        stats["logical_payload_write_amplification"] = (
            1 + stats["acked_payload_bytes"] / baseline)
    record_phase(args, result, "after-pressure", started)


def concurrent_pressure(args, clients, dataset, result, started, lib, client_factory):
    """Eight private reader handles overlap paced writes, without miss refill.

    Logs and counters belong to individual workers. Join every worker before
    its handle can close; Python cancellation cannot interrupt a blocked C ABI
    call, so the outer supervisor retains its process-level timeout.
    """
    result.update(hot_hits=0, hot_misses=0, hot_reads=0, filler_warmup_reads=0)
    stats = {"warmup_reads": 0, "readers": [], "writer_steps": []}
    result["concurrency"] = stats
    for client, namespace in zip(clients, NAMESPACES):
        for first in range(0, QUICK_PREFILL, BATCH_SIZE):
            client.put(*dataset(namespace, first, first + BATCH_SIZE))
    record_phase(args, result, "prefill", started)
    for _ in range(2):
        for client, namespace in zip(clients, NAMESPACES):
            for first in range(0, CONCURRENT_HOT, BATCH_SIZE):
                stats["warmup_reads"] += client.read(
                    *dataset(namespace, first, first + BATCH_SIZE))
    result["hot_hits"] = stats["warmup_reads"]
    result["hot_reads"] = stats["warmup_reads"]
    record_phase(args, result, "warm", started)
    stop = threading.Event()
    barrier = threading.Barrier(CONCURRENT_READERS + 1, timeout=10)
    deadline = None

    def reader(client, namespace, row):
        try:
            with (args.output / f'concurrent-reader-{row["reader_id"]:02d}.jsonl').open("x") as log:
                barrier.wait()
                for batch in range(CONCURRENT_MAX_BATCHES):
                    if stop.is_set():
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError("concurrent reader soft deadline exceeded")
                    first = row["start_index"] + batch % 2 * BATCH_SIZE
                    keys, values = dataset(namespace, first, first + BATCH_SIZE)
                    timing = {}
                    call = {"batch": batch, "start_index": first}
                    try:
                        hits = client.read(keys, values, allow_miss=True, call_timing=timing)
                        call.update({key: value - started for key, value in timing.items()})
                        call.update(hits=hits, misses=BATCH_SIZE - hits)
                    except Exception as error:
                        call.update({key: value - started for key, value in timing.items()})
                        call["error"] = str(error)
                        log.write(json.dumps(call) + "\n")
                        log.flush()
                        raise
                    row["batches"].append(call)
                    row["hits"] += hits
                    row["misses"] += BATCH_SIZE - hits
                    log.write(json.dumps(call) + "\n")
                    log.flush()
                else:
                    if not stop.is_set():
                        raise RuntimeError("concurrent reader batch limit exhausted")
                row["status"] = "passed"
        except Exception as error:
            row["error"] = str(error)
            stop.set()
            barrier.abort()
            raise

    # Open all handles before launching threads. The pool exits (and joins)
    # before this nested ExitStack releases any independently owned handle.
    with ExitStack() as reader_stack:
        readers = []
        for index in range(CONCURRENT_READERS):
            namespace = NAMESPACES[index % len(NAMESPACES)]
            client = client_factory(lib, args.members, namespace)
            reader_stack.callback(client.close)
            readers.append((client, namespace))
            stats["readers"].append({"reader_id": index, "namespace_hex": namespace.hex(),
                                     "start_index": index // 2 * CONCURRENT_WINDOW,
                                     "count": CONCURRENT_WINDOW, "transport_mode": client.transport,
                                     "status": "failed", "hits": 0, "misses": 0, "batches": []})
        record_phase(args, result, "before-pressure", started)
        writer_error, reader_errors = None, []
        with ThreadPoolExecutor(max_workers=CONCURRENT_READERS) as pool:
            futures = []
            try:
                for (client, namespace), row in zip(readers, stats["readers"]):
                    futures.append(pool.submit(reader, client, namespace, row))
                origin = time.monotonic()
                stats["pressure_started_sec"] = origin - started
                deadline = origin + CONCURRENT_SOFT_DEADLINE_SEC
                barrier.wait()
                next_at = origin
                with (args.output / "concurrent-writer.jsonl").open("x") as log:
                    for step in range(SUSTAIN_STEPS):
                        if stop.wait(max(0, next_at - time.monotonic())):
                            raise RuntimeError("concurrent reader aborted writer")
                        if time.monotonic() > deadline:
                            raise RuntimeError("concurrent writer soft deadline exceeded")
                        at = time.monotonic()
                        next_at = at + CONCURRENT_INTERVAL_SEC
                        row = {"step": step, "start_sec": at - started,
                               "put_attempts": 0, "put_acks": 0}
                        stats["writer_steps"].append(row)
                        first = QUICK_PREFILL + step * BATCH_SIZE
                        try:
                            for client, namespace in zip(clients, NAMESPACES):
                                row["put_attempts"] += BATCH_SIZE
                                client.put(*dataset(namespace, first, first + BATCH_SIZE))
                                row["put_acks"] += BATCH_SIZE
                        finally:
                            row["end_sec"] = time.monotonic() - started
                            log.write(json.dumps(row) + "\n")
                            log.flush()
                        if step == 99:
                            record_phase(args, result, "step-100", started)
                            stats["midpoint_sec"] = time.monotonic() - started
                    if time.monotonic() > deadline:
                        raise RuntimeError("concurrent writer soft deadline exceeded")
            except Exception as error:
                writer_error = error
            finally:
                stats["pressure_finished_sec"] = time.monotonic() - started
                stop.set()
                barrier.abort()
                for future in futures:
                    try:
                        future.result()
                    except Exception as error:
                        reader_errors.append(error)
                result["hot_hits"] += sum(row["hits"] for row in stats["readers"])
                result["hot_misses"] = sum(row["misses"] for row in stats["readers"])
                result["hot_reads"] = result["hot_hits"] + result["hot_misses"]
                if any("wrong bytes/length" in row.get("error", "") for row in stats["readers"]):
                    result["wrong_bytes"] = 1
            if reader_errors and (writer_error is None or
                                  isinstance(writer_error, threading.BrokenBarrierError) or
                                  "reader aborted" in str(writer_error)):
                # Abort can wake peers with BrokenBarrierError; retain the
                # originating IO/oracle error rather than its empty wakeup.
                raise next((error for error in reader_errors
                            if not isinstance(error, threading.BrokenBarrierError)), reader_errors[0])
            if writer_error is not None:
                raise writer_error
    record_phase(args, result, "after-pressure", started)


def run(args, *, library_loader=load_library, client_factory=Client):
    args.output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    pressure = args.scenario in ("pressure", "quick-pressure", "sustained-pressure", "mixed-pressure", "arrivals-pressure", "multisize-pressure", "concurrent-pressure")
    config = {"schema": 1, "lib": args.lib, "members": args.members,
              "scenario": args.scenario, "seed": args.seed, "count": args.count,
              "readers": args.readers, "namespaces_hex": [n.hex() for n in NAMESPACES],
              "lengths": [4096] if pressure else list(LENGTHS),
              "batch_size": BATCH_SIZE, "generator": "shake256-disk-synthetic-v1"}
    if args.scenario in ("quick-pressure", "sustained-pressure"):
        config.update(prefill=QUICK_PREFILL, hot=QUICK_HOT,
                      warmup_rounds=0 if args.first_reuse_sec else 2,
                      filler_warmup_rounds=2 if args.first_reuse_sec else 0,
                      first_reuse_sec=args.first_reuse_sec,
                      pressure_delay_sec=args.pressure_delay_sec, metrics_url=args.metrics_url)
    if args.scenario in ("sustained-pressure", "mixed-pressure", "arrivals-pressure"):
        config.update(steps=SUSTAIN_STEPS, step_interval_sec=SUSTAIN_INTERVAL_SEC,
                      refill_on_miss=False, independent_read_threads=False)
    if args.scenario == "mixed-pressure":
        config.update(prefill=QUICK_PREFILL, hot=QUICK_HOT, warmup_rounds=2,
                      filler_start=QUICK_HOT + len(MIXED_REUSE_DELAYS) * MIXED_COHORT_SIZE,
                      filler_warmup_rounds=2, metrics_url=args.metrics_url,
                      reuse_cohorts=[{"delay_sec": delay,
                                      "start_index": QUICK_HOT + index * MIXED_COHORT_SIZE,
                                      "count_per_namespace": MIXED_COHORT_SIZE}
                                     for index, delay in enumerate(MIXED_REUSE_DELAYS)],
                      reuse_once=True, pressure_delay_sec=0, first_reuse_sec=0)
    if args.scenario == "arrivals-pressure":
        config.update(prefill=QUICK_PREFILL, hot=QUICK_HOT, warmup_rounds=2,
                      filler_start=QUICK_HOT, filler_warmup_rounds=2,
                      metrics_url=args.metrics_url, sampled_arrival_steps=ARRIVAL_SAMPLE_STEPS,
                      reuse_delays_sec=list(MIXED_REUSE_DELAYS),
                      reuse_delay_origin="later-namespace-put-ack",
                      reuse_once=True, pressure_delay_sec=0, first_reuse_sec=0)
    if args.scenario == "multisize-pressure":
        config.update(lengths=list(MULTISIZE_LENGTHS),
                      length_layout="alternating-homogeneous-16-key-batches",
                      prefill=MULTISIZE_PREFILL, hot=QUICK_HOT, warmup_rounds=2,
                      warmup_allow_miss=True, admission_read_batches_per_namespace=2,
                      refill_on_miss=False,
                      independent_read_threads=False, metrics_url=args.metrics_url)
    if args.scenario == "concurrent-pressure":
        config.update(prefill=QUICK_PREFILL, hot=CONCURRENT_HOT, warmup_rounds=2,
                      independent_readers=CONCURRENT_READERS, reader_window=CONCURRENT_WINDOW,
                      steps=SUSTAIN_STEPS, step_interval_sec=CONCURRENT_INTERVAL_SEC,
                      max_reader_batches=CONCURRENT_MAX_BATCHES,
                      soft_deadline_sec=CONCURRENT_SOFT_DEADLINE_SEC,
                      independent_read_threads=True, refill_on_miss=False,
                      timing_origin="driver-start", metrics_url=args.metrics_url)
    if args.refill_on_miss:
        config.update(refill_on_miss=True, refill_scope="sampled-first-reuse-only",
                      refill_attempts_per_miss=1, refill_verification_reads=1,
                      first_reuse_get_batch_size=1, business_weights=None)
    encoded = json.dumps(config, sort_keys=True).encode()
    config_hash = hashlib.sha256(encoded).hexdigest()
    # A compact replay trace describes every index/generation before any C ABI call.
    trace = {"config": config, "config_sha256": config_hash,
             "operations": ["indices [0,count) in both namespaces; generation=0",
                            "write unless verify; read unless write",
                            "roundtrip: never-written key must miss; duplicate gen1 PUT keeps gen0",
                            "roundtrip: remove index0, assert miss, reinsert gen1",
                            "pressure: only post-write reads allow misses",
                            "quick-pressure: prefill both namespaces; two strict hot warmups; "
                            "interleave cold batches and hot probes; final full scan",
                            "sustained-pressure: fixed paced cold/hot steps; no catch-up or refill",
                            "first-reuse-sec>0: warm only filler [hot,prefill) twice; "
                            "cohort [0,hot) gets no GET before the requested pressure-origin delay",
                            "mixed-pressure: warm hot/filler only; each delayed cohort gets one "
                            "first GET when actual pressure time is due; no refill"]}
    if args.scenario == "arrivals-pressure":
        trace["operations"].append(
            "arrivals-pressure: sample only the first 12 arriving batches; rotate 45/90/185 "
            "seconds from the later namespace PUT ACK; once-only GET, no refill; "
            "later arrivals remain unsampled, incomplete cohorts fail")
    if args.scenario == "multisize-pressure":
        trace["operations"].append(
            "multisize-pressure: alternate 16-key homogeneous 4096/1048577-byte batches; "
            "probe each namespace first two admitted batches before the next PUT; "
            "warmup permits donor losses and is separate from pressure hot reads; "
            "per-size PUT/warmup/hot/scan counts, no refill")
    if args.scenario == "concurrent-pressure":
        trace["operations"].append(
            "concurrent-pressure: strict warmup of 128 keys per namespace; eight new "
            "independent reader handles rotate private 32-key windows while two writers "
            "ACK 200 paced cold batches; native GET timing and private per-worker journals; "
            "join readers before closing handles, no refill")
    if args.refill_on_miss:
        # Replace the no-refill arrival description only for this opt-in trace.
        trace["operations"][-1] = (
            "arrivals-pressure refill: ACK-aged cohorts, sequential single-key first GET; "
            "each miss gets one same-byte PUT and one verification GET; no retries; "
            "later first outcomes are conditional on earlier refills; hot/filler/scan never refill")
    (args.output / "trace.json").write_text(json.dumps(trace, indent=2) + "\n")
    result = {"config_sha256": config_hash, "status": "failed", "hits": 0,
              "misses": 0, "wrong_bytes": 0, "transport_mode": []}

    def dataset(namespace, start, stop, changed=False):
        keys = [key_for(args.seed, i) for i in range(start, stop)]
        values = [payload(args.seed, namespace, key, int(changed and i == 0),
                          MULTISIZE_LENGTHS[(i // BATCH_SIZE) % 2]
                          if args.scenario == "multisize-pressure" else
                          4096 if pressure else LENGTHS[i % len(LENGTHS)])
                  for i, key in zip(range(start, stop), keys)]
        return keys, values

    def read_all(client, namespace, changed=False):
        hits = 0
        for start in range(0, args.count, BATCH_SIZE):
            keys, values = dataset(namespace, start, min(start + BATCH_SIZE, args.count), changed)
            found = client.read(keys, values, allow_miss=pressure)
            hits += found
            if args.scenario == "multisize-pressure":
                row = result["multisize"][(start // BATCH_SIZE) % 2]
                row["scan_hits"] += found
                row["scan_misses"] += len(keys) - found
        return hits

    try:
        lib = library_loader(args.lib)
        with ExitStack() as stack:
            clients = []
            for namespace in NAMESPACES:
                client = client_factory(lib, args.members, namespace)
                stack.callback(client.close)
                clients.append(client)
                result["transport_mode"].append(client.transport)
            if args.scenario == "roundtrip":
                for client in clients:
                    client.expect_missing(b"never-written\x00" + key_for(args.seed, 0))
            if args.scenario == "concurrent-pressure":
                concurrent_pressure(args, clients, dataset, result, started, lib, client_factory)
            elif args.scenario == "multisize-pressure":
                multisize_pressure(args, clients, dataset, result, started)
            elif args.scenario in ("mixed-pressure", "arrivals-pressure"):
                mixed_pressure(args, clients, dataset, result, started)
            elif args.scenario in ("quick-pressure", "sustained-pressure"):
                quick_pressure(args, clients, dataset, result, started)
            elif args.scenario != "verify":
                for client, namespace in zip(clients, NAMESPACES):
                    for start in range(0, args.count, BATCH_SIZE):
                        client.put(*dataset(namespace, start, min(start + BATCH_SIZE, args.count)))
            if args.scenario != "write":
                for client, namespace in zip(clients, NAMESPACES):
                    hits = read_all(client, namespace)
                    result["hits"] += hits
                    result["misses"] += args.count - hits
                if pressure and result["hits"] == 0:
                    raise RuntimeError("pressure had no successful reads; unavailable is not eviction proof")
            if args.scenario == "roundtrip":
                for client, namespace in zip(clients, NAMESPACES):
                    key = key_for(args.seed, 0)
                    original = payload(args.seed, namespace, key, 0, LENGTHS[0])
                    updated = payload(args.seed, namespace, key, 1, LENGTHS[0])
                    client.put([key], [updated])
                    client.read([key], [original])
                    client.remove(key)
                    client.expect_missing(key)
                    client.put([key], [updated])
                    client.read([key], [updated])
                # No writers exist while readers check the immutable gen1 oracle.
                if args.readers:
                    with ThreadPoolExecutor(max_workers=args.readers) as pool:
                        futures = [pool.submit(read_all, clients[i % 2], NAMESPACES[i % 2], True)
                                   for i in range(args.readers)]
                        result["concurrent_reader_hits"] = sum(f.result() for f in futures)
            if args.scenario in ("quick-pressure", "sustained-pressure", "mixed-pressure", "arrivals-pressure", "multisize-pressure", "concurrent-pressure"):
                record_phase(args, result, "after-scan", started)
            result["status"] = "passed"
    except Exception as error:
        result["error"] = str(error)
        if "wrong bytes/length" in str(error):
            result["wrong_bytes"] = 1
        raise
    finally:
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    run(parse_args())
