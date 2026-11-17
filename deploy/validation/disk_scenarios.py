#!/usr/bin/env python3
"""External-client correctness diagnostics; never starts or resets a server.

Run each case with a fresh --output directory. write/verify share a deterministic
initial dataset for clean-restart checks. pressure permits eviction misses only
AFTER all writes; quick-pressure also probes hot keys during cold writes, after
a strict warmup. sustained-pressure adds paced interleaving without catch-up or
miss refill. These are not persistence, performance, or transport-backend proof.
Requires the repository's dfkv_common on PYTHONPATH (no third-party dependencies).
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import ctypes as ct
import hashlib
import json
from pathlib import Path
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

    def read(self, keys, expected, *, allow_miss=False):
        pointers, lengths, owners = make_key_array(keys)
        # Extra capacity detects incorrect returned lengths without truncating them.
        buffers = [ct.create_string_buffer(len(value) + 1) for value in expected]
        data = (ct.c_void_p * len(keys))(*[ct.addressof(b) for b in buffers])
        caps = (ct.c_uint64 * len(keys))(*[ct.sizeof(b) for b in buffers])
        hits = (ct.c_int * len(keys))()
        sizes = (ct.c_uint64 * len(keys))()
        rc = self.lib.dfkv_batch_get_auto(self.handle, pointers, lengths, data,
                                          caps, len(keys), hits, sizes)
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
    return lib


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib", required=True)
    parser.add_argument("--members", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", required=True,
                        choices=("roundtrip", "write", "verify", "pressure", "quick-pressure", "sustained-pressure"))
    parser.add_argument("--seed", type=lambda s: int(s, 0), default=0xD1A60001)
    parser.add_argument("--count", type=int, help="objects PER namespace")
    parser.add_argument("--readers", type=int, choices=(0, 8), default=0,
                        help="optional stable roundtrip readers")
    parser.add_argument("--pressure-delay-sec", type=int, default=0,
                        help="quick-pressure: real wait after warmup (0..300 seconds)")
    parser.add_argument("--metrics-url", help="quick-pressure: optional phase metrics endpoint")
    args = parser.parse_args(argv)
    if not 0 <= args.seed < 2**64:
        parser.error("seed must fit uint64")
    if args.count is None:
        args.count = (QUICK_PREFILL + SUSTAIN_STEPS * BATCH_SIZE
                      if args.scenario == "sustained-pressure" else
                      6452 if args.scenario == "quick-pressure" else
                      26000 if args.scenario == "pressure" else len(LENGTHS))
    if args.count < (1 if args.scenario == "pressure" else len(LENGTHS)):
        parser.error("count must be positive and cover all nine length boundaries")
    if args.scenario == "quick-pressure" and args.count <= QUICK_PREFILL:
        parser.error("quick-pressure count must exceed prefill (4608 per namespace)")
    if not 0 <= args.pressure_delay_sec <= 300:
        parser.error("pressure-delay-sec must be in 0..300")
    if args.scenario == "sustained-pressure" and (
            args.pressure_delay_sec or args.count != QUICK_PREFILL + SUSTAIN_STEPS * BATCH_SIZE):
        parser.error("sustained-pressure requires fixed count and zero pressure delay")
    if (args.pressure_delay_sec or args.metrics_url) and args.scenario not in ("quick-pressure", "sustained-pressure"):
        parser.error("delay and phase metrics require quick-pressure")
    if args.readers and args.scenario != "roundtrip":
        parser.error("readers require roundtrip")
    return args


def record_phase(args, result, name, started):
    """Capture phase counters before moving on; HTTP metrics never heat keys."""
    row = {"name": name, "elapsed_sec": time.monotonic() - started,
           "hot_hits": result["hot_hits"], "hot_misses": result["hot_misses"]}
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
            hot_reads(True)
            row = {"step": step, "start_sec": step_start - origin,
                   "end_sec": time.monotonic() - origin,
                   "cold_written": len(NAMESPACES) * BATCH_SIZE,
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
        for client, namespace in zip(clients, NAMESPACES):
            hits = client.read(*dataset(namespace, 0, QUICK_HOT),
                               allow_miss=allow_miss)
            result["hot_hits"] += hits
            result["hot_misses"] += QUICK_HOT - hits

    result.update(hot_hits=0, hot_misses=0)
    for client, namespace in zip(clients, NAMESPACES):
        for start in range(0, QUICK_PREFILL, BATCH_SIZE):
            client.put(*dataset(namespace, start,
                                min(start + BATCH_SIZE, QUICK_PREFILL)))
    record_phase(args, result, "prefill", started)
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


def run(args, *, library_loader=load_library, client_factory=Client):
    args.output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    pressure = args.scenario in ("pressure", "quick-pressure", "sustained-pressure")
    config = {"schema": 1, "lib": args.lib, "members": args.members,
              "scenario": args.scenario, "seed": args.seed, "count": args.count,
              "readers": args.readers, "namespaces_hex": [n.hex() for n in NAMESPACES],
              "lengths": [4096] if pressure else list(LENGTHS),
              "batch_size": BATCH_SIZE, "generator": "shake256-disk-synthetic-v1"}
    if args.scenario in ("quick-pressure", "sustained-pressure"):
        config.update(prefill=QUICK_PREFILL, hot=QUICK_HOT, warmup_rounds=2,
                      pressure_delay_sec=args.pressure_delay_sec, metrics_url=args.metrics_url)
    if args.scenario == "sustained-pressure":
        config.update(steps=SUSTAIN_STEPS, step_interval_sec=SUSTAIN_INTERVAL_SEC,
                      refill_on_miss=False, independent_read_threads=False)
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
                            "sustained-pressure: fixed paced cold/hot steps; no catch-up or refill"]}
    (args.output / "trace.json").write_text(json.dumps(trace, indent=2) + "\n")
    result = {"config_sha256": config_hash, "status": "failed", "hits": 0,
              "misses": 0, "wrong_bytes": 0, "transport_mode": []}

    def dataset(namespace, start, stop, changed=False):
        keys = [key_for(args.seed, i) for i in range(start, stop)]
        values = [payload(args.seed, namespace, key, int(changed and i == 0),
                          4096 if pressure else LENGTHS[i % len(LENGTHS)])
                  for i, key in zip(range(start, stop), keys)]
        return keys, values

    def read_all(client, namespace, changed=False):
        hits = 0
        for start in range(0, args.count, BATCH_SIZE):
            keys, values = dataset(namespace, start, min(start + BATCH_SIZE, args.count), changed)
            hits += client.read(keys, values, allow_miss=pressure)
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
            if args.scenario in ("quick-pressure", "sustained-pressure"):
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
            if args.scenario in ("quick-pressure", "sustained-pressure"):
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
