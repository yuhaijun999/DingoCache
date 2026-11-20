#!/usr/bin/env python3
"""Fixed child for an owned server-stop / same-handle CPU TTL boundary.

Only run_cpu_dedup_validation owns/stops the server. This child accepts no PID
or command, waits for its owner's acknowledgement and never unlinks shared data.
"""

import hashlib
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_dedup as dedup
import eviction_trace as trace


def payload():
    return b"\0" + hashlib.shake_256(b"cpu-ttl-boundary-v1").digest(63)


def verify(result, config):
    fields = {"config", "before", "after", "warm", "local_after_stop", "cold_after_stop",
              "warm_to_local_ns", "passed"}
    if not isinstance(result, dict) or set(result) != fields or trace.encode(result["config"]) != trace.encode(config):
        raise ValueError("boundary schema/config changed")
    zero = dict.fromkeys(dedup.COUNTERS, 0)
    after = dict(zero, fetches=2, hits=1)
    if (trace.encode(result["before"]) != trace.encode(zero)
            or trace.encode(result["after"]) != trace.encode(after)
            or trace.encode(result["warm"]) != trace.encode({"outcome": "get_hit", "status": 0})
            or trace.encode(result["local_after_stop"]) != trace.encode({"outcome": "get_hit", "status": 0})
            or not isinstance(result["cold_after_stop"], dict)
            or set(result["cold_after_stop"]) != {"outcome", "status", "error"}
            or result["cold_after_stop"]["outcome"] != "client_error"
            or type(result["cold_after_stop"]["status"]) is not int
            or result["cold_after_stop"]["status"] != 4
            or not isinstance(result["cold_after_stop"]["error"], str)
            or not result["cold_after_stop"]["error"]):
        raise ValueError("boundary outcomes/counters do not match")
    elapsed = result["warm_to_local_ns"]
    if type(elapsed) is not int or not 0 <= elapsed < 25_000_000_000 or result["passed"] is not True:
        raise ValueError("TTL boundary not within the frozen safe window")
    return after


def child(output):
    output = Path(output)
    config = trace.load(output / "config.json")
    actual_environment = {k: v for k, v in os.environ.items() if k.startswith("DFKV_")}
    if actual_environment != config["client_environment"]:
        raise ValueError("boundary client environment differs from frozen plan")
    library = Path(config["lib"])
    if hashlib.sha256(library.read_bytes()).hexdigest() != config["lib_sha256"]:
        raise ValueError("boundary library changed")
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "integration/common/src"))
    from disk_scenarios import Client, load_library
    lib = load_library(str(library))
    namespace = dedup.namespace(config["token"], bytes.fromhex(config["namespace_hex"]))
    value = payload()
    client = Client(lib, config["members"], namespace)
    try:
        before = dedup.snapshot(lib, client.handle)
        if before != dict.fromkeys(dedup.COUNTERS, 0):
            raise ValueError("boundary CPU not fresh")
        client.put([b"warm\0"], [value])
        warm = client.read_deduplicated_outcomes([b"warm\0"], [value])[0]
        if warm != {"outcome": "get_hit", "status": 0}:
            raise ValueError("boundary warm failed")
        warmed = time.monotonic_ns()
        ready = {"config": config, "warm": warm, "before": before}
        temporary = output / "ready.tmp"
        temporary.write_bytes(trace.encode(ready))
        temporary.rename(output / "ready.json")
        deadline = time.monotonic() + 20
        while not (output / "stop-ack.json").is_file():
            if time.monotonic() >= deadline:
                raise TimeoutError("owned server stop acknowledgement deadline")
            time.sleep(.02)
        if trace.encode(trace.load(output / "stop-ack.json")) != trace.encode(
                {"config": config, "owned_server_reaped": True}):
            raise ValueError("wrong server stop acknowledgement")
        local = client.read_deduplicated_outcomes([b"warm\0"], [value])[0]
        elapsed = time.monotonic_ns() - warmed
        cold = client.read_deduplicated_outcomes([b"never-read\0"], [value])[0]
        result = dict(config=config, before=before, after=dedup.snapshot(lib, client.handle),
                      warm=warm, local_after_stop=local, cold_after_stop=cold,
                      warm_to_local_ns=elapsed, passed=True)
        verify(result, config)
        (output / "result.json").write_bytes(trace.encode(result))
    finally:
        client.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit("requires one NEW owner-created output directory")
    child(sys.argv[1])
