#!/usr/bin/env python3
"""Bounded explicit CPU C/Python status probe; caller owns the local service.

Success is a frozen diagnostic expectation, not an all-hit business gate. No
service control, arbitrary commands, shared-memory unlink or disk cleanup here.
"""

import argparse
import ctypes as ct
import hashlib
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_dedup as dedup
import eviction_smoke as smoke
import eviction_trace as trace
from eviction_guard import _stop

SCHEMA = "dfkv-cpu-dedup-runtime-v1"


def config(lib, members, tag, mode="live", tcp=False, *, token=None):
    if mode not in ("live", "stopped") or type(tcp) is not bool:
        raise ValueError("invalid probe mode/transport")
    if not isinstance(tag, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", tag) is None:
        raise ValueError("tag must be 1..64 ASCII letters/digits/_/-")
    base = smoke._config(lib, members, batch_gets=True, tcp=tcp,
                         cpu_dedup=True, cpu_dedup_token=token)
    original = [b"cpu-runtime\0" + tag.encode("ascii") + b"\0" + bytes([index])
                for index in range(2)]
    document = {"requests": [{"namespace_hex": value.hex()} for value in original]}
    return dict(base, tag=tag, mode=mode,
                namespaces=dedup.identities(document, token))


def _payload(namespace, key, length=64):
    return b"\0" + hashlib.shake_256(b"cpu-runtime-v1\0" + bytes([namespace]) + key).digest(length - 1)


def _item(name, length=64):
    return {"key_hex": (b"cpu\0" + name.encode("ascii")).hex(), "length": length}


def plan(cfg):
    a, big, absent = _item("A"), _item("big"), _item("absent")
    if cfg["mode"] == "stopped":
        return {"puts": [], "calls": [{"name": "stopped", "namespace": 0,
                                        "items": [a, absent], "expected": [4, 4]}]}
    return {"puts": [{"namespace": 0, "items": [a, big]},
                      {"namespace": 1, "items": [a]}],
            "calls": [{"name": "first", "namespace": 0, "items": [a], "expected": [0]},
                      {"name": "ready", "namespace": 0, "items": [a], "expected": [0]},
                      {"name": "namespace", "namespace": 1, "items": [a], "expected": [0]},
                      {"name": "mixed", "namespace": 0,
                       "items": [a, absent, _item("big", 1)], "expected": [0, 1, 5]}]}


def check_stopped_boundary(client, warm_key, expected, cold_key):
    """Check one still-open handle after its owner stopped the service.

    Caller must prove the stop and that the warmed READY entry remains within
    TTL. This helper does not stop servers, infer cancellation, or prove timing.
    Full hit bytes are checked by Client.read_deduplicated_outcomes itself.
    """
    if (type(warm_key) is not bytes or type(cold_key) is not bytes
            or not warm_key or not cold_key or warm_key == cold_key
            or type(expected) is not bytes):
        raise ValueError("distinct nonempty binary keys and expected bytes required")
    warm = client.read_deduplicated_outcomes([warm_key], [expected])
    cold = client.read_deduplicated_outcomes([cold_key], [expected])
    if trace.encode(warm) != trace.encode([{"outcome": "get_hit", "status": 0}]):
        raise ValueError("warm READY data was not served after stop")
    if (not isinstance(cold, list) or len(cold) != 1 or cold[0].get("outcome") != "client_error"
            or type(cold[0].get("status")) is not int or cold[0].get("status") != 4 or not isinstance(cold[0].get("error"), str)
            or not cold[0]["error"]):
        raise ValueError("never-read stopped key was not an explicit I/O failure")
    return {"warm": warm, "cold": cold}


class _Capture:
    """Intercept one existing Python binding call while its owners are live."""
    def __init__(self, library):
        self.library, self.raw, self.put_raw = library, None, None

    def __getattr__(self, name):
        return getattr(self.library, name)

    def dfkv_batch_put(self, handle, keys, key_lens, data, sizes, count, acks):
        code = self.library.dfkv_batch_put(handle, keys, key_lens, data, sizes, count, acks)
        self.put_raw = {"rc": code, "acks": list(acks)}
        return code

    def dfkv_batch_get_auto_deduplicated_status(self, handle, keys, key_lens, data,
                                                caps, count, statuses, lengths):
        # Freeze the caller-owned bounds/addresses before native code runs; do
        # not trust mutated input descriptors when preserving byte evidence.
        addresses, bounds = list(data), list(caps)
        code = self.library.dfkv_batch_get_auto_deduplicated_status(
            handle, keys, key_lens, data, caps, count, statuses, lengths)
        self.raw = {"rc": code, "statuses": list(statuses), "lengths": list(lengths),
                    "values_hex": [ct.string_at(addresses[i], min(lengths[i], bounds[i])).hex()
                                   if code == 0 and statuses[i] == 0 else None
                                   for i in range(count)]}
        return code


class _Native:
    def __init__(self, cfg):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "integration/common/src"))
        from disk_scenarios import Client, load_library
        self.lib = load_library(cfg["lib"])
        if not hasattr(self.lib, "dfkv_batch_get_auto_deduplicated_status"):
            raise RuntimeError("explicit CPU dedup status symbol required")
        self.capture = _Capture(self.lib)
        self.clients = []
        try:
            for identity in cfg["namespaces"]:
                self.clients.append(Client(self.capture, cfg["members"],
                                           bytes.fromhex(identity["backend_namespace_hex"])))
        except BaseException:
            self.close()
            raise
        self.transports = [client.transport for client in self.clients]

    def close(self):
        for client in self.clients:
            client.close()

    def metrics(self):
        return [dedup.snapshot(self.lib, client.handle) for client in self.clients]

    def put(self, row):
        client = self.clients[row["namespace"]]
        keys = [bytes.fromhex(item["key_hex"]) for item in row["items"]]
        self.capture.put_raw = None
        try:
            client.put(keys, [_payload(row["namespace"], key) for key in keys])
        except Exception as error:
            return {"raw": self.capture.put_raw, "error": f"{type(error).__name__}: {error}"}
        return {"raw": self.capture.put_raw}

    def get(self, row):
        self.capture.raw = None
        keys = [bytes.fromhex(item["key_hex"]) for item in row["items"]]
        expected = [_payload(row["namespace"], key, item["length"])
                    for key, item in zip(keys, row["items"])]
        try:
            outcomes = self.clients[row["namespace"]].read_deduplicated_outcomes(keys, expected)
        except Exception as error:
            return {"raw": self.capture.raw, "error": f"{type(error).__name__}: {error}"}
        return {"raw": self.capture.raw, "outcomes": outcomes}


def _same(left, right):
    return trace.encode(left) == trace.encode(right)


def _matches(row):
    if row["state"] != "completed":
        return False
    raw = row["raw"]
    if raw["statuses"] != row["expected"]:
        return False
    for item, status, length, value, outcome in zip(
            row["items"], raw["statuses"], raw["lengths"], raw["values_hex"], row["outcomes"]):
        if status == 0:
            if length != item["length"] or value != _payload(row["namespace"], bytes.fromhex(item["key_hex"]), item["length"]).hex():
                return False
            if outcome != {"status": 0, "outcome": "get_hit"}:
                return False
        elif status == 1:
            if outcome != {"status": 1, "outcome": "get_miss"}:
                return False
        elif outcome.get("status") != status or outcome.get("outcome") != "client_error" or not outcome.get("error"):
            return False
    return True


def _summary(result):
    calls = result["calls"]
    outcomes = dict.fromkeys(("hit", "miss", "error", "wrong_bytes", "call_error", "not_attempted"), 0)
    hit_bytes = 0
    for row in calls:
        if row["state"] != "completed":
            outcomes["call_error" if row["state"] == "call_error" else "not_attempted"] += len(row["items"])
            continue
        for item, status, length, value in zip(row["items"], row["raw"]["statuses"],
                                              row["raw"]["lengths"], row["raw"]["values_hex"]):
            if status == 0:
                expected = _payload(row["namespace"], bytes.fromhex(item["key_hex"]), item["length"]).hex()
                if length == item["length"] and value == expected:
                    outcomes["hit"] += 1
                    hit_bytes += length
                else:
                    outcomes["wrong_bytes"] += 1
            else:
                outcomes["miss" if status == 1 else "error"] += 1
    before, after = result["before"], result["after"]
    observation = None if before is None or after is None else {
        name: sum(a[name] - b[name] for b, a in zip(before, after)) for name in dedup.COUNTERS}
    return {"planned_puts": sum(len(row["items"]) for row in result["setup"]),
            "acked_puts": sum(sum(ack == 1 for ack in row["raw"]["acks"]) for row in result["setup"]
                             if row.get("raw") is not None and row["raw"]["rc"] == 0),
            "acked_put_bytes": sum(64 * sum(ack == 1 for ack in row["raw"]["acks"]) for row in result["setup"]
                                   if row.get("raw") is not None and row["raw"]["rc"] == 0),
            "unresolved_puts": sum(len(row["items"]) for row in result["setup"]
                                   if row["state"] == "call_error" and
                                   (row.get("raw") is None or row["raw"]["rc"] != 0)),
            "planned_gets": sum(len(row["items"]) for row in calls),
            "attempted_gets": sum(len(row["items"]) for row in calls if row["state"] != "not_attempted"),
            "completed_gets": sum(len(row["items"]) for row in calls if row["state"] == "completed"),
            "get_calls": sum(row["state"] != "not_attempted" for row in calls),
            "get_outcomes": outcomes, "hit_get_bytes": hit_bytes,
            "cpu_dedup_observations": observation}


def run(cfg, *, client_factory=_Native):
    frozen = plan(cfg)
    result = {"schema": SCHEMA, "config": cfg, "transports": None,
              "before": None, "after": None,
              "setup": [dict(row, state="not_attempted") for row in frozen["puts"]],
              "calls": [dict(row, state="not_attempted") for row in frozen["calls"]],
              "errors": [], "passed": False}
    origin, client = time.monotonic_ns(), None
    try:
        if not _same(config(cfg["lib"], cfg["members"], cfg["tag"], cfg["mode"],
                            cfg["client_environment"]["DFKV_RDMA"] == "0",
                            token=cfg["cpu_dedup_token"]), cfg):
            raise ValueError("library/config changed")
        client = client_factory(cfg)
        result["transports"] = client.transports
        if result["transports"] != ["tcp(rdma-not-requested)" if cfg["client_environment"]["DFKV_RDMA"] == "0" else "rdma"] * 2:
            raise ValueError("unexpected actual transport")
        result["before"] = client.metrics()
        if any(any(value.values()) for value in result["before"]):
            raise ValueError("CPU counters not fresh")
        for row in result["setup"]:
            row.update(state="call_error", started_ns=time.monotonic_ns() - origin)
            try:
                row.update(client.put(row))
                if row.get("raw") == {"rc": 0, "acks": [1] * len(row["items"])} and "error" not in row:
                    row["state"] = "completed"
            except Exception as error:
                row["error"] = f"{type(error).__name__}: {error}"
            row["completed_ns"] = time.monotonic_ns() - origin
            if row["state"] != "completed":
                raise RuntimeError("setup PUT not fully acknowledged")
        for row in result["calls"]:
            row.update(state="call_error", started_ns=time.monotonic_ns() - origin)
            try:
                row.update(client.get(row))
                row["state"] = "completed" if isinstance(row.get("raw"), dict) and row["raw"].get("rc") == 0 and "outcomes" in row else "call_error"
            except Exception as error:
                row["error"] = f"{type(error).__name__}: {error}"
            row["completed_ns"] = time.monotonic_ns() - origin
            if not _matches(row):
                break
        result["after"] = client.metrics()
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as error:
                result["errors"].append(f"close: {type(error).__name__}: {error}")
    result["elapsed_ns"] = time.monotonic_ns() - origin
    result["summary"] = _summary(result)
    result["passed"] = (not result["errors"] and result["before"] is not None and result["after"] is not None
                        and all(row["state"] == "completed" for row in result["setup"])
                        and all(_matches(row) for row in result["calls"])
                        and _counter_match(result, cfg))
    return result


def _counter_match(result, cfg):
    expected = [{"hits": 2, "fetches": 3, "wait_hits": 0, "wait_timeouts": 0},
                {"hits": 0, "fetches": 1, "wait_hits": 0, "wait_timeouts": 0}]
    if cfg["mode"] == "stopped":
        expected = [dict(expected[0], hits=0, fetches=2), dict(expected[1], fetches=0)]
    return result["after"] == expected


def _time(row, previous):
    begin, end = row.get("started_ns"), row.get("completed_ns")
    if type(begin) is not int or type(end) is not int or not previous <= begin <= end:
        raise ValueError("invalid call timestamps")
    return end


def verify(result, cfg):
    fields = {"schema", "config", "transports", "before", "after", "setup", "calls", "errors", "passed", "elapsed_ns", "summary"}
    if (not isinstance(result, dict) or set(result) != fields or result["schema"] != SCHEMA
            or not _same(result["config"], cfg) or not isinstance(result["errors"], list)
            or any(not isinstance(error, str) or not error for error in result["errors"])):
        raise ValueError("invalid result identity/errors")
    frozen = plan(cfg)
    if any(not isinstance(result[key], list) or len(result[key]) != len(frozen[source])
           for key, source in (("setup", "puts"), ("calls", "calls"))):
        raise ValueError("incomplete plan ledger")
    for value in (result["before"], result["after"]):
        if value is not None and (not isinstance(value, list) or len(value) != 2 or any(
                not isinstance(row, dict) or set(row) != set(dedup.COUNTERS)
                or any(type(n) is not int or not 0 <= n < 2**64 for n in row.values()) for row in value)):
            raise ValueError("invalid CPU counter snapshot")
    if result["before"] is not None and any(any(row.values()) for row in result["before"]):
        # A known setup refusal may record the nonfresh counters but must not
        # proceed to workload activity or return a successful diagnostic.
        if not result["errors"] or any(row["state"] != "not_attempted" for row in result["setup"] + result["calls"]):
            raise ValueError("CPU counters not fresh before work")
    if result["after"] is not None and result["before"] is None:
        raise ValueError("final counters without enabled observation")
    previous, stopped, activity = 0, False, False
    for row, expected in zip(result["setup"], frozen["puts"]):
        if not isinstance(row, dict) or any(not _same(row.get(k), v) for k, v in expected.items()):
            raise ValueError("setup plan changed")
        state = row.get("state")
        if state == "not_attempted":
            if set(row) != set(expected) | {"state"} or not (stopped or result["errors"]):
                raise ValueError("unexplained skipped setup")
            stopped = True
            continue
        if stopped or state not in ("completed", "call_error"):
            raise ValueError("invalid setup state")
        activity = True
        previous = _time(row, previous)
        allowed = set(expected) | {"state", "started_ns", "completed_ns", "raw"}
        if state == "call_error":
            allowed.add("error")
        if set(row) - allowed:
            raise ValueError("unexpected setup fields")
        raw = row.get("raw")
        if raw is not None and (not isinstance(raw, dict) or set(raw) != {"rc", "acks"}
                or type(raw["rc"]) is not int or not isinstance(raw["acks"], list)
                or len(raw["acks"]) != len(row["items"]) or any(type(n) is not int for n in raw["acks"])):
            raise ValueError("invalid setup C output")
        if raw is not None and raw["rc"] == 0 and any(ack not in (0, 1) for ack in raw["acks"]):
            raise ValueError("invalid authoritative PUT mask")
        if state == "completed":
            if set(row) != allowed or raw != {"rc": 0, "acks": [1] * len(row["items"])}:
                raise ValueError("unacknowledged completed setup")
        else:
            if not isinstance(row.get("error"), str) or not row["error"]:
                raise ValueError("missing setup error")
            stopped = True
    for row, expected in zip(result["calls"], frozen["calls"]):
        if not isinstance(row, dict) or any(not _same(row.get(k), v) for k, v in expected.items()):
            raise ValueError("GET plan changed")
        state = row.get("state")
        if state == "not_attempted":
            if set(row) != set(expected) | {"state"} or not (stopped or result["errors"]):
                raise ValueError("unexplained skipped GET")
            stopped = True
            continue
        if stopped or state not in ("completed", "call_error"):
            raise ValueError("GET after failure or invalid state")
        activity = True
        previous = _time(row, previous)
        base = set(expected) | {"state", "started_ns", "completed_ns"}
        raw = row.get("raw")
        if state == "call_error":
            if not set(row) <= base | {"raw", "error"} or not isinstance(row.get("error"), str) or not row["error"]:
                raise ValueError("call error without evidence")
            if raw is not None and (not isinstance(raw, dict) or type(raw.get("rc")) is not int or raw["rc"] == 0):
                raise ValueError("whole call failure has no failed rc")
            stopped = True
            continue  # Discard every whole-call output array, even apparent hits.
        if set(row) != base | {"raw", "outcomes"}:
            raise ValueError("unexpected completed GET fields")
        count = len(row["items"])
        if (not isinstance(raw, dict) or set(raw) != {"rc", "statuses", "lengths", "values_hex"}
                or type(raw["rc"]) is not int or raw["rc"] != 0
                or any(not isinstance(raw[key], list) or len(raw[key]) != count
                       for key in ("statuses", "lengths", "values_hex"))
                or not isinstance(row["outcomes"], list) or len(row["outcomes"]) != count):
            raise ValueError("invalid GET output shape")
        for item, status, length, value, outcome in zip(row["items"], raw["statuses"], raw["lengths"], raw["values_hex"], row["outcomes"]):
            if type(status) is not int or status not in range(9) or type(length) is not int or not 0 <= length < 2**64:
                raise ValueError("invalid native code/length")
            if status == 0:
                if not isinstance(value, str) or re.fullmatch(r"(?:[0-9a-f]{2})*", value) is None or len(value) != length * 2 or length > item["length"] + 1:
                    raise ValueError("invalid bounded byte evidence")
            elif length != 0 or value is not None:
                raise ValueError("failed slot publishes bytes/length")
            from eviction_batch import check_outcome
            check_outcome(outcome)
            if outcome.get("status") != status:
                raise ValueError("Python decoder contradicts native status")
        stopped = not _matches(row)
    if result["before"] is not None and result["after"] is not None:
        for index, (before, after) in enumerate(zip(result["before"], result["after"])):
            attempted = sum(len(row["items"]) for row in result["calls"]
                            if row["namespace"] == index and row["state"] != "not_attempted")
            if any(after[name] < before[name] for name in dedup.COUNTERS):
                raise ValueError("CPU counter moved backwards")
            if sum(after[name] - before[name] for name in dedup.COUNTERS) > attempted:
                raise ValueError("CPU counters exceed attempted GET denominator")
    if activity:
        expected = "tcp(rdma-not-requested)" if cfg["client_environment"]["DFKV_RDMA"] == "0" else "rdma"
        if result["transports"] != [expected] * 2 or result["before"] is None:
            raise ValueError("activity without correct transport/enabled CPU")
    elif result["transports"] is not None and (not isinstance(result["transports"], list) or len(result["transports"]) != 2):
        raise ValueError("invalid setup transports")
    if type(result["elapsed_ns"]) is not int or result["elapsed_ns"] < previous:
        raise ValueError("invalid elapsed time")
    summary = _summary(result)
    passed = (not result["errors"] and result["before"] is not None and result["after"] is not None
              and all(row["state"] == "completed" for row in result["setup"])
              and all(_matches(row) for row in result["calls"]) and _counter_match(result, cfg))
    if not _same(summary, result["summary"]) or type(result["passed"]) is not bool or result["passed"] != passed:
        raise ValueError("summary/diagnostic pass mismatch")
    return summary


def _command(frozen, raw):
    return [sys.executable, "-I", str(Path(__file__).resolve()), "--child", str(frozen), str(raw)]


def supervise(output, *, lib, members, tag, mode="live", tcp=False, process_timeout_sec=20):
    if (type(process_timeout_sec) not in (int, float) or not math.isfinite(process_timeout_sec)
            or not .05 <= process_timeout_sec <= 30):
        raise ValueError("process timeout must be in [0.05,30]")
    cfg = config(lib, members, tag, mode, tcp, token=uuid.uuid4().hex)
    for identity in cfg["namespaces"]:
        if os.path.lexists(identity["possible_shm_path"]):
            raise ValueError("CPU namespace occupied before child start")
    root = Path(output).absolute()
    root.mkdir()
    frozen, raw = root / "config.json", root / "child-result.json"
    frozen.write_bytes(trace.encode(cfg))
    report = {"schema": SCHEMA, "config": cfg, "passed": False, "status": "pending",
              "outcomes": "unknown", "summary": None, "actions": [], "child_pid": None,
              "returncode": None, "reaped": False, "process_timeout_sec": process_timeout_sec,
              "command": _command(frozen, raw),
              "possible_retained_shm_paths": [item["possible_shm_path"] for item in cfg["namespaces"]],
              "shm_cleanup": "not_attempted_no_exclusive_creator_proof"}
    child, interruption, origin = None, None, time.monotonic_ns()
    env = {key: value for key, value in os.environ.items() if not key.startswith("DFKV_")}
    env.update(cfg["client_environment"])
    try:
        with (root / "stdout.log").open("xb") as out, (root / "stderr.log").open("xb") as err:
            child = subprocess.Popen(report["command"], stdin=subprocess.DEVNULL, stdout=out, stderr=err, env=env)
            report["child_pid"] = child.pid
            remaining = process_timeout_sec - (time.monotonic_ns() - origin) / 1e9
            if remaining <= 0:
                report["status"] = "process_timeout"
            else:
                try:
                    child.wait(timeout=remaining)
                    if (time.monotonic_ns() - origin) / 1e9 > process_timeout_sec:
                        report["status"] = "process_timeout"
                except subprocess.TimeoutExpired:
                    report["status"] = "process_timeout"
    except BaseException as error:
        report.update(status="supervisor_error", error=f"{type(error).__name__}: {error}")
        if not isinstance(error, Exception):
            interruption = error
    finally:
        if child is not None:
            if child.poll() is None:
                report["actions"] = _stop(child)
            report.update(returncode=child.poll(), reaped=child.returncode is not None)
            if not report["reaped"]:
                report.update(previous_status=report["status"], status="cleanup_failed")
        report["elapsed_ns"] = time.monotonic_ns() - origin
    if report["status"] == "pending":
        if report["returncode"] not in (0, 1):
            report["status"] = "child_exit"
        else:
            try:
                result = trace.load(raw)
                counts = verify(result, cfg)
                if report["returncode"] != (0 if result["passed"] else 1):
                    raise ValueError("exit contradicts ledger")
                report.update(status="passed" if result["passed"] else "workload_failed", passed=result["passed"],
                              outcomes="complete_child_ledger", summary=counts)
            except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
                report.update(status="invalid_result", error=f"{type(error).__name__}: {error}")
    (root / "report.json").write_bytes(trace.encode(report))
    if interruption is not None:
        raise interruption
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", nargs=2, metavar=("CONFIG", "RESULT"), help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--lib")
    parser.add_argument("--members")
    parser.add_argument("--tag")
    parser.add_argument("--mode", choices=("live", "stopped"), default="live")
    parser.add_argument("--tcp", action="store_true")
    parser.add_argument("--process-timeout-sec", type=float, default=20)
    args = parser.parse_args()
    try:
        if args.child:
            cfg = trace.load(args.child[0])
            result = run(cfg)
            with Path(args.child[1]).open("xb") as out:
                out.write(trace.encode(result))
        else:
            if args.output is None or any(value is None for value in (args.lib, args.members, args.tag)):
                raise ValueError("output/lib/members/tag required")
            result = supervise(args.output, lib=args.lib, members=args.members, tag=args.tag,
                               mode=args.mode, tcp=args.tcp, process_timeout_sec=args.process_timeout_sec)
        print(trace.encode(result).decode("ascii"), end="")
        return 0 if result["passed"] else 1
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"CPU dedup probe error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
