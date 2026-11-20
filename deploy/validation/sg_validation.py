#!/usr/bin/env python3
"""Bounded loopback SG C ABI correctness probe; never a public Python binding.

The caller owns the service. This tool never starts/stops servers or deletes data.
Use a fresh tag/service for live mode; stopped mode uses the same tag after its
owner has stopped that service. RXE zero-capacity refusal is not a cache miss.
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_trace as trace
from eviction_guard import _stop

SCHEMA = "dfkv-sg-cabi-correctness-v1"
CLIENT_ENV = {"DFKV_RDMA": "1", "DFKV_RDMA_DEV": "rxe0", "DFKV_RDMA_DEPTH": "4",
              "DFKV_RDMA_RECV_CHUNK_BYTES": "8388608", "DFKV_RDMA_MAX_PAYLOAD_BYTES": "2097152",
              "DFKV_RDMA_MAX_BLOCK_BYTES": "2097152", "DFKV_GET_MISS_RETRIES": "0"}


def config(lib, members, tag, mode="live", tcp=False):
    if type(tcp) is not bool or mode not in ("live", "stopped"):
        raise ValueError("invalid mode/transport")
    if not isinstance(tag, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,80}", tag) is None:
        raise ValueError("tag must be 1..80 ASCII letters/digits/underscore/hyphen")
    match = re.fullmatch(r"[A-Za-z0-9_-]+=127\.0\.0\.1:([0-9]{1,5})", members)
    if match is None or not 1 <= int(match[1]) <= 65535:
        raise ValueError("one explicit loopback member required")
    library = Path(lib).resolve(strict=True)
    if not library.is_file():
        raise ValueError("library must be a regular file")
    return {"lib": str(library), "lib_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
            "members": members, "tag": tag, "mode": mode,
            "namespace_hex": (b"sg-validation\0" + tag.encode("ascii")).hex(),
            "client_environment": dict(CLIENT_ENV, DFKV_RDMA="0" if tcp else "1")}


def _payload(key, length):
    # Include a guaranteed NUL and offset-varying bytes, not repeated fill bytes.
    return (b"\0" + hashlib.shake_256(b"sg-validation-v1\0" + key).digest(length - 1))


def _item(name, caps, *, shape="normal"):
    key = b"sg\0" + name.encode("ascii")
    length = 129 if name == "many" else 64
    return {"key_hex": key.hex(), "caps": caps, "shape": shape, "length": length,
            "payload_sha256": hashlib.sha256(_payload(key, length)).hexdigest()}


def plan(cfg):
    a, big, other = _item("A", [64]), _item("big", [64]), _item("other", [64])
    missing = _item("missing", [64])
    def call(name, api, items, expected):
        return {"name": name, "api": api, "items": items, "expected": expected}
    if cfg["mode"] == "stopped":
        return {"puts": [], "calls": [call("stopped", "status", [a, missing, big, other], [4] * 4)]}
    tcp = cfg["client_environment"]["DFKV_RDMA"] == "0"
    # TCP length=0 means whole-object: a nonempty zero-cap reply is an I/O
    # decode refusal, not an Invalid/miss proof. The ordinary TCP zero case
    # uses absent keys; its separate FINAL case preserves this known refusal
    # without allowing peer cooldown to mask subsequent normal operations.
    zero_items = ([_item("missing", []), _item("missing2", [0]), _item("missing3", [])]
                  if tcp else [_item("A", []), _item("A", [0]), _item("missing", [])])
    result = {"puts": [a, big, other, _item("many", [129])], "calls": [
        call("binary", "status", [_item("A", [3, 0, 61])], [0]),
        call("mixed", "status", [_item("A", [16, 48]), missing, _item("big", [63]), other], [0, 1, 5, 0]),
        call("many", "status", [_item("many", [1] * 129)], [0]),
        call("zero", "status", zero_items, [1, 1, 1] if tcp else [5, 5, 5]),
        call("invalid", "status", [_item("A", [], shape="negative"),
             _item("A", [1], shape="null_nested"),
             _item("A", [2**64 - 1, 1], shape="overflow"), other], [5, 5, 5, 0]),
        call("legacy", "legacy", [a, missing, _item("big", [63]), other], [1, 0, 0, 1])]}
    if tcp:
        # Preserve the known TCP zero-capacity limitation as the FINAL case.
        # Its I/O refusal can cool the peer; no later normal case may be masked.
        result["calls"].append(call("tcp-zero-nonempty", "status",
                                    [_item("A", []), _item("big", [0])], [4, 4]))
    return result


class _Native:
    """Local validation-only binding; all descriptor/backing owners survive calls."""
    def __init__(self, cfg):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "integration/common/src"))
        from disk_scenarios import Client, load_library
        from dfkv_common.cabi import make_key_array
        self.make_keys = make_key_array
        self.lib = load_library(cfg["lib"])
        args = [ct.c_void_p, ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_uint64),
                ct.POINTER(ct.POINTER(ct.c_void_p)), ct.POINTER(ct.POINTER(ct.c_uint64)),
                ct.POINTER(ct.c_int), ct.c_int, ct.POINTER(ct.c_int), ct.POINTER(ct.c_uint64)]
        # Missing symbols fail closed; no scalar/EXIST fallback.
        for name in ("dfkv_batch_get_auto_sg_status", "dfkv_batch_get_auto_sg"):
            function = getattr(self.lib, name)
            function.argtypes, function.restype = args, ct.c_int
        self.lib.dfkv_max_sg_segs.argtypes = [ct.c_void_p]
        self.lib.dfkv_max_sg_segs.restype = ct.c_uint32
        self.client = Client(self.lib, cfg["members"], bytes.fromhex(cfg["namespace_hex"]))
        self.transport = self.client.transport
        self.sg_width = self.lib.dfkv_max_sg_segs(self.client.handle)

    def close(self):
        self.client.close()

    def put(self, items):
        keys = [bytes.fromhex(item["key_hex"]) for item in items]
        pointers, lengths, owners = self.make_keys(keys)
        payloads = [_payload(key, item["length"]) for key, item in zip(keys, items)]
        buffers = [ct.create_string_buffer(value) for value in payloads]
        data = (ct.c_void_p * len(items))(*[ct.addressof(buf) for buf in buffers])
        sizes = (ct.c_uint64 * len(items))(*[len(value) for value in payloads])
        acks = (ct.c_int * len(items))(*([-999] * len(items)))
        rc = self.lib.dfkv_batch_put(self.client.handle, pointers, lengths, data, sizes, len(items), acks)
        return rc, list(acks)

    def get(self, call):
        items, count = call["items"], len(call["items"])
        keys, key_lens, owners = self.make_keys([bytes.fromhex(i["key_hex"]) for i in items])
        destinations = (ct.POINTER(ct.c_void_p) * count)()
        capacities = (ct.POINTER(ct.c_uint64) * count)()
        counts = (ct.c_int * count)()
        buffers, descriptor_owners = [], []
        for index, item in enumerate(items):
            caps, shape = item["caps"], item["shape"]
            counts[index] = -1 if shape == "negative" else len(caps)
            item_buffers = [ct.create_string_buffer(cap if cap <= 4096 else 1) if cap else None for cap in caps]
            buffers.append(item_buffers)
            if shape == "null_nested" or not caps:
                continue
            dst = (ct.c_void_p * len(caps))(*[ct.addressof(buf) if buf is not None else None for buf in item_buffers])
            sizes = (ct.c_uint64 * len(caps))(*caps)
            destinations[index], capacities[index] = dst, sizes
            descriptor_owners.extend((dst, sizes))
        statuses = (ct.c_int * count)(*([-999] * count))
        lengths = (ct.c_uint64 * count)(*([999] * count))
        name = "dfkv_batch_get_auto_sg_status" if call["api"] == "status" else "dfkv_batch_get_auto_sg"
        rc = getattr(self.lib, name)(self.client.handle, keys, key_lens, destinations, capacities,
                                     counts, count, statuses, lengths)
        values = []
        for index, item in enumerate(items):
            hit = statuses[index] == (0 if call["api"] == "status" else 1)
            if rc != 0 or not hit:
                values.append(None)
            else:
                # Never trust a native length for a Python memory read. Every
                # owner is bounded; invalid/oversize metadata stays in the ledger.
                raw = b"".join(buf.raw for buf in buffers[index] if buf is not None)
                values.append(raw[:min(lengths[index], sum(item["caps"]))].hex())
        return {"rc": rc, "statuses": list(statuses), "lengths": list(lengths), "values_hex": values}


def _same(left, right):
    return trace.encode(left) == trace.encode(right)


def _summary(result):
    setup, calls = result["setup"], result["calls"]
    attempted_puts = len(setup["items"]) if setup["state"] != "not_attempted" else 0
    completed_puts = len(setup["items"]) if setup["state"] == "completed" else 0
    attempted_gets = sum(len(c["items"]) for c in calls if c["state"] != "not_attempted")
    completed_gets = sum(len(c["items"]) for c in calls if c["state"] == "completed")
    outcomes = {key: 0 for key in ("hit", "miss", "unresolved", "error", "wrong_bytes", "call_error", "not_attempted")}
    hit_bytes = 0
    for call in calls:
        count = len(call["items"])
        if call["state"] != "completed":
            outcomes["not_attempted" if call["state"] == "not_attempted" else "call_error"] += count
            continue
        for item, status, length, value in zip(call["items"], call["statuses"], call["lengths"], call["values_hex"]):
            hit = status == (0 if call["api"] == "status" else 1)
            if hit:
                expected = _payload(bytes.fromhex(item["key_hex"]), item["length"]).hex()
                if length != item["length"] or value != expected:
                    outcomes["wrong_bytes"] += 1
                else:
                    outcomes["hit"] += 1
                    hit_bytes += length
            elif call["api"] == "legacy":
                outcomes["unresolved"] += 1
            else:
                outcomes["miss" if status == 1 else "error"] += 1
    acks = setup.get("acks", []) if setup["state"] == "completed" else []
    return {"planned_puts": len(setup["items"]), "planned_gets": sum(len(c["items"]) for c in calls),
            "attempted_puts": attempted_puts, "completed_puts": completed_puts,
            "attempted_gets": attempted_gets, "completed_gets": completed_gets,
            "acked_puts": sum(ack == 1 for ack in acks),
            "acked_put_bytes": sum(i["length"] for i, ack in zip(setup["items"], acks) if ack == 1),
            "sg_calls": sum(c["state"] != "not_attempted" for c in calls),
            "sg_failed_calls": sum(c["state"] == "call_error" for c in calls),
            "hit_get_bytes": hit_bytes, "get_outcomes": outcomes}


def _call_matches(call):
    if call["state"] != "completed" or call["rc"] != 0 or call["statuses"] != call["expected"]:
        return False
    for item, status, length, value in zip(call["items"], call["statuses"], call["lengths"], call["values_hex"]):
        hit = status == (0 if call["api"] == "status" else 1)
        if hit and (length != item["length"] or value != _payload(bytes.fromhex(item["key_hex"]), item["length"]).hex()):
            return False
    return True


def run(cfg, *, client_factory=_Native):
    frozen = plan(cfg)
    result = {"schema": SCHEMA, "config": cfg, "transport": None, "sg_width": None,
              "setup": {"items": frozen["puts"], "state": "not_attempted" if frozen["puts"] else "completed"},
              "calls": [dict(c, state="not_attempted") for c in frozen["calls"]],
              "errors": [], "passed": False}
    started, client = time.monotonic_ns(), None
    try:
        if not _same(config(cfg["lib"], cfg["members"], cfg["tag"], cfg["mode"], cfg["client_environment"]["DFKV_RDMA"] == "0"), cfg):
            raise ValueError("library/config changed")
        client = client_factory(cfg)
        result.update(transport=client.transport, sg_width=client.sg_width)
        if client.transport != ("tcp(rdma-not-requested)" if cfg["client_environment"]["DFKV_RDMA"] == "0" else "rdma"):
            raise ValueError("unexpected actual transport")
        if not 1 <= client.sg_width < 129:
            raise ValueError("129 descriptors must exceed actual runtime SG width")
        setup = result["setup"]
        if setup["items"]:
            setup.update(state="call_error", started_ns=time.monotonic_ns() - started)
            try:
                setup["rc"], setup["acks"] = client.put(setup["items"])
                setup["state"] = "completed" if setup["rc"] == 0 else "call_error"
            except Exception as error:
                setup["error"] = f"{type(error).__name__}: {error}"
            setup["completed_ns"] = time.monotonic_ns() - started
            if setup["state"] != "completed" or setup.get("acks") != [1] * len(setup["items"]):
                raise RuntimeError("setup PUT not fully acknowledged")
        else:
            setup["state"] = "completed"
        for call in result["calls"]:
            call.update(state="call_error", started_ns=time.monotonic_ns() - started)
            try:
                call.update(client.get(call))
                call["state"] = "completed" if call["rc"] == 0 else "call_error"
            except Exception as error:
                call["error"] = f"{type(error).__name__}: {error}"
            call["completed_ns"] = time.monotonic_ns() - started
            if not _call_matches(call):
                break
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as error:
                result["errors"].append(f"close: {type(error).__name__}: {error}")
    result["elapsed_ns"] = time.monotonic_ns() - started
    result["summary"] = _summary(result)
    result["passed"] = not result["errors"] and all(_call_matches(c) for c in result["calls"])
    return result


def _times(row, previous):
    begin, end = row.get("started_ns"), row.get("completed_ns")
    if type(begin) is not int or type(end) is not int or not previous <= begin <= end:
        raise ValueError("invalid sequential timestamps")
    return end


def verify(result, cfg):
    frozen = plan(cfg)
    if (not isinstance(result, dict) or set(result) != {"schema", "config", "transport", "sg_width", "setup", "calls", "errors", "passed", "elapsed_ns", "summary"}
            or result.get("schema") != SCHEMA or not _same(result.get("config"), cfg)
            or not isinstance(result.get("errors"), list) or any(not isinstance(e, str) or not e for e in result["errors"])):
        raise ValueError("wrong result identity/errors")
    setup, calls = result.get("setup"), result.get("calls")
    if not isinstance(setup, dict) or not _same(setup.get("items"), frozen["puts"]):
        raise ValueError("setup identity changed")
    if not isinstance(calls, list) or len(calls) != len(frozen["calls"]):
        raise ValueError("incomplete call ledger")
    previous, stopped = 0, False
    setup_state = setup.get("state")
    if setup_state not in ("completed", "call_error", "not_attempted"):
        raise ValueError("invalid setup state")
    if not setup["items"]:
        if setup_state != "completed" or set(setup) != {"items", "state"}:
            raise ValueError("empty setup claims activity")
    elif setup_state == "not_attempted":
        if set(setup) != {"items", "state"} or not result["errors"]:
            raise ValueError("unexplained skipped setup")
        stopped = True
    else:
        previous = _times(setup, previous)
        activity = {"items", "state", "started_ns", "completed_ns"}
        if setup_state == "call_error":
            if "rc" in setup:
                if (set(setup) != activity | {"rc", "acks"} or type(setup["rc"]) is not int or setup["rc"] == 0
                        or not isinstance(setup.get("acks"), list) or len(setup["acks"]) != len(setup["items"])
                        or any(type(ack) is not int for ack in setup["acks"])):
                    raise ValueError("setup error has invalid rc/discarded outputs")
            elif (set(setup) != activity | {"error"} or not isinstance(setup.get("error"), str) or not setup["error"]):
                raise ValueError("setup error lacks evidence")
            stopped = True
        else:
            if set(setup) != activity | {"rc", "acks"}:
                raise ValueError("completed setup has unexpected fields")
            if type(setup.get("rc")) is not int or setup["rc"] != 0 or not isinstance(setup.get("acks"), list) or len(setup["acks"]) != len(setup["items"]):
                raise ValueError("invalid setup call ledger")
            if any(type(ack) is not int or ack not in (0, 1) for ack in setup["acks"]):
                raise ValueError("invalid PUT mask")
            stopped = setup["acks"] != [1] * len(setup["items"])
    active = False
    for call, expected in zip(calls, frozen["calls"]):
        if not isinstance(call, dict) or any(not _same(call.get(k), v) for k, v in expected.items()):
            raise ValueError("call identity/expectation changed")
        state = call.get("state")
        if state == "not_attempted":
            if set(call) != set(expected) | {"state"} or not (stopped or result["errors"]):
                raise ValueError("unexplained skipped call or fabricated activity")
            stopped = True
            continue
        if stopped or state not in ("completed", "call_error"):
            raise ValueError("call executed after failure or invalid state")
        active = True
        previous = _times(call, previous)
        allowed = set(expected) | {"state", "started_ns", "completed_ns", "rc", "statuses", "lengths", "values_hex"}
        if state == "call_error":
            allowed.add("error")
        if not set(call) <= allowed:
            raise ValueError("unexpected native call fields")
        count = len(call["items"])
        if state == "call_error":
            if "error" in call and (not isinstance(call["error"], str) or not call["error"]):
                raise ValueError("invalid call error message")
            if "rc" in call and (type(call["rc"]) is not int or call["rc"] == 0):
                raise ValueError("call error rc invalid")
            if "rc" not in call and (not isinstance(call.get("error"), str) or not call["error"]):
                raise ValueError("call error missing evidence")
            # rc failure's out arrays are NOT authoritative per-item results.
            stopped = True
            continue
        if type(call.get("rc")) is not int or call["rc"] != 0:
            raise ValueError("completed call has no successful call rc")
        if any(not isinstance(call.get(k), list) or len(call[k]) != count for k in ("statuses", "lengths", "values_hex")):
            raise ValueError("bad native result shape")
        for item, status, length, value in zip(call["items"], call["statuses"], call["lengths"], call["values_hex"]):
            if type(status) is not int or status not in (range(9) if call["api"] == "status" else (0, 1)):
                raise ValueError("invalid native code")
            if type(length) is not int or not 0 <= length < 2**64:
                raise ValueError("invalid native length")
            hit = status == (0 if call["api"] == "status" else 1)
            if hit:
                if not isinstance(value, str) or re.fullmatch(r"(?:[0-9a-f]{2})*", value) is None or len(value) != length * 2 or length > sum(item["caps"]):
                    raise ValueError("invalid hit byte/length evidence")
            elif length != 0 or value is not None:
                raise ValueError("non-hit publishes length/bytes")
        stopped = not _call_matches(call)
    if active or setup.get("started_ns") is not None:
        if result.get("transport") != ("tcp(rdma-not-requested)" if cfg["client_environment"]["DFKV_RDMA"] == "0" else "rdma"):
            raise ValueError("wrong actual transport")
        if type(result.get("sg_width")) is not int or not 1 <= result["sg_width"] < 129:
            raise ValueError("invalid SG width proof")
    elif result.get("transport") not in (None, "tcp(rdma-not-requested)", "rdma"):
        raise ValueError("invalid setup transport")
    if type(result.get("elapsed_ns")) is not int or result["elapsed_ns"] < previous:
        raise ValueError("invalid elapsed time")
    summary = _summary(result)
    passed = not result["errors"] and setup_state == "completed" and all(_call_matches(c) for c in calls)
    if not _same(summary, result.get("summary")) or type(result.get("passed")) is not bool or result["passed"] != passed:
        raise ValueError("summary/pass mismatch")
    return summary


def _command(frozen, raw):
    return [sys.executable, "-I", str(Path(__file__).resolve()), "--child", str(frozen), str(raw)]


def supervise(output, *, lib, members, tag, mode="live", tcp=False, process_timeout_sec=20):
    if type(process_timeout_sec) not in (int, float) or not math.isfinite(process_timeout_sec) or not .05 <= process_timeout_sec <= 30:
        raise ValueError("process timeout must be in [0.05, 30]")
    cfg = config(lib, members, tag, mode, tcp)
    root = Path(output).absolute()
    root.mkdir()
    frozen, raw = root / "config.json", root / "child-result.json"
    frozen.write_bytes(trace.encode(cfg))
    report = {"schema": SCHEMA, "config": cfg, "planned_puts": len(plan(cfg)["puts"]),
              "planned_gets": sum(len(c["items"]) for c in plan(cfg)["calls"]),
              "status": "pending", "passed": False, "outcomes": "unknown", "summary": None,
              "actions": [], "child_pid": None, "returncode": None, "reaped": False,
              "process_timeout_sec": process_timeout_sec, "command": _command(frozen, raw)}
    child, interruption = None, None
    started = time.monotonic_ns()
    env = {key: value for key, value in os.environ.items() if not key.startswith("DFKV_")}
    env.update(cfg["client_environment"])
    try:
        with (root / "stdout.log").open("xb") as out, (root / "stderr.log").open("xb") as err:
            child = subprocess.Popen(report["command"], stdin=subprocess.DEVNULL, stdout=out, stderr=err, env=env)
            report["child_pid"] = child.pid
            remaining = process_timeout_sec - (time.monotonic_ns() - started) / 1e9
            if remaining <= 0:
                report["status"] = "process_timeout"
            else:
                try:
                    child.wait(timeout=remaining)
                    if (time.monotonic_ns() - started) / 1e9 > process_timeout_sec:
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
        report["elapsed_ns"] = time.monotonic_ns() - started
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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib")
    parser.add_argument("--members")
    parser.add_argument("--tag")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--mode", choices=("live", "stopped"), default="live")
    parser.add_argument("--tcp", action="store_true")
    parser.add_argument("--process-timeout-sec", type=float, default=20)
    parser.add_argument("--child", nargs=2, metavar=("CONFIG", "RESULT"), help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.child:
            cfg = trace.load(args.child[0])
            result = run(cfg)
            with Path(args.child[1]).open("xb") as out:
                out.write(trace.encode(result))
        else:
            if any(v is None for v in (args.lib, args.members, args.tag, args.output)):
                parser.error("--lib, --members, --tag and --output are required")
            result = supervise(args.output, lib=args.lib, members=args.members, tag=args.tag,
                               mode=args.mode, tcp=args.tcp, process_timeout_sec=args.process_timeout_sec)
        print(trace.encode(result).decode("ascii"), end="")
        return 0 if result["passed"] else 1
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"SG validation error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
