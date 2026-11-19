#!/usr/bin/env python3
"""Bounded loopback-only real-client correctness smoke, not arrival/performance replay."""

import argparse
from contextlib import ExitStack
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
from eviction_client import TraceClient
from eviction_guard import _stop

MODEL = "sequential-real-client-correctness-only"
CLIENT_ENV = {"DFKV_RDMA": "1", "DFKV_RDMA_DEV": "rxe0", "DFKV_RDMA_DEPTH": "4",
              "DFKV_RDMA_RECV_CHUNK_BYTES": "8388608", "DFKV_RDMA_MAX_PAYLOAD_BYTES": "2097152",
              "DFKV_RDMA_MAX_BLOCK_BYTES": "2097152"}


def _bounded(document):
    counts = trace.validate(document)
    config = document["config"]
    if config["objects"] > 8 or config["rounds"] > 4 or config["length"] > 4096:
        raise ValueError("smoke limited to 8 initial objects, 4 new writes, 4096 bytes each")
    return counts


def _config(lib, members):
    match = re.fullmatch(r"[A-Za-z0-9_-]+=127\.0\.0\.1:([0-9]{1,5})", members)
    if match is None or not 1 <= int(match[1]) <= 65535:
        raise ValueError("smoke requires one explicit loopback member: name=127.0.0.1:port")
    library = Path(lib).resolve(strict=True)
    if not library.is_file():
        raise ValueError("library must be a regular file")
    return {"lib": str(library), "lib_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
            "members": members, "client_environment": CLIENT_ENV}


def _summary(rows):
    return {"planned": len(rows), "attempted": sum("started_ns" in r for r in rows),
            "completed": sum("completed_ns" in r for r in rows),
            "acked_put_bytes": sum(r["length"] for r in rows if r["terminal"] == "put_ack"),
            "hit_get_bytes": sum(r["length"] for r in rows if r["terminal"] == "get_hit")}


def run(document, config, *, library_loader=None, client_factory=TraceClient):
    """Sequential strict ACK/hit gates. Open/call/close all remain in the child.

    Source scheduled_ns is identity only: no pacing or fixed-arrival claims. A
    no-hit is unresolved and fails the smoke. No resident/heat snapshot inferred.
    """
    _bounded(document)
    rows = [dict(row, terminal="not_attempted") for row in document["requests"]]
    result = {"model": MODEL, "schedule_mode": "sequential-unpaced", "config": config,
              "trace_sha256": document["sha256"], "requests": rows, "errors": [],
              "transports": [], "passed": False}
    origin = time.monotonic_ns()
    try:
        if library_loader is None:
            sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "integration/common/src"))
            from disk_scenarios import load_library
            library_loader = load_library
        if _config(config["lib"], config["members"]) != config:
            raise ValueError("library/config changed before loading")
        with ExitStack() as stack:
            clients = {}
            for namespace in sorted({row["namespace_hex"] for row in rows}):
                # Load once, and close all already-opened handles on setup failure.
                if not clients:
                    library = library_loader(config["lib"])
                client = stack.enter_context(client_factory(document, library, config["members"], bytes.fromhex(namespace)))
                clients[namespace] = client
                result["transports"].append({"namespace_hex": namespace, "mode": client.transport})
            for row in rows:
                row["started_ns"] = time.monotonic_ns() - origin
                try:
                    row["terminal"] = clients[row["namespace_hex"]].execute(row["request_id"])["outcome"]
                except Exception as error:
                    row.update(terminal="client_error", error=f"{type(error).__name__}: {error}")
                row["completed_ns"] = time.monotonic_ns() - origin
                if row["terminal"] != ("put_ack" if row["op"] == "put" else "get_hit"):
                    break  # Failure also blocks the rest of the current phase.
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    result["summary"] = _summary(rows)
    result["passed"] = not result["errors"] and all(
        row["terminal"] == ("put_ack" if row["op"] == "put" else "get_hit") for row in rows)
    result["elapsed_ns"] = time.monotonic_ns() - origin
    return result


def _verify(document, result, config):
    if (not isinstance(result, dict) or result.get("model") != MODEL
            or result.get("schedule_mode") != "sequential-unpaced"
            or result.get("trace_sha256") != document["sha256"]
            or trace.encode(result.get("config")) != trace.encode(config)):
        raise ValueError("wrong smoke model/trace/config")
    rows, errors = result.get("requests"), result.get("errors")
    if not isinstance(rows, list) or len(rows) != len(document["requests"]):
        raise ValueError("incomplete request ledger")
    if not isinstance(errors, list) or any(not isinstance(e, str) or not e for e in errors):
        raise ValueError("invalid lifecycle errors")
    previous, stopped = 0, False
    for source, row in zip(document["requests"], rows):
        if not isinstance(row, dict) or any(trace.encode(row.get(k)) != trace.encode(v) for k, v in source.items()):
            raise ValueError("request identity changed")
        terminal = row.get("terminal")
        if terminal == "not_attempted":
            if not stopped and not errors:
                raise ValueError("unexplained skipped request")
            if any(k in row for k in ("started_ns", "completed_ns", "error")):
                raise ValueError("unattempted request claims activity")
            stopped = True
            continue
        if stopped:
            raise ValueError("request executed after failure")
        start, end = row.get("started_ns"), row.get("completed_ns")
        if type(start) is not int or type(end) is not int or not previous <= start <= end:
            raise ValueError("invalid sequential call timing")
        previous = end
        expected = "put_ack" if source["op"] == "put" else "get_hit"
        if terminal not in ({expected, "client_error"} | ({"get_unresolved"} if source["op"] == "get" else set())):
            raise ValueError("invalid operation outcome")
        if terminal == "client_error" and (not isinstance(row.get("error"), str) or not row["error"]):
            raise ValueError("missing call error")
        if terminal != "client_error" and "error" in row:
            raise ValueError("non-error outcome claims call error")
        stopped = terminal != expected
    if type(result.get("elapsed_ns")) is not int or result["elapsed_ns"] < previous:
        raise ValueError("elapsed time precedes final completion")
    transports = result.get("transports")
    namespaces = sorted({r["namespace_hex"] for r in rows})
    if (not isinstance(transports, list) or any(not isinstance(t, dict) or set(t) != {"namespace_hex", "mode"}
            or not isinstance(t["mode"], str) or not t["mode"] for t in transports)
            or [t["namespace_hex"] for t in transports] != namespaces[:len(transports)]
            or len(transports) > len(namespaces)
            or any("started_ns" in r for r in rows) and len(transports) != len(namespaces)):
        raise ValueError("invalid client handle/transport ledger")
    summary = _summary(rows)
    passed = not errors and all(r["terminal"] == ("put_ack" if r["op"] == "put" else "get_hit") for r in rows)
    if (trace.encode(result.get("summary")) != trace.encode(summary)
            or type(result.get("passed")) is not bool or result["passed"] != passed):
        raise ValueError("summary/passed disagrees with ledger")
    return summary


def _command(source, result, config):
    return [sys.executable, "-I", str(Path(__file__).resolve()), str(source), str(result),
            "--lib", config["lib"], "--members", config["members"], "--child"]


def supervise(source, output, *, lib, members, process_timeout_sec=10):
    if (type(process_timeout_sec) not in (int, float) or not math.isfinite(process_timeout_sec)
            or not .05 <= process_timeout_sec <= 30):
        raise ValueError("process timeout must be finite in [0.05, 30] seconds")
    document = trace.load(source)
    counts, config = _bounded(document), _config(lib, members)
    output = Path(output).absolute()
    output.mkdir()  # New owned directory only, no overwrite/retry or server reset.
    frozen, raw = output / "trace.json", output / "child-result.json"
    frozen.write_bytes(trace.encode(document))
    report = {"model": "guarded-real-client-smoke", "status": "pending", "passed": False,
              "planned_requests": counts["requests"], "request_outcomes": "unknown",
              "attempted_requests": None, "completed_requests": None, "acked_put_bytes": None,
              "config": config, "trace_sha256": document["sha256"],
              "process_timeout_sec": process_timeout_sec, "actions": [], "child_pid": None,
              "returncode": None, "reaped": False, "command": _command(frozen, raw, config)}
    child, interruption = None, None
    started = time.monotonic_ns()
    env = {key: value for key, value in os.environ.items() if not key.startswith("DFKV_")}
    env.update(CLIENT_ENV)
    try:
        with (output / "stdout.log").open("xb") as stdout, (output / "stderr.log").open("xb") as stderr:
            child = subprocess.Popen(report["command"], stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, env=env)
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
                summary = _verify(document, result, config)
                if report["returncode"] != (0 if result["passed"] else 1):
                    raise ValueError("exit code contradicts child ledger")
                report.update(status="passed" if result["passed"] else "workload_failed", passed=result["passed"],
                              request_outcomes="complete_child_ledger", attempted_requests=summary["attempted"],
                              completed_requests=summary["completed"], acked_put_bytes=summary["acked_put_bytes"])
            except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
                report.update(status="invalid_result", error=f"{type(error).__name__}: {error}")
    (output / "report.json").write_bytes(trace.encode(report))
    if interruption is not None:
        raise interruption
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace")
    parser.add_argument("output", help="NEW owned directory (parent mode)")
    parser.add_argument("--lib", required=True)
    parser.add_argument("--members", required=True)
    parser.add_argument("--process-timeout-sec", type=float, default=10)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.child:
            document = trace.load(args.trace)
            result = run(document, _config(args.lib, args.members))
            with Path(args.output).open("xb") as output:
                output.write(trace.encode(result))
        else:
            result = supervise(args.trace, args.output, lib=args.lib, members=args.members,
                               process_timeout_sec=args.process_timeout_sec)
        print(trace.encode(result).decode("ascii"), end="")
        return 0 if result["passed"] else 1
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"smoke error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
