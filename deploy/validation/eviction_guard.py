#!/usr/bin/env python3
"""Bounded exact-child supervision for threaded FAKE replay, never a service."""

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_trace as trace
from eviction_replay import integer

TERM_GRACE_SEC = 1
KILL_GRACE_SEC = 1
GOOD = {"put_ack", "get_hit", "get_miss"}
TERMINALS = GOOD | {"queue_full", "timeout", "arrival_late", "backend_error", "wrong_bytes"}


def _command(source, result, config):
    script = Path(__file__).resolve().with_name("eviction_realtime.py")
    return [sys.executable, "-I", str(script), str(source), str(result),
            *[value for name, setting in config.items()
              for value in ("--" + name.replace("_", "-"), str(setting))]]


def _stop(child):
    """No PID searches or process-group signals: only this owned Popen child."""
    actions = []
    for name, grace in (("terminate", TERM_GRACE_SEC), ("kill", KILL_GRACE_SEC)):
        if child.poll() is not None:
            break
        action = {"action": name, "grace_sec": grace}
        actions.append(action)
        try:
            getattr(child, name)()
            child.wait(timeout=grace)
        except (OSError, subprocess.TimeoutExpired) as error:
            action["error"] = f"{type(error).__name__}: {error}"
    return actions


def _verify(document, result, config):
    """Minimum whole-ledger consistency, not independent performance acceptance."""
    if not isinstance(result, dict) or result.get("model") != "monotonic-threaded-fake-only":
        raise ValueError("wrong child result model")
    expected_config = dict(config, durations={}, error_ids=[], miss_ids=[], corrupt_ids=[])
    if (result.get("trace_sha256") != document["sha256"]
            or trace.encode(result.get("config")) != trace.encode(expected_config)):
        raise ValueError("child trace/config identity mismatch")
    rows = result.get("requests")
    if not isinstance(rows, list) or len(rows) != len(document["requests"]):
        raise ValueError("incomplete child request ledger")
    for original, row in zip(document["requests"], rows):
        if not isinstance(row, dict) or trace.encode({k: row.get(k) for k in original}) != trace.encode(original):
            raise ValueError("child request identity mismatch")
        if row.get("terminal") not in TERMINALS:
            raise ValueError("missing/invalid terminal")
        for name in ("terminal_ns", "settled_ns", "deadline_ns"):
            integer(name, row.get(name), 0, 10**18)
        if not row["scheduled_ns"] <= row["terminal_ns"] <= row["settled_ns"]:
            raise ValueError("invalid child settlement times")
        if row["deadline_ns"] != row["scheduled_ns"] + config["timeout_ns"]:
            raise ValueError("child deadline mismatch")
        if ("started_ns" in row) != ("completed_ns" in row):
            raise ValueError("child calls not drained")
        if "started_ns" in row:
            integer("started_ns", row["started_ns"], 0, 10**18)
            integer("completed_ns", row["completed_ns"], 0, 10**18)
            for name in ("admitted_ns", "submitted_ns"):
                integer(name, row.get(name), 0, 10**18)
            if not row["scheduled_ns"] <= row["admitted_ns"] <= row["submitted_ns"] <= row["started_ns"]:
                raise ValueError("child call missing admission/submission provenance")
            if not row["scheduled_ns"] <= row["started_ns"] < row["deadline_ns"] or row["completed_ns"] < row["started_ns"]:
                raise ValueError("invalid child call times")
        if row["terminal"] in GOOD:
            allowed = {"put_ack"} if row["op"] == "put" else {"get_hit", "get_miss"}
            if row["terminal"] not in allowed or row["terminal_ns"] != row.get("completed_ns"):
                raise ValueError("child success contradicts operation/completion")
            if row.get("backend_outcome") != row["terminal"] or "completed_ns" not in row or row["completed_ns"] > row["deadline_ns"]:
                raise ValueError("unsubstantiated child success")
        if row["terminal"] == "timeout" and row["terminal_ns"] != row["deadline_ns"]:
            raise ValueError("child timeout mismatch")
    expected = {"offered": len(rows), "terminal": dict(Counter(row["terminal"] for row in rows)),
                "submitted": sum("submitted_ns" in row for row in rows),
                "started": sum("started_ns" in row for row in rows),
                "completed": sum("completed_ns" in row for row in rows),
                "late_completions": sum(row.get("late_completion", False) for row in rows),
                "wrong_bytes": sum(row.get("backend_outcome") == "wrong_bytes" for row in rows),
                "acked_put_bytes": sum(row["length"] for row in rows if row["terminal"] == "put_ack"),
                "hit_get_bytes": sum(row["length"] for row in rows if row["terminal"] == "get_hit")}
    summary = result.get("summary")
    if not isinstance(summary, dict) or trace.encode({k: summary.get(k) for k in expected}) != trace.encode(expected):
        raise ValueError("child summary does not reconcile")
    passed = all(row["terminal"] in GOOD for row in rows) and expected["wrong_bytes"] == 0
    if type(result.get("passed")) is not bool or result["passed"] != passed:
        raise ValueError("child passed flag contradicts ledger")
    return summary


def supervise(source, output, *, process_timeout_sec=30, workers=1,
              queue_capacity=16, timeout_ns=1000000000, service_ns=1):
    if (type(process_timeout_sec) not in (int, float) or not .05 <= process_timeout_sec <= 60
            or not math.isfinite(process_timeout_sec)):
        raise ValueError("process_timeout_sec must be finite in [0.05, 60]")
    config = dict(workers=workers, queue_capacity=queue_capacity,
                  timeout_ns=timeout_ns, service_ns=service_ns)
    for name, low, high in (("workers", 1, 16), ("queue_capacity", 0, 1024),
                            ("timeout_ns", 1, 5000000000), ("service_ns", 1, 1000000000)):
        integer(name, config[name], low, high)
    document = trace.load(source)
    planned = trace.validate(document)
    if document["requests"][-1]["scheduled_ns"] > 5000000000 or planned["requests"] * service_ns > 10000000000:
        raise ValueError("fake replay limited to 5s arrival span and 10s total waits")
    output = Path(output).absolute()
    output.mkdir()  # Exclusive ownership; no parents, overwrite, resume or retry.
    frozen = output / "trace.json"
    frozen.write_bytes(trace.encode(document))
    report = {"model": "process-guard-fake-only", "passed": False,
              "status": "pending", "planned_requests": planned["requests"],
              "request_outcomes": "unknown", "completed_requests": None,
              "acked_put_bytes": None, "trace_sha256": document["sha256"],
              "trace_file_sha256": hashlib.sha256(frozen.read_bytes()).hexdigest(),
              "config": config, "process_timeout_sec": process_timeout_sec,
              "child_pid": None, "returncode": None, "reaped": False,
              "actions": [], "command": _command(frozen, output / "child-result.json", config)}
    child = None
    interruption = None
    started = time.monotonic_ns()
    try:
        with (output / "stdout.log").open("xb") as stdout, (output / "stderr.log").open("xb") as stderr:
            child = subprocess.Popen(report["command"], stdin=subprocess.DEVNULL,
                                     stdout=stdout, stderr=stderr)
            report["child_pid"] = child.pid
            remaining = process_timeout_sec - (time.monotonic_ns() - started) / 1e9
            if remaining <= 0:
                report["status"] = "process_timeout"
            else:
                try:
                    child.wait(timeout=remaining)
                    # Conservatively reject late observation, even if wait reaped it.
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
        raw = output / "child-result.json"
        if report["returncode"] not in (0, 1):
            report["status"] = "child_exit"
        else:
            try:
                result = trace.load(raw)
                summary = _verify(document, result, config)
                if report["returncode"] != (0 if result["passed"] else 1):
                    raise ValueError("child exit code contradicts passed flag")
                report.update(status="passed" if result["passed"] else "workload_failed",
                              passed=result["passed"], request_outcomes="complete_child_ledger",
                              completed_requests=summary["completed"],
                              acked_put_bytes=summary["acked_put_bytes"])
            except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
                report.update(status="invalid_result", error=f"{type(error).__name__}: {error}")
    with (output / "report.json").open("x") as destination:
        destination.write(trace.encode(report).decode("ascii"))
    if interruption is not None:
        raise interruption
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace")
    parser.add_argument("output", help="NEW owned directory")
    parser.add_argument("--process-timeout-sec", type=float, default=30)
    for name, default in (("workers", 1), ("queue_capacity", 16),
                          ("timeout_ns", 1000000000), ("service_ns", 1)):
        parser.add_argument("--" + name.replace("_", "-"), type=int, default=default)
    args = parser.parse_args(argv)
    try:
        report = supervise(args.trace, args.output, process_timeout_sec=args.process_timeout_sec,
                           workers=args.workers, queue_capacity=args.queue_capacity,
                           timeout_ns=args.timeout_ns, service_ns=args.service_ns)
        print(json.dumps(report, sort_keys=True))
        return 0 if report["passed"] else 1
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"guard error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
