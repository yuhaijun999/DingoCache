#!/usr/bin/env python3
"""Offline virtual-time fake replay; not a wall-clock or real-service executor."""

import argparse
from collections import Counter, deque
import hashlib
import heapq
import json
from pathlib import Path
import sys

# Keep standalone execution (including python -I) independent of PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_trace as trace


def integer(name, value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")


class FakeBackend:
    """Fresh in-memory state, observed/committed at virtual completion time.

    Fault IDs are only for deterministic tests. A timed-out PUT can still commit;
    no real cancellation, threads, network, disk ACK or persistence is modeled.
    """

    def __init__(self, service_ns=1, *, durations=None, error_ids=(),
                 miss_ids=(), corrupt_ids=()):
        integer("service_ns", service_ns, 1, 10**12)
        self.service_ns = service_ns
        self.durations = dict(durations or {})
        self.error_ids = set(error_ids)
        self.miss_ids = set(miss_ids)
        self.corrupt_ids = set(corrupt_ids)
        for request_id in (set(self.durations) | self.error_ids |
                           self.miss_ids | self.corrupt_ids):
            integer("fault/duration request_id", request_id, 0, 1023)
        for duration in self.durations.values():
            integer("duration", duration, 1, 10**12)
        self.store = {}
        self.used = False

    def complete(self, row):
        request_id = row["request_id"]
        if request_id in self.error_ids:
            return "backend_error"
        key = (row["namespace_hex"], row["key_hex"])
        if row["op"] == "put":
            self.store[key] = trace.payload(row)
            return "put_ack"
        actual = None if request_id in self.miss_ids else self.store.get(key)
        if actual is None:
            return "get_miss"
        if request_id in self.corrupt_ids:
            actual = bytes([actual[0] ^ 1]) + actual[1:]
        if len(actual) != row["length"] or hashlib.sha256(actual).hexdigest() != row["payload_sha256"]:
            return "wrong_bytes"
        return "get_hit"


def run(document, backend, *, workers=1, queue_capacity=16, timeout_ns=1000000000):
    """Replay every planned arrival with FIFO waiting and bounded active calls.

    Completion wins a tie with its own deadline. Waiting expiry is processed
    BEFORE dispatch, so a queued request cannot start at its deadline. Timed-out
    active calls retain physical slots until completion, preserving late outcomes.
    These are virtual timestamps, never observed scheduler or client timings.
    """
    trace.validate(document)
    for name, value, minimum, maximum in (
            ("workers", workers, 1, 64), ("queue_capacity", queue_capacity, 0, 1024),
            ("timeout_ns", timeout_ns, 1, 10**12)):
        integer(name, value, minimum, maximum)
    if not isinstance(backend, FakeBackend) or backend.used:
        raise ValueError("a fresh FakeBackend is required")
    rows = [dict(row, deadline_ns=row["scheduled_ns"] + timeout_ns)
            for row in document["requests"]]
    configured_ids = (set(backend.durations) | backend.error_ids |
                      backend.miss_ids | backend.corrupt_ids)
    if any(index >= len(rows) for index in configured_ids):
        raise ValueError("fault/duration ID not present in trace")
    if any(rows[index]["op"] != "get" for index in backend.miss_ids | backend.corrupt_ids):
        raise ValueError("miss/corruption injection requires a GET")
    backend.used = True
    waiting, active = deque(), []
    next_arrival = 0
    now = 0
    peak_active = peak_queue = 0

    def terminal(row, outcome):
        if "terminal" in row:
            raise RuntimeError("request settled twice")
        row.update(terminal=outcome, terminal_ns=now,
                   latency_ns=now - row["scheduled_ns"])

    def start(index):
        row = rows[index]
        row.update(started_ns=now, queue_wait_ns=now - row["scheduled_ns"])
        duration = backend.durations.get(index, backend.service_ns)
        heapq.heappush(active, (now + duration, index))

    while next_arrival < len(rows) or active or waiting:
        events = []
        if next_arrival < len(rows):
            events.append(rows[next_arrival]["scheduled_ns"])
        if active:
            events.append(active[0][0])
        events.extend(rows[index]["deadline_ns"] for index in waiting)
        events.extend(rows[index]["deadline_ns"] for _, index in active
                      if "terminal" not in rows[index])
        now = min(events)
        # Ties between completions use request ID, not nondeterministic heap data.
        while active and active[0][0] == now:
            _, index = heapq.heappop(active)
            row = rows[index]
            outcome = backend.complete(row)
            row.update(completed_ns=now, backend_outcome=outcome,
                       service_ns=now - row["started_ns"],
                       late_completion="terminal" in row)
            if "terminal" not in row:
                terminal(row, outcome)
        retained = deque()
        for index in waiting:
            if rows[index]["deadline_ns"] == now:
                terminal(rows[index], "timeout")
            else:
                retained.append(index)
        waiting = retained
        for _, index in active:
            if "terminal" not in rows[index] and rows[index]["deadline_ns"] == now:
                terminal(rows[index], "timeout")
        while waiting and len(active) < workers:
            start(waiting.popleft())
        while next_arrival < len(rows) and rows[next_arrival]["scheduled_ns"] == now:
            index = next_arrival
            next_arrival += 1
            if len(active) < workers:
                start(index)
            elif len(waiting) < queue_capacity:
                waiting.append(index)
            else:
                terminal(rows[index], "queue_full")
        peak_active = max(peak_active, len(active))
        peak_queue = max(peak_queue, len(waiting))

    counts = dict(Counter(row["terminal"] for row in rows))
    summary = {"offered": len(rows), "terminal": counts,
               "started": sum("started_ns" in row for row in rows),
               "completed": sum("completed_ns" in row for row in rows),
               "late_completions": sum(row.get("late_completion", False) for row in rows),
               "wrong_bytes": sum(row.get("backend_outcome") == "wrong_bytes" for row in rows),
               "acked_put_bytes": sum(row["length"] for row in rows if row["terminal"] == "put_ack"),
               "hit_get_bytes": sum(row["length"] for row in rows if row["terminal"] == "get_hit"),
               "peak_active": peak_active, "peak_queue": peak_queue,
               "drained_ns": now}
    return {"model": "virtual-time-only", "trace_sha256": document["sha256"],
            "config": {"workers": workers, "queue_capacity": queue_capacity,
                       "timeout_ns": timeout_ns, "service_ns": backend.service_ns,
                       "durations": {str(k): v for k, v in backend.durations.items()},
                       "error_ids": sorted(backend.error_ids),
                       "miss_ids": sorted(backend.miss_ids),
                       "corrupt_ids": sorted(backend.corrupt_ids)},
            "passed": all(row["terminal"] in {"put_ack", "get_hit", "get_miss"}
                          for row in rows), "summary": summary, "requests": rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace")
    parser.add_argument("output", help="new output file; never overwrite")
    for name, default in (("workers", 1), ("queue_capacity", 16),
                          ("timeout_ns", 1000000000), ("service_ns", 1)):
        parser.add_argument("--" + name.replace("_", "-"), type=int, default=default)
    args = parser.parse_args(argv)
    try:
        result = run(trace.load(args.trace), FakeBackend(args.service_ns),
                     workers=args.workers, queue_capacity=args.queue_capacity,
                     timeout_ns=args.timeout_ns)
        with Path(args.output).open("xb") as output:
            output.write(trace.encode(result))
        print(json.dumps(result["summary"], sort_keys=True))
        return 0 if result["passed"] else 1
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"replay error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
