#!/usr/bin/env python3
"""Monotonic-clock threaded fake replay. Never connects to a service."""

import argparse
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_trace as trace
from eviction_replay import FakeBackend, integer


class SleepingFakeBackend(FakeBackend):
    """Only simulated service waiting is outside the scheduler/store lock."""

    def wait(self, row):
        time.sleep(self.durations.get(row["request_id"], self.service_ns) / 1e9)


def run(document, backend, *, workers=1, queue_capacity=16, timeout_ns=1000000000):
    trace.validate(document)
    for name, value, low, high in (("workers", workers, 1, 16),
                                  ("queue_capacity", queue_capacity, 0, 1024),
                                  ("timeout_ns", timeout_ns, 1, 5000000000)):
        integer(name, value, low, high)
    if not isinstance(backend, SleepingFakeBackend) or backend.used:
        raise ValueError("a fresh SleepingFakeBackend is required")
    rows = [dict(row, deadline_ns=row["scheduled_ns"] + timeout_ns)
            for row in document["requests"]]
    ids = set(backend.durations) | backend.error_ids | backend.miss_ids | backend.corrupt_ids
    if any(index >= len(rows) for index in ids):
        raise ValueError("fault/duration ID not present in trace")
    if any(rows[index]["op"] != "get" for index in backend.miss_ids | backend.corrupt_ids):
        raise ValueError("miss/corruption injection requires a GET")
    durations = [backend.durations.get(index, backend.service_ns) for index in range(len(rows))]
    if (rows[-1]["scheduled_ns"] > 5000000000 or max(durations) > 1000000000
            or sum(durations) > 10000000000):
        raise ValueError("fake replay limited to 5s arrival span, 1s per call, 10s total waits")
    backend.used = True
    condition = threading.Condition()
    waiting, reserved, running = deque(), set(), set()
    futures = []
    peak_reserved = peak_running = peak_queue = 0
    next_arrival = 0
    interval = document["config"]["interval_ns"]
    origin = time.monotonic_ns()

    def elapsed():
        return time.monotonic_ns() - origin

    def settle(row, outcome, effective, observed):
        # All ledger/store mutations are serialized by condition, including workers.
        if "terminal" in row:
            raise RuntimeError("request settled twice")
        row.update(terminal=outcome, terminal_ns=effective, settled_ns=observed,
                   latency_ns=effective - row["scheduled_ns"],
                   observed_latency_ns=observed - row["scheduled_ns"])

    def execute(index):
        nonlocal peak_running
        row = rows[index]
        with condition:
            now = elapsed()
            if "terminal" in row or now >= row["deadline_ns"]:
                if "terminal" not in row:
                    settle(row, "timeout", row["deadline_ns"], now)
                row["worker_expired_ns"] = now
                reserved.remove(index)
                condition.notify_all()
                return
            row.update(started_ns=now, queue_wait_ns=now - row["admitted_ns"],
                       dispatch_lag_ns=now - row["scheduled_ns"])
            running.add(index)
            peak_running = max(peak_running, len(running))
        error_text = None
        try:
            backend.wait(row)
        except Exception as error:
            error_text = f"{type(error).__name__}: {error}"
        with condition:
            try:
                outcome = backend.complete(row) if error_text is None else "backend_error"
            except Exception as error:
                outcome = "backend_error"
                error_text = f"{type(error).__name__}: {error}"
            if error_text is not None:
                row["backend_error"] = error_text
            now = elapsed()
            row.update(completed_ns=now, backend_outcome=outcome,
                       service_ns=now - row["started_ns"],
                       late_completion=now > row["deadline_ns"])
            if "terminal" not in row:
                if now <= row["deadline_ns"]:
                    settle(row, outcome, now, now)
                else:
                    settle(row, "timeout", row["deadline_ns"], now)
            running.remove(index)
            reserved.remove(index)
            condition.notify_all()

    # Only reserved calls are submitted: the executor never owns our waiting queue.
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dfkv-fake-replay") as pool:
        def submit(index):
            nonlocal peak_reserved
            reserved.add(index)
            rows[index]["submitted_ns"] = elapsed()
            futures.append(pool.submit(execute, index))
            peak_reserved = max(peak_reserved, len(reserved))

        with condition:
            while next_arrival < len(rows) or reserved or waiting:
                now = elapsed()
                retained = deque()
                for index in waiting:
                    row = rows[index]
                    if now >= row["deadline_ns"]:
                        settle(row, "timeout", row["deadline_ns"], now)
                    else:
                        retained.append(index)
                waiting = retained
                for index in reserved:
                    row = rows[index]
                    if "terminal" not in row and now > row["deadline_ns"]:
                        settle(row, "timeout", row["deadline_ns"], now)
                while waiting and len(reserved) < workers:
                    index = waiting.popleft()
                    if elapsed() >= rows[index]["deadline_ns"]:
                        settle(rows[index], "timeout", rows[index]["deadline_ns"], elapsed())
                    else:
                        submit(index)
                while next_arrival < len(rows) and rows[next_arrival]["scheduled_ns"] <= elapsed():
                    index = next_arrival
                    next_arrival += 1
                    row = rows[index]
                    now = elapsed()
                    row.update(arrival_observed_ns=now,
                               schedule_lag_ns=now - row["scheduled_ns"])
                    if now >= row["deadline_ns"]:
                        settle(row, "timeout", row["deadline_ns"], now)
                    elif now >= row["scheduled_ns"] + interval:
                        # Missed slots fail, rather than creating a catch-up burst.
                        settle(row, "arrival_late", now, now)
                    elif len(reserved) < workers:
                        row["admitted_ns"] = now
                        submit(index)
                    elif len(waiting) < queue_capacity:
                        row["admitted_ns"] = now
                        waiting.append(index)
                    else:
                        settle(row, "queue_full", now, now)
                peak_queue = max(peak_queue, len(waiting))
                if next_arrival == len(rows) and not reserved and not waiting:
                    break
                wake = [rows[index]["deadline_ns"] for index in waiting]
                wake.extend(rows[index]["deadline_ns"] for index in reserved
                            if "terminal" not in rows[index])
                if next_arrival < len(rows):
                    wake.append(rows[next_arrival]["scheduled_ns"])
                condition.wait(max(0, min(wake) - elapsed()) / 1e9 if wake else None)
        for future in futures:
            future.result()
    summary = {"offered": len(rows), "terminal": dict(Counter(row["terminal"] for row in rows)),
               "submitted": len(futures), "started": sum("started_ns" in row for row in rows),
               "completed": sum("completed_ns" in row for row in rows),
               "late_completions": sum(row.get("late_completion", False) for row in rows),
               "wrong_bytes": sum(row.get("backend_outcome") == "wrong_bytes" for row in rows),
               "acked_put_bytes": sum(row["length"] for row in rows if row["terminal"] == "put_ack"),
               "hit_get_bytes": sum(row["length"] for row in rows if row["terminal"] == "get_hit"),
               "peak_reserved": peak_reserved, "peak_running": peak_running,
               "peak_queue": peak_queue, "drained_ns": elapsed()}
    return {"model": "monotonic-threaded-fake-only", "trace_sha256": document["sha256"],
            "config": {"workers": workers, "queue_capacity": queue_capacity,
                       "timeout_ns": timeout_ns, "service_ns": backend.service_ns,
                       "durations": {str(k): v for k, v in backend.durations.items()},
                       "error_ids": sorted(backend.error_ids), "miss_ids": sorted(backend.miss_ids),
                       "corrupt_ids": sorted(backend.corrupt_ids)},
            "passed": all(row["terminal"] in {"put_ack", "get_hit", "get_miss"} for row in rows),
            "summary": summary, "requests": rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace")
    parser.add_argument("output", help="new file; never overwrite")
    for name, default in (("workers", 1), ("queue_capacity", 16),
                          ("timeout_ns", 1000000000), ("service_ns", 1)):
        parser.add_argument("--" + name.replace("_", "-"), type=int, default=default)
    args = parser.parse_args(argv)
    try:
        result = run(trace.load(args.trace), SleepingFakeBackend(args.service_ns),
                     workers=args.workers, queue_capacity=args.queue_capacity,
                     timeout_ns=args.timeout_ns)
        with Path(args.output).open("xb") as output:
            output.write(trace.encode(result))
        print(json.dumps(result["summary"], sort_keys=True))
        return 0 if result["passed"] else 1
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"realtime replay error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
