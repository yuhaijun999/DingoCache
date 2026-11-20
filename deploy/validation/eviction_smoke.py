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
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_trace as trace
import eviction_batch as batch
import eviction_dedup as dedup
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


def _config(lib, members, *, batch_gets=False, tcp=False, cpu_dedup=False,
            cpu_dedup_token=None):
    if any(type(value) is not bool for value in (batch_gets, tcp, cpu_dedup)):
        raise ValueError("batch_gets and tcp must be bool")
    if cpu_dedup:
        if not batch_gets:
            raise ValueError("CPU dedup requires --batch-gets")
        dedup.namespace(cpu_dedup_token, b"")
    elif cpu_dedup_token is not None:
        raise ValueError("unexpected CPU dedup token")
    match = re.fullmatch(r"[A-Za-z0-9_-]+=127\.0\.0\.1:([0-9]{1,5})", members)
    if match is None or not 1 <= int(match[1]) <= 65535:
        raise ValueError("smoke requires one explicit loopback member: name=127.0.0.1:port")
    library = Path(lib).resolve(strict=True)
    if not library.is_file():
        raise ValueError("library must be a regular file")
    config = {"lib": str(library), "lib_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
              "members": members, "client_environment": dict(CLIENT_ENV, DFKV_RDMA="0") if tcp else CLIENT_ENV}
    if batch_gets:
        config["batch_gets"] = True
    if cpu_dedup:
        config.update(cpu_dedup=True, cpu_dedup_token=cpu_dedup_token)
        config["client_environment"] = dict(config["client_environment"], **dedup.ENV)
    return config


def _summary(rows):
    return {"planned": len(rows), "attempted": sum("started_ns" in r for r in rows),
            "completed": sum("completed_ns" in r for r in rows),
            "acked_put_bytes": sum(r["length"] for r in rows if r["terminal"] == "put_ack"),
            "hit_get_bytes": sum(r["length"] for r in rows if r["terminal"] == "get_hit")}


def run(document, config, *, library_loader=None, client_factory=TraceClient):
    """Strict ACK/hit gates with optional namespace-batched GET windows.

    Source scheduled_ns is identity only: no pacing or fixed-arrival claims. An
    explicit miss or legacy unresolved no-hit fails the smoke. No resident/heat
    snapshot is inferred.
    """
    _bounded(document)
    rows = [dict(row, terminal="not_attempted") for row in document["requests"]]
    result = {"model": MODEL, "schedule_mode": "sequential-unpaced", "config": config,
              "trace_sha256": document["sha256"], "requests": rows, "errors": [],
              "transports": [], "passed": False}
    batch_gets = config.get("batch_gets", False)
    if batch_gets:
        result.update(model=batch.MODEL, schedule_mode=batch.SCHEDULE,
                      groups=[{"group_id": index, "request_ids": ids, "state": "not_attempted"}
                              for index, ids in enumerate(batch.plan(document))])
    cpu_dedup = config.get("cpu_dedup", False)
    if cpu_dedup:
        result.update(model=batch.DEDUP_MODEL,
                      cpu_dedup=[dict(item, before=None, after=None) for item in
                                 dedup.identities(document, config["cpu_dedup_token"])])
    origin = time.monotonic_ns()
    try:
        if library_loader is None:
            sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "integration/common/src"))
            from disk_scenarios import load_library
            library_loader = load_library
        if _config(config["lib"], config["members"], batch_gets=batch_gets,
                   tcp=config["client_environment"].get("DFKV_RDMA") == "0",
                   cpu_dedup=cpu_dedup, cpu_dedup_token=config.get("cpu_dedup_token")) != config:
            raise ValueError("library/config changed before loading")
        with ExitStack() as stack:
            clients = {}
            for namespace in sorted({row["namespace_hex"] for row in rows}):
                # Load once, and close all already-opened handles on setup failure.
                if not clients:
                    library = library_loader(config["lib"])
                options = {"cpu_dedup_token": config["cpu_dedup_token"]} if cpu_dedup else {}
                client = stack.enter_context(client_factory(document, library, config["members"],
                                                            bytes.fromhex(namespace), **options))
                clients[namespace] = client
                result["transports"].append({"namespace_hex": namespace, "mode": client.transport})
                if cpu_dedup:
                    entry = result["cpu_dedup"][len(clients) - 1]
                    entry["before"] = client.cpu_dedup_metrics()
                    if any(entry["before"].values()):
                        raise ValueError("CPU dedup counters nonzero before workload")
            if batch_gets:
                batch.execute(rows, clients, result["groups"], origin)
            else:
                for row in rows:
                    row["started_ns"] = time.monotonic_ns() - origin
                    try:
                        row["terminal"] = clients[row["namespace_hex"]].execute(row["request_id"])["outcome"]
                    except Exception as error:
                        row.update(terminal="client_error", error=f"{type(error).__name__}: {error}")
                    row["completed_ns"] = time.monotonic_ns() - origin
                    if row["terminal"] != ("put_ack" if row["op"] == "put" else "get_hit"):
                        break  # Failure also blocks the rest of the current phase.
            if cpu_dedup:
                for entry in result["cpu_dedup"]:
                    entry["after"] = clients[entry["namespace_hex"]].cpu_dedup_metrics()
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    result["summary"] = batch.summary(rows, result["groups"]) if batch_gets else _summary(rows)
    if cpu_dedup:
        result["summary"]["cpu_dedup_observations"] = dedup.observations(result["cpu_dedup"])
    result["passed"] = not result["errors"] and all(
        row["terminal"] == ("put_ack" if row["op"] == "put" else "get_hit") for row in rows)
    result["elapsed_ns"] = time.monotonic_ns() - origin
    return result


def _verify(document, result, config):
    # The frozen parent configuration, never the child's model, picks the rules.
    if config.get("batch_gets") is True:
        return batch.verify(document, result, config)
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
        if terminal not in ({expected, "client_error"} | ({"get_unresolved", "get_miss"} if source["op"] == "get" else set())):
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
    command = [sys.executable, "-I", str(Path(__file__).resolve()), str(source), str(result),
               "--lib", config["lib"], "--members", config["members"], "--child"]
    if config.get("batch_gets") is True:
        command.append("--batch-gets")
    if config.get("cpu_dedup") is True:
        command.extend(["--cpu-dedup", "--cpu-dedup-token", config["cpu_dedup_token"]])
    if config["client_environment"]["DFKV_RDMA"] == "0":
        command.append("--tcp")
    return command


def supervise(source, output, *, lib, members, process_timeout_sec=10, batch_gets=False,
              tcp=False, cpu_dedup=False):
    if (type(process_timeout_sec) not in (int, float) or not math.isfinite(process_timeout_sec)
            or not .05 <= process_timeout_sec <= 30):
        raise ValueError("process timeout must be finite in [0.05, 30] seconds")
    document = trace.load(source)
    counts, config = _bounded(document), _config(
        lib, members, batch_gets=batch_gets, tcp=tcp, cpu_dedup=cpu_dedup,
        cpu_dedup_token=uuid.uuid4().hex if cpu_dedup else None)
    if cpu_dedup:
        dedup.require_fresh(document, config["cpu_dedup_token"])
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
    if batch_gets:
        report.update(get_outcomes=None, get_adapter_calls=None, get_failed_adapter_calls=None)
    if cpu_dedup:
        report.update(cpu_dedup_observations=None,
                      possible_retained_shm_paths=[item["possible_shm_path"] for item in
                                                   dedup.identities(document, config["cpu_dedup_token"])])
    child, interruption = None, None
    started = time.monotonic_ns()
    env = {key: value for key, value in os.environ.items() if not key.startswith("DFKV_")}
    env.update(config["client_environment"])
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
    if cpu_dedup:
        report["shm_cleanup"] = "not_attempted_no_exclusive_creator_proof"
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
                if batch_gets:
                    report.update({key: summary[key] for key in
                                   ("get_outcomes", "get_adapter_calls", "get_failed_adapter_calls")})
                if cpu_dedup:
                    report["cpu_dedup_observations"] = summary["cpu_dedup_observations"]
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
    parser.add_argument("--tcp", action="store_true", help="explicit TCP correctness smoke; default still requests RDMA/rxe0")
    parser.add_argument("--batch-gets", action="store_true", help="opt-in namespace-batched correctness mode; not arrival/performance replay")
    parser.add_argument("--cpu-dedup", action="store_true", help="explicit CPU status path; requires --batch-gets; remaps backend namespaces")
    parser.add_argument("--cpu-dedup-token", help=argparse.SUPPRESS)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.cpu_dedup_token is not None and not args.child:
            raise ValueError("CPU dedup token is parent-generated, not a replay option")
        if args.child:
            document = trace.load(args.trace)
            result = run(document, _config(args.lib, args.members, batch_gets=args.batch_gets, tcp=args.tcp,
                                           cpu_dedup=args.cpu_dedup, cpu_dedup_token=args.cpu_dedup_token))
            with Path(args.output).open("xb") as output:
                output.write(trace.encode(result))
        else:
            result = supervise(args.trace, args.output, lib=args.lib, members=args.members,
                               process_timeout_sec=args.process_timeout_sec, batch_gets=args.batch_gets,
                               tcp=args.tcp, cpu_dedup=args.cpu_dedup)
        print(trace.encode(result).decode("ascii"), end="")
        return 0 if result["passed"] else 1
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"smoke error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
