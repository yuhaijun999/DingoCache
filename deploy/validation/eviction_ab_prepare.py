#!/usr/bin/env python3
"""Offline A/B preparation and six guarded FAKE runs; never formal sampling."""

import argparse
import hashlib
import math
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_guard as guard
import eviction_trace as trace
from eviction_replay import integer

SCHEMA = "dfkv-ab-preparation-v1"
ORDER = ["A", "B", "B", "A", "A", "B"]
TOOLS = ("eviction_ab_prepare.py", "eviction_trace.py", "eviction_replay.py",
         "eviction_realtime.py", "eviction_guard.py")
BLOCKERS = ["business_trace_and_weights", "target_resources_and_effective_config",
            "old_version_O_baseline", "migration_M_applicability_and_baseline",
            "independent_roles_and_visibility_audit", "F01_F05_numeric_registration",
            "external_freeze_and_approvals", "formal_real_backend_not_implemented"]
FIELDS = {"capacity_bytes", "extent_bytes", "slot_sizes", "ram_bytes", "coalescing",
          "workers", "queue_capacity", "timeout_ns", "process_timeout_sec"}


def safe_path(path):
    path = Path(path).absolute()
    resolved = path.resolve()
    if resolved == Path("/mnt/disk1") or Path("/mnt/disk1") in resolved.parents:
        raise ValueError("host /mnt/disk1 is prohibited")
    return path


def regular_file(path):
    path = safe_path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"expected regular nonsymlink file: {path}")
    return path


def load(path):
    # Reject aliases before the JSON loader can open a prohibited target.
    return trace.load(regular_file(path))


def file_sha(path):
    path = regular_file(path)
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def same(left, right):
    return trace.encode(left) == trace.encode(right)


def validate_config(config):
    if not isinstance(config, dict) or set(config) != FIELDS:
        raise ValueError("invalid common configuration fields")
    # Resource declarations may remain unknown. Fake runs cannot validate them.
    for name in ("capacity_bytes", "extent_bytes", "ram_bytes"):
        if config[name] is not None:
            integer(name, config[name], 0 if name == "ram_bytes" else 1, 2**63 - 1)
    slots = config["slot_sizes"]
    if slots is not None:
        if not isinstance(slots, list) or not 1 <= len(slots) <= 256:
            raise ValueError("slot_sizes must be null or a bounded list")
        for size in slots:
            integer("slot size", size, 1, 2**63 - 1)
        if slots != sorted(set(slots)):
            raise ValueError("slot_sizes must be unique and increasing")
    if (config["capacity_bytes"] is not None and config["extent_bytes"] is not None
            and config["capacity_bytes"] < config["extent_bytes"]):
        raise ValueError("capacity_bytes cannot be less than extent_bytes")
    if slots and config["extent_bytes"] is not None and max(slots) > config["extent_bytes"]:
        raise ValueError("slot size exceeds declared extent")
    if config["coalescing"] is not None and type(config["coalescing"]) is not bool:
        raise ValueError("coalescing must be null or boolean")
    for name, low, high in (("workers", 1, 16), ("queue_capacity", 0, 1024),
                            ("timeout_ns", 1, 5000000000)):
        integer(name, config[name], low, high)
    value = config["process_timeout_sec"]
    if type(value) not in (int, float) or not math.isfinite(value) or not .05 <= value <= 60:
        raise ValueError("process_timeout_sec must be finite in [0.05, 60]")
    return config


def write(path, value):
    with path.open("xb") as destination:
        destination.write(trace.encode(value))


def expected_plan(binary, source_revision, config, document):
    if not isinstance(source_revision, str) or not re.fullmatch(r"[0-9a-f]{40}", source_revision):
        raise ValueError("source_revision must be a full git SHA; provenance is declared, not proved")
    validate_config(config)
    identity = {"path": str(safe_path(binary)), "sha256": file_sha(binary)}
    tools = {name: file_sha(Path(__file__).with_name(name)) for name in TOOLS}
    groups = {name: {"binary": identity, "engine": "slab", "policy": policy,
                     "ack": "disk", "common_config": config}
              for name, policy in (("A", "legacy"), ("B", "lifetime"))}
    return {"schema": SCHEMA, "scope": "offline-synthetic-preparation-only",
            "formal_run_allowed": False, "gpu": "excluded", "source_revision": source_revision,
            "source_provenance": "caller-declaration-not-binary-to-source-proof",
            "trace_file_sha256": hashlib.sha256(trace.encode(document)).hexdigest(),
            "trace_sha256": document["sha256"], "tools": tools, "groups": groups,
            "run_order": ORDER, "external_blockers": BLOCKERS,
            "references": {"O": "blocked-v2.33.0-separate-binary-and-directories",
                           "M": "blocked-RAM-ACK-migration-reference-not-algorithm-benefit"}}


def create(output, *, binary, source_revision, config):
    output = safe_path(output)
    binary = safe_path(binary)
    document = trace.generate(objects=2, rounds=2, length=128, interval_ns=80000000)
    plan = expected_plan(binary, source_revision, config, document)
    output.mkdir()  # Exclusive owner, no overwrite, resume or retry.
    write(output / "trace.json", document)
    write(output / "plan.json", plan)
    return {"plan_sha256": file_sha(output / "plan.json"), "formal_run_allowed": False}


def check(package, plan_sha256):
    package = safe_path(package)
    if file_sha(package / "plan.json") != plan_sha256:
        raise ValueError("plan does not match external frozen SHA256")
    plan = load(package / "plan.json")
    document = load(package / "trace.json")
    trace.validate(document)
    try:
        group = plan["groups"]["A"]
        expected = expected_plan(group["binary"]["path"], plan["source_revision"],
                                 group["common_config"], document)
    except (KeyError, TypeError) as error:
        raise ValueError("invalid preparation plan") from error
    if not same(plan, expected) or file_sha(package / "trace.json") != plan["trace_file_sha256"]:
        raise ValueError("plan, trace, binary, configuration or tools drifted")
    return plan, document


def replay_config(plan):
    common = plan["groups"]["A"]["common_config"]
    return {key: common[key] for key in ("workers", "queue_capacity", "timeout_ns")}


def audit_run(directory, document, plan):
    """Reconcile retained raw ledger against the supervisor, not its passed flag."""
    report = load(directory / "report.json")
    config = dict(replay_config(plan), service_ns=1, staged=True)
    if (report.get("model") != "process-guard-fake-only"
            or not same(report.get("config"), config)
            or not same(report.get("process_timeout_sec"), plan["groups"]["A"]["common_config"]["process_timeout_sec"])
            or report.get("trace_sha256") != document["sha256"]
            or report.get("trace_file_sha256") != file_sha(directory / "trace.json")
            or not same(load(directory / "trace.json"), document)
            or report.get("planned_requests") != len(document["requests"])
            or type(report.get("passed")) is not bool):
        raise ValueError("supervisor identity/configuration mismatch")
    if report.get("request_outcomes") == "complete_child_ledger":
        result = load(directory / "child-result.json")
        summary = guard._verify(document, result, config)
        expected_status = "passed" if result["passed"] else "workload_failed"
        expected = {"status": expected_status, "passed": result["passed"],
                    "completed_requests": summary["completed"], "offered_requests": summary["offered"],
                    "acked_put_bytes": summary["acked_put_bytes"], "reaped": True,
                    "returncode": 0 if result["passed"] else 1}
        if not same({key: report.get(key) for key in expected}, expected):
            raise ValueError("supervisor contradicts complete ledger")
    else:
        if (report.get("request_outcomes") != "unknown" or report["passed"]
                or report.get("status") not in {"process_timeout", "supervisor_error", "cleanup_failed", "child_exit", "invalid_result"}
                or any(report.get(key) is not None for key in
                       ("completed_requests", "offered_requests", "acked_put_bytes"))):
            raise ValueError("unknown outcomes converted to success or zero")
    return {"status": report["status"], "passed": report["passed"],
            "planned_requests": len(document["requests"]),
            "completed_requests": report["completed_requests"],
            "offered_requests": report["offered_requests"],
            "acked_put_bytes": report["acked_put_bytes"]}


def artifact_hashes(directory):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("expected owned nonsymlink run directory")
    result = {}
    allowed = {"report.json", "trace.json", "stdout.log", "stderr.log", "child-result.json"}
    for path in sorted(directory.iterdir()):
        if path.name not in allowed:
            raise ValueError("unexpected run artifact name")
        if not path.is_file() or path.is_symlink():
            raise ValueError("unexpected run artifact")
        result[path.name] = file_sha(path)
    return result


def run_fake(package, output, plan_sha256):
    plan, document = check(package, plan_sha256)
    output = safe_path(output)
    output.mkdir()
    report = {"schema": SCHEMA, "scope": "guarded-fake-correctness-only",
              "formal_run_allowed": False, "policy_effects_simulated": False,
              "plan_sha256": plan_sha256, "passed": False, "runs": []}
    failed = False
    interruption = None
    for index, group in enumerate(ORDER):
        entry = {"run_id": f"{index + 1:02d}-{group}", "group": group, "status": "not_attempted"}
        report["runs"].append(entry)
        if failed:
            continue
        directory = output / entry["run_id"]
        try:
            guard.supervise(safe_path(package) / "trace.json", directory, staged=True,
                            process_timeout_sec=plan["groups"]["A"]["common_config"]["process_timeout_sec"],
                            **replay_config(plan))
            entry.update(audit_run(directory, document, plan))
            if directory.exists():
                entry["artifacts"] = artifact_hashes(directory)
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
            entry.update(status="preparation_error", passed=False,
                         planned_requests=len(document["requests"]), completed_requests=None,
                         offered_requests=None, acked_put_bytes=None,
                         error=f"{type(error).__name__}: {error}")
        except BaseException as error:
            # The child supervisor has already reaped its owned process. Preserve
            # the complete outer ledger before propagating process interruption.
            interruption = error
            entry.update(status="preparation_error", passed=False,
                         planned_requests=len(document["requests"]), completed_requests=None,
                         offered_requests=None, acked_put_bytes=None,
                         error=f"{type(error).__name__}: {error}")
        failed = not entry.get("passed", False)
    # Recheck after execution: concurrent drift invalidates, never salvages, runs.
    try:
        check(package, plan_sha256)
    except (OSError, ValueError, TypeError, KeyError) as error:
        report["postcheck_error"] = str(error)
        failed = True
    report["passed"] = not failed
    write(output / "report.json", report)
    if interruption is not None:
        raise interruption
    return dict(report, report_sha256=file_sha(output / "report.json"))


def check_results(package, output, plan_sha256, report_sha256):
    plan, document = check(package, plan_sha256)
    output = safe_path(output)
    if file_sha(output / "report.json") != report_sha256:
        raise ValueError("result does not match external SHA256")
    report = load(output / "report.json")
    allowed_fields = {"schema", "scope", "formal_run_allowed", "policy_effects_simulated",
                      "plan_sha256", "passed", "runs"}
    if "postcheck_error" in report:
        allowed_fields.add("postcheck_error")
    if set(report) != allowed_fields:
        raise ValueError("invalid aggregate fields")
    if (report.get("schema") != SCHEMA or report.get("scope") != "guarded-fake-correctness-only"
            or report.get("formal_run_allowed") is not False or report.get("policy_effects_simulated") is not False
            or report.get("plan_sha256") != plan_sha256 or type(report.get("passed")) is not bool
            or not isinstance(report.get("runs"), list) or len(report["runs"]) != len(ORDER)):
        raise ValueError("invalid aggregate identity")
    expected_paths = {"report.json"}
    failed = False
    for index, (group, entry) in enumerate(zip(ORDER, report["runs"])):
        run_id = f"{index + 1:02d}-{group}"
        if entry.get("run_id") != run_id or entry.get("group") != group:
            raise ValueError("run order drifted")
        if failed:
            if not same(entry, {"run_id": run_id, "group": group, "status": "not_attempted"}):
                raise ValueError("failed run was retried or later run executed")
            continue
        directory = output / run_id
        if directory.exists():
            expected_paths.add(run_id)
            if not same(entry.get("artifacts"), artifact_hashes(directory)):
                raise ValueError("run artifacts missing, added or modified")
        if entry.get("status") == "preparation_error":
            if entry.get("passed") is not False or any(entry.get(k) is not None for k in
                    ("completed_requests", "offered_requests", "acked_put_bytes")):
                raise ValueError("invalid preparation error accounting")
        else:
            audited = audit_run(directory, document, plan)
            if not same({key: entry.get(key) for key in audited}, audited):
                raise ValueError("aggregate contradicts raw run")
        failed = not entry.get("passed", False)
    if set(path.name for path in output.iterdir()) != expected_paths:
        raise ValueError("unexpected/missing result directory entries")
    if report["passed"] != (not failed and "postcheck_error" not in report):
        raise ValueError("aggregate passed flag contradicts runs")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    creator = commands.add_parser("create")
    creator.add_argument("output")
    creator.add_argument("--binary", required=True)
    creator.add_argument("--source-revision", required=True)
    creator.add_argument("--config", required=True)
    for name in ("check", "run-fake", "check-results"):
        command = commands.add_parser(name)
        command.add_argument("package")
        command.add_argument("--plan-sha256", required=True)
        if name != "check":
            command.add_argument("output")
        if name == "check-results":
            command.add_argument("--report-sha256", required=True)
    commands.add_parser("formal", help="always denied: formal execution is not implemented")
    args = parser.parse_args(argv)
    try:
        if args.command == "formal":
            raise ValueError("FORMAL_RUN_BLOCKED: external registration and real backend required")
        if args.command == "create":
            result = create(args.output, binary=args.binary, source_revision=args.source_revision,
                            config=load(args.config))
        elif args.command == "check":
            check(args.package, args.plan_sha256)
            result = {"preparation_valid": True, "formal_run_allowed": False}
        elif args.command == "run-fake":
            result = run_fake(args.package, args.output, args.plan_sha256)
        else:
            result = check_results(args.package, args.output, args.plan_sha256, args.report_sha256)
        print(trace.encode(result).decode("ascii"), end="")
        return 0 if result.get("passed", True) else 1
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
        print(f"preparation error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
