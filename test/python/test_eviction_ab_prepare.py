"""Offline preparation identity, complete ledger and owned-child regressions."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

DIRECTORY = Path(__file__).resolve().parents[2] / "deploy/validation"
sys.path.insert(0, str(DIRECTORY))
import eviction_ab_prepare as prepare
import eviction_trace as trace


class PreparationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "binary"
        self.binary.write_bytes(b"identity only, never executed")
        self.package = self.root / "package"
        self.output = self.root / "result"
        self.config = dict(capacity_bytes=None, extent_bytes=None, slot_sizes=None,
                           ram_bytes=None, coalescing=None, workers=1,
                           queue_capacity=16, timeout_ns=1000000000,
                           process_timeout_sec=5)
        self.receipt = prepare.create(self.package, binary=self.binary,
                                      source_revision="a" * 40, config=self.config)
        self.plan_sha = self.receipt["plan_sha256"]

    def rewrite_plan(self, transform):
        path = self.package / "plan.json"
        plan = trace.load(path)
        transform(plan)
        path.write_bytes(trace.encode(plan))
        return prepare.file_sha(path)

    def run_success(self):
        result = prepare.run_fake(self.package, self.output, self.plan_sha)
        self.assertTrue(result["passed"], result)
        return result

    def audit(self, result):
        return prepare.check_results(self.package, self.output, self.plan_sha,
                                     result["report_sha256"])

    def test_create_checks_real_binary_tools_trace_and_never_authorizes_formal(self):
        plan, document = prepare.check(self.package, self.plan_sha)
        self.assertFalse(plan["formal_run_allowed"])
        self.assertEqual(plan["gpu"], "excluded")
        self.assertEqual(plan["run_order"], ["A", "B", "B", "A", "A", "B"])
        self.assertEqual(len(document["requests"]), 8)
        self.assertEqual(set(plan["tools"]), set(prepare.TOOLS))
        self.assertEqual(plan["groups"]["A"]["binary"], plan["groups"]["B"]["binary"])
        self.assertEqual(plan["groups"]["A"]["common_config"], self.config)
        self.assertIn("old_version_O_baseline", plan["external_blockers"])
        self.assertEqual(plan["source_provenance"], "caller-declaration-not-binary-to-source-proof")

    def test_normal_six_actual_guarded_children_full_denominators_and_raw_audit(self):
        result = self.run_success()
        audited = self.audit(result)
        self.assertTrue(audited["passed"])
        self.assertFalse(audited["policy_effects_simulated"])
        for entry in audited["runs"]:
            self.assertEqual(entry["planned_requests"], 8)
            self.assertEqual(entry["offered_requests"], 8)
            self.assertEqual(entry["completed_requests"], 8)
            self.assertEqual(entry["acked_put_bytes"], 512)
            child = trace.load(self.output / entry["run_id"] / "report.json")
            self.assertTrue(child["reaped"])
            self.assertEqual(child["returncode"], 0)

    def test_configuration_bounds_types_and_geometry(self):
        for field, value in (("workers", True), ("timeout_ns", False),
                             ("process_timeout_sec", True), ("process_timeout_sec", float("inf")),
                             ("slot_sizes", [1, 1]), ("capacity_bytes", False),
                             ("coalescing", "false"), ("ram_bytes", -1)):
            with self.subTest(field=field, value=value):
                config = dict(self.config, **{field: value})
                with self.assertRaises(ValueError):
                    prepare.validate_config(config)
        for config in (dict(self.config, capacity_bytes=4, extent_bytes=8),
                       dict(self.config, extent_bytes=8, slot_sizes=[16])):
            with self.assertRaises(ValueError):
                prepare.validate_config(config)

    def test_binary_change_blocks_before_new_output_creation(self):
        self.binary.write_bytes(b"changed")
        with self.assertRaises(ValueError):
            prepare.run_fake(self.package, self.output, self.plan_sha)
        self.assertFalse(self.output.exists())

    def test_external_plan_sha_rejects_rehashed_configuration(self):
        self.rewrite_plan(lambda p: p["groups"]["A"]["common_config"].update(workers=2))
        with self.assertRaisesRegex(ValueError, "external frozen"):
            prepare.check(self.package, self.plan_sha)

    def test_rehashed_ack_binary_policy_or_formal_drift_cannot_pass(self):
        original = (self.package / "plan.json").read_bytes()
        transforms = [lambda p: p["groups"]["B"].update(ack="ram"),
                      lambda p: p["groups"]["B"]["binary"].update(sha256="0" * 64),
                      lambda p: p["groups"]["B"].update(policy="legacy"),
                      lambda p: p.update(formal_run_allowed=True),
                      lambda p: p["tools"].update({"eviction_guard.py": "0" * 64})]
        for transform in transforms:
            (self.package / "plan.json").write_bytes(original)
            changed_sha = self.rewrite_plan(transform)
            with self.assertRaises(ValueError):
                prepare.check(self.package, changed_sha)

    def test_trace_mutation_even_new_valid_trace_rejected_by_plan(self):
        other = trace.generate(seed=99, objects=2, rounds=2, length=128, interval_ns=80000000)
        (self.package / "trace.json").write_bytes(trace.encode(other))
        with self.assertRaises(ValueError):
            prepare.check(self.package, self.plan_sha)

    def test_tool_change_detected_without_mutating_shared_source(self):
        original = prepare.file_sha
        def changed(path):
            if Path(path).name == "eviction_realtime.py":
                return "0" * 64
            return original(path)
        with mock.patch.object(prepare, "file_sha", side_effect=changed):
            with self.assertRaises(ValueError):
                prepare.check(self.package, self.plan_sha)

    def test_exclusive_create_and_no_output_overwrite(self):
        with self.assertRaises(FileExistsError):
            prepare.create(self.package, binary=self.binary, source_revision="a" * 40,
                           config=self.config)
        self.output.mkdir()
        with self.assertRaises(FileExistsError):
            prepare.run_fake(self.package, self.output, self.plan_sha)

    def test_real_timeout_keeps_unknowns_retains_files_and_never_retries(self):
        # Real owned Python child, forced to exceed the frozen process deadline.
        command = [sys.executable, "-I", "-c", "import time; time.sleep(5)"]
        sha = self.rewrite_plan(lambda p: [p["groups"][g]["common_config"].update(
            process_timeout_sec=.05) for g in ("A", "B")])
        with mock.patch.object(prepare.guard, "_command", return_value=command):
            result = prepare.run_fake(self.package, self.output, sha)
        self.assertFalse(result["passed"])
        self.assertEqual(result["runs"][0]["status"], "process_timeout")
        self.assertIsNone(result["runs"][0]["completed_requests"])
        self.assertEqual([r["status"] for r in result["runs"]][1:], ["not_attempted"] * 5)
        self.assertTrue(trace.load(self.output / "01-A/report.json")["reaped"])
        prepare.check_results(self.package, self.output, sha, result["report_sha256"])

    def test_failed_complete_child_retains_denominators_and_blocks_five(self):
        sha = self.rewrite_plan(lambda p: [p["groups"][g]["common_config"].update(
            timeout_ns=1) for g in ("A", "B")])
        result = prepare.run_fake(self.package, self.output, sha)
        self.assertFalse(result["passed"])
        self.assertEqual(result["runs"][0]["status"], "workload_failed")
        self.assertEqual(result["runs"][0]["completed_requests"], 0)
        self.assertEqual(result["runs"][0]["planned_requests"], 8)
        prepare.check_results(self.package, self.output, sha, result["report_sha256"])

    def test_missing_ledger_or_added_artifact_rejected(self):
        result = self.run_success()
        child = self.output / "01-A/child-result.json"
        original = child.read_bytes()
        child.unlink()
        with self.assertRaises(ValueError):
            self.audit(result)
        child.write_bytes(original)
        (self.output / "01-A/extra").write_text("bad")
        with self.assertRaises(ValueError):
            self.audit(result)

    def test_rehashed_forged_child_counts_fail_independent_ledger_validation(self):
        result = self.run_success()
        child_path = self.output / "01-A/child-result.json"
        child = trace.load(child_path)
        child["summary"]["completed"] = 1
        child_path.write_bytes(trace.encode(child))
        report_path = self.output / "report.json"
        report = trace.load(report_path)
        report["runs"][0]["artifacts"] = prepare.artifact_hashes(self.output / "01-A")
        report_path.write_bytes(trace.encode(report))
        with self.assertRaisesRegex(ValueError, "summary does not reconcile"):
            prepare.check_results(self.package, self.output, self.plan_sha,
                                  prepare.file_sha(report_path))

    def test_rehashed_run_order_and_extra_directory_rejected(self):
        result = self.run_success()
        path = self.output / "report.json"
        report = trace.load(path)
        report["runs"][0], report["runs"][1] = report["runs"][1], report["runs"][0]
        path.write_bytes(trace.encode(report))
        with self.assertRaisesRegex(ValueError, "order"):
            prepare.check_results(self.package, self.output, self.plan_sha, prepare.file_sha(path))
        path.write_bytes(trace.encode({k: v for k, v in result.items() if k != "report_sha256"}))
        (self.output / "unexpected").mkdir()
        with self.assertRaises(ValueError):
            self.audit(result)

    def test_symlinks_duplicates_and_banned_host_disk_rejected(self):
        link = self.root / "link"
        link.symlink_to(self.binary)
        with self.assertRaises(ValueError):
            prepare.file_sha(link)
        with self.assertRaises(ValueError):
            prepare.safe_path("/mnt/disk1/example")
        path = self.package / "plan.json"
        path.write_text('{"groups":{},"groups":{}}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            prepare.check(self.package, prepare.file_sha(path))

    def test_invalid_artifact_still_saves_failed_aggregate_and_retains_files(self):
        original = prepare.guard.supervise
        for kind in ("symlink", "directory"):
            output = self.root / kind
            def inject(source, directory, **kwargs):
                result = original(source, directory, **kwargs)
                extra = directory / "invalid-artifact"
                if kind == "symlink":
                    extra.symlink_to(self.binary)
                else:
                    extra.mkdir()
                return result
            with self.subTest(kind=kind), mock.patch.object(prepare.guard, "supervise", side_effect=inject):
                result = prepare.run_fake(self.package, output, self.plan_sha)
                retained = trace.load(output / "report.json")
                self.assertFalse(result["passed"])
                self.assertEqual(retained["runs"][0]["status"], "preparation_error")
                self.assertIsNone(retained["runs"][0]["completed_requests"])
                self.assertEqual([r["status"] for r in retained["runs"]][1:], ["not_attempted"] * 5)
                self.assertTrue((output / "01-A/invalid-artifact").exists())
                with self.assertRaises(ValueError):
                    prepare.check_results(self.package, output, self.plan_sha, result["report_sha256"])

    def test_rejected_trace_symlink_is_never_opened_by_json_loader(self):
        path = self.package / "trace.json"
        path.unlink()
        path.symlink_to(self.binary)
        original = trace.load
        opened = []
        def observed(path):
            opened.append(Path(path))
            return original(path)
        with mock.patch.object(trace, "load", side_effect=observed):
            with self.assertRaises(ValueError):
                prepare.check(self.package, self.plan_sha)
        self.assertNotIn(path, opened)

    def test_interrupt_is_reraised_only_after_full_failed_aggregate_is_saved(self):
        with mock.patch.object(prepare.guard, "supervise", side_effect=KeyboardInterrupt) as supervisor:
            with self.assertRaises(KeyboardInterrupt):
                prepare.run_fake(self.package, self.output, self.plan_sha)
        self.assertEqual(supervisor.call_count, 1)
        result = trace.load(self.output / "report.json")
        self.assertFalse(result["passed"])
        self.assertEqual(len(result["runs"]), 6)
        self.assertEqual(result["runs"][0]["status"], "preparation_error")
        self.assertIsNone(result["runs"][0]["completed_requests"])
        self.assertIn("KeyboardInterrupt", result["runs"][0]["error"])
        self.assertEqual([r["status"] for r in result["runs"]][1:], ["not_attempted"] * 5)

    def test_formal_command_always_denied_and_cli_check_works(self):
        command = [sys.executable, str(DIRECTORY / "eviction_ab_prepare.py")]
        result = subprocess.run(command + ["formal"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("FORMAL_RUN_BLOCKED", result.stderr)
        result = subprocess.run(command + ["check", str(self.package), "--plan-sha256", self.plan_sha],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"formal_run_allowed":false', result.stdout)


if __name__ == "__main__":
    unittest.main()
