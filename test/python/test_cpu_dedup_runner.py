"""Guest runner, frozen server oracle and same-handle stop-boundary contracts."""

import copy
from contextlib import ExitStack
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import cpu_dedup_boundary as boundary
import eviction_dedup as dedup
import eviction_trace as trace
import run_cpu_dedup_validation as runner
import run_sg_validation as common
import test_sg_validation_runner as sg_tests


def metrics(policy, before=False, cross_process=False):
    values = {'dfkv_cache_put_total': 19, 'dfkv_bytes_written_total': 746,
              'dfkv_rdma_pull_requests_dispatched_total': 20,
              'dfkv_uring_reads_total': 17, 'dfkv_uring_completions_total': 17,
              'dfkv_slab_lifetime_read_success_total': 17 if policy == 'lifetime' else 0}
    if cross_process:
        values.update(dfkv_cache_put_total=25, dfkv_bytes_written_total=1130,
                      dfkv_rdma_pull_requests_dispatched_total=27,
                      dfkv_uring_reads_total=24, dfkv_uring_completions_total=24,
                      dfkv_slab_lifetime_read_success_total=24 if policy == 'lifetime' else 0)
    rows = [f'{name} {0 if before else value}' for name, value in values.items()]
    for name, value in (('success_nonempty', 24 if cross_process else 17), ('not_found', 3)):
        rows.append('dfkv_rdma_pull_requests_terminal_total{path="uring",result="'
                    + name + '"} ' + str(0 if before else value))
    rows += ['dfkv_storage_healthy 1', 'dfkv_slab_healthy 1', 'dfkv_errors_total 0',
             'dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_inflight 0', 'dfkv_evictions_total 0']
    return '\n'.join(rows) + '\n'


def native_xml():
    root = ET.Element('testsuites')
    suite = ET.SubElement(root, 'testsuite', name='CpuDedupRuntime')
    for name, expected in runner.NATIVE_CASES.items():
        case = ET.SubElement(suite, 'testcase', name=name, classname='CpuDedupRuntime',
                             status='run', result='completed')
        properties = ET.SubElement(case, 'properties')
        for key, value in dict(expected, inner_transport='rdma',
                               shm_cleanup='verified_dev_inode_unlinked').items():
            ET.SubElement(properties, 'property', name=key, value=str(value))
    return root


def process_native_xml(parent_pid=999):
    root = ET.Element('testsuites')
    suite = ET.SubElement(root, 'testsuite', name='CpuDedupProcessRuntime')
    # Deliberately independent of runner.PROCESS_CASES.
    cases = [('CrossProcessWaiterSharesPublishedBytes', 2, 0, 0, 2),
             ('OwnerLocalErrorFallsBackAcrossProcesses', 2, 1, 0, 2),
             ('KilledOwnerFallsBackThenLaterClaimTakesOver', 3, 0, 1, 3)]
    for name, pulls, injected, killed, children in cases:
        case = ET.SubElement(suite, 'testcase', name=name, classname='CpuDedupProcessRuntime',
                             status='run', result='completed')
        properties = ET.SubElement(case, 'properties')
        values = {'expected_server_puts': 2, 'expected_server_put_bytes': 128,
                  'expected_server_pulls': pulls, 'expected_server_grants': pulls,
                  'expected_server_not_found': 0, 'injected_local_owner_errors': injected,
                  'killed_owner_count': killed, 'owned_children_reaped': children,
                  'child_launch': 'posix_spawn_exec', 'claim_schedule': 'owner_then_waiter_marker',
                  'shm_cleanup': 'verified_dev_inode_unlinked', 'inner_transport': 'rdma',
                  'parent_pid': parent_pid, 'owner_pid': 1001, 'waiter_pid': 1002}
        if killed:
            values['recovery_pid'] = 1003
        for key, value in values.items():
            ET.SubElement(properties, 'property', name=key, value=str(value))
    return root


class CpuRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def test_frozen_server_oracle_both_policies(self):
        for policy in ('legacy', 'lifetime'):
            result = runner.check_metrics(metrics(policy, True), metrics(policy), policy)
            self.assertEqual(result['dfkv_uring_reads_total'], 17)
            self.assertEqual(result['dfkv_bytes_written_total'], 746)

    def test_process_matrix_server_oracle_is_independent_of_native_xml(self):
        for policy in ('legacy', 'lifetime'):
            before, after = metrics(policy, True, True), metrics(policy, cross_process=True)
            result = runner.check_metrics(before, after, policy, cross_process=True)
            self.assertEqual(result['dfkv_uring_reads_total'], 24)
            self.assertEqual(result['dfkv_bytes_written_total'], 1130)
            with self.assertRaises(ValueError):
                runner.check_metrics(before, after, policy)
            for old, new in [('reads_total 24', 'reads_total 23'),
                             ('success_nonempty"} 24', 'success_nonempty"} 25'),
                             ('put_total 25', 'put_total 26')]:
                with self.subTest(old=old), self.assertRaises(ValueError):
                    runner.check_metrics(before, after.replace(old, new), policy, cross_process=True)

    def test_bad_missing_metrics_and_wrong_terminal_path_rejected(self):
        before, after = metrics('lifetime', True), metrics('lifetime')
        for old, new in [('reads_total 17', 'reads_total 16'), ('errors_total 0', 'errors_total 1'),
                         ('healthy 1', 'healthy 0'), ('path="uring"', 'path="sync"'),
                         ('completions_total 17', 'completions_total 18'),
                         ('inflight 0', 'inflight 1'), ('read_success_total 17', 'read_success_total 16')]:
            with self.subTest(new=new), self.assertRaises(ValueError):
                runner.check_metrics(before, after.replace(old, new), 'lifetime')
        with self.assertRaises(RuntimeError):
            runner.check_metrics(before, after.replace('dfkv_uring_reads_total 17\n', ''), 'lifetime')
        with self.assertRaises(ValueError):
            runner.check_metrics('', '', 'bad')

    def test_xml_requires_exact_cases_mode_properties_and_no_fail_or_skip(self):
        path = self.base / 'native.xml'
        ET.ElementTree(native_xml()).write(path)
        self.assertEqual(runner.check_native(path), 3)
        def change(root, key, value):
            root.find('.//testcase').set(key, value)
        mutations = [lambda r: change(r, 'classname', 'Other'),
                     lambda r: change(r, 'status', 'notrun'),
                     lambda r: change(r, 'result', 'skipped'),
                     lambda r: change(r, 'name', 'wrong'),
                     lambda r: ET.SubElement(r.find('.//testcase'), 'skipped'),
                     lambda r: ET.SubElement(r.find('.//testcase'), 'failure'),
                     lambda r: r.find('.//property[@name="inner_transport"]').set('value', 'tcp'),
                     lambda r: r.find('.//property[@name="expected_server_pulls"]').set('value', '99'),
                     lambda r: r.find('.//property[@name="shm_cleanup"]').set('value', 'unknown'),
                     lambda r: ET.SubElement(r.find('.//properties'), 'property', name='inner_transport', value='rdma')]
        for mutation in mutations:
            root = native_xml()
            mutation(root)
            ET.ElementTree(root).write(path)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                runner.check_native(path)

    def test_process_xml_proves_distinct_exec_identities_gates_and_exact_cleanup(self):
        path = self.base / 'process.xml'
        ET.ElementTree(process_native_xml()).write(path)
        self.assertEqual(runner.check_process_native(path, 999), 3)
        def change(root, field, value):
            root.find('.//property[@name="' + field + '"]').set('value', value)
        mutations = [lambda r: change(r, 'parent_pid', '998'),
                     lambda r: change(r, 'owner_pid', '999'),
                     lambda r: change(r, 'waiter_pid', '1001'),
                     lambda r: change(r, 'owner_pid', '01001'),
                     lambda r: change(r, 'owner_pid', '0'),
                     lambda r: change(r, 'owner_pid', '-1'),
                     lambda r: change(r, 'owner_pid', '2147483648'),
                     lambda r: change(r, 'owner_pid', 'True'),
                     lambda r: change(r, 'owner_pid', '١٠٠١'),
                     lambda r: change(r, 'recovery_pid', '1002'),
                     lambda r: change(r, 'child_launch', 'fork_inherited_transport'),
                     lambda r: change(r, 'claim_schedule', 'sleep_only'),
                     lambda r: change(r, 'owned_children_reaped', '1'),
                     lambda r: change(r, 'shm_cleanup', 'unknown'),
                     lambda r: change(r, 'inner_transport', 'tcp'),
                     lambda r: change(r, 'expected_server_pulls', '99'),
                     lambda r: ET.SubElement(r.find('.//testcase'), 'skipped'),
                     lambda r: ET.SubElement(r.find('.//testcase'), 'error'),
                     lambda r: r.find('.//testcase').set('classname', 'CpuDedupRuntime'),
                     lambda r: r.find('.//testcase').set('status', 'notrun'),
                     lambda r: ET.SubElement(r.find('.//properties'), 'property', name='owner_pid', value='1001'),
                     lambda r: ET.SubElement(r.find('.//properties'), 'property', name='recovery_pid', value='1003')]
        for mutation in mutations:
            root = process_native_xml()
            mutation(root)
            ET.ElementTree(root).write(path)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                runner.check_process_native(path, 999)

    def test_process_native_owned_parent_is_reaped_and_reported(self):
        child = mock.Mock(pid=999, returncode=0)
        child.poll.return_value = 0
        def wait(timeout):
            self.assertEqual(timeout, 45)
            ET.ElementTree(process_native_xml()).write(self.base / 'process-native.xml')
        child.wait.side_effect = wait
        with mock.patch.object(runner.subprocess, 'Popen', return_value=child) as popen:
            result = runner.run_process_native('/build/native', self.base, {'DFKV_RDMA': '1'})
        self.assertTrue(result['passed'])
        self.assertTrue(result['reaped'])
        self.assertEqual(result['cases'], 3)
        self.assertEqual(result['returncode'], 0)
        self.assertEqual(result['logical_counts'], {
            'put_ack': 6, 'put_bytes': 384, 'get_attempted': 11, 'get_completed': 10,
            'get_hit': 9, 'get_hit_bytes': 576, 'get_error': 1, 'get_inflight_killed': 1})
        self.assertEqual(popen.call_args.kwargs['stdin'], subprocess.DEVNULL)
        self.assertEqual(trace.load(self.base / 'process-native-report.json'), result)
        child.terminate.assert_not_called()
        child.kill.assert_not_called()

    def test_process_native_nonzero_or_invalid_xml_cannot_claim_pass(self):
        for code in (9, 0):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as base:
                path = Path(base)
                child = mock.Mock(pid=999, returncode=code)
                child.poll.return_value = code
                root = process_native_xml(parent_pid=998)
                ET.ElementTree(root).write(path / 'process-native.xml')
                with mock.patch.object(runner.subprocess, 'Popen', return_value=child):
                    with self.assertRaises((RuntimeError, ValueError)):
                        runner.run_process_native('/build/native', path, {})
                result = trace.load(path / 'process-native-report.json')
                self.assertFalse(result['passed'])
                self.assertTrue(result['reaped'])
                self.assertEqual(result['returncode'], code)
                self.assertIsNone(result['logical_counts'])
                child.terminate.assert_not_called()
                child.kill.assert_not_called()

    def test_process_native_launch_failure_reports_no_owned_child(self):
        with mock.patch.object(runner.subprocess, 'Popen', side_effect=OSError('cannot exec')):
            with self.assertRaises(OSError):
                runner.run_process_native('/build/native', self.base, {})
        result = trace.load(self.base / 'process-native-report.json')
        self.assertFalse(result['passed'])
        self.assertFalse(result['reaped'])
        self.assertNotIn('pid', result)
        self.assertNotIn('returncode', result)
        self.assertIsNone(result['logical_counts'])

    def test_process_native_timeout_cleanup_reports_failure_and_uses_owned_handle(self):
        child = mock.Mock(pid=999, returncode=None)
        child.poll.side_effect = lambda: child.returncode
        def stop(owned):
            self.assertIs(owned, child)
            child.returncode = -15
            return ['terminate', 'reap']
        child.wait.side_effect = subprocess.TimeoutExpired('native', 45)
        with mock.patch.object(runner.subprocess, 'Popen', return_value=child), \
                mock.patch.object(runner, '_stop', side_effect=stop):
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.run_process_native('/build/native', self.base, {})
        result = trace.load(self.base / 'process-native-report.json')
        self.assertFalse(result['passed'])
        self.assertTrue(result['reaped'])
        self.assertEqual(result['returncode'], -15)
        self.assertEqual(result['cleanup_actions'], ['terminate', 'reap'])
        self.assertIsNone(result['logical_counts'])

    def test_parent_tag_limit_before_any_guest_io(self):
        for tag in ('x' * 56, 'x' * 64, None):
            with self.assertRaises(ValueError):
                runner.run('/missing', [], '/missing', tag)
        with mock.patch.object(common, 'guest_paths', side_effect=RuntimeError('guard reached')):
            with self.assertRaisesRegex(RuntimeError, 'guard reached'):
                runner.run('/missing', [], '/missing', 'x' * 55)

    def test_guest_path_cpu_prefix_and_dangling_occupancy_checks(self):
        helper = sg_tests.RunnerTest()
        roots = [Path(f'/data/disk{i}/cases') for i in (1, 2, 3)]
        with helper._guest_filesystems():
            self.assertEqual(common.guest_paths(roots, '/data/disk1/results/cpu', 'tag', case_prefix='cpu')[0], roots)
        with helper._guest_filesystems(existing='/data/disk2/cases/cpu-tag-lifetime'):
            with self.assertRaises(ValueError):
                common.guest_paths(roots, '/data/disk1/results/cpu', 'tag', case_prefix='cpu')
        with self.assertRaises(ValueError):
            common.guest_paths([], '/tmp/no', 'tag', case_prefix='arbitrary')

    def boundary_result(self):
        return {'config': {'frozen': True}, 'before': dict.fromkeys(dedup.COUNTERS, 0),
                'after': dict.fromkeys(dedup.COUNTERS, 0) | {'hits': 1, 'fetches': 2},
                'warm': {'outcome': 'get_hit', 'status': 0},
                'local_after_stop': {'outcome': 'get_hit', 'status': 0},
                'cold_after_stop': {'outcome': 'client_error', 'status': 4, 'error': 'I/O'},
                'warm_to_local_ns': 1_000_000, 'passed': True}

    def test_ttl_result_preserves_local_hit_vs_cold_io_and_rejects_forgery(self):
        original = self.boundary_result()
        self.assertEqual(boundary.verify(original, {'frozen': True})['hits'], 1)
        mutations = [lambda r: r['warm'].update(status=False),
                     lambda r: r['local_after_stop'].update(status=False),
                     lambda r: r['config'].update(frozen=1),
                     lambda r: r['cold_after_stop'].update(status=1),
                     lambda r: r['after'].update(fetches=True),
                     lambda r: r['before'].update(hits=1),
                     lambda r: r.update(warm_to_local_ns=True),
                     lambda r: r.update(warm_to_local_ns=25_000_000_000),
                     lambda r: r.update(passed=1)]
        for mutation in mutations:
            result = copy.deepcopy(original)
            mutation(result)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                boundary.verify(result, {'frozen': True})

    def matrix(self, *, child_passed=True, mutate=False, cross_process=False,
               process_failure=None, mutate_process=False):
        build = self.base / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so', 'cpu_dedup_runtime_test',
                     'cpu_dedup_process_runtime_test'):
            (build / name).write_bytes(b'fake')
        roots = [self.base / str(i) for i in (1, 2, 3)]
        for root in roots:
            root.mkdir()
        output = self.base / 'matrix'
        processes = []
        native_commands = []
        process_commands = []
        def start(*args, **kwargs):
            server = mock.Mock(returncode=None, pid=321 + len(processes))
            server.poll.side_effect = lambda: server.returncode
            def wait(timeout):
                server.returncode = 0
                return 0
            server.wait.side_effect = wait
            processes.append(server)
            return server
        def native(command, **kwargs):
            native_commands.append((command, kwargs))
            path = Path(command[1].split('xml:', 1)[1])
            ET.ElementTree(native_xml()).write(path)
        def process_native(binary, case, env):
            process_commands.append((binary, case, env))
            if process_failure is not None:
                raise process_failure
            return {'passed': True, 'cases': 3, 'reaped': True}
        def scrape():
            policy = 'legacy' if len(processes) == 1 else 'lifetime'
            result = metrics(policy, True)
            for root in roots:
                result += 'dfkv_disk_objects{disk=' + __import__('json').dumps(str(root / f'cpu-tag-{policy}')) + '} 0\n'
            return result
        def stop_boundary(library, members, path, tag, env, server, before, policy, case, **kwargs):
            self.assertEqual(kwargs, {'cross_process': cross_process})
            common.stop_owned(server)
            if mutate:
                library.write_bytes(b'changed')
            if mutate_process:
                (build / 'cpu_dedup_process_runtime_test').write_bytes(b'changed')
            return {'passed': True, 'reaped': True}
        response = mock.MagicMock()
        response.__enter__.return_value.status = 200
        with ExitStack() as stack:
            for target, replacement in [('guest_paths', mock.Mock(return_value=(roots, output))),
                                        ('scrape', scrape)]:
                stack.enter_context(mock.patch.object(common, target, replacement))
            stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
            stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=start))
            stack.enter_context(mock.patch.object(runner.subprocess, 'run', side_effect=native))
            stack.enter_context(mock.patch.object(runner.urllib.request, 'urlopen', return_value=response))
            stack.enter_context(mock.patch.object(runner.probe, 'supervise', return_value={'passed': child_passed}))
            stack.enter_context(mock.patch.object(runner.smoke, 'supervise', return_value={'passed': True}))
            stack.enter_context(mock.patch.object(runner, 'boundary_stop', side_effect=stop_boundary))
            stack.enter_context(mock.patch.object(runner, 'run_process_native', side_effect=process_native))
            result = runner.run(build, roots, output, 'tag', cross_process=cross_process)
        result['_test_native_commands'] = native_commands
        result['_test_process_commands'] = process_commands
        return result, processes

    def test_matrix_two_owned_servers_success_and_no_global_kill(self):
        result, processes = self.matrix()
        self.assertTrue(result['passed'], result)
        self.assertEqual(len(processes), 2)
        for server in processes:
            self.assertEqual(server.returncode, 0)
            server.kill.assert_not_called()

    def test_optional_process_matrix_runs_both_policies_and_hashes_extra_binary(self):
        result, processes = self.matrix(cross_process=True)
        self.assertTrue(result['passed'], result)
        self.assertTrue(result['cross_process'])
        self.assertIn('cpu_dedup_process_runtime_test', result['build_sha256'])
        self.assertEqual(len(result['_test_native_commands']), 2)
        self.assertEqual(len(result['_test_process_commands']), 2)
        for binary, case, env in result['_test_process_commands']:
            self.assertEqual(binary.name, 'cpu_dedup_process_runtime_test')
            self.assertEqual(env['DFKV_CPU_DEDUP_RUNTIME_NAMESPACE'], 'tag-' + case.name)
        self.assertEqual([row['process_native']['cases'] for row in result['policies']], [3, 3])
        self.assertTrue(all(process.returncode == 0 for process in processes))

    def test_default_matrix_does_not_run_or_hash_process_binary(self):
        result, _ = self.matrix()
        self.assertNotIn('cross_process', result)
        self.assertNotIn('cpu_dedup_process_runtime_test', result['build_sha256'])
        self.assertEqual(len(result['_test_native_commands']), 2)
        self.assertEqual(result['_test_process_commands'], [])
        self.assertTrue(all('process_native' not in row for row in result['policies']))

    def test_process_suite_timeout_or_failure_stops_owned_server_and_later_policy(self):
        for error in (subprocess.TimeoutExpired('native', 45),
                      subprocess.CalledProcessError(9, 'native')):
            with self.subTest(error=type(error).__name__):
                with tempfile.TemporaryDirectory() as base:
                    old = self.base
                    self.base = Path(base)
                    try:
                        result, processes = self.matrix(cross_process=True, process_failure=error)
                    finally:
                        self.base = old
                self.assertFalse(result['passed'])
                self.assertEqual(len(processes), 1)
                self.assertEqual(processes[0].returncode, 0)
                self.assertIn(type(error).__name__, result['errors'][0])

    def test_process_binary_mutation_cannot_pass(self):
        result, _ = self.matrix(cross_process=True, mutate_process=True)
        self.assertFalse(result['passed'])
        self.assertIn('build changed', result['errors'][0])

    def test_failure_stops_first_owned_server_and_does_not_run_later_policy(self):
        result, processes = self.matrix(child_passed=False)
        self.assertFalse(result['passed'])
        self.assertEqual(len(processes), 1)
        self.assertEqual(processes[0].returncode, 0)
        self.assertTrue((self.base / 'matrix/matrix.json').is_file())

    def test_mutated_build_cannot_pass(self):
        result, _ = self.matrix(mutate=True)
        self.assertFalse(result['passed'])
        self.assertIn('build changed', result['errors'][0])

    def test_boundary_failed_child_is_reaped_and_has_no_unknown_pid_kill(self):
        library = self.base / 'lib.so'
        library.write_bytes(b'fake')
        output = self.base / 'boundary'
        child = mock.Mock(returncode=2, pid=123)
        child.poll.return_value = 2
        with mock.patch.object(runner.subprocess, 'Popen', return_value=child):
            with self.assertRaisesRegex(RuntimeError, 'exited before'):
                runner.boundary_stop(library, 'n=127.0.0.1:1', output, 'tag', {}, mock.Mock(), '', 'legacy', self.base)
        report = trace.load(output / 'report.json')
        self.assertFalse(report['passed'])
        self.assertTrue(report['reaped'])
        child.terminate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
