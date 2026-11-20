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


def metrics(policy, before=False):
    values = {'dfkv_cache_put_total': 19, 'dfkv_bytes_written_total': 746,
              'dfkv_rdma_pull_requests_dispatched_total': 20,
              'dfkv_uring_reads_total': 17, 'dfkv_uring_completions_total': 17,
              'dfkv_slab_lifetime_read_success_total': 17 if policy == 'lifetime' else 0}
    rows = [f'{name} {0 if before else value}' for name, value in values.items()]
    for name, value in (('success_nonempty', 17), ('not_found', 3)):
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

    def matrix(self, *, child_passed=True, mutate=False):
        build = self.base / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so', 'cpu_dedup_runtime_test'):
            (build / name).write_bytes(b'fake')
        roots = [self.base / str(i) for i in (1, 2, 3)]
        for root in roots:
            root.mkdir()
        output = self.base / 'matrix'
        processes = []
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
            path = Path(command[1].split('xml:', 1)[1])
            ET.ElementTree(native_xml()).write(path)
        def scrape():
            policy = 'legacy' if len(processes) == 1 else 'lifetime'
            result = metrics(policy, True)
            for root in roots:
                result += 'dfkv_disk_objects{disk=' + __import__('json').dumps(str(root / f'cpu-tag-{policy}')) + '} 0\n'
            return result
        def stop_boundary(library, members, path, tag, env, server, before, policy, case):
            common.stop_owned(server)
            if mutate:
                library.write_bytes(b'changed')
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
            result = runner.run(build, roots, output, 'tag')
        return result, processes

    def test_matrix_two_owned_servers_success_and_no_global_kill(self):
        result, processes = self.matrix()
        self.assertTrue(result['passed'], result)
        self.assertEqual(len(processes), 2)
        for server in processes:
            self.assertEqual(server.returncode, 0)
            server.kill.assert_not_called()

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
