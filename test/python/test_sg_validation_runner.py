"""Pure tests for the guest-only SG matrix, never launch disks or RDMA."""

from contextlib import ExitStack, contextmanager
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_sg_validation as runner


def metrics(policy, *, before=False):
    values = {'dfkv_cache_put_total': 4, 'dfkv_bytes_written_total': 321,
              'dfkv_rdma_pull_requests_dispatched_total': 11,
              'dfkv_uring_reads_total': 9, 'dfkv_uring_completions_total': 9,
              'dfkv_slab_lifetime_read_success_total': 9 if policy == 'lifetime' else 0}
    rows = [f'{key} {0 if before else value}' for key, value in values.items()]
    for name, value in (('success_nonempty', 9), ('not_found', 2)):
        rows.append('dfkv_rdma_pull_requests_terminal_total{path="uring",result="'
                    + name + '"} ' + str(0 if before else value))
    rows += ['dfkv_storage_healthy 1', 'dfkv_slab_healthy 1', 'dfkv_errors_total 0',
             'dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_inflight 0', 'dfkv_evictions_total 0']
    return '\n'.join(rows) + '\n'


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)

    def test_metric_oracle_both_policies(self):
        for policy in ('legacy', 'lifetime'):
            with self.subTest(policy=policy):
                result = runner.check_metrics(metrics(policy, before=True), metrics(policy), policy)
                self.assertEqual(result['dfkv_uring_reads_total'], 9)
                self.assertEqual(result['dfkv_cache_put_total'], 4)

    def test_metric_oracle_rejects_bad_counters_and_paths(self):
        before = metrics('lifetime', before=True)
        after = metrics('lifetime')
        for old, new in [('dfkv_uring_reads_total 9', 'dfkv_uring_reads_total 8'),
                         ('dfkv_errors_total 0', 'dfkv_errors_total 1'),
                         ('dfkv_storage_healthy 1', 'dfkv_storage_healthy 0'),
                         ('path="uring"', 'path="sync"'),
                         ('dfkv_uring_inflight 0', 'dfkv_uring_inflight 1'),
                         ('dfkv_slab_lifetime_read_success_total 9',
                          'dfkv_slab_lifetime_read_success_total 7')]:
            with self.subTest(new=new), self.assertRaises(ValueError):
                runner.check_metrics(before, after.replace(old, new), 'lifetime')

    def test_missing_or_nonfinite_metric_rejected(self):
        for after in (metrics('legacy').replace('dfkv_uring_reads_total 9\n', ''),
                      metrics('legacy').replace('dfkv_uring_reads_total 9', 'dfkv_uring_reads_total nan')):
            with self.assertRaises(RuntimeError):
                runner.check_metrics(metrics('legacy', before=True), after, 'legacy')
        with self.assertRaises(ValueError):
            runner.check_metrics('', '', 'unknown')

    def test_guest_preflight_refuses_invalid_tag_before_path_io(self):
        for tag in ('', '../x', 'a/b', 'a b', '中', 'x' * 65):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                runner.guest_paths([], '/tmp/unused', tag)

    def test_guest_marker_absent_refuses_host_run(self):
        with mock.patch.object(Path, 'is_file', return_value=False):
            with self.assertRaisesRegex(ValueError, 'guest'):
                runner.guest_paths([], '/tmp/unused', 'tag')

    @contextmanager
    def _guest_filesystems(self, *, shared=False, nested=False, mounted=True,
                           existing=None, escape=False):
        def device(path):
            text = str(path)
            index = next((i for i in (1, 2, 3) if text.startswith(f'/data/disk{i}')), 0)
            if nested and text == '/data/disk1/cases':
                index = 99
            return SimpleNamespace(st_dev=1 if shared else index)

        def resolve(path, strict=False):
            return Path('/mnt/forbidden') if escape and str(path) == '/data/disk1/cases' else path

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(Path, 'is_file', return_value=True))
            stack.enter_context(mock.patch.object(Path, 'is_dir', return_value=True))
            stack.enter_context(mock.patch.object(Path, 'resolve', autospec=True, side_effect=resolve))
            stack.enter_context(mock.patch.object(Path, 'exists', autospec=True,
                                                  side_effect=lambda path: str(path) == existing))
            stack.enter_context(mock.patch.object(Path, 'stat', autospec=True, side_effect=device))
            stack.enter_context(mock.patch.object(runner.os.path, 'ismount', return_value=mounted))
            yield

    def test_guest_paths_positive_uses_three_distinct_filesystems(self):
        roots = [Path(f'/data/disk{i}/cases') for i in (1, 2, 3)]
        with self._guest_filesystems():
            actual, output = runner.guest_paths(roots, '/data/disk1/results/out', 'tag')
        self.assertEqual(actual, roots)
        self.assertEqual(output, Path('/data/disk1/results/out'))

    def test_guest_paths_reject_shared_nested_unmounted_and_escape(self):
        roots = [Path(f'/data/disk{i}/cases') for i in (1, 2, 3)]
        for options in ({'shared': True}, {'nested': True}, {'mounted': False}, {'escape': True}):
            with self.subTest(options=options), self._guest_filesystems(**options):
                with self.assertRaises(ValueError):
                    runner.guest_paths(roots, '/data/disk1/results/out', 'tag')
        with self._guest_filesystems(), self.assertRaises(ValueError):
            runner.guest_paths(roots[:2], '/data/disk1/results/out', 'tag')

    def test_guest_paths_refuse_output_or_data_reuse_and_outside_output(self):
        roots = [Path(f'/data/disk{i}/cases') for i in (1, 2, 3)]
        for existing in ('/data/disk1/results/out', '/data/disk2/cases/sg-tag-lifetime'):
            with self.subTest(existing=existing), self._guest_filesystems(existing=existing):
                with self.assertRaises(ValueError):
                    runner.guest_paths(roots, '/data/disk1/results/out', 'tag')
        with self._guest_filesystems(), self.assertRaises(ValueError):
            runner.guest_paths(roots, '/tmp/out', 'tag')

    def test_stop_only_owned_popen_and_waits(self):
        server = mock.Mock()
        server.poll.return_value = None
        server.wait.return_value = 0
        runner.stop_owned(server)
        server.terminate.assert_called_once_with()
        server.wait.assert_called_once_with(timeout=15)
        server.kill.assert_not_called()

    def test_stop_timeout_is_failure_even_after_kill_reaped(self):
        server = mock.Mock()
        server.poll.return_value = None
        server.wait.side_effect = [subprocess.TimeoutExpired('owned', 15), -9]
        with self.assertRaisesRegex(RuntimeError, 'required KILL'):
            runner.stop_owned(server)
        server.kill.assert_called_once_with()
        self.assertEqual(server.wait.call_args_list, [mock.call(timeout=15), mock.call(timeout=3)])

    def test_unreapable_and_nonzero_server_are_failures(self):
        server = mock.Mock()
        server.poll.return_value = None
        server.wait.side_effect = subprocess.TimeoutExpired('owned', 3)
        with self.assertRaisesRegex(RuntimeError, 'unreaped'):
            runner.stop_owned(server)
        server = mock.Mock()
        server.poll.return_value = 2
        server.wait.return_value = 2
        with self.assertRaisesRegex(RuntimeError, 'exited 2'):
            runner.stop_owned(server)
        server.terminate.assert_not_called()

    def _matrix(self, child_passed, *, mutate_build=False):
        temporary = self.stack.enter_context(tempfile.TemporaryDirectory())
        root = Path(temporary)
        build = root / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build / name).write_bytes(b'fixture')
        roots = [root / f'disk{i}' for i in (1, 2, 3)]
        for disk in roots:
            disk.mkdir()
        output = root / 'results'
        self.stack.enter_context(mock.patch.object(runner, 'guest_paths', return_value=(roots, output)))
        self.stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
        processes = []

        def start(*args, **kwargs):
            server = mock.Mock()
            server.returncode = None
            server.poll.side_effect = lambda: server.returncode

            def finish(timeout):
                server.returncode = 0
                return 0

            server.wait.side_effect = finish
            server.pid = len(processes) + 1
            processes.append(server)
            return server

        self.stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=start))
        response = mock.MagicMock()
        response.__enter__.return_value.status = 200
        self.stack.enter_context(mock.patch.object(runner.urllib.request, 'urlopen', return_value=response))

        def scrape():
            policy = ('legacy', 'lifetime')[len(processes) - 1]
            return ''.join('dfkv_disk_objects{disk="' + str(disk / f'sg-tag-{policy}') + '"} 0\n'
                           for disk in roots)

        self.stack.enter_context(mock.patch.object(runner, 'scrape', side_effect=scrape))
        self.stack.enter_context(mock.patch.object(runner, 'check_metrics', return_value={'fixture': 1}))

        def probe(*args, **kwargs):
            if kwargs['mode'] == 'stopped':
                self.assertTrue(processes[-1].wait.called)
                self.assertEqual(processes[-1].returncode, 0)
                if mutate_build and len(processes) == 2:
                    (build / 'libdfkv.so').write_bytes(b'changed')
            return {'passed': child_passed}

        supervise = self.stack.enter_context(mock.patch.object(runner.sg, 'supervise', side_effect=probe))
        result = runner.run(build, roots, output, 'tag')
        return result, output, processes, supervise

    def test_two_policy_lifecycle_and_stopped_after_owned_exit(self):
        result, output, processes, supervise = self._matrix(True)
        self.assertTrue(result['passed'])
        self.assertTrue((output / 'matrix.json').is_file())
        self.assertEqual([row['policy'] for row in result['policies']], ['legacy', 'lifetime'])
        self.assertEqual([call.kwargs['mode'] for call in supervise.call_args_list],
                         ['live', 'stopped', 'live', 'stopped'])
        for process in processes:
            process.terminate.assert_called()
        self.assertEqual(len(supervise.call_args_list), 4)

    def test_live_failure_stops_matrix_and_preserves_failure_report(self):
        result, output, processes, supervise = self._matrix(False)
        self.assertFalse(result['passed'])
        self.assertTrue(result['errors'])
        self.assertEqual(len(result['policies']), 1)
        self.assertEqual(len(processes), 1)
        self.assertEqual(supervise.call_count, 1)
        processes[0].terminate.assert_called_once()
        self.assertTrue((output / 'matrix.json').is_file())

    def test_build_mutation_between_policies_cannot_pass(self):
        result, output, processes, _ = self._matrix(True, mutate_build=True)
        self.assertFalse(result['passed'])
        self.assertIn('build changed', result['errors'][0])
        self.assertEqual(len(processes), 2)
        self.assertTrue((output / 'matrix.json').is_file())


if __name__ == '__main__':
    unittest.main()
