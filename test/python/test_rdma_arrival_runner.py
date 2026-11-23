"""Pure RDMA matrix planning/orchestration tests; no native service is started."""

from contextlib import ExitStack
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_rdma_arrival as runner


def metrics(policy, *, before=False):
    values = {'dfkv_cache_put_total': 12, 'dfkv_bytes_written_total': 49152,
              'dfkv_disk_objects': 12,
              'dfkv_rdma_pull_requests_dispatched_total': 12,
              'dfkv_uring_reads_total': 12, 'dfkv_uring_completions_total': 12,
              'dfkv_slab_lifetime_read_success_total': 12 if policy == 'lifetime' else 0}
    rows = [f'{name} {0 if before else value}' for name, value in values.items()]
    for name, value in (('success_nonempty', 12), ('not_found', 0)):
        rows.append('dfkv_rdma_pull_requests_terminal_total{path="uring",result="'
                    + name + '"} ' + str(0 if before else value))
    rows += ['dfkv_storage_healthy 1', 'dfkv_slab_healthy 1', 'dfkv_errors_total 0',
             'dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_inflight 0', 'dfkv_evictions_total 0']
    return '\n'.join(rows) + '\n'


class RdmaArrivalRunnerTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.counts = runner.trace.validate(runner.trace.generate(objects=8, rounds=4, length=4096))

    def test_default_plan_is_three_worker_levels_with_ab_ba_ab(self):
        result = runner.plan()
        self.assertEqual(len(result), 18)
        self.assertEqual([row['case_id'] for row in result], list(range(18)))
        for worker in (1, 4, 16):
            subset = [row for row in result if row['workers'] == worker]
            self.assertEqual([row['policy'] for row in subset],
                             ['legacy', 'lifetime', 'lifetime', 'legacy', 'legacy', 'lifetime'])
            self.assertEqual([row['pair'] for row in subset], [0, 0, 1, 1, 2, 2])
        self.assertEqual(len(runner.plan([4], 1)), 2)

    def test_invalid_plan_options_rejected(self):
        for workers in ([], [True], [2], [16, 1], [1, 1], '1'):
            with self.subTest(workers=workers), self.assertRaises(ValueError):
                runner.plan(workers)
        for pairs in (True, 0, 4, 1.0):
            with self.subTest(pairs=pairs), self.assertRaises(ValueError):
                runner.plan(pairs=pairs)

    def test_metric_oracle_both_policies(self):
        for policy in ('legacy', 'lifetime'):
            result = runner.check_metrics(metrics(policy, before=True), metrics(policy), policy, self.counts)
            self.assertEqual(result['dfkv_uring_completions_total'], 12)
            self.assertEqual(result['dfkv_slab_lifetime_read_success_total'], 12 if policy == 'lifetime' else 0)

    def test_oracle_rejects_wrong_path_heat_fallback_and_denominator(self):
        before, after = metrics('lifetime', before=True), metrics('lifetime')
        replacements = [('dfkv_uring_reads_total 12', 'dfkv_uring_reads_total 11'),
                        ('dfkv_slab_lifetime_read_success_total 12', 'dfkv_slab_lifetime_read_success_total 0'),
                        ('dfkv_bytes_written_total 49152', 'dfkv_bytes_written_total 49151'),
                        ('path="uring"', 'path="sync"'),
                        ('dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_init_fallbacks_total 1'),
                        ('dfkv_uring_inflight 0', 'dfkv_uring_inflight 1'),
                        ('dfkv_storage_healthy 1', 'dfkv_storage_healthy 0'),
                        ('dfkv_evictions_total 0', 'dfkv_evictions_total 1')]
        for old, new in replacements:
            with self.subTest(new=new), self.assertRaises(ValueError):
                runner.check_metrics(before, after.replace(old, new), 'lifetime', self.counts)
        with self.assertRaises(ValueError):
            runner.check_metrics(before, after, 'unknown', self.counts)

    def test_oracle_rejects_missing_nonfinite_and_extra_terminal_activity(self):
        before, after = metrics('legacy', before=True), metrics('legacy')
        for value in ('nan', '-1', 'inf'):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                runner.check_metrics(before, after.replace('dfkv_uring_reads_total 12',
                                     'dfkv_uring_reads_total ' + value), 'legacy', self.counts)
        with self.assertRaises(RuntimeError):
            runner.check_metrics(before, after.replace('dfkv_uring_reads_total 12\n', ''), 'legacy', self.counts)
        with self.assertRaises(ValueError):
            runner.check_metrics(before, after.replace('result="not_found"} 0',
                                 'result="not_found"} 1'), 'legacy', self.counts)

    def _matrix(self, *, child_ok=True, audit_error=False, mutate_build=False,
                bad_denominator=False, stop_error=False, mutate_tools=False, bad_tools=False,
                bad_initial=False, ready_error=False):
        temporary = self.stack.enter_context(tempfile.TemporaryDirectory())
        root = Path(temporary)
        build = root / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build / name).write_bytes(b'fixture-not-executed')
        roots = [root / f'disk{index}' for index in (1, 2, 3)]
        for disk in roots:
            disk.mkdir()
        output = root / 'results'
        self.stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
        self.stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
        processes = []

        def start(command, **kwargs):
            server = mock.Mock()
            server.returncode, server.pid = None, len(processes) + 1
            server.poll.side_effect = lambda: server.returncode
            def wait(timeout):
                server.returncode = 0
                return 0
            server.wait.side_effect = wait
            processes.append(server)
            return server
        self.stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=start))

        def ready(server, port, log, policy):
            if ready_error:
                raise TimeoutError('controlled readiness deadline')
            index = len(processes) - 1
            initial = 1 if bad_initial else 0
            return ''.join('dfkv_disk_objects{disk="' + str(disk / f'sg-arrival-tag-{index:02d}-{policy}') + '"} ' + str(initial) + '\n'
                           for disk in roots)
        self.stack.enter_context(mock.patch.object(runner.baseline, '_ready', side_effect=ready))
        scrape = self.stack.enter_context(mock.patch.object(runner.baseline, '_scrape', return_value='fixture'))
        self.stack.enter_context(mock.patch.object(runner, 'check_metrics', return_value={'fixture': 1}))

        tools = runner._source_hashes()
        source_hashes = self.stack.enter_context(mock.patch.object(runner, '_source_hashes', return_value=dict(tools)))

        def client(source, output, **kwargs):
            self.assertFalse(kwargs['tcp'])
            self.assertEqual(kwargs['timeout_ns'], 5000000000)
            self.assertEqual(kwargs['process_timeout_sec'], 30)
            output.mkdir()
            (output / 'report.json').write_text('{}\n')
            if mutate_build:
                (build / 'libdfkv.so').write_bytes(b'changed')
            if mutate_tools:
                source_hashes.return_value = dict(tools, **{'run_rdma_arrival.py': 'changed'})
            return {'passed': child_ok}
        supervise = self.stack.enter_context(mock.patch.object(runner.arrival, 'supervise', side_effect=client))
        checked = dict(config={'tools': {name: tools[name] for name in runner.arrival.TOOLS}},
                       passed=child_ok, completed_requests=24, offered_requests=24,
                       acked_put_bytes=49152, hit_get_bytes=49152)
        if bad_tools:
            checked['config']['tools']['eviction_arrival.py'] = 'forged'
        if bad_denominator:
            checked['completed_requests'] = 23
        audit = self.stack.enter_context(mock.patch.object(runner.arrival, 'check', return_value=checked))
        if audit_error:
            audit.side_effect = ValueError('controlled forged report')
        if stop_error:
            original = runner.common.stop_owned
            def stop(server):
                original(server)
                raise RuntimeError('controlled stop failure')
            self.stack.enter_context(mock.patch.object(runner.common, 'stop_owned', side_effect=stop))
        result = runner.run(build, roots, output, 'tag', workers=[1], pairs=1)
        for call in scrape.call_args_list:
            self.assertEqual(call, mock.call(12010))
        return result, output, processes, supervise

    def test_matrix_records_both_policies_fresh_dirs_and_forced_rdma(self):
        result, output, processes, supervise = self._matrix()
        self.assertTrue(result['passed'])
        self.assertFalse(result['formal_ab'])
        self.assertEqual(result['gpu'], 'excluded')
        self.assertEqual([row['state'] for row in result['cases']], ['passed', 'passed'])
        self.assertEqual(supervise.call_count, 2)
        self.assertTrue((output / 'matrix.json').is_file())
        self.assertEqual(len({path for row in result['cases'] for path in row['data_dirs']}), 6)
        for row in result['cases']:
            self.assertEqual(row['server_environment']['DFKV_PUT_ACK_MODE'], 'disk')
            self.assertEqual(row['server_environment']['DFKV_SERVER_URING'], '1')
            self.assertEqual(row['server_environment']['DFKV_RDMA'], '1')
            self.assertTrue(row['server_reaped'])
            self.assertEqual(row['server_exit'], 0)
        for process in processes:
            process.terminate.assert_called_once()

    def test_failed_child_or_bad_audit_stops_without_retry_and_retains_plan(self):
        for option in ('child_ok', 'audit_error', 'bad_denominator', 'stop_error', 'bad_tools'):
            with self.subTest(option=option):
                options = {option: False if option == 'child_ok' else True}
                result, output, processes, supervise = self._matrix(**options)
                self.assertFalse(result['passed'])
                self.assertTrue(result['errors'])
                self.assertEqual([row['state'] for row in result['cases']], ['failed', 'not_attempted'])
                self.assertEqual(supervise.call_count, 1)
                self.assertEqual(len(processes), 1)
                self.assertTrue((output / 'matrix.json').is_file())
                self.assertTrue(result['cases'][0]['server_reaped'])

    def test_bad_initial_state_or_readiness_stops_before_client_and_reaps(self):
        for options in ({'bad_initial': True}, {'ready_error': True}):
            with self.subTest(options=options):
                result, output, processes, supervise = self._matrix(**options)
                self.assertFalse(result['passed'])
                self.assertEqual(len(processes), 1)
                self.assertEqual(supervise.call_count, 0)
                self.assertEqual([row['state'] for row in result['cases']], ['failed', 'not_attempted'])
                self.assertTrue(result['cases'][0]['server_reaped'])
                self.assertTrue((output / 'matrix.json').is_file())

    def test_build_mutation_is_rejected_before_second_case(self):
        result, _, processes, _ = self._matrix(mutate_build=True)
        self.assertFalse(result['passed'])
        self.assertIn('build, tools or frozen trace changed', result['errors'][0])
        self.assertEqual(len(processes), 1)
        self.assertEqual(result['cases'][1]['state'], 'not_attempted')

    def test_source_mutation_is_rejected_before_second_case(self):
        result, _, processes, _ = self._matrix(mutate_tools=True)
        self.assertFalse(result['passed'])
        self.assertIn('tools or frozen trace changed', result['errors'][0])
        self.assertEqual(len(processes), 1)
        self.assertEqual(result['cases'][1]['state'], 'not_attempted')

    def test_invalid_tag_or_plan_rejected_before_guest_or_path_operations(self):
        with mock.patch.object(runner.common, 'guest_paths') as guest:
            for tag in ('../x', '', 'x' * 41):
                with self.subTest(tag=tag), self.assertRaises(ValueError):
                    runner.run('/absent', [], '/unused', tag)
            with self.assertRaises(ValueError):
                runner.run('/absent', [], '/unused', 'tag', workers=[2])
            guest.assert_not_called()

    def test_comma_in_data_root_is_rejected_before_guest_or_server_operations(self):
        with mock.patch.object(runner.common, 'guest_paths') as guest, \
                mock.patch.object(runner.subprocess, 'Popen') as spawn:
            with self.assertRaisesRegex(ValueError, 'comma separator'):
                runner.run('/absent', ['/data/disk1/cases/a,b'], '/unused', 'tag')
            guest.assert_not_called()
            spawn.assert_not_called()


if __name__ == '__main__':
    unittest.main()
