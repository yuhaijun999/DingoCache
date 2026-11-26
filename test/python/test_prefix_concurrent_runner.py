"""Pure independent-prefix controller tests; no native service or host I/O."""
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_prefix_concurrent as runner
from test_prefix_mixed_runner import fixture as mixed_fixture


def fixture(policy, dirs, initial=False, pressure=True):
    text = mixed_fixture(policy, dirs, 'class-pressure', initial)
    text = text.replace('slot_size="1048576"} 60', 'slot_size="1048576"} 95')
    text = text.replace('slot_size="2097152"} 40', 'slot_size="2097152"} 0')
    if not pressure:
        text = text.replace('dfkv_evictions_total 64', 'dfkv_evictions_total 0')
        text = text.replace('path="foreground_evict"} 64', 'path="foreground_evict"} 0')
    return text


class PrefixConcurrentRunnerTest(unittest.TestCase):
    def setUp(self):
        self.dirs = ['/data/disk1/cases/a', '/data/disk2/cases/a', '/data/disk3/cases/a']
        self.summary = dict(native=dict(put_acks=100, put_bytes=409600, put_attempts=100,
            put_errors=0, put_unknown=0, get_hits=17, get_misses=3, get_attempts=20,
            get_errors=0, get_unknown=0), peak_abi_overlap=4, peak_get_abi_overlap=4, peak_backend_overlap=4)

    def check(self, before, after, policy='legacy', **kwargs):
        return runner.check_metrics(before, after, policy, self.summary, self.dirs,
                                    ack_puts=100, distinct_puts=90, **kwargs)

    def test_three_worker_levels_three_alternating_pairs(self):
        rows = runner.plan()
        self.assertEqual(len(rows), 18)
        self.assertEqual([row['case_id'] for row in rows], list(range(18)))
        for offset, workers in ((0, 1), (6, 4), (12, 16)):
            subset = rows[offset:offset+6]
            self.assertTrue(all(row['workers'] == workers for row in subset))
            self.assertEqual([row['policy'] for row in subset],
                ['legacy', 'lifetime', 'lifetime', 'legacy', 'legacy', 'lifetime'])
            self.assertTrue(all(row['state'] == 'not_attempted' for row in subset))
        for workers, pairs in [((4, 1), 1), ((2,), 1), ((1, 1), 1), ((), 1), ((True,), 1), ((1,), 0), ((1,), True)]:
            with self.subTest(workers=workers, pairs=pairs), self.assertRaises(ValueError):
                runner.plan(workers, pairs)

    def test_native_abi_overlap_not_handle_count_or_wrapper_overlap(self):
        for workers, abi in ((1, 1), (4, 2), (16, 16)):
            value = runner.check_concurrency(dict(peak_abi_overlap=abi, peak_get_abi_overlap=abi, peak_backend_overlap=workers), workers)
            self.assertEqual(value['peak_abi_overlap'], abi)
        for workers, abi in ((4, 1), (1, 2), (4, 0), (4, True), (4, None), (4, 5)):
            with self.subTest(workers=workers, abi=abi), self.assertRaises(ValueError):
                runner.check_concurrency(dict(peak_abi_overlap=abi, peak_get_abi_overlap=1, peak_backend_overlap=4), workers)

    def test_put_only_overlap_does_not_prove_concurrent_prefix_gets(self):
        for get_peak in (1, 0, None, True, 5):
            with self.subTest(get_peak=get_peak), self.assertRaises(ValueError):
                runner.check_concurrency(dict(peak_abi_overlap=4,
                    peak_get_abi_overlap=get_peak, peak_backend_overlap=4), 4)
        result = runner.check_concurrency(dict(peak_abi_overlap=4,
            peak_get_abi_overlap=2, peak_backend_overlap=4), 4)
        self.assertEqual(result['peak_get_abi_overlap'], 2)

    def test_native_misses_do_not_become_io_reads_and_upserts_not_admissions(self):
        for policy in ('legacy', 'lifetime'):
            result = self.check(fixture(policy, self.dirs, True), fixture(policy, self.dirs), policy,
                                require_pressure=True)
            self.assertEqual(result['dfkv_rdma_pull_requests_terminal_total'], 20)
            self.assertEqual(result['dfkv_uring_reads_total'], 17)
            self.assertEqual(result['allocator_admissions'], 95)
            self.assertEqual(result['ack_put_upserts'], 100)
            self.assertEqual(result['distinct_ack_objects'], 90)
            self.assertEqual(result['foreground_metadata_completed'], 64)

    def test_pressure_optional_for_smoke_but_not_falsely_proven(self):
        before = fixture('legacy', self.dirs, True)
        after = fixture('legacy', self.dirs, pressure=False)
        self.assertEqual(self.check(before, after)['capacity_evictions'], 0)
        with self.assertRaises(ValueError):
            self.check(before, after, require_pressure=True)

    def test_donor_and_metadata_denominators_are_not_fabricated_atomic(self):
        before, after = fixture('legacy', self.dirs, True), fixture('legacy', self.dirs)
        after = after.replace('dfkv_slab_extent_steals_total 0', 'dfkv_slab_extent_steals_total 3')
        after = after.replace('dfkv_slab_metadata_ops_attempts_total{path="foreground_evict"} 64',
                              'dfkv_slab_metadata_ops_attempts_total{path="foreground_evict"} 65')
        result = self.check(before, after)
        self.assertTrue(result['donor_confounded'])
        self.assertEqual(result['foreground_metadata_attempts'], 65)
        self.assertEqual(result['foreground_metadata_completed'], 64)

    def test_metric_missing_duplicate_fractional_nan_wrong_path_rejected(self):
        before, after = fixture('legacy', self.dirs, True), fixture('legacy', self.dirs)
        edits = [('dfkv_cache_put_total 100', 'dfkv_cache_put_total 99'),
            ('dfkv_uring_reads_total 17', 'dfkv_uring_reads_total 20'),
            ('result="not_found"} 3', 'result="not_found"} 2'),
            ('result="not_found"} 3', 'result="not_found",path2="fallback"} 3'),
            ('slot_size="1048576"} 95', 'slot_size="1048576"} 89'),
            ('slot_size="1048576"} 95', 'slot_size="1048576"} 101'),
            ('slot_size="2097152"} 0', 'slot_size="2097152"} 1'),
            ('dfkv_slab_extent_steals_total 0', 'dfkv_slab_extent_steals_total 0.5'),
            ('dfkv_cache_put_total 100', 'dfkv_cache_put_total NaN'),
            ('dfkv_storage_healthy 1', 'dfkv_storage_healthy 0'),
            ('dfkv_uring_inflight 0', 'dfkv_uring_inflight 1'),
            ('dfkv_slab_reclaimed_total 0', 'dfkv_slab_reclaimed_total 1'),
            ('dfkv_slab_metadata_ops_errors_total{path="foreground_evict"} 0',
             'dfkv_slab_metadata_ops_errors_total{path="foreground_evict"} 1')]
        # An unexpected label is itself a different series; the frozen expected
        # io_uring series must still exist exactly once.
        edits[3] = ('path="uring",result="not_found"', 'path="sync",result="not_found"')
        for old, new in edits:
            self.assertIn(old, after)
            with self.subTest(new=new), self.assertRaises((ValueError, RuntimeError)):
                self.check(before, after.replace(old, new))
        for extra in ('dfkv_cache_put_total 0\n', 'dfkv_storage_healthy 0\n',
                      'dfkv_slab_class_put_total{slot_size="1048576"} 0\n'):
            with self.subTest(extra=extra), self.assertRaises((ValueError, RuntimeError)):
                self.check(before, after + extra)
        with self.assertRaises(RuntimeError):
            self.check(before, after.replace('dfkv_cache_put_total 100\n', ''))

    def test_actual_labeled_zero_error_series_are_valid(self):
        # Retained first native smoke had these three zero-valued error series,
        # not a scalar counter. Valid labels must not turn a client PASS to FAIL.
        errors = ('dfkv_errors_total{op="put",status="io"} 0\n'
                  'dfkv_errors_total{op="get",status="io"} 0\n'
                  'dfkv_errors_total{op="any",status="invalid"} 0\n')
        before = fixture('legacy', self.dirs, True).replace('dfkv_errors_total 0\n', errors)
        after = fixture('legacy', self.dirs).replace('dfkv_errors_total 0\n', errors)
        result = self.check(before, after)
        self.assertEqual(result['dfkv_cache_put_total'], 100)

    def test_labeled_error_series_nonzero_or_identity_change_are_rejected(self):
        errors = ('dfkv_errors_total{op="put",status="io"} 0\n'
                  'dfkv_errors_total{op="get",status="io"} 0\n'
                  'dfkv_errors_total{op="any",status="invalid"} 0\n')
        before = fixture('legacy', self.dirs, True).replace('dfkv_errors_total 0\n', errors)
        after = fixture('legacy', self.dirs).replace('dfkv_errors_total 0\n', errors)
        line = 'dfkv_errors_total{op="get",status="io"} 0\n'
        replacements = ['', line.replace('op="get"', 'op="changed"'),
                        line.replace(' 0\n', ' 1\n'), line.replace(' 0\n', ' 0.5\n'),
                        line.replace(' 0\n', ' NaN\n'), line.replace(' 0\n', ' -1\n')]
        for replacement in replacements:
            with self.subTest(replacement=replacement), self.assertRaises((ValueError, RuntimeError)):
                self.check(before, after.replace(line, replacement))
        with self.assertRaises(ValueError):
            self.check(before, after + line)
        with self.assertRaises(ValueError):
            self.check(before.replace(line, line.replace(' 0\n', ' 1\n')), after)
        # Fallback and inflight remain scalar counters; arbitrary new labels
        # cannot bypass their zero/identity checks.
        for name in ('dfkv_uring_init_fallbacks_total', 'dfkv_uring_inflight'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.check(before, after.replace(name + ' 0', name + '{extra="x"} 0'))

    def test_native_counter_errors_unknown_and_boolean_rejected(self):
        before, after = fixture('legacy', self.dirs, True), fixture('legacy', self.dirs)
        for key, value in [('get_unknown', 1), ('put_errors', 1), ('get_attempts', 19),
                           ('put_bytes', -1), ('get_hits', True)]:
            saved = self.summary['native'][key]
            self.summary['native'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.check(before, after)
            self.summary['native'][key] = saved

    def test_distinct_ack_identity_excludes_errors_and_shared_refill_upserts(self):
        report = dict(initial_events=[dict(op='put', status='ack', object_id=0)],
            requests=[dict(events=[dict(op='put', status='ack', object_id=0),
                                   dict(op='put', status='ack', object_id=1),
                                   dict(op='get', status='hit', object_id=1),
                                   dict(op='put', status='unknown', object_id=2)])])
        self.assertEqual(runner.put_identities(report), (3, 2))

    def test_environment_clears_inherited_policy_and_requires_rdma(self):
        with mock.patch.dict(runner.os.environ, {'DFKV_RDMA': '0', 'DFKV_SLAB_PROTECT_SEC': '1'}, clear=True):
            env = runner.environment()
        self.assertEqual(env['DFKV_RDMA'], '1')
        self.assertEqual(env['DFKV_PUT_ACK_MODE'], 'disk')
        self.assertEqual(env['DFKV_RAM_TIER'], '0')
        self.assertNotIn('DFKV_SLAB_PROTECT_SEC', env)

    def _matrix(self, failure=None):
        stack = ExitStack()
        self.addCleanup(stack.close)
        root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        build = root / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build / name).write_bytes(b'fixture-no-execution')
        roots = [root / ('disk' + str(index)) for index in (1, 2, 3)]
        for path in roots:
            path.mkdir()
        output = root / 'out'
        workload = SimpleNamespace(generate=mock.Mock(side_effect=lambda **kwargs:dict(sha256=str(kwargs['workers']))),
            validate=mock.Mock(), load=lambda path:json.loads(Path(path).read_text()),
            check=mock.Mock(return_value=self.summary))
        stack.enter_context(mock.patch.dict(sys.modules, {'prefix_concurrent': workload}))
        stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
        stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
        stack.enter_context(mock.patch.object(runner, 'check_initial'))
        startup = stack.enter_context(mock.patch.object(runner, 'check_startup'))
        oracle = stack.enter_context(mock.patch.object(runner, 'check_metrics', return_value={'capacity_evictions': 4}))
        concurrency = stack.enter_context(mock.patch.object(runner, 'check_concurrency', return_value={'peak_abi_overlap': 4}))
        stack.enter_context(mock.patch.object(runner, 'put_identities', return_value=(100, 90)))
        stack.enter_context(mock.patch.object(runner.baseline, '_ready', return_value='before'))
        stack.enter_context(mock.patch.object(runner.baseline, '_scrape', return_value='after'))
        tools = stack.enter_context(mock.patch.object(runner, '_tools', return_value={'fixture': 'sha'}))
        sampler = stack.enter_context(mock.patch.object(runner, 'Sampler'))
        sampler.return_value.rows = [{'fixture': 'sample'}]
        processes = []
        def spawn(command, **kwargs):
            child = mock.Mock(pid=len(processes) + 100)
            child.returncode = None
            child.poll.side_effect = lambda:child.returncode
            def wait(timeout):
                child.returncode = 0
                return 0
            child.wait.side_effect = wait
            processes.append(child)
            return child
        stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=spawn))
        def client(command, env, log_path, sampler, *, timeout, receipt):
            self.assertEqual(timeout, 600)
            receipt.update(pid=200, exit=0, reaped=True, state='exited')
            path = Path(command[command.index('--output') + 1])
            path.mkdir()
            (path / 'report.json').write_text(json.dumps({'passed': True, 'transport': 'rdma'}))
            if failure == 'timeout':
                receipt.update(state='failed_reaped', exit=-15)
                raise TimeoutError('fixture timeout')
            if failure == 'build':
                (build / 'libdfkv.so').write_bytes(b'mutated')
            if failure == 'tools':
                tools.return_value = {'fixture': 'changed'}
            if failure == 'plan':
                Path(command[command.index('--plan') + 1]).write_text('{}')
            if failure == 'transport':
                (path / 'report.json').write_text(json.dumps({'passed': True, 'transport': 'fake-rdma-test'}))
        clients = stack.enter_context(mock.patch.object(runner, 'run_client', side_effect=client))
        if failure == 'oracle':
            oracle.side_effect = ValueError('native oracle failure')
        if failure == 'overlap':
            concurrency.side_effect = ValueError('no ABI overlap')
        if failure == 'cleanup':
            original = runner.common.stop_owned
            def stop(child):
                original(child)
                raise RuntimeError('cleanup failure')
            stack.enter_context(mock.patch.object(runner.common, 'stop_owned', side_effect=stop))
        result = runner.run(build, roots, output, 'tag')
        return result, output, processes, clients, startup, workload, oracle

    def test_matrix_three_worker_levels_fresh_alternating_cases_and_reap(self):
        result, output, processes, clients, startup, workload, oracle = self._matrix()
        self.assertTrue(result['passed'])
        self.assertEqual(clients.call_count, 18)
        self.assertEqual(startup.call_count, 18)
        self.assertEqual(workload.generate.call_count, 3)
        self.assertEqual(workload.generate.call_args_list, [mock.call(workers=value, rounds=120,
            interval_ns=100000000, arrival_batch=8, prefill=12288, length=4096,
            timeout_ns=5000000000) for value in (1, 4, 16)])
        self.assertEqual(len({path for row in result['cases'] for path in row['data_dirs']}), 54)
        for row, child in zip(result['cases'], processes):
            self.assertEqual(row['state'], 'passed')
            self.assertEqual(row['environment']['DFKV_RDMA'], '1')
            self.assertEqual(row['client_timeout_sec'], 600)
            self.assertTrue(row['server_reaped'])
            self.assertEqual(row['server_exit'], 0)
            child.terminate.assert_called_once()
        self.assertTrue(all(call.kwargs['require_pressure'] for call in oracle.call_args_list))
        saved = json.loads((output / 'matrix.json').read_text())
        self.assertEqual(saved['cases'], result['cases'])

    def test_matrix_failure_retains_raw_report_and_summary_then_fail_stops(self):
        for failure in ('timeout', 'build', 'tools', 'plan', 'transport', 'oracle', 'overlap', 'cleanup'):
            with self.subTest(failure=failure):
                result, output, processes, clients, startup, workload, oracle = self._matrix(failure)
                self.assertFalse(result['passed'])
                self.assertEqual(clients.call_count, 1)
                row = result['cases'][0]
                self.assertEqual(row['state'], 'failed')
                self.assertTrue(row['server_reaped'])
                self.assertIn('client_report_sha256', row)
                self.assertIn('summary', row)
                self.assertTrue(all(row['state'] == 'not_attempted' for row in result['cases'][1:]))
                self.assertTrue((output / 'case-00-legacy/client/report.json').is_file())
                self.assertTrue((output / 'matrix.json').is_file())

    def test_invalid_inputs_before_guest_paths_or_spawn(self):
        workload = SimpleNamespace(generate=mock.Mock(side_effect=ValueError('bad config')))
        with mock.patch.dict(sys.modules, {'prefix_concurrent': workload}), \
                mock.patch.object(runner.common, 'guest_paths') as paths, \
                mock.patch.object(runner.subprocess, 'Popen') as spawn:
            for kwargs in ({'workers': (2,)}, {'pairs': 0}, {'rounds': 0}):
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    runner.run('/missing', [], '/unused', 'tag', **kwargs)
            with self.assertRaises(ValueError):
                runner.run('/missing', ['/data/a,b'], '/unused', 'tag')
            with self.assertRaises(ValueError):
                runner.run('/missing', [], '/unused', 'bad/tag')
            paths.assert_not_called()
            spawn.assert_not_called()

    def test_cli_forwards_frozen_workload_configuration(self):
        with mock.patch.object(runner, 'run', return_value={'passed': True}) as run:
            self.assertEqual(runner.main(['--build', '/build', '--data-root', '/data/a', '--output', '/out',
                '--tag', 't', '--workers', '4', '16', '--pairs', '1', '--rounds', '8',
                '--interval-ns', '1000000', '--arrival-batch', '2']), 0)
            self.assertEqual(run.call_args.kwargs, dict(workers=[4, 16], pairs=1, rounds=8,
                interval_ns=1000000, arrival_batch=2))


if __name__ == '__main__':
    unittest.main()
