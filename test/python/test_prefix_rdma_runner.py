"""Pure prefix RDMA matrix tests; no native service, VM or disks are used."""

from contextlib import ExitStack
import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_prefix_rdma as runner
from test_long_residence_runner import metrics


def edge_rows():
    rows = []
    for name, boundary in (('head', 0), ('middle', 4), ('tail', 7)):
        for phase in ('first', 'revisit'):
            rows.append(dict(kind=f'forced-{name}-{phase}', object_ids=list(range(8)),
                state='completed', first_unconfirmed_block=boundary if phase == 'first' else 8,
                outcomes=(['hit'] * boundary + ['miss'] + ['not_attempted'] * (7 - boundary)
                          if phase == 'first' else ['hit'] * 8),
                refill_put_acks=8-boundary if phase == 'first' else 0,
                usable_tokens=sum((index % 4 + 1) * 8 for index in range(boundary))
                    if phase == 'first' else 160,
                confirmed_prefix_tokens=sum((index % 4 + 1) * 8 for index in range(boundary))
                    if phase == 'first' else 160, target_tokens=160))
    return rows


class PrefixRdmaRunnerTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.dirs = ['/data/disk1/cases/a', '/data/disk2/cases/a', '/data/disk3/cases/a']
        self.summary = dict(native=dict(put_acks=100, put_bytes=409600, put_errors=0,
            put_unknown=0, put_attempts=100, get_hits=17, get_misses=3,
            get_errors=0, get_unknown=0, get_attempts=20))

    def test_fixed_ab_ba_plan_and_full_case_identity(self):
        rows = runner.plan()
        self.assertEqual([r['policy'] for r in rows], ['legacy', 'lifetime', 'lifetime', 'legacy'])
        self.assertEqual([r['case_id'] for r in rows], list(range(4)))
        self.assertEqual(runner.plan(1, 1), rows[2:])
        for pairs, start in ((True, 0), (0, 0), (3, 0), (2, 1), (1, True), (1, -1)):
            with self.subTest(pairs=pairs, start=start), self.assertRaises(ValueError):
                runner.plan(pairs, start)

    def test_environment_is_rdma_only_and_discards_inherited_scores(self):
        with mock.patch.dict(runner.os.environ, {'DFKV_RDMA': '0', 'DFKV_SLAB_PROTECT_SEC': '1',
                                               'DFKV_RAM_TIER': '1', 'OTHER': 'retain'}, clear=True):
            env = runner.environment()
        self.assertEqual(env['DFKV_RDMA'], '1')
        self.assertEqual(env['DFKV_RAM_TIER'], '0')
        self.assertEqual(env['DFKV_SERVER_URING'], '1')
        self.assertEqual(env['DFKV_PUT_ACK_MODE'], 'disk')
        self.assertEqual(env['DFKV_GET_MISS_RETRIES'], '0')
        self.assertNotIn('DFKV_SLAB_PROTECT_SEC', env)
        self.assertEqual(env['OTHER'], 'retain')

    def test_invalid_tag_paths_selection_fail_before_guest_or_spawn(self):
        with mock.patch.object(runner.common, 'guest_paths') as guest, \
                mock.patch.object(runner.subprocess, 'Popen') as spawn:
            for tag in ('', '../bad', 'x' * 41):
                with self.subTest(tag=tag), self.assertRaises(ValueError):
                    runner.run('/missing', [], '/unused', tag)
            with self.assertRaises(ValueError):
                runner.run('/missing', ['/data/a,b'], '/unused', 'tag')
            with self.assertRaises(ValueError):
                runner.run('/missing', [], '/unused', 'tag', pairs=2, start_pair=1)
            guest.assert_not_called()
            spawn.assert_not_called()


    def test_metric_oracle_reconciles_all_native_calls_and_class_eviction(self):
        for policy in ('legacy', 'lifetime'):
            with self.subTest(policy=policy):
                values = runner.check_metrics(metrics(policy, self.dirs, True),
                    metrics(policy, self.dirs), policy, self.summary, self.dirs)
                self.assertEqual(values['class_foreground_evictions'], 64)
                self.assertEqual(values['dfkv_uring_completions_total'], 17)

    def test_error_unknown_bad_types_missing_and_wrong_denominators_fail(self):
        before, after = metrics('legacy', self.dirs, True), metrics('legacy', self.dirs)
        for key, value in [('get_errors', 1), ('put_errors', 1), ('get_unknown', 1),
                           ('put_unknown', 1), ('get_attempts', 19), ('put_attempts', 99),
                           ('get_hits', True), ('put_bytes', -1)]:
            changed = dict(native=dict(self.summary['native']))
            changed['native'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.check_metrics(before, after, 'legacy', changed, self.dirs)
        with self.assertRaises(ValueError):
            runner.check_metrics(before, after, 'legacy', {'native': {}}, self.dirs)

    def test_metric_paths_fallback_health_and_exact_counts_fail_closed(self):
        before, after = metrics('lifetime', self.dirs, True), metrics('lifetime', self.dirs)
        for old, new in [('dfkv_uring_reads_total 17', 'dfkv_uring_reads_total 16'),
                         ('path="uring"', 'path="sync"'),
                         ('dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_init_fallbacks_total 1'),
                         ('dfkv_slab_healthy 1', 'dfkv_slab_healthy 0'),
                         ('dfkv_evictions_total 64', 'dfkv_evictions_total 0'),
                         ('dfkv_slab_watermark_evictions_total 0', 'dfkv_slab_watermark_evictions_total 1')]:
            with self.subTest(new=new), self.assertRaises((ValueError, RuntimeError)):
                runner.check_metrics(before, after.replace(old, new), 'lifetime', self.summary, self.dirs)

    def test_forced_gap_and_revisit_oracle_checks_raw_boundaries_and_writes(self):
        report = {'requests': edge_rows()}
        runner.check_forced_edges(report)
        changes = [lambda rows: rows.pop(), lambda rows: rows.append(dict(rows[0])),
                   lambda rows: rows[0].update(first_unconfirmed_block=1),
                   lambda rows: rows[2].update(outcomes=['hit'] * 8),
                   lambda rows: rows[4].update(refill_put_acks=0),
                   lambda rows: rows[1].update(usable_tokens=0)]
        for change in changes:
            rows = copy.deepcopy(report['requests'])
            change(rows)
            with self.subTest(change=change), self.assertRaises(ValueError):
                runner.check_forced_edges({'requests': rows})

    def test_native_capacity_loss_on_refilled_revisit_is_valid_not_an_io_failure(self):
        # Native ABBA case 02: no client read error/unknown and all PUTs ACKed.
        # A revisit observed prefix loss under full-capacity pressure; there
        # is no per-victim trace or cross-request retention guarantee.
        rows = edge_rows()
        rows[3].update(first_unconfirmed_block=0, outcomes=['miss'] + ['not_attempted'] * 7,
                       usable_tokens=0, confirmed_prefix_tokens=0, refill_put_acks=8)
        rows[5].update(first_unconfirmed_block=1,
                       outcomes=['hit', 'miss'] + ['not_attempted'] * 6,
                       usable_tokens=8, confirmed_prefix_tokens=8, refill_put_acks=7)
        runner.check_forced_edges({'requests': rows})

    def test_capacity_revisit_still_rejects_wrong_prefix_tokens_refill_and_unknown(self):
        base = edge_rows()
        base[3].update(first_unconfirmed_block=0, outcomes=['miss'] + ['not_attempted'] * 7,
                       usable_tokens=0, confirmed_prefix_tokens=0, refill_put_acks=8)
        changes = [lambda rows: rows[3].update(outcomes=['hit'] + ['not_attempted'] * 7),
                   lambda rows: rows[3].update(usable_tokens=8),
                   lambda rows: rows[3].update(confirmed_prefix_tokens=8),
                   lambda rows: rows[3].update(refill_put_acks=7),
                   lambda rows: rows[3].update(first_unconfirmed_block=True),
                   lambda rows: rows[3].update(outcomes=['unknown'] + ['not_attempted'] * 7),
                   lambda rows: rows[3].update(state='failed')]
        for change in changes:
            rows = copy.deepcopy(base)
            change(rows)
            with self.subTest(change=change), self.assertRaises(ValueError):
                runner.check_forced_edges({'requests': rows})

    def _matrix(self, *, timeout=False, failed_exit=False, ready_fail=False,
                bad_report=False, fake_transport=False, mutate_build=False, mutate_tools=False,
                mutate_plan=False, stop_fail=False):
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        build = root/'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build/name).write_bytes(b'never-executed-fixture')
        roots = [root/f'disk{i}' for i in (1, 2, 3)]
        for path in roots:
            path.mkdir()
        output = root/'results'
        workload = SimpleNamespace(generate=mock.Mock(return_value={'sha256': 'fixture'}),
            check=mock.Mock(return_value=self.summary), load=lambda p: json.loads(Path(p).read_text()))
        self.stack.enter_context(mock.patch.dict(sys.modules, {'prefix_rdma': workload}))
        self.stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
        self.stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
        self.stack.enter_context(mock.patch.object(runner.residence, 'check_initial'))
        startup = self.stack.enter_context(mock.patch.object(runner.residence, 'check_startup'))
        oracle = self.stack.enter_context(mock.patch.object(runner, 'check_metrics', return_value={'evictions': 1}))
        self.stack.enter_context(mock.patch.object(runner.baseline, '_scrape', return_value='after'))
        ready = self.stack.enter_context(mock.patch.object(runner.baseline, '_ready', return_value='before'))
        if ready_fail:
            ready.side_effect = TimeoutError('controlled readiness failure')
        tools = self.stack.enter_context(mock.patch.object(runner, '_tools', return_value={'fixture': 'sha'}))
        processes = []
        def spawn(command, **kwargs):
            server = mock.Mock(pid=len(processes) + 1, returncode=None)
            server.poll.side_effect = lambda: server.returncode
            def wait(timeout):
                server.returncode = 0
                return 0
            server.wait.side_effect = wait
            processes.append(server)
            return server
        self.stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=spawn))
        def child(command, **kwargs):
            self.assertEqual(kwargs['env']['DFKV_RDMA'], '1')
            self.assertEqual(kwargs['env']['DFKV_SERVER_URING'], '1')
            self.assertEqual(kwargs['timeout'], 300)
            path = Path(command[command.index('--output') + 1])
            path.mkdir()
            (path/'report.json').write_text(json.dumps({'passed': not bad_report,
                'transport': 'fake-rdma-test' if fake_transport else 'rdma', 'requests': edge_rows()}))
            if timeout:
                raise runner.subprocess.TimeoutExpired(command, kwargs['timeout'])
            if failed_exit:
                raise runner.subprocess.CalledProcessError(1, command)
            if mutate_build:
                (build/'libdfkv.so').write_bytes(b'changed')
            if mutate_tools:
                tools.return_value = {'fixture': 'changed'}
            if mutate_plan:
                (output/'plan.json').write_text('{}')
        children = self.stack.enter_context(mock.patch.object(runner.subprocess, 'run', side_effect=child))
        if stop_fail:
            original = runner.common.stop_owned
            def stop(server):
                original(server)
                raise RuntimeError('controlled cleanup failure')
            self.stack.enter_context(mock.patch.object(runner.common, 'stop_owned', side_effect=stop))
        result = runner.run(build, roots, output, 'tag')
        return result, output, processes, children, startup, oracle

    def test_matrix_fresh_paths_fixed_order_rdma_defaults_and_normal_reap(self):
        result, output, processes, children, startup, oracle = self._matrix()
        self.assertTrue(result['passed'])
        self.assertFalse(result['formal_ab'])
        self.assertEqual(result['gpu'], 'excluded')
        self.assertEqual(children.call_count, 4)
        self.assertEqual(oracle.call_count, 4)
        self.assertEqual(startup.call_count, 4)
        self.assertEqual(len({path for row in result['cases'] for path in row['data_dirs']}), 12)
        self.assertTrue((output/'matrix.json').is_file())
        for row, process in zip(result['cases'], processes):
            self.assertEqual(row['state'], 'passed')
            self.assertTrue(row['server_reaped'])
            self.assertEqual(row['server_exit'], 0)
            self.assertEqual(row['client_timeout_sec'], 300)
            self.assertEqual(row['command'][row['command'].index('--cap') + 1], '12884901888')
            self.assertEqual(row['environment']['DFKV_PUT_ACK_MODE'], 'disk')
            self.assertNotIn('DFKV_SLAB_PROTECT_SEC', row['environment'])
            self.assertEqual(row['client_command'][2], 'run')
            process.terminate.assert_called_once()

    def test_timeout_exit_bad_report_and_cleanup_fail_stop_without_fake_counts(self):
        for option in ('timeout', 'failed_exit', 'bad_report', 'stop_fail'):
            with self.subTest(option=option):
                result, output, processes, children, startup, oracle = self._matrix(**{option: True})
                self.assertFalse(result['passed'])
                self.assertTrue(result['errors'])
                self.assertEqual([r['state'] for r in result['cases']],
                                 ['failed', 'not_attempted', 'not_attempted', 'not_attempted'])
                self.assertEqual(children.call_count, 1)
                self.assertEqual(len(processes), 1)
                self.assertTrue(result['cases'][0]['server_reaped'])
                self.assertTrue((output/'case-00-legacy/client/report.json').exists())
                if option in ('timeout', 'failed_exit'):
                    self.assertIsNone(result['cases'][0]['counts'])
                    self.assertEqual(oracle.call_count, 0)
                if option == 'timeout':
                    self.assertEqual(result['cases'][0]['client_state'], 'process_timeout_reaped')

    def test_fake_transport_report_cannot_pass_real_matrix(self):
        result, output, processes, children, startup, oracle = self._matrix(fake_transport=True)
        self.assertFalse(result['passed'])
        self.assertIn('real RDMA transport', result['errors'][0])
        self.assertIsNone(result['cases'][0]['counts'])
        self.assertEqual(oracle.call_count, 0)
        self.assertEqual(len(processes), 1)
        self.assertTrue(result['cases'][0]['server_reaped'])

    def test_readiness_failure_never_runs_client_and_reaps_owned_server(self):
        result, output, processes, children, startup, oracle = self._matrix(ready_fail=True)
        self.assertFalse(result['passed'])
        self.assertEqual(children.call_count, 0)
        self.assertEqual(len(processes), 1)
        self.assertTrue(result['cases'][0]['server_reaped'])

    def test_frozen_build_tools_and_plan_checked_before_next_case(self):
        for option in ('mutate_build', 'mutate_tools', 'mutate_plan'):
            with self.subTest(option=option):
                result, output, processes, children, startup, oracle = self._matrix(**{option: True})
                self.assertFalse(result['passed'])
                self.assertIn('frozen tools or plan changed', result['errors'][0])
                self.assertEqual(len(processes), 1)
                self.assertEqual(children.call_count, 1)
                self.assertEqual(result['cases'][1]['state'], 'not_attempted')

    def test_main_forwards_only_pair_selection_and_exits_on_failure(self):
        with mock.patch.object(runner, 'run', return_value={'passed': False}) as run:
            self.assertEqual(runner.main(['--build', '/unused/build', '--data-root', '/unused/disk',
                '--output', '/unused/output', '--tag', 'fresh', '--pairs', '1', '--start-pair', '1']), 1)
        self.assertEqual(run.call_args.kwargs, dict(pairs=1, start_pair=1))


if __name__ == '__main__':
    unittest.main()
