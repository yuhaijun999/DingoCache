"""Recovery controller tests use owned-process mocks, never native services."""
from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_prefix_recovery as runner
from test_prefix_mixed_runner import fixture as mixed_fixture


def fixture(*args):
    text = mixed_fixture(*args).replace('dfkv_errors_total 0\n',
        'dfkv_errors_total{op="put",status="io"} 0\n'
        'dfkv_errors_total{op="get",status="io"} 0\n'
        'dfkv_errors_total{op="any",status="invalid"} 0\n')
    for path in ('record_write', 'extent_bind', 'donor_bind', 'table_sync', 'bind_sync', 'donor_sync'):
        text += f'dfkv_slab_metadata_ops_errors_total{{path="{path}"}} 0\n'
    return text


class RecoveryRunnerTest(unittest.TestCase):
    def test_matrix_covers_both_starts_and_alternates(self):
        rows = runner.plan(3)
        self.assertEqual(len(rows), 8)
        self.assertEqual({(r['stop'], r['transition'], r['policies'][0]) for r in rows},
                         {(s, t, p) for s in ('clean', 'dirty') for t in ('same', 'switch')
                          for p in ('legacy', 'lifetime')})
        for row in rows:
            self.assertEqual(len(row['policies']), 4)
            for a, b in zip(row['policies'], row['policies'][1:]):
                self.assertEqual(a == b, row['transition'] == 'same')
        for value in (True, 0, 101, 1.0):
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.plan(value)

    def test_recovery_geometry_health_and_dirty_reset_contract(self):
        dirs = ['/data/disk1/a', '/data/disk2/a', '/data/disk3/a']
        text = fixture('legacy', dirs, 'class-pressure', True)
        text += 'dfkv_slab_unclean_resets_total 0\ndfkv_slab_lifetime_visible_recovered_objects 0\n'
        runner.check_restart(text, dirs, dirty=False, policy='legacy')
        dirty = text.replace('dfkv_slab_unclean_resets_total 0', 'dfkv_slab_unclean_resets_total 3')
        runner.check_restart(dirty, dirs, dirty=True, policy='legacy')
        for malformed in (dirty.replace('total 3', 'total 2'),
                          dirty.replace('dfkv_slab_healthy 1', 'dfkv_slab_healthy 0'),
                          dirty.replace('dfkv_uring_inflight 0', 'dfkv_uring_inflight 1'),
                          dirty + 'dfkv_slab_metadata_ops_errors_total{path="donor_bind"} 1\n',
                          dirty + 'dfkv_slab_unclean_resets_total 3\n',
                          dirty.replace('disk="/data/disk1/a"} 0', 'disk="/data/disk1/a"} 1'),
                          dirty.replace('disk="/data/disk1/a"} 0', 'disk="/data/disk1/a"} nan')):
            with self.subTest(malformed=malformed[-100:]), self.assertRaises((ValueError, RuntimeError)):
                runner.check_restart(malformed, dirs, dirty=True, policy='legacy')

    def test_lifetime_startup_visible_recovery_before_reads(self):
        dirs = ['/data/disk1/a', '/data/disk2/a', '/data/disk3/a']
        text = fixture('lifetime', dirs, 'class-pressure', True)
        text = text.replace('disk="/data/disk1/a"} 0', 'disk="/data/disk1/a"} 4')
        text += 'dfkv_slab_unclean_resets_total 0\ndfkv_slab_lifetime_visible_recovered_objects 4\n'
        runner.check_restart(text, dirs, dirty=False, policy='lifetime')
        with self.assertRaises(ValueError):
            runner.check_restart(text.replace('recovered_objects 4', 'recovered_objects 3'),
                                 dirs, dirty=False, policy='lifetime')

    def test_native_prometheus_reconciliation_rejects_false_totals_and_paths(self):
        dirs = ['/data/disk1/a', '/data/disk2/a', '/data/disk3/a']
        counts = dict(put_acks=100, put_bytes=409600, get_hits=17, get_misses=3,
                      get_errors=0, get_unknown=0, put_errors=0, put_unknown=0,
                      get_attempts=20, put_attempts=100)
        for policy in ('legacy', 'lifetime'):
            before = fixture(policy, dirs, 'class-pressure', True)
            after = fixture(policy, dirs, 'class-pressure')
            result = runner.check_client_metrics(before, after, policy, counts)
            self.assertEqual(result['capacity_evictions'], 64)
            for old, new in [('dfkv_cache_put_total 100', 'dfkv_cache_put_total 99'),
                             ('dfkv_bytes_written_total 409600', 'dfkv_bytes_written_total 1'),
                             ('dfkv_uring_completions_total 17', 'dfkv_uring_completions_total 16'),
                             ('result="not_found"} 3', 'result="not_found"} 2'),
                             ('path="uring",result="not_found"', 'path="sync",result="not_found"'),
                             ('result="io_error"} 0', 'result="io_error"} 1'),
                             ('dfkv_slab_healthy 1', ''),
                             ('dfkv_errors_total{op="put",status="io"} 0', ''),
                             ('dfkv_errors_total{op="put",status="io"} 0',
                              'dfkv_errors_total{op="put",status="io"} 0\ndfkv_errors_total{op="put",status="io"} 0'),
                             ('dfkv_slab_metadata_ops_errors_total{path="donor_sync"} 0', ''),
                             ('dfkv_cache_put_total 100', 'dfkv_cache_put_total 100.5')]:
                with self.subTest(policy=policy, old=old), self.assertRaises((ValueError, RuntimeError)):
                    runner.check_client_metrics(before, after.replace(old, new), policy, counts)
            with self.assertRaises(ValueError):
                runner.check_client_metrics(before, after, policy, dict(counts, get_unknown=1))

    def test_snapshot_cannot_pass_with_no_residents_or_partial_prefix(self):
        good = dict(objects=['hit'] * 32, summary={'confirmed_prefix_tokens': 640})
        runner.check_snapshot(good)
        for bad in (dict(good, objects=['miss'] * 32),
                    dict(good, objects=['hit'] * 31),
                    dict(good, summary={'confirmed_prefix_tokens': 639})):
            with self.assertRaises(ValueError):
                runner.check_snapshot(bad)

    def test_stop_requires_exact_exit_and_owns_only_child(self):
        for mode, code in (('clean', 0), ('dirty', -9), ('clean', -15), ('dirty', 0)):
            child = mock.Mock()
            child.poll.side_effect = [None, None, code, code] if mode == 'clean' else [None, code, code]
            child.wait.return_value = code
            receipt = {}
            with self.subTest(mode=mode, code=code):
                if (mode, code) in (('clean', 0), ('dirty', -9)):
                    runner.stop_server(child, mode, receipt)
                else:
                    with self.assertRaises(RuntimeError):
                        runner.stop_server(child, mode, receipt)
                self.assertTrue(receipt['reaped'])
                self.assertEqual(receipt['exit'], code)
                if mode == 'dirty':
                    child.kill.assert_called_once()
                    child.terminate.assert_not_called()
                else:
                    child.terminate.assert_called_once()

    def test_dirty_exit_before_kill_and_unreaped_timeout_fail(self):
        child = mock.Mock()
        child.poll.return_value = 0
        with self.assertRaises(RuntimeError):
            runner.stop_server(child, 'dirty', {})
        child.kill.assert_not_called()
        with self.assertRaises(RuntimeError):
            runner.stop_server(child, 'clean', {})
        child.terminate.assert_not_called()
        child.poll.return_value = None
        child.wait.side_effect = subprocess.TimeoutExpired('owned', 3)
        receipt = {}
        with self.assertRaises(subprocess.TimeoutExpired):
            runner.stop_server(child, 'dirty', receipt)
        self.assertFalse(receipt['reaped'])
        self.assertIsNone(receipt['exit'])

    def test_bounds_reject_before_guest_or_process_mutation(self):
        with mock.patch.object(runner.common, 'guest_paths') as paths, \
                mock.patch.object(runner.subprocess, 'Popen') as spawn:
            for kwargs in ({'cycles': 0}, {'dwell_sec': 61}, {'pressure_rounds': 601},
                           {'pressure_interval_ms': 0}, {'dwell_sec': True}):
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    runner.run('/missing', [], '/unused', 'tag', **kwargs)
            with self.assertRaises(ValueError):
                runner.run('/missing', ['/data/a,b'], '/unused', 'tag')
            paths.assert_not_called()
            spawn.assert_not_called()

    def matrix(self, failure=None, pressure=False):
        stack = ExitStack()
        self.addCleanup(stack.close)
        root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        build = root / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build / name).write_bytes(b'never-executed')
        roots = [root / f'disk{i}' for i in (1, 2, 3)]
        for path in roots:
            path.mkdir()
        output = root / 'out'
        stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
        stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
        stack.enter_context(mock.patch.object(runner.mixed, 'check_initial'))
        stack.enter_context(mock.patch.object(runner.mixed, 'check_startup'))
        restart = stack.enter_context(mock.patch.object(runner, 'check_restart'))
        stack.enter_context(mock.patch.object(runner, 'check_health'))
        stack.enter_context(mock.patch.object(runner, 'check_client_metrics', return_value={'capacity_evictions': 1}))
        stack.enter_context(mock.patch.object(runner.baseline, '_ready', return_value='startup'))
        stack.enter_context(mock.patch.object(runner.baseline, '_scrape', return_value='raw-prom'))
        tools = stack.enter_context(mock.patch.object(runner, '_tools', return_value={'fixture': 'sha'}))
        stack.enter_context(mock.patch.object(runner.workload, 'generate', side_effect=lambda checkpoint=0: {'sha256': f'manifest-{checkpoint}', 'checkpoint': checkpoint}))
        stack.enter_context(mock.patch.object(runner.workload, 'validate'))
        def check(manifest, report, expected=None, expect=None):
            if report['manifest_sha256'] != manifest['sha256']:
                raise ValueError('wrong selected manifest')
            if expected is not None and expected['manifest_sha256'] != manifest['sha256']:
                raise ValueError('wrong snapshot manifest')
            return dict(native=dict(put_ack=0, put_error=0, put_unknown=0, get_hit=0,
                                    get_miss=0, get_error=0, get_unknown=0), put_bytes=0)
        stack.enter_context(mock.patch.object(runner.workload, 'check', side_effect=check))
        sampler = stack.enter_context(mock.patch.object(runner.mixed, 'Sampler'))
        sampler.return_value.rows = []
        children = []

        def spawn(command, **kwargs):
            child = mock.Mock(pid=100 + len(children))
            child.returncode = None
            child.poll.side_effect = lambda: child.returncode
            child.terminate.side_effect = lambda: setattr(child, 'returncode', 0)
            child.kill.side_effect = lambda: setattr(child, 'returncode', -9)
            child.wait.side_effect = lambda timeout: child.returncode
            children.append(child)
            return child

        stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=spawn))
        if failure == 'stop':
            stack.enter_context(mock.patch.object(runner.common, 'stop_owned', side_effect=RuntimeError('stop failed')))

        def client(command, env, log, sampler, *, timeout, receipt):
            receipt.update(state='exited', pid=999, exit=0, reaped=True)
            if failure == 'timeout':
                receipt.update(state='failed_reaped', exit=-9)
                raise TimeoutError('owned client deadline')
            path = Path(command[command.index('--output') + 1])
            path.mkdir()
            mode = command[command.index('--mode') + 1] if '--mode' in command else 'pressure'
            report = dict(mode=mode, passed=True, transport='rdma', objects=['hit'] * 32,
                          summary={'confirmed_prefix_tokens': 640})
            if '--manifest' in command:
                report['manifest_sha256'] = json.loads(Path(command[command.index('--manifest') + 1]).read_text())['sha256']
            if failure == 'wrong_manifest' and mode == 'verify':
                report['manifest_sha256'] = 'wrong-current-cohort'
            if failure == 'partial_snapshot' and mode == 'snapshot':
                report['objects'][0] = 'miss'
            if failure == 'malformed':
                (path / 'report.json').write_text('not-json')
            else:
                if failure == 'transport':
                    report['transport'] = 'fake-rdma-test'
                if failure == 'failed':
                    report['passed'] = False
                (path / 'report.json').write_text(json.dumps(report))
            Path(log).write_text('raw-client-output')
            if failure == 'build':
                (build / 'libdfkv.so').write_bytes(b'mutated')
            if failure == 'tools':
                tools.return_value = {'fixture': 'changed'}
            if failure == 'manifest':
                (output / 'manifest.json').write_text('{}')

        clients = stack.enter_context(mock.patch.object(runner.mixed, 'run_client', side_effect=client))
        if pressure:
            import prefix_concurrent
            stack.enter_context(mock.patch.object(prefix_concurrent, 'check', return_value={'pressure': True, 'native': {}}))
        result = runner.run(build, roots, output, 'tag', cycles=2,
                            pressure_rounds=1 if pressure else 0)
        return result, output, children, clients, restart

    def test_matrix_reuses_case_directories_but_never_between_cases(self):
        result, output, children, clients, restart = self.matrix()
        self.assertTrue(result['passed'])
        self.assertEqual(len(children), 24)
        self.assertEqual(clients.call_count, 136)
        self.assertEqual(restart.call_count, 24)
        self.assertEqual(len({p for row in result['cases'] for p in row['data_dirs']}), 24)
        for row in result['cases']:
            self.assertEqual(row['state'], 'passed')
            for index, stage in enumerate(row['stages']):
                self.assertEqual(stage['command'][2], ','.join(row['data_dirs']))
                expected_exit = -9 if row['stop'] == 'dirty' and index < 2 else 0
                self.assertEqual(stage['server_exit'], expected_exit)
                self.assertTrue(stage['server_reaped'])
                self.assertEqual(stage['state'], 'passed')
                self.assertGreaterEqual(stage['startup_elapsed_sec'], 0)
                self.assertGreaterEqual(stage['elapsed_sec'], stage['startup_elapsed_sec'])
                self.assertTrue(list((output / f"case-{row['case_id']:02d}-{row['stop']}-{row['transition']}" / f"start-{index:03d}-{stage['policy']}").glob('*-before.prom')))
                modes = [client['mode'] for client in stage['clients']]
                self.assertEqual(modes, ['seed', 'refill', 'seed', 'seed', 'snapshot'] if index == 0 else
                                 ['verify', 'refill', 'refill', 'seed', 'seed', 'snapshot'])
                self.assertEqual([c['label'] for c in stage['clients']].count('stabilize'), 1)
                self.assertEqual([c['label'] for c in stage['clients']][-3:],
                                 ['checkpoint-seed', 'checkpoint-stabilize', 'snapshot'])
                previous = output / ('manifest.json' if index == 0 else f'checkpoint-{index:03d}.json')
                current = output / f'checkpoint-{index + 1:03d}.json'
                self.assertEqual(stage['previous_manifest'], str(previous))
                self.assertEqual(stage['current_manifest'], str(current))
                self.assertEqual(stage['previous_manifest_sha256'], runner.baseline.file_sha(previous))
                self.assertEqual(stage['current_manifest_sha256'], runner.baseline.file_sha(current))
                for receipt in stage['clients'][:-3]:
                    self.assertEqual(receipt['manifest'], str(previous))
                for receipt in stage['clients'][-3:]:
                    self.assertEqual(receipt['manifest'], str(current))
                    command = receipt['command']
                    self.assertEqual(command[command.index('--manifest') + 1], str(current))
                if index:
                    command = stage['clients'][0]['command']
                    self.assertEqual(command[command.index('--expect') + 1],
                                     'empty' if row['stop'] == 'dirty' else 'preserve')
                    expected = Path(command[command.index('--expected') + 1])
                    self.assertEqual(expected.parent.name, 'snapshot')
                    self.assertIn('start-' + f'{index - 1:03d}', str(expected))
                    self.assertEqual(command[command.index('--manifest') + 1], str(previous))
                    self.assertNotEqual(str(previous), str(current))
                    self.assertEqual(stage['expected_snapshot_sha256'], runner.baseline.file_sha(expected))
        self.assertEqual(json.loads((output / 'matrix.json').read_text()), result)

    def test_failure_is_fail_stop_preserves_evidence_and_cleanup_truth(self):
        for failure in ('timeout', 'malformed', 'transport', 'failed', 'build', 'tools', 'manifest', 'stop'):
            with self.subTest(failure=failure):
                result, output, children, clients, restart = self.matrix(failure)
                self.assertFalse(result['passed'])
                self.assertEqual(len(children), 1)
                self.assertEqual(result['cases'][0]['state'], 'failed')
                self.assertTrue(all(row['state'] == 'not_attempted' for row in result['cases'][1:]))
                stage = result['cases'][0]['stages'][0]
                self.assertEqual(stage['state'], 'failed')
                self.assertEqual(stage['server_reaped'], failure != 'stop')
                self.assertTrue((output / 'matrix.json').is_file())
                self.assertTrue(list(output.glob('case-*/start-*/startup.prom')))
                self.assertEqual(clients.call_count, 5 if failure == 'stop' else 1)
                self.assertTrue(list(output.glob('case-*/start-*/failure.prom')))

    def test_verify_rejects_new_cohort_instead_of_previous_snapshot_cohort(self):
        result, output, children, clients, restart = self.matrix(failure='wrong_manifest')
        self.assertFalse(result['passed'])
        self.assertEqual(len(children), 2)
        stage = result['cases'][0]['stages'][1]
        self.assertEqual([receipt['mode'] for receipt in stage['clients']], ['verify'])
        self.assertIn('wrong selected manifest', stage['error'])
        self.assertTrue(list(output.glob('case-*/start-001-*/failure.prom')))

    def test_partial_checkpoint_snapshot_fails_without_retry(self):
        result, output, children, clients, restart = self.matrix(failure='partial_snapshot')
        self.assertFalse(result['passed'])
        self.assertEqual(len(children), 1)
        self.assertEqual(clients.call_count, 5)
        stage = result['cases'][0]['stages'][0]
        self.assertEqual([receipt['label'] for receipt in stage['clients']],
                         ['seed', 'stabilize', 'checkpoint-seed', 'checkpoint-stabilize', 'snapshot'])
        self.assertIn('snapshot must preserve all 32', stage['error'])
        self.assertEqual(stage['server_exit'], 0)
        self.assertTrue(list(output.glob('case-*/start-*/failure.prom')))

    def test_optional_pressure_is_bounded_before_snapshot(self):
        result, output, children, clients, restart = self.matrix(pressure=True)
        self.assertTrue(result['passed'])
        self.assertEqual(clients.call_count, 160)
        plans = list(output.glob('pressure-*.json'))
        self.assertEqual(len(plans), 24)
        self.assertEqual(len({json.loads(path.read_text())['config']['seed'] for path in plans}), 24)
        for row in result['cases']:
            for stage in row['stages']:
                self.assertEqual([c['mode'] for c in stage['clients']][-4:],
                                 ['pressure', 'seed', 'seed', 'snapshot'])
                self.assertEqual(stage['clients'][-3]['label'], 'checkpoint-seed')
                self.assertEqual(stage['clients'][-2]['label'], 'checkpoint-stabilize')
                labels = [c.get('label', c['mode']) for c in stage['clients']]
                self.assertEqual(labels.count('stabilize'), 1)
                self.assertLess(labels.index('stabilize'), labels.index('pressure'))
                self.assertNotIn('restore', labels)
                self.assertNotIn('restore-stabilize', labels)
                self.assertEqual(labels.count('checkpoint-seed'), 1)
                self.assertEqual(labels.count('checkpoint-stabilize'), 1)
                self.assertEqual(labels[-4:], ['pressure', 'checkpoint-seed', 'checkpoint-stabilize', 'snapshot'])
                self.assertEqual(stage['clients'][-4]['summary'], {'pressure': True, 'native': {}})


if __name__ == '__main__':
    unittest.main()
