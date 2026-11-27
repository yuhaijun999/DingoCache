"""Boundary runner contracts using fake owned processes; no native service."""
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_boundary_validation as runner


def startup(policy, route, reads):
    env = runner.environment(route, policy, reads)
    values = dict(DFKV_PUT_ACK_MODE='disk', DFKV_RAM_TIER='off', DFKV_SERVER_URING='on',
                  disk_eviction_requested=policy, disk_eviction_effective=policy)
    for name in ('DFKV_SLAB_RECLAIM_MS', 'DFKV_SLAB_EVICT_LOW_PCT', 'DFKV_SLAB_EVICT_HIGH_PCT'):
        values[name] = env[name]
    defaults = dict(DFKV_SLAB_LIFE_BASE_SEC='604800', DFKV_SLAB_LIFE_READ_BONUS_SEC='302400',
                    DFKV_SLAB_LIFE_MAX_READS=str(reads), DFKV_SLAB_PROTECT_SEC='180',
                    DFKV_SLAB_PROTECT_SLOWDOWN='4',
                    DFKV_SLAB_PROTECT_BOOST_SEC=str(604800 + 302400 * (reads or 0)),
                    DFKV_SLAB_KEEP_SCORE_SEC='475200', DFKV_SLAB_BUCKET_SEC='400000',
                    DFKV_SLAB_BUCKET_COUNT='64')
    values.update({key: value if policy == 'lifetime' else 'inactive' for key, value in defaults.items()})
    return '\n'.join(key + ' = ' + value + ' (fixture)' for key, value in values.items())


class BoundaryValidationTest(unittest.TestCase):
    def test_matrix_five_configs_two_routes_with_fresh_identity(self):
        rows = runner.plan()
        self.assertEqual(len(rows), 10)
        self.assertEqual([row['max_reads'] for row in rows[:5]], [None, 1, 2, 4, 63])
        self.assertEqual(rows[4]['configured_bands'], 64)
        self.assertEqual(rows[4]['band_count_scope'], 'configuration-derived-not-measured-allocation')
        self.assertTrue(all(row['state'] == 'not_attempted' for row in rows))
        self.assertEqual(len(runner.plan(('watermark',), (63,), False)), 1)

    def test_matrix_invalid_input_before_io(self):
        for args in [((), (1,), True), (('bad',), (1,), True),
                     (('watermark',) * 2, (1,), True), (('watermark',), (), True),
                     (('watermark',), (True,), True), (('watermark',), (0,), True),
                     (('watermark',), (64,), True), (('watermark',), (1, 1), True),
                     (('watermark',), (1,), 1)]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                runner.plan(*args)

    def test_coupled_bucket_geometry_all_values_and_derived_boost(self):
        for m in (1, 2, 4, 63):
            env = runner.environment('class-pressure', 'lifetime', m)
            self.assertEqual(env['DFKV_SLAB_BUCKET_SEC'], '400000')
            self.assertEqual(env['DFKV_SLAB_BUCKET_COUNT'], '64')
            self.assertEqual(env['DFKV_SLAB_LIFE_MAX_READS'], str(m))
            self.assertNotIn('DFKV_SLAB_PROTECT_BOOST_SEC', env)
            # Mirror the documented arithmetic only, not a replacement for the
            # native validator: B*G >= max(P,Lmax-KEEP+P-floor(P/SLOW))+G.
            horizon = max(180, 604800 + 302400*m - 475200 + 180 - 180//4) + 400000
            self.assertGreaterEqual(64 * 400000, horizon)
        default_m4_horizon = 604800 + 302400*4 - 475200 + 135 + 43200
        self.assertGreater(default_m4_horizon, 24 * 43200)

    def test_environment_clears_inherited_tuning(self):
        with mock.patch.dict(runner.os.environ, {'DFKV_SLAB_PROTECT_SEC': '1'}, clear=True):
            env = runner.environment('watermark', 'lifetime', 63)
        self.assertNotIn('DFKV_SLAB_PROTECT_SEC', env)
        self.assertEqual(env['DFKV_RDMA'], '1')
        self.assertEqual(env['DFKV_PUT_ACK_MODE'], 'disk')
        for policy, m in [('bad', 2), ('legacy', 2), ('lifetime', None), ('lifetime', True)]:
            with self.subTest(policy=policy, m=m), self.assertRaises(ValueError):
                runner.environment('watermark', policy, m)

    def test_exact_effective_startup_before_canonical_oracle(self):
        for route in runner.mixed.ROUTES:
            for policy, reads in [('legacy', None)] + [('lifetime', m) for m in (1, 2, 4, 63)]:
                text = startup(policy, route, reads)
                runner.check_startup(text, policy, route, reads)
                for key in ('DFKV_SLAB_LIFE_MAX_READS', 'DFKV_SLAB_BUCKET_SEC',
                            'DFKV_SLAB_BUCKET_COUNT', 'DFKV_SLAB_PROTECT_BOOST_SEC',
                            'DFKV_SLAB_PROTECT_SEC'):
                    bad = '\n'.join(line.replace(' = ', ' = WRONG', 1) if line.startswith(key+' = ')
                                    else line for line in text.splitlines())
                    with self.subTest(route=route, policy=policy, reads=reads, key=key), self.assertRaises(ValueError):
                        runner.check_startup(bad, policy, route, reads)
                line = next(line for line in text.splitlines() if line.startswith('DFKV_SLAB_BUCKET_SEC = '))
                with self.assertRaises(ValueError):
                    runner.check_startup(text+'\n'+line, policy, route, reads)

    def test_invalid_run_arguments_never_touch_guest(self):
        with mock.patch.object(runner.common, 'guest_paths') as paths:
            for kwargs in [dict(preset='bad'), dict(steps=True), dict(steps=0), dict(steps=1801)]:
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    runner.run('/missing', [], '/unused', 'tag', **kwargs)
            with self.assertRaises(ValueError):
                runner.run('/missing', ['/data/a,b'], '/unused', 'tag')
            paths.assert_not_called()

    def _run(self, failure=None, preset='smoke', steps=8):
        stack = ExitStack()
        self.addCleanup(stack.close)
        root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        build = root/'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build/name).write_bytes(b'fixture-not-native')
        roots = [root/f'disk{i}' for i in (1, 2, 3)]
        for path in roots:
            path.mkdir()
        output = root/'out'
        stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
        stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
        stack.enter_context(mock.patch.object(runner.mixed, 'check_initial'))
        stack.enter_context(mock.patch.object(runner, 'check_startup'))
        stack.enter_context(mock.patch.object(runner.mixed, 'check_metrics', return_value={'evictions': 3}))
        stack.enter_context(mock.patch.object(runner.mixed, 'put_classes', return_value={1: 1, 2: 1}))
        stack.enter_context(mock.patch.object(runner.baseline, '_ready', return_value='before'))
        stack.enter_context(mock.patch.object(runner.baseline, '_scrape', return_value='after'))
        stack.enter_context(mock.patch.object(runner.workload, 'check', return_value={'native': {}}))
        stack.enter_context(mock.patch.object(runner, '_tools', return_value={'fixture': 'same'}))
        sample = stack.enter_context(mock.patch.object(runner, 'Sampler'))
        sample.return_value.rows = [{'role': 'fixture'}]
        sample.return_value.metrics.rows = [1]
        stack.enter_context(mock.patch.object(runner.resources, 'summarize', return_value={'scope': 'sampled', 'roles': {'server': [1], 'client': [] if failure == 'resources' else [1]}}))
        processes = []
        def spawn(command, **kwargs):
            child = mock.Mock(pid=100+len(processes), returncode=None)
            child.poll.side_effect = lambda: child.returncode
            def wait(timeout):
                child.returncode = 0
                return 0
            child.wait.side_effect = wait
            processes.append(child)
            return child
        stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=spawn))
        def client(command, env, log_path, sampler, *, timeout, receipt):
            self.assertTrue((output/'frozen-matrix.json').is_file())
            frozen = json.loads((output/'frozen-matrix.json').read_text())
            self.assertTrue(all(row['state'] == 'not_attempted' for row in frozen['cases']))
            receipt.update(pid=201, state='exited', exit=0, reaped=True)
            out = Path(command[command.index('--output')+1])
            out.mkdir()
            (out/'report.json').write_text(json.dumps({'passed': True, 'transport': 'rdma'}))
            if failure == 'timeout':
                raise TimeoutError('fixture timeout')
            if failure == 'binary':
                (build/'libdfkv.so').write_bytes(b'changed')
            if failure == 'matrix':
                (output/'frozen-matrix.json').write_text('{}')
            if failure == 'transport':
                (out/'report.json').write_text(json.dumps({'passed': True, 'transport': 'fake'}))
            if failure == 'early-exit':
                processes[-1].returncode = 0
        clients = stack.enter_context(mock.patch.object(runner.mixed, 'run_client', side_effect=client))
        result = runner.run(build, roots, output, 'tag', steps=steps, preset=preset)
        return result, output, processes, clients

    def test_complete_matrix_freeze_native_parameters_and_clean_reap(self):
        result, output, processes, clients = self._run()
        self.assertTrue(result['passed'])
        self.assertFalse(result['formal_ab'])
        self.assertEqual(clients.call_count, 10)
        self.assertEqual(result['delays_sec'], [1, 2, 3])
        self.assertEqual(len({path for row in result['cases'] for path in row['data_dirs']}), 30)
        for row, child in zip(result['cases'], processes):
            self.assertEqual(row['state'], 'passed')
            self.assertTrue(row['server_reaped'])
            self.assertEqual(row['server_exit'], 0)
            self.assertEqual(row['environment']['DFKV_SLAB_BUCKET_COUNT'], '64')
            self.assertEqual(row['client_timeout_sec'], 308)
            child.terminate.assert_called_once()
        self.assertEqual(json.loads((output/'matrix.json').read_text()), result)

    def test_fail_stop_keeps_first_evidence_and_remaining_not_attempted(self):
        for failure in ('timeout', 'binary', 'matrix', 'transport', 'early-exit', 'resources'):
            with self.subTest(failure=failure):
                result, output, processes, clients = self._run(failure)
                self.assertFalse(result['passed'])
                self.assertEqual(clients.call_count, 1)
                self.assertEqual(result['cases'][0]['state'], 'failed')
                self.assertTrue(all(row['state'] == 'not_attempted' for row in result['cases'][1:]))
                self.assertTrue(result['cases'][0]['server_reaped'])
                self.assertTrue((output/'case-00-legacy/client/report.json').is_file())

    def test_long_preset_preserves_real_delay_and_scoring_windows(self):
        result, output, processes, clients = self._run(preset='long', steps=None)
        self.assertTrue(result['passed'])
        self.assertEqual(result['steps'], 1800)
        self.assertEqual(result['delays_sec'], [45, 185, 600])
        document = json.loads((output/'plan.json').read_text())
        self.assertEqual(document['config']['max_lateness_sec'], 2)
        for row in result['cases']:
            self.assertEqual(row['client_timeout_sec'], 2100)
            self.assertNotIn('DFKV_SLAB_PROTECT_SEC', row['environment'])
            self.assertNotIn('DFKV_SLAB_PROTECT_BOOST_SEC', row['environment'])
        self.assertEqual(result['frozen_matrix_sha256'], runner.baseline.file_sha(output/'frozen-matrix.json'))

    def test_sampler_live_roles_and_no_exited_client_fabrication(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(runner.mixed, 'Sampler') as metrics, \
                mock.patch.object(runner.resources, 'ProcessSampler') as process:
            process.side_effect = [SimpleNamespace(sample=lambda: {'role': 'server'}),
                                   SimpleNamespace(sample=lambda: {'role': 'client'})]
            receipt = {'state': 'running', 'pid': 12}
            sampler = runner.Sampler(tmp, 11, receipt)
            sampler.sample()
            receipt['state'] = 'exited'
            sampler.sample()
            self.assertEqual(sampler.rows, [{'role': 'server'}, {'role': 'client'}, {'role': 'server'}])
            self.assertEqual(process.call_args_list, [mock.call(11, 'server'), mock.call(12, 'client')])
            self.assertEqual(metrics.return_value.sample.call_count, 2)
            self.assertEqual(json.loads((Path(tmp)/'resource-samples.json').read_text()), sampler.rows)


if __name__ == '__main__':
    unittest.main()
