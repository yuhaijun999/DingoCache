"""Pure orchestration and metrics tests; no native service or I/O is started."""

from contextlib import ExitStack
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import run_long_residence as runner


def metrics(policy, dirs, before=False):
    result = []
    for index, path in enumerate(dirs):
        disk = json.dumps(str(path))
        result.append(f'dfkv_disk_objects{{disk={disk}}} {0 if before else 12}')
        for suffix in ('watermark_high_bytes', 'watermark_low_bytes', 'reclaim_interval_ms'):
            result.append(f'dfkv_slab_disk_{suffix}{{disk={disk},disk_index="{index}"}} 0')
    counts = {'dfkv_cache_put_total': 100, 'dfkv_bytes_written_total': 409600,
              'dfkv_rdma_pull_requests_dispatched_total': 20,
              'dfkv_rdma_pull_requests_terminal_total': None,
              'dfkv_uring_reads_total': 17, 'dfkv_uring_completions_total': 17,
              'dfkv_evictions_total': 64,
              'dfkv_slab_lifetime_read_success_total': 17 if policy == 'lifetime' else 0}
    for name, value in counts.items():
        if value is not None:
            result.append(f'{name} {0 if before else value}')
    for name, value in (('success_nonempty', 17), ('not_found', 3), ('io_error', 0)):
        result.append('dfkv_rdma_pull_requests_terminal_total{path="uring",result="'
                      + name + '"} ' + str(0 if before else value))
    for suffix in ('attempts', 'completed', 'errors'):
        for path in ('foreground_evict', 'watermark_evict', 'background_evict'):
            value = 64 if not before and path == 'foreground_evict' and suffix != 'errors' else 0
            result.append(f'dfkv_slab_metadata_ops_{suffix}_total{{path="{path}"}} {value}')
    for name in ('dfkv_slab_watermark_evictions_total', 'dfkv_slab_reclaimed_total',
                 'dfkv_slab_extent_steals_total', 'dfkv_errors_total',
                 'dfkv_uring_init_fallbacks_total', 'dfkv_uring_inflight'):
        result.append(name + ' 0')
    result += ['dfkv_storage_healthy 1', 'dfkv_slab_healthy 1']
    return '\n'.join(result) + '\n'


class LongResidenceRunnerTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.dirs = ['/data/disk1/cases/a', '/data/disk2/cases/a', '/data/disk3/cases/a']
        self.counts = dict(put_acks=100, put_bytes=409600, get_hits=17, get_misses=3,
                           get_attempts=20)

    def test_alternating_plan_and_split_pair_identity(self):
        rows = runner.plan()
        self.assertEqual([row['policy'] for row in rows],
                         ['legacy', 'lifetime', 'lifetime', 'legacy', 'legacy', 'lifetime'])
        self.assertEqual([row['case_id'] for row in rows], list(range(6)))
        self.assertEqual(runner.plan(2, 1), rows[2:])
        for pairs, start in ((True, 0), (0, 0), (4, 0), (2, 2), (1, -1), (1, 3), (1, True)):
            with self.subTest(pairs=pairs, start=start), self.assertRaises(ValueError):
                runner.plan(pairs, start)

    def test_environment_is_isolated_rdma_and_default_scores_not_overridden(self):
        with mock.patch.dict(runner.os.environ, {'DFKV_RDMA': '0',
                'DFKV_SLAB_PROTECT_SEC': 'invalid', 'OTHER': 'kept'}, clear=True):
            accelerated = runner.environment('accelerated')
            default = runner.environment('default')
        self.assertEqual(default['DFKV_RDMA'], '1')
        self.assertEqual(default['DFKV_SLAB_RECLAIM_MS'], '0')
        self.assertEqual(default['DFKV_GET_MISS_RETRIES'], '0')
        self.assertEqual(default['OTHER'], 'kept')
        self.assertNotIn('DFKV_SLAB_PROTECT_SEC', default)
        self.assertEqual(accelerated['DFKV_SLAB_PROTECT_SEC'], '12')
        self.assertEqual(accelerated['DFKV_SLAB_BUCKET_SEC'], '4')
        accelerated['DFKV_RDMA'] = '0'
        self.assertEqual(runner.environment('accelerated')['DFKV_RDMA'], '1')
        with self.assertRaises(ValueError):
            runner.environment('invalid')

    def test_default_environment_discards_all_inherited_lifetime_overrides(self):
        inherited = {name: 'untrusted' for name in runner.ACCELERATED}
        inherited.update(DFKV_RDMA='0', DFKV_RAM_TIER='1',
                         DFKV_PUT_ACK_MODE='ram', DFKV_SERVER_URING='0',
                         DFKV_SLAB_RECLAIM_MS='999', DFKV_SLAB_EVICT_LOW_PCT='88',
                         DFKV_SLAB_EVICT_HIGH_PCT='92', DFKV_UNRELATED='discard',
                         UNRELATED='retain')
        with mock.patch.dict(runner.os.environ, inherited, clear=True):
            env = runner.environment('default')
        self.assertTrue(set(runner.ACCELERATED).isdisjoint(env))
        self.assertNotIn('DFKV_UNRELATED', env)
        self.assertEqual(env['UNRELATED'], 'retain')
        self.assertEqual({name: env[name] for name in (
            'DFKV_RDMA', 'DFKV_RAM_TIER', 'DFKV_PUT_ACK_MODE',
            'DFKV_SERVER_URING', 'DFKV_SLAB_RECLAIM_MS',
            'DFKV_SLAB_EVICT_LOW_PCT', 'DFKV_SLAB_EVICT_HIGH_PCT')}, {
                'DFKV_RDMA': '1', 'DFKV_RAM_TIER': '0', 'DFKV_PUT_ACK_MODE': 'disk',
                'DFKV_SERVER_URING': '1', 'DFKV_SLAB_RECLAIM_MS': '0',
                'DFKV_SLAB_EVICT_LOW_PCT': '0', 'DFKV_SLAB_EVICT_HIGH_PCT': '0'})

    def test_default_startup_requires_every_unscaled_score_and_inactive_legacy(self):
        scores = {
            'DFKV_SLAB_LIFE_BASE_SEC': '604800',
            'DFKV_SLAB_LIFE_READ_BONUS_SEC': '302400',
            'DFKV_SLAB_LIFE_MAX_READS': '2', 'DFKV_SLAB_PROTECT_SEC': '180',
            'DFKV_SLAB_PROTECT_SLOWDOWN': '4',
            'DFKV_SLAB_PROTECT_BOOST_SEC': '1209600',
            'DFKV_SLAB_KEEP_SCORE_SEC': '475200',
            'DFKV_SLAB_BUCKET_SEC': '43200', 'DFKV_SLAB_BUCKET_COUNT': '24'}
        common = {'DFKV_PUT_ACK_MODE': 'disk', 'DFKV_RAM_TIER': 'off',
                  'DFKV_SERVER_URING': 'on', 'DFKV_SLAB_RECLAIM_MS': '0',
                  'DFKV_SLAB_EVICT_LOW_PCT': '0', 'DFKV_SLAB_EVICT_HIGH_PCT': '0'}
        for policy in ('legacy', 'lifetime'):
            values = dict(common, disk_eviction_requested=policy,
                          disk_eviction_effective=policy)
            values.update({name: value if policy == 'lifetime' else 'inactive'
                           for name, value in scores.items()})
            text = '\n'.join(name + ' = ' + value + ' (default)'
                             for name, value in values.items())
            runner.check_startup(text, 'default', policy)
            for name in scores:
                altered = text.replace(name + ' = ' + values[name], name + ' = 999')
                with self.subTest(policy=policy, name=name), self.assertRaises(ValueError):
                    runner.check_startup(altered, 'default', policy)

    def test_accelerated_score_relationships_and_horizon(self):
        env = runner.ACCELERATED
        base, bonus, cap, protect, slow, boost, keep, width, buckets = [int(env[name]) for name in
            ('DFKV_SLAB_LIFE_BASE_SEC', 'DFKV_SLAB_LIFE_READ_BONUS_SEC', 'DFKV_SLAB_LIFE_MAX_READS',
             'DFKV_SLAB_PROTECT_SEC', 'DFKV_SLAB_PROTECT_SLOWDOWN', 'DFKV_SLAB_PROTECT_BOOST_SEC',
             'DFKV_SLAB_KEEP_SCORE_SEC', 'DFKV_SLAB_BUCKET_SEC', 'DFKV_SLAB_BUCKET_COUNT')]
        self.assertGreater(base - (protect - 1) // slow + boost, base + bonus * cap)
        horizon = max(protect, base + bonus * cap - keep + protect - protect // slow) + width
        self.assertEqual(horizon, 86)
        self.assertGreaterEqual(width * buckets, horizon)
        self.assertLess(keep, base)

    def test_metric_oracle_checks_full_counts_and_foreground_evictions(self):
        for policy in ('legacy', 'lifetime'):
            with self.subTest(policy=policy):
                value = runner.check_metrics(metrics(policy, self.dirs, True),
                    metrics(policy, self.dirs), policy, self.counts, self.dirs)
                self.assertEqual(value['class_foreground_evictions'], 64)
                self.assertEqual(value['dfkv_uring_reads_total'], 17)
        with self.assertRaises(ValueError):
            runner.check_metrics('', '', 'unknown', self.counts, self.dirs)

    def test_metric_oracle_rejects_fallback_wrong_path_denominator_and_unsettled(self):
        before, after = metrics('lifetime', self.dirs, True), metrics('lifetime', self.dirs)
        replacements = [('dfkv_bytes_written_total 409600', 'dfkv_bytes_written_total 409599'),
                        ('dfkv_uring_reads_total 17', 'dfkv_uring_reads_total 16'),
                        ('dfkv_slab_lifetime_read_success_total 17', 'dfkv_slab_lifetime_read_success_total 16'),
                        ('path="uring"', 'path="sync"'), ('result="io_error"} 0', 'result="io_error"} 1'),
                        ('dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_init_fallbacks_total 1'),
                        ('dfkv_uring_inflight 0', 'dfkv_uring_inflight 1'),
                        ('dfkv_slab_healthy 1', 'dfkv_slab_healthy 0'),
                        ('dfkv_slab_watermark_evictions_total 0', 'dfkv_slab_watermark_evictions_total 1'),
                        ('dfkv_evictions_total 64', 'dfkv_evictions_total 0')]
        for old, new in replacements:
            with self.subTest(new=new), self.assertRaises((ValueError, RuntimeError)):
                runner.check_metrics(before, after.replace(old, new), 'lifetime', self.counts, self.dirs)

    def test_duplicate_terminal_labels_rejected_even_when_zero(self):
        before = metrics('legacy', self.dirs, True)
        after = metrics('legacy', self.dirs)
        duplicate = 'dfkv_rdma_pull_requests_terminal_total{path="uring",result="io_error"} 0\n'
        with self.assertRaisesRegex(ValueError, 'duplicate PULL'):
            runner.check_metrics(before, after + duplicate, 'legacy', self.counts, self.dirs)

    def test_duplicate_final_disk_series_is_rejected(self):
        before = metrics('legacy', self.dirs, True)
        after = metrics('legacy', self.dirs)
        duplicate = 'dfkv_disk_objects{disk="/data/disk1/cases/a"} 12\n'
        with self.assertRaises((ValueError, RuntimeError)):
            runner.check_metrics(before, after + duplicate, 'legacy', self.counts, self.dirs)

    def test_initial_state_requires_empty_exact_owned_disks_and_zero_activity(self):
        before = metrics('legacy', self.dirs, True)
        runner.check_initial(before, self.dirs)
        for old, new in (('dfkv_cache_put_total 0', 'dfkv_cache_put_total 1'),
                         ('disk1/cases/a', 'disk1/cases/foreign'),
                         ('dfkv_disk_objects{disk="/data/disk1/cases/a"} 0',
                          'dfkv_disk_objects{disk="/data/disk1/cases/a"} 1')):
            with self.subTest(new=new), self.assertRaises((ValueError, RuntimeError)):
                runner.check_initial(before.replace(old, new), self.dirs)

    def test_startup_reads_back_policy_and_all_effective_score_fields(self):
        values = dict(runner.ACCELERATED, DFKV_PUT_ACK_MODE='disk', DFKV_RAM_TIER='off',
                      DFKV_SERVER_URING='on', DFKV_SLAB_RECLAIM_MS='0',
                      DFKV_SLAB_EVICT_LOW_PCT='0', DFKV_SLAB_EVICT_HIGH_PCT='0',
                      disk_eviction_requested='lifetime', disk_eviction_effective='lifetime')
        text = '\n'.join(name + ' = ' + value + ' (env)' for name, value in values.items())
        runner.check_startup(text, 'accelerated', 'lifetime')
        for changed in ('DFKV_SLAB_BUCKET_SEC = 5', 'DFKV_PUT_ACK_MODE = ram',
                        'disk_eviction_effective = legacy'):
            name = changed.split()[0]
            bad = text.replace(name + ' = ' + values[name], changed)
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                runner.check_startup(bad, 'accelerated', 'lifetime')
        with self.assertRaises(ValueError):
            runner.check_startup(text + '\nDFKV_RAM_TIER = off (env)', 'accelerated', 'lifetime')
        with self.assertRaises(ValueError):
            runner.check_startup(text, 'default', 'lifetime')
        legacy = '\n'.join(name + ' = ' + ('inactive' if name in runner.ACCELERATED else
                          'legacy' if name.startswith('disk_eviction_') else value) + ' (env)'
                          for name, value in values.items())
        runner.check_startup(legacy, 'accelerated', 'legacy')
        configured = legacy.replace('inactive (env)', "inactive (configured='60') (env)")
        runner.check_startup(configured, 'accelerated', 'legacy')

    def test_raw_pressure_scrapes_bind_counters_ownership_and_hashes(self):
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        samples = [{'file': f'metrics-{index:03d}.prom', 'started_monotonic_sec': index + 1,
                    'finished_monotonic_sec': index + 1.2} for index in range(2)]
        index = root / 'metrics-samples.json'
        (root / 'metrics-000.prom').write_text(metrics('legacy', self.dirs, True))
        last = root / 'metrics-001.prom'
        last.write_text(metrics('legacy', self.dirs))
        for sample in samples:
            sample['sha256'] = runner.baseline.file_sha(root / sample['file'])
        index.write_text(json.dumps(samples))
        report = {'pressure_samples': [{'counter': 0}, {'counter': 64}]}
        hashes = runner.check_pressure_samples(root, report, self.dirs)
        self.assertEqual(set(hashes), {'metrics-samples.json', 'metrics-000.prom', 'metrics-001.prom'})
        samples[1]['sha256'] = '0' * 64
        index.write_text(json.dumps(samples))
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            runner.check_pressure_samples(root, report, self.dirs)
        samples[1]['sha256'] = runner.baseline.file_sha(last)
        index.write_text(json.dumps(samples))
        report['pressure_samples'][1]['counter'] = 63
        with self.assertRaisesRegex(ValueError, 'counter differs'):
            runner.check_pressure_samples(root, report, self.dirs)
        report['pressure_samples'][1]['counter'] = 64
        last.write_text(metrics('legacy', self.dirs).replace('disk1/cases/a', 'disk1/cases/foreign'))
        with self.assertRaises((ValueError, RuntimeError)):
            runner.check_pressure_samples(root, report, self.dirs)

    def test_raw_pressure_denominator_paths_and_times_cannot_be_forged(self):
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        index = root / 'metrics-samples.json'
        report = {'pressure_samples': [{'counter': 0}]}
        for samples in ([], [{'file': '../metrics-000.prom'}],
                        [{'file': 'metrics-000.prom', 'started_monotonic_sec': 2,
                          'finished_monotonic_sec': 1}]):
            index.write_text(json.dumps(samples))
            with self.subTest(samples=samples), self.assertRaises(ValueError):
                runner.check_pressure_samples(root, report, self.dirs)

    def _matrix(self, *, fail_child=False, bad_report=False, ready_fail=False,
                mutate_build=False, mutate_tools=False, stop_fail=False, profile='accelerated', hot_refill=False):
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        build = root / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build / name).write_bytes(b'fixture-never-executed')
        roots = [root / f'disk{i}' for i in (1, 2, 3)]
        for disk in roots:
            disk.mkdir()
        output = root / 'results'
        self.stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
        self.stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
        self.stack.enter_context(mock.patch.object(runner, 'check_pressure_samples', return_value={'raw': 'sha'}))
        self.stack.enter_context(mock.patch.object(runner, 'check_initial'))
        self.stack.enter_context(mock.patch.object(runner, 'check_startup'))
        self.stack.enter_context(mock.patch.object(runner, 'check_metrics', return_value={'evictions': 1}))
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
            expected_profile = runner.workload.profile(profile)
            self.assertEqual(kwargs['timeout'], expected_profile['steps'] * expected_profile['interval_sec']
                             + expected_profile['preage_sec'] + 120)
            path = Path(command[command.index('--output') + 1])
            path.mkdir()
            (path / 'report.json').write_text('{}')
            (path.parent / 'server.log').touch()
            if fail_child:
                raise runner.subprocess.TimeoutExpired(command, kwargs['timeout'])
            if mutate_build:
                (build / 'libdfkv.so').write_bytes(b'changed')
            if mutate_tools:
                tools.return_value = {'fixture': 'changed'}
        child_run = self.stack.enter_context(mock.patch.object(runner.subprocess, 'run', side_effect=child))
        audit = self.stack.enter_context(mock.patch.object(runner.workload, 'check_report', return_value=self.counts))
        if bad_report:
            audit.side_effect = ValueError('controlled bad denominator')
        if stop_fail:
            original = runner.common.stop_owned
            def stop(server):
                original(server)
                raise RuntimeError('controlled stop error')
            self.stack.enter_context(mock.patch.object(runner.common, 'stop_owned', side_effect=stop))
        result = runner.run(build, roots, output, 'tag', pairs=1, profile=profile, hot_refill=hot_refill)
        return result, output, processes, child_run

    def test_matrix_fresh_paths_rdma_bounded_child_and_normal_reaping(self):
        result, output, processes, child = self._matrix()
        self.assertTrue(result['passed'])
        self.assertFalse(result['formal_ab'])
        self.assertEqual(result['gpu'], 'excluded')
        self.assertEqual(result['planned_per_case'],
                         runner.workload.expected_counts(runner.workload.profile('accelerated')))
        self.assertEqual(child.call_count, 2)
        self.assertTrue((output / 'matrix.json').is_file())
        self.assertEqual(len({path for row in result['cases'] for path in row['data_dirs']}), 6)
        for row in result['cases']:
            self.assertEqual(row['state'], 'passed')
            self.assertTrue(row['server_reaped'])
            self.assertEqual(row['server_exit'], 0)
            self.assertEqual(row['environment']['DFKV_PUT_ACK_MODE'], 'disk')
        for process in processes:
            process.terminate.assert_called_once()

    def test_hot_refill_mode_is_manifest_bound_and_forwarded_without_changing_server(self):
        result, output, processes, child = self._matrix(hot_refill=True, profile='default')
        self.assertTrue(result['passed'])
        self.assertIs(result['profile']['hot_refill'], True)
        self.assertIs(json.loads((output / 'matrix.json').read_text())['profile']['hot_refill'], True)
        self.assertEqual(result['planned_per_case'],
                         runner.workload.expected_counts(runner.workload.profile('default')))
        self.assertEqual(child.call_count, 2)
        for call, row in zip(child.call_args_list, result['cases']):
            self.assertIn('--hot-refill', call.args[0])
            self.assertNotIn('--hot-refill', row['command'])
            self.assertEqual(row['client_timeout_sec'], 2160)
            self.assertTrue(row['server_reaped'])
        self.assertEqual(len(processes), 2)

    def test_default_matrix_keeps_full_window_and_2160_second_child_budget(self):
        result, _, processes, child = self._matrix(profile='default')
        self.assertTrue(result['passed'])
        self.assertEqual(result['profile'], runner.workload.profile('default'))
        self.assertEqual(result['planned_per_case']['arrival_puts'], 230400)
        self.assertEqual(result['planned_per_case']['hot_gets'], 115200)
        self.assertEqual(child.call_count, 2)
        self.assertEqual(len(processes), 2)
        for call, row in zip(child.call_args_list, result['cases']):
            self.assertEqual(call.kwargs['timeout'], 2160)
            self.assertEqual(row['client_timeout_sec'], 2160)
            server = row['command']
            self.assertEqual(server[server.index('--cap') + 1], '12884901888')
            self.assertEqual(len(row['data_dirs']), 3)
            command = call.args[0]
            self.assertEqual(command[command.index('--profile') + 1], 'default')
            self.assertNotIn('--hot-refill', command)
            self.assertTrue(set(runner.ACCELERATED).isdisjoint(row['environment']))
            self.assertTrue(row['server_reaped'])
            self.assertEqual(row['server_exit'], 0)

    def test_failure_is_failstop_reported_and_server_reaped(self):
        for option in ('fail_child', 'bad_report', 'stop_fail'):
            with self.subTest(option=option):
                result, output, processes, child = self._matrix(**{option: True})
                self.assertFalse(result['passed'])
                self.assertTrue(result['errors'])
                self.assertEqual([row['state'] for row in result['cases']], ['failed', 'not_attempted'])
                self.assertEqual(child.call_count, 1)
                self.assertEqual(len(processes), 1)
                self.assertTrue(result['cases'][0]['server_reaped'])
                if option == 'fail_child':
                    self.assertEqual(result['cases'][0]['client_state'], 'process_timeout_reaped')
                    self.assertIsNone(result['cases'][0]['counts'])
                self.assertTrue((output / 'matrix.json').is_file())

    def test_readiness_failure_never_starts_client_and_reaps_server(self):
        result, _, processes, child = self._matrix(ready_fail=True)
        self.assertFalse(result['passed'])
        self.assertEqual(child.call_count, 0)
        self.assertEqual(len(processes), 1)
        self.assertTrue(result['cases'][0]['server_reaped'])

    def test_changed_binary_or_source_stops_before_second_case(self):
        for option in ('mutate_build', 'mutate_tools'):
            with self.subTest(option=option):
                result, _, processes, child = self._matrix(**{option: True})
                self.assertFalse(result['passed'])
                self.assertIn('frozen tools changed', result['errors'][0])
                self.assertEqual(len(processes), 1)
                self.assertEqual(child.call_count, 1)
                self.assertEqual(result['cases'][1]['state'], 'not_attempted')

    def test_bad_tag_path_or_selection_rejected_before_guest_or_spawn(self):
        with mock.patch.object(runner.common, 'guest_paths') as guest, \
                mock.patch.object(runner.subprocess, 'Popen') as spawn:
            for tag in ('', '../a', 'a' * 41):
                with self.subTest(tag=tag), self.assertRaises(ValueError):
                    runner.run('/absent', [], '/unused', tag)
            with self.assertRaises(ValueError):
                runner.run('/absent', ['/data/a,b'], '/unused', 'tag')
            with self.assertRaises(ValueError):
                runner.run('/absent', [], '/unused', 'tag', pairs=3, start_pair=1)
            guest.assert_not_called()
            spawn.assert_not_called()


if __name__ == '__main__':
    unittest.main()
