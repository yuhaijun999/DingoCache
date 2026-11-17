"""Hardware-free checks for the bounded eviction supervisor, not RDMA proof."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'quick_eviction', ROOT / 'deploy/validation/quick_eviction.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def good_client():
    return {'status': 'passed', 'wrong_bytes': 0, 'transport_mode': ['rdma', 'rdma'],
            'hits': 12000, 'misses': 904, 'hot_hits': 128, 'hot_misses': 7424, 'hot_reads': 7552}


def good_metrics():
    return '\n'.join((
        'dfkv_storage_healthy 1', 'dfkv_slab_healthy 1', 'dfkv_errors_total 0',
        'dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_inflight 0',
        'dfkv_evictions_total 904', 'dfkv_uring_reads_total 12128',
        'dfkv_slab_watermark_evictions_total 900', 'dfkv_slab_reclaimed_total 4',
        'dfkv_slab_extent_steals_total 0',
        'dfkv_slab_lifetime_read_success_total 12128',
        'dfkv_slab_lifetime_read_applied_total 12128',
        'dfkv_uring_completions_total 12128',
        'dfkv_disk_objects{disk="a"} 1', 'dfkv_disk_objects{disk="b"} 1',
        'dfkv_disk_objects{disk="c"} 1',
        'dfkv_rdma_pull_requests_dispatched_total 20456',
        'dfkv_rdma_pull_requests_terminal_total{path="async",result="success_nonempty"} 12128',
        'dfkv_rdma_pull_requests_terminal_total{path="async",result="not_found"} 8328',
        'dfkv_rdma_pull_requests_terminal_total{path="sync",result="io_error"} 0')) + '\n'


def sustained_client():
    return {**good_client(), 'hits': 14712, 'misses': 904,
            'hot_hits': 12928, 'hot_misses': 0, 'hot_reads': 12928,
            'pressure_schedule_origin_sec': 3.0, 'pressure_schedule_elapsed_sec': 199.02,
            'steps': [{'step': index, 'start_sec': index + .001, 'end_sec': index + .01,
                       'cold_written': 32, 'hot_hits': 64, 'hot_misses': 0}
                      for index in range(200)]}


def sustained_metrics():
    return good_metrics().replace('12128', '27640').replace('20456', '28544').replace('8328', '904')


class QuickEvictionTest(unittest.TestCase):
    def test_metric_presence_finiteness_and_label_sums(self):
        self.assertEqual(runner.total('counter{disk="a"} 2\ncounter{disk="b"} 3', 'counter'), 5)
        for text in ('', '# counter 0', 'other 0', 'counter NaN', 'counter inf',
                     'counter -1', 'counter invalid'):
            with self.subTest(text=text), self.assertRaises((RuntimeError, ValueError)):
                runner.total(text, 'counter')

    def test_real_activity_and_read_denominators_required(self):
        checked = runner.check_result(good_metrics(), good_client())
        self.assertEqual(checked['evictions'], 904)
        changes = (
            ('dfkv_evictions_total 904', 'dfkv_evictions_total 0'),
            ('dfkv_slab_lifetime_read_success_total 12128', 'dfkv_slab_lifetime_read_success_total 12127'),
            ('dfkv_slab_lifetime_read_applied_total 12128', 'dfkv_slab_lifetime_read_applied_total 12129'),
            ('dfkv_storage_healthy 1', 'dfkv_storage_healthy 0'),
            ('dfkv_slab_healthy 1', 'dfkv_slab_healthy 0'),
            ('dfkv_errors_total 0', 'dfkv_errors_total 1'),
            ('dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_init_fallbacks_total 1'),
            ('dfkv_uring_inflight 0', 'dfkv_uring_inflight 1'),
            ('dfkv_uring_reads_total 12128', 'dfkv_uring_reads_total 0'),
            ('dfkv_uring_completions_total 12128', 'dfkv_uring_completions_total 0'),
            ('dfkv_rdma_pull_requests_dispatched_total 20456',
             'dfkv_rdma_pull_requests_dispatched_total 20457'),
            ('result="io_error"} 0', 'result="io_error"} 1'),
            ('result="not_found"} 8328', 'result="not_found"} 8327'),
            ('result="success_nonempty"} 12128', 'result="success_nonempty"} 12127'),
            ('dfkv_disk_objects{disk="c"} 1', 'dfkv_disk_objects{disk="c"} 0'),
            ('dfkv_disk_objects{disk="c"} 1', 'dfkv_disk_objects{disk="a"} 1'),
        )
        for before, after in changes:
            with self.subTest(after=after), self.assertRaises(RuntimeError):
                runner.check_result(good_metrics().replace(before, after), good_client())
        with self.assertRaisesRegex(RuntimeError, 'missing metric'):
            runner.check_result(good_metrics().replace('dfkv_errors_total 0\n', ''), good_client())

    def test_pressure_can_lose_original_resident_before_heat_notification(self):
        metrics = good_metrics().replace('dfkv_slab_lifetime_read_applied_total 12128',
                                         'dfkv_slab_lifetime_read_applied_total 12127')
        result = runner.check_result(metrics, good_client())
        self.assertEqual(result['read_success'], 12128)
        self.assertEqual(result['read_applied'], 12127)

    def test_legacy_does_not_require_lifetime_heat_but_paths_must_exist(self):
        metrics = '\n'.join(line for line in good_metrics().splitlines()
                            if not line.startswith('dfkv_slab_lifetime_read_'))
        self.assertEqual(runner.check_result(metrics, good_client(), 'legacy')['watermark_evictions'], 900)
        with self.assertRaisesRegex(RuntimeError, 'missing metric'):
            runner.check_result(metrics.replace('dfkv_slab_extent_steals_total 0', ''),
                                good_client(), 'legacy')

    def test_client_failure_wrong_bytes_transport_and_counts_rejected(self):
        for field, value in (('status', 'failed'), ('wrong_bytes', 1),
                             ('transport_mode', ['tcp', 'tcp']), ('hits', 11999),
                             ('hot_reads', 129), ('hot_hits', -1), ('misses', True)):
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                runner.check_result(good_metrics(), {**good_client(), field: value})

    def test_fixed_hot_probe_count_required_even_if_counters_reconcile(self):
        client = {**good_client(), 'hot_misses': 7423, 'hot_reads': 7551}
        metrics = good_metrics().replace('dispatched_total 20456', 'dispatched_total 20455')
        metrics = metrics.replace('result="not_found"} 8328', 'result="not_found"} 8327')
        with self.assertRaisesRegex(RuntimeError, 'read denominator'):
            runner.check_result(metrics, client)

    def test_sustained_trace_requires_real_duration_and_exact_cohorts(self):
        result = runner.check_result(sustained_metrics(), sustained_client(),
                                     scenario='sustained-pressure')
        self.assertEqual(result['sustained_steps'], 200)
        self.assertEqual(result['schedule_elapsed_sec'], 199.02)
        self.assertAlmostEqual(result['max_step_gap_sec'], 1)
        self.assertAlmostEqual(result['max_step_lateness_sec'], .001)
        self.assertAlmostEqual(result['max_step_duration_sec'], .009)
        with self.assertRaisesRegex(RuntimeError, 'unknown scenario'):
            runner.check_result(good_metrics(), good_client(), scenario='unknown')
        with self.assertRaisesRegex(RuntimeError, 'read denominator'):
            runner.check_result(good_metrics(), good_client(), scenario='sustained-pressure')

    def test_sustained_trace_rejects_missing_steps_bad_clocks_and_operations(self):
        invalid_rows = (
            ('step', 1), ('step', True), ('cold_written', 31), ('cold_written', True),
            ('hot_hits', 63), ('hot_misses', -1), ('start_sec', float('nan')),
            ('start_sec', -.1), ('start_sec', True), ('start_sec', 6),
            ('end_sec', float('inf')), ('end_sec', 0),
        )
        for field, value in invalid_rows:
            with self.subTest(field=field, value=value), self.assertRaises(RuntimeError):
                client = sustained_client()
                client['steps'][0][field] = value
                runner.check_steps(client)
        for steps in (None, [], sustained_client()['steps'][:-1]):
            with self.subTest(steps_count=None if steps is None else len(steps)), self.assertRaises(RuntimeError):
                runner.check_steps({**sustained_client(), 'steps': steps})
        for field, value in (('pressure_schedule_elapsed_sec', 198),
                             ('pressure_schedule_elapsed_sec', float('nan')),
                             ('pressure_schedule_origin_sec', -1)):
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                runner.check_steps({**sustained_client(), field: value})
        for index, start in ((1, .5), (1, 6.1), (199, 198.99)):
            with self.subTest(index=index, start=start), self.assertRaises(RuntimeError):
                client = sustained_client()
                client['steps'][index]['start_sec'] = start
                client['steps'][index]['end_sec'] = start + .01
                runner.check_steps(client)
        with self.assertRaisesRegex(RuntimeError, 'hot cohort'):
            runner.check_steps({**sustained_client(), 'hot_hits': 12927, 'hot_misses': 1})

    def test_sustained_pressure_options_reject_delay_and_unknown_scenario(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            args = runner.parse_args(argv + ['--scenario', 'sustained-pressure'])
            self.assertEqual(args.scenario, 'sustained-pressure')
            for invalid in (['--scenario', 'sustained-pressure', '--pressure-delay-sec', '1'],
                            ['--scenario', 'unknown']):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + invalid)

    def arguments(self, root):
        build = root / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build / name).write_text('fake-only')
        roots = [root / ('disk' + str(i)) for i in range(3)]
        for path in roots:
            path.mkdir()
        argv = ['--build', str(build), '--output', str(root / 'output')]
        for path in roots:
            argv += ['--data-root', str(path)]
        return argv

    def test_arguments_require_new_output_three_distinct_existing_roots(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            argv = self.arguments(root)
            self.assertEqual(runner.parse_args(argv).policy, 'lifetime')
            for invalid in (argv[:-2], argv + ['--data-root', str(root / 'disk0')],
                            argv[:-1] + [str(root / 'absent')],
                            argv + ['--policy', 'unknown']):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(invalid)
            (root / 'output').mkdir()
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                runner.parse_args(argv)

    def test_delay_and_data_name_arguments_are_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            args = runner.parse_args(argv + ['--pressure-delay-sec', '185',
                                             '--data-name', 'age-study.1_early'])
            self.assertEqual(args.pressure_delay_sec, 185)
            self.assertEqual(args.data_name, 'age-study.1_early')
            for value in ('0', '300'):
                self.assertEqual(runner.parse_args(argv + ['--pressure-delay-sec', value]).pressure_delay_sec,
                                 int(value))
            invalid = [('--pressure-delay-sec', value) for value in ('-1', '301', '1.5')]
            invalid += [('--data-name', value) for value in
                        ('', '.', '..', '../outside', 'a/b', 'a,b', 'a b', '中文', 'a' * 81)]
            for flag, value in invalid:
                with self.subTest(flag=flag, value=value), \
                        contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + [flag, value])

    def test_existing_fixed_data_name_is_not_reused_or_removed(self):
        with tempfile.TemporaryDirectory() as temp:
            args = runner.parse_args(self.arguments(Path(temp)) + ['--data-name', 'fixed'])
            existing = args.data_root[0] / 'fixed'
            existing.mkdir()
            sentinel = existing / 'keep'
            sentinel.write_text('existing data')
            with mock.patch.object(runner, 'check_ports'), \
                    mock.patch.object(runner.subprocess, 'Popen') as popen:
                result = runner.run(args)
            self.assertEqual(result['status'], 'failed')
            popen.assert_not_called()
            self.assertEqual(sentinel.read_text(), 'existing data')
            self.assertEqual(list(args.data_root[1].iterdir()), [])
            self.assertEqual(list(args.data_root[2].iterdir()), [])

    def test_delay_and_metrics_are_passed_to_client_with_extended_timeout(self):
        for scenario, delay, count, timeout, client, metrics in (
                ('quick-pressure', 185, 6452, 305, good_client(), good_metrics()),
                ('sustained-pressure', 0, 7808, 320, sustained_client(), sustained_metrics())):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temp:
                args = runner.parse_args(self.arguments(Path(temp)) +
                                         ['--data-name', 'fixed', '--scenario', scenario,
                                          '--pressure-delay-sec', str(delay)])
                server = mock.Mock()
                server.poll.return_value = None
                server.wait.return_value = 0
                server.returncode = 0
                response = mock.MagicMock()
                response.__enter__.return_value.status = 200
                text = metrics
                for label, root in zip(('a', 'b', 'c'), args.data_root):
                    text = text.replace('disk="' + label + '"', 'disk=' + json.dumps(str(root / 'fixed')))
                response.__enter__.return_value.read.return_value = text.encode()

                def complete_client(command, **kwargs):
                    trace = args.output / 'trace'
                    trace.mkdir()
                    (trace / 'result.json').write_text(json.dumps(client))
                    return subprocess.CompletedProcess(command, 0)

                with mock.patch.object(runner, 'check_ports'), \
                        mock.patch.object(runner.subprocess, 'Popen', return_value=server), \
                        mock.patch.object(runner.subprocess, 'run', side_effect=complete_client) as client_run, \
                        mock.patch.object(runner.urllib.request, 'urlopen', return_value=response):
                    result = runner.run(args)
                self.assertEqual(result['status'], 'passed')
                command = client_run.call_args.args[0]
                self.assertEqual(command[command.index('--scenario') + 1], scenario)
                self.assertEqual(command[command.index('--count') + 1], str(count))
                self.assertEqual(command[command.index('--pressure-delay-sec') + 1], str(delay))
                self.assertEqual(command[command.index('--metrics-url') + 1], runner.METRICS_URL)
                self.assertEqual(client_run.call_args.kwargs['timeout'], timeout)
                case = json.loads((args.output / 'case.json').read_text())
                self.assertEqual(case['pressure_delay_sec'], delay)
                self.assertEqual(case['scenario'], scenario)
                self.assertEqual(case['client_command'], command)
                self.assertEqual(case['client_timeout_sec'], timeout)
                self.assertEqual(case['data_dirs'], [str(root / 'fixed') for root in args.data_root])
                server.terminate.assert_called_once()

    def test_start_failure_records_result_and_clears_inherited_disk_settings(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = runner.parse_args(self.arguments(root))
            with mock.patch.object(runner, 'check_ports'), \
                 mock.patch.dict(runner.os.environ, {'DFKV_SLAB_LIFE_MAX_READS': '63'}), \
                 mock.patch.object(runner.subprocess, 'Popen', side_effect=OSError('spawn failed')) as popen:
                result = runner.run(args)
            self.assertEqual(result['status'], 'failed')
            self.assertIn('spawn failed', result['error'])
            self.assertNotIn('DFKV_SLAB_LIFE_MAX_READS', popen.call_args.kwargs['env'])
            self.assertEqual(popen.call_args.kwargs['env']['DFKV_RAM_TIER'], '0')
            saved = json.loads((args.output / 'result.json').read_text())
            self.assertEqual(saved['status'], 'failed')
            self.assertTrue((args.output / 'case.json').is_file())
            for path in args.data_root:
                self.assertEqual(len(list(path.iterdir())), 1)

    def test_teardown_timeout_and_nonzero_exit_are_failures(self):
        server = mock.Mock()
        server.poll.return_value = None
        server.wait.return_value = 0
        runner.stop_server(server)
        server.terminate.assert_called_once()
        server.wait.assert_called_once_with(timeout=15)
        server.poll.return_value = 2
        server.wait.return_value = 2
        with self.assertRaisesRegex(RuntimeError, 'exited 2'):
            runner.stop_server(server)
        server.wait.side_effect = [subprocess.TimeoutExpired('server', 15), -9]
        with self.assertRaisesRegex(RuntimeError, 'SIGKILL'):
            runner.stop_server(server)
        server.kill.assert_called_once()

    def test_busy_port_closes_all_probe_sockets(self):
        first, busy = mock.Mock(), mock.Mock()
        busy.bind.side_effect = OSError('address in use')
        with mock.patch.object(runner.socket, 'socket', side_effect=[first, busy]):
            with self.assertRaises(OSError):
                runner.check_ports()
        first.close.assert_called_once()
        busy.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
