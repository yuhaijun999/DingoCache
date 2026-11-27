"""Real /proc and malformed/reused-process resource accounting contracts."""
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import boundary_resources as resources


def stat(pid=123, *, start=100, user=12, system=3, state='S'):
    fields = [state] + ['0'] * 19
    fields[11], fields[12], fields[19] = str(user), str(system), str(start)
    return f'{pid} (test ) comm) ' + ' '.join(fields) + '\n'


def status(pid=123):
    return f'Name:\ttest\nPid:\t{pid}\nVmRSS:\t128 kB\nVmHWM:\t256 kB\nVmLck:\t0 kB\nThreads:\t2\n'


def row(role='server', pid=123, start=100, begin=1., finish=2., user=12):
    return dict(role=role, pid=pid, starttime_ticks=start,
                started_monotonic_sec=begin, finished_monotonic_sec=finish,
                resource=dict(rss_kib=128, vm_hwm_kib=256, threads=2, fd_count=3, vm_lck_kib=0,
                              user_ticks=user, system_ticks=3, clock_ticks_per_sec=100))


class BoundaryResourcesTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.proc = Path(self.directory.name) / '123'
        self.proc.mkdir()
        (self.proc / 'fd').mkdir()
        for fd in ('0', '1', '2'):
            (self.proc / 'fd' / fd).touch()
        (self.proc / 'stat').write_text(stat())
        (self.proc / 'status').write_text(status())

    def sampler(self, role='server'):
        return resources.ProcessSampler(123, role, proc_root=self.directory.name)

    def test_snapshot_includes_identity_units_and_observation_interval(self):
        with mock.patch.object(resources.time, 'monotonic', side_effect=[4., 5.]), \
                mock.patch.object(resources.os, 'sysconf', return_value=100):
            observed = self.sampler().sample()
        expected = row(begin=4., finish=5.)
        self.assertEqual(observed, expected)
        self.assertEqual(observed['resource']['vm_lck_kib'], 0)  # A real reported zero.

    def test_pid_and_role_inputs_are_strict(self):
        for pid in (True, 0, -1, '123', 1.5):
            with self.subTest(pid=pid), self.assertRaises(ValueError):
                resources.ProcessSampler(pid, 'server', proc_root=self.directory.name)
        for role in ('host', '', None, True):
            with self.subTest(role=role), self.assertRaises(ValueError):
                self.sampler(role)

    def test_missing_process_is_not_zero_sample(self):
        (self.proc / 'stat').unlink()
        with self.assertRaisesRegex(RuntimeError, 'stat unavailable'):
            self.sampler()

    def test_missing_status_and_fd_directory_are_not_zero_samples(self):
        sampler = self.sampler()
        (self.proc / 'status').unlink()
        with self.assertRaisesRegex(RuntimeError, 'resources unavailable'):
            sampler.sample()
        (self.proc / 'status').write_text(status())
        for entry in (self.proc / 'fd').iterdir():
            entry.unlink()
        (self.proc / 'fd').rmdir()
        with self.assertRaisesRegex(RuntimeError, 'resources unavailable'):
            sampler.sample()

    def test_reuse_before_or_during_resource_reads_rejected(self):
        for stats in ([stat(start=101), stat(start=101)], [stat(), stat(start=101)]):
            sampler = self.sampler()
            with mock.patch.object(sampler, '_read_stat', side_effect=[
                    resources._stat(text, 123) for text in stats]):
                with self.assertRaisesRegex(ValueError, 'identity changed'):
                    sampler.sample()

    def test_exit_during_reads_and_permission_failure_rejected(self):
        sampler = self.sampler()
        with mock.patch.object(sampler, '_read_stat', side_effect=[
                resources._stat(stat(), 123), RuntimeError('owned process stat unavailable')]):
            with self.assertRaisesRegex(RuntimeError, 'stat unavailable'):
                sampler.sample()
        with mock.patch.object(Path, 'read_text', side_effect=PermissionError('denied')):
            with self.assertRaisesRegex(RuntimeError, 'stat unavailable'):
                sampler.sample()

    def test_dead_malformed_wrong_pid_and_negative_stat_rejected(self):
        for text in (stat(state='Z'), stat(state='X'), stat(state='x'), stat(state='?'),
                     stat(pid=124), stat(start=-1), stat(user=-1), '123 (short) S 0\n',
                     'no stat', stat().replace(' 12 ', ' nan ')):
            with self.subTest(text=text), self.assertRaises(ValueError):
                resources._stat(text, 123)

    def test_status_missing_duplicate_wrong_units_and_wrong_pid_rejected(self):
        valid = status()
        for text in (valid.replace('VmLck:\t0 kB\n', ''),
                     valid.replace('VmHWM:\t256 kB\n', ''), valid + 'VmRSS: 1 kB\n',
                     valid.replace('128 kB', '128 MB'), valid.replace('128 kB', '-1 kB'),
                     valid.replace('Threads:\t2', 'Threads:\t0'), status(124),
                     valid.replace('VmRSS:\t128', 'VmRSS:\tnan')):
            with self.subTest(text=text), self.assertRaises(ValueError):
                resources._status(text, 123)

    def test_invalid_fd_entry_and_regressed_cpu_rejected(self):
        sampler = self.sampler()
        (self.proc / 'fd' / 'not-a-fd').touch()
        with self.assertRaisesRegex(ValueError, 'FD entry'):
            sampler.sample()
        (self.proc / 'fd' / 'not-a-fd').unlink()
        with mock.patch.object(sampler, '_read_stat', side_effect=[
                resources._stat(stat(), 123), resources._stat(stat(user=11), 123)]):
            with self.assertRaisesRegex(ValueError, 'CPU counters regressed'):
                sampler.sample()

    def test_invalid_clock_or_monotonic_observation_rejected(self):
        for clock in (0, -1, True, 100.):
            with self.subTest(clock=clock), \
                    mock.patch.object(resources.os, 'sysconf', return_value=clock), \
                    self.assertRaises(ValueError):
                self.sampler().sample()
        for clocks in ([5., 4.], [float('nan'), 5.], [1., float('inf')]):
            with self.subTest(clocks=clocks), \
                    mock.patch.object(resources.time, 'monotonic', side_effect=clocks), \
                    self.assertRaises(ValueError):
                self.sampler().sample()

    def test_summary_keeps_roles_identities_and_nonsimultaneous_peaks_separate(self):
        a = row()
        b = row(begin=3., finish=4., user=32)
        b['resource']['fd_count'] = 9
        b['resource']['rss_kib'] = 64
        client = row('client', 456, begin=2., finish=3.)
        client['resource']['rss_kib'] = 512
        reused_pid = row(pid=123, start=999, begin=5., finish=6.)
        result = resources.summarize([a, client, b, reused_pid])
        self.assertEqual(len(result['roles']['server']), 2)
        server = result['roles']['server'][0]
        self.assertEqual(server['samples'], 2)
        self.assertEqual(server['observed_peaks'], dict(rss_kib=128, vm_hwm_kib=256, threads=2,
                                                       fd_count=9, vm_lck_kib=0))
        self.assertEqual(server['observed_cpu_ticks'], 20)
        self.assertEqual(server['observed_cpu_sec'], .2)
        self.assertEqual(result['roles']['client'][0]['observed_peaks']['rss_kib'], 512)
        self.assertNotIn('total_rss_kib', result)
        self.assertTrue(any('not total RDMA' in item for item in result['limitations']))
        self.assertTrue(any('not complete allocator' in item for item in result['limitations']))
        self.assertTrue(any('before measurement' in item for item in result['limitations']))

    def test_summary_rejects_empty_unknown_or_invalid_rows(self):
        with self.assertRaisesRegex(ValueError, 'no resource samples'):
            resources.summarize([])
        broken = [None, {}, dict(row(), role='host'), dict(row(), pid=True),
                  dict(row(), finished_monotonic_sec=0), dict(row(), started_monotonic_sec=float('nan'))]
        for key, value in (('rss_kib', -1), ('threads', 0), ('fd_count', True),
                           ('user_ticks', None), ('clock_ticks_per_sec', 0)):
            value_row = row()
            value_row['resource'][key] = value
            broken.append(value_row)
        extra = row()
        extra['resource']['estimated_mr_bytes'] = 1
        broken.append(extra)
        for value in broken:
            with self.subTest(value=value), self.assertRaises(ValueError):
                resources.summarize([value])

    def test_summary_rejects_reordered_overlap_cpu_regression_and_clock_changes(self):
        for second in (row(begin=.5, finish=.9), row(begin=1.5, finish=2.5),
                       row(begin=3., finish=4., user=11)):
            with self.subTest(second=second), self.assertRaises(ValueError):
                resources.summarize([row(), second])
        second = row(begin=3., finish=4.)
        second['resource']['clock_ticks_per_sec'] = 250
        with self.assertRaisesRegex(ValueError, 'clock changed'):
            resources.summarize([row(), second])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux /proc required')
    def test_real_self_proc_sample(self):
        sampler = resources.ProcessSampler(os.getpid(), 'client')
        observed = sampler.sample()
        self.assertEqual(observed['pid'], os.getpid())
        self.assertGreater(observed['resource']['rss_kib'], 0)
        self.assertGreaterEqual(observed['resource']['vm_hwm_kib'], 0)
        self.assertGreaterEqual(observed['resource']['threads'], 1)
        self.assertGreaterEqual(observed['resource']['fd_count'], 3)
        self.assertEqual(observed['starttime_ticks'], sampler.starttime_ticks)
        self.assertEqual(resources.summarize([observed])['roles']['server'], [])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux /proc required')
    def test_real_owned_child_and_exit_not_silently_sampled(self):
        child = subprocess.Popen([sys.executable, '-c',
            "import time; print('ready', flush=True); time.sleep(30)"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([child.stdout], [], [], 5)[0], 'child readiness timeout')
            self.assertEqual(child.stdout.readline().strip(), 'ready')
            sampler = resources.ProcessSampler(child.pid, 'server')
            first, second = sampler.sample(), sampler.sample()
            summary = resources.summarize([first, second])
            self.assertEqual(summary['roles']['server'][0]['samples'], 2)
            self.assertGreater(first['resource']['rss_kib'], 0)
            child.terminate()
            child.wait(timeout=5)
            with self.assertRaises(RuntimeError):
                sampler.sample()
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            child.stdout.close()


if __name__ == '__main__':
    unittest.main()
