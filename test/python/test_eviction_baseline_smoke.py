"""Two-policy baseline smoke safety and actual native TCP correctness tests."""

import os
from pathlib import Path
import subprocess
import socket
import threading
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'deploy/validation'))
import eviction_baseline_smoke as baseline
import eviction_trace as trace


class BaselineSmokeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='dfkv-baseline-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.build = self.base / 'build'
        self.build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (self.build / name).write_bytes(b'not executable')

    def test_output_rejects_mnt_existing_symlink_and_missing_parent(self):
        for path in ('/mnt/disk1/no-load', '/mnt/disk2/no-load', self.base,
                     self.base / 'missing' / 'output'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                baseline._new_output(path)
        alias = self.base / 'alias'
        alias.symlink_to('/mnt/disk1', target_is_directory=True)
        with self.assertRaises(ValueError):
            baseline._new_output(alias / 'no-load')
        dangling = self.base / 'dangling'
        dangling.symlink_to(self.base / 'absent')
        with self.assertRaises(ValueError):
            baseline._new_output(dangling)

    def test_banned_build_and_symlink_artifacts_rejected_before_hashing(self):
        with mock.patch.object(baseline, 'file_sha') as checksum:
            with self.assertRaises(ValueError):
                baseline.run('/mnt/disk1/banned-build', self.base / 'out')
            checksum.assert_not_called()
        for name in ('dfkv_server', 'libdfkv.so'):
            target = self.build / name
            target.unlink()
            target.symlink_to('/mnt/disk1/banned-artifact')
            with mock.patch.object(baseline, 'file_sha') as checksum:
                with self.assertRaises(ValueError):
                    baseline.run(self.build, self.base / 'out')
                checksum.assert_not_called()
            target.unlink()
            target.write_bytes(b'not executable')
        self.assertFalse((self.base / 'out').exists())

    def test_environment_clears_inherited_dfkv_and_freezes_tcp_disk_ack(self):
        with mock.patch.dict(os.environ, {'DFKV_SLAB_LIFE_MAX_READS': '63',
                                        'DFKV_RDMA': '1', 'DFKV_PUT_ACK_MODE': 'ram'}):
            env = baseline.environment()
        self.assertNotIn('DFKV_SLAB_LIFE_MAX_READS', env)
        self.assertEqual(env['DFKV_RDMA'], '0')
        self.assertEqual(env['DFKV_PUT_ACK_MODE'], 'disk')
        self.assertEqual(env['DFKV_RAM_TIER'], '0')
        self.assertEqual(env['DFKV_READ_COALESCE'], '0')

    def test_missing_build_rejected_before_output_creation(self):
        (self.build / 'libdfkv.so').unlink()
        output = self.base / 'out'
        with self.assertRaises(ValueError):
            baseline.run(self.build, output)
        self.assertFalse(output.exists())

    def test_spawn_failure_retains_unknown_complete_plan_and_never_retries(self):
        output = self.base / 'out'
        with mock.patch.object(baseline.subprocess, 'Popen', side_effect=OSError('spawn failed')) as popen:
            result = baseline.run(self.build, output)
        self.assertFalse(result['passed'])
        self.assertEqual(popen.call_count, 1)
        self.assertEqual(result['policies'][1]['status'], 'not_attempted')
        self.assertEqual(result['planned_per_policy'], 8)
        self.assertEqual(result['policies'][0]['request_outcomes'], 'unknown')
        self.assertIsNone(result['policies'][0]['completed_requests'])
        self.assertEqual(trace.load(output / 'report.json'), result)
        with self.assertRaises(ValueError):
            baseline.run(self.build, output)

    def test_binary_drift_after_failure_is_still_recorded(self):
        def fail(*args, **kwargs):
            (self.build / 'dfkv_server').write_bytes(b'changed')
            raise OSError('controlled spawn failure')
        with mock.patch.object(baseline.subprocess, 'Popen', side_effect=fail):
            result = baseline.run(self.build, self.base / 'out')
        self.assertFalse(result['passed'])
        self.assertIn('binary/library/trace changed during run', result['errors'])
        self.assertNotEqual(result['build_sha256'], result['build_sha256_after'])

    def test_whole_http_deadline_reaps_child_for_trickling_headers_and_body(self):
        for mode in ('headers', 'body'):
            with self.subTest(mode=mode), socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                listener.listen(1)
                listener.settimeout(2)
                port = listener.getsockname()[1]
                stop = threading.Event()
                owned = []
                original_popen = subprocess.Popen

                def launch(*args, **kwargs):
                    child = original_popen(*args, **kwargs)
                    owned.append(child)
                    return child

                def trickle():
                    try:
                        connection, _ = listener.accept()
                        with connection:
                            connection.settimeout(1)
                            connection.recv(4096)
                            if mode == 'headers':
                                connection.sendall(b'HTTP/1.1 200 OK\r\nX-Trickle: ')
                            else:
                                connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n')
                            while not stop.wait(.03):
                                connection.sendall(b'x')
                    except OSError:
                        pass

                server = threading.Thread(target=trickle)
                server.start()
                try:
                    with mock.patch.object(baseline, 'HTTP_TIMEOUT_SEC', .4), \
                            mock.patch.object(baseline.subprocess, 'Popen', side_effect=launch):
                        with self.assertRaisesRegex(TimeoutError, 'whole metrics HTTP'):
                            baseline._scrape(port)
                finally:
                    stop.set()
                    server.join(timeout=3)
                self.assertFalse(server.is_alive())
                self.assertEqual(len(owned), 1)
                self.assertIsNotNone(owned[0].returncode)
                with self.assertRaises(ChildProcessError):
                    os.waitpid(owned[0].pid, os.WNOHANG)

    def test_partial_greeting_real_child_has_bounded_deadline(self):
        child = subprocess.Popen([sys.executable, '-c',
                                  "import sys,time;sys.stdout.write('PORT ');"
                                  "sys.stdout.flush();time.sleep(60)"],
                                 stdout=subprocess.PIPE, stdin=subprocess.DEVNULL)
        try:
            with self.assertRaises(TimeoutError):
                baseline._port_greeting(child, timeout_sec=.2)
        finally:
            child.terminate()
            child.wait(timeout=3)
            child.stdout.close()
        self.assertIsNotNone(child.returncode)

    def test_stop_owned_normal_nonzero_and_term_timeout(self):
        child = mock.Mock()
        child.poll.return_value = None
        child.wait.return_value = 0
        baseline.stop_owned(child)
        child.terminate.assert_called_once_with()
        child.kill.assert_not_called()
        child.wait.return_value = 2
        with self.assertRaises(RuntimeError):
            baseline.stop_owned(child)
        child.wait.side_effect = [subprocess.TimeoutExpired('owned', 5), -9]
        with self.assertRaisesRegex(RuntimeError, 'killed=True'):
            baseline.stop_owned(child)
        child.kill.assert_called_once_with()

    def test_readiness_exit_timeout_and_wrong_policy_never_pass(self):
        child = mock.Mock()
        child.poll.return_value = 2
        log = self.base / 'server.log'
        log.write_text('disk_eviction_effective = legacy (default)\n')
        with self.assertRaisesRegex(RuntimeError, 'exited'):
            baseline._ready(child, 1234, log, 'lifetime')
        child.poll.return_value = None
        with mock.patch.object(baseline, '_scrape', return_value='ok'), \
                mock.patch.object(baseline.time, 'monotonic', side_effect=[0, 1, 11]), \
                mock.patch.object(baseline.time, 'sleep'):
            with self.assertRaises(TimeoutError):
                baseline._ready(child, 1234, log, 'lifetime')


class NativeBaselineSmokeTest(unittest.TestCase):
    def test_same_native_binary_two_explicit_slab_policies_full_byte_ledger(self):
        build = Path(os.environ.get('DFKV_BUILD', ROOT / 'build')).resolve()
        if not (build / 'dfkv_server').is_file() or not (build / 'libdfkv.so').is_file():
            self.skipTest('requires built dfkv_server and libdfkv.so')
        with tempfile.TemporaryDirectory(prefix='dfkv-baseline-native-') as temp:
            output = Path(temp) / 'run'
            result = baseline.run(build, output)
            self.assertTrue(result['passed'], result['errors'])
            self.assertFalse(result['formal_ab'])
            self.assertFalse(result['performance_baseline'])
            self.assertEqual(result['build_sha256'], result['build_sha256_after'])
            self.assertEqual([r['policy'] for r in result['policies']], ['legacy', 'lifetime'])
            for row in result['policies']:
                self.assertEqual((row['server_exit'], row['server_reaped']), (0, True))
                self.assertEqual(row['client']['completed_requests'], 8)
                self.assertEqual(row['lifetime_read_success_delta'], 4 if row['policy'] == 'lifetime' else 0)
                self.assertEqual(row['client']['acked_put_bytes'], 512)
                self.assertEqual(row['client']['config']['client_environment']['DFKV_RDMA'], '0')
                ledger = trace.load(output / row['policy'] / 'client' / 'child-result.json')
                self.assertEqual([r['terminal'] for r in ledger['requests']],
                                 ['put_ack'] * 2 + ['get_hit'] * 2 + ['put_ack', 'get_hit'] * 2)
                self.assertEqual(ledger['summary']['hit_get_bytes'], 512)
                self.assertEqual(len(ledger['requests']), 8)
                self.assertTrue((output / row['policy'] / 'before.prom').is_file())
                self.assertTrue((output / row['policy'] / 'after.prom').is_file())


if __name__ == '__main__':
    unittest.main()
