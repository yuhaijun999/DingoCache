"""Owned-process active-kill contracts; no native services or hardware loads."""
from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'deploy/validation'))
import run_prefix_recovery_active as runner


class Process:
    pid = 123

    def __init__(self, code=None, hung=False):
        self.code, self.hung = code, hung
        self.kills = 0
        self.waits = []

    def poll(self):
        return self.code

    def kill(self):
        self.kills += 1
        self.code = -9

    def terminate(self):
        self.code = 0

    def wait(self, timeout):
        self.waits.append(timeout)
        if self.hung and self.code is None:
            raise subprocess.TimeoutExpired('owned', timeout)
        if self.code is None:
            self.code = 1
        return self.code


def prom(puts, gets):
    text = (f'dfkv_cache_put_total {puts}\n'
            f'dfkv_rdma_pull_requests_terminal_total{{path="uring",result="success_nonempty"}} {gets}\n'
            'dfkv_rdma_pull_requests_terminal_total{path="uring",result="not_found"} 0\n'
            'dfkv_rdma_pull_requests_terminal_total{path="sync",result="io_error"} 0\n'
            'dfkv_storage_healthy 1\ndfkv_slab_healthy 1\n'
            'dfkv_errors_total{op="get"} 0\ndfkv_uring_init_fallbacks_total 0\n'
            'dfkv_uring_inflight 7\ndfkv_rdma_pull_requests_dispatched_total 100\n')
    for path in ('record_write', 'foreground_evict', 'background_evict', 'watermark_evict',
                 'extent_bind', 'donor_bind', 'table_sync', 'bind_sync', 'donor_sync'):
        text += f'dfkv_slab_metadata_ops_errors_total{{path="{path}"}} 0\n'
    return text


class ActiveRecoveryTest(unittest.TestCase):
    def test_four_cases_and_bounded_pressure_plan(self):
        rows = runner.plan()
        self.assertEqual(len(rows), 4)
        self.assertEqual({(row['policies'][0], row['transition']) for row in rows},
                         {(p, t) for p in ('legacy', 'lifetime') for t in ('same', 'switch')})
        for row in rows:
            self.assertEqual(row['policies'][0] == row['policies'][1], row['transition']=='same')
        plan = runner.concurrent.generate(workers=4, rounds=600, prefill=12288,
                                          interval_ns=100000000, arrival_batch=8)
        runner.concurrent.validate(plan)

    def test_activity_gate_records_two_progressing_scrapes(self):
        server, client, receipt = Process(), Process(), {}
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(runner.baseline, '_scrape', side_effect=[prom(12318, 58), prom(12320, 60), prom(12330, 70)]), \
                mock.patch.object(runner.time, 'sleep'):
            runner.await_activity(server, client, directory, receipt)
            self.assertTrue(receipt['active_gate'])
            self.assertEqual(receipt['progressing_observations'], 2)
            self.assertEqual(len(receipt['activity']), 3)
            self.assertEqual((Path(directory)/'before-kill.prom').read_text(), prom(12330, 70))
            self.assertEqual(server.kills, 0)

    def test_no_activity_or_early_exit_cannot_kill(self):
        for child in (Process(code=0), Process()):
            receipt = {}
            with tempfile.TemporaryDirectory() as directory, \
                    mock.patch.object(runner.time, 'monotonic', side_effect=[0, 0, 1, 100]), \
                    mock.patch.object(runner.time, 'sleep'), \
                    mock.patch.object(runner.baseline, '_scrape', return_value=prom(12320, 60)):
                with self.assertRaises((RuntimeError, TimeoutError)):
                    runner.await_activity(Process(), child, directory, receipt)
            self.assertFalse(receipt.get('active_gate', False))
            self.assertEqual(child.kills, 0)

    def test_regressed_nan_missing_metric_rejected(self):
        for texts in ([prom(12320, 60), prom(12319, 70)],
                      [prom('nan', 60)], ['unrelated 1\n']):
            with tempfile.TemporaryDirectory() as directory, \
                    mock.patch.object(runner.baseline, '_scrape', side_effect=texts), \
                    mock.patch.object(runner.time, 'sleep'):
                with self.assertRaises((ValueError, RuntimeError)):
                    runner.await_activity(Process(), Process(), directory, {})

    def test_live_health_rejects_labeled_errors_and_wrong_terminal_paths(self):
        before = prom(12320, 60)
        after = prom(12330, 70)
        runner.check_active_health(after, before)  # inflight=7 is deliberately allowed.
        for broken in (after.replace('op="get"} 0', 'op="get"} 1'),
                       after.replace('dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_init_fallbacks_total 1'),
                       after.replace('path="record_write"} 0', 'path="record_write"} 1'),
                       after.replace('path="sync",result="io_error"} 0', 'path="sync",result="io_error"} 1'),
                       after.replace('dfkv_storage_healthy 1', 'dfkv_storage_healthy 0')):
            with self.assertRaises(ValueError):
                runner.check_active_health(broken, before)
            with self.assertRaises(ValueError):
                runner.check_active_health(broken)

    def test_exact_owned_kill_and_receipt(self):
        server, client, receipt = Process(), Process(), {}
        runner.kill_active(server, client, receipt)
        self.assertEqual(server.kills, 1)
        self.assertEqual(client.kills, 0)
        self.assertEqual(server.waits, [3])
        self.assertEqual(receipt['server_exit'], -9)
        self.assertTrue(receipt['server_reaped'])
        self.assertIsNone(receipt['client_exit_before_kill'])
        self.assertIn('not-kernel-inflight', receipt['scope'])
        for server, client in ((Process(0), Process()), (Process(), Process(1))):
            with self.assertRaises(RuntimeError):
                runner.kill_active(server, client, {})
            self.assertEqual(server.kills, 0)

    def test_client_exit_timeout_and_unreaped_are_not_misses(self):
        client, receipt = Process(7), {}
        runner.finish_client(client, receipt)
        self.assertEqual(receipt['exit_before_cleanup'], 7)
        self.assertEqual(receipt['outcome'], 'nonzero')
        self.assertEqual(client.waits, [30])
        client, receipt = Process(hung=True), {}
        runner.finish_client(client, receipt)
        self.assertEqual(client.waits, [30, 3])
        self.assertEqual(client.kills, 1)
        self.assertEqual(receipt['outcome'], 'timeout-unknown')
        self.assertIsNone(receipt['exit_before_cleanup'])
        self.assertEqual(receipt['exit'], -9)
        self.assertTrue(receipt['reaped'])
        client, receipt = Process(hung=True), {}
        with mock.patch.object(client, 'kill'):
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.finish_client(client, receipt)
        self.assertFalse(receipt['reaped'])
        for timeout in (0, 31, float('nan')):
            with self.assertRaises(ValueError):
                runner.finish_client(Process(), {}, timeout=timeout)

    def test_partial_report_is_checked_and_missing_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'report.json'
            receipt = dict(outcome='nonzero')
            runner.inspect_client({}, path, receipt)
            self.assertFalse(receipt['report_present'])
            self.assertEqual(receipt['missing_report_means'], 'unknown-not-miss')
            with self.assertRaises(ValueError):
                runner.inspect_client({}, path, dict(outcome='zero'))
            path.write_text(json.dumps(dict(transport='rdma', passed=False)))
            with mock.patch.object(runner.concurrent, 'check', return_value={'unknown': 9}) as check:
                receipt = dict(outcome='nonzero')
                runner.inspect_client({'plan': 1}, path, receipt)
                check.assert_called_once()
                self.assertFalse(receipt['report_passed'])
                self.assertEqual(len(receipt['report_sha256']), 64)
            with mock.patch.object(runner.concurrent, 'check', side_effect=ValueError('bad evidence')):
                with self.assertRaises(ValueError):
                    runner.inspect_client({}, path, dict(outcome='nonzero'))
            path.write_text(json.dumps(dict(transport='rdma', passed=True)))
            with mock.patch.object(runner.concurrent, 'check', return_value={}):
                with self.assertRaises(ValueError):
                    runner.inspect_client({}, path, dict(outcome='zero'))

    def test_full_four_case_flow_with_owned_process_mocks(self):
        self._full_flow()

    def test_initial_missing_snapshot_blocks_pressure_kill(self):
        self._full_flow('initial')

    def test_resumed_missing_snapshot_fails_strict_gate(self):
        self._full_flow('resumed')

    def _full_flow(self, missing_stage=None):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory)
            build = root/'build'
            build.mkdir()
            for name in ('dfkv_server', 'libdfkv.so'):
                (build/name).write_text('fake binary')
            roots = [root/str(i) for i in range(3)]
            for path in roots:
                path.mkdir()
            output = root/'output'
            children = [Process() for _ in range(12)]
            stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
            stack.enter_context(mock.patch.object(runner.mixed, 'environment', return_value={}))
            stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
            stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', side_effect=children))
            stack.enter_context(mock.patch.object(runner.baseline, '_ready', return_value='startup'))
            stack.enter_context(mock.patch.object(runner.baseline, '_scrape', return_value='metrics'))
            for name in ('check_startup', 'check_initial'):
                stack.enter_context(mock.patch.object(runner.mixed, name))
            startup = stack.enter_context(mock.patch.object(runner.recovery, 'check_restart'))
            stack.enter_context(mock.patch.object(runner.recovery, 'check_client_metrics', return_value={}))
            stack.enter_context(mock.patch.object(runner.mixed, 'Sampler'))
            stack.enter_context(mock.patch.object(runner, 'await_activity'))
            stack.enter_context(mock.patch.object(runner, 'inspect_client'))
            summary = dict(native={key: 0 for key in ('put_ack', 'put_error', 'put_unknown',
                                                     'get_hit', 'get_miss', 'get_error', 'get_unknown')}, put_bytes=0)
            stack.enter_context(mock.patch.object(runner.workload, 'check', return_value=summary))
            def run_client(cmd, *args, **kwargs):
                child_output = Path(cmd[cmd.index('--output')+1])
                mode = cmd[cmd.index('--mode')+1]
                child_output.mkdir()
                (child_output/'report.json').write_text(json.dumps(
                    dict(passed=True, transport='rdma', mode=mode, objects=(['miss']+['hit']*31 if
                         mode == 'snapshot' and ((missing_stage == 'initial' and child_output.parent.name.startswith('start-0')) or
                         (missing_stage == 'resumed' and child_output.parent.name.startswith('start-1'))) else ['hit']*32),
                         summary=dict(confirmed_prefix_tokens=640))))
            stack.enter_context(mock.patch.object(runner.mixed, 'run_client', side_effect=run_client))
            result = runner.run(build, roots, output, 'full')
            if missing_stage is not None:
                self.assertFalse(result['passed'])
                self.assertIn('snapshot must preserve all 32', result['errors'][0])
                self.assertEqual(sum(child.kills for child in children), 0 if missing_stage == 'initial' else 1)
                self.assertEqual(result['cases'][0]['state'], 'failed')
                self.assertEqual(result['cases'][1]['state'], 'not_attempted')
                return
            self.assertTrue(result['passed'], result['errors'])
            self.assertEqual(startup.call_count, 8)
            self.assertEqual(sum(call.kwargs['dirty'] for call in startup.call_args_list), 4)
            self.assertTrue(all(row['state']=='passed' for row in result['cases']))
            self.assertEqual(sum(child.kills for child in children), 4)
            for row in result['cases']:
                first, second = row['stages']
                self.assertEqual(first['stop']['server_exit'], -9)
                self.assertEqual(first['pressure_receipt']['outcome'], 'nonzero')
                self.assertEqual([client['label'] for client in first['clients']], ['seed', 'stabilize', 'snapshot'])
                self.assertEqual([client['label'] for client in second['clients']], ['verify', 'refill', 'stabilize', 'snapshot'])
                self.assertEqual([client['mode'] for client in first['clients']], ['seed', 'refill', 'snapshot'])
                self.assertEqual([client['mode'] for client in second['clients']], ['verify', 'refill', 'refill', 'snapshot'])

    def test_run_failure_owns_and_reaps_server_and_writes_matrix(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory)
            build = root/'build'
            build.mkdir()
            for name in ('dfkv_server', 'libdfkv.so'):
                (build/name).write_text('fake binary')
            roots = [root/str(i) for i in range(3)]
            for path in roots:
                path.mkdir()
            output = root/'output'
            server = Process()
            stack.enter_context(mock.patch.object(runner.common, 'guest_paths', return_value=(roots, output)))
            stack.enter_context(mock.patch.object(runner.mixed, 'environment', return_value={}))
            stack.enter_context(mock.patch.object(runner.quick, 'check_ports'))
            stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', return_value=server))
            stack.enter_context(mock.patch.object(runner.baseline, '_ready', side_effect=RuntimeError('not ready')))
            result = runner.run(build, roots, output, 'test')
            self.assertFalse(result['passed'])
            self.assertEqual(result['cases'][0]['state'], 'failed')
            self.assertEqual(server.poll(), 0)
            self.assertEqual(server.waits, [15])
            self.assertTrue((output/'matrix.json').is_file())
            self.assertEqual(result['cases'][1]['state'], 'not_attempted')
            self.assertTrue(result['cases'][0]['stages'][0]['server_reaped'])


if __name__ == '__main__':
    unittest.main()
