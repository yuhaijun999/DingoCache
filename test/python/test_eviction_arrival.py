"""Native-arrival contracts with controlled adapters and exact owned children."""

import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'deploy/validation'))
import eviction_arrival as arrival
import eviction_trace as trace


class Adapter:
    """Controlled in-memory fixture, never a native/transport proof."""
    instances = []
    store = {}
    outcomes = {}
    durations = {}
    entered = {}
    release = None
    fail_open = None
    close_error = False
    lock = threading.Lock()

    def __init__(self, document, lib, members, namespace):
        if len(self.instances) == self.fail_open:
            raise RuntimeError('controlled open failure')
        self.rows, self.namespace = document['requests'], namespace.hex()
        self.transport, self.closed = arrival.TCP_MODE, False
        self.active, self.threads = 0, set()
        self.instances.append(self)

    def execute(self, request_id):
        row = self.rows[request_id]
        self.threads.add(threading.get_ident())
        self.active += 1
        try:
            if request_id in self.entered:
                self.entered[request_id].set()
                if not self.release.wait(3):
                    raise TimeoutError('controller did not release call')
            if request_id in self.durations:
                time.sleep(self.durations[request_id])
            if request_id in self.outcomes:
                value = self.outcomes[request_id]
                if isinstance(value, BaseException):
                    raise value
                return {'outcome': value}
            key = (row['namespace_hex'], row['key_hex'])
            with self.lock:
                if row['op'] == 'put':
                    self.store[key] = trace.payload(row)
                    return {'outcome': 'put_ack'}
                return {'outcome': 'get_hit' if self.store.get(key) == trace.payload(row) else 'get_miss'}
        finally:
            self.active -= 1

    def close(self):
        if self.active:
            raise RuntimeError('closed while call active')
        self.closed = True
        if self.close_error:
            raise RuntimeError('controlled close failure')


class RdmaAdapter(Adapter):
    """Controlled RDMA label fixture, not an actual verbs execution."""
    def __init__(self, *args):
        super().__init__(*args)
        self.transport = 'rdma'


class ArrivalTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='dfkv-arrival-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.binary = self.base / 'library'
        self.binary.write_bytes(b'controlled fixture identity, never loaded')
        self.document = trace.generate(objects=2, rounds=2, length=3, interval_ns=80000000)
        self.source = self.base / 'source.json'
        self.source.write_bytes(trace.encode(self.document))
        self.output = self.base / 'run'
        self.config = arrival._config(self.binary, 'n=127.0.0.1:12000')
        Adapter.instances, Adapter.store, Adapter.outcomes = [], {}, {}
        Adapter.durations, Adapter.entered, Adapter.release = {}, {}, threading.Event()
        Adapter.fail_open, Adapter.close_error = None, False

    def run_client(self, config=None, library=None):
        return arrival.run(self.document, config or self.config,
                           library_loader=lambda _: library if library is not None else SimpleNamespace(dfkv_get_auto_status=object()),
                           client_factory=Adapter)

    def test_rdma_configs_do_not_alias_each_other_or_the_validation_template(self):
        with mock.patch.object(arrival.smoke, 'CLIENT_ENV', dict(arrival.smoke.CLIENT_ENV)):
            first = arrival._config(self.binary, 'n=127.0.0.1:12001', tcp=False)
            second = arrival._config(self.binary, 'n=127.0.0.1:12001', tcp=False)
            first['client_environment']['DFKV_RDMA_DEV'] = 'changed-device'
            self.assertEqual(second['client_environment']['DFKV_RDMA_DEV'], 'rxe0')
            self.assertEqual(arrival.smoke.CLIENT_ENV['DFKV_RDMA_DEV'], 'rxe0')
            with self.assertRaises(ValueError):
                arrival._validate_config(first)

    def supervise(self, command, **options):
        with mock.patch.object(arrival, '_command', return_value=command):
            return arrival.supervise(self.source, self.output, lib=self.binary,
                                      members='n=127.0.0.1:12000', **options)

    def fixture_command(self, suffix='', exitcode=None, *, rdma=False):
        return [sys.executable, '-I', '-c', f'''
import sys
sys.path.insert(0,{str(Path(__file__).parent)!r})
from test_eviction_arrival import {'RdmaAdapter as Adapter' if rdma else 'Adapter'}
import eviction_arrival as a
import eviction_trace as t
from pathlib import Path
from types import SimpleNamespace
p=Path({str(self.output)!r})
r=a.run(t.load(p/'trace.json'), t.load(p/'config.json'), library_loader=lambda _:SimpleNamespace(dfkv_get_auto_status=object()), client_factory=Adapter)
(p/'child-result.json').write_bytes(t.encode(r))
{suffix}
sys.exit({exitcode!r} if {exitcode!r} is not None else (0 if r['passed'] else 1))
''']

    def test_success_full_plan_logical_state_transport_and_owned_handles(self):
        before = trace.encode(self.document)
        result = self.run_client()
        summary = arrival._verify(self.document, result, self.config)
        self.assertTrue(result['passed'])
        self.assertEqual(summary['planned'], 8)
        self.assertEqual(summary['offered'], 8)
        self.assertEqual(summary['completed'], 8)
        self.assertEqual(summary['acked_put_bytes'], 12)
        self.assertEqual(summary['hit_get_bytes'], 12)
        self.assertEqual(len(result['handles']), 4)
        self.assertTrue(all(h['closed'] for h in result['handles']))
        self.assertTrue(all(len(c.threads) <= 1 for c in Adapter.instances))
        self.assertEqual(result['state_scope'], arrival.SCOPE)
        self.assertIn('acknowledged_objects', result['ledger']['phases'][0]['start_state'])
        self.assertNotIn('residents', result['ledger']['phases'][0]['start_state'])
        self.assertEqual(before, trace.encode(self.document))

    def test_rdma_config_success_and_strict_report_transport(self):
        config = arrival._config(self.binary, 'n=127.0.0.1:12001', tcp=False)
        self.assertEqual(config['client_environment']['DFKV_RDMA'], '1')
        self.assertEqual(config['client_environment']['DFKV_RDMA_DEV'], 'rxe0')
        result = arrival.run(self.document, config,
                             library_loader=lambda _: SimpleNamespace(dfkv_get_auto_status=object()),
                             client_factory=RdmaAdapter)
        self.assertTrue(result['passed'])
        self.assertEqual(arrival._verify(self.document, result, config)['completed'], 8)
        self.assertTrue(all(h['closed'] and h['transport'] == 'rdma' for h in result['handles']))
        for mode in ('tcp(rdma-failed)', arrival.TCP_MODE, 'rdma-fallback'):
            forged = copy.deepcopy(result)
            forged['handles'][0]['transport'] = mode
            with self.assertRaises(ValueError):
                arrival._verify(self.document, forged, config)
        forged = copy.deepcopy(result)
        forged['config']['client_environment']['DFKV_RDMA'] = '0'
        with self.assertRaises(ValueError):
            arrival._verify(self.document, forged, config)

    def test_rdma_child_ledger_audit_and_transport_configuration_forgery(self):
        report = self.supervise(self.fixture_command(rdma=True), tcp=False, process_timeout_sec=5)
        self.assertTrue(report['passed'], report)
        self.assertEqual(report['config']['client_environment']['DFKV_RDMA'], '1')
        receipt = arrival.baseline.file_sha(self.output / 'report.json')
        self.assertTrue(arrival.check(self.output, receipt)['passed'])
        document = arrival.load(self.output / 'trace.json')
        config = arrival.load(self.output / 'config.json')
        result = arrival.load(self.output / 'child-result.json')
        for value in ('0', 'invalid', None):
            with self.subTest(value=value):
                forged_config = copy.deepcopy(config)
                forged_config['client_environment']['DFKV_RDMA'] = value
                forged = copy.deepcopy(result)
                # Rewrite all duplicated config fields, not only one envelope.
                forged['config'] = forged_config
                forged['ledger']['config'] = forged_config
                with self.assertRaises(ValueError):
                    arrival._verify(document, forged, forged_config)

    def test_rdma_fallback_stops_before_any_request_and_closes_handle(self):
        config = arrival._config(self.binary, 'n=127.0.0.1:12001', tcp=False)
        result = self.run_client(config)
        arrival._verify(self.document, result, config)
        self.assertFalse(result['execution_started'])
        self.assertFalse(result['passed'])
        self.assertIn('observed RDMA', result['setup_error'])
        self.assertEqual(result['summary']['offered'], 0)
        self.assertEqual(result['summary']['planned'], 8)
        self.assertEqual(len(Adapter.instances), 1)
        self.assertTrue(Adapter.instances[0].closed)
        self.assertEqual(Adapter.instances[0].threads, set())

    def test_rdma_supervisor_environment_and_cli_forward_explicit_selection(self):
        with mock.patch.object(arrival.subprocess, 'Popen', side_effect=OSError('controlled no service')) as launch:
            report = arrival.supervise(self.source, self.output, lib=self.binary,
                                        members='n=127.0.0.1:12001', tcp=False)
        self.assertEqual(report['status'], 'supervisor_error')
        self.assertEqual(launch.call_args.kwargs['env']['DFKV_RDMA'], '1')
        with mock.patch.object(arrival, 'supervise', return_value={'passed': True}) as supervise:
            self.assertEqual(arrival.main([str(self.source), str(self.base / 'cli'),
                                          '--lib', str(self.binary), '--members',
                                          'n=127.0.0.1:12001', '--rdma']), 0)
        self.assertIs(supervise.call_args.kwargs['tcp'], False)
        for tcp in (None, 0, 'rdma'):
            with self.subTest(tcp=tcp), self.assertRaises(ValueError):
                arrival._config(self.binary, 'n=127.0.0.1:12001', tcp=tcp)

    def test_two_independent_worker_handles_execute_concurrently(self):
        Adapter.entered = {0: threading.Event(), 1: threading.Event()}
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.run_client)
            try:
                self.assertTrue(Adapter.entered[0].wait(2))
                self.assertTrue(Adapter.entered[1].wait(2))
            finally:
                Adapter.release.set()
            result = future.result(timeout=5)
        self.assertTrue(result['passed'])
        self.assertEqual(result['ledger']['summary']['peak_running'], 2)
        rows = result['ledger']['requests']
        self.assertNotEqual(rows[0]['worker_id'], rows[1]['worker_id'])
        self.assertLess(rows[1]['started_ns'], rows[0]['completed_ns'])

    def test_queue_wait_and_late_put_keep_slot_without_logical_ack(self):
        config = arrival._config(self.binary, 'n=127.0.0.1:12000', workers=1, timeout_ns=120000000)
        Adapter.durations = {0: .16}
        result = self.run_client(config)
        arrival._verify(self.document, result, config)
        self.assertFalse(result['passed'])
        rows = result['ledger']['requests']
        self.assertEqual(rows[0]['terminal'], 'timeout')
        self.assertEqual(rows[0]['backend_outcome'], 'put_ack')
        self.assertTrue(rows[0]['late_completion'])
        self.assertGreater(rows[1]['queue_wait_ns'], 0)
        self.assertGreaterEqual(rows[1]['started_ns'], rows[0]['completed_ns'])
        self.assertEqual(result['summary']['acked_put_bytes'], 3)
        self.assertTrue(all(c.closed and not c.active for c in Adapter.instances))
        self.assertEqual([p['status'] for p in result['ledger']['phases']], ['failed', 'blocked', 'blocked'])

    def test_queue_full_keeps_complete_planned_denominator(self):
        config = arrival._config(self.binary, 'n=127.0.0.1:12000', workers=1, queue_capacity=0)
        Adapter.durations = {0: .15}
        result = self.run_client(config)
        arrival._verify(self.document, result, config)
        self.assertEqual(result['ledger']['requests'][1]['terminal'], 'queue_full')
        self.assertEqual(result['summary']['planned'], 8)
        self.assertEqual(len(result['ledger']['requests']), 8)
        self.assertFalse(result['passed'])

    def test_measure_miss_is_known_absence_not_error_or_denominator_reduction(self):
        Adapter.outcomes = {5: 'get_miss'}
        result = self.run_client()
        arrival._verify(self.document, result, self.config)
        self.assertTrue(result['passed'])
        self.assertEqual(result['summary']['completed'], 8)
        self.assertEqual(result['summary']['terminal']['get_miss'], 1)
        self.assertEqual(result['summary']['hit_get_bytes'], 9)

    def test_warmup_miss_unresolved_and_native_errors_block_measurement(self):
        for outcome in ('get_miss', 'get_unresolved', RuntimeError('scalar GET failed: status=4'),
                        RuntimeError('wrong bytes/length: mismatch')):
            with self.subTest(outcome=outcome):
                Adapter.instances, Adapter.store, Adapter.outcomes = [], {}, {2: outcome}
                result = self.run_client()
                arrival._verify(self.document, result, self.config)
                self.assertFalse(result['passed'])
                row = result['ledger']['requests'][2]
                self.assertEqual(row['terminal'], 'backend_error' if isinstance(outcome, Exception) else outcome)
                if isinstance(outcome, Exception):
                    self.assertIn(str(outcome), row['backend_error'])
                    self.assertNotEqual(row['terminal'], 'get_miss')
                self.assertEqual(result['ledger']['phases'][2]['status'], 'blocked')
                self.assertEqual(result['summary']['planned'], 8)

    def test_partial_open_failure_and_missing_status_symbol_never_offer_work(self):
        Adapter.fail_open = 2
        result = self.run_client()
        arrival._verify(self.document, result, self.config)
        self.assertFalse(result['passed'])
        self.assertFalse(result['execution_started'])
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(len(result['unattempted_requests']), 8)
        self.assertTrue(all(c.closed for c in Adapter.instances))
        Adapter.instances = []
        result = self.run_client(library=object())
        arrival._verify(self.document, result, self.config)
        self.assertIn('status symbol', result['setup_error'])
        self.assertEqual(result['handles'], [])

    def test_close_failure_cannot_turn_a_successful_ledger_into_pass(self):
        Adapter.close_error = True
        result = self.run_client()
        arrival._verify(self.document, result, self.config)
        self.assertTrue(result['ledger']['passed'])
        self.assertFalse(result['passed'])
        self.assertTrue(result['close_errors'])

    def test_bad_config_identity_transport_and_timing_are_rejected(self):
        for name, value in (('workers', True), ('queue_capacity', -1), ('timeout_ns', False)):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    arrival._config(self.binary, 'n=127.0.0.1:12000', **{name: value})
        result = self.run_client()
        mutations = [lambda r:r.update(state_scope='actual-resident-snapshot'),
                     lambda r:r['handles'][0].update(worker_id=True),
                     lambda r:r['handles'][0].update(transport='rdma'),
                     lambda r:r['ledger']['requests'][0].update(deadline_ns=0),
                     lambda r:r['ledger']['requests'][0].update(service_ns=0),
                     lambda r:r['ledger']['requests'][0].update(queue_wait_ns=-1),
                     lambda r:r['ledger']['requests'][0].update(late_completion=True),
                     lambda r:r['summary'].update(completed=0),
                     lambda r:r['ledger']['requests'].pop()]
        for mutate in mutations:
            forged = copy.deepcopy(result)
            mutate(forged)
            with self.assertRaises(ValueError):
                arrival._verify(self.document, forged, self.config)

    def test_summary_peaks_and_drain_forgery_are_rejected(self):
        result = self.run_client()
        for field, value in (('drained_ns', 0), ('peak_running', 999),
                             ('peak_reserved', 999), ('peak_queue', 999),
                             ('peak_running', True), ('peak_queue', -1)):
            with self.subTest(field=field, value=value):
                forged = copy.deepcopy(result)
                forged['ledger']['summary'][field] = value
                with self.assertRaises(ValueError):
                    arrival._verify(self.document, forged, self.config)
        forged = copy.deepcopy(result)
        forged['ledger']['summary']['invented'] = 0
        with self.assertRaises(ValueError):
            arrival._verify(self.document, forged, self.config)

    def test_only_explicit_tcp_transport_label_is_accepted(self):
        class Unexpected(Adapter):
            def __init__(self, *args):
                super().__init__(*args)
                self.transport = 'tcp(rdma-failed)'
        result = arrival.run(self.document, self.config,
                             library_loader=lambda _: SimpleNamespace(dfkv_get_auto_status=object()),
                             client_factory=Unexpected)
        self.assertFalse(result['passed'])
        self.assertFalse(result['execution_started'])
        self.assertIn('observed TCP', result['setup_error'])
        self.assertEqual(result['summary']['offered'], 0)

    def test_library_tool_drift_and_banned_paths_rejected_before_open(self):
        self.binary.write_bytes(b'changed')
        with self.assertRaises(ValueError):
            self.run_client()
        with self.assertRaises(ValueError):
            arrival._config('/mnt/disk1/never-open.so', 'n=127.0.0.1:12000')
        link = self.base / 'link.json'
        link.symlink_to('/mnt/disk1/never-read.json')
        with mock.patch.object(arrival.trace, 'load') as loader:
            with self.assertRaises(ValueError):
                arrival.load(link)
            loader.assert_not_called()

    def test_actual_child_success_artifacts_and_external_audit(self):
        report = self.supervise(self.fixture_command(), process_timeout_sec=5)
        self.assertTrue(report['passed'], report)
        self.assertEqual(report['completed_requests'], 8)
        self.assertEqual(report['offered_requests'], 8)
        self.assertEqual(report['hit_get_bytes'], 12)
        self.assertTrue(report['reaped'])
        receipt = arrival.baseline.file_sha(self.output / 'report.json')
        self.assertTrue(arrival.check(self.output, receipt)['passed'])
        (self.output / 'child-result.json').write_bytes(b'{}\n')
        with self.assertRaises(ValueError):
            arrival.check(self.output, receipt)

    def test_timeout_and_crash_preserve_unknown_not_zero_and_reap(self):
        for name, command in [('timeout', [sys.executable, '-I', '-c', 'import time;time.sleep(60)']),
                              ('crash', [sys.executable, '-I', '-c', 'import os;os._exit(7)'])]:
            with self.subTest(name=name):
                self.output = self.base / name
                report = self.supervise(command, process_timeout_sec=.3)
                self.assertFalse(report['passed'])
                self.assertEqual(report['request_outcomes'], 'unknown')
                self.assertEqual(report['planned_requests'], 8)
                self.assertTrue(all(report[k] is None for k in arrival.COUNTERS))
                self.assertTrue(report['reaped'])
                receipt = arrival.baseline.file_sha(self.output / 'report.json')
                self.assertFalse(arrival.check(self.output, receipt)['passed'])

    def test_missing_forged_result_or_exit_contradiction_stays_unknown(self):
        for index, command in enumerate(([sys.executable, '-I', '-c', 'pass'],
                self.fixture_command("r['summary']['completed']=0;(p/'child-result.json').write_bytes(t.encode(r))"),
                self.fixture_command(exitcode=1))):
            with self.subTest(index=index):
                self.output = self.base / f'bad-{index}'
                # fixture commands embed the output path; rebuild where necessary.
                if index == 1:
                    command = self.fixture_command("r['summary']['completed']=0;(p/'child-result.json').write_bytes(t.encode(r))")
                elif index == 2:
                    command = self.fixture_command(exitcode=1)
                report = self.supervise(command, process_timeout_sec=5)
                self.assertEqual(report['status'], 'invalid_result')
                self.assertEqual(report['request_outcomes'], 'unknown')
                self.assertIsNone(report['completed_requests'])

    def test_output_is_exclusive_and_unknown_counter_forgery_fails_audit(self):
        report = self.supervise([sys.executable, '-I', '-c', 'import os;os._exit(7)'])
        with self.assertRaises(ValueError):
            self.supervise([sys.executable, '-I', '-c', 'pass'])
        report['completed_requests'] = 0
        (self.output / 'report.json').write_bytes(trace.encode(report))
        with self.assertRaises(ValueError):
            arrival.check(self.output, arrival.baseline.file_sha(self.output / 'report.json'))

    def test_supervisor_spawn_failure_environment_and_interrupt_save_report(self):
        with mock.patch.object(arrival.subprocess, 'Popen', side_effect=OSError('spawn failure')) as launch:
            report = arrival.supervise(self.source, self.output, lib=self.binary, members='n=127.0.0.1:12000')
        self.assertEqual(report['status'], 'supervisor_error')
        env = launch.call_args.kwargs['env']
        self.assertEqual(env['DFKV_RDMA'], '0')
        self.assertIsNone(report['completed_requests'])
        self.output = self.base / 'interrupt'
        with mock.patch.object(arrival.subprocess, 'Popen', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                arrival.supervise(self.source, self.output, lib=self.binary, members='n=127.0.0.1:12000')
        self.assertTrue((self.output / 'report.json').is_file())


if __name__ == '__main__':
    unittest.main()
