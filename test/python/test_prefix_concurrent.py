"""Threaded fake adapters prove diagnostic contracts, never hardware behavior."""
import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'deploy/validation'))
import prefix_concurrent as concurrent
from prefix_chain import _seal


class Factory:
    def __init__(self, *, miss=None, error=None, sleep=0, close_error=None,
                 setup_error=None, transport='fake-rdma-test', gate=None):
        self.values = {}
        self.lock = threading.Lock()
        self.backends = []
        self.miss = miss
        self.error = error
        self.sleep = sleep
        self.close_error = close_error
        self.setup_error = setup_error
        self.transport = transport
        self.gate = gate
        self.active = self.peak = 0

    def __call__(self, worker):
        if worker == self.setup_error:
            raise RuntimeError('injected setup error')
        result = Backend(self, worker)
        self.backends.append(result)
        return result


class Backend:
    def __init__(self, factory, worker):
        self.factory, self.worker = factory, worker
        self.transport = factory.transport
        self.closed = False
        self.owner = None
        self.last_call_timing = None
        self.calls = 0

    def reset_call_timing(self):
        self.last_call_timing = None

    def call(self, op, obj, value):
        self.assert_owner()
        self.calls += 1
        f = self.factory
        started = time.monotonic_ns()
        if self.worker >= 0:
            with f.lock:
                f.active += 1
                f.peak = max(f.peak, f.active)
            if f.gate and self.calls == 1:
                f.gate.wait(timeout=3)
            if f.sleep:
                time.sleep(f.sleep)
        try:
            if self.worker >= 0 and f.error == op:
                raise RuntimeError('injected native failure')
            with f.lock:
                if op == 'put':
                    f.values[obj['key_hex']] = value
                    return 'ack'
                actual = f.values.get(obj['key_hex'])
                if self.worker >= 0 and obj['object_id'] == f.miss:
                    actual = None
                return ('miss', None) if actual is None else ('hit', actual)
        finally:
            self.last_call_timing = (started, time.monotonic_ns())
            if self.worker >= 0:
                with f.lock:
                    f.active -= 1

    def assert_owner(self):
        if self.closed:
            raise AssertionError('call after close')
        owner = threading.get_ident()
        if self.owner is None:
            self.owner = owner
        elif self.owner != owner:
            raise AssertionError('shared handle across workers')

    def put(self, obj, value):
        return self.call('put', obj, value)

    def get(self, obj, expected):
        return self.call('get', obj, expected)

    def close(self):
        if self.factory.active:
            # Initializer closes before workers start; worker handles close only
            # after ALL physical calls finish, including late calls.
            raise AssertionError('closed while physical calls still active')
        if self.worker == self.factory.close_error:
            raise RuntimeError('injected close failure')
        self.closed = True


class ConcurrentTest(unittest.TestCase):
    def plan(self, **kwargs):
        return concurrent.generate(**dict(dict(workers=2, rounds=4, interval_ns=10000000,
                                               prefill=28, arrival_batch=1, length=32,
                                               timeout_ns=1000000000), **kwargs))

    def replay(self, plan=None, factory=None):
        plan = plan or self.plan()
        factory = factory or Factory()
        report = concurrent.replay(plan, factory)
        self.assertEqual(concurrent.check(plan, report), report['summary'])
        return plan, report, factory

    def test_deterministic_plan_and_bounds(self):
        plan = self.plan()
        self.assertEqual(plan, self.plan())
        self.assertEqual(concurrent.validate(plan), plan['config'])
        for key, value in [('workers', True), ('workers', 17), ('rounds', 0),
                           ('interval_ns', 999999), ('timeout_ns', 0), ('length', 4097),
                           ('prefill', 11), ('arrival_batch', 65), ('seed', -1), ('extra', 0)]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.plan(**{key: value})
        with self.assertRaises(ValueError):
            self.plan(workers=16, rounds=600, arrival_batch=64)
        concurrent.generate(workers=16, rounds=600, arrival_batch=8, timeout_ns=5000000000)

    def test_plan_checksum_and_extra_fields_rejected(self):
        plan = self.plan()
        plan['config']['rounds'] = 3
        with self.assertRaises(ValueError):
            concurrent.validate(plan)
        plan = self.plan()
        plan['extra'] = True
        with self.assertRaises(ValueError):
            concurrent.validate(_seal(plan))

    def test_full_prefix_denominators_and_native_phase_accounting(self):
        plan, report, factory = self.replay()
        summary = report['summary']
        self.assertTrue(summary['passed'])
        self.assertEqual(summary['planned_requests'], 8)
        self.assertEqual(summary['completed_requests'], 8)
        self.assertEqual(summary['full_prefix_hits'], 8)
        self.assertEqual(summary['unknown_requests'], 0)
        self.assertEqual(summary['token_reuse_ratio'], dict(numerator=1, denominator=1))
        self.assertEqual(summary['native']['put_acks'], 36)
        self.assertEqual(summary['native']['put_bytes'], 36*32)
        self.assertEqual(summary['by_phase']['initialization']['put_acks'], 28)
        self.assertEqual(summary['by_phase']['warmup']['get_hits'], 12)
        self.assertEqual(summary['by_phase']['measurement']['get_hits'], 52)
        self.assertEqual(summary['refill_put_acks'], 0)
        self.assertTrue(all(backend.closed for backend in factory.backends))
        self.assertEqual([item['worker_id'] for item in report['handles']], [-1, 0, 1])
        for row in report['requests']:
            self.assertGreaterEqual(row['started_ns'], row['scheduled_ns'])
        self.assertEqual({row['kind'] for row in report['requests']},
                         {'hot-long', 'hot-branch', 'hot-short', 'initialized-rotating'})

    def test_real_threads_independent_handles_and_overlap(self):
        factory = Factory(gate=threading.Barrier(4), sleep=.001)
        _, report, _ = self.replay(self.plan(workers=4, rounds=1, arrival_batch=0), factory)
        self.assertTrue(report['summary']['passed'])
        self.assertEqual(factory.peak, 4)
        self.assertEqual(report['summary']['peak_backend_overlap'], 4)
        self.assertEqual(report['summary']['peak_abi_overlap'], 4)
        self.assertEqual(report['summary']['peak_get_abi_overlap'], 4)
        self.assertEqual(report['summary']['peak_put_abi_overlap'], 0)
        self.assertEqual(report['summary']['abi_timed_measurement_calls'], {'get': 32, 'put': 0})
        # These ABI brackets come from a fake adapter, NOT a CDLL/hardware run.
        owners = {backend.owner for backend in factory.backends if backend.worker >= 0}
        self.assertEqual(len(owners), 4)

    def test_partial_prefix_frozen_before_suffix_refill(self):
        _, report, _ = self.replay(self.plan(workers=1, rounds=1, arrival_batch=0), Factory(miss=3))
        row = report['requests'][0]
        self.assertEqual(row['usable_tokens'], 48)
        self.assertEqual(row['confirmed_prefix_tokens'], 48)
        self.assertEqual(row['outcomes'], ['hit', 'hit', 'hit', 'miss']+['not_attempted']*4)
        self.assertEqual(row['refill_put_acks'], 5)
        self.assertEqual(row['refill_payload_bytes'], 160)
        self.assertEqual([event['op'] for event in row['events']], ['get']*4+['put']*5)
        self.assertEqual(report['summary']['full_prefix_hits'], 0)

    def test_shared_identities_immutable_generation_and_unique_arrivals(self):
        plan, report, _ = self.replay()
        identities = [event['object_id'] for row in report['requests'] for event in row['events']
                      if event['kind'] == 'arrival']
        self.assertEqual(len(set(identities)), 8)
        self.assertEqual(sorted(identities), list(range(28, 36)))
        self.assertEqual(report['requests'][0]['object_ids'], report['requests'][4]['object_ids'])
        for index in range(36):
            obj = concurrent.object_for(plan, index)
            self.assertEqual(obj['generation'], 0)
            self.assertEqual(concurrent.payload(plan, obj), concurrent.payload(plan, obj))
        with self.assertRaises(ValueError):
            concurrent.object_for(plan, 36)

    def test_get_failure_is_not_a_miss_and_preserves_future_denominators(self):
        _, report, _ = self.replay(factory=Factory(error='get'))
        summary = report['summary']
        self.assertFalse(summary['passed'])
        self.assertEqual(summary['planned_requests'], 8)
        self.assertEqual(summary['unknown_requests'], 8)
        self.assertEqual(summary['native']['get_errors'], 2)
        self.assertEqual(summary['native']['get_misses'], 0)
        self.assertIsNone(summary['token_reuse_ratio'])
        self.assertEqual(summary['states'], {'failed': 2, 'not_attempted': 6})

    def test_put_exception_is_unknown_not_ack(self):
        _, report, _ = self.replay(factory=Factory(error='put'))
        self.assertFalse(report['summary']['passed'])
        self.assertEqual(report['summary']['native']['put_unknown'], 2)
        self.assertEqual(report['summary']['native']['put_acks'], 28)
        self.assertEqual(report['summary']['by_phase']['measurement']['get_attempts'], 0)

    def test_late_physical_call_drains_before_close_and_blocks_later_calls(self):
        _, report, factory = self.replay(self.plan(rounds=3, arrival_batch=0, timeout_ns=1000000),
                                        Factory(sleep=.02))
        self.assertFalse(report['summary']['passed'])
        self.assertEqual(report['summary']['states'], {'late': 2, 'not_attempted': 4})
        self.assertEqual(report['summary']['native']['get_hits'], 14)
        self.assertTrue(all(backend.closed for backend in factory.backends))
        for row in report['requests']:
            if row['state'] == 'late':
                self.assertGreater(row['finished_ns'], row['deadline_ns'])
                self.assertIsNone(row['usable_tokens'])
                self.assertEqual(len(row['events']), 1)

    def test_expired_unstarted_round_never_calls_backend(self):
        plan = self.plan(workers=1, rounds=1, timeout_ns=1)
        _, report, _ = self.replay(plan)
        row = report['requests'][0]
        self.assertEqual(row['state'], 'expired')
        self.assertEqual(row['events'], [])
        self.assertEqual(report['summary']['by_phase']['measurement']['get_attempts'], 0)
        self.assertEqual(report['summary']['unknown_requests'], 1)

    def test_preparation_crossing_deadline_never_starts_backend(self):
        plan = self.plan(workers=1, rounds=1, arrival_batch=0, timeout_ns=1000000)
        original = concurrent.payload
        def delayed_payload(document, obj):
            if threading.current_thread().name.startswith('prefix-worker-'):
                time.sleep(.01)
            return original(document, obj)
        with mock.patch.object(concurrent, 'payload', side_effect=delayed_payload):
            report = concurrent.replay(plan, Factory())
        concurrent.check(plan, report)
        row = report['requests'][0]
        self.assertEqual(row['state'], 'expired')
        self.assertEqual(row['events'][0]['status'], 'expired')
        self.assertIsNone(row['events'][0]['abi_started_ns'])
        self.assertEqual(report['summary']['by_phase']['measurement']['get_attempts'], 0)

    def test_setup_failure_closes_already_opened_handles(self):
        _, report, factory = self.replay(factory=Factory(setup_error=1))
        self.assertFalse(report['summary']['passed'])
        self.assertIn('setup error', report['setup_error'])
        self.assertEqual(report['summary']['states'], {'not_attempted': 8})
        self.assertTrue(all(backend.closed for backend in factory.backends))

    def test_transport_fallback_rejected_and_closed(self):
        _, report, factory = self.replay(factory=Factory(transport='tcp'))
        self.assertFalse(report['summary']['passed'])
        self.assertIn('RDMA transport required', report['setup_error'])
        self.assertEqual(report['initial_events'], [])
        self.assertTrue(factory.backends[0].closed)

    def test_close_failure_cannot_report_passed(self):
        _, report, _ = self.replay(factory=Factory(close_error=1))
        self.assertFalse(report['summary']['passed'])
        self.assertEqual(report['summary']['unknown_requests'], 0)
        self.assertEqual(report['handles'][-1]['closed'], False)
        self.assertIn('close failure', report['handles'][-1]['close_error'])

    def test_initializer_failure_keeps_every_request(self):
        factory = Factory()
        original = factory.__call__
        def construct(worker):
            backend = original(worker)
            if worker == -1:
                backend.put = lambda obj, value: 'unknown'
            return backend
        plan = self.plan()
        report = concurrent.replay(plan, construct)
        concurrent.check(plan, report)
        self.assertFalse(report['summary']['passed'])
        self.assertEqual(len(report['initial_events']), 1)
        self.assertEqual(report['summary']['planned_requests'], 8)
        self.assertEqual(report['summary']['unknown_requests'], 8)
        self.assertTrue(factory.backends[0].closed)

    def test_partial_thread_start_failure_aborts_barrier_and_joins(self):
        original = threading.Thread.start
        def start(thread):
            if thread.name == 'prefix-worker-1':
                raise RuntimeError('injected thread start failure')
            return original(thread)
        with mock.patch.object(threading.Thread, 'start', start), contextlib.redirect_stderr(io.StringIO()):
            _, report, factory = self.replay()
        self.assertIn('thread start failure', report['setup_error'])
        self.assertFalse(report['summary']['passed'])
        self.assertTrue(all(backend.closed for backend in factory.backends))
        self.assertFalse(any(thread.name.startswith('prefix-worker-') for thread in threading.enumerate()))

    def test_missing_duplicate_tampered_event_request_summary_rejected(self):
        plan, original, _ = self.replay()
        variants = []
        value = copy.deepcopy(original)
        value['requests'].pop()
        variants.append(value)
        value = copy.deepcopy(original)
        value['requests'][0]['events'].append(value['requests'][0]['events'][0])
        variants.append(value)
        value = copy.deepcopy(original)
        value['requests'][0]['events'][1]['object_id'] += 1
        variants.append(value)
        value = copy.deepcopy(original)
        value['requests'][0]['events'][1]['observed_sha256'] = '0'*64
        variants.append(value)
        value = copy.deepcopy(original)
        value['summary']['known_usable_tokens'] += 1
        variants.append(value)
        value = copy.deepcopy(original)
        value['requests'][0]['events'][0]['abi_finished_ns'] = value['requests'][0]['events'][0]['finished_ns']+1
        variants.append(value)
        for variant in variants:
            with self.assertRaises(ValueError):
                concurrent.check(plan, _seal(variant))

    def test_corrupt_get_bytes_fail_and_do_not_become_miss(self):
        factory = Factory()
        original = factory.__call__
        def construct(worker):
            backend = original(worker)
            if worker >= 0:
                backend.get = lambda obj, expected: ('hit', b'wrong')
            return backend
        plan = self.plan(workers=1, rounds=1, arrival_batch=0)
        report = concurrent.replay(plan, construct)
        concurrent.check(plan, report)
        self.assertFalse(report['passed'])
        self.assertEqual(report['summary']['native']['get_errors'], 1)
        self.assertEqual(report['summary']['native']['get_misses'], 0)
        self.assertIn('payload mismatch', report['requests'][0]['events'][0]['error'])

    def test_refill_failure_keeps_confirmed_prefix_but_exact_request_unknown(self):
        _, report, _ = self.replay(self.plan(workers=1, rounds=1, arrival_batch=0),
                                  Factory(miss=3, error='put'))
        row = report['requests'][0]
        self.assertEqual(row['state'], 'failed')
        self.assertIsNone(row['usable_tokens'])
        self.assertEqual(row['confirmed_prefix_tokens'], 48)
        self.assertEqual(row['refill_put_acks'], 0)
        self.assertEqual(report['summary']['native']['put_unknown'], 1)
        self.assertFalse(report['passed'])

    def test_ack_with_error_rejected_even_after_resealing(self):
        plan, report, _ = self.replay()
        report['initial_events'][0]['error'] = 'impossible successful ACK error'
        with self.assertRaisesRegex(ValueError, 'carries an error'):
            concurrent.check(plan, _seal(report))

    def test_timed_library_facade_does_not_modify_shared_functions(self):
        class Library:
            def dfkv_get_auto_status(self, result):
                return result
            def dfkv_batch_put(self, result):
                return result
            dfkv_close = object()
        library = Library()
        first = concurrent._TimedLibrary(library)
        second = concurrent._TimedLibrary(library)
        self.assertEqual(first.dfkv_get_auto_status(17), 17)
        self.assertIsNotNone(first.timing)
        self.assertIsNone(second.timing)
        self.assertIs(first.dfkv_close, library.dfkv_close)
        self.assertEqual(library.dfkv_batch_put(9), 9)
        self.assertEqual(second.dfkv_batch_put(4), 4)
        self.assertIsNotNone(second.timing)

    def test_reused_backend_instance_rejected_without_double_close(self):
        factory = Factory()
        backend = factory(-1)
        plan = self.plan()
        report = concurrent.replay(plan, lambda worker: backend)
        concurrent.check(plan, report)
        self.assertFalse(report['passed'])
        self.assertIn('reused a handle instance', report['setup_error'])
        self.assertEqual(len(report['handles']), 1)
        self.assertTrue(backend.closed)

    def test_mixed_backend_transports_rejected(self):
        factory = Factory()
        def construct(worker):
            backend = factory(worker)
            if worker >= 0:
                backend.transport = 'rdma'
            return backend
        plan = self.plan()
        report = concurrent.replay(plan, construct)
        concurrent.check(plan, report)
        self.assertFalse(report['passed'])
        self.assertIn('mixed backend transports', report['setup_error'])
        self.assertTrue(all(backend.closed for backend in factory.backends))

    def test_rdma_success_requires_every_direct_abi_bracket(self):
        plan, report, _ = self.replay(factory=Factory(transport='rdma'))
        self.assertTrue(report['passed'])
        report['requests'][0]['events'][0]['abi_started_ns'] = None
        report['requests'][0]['events'][0]['abi_finished_ns'] = None
        with self.assertRaisesRegex(ValueError, 'lacks direct ABI timing'):
            concurrent.check(plan, _seal(report))

    def test_128mib_loader_writer_and_strict_json(self):
        self.assertEqual(concurrent.MAX_FILE_BYTES, 128*1024*1024)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = {'large': 'x'*(17*1024*1024)}
            concurrent._write(root/'large.json', document)
            self.assertGreater((root/'large.json').stat().st_size, 16*1024*1024)
            self.assertEqual(concurrent.load(root/'large.json'), document)
            with self.assertRaises(FileExistsError):
                concurrent._write(root/'large.json', {})
            with mock.patch.object(concurrent, 'MAX_FILE_BYTES', 32):
                with self.assertRaises(ValueError):
                    concurrent._write(root/'overflow.json', {'value': 'x'*32})
            import prefix_delayed
            with mock.patch.object(prefix_delayed, 'MAX_FILE_BYTES', 32):
                with self.assertRaises(ValueError):
                    concurrent.load(root/'large.json')
            for text in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}'):
                (root/'invalid.json').write_text(text)
                with self.assertRaises(ValueError):
                    concurrent.load(root/'invalid.json')

    def test_isolated_cli_resolves_common_package_without_pythonpath(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            concurrent._write(root/'plan.json', self.plan(workers=1, rounds=1))
            process = subprocess.run([sys.executable, '-I', concurrent.__file__, 'run',
                                      '--plan', str(root/'plan.json'), '--output', str(root/'out'),
                                      '--lib', str(root/'nonexistent.so'), '--members', 'n=127.0.0.1:12001'],
                                     capture_output=True, text=True, timeout=10)
            self.assertEqual(process.returncode, 1)
            self.assertIn('nonexistent.so', process.stderr)
            self.assertNotIn('ModuleNotFoundError', process.stderr)
            self.assertNotIn('Traceback', process.stderr)

    def test_cli_roundtrip_check_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = ['generate', '--output', str(root/'plan.json'), '--workers', '1', '--rounds', '1',
                    '--prefill', '12', '--arrival-batch', '0', '--length', '8']
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(concurrent.main(args), 0)
                self.assertEqual(concurrent.main(args), 1)
            plan = json.loads((root/'plan.json').read_text())
            report = concurrent.replay(plan, Factory())
            (root/'report.json').write_text(json.dumps(report))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(concurrent.main(['check', '--plan', str(root/'plan.json'),
                                                  '--ledger', str(root/'report.json')]), 0)


if __name__ == '__main__':
    unittest.main()
