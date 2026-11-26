"""Clock-driven synthetic adapters only; no service or accelerated production ages."""
import copy
import contextlib
import io
import json
import tempfile
from unittest import mock
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'deploy/validation'))
import prefix_delayed as delayed
from prefix_chain import _seal


class Clock:
    def __init__(self):
        self.now=100*delayed.NS
    def clock(self):
        return self.now
    def sleep(self,seconds):
        self.now+=round(seconds*delayed.NS)


class Backend:
    transport='fake-rdma-test'
    def __init__(self,clock,fail=None,capacity=None):
        self.clock=clock
        self.values={}
        self.fail=fail
        self.capacity=capacity
        self.calls=0
    def put_many(self,objects,values):
        self.calls+=1
        self.clock.now+=1000
        if self.fail==('put',self.calls):
            return 'unknown'
        for obj,value in zip(objects,values):
            self.values[obj['key_hex']]=value
            if self.capacity is not None and len(self.values)>self.capacity:
                del self.values[next(iter(self.values))]
        return 'ack'
    def get(self,obj,expected):
        self.calls+=1
        self.clock.now+=1000
        if self.fail==('get',self.calls):
            return 'error',None
        value=self.values.get(obj['key_hex'])
        return ('miss',None) if value is None else ('hit',value)


class DelayedTest(unittest.TestCase):
    def plan(self,**kwargs):
        return delayed.generate(**dict(dict(prefill=24,steps=12,arrival_batch=24,
                                             delays_sec=[1,3,7],hot_interval_sec=2,length=32),**kwargs))
    def run_plan(self,plan=None,**backend_args):
        plan=plan or self.plan()
        clock=Clock()
        backend=Backend(clock,**backend_args)
        report=delayed.replay(plan,backend,clock=clock.clock,sleep=clock.sleep)
        return plan,report,backend
    def test_deterministic_strict_plan(self):
        plan=self.plan()
        self.assertEqual(plan,self.plan())
        delayed.validate(plan)
        for field,value in [('steps',True),('arrival_batch',23),('delays_sec',[1,1,2]),('interval_sec',2)]:
            with self.assertRaises(ValueError):
                self.plan(**{field:value})
        altered=copy.deepcopy(plan)
        altered['config']['extra']=1
        with self.assertRaises(ValueError):
            delayed.validate(_seal(altered))
    def test_full_enrollment_ack_due_censor_and_hot_denominators(self):
        plan,report,backend=self.run_plan()
        self.assertTrue(report['passed'])
        summary=delayed.check(plan,report)
        self.assertEqual(summary['planned_chains'],36)
        self.assertEqual(summary['enrolled_chains'],36)
        self.assertEqual(summary['arrival_batches_acked'],12)
        self.assertEqual(summary['planned_requests'],54)
        self.assertEqual(summary['by_kind']['hot-long']['planned'],6)
        self.assertEqual(summary['by_delay']['1']['completed'],11)
        self.assertEqual(summary['by_delay']['3']['completed'],9)
        self.assertEqual(summary['by_delay']['7']['completed'],5)
        self.assertEqual(summary['censored_chains'],11)
        for row in report['requests']:
            if row['kind']=='delayed' and row['state']=='completed':
                self.assertGreaterEqual(row['age_ns'],row['delay_sec']*delayed.NS)
                self.assertLessEqual(row['age_ns'],(row['delay_sec']+2)*delayed.NS)
                self.assertEqual(row['age_ns'],row['started_ns']-row['ack_ns'])
        self.assertEqual(summary['native']['get_errors'],0)
        self.assertEqual(summary['native']['put_bytes'],summary['native']['put_acks']*32)
    def test_first_prefix_freezes_before_refill(self):
        plan,report,_=self.run_plan(capacity=24)
        self.assertTrue(report['passed'])
        delayed.check(plan,report)
        rows=[row for row in report['requests'] if row['kind'].startswith('hot-') and row.get('refill_put_acks')]
        self.assertTrue(rows)
        for row in rows:
            self.assertLess(row['usable_tokens'],row['target_tokens'])
            self.assertEqual(row['refill_put_acks'],len(row['object_ids'])-row['first_unconfirmed_block'])
            self.assertEqual(row['outcomes'][row['first_unconfirmed_block']],'miss')
    def test_unknown_batch_aborts_and_preserves_fixed_denominators(self):
        plan,report,backend=self.run_plan(fail=('put',1))
        self.assertFalse(report['passed'])
        summary=delayed.check(plan,report)
        self.assertEqual(backend.calls,1)
        self.assertEqual(summary['native']['put_unknown'],24)
        self.assertEqual(summary['native']['put_acks'],0)
        self.assertEqual(summary['planned_requests'],54)
        self.assertEqual(summary['by_kind']['delayed']['not_attempted'],36)
        self.assertEqual(summary['by_kind']['hot-long']['not_attempted'],6)
        self.assertIsNone(summary['by_kind']['hot-long']['token_reuse_ratio'])
    def test_get_error_not_cache_miss(self):
        plan,report,_=self.run_plan(fail=('get',2))
        self.assertFalse(report['passed'])
        summary=delayed.check(plan,report)
        self.assertEqual(summary['native']['get_errors'],1)
        self.assertEqual(summary['native']['get_misses'],0)
    def test_warmup_native_error_or_unknown_preserves_first_failure(self):
        for status in ('error','unknown'):
            with self.subTest(status=status):
                clock=Clock()
                backend=Backend(clock)
                def failed_get(obj,expected):
                    backend.calls+=1
                    return status,None
                backend.get=failed_get
                plan=self.plan()
                report=delayed.replay(plan,backend,clock=clock.clock,sleep=clock.sleep)
                self.assertFalse(report['passed'])
                delayed.check(plan,report)
                self.assertEqual(report['failure'],dict(reason='native_failure',event_index=1,status=status))
                self.assertEqual(backend.calls,2)
                self.assertEqual(len(report['events']),2)
                self.assertEqual(report['events'][-1]['kind'],'warmup')
    def test_genuine_warmup_miss_still_fails_without_refill(self):
        clock=Clock()
        backend=Backend(clock)
        def missing_get(obj,expected):
            backend.calls+=1
            return 'miss',None
        backend.get=missing_get
        plan=self.plan()
        report=delayed.replay(plan,backend,clock=clock.clock,sleep=clock.sleep)
        self.assertFalse(report['passed'])
        delayed.check(plan,report)
        self.assertEqual(report['failure'],dict(reason='warmup_miss'))
        self.assertEqual(backend.calls,2)
        self.assertEqual([event['op'] for event in report['events']],['put','get'])
        self.assertEqual(report['requests'][0]['refill_put_acks'],0)
    def test_late_first_get_native_error_or_unknown_remains_original_cause(self):
        for status in ('error','unknown'):
            with self.subTest(status=status):
                clock=Clock()
                backend=Backend(clock)
                put=backend.put_many
                get=backend.get
                def slow_second_arrival(objects,values):
                    result=put(objects,values)
                    if objects[0]['object_id']==48:
                        clock.now+=3*delayed.NS
                    return result
                def failed_new_get(obj,expected):
                    if obj['object_id']>=24:
                        backend.calls+=1
                        return status,None
                    return get(obj,expected)
                backend.put_many=slow_second_arrival
                backend.get=failed_new_get
                plan=self.plan()
                report=delayed.replay(plan,backend,clock=clock.clock,sleep=clock.sleep)
                self.assertFalse(report['passed'])
                delayed.check(plan,report)
                self.assertEqual(report['failure'],dict(reason='native_failure',event_index=len(report['events'])-1,status=status))
                self.assertEqual(report['events'][-1]['kind'],'delayed')
                self.assertEqual(len(report['events']),backend.calls)
                rows=[row for row in report['requests'] if row['kind']=='delayed' and row['state']=='failed']
                self.assertEqual(len(rows),1)
                self.assertGreater(rows[0]['age_ns'],(rows[0]['delay_sec']+2)*delayed.NS)
                self.assertEqual(rows[0]['event_end']-rows[0]['event_start'],1)
                self.assertEqual(rows[0]['outcomes'][0],status)
                self.assertIsNone(rows[0]['usable_tokens'])
    def test_wrong_payload_fails_closed(self):
        clock=Clock()
        backend=Backend(clock)
        backend.get=lambda obj,expected:('hit',b'x')
        plan=self.plan()
        report=delayed.replay(plan,backend,clock=clock.clock,sleep=clock.sleep)
        self.assertFalse(report['passed'])
        self.assertEqual(delayed.check(plan,report)['native']['get_errors'],1)
    def test_missed_tick_is_failure_not_reduced_successful_load(self):
        clock=Clock()
        backend=Backend(clock)
        def late_sleep(seconds):
            clock.now+=round((seconds+4)*delayed.NS)
        plan=self.plan()
        report=delayed.replay(plan,backend,clock=clock.clock,sleep=late_sleep)
        self.assertFalse(report['passed'])
        self.assertEqual(report['failure']['reason'],'arrival_deadline')
        summary=delayed.check(plan,report)
        self.assertEqual(summary['planned_requests'],54)
        self.assertLess(summary['arrival_batches_acked'],12)
    def test_rehashed_raw_and_accounting_tamper_rejected(self):
        plan,report,_=self.run_plan()
        changes=[lambda r:r['events'][0].update(ids=[999]),
                 lambda r:r['events'][0].update(finished_ns=0),
                 lambda r:r['events'][0]['ids'].__setitem__(0,False),
                 lambda r:r['controls'][1].update(index=False),
                 lambda r:r['events'][1].update(status='miss'),
                 lambda r:r['controls'][1].update(target_ns=0),
                 lambda r:r['requests'][3].update(target_tokens=1),
                 lambda r:r['summary']['native'].update(put_acks=1),
                 lambda r:r['summary'].update(censored_chains=0)]
        for change in changes:
            value=copy.deepcopy(report)
            change(value)
            value=_seal(value)
            with self.assertRaises(ValueError):
                delayed.check(plan,value)
    def test_plan_lists_do_not_alias_defaults(self):
        plan=delayed.generate()
        plan['config']['delays_sec'][0]=2
        self.assertEqual(delayed.generate()['config']['delays_sec'],[45,185,600])
    def test_actual_ack_edge_changes_censor_not_planned_enrollment(self):
        plan=self.plan()
        _,report,_=self.run_plan(plan)
        self.assertEqual(report['summary']['by_delay']['1']['due'],11)
        self.assertEqual(report['summary']['by_delay']['1']['planned'],12)
        self.assertEqual(report['summary']['by_delay']['1']['planned_target_tokens'],12*160)
        self.assertEqual(report['summary']['by_delay']['1']['censored_target_tokens'],160)
        self.assertEqual(report['summary']['by_delay']['1']['full_prefix_count'],11)
    def test_late_reuse_stops_after_first_observation(self):
        clock=Clock()
        backend=Backend(clock)
        original=backend.put_many
        def slow_third_arrival(objects,values):
            status=original(objects,values)
            if objects[0]['object_id']==72:
                clock.now+=3*delayed.NS
            return status
        backend.put_many=slow_third_arrival
        plan=self.plan()
        report=delayed.replay(plan,backend,clock=clock.clock,sleep=clock.sleep)
        self.assertFalse(report['passed'])
        delayed.check(plan,report)
        self.assertEqual(report['failure']['reason'],'reuse_deadline')
        self.assertEqual(report['events'][-1]['kind'],'delayed')
        failed=[row for row in report['requests'] if row['kind']=='delayed' and row['state']=='failed']
        self.assertEqual(len(failed),1)
        self.assertEqual(failed[0]['event_end']-failed[0]['event_start'],1)
        self.assertIsNone(failed[0]['usable_tokens'])
        self.assertEqual(failed[0]['confirmed_prefix_tokens'],8)
    def test_unknown_suffix_put_aborts_without_ack_or_later_calls(self):
        clock=Clock()
        backend=Backend(clock,capacity=24)
        original=backend.put_many
        def fail_suffix(objects,values):
            if objects[0]['object_id']==0:
                backend.calls+=1
                return 'unknown'
            return original(objects,values)
        backend.put_many=fail_suffix
        plan=self.plan()
        report=delayed.replay(plan,backend,clock=clock.clock,sleep=clock.sleep)
        self.assertFalse(report['passed'])
        summary=delayed.check(plan,report)
        self.assertEqual(report['events'][-1]['kind'],'refill-hot-long')
        self.assertEqual(report['events'][-1]['status'],'unknown')
        self.assertEqual(summary['native']['put_unknown'],8)
        self.assertEqual(summary['by_kind']['hot-long']['refill_put_acks'],0)
        self.assertEqual(len(report['events']),backend.calls)
    def test_actual_native_batch_adapter_passes_every_key_and_value(self):
        adapter=object.__new__(delayed.RdmaBackend)
        adapter.client=mock.Mock()
        plan=self.plan()
        objects=[delayed.object_for(plan,index) for index in (3,4,5)]
        values=[delayed.payload(plan,obj) for obj in objects]
        self.assertEqual(adapter.put_many(objects,values),'ack')
        adapter.client.put.assert_called_once_with([bytes.fromhex(obj['key_hex']) for obj in objects],values)
        adapter.client.put.side_effect=RuntimeError('native failed')
        with self.assertRaises(RuntimeError):
            adapter.put_many(objects,values)
    def test_loader_duplicate_nonfinite_bound_and_cli_exclusive_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            path=root/'input.json'
            for data in ('{"x":1,"x":2}','{"x":NaN}'):
                path.write_text(data)
                with self.assertRaises(ValueError):
                    delayed.load(path)
            path.write_bytes(b'x'*100)
            with mock.patch.object(delayed,'MAX_FILE_BYTES',16):
                with self.assertRaises(ValueError):
                    delayed.load(path)
            plan=self.plan()
            plan_path=root/'plan.json'
            delayed._write(plan_path,plan)
            clock=Clock()
            backend=Backend(clock)
            backend.close=mock.Mock()
            replay=delayed.replay
            output=root/'result'
            sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'integration/common/src'))
            import disk_scenarios
            with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()), \
                    mock.patch.object(delayed,'RdmaBackend',return_value=backend), \
                    mock.patch.object(disk_scenarios,'load_library',return_value=object()), \
                    mock.patch.object(delayed,'replay',side_effect=lambda p,b:replay(p,b,clock=clock.clock,sleep=clock.sleep)):
                argv=['run','--lib','unused','--members','n=127.0.0.1:12001','--plan',str(plan_path),'--output',str(output)]
                self.assertEqual(delayed.main(argv),0)
                backend.close.assert_called_once()
                self.assertEqual(delayed.main(argv),1)
                self.assertEqual(delayed.main(['check','--plan',str(plan_path),'--ledger',str(output/'report.json')]),0)
            delayed.check(plan,delayed.load(output/'report.json'))
    def test_transport_has_no_tcp_fallback(self):
        clock=Clock()
        backend=Backend(clock)
        backend.transport='tcp'
        with self.assertRaises(ValueError):
            delayed.replay(self.plan(),backend,clock=clock.clock,sleep=clock.sleep)
    def test_default_no_sleep_clock_simulation_bounded_size(self):
        plan=delayed.generate()
        _,report,_=self.run_plan(plan)
        self.assertTrue(report['passed'])
        delayed.check(plan,report)
        self.assertLess(len(delayed.encode(report)),delayed.MAX_FILE_BYTES)
        self.assertEqual(report['summary']['enrolled_chains'],2700)
        self.assertEqual(report['summary']['planned_requests'],3240)


class MixedDelayedTest(unittest.TestCase):
    run_plan = DelayedTest.run_plan
    def plan(self, **kwargs):
        return delayed.generate(**dict(dict(prefill=24, steps=12, arrival_batch=24,
                                             delays_sec=[1, 3, 7], hot_interval_sec=2,
                                             sizes=[4096, 16384, 65536]), **kwargs))

    def test_mixed_size_identity_payload_and_no_alias(self):
        plan = self.plan()
        self.assertEqual(plan['schema'], 'dfkv-prefix-delayed-plan-v2')
        lengths = [delayed.object_for(plan, index)['length'] for index in range(12)]
        self.assertEqual(lengths, [4096, 16384, 65536] * 4)
        for index in range(12):
            obj = delayed.object_for(plan, index)
            self.assertEqual(len(delayed.payload(plan, obj)), obj['length'])
        plan['config']['sizes'][0] = 1
        self.assertEqual(self.plan()['config']['sizes'], [4096, 16384, 65536])
        self.assertEqual(delayed.generate()['schema'], 'dfkv-prefix-delayed-plan-v1')
        self.assertNotIn('sizes', delayed.generate()['config'])
        scalar = delayed.object_for(delayed.generate(), 0)
        mixed = delayed.object_for(self.plan(), 0)
        self.assertNotEqual(mixed['namespace_hex'], scalar['namespace_hex'])
        self.assertNotEqual(mixed['key_hex'], scalar['key_hex'])
        self.assertEqual(delayed.generate()['sha256'], '280a04bbd7ff58fd7dcd4d7fc6e998f8cce9c5099af08cc5b48957d3506b86a8')

    def test_mixed_byte_accounting_and_enrollment_bins(self):
        plan, report, _ = self.run_plan(capacity=24)
        self.assertTrue(report['passed'])
        self.assertEqual(report['schema'], 'dfkv-prefix-delayed-ledger-v2')
        summary = delayed.check(plan, report)
        put_bytes = sum(sum(delayed.object_for(plan, index)['length'] for index in event['ids'])
                        for event in report['events'] if event['op'] == 'put' and event['status'] == 'ack')
        self.assertEqual(summary['native']['put_bytes'], put_bytes)
        for row in report['requests']:
            if row.get('refill_put_acks'):
                suffix = row['object_ids'][-row['refill_put_acks']:]
                self.assertEqual(row['refill_payload_bytes'], sum(delayed.object_for(plan, index)['length'] for index in suffix))
        business = [row for row in report['requests'] if row['kind'] != 'warmup']
        self.assertEqual(sum(row['planned'] for row in summary['by_enrollment_window'].values()), len(business))
        self.assertEqual(summary['by_enrollment_window']['0']['planned'], len(business))
        self.assertEqual(summary['enrollment_window_sec'], 300)
        # Size summaries describe native objects, not business prefix hit ratios.
        for length in ('4096', '16384', '65536'):
            native = summary['native_by_size'][length]
            self.assertEqual(native['put_bytes'], native['put_acks'] * int(length))
        self.assertEqual(sum(row['put_bytes'] for row in summary['native_by_size'].values()), put_bytes)

    def test_mixed_cli_opens_matching_namespace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = self.plan(steps=1)
            delayed._write(root / 'plan.json', plan)
            clock = Clock()
            backend = Backend(clock)
            backend.close = mock.Mock()
            replay = delayed.replay
            import disk_scenarios
            with contextlib.redirect_stdout(io.StringIO()), \
                    mock.patch.object(delayed, 'RdmaBackend', return_value=backend) as constructor, \
                    mock.patch.object(disk_scenarios, 'load_library', return_value='library'), \
                    mock.patch.object(delayed, 'replay', side_effect=lambda p, b: replay(p, b, clock=clock.clock, sleep=clock.sleep)):
                self.assertEqual(delayed.main(['run', '--lib', 'unused', '--members', 'n=127.0.0.1:12001',
                                               '--plan', str(root / 'plan.json'), '--output', str(root / 'result')]), 0)
            constructor.assert_called_once_with('library', 'n=127.0.0.1:12001', b'prefix-mixed-v2\x00')
            backend.close.assert_called_once()
            delayed.check(plan, delayed.load(root / 'result/report.json'))

    def test_mixed_invalid_sizes_and_batch_memory_bound(self):
        for sizes in ([], [4096, 4096], [16384, 4096], [4096, True], [8192], (4096,), None):
            with self.subTest(sizes=sizes), self.assertRaises(ValueError):
                self.plan(sizes=sizes)
        for kwargs in (dict(length=32), dict(arrival_batch=65), dict(steps=1801)):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.plan(**kwargs)

    def test_mixed_rehashed_byte_or_segment_tamper_rejected(self):
        plan, report, _ = self.run_plan(capacity=24)
        changes = [lambda row: row['summary']['native_by_size']['4096'].update(put_bytes=1),
                   lambda row: row['summary']['by_enrollment_window']['0'].update(planned=1),
                   lambda row: row['summary'].update(enrollment_window_sec=1)]
        for change in changes:
            altered = copy.deepcopy(report)
            change(altered)
            with self.assertRaises(ValueError):
                delayed.check(plan, _seal(altered))

    def test_payload_sizes_cross_default_slot_boundary_and_warmup_miss(self):
        # The fixture evicts by object count, not production extent geometry.
        plan = self.plan(sizes=[4096, 1048577], steps=3)
        _, report, _ = self.run_plan(plan, capacity=8)
        self.assertTrue(report['passed'])
        delayed.check(plan, report)
        warmup = [row for row in report['requests'] if row['kind'] == 'warmup']
        self.assertEqual(len(warmup), 3)
        self.assertTrue(any(row['usable_tokens'] != row['target_tokens'] for row in warmup))
        self.assertTrue(all(row['refill_put_acks'] == 0 for row in warmup))
        self.assertEqual([delayed.object_for(plan, index)['length'] for index in range(4)],
                         [4096, 1048577, 4096, 1048577])

    def test_mixed_unknown_put_or_native_error_preserves_fixed_plan(self):
        for failure in (('put', 1), ('get', 2)):
            with self.subTest(failure=failure):
                plan, report, _ = self.run_plan(fail=failure)
                self.assertFalse(report['passed'])
                summary = delayed.check(plan, report)
                self.assertEqual(report['failure']['reason'], 'native_failure')
                self.assertEqual(sum(row['planned'] for row in summary['by_enrollment_window'].values()), 54)
                self.assertEqual(summary['native']['get_misses'], 0)
                self.assertEqual(summary['native']['put_bytes'], 0 if failure[0] == 'put' else 24 * (4096 + 16384 + 65536) // 3)

    def test_long_clock_1800_seconds_preserves_all_planned_bins(self):
        plan = self.plan(steps=1800, sizes=[4096], hot_interval_sec=30,
                         delays_sec=[45, 185, 600])
        _, report, _ = self.run_plan(plan, capacity=24)
        self.assertTrue(report['passed'])
        summary = delayed.check(plan, report)
        self.assertEqual(summary['planned_chains'], 5400)
        self.assertEqual(summary['planned_requests'], 5580)
        self.assertEqual(set(summary['by_enrollment_window']), {'0', '300', '600', '900', '1200', '1500'})
        self.assertEqual(sum(row['planned'] for row in summary['by_enrollment_window'].values()), 5580)
        self.assertEqual(summary['by_delay']['600']['due'], 1200)
        self.assertEqual(summary['by_delay']['600']['censored'], 600)
        self.assertEqual(report['cutoff_ns'] - report['measurement_started_ns'], 1800 * delayed.NS)
        self.assertLess(len(delayed.encode(report)), delayed.MAX_FILE_BYTES)


class DueFirstTest(unittest.TestCase):
    def scenario(self, due_first=None, *, slow_first_get=False, busy_seconds=1.5, failure=None, tick_slip=0):
        config = dict(prefill=24, steps=8, arrival_batch=24, delays_sec=[1, 3, 5],
                      hot_interval_sec=30, sizes=[4096])
        if due_first is not None:
            config['due_first'] = due_first
        plan = delayed.generate(**config)
        clock = Clock()
        backend = Backend(clock, fail=failure)
        original_put, original_get = backend.put_many, backend.get
        first_query = True
        def put(objects, values):
            status = original_put(objects, values)
            index = objects[0]['object_id']
            if index == 24:
                clock.now += delayed.NS // 10
            elif index == 48:
                clock.now += delayed.NS // 1000
            elif index == 72:
                clock.now += round(busy_seconds * delayed.NS)
            return status
        def get(obj, expected):
            nonlocal first_query
            result = original_get(obj, expected)
            if slow_first_get and obj['object_id'] == 24 and first_query:
                clock.now += round(float(slow_first_get) * delayed.NS)
                first_query = False
            return result
        backend.put_many, backend.get = put, get
        sleep = clock.sleep
        def shifted_sleep(seconds):
            nonlocal tick_slip
            sleep(seconds + tick_slip)
            tick_slip = 0
        report = delayed.replay(plan, backend, clock=clock.clock, sleep=shifted_sleep)
        delayed.check(plan, report)
        return plan, report

    def test_busy_arrival_old_late_and_due_first_timely(self):
        _, old = self.scenario()
        self.assertFalse(old['passed'])
        self.assertEqual(old['failure']['reason'], 'reuse_deadline')
        first = next(row for row in old['requests'] if row['kind'] == 'delayed')
        self.assertGreater(first['age_ns'], 3 * delayed.NS)
        _, current = self.scenario(True)
        self.assertTrue(current['passed'])
        first = next(row for row in current['requests'] if row['kind'] == 'delayed')
        self.assertLessEqual(first['age_ns'], 3 * delayed.NS)
        self.assertEqual(current['summary']['planned_requests'], old['summary']['planned_requests'])
        self.assertEqual(current['summary']['arrival_batches_acked'], 8)

    def test_due_horizon_refreshes_between_requests_before_busy_arrival(self):
        _, report = self.scenario(True, slow_first_get=.2)
        self.assertTrue(report['passed'])
        events = report['events']
        first = next(index for index, event in enumerate(events) if event['kind'] == 'delayed' and event['ids'] == [24])
        second = next(index for index, event in enumerate(events) if event['kind'] == 'delayed' and event['ids'] == [48])
        third_arrival = next(index for index, event in enumerate(events) if event['kind'] == 'arrival' and event['ids'][0] == 72)
        self.assertLess(first, second)
        self.assertLess(second, third_arrival)
        # The second chain becomes eligible during the first chain's GETs.
        second_row = next(row for row in report['requests'] if row['kind'] == 'delayed' and row['step'] == 1 and row['delay_sec'] == 1)
        self.assertGreater(events[first]['started_ns'], second_row['ack_ns'])
        self.assertLess(events[first]['started_ns'], second_row['ack_ns'] + delayed.NS)
        self.assertGreaterEqual(events[second]['started_ns'], second_row['ack_ns'] + delayed.NS)

    def test_strict_flag_only_v2_and_default_absent(self):
        for value in (0, 1, None, 'true', [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                delayed.generate(sizes=[4096], due_first=value)
        for value in (False, True):
            with self.assertRaises(ValueError):
                delayed.generate(due_first=value)
        plan, absent = self.scenario()
        self.assertNotIn('due_first', plan['config'])
        explicit_plan, explicit_false = self.scenario(False)
        self.assertNotEqual(plan['sha256'], explicit_plan['sha256'])
        # Explicit false changes plan identity only, not schedule/outcomes.
        explicit_false['plan_sha256'] = absent['plan_sha256']
        self.assertEqual(delayed.encode(_seal(explicit_false)), delayed.encode(absent))
        self.assertEqual(delayed.generate()['sha256'], '280a04bbd7ff58fd7dcd4d7fc6e998f8cce9c5099af08cc5b48957d3506b86a8')

    def test_old_v1_and_v2_reference_ledgers_remain_byte_compatible(self):
        # Frozen pre-repair a0c01406 client, small deterministic fixture ledgers.
        expected = ['3a8c190af35259ca50f82d35a21af87b26f413c4a1e0912de89c13a1f3ab5601', '6f3a2757892e6a25ea885fc9840539385a64aee0ea8a4b4d2a733539a7b1ab52']
        for mixed, digest in zip((False, True), expected):
            config = dict(prefill=24, steps=4, arrival_batch=24, delays_sec=[1, 2, 3], hot_interval_sec=30)
            config.update(sizes=[4096]) if mixed else config.update(length=32)
            plan = delayed.generate(**config)
            clock = Clock()
            report = delayed.replay(plan, Backend(clock), clock=clock.clock, sleep=clock.sleep)
            self.assertEqual(report['sha256'], digest)
            self.assertTrue(report['passed'])
            delayed.check(plan, report)

    def test_predrain_past_tick_budget_stops_before_new_arrival(self):
        _, report = self.scenario(True, slow_first_get=3, tick_slip=.2)
        self.assertFalse(report['passed'])
        self.assertEqual(report['failure'], dict(reason='arrival_deadline', step=1))
        self.assertEqual(report['summary']['arrival_batches_acked'], 1)
        self.assertEqual(report['summary']['native']['get_errors'], 0)
        self.assertFalse(any(event['op'] == 'put' and event['kind'] == 'arrival' and event['ids'][0] == 48
                             for event in report['events']))

    def test_due_first_does_not_relax_deadline_or_hide_native_failure(self):
        _, report = self.scenario(True, busy_seconds=4)
        self.assertFalse(report['passed'])
        self.assertEqual(report['failure']['reason'], 'reuse_deadline')
        self.assertEqual(report['summary']['native']['get_errors'], 0)
        for failure in (('put', 1), ('get', 2)):
            _, failed = self.scenario(True, failure=failure)
            self.assertFalse(failed['passed'])
            self.assertEqual(failed['failure']['reason'], 'native_failure')
            self.assertEqual(failed['summary']['planned_requests'], 27)
            self.assertEqual(failed['summary']['enrolled_chains'], 0)


if __name__=='__main__':
    unittest.main()
