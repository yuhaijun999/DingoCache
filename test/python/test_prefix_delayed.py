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


if __name__=='__main__':
    unittest.main()
