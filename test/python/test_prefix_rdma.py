"""Prefix RDMA accounting with an in-memory test adapter, never a service."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import types

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'deploy/validation'))
import prefix_rdma as prefix


class MemoryBackend:
    transport = 'fake-rdma-test'
    def __init__(self, fail=None):
        self.values = {}
        self.fail = fail
        self.calls = 0
    def put(self, obj, value):
        self.calls += 1
        if self.fail == ('put', self.calls):
            return 'unknown'
        self.values[obj['key_hex']] = value
        return 'ack'
    def get(self, obj, expected):
        self.calls += 1
        if self.fail == ('get', self.calls):
            return 'unknown', None
        value = self.values.get(obj['key_hex'])
        return ('miss', None) if value is None else ('hit', value)


class PrefixRdmaTest(unittest.TestCase):
    def plan(self):
        return prefix.generate(prefill=16, steps=2, arrival_batch=8, length=32)
    def test_plan_deterministic_and_strict(self):
        plan = self.plan()
        self.assertEqual(plan, self.plan())
        prefix.validate(plan)
        changed = copy.deepcopy(plan)
        changed['actions'][0]['object_id'] = 999
        changed['sha256'] = prefix.checksum(changed)
        with self.assertRaises(ValueError):
            prefix.validate(changed)
    def test_full_denominators_and_native_totals(self):
        plan = self.plan()
        report = prefix.replay(plan, MemoryBackend())
        self.assertTrue(report['passed'])
        summary = prefix.check(plan, report)
        self.assertEqual(summary['planned_requests'], summary['completed_requests'])
        self.assertEqual(summary['unknown_requests'], 0)
        self.assertGreater(summary['native']['get_hits'], 0)
        self.assertEqual(summary['native']['put_bytes'], summary['native']['put_acks']*32)
        self.assertEqual(summary['native']['get_errors'], 0)
        self.assertFalse(summary['formal_ab'])
        self.assertEqual(summary['benefit_scope'], 'validated-loaded-prefix-proxy-not-engine-adoption')
    def test_first_hole_refill_then_revisit_is_frozen(self):
        report = prefix.replay(self.plan(), MemoryBackend())
        rows = [row for row in report['requests'] if row['kind'].startswith('forced-head-')]
        self.assertEqual(rows[0]['usable_tokens'], 0)
        self.assertGreater(rows[1]['usable_tokens'], 0)
        self.assertEqual(rows[0]['refill_put_acks'], 8)
    def test_error_never_becomes_miss_and_future_denominator_remains(self):
        backend = MemoryBackend()
        original = backend.get
        def failed(obj, expected):
            if backend.calls > 30:
                return 'error', None
            return original(obj, expected)
        backend.get = failed
        report = prefix.replay(self.plan(), backend)
        self.assertFalse(report['passed'])
        summary = prefix.check(self.plan(), report)
        self.assertGreater(summary['unknown_requests'], 0)
        self.assertIsNone(summary['token_reuse_ratio'])
        self.assertEqual(summary['native']['get_errors'], 1)
        self.assertGreater(summary['not_attempted_actions'], 0)
    def test_unknown_put_has_no_ack_and_no_later_native_calls(self):
        backend = MemoryBackend(('put', 1))
        report = prefix.replay(self.plan(), backend)
        self.assertFalse(report['passed'])
        self.assertEqual(backend.calls, 1)
        self.assertEqual(report['summary']['native']['put_acks'], 0)
        self.assertEqual(report['summary']['native']['put_unknown'], 1)
        prefix.check(self.plan(), report)
    def test_corrupt_payload_is_an_error_not_a_hit(self):
        backend = MemoryBackend()
        backend.get = lambda obj, expected: ('hit', b'x')
        report = prefix.replay(self.plan(), backend)
        self.assertFalse(report['passed'])
        self.assertEqual(report['summary']['native']['get_hits'], 0)
        self.assertEqual(report['summary']['native']['get_errors'], 1)
        prefix.check(self.plan(), report)
    def test_rehashed_tampering_is_rejected(self):
        plan = self.plan()
        report = prefix.replay(plan, MemoryBackend())
        for target in ('event', 'summary', 'request', 'drop', 'order'):
            changed = copy.deepcopy(report)
            if target == 'event': changed['events'][0]['object']['object_id'] += 1
            if target == 'summary': changed['summary']['target_tokens'] += 1
            if target == 'request': changed['requests'][0]['usable_tokens'] += 1
            if target == 'drop': changed['events'].pop()
            if target == 'order': changed['events'].reverse()
            changed['sha256'] = prefix.checksum(changed)
            with self.subTest(target=target), self.assertRaises(ValueError):
                prefix.check(plan, changed)
    def test_transport_is_explicit(self):
        backend = MemoryBackend()
        backend.transport = 'tcp'
        with self.assertRaises(ValueError): prefix.replay(self.plan(), backend)
    def test_generation_payload_identity(self):
        plan = self.plan()
        a = prefix.object_for(plan, 0)
        b = prefix.object_for(plan, 1)
        self.assertNotEqual(a['key_hex'], b['key_hex'])
        self.assertNotEqual(prefix.payload(plan,a), prefix.payload(plan,b))
    def test_invalid_configuration(self):
        for kwargs in ({'prefill': True}, {'prefill': 11}, {'steps': 0},
                       {'arrival_batch': 7}, {'length': 0}, {'seed': -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError): prefix.generate(**kwargs)
    def test_forced_head_middle_tail_gaps_are_hand_calculated(self):
        value = self.plan()
        report = prefix.replay(value, MemoryBackend())
        rows = {row['kind']:row for row in report['requests']}
        for group, boundary in [('head',0),('middle',4),('tail',7)]:
            first = rows[f'forced-{group}-first']
            again = rows[f'forced-{group}-revisit']
            expected = sum(prefix.object_for(value,i)['tokens']
                           for i in first['object_ids'][:boundary])
            self.assertEqual(first['first_unconfirmed_block'],boundary)
            self.assertEqual(first['usable_tokens'],expected)
            self.assertEqual(first['refill_put_acks'],8-boundary)
            self.assertEqual(again['usable_tokens'],again['target_tokens'])
            self.assertEqual(first['outcomes'][boundary+1:],['not_attempted']*(7-boundary))
    def test_partial_refill_failure_preserves_original_and_stops(self):
        value = self.plan()
        missing = value['config']['prefill']+value['config']['steps']*value['config']['arrival_batch']
        backend = MemoryBackend()
        original = backend.put
        def failed(obj,data):
            if obj['object_id'] == missing+1:
                return 'unknown'
            return original(obj,data)
        backend.put = failed
        report = prefix.replay(value,backend)
        first = next(row for row in report['requests'] if row['kind']=='forced-head-first')
        self.assertEqual(first['usable_tokens'],0)
        self.assertEqual(first['refill_put_acks'],1)
        self.assertEqual(first['state'],'failed')
        self.assertEqual(report['summary']['native']['put_unknown'],1)
        self.assertFalse(report['passed'])
        prefix.check(value,report)
    def test_unknown_get_keeps_confirmed_prefix_and_no_refill(self):
        value = self.plan()
        backend = MemoryBackend()
        original = backend.get
        count = 0
        def failed(obj,expected):
            nonlocal count
            count += 1
            if count == 19:
                return 'unknown',None
            return original(obj,expected)
        backend.get = failed
        report = prefix.replay(value,backend)
        failedrow = next(row for row in report['requests'] if row['state']=='failed')
        self.assertGreater(failedrow['confirmed_prefix_tokens'],0)
        self.assertIsNone(failedrow['usable_tokens'])
        self.assertEqual(failedrow['refill_put_acks'],0)
        self.assertEqual(report['summary']['native']['get_unknown'],1)
        prefix.check(value,report)
    def test_native_adapter_requires_status_before_open_and_rejects_tcp(self):
        class Client:
            closed = False
            def __init__(self,*args): self.transport = 'tcp'
            def close(self): Client.closed = True
        module = types.SimpleNamespace(Client=Client)
        with mock.patch.dict(sys.modules,{'disk_scenarios':module}):
            with self.assertRaisesRegex(RuntimeError,'status ABI'):
                prefix.RdmaBackend(types.SimpleNamespace(),'unused',b'ns')
            with self.assertRaisesRegex(RuntimeError,'transport fallback'):
                prefix.RdmaBackend(types.SimpleNamespace(dfkv_get_auto_status=True),'unused',b'ns')
        self.assertTrue(Client.closed)
    def test_native_adapter_has_no_unresolved_fallback(self):
        class Client:
            def __init__(self,*args): self.transport = 'rdma'
            def read_outcome(self,*args): return 'get_unresolved'
            def close(self): pass
        with mock.patch.dict(sys.modules,{'disk_scenarios':types.SimpleNamespace(Client=Client)}):
            backend = prefix.RdmaBackend(types.SimpleNamespace(dfkv_get_auto_status=True),'unused',b'ns')
            with self.assertRaisesRegex(RuntimeError,'unresolved'):
                backend.get(prefix.object_for(self.plan(),0),b'x')
            backend.close()
    def test_cli_generate_check_isolated_and_no_overwrite(self):
        script = ROOT/'deploy/validation/prefix_rdma.py'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            plan = self.plan(); report = prefix.replay(plan, MemoryBackend())
            (path/'plan.json').write_bytes(prefix.encode(plan))
            (path/'report.json').write_bytes(prefix.encode(report))
            args = [sys.executable,'-I',str(script),'check','--plan',str(path/'plan.json'),
                    '--ledger',str(path/'report.json'),'--output',str(path/'summary.json')]
            result = subprocess.run(args,cwd=directory,capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(subprocess.run(args,cwd=directory,capture_output=True,timeout=10).returncode,1)

if __name__ == '__main__': unittest.main()
