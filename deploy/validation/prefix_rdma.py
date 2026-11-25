#!/usr/bin/env python3
"""Bounded RDMA prefix-chain correctness diagnostic, never formal A/B.

Successful payload-checked GETs prove a usable-prefix proxy, NOT engine adoption.
No server launch, transport fallback, sleeps, production tuning or disk cleanup.
The supervisor owns fresh stores, disk ACK, io_uring, thermal safety and timeout.
"""
import argparse
from collections import Counter
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eviction_trace import checksum, encode, load, MAX_FILE_BYTES
from prefix_chain import _fields, _integer, _seal, _write

PLAN_SCHEMA = 'dfkv-prefix-rdma-plan-v1'
LEDGER_SCHEMA = 'dfkv-prefix-rdma-ledger-v1'
BENEFIT_SCOPE = 'validated-loaded-prefix-proxy-not-engine-adoption'
DEFAULTS = dict(seed=0xD1A60071, prefill=12288, steps=8, arrival_batch=1024, length=4096)


def generate(**kwargs):
    """Immutable synthetic workload; object identity is derived from its index."""
    if set(kwargs) - set(DEFAULTS):
        raise ValueError('unknown configuration')
    config = dict(DEFAULTS, **kwargs)
    for key, low, high in [('seed', 0, 2**64-1), ('prefill', 12, 12288),
                           ('steps', 1, 8), ('arrival_batch', 8, 1024), ('length', 1, 4096)]:
        _integer(config[key], key, low, high)
    actions = []
    def put(index, phase, kind):
        actions.append(dict(op='put', phase=phase, kind=kind, object_id=index))
    def request(ids, kind, phase='measurement', refill=True):
        actions.append(dict(op='request', phase=phase, kind=kind,
                            request_id=f'r{len(actions)}', object_ids=list(ids), refill=refill))
    # Fillers precede the 12 hot/shared blocks so warm-up does not assume that
    # an entire near-capacity prefill remains resident under hash imbalance.
    for index in list(range(12, config['prefill'])) + list(range(12)):
        put(index, 'initialization', 'initialization')
    request(range(8), 'warm-long', 'warmup', False)
    request([0,1,2,3,8,9,10,11], 'warm-branch', 'warmup', False)
    for step in range(config['steps']):
        start = config['prefill'] + step * config['arrival_batch']
        for index in range(start, start + config['arrival_batch']):
            put(index, 'measurement', 'arrival')
        request(range(8), 'hot-long')
        request([0,1,2,3,8,9,10,11], 'hot-branch')
        request(range(2), 'hot-short')
        request(range(start, start+8), 'new-first')
        request(range(start, start+8), 'new-revisit')
        if step:
            previous = start-config['arrival_batch']
            request(range(previous, previous+8), 'delayed-new')
    missing = config['prefill'] + config['steps']*config['arrival_batch']
    for group, seeded in [('head',0), ('middle',4), ('tail',7)]:
        for index in range(missing, missing+seeded):
            put(index, 'measurement', 'forced-gap-seed')
        request(range(missing, missing+8), f'forced-{group}-first')
        request(range(missing, missing+8), f'forced-{group}-revisit')
        missing += 8
    result = _seal(dict(schema=PLAN_SCHEMA, synthetic=True, config=config, actions=actions))
    if len(encode(result)) > MAX_FILE_BYTES:
        raise ValueError('plan exceeds file bound')
    return result


def validate(plan):
    _fields(plan, {'schema','synthetic','config','actions','sha256'}, 'plan')
    if not isinstance(plan['config'], dict) or set(plan['config']) != set(DEFAULTS):
        raise ValueError('invalid config')
    if encode(plan) != encode(generate(**plan['config'])):
        raise ValueError('plan differs from deterministic workload/checksum')
    return plan['config']


def object_for(plan, index):
    config = plan['config']
    _integer(index, 'object_id', 0, config['prefill']+config['steps']*config['arrival_batch']+23)
    namespace = b'prefix-rdma-v1\x00'
    key = b'prefix\x00'+config['seed'].to_bytes(8,'big')+index.to_bytes(8,'big')
    return dict(object_id=index, namespace_hex=namespace.hex(), key_hex=key.hex(),
                generation=0, tokens=(index%4+1)*8, length=config['length'])


def payload(plan, obj):
    identity = encode(dict(schema=PLAN_SCHEMA, seed=plan['config']['seed'], object=obj))
    return hashlib.shake_256(identity).digest(obj['length'])


def _ratio(value, total):
    if not total:
        return None
    fraction = Fraction(value,total)
    return dict(numerator=fraction.numerator, denominator=fraction.denominator)


def _execute(plan, invoke, transport):
    """Shared request state machine; checker supplies raw observations, not I/O."""
    validate(plan)
    events, rows, states = [], [], []
    aborted, failure = False, None
    native = Counter()
    for action_index, action in enumerate(plan['actions']):
        begin = len(events)
        state = 'not_attempted' if aborted else 'completed'
        def call(op, object_id, kind):
            nonlocal aborted, failure, state
            obj = object_for(plan, object_id)
            expected = payload(plan,obj)
            event = invoke(action_index, op, kind, obj, expected)
            events.append(event)
            status = event['status']
            native[f'{op}_attempts'] += 1
            if op == 'put':
                native['put_acks' if status == 'ack' else 'put_errors' if status == 'error' else 'put_unknown'] += 1
                if status == 'ack':
                    native['put_bytes'] += obj['length']
            else:
                native[{'hit':'get_hits','miss':'get_misses','error':'get_errors','unknown':'get_unknown'}[status]] += 1
            if status in ('error','unknown'):
                aborted = True
                state = 'failed'
                failure = dict(action_index=action_index, op=op, object_id=object_id, status=status)
            return status
        if action['op'] == 'put':
            if not aborted:
                call('put',action['object_id'],action['kind'])
        else:
            ids = action['object_ids']
            target = sum(object_for(plan,i)['tokens'] for i in ids)
            outcomes = ['not_attempted']*len(ids)
            usable, known, boundary = 0, not aborted, 0 if aborted else len(ids)
            refill_acks = refill_bytes = proxy = 0
            if not aborted:
                for block_index, object_id in enumerate(ids):
                    status = call('get',object_id,action['kind'])
                    outcomes[block_index] = status
                    if status != 'hit':
                        boundary = block_index
                        known = status == 'miss'
                        break
                    usable += object_for(plan,object_id)['tokens']
                # Original benefit is frozen before any suffix writes occur.
                if known and boundary < len(ids) and action['phase'] == 'warmup':
                    aborted = True
                    state = 'failed'
                    failure = dict(action_index=action_index, op='get',
                                   object_id=ids[boundary], status='warmup_miss')
                if known and not aborted and boundary < len(ids) and action['refill']:
                    proxy = target-usable
                    for object_id in ids[boundary:]:
                        if call('put',object_id,'refill') == 'ack':
                            refill_acks += 1
                            refill_bytes += object_for(plan,object_id)['length']
                        if aborted:
                            break
            rows.append(dict(action_index=action_index,request_id=action['request_id'],
                             phase=action['phase'],kind=action['kind'],object_ids=ids,
                             target_tokens=target,usable_tokens=usable if known else None,
                             confirmed_prefix_tokens=usable,first_unconfirmed_block=boundary,
                             outcomes=outcomes,recompute_proxy_tokens=proxy,
                             refill_put_acks=refill_acks,refill_payload_bytes=refill_bytes,
                             event_start=begin,event_end=len(events),state=state))
        states.append(state)
    business = [row for row in rows if row['phase']=='measurement']
    total = sum(row['target_tokens'] for row in business)
    lower = sum(row['confirmed_prefix_tokens'] for row in business)
    unknown = sum(row['usable_tokens'] is None for row in business)
    known_tokens = sum(row['usable_tokens'] for row in business if row['usable_tokens'] is not None)
    exact = bool(business) and not unknown
    full = sum(row['usable_tokens']==row['target_tokens'] for row in business)
    def group(values):
        return dict(requests=len(values),target_tokens=sum(row['target_tokens'] for row in values),
                    known_usable_tokens=sum(row['usable_tokens'] for row in values if row['usable_tokens'] is not None),
                    unknown_requests=sum(row['usable_tokens'] is None for row in values),
                    refill_put_acks=sum(row['refill_put_acks'] for row in values),
                    refill_payload_bytes=sum(row['refill_payload_bytes'] for row in values))
    summary = dict(formal_ab=False,benefit_scope=BENEFIT_SCOPE,
                   planned_requests=len(business),completed_requests=sum(row['state']=='completed' for row in business),
                   unknown_requests=unknown,target_tokens=total,known_usable_tokens=known_tokens,
                   confirmed_prefix_lower_bound_tokens=lower,
                   token_reuse_ratio=_ratio(known_tokens,total) if exact else None,
                   mean_usable_tokens=_ratio(known_tokens,len(business)) if exact else None,
                   mean_request_reuse_ratio=_ratio(sum(Fraction(row['usable_tokens'],row['target_tokens']) for row in business),len(business)) if exact else None,
                   full_prefix_hit_rate=_ratio(full,len(business)) if exact else None,
                   token_reuse_lower_bound=_ratio(lower,total),
                   recompute_proxy_tokens=sum(row['recompute_proxy_tokens'] for row in business),
                   refill_put_acks=sum(row['refill_put_acks'] for row in business),
                   refill_payload_bytes=sum(row['refill_payload_bytes'] for row in business),
                   not_attempted_actions=states.count('not_attempted'),
                   by_kind={kind:group([row for row in business if row['kind']==kind]) for kind in sorted({row['kind'] for row in business})},
                   native={key:native[key] for key in ('get_hits','get_misses','get_errors','get_unknown','put_acks','put_bytes','put_errors','put_unknown','get_attempts','put_attempts')})
    return _seal(dict(schema=LEDGER_SCHEMA,formal_ab=False,synthetic=True,
                      benefit_scope=BENEFIT_SCOPE,plan_sha256=plan['sha256'],transport=transport,
                      passed=not aborted,failure=failure,events=events,requests=rows,
                      action_states=states,summary=summary))


def replay(plan, backend):
    """Stop native work after first failure; retain every planned request denominator."""
    transport = getattr(backend,'transport',None)
    if transport not in ('rdma','fake-rdma-test'):
        raise ValueError('RDMA transport required; no fallback')
    def invoke(index,op,kind,obj,expected):
        started = time.monotonic_ns()
        actual, error = None, None
        try:
            if op=='put':
                status = backend.put(obj,expected)
                if status not in ('ack','error','unknown'):
                    raise RuntimeError('invalid PUT outcome')
            else:
                status, actual = backend.get(obj,expected)
                if status not in ('hit','miss','error','unknown'):
                    raise RuntimeError('invalid GET outcome')
                if status=='hit' and (type(actual) is not bytes or actual!=expected):
                    status,error = 'error','wrong payload/length'
                if status!='hit' and actual is not None:
                    status,error = 'error','non-hit returned payload'
        except Exception as caught:
            status = 'unknown' if op=='put' else 'error'
            error = f'{type(caught).__name__}: {caught}'[:512]
        return dict(action_index=index,op=op,kind=kind,object=obj,status=status,
                    expected_sha256=hashlib.sha256(expected).hexdigest(),
                    observed_sha256=hashlib.sha256(actual).hexdigest() if type(actual) is bytes else None,
                    observed_length=len(actual) if type(actual) is bytes else 0,
                    started_ns=started,finished_ns=time.monotonic_ns(),error=error)
    result = _execute(plan,invoke,transport)
    if len(encode(result)) > MAX_FILE_BYTES:
        raise ValueError('ledger exceeds file bound')
    return result


def check(plan, ledger):
    """Re-derive operation order and all metrics from raw calls, not claimed totals.

    This shares the request state machine and is not independent execution proof.
    A payload digest is a validation claim, not an authentication signature.
    """
    if not isinstance(ledger,dict) or ledger.get('transport') not in ('rdma','fake-rdma-test'):
        raise ValueError('invalid ledger transport')
    source = ledger.get('events')
    if not isinstance(source,list):
        raise ValueError('invalid raw events')
    position, previous = 0, 0
    def observed(index,op,kind,obj,expected):
        nonlocal position, previous
        if position>=len(source):
            raise ValueError('missing raw event')
        event = source[position]
        position += 1
        _fields(event,{'action_index','op','kind','object','status','expected_sha256','observed_sha256','observed_length','started_ns','finished_ns','error'},'event')
        for key,value in [('action_index',index),('op',op),('kind',kind),('object',obj),('expected_sha256',hashlib.sha256(expected).hexdigest())]:
            if encode(event[key])!=encode(value):
                raise ValueError('raw event identity/order mismatch')
        _integer(event['started_ns'],'started_ns',previous)
        _integer(event['finished_ns'],'finished_ns',event['started_ns'])
        previous=event['finished_ns']
        statuses=('ack','error','unknown') if op=='put' else ('hit','miss','error','unknown')
        if event['status'] not in statuses:
            raise ValueError('invalid terminal status')
        if event['error'] is not None and (not isinstance(event['error'],str) or len(event['error'])>512):
            raise ValueError('invalid error')
        _integer(event['observed_length'],'observed_length')
        if op=='put' or event['status']=='miss':
            if event['observed_length'] or event['observed_sha256'] is not None:
                raise ValueError('unexpected payload')
        if op=='get' and event['status']=='hit':
            if (event['observed_sha256']!=event['expected_sha256'] or event['observed_length']!=len(expected) or event['error'] is not None):
                raise ValueError('unchecked hit payload')
        return event
    expected=_execute(plan,observed,ledger['transport'])
    if position!=len(source) or encode(ledger)!=encode(expected):
        raise ValueError('ledger differs from raw observations/accounting')
    return expected['summary']


class RdmaBackend:
    """One namespace-bound scalar C ABI handle; caller proves disk-ACK/server state."""
    def __init__(self,lib,members,namespace):
        if getattr(lib,'dfkv_get_auto_status',None) is None:
            raise RuntimeError('scalar status ABI required; no bool fallback')
        from disk_scenarios import Client
        self.client=Client(lib,members,namespace)
        self.transport=self.client.transport
        if self.transport!='rdma':
            self.client.close()
            raise RuntimeError('RDMA-only diagnostic refuses transport fallback')
    def put(self,obj,value):
        self.client.put([bytes.fromhex(obj['key_hex'])],[value])
        return 'ack'
    def get(self,obj,expected):
        status=self.client.read_outcome(bytes.fromhex(obj['key_hex']),expected)
        if status=='get_hit':
            # Client.read_outcome already checked every byte and returned length.
            return 'hit',expected
        if status=='get_miss':
            return 'miss',None
        raise RuntimeError('unresolved scalar GET; no miss inference')
    def close(self):
        self.client.close()


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    generator=commands.add_parser('generate')
    generator.add_argument('--output',type=Path,required=True)
    for name,value in DEFAULTS.items():
        generator.add_argument('--'+name.replace('_','-'),type=int,default=value)
    for name in ('run','check'):
        command=commands.add_parser(name)
        command.add_argument('--plan',type=Path,required=True)
        command.add_argument('--output',type=Path,required=name=='run')
        if name=='run':
            command.add_argument('--lib',required=True)
            command.add_argument('--members',required=True)
        else:
            command.add_argument('--ledger',type=Path,required=True)
    args=parser.parse_args(argv)
    backend=None
    try:
        if args.command=='generate':
            result=generate(**{key:getattr(args,key) for key in DEFAULTS})
            _write(args.output,result)
        elif args.command=='check':
            result=check(load(args.plan),load(args.ledger))
            if args.output:
                _write(args.output,result)
        else:
            plan=load(args.plan)
            validate(plan)
            args.output.mkdir(exist_ok=False)
            sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'integration/common/src'))
            from disk_scenarios import load_library
            obj=object_for(plan,0)
            backend=RdmaBackend(load_library(args.lib),args.members,bytes.fromhex(obj['namespace_hex']))
            result=replay(plan,backend)
            backend.close()
            backend=None
            _write(args.output/'report.json',result)
            print(json.dumps(result['summary'],sort_keys=True))
            return 0 if result['passed'] else 1
        print(json.dumps(result,sort_keys=True))
        return 0
    except (OSError,ValueError,RuntimeError,TypeError,UnicodeError,RecursionError) as error:
        print(f'prefix RDMA error: {error}',file=sys.stderr)
        return 1
    finally:
        if backend is not None:
            backend.close()

if __name__=='__main__':
    sys.exit(main())
