#!/usr/bin/env python3
"""ACK-aged RDMA prefix diagnostic. Not engine adoption, TTFT or formal A/B.

Absolute one-second arrival slots are not caught up or silently dropped. Batch
ACK time is a conservative chain-ready upper bound, not a device commit time.
The external supervisor owns disk ACK, thermal safety, stores and process limits.
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
import prefix_rdma as prefix
from eviction_trace import encode
from prefix_chain import _integer, _seal, _write

MAX_FILE_BYTES = 128 * 1024 * 1024
DEFAULTS = dict(seed=0xD1A60081, prefill=12288, steps=900, interval_sec=1,
                arrival_batch=64, chain_length=8, delays_sec=[45,185,600],
                hot_interval_sec=5, length=4096, max_lateness_sec=2)
NS = 1_000_000_000


def generate(**kwargs):
    if set(kwargs)-set(DEFAULTS):
        raise ValueError('unknown config')
    config = dict(DEFAULTS, **kwargs)
    for name, low, high in [('seed',0,2**64-1),('prefill',12,12288),('steps',1,900),
                            ('interval_sec',1,1),('arrival_batch',24,128),
                            ('chain_length',8,8),('hot_interval_sec',1,30),
                            ('length',1,4096),('max_lateness_sec',1,5)]:
        _integer(config[name],name,low,high)
    delays=config['delays_sec']
    if (type(delays) is not list or len(delays)!=3 or
            any(type(value) is not int or not 1<=value<=600 for value in delays) or
            delays!=sorted(set(delays))):
        raise ValueError('invalid delays')
    config['delays_sec']=list(delays)
    return _seal(dict(schema='dfkv-prefix-delayed-plan-v1',synthetic=True,config=config))


def validate(plan):
    if not isinstance(plan,dict) or not isinstance(plan.get('config'),dict):
        raise ValueError('invalid plan')
    if encode(plan)!=encode(generate(**plan['config'])):
        raise ValueError('changed plan')
    return dict(plan['config'])


def load(path):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:
                raise ValueError('duplicate JSON field')
            result[key]=value
        return result
    with Path(path).open('rb') as stream:
        data=stream.read(MAX_FILE_BYTES+1)
    if len(data)>MAX_FILE_BYTES:
        raise ValueError('file bound exceeded')
    return json.loads(data,object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def object_for(plan,index):
    config=plan['config']
    _integer(index,'object_id',0,config['prefill']+config['steps']*config['arrival_batch']-1)
    return dict(object_id=index,namespace_hex=b'prefix-delayed-v1\x00'.hex(),
                key_hex=(b'pd\x00'+config['seed'].to_bytes(8,'big')+index.to_bytes(8,'big')).hex(),
                generation=0,tokens=(index%4+1)*8,length=config['length'])


def payload(plan,obj):
    return hashlib.shake_256(encode(dict(seed=plan['config']['seed'],object=obj))).digest(obj['length'])


def _fraction(value,total):
    if not total:
        return None
    result=Fraction(value,total)
    return dict(numerator=result.numerator,denominator=result.denominator)


def _execute(plan,invoke,schedule,transport):
    config=validate(plan)
    events,controls,requests=[],[],[]
    counts=Counter()
    failed=None
    start=cutoff=None
    enrolled={}
    def control(kind,index,target):
        row=schedule(kind,index,target)
        controls.append(row)
        return row['time_ns']
    def call(op,ids,kind):
        nonlocal failed
        event=invoke(op,list(ids),kind)
        events.append(event)
        size=len(ids)
        counts[op+'_attempts']+=size
        status=event['status']
        if op=='put':
            counts[{'ack':'put_acks','error':'put_errors','unknown':'put_unknown'}[status]]+=size
            if status=='ack':
                counts['put_bytes']+=size*config['length']
        else:
            counts[{'hit':'get_hits','miss':'get_misses','error':'get_errors','unknown':'get_unknown'}[status]]+=1
        if status in ('error','unknown'):
            failed=dict(reason='native_failure',event_index=len(events)-1,status=status)
        return event
    def request(ids,kind,step,delay=None,ack=None):
        nonlocal failed
        begin=len(events)
        usable=0
        statuses=[]
        boundary=len(ids)
        refill=0
        known=True
        first=None
        for offset,index in enumerate(ids):
            event=call('get',[index],kind)
            first=event['started_ns'] if first is None else first
            if failed is None and ack is not None and offset==0 and not ack+delay*NS<=first<=ack+(delay+config['max_lateness_sec'])*NS:
                failed=dict(reason='reuse_deadline',step=step,delay_sec=delay)
            status=event['status']
            statuses.append(status)
            if status!='hit':
                boundary=offset
                known=status=='miss'
                break
            usable+=object_for(plan,index)['tokens']
            if failed is not None:
                known=False
                boundary=offset+1
                break
        target=sum(object_for(plan,index)['tokens'] for index in ids)
        # Freeze original prefix before any refill; never verify-read the refill.
        if known and boundary<len(ids) and failed is None and kind!='warmup':
            event=call('put',ids[boundary:],'refill-'+kind)
            if event['status']=='ack':
                refill=len(ids)-boundary
        requests.append(dict(kind=kind,step=step,delay_sec=delay,object_ids=list(ids),
                             ack_ns=ack,started_ns=first,age_ns=None if ack is None else first-ack,
                             target_tokens=target,usable_tokens=usable if known else None,
                             confirmed_prefix_tokens=usable,first_unconfirmed_block=boundary,
                             outcomes=statuses+['not_attempted']*(len(ids)-len(statuses)),
                             refill_put_acks=refill,refill_payload_bytes=refill*config['length'],
                             recompute_proxy_tokens=target-usable if known and boundary<len(ids) else 0,
                             state='completed' if failed is None else 'failed',
                             event_start=begin,event_end=len(events)))
    def due(now):
        for identity,item in sorted(enrolled.items(),key=lambda entry:(entry[1]['ack']+entry[1]['delay']*NS,entry[0])):
            if failed is not None:
                return
            if not item['done'] and item['ack']+item['delay']*NS<=min(now,cutoff):
                request(item['ids'],'delayed',identity[0],item['delay'],item['ack'])
                item['done']=True
    try:
        # Fillers first, hot shared blocks last, in bounded native batches.
        indexes=list(range(12,config['prefill']))+list(range(12))
        for offset in range(0,len(indexes),config['arrival_batch']):
            call('put',indexes[offset:offset+config['arrival_batch']],'initialization')
            if failed is not None:
                break
        if failed is None:
            for ids in (list(range(8)),[0,1,2,3,8,9,10,11],list(range(2))):
                request(ids,'warmup',-1)
                row=requests[-1]
                if row['usable_tokens']!=row['target_tokens']:
                    if failed is None:
                        failed=dict(reason='warmup_miss')
                    break
        if failed is None:
            start=control('start',None,0)
            cutoff=start+config['steps']*config['interval_sec']*NS
            for step in range(config['steps']):
                target=start+step*config['interval_sec']*NS
                now=control('tick',step,target)
                if now>target+config['max_lateness_sec']*NS or now>=cutoff:
                    failed=dict(reason='arrival_deadline',step=step)
                    break
                first=config['prefill']+step*config['arrival_batch']
                event=call('put',range(first,first+config['arrival_batch']),'arrival')
                if failed is not None:
                    break
                for group,delay in enumerate(config['delays_sec']):
                    enrolled[(step,delay)]=dict(ids=list(range(first+group*8,first+(group+1)*8)),
                                              ack=event['finished_ns'],delay=delay,done=False)
                due(event['finished_ns'])
                if failed is not None:
                    break
                if step%config['hot_interval_sec']==0:
                    for ids,kind in ((list(range(8)),'hot-long'),([0,1,2,3,8,9,10,11],'hot-branch'),(list(range(2)),'hot-short')):
                        request(ids,kind,step)
                        if failed is not None:
                            break
                if failed is not None:
                    break
            if failed is None:
                now=control('end',None,cutoff)
                if now>cutoff+config['max_lateness_sec']*NS:
                    failed=dict(reason='terminal_deadline')
                else:
                    due(cutoff)
    except (OSError,ValueError,RuntimeError) as error:
        # Internal/checker errors are not converted into successful terminal calls.
        raise
    existing={(row['step'],row['delay_sec']) for row in requests if row['kind']=='delayed'}
    for step in range(config['steps']):
        for group,delay in enumerate(config['delays_sec']):
            identity=(step,delay)
            if identity in existing:
                continue
            first=config['prefill']+step*config['arrival_batch']+group*8
            item=enrolled.get(identity)
            censored=item is not None and cutoff is not None and item['ack']+delay*NS>cutoff
            requests.append(dict(kind='delayed',step=step,delay_sec=delay,
                                 object_ids=list(range(first,first+8)),ack_ns=item['ack'] if item else None,
                                 state='censored' if censored else 'not_attempted',
                                 target_tokens=sum(object_for(plan,index)['tokens'] for index in range(first,first+8)),
                                 usable_tokens=None,confirmed_prefix_tokens=0,refill_put_acks=0,refill_payload_bytes=0))
    existing={(row['step'],row['kind']) for row in requests}
    for step in range(0,config['steps'],config['hot_interval_sec']):
        for ids,kind in ((list(range(8)),'hot-long'),([0,1,2,3,8,9,10,11],'hot-branch'),(list(range(2)),'hot-short')):
            if (step,kind) not in existing:
                requests.append(dict(kind=kind,step=step,state='not_attempted',object_ids=ids,
                                     target_tokens=sum(object_for(plan,index)['tokens'] for index in ids),
                                     usable_tokens=None,confirmed_prefix_tokens=0,refill_put_acks=0,refill_payload_bytes=0))
    business=[row for row in requests if row['kind']!='warmup']
    def summarize(rows):
        eligible=[row for row in rows if row['state']!='censored']
        known=[row for row in eligible if row['usable_tokens'] is not None]
        ages=[row['age_ns'] for row in rows if row.get('age_ns') is not None]
        result=dict(planned=len(rows),completed=sum(row['state']=='completed' for row in rows),
                    censored=sum(row['state']=='censored' for row in rows),
                    not_attempted=sum(row['state']=='not_attempted' for row in rows),
                    failed=sum(row['state']=='failed' for row in rows),
                    planned_target_tokens=sum(row['target_tokens'] for row in rows),
                    target_tokens=sum(row['target_tokens'] for row in eligible),
                    censored_target_tokens=sum(row['target_tokens'] for row in rows if row['state']=='censored'),
                    known_usable_tokens=sum(row['usable_tokens'] for row in known),
                    confirmed_prefix_lower_bound_tokens=sum(row['confirmed_prefix_tokens'] for row in eligible),
                    unknown_requests=len(eligible)-len(known),
                    full_prefix_count=sum(row['usable_tokens']==row['target_tokens'] for row in known),
                    refill_put_acks=sum(row['refill_put_acks'] for row in rows),
                    refill_payload_bytes=sum(row['refill_payload_bytes'] for row in rows),
                    age_min_ns=min(ages) if ages else None,age_max_ns=max(ages) if ages else None)
        exact=result['unknown_requests']==0
        result['token_reuse_ratio']=_fraction(result['known_usable_tokens'],result['target_tokens']) if exact else None
        result['token_reuse_lower_bound']=_fraction(result['confirmed_prefix_lower_bound_tokens'],result['target_tokens'])
        result['full_prefix_hit_rate']=_fraction(result['full_prefix_count'],len(eligible)) if exact else None
        result['mean_request_reuse_ratio']=_fraction(sum(Fraction(row['usable_tokens'],row['target_tokens']) for row in known),len(eligible)) if exact else None
        return result
    groups={kind:summarize([row for row in business if row['kind']==kind])
            for kind in sorted({row['kind'] for row in business})}
    fields=('get_hits','get_misses','get_errors','get_unknown','put_acks','put_bytes','put_errors','put_unknown','get_attempts','put_attempts')
    summary=dict(formal_ab=False,benefit_scope=prefix.BENEFIT_SCOPE,by_kind=groups,
                 enrolled_chains=len(enrolled),planned_chains=config['steps']*3,
                 censored_chains=sum(row['state']=='censored' for row in business),
                 completed_requests=sum(row['state']=='completed' for row in business),
                 planned_requests=len(business),
                 arrival_batches_planned=config['steps'],
                 arrival_batches_acked=sum(event['op']=='put' and event['kind']=='arrival' and event['status']=='ack' for event in events),
                 native={field:counts[field] for field in fields})
    summary['by_delay']={}
    for delay in config['delays_sec']:
        rows=[row for row in business if row.get('delay_sec')==delay]
        group=summarize(rows)
        group['enrolled']=sum(key[1]==delay for key in enrolled)
        group['due']=sum(item['ack']+delay*NS<=cutoff for key,item in enrolled.items() if key[1]==delay) if cutoff is not None else 0
        group['due_target_tokens']=sum(row['target_tokens'] for row in rows if row.get('ack_ns') is not None and row['ack_ns']+delay*NS<=cutoff) if cutoff is not None else 0
        summary['by_delay'][str(delay)]=group
    return _seal(dict(schema='dfkv-prefix-delayed-ledger-v1',plan_sha256=plan['sha256'],
                      transport=transport,passed=failed is None,failure=failed,
                      measurement_started_ns=start,cutoff_ns=cutoff,controls=controls,
                      events=events,requests=requests,summary=summary))


def replay(plan,backend,*,clock=time.monotonic_ns,sleep=time.sleep):
    if getattr(backend,'transport',None) not in ('rdma','fake-rdma-test'):
        raise ValueError('RDMA required')
    def schedule(kind,index,target):
        now=clock()
        if now<target:
            sleep((target-now)/NS)
        return dict(kind=kind,index=index,target_ns=target,time_ns=clock())
    def invoke(op,ids,kind):
        objects=[object_for(plan,index) for index in ids]
        values=[payload(plan,obj) for obj in objects]
        begin=clock()
        actual=None
        error=None
        try:
            if op=='put':
                status=backend.put_many(objects,values)
                if status not in ('ack','error','unknown'):
                    raise ValueError('invalid PUT status')
            else:
                status,actual=backend.get(objects[0],values[0])
                if status not in ('hit','miss','error','unknown'):
                    raise ValueError('invalid GET status')
                if status=='hit' and (type(actual) is not bytes or actual!=values[0]):
                    status,error='error','wrong payload/length'
                if status!='hit' and actual is not None:
                    status,error='error','non-hit payload'
        except Exception as caught:
            status='unknown' if op=='put' else 'error'
            error=f'{type(caught).__name__}: {caught}'[:512]
        return dict(op=op,ids=ids,kind=kind,status=status,started_ns=begin,finished_ns=clock(),
                    observed_sha256=hashlib.sha256(actual).hexdigest() if type(actual) is bytes else None,
                    observed_length=len(actual) if type(actual) is bytes else 0,error=error)
    result=_execute(plan,invoke,schedule,backend.transport)
    if len(encode(result))>MAX_FILE_BYTES:
        raise ValueError('ledger bound exceeded')
    return result


def check(plan,report):
    events=iter(report['events'])
    controls=iter(report['controls'])
    previous=0
    def invoke(op,ids,kind):
        nonlocal previous
        event=next(events,None)
        if not isinstance(event,dict) or set(event)!={'op','ids','kind','status','started_ns','finished_ns','observed_sha256','observed_length','error'}:
            raise ValueError('missing/invalid event')
        if encode([event['op'],event['ids'],event['kind']])!=encode([op,ids,kind]):
            raise ValueError('event identity/order')
        _integer(event['started_ns'],'started',previous)
        _integer(event['finished_ns'],'finished',event['started_ns'])
        previous=event['finished_ns']
        if event['status'] not in (('ack','error','unknown') if op=='put' else ('hit','miss','error','unknown')):
            raise ValueError('invalid terminal')
        if event['error'] is not None and (type(event['error']) is not str or len(event['error'])>512):
            raise ValueError('invalid error')
        _integer(event['observed_length'],'observed_length')
        if op=='put' or event['status']=='miss':
            if event['observed_length']!=0 or event['observed_sha256'] is not None:
                raise ValueError('unexpected payload')
        if op=='get' and event['status']=='hit':
            expected=payload(plan,object_for(plan,ids[0]))
            if event['observed_length']!=len(expected) or event['observed_sha256']!=hashlib.sha256(expected).hexdigest() or event['error'] is not None:
                raise ValueError('unchecked payload')
        return event
    def schedule(kind,index,target):
        nonlocal previous
        row=next(controls,None)
        if not isinstance(row,dict) or set(row)!={'kind','index','target_ns','time_ns'} or encode([row['kind'],row['index'],row['target_ns']])!=encode([kind,index,target]):
            raise ValueError('schedule mismatch')
        _integer(row['time_ns'],'schedule time',max(previous,target))
        previous=row['time_ns']
        return row
    if report.get('transport') not in ('rdma','fake-rdma-test'):
        raise ValueError('invalid transport')
    expected=_execute(plan,invoke,schedule,report['transport'])
    if next(events,None) is not None or next(controls,None) is not None or encode(report)!=encode(expected):
        raise ValueError('changed ledger/accounting')
    return expected['summary']


class RdmaBackend(prefix.RdmaBackend):
    def put_many(self,objects,values):
        self.client.put([bytes.fromhex(obj['key_hex']) for obj in objects],values)
        return 'ack'


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    generator=commands.add_parser('generate')
    generator.add_argument('--output',required=True,type=Path)
    for command_name in ('run','check'):
        command=commands.add_parser(command_name)
        command.add_argument('--plan',required=True,type=Path)
        command.add_argument('--output',type=Path,required=command_name=='run')
        if command_name=='run':
            command.add_argument('--lib',required=True)
            command.add_argument('--members',required=True)
        else:
            command.add_argument('--ledger',required=True,type=Path)
    args=parser.parse_args(argv)
    backend=None
    try:
        if args.command=='generate':
            result=generate()
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
            backend=RdmaBackend(load_library(args.lib),args.members,b'prefix-delayed-v1\x00')
            result=replay(plan,backend)
            backend.close()
            backend=None
            # _write's smaller generic limit is deliberately not used here.
            with (args.output/'report.json').open('xb') as stream:
                stream.write(encode(result)+b'\n')
            print(json.dumps(result['summary'],sort_keys=True))
            return 0 if result['passed'] else 1
        print(json.dumps(result,sort_keys=True))
        return 0
    except (OSError,ValueError,RuntimeError,TypeError,KeyError,UnicodeError,RecursionError) as error:
        print('prefix delayed error: '+str(error),file=sys.stderr)
        return 1
    finally:
        if backend is not None:
            backend.close()


if __name__=='__main__':
    sys.exit(main())
