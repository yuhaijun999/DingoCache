#!/usr/bin/env python3
"""Independent paced RDMA prefix streams; not global open-loop or formal A/B.

Each worker owns one handle and serializes its own calls. Concurrent requests
may share immutable objects and refill them, but no atomic chain snapshot or
refill coalescing is promised. A parent process must bound hung native calls.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eviction_trace import encode
from prefix_delayed import load, MAX_FILE_BYTES
from prefix_chain import _fields, _integer, _seal
from prefix_rdma import BENEFIT_SCOPE, RdmaBackend, _ratio

PLAN_SCHEMA = 'dfkv-prefix-concurrent-plan-v1'
REPORT_SCHEMA = 'dfkv-prefix-concurrent-report-v1'
DEFAULTS = dict(seed=0xD1A60091, workers=4, rounds=120, interval_ns=100000000,
                prefill=12288, arrival_batch=16, length=4096, timeout_ns=2000000000)
NATIVE_KEYS = ('get_attempts', 'get_hits', 'get_misses', 'get_errors', 'get_unknown',
               'put_attempts', 'put_acks', 'put_bytes', 'put_errors', 'put_unknown')


def _write(path, document):
    raw = encode(document)
    if len(raw) > MAX_FILE_BYTES:
        raise ValueError('output exceeds 128 MiB file limit')
    with Path(path).open('xb') as output:
        output.write(raw)


def generate(**kwargs):
    if set(kwargs) - set(DEFAULTS):
        raise ValueError('unknown configuration')
    config = dict(DEFAULTS, **kwargs)
    for key, low, high in [('seed', 0, 2**64-1), ('workers', 1, 16),
                           ('rounds', 1, 600), ('interval_ns', 1000000, 1000000000),
                           ('prefill', 12, 12288), ('arrival_batch', 0, 64),
                           ('length', 1, 4096), ('timeout_ns', 1, 5000000000)]:
        _integer(config[key], key, low, high)
    # A conservative projected JSON budget rejects huge runs BEFORE I/O.
    # Nine GET/refill calls bound an eight-block chain stopped at first miss.
    calls = config['prefill']+12+config['workers']*config['rounds']*(config['arrival_batch']+9)
    if calls*650+config['workers']*config['rounds']*1300 > MAX_FILE_BYTES:
        raise ValueError('projected event ledger exceeds bounded JSON size')
    return _seal(dict(schema=PLAN_SCHEMA, synthetic=True, config=config))


def validate(plan):
    _fields(plan, {'schema', 'synthetic', 'config', 'sha256'}, 'plan')
    if not isinstance(plan['config'], dict) or set(plan['config']) != set(DEFAULTS):
        raise ValueError('invalid configuration')
    if encode(plan) != encode(generate(**plan['config'])):
        raise ValueError('plan differs from deterministic configuration/checksum')
    return plan['config']


def object_for(plan, index):
    config = plan['config']
    maximum = config['prefill'] + config['workers']*config['rounds']*config['arrival_batch']
    _integer(index, 'object_id', 0, maximum-1)
    return dict(object_id=index, namespace_hex=b'prefix-concurrent-v1\0'.hex(),
                key_hex=(b'pc\0'+config['seed'].to_bytes(8, 'big')+index.to_bytes(8, 'big')).hex(),
                generation=0, tokens=(index % 4+1)*8, length=config['length'])


def payload(plan, obj):
    return hashlib.shake_256(encode(dict(schema=PLAN_SCHEMA, seed=plan['config']['seed'],
                                        object=obj))).digest(obj['length'])


def request_for(plan, worker, round_index):
    """Rotating initialized chains can overlap hot chains with prefill <20."""
    config = plan['config']
    kind = ('hot-long', 'hot-branch', 'hot-short', 'initialized-rotating')[round_index % 4]
    if kind == 'hot-long':
        ids = list(range(8))
    elif kind == 'hot-branch':
        ids = [0, 1, 2, 3, 8, 9, 10, 11]
    elif kind == 'hot-short':
        ids = [0, 1]
    else:
        groups = (config['prefill']-12)//8
        start = 12+8*((worker*config['rounds']+round_index)//4 % groups) if groups else config['prefill']-8
        ids = list(range(start, start+8))
    begin = config['prefill']+(worker*config['rounds']+round_index)*config['arrival_batch']
    return dict(worker_id=worker, round=round_index, request_id=f'w{worker}-r{round_index}',
                kind=kind, object_ids=ids, arrival_ids=list(range(begin, begin+config['arrival_batch'])),
                target_tokens=sum(object_for(plan, index)['tokens'] for index in ids))


def _call(plan, backend, phase, worker, round_index, op, kind, index, deadline_ns=None):
    obj = object_for(plan, index)
    expected = payload(plan, obj)
    started = time.monotonic_ns()
    actual = error = None
    reset = getattr(backend, 'reset_call_timing', None)
    if reset is not None:
        reset()
    try:
        if deadline_ns is not None and started >= deadline_ns:
            status = 'expired'  # No backend or native call has begun.
        elif op == 'put':
            status = backend.put(obj, expected)
            if status not in ('ack', 'error', 'unknown'):
                raise RuntimeError('invalid PUT outcome')
        else:
            status, actual = backend.get(obj, expected)
            if status not in ('hit', 'miss', 'error', 'unknown'):
                raise RuntimeError('invalid GET outcome')
            if status == 'hit' and (type(actual) is not bytes or actual != expected):
                raise RuntimeError('GET payload mismatch')
            if status == 'miss' and actual is not None:
                raise RuntimeError('miss returned payload')
    except Exception as caught:
        status = 'unknown' if op == 'put' else 'error'
        error = f'{type(caught).__name__}: {caught}'[:512]
    timing = getattr(backend, 'last_call_timing', None)
    return dict(phase=phase, worker_id=worker, round=round_index, op=op, kind=kind,
                object_id=index, status=status, expected_sha256=hashlib.sha256(expected).hexdigest(),
                observed_sha256=hashlib.sha256(actual).hexdigest() if type(actual) is bytes else None,
                observed_length=len(actual) if type(actual) is bytes else 0,
                started_ns=started, finished_ns=time.monotonic_ns(), error=error,
                abi_started_ns=timing[0] if timing else None,
                abi_finished_ns=timing[1] if timing else None)


def _request(plan, identity, call, *, scheduled_ns, deadline_ns, started_ns, blocked=False):
    """The checker reuses this state machine with recorded native observations."""
    events = []
    state = 'not_attempted' if blocked else 'expired' if started_ns >= deadline_ns else 'completed'
    outcomes = ['not_attempted']*len(identity['object_ids'])
    usable = 0
    boundary = 0
    refill_acks = refill_bytes = 0
    def invoke(op, kind, index):
        nonlocal state
        event = call(op, kind, index)
        events.append(event)
        if event['status'] == 'expired':
            state = 'expired'
        elif event['status'] in ('error', 'unknown'):
            state = 'failed'
        elif event['finished_ns'] > deadline_ns:
            state = 'late'
        return event['status']
    if state == 'completed':
        for index in identity['arrival_ids']:
            invoke('put', 'arrival', index)
            if state != 'completed':
                break
    if state == 'completed':
        for position, index in enumerate(identity['object_ids']):
            status = invoke('get', identity['kind'], index)
            outcomes[position] = status
            if status == 'hit':
                usable += object_for(plan, index)['tokens']
                boundary = position+1
            if status != 'hit' or state != 'completed':
                boundary = position if status != 'hit' else position+1
                break
        # Freeze the first-read prefix BEFORE any suffix refill. Other workers
        # may refill overlapping objects, which never changes this request's U.
        if state == 'completed' and boundary < len(outcomes):
            for index in identity['object_ids'][boundary:]:
                if invoke('put', 'refill', index) == 'ack':
                    refill_acks += 1
                    refill_bytes += object_for(plan, index)['length']
                if state != 'completed':
                    break
    return dict(identity, scheduled_ns=scheduled_ns, deadline_ns=deadline_ns,
                started_ns=started_ns, finished_ns=events[-1]['finished_ns'] if events else started_ns,
                state=state, outcomes=outcomes, first_unconfirmed_block=boundary,
                confirmed_prefix_tokens=usable, usable_tokens=usable if state == 'completed' else None,
                refill_put_acks=refill_acks, refill_payload_bytes=refill_bytes,
                events=events)


def _native(events):
    counts = Counter()
    for event in events:
        op, status = event['op'], event['status']
        if status == 'expired':
            continue
        counts[op+'_attempts'] += 1
        key = ('put_acks' if status == 'ack' else 'put_errors' if status == 'error' else 'put_unknown') if op == 'put' else {
            'hit': 'get_hits', 'miss': 'get_misses', 'error': 'get_errors', 'unknown': 'get_unknown'}[status]
        counts[key] += 1
    return {key: counts[key] for key in NATIVE_KEYS}


def _summary(plan, requests, initial_events, handles, setup_error, origin_ns):
    events = initial_events+[event for row in requests for event in row['events']]
    def counts(values):
        result = _native(values)
        result['put_bytes'] = sum(object_for(plan, event['object_id'])['length'] for event in values
                                  if event['op'] == 'put' and event['status'] == 'ack')
        return result
    def overlap(start_key, finish_key, op=None):
        points = []
        for event in events:
            if (event['phase'] == 'measurement' and event['status'] != 'expired'
                    and (op is None or event['op'] == op) and event[start_key] is not None and event[finish_key] > event[start_key]):
                points.extend([(event[start_key], 1), (event[finish_key], -1)])
        active = peak = 0
        for _, change in sorted(points):  # Half-open spans: ends precede starts.
            active += change
            peak = max(peak, active)
        return peak
    target = sum(row['target_tokens'] for row in requests)
    unknown = sum(row['usable_tokens'] is None for row in requests)
    usable = sum(row['usable_tokens'] or 0 for row in requests)
    lower = sum(row['confirmed_prefix_tokens'] for row in requests)
    failed = bool(setup_error or not origin_ns or unknown or any(not item['closed'] for item in handles)
                  or any(item['close_error'] for item in handles))
    return dict(formal_ab=False, benefit_scope=BENEFIT_SCOPE,
                concurrency_model='independent-per-worker-paced-streams-no-global-queue',
                planned_requests=len(requests), completed_requests=sum(row['state']=='completed' for row in requests),
                states=dict(Counter(row['state'] for row in requests)), unknown_requests=unknown,
                target_tokens=target, known_usable_tokens=usable, confirmed_prefix_lower_bound_tokens=lower,
                token_reuse_ratio=_ratio(usable, target) if not unknown else None,
                token_reuse_lower_bound=_ratio(lower, target),
                full_prefix_hits=sum(row['usable_tokens']==row['target_tokens'] for row in requests),
                refill_put_acks=sum(row['refill_put_acks'] for row in requests),
                refill_payload_bytes=sum(row['refill_payload_bytes'] for row in requests),
                workers=plan['config']['workers'],
                peak_backend_overlap=overlap('started_ns', 'finished_ns'),
                peak_abi_overlap=overlap('abi_started_ns', 'abi_finished_ns'),
                peak_get_abi_overlap=overlap('abi_started_ns', 'abi_finished_ns', 'get'),
                peak_put_abi_overlap=overlap('abi_started_ns', 'abi_finished_ns', 'put'),
                abi_timed_measurement_calls={op: sum(event['phase']=='measurement' and event['op']==op
                                                     and event['abi_started_ns'] is not None for event in events)
                                             for op in ('get', 'put')},
                native=counts(events), by_phase={phase: counts([event for event in events if event['phase']==phase])
                                                for phase in ('initialization', 'warmup', 'measurement')},
                passed=not failed)


def replay(plan, backend_factory):
    """Open/close all handles here; only one thread ever calls each worker handle.

    A late blocking call is physically drained, not cancelled or replaced. Each
    worker aborts its own future rounds on call failure/late completion. Expired
    unstarted rounds remain unknown and subsequent fixed arrivals are retained.
    """
    config = validate(plan)
    handles, backends, initial_events, requests = [], {}, [], {}
    setup_error, origin = None, None
    def open_backend(worker):
        backend = backend_factory(worker)
        if any(backend is existing for existing in backends.values()):
            raise RuntimeError('backend factory reused a handle instance')
        metadata = dict(worker_id=worker, transport=getattr(backend, 'transport', None),
                        closed=False, close_error=None)
        handles.append(metadata)
        backends[worker] = backend
        if metadata['transport'] not in ('rdma', 'fake-rdma-test'):
            raise RuntimeError('RDMA transport required; no fallback')
        if any(item['transport'] != metadata['transport'] for item in handles):
            raise RuntimeError('mixed backend transports are not supported')
        return backend
    def close_backend(worker):
        metadata = next(item for item in handles if item['worker_id']==worker)
        try:
            backends[worker].close()
            metadata['closed'] = True
        except Exception as error:
            metadata['close_error'] = f'{type(error).__name__}: {error}'[:512]
    try:
        initializer = open_backend(-1)
        for index in list(range(12, config['prefill']))+list(range(12)):
            event = _call(plan, initializer, 'initialization', -1, -1, 'put', 'initialization', index)
            initial_events.append(event)
            if event['status'] != 'ack':
                raise RuntimeError('initialization PUT failed')
        # Warm-up is recorded separately and does not assume all near-capacity
        # objects survive prefill. Valid misses are allowed, with no refill.
        for index in range(12):
            event = _call(plan, initializer, 'warmup', -1, -1, 'get', 'warmup', index)
            initial_events.append(event)
            if event['status'] not in ('hit', 'miss'):
                raise RuntimeError('warmup GET failed')
        close_backend(-1)
        if handles[0]['close_error']:
            raise RuntimeError('initializer close failed')
        for worker in range(config['workers']):
            open_backend(worker)
        # The main thread publishes an origin only when all worker threads have
        # reached the barrier; no initialization cost enters measurement age.
        def set_origin():
            nonlocal origin
            origin = time.monotonic_ns()
        barrier = threading.Barrier(config['workers'], action=set_origin)
        def work(worker):
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                return  # Partial thread startup failed; parent records failure.
            blocked = False
            for round_index in range(config['rounds']):
                scheduled = origin+round_index*config['interval_ns']
                if not blocked:
                    remaining = scheduled-time.monotonic_ns()
                    if remaining > 0:
                        time.sleep(remaining/1e9)
                identity = request_for(plan, worker, round_index)
                row = _request(plan, identity,
                    lambda op, kind, index: _call(plan, backends[worker], 'measurement', worker,
                                                  round_index, op, kind, index, scheduled+config['timeout_ns']),
                    scheduled_ns=scheduled, deadline_ns=scheduled+config['timeout_ns'],
                    started_ns=time.monotonic_ns(), blocked=blocked)
                requests[(worker, round_index)] = row
                blocked = blocked or row['state'] in ('failed', 'late')
        threads = [threading.Thread(target=work, args=(worker,), name=f'prefix-worker-{worker}')
                   for worker in range(config['workers'])]
        started_threads = []
        try:
            for thread in threads:
                thread.start()
                started_threads.append(thread)
        finally:
            if len(started_threads) != len(threads):
                barrier.abort()
            for thread in started_threads:
                thread.join()
    except Exception as error:
        setup_error = f'{type(error).__name__}: {error}'[:512]
    finally:
        for worker in reversed(list(backends)):
            metadata = next(item for item in handles if item['worker_id']==worker)
            if not metadata['closed'] and metadata['close_error'] is None:
                close_backend(worker)
    rows = []
    for worker in range(config['workers']):
        for round_index in range(config['rounds']):
            row = requests.get((worker, round_index))
            if row is None:
                scheduled = origin+round_index*config['interval_ns'] if origin else 0
                row = _request(plan, request_for(plan, worker, round_index), None,
                               scheduled_ns=scheduled, deadline_ns=scheduled+config['timeout_ns'],
                               started_ns=scheduled, blocked=True)
            rows.append(row)
    summary = _summary(plan, rows, initial_events, handles, setup_error, origin)
    modes = {item['transport'] for item in handles}
    transport = next(iter(modes)) if len(modes) == 1 else 'mixed-or-unavailable'
    result = _seal(dict(schema=REPORT_SCHEMA, synthetic=True, formal_ab=False,
                        transport=transport, passed=summary['passed'],
                        benefit_scope=BENEFIT_SCOPE, plan_sha256=plan['sha256'],
                        origin_ns=origin, setup_error=setup_error, handles=handles,
                        initial_events=initial_events, requests=rows,
                        summary=summary))
    if len(encode(result)) > MAX_FILE_BYTES:
        raise ValueError('report exceeds bounded JSON size')
    return result


def check(plan, report):
    """Verify raw identities, sequential worker order and derived accounting.

    Shared request accounting is not an independent policy oracle. Recorded
    timestamps/digests are diagnostic claims, not authenticated execution proof.
    """
    config = validate(plan)
    _fields(report, {'schema', 'synthetic', 'formal_ab', 'benefit_scope', 'plan_sha256',
                     'origin_ns', 'setup_error', 'handles', 'initial_events', 'requests', 'summary', 'sha256',
                     'transport', 'passed'}, 'report')
    if (report['schema'] != REPORT_SCHEMA or report['synthetic'] is not True or report['formal_ab'] is not False
            or report['benefit_scope'] != BENEFIT_SCOPE or report['plan_sha256'] != plan['sha256']):
        raise ValueError('invalid report identity')
    origin = report['origin_ns']
    if origin is not None:
        _integer(origin, 'origin_ns', 1)
    if report['setup_error'] is not None and (not isinstance(report['setup_error'], str) or not report['setup_error']):
        raise ValueError('invalid setup error')
    handles = report['handles']
    if not isinstance(handles, list) or len(handles) > config['workers']+1:
        raise ValueError('invalid handles')
    for item, worker in zip(handles, range(-1, config['workers'])):
        _fields(item, {'worker_id', 'transport', 'closed', 'close_error'}, 'handle')
        if type(item['worker_id']) is not int or item['worker_id'] != worker or type(item['closed']) is not bool:
            raise ValueError('invalid handle identity')
        if item['transport'] is not None and not isinstance(item['transport'], str):
            raise ValueError('invalid transport')
        if item['closed'] and item['close_error'] is not None:
            raise ValueError('closed handle has close error')
        if item['close_error'] is not None and (not isinstance(item['close_error'], str) or not item['close_error']):
            raise ValueError('invalid close error')
    if origin and (len(handles) != config['workers']+1
                   or len({item['transport'] for item in handles}) != 1
                   or any(item['transport'] not in ('rdma', 'fake-rdma-test') for item in handles)):
        raise ValueError('measurement requires all RDMA worker handles')
    def event_check(event, phase, worker, round_index, op, kind, index, previous):
        _fields(event, {'phase', 'worker_id', 'round', 'op', 'kind', 'object_id', 'status',
                        'expected_sha256', 'observed_sha256', 'observed_length', 'started_ns', 'finished_ns', 'error',
                        'abi_started_ns', 'abi_finished_ns'}, 'event')
        for key, value in [('phase', phase), ('worker_id', worker), ('round', round_index), ('op', op), ('kind', kind), ('object_id', index)]:
            if encode(event[key]) != encode(value):
                raise ValueError('event identity/order mismatch')
        expected = payload(plan, object_for(plan, index))
        if event['expected_sha256'] != hashlib.sha256(expected).hexdigest():
            raise ValueError('expected payload digest mismatch')
        _integer(event['started_ns'], 'started_ns', previous)
        _integer(event['finished_ns'], 'finished_ns', event['started_ns'])
        _integer(event['observed_length'], 'observed_length')
        if (event['abi_started_ns'] is None) != (event['abi_finished_ns'] is None):
            raise ValueError('incomplete ABI timing')
        if (report['transport'] == 'rdma' and event['status'] in ('ack', 'hit', 'miss')
                and event['abi_started_ns'] is None):
            raise ValueError('successful RDMA call lacks direct ABI timing')
        if event['abi_started_ns'] is not None:
            _integer(event['abi_started_ns'], 'abi_started_ns', event['started_ns'])
            _integer(event['abi_finished_ns'], 'abi_finished_ns', event['abi_started_ns'])
            if event['abi_finished_ns'] > event['finished_ns'] or event['status'] == 'expired':
                raise ValueError('invalid ABI timing')
        if event['status'] not in (('ack', 'error', 'unknown', 'expired') if op == 'put' else ('hit', 'miss', 'error', 'unknown', 'expired')):
            raise ValueError('invalid native status')
        if event['error'] is not None and (not isinstance(event['error'], str) or len(event['error']) > 512):
            raise ValueError('invalid error')
        if event['status'] in ('ack', 'hit', 'miss', 'expired') and event['error'] is not None:
            raise ValueError('successful or unstarted call carries an error')
        if op == 'get' and event['status'] == 'hit':
            if event['observed_length'] != len(expected) or event['observed_sha256'] != event['expected_sha256'] or event['error'] is not None:
                raise ValueError('unchecked hit')
        if op == 'put' or event['status'] == 'miss':
            if event['observed_length'] or event['observed_sha256'] is not None:
                raise ValueError('unexpected payload')
        return event['finished_ns']
    initial = report['initial_events']
    if not isinstance(initial, list):
        raise ValueError('invalid initialization events')
    specs = [('initialization', 'put', 'initialization', index)
             for index in list(range(12, config['prefill']))+list(range(12))]
    specs += [('warmup', 'get', 'warmup', index) for index in range(12)]
    if len(initial) > len(specs) or origin and len(initial) != len(specs):
        raise ValueError('incomplete initialization')
    previous = 0
    for position, event in enumerate(initial):
        phase, op, kind, index = specs[position]
        previous = event_check(event, phase, -1, -1, op, kind, index, previous)
        if event['status'] not in (('ack',) if op == 'put' else ('hit', 'miss')):
            if position != len(initial)-1 or report['setup_error'] is None or origin:
                raise ValueError('invalid initialization failure')
    if origin and previous > origin:
        raise ValueError('measurement preceded initialization completion')
    rows = report['requests']
    if not isinstance(rows, list) or len(rows) != config['workers']*config['rounds']:
        raise ValueError('missing planned request denominator')
    derived = []
    for worker in range(config['workers']):
        previous, blocked = origin or 0, not bool(origin)
        for round_index in range(config['rounds']):
            row = rows[worker*config['rounds']+round_index]
            identity = request_for(plan, worker, round_index)
            scheduled = origin+round_index*config['interval_ns'] if origin else 0
            _integer(row.get('started_ns'), 'request started_ns', previous)
            if origin and not blocked and row['started_ns'] < scheduled:
                raise ValueError('request began before its scheduled arrival')
            events = row.get('events')
            if not isinstance(events, list):
                raise ValueError('invalid request events')
            position, last = 0, row['started_ns']
            def observed(op, kind, index):
                nonlocal position, last
                if position >= len(events):
                    raise ValueError('missing native event')
                event = events[position]
                position += 1
                last = event_check(event, 'measurement', worker, round_index, op, kind, index, last)
                if event['status'] == 'expired':
                    if event['started_ns'] < scheduled+config['timeout_ns']:
                        raise ValueError('unjustified expired native call')
                elif event['started_ns'] >= scheduled+config['timeout_ns']:
                    raise ValueError('backend call began after deadline')
                return event
            expected = _request(plan, identity, observed, scheduled_ns=scheduled,
                                deadline_ns=scheduled+config['timeout_ns'],
                                started_ns=row['started_ns'], blocked=blocked)
            if position != len(events) or encode(row) != encode(expected):
                raise ValueError('request differs from raw observations')
            derived.append(expected)
            previous = expected['finished_ns']
            blocked = blocked or expected['state'] in ('failed', 'late')
    summary = _summary(plan, derived, initial, handles, report['setup_error'], origin)
    modes = {item['transport'] for item in handles}
    transport = next(iter(modes)) if len(modes) == 1 else 'mixed-or-unavailable'
    expected = _seal(dict(report, summary=summary, transport=transport, passed=summary['passed']))
    if encode(report) != encode(expected):
        raise ValueError('report summary/checksum mismatch')
    return summary


class _TimedLibrary:
    """Per-handle facade; never mutate the shared CDLL function attributes."""
    def __init__(self, library):
        self.library = library
        self.timing = None

    def __getattr__(self, name):
        function = getattr(self.library, name)
        if name not in ('dfkv_get_auto_status', 'dfkv_batch_put'):
            return function
        def invoke(*args):
            started = time.monotonic_ns()
            try:
                return function(*args)
            finally:
                self.timing = (started, time.monotonic_ns())
        return invoke


class NativeBackend(RdmaBackend):
    """Direct CDLL call brackets exclude Python preparation and validation.

    ctypes releases the GIL inside these calls. Brackets prove simultaneous
    in-flight C ABI invocations, not simultaneous disk I/O or CPU execution.
    """
    def __init__(self, library, members, namespace):
        self.timed_library = _TimedLibrary(library)
        super().__init__(self.timed_library, members, namespace)

    def reset_call_timing(self):
        self.timed_library.timing = None

    @property
    def last_call_timing(self):
        return self.timed_library.timing


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    generator = commands.add_parser('generate')
    generator.add_argument('--output', type=Path, required=True)
    for key, value in DEFAULTS.items():
        generator.add_argument('--'+key.replace('_', '-'), type=int, default=value)
    for name in ('run', 'check'):
        command = commands.add_parser(name)
        command.add_argument('--plan', type=Path, required=True)
        if name == 'run':
            command.add_argument('--output', type=Path, required=True)
            command.add_argument('--lib', required=True)
            command.add_argument('--members', required=True)
        else:
            command.add_argument('--ledger', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'generate':
            result = generate(**{key: getattr(args, key) for key in DEFAULTS})
            _write(args.output, result)
        elif args.command == 'check':
            result = check(load(args.plan), load(args.ledger))
        else:
            plan = load(args.plan)
            validate(plan)
            args.output.mkdir(exist_ok=False)
            sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'integration/common/src'))
            from disk_scenarios import load_library
            library = load_library(args.lib)
            namespace = bytes.fromhex(object_for(plan, 0)['namespace_hex'])
            result = replay(plan, lambda worker: NativeBackend(library, args.members, namespace))
            check(plan, result)
            _write(args.output/'report.json', result)
            print(json.dumps(result['summary'], sort_keys=True))
            return 0 if result['summary']['passed'] else 1
        print(json.dumps(result, sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError, TypeError, UnicodeError, RecursionError) as error:
        print(f'prefix concurrent error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
