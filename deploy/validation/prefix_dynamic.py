#!/usr/bin/env python3
"""Bounded serial stateful fake prefix cache; not legacy/lifetime or formal A/B.

All actions are a finite immutable workload, not a clock/window or threaded
concurrency model. Request failure stimuli are separate from workload actions and observations.
FIFO by application payload bytes is only a test fixture. Replaying the same
model checks evidence consistency, not independent algorithm correctness.
"""

import argparse
from collections import Counter, OrderedDict
from fractions import Fraction
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prefix_chain import _digest, _fields, _integer, _seal, _text, _write
from eviction_trace import checksum, encode, load, MAX_FILE_BYTES

PLAN_SCHEMA = 'dfkv-prefix-dynamic-plan-v1'
LEDGER_SCHEMA = 'dfkv-prefix-dynamic-ledger-v1'
MAX_ACTIONS = 4096
MAX_OBJECTS = 1024
MAX_BLOCKS = 128
MAX_TOTAL_BLOCKS = 16384
PHASE_STATES = {'lookup': ('io_error', 'timeout', 'unknown'),
                'load': ('rejected', 'io_error', 'timeout', 'unknown'),
                'refill': ('io_error',)}


def validate(plan):
    """Reject unsupported modes, ambiguous dependencies and invalid fault slots.

    Replay additionally rejects valid slots that are never reached."""
    _fields(plan, {'schema', 'synthetic', 'replacement', 'capacity_bytes',
                   'objects', 'actions', 'faults', 'sha256'}, 'plan')
    if (plan['schema'] != PLAN_SCHEMA or plan['synthetic'] is not True or
            plan['replacement'] != 'test-fifo-bytes'):
        raise ValueError('only synthetic test-fifo-bytes is supported')
    _integer(plan['capacity_bytes'], 'capacity_bytes', 1, 2**40)
    objects, actions, faults = plan['objects'], plan['actions'], plan['faults']
    for rows, limit, name in [(objects, MAX_OBJECTS, 'objects'),
                               (actions, MAX_ACTIONS, 'actions'),
                               (faults, MAX_TOTAL_BLOCKS, 'faults')]:
        if not isinstance(rows, list) or len(rows) > limit:
            raise ValueError(f'invalid or too many {name}')
    catalog, identities = {}, set()
    for obj in objects:
        _fields(obj, {'object_id', 'namespace', 'key', 'generation', 'tokens', 'bytes'}, 'object')
        for name in ('object_id', 'namespace', 'key'):
            _text(obj[name], name)
        _integer(obj['generation'], 'generation')
        _integer(obj['tokens'], 'tokens', 1, 1000000)
        _integer(obj['bytes'], 'bytes', 1, 2**40)
        identity = (obj['namespace'], obj['key'], obj['generation'])
        if obj['object_id'] in catalog or identity in identities:
            raise ValueError('duplicate object id/physical identity, including metadata aliases')
        catalog[obj['object_id']] = obj
        identities.add(identity)
    requests, prefixes, total = set(), {}, 0
    for action in actions:
        if not isinstance(action, dict):
            raise ValueError('invalid action')
        op = action.get('op')
        if op in ('seed', 'put'):
            _fields(action, {'op', 'object_id', 'status'}, 'write')
            if action['status'] not in ('ack', 'io_error'):
                raise ValueError('writes require definite ack/io_error; unknown mutation unsupported')
            ids = [action['object_id']]
        elif op == 'remove':
            _fields(action, {'op', 'object_id'}, 'remove')
            ids = [action['object_id']]
        elif op == 'request':
            _fields(action, {'op', 'request_id', 'chain_id', 'object_ids', 'refill'}, 'request')
            for name in ('request_id', 'chain_id'):
                _text(action[name], name)
            if action['request_id'] in requests or type(action['refill']) is not bool:
                raise ValueError('duplicate request or invalid refill flag')
            requests.add(action['request_id'])
            ids = action['object_ids']
            if not isinstance(ids, list) or len(ids) > MAX_BLOCKS:
                raise ValueError('invalid or too many request blocks')
            if any(type(key) is not str for key in ids) or len(set(ids)) != len(ids):
                raise ValueError('invalid or duplicate request object')
            total += len(ids)
            for index, key in enumerate(ids):
                prefix = tuple(ids[:index + 1])
                if key in prefixes and prefixes[key] != prefix:
                    raise ValueError('shared object has incompatible ordered dependencies')
                prefixes[key] = prefix
        else:
            raise ValueError('unsupported action')
        if any(type(key) is not str or key not in catalog for key in ids):
            raise ValueError('unknown object_id')
    if total > MAX_TOTAL_BLOCKS:
        raise ValueError('too many total request blocks')
    slots = set()
    for fault in faults:
        _fields(fault, {'action_index', 'block_index', 'phase', 'status'}, 'fault')
        _integer(fault['action_index'], 'action_index', 0, len(actions) - 1)
        action = actions[fault['action_index']]
        if action['op'] != 'request':
            raise ValueError('fault must target request')
        _integer(fault['block_index'], 'block_index', 0, len(action['object_ids']) - 1)
        phase = fault['phase']
        if type(phase) is not str or phase not in PHASE_STATES or fault['status'] not in PHASE_STATES[phase]:
            raise ValueError('invalid fault phase/status')
        slot = (fault['action_index'], fault['block_index'], phase)
        if slot in slots:
            raise ValueError('duplicate fault slot')
        slots.add(slot)
    _digest(plan)
    return catalog


def _ratio(numerator, denominator):
    if not denominator:
        return None
    value = Fraction(numerator, denominator)
    return dict(numerator=value.numerator, denominator=value.denominator)


class FakeCache:
    """Fresh in-memory FIFO fixture; no native backend, pins, clocks or disk I/O."""

    def __init__(self, catalog, capacity):
        self.catalog = catalog
        self.capacity = capacity
        self.residents = OrderedDict()
        self.used_bytes = 0
        self.adopted_objects = set()
        self.events = []
        self.counts = Counter()

    def event(self, action_index, phase, key, status, **extra):
        obj = self.catalog[key]
        row = dict(action_index=action_index, phase=phase, object_id=key,
                   namespace=obj['namespace'], key=obj['key'], generation=obj['generation'],
                   tokens=obj['tokens'], bytes=obj['bytes'], status=status, **extra)
        self.events.append(row)
        return row

    def write(self, action_index, phase, key, status):
        size = self.catalog[key]['bytes']
        if status == 'ack' and size > self.capacity:
            status = 'oversize'
        if phase == 'refill':
            self.counts['refill_attempts'] += 1
            self.counts['refill_attempt_bytes'] += size
        if status == 'ack':
            if key in self.residents:
                self.used_bytes -= self.residents.pop(key)
            while self.used_bytes + size > self.capacity:
                victim, victim_size = self.residents.popitem(last=False)
                self.used_bytes -= victim_size
                self.counts['evictions'] += 1
                if victim in self.adopted_objects:
                    self.counts['evicted_previously_adopted_objects'] += 1
                self.event(action_index, 'evict', victim, 'evicted',
                           previously_adopted=victim in self.adopted_objects)
            self.residents[key] = size
            self.used_bytes += size
            self.counts[f'{phase}_ack_bytes'] += size
        elif status == 'oversize':
            self.counts['oversize_writes'] += 1
        self.event(action_index, phase, key, status)
        return status


def _request(cache, index, action, fault_map, consumed):
    ids = action['object_ids']
    event_start = len(cache.events)
    lookup, loads = ['not_attempted'] * len(ids), ['not_attempted'] * len(ids)

    def observe(block_index, phase, default):
        slot = (index, block_index, phase)
        if slot in fault_map:
            consumed.add(slot)
        status = fault_map.get(slot, default)
        cache.event(index, phase, ids[block_index], status, block_index=block_index)
        return status

    for block_index, key in enumerate(ids):
        lookup[block_index] = observe(block_index, 'lookup',
                                      'hit' if key in cache.residents else 'miss')
        if lookup[block_index] != 'hit':
            break
    adopted, candidate, known, boundary = 0, 0, True, len(ids)
    for block_index, key in enumerate(ids):
        if lookup[block_index] != 'hit':
            boundary = block_index
            break
        candidate += cache.catalog[key]['tokens']
    for block_index in range(boundary):
        key = ids[block_index]
        loads[block_index] = observe(block_index, 'load', 'adopted')
        if loads[block_index] != 'adopted':
            boundary = block_index
            known = loads[block_index] not in ('timeout', 'unknown')
            break
        adopted += cache.catalog[key]['tokens']
        cache.adopted_objects.add(key)
    # Freeze original-request benefit BEFORE any recompute/refill changes state.
    target = sum(cache.catalog[key]['tokens'] for key in ids)
    suppressed = None
    proxy = 0
    if action['refill'] and boundary < len(ids):
        if not known:
            suppressed = 'unknown_adoption'
        elif any(state in ('io_error', 'timeout', 'unknown') for state in lookup + loads):
            suppressed = 'read_failure'
        else:
            proxy = target - adopted
            for block_index in range(boundary, len(ids)):
                slot = (index, block_index, 'refill')
                if slot in fault_map:
                    consumed.add(slot)
                cache.write(index, 'refill', ids[block_index], fault_map.get(slot, 'ack'))
    return dict(action_index=index, request_id=action['request_id'], chain_id=action['chain_id'],
                object_ids=list(ids), target_tokens=target, candidate_tokens=candidate,
                adopted_tokens=adopted if known else None,
                adopted_lower_bound_tokens=adopted, first_unconfirmed_block=boundary,
                adoption_known=known, lookup=lookup, load=loads,
                recompute_proxy_tokens=proxy, refill_suppressed=suppressed,
                events=cache.events[event_start:])


def replay(plan):
    """Execute finite serial actions; every business request retains its denominator."""
    catalog = validate(plan)
    cache = FakeCache(catalog, plan['capacity_bytes'])
    fault_map = {(f['action_index'], f['block_index'], f['phase']): f['status'] for f in plan['faults']}
    consumed, requests, states = set(), [], []
    for index, action in enumerate(plan['actions']):
        op = action['op']
        if op in ('seed', 'put'):
            cache.write(index, op, action['object_id'], action['status'])
        elif op == 'remove':
            key = action['object_id']
            size = cache.residents.pop(key, None)
            if size is not None:
                cache.used_bytes -= size
                cache.counts['remove_hits'] += 1
            cache.event(index, 'remove', key, 'removed' if size is not None else 'absent')
        else:
            requests.append(_request(cache, index, action, fault_map, consumed))
        states.append(dict(action_index=index, occupancy_bytes=cache.used_bytes,
                           resident_count=len(cache.residents)))
    if consumed != set(fault_map):
        raise ValueError('unreached fault injection; no silent scenario coverage')
    rows = [row for row in requests if row['target_tokens']]
    target = sum(row['target_tokens'] for row in rows)
    lower = sum(row['adopted_lower_bound_tokens'] for row in rows)
    known = sum(row['adopted_tokens'] for row in rows if row['adoption_known'])
    unknown = sum(not row['adoption_known'] for row in rows)
    full = sum(row['adoption_known'] and row['adopted_tokens'] == row['target_tokens'] for row in rows)
    exact = bool(rows) and not unknown
    summary = dict(synthetic=True, fake_only=True, formal_ab=False,
                   replacement='test-fifo-bytes', denominator='all-nonempty-request-actions',
                   planned_requests=len(requests), eligible_requests=len(rows),
                   zero_target_requests=len(requests)-len(rows), unknown_requests=unknown,
                   target_tokens=target, known_adopted_tokens=known,
                   confirmed_adopted_lower_bound_tokens=lower,
                   token_reuse_ratio=_ratio(known, target) if exact else None,
                   full_prefix_hit_rate=_ratio(full, len(rows)) if exact else None,
                   mean_adopted_tokens=_ratio(known, len(rows)) if exact else None,
                   mean_request_reuse_ratio=_ratio(sum(Fraction(row['adopted_tokens'], row['target_tokens'])
                                                       for row in rows), len(rows)) if exact else None,
                   token_reuse_lower_bound=_ratio(lower, target),
                   full_prefix_hit_lower_bound=_ratio(full, len(rows)),
                   recompute_proxy_tokens=sum(row['recompute_proxy_tokens'] for row in rows),
                   occupancy_bytes=cache.used_bytes,
                   status='not_applicable' if not rows else 'inconclusive' if unknown else 'measured')
    for key in ('refill_attempts', 'refill_attempt_bytes', 'refill_ack_bytes', 'put_ack_bytes',
                'seed_ack_bytes', 'evictions', 'evicted_previously_adopted_objects',
                'oversize_writes', 'remove_hits'):
        summary[key] = cache.counts[key]
    for phase, statuses in [('lookup', ('hit', 'miss', 'io_error', 'timeout', 'unknown')),
                             ('load', ('adopted', 'rejected', 'io_error', 'timeout', 'unknown'))]:
        counts = Counter(event['status'] for event in cache.events if event['phase'] == phase)
        summary[f'{phase}_outcomes'] = {status: counts[status] for status in statuses}
    ledger = _seal(dict(schema=LEDGER_SCHEMA, synthetic=True, fake_only=True, formal_ab=False,
                        plan_sha256=plan['sha256'], requests=requests, events=cache.events,
                        action_states=states, final_residents=list(cache.residents), summary=summary))
    if len(encode(ledger)) > MAX_FILE_BYTES:
        raise ValueError('ledger exceeds bounded file limit')
    return ledger


def check(plan, ledger):
    """Re-execute the SAME model; consistency check, not independent hardware proof."""
    expected = replay(plan)
    if encode(ledger) != encode(expected):
        raise ValueError('ledger differs from deterministic state transitions/accounting')
    return expected['summary']


def generate():
    """Shared ABC/ABD: first fill, later reuse, then pressure loses chain head."""
    objects = [dict(object_id=key, namespace='demo', key=key, generation=0, tokens=10, bytes=1)
               for key in 'ABCD']
    def request(name, keys, refill=False):
        return dict(op='request', request_id=name, chain_id=keys,
                    object_ids=list(keys), refill=refill)
    actions = [dict(op='seed', object_id=key, status='ack') for key in 'AB']
    actions += [request('first', 'ABC', True), request('branch', 'ABD'),
                request('repeat', 'ABC'), dict(op='put', object_id='D', status='ack'),
                request('after-pressure', 'ABC')]
    return _seal(dict(schema=PLAN_SCHEMA, synthetic=True, replacement='test-fifo-bytes',
                      capacity_bytes=3, objects=objects, actions=actions, faults=[]))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    generate_parser = commands.add_parser('generate')
    generate_parser.add_argument('--output', type=Path, required=True)
    for name in ('replay', 'check'):
        command = commands.add_parser(name)
        command.add_argument('--plan', type=Path, required=True)
        command.add_argument('--output', type=Path, required=name == 'replay')
        if name == 'check':
            command.add_argument('--ledger', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'generate':
            result = generate()
        else:
            plan = load(args.plan)
            result = replay(plan) if args.command == 'replay' else check(plan, load(args.ledger))
        if args.output:
            _write(args.output, result)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f'prefix dynamic error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
