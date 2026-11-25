#!/usr/bin/env python3
"""Bounded offline prefix-chain accounting with a scripted fake backend.

V1 supports linear, fixed dependencies, one object per positive-token block and
no local cache. This is not an eviction-policy simulator, engine adapter, RDMA
proof or formal A/B. Lookup and adoption are separate fake calls. Refills,
verification reads, multiple shards, checkpoint masks and local reuse are not
implemented and cannot be encoded as extra fields.
"""

import argparse
from fractions import Fraction
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eviction_trace import checksum, encode, load, MAX_FILE_BYTES

TRACE_SCHEMA = 'dfkv-prefix-trace-v1'
SCENARIO_SCHEMA = 'dfkv-prefix-fake-scenario-v1'
LEDGER_SCHEMA = 'dfkv-prefix-fake-ledger-v1'
MODE = 'linear-single-object-no-local'
MAX_REQUESTS = 10000
MAX_BLOCKS = 256
MAX_TOTAL_BLOCKS = 65536
MAX_INT = 2**63 - 1
LOOKUP_STATES = ('hit', 'miss', 'stale', 'io_error', 'timeout', 'unknown')
LOAD_STATES = ('adopted', 'rejected', 'io_error', 'timeout', 'unknown')
IDENTITY_FIELDS = {'namespace', 'key', 'generation', 'tokens'}


def _fields(value, fields, name):
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f'invalid {name} fields')


def _integer(value, name, minimum=0, maximum=MAX_INT):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f'{name} must be an integer in [{minimum}, {maximum}]')


def _text(value, name):
    if not isinstance(value, str) or not 1 <= len(value) <= 256 or any(ord(c) < 32 for c in value):
        raise ValueError(f'invalid {name}')


def _digest(document):
    digest = document['sha256']
    if not isinstance(digest, str) or len(digest) != 64 or digest != checksum(document):
        raise ValueError('invalid checksum; digests are identity checks, not signatures')


def _seal(document):
    document['sha256'] = checksum(document)
    return document


def validate_trace(trace):
    """Reject unsupported features rather than silently approximate them."""
    _fields(trace, {'schema', 'synthetic', 'mode', 'window_ns', 'requests', 'sha256'}, 'trace')
    if trace['schema'] != TRACE_SCHEMA or trace['synthetic'] is not True or trace['mode'] != MODE:
        raise ValueError('unsupported trace schema/mode; synthetic V1 only')
    window = trace['window_ns']
    if not isinstance(window, list) or len(window) != 2:
        raise ValueError('window_ns must contain start and exclusive end')
    for value in window:
        _integer(value, 'window_ns')
    if window[0] >= window[1]:
        raise ValueError('window must have positive duration')
    requests = trace['requests']
    if not isinstance(requests, list) or len(requests) > MAX_REQUESTS:
        raise ValueError('too many or invalid requests')
    seen_ids, object_tokens = set(), {}
    previous_ns, total_blocks = -1, 0
    for request in requests:
        _fields(request, {'request_id', 'chain_id', 'scheduled_ns', 'blocks'}, 'request')
        for field in ('request_id', 'chain_id'):
            _text(request[field], field)
        if request['request_id'] in seen_ids:
            raise ValueError('duplicate request_id')
        seen_ids.add(request['request_id'])
        scheduled = request['scheduled_ns']
        _integer(scheduled, 'scheduled_ns')
        if scheduled < previous_ns:
            raise ValueError('requests must be in scheduled order')
        previous_ns = scheduled
        blocks = request['blocks']
        if not isinstance(blocks, list) or len(blocks) > MAX_BLOCKS:
            raise ValueError('invalid or too many blocks')
        total_blocks += len(blocks)
        if total_blocks > MAX_TOTAL_BLOCKS:
            raise ValueError('too many total blocks')
        request_objects = set()
        for block in blocks:
            _fields(block, IDENTITY_FIELDS, 'block')
            for field in ('namespace', 'key'):
                _text(block[field], field)
            _integer(block['generation'], 'generation')
            _integer(block['tokens'], 'tokens', 1, 1000000)
            identity = (block['namespace'], block['key'], block['generation'])
            if identity in request_objects:
                raise ValueError('duplicate object identity within a chain')
            request_objects.add(identity)
            if identity in object_tokens and object_tokens[identity] != block['tokens']:
                raise ValueError('conflicting tokens for shared object identity')
            object_tokens[identity] = block['tokens']
    _digest(trace)


def validate_scenario(trace, scenario):
    """The fake outcomes are separate from the immutable business trace."""
    validate_trace(trace)
    _fields(scenario, {'schema', 'trace_sha256', 'call_ns', 'requests', 'sha256'}, 'scenario')
    if scenario['schema'] != SCENARIO_SCHEMA or scenario['trace_sha256'] != trace['sha256']:
        raise ValueError('scenario does not match trace')
    _integer(scenario['call_ns'], 'call_ns', 0, 1000000000)
    scripts = scenario['requests']
    if not isinstance(scripts, list) or len(scripts) != len(trace['requests']):
        raise ValueError('scenario must cover every request')
    for request, script in zip(trace['requests'], scripts):
        _fields(script, {'request_id', 'lookup', 'load'}, 'scenario request')
        if script['request_id'] != request['request_id']:
            raise ValueError('scenario request identity/order mismatch')
        for phase, states in [('lookup', LOOKUP_STATES), ('load', LOAD_STATES)]:
            values = script[phase]
            if not isinstance(values, list) or len(values) != len(request['blocks']):
                raise ValueError('scenario must cover every block')
            if any(value not in states for value in values):
                raise ValueError(f'invalid {phase} state')
    _digest(scenario)


def generate():
    """Four 64-token requests: tail/middle/head miss and all hit; no winner."""
    blocks = [dict(namespace='demo', key=key, generation=0, tokens=16) for key in 'ABCD']
    ids = ['tail-miss', 'middle-miss', 'head-miss', 'all-hit']
    trace = _seal(dict(schema=TRACE_SCHEMA, synthetic=True, mode=MODE, window_ns=[0, 4],
                       requests=[dict(request_id=name, chain_id='shared-chain', scheduled_ns=index,
                                      blocks=[dict(block) for block in blocks])
                                 for index, name in enumerate(ids)]))
    states = [['hit', 'hit', 'hit', 'miss'], ['hit', 'miss', 'hit', 'hit'],
              ['miss', 'hit', 'hit', 'hit'], ['hit'] * 4]
    scenario = _seal(dict(schema=SCENARIO_SCHEMA, trace_sha256=trace['sha256'], call_ns=1,
                          requests=[dict(request_id=name, lookup=lookup, load=['adopted'] * 4)
                                    for name, lookup in zip(ids, states)]))
    return trace, scenario


class FakeBackend:
    """A deterministic call script, not a residency or capacity simulation."""

    def __init__(self, scenario):
        self.scripts = {row['request_id']: row for row in scenario['requests']}
        self.call_ns = scenario['call_ns']
        self.scenario_sha256 = scenario['sha256']
        self.now_ns = 0
        self.calls = []
        self.used = False

    def start(self, scheduled_ns):
        self.now_ns = max(self.now_ns, scheduled_ns)
        return self.now_ns

    def _call(self, phase, request_id, index, block):
        self.calls.append(dict(phase=phase, request_id=request_id, block_index=index,
                               namespace=block['namespace'], key=block['key'],
                               generation=block['generation']))
        self.now_ns += self.call_ns
        return self.scripts[request_id][phase][index]

    def lookup(self, request_id, index, block):
        return self._call('lookup', request_id, index, block)

    def load(self, request_id, index, block):
        return self._call('load', request_id, index, block)


def _derive(request, observations, script, window, start_ns, call_ns):
    """Validate raw phase order and derive facts without trusting row summaries."""
    if not isinstance(observations, list) or len(observations) != len(request['blocks']):
        raise ValueError('ledger must contain every block exactly once')
    in_window = window[0] <= request['scheduled_ns'] < window[1]
    eligible = in_window and bool(observations)
    lookup_open, candidate, lookup_stop, calls = eligible, 0, None, 0
    for index, (block, observed) in enumerate(zip(request['blocks'], observations)):
        _fields(observed, IDENTITY_FIELDS | {'lookup', 'load'}, 'observed block')
        if encode({key: observed[key] for key in IDENTITY_FIELDS}) != encode(block):
            raise ValueError('ledger block identity/tokens mismatch')
        expected = script['lookup'][index] if lookup_open else 'not_attempted'
        if observed['lookup'] != expected:
            raise ValueError('lookup result/script or short-circuit mismatch')
        if lookup_open:
            calls += 1
            if expected == 'hit':
                candidate += block['tokens']
            else:
                lookup_open, lookup_stop = False, expected
    load_open, adopted, load_stop = eligible, 0, None
    for index, (block, observed) in enumerate(zip(request['blocks'], observations)):
        attempted = load_open and observed['lookup'] == 'hit'
        expected = script['load'][index] if attempted else 'not_attempted'
        if observed['load'] != expected:
            raise ValueError('load result/script or short-circuit mismatch')
        if attempted:
            calls += 1
            if expected == 'adopted':
                adopted += block['tokens']
            else:
                load_open, load_stop = False, expected
        else:
            load_open = False
    # Lookup unknown leaves an incomplete candidate observation, but this fake
    # never loads its suffix. Only unknown LOAD adoption makes actual U unknown.
    unknown = load_stop in ('timeout', 'unknown')
    terminal = ('outside_window' if not in_window else 'zero_target' if not observations else
                'load_' + load_stop if load_stop else 'lookup_' + lookup_stop if lookup_stop else 'complete')
    return dict(request_id=request['request_id'], chain_id=request['chain_id'],
                scheduled_ns=request['scheduled_ns'], blocks=observations,
                terminal=terminal, candidate_tokens=candidate,
                candidate_observation_complete=eligible and lookup_stop not in ('io_error', 'timeout', 'unknown'),
                adopted_tokens=None if unknown else adopted,
                adopted_lower_bound_tokens=adopted,
                started_ns=start_ns if eligible else None,
                finished_ns=start_ns + calls * call_ns if eligible else None)


def _fraction(numerator, denominator):
    if not denominator:
        return None
    value = Fraction(numerator, denominator)
    return dict(numerator=value.numerator, denominator=value.denominator)


def _aggregate(trace, rows):
    eligible = [row for row in rows if row['terminal'] not in ('outside_window', 'zero_target')]
    targets = [sum(block['tokens'] for block in row['blocks']) for row in eligible]
    unknown = sum(row['adopted_tokens'] is None for row in eligible)
    known_sum = sum(row['adopted_tokens'] or 0 for row in eligible)
    lower_sum = sum(row['adopted_lower_bound_tokens'] for row in eligible)
    n, total = len(eligible), sum(targets)
    full_known = sum(row['adopted_tokens'] == target for row, target in zip(eligible, targets))
    lower_ratio_sum = sum((Fraction(row['adopted_lower_bound_tokens'], target)
                           for row, target in zip(eligible, targets)), Fraction(0))
    lower = dict(mean_adopted_tokens=_fraction(lower_sum, n),
                 token_reuse_ratio=_fraction(lower_sum, total),
                 mean_request_reuse_ratio=_fraction(lower_ratio_sum, n),
                 full_prefix_hit_rate=_fraction(full_known, n))
    outcomes = {phase + '_outcomes': {state: 0 for state in states}
                for phase, states in [('lookup', LOOKUP_STATES), ('load', LOAD_STATES)]}
    breakpoints = []
    for row in eligible:
        breakpoint = None
        for index, block in enumerate(row['blocks']):
            for phase in ('lookup', 'load'):
                if block[phase] != 'not_attempted':
                    outcomes[phase + '_outcomes'][block[phase]] += 1
            if breakpoint is None and (block['lookup'] != 'hit' or block['load'] != 'adopted'):
                breakpoint = index
        breakpoints.append(dict(request_id=row['request_id'], first_unadopted_block=breakpoint,
                                actual_boundary_known=row['adopted_tokens'] is not None))
    return dict(synthetic=True, fake_only=True, formal_ab=False, mode=MODE,
                planned_requests=len(trace['requests']), eligible_requests=n,
                censored_requests=sum(row['terminal'] == 'outside_window' for row in rows),
                zero_target_requests=sum(row['terminal'] == 'zero_target' for row in rows),
                target_tokens=total, known_requests=n - unknown, unknown_requests=unknown,
                known_adopted_tokens=known_sum, confirmed_adopted_lower_bound_tokens=lower_sum,
                candidate_tokens=sum(row['candidate_tokens'] for row in eligible),
                candidate_incomplete_requests=sum(not row['candidate_observation_complete'] for row in eligible),
                unreused_target_tokens=None if unknown else total - known_sum,
                metrics={key: None for key in lower} if unknown else dict(lower),
                lower_bounds=lower, first_breakpoints=breakpoints,
                status='not_applicable' if not n else 'inconclusive' if unknown else 'measured',
                **outcomes)


def replay(trace, scenario, *, backend=None):
    """Execute only the fake sequential contract, preserving all planned rows."""
    validate_scenario(trace, scenario)
    backend = backend if backend is not None else FakeBackend(scenario)
    if type(backend) is not FakeBackend:
        raise ValueError('replay accepts only the offline FakeBackend')
    if backend.scenario_sha256 != scenario['sha256']:
        raise ValueError('FakeBackend scenario does not match the current scenario')
    if backend.used or backend.now_ns or backend.calls:
        raise ValueError('a fresh, unused FakeBackend is required')
    backend.used = True
    rows = []
    for request, script in zip(trace['requests'], scenario['requests']):
        observations = [dict(block, lookup='not_attempted', load='not_attempted')
                        for block in request['blocks']]
        eligible = trace['window_ns'][0] <= request['scheduled_ns'] < trace['window_ns'][1] and bool(observations)
        started = backend.start(request['scheduled_ns']) if eligible else 0
        if eligible:
            for index, block in enumerate(observations):
                block['lookup'] = backend.lookup(request['request_id'], index, block)
                if block['lookup'] != 'hit':
                    break
            for index, block in enumerate(observations):
                if block['lookup'] != 'hit':
                    break
                block['load'] = backend.load(request['request_id'], index, block)
                if block['load'] != 'adopted':
                    break
        row = _derive(request, observations, script, trace['window_ns'], started, scenario['call_ns'])
        if eligible and row['finished_ns'] != backend.now_ns:
            raise ValueError('fake backend clock does not match observed calls')
        rows.append(row)
    return _seal(dict(schema=LEDGER_SCHEMA, synthetic=True, fake_only=True, formal_ab=False,
                      mode=MODE, trace_sha256=trace['sha256'], scenario_sha256=scenario['sha256'],
                      rows=rows, summary=_aggregate(trace, rows)))


def summarize(trace, scenario, ledger):
    """Reject observations inconsistent with the supplied trace and scenario.

    Matching hashes are not authenticity or engine evidence. The separate script
    is required so changing both raw rows and their claimed totals is detected.
    """
    validate_scenario(trace, scenario)
    _fields(ledger, {'schema', 'synthetic', 'fake_only', 'formal_ab', 'mode', 'trace_sha256',
                     'scenario_sha256', 'rows', 'summary', 'sha256'}, 'ledger')
    if (ledger['schema'] != LEDGER_SCHEMA or ledger['synthetic'] is not True or
            ledger['fake_only'] is not True or ledger['formal_ab'] is not False or ledger['mode'] != MODE or
            ledger['trace_sha256'] != trace['sha256'] or ledger['scenario_sha256'] != scenario['sha256']):
        raise ValueError('ledger scope/trace/scenario mismatch')
    rows = ledger['rows']
    if not isinstance(rows, list) or len(rows) != len(trace['requests']):
        raise ValueError('ledger must contain every request exactly once')
    previous_ns = 0
    for request, script, row in zip(trace['requests'], scenario['requests'], rows):
        if not isinstance(row, dict) or 'blocks' not in row:
            raise ValueError('invalid ledger row')
        expected = _derive(request, row['blocks'], script, trace['window_ns'],
                           max(previous_ns, request['scheduled_ns']), scenario['call_ns'])
        if encode(row) != encode(expected):
            raise ValueError('ledger identity, time or derived values mismatch')
        if expected['finished_ns'] is not None:
            previous_ns = expected['finished_ns']
    result = _aggregate(trace, rows)
    if encode(ledger['summary']) != encode(result):
        raise ValueError('ledger summary does not match raw observations')
    _digest(ledger)
    return result


def _write(path, document):
    raw = encode(document)
    if len(raw) > MAX_FILE_BYTES:
        raise ValueError('output exceeds 16 MiB file limit')
    # Exclusive creation refuses existing files, input aliases and symlinks.
    with Path(path).open('xb') as output:
        output.write(raw)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    generator = commands.add_parser('generate', help='write a NEW directory with trace/scenario JSON')
    generator.add_argument('--output', type=Path, required=True)
    for name in ('replay', 'check'):
        command = commands.add_parser(name)
        command.add_argument('--trace', type=Path, required=True)
        command.add_argument('--scenario', type=Path, required=True)
        command.add_argument('--output', type=Path, required=name == 'replay')
        if name == 'check':
            command.add_argument('--ledger', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'generate':
            trace, scenario = generate()
            args.output.mkdir()
            _write(args.output/'trace.json', trace)
            _write(args.output/'scenario.json', scenario)
            result = dict(synthetic=True, fake_only=True, formal_ab=False,
                          trace_sha256=trace['sha256'], scenario_sha256=scenario['sha256'])
        else:
            trace, scenario = load(args.trace), load(args.scenario)
            if args.command == 'replay':
                result = replay(trace, scenario)
            else:
                result = summarize(trace, scenario, load(args.ledger))
            if args.output:
                _write(args.output, result)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f'prefix chain error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
