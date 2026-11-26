#!/usr/bin/env python3
"""Small RDMA-only restart diagnostic; caller owns servers and timeout.

Hashes seal reproducible identities/accounting, not authenticated evidence.
Prefix metrics are payload-validated proxies, not engine adoption claims.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eviction_trace import encode, load, MAX_FILE_BYTES
from prefix_chain import _fields, _seal, _write
from prefix_rdma import RdmaBackend


SCHEMA = 'dfkv-prefix-recovery-v1'
NAMESPACE = b'prefix-recovery-v1'
MODES = ('seed', 'snapshot', 'verify', 'refill')


def generate(checkpoint=0):
    """32 immutable objects; positive checkpoints use fresh keys, not upserts.

    Checkpoint zero retains the original manifest and payload identities exactly.
    """
    if type(checkpoint) is not int or not 0 <= checkpoint <= 101:
        raise ValueError('checkpoint must be an integer in [0, 101]')
    objects, chains = [], []
    for group, length in enumerate((4096, 1048577)):
        start = len(objects)
        for offset in range(16):
            index = start + offset
            key = (f'recovery-v1-c{checkpoint:03d}-{index:02d}' if checkpoint else
                   f'recovery-v1-{index:02d}')
            objects.append(dict(object_id=index, namespace_hex=NAMESPACE.hex(),
                                key_hex=key.encode().hex(),
                                generation=0, tokens=16, length=length))
        for branch in range(2):
            chains.append(list(range(start, start + 4)) +
                          list(range(start + 4 + branch * 6,
                                     start + 10 + branch * 6)))
    manifest = dict(schema=SCHEMA, objects=objects, chains=chains)
    if checkpoint:
        manifest['checkpoint'] = checkpoint
    return _seal(manifest)


def validate(manifest):
    if not isinstance(manifest, dict):
        raise ValueError('manifest must be an object')
    if encode(manifest) != encode(generate(manifest.get('checkpoint', 0))):
        raise ValueError('manifest differs from immutable workload/checksum')


def payload(obj):
    return hashlib.shake_256(encode(dict(schema=SCHEMA, object=obj))).digest(obj['length'])


def _run(manifest, invoke, transport, mode, expected, expect):
    validate(manifest)
    if mode not in MODES or transport not in ('rdma', 'fake-rdma-test'):
        raise ValueError('unsupported mode or non-RDMA transport')
    if mode == 'verify':
        if expect not in ('preserve', 'empty'):
            raise ValueError('verify requires preserve or empty expectation')
        if expected is None or expected.get('mode') != 'snapshot':
            raise ValueError('verify requires a successful snapshot')
        check(manifest, expected)
        if not expected['passed']:
            raise ValueError('failed snapshot cannot be expected evidence')
        if expected['transport'] != transport:
            raise ValueError('snapshot transport differs from verification transport')
    elif expected is not None or expect is not None:
        raise ValueError('expectations only apply to verify')
    events, rows = [], []
    aborted = False
    observations = ['not_attempted'] * len(manifest['objects'])

    def call(op, index, phase):
        nonlocal aborted
        obj = manifest['objects'][index]
        event = invoke(op, obj, phase)
        events.append(event)
        if event['status'] in ('error', 'unknown'):
            aborted = True
        return event['status']

    if mode != 'refill':
        for index in range(len(observations)):
            if not aborted:
                observations[index] = call('put' if mode == 'seed' else 'get', index, mode)
        for chain in manifest['chains']:
            outcomes = [observations[index] for index in chain]
            prefix = 0
            for status in outcomes:
                if status != 'hit':
                    break
                prefix += 16
            rows.append(dict(object_ids=chain, outcomes=outcomes,
                             confirmed_prefix_tokens=prefix, target_tokens=len(chain)*16,
                             known=all(status in ('hit', 'miss') for status in outcomes)))
    else:
        for chain in manifest['chains']:
            outcomes = ['not_attempted'] * len(chain)
            verified = ['not_attempted'] * len(chain)
            prefix, boundary, known = 0, len(chain), not aborted
            if not aborted:
                for offset, index in enumerate(chain):
                    status = call('get', index, 'prefix')
                    outcomes[offset] = status
                    if status != 'hit':
                        boundary, known = offset, status == 'miss'
                        break
                    prefix += 16
                # Freeze original prefix before writes; suffix hits are not reuse.
                if not aborted:
                    for index in chain[boundary:]:
                        call('put', index, 'refill')
                        if aborted:
                            break
                if not aborted:
                    for offset, index in enumerate(chain):
                        status = call('get', index, 'reread')
                        verified[offset] = status
                        observations[index] = status
                        if status != 'hit':
                            aborted = True
                            break
            rows.append(dict(object_ids=chain, outcomes=outcomes, reread=verified,
                             confirmed_prefix_tokens=prefix, target_tokens=len(chain)*16,
                             known=known))
    matches = True
    if mode == 'verify':
        wanted = expected['objects'] if expect == 'preserve' else ['miss']*len(observations)
        matches = observations == wanted
    counts = Counter(event['op'] + '_' + event['status'] for event in events)
    summary = dict(planned_objects=len(observations), planned_chains=len(rows),
                   target_tokens=sum(row['target_tokens'] for row in rows),
                   confirmed_prefix_tokens=sum(row['confirmed_prefix_tokens'] for row in rows),
                   unknown_chains=sum(not row['known'] for row in rows),
                   not_attempted_objects=observations.count('not_attempted'),
                   expectation_matches=matches,
                   native={name: counts[name] for name in
                           ('get_hit', 'get_miss', 'get_error', 'get_unknown',
                            'put_ack', 'put_error', 'put_unknown')},
                   put_bytes=sum(manifest['objects'][event['object_id']]['length']
                                 for event in events if event['op']=='put' and event['status']=='ack'))
    return _seal(dict(schema=SCHEMA, manifest_sha256=manifest['sha256'],
                      transport=transport, mode=mode, expect=expect,
                      expected_sha256=expected['sha256'] if expected else None,
                      passed=not aborted and matches, objects=observations,
                      chains=rows, events=events, summary=summary))


def execute(manifest, backend, mode, expected=None, expect=None):
    """Bounded synchronous calls; first error stops I/O, never infers a miss."""
    def invoke(op, obj, phase):
        value = payload(obj)
        actual, error = None, None
        try:
            if op == 'put':
                status = backend.put(obj, value)
                if status not in ('ack', 'error', 'unknown'):
                    raise RuntimeError('invalid PUT status')
            else:
                status, actual = backend.get(obj, value)
                if status not in ('hit', 'miss', 'error', 'unknown'):
                    raise RuntimeError('invalid GET status')
                if status == 'hit' and (type(actual) is not bytes or actual != value):
                    status, error = 'error', 'wrong payload/length'
                if status != 'hit' and actual is not None:
                    status, error = 'error', 'unexpected payload'
        except Exception as caught:
            status = 'unknown' if op == 'put' else 'error'
            error = f'{type(caught).__name__}: {caught}'[:512]
        return dict(op=op, object_id=obj['object_id'], phase=phase, status=status,
                    expected_sha256=hashlib.sha256(value).hexdigest(),
                    observed_sha256=hashlib.sha256(actual).hexdigest() if type(actual) is bytes else None,
                    observed_length=len(actual) if type(actual) is bytes else 0, error=error)
    return _run(manifest, invoke, getattr(backend, 'transport', None), mode, expected, expect)


def check(manifest, report, expected=None, expect=None):
    """Replay raw observations and reject missing events, tampering and totals."""
    if not isinstance(report, dict) or not isinstance(report.get('events'), list):
        raise ValueError('invalid report')
    source = iter(report['events'])
    def invoke(op, obj, phase):
        event = next(source, None)
        _fields(event, {'op', 'object_id', 'phase', 'status', 'expected_sha256',
                        'observed_sha256', 'observed_length', 'error'}, 'event')
        if (event['op'], event['object_id'], event['phase']) != (op, obj['object_id'], phase):
            raise ValueError('event identity/order mismatch')
        digest = hashlib.sha256(payload(obj)).hexdigest()
        if event['expected_sha256'] != digest:
            raise ValueError('expected payload mismatch')
        if event['status'] not in (('ack', 'error', 'unknown') if op=='put' else ('hit', 'miss', 'error', 'unknown')):
            raise ValueError('invalid status')
        if event['error'] is not None and (not isinstance(event['error'], str) or len(event['error'])>512):
            raise ValueError('invalid error')
        if event['status'] in ('ack', 'hit', 'miss') and event['error'] is not None:
            raise ValueError('successful outcome cannot carry an error')
        if type(event['observed_length']) is not int or event['observed_length'] < 0:
            raise ValueError('invalid payload length')
        if op=='put' or event['status']=='miss':
            if event['observed_length'] != 0 or event['observed_sha256'] is not None:
                raise ValueError('unexpected payload')
        if event['status']=='hit' and (event['observed_sha256'] != digest or
                event['observed_length'] != obj['length'] or event['error'] is not None):
            raise ValueError('unchecked hit')
        return event
    rebuilt = _run(manifest, invoke, report.get('transport'), report.get('mode'), expected, expect)
    if next(source, None) is not None or encode(report) != encode(rebuilt):
        raise ValueError('report differs from replayed evidence/checksum')
    return rebuilt['summary']


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lib', required=True)
    parser.add_argument('--members', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=MODES, required=True)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--expected', type=Path)
    parser.add_argument('--expect', choices=('preserve', 'empty'))
    args = parser.parse_args(argv)
    backend = None
    try:
        if args.mode != 'seed' and args.manifest is None:
            raise ValueError('--manifest required after seed')
        manifest = load(args.manifest) if args.manifest else generate()
        validate(manifest)
        expected = load(args.expected) if args.expected else None
        # Validate expectations before loading a native library or doing I/O.
        if args.mode == 'verify':
            if expected is None or args.expect is None or expected.get('mode') != 'snapshot':
                raise ValueError('verify requires --expected snapshot and --expect')
            check(manifest, expected)
            if not expected['passed']:
                raise ValueError('failed snapshot')
        elif expected is not None or args.expect is not None:
            raise ValueError('expectations only apply to verify')
        args.output.mkdir(exist_ok=False)
        _write(args.output/'manifest.json', manifest)
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'integration/common/src'))
        from disk_scenarios import load_library
        backend = RdmaBackend(load_library(args.lib), args.members, NAMESPACE)
        report = execute(manifest, backend, args.mode, expected, args.expect)
        check(manifest, report, expected, args.expect)
        if len(encode(report)) > MAX_FILE_BYTES:
            raise ValueError('report exceeds file bound')
        _write(args.output/'report.json', report)
        print(json.dumps(report['summary'], sort_keys=True))
        return 0 if report['passed'] else 1
    except (OSError, ValueError, RuntimeError, TypeError, UnicodeError, RecursionError) as error:
        print(f'prefix recovery error: {error}', file=sys.stderr)
        return 1
    finally:
        if backend is not None:
            backend.close()


if __name__ == '__main__':
    sys.exit(main())
