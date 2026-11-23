#!/usr/bin/env python3
"""Guest-only RXE/io_uring fixed-arrival diagnostics, not formal A/B.

Host backing and thermal-watchdog proof is a prerequisite owned by the operator.
This runner creates fresh data only, never mounts, deletes or falls back to TCP.
Small below-capacity samples do not prove eviction pressure or parallel I/O.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_arrival as arrival
import eviction_baseline_smoke as baseline
import eviction_smoke as smoke
import eviction_trace as trace
import quick_eviction as quick
import run_sg_validation as common


def plan(workers=(1, 4, 16), pairs=3):
    if (not isinstance(workers, (tuple, list)) or not workers
            or any(type(value) is not int or value not in (1, 4, 16) for value in workers)
            or list(workers) != sorted(set(workers))):
        raise ValueError('workers must be an increasing subset of 1,4,16')
    if type(pairs) is not int or not 1 <= pairs <= 3:
        raise ValueError('pairs must be in [1,3]')
    cases = []
    for worker in workers:
        for pair in range(pairs):
            policies = ('legacy', 'lifetime') if pair % 2 == 0 else ('lifetime', 'legacy')
            for policy in policies:
                cases.append({'case_id': len(cases), 'workers': worker,
                              'pair': pair, 'policy': policy})
    return cases



def check_metrics(before, after, policy, counts):
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    gets = counts['get_requests']
    expected = {'dfkv_cache_put_total': counts['put_requests'],
                'dfkv_bytes_written_total': counts['put_payload_bytes'],
                'dfkv_disk_objects': counts['put_requests'],
                'dfkv_rdma_pull_requests_dispatched_total': gets,
                'dfkv_rdma_pull_requests_terminal_total': gets,
                'dfkv_uring_reads_total': gets, 'dfkv_uring_completions_total': gets,
                'dfkv_slab_lifetime_read_success_total': gets if policy == 'lifetime' else 0}
    deltas = {}
    for name, wanted in expected.items():
        delta = quick.total(after, name) - quick.total(before, name)
        if delta != wanted:
            raise ValueError(f'{name}: expected {wanted}, got {delta}')
        deltas[name] = delta
    old = dict(quick.samples(before, 'dfkv_rdma_pull_requests_terminal_total'))
    new = dict(quick.samples(after, 'dfkv_rdma_pull_requests_terminal_total'))
    if set(old) != set(new):
        raise ValueError('PULL terminal metric label set changed')
    success_labels = [labels for labels in new
                      if 'path="uring"' in labels and 'result="success_nonempty"' in labels]
    if len(success_labels) != 1:
        raise ValueError('expected one io_uring success terminal series')
    for labels, value in new.items():
        wanted = gets if labels == success_labels[0] else 0
        if value - old[labels] != wanted:
            raise ValueError('unexpected PULL terminal path/result: ' + labels)
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if quick.total(after, name) != 1:
            raise ValueError('unhealthy storage: ' + name)
    for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total',
                 'dfkv_uring_inflight', 'dfkv_evictions_total'):
        if quick.total(before, name) != 0 or quick.total(after, name) != 0:
            raise ValueError('unexpected error/fallback/inflight/eviction: ' + name)
    return deltas


def _hashes(paths):
    return {path.name: baseline.file_sha(path) for path in paths}


def _source_hashes():
    names = set(arrival.TOOLS) | {Path(__file__).name, 'quick_eviction.py',
        'run_sg_validation.py', 'sg_validation.py', 'eviction_batch.py', 'eviction_dedup.py'}
    return _hashes([Path(__file__).with_name(name) for name in sorted(names)])


def run(build, roots, output, tag, *, workers=(1, 4, 16), pairs=3):
    cases = plan(workers, pairs)
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('tag must be 1..40 ASCII letters/digits/_/-')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain the --dir comma separator')
    roots, output = common.guest_paths(roots, output, 'arrival-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain server and shared library')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-arrival-{tag}-{row['case_id']:02d}-{row['policy']}")
                            for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data already exists; use a fresh tag')
        row['state'] = 'not_attempted'
    document = trace.generate(objects=8, rounds=4, length=4096, interval_ns=20000000)
    counts = trace.validate(document)
    output.mkdir()
    source = output / 'trace.json'
    source.write_bytes(trace.encode(document))
    result = {'model': 'rdma-fixed-arrival-diagnostic-v1', 'formal_ab': False,
              'gpu': 'excluded', 'passed': False, 'errors': [], 'cases': cases,
              'scope': 'below-capacity-correctness-not-eviction-or-performance',
              'trace_sha256': document['sha256'], 'trace_file_sha256': baseline.file_sha(source),
              'build_sha256': _hashes(binaries), 'tools_sha256': _source_hashes(),
              'request_counts_per_case': counts}
    env = {key: value for key, value in os.environ.items() if not key.startswith('DFKV_')}
    env.update(smoke.CLIENT_ENV, DFKV_RAM_TIER='0', DFKV_PUT_ACK_MODE='disk',
               DFKV_SLAB_RECLAIM_MS='50', DFKV_SERVER_URING='1', DFKV_GET_MISS_RETRIES='0')
    try:
        for row in cases:
            if (_hashes(binaries) != result['build_sha256']
                    or baseline.file_sha(source) != result['trace_file_sha256']
                    or _source_hashes() != result['tools_sha256']):
                raise ValueError('build, tools or frozen trace changed before case')
            quick.check_ports()
            case = output / f"case-{row['case_id']:02d}-{row['policy']}-w{row['workers']}"
            case.mkdir()
            for path in row['data_dirs']:
                Path(path).mkdir()
            command = [str(binaries[0]), '--dir', ','.join(row['data_dirs']),
                       '--cap', '12884901888', '--store-engine', 'slab',
                       '--slab-write', 'buffered', '--slab-eviction-policy', row['policy'],
                       '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0',
                       '--max-msg', '2097152', '--metrics-port', '12010',
                       '--metrics-bind', '127.0.0.1']
            row.update(state='running', command=command,
                       server_environment={key: value for key, value in env.items() if key.startswith('DFKV_')})
            server = None
            try:
                with (case / 'server.log').open('x') as log:
                    server = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                    row['server_pid'] = server.pid
                    before = baseline._ready(server, 12010, case / 'server.log', row['policy'])
                    objects = quick.samples(before, 'dfkv_disk_objects')
                    if len(objects) != 3 or any(value != 0 for _, value in objects):
                        raise ValueError('fresh three-disk initial state required')
                    for path in row['data_dirs']:
                        if not any('disk=' + json.dumps(path) in labels for labels, _ in objects):
                            raise ValueError('disk identity mismatch')
                    (case / 'before.prom').write_text(before)
                    row['client'] = arrival.supervise(source, case / 'client', lib=binaries[1],
                        members='n=127.0.0.1:12001', workers=row['workers'], queue_capacity=16,
                        timeout_ns=5000000000, process_timeout_sec=30, tcp=False)
                    row['client_report_sha256'] = baseline.file_sha(case / 'client/report.json')
                    checked = arrival.check(case / 'client', row['client_report_sha256'])
                    expected_tools = {name: result['tools_sha256'][name] for name in arrival.TOOLS}
                    if checked.get('config', {}).get('tools') != expected_tools:
                        raise ValueError('child tool identity differs from frozen matrix')
                    if not checked['passed'] or not row['client']['passed']:
                        raise ValueError('RDMA fixed-arrival child failed')
                    if (checked['completed_requests'] != counts['requests']
                            or checked['offered_requests'] != counts['requests']
                            or checked['acked_put_bytes'] != counts['put_payload_bytes']
                            or checked['hit_get_bytes'] != counts['requested_get_bytes']):
                        raise ValueError('incomplete planned request or byte denominator')
                    after = baseline._scrape(12010)
                    (case / 'after.prom').write_text(after)
                    row['server_delta'] = check_metrics(before, after, row['policy'], counts)
                    row['state'] = 'passed'
            except BaseException:
                row['state'] = 'failed'
                raise
            finally:
                if server is not None:
                    row['server_reaped'] = False
                    try:
                        common.stop_owned(server)
                    except BaseException:
                        row['state'] = 'failed'
                        raise
                    finally:
                        row['server_exit'] = server.poll()
                        row['server_reaped'] = server.poll() is not None
            quick.check_ports()
        if (_hashes(binaries) != result['build_sha256']
                or baseline.file_sha(source) != result['trace_file_sha256']
                or _source_hashes() != result['tools_sha256']):
            raise ValueError('build, tools or frozen trace changed during matrix')
        result['passed'] = True
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            raise
    finally:
        (output / 'matrix.json').write_bytes(trace.encode(result))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', required=True, type=Path)
    parser.add_argument('--data-root', action='append', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--workers', type=int, nargs='+', default=[1, 4, 16])
    parser.add_argument('--pairs', type=int, default=3)
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag,
                     workers=args.workers, pairs=args.pairs)
        print(json.dumps({'passed': result['passed'], 'output': str(args.output)}))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print(f'RDMA arrival error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
