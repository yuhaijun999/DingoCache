#!/usr/bin/env python3
"""Guest-only, class-capacity RDMA lifetime diagnostics, not formal A/B.

Host backing identity and thermal watchdog are mandatory operator prerequisites.
No mounts, deletion, TCP fallback, production default changes or GPU operations.
"""

import argparse
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_baseline_smoke as baseline
import eviction_smoke as smoke
import long_residence as workload
import quick_eviction as quick
import run_sg_validation as common

ACCELERATED = {
    'DFKV_SLAB_LIFE_BASE_SEC': '60', 'DFKV_SLAB_LIFE_READ_BONUS_SEC': '30',
    'DFKV_SLAB_LIFE_MAX_READS': '2', 'DFKV_SLAB_PROTECT_SEC': '12',
    'DFKV_SLAB_PROTECT_SLOWDOWN': '4', 'DFKV_SLAB_PROTECT_BOOST_SEC': '120',
    'DFKV_SLAB_KEEP_SCORE_SEC': '47', 'DFKV_SLAB_BUCKET_SEC': '4',
    'DFKV_SLAB_BUCKET_COUNT': '24',
}


def plan(pairs=3, start_pair=0):
    if (type(pairs) is not int or type(start_pair) is not int
            or not 0 <= start_pair <= 2 or not 1 <= pairs <= 3 - start_pair):
        raise ValueError('pairs/start-pair must select consecutive pairs within 0..2')
    return [dict(case_id=pair * 2 + slot, pair=pair, policy=policy, state='not_attempted')
            for pair in range(start_pair, start_pair + pairs)
            for slot, policy in enumerate(('legacy', 'lifetime') if pair % 2 == 0
                                          else ('lifetime', 'legacy'))]


def environment(profile, *, pressure_case=None):
    config = workload.profile(profile, pressure_case=pressure_case)
    env = {key: value for key, value in os.environ.items() if not key.startswith('DFKV_')}
    env.update(smoke.CLIENT_ENV, DFKV_RAM_TIER='0', DFKV_PUT_ACK_MODE='disk',
               DFKV_SERVER_URING='1', DFKV_GET_MISS_RETRIES='0',
               DFKV_SLAB_RECLAIM_MS='0', DFKV_SLAB_EVICT_LOW_PCT='0',
               DFKV_SLAB_EVICT_HIGH_PCT='0')
    if profile == 'accelerated':
        env.update(ACCELERATED)
    elif pressure_case in ('p60', 'p120'):
        env['DFKV_SLAB_PROTECT_SEC'] = str(config['protect_sec'])
    source = Path(__file__).resolve().parents[2]
    env['PYTHONPATH'] = str(source / 'integration/common/src')
    return env



def check_startup(text, profile, policy, *, pressure_case=None):
    """Read back effective settings before sending any workload writes."""
    config = workload.profile(profile, pressure_case=pressure_case)
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    expected = {'DFKV_PUT_ACK_MODE': 'disk', 'DFKV_RAM_TIER': 'off',
                'DFKV_SERVER_URING': 'on', 'DFKV_SLAB_RECLAIM_MS': '0',
                'DFKV_SLAB_EVICT_LOW_PCT': '0', 'DFKV_SLAB_EVICT_HIGH_PCT': '0',
                'disk_eviction_requested': policy, 'disk_eviction_effective': policy}
    scores = ACCELERATED if profile == 'accelerated' else {
        'DFKV_SLAB_LIFE_BASE_SEC': '604800', 'DFKV_SLAB_LIFE_READ_BONUS_SEC': '302400',
        'DFKV_SLAB_LIFE_MAX_READS': '2', 'DFKV_SLAB_PROTECT_SEC': str(config['protect_sec']),
        'DFKV_SLAB_PROTECT_SLOWDOWN': '4', 'DFKV_SLAB_PROTECT_BOOST_SEC': '1209600',
        'DFKV_SLAB_KEEP_SCORE_SEC': '475200', 'DFKV_SLAB_BUCKET_SEC': '43200',
        'DFKV_SLAB_BUCKET_COUNT': '24'}
    expected.update({name: value if policy == 'lifetime' else 'inactive'
                     for name, value in scores.items()})
    for name, value in expected.items():
        matches = re.findall(r'\b' + re.escape(name) + r'\s*=\s*(\S+)\s+\([^\n]*\)', text)
        if matches != [value]:
            raise ValueError('effective startup mismatch: ' + name)


def _hashes(paths):
    return {path.name: baseline.file_sha(path) for path in paths}


def _tools():
    names = ('run_long_residence.py', 'long_residence.py', 'disk_scenarios.py',
             'quick_eviction.py', 'eviction_baseline_smoke.py', 'eviction_smoke.py',
             'run_sg_validation.py', 'sg_validation.py', 'eviction_trace.py',
             'eviction_client.py', 'eviction_batch.py', 'eviction_dedup.py',
             'eviction_guard.py')
    paths = [Path(__file__).with_name(name) for name in names]
    paths.append(Path(__file__).resolve().parents[2] / 'integration/common/src/dfkv_common/cabi.py')
    return _hashes(paths)


def check_initial(text, dirs):
    quick.check_class_only(text, activity=False)
    objects = quick.samples(text, 'dfkv_disk_objects')
    expected = {'{disk=' + json.dumps(str(path), ensure_ascii=False) + '}' for path in dirs}
    if ({labels for labels, _ in objects} != expected
            or len(objects) != 3 or any(value != 0 for _, value in objects)):
        raise ValueError('initial disk identity/empty state mismatch')
    for name in ('dfkv_cache_put_total', 'dfkv_bytes_written_total', 'dfkv_evictions_total',
                 'dfkv_rdma_pull_requests_dispatched_total', 'dfkv_rdma_pull_requests_terminal_total',
                 'dfkv_uring_reads_total', 'dfkv_uring_completions_total'):
        if quick.total(text, name) != 0:
            raise ValueError('nonempty initial counter: ' + name)


def check_metrics(before, after, policy, counts, dirs):
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    check_initial(before, dirs)
    class_counts = quick.check_class_only(after)
    objects = quick.samples(after, 'dfkv_disk_objects')
    if (len(objects) != 3 or {labels for labels, _ in objects}
            != {'{disk=' + json.dumps(str(path), ensure_ascii=False) + '}' for path in dirs}
            or any(value <= 0 for _, value in objects)):
        raise ValueError('final disk identity/progress mismatch')
    hits, misses = counts['get_hits'], counts['get_misses']
    expected = {'dfkv_cache_put_total': counts['put_acks'],
                'dfkv_bytes_written_total': counts['put_bytes'],
                'dfkv_rdma_pull_requests_dispatched_total': hits + misses,
                'dfkv_rdma_pull_requests_terminal_total': hits + misses,
                'dfkv_uring_reads_total': hits, 'dfkv_uring_completions_total': hits,
                'dfkv_slab_lifetime_read_success_total': hits if policy == 'lifetime' else 0}
    deltas = {}
    for name, wanted in expected.items():
        delta = quick.total(after, name) - quick.total(before, name)
        if delta != wanted:
            raise ValueError(f'{name}: expected {wanted}, got {delta}')
        deltas[name] = delta
    old_rows = quick.samples(before, 'dfkv_rdma_pull_requests_terminal_total')
    new_rows = quick.samples(after, 'dfkv_rdma_pull_requests_terminal_total')
    old, new = dict(old_rows), dict(new_rows)
    if len(old) != len(old_rows) or len(new) != len(new_rows):
        raise ValueError('duplicate PULL terminal labels')
    if old.keys() != new.keys():
        raise ValueError('PULL terminal metric labels changed')
    for result, wanted in (('success_nonempty', hits), ('not_found', misses)):
        labels = [label for label in new if 'path="uring"' in label
                  and 'result="' + result + '"' in label]
        if len(labels) != 1 or new[labels[0]] - old[labels[0]] != wanted:
            raise ValueError('PULL terminal io_uring result mismatch: ' + result)
    for labels, value in new.items():
        if not any('path="uring"' in labels and 'result="' + result + '"' in labels
                   for result in ('success_nonempty', 'not_found')) and value - old[labels] != 0:
            raise ValueError('unexpected PULL terminal path/result')
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if quick.total(after, name) != 1:
            raise ValueError('unhealthy storage: ' + name)
    for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total', 'dfkv_uring_inflight'):
        if quick.total(before, name) != 0 or quick.total(after, name) != 0:
            raise ValueError('error/fallback/unsettled request: ' + name)
    return dict(deltas, **class_counts)



def check_pressure_samples(directory, report, dirs):
    """Bind each report counter to its separately retained owned-server scrape."""
    directory = Path(directory)
    index = directory / 'metrics-samples.json'
    samples = json.loads(index.read_text())
    observed = report['pressure_samples']
    if not isinstance(samples, list) or len(samples) != len(observed) or not samples:
        raise ValueError('raw pressure sample denominator mismatch')
    hashes = {'metrics-samples.json': baseline.file_sha(index)}
    previous_end = 0
    for number, (sample, event) in enumerate(zip(samples, observed)):
        name = f'metrics-{number:03d}.prom'
        if sample.get('file') != name:
            raise ValueError('raw pressure sample identity mismatch')
        begin, end = sample.get('started_monotonic_sec'), sample.get('finished_monotonic_sec')
        if (any(type(value) not in (int, float) or not math.isfinite(value) for value in (begin, end))
                or not previous_end <= begin <= end):
            raise ValueError('raw pressure sample timing mismatch')
        previous_end = end
        path = directory / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4 * 1024 * 1024:
            raise ValueError('invalid raw pressure artifact')
        text = path.read_text()
        quick.check_class_only(text, activity=number != 0)
        objects = quick.samples(text, 'dfkv_disk_objects')
        expected = {'{disk=' + json.dumps(str(path), ensure_ascii=False) + '}' for path in dirs}
        if len(objects) != 3 or {label for label, _ in objects} != expected:
            raise ValueError('raw pressure disk identity mismatch')
        if quick.total(text, 'dfkv_evictions_total') != event['counter']:
            raise ValueError('raw pressure counter differs from report')
        for metric in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
            if quick.total(text, metric) != 1:
                raise ValueError('unhealthy raw pressure sample')
        for metric in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total'):
            if quick.total(text, metric) != 0:
                raise ValueError('raw pressure error/fallback')
        hashes[name] = baseline.file_sha(path)
        if sample.get('sha256') != hashes[name]:
            raise ValueError('raw pressure artifact hash mismatch')
    return hashes


def run(build, roots, output, tag, *, profile='accelerated', pairs=3, start_pair=0, hot_refill=False,
        pressure_case=None):
    cases = plan(pairs, start_pair)
    config = workload.profile(profile, hot_refill=hot_refill, pressure_case=pressure_case)
    env = environment(profile, pressure_case=pressure_case)
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('tag must be 1..40 ASCII letters/digits/_/-')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain comma separator')
    roots, output = common.guest_paths(roots, output, 'long-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-long-{tag}-{row['case_id']:02d}-{row['policy']}")
                            for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data exists; use a fresh tag')
    output.mkdir()
    result = dict(model='long-residence-rdma-class-diagnostic-v1', formal_ab=False,
                  gpu='excluded', passed=False, errors=[], cases=cases, profile=config,
                  build_sha256=_hashes(binaries), tools_sha256=_tools(),
                  accelerated_scores=ACCELERATED if profile == 'accelerated' else None,
                  planned_per_case=workload.expected_counts(config),
                  scope='compact-capacity-turnover-not-production-scale-or-day-aging')
    if pressure_case is not None:
        result['pressure_case'] = pressure_case
    timeout = config['steps'] * config['interval_sec'] + config['preage_sec'] + 120
    try:
        for row in cases:
            if _hashes(binaries) != result['build_sha256'] or _tools() != result['tools_sha256']:
                raise ValueError('build or frozen tools changed')
            quick.check_ports()
            case = output / f"case-{row['case_id']:02d}-{row['policy']}"
            case.mkdir()
            for path in row['data_dirs']:
                Path(path).mkdir()
            command = [str(binaries[0]), '--dir', ','.join(row['data_dirs']),
                       '--cap', '12884901888', '--store-engine', 'slab', '--slab-write', 'buffered',
                       '--slab-eviction-policy', row['policy'], '--port', '12000', '--rdma-port', '12001',
                       '--rdma-dev', 'rxe0', '--max-msg', '2097152', '--metrics-port', '12010',
                       '--metrics-bind', '127.0.0.1']
            child_command = [sys.executable, str(Path(__file__).with_name('long_residence.py')),
                             '--lib', str(binaries[1]), '--members', 'n=127.0.0.1:12001',
                             '--output', str(case / 'client'), '--profile', profile, '--metrics-port', '12010']
            if pressure_case is not None:
                child_command.extend(('--pressure-case', pressure_case))
            if hot_refill:
                child_command.append('--hot-refill')
            row.update(state='running', command=command, client_command=child_command,
                       client_timeout_sec=timeout, counts=None, client_state='not_attempted',
                       environment={key: value for key, value in env.items() if key.startswith('DFKV_')})
            server = None
            try:
                with (case / 'server.log').open('x') as log:
                    server = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                    row['server_pid'] = server.pid
                    before = baseline._ready(server, 12010, case / 'server.log', row['policy'])
                    check_initial(before, row['data_dirs'])
                    check_startup((case / 'server.log').read_text(), profile, row['policy'],
                                  pressure_case=pressure_case)
                    (case / 'before.prom').write_text(before)
                    row['client_state'] = 'running'
                    try:
                        with (case / 'client.log').open('x') as log:
                            subprocess.run(child_command, env=env, stdin=subprocess.DEVNULL, stdout=log,
                                           stderr=subprocess.STDOUT, timeout=timeout, check=True)
                        row.update(client_state='exited', client_exit=0)
                    except subprocess.TimeoutExpired:
                        # subprocess.run kills and reaps its own exact child. Never
                        # turn absent/partial native outcomes into zero counters.
                        row['client_state'] = 'process_timeout_reaped'
                        raise
                    except subprocess.CalledProcessError as error:
                        row.update(client_state='failed_exit_reaped', client_exit=error.returncode)
                        raise
                    report_path = case / 'client/report.json'
                    report = json.loads(report_path.read_text())
                    row['client_report_sha256'] = baseline.file_sha(report_path)
                    counts = workload.check_report(report, config)
                    row['pressure_artifacts_sha256'] = check_pressure_samples(
                        case / 'client', report, row['data_dirs'])
                    row['counts'] = counts
                    after = baseline._scrape(12010)
                    (case / 'after.prom').write_text(after)
                    row['server_delta'] = check_metrics(before, after, row['policy'], counts, row['data_dirs'])
                    row['state'] = 'passed'
            except BaseException:
                row['state'] = 'failed'
                raise
            finally:
                if server is not None:
                    try:
                        common.stop_owned(server)
                    except BaseException:
                        row['state'] = 'failed'
                        raise
                    finally:
                        row.update(server_exit=server.poll(), server_reaped=server.poll() is not None)
            quick.check_ports()
        if _hashes(binaries) != result['build_sha256'] or _tools() != result['tools_sha256']:
            raise ValueError('build or frozen tools changed during matrix')
        result['passed'] = True
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            raise
    finally:
        (output / 'matrix.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--data-root', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--profile', choices=('accelerated', 'default'), default='accelerated')
    parser.add_argument('--pressure-case', choices=workload.PRESSURE_CASES,
                        help='opt-in 660-step default-score pressure/P diagnostic')
    parser.add_argument('--hot-refill', action='store_true',
                        help='enable separate verified hot-miss refills')
    parser.add_argument('--pairs', type=int, default=3)
    parser.add_argument('--start-pair', type=int, default=0)
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag, profile=args.profile,
                     pairs=args.pairs, start_pair=args.start_pair, hot_refill=args.hot_refill,
                     pressure_case=args.pressure_case)
        print(json.dumps({'passed': result['passed'], 'output': str(args.output)}))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print('long residence error: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
