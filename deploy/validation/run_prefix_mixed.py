#!/usr/bin/env python3
"""Guest-only mixed-size prefix diagnostics, not formal A/B or performance proof.

Fresh host backing proof and a live thermal watchdog are external prerequisites.
Global-enabled tests retain donor/background activity as separate confounders;
aggregate watermark counters do not identify the sizes of individual victims.
"""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_baseline_smoke as baseline
import quick_eviction as quick
import run_long_residence as residence
import run_prefix_rdma as prefix
import run_sg_validation as common

CAPACITY_BYTES = 12884901888
CLIENT_TIMEOUT_SEC = 2100
SAMPLE_INTERVAL_SEC = 10
ROUTES = ('class-pressure', 'watermark')
plan = residence.plan


def environment(route):
    if route not in ROUTES:
        raise ValueError('unknown pressure route')
    env = residence.environment('default')
    if route == 'watermark':
        env.update(DFKV_SLAB_RECLAIM_MS='50', DFKV_SLAB_EVICT_LOW_PCT='40',
                   DFKV_SLAB_EVICT_HIGH_PCT='50')
    return env


def check_startup(text, policy, route):
    env = environment(route)
    expected = {'DFKV_PUT_ACK_MODE': 'disk', 'DFKV_RAM_TIER': 'off',
                'DFKV_SERVER_URING': 'on', 'disk_eviction_requested': policy,
                'disk_eviction_effective': policy}
    for name in ('DFKV_SLAB_RECLAIM_MS', 'DFKV_SLAB_EVICT_LOW_PCT', 'DFKV_SLAB_EVICT_HIGH_PCT'):
        expected[name] = env[name]
    scores = {'DFKV_SLAB_LIFE_BASE_SEC': '604800', 'DFKV_SLAB_LIFE_READ_BONUS_SEC': '302400',
              'DFKV_SLAB_LIFE_MAX_READS': '2', 'DFKV_SLAB_PROTECT_SEC': '180',
              'DFKV_SLAB_PROTECT_SLOWDOWN': '4', 'DFKV_SLAB_PROTECT_BOOST_SEC': '1209600',
              'DFKV_SLAB_KEEP_SCORE_SEC': '475200', 'DFKV_SLAB_BUCKET_SEC': '43200',
              'DFKV_SLAB_BUCKET_COUNT': '24'}
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    expected.update({key: value if policy == 'lifetime' else 'inactive' for key, value in scores.items()})
    for name, value in expected.items():
        matches = re.findall(r'\b' + re.escape(name) + r'\s*=\s*(\S+)\s+\([^\n]*\)', text)
        if matches != [value]:
            raise ValueError('effective startup mismatch: ' + name)


def _rows(text, name):
    rows = quick.samples(text, name)
    if len(dict(rows)) != len(rows):
        raise ValueError('duplicate metric labels: ' + name)
    return dict(rows)


def _delta(before, after, name):
    old, new = _rows(before, name), _rows(after, name)
    if (old.keys() != new.keys() or
            any(not value.is_integer() for value in list(old.values()) + list(new.values())) or
            any(new[key] < value for key, value in old.items())):
        raise ValueError('counter identity/regression: ' + name)
    return sum(new.values()) - sum(old.values())


def check_geometry(text, dirs, route):
    environment(route)
    if len(dirs) != 3 or len(set(map(str, dirs))) != 3:
        raise ValueError('three distinct disk identities required')
    objects = _rows(text, 'dfkv_disk_objects')
    if set(objects) != {'{disk=' + json.dumps(str(path), ensure_ascii=False) + '}' for path in dirs}:
        raise ValueError('disk identity mismatch')
    expected = {'capacity_bytes': CAPACITY_BYTES // 3, 'watermark_high_bytes': 0,
                'watermark_low_bytes': 0, 'reclaim_interval_ms': 0}
    if route == 'watermark':
        expected.update(watermark_high_bytes=2147483600, watermark_low_bytes=1717986880,
                        reclaim_interval_ms=50)
    labels = {'{disk=' + json.dumps(str(path), ensure_ascii=False) + ',disk_index="' + str(index) + '"}'
              for index, path in enumerate(dirs)}
    for suffix, value in expected.items():
        rows = _rows(text, 'dfkv_slab_disk_' + suffix)
        if set(rows) != labels or any(actual != value for actual in rows.values()):
            raise ValueError('effective threshold/disk mismatch: ' + suffix)
    return labels


def check_initial(text, dirs, route):
    check_geometry(text, dirs, route)
    if any(_rows(text, 'dfkv_disk_objects').values()):
        raise ValueError('nonempty initial objects')
    for name in ('dfkv_cache_put_total', 'dfkv_bytes_written_total', 'dfkv_evictions_total',
                 'dfkv_rdma_pull_requests_dispatched_total', 'dfkv_rdma_pull_requests_terminal_total',
                 'dfkv_uring_reads_total', 'dfkv_uring_completions_total',
                 'dfkv_slab_watermark_evictions_total', 'dfkv_slab_watermark_freed_slot_bytes_total',
                 'dfkv_slab_watermark_freed_payload_bytes_total',
                 'dfkv_slab_disk_watermark_high_observations_total'):
        if quick.total(text, name) != 0:
            raise ValueError('nonempty initial counter: ' + name)


def put_classes(workload, document, report, *, distinct=False):
    counts = Counter()
    seen = set()
    for event in report['events']:
        if event['op'] == 'put' and event['status'] == 'ack':
            for index in event['ids']:
                if distinct and index in seen:
                    continue
                seen.add(index)
                length = workload.object_for(document, index)['length']
                counts[((length + 1048575) // 1048576) * 1048576] += 1
    return dict(counts)


def check_metrics(before, after, policy, summary, dirs, route, classes, *, distinct_classes=None):
    check_initial(before, dirs, route)
    labels = check_geometry(after, dirs, route)
    counts = summary['native']
    fields = {'get_hits', 'get_misses', 'get_errors', 'get_unknown', 'get_attempts',
              'put_acks', 'put_bytes', 'put_errors', 'put_unknown', 'put_attempts'}
    if (not isinstance(counts, dict) or set(counts) != fields or
            any(type(value) is not int or value < 0 for value in counts.values())):
        raise ValueError('invalid native denominator')
    if (any(counts[key] for key in ('get_errors', 'get_unknown', 'put_errors', 'put_unknown')) or
            counts['get_attempts'] != counts['get_hits'] + counts['get_misses'] or
            counts['put_attempts'] != counts['put_acks'] or not counts['get_hits'] or not counts['put_acks']):
        raise ValueError('native error/unknown or incomplete terminal denominator')
    expected = {'dfkv_cache_put_total': counts['put_acks'], 'dfkv_bytes_written_total': counts['put_bytes'],
                'dfkv_rdma_pull_requests_dispatched_total': counts['get_attempts'],
                'dfkv_rdma_pull_requests_terminal_total': counts['get_attempts'],
                'dfkv_uring_reads_total': counts['get_hits'], 'dfkv_uring_completions_total': counts['get_hits'],
                'dfkv_slab_lifetime_read_success_total': counts['get_hits'] if policy == 'lifetime' else 0}
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    deltas = {}
    for name, wanted in expected.items():
        deltas[name] = _delta(before, after, name)
        if deltas[name] != wanted:
            raise ValueError('native/Prom mismatch: ' + name)
    old, new = _rows(before, 'dfkv_rdma_pull_requests_terminal_total'), _rows(after, 'dfkv_rdma_pull_requests_terminal_total')
    for result, wanted in (('success_nonempty', counts['get_hits']), ('not_found', counts['get_misses'])):
        matching = [key for key in new if 'path="uring"' in key and 'result="' + result + '"' in key]
        if len(matching) != 1 or new[matching[0]] - old[matching[0]] != wanted:
            raise ValueError('RDMA terminal path mismatch')
    for key, value in new.items():
        if not any('path="uring"' in key and 'result="' + result + '"' in key
                   for result in ('success_nonempty', 'not_found')) and value != old[key]:
            raise ValueError('unexpected RDMA terminal path/result')
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if quick.total(after, name) != 1:
            raise ValueError('unhealthy storage')
    for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total', 'dfkv_uring_inflight'):
        if quick.total(before, name) != 0 or quick.total(after, name) != 0:
            raise ValueError('error/fallback/inflight')
    old_classes, new_classes = _rows(before, 'dfkv_slab_class_put_total'), _rows(after, 'dfkv_slab_class_put_total')
    distinct_classes = classes if distinct_classes is None else distinct_classes
    if (not isinstance(classes, dict) or not isinstance(distinct_classes, dict) or
            set(distinct_classes) != set(classes) or len(classes) < 2 or
            any(type(key) is not int or key <= 0 or type(value) is not int or value <= 0
                for key, value in classes.items()) or
            any(type(value) is not int or not 0 < value <= classes[key]
                for key, value in distinct_classes.items()) or
            sum(classes.values()) != counts['put_acks'] or old_classes.keys() != new_classes.keys()):
        raise ValueError('invalid per-class ACK/distinct denominator')
    admissions = {}
    # Class puts count slot admissions, not all successful PUT upserts. Existing
    # residents can be overwritten without incrementing this allocator counter.
    # Fresh stores and immutable generation-zero IDs guarantee each distinct ACK
    # object was admitted at least once; eviction/reinsert can increase that up
    # to its ACK count. Never label every ACK as a new allocator admission.
    for key, value in new_classes.items():
        match = re.fullmatch(r'\{slot_size="([0-9]+)"\}', key)
        if match is None or old_classes[key] != 0 or not value.is_integer():
            raise ValueError('invalid actual class admission counter')
        slot = int(match[1])
        admitted = int(value - old_classes[key])
        if not distinct_classes.get(slot, 0) <= admitted <= classes.get(slot, 0):
            raise ValueError('actual class admissions outside distinct/upsert bounds')
        if slot in classes:
            admissions[slot] = admitted
    if set(admissions) != set(classes):
        raise ValueError('actual size class absent')
    route_counts = {}
    for short, name in [('capacity_evictions', 'dfkv_evictions_total'),
                        ('watermark_evictions', 'dfkv_slab_watermark_evictions_total'),
                        ('background_reclaimed_slots', 'dfkv_slab_reclaimed_total'),
                        ('donor_extent_steals', 'dfkv_slab_extent_steals_total'),
                        ('watermark_freed_slot_bytes', 'dfkv_slab_watermark_freed_slot_bytes_total'),
                        ('watermark_freed_payload_bytes', 'dfkv_slab_watermark_freed_payload_bytes_total')]:
        route_counts[short] = _delta(before, after, name)
    # Metadata operation units are not victim counts (whole-extent callbacks
    # exist). Background reclaim can run during a non-transactional final
    # scrape: retain attempts/completions separately, require no I/O errors,
    # and do not invent a settled 1:1 or cross-counter snapshot invariant.
    if any(_rows(after, 'dfkv_slab_metadata_ops_errors_total').values()):
        raise ValueError('metadata I/O error')
    for path in ('foreground_evict', 'watermark_evict', 'background_evict'):
        values = {}
        for suffix in ('attempts', 'completed', 'errors'):
            name = 'dfkv_slab_metadata_ops_' + suffix + '_total'
            old, new = _rows(before, name), _rows(after, name)
            key = '{path="' + path + '"}'
            if key not in old or old.keys() != new.keys():
                raise ValueError('metadata path missing')
            values[suffix] = new[key] - old[key]
        if min(values.values()) < 0 or values['errors']:
            raise ValueError('metadata I/O regression/error')
        route_counts[path + '_metadata_attempts'] = values['attempts']
        route_counts[path + '_metadata_completed'] = values['completed']
    high_name = 'dfkv_slab_disk_watermark_high_observations_total'
    old_high, new_high = _rows(before, high_name), _rows(after, high_name)
    last = _rows(after, 'dfkv_slab_disk_watermark_last_high_bytes')
    if set(old_high) != labels or set(new_high) != labels or set(last) != labels:
        raise ValueError('watermark observation identity')
    if any(not value.is_integer() for value in list(old_high.values()) + list(new_high.values())):
        raise ValueError('noninteger watermark observation')
    high_delta = {key: new_high[key] - old_high[key] for key in labels}
    if any(value < 0 for value in high_delta.values()):
        raise ValueError('watermark observations regressed')
    if route == 'watermark':
        if (not any(value > 0 for value in high_delta.values()) or
                any(last[key] <= 2147483600 for key, value in high_delta.items() if value > 0) or
                any(route_counts[key] <= 0 for key in ('watermark_evictions', 'watermark_freed_slot_bytes',
                                                      'watermark_freed_payload_bytes', 'watermark_evict_metadata_completed'))):
            raise ValueError('no proven global watermark progress')
    elif (any(high_delta.values()) or any(route_counts[key] for key in
            ('watermark_evictions', 'background_reclaimed_slots', 'watermark_freed_slot_bytes',
             'watermark_freed_payload_bytes', 'watermark_evict_metadata_completed', 'background_evict_metadata_completed'))):
        raise ValueError('competing global/background route in disabled case')
    if route == 'class-pressure' and route_counts['foreground_evict_metadata_completed'] <= 0:
        raise ValueError('foreground class eviction not observed')
    route_counts.update(donor_confounded=route_counts['donor_extent_steals'] > 0,
                        background_active=route_counts['background_reclaimed_slots'] > 0,
                        above_high_observations=sum(high_delta.values()),
                        per_class_ack_upserts={str(key): value for key, value in classes.items()},
                        per_class_distinct_objects={str(key): value for key, value in distinct_classes.items()},
                        per_class_allocator_admissions={str(key): value for key, value in admissions.items()})
    return dict(deltas, **route_counts)


class Sampler:
    """Synchronous owned-server snapshots; no background thread can outlive it."""
    def __init__(self, directory, pid, *, proc=None):
        self.directory = Path(directory)
        self.pid = pid
        self.proc = Path(proc) if proc is not None else Path('/proc') / str(pid)
        self.rows = []
        self.identity = None

    def sample(self):
        begin = time.monotonic()
        text = baseline._scrape(12010)
        status = self.proc.joinpath('status').read_text()
        # comm may contain spaces: fields after the final ')' start at field3.
        fields = self.proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()
        identity = fields[19]
        if self.identity is not None and identity != self.identity:
            raise ValueError('owned server process identity changed')
        self.identity = identity
        values = {}
        for name, key in [('VmRSS', 'rss_kib'), ('Threads', 'threads')]:
            match = re.search(r'^' + name + r':\s+(\d+)', status, re.MULTILINE)
            if match is None:
                raise ValueError('missing owned-server resource: ' + name)
            values[key] = int(match[1])
        values.update(user_ticks=int(fields[11]), system_ticks=int(fields[12]),
                      starttime_ticks=int(identity), clock_ticks_per_sec=os.sysconf('SC_CLK_TCK'))
        for metric in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
            if quick.total(text, metric) != 1:
                raise ValueError('unhealthy periodic sample')
        for metric in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total'):
            if quick.total(text, metric) != 0:
                raise ValueError('periodic error/fallback')
        name = f'metrics-{len(self.rows):04d}.prom'
        path = self.directory / name
        with path.open('x') as stream:
            stream.write(text)
        self.rows.append(dict(file=name, sha256=baseline.file_sha(path), server_pid=self.pid,
                             started_monotonic_sec=begin, finished_monotonic_sec=time.monotonic(), resource_scope='owned-server-only-not-client-or-host', resource=values))
        # Failure preserves all previously completed snapshots/index rows.
        (self.directory / 'metrics-samples.json').write_text(json.dumps(self.rows, indent=2) + '\n')


def run_client(command, env, log_path, sampler, *, timeout=CLIENT_TIMEOUT_SEC, receipt=None):
    # This mutable receipt survives exceptions. A timeout is not proof of reap,
    # and a spawn failure must not claim a process ever existed.
    receipt = {} if receipt is None else receipt
    receipt.update(state='starting', pid=None, exit=None, reaped=False)
    child = None
    try:
        with Path(log_path).open('x') as log:
            child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT)
            receipt.update(state='running', pid=child.pid)
            start = time.monotonic()
            next_sample = start
            while child.poll() is None:
                now = time.monotonic()
                if now - start >= timeout:
                    raise TimeoutError('owned client process deadline exceeded')
                if now >= next_sample:
                    sampler.sample()
                    next_sample = time.monotonic() + SAMPLE_INTERVAL_SEC
                time.sleep(.2)
            code = child.wait(timeout=3)
            receipt.update(state='exited' if code == 0 else 'failed_exit', exit=code, reaped=True)
            if code != 0:
                raise subprocess.CalledProcessError(code, command)
            sampler.sample()
    except BaseException as error:
        receipt['error'] = f'{type(error).__name__}: {error}'
        if child is None:
            receipt['state'] = 'spawn_failed'
        raise
    finally:
        if child is not None and receipt['reaped'] is not True:
            try:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=3)
                code = child.poll()
                receipt.update(state='failed_reaped' if code is not None else 'unreaped',
                               exit=code, reaped=code is not None)
            except BaseException as error:
                code = child.poll()
                receipt.update(state='cleanup_failed', exit=code, reaped=code is not None,
                               cleanup_error=f'{type(error).__name__}: {error}')
                raise


def _hashes(paths):
    return {path.name: baseline.file_sha(path) for path in paths}


def _tools():
    paths = [Path(__file__), Path(__file__).with_name('prefix_delayed.py')]
    return dict(prefix._tools(), **_hashes(paths))


def run(build, roots, output, tag, *, route='class-pressure', pairs=3, start_pair=0,
        steps=1800, delays_sec=None):
    cases = plan(pairs, start_pair)
    env = environment(route)
    if type(steps) is not int or not 1 <= steps <= 1800:
        raise ValueError('steps must be 1..1800')
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('tag must be 1..40 ASCII letters/digits/_/-')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain comma separator')
    import prefix_delayed as workload
    config = dict(sizes=[4096, 1048577], steps=steps, arrival_batch=64, due_first=True)
    if delays_sec is not None:
        config['delays_sec'] = delays_sec
    document = workload.generate(**config)
    workload.validate(document)
    roots, output = common.guest_paths(roots, output, 'prefix-mixed-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-prefix-mixed-{tag}-{row['case_id']:02d}-{row['policy']}") for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data exists; use fresh tag')
    frozen_build, frozen_tools = _hashes(binaries), _tools()
    output.mkdir()
    plan_path = output / 'plan.json'
    with plan_path.open('x') as stream:
        json.dump(document, stream, sort_keys=True, allow_nan=False)
        stream.write('\n')
    frozen_plan = baseline.file_sha(plan_path)
    result = dict(model='prefix-mixed-rdma-v1', formal_ab=False, route=route,
                  gpu='excluded', inference_engine='excluded', ttft='excluded', passed=False,
                  errors=[], cases=cases, build_sha256=frozen_build, tools_sha256=frozen_tools,
                  plan_sha256=frozen_plan, plan_document_sha256=document['sha256'],
                  host_safety='external-fresh-backing-proof-and-live-thermal-watchdog-required',
                  scope='serial-mixed-size-prefix-proxy-donor-background-qualified-not-independent-concurrency')
    def check_frozen():
        if (_hashes(binaries) != frozen_build or _tools() != frozen_tools or baseline.file_sha(plan_path) != frozen_plan):
            raise ValueError('build, tools or plan changed')
    try:
        for row in cases:
            check_frozen()
            quick.check_ports()
            case = output / f"case-{row['case_id']:02d}-{row['policy']}"
            case.mkdir()
            for path in row['data_dirs']:
                Path(path).mkdir()
            command = [str(binaries[0]), '--dir', ','.join(row['data_dirs']), '--cap', str(CAPACITY_BYTES),
                       '--store-engine', 'slab', '--slab-write', 'buffered', '--slab-eviction-policy', row['policy'],
                       '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0', '--max-msg', '2097152',
                       '--metrics-port', '12010', '--metrics-bind', '127.0.0.1']
            client_command = [sys.executable, str(Path(__file__).with_name('prefix_delayed.py')), 'run',
                              '--lib', str(binaries[1]), '--members', 'n=127.0.0.1:12001',
                              '--plan', str(plan_path), '--output', str(case / 'client')]
            row.update(state='running', command=command, client_command=client_command,
                       client_state='not_attempted', client_process={}, client_timeout_sec=CLIENT_TIMEOUT_SEC,
                       environment={key: value for key, value in env.items() if key.startswith('DFKV_')})
            server = None
            try:
                with (case / 'server.log').open('x') as log:
                    server = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                    row['server_pid'] = server.pid
                    before = baseline._ready(server, 12010, case / 'server.log', row['policy'])
                    (case / 'before.prom').write_text(before)
                    check_initial(before, row['data_dirs'], route)
                    check_startup((case / 'server.log').read_text(), row['policy'], route)
                    check_frozen()
                    sampler = Sampler(case, server.pid)
                    row['client_state'] = 'running'
                    run_client(client_command, env, case / 'client.log', sampler, receipt=row['client_process'])
                    row.update(client_state='exited', client_exit=0)
                    check_frozen()
                    after = baseline._scrape(12010)
                    (case / 'after.prom').write_text(after)
                    report_path = case / 'client/report.json'
                    report = workload.load(report_path)
                    summary = workload.check(document, report)
                    if report.get('passed') is not True or report.get('transport') != 'rdma':
                        raise ValueError('real RDMA report must pass')
                    row.update(client_report_sha256=baseline.file_sha(report_path), summary=summary,
                               server_delta=check_metrics(before, after, row['policy'], summary,
                                                          row['data_dirs'], route, put_classes(workload, document, report),
                                                          distinct_classes=put_classes(workload, document, report, distinct=True)),
                               sample_count=len(sampler.rows), state='passed')
            except BaseException as error:
                row.update(state='failed', error=f'{type(error).__name__}: {error}')
                if row['client_state'] == 'running':
                    row['client_state'] = row['client_process'].get('state', 'not_started')
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
        check_frozen()
        result['passed'] = True
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            raise
    finally:
        (output / 'matrix.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--data-root', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--route', choices=ROUTES, required=True)
    parser.add_argument('--pairs', type=int, default=3)
    parser.add_argument('--start-pair', type=int, default=0)
    parser.add_argument('--steps', type=int, default=1800)
    parser.add_argument('--delays-sec', nargs=3, type=int)
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag, route=args.route,
                     pairs=args.pairs, start_pair=args.start_pair, steps=args.steps, delays_sec=args.delays_sec)
        print(json.dumps(dict(passed=result['passed'], output=str(args.output))))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print('mixed prefix runner error: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
