#!/usr/bin/env python3
"""Guest-only independent prefix streams, not global open-loop or formal A/B.

Fresh host backing proof and a live thermal watchdog are external prerequisites.
Each worker owns a native handle. Overlapping direct C ABI intervals prove
independent client calls, not physical disk I/O or an atomic chain snapshot.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_baseline_smoke as baseline
import quick_eviction as quick
import run_prefix_mixed as mixed
import run_rdma_arrival as arrival
import run_sg_validation as common

CAPACITY_BYTES = mixed.CAPACITY_BYTES
CLIENT_TIMEOUT_SEC = 600
Sampler = mixed.Sampler
run_client = mixed.run_client


def plan(workers=(1, 4, 16), pairs=3):
    return [dict(row, state='not_attempted') for row in arrival.plan(workers, pairs)]


def environment():
    return mixed.environment('class-pressure')


def check_startup(text, policy):
    mixed.check_startup(text, policy, 'class-pressure')


def check_initial(text, dirs):
    mixed.check_initial(text, dirs, 'class-pressure')


def check_concurrency(summary, workers):
    peak = summary.get('peak_abi_overlap')
    get_peak = summary.get('peak_get_abi_overlap')
    if (type(peak) is not int or not 1 <= peak <= workers or
            type(get_peak) is not int or not 1 <= get_peak <= peak):
        raise ValueError('invalid observed native ABI overlap')
    # PUT-only overlap cannot establish concurrent prefix reads. Both fields
    # derive from direct ABI brackets in the measurement phase only.
    if workers > 1 and (peak < 2 or get_peak < 2):
        raise ValueError('multiple handles do not prove concurrent prefix GET activity')
    return dict(workers=workers, peak_abi_overlap=peak,
                peak_get_abi_overlap=get_peak,
                peak_backend_overlap=summary.get('peak_backend_overlap'),
                scope='direct-c-abi-intervals-not-physical-io-parallelism')


def put_identities(report):
    """ACK upserts and distinct immutable objects are different denominators."""
    events = list(report['initial_events'])
    for row in report['requests']:
        events.extend(row['events'])
    acks = [event for event in events if event['op'] == 'put' and event['status'] == 'ack']
    return len(acks), len({event['object_id'] for event in acks})


def check_metrics(before, after, policy, summary, dirs, *, ack_puts, distinct_puts,
                  require_pressure=False):
    check_initial(before, dirs)
    mixed.check_geometry(after, dirs, 'class-pressure')
    if type(require_pressure) is not bool or policy not in ('legacy', 'lifetime'):
        raise ValueError('invalid pressure/policy selection')
    counts = summary['native']
    fields = {'get_hits', 'get_misses', 'get_errors', 'get_unknown', 'get_attempts',
              'put_acks', 'put_bytes', 'put_errors', 'put_unknown', 'put_attempts'}
    if (not isinstance(counts, dict) or set(counts) != fields or
            any(type(value) is not int or value < 0 for value in counts.values())):
        raise ValueError('invalid native denominator')
    if (any(counts[key] for key in ('get_errors', 'get_unknown', 'put_errors', 'put_unknown')) or
            counts['get_attempts'] != counts['get_hits'] + counts['get_misses'] or
            counts['put_attempts'] != counts['put_acks'] or not counts['get_hits'] or
            type(ack_puts) is not int or ack_puts != counts['put_acks'] or
            type(distinct_puts) is not int or not 0 < distinct_puts <= ack_puts):
        raise ValueError('native errors/unknown or incomplete ACK denominator')
    expected = {'dfkv_cache_put_total': counts['put_acks'],
                'dfkv_bytes_written_total': counts['put_bytes'],
                'dfkv_rdma_pull_requests_dispatched_total': counts['get_attempts'],
                'dfkv_rdma_pull_requests_terminal_total': counts['get_attempts'],
                'dfkv_uring_reads_total': counts['get_hits'],
                'dfkv_uring_completions_total': counts['get_hits'],
                'dfkv_slab_lifetime_read_success_total': counts['get_hits'] if policy == 'lifetime' else 0}
    delta = {name: mixed._delta(before, after, name) for name in expected}
    if delta != expected:
        raise ValueError('native/Prom terminal or byte denominator mismatch')
    old = mixed._rows(before, 'dfkv_rdma_pull_requests_terminal_total')
    new = mixed._rows(after, 'dfkv_rdma_pull_requests_terminal_total')
    if old.keys() != new.keys():
        raise ValueError('RDMA terminal identity changed')
    accepted = set()
    for outcome, wanted in (('success_nonempty', counts['get_hits']), ('not_found', counts['get_misses'])):
        labels = [key for key in new if 'path="uring"' in key and 'result="' + outcome + '"' in key]
        if len(labels) != 1 or new[labels[0]] - old[labels[0]] != wanted:
            raise ValueError('unexpected RDMA terminal path/result')
        accepted.add(labels[0])
    if any(new[key] != old[key] for key in new if key not in accepted):
        raise ValueError('unexpected fallback/error terminal')
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if mixed._rows(after, name) != {'': 1.0}:
            raise ValueError('unhealthy storage')
    # Error counters are an op/status-labeled family, not a scalar. Preserve
    # every initial series identity and reject any error, not just a net delta.
    old_errors = mixed._rows(before, 'dfkv_errors_total')
    new_errors = mixed._rows(after, 'dfkv_errors_total')
    if (old_errors.keys() != new_errors.keys() or
            any(not value.is_integer() or value != 0
                for value in list(old_errors.values()) + list(new_errors.values()))):
        raise ValueError('error counter identity/nonzero mismatch')
    for name in ('dfkv_uring_init_fallbacks_total', 'dfkv_uring_inflight'):
        if mixed._rows(before, name) != {'': 0.0} or mixed._rows(after, name) != {'': 0.0}:
            raise ValueError('fallback/unsettled native calls')
    for name in ('dfkv_slab_watermark_evictions_total', 'dfkv_slab_reclaimed_total',
                 'dfkv_slab_watermark_freed_slot_bytes_total',
                 'dfkv_slab_watermark_freed_payload_bytes_total'):
        if mixed._delta(before, after, name) != 0:
            raise ValueError('competing global/background reclaim activity')
    if any(mixed._rows(after, 'dfkv_slab_disk_watermark_high_observations_total').values()):
        raise ValueError('disabled watermark observed high')
    # One payload class (<=4KiB) maps to the default 1MiB slot. Existing-resident
    # refill upserts need not allocate; every distinct ACK identity must do so.
    old_classes = mixed._rows(before, 'dfkv_slab_class_put_total')
    new_classes = mixed._rows(after, 'dfkv_slab_class_put_total')
    if old_classes.keys() != new_classes.keys() or any(old_classes.values()):
        raise ValueError('class counter identity/initial state mismatch')
    admitted = None
    for label, value in new_classes.items():
        match = re.fullmatch(r'\{slot_size="([0-9]+)"\}', label)
        if match is None or not value.is_integer() or value < 0:
            raise ValueError('invalid allocator admission')
        if int(match[1]) == 1048576:
            admitted = int(value)
        elif value:
            raise ValueError('unexpected active size class')
    if admitted is None or not distinct_puts <= admitted <= ack_puts:
        raise ValueError('allocator admissions outside distinct/upsert bounds')
    if any(mixed._rows(after, 'dfkv_slab_metadata_ops_errors_total').values()):
        raise ValueError('metadata I/O error')
    evictions = mixed._delta(before, after, 'dfkv_evictions_total')
    donor = mixed._delta(before, after, 'dfkv_slab_extent_steals_total')
    foreground = {}
    # Aggregate evictions and metadata callbacks need not be one-for-one when
    # donor whole extents exist. Keep the denominators separate, not atomic.
    for suffix in ('attempts', 'completed', 'errors'):
        name = 'dfkv_slab_metadata_ops_' + suffix + '_total'
        old_rows, new_rows = mixed._rows(before, name), mixed._rows(after, name)
        if old_rows.keys() != new_rows.keys():
            raise ValueError('metadata identity changed')
        for path in ('foreground_evict', 'watermark_evict', 'background_evict'):
            key = '{path="' + path + '"}'
            if key not in new_rows:
                raise ValueError('metadata path missing')
            value = new_rows[key] - old_rows[key]
            if not value.is_integer() or value < 0:
                raise ValueError('invalid metadata progress')
            if (path != 'foreground_evict' or suffix == 'errors') and value:
                raise ValueError('competing reclaim or metadata error')
            if path == 'foreground_evict':
                foreground[suffix] = value
    if require_pressure and (evictions <= 0 or foreground['completed'] <= 0):
        raise ValueError('required class foreground eviction was not observed')
    return dict(delta, capacity_evictions=evictions,
                foreground_metadata_attempts=foreground['attempts'],
                foreground_metadata_completed=foreground['completed'],
                donor_extent_steals=donor, donor_confounded=donor > 0,
                ack_put_upserts=ack_puts, distinct_ack_objects=distinct_puts,
                allocator_admissions=admitted, pressure_required=require_pressure)


def _tools():
    names = ('prefix_concurrent.py', 'run_prefix_concurrent.py', 'run_rdma_arrival.py')
    return dict(mixed._tools(), **mixed._hashes([Path(__file__).with_name(name) for name in names]))


def run(build, roots, output, tag, *, workers=(1, 4, 16), pairs=3,
        rounds=120, interval_ns=100000000, arrival_batch=8):
    cases = plan(workers, pairs)
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('tag must be 1..40 ASCII letters/digits/_/-')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain comma separator')
    import prefix_concurrent as workload
    documents = {worker: workload.generate(workers=worker, rounds=rounds,
                  interval_ns=interval_ns, arrival_batch=arrival_batch, prefill=12288,
                  length=4096, timeout_ns=5000000000) for worker in workers}
    for document in documents.values():
        workload.validate(document)
    roots, output = common.guest_paths(roots, output, 'prefix-concurrent-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-prefix-concurrent-{tag}-{row['case_id']:02d}-{row['policy']}") for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data exists; use fresh tag')
    frozen_build, frozen_tools = mixed._hashes(binaries), _tools()
    output.mkdir()
    frozen_plans, plan_paths = {}, {}
    for worker, document in documents.items():
        path = output / f'plan-workers-{worker}.json'
        with path.open('x') as stream:
            json.dump(document, stream, sort_keys=True, allow_nan=False)
            stream.write('\n')
        plan_paths[worker] = path
        frozen_plans[worker] = baseline.file_sha(path)
    env = environment()
    require_pressure = rounds >= 120 and arrival_batch > 0
    result = dict(model='prefix-concurrent-rdma-v1', formal_ab=False,
                  gpu='excluded', inference_engine='excluded', ttft='excluded',
                  passed=False, errors=[], cases=cases, build_sha256=frozen_build,
                  tools_sha256=frozen_tools, plans_sha256={str(key): value for key, value in frozen_plans.items()},
                  plan_document_sha256={str(key): value['sha256'] for key, value in documents.items()},
                  pressure_required=require_pressure,
                  host_safety='external-fresh-backing-proof-and-live-thermal-watchdog-required',
                  scope='independent-paced-worker-streams-not-global-open-loop-or-physical-io-proof')

    def check_frozen():
        if (mixed._hashes(binaries) != frozen_build or _tools() != frozen_tools or
                any(baseline.file_sha(path) != frozen_plans[worker] for worker, path in plan_paths.items())):
            raise ValueError('build, tools or plans changed')

    def capture(row, case, document):
        path = case / 'client/report.json'
        if path.is_file():
            row['client_report_sha256'] = baseline.file_sha(path)
            report = workload.load(path)
            row['summary'] = workload.check(document, report)
            return report
        raise ValueError('client report missing; outcomes remain unknown')

    try:
        for row in cases:
            check_frozen()
            quick.check_ports()
            case = output / f"case-{row['case_id']:02d}-{row['policy']}"
            case.mkdir()
            for path in row['data_dirs']:
                Path(path).mkdir()
            document = documents[row['workers']]
            command = [str(binaries[0]), '--dir', ','.join(row['data_dirs']), '--cap', str(CAPACITY_BYTES),
                       '--store-engine', 'slab', '--slab-write', 'buffered', '--slab-eviction-policy', row['policy'],
                       '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0', '--max-msg', '2097152',
                       '--metrics-port', '12010', '--metrics-bind', '127.0.0.1']
            client_command = [sys.executable, str(Path(__file__).with_name('prefix_concurrent.py')), 'run',
                              '--lib', str(binaries[1]), '--members', 'n=127.0.0.1:12001',
                              '--plan', str(plan_paths[row['workers']]), '--output', str(case / 'client')]
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
                    check_initial(before, row['data_dirs'])
                    check_startup((case / 'server.log').read_text(), row['policy'])
                    check_frozen()
                    sampler = Sampler(case, server.pid)
                    row['client_state'] = 'running'
                    run_client(client_command, env, case / 'client.log', sampler,
                               timeout=CLIENT_TIMEOUT_SEC, receipt=row['client_process'])
                    row.update(client_state='exited', client_exit=0)
                    check_frozen()
                    report = capture(row, case, document)
                    if report.get('passed') is not True or report.get('transport') != 'rdma':
                        raise ValueError('real RDMA report must pass')
                    row['concurrency'] = check_concurrency(row['summary'], row['workers'])
                    after = baseline._scrape(12010)
                    (case / 'after.prom').write_text(after)
                    ack_puts, distinct_puts = put_identities(report)
                    row['server_delta'] = check_metrics(before, after, row['policy'], row['summary'],
                        row['data_dirs'], ack_puts=ack_puts, distinct_puts=distinct_puts,
                        require_pressure=require_pressure)
                    row.update(sample_count=len(sampler.rows), state='passed')
            except BaseException as error:
                row.update(state='failed', error=f'{type(error).__name__}: {error}')
                if row['client_state'] == 'running':
                    row['client_state'] = row['client_process'].get('state', 'not_started')
                try:
                    capture(row, case, document)
                except Exception as capture_error:
                    row['report_error'] = f'{type(capture_error).__name__}: {capture_error}'
                if server is not None and server.poll() is None:
                    try:
                        (case / 'after-failure.prom').write_text(baseline._scrape(12010))
                    except Exception as scrape_error:
                        row['failure_scrape_error'] = f'{type(scrape_error).__name__}: {scrape_error}'
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
    parser.add_argument('--workers', type=int, nargs='+', default=[1, 4, 16])
    parser.add_argument('--pairs', type=int, default=3)
    parser.add_argument('--rounds', type=int, default=120)
    parser.add_argument('--interval-ns', type=int, default=100000000)
    parser.add_argument('--arrival-batch', type=int, default=8)
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag, workers=args.workers,
                     pairs=args.pairs, rounds=args.rounds, interval_ns=args.interval_ns,
                     arrival_batch=args.arrival_batch)
        print(json.dumps(dict(passed=result['passed'], output=str(args.output))))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print('concurrent prefix runner error: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
