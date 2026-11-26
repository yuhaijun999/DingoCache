#!/usr/bin/env python3
"""Guest-only clean and drained-SIGKILL RDMA prefix recovery diagnostics.

No kernel in-flight kill or power-loss claim. Fresh backing proof and the live
thermal watchdog remain external prerequisites; this controller never edits host
safety controls. Dirty slab startup intentionally discards acknowledged objects.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_prefix_mixed as mixed
import prefix_recovery as workload

baseline, common, quick = mixed.baseline, mixed.common, mixed.quick
CLIENT_TIMEOUT_SEC = 2100
ROUTE = 'class-pressure'


def plan(cycles=1):
    if type(cycles) is not int or not 1 <= cycles <= 100:
        raise ValueError('cycles must be 1..100')
    rows = []
    for stop in ('clean', 'dirty'):
        for transition in ('same', 'switch'):
            for first in ('legacy', 'lifetime'):
                policies = [first]
                for _ in range(cycles):
                    policies.append(policies[-1] if transition == 'same' else
                                    ('lifetime' if policies[-1] == 'legacy' else 'legacy'))
                rows.append(dict(case_id=len(rows), stop=stop, transition=transition,
                                 policies=policies, state='not_attempted', stages=[]))
    return rows


def check_restart(text, dirs, *, dirty, policy):
    mixed.check_geometry(text, dirs, ROUTE)
    resets = mixed._rows(text, 'dfkv_slab_unclean_resets_total')
    if resets != {'': 3 if dirty else 0}:
        raise ValueError('unexpected unclean reset count')
    objects = mixed._rows(text, 'dfkv_disk_objects')
    if any(not value.is_integer() or value < 0 for value in objects.values()):
        raise ValueError('invalid recovered object count')
    if dirty and any(objects.values()):
        raise ValueError('dirty reset retained objects')
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    # Recovery registers every committed resident before readiness. Sample this
    # gauge before any GET can change recovered-unread accounting or refill.
    visible = mixed._rows(text, 'dfkv_slab_lifetime_visible_recovered_objects')
    if visible != {'': sum(objects.values()) if policy == 'lifetime' else 0}:
        raise ValueError('visible recovered residents do not match startup objects')
    check_health(text)


def check_health(text):
    for metric in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if mixed._rows(text, metric) != {'': 1}:
            raise ValueError('unhealthy storage: ' + metric)
    expected_errors = {'{op="put",status="io"}': 0, '{op="get",status="io"}': 0,
                       '{op="any",status="invalid"}': 0}
    if mixed._rows(text, 'dfkv_errors_total') != expected_errors:
        raise ValueError('op errors or incomplete error family')
    for metric in ('dfkv_uring_init_fallbacks_total', 'dfkv_uring_inflight'):
        if mixed._rows(text, metric) != {'': 0}:
            raise ValueError('error/fallback/inflight: ' + metric)
    errors = mixed._rows(text, 'dfkv_slab_metadata_ops_errors_total')
    paths = ('record_write', 'foreground_evict', 'background_evict', 'watermark_evict',
             'extent_bind', 'donor_bind', 'table_sync', 'bind_sync', 'donor_sync')
    if errors != {'{path="' + path + '"}': 0 for path in paths}:
        raise ValueError('metadata I/O error or missing path')


def check_client_metrics(before, after, policy, counts):
    """Reconcile every native terminal call, not just successful prefix hits."""
    check_health(before)
    check_health(after)
    fields = {'put_acks', 'put_bytes', 'put_attempts', 'put_errors', 'put_unknown',
              'get_hits', 'get_misses', 'get_attempts', 'get_errors', 'get_unknown'}
    if (not isinstance(counts, dict) or set(counts) != fields or
            any(type(value) is not int or value < 0 for value in counts.values()) or
            any(counts[key] for key in ('get_errors', 'get_unknown', 'put_errors', 'put_unknown')) or
            counts['get_attempts'] != counts['get_hits'] + counts['get_misses'] or
            counts['put_attempts'] != counts['put_acks']):
        raise ValueError('incomplete native denominator or error/unknown')
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    expected = {'dfkv_cache_put_total': counts['put_acks'],
                'dfkv_bytes_written_total': counts['put_bytes'],
                'dfkv_rdma_pull_requests_dispatched_total': counts['get_attempts'],
                'dfkv_rdma_pull_requests_terminal_total': counts['get_attempts'],
                'dfkv_uring_reads_total': counts['get_hits'],
                'dfkv_uring_completions_total': counts['get_hits'],
                'dfkv_slab_lifetime_read_success_total': counts['get_hits'] if policy == 'lifetime' else 0}
    deltas = {}
    for name, wanted in expected.items():
        deltas[name] = mixed._delta(before, after, name)
        if deltas[name] != wanted:
            raise ValueError('native/Prom mismatch: ' + name)
    name = 'dfkv_rdma_pull_requests_terminal_total'
    old, new = mixed._rows(before, name), mixed._rows(after, name)
    wanted = {'{path="uring",result="success_nonempty"}': counts['get_hits'],
              '{path="uring",result="not_found"}': counts['get_misses']}
    if not wanted.keys() <= new.keys():
        raise ValueError('missing uring terminal result/path')
    for key in new:
        if new[key] - old[key] != wanted.get(key, 0):
            raise ValueError('unexpected terminal result/path: ' + key)
    deltas['capacity_evictions'] = mixed._delta(before, after, 'dfkv_evictions_total')
    return deltas


def check_snapshot(report):
    # Restore after unrelated pressure so clean preservation cannot pass on an
    # all-miss snapshot. The immutable manifest has both sizes and four chains.
    if (report.get('objects') != ['hit'] * 32 or
            report.get('summary', {}).get('confirmed_prefix_tokens') != 640):
        raise ValueError('snapshot must preserve all 32 payload-validated objects and 640 tokens')


def stop_server(server, mode, receipt):
    """Bounded stop of this Popen only; never hide an unexpected exit."""
    receipt.update(requested=mode, scope='drained-client-not-kernel-inflight')
    try:
        if mode not in ('clean', 'dirty'):
            raise ValueError('unknown stop mode')
        if server.poll() is not None:
            raise RuntimeError('owned server exited before requested stop')
        if mode == 'clean':
            common.stop_owned(server)
        elif mode == 'dirty':
            server.kill()
            if server.wait(timeout=3) != -9:
                raise RuntimeError('owned SIGKILL did not exit -9')
        else:
            raise ValueError('unknown stop mode')
    finally:
        receipt.update(exit=server.poll(), reaped=server.poll() is not None)


def _tools():
    paths = [Path(__file__), Path(workload.__file__),
             Path(__file__).with_name('prefix_concurrent.py')]
    return dict(mixed._tools(), **mixed._hashes(paths))


def run(build, roots, output, tag, *, cycles=1, dwell_sec=0, pressure_rounds=0,
        pressure_interval_ms=100):
    cases = plan(cycles)
    for name, value, low, high in [('dwell_sec', dwell_sec, 0, 60),
                                  ('pressure_rounds', pressure_rounds, 0, 600),
                                  ('pressure_interval_ms', pressure_interval_ms, 1, 1000)]:
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f'{name} must be {low}..{high}')
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('tag must be 1..40 ASCII letters/digits/_/-')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain comma separator')
    roots, output = common.guest_paths(roots, output, 'prefix-recovery-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-prefix-recovery-{tag}-{row['case_id']:02d}") for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data exists; use fresh tag')
    manifest = workload.generate()
    workload.validate(manifest)
    pressure_plans = {}
    if pressure_rounds:
        import prefix_concurrent as concurrent
        for row in cases:
            for index in range(cycles + 1):
                seed = concurrent.DEFAULTS['seed'] + row['case_id'] * (cycles + 1) + index
                pressure_plans[row['case_id'], index] = concurrent.generate(
                    seed=seed, rounds=pressure_rounds, workers=4, arrival_batch=8,
                    interval_ns=pressure_interval_ms * 1000000)
    frozen_build, frozen_tools = mixed._hashes(binaries), _tools()
    output.mkdir()
    manifest_path = output / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, allow_nan=False) + '\n')
    frozen_files = {manifest_path: baseline.file_sha(manifest_path)}
    manifests = {0: manifest}
    manifest_paths = {0: manifest_path}
    for checkpoint in range(1, cycles + 2):
        document = workload.generate(checkpoint=checkpoint)
        workload.validate(document)
        path = output / f'checkpoint-{checkpoint:03d}.json'
        path.write_text(json.dumps(document, sort_keys=True, allow_nan=False) + '\n')
        manifests[checkpoint] = document
        manifest_paths[checkpoint] = path
        frozen_files[path] = baseline.file_sha(path)
    pressure_paths = {}
    for identity, pressure_plan in pressure_plans.items():
        path = output / f'pressure-{identity[0]:02d}-{identity[1]:03d}.json'
        path.write_text(json.dumps(pressure_plan, sort_keys=True, allow_nan=False) + '\n')
        pressure_paths[identity] = path
        frozen_files[path] = baseline.file_sha(path)
    result = dict(model='prefix-recovery-rdma-v1', passed=False, errors=[], cases=cases,
                  formal_ab=False, scope='guest-only-drained-client-restarts-not-power-loss-or-inflight',
                  host_safety='external-fresh-backing-proof-and-live-thermal-watchdog-required',
                  build_sha256=frozen_build, tools_sha256=frozen_tools,
                  manifest_sha256=frozen_files[manifest_path], cycles=cycles, dwell_sec=dwell_sec,
                  pressure_rounds=pressure_rounds, pressure_interval_ms=pressure_interval_ms,
                  pressure_namespace='separate-prefix-concurrent-v1-unique-seed-per-stage',
                  checkpoint_scope='new-key-cohort-prepared-after-pressure-not-pressure-survivor-benefit',
                  frozen_files_sha256={path.name: digest for path, digest in frozen_files.items()})
    env = mixed.environment(ROUTE)

    def check_frozen():
        if (mixed._hashes(binaries) != frozen_build or _tools() != frozen_tools or
                any(baseline.file_sha(path) != digest for path, digest in frozen_files.items())):
            raise ValueError('build, tools or frozen manifests changed')

    try:
        for row in cases:
            case = output / f"case-{row['case_id']:02d}-{row['stop']}-{row['transition']}"
            case.mkdir()
            for directory in row['data_dirs']:
                Path(directory).mkdir()
            row.update(state='running', environment={key: value for key, value in env.items() if key.startswith('DFKV_')})
            expected_path = None
            try:
                for index, policy in enumerate(row['policies']):
                    check_frozen()
                    quick.check_ports()
                    stage = case / f'start-{index:03d}-{policy}'
                    stage.mkdir()
                    command = [str(binaries[0]), '--dir', ','.join(row['data_dirs']), '--cap', str(mixed.CAPACITY_BYTES),
                               '--store-engine', 'slab', '--slab-write', 'buffered', '--slab-eviction-policy', policy,
                               '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0', '--max-msg', '2097152',
                               '--metrics-port', '12010', '--metrics-bind', '127.0.0.1']
                    stage_started = time.monotonic()
                    entry = dict(policy=policy, state='running', command=command, clients=[], stop={},
                                 started_monotonic_sec=stage_started, startup_elapsed_sec=None)
                    entry.update(previous_manifest=str(manifest_paths[index]),
                                 previous_manifest_sha256=frozen_files[manifest_paths[index]],
                                 current_manifest=str(manifest_paths[index + 1]),
                                 current_manifest_sha256=frozen_files[manifest_paths[index + 1]],
                                 expected_snapshot=str(expected_path) if expected_path is not None else None,
                                 expected_snapshot_sha256=baseline.file_sha(expected_path) if expected_path is not None else None)
                    row['stages'].append(entry)
                    server = None
                    try:
                        with (stage / 'server.log').open('x') as log:
                            server = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                                      stdout=log, stderr=subprocess.STDOUT)
                            entry['server_pid'] = server.pid
                            before = baseline._ready(server, 12010, stage / 'server.log', policy)
                            entry['startup_elapsed_sec'] = time.monotonic() - stage_started
                            (stage / 'startup.prom').write_text(before)
                            mixed.check_startup((stage / 'server.log').read_text(), policy, ROUTE)
                            if index == 0:
                                mixed.check_initial(before, row['data_dirs'], ROUTE)
                                check_restart(before, row['data_dirs'], dirty=False, policy=policy)
                            else:
                                check_restart(before, row['data_dirs'], dirty=row['stop'] == 'dirty', policy=policy)
                            sampler = mixed.Sampler(stage, server.pid)

                            def client(mode, expected=None, label=None, *, checkpoint=None):
                                checkpoint = index if checkpoint is None else checkpoint
                                selected_manifest = manifests[checkpoint]
                                selected_path = manifest_paths[checkpoint]
                                label = mode if label is None else label
                                check_frozen()
                                child_output = stage / label
                                cmd = [sys.executable, str(Path(workload.__file__)), '--lib', str(binaries[1]),
                                       '--members', 'n=127.0.0.1:12001', '--manifest', str(selected_path),
                                       '--output', str(child_output), '--mode', mode]
                                expect = None
                                if expected is not None:
                                    expect = 'empty' if row['stop'] == 'dirty' else 'preserve'
                                    cmd += ['--expected', str(expected), '--expect', expect]
                                receipt = dict(mode=mode, label=label, command=cmd, timeout_sec=CLIENT_TIMEOUT_SEC, process={},
                                               manifest=str(selected_path), manifest_sha256=frozen_files[selected_path])
                                entry['clients'].append(receipt)
                                client_before = baseline._scrape(12010)
                                (stage / (label + '-before.prom')).write_text(client_before)
                                check_health(client_before)
                                mixed.run_client(cmd, env, stage / (label + '.log'), sampler,
                                                 timeout=CLIENT_TIMEOUT_SEC, receipt=receipt['process'])
                                check_frozen()
                                report_path = child_output / 'report.json'
                                report = workload.load(report_path)
                                summary = workload.check(selected_manifest, report,
                                                         workload.load(expected) if expected is not None else None, expect)
                                if report.get('passed') is not True or report.get('transport') != 'rdma' or report.get('mode') != mode:
                                    raise ValueError('real RDMA recovery report must pass selected mode')
                                if mode == 'snapshot':
                                    check_snapshot(report)
                                receipt.update(report_sha256=baseline.file_sha(report_path), summary=summary)
                                text = baseline._scrape(12010)
                                (stage / (label + '-after.prom')).write_text(text)
                                native = summary['native']
                                counts = dict(put_acks=native['put_ack'], put_bytes=summary['put_bytes'],
                                              put_attempts=native['put_ack'] + native['put_error'] + native['put_unknown'],
                                              put_errors=native['put_error'], put_unknown=native['put_unknown'],
                                              get_hits=native['get_hit'], get_misses=native['get_miss'],
                                              get_attempts=sum(native[key] for key in ('get_hit', 'get_miss', 'get_error', 'get_unknown')),
                                              get_errors=native['get_error'], get_unknown=native['get_unknown'])
                                receipt['server_delta'] = check_client_metrics(client_before, text, policy, counts)
                                return report_path

                            if index == 0:
                                client('seed')
                            else:
                                client('verify', expected_path)
                                client('refill')
                            # The first size class can bind all four extents on
                            # each disk. Binding the second class may then steal
                            # a donor extent and evict earlier small objects,
                            # despite this probe's tiny total payload. Both
                            # classes now have free slots: one bounded extra
                            # pass restores those setup victims. Never loop or
                            # weaken the all-resident snapshot gate on failure.
                            client('refill', label='stabilize')
                            if pressure_plans:
                                pressure_plan = pressure_plans[row['case_id'], index]
                                pressure_path = pressure_paths[row['case_id'], index]
                                cmd = [sys.executable, str(Path(__file__).with_name('prefix_concurrent.py')), 'run',
                                       '--lib', str(binaries[1]), '--members', 'n=127.0.0.1:12001',
                                       '--plan', str(pressure_path), '--output', str(stage / 'pressure')]
                                receipt = dict(mode='pressure', command=cmd, timeout_sec=CLIENT_TIMEOUT_SEC, process={})
                                entry['clients'].append(receipt)
                                pressure_before = baseline._scrape(12010)
                                (stage / 'pressure-before.prom').write_text(pressure_before)
                                check_health(pressure_before)
                                mixed.run_client(cmd, env, stage / 'pressure.log', sampler,
                                                 timeout=CLIENT_TIMEOUT_SEC, receipt=receipt['process'])
                                report_path = stage / 'pressure/report.json'
                                report = concurrent.load(report_path)
                                receipt['summary'] = concurrent.check(pressure_plan, report)
                                receipt['report_sha256'] = baseline.file_sha(report_path)
                                if report.get('passed') is not True or report.get('transport') != 'rdma':
                                    raise ValueError('real RDMA pressure report must pass')
                                pressure_after = baseline._scrape(12010)
                                (stage / 'pressure-after.prom').write_text(pressure_after)
                                receipt['server_delta'] = check_client_metrics(
                                    pressure_before, pressure_after, policy, receipt['summary']['native'])
                                if receipt['server_delta']['capacity_evictions'] <= 0:
                                    raise ValueError('pressure phase observed no capacity eviction')
                                check_frozen()
                            deadline = time.monotonic() + dwell_sec
                            while time.monotonic() < deadline:
                                sampler.sample()
                                time.sleep(min(1, max(0, deadline - time.monotonic())))
                            # Old hot upserts preserve their heat and cannot
                            # safely re-enroll a class after pressure. Prepare a
                            # new immutable key cohort with two fixed PUT-only
                            # passes; the second repairs donor setup victims.
                            # This is restart evidence, not pressure-survivor
                            # benefit. Snapshot is GET-only; no writes follow.
                            client('seed', label='checkpoint-seed', checkpoint=index + 1)
                            client('seed', label='checkpoint-stabilize', checkpoint=index + 1)
                            expected_path = client('snapshot', checkpoint=index + 1)
                            # Last startup is also stopped cleanly. Every requested
                            # crash is preceded by a completed, drained snapshot.
                            stop_server(server, row['stop'] if index < cycles else 'clean', entry['stop'])
                            check_frozen()
                            entry.update(state='passed', sample_count=len(sampler.rows))
                    except BaseException as error:
                        entry.update(state='failed', error=f'{type(error).__name__}: {error}')
                        if server is not None and server.poll() is None:
                            try:
                                (stage / 'failure.prom').write_text(baseline._scrape(12010))
                            except BaseException as scrape_error:
                                entry['failure_scrape_error'] = f'{type(scrape_error).__name__}: {scrape_error}'
                        raise
                    finally:
                        if server is not None:
                            try:
                                if server.poll() is None:
                                    common.stop_owned(server)
                            except BaseException as error:
                                entry.update(state='failed', cleanup_error=f'{type(error).__name__}: {error}')
                                raise
                            finally:
                                entry.update(server_exit=server.poll(), server_reaped=server.poll() is not None)
                                entry['finished_monotonic_sec'] = time.monotonic()
                                entry['elapsed_sec'] = entry['finished_monotonic_sec'] - stage_started
                        else:
                            entry['finished_monotonic_sec'] = time.monotonic()
                            entry['elapsed_sec'] = entry['finished_monotonic_sec'] - stage_started
                    quick.check_ports()
                row['state'] = 'passed'
            except BaseException as error:
                row.update(state='failed', error=f'{type(error).__name__}: {error}')
                raise
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
    parser.add_argument('--cycles', type=int, default=1)
    parser.add_argument('--dwell-sec', type=int, default=0)
    parser.add_argument('--pressure-rounds', type=int, default=0)
    parser.add_argument('--pressure-interval-ms', type=int, default=100)
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag, cycles=args.cycles,
                     dwell_sec=args.dwell_sec, pressure_rounds=args.pressure_rounds,
                     pressure_interval_ms=args.pressure_interval_ms)
        print(json.dumps(dict(passed=result['passed'], output=str(args.output))))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print('recovery runner error: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
