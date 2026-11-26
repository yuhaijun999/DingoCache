#!/usr/bin/env python3
"""Guest-only active-client SIGKILL recovery; no kernel-inflight/power-loss claim.

Fresh backing and live host thermal supervision are external prerequisites.
Only exact Popen children and newly allocated guest directories are owned here.
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
import prefix_concurrent as concurrent
import run_prefix_recovery as recovery

mixed, baseline, common, quick = recovery.mixed, recovery.baseline, recovery.common, recovery.quick
workload = recovery.workload


def plan():
    rows = []
    for first in ('legacy', 'lifetime'):
        for transition in ('same', 'switch'):
            second = first if transition == 'same' else ('lifetime' if first == 'legacy' else 'legacy')
            rows.append(dict(case_id=len(rows), transition=transition, policies=[first, second],
                             state='not_attempted', stages=[]))
    return rows


def check_active_health(text, initial=None):
    """Validate live health without requiring drain or dispatched==terminal."""
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if mixed._rows(text, name) != {'': 1}:
            raise ValueError('unhealthy active storage: '+name)
    for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total'):
        if any(mixed._rows(text, name).values()):
            raise ValueError('active errors/fallback: '+name)
    paths = ('record_write', 'foreground_evict', 'background_evict', 'watermark_evict',
             'extent_bind', 'donor_bind', 'table_sync', 'bind_sync', 'donor_sync')
    if mixed._rows(text, 'dfkv_slab_metadata_ops_errors_total') != {
            '{path="'+path+'"}': 0 for path in paths}:
        raise ValueError('active metadata I/O error or missing path')
    name = 'dfkv_rdma_pull_requests_terminal_total'
    new = mixed._rows(text, name)
    old = mixed._rows(initial, name) if initial is not None else dict.fromkeys(new, 0)
    allowed = {'{path="uring",result="success_nonempty"}', '{path="uring",result="not_found"}'}
    if old.keys() != new.keys() or not allowed <= new.keys():
        raise ValueError('active terminal path identity changed/missing')
    for key in new:
        if (not new[key].is_integer() or new[key] < old[key] or
                (new[key] != old[key] and key not in allowed)):
            raise ValueError('active terminal error/fallback/regression')


def await_activity(server, client, directory, receipt, *, timeout=90):
    """Two progressing Prom observations plus ACK/read thresholds, bounded wall time."""
    if not 0 < timeout <= 90:
        raise ValueError('activity timeout must be 0..90 seconds')
    deadline = time.monotonic() + timeout
    previous, progress, initial = None, 0, None
    receipt['activity'] = []
    while time.monotonic() < deadline:
        if server.poll() is not None or client.poll() is not None:
            raise RuntimeError('owned server/client exited before active kill gate')
        text = baseline._scrape(12010)
        check_active_health(text, initial)
        if initial is None:
            initial = text
        path = Path(directory)/f"activity-{len(receipt['activity']):03d}.prom"
        path.write_text(text)
        puts = quick.total(text, 'dfkv_cache_put_total')
        gets = quick.total(text, 'dfkv_rdma_pull_requests_terminal_total')
        if any(not value.is_integer() or value < 0 for value in (puts, gets)):
            raise ValueError('invalid activity counters')
        if previous is not None:
            if puts < previous[0] or gets < previous[1]:
                raise ValueError('activity counters regressed')
            if puts > previous[0] and gets > previous[1]:
                progress += 1
            else:
                progress = 0
        receipt['activity'].append(dict(put_acks=puts, get_terminals=gets,
                                       prom_sha256=baseline.file_sha(path), path=path.name))
        previous = puts, gets
        if progress >= 2 and puts >= 12288+32 and gets >= 32+12+16:
            if server.poll() is not None or client.poll() is not None:
                raise RuntimeError('owned process exited at active gate')
            (Path(directory)/'before-kill.prom').write_text(text)
            receipt.update(active_gate=True, progressing_observations=progress)
            return
        time.sleep(min(.5, max(0, deadline-time.monotonic())))
    raise TimeoutError('no proven active client within bounded deadline')


def kill_active(server, client, receipt):
    receipt.update(scope='client-process-active-not-kernel-inflight', requested='SIGKILL',
                   client_exit_before_kill=client.poll(), server_exit_before_kill=server.poll())
    if receipt['client_exit_before_kill'] is not None or receipt['server_exit_before_kill'] is not None:
        raise RuntimeError('active kill requires both owned processes alive')
    server.kill()
    try:
        if server.wait(timeout=3) != -9:
            raise RuntimeError('owned SIGKILL did not exit -9')
    finally:
        receipt.update(server_exit=server.poll(), server_reaped=server.poll() is not None,
                       client_exit_after_kill=client.poll())


def finish_client(client, receipt, *, timeout=30):
    if not 0 < timeout <= 30:
        raise ValueError('client timeout must be 0..30 seconds')
    receipt.update(pid=client.pid, timed_out=False, exit_before_cleanup=None, reaped=False)
    try:
        code = client.wait(timeout=timeout)
        receipt.update(exit_before_cleanup=code, outcome='nonzero' if code else 'zero')
    except subprocess.TimeoutExpired:
        receipt.update(timed_out=True, outcome='timeout-unknown', exit_before_cleanup=client.poll())
        client.kill()
        client.wait(timeout=3)
    finally:
        receipt.update(exit=client.poll(), reaped=client.poll() is not None)


def inspect_client(pressure_plan, path, receipt):
    """Absence is unknown, never a cache miss; malformed evidence fails closed."""
    receipt['report_present'] = Path(path).is_file()
    if receipt['report_present']:
        receipt['report_sha256'] = baseline.file_sha(path)
        report = concurrent.load(path)
        receipt['summary'] = concurrent.check(pressure_plan, report)
        if report.get('transport') != 'rdma':
            raise ValueError('active client must use real RDMA')
        receipt['report_passed'] = report['passed']
        if receipt['outcome'] == 'zero' and report['passed']:
            raise ValueError('active client unexpectedly completed successfully after kill')
    elif receipt['outcome'] == 'zero':
        raise ValueError('successful client missing its report')
    receipt['missing_report_means'] = 'unknown-not-miss'


def run(build, roots, output, tag):
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('invalid tag')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data root contains comma')
    roots, output = common.guest_paths(roots, output, 'prefix-recovery-active-'+tag)
    build = Path(build).resolve(strict=True)
    binaries = [build/name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('missing build binaries')
    cases = plan()
    for row in cases:
        row['data_dirs'] = [str(root/f"sg-prefix-recovery-active-{tag}-{row['case_id']:02d}") for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data exists; use fresh tag')
    manifest = workload.generate()
    pressure_plans = {row['case_id']: concurrent.generate(
        seed=concurrent.DEFAULTS['seed']+row['case_id'], workers=4, rounds=600,
        prefill=12288, interval_ns=100000000, arrival_batch=8) for row in cases}
    tools = [Path(__file__), Path(concurrent.__file__), Path(recovery.__file__)]
    def hashes():
        return dict(recovery._tools(), **mixed._hashes(tools))
    frozen_build, frozen_tools = mixed._hashes(binaries), hashes()
    output.mkdir()
    manifest_path = output/'manifest.json'
    workload._write(manifest_path, manifest)
    paths = {}
    for index, pressure_plan in pressure_plans.items():
        paths[index] = output/f'pressure-{index:02d}.json'
        concurrent._write(paths[index], pressure_plan)
    files = [manifest_path]+list(paths.values())
    frozen_files = {path: baseline.file_sha(path) for path in files}
    def frozen():
        if (mixed._hashes(binaries) != frozen_build or hashes() != frozen_tools or
                any(baseline.file_sha(path) != digest for path, digest in frozen_files.items())):
            raise ValueError('build/tools/manifests changed')
    result = dict(model='prefix-recovery-active-rdma-v1', passed=False, cases=cases, errors=[],
                  formal_ab=False, scope='guest-only-client-active-not-kernel-inflight-or-power-loss',
                  host_safety='external-fresh-backing-proof-and-live-thermal-watchdog-required',
                  environment={key: value for key, value in mixed.environment(recovery.ROUTE).items() if key.startswith('DFKV_')},
                  build_sha256=frozen_build, tools_sha256=frozen_tools,
                  frozen_files_sha256={path.name: digest for path, digest in frozen_files.items()})
    env = mixed.environment(recovery.ROUTE)
    try:
        for row in cases:
            case = output/f"case-{row['case_id']:02d}"
            case.mkdir()
            for directory in row['data_dirs']:
                Path(directory).mkdir()
            row['state'] = 'running'
            expected = None
            for index, policy in enumerate(row['policies']):
                frozen()
                quick.check_ports()
                stage = case/f'start-{index}-{policy}'
                stage.mkdir()
                command = [str(binaries[0]), '--dir', ','.join(row['data_dirs']), '--cap', str(mixed.CAPACITY_BYTES),
                           '--store-engine', 'slab', '--slab-write', 'buffered', '--slab-eviction-policy', policy,
                           '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0', '--max-msg', '2097152',
                           '--metrics-port', '12010', '--metrics-bind', '127.0.0.1']
                entry = dict(policy=policy, state='running', command=command, clients=[], stop={})
                row['stages'].append(entry)
                server, active = None, None
                with (stage/'server.log').open('x') as log, (stage/'pressure.log').open('x') as pressure_log:
                    try:
                        server = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
                        entry['server_pid'] = server.pid
                        before = baseline._ready(server, 12010, stage/'server.log', policy)
                        (stage/'startup.prom').write_text(before)
                        mixed.check_startup((stage/'server.log').read_text(), policy, recovery.ROUTE)
                        if index == 0:
                            mixed.check_initial(before, row['data_dirs'], recovery.ROUTE)
                        recovery.check_restart(before, row['data_dirs'], dirty=index == 1, policy=policy)
                        sampler = mixed.Sampler(stage, server.pid)
                        def client(mode, expected_path=None, label=None):
                            label = label or mode
                            frozen()
                            cmd = [sys.executable, str(Path(workload.__file__)), '--lib', str(binaries[1]),
                                   '--members', 'n=127.0.0.1:12001', '--manifest', str(manifest_path),
                                   '--output', str(stage/label), '--mode', mode]
                            if expected_path is not None:
                                cmd += ['--expected', str(expected_path), '--expect', 'empty']
                            receipt = dict(mode=mode, label=label, command=cmd, process={})
                            entry['clients'].append(receipt)
                            old = baseline._scrape(12010)
                            mixed.run_client(cmd, env, stage/(label+'.log'), sampler,
                                             timeout=recovery.CLIENT_TIMEOUT_SEC, receipt=receipt['process'])
                            report_path = stage/label/'report.json'
                            report = workload.load(report_path)
                            summary = workload.check(manifest, report, workload.load(expected_path) if expected_path else None,
                                                     'empty' if expected_path else None)
                            if not report['passed'] or report['transport'] != 'rdma':
                                raise ValueError('recovery client must pass real RDMA')
                            receipt.update(report_sha256=baseline.file_sha(report_path), summary=summary)
                            new = baseline._scrape(12010)
                            (stage/(label+'-before.prom')).write_text(old)
                            (stage/(label+'-after.prom')).write_text(new)
                            native = summary['native']
                            counts = dict(put_acks=native['put_ack'], put_bytes=summary['put_bytes'],
                                          put_attempts=sum(native[k] for k in ('put_ack','put_error','put_unknown')),
                                          put_errors=native['put_error'], put_unknown=native['put_unknown'],
                                          get_hits=native['get_hit'], get_misses=native['get_miss'],
                                          get_attempts=sum(native[k] for k in ('get_hit','get_miss','get_error','get_unknown')),
                                          get_errors=native['get_error'], get_unknown=native['get_unknown'])
                            receipt['server_delta'] = recovery.check_client_metrics(old, new, policy, counts)
                            frozen()
                            return report_path
                        if index == 0:
                            client('seed')
                            # Cold class binding may donate a resident small slot.
                            # One bounded refill repairs that first-bind loss.
                            client('refill', label='stabilize')
                            expected = client('snapshot')
                            recovery.check_snapshot(workload.load(expected))
                            cmd = [sys.executable, str(Path(concurrent.__file__)), 'run', '--lib', str(binaries[1]),
                                   '--members', 'n=127.0.0.1:12001', '--plan', str(paths[row['case_id']]),
                                   '--output', str(stage/'pressure')]
                            entry['pressure_command'] = cmd
                            active = subprocess.Popen(cmd, env=env, stdin=subprocess.DEVNULL, stdout=pressure_log, stderr=subprocess.STDOUT)
                            entry['pressure_pid'] = active.pid
                            await_activity(server, active, stage, entry['stop'])
                            frozen()
                            kill_active(server, active, entry['stop'])
                            entry['pressure_receipt'] = {}
                            finish_client(active, entry['pressure_receipt'])
                            inspect_client(pressure_plans[row['case_id']], stage/'pressure/report.json', entry['pressure_receipt'])
                        else:
                            client('verify', expected)
                            client('refill')
                            client('refill', label='stabilize')
                            recovery.check_snapshot(workload.load(client('snapshot')))
                            recovery.stop_server(server, 'clean', entry['stop'])
                        frozen()
                        entry['state'] = 'passed'
                    finally:
                        try:
                            if active is not None and not entry.get('pressure_receipt', {}).get('reaped', False):
                                finish_client(active, entry.setdefault('cleanup_client', {}), timeout=1)
                        finally:
                            if server is not None:
                                try:
                                    if server.poll() is None:
                                        common.stop_owned(server)
                                finally:
                                    entry.update(server_exit=server.poll(), server_reaped=server.poll() is not None)
                quick.check_ports()
            row['state'] = 'passed'
        frozen()
        result['passed'] = True
    except BaseException as error:
        if 'row' in locals() and row['state'] == 'running':
            row.update(state='failed', error=f'{type(error).__name__}: {error}')
        if 'entry' in locals() and entry['state'] == 'running':
            entry['state'] = 'failed'
        result['errors'].append(f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            raise
    finally:
        (output/'matrix.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--data-root', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tag', required=True)
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag)
        print(json.dumps(dict(passed=result['passed'], output=str(args.output))))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print('active recovery runner error: '+str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
