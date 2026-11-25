#!/usr/bin/env python3
"""Guest-only timed prefix RDMA diagnostic, AB then BA, not formal A/B.

A fresh host backing-identity proof and live thermal watchdog are mandatory
operator prerequisites. This runner does not create that host-side authority.
It never mounts/deletes data or switches production scoring/defaults.
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
import run_long_residence as residence
import run_sg_validation as common
import run_prefix_rdma as prefix

CLIENT_TIMEOUT_SEC = 1100
CAPACITY_BYTES = 12884901888


# Reuse the already-tested policy order, default environment and native oracle.
plan = prefix.plan
environment = prefix.environment
check_metrics = prefix.check_metrics


def _hashes(paths):
    return {path.name: baseline.file_sha(path) for path in paths}


def _tools():
    hashes = prefix._tools()
    names = ('run_prefix_delayed.py', 'prefix_delayed.py')
    hashes.update(_hashes([Path(__file__).with_name(name) for name in names]))
    return hashes


def run(build, roots, output, tag, *, pairs=2, start_pair=0):
    cases = plan(pairs, start_pair)
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('tag must be 1..40 ASCII letters/digits/_/-')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain comma separator')
    # Input checks precede client import and any guest/filesystem operation.
    import prefix_delayed as workload

    roots, output = common.guest_paths(roots, output, 'prefix-delay-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-prefix-delay-{tag}-{row['case_id']:02d}-{row['policy']}")
                            for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data exists; use a fresh tag')
    workload_plan = workload.generate()
    workload.validate(workload_plan)
    env = environment()
    frozen_build, frozen_tools = _hashes(binaries), _tools()
    output.mkdir()
    plan_path = output / 'plan.json'
    with plan_path.open('x') as stream:
        json.dump(workload_plan, stream, sort_keys=True, allow_nan=False)
        stream.write('\n')
    frozen_plan = baseline.file_sha(plan_path)
    result = dict(model='prefix-delayed-rdma-two-policy-v1', formal_ab=False,
                  gpu='excluded', inference_engine='excluded', ttft='excluded', passed=False, errors=[], cases=cases,
                  build_sha256=frozen_build, tools_sha256=frozen_tools,
                  plan_sha256=frozen_plan, plan_document_sha256=workload_plan['sha256'],
                  scope='timed-serial-prefix-delay-turnover-refill-proxy-not-engine-or-ttft',
                  host_safety='external-fresh-backing-proof-and-live-thermal-watchdog-required')

    def check_frozen():
        if (_hashes(binaries) != frozen_build or _tools() != frozen_tools or
                baseline.file_sha(plan_path) != frozen_plan):
            raise ValueError('build, frozen tools or plan changed')

    try:
        for row in cases:
            check_frozen()
            quick.check_ports()
            case = output / f"case-{row['case_id']:02d}-{row['policy']}"
            case.mkdir()
            for path in row['data_dirs']:
                Path(path).mkdir()
            command = [str(binaries[0]), '--dir', ','.join(row['data_dirs']),
                       '--cap', str(CAPACITY_BYTES), '--store-engine', 'slab',
                       '--slab-write', 'buffered', '--slab-eviction-policy', row['policy'],
                       '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0',
                       '--max-msg', '2097152', '--metrics-port', '12010',
                       '--metrics-bind', '127.0.0.1']
            child_command = [sys.executable, str(Path(__file__).with_name('prefix_delayed.py')), 'run',
                             '--lib', str(binaries[1]), '--members', 'n=127.0.0.1:12001',
                             '--plan', str(plan_path), '--output', str(case / 'client')]
            row.update(state='running', command=command, client_command=child_command,
                       client_timeout_sec=CLIENT_TIMEOUT_SEC, counts=None,
                       client_state='not_attempted', environment={key: value for key, value in env.items()
                                                                if key.startswith('DFKV_')})
            server = None
            try:
                with (case / 'server.log').open('x') as log:
                    server = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                    row['server_pid'] = server.pid
                    before = baseline._ready(server, 12010, case / 'server.log', row['policy'])
                    residence.check_initial(before, row['data_dirs'])
                    residence.check_startup((case / 'server.log').read_text(), 'default', row['policy'])
                    (case / 'before.prom').write_text(before)
                    check_frozen()
                    row['client_state'] = 'running'
                    try:
                        with (case / 'client.log').open('x') as log:
                            subprocess.run(child_command, env=env, stdin=subprocess.DEVNULL,
                                           stdout=log, stderr=subprocess.STDOUT,
                                           timeout=CLIENT_TIMEOUT_SEC, check=True)
                        row.update(client_state='exited', client_exit=0)
                    except subprocess.TimeoutExpired:
                        # subprocess.run kills/reaps its own child, including a
                        # native call hung below Python. Partial evidence stays.
                        row['client_state'] = 'process_timeout_reaped'
                        raise
                    except subprocess.CalledProcessError as error:
                        row.update(client_state='failed_exit_reaped', client_exit=error.returncode)
                        raise
                    check_frozen()
                    # Preserve the native scrape even if raw report validation
                    # rejects an accounting claim; never rewrite a failed case.
                    after = baseline._scrape(12010)
                    (case / 'after.prom').write_text(after)
                    report_path = case / 'client/report.json'
                    report = workload.load(report_path)
                    row['client_report_sha256'] = baseline.file_sha(report_path)
                    summary = workload.check(workload_plan, report)
                    if report.get('passed') is not True or report.get('transport') != 'rdma':
                        raise ValueError('client prefix report must pass on real RDMA transport')
                    row['counts'] = summary['native']
                    row['summary'] = summary
                    row['server_delta'] = check_metrics(before, after, row['policy'], summary,
                                                        row['data_dirs'])
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
    parser.add_argument('--pairs', type=int, default=2)
    parser.add_argument('--start-pair', type=int, default=0)
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag,
                     pairs=args.pairs, start_pair=args.start_pair)
        print(json.dumps(dict(passed=result['passed'], output=str(args.output))))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print('delayed prefix RDMA runner error: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
