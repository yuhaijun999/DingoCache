#!/usr/bin/env python3
"""Guest-only bounded prefix RDMA correctness, AB then BA, not formal A/B.

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

CLIENT_TIMEOUT_SEC = 300
CAPACITY_BYTES = 12884901888


def plan(pairs=2, start_pair=0):
    """Fixed fresh AB/BA pairs; splitting never renumbers case identities."""
    if (type(pairs) is not int or type(start_pair) is not int or
            not 0 <= start_pair <= 1 or not 1 <= pairs <= 2 - start_pair):
        raise ValueError('pairs/start-pair must select consecutive pairs within 0..1')
    return [dict(case_id=pair * 2 + slot, pair=pair, policy=policy, state='not_attempted')
            for pair in range(start_pair, start_pair + pairs)
            for slot, policy in enumerate(('legacy', 'lifetime') if pair == 0
                                          else ('lifetime', 'legacy'))]


def environment():
    """Discard inherited DFKV overrides; retain all default scoring values."""
    return residence.environment('default')


def _hashes(paths):
    return {path.name: baseline.file_sha(path) for path in paths}


def _tools():
    hashes = residence._tools()
    names = ('run_prefix_rdma.py', 'prefix_rdma.py', 'prefix_chain.py')
    hashes.update(_hashes([Path(__file__).with_name(name) for name in names]))
    return hashes


def check_metrics(before, after, policy, summary, dirs):
    """Reconcile ALL native calls, not just business-prefix hit counters."""
    counts = summary['native']
    fields = {'get_hits', 'get_misses', 'get_errors', 'get_unknown', 'get_attempts',
              'put_acks', 'put_bytes', 'put_errors', 'put_unknown', 'put_attempts'}
    if not isinstance(counts, dict) or set(counts) != fields:
        raise ValueError('incomplete native counter fields')
    if any(type(value) is not int or value < 0 for value in counts.values()):
        raise ValueError('native counters must be nonnegative integers')
    if any(counts[name] != 0 for name in ('get_errors', 'get_unknown', 'put_errors', 'put_unknown')):
        raise ValueError('native error/unknown is not a cache miss or successful case')
    if (counts['get_attempts'] != counts['get_hits'] + counts['get_misses'] or
            counts['put_attempts'] != counts['put_acks'] or not counts['put_acks'] or
            not counts['get_hits']):
        raise ValueError('incomplete native terminal denominator or no activity')
    return residence.check_metrics(before, after, policy, counts, dirs)



def check_forced_edges(report):
    """Prove first-gap/refill mechanics without assuming post-refill residency."""
    for name, first_boundary in (('head', 0), ('middle', 4), ('tail', 7)):
        for phase in ('first', 'revisit'):
            kind = f'forced-{name}-{phase}'
            rows = [row for row in report['requests'] if row['kind'] == kind]
            if len(rows) != 1:
                raise ValueError('missing or duplicate correctness request: ' + kind)
            row = rows[0]
            ids, boundary = row['object_ids'], row['first_unconfirmed_block']
            if (len(ids) != 8 or any(type(index) is not int or index < 0 for index in ids) or
                    type(boundary) is not int or not 0 <= boundary <= 8 or
                    (phase == 'first' and boundary != first_boundary)):
                raise ValueError('invalid first-gap/revisit boundary: ' + kind)
            # No pin/retention guarantee spans requests: suffix refill may evict
            # its own prefix. A revisit miss is therefore valid capacity loss,
            # but its prefix, original benefit and new refill must still agree.
            outcomes = (['hit'] * boundary + ['miss'] + ['not_attempted'] * (7 - boundary)
                        if boundary < 8 else ['hit'] * 8)
            # This frozen plan schema assigns token weight from the object ID.
            tokens = [(index % 4 + 1) * 8 for index in ids]
            usable = sum(tokens[:boundary])
            if (row['state'] != 'completed' or row['outcomes'] != outcomes or
                    row['refill_put_acks'] != 8 - boundary or
                    row['usable_tokens'] != usable or
                    row['confirmed_prefix_tokens'] != usable or row['target_tokens'] != sum(tokens)):
                raise ValueError('first-gap/refill/revisit correctness mismatch: ' + kind)


def run(build, roots, output, tag, *, pairs=2, start_pair=0):
    cases = plan(pairs, start_pair)
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('tag must be 1..40 ASCII letters/digits/_/-')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain comma separator')
    # Input checks precede client import and any guest/filesystem operation.
    import prefix_rdma as workload

    roots, output = common.guest_paths(roots, output, 'prefix-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-prefix-{tag}-{row['case_id']:02d}-{row['policy']}")
                            for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('case data exists; use a fresh tag')
    workload_plan = workload.generate()
    env = environment()
    frozen_build, frozen_tools = _hashes(binaries), _tools()
    output.mkdir()
    plan_path = output / 'plan.json'
    with plan_path.open('x') as stream:
        json.dump(workload_plan, stream, sort_keys=True, allow_nan=False)
        stream.write('\n')
    frozen_plan = baseline.file_sha(plan_path)
    result = dict(model='prefix-rdma-two-policy-correctness-v1', formal_ab=False,
                  gpu='excluded', passed=False, errors=[], cases=cases,
                  build_sha256=frozen_build, tools_sha256=frozen_tools,
                  plan_sha256=frozen_plan, plan_document_sha256=workload_plan['sha256'],
                  scope='bounded-serial-prefix-usability-proxy-not-engine-adoption-or-performance',
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
            child_command = [sys.executable, str(Path(__file__).with_name('prefix_rdma.py')), 'run',
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
                    report_path = case / 'client/report.json'
                    report = workload.load(report_path)
                    row['client_report_sha256'] = baseline.file_sha(report_path)
                    summary = workload.check(workload_plan, report)
                    if report.get('passed') is not True or report.get('transport') != 'rdma':
                        raise ValueError('client prefix report must pass on real RDMA transport')
                    check_forced_edges(report)
                    row['counts'] = summary['native']
                    row['summary'] = summary
                    after = baseline._scrape(12010)
                    (case / 'after.prom').write_text(after)
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
        print('prefix RDMA runner error: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
