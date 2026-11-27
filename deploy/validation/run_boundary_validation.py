#!/usr/bin/env python3
"""Guest-only RDMA parameter/resource diagnostics, not formal A/B.

All cases use B=64/G=400000 for valid M63 coverage. Other scoring inputs remain
at defaults, including BOOST derived from M; this is not an isolated M effect.
Fresh host backing proof and a live thermal watchdog remain prerequisites.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import boundary_resources as resources
import prefix_delayed as workload
import run_prefix_mixed as mixed

baseline, quick, common = mixed.baseline, mixed.quick, mixed.common
CAPACITY_BYTES = mixed.CAPACITY_BYTES
BUCKET_SEC = 400000
BUCKET_COUNT = 64
PRESETS = {'smoke': (8, [1, 2, 3]), 'long': (1800, [45, 185, 600])}


def plan(routes=mixed.ROUTES, max_reads=(1, 2, 4, 63), include_legacy=True):
    if (not isinstance(routes, (tuple, list)) or not routes or
            len(set(routes)) != len(routes) or any(route not in mixed.ROUTES for route in routes) or
            not isinstance(max_reads, (tuple, list)) or not max_reads or
            any(type(value) is not int or not 1 <= value <= 63 for value in max_reads) or
            len(set(max_reads)) != len(max_reads) or type(include_legacy) is not bool):
        raise ValueError('invalid route/MAX_READS matrix')
    configs = ([('legacy', None)] if include_legacy else []) + [('lifetime', m) for m in max_reads]
    return [dict(case_id=index, route=route, policy=policy, max_reads=m,
                 configured_bands=m + 1 if m is not None else None,
                 band_count_scope='configuration-derived-not-measured-allocation', state='not_attempted')
            for index, (route, policy, m) in enumerate(
                (route, policy, m) for route in routes for policy, m in configs)]


def environment(route, policy, max_reads):
    if policy not in ('legacy', 'lifetime') or (policy == 'legacy' and max_reads is not None):
        raise ValueError('invalid policy/MAX_READS combination')
    if policy == 'lifetime' and (type(max_reads) is not int or not 1 <= max_reads <= 63):
        raise ValueError('MAX_READS must be 1..63')
    env = mixed.environment(route)
    env.update(DFKV_SLAB_BUCKET_SEC=str(BUCKET_SEC), DFKV_SLAB_BUCKET_COUNT=str(BUCKET_COUNT))
    if policy == 'lifetime':
        env['DFKV_SLAB_LIFE_MAX_READS'] = str(max_reads)
    return env


def check_startup(text, policy, route, max_reads):
    environment(route, policy, max_reads)
    overrides = {'DFKV_SLAB_LIFE_MAX_READS': (str(max_reads), '2'),
                 'DFKV_SLAB_BUCKET_SEC': (str(BUCKET_SEC), '43200'),
                 'DFKV_SLAB_BUCKET_COUNT': (str(BUCKET_COUNT), '24'),
                 'DFKV_SLAB_PROTECT_BOOST_SEC': (str(604800 + 302400 * max_reads)
                                                  if max_reads is not None else 'inactive', '1209600')}
    # First prove every actual override exactly once; only then canonicalize
    # these fields to reuse the existing oracle for all other defaults/routes.
    for key, (selected, canonical) in overrides.items():
        wanted = selected if policy == 'lifetime' else 'inactive'
        pattern = r'(\b' + re.escape(key) + r'\s*=\s*)(\S+)(\s+\([^\n]*\))'
        matches = list(re.finditer(pattern, text))
        if len(matches) != 1 or matches[0].group(2) != wanted:
            raise ValueError('effective boundary startup mismatch: ' + key)
        if policy == 'lifetime':
            text = re.sub(pattern, lambda match: match.group(1) + canonical + match.group(3), text)
    mixed.check_startup(text, policy, route)


class Sampler:
    """Owned server metrics plus identity-pinned server/client /proc samples."""
    def __init__(self, directory, pid, receipt):
        self.directory = Path(directory)
        self.metrics = mixed.Sampler(directory, pid)
        self.server = resources.ProcessSampler(pid, 'server')
        self.client = None
        self.receipt = receipt
        self.rows = []

    def sample(self):
        self.metrics.sample()
        self.rows.append(self.server.sample())
        # mixed.run_client calls once after wait/reap as well. Never pretend
        # an exited client has a zero resource footprint or sample a reused PID.
        if self.receipt.get('state') == 'running':
            if self.client is None:
                self.client = resources.ProcessSampler(self.receipt['pid'], 'client')
            self.rows.append(self.client.sample())
        (self.directory / 'resource-samples.json').write_text(json.dumps(self.rows, indent=2) + '\n')


def _tools():
    paths = [Path(__file__), Path(__file__).with_name('boundary_resources.py'),
             Path(__file__).with_name('run_prefix_mixed.py')]
    return dict(mixed._tools(), **mixed._hashes(paths))


def run(build, roots, output, tag, *, preset='smoke', steps=None,
        routes=mixed.ROUTES, max_reads=(1, 2, 4, 63), include_legacy=True):
    cases = plan(routes, max_reads, include_legacy)
    if preset not in PRESETS:
        raise ValueError('unknown preset')
    default_steps, delays = PRESETS[preset]
    steps = default_steps if steps is None else steps
    if type(steps) is not int or not 1 <= steps <= 1800:
        raise ValueError('steps must be 1..1800')
    if not isinstance(tag, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,40}', tag) is None:
        raise ValueError('invalid tag')
    if any(',' in str(Path(root).resolve()) for root in roots):
        raise ValueError('data roots must not contain comma')
    document = workload.generate(sizes=[4096, 1048577], steps=steps, arrival_batch=64,
                                 due_first=True, delays_sec=list(delays))
    workload.validate(document)
    roots, output = common.guest_paths(roots, output, 'boundary-' + tag)
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    for row in cases:
        row['data_dirs'] = [str(root / f"sg-boundary-{tag}-{row['case_id']:02d}") for root in roots]
        if any(os.path.lexists(path) for path in row['data_dirs']):
            raise ValueError('existing case data; use fresh tag')
        row['environment'] = {key: value for key, value in environment(
            row['route'], row['policy'], row['max_reads']).items() if key.startswith('DFKV_')}
    frozen_build, frozen_tools = mixed._hashes(binaries), _tools()
    output.mkdir()
    plan_path = output / 'plan.json'
    plan_path.write_text(json.dumps(document, sort_keys=True, allow_nan=False) + '\n')
    frozen_plan = baseline.file_sha(plan_path)
    result = dict(model='boundary-rdma-v1', formal_ab=False, passed=False, errors=[], cases=cases,
                  preset=preset, steps=steps, delays_sec=delays, bucket_sec=BUCKET_SEC,
                  bucket_count=BUCKET_COUNT, build_sha256=frozen_build, tools_sha256=frozen_tools,
                  plan_sha256=frozen_plan, plan_document_sha256=document['sha256'],
                  scope='mixed-size-serial-prefix-config-boundary-not-isolated-M-benefit-or-full-resource-account',
                  gpu='excluded', inference_engine='excluded', ttft='excluded',
                  host_safety='external-fresh-backing-proof-and-live-thermal-watchdog-required')
    # Freeze the entire pending matrix before the first native process.
    matrix_path = output / 'frozen-matrix.json'
    matrix_path.write_text(json.dumps(result, indent=2) + '\n')
    frozen_matrix = baseline.file_sha(matrix_path)
    result['frozen_matrix_sha256'] = frozen_matrix

    def check_frozen():
        if (mixed._hashes(binaries) != frozen_build or _tools() != frozen_tools or
                baseline.file_sha(plan_path) != frozen_plan or baseline.file_sha(matrix_path) != frozen_matrix):
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
            client = [sys.executable, str(Path(__file__).with_name('prefix_delayed.py')), 'run',
                      '--lib', str(binaries[1]), '--members', 'n=127.0.0.1:12001',
                      '--plan', str(plan_path), '--output', str(case / 'client')]
            env = environment(row['route'], row['policy'], row['max_reads'])
            timeout = steps + 300
            row.update(state='running', command=command, client_command=client,
                       client_timeout_sec=timeout, client_process={})
            server = None
            try:
                with (case / 'server.log').open('x') as log:
                    server = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                              stdout=log, stderr=subprocess.STDOUT)
                    row['server_pid'] = server.pid
                    before = baseline._ready(server, 12010, case / 'server.log', row['policy'])
                    (case / 'before.prom').write_text(before)
                    mixed.check_initial(before, row['data_dirs'], row['route'])
                    check_startup((case / 'server.log').read_text(), row['policy'], row['route'], row['max_reads'])
                    check_frozen()
                    sampler = Sampler(case, server.pid, row['client_process'])
                    mixed.run_client(client, env, case / 'client.log', sampler,
                                     timeout=timeout, receipt=row['client_process'])
                    check_frozen()
                    after = baseline._scrape(12010)
                    (case / 'after.prom').write_text(after)
                    report_path = case / 'client/report.json'
                    report = workload.load(report_path)
                    summary = workload.check(document, report)
                    if report.get('passed') is not True or report.get('transport') != 'rdma':
                        raise ValueError('passed real RDMA report required')
                    delta = mixed.check_metrics(before, after, row['policy'], summary, row['data_dirs'],
                                                row['route'], mixed.put_classes(workload, document, report),
                                                distinct_classes=mixed.put_classes(workload, document, report, distinct=True))
                    resource_summary = resources.summarize(sampler.rows)
                    if any(not resource_summary['roles'][role] for role in ('server', 'client')):
                        raise ValueError('both owned server and client resource observations required')
                    row.update(summary=summary, server_delta=delta, state='passed',
                               client_report_sha256=baseline.file_sha(report_path),
                               resource_summary=resource_summary, sample_count=len(sampler.metrics.rows))
            except BaseException as error:
                row.update(state='failed', error=f'{type(error).__name__}: {error}')
                try:
                    (case / 'failure.prom').write_text(baseline._scrape(12010))
                except Exception as scrape_error:
                    row['failure_scrape_error'] = str(scrape_error)
                raise
            finally:
                if server is not None:
                    try:
                        # Already-exited 0 must not be credited as our clean stop.
                        if server.poll() is not None:
                            raise RuntimeError('owned server exited before requested stop')
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
    parser.add_argument('--preset', choices=PRESETS, default='smoke')
    parser.add_argument('--steps', type=int)
    parser.add_argument('--route', choices=mixed.ROUTES, action='append')
    parser.add_argument('--max-reads', type=int, nargs='+', default=[1, 2, 4, 63])
    parser.add_argument('--no-legacy', action='store_true')
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.data_root, args.output, args.tag, preset=args.preset,
                     steps=args.steps, routes=args.route or mixed.ROUTES,
                     max_reads=args.max_reads, include_legacy=not args.no_legacy)
        print(json.dumps(dict(passed=result['passed'], output=str(args.output))))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError) as error:
        print('boundary runner error: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
