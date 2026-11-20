#!/usr/bin/env python3
"""Guest-only two-policy CPU rendezvous correctness matrix; not formal A/B.

Requires independent host backing/thermal proof. No mounts, Docker, arbitrary
commands or data deletion; stop only owned server/child Popen objects.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
import urllib.request
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cpu_dedup_boundary as boundary
import cpu_dedup_validation as probe
import eviction_dedup as dedup
import eviction_smoke as smoke
import eviction_trace as trace
from eviction_guard import _stop
import quick_eviction as quick
import run_sg_validation as common


def check_metrics(before, after, policy, *, cross_process=False):
    # Frozen independently: native3=7pull(5grant2NF), probe=4(3grant1NF),
    # trace=8grant, boundarywarm=1grant. Local READY/cold-after-stop aren't I/O.
    expected = {'dfkv_cache_put_total': 19, 'dfkv_bytes_written_total': 746,
                'dfkv_rdma_pull_requests_dispatched_total': 20,
                'dfkv_rdma_pull_requests_terminal_total': 20,
                'dfkv_uring_reads_total': 17, 'dfkv_uring_completions_total': 17,
                'dfkv_slab_lifetime_read_success_total': 17 if policy == 'lifetime' else 0}
    if cross_process:
        # Extra process suite: 6 PUT/384B, 7 actual grants, no not-found.
        expected.update(dfkv_cache_put_total=25, dfkv_bytes_written_total=1130,
                        dfkv_rdma_pull_requests_dispatched_total=27,
                        dfkv_rdma_pull_requests_terminal_total=27,
                        dfkv_uring_reads_total=24, dfkv_uring_completions_total=24,
                        dfkv_slab_lifetime_read_success_total=24 if policy == 'lifetime' else 0)
    if policy not in ('legacy', 'lifetime'):
        raise ValueError('unknown policy')
    result = {}
    for name, wanted in expected.items():
        result[name] = quick.total(after, name) - quick.total(before, name)
        if result[name] != wanted:
            raise ValueError(f'{name}: expected {wanted}, got {result[name]}')
    old = dict(quick.samples(before, 'dfkv_rdma_pull_requests_terminal_total'))
    for labels, value in quick.samples(after, 'dfkv_rdma_pull_requests_terminal_total'):
        wanted = ((24 if cross_process else 17) if 'path="uring"' in labels and 'result="success_nonempty"' in labels
                  else 3 if 'path="uring"' in labels and 'result="not_found"' in labels else 0)
        if value - old.get(labels, 0) != wanted:
            raise ValueError('wrong PULL terminal path/result: ' + labels)
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if quick.total(after, name) != 1:
            raise ValueError('unhealthy storage: ' + name)
    for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total',
                 'dfkv_uring_inflight', 'dfkv_evictions_total'):
        if quick.total(after, name) != 0:
            raise ValueError('unexpected error/fallback/inflight/eviction: ' + name)
    return result


def _native_properties(path, expected, classname):
    cases = ET.parse(path).getroot().findall('.//testcase')
    # Case names and values are frozen below, never supplied by XML.
    result = {}
    if len(cases) != len(expected) or {case.get('name') for case in cases} != set(expected):
        raise ValueError('native suite membership changed')
    for case in cases:
        if (case.get('classname') != classname or case.get('status') != 'run'
                or case.get('result') != 'completed'):
            raise ValueError('native execution identity invalid')
        if case.find('failure') is not None or case.find('error') is not None or case.find('skipped') is not None:
            raise ValueError('native case did not pass')
        items = case.findall('./properties/property')
        properties = {item.get('name'): item.get('value') for item in items}
        if len(properties) != len(items) or properties.get('shm_cleanup') != 'verified_dev_inode_unlinked':
            raise ValueError('native properties duplicated or shm cleanup unverified')
        if properties.get('inner_transport') != 'rdma':
            raise ValueError('native tests did not use RDMA')
        for name, value in expected[case.get('name')].items():
            if properties.get(name) != str(value):
                raise ValueError('native oracle property mismatch')
        result[case.get('name')] = properties
    return result


def check_native(path):
    return len(_native_properties(path, NATIVE_CASES, 'CpuDedupRuntime'))


def check_process_native(path, parent_pid):
    cases = _native_properties(path, PROCESS_CASES, 'CpuDedupProcessRuntime')
    for name, properties in cases.items():
        fields = ['parent_pid', 'owner_pid', 'waiter_pid']
        if PROCESS_CASES[name]['killed_owner_count']:
            fields.append('recovery_pid')
        elif 'recovery_pid' in properties:
            raise ValueError('unexpected recovery child')
        pids = []
        for field in fields:
            value = properties.get(field, '')
            if (not value.isascii() or not value.isdecimal() or not 0 < int(value) <= 2147483647
                    or str(int(value)) != value):
                raise ValueError('invalid native process identity')
            pids.append(int(value))
        if pids[0] != parent_pid or len(set(pids)) != len(pids):
            raise ValueError('native clients are not distinct owned exec processes')
    return len(cases)


def run_process_native(binary, case, env):
    report = {'passed': False, 'reaped': False, 'timeout_sec': 45, 'logical_counts': None,
              'command': [str(binary), '--gtest_output=xml:' + str(case / 'process-native.xml')]}
    child = None
    started = time.monotonic()
    try:
        with (case / 'process-native.log').open('x') as log:
            child = subprocess.Popen(report['command'], env=env, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT)
            report['pid'] = child.pid
            child.wait(timeout=45)
            if child.returncode != 0:
                raise RuntimeError('process native suite failed: ' + str(child.returncode))
            report['cases'] = check_process_native(case / 'process-native.xml', child.pid)
            # Frozen logical denominator, never inferred from an incomplete run.
            report['logical_counts'] = {
                'put_ack': 6, 'put_bytes': 384, 'get_attempted': 11, 'get_completed': 10,
                'get_hit': 9, 'get_hit_bytes': 576, 'get_error': 1, 'get_inflight_killed': 1}
            report['passed'] = True
    finally:
        if child is not None:
            if child.poll() is None:
                report['cleanup_actions'] = _stop(child)
            report.update(returncode=child.poll(), reaped=child.returncode is not None)
        report['elapsed_sec'] = time.monotonic() - started
        (case / 'process-native-report.json').write_bytes(trace.encode(report))
    return report


def boundary_stop(library, members, output, tag, env, server, before, policy, case, *, cross_process=False):
    output.mkdir()
    token = uuid.uuid4().hex
    original = b'cpu-stop-boundary\0' + tag.encode('ascii')
    document = {'requests': [{'namespace_hex': original.hex()}]}
    dedup.require_fresh(document, token)
    client_environment = dict(smoke.CLIENT_ENV, **dedup.ENV)
    client_environment['DFKV_NODE_DEDUP_TTL_MS'] = '30000'
    cfg = {'lib': str(library), 'lib_sha256': hashlib.sha256(library.read_bytes()).hexdigest(),
           'members': members, 'token': token, 'namespace_hex': original.hex(),
           'client_environment': client_environment}
    (output / 'config.json').write_bytes(trace.encode(cfg))
    result = {'passed': False, 'reaped': False,
              'possible_retained_shm_paths': [item['possible_shm_path'] for item in dedup.identities(document, token)]}
    child = None
    started = time.monotonic()
    try:
        child_env = {k: v for k, v in env.items() if not k.startswith('DFKV_')}
        child_env.update(client_environment)
        with (output / 'stdout.log').open('x') as stdout, (output / 'stderr.log').open('x') as stderr:
            result['command'] = [sys.executable, '-I', str(Path(boundary.__file__).resolve()), str(output)]
            child = subprocess.Popen(result['command'], env=child_env, stdin=subprocess.DEVNULL,
                                     stdout=stdout, stderr=stderr)
            result['child_pid'] = child.pid
            ready = output / 'ready.json'
            while not ready.is_file():
                if child.poll() is not None:
                    raise RuntimeError('boundary child exited before warm readiness')
                if time.monotonic() - started >= 10:
                    raise TimeoutError('boundary warm startup deadline')
                time.sleep(.02)
            wanted = {'config': cfg, 'warm': {'outcome': 'get_hit', 'status': 0},
                      'before': dict.fromkeys(dedup.COUNTERS, 0)}
            if trace.encode(trace.load(ready)) != trace.encode(wanted):
                raise ValueError('wrong boundary readiness proof')
            after = common.scrape()
            (case / 'before-stop.prom').write_text(after)
            result['server_delta'] = check_metrics(before, after, policy, cross_process=cross_process)
            common.stop_owned(server)
            result['server_exit'] = server.returncode
            (output / 'stop-ack.json').write_bytes(trace.encode({'config': cfg, 'owned_server_reaped': True}))
            child.wait(timeout=max(.01, 30 - (time.monotonic() - started)))
            if child.returncode != 0 or time.monotonic() - started >= 30:
                raise RuntimeError('boundary child failed or exceeded process budget')
            raw = trace.load(output / 'result.json')
            boundary.verify(raw, cfg)
            result.update(passed=True, result=raw)
    finally:
        if child is not None:
            if child.poll() is None:
                result['cleanup_actions'] = _stop(child)
            result.update(returncode=child.poll(), reaped=child.returncode is not None)
        result['elapsed_sec'] = time.monotonic() - started
        (output / 'report.json').write_bytes(trace.encode(result))
    return result


def run(build, roots, output, tag, *, cross_process=False):
    if not isinstance(tag, str) or len(tag) > 55:
        raise ValueError("matrix tag must fit 55 characters before policy suffix")
    roots, output = common.guest_paths(roots, output, tag, case_prefix='cpu')
    build = Path(build).resolve(strict=True)
    binaries = [build / name for name in ('dfkv_server', 'libdfkv.so', 'cpu_dedup_runtime_test')]
    if cross_process:
        binaries.append(build / 'cpu_dedup_process_runtime_test')
    if any(not path.is_file() for path in binaries):
        raise ValueError('requires server, shared library and native runtime test')
    output.mkdir()
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in binaries}
    result = {'model': 'cpu-dedup-two-policy-correctness-v1', 'formal_ab': False,
              'passed': False, 'build_sha256': hashes, 'policies': [], 'errors': []}
    if cross_process:
        result['cross_process'] = True
    env = {k: v for k, v in os.environ.items() if not k.startswith('DFKV_')}
    env.update(smoke.CLIENT_ENV, DFKV_RAM_TIER='0', DFKV_PUT_ACK_MODE='disk',
               DFKV_SLAB_RECLAIM_MS='50', DFKV_SERVER_URING='1')
    try:
        for policy in ('legacy', 'lifetime'):
            quick.check_ports()
            case = output / policy
            case.mkdir()
            dirs = [root / f'cpu-{tag}-{policy}' for root in roots]
            for path in dirs:
                path.mkdir()
            command = [str(binaries[0]), '--dir', ','.join(map(str, dirs)), '--cap', '12884901888',
                       '--store-engine', 'slab', '--slab-write', 'buffered', '--slab-eviction-policy', policy,
                       '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0', '--max-msg', '2097152',
                       '--metrics-port', '12010', '--metrics-bind', '127.0.0.1']
            row = {'policy': policy, 'command': command, 'data_dirs': list(map(str, dirs)),
                   'server_environment': {k: v for k, v in env.items() if k.startswith('DFKV_')}}
            result['policies'].append(row)
            server = None
            try:
                with (case / 'server.log').open('x') as log:
                    server = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                    row['server_pid'] = server.pid
                    deadline = time.monotonic() + 30
                    while True:
                        if server.poll() is not None:
                            raise RuntimeError('server exited before ready')
                        try:
                            with urllib.request.urlopen('http://127.0.0.1:12010/readyz', timeout=1) as response:
                                if response.status == 200:
                                    break
                        except OSError:
                            pass
                        if time.monotonic() >= deadline:
                            raise TimeoutError('server readiness deadline')
                        time.sleep(.1)
                    before = common.scrape()
                    objects = quick.samples(before, 'dfkv_disk_objects')
                    if len(objects) != 3 or any(value != 0 for _, value in objects):
                        raise ValueError('fresh three-disk initial state required')
                    if any(not any('disk=' + json.dumps(str(path)) in labels for labels, _ in objects) for path in dirs):
                        raise ValueError('disk identity mismatch')
                    (case / 'before.prom').write_text(before)
                    native_env = dict(env, DFKV_CPU_DEDUP_RUNTIME_MEMBERS='n=127.0.0.1:12001',
                                      DFKV_CPU_DEDUP_RUNTIME_NAMESPACE=tag + '-' + policy)
                    with (case / 'native.log').open('x') as native_log:
                        subprocess.run([str(binaries[2]), '--gtest_output=xml:' + str(case / 'native.xml')],
                                       env=native_env, stdout=native_log, stderr=subprocess.STDOUT, check=True, timeout=30)
                    row['native_cases'] = check_native(case / 'native.xml')
                    if cross_process:
                        row['process_native'] = run_process_native(binaries[3], case, native_env)
                    row['live'] = probe.supervise(case / 'live', lib=str(binaries[1]), members='n=127.0.0.1:12001',
                                                   tag=tag + '-' + policy, mode='live', process_timeout_sec=20)
                    if not row['live']['passed']:
                        raise ValueError('live CPU probe failed')
                    source = case / 'trace.json'
                    source.write_bytes(trace.encode(trace.generate(objects=8, rounds=2, length=17)))
                    row['trace'] = smoke.supervise(source, case / 'trace-report', lib=str(binaries[1]),
                                                   members='n=127.0.0.1:12001', batch_gets=True, cpu_dedup=True,
                                                   process_timeout_sec=20)
                    if not row['trace']['passed']:
                        raise ValueError('full CPU C/Python/report chain failed')
                    row['boundary'] = boundary_stop(binaries[1], 'n=127.0.0.1:12001', case / 'boundary',
                                                      tag + '-' + policy, env, server, before, policy, case,
                                                      cross_process=cross_process)
                    row['server_exit'] = server.returncode
                    if not row['boundary']['passed'] or not row['boundary']['reaped']:
                        raise ValueError('CPU TTL/stopped boundary failed')
                    row['stopped'] = probe.supervise(case / 'stopped', lib=str(binaries[1]), members='n=127.0.0.1:12001',
                                                      tag=tag + '-' + policy, mode='stopped', process_timeout_sec=20)
                    if not row['stopped']['passed']:
                        raise ValueError('stopped CPU probe failed')
            finally:
                if server is not None:
                    common.stop_owned(server)
                    row['server_exit'] = server.returncode
            quick.check_ports()
        if hashes != {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in binaries}:
            raise ValueError('build changed during matrix')
        result['passed'] = True
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            raise
    finally:
        (output / 'matrix.json').write_bytes(trace.encode(result))
    return result


# Frozen case names/properties; filled from the reviewed source, not test output.
NATIVE_CASES = {
    'InFlightSuccessSharesBytesWithoutSecondRemoteRead': {
        'expected_server_puts': 2, 'expected_server_put_bytes': 128,
        'expected_server_pulls': 2, 'expected_server_grants': 2, 'expected_server_not_found': 0,
        'injected_local_owner_errors': 0},
    'InjectedOwnerLocalErrorUsesWaitersOwnRemoteSuccess': {
        'expected_server_puts': 2, 'expected_server_put_bytes': 128,
        'expected_server_pulls': 2, 'expected_server_grants': 2, 'expected_server_not_found': 0,
        'injected_local_owner_errors': 1},
    'GenuineAbsentOwnerAndWaiterRetainNotFound': {
        'expected_server_puts': 1, 'expected_server_put_bytes': 64,
        'expected_server_pulls': 3, 'expected_server_grants': 1, 'expected_server_not_found': 2,
        'injected_local_owner_errors': 0}}


# Independent oracle for the additional exec-process suite; XML cannot choose it.
PROCESS_CASES = {
    'CrossProcessWaiterSharesPublishedBytes': {
        'expected_server_puts': 2, 'expected_server_put_bytes': 128,
        'expected_server_pulls': 2, 'expected_server_grants': 2, 'expected_server_not_found': 0,
        'injected_local_owner_errors': 0, 'killed_owner_count': 0, 'owned_children_reaped': 2,
        'child_launch': 'posix_spawn_exec', 'claim_schedule': 'owner_then_waiter_marker'},
    'OwnerLocalErrorFallsBackAcrossProcesses': {
        'expected_server_puts': 2, 'expected_server_put_bytes': 128,
        'expected_server_pulls': 2, 'expected_server_grants': 2, 'expected_server_not_found': 0,
        'injected_local_owner_errors': 1, 'killed_owner_count': 0, 'owned_children_reaped': 2,
        'child_launch': 'posix_spawn_exec', 'claim_schedule': 'owner_then_waiter_marker'},
    'KilledOwnerFallsBackThenLaterClaimTakesOver': {
        'expected_server_puts': 2, 'expected_server_put_bytes': 128,
        'expected_server_pulls': 3, 'expected_server_grants': 3, 'expected_server_not_found': 0,
        'injected_local_owner_errors': 0, 'killed_owner_count': 1, 'owned_children_reaped': 3,
        'child_launch': 'posix_spawn_exec', 'claim_schedule': 'owner_then_waiter_marker'}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', required=True, type=Path)
    parser.add_argument('--data-root', required=True, action='append', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--cross-process', action='store_true',
                        help='also verify distinct exec clients, owner death and recovery')
    args = parser.parse_args()
    result = run(args.build, args.data_root, args.output, args.tag, cross_process=args.cross_process)
    print(json.dumps({'passed': result['passed'], 'output': str(args.output)}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
