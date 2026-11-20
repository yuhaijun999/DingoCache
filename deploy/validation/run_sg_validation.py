#!/usr/bin/env python3
"""Two-policy SG correctness matrix in the disposable validation guest only.

The host operator must first prove all VM backing is on allowed disks and run
the host thermal watchdog. This program never mounts disks, invokes Docker or
deletes data. Expected stopped-service errors are diagnostic successes, not hits.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import quick_eviction as quick
import sg_validation as sg


def guest_paths(roots, output, tag):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", tag):
        raise ValueError("tag must contain 1..64 ASCII letters/digits/_/-")
    if not Path('/etc/dfkv-validation-guest').is_file():
        raise ValueError("requires the disposable validation guest")
    if len(roots) != 3:
        raise ValueError("three guest data roots required")
    mounts = [Path(f'/data/disk{i}') for i in (1, 2, 3)]
    resolved = [Path(root).resolve(strict=True) for root in roots]
    for root, mount in zip(resolved, mounts):
        if not root.is_dir() or not os.path.ismount(mount):
            raise ValueError("data root must be on a mounted guest data disk")
        root.relative_to(mount)
        if root.stat().st_dev != mount.stat().st_dev:
            raise ValueError("data root crosses a nested mount")
    if len({root.stat().st_dev for root in resolved}) != 3:
        raise ValueError("three distinct guest data filesystems required")
    out = Path(output).resolve()
    out.relative_to(Path('/data/disk1/results'))
    if out.exists() or not out.parent.is_dir():
        raise ValueError("output must be new, with an existing parent")
    for policy in ('legacy', 'lifetime'):
        if any((root / f'sg-{tag}-{policy}').exists() for root in resolved):
            raise ValueError("case data already exists; use a fresh tag")
    return resolved, out


def check_metrics(before, after, policy):
    """Frozen RDMA oracle: zero-cap is local, oversize still grants disk data."""
    if policy not in ('legacy', 'lifetime'):
        raise ValueError("unknown policy")
    expected = {'dfkv_cache_put_total': 4, 'dfkv_bytes_written_total': 321,
                'dfkv_rdma_pull_requests_dispatched_total': 11,
                'dfkv_rdma_pull_requests_terminal_total': 11,
                'dfkv_uring_reads_total': 9, 'dfkv_uring_completions_total': 9,
                'dfkv_slab_lifetime_read_success_total': 9 if policy == 'lifetime' else 0}
    deltas = {}
    for name, wanted in expected.items():
        value = quick.total(after, name) - quick.total(before, name)
        if value != wanted:
            raise ValueError(f'{name}: expected delta {wanted}, got {value}')
        deltas[name] = value
    old = dict(quick.samples(before, 'dfkv_rdma_pull_requests_terminal_total'))
    for labels, value in quick.samples(after, 'dfkv_rdma_pull_requests_terminal_total'):
        wanted = (9 if 'path="uring"' in labels and 'result="success_nonempty"' in labels
                  else 2 if 'path="uring"' in labels and 'result="not_found"' in labels else 0)
        if value - old.get(labels, 0) != wanted:
            raise ValueError('unexpected PULL terminal path/result: ' + labels)
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if quick.total(after, name) != 1:
            raise ValueError('unhealthy storage: ' + name)
    for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total',
                 'dfkv_uring_inflight', 'dfkv_evictions_total'):
        if quick.total(after, name) != 0:
            raise ValueError('unexpected error/fallback/inflight/eviction: ' + name)
    return deltas


def stop_owned(server):
    """Only a Popen child created here; bounded TERM/KILL/reap, no process groups."""
    if server.poll() is None:
        server.terminate()
    try:
        code = server.wait(timeout=15)
    except subprocess.TimeoutExpired:
        server.kill()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError('owned server unreaped after KILL') from error
        raise RuntimeError('owned server required KILL')
    if code != 0:
        raise RuntimeError('owned server exited ' + str(code))


def scrape():
    with urllib.request.urlopen(quick.METRICS_URL, timeout=3) as response:
        return response.read().decode()


def run(build, roots, output, tag):
    roots, output = guest_paths(roots, output, tag)
    build = Path(build).resolve(strict=True)
    server_bin, library = build / 'dfkv_server', build / 'libdfkv.so'
    if not server_bin.is_file() or not library.is_file():
        raise ValueError('build must contain server and shared library')
    output.mkdir()
    result = {'model': 'sg-two-policy-correctness-v1', 'passed': False,
              'formal_ab': False, 'policies': [], 'errors': [],
              'build_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                               for p in (server_bin, library)}}
    env = {k: v for k, v in os.environ.items() if not k.startswith('DFKV_')}
    env.update(DFKV_RAM_TIER='0', DFKV_PUT_ACK_MODE='disk', DFKV_SLAB_RECLAIM_MS='50',
               DFKV_RDMA_DEV='rxe0', DFKV_RDMA_DEPTH='4', DFKV_GET_MISS_RETRIES='0',
               DFKV_RDMA_RECV_CHUNK_BYTES='8388608', DFKV_RDMA_MAX_PAYLOAD_BYTES='2097152',
               DFKV_RDMA_MAX_BLOCK_BYTES='2097152', DFKV_SERVER_URING='1')
    try:
        for policy in ('legacy', 'lifetime'):
            quick.check_ports()
            case = output / policy
            case.mkdir()
            dirs = [root / f'sg-{tag}-{policy}' for root in roots]
            for path in dirs:
                path.mkdir()
            command = [str(server_bin), '--dir', ','.join(map(str, dirs)),
                       '--cap', '12884901888', '--store-engine', 'slab',
                       '--slab-write', 'buffered', '--slab-eviction-policy', policy,
                       '--port', '12000', '--rdma-port', '12001', '--rdma-dev', 'rxe0',
                       '--max-msg', '2097152', '--metrics-port', '12010',
                       '--metrics-bind', '127.0.0.1']
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
                            raise RuntimeError('server exited before readiness')
                        try:
                            with urllib.request.urlopen('http://127.0.0.1:12010/readyz', timeout=1) as response:
                                if response.status == 200:
                                    break
                        except OSError:
                            pass
                        if time.monotonic() >= deadline:
                            raise TimeoutError('server readiness deadline')
                        time.sleep(.1)
                    before = scrape()
                    objects = quick.samples(before, 'dfkv_disk_objects')
                    if len(objects) != 3 or any(value != 0 for _, value in objects):
                        raise ValueError('fresh three-disk initial state required')
                    for path in dirs:
                        if not any('disk=' + json.dumps(str(path)) in labels for labels, _ in objects):
                            raise ValueError('disk identity mismatch')
                    (case / 'before.prom').write_text(before)
                    row['live'] = sg.supervise(case / 'live', lib=str(library),
                                               members='n=127.0.0.1:12001', tag=tag + '-' + policy,
                                               mode='live', process_timeout_sec=20)
                    if not row['live']['passed']:
                        raise ValueError('live SG oracle failed')
                    after = scrape()
                    (case / 'before-stop.prom').write_text(after)
                    row['server_delta'] = check_metrics(before, after, policy)
                    stop_owned(server)
                    row['server_exit'] = server.returncode
                    row['stopped'] = sg.supervise(case / 'stopped', lib=str(library),
                                                  members='n=127.0.0.1:12001', tag=tag + '-' + policy,
                                                  mode='stopped', process_timeout_sec=20)
                    if not row['stopped']['passed']:
                        raise ValueError('stopped-service SG oracle failed')
            finally:
                if server is not None:
                    stop_owned(server)
                    row['server_exit'] = server.returncode
            quick.check_ports()
        if result['build_sha256'] != {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                     for p in (server_bin, library)}:
            raise ValueError('build changed during the two-policy matrix')
        result['passed'] = True
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            raise
    finally:
        (output / 'matrix.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', required=True, type=Path)
    parser.add_argument('--data-root', action='append', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    result = run(args.build, args.data_root, args.output, args.tag)
    print(json.dumps({'passed': result['passed'], 'output': str(args.output)}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
