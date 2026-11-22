#!/usr/bin/env python3
"""Repeatable tiny TCP/slab correctness baseline, never formal A/B or performance.

Only fresh outputs outside /mnt are accepted. Each policy has its own empty
1-GiB sparse-capacity slab; the workload writes only four 128-byte payloads.
The native TCP listener uses its existing bind behavior; clients use loopback.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import socket
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_smoke as smoke
import eviction_trace as trace
import quick_eviction as quick


def _artifact(path):
    """Reject banned backing paths before stat/hash/exec, including symlinks."""
    path = Path(path).absolute()
    banned = Path('/mnt/disk1')
    if path == banned or banned in path.parents:
        raise ValueError('host /mnt/disk1 artifacts are forbidden')
    resolved = path.resolve()
    if resolved == banned or banned in resolved.parents:
        raise ValueError('host /mnt/disk1 artifacts are forbidden')
    return resolved


def file_sha(path):
    digest = hashlib.sha256()
    with _artifact(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def environment():
    result = {key: value for key, value in os.environ.items() if not key.startswith('DFKV_')}
    result.update(DFKV_RDMA='0', DFKV_RAM_TIER='0', DFKV_PUT_ACK_MODE='disk',
                  DFKV_READ_COALESCE='0', DFKV_SLAB_RECLAIM_MS='0', DFKV_LOG='INFO')
    return result


def stop_owned(server):
    """Stop only this owned Popen child; KILL/nonzero exit is a failure."""
    forced = False
    if server.poll() is None:
        server.terminate()
    try:
        code = server.wait(timeout=5)
    except subprocess.TimeoutExpired:
        forced = True
        server.kill()
        code = server.wait(timeout=3)
    if forced or code != 0:
        raise RuntimeError(f'owned server shutdown failed: exit={code}, killed={forced}')


HTTP_TIMEOUT_SEC = 3
_SCRAPE_CODE = """
import sys
import urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open('http://127.0.0.1:' + sys.argv[1] + '/' + sys.argv[2], timeout=1) as response:
    body = response.read(4 * 1024 * 1024 + 1)
    if len(body) > 4 * 1024 * 1024:
        raise ValueError('metrics response exceeds 4 MiB')
    sys.stdout.buffer.write(body)
"""


def _scrape(port, path='metrics'):
    """Bound the whole HTTP request, including trickling response headers.

    subprocess.run kills and reaps only its own exact child on timeout. Socket
    timeouts alone are insufficient: repeated short reads can extend them.
    """
    if type(port) is not int or not 1 <= port <= 65535 or path not in ('metrics', 'readyz'):
        raise ValueError('invalid local metrics endpoint')
    try:
        completed = subprocess.run([sys.executable, '-I', '-c', _SCRAPE_CODE, str(port), path],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, timeout=HTTP_TIMEOUT_SEC, check=True)
    except subprocess.TimeoutExpired as error:
        raise TimeoutError('whole metrics HTTP request deadline') from error
    except subprocess.CalledProcessError as error:
        raise OSError('local metrics request failed: ' +
                      error.stderr[:4000].decode('utf-8', errors='replace')) from error
    return completed.stdout.decode('utf-8')


def _port_greeting(server, timeout_sec=10):
    """Read a bounded startup line; partial output never causes readline hangs."""
    deadline = time.monotonic() + timeout_sec
    greeting = b''
    with selectors.DefaultSelector() as selector:
        selector.register(server.stdout, selectors.EVENT_READ)
        while b'\n' not in greeting and len(greeting) < 128:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not selector.select(timeout=remaining):
                raise TimeoutError('owned server port greeting deadline')
            chunk = os.read(server.stdout.fileno(), 128 - len(greeting))
            if not chunk:
                raise ValueError('owned server closed before port greeting')
            greeting += chunk
    text = greeting.decode('ascii').strip()
    if b'\n' not in greeting or not re.fullmatch(r'PORT [0-9]{1,5}', text):
        raise ValueError('invalid owned server port greeting')
    if not 1 <= int(text.split()[1]) <= 65535:
        raise ValueError('invalid TCP port')
    return text


def _ready(server, port, log, policy):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError('owned server exited before readiness')
        try:
            _scrape(port, 'readyz')
            text = log.read_text()
            for name in ('disk_eviction_requested', 'disk_eviction_effective'):
                if not re.search(r'\b' + name + r'\s*=\s*' + policy + r'\s+\(', text):
                    raise ValueError('effective policy not yet recorded')
            return _scrape(port)
        except (OSError, ValueError):
            time.sleep(.05)
    raise TimeoutError('owned server readiness deadline')


def _new_output(output):
    output = Path(output).absolute()
    resolved = output.resolve()
    if resolved == Path('/mnt') or Path('/mnt') in resolved.parents:
        raise ValueError('this tiny smoke refuses all /mnt outputs')
    if os.path.lexists(output) or not output.parent.is_dir():
        raise ValueError('output must be new, with an existing parent')
    return output


def run(build, output):
    output = _new_output(output)
    build = _artifact(build).resolve(strict=True)
    binaries = [_artifact(build / name) for name in ('dfkv_server', 'libdfkv.so')]
    if any(not path.is_file() for path in binaries):
        raise ValueError('build must contain dfkv_server and libdfkv.so')
    hashes = {name: file_sha(path) for name, path in zip(('dfkv_server', 'libdfkv.so'), binaries)}
    output.mkdir()
    document = trace.generate(objects=2, rounds=2, length=128, interval_ns=80000000)
    source = output / 'trace.json'
    source.write_bytes(trace.encode(document))
    result = {'model': 'two-policy-tcp-slab-correctness-only-v1', 'passed': False,
              'formal_ab': False, 'performance_baseline': False,
              'build': str(build), 'build_sha256': hashes,
              'trace_file_sha256': file_sha(source), 'planned_per_policy': 8,
              'policies': [{'policy': policy, 'status': 'not_attempted', 'passed': False,
                            'request_outcomes': 'unknown', 'completed_requests': None,
                            'server_exit': None, 'server_reaped': False}
                           for policy in ('legacy', 'lifetime')],
              'errors': [], 'gpu': 'excluded',
              'common': {'engine': 'slab', 'ack': 'disk', 'ram': False,
                         'coalescing': False, 'capacity_bytes': 1 << 30,
                         'slab_write': 'buffered', 'slot_granularity': 1 << 20}}
    interruption = None
    try:
        for row in result['policies']:
            policy = row['policy']
            row['status'] = 'started'
            case = output / policy
            case.mkdir()
            data = case / 'data'
            data.mkdir()
            # A released ephemeral metrics port has a race. Never take over its
            # occupant: readiness additionally needs our own effective-config log.
            with socket.socket() as reservation:
                reservation.bind(('127.0.0.1', 0))
                metrics_port = reservation.getsockname()[1]
            command = [str(binaries[0]), '--dir', str(data), '--port', '0',
                       '--cap', str(1 << 30), '--store-engine', 'slab',
                       '--slab-write', 'buffered', '--slab-eviction-policy', policy,
                       '--ram-tier', 'off', '--metrics-port', str(metrics_port),
                       '--metrics-bind', '127.0.0.1', '--log', 'INFO']
            env = environment()
            row.update(command=command, data_dir=str(data), metrics_port=metrics_port,
                       server_environment={k: v for k, v in env.items() if k.startswith('DFKV_')})
            server = None
            try:
                log_path = case / 'server.log'
                with log_path.open('xb') as log:
                    server = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                              stdout=subprocess.PIPE, stderr=log)
                    row['server_pid'] = server.pid
                    greeting = _port_greeting(server)
                    (case / 'stdout.log').write_text(greeting + '\n')
                    port = int(greeting.split()[1])
                    before = _ready(server, metrics_port, log_path, policy)
                    # Detect a metrics-port bind race rather than observing an
                    # unrelated listener whose health happens to be good.
                    if ' /metrics failed to start ' in log_path.read_text():
                        raise ValueError('owned metrics listener failed')
                    objects = quick.samples(before, 'dfkv_disk_objects')
                    if len(objects) != 1 or objects[0][1] != 0 or 'disk=' + json.dumps(str(data)) not in objects[0][0]:
                        raise ValueError('fresh owned one-disk state required')
                    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
                        if quick.total(before, name) != 1:
                            raise ValueError('storage unhealthy')
                    (case / 'before.prom').write_text(before)
                    live = smoke.supervise(source, case / 'client', lib=str(binaries[1]),
                                            members=f'n=127.0.0.1:{port}', tcp=True,
                                            process_timeout_sec=10)
                    row['client'] = live
                    row.update(request_outcomes=live['request_outcomes'],
                               completed_requests=live['completed_requests'])
                    if not live['passed'] or live['completed_requests'] != 8 or live['acked_put_bytes'] != 512:
                        raise ValueError('full tiny correctness ledger failed')
                    after = _scrape(metrics_port)
                    (case / 'after.prom').write_text(after)
                    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
                        if quick.total(after, name) != 1:
                            raise ValueError('storage unhealthy after client')
                    expected_heat = 4 if policy == 'lifetime' else 0
                    heat = quick.total(after, 'dfkv_slab_lifetime_read_success_total') - quick.total(before, 'dfkv_slab_lifetime_read_success_total')
                    if heat != expected_heat:
                        raise ValueError('effective lifetime heat oracle failed')
                    row['lifetime_read_success_delta'] = heat
                    row['passed'] = True
                    row['status'] = 'passed'
            finally:
                if server is not None:
                    try:
                        stop_owned(server)
                    except BaseException:
                        row['passed'] = False
                        row['status'] = 'shutdown_failed'
                        raise
                    finally:
                        row.update(server_exit=server.poll(), server_reaped=server.returncode is not None)
                        server.stdout.close()
        result['passed'] = len(result['policies']) == 2 and all(row['passed'] for row in result['policies'])
    except BaseException as error:
        result['passed'] = False
        result['errors'].append(f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            interruption = error
    finally:
        try:
            after_hashes = {name: file_sha(path) for name, path in zip(('dfkv_server', 'libdfkv.so'), binaries)}
            result['build_sha256_after'] = after_hashes
            if after_hashes != hashes or file_sha(source) != result['trace_file_sha256']:
                result['passed'] = False
                result['errors'].append('binary/library/trace changed during run')
        except (OSError, ValueError) as error:
            result['passed'] = False
            result['errors'].append(f'final hash check: {error}')
        (output / 'report.json').write_bytes(trace.encode(result))
    if interruption is not None:
        raise interruption
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', required=True)
    parser.add_argument('output', help='new retained directory outside /mnt')
    args = parser.parse_args(argv)
    try:
        result = run(args.build, args.output)
        print(json.dumps(result, sort_keys=True))
        return 0 if result['passed'] else 1
    except (OSError, ValueError, UnicodeError) as error:
        print(f'baseline smoke error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
