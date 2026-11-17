#!/usr/bin/env python3
"""Bounded RXE/io_uring eviction smoke; not a performance A/B or aging test.

Run in the disposable validation guest with unlimited memlock. Data is written
only to fresh children of the three supplied directories and is never deleted.
"""

import argparse
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import urllib.request
import uuid

COUNT = 6452
PORTS = (12000, 12001, 12010)
METRICS_URL = 'http://127.0.0.1:12010/metrics'


def samples(text, name):
    rows = []
    pattern = re.compile(r'^' + re.escape(name) + r'(\{.*\})?\s+(\S+)$')
    for line in text.splitlines():
        match = pattern.fullmatch(line)
        if match:
            value = float(match.group(2))
            if not math.isfinite(value) or value < 0:
                raise RuntimeError('invalid metric ' + name)
            rows.append((match.group(1) or '', value))
    if not rows:
        raise RuntimeError('missing metric ' + name)
    return rows


def total(text, name):
    return sum(value for _, value in samples(text, name))


def check_result(text, client, policy='lifetime'):
    if client.get('status') != 'passed' or client.get('wrong_bytes') != 0:
        raise RuntimeError('client correctness failure')
    if client.get('transport_mode') != ['rdma', 'rdma']:
        raise RuntimeError('client did not use RDMA')
    counts = [client.get(name) for name in ('hits', 'misses', 'hot_hits', 'hot_misses', 'hot_reads')]
    if any(type(value) is not int or value < 0 for value in counts):
        raise RuntimeError('invalid client read counts')
    hits, misses, hot_hits, hot_misses, hot_reads = counts
    if hits + misses != COUNT * 2 or hot_reads != hot_hits + hot_misses or not hot_hits:
        raise RuntimeError('client read denominator mismatch')
    for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
        if total(text, name) != 1:
            raise RuntimeError('unhealthy: ' + name)
    for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total', 'dfkv_uring_inflight'):
        if total(text, name) != 0:
            raise RuntimeError('unexpected errors/fallback/inflight: ' + name)
    for name in ('dfkv_evictions_total', 'dfkv_uring_reads_total', 'dfkv_uring_completions_total'):
        if total(text, name) <= 0:
            raise RuntimeError('required activity not observed: ' + name)
    objects = samples(text, 'dfkv_disk_objects')
    if len(objects) != 3 or len({labels for labels, _ in objects}) != 3 or any(value <= 0 for _, value in objects):
        raise RuntimeError('all three disks must contain objects')
    terminal = {}
    for labels, value in samples(text, 'dfkv_rdma_pull_requests_terminal_total'):
        match = re.search(r'(?:\{|,)result="([^"]+)"(?:,|\})', labels)
        if not match:
            raise RuntimeError('terminal result label missing')
        key = match.group(1)
        terminal[key] = terminal.get(key, 0) + value
        if key not in ('success_nonempty', 'not_found') and value:
            raise RuntimeError('unexpected PULL terminal: ' + key)
    if terminal.get('success_nonempty', 0) != hits + hot_hits:
        raise RuntimeError('success denominator mismatch')
    if terminal.get('not_found', 0) != misses + hot_misses:
        raise RuntimeError('missing denominator mismatch')
    dispatched = total(text, 'dfkv_rdma_pull_requests_dispatched_total')
    if dispatched != total(text, 'dfkv_rdma_pull_requests_terminal_total') or dispatched != sum(counts[:4]):
        raise RuntimeError('PULL requests not settled')
    heat = {}
    if policy == 'lifetime':
        success = total(text, 'dfkv_slab_lifetime_read_success_total')
        applied = total(text, 'dfkv_slab_lifetime_read_applied_total')
        if success != hits + hot_hits or applied > success:
            raise RuntimeError('lifetime heat denominator mismatch')
        # Pressure can reclaim the original resident before its heat notification.
        heat = {'read_success': success, 'read_applied': applied}
    return {**heat, 'watermark_evictions': total(text, 'dfkv_slab_watermark_evictions_total'),
            'background_reclaimed': total(text, 'dfkv_slab_reclaimed_total'),
            'extent_steals': total(text, 'dfkv_slab_extent_steals_total'),
            'evictions': total(text, 'dfkv_evictions_total'), 'terminal_results': terminal,
            'uring_reads': total(text, 'dfkv_uring_reads_total'),
            'uring_completions': total(text, 'dfkv_uring_completions_total')}


def check_ports():
    sockets = []
    try:
        for port in PORTS:
            sock = socket.socket()
            sockets.append(sock)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('0.0.0.0', port))
    finally:
        for sock in sockets:
            sock.close()


def stop_server(server):
    if server.poll() is None:
        server.terminate()
    try:
        code = server.wait(timeout=15)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()
        raise RuntimeError('server teardown required SIGKILL')
    if code != 0:
        raise RuntimeError('server exited ' + str(code))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--data-root', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--policy', choices=('legacy', 'lifetime'), default='lifetime')
    args = parser.parse_args(argv)
    args.build, args.output = args.build.resolve(), args.output.resolve()
    args.data_root = [path.resolve() for path in args.data_root]
    if len(args.data_root) != 3 or len(set(args.data_root)) != 3:
        parser.error('exactly three distinct --data-root directories are required')
    if any(not path.is_dir() or ',' in str(path) for path in args.data_root):
        parser.error('data roots must be existing directories without commas')
    if not (args.build / 'dfkv_server').is_file() or not (args.build / 'libdfkv.so').is_file():
        parser.error('build must contain dfkv_server and libdfkv.so')
    if args.output.exists():
        parser.error('output must be a new directory')
    return args


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    result = {'status': 'failed', 'policy': args.policy}
    server = None
    try:
        check_ports()
        dirs = [root / ('quick-' + uuid.uuid4().hex) for root in args.data_root]
        for path in dirs:
            path.mkdir(exist_ok=False)
        source = Path(__file__).resolve().parents[2]
        env = {key: value for key, value in os.environ.items() if not key.startswith('DFKV_')}
        env.update(DFKV_RAM_TIER='0', DFKV_PUT_ACK_MODE='disk', DFKV_SLAB_RECLAIM_MS='50',
                   DFKV_RDMA_DEV='rxe0', DFKV_RDMA_DEPTH='4',
                   DFKV_RDMA_RECV_CHUNK_BYTES='8388608', DFKV_RDMA_MAX_PAYLOAD_BYTES='2097152',
                   DFKV_RDMA_MAX_BLOCK_BYTES='2097152', DFKV_SERVER_URING='1',
                   PYTHONPATH=str(source / 'integration/common/src'))
        command = [str(args.build / 'dfkv_server'), '--dir', ','.join(map(str, dirs)),
                   '--cap', '12884901888', '--store-engine', 'slab', '--slab-write', 'buffered',
                   '--slab-eviction-policy', args.policy, '--port', '12000', '--rdma-port', '12001',
                   '--rdma-dev', 'rxe0', '--max-msg', '2097152', '--metrics-port', '12010',
                   '--metrics-bind', '127.0.0.1']
        (args.output / 'case.json').write_text(json.dumps({'command': command, 'data_dirs': list(map(str, dirs)),
            'environment': {k: v for k, v in env.items() if k.startswith('DFKV_') or k == 'PYTHONPATH'}}, indent=2) + '\n')
        with (args.output / 'server.log').open('w') as server_log:
            server = subprocess.Popen(command, env=env, stdout=server_log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 30
            while True:
                if server.poll() is not None:
                    raise RuntimeError('server exited during startup')
                try:
                    with urllib.request.urlopen('http://127.0.0.1:12010/readyz', timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError('readiness timeout')
                time.sleep(.1)
            def scrape():
                with urllib.request.urlopen(METRICS_URL, timeout=3) as response:
                    return response.read().decode()
            before = scrape()
            # Refuse to send writes if a competing process won the fixed-port race.
            labels = [labels for labels, _ in samples(before, 'dfkv_disk_objects')]
            expected_labels = ['disk=' + json.dumps(str(path), ensure_ascii=False) for path in dirs]
            if len(labels) != 3 or any(not any(expected in label for label in labels)
                                       for expected in expected_labels):
                raise RuntimeError('metrics do not belong to this fresh server')
            (args.output / 'before.prom').write_text(before)
            client_command = [sys.executable, str(source / 'deploy/validation/disk_scenarios.py'),
                              '--lib', str(args.build / 'libdfkv.so'), '--members', 'n=127.0.0.1:12001',
                              '--scenario', 'quick-pressure', '--count', str(COUNT),
                              '--output', str(args.output / 'trace')]
            with (args.output / 'client.log').open('w') as log:
                subprocess.run(client_command, env={**env, 'DFKV_RDMA': '1'}, stdout=log,
                               stderr=subprocess.STDOUT, timeout=120, check=True)
            client = json.loads((args.output / 'trace/result.json').read_text())
            after = scrape()
            (args.output / 'after.prom').write_text(after)
            result.update(check_result(after, client, args.policy), client=client, status='passed')
    except Exception as error:
        result.update(status='failed', error=str(error))
    finally:
        if server is not None:
            try:
                stop_server(server)
            except Exception as error:
                result.update(status='failed', cleanup_error=str(error))
            result['server_exit'] = server.returncode
        result['elapsed_sec'] = time.monotonic() - started
        (args.output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    outcome = run(parse_args())
    print(json.dumps(outcome, indent=2))
    raise SystemExit(0 if outcome['status'] == 'passed' else 1)
