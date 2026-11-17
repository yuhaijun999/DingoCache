#!/usr/bin/env python3
"""Bounded RXE/io_uring eviction smoke; not performance A/B or long-term aging acceptance.

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
# Two 32-key namespaces: two warmups plus 116 cold-batch probes.
HOT_READS = 7552
SUSTAINED_COUNT = 7808
SUSTAINED_HOT_READS = 12928
SUSTAINED_STEPS = 200
# Frozen diagnostic continuity eligibility, not a release performance threshold.
MAX_STEP_GAP_SEC = 5
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


def check_steps(client):
    steps = client.get('steps')
    if not isinstance(steps, list) or len(steps) != SUSTAINED_STEPS:
        raise RuntimeError('sustained step count mismatch')
    previous_end = 0
    previous_start = None
    gaps = []
    hot_hits = 128  # The two strict warmups precede all pressure steps.
    hot_misses = 0
    lateness = []
    durations = []
    for index, row in enumerate(steps):
        if not isinstance(row, dict) or type(row.get('step')) is not int or row['step'] != index:
            raise RuntimeError('sustained step index mismatch')
        values = [row.get(name) for name in ('cold_written', 'hot_hits', 'hot_misses')]
        if (any(type(value) is not int or value < 0 for value in values) or
                values[0] != 32 or values[1] + values[2] != 64):
            raise RuntimeError('sustained step operation mismatch')
        start, end = row.get('start_sec'), row.get('end_sec')
        if (any(type(value) not in (int, float) or not math.isfinite(value) or value < 0
                for value in (start, end)) or start < previous_end or end < start or start + 1e-6 < index):
            raise RuntimeError('sustained step timing mismatch')
        if previous_start is None:
            if start > MAX_STEP_GAP_SEC:
                raise RuntimeError('sustained pressure started too late')
        else:
            gap = start - previous_start
            if gap < 1 - 1e-6 or gap > MAX_STEP_GAP_SEC:
                raise RuntimeError('sustained continuity gap outside 1..5 seconds')
            gaps.append(gap)
        previous_start = start
        previous_end = end
        hot_hits += values[1]
        hot_misses += values[2]
        lateness.append(max(0, start - index))
        durations.append(end - start)
    origin = client.get('pressure_schedule_origin_sec')
    elapsed = client.get('pressure_schedule_elapsed_sec')
    if (any(type(value) not in (int, float) or not math.isfinite(value) or value < 0
            for value in (origin, elapsed)) or elapsed < previous_end or elapsed < 199):
        raise RuntimeError('sustained schedule duration mismatch')
    if hot_hits != client['hot_hits'] or hot_misses != client['hot_misses']:
        raise RuntimeError('sustained hot cohort mismatch')
    return {'sustained_steps': len(steps), 'schedule_elapsed_sec': elapsed, 'max_step_gap_sec': max(gaps),
            'max_step_lateness_sec': max(lateness), 'max_step_duration_sec': max(durations)}


def check_result(text, client, policy='lifetime', scenario='quick-pressure'):
    if scenario not in ('quick-pressure', 'sustained-pressure'):
        raise RuntimeError('unknown scenario')
    expected_count = SUSTAINED_COUNT if scenario == 'sustained-pressure' else COUNT
    expected_hot_reads = SUSTAINED_HOT_READS if scenario == 'sustained-pressure' else HOT_READS
    if client.get('status') != 'passed' or client.get('wrong_bytes') != 0:
        raise RuntimeError('client correctness failure')
    if client.get('transport_mode') != ['rdma', 'rdma']:
        raise RuntimeError('client did not use RDMA')
    counts = [client.get(name) for name in ('hits', 'misses', 'hot_hits', 'hot_misses', 'hot_reads')]
    if any(type(value) is not int or value < 0 for value in counts):
        raise RuntimeError('invalid client read counts')
    hits, misses, hot_hits, hot_misses, hot_reads = counts
    if (hits + misses != expected_count * 2 or hot_reads != expected_hot_reads or
            hot_reads != hot_hits + hot_misses or not hot_hits):
        raise RuntimeError('client read denominator mismatch')
    schedule = check_steps(client) if scenario == 'sustained-pressure' else {}
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
    return {**schedule, **heat, 'watermark_evictions': total(text, 'dfkv_slab_watermark_evictions_total'),
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
    parser.add_argument('--scenario', choices=('quick-pressure', 'sustained-pressure'),
                        default='quick-pressure')
    parser.add_argument('--pressure-delay-sec', type=int, default=0,
                        help='wait after hot warmup before cold writes (0..300)')
    parser.add_argument('--data-name', help='new child name shared across all data roots')
    args = parser.parse_args(argv)
    if not 0 <= args.pressure_delay_sec <= 300:
        parser.error('pressure delay must be in 0..300 seconds')
    if args.scenario == 'sustained-pressure' and args.pressure_delay_sec:
        parser.error('sustained-pressure does not accept a pre-pressure delay')
    if args.data_name is not None and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', args.data_name):
        parser.error('data name must be a safe ASCII component of 1..80 characters')
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
    result = {'status': 'failed', 'policy': args.policy, 'scenario': args.scenario}
    server = None
    try:
        check_ports()
        dirs = [root / (args.data_name or ('quick-' + uuid.uuid4().hex))
                for root in args.data_root]
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
        count = SUSTAINED_COUNT if args.scenario == 'sustained-pressure' else COUNT
        client_command = [sys.executable, str(source / 'deploy/validation/disk_scenarios.py'),
                          '--lib', str(args.build / 'libdfkv.so'), '--members', 'n=127.0.0.1:12001',
                          '--scenario', args.scenario, '--count', str(count),
                          '--pressure-delay-sec', str(args.pressure_delay_sec),
                          '--metrics-url', METRICS_URL, '--output', str(args.output / 'trace')]
        client_timeout_sec = (320 if args.scenario == 'sustained-pressure' else
                              120 + args.pressure_delay_sec)
        (args.output / 'case.json').write_text(json.dumps({'command': command, 'data_dirs': list(map(str, dirs)),
            'client_command': client_command, 'client_timeout_sec': client_timeout_sec,
            'pressure_delay_sec': args.pressure_delay_sec, 'scenario': args.scenario,
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
            with (args.output / 'client.log').open('w') as log:
                subprocess.run(client_command, env={**env, 'DFKV_RDMA': '1'}, stdout=log,
                               stderr=subprocess.STDOUT, timeout=client_timeout_sec, check=True)
            client = json.loads((args.output / 'trace/result.json').read_text())
            after = scrape()
            (args.output / 'after.prom').write_text(after)
            result.update(check_result(after, client, args.policy, args.scenario), client=client, status='passed')
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
