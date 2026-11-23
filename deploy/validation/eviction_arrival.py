#!/usr/bin/env python3
"""Bounded scalar TCP/RDMA fixed-arrival diagnostics; not a formal A/B executor."""

import argparse
import math
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eviction_baseline_smoke as baseline
import eviction_guard as guard
import eviction_realtime as realtime
import eviction_smoke as smoke
import eviction_trace as trace
from eviction_client import TraceClient
from eviction_replay import integer

MODEL = 'monotonic-real-client-arrival-v1'
TCP_MODE = 'tcp(rdma-not-requested)'
RDMA_MODE = 'rdma'
SCOPE = 'acknowledged-logical-shadow-not-residency'
TOOLS = ('eviction_arrival.py', 'eviction_realtime.py', 'eviction_guard.py',
         'eviction_trace.py', 'eviction_replay.py', 'eviction_client.py',
         'eviction_smoke.py', 'disk_scenarios.py', 'eviction_baseline_smoke.py')
COUNTERS = ('completed_requests', 'offered_requests', 'acked_put_bytes', 'hit_get_bytes')
ARTIFACTS = {'trace.json', 'config.json', 'child-result.json', 'stdout.log', 'stderr.log'}


def _file(path):
    if Path(path).is_symlink():
        raise ValueError('input file must not be a symlink')
    path = baseline._artifact(path)
    if not path.is_file():
        raise ValueError('expected regular input file')
    return path


def load(path):
    # Reject a final symlink before resolving it; no loader opens banned backing.
    if Path(path).is_symlink():
        raise ValueError('JSON input must not be a symlink')
    return trace.load(_file(path))


def _bounded(document):
    counts = smoke._bounded(document)
    if document['requests'][-1]['scheduled_ns'] > 5000000000:
        raise ValueError('arrival span exceeds five seconds')
    return counts


def _config(lib, members, *, workers=2, queue_capacity=16, timeout_ns=1000000000, tcp=True):
    lib = _file(baseline._artifact(lib))
    config = smoke._config(str(lib), members, tcp=tcp)
    # Each frozen config owns its environment, not the shared RDMA template.
    config['client_environment'] = dict(config['client_environment'])
    for name, value, low, high in (('workers', workers, 1, 16),
                                  ('queue_capacity', queue_capacity, 0, 1024),
                                  ('timeout_ns', timeout_ns, 1, 5000000000)):
        integer(name, value, low, high)
    config.update(workers=workers, queue_capacity=queue_capacity,
                  timeout_ns=timeout_ns, staged=True,
                  tools={name: baseline.file_sha(Path(__file__).with_name(name)) for name in TOOLS})
    return config


def _same(left, right):
    return trace.encode(left) == trace.encode(right)


def _transport(config):
    # Freeze the requested mode separately from the observed native transport.
    requested = config['client_environment']['DFKV_RDMA']
    if requested not in ('0', '1'):
        raise ValueError('explicit TCP/RDMA selection required')
    return TCP_MODE if requested == '0' else RDMA_MODE


def _validate_config(config):
    expected = _config(config['lib'], config['members'], workers=config['workers'],
                       queue_capacity=config['queue_capacity'], timeout_ns=config['timeout_ns'],
                       tcp=_transport(config) == TCP_MODE)
    if not _same(expected, config):
        raise ValueError('library, client configuration or replay sources changed')


class NativeBackend(realtime.SleepingFakeBackend):
    """Native calls run in wait(), outside the scheduler condition lock.

    store is only an acknowledged logical PUT shadow. It is not a disk resident
    census. Each executor thread leases its own pre-opened worker/namespace set;
    handles stay alive through physical drain, including timed-out native calls.
    """
    model = MODEL
    state_count_name = 'acknowledged_objects'

    def __init__(self, config, clients):
        super().__init__()
        self.config = config
        self.clients = clients
        self.local = threading.local()
        self.lease_lock = threading.Lock()
        self.leased = 0
        self.outcomes = {}

    def report_config(self, scheduler_config):
        return self.config

    def wait(self, row):
        if not hasattr(self.local, 'worker_id'):
            with self.lease_lock:
                self.local.worker_id = self.leased
                self.leased += 1
                if self.leased > len(self.clients):
                    raise RuntimeError('executor used more workers than configured')
        worker_id = self.local.worker_id
        observed = self.clients[worker_id][row['namespace_hex']].execute(row['request_id'])
        allowed = {'put_ack'} if row['op'] == 'put' else {'get_hit', 'get_miss', 'get_unresolved'}
        if not isinstance(observed, dict) or set(observed) != {'outcome'} or observed['outcome'] not in allowed:
            raise ValueError('native adapter returned an invalid operation outcome')
        # Unique request IDs mean independent workers never overwrite a result.
        # This lock transfers bookkeeping only, never encloses a native call.
        with self.lease_lock:
            self.outcomes[row['request_id']] = (observed['outcome'], worker_id)

    def complete(self, row):
        with self.lease_lock:
            outcome, worker_id = self.outcomes.pop(row['request_id'])
        row['worker_id'] = worker_id
        if outcome == 'put_ack':
            self.store[(row['namespace_hex'], row['key_hex'])] = trace.payload(row)
        return outcome


def run(document, config, *, library_loader=None, client_factory=TraceClient):
    counts = _bounded(document)
    _validate_config(config)
    result = {'model': MODEL, 'state_scope': SCOPE, 'formal_ab': False, 'gpu': 'excluded',
              'config': config, 'trace_sha256': document['sha256'], 'passed': False,
              'execution_started': False, 'setup_error': None, 'close_errors': [],
              'handles': [], 'ledger': None, 'summary': None}
    clients, opened = [], []
    try:
        if library_loader is None:
            sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'integration/common/src'))
            from disk_scenarios import load_library
            library_loader = load_library
        library = library_loader(config['lib'])
        if getattr(library, 'dfkv_get_auto_status', None) is None:
            raise ValueError('additive scalar status symbol required; no legacy no-hit fallback')
        for worker_id in range(config['workers']):
            worker = {}
            clients.append(worker)
            for namespace in sorted({r['namespace_hex'] for r in document['requests']}):
                client = client_factory(document, library, config['members'], bytes.fromhex(namespace))
                metadata = {'worker_id': worker_id, 'namespace_hex': namespace,
                            'transport': client.transport, 'closed': False}
                result['handles'].append(metadata)
                opened.append((client, metadata))
                worker[namespace] = client
                if client.transport != _transport(config):
                    required = 'TCP' if _transport(config) == TCP_MODE else 'RDMA'
                    raise ValueError(f'fixed-arrival diagnostic requires observed {required}; no transport fallback')
        result['execution_started'] = True
        backend = NativeBackend(config, clients)
        result['ledger'] = realtime.run(document, backend, workers=config['workers'],
                                        queue_capacity=config['queue_capacity'],
                                        timeout_ns=config['timeout_ns'], staged=True)
        result['summary'] = result['ledger']['summary']
    except Exception as error:
        if result['execution_started']:
            raise  # A partial scheduler crash must remain unknown to the parent.
        result['setup_error'] = f'{type(error).__name__}: {error}'
        result['unattempted_requests'] = [dict(row, terminal='not_attempted') for row in document['requests']]
        result['summary'] = {'planned': counts['requests'], 'offered': 0, 'completed': 0,
                             'acked_put_bytes': 0, 'hit_get_bytes': 0}
    finally:
        for client, metadata in reversed(opened):
            try:
                client.close()
                metadata['closed'] = True
            except Exception as error:
                result['close_errors'].append(f"worker={metadata['worker_id']} namespace={metadata['namespace_hex']}: {type(error).__name__}: {error}")
    _validate_config(config)
    result['passed'] = bool(result['ledger'] and result['ledger']['passed'] and not result['close_errors'])
    return result


def _verify(document, result, config):
    _bounded(document)
    _validate_config(config)
    if (not isinstance(result, dict) or result.get('model') != MODEL or result.get('state_scope') != SCOPE
            or result.get('formal_ab') is not False or result.get('gpu') != 'excluded'
            or not _same(result.get('config'), config) or result.get('trace_sha256') != document['sha256']
            or type(result.get('passed')) is not bool or type(result.get('execution_started')) is not bool
            or not isinstance(result.get('close_errors'), list)
            or any(not isinstance(e, str) or not e for e in result['close_errors'])):
        raise ValueError('invalid native arrival envelope')
    fields = {'model', 'state_scope', 'formal_ab', 'gpu', 'config', 'trace_sha256', 'passed',
              'execution_started', 'setup_error', 'close_errors', 'handles', 'ledger', 'summary'}
    if not result['execution_started']:
        fields.add('unattempted_requests')
    if set(result) != fields:
        raise ValueError('unexpected native envelope fields')
    handles = result.get('handles')
    expected = [(worker, namespace) for worker in range(config['workers'])
                for namespace in sorted({r['namespace_hex'] for r in document['requests']})]
    if not isinstance(handles, list) or len(handles) > len(expected):
        raise ValueError('invalid handle ownership proof')
    for handle, identity in zip(handles, expected):
        if (not isinstance(handle, dict) or set(handle) != {'worker_id', 'namespace_hex', 'transport', 'closed'}
                or not _same((handle['worker_id'], handle['namespace_hex']), identity)
                or type(handle['closed']) is not bool or not isinstance(handle['transport'], str)):
            raise ValueError('worker/namespace handle identity mismatch')
    if result['execution_started']:
        if len(handles) != len(expected) or any(h['transport'] != _transport(config) for h in handles) or result.get('setup_error') is not None:
            raise ValueError('native scheduler started before complete requested transport setup')
        scheduler = {name: config[name] for name in ('workers', 'queue_capacity', 'timeout_ns', 'staged')}
        summary = guard._verify(document, result.get('ledger'), scheduler,
                                expected_model=MODEL, expected_config=config,
                                state_count_name='acknowledged_objects', extra_terminals=('get_unresolved',))
        count_fields = set(guard._counts(result['ledger']['requests'], True))
        if set(summary) != count_fields | {'peak_running', 'peak_reserved', 'peak_queue', 'drained_ns'}:
            raise ValueError('unexpected native summary fields')
        for name, high in (('peak_running', config['workers']), ('peak_reserved', config['workers']),
                           ('peak_queue', config['queue_capacity'])):
            integer(name, summary.get(name), 0, high)
        if summary['peak_running'] > summary['peak_reserved']:
            raise ValueError('running calls exceed reserved slots')
        drain_lower = max([p.get('finished_ns', p.get('blocked_ns', 0)) for p in result['ledger']['phases']] +
                          [max(r['settled_ns'], r.get('completed_ns', 0)) for r in result['ledger']['requests']])
        integer('drained_ns', summary.get('drained_ns'), drain_lower, 10**18)
        events = sorted((timestamp, delta) for r in result['ledger']['requests'] if 'started_ns' in r
                        for timestamp, delta in ((r['started_ns'], 1), (r['completed_ns'], -1)))
        running, observed_peak = 0, 0
        for _, delta in events:
            running += delta
            observed_peak = max(observed_peak, running)
        if summary['peak_running'] < observed_peak:
            raise ValueError('native running peak understates observed overlap')
        for row in result['ledger']['requests']:
            if row['terminal'] != 'phase_blocked':
                planned = row['effective_scheduled_ns']
                derived = {'latency_ns': row['terminal_ns'] - planned,
                           'observed_latency_ns': row['settled_ns'] - planned,
                           'schedule_lag_ns': row['arrival_observed_ns'] - planned}
                if 'started_ns' in row:
                    derived.update(queue_wait_ns=row['started_ns'] - row['admitted_ns'],
                                   dispatch_lag_ns=row['started_ns'] - planned,
                                   service_ns=row['completed_ns'] - row['started_ns'],
                                   late_completion=row['completed_ns'] > row['deadline_ns'])
                if not _same({name: row.get(name) for name in derived}, derived):
                    raise ValueError('native derived timing does not reconcile')
            if row.get('backend_outcome') in {'put_ack', 'get_hit', 'get_miss', 'get_unresolved'}:
                integer('worker_id', row.get('worker_id'), 0, config['workers'] - 1)
        passed = result['ledger']['passed'] and not result['close_errors']
    else:
        if (result.get('ledger') is not None or not isinstance(result.get('setup_error'), str)
                or not result['setup_error'] or not _same(result.get('unattempted_requests'),
                    [dict(row, terminal='not_attempted') for row in document['requests']])):
            raise ValueError('invalid pre-workload failure proof')
        summary = {'planned': len(document['requests']), 'offered': 0, 'completed': 0,
                   'acked_put_bytes': 0, 'hit_get_bytes': 0}
        passed = False
    if not _same(result.get('summary'), summary) or result['passed'] != passed:
        raise ValueError('native summary or passed flag contradicts evidence')
    if not result['close_errors'] and any(not h['closed'] for h in handles):
        raise ValueError('unclosed handle reported without failure')
    return summary


def _command(source, raw, config_path):
    return [sys.executable, '-I', str(Path(__file__).resolve()), str(source), str(raw),
            '--child', '--config', str(config_path)]


def _hashes(directory):
    directory = baseline._artifact(directory)
    found = set(p.name for p in directory.iterdir()) - {'report.json'}
    if not found <= ARTIFACTS:
        raise ValueError('unexpected arrival artifact')
    return {name: baseline.file_sha(_file(directory / name)) for name in sorted(found)}


def supervise(source, output, *, lib, members, workers=2, queue_capacity=16,
              timeout_ns=1000000000, process_timeout_sec=10, tcp=True):
    if (type(process_timeout_sec) not in (int, float) or not math.isfinite(process_timeout_sec)
            or not .05 <= process_timeout_sec <= 30):
        raise ValueError('process timeout must be finite in [0.05,30]')
    document = load(source)
    counts = _bounded(document)
    config = _config(lib, members, workers=workers, queue_capacity=queue_capacity, timeout_ns=timeout_ns, tcp=tcp)
    output = baseline._new_output(output)
    output.mkdir()
    for name, value in (('trace.json', document), ('config.json', config)):
        (output / name).write_bytes(trace.encode(value))
    frozen, raw = output / 'trace.json', output / 'child-result.json'
    report = {'model': 'guarded-native-arrival-v1', 'formal_ab': False, 'gpu': 'excluded',
              'status': 'pending', 'passed': False, 'planned_requests': counts['requests'],
              'request_outcomes': 'unknown', **{name: None for name in COUNTERS},
              'trace_sha256': document['sha256'], 'trace_file_sha256': baseline.file_sha(frozen),
              'config': config, 'process_timeout_sec': process_timeout_sec,
              'child_pid': None, 'returncode': None, 'reaped': False, 'actions': [],
              'command': _command(frozen, raw, output / 'config.json')}
    started, child, interruption = time.monotonic_ns(), None, None
    env = {key: value for key, value in os.environ.items() if not key.startswith('DFKV_')}
    env.update(config['client_environment'])
    try:
        with (output / 'stdout.log').open('xb') as stdout, (output / 'stderr.log').open('xb') as stderr:
            child = subprocess.Popen(report['command'], stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, env=env)
            report['child_pid'] = child.pid
            remaining = process_timeout_sec - (time.monotonic_ns() - started) / 1e9
            if remaining <= 0:
                report['status'] = 'process_timeout'
            else:
                try:
                    child.wait(timeout=remaining)
                    if (time.monotonic_ns() - started) / 1e9 > process_timeout_sec:
                        report['status'] = 'process_timeout'
                except subprocess.TimeoutExpired:
                    report['status'] = 'process_timeout'
    except BaseException as error:
        report.update(status='supervisor_error', error=f'{type(error).__name__}: {error}')
        if not isinstance(error, Exception):
            interruption = error
    finally:
        if child is not None:
            if child.poll() is None:
                report['actions'] = guard._stop(child)
            report.update(returncode=child.poll(), reaped=child.returncode is not None)
            if not report['reaped']:
                report['status'] = 'cleanup_failed'
        report['elapsed_ns'] = time.monotonic_ns() - started
    if report['status'] == 'pending':
        if report['returncode'] not in (0, 1):
            report['status'] = 'child_exit'
        else:
            try:
                result = load(raw)
                summary = _verify(document, result, config)
                if report['returncode'] != (0 if result['passed'] else 1):
                    raise ValueError('child exit contradicts result')
                report.update(status='passed' if result['passed'] else 'workload_failed', passed=result['passed'],
                              request_outcomes='complete_child_ledger', completed_requests=summary['completed'],
                              offered_requests=summary['offered'], acked_put_bytes=summary['acked_put_bytes'],
                              hit_get_bytes=summary['hit_get_bytes'])
            except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
                report.update(status='invalid_result', error=f'{type(error).__name__}: {error}')
    try:
        _validate_config(config)
        if not _same(load(frozen), document) or not _same(load(output / 'config.json'), config):
            raise ValueError('frozen trace/config changed')
        report['artifacts'] = _hashes(output)
    except (OSError, ValueError, TypeError, KeyError) as error:
        report.update(status='integrity_error', passed=False, request_outcomes='unknown',
                      **{name: None for name in COUNTERS}, error=str(error))
    (output / 'report.json').write_bytes(trace.encode(report))
    if interruption is not None:
        raise interruption
    return report


def check(output, report_sha256):
    output = baseline._artifact(output)
    if baseline.file_sha(_file(output / 'report.json')) != report_sha256:
        raise ValueError('report external SHA256 mismatch')
    report = load(output / 'report.json')
    document, config = load(output / 'trace.json'), load(output / 'config.json')
    _validate_config(config)
    if (report.get('model') != 'guarded-native-arrival-v1' or report.get('formal_ab') is not False
            or report.get('gpu') != 'excluded' or not _same(report.get('config'), config)
            or report.get('trace_sha256') != document.get('sha256')
            or report.get('trace_file_sha256') != baseline.file_sha(output / 'trace.json')
            or report.get('planned_requests') != _bounded(document)['requests']
            or not _same(report.get('artifacts'), _hashes(output))):
        raise ValueError('report identity/artifact mismatch')
    if report.get('request_outcomes') == 'complete_child_ledger':
        result = load(output / 'child-result.json')
        summary = _verify(document, result, config)
        expected = dict(zip(COUNTERS, (summary['completed'], summary['offered'], summary['acked_put_bytes'], summary['hit_get_bytes'])))
        expected.update(passed=result['passed'], status='passed' if result['passed'] else 'workload_failed',
                        reaped=True, returncode=0 if result['passed'] else 1)
        if not _same({name: report.get(name) for name in expected}, expected):
            raise ValueError('parent contradicts independently checked child ledger')
    elif (report.get('request_outcomes') != 'unknown' or report.get('passed') is not False
          or report.get('status') not in {'process_timeout', 'supervisor_error', 'cleanup_failed', 'child_exit', 'invalid_result', 'integrity_error'}
          or any(report.get(name) is not None for name in COUNTERS)):
        raise ValueError('unknown outcomes were converted to zero or success')
    return report


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description=__doc__)
    if argv and argv[0] == 'check':
        parser.add_argument('check')
        parser.add_argument('output')
        parser.add_argument('--report-sha256', required=True)
        args = parser.parse_args(argv)
        try:
            result = check(args.output, args.report_sha256)
            print(trace.encode(result).decode(), end='')
            return 0 if result['passed'] else 1
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
            print(f'arrival check error: {error}', file=sys.stderr)
            return 1
    parser.add_argument('trace')
    parser.add_argument('output')
    parser.add_argument('--lib')
    parser.add_argument('--members')
    parser.add_argument('--child', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--config', help=argparse.SUPPRESS)
    parser.add_argument('--rdma', action='store_true', help='require observed RDMA; reject TCP fallback')
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--queue-capacity', type=int, default=16)
    parser.add_argument('--timeout-ns', type=int, default=1000000000)
    parser.add_argument('--process-timeout-sec', type=float, default=10)
    args = parser.parse_args(argv)
    try:
        if args.child:
            config = load(args.config)
            result = run(load(args.trace), config)
            with Path(args.output).open('xb') as destination:
                destination.write(trace.encode(result))
        else:
            if args.lib is None or args.members is None:
                raise ValueError('--lib and --members required')
            result = supervise(args.trace, args.output, lib=args.lib, members=args.members,
                               workers=args.workers, queue_capacity=args.queue_capacity,
                               timeout_ns=args.timeout_ns, process_timeout_sec=args.process_timeout_sec,
                               tcp=not args.rdma)
        print(trace.encode(result).decode(), end='')
        return 0 if result['passed'] else 1
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
        print(f'arrival error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
