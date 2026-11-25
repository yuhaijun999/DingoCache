#!/usr/bin/env python3
"""External RDMA-only long-residence diagnostic, not formal A/B or day-scale proof.

An operator owns fresh capacity, effective policy, disk-ACK, thermal safeguards,
server lifetime and process timeout. This tool never starts or resets a server.
The accelerated workload uses seconds, not a claim of equivalent default aging.
"""

import argparse
import json
import math
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'integration/common/src'))
import disk_scenarios as disk
import eviction_baseline_smoke as baseline
import quick_eviction as quick


# Each tuple is (new arrivals per namespace per step, P in seconds).
# These bounded diagnostics do not change either existing profile by default.
PRESSURE_CASES = {'low': (16, 180), 'medium': (32, 180), 'high': (64, 180),
                  'p60': (64, 60), 'p120': (64, 120)}


def profile(name, *, hot_refill=False, pressure_case=None, pressure_steps=None,
            continuous_reuse=False):
    if name not in ('accelerated', 'default'):
        raise ValueError('unknown residence profile')
    if type(continuous_reuse) is not bool:
        raise ValueError('continuous_reuse must be bool')
    if type(hot_refill) is not bool:
        raise ValueError('hot_refill must be bool')
    if pressure_case is not None and (name != 'default' or type(pressure_case) is not str
                                      or pressure_case not in PRESSURE_CASES):
        raise ValueError('pressure_case requires default profile and a known preset')
    if pressure_steps is not None and (pressure_case is None or type(pressure_steps) is not int
                                       or pressure_steps not in (660, 1800)):
        raise ValueError('pressure_steps requires a pressure preset and must be 660 or 1800')
    config = {'name': name, 'protect_sec': 12 if name == 'accelerated' else 180,
            'delays_sec': [3, 13, 24] if name == 'accelerated' else [45, 185, 600],
            'steps': 300 if name == 'accelerated' else 1800,
            'preage_sec': 120 if name == 'accelerated' else 240,
            'interval_sec': 1, 'prefill': 6144, 'cohort_size': 32,
            'arrival_batch': 64, 'length': 4096, 'seed': 0xD1A60051}
    if pressure_case is not None:
        config['arrival_batch'], config['protect_sec'] = PRESSURE_CASES[pressure_case]
        config['steps'] = 660 if pressure_steps is None else pressure_steps
    if hot_refill:
        config['hot_refill'] = True
    if continuous_reuse:
        config['continuous_reuse'] = True
    return config


def validate(config):
    if not isinstance(config, dict) or set(config) - {'hot_refill', 'continuous_reuse'} != set(profile('default')):
        raise ValueError('invalid profile fields')
    if 'hot_refill' in config and type(config['hot_refill']) is not bool:
        raise ValueError('hot_refill must be bool')
    if config['name'] not in ('accelerated', 'default'):
        raise ValueError('unknown residence profile')
    for name, low, high in (('protect_sec', 1, 180), ('steps', 1, 1800),
                            ('preage_sec', 1, 240), ('interval_sec', 1, 10), ('prefill', 6, 16384),
                            ('cohort_size', 1, 128), ('arrival_batch', 1, 128),
                            ('length', 1, 1048576), ('seed', 0, (1 << 64) - 1)):
        if type(config[name]) is not int or not low <= config[name] <= high:
            raise ValueError('invalid ' + name)
    if ('continuous_reuse' in config and type(config['continuous_reuse']) is not bool
            or config.get('continuous_reuse', False) and config['arrival_batch'] < 3):
        raise ValueError('continuous reuse requires bool and at least three arrival keys')
    delays = config['delays_sec']
    if (not isinstance(delays, list) or len(delays) != 3
            or any(type(value) is not int or not 1 <= value <= 600 for value in delays)
            or delays != sorted(set(delays))
            or not delays[0] < config['protect_sec'] < delays[1]
            or delays[-1] >= config['steps'] * config['interval_sec']
            or config['prefill'] < 5 * config['cohort_size']):
        raise ValueError('invalid delay/cohort schedule')
    return dict(config, delays_sec=list(delays))


def expected_counts(config):
    config = validate(config)
    return {'initial_puts': 2 * config['prefill'],
            'arrival_puts': 2 * config['arrival_batch'] * config['steps'],
            'warm_gets': 8 * config['cohort_size'],
            'preage_gets': 2 * config['cohort_size'] * config['preage_sec'],
            'ready_gets': 2 * config['cohort_size'],
            'hot_gets': 2 * config['cohort_size'] * config['steps'],
            'first_gets': 6 * config['cohort_size'],
            'cooled_gets': 2 * config['cohort_size']}


def reuse_summary(config):
    """Fixed enrollment, with due outcomes and right-censoring counted separately."""
    fields = ('enrolled', 'completed', 'censored', 'hits', 'misses', 'refill_puts',
              'refill_payload_bytes', 'verify_gets', 'verify_hits')
    result = dict.fromkeys(fields, 0)
    result['by_delay'] = {str(delay): dict.fromkeys(fields, 0)
                          for delay in config['delays_sec']}
    result['censored_keys'] = []
    return result


def run(clients, config, *, clock=time.monotonic, sleep=time.sleep, eviction_counter=None):
    """Run two namespace-bound adapters, returning partial evidence on failure.

    A batch ACK timestamps every key in that batch. Ages are client-observed ACK
    ages, not disk commit ages. GET completion is never substituted for start.
    Closed-loop steps do not catch up: overruns shift the next step and are
    reported. No full-device occupation or throughput guarantee is inferred.
    """
    config = validate(config)
    planned = expected_counts(config)
    result = {'model': 'long-residence-rdma-v1', 'formal_ab': False,
              'gpu': 'excluded', 'passed': False, 'config': config,
              'planned': planned, 'actual': {key: 0 for key in planned},
              'put_payload_bytes': 0, 'refill_puts': 0, 'refill_payload_bytes': 0,
              'verify_gets': 0, 'verify_hits': 0, 'rows': [], 'errors': [],
              'steps': [], 'preage_steps': [], 'pressure_samples': [], 'unresolved_operation': None,
              'counter_scope': 'validated-terminals-and-acknowledged-PUTs-not-total-attempts',
              'max_lateness_sec': 2, 'max_step_sec': 5,
              'transport_modes': [], 'scope': 'closed-loop-client-ACK-aged-diagnostic'}
    continuous = config.get('continuous_reuse', False)
    pending_reuse = []
    if continuous:
        result['continuous_reuse'] = reuse_summary(config)
    hot_refill = config.get('hot_refill', False)
    lost_hot = set()
    result['hot_read_summary'] = {'distinct_lost_keys': 0, 'repeat_misses': 0}
    if hot_refill:
        result['hot_refill'] = dict.fromkeys((
            'distinct_lost_keys', 'repeat_misses', 'refill_puts',
            'refill_payload_bytes', 'verify_gets', 'verify_hits',
            'subsequent_hits', 'subsequent_misses'), 0)
    started = clock()
    cohorts = []

    def data(namespace, indexes):
        keys = [disk.key_for(config['seed'], value) for value in indexes]
        return keys, [disk.payload(config['seed'], namespace, key, 0, config['length'])
                      for key in keys]

    def write(client, namespace, indexes, counter):
        keys, values = data(namespace, indexes)
        result['unresolved_operation'] = {'kind': counter, 'namespace_hex': namespace.hex(),
                                          'start_index': indexes[0], 'count': len(keys),
                                          'terminal': 'unknown-if-native-call-fails'}
        client.put(keys, values)
        ack = clock()
        result['unresolved_operation'] = None
        result['actual'][counter] += len(keys)
        result['put_payload_bytes'] += len(keys) * config['length']
        result['rows'].append({'kind': counter, 'namespace_hex': namespace.hex(),
                               'start_index': indexes[0], 'count': len(keys),
                               'ack_sec': ack - started})
        return ack

    def read(client, namespace, indexes, kind, *, strict=False, step=0):
        indexes = list(indexes)
        begin = clock()
        result['unresolved_operation'] = {'kind': kind, 'count': len(indexes),
                                          'namespace_hex': namespace.hex(),
                                          'terminal': 'unknown-if-native-call-fails'}
        outcomes = client.read_outcomes(*data(namespace, indexes))
        if (not isinstance(outcomes, list) or len(outcomes) != len(indexes)
                or any(row.get('outcome') not in ('get_hit', 'get_miss')
                       or row.get('status') != (0 if row['outcome'] == 'get_hit' else 1)
                       for row in outcomes)):
            raise RuntimeError('invalid/unresolved GET outcomes')
        result['unresolved_operation'] = None
        hits = sum(row['outcome'] == 'get_hit' for row in outcomes)
        result['actual'][kind + '_gets'] += len(indexes)
        row = {'kind': kind, 'namespace_hex': namespace.hex(),
               'start_index': indexes[0], 'count': len(indexes), 'step': step,
               'started_sec': begin - started, 'finished_sec': clock() - started,
               'hits': hits, 'misses': len(indexes) - hits}
        result['rows'].append(row)
        if kind == 'hot':
            # Every hot key is observed in both modes. Refill verification is
            # extra traffic, never a replacement for the original GET outcome.
            row['hot_outcomes'] = []
            summary = result.get('hot_refill')
            for index, outcome in zip(indexes, outcomes):
                terminal = outcome['outcome']
                identity = (namespace, index)
                seen = identity in lost_hot
                if seen and hot_refill:
                    summary['subsequent_hits' if terminal == 'get_hit' else 'subsequent_misses'] += 1
                event = {'index': index, 'outcome': terminal,
                         'refill_ack': False, 'ack_sec': None, 'verify_outcome': None,
                         'started_sec': row['finished_sec'],
                         'finished_sec': row['finished_sec']}
                row['hot_outcomes'].append(event)
                if terminal == 'get_miss':
                    result['hot_read_summary']['repeat_misses' if seen else 'distinct_lost_keys'] += 1
                    lost_hot.add(identity)
                    if not hot_refill:
                        continue
                    summary['repeat_misses' if seen else 'distinct_lost_keys'] += 1
                    keys, values = data(namespace, [index])
                    event['started_sec'] = clock() - started
                    result['unresolved_operation'] = {
                        'kind': 'hot_refill', 'namespace_hex': namespace.hex(), 'index': index,
                        'terminal': 'unknown-if-native-call-fails'}
                    client.put(keys, values)
                    result['unresolved_operation'] = None
                    event['ack_sec'] = clock() - started
                    summary['refill_puts'] += 1
                    summary['refill_payload_bytes'] += config['length']
                    event['refill_ack'] = True
                    result['unresolved_operation'] = {
                        'kind': 'hot_verify', 'namespace_hex': namespace.hex(), 'index': index,
                        'terminal': 'unknown-if-native-call-fails'}
                    observed = client.read_outcome(keys[0], values[0])
                    result['unresolved_operation'] = None
                    summary['verify_gets'] += 1
                    event['verify_outcome'] = observed
                    event['finished_sec'] = clock() - started
                    if observed != 'get_hit':
                        raise RuntimeError('hot refill verification failed')
                    summary['verify_hits'] += 1
        if strict and hits != len(indexes):
            raise RuntimeError('initial warmup miss invalidates initial state')

    def sample_evictions(kind):
        if eviction_counter is None:
            raise RuntimeError('eviction counter callback required')
        value = eviction_counter()
        if (type(value) not in (int, float) or not math.isfinite(value)
                or value < 0 or int(value) != value):
            raise RuntimeError('invalid eviction counter')
        result['pressure_samples'].append({'kind': kind, 'counter': value,
                                           'sampled_sec': clock() - started})
        return value

    def first_reads():
        due = any(clock() >= ack + cohort['delay_sec']
                  for cohort in cohorts for _, ack in cohort['pending'])
        if not due:
            return
        evictions = sample_evictions('before-first') - result['evictions_at_measurement_start']
        if evictions <= 0:
            raise RuntimeError('first reuse occurred without measured eviction pressure')
        for cohort in cohorts:
            client, namespace = clients[cohort['client_index']], cohort['namespace']
            pending = []
            for index, ack in cohort['pending']:
                if clock() < ack + cohort['delay_sec']:
                    pending.append((index, ack))
                    continue
                key, values = data(namespace, [index])
                begin = clock()
                age = begin - ack
                if (age - cohort['delay_sec'] > result['max_lateness_sec']
                        or (cohort['delay_sec'] < config['protect_sec']
                            and age >= config['protect_sec'])):
                    raise RuntimeError('first-reuse scheduling window missed')
                result['unresolved_operation'] = {'kind': 'first', 'namespace_hex': namespace.hex(),
                                                  'index': index, 'terminal': 'unknown-if-native-call-fails'}
                outcome = client.read_outcome(key[0], values[0])
                if outcome not in ('get_hit', 'get_miss'):
                    raise RuntimeError('unresolved first GET')
                result['unresolved_operation'] = None
                result['actual']['first_gets'] += 1
                row = {'kind': 'first', 'namespace_hex': namespace.hex(),
                       'key_hex': key[0].hex(), 'index': index,
                       'target_age_sec': cohort['delay_sec'], 'ack_sec': ack - started,
                       'eviction_delta_before_first': evictions,
                       'eviction_sample': len(result['pressure_samples']) - 1,
                       'started_sec': begin - started, 'age_sec': begin - ack,
                       'lateness_sec': begin - ack - cohort['delay_sec'],
                       'outcome': outcome, 'refill_ack': False, 'verify_outcome': None}
                result['rows'].append(row)
                if outcome == 'get_miss':
                    result['unresolved_operation'] = {'kind': 'refill', 'key_hex': key[0].hex(),
                                                      'terminal': 'unknown-if-native-call-fails'}
                    client.put(key, values)
                    result['unresolved_operation'] = None
                    result['refill_puts'] += 1
                    result['refill_payload_bytes'] += config['length']
                    row['refill_ack'] = True
                    result['unresolved_operation'] = {'kind': 'verify', 'key_hex': key[0].hex(),
                                                      'terminal': 'unknown-if-native-call-fails'}
                    observed = client.read_outcome(key[0], values[0])
                    result['unresolved_operation'] = None
                    result['verify_gets'] += 1
                    row['verify_outcome'] = observed
                    if observed != 'get_hit':
                        raise RuntimeError('refill verification failed')
                    result['verify_hits'] += 1
            cohort['pending'] = pending

    def reuse_reads(cutoff):
        # The sweep shares one pressure observation. In the terminal sweep the
        # cutoff is frozen: processing time cannot admit more delayed keys.
        due = [item for item in pending_reuse if item['ack_sec'] + item['target_age_sec'] <= cutoff]
        if not due:
            return
        delta = sample_evictions('before-first') - result['evictions_at_measurement_start']
        if delta <= 0:
            raise RuntimeError('continuous reuse without measured eviction pressure')
        for item in due:
            client = clients[item['client_index']]
            namespace = bytes.fromhex(item['namespace_hex'])
            keys, values = data(namespace, [item['index']])
            begin = clock() - started
            age = begin - item['ack_sec']
            if (age - item['target_age_sec'] > result['max_lateness_sec']
                    or item['target_age_sec'] < config['protect_sec'] <= age):
                raise RuntimeError('continuous reuse scheduling window missed')
            row = {key: value for key, value in item.items() if key != 'client_index'}
            row.update(kind='reuse', key_hex=keys[0].hex(), started_sec=begin,
                       age_sec=age, lateness_sec=age - item['target_age_sec'],
                       eviction_delta_before_first=delta,
                       eviction_sample=len(result['pressure_samples']) - 1,
                       refill_ack=False, refill_ack_sec=None, verify_outcome=None)
            result['unresolved_operation'] = dict(kind='reuse', index=item['index'],
                namespace_hex=item['namespace_hex'], terminal='unknown-if-native-call-fails')
            outcome = client.read_outcome(keys[0], values[0])
            if outcome not in ('get_hit', 'get_miss'):
                raise RuntimeError('unresolved continuous GET')
            result['unresolved_operation'] = None
            row.update(outcome=outcome, read_finished_sec=clock() - started)
            result['rows'].append(row)
            summary = result['continuous_reuse']
            counters = (summary, summary['by_delay'][str(item['target_age_sec'])])
            for counter in counters:
                counter['completed'] += 1
                counter['hits' if outcome == 'get_hit' else 'misses'] += 1
            if outcome == 'get_miss':
                result['unresolved_operation'] = dict(kind='reuse_refill', index=item['index'],
                    namespace_hex=item['namespace_hex'], terminal='unknown-if-native-call-fails')
                client.put(keys, values)
                result['unresolved_operation'] = None
                row.update(refill_ack=True, refill_ack_sec=clock() - started)
                for counter in counters:
                    counter['refill_puts'] += 1
                    counter['refill_payload_bytes'] += config['length']
                result['unresolved_operation'] = dict(kind='reuse_verify', index=item['index'],
                    namespace_hex=item['namespace_hex'], terminal='unknown-if-native-call-fails')
                observed = client.read_outcome(keys[0], values[0])
                result['unresolved_operation'] = None
                row['verify_outcome'] = observed
                for counter in counters:
                    counter['verify_gets'] += 1
                if observed != 'get_hit':
                    raise RuntimeError('continuous refill verification failed')
                for counter in counters:
                    counter['verify_hits'] += 1
            row['finished_sec'] = (clock() - started if outcome == 'get_miss'
                                   else row['read_finished_sec'])
            pending_reuse.remove(item)

    try:
        if len(clients) != len(disk.NAMESPACES):
            raise ValueError('exactly two namespace clients required')
        result['transport_modes'] = [client.transport for client in clients]
        if result['transport_modes'] != ['rdma', 'rdma']:
            raise RuntimeError('RDMA required; no TCP fallback')
        size = config['cohort_size']
        # Filler first: no cohort ages while a large initial dataset is built.
        for client, namespace in zip(clients, disk.NAMESPACES):
            for start in range(5 * size, config['prefill'], 16):
                write(client, namespace, range(start, min(start + 16, config['prefill'])),
                      'initial_puts')
        def put_cohorts(groups):
            for client_index, (client, namespace) in enumerate(zip(clients, disk.NAMESPACES)):
                for group in groups:
                    pending = []
                    for start in range(group * size, (group + 1) * size, 16):
                        indexes = list(range(start, min(start + 16, (group + 1) * size)))
                        ack = write(client, namespace, indexes, 'initial_puts')
                        pending.extend((index, ack) for index in indexes)
                    if group >= 2:
                        cohorts.append({'client_index': client_index, 'namespace': namespace,
                                        'delay_sec': config['delays_sec'][group - 2],
                                        'pending': pending})
        put_cohorts(range(2))
        for warm_round in range(2):
            for client, namespace in zip(clients, disk.NAMESPACES):
                read(client, namespace, range(2 * size), 'warm', strict=True, step=warm_round)
        # Age existing residents BEFORE delayed objects arrive. Only the hot set
        # is refreshed; the formerly hot set and untouched filler become cold.
        result['preage_started_sec'] = clock() - started
        for step in range(config['preage_sec']):
            begin = clock()
            for client, namespace in zip(clients, disk.NAMESPACES):
                read(client, namespace, range(size), 'preage', strict=True, step=step)
            remaining = begin + 1 - clock()
            if remaining > 0:
                sleep(remaining)
            if clock() - begin > result['max_step_sec']:
                raise RuntimeError('preage step/stall exceeded scheduling window')
            result['preage_steps'].append({'step': step, 'started_sec': begin - started,
                                           'finished_sec': clock() - started})
        result['preage_finished_sec'] = clock() - started
        put_cohorts(range(2, 5))
        for client, namespace in zip(clients, disk.NAMESPACES):
            read(client, namespace, range(size), 'ready', strict=True)
        result['evictions_at_measurement_start'] = sample_evictions('measurement-start')
        result['measurement_started_sec'] = clock() - started
        for step in range(config['steps']):
            step_start = clock()
            first_reads()
            if continuous:
                reuse_reads(clock() - started)
            start = config['prefill'] + step * config['arrival_batch']
            for client_index, (client, namespace) in enumerate(zip(clients, disk.NAMESPACES)):
                ack = write(client, namespace, range(start, start + config['arrival_batch']), 'arrival_puts')
                if continuous:
                    summary = result['continuous_reuse']
                    for offset, delay in enumerate(config['delays_sec']):
                        pending_reuse.append(dict(client_index=client_index, namespace_hex=namespace.hex(),
                            index=start + offset, source_step=step, target_age_sec=delay, ack_sec=ack - started))
                        summary['enrolled'] += 1
                        summary['by_delay'][str(delay)]['enrolled'] += 1
                read(client, namespace, range(size), 'hot', step=step)
            first_reads()
            if continuous:
                reuse_reads(clock() - started)
            remaining = step_start + config['interval_sec'] - clock()
            if clock() - step_start > result['max_step_sec']:
                raise RuntimeError('measurement step exceeded bounded scheduling window')
            if remaining > 0:
                sleep(remaining)
            if clock() - step_start > result['max_step_sec']:
                raise RuntimeError('measurement sleep/stall exceeded scheduling window')
            result['steps'].append({'step': step, 'started_sec': step_start - started,
                                    'finished_sec': clock() - started})
        if continuous:
            cutoff = result['steps'][-1]['finished_sec']
            result['continuous_reuse']['cutoff_sec'] = cutoff
            reuse_reads(cutoff)
            if clock() - started - cutoff > result['max_step_sec']:
                raise RuntimeError('continuous terminal sweep exceeded bounded window')
            for item in pending_reuse:
                censored = {key: value for key, value in item.items() if key != 'client_index'}
                result['continuous_reuse']['censored_keys'].append(censored)
                result['continuous_reuse']['censored'] += 1
                result['continuous_reuse']['by_delay'][str(item['target_age_sec'])]['censored'] += 1
        first_reads()
        for client, namespace in zip(clients, disk.NAMESPACES):
            read(client, namespace, range(size, 2 * size), 'cooled')
        if result['actual'] != planned or any(cohort['pending'] for cohort in cohorts):
            raise RuntimeError('incomplete planned denominator')
        result['passed'] = True
    except Exception as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
    finally:
        # Both durations describe the same terminal instant, even if this
        # thread is preempted while assigning the report fields.
        finished = clock()
        result['elapsed_sec'] = finished - started
        result['measurement_elapsed_sec'] = (finished - started - result['measurement_started_sec']
                                             if 'measurement_started_sec' in result else None)
    return result


def check_reuse(result, config, acknowledgments):
    """Reconstruct every sampled arrival key, including the unobserved tail."""
    summary = reuse_summary(config)
    actual = result.get('continuous_reuse', {})
    cutoff = actual.get('cutoff_sec')
    def seconds(value):
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError('invalid continuous timing')
        return value
    seconds(cutoff)
    if cutoff != result['steps'][-1]['finished_sec'] or cutoff > result['elapsed_sec']:
        raise ValueError('continuous cutoff differs from terminal measurement boundary')
    expected = {}
    for namespace in disk.NAMESPACES:
        for step in range(config['steps']):
            for offset, delay in enumerate(config['delays_sec']):
                index = config['prefill'] + step * config['arrival_batch'] + offset
                identity = (namespace.hex(), index)
                expected[identity] = dict(namespace_hex=namespace.hex(), index=index,
                    source_step=step, target_age_sec=delay, ack_sec=acknowledgments[identity])
                summary['enrolled'] += 1
                summary['by_delay'][str(delay)]['enrolled'] += 1
    seen = set()
    previous_end = result['measurement_started_sec']
    for row in result['rows']:
        if row['kind'] != 'reuse':
            continue
        index = row['index']
        if type(index) is not int:
            raise ValueError('invalid continuous index')
        identity = (row['namespace_hex'], index)
        item = expected.get(identity)
        if identity in seen or item is None or any(row.get(key) != value for key, value in item.items()):
            raise ValueError('duplicate/unknown continuous identity or ACK')
        if (type(row['source_step']) is not int or type(row['target_age_sec']) is not int
                or type(row['ack_sec']) not in (int, float)):
            raise ValueError('invalid continuous source step')
        seen.add(identity)
        begin, end = seconds(row['started_sec']), seconds(row['finished_sec'])
        age, late = seconds(row['age_sec']), seconds(row['lateness_sec'])
        read_end = seconds(row['read_finished_sec'])
        due = item['ack_sec'] + item['target_age_sec']
        if (row['key_hex'] != disk.key_for(config['seed'], index).hex()
                or due > cutoff or begin < due or late > result['max_lateness_sec']
                or not result['measurement_started_sec'] <= previous_end <= begin <= read_end <= end <= result['elapsed_sec']
                or end > cutoff + result['max_step_sec']
                or abs(begin - item['ack_sec'] - age) > 1e-6
                or abs(age - item['target_age_sec'] - late) > 1e-6
                or item['target_age_sec'] < config['protect_sec'] <= age):
            raise ValueError('invalid continuous identity/age/phase')
        previous_end = end
        sample_index = row['eviction_sample']
        if type(sample_index) is not int or not 1 <= sample_index < len(result['pressure_samples']):
            raise ValueError('invalid continuous pressure identity')
        sample = result['pressure_samples'][sample_index]
        delta = row['eviction_delta_before_first']
        if (sample['kind'] != 'before-first' or sample['sampled_sec'] > begin
                or type(delta) not in (int, float) or not math.isfinite(delta) or delta <= 0
                or delta != sample['counter'] - result['evictions_at_measurement_start']):
            raise ValueError('invalid continuous pressure evidence')
        outcome = row['outcome']
        if outcome not in ('get_hit', 'get_miss'):
            raise ValueError('unresolved continuous GET')
        miss = outcome == 'get_miss'
        if (row['refill_ack'] is not miss or row['verify_outcome'] != ('get_hit' if miss else None)
                or miss and not read_end <= seconds(row['refill_ack_sec']) <= end
                or not miss and (row['refill_ack_sec'] is not None or read_end != end)):
            raise ValueError('invalid continuous refill/verify')
        for counter in (summary, summary['by_delay'][str(item['target_age_sec'])]):
            counter['completed'] += 1
            counter['misses' if miss else 'hits'] += 1
            counter['refill_puts'] += miss
            counter['refill_payload_bytes'] += config['length'] if miss else 0
            counter['verify_gets'] += miss
            counter['verify_hits'] += miss
    for row in actual.get('censored_keys', []):
        index = row.get('index')
        if (type(index) is not int or type(row.get('source_step')) is not int
                or type(row.get('target_age_sec')) is not int
                or type(row.get('ack_sec')) not in (int, float)):
            raise ValueError('invalid censored identity')
        identity = (row.get('namespace_hex'), index)
        item = expected.get(identity)
        if (identity in seen or item is None or row != item
                or item['ack_sec'] + item['target_age_sec'] <= cutoff):
            raise ValueError('invalid or already due right-censored key')
        seen.add(identity)
        summary['censored_keys'].append(item)
        summary['censored'] += 1
        summary['by_delay'][str(item['target_age_sec'])]['censored'] += 1
    summary['cutoff_sec'] = cutoff
    if seen != expected.keys() or actual != summary:
        raise ValueError('incomplete continuous enrollment/outcome/censor denominator')
    for counters in (actual, *actual['by_delay'].values()):
        if any(type(value) is not int for key, value in counters.items()
               if key not in ('by_delay', 'censored_keys', 'cutoff_sec')):
            raise ValueError('invalid continuous counter types')
    return summary


def check_report(result, config):
    """Validate complete diagnostic denominators; return metric-comparison totals."""
    config = validate(config)
    if (result.get('model') != 'long-residence-rdma-v1' or result.get('config') != config
            or result.get('passed') is not True or result.get('errors')
            or result.get('close_errors', []) or result.get('formal_ab') is not False
            or result.get('unresolved_operation') is not None
            or result.get('max_lateness_sec') != 2 or result.get('max_step_sec') != 5
            or result.get('transport_modes') != ['rdma', 'rdma']):
        raise ValueError('failed or mismatched residence report')
    planned = expected_counts(config)
    if result.get('planned') != planned or result.get('actual') != planned:
        raise ValueError('incomplete planned denominators')
    observed = dict.fromkeys(planned, 0)
    first, write_indexes, read_ids = set(), set(), set()
    acknowledgments = {}
    get_hits = get_misses = refill_puts = verify_gets = verify_hits = 0
    hot_enabled = config.get('hot_refill', False)
    # Old no-refill reports have no per-key ledger. Accept that complete old
    # schema, but never a partially stripped new ledger or refill-mode report.
    hot_ledger = 'hot_read_summary' in result
    if hot_enabled and not hot_ledger:
        raise ValueError('missing hot read summary')
    hot_losses = {'distinct_lost_keys': 0, 'repeat_misses': 0}
    hot_summary = dict.fromkeys((
        'distinct_lost_keys', 'repeat_misses', 'refill_puts',
        'refill_payload_bytes', 'verify_gets', 'verify_hits',
        'subsequent_hits', 'subsequent_misses'), 0)
    lost_hot = set()
    hot_next_step = {namespace.hex(): 0 for namespace in disk.NAMESPACES}
    if not hot_enabled and 'hot_refill' in result:
        raise ValueError('unexpected hot refill mode')
    def seconds(value):
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError('invalid/nonfinite timing')
        return value

    pressure = result['pressure_samples']
    if not pressure or pressure[0]['kind'] != 'measurement-start':
        raise ValueError('missing pressure baseline')
    for sample in pressure:
        seconds(sample['sampled_sec'])
        value = sample['counter']
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or int(value) != value:
            raise ValueError('invalid pressure sample')
    if pressure[0]['counter'] != result['evictions_at_measurement_start']:
        raise ValueError('pressure baseline mismatch')
    seconds(result['elapsed_sec'])
    seconds(result['measurement_started_sec'])
    seconds(result['measurement_elapsed_sec'])
    previous = seconds(result['preage_started_sec'])
    if len(result['preage_steps']) != config['preage_sec']:
        raise ValueError('incomplete resident preaging')
    for index, step in enumerate(result['preage_steps']):
        begin, end = seconds(step['started_sec']), seconds(step['finished_sec'])
        if (step['step'] != index or abs(begin - previous) > .01
                or end - begin < 1 - 1e-6 or end - begin > result['max_step_sec']):
            raise ValueError('preage continuity/stall violation')
        previous = end
    if (abs(previous - seconds(result['preage_finished_sec'])) > .01
            or previous > result['measurement_started_sec']):
        raise ValueError('preage lifecycle order violation')
    previous = result['measurement_started_sec']
    if len(result['steps']) != config['steps']:
        raise ValueError('incomplete measurement steps')
    for index, step in enumerate(result['steps']):
        begin, end = seconds(step['started_sec']), seconds(step['finished_sec'])
        if (step['step'] != index or abs(begin - previous) > .01
                or end - begin < config['interval_sec'] - 1e-6
                or end - begin > result['max_step_sec']):
            raise ValueError('measurement continuity/stall violation')
        previous = end
    for row in result['rows']:
        namespace = row['namespace_hex']
        if namespace not in {value.hex() for value in disk.NAMESPACES}:
            raise ValueError('unknown namespace')
        kind = row['kind']
        if kind in ('initial_puts', 'arrival_puts'):
            start, count = row['start_index'], row['count']
            ack = seconds(row['ack_sec'])
            if type(start) is not int or type(count) is not int or count < 1:
                raise ValueError('invalid PUT row')
            for index in range(start, start + count):
                identity = (namespace, index)
                if identity in write_indexes:
                    raise ValueError('duplicate initial/arrival PUT')
                write_indexes.add(identity)
                acknowledgments[identity] = ack
            low, high = ((0, config['prefill']) if kind == 'initial_puts' else
                         (config['prefill'], config['prefill'] +
                          config['steps'] * config['arrival_batch']))
            if not low <= start < start + count <= high:
                raise ValueError('PUT indexes outside frozen workload')
            if kind == 'initial_puts':
                if (ack > result['measurement_started_sec']
                        or (start < 2 * config['cohort_size'] and ack > result['preage_started_sec'])
                        or (2 * config['cohort_size'] <= start < 5 * config['cohort_size']
                            and ack < result['preage_finished_sec'])):
                    raise ValueError('initial ACK after measurement start')
            else:
                step = (start - config['prefill']) // config['arrival_batch']
                if (count != config['arrival_batch']
                        or (start - config['prefill']) % config['arrival_batch']
                        or not result['steps'][step]['started_sec'] <= ack <= result['steps'][step]['finished_sec']):
                    raise ValueError('arrival ACK outside frozen measurement step')
            observed[kind] += count
        elif kind in ('warm', 'preage', 'ready', 'hot', 'cooled'):
            hits, misses = row['hits'], row['misses']
            begin, end = seconds(row['started_sec']), seconds(row['finished_sec'])
            step = row['step']
            identity = (namespace, kind, step)
            expected_start = config['cohort_size'] if kind == 'cooled' else 0
            step_count = {'warm': 2, 'preage': config['preage_sec'],
                          'hot': config['steps'], 'ready': 1, 'cooled': 1}[kind]
            if (type(step) is not int or identity in read_ids
                    or not 0 <= step < step_count
                    or row['start_index'] != expected_start
                    or row['count'] != config['cohort_size'] * (2 if kind == 'warm' else 1)):
                raise ValueError('duplicate/missing batch GET identity')
            read_ids.add(identity)
            if ((kind == 'warm' and end > result['preage_started_sec'])
                    or (kind == 'ready' and not result['preage_finished_sec'] <= begin <= end <= result['measurement_started_sec'])
                    or (kind == 'cooled' and begin < previous)):
                raise ValueError('batch read outside lifecycle phase')
            if kind == 'preage' and not (result['preage_steps'][step]['started_sec'] <= begin <= end
                                                <= result['preage_steps'][step]['finished_sec']):
                raise ValueError('hot refresh outside preage step')
            if kind == 'hot' and not (result['steps'][step]['started_sec'] <= begin <= end
                                             <= result['steps'][step]['finished_sec']):
                raise ValueError('hot read outside measurement step')
            if (end > result['elapsed_sec']
                    or type(hits) is not int or type(misses) is not int or min(hits, misses) < 0
                    or hits + misses != config['cohort_size'] * (2 if kind == 'warm' else 1)
                    or row['finished_sec'] < row['started_sec']
                    or (kind in ('warm', 'preage', 'ready') and misses)):
                raise ValueError('invalid batch GET denominator')
            if kind == 'hot' and hot_ledger:
                if step != hot_next_step[namespace]:
                    raise ValueError('hot ledger reordered')
                hot_next_step[namespace] += 1
                events = row.get('hot_outcomes', [])
                if len(events) != config['cohort_size']:
                    raise ValueError('missing per-key hot outcomes')
                event_hits = 0
                cursor = end
                for index, event in enumerate(events):
                    terminal = event['outcome']
                    begin_event, end_event = seconds(event['started_sec']), seconds(event['finished_sec'])
                    miss = terminal == 'get_miss'
                    refill = miss and hot_enabled
                    ack = seconds(event['ack_sec']) if refill else event['ack_sec']
                    if (type(event['index']) is not int or event['index'] != index
                            or terminal not in ('get_hit', 'get_miss')
                            or event['refill_ack'] is not refill
                            or event['verify_outcome'] != ('get_hit' if refill else None)
                            or (refill and not cursor <= begin_event <= ack <= end_event
                                <= result['steps'][step]['finished_sec'])
                            or (not refill and (ack is not None or begin_event != end or end_event != end))):
                        raise ValueError('invalid hot identity/refill/phase')
                    identity = (namespace, index)
                    seen = identity in lost_hot
                    if seen and hot_enabled:
                        hot_summary['subsequent_misses' if miss else 'subsequent_hits'] += 1
                    if miss:
                        hot_losses['repeat_misses' if seen else 'distinct_lost_keys'] += 1
                        lost_hot.add(identity)
                    if refill:
                        hot_summary['repeat_misses' if seen else 'distinct_lost_keys'] += 1
                        hot_summary['refill_puts'] += 1
                        hot_summary['refill_payload_bytes'] += config['length']
                        hot_summary['verify_gets'] += 1
                        hot_summary['verify_hits'] += 1
                        cursor = end_event
                    event_hits += not miss
                if event_hits != hits:
                    raise ValueError('hot outcomes disagree with batch totals')
            elif 'hot_outcomes' in row:
                raise ValueError('unexpected hot outcome ledger')
            observed[kind + '_gets'] += hits + misses
            get_hits += hits
            get_misses += misses
        elif kind == 'reuse':
            if not config.get('continuous_reuse', False):
                raise ValueError('unexpected continuous reuse ledger')
        elif kind == 'first':
            index = row['index']
            if type(index) is not int:
                raise ValueError('invalid first-reuse index')
            for field in ('ack_sec', 'started_sec', 'age_sec', 'lateness_sec'):
                seconds(row[field])
            group = index // config['cohort_size'] - 2
            if group not in (0, 1, 2):
                raise ValueError('first reuse outside cohort')
            identity = (namespace, index)
            target = config['delays_sec'][group]
            eviction_delta = row['eviction_delta_before_first']
            sample_index = row['eviction_sample']
            if type(sample_index) is not int or not 1 <= sample_index < len(pressure):
                raise ValueError('invalid pressure sample identity')
            sample = pressure[sample_index]
            if (sample['kind'] != 'before-first' or sample['sampled_sec'] > row['started_sec']
                    or sample['counter'] - pressure[0]['counter'] != eviction_delta):
                raise ValueError('first pressure delta does not match sampled counter')
            if (type(eviction_delta) not in (int, float)
                    or not math.isfinite(eviction_delta) or eviction_delta <= 0):
                raise ValueError('missing first-reuse pressure evidence')
            if (identity in first or row['target_age_sec'] != target
                    or row['key_hex'] != disk.key_for(config['seed'], index).hex()
                    or not result['measurement_started_sec'] <= row['started_sec'] <= previous
                    or row['age_sec'] < target
                    or acknowledgments.get(identity) != row['ack_sec']
                    or row['lateness_sec'] > result['max_lateness_sec']
                    or (target < config['protect_sec'] and row['age_sec'] >= config['protect_sec'])
                    or abs(row['started_sec'] - row['ack_sec'] - row['age_sec']) > 1e-6
                    or abs(row['age_sec'] - target - row['lateness_sec']) > 1e-6
                    or row['outcome'] not in ('get_hit', 'get_miss')):
                raise ValueError('invalid first-reuse identity/age')
            first.add(identity)
            observed['first_gets'] += 1
            miss = row['outcome'] == 'get_miss'
            get_hits += not miss
            get_misses += miss
            if row['refill_ack'] is not miss or row['verify_outcome'] != ('get_hit' if miss else None):
                raise ValueError('refill must be separate verified same-byte PUT')
            refill_puts += miss
            verify_gets += miss
            verify_hits += miss
        else:
            raise ValueError('unknown ledger kind')
    if observed != planned:
        raise ValueError('ledger differs from denominators')
    if (result['put_payload_bytes'] != (planned['initial_puts'] + planned['arrival_puts']) * config['length']
            or result['refill_puts'] != refill_puts
            or result['refill_payload_bytes'] != refill_puts * config['length']
            or result['verify_gets'] != verify_gets or result['verify_hits'] != verify_hits
            or result['measurement_elapsed_sec'] < config['steps'] * config['interval_sec']
            or previous > result['elapsed_sec']
            or abs(result['measurement_elapsed_sec'] + result['measurement_started_sec']
                   - result['elapsed_sec']) > 1e-6):
        raise ValueError('incomplete byte/refill/time denominator')
    if hot_ledger and (result['hot_read_summary'] != hot_losses
                       or any(type(value) is not int for value in result['hot_read_summary'].values())):
        raise ValueError('incomplete per-key hot loss denominator')
    if hot_enabled and (result.get('hot_refill') != hot_summary
                        or any(type(value) is not int for value in result['hot_refill'].values())):
        raise ValueError('incomplete hot refill denominator')
    continuous_counts = check_reuse(result, config, acknowledgments) if config.get('continuous_reuse', False) else None
    if not config.get('continuous_reuse', False) and 'continuous_reuse' in result:
        raise ValueError('unexpected continuous reuse summary')
    if continuous_counts:
        get_hits += continuous_counts['hits'] + continuous_counts['verify_hits']
        get_misses += continuous_counts['misses']
        refill_puts += continuous_counts['refill_puts']
        # Bytes use the separate continuous summary below, not first-cohort totals.
    return {'put_acks': planned['initial_puts'] + planned['arrival_puts'] + refill_puts + hot_summary['refill_puts'],
            'put_bytes': result['put_payload_bytes'] + result['refill_payload_bytes'] + hot_summary['refill_payload_bytes']
                         + (continuous_counts['refill_payload_bytes'] if continuous_counts else 0),
            'get_hits': get_hits + verify_hits + hot_summary['verify_hits'], 'get_misses': get_misses,
            'get_attempts': get_hits + get_misses + verify_gets + hot_summary['verify_gets']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lib', required=True)
    parser.add_argument('--members', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--hot-refill', action='store_true',
                        help='refill hot misses and verify separately; diagnostic PUT bytes, not device amplification')
    parser.add_argument('--continuous-reuse', action='store_true',
                        help='sample three arrival keys per namespace/step for delayed first reuse; censor at cutoff')
    parser.add_argument('--metrics-port', type=int, default=12010)
    parser.add_argument('--profile', choices=('accelerated', 'default'), required=True)
    parser.add_argument('--pressure-case', choices=PRESSURE_CASES,
                        help='opt-in default-score pressure/P diagnostic (660 steps unless overridden)')
    parser.add_argument('--pressure-steps', type=int, choices=(660, 1800),
                        help='explicit measurement window; requires --pressure-case')
    args = parser.parse_args(argv)
    if not 1 <= args.metrics_port <= 65535:
        parser.error('invalid metrics port')
    try:
        config = profile(args.profile, hot_refill=args.hot_refill, pressure_case=args.pressure_case,
                         pressure_steps=args.pressure_steps, continuous_reuse=args.continuous_reuse)
    except ValueError as error:
        parser.error(str(error))
    args.output.mkdir(parents=True, exist_ok=False)
    clients = []
    result = {'passed': False, 'config': config, 'errors': [], 'close_errors': []}
    try:
        library = disk.load_library(args.lib)
        if (getattr(library, 'dfkv_get_auto_status', None) is None
                or getattr(library, 'dfkv_batch_get_auto_status', None) is None):
            raise RuntimeError('scalar and batch status symbols required')
        for namespace in disk.NAMESPACES:
            clients.append(disk.Client(library, args.members, namespace))
        metric_samples = []
        def evictions():
            begin = time.monotonic()
            text = baseline._scrape(args.metrics_port)
            name = f'metrics-{len(metric_samples):03d}.prom'
            (args.output / name).write_text(text)
            metric_samples.append({'file': name, 'sha256': baseline.file_sha(args.output / name),
                                   'started_monotonic_sec': begin,
                                   'finished_monotonic_sec': time.monotonic()})
            (args.output / 'metrics-samples.json').write_text(json.dumps(metric_samples, indent=2) + '\n')
            for metric in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
                if quick.total(text, metric) != 1:
                    raise RuntimeError('unhealthy server before first reuse')
            return quick.total(text, 'dfkv_evictions_total')
        result = run(clients, config, eviction_counter=evictions)
        result['close_errors'] = []
    except Exception as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
    finally:
        for client in reversed(clients):
            try:
                client.close()
            except Exception as error:
                result['close_errors'].append(f'{type(error).__name__}: {error}')
                result['passed'] = False
        (args.output / 'report.json').write_text(json.dumps(result, indent=2) + '\n')
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
