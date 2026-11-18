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
MULTISIZE_COUNT = 6400
MULTISIZE_HOT_READS = 23552
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


def check_steps(client, first_reuse_sec=0):
    steps = client.get('steps')
    if not isinstance(steps, list) or len(steps) != SUSTAINED_STEPS:
        raise RuntimeError('sustained step count mismatch')
    previous_end = 0
    previous_start = None
    gaps = []
    hot_hits = 0 if first_reuse_sec else 128
    first_probe = None
    hot_misses = 0
    lateness = []
    durations = []
    for index, row in enumerate(steps):
        if not isinstance(row, dict) or type(row.get('step')) is not int or row['step'] != index:
            raise RuntimeError('sustained step index mismatch')
        values = [row.get(name) for name in ('cold_written', 'hot_hits', 'hot_misses')]
        if (any(type(value) is not int or value < 0 for value in values) or
                values[0] != 32 or values[1] + values[2] not in ((0, 64) if first_reuse_sec else (64,))):
            raise RuntimeError('sustained step operation mismatch')
        start, end = row.get('start_sec'), row.get('end_sec')
        if (any(type(value) not in (int, float) or not math.isfinite(value) or value < 0
                for value in (start, end)) or start < previous_end or end < start or start + 1e-6 < index):
            raise RuntimeError('sustained step timing mismatch')
        if first_reuse_sec:
            probe = row.get('hot_probe_started_sec')
            if values[1] + values[2]:
                if (type(probe) not in (int, float) or not math.isfinite(probe) or
                        not start <= probe <= end or probe < first_reuse_sec):
                    raise RuntimeError('early or invalid first reuse probe')
                if first_probe is None:
                    first_probe = row
            # Row assembly can cross the deadline after the driver decided to skip.
            elif probe is not None or start >= first_reuse_sec or first_probe is not None:
                raise RuntimeError('missing delayed hot probe')
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
    if first_reuse_sec:
        writes, probes = client.get('hot_cohort_write_completed_sec'), client.get('first_reuse')
        if (client.get('first_reuse_sec') != first_reuse_sec or
                client.get('filler_warmup_reads') != 18304 or first_probe is None or
                not isinstance(writes, list) or len(writes) != 2 or
                not isinstance(probes, list) or len(probes) != 2):
            raise RuntimeError('first reuse cohort metadata mismatch')
        for index, (written, probe) in enumerate(zip(writes, probes)):
            if not isinstance(probe, dict):
                raise RuntimeError('invalid first reuse metadata')
            at, age = probe.get('probe_started_sec'), probe.get('write_to_probe_sec')
            if (any(type(v) not in (int, float) or not math.isfinite(v) or v < 0
                    for v in (written, at, age)) or written > origin or
                    not origin + first_probe['hot_probe_started_sec'] - 1e-6 <= at <= origin + first_probe['end_sec'] or
                    abs(at - written - age) > 1e-6):
                raise RuntimeError('first reuse write age mismatch')
            expected_ns = (b'disk-validation\x00A\xff', b'disk-validation\x00B\xfe')[index].hex()
            if (probe.get('namespace_hex') != expected_ns or
                    any(type(probe.get(k)) is not int or probe[k] < 0 for k in ('hits', 'misses')) or
                    probe['hits'] + probe['misses'] != 32):
                raise RuntimeError('first reuse outcome mismatch')
        if (sum(p['hits'] for p in probes) != first_probe['hot_hits'] or
                sum(p['misses'] for p in probes) != first_probe['hot_misses']):
            raise RuntimeError('first reuse outcome disagrees with step')
    return {'sustained_steps': len(steps), 'schedule_elapsed_sec': elapsed, 'max_step_gap_sec': max(gaps),
            'max_step_lateness_sec': max(lateness), 'max_step_duration_sec': max(durations)}


def check_mixed(client):
    """Validate separate repeated-hot and once-only first-reuse cohorts."""
    schedule = check_steps(client)
    if client.get('filler_warmup_reads') != 18112:
        raise RuntimeError('mixed filler warmup mismatch')
    cohorts = client.get('reuse_cohorts')
    if not isinstance(cohorts, list) or len(cohorts) != 3:
        raise RuntimeError('mixed reuse cohort count mismatch')
    events = {}
    for row in client['steps']:
        probes = row.get('reuse_probes')
        if not isinstance(probes, list):
            raise RuntimeError('mixed reuse step probes missing')
        previous = row['start_sec']
        for probe in probes:
            if not isinstance(probe, dict):
                raise RuntimeError('invalid mixed reuse event')
            delay, at = probe.get('delay_sec'), probe.get('probe_started_sec')
            if (type(delay) is not int or delay not in (45, 90, 185) or delay in events or
                    type(at) not in (int, float) or not math.isfinite(at) or
                    not previous <= at <= row['end_sec'] or at < delay or
                    any(type(probe.get(k)) is not int or probe[k] < 0 for k in ('hits', 'misses')) or
                    probe['hits'] + probe['misses'] != 32):
                raise RuntimeError('mixed reuse event timing/count mismatch')
            previous = at
            events[delay] = (probe, row['end_sec'])
        # A skipped decision may precede a row end that crosses the deadline.
        if any(row['start_sec'] >= delay and delay not in events for delay in (45, 90, 185)):
            raise RuntimeError('mixed reuse cohort was not probed when due')
    if set(events) != {45, 90, 185}:
        raise RuntimeError('mixed reuse event missing')
    hits, misses = 0, 0
    origin = client['pressure_schedule_origin_sec']
    for index, (cohort, delay) in enumerate(zip(cohorts, (45, 90, 185))):
        if (not isinstance(cohort, dict) or cohort.get('delay_sec') != delay or
                cohort.get('start_index') != 32 + index * 16 or cohort.get('count_per_namespace') != 16):
            raise RuntimeError('mixed reuse cohort layout mismatch')
        writes, outcomes = cohort.get('write_completed_sec'), cohort.get('first_reuse')
        if (not isinstance(writes, list) or len(writes) != 2 or
                not isinstance(outcomes, list) or len(outcomes) != 2):
            raise RuntimeError('mixed reuse namespace cohort mismatch')
        event, end = events[delay]
        previous = origin + event['probe_started_sec']
        for ns, written, outcome in zip((b'disk-validation\x00A\xff', b'disk-validation\x00B\xfe'), writes, outcomes):
            if not isinstance(outcome, dict):
                raise RuntimeError('invalid mixed reuse outcome')
            at, age = outcome.get('probe_started_sec'), outcome.get('write_to_probe_sec')
            if (any(type(v) not in (int, float) or not math.isfinite(v) or v < 0
                    for v in (written, at, age)) or written > origin or
                    not previous - 1e-6 <= at <= origin + end or abs(at - written - age) > 1e-6 or
                    outcome.get('namespace_hex') != ns.hex() or
                    any(type(outcome.get(k)) is not int or outcome[k] < 0 for k in ('hits', 'misses')) or
                    outcome['hits'] + outcome['misses'] != 16):
                raise RuntimeError('mixed reuse namespace age/outcome mismatch')
            previous = at
        if any(sum(o[k] for o in outcomes) != event[k] for k in ('hits', 'misses')):
            raise RuntimeError('mixed reuse outcomes disagree with step')
        hits += event['hits']
        misses += event['misses']
    for field, expected in (('reuse_hits', hits), ('reuse_misses', misses), ('reuse_reads', 96)):
        if type(client.get(field)) is not int or client[field] != expected:
            raise RuntimeError('mixed reuse denominator mismatch')
    return {**schedule, 'reuse_hits': hits, 'reuse_misses': misses, 'reuse_reads': 96}


def check_arrivals(client):
    """Validate early arrival samples, using the later namespace ACK as origin."""
    schedule = check_steps(client)
    cohorts = client.get('reuse_cohorts')
    if (type(client.get('filler_warmup_reads')) is not int or
            client['filler_warmup_reads'] != 18304 or
            not isinstance(cohorts, list) or len(cohorts) != 12):
        raise RuntimeError('arrival cohort count/warmup mismatch')
    origin = client['pressure_schedule_origin_sec']
    for index, cohort in enumerate(cohorts):
        layout = {'admission_step': index, 'start_index': 4608 + index * 16,
                  'count_per_namespace': 16, 'delay_sec': (45, 90, 185)[index % 3]}
        if (not isinstance(cohort, dict) or any(type(cohort.get(k)) is not int or cohort[k] != v
                                               for k, v in layout.items())):
            raise RuntimeError('arrival cohort identity/layout mismatch')
        writes, outcomes = cohort.get('write_completed_sec'), cohort.get('first_reuse')
        if (not isinstance(writes, list) or len(writes) != 2 or
                not isinstance(outcomes, list) or len(outcomes) != 2):
            raise RuntimeError('arrival namespace cohort mismatch')
        row = client['steps'][index]
        due = cohort.get('due_sec')
        if (any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in (*writes, due)) or
                not origin + row['start_sec'] <= writes[0] <= writes[1] <= origin + row['end_sec'] or
                abs(due - (writes[1] - origin + cohort['delay_sec'])) > 1e-6):
            raise RuntimeError('arrival ACK/deadline mismatch')
    events = set()
    hits, misses = 0, 0
    for row in client['steps']:
        probes = row.get('reuse_probes')
        if not isinstance(probes, list):
            raise RuntimeError('arrival step probes missing')
        previous = row['start_sec']
        previous_id = -1
        for probe in probes:
            if not isinstance(probe, dict):
                raise RuntimeError('invalid arrival event')
            identity, at = probe.get('admission_step'), probe.get('probe_started_sec')
            if (type(identity) is not int or not 0 <= identity < 12 or identity in events or
                    identity > row['step'] or identity <= previous_id):
                raise RuntimeError('arrival event identity mismatch')
            cohort = cohorts[identity]
            if (type(probe.get('delay_sec')) is not int or probe['delay_sec'] != cohort['delay_sec'] or
                    type(at) not in (int, float) or not math.isfinite(at) or
                    not previous - 1e-6 <= at <= row['end_sec'] or at + 1e-6 < cohort['due_sec'] or
                    any(type(probe.get(k)) is not int or probe[k] < 0 for k in ('hits', 'misses')) or
                    probe['hits'] + probe['misses'] != 32):
                raise RuntimeError('arrival event timing/count mismatch')
            previous = origin + at
            for ns, written, outcome in zip((b'disk-validation\x00A\xff', b'disk-validation\x00B\xfe'),
                                             cohort['write_completed_sec'], cohort['first_reuse']):
                if not isinstance(outcome, dict):
                    raise RuntimeError('invalid arrival namespace outcome')
                when, age = outcome.get('probe_started_sec'), outcome.get('write_to_probe_sec')
                if (any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in (when, age)) or
                        not previous - 1e-6 <= when <= origin + row['end_sec'] or
                        abs(when - written - age) > 1e-6 or age + 1e-6 < cohort['delay_sec'] or
                        outcome.get('namespace_hex') != ns.hex() or
                        any(type(outcome.get(k)) is not int or outcome[k] < 0 for k in ('hits', 'misses')) or
                        outcome['hits'] + outcome['misses'] != 16):
                    raise RuntimeError('arrival namespace age/outcome mismatch')
                previous = when
            if any(sum(o[k] for o in cohort['first_reuse']) != probe[k] for k in ('hits', 'misses')):
                raise RuntimeError('arrival outcomes disagree with event')
            # The next event cannot start before the preceding namespace GET.
            previous -= origin
            previous_id = identity
            events.add(identity)
            hits += probe['hits']
            misses += probe['misses']
        # A decision can precede a row end crossing its deadline. Only row-start
        # deadlines prove a skipped due probe; cohorts must already be admitted.
        if any(c['admission_step'] <= row['step'] and row['start_sec'] >= c['due_sec'] and
               c['admission_step'] not in events for c in cohorts):
            raise RuntimeError('arrival cohort was not probed when due')
    if len(events) != 12:
        raise RuntimeError('arrival first reuse event missing')
    for field, expected in (('reuse_hits', hits), ('reuse_misses', misses), ('reuse_reads', 384)):
        if type(client.get(field)) is not int or client[field] != expected:
            raise RuntimeError('arrival reuse denominator mismatch')
    return {**schedule, 'reuse_hits': hits, 'reuse_misses': misses, 'reuse_reads': 384}


def check_refill(client):
    """Recompute sampled first outcomes, once-only refill and verification counts.

    Called after check_arrivals validates the schedule/cohort metadata. Records
    must follow actual event/namespace/key order, not admission order: delayed
    cohorts can become due out of order. Only complete acknowledged runs pass;
    a failed PUT has unknown completion and belongs in retained failure evidence.
    """
    stats = client.get('refill')
    if not isinstance(stats, dict) or not isinstance(stats.get('objects'), list) or len(stats['objects']) != 384:
        raise RuntimeError('refill object count mismatch')
    objects = iter(stats['objects'])
    first_hits, verify_hits, verify_misses = 0, 0, 0
    previous = 0
    origin = client['pressure_schedule_origin_sec']
    for step in client['steps']:
        for event in step['reuse_probes']:
            cohort = client['reuse_cohorts'][event['admission_step']]
            if origin + event['probe_started_sec'] + 1e-6 < previous:
                raise RuntimeError('refill event chronology mismatch')
            for ns_index, outcome in enumerate(cohort['first_reuse']):
                upper = (cohort['first_reuse'][ns_index + 1]['probe_started_sec'] if ns_index == 0 else
                         origin + step['end_sec'])
                namespace_hits = 0
                for offset in range(16):
                    row = next(objects)
                    if (not isinstance(row, dict) or row.get('namespace_hex') != outcome['namespace_hex'] or
                            type(row.get('index')) is not int or row['index'] != cohort['start_index'] + offset or
                            type(row.get('first_hit')) is not int or row['first_hit'] not in (0, 1)):
                        raise RuntimeError('refill object identity/first outcome mismatch')
                    at = row.get('first_get_started_sec')
                    if (type(at) not in (int, float) or not math.isfinite(at) or
                            not max(previous, outcome['probe_started_sec']) <= at <= upper or
                            at - cohort['write_completed_sec'][ns_index] + 1e-6 < cohort['delay_sec']):
                        raise RuntimeError('refill first GET timing/age mismatch')
                    miss = not row['first_hit']
                    if (type(row.get('put_attempted')) is not bool or row['put_attempted'] != miss or
                            type(row.get('put_acked')) is not bool or row['put_acked'] != miss):
                        raise RuntimeError('refill PUT attempt/ACK mismatch')
                    previous = at
                    if miss:
                        ack, verify = row.get('put_ack_sec'), row.get('verify_started_sec')
                        if (any(type(v) not in (int, float) or not math.isfinite(v) for v in (ack, verify)) or
                                not at <= ack <= verify <= upper or
                                type(row.get('verify_hit')) is not int or row['verify_hit'] not in (0, 1)):
                            raise RuntimeError('refill verification timing/outcome mismatch')
                        previous = verify
                        verify_hits += row['verify_hit']
                        verify_misses += 1 - row['verify_hit']
                    elif row.get('verify_hit') is not None or 'put_ack_sec' in row or 'verify_started_sec' in row:
                        raise RuntimeError('refill attempted for a first hit')
                    namespace_hits += row['first_hit']
                if namespace_hits != outcome['hits']:
                    raise RuntimeError('refill first outcomes disagree with cohort')
                first_hits += namespace_hits
    misses = 384 - first_hits
    expected = {'first_hits': first_hits, 'first_misses': misses,
                'put_attempts': misses, 'put_acks': misses,
                'attempted_payload_bytes': misses * 4096, 'acked_payload_bytes': misses * 4096,
                'verify_hits': verify_hits, 'verify_misses': verify_misses,
                'baseline_payload_bytes': 15616 * 4096}
    if any(type(stats.get(name)) is not int or stats[name] != value for name, value in expected.items()):
        raise RuntimeError('refill aggregate mismatch')
    ratio = stats.get('logical_payload_write_amplification')
    if (type(ratio) not in (int, float) or not math.isfinite(ratio) or
            abs(ratio - (1 + misses / 15616)) > 1e-12):
        raise RuntimeError('refill logical payload amplification mismatch')
    return {'refill_put_acks': misses, 'refill_verify_hits': verify_hits,
            'refill_verify_misses': verify_misses,
            'logical_payload_write_amplification': ratio}


def check_multisize(text, client):
    """Validate compact two-class pressure without requiring hot residence.

    Four extents per disk cannot provide eight stripes for both size classes.
    Donor eviction can therefore precede high-watermark pressure. Admission,
    warmup, recurring hot and final scan reads retain separate denominators.
    Class PUT counters prove both actual classes were used; the global classes
    gauge describes canonical geometry, not simultaneous resident classes.
    """
    rows = client.get('multisize')
    if not isinstance(rows, list) or len(rows) != 2:
        raise RuntimeError('multisize class count mismatch')
    totals = {name: 0 for name in ('admission_hits', 'admission_misses',
                                  'warmup_hits', 'warmup_misses',
                                  'hot_hits', 'hot_misses', 'scan_hits', 'scan_misses')}
    for row, length, slot in zip(rows, (4096, 1048577), (1048576, 2097152)):
        if (not isinstance(row, dict) or
                any(type(row.get(name)) is not int or row[name] != expected for name, expected in
                    (('payload_bytes', length), ('slot_size', slot), ('put_attempts', 6400), ('put_acks', 6400))) or
                any(type(row.get(name)) is not int or row[name] < 0 for name in totals)):
            raise RuntimeError('multisize class identity/count mismatch')
        if any(row[stage + '_hits'] + row[stage + '_misses'] != count for stage, count in
               (('admission', 32), ('warmup', 64), ('hot', 11776), ('scan', 6400))):
            raise RuntimeError('multisize per-class read denominator mismatch')
        if sum(row[stage + '_hits'] for stage in ('admission', 'warmup', 'hot', 'scan')) == 0:
            raise RuntimeError('multisize class has no successful read')
        for name in totals:
            totals[name] += row[name]
    for name, count in totals.items():
        field = {'scan_hits': 'hits', 'scan_misses': 'misses'}.get(name, name)
        if type(client.get(field)) is not int or client[field] != count:
            raise RuntimeError('multisize aggregate mismatch')
    if total(text, 'dfkv_cache_put_total') != 12800 or total(text, 'dfkv_bytes_written_total') != 6737107200:
        raise RuntimeError('multisize accepted PUT/payload mismatch')
    class_puts = {}
    for labels, value in samples(text, 'dfkv_slab_class_put_total'):
        match = re.fullmatch(r'\{slot_size="([0-9]+)"\}', labels)
        if match is None or match[1] in class_puts:
            raise RuntimeError('multisize class metric labels mismatch')
        class_puts[match[1]] = value
    if (class_puts.get('1048576') != 6400 or class_puts.get('2097152') != 6400 or
            any(value != 0 for slot, value in class_puts.items() if slot not in ('1048576', '2097152')) or
            total(text, 'dfkv_slab_classes') < 2):
        raise RuntimeError('multisize actual class PUT mismatch')
    return {'multisize_gets': 36544, 'multisize_puts': 12800,
            'multisize_payload_bytes': 6737107200}


def check_concurrent(client):
    """Reconcile independent callers; C ABI overlap is not kernel pin overlap."""
    data = client.get('concurrency')
    if not isinstance(data, dict) or type(data.get('warmup_reads')) is not int or data['warmup_reads'] != 512:
        raise RuntimeError('concurrent warmup metadata mismatch')
    origin, end, middle = (data.get(key) for key in
                           ('pressure_started_sec', 'pressure_finished_sec', 'midpoint_sec'))
    if (any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in (origin, end, middle)) or
            not origin < middle < end or end - origin > 90):
        raise RuntimeError('concurrent pressure timing mismatch')
    steps = data.get('writer_steps')
    if not isinstance(steps, list) or len(steps) != 200:
        raise RuntimeError('concurrent writer step count mismatch')
    previous_end, previous_start = origin, None
    for index, row in enumerate(steps):
        if (not isinstance(row, dict) or any(type(row.get(k)) is not int for k in ('step', 'put_attempts', 'put_acks')) or
                row['step'] != index or row['put_attempts'] != 32 or row['put_acks'] != 32):
            raise RuntimeError('concurrent writer ACK mismatch')
        start, finish = row.get('start_sec'), row.get('end_sec')
        if (any(type(v) not in (int, float) or not math.isfinite(v) for v in (start, finish)) or
                not previous_end <= start <= finish <= end or
                (previous_start is not None and start - previous_start < .1 - 1e-6)):
            raise RuntimeError('concurrent writer chronology mismatch')
        previous_start, previous_end = start, finish
    if not steps[99]['end_sec'] <= middle <= steps[100]['start_sec']:
        raise RuntimeError('concurrent midpoint chronology mismatch')
    rows = data.get('readers')
    if not isinstance(rows, list) or len(rows) != 8:
        raise RuntimeError('concurrent reader count mismatch')
    hits = misses = batches = 0
    events = []
    last_read = end
    for index, row in enumerate(rows):
        namespace = (b'disk-validation\x00A\xff', b'disk-validation\x00B\xfe')[index % 2].hex()
        start_index = index // 2 * 32
        if (not isinstance(row, dict) or any(type(row.get(k)) is not int for k in ('reader_id', 'start_index', 'count', 'hits', 'misses')) or
                row['reader_id'] != index or row.get('namespace_hex') != namespace or
                row['start_index'] != start_index or row['count'] != 32 or
                row.get('transport_mode') != 'rdma' or row.get('status') != 'passed' or row.get('error')):
            raise RuntimeError('concurrent independent reader identity/status mismatch')
        calls = row.get('batches')
        if not isinstance(calls, list) or not 1 <= len(calls) <= 32768:
            raise RuntimeError('concurrent reader batch count mismatch')
        reader_hits = reader_misses = 0
        previous = origin
        before_midpoint = False
        for number, call in enumerate(calls):
            if (not isinstance(call, dict) or
                    any(type(call.get(k)) is not int or call[k] < 0 for k in ('batch', 'start_index', 'hits', 'misses')) or
                    call['batch'] != number or call['start_index'] != start_index + number % 2 * 16 or
                    call['hits'] + call['misses'] != 16 or call.get('error')):
                raise RuntimeError('concurrent read batch identity/count mismatch')
            start, finish = call.get('call_started_sec'), call.get('call_finished_sec')
            if (any(type(v) not in (int, float) or not math.isfinite(v) for v in (start, finish)) or
                    not previous <= start < finish):
                raise RuntimeError('concurrent C ABI chronology mismatch')
            previous = finish
            before_midpoint |= finish <= middle
            reader_hits += call['hits']
            reader_misses += call['misses']
            # An already prepared call may start/end just after writer stop.
            # Clip to its actual writer overlap, and sort ends before starts.
            clipped_start, clipped_end = max(start, origin), min(finish, end)
            if clipped_start < clipped_end:
                events.extend(((clipped_start, 1), (clipped_end, -1)))
            last_read = max(last_read, finish)
        if (not before_midpoint or row['hits'] != reader_hits or row['misses'] != reader_misses):
            raise RuntimeError('concurrent reader progress/aggregate mismatch')
        hits += reader_hits
        misses += reader_misses
        batches += len(calls)
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    if peak < 2:
        raise RuntimeError('concurrent C ABI overlap coverage not observed')
    if (client.get('filler_warmup_reads') != 0 or type(client.get('filler_warmup_reads')) is not int or
            client.get('hot_hits') != 512 + hits or client.get('hot_misses') != misses or
            client.get('hot_reads') != 512 + batches * 16):
        raise RuntimeError('concurrent hot read denominator mismatch')
    phases = client.get('phases')
    names = ('prefill', 'warm', 'before-pressure', 'step-100', 'after-pressure', 'after-scan')
    if not isinstance(phases, list) or len(phases) != len(names):
        raise RuntimeError('concurrent phase count mismatch')
    previous = 0
    for index, (row, name) in enumerate(zip(phases, names)):
        if not isinstance(row, dict) or row.get('name') != name:
            raise RuntimeError('concurrent phase identity mismatch')
        at = row.get('elapsed_sec')
        if (type(at) not in (int, float) or not math.isfinite(at) or at < previous or
                any(type(row.get(k)) is not int for k in ('hot_hits', 'hot_misses', 'filler_warmup_reads')) or
                row.get('hot_hits') != (0 if index == 0 else 512 if index < 4 else 512 + hits) or
                row.get('hot_misses') != (0 if index < 4 else misses) or row.get('filler_warmup_reads') != 0):
            raise RuntimeError('concurrent phase timing/count mismatch')
        previous = at
    if (phases[2]['elapsed_sec'] > origin or
            not steps[99]['end_sec'] <= phases[3]['elapsed_sec'] <= middle or
            phases[4]['elapsed_sec'] < last_read):
        raise RuntimeError('concurrent phase chronology mismatch')
    return {'concurrent_readers': 8, 'concurrent_reader_batches': batches,
            'concurrent_reader_gets': batches * 16, 'concurrent_gets': 15616 + 512 + batches * 16,
            'concurrent_peak_calls': peak, 'concurrent_pressure_elapsed_sec': end - origin}


def check_concurrent_watermarks(before, middle, after):
    """Prove eviction in the active read/write stage, not a victim-instant race."""
    counts, routes = [], []
    for text in (before, middle, after):
        rows = samples(text, 'dfkv_slab_watermark_evictions_total')
        if len(rows) != 1 or rows[0][0] or not rows[0][1].is_integer():
            raise RuntimeError('concurrent watermark counter mismatch')
        counts.append(rows[0][1])
        disks = samples(text, 'dfkv_disk_objects')
        labels = {label for label, _ in disks}
        if len(disks) != 3 or len(labels) != 3:
            raise RuntimeError('concurrent watermark disk count mismatch')
        routes.append(labels)
        for name in ('dfkv_storage_healthy', 'dfkv_slab_healthy'):
            if total(text, name) != 1:
                raise RuntimeError('concurrent phase unhealthy')
        for name in ('dfkv_errors_total', 'dfkv_uring_init_fallbacks_total'):
            if total(text, name) != 0:
                raise RuntimeError('concurrent phase error/fallback')
    if not counts[0] < counts[1] <= counts[2] or routes[0] != routes[1] or routes[1] != routes[2]:
        raise RuntimeError('concurrent midpoint watermark progress/route mismatch')
    # Midpoint inflight and terminal counters are deliberately not required to
    # be settled: readers still run, and metrics are not a transactional snapshot.
    return {'concurrent_watermark_delta': int(counts[1] - counts[0])}


def check_multisize_watermarks(text):
    """Check resolved per-store thresholds before writes and at completion."""
    expected = {'capacity_bytes': 4294967296, 'watermark_high_bytes': 2147483600,
                'watermark_low_bytes': 1717986880}
    labels = None
    for suffix, value in expected.items():
        rows = samples(text, 'dfkv_slab_disk_' + suffix)
        current = {label for label, _ in rows}
        if len(rows) != 3 or len(current) != 3 or any(actual != value for _, actual in rows):
            raise RuntimeError('multisize watermark effective threshold mismatch: ' + suffix)
        if labels is not None and current != labels:
            raise RuntimeError('multisize watermark disk labels mismatch')
        labels = current
    disks, indices = set(), set()
    for label in labels:
        match = re.fullmatch(r'\{disk=("(?:[^"\\]|\\.)*"),disk_index="([012])"\}', label)
        if not match:
            raise RuntimeError('multisize watermark disk identity mismatch')
        disks.add(json.loads(match[1]))
        indices.add(match[2])
    objects = samples(text, 'dfkv_disk_objects')
    if (len(disks) != 3 or indices != {'0', '1', '2'} or len(objects) != 3 or
            {label for label, _ in objects} != {'{disk=' + json.dumps(disk, ensure_ascii=False) + '}' for disk in disks}):
        raise RuntimeError('multisize watermark disk ownership mismatch')


def check_result(text, client, policy='lifetime', scenario='quick-pressure', first_reuse_sec=0,
                 refill_on_miss=False, multisize_watermark=False):
    if (type(multisize_watermark) is not bool or
            (multisize_watermark and scenario != 'multisize-pressure')):
        raise RuntimeError('multisize watermark mode mismatch')
    if multisize_watermark:
        check_multisize_watermarks(text)
        if total(text, 'dfkv_slab_watermark_evictions_total') <= 0:
            raise RuntimeError('multisize watermark activity not observed')
    if (type(refill_on_miss) is not bool or ('refill' in client) != refill_on_miss or
            (refill_on_miss and scenario != 'arrivals-pressure')):
        raise RuntimeError('refill mode/result mismatch')
    if ('concurrency' in client) != (scenario == 'concurrent-pressure'):
        raise RuntimeError('concurrent mode/result mismatch')
    if ('multisize' in client) != (scenario == 'multisize-pressure'):
        raise RuntimeError('multisize mode/result mismatch')
    if scenario not in ('quick-pressure', 'sustained-pressure', 'mixed-pressure', 'arrivals-pressure', 'multisize-pressure', 'concurrent-pressure'):
        raise RuntimeError('unknown scenario')
    if first_reuse_sec not in (0, 45, 90, 185) or (first_reuse_sec and scenario != 'sustained-pressure'):
        raise RuntimeError('invalid first reuse scenario')
    expected_count = SUSTAINED_COUNT if scenario in ('sustained-pressure', 'mixed-pressure', 'arrivals-pressure', 'concurrent-pressure') else COUNT
    expected_hot_reads = SUSTAINED_HOT_READS if scenario in ('sustained-pressure', 'mixed-pressure', 'arrivals-pressure') else HOT_READS
    if scenario == 'multisize-pressure':
        expected_count, expected_hot_reads = MULTISIZE_COUNT, MULTISIZE_HOT_READS
    if client.get('status') != 'passed' or client.get('wrong_bytes') != 0:
        raise RuntimeError('client correctness failure')
    if client.get('transport_mode') != ['rdma', 'rdma']:
        raise RuntimeError('client did not use RDMA')
    counts = [client.get(name) for name in ('hits', 'misses', 'hot_hits', 'hot_misses', 'hot_reads')]
    if any(type(value) is not int or value < 0 for value in counts):
        raise RuntimeError('invalid client read counts')
    hits, misses, hot_hits, hot_misses, hot_reads = counts
    if (hits + misses != expected_count * 2 or
            (not first_reuse_sec and scenario != 'concurrent-pressure' and hot_reads != expected_hot_reads) or
            hot_reads != hot_hits + hot_misses or
            (scenario != 'multisize-pressure' and not first_reuse_sec and not hot_hits)):
        raise RuntimeError('client read denominator mismatch')
    schedule = (check_concurrent(client) if scenario == 'concurrent-pressure' else
                check_multisize(text, client) if scenario == 'multisize-pressure' else
                check_arrivals(client) if scenario == 'arrivals-pressure' else
                check_mixed(client) if scenario == 'mixed-pressure' else
                check_steps(client, first_reuse_sec) if scenario == 'sustained-pressure' else {})
    if scenario == 'concurrent-pressure':
        if (total(text, 'dfkv_cache_put_total') != 15616 or
                total(text, 'dfkv_bytes_written_total') != 63963136 or
                total(text, 'dfkv_slab_watermark_evictions_total') <= 0):
            raise RuntimeError('concurrent PUT/payload/watermark denominator mismatch')
    refill = check_refill(client) if refill_on_miss else {}
    verify_hits = refill.get('refill_verify_hits', 0)
    verify_misses = refill.get('refill_verify_misses', 0)
    if refill_on_miss:
        # These server counters count accepted logical PUTs/payload, not device IO.
        if (total(text, 'dfkv_cache_put_total') != 15616 + refill['refill_put_acks'] or
                total(text, 'dfkv_bytes_written_total') != 63963136 + client['refill']['acked_payload_bytes']):
            raise RuntimeError('refill accepted PUT/payload denominator mismatch')
    reuse_hits = client['reuse_hits'] if scenario in ('mixed-pressure', 'arrivals-pressure') else 0
    reuse_misses = client['reuse_misses'] if scenario in ('mixed-pressure', 'arrivals-pressure') else 0
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
    filler_reads = (18112 if scenario == 'mixed-pressure' else
                    18304 if first_reuse_sec or scenario == 'arrivals-pressure' else 0)
    early_hits = client['admission_hits'] + client['warmup_hits'] if scenario == 'multisize-pressure' else 0
    early_misses = client['admission_misses'] + client['warmup_misses'] if scenario == 'multisize-pressure' else 0
    terminal = {}
    for labels, value in samples(text, 'dfkv_rdma_pull_requests_terminal_total'):
        match = re.search(r'(?:\{|,)result="([^"]+)"(?:,|\})', labels)
        if not match:
            raise RuntimeError('terminal result label missing')
        key = match.group(1)
        terminal[key] = terminal.get(key, 0) + value
        if key not in ('success_nonempty', 'not_found') and value:
            raise RuntimeError('unexpected PULL terminal: ' + key)
    if terminal.get('success_nonempty', 0) != hits + hot_hits + filler_reads + reuse_hits + verify_hits + early_hits:
        raise RuntimeError('success denominator mismatch')
    if terminal.get('not_found', 0) != misses + hot_misses + reuse_misses + verify_misses + early_misses:
        raise RuntimeError('missing denominator mismatch')
    dispatched = total(text, 'dfkv_rdma_pull_requests_dispatched_total')
    if dispatched != total(text, 'dfkv_rdma_pull_requests_terminal_total') or dispatched != sum(counts[:4]) + filler_reads + reuse_hits + reuse_misses + verify_hits + verify_misses + early_hits + early_misses:
        raise RuntimeError('PULL requests not settled')
    heat = {}
    if policy == 'lifetime':
        success = total(text, 'dfkv_slab_lifetime_read_success_total')
        applied = total(text, 'dfkv_slab_lifetime_read_applied_total')
        if success != hits + hot_hits + filler_reads + reuse_hits + verify_hits + early_hits or applied > success:
            raise RuntimeError('lifetime heat denominator mismatch')
        # Pressure can reclaim the original resident before its heat notification.
        heat = {'read_success': success, 'read_applied': applied}
    return {**schedule, **refill, **heat, 'watermark_evictions': total(text, 'dfkv_slab_watermark_evictions_total'),
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
    parser.add_argument('--scenario', choices=('quick-pressure', 'sustained-pressure', 'mixed-pressure', 'arrivals-pressure', 'multisize-pressure', 'concurrent-pressure'),
                        default='quick-pressure')
    parser.add_argument('--pressure-delay-sec', type=int, default=0,
                        help='wait after hot warmup before cold writes (0..300)')
    parser.add_argument('--first-reuse-sec', type=int, choices=(0, 45, 90, 185), default=0,
                        help='sustained-pressure: withhold first cohort read while cold writes continue')
    parser.add_argument('--data-name', help='new child name shared across all data roots')
    parser.add_argument('--protect-sec', type=int, choices=(30, 60, 180),
                        help='explicit lifetime P preset for isolated diagnostics only')
    parser.add_argument('--refill-on-miss', action='store_true',
                        help='arrivals-pressure: refill sampled first misses once and verify once')
    parser.add_argument('--multisize-watermark', action='store_true',
                        help='multisize-pressure: isolated low/high watermarks of 40/50 percent')
    args = parser.parse_args(argv)
    if args.multisize_watermark and args.scenario != 'multisize-pressure':
        parser.error('multisize-watermark requires multisize-pressure')
    if args.refill_on_miss and args.scenario != 'arrivals-pressure':
        parser.error('refill-on-miss requires arrivals-pressure')
    if args.first_reuse_sec and args.scenario != 'sustained-pressure':
        parser.error('first-reuse-sec requires sustained-pressure')
    if args.scenario == 'concurrent-pressure' and (args.pressure_delay_sec or args.protect_sec is not None):
        parser.error('concurrent-pressure requires default timing and watermarks')
    if args.protect_sec is not None and args.policy != 'lifetime':
        parser.error('protect-sec requires lifetime policy')
    if not 0 <= args.pressure_delay_sec <= 300:
        parser.error('pressure delay must be in 0..300 seconds')
    if args.scenario in ('sustained-pressure', 'mixed-pressure', 'arrivals-pressure', 'multisize-pressure', 'concurrent-pressure') and args.pressure_delay_sec:
        parser.error('paced pressure scenarios do not accept a pre-pressure delay')
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
    watermark_override = {'low': 40, 'high': 50} if args.multisize_watermark else None
    result = {'status': 'failed', 'policy': args.policy, 'scenario': args.scenario,
              'first_reuse_sec': args.first_reuse_sec, 'multisize_watermark': args.multisize_watermark,
              'watermark_pct_override': watermark_override}
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
        if args.protect_sec is not None:
            env['DFKV_SLAB_PROTECT_SEC'] = str(args.protect_sec)
        if watermark_override is not None:
            # Diagnostic only: C++ resolves capacity / 100 * percentage.
            env['DFKV_SLAB_EVICT_LOW_PCT'] = str(watermark_override['low'])
            env['DFKV_SLAB_EVICT_HIGH_PCT'] = str(watermark_override['high'])
        command = [str(args.build / 'dfkv_server'), '--dir', ','.join(map(str, dirs)),
                   '--cap', '12884901888', '--store-engine', 'slab', '--slab-write', 'buffered',
                   '--slab-eviction-policy', args.policy, '--port', '12000', '--rdma-port', '12001',
                   '--rdma-dev', 'rxe0', '--max-msg', '2097152', '--metrics-port', '12010',
                   '--metrics-bind', '127.0.0.1']
        count = SUSTAINED_COUNT if args.scenario in ('sustained-pressure', 'mixed-pressure', 'arrivals-pressure', 'concurrent-pressure') else COUNT
        if args.scenario == 'multisize-pressure':
            count = MULTISIZE_COUNT
        client_command = [sys.executable, str(source / 'deploy/validation/disk_scenarios.py'),
                          '--lib', str(args.build / 'libdfkv.so'), '--members', 'n=127.0.0.1:12001',
                          '--scenario', args.scenario, '--count', str(count),
                          '--pressure-delay-sec', str(args.pressure_delay_sec),
                          '--first-reuse-sec', str(args.first_reuse_sec),
                          '--metrics-url', METRICS_URL, '--output', str(args.output / 'trace')]
        if args.refill_on_miss:
            client_command.append('--refill-on-miss')
        client_timeout_sec = (320 if args.scenario in ('sustained-pressure', 'mixed-pressure', 'arrivals-pressure', 'multisize-pressure', 'concurrent-pressure') else
                              120 + args.pressure_delay_sec)
        (args.output / 'case.json').write_text(json.dumps({'command': command, 'data_dirs': list(map(str, dirs)),
            'client_command': client_command, 'client_timeout_sec': client_timeout_sec,
            'pressure_delay_sec': args.pressure_delay_sec, 'scenario': args.scenario,
            'protect_sec_override': args.protect_sec, 'first_reuse_sec': args.first_reuse_sec,
            'refill_on_miss': args.refill_on_miss, 'multisize_watermark': args.multisize_watermark,
            'watermark_pct_override': watermark_override,
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
            if args.multisize_watermark:
                check_multisize_watermarks(before)
            with (args.output / 'client.log').open('w') as log:
                subprocess.run(client_command, env={**env, 'DFKV_RDMA': '1'}, stdout=log,
                               stderr=subprocess.STDOUT, timeout=client_timeout_sec, check=True)
            client = json.loads((args.output / 'trace/result.json').read_text())
            after = scrape()
            (args.output / 'after.prom').write_text(after)
            checked = check_result(after, client, args.policy, args.scenario, args.first_reuse_sec,
                                   args.refill_on_miss, args.multisize_watermark)
            if args.scenario == 'concurrent-pressure':
                checked.update(check_concurrent_watermarks(
                    (args.output / 'trace/phase-before-pressure.prom').read_text(),
                    (args.output / 'trace/phase-step-100.prom').read_text(), after))
            result.update(checked, client=client, status='passed')
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
