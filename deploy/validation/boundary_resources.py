#!/usr/bin/env python3
"""Fail-closed Linux process snapshots, not a complete allocation/MR account.

The resource keys match the existing mixed-prefix sampler where applicable.
Unlike that Prometheus-coupled sampler, this helper also brackets resource reads
with exact PID/starttime checks and supports separate owned server/client PIDs.
Snapshots are observations over an interval, not an atomic /proc transaction.
"""
import math
import os
from pathlib import Path
import re
import time

RESOURCE_FIELDS = ('rss_kib', 'vm_hwm_kib', 'threads', 'fd_count', 'vm_lck_kib', 'user_ticks',
                   'system_ticks', 'clock_ticks_per_sec')
ROW_FIELDS = {'role', 'pid', 'starttime_ticks', 'started_monotonic_sec',
              'finished_monotonic_sec', 'resource'}
LIMITATIONS = [
    'Sampled peaks can miss between-sample peaks; no leak or resource SLO verdict.',
    'Server and client processes are separate; peaks are not simultaneous sums.',
    'RSS is not complete allocator accounting or host/VM memory usage.',
    'VmHWM is kernel lifetime RSS high-water, including activity before measurement.',
    'VmLck is kernel-reported locked memory, not total RDMA registered memory.',
    'FD count and status fields are interval observations, not an atomic snapshot.',
]


def _integer(name, value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError('invalid resource ' + name)
    return value


def _stat(text, pid):
    # The parenthesized comm field can contain whitespace and ')' characters.
    match = re.fullmatch(r'([0-9]+) \(.*\) (.+)\n?', text, re.DOTALL)
    if match is None or int(match[1]) != pid:
        raise ValueError('invalid /proc stat PID/format')
    fields = match[2].split()
    if len(fields) < 20 or fields[0] not in ('R', 'S', 'D', 'T', 't', 'K', 'W', 'P', 'I'):
        raise ValueError('exited or invalid /proc process state')
    values = []
    for index in (11, 12, 19):
        if re.fullmatch(r'[0-9]+', fields[index]) is None:
            raise ValueError('invalid /proc stat counter')
        values.append(int(fields[index]))
    return dict(user_ticks=values[0], system_ticks=values[1], starttime_ticks=values[2])


def _status(text, pid):
    result = {}
    for field, key, unit, minimum in (
            ('Pid', 'pid', '', 1), ('VmRSS', 'rss_kib', ' kB', 0),
            ('VmHWM', 'vm_hwm_kib', ' kB', 0), ('VmLck', 'vm_lck_kib', ' kB', 0),
            ('Threads', 'threads', '', 1)):
        matches = re.findall(r'^' + field + r':\s*([0-9]+)' + unit + r'[ \t]*$',
                             text, re.MULTILINE)
        if len(matches) != 1:
            raise ValueError('missing, duplicate or invalid /proc status ' + field)
        result[key] = _integer(field, int(matches[0]), minimum)
    if result.pop('pid') != pid:
        raise ValueError('/proc status PID changed')
    return result


class ProcessSampler:
    """Sample a caller-owned live process; this helper never signals processes."""
    def __init__(self, pid, role, *, proc_root='/proc'):
        self.pid = _integer('pid', pid, 1)
        if role not in ('server', 'client'):
            raise ValueError('resource role must be server or client')
        self.role = role
        self.proc = Path(proc_root) / str(pid)
        self.starttime_ticks = self._read_stat()['starttime_ticks']

    def _read_stat(self):
        try:
            return _stat(self.proc.joinpath('stat').read_text(), self.pid)
        except OSError as error:
            raise RuntimeError('owned process stat unavailable: ' + str(self.pid)) from error

    def sample(self):
        begin = time.monotonic()
        before = self._read_stat()
        if before['starttime_ticks'] != self.starttime_ticks:
            raise ValueError('owned process identity changed before resource reads')
        try:
            values = _status(self.proc.joinpath('status').read_text(), self.pid)
            fds = list(self.proc.joinpath('fd').iterdir())
        except OSError as error:
            raise RuntimeError('owned process resources unavailable: ' + str(self.pid)) from error
        if any(re.fullmatch(r'[0-9]+', entry.name) is None for entry in fds):
            raise ValueError('invalid /proc FD entry')
        after = self._read_stat()
        if after['starttime_ticks'] != self.starttime_ticks:
            raise ValueError('owned process identity changed during resource reads')
        if any(after[key] < before[key] for key in ('user_ticks', 'system_ticks')):
            raise ValueError('owned process CPU counters regressed')
        values.update(fd_count=len(fds), user_ticks=after['user_ticks'],
                      system_ticks=after['system_ticks'],
                      clock_ticks_per_sec=_integer('clock ticks', os.sysconf('SC_CLK_TCK'), 1))
        row = dict(role=self.role, pid=self.pid, starttime_ticks=self.starttime_ticks,
                   started_monotonic_sec=begin, finished_monotonic_sec=time.monotonic(),
                   resource=values)
        _validate_row(row)
        return row


def _validate_row(row):
    if not isinstance(row, dict) or set(row) != ROW_FIELDS or row['role'] not in ('server', 'client'):
        raise ValueError('invalid resource row identity/schema')
    _integer('pid', row['pid'], 1)
    _integer('starttime', row['starttime_ticks'])
    for name in ('started_monotonic_sec', 'finished_monotonic_sec'):
        value = row[name]
        if type(value) not in (float, int) or not math.isfinite(value) or value < 0:
            raise ValueError('invalid resource monotonic timestamp')
    if row['finished_monotonic_sec'] < row['started_monotonic_sec']:
        raise ValueError('resource interval is reversed')
    values = row['resource']
    if not isinstance(values, dict) or set(values) != set(RESOURCE_FIELDS):
        raise ValueError('invalid resource fields')
    for key in RESOURCE_FIELDS:
        _integer(key, values[key], 1 if key in ('threads', 'clock_ticks_per_sec') else 0)


def summarize(rows):
    """Keep exact identities and roles separate; never sum independent peaks."""
    groups = {}
    for row in rows:
        _validate_row(row)
        identity = (row['role'], row['pid'], row['starttime_ticks'])
        group = groups.setdefault(identity, [])
        if group:
            previous = group[-1]
            if row['started_monotonic_sec'] < previous['finished_monotonic_sec']:
                raise ValueError('resource process samples overlap or are out of order')
            if (row['resource']['clock_ticks_per_sec'] != previous['resource']['clock_ticks_per_sec'] or
                    any(row['resource'][key] < previous['resource'][key]
                        for key in ('user_ticks', 'system_ticks'))):
                raise ValueError('resource clock changed or CPU counters regressed')
        group.append(row)
    if not groups:
        raise ValueError('no resource samples; missing data is not zero usage')
    roles = dict(server=[], client=[])
    for (role, pid, starttime), group in sorted(groups.items()):
        first, last = group[0], group[-1]
        ticks = sum(last['resource'][key] - first['resource'][key]
                    for key in ('user_ticks', 'system_ticks'))
        roles[role].append(dict(pid=pid, starttime_ticks=starttime, samples=len(group),
            first_started_monotonic_sec=first['started_monotonic_sec'],
            last_finished_monotonic_sec=last['finished_monotonic_sec'],
            observed_peaks={key: max(row['resource'][key] for row in group)
                            for key in ('rss_kib', 'vm_hwm_kib', 'threads', 'fd_count', 'vm_lck_kib')},
            observed_cpu_ticks=ticks,
            observed_cpu_sec=ticks / first['resource']['clock_ticks_per_sec']))
    return dict(scope='observed-per-process-resources-only', roles=roles,
                limitations=list(LIMITATIONS))
