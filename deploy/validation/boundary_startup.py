#!/usr/bin/env python3
"""Bounded negative startup checks; metadata-only, not payload preservation.

Callers supply the real server command, complete environment and dedicated cache
roots. This helper never creates or deletes cache roots. It may inspect existing
metadata or prove nonexistent roots stayed absent, without following symlinks.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import time

METADATA = {'slab_meta', 'slab_state', 'slots.tbl', 'slab_initializing'}
PARAMETERS = (
    'DFKV_SLAB_LIFE_BASE_SEC', 'DFKV_SLAB_LIFE_READ_BONUS_SEC',
    'DFKV_SLAB_LIFE_MAX_READS', 'DFKV_SLAB_PROTECT_SEC',
    'DFKV_SLAB_PROTECT_SLOWDOWN', 'DFKV_SLAB_PROTECT_BOOST_SEC',
    'DFKV_SLAB_KEEP_SCORE_SEC', 'DFKV_SLAB_BUCKET_SEC',
    'DFKV_SLAB_BUCKET_COUNT', 'DFKV_SLAB_EVICT_LOW_PCT',
    'DFKV_SLAB_EVICT_HIGH_PCT',
)
MAX_ENTRIES = 20000
MAX_DEPTH = 8
MAX_METADATA_BYTES = 256 << 20
MAX_LOG_BYTES = 1 << 20


def cases():
    """Defaults are intentional: M4/M63 alone must reject B24/G43200 coupling."""
    rows = []
    for name, parameter, raw, culprit, value in (
            ('reads-zero', 'DFKV_SLAB_LIFE_MAX_READS', '0', None, None),
            ('reads-64', 'DFKV_SLAB_LIFE_MAX_READS', '64', None, None),
            ('reads-four-default-buckets', 'DFKV_SLAB_LIFE_MAX_READS', '4',
             'DFKV_SLAB_BUCKET_COUNT', '24'),
            ('reads-63-default-buckets', 'DFKV_SLAB_LIFE_MAX_READS', '63',
             'DFKV_SLAB_BUCKET_COUNT', '24'),
            ('bucket-width-zero', 'DFKV_SLAB_BUCKET_SEC', '0', None, None),
            ('bucket-count-65', 'DFKV_SLAB_BUCKET_COUNT', '65', None, None),
            ('keep-equals-base', 'DFKV_SLAB_KEEP_SCORE_SEC', '604800', None, None),
            ('unsigned-overflow', 'DFKV_SLAB_LIFE_MAX_READS',
             '18446744073709551616', None, None),
            ('malformed', 'DFKV_SLAB_LIFE_MAX_READS', '4x', None, None),
            ('signed', 'DFKV_SLAB_LIFE_MAX_READS', '-1', None, None),
            ('zero-prefixed-range', 'DFKV_SLAB_LIFE_MAX_READS', '00064', None, None),
            ('equal-watermarks', 'DFKV_SLAB_EVICT_LOW_PCT', '92', None, None)):
        rows.append(dict(case_id=name, overrides={parameter: raw},
                         expected_name=culprit or parameter,
                         expected_raw=value if value is not None else raw))
    return rows


def _path(value):
    path = Path(value).absolute()
    if '..' in path.parts:
        raise ValueError('cache/output path must not contain ..')
    # Refuse even ancestor symlinks; callers must provide real dedicated paths.
    for part in [*reversed(path.parents), path]:
        try:
            info = part.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise ValueError('symlink path is not allowed: ' + str(part))
    return path


def inventory(directories):
    """Hash only named metadata; list all entry types/sizes, never payload bytes."""
    rows = []
    count = 0
    hashed_bytes = 0
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW

    def walk(fd, prefix, depth, entries):
        nonlocal count, hashed_bytes
        if depth > MAX_DEPTH:
            raise ValueError('cache inventory depth bound exceeded')
        for name in sorted(os.listdir(fd)):
            count += 1
            if count > MAX_ENTRIES:
                raise ValueError('cache inventory entry bound exceeded')
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            relative = str(prefix / name)
            record = dict(name=relative, size=info.st_size, dev=info.st_dev, inode=info.st_ino)
            if stat.S_ISDIR(info.st_mode):
                record['type'] = 'directory'
                entries.append(record)
                child = os.open(name, flags, dir_fd=fd)
                try:
                    opened = os.fstat(child)
                    if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                        raise ValueError('cache directory identity changed during inventory')
                    walk(child, prefix / name, depth + 1, entries)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode):
                record['type'] = 'file'
                if name in METADATA:
                    hashed_bytes += info.st_size
                    if hashed_bytes > MAX_METADATA_BYTES:
                        raise ValueError('cache metadata hashing bound exceeded')
                    child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                    try:
                        opened = os.fstat(child)
                        if ((opened.st_dev, opened.st_ino, opened.st_size) !=
                                (info.st_dev, info.st_ino, info.st_size)):
                            raise ValueError('metadata identity changed before hashing')
                        digest = hashlib.sha256()
                        remaining = info.st_size
                        while remaining:
                            chunk = os.read(child, min(131072, remaining))
                            if not chunk:
                                raise ValueError('metadata truncated during hashing')
                            remaining -= len(chunk)
                            digest.update(chunk)
                        finished = os.fstat(child)
                        if (os.read(child, 1) or finished.st_size != info.st_size or
                                (finished.st_mtime_ns, finished.st_ctime_ns) !=
                                (opened.st_mtime_ns, opened.st_ctime_ns)):
                            raise ValueError('metadata changed during hashing')
                        record['sha256'] = digest.hexdigest()
                    finally:
                        os.close(child)
                entries.append(record)
            else:
                raise ValueError('symlink/special cache entry is not allowed: ' + relative)

    for directory in directories:
        path = _path(directory)
        try:
            info = path.lstat()
        except FileNotFoundError:
            rows.append(dict(path=str(path), exists=False, entries=[]))
            continue
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError('cache root must be directory or nonexistent')
        fd = os.open(path, flags)
        try:
            opened = os.fstat(fd)
            if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                raise ValueError('cache root identity changed during inventory')
            record = dict(path=str(path), exists=True, dev=info.st_dev,
                          inode=info.st_ino, size=info.st_size, entries=[])
            walk(fd, Path(), 0, record['entries'])
            rows.append(record)
        finally:
            os.close(fd)
    return dict(scope='metadata-hashes-and-entry-inventory-not-payload-bytes', roots=rows)


def _stop(child, receipt):
    for action in ('terminate', 'kill'):
        if child.poll() is not None:
            break
        receipt['cleanup'].append(action)
        try:
            getattr(child, action)()
            child.wait(timeout=1)
        except (OSError, subprocess.TimeoutExpired) as error:
            receipt['cleanup_errors'].append(type(error).__name__ + ': ' + str(error))
    if child.poll() is not None:
        receipt.update(exit=child.wait(timeout=1), reaped=True)


def _attempt(command, env, log, timeout):
    receipt = dict(pid=None, exit=None, reaped=False, timed_out=False,
                   log_limit=False, cleanup=[], cleanup_errors=[],
                   started_monotonic_sec=time.monotonic())
    child = None
    try:
        with log.open('xb') as stream:
            child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                     stdout=stream, stderr=subprocess.STDOUT)
            receipt['pid'] = child.pid
            deadline = time.monotonic() + timeout
            while child.poll() is None:
                if log.stat().st_size > MAX_LOG_BYTES:
                    receipt['log_limit'] = True
                    break
                if time.monotonic() >= deadline:
                    receipt['timed_out'] = True
                    break
                time.sleep(.01)
    except Exception as error:
        receipt['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        if child is not None:
            _stop(child, receipt)
        receipt['finished_monotonic_sec'] = time.monotonic()
    try:
        with log.open('rb') as stream:
            data = stream.read(MAX_LOG_BYTES + 1)
    except OSError as error:
        receipt['error'] = type(error).__name__ + ': ' + str(error)
        data = b''
    if len(data) > MAX_LOG_BYTES:
        receipt['log_limit'] = True
    receipt['log'] = log.name
    return receipt, data[:MAX_LOG_BYTES].decode('utf-8', errors='replace')


def run(command, env, directories, output, *, timeout=5):
    """Return a persisted report; fail fast and retain every unattempted case."""
    if (not isinstance(command, (list, tuple)) or not command or
            any(not isinstance(item, str) or not item or '\0' in item for item in command)):
        raise ValueError('command must be a nonempty argument list')
    if (not isinstance(env, dict) or any(not isinstance(key, str) or not key or '=' in key or '\0' in key or
            not isinstance(value, str) or '\0' in value for key, value in env.items())):
        raise ValueError('complete string environment required')
    if type(timeout) not in (float, int) or not math.isfinite(timeout) or not .05 <= timeout <= 30:
        raise ValueError('startup timeout must be in .05..30 seconds')
    roots = [_path(item) for item in directories]
    if not roots or len(set(roots)) != len(roots):
        raise ValueError('cache roots must be nonempty and distinct')
    output = _path(output)
    if os.path.lexists(output) or not output.parent.is_dir():
        raise ValueError('output must be new with an existing parent')
    for index, root in enumerate(roots):
        if output == root or root in output.parents or output in root.parents:
            raise ValueError('output and cache roots must not overlap')
        if any(root in other.parents or other in root.parents for other in roots[index + 1:]):
            raise ValueError('cache roots must not overlap')
    before = inventory(roots)  # Reject unsafe traversal before spawning/creating artifacts.
    output.mkdir()
    rows = [dict(case, state='not_attempted') for case in cases()]
    result = dict(scope='invalid-lifetime-startup-metadata-only-not-payload-preservation',
                  passed=False, command=list(command), timeout_sec=timeout,
                  cleared_parameters=list(PARAMETERS), policy='lifetime',
                  directories=[str(root) for root in roots], cases=rows, errors=[])
    (output / 'before.json').write_text(json.dumps(before, indent=2) + '\n')
    report_path = output / 'report.json'
    report_path.write_text(json.dumps(result, indent=2) + '\n')
    for index, row in enumerate(rows):
        row['state'] = 'running'
        selected = dict(env)
        for parameter in PARAMETERS:
            selected.pop(parameter, None)
        selected['DFKV_SLAB_EVICTION_POLICY'] = 'lifetime'
        selected.update(row['overrides'])
        try:
            receipt, text = _attempt(command, selected, output / f'{index:02d}-{row["case_id"]}.log', timeout)
            row['process'] = receipt
            after = inventory(roots)
            name = f'{index:02d}-after.json'
            (output / name).write_text(json.dumps(after, indent=2) + '\n')
            row['after_inventory'] = name
            row['metadata_unchanged'] = after == before
            token = row['expected_name'] + "='" + row['expected_raw'] + "'"
            row['diagnostic_matches'] = any(re.search(r'\bERROR\b', line) and token in line for line in text.splitlines())
            if (receipt['exit'] != 1 or not receipt['reaped'] or receipt['timed_out'] or
                    receipt['log_limit'] or receipt.get('error') or not row['diagnostic_matches'] or
                    not row['metadata_unchanged']):
                raise ValueError('startup must reject with exit1/ERROR/raw value and unchanged metadata inventory')
            row['state'] = 'passed'
        except Exception as error:
            row['state'] = 'failed'
            row['error'] = type(error).__name__ + ': ' + str(error)
            result['errors'].append(row['case_id'] + ': ' + row['error'])
            break
        finally:
            report_path.write_text(json.dumps(result, indent=2) + '\n')
    result['passed'] = all(row['state'] == 'passed' for row in rows)
    report_path.write_text(json.dumps(result, indent=2) + '\n')
    return result
