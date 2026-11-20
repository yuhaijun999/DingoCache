"""Explicit CPU rendezvous smoke identity/observations; not a shared-memory owner.

A fresh parent token changes backend namespace/tenant/routing intentionally. We
never unlink shm: NodeDedup does not expose exclusive-creator ownership. Report
possible retained paths, not a claim that every planned segment was created.
"""

import ctypes as ct
import hashlib
import os
import re

COUNTERS = ("hits", "fetches", "wait_hits", "wait_timeouts")
ENV = {"DFKV_CLIENT_NODE_DEDUP": "1", "DFKV_NODE_DEDUP_ARENA_MB": "1",
       "DFKV_NODE_DEDUP_SLOTS": "1024", "DFKV_NODE_DEDUP_WAIT_MS": "20",
       "DFKV_NODE_DEDUP_TAKEOVER_MS": "2000", "DFKV_NODE_DEDUP_TTL_MS": "5000",
       "DFKV_GET_MISS_RETRIES": "0"}


def namespace(token, original):
    if not isinstance(token, str) or re.fullmatch("[0-9a-f]{32}", token) is None:
        raise ValueError("CPU dedup requires a frozen 32-hex parent token")
    if type(original) is not bytes:
        raise ValueError("namespace must be bytes")
    return b"dfkv-cpu-dedup-validation\0" + token.encode("ascii") + b"\0" + original


def identities(document, token):
    result = []
    for original in sorted({row["namespace_hex"] for row in document["requests"]}):
        actual = namespace(token, bytes.fromhex(original))
        # key_map.h ToBlockKey(namespace, empty).digest_hi and v4 EnvSegmentName.
        digest = hashlib.sha256(b"DFKVKEY2" + len(actual).to_bytes(8, "little")
                                + actual + bytes(8)).hexdigest()[:16]
        name = f"/dfkv-dedup-v4-{os.getuid()}-{digest}"
        result.append({"namespace_hex": original, "backend_namespace_hex": actual.hex(),
                       "shm_name": name, "possible_shm_path": "/dev/shm" + name})
    return result


def require_fresh(document, token):
    for item in identities(document, token):
        # lexists also rejects dangling symlinks. This is a safety precheck,
        # not an atomic create/ownership proof; tokens are single-run random.
        if os.path.lexists(item["possible_shm_path"]):
            raise ValueError("CPU dedup namespace already has shared memory")


def snapshot(lib, handle):
    function = getattr(lib, "dfkv_stats_snapshot", None)
    if function is None:
        raise RuntimeError("CPU dedup smoke requires dfkv_stats_snapshot")
    # The shared native signature is bound once by load_library, not mutated
    # while another namespace handle might be using the same callable.
    buffer = ct.create_string_buffer(65536)
    size = function(handle, buffer, ct.sizeof(buffer))
    if not 0 < size < ct.sizeof(buffer):
        raise RuntimeError("incomplete CPU dedup metrics snapshot")
    text = buffer.raw[:size].decode("ascii")
    result = {}
    for name in COUNTERS:
        values = re.findall(r"^dfkv_client_dedup_" + name + r"_total ([0-9]+)$", text, re.M)
        if len(values) != 1:
            raise RuntimeError("CPU dedup was not observed enabled")
        result[name] = int(values[0])
    return result


def observations(entries):
    if any(item["before"] is None or item["after"] is None for item in entries):
        return None
    return {name: sum(item["after"][name] - item["before"][name] for item in entries)
            for name in COUNTERS}


def verify(document, result, config):
    expected = identities(document, config["cpu_dedup_token"])
    entries = result.get("cpu_dedup")
    if not isinstance(entries, list) or len(entries) != len(expected):
        raise ValueError("incomplete CPU dedup namespace ledger")
    errors = result["errors"]
    started = any("started_ns" in row for row in result["requests"])
    before_missing = False
    for identity, entry in zip(expected, entries):
        if (not isinstance(entry, dict) or set(entry) != set(identity) | {"before", "after"}
                or any(entry.get(key) != value for key, value in identity.items())):
            raise ValueError("CPU dedup backend identity changed")
        before, after = entry["before"], entry["after"]
        for value in (before, after):
            if value is not None and (not isinstance(value, dict) or set(value) != set(COUNTERS)
                    or any(type(n) is not int or not 0 <= n < 2**64 for n in value.values())):
                raise ValueError("invalid CPU dedup counter snapshot")
        if before is None:
            before_missing = True
            if after is not None or started or not errors:
                raise ValueError("CPU dedup activity before enabled observation")
        else:
            if before_missing or any(before.values()):
                raise ValueError("CPU dedup initial counters are not fresh")
            if after is None and not errors:
                raise ValueError("missing final CPU dedup observation")
            if after is not None and any(after[name] < before[name] for name in COUNTERS):
                raise ValueError("CPU dedup counter moved backwards")
        if after is not None:
            completed = sum(row["op"] == "get" and row["namespace_hex"] == identity["namespace_hex"]
                            and "completed_ns" in row for row in result["requests"])
            observed = sum(after[name] - before[name] for name in COUNTERS)
            if observed > completed or result.get("passed") is True and observed != completed:
                raise ValueError("CPU dedup counters contradict completed GET denominator")
    return observations(entries)
