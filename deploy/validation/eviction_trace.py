#!/usr/bin/env python3
"""Bounded synthetic trace generation/validation; no client or service imports.

This first offline schema supports unique generation-zero PUTs and subsequent
GETs only. GET digests describe bytes IF hit, not a promise of cache residency.
It is not a formal workload registration or an arbitrary business-trace parser.
"""

import argparse
import hashlib
import json
from pathlib import Path
import sys

SCHEMA = "dfkv-offline-trace-v1"
MAX_FILE_BYTES = 16 * 1024 * 1024
LIMITS = {"seed": (0, 2**64 - 1), "objects": (1, 256),
          "rounds": (1, 256), "length": (1, 65536),
          "interval_ns": (1, 1000000000)}


def encode(value):
    """Canonical UTF-8 JSON, including one final newline."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode("ascii")


def checksum(document):
    """Hash all fields except this envelope's digest; not a signature."""
    return hashlib.sha256(encode({key: value for key, value in document.items()
                                  if key != "sha256"})).hexdigest()


def payload(row):
    """Regenerate bytes without materializing/storing them in the trace file."""
    fields = (bytes.fromhex(row["namespace_hex"]), bytes.fromhex(row["key_hex"]),
              row["generation"].to_bytes(8, "big"),
              row["length"].to_bytes(8, "big"))
    identity = SCHEMA.encode("ascii") + b"".join(
        len(field).to_bytes(8, "big") + field for field in fields)
    return hashlib.shake_256(identity).digest(row["length"])


def generate(*, seed=1, objects=16, rounds=4, length=4096, interval_ns=1000000):
    config = dict(seed=seed, objects=objects, rounds=rounds, length=length,
                  interval_ns=interval_ns)
    for name, (minimum, maximum) in LIMITS.items():
        value = config[name]
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
    rows = []

    def append(op, phase, index):
        row = {"request_id": len(rows), "scheduled_ns": len(rows) * interval_ns,
               "op": op, "phase": phase,
               "namespace_hex": (b"offline\x00" + bytes([index % 2])).hex(),
               "key_hex": (seed.to_bytes(8, "big") + index.to_bytes(8, "big")).hex(),
               "generation": 0, "length": length}
        row["payload_sha256"] = hashlib.sha256(payload(row)).hexdigest()
        rows.append(row)

    for index in range(objects):
        append("put", "init", index)
    for index in range(objects):
        append("get", "warmup", index)
    for index in range(rounds):
        append("put", "measure", objects + index)
        append("get", "measure", index % objects)
    document = {"schema": SCHEMA, "config": config, "requests": rows}
    document["sha256"] = checksum(document)
    return document


def validate(document):
    """Rebuild this exact synthetic plan; a self-consistent digest is not enough.

    Canonical-byte comparison also rejects bools/floats standing in for integers,
    unknown fields, omissions, duplicate request IDs and rehashed forged plans.
    No declared request totals or resident-state claims are trusted.
    """
    if not isinstance(document, dict) or set(document) != {
            "schema", "config", "requests", "sha256"}:
        raise ValueError("invalid trace envelope")
    config = document["config"]
    if not isinstance(config, dict) or set(config) != set(LIMITS):
        raise ValueError("invalid config fields")
    expected = generate(**config)
    if encode(document) != encode(expected):
        raise ValueError("trace does not match its deterministic plan/checksum")
    reads = config["objects"] + config["rounds"]
    return {"requests": reads * 2, "put_requests": reads, "get_requests": reads,
            "put_payload_bytes": reads * config["length"],
            "requested_get_bytes": reads * config["length"],
            "measurement_requests": config["rounds"] * 2}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError(f"non-finite JSON number: {value}")


def load(path):
    # Bounded read also rejects a file that grows after the initial open.
    with Path(path).open("rb") as source:
        raw = source.read(MAX_FILE_BYTES + 1)
    if len(raw) > MAX_FILE_BYTES:
        raise ValueError("trace exceeds 16 MiB file limit")
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=reject_constant)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    writer = commands.add_parser("generate", help="write a NEW synthetic trace")
    writer.add_argument("path")
    for name, default in (("seed", 1), ("objects", 16), ("rounds", 4),
                          ("length", 4096), ("interval_ns", 1000000)):
        writer.add_argument("--" + name.replace("_", "-"), type=int, default=default)
    checker = commands.add_parser("check", help="validate an offline trace")
    checker.add_argument("path")
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            document = generate(**{name: getattr(args, name) for name in LIMITS})
            summary = validate(document)
            # Exclusive creation: never overwrite an existing file or symlink.
            # Failed/interrupted writes are retained and fail subsequent checks.
            with Path(args.path).open("xb") as output:
                output.write(encode(document))
        else:
            summary = validate(load(args.path))
        print(json.dumps(summary, sort_keys=True))
        return 0
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError) as error:
        print(f"trace error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
