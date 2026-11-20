"""Deterministic namespace batching and ledgers for opt-in correctness smoke.

No arrival scheduler: GET windows may reorder namespaces. Shared timestamps
bracket the adapter call, not per-item network completion or latency.
"""

import time

import eviction_trace as trace

MODEL = "namespace-batched-real-client-correctness-only"
SCHEDULE = "namespace-batched-unpaced"
GET_TERMINALS = ("get_hit", "get_miss", "get_unresolved", "client_error",
                 "get_batch_error", "not_attempted")


def check_outcome(result):
    if (not isinstance(result, dict) or "outcome" not in result
            or set(result) - {"outcome", "status", "error"}):
        raise ValueError("invalid batch slot shape")
    outcome = result["outcome"]
    if outcome not in GET_TERMINALS[:4]:
        raise ValueError("invalid batch slot outcome")
    if "status" in result and type(result["status"]) is not int:
        raise ValueError("invalid batch numeric status")
    if outcome == "get_miss" and result.get("status") != 1:
        raise ValueError("only explicit NOT_FOUND proves a miss")
    if outcome == "get_hit" and result.get("status", 0) != 0:
        raise ValueError("hit contradicts numeric status")
    if outcome == "get_unresolved" and "status" in result:
        raise ValueError("legacy unresolved cannot claim a status")
    if outcome == "client_error":
        if not isinstance(result.get("error"), str) or not result["error"]:
            raise ValueError("missing batch slot error")
    elif "error" in result:
        raise ValueError("non-error slot claims error")


def plan(document):
    """Singleton PUTs; each GET-only phase window grouped by namespace."""
    rows, groups, index = document["requests"], [], 0
    while index < len(rows):
        row = rows[index]
        if row["op"] == "put":
            groups.append([row["request_id"]])
            index += 1
            continue
        namespaces = {}
        phase = row["phase"]
        while index < len(rows) and rows[index]["op"] == "get" and rows[index]["phase"] == phase:
            row = rows[index]
            namespaces.setdefault(row["namespace_hex"], []).append(row["request_id"])
            index += 1
        groups.extend(namespaces.values())
    return groups


def execute(rows, clients, groups, origin):
    """Mutate the bounded child ledger; stop only after all siblings settle."""
    for group in groups:
        selected = [rows[index] for index in group["request_ids"]]
        client = clients[selected[0]["namespace_hex"]]
        start = time.monotonic_ns() - origin
        try:
            if selected[0]["op"] == "put":
                results = [client.execute(selected[0]["request_id"])]
                if results != [{"outcome": "put_ack"}]:
                    raise ValueError("invalid PUT result")
            else:
                results = client.execute_batch(group["request_ids"])
                if not isinstance(results, list) or len(results) != len(selected):
                    raise ValueError("incomplete batch GET results")
                for result in results:
                    check_outcome(result)
            group["state"] = "completed"
        except Exception as error:
            message = f"{type(error).__name__}: {error}"
            group.update(state="call_error", error=message)
            terminal = "get_batch_error" if selected[0]["op"] == "get" else "client_error"
            results = [{"outcome": terminal, "error": message} for _ in selected]
        end = time.monotonic_ns() - origin
        group.update(started_ns=start, completed_ns=end)
        for row, result in zip(selected, results):
            row.update(group_id=group["group_id"], started_ns=start, completed_ns=end,
                       terminal=result["outcome"])
            row.update({key: value for key, value in result.items() if key != "outcome"})
        if any(row["terminal"] != ("put_ack" if row["op"] == "put" else "get_hit") for row in selected):
            break


def summary(rows, groups):
    reads = [row for row in rows if row["op"] == "get"]
    counts = {terminal: sum(row["terminal"] == terminal for row in reads)
              for terminal in GET_TERMINALS}
    counts.update(planned=len(reads), attempted=sum("started_ns" in row for row in reads),
                  completed=sum("completed_ns" in row for row in reads))
    read_groups = [group for group in groups if rows[group["request_ids"][0]]["op"] == "get"]
    return {"planned": len(rows), "attempted": sum("started_ns" in row for row in rows),
            "completed": sum("completed_ns" in row for row in rows),
            "acked_put_bytes": sum(row["length"] for row in rows if row["terminal"] == "put_ack"),
            "hit_get_bytes": sum(row["length"] for row in reads if row["terminal"] == "get_hit"),
            "get_outcomes": counts,
            "get_adapter_calls": sum(group["state"] != "not_attempted" for group in read_groups),
            "get_failed_adapter_calls": sum(group["state"] == "call_error" for group in read_groups)}


def verify(document, result, config):
    """Independently regenerate groups, identities, stop gates and counters."""
    if (not isinstance(result, dict) or result.get("model") != MODEL
            or result.get("schedule_mode") != SCHEDULE
            or result.get("trace_sha256") != document["sha256"]
            or trace.encode(result.get("config")) != trace.encode(config)):
        raise ValueError("wrong batch model/trace/config")
    rows, groups, errors = result.get("requests"), result.get("groups"), result.get("errors")
    planned = plan(document)
    if (not isinstance(rows, list) or len(rows) != len(document["requests"])
            or not isinstance(groups, list) or len(groups) != len(planned)):
        raise ValueError("incomplete batch ledger")
    if not isinstance(errors, list) or any(not isinstance(error, str) or not error for error in errors):
        raise ValueError("invalid lifecycle errors")
    for source, row in zip(document["requests"], rows):
        if not isinstance(row, dict) or any(trace.encode(row.get(key)) != trace.encode(value) for key, value in source.items()):
            raise ValueError("request identity changed")
    previous, stopped = 0, False
    for group_id, (ids, group) in enumerate(zip(planned, groups)):
        if (not isinstance(group, dict) or trace.encode(group.get("group_id")) != trace.encode(group_id)
                or trace.encode(group.get("request_ids")) != trace.encode(ids)):
            raise ValueError("batch plan changed")
        state = group.get("state")
        selected = [rows[index] for index in ids]
        if state == "not_attempted":
            if not stopped and not errors:
                raise ValueError("unexplained unattempted group")
            if set(group) != {"group_id", "request_ids", "state"}:
                raise ValueError("unattempted group claims activity")
            for index, row in zip(ids, selected):
                if trace.encode(row) != trace.encode(dict(document["requests"][index], terminal="not_attempted")):
                    raise ValueError("unattempted row claims activity")
            stopped = True
            continue
        if stopped or state not in ("completed", "call_error"):
            raise ValueError("group executed after failure or invalid group state")
        fields = {"group_id", "request_ids", "state", "started_ns", "completed_ns"}
        if state == "call_error":
            fields.add("error")
            if not isinstance(group.get("error"), str) or not group["error"]:
                raise ValueError("missing group error")
        if set(group) != fields:
            raise ValueError("invalid group fields")
        start, end = group.get("started_ns"), group.get("completed_ns")
        if type(start) is not int or type(end) is not int or not previous <= start <= end:
            raise ValueError("invalid batch call timing")
        previous = end
        for index, row in zip(ids, selected):
            if any(trace.encode(row.get(key)) != trace.encode(value) for key, value in
                   (("group_id", group_id), ("started_ns", start), ("completed_ns", end))):
                raise ValueError("row contradicts batch call")
            outcome = {"outcome": row.get("terminal")}
            for key in ("status", "error"):
                if key in row:
                    outcome[key] = row[key]
            if set(row) != set(document["requests"][index]) | {"terminal", "group_id", "started_ns", "completed_ns"} | (set(outcome) - {"outcome"}):
                raise ValueError("unexpected batch row fields")
            if state == "call_error":
                terminal = "get_batch_error" if row["op"] == "get" else "client_error"
                if outcome != {"outcome": terminal, "error": group["error"]}:
                    raise ValueError("whole-call failure claims partial results")
            elif row["op"] == "get":
                check_outcome(outcome)
            elif outcome != {"outcome": "put_ack"}:
                raise ValueError("invalid PUT outcome")
        stopped = any(row["terminal"] != ("put_ack" if row["op"] == "put" else "get_hit") for row in selected)
    if type(result.get("elapsed_ns")) is not int or result["elapsed_ns"] < previous:
        raise ValueError("elapsed time precedes completion")
    transports = result.get("transports")
    namespaces = sorted({row["namespace_hex"] for row in rows})
    if (not isinstance(transports, list) or len(transports) > len(namespaces)
            or any(not isinstance(item, dict) or set(item) != {"namespace_hex", "mode"}
                   or not isinstance(item["mode"], str) or not item["mode"] for item in transports)
            or [item["namespace_hex"] for item in transports] != namespaces[:len(transports)]
            or any("started_ns" in row for row in rows) and len(transports) != len(namespaces)):
        raise ValueError("invalid transport ledger")
    counts = summary(rows, groups)
    passed = not errors and all(row["terminal"] == ("put_ack" if row["op"] == "put" else "get_hit") for row in rows)
    if (trace.encode(result.get("summary")) != trace.encode(counts)
            or type(result.get("passed")) is not bool or result["passed"] != passed):
        raise ValueError("batch summary/passed disagrees with ledger")
    return counts
