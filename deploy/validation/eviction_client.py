"""Minimal synchronous trace adapter; no scheduler, service launcher or CLI."""

import threading

import eviction_trace as trace


class TraceClient:
    """Own one namespace-bound Client handle, serialized through close.

    The caller supplies a loaded C ABI library and explicit members. Construction
    opens a connection; importing this module does not. Use one handle per worker
    and namespace for independent concurrency. No disk-ACK/transport guarantee is
    inferred from a successful PUT: the external server setup must prove those.
    """

    def __init__(self, document, lib, members, namespace, *, client_factory=None,
                 cpu_dedup_token=None):
        trace.validate(document)
        self._rows = [dict(row) for row in document["requests"]]
        if type(namespace) is not bytes or namespace.hex() not in {
                row["namespace_hex"] for row in self._rows}:
            raise ValueError("namespace must be bytes present in the trace")
        if client_factory is None:
            # Keep offline/fake imports independent of ctypes client dependencies.
            from disk_scenarios import Client
            client_factory = Client
        self._namespace = namespace.hex()
        self._lock = threading.Lock()
        self._closed = False
        self._cpu_dedup = cpu_dedup_token is not None
        self._lib = lib
        if self._cpu_dedup:
            from eviction_dedup import namespace as backend_namespace
            namespace = backend_namespace(cpu_dedup_token, namespace)
        self._client = client_factory(lib, members, namespace)
        self.transport = self._client.transport

    def execute(self, request_id):
        """Return an observed outcome, or propagate errors without fabricating ACK.

        No scheduling/deadline/cancellation occurs here. A caller timing out cannot
        release this handle or assume an in-flight native PUT was cancelled.
        The additive scalar C ABI distinguishes explicit absence from failures.
        Older libraries still fold errors into hit=0: those remain unresolved.
        """
        if type(request_id) is not int or not 0 <= request_id < len(self._rows):
            raise ValueError("request_id not present in trace")
        row = self._rows[request_id]
        if row["namespace_hex"] != self._namespace:
            raise ValueError("request belongs to a different namespace handle")
        key, value = bytes.fromhex(row["key_hex"]), trace.payload(row)
        with self._lock:
            if self._closed:
                raise RuntimeError("trace client is closed")
            if row["op"] == "put":
                self._client.put([key], [value])
                return {"outcome": "put_ack"}
            return {"outcome": self._client.read_outcome(key, value)}

    def execute_batch(self, request_ids):
        """Read one same-namespace GET group, without scalar retries or reordering.

        Complete group results are returned together, even if one sibling fails.
        A raised exception invalidates the whole group; already performed I/O is
        not rolled back. The lock keeps all native backing storage alive through
        close, just as for execute().
        """
        if not isinstance(request_ids, (list, tuple)):
            raise ValueError("request_ids must be a list or tuple")
        rows = []
        for request_id in request_ids:
            if type(request_id) is not int or not 0 <= request_id < len(self._rows):
                raise ValueError("request_id not present in trace")
            row = self._rows[request_id]
            if row["op"] != "get" or row["namespace_hex"] != self._namespace:
                raise ValueError("batch requires GETs from this namespace")
            rows.append(row)
        if len(set(request_ids)) != len(request_ids):
            raise ValueError("duplicate request_id in batch")
        with self._lock:
            if self._closed:
                raise RuntimeError("trace client is closed")
            read = (self._client.read_deduplicated_outcomes if self._cpu_dedup
                    else self._client.read_outcomes)
            results = read(
                [bytes.fromhex(row["key_hex"]) for row in rows],
                [trace.payload(row) for row in rows])
            # Check the complete shape before exposing even the first sibling.
            if not isinstance(results, list) or len(results) != len(rows):
                raise RuntimeError("incomplete batch GET results")
            from eviction_batch import check_outcome
            for result in results:
                check_outcome(result)
            return [dict(result) for result in results]

    def cpu_dedup_metrics(self):
        """Observe enabled CPU shm, under the same native-handle lifetime lock."""
        with self._lock:
            if self._closed or not self._cpu_dedup:
                raise RuntimeError("CPU dedup metrics require an open explicit handle")
            from eviction_dedup import snapshot
            return snapshot(self._lib, self._client.handle)

    def close(self):
        # Never close a handle while a native call still owns its buffers.
        with self._lock:
            if not self._closed:
                self._closed = True
                self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
