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

    def __init__(self, document, lib, members, namespace, *, client_factory=None):
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
        self._client = client_factory(lib, members, namespace)
        self.transport = self._client.transport

    def execute(self, request_id):
        """Return an observed outcome, or propagate errors without fabricating ACK.

        No scheduling/deadline/cancellation occurs here. A caller timing out cannot
        release this handle or assume an in-flight native PUT was cancelled.
        The existing C ABI folds some network errors and capacity failures into
        hit=0. Even EXIST returns false on some errors, so no-hit is unresolved,
        never a proven cache miss or a successful measurement terminal.
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
            hits = self._client.read([key], [value], allow_miss=True)
            if hits == 1:
                return {"outcome": "get_hit"}
            if hits != 0:
                raise RuntimeError("invalid single-key hit count")
            return {"outcome": "get_unresolved"}

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
