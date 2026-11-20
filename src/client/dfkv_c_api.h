/* Native v2-only C ABI for the DingoFS KV client. Namespaces and object keys
 * are explicit length-delimited binary identities; values are opaque bytes.
 * Every pointed-to key is copied before its call returns. */
#ifndef DFKV_DFKV_C_API_H_
#define DFKV_DFKV_C_API_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* dfkv_client_t;

#define DFKV_CLIENT_ABI_VERSION_V2 2u
#define DFKV_CLIENT_OPT_REGISTER_WITH_MDS (1u << 0)

// Versioned, size-delimited client construction contract. All pointed-to data
// is copied before dfkv_open_v2 returns. A zero/NULL optional field selects the
// documented default. Unknown flags, a short struct, or a non-v2 version fail
// closed and return NULL.
typedef struct dfkv_client_options_v2 {
  uint32_t struct_size;             // set to sizeof(dfkv_client_options_v2)
  uint32_t abi_version;             // DFKV_CLIENT_ABI_VERSION_V2
  const char* members;              // optional static "name=ip:port,..." list
  const void* key_namespace;        // required length-delimited binary identity
  uint64_t key_namespace_len;       // required; namespace may contain NUL bytes
  uint64_t batch_concurrency;       // 0 = automatic
  const char* mds_endpoints;        // optional comma-separated discovery seeds
  const char* mds_group;            // default "default"
  int32_t mds_poll_ms;              // <=0 => 3000
  uint32_t flags;                   // DFKV_CLIENT_OPT_*
  const char* client_id;            // required with REGISTER_WITH_MDS
  const char* client_info;          // optional registration metadata
  int32_t client_heartbeat_ms;      // <=0 => 10000
  uint32_t reserved0;               // must be zero
} dfkv_client_options_v2;

dfkv_client_t dfkv_open_v2(const dfkv_client_options_v2* options);

// PUT values must be non-empty. A zero byte count fails before routing.
int dfkv_put(dfkv_client_t c, const void* key, uint64_t key_len,
             const void* ptr, uint64_t n);  // 0=ok,-1=failure
int dfkv_get(dfkv_client_t c, const void* key, uint64_t key_len,
             void* ptr, uint64_t n);  // 1=hit,0=miss/failure
// Variable-size get: writes the stored payload (whatever length it was put with)
// into ptr (which has capacity `cap`) and reports the actual byte length via
// *out_len. Unlike dfkv_get, the caller does NOT need to know the exact stored
// size; a stored payload larger than cap is a miss. 1=hit, 0=miss/failure.
int dfkv_get_auto(dfkv_client_t c, const void* key, uint64_t key_len,
                  void* ptr, uint64_t cap, uint64_t* out_len);
// Stable scalar/batch GET results, independent of the internal C++ Status
// layout. Macros keep this header C-compatible without exposing an enum
// representation.
#define DFKV_GET_OK 0
#define DFKV_GET_NOT_FOUND 1
#define DFKV_GET_CACHE_FULL 2
#define DFKV_GET_QUOTA_EXCEEDED 3
#define DFKV_GET_IO_ERROR 4
#define DFKV_GET_INVALID 5
#define DFKV_GET_RESOURCE_EXHAUSTED 6
#define DFKV_GET_NO_COMPATIBLE_RAIL 7
#define DFKV_GET_INTERNAL_ERROR 8

// Additive status-preserving scalar GET. Only NOT_FOUND proves a cache miss.
// Invalid arguments (including null client) return INVALID; caught exceptions
// return INTERNAL_ERROR. IO_ERROR also includes local no-route/peer-cooldown
// refusal, not necessarily a new failed network operation. Unknown future codes
// must be treated as errors. No exception crosses this boundary.
// out_len is optional; when supplied it is cleared on entry and set only on OK.
// A zero-length OK is a hit. Discard the destination on any non-OK result: it
// may already contain partial bytes. Nonzero cap requires a non-null ptr.
// As with other C calls, pointers and non-null client handles must be live.
int dfkv_get_auto_status(dfkv_client_t c, const void* key, uint64_t key_len,
                         void* ptr, uint64_t cap, uint64_t* out_len);
int dfkv_exist(dfkv_client_t c, const void* key, uint64_t key_len);  // 1/0
// Drop a key from the cache (LMCache L2 eviction). Returns 1 if the owning node
// confirmed the op (block removed OR already absent), 0 on route/health/IO error.
int dfkv_remove(dfkv_client_t c, const void* key, uint64_t key_len);  // 1/0
// Register a large host memory region (e.g. the whole SGLang host KV pool) so
// put/get into any covered buffer uses a pre-registered RDMA MR. Call once at
// startup after allocation and before traffic. Copying transports succeed
// without work. Returns 0 only when the full range is ready, -1 otherwise.
int dfkv_register_memory(dfkv_client_t c, const void* base, uint64_t size);
// Runtime payload-segment limit reported by this client's active transport.
// Size batch_put_sg / batch_get_auto_sg grouping from this value; do not assume
// a fixed HCA or transport limit. Returns 0 on a null client.
uint32_t dfkv_max_sg_segs(dfkv_client_t c);

// Effective client-local logical object ceiling in bytes, not a negotiated
// server limit. Sum SG segment sizes for one object. Returns 0 for unsupported
// transports (including TCP), a null client, or an error.
uint64_t dfkv_max_block_bytes(dfkv_client_t c);

// Actual client transport selected by dfkv_open_v2(), e.g. "rdma",
// "tcp(rdma-not-requested)", or "injected". Returns "" for null clients.
const char* dfkv_transport_mode(dfkv_client_t c);

// Batched, concurrently fanned out. Keys are binary spans
// (keys[i], key_lens[i]); zero-length/null/overflow keys fail their slot.
// PUT slots with a zero-length value also fail without affecting valid siblings.
// out_* arrays (len n) receive per-item results. Return 0 on call success.
int dfkv_batch_put(dfkv_client_t c, const void* const* keys,
                   const uint64_t* key_lens, const void** ptrs,
                   const uint64_t* sizes, int n, int* out_ok);
int dfkv_batch_get(dfkv_client_t c, const void* const* keys,
                   const uint64_t* key_lens, void** ptrs,
                   const uint64_t* sizes, int n, int* out_hit);
int dfkv_batch_get_auto(dfkv_client_t c, const void* const* keys,
                        const uint64_t* key_lens, void** ptrs,
                        const uint64_t* caps, int n, int* out_hit,
                        uint64_t* out_len);
// Additive direct-only status batch GET; uses the scalar DFKV_GET_* codes.
// Return 0 means all n per-item results are available, NOT that all are OK.
// Only NOT_FOUND proves absence. Return -1 rejects the entire call: discard all
// results/buffers; reads may already have happened, with no rollback/cancel.
// A live non-null client is required even for n=0. For n>0 all descriptor
// arrays and out_status are required; out_len is optional. For n=0 arrays may
// be null. n<0 touches no outputs. Otherwise supplied status/length arrays are
// set to INVALID/0 before validation. Caught exceptions return -1 with all
// statuses INTERNAL_ERROR and lengths 0. Executor-contained item exceptions may
// instead produce return 0 with IO_ERROR slots. No exception crosses this
// boundary. Invalid keys/buffers fail only their slot as INVALID; valid
// siblings execute. Null destination with cap=0 is valid. Only OK sets length
// (0 is still a hit). Discard failed buffers, which may contain partial bytes.
// Always bypasses opt-in dedup, potentially adding I/O vs the old API; no SG
// status propagation. All supplied arrays must have n live elements. Output
// arrays must not overlap each other, input descriptors, keys, or destinations;
// concurrent destination buffers must not overlap. Handles/storage stay valid
// until this call returns.
int dfkv_batch_get_auto_status(dfkv_client_t c, const void* const* keys,
                               const uint64_t* key_lens, void** ptrs,
                               const uint64_t* caps, int n, int* out_status,
                               uint64_t* out_len);
// Explicit CPU-deduplicated variant with the same argument, output, status and
// whole-call failure contract as dfkv_batch_get_auto_status above. CPU sharing
// requires DFKV_CLIENT_NODE_DEDUP=1 when opening the client; otherwise direct.
// Reuses successful payloads under the existing TTL/freshness rules, not a
// coherent cache or proof that the server is currently reachable. Non-OK
// statuses are not shared: waiters use the existing per-item bounded wait,
// then return their own direct read's status. Abort is not immediate wakeup;
// multiple waits may accumulate. No batch-wide deadline/cancel is introduced.
// Device destinations bypass CPU shm; this does not enable GPU dedup or SG.
// Existing direct-only and legacy symbols retain their behavior.
int dfkv_batch_get_auto_deduplicated_status(dfkv_client_t c,
                                            const void* const* keys,
                                            const uint64_t* key_lens,
                                            void** ptrs, const uint64_t* caps,
                                            int n, int* out_status,
                                            uint64_t* out_len);
int dfkv_batch_exist(dfkv_client_t c, const void* const* keys,
                     const uint64_t* key_lens, int n, int* out_exist);
int dfkv_batch_remove(dfkv_client_t c, const void* const* keys,
                      const uint64_t* key_lens, int n, int* out_ok);

// Scatter-gather batch put: each of the n keys gathers num_bufs[i] non-contiguous
// source buffers (ptrs[i][0..num_bufs[i]-1], sizes[i][...]) into one stored blob
// (the in-order concatenation; segment boundaries are client-side only). Coalesces
// many tiny KV chunks into one key + one transport SG op. A key exceeding
// dfkv_max_sg_segs(c), whose total byte length is zero, or whose total overflows
// size_t reports out_ok[i]=0 without affecting valid siblings. Return 0 on call
// success.
int dfkv_batch_put_sg(dfkv_client_t c, const void* const* keys,
                      const uint64_t* key_lens, const void*** ptrs,
                      const uint64_t** sizes, const int* num_bufs, int n,
                      int* out_ok);
// Scatter-gather variable-size batch get: each key's stored blob is scattered
// across num_dsts[i] destination buffers (dsts[i][...], caps[i][...]) in order
// (segment sizes define the split). Accepts any stored size <= sum(caps[i]).
// out_hit[i]=1/0; out_len[i] receives the true stored payload byte length (0 on
// miss). A key exceeding the runtime SG limit, or whose capacity sum overflows
// size_t, is a miss. Return 0 on success.
int dfkv_batch_get_auto_sg(dfkv_client_t c, const void* const* keys,
                           const uint64_t* key_lens, void*** dsts,
                           const uint64_t** caps, const int* num_dsts, int n,
                           int* out_hit, uint64_t* out_len);

// Additive direct-only SG status GET using the scalar DFKV_GET_* codes.
// Return 0 means n per-item outcomes are available, not that every read hit;
// only NOT_FOUND proves absence. Return -1 rejects the whole call: discard all
// results and destinations, even if earlier reads modified some buffers.
// A live client is required even for n=0, when all arrays may be null. For n>0,
// all top-level descriptor arrays and out_status are required; out_len is
// optional. Negative n touches no outputs; otherwise supplied status/length
// arrays are initialized to INVALID/0. After whole-call validation, a caught
// exception returns -1 with INTERNAL_ERROR/0 for every slot. Executor-contained
// shard exceptions instead return 0 with IO_ERROR slots. No exception escapes.
// Invalid keys, negative segment counts, missing nonempty nested arrays,
// nonzero capacities with null destinations, and capacity-sum overflow fail
// only their slot as INVALID. Zero segments (nested arrays may be null) and
// null segments with zero capacity are valid. Descriptor count is not limited
// to one transport WR's SGE width. Only OK sets length; zero-byte OK is a hit.
// Failed buffers may contain partial bytes and must be discarded.
// This API bypasses opt-in dedup and performs no transient miss retries, unlike
// the old SG API; I/O counts and observations may therefore differ. Existing
// interfaces, wire protocol and status values remain unchanged. Supplied
// arrays need n live elements and nested arrays need num_dsts[i] live elements.
// Output arrays must not overlap each other, keys, descriptors or destinations;
// concurrently used destination regions must not overlap. Handles and all
// descriptor/destination storage must remain valid until the call returns.
int dfkv_batch_get_auto_sg_status(dfkv_client_t c, const void* const* keys,
                                  const uint64_t* key_lens, void*** dsts,
                                  const uint64_t** caps, const int* num_dsts,
                                  int n, int* out_status, uint64_t* out_len);

// Client-side Prometheus metrics snapshot. Every public scalar/batch call is
// counted exactly once with its submitted key count, including early failures
// and rendezvous-served hits. Writes up to `cap` bytes (NUL-terminated when it
// fits) and returns the full text length excluding NUL. Returns 0 on failure.
uint64_t dfkv_stats_snapshot(dfkv_client_t c, char* buf, uint64_t cap);

// dfkv native library version (compile-time DFKV_VERSION, e.g. "1.6.3" or "dev").
// Process-global (no client handle); never NULL; the returned string is static
// and valid for the process lifetime. Connectors report it as the
// dfkv_native_version metric label so the version of the linked libdfkv.so is
// visible per connector instance (alongside daemons' dfkv_build_info gauge).
const char* dfkv_version(void);

void dfkv_close(dfkv_client_t c);

#ifdef __cplusplus
}
#endif

#endif  // DFKV_DFKV_C_API_H_
