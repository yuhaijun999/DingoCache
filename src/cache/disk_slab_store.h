/* DiskSlabStore — a disk-backed KV cache node built on SlabAllocator.
 *
 * Replaces the "one file per block" KVStore geometry (whose tmp-leak / ENOSPC-
 * dead-end / unbounded-inode / lock-held-unlink pathologies all stem from that
 * base) with a fixed pool of pre-allocated EXTENT files carved into slots by the
 * media-agnostic SlabAllocator. A compact slots.tbl records which key occupies
 * each slot so the index rebuilds on restart -- keeping cache warmth across a
 * rolling upgrade without the per-block file churn.
 *
 * Payload I/O defaults to O_DIRECT in the production daemon; the aligned
 * transport paths avoid the page cache and a bounded buffered mode remains an
 * explicit diagnostic option. Extent descriptors stay open for the store
 * lifetime. Each table record is CRC-checked and dirty-epoch recovery fails
 * safe by discarding uncommitted cache warmth rather than serving torn bytes. */
#ifndef DFKV_DISK_SLAB_STORE_H_
#define DFKV_DISK_SLAB_STORE_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/async_read_counters.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_write_result.h"
#include "cache/new_put_demand_latch.h"
#include "cache/slab_allocator.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"

namespace dfkv {

class DiskLifetimeClock;
class DiskSlabStoreTestPeer;

class DiskSlabStore : public StoreEngine {

 public:
  struct Options {
    std::string dir;
    uint64_t capacity_bytes = (1ull << 30);
    uint64_t extent_bytes = (1ull << 30);    // 1 GiB per extent file
    // Slot-size quantum: every class slot_size is a multiple of this, so the
    // per-extent slot count (and thus slots.tbl size) is bounded. Real KV blocks
    // are MiB-scale, so 1 MiB is a fine default; tests use small values.
    uint64_t slot_granularity = (1ull << 20);
    // O_DIRECT on the aligned data paths (CacheDirect writes, RangeDirect reads,
    // async prep): zero page-cache footprint -- on GPU nodes the cache/dirty
    // growth of buffered I/O competes with training/inference memory, so the
    // DEPLOYMENT default is direct (DiskCacheGroup: DFKV_SLAB_WRITE=buffered
    // opts out); burst absorption comes from the explicit RAM tier instead.
    // Extents are fallocate'd in this mode: DIO OVERWRITES parallelize on XFS,
    // allocating writes into holes would serialize on the exclusive iolock.
    // If the filesystem rejects O_DIRECT (tmpfs in tests/CI), the store falls
    // back to buffered -- DirectWritesActive() reports the resolved truth.
    bool direct_writes = false;
    // Background free-slot reclaimer cadence (ms; 0 = off). Keeps a demand-
    // driven headroom of free slots per active class so Put's fast path stays
    // pop-free-slot; without it every Put under capacity pressure runs the
    // CLOCK eviction sweep inline under the store lock (the measured write-
    // path serialization on a full store).
    uint32_t reclaim_interval_ms = 50;
    // slots.tbl fdatasync cadence (ms; 0 = off). Bounds the crash window in
    // which a REUSED slot's new payload is on disk while the reused record is
    // still page-cache-only -- rebuild would then resurrect the PREVIOUS
    // occupant's key pointing at the new occupant's bytes. Payload writes give
    // no such window in direct mode (DIO is durable at return); this closes
    // the record side to <= the cadence instead of the ~30s dirty expiry.
    uint32_t table_sync_ms = 100;
    // Internal lifetime building block, not a policy enable switch. Startup
    // leaves this off until identity/result propagation is integrated. Legacy
    // stores allocate no identity table and retain existing eviction behavior.
    bool track_resident_identity = false;
    // Shared process origin, installed before any disk recovery. Internal
    // staging dependency only: does not enable scoring. The group prevalidates
    // this clock; direct internal callers must do the same. It gates new
    // reclaim/bind decisions, not reserved I/O or required resource cleanup.
    std::shared_ptr<DiskLifetimeClock> lifetime_clock{};
    // Internal scoring snapshot; validated before any file is opened. Requires
    // a usable clock with matching H and also enables resident identities.
    // Absent by default; does not select a new eviction policy.
    std::optional<DiskLifetimeConfig> lifetime_config{};
    // Runtime selector gate. Group enables it for explicit lifetime; an
    // injected scoring snapshot alone still preserves legacy selection.
    bool lifetime_class_eviction = false;
    // Formal clean recovery uses virtual n=2. Internal staging fixtures keep
    // n=1; neither seed represents successful business reads or disk history.
    bool lifetime_recovery_two_reads = false;
  };

  // Caller-observed completion path, not physical I/O or current residency.
  enum class LifetimeReadSource : unsigned char {
    kUnclassified = 0,   // No completion-path classification was supplied.
    kRam = 1,            // A synchronous request copied a RAM resident.
    kDisk = 2,           // An independent synchronous storage read completed.
    kCopyLeader = 3,     // A synchronous coalescer leader completed its read.
    kCopyFollower = 4,   // A follower copied the completed shared result.
    kTransportGrant = 5  // A transport established a successful read grant.
  };

  // Enabled, nonempty logical successes partition separately by outcome and
  // source. Real I/O health wins over binding/race/clock diagnostics.
  struct LifetimeReadCounters {
    uint64_t successful = 0;
    uint64_t applied = 0;
    uint64_t no_binding = 0;
    uint64_t race_lost = 0;
    // Exclusive children of race_lost, not additional logical successes.
    uint64_t race_identity_scope = 0;  // Unowned or foreign instance/epoch.
    uint64_t race_not_visible = 0;     // Store no longer exposes this key.
    uint64_t race_generation_mismatch = 0;  // Allocator generation differs.
    uint64_t race_allocator_unavailable =
        0;  // Missing/ineligible entry, not OOM.
    uint64_t clock_hard_exhausted = 0;
    uint64_t io_unhealthy = 0;
    uint64_t state_unavailable = 0;
    uint64_t source_unclassified = 0;
    uint64_t source_ram = 0;
    uint64_t source_disk = 0;
    uint64_t source_copy_leader = 0;
    uint64_t source_copy_follower = 0;
    uint64_t source_transport_grant = 0;
    // A separate partition of transport grants by preparation path only.
    // These do not add to the six-source partition above or prove payload I/O.
    uint64_t transport_unclassified = 0;
    uint64_t transport_ram_hit = 0;
    uint64_t transport_disk_staging = 0;
    uint64_t transport_disk_to_ram = 0;
    uint64_t transport_published_follower = 0;
    uint64_t transport_synchronous_fallback = 0;
    // Nested partition of synchronous fallbacks, not additional grants.
    uint64_t transport_fallback_unclassified = 0;
    uint64_t transport_fallback_ram_hit = 0;
    uint64_t transport_fallback_disk = 0;
    uint64_t transport_fallback_copy_leader = 0;
    uint64_t transport_fallback_copy_follower = 0;
  };

  // Actual synchronous payload pread results, published at read finalization.
  // Excludes metadata, recovery, async I/O and external ReadLease fd reads.
  struct SyncPayloadReadCounters {
    uint64_t syscalls = 0;  // Every pread attempt, including EINTR retries.
    uint64_t bytes = 0;   // Positive kernel-returned bytes, including padding.
    uint64_t errors = 0;  // Negative pread results, including EINTR.
    uint64_t short_reads =
        0;  // Nonnegative results smaller than requested; EOF too.
  };

  // One store's runtime counters (see KvNodeServer's dfkv_slab_* metrics).
  struct Stats {
    LifetimeReadCounters lifetime_reads{};
    SyncPayloadReadCounters sync_payload_reads{};
    // Explicitly attributed io_uring CQEs, published on storage-lease release.
    AsyncReadCounters uring_payload_reads{};
    uint64_t lifetime_class_scan_visits = 0;
    uint64_t lifetime_class_scan_slices = 0;
    uint64_t lifetime_scan_budget_exhausted = 0;
    uint64_t lifetime_class_no_candidate = 0;
    uint64_t lifetime_class_victims = 0;
    uint64_t lifetime_class_freed_slot_bytes = 0;
    uint64_t lifetime_class_freed_payload_bytes = 0;
    uint64_t lifetime_background_scan_visits = 0;
    uint64_t lifetime_background_scan_slices = 0;
    uint64_t lifetime_background_budget_exhausted = 0;
    uint64_t lifetime_background_no_candidate = 0;
    // Successful background class victims only; excludes watermark/donor work.
    uint64_t lifetime_background_victims = 0;
    uint64_t lifetime_background_freed_slot_bytes = 0;
    uint64_t lifetime_background_freed_payload_bytes = 0;
    uint64_t lifetime_watermark_budget_exhausted = 0;
    uint64_t lifetime_watermark_no_candidate = 0;
    uint64_t lifetime_watermark_pin_skips = 0;
    uint64_t lifetime_watermark_protected_victims = 0;
    uint64_t lifetime_donor_coarse_extents = 0;
    uint64_t lifetime_donor_precise_extents = 0;
    uint64_t lifetime_donor_precise_residents = 0;
    uint64_t lifetime_donor_budget_fallback = 0;
    uint64_t lifetime_donor_unavailable = 0;
    uint64_t lifetime_donor_persistence_errors = 0;
    uint64_t lifetime_donor_selection_ns = 0;
    uint64_t lifetime_donor_persistence_ns = 0;
    uint64_t lifetime_donor_released_residents = 0;
    uint64_t lifetime_donor_freed_slot_bytes = 0;
    uint64_t lifetime_donor_freed_payload_bytes = 0;
    uint64_t lifetime_donor_protected_victims = 0;
    uint64_t lifetime_donor_protected_payload_bytes = 0;
    uint64_t lifetime_donor_unscored_victims = 0;
    uint64_t lifetime_bucket_entries = 0;
    uint64_t lifetime_bucket_bytes = 0;
    uint64_t lifetime_bucket_token_bytes = 0;
    // Retained lifetime/identity vector backing, not live entries or RSS.
    uint64_t lifetime_identity_bytes = 0;
    uint64_t lifetime_state_bytes = 0;
    uint64_t lifetime_link_bytes = 0;
    uint64_t lifetime_queue_bytes = 0;
    uint64_t lifetime_version_bytes = 0;
    // Retained base metadata arrays, measured as capacity * element size.
    // Excludes nested Class allocations, fixed objects, heap overhead and RSS.
    uint64_t metadata_slot_bytes = 0;
    uint64_t metadata_extent_bytes = 0;
    uint64_t metadata_class_bytes = 0;
    uint64_t metadata_key_index_bytes = 0;
    // Nested Class vector backing only; map nodes/buckets remain excluded.
    uint64_t metadata_class_rotation_bytes = 0;
    uint64_t metadata_class_free_slot_bytes = 0;
    // Allocator-retained residents with initialized recovered lifetime state.
    // Unread means no applied real reads; virtual recovery seeds do not count.
    uint64_t lifetime_recovered_objects = 0;
    uint64_t lifetime_recovered_unread_objects = 0;

    uint64_t dio_write_fallbacks = 0;
    uint64_t dio_read_fallbacks = 0;
    uint64_t table_syncs = 0;
    uint64_t bind_wipes = 0;
    uint64_t steals = 0;
    uint64_t cold_steals = 0;
    uint64_t watermark_evictions = 0;
    uint64_t watermark_extent_clears = 0;
    // Global slot reclamation and durable invalidation work.
    uint64_t watermark_slot_removals = 0;
    uint64_t watermark_slot_record_clears = 0;
    uint64_t watermark_record_clear_bytes = 0;
    uint64_t watermark_freed_slot_bytes = 0;
    // Committed payload removed by successful watermark victims, not slot
    // capacity or metadata I/O bytes. Failed invalidations do not contribute.
    uint64_t watermark_freed_payload_bytes = 0;
    uint64_t watermark_scan_visits = 0;
    uint64_t watermark_max_victims_per_tick = 0;
    uint64_t watermark_max_visits_per_tick = 0;
    uint64_t watermark_ticks = 0;
    uint64_t watermark_active = 0;
    uint64_t watermark_last_tick_us = 0;
    uint64_t watermark_max_tick_us = 0;
    uint64_t extent_returns = 0;
    uint64_t deferred_removes = 0;
    uint64_t inflight = 0;
    uint64_t prep_holds = 0;
    uint64_t reclaimed_slots = 0;
    uint64_t rebalanced_extents = 0;
    uint64_t batched_writes = 0;
    uint64_t uring_write_batches = 0;
    uint64_t metadata_io_errors = 0;
    // Successful NEW/recovered residents left deliberately unscored when their
    // initialization crossed the hard clock bound. Not failed disk writes.
    uint64_t lifetime_init_clock_hard_exhausted = 0;
    uint64_t unclean_resets = 0;
    uint64_t eviction_record_clears = 0;
    uint64_t record_writes = 0;
    uint64_t table_rebuilt = 0;
    uint64_t rebuild_corrupt_records = 0;
    uint64_t rebuild_rejected_records = 0;
    uint64_t rebuild_scanned_bytes = 0;
    uint64_t rebuild_scan_chunks = 0;
    uint64_t rebuild_sparse_ranges = 0;
    uint64_t rebuild_sequential_fallbacks = 0;
    uint64_t rebuild_mmap_scans = 0;
    uint64_t capacity_bytes = 0;
    uint64_t allocated_bytes = 0;
    // Reserved resident lengths, including uncommitted and hidden residents.
    // These two fields share one class snapshot, not the visible payload view.
    uint64_t allocator_payload_bytes = 0;
    uint64_t allocator_fragmentation_bytes = 0;
    uint64_t payload_bytes = 0;
    uint64_t allocator_objects = 0;
    uint64_t committed_objects = 0;
    uint64_t class_count = 0;
    uint64_t bound_extents = 0;
    uint64_t pool_extents = 0;
    uint64_t failed_disks = 0;
    bool failed = false;
  };

  // A narrow locked view: no allocator class enumeration or temporary vectors.
  struct WatermarkObservation {
    uint64_t watermark_high_observations;
    uint64_t watermark_last_high_bytes;
    uint64_t watermark_peak_high_bytes;  // lifetime sampled peak, not phase proof
    uint64_t run_epoch;
    uint64_t capacity_bytes;
    uint64_t watermark_high_bytes;
    uint64_t watermark_low_bytes;
    uint64_t reclaim_interval_ms;
    uint64_t watermark_active;
    uint64_t watermark_freed_payload_bytes;
  };
  WatermarkObservation GetWatermarkObservation() const {
    std::lock_guard<std::mutex> lk(mu_);
    return {watermark_high_observations_,
            watermark_last_high_bytes_,
            watermark_peak_high_bytes_,
            run_epoch_,
            static_cast<uint64_t>(opt_.extent_bytes) * num_extents_,
            evict_high_bytes_,
            evict_low_bytes_,
            opt_.reclaim_interval_ms,
            watermark_active_.load(std::memory_order_relaxed) ? 1u : 0u,
            watermark_freed_payload_bytes_};
  }

  // Opens (or creates) the store under Options::dir, pre-allocating extents and
  // rebuilding the index from slots.tbl. An interrupted first initialization
  // resumes only with a matching initialization marker, absent slab_state,
  // zeroed table and strictly validated directory/extent layout; unknown and
  // previously populated layouts are never auto-recreated. `*ok` (nullable)
  // reports success; on fatal open error every op returns kIOError and
  // StartupError() includes the failing syscall's errno when applicable.
  explicit DiskSlabStore(Options opt, bool* ok = nullptr);
  ~DiskSlabStore();

  // Pure configuration check, shared by single-store and all-disk startup.
  // Does not inspect or modify disk files. error must be non-null; it is
  // cleared on success and receives a diagnostic on failure.
  static bool ValidateGeometry(const Options& options, std::string* error);

  // Read-only geometry/header check of slab_meta, sharing normal startup's
  // decoder. Missing metadata is left to fresh/resume/migration handling.
  // Not a full layout/epoch check or a guarantee against later I/O failures.
  // error must be non-null; it is cleared on success.
  static bool ValidateExistingMeta(const Options& options, std::string* error);

  // Read-only slab_initializing format/geometry check. ENOENT is normal; an
  // existing marker is never removed or repaired by this check.
  // error must be non-null; it is cleared on success.
  static bool ValidateExistingInitMarker(const Options& options,
                                         std::string* error);

  // Read-only slab_state format/CRC/state/epoch-range check. ENOENT is left to
  // the existing migration/resume path; it does not create or repair files.
  // error must be non-null; it is cleared on success.
  static bool ValidateExistingEpoch(const Options& options, std::string* error);

  // Read-only fresh/resume/existing layout guard and table/extent check.
  // Missing meta requires a missing/empty root or a valid initialization marker
  // without state. Resume requires zero table content and rejects data-file
  // symlinks, but permits missing files. Resume also requires a real root
  // directory and only known root entries and canonical in-range extent names.
  // error must be non-null, cleared on success.
  static bool ValidateExistingDataFiles(const Options& options,
                                        std::string* error);

  DiskSlabStore(const DiskSlabStore&) = delete;
  DiskSlabStore& operator=(const DiskSlabStore&) = delete;

  Status Cache(const BlockKey& key, const void* data, size_t len) override;
  // Concrete-store NEW PUT result: requires tracking and a non-null output.
  // Clears *identity on entry; only the real successful NEW leader receives
  // a credential captured under the commit lock. Keep-first/flight followers,
  // failures and a write removed before commit return no credential, even if
  // Status is kOk. Never bind a duplicate's attempted bytes to an old resident.
  // A later Remove/eviction can invalidate this credential; it is not a pin.
  // Identity exhaustion rejects NEW admissions with kCacheFull, logs once,
  // and cannot be fixed by eviction. Typed reason/drain propagation remains
  // required before enabling lifetime; transport-only statuses are not reused.
  Status CacheWithIdentity(const BlockKey& key, const void* data, size_t len,
                           DiskResidentIdentity* identity);
  // Writes an already aligned transport buffer without a value envelope.
  // Direct mode uses O_DIRECT; the bounded fallback preserves raw bytes.
  Status CacheDirect(const BlockKey& key, char* data, size_t len,
                     size_t cap) override;
  // Batched CacheDirect: ONE lock hold allocates every slot, payloads go out
  // unlocked (io_uring one-submit when built+enabled, else a sequential loop),
  // ONE lock hold commits. Per-item semantics identical to CacheDirect.
  std::vector<Status> CacheDirectBatch(const std::vector<CacheBatchItem>& items) override;
  DiskWriteResult CacheWithResult(const BlockKey& key, const void* data,
                                  size_t len) override;
  DiskWriteResult CacheDirectWithResult(const BlockKey& key, char* data,
                                        size_t len, size_t cap) override;
  std::vector<DiskWriteResult> CacheDirectBatchWithResults(
      const std::vector<CacheBatchItem>& items) override;
  Status Range(const BlockKey& key, uint64_t offset, uint64_t length,
               std::string* out, size_t* value_len = nullptr) override;
  Status RangeInto(const BlockKey& key, uint64_t offset, uint64_t length,
                   char* dst, size_t dst_cap, size_t* out_len,
                   size_t* value_len = nullptr) override;
  // Reads into the caller's bounded registered buffer and returns the payload
  // slice directly; direct mode uses its aligned O_DIRECT window.
  Status RangeDirect(const BlockKey& key, uint64_t offset, uint64_t length,
                     char* io_buf, size_t io_cap, const char** out_data,
                     size_t* out_len, size_t* value_len = nullptr) override;
  // With internal lifetime metadata enabled, successful nonzero reads retain
  // original byte provenance even after removal. It is not a residency proof;
  // NotifyLifetimeReadSuccess validates it again. Failed/empty reads clear it.
  Status RangeWithIdentity(const BlockKey& key, uint64_t offset,
                           uint64_t length, std::string* out,
                           DiskResidentIdentity* identity,
                           size_t* value_len = nullptr) override;
  Status RangeIntoWithIdentity(const BlockKey& key, uint64_t offset,
                               uint64_t length, char* dst, size_t dst_cap,
                               size_t* out_len, DiskResidentIdentity* identity,
                               size_t* value_len = nullptr) override;
  Status RangeDirectWithIdentity(const BlockKey& key, uint64_t offset,
                                 uint64_t length, char* io_buf, size_t io_cap,
                                 const char** out_data, size_t* out_len,
                                 DiskResidentIdentity* identity,
                                 size_t* value_len = nullptr) override;
  // Async prep pins the resolved slot and returns a move-only lease over both
  // the dup'd extent descriptor and that pin. Abandoning or completing the read
  // destroys the lease and balances the hold without a manual release protocol.
  Status RangeDirectPrep(const BlockKey& key, uint64_t offset, uint64_t length,
                         size_t io_cap, ReadLease* out) override;
  bool IsCached(const BlockKey& key) const override;
  Status Lookup(const BlockKey& key, ValueMetadata* out) const override;
  Status Remove(const BlockKey& key) override;

  size_t Count() const override;
  uint64_t UsedBytes() const override;
  uint64_t TenantUsedBytes(uint64_t tenant_hash) const override;
  uint64_t Capacity() const;
  uint64_t Evictions() const override;
  uint64_t EvictedBytes() const override;
  const std::string& Dir() const override { return opt_.dir; }
  uint64_t TableRebuilt() const { return table_rebuilt_; }  // records restored at open
  // Resolved I/O mode: direct_writes requested AND the filesystem took O_DIRECT.
  bool DirectWritesActive() const { return !extent_dio_fds_.empty(); }
  // Logical request success only, not a storage read/fill notification. The
  // captured identity proves byte provenance, not current residency: recheck
  // the same generation before heating, never reconstruct it from the key.
  bool TracksLifetimeState() const { return opt_.lifetime_config.has_value(); }
  // transport_source is used only for kTransportGrant; other sources ignore it.
  void NotifyLifetimeReadSuccess(
      const BlockKey& key, const DiskResidentIdentity& identity, size_t bytes,
      LifetimeReadSource source = LifetimeReadSource::kUnclassified,
      PreparedRead::Source transport_source =
          PreparedRead::Source::kUnclassified);
  Stats GetStats() const;
  std::vector<SlabAllocator::ClassStat> ClassStats() const;
  // Metrics-only retained queue populations; not scores or logical read
  // classes.
  std::vector<SlabAllocator::LifetimeQueueStat> LifetimeQueueStats() const;
  // Current coarse protection estimate; unavailable is not an empty cache.
  SlabAllocator::LifetimeBucketObservation ObserveLifetimeBuckets() const;
  bool Healthy() const override {
    return ok_ && !metadata_failed_.load(std::memory_order_acquire);
  }
  const std::string& StartupError() const override { return startup_error_; }
  // Sticky owner veto, set before destruction begins. Still syncs payload and
  // table on close, but leaves the epoch dirty; does not change I/O health.
  void PreventCleanShutdown() {
    clean_shutdown_allowed_.store(false, std::memory_order_relaxed);
  }

 private:
  friend class DiskSlabStoreTestPeer;
  static constexpr size_t kRecBytes = 64;

  bool ValidateOptions();
  bool SetStartupError(std::string error);
  bool SetStartupErrno(std::string error, int saved_errno);
  bool ClearInitMarker();
  bool OpenOrInit();
  bool Rebuild();
  bool OpenEpochState(bool fresh);
  bool ResetUncleanEpoch();
  bool WriteEpochState(uint32_t state, uint64_t epoch);
  bool MarkDirtyEpoch();
  bool MarkCleanEpoch();
  bool SyncPayloads();
  bool WriteRecord(const SlabAllocator::SlotRef& r, const BlockKey& key,
                   uint32_t payload_len, bool valid);
  bool SyncTable(uint64_t seen);
  bool FailMetadata();
  bool WritePayload(const SlabAllocator::SlotRef& r, const void* data, size_t len);
  // O_DIRECT payload write from the caller's ALIGNED buffer (CacheDirect path):
  // zeroes the padding bytes in place (cap allows) and pwrites the 4 KiB-rounded
  // length via the extent's DIO fd. Caller pre-checked alignment/cap fit.
  bool WritePayloadDirect(const SlabAllocator::SlotRef& r, char* data, size_t len);
  bool ReserveResidentGenerationLocked(uint64_t* generation);
  DiskResidentIdentity ResidentIdentityLocked(const BlockKey& key,
                                              const std::string& fn) const;
  void CommitPayloadLocked(const BlockKey& resident_key, const std::string& key,
                           uint32_t payload_len, uint64_t generation,
                           bool recovered = false);
  // Visible payload/tenant accounting only; deferred Remove hides immediately.
  uint32_t ErasePayloadLocked(const std::string& key);
  // Shared allocate/pin -> unlocked payload write -> commit skeleton for
  // Cache/CacheDirect; `write_payload` does just the payload I/O for the slot.
  template <typename WriteFn>
  Status CacheImpl(const BlockKey& key, size_t len,
                   const WriteFn& write_payload,
                   DiskWriteResult* details = nullptr);
  Status CacheDirectImpl(const BlockKey& key, char* data, size_t len,
                         size_t cap, DiskWriteResult* details);
  std::vector<Status> CacheDirectBatchImpl(
      const std::vector<CacheBatchItem>& items,
      std::vector<DiskWriteResult>* details);
  Status RangeImpl(const BlockKey& key, uint64_t offset, uint64_t length,
                   std::string* out, size_t* value_len,
                   DiskResidentIdentity* identity);
  Status RangeIntoImpl(const BlockKey& key, uint64_t offset, uint64_t length,
                       char* dst, size_t dst_cap, size_t* out_len,
                       size_t* value_len, DiskResidentIdentity* identity);
  Status RangeDirectImpl(const BlockKey& key, uint64_t offset, uint64_t length,
                         char* io_buf, size_t io_cap, const char** out_data,
                         size_t* out_len, size_t* value_len,
                         DiskResidentIdentity* identity);
  // Caller holds D. Last release may change the final status; only keep the
  // originally captured source if it is still visible, never relabel by key.
  Status FinishReadLocked(const BlockKey& key, const std::string& fn,
                          Status status, size_t bytes,
                          DiskResidentIdentity* identity,
                          const SyncPayloadReadCounters* io = nullptr);
  // Returns with lock held. The caller retains its unique PutFlight across
  // all slices; this helper never re-enters owner/follower arbitration.
  DiskWriteResult AllocateForPutLocked(const BlockKey& key, size_t len,
                                       SlabAllocator::SlotRef* ref,
                                       std::unique_lock<std::mutex>& lock);
  struct PutFlight {
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    DiskWriteResult result{Status::kIOError};
  };
  static void CompletePut(const std::shared_ptr<PutFlight>& flight,
                          const DiskWriteResult& result);
  static DiskWriteResult WaitPut(const std::shared_ptr<PutFlight>& flight);
  // Resolve fn's slot + committed payload length and acquire it for an unlocked
  // read (pin + inflight count, under mu_). False = miss (absent, or a write
  // still in flight). Balanced by a Release{...}Locked call.
  bool AcquireForRead(const BlockKey& key, const std::string& fn,
                      SlabAllocator::SlotRef* ref, uint32_t* plen,
                      DiskResidentIdentity* identity = nullptr);
  // Drop one in-flight hold on fn (unpin + count). The LAST releaser performs
  // the durable invalidation for a Remove that arrived while I/O was active.
  // SlabAllocator itself also defers pinned removal, but this caller-level state
  // is still required to order the slots.tbl record before physical reuse.
  // Call with mu_ held.
  bool ReleaseInflightLocked(const BlockKey& key, const std::string& fn);
  static Status ReleaseReadLeaseThunk(void* owner, uint64_t token, size_t bytes,
                                      DiskResidentIdentity* identity,
                                      const AsyncReadCounters& io) noexcept;
  Status ReleaseReadLease(uint64_t token, size_t bytes,
                          DiskResidentIdentity* identity,
                          const AsyncReadCounters& io) noexcept;
  uint64_t TableOffset(uint32_t extent, uint32_t slot) const {
    return static_cast<uint64_t>(extent) * max_slots_per_extent_ * kRecBytes +
           static_cast<uint64_t>(slot) * kRecBytes;
  }

  Options opt_;
  uint32_t num_extents_ = 0;
  uint32_t max_slots_per_extent_ = 0;
  std::vector<char> zero_extent_records_;
  std::unique_ptr<SlabAllocator> alloc_;
  std::vector<int> extent_fds_;      // resident, one per extent (buffered)
  std::vector<int> extent_dio_fds_;  // O_DIRECT twins (only when direct_writes)
  int table_fd_ = -1;                // slots.tbl
  int state_fd_ = -1;                // slab_state clean/dirty epoch marker
  uint64_t run_epoch_ = 0;
  bool initializing_ = false;        // validated on-disk init marker to clear
  bool unclean_start_ = false;
  // Outstanding asynchronous leases. The opaque id remains entirely internal:
  // ReadLease destruction routes it back through ReleaseReadLeaseThunk.
  struct PrepHold { BlockKey key; std::string fn; };
  std::unordered_map<uint64_t, PrepHold> prep_holds_;
  uint64_t prep_token_seq_ = 0;
  // key.Filename() -> its true payload length (slot_size >= this). Rebuilt on
  // open from the table; the allocator only tracks slot size.
  std::unordered_map<std::string, uint32_t> payload_len_;
  // Enabled-only store identity. Failed writes consume reserved generations;
  // physical per-resident generations live in the allocator's dense array and
  // are cleared only when the slot is actually freed, not when hidden.
  struct ResidentIdentityState {
    uint64_t store_instance = 0;
    uint64_t sequence = 0;
    bool exhausted = false;
  };
  std::unique_ptr<ResidentIdentityState> resident_identity_;
  std::unordered_map<uint64_t, uint64_t> tenant_used_bytes_;
  // Keys with an unlocked pread/pwrite in flight (value = op count), and keys
  // whose Remove arrived during that window (executed by the last releaser).
  std::unordered_map<std::string, uint32_t> inflight_;
  std::unordered_set<std::string> deferred_remove_;
  // Signaled by the last in-flight releaser right after it executes a deferred
  // Remove (mu_ held): a re-PUT of that key waits out the window (bounded)
  // thereon instead of failing hard against the still-resident slot.
  std::condition_variable remove_cv_;
  // A reserved-but-uncommitted key has exactly one leader. Scalar and batch
  // duplicates join this flight and observe its real commit result.
  std::unordered_map<std::string, std::shared_ptr<PutFlight>> put_flights_;
  // Guards the in-memory maps and the allocate/lookup+pin step ONLY -- never
  // held across payload I/O (a pin protects the slot from eviction in the
  // unlocked window, inflight_/deferred_remove_ protect it from Remove;
  // payload_len_ install after the write is the reader-visible commit).
  mutable std::mutex mu_;
  uint64_t lifetime_class_scan_visits_ = 0;
  uint64_t lifetime_class_scan_slices_ = 0;
  uint64_t lifetime_scan_budget_exhausted_ = 0;
  uint64_t lifetime_class_no_candidate_ = 0;
  uint64_t lifetime_class_victims_ = 0;
  // Successful foreground class byte counters, guarded by mu_.
  uint64_t lifetime_class_freed_slot_bytes_ = 0;
  uint64_t lifetime_class_freed_payload_bytes_ = 0;
  uint64_t lifetime_background_scan_visits_ = 0;
  uint64_t lifetime_background_scan_slices_ = 0;
  uint64_t lifetime_background_budget_exhausted_ = 0;
  uint64_t lifetime_background_no_candidate_ = 0;
  // Successful background class counters, guarded by mu_.
  uint64_t lifetime_background_victims_ = 0;
  uint64_t lifetime_background_freed_slot_bytes_ = 0;
  uint64_t lifetime_background_freed_payload_bytes_ = 0;
  uint64_t lifetime_watermark_budget_exhausted_ = 0;
  uint64_t lifetime_watermark_no_candidate_ = 0;
  uint64_t lifetime_watermark_pin_skips_ = 0;
  uint64_t lifetime_watermark_protected_victims_ = 0;

  // Invoked only between class slices, after releasing BOTH lifecycle locks.
  std::function<void()> lifetime_scan_yield_hook_for_test_{};
  LifetimeReadCounters lifetime_reads_{};  // under mu_
  SyncPayloadReadCounters sync_payload_reads_{};  // under mu_
  AsyncReadCounters uring_payload_reads_{};       // under mu_
  uint64_t committed_payload_bytes_ = 0;  // under mu_
  uint64_t lifetime_init_clock_hard_exhausted_ = 0;  // under mu_
  bool ok_ = false;
  std::atomic<bool> metadata_failed_{false};
  std::atomic<bool> clean_shutdown_allowed_{true};
  std::string startup_error_;
  uint64_t table_rebuilt_ = 0;
  uint64_t evicted_bytes_ = 0;
  uint64_t watermark_freed_payload_bytes_ = 0;  // under mu_
  uint64_t deferred_remove_total_ = 0;  // under mu_
  std::atomic<uint64_t> dio_write_fallbacks_{0};
  std::atomic<uint64_t> dio_read_fallbacks_{0};
  std::atomic<uint64_t> table_syncs_{0};
  std::atomic<uint64_t> bind_wipes_{0};
  std::atomic<uint64_t> metadata_io_errors_{0};
  std::function<bool()> payload_write_hook_for_test_;
  std::function<bool()> record_write_hook_for_test_;
  // Set before I/O starts, never concurrently modified; production leaves
  // empty. Runs after read pin/inflight acquisition and after mu_ has been
  // released.
  std::function<void()> read_acquired_hook_for_test_;
  // Test-only seam (never set in production; same discipline as the hooks
  // above): when set, replaces EVERY CQE reap of the batched io_uring write
  // path -- the blocking wait AND the post-failure non-blocking drain -- so
  // tests can inject a hard reap failure. The void* signature keeps liburing
  // types out of the header (build-flag-independent class layout).
  std::function<int(void* ring, void* cqe)> uring_reap_hook_for_test_;
  std::atomic<uint64_t> unclean_resets_{0};
  std::atomic<uint64_t> eviction_record_clears_{0};
  std::atomic<uint64_t> watermark_extent_clears_{0};
  std::atomic<uint64_t> watermark_slot_record_clears_{0};
  std::atomic<uint64_t> watermark_record_clear_bytes_{0};
  std::atomic<uint64_t> watermark_freed_slot_bytes_{0};
  // Under mu_: shared callbacks count global-selector pwrite completions only.
  bool watermark_selector_call_ = false;
  // slots.tbl sync thread (see Options::table_sync_ms): fdatasync only when
  uint64_t rebuild_corrupt_records_ = 0;
  uint64_t rebuild_rejected_records_ = 0;
  uint64_t rebuild_scanned_bytes_ = 0;
  uint64_t rebuild_scan_chunks_ = 0;
  uint64_t rebuild_sparse_ranges_ = 0;
  uint64_t rebuild_mmap_scans_ = 0;
  uint64_t rebuild_sequential_fallbacks_ = 0;
  // records were written since the last cycle.
  std::atomic<uint64_t> record_writes_{0};
  uint64_t synced_marker_ = 0;  // sync-thread-local snapshot of record_writes_
  std::thread sync_thread_;
  std::condition_variable sync_cv_;
  std::mutex sync_mu_;
  bool sync_stop_ = false;
  // Background free-slot reclaimer (see Options::reclaim_interval_ms): runs
  // ReclaimTick every interval, evicting ahead of demand in bounded batches
  // and rebalancing extents from cold classes to hot ones on a full store.
  // Does not acquire mu_; allocator callbacks may call this under their lock.
  bool CanStartReclaim() const;
  // Reclaimer-thread-only pending targets keep budget-limited work eligible
  // on quiet ticks. Tests call manually only with the thread disabled.
  bool ReclaimLifetimeGrowTick();
  void ReclaimLifetimeTick(size_t max_victims = 4096);
  bool ReclaimLifetimeClass(size_t cls_index, size_t target_free,
                            size_t max_victims);
  void ReclaimTick(const std::function<void()>& after_watermark_evict = {},
                   const std::function<void()>& before_lock_for_test = {});
  // Rebalance rate cap: extents moved per tick per hot class. Each move evicts
  // a donor extent's residents + wipes its table region under the lock, so it
  // is deliberately slow-drip (converges in seconds at the 50 ms tick).
  static constexpr size_t kGrowExtentsPerTick = 2;
  std::atomic<uint64_t> reclaimed_{0};
  std::atomic<uint64_t> rebalanced_{0};
  std::atomic<uint64_t> batched_writes_{0};
  std::atomic<uint64_t> uring_write_batches_{0};
  // Kept in every build so the class layout does not depend on io_uring.
  bool uring_write_enabled_ = false;
  // The batched-write submission ring is thread_local (see disk_slab_store.cc):
  // one per flush worker, no shared lock across the blocking CQE wait.
  std::vector<size_t> lifetime_reclaim_targets_;
  std::vector<uint64_t> reclaim_last_puts_;  // reclaim-thread-local puts snapshot
  NewPutDemandLatch watermark_latch_;  // under mu_
  uint64_t new_put_bytes_since_tick_ = 0;  // successful NEW commits, under mu_
  uint64_t evict_high_bytes_ = 0;  // high crossing activates eviction
  uint64_t evict_low_bytes_ = 0;   // active eviction drains to this target
  size_t evict_max_victims_per_tick_ = 256;
  std::vector<BlockKey> watermark_evicted_;  // bounded reusable output, under mu_
  std::atomic<bool> watermark_active_{false};
  std::atomic<uint64_t> watermark_ticks_{0};
  std::atomic<uint64_t> watermark_last_tick_us_{0};
  std::atomic<uint64_t> watermark_max_tick_us_{0};
  // Reclaimer observations, read/written together under mu_.
  uint64_t watermark_high_observations_ = 0;
  uint64_t watermark_last_high_bytes_ = 0;
  uint64_t watermark_peak_high_bytes_ = 0;
  std::thread reclaim_thread_;
  std::condition_variable reclaim_cv_;
  std::mutex reclaim_mu_;
  bool reclaim_stop_ = false;
};

}  // namespace dfkv

#endif  // DFKV_DISK_SLAB_STORE_H_
