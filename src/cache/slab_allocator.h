/* SlabAllocator — media-agnostic slot lifecycle manager for the slab store.
 *
 * It owns the LAYOUT (extent, slot and byte offset) of fixed-size slots, not
 * the bytes themselves. DiskSlabStore and RamTier map each returned SlotRef to
 * their own physical medium and perform I/O outside this class. Keeping the
 * allocator pure logic makes its lifecycle, reclaim and recovery invariants
 * hermetically testable while sharing one policy across disk and RAM.
 *
 * The pool consists of equal-size extents. Each bound extent belongs to one
 * size class and is carved into uniform slots. A value is placed in the
 * smallest existing class that can hold its aligned length without exceeding
 * max_waste; otherwise a canonical class is created. Full pools reclaim with
 * CLOCK second chance and may rebind an entirely unpinned cold extent to a
 * needy class. A pinned slot is never evicted.
 *
 * Concurrency: one mutex protects the allocator's dense metadata, intrusive
 * rings and native BlockKey index. Medium I/O and persistence callbacks run
 * under the caller's lifecycle protocol; they must not create a second source
 * of truth for residency. */
#ifndef DFKV_SLAB_ALLOCATOR_H_
#define DFKV_SLAB_ALLOCATOR_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "common/kv_types.h"

namespace dfkv {

class SlabAllocator {
 public:
  // Physical placement of a key's value. `offset` is within extent `extent`.
  struct SlotRef {
    uint32_t cls = 0;
    uint32_t extent = 0;
    uint32_t slot = 0;
    uint32_t slot_size = 0;
    uint64_t offset = 0;
    bool valid() const { return slot_size != 0; }
  };

  // Stable only while the corresponding key remains resident. Callers that
  // already serialize the key lifecycle can use this handle to pin without a
  // second hash-table probe.
  struct SlotHandle {
    uint32_t index = UINT32_MAX;
    bool valid() const { return index != UINT32_MAX; }
  };

  enum class RemoveResult { kRemoved, kDeferred, kNotFound };

  struct Options {
    uint64_t extent_bytes = (1ull << 30);
    uint32_t num_extents = 8;
    uint32_t align = 4096;
    // Reuse an existing larger class when its internal waste
    // (slot_size - aligned_len) stays below this fraction.
    double max_waste = 0.25;
    std::function<bool(uint32_t extent)> on_extent_bind;
    // A last-resident eviction may clear the whole extent in one write.
    // Partial eviction requires on_slot_evict when persistence is configured.
    std::function<bool(uint32_t extent, uint32_t total_slots,
                       uint32_t resident_slots)>
        on_extent_evict;
    std::function<bool(const SlotRef&)> on_slot_evict;
    // Internal disk identity storage, not a policy switch. Disabled for RAM
    // and legacy; no generation array is allocated while disabled.
    bool track_resident_identity = false;
    // Optional decision guard, called under the allocator mutex. It must not
    // re-enter the allocator or acquire the owning store mutex. A false result
    // forbids new binding/reclaim decisions, not already-bound free-slot use
    // or finalization after successful persistence. Empty preserves legacy.
    std::function<bool()> can_reclaim{};
    // Internal lifetime storage. Enabling it also enables resident identity so
    // the generation and state commit under the same allocator lock.
    bool track_lifetime_state = false;
    // Validated disk MAX_READS (1..63); ignored when tracking is disabled.
    uint32_t lifetime_max_reads = 2;
    // Internal protection accounting; zero count disables all bucket storage.
    // Derived from validated config, never independent environment reads.
    uint64_t lifetime_bucket_sec = 0;
    uint32_t lifetime_bucket_count = 0;
    // Borrowed immutable donor context. The owning Store must outlive this
    // allocator, as for can_reclaim. Both are null in legacy/RAM/shadow paths.
    const DiskLifetimeConfig* lifetime_donor_config = nullptr;
    const DiskLifetimeClock* lifetime_donor_clock = nullptr;
  };

  // Target write parallelism per class: Put keeps up to this many extents with
  // free slots so concurrent writers stripe across inodes. This is also the
  // per-class capacity floor for rebalance: a donor is never shrunk below its
  // striping width.
  static constexpr size_t kStripeWays = 8;

  explicit SlabAllocator(Options opt);

  bool Put(const BlockKey& key, size_t len, SlotRef* out,
           std::vector<BlockKey>* evicted, SlotHandle* out_handle = nullptr);
  bool Get(const BlockKey& key, SlotRef* out);
  bool GetAndPin(const BlockKey& key, SlotRef* out);
  bool Contains(const BlockKey& key) const;

  // A rebuild record carrying the authoritative caller-visible stored length.
  struct RestoreEntry {
    BlockKey key;
    uint32_t slot_size = 0;
    uint32_t extent = 0;
    uint32_t slot = 0;
    uint64_t value_bytes = 0;
  };

  // Validate the whole batch, reserve the dense metadata/index once, then bind
  // and install it under one lock. On invalid input no resident is installed.
  bool RestoreBulk(const std::vector<RestoreEntry>& entries);

  RemoveResult Remove(const BlockKey& key);
  bool Pin(const BlockKey& key);
  bool Unpin(const BlockKey& key);
  bool Pin(const BlockKey& key, SlotHandle handle);
  bool Unpin(const BlockKey& key, SlotHandle handle);

  struct ClassStat {
    uint32_t slot_size = 0;
    size_t free_slots = 0;
    size_t resident = 0;
    uint64_t puts = 0;
    uint32_t extents = 0;
    uint32_t slots_per_extent = 0;
    uint64_t useful_bytes = 0;    // exact live caller bytes
    uint64_t allocated_bytes = 0; // slot bytes occupied by residents
    uint64_t read_touches = 0;    // cumulative Get/GetAndPin hits
    uint64_t read_heat = 0;       // recent, operation-decayed read evidence
    // Retained residents with initialized recovered state, including pins.
    uint64_t recovered_residents = 0;
    // Recovered residents with no applied real read since this startup.
    uint64_t recovered_unread_residents = 0;
  };
  std::vector<ClassStat> Classes() const;
  uint32_t PoolExtents() const;

  // Evict up to `max_victims` unpinned entries from `cls_index` in CLOCK order
  // until the class has at least `target_free` slots. The bounded background
  // sweep keeps Put on the pop-free-slot fast path. It is a no-op while an
  // unbound extent remains, because binding grows capacity without losing
  // residency. If reclaim frees an entire extent, stop before cascading into a
  // second class shrink. Evicted keys are returned so the caller can remove its
  // payload index in the same lifecycle transaction.
  size_t ReclaimClass(size_t cls_index, size_t target_free, size_t max_victims,
                      std::vector<BlockKey>* evicted);
  bool StealFrom(size_t donor_cls, size_t target_cls,
                 std::vector<BlockKey>* evicted);

  size_t Count() const;
  uint64_t UsedBytes() const;
  uint64_t Capacity() const;
  uint64_t Evictions() const;
  uint64_t Steals() const;
  uint64_t ColdSteals() const;
  // Fixed display bounds, not lifetime configuration or acceptance thresholds.
  static constexpr std::array<uint32_t, 7> kLifetimeDonorAgeBoundsSec = {
      0, 1, 60, 3600, 86400, 604800, 2592000};
  struct LifetimeDonorAgeStats {
    // Noncumulative known-history age counts; the final bin is unbounded.
    std::array<uint64_t, 8> buckets{};
    uint64_t total_sec = 0;  // Sum of max(0, pre-wipe time - anchor).
    // Recovery origin remains unknown even after subsequent real reads.
    uint64_t history_unknown = 0;
  };
  struct LifetimeDonorStats {
    LifetimeDonorAgeStats age{};
    uint64_t coarse_extents = 0;
    uint64_t precise_extents = 0;
    uint64_t precise_residents = 0;
    uint64_t budget_fallback = 0;
    uint64_t unavailable = 0;
    uint64_t persistence_errors = 0;
    uint64_t selection_ns = 0;
    uint64_t persistence_ns = 0;
    uint64_t released_residents = 0;
    // Occupied resident slots and payload, not the whole rebound extent.
    uint64_t freed_slot_bytes = 0;
    uint64_t freed_payload_bytes = 0;
    // Successful loss at the pre-wipe time, not selection's bucket estimate.
    uint64_t protected_victims = 0;
    uint64_t protected_payload_bytes = 0;
    uint64_t unscored_victims = 0;
  };
  LifetimeDonorStats DonorStats() const;
  // Preserve the original installed return type and method ABI.
  struct LifetimeBucketMemory {
    uint64_t entries =
        0;  // Logical bucket positions, including unbound extents.
    uint64_t bucket_bytes = 0;  // Reserved bucket element backing storage.
    uint64_t token_bytes = 0;   // Reserved token element backing storage.
  };
  LifetimeBucketMemory BucketMemory() const;
  struct LifetimeMemory {
    uint64_t entries =
        0;  // Logical bucket positions, including unbound extents.
    uint64_t bucket_bytes = 0;    // Reserved bucket element backing storage.
    uint64_t token_bytes = 0;     // Reserved token element backing storage.
    uint64_t identity_bytes = 0;  // Resident generation vector backing storage.
    uint64_t state_bytes =
        0;  // Optional lifetime state vector backing storage.
    uint64_t link_bytes =
        0;  // Dual-index resident link vector backing storage.
    uint64_t queue_bytes = 0;  // Global and class-local queue vector backing.
    uint64_t version_bytes =
        0;  // Class mutation version vector backing storage.
  };
  // O(1) locked capacity snapshot, not active/protected registration counts,
  // allocator overhead, vector object sizes or process/recovery peak memory.
  // Reports actual retained storage even for identity-only or shadow
  // allocators; these fields are not the full memory increment over a legacy
  // binary.
  LifetimeMemory LifetimeMemoryUsage() const;
  struct MetadataMemory {
    uint64_t slot_bytes = 0;       // SlotMeta vector backing storage.
    uint64_t extent_bytes = 0;     // ExtentMeta vector backing storage.
    uint64_t class_bytes = 0;      // Class vector backing storage.
    uint64_t key_index_bytes = 0;  // Flat key-index entry vector backing.
    uint64_t class_rotation_bytes = 0;   // Class extent rotation vectors.
    uint64_t class_free_slot_bytes = 0;  // Per-extent free-slot vectors.
  };
  // O(1) A-locked retained capacity snapshot, in every allocator mode. Includes
  // element padding and nested Class vector capacity, even when empty. Excludes
  // Class map buckets/nodes, lifetime arrays, fixed objects, heap bookkeeping
  // and RSS; this is not total allocator memory.
  MetadataMemory MetadataMemoryUsage() const;
  struct LifetimeBucketObservation {
    uint64_t protected_estimate =
        0;                       // Coarse live-page count, not exact scores.
    uint64_t summary_bytes = 0;  // Retained aggregate ring element backing.
    bool available = false;  // False when disabled or the clock is exhausted.
  };
  // O(B) A-locked current-time observation, not a historical query. The clock
  // must be the owner's shared monotonic clock; sampling advances its floor.
  // Hard exhaustion keeps backing bytes but provides no usable estimate.
  LifetimeBucketObservation ObserveLifetimeBuckets(
      const DiskLifetimeClock& clock) const;
  struct LifetimeQueueStat {
    uint32_t slot_size = 0;  // Zero denotes the global index, not a size class.
    uint32_t band = 0;  // Effective scoring band, not a logical read class.
    uint64_t residents = 0;  // Indexed residents, including hidden pinned data.
  };
  // One A-locked snapshot of both indexes; no resident walk or clock read.
  // Enabled stores include all global bands and all bands of resident classes.
  // Disabled storage returns empty. Cost is O(classes + active classes *
  // bands).
  std::vector<LifetimeQueueStat> LifetimeQueueStats() const;
  uint64_t WatermarkEvictions() const;
  // Local candidate: reclaim slots in refreshed actual-read/new-write order
  // until allocated bytes reach target_bytes. A call removes at most max_victims
  // residents and visits at most 4 * max_victims slots, preserving cursor progress.
  // Pins are skipped; reference credit belongs only to class-local CLOCK.
  // Persistence commits before removal. WatermarkEvictions counts slots here.
  size_t EvictColdToTarget(uint64_t target_bytes, size_t max_victims,
                           std::vector<BlockKey>* evicted);
  uint64_t WatermarkScanVisits() const;
  uint64_t ExtentReturns() const;
  size_t ClassCount() const;
  uint32_t BoundExtents() const;

  struct IndexStats {
    size_t size = 0;
    size_t capacity = 0;
    size_t tombstones = 0;
    uint64_t table_allocations = 0;
    uint64_t rehashes = 0;
  };
  // Test-only observability for the no-allocation hot-path contract.
  IndexStats IndexStatsForTest() const;
  size_t IndexBucketForTest(const BlockKey& key) const;

 private:
  friend class DiskSlabStore;
  friend class SlabAllocatorTestPeer;
  // Internal commit preconditions: caller serializes the resident lifecycle,
  // tracking is enabled, key is resident, generation is reserved/nonzero and
  // not yet installed. A supplied state requires lifetime tracking. With that
  // tracking enabled, nullptr deliberately leaves the committed resident
  // uninitialized (hard-stop drain), rather than inventing a default state.
  // With buckets enabled, state requires the matching validated config and
  // actual current offset (not a recovered state's anchor). Both stay under A.
  // This is not an external fallible request interface.
  void CommitResidentGeneration(
      const BlockKey& key, uint64_t generation,
      const DiskLifetimeState* lifetime_state = nullptr,
      const DiskLifetimeConfig* config = nullptr, uint32_t now = 0);
  enum class LifetimeReadResult : unsigned char {
    kApplied = 0,             // Updated the matched resident.
    kResidentChanged = 1,     // No eligible allocator resident remains.
    kClockHardExhausted = 2,  // The shared clock cannot represent this event.
    kStateUnavailable = 3,    // Missing state or inconsistent event time.
    kGenerationMismatch = 4,  // Eligible entry has a different generation.
  };
  // Caller holds the store lifecycle lock (D before A). Validate ownership,
  // sample the shared clock and update heat in one A critical section; a stale
  // credential must never sample time or mutate a later same-key resident.
  LifetimeReadResult ApplyLifetimeRead(const BlockKey& key, uint64_t generation,
                                       const DiskLifetimeConfig& config,
                                       const DiskLifetimeClock& clock);
  enum class LifetimeScanResult : unsigned char {
    kIdle = 0,             // No class scan was needed in this allocation.
    kRetry = 1,            // More class work remains; release D and A first.
    kBudgetExhausted = 2,  // This owner must terminate, not restart its budget.
    kNoCandidate = 3,      // Completed the current stable class traversal.
    kEvicted = 4,          // One checked resident was durably removed.
    kPersistenceError = 5  // Stop immediately; do not escalate to a donor.
  };
  // Owned by one logical PUT or one background class tick, never shared across
  // owners or recreated between slices. Foreground uses 2*N+2 visits;
  // background installs its own tick limit. Only positions survive unlocking,
  // not scores.
  struct LifetimeScan {
    LifetimeScan(const DiskLifetimeConfig& scoring,
                 const DiskLifetimeClock& time)
        : config(scoring), clock(time) {}
    const DiskLifetimeConfig& config;
    const DiskLifetimeClock& clock;
    uint64_t visits = 0;
    uint64_t slices = 0;
    // Cumulative successful class events for this owner, not its final result.
    // Donor work never contributes. Snapshot payload before freeing its slot.
    uint64_t victims = 0;
    uint64_t freed_slot_bytes = 0;
    uint64_t freed_payload_bytes = 0;
    uint64_t visit_limit = 0;
    uint64_t slice_limit = 0;
    uint64_t version = 0;
    bool initialized = false;
    bool progress_initialized = false;
    std::array<uint32_t, 64> starts{};
    std::array<uint32_t, 64> cursors{};
    std::array<bool, 64> done{};
    LifetimeScanResult result = LifetimeScanResult::kIdle;
  };
  bool EvictLifetimeFromLocked(size_t cls_index, LifetimeScan& scan,
                               std::vector<BlockKey>* evicted,
                               bool* reclaim_denied,
                               uint32_t max_visits = 1024);
  struct LifetimeReclaimResult {
    size_t victims = 0;
    bool more = false;  // Release D and A before continuing this tick.
    uint64_t freed_slot_bytes = 0;  // Successful class victims in this call.
  };
  LifetimeReclaimResult ReclaimLifetimeClass(size_t cls_index,
                                             size_t target_free,
                                             size_t max_victims,
                                             LifetimeScan& scan,
                                             std::vector<BlockKey>* evicted);
  struct LifetimeWatermarkStats {
    uint64_t budget_exhausted = 0;
    uint64_t no_candidate = 0;
    uint64_t pin_skips = 0;
    uint64_t protected_victims = 0;
  };
  // Internal disk watermark selector. Candidates never survive this A lock;
  // all scans and cached-candidate checks share one 4*max_victims budget.
  size_t EvictLifetimeToTarget(uint64_t target_bytes, size_t max_victims,
                               const DiskLifetimeConfig& config,
                               const DiskLifetimeClock& clock,
                               std::vector<BlockKey>* evicted,
                               LifetimeWatermarkStats* stats = nullptr);
  // A-lock-only physical accounting. Tokens belong to the current dense
  // resident; hiding visibility does not clear them. Config/time must match
  // the owning Store's immutable snapshot and nondecreasing clock.
  void RegisterLifetimeBucketLocked(uint32_t index,
                                    const DiskLifetimeConfig& config,
                                    uint32_t now);
  void UnregisterLifetimeBucketLocked(uint32_t index);
  void ResetLifetimeBucketsLocked(uint32_t extent);
  uint64_t LifetimeProtectedEstimateLocked(uint32_t extent, uint64_t now) const;
  void BumpLifetimeVersionLocked(uint32_t meta_index);
  // Physical ownership only, including pending removal. Does not touch CLOCK.
  // Zero means disabled, absent or uncommitted; never an exportable identity.
  uint64_t ResidentGeneration(const BlockKey& key) const;

  static constexpr int kUnbound = -1;
  static constexpr uint32_t kNoSlot = UINT32_MAX;
  static constexpr uint64_t kReadHeatUnit = 1024;
  static constexpr uint64_t kReadDecayOps = 64;

  enum class ExtentBindResult : unsigned char {
    kBound = 0,             // Binding is ready for allocation.
    kUnavailable = 1,       // No eligible extent is available.
    kPersistenceError = 2,  // The durable metadata change failed.
    kReclaimDenied = 3      // The runtime decision guard refused this work.
  };
  struct Slot { uint32_t extent; uint32_t slot; };

  // Exactly one live SlotMeta is authoritative for each resident. Both the
  // index and reverse metadata hold the canonical 24-byte BlockKey by value;
  // neither owns heap-backed key storage. All rings are integer intrusive links
  // into this dense table, whose free entries are reused.
  struct SlotMeta {
    BlockKey key;
    SlotRef ref;
    uint64_t useful_bytes = 0;
    uint32_t global_prev = kNoSlot;
    uint32_t global_next = kNoSlot;
    uint32_t free_next = kNoSlot;
    uint64_t read_touches = 0;
    uint32_t refs = 0;
    uint32_t class_prev = kNoSlot;
    uint32_t class_next = kNoSlot;
    uint32_t extent_prev = kNoSlot;
    uint32_t extent_next = kNoSlot;
    bool referenced = false;
    bool remove_pending = false;
    bool live = false;
  };

  struct LifetimeLink {
    uint32_t prev = kNoSlot;
    uint32_t next = kNoSlot;
  };
  struct LifetimeLinks {
    // Index 0 spans the disk; index 1 is restricted to the resident's class.
    std::array<LifetimeLink, 2> indexes{};
    uint32_t band = kNoSlot;  // Uncommitted/uninitialized residents are absent.
  };
  struct LifetimeBucket {
    // Absolute expiry-page + 1; zero means this ring position is unused.
    uint64_t generation = 0;
    uint64_t count = 0;  // Covers all supported extent/slot geometries.
  };
  struct LifetimeQueue {
    uint32_t head = kNoSlot;
    uint32_t tail = kNoSlot;
    uint32_t cursor = kNoSlot;
    uint64_t residents = 0;  // Maintained by the same A-locked link mutations.
  };

  struct Class {
    uint32_t slot_size = 0;
    uint32_t slots_per_extent = 0;
    // Free slots are bucketed per extent and selected round-robin, so
    // consecutive Puts land on different files. A single free stack would hand
    // out one extent back-to-back and serialize buffered writes on the
    // filesystem's per-inode lock (measured 2.1 vs 18.2 GB/s at 8 writers).
    std::unordered_map<uint32_t, std::vector<uint32_t>> free_by_ext;
    std::vector<uint32_t> ext_rr;
    size_t rr_next = 0;
    size_t free_count = 0;
    size_t resident_count = 0;
    uint32_t bound_extents = 0;
    uint64_t puts = 0;
    uint64_t useful_bytes = 0;
    uint64_t read_touches = 0;
    uint64_t read_heat = 0;
    uint64_t recovered_residents = 0;
    uint64_t recovered_unread_residents = 0;
    uint64_t heat_seq = 0;
    uint32_t ring_head = kNoSlot;
    uint32_t hand = kNoSlot;
  };

  struct ExtentMeta {
    int cls = kUnbound;
    uint32_t free_slots = 0;
    uint32_t total_slots = 0;
    uint32_t pinned = 0;
    // Newest insert sequence among residents (0 means empty). Freeing a slot
    // never lowers it: the conservative age prevents an extent from appearing
    // colder than a recently inserted survivor.
    uint64_t youngest_seq = 0;
    uint64_t useful_bytes = 0;
    uint64_t read_heat = 0;
    uint64_t heat_seq = 0;
    uint32_t resident_head = kNoSlot;
    uint32_t resident_count = 0;
  };

  uint32_t CanonicalSlotSize(size_t aligned_len) const;
  // Preserve Put's public interface. The store receives a per-call reason,
  // never a later observation of a shared failure flag. Denial is meaningful
  // only on false; optional growth refusal may still use an existing free slot.
  bool PutImpl(const BlockKey& key, size_t len, SlotRef* out,
               std::vector<BlockKey>* evicted, SlotHandle* out_handle,
               bool* reclaim_denied, LifetimeScan* scan = nullptr);
  bool ReclaimAllowed(bool* reclaim_denied = nullptr) const;
  size_t ClassForLen(size_t aligned_len);
  size_t ClassForExactSize(uint32_t slot_size);
  ExtentBindResult BindExtentLocked(size_t cls, uint32_t extent,
                                    bool metadata_prepared,
                                    bool* reclaim_denied = nullptr);
  ExtentBindResult BindFreeExtent(size_t cls, bool* reclaim_denied = nullptr);
  bool EvictOneFrom(size_t cls, std::vector<BlockKey>* evicted,
                    bool* reclaim_denied = nullptr);
  ExtentBindResult StealExtentFor(size_t cls, std::vector<BlockKey>* evicted,
                                  size_t min_donor_extents = 0,
                                  bool* reclaim_denied = nullptr);
  ExtentBindResult StealColdExtentFor(size_t cls,
                                      std::vector<BlockKey>* evicted,
                                      bool* reclaim_denied = nullptr);
  // lifetime_now is the final pre-wipe clock snapshot in lifetime mode;
  // legacy callers pass zero and do not score or record protection losses.
  ExtentBindResult StealExtentLocked(uint32_t extent, size_t target_cls,
                                     std::vector<BlockKey>* evicted,
                                     uint32_t lifetime_now,
                                     bool* reclaim_denied = nullptr);
  // New disk paths retain the distinction between no donor and failed
  // persistence. The public bool wrapper preserves legacy callers.
  ExtentBindResult StealFromResult(size_t donor_cls, size_t target_cls,
                                   std::vector<BlockKey>* evicted,
                                   size_t min_donor_extents = 0);
  ExtentBindResult StealLifetimeDonorLocked(size_t target_cls, size_t donor_cls,
                                            size_t min_donor_extents,
                                            uint64_t cold_before,
                                            std::vector<BlockKey>* evicted,
                                            bool* reclaim_denied = nullptr);

  uint32_t AcquireSlotMetaLocked();
  void LinkBeforeHandLocked(uint32_t meta_index);
  void LinkBeforeGlobalHandLocked(uint32_t meta_index);
  void LinkResidentLocked(uint32_t meta_index);
  void FreeSlotLocked(uint32_t meta_index);
  void LinkLifetimeLocked(uint32_t meta_index);
  void UnlinkLifetimeLocked(uint32_t meta_index);
  LifetimeQueue& LifetimeQueueFor(uint32_t meta_index, size_t index,
                                  uint32_t band);
  void TouchLocked(uint32_t meta_index);
  uint64_t DecayedHeat(uint64_t heat, uint64_t heat_seq) const;
  uint64_t ExtentHeatLocked(const ExtentMeta& extent) const;

  void PushFreeLocked(Class& cls, uint32_t extent, uint32_t slot);
  bool PopFreeLocked(Class& cls, Slot* out);
  bool TakeFreeSlotLocked(Class& cls, uint32_t extent, uint32_t slot);
  void DropExtentFreeLocked(Class& cls, uint32_t extent);
  void DropFromRotationLocked(Class& cls, uint32_t extent);

  // Power-of-two linear probing. Erase only installs a tombstone; compaction
  // and growth happen on Reserve/Insert so Find, existing-key Put, and Remove
  // never allocate.
  class FlatKeyIndex {
   public:
    struct Entry {
      BlockKey key;
      uint32_t value = kNoSlot;
      uint8_t state = 0;  // 0 empty, 1 occupied, 2 tombstone
    };

    uint32_t* Find(const BlockKey& key);
    const uint32_t* Find(const BlockKey& key) const;
    bool Insert(const BlockKey& key, uint32_t value);
    bool Erase(const BlockKey& key);
    void Reserve(size_t expected_size);
    size_t size() const { return size_; }
    IndexStats stats() const;
    uint64_t BackingBytes() const {
      return entries_.capacity() * sizeof(Entry);
    }
    size_t BucketForTest(const BlockKey& key) const;

   private:
    friend class SlabAllocatorTestPeer;
    static uint64_t Hash(const BlockKey& key);
    void Rehash(size_t capacity);

    std::vector<Entry> entries_;
    size_t size_ = 0;
    size_t tombstones_ = 0;
    uint64_t table_allocations_ = 0;
    uint64_t rehashes_ = 0;
  };

  Options opt_;
  mutable std::mutex mu_;
  std::vector<Class> classes_;
  std::vector<ExtentMeta> extents_;
  std::vector<SlotMeta> slots_;
  // Enabled-only parallel dense storage. Reused SlotMeta indexes start at zero;
  // physical FreeSlotLocked clears ownership, not payload visibility changes.
  // Capacity follows metadata high-water usage, not current resident count.
  std::vector<uint64_t> resident_generations_;
  // Enabled-only parallel storage; nullopt distinguishes an uncommitted or
  // deliberately uninitialized resident from a valid state whose anchor is 0.
  // Only physical removal clears a committed state, not payload visibility.
  std::vector<std::optional<DiskLifetimeState>> resident_lifetime_states_;
  // Optional score indexes, used only by the internal disk selectors.
  // Legacy rings remain authoritative without that Store gate. Disabled paths
  // allocate no links or queue heads; the RAM per-slot layout stays unchanged.
  std::vector<LifetimeLinks> lifetime_links_;
  // Row 0 is global; row cls+1 is class-local, each with MAX_READS+1 bands.
  std::vector<LifetimeQueue> lifetime_queues_;
  // Enabled-only physical ledger. No duplicate resident generation is needed:
  // owner validation and every token transition share A with slot reuse.
  std::vector<uint64_t> lifetime_bucket_tokens_;
  std::vector<LifetimeBucket> lifetime_buckets_;
  // One latest-generation aggregate per ring position across all extents.
  // Older delayed registrations must never replace a newer generation.
  std::vector<LifetimeBucket> lifetime_bucket_summary_;
  uint32_t lifetime_next_band_ =
      0;  // Global watermark turn, including quiet ticks.
  // Enabled-only class mutation versions, including pin eligibility changes.
  std::vector<uint64_t> lifetime_versions_;
  uint32_t free_slot_meta_head_ = kNoSlot;
  uint32_t global_ring_head_ = kNoSlot;
  uint32_t global_hand_ = kNoSlot;
  FlatKeyIndex index_;
  uint32_t unbound_ = 0;
  uint64_t used_bytes_ = 0;
  uint64_t evictions_ = 0;
  uint64_t steals_ = 0;
  uint64_t cold_steals_ = 0;
  // A-locked backing totals: pop/Take retain vector storage; extent removal
  // releases its free-slot vector but preserves the class rotation capacity.
  uint64_t class_rotation_bytes_ = 0;
  uint64_t class_free_slot_bytes_ = 0;
  LifetimeDonorStats lifetime_donor_stats_;
  uint64_t watermark_evictions_ = 0;
  uint64_t watermark_scan_steps_ = 0;
  uint64_t extent_returns_ = 0;
  uint64_t put_seq_ = 0;
  uint64_t activity_seq_ = 0;
  uint64_t cold_window_ = 0;
  bool cold_steal_enabled_ = true;
};

}  // namespace dfkv

#endif  // DFKV_SLAB_ALLOCATOR_H_
