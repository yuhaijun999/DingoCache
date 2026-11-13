#ifndef DFKV_CACHE_DISK_EVICTION_GHOST_H_
#define DFKV_CACHE_DISK_EVICTION_GHOST_H_

#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <unordered_map>

#include "common/kv_types.h"

namespace dfkv {

// Bounded historical evidence, not a cache, heat ledger or miss counter.
// The caller serializes ALL access and supplies monotonic seconds. Record only
// successful disk evictions, never failed invalidations, Remove, PUT or
// recovery. Recording is explicit opt-in; association with actual subsequent
// demand is a separate integration step.
class DiskEvictionGhost {
 public:
  struct Snapshot {
    // Retained-entry limit, not a byte/RSS budget. A new insertion may stage
    // one extra list/index entry to keep allocation failures transactional.
    size_t capacity = 0;
    size_t retained = 0;  // Unique keys currently covered, not all evictions.
    bool capacity_limited = false;  // Sticky after discarding a retained key.
    // Maximum known timestamp of discarded/dropped evidence. Any future
    // complete interval must start strictly AFTER this inclusive loss boundary;
    // it does not mean every earlier key is missing. Zero and UINT64_MAX are
    // valid.
    std::optional<uint64_t> lost_through_sec;
    // Sticky until reconstruction after a loss without a usable timestamp.
    // Successful later records cannot establish complete coverage across it.
    bool loss_time_unknown = false;
    // Raw retained timestamps are not complete time coverage: equal-time
    // records may already have been discarded at capacity.
    std::optional<uint64_t> oldest_evicted_at_sec;
    std::optional<uint64_t> newest_evicted_at_sec;
    // Complete key-membership evidence for this settled prefix in
    // (window_after_sec, window_through_sec], not a complete event timeline.
    // Both absent means unavailable; equal values mean an available EMPTY
    // interval. The entire owner-start second is conservatively excluded.
    std::optional<uint64_t> window_after_sec;
    std::optional<uint64_t> window_through_sec;
  };

  // Unknown start permits recording/raw snapshots, never a complete window.
  explicit DiskEvictionGhost(
      size_t capacity, std::optional<uint64_t> started_at_sec = std::nullopt);
  // The index borrows iterators from this owner's list.
  DiskEvictionGhost(const DiskEvictionGhost&) = delete;
  DiskEvictionGhost& operator=(const DiskEvictionGhost&) = delete;
  DiskEvictionGhost(DiskEvictionGhost&&) = delete;
  DiskEvictionGhost& operator=(DiskEvictionGhost&&) = delete;
  ~DiskEvictionGhost() = default;

  // Capacity zero disables recording. False also means time regressed against
  // the latest accepted eviction; either refusal leaves all evidence intact.
  // Equal timestamps and zero are valid. A repeated key refreshes its timestamp
  // and moves newest without growing the retained set.
  bool RecordEviction(const BlockKey& key, uint64_t evicted_at_sec);

  // Record an evidence gap without changing retained keys. Known times advance
  // the loss boundary by max; missing time makes unknown loss sticky. Disabled
  // owners ignore both. Caller holds the same serialization as RecordEviction.
  void NoteDroppedEviction(std::optional<uint64_t> evicted_at_sec) noexcept;

  // Pure historical query: does not consume evidence, update recency or count
  // demand. Missing/future timestamps return nullopt, never wrapped ages.
  // A match alone proves neither a current miss nor its cause; the caller must
  // separately establish an actual request/result or recomputation association.
  std::optional<uint64_t> LookupEvictionAge(const BlockKey& key,
                                            uint64_t now_sec) const;
  // A usable end must cover the latest retained event and all known losses.
  // Unknown start/end/loss makes the window unavailable, not its raw history.
  // The end covers only events already settled at this serialized snapshot;
  // future events in the same whole second are NOT part of this prefix.
  Snapshot GetSnapshot(
      std::optional<uint64_t> through_sec = std::nullopt) const;

 private:
  struct Entry {
    BlockKey key;
    uint64_t evicted_at_sec;
  };
  struct KeyHash {
    size_t operator()(const BlockKey& key) const noexcept;
  };
  using Entries = std::list<Entry>;
  using Index = std::unordered_map<BlockKey, Entries::iterator, KeyHash>;

  const size_t capacity_;
  const std::optional<uint64_t> started_at_sec_;
  Entries entries_;  // Oldest accepted unique eviction first.
  Index index_;
  bool capacity_limited_ = false;
  std::optional<uint64_t> lost_through_sec_;
  bool loss_time_unknown_ = false;
};

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_EVICTION_GHOST_H_
