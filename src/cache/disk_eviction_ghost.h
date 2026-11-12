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
    std::optional<uint64_t> oldest_evicted_at_sec;
    std::optional<uint64_t> newest_evicted_at_sec;
    // These timestamps describe retained evidence, not complete time coverage:
    // equal-time evictions may already have been discarded at the capacity cap.
  };

  explicit DiskEvictionGhost(size_t capacity);
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

  // Pure historical query: does not consume evidence, update recency or count
  // demand. Missing/future timestamps return nullopt, never wrapped ages.
  // A match alone proves neither a current miss nor its cause; the caller must
  // separately establish an actual request/result or recomputation association.
  std::optional<uint64_t> LookupEvictionAge(const BlockKey& key,
                                            uint64_t now_sec) const;
  Snapshot GetSnapshot() const;

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
  Entries entries_;  // Oldest accepted unique eviction first.
  Index index_;
  bool capacity_limited_ = false;
};

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_EVICTION_GHOST_H_
