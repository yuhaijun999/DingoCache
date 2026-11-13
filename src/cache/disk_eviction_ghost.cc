#include "cache/disk_eviction_ghost.h"

#include <cstddef>
#include <cstdint>
#include <optional>

#include "common/kv_types.h"

namespace dfkv {

DiskEvictionGhost::DiskEvictionGhost(size_t capacity) : capacity_(capacity) {}

size_t DiskEvictionGhost::KeyHash::operator()(
    const BlockKey& key) const noexcept {
  // Match the existing RAM tier's private, non-cryptographic full-key mixer.
  const auto mix = [](uint64_t value) {
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
  };
  uint64_t hash = mix(key.digest_hi + 0x9e3779b97f4a7c15ULL);
  hash ^= mix(key.digest_lo + 0x3c6ef372fe94f82aULL);
  hash ^= mix(key.tenant_hash + 0xdaa66d2c7ddef743ULL);
  return static_cast<size_t>(mix(hash));
}

bool DiskEvictionGhost::RecordEviction(const BlockKey& key,
                                       uint64_t evicted_at_sec) {
  if (capacity_ == 0 ||
      (!entries_.empty() && evicted_at_sec < entries_.back().evicted_at_sec)) {
    return false;
  }
  const Index::iterator existing = index_.find(key);
  if (existing != index_.end()) {
    existing->second->evicted_at_sec = evicted_at_sec;
    entries_.splice(entries_.end(), entries_, existing->second);
    return true;
  }

  // Stage both allocations before discarding evidence. If either throws, the
  // temporary list cleans up and retained state stays intact. Splice preserves
  // the iterator placed in the index and does not allocate.
  Entries staged;
  staged.push_back({key, evicted_at_sec});
  index_.emplace(key, staged.begin());
  if (entries_.size() == capacity_) {
    // Only committed removal loses key membership; failed staging above does
    // not move this boundary. Use the removed time, not the incoming time.
    NoteDroppedEviction(entries_.front().evicted_at_sec);
    index_.erase(entries_.front().key);
    entries_.pop_front();
    capacity_limited_ = true;
  }
  entries_.splice(entries_.end(), staged);
  return true;
}

void DiskEvictionGhost::NoteDroppedEviction(
    std::optional<uint64_t> evicted_at_sec) noexcept {
  if (capacity_ == 0) return;
  if (!evicted_at_sec.has_value()) {
    loss_time_unknown_ = true;
  } else if (!lost_through_sec_.has_value() ||
             *evicted_at_sec > *lost_through_sec_) {
    lost_through_sec_ = evicted_at_sec;
  }
}

std::optional<uint64_t> DiskEvictionGhost::LookupEvictionAge(
    const BlockKey& key, uint64_t now_sec) const {
  const Index::const_iterator found = index_.find(key);
  if (found == index_.end() || now_sec < found->second->evicted_at_sec) {
    return std::nullopt;
  }
  return now_sec - found->second->evicted_at_sec;
}

DiskEvictionGhost::Snapshot DiskEvictionGhost::GetSnapshot() const {
  Snapshot snapshot;
  snapshot.capacity = capacity_;
  snapshot.retained = entries_.size();
  snapshot.capacity_limited = capacity_limited_;
  snapshot.lost_through_sec = lost_through_sec_;
  snapshot.loss_time_unknown = loss_time_unknown_;
  if (!entries_.empty()) {
    snapshot.oldest_evicted_at_sec = entries_.front().evicted_at_sec;
    snapshot.newest_evicted_at_sec = entries_.back().evicted_at_sec;
  }
  return snapshot;
}

}  // namespace dfkv
