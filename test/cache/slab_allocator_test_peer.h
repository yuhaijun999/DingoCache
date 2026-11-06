#ifndef DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_
#define DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"

namespace dfkv {

class SlabAllocatorTestPeer {
 public:
  static size_t EvictLifetimeToTarget(SlabAllocator& allocator, uint64_t target,
                                      size_t max_victims,
                                      const DiskLifetimeConfig& config,
                                      const DiskLifetimeClock& clock,
                                      std::vector<BlockKey>* evicted) {
    return allocator.EvictLifetimeToTarget(target, max_victims, config, clock,
                                           evicted);
  }
  using LifetimeScan = SlabAllocator::LifetimeScan;
  using LifetimeScanResult = SlabAllocator::LifetimeScanResult;
  using LifetimeReclaimResult = SlabAllocator::LifetimeReclaimResult;
  static LifetimeReclaimResult ReclaimLifetimeClass(
      SlabAllocator& allocator, size_t cls, size_t target, size_t max_victims,
      LifetimeScan& scan, std::vector<BlockKey>* evicted) {
    return allocator.ReclaimLifetimeClass(cls, target, max_victims, scan,
                                          evicted);
  }

  static bool PutWithLifetimeScan(SlabAllocator& allocator, const BlockKey& key,
                                  size_t len, SlabAllocator::SlotRef* ref,
                                  std::vector<BlockKey>* evicted,
                                  LifetimeScan& scan, bool* denied) {
    return allocator.PutImpl(key, len, ref, evicted, nullptr, denied, &scan);
  }
  static bool ScanLifetimeClass(SlabAllocator& allocator, size_t cls,
                                LifetimeScan& scan,
                                std::vector<BlockKey>* evicted, bool* denied) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.EvictLifetimeFromLocked(cls, scan, evicted, denied);
  }
  static void SetLifetimeVersion(SlabAllocator& allocator, size_t cls,
                                 uint64_t version) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    allocator.lifetime_versions_[cls] = version;
  }
  static uint64_t LifetimeVersion(const SlabAllocator& allocator, size_t cls) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.lifetime_versions_[cls];
  }
  using LifetimeReadResult = SlabAllocator::LifetimeReadResult;
  static LifetimeReadResult ApplyLifetimeRead(SlabAllocator& allocator,
                                              const BlockKey& key,
                                              uint64_t generation,
                                              const DiskLifetimeConfig& config,
                                              const DiskLifetimeClock& clock) {
    return allocator.ApplyLifetimeRead(key, generation, config, clock);
  }
  using LifetimeSnapshot =
      std::pair<uint64_t, std::optional<DiskLifetimeState>>;

  static LifetimeSnapshot LifetimeSnapshotFor(const SlabAllocator& allocator,
                                              const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    const uint32_t* index = allocator.index_.Find(key);
    if (index == nullptr) return {0, std::nullopt};
    const uint64_t generation = allocator.opt_.track_resident_identity
                                    ? allocator.resident_generations_[*index]
                                    : 0;
    const std::optional<DiskLifetimeState> state =
        allocator.opt_.track_lifetime_state
            ? allocator.resident_lifetime_states_[*index]
            : std::nullopt;
    return {generation, state};
  }
  static void CommitGeneration(
      SlabAllocator* allocator, const BlockKey& key, uint64_t generation,
      const DiskLifetimeState* lifetime_state = nullptr) {
    allocator->CommitResidentGeneration(key, generation, lifetime_state);
  }
  static uint64_t Generation(const SlabAllocator& allocator,
                             const BlockKey& key) {
    return allocator.ResidentGeneration(key);
  }
  static size_t GenerationCount(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return static_cast<size_t>(
        std::count_if(allocator.resident_generations_.begin(),
                      allocator.resident_generations_.end(),
                      [](uint64_t generation) { return generation != 0; }));
  }
  static size_t GenerationSize(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.resident_generations_.size();
  }
  static size_t GenerationCapacity(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.resident_generations_.capacity();
  }
  static std::optional<DiskLifetimeState> LifetimeState(
      const SlabAllocator& allocator, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    if (!allocator.opt_.track_lifetime_state) return std::nullopt;
    const uint32_t* index = allocator.index_.Find(key);
    return index != nullptr ? allocator.resident_lifetime_states_[*index]
                            : std::nullopt;
  }
  static size_t LifetimeStateSize(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.resident_lifetime_states_.size();
  }
  static size_t LifetimeStateCapacity(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.resident_lifetime_states_.capacity();
  }
  // Inspect both indexes under the same lock as state publication. Traversal is
  // bounded by dense metadata size so a broken cycle fails rather than hangs.
  static bool LifetimeIndexesValid(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    if (!allocator.opt_.track_lifetime_state) {
      return allocator.lifetime_links_.capacity() == 0 &&
             allocator.lifetime_queues_.capacity() == 0;
    }
    const size_t size = allocator.slots_.size();
    const size_t bands = allocator.opt_.lifetime_max_reads + 1;
    if (allocator.lifetime_links_.size() != size ||
        allocator.resident_generations_.size() != size ||
        allocator.resident_lifetime_states_.size() != size ||
        allocator.lifetime_queues_.size() !=
            (allocator.classes_.size() + 1) * bands) {
      return false;
    }
    std::vector<std::array<bool, 2>> seen(size);
    for (size_t row = 0; row <= allocator.classes_.size(); ++row) {
      const size_t scope = row == 0 ? 0 : 1;
      for (size_t band = 0; band < bands; ++band) {
        const SlabAllocator::LifetimeQueue& queue =
            allocator.lifetime_queues_[row * bands + band];
        uint32_t prev = SlabAllocator::kNoSlot;
        uint32_t current = queue.head;
        uint32_t anchor = 0;
        bool cursor_seen = false;
        while (current != SlabAllocator::kNoSlot) {
          if (current >= size || seen[current][scope]) return false;
          const SlabAllocator::SlotMeta& meta = allocator.slots_[current];
          const SlabAllocator::LifetimeLinks& links =
              allocator.lifetime_links_[current];
          const std::optional<DiskLifetimeState>& state =
              allocator.resident_lifetime_states_[current];
          if (!meta.live || allocator.resident_generations_[current] == 0 ||
              !state.has_value() || links.band != band ||
              (row != 0 && meta.ref.cls + 1 != row) ||
              links.indexes[scope].prev != prev || state->anchor < anchor) {
            return false;
          }
          const uint32_t expected_band =
              std::min<uint32_t>(static_cast<uint32_t>(state->scoring_reads()) +
                                     state->first_early_read(),
                                 allocator.opt_.lifetime_max_reads);
          if (expected_band != band) return false;
          seen[current][scope] = true;
          cursor_seen = cursor_seen || current == queue.cursor;
          anchor = state->anchor;
          prev = current;
          current = links.indexes[scope].next;
        }
        if (prev != queue.tail || (queue.head == SlabAllocator::kNoSlot
                                       ? queue.cursor != SlabAllocator::kNoSlot
                                       : !cursor_seen)) {
          return false;
        }
      }
    }
    for (size_t index = 0; index < size; ++index) {
      // Hidden pending Remove still owns its physical slot until final unpin.
      const bool eligible =
          allocator.slots_[index].live &&
          allocator.resident_generations_[index] != 0 &&
          allocator.resident_lifetime_states_[index].has_value();
      if (seen[index][0] != eligible || seen[index][1] != eligible ||
          (allocator.lifetime_links_[index].band != SlabAllocator::kNoSlot) !=
              eligible) {
        return false;
      }
    }
    return true;
  }
  static size_t LifetimeQueueCount(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.lifetime_queues_.size();
  }
  static std::vector<BlockKey> LifetimeQueueKeys(const SlabAllocator& allocator,
                                                 size_t row, uint32_t band) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    const size_t bands = allocator.opt_.lifetime_max_reads + 1;
    if (!allocator.opt_.track_lifetime_state || band >= bands ||
        row > allocator.classes_.size()) {
      return {};
    }
    const size_t scope = row == 0 ? 0 : 1;
    uint32_t index = allocator.lifetime_queues_[row * bands + band].head;
    std::vector<BlockKey> keys;
    while (index < allocator.slots_.size() &&
           keys.size() < allocator.slots_.size()) {
      keys.push_back(allocator.slots_[index].key);
      index = allocator.lifetime_links_[index].indexes[scope].next;
    }
    return keys;
  }
  static bool PointLifetimeCursorsAt(SlabAllocator& allocator,
                                     const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    const uint32_t* index = allocator.index_.Find(key);
    if (index == nullptr || !allocator.opt_.track_lifetime_state) return false;
    const uint32_t band = allocator.lifetime_links_[*index].band;
    if (band == SlabAllocator::kNoSlot) return false;
    for (size_t scope = 0; scope < 2; ++scope) {
      allocator.LifetimeQueueFor(*index, scope, band).cursor = *index;
    }
    return true;
  }
  static std::optional<BlockKey> LifetimeCursorKey(
      const SlabAllocator& allocator, size_t row, uint32_t band) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    const size_t bands = allocator.opt_.lifetime_max_reads + 1;
    if (!allocator.opt_.track_lifetime_state || band >= bands ||
        row > allocator.classes_.size()) {
      return std::nullopt;
    }
    const uint32_t cursor =
        allocator.lifetime_queues_[row * bands + band].cursor;
    return cursor < allocator.slots_.size()
               ? std::optional<BlockKey>(allocator.slots_[cursor].key)
               : std::nullopt;
  }
  static size_t SlotMetaBytes() { return sizeof(SlabAllocator::SlotMeta); }
};

}  // namespace dfkv

#endif  // DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_
