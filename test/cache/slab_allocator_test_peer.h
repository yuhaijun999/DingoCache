#ifndef DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_
#define DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>

#include "cache/disk_lifetime_state.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"

namespace dfkv {

class SlabAllocatorTestPeer {
 public:
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
  static size_t SlotMetaBytes() { return sizeof(SlabAllocator::SlotMeta); }
};

}  // namespace dfkv

#endif  // DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_
