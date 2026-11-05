#ifndef DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_
#define DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "cache/slab_allocator.h"
#include "common/kv_types.h"

namespace dfkv {

class SlabAllocatorTestPeer {
 public:
  static void CommitGeneration(SlabAllocator* allocator, const BlockKey& key,
                               uint64_t generation) {
    allocator->CommitResidentGeneration(key, generation);
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
  static size_t SlotMetaBytes() { return sizeof(SlabAllocator::SlotMeta); }
};

}  // namespace dfkv

#endif  // DFKV_TEST_CACHE_SLAB_ALLOCATOR_TEST_PEER_H_
