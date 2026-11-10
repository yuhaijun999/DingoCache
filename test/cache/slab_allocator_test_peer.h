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
  using ExtentBindResult = SlabAllocator::ExtentBindResult;
  static ExtentBindResult StealGeneric(SlabAllocator& allocator, size_t target,
                                       std::vector<BlockKey>* evicted,
                                       size_t floor = 0) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    bool denied = false;
    return allocator.StealExtentFor(target, evicted, floor, &denied);
  }
  static ExtentBindResult StealCold(SlabAllocator& allocator, size_t target,
                                    std::vector<BlockKey>* evicted) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    bool denied = false;
    return allocator.StealColdExtentFor(target, evicted, &denied);
  }
  static ExtentBindResult StealRestricted(SlabAllocator& allocator,
                                          size_t donor, size_t target,
                                          std::vector<BlockKey>* evicted,
                                          size_t floor = 0) {
    return allocator.StealFromResult(donor, target, evicted, floor);
  }
  static ExtentBindResult BindExtent(SlabAllocator& allocator, size_t cls,
                                     uint32_t extent) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.BindExtentLocked(cls, extent, false);
  }
  static void SetColdEligibility(SlabAllocator& allocator, uint64_t sequence,
                                 uint64_t window,
                                 const std::vector<uint64_t>& youngest) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    allocator.cold_steal_enabled_ = true;
    allocator.put_seq_ = sequence;
    allocator.cold_window_ = window;
    for (size_t extent = 0; extent < youngest.size(); ++extent)
      allocator.extents_[extent].youngest_seq = youngest[extent];
  }
  static size_t ClassIndex(const SlabAllocator& allocator, uint32_t slot_size) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    for (size_t cls = 0; cls < allocator.classes_.size(); ++cls)
      if (allocator.classes_[cls].slot_size == slot_size) return cls;
    return allocator.classes_.size();
  }
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
      const DiskLifetimeState* lifetime_state = nullptr,
      const DiskLifetimeConfig* config = nullptr, uint32_t now = 0) {
    allocator->CommitResidentGeneration(key, generation, lifetime_state, config,
                                        now);
  }
  static uint64_t ProtectedEstimate(const SlabAllocator& allocator,
                                    uint32_t extent, uint64_t now) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.LifetimeProtectedEstimateLocked(extent, now);
  }
  static uint64_t BucketToken(const SlabAllocator& allocator,
                              const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    const uint32_t* index = allocator.index_.Find(key);
    return index != nullptr && *index < allocator.lifetime_bucket_tokens_.size()
               ? allocator.lifetime_bucket_tokens_[*index]
               : 0;
  }
  static size_t BucketPageCount(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.lifetime_buckets_.size();
  }
  static size_t BucketSummaryCapacity(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.lifetime_bucket_summary_.capacity();
  }
  static size_t BucketTokenSize(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.lifetime_bucket_tokens_.size();
  }
  static size_t BucketTokenCapacity(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return allocator.lifetime_bucket_tokens_.capacity();
  }
  static bool MetadataMemoryMatches(
      const SlabAllocator& allocator,
      const SlabAllocator::MetadataMemory& memory) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return memory.slot_bytes ==
               allocator.slots_.capacity() * sizeof(SlabAllocator::SlotMeta) &&
           memory.extent_bytes == allocator.extents_.capacity() *
                                      sizeof(SlabAllocator::ExtentMeta) &&
           memory.class_bytes ==
               allocator.classes_.capacity() * sizeof(SlabAllocator::Class) &&
           memory.key_index_bytes ==
               allocator.index_.entries_.capacity() *
                   sizeof(SlabAllocator::FlatKeyIndex::Entry);
  }
  static bool LifetimeMemoryMatches(
      const SlabAllocator& allocator,
      const SlabAllocator::LifetimeMemory& memory) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    return memory.identity_bytes ==
               allocator.resident_generations_.capacity() * sizeof(uint64_t) &&
           memory.state_bytes ==
               allocator.resident_lifetime_states_.capacity() *
                   sizeof(std::optional<DiskLifetimeState>) &&
           memory.link_bytes == allocator.lifetime_links_.capacity() *
                                    sizeof(SlabAllocator::LifetimeLinks) &&
           memory.queue_bytes == allocator.lifetime_queues_.capacity() *
                                     sizeof(SlabAllocator::LifetimeQueue) &&
           memory.version_bytes ==
               allocator.lifetime_versions_.capacity() * sizeof(uint64_t);
  }
  static size_t LifetimeQueueBytes() {
    return sizeof(SlabAllocator::LifetimeQueue);
  }
  static bool BucketRowEmpty(const SlabAllocator& allocator, uint32_t extent) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    const size_t bands = allocator.opt_.lifetime_bucket_count;
    for (size_t band = 0; band < bands; ++band) {
      const SlabAllocator::LifetimeBucket& page =
          allocator.lifetime_buckets_[extent * bands + band];
      if (page.generation != 0 || page.count != 0) return false;
    }
    return true;
  }
  static size_t BucketPageBytes() {
    return sizeof(SlabAllocator::LifetimeBucket);
  }
  static bool LifetimeBucketsValid(const SlabAllocator& allocator) {
    const std::lock_guard<std::mutex> lock(allocator.mu_);
    const size_t bands = allocator.opt_.lifetime_bucket_count;
    if (bands == 0)
      return allocator.lifetime_buckets_.empty() &&
             allocator.lifetime_bucket_tokens_.empty() &&
             allocator.lifetime_bucket_summary_.empty();
    if (allocator.lifetime_bucket_tokens_.size() != allocator.slots_.size() ||
        allocator.lifetime_buckets_.size() !=
            allocator.extents_.size() * bands ||
        allocator.lifetime_bucket_summary_.size() != bands)
      return false;
    std::vector<uint64_t> counts(allocator.lifetime_buckets_.size(), 0);
    for (size_t index = 0; index < allocator.slots_.size(); ++index) {
      const uint64_t token = allocator.lifetime_bucket_tokens_[index];
      if (token == 0) continue;
      if (!allocator.slots_[index].live ||
          allocator.resident_generations_[index] == 0 ||
          !allocator.resident_lifetime_states_[index].has_value())
        return false;
      const size_t page = allocator.slots_[index].ref.extent * bands +
                          static_cast<size_t>((token - 1) % bands);
      if (allocator.lifetime_buckets_[page].generation == token) ++counts[page];
    }
    std::vector<uint64_t> summary_counts(bands, 0);
    for (size_t page = 0; page < counts.size(); ++page) {
      const SlabAllocator::LifetimeBucket& bucket =
          allocator.lifetime_buckets_[page];
      if (bucket.count != counts[page]) return false;
      const SlabAllocator::LifetimeBucket& summary =
          allocator.lifetime_bucket_summary_[page % bands];
      // Derive the aggregate from resident tokens, not its maintained ledger.
      // Older ring generations may remain in other extent rows after expiry.
      if (counts[page] != 0 && bucket.generation > summary.generation)
        return false;
      if (bucket.generation == summary.generation)
        summary_counts[page % bands] += counts[page];
    }
    for (size_t page = 0; page < bands; ++page) {
      if (allocator.lifetime_bucket_summary_[page].count !=
          summary_counts[page])
        return false;
    }
    return true;
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
      for (const SlabAllocator::Class& cls : allocator.classes_) {
        if (cls.recovered_residents != 0 ||
            cls.recovered_unread_residents != 0) {
          return false;
        }
      }
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
        uint64_t residents = 0;
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
          ++residents;
          seen[current][scope] = true;
          cursor_seen = cursor_seen || current == queue.cursor;
          anchor = state->anchor;
          prev = current;
          current = links.indexes[scope].next;
        }
        if (residents != queue.residents || prev != queue.tail ||
            (queue.head == SlabAllocator::kNoSlot
                 ? queue.cursor != SlabAllocator::kNoSlot
                 : !cursor_seen)) {
          return false;
        }
      }
    }
    // Independent population oracle: [0] is recovered, [1] is still unread.
    std::vector<std::array<uint64_t, 2>> recovered(allocator.classes_.size());
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
      if (eligible) {
        const uint32_t cls = allocator.slots_[index].ref.cls;
        if (cls >= recovered.size()) return false;
        const DiskLifetimeState& state =
            *allocator.resident_lifetime_states_[index];
        if (state.history_unknown) {
          ++recovered[cls][0];
          if (state.actual_reads == 0) ++recovered[cls][1];
        }
      }
    }
    for (size_t cls = 0; cls < allocator.classes_.size(); ++cls) {
      if (allocator.classes_[cls].recovered_residents != recovered[cls][0] ||
          allocator.classes_[cls].recovered_unread_residents !=
              recovered[cls][1]) {
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
