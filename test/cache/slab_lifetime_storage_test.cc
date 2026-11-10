#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
namespace {

SlabAllocator::Options LifetimeOptions() {
  SlabAllocator::Options options;
  options.extent_bytes = 4 * 4096;
  options.num_extents = 1;
  options.track_lifetime_state = true;
  return options;
}

void ExpectState(const SlabAllocator& allocator, const BlockKey& key,
                 const DiskLifetimeState& expected) {
  const std::optional<DiskLifetimeState> actual =
      SlabAllocatorTestPeer::LifetimeState(allocator, key);
  if (!actual.has_value()) {
    ADD_FAILURE() << "the committed lifetime state is missing";
    return;
  }
  EXPECT_EQ(actual->anchor, expected.anchor);
  EXPECT_EQ(actual->put_at, expected.put_at);
  EXPECT_EQ(actual->hstate, expected.hstate);
  EXPECT_EQ(actual->actual_reads, expected.actual_reads);
  EXPECT_EQ(actual->history_unknown, expected.history_unknown);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

std::optional<uint64_t> QueuePopulation(
    const std::vector<SlabAllocator::LifetimeQueueStat>& stats,
    uint32_t slot_size, uint32_t band) {
  for (const SlabAllocator::LifetimeQueueStat& stat : stats) {
    if (stat.slot_size == slot_size && stat.band == band) return stat.residents;
  }
  return std::nullopt;
}

bool QueueSnapshotConservesResidents(
    const std::vector<SlabAllocator::LifetimeQueueStat>& stats,
    uint32_t max_reads) {
  std::array<uint64_t, 64> global{};
  std::array<uint64_t, 64> classes{};
  std::array<bool, 64> seen{};
  size_t global_bands = 0;
  for (const SlabAllocator::LifetimeQueueStat& stat : stats) {
    if (stat.band > max_reads) return false;
    if (stat.slot_size == 0) {
      if (seen[stat.band]) return false;
      seen[stat.band] = true;
      ++global_bands;
      global[stat.band] = stat.residents;
    } else {
      classes[stat.band] += stat.residents;
    }
  }
  return global_bands == max_reads + 1 && global == classes;
}

void ExpectMetadataMemory(const SlabAllocator::MetadataMemory& actual,
                          const SlabAllocator::MetadataMemory& expected) {
  EXPECT_EQ(actual.slot_bytes, expected.slot_bytes);
  EXPECT_EQ(actual.extent_bytes, expected.extent_bytes);
  EXPECT_EQ(actual.class_bytes, expected.class_bytes);
  EXPECT_EQ(actual.key_index_bytes, expected.key_index_bytes);
  EXPECT_EQ(actual.class_rotation_bytes, expected.class_rotation_bytes);
  EXPECT_EQ(actual.class_free_slot_bytes, expected.class_free_slot_bytes);
}

TEST(SlabLifetimeStorage, MetadataBackingTracksCapacityInEveryMode) {
  for (const bool track_state : {false, true}) {
    for (const uint32_t max_reads : {1U, 2U, 4U, 63U}) {
      SCOPED_TRACE(track_state);
      SCOPED_TRACE(max_reads);
      SlabAllocator::Options options = LifetimeOptions();
      options.track_lifetime_state = track_state;
      options.track_resident_identity = false;
      options.lifetime_max_reads = max_reads;
      SlabAllocator allocator(options);
      const SlabAllocator::MetadataMemory empty =
          allocator.MetadataMemoryUsage();
      EXPECT_TRUE(
          SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, empty));
      EXPECT_EQ(empty.slot_bytes, 0U);
      EXPECT_EQ(empty.key_index_bytes, 0U);
      EXPECT_EQ(empty.class_rotation_bytes, 0U);
      EXPECT_EQ(empty.class_free_slot_bytes, 0U);
      EXPECT_GT(empty.extent_bytes, 0U);
      EXPECT_GT(empty.class_bytes, 0U);
      SlabAllocator::SlotRef ref;
      std::vector<BlockKey> evicted;
      for (uint64_t i = 1; i <= 3; ++i)
        ASSERT_TRUE(allocator.Put({i, 0, 0}, 4096, &ref, &evicted));
      ASSERT_TRUE(evicted.empty());
      const SlabAllocator::MetadataMemory occupied =
          allocator.MetadataMemoryUsage();
      EXPECT_TRUE(
          SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, occupied));
      EXPECT_GT(occupied.slot_bytes, empty.slot_bytes);
      EXPECT_GT(occupied.key_index_bytes, empty.key_index_bytes);
      EXPECT_EQ(occupied.extent_bytes, empty.extent_bytes);
      EXPECT_EQ(occupied.class_bytes, empty.class_bytes);
      EXPECT_GE(occupied.class_rotation_bytes, sizeof(uint32_t));
      EXPECT_GE(occupied.class_free_slot_bytes, 4 * sizeof(uint32_t));
      ASSERT_TRUE(allocator.Pin({1, 0, 0}));
      ASSERT_EQ(allocator.Remove({1, 0, 0}),
                SlabAllocator::RemoveResult::kDeferred);
      EXPECT_FALSE(allocator.Contains({1, 0, 0}));
      ExpectMetadataMemory(allocator.MetadataMemoryUsage(), occupied);
      ASSERT_TRUE(allocator.Unpin({1, 0, 0}));
      for (uint64_t i = 2; i <= 3; ++i)
        ASSERT_EQ(allocator.Remove({i, 0, 0}),
                  SlabAllocator::RemoveResult::kRemoved);
      ASSERT_EQ(allocator.Count(), 0U);
      ExpectMetadataMemory(allocator.MetadataMemoryUsage(), occupied);
      ASSERT_TRUE(allocator.Put({4, 0, 0}, 4096, &ref, &evicted));
      ExpectMetadataMemory(allocator.MetadataMemoryUsage(), occupied);
      EXPECT_TRUE(SlabAllocatorTestPeer::MetadataMemoryMatches(
          allocator, allocator.MetadataMemoryUsage()));
    }
  }
}

TEST(SlabLifetimeStorage, MetadataBackingTracksRestoreAndRejectedOperations) {
  SlabAllocator::Options options = LifetimeOptions();
  options.extent_bytes = 64 * 4096;
  bool allow = false;
  options.on_extent_evict = [&](uint32_t, uint32_t, uint32_t) { return allow; };
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) { return allow; };
  SlabAllocator allocator(options);
  const SlabAllocator::MetadataMemory empty = allocator.MetadataMemoryUsage();
  ASSERT_FALSE(allocator.RestoreBulk({{{1, 0, 0}, 4096, 0, 64, 1}}));
  ExpectMetadataMemory(allocator.MetadataMemoryUsage(), empty);
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t i = 0; i < 64; ++i)
    entries.push_back({{i + 1, 0, 0}, 4096, 0, i, 1});
  ASSERT_TRUE(allocator.RestoreBulk(entries));
  ASSERT_EQ(allocator.Count(), entries.size());
  const SlabAllocator::MetadataMemory restored =
      allocator.MetadataMemoryUsage();
  EXPECT_TRUE(
      SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, restored));
  EXPECT_GT(restored.slot_bytes, empty.slot_bytes);
  EXPECT_GT(restored.key_index_bytes, empty.key_index_bytes);
  // Restore takes all free slots, but pop/Take do not release their backing.
  EXPECT_GE(restored.class_rotation_bytes, sizeof(uint32_t));
  EXPECT_GE(restored.class_free_slot_bytes, 64 * sizeof(uint32_t));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(allocator.EvictColdToTarget(0, entries.size(), &evicted), 0U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), entries.size());
  ExpectMetadataMemory(allocator.MetadataMemoryUsage(), restored);
  allow = true;
  ASSERT_EQ(allocator.EvictColdToTarget(0, entries.size(), &evicted),
            entries.size());
  EXPECT_EQ(allocator.Count(), 0U);
  ExpectMetadataMemory(allocator.MetadataMemoryUsage(), restored);
  EXPECT_TRUE(
      SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, restored));
}

TEST(SlabLifetimeStorage, ConcurrentMetadataSnapshotsObserveActualGrowth) {
  SlabAllocator::Options options = LifetimeOptions();
  options.extent_bytes = 128 * 4096;
  SlabAllocator allocator(options);
  const SlabAllocator::MetadataMemory empty = allocator.MetadataMemoryUsage();
  std::atomic<bool> done{false};
  std::atomic<bool> valid{true};
  std::promise<void> growth_seen;
  const std::future<void> growth = growth_seen.get_future();
  std::thread observer([&] {
    SlabAllocator::MetadataMemory previous = empty;
    bool published = false;
    while (!done.load()) {
      const SlabAllocator::MetadataMemory current =
          allocator.MetadataMemoryUsage();
      if (current.slot_bytes < previous.slot_bytes ||
          current.key_index_bytes < previous.key_index_bytes ||
          current.class_rotation_bytes < previous.class_rotation_bytes ||
          current.class_free_slot_bytes < previous.class_free_slot_bytes ||
          current.extent_bytes != empty.extent_bytes ||
          current.class_bytes != empty.class_bytes)
        valid.store(false);
      if (!published && current.slot_bytes > empty.slot_bytes &&
          current.key_index_bytes > empty.key_index_bytes &&
          current.class_rotation_bytes > 0 &&
          current.class_free_slot_bytes > 0) {
        growth_seen.set_value();
        published = true;
      }
      previous = current;
      std::this_thread::yield();
    }
  });
  bool observed_growth = false;
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  for (uint64_t i = 1; i <= 128; ++i) {
    if (!allocator.Put({i, 0, 0}, 4096, &ref, &evicted)) {
      valid.store(false);
      break;
    }
    if (i == 32) {
      // Pause the writer until a nonempty snapshot is actually observed. The
      // timeout bounds failure and the observer is joined before assertions.
      observed_growth =
          growth.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
      if (!observed_growth) valid.store(false);
    }
  }
  for (uint64_t i = 1; i <= 128; ++i) {
    if (allocator.Remove({i, 0, 0}) != SlabAllocator::RemoveResult::kRemoved)
      valid.store(false);
  }
  done.store(true);
  observer.join();
  EXPECT_TRUE(observed_growth);
  EXPECT_TRUE(valid.load());
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), 0U);
  const SlabAllocator::MetadataMemory retained =
      allocator.MetadataMemoryUsage();
  EXPECT_GT(retained.slot_bytes, empty.slot_bytes);
  EXPECT_GT(retained.key_index_bytes, empty.key_index_bytes);
  EXPECT_GE(retained.class_rotation_bytes, sizeof(uint32_t));
  EXPECT_GE(retained.class_free_slot_bytes, 128 * sizeof(uint32_t));
  EXPECT_TRUE(
      SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, retained));
}

TEST(SlabLifetimeStorage, ClassVectorBackingDropsOnSuccessfulRebindOnly) {
  for (const bool track_state : {false, true}) {
    SCOPED_TRACE(track_state);
    bool allow = false;
    SlabAllocator::Options options = LifetimeOptions();
    options.track_lifetime_state = track_state;
    options.on_extent_bind = [&](uint32_t) { return allow; };
    SlabAllocator allocator(options);
    const BlockKey key{1, 0, 0};
    ASSERT_TRUE(allocator.RestoreBulk({{key, 4096, 0, 0, 1}}));
    const SlabAllocator::MetadataMemory small = allocator.MetadataMemoryUsage();
    ASSERT_TRUE(SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, small));
    ASSERT_GE(small.class_free_slot_bytes, 4 * sizeof(uint32_t));
    const std::vector<SlabAllocator::ClassStat> classes = allocator.Classes();
    size_t target = classes.size();
    for (size_t i = 0; i < classes.size(); ++i) {
      if (classes[i].slot_size == options.extent_bytes) target = i;
    }
    ASSERT_LT(target, classes.size());
    std::vector<BlockKey> evicted;
    ASSERT_TRUE(allocator.Pin(key));
    allow = true;
    EXPECT_FALSE(allocator.StealFrom(0, target, &evicted));
    ExpectMetadataMemory(allocator.MetadataMemoryUsage(), small);
    ASSERT_TRUE(allocator.Unpin(key));
    allow = false;
    EXPECT_FALSE(allocator.StealFrom(0, target, &evicted));
    EXPECT_TRUE(evicted.empty());
    EXPECT_TRUE(allocator.Contains(key));
    ExpectMetadataMemory(allocator.MetadataMemoryUsage(), small);
    allow = true;
    ASSERT_TRUE(allocator.StealFrom(0, target, &evicted));
    ASSERT_EQ(evicted.size(), 1U);
    EXPECT_EQ(evicted[0], key);
    const SlabAllocator::MetadataMemory rebound =
        allocator.MetadataMemoryUsage();
    EXPECT_TRUE(
        SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, rebound));
    // The donor's four-slot vector is destroyed, not merely popped empty.
    // Its rotation retains storage; the target gets its own one-slot vectors.
    EXPECT_GT(rebound.class_free_slot_bytes, 0U);
    EXPECT_LT(rebound.class_free_slot_bytes, small.class_free_slot_bytes);
    EXPECT_GT(rebound.class_rotation_bytes, small.class_rotation_bytes);
    SlabAllocator::SlotRef ref;
    ASSERT_TRUE(allocator.Put({2, 0, 0}, options.extent_bytes, &ref, &evicted));
    ExpectMetadataMemory(allocator.MetadataMemoryUsage(), rebound);
  }
}

TEST(SlabLifetimeStorage, ClassVectorBackingReleasesReturnedExtentCapacity) {
  SlabAllocator::Options options = LifetimeOptions();
  options.num_extents = 9;
  SlabAllocator allocator(options);
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t extent = 0; extent < options.num_extents; ++extent)
    entries.push_back({{extent + 1, 0, 0}, 4096, extent, 0, 1});
  ASSERT_TRUE(allocator.RestoreBulk(entries));
  const SlabAllocator::MetadataMemory occupied =
      allocator.MetadataMemoryUsage();
  EXPECT_TRUE(
      SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, occupied));
  ASSERT_EQ(allocator.Remove({1, 0, 0}), SlabAllocator::RemoveResult::kRemoved);
  ASSERT_EQ(allocator.ExtentReturns(), 1U);
  const SlabAllocator::MetadataMemory returned =
      allocator.MetadataMemoryUsage();
  EXPECT_EQ(returned.class_rotation_bytes, occupied.class_rotation_bytes);
  EXPECT_LT(returned.class_free_slot_bytes, occupied.class_free_slot_bytes);
  EXPECT_TRUE(
      SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, returned));
  // Rebinding the returned extent with the same geometry restores the lost
  // per-extent capacity without growing its retained rotation vector.
  ASSERT_TRUE(allocator.RestoreBulk({{{10, 0, 0}, 4096, 0, 0, 1}}));
  const SlabAllocator::MetadataMemory rebound = allocator.MetadataMemoryUsage();
  EXPECT_EQ(rebound.class_rotation_bytes, occupied.class_rotation_bytes);
  EXPECT_EQ(rebound.class_free_slot_bytes, occupied.class_free_slot_bytes);
  EXPECT_TRUE(SlabAllocatorTestPeer::MetadataMemoryMatches(allocator, rebound));
}

TEST(SlabLifetimeStorage, DisabledStateStorageNeverAllocatesPerSlot) {
  for (const bool track_identity : {false, true}) {
    SlabAllocator::Options options = LifetimeOptions();
    options.track_lifetime_state = false;
    options.track_resident_identity = track_identity;
    SlabAllocator allocator(options);
    std::vector<BlockKey> evicted;
    SlabAllocator::SlotRef ref;
    for (uint64_t i = 0; i < 32; ++i)
      ASSERT_TRUE(allocator.Put({i, 0, 0}, 4096, &ref, &evicted));
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueCount(allocator), 0U);
    EXPECT_TRUE(allocator.LifetimeQueueStats().empty());
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeStateSize(allocator), 0U);
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeStateCapacity(allocator), 0U);
    EXPECT_FALSE(SlabAllocatorTestPeer::LifetimeState(allocator, {31, 0, 0})
                     .has_value());
  }
}

TEST(SlabLifetimeStorage, EnabledStateAndGenerationCommitAtValidZero) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey key{1, 2, 3};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeStateSize(allocator), 1U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationSize(allocator), 1U);
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 41, &state);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 41U);
  ExpectState(allocator, key, state);
}

TEST(SlabLifetimeStorage, NullStateCommitDoesNotInventInitializedHeat) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey key{4, 5, 6};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 42);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 42U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
  EXPECT_TRUE(allocator.Contains(key));
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeStateSize(allocator), 1U);
}

TEST(SlabLifetimeStorage, DuplicatePutPreservesTheOriginalCommittedState) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey key{7, 8, 9};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 43, &state);
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 43U);
  ExpectState(allocator, key, state);
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeStateSize(allocator), 1U);
}

TEST(SlabLifetimeStorage, PhysicalReleaseClearsSameAndDifferentKeyReuse) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey key{10, 0, 0};
  const BlockKey other{11, 0, 0};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  SlabAllocator::SlotHandle first;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted, &first));
  const DiskLifetimeState original = NewDiskLifetimeState(17);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 44, &original);
  ASSERT_EQ(allocator.Remove(key), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
  SlabAllocator::SlotHandle reused;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted, &reused));
  EXPECT_EQ(reused.index, first.index);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
  const DiskLifetimeState replacement = NewDiskLifetimeState(29);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 45, &replacement);
  ExpectState(allocator, key, replacement);
  ASSERT_EQ(allocator.Remove(key), SlabAllocator::RemoveResult::kRemoved);
  ASSERT_TRUE(allocator.Put(other, 4096, &ref, &evicted, &reused));
  EXPECT_EQ(reused.index, first.index);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, other), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, other).has_value());
  const DiskLifetimeState recovered = RecoveredDiskLifetimeState(2);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, other, 46, &recovered);
  ExpectState(allocator, other, recovered);
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeStateSize(allocator), 1U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationSize(allocator), 1U);
}

TEST(SlabLifetimeStorage, PendingPinnedRemovalRetainsPhysicalState) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey key{12, 0, 0};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  const DiskLifetimeState state = NewDiskLifetimeState(31);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 47, &state);
  ASSERT_TRUE(allocator.Pin(key));
  EXPECT_EQ(allocator.Remove(key), SlabAllocator::RemoveResult::kDeferred);
  EXPECT_FALSE(allocator.Contains(key));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 47U);
  ExpectState(allocator, key, state);
  ASSERT_TRUE(allocator.Unpin(key));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
  EXPECT_EQ(allocator.Count(), 0U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeStorage, FailedPersistenceRetainsStateUntilPhysicalEviction) {
  SlabAllocator::Options options = LifetimeOptions();
  bool allow = false;
  options.on_extent_evict = [&](uint32_t, uint32_t, uint32_t) { return allow; };
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) { return allow; };
  SlabAllocator allocator(options);
  const BlockKey key{13, 0, 0};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  const DiskLifetimeState state = NewDiskLifetimeState(37);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 48, &state);
  EXPECT_EQ(allocator.EvictColdToTarget(0, 4, &evicted), 0U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 48U);
  ExpectState(allocator, key, state);
  allow = true;
  ASSERT_EQ(allocator.EvictColdToTarget(0, 4, &evicted), 1U);
  ASSERT_EQ(evicted.size(), 1U);
  EXPECT_EQ(evicted[0], key);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
}

TEST(SlabLifetimeStorage, BulkRestoreReservesStateWithoutInventingCommit) {
  SlabAllocator::Options options = LifetimeOptions();
  options.num_extents = 2;
  SlabAllocator allocator(options);
  const BlockKey first{14, 0, 0};
  const BlockKey second{15, 0, 0};
  const BlockKey uninitialized{16, 0, 0};
  ASSERT_TRUE(allocator.RestoreBulk({{first, 4096, 0, 0, 3},
                                     {second, 4096, 0, 1, 5},
                                     {uninitialized, 4096, 1, 0, 7}}));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationSize(allocator), 3U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCount(allocator), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeStateSize(allocator), 3U);
  EXPECT_GE(SlabAllocatorTestPeer::LifetimeStateCapacity(allocator), 3U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, first).has_value());
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, second).has_value());
  const DiskLifetimeState first_state = RecoveredDiskLifetimeState(1);
  const DiskLifetimeState second_state = RecoveredDiskLifetimeState(2);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, first, 49, &first_state);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, second, 50,
                                          &second_state);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, uninitialized, 51);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, first), 49U);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, second), 50U);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, uninitialized), 51U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCount(allocator), 3U);
  ExpectState(allocator, first, first_state);
  ExpectState(allocator, second, second_state);
  EXPECT_FALSE(SlabAllocatorTestPeer::LifetimeState(allocator, uninitialized)
                   .has_value());
}

TEST(SlabLifetimeStorage, ConcurrentIndependentOwnersCommitAndReleaseState) {
  SlabAllocator::Options options = LifetimeOptions();
  options.extent_bytes = 8 * 4096;
  SlabAllocator allocator(options);
  const DiskLifetimeConfig config;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 100000;
  std::atomic<bool> valid{true};
  std::vector<std::thread> owners;
  for (uint64_t owner = 0; owner < 4; ++owner) {
    owners.emplace_back([&, owner] {
      for (uint64_t iteration = 1; iteration <= 100; ++iteration) {
        const BlockKey key{owner, iteration, 0};
        std::vector<BlockKey> evicted;
        SlabAllocator::SlotRef ref;
        if (!allocator.Put(key, 4096, &ref, &evicted)) {
          valid.store(false);
          return;
        }
        const uint64_t generation = owner * 100 + iteration;
        const DiskLifetimeState state = NewDiskLifetimeState(
            static_cast<uint32_t>(owner * 1000 + iteration));
        SlabAllocatorTestPeer::CommitGeneration(&allocator, key, generation,
                                                &state);
        const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
            SlabAllocatorTestPeer::LifetimeSnapshotFor(allocator, key);
        const std::optional<DiskLifetimeState>& actual = snapshot.second;
        if (snapshot.first != generation || !actual.has_value()) {
          valid.store(false);
        } else if (actual->anchor != state.anchor ||
                   actual->put_at != state.put_at ||
                   actual->hstate != state.hstate ||
                   actual->actual_reads != state.actual_reads ||
                   actual->history_unknown != state.history_unknown) {
          valid.store(false);
        }
        if (SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, key, generation,
                                                     config, clock) !=
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied) {
          valid.store(false);
        }
        if (!SlabAllocatorTestPeer::LifetimeIndexesValid(allocator))
          valid.store(false);
        if (allocator.Remove(key) != SlabAllocator::RemoveResult::kRemoved)
          valid.store(false);
      }
    });
  }
  for (std::thread& owner : owners) owner.join();
  EXPECT_TRUE(valid.load());
  EXPECT_EQ(allocator.Count(), 0U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCount(allocator), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationSize(allocator),
            SlabAllocatorTestPeer::LifetimeStateSize(allocator));
}

TEST(SlabLifetimeStorage, ConcurrentObserverSeesCoherentSameKeyReuse) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey key{99, 0, 0};
  std::atomic<bool> done{false};
  std::atomic<bool> valid{true};
  std::promise<void> first_live_seen;
  const std::future<void> first_live = first_live_seen.get_future();
  std::thread observer([&] {
    bool published = false;
    while (!done.load()) {
      if (!SlabAllocatorTestPeer::LifetimeIndexesValid(allocator))
        valid.store(false);
      const std::vector<SlabAllocator::LifetimeQueueStat> queue_stats =
          allocator.LifetimeQueueStats();
      if (!QueueSnapshotConservesResidents(queue_stats, 2)) valid.store(false);
      const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
          SlabAllocatorTestPeer::LifetimeSnapshotFor(allocator, key);
      if (snapshot.first == 0) {
        if (snapshot.second.has_value()) valid.store(false);
        continue;
      }
      if (!snapshot.second.has_value()) {
        valid.store(false);
        continue;
      }
      const DiskLifetimeState& state = *snapshot.second;
      const DiskLifetimeState expected =
          NewDiskLifetimeState(static_cast<uint32_t>(snapshot.first));
      const bool coherent = state.anchor == snapshot.first &&
                            state.put_at == snapshot.first &&
                            state.hstate == expected.hstate &&
                            state.actual_reads == expected.actual_reads &&
                            state.history_unknown == expected.history_unknown;
      if (!coherent) valid.store(false);
      if (coherent && !published &&
          QueuePopulation(queue_stats, 0, 0) == std::optional<uint64_t>(1)) {
        first_live_seen.set_value();
        published = true;
      }
    }
  });
  bool observed_first_live = false;
  for (uint64_t generation = 1; generation <= 1000; ++generation) {
    std::vector<BlockKey> evicted;
    SlabAllocator::SlotRef ref;
    if (!allocator.Put(key, 4096, &ref, &evicted)) {
      valid.store(false);
      break;
    }
    const DiskLifetimeState state =
        NewDiskLifetimeState(static_cast<uint32_t>(generation));
    SlabAllocatorTestPeer::CommitGeneration(&allocator, key, generation,
                                            &state);
    if (generation == 1) {
      // Keep the first tuple resident until the observer has sampled it. The
      // timeout bounds failure without leaving a blocked thread or promise.
      observed_first_live = first_live.wait_for(std::chrono::seconds(5)) ==
                            std::future_status::ready;
      if (!observed_first_live) valid.store(false);
    }
    if (allocator.Remove(key) != SlabAllocator::RemoveResult::kRemoved) {
      valid.store(false);
      break;
    }
  }
  done.store(true);
  observer.join();
  EXPECT_TRUE(observed_first_live);
  EXPECT_TRUE(valid.load());
  EXPECT_EQ(allocator.Count(), 0U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeStorage, QueuePopulationSnapshotTracksPhysicalMembership) {
  for (const uint32_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    SlabAllocator::Options options = LifetimeOptions();
    options.num_extents = 2;
    options.lifetime_max_reads = max_reads;
    SlabAllocator allocator(options);
    std::vector<SlabAllocator::LifetimeQueueStat> stats =
        allocator.LifetimeQueueStats();
    ASSERT_EQ(stats.size(), max_reads + 1);
    EXPECT_TRUE(QueueSnapshotConservesResidents(stats, max_reads));
    const BlockKey fresh{201, 0, 0};
    const BlockKey recovered{202, 0, 0};
    const BlockKey uninitialized{203, 0, 0};
    SlabAllocator::SlotRef ref;
    std::vector<BlockKey> evicted;
    ASSERT_TRUE(allocator.Put(fresh, 4096, &ref, &evicted));
    const DiskLifetimeState fresh_state = NewDiskLifetimeState(0);
    SlabAllocatorTestPeer::CommitGeneration(&allocator, fresh, 201,
                                            &fresh_state);
    ASSERT_TRUE(allocator.Put(recovered, 8192, &ref, &evicted));
    const DiskLifetimeState recovered_state = RecoveredDiskLifetimeState(2);
    SlabAllocatorTestPeer::CommitGeneration(&allocator, recovered, 202,
                                            &recovered_state);
    ASSERT_TRUE(allocator.Put(uninitialized, 4096, &ref, &evicted));
    ASSERT_TRUE(evicted.empty());
    const uint32_t recovered_band = max_reads < 2 ? max_reads : 2;
    stats = allocator.LifetimeQueueStats();
    EXPECT_EQ(stats.size(), 3 * (max_reads + 1));
    EXPECT_EQ(QueuePopulation(stats, 0, 0), std::optional<uint64_t>(1));
    EXPECT_EQ(QueuePopulation(stats, 4096, 0), std::optional<uint64_t>(1));
    EXPECT_EQ(QueuePopulation(stats, 0, recovered_band),
              std::optional<uint64_t>(1));
    EXPECT_EQ(QueuePopulation(stats, 8192, recovered_band),
              std::optional<uint64_t>(1));
    EXPECT_TRUE(QueueSnapshotConservesResidents(stats, max_reads));
    EXPECT_EQ(allocator.Count(), 3U);  // One resident has no scoring state.
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
    ASSERT_TRUE(allocator.Pin(fresh));
    ASSERT_EQ(allocator.Remove(fresh), SlabAllocator::RemoveResult::kDeferred);
    stats = allocator.LifetimeQueueStats();
    EXPECT_EQ(QueuePopulation(stats, 0, 0), std::optional<uint64_t>(1));
    EXPECT_EQ(QueuePopulation(stats, 4096, 0), std::optional<uint64_t>(1));
    ASSERT_TRUE(allocator.Unpin(fresh));
    stats = allocator.LifetimeQueueStats();
    EXPECT_EQ(QueuePopulation(stats, 0, 0), std::optional<uint64_t>(0));
    // The raw resident keeps this class visible, but adds no indexed member.
    EXPECT_EQ(QueuePopulation(stats, 4096, 0), std::optional<uint64_t>(0));
    ASSERT_EQ(allocator.Remove(uninitialized),
              SlabAllocator::RemoveResult::kRemoved);
    stats = allocator.LifetimeQueueStats();
    EXPECT_FALSE(QueuePopulation(stats, 4096, 0).has_value());
    ASSERT_EQ(allocator.Remove(recovered),
              SlabAllocator::RemoveResult::kRemoved);
    stats = allocator.LifetimeQueueStats();
    EXPECT_EQ(stats.size(), max_reads + 1);
    EXPECT_EQ(QueuePopulation(stats, 0, recovered_band),
              std::optional<uint64_t>(0));
    EXPECT_TRUE(QueueSnapshotConservesResidents(stats, max_reads));
    // Reuse starts in the new write's band, not the previous resident's band.
    ASSERT_TRUE(allocator.Put(fresh, 4096, &ref, &evicted));
    SlabAllocatorTestPeer::CommitGeneration(&allocator, fresh, 204,
                                            &fresh_state);
    stats = allocator.LifetimeQueueStats();
    EXPECT_EQ(QueuePopulation(stats, 0, 0), std::optional<uint64_t>(1));
    EXPECT_EQ(QueuePopulation(stats, 4096, 0), std::optional<uint64_t>(1));
    EXPECT_TRUE(QueueSnapshotConservesResidents(stats, max_reads));
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

TEST(SlabLifetimeStorage, ConfiguredBandCountsCoverBothIndexesAndClasses) {
  for (const uint32_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    SlabAllocator::Options options = LifetimeOptions();
    options.num_extents = 2;
    options.lifetime_max_reads = max_reads;
    SlabAllocator allocator(options);
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueCount(allocator),
              (allocator.ClassCount() + 1) * (max_reads + 1));
    std::vector<BlockKey> evicted;
    const BlockKey small{101, 0, 0};
    const BlockKey large{102, 0, 0};
    SlabAllocator::SlotRef small_ref;
    SlabAllocator::SlotRef large_ref;
    ASSERT_TRUE(allocator.Put(small, 4096, &small_ref, &evicted));
    ASSERT_TRUE(allocator.Put(large, 8192, &large_ref, &evicted));
    ASSERT_NE(small_ref.cls, large_ref.cls);
    EXPECT_TRUE(
        SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, 0, 0).empty());
    const DiskLifetimeState state = NewDiskLifetimeState(0);
    SlabAllocatorTestPeer::CommitGeneration(&allocator, small, 101, &state);
    SlabAllocatorTestPeer::CommitGeneration(&allocator, large, 102, &state);
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, 0, 0),
              (std::vector<BlockKey>{small, large}));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator,
                                                       small_ref.cls + 1, 0),
              (std::vector<BlockKey>{small}));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator,
                                                       large_ref.cls + 1, 0),
              (std::vector<BlockKey>{large}));
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

TEST(SlabLifetimeStorage, EarlyReadUsesEffectiveBandAndRejectedReadKeepsOrder) {
  SlabAllocator::Options options = LifetimeOptions();
  options.lifetime_max_reads = 4;
  SlabAllocator allocator(options);
  const BlockKey key{103, 0, 0};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 103, &state);
  DiskLifetimeConfig config;
  config.max_reads = 4;
  config.bucket_sec = 864000;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 1;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, key, 103,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  const std::optional<DiskLifetimeState> read_state =
      SlabAllocatorTestPeer::LifetimeState(allocator, key);
  ASSERT_TRUE(read_state.has_value());
  EXPECT_EQ(read_state->scoring_reads(), 1U);
  EXPECT_TRUE(read_state->first_early_read());
  for (const size_t row : {size_t{0}, size_t{ref.cls + 1}}) {
    EXPECT_TRUE(
        SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 0).empty());
    EXPECT_TRUE(
        SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 1).empty());
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 2),
              (std::vector<BlockKey>{key}));
  }
  seconds = 2;
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, key, 104,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kGenerationMismatch);
  ExpectState(allocator, key, *read_state);
  const std::vector<SlabAllocator::LifetimeQueueStat> stats =
      allocator.LifetimeQueueStats();
  EXPECT_EQ(QueuePopulation(stats, 0, 2), std::optional<uint64_t>(1));
  EXPECT_EQ(QueuePopulation(stats, 4096, 2), std::optional<uint64_t>(1));
  EXPECT_TRUE(QueueSnapshotConservesResidents(stats, 4));
}

TEST(SlabLifetimeStorage, SaturatedReadsMoveBothQueuesAndRepairCursors) {
  SlabAllocator::Options options = LifetimeOptions();
  options.lifetime_max_reads = 1;
  SlabAllocator allocator(options);
  const BlockKey first{104, 0, 0};
  const BlockKey second{105, 0, 0};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(first, 4096, &ref, &evicted));
  ASSERT_TRUE(allocator.Put(second, 4096, &ref, &evicted));
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, first, 104, &state);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, second, 105, &state);
  ASSERT_TRUE(SlabAllocatorTestPeer::PointLifetimeCursorsAt(allocator, first));
  DiskLifetimeConfig config;
  config.max_reads = 1;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 200;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, first, 104,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  for (const size_t row : {size_t{0}, size_t{ref.cls + 1}}) {
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 1),
              (std::vector<BlockKey>{second, first}));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 1),
              std::optional<BlockKey>(second));
  }
  seconds = 300;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, second, 105,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  for (const size_t row : {size_t{0}, size_t{ref.cls + 1}}) {
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 1),
              (std::vector<BlockKey>{first, second}));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 1),
              std::optional<BlockKey>(first));
  }
  // Equal-second logical requests still update order, even at the same band.
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, first, 104,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  for (const size_t row : {size_t{0}, size_t{ref.cls + 1}}) {
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 1),
              (std::vector<BlockKey>{second, first}));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 1),
              std::optional<BlockKey>(second));
  }
  const std::vector<SlabAllocator::LifetimeQueueStat> stats =
      allocator.LifetimeQueueStats();
  EXPECT_EQ(QueuePopulation(stats, 0, 1), std::optional<uint64_t>(2));
  EXPECT_EQ(QueuePopulation(stats, 4096, 1), std::optional<uint64_t>(2));
  EXPECT_TRUE(QueueSnapshotConservesResidents(stats, 1));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeStorage, DelayedCommitAndRestoreInsertByAnchorNotEnumeration) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey later{106, 0, 0};
  const BlockKey earlier{107, 0, 0};
  const BlockKey restored{108, 0, 0};
  ASSERT_TRUE(allocator.RestoreBulk({{later, 4096, 0, 0, 1},
                                     {earlier, 4096, 0, 1, 1},
                                     {restored, 4096, 0, 2, 1}}));
  const DiskLifetimeConfig config;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  DiskLifetimeState late_state = NewDiskLifetimeState(0);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 400, &late_state));
  DiskLifetimeState early_state = NewDiskLifetimeState(0);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 200, &early_state));
  const DiskLifetimeState restored_state = RecoveredDiskLifetimeState(1);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, later, 106, &late_state);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, earlier, 107,
                                          &early_state);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, restored, 108,
                                          &restored_state);
  for (const size_t row : {size_t{0}, size_t{1}}) {
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 1),
              (std::vector<BlockKey>{restored, earlier, later}));
  }
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeStorage, DeleteAndMigrationRepairMiddleTailAndEmptyCursors) {
  SlabAllocator allocator(LifetimeOptions());
  const BlockKey first{109, 0, 0};
  const BlockKey middle{110, 0, 0};
  const BlockKey last{111, 0, 0};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  uint64_t generation = 109;
  for (const BlockKey key : {first, middle, last}) {
    ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
    const DiskLifetimeState state =
        NewDiskLifetimeState(static_cast<uint32_t>(generation));
    SlabAllocatorTestPeer::CommitGeneration(&allocator, key, generation,
                                            &state);
    ++generation;
  }
  ASSERT_TRUE(SlabAllocatorTestPeer::PointLifetimeCursorsAt(allocator, middle));
  ASSERT_EQ(allocator.Remove(middle), SlabAllocator::RemoveResult::kRemoved);
  for (const size_t row : {size_t{0}, size_t{ref.cls + 1}}) {
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 0),
              std::optional<BlockKey>(last));
  }
  ASSERT_EQ(allocator.Remove(last), SlabAllocator::RemoveResult::kRemoved);
  for (const size_t row : {size_t{0}, size_t{ref.cls + 1}}) {
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 0),
              std::optional<BlockKey>(first));
  }
  const DiskLifetimeConfig config;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 112;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, first, 109,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  for (const size_t row : {size_t{0}, size_t{ref.cls + 1}}) {
    EXPECT_FALSE(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 0)
                     .has_value());
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 2),
              std::optional<BlockKey>(first));
  }
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  ASSERT_EQ(allocator.Remove(first), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

}  // namespace
}  // namespace dfkv
