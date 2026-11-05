#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
namespace {

SlabAllocator::Options IdentityOptions() {
  SlabAllocator::Options options;
  options.extent_bytes = 4 * 4096;
  options.num_extents = 1;
  options.track_resident_identity = true;
  return options;
}

TEST(SlabResidentIdentity, DisabledStorageNeverAllocatesPerSlot) {
  SlabAllocator::Options options = IdentityOptions();
  options.track_resident_identity = false;
  SlabAllocator allocator(options);
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  for (uint64_t i = 0; i < 32; ++i)
    ASSERT_TRUE(allocator.Put({i, 0, 0}, 4096, &ref, &evicted));
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationSize(allocator), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCapacity(allocator), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, {31, 0, 0}), 0U);
}

TEST(SlabResidentIdentity, CommitIsSeparateFromReservationAndReuse) {
  SlabAllocator allocator(IdentityOptions());
  const BlockKey key{1, 2, 3};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  SlabAllocator::SlotHandle original;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted, &original));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 41);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 41U);
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 41U);
  ASSERT_TRUE(allocator.Pin(key));
  EXPECT_EQ(allocator.Remove(key), SlabAllocator::RemoveResult::kDeferred);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 41U);
  ASSERT_TRUE(allocator.Unpin(key));
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  SlabAllocator::SlotHandle replacement;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted, &replacement));
  EXPECT_EQ(replacement.index, original.index);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 42);
  EXPECT_EQ(allocator.Remove(key), SlabAllocator::RemoveResult::kRemoved);
  const BlockKey other{4, 5, 6};
  ASSERT_TRUE(allocator.Put(other, 4096, &ref, &evicted, &replacement));
  EXPECT_EQ(replacement.index, original.index);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, other), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationSize(allocator), 1U);
}

TEST(SlabResidentIdentity, FailedPersistenceRetainsPhysicalGeneration) {
  SlabAllocator::Options options = IdentityOptions();
  bool allow = false;
  options.on_extent_evict = [&](uint32_t, uint32_t, uint32_t) { return allow; };
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) { return allow; };
  SlabAllocator allocator(options);
  const BlockKey key{7, 8, 9};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted));
  SlabAllocatorTestPeer::CommitGeneration(&allocator, key, 51);
  EXPECT_EQ(allocator.EvictColdToTarget(0, 4, &evicted), 0U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 51U);
  allow = true;
  EXPECT_EQ(allocator.EvictColdToTarget(0, 4, &evicted), 1U);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCount(allocator), 0U);
}

TEST(SlabResidentIdentity, BulkRestoreDoesNotInventCommittedGenerations) {
  SlabAllocator allocator(IdentityOptions());
  const BlockKey first{10, 0, 0};
  const BlockKey second{11, 0, 0};
  ASSERT_TRUE(
      allocator.RestoreBulk({{first, 4096, 0, 0, 3}, {second, 4096, 0, 1, 5}}));
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationSize(allocator), 2U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCount(allocator), 0U);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, first, 61);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, second, 62);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, first), 61U);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, second), 62U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCount(allocator), 2U);
  RecordProperty("slot_meta_bytes",
                 std::to_string(SlabAllocatorTestPeer::SlotMetaBytes()));
  RecordProperty(
      "generation_capacity_bytes",
      SlabAllocatorTestPeer::GenerationCapacity(allocator) * sizeof(uint64_t));
}

TEST(SlabResidentIdentity, InspectionDoesNotRefreshLegacyVictimOrder) {
  SlabAllocator allocator(IdentityOptions());
  const BlockKey oldest{12, 0, 0};
  const BlockKey newest{13, 0, 0};
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(oldest, 4096, &ref, &evicted));
  SlabAllocatorTestPeer::CommitGeneration(&allocator, oldest, 71);
  ASSERT_TRUE(allocator.Put(newest, 4096, &ref, &evicted));
  SlabAllocatorTestPeer::CommitGeneration(&allocator, newest, 72);
  for (int i = 0; i < 10; ++i)
    EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, oldest), 71U);
  EXPECT_EQ(allocator.Classes()[0].read_touches, 0U);
  ASSERT_EQ(allocator.EvictColdToTarget(4096, 1, &evicted), 1U);
  ASSERT_EQ(evicted.size(), 1U);
  EXPECT_EQ(evicted[0], oldest);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, oldest), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, newest), 72U);
}

TEST(SlabResidentIdentity, ConcurrentDistinctOwnersCommitQueryAndRemove) {
  SlabAllocator allocator(IdentityOptions());
  std::atomic<bool> valid{true};
  std::vector<std::thread> threads;
  // At most four live keys fit without eviction. Each owner serializes its
  // own key; the internal commit API must not race with same-key removal.
  for (uint64_t owner = 0; owner < 4; ++owner) {
    threads.emplace_back([&, owner] {
      for (uint64_t i = 1; i <= 100; ++i) {
        const BlockKey key{owner, i, 0};
        std::vector<BlockKey> evicted;
        SlabAllocator::SlotRef ref;
        if (!allocator.Put(key, 4096, &ref, &evicted)) {
          valid.store(false);
          return;
        }
        const uint64_t generation = owner * 100 + i;
        SlabAllocatorTestPeer::CommitGeneration(&allocator, key, generation);
        if (SlabAllocatorTestPeer::Generation(allocator, key) != generation)
          valid.store(false);
        if (allocator.Remove(key) != SlabAllocator::RemoveResult::kRemoved)
          valid.store(false);
      }
    });
  }
  for (std::thread& thread : threads) thread.join();
  EXPECT_TRUE(valid.load());
  EXPECT_EQ(allocator.Count(), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::GenerationCount(allocator), 0U);
}

}  // namespace
}  // namespace dfkv
