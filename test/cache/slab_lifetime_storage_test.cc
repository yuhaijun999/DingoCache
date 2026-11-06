#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <optional>
#include <thread>
#include <vector>

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
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
  SlabAllocator::SlotHandle reused;
  ASSERT_TRUE(allocator.Put(key, 4096, &ref, &evicted, &reused));
  EXPECT_EQ(reused.index, first.index);
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
  EXPECT_EQ(SlabAllocatorTestPeer::Generation(allocator, key), 0U);
  EXPECT_FALSE(
      SlabAllocatorTestPeer::LifetimeState(allocator, key).has_value());
  EXPECT_EQ(allocator.Count(), 0U);
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
        if (allocator.Remove(key) != SlabAllocator::RemoveResult::kRemoved)
          valid.store(false);
      }
    });
  }
  for (std::thread& owner : owners) owner.join();
  EXPECT_TRUE(valid.load());
  EXPECT_EQ(allocator.Count(), 0U);
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
      if (coherent && !published) {
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
}

}  // namespace
}  // namespace dfkv
