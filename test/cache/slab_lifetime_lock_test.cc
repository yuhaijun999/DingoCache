#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
namespace {

SlabAllocator::Options LockOptions(const DiskLifetimeConfig* config,
                                   const DiskLifetimeClock* clock) {
  SlabAllocator::Options options;
  options.extent_bytes = 4096;
  options.num_extents = 1;
  options.track_lifetime_state = config != nullptr;
  options.lifetime_donor_config = config;
  options.lifetime_donor_clock = clock;
  if (config != nullptr) {
    options.lifetime_max_reads = static_cast<uint32_t>(config->max_reads);
    options.lifetime_bucket_sec = config->bucket_sec;
    options.lifetime_bucket_count = static_cast<uint32_t>(config->bucket_count);
    options.lifetime_read_window_sec = config->protect_sec;
  }
  return options;
}

TEST(SlabLifetimeLock, CountsEarlyReturnsAndKeepsPathsSeparate) {
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  SlabAllocator allocator(LockOptions(&config, &clock));
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put({1, 0}, 1, &ref, &evicted));
  ASSERT_TRUE(allocator.Put({1, 0}, 1, &ref, &evicted));
  SlabAllocatorTestPeer::LifetimeScan scan(config, clock);
  EXPECT_EQ(SlabAllocatorTestPeer::ReclaimLifetimeClass(allocator, 99, 1, 1,
                                                        scan, &evicted)
                .victims,
            0U);
  EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 0, config, clock, &evicted),
            0U);
  EXPECT_FALSE(allocator.StealFrom(99, 99, &evicted));
  const std::array<SlabAllocator::LifetimeReclaimLockStats, 4> stats =
      allocator.ReclaimLockStats();
  for (size_t path = 0; path < stats.size(); ++path) {
    EXPECT_EQ(stats[path].calls, path == 0 ? 2U : 1U);
    EXPECT_GT(stats[path].held_ns, 0U);
  }
  // Snapshot reads must not themselves become reclaim lock observations.
  const std::array<SlabAllocator::LifetimeReclaimLockStats, 4> again =
      allocator.ReclaimLockStats();
  for (size_t path = 0; path < stats.size(); ++path) {
    EXPECT_EQ(again[path].calls, stats[path].calls);
    EXPECT_EQ(again[path].wait_ns, stats[path].wait_ns);
    EXPECT_EQ(again[path].held_ns, stats[path].held_ns);
  }
}

TEST(SlabLifetimeLock, PersistenceFailureStillSettlesAllocationInterval) {
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  SlabAllocator::Options options = LockOptions(&config, &clock);
  options.on_extent_bind = [](uint32_t) { return false; };
  SlabAllocator allocator(options);
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  EXPECT_FALSE(allocator.Put({2, 0}, 1, &ref, &evicted));
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), 0U);
  EXPECT_EQ(allocator.ReclaimLockStats()[0].calls, 1U);
  EXPECT_GT(allocator.ReclaimLockStats()[0].held_ns, 0U);
}

TEST(SlabLifetimeLock, ConcurrentCallsHaveOneIntervalPerAcquisition) {
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  SlabAllocator allocator(LockOptions(&config, &clock));
  std::array<std::thread, 4> threads;
  for (std::thread& thread : threads) {
    thread = std::thread([&allocator] {
      for (size_t iteration = 0; iteration < 100; ++iteration) {
        SlabAllocator::SlotRef ref;
        std::vector<BlockKey> evicted;
        EXPECT_TRUE(allocator.Put({3, 0}, 1, &ref, &evicted));
      }
    });
  }
  for (std::thread& thread : threads) thread.join();
  EXPECT_EQ(allocator.ReclaimLockStats()[0].calls, 400U);
  EXPECT_EQ(allocator.Count(), 1U);
  EXPECT_GT(allocator.ReclaimLockStats()[0].held_ns, 0U);
  // No scheduler-sensitive latency threshold or throughput assertion.
}

TEST(SlabLifetimeLock, DonorCallbackRetainsLegacyFallbackAndFailureIsolation) {
  for (const bool separate_callback : {false, true}) {
    SCOPED_TRACE(separate_callback);
    SlabAllocator::Options options;
    options.extent_bytes = 8192;
    options.num_extents = 2;
    size_t initial_calls = 0;
    size_t donor_calls = 0;
    options.on_extent_bind = [&initial_calls](uint32_t) {
      ++initial_calls;
      return true;
    };
    if (separate_callback) {
      options.on_extent_rebind = [&donor_calls](uint32_t) {
        ++donor_calls;
        return false;
      };
    }
    SlabAllocator allocator(options);
    ASSERT_TRUE(allocator.RestoreBulk(
        {{{10, 0}, 4096, 0, 0, 1}, {{11, 0}, 8192, 1, 0, 1}}));
    EXPECT_EQ(initial_calls, 0U);  // Restore never wipes validated metadata.
    const size_t donor = SlabAllocatorTestPeer::ClassIndex(allocator, 4096);
    const size_t target = SlabAllocatorTestPeer::ClassIndex(allocator, 8192);
    std::vector<BlockKey> evicted;
    const SlabAllocatorTestPeer::ExtentBindResult result =
        SlabAllocatorTestPeer::StealRestricted(allocator, donor, target,
                                               &evicted);
    if (separate_callback) {
      EXPECT_EQ(result,
                SlabAllocatorTestPeer::ExtentBindResult::kPersistenceError);
      EXPECT_EQ(donor_calls, 1U);
      EXPECT_EQ(initial_calls, 0U);
      EXPECT_TRUE(evicted.empty());
      EXPECT_TRUE(allocator.Contains({10, 0}));
      EXPECT_EQ(allocator.Count(), 2U);
    } else {
      EXPECT_EQ(result, SlabAllocatorTestPeer::ExtentBindResult::kBound);
      EXPECT_EQ(initial_calls, 1U);
      EXPECT_EQ(evicted, std::vector<BlockKey>({{10, 0}}));
      EXPECT_FALSE(allocator.Contains({10, 0}));
      EXPECT_EQ(allocator.Count(), 1U);
    }
    EXPECT_TRUE(allocator.Contains({11, 0}));
  }
}

TEST(SlabLifetimeLock, LegacyAndRamDoNotRecordIntervals) {
  SlabAllocator allocator(LockOptions(nullptr, nullptr));
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put({4, 0}, 1, &ref, &evicted));
  allocator.StealFrom(99, 99, &evicted);
  for (const SlabAllocator::LifetimeReclaimLockStats& stats :
       allocator.ReclaimLockStats()) {
    EXPECT_EQ(stats.calls, 0U);
    EXPECT_EQ(stats.wait_ns, 0U);
    EXPECT_EQ(stats.held_ns, 0U);
  }
}

}  // namespace
}  // namespace dfkv
