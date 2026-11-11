#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <numeric>
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
using Observation = SlabAllocator::LifetimeReadClassObservation;
using ReadResult = SlabAllocatorTestPeer::LifetimeReadResult;
using Population = std::array<uint64_t, 5>;

BlockKey Key(uint64_t id) { return {id, 0, 7}; }

DiskLifetimeConfig Config(uint64_t window = 10, uint64_t buckets = 4,
                          uint64_t max_reads = 2) {
  DiskLifetimeConfig config;
  config.base_sec = 1;
  config.read_bonus_sec = 0;
  config.max_reads = max_reads;
  config.protect_sec = window;
  config.slowdown = 1;
  config.boost_sec = window + 2;
  config.keep_score_sec = 0;
  config.bucket_sec = window;
  config.bucket_count = buckets;
  config.high_pct = 0;
  std::string error;
  EXPECT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  return config;
}

SlabAllocator::Options Options(const DiskLifetimeConfig& config,
                               uint32_t slots = 8, uint32_t extents = 1) {
  SlabAllocator::Options options;
  options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
  options.num_extents = extents;
  options.track_lifetime_state = true;
  options.lifetime_max_reads = static_cast<uint32_t>(config.max_reads);
  options.lifetime_bucket_sec = config.bucket_sec;
  options.lifetime_bucket_count = static_cast<uint32_t>(config.bucket_count);
  options.lifetime_read_window_sec = config.protect_sec;
  return options;
}

void Seed(SlabAllocator& allocator, uint64_t id, const DiskLifetimeState* state,
          const DiskLifetimeConfig& config, uint32_t now = 0) {
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(id), 1, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  // Each raw allocation receives exactly one committed generation.
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, state,
                                          &config, now);
}

void SeedNew(SlabAllocator& allocator, uint64_t id,
             const DiskLifetimeConfig& config, uint32_t at = 0) {
  const DiskLifetimeState state = NewDiskLifetimeState(at);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, &state, config, at));
}

void Read(SlabAllocator& allocator, uint64_t id,
          const DiskLifetimeConfig& config, const DiskLifetimeClock& clock) {
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(id), id,
                                                     config, clock),
            ReadResult::kApplied);
}

Observation ExpectPopulation(const SlabAllocator& allocator,
                             const DiskLifetimeClock& clock,
                             const Population& expected) {
  const Observation observation = allocator.ObserveLifetimeReadClasses(clock);
  EXPECT_TRUE(observation.available);
  EXPECT_EQ(observation.resident_estimates, expected);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeBucketsValid(allocator));
  return observation;
}

TEST(SlabLifetimeReadClass, ActualReadsDoNotDependOnScoringCapOrRecoverySeed) {
  for (const uint64_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    const DiskLifetimeConfig config = Config(3, 4, max_reads);
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&] { return seconds; });
    SlabAllocator allocator(Options(config));
    ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
    const DiskLifetimeState recovered = RecoveredDiskLifetimeState(2);
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, &recovered, config));
    ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 3, config));
    ExpectPopulation(allocator, clock, {3, 0, 0, 0, 0});
    for (unsigned count = 0; count < 5; ++count) {
      ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
    }
    ExpectPopulation(allocator, clock, {2, 1, 0, 0, 0});
    ASSERT_NO_FATAL_FAILURE(Read(allocator, 2, config, clock));
    ExpectPopulation(allocator, clock, {1, 1, 1, 0, 0});
    ASSERT_NO_FATAL_FAILURE(Read(allocator, 2, config, clock));
    ExpectPopulation(allocator, clock, {1, 1, 0, 1, 0});
    for (unsigned count = 0; count < 3; ++count) {
      ASSERT_NO_FATAL_FAILURE(Read(allocator, 2, config, clock));
    }
    ExpectPopulation(allocator, clock, {1, 1, 0, 0, 1});
    seconds = 4;  // Q=1: R expires without another GET, unread NEW stays W.
    ExpectPopulation(allocator, clock, {1, 0, 0, 0, 2});
  }
}

TEST(SlabLifetimeReadClass, FirstReadAtInclusiveWindowEndExpiresWithoutGet) {
  const DiskLifetimeConfig config = Config();  // P10/B4 => Q4.
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  seconds = 10;
  EXPECT_FALSE(DiskLifetimeEarlyRead(config, 0, seconds));  // fw is not R.
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  const Observation at_end =
      ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  EXPECT_EQ(at_end.sampled_at_sec, 10U);
  EXPECT_EQ(at_end.expiry_quantum_sec, 4U);
  EXPECT_EQ(at_end.expiry_lag_bound_sec, 3U);
  seconds = 11;  // Exact class is O1, but rounded expiry permits this R lag.
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  seconds = 12;
  ExpectPopulation(allocator, clock, {0, 0, 1, 0, 0});
}

TEST(SlabLifetimeReadClass, RefreshedAnchorDoesNotExtendOriginalPutWindow) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  seconds = 5;
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config, 5));
  seconds = 6;
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  seconds = 15;
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  seconds = 16;  // Original 5+P+1, not refreshed anchor15+P+1.
  ExpectPopulation(allocator, clock, {0, 0, 0, 1, 0});
}

TEST(SlabLifetimeReadClass, DefaultResolutionIsIndependentOfProtectionBuckets) {
  const DiskLifetimeConfig config;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  const Observation first = ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  EXPECT_EQ(config.bucket_sec, 43200U);
  EXPECT_EQ(first.expiry_quantum_sec, 8U);  // ceil(180/(24-1)).
  EXPECT_EQ(first.expiry_lag_bound_sec, 7U);
  EXPECT_GE(first.ring_bytes, 24U * 4U * sizeof(uint64_t));
  seconds = 183;
  EXPECT_EQ(ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0}).ring_bytes,
            first.ring_bytes);
  seconds = 184;
  ExpectPopulation(allocator, clock, {0, 0, 1, 0, 0});
}

TEST(SlabLifetimeReadClass, LateSaturatedReadCancelsOnlyItsOldMembership) {
  const DiskLifetimeConfig config = Config(10, 2);  // Q10, expiry20.
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  for (uint64_t id = 1; id <= 2; ++id) {
    ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, id, config));
    for (unsigned count = 0; count < 3; ++count) {
      ASSERT_NO_FATAL_FAILURE(Read(allocator, id, config, clock));
    }
  }
  seconds = 11;  // Both expired logically, but still occupy rounded R page.
  ExpectPopulation(allocator, clock, {0, 2, 0, 0, 0});
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 1});
  ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  seconds = 20;
  ExpectPopulation(allocator, clock, {0, 0, 0, 0, 1});
  ASSERT_EQ(allocator.Remove(Key(2)), SlabAllocator::RemoveResult::kRemoved);
  ExpectPopulation(allocator, clock, {0, 0, 0, 0, 0});
}

TEST(SlabLifetimeReadClass,
     ReusedPageIgnoresOldResidentsAndExpiredSuppliedState) {
  const DiskLifetimeConfig config = Config(10, 2);
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  seconds = 20;
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 2, config, 20));
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 2, config, clock));
  ExpectPopulation(allocator, clock, {0, 1, 1, 0, 0});
  ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  DiskLifetimeState expired = NewDiskLifetimeState(0);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 0, &expired));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 3, &expired, config, 20));
  ExpectPopulation(allocator, clock, {0, 1, 1, 0, 0});
  ASSERT_EQ(allocator.Remove(Key(3)), SlabAllocator::RemoveResult::kRemoved);
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  seconds = 40;
  ExpectPopulation(allocator, clock, {0, 0, 1, 0, 0});
}

TEST(SlabLifetimeReadClass, ExpiredSuppliedStateStillBalancesSamePageRemoval) {
  const DiskLifetimeConfig config = Config(10, 2);
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  seconds = 20;
  DiskLifetimeState expired = NewDiskLifetimeState(0);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 0, &expired));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, &expired, config, 20));
  ExpectPopulation(allocator, clock, {0, 0, 2, 0, 0});
  ASSERT_EQ(allocator.Remove(Key(2)), SlabAllocator::RemoveResult::kRemoved);
  ExpectPopulation(allocator, clock, {0, 0, 1, 0, 0});
  ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  ExpectPopulation(allocator, clock, {0, 0, 0, 0, 0});
}

TEST(SlabLifetimeReadClass, DeferredRemoveRetainsPopulationUntilFinalUnpin) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  ASSERT_TRUE(allocator.Pin(Key(1)));
  ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kDeferred);
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  ASSERT_TRUE(allocator.Unpin(Key(1)));
  EXPECT_EQ(allocator.Count(), 0U);
  ExpectPopulation(allocator, clock, {0, 0, 0, 0, 0});
}

TEST(SlabLifetimeReadClass,
     FailedClassInvalidationDoesNotDebitSuccessfulRetry) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  bool persist = false;
  SlabAllocator::Options options = Options(config, 2);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    return persist;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 2, config));
  std::vector<BlockKey> evicted;
  bool denied = false;
  SlabAllocatorTestPeer::LifetimeScan failed(config, clock);
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, failed,
                                                        &evicted, &denied));
  EXPECT_TRUE(evicted.empty());
  ExpectPopulation(allocator, clock, {2, 0, 0, 0, 0});
  persist = true;
  SlabAllocatorTestPeer::LifetimeScan retried(config, clock);
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, retried,
                                                       &evicted, &denied));
  EXPECT_EQ(evicted.size(), 1U);
  ExpectPopulation(allocator, clock, {1, 0, 0, 0, 0});
}

TEST(SlabLifetimeReadClass, WatermarkFailureAndPinsPreserveRetainedPopulation) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  bool persist = false;
  SlabAllocator::Options options = Options(config);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    return persist;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 2, config));
  ASSERT_TRUE(allocator.Pin(Key(1)));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 2, config, clock, &evicted),
            0U);
  ExpectPopulation(allocator, clock, {2, 0, 0, 0, 0});
  persist = true;
  EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 2, config, clock, &evicted),
            1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2)});
  ExpectPopulation(allocator, clock, {1, 0, 0, 0, 0});
  ASSERT_TRUE(allocator.Unpin(Key(1)));
}

TEST(SlabLifetimeReadClass, DonorFailureAndWholeExtentReleaseUseSameDebit) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  bool persist = false;
  unsigned writes = 0;
  uint32_t wiped_extent = UINT32_MAX;
  SlabAllocator::Options options = Options(config, 4, 2);
  options.lifetime_donor_config = &config;
  options.lifetime_donor_clock = &clock;
  // Rebinding uses the durable bind/wipe callback, not last-slot eviction.
  options.on_extent_bind = [&](uint32_t extent) {
    ++writes;
    wiped_extent = extent;
    return persist;
  };
  SlabAllocator allocator(options);
  ASSERT_TRUE(allocator.RestoreBulk({{Key(1), 4096, 0, 0, 1},
                                     {Key(2), 4096, 0, 1, 1},
                                     {Key(3), 8192, 1, 0, 1}}));
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  for (uint64_t id = 1; id <= 3; ++id) {
    SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state,
                                            &config, 0);
  }
  EXPECT_EQ(writes, 0U);  // RestoreBulk installs already-prepared metadata.
  EXPECT_EQ(allocator.Count(), 3U);
  ExpectPopulation(allocator, clock, {3, 0, 0, 0, 0});
  std::vector<BlockKey> evicted;
  const size_t donor = SlabAllocatorTestPeer::ClassIndex(allocator, 4096);
  const size_t target = SlabAllocatorTestPeer::ClassIndex(allocator, 8192);
  EXPECT_EQ(SlabAllocatorTestPeer::StealRestricted(allocator, donor, target,
                                                   &evicted),
            SlabAllocatorTestPeer::ExtentBindResult::kPersistenceError);
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(writes, 1U);
  EXPECT_EQ(wiped_extent, 0U);
  EXPECT_EQ(allocator.Count(), 3U);
  ExpectPopulation(allocator, clock, {3, 0, 0, 0, 0});
  persist = true;
  EXPECT_EQ(SlabAllocatorTestPeer::StealRestricted(allocator, donor, target,
                                                   &evicted),
            SlabAllocatorTestPeer::ExtentBindResult::kBound);
  EXPECT_EQ(evicted.size(), 2U);
  EXPECT_EQ(writes, 2U);
  EXPECT_EQ(wiped_extent, 0U);
  EXPECT_EQ(allocator.Count(), 1U);
  EXPECT_FALSE(allocator.Contains(Key(1)));
  EXPECT_FALSE(allocator.Contains(Key(2)));
  EXPECT_TRUE(allocator.Contains(Key(3)));
  ExpectPopulation(allocator, clock, {1, 0, 0, 0, 0});
}

TEST(SlabLifetimeReadClass, UninitializedResidentsAndRejectedReadsAreExcluded) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, nullptr, config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 2, config));
  EXPECT_EQ(allocator.Count(), 2U);
  ExpectPopulation(allocator, clock, {1, 0, 0, 0, 0});
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                     config, clock),
            ReadResult::kStateUnavailable);
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(2), 99,
                                                     config, clock),
            ReadResult::kGenerationMismatch);
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(99), 99,
                                                     config, clock),
            ReadResult::kResidentChanged);
  ExpectPopulation(allocator, clock, {1, 0, 0, 0, 0});
  ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  ExpectPopulation(allocator, clock, {1, 0, 0, 0, 0});
}

TEST(SlabLifetimeReadClass, DisabledAndShadowModesDoNotAllocateOrSampleClock) {
  const DiskLifetimeConfig config = Config();
  unsigned samples = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    ++samples;
    return uint64_t{0};
  });
  for (const unsigned mode : {0U, 1U, 2U}) {
    SCOPED_TRACE(mode);
    SlabAllocator::Options options = Options(config);
    options.lifetime_read_window_sec = 0;
    options.track_lifetime_state = mode != 0;
    if (mode < 2) {
      options.lifetime_bucket_sec = 0;
      options.lifetime_bucket_count = 0;
    }
    SlabAllocator allocator(options);
    SlabAllocator::SlotRef ref;
    std::vector<BlockKey> evicted;
    ASSERT_TRUE(allocator.Put(Key(1), 1, &ref, &evicted));
    samples = 0;
    const Observation observation = allocator.ObserveLifetimeReadClasses(clock);
    EXPECT_FALSE(observation.available);
    EXPECT_EQ(observation.resident_estimates, Population{});
    EXPECT_EQ(observation.ring_bytes, 0U);
    EXPECT_EQ(observation.expiry_quantum_sec, 0U);
    EXPECT_EQ(samples, 0U);
  }
}

TEST(SlabLifetimeReadClass, ObservationSamplesOnceButBookkeepingAddsNoClock) {
  const DiskLifetimeConfig config = Config();
  unsigned samples = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    ++samples;
    return uint64_t{0};
  });
  SlabAllocator allocator(Options(config));
  samples = 0;
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  EXPECT_EQ(samples, 0U);
  ExpectPopulation(allocator, clock, {1, 0, 0, 0, 0});
  EXPECT_EQ(samples, 1U);
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  EXPECT_EQ(samples, 2U);  // Existing accepted-read sample only.
  ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  EXPECT_EQ(samples, 3U);
  ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_EQ(samples, 3U);
  ExpectPopulation(allocator, clock, {0, 0, 0, 0, 0});
  EXPECT_EQ(samples, 4U);
}

TEST(SlabLifetimeReadClass, Uint32BoundaryAndStickyHardClockKeepBackingFacts) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config));
  ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, 1, config));
  ASSERT_NO_FATAL_FAILURE(Read(allocator, 1, config, clock));
  const Observation before =
      ExpectPopulation(allocator, clock, {0, 1, 0, 0, 0});
  seconds = UINT32_MAX;
  const Observation edge = ExpectPopulation(allocator, clock, {0, 0, 1, 0, 0});
  EXPECT_EQ(edge.sampled_at_sec, UINT32_MAX);
  seconds = uint64_t{UINT32_MAX} + 1;
  const Observation stopped = allocator.ObserveLifetimeReadClasses(clock);
  EXPECT_FALSE(stopped.available);
  EXPECT_EQ(stopped.resident_estimates, Population{});
  EXPECT_EQ(stopped.ring_bytes, before.ring_bytes);
  EXPECT_EQ(stopped.expiry_quantum_sec, 4U);
  EXPECT_EQ(stopped.expiry_lag_bound_sec, 3U);
  seconds = 0;
  EXPECT_FALSE(allocator.ObserveLifetimeReadClasses(clock).available);
  EXPECT_EQ(allocator.Count(), 1U);
}

TEST(SlabLifetimeReadClass, ConcurrentReadTransitionsConserveOneSnapshotTotal) {
  const DiskLifetimeConfig config = Config();
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [] { return uint64_t{0}; });
  SlabAllocator allocator(Options(config, 64));
  for (uint64_t id = 1; id <= 64; ++id) {
    ASSERT_NO_FATAL_FAILURE(SeedNew(allocator, id, config));
  }
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::atomic<bool> first_snapshot{false};
  std::atomic<unsigned> done{0};
  std::vector<std::thread> workers;
  for (uint64_t worker = 0; worker < 4; ++worker) {
    workers.emplace_back([&, worker] {
      while (!start.load()) std::this_thread::yield();
      for (uint64_t id = worker + 1; id <= 64; id += 4) {
        for (unsigned read = 0; read < 8; ++read) {
          if (SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(id), id,
                                                       config, clock) !=
              ReadResult::kApplied) {
            failed.store(true);
          }
        }
        if (id == worker + 1) {
          while (!first_snapshot.load()) std::this_thread::yield();
        }
      }
      ++done;
    });
  }
  start.store(true);
  bool snapshots_valid = true;
  size_t snapshots = 0;
  do {
    const Observation observation = allocator.ObserveLifetimeReadClasses(clock);
    ++snapshots;
    if (!observation.available ||
        std::accumulate(observation.resident_estimates.begin(),
                        observation.resident_estimates.end(),
                        uint64_t{0}) != 64 ||
        observation.resident_estimates[2] != 0 ||
        observation.resident_estimates[3] != 0 ||
        observation.resident_estimates[4] != 0) {
      snapshots_valid = false;
    }
    // Workers cannot finish their remaining keys before this actual snapshot.
    first_snapshot.store(true);
  } while (done.load() != 4);
  for (std::thread& worker : workers) worker.join();
  EXPECT_FALSE(failed.load());
  EXPECT_GT(snapshots, 0U);
  EXPECT_TRUE(snapshots_valid);
  ExpectPopulation(allocator, clock, {0, 64, 0, 0, 0});
}

}  // namespace
}  // namespace dfkv
