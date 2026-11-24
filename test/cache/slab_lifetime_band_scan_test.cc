#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
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
using Path = SlabAllocator::LifetimeScanPath;
using BandStat = SlabAllocator::LifetimeBandScanStat;
using Scan = SlabAllocatorTestPeer::LifetimeScan;
using ScanResult = SlabAllocatorTestPeer::LifetimeScanResult;

BlockKey Key(uint64_t id) { return {id, 0, 7}; }

SlabAllocator::Options Options(uint32_t slots, uint32_t max_reads = 2) {
  SlabAllocator::Options options;
  options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
  options.num_extents = 1;
  options.track_lifetime_state = true;
  options.lifetime_max_reads = max_reads;
  return options;
}

void Seed(SlabAllocator& allocator, uint64_t id, uint8_t reads,
          bool pin = false, uint32_t anchor = 0) {
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(id), 1, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  state.anchor = anchor;
  state.hstate = static_cast<uint8_t>(reads << 2);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state);
  if (pin) {
    ASSERT_TRUE(allocator.Pin(Key(id)));
  }
}

// Build reachable NEW/read states for candidate-order diagnostics. Unlike
// recovery, an unread NEW resident retains wa and its original put timestamp.
void SeedReachable(SlabAllocator& allocator, uint64_t id, uint8_t reads,
                   bool pin = false, uint32_t anchor = 0) {
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(id), 1, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  DiskLifetimeState state = NewDiskLifetimeState(reads == 0 ? anchor : 0);
  const DiskLifetimeConfig config;
  for (uint8_t read = 0; read < reads; ++read) {
    ASSERT_TRUE(ApplyDiskLifetimeRead(config, anchor, &state));
  }
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state);
  if (pin) {
    ASSERT_TRUE(allocator.Pin(Key(id)));
  }
}

void ExpectCounts(const std::vector<BandStat>& stats, uint32_t max_reads,
                  Path path, uint32_t band, uint64_t visits,
                  uint64_t candidates, uint64_t pin_skips) {
  const size_t index = static_cast<size_t>(path) * (max_reads + 1) + band;
  ASSERT_LT(index, stats.size());
  const BandStat& stat = stats[index];
  EXPECT_EQ(stat.path, path);
  EXPECT_EQ(stat.band, band);
  EXPECT_EQ(stat.visits, visits);
  EXPECT_EQ(stat.candidates, candidates);
  EXPECT_EQ(stat.pin_skips, pin_skips);
}

void ExpectZero(const std::vector<BandStat>& stats, uint32_t max_reads) {
  ASSERT_EQ(stats.size(), 3 * (max_reads + 1));
  for (const Path path : {Path::kClass, Path::kBackground, Path::kWatermark}) {
    for (uint32_t band = 0; band <= max_reads; ++band) {
      ASSERT_NO_FATAL_FAILURE(
          ExpectCounts(stats, max_reads, path, band, 0, 0, 0));
    }
  }
}

void SetTickBudget(Scan& scan, uint64_t victims) {
  scan.initialized = true;
  scan.visit_limit = 4 * victims;
  scan.slice_limit = scan.visit_limit + 1;
}

TEST(SlabLifetimeBandScan,
     SupportedCapsIncludeHighestBandAndRetainEmptyHistory) {
  for (const uint32_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    const SlabAllocator::Options options = Options(1, max_reads);
    SlabAllocator allocator(options);
    ASSERT_NO_FATAL_FAILURE(
        ExpectZero(allocator.LifetimeBandScanStats(), max_reads));
    ASSERT_NO_FATAL_FAILURE(
        Seed(allocator, 1, static_cast<uint8_t>(max_reads)));
    DiskLifetimeConfig config;
    config.max_reads = max_reads;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [] { return 0; });
    std::vector<BlockKey> evicted;
    ASSERT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                  allocator, 0, 1, config, clock, &evicted),
              1U);
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    EXPECT_EQ(allocator.Count(), 0U);
    const std::vector<BandStat> stats = allocator.LifetimeBandScanStats();
    ASSERT_EQ(stats.size(), 3 * (max_reads + 1));
    for (const BandStat& stat : stats) {
      const bool checked =
          stat.path == Path::kWatermark && stat.band == max_reads;
      EXPECT_EQ(stat.visits, checked ? 1U : 0U);
      EXPECT_EQ(stat.candidates, checked ? 1U : 0U);
      EXPECT_EQ(stat.pin_skips, 0U);
    }
    // A later empty tick does not erase history or manufacture opportunities.
    EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                  allocator, 0, 1, config, clock, &evicted),
              0U);
    EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(),
                                         max_reads, Path::kWatermark, max_reads,
                                         1, 1, 0));
    // A new owner starts a new counter cohort even with the same geometry.
    const SlabAllocator reopened(options);
    EXPECT_NO_FATAL_FAILURE(
        ExpectZero(reopened.LifetimeBandScanStats(), max_reads));
  }
}

TEST(SlabLifetimeBandScan, ForegroundAndBackgroundChecksAreDisjoint) {
  SlabAllocator allocator(Options(4));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0, true));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, 1));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  bool denied = false;
  Scan foreground(config, clock);
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, foreground,
                                                       &evicted, &denied));
  EXPECT_FALSE(denied);
  EXPECT_EQ(foreground.visits, 2U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2)});
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 3, 2));
  Scan background(config, clock);
  SetTickBudget(background, 1);
  const SlabAllocatorTestPeer::LifetimeReclaimResult result =
      SlabAllocatorTestPeer::ReclaimLifetimeClass(allocator, 0, 3, 1,
                                                  background, &evicted);
  EXPECT_EQ(result.victims, 1U);
  EXPECT_EQ(background.visits, 2U);
  EXPECT_EQ(evicted, (std::vector<BlockKey>{Key(2), Key(3)}));
  const std::vector<BandStat> stats = allocator.LifetimeBandScanStats();
  ASSERT_EQ(stats.size(), 9U);
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, Path::kClass, 0, 1, 0, 1));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, Path::kClass, 1, 1, 1, 0));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, Path::kClass, 2, 0, 0, 0));
  EXPECT_NO_FATAL_FAILURE(
      ExpectCounts(stats, 2, Path::kBackground, 0, 1, 0, 1));
  EXPECT_NO_FATAL_FAILURE(
      ExpectCounts(stats, 2, Path::kBackground, 1, 0, 0, 0));
  EXPECT_NO_FATAL_FAILURE(
      ExpectCounts(stats, 2, Path::kBackground, 2, 1, 1, 0));
  for (uint32_t band = 0; band <= 2; ++band) {
    EXPECT_NO_FATAL_FAILURE(
        ExpectCounts(stats, 2, Path::kWatermark, band, 0, 0, 0));
  }
  EXPECT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kDeferred);
  ASSERT_TRUE(allocator.Unpin(Key(1)));
  EXPECT_EQ(allocator.Count(), 0U);
  // Deleting the last class resident must not remove public counter series.
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kBackground, 2, 1, 1, 0));
}

TEST(SlabLifetimeBandScan,
     TinyTicksGivePinnedBandsChecksAndContinueTheirCursor) {
  SlabAllocator allocator(Options(19, 1));
  for (uint64_t id = 1; id <= 17; ++id) {
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, 0, true));
  }
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 18, 0));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 19, 1, true));
  DiskLifetimeConfig config;
  config.max_reads = 1;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  for (uint64_t tick = 1; tick <= 6; ++tick) {
    ASSERT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                  allocator, 0, 1, config, clock, &evicted),
              0U);
    const std::vector<BandStat> stats = allocator.LifetimeBandScanStats();
    EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 1, Path::kWatermark, 0,
                                         3 * tick, tick == 6 ? 1 : 0,
                                         tick == 6 ? 17 : 3 * tick));
    EXPECT_NO_FATAL_FAILURE(
        ExpectCounts(stats, 1, Path::kWatermark, 1, tick, 0, tick));
  }
  // The candidate on the sixth tick's final paid visit is checked again;
  // it was not a successful victim until this seventh tick.
  ASSERT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 1, config, clock, &evicted),
            1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(18)});
  const std::vector<BandStat> stats = allocator.LifetimeBandScanStats();
  EXPECT_NO_FATAL_FAILURE(
      ExpectCounts(stats, 1, Path::kWatermark, 0, 19, 2, 17));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 1, Path::kWatermark, 1, 7, 0, 7));
  EXPECT_EQ(allocator.WatermarkScanVisits(), 26U);
  EXPECT_EQ(allocator.WatermarkEvictions(), 1U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBandScan, CachedCandidatesPayAgainBeforeEveryWinner) {
  SlabAllocator allocator(Options(4));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0, false, 302401));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 100, 0, false, 302500));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, 1, false, 2));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 3, 1, false, 3));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 907200;
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 3, config, clock, &evicted),
            3U);
  EXPECT_EQ(evicted, (std::vector<BlockKey>{Key(1), Key(2), Key(3)}));
  const std::vector<BandStat> stats = allocator.LifetimeBandScanStats();
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, Path::kWatermark, 0, 3, 3, 0));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, Path::kWatermark, 1, 3, 3, 0));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, Path::kWatermark, 2, 0, 0, 0));
  EXPECT_EQ(allocator.WatermarkScanVisits(), 6U);
  EXPECT_EQ(allocator.Remove(Key(100)), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kWatermark, 0, 3, 3, 0));
}

// These fixtures diagnose bounded selection, not a global-minimum contract.
// Existing test-peer cursors expose the checked range without production hooks.
TEST(SlabLifetimeBandScan, NonHeadCursorCanSkipALowerScorePrefix) {
  SlabAllocator allocator(Options(3));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 1, 0, false, 0));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 2, 0, false, 400000));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 3, 1, false, 20000));
  ASSERT_TRUE(SlabAllocatorTestPeer::PointLifetimeCursorsAt(allocator, Key(2)));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 500000;
  ASSERT_LT(DiskLifetimeScore(config, 0, 0, false, true, seconds),
            DiskLifetimeScore(config, 20000, 1, false, false, seconds));
  ASSERT_LT(DiskLifetimeScore(config, 20000, 1, false, false, seconds),
            DiskLifetimeScore(config, 400000, 0, false, true, seconds));
  std::vector<BlockKey> evicted;
  bool denied = false;
  Scan scan(config, clock);
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_FALSE(denied);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(3)});
  EXPECT_EQ(scan.visits, 2U);
  // Key 1 is colder than the victim but was outside this checked range.
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, 1, 0),
            (std::vector<BlockKey>{Key(1), Key(2)}));
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 1, 0), Key(1));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kClass, 0, 1, 1, 0));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kClass, 1, 1, 1, 0));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBandScan, ContinuousAppendCanDelayOldPrefixRevisit) {
  SlabAllocator allocator(Options(32));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 1, 0, false, 0));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 2, 0, false, 400000));
  ASSERT_TRUE(SlabAllocatorTestPeer::PointLifetimeCursorsAt(allocator, Key(2)));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 500000;
  std::vector<BlockKey> evicted;
  bool denied = false;
  for (uint64_t round = 0; round < 8; ++round) {
    SCOPED_TRACE(round);
    ASSERT_NO_FATAL_FAILURE(
        SeedReachable(allocator, 100 + round, 0, false, 400001 + round));
    ASSERT_NO_FATAL_FAILURE(
        SeedReachable(allocator, 200 + round, 1, false, 20000 + round));
    Scan scan(config, clock);
    ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                         &evicted, &denied));
    EXPECT_FALSE(denied);
    ASSERT_EQ(evicted.size(), round + 1);
    EXPECT_EQ(evicted.back(), Key(200 + round));
    EXPECT_EQ(scan.visits, 2U);
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 1, 0),
              Key(100 + round));
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeState(allocator, Key(1)));
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
  // Every losing unread candidate advanced the cursor. Append did not reset it;
  // eight old-prefix bypasses required neither pins nor a visit-budget cutoff.
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kClass, 0, 8, 8, 0));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kClass, 1, 8, 8, 0));
  // Once appends stop, crossing the tail permits the old prefix to return.
  Scan tail(config, clock);
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, tail,
                                                       &evicted, &denied));
  EXPECT_EQ(evicted.back(), Key(107));
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 1, 0), Key(1));
  Scan prefix(config, clock);
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, prefix,
                                                       &evicted, &denied));
  EXPECT_EQ(evicted.back(), Key(1));
  EXPECT_EQ(tail.visits, 1U);
  EXPECT_EQ(prefix.visits, 1U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

// Diagnose the current selectors, not a new fairness or global-minimum
// contract. All paths start naturally at the oldest NEW and use the same
// reachable states.
void ExpectCurrentCandidateBehaviorAcrossAppends(Path path) {
  SlabAllocator allocator(Options(32));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 1, 0));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  std::vector<BlockKey> evicted;
  const auto select_one = [&] {
    const uint64_t before = allocator.WatermarkScanVisits();
    if (path == Path::kWatermark) {
      ASSERT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                    allocator, 0, 1, config, clock, &evicted),
                1U);
      EXPECT_EQ(allocator.WatermarkScanVisits() - before, 2U);
      return;
    }
    Scan scan(config, clock);
    if (path == Path::kBackground) {
      SetTickBudget(scan, 1);
      const size_t target = allocator.Classes()[0].free_slots + 1;
      const SlabAllocatorTestPeer::LifetimeReclaimResult result =
          SlabAllocatorTestPeer::ReclaimLifetimeClass(allocator, 0, target, 1,
                                                      scan, &evicted);
      ASSERT_EQ(result.victims, 1U);
    } else {
      bool denied = false;
      ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                           &evicted, &denied));
      EXPECT_FALSE(denied);
    }
    EXPECT_EQ(scan.visits, 2U);
  };
  for (uint64_t round = 0; round < 6; ++round) {
    SCOPED_TRACE(round);
    seconds = 10 + round;
    ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 100 + round, 0, false,
                                          static_cast<uint32_t>(seconds)));
    ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 200 + round, 2, false,
                                          static_cast<uint32_t>(seconds)));
    ASSERT_NO_FATAL_FAILURE(select_one());
    ASSERT_EQ(evicted.size(), round + 1);
    EXPECT_EQ(evicted.back(), Key(200 + round));
    EXPECT_TRUE(allocator.Contains(Key(1)));
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
  // At P the oldest NEW loses its BOOST. Class/background have advanced past
  // it, while watermark has retained its losing candidate across selections.
  // This records a path difference; it does not label either behavior a new
  // spec.
  seconds = config.protect_sec;
  ASSERT_NO_FATAL_FAILURE(
      SeedReachable(allocator, 199, 0, false, static_cast<uint32_t>(seconds)));
  ASSERT_NO_FATAL_FAILURE(
      SeedReachable(allocator, 299, 2, false, static_cast<uint32_t>(seconds)));
  ASSERT_LT(DiskLifetimeScore(config, 0, 0, false, true, seconds),
            DiskLifetimeScore(config, static_cast<uint32_t>(seconds), 2, false,
                              false, seconds));
  ASSERT_NO_FATAL_FAILURE(select_one());
  ASSERT_EQ(evicted.size(), 7U);
  const bool retained = path == Path::kWatermark;
  EXPECT_EQ(evicted.back(), retained ? Key(1) : Key(299));
  EXPECT_EQ(allocator.Contains(Key(1)), !retained);
  EXPECT_EQ(allocator.Contains(Key(299)), retained);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_NO_FATAL_FAILURE(
      ExpectCounts(allocator.LifetimeBandScanStats(), 2, path, 0, 7, 7, 0));
  EXPECT_NO_FATAL_FAILURE(
      ExpectCounts(allocator.LifetimeBandScanStats(), 2, path, 2, 7, 7, 0));
}

TEST(SlabLifetimeBandScan,
     ClassAdvancesPastLosingCandidateAcrossContinuousAppends) {
  ASSERT_NO_FATAL_FAILURE(
      ExpectCurrentCandidateBehaviorAcrossAppends(Path::kClass));
}

TEST(SlabLifetimeBandScan,
     BackgroundAdvancesPastLosingCandidateAcrossContinuousAppends) {
  ASSERT_NO_FATAL_FAILURE(
      ExpectCurrentCandidateBehaviorAcrossAppends(Path::kBackground));
}

TEST(SlabLifetimeBandScan,
     WatermarkRetainsLosingCandidateAcrossContinuousAppends) {
  ASSERT_NO_FATAL_FAILURE(
      ExpectCurrentCandidateBehaviorAcrossAppends(Path::kWatermark));
}

TEST(SlabLifetimeBandScan,
     WatermarkRetainedCandidateIsRecheckedAfterPinOrRemove) {
  for (const bool remove : {false, true}) {
    SCOPED_TRACE(remove);
    SlabAllocator allocator(Options(4));
    ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 1, 0));
    ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 2, 0, false, 1));
    ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 3, 2, false, 10));
    const DiskLifetimeConfig config;
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&seconds] { return seconds; });
    seconds = 10;
    std::vector<BlockKey> evicted;
    ASSERT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                  allocator, 0, 1, config, clock, &evicted),
              1U);
    ASSERT_EQ(evicted, std::vector<BlockKey>{Key(3)});
    ASSERT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 0, 0),
              Key(1));
    if (remove) {
      ASSERT_EQ(allocator.Remove(Key(1)),
                SlabAllocator::RemoveResult::kRemoved);
    } else {
      ASSERT_TRUE(allocator.Pin(Key(1)));
    }
    seconds = config.protect_sec + 1;
    const uint64_t before = allocator.WatermarkScanVisits();
    ASSERT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                  allocator, 0, 1, config, clock, &evicted),
              1U);
    EXPECT_EQ(evicted, (std::vector<BlockKey>{Key(3), Key(2)}));
    EXPECT_EQ(allocator.WatermarkScanVisits() - before, remove ? 1U : 2U);
    EXPECT_EQ(allocator.Contains(Key(1)), !remove);
    if (!remove) {
      ASSERT_TRUE(allocator.Unpin(Key(1)));
    }
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

TEST(SlabLifetimeBandScan, CappedReadRelinksAndRepairsBothCursors) {
  SlabAllocator allocator(Options(3));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 1, 63, false, 10));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 2, 63, false, 20));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 3, 63, false, 30));
  ASSERT_TRUE(SlabAllocatorTestPeer::PointLifetimeCursorsAt(allocator, Key(1)));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 40;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  for (const size_t row : {0U, 1U}) {
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, row, 2),
              (std::vector<BlockKey>{Key(2), Key(3), Key(1)}));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeCursorKey(allocator, row, 2),
              Key(2));
  }
  const std::optional<DiskLifetimeState> refreshed =
      SlabAllocatorTestPeer::LifetimeState(allocator, Key(1));
  ASSERT_TRUE(refreshed.has_value());
  EXPECT_EQ(refreshed->scoring_reads(), 63U);
  EXPECT_EQ(refreshed->anchor, 40U);
  std::vector<BlockKey> evicted;
  bool denied = false;
  Scan scan(config, clock);
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2)});
  EXPECT_FALSE(denied);
  EXPECT_EQ(scan.visits, 1U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBandScan, PinnedPrefixPaysChecksBeforeLegalCandidate) {
  SlabAllocator allocator(Options(3));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 1, 0, true, 0));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 2, 0, false, 1));
  ASSERT_NO_FATAL_FAILURE(SeedReachable(allocator, 3, 1, false, 20000));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 500000;
  std::vector<BlockKey> evicted;
  bool denied = false;
  Scan scan(config, clock);
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_FALSE(denied);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2)});
  EXPECT_EQ(scan.visits, 3U);
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kClass, 0, 2, 1, 1));
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kClass, 1, 1, 1, 0));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeState(allocator, Key(1)));
  ASSERT_TRUE(allocator.Unpin(Key(1)));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBandScan, PersistenceFailureRetainsChecksButNotVictims) {
  for (const Path path : {Path::kClass, Path::kBackground, Path::kWatermark}) {
    SCOPED_TRACE(static_cast<unsigned int>(path));
    uint64_t attempts = 0;
    SlabAllocator::Options options = Options(2);
    options.on_slot_evict = [&attempts](const SlabAllocator::SlotRef&) {
      ++attempts;
      return false;
    };
    SlabAllocator allocator(options);
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0, true));
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, 1));
    const DiskLifetimeConfig config;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [] { return 0; });
    std::vector<BlockKey> evicted;
    Scan scan(config, clock);
    if (path == Path::kClass) {
      bool denied = false;
      EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                            &evicted, &denied));
      EXPECT_FALSE(denied);
      EXPECT_EQ(scan.result, ScanResult::kPersistenceError);
      EXPECT_EQ(scan.visits, 2U);
    } else if (path == Path::kBackground) {
      SetTickBudget(scan, 1);
      EXPECT_EQ(SlabAllocatorTestPeer::ReclaimLifetimeClass(allocator, 0, 1, 1,
                                                            scan, &evicted)
                    .victims,
                0U);
      EXPECT_EQ(scan.result, ScanResult::kPersistenceError);
      EXPECT_EQ(scan.visits, 2U);
    } else {
      EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                    allocator, 0, 1, config, clock, &evicted),
                0U);
      EXPECT_EQ(allocator.WatermarkEvictions(), 0U);
    }
    EXPECT_EQ(attempts, 1U);
    EXPECT_TRUE(evicted.empty());
    EXPECT_EQ(allocator.Count(), 2U);
    const std::vector<BandStat> stats = allocator.LifetimeBandScanStats();
    ASSERT_EQ(stats.size(), 9U);
    for (const Path observed :
         {Path::kClass, Path::kBackground, Path::kWatermark}) {
      EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, observed, 0,
                                           observed == path ? 1 : 0, 0,
                                           observed == path ? 1 : 0));
      EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, observed, 1,
                                           observed == path ? 1 : 0,
                                           observed == path ? 1 : 0, 0));
      EXPECT_NO_FATAL_FAILURE(ExpectCounts(stats, 2, observed, 2, 0, 0, 0));
    }
  }
}

TEST(SlabLifetimeBandScan, EmptyZeroBudgetAndClockFailureDoNotInventChecks) {
  SlabAllocator allocator(Options(2));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  std::vector<BlockKey> evicted;
  Scan empty(config, clock);
  bool denied = false;
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, empty,
                                                        &evicted, &denied));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0));
  EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 0, config, clock, &evicted),
            0U);
  Scan background(config, clock);
  SetTickBudget(background, 1);
  EXPECT_EQ(SlabAllocatorTestPeer::ReclaimLifetimeClass(allocator, 0, 1, 0,
                                                        background, &evicted)
                .victims,
            0U);
  seconds = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
  Scan exhausted(config, clock);
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, exhausted,
                                                        &evicted, &denied));
  EXPECT_TRUE(denied);
  EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 1, config, clock, &evicted),
            0U);
  EXPECT_NO_FATAL_FAILURE(ExpectZero(allocator.LifetimeBandScanStats(), 2));
}

TEST(SlabLifetimeBandScan, LegacyHasNoBandLedger) {
  SlabAllocator::Options options = Options(2);
  options.track_lifetime_state = false;
  SlabAllocator allocator(options);
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(1), 1, &ref, &evicted));
  EXPECT_TRUE(allocator.LifetimeBandScanStats().empty());
  EXPECT_EQ(allocator.EvictColdToTarget(0, 1, &evicted), 1U);
  EXPECT_TRUE(allocator.LifetimeBandScanStats().empty());
}

TEST(SlabLifetimeBandScan, ConcurrentSnapshotsKeepEachCheckedPinConsistent) {
  SlabAllocator allocator(Options(8));
  for (uint64_t id = 1; id <= 8; ++id) {
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, 0, true));
  }
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::mutex mutex;
  std::condition_variable condition;
  bool first_tick_done = false;
  bool snapshot_taken = false;
  std::atomic<bool> done{false};
  std::thread worker([&] {
    for (size_t tick = 0; tick < 32; ++tick) {
      EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                    allocator, 0, 1, config, clock, nullptr),
                0U);
      if (tick == 0) {
        std::unique_lock<std::mutex> lock(mutex);
        first_tick_done = true;
        condition.notify_all();
        EXPECT_TRUE(condition.wait_for(lock, std::chrono::seconds(5),
                                       [&] { return snapshot_taken; }));
      }
    }
    done.store(true, std::memory_order_release);
  });
  bool observed_first_tick = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    observed_first_tick = condition.wait_for(lock, std::chrono::seconds(5),
                                             [&] { return first_tick_done; });
  }
  // Positive handshake ensures at least one snapshot of actual scan work.
  // Always release/join the worker before any fatal assertion.
  if (observed_first_tick) {
    const std::vector<BandStat> first = allocator.LifetimeBandScanStats();
    EXPECT_NO_FATAL_FAILURE(
        ExpectCounts(first, 2, Path::kWatermark, 0, 4, 0, 4));
  }
  {
    const std::lock_guard<std::mutex> lock(mutex);
    snapshot_taken = true;
  }
  condition.notify_all();
  uint64_t previous_visits = 4;
  while (!done.load(std::memory_order_acquire)) {
    const std::vector<BandStat> stats = allocator.LifetimeBandScanStats();
    for (const BandStat& stat : stats) {
      if (stat.path == Path::kWatermark && stat.band == 0) {
        EXPECT_GE(stat.visits, previous_visits);
        EXPECT_EQ(stat.visits, stat.pin_skips);
        EXPECT_EQ(stat.candidates, 0U);
        previous_visits = stat.visits;
      } else {
        EXPECT_EQ(stat.visits, 0U);
        EXPECT_EQ(stat.candidates, 0U);
        EXPECT_EQ(stat.pin_skips, 0U);
      }
    }
  }
  worker.join();
  ASSERT_TRUE(observed_first_tick);
  EXPECT_NO_FATAL_FAILURE(ExpectCounts(allocator.LifetimeBandScanStats(), 2,
                                       Path::kWatermark, 0, 128, 0, 128));
  EXPECT_EQ(allocator.WatermarkScanVisits(), 128U);
}

}  // namespace
}  // namespace dfkv
