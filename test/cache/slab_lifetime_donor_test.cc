#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/disk_slab_store.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static bool Bind(DiskSlabStore& store, uint32_t size, uint32_t extent) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::BindExtent(
               *store.alloc_,
               SlabAllocatorTestPeer::ClassIndex(*store.alloc_, size),
               extent) == SlabAllocatorTestPeer::ExtentBindResult::kBound;
  }
  static void Quiesce(DiskSlabStore& store) {
    const std::vector<SlabAllocator::ClassStat> classes =
        store.alloc_->Classes();
    store.reclaim_last_puts_.resize(classes.size());
    for (size_t cls = 0; cls < classes.size(); ++cls)
      store.reclaim_last_puts_[cls] = classes[cls].puts;
  }
  static void Tick(DiskSlabStore& store) { store.ReclaimTick(); }
  static void FailMetadata(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) ::close(store.table_fd_);
    store.table_fd_ = -1;
  }
  static uint64_t Estimate(DiskSlabStore& store, uint32_t extent) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::ProtectedEstimate(*store.alloc_, extent, 0);
  }
  static bool Valid(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeBucketsValid(*store.alloc_) &&
           SlabAllocatorTestPeer::LifetimeIndexesValid(*store.alloc_);
  }
};
namespace {
using Result = SlabAllocatorTestPeer::ExtentBindResult;

BlockKey Key(uint64_t id) { return {id, 0, 7}; }

DiskLifetimeConfig Config() {
  DiskLifetimeConfig config;
  config.base_sec = 100;
  config.read_bonus_sec = 10;
  config.protect_sec = 8;
  config.slowdown = 4;
  config.keep_score_sec = 90;
  config.bucket_sec = 16;
  config.bucket_count = 8;
  config.high_pct = 0;
  std::string error;
  EXPECT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  return config;
}

SlabAllocator::Options Options(const DiskLifetimeConfig& config,
                               const DiskLifetimeClock& clock,
                               uint32_t slots = 4, uint32_t extents = 2) {
  SlabAllocator::Options options;
  options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
  options.num_extents = extents;
  options.track_lifetime_state = true;
  options.lifetime_max_reads = static_cast<uint32_t>(config.max_reads);
  options.lifetime_bucket_sec = config.bucket_sec;
  options.lifetime_bucket_count = static_cast<uint32_t>(config.bucket_count);
  options.lifetime_donor_config = &config;
  options.lifetime_donor_clock = &clock;
  return options;
}

void Restore(SlabAllocator& allocator,
             const std::vector<SlabAllocator::RestoreEntry>& entries,
             const DiskLifetimeConfig& config, uint32_t now,
             const std::vector<uint32_t>& anchors = {}) {
  ASSERT_TRUE(allocator.RestoreBulk(entries));
  ASSERT_TRUE(anchors.empty() || anchors.size() == entries.size());
  for (size_t index = 0; index < entries.size(); ++index) {
    DiskLifetimeState state = RecoveredDiskLifetimeState(1);
    state.anchor = anchors.empty() ? 0 : anchors[index];
    SlabAllocatorTestPeer::CommitGeneration(&allocator, entries[index].key,
                                            index + 1, &state, &config, now);
  }
}

DiskLifetimeConfig ScoreConfig(uint64_t base) {
  DiskLifetimeConfig config;
  config.base_sec = base;
  config.read_bonus_sec = 0;
  config.max_reads = 1;
  config.boost_sec = 1;
  config.keep_score_sec = base - 1;
  config.protect_sec = config.slowdown = config.bucket_sec = 1;
  config.bucket_count = 2;
  config.high_pct = 0;
  std::string error;
  EXPECT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  return config;
}

void ExpectEmptyScore(const SlabAllocator::LifetimeDonorScoreStats& score) {
  EXPECT_EQ(score.nonnegative_buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(score.negative_magnitude_buckets, (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(score.nonnegative_total_sec, 0);
  EXPECT_DOUBLE_EQ(score.negative_magnitude_total_sec, 0);
}

void ExpectValid(const SlabAllocator& allocator) {
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeBucketsValid(allocator));
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  uint64_t known = 0;
  for (const uint64_t count : stats.age.buckets) known += count;
  EXPECT_EQ(known + stats.age.history_unknown + stats.unscored_victims,
            stats.released_residents);
  uint64_t scored = 0;
  for (const uint64_t count : stats.score.nonnegative_buckets) scored += count;
  for (const uint64_t count : stats.score.negative_magnitude_buckets)
    scored += count;
  EXPECT_EQ(scored + stats.unscored_victims, stats.released_residents);
}

TEST(SlabLifetimeDonor, ScoreBinsSeparateSignsIncludingZero) {
  struct Case {
    uint32_t magnitude;
    size_t bucket;
  };
  const std::array<Case, 13> cases = {{{0, 0},
                                       {1, 1},
                                       {2, 2},
                                       {60, 2},
                                       {61, 3},
                                       {3600, 3},
                                       {3601, 4},
                                       {86400, 4},
                                       {86401, 5},
                                       {604800, 5},
                                       {604801, 6},
                                       {2592000, 6},
                                       {2592001, 7}}};
  for (unsigned sign = 0; sign < 2; ++sign) {
    const bool negative = sign != 0;
    for (const Case& test : cases) {
      if (negative && test.magnitude == 0) continue;
      SCOPED_TRACE(negative);
      SCOPED_TRACE(test.magnitude);
      const DiskLifetimeConfig config = ScoreConfig(3000000);
      uint64_t seconds = 0;
      const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                    [&] { return seconds; });
      SlabAllocator::Options options = Options(config, clock, 4, 1);
      unsigned wipes = 0;
      options.on_extent_bind = [&](uint32_t) {
        ++wipes;
        return true;
      };
      SlabAllocator allocator(options);
      ASSERT_NO_FATAL_FAILURE(
          Restore(allocator, {{Key(1), 4096, 0, 0, 1}}, config, 0));
      seconds = negative ? config.base_sec + test.magnitude
                         : config.base_sec - test.magnitude;
      std::vector<BlockKey> evicted;
      ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
                Result::kBound);
      EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
      EXPECT_EQ(wipes, 1U);
      const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
      for (size_t bucket = 0; bucket < 8; ++bucket) {
        EXPECT_EQ(stats.score.nonnegative_buckets[bucket],
                  !negative && bucket == test.bucket ? 1U : 0U);
        EXPECT_EQ(stats.score.negative_magnitude_buckets[bucket],
                  negative && bucket == test.bucket ? 1U : 0U);
      }
      EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec,
                       negative ? 0 : test.magnitude);
      EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec,
                       negative ? test.magnitude : 0);
      EXPECT_EQ(stats.age.history_unknown, 1U);
      ExpectValid(allocator);
    }
  }
}

TEST(SlabLifetimeDonor, ScoreUsesRecheckedPreWipeTime) {
  const DiskLifetimeConfig config = ScoreConfig(100);
  uint64_t seconds = 0;
  bool advance = false;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    const uint64_t sampled = seconds;
    if (advance) seconds = 20;
    return sampled;
  });
  SlabAllocator::Options options = Options(config, clock, 4, 1);
  options.on_extent_bind = [&](uint32_t) {
    seconds = 1000;
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(
      Restore(allocator, {{Key(1), 4096, 0, 0, 1}}, config, 0));
  seconds = 19;
  advance = true;  // Selection score81; final pre-wipe score80; post-I/O -900.
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.score.nonnegative_buckets[3], 1U);
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, 80);
  EXPECT_EQ(stats.score.negative_magnitude_buckets,
            (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec, 0);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, ScoreSumDoesNotWrapForValidNearInt64Maximum) {
  const uint64_t maximum = std::numeric_limits<int64_t>::max();
  const DiskLifetimeConfig config = ScoreConfig(maximum - 1);
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  SlabAllocator allocator(Options(config, clock, 4, 1));
  ASSERT_TRUE(allocator.RestoreBulk({{Key(1), 4096, 0, 0, 1},
                                     {Key(2), 4096, 0, 1, 1},
                                     {Key(3), 4096, 0, 2, 1}}));
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  for (uint64_t id = 1; id <= 3; ++id) {
    SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state,
                                            &config, 0);
  }
  ASSERT_EQ(DiskLifetimeScore(config, 0, 0, false, true, 0),
            std::numeric_limits<int64_t>::max());
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(evicted.size(), 3U);
  EXPECT_EQ(stats.score.nonnegative_buckets[7], 3U);
  EXPECT_TRUE(std::isfinite(stats.score.nonnegative_total_sec));
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec,
                   3.0 * static_cast<double>(maximum));
  EXPECT_GT(stats.score.nonnegative_total_sec,
            static_cast<double>(std::numeric_limits<uint64_t>::max()));
  EXPECT_EQ(stats.score.negative_magnitude_buckets,
            (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec, 0);
  EXPECT_EQ(stats.protected_victims, 3U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, ScoreCountsLargestClockReachableDeficit) {
  const DiskLifetimeConfig config = ScoreConfig(1);
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 4, 1));
  ASSERT_NO_FATAL_FAILURE(
      Restore(allocator, {{Key(1), 4096, 0, 0, 1}}, config, 0));
  // The admitted-work clock can drain through UINT32_MAX, not UINT64_MAX.
  // INT64_MIN is only a pure-math defensive case, not a runtime donor score.
  seconds = UINT32_MAX;
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.score.negative_magnitude_buckets[7], 1U);
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec,
                   static_cast<double>(UINT32_MAX) - 1);
  EXPECT_EQ(stats.score.nonnegative_buckets, (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, 0);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, AgeBinsUsePreWipeTime) {
  struct Case {
    uint32_t age;
    size_t bucket;
  };
  const std::array<Case, 13> cases = {{{0, 0},
                                       {1, 1},
                                       {2, 2},
                                       {60, 2},
                                       {61, 3},
                                       {3600, 3},
                                       {3601, 4},
                                       {86400, 4},
                                       {86401, 5},
                                       {604800, 5},
                                       {604801, 6},
                                       {2592000, 6},
                                       {2592001, 7}}};
  for (const Case& test : cases) {
    SCOPED_TRACE(test.age);
    const DiskLifetimeConfig config = Config();
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&] { return seconds; });
    SlabAllocator::Options options = Options(config, clock, 4, 1);
    unsigned wipes = 0;
    options.on_extent_bind = [&](uint32_t) {
      ++wipes;
      seconds += 100;  // Persistence time must not change the age snapshot.
      return true;
    };
    SlabAllocator allocator(options);
    ASSERT_TRUE(allocator.RestoreBulk({{Key(1), 4096, 0, 0, 1}}));
    const DiskLifetimeState state = NewDiskLifetimeState(0);
    SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1, &state,
                                            &config, 0);
    seconds = test.age;
    std::vector<BlockKey> evicted;
    ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
              Result::kBound);
    ASSERT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    EXPECT_EQ(wipes, 1U);
    const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
    for (size_t bucket = 0; bucket < 8; ++bucket) {
      EXPECT_EQ(stats.age.buckets[bucket], bucket == test.bucket ? 1U : 0U);
    }
    EXPECT_EQ(stats.age.total_sec, test.age);
    EXPECT_EQ(stats.age.history_unknown, 0U);
    EXPECT_EQ(stats.unscored_victims, 0U);
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeDonor, AgeUsesRecheckedPreWipeTime) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  bool advance = false;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    const uint64_t sampled = seconds;
    if (advance) seconds = 20;
    return sampled;
  });
  SlabAllocator::Options options = Options(config, clock, 4, 1);
  options.on_extent_bind = [&](uint32_t) {
    seconds = 1000;
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_TRUE(allocator.RestoreBulk({{Key(1), 4096, 0, 0, 1}}));
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1, &state,
                                          &config, 0);
  seconds = 19;
  advance = true;  // Selection sees 19; the final pre-wipe recheck sees 20.
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.age.buckets[2], 1U);
  EXPECT_EQ(stats.age.total_sec, 20U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  EXPECT_EQ(stats.unscored_victims, 0U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, AgeSaturatesFutureAnchorAtZero) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 4, 1));
  ASSERT_TRUE(allocator.RestoreBulk({{Key(1), 4096, 0, 0, 1}}));
  const DiskLifetimeState state = NewDiskLifetimeState(20);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1, &state,
                                          &config, 20);
  seconds = 10;  // Defensive state injection; a normal clock never regresses.
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.age.buckets[0], 1U);
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  EXPECT_EQ(stats.unscored_victims, 0U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, AgeSeparatesUnknownAndUninitializedHistory) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 8, 1));
  ASSERT_TRUE(allocator.RestoreBulk({{Key(1), 4096, 0, 0, 1},
                                     {Key(2), 4096, 0, 1, 1},
                                     {Key(3), 4096, 0, 2, 1},
                                     {Key(4), 4096, 0, 3, 1},
                                     {Key(5), 4096, 0, 4, 1}}));
  const DiskLifetimeState known = NewDiskLifetimeState(0);
  const DiskLifetimeState recovered = RecoveredDiskLifetimeState(2);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1, &known,
                                          &config, 0);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(2), 2, &recovered,
                                          &config, 0);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(3), 3, &recovered,
                                          &config, 0);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(4), 4);
  // Key(5) deliberately has neither a committed generation nor a heat state.
  seconds = 40;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(3), 3,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  seconds = 100;
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted.size(), 5U);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  for (size_t bucket = 0; bucket < 8; ++bucket) {
    EXPECT_EQ(stats.age.buckets[bucket], bucket == 2 ? 1U : 0U);
  }
  EXPECT_EQ(stats.age.total_sec, 60U);
  EXPECT_EQ(stats.age.history_unknown, 2U);
  EXPECT_EQ(stats.unscored_victims, 2U);
  EXPECT_EQ(stats.released_residents, 5U);
  // Known-after-read score50, recovered-unread20, recovered-after-read60.
  EXPECT_EQ(stats.score.nonnegative_buckets[2], 3U);
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, 130);
  EXPECT_EQ(stats.score.negative_magnitude_buckets,
            (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec, 0);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, FullPoolFindsTheOnlyLegalExtentAfter64) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator::Options options = Options(config, clock, 4, 70);
  std::vector<uint32_t> wiped;
  options.on_extent_bind = [&](uint32_t extent) {
    wiped.push_back(extent);
    return true;
  };
  SlabAllocator allocator(options);
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t extent = 0; extent < 70; ++extent)
    entries.push_back({Key(extent + 1), 4096, extent, 0, 1});
  seconds = 100;
  ASSERT_NO_FATAL_FAILURE(Restore(allocator, entries, config, 100));
  for (uint64_t id = 1; id < 70; ++id) ASSERT_TRUE(allocator.Pin(Key(id)));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(70)});
  EXPECT_EQ(wiped, std::vector<uint32_t>{69});
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.coarse_extents, 70U);
  EXPECT_EQ(stats.precise_extents, 1U);
  EXPECT_EQ(stats.precise_residents, 1U);
  EXPECT_EQ(stats.budget_fallback, 0U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, AllEntriesCountOnlyOccupiedDonorSlots) {
  for (unsigned entry = 0; entry < 3; ++entry) {
    SCOPED_TRACE(entry);
    const DiskLifetimeConfig config = Config();
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [] { return 0; });
    SlabAllocator allocator(Options(config, clock));
    ASSERT_NO_FATAL_FAILURE(Restore(allocator,
                                    {{Key(1), 4096, 0, 0, 17},
                                     {Key(2), 4096, 0, 1, 31},
                                     {Key(3), 4096, 1, 0, 101}},
                                    config, 0));
    // The smaller donor stays pinned; none of its bytes may be counted.
    ASSERT_TRUE(allocator.Pin(Key(3)));
    SlabAllocatorTestPeer::SetColdEligibility(allocator, 30, 10, {1, 1});
    std::vector<BlockKey> evicted;
    const Result result =
        entry == 0
            ? SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted)
            : (entry == 1
                   ? SlabAllocatorTestPeer::StealCold(allocator, 1, &evicted)
                   : SlabAllocatorTestPeer::StealRestricted(allocator, 0, 1,
                                                            &evicted));
    ASSERT_EQ(result, Result::kBound);
    EXPECT_EQ(evicted.size(), 2U);
    const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
    EXPECT_EQ(stats.released_residents, 2U);
    EXPECT_EQ(stats.freed_slot_bytes, 8192U);
    EXPECT_EQ(stats.freed_payload_bytes, 48U);
    EXPECT_EQ(allocator.UsedBytes(), 4096U);
    EXPECT_TRUE(allocator.Contains(Key(3)));
    evicted.clear();
    EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
              Result::kUnavailable);
    EXPECT_TRUE(evicted.empty());
    EXPECT_EQ(allocator.DonorStats().freed_payload_bytes, 48U);
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeDonor, ProtectedLossIsExactOnAllEntriesIncludingFallback) {
  for (const bool missing_state : {false, true}) {
    for (unsigned entry = 0; entry < 3; ++entry) {
      SCOPED_TRACE(missing_state);
      SCOPED_TRACE(entry);
      const DiskLifetimeConfig config = Config();
      uint64_t seconds = 0;
      const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                    [&] { return seconds; });
      SlabAllocator allocator(Options(config, clock));
      std::vector<SlabAllocator::RestoreEntry> entries{
          {Key(1), 4096, 0, 0, 17},
          {Key(2), 4096, 0, 1, 31},
          {Key(4), 4096, 1, 0, 101}};
      if (missing_state) entries.push_back({Key(3), 4096, 0, 2, 7});
      ASSERT_TRUE(allocator.RestoreBulk(entries));
      seconds = 100;
      DiskLifetimeState protected_state = RecoveredDiskLifetimeState(1);
      protected_state.anchor = 100;
      DiskLifetimeState expired = RecoveredDiskLifetimeState(1);
      expired.anchor = 80;  // Score exactly KEEP at now100, not protected.
      SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1,
                                              &protected_state, &config, 100);
      SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(2), 2, &expired,
                                              &config, 100);
      SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(4), 4,
                                              &protected_state, &config, 100);
      if (missing_state) {
        SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(3), 3, nullptr,
                                                &config, 100);
      }
      ASSERT_TRUE(allocator.Pin(Key(4)));
      SlabAllocatorTestPeer::SetColdEligibility(allocator, 30, 10, {1, 1});
      std::vector<BlockKey> evicted;
      const Result result =
          entry == 0
              ? SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted)
              : (entry == 1
                     ? SlabAllocatorTestPeer::StealCold(allocator, 1, &evicted)
                     : SlabAllocatorTestPeer::StealRestricted(allocator, 0, 1,
                                                              &evicted));
      ASSERT_EQ(result, Result::kBound);
      const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
      EXPECT_EQ(stats.released_residents, missing_state ? 3U : 2U);
      EXPECT_EQ(stats.protected_victims, 1U);
      EXPECT_EQ(stats.protected_payload_bytes, 17U);
      EXPECT_EQ(stats.unscored_victims, missing_state ? 1U : 0U);
      EXPECT_EQ(stats.budget_fallback, missing_state ? 1U : 0U);
      EXPECT_TRUE(allocator.Contains(Key(4)));
      ExpectValid(allocator);
    }
  }
}

TEST(SlabLifetimeDonor, ProtectedLossUsesRecheckedPreWipeTime) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  bool advance = false;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    const uint64_t sampled = seconds;
    if (advance) seconds = 20;
    return sampled;
  });
  SlabAllocator::Options options = Options(config, clock, 4, 1);
  options.on_extent_bind = [&](uint32_t) {
    seconds = 1000;  // Persistence delay must not change the victim snapshot.
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(
      Restore(allocator, {{Key(1), 4096, 0, 0, 17}, {Key(2), 4096, 0, 1, 31}},
              config, 1, {0, 1}));
  seconds = 19;
  advance = true;
  std::vector<BlockKey> evicted;
  ASSERT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.released_residents, 2U);
  EXPECT_EQ(stats.protected_victims, 1U);
  EXPECT_EQ(stats.protected_payload_bytes, 31U);
  EXPECT_EQ(stats.unscored_victims, 0U);
}

TEST(SlabLifetimeDonor, ExactInspectionIsLimitedToEightCompleteExtents) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 4, 12));
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t extent = 0; extent < 12; ++extent)
    entries.push_back({Key(extent + 1), 4096, extent, 0, 1});
  seconds = 100;
  ASSERT_NO_FATAL_FAILURE(Restore(allocator, entries, config, 100));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.coarse_extents, 12U);
  EXPECT_EQ(stats.precise_extents, 8U);
  EXPECT_EQ(stats.precise_residents, 8U);
  EXPECT_EQ(stats.budget_fallback, 0U);
}

TEST(SlabLifetimeDonor, ExactFiveLevelOrderingDoesNotUseLegacyReadHeat) {
  for (unsigned tier = 0; tier < 5; ++tier) {
    SCOPED_TRACE(tier);
    const DiskLifetimeConfig config = Config();
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&] { return seconds; });
    SlabAllocator allocator(Options(config, clock));
    std::vector<SlabAllocator::RestoreEntry> entries;
    std::vector<uint32_t> anchors;
    uint64_t winner = 2;
    if (tier == 0) {
      // Both pages look protected, but anchor80 ends exactly at now100.
      entries = {{Key(1), 4096, 0, 0, 1}, {Key(2), 4096, 1, 0, 1}};
      anchors = {100, 80};
    } else if (tier == 1) {
      entries = {{Key(1), 4096, 0, 0, 8},
                 {Key(11), 4096, 0, 1, 2},
                 {Key(2), 4096, 1, 0, 2},
                 {Key(12), 4096, 1, 1, 8}};
      anchors = {100, 0, 100, 0};
    } else if (tier == 2) {
      entries = {{Key(1), 4096, 0, 0, 8}, {Key(2), 4096, 1, 0, 2}};
      anchors = {0, 0};
    } else if (tier == 3) {
      entries = {{Key(1), 8192, 0, 0, 1}, {Key(2), 4096, 1, 0, 1}};
      anchors = {0, 0};
    } else {
      entries = {{Key(1), 4096, 0, 0, 1}, {Key(2), 4096, 1, 0, 1}};
      anchors = {0, 0};
      winner = 1;
    }
    seconds = 100;
    ASSERT_NO_FATAL_FAILURE(Restore(allocator, entries, config, 100, anchors));
    // Refresh the desired victim through legacy Get only; lifetime ranking
    // must still use the independently committed scoring state.
    SlabAllocator::SlotRef ref;
    ASSERT_TRUE(allocator.Get(Key(winner), &ref));
    std::vector<BlockKey> evicted;
    EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 2, &evicted),
              Result::kBound);
    ASSERT_FALSE(evicted.empty());
    EXPECT_NE(std::find(evicted.begin(), evicted.end(), Key(winner)),
              evicted.end());
    EXPECT_EQ(allocator.DonorStats().budget_fallback, 0U);
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeDonor, ExistingExactCandidateWinsWhenNextCannotFitBudget) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 8192, 2));
  std::vector<SlabAllocator::RestoreEntry> entries;
  entries.push_back({Key(1), 4096, 0, 0, 1});
  for (uint32_t slot = 0; slot < 8192; ++slot)
    entries.push_back({Key(slot + 2), 4096, 1, slot, 1});
  seconds = 100;
  ASSERT_NO_FATAL_FAILURE(Restore(allocator, entries, config, 100));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.precise_extents, 1U);
  EXPECT_EQ(stats.precise_residents, 1U);
  EXPECT_EQ(stats.budget_fallback, 0U);
  EXPECT_EQ(allocator.Count(), 8192U);
}

TEST(SlabLifetimeDonor, Exactly8192ResidentsCanCompleteTwoCandidates) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 4096, 2));
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t extent = 0; extent < 2; ++extent)
    for (uint32_t slot = 0; slot < 4096; ++slot)
      entries.push_back({Key(static_cast<uint64_t>(extent) * 4096 + slot + 1),
                         4096, extent, slot, 1});
  seconds = 100;
  ASSERT_NO_FATAL_FAILURE(Restore(allocator, entries, config, 100));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted.size(), 4096U);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.precise_extents, 2U);
  EXPECT_EQ(stats.precise_residents, 8192U);
  EXPECT_EQ(stats.budget_fallback, 0U);
  EXPECT_EQ(stats.released_residents, 4096U);
  EXPECT_EQ(allocator.Count(), 4096U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, PartialUnknownExtentNeverEntersExactRanking) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock));
  ASSERT_TRUE(allocator.RestoreBulk({{Key(1), 4096, 0, 0, 1},
                                     {Key(11), 4096, 0, 1, 1},
                                     {Key(2), 4096, 1, 0, 3}}));
  seconds = 100;
  const DiskLifetimeState cold = RecoveredDiskLifetimeState(1);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1, &cold, &config,
                                          100);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(11), 11, nullptr,
                                          &config, 100);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(2), 2, &cold, &config,
                                          100);
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2)});
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.precise_extents, 1U);
  EXPECT_LE(stats.precise_residents, 3U);
  EXPECT_GT(stats.precise_residents, 1U);
  EXPECT_EQ(stats.budget_fallback, 0U);
  EXPECT_TRUE(allocator.Contains(Key(1)));
  EXPECT_TRUE(allocator.Contains(Key(11)));
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor,
     OversizedExtentUsesCoarseFallbackAndCountsReleaseSeparately) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 8193, 1));
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t slot = 0; slot < 8193; ++slot)
    entries.push_back({Key(slot + 1), 4096, 0, slot, 1});
  seconds = 100;
  std::vector<uint32_t> anchors(entries.size(), 0);
  anchors.back() = 100;  // The last release exceeds the selection budget.
  ASSERT_NO_FATAL_FAILURE(Restore(allocator, entries, config, 100, anchors));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted.size(), 8193U);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.coarse_extents, 1U);
  EXPECT_EQ(stats.precise_extents, 0U);
  EXPECT_EQ(stats.precise_residents, 0U);
  EXPECT_EQ(stats.budget_fallback, 1U);
  EXPECT_EQ(stats.released_residents, 8193U);
  EXPECT_EQ(stats.protected_victims, 1U);
  EXPECT_EQ(stats.protected_payload_bytes, 1U);
  EXPECT_EQ(stats.unscored_victims, 0U);
  EXPECT_EQ(stats.freed_slot_bytes, 8193ULL * 4096);
  EXPECT_EQ(stats.freed_payload_bytes, 8193U);
  EXPECT_EQ(allocator.Count(), 0U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor,
     OnlySelectedExtentAttemptsPersistenceAndFailureKeepsAccounts) {
  const DiskLifetimeConfig config = Config();
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  SlabAllocator::Options options = Options(config, clock);
  std::vector<uint32_t> attempts;
  options.on_extent_bind = [&](uint32_t extent) {
    attempts.push_back(extent);
    return false;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(
      Restore(allocator, {{Key(1), 4096, 0, 0, 2}, {Key(2), 4096, 1, 0, 1}},
              config, 0));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kPersistenceError);
  EXPECT_EQ(attempts, std::vector<uint32_t>{1});
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 1, 0), 1U);
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.persistence_errors, 1U);
  EXPECT_EQ(stats.released_residents, 0U);
  EXPECT_EQ(stats.freed_slot_bytes, 0U);
  EXPECT_EQ(stats.freed_payload_bytes, 0U);
  EXPECT_EQ(stats.protected_victims, 0U);
  EXPECT_EQ(stats.protected_payload_bytes, 0U);
  EXPECT_EQ(stats.unscored_victims, 0U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  ExpectEmptyScore(stats.score);
  ExpectValid(allocator);
}

TEST(SlabLifetimeDonor, BoundEmptyExtentsAreEligibleOnAllThreeEntries) {
  for (unsigned entry = 0; entry < 3; ++entry) {
    SCOPED_TRACE(entry);
    const DiskLifetimeConfig config = Config();
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&] { return seconds; });
    SlabAllocator allocator(Options(config, clock));
    ASSERT_NO_FATAL_FAILURE(
        Restore(allocator, {{Key(1), 4096, 0, 0, 1}, {Key(2), 4096, 1, 0, 1}},
                config, 0));
    ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
    SlabAllocatorTestPeer::SetColdEligibility(allocator, 30, 10, {1, 1});
    std::vector<BlockKey> evicted;
    const Result result =
        entry == 0
            ? SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted)
            : (entry == 1
                   ? SlabAllocatorTestPeer::StealCold(allocator, 1, &evicted)
                   : SlabAllocatorTestPeer::StealRestricted(allocator, 0, 1,
                                                            &evicted));
    EXPECT_EQ(result, Result::kBound);
    EXPECT_TRUE(evicted.empty());
    EXPECT_EQ(allocator.DonorStats().released_residents, 0U);
    EXPECT_EQ(allocator.DonorStats().freed_slot_bytes, 0U);
    EXPECT_EQ(allocator.DonorStats().freed_payload_bytes, 0U);
    EXPECT_EQ(allocator.DonorStats().protected_victims, 0U);
    EXPECT_EQ(allocator.DonorStats().protected_payload_bytes, 0U);
    EXPECT_EQ(allocator.DonorStats().unscored_victims, 0U);
    ExpectEmptyScore(allocator.DonorStats().score);
    EXPECT_TRUE(SlabAllocatorTestPeer::BucketRowEmpty(allocator, 0));
    EXPECT_EQ(allocator.DonorStats().precise_residents, 1U);
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeDonor,
     PinTargetClassColdWindowAndStripeFloorRemainHardEligibility) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(config, clock, 4, 4));
  ASSERT_NO_FATAL_FAILURE(Restore(allocator,
                                  {{Key(1), 4096, 0, 0, 1},
                                   {Key(2), 4096, 1, 0, 1},
                                   {Key(3), 8192, 2, 0, 1},
                                   {Key(4), 4096, 3, 0, 1}},
                                  config, 0));
  ASSERT_TRUE(allocator.Pin(Key(1)));
  SlabAllocatorTestPeer::SetColdEligibility(allocator, 30, 10, {1, 25, 1, 1});
  std::vector<BlockKey> evicted;
  EXPECT_EQ(
      SlabAllocatorTestPeer::StealRestricted(allocator, 0, 1, &evicted, 3),
      Result::kUnavailable);
  ExpectEmptyScore(allocator.DonorStats().score);
  EXPECT_EQ(SlabAllocatorTestPeer::StealCold(allocator, 1, &evicted),
            Result::kBound);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(4)});
  EXPECT_TRUE(allocator.Contains(Key(1)));
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_TRUE(allocator.Contains(Key(3)));
  EXPECT_EQ(allocator.ColdSteals(), 1U);
}

TEST(SlabLifetimeDonor, GuardAndHardClockStopBeforeAnyPersistence) {
  const DiskLifetimeConfig config = Config();
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  bool allowed = false;
  unsigned attempts = 0;
  SlabAllocator::Options options = Options(config, clock);
  options.can_reclaim = [&] { return allowed; };
  options.on_extent_bind = [&](uint32_t) {
    ++attempts;
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(
      Restore(allocator, {{Key(1), 4096, 0, 0, 1}}, config, 0));
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kReclaimDenied);
  allowed = true;
  seconds = static_cast<uint64_t>(UINT32_MAX) + 1;
  EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
            Result::kReclaimDenied);
  EXPECT_EQ(attempts, 0U);
  EXPECT_TRUE(allocator.Contains(Key(1)));
  const SlabAllocator::LifetimeDonorStats stats = allocator.DonorStats();
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  ExpectEmptyScore(stats.score);
  ExpectValid(allocator);
}

class Gate {
 public:
  void Enter() {
    std::unique_lock<std::mutex> lock(mu_);
    entered_ = true;
    cv_.notify_all();
    cv_.wait(lock, [&] { return open_; });
  }
  bool Await() {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, std::chrono::seconds(5),
                        [&] { return entered_; });
  }
  void Open() {
    const std::lock_guard<std::mutex> lock(mu_);
    open_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool entered_ = false;
  bool open_ = false;
};
struct OpenOnExit {
  Gate& gate;
  ~OpenOnExit() { gate.Open(); }
};

TEST(SlabLifetimeDonor, PinAndPutCannotOvertakeSelectedPersistence) {
  const DiskLifetimeConfig config = Config();
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Gate gate;
  SlabAllocator::Options options = Options(config, clock);
  std::atomic<bool> first{true};
  options.on_extent_bind = [&](uint32_t) {
    if (first.exchange(false)) gate.Enter();
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(
      Restore(allocator, {{Key(1), 4096, 0, 0, 1}, {Key(2), 4096, 1, 0, 2}},
              config, 0));
  std::atomic<bool> pin_started{false};
  std::atomic<bool> put_started{false};
  std::atomic<bool> snapshot_started{false};
  std::vector<BlockKey> evicted;
  std::future<Result> steal;
  std::future<bool> pin;
  std::future<bool> put;
  std::future<SlabAllocator::LifetimeDonorStats> snapshot;
  const OpenOnExit release{gate};
  steal = std::async(std::launch::async, [&] {
    return SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted);
  });
  ASSERT_TRUE(gate.Await());
  pin = std::async(std::launch::async, [&] {
    pin_started.store(true);
    return allocator.Pin(Key(1));
  });
  put = std::async(std::launch::async, [&] {
    put_started.store(true);
    SlabAllocator::SlotRef ref;
    std::vector<BlockKey> removed;
    return allocator.Put(Key(3), 1, &ref, &removed);
  });
  snapshot = std::async(std::launch::async, [&] {
    snapshot_started.store(true);
    return allocator.DonorStats();
  });
  const std::chrono::steady_clock::time_point until =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((!pin_started.load() || !put_started.load() ||
          !snapshot_started.load()) &&
         std::chrono::steady_clock::now() < until)
    std::this_thread::yield();
  ASSERT_TRUE(pin_started.load());
  ASSERT_TRUE(put_started.load());
  ASSERT_TRUE(snapshot_started.load());
  EXPECT_EQ(snapshot.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  EXPECT_EQ(pin.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  EXPECT_EQ(put.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  gate.Open();
  ASSERT_EQ(steal.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(steal.get(), Result::kBound);
  ASSERT_EQ(pin.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_FALSE(pin.get());
  ASSERT_EQ(put.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_TRUE(put.get());
  ASSERT_EQ(snapshot.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  const SlabAllocator::LifetimeDonorStats captured = snapshot.get();
  EXPECT_EQ(captured.released_residents, 1U);
  EXPECT_EQ(captured.age.history_unknown, 1U);
  EXPECT_EQ(captured.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(captured.age.total_sec, 0U);
  EXPECT_EQ(captured.score.nonnegative_buckets[3], 1U);
  EXPECT_DOUBLE_EQ(captured.score.nonnegative_total_sec, 110);
  EXPECT_EQ(captured.score.negative_magnitude_buckets,
            (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(captured.score.negative_magnitude_total_sec, 0);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  ExpectValid(allocator);
}

class LifetimeDonorStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_donor_" +
         std::string(
             ::testing::UnitTest::GetInstance()->current_test_info()->name()));
    config_ = Config();
    clock_ = std::make_shared<DiskLifetimeClock>(DiskLifetimeHorizon(config_),
                                                 [] { return 0; });
  }
  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  void Create(uint32_t extents, uint32_t slots = 4) {
    store_.reset();
    std::filesystem::remove_all(dir_);
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
    options.capacity_bytes =
        static_cast<uint64_t>(extents) * options.extent_bytes;
    options.slot_granularity = 4096;
    options.table_sync_ms = 0;
    options.reclaim_interval_ms = 0;
    options.lifetime_config = config_;
    options.lifetime_clock = clock_;
    options.lifetime_class_eviction = true;
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  std::filesystem::path dir_;
  DiskLifetimeConfig config_;
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::unique_ptr<DiskSlabStore> store_;
};

TEST_F(LifetimeDonorStoreTest, GrowRespectsTwoExtentLimitAndEightExtentFloor) {
  for (const uint32_t donors : {9U, 11U}) {
    SCOPED_TRACE(donors);
    ASSERT_NO_FATAL_FAILURE(Create(donors + 1));
    for (uint32_t extent = 0; extent < donors; ++extent)
      ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 4096, extent));
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 8192, donors));
    DiskSlabStoreTestPeer::Quiesce(*store_);
    const std::string payload(8192, 'x');
    ASSERT_EQ(store_->Cache(Key(1), payload.data(), payload.size()),
              Status::kOk);
    ASSERT_EQ(store_->Cache(Key(2), payload.data(), payload.size()),
              Status::kOk);
    DiskSlabStoreTestPeer::Tick(*store_);
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_EQ(stats.rebalanced_extents, donors == 9 ? 1U : 2U);
    EXPECT_EQ(stats.reclaimed_slots, 0U);
    EXPECT_EQ(stats.lifetime_donor_released_residents, 0U);
    EXPECT_EQ(stats.lifetime_donor_freed_slot_bytes, 0U);
    EXPECT_EQ(stats.lifetime_donor_freed_payload_bytes, 0U);
    EXPECT_EQ(stats.lifetime_donor_protected_victims, 0U);
    EXPECT_EQ(stats.lifetime_donor_protected_payload_bytes, 0U);
    EXPECT_EQ(stats.lifetime_donor_unscored_victims, 0U);
    EXPECT_EQ(stats.lifetime_donor_persistence_errors, 0U);
    for (uint64_t id = 1; id <= 2; ++id) {
      std::string value;
      EXPECT_EQ(store_->Range(Key(id), 0, 0, &value), Status::kOk);
      EXPECT_EQ(value, payload);
    }
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
  }
}

TEST_F(LifetimeDonorStoreTest, GrowCountsResidentBytesNotExtentCapacity) {
  ASSERT_NO_FATAL_FAILURE(Create(10));
  for (uint32_t extent = 0; extent < 9; ++extent)
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 4096, extent));
  ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 8192, 9));
  const std::string small(17, 's');
  for (uint64_t id = 1; id <= 36; ++id)
    ASSERT_EQ(store_->Cache(Key(id), small.data(), small.size()), Status::kOk);
  DiskSlabStoreTestPeer::Quiesce(*store_);
  const std::string large(5000, 'l');
  ASSERT_EQ(store_->Cache(Key(100), large.data(), large.size()), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(101), large.data(), large.size()), Status::kOk);
  DiskSlabStoreTestPeer::Tick(*store_);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(stats.rebalanced_extents, 1U);
  EXPECT_EQ(stats.lifetime_donor_released_residents, 4U);
  EXPECT_EQ(stats.lifetime_donor_freed_slot_bytes, 4U * 4096);
  EXPECT_EQ(stats.lifetime_donor_freed_payload_bytes, 68U);
  EXPECT_EQ(stats.lifetime_donor_protected_victims, 4U);
  EXPECT_EQ(stats.lifetime_donor_protected_payload_bytes, 68U);
  EXPECT_EQ(stats.lifetime_donor_unscored_victims, 0U);
  EXPECT_EQ(stats.lifetime_class_victims, 0U);
  EXPECT_EQ(stats.lifetime_background_victims, 0U);
  EXPECT_EQ(stats.watermark_slot_removals, 0U);
  EXPECT_EQ(store_->EvictedBytes(), 68U);
  EXPECT_EQ(store_->Count(), 34U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 32U * 17 + 10000);
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->GetStats().lifetime_donor_freed_payload_bytes, 68U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
}

TEST_F(LifetimeDonorStoreTest,
       HotDonorIsSkippedWithoutConsumingClassReclaimDemand) {
  ASSERT_NO_FATAL_FAILURE(Create(10, 8));
  for (uint32_t extent = 0; extent < 9; ++extent)
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 4096, extent));
  ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 8192, 9));
  DiskSlabStoreTestPeer::Quiesce(*store_);
  const std::string payload(8192, 'h');
  for (uint64_t id = 1; id <= 4; ++id)
    ASSERT_EQ(store_->Cache(Key(id), payload.data(), payload.size()),
              Status::kOk);
  for (uint64_t id = 100; id <= 101; ++id)
    ASSERT_EQ(store_->Cache(Key(id), "donor", 5), Status::kOk);
  // Donor delta2 is greater than target delta4/4. Its nine extents are large
  // enough to donate, but the demand qualification must exclude it.
  DiskSlabStoreTestPeer::Tick(*store_);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(stats.rebalanced_extents, 0U);
  EXPECT_EQ(stats.lifetime_donor_coarse_extents, 0U);
  // The class pass must still see the same four NEW writes after GROW reads
  // the snapshot; with no free target slots it should reclaim one headroom.
  EXPECT_EQ(stats.reclaimed_slots, 1U);
  EXPECT_EQ(store_->Count(), 5U);
  size_t survivors = 0;
  for (uint64_t id = 1; id <= 4; ++id) {
    if (!store_->IsCached(Key(id))) continue;
    ++survivors;
    std::string value;
    EXPECT_EQ(store_->Range(Key(id), 0, 0, &value), Status::kOk);
    EXPECT_EQ(value, payload);
  }
  EXPECT_EQ(survivors, 3U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 3 * payload.size() + 10);
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->GetStats().reclaimed_slots, 1U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
}

TEST_F(LifetimeDonorStoreTest,
       BackgroundPersistenceFailureDoesNotTryAnotherClass) {
  ASSERT_NO_FATAL_FAILURE(Create(19));
  for (uint32_t extent = 0; extent < 9; ++extent)
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 4096, extent));
  for (uint32_t extent = 9; extent < 18; ++extent)
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 8192, extent));
  ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 16384, 18));
  DiskSlabStoreTestPeer::Quiesce(*store_);
  const std::string payload(16384, 't');
  ASSERT_EQ(store_->Cache(Key(1), payload.data(), payload.size()), Status::kOk);
  const uint64_t wipes = store_->GetStats().bind_wipes;
  DiskSlabStoreTestPeer::FailMetadata(*store_);
  DiskSlabStoreTestPeer::Tick(*store_);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(stats.metadata_io_errors, 1U);
  EXPECT_EQ(stats.lifetime_donor_released_residents, 0U);
  EXPECT_EQ(stats.lifetime_donor_freed_slot_bytes, 0U);
  EXPECT_EQ(stats.lifetime_donor_freed_payload_bytes, 0U);
  EXPECT_EQ(stats.lifetime_donor_protected_victims, 0U);
  EXPECT_EQ(stats.lifetime_donor_protected_payload_bytes, 0U);
  EXPECT_EQ(stats.lifetime_donor_unscored_victims, 0U);
  EXPECT_EQ(stats.lifetime_donor_persistence_errors, 1U);
  EXPECT_EQ(stats.lifetime_donor_coarse_extents, 19U);
  EXPECT_EQ(stats.rebalanced_extents, 0U);
  EXPECT_EQ(stats.bind_wipes, wipes);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_EQ(store_->TenantUsedBytes(7), payload.size());
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
}

TEST_F(LifetimeDonorStoreTest,
       ForegroundMetadataFailureKeepsEveryResidentAndBucket) {
  ASSERT_NO_FATAL_FAILURE(Create(10));
  for (uint32_t extent = 0; extent < 10; ++extent)
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 4096, extent));
  for (uint64_t id = 1; id <= 10; ++id)
    ASSERT_EQ(store_->Cache(Key(id), "old", 3), Status::kOk);
  const uint64_t estimate = DiskSlabStoreTestPeer::Estimate(*store_, 0);
  ASSERT_EQ(estimate, 1U);
  DiskSlabStoreTestPeer::FailMetadata(*store_);
  const std::string payload(8192, 'n');
  EXPECT_EQ(store_->Cache(Key(100), payload.data(), payload.size()),
            Status::kIOError);
  EXPECT_EQ(store_->Count(), 10U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 30U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 0), estimate);
  EXPECT_EQ(store_->GetStats().lifetime_donor_persistence_errors, 1U);
  EXPECT_EQ(store_->GetStats().metadata_io_errors, 1U);
  EXPECT_EQ(store_->GetStats().lifetime_donor_freed_slot_bytes, 0U);
  EXPECT_EQ(store_->GetStats().lifetime_donor_freed_payload_bytes, 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
}

}  // namespace
}  // namespace dfkv
