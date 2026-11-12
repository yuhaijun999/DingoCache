#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/disk_write_result.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static void ConfigureWatermark(DiskSlabStore& store, uint64_t high,
                                 uint64_t low, size_t victims) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    store.evict_high_bytes_ = high;
    store.evict_low_bytes_ = low;
    store.evict_max_victims_per_tick_ = victims;
  }
  static void Tick(DiskSlabStore& store) {
    // Isolate the watermark result from the independent class headroom pass.
    const std::vector<SlabAllocator::ClassStat> classes =
        store.alloc_->Classes();
    store.reclaim_last_puts_.resize(classes.size());
    store.lifetime_reclaim_targets_.assign(classes.size(), 0);
    for (size_t i = 0; i < classes.size(); ++i)
      store.reclaim_last_puts_[i] = classes[i].puts;
    store.ReclaimTick();
  }
  static bool Pin(DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->Pin(key);
  }
  static bool Unpin(DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->Unpin(key);
  }
  static void FailInvalidation(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) close(store.table_fd_);
    store.table_fd_ = -1;
  }
  static std::pair<uint64_t, uint64_t> Watermarks(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return {store.evict_high_bytes_, store.evict_low_bytes_};
  }
  static uint64_t NewPutBytes(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.new_put_bytes_since_tick_;
  }
  static bool IndexesValid(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeIndexesValid(*store.alloc_);
  }
};
namespace {
BlockKey Key(uint64_t id) { return {id, 0, 7}; }

SlabAllocator::Options Options(uint32_t slots, uint32_t max_reads = 2) {
  SlabAllocator::Options options;
  options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
  options.num_extents = 1;
  options.track_lifetime_state = true;
  options.lifetime_max_reads = max_reads;
  return options;
}

void Seed(SlabAllocator& allocator, uint64_t id, uint8_t reads = 0,
          uint32_t anchor = 0, bool pin = false, size_t bytes = 1) {
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(id), bytes, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  // Construct a known scoring band independently of NEW write protection.
  DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  state.anchor = anchor;
  state.hstate = static_cast<uint8_t>(reads << 2);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state);
  if (pin) {
    ASSERT_TRUE(allocator.Pin(Key(id)));
  }
}

size_t Evict(SlabAllocator& allocator, uint64_t target, size_t victims,
             const DiskLifetimeConfig& config, const DiskLifetimeClock& clock,
             std::vector<BlockKey>* evicted,
             SlabAllocatorTestPeer::LifetimeWatermarkStats* stats = nullptr) {
  return SlabAllocatorTestPeer::EvictLifetimeToTarget(
      allocator, target, victims, config, clock, evicted, stats);
}

void SeedState(SlabAllocator& allocator, uint64_t id,
               const DiskLifetimeState& state) {
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(id), 1, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state);
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

TEST(SlabLifetimeWatermark, ScoreBinsSeparateSignsIncludingZero) {
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
  for (const bool negative : {false, true}) {
    for (const Case& test : cases) {
      if (negative && test.magnitude == 0) continue;
      SCOPED_TRACE(negative);
      SCOPED_TRACE(test.magnitude);
      const DiskLifetimeConfig config = ScoreConfig(3000000);
      uint64_t seconds = 0;
      const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                    [&] { return seconds; });
      unsigned writes = 0;
      SlabAllocator::Options options = Options(2, 1);
      options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
        ++writes;
        return true;
      };
      SlabAllocator allocator(options);
      ASSERT_NO_FATAL_FAILURE(
          SeedState(allocator, 1, RecoveredDiskLifetimeState(1)));
      seconds = negative ? config.base_sec + test.magnitude
                         : config.base_sec - test.magnitude;
      std::vector<BlockKey> evicted;
      SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
      ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted, &stats), 1U);
      EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
      EXPECT_EQ(writes, 1U);
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
      EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
    }
  }
}

TEST(SlabLifetimeWatermark, ScoreUsesFinalPreWipeTimeAndNullableStats) {
  for (const bool with_stats : {false, true}) {
    SCOPED_TRACE(with_stats);
    const DiskLifetimeConfig config = ScoreConfig(100);
    uint64_t seconds = 0;
    bool armed = false;
    unsigned samples = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
      const uint64_t sampled = seconds;
      if (armed) {
        ++samples;
        seconds = 20;
      }
      return sampled;
    });
    unsigned writes = 0;
    SlabAllocator::Options options = Options(2, 1);
    options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
      ++writes;
      seconds = 1000;
      return true;
    };
    SlabAllocator allocator(options);
    ASSERT_NO_FATAL_FAILURE(
        SeedState(allocator, 1, RecoveredDiskLifetimeState(1)));
    seconds = 19;  // Selection81; final80; post-I/O -900.
    armed = true;
    SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
    stats.score.nonnegative_buckets[0] = 99;
    stats.score.negative_magnitude_buckets[0] = 99;
    stats.score.nonnegative_total_sec = 99;
    stats.score.negative_magnitude_total_sec = 99;
    std::vector<BlockKey> evicted;
    ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted,
                    with_stats ? &stats : nullptr),
              1U);
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    EXPECT_EQ(writes, 1U);
    EXPECT_EQ(samples, 2U);
    EXPECT_EQ(allocator.WatermarkScanVisits(), 1U);
    EXPECT_EQ(stats.score.nonnegative_buckets[0], with_stats ? 0U : 99U);
    EXPECT_EQ(stats.score.nonnegative_buckets[3], with_stats ? 1U : 0U);
    EXPECT_EQ(stats.score.negative_magnitude_buckets,
              (std::array<uint64_t, 8>{with_stats ? 0U : 99U}));
    EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, with_stats ? 80 : 99);
    EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec,
                     with_stats ? 0 : 99);
  }
}

TEST(SlabLifetimeWatermark, ScoreSumDoesNotWrapForValidNearInt64Maximum) {
  const uint64_t maximum = std::numeric_limits<int64_t>::max();
  const DiskLifetimeConfig config = ScoreConfig(maximum - 1);
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  SlabAllocator allocator(Options(4, 1));
  for (uint64_t id = 1; id <= 3; ++id) {
    ASSERT_NO_FATAL_FAILURE(SeedState(allocator, id, NewDiskLifetimeState(0)));
  }
  ASSERT_EQ(DiskLifetimeScore(config, 0, 0, false, true, 0),
            std::numeric_limits<int64_t>::max());
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  ASSERT_EQ(Evict(allocator, 0, 3, config, clock, &evicted, &stats), 3U);
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
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, ScoreCountsLargestClockReachableDeficit) {
  const DiskLifetimeConfig config = ScoreConfig(1);
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(2, 1));
  ASSERT_NO_FATAL_FAILURE(
      SeedState(allocator, 1, RecoveredDiskLifetimeState(1)));
  // Draining reaches UINT32_MAX, not UINT64_MAX. Runtime watermarks cannot
  // reach INT64_MIN; its safe conversion is a defensive arithmetic property.
  seconds = UINT32_MAX;
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted, &stats), 1U);
  EXPECT_EQ(stats.score.negative_magnitude_buckets[7], 1U);
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec,
                   static_cast<double>(UINT32_MAX) - 1);
  EXPECT_EQ(stats.score.nonnegative_buckets, (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, 0);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, RecoveredBootAgeUsesFinalTimeAndNullableStats) {
  for (const bool with_stats : {false, true}) {
    SCOPED_TRACE(with_stats);
    uint64_t seconds = 0;
    bool armed = false;
    unsigned samples = 0;
    const DiskLifetimeConfig config;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
      const uint64_t sampled = seconds;
      if (armed) {
        ++samples;
        seconds = 20;
      }
      return sampled;
    });
    unsigned writes = 0;
    SlabAllocator::Options options = Options(2);
    options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
      ++writes;
      seconds = 1000;
      return true;
    };
    SlabAllocator allocator(options);
    ASSERT_NO_FATAL_FAILURE(
        SeedState(allocator, 1, RecoveredDiskLifetimeState(2)));
    seconds = 10;
    ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                       config, clock),
              SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
    SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
    stats.age.recovered_boot_age_buckets[0] = 99;
    stats.age.recovered_boot_age_total_sec = 99;
    seconds = 19;
    armed = true;
    std::vector<BlockKey> evicted;
    ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted,
                    with_stats ? &stats : nullptr),
              1U);
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    EXPECT_EQ(writes, 1U);
    EXPECT_EQ(samples, 2U);
    EXPECT_EQ(stats.age.recovered_boot_age_buckets[0], with_stats ? 0U : 99U);
    EXPECT_EQ(stats.age.recovered_boot_age_buckets[2], with_stats ? 1U : 0U);
    EXPECT_EQ(stats.age.recovered_boot_age_total_sec, with_stats ? 20U : 99U);
    EXPECT_EQ(stats.age.history_unknown, with_stats ? 1U : 0U);
    EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
    EXPECT_EQ(stats.age.total_sec, 0U);
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

TEST(SlabLifetimeWatermark, AgeUsesFixedDisplayBoundaries) {
  struct Case {
    uint32_t seconds;
    size_t bucket;
  };
  const std::array<Case, 14> cases = {{{0, 0},
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
                                       {2592001, 7},
                                       {UINT32_MAX, 7}}};
  for (const Case& test : cases) {
    SCOPED_TRACE(test.seconds);
    uint64_t seconds = 0;
    const DiskLifetimeConfig config;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&] { return seconds; });
    SlabAllocator allocator(Options(2));
    ASSERT_NO_FATAL_FAILURE(SeedState(allocator, 1, NewDiskLifetimeState(0)));
    seconds = test.seconds;
    std::vector<BlockKey> evicted;
    SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
    ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted, &stats), 1U);
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    for (size_t bucket = 0; bucket < stats.age.buckets.size(); ++bucket) {
      EXPECT_EQ(stats.age.buckets[bucket], bucket == test.bucket ? 1U : 0U);
    }
    EXPECT_EQ(stats.age.total_sec, test.seconds);
    EXPECT_EQ(stats.age.history_unknown, 0U);
    EXPECT_EQ(stats.age.recovered_boot_age_buckets,
              (std::array<uint64_t, 8>{}));
    EXPECT_EQ(stats.age.recovered_boot_age_total_sec, 0U);
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

TEST(SlabLifetimeWatermark, AgeUsesFinalPreWipeTimeAndNullableStats) {
  for (const bool with_stats : {false, true}) {
    SCOPED_TRACE(with_stats);
    uint64_t seconds = 0;
    bool armed = false;
    unsigned samples = 0;
    const DiskLifetimeConfig config;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
      const uint64_t sampled = seconds;
      if (armed) {
        ++samples;
        seconds = 20;
      }
      return sampled;
    });
    SlabAllocator::Options options = Options(2);
    unsigned writes = 0;
    options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
      ++writes;
      seconds = 1000;
      return true;
    };
    SlabAllocator allocator(options);
    ASSERT_NO_FATAL_FAILURE(SeedState(allocator, 1, NewDiskLifetimeState(0)));
    seconds = 19;
    armed = true;
    SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
    stats.age.buckets[0] = 99;
    stats.age.total_sec = 99;
    stats.age.history_unknown = 99;
    std::vector<BlockKey> evicted;
    ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted,
                    with_stats ? &stats : nullptr),
              1U);
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    EXPECT_EQ(writes, 1U);
    EXPECT_EQ(samples, 2U);
    EXPECT_EQ(allocator.WatermarkScanVisits(), 1U);
    EXPECT_EQ(stats.age.buckets[0], with_stats ? 0U : 99U);
    EXPECT_EQ(stats.age.buckets[2], with_stats ? 1U : 0U);
    EXPECT_EQ(stats.age.total_sec, with_stats ? 20U : 99U);
    EXPECT_EQ(stats.age.history_unknown, with_stats ? 0U : 99U);
  }
}

TEST(SlabLifetimeWatermark, AgeExcludesRecoveredHistoryEvenAfterRealRead) {
  uint64_t seconds = 0;
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(4));
  ASSERT_NO_FATAL_FAILURE(
      SeedState(allocator, 1, RecoveredDiskLifetimeState(2)));
  ASSERT_NO_FATAL_FAILURE(
      SeedState(allocator, 2, RecoveredDiskLifetimeState(2)));
  seconds = 10;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(2), 2,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  seconds = 20;
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  ASSERT_EQ(Evict(allocator, 0, 2, config, clock, &evicted, &stats), 2U);
  EXPECT_EQ(evicted.size(), 2U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 2U);
  EXPECT_EQ(stats.age.recovered_boot_age_buckets,
            (std::array<uint64_t, 8>{0, 0, 2}));
  EXPECT_EQ(stats.age.recovered_boot_age_total_sec, 40U);
  EXPECT_EQ(stats.score.nonnegative_buckets,
            (std::array<uint64_t, 8>{0, 0, 0, 0, 0, 0, 2}));
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, 2419170);
  EXPECT_EQ(stats.score.negative_magnitude_buckets,
            (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec, 0);
  EXPECT_EQ(allocator.Count(), 0U);
}

TEST(SlabLifetimeWatermark, AgeSaturatesFutureAnchorAtZero) {
  uint64_t seconds = 0;
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(Options(2));
  // Defensive synthetic state: valid production anchors never lead the clock.
  ASSERT_NO_FATAL_FAILURE(SeedState(allocator, 1, NewDiskLifetimeState(30)));
  seconds = 20;
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted, &stats), 1U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{1}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
}

TEST(SlabLifetimeWatermark, ProtectedCountUsesFinalPreWipeTime) {
  for (const uint64_t final_now : {129599U, 129600U, 129601U}) {
    for (const bool with_stats : {false, true}) {
      SCOPED_TRACE(final_now);
      SCOPED_TRACE(with_stats);
      const DiskLifetimeConfig config;
      uint64_t seconds = 0;
      bool armed = false;
      unsigned samples = 0;
      const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
        const uint64_t sampled = seconds;
        if (armed) {
          ++samples;
          seconds = final_now;
        }
        return sampled;
      });
      unsigned attempts = 0;
      SlabAllocator::Options options = Options(2);
      options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
        ++attempts;
        seconds = 1000000;  // Do not classify again after persistence.
        return true;
      };
      SlabAllocator allocator(options);
      ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1));
      ASSERT_EQ(DiskLifetimeScore(config, 0, 0, false, false, 129599),
                static_cast<int64_t>(config.keep_score_sec) + 1);
      seconds = 129599;
      armed = true;
      SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
      stats.protected_victims = 99;
      std::vector<BlockKey> evicted;
      ASSERT_EQ(Evict(allocator, 0, 1, config, clock, &evicted,
                      with_stats ? &stats : nullptr),
                1U);
      EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
      EXPECT_EQ(attempts, 1U);
      // The constructor sampled its origin before arming: only selection and
      // final pre-wipe reads belong to this actual allocator operation.
      EXPECT_EQ(samples, 2U);
      EXPECT_EQ(allocator.WatermarkScanVisits(), 1U);
      EXPECT_EQ(allocator.WatermarkEvictions(), 1U);
      EXPECT_EQ(allocator.Count(), 0U);
      EXPECT_EQ(stats.protected_victims,
                with_stats ? (final_now < 129600 ? 1U : 0U) : 99U);
      EXPECT_EQ(stats.budget_exhausted, 0U);
      EXPECT_EQ(stats.no_candidate, 0U);
      EXPECT_EQ(stats.pin_skips, 0U);
      EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
    }
  }
}

TEST(SlabLifetimeWatermark, LongPinnedBandDoesNotStarveAnotherBand) {
  SlabAllocator allocator(Options(2050));
  for (uint64_t id = 1; id <= 2048; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, 0, 0, true));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2049, 1));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2049)});
  EXPECT_LE(allocator.WatermarkScanVisits(), 1024U);
  EXPECT_EQ(allocator.Count(), 2048U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, WinnerIsReplenishedBeforeNextCrossBandChoice) {
  SlabAllocator allocator(Options(4));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0, 302401));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 100, 0, 302500));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, 1, 2));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 3, 1, 3));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 907200;
  ASSERT_EQ(DiskLifetimeScore(config, 302401, 0, false, false, seconds), 1);
  ASSERT_EQ(DiskLifetimeScore(config, 302500, 0, false, false, seconds), 100);
  ASSERT_EQ(DiskLifetimeScore(config, 2, 1, false, false, seconds), 2);
  ASSERT_EQ(DiskLifetimeScore(config, 3, 1, false, false, seconds), 3);
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 3, config, clock, &evicted), 3U);
  EXPECT_EQ(evicted, (std::vector<BlockKey>{Key(1), Key(2), Key(3)}));
  EXPECT_EQ(allocator.WatermarkScanVisits(), 6U);
  EXPECT_TRUE(allocator.Contains(Key(100)));
}

TEST(SlabLifetimeWatermark, RechecksPayTheSameBudgetAndContinueNextTick) {
  SlabAllocator allocator(Options(8, 4));
  for (uint64_t id = 1; id <= 5; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, static_cast<uint8_t>(id - 1)));
  DiskLifetimeConfig config;
  config.max_reads = 4;
  config.read_bonus_sec = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  // A partial round may select only checked candidates, but every cached
  // comparison on the next choice must still pay the same tick budget.
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
  EXPECT_LT(allocator.WatermarkScanVisits(), 4U);
  const std::optional<BlockKey> untouched =
      SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 0, 4);
  ASSERT_TRUE(untouched.has_value());
  EXPECT_EQ(untouched.value_or(Key(0)), Key(5));
  EXPECT_EQ(Evict(allocator, 0, 2, config, clock, &evicted), 2U);
  EXPECT_EQ(evicted, (std::vector<BlockKey>{Key(1), Key(2), Key(3)}));
  EXPECT_EQ(allocator.WatermarkScanVisits(), 10U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, FourNonemptyBandsMakeProgressWithTinyVictimCap) {
  SlabAllocator allocator(Options(4, 4));
  for (uint64_t id = 1; id <= 4; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, static_cast<uint8_t>(id - 1)));
  DiskLifetimeConfig config;
  config.max_reads = 4;
  config.read_bonus_sec = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  for (unsigned tick = 0; tick < 4; ++tick) {
    const uint64_t before = allocator.WatermarkScanVisits();
    EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
    EXPECT_LE(allocator.WatermarkScanVisits() - before, 4U);
  }
  EXPECT_EQ(allocator.Count(), 0U);
  EXPECT_EQ(evicted.size(), 4U);
}

TEST(SlabLifetimeWatermark, PartialRoundNeverIgnoresColderCachedCandidate) {
  DiskLifetimeConfig config;
  config.max_reads = 4;
  config.base_sec = 1000;
  config.read_bonus_sec = 1000;
  config.keep_score_sec = 0;
  config.bucket_sec = 1000;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  SlabAllocator allocator(Options(8, 4));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0, 4001));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 100, 0, 4100));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 101, 1, 3100));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 102, 2, 2101));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, 3, 1002));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 3, 4, 3));
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 5000;
  std::vector<BlockKey> evicted;
  // After the first winner, reaching the visit cap while rechecking early
  // bands must not allow score 100 to bypass the still-cached scores 2/3.
  EXPECT_EQ(Evict(allocator, 0, 2, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_EQ(allocator.WatermarkScanVisits(), 8U);
  Evict(allocator, 0, 2, config, clock, &evicted);
  ASSERT_GE(evicted.size(), 2U);
  EXPECT_EQ(evicted[1], Key(2));
  EXPECT_TRUE(allocator.Contains(Key(100)));
}

TEST(SlabLifetimeWatermark, CandidateOnLastPaidVisitIsFirstNextTick) {
  SlabAllocator allocator(Options(4));
  for (uint64_t id = 1; id <= 4; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, 0, 0, id != 4));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 0U);
  EXPECT_EQ(allocator.WatermarkScanVisits(), 4U);
  const std::optional<BlockKey> cursor =
      SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 0, 0);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_EQ(cursor.value_or(Key(0)), Key(4));
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(4)});
  EXPECT_EQ(allocator.WatermarkScanVisits(), 5U);
}

TEST(SlabLifetimeWatermark, SupportedReadCapsIncludeHighestBand) {
  for (const uint32_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    SlabAllocator allocator(Options(2, max_reads));
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0, 0, true));
    ASSERT_NO_FATAL_FAILURE(
        Seed(allocator, 2, static_cast<uint8_t>(max_reads)));
    DiskLifetimeConfig config;
    config.max_reads = max_reads;
    config.read_bonus_sec = 0;
    std::string error;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [] { return 0; });
    std::vector<BlockKey> evicted;
    EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2)});
    EXPECT_EQ(allocator.WatermarkScanVisits(), 2U);
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

TEST(SlabLifetimeWatermark, NextBandSurvivesBudgetEndingInsideWideRound) {
  SlabAllocator allocator(Options(16, 63));
  for (uint64_t band = 0; band < 8; ++band) {
    ASSERT_NO_FATAL_FAILURE(
        Seed(allocator, band * 2 + 1, static_cast<uint8_t>(band), 0, true));
    ASSERT_NO_FATAL_FAILURE(
        Seed(allocator, band * 2 + 2, static_cast<uint8_t>(band), 0, true));
  }
  DiskLifetimeConfig config;
  config.max_reads = 63;
  config.read_bonus_sec = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  for (unsigned tick = 0; tick < 2; ++tick) {
    EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 0U);
    EXPECT_EQ(allocator.WatermarkScanVisits(), (tick + 1) * 4U);
    for (uint32_t band = 0; band < 8; ++band) {
      const std::optional<BlockKey> cursor =
          SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 0, band);
      ASSERT_TRUE(cursor.has_value());
      const bool checked = band < (tick + 1) * 4;
      EXPECT_EQ(cursor.value_or(Key(0)), Key(band * 2 + (checked ? 2 : 1)));
    }
  }
  EXPECT_TRUE(evicted.empty());
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, ComparedTiesUseAnchorThenResidentGeneration) {
  DiskLifetimeConfig config;
  config.read_bonus_sec = 0;
  SlabAllocator allocator(Options(4));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 3, 0));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 1));
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});

  const DiskLifetimeConfig default_config;
  SlabAllocator anchored(Options(4));
  ASSERT_NO_FATAL_FAILURE(Seed(anchored, 1, 0, 302400));
  ASSERT_NO_FATAL_FAILURE(Seed(anchored, 2, 1));
  uint64_t seconds = 0;
  const DiskLifetimeClock later(DiskLifetimeHorizon(default_config),
                                [&seconds] { return seconds; });
  seconds = 1000000;
  evicted.clear();
  EXPECT_EQ(Evict(anchored, 0, 1, default_config, later, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(2)});
}

TEST(SlabLifetimeWatermark, CrossClassScoringDoesNotClearPeerExtent) {
  SlabAllocator::Options options = Options(8);
  options.num_extents = 2;
  unsigned slot_calls = 0;
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++slot_calls;
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, 0, 0, false, 4096));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, 1, 0, false, 8192));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 8192, 256, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_EQ(allocator.UsedBytes(), 8192U);
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_EQ(slot_calls, 1U);
}

TEST(SlabLifetimeWatermark, CountByteAndZeroLimitsStopImmediately) {
  SlabAllocator allocator(Options(8));
  EXPECT_FALSE(SlabAllocatorTestPeer::ObserveSlotEvict(allocator, [] {
    ADD_FAILURE() << "No persistence callback exists to observe";
  }));
  for (uint64_t id = 1; id <= 4; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  stats.age.buckets[0] = 99;
  stats.age.total_sec = 99;
  stats.age.history_unknown = 99;
  stats.age.recovered_boot_age_buckets[0] = 99;
  stats.age.recovered_boot_age_total_sec = 99;
  stats.score.nonnegative_buckets[0] = 99;
  stats.score.negative_magnitude_buckets[0] = 99;
  stats.score.nonnegative_total_sec = 99;
  stats.score.negative_magnitude_total_sec = 99;
  EXPECT_EQ(Evict(allocator, 0, 0, config, clock, &evicted, &stats), 0U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  EXPECT_EQ(stats.age.recovered_boot_age_buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.recovered_boot_age_total_sec, 0U);
  ExpectEmptyScore(stats.score);
  EXPECT_EQ(allocator.WatermarkScanVisits(), 0U);
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
  EXPECT_EQ(allocator.Count(), 3U);
  // The final complete slot may cross a non-aligned byte target by <4096.
  EXPECT_EQ(Evict(allocator, 2 * 4096 + 1, 8, config, clock, &evicted), 1U);
  EXPECT_EQ(allocator.UsedBytes(), 2 * 4096U);
  const uint64_t visits = allocator.WatermarkScanVisits();
  EXPECT_EQ(Evict(allocator, 2 * 4096 + 1, 8, config, clock, &evicted), 0U);
  EXPECT_EQ(allocator.WatermarkScanVisits(), visits);
  EXPECT_EQ(evicted, (std::vector<BlockKey>{Key(1), Key(2)}));
}

TEST(SlabLifetimeWatermark, DefaultVictimLimitIsNotRefilledPerCandidate) {
  SlabAllocator allocator(Options(600));
  for (uint64_t id = 1; id <= 600; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted), 256U);
  EXPECT_EQ(evicted.size(), 256U);
  EXPECT_EQ(allocator.Count(), 344U);
  EXPECT_LE(allocator.WatermarkScanVisits(), 1024U);
}

TEST(SlabLifetimeWatermark, FullPinnedTraversalStopsWithoutRevisiting) {
  SlabAllocator allocator(Options(16));
  for (uint64_t id = 1; id <= 12; ++id)
    ASSERT_NO_FATAL_FAILURE(
        Seed(allocator, id, static_cast<uint8_t>(id % 3), 0, true));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted, &stats), 0U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  ExpectEmptyScore(stats.score);
  EXPECT_EQ(allocator.WatermarkScanVisits(), 12U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), 12U);
  ASSERT_TRUE(allocator.Unpin(Key(11)));
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 0U);
  EXPECT_LE(allocator.WatermarkScanVisits(), 16U);
  for (unsigned tick = 0; tick < 3 && evicted.empty(); ++tick)
    Evict(allocator, 0, 1, config, clock, &evicted);
  ASSERT_EQ(evicted.size(), 1U);
  EXPECT_EQ(evicted[0], Key(11));
}

TEST(SlabLifetimeWatermark, PinnedPrefixContinuationAndDeletionRepairCursor) {
  SlabAllocator allocator(Options(8));
  for (uint64_t id = 1; id <= 6; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, 0, 0, id != 6));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 0U);
  EXPECT_EQ(allocator.WatermarkScanVisits(), 4U);
  const std::optional<BlockKey> cursor =
      SlabAllocatorTestPeer::LifetimeCursorKey(allocator, 0, 0);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_EQ(cursor.value_or(Key(0)), Key(5));
  ASSERT_TRUE(allocator.Unpin(Key(5)));
  EXPECT_EQ(allocator.Remove(Key(5)), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(6)});
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 7));
  for (unsigned tick = 0; tick < 3 && allocator.Contains(Key(7)); ++tick)
    Evict(allocator, 0, 1, config, clock, &evicted);
  EXPECT_FALSE(allocator.Contains(Key(7)));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, HeatMigrationRepairsBothIndexesBetweenTicks) {
  SlabAllocator allocator(Options(8));
  for (uint64_t id = 1; id <= 5; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, 0, 0, id != 5));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 1; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(5), 5,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(5)});
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, PersistenceFailureDoesNotFreeOrRetry) {
  unsigned attempts = 0;
  unsigned observed = 0;
  SlabAllocator::Options options = Options(4);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++attempts;
    return false;
  };
  SlabAllocator allocator(options);
  ASSERT_TRUE(
      SlabAllocatorTestPeer::ObserveSlotEvict(allocator, [&] { ++observed; }));
  for (uint64_t id = 1; id <= 3; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  stats.protected_victims = 99;
  stats.age.buckets[0] = 99;
  stats.age.total_sec = 99;
  stats.age.history_unknown = 99;
  stats.score.nonnegative_buckets[0] = 99;
  stats.score.negative_magnitude_buckets[0] = 99;
  stats.score.nonnegative_total_sec = 99;
  stats.score.negative_magnitude_total_sec = 99;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted, &stats), 0U);
  EXPECT_EQ(stats.protected_victims, 0U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  ExpectEmptyScore(stats.score);
  EXPECT_EQ(attempts, 1U);
  EXPECT_EQ(observed, 1U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), 3U);
  EXPECT_EQ(allocator.WatermarkEvictions(), 0U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark,
     PartialPersistenceFailureKeepsOnlyCompletedVictims) {
  unsigned attempts = 0;
  SlabAllocator::Options options = Options(4);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    return ++attempts == 1;
  };
  SlabAllocator allocator(options);
  for (uint64_t id = 1; id <= 3; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted, &stats), 1U);
  EXPECT_EQ(stats.protected_victims, 1U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 1U);
  EXPECT_EQ(stats.age.recovered_boot_age_buckets, (std::array<uint64_t, 8>{1}));
  EXPECT_EQ(stats.age.recovered_boot_age_total_sec, 0U);
  EXPECT_EQ(stats.score.nonnegative_buckets,
            (std::array<uint64_t, 8>{0, 0, 0, 0, 0, 1}));
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, 604800);
  EXPECT_EQ(stats.score.negative_magnitude_buckets,
            (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec, 0);
  EXPECT_EQ(attempts, 2U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_TRUE(allocator.Contains(Key(3)));
}

TEST(SlabLifetimeWatermark, LastResidentStillUsesOnlySlotInvalidation) {
  unsigned slot_calls = 0;
  unsigned extent_calls = 0;
  SlabAllocator::Options options = Options(2);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++slot_calls;
    return true;
  };
  options.on_extent_evict = [&](uint32_t, uint32_t, uint32_t) {
    ++extent_calls;
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 1, config, clock, &evicted), 1U);
  EXPECT_EQ(slot_calls, 1U);
  EXPECT_EQ(extent_calls, 0U);
  EXPECT_EQ(allocator.Count(), 0U);
}

TEST(SlabLifetimeWatermark, ExtentOnlyCallbackCannotForgetSlot) {
  unsigned extent_calls = 0;
  SlabAllocator::Options options = Options(4);
  options.on_extent_evict = [&](uint32_t, uint32_t, uint32_t) {
    ++extent_calls;
    return true;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted), 0U);
  EXPECT_EQ(extent_calls, 0U);
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_TRUE(evicted.empty());
}

TEST(SlabLifetimeWatermark, UncommittedResidentNeverBecomesCandidate) {
  SlabAllocator allocator(Options(4));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1));
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(2), 1, &ref, &evicted));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted), 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_EQ(allocator.Count(), 1U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeWatermark, HardClockStopsAfterCompletedInvalidation) {
  uint64_t seconds = 0;
  unsigned attempts = 0;
  SlabAllocator::Options options = Options(4);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++attempts;
    seconds = static_cast<uint64_t>(UINT32_MAX) + 1;
    return true;
  };
  SlabAllocator allocator(options);
  for (uint64_t id = 1; id <= 3; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  std::vector<BlockKey> evicted;
  SlabAllocatorTestPeer::LifetimeWatermarkStats stats;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted, &stats), 1U);
  EXPECT_EQ(stats.protected_victims, 1U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 1U);
  EXPECT_EQ(stats.score.nonnegative_buckets,
            (std::array<uint64_t, 8>{0, 0, 0, 0, 0, 1}));
  EXPECT_DOUBLE_EQ(stats.score.nonnegative_total_sec, 604800);
  EXPECT_EQ(stats.score.negative_magnitude_buckets,
            (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(stats.score.negative_magnitude_total_sec, 0);
  EXPECT_EQ(attempts, 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_EQ(allocator.Count(), 2U);
  seconds = 0;
  EXPECT_EQ(Evict(allocator, 0, 256, config, clock, &evicted, &stats), 0U);
  EXPECT_EQ(stats.protected_victims, 0U);
  EXPECT_EQ(stats.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(stats.age.total_sec, 0U);
  EXPECT_EQ(stats.age.history_unknown, 0U);
  ExpectEmptyScore(stats.score);
  EXPECT_EQ(attempts, 1U);
}

class LifetimeWatermarkStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_watermark_" +
         std::string(
             ::testing::UnitTest::GetInstance()->current_test_info()->name()));
    std::filesystem::remove_all(dir_);
    clock_ = std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(config_), [this] { return seconds_.load(); });
  }
  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  void Create(uint32_t slots = 8, bool lifetime = true) {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
    options.capacity_bytes = options.extent_bytes;
    options.slot_granularity = 4096;
    options.reclaim_interval_ms = 0;
    options.table_sync_ms = 0;
    if (lifetime) {
      options.lifetime_config = config_;
      options.lifetime_clock = clock_;
      options.lifetime_class_eviction = true;
    }
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  void SeedStore(uint64_t count, bool pin = false) {
    for (uint64_t id = 1; id <= count; ++id) {
      const std::string payload = "payload-" + std::to_string(id);
      ASSERT_EQ(store_->CacheWithResult(Key(id), payload.data(), payload.size())
                    .status,
                Status::kOk);
      if (pin) {
        ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(id)));
      }
    }
  }
  DiskLifetimeConfig config_;
  std::atomic<uint64_t> seconds_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::filesystem::path dir_;
  std::unique_ptr<DiskSlabStore> store_;
};

TEST_F(LifetimeWatermarkStoreTest,
       WatermarkUsesSlotRecordsAndCleansTenantBytes) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(SeedStore(4));
  DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 2 * 4096, 256);
  DiskSlabStoreTestPeer::Tick(*store_);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(stats.watermark_evictions, 2U);
  EXPECT_EQ(stats.watermark_slot_record_clears, 2U);
  EXPECT_EQ(stats.watermark_extent_clears, 0U);
  EXPECT_EQ(stats.watermark_freed_slot_bytes, 2 * 4096U);
  EXPECT_EQ(store_->EvictedBytes(), 18U);
  EXPECT_EQ(stats.reclaimed_slots, 0U);
  EXPECT_EQ(stats.lifetime_watermark_protected_victims, 2U);
  EXPECT_EQ(stats.lifetime_watermark_budget_exhausted, 0U);
  EXPECT_EQ(stats.lifetime_watermark_no_candidate, 0U);
  EXPECT_EQ(stats.rebalanced_extents, 0U);
  EXPECT_EQ(store_->Count(), 2U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 18U);
  for (uint64_t id = 3; id <= 4; ++id) {
    std::string value;
    EXPECT_EQ(store_->Range(Key(id), 0, 0, &value), Status::kOk);
    EXPECT_EQ(value, "payload-" + std::to_string(id));
  }
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
  store_.reset();
  ASSERT_NO_FATAL_FAILURE(Create());
  EXPECT_EQ(store_->Count(), 2U);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_FALSE(store_->IsCached(Key(2)));
  std::string value;
  EXPECT_EQ(store_->Range(Key(3), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "payload-3");
}

TEST_F(LifetimeWatermarkStoreTest, ValidatedSnapshotSetsActualWatermarks) {
  config_.high_pct = 51;
  config_.low_pct = 27;
  ASSERT_NO_FATAL_FAILURE(Create());
  const std::pair<uint64_t, uint64_t> thresholds =
      DiskSlabStoreTestPeer::Watermarks(*store_);
  EXPECT_EQ(thresholds.first, 8 * 4096U / 100 * 51);
  EXPECT_EQ(thresholds.second, 8 * 4096U / 100 * 27);
  ASSERT_NO_FATAL_FAILURE(SeedStore(5));
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->GetStats().watermark_evictions, 3U);
  EXPECT_EQ(store_->Count(), 2U);
}

TEST_F(LifetimeWatermarkStoreTest, PayloadAndSlotBytesConserveInBothPolicies) {
  for (const bool lifetime : {false, true}) {
    SCOPED_TRACE(lifetime ? "lifetime" : "legacy");
    store_.reset();
    std::filesystem::remove_all(dir_);
    ASSERT_NO_FATAL_FAILURE(Create(8, lifetime));
    const std::vector<size_t> sizes = {1, 100, 2048, 4096};
    uint64_t total = 0;
    for (size_t index = 0; index < sizes.size(); ++index) {
      const std::string payload(sizes[index], 'x');
      ASSERT_EQ(store_->Cache(Key(index + 1), payload.data(), payload.size()),
                Status::kOk);
      total += payload.size();
    }
    ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(4)));
    DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 2 * 4096, 256);
    DiskSlabStoreTestPeer::Tick(*store_);
    ASSERT_EQ(store_->Count(), 2U);
    EXPECT_TRUE(store_->IsCached(Key(4)));
    uint64_t removed = 0;
    for (size_t index = 0; index < sizes.size(); ++index) {
      if (!store_->IsCached(Key(index + 1))) removed += sizes[index];
    }
    EXPECT_GT(removed, 0U);
    const DiskSlabStore::Stats first = store_->GetStats();
    EXPECT_EQ(first.watermark_freed_payload_bytes, removed);
    EXPECT_EQ(store_->EvictedBytes(), removed);
    EXPECT_EQ(first.payload_bytes + removed, total);
    EXPECT_EQ(store_->TenantUsedBytes(7), first.payload_bytes);
    EXPECT_EQ(first.watermark_freed_slot_bytes, 2 * 4096U);
    EXPECT_EQ(first.watermark_slot_removals, 2U);
    DiskSlabStoreTestPeer::Tick(*store_);
    EXPECT_EQ(store_->GetStats().watermark_freed_payload_bytes, removed);
    EXPECT_EQ(store_->EvictedBytes(), removed);
    ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(4)));
    DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 0, 256);
    DiskSlabStoreTestPeer::Tick(*store_);
    const DiskSlabStore::Stats drained = store_->GetStats();
    EXPECT_EQ(store_->Count(), 0U);
    EXPECT_EQ(drained.payload_bytes, 0U);
    EXPECT_EQ(drained.watermark_freed_payload_bytes, total);
    EXPECT_EQ(store_->EvictedBytes(), total);
    EXPECT_EQ(drained.watermark_freed_slot_bytes, 4 * 4096U);
    EXPECT_EQ(drained.watermark_slot_removals, 4U);
    DiskSlabStoreTestPeer::Tick(*store_);
    EXPECT_EQ(store_->EvictedBytes(), total);
    EXPECT_EQ(store_->GetStats().watermark_freed_payload_bytes, total);
  }
}

TEST_F(LifetimeWatermarkStoreTest, ZeroHighDisablesOnlyWatermarkPath) {
  config_.high_pct = 0;
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(SeedStore(8));
  EXPECT_EQ(DiskSlabStoreTestPeer::Watermarks(*store_).first, 0U);
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->Count(), 8U);
  EXPECT_EQ(store_->GetStats().watermark_evictions, 0U);
  EXPECT_EQ(store_->GetStats().watermark_scan_visits, 0U);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(LifetimeWatermarkStoreTest,
       FullTraversalAndExactBudgetHaveDistinctReasons) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(SeedStore(4, true));
  DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 0, 1);
  DiskSlabStoreTestPeer::Tick(*store_);
  const DiskSlabStore::Stats bounded = store_->GetStats();
  EXPECT_EQ(bounded.watermark_scan_visits, 4U);
  EXPECT_EQ(bounded.lifetime_watermark_budget_exhausted, 1U);
  EXPECT_EQ(bounded.lifetime_watermark_no_candidate, 0U);
  EXPECT_EQ(bounded.lifetime_watermark_pin_skips, 4U);
  DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 0, 2);
  DiskSlabStoreTestPeer::Tick(*store_);
  const DiskSlabStore::Stats complete = store_->GetStats();
  EXPECT_EQ(complete.watermark_scan_visits, 8U);
  EXPECT_EQ(complete.lifetime_watermark_budget_exhausted, 1U);
  EXPECT_EQ(complete.lifetime_watermark_no_candidate, 1U);
  EXPECT_EQ(complete.lifetime_watermark_pin_skips, 8U);
  EXPECT_EQ(complete.metadata_io_errors, 0U);
  EXPECT_EQ(complete.watermark_evictions, 0U);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(LifetimeWatermarkStoreTest, ConcurrentReadsKeepPinnedSurvivorByteExact) {
  ASSERT_NO_FATAL_FAILURE(Create(64));
  ASSERT_NO_FATAL_FAILURE(SeedStore(64));
  ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(64)));
  DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 4096, 1);
  std::atomic<bool> started{false};
  std::atomic<unsigned> failures{0};
  std::thread reader([&] {
    started.store(true);
    for (unsigned read = 0; read < 256; ++read) {
      std::string value;
      DiskResidentIdentity identity;
      if (store_->RangeWithIdentity(Key(64), 0, 0, &value, &identity) !=
              Status::kOk ||
          value != "payload-64" || !identity.valid()) {
        failures.fetch_add(1);
      } else {
        store_->NotifyLifetimeReadSuccess(Key(64), identity, value.size());
        const DiskSlabStore::Stats snapshot = store_->GetStats();
        uint64_t scored = 0;
        uint64_t aged = snapshot.lifetime_watermark_age.history_unknown;
        for (const uint64_t count :
             snapshot.lifetime_watermark_score.nonnegative_buckets) {
          scored += count;
        }
        for (const uint64_t count :
             snapshot.lifetime_watermark_score.negative_magnitude_buckets) {
          scored += count;
        }
        for (const uint64_t count : snapshot.lifetime_watermark_age.buckets) {
          aged += count;
        }
        // Both counters share D in this snapshot; allocator-wide values
        // gathered earlier by GetStats are not part of this live equality.
        if (scored != aged) failures.fetch_add(1);
      }
    }
  });
  while (!started.load()) std::this_thread::yield();
  for (unsigned tick = 0; tick < 63; ++tick)
    DiskSlabStoreTestPeer::Tick(*store_);
  reader.join();
  EXPECT_EQ(failures.load(), 0U);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_TRUE(store_->IsCached(Key(64)));
  EXPECT_EQ(store_->GetStats().watermark_evictions, 63U);
  EXPECT_EQ(store_->GetStats().lifetime_watermark_protected_victims, 63U);
  const SlabAllocator::LifetimeDonorAgeStats age =
      store_->GetStats().lifetime_watermark_age;
  EXPECT_EQ(age.buckets, (std::array<uint64_t, 8>{63}));
  EXPECT_EQ(age.total_sec, 0U);
  EXPECT_EQ(age.history_unknown, 0U);
  const SlabAllocator::LifetimeDonorScoreStats score =
      store_->GetStats().lifetime_watermark_score;
  EXPECT_EQ(score.nonnegative_buckets,
            (std::array<uint64_t, 8>{0, 0, 0, 0, 0, 0, 63}));
  EXPECT_DOUBLE_EQ(score.nonnegative_total_sec, 114307200);
  EXPECT_EQ(score.negative_magnitude_buckets, (std::array<uint64_t, 8>{}));
  EXPECT_DOUBLE_EQ(score.negative_magnitude_total_sec, 0);
  EXPECT_EQ(store_->GetStats().lifetime_reads.successful, 256U);
  EXPECT_EQ(store_->GetStats().lifetime_reads.applied, 256U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 10U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeWatermarkStoreTest, ActiveLatchContinuesBelowHighWithoutNewPut) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(SeedStore(4));
  DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 3 * 4096, 4096, 1);
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->Count(), 3U);
  EXPECT_EQ(store_->GetStats().watermark_active, 1U);
  EXPECT_EQ(DiskSlabStoreTestPeer::NewPutBytes(*store_), 0U);
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->Count(), 2U);
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_EQ(store_->GetStats().watermark_evictions, 3U);
  EXPECT_EQ(store_->GetStats().watermark_active, 0U);
  const uint64_t visits = store_->GetStats().watermark_scan_visits;
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_EQ(store_->GetStats().watermark_scan_visits, visits);
  EXPECT_EQ(store_->Count(), 1U);
}

TEST_F(LifetimeWatermarkStoreTest, PinnedQuietTicksResumeAndKeepOriginalBytes) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(SeedStore(4, true));
  DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 3 * 4096, 1);
  for (unsigned tick = 0; tick < 3; ++tick) {
    DiskSlabStoreTestPeer::Tick(*store_);
    EXPECT_EQ(store_->Count(), 4U);
    EXPECT_EQ(store_->GetStats().watermark_active, 1U);
    EXPECT_EQ(store_->GetStats().watermark_freed_payload_bytes, 0U);
    EXPECT_EQ(store_->EvictedBytes(), 0U);
  }
  ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(4)));
  for (unsigned tick = 0; tick < 3 && store_->IsCached(Key(4)); ++tick)
    DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_FALSE(store_->IsCached(Key(4)));
  EXPECT_EQ(store_->GetStats().watermark_evictions, 1U);
  EXPECT_EQ(store_->GetStats().watermark_slot_record_clears, 1U);
  EXPECT_TRUE(store_->Healthy());
  std::string value;
  EXPECT_EQ(store_->Range(Key(2), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "payload-2");
}

TEST_F(LifetimeWatermarkStoreTest, RealMetadataFailureDoesNotReleaseResident) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(SeedStore(4));
  DiskSlabStoreTestPeer::ConfigureWatermark(*store_, 1, 0, 256);
  DiskSlabStoreTestPeer::FailInvalidation(*store_);
  DiskSlabStoreTestPeer::Tick(*store_);
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(store_->Count(), 4U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 36U);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(stats.watermark_evictions, 0U);
  EXPECT_EQ(stats.watermark_slot_record_clears, 0U);
  EXPECT_EQ(stats.watermark_freed_slot_bytes, 0U);
  EXPECT_EQ(stats.watermark_freed_payload_bytes, 0U);
  EXPECT_EQ(store_->EvictedBytes(), 0U);
  EXPECT_EQ(stats.payload_bytes, 36U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 36U);
  EXPECT_EQ(stats.metadata_io_errors, 1U);
  EXPECT_EQ(stats.lifetime_watermark_budget_exhausted, 0U);
  EXPECT_EQ(stats.lifetime_watermark_no_candidate, 0U);
  EXPECT_EQ(stats.lifetime_watermark_protected_victims, 0U);
  EXPECT_EQ(stats.reclaimed_slots, 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

}  // namespace
}  // namespace dfkv
