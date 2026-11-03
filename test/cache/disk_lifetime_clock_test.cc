#include "cache/disk_lifetime_clock.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "cache/disk_lifetime_math.h"

namespace dfkv {
namespace {

TEST(DiskLifetimeClock, OriginIsCapturedOnceAndZeroIsValid) {
  uint64_t seconds = 10000;
  unsigned samples = 0;
  const DiskLifetimeClock clock(100, [&] {
    ++samples;
    return seconds;
  });
  EXPECT_EQ(samples, 1U);
  uint32_t offset = 99;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 0U);
  seconds += 17;
  // Both simulated disk consumers use the same clock, not fresh origins.
  const DiskLifetimeClock& disk_a = clock;
  const DiskLifetimeClock& disk_b = clock;
  ASSERT_TRUE(disk_a.TryNow(&offset));
  EXPECT_EQ(offset, 17U);
  ASSERT_TRUE(disk_b.TryNow(&offset));
  EXPECT_EQ(offset, 17U);
  EXPECT_EQ(samples, 4U);
}

TEST(DiskLifetimeClock, LargeAbsoluteOriginIsNotTruncated) {
  uint64_t seconds = UINT64_MAX - 10;
  const DiskLifetimeClock clock(100, [&] { return seconds; });
  uint32_t offset = 99;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 0U);
  seconds = UINT64_MAX;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 10U);
}

TEST(DiskLifetimeClock, ThresholdBeforeEqualAndAfterNeverWraps) {
  const DiskLifetimeConfig config;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  const uint64_t horizon = DiskLifetimeHorizon(config);
  const uint64_t limit = uint64_t{UINT32_MAX} - horizon;
  uint64_t seconds = 10000;
  const uint64_t origin = seconds;
  const DiskLifetimeClock clock(horizon, [&] { return seconds; });
  uint32_t offset = 99;
  seconds = origin + limit - 1;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, limit - 1);
  seconds = origin + limit;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, limit);
  seconds = origin + limit + 1;
  EXPECT_FALSE(clock.TryNow(&offset));
  EXPECT_EQ(offset, limit);  // Failed encoding must not publish a new anchor.
  seconds = origin + limit;
  EXPECT_FALSE(clock.TryNow(&offset));  // No revival after a source rollback.
  EXPECT_EQ(offset, limit);
  seconds = UINT64_MAX;
  EXPECT_FALSE(clock.TryNow(&offset));
  EXPECT_EQ(offset, limit);
}

TEST(DiskLifetimeClock, WideElapsedCannotWrapBackToZero) {
  uint64_t seconds = 100;
  const DiskLifetimeClock clock(100, [&] { return seconds; });
  seconds += uint64_t{1} << 32;
  uint32_t offset = 99;
  EXPECT_FALSE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 99U);
}

TEST(DiskLifetimeClock, AppliedOffsetFeedsMathNotCompletionTime) {
  const DiskLifetimeConfig config;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = uint64_t{UINT32_MAX} + 1000;
  const uint64_t origin = seconds;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  uint32_t put_at = 99;
  ASSERT_TRUE(clock.TryNow(&put_at));
  EXPECT_EQ(put_at, 0U);
  seconds = origin + 179;
  uint32_t completed_at = 0;
  ASSERT_TRUE(clock.TryNow(&completed_at));
  EXPECT_TRUE(DiskLifetimeEarlyRead(config, put_at, completed_at));
  seconds = origin + 181;
  uint32_t applied_at = 0;
  ASSERT_TRUE(clock.TryNow(&applied_at));
  EXPECT_FALSE(DiskLifetimeEarlyRead(config, put_at, applied_at));
  EXPECT_EQ(DiskLifetimeScore(config, put_at, 0, false, true, applied_at),
            604754);
  // Only checks the time dependency; no resident mutation or heat is installed.
}

TEST(DiskLifetimeClock, ValidHorizonExtremesKeepFullProtectionHeadroom) {
  for (const uint64_t horizon : {uint64_t{1}, uint64_t{INT32_MAX}}) {
    SCOPED_TRACE(horizon);
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(horizon, [&] { return seconds; });
    seconds = uint64_t{UINT32_MAX} - horizon;
    uint32_t offset = 0;
    ASSERT_TRUE(clock.TryNow(&offset));
    EXPECT_EQ(uint64_t{offset} + horizon, UINT32_MAX);
    ++seconds;
    EXPECT_FALSE(clock.TryNow(&offset));
    EXPECT_EQ(uint64_t{offset} + horizon, UINT32_MAX);
  }
}

TEST(DiskLifetimeClock, InvalidHorizonAndEmptySourcePreserveOutput) {
  for (const uint64_t horizon :
       {uint64_t{0}, uint64_t{INT32_MAX} + 1, uint64_t{UINT64_MAX}}) {
    const DiskLifetimeClock clock(horizon, [] { return uint64_t{10}; });
    uint32_t offset = 99;
    EXPECT_FALSE(clock.TryNow(&offset));
    EXPECT_EQ(offset, 99U);
  }
  const DiskLifetimeClock empty(100, DiskLifetimeClock::NowFn{});
  uint32_t offset = 99;
  EXPECT_FALSE(empty.TryNow(&offset));
  EXPECT_EQ(offset, 99U);
}

TEST(DiskLifetimeClock, RegressingInjectedSourceCannotMoveOffsetBack) {
  uint64_t seconds = 100;
  const DiskLifetimeClock clock(100, [&] { return seconds; });
  uint32_t offset = 99;
  seconds = 90;  // Before origin: saturate the subtraction at zero.
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 0U);
  seconds = 120;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 20U);
  seconds = 110;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 20U);
  seconds = 1;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 20U);
  seconds = 121;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 21U);
}

TEST(DiskLifetimeClock, NewInstanceResetsOriginNotOldInstance) {
  uint64_t seconds = 0;
  const DiskLifetimeClock old(100, [&] { return seconds; });
  seconds = UINT32_MAX;
  uint32_t offset = 99;
  EXPECT_FALSE(old.TryNow(&offset));
  const DiskLifetimeClock restarted(100, [&] { return seconds; });
  ASSERT_TRUE(restarted.TryNow(&offset));
  EXPECT_EQ(offset, 0U);
  EXPECT_FALSE(old.TryNow(&offset));
  // This models encoding reset only, not clean server shutdown/recovery.
}

TEST(DiskLifetimeClock, ConcurrentConsumersShareMonotonicFloor) {
  std::atomic<uint64_t> seconds{100};
  const DiskLifetimeClock clock(
      100, [&] { return seconds.load(std::memory_order_relaxed); });
  seconds.store(117, std::memory_order_relaxed);
  std::atomic<unsigned> failures{0};
  std::vector<std::thread> readers;
  for (unsigned i = 0; i < 8; ++i) {
    readers.emplace_back([&] {
      for (unsigned sample = 0; sample < 1000; ++sample) {
        uint32_t offset = 99;
        if (!clock.TryNow(&offset) || offset != 17) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (std::thread& reader : readers) reader.join();
  EXPECT_EQ(failures.load(std::memory_order_relaxed), 0U);
  seconds.store(101, std::memory_order_relaxed);
  uint32_t offset = 0;
  ASSERT_TRUE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 17U);
}

TEST(DiskLifetimeClock, DelayedOldSampleCannotUndoAdvanceOrExhaustion) {
  for (const bool exhaust : {false, true}) {
    SCOPED_TRACE(exhaust);
    std::atomic<uint64_t> seconds{0};
    std::atomic<bool> delay_next{false};
    std::promise<void> captured;
    const std::future<void> sampled = captured.get_future();
    std::promise<void> release;
    const std::future<void> released = release.get_future();
    const DiskLifetimeClock clock(100, [&] {
      const uint64_t sample = seconds.load(std::memory_order_relaxed);
      if (delay_next.exchange(false, std::memory_order_relaxed)) {
        captured.set_value();
        released.wait();
      }
      return sample;
    });
    seconds.store(10, std::memory_order_relaxed);
    delay_next.store(true, std::memory_order_relaxed);
    bool late_result = false;
    uint32_t late_offset = 99;
    std::thread old_reader([&] { late_result = clock.TryNow(&late_offset); });
    const bool paused =
        sampled.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    EXPECT_TRUE(paused);
    if (paused) {
      const uint64_t newer = exhaust ? uint64_t{UINT32_MAX} - 100 + 1 : 20;
      seconds.store(newer, std::memory_order_relaxed);
      uint32_t current = 99;
      EXPECT_EQ(clock.TryNow(&current), !exhaust);
      EXPECT_EQ(current, exhaust ? 99U : 20U);
    }
    // Always unblock and join, including when the barrier expectation failed.
    release.set_value();
    old_reader.join();
    if (paused) {
      EXPECT_EQ(late_result, !exhaust);
      EXPECT_EQ(late_offset, exhaust ? 99U : 20U);
    }
  }
}

TEST(DiskLifetimeClock, DefaultSourceWorksWithoutEnvironmentOrWallClock) {
  const DiskLifetimeClock clock(100);
  uint32_t before = 0;
  uint32_t after = 0;
  ASSERT_TRUE(clock.TryNow(&before));
  ASSERT_TRUE(clock.TryNow(&after));
  EXPECT_GE(after, before);
}

}  // namespace
}  // namespace dfkv
