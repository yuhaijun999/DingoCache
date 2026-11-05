#include "cache/disk_lifetime_state.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

#include "cache/disk_lifetime_math.h"

namespace dfkv {
namespace {

TEST(DiskLifetimeState, NewCommitArmsWriteAtValidZero) {
  const DiskLifetimeConfig config;
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  EXPECT_EQ(state.anchor, 0U);
  EXPECT_EQ(state.put_at, 0U);
  EXPECT_EQ(state.hstate, 1U);
  EXPECT_EQ(state.actual_reads, 0U);
  EXPECT_FALSE(state.history_unknown);
  EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 99999),
            DiskLifetimeReadClass::kUnread);
  EXPECT_EQ(DiskLifetimeScore(config, state.anchor, state.scoring_reads(),
                              state.first_early_read(), state.write_armed(), 1),
            1814400);
}

TEST(DiskLifetimeState, FirstReadUsesApplicationTimeAtProtectionBoundary) {
  const DiskLifetimeConfig config;
  for (const uint32_t elapsed : {0U, 179U, 180U, 181U}) {
    SCOPED_TRACE(elapsed);
    DiskLifetimeState state = NewDiskLifetimeState(10);
    ASSERT_TRUE(ApplyDiskLifetimeRead(config, 10 + elapsed, &state));
    EXPECT_EQ(state.anchor, 10 + elapsed);
    EXPECT_EQ(state.put_at, 10U);
    EXPECT_FALSE(state.write_armed());
    EXPECT_EQ(state.first_early_read(), elapsed < 180);
    EXPECT_EQ(state.scoring_reads(), 1U);
    EXPECT_EQ(state.actual_reads, 1U);
    EXPECT_EQ(state.hstate, elapsed < 180 ? 6U : 4U);
  }
}

TEST(DiskLifetimeState, FirstReadRewardIsNeverRecomputed) {
  const DiskLifetimeConfig config;
  for (const uint32_t first : {179U, 181U}) {
    DiskLifetimeState state = NewDiskLifetimeState(0);
    ASSERT_TRUE(ApplyDiskLifetimeRead(config, first, &state));
    ASSERT_TRUE(ApplyDiskLifetimeRead(config, 1000, &state));
    EXPECT_EQ(state.first_early_read(), first == 179);
    EXPECT_EQ(state.anchor, 1000U);
    EXPECT_EQ(state.put_at, 0U);
    EXPECT_EQ(state.actual_reads, 2U);
  }
}

TEST(DiskLifetimeState, ScoringAndBusinessCountsSaturateIndependently) {
  for (const uint64_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    DiskLifetimeConfig config;
    config.max_reads = max_reads;
    config.bucket_sec = 400000;
    config.bucket_count = 64;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(config, nullptr));
    DiskLifetimeState state = NewDiskLifetimeState(0);
    for (uint32_t second = 1; second <= 100; ++second) {
      ASSERT_TRUE(ApplyDiskLifetimeRead(config, second, &state));
      EXPECT_EQ(state.anchor, second);  // Even a capped band must refresh.
      EXPECT_EQ(state.scoring_reads(), second < 63 ? second : 63U);
      EXPECT_EQ(state.actual_reads, second < 3 ? second : 3U);
      EXPECT_EQ(DiskLifetimeBand(config, state.scoring_reads(),
                                 state.first_early_read()),
                second + 1 < max_reads ? second + 1 : max_reads);
    }
    EXPECT_EQ(state.hstate, 254U);  // n=63, fw=1, wa=0; n+fw must not wrap.
  }
}

TEST(DiskLifetimeState, RecoveredHistoryNeverEarnsEarlyReadOrRClass) {
  const DiskLifetimeConfig config;
  for (const uint8_t baseline : {1, 2}) {
    DiskLifetimeState state = RecoveredDiskLifetimeState(baseline);
    EXPECT_EQ(state.anchor, 0U);
    EXPECT_EQ(state.scoring_reads(), baseline);
    EXPECT_EQ(state.actual_reads, 0U);
    EXPECT_TRUE(state.history_unknown);
    EXPECT_FALSE(state.write_armed());
    EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 0),
              DiskLifetimeReadClass::kUnread);
    for (uint32_t read = 1; read <= 4; ++read) {
      ASSERT_TRUE(ApplyDiskLifetimeRead(config, 0, &state));
      EXPECT_EQ(state.actual_reads, read < 3 ? read : 3U);
      EXPECT_EQ(state.scoring_reads(), baseline + read);
      EXPECT_FALSE(state.first_early_read());
      EXPECT_TRUE(state.history_unknown);
      EXPECT_EQ(
          ClassifyDiskLifetimeRead(config, state, 0),
          read == 1 ? DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow
          : read == 2
              ? DiskLifetimeReadClass::kReadTwiceOutsideNewWriteWindow
              : DiskLifetimeReadClass::kReadThreeOrMoreOutsideNewWriteWindow);
    }
  }
}

TEST(DiskLifetimeState, RExpiryUsesOriginalPutNotLastRead) {
  const DiskLifetimeConfig config;
  DiskLifetimeState state = NewDiskLifetimeState(0);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 100, &state));
  EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 180),
            DiskLifetimeReadClass::kReadWithinNewWriteWindow);
  EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 181),
            DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
  EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 280),
            DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 280, &state));
  EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 280),
            DiskLifetimeReadClass::kReadTwiceOutsideNewWriteWindow);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 280, &state));
  EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 280),
            DiskLifetimeReadClass::kReadThreeOrMoreOutsideNewWriteWindow);
}

TEST(DiskLifetimeState, BackwardApplicationIsRejectedWithoutMutation) {
  const DiskLifetimeConfig config;
  DiskLifetimeState state = NewDiskLifetimeState(10);
  EXPECT_FALSE(ApplyDiskLifetimeRead(config, 9, &state));
  EXPECT_EQ(state.hstate, 1U);
  EXPECT_EQ(state.anchor, 10U);
  EXPECT_EQ(state.actual_reads, 0U);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 191, &state));
  const DiskLifetimeState before = state;
  EXPECT_FALSE(ApplyDiskLifetimeRead(config, 189, &state));
  EXPECT_EQ(state.anchor, before.anchor);
  EXPECT_EQ(state.hstate, before.hstate);
  EXPECT_EQ(state.actual_reads, before.actual_reads);
  EXPECT_EQ(state.put_at, before.put_at);
  EXPECT_EQ(state.history_unknown, before.history_unknown);
  // A read completed at 179 but first APPLIED at 181 earns no fw. Passing
  // completed_at here is invalid; another logical read at the same applied
  // second still counts instead of being mistaken for a duplicate completion.
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 191, &state));
  EXPECT_FALSE(state.first_early_read());
  EXPECT_EQ(state.actual_reads, 2U);
}

TEST(DiskLifetimeState, WideClassificationDoesNotWrapAtUint32Edge) {
  const DiskLifetimeConfig config;
  // Pure arithmetic boundary only: the production clock must enforce the
  // stricter UINT32_MAX-H admission limit before it ever calls this helper.
  const uint32_t edge = std::numeric_limits<uint32_t>::max();
  DiskLifetimeState state = NewDiskLifetimeState(edge - 100);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, edge, &state));
  EXPECT_EQ(state.anchor, edge);
  EXPECT_EQ(
      ClassifyDiskLifetimeRead(config, state, static_cast<uint64_t>(edge) + 80),
      DiskLifetimeReadClass::kReadWithinNewWriteWindow);
  EXPECT_EQ(
      ClassifyDiskLifetimeRead(config, state, static_cast<uint64_t>(edge) + 81),
      DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
}

TEST(DiskLifetimeState, ConfiguredProtectionWindowControlsBothBoundaries) {
  for (const uint32_t protect : {1U, 7U}) {
    DiskLifetimeConfig config;
    config.protect_sec = protect;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(config, nullptr));
    for (const uint32_t elapsed : {protect - 1, protect, protect + 1}) {
      DiskLifetimeState state = NewDiskLifetimeState(10);
      ASSERT_TRUE(ApplyDiskLifetimeRead(config, 10 + elapsed, &state));
      EXPECT_EQ(state.first_early_read(), elapsed < protect);
      EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 10 + elapsed),
                elapsed <= protect
                    ? DiskLifetimeReadClass::kReadWithinNewWriteWindow
                    : DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
    }
  }
}

TEST(DiskLifetimeState, FirstEarlyReadWithdrawsBoostAndUsesExistingScoreMath) {
  const DiskLifetimeConfig config;
  DiskLifetimeState state = NewDiskLifetimeState(0);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 1, &state));
  EXPECT_EQ(DiskLifetimeScore(config, state.anchor, state.scoring_reads(),
                              state.first_early_read(), state.write_armed(), 1),
            1209600);
  const uint64_t end =
      DiskLifetimeProtectEnd(config, state.anchor, state.scoring_reads(),
                             state.first_early_read(), state.write_armed());
  EXPECT_GT(
      DiskLifetimeScore(config, state.anchor, state.scoring_reads(),
                        state.first_early_read(), state.write_armed(), end - 1),
      config.keep_score_sec);
  EXPECT_LE(
      DiskLifetimeScore(config, state.anchor, state.scoring_reads(),
                        state.first_early_read(), state.write_armed(), end),
      config.keep_score_sec);
}

TEST(DiskLifetimeState, ThreeOrMoreClassIncludesFourthAndFifthRead) {
  const DiskLifetimeConfig config;
  for (const bool recovered : {false, true}) {
    SCOPED_TRACE(recovered);
    DiskLifetimeState state =
        recovered ? RecoveredDiskLifetimeState(2) : NewDiskLifetimeState(0);
    for (uint32_t read = 1; read <= 5; ++read) {
      ASSERT_TRUE(ApplyDiskLifetimeRead(config, 180 + read, &state));
      if (read >= 3) {
        EXPECT_EQ(ClassifyDiskLifetimeRead(config, state, 180 + read),
                  DiskLifetimeReadClass::kReadThreeOrMoreOutsideNewWriteWindow);
        EXPECT_EQ(state.actual_reads, 3U);
      }
    }
  }
}

}  // namespace
}  // namespace dfkv
