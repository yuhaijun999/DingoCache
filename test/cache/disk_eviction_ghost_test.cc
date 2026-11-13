#include "cache/disk_eviction_ghost.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>

// Third-party headers.
#include <gtest/gtest.h>

#include "common/kv_types.h"

namespace dfkv {
namespace {

static_assert(!std::is_copy_constructible_v<DiskEvictionGhost>);
static_assert(!std::is_move_constructible_v<DiskEvictionGhost>);
static_assert(!std::is_copy_assignable_v<DiskEvictionGhost>);
static_assert(!std::is_move_assignable_v<DiskEvictionGhost>);

TEST(DiskEvictionGhostTest, ZeroCapacityIsDisabled) {
  DiskEvictionGhost ghost(0);
  EXPECT_FALSE(ghost.RecordEviction({1, 2, 3}, 0));
  EXPECT_FALSE(ghost.LookupEvictionAge({1, 2, 3}, 1).has_value());
  const DiskEvictionGhost::Snapshot snapshot = ghost.GetSnapshot();
  EXPECT_EQ(snapshot.capacity, 0U);
  EXPECT_EQ(snapshot.retained, 0U);
  EXPECT_FALSE(snapshot.capacity_limited);
  EXPECT_FALSE(snapshot.oldest_evicted_at_sec.has_value());
  EXPECT_FALSE(snapshot.newest_evicted_at_sec.has_value());
}

TEST(DiskEvictionGhostTest, EmptyAndZeroTimestampAreDistinct) {
  DiskEvictionGhost ghost(2);
  EXPECT_FALSE(ghost.GetSnapshot().oldest_evicted_at_sec.has_value());
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 0));
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 0), 0U);
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 7), 7U);
  const DiskEvictionGhost::Snapshot snapshot = ghost.GetSnapshot();
  EXPECT_EQ(snapshot.capacity, 2U);
  EXPECT_EQ(snapshot.retained, 1U);
  EXPECT_FALSE(snapshot.capacity_limited);
  EXPECT_EQ(snapshot.oldest_evicted_at_sec, 0U);
  EXPECT_EQ(snapshot.newest_evicted_at_sec, 0U);
}

TEST(DiskEvictionGhostTest, FullCapacityDiscardsOldestNotMostRecentlyQueried) {
  DiskEvictionGhost ghost(2);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 10));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 20));
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 25), 15U);
  ASSERT_TRUE(ghost.RecordEviction({3, 0}, 30));
  EXPECT_FALSE(ghost.LookupEvictionAge({1, 0}, 40).has_value());
  EXPECT_EQ(ghost.LookupEvictionAge({2, 0}, 40), 20U);
  EXPECT_EQ(ghost.LookupEvictionAge({3, 0}, 40), 10U);
  const DiskEvictionGhost::Snapshot snapshot = ghost.GetSnapshot();
  EXPECT_EQ(snapshot.retained, 2U);
  EXPECT_TRUE(snapshot.capacity_limited);
  EXPECT_EQ(snapshot.oldest_evicted_at_sec, 20U);
  EXPECT_EQ(snapshot.newest_evicted_at_sec, 30U);
}

TEST(DiskEvictionGhostTest, DuplicateEvictionRefreshesWithoutCapacityLoss) {
  DiskEvictionGhost ghost(2);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 10));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 20));
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 30));
  EXPECT_EQ(ghost.GetSnapshot().retained, 2U);
  EXPECT_FALSE(ghost.GetSnapshot().capacity_limited);
  EXPECT_EQ(ghost.GetSnapshot().oldest_evicted_at_sec, 20U);
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 35), 5U);
  ASSERT_TRUE(ghost.RecordEviction({3, 0}, 40));
  EXPECT_FALSE(ghost.LookupEvictionAge({2, 0}, 50).has_value());
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 50), 20U);
}

TEST(DiskEvictionGhostTest, EveryKeyWordIncludingTenantParticipatesInIdentity) {
  DiskEvictionGhost ghost(4);
  const BlockKey keys[] = {{1, 2, 3}, {2, 2, 3}, {1, 3, 3}, {1, 2, 4}};
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(ghost.RecordEviction(keys[i], i));
  }
  EXPECT_EQ(ghost.GetSnapshot().retained, 4U);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(ghost.LookupEvictionAge(keys[i], 10), 10 - i);
  }
  EXPECT_FALSE(ghost.LookupEvictionAge({1, 2, 5}, 10).has_value());
}

TEST(DiskEvictionGhostTest, RegressingRecordDoesNotChangeHistoryOrDiscard) {
  DiskEvictionGhost ghost(1);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 20));
  EXPECT_FALSE(ghost.RecordEviction({2, 0}, 19));
  EXPECT_FALSE(ghost.RecordEviction({1, 0}, 19));
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 30), 10U);
  EXPECT_FALSE(ghost.LookupEvictionAge({2, 0}, 30).has_value());
  EXPECT_FALSE(ghost.LookupEvictionAge({1, 0}, 19).has_value());
  const DiskEvictionGhost::Snapshot snapshot = ghost.GetSnapshot();
  EXPECT_EQ(snapshot.retained, 1U);
  EXPECT_FALSE(snapshot.capacity_limited);
  EXPECT_EQ(snapshot.oldest_evicted_at_sec, 20U);
  EXPECT_EQ(snapshot.newest_evicted_at_sec, 20U);
  EXPECT_FALSE(snapshot.lost_through_sec.has_value());
  EXPECT_FALSE(snapshot.loss_time_unknown);
}

TEST(DiskEvictionGhostTest, EqualTimesStillExposeCapacityTruncation) {
  DiskEvictionGhost ghost(1);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 5));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 5));
  const DiskEvictionGhost::Snapshot snapshot = ghost.GetSnapshot();
  EXPECT_EQ(snapshot.oldest_evicted_at_sec, 5U);
  EXPECT_EQ(snapshot.newest_evicted_at_sec, 5U);
  EXPECT_TRUE(snapshot.capacity_limited);
  EXPECT_FALSE(ghost.LookupEvictionAge({1, 0}, 5).has_value());
  EXPECT_EQ(ghost.LookupEvictionAge({2, 0}, 5), 0U);
}

TEST(DiskEvictionGhostTest, QueriesDoNotConsumeOrInventDemandEvidence) {
  DiskEvictionGhost ghost(1);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 10));
  for (unsigned int i = 0; i < 10; ++i) {
    EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 20), 10U);
  }
  EXPECT_FALSE(ghost.LookupEvictionAge({2, 0}, 20).has_value());
  EXPECT_EQ(ghost.GetSnapshot().retained, 1U);
  EXPECT_EQ(ghost.GetSnapshot().newest_evicted_at_sec, 10U);
}

TEST(DiskEvictionGhostTest, MaximumTimeDoesNotOverflowAge) {
  DiskEvictionGhost ghost(2);
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 0));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, kMax));
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, kMax), kMax);
  EXPECT_EQ(ghost.LookupEvictionAge({2, 0}, kMax), 0U);
  EXPECT_FALSE(ghost.RecordEviction({3, 0}, kMax - 1));
  EXPECT_EQ(ghost.GetSnapshot().retained, 2U);
}

TEST(DiskEvictionGhostTest, DisabledAndFreshOwnersHaveNoLossBoundary) {
  DiskEvictionGhost disabled(0);
  disabled.NoteDroppedEviction(0);
  disabled.NoteDroppedEviction(std::nullopt);
  EXPECT_FALSE(disabled.GetSnapshot().lost_through_sec.has_value());
  EXPECT_FALSE(disabled.GetSnapshot().loss_time_unknown);
  DiskEvictionGhost enabled(1);
  ASSERT_TRUE(enabled.RecordEviction({1, 0}, 0));
  ASSERT_TRUE(enabled.RecordEviction({1, 0}, 10));
  EXPECT_FALSE(enabled.GetSnapshot().lost_through_sec.has_value());
  EXPECT_FALSE(enabled.GetSnapshot().loss_time_unknown);
}

TEST(DiskEvictionGhostTest, CapacityLossUsesRemovedTimeNotIncomingTime) {
  DiskEvictionGhost ghost(2);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 0));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 5));
  ASSERT_TRUE(ghost.RecordEviction({3, 0}, 10));
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 0U);
  // Refreshing a retained key loses no key-membership evidence.
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 20));
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 0U);
  ASSERT_TRUE(ghost.RecordEviction({4, 0}, 30));
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 10U);
  EXPECT_FALSE(ghost.GetSnapshot().loss_time_unknown);
}

TEST(DiskEvictionGhostTest, EqualTimeLossBoundaryExcludesTheEntireSecond) {
  DiskEvictionGhost ghost(1);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 5));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 5));
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 5U);
  EXPECT_EQ(ghost.GetSnapshot().oldest_evicted_at_sec, 5U);
  EXPECT_FALSE(ghost.LookupEvictionAge({1, 0}, 5));
  EXPECT_EQ(ghost.LookupEvictionAge({2, 0}, 5), 0U);
}

TEST(DiskEvictionGhostTest, KnownDropsAdvanceByMaximumWithoutChangingHistory) {
  DiskEvictionGhost ghost(1);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 0));
  ghost.NoteDroppedEviction(0);
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 0U);
  ghost.NoteDroppedEviction(50);
  ghost.NoteDroppedEviction(3);
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 10));
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 50U);
  ghost.NoteDroppedEviction(std::nullopt);
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 50U);
  EXPECT_TRUE(ghost.GetSnapshot().loss_time_unknown);
  EXPECT_EQ(ghost.GetSnapshot().retained, 1U);
  EXPECT_EQ(ghost.LookupEvictionAge({2, 0}, 10), 0U);
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 50U);
}

TEST(DiskEvictionGhostTest, UnknownTimeLossStaysStickyUntilReconstruction) {
  DiskEvictionGhost ghost(1);
  ghost.NoteDroppedEviction(std::nullopt);
  EXPECT_TRUE(ghost.GetSnapshot().loss_time_unknown);
  EXPECT_FALSE(ghost.GetSnapshot().lost_through_sec.has_value());
  ghost.NoteDroppedEviction(7);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 20));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 30));
  EXPECT_TRUE(ghost.GetSnapshot().loss_time_unknown);
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 20U);
  const DiskEvictionGhost reconstructed(1);
  EXPECT_FALSE(reconstructed.GetSnapshot().loss_time_unknown);
  EXPECT_FALSE(reconstructed.GetSnapshot().lost_through_sec.has_value());
}

TEST(DiskEvictionGhostTest, MaximumLossTimeNeedsNoIncrementOrWrapping) {
  DiskEvictionGhost ghost(1);
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, kMax));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, kMax));
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, kMax);
  ghost.NoteDroppedEviction(0);
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, kMax);
  EXPECT_EQ(ghost.LookupEvictionAge({2, 0}, kMax), 0U);
}

TEST(DiskEvictionGhostTest, WindowRequiresEnabledOwnerAndBothKnownBounds) {
  const DiskEvictionGhost disabled(0, 0);
  EXPECT_FALSE(disabled.GetSnapshot(1).window_after_sec);
  EXPECT_FALSE(disabled.GetSnapshot(1).window_through_sec);
  const DiskEvictionGhost unknown_start(2);
  EXPECT_FALSE(unknown_start.GetSnapshot(1).window_after_sec);
  EXPECT_FALSE(unknown_start.GetSnapshot(1).window_through_sec);
  const DiskEvictionGhost known_start(2, 0);
  EXPECT_FALSE(known_start.GetSnapshot().window_after_sec);
  EXPECT_FALSE(known_start.GetSnapshot().window_through_sec);
  EXPECT_EQ(known_start.GetSnapshot(0).window_after_sec, 0U);
  EXPECT_EQ(known_start.GetSnapshot(0).window_through_sec, 0U);
}

TEST(DiskEvictionGhostTest,
     EmptyWindowUsesOwnerStartAndResetsOnReconstruction) {
  DiskEvictionGhost ghost(2, 13);
  EXPECT_EQ(ghost.GetSnapshot(13).window_after_sec, 13U);
  EXPECT_EQ(ghost.GetSnapshot(13).window_through_sec, 13U);
  EXPECT_FALSE(ghost.GetSnapshot(13).oldest_evicted_at_sec);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 14));
  ghost.NoteDroppedEviction(std::nullopt);
  EXPECT_FALSE(ghost.GetSnapshot(15).window_after_sec);
  const DiskEvictionGhost reconstructed(2, 15);
  EXPECT_EQ(reconstructed.GetSnapshot(17).window_after_sec, 15U);
  EXPECT_EQ(reconstructed.GetSnapshot(17).window_through_sec, 17U);
  EXPECT_EQ(reconstructed.GetSnapshot(17).retained, 0U);
}

TEST(DiskEvictionGhostTest, WindowExcludesLostSecondNotOldestRetainedRecord) {
  DiskEvictionGhost ghost(2, 0);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 5));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 7));
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 9));
  EXPECT_EQ(ghost.GetSnapshot(10).oldest_evicted_at_sec, 7U);
  EXPECT_EQ(ghost.GetSnapshot(10).window_after_sec, 0U);
  ASSERT_TRUE(ghost.RecordEviction({3, 0}, 12));
  EXPECT_EQ(ghost.GetSnapshot(12).window_after_sec, 7U);
  EXPECT_EQ(ghost.GetSnapshot(12).window_through_sec, 12U);
  ASSERT_TRUE(ghost.RecordEviction({4, 0}, 12));
  EXPECT_EQ(ghost.GetSnapshot(12).window_after_sec, 9U);
  ASSERT_TRUE(ghost.RecordEviction({5, 0}, 12));
  // Equal-time losses leave a valid empty prefix, not coverage of this second.
  EXPECT_EQ(ghost.GetSnapshot(12).window_after_sec, 12U);
  EXPECT_EQ(ghost.GetSnapshot(12).window_through_sec, 12U);
}

TEST(DiskEvictionGhostTest,
     WindowKnownLossCanAdvanceButUnknownLossStaysSticky) {
  DiskEvictionGhost ghost(2, 2);
  ghost.NoteDroppedEviction(5);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 6));
  EXPECT_EQ(ghost.GetSnapshot(6).window_after_sec, 5U);
  EXPECT_EQ(ghost.GetSnapshot(6).window_through_sec, 6U);
  ghost.NoteDroppedEviction(std::nullopt);
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, 7));
  EXPECT_FALSE(ghost.GetSnapshot(8).window_after_sec);
  EXPECT_FALSE(ghost.GetSnapshot(8).window_through_sec);
  EXPECT_EQ(ghost.GetSnapshot(8).retained, 2U);
  EXPECT_EQ(ghost.LookupEvictionAge({1, 0}, 8), 2U);
}

TEST(DiskEvictionGhostTest, WindowRejectsEndBeforeStartLossOrLatestRecord) {
  DiskEvictionGhost ghost(2, 10);
  EXPECT_FALSE(ghost.GetSnapshot(9).window_after_sec);
  EXPECT_FALSE(ghost.GetSnapshot(9).window_through_sec);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, 15));
  EXPECT_FALSE(ghost.GetSnapshot(14).window_after_sec);
  EXPECT_FALSE(ghost.GetSnapshot(14).window_through_sec);
  ghost.NoteDroppedEviction(20);
  EXPECT_FALSE(ghost.GetSnapshot(19).window_after_sec);
  EXPECT_FALSE(ghost.GetSnapshot(19).window_through_sec);
  EXPECT_EQ(ghost.GetSnapshot(20).window_after_sec, 20U);
  EXPECT_EQ(ghost.GetSnapshot(20).window_through_sec, 20U);
  EXPECT_EQ(ghost.GetSnapshot().lost_through_sec, 20U);
  EXPECT_FALSE(ghost.GetSnapshot().loss_time_unknown);
}

TEST(DiskEvictionGhostTest, MaximumWindowBoundaryNeverAddsOrWrapsSeconds) {
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  const DiskEvictionGhost late_owner(1, kMax);
  EXPECT_EQ(late_owner.GetSnapshot(kMax).window_after_sec, kMax);
  EXPECT_EQ(late_owner.GetSnapshot(kMax).window_through_sec, kMax);
  DiskEvictionGhost ghost(1, 0);
  ASSERT_TRUE(ghost.RecordEviction({1, 0}, kMax));
  ASSERT_TRUE(ghost.RecordEviction({2, 0}, kMax));
  EXPECT_EQ(ghost.GetSnapshot(kMax).window_after_sec, kMax);
  EXPECT_EQ(ghost.GetSnapshot(kMax).window_through_sec, kMax);
  EXPECT_FALSE(ghost.GetSnapshot(kMax - 1).window_after_sec);
  EXPECT_FALSE(ghost.GetSnapshot(kMax - 1).window_through_sec);
}

TEST(DiskEvictionGhostTest, LongTraceRemainsBoundedAndRefreshesLatestEvidence) {
  constexpr size_t kCapacity = 3;
  DiskEvictionGhost ghost(kCapacity);
  for (uint64_t time = 0; time < 1000; ++time) {
    const BlockKey key{time % 5, 0};
    ASSERT_TRUE(ghost.RecordEviction(key, time));
    ASSERT_TRUE(ghost.RecordEviction(key, time));
    EXPECT_LE(ghost.GetSnapshot().retained, kCapacity);
    EXPECT_EQ(ghost.LookupEvictionAge(key, time), 0U);
  }
  const DiskEvictionGhost::Snapshot snapshot = ghost.GetSnapshot();
  EXPECT_EQ(snapshot.retained, kCapacity);
  EXPECT_EQ(snapshot.oldest_evicted_at_sec, 997U);
  EXPECT_EQ(snapshot.newest_evicted_at_sec, 999U);
  EXPECT_TRUE(snapshot.capacity_limited);
  EXPECT_FALSE(ghost.LookupEvictionAge({1, 0}, 999).has_value());
}

}  // namespace
}  // namespace dfkv
