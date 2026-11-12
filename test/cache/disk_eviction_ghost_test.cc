#include "cache/disk_eviction_ghost.h"

#include <cstddef>
#include <cstdint>
#include <limits>
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
