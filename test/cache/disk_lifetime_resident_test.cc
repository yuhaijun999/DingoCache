#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/disk_slab_store.h"
#include "cache/disk_write_result.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static SlabAllocatorTestPeer::LifetimeSnapshot Snapshot(
      const DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeSnapshotFor(*store.alloc_, key);
  }
  static size_t Capacity(const DiskSlabStore& store) {
    return SlabAllocatorTestPeer::LifetimeStateCapacity(*store.alloc_);
  }
  static uint64_t Base(const DiskSlabStore& store) {
    if (!store.opt_.lifetime_config) {
      ADD_FAILURE() << "scoring configuration is missing";
      return 0;
    }
    return store.opt_.lifetime_config->base_sec;
  }
  static void PayloadHook(DiskSlabStore& store, std::function<bool()> hook) {
    store.payload_write_hook_for_test_ = std::move(hook);
  }
  static void RecordHook(DiskSlabStore& store, std::function<bool()> hook) {
    store.record_write_hook_for_test_ = std::move(hook);
  }
};
namespace {
BlockKey Key(uint64_t id) { return {id, 0}; }

DiskLifetimeState RequireState(const std::optional<DiskLifetimeState>& state) {
  if (!state.has_value()) {
    ADD_FAILURE() << "the committed scoring state is missing";
    return {};
  }
  return *state;
}

class DiskLifetimeResidentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_resident_" +
         std::to_string(
             ::testing::UnitTest::GetInstance()->current_test_info()->line()));
    std::filesystem::remove_all(dir_);
    clock_ = std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(config_),
        [this] { return seconds_.load(std::memory_order_relaxed); });
  }
  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  DiskSlabStore::Options Options() const {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.capacity_bytes = 8192;
    options.extent_bytes = 8192;
    options.slot_granularity = 4096;
    options.table_sync_ms = 0;
    options.reclaim_interval_ms = 0;
    options.direct_writes = false;
    options.lifetime_config = config_;
    options.lifetime_clock = clock_;
    return options;
  }
  void Create() {
    store_ = std::make_unique<DiskSlabStore>(Options());
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  std::optional<DiskLifetimeState> State(const BlockKey& key) const {
    return DiskSlabStoreTestPeer::Snapshot(*store_, key).second;
  }
  DiskLifetimeConfig config_;
  std::atomic<uint64_t> seconds_{100};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::unique_ptr<DiskSlabStore> store_;
  std::filesystem::path dir_;
};

TEST_F(DiskLifetimeResidentTest,
       NewCommitUsesActualCommitTimeAndKeepsFirstState) {
  ASSERT_NO_FATAL_FAILURE(Create());
  DiskSlabStoreTestPeer::PayloadHook(*store_, [this] {
    seconds_.store(132, std::memory_order_relaxed);
    return true;
  });
  const DiskWriteResult written = store_->CacheWithResult(Key(1), "first", 5);
  ASSERT_EQ(written.status, Status::kOk);
  ASSERT_TRUE(written.identity.valid());
  const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
      DiskSlabStoreTestPeer::Snapshot(*store_, Key(1));
  EXPECT_EQ(snapshot.first, written.identity.generation);
  const DiskLifetimeState committed_state = RequireState(snapshot.second);
  EXPECT_EQ(committed_state.anchor, 32U);
  EXPECT_EQ(committed_state.put_at, 32U);
  EXPECT_TRUE(committed_state.write_armed());
  EXPECT_EQ(committed_state.actual_reads, 0U);
  EXPECT_FALSE(committed_state.history_unknown);
  seconds_.store(300, std::memory_order_relaxed);
  const DiskWriteResult kept = store_->CacheWithResult(Key(1), "other", 5);
  EXPECT_EQ(kept.outcome, DiskWriteOutcome::kKeptExisting);
  const DiskLifetimeState kept_state = RequireState(State(Key(1)));
  EXPECT_EQ(kept_state.anchor, 32U);
  std::string value;
  EXPECT_EQ(store_->Range(Key(1), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "first");
}

TEST_F(DiskLifetimeResidentTest, BatchInitializesEachNewOwnerOnlyOnce) {
  ASSERT_NO_FATAL_FAILURE(Create());
  unsigned records = 0;
  DiskSlabStoreTestPeer::RecordHook(*store_, [&] {
    seconds_.store(100 + 10 * ++records, std::memory_order_relaxed);
    return true;
  });
  char a[] = "a";
  char b[] = "b";
  char other[] = "x";
  const std::vector<StoreEngine::CacheBatchItem> items{
      {Key(1), a, 1, sizeof(a)},
      {Key(2), b, 1, sizeof(b)},
      {Key(1), other, 1, sizeof(other)}};
  const std::vector<DiskWriteResult> results =
      store_->CacheDirectBatchWithResults(items);
  ASSERT_EQ(results.size(), 3U);
  EXPECT_EQ(results[0].outcome, DiskWriteOutcome::kNew);
  EXPECT_EQ(results[1].outcome, DiskWriteOutcome::kNew);
  EXPECT_EQ(results[2].outcome, DiskWriteOutcome::kKeptExisting);
  const DiskLifetimeState first_state = RequireState(State(Key(1)));
  const DiskLifetimeState second_state = RequireState(State(Key(2)));
  // Both records finish before the single L2 commit lock. Initialization
  // uses publication time, not each earlier record-write timestamp.
  EXPECT_EQ(first_state.anchor, 20U);
  EXPECT_EQ(second_state.anchor, 20U);
  EXPECT_EQ(records, 2U);
}

TEST_F(DiskLifetimeResidentTest, RecoveryUsesStagedSeedAtSharedOrigin) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "old", 3), Status::kOk);
  store_.reset();
  seconds_.store(1120, std::memory_order_relaxed);
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskLifetimeState state = RequireState(State(Key(1)));
  EXPECT_EQ(state.anchor, 0U);
  EXPECT_EQ(state.put_at, 0U);
  EXPECT_EQ(state.scoring_reads(), 1U);  // R45 alone changes this to 2.
  EXPECT_EQ(state.actual_reads, 0U);
  EXPECT_TRUE(state.history_unknown);
  EXPECT_FALSE(state.write_armed());
  EXPECT_FALSE(state.first_early_read());
  EXPECT_EQ(ClassifyDiskLifetimeRead(config_, state, 1020),
            DiskLifetimeReadClass::kUnread);
}

TEST_F(DiskLifetimeResidentTest, HardCrossingPreservesCommittedDataButNotHeat) {
  ASSERT_NO_FATAL_FAILURE(Create());
  DiskSlabStoreTestPeer::PayloadHook(*store_, [this] {
    seconds_.store(uint64_t{UINT32_MAX} + 101, std::memory_order_relaxed);
    return true;
  });
  const DiskWriteResult result = store_->CacheWithResult(Key(1), "ok", 2);
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.reason, DiskWriteReason::kNone);
  EXPECT_EQ(result.outcome, DiskWriteOutcome::kNew);
  EXPECT_TRUE(result.identity.valid());
  EXPECT_FALSE(State(Key(1)).has_value());
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 1U);
  EXPECT_EQ(store_->GetStats().metadata_io_errors, 0U);
  EXPECT_TRUE(store_->Healthy());
  std::string value;
  EXPECT_EQ(store_->Range(Key(1), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "ok");
  EXPECT_EQ(store_->CacheWithResult(Key(1), "no", 2).outcome,
            DiskWriteOutcome::kKeptExisting);
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 1U);
  DiskSlabStoreTestPeer::PayloadHook(*store_, {});
  seconds_.store(100, std::memory_order_relaxed);
  EXPECT_EQ(store_->Cache(Key(2), "accepted", 8), Status::kOk);
  EXPECT_FALSE(State(Key(2)).has_value());  // Rollback cannot re-enable heat.
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 2U);
}

TEST_F(DiskLifetimeResidentTest, BatchHardCrossingCountsEveryActualNewCommit) {
  ASSERT_NO_FATAL_FAILURE(Create());
  DiskSlabStoreTestPeer::RecordHook(*store_, [this] {
    seconds_.store(uint64_t{UINT32_MAX} + 101, std::memory_order_relaxed);
    return true;
  });
  char a[] = "a";
  char b[] = "b";
  const std::vector<DiskWriteResult> results =
      store_->CacheDirectBatchWithResults(
          {{Key(1), a, 1, sizeof(a)}, {Key(2), b, 1, sizeof(b)}});
  ASSERT_EQ(results.size(), 2U);
  for (const DiskWriteResult& result : results) {
    EXPECT_EQ(result.status, Status::kOk);
    EXPECT_TRUE(result.identity.valid());
  }
  EXPECT_FALSE(State(Key(1)).has_value());
  EXPECT_FALSE(State(Key(2)).has_value());
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 2U);
  EXPECT_EQ(store_->Count(), 2U);
}

TEST_F(DiskLifetimeResidentTest, RecoveryCrossingHardBoundDoesNotInventHeat) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "old", 3), Status::kOk);
  store_.reset();
  unsigned samples = 0;
  clock_ =
      std::make_shared<DiskLifetimeClock>(DiskLifetimeHorizon(config_), [&] {
        // Origin and direct-store startup validation succeed; recovery's
        // initialization sample then crosses the bound.
        return ++samples <= 2 ? uint64_t{100} : uint64_t{UINT32_MAX} + 101;
      });
  ASSERT_NO_FATAL_FAILURE(Create());
  EXPECT_GE(samples, 3U);
  EXPECT_FALSE(State(Key(1)).has_value());
  EXPECT_NE(DiskSlabStoreTestPeer::Snapshot(*store_, Key(1)).first, 0U);
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 1U);
  EXPECT_TRUE(store_->Healthy());
  std::string value;
  EXPECT_EQ(store_->Range(Key(1), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "old");
}

TEST_F(DiskLifetimeResidentTest, SoftStopStillInitializesAcceptedNewCommit) {
  ASSERT_NO_FATAL_FAILURE(Create());
  DiskSlabStoreTestPeer::PayloadHook(*store_, [this] {
    seconds_.store(uint64_t{UINT32_MAX} + 100, std::memory_order_relaxed);
    return true;
  });
  ASSERT_EQ(store_->Cache(Key(1), "ok", 2), Status::kOk);
  const DiskLifetimeState state = RequireState(State(Key(1)));
  EXPECT_EQ(state.anchor, UINT32_MAX);
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 0U);
}

TEST_F(DiskLifetimeResidentTest, MetadataFailureNeverPublishesScoringState) {
  ASSERT_NO_FATAL_FAILURE(Create());
  DiskSlabStoreTestPeer::RecordHook(*store_, [] { return false; });
  EXPECT_EQ(store_->Cache(Key(1), "bad", 3), Status::kIOError);
  const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
      DiskSlabStoreTestPeer::Snapshot(*store_, Key(1));
  EXPECT_EQ(snapshot.first, 0U);
  EXPECT_FALSE(snapshot.second.has_value());
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 0U);
}

TEST_F(DiskLifetimeResidentTest, HardCrossingDoesNotHideRealRecordFailure) {
  ASSERT_NO_FATAL_FAILURE(Create());
  DiskSlabStoreTestPeer::PayloadHook(*store_, [this] {
    seconds_.store(uint64_t{UINT32_MAX} + 101, std::memory_order_relaxed);
    return true;
  });
  DiskSlabStoreTestPeer::RecordHook(*store_, [] { return false; });
  const DiskWriteResult result = store_->CacheWithResult(Key(1), "bad", 3);
  EXPECT_EQ(result.status, Status::kIOError);
  EXPECT_EQ(result.reason, DiskWriteReason::kNone);
  EXPECT_FALSE(result.identity.valid());
  EXPECT_FALSE(State(Key(1)).has_value());
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 0U);
  EXPECT_FALSE(store_->Healthy());
}

TEST_F(DiskLifetimeResidentTest, HiddenFailedInvalidationRetainsPhysicalState) {
  ASSERT_NO_FATAL_FAILURE(Create());
  seconds_.store(140, std::memory_order_relaxed);
  ASSERT_EQ(store_->Cache(Key(1), "old", 3), Status::kOk);
  ReadLease lease;
  ASSERT_EQ(store_->RangeDirectPrep(Key(1), 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  ASSERT_TRUE(State(Key(1)).has_value());
  DiskSlabStoreTestPeer::RecordHook(*store_, [] { return false; });
  lease = ReadLease{};
  const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
      DiskSlabStoreTestPeer::Snapshot(*store_, Key(1));
  EXPECT_NE(snapshot.first, 0U);
  const DiskLifetimeState committed_state = RequireState(snapshot.second);
  EXPECT_EQ(committed_state.anchor, 40U);
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(store_->GetStats().allocator_objects, 1U);
}

TEST_F(DiskLifetimeResidentTest, LastReleaseAndReuseStartFreshState) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "old", 3), Status::kOk);
  ReadLease lease;
  ASSERT_EQ(store_->RangeDirectPrep(Key(1), 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
  EXPECT_TRUE(State(Key(1)).has_value());
  lease = ReadLease{};
  EXPECT_FALSE(State(Key(1)).has_value());
  seconds_.store(175, std::memory_order_relaxed);
  ASSERT_EQ(store_->Cache(Key(1), "new", 3), Status::kOk);
  const DiskLifetimeState state = RequireState(State(Key(1)));
  EXPECT_EQ(state.anchor, 75U);
  EXPECT_FALSE(state.history_unknown);
}

TEST_F(DiskLifetimeResidentTest, RemovedInFlightWriteNeverInitializesHeat) {
  ASSERT_NO_FATAL_FAILURE(Create());
  DiskSlabStoreTestPeer::PayloadHook(*store_, [this] {
    EXPECT_EQ(store_->Remove(Key(1)), Status::kOk);
    return true;
  });
  const DiskWriteResult result = store_->CacheWithResult(Key(1), "gone", 4);
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.outcome, DiskWriteOutcome::kRemoved);
  EXPECT_FALSE(result.identity.valid());
  EXPECT_FALSE(State(Key(1)).has_value());
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 0U);
}

TEST_F(DiskLifetimeResidentTest, ValueSnapshotDoesNotAliasCallerConfiguration) {
  DiskSlabStore::Options options = Options();
  store_ = std::make_unique<DiskSlabStore>(options);
  ASSERT_TRUE(store_->Healthy());
  DiskLifetimeConfig changed = config_;
  changed.base_sec = 42;
  options.lifetime_config = changed;
  EXPECT_EQ(DiskSlabStoreTestPeer::Base(*store_), config_.base_sec);
}

TEST_F(DiskLifetimeResidentTest,
       MissingInvalidOrMismatchedConfigRejectsBeforeIo) {
  for (unsigned which = 0; which < 3; ++which) {
    SCOPED_TRACE(which);
    DiskSlabStore::Options options = Options();
    options.dir = (dir_ / std::to_string(which)).string();
    if (which == 0) {
      DiskLifetimeConfig invalid = config_;
      invalid.max_reads = 0;
      options.lifetime_config = invalid;
    }
    if (which == 1) options.lifetime_clock.reset();
    if (which == 2)
      options.lifetime_clock = std::make_shared<DiskLifetimeClock>(1);
    const DiskSlabStore store(options);
    EXPECT_FALSE(store.Healthy());
    EXPECT_FALSE(store.StartupError().empty());
    EXPECT_FALSE(std::filesystem::exists(options.dir));
  }
}

TEST_F(DiskLifetimeResidentTest,
       IdentityOnlyAndClockOnlyNeverAllocateHeatState) {
  for (const bool identity : {false, true}) {
    DiskSlabStore::Options options = Options();
    options.lifetime_config.reset();
    options.track_resident_identity = identity;
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy());
    ASSERT_EQ(store_->Cache(Key(1), "old", 3), Status::kOk);
    EXPECT_EQ(DiskSlabStoreTestPeer::Capacity(*store_), 0U);
    EXPECT_FALSE(State(Key(1)).has_value());
    EXPECT_EQ(store_->GetStats().lifetime_init_clock_hard_exhausted, 0U);
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
}
TEST_F(DiskLifetimeResidentTest,
       EmptyOrAdmissionExhaustedClockRejectsBeforeIo) {
  for (const bool empty : {true, false}) {
    DiskSlabStore::Options options = Options();
    if (empty) {
      options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
          DiskLifetimeHorizon(config_), DiskLifetimeClock::NowFn{});
    } else {
      seconds_.store(
          uint64_t{100} + UINT32_MAX - DiskLifetimeHorizon(config_) + 1,
          std::memory_order_relaxed);
    }
    const DiskSlabStore rejected(options);
    EXPECT_FALSE(rejected.Healthy());
    EXPECT_NE(rejected.StartupError().find("clock"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(dir_));
  }
}

TEST_F(DiskLifetimeResidentTest, ClockOnlyAddsNoCommitSampling) {
  std::atomic<unsigned> samples{0};
  DiskSlabStore::Options options = Options();
  options.lifetime_config.reset();
  options.track_resident_identity = true;
  options.lifetime_clock =
      std::make_shared<DiskLifetimeClock>(DiskLifetimeHorizon(config_), [&] {
        samples.fetch_add(1, std::memory_order_relaxed);
        return uint64_t{100};
      });
  DiskSlabStore store(options);
  ASSERT_TRUE(store.Healthy());
  unsigned at_record = 0;
  DiskSlabStoreTestPeer::RecordHook(store, [&] {
    at_record = samples.load(std::memory_order_relaxed);
    return true;
  });
  ASSERT_EQ(store.Cache(Key(1), "old", 3), Status::kOk);
  EXPECT_EQ(samples.load(std::memory_order_relaxed), at_record);
  EXPECT_EQ(DiskSlabStoreTestPeer::Capacity(store), 0U);
}
}  // namespace
}  // namespace dfkv
