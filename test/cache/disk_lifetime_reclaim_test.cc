#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_slab_store.h"
#include "cache/disk_write_result.h"
#include "cache/slab_allocator.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static void FailRecordWrites(DiskSlabStore& store) {
    store.record_write_hook_for_test_ = [] { return false; };
  }
  static void ReclaimAfterAdmission(DiskSlabStore& store,
                                    const std::function<void()>& before_lock) {
    store.evict_high_bytes_ = 1;
    store.evict_low_bytes_ = 0;
    store.ReclaimTick({}, before_lock);
  }
};
namespace {

BlockKey Key(uint64_t id) { return {id, 0}; }

TEST(LifetimeReclaimGuard, RefusedGrowthStillUsesAlreadyBoundFreeSlots) {
  bool allowed = true;
  unsigned binds = 0;
  SlabAllocator::Options options;
  options.extent_bytes = 4096;
  options.num_extents = 9;
  options.can_reclaim = [&] { return allowed; };
  options.on_extent_bind = [&](uint32_t) {
    ++binds;
    return true;
  };
  SlabAllocator allocator(options);
  SlabAllocator::SlotRef slot;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(1), 1, &slot, &evicted));
  ASSERT_EQ(binds, 8U);
  allowed = false;
  for (uint64_t id = 2; id <= 8; ++id)
    ASSERT_TRUE(allocator.Put(Key(id), 1, &slot, &evicted));
  EXPECT_EQ(binds, 8U);
  EXPECT_FALSE(allocator.Put(Key(9), 1, &slot, &evicted));
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), 8U);
  EXPECT_EQ(allocator.PoolExtents(), 1U);
}

TEST(LifetimeReclaimGuard, ClassGlobalAndDonorDecisionsHonorRefusal) {
  for (const char* path : {"class", "global", "donor"}) {
    SCOPED_TRACE(path);
    bool allowed = true;
    unsigned invalidations = 0;
    SlabAllocator::Options options;
    options.extent_bytes = 8192;
    options.num_extents = 1;
    options.can_reclaim = [&] { return allowed; };
    options.on_extent_bind = [&](uint32_t) {
      ++invalidations;
      return true;
    };
    options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
      ++invalidations;
      return true;
    };
    options.on_extent_evict = [&](uint32_t, uint32_t, uint32_t) {
      ++invalidations;
      return true;
    };
    SlabAllocator allocator(options);
    SlabAllocator::SlotRef slot;
    std::vector<BlockKey> evicted;
    ASSERT_TRUE(allocator.Put(Key(1), 1, &slot, &evicted));
    ASSERT_TRUE(allocator.Put(Key(2), 1, &slot, &evicted));
    allocator.Pin(Key(1));
    allocator.Pin(Key(2));
    // Register a second class without letting it steal the pinned donor.
    ASSERT_FALSE(allocator.Put(Key(3), 8000, &slot, &evicted));
    allocator.Unpin(Key(1));
    allocator.Unpin(Key(2));
    ASSERT_TRUE(evicted.empty());
    const unsigned before = invalidations;
    allowed = false;
    const std::string operation(path);
    if (operation == "class") {
      EXPECT_EQ(allocator.ReclaimClass(0, 1, 4, &evicted), 0U);
    } else if (operation == "global") {
      EXPECT_EQ(allocator.EvictColdToTarget(0, 4, &evicted), 0U);
    } else {
      EXPECT_FALSE(allocator.StealFrom(0, 1, &evicted));
    }
    EXPECT_EQ(invalidations, before);
    EXPECT_TRUE(evicted.empty());
    EXPECT_EQ(allocator.Count(), 2U);
    EXPECT_TRUE(allocator.Contains(Key(1)));
    EXPECT_TRUE(allocator.Contains(Key(2)));
  }
}

TEST(LifetimeReclaimGuard, SuccessfulWipeFinishesEvenIfGuardClosesDuringIo) {
  bool allowed = true;
  bool stop_after_wipe = false;
  SlabAllocator::Options options;
  options.extent_bytes = 8192;
  options.num_extents = 1;
  options.can_reclaim = [&] { return allowed; };
  options.on_extent_bind = [&](uint32_t) {
    if (stop_after_wipe) allowed = false;
    return true;
  };
  SlabAllocator allocator(options);
  SlabAllocator::SlotRef slot;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(1), 1, &slot, &evicted));
  ASSERT_TRUE(allocator.Put(Key(2), 1, &slot, &evicted));
  stop_after_wipe = true;
  ASSERT_TRUE(allocator.Put(Key(3), 8000, &slot, &evicted));
  EXPECT_FALSE(allowed);
  EXPECT_EQ(evicted.size(), 2U);
  EXPECT_EQ(allocator.Count(), 1U);
  EXPECT_TRUE(allocator.Contains(Key(3)));
  EXPECT_FALSE(allocator.Contains(Key(1)));
  EXPECT_FALSE(allocator.Contains(Key(2)));
}

TEST(LifetimeReclaimGuard, WatermarkFinishesFirstVictimThenStopsNext) {
  bool allowed = true;
  unsigned invalidations = 0;
  SlabAllocator::Options options;
  options.extent_bytes = 8192;
  options.num_extents = 1;
  options.can_reclaim = [&] { return allowed; };
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++invalidations;
    allowed = false;
    return true;
  };
  options.on_extent_evict = [&](uint32_t, uint32_t, uint32_t) {
    ++invalidations;
    return true;
  };
  SlabAllocator allocator(options);
  SlabAllocator::SlotRef slot;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(1), 1, &slot, &evicted));
  ASSERT_TRUE(allocator.Put(Key(2), 1, &slot, &evicted));
  EXPECT_EQ(allocator.EvictColdToTarget(0, 8, &evicted), 1U);
  EXPECT_EQ(invalidations, 1U);
  ASSERT_EQ(evicted.size(), 1U);
  EXPECT_FALSE(allocator.Contains(evicted.front()));
  EXPECT_EQ(allocator.Count(), 1U);
}

TEST(LifetimeReclaimGuard, ClosesBetweenSelectionAndPersistence) {
  unsigned checks = 0;
  unsigned invalidations = 0;
  bool stop_during_decision = false;
  SlabAllocator::Options options;
  options.extent_bytes = 8192;
  options.num_extents = 1;
  options.can_reclaim = [&] { return !stop_during_decision || ++checks < 2; };
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++invalidations;
    return true;
  };
  SlabAllocator allocator(options);
  SlabAllocator::SlotRef slot;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(1), 1, &slot, &evicted));
  stop_during_decision = true;
  EXPECT_EQ(allocator.EvictColdToTarget(0, 1, &evicted), 0U);
  EXPECT_EQ(checks, 2U);
  EXPECT_EQ(invalidations, 0U);
  EXPECT_EQ(allocator.Count(), 1U);
  EXPECT_TRUE(evicted.empty());
}

TEST(LifetimeReclaimGuard, RestoreUsesAlreadyPreparedRecordsWithClosedGuard) {
  unsigned checks = 0;
  unsigned binds = 0;
  SlabAllocator::Options options;
  options.extent_bytes = 8192;
  options.num_extents = 1;
  options.can_reclaim = [&] {
    ++checks;
    return false;
  };
  options.on_extent_bind = [&](uint32_t) {
    ++binds;
    return false;
  };
  SlabAllocator allocator(options);
  const std::vector<SlabAllocator::RestoreEntry> recovered{
      {Key(1), 4096, 0, 0, 1}};
  ASSERT_TRUE(allocator.RestoreBulk(recovered));
  EXPECT_EQ(checks, 0U);
  EXPECT_EQ(binds, 0U);
  SlabAllocator::SlotRef slot;
  std::vector<BlockKey> evicted;
  EXPECT_TRUE(allocator.Put(Key(2), 1, &slot, &evicted));
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_EQ(binds, 0U);
}

class LifetimeStoreReclaimTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_hard_reclaim_" +
         std::to_string(
             ::testing::UnitTest::GetInstance()->current_test_info()->line()));
    std::filesystem::remove_all(dir_);
    clock_ = std::make_shared<DiskLifetimeClock>(
        100, [this] { return seconds_.load(std::memory_order_relaxed); });
  }
  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  void Create(uint64_t extent_bytes = 8192) {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.extent_bytes = extent_bytes;
    options.capacity_bytes = extent_bytes;
    options.slot_granularity = 4096;
    options.direct_writes = false;
    options.table_sync_ms = 0;
    options.reclaim_interval_ms = 0;
    options.track_resident_identity = true;
    options.lifetime_clock = clock_;
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy());
  }
  void Expire() {
    seconds_.store(uint64_t{UINT32_MAX} + 1, std::memory_order_relaxed);
  }
  std::filesystem::path dir_;
  std::atomic<uint64_t> seconds_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::unique_ptr<DiskSlabStore> store_;
};

TEST_F(LifetimeStoreReclaimTest, FreeSlotAndExistingValueSurviveHardStop) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "a", 1), Status::kOk);
  Expire();
  const DiskWriteResult free_write = store_->CacheWithResult(Key(2), "b", 1);
  EXPECT_EQ(free_write.status, Status::kOk);
  EXPECT_EQ(free_write.outcome, DiskWriteOutcome::kNew);
  EXPECT_TRUE(free_write.identity.valid());
  const DiskWriteResult stopped = store_->CacheWithResult(Key(3), "c", 1);
  EXPECT_EQ(stopped.status, Status::kCacheFull);
  EXPECT_EQ(stopped.reason, DiskWriteReason::kLifetimeClockExhausted);
  EXPECT_FALSE(stopped.identity.valid());
  EXPECT_EQ(stopped.outcome, DiskWriteOutcome::kUnknown);
  const DiskWriteResult duplicate = store_->CacheWithResult(Key(1), "x", 1);
  EXPECT_EQ(duplicate.status, Status::kOk);
  EXPECT_EQ(duplicate.outcome, DiskWriteOutcome::kKeptExisting);
  std::string value;
  ASSERT_EQ(store_->Range(Key(1), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "a");
  EXPECT_EQ(store_->Count(), 2U);
  EXPECT_EQ(store_->GetStats().metadata_io_errors, 0U);
  EXPECT_TRUE(store_->Healthy());
  seconds_.store(0, std::memory_order_relaxed);
  EXPECT_EQ(store_->CacheWithResult(Key(3), "c", 1).reason,
            DiskWriteReason::kLifetimeClockExhausted);
}

TEST_F(LifetimeStoreReclaimTest, HardStopCannotCreateANewBinding) {
  ASSERT_NO_FATAL_FAILURE(Create());
  Expire();
  const DiskWriteResult result = store_->CacheWithResult(Key(1), "a", 1);
  EXPECT_EQ(result.status, Status::kCacheFull);
  EXPECT_EQ(result.reason, DiskWriteReason::kLifetimeClockExhausted);
  EXPECT_EQ(store_->Count(), 0U);
  EXPECT_EQ(store_->UsedBytes(), 0U);
  EXPECT_EQ(store_->GetStats().bind_wipes, 0U);
  EXPECT_EQ(store_->GetStats().metadata_io_errors, 0U);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(LifetimeStoreReclaimTest, MixedBatchKeepsReservedFreeWriteAndFollowers) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "a", 1), Status::kOk);
  Expire();
  char first[] = "b";
  char denied[] = "c";
  char duplicate[] = "x";
  const std::vector<StoreEngine::CacheBatchItem> items{
      {Key(2), first, 1, sizeof(first)},
      {Key(3), denied, 1, sizeof(denied)},
      {Key(2), duplicate, 1, sizeof(duplicate)}};
  const std::vector<DiskWriteResult> results =
      store_->CacheDirectBatchWithResults(items);
  ASSERT_EQ(results.size(), 3U);
  EXPECT_EQ(results[0].status, Status::kOk);
  EXPECT_EQ(results[0].outcome, DiskWriteOutcome::kNew);
  EXPECT_EQ(results[1].status, Status::kCacheFull);
  EXPECT_EQ(results[1].reason, DiskWriteReason::kLifetimeClockExhausted);
  EXPECT_EQ(results[2].status, Status::kOk);
  EXPECT_EQ(results[2].outcome, DiskWriteOutcome::kKeptExisting);
  EXPECT_FALSE(results[2].identity.valid());
  std::string value;
  ASSERT_EQ(store_->Range(Key(2), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "b");
  EXPECT_FALSE(store_->IsCached(Key(3)));
  EXPECT_EQ(store_->GetStats().inflight, 0U);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(LifetimeStoreReclaimTest, SoftLimitStillPermitsAcceptedReclaim) {
  ASSERT_NO_FATAL_FAILURE(Create(4096));
  ASSERT_EQ(store_->Cache(Key(1), "a", 1), Status::kOk);
  seconds_.store(UINT32_MAX, std::memory_order_relaxed);
  const DiskWriteResult result = store_->CacheWithResult(Key(2), "b", 1);
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.outcome, DiskWriteOutcome::kNew);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_TRUE(store_->IsCached(Key(2)));
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(LifetimeStoreReclaimTest, HardStopDoesNotBlockDeferredRemoveRelease) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "a", 1), Status::kOk);
  ReadLease lease;
  ASSERT_EQ(store_->RangeDirectPrep(Key(1), 0, 0, 4096, &lease), Status::kOk);
  Expire();
  ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_EQ(store_->GetStats().committed_objects, 0U);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_EQ(store_->GetStats().allocator_objects, 1U);
  lease = ReadLease{};  // Exercise the public RAII release path.
  EXPECT_EQ(store_->GetStats().allocator_objects, 0U);
  EXPECT_EQ(store_->GetStats().prep_holds, 0U);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(LifetimeStoreReclaimTest, HardStopDoesNotMaskReservedRecordIoFailure) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "a", 1), Status::kOk);
  Expire();
  DiskSlabStoreTestPeer::FailRecordWrites(*store_);
  const DiskWriteResult result = store_->CacheWithResult(Key(2), "b", 1);
  EXPECT_EQ(result.status, Status::kIOError);
  EXPECT_EQ(result.reason, DiskWriteReason::kNone);
  EXPECT_FALSE(result.identity.valid());
  EXPECT_FALSE(store_->Healthy());
  EXPECT_GT(store_->GetStats().metadata_io_errors, 0U);
}

TEST_F(LifetimeStoreReclaimTest, AdmittedBackgroundTickRechecksUnderStoreLock) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache(Key(1), "a", 1), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(2), "b", 1), Status::kOk);
  DiskSlabStoreTestPeer::ReclaimAfterAdmission(*store_, [this] { Expire(); });
  EXPECT_EQ(store_->Count(), 2U);
  EXPECT_EQ(store_->GetStats().watermark_ticks, 0U);
  EXPECT_EQ(store_->GetStats().watermark_evictions, 0U);
  EXPECT_TRUE(store_->Healthy());
}

}  // namespace
}  // namespace dfkv
