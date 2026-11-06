#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/disk_slab_store.h"
#include "cache/disk_write_result.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static void Tick(DiskSlabStore& store, size_t victims) {
    store.ReclaimLifetimeTick(victims);
  }
  static void Dispatch(DiskSlabStore& store) { store.ReclaimTick(); }
  static bool Pin(DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->Pin(key);
  }
  static bool Unpin(DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->Unpin(key);
  }
  static void YieldHook(DiskSlabStore& store, std::function<void()> hook) {
    store.lifetime_scan_yield_hook_for_test_ = std::move(hook);
  }
  static void FailInvalidation(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) close(store.table_fd_);
    store.table_fd_ = -1;
  }
  static void ForceLegacyWatermark(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    store.evict_high_bytes_ = 1;
    store.evict_low_bytes_ = 0;
  }
  static bool IndexesValid(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeIndexesValid(*store.alloc_);
  }
};
namespace {
using Scan = SlabAllocatorTestPeer::LifetimeScan;
using ScanResult = SlabAllocatorTestPeer::LifetimeScanResult;
using ReclaimResult = SlabAllocatorTestPeer::LifetimeReclaimResult;

BlockKey Key(uint64_t id) { return {id, 0, 7}; }

SlabAllocator::Options Options(uint32_t slots, uint32_t extents = 1) {
  SlabAllocator::Options options;
  options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
  options.num_extents = extents;
  options.track_lifetime_state = true;
  return options;
}

void SetTickBudget(Scan& scan, uint64_t victims) {
  scan.initialized = true;
  scan.visit_limit = 4 * victims;
  scan.slice_limit = scan.visit_limit + 1;
}

void Seed(SlabAllocator& allocator, uint64_t id, bool pin = false) {
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(id), 1, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state);
  if (pin) {
    ASSERT_TRUE(allocator.Pin(Key(id)));
  }
}

TEST(SlabLifetimeBackground, VictimAndHeadroomLimitsAreIndependent) {
  SlabAllocator allocator(Options(8));
  for (uint64_t id = 1; id <= 8; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  SetTickBudget(scan, 8);
  std::vector<BlockKey> evicted;
  const ReclaimResult first = SlabAllocatorTestPeer::ReclaimLifetimeClass(
      allocator, 0, 3, 2, scan, &evicted);
  EXPECT_EQ(first.victims, 2U);
  EXPECT_TRUE(first.more);
  const ReclaimResult second = SlabAllocatorTestPeer::ReclaimLifetimeClass(
      allocator, 0, 3, 8, scan, &evicted);
  EXPECT_EQ(second.victims, 1U);
  EXPECT_FALSE(second.more);
  EXPECT_EQ(allocator.Classes()[0].free_slots, 3U);
  EXPECT_EQ(SlabAllocatorTestPeer::ReclaimLifetimeClass(allocator, 0, 3, 8,
                                                        scan, &evicted)
                .victims,
            0U);
  EXPECT_LE(scan.visits, 32U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBackground, EntireAllocatorLockHoldVisitsAtMost1024) {
  SlabAllocator allocator(Options(1200));
  for (uint64_t id = 1; id <= 1200; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, id % 600 != 0));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  SetTickBudget(scan, 512);
  std::vector<BlockKey> evicted;
  const ReclaimResult first = SlabAllocatorTestPeer::ReclaimLifetimeClass(
      allocator, 0, 2, 128, scan, &evicted);
  EXPECT_EQ(first.victims, 1U);
  EXPECT_TRUE(first.more);
  EXPECT_EQ(scan.visits, 1024U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(600)});
  const uint64_t before = scan.visits;
  const ReclaimResult second = SlabAllocatorTestPeer::ReclaimLifetimeClass(
      allocator, 0, 2, 128, scan, &evicted);
  EXPECT_EQ(second.victims, 1U);
  EXPECT_FALSE(second.more);
  EXPECT_LE(scan.visits - before, 1024U);
  EXPECT_EQ(scan.visits, 1200U);
  EXPECT_EQ(evicted, (std::vector<BlockKey>{Key(600), Key(1200)}));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBackground, FourTimesTickCapPersistsAcrossAllocatorCalls) {
  SlabAllocator allocator(Options(16));
  for (uint64_t id = 1; id <= 16; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, id != 16));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  for (unsigned tick = 0; tick < 4; ++tick) {
    Scan scan(config, clock);
    SetTickBudget(scan, 1);
    const ReclaimResult result = SlabAllocatorTestPeer::ReclaimLifetimeClass(
        allocator, 0, 4, 1, scan, &evicted);
    EXPECT_LE(scan.visits, 4U);
    EXPECT_EQ(result.victims, tick == 3 ? 1U : 0U);
    if (tick < 3) {
      EXPECT_EQ(scan.result, ScanResult::kBudgetExhausted);
    }
  }
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(16)});
  EXPECT_EQ(allocator.Count(), 15U);
}

TEST(SlabLifetimeBackground, UnboundCapacityAndInvalidClassDoNotEvict) {
  SlabAllocator allocator(Options(1, 9));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1));
  ASSERT_GT(allocator.PoolExtents(), 0U);
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  SetTickBudget(scan, 8);
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::ReclaimLifetimeClass(allocator, 0, 8, 8,
                                                        scan, &evicted)
                .victims,
            0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ReclaimLifetimeClass(
                allocator, allocator.ClassCount(), 8, 8, scan, &evicted)
                .victims,
            0U);
  EXPECT_EQ(scan.visits, 0U);
  EXPECT_TRUE(evicted.empty());
}

TEST(SlabLifetimeBackground, ReturningAnExtentStopsTheClassPass) {
  SlabAllocator allocator(Options(1, 12));
  std::vector<SlabAllocator::RestoreEntry> records;
  for (uint32_t extent = 0; extent < 12; ++extent)
    records.push_back({Key(extent + 1), 4096, extent, 0, 1});
  ASSERT_TRUE(allocator.RestoreBulk(records));
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  for (uint64_t id = 1; id <= 12; ++id)
    SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state);
  for (uint64_t id = 1; id <= 8; ++id)
    ASSERT_EQ(allocator.Remove(Key(id)), SlabAllocator::RemoveResult::kRemoved);
  ASSERT_EQ(allocator.ExtentReturns(), 0U);
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  SetTickBudget(scan, 12);
  std::vector<BlockKey> evicted;
  const ReclaimResult result = SlabAllocatorTestPeer::ReclaimLifetimeClass(
      allocator, 0, 12, 12, scan, &evicted);
  EXPECT_EQ(result.victims, 1U);
  EXPECT_FALSE(result.more);
  EXPECT_EQ(allocator.ExtentReturns(), 1U);
  EXPECT_EQ(allocator.Count(), 3U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBackground, FailedPersistenceStopsWithoutFreeingOrRetrying) {
  unsigned attempts = 0;
  SlabAllocator::Options options = Options(2);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++attempts;
    return false;
  };
  SlabAllocator allocator(options);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  SetTickBudget(scan, 2);
  std::vector<BlockKey> evicted;
  const ReclaimResult result = SlabAllocatorTestPeer::ReclaimLifetimeClass(
      allocator, 0, 2, 2, scan, &evicted);
  EXPECT_EQ(result.victims, 0U);
  EXPECT_FALSE(result.more);
  EXPECT_EQ(scan.result, ScanResult::kPersistenceError);
  EXPECT_EQ(attempts, 1U);
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeBackground, FailureWithinPieceKeepsEarlierSuccessOnly) {
  unsigned attempts = 0;
  SlabAllocator::Options options = Options(3);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    return ++attempts == 1;
  };
  SlabAllocator allocator(options);
  for (uint64_t id = 1; id <= 3; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  SetTickBudget(scan, 4);
  std::vector<BlockKey> evicted;
  const ReclaimResult result = SlabAllocatorTestPeer::ReclaimLifetimeClass(
      allocator, 0, 2, 4, scan, &evicted);
  EXPECT_EQ(result.victims, 1U);
  EXPECT_FALSE(result.more);
  EXPECT_EQ(scan.result, ScanResult::kPersistenceError);
  EXPECT_EQ(attempts, 2U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_TRUE(allocator.Contains(Key(3)));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

class LifetimeBackgroundStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_bg_" +
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
  DiskSlabStore::Options StoreOptions(uint32_t slots) const {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
    options.capacity_bytes = options.extent_bytes;
    options.slot_granularity = 4096;
    options.reclaim_interval_ms = 0;
    options.table_sync_ms = 0;
    options.lifetime_config = config_;
    options.lifetime_clock = clock_;
    options.lifetime_class_eviction = true;
    return options;
  }
  void Create(uint32_t slots) {
    store_ = std::make_unique<DiskSlabStore>(StoreOptions(slots));
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  void SeedStore(uint32_t count, bool pin = false) {
    for (uint64_t id = 1; id <= count; ++id) {
      const DiskWriteResult result = store_->CacheWithResult(Key(id), "old", 3);
      ASSERT_EQ(result.status, Status::kOk);
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

TEST_F(LifetimeBackgroundStoreTest, QuietTicksResumeUntilSavedHeadroomIsMet) {
  ASSERT_NO_FATAL_FAILURE(Create(16));
  ASSERT_NO_FATAL_FAILURE(SeedStore(16, true));
  uint64_t visits = 0;
  for (unsigned tick = 0; tick < 3; ++tick) {
    DiskSlabStoreTestPeer::Tick(*store_, 1);
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_LE(stats.lifetime_background_scan_visits - visits, 4U);
    visits = stats.lifetime_background_scan_visits;
    EXPECT_EQ(stats.reclaimed_slots, 0U);
  }
  ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(16)));
  DiskSlabStoreTestPeer::Tick(*store_, 1);
  EXPECT_FALSE(store_->IsCached(Key(16)));
  EXPECT_EQ(store_->GetStats().reclaimed_slots, 1U);
  EXPECT_GE(store_->GetStats().lifetime_background_budget_exhausted, 3U);
  for (uint64_t id = 1; id <= 3; ++id)
    ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(id)));
  for (unsigned tick = 0; tick < 3; ++tick)
    DiskSlabStoreTestPeer::Tick(*store_, 1);
  const DiskSlabStore::Stats completed = store_->GetStats();
  EXPECT_EQ(completed.reclaimed_slots, 4U);
  DiskSlabStoreTestPeer::Tick(*store_, 1);
  EXPECT_EQ(store_->GetStats().lifetime_background_scan_visits,
            completed.lifetime_background_scan_visits);
  EXPECT_EQ(store_->TenantUsedBytes(7), 36U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeBackgroundStoreTest, FullPinnedTraversalIsNotBudgetOrIoFailure) {
  ASSERT_NO_FATAL_FAILURE(Create(4));
  ASSERT_NO_FATAL_FAILURE(SeedStore(4, true));
  DiskSlabStoreTestPeer::Tick(*store_, 2);
  const DiskSlabStore::Stats pinned = store_->GetStats();
  EXPECT_EQ(pinned.lifetime_background_no_candidate, 1U);
  EXPECT_EQ(pinned.lifetime_background_budget_exhausted, 0U);
  EXPECT_EQ(pinned.reclaimed_slots, 0U);
  EXPECT_TRUE(store_->Healthy());
  ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(4)));
  DiskSlabStoreTestPeer::Tick(*store_, 2);  // No new PUT renews this demand.
  EXPECT_FALSE(store_->IsCached(Key(4)));
  EXPECT_EQ(store_->GetStats().reclaimed_slots, 1U);
  EXPECT_EQ(store_->GetStats().lifetime_background_budget_exhausted, 0U);
}

TEST_F(LifetimeBackgroundStoreTest,
       DispatchRunsLifetimeWatermarkWithoutLegacyDonor) {
  ASSERT_NO_FATAL_FAILURE(Create(4));
  ASSERT_NO_FATAL_FAILURE(SeedStore(4));
  DiskSlabStoreTestPeer::ForceLegacyWatermark(*store_);
  DiskSlabStoreTestPeer::Dispatch(*store_);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(stats.reclaimed_slots, 0U);
  EXPECT_EQ(stats.watermark_evictions, 4U);
  EXPECT_EQ(stats.watermark_slot_record_clears, 4U);
  EXPECT_EQ(stats.watermark_extent_clears, 0U);
  EXPECT_EQ(stats.rebalanced_extents, 0U);
  EXPECT_EQ(store_->Count(), 0U);
}

TEST_F(LifetimeBackgroundStoreTest, RealInvalidationFailureRetainsResident) {
  ASSERT_NO_FATAL_FAILURE(Create(4));
  ASSERT_NO_FATAL_FAILURE(SeedStore(4));
  DiskSlabStoreTestPeer::FailInvalidation(*store_);
  DiskSlabStoreTestPeer::Tick(*store_, 1);
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(store_->Count(), 4U);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_GT(stats.metadata_io_errors, 0U);
  EXPECT_EQ(stats.reclaimed_slots, 0U);
  EXPECT_EQ(stats.lifetime_background_budget_exhausted, 0U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 12U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeBackgroundStoreTest,
       FailureAfterSuccessfulPiecePreservesOnlyCompletedInvalidations) {
  ASSERT_NO_FATAL_FAILURE(Create(600));
  ASSERT_NO_FATAL_FAILURE(SeedStore(600));
  unsigned yields = 0;
  DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
    EXPECT_EQ(store_->GetStats().reclaimed_slots, 128U);
    DiskSlabStoreTestPeer::FailInvalidation(*store_);
    ++yields;
  });
  DiskSlabStoreTestPeer::Tick(*store_, 256);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(yields, 1U);
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(stats.reclaimed_slots, 128U);
  EXPECT_EQ(stats.metadata_io_errors, 1U);
  EXPECT_EQ(stats.committed_objects, 472U);
  EXPECT_EQ(store_->Count(), 472U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 3U * 472U);
  EXPECT_EQ(stats.lifetime_background_budget_exhausted, 0U);
  EXPECT_EQ(stats.lifetime_background_no_candidate, 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeBackgroundStoreTest, HardClockStopsOnlyUnstartedPieces) {
  ASSERT_NO_FATAL_FAILURE(Create(600));
  ASSERT_NO_FATAL_FAILURE(SeedStore(600));
  unsigned yields = 0;
  DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
    seconds_.store(uint64_t{UINT32_MAX} + 1);
    ++yields;
  });
  DiskSlabStoreTestPeer::Tick(*store_, 256);
  const DiskSlabStore::Stats stopped = store_->GetStats();
  EXPECT_EQ(yields, 1U);
  EXPECT_EQ(stopped.reclaimed_slots, 128U);
  EXPECT_EQ(stopped.committed_objects, 472U);
  EXPECT_EQ(store_->Count(), 472U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 3U * 472U);
  EXPECT_EQ(stopped.metadata_io_errors, 0U);
  EXPECT_EQ(stopped.lifetime_background_budget_exhausted, 0U);
  EXPECT_TRUE(store_->Healthy());
  seconds_.store(0);  // A faulty rollback cannot reopen the hard clock gate.
  DiskSlabStoreTestPeer::Tick(*store_, 256);
  EXPECT_EQ(store_->GetStats().lifetime_background_scan_visits,
            stopped.lifetime_background_scan_visits);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeBackgroundStoreTest,
       BothLifecycleLocksAreReleasedBetweenPieces) {
  ASSERT_NO_FATAL_FAILURE(Create(1200));
  for (uint64_t id = 1; id <= 1200; ++id) {
    ASSERT_EQ(store_->Cache(Key(id), "old", 3), Status::kOk);
    if (id % 600 != 0) {
      ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(id)));
    }
  }
  unsigned yields = 0;
  uint64_t previous = 0;
  DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_LE(stats.lifetime_background_scan_visits - previous, 1024U);
    previous = stats.lifetime_background_scan_visits;
    std::string bytes;
    EXPECT_EQ(store_->Range(Key(1), 0, 0, &bytes), Status::kOk);
    EXPECT_EQ(bytes, "old");
    ++yields;
  });
  DiskSlabStoreTestPeer::Tick(*store_, 512);
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_GE(yields, 1U);
  EXPECT_LE(stats.lifetime_background_scan_visits, 2048U);
  EXPECT_EQ(stats.reclaimed_slots, 2U);
  EXPECT_FALSE(store_->IsCached(Key(600)));
  EXPECT_FALSE(store_->IsCached(Key(1200)));
  EXPECT_TRUE(store_->Healthy());
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeBackgroundStoreTest, ActualBackgroundThreadReclaimsAndJoins) {
  DiskSlabStore::Options options = StoreOptions(16);
  options.reclaim_interval_ms = 1;
  store_ = std::make_unique<DiskSlabStore>(options);
  ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  ASSERT_NO_FATAL_FAILURE(SeedStore(16));
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (store_->GetStats().reclaimed_slots == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_GT(store_->GetStats().reclaimed_slots, 0U);
  EXPECT_LT(store_->Count(), 16U);
  store_.reset();  // Joins the real reclaim thread before reopening its files.
  store_ = std::make_unique<DiskSlabStore>(StoreOptions(16));
  ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  EXPECT_EQ(store_->GetStats().unclean_resets, 0U);
  for (uint64_t id = 1; id <= 16; ++id) {
    if (!store_->IsCached(Key(id))) continue;
    std::string bytes;
    ASSERT_EQ(store_->Range(Key(id), 0, 0, &bytes), Status::kOk);
    EXPECT_EQ(bytes, "old");
  }
}

}  // namespace
}  // namespace dfkv
