#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/slab_allocator.h"
#include "cache/store_engine.h"
#include "common/status.h"

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static void FailInvalidation(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) ::close(store.table_fd_);
    store.table_fd_ = -1;
  }
};
namespace {
using Observation = SlabAllocator::LifetimeReadClassObservation;

class DiskLifetimeReadClassTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.high_pct = 0;
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_read_class_" +
         std::to_string(
             ::testing::UnitTest::GetInstance()->current_test_info()->line()));
    std::filesystem::remove_all(dir_);
    clock_ =
        std::make_shared<DiskLifetimeClock>(DiskLifetimeHorizon(config_), [&] {
          samples_.fetch_add(1);
          return seconds_.load();
        });
  }
  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  DiskSlabStore::Options Options() const {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.capacity_bytes = 65536;
    options.extent_bytes = 65536;
    options.slot_granularity = 4096;
    options.table_sync_ms = 0;
    options.reclaim_interval_ms = 0;
    options.direct_writes = false;
    options.lifetime_config = config_;
    options.lifetime_clock = clock_;
    options.lifetime_class_eviction = true;
    return options;
  }
  void Create() {
    store_ = std::make_unique<DiskSlabStore>(Options());
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  void ReadAndNotify() {
    std::string value;
    DiskResidentIdentity identity;
    ASSERT_EQ(store_->RangeWithIdentity({1, 0}, 0, 0, &value, &identity),
              Status::kOk);
    ASSERT_EQ(value, "old");
    store_->NotifyLifetimeReadSuccess({1, 0}, identity, value.size(),
                                      DiskSlabStore::LifetimeReadSource::kDisk);
  }
  void ExpectCounts(const std::array<uint64_t, 5>& expected) const {
    const Observation observation = store_->ObserveLifetimeReadClasses();
    EXPECT_TRUE(observation.available);
    EXPECT_EQ(observation.resident_estimates, expected);
    EXPECT_EQ(observation.sampled_at_sec, seconds_.load());
    EXPECT_EQ(observation.expiry_quantum_sec, 8U);
    EXPECT_EQ(observation.expiry_lag_bound_sec, 7U);
    EXPECT_EQ(observation.ring_bytes, 24U * 32U);
  }
  DiskLifetimeConfig config_;
  std::atomic<uint64_t> seconds_{0};
  std::atomic<uint64_t> samples_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::unique_ptr<DiskSlabStore> store_;
  std::filesystem::path dir_;
};

TEST_F(DiskLifetimeReadClassTest, RealReadAtInclusiveWindowExpiresWithoutGet) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache({1, 0}, "old", 3), Status::kOk);
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({1, 0, 0, 0, 0}));
  seconds_.store(180);  // Inclusive R boundary, despite no early-read bonus.
  ASSERT_NO_FATAL_FAILURE(ReadAndNotify());
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({0, 1, 0, 0, 0}));
  seconds_.store(181);  // Exact expiry; estimate can lag to the next Q=8 page.
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({0, 1, 0, 0, 0}));
  seconds_.store(183);
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({0, 1, 0, 0, 0}));
  seconds_.store(184);  // ceil((180+1)/8)*8, no intervening GET.
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({0, 0, 1, 0, 0}));
  EXPECT_EQ(store_->GetStats().lifetime_reads.applied, 1U);
  std::string value;
  EXPECT_EQ(store_->Range({1, 0}, 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "old");
}

TEST_F(DiskLifetimeReadClassTest, CleanRecoveryVirtualReadsRemainUnread) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache({1, 0}, "old", 3), Status::kOk);
  store_.reset();
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({1, 0, 0, 0, 0}));
  EXPECT_EQ(store_->GetStats().lifetime_reads.applied, 0U);
  ASSERT_NO_FATAL_FAILURE(ReadAndNotify());
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({0, 0, 1, 0, 0}));
  EXPECT_EQ(store_->GetStats().lifetime_reads.applied, 1U);
}

TEST_F(DiskLifetimeReadClassTest, HiddenPinnedRemoveCountsUntilFinalRelease) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache({1, 0}, "old", 3), Status::kOk);
  ASSERT_NO_FATAL_FAILURE(ReadAndNotify());
  ReadLease lease;
  ASSERT_EQ(store_->RangeDirectPrep({1, 0}, 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(store_->Remove({1, 0}), Status::kOk);
  EXPECT_FALSE(store_->IsCached({1, 0}));
  EXPECT_EQ(store_->GetStats().committed_objects, 0U);
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({0, 1, 0, 0, 0}));
  lease = ReadLease{};  // Release through the public RAII lifetime boundary.
  EXPECT_TRUE(store_->Healthy());
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({0, 0, 0, 0, 0}));
  EXPECT_EQ(store_->GetStats().allocator_objects, 0U);
}

TEST_F(DiskLifetimeReadClassTest, FailedInvalidationRetainsHiddenPopulation) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache({1, 0}, "old", 3), Status::kOk);
  ReadLease lease;
  ASSERT_EQ(store_->RangeDirectPrep({1, 0}, 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(store_->Remove({1, 0}), Status::kOk);
  DiskSlabStoreTestPeer::FailInvalidation(*store_);
  lease = ReadLease{};  // Real deferred invalidation fails during release.
  EXPECT_FALSE(store_->Healthy());
  EXPECT_GT(store_->GetStats().metadata_io_errors, 0U);
  EXPECT_EQ(store_->GetStats().committed_objects, 0U);
  EXPECT_EQ(store_->GetStats().allocator_objects, 1U);
  ASSERT_NO_FATAL_FAILURE(ExpectCounts({1, 0, 0, 0, 0}));
}

TEST_F(DiskLifetimeReadClassTest, LegacyAndShadowObservationDoNotSampleClock) {
  for (const bool shadow : {false, true}) {
    SCOPED_TRACE(shadow);
    DiskSlabStore::Options options = Options();
    options.dir = (dir_ / (shadow ? "shadow" : "legacy")).string();
    options.lifetime_class_eviction = false;
    if (!shadow) {
      options.lifetime_config.reset();
      options.lifetime_clock.reset();
    }
    DiskSlabStore store(options);
    ASSERT_TRUE(store.Healthy()) << store.StartupError();
    ASSERT_EQ(store.Cache({1, 0}, "old", 3), Status::kOk);
    const uint64_t before = samples_.load();
    const Observation observation = store.ObserveLifetimeReadClasses();
    EXPECT_FALSE(observation.available);
    EXPECT_EQ(observation.resident_estimates, (std::array<uint64_t, 5>{}));
    EXPECT_EQ(observation.ring_bytes, 0U);
    EXPECT_EQ(observation.expiry_quantum_sec, 0U);
    EXPECT_EQ(samples_.load(), before);
  }
}

TEST_F(DiskLifetimeReadClassTest, FailedStartupObservationDoesNotSampleClock) {
  DiskSlabStore::Options options = Options();
  options.extent_bytes = 0;  // Reject before the allocator can be constructed.
  const DiskSlabStore store(options);
  ASSERT_FALSE(store.Healthy());
  const uint64_t before = samples_.load();
  const Observation observation = store.ObserveLifetimeReadClasses();
  EXPECT_FALSE(observation.available);
  EXPECT_EQ(observation.resident_estimates, (std::array<uint64_t, 5>{}));
  EXPECT_EQ(observation.ring_bytes, 0U);
  EXPECT_EQ(samples_.load(), before);
}

TEST_F(DiskLifetimeReadClassTest, HardClockKeepsBackingButNoUsableCounts) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_EQ(store_->Cache({1, 0}, "old", 3), Status::kOk);
  seconds_.store(static_cast<uint64_t>(UINT32_MAX) + 1);
  const Observation unavailable = store_->ObserveLifetimeReadClasses();
  EXPECT_FALSE(unavailable.available);
  EXPECT_EQ(unavailable.resident_estimates, (std::array<uint64_t, 5>{}));
  EXPECT_EQ(unavailable.ring_bytes, 24U * 32U);
  EXPECT_EQ(unavailable.expiry_quantum_sec, 8U);
  EXPECT_EQ(unavailable.expiry_lag_bound_sec, 7U);
  seconds_.store(0);
  EXPECT_FALSE(store_->ObserveLifetimeReadClasses().available);
  EXPECT_TRUE(store_->Healthy());
  EXPECT_TRUE(store_->IsCached({1, 0}));
}

}  // namespace
}  // namespace dfkv
