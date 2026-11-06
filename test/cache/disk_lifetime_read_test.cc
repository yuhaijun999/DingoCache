#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
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
  static SlabAllocatorTestPeer::LifetimeSnapshot ReadSnapshot(
      const DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeSnapshotFor(*store.alloc_, key);
  }
  static void ReadHook(DiskSlabStore& store, std::function<void()> hook) {
    store.read_acquired_hook_for_test_ = std::move(hook);
  }
  static void FailReadInvalidation(DiskSlabStore& store) {
    store.record_write_hook_for_test_ = [] { return false; };
  }
};
namespace {

class DiskLifetimeReadTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_read_" +
         std::to_string(
             ::testing::UnitTest::GetInstance()->current_test_info()->line()));
    std::filesystem::remove_all(dir_);
    clock_ = std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(config_), [this] {
          samples_.fetch_add(1, std::memory_order_relaxed);
          return seconds_.load(std::memory_order_relaxed);
        });
  }
  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  void Create(bool enabled = true) {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.capacity_bytes = 8192;
    options.extent_bytes = 8192;
    options.slot_granularity = 4096;
    options.table_sync_ms = 0;
    options.reclaim_interval_ms = 0;
    options.direct_writes = false;
    options.track_resident_identity = true;
    options.lifetime_clock = clock_;
    if (enabled) options.lifetime_config = config_;
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  DiskResidentIdentity Put() {
    const DiskWriteResult result = store_->CacheWithResult(key_, "value", 5);
    EXPECT_EQ(result.status, Status::kOk);
    EXPECT_TRUE(result.identity.valid());
    return result.identity;
  }
  DiskLifetimeState State() const {
    const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
        DiskSlabStoreTestPeer::ReadSnapshot(*store_, key_);
    if (!snapshot.second.has_value()) {
      ADD_FAILURE() << "missing lifetime state";
      return {};
    }
    return *snapshot.second;
  }
  DiskSlabStore::LifetimeReadCounters Counters() const {
    const DiskSlabStore::LifetimeReadCounters counters =
        store_->GetStats().lifetime_reads;
    EXPECT_EQ(counters.successful,
              counters.applied + counters.no_binding + counters.race_lost +
                  counters.clock_hard_exhausted + counters.io_unhealthy +
                  counters.state_unavailable);
    return counters;
  }
  const BlockKey key_{1, 0};
  DiskLifetimeConfig config_;
  std::atomic<uint64_t> seconds_{100};
  std::atomic<uint64_t> samples_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::unique_ptr<DiskSlabStore> store_;
  std::filesystem::path dir_;
};

TEST_F(DiskLifetimeReadTest, AppliesAtNotificationTimeNotStorageReadTime) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_TRUE(store_->TracksLifetimeState());
  const DiskResidentIdentity identity = Put();
  seconds_.store(100 + config_.protect_sec);
  std::string bytes;
  DiskResidentIdentity source;
  ASSERT_EQ(store_->RangeWithIdentity(key_, 0, 0, &bytes, &source),
            Status::kOk);
  EXPECT_TRUE(source == identity);
  EXPECT_EQ(State().actual_reads, 0U);
  seconds_.fetch_add(1);
  store_->NotifyLifetimeReadSuccess(key_, source, bytes.size());
  const DiskLifetimeState state = State();
  EXPECT_EQ(state.anchor, config_.protect_sec + 1);
  EXPECT_EQ(state.put_at, 0U);
  EXPECT_EQ(state.scoring_reads(), 1U);
  EXPECT_EQ(state.actual_reads, 1U);
  EXPECT_FALSE(state.first_early_read());
  EXPECT_FALSE(state.write_armed());
  EXPECT_EQ(Counters().applied, 1U);
}

TEST_F(DiskLifetimeReadTest, InclusiveProtectBoundaryAndSameSecondEachCount) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  seconds_.store(100 + config_.protect_sec);
  store_->NotifyLifetimeReadSuccess(key_, identity, 5);
  store_->NotifyLifetimeReadSuccess(key_, identity, 5);
  const DiskLifetimeState state = State();
  EXPECT_EQ(state.anchor, config_.protect_sec);
  EXPECT_EQ(state.actual_reads, 2U);
  EXPECT_EQ(state.scoring_reads(), 2U);
  EXPECT_FALSE(state.first_early_read());
  EXPECT_EQ(ClassifyDiskLifetimeRead(config_, state, config_.protect_sec),
            DiskLifetimeReadClass::kReadWithinNewWriteWindow);
  EXPECT_EQ(Counters().applied, 2U);
}

TEST_F(DiskLifetimeReadTest, SaturatedReadCountStillRefreshesAnchor) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  for (uint64_t i = 1; i <= 70; ++i) {
    seconds_.store(100 + i);
    store_->NotifyLifetimeReadSuccess(key_, identity, 5);
  }
  const DiskLifetimeState state = State();
  EXPECT_EQ(state.scoring_reads(), 63U);
  EXPECT_EQ(state.actual_reads, 3U);
  EXPECT_EQ(state.anchor, 70U);
  EXPECT_EQ(Counters().applied, 70U);
}

TEST_F(DiskLifetimeReadTest,
       ConcurrentLogicalSuccessesDoNotLoseSameSecondReads) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  seconds_.store(105);
  std::vector<std::thread> readers;
  for (unsigned i = 0; i < 4; ++i) {
    readers.emplace_back([&] {
      for (unsigned read = 0; read < 10; ++read)
        store_->NotifyLifetimeReadSuccess(key_, identity, 5);
    });
  }
  for (std::thread& reader : readers) reader.join();
  const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
      DiskSlabStoreTestPeer::ReadSnapshot(*store_, key_);
  EXPECT_EQ(snapshot.first, identity.generation);
  if (!snapshot.second.has_value()) {
    ADD_FAILURE() << "missing lifetime state";
    return;
  }
  EXPECT_EQ(snapshot.second->anchor, 5U);
  EXPECT_EQ(snapshot.second->put_at, 0U);
  EXPECT_EQ(snapshot.second->scoring_reads(), 40U);
  EXPECT_EQ(snapshot.second->actual_reads, 3U);
  EXPECT_EQ(Counters().applied, 40U);
}

TEST_F(DiskLifetimeReadTest, RejectsForeignAndStaleCredentialsBeforeSampling) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  const uint64_t samples = samples_.load();
  DiskResidentIdentity wrong = identity;
  ++wrong.generation;
  store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  wrong = identity;
  ++wrong.store_instance;
  store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  wrong = identity;
  ++wrong.run_epoch;
  store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  store_->NotifyLifetimeReadSuccess(key_, {}, 5);
  EXPECT_EQ(samples_.load(), samples);
  EXPECT_EQ(State().actual_reads, 0U);
  const DiskSlabStore::LifetimeReadCounters counters = Counters();
  EXPECT_EQ(counters.race_lost, 3U);
  EXPECT_EQ(counters.no_binding, 1U);
  EXPECT_EQ(counters.successful, 4U);
}

TEST_F(DiskLifetimeReadTest, SameKeyReplacementNeverInheritsOldOrRamHeat) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity old = Put();
  ASSERT_EQ(store_->Remove(key_), Status::kOk);
  const DiskResidentIdentity current = Put();
  ASSERT_NE(current.generation, old.generation);
  store_->NotifyLifetimeReadSuccess(key_, old, 5);
  store_->NotifyLifetimeReadSuccess(key_, {}, 5);
  EXPECT_EQ(State().actual_reads, 0U);
  EXPECT_EQ(Counters().race_lost, 1U);
  EXPECT_EQ(Counters().no_binding, 1U);
}

TEST_F(DiskLifetimeReadTest, LastPinRemovePreservesSourceButRejectsHeat) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  DiskSlabStoreTestPeer::ReadHook(
      *store_, [this] { EXPECT_EQ(store_->Remove(key_), Status::kOk); });
  std::string bytes;
  DiskResidentIdentity source;
  ASSERT_EQ(store_->RangeWithIdentity(key_, 0, 0, &bytes, &source),
            Status::kOk);
  EXPECT_EQ(bytes, "value");
  EXPECT_TRUE(source == identity);
  EXPECT_FALSE(store_->IsCached(key_));
  store_->NotifyLifetimeReadSuccess(key_, source, bytes.size());
  EXPECT_EQ(Counters().race_lost, 1U);
}

TEST_F(DiskLifetimeReadTest,
       FailedLastPinInvalidationClearsSourceAndDoesNotNotify) {
  ASSERT_NO_FATAL_FAILURE(Create());
  Put();
  DiskSlabStoreTestPeer::ReadHook(*store_, [this] {
    EXPECT_EQ(store_->Remove(key_), Status::kOk);
    DiskSlabStoreTestPeer::FailReadInvalidation(*store_);
  });
  std::string bytes;
  DiskResidentIdentity source;
  EXPECT_EQ(store_->RangeWithIdentity(key_, 0, 0, &bytes, &source),
            Status::kIOError);
  EXPECT_FALSE(source.valid());
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(Counters().successful, 0U);
}

TEST_F(DiskLifetimeReadTest, SoftCrossingAppliesButHardCrossingIsSticky) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  const uint64_t hard = std::numeric_limits<uint32_t>::max();
  seconds_.store(100 + hard - DiskLifetimeHorizon(config_) + 1);
  store_->NotifyLifetimeReadSuccess(key_, identity, 5);
  EXPECT_EQ(Counters().applied, 1U);
  const uint32_t soft_anchor = State().anchor;
  seconds_.store(100 + hard + 1);
  store_->NotifyLifetimeReadSuccess(key_, identity, 5);
  seconds_.store(101);
  store_->NotifyLifetimeReadSuccess(key_, identity, 5);
  EXPECT_EQ(Counters().clock_hard_exhausted, 2U);
  EXPECT_EQ(State().anchor, soft_anchor);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(DiskLifetimeReadTest,
       RealIoFailureHasPriorityOverMissingBindingAndClock) {
  ASSERT_NO_FATAL_FAILURE(Create());
  Put();
  DiskSlabStoreTestPeer::FailReadInvalidation(*store_);
  EXPECT_EQ(store_->Remove(key_), Status::kIOError);
  EXPECT_FALSE(store_->Healthy());
  seconds_.store(uint64_t{std::numeric_limits<uint32_t>::max()} + 101);
  const uint64_t samples = samples_.load();
  store_->NotifyLifetimeReadSuccess(key_, {}, 5);
  EXPECT_EQ(Counters().io_unhealthy, 1U);
  EXPECT_EQ(Counters().no_binding, 0U);
  EXPECT_EQ(samples_.load(), samples);
}

TEST_F(DiskLifetimeReadTest, DefaultOffAndZeroBytesNeverSampleOrCount) {
  ASSERT_NO_FATAL_FAILURE(Create(false));
  EXPECT_FALSE(store_->TracksLifetimeState());
  const DiskResidentIdentity identity = Put();
  const uint64_t samples = samples_.load();
  store_->NotifyLifetimeReadSuccess(key_, identity, 5);
  EXPECT_EQ(samples_.load(), samples);
  EXPECT_EQ(Counters().successful, 0U);
  store_.reset();
  std::filesystem::remove_all(dir_);
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity enabled = Put();
  const uint64_t enabled_samples = samples_.load();
  store_->NotifyLifetimeReadSuccess(key_, enabled, 0);
  EXPECT_EQ(samples_.load(), enabled_samples);
  EXPECT_EQ(Counters().successful, 0U);
}

TEST(LifetimeReadStateValidation, UninitializedAndFutureStateFailClosed) {
  const DiskLifetimeConfig config;
  uint64_t now = 100;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return now; });
  SlabAllocator::Options options;
  options.extent_bytes = 8192;
  options.num_extents = 1;
  options.track_lifetime_state = true;
  SlabAllocator allocator(options);
  const BlockKey empty{1, 0};
  const BlockKey future{2, 0};
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(empty, 4096, &ref, &evicted));
  SlabAllocatorTestPeer::CommitGeneration(&allocator, empty, 1);
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, empty, 1,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kStateUnavailable);
  ASSERT_TRUE(allocator.Put(future, 4096, &ref, &evicted));
  const DiskLifetimeState state = NewDiskLifetimeState(10);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, future, 2, &state);
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, future, 2,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kStateUnavailable);
  const SlabAllocatorTestPeer::LifetimeSnapshot snapshot =
      SlabAllocatorTestPeer::LifetimeSnapshotFor(allocator, future);
  if (!snapshot.second.has_value()) {
    ADD_FAILURE() << "missing lifetime state";
    return;
  }
  EXPECT_EQ(snapshot.second->anchor, 10U);
  EXPECT_EQ(snapshot.second->actual_reads, 0U);
  now = uint64_t{std::numeric_limits<uint32_t>::max()} + 101;
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, empty, 1,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kClockHardExhausted);
}

}  // namespace
}  // namespace dfkv
