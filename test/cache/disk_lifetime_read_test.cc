#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
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
#include "cache/store_engine.h"
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
  static std::unique_lock<std::mutex> HoldLifecycle(DiskSlabStore& store) {
    return std::unique_lock<std::mutex>(store.mu_);
  }
  static std::pair<size_t, size_t> MetadataCapacity(
      const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return {SlabAllocatorTestPeer::GenerationCapacity(*store.alloc_),
            SlabAllocatorTestPeer::LifetimeStateCapacity(*store.alloc_)};
  }
  static void ReadHook(DiskSlabStore& store, std::function<void()> hook) {
    store.read_acquired_hook_for_test_ = std::move(hook);
  }
  static void FailReadInvalidation(DiskSlabStore& store) {
    store.record_write_hook_for_test_ = [] { return false; };
  }
  static SlabAllocator::RemoveResult RemoveAllocatorResidentForTest(
      DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    // Deliberately corrupt only the in-memory ledger for the defensive
    // diagnostic branch. This is not ordinary Remove or a production race:
    // the Store payload and real persistent record intentionally remain.
    return store.alloc_->Remove(key);
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
    EXPECT_EQ(counters.race_lost, counters.race_identity_scope +
                                      counters.race_not_visible +
                                      counters.race_generation_mismatch +
                                      counters.race_allocator_unavailable);
    EXPECT_EQ(counters.successful,
              counters.source_unclassified + counters.source_ram +
                  counters.source_disk + counters.source_copy_leader +
                  counters.source_copy_follower +
                  counters.source_transport_grant);
    EXPECT_EQ(counters.source_transport_grant,
              counters.transport_unclassified + counters.transport_ram_hit +
                  counters.transport_disk_staging +
                  counters.transport_disk_to_ram +
                  counters.transport_published_follower +
                  counters.transport_synchronous_fallback);
    EXPECT_EQ(counters.transport_synchronous_fallback,
              counters.transport_fallback_unclassified +
                  counters.transport_fallback_ram_hit +
                  counters.transport_fallback_disk +
                  counters.transport_fallback_copy_leader +
                  counters.transport_fallback_copy_follower);
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

TEST_F(DiskLifetimeReadTest,
       TransportPreparationPartitionPreservesCoarseTotal) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  const PreparedRead::Source sources[] = {
      PreparedRead::Source::kUnclassified,
      PreparedRead::Source::kRamHit,
      PreparedRead::Source::kDiskStaging,
      PreparedRead::Source::kDiskToRam,
      PreparedRead::Source::kPublishedFollower,
      PreparedRead::Source::kSynchronousFallback};
  uint64_t repeats = 1;
  for (const PreparedRead::Source source : sources) {
    for (uint64_t i = 0; i < repeats; ++i) {
      store_->NotifyLifetimeReadSuccess(
          key_, identity, 5, DiskSlabStore::LifetimeReadSource::kTransportGrant,
          source);
    }
    ++repeats;
  }
  DiskSlabStore::LifetimeReadCounters counters = Counters();
  EXPECT_EQ(counters.successful, 21U);
  EXPECT_EQ(counters.source_transport_grant, 21U);
  EXPECT_EQ(counters.transport_unclassified, 1U);
  EXPECT_EQ(counters.transport_ram_hit, 2U);
  EXPECT_EQ(counters.transport_disk_staging, 3U);
  EXPECT_EQ(counters.transport_disk_to_ram, 4U);
  EXPECT_EQ(counters.transport_published_follower, 5U);
  EXPECT_EQ(counters.transport_synchronous_fallback, 6U);
  // Empty events stay outside both partitions. A non-transport read
  // ignores the optional transport tag rather than counting a second source.
  store_->NotifyLifetimeReadSuccess(
      key_, identity, 0, DiskSlabStore::LifetimeReadSource::kTransportGrant,
      PreparedRead::Source::kRamHit);
  store_->NotifyLifetimeReadSuccess(key_, identity, 5,
                                    DiskSlabStore::LifetimeReadSource::kDisk,
                                    PreparedRead::Source::kRamHit);
  counters = Counters();
  EXPECT_EQ(counters.successful, 22U);
  EXPECT_EQ(counters.source_disk, 1U);
  EXPECT_EQ(counters.source_transport_grant, 21U);
  EXPECT_EQ(counters.transport_ram_hit, 2U);
}

TEST_F(DiskLifetimeReadTest, SynchronousFallbackPartitionPreservesBothTotals) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  const PreparedRead::Source sources[] = {
      PreparedRead::Source::kSynchronousFallback,
      PreparedRead::Source::kSynchronousRamHit,
      PreparedRead::Source::kSynchronousDisk,
      PreparedRead::Source::kSynchronousCopyLeader,
      PreparedRead::Source::kSynchronousCopyFollower};
  uint64_t repeats = 1;
  for (const PreparedRead::Source source : sources) {
    for (uint64_t i = 0; i < repeats; ++i) {
      store_->NotifyLifetimeReadSuccess(
          key_, identity, 5, DiskSlabStore::LifetimeReadSource::kTransportGrant,
          source);
    }
    // Neither an empty grant nor a non-transport source adds a fallback event.
    store_->NotifyLifetimeReadSuccess(
        key_, identity, 0, DiskSlabStore::LifetimeReadSource::kTransportGrant,
        source);
    store_->NotifyLifetimeReadSuccess(
        key_, identity, 5, DiskSlabStore::LifetimeReadSource::kDisk, source);
    ++repeats;
  }
  DiskSlabStore::LifetimeReadCounters counters = Counters();
  EXPECT_EQ(counters.successful, 20U);
  EXPECT_EQ(counters.source_disk, 5U);
  EXPECT_EQ(counters.source_transport_grant, 15U);
  EXPECT_EQ(counters.transport_synchronous_fallback, 15U);
  EXPECT_EQ(counters.transport_fallback_unclassified, 1U);
  EXPECT_EQ(counters.transport_fallback_ram_hit, 2U);
  EXPECT_EQ(counters.transport_fallback_disk, 3U);
  EXPECT_EQ(counters.transport_fallback_copy_leader, 4U);
  EXPECT_EQ(counters.transport_fallback_copy_follower, 5U);
  // Provenance is independent of whether the successful grant can be heated.
  store_->NotifyLifetimeReadSuccess(
      key_, {}, 5, DiskSlabStore::LifetimeReadSource::kTransportGrant,
      PreparedRead::Source::kSynchronousDisk);
  counters = Counters();
  EXPECT_EQ(counters.no_binding, 1U);
  EXPECT_EQ(counters.transport_synchronous_fallback, 16U);
  EXPECT_EQ(counters.transport_fallback_disk, 4U);
  ASSERT_EQ(store_->Remove(key_), Status::kOk);
  store_->NotifyLifetimeReadSuccess(
      key_, identity, 5, DiskSlabStore::LifetimeReadSource::kTransportGrant,
      PreparedRead::Source::kSynchronousCopyFollower);
  counters = Counters();
  EXPECT_EQ(counters.race_lost, 1U);
  EXPECT_EQ(counters.transport_fallback_copy_follower, 6U);
  store_.reset();
  ASSERT_NO_FATAL_FAILURE(Create(false));
  const uint64_t samples = samples_.load(std::memory_order_relaxed);
  store_->NotifyLifetimeReadSuccess(
      key_, identity, 5, DiskSlabStore::LifetimeReadSource::kTransportGrant,
      PreparedRead::Source::kSynchronousRamHit);
  EXPECT_EQ(Counters().successful, 0U);
  EXPECT_EQ(samples_.load(std::memory_order_relaxed), samples);
}

TEST_F(DiskLifetimeReadTest, SourcesPartitionSuccessfulNotifications) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  const DiskSlabStore::LifetimeReadSource sources[] = {
      DiskSlabStore::LifetimeReadSource::kUnclassified,
      DiskSlabStore::LifetimeReadSource::kRam,
      DiskSlabStore::LifetimeReadSource::kDisk,
      DiskSlabStore::LifetimeReadSource::kCopyLeader,
      DiskSlabStore::LifetimeReadSource::kCopyFollower,
      DiskSlabStore::LifetimeReadSource::kTransportGrant};
  // These are real notification applications, not fabricated counter values.
  // The caller supplies the path; this Store test does not simulate transport.
  uint64_t notifications = 0;
  for (const DiskSlabStore::LifetimeReadSource source : sources) {
    ++notifications;
    for (uint64_t i = 0; i < notifications; ++i) {
      store_->NotifyLifetimeReadSuccess(key_, identity, 5, source);
    }
  }
  const DiskSlabStore::LifetimeReadCounters counters = Counters();
  EXPECT_EQ(counters.successful, 21U);
  EXPECT_EQ(counters.applied, 21U);
  EXPECT_EQ(counters.source_unclassified, 1U);
  EXPECT_EQ(counters.source_ram, 2U);
  EXPECT_EQ(counters.source_disk, 3U);
  EXPECT_EQ(counters.source_copy_leader, 4U);
  EXPECT_EQ(counters.source_copy_follower, 5U);
  EXPECT_EQ(counters.source_transport_grant, 6U);
  EXPECT_EQ(State().scoring_reads(), 21U);
}

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

TEST_F(DiskLifetimeReadTest,
       ReorderedCompletedReadsUseMonotonicApplicationTime) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  const uint64_t early = 100 + config_.protect_sec - 1;
  seconds_.store(early);
  std::string first_bytes;
  DiskResidentIdentity first_source;
  ASSERT_EQ(store_->RangeWithIdentity(key_, 0, 0, &first_bytes, &first_source),
            Status::kOk);
  seconds_.store(early + 2);
  std::string second_bytes;
  DiskResidentIdentity second_source;
  ASSERT_EQ(
      store_->RangeWithIdentity(key_, 0, 0, &second_bytes, &second_source),
      Status::kOk);
  ASSERT_TRUE(first_source == identity);
  ASSERT_TRUE(second_source == identity);
  ASSERT_EQ(first_bytes, "value");
  ASSERT_EQ(second_bytes, first_bytes);

  store_->NotifyLifetimeReadSuccess(key_, second_source, second_bytes.size());
  seconds_.store(early);  // Older completion arrives after the newer event.
  store_->NotifyLifetimeReadSuccess(key_, first_source, first_bytes.size());
  const DiskLifetimeState state = State();
  EXPECT_EQ(state.anchor, config_.protect_sec + 1);
  EXPECT_EQ(state.actual_reads, 2U);
  EXPECT_EQ(state.scoring_reads(), 2U);
  EXPECT_FALSE(state.first_early_read());
  EXPECT_FALSE(state.write_armed());
  EXPECT_EQ(Counters().applied, 2U);
}

TEST_F(DiskLifetimeReadTest, LifecycleLockWaitSamplesOnlyAfterUnlock) {
  for (const bool cross_hard : {false, true}) {
    SCOPED_TRACE(cross_hard);
    store_.reset();
    std::filesystem::remove_all(dir_);
    seconds_.store(100);
    clock_ = std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(config_), [this] {
          samples_.fetch_add(1, std::memory_order_relaxed);
          return seconds_.load(std::memory_order_relaxed);
        });
    ASSERT_NO_FATAL_FAILURE(Create());
    Put();
    seconds_.store(100 + config_.protect_sec - 1);
    std::string bytes;
    DiskResidentIdentity source;
    ASSERT_EQ(store_->RangeWithIdentity(key_, 0, 0, &bytes, &source),
              Status::kOk);
    std::unique_lock<std::mutex> lifecycle =
        DiskSlabStoreTestPeer::HoldLifecycle(*store_);
    const uint64_t before = samples_.load();
    std::promise<void> entered;
    std::promise<void> finished;
    const std::future<void> requested = entered.get_future();
    const std::future<void> completion = finished.get_future();
    std::thread notifier([&] {
      entered.set_value();
      store_->NotifyLifetimeReadSuccess(key_, source, bytes.size());
      finished.set_value();
    });
    requested.wait();
    // D is owned by this thread. The bounded wait observes an outstanding
    // operation, not a throughput/p99 threshold; unlock on every branch.
    const std::future_status waiting =
        completion.wait_for(std::chrono::milliseconds(30));
    const uint64_t while_locked = samples_.load();
    seconds_.store(cross_hard ? uint64_t{UINT32_MAX} + 101
                              : 101 + config_.protect_sec);
    lifecycle.unlock();
    notifier.join();
    EXPECT_EQ(waiting, std::future_status::timeout);
    EXPECT_EQ(while_locked, before);
    EXPECT_EQ(Counters().successful, 1U);
    EXPECT_EQ(Counters().applied, cross_hard ? 0U : 1U);
    EXPECT_EQ(Counters().clock_hard_exhausted, cross_hard ? 1U : 0U);
    EXPECT_EQ(State().anchor, cross_hard ? 0U : config_.protect_sec + 1);
    EXPECT_FALSE(State().first_early_read());
  }
}

TEST_F(DiskLifetimeReadTest, ReadHeatKeepsMetadataCapacityAtItsHighWaterMark) {
  ASSERT_NO_FATAL_FAILURE(Create());
  Put();
  const BlockKey other{2, 0};
  ASSERT_EQ(store_->Cache(other, "other", 5), Status::kOk);
  const std::pair<size_t, size_t> allocated =
      DiskSlabStoreTestPeer::MetadataCapacity(*store_);
  ASSERT_GE(allocated.first, 2U);
  ASSERT_GE(allocated.second, 2U);
  ASSERT_EQ(store_->Remove(other), Status::kOk);
  std::string bytes;
  DiskResidentIdentity source;
  ASSERT_EQ(store_->RangeWithIdentity(key_, 0, 0, &bytes, &source),
            Status::kOk);
  for (unsigned i = 0; i < 100; ++i)
    store_->NotifyLifetimeReadSuccess(key_, source, bytes.size());
  EXPECT_EQ(DiskSlabStoreTestPeer::MetadataCapacity(*store_), allocated);
  EXPECT_EQ(Counters().applied, 100U);
  RecordProperty("generation_capacity_bytes",
                 std::to_string(allocated.first * sizeof(uint64_t)));
  RecordProperty("state_capacity_bytes",
                 std::to_string(allocated.second *
                                sizeof(std::optional<DiskLifetimeState>)));
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
  EXPECT_EQ(counters.race_identity_scope, 2U);
  EXPECT_EQ(counters.race_generation_mismatch, 1U);
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
  EXPECT_EQ(Counters().race_generation_mismatch, 1U);
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
  EXPECT_EQ(Counters().race_not_visible, 1U);
}

TEST_F(DiskLifetimeReadTest, RaceReasonsPartitionAtValidationBoundaries) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity identity = Put();
  DiskResidentIdentity wrong = identity;
  ++wrong.store_instance;
  store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  wrong = identity;
  ++wrong.generation;
  for (unsigned i = 0; i < 3; ++i)
    store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  ASSERT_EQ(store_->Remove(key_), Status::kOk);
  // Scope wins over hidden visibility, which wins over a wrong generation.
  wrong = identity;
  ++wrong.run_epoch;
  store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  wrong = identity;
  ++wrong.generation;
  for (unsigned i = 0; i < 5; ++i)
    store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  // An incomplete tuple wins over wrong scope and hidden visibility.
  wrong.store_instance = identity.store_instance + 1;
  wrong.generation = 0;
  store_->NotifyLifetimeReadSuccess(key_, wrong, 5);
  const DiskResidentIdentity replacement = Put();
  ASSERT_EQ(
      DiskSlabStoreTestPeer::RemoveAllocatorResidentForTest(*store_, key_),
      SlabAllocator::RemoveResult::kRemoved);
  ASSERT_TRUE(store_->IsCached(key_));
  const uint64_t samples = samples_.load();
  for (unsigned i = 0; i < 7; ++i)
    store_->NotifyLifetimeReadSuccess(key_, replacement, 5);
  const DiskSlabStore::LifetimeReadCounters counters = Counters();
  EXPECT_EQ(counters.successful, 18U);
  EXPECT_EQ(counters.race_lost, 17U);
  EXPECT_EQ(counters.no_binding, 1U);
  EXPECT_EQ(counters.applied, 0U);
  EXPECT_EQ(counters.race_identity_scope, 2U);
  EXPECT_EQ(counters.race_not_visible, 5U);
  EXPECT_EQ(counters.race_generation_mismatch, 3U);
  EXPECT_EQ(counters.race_allocator_unavailable, 7U);
  EXPECT_EQ(samples_.load(), samples);
  // The fixture removed only allocator metadata, not the real disk record.
  // Restart reconstructs that record and begins fresh runtime counters.
  store_.reset();
  ASSERT_NO_FATAL_FAILURE(Create());
  std::string bytes;
  EXPECT_EQ(store_->Range(key_, 0, 0, &bytes), Status::kOk);
  EXPECT_EQ(bytes, "value");
  EXPECT_EQ(Counters().successful, 0U);
}

TEST_F(DiskLifetimeReadTest, LifecycleWaitRejectsReplacementBeforeSampling) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity old = Put();
  ASSERT_EQ(store_->Remove(key_), Status::kOk);
  const DiskResidentIdentity replacement = Put();
  ASSERT_NE(old.generation, replacement.generation);
  std::unique_lock<std::mutex> lifecycle =
      DiskSlabStoreTestPeer::HoldLifecycle(*store_);
  const uint64_t samples = samples_.load();
  std::promise<void> entered;
  std::promise<void> finished;
  const std::future<void> requested = entered.get_future();
  const std::future<void> completion = finished.get_future();
  std::thread notifier([&] {
    entered.set_value();
    store_->NotifyLifetimeReadSuccess(key_, old, 5);
    finished.set_value();
  });
  requested.wait();
  const std::future_status waiting =
      completion.wait_for(std::chrono::milliseconds(30));
  // A stale generation must be rejected before even sampling an exhausted
  // clock. This wait is a functional handshake, not a performance threshold.
  seconds_.store(uint64_t{std::numeric_limits<uint32_t>::max()} + 101);
  lifecycle.unlock();
  notifier.join();
  EXPECT_EQ(waiting, std::future_status::timeout);
  EXPECT_EQ(samples_.load(), samples);
  EXPECT_EQ(State().actual_reads, 0U);
  const DiskSlabStore::LifetimeReadCounters counters = Counters();
  EXPECT_EQ(counters.successful, 1U);
  EXPECT_EQ(counters.race_lost, 1U);
  EXPECT_EQ(counters.race_generation_mismatch, 1U);
  EXPECT_EQ(counters.clock_hard_exhausted, 0U);
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
  store_->NotifyLifetimeReadSuccess(
      key_, {}, 5, DiskSlabStore::LifetimeReadSource::kTransportGrant,
      PreparedRead::Source::kDiskStaging);
  EXPECT_EQ(Counters().transport_disk_staging, 1U);
  EXPECT_EQ(Counters().io_unhealthy, 1U);
  EXPECT_EQ(Counters().no_binding, 0U);
  EXPECT_EQ(samples_.load(), samples);
}

TEST_F(DiskLifetimeReadTest, DefaultOffAndZeroBytesNeverSampleOrCount) {
  ASSERT_NO_FATAL_FAILURE(Create(false));
  EXPECT_FALSE(store_->TracksLifetimeState());
  const DiskResidentIdentity identity = Put();
  const uint64_t samples = samples_.load();
  store_->NotifyLifetimeReadSuccess(key_, identity, 5,
                                    DiskSlabStore::LifetimeReadSource::kRam);
  store_->NotifyLifetimeReadSuccess(
      key_, identity, 5, DiskSlabStore::LifetimeReadSource::kTransportGrant,
      PreparedRead::Source::kRamHit);
  EXPECT_EQ(Counters().source_transport_grant, 0U);
  EXPECT_EQ(Counters().transport_ram_hit, 0U);
  EXPECT_EQ(samples_.load(), samples);
  EXPECT_EQ(Counters().successful, 0U);
  store_.reset();
  std::filesystem::remove_all(dir_);
  ASSERT_NO_FATAL_FAILURE(Create());
  const DiskResidentIdentity enabled = Put();
  const uint64_t enabled_samples = samples_.load();
  store_->NotifyLifetimeReadSuccess(
      key_, enabled, 0, DiskSlabStore::LifetimeReadSource::kTransportGrant);
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
