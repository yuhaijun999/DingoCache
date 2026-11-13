#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cache/disk_slab_store.h"

// Third-party headers.
#include <gtest/gtest.h>

#include "cache/disk_eviction_ghost.h"
#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"

namespace {
// Only this executable replaces allocation. Arming is thread-local and limited
// to a peer's recording call; GTest and authoritative Store work stay unarmed.
thread_local int allocations_before_failure = -1;
thread_local bool allocation_failed = false;
}  // namespace

// Keep replaceable allocation/deallocation boundaries out of line, including
// under optimized builds; their implementation is deliberately malloc/free.
[[gnu::noinline]] void* operator new(std::size_t size) {
  if (allocations_before_failure == 0) {
    allocations_before_failure = -1;
    allocation_failed = true;
    throw std::bad_alloc();
  }
  if (allocations_before_failure > 0) --allocations_before_failure;
  if (void* memory = std::malloc(size == 0 ? 1 : size)) return memory;
  throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
[[gnu::noinline]] void operator delete(void* memory) noexcept {
  std::free(memory);
}
[[gnu::noinline]] void operator delete[](void* memory) noexcept {
  std::free(memory);
}
[[gnu::noinline]] void operator delete(void* memory, std::size_t) noexcept {
  std::free(memory);
}
[[gnu::noinline]] void operator delete[](void* memory, std::size_t) noexcept {
  std::free(memory);
}

// Include the nothrow family: standard-library temporary buffers use it, and
// sanitizer interceptors must not mix their allocation provenance with free.
[[gnu::noinline]] void* operator new(std::size_t size,
                                     const std::nothrow_t&) noexcept {
  try {
    return ::operator new(size);
  } catch (const std::bad_alloc&) {
    return nullptr;
  }
}
[[gnu::noinline]] void* operator new[](std::size_t size,
                                       const std::nothrow_t& tag) noexcept {
  return ::operator new(size, tag);
}
[[gnu::noinline]] void operator delete(void* memory,
                                       const std::nothrow_t&) noexcept {
  ::operator delete(memory);
}
[[gnu::noinline]] void operator delete[](void* memory,
                                         const std::nothrow_t&) noexcept {
  ::operator delete[](memory);
}

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static std::optional<uint64_t> Age(const DiskSlabStore& store,
                                     const BlockKey& key, uint64_t now) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.eviction_ghost_
               ? store.eviction_ghost_->LookupEvictionAge(key, now)
               : std::nullopt;
  }
  static bool Retained(const DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->Contains(key);
  }
  static bool Pin(DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->Pin(key);
  }
  static bool Unpin(DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->Unpin(key);
  }
  static void FailMetadata(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    ::close(store.table_fd_);
    store.table_fd_ = -1;
  }
  static void FailSecondInvalidation(DiskSlabStore& store) {
    SlabAllocatorTestPeer::ObserveSlotEvict(*store.alloc_,
                                            [&store, attempt = 0U]() mutable {
                                              if (++attempt == 2) {
                                                ::close(store.table_fd_);
                                                store.table_fd_ = -1;
                                              }
                                            });
  }
  static void ExhaustClockOnInvalidation(DiskSlabStore& store,
                                         std::atomic<uint64_t>& seconds) {
    SlabAllocatorTestPeer::ObserveSlotEvict(*store.alloc_, [&seconds] {
      seconds = std::numeric_limits<uint64_t>::max();
    });
  }
  static void FailPayload(DiskSlabStore& store) {
    store.payload_write_hook_for_test_ = [] { return false; };
  }
  static bool Background(DiskSlabStore& store, size_t target) {
    return store.ReclaimLifetimeClass(0, target, 16);
  }
  static void Watermark(DiskSlabStore& store) {
    store.evict_high_bytes_ = 1;
    store.evict_low_bytes_ = 0;
    store.ReclaimTick();
  }
  static bool Bind(DiskSlabStore& store, uint32_t bytes, uint32_t extent) {
    return SlabAllocatorTestPeer::BindExtent(
               *store.alloc_,
               SlabAllocatorTestPeer::ClassIndex(*store.alloc_, bytes),
               extent) == SlabAllocatorTestPeer::ExtentBindResult::kBound;
  }
  static void Quiesce(DiskSlabStore& store) {
    const std::vector<SlabAllocator::ClassStat> classes =
        store.alloc_->Classes();
    store.reclaim_last_puts_.resize(classes.size());
    for (size_t cls = 0; cls < classes.size(); ++cls) {
      store.reclaim_last_puts_[cls] = classes[cls].puts;
    }
  }
  static bool Grow(DiskSlabStore& store) {
    return store.ReclaimLifetimeGrowTick();
  }
  // Tests the real auxiliary allocation boundary, not the entire PUT's OOM
  // behavior. All input vectors and assertions are prepared outside the fault.
  static void Record(DiskSlabStore& store, const std::vector<BlockKey>& evicted,
                     int fail_after = -1) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    allocations_before_failure = fail_after;
    store.RecordEvictionGhostLocked(evicted);
    allocations_before_failure = -1;
  }
};
namespace {
BlockKey Key(uint64_t id) { return {id, 0, 7}; }

class DiskEvictionGhostStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("dfkv_ghost_store_" + std::to_string(::getpid()) + "_" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    config_.base_sec = 100;
    config_.read_bonus_sec = 10;
    config_.protect_sec = 8;
    config_.slowdown = 4;
    config_.keep_score_sec = 90;
    config_.bucket_sec = 16;
    config_.bucket_count = 8;
    config_.high_pct = 0;
    clock_ = std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(config_), [this] { return seconds_.load(); });
  }
  void TearDown() override {
    allocations_before_failure = -1;
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  DiskSlabStore::Options Options(uint32_t slots = 4,
                                 uint32_t extents = 1) const {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
    options.capacity_bytes = options.extent_bytes * extents;
    options.slot_granularity = 4096;
    options.reclaim_interval_ms = options.table_sync_ms = 0;
    options.lifetime_config = config_;
    options.lifetime_clock = clock_;
    options.lifetime_class_eviction = true;
    options.eviction_ghost_capacity = 2;
    return options;
  }
  void Create(uint32_t slots = 4, uint32_t extents = 1) {
    store_ = std::make_unique<DiskSlabStore>(Options(slots, extents));
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  void Seed(uint64_t count) {
    for (uint64_t id = 1; id <= count; ++id) {
      ASSERT_EQ(store_->Cache(Key(id), "old", 3), Status::kOk);
    }
  }
  std::filesystem::path dir_;
  DiskLifetimeConfig config_;
  std::atomic<uint64_t> seconds_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::unique_ptr<DiskSlabStore> store_;
};

TEST_F(DiskEvictionGhostStoreTest, DisabledKeepsSuccessfulEvictionsUnobserved) {
  DiskSlabStore::Options options = Options(1);
  options.eviction_ghost_capacity = 0;
  store_ = std::make_unique<DiskSlabStore>(options);
  ASSERT_TRUE(store_->Healthy());
  ASSERT_NO_FATAL_FAILURE(Seed(2));
  EXPECT_EQ(store_->Evictions(), 1U);
  const DiskSlabStore::EvictionGhostObservation observation =
      store_->ObserveEvictionGhost();
  EXPECT_EQ(observation.history.capacity, 0U);
  EXPECT_EQ(observation.recorded, 0U);
  EXPECT_EQ(observation.dropped, 0U);
}

TEST_F(DiskEvictionGhostStoreTest, RejectsNonLifetimeBeforeOpeningFiles) {
  for (const bool shadow : {false, true}) {
    SCOPED_TRACE(shadow);
    DiskSlabStore::Options options = Options();
    options.lifetime_class_eviction = false;
    if (!shadow) {
      options.lifetime_config.reset();
      options.lifetime_clock.reset();
    }
    const DiskSlabStore rejected(options);
    EXPECT_FALSE(rejected.Healthy());
    EXPECT_FALSE(rejected.StartupError().empty());
    EXPECT_FALSE(std::filesystem::exists(dir_));
  }
}

TEST_F(DiskEvictionGhostStoreTest,
       ForegroundRecordsOnlySuccessfulUnpinnedVictim) {
  ASSERT_NO_FATAL_FAILURE(Create(2));
  ASSERT_NO_FATAL_FAILURE(Seed(2));
  ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(1)));
  seconds_ = 31;
  ASSERT_EQ(store_->Cache(Key(3), "new", 3), Status::kOk);
  EXPECT_TRUE(store_->IsCached(Key(1)));
  EXPECT_FALSE(store_->IsCached(Key(2)));
  EXPECT_EQ(DiskSlabStoreTestPeer::Age(*store_, Key(2), 40), 9U);
  EXPECT_FALSE(DiskSlabStoreTestPeer::Age(*store_, Key(1), 40));
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 1U);
  ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(1)));
}

TEST_F(DiskEvictionGhostStoreTest, FailedInvalidationDoesNotInventEviction) {
  ASSERT_NO_FATAL_FAILURE(Create(1));
  ASSERT_NO_FATAL_FAILURE(Seed(1));
  DiskSlabStoreTestPeer::FailMetadata(*store_);
  EXPECT_EQ(store_->Cache(Key(2), "new", 3), Status::kIOError);
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 0U);
  EXPECT_EQ(store_->ObserveEvictionGhost().dropped, 0U);
  EXPECT_FALSE(DiskSlabStoreTestPeer::Age(*store_, Key(1), 0));
}

TEST_F(DiskEvictionGhostStoreTest, LaterPayloadFailureKeepsSuccessfulEviction) {
  ASSERT_NO_FATAL_FAILURE(Create(1));
  ASSERT_NO_FATAL_FAILURE(Seed(1));
  seconds_ = 22;
  DiskSlabStoreTestPeer::FailPayload(*store_);
  EXPECT_EQ(store_->Cache(Key(2), "new", 3), Status::kIOError);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_EQ(DiskSlabStoreTestPeer::Age(*store_, Key(1), 25), 3U);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 1U);
}

TEST_F(DiskEvictionGhostStoreTest, BackgroundBoundsRetentionAndSkipsPinned) {
  ASSERT_NO_FATAL_FAILURE(Create(4));
  ASSERT_NO_FATAL_FAILURE(Seed(4));
  ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(1)));
  seconds_ = 23;
  // False means the target is met (no pending headroom), not failed eviction.
  EXPECT_FALSE(DiskSlabStoreTestPeer::Background(*store_, 3));
  const DiskSlabStore::EvictionGhostObservation observation =
      store_->ObserveEvictionGhost();
  EXPECT_EQ(observation.recorded, 3U);
  EXPECT_EQ(observation.history.retained, 2U);
  EXPECT_TRUE(observation.history.capacity_limited);
  EXPECT_TRUE(store_->IsCached(Key(1)));
  EXPECT_FALSE(DiskSlabStoreTestPeer::Age(*store_, Key(1), 30));
  EXPECT_EQ(observation.history.oldest_evicted_at_sec, 23U);
  ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(1)));
}

TEST_F(DiskEvictionGhostStoreTest, WatermarkRecordsAllRemovedKeysOnce) {
  ASSERT_NO_FATAL_FAILURE(Create(4));
  ASSERT_NO_FATAL_FAILURE(Seed(4));
  seconds_ = 44;
  DiskSlabStoreTestPeer::Watermark(*store_);
  EXPECT_EQ(store_->Count(), 0U);
  EXPECT_EQ(store_->GetStats().watermark_slot_removals, 4U);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 4U);
  EXPECT_EQ(store_->ObserveEvictionGhost().history.newest_evicted_at_sec, 44U);
}

TEST_F(DiskEvictionGhostStoreTest,
       ForegroundDonorRecordsResidentKeysNotExtents) {
  ASSERT_NO_FATAL_FAILURE(Create(4, 10));
  for (uint32_t extent = 0; extent < 10; ++extent) {
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 4096, extent));
  }
  ASSERT_NO_FATAL_FAILURE(Seed(10));
  seconds_ = 12;
  const std::string large(8192, 'n');
  ASSERT_EQ(store_->Cache(Key(100), large.data(), large.size()), Status::kOk);
  const uint64_t victims = store_->GetStats().lifetime_donor_released_residents;
  ASSERT_GT(victims, 0U);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, victims);
  EXPECT_EQ(store_->ObserveEvictionGhost().history.newest_evicted_at_sec, 12U);
}

TEST_F(DiskEvictionGhostStoreTest,
       BackgroundDonorRecordsEachSuccessfulResident) {
  ASSERT_NO_FATAL_FAILURE(Create(4, 10));
  for (uint32_t extent = 0; extent < 9; ++extent) {
    ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 4096, extent));
  }
  ASSERT_TRUE(DiskSlabStoreTestPeer::Bind(*store_, 8192, 9));
  ASSERT_NO_FATAL_FAILURE(Seed(36));
  DiskSlabStoreTestPeer::Quiesce(*store_);
  const std::string large(5000, 'n');
  ASSERT_EQ(store_->Cache(Key(100), large.data(), large.size()), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(101), large.data(), large.size()), Status::kOk);
  seconds_ = 12;
  ASSERT_TRUE(DiskSlabStoreTestPeer::Grow(*store_));
  EXPECT_EQ(store_->GetStats().lifetime_donor_released_residents, 4U);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 4U);
  EXPECT_EQ(store_->ObserveEvictionGhost().history.retained, 2U);
}

TEST_F(DiskEvictionGhostStoreTest, RemoveRewriteAndCleanRecoveryDoNotRecord) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(Seed(2));
  ASSERT_EQ(store_->Cache(Key(1), "rewrite", 7), Status::kOk);
  ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(1)));
  ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
  ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(1)));
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 0U);
  store_.reset();
  ASSERT_NO_FATAL_FAILURE(Create());
  EXPECT_TRUE(store_->IsCached(Key(2)));
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 0U);
  EXPECT_EQ(store_->ObserveEvictionGhost().history.retained, 0U);
}

TEST_F(DiskEvictionGhostStoreTest,
       ClockFailureDropsEvidenceNotAuthoritativeData) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const std::vector<BlockKey> batch{Key(1), Key(2)};
  seconds_ = std::numeric_limits<uint64_t>::max();
  DiskSlabStoreTestPeer::Record(*store_, batch);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 0U);
  EXPECT_EQ(store_->ObserveEvictionGhost().dropped, 2U);
  EXPECT_TRUE(store_->ObserveEvictionGhost().history.loss_time_unknown);
  EXPECT_FALSE(store_->ObserveEvictionGhost().history.lost_through_sec);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(DiskEvictionGhostStoreTest,
       AllocationFailurePreservesHistoryAndResumes) {
  // Sweep the actual list-node/map-node/initial-bucket allocations. A fresh
  // container per countdown avoids assuming implementation-specific counts.
  unsigned failures = 0;
  for (int countdown = 0; countdown < 5; ++countdown) {
    SCOPED_TRACE(countdown);
    store_.reset();
    std::filesystem::remove_all(dir_);
    ASSERT_NO_FATAL_FAILURE(Create());
    const std::vector<BlockKey> first{Key(1)};
    allocation_failed = false;
    DiskSlabStoreTestPeer::Record(*store_, first, countdown);
    const bool failed = allocation_failed;
    const DiskSlabStore::EvictionGhostObservation observation =
        store_->ObserveEvictionGhost();
    EXPECT_EQ(observation.recorded, failed ? 0U : 1U);
    EXPECT_EQ(observation.dropped, failed ? 1U : 0U);
    EXPECT_EQ(observation.history.retained, failed ? 0U : 1U);
    if (failed) ++failures;
    DiskSlabStoreTestPeer::Record(*store_, first);
    const std::vector<BlockKey> second{Key(2)};
    DiskSlabStoreTestPeer::Record(*store_, second);
    const std::vector<BlockKey> third{Key(3)};
    // At full capacity both node allocations must happen before old removal.
    DiskSlabStoreTestPeer::Record(*store_, third, countdown % 2);
    EXPECT_TRUE(DiskSlabStoreTestPeer::Age(*store_, Key(1), 0));
    EXPECT_TRUE(DiskSlabStoreTestPeer::Age(*store_, Key(2), 0));
    EXPECT_FALSE(store_->ObserveEvictionGhost().history.capacity_limited);
    DiskSlabStoreTestPeer::Record(*store_, third);
    EXPECT_FALSE(DiskSlabStoreTestPeer::Age(*store_, Key(1), 0));
    EXPECT_EQ(DiskSlabStoreTestPeer::Age(*store_, Key(3), 0), 0U);
    EXPECT_TRUE(store_->ObserveEvictionGhost().history.capacity_limited);
    EXPECT_TRUE(store_->Healthy());
  }
  EXPECT_GE(failures, 3U);
}

TEST_F(DiskEvictionGhostStoreTest,
       AllocationGapRemainsAfterSuccessfulRecording) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const std::vector<BlockKey> batch{Key(1)};
  seconds_ = 12;
  allocation_failed = false;
  DiskSlabStoreTestPeer::Record(*store_, batch, 0);
  ASSERT_TRUE(allocation_failed);
  EXPECT_EQ(store_->ObserveEvictionGhost().dropped, 1U);
  EXPECT_EQ(store_->ObserveEvictionGhost().history.lost_through_sec, 12U);
  EXPECT_FALSE(store_->ObserveEvictionGhost().history.loss_time_unknown);
  seconds_ = 15;
  DiskSlabStoreTestPeer::Record(*store_, batch);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 1U);
  EXPECT_EQ(store_->ObserveEvictionGhost().history.lost_through_sec, 12U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Age(*store_, Key(1), 20), 5U);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(DiskEvictionGhostStoreTest, LaterInvalidationFailureKeepsEarlierVictim) {
  ASSERT_NO_FATAL_FAILURE(Create(4));
  ASSERT_NO_FATAL_FAILURE(Seed(4));
  seconds_ = 17;
  DiskSlabStoreTestPeer::FailSecondInvalidation(*store_);
  DiskSlabStoreTestPeer::Background(*store_, 4);
  EXPECT_FALSE(store_->Healthy());
  EXPECT_EQ(store_->Count(), 3U);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 1U);
  EXPECT_EQ(store_->ObserveEvictionGhost().dropped, 0U);
  unsigned absent = 0;
  for (uint64_t id = 1; id <= 4; ++id) {
    // Public IsCached deliberately rejects all keys when unhealthy; inspect
    // retained ownership to distinguish failed invalidation from success.
    const bool removed = !DiskSlabStoreTestPeer::Retained(*store_, Key(id));
    EXPECT_EQ(DiskSlabStoreTestPeer::Age(*store_, Key(id), 20).has_value(),
              removed);
    if (removed) ++absent;
  }
  EXPECT_EQ(absent, 1U);
}

TEST_F(DiskEvictionGhostStoreTest,
       PostInvalidationClockFailureStillCleansPayload) {
  ASSERT_NO_FATAL_FAILURE(Create(1));
  ASSERT_NO_FATAL_FAILURE(Seed(1));
  DiskSlabStoreTestPeer::ExhaustClockOnInvalidation(*store_, seconds_);
  // Reuse succeeds even though both optional ghost time and NEW heat time
  // become unavailable. Neither may resurrect the successfully evicted key.
  ASSERT_EQ(store_->Cache(Key(2), "new", 3), Status::kOk);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_TRUE(store_->IsCached(Key(2)));
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_EQ(store_->EvictedBytes(), 3U);
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 0U);
  EXPECT_EQ(store_->ObserveEvictionGhost().dropped, 1U);
  EXPECT_TRUE(store_->ObserveEvictionGhost().history.loss_time_unknown);
  EXPECT_FALSE(store_->ObserveEvictionGhost().history.lost_through_sec);
  EXPECT_TRUE(store_->Healthy());
}

TEST_F(DiskEvictionGhostStoreTest, RepeatedRealEvictionRefreshesSameKey) {
  ASSERT_NO_FATAL_FAILURE(Create(1));
  ASSERT_NO_FATAL_FAILURE(Seed(1));
  for (const uint64_t id : {2U, 1U, 3U, 1U, 4U}) {
    seconds_.fetch_add(1);
    ASSERT_EQ(store_->Cache(Key(id), "new", 3), Status::kOk);
  }
  const DiskSlabStore::EvictionGhostObservation observation =
      store_->ObserveEvictionGhost();
  EXPECT_EQ(observation.recorded, 5U);
  EXPECT_EQ(observation.history.retained, 2U);
  EXPECT_TRUE(observation.history.capacity_limited);
  EXPECT_EQ(DiskSlabStoreTestPeer::Age(*store_, Key(1), 10), 5U);
}

TEST_F(DiskEvictionGhostStoreTest, ConcurrentSnapshotsConserveCompletedEvents) {
  ASSERT_NO_FATAL_FAILURE(Create(1));
  ASSERT_NO_FATAL_FAILURE(Seed(1));
  std::atomic<bool> done{false};
  std::thread observer([&] {
    while (!done.load()) {
      const DiskSlabStore::EvictionGhostObservation observation =
          store_->ObserveEvictionGhost();
      EXPECT_LE(observation.history.retained, observation.history.capacity);
      EXPECT_LE(observation.history.retained, observation.recorded);
      EXPECT_EQ(observation.dropped, 0U);
      EXPECT_FALSE(observation.history.loss_time_unknown);
      if (observation.history.capacity_limited) {
        EXPECT_EQ(observation.history.lost_through_sec, 0U);
      } else {
        EXPECT_FALSE(observation.history.lost_through_sec.has_value());
      }
    }
  });
  // No fatal assertion while a joinable observer remains.
  for (uint64_t id = 2; id <= 50; ++id) {
    EXPECT_EQ(store_->Cache(Key(id), "new", 3), Status::kOk);
  }
  done = true;
  observer.join();
  EXPECT_EQ(store_->ObserveEvictionGhost().recorded, 49U);
  EXPECT_EQ(store_->Evictions(), 49U);
}

}  // namespace
}  // namespace dfkv
