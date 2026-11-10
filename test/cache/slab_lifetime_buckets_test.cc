#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <future>
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
#include "cache/slab_allocator.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static uint64_t Estimate(const DiskSlabStore& store, uint64_t now) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::ProtectedEstimate(*store.alloc_, 0, now);
  }
  static uint64_t Token(const DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::BucketToken(*store.alloc_, key);
  }
  static size_t Pages(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::BucketPageCount(*store.alloc_);
  }
  static bool Valid(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeBucketsValid(*store.alloc_);
  }
  static std::optional<DiskLifetimeState> State(const DiskSlabStore& store,
                                                const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeState(*store.alloc_, key);
  }
  static void FailMetadata(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) ::close(store.table_fd_);
    store.table_fd_ = -1;
  }
};
namespace {
BlockKey Key(uint64_t id) { return {id, 0, 7}; }

DiskLifetimeConfig Config() {
  DiskLifetimeConfig config;
  config.base_sec = 100;
  config.read_bonus_sec = 10;
  config.max_reads = 2;
  config.protect_sec = 8;
  config.slowdown = 4;
  config.keep_score_sec = 90;
  config.bucket_sec = 16;
  config.bucket_count = 8;
  config.high_pct = 0;
  return config;
}

SlabAllocator::Options Options(const DiskLifetimeConfig& config,
                               uint32_t slots = 8, uint32_t extents = 1) {
  SlabAllocator::Options options;
  options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
  options.num_extents = extents;
  options.track_lifetime_state = true;
  options.lifetime_max_reads = static_cast<uint32_t>(config.max_reads);
  options.lifetime_bucket_sec = config.bucket_sec;
  options.lifetime_bucket_count = static_cast<uint32_t>(config.bucket_count);
  return options;
}

void Seed(SlabAllocator& allocator, uint64_t id, const DiskLifetimeState* state,
          const DiskLifetimeConfig& config, uint32_t now = 0,
          uint64_t generation = 0) {
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(id), 1, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id),
                                          generation == 0 ? id : generation,
                                          state, &config, now);
}

uint64_t End(const DiskLifetimeConfig& config, const DiskLifetimeState& state) {
  return DiskLifetimeProtectEnd(config, state.anchor, state.scoring_reads(),
                                state.first_early_read(), state.write_armed());
}

uint64_t TokenForEnd(const DiskLifetimeConfig& config, uint64_t end) {
  return (end - 1) / config.bucket_sec + 1;
}

void ExpectValid(const SlabAllocator& allocator) {
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeBucketsValid(allocator));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

void ExpectObservation(const SlabAllocator& allocator,
                       const DiskLifetimeClock& clock, uint64_t expected) {
  const SlabAllocator::LifetimeBucketObservation observation =
      allocator.ObserveLifetimeBuckets(clock);
  EXPECT_TRUE(observation.available);
  EXPECT_EQ(observation.protected_estimate, expected);
  EXPECT_EQ(observation.summary_bytes,
            SlabAllocatorTestPeer::BucketSummaryCapacity(allocator) *
                SlabAllocatorTestPeer::BucketPageBytes());
  ExpectValid(allocator);
}

// The installed legacy return type must keep both size and field offsets;
// aliasing the larger snapshot would change its return calling convention.
static_assert(sizeof(SlabAllocator::LifetimeBucketMemory) ==
              3 * sizeof(uint64_t));
static_assert(offsetof(SlabAllocator::LifetimeBucketMemory, entries) == 0);
static_assert(offsetof(SlabAllocator::LifetimeBucketMemory, bucket_bytes) ==
              sizeof(uint64_t));
static_assert(offsetof(SlabAllocator::LifetimeBucketMemory, token_bytes) ==
              2 * sizeof(uint64_t));

void ExpectBucketMemoryCompatibility(
    const SlabAllocator& allocator,
    const SlabAllocator::LifetimeMemory& memory) {
  // Compare separate snapshots only while the allocator is quiescent.
  const SlabAllocator::LifetimeBucketMemory original = allocator.BucketMemory();
  EXPECT_EQ(original.entries, memory.entries);
  EXPECT_EQ(original.bucket_bytes, memory.bucket_bytes);
  EXPECT_EQ(original.token_bytes, memory.token_bytes);
}

TEST(SlabLifetimeBuckets, DisabledAllocatorsNeverAllocateBucketStorage) {
  const DiskLifetimeConfig config = Config();
  for (const bool track_state : {false, true}) {
    SlabAllocator::Options options = Options(config);
    options.track_lifetime_state = track_state;
    options.lifetime_bucket_sec = 0;
    options.lifetime_bucket_count = 0;
    SlabAllocator allocator(options);
    SlabAllocator::SlotRef ref;
    std::vector<BlockKey> evicted;
    ASSERT_TRUE(allocator.Put(Key(1), 1, &ref, &evicted));
    EXPECT_EQ(SlabAllocatorTestPeer::BucketPageCount(allocator), 0U);
    EXPECT_EQ(SlabAllocatorTestPeer::BucketTokenSize(allocator), 0U);
    EXPECT_EQ(SlabAllocatorTestPeer::BucketTokenCapacity(allocator), 0U);
    const SlabAllocator::LifetimeMemory memory =
        allocator.LifetimeMemoryUsage();
    ExpectBucketMemoryCompatibility(allocator, memory);
    EXPECT_EQ(memory.entries, 0U);
    EXPECT_EQ(memory.bucket_bytes, 0U);
    EXPECT_EQ(memory.token_bytes, 0U);
    EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 0U);
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeBuckets,
     MemoryViewDistinguishesDisabledIdentityAndShadowModes) {
  const DiskLifetimeConfig config = Config();
  for (const unsigned int mode : {0U, 1U, 2U}) {
    SCOPED_TRACE(mode);
    SlabAllocator::Options options = Options(config);
    options.track_lifetime_state = mode == 2;
    options.track_resident_identity = mode != 0;
    options.lifetime_bucket_sec = 0;
    options.lifetime_bucket_count = 0;
    SlabAllocator allocator(options);
    SlabAllocator::SlotRef ref;
    std::vector<BlockKey> evicted;
    ASSERT_TRUE(allocator.Put(Key(1), 1, &ref, &evicted));
    ASSERT_TRUE(evicted.empty());
    const SlabAllocator::LifetimeMemory memory =
        allocator.LifetimeMemoryUsage();
    ExpectBucketMemoryCompatibility(allocator, memory);
    EXPECT_EQ(memory.identity_bytes != 0, mode != 0);
    EXPECT_EQ(memory.state_bytes != 0, mode == 2);
    EXPECT_EQ(memory.link_bytes != 0, mode == 2);
    EXPECT_EQ(memory.queue_bytes != 0, mode == 2);
    EXPECT_EQ(memory.version_bytes != 0, mode == 2);
    EXPECT_EQ(memory.bucket_bytes, 0U);
    EXPECT_EQ(memory.token_bytes, 0U);
    EXPECT_TRUE(
        SlabAllocatorTestPeer::LifetimeMemoryMatches(allocator, memory));
    ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
    const SlabAllocator::LifetimeMemory removed =
        allocator.LifetimeMemoryUsage();
    ExpectBucketMemoryCompatibility(allocator, removed);
    EXPECT_EQ(removed.identity_bytes, memory.identity_bytes);
    EXPECT_EQ(removed.state_bytes, memory.state_bytes);
    EXPECT_EQ(removed.link_bytes, memory.link_bytes);
    EXPECT_EQ(removed.queue_bytes, memory.queue_bytes);
    EXPECT_EQ(removed.version_bytes, memory.version_bytes);
    EXPECT_TRUE(
        SlabAllocatorTestPeer::LifetimeMemoryMatches(allocator, removed));
  }
}

TEST(SlabLifetimeBuckets, MemoryViewTracksCapacityNotActiveRegistrations) {
  for (const uint64_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    DiskLifetimeConfig config = Config();
    config.max_reads = max_reads;
    config.bucket_count = max_reads == 63 ? 64 : 8;
    std::string error;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
    SlabAllocator allocator(Options(config, 8, 2));
    const SlabAllocator::LifetimeMemory empty = allocator.LifetimeMemoryUsage();
    ExpectBucketMemoryCompatibility(allocator, empty);
    EXPECT_EQ(empty.entries, 2 * config.bucket_count);
    EXPECT_GE(empty.bucket_bytes,
              empty.entries * SlabAllocatorTestPeer::BucketPageBytes());
    EXPECT_EQ(empty.token_bytes, 0U);
    EXPECT_EQ(empty.identity_bytes, 0U);
    EXPECT_EQ(empty.state_bytes, 0U);
    EXPECT_EQ(empty.link_bytes, 0U);
    const size_t classes = allocator.Classes().size();
    const size_t queues = (classes + 1) * (max_reads + 1);
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueCount(allocator), queues);
    EXPECT_GE(empty.queue_bytes,
              queues * SlabAllocatorTestPeer::LifetimeQueueBytes());
    EXPECT_GE(empty.version_bytes, classes * sizeof(uint64_t));
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeMemoryMatches(allocator, empty));
    const DiskLifetimeState state = NewDiskLifetimeState(0);
    for (uint64_t id = 1; id <= 9; ++id)
      ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, &state, config));
    const SlabAllocator::LifetimeMemory populated =
        allocator.LifetimeMemoryUsage();
    ExpectBucketMemoryCompatibility(allocator, populated);
    EXPECT_EQ(populated.entries, empty.entries);
    EXPECT_EQ(populated.bucket_bytes, empty.bucket_bytes);
    EXPECT_GE(populated.token_bytes, 9 * sizeof(uint64_t));
    EXPECT_EQ(populated.token_bytes,
              SlabAllocatorTestPeer::BucketTokenCapacity(allocator) *
                  sizeof(uint64_t));
    EXPECT_GE(populated.identity_bytes, 9 * sizeof(uint64_t));
    EXPECT_GE(populated.state_bytes,
              9 * sizeof(std::optional<DiskLifetimeState>));
    EXPECT_GT(populated.link_bytes, 0U);
    EXPECT_EQ(populated.queue_bytes, empty.queue_bytes);
    EXPECT_EQ(populated.version_bytes, empty.version_bytes);
    EXPECT_TRUE(
        SlabAllocatorTestPeer::LifetimeMemoryMatches(allocator, populated));
    for (uint64_t id = 1; id <= 9; ++id)
      ASSERT_EQ(allocator.Remove(Key(id)),
                SlabAllocator::RemoveResult::kRemoved);
    ASSERT_EQ(allocator.Count(), 0U);
    std::vector<BlockKey> evicted;
    EXPECT_EQ(SlabAllocatorTestPeer::StealGeneric(allocator, 1, &evicted),
              SlabAllocatorTestPeer::ExtentBindResult::kBound);
    EXPECT_TRUE(evicted.empty());
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 10, &state, config));
    const SlabAllocator::LifetimeMemory reused =
        allocator.LifetimeMemoryUsage();
    ExpectBucketMemoryCompatibility(allocator, reused);
    EXPECT_EQ(reused.entries, populated.entries);
    EXPECT_EQ(reused.bucket_bytes, populated.bucket_bytes);
    EXPECT_EQ(reused.token_bytes, populated.token_bytes);
    EXPECT_EQ(reused.identity_bytes, populated.identity_bytes);
    EXPECT_EQ(reused.state_bytes, populated.state_bytes);
    EXPECT_EQ(reused.link_bytes, populated.link_bytes);
    EXPECT_EQ(reused.queue_bytes, populated.queue_bytes);
    EXPECT_EQ(reused.version_bytes, populated.version_bytes);
    EXPECT_TRUE(
        SlabAllocatorTestPeer::LifetimeMemoryMatches(allocator, reused));
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeBuckets, MemorySnapshotIsSafeDuringSlotMetadataGrowth) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config, 128));
  const size_t classes = allocator.Classes().size();
  std::atomic<bool> done{false};
  std::mutex midpoint_mu;
  std::condition_variable midpoint_cv;
  bool midpoint_ready = false;
  bool midpoint_sampled = false;
  bool writer_ok = true;
  std::thread writer([&] {
    for (uint64_t id = 1; id <= 128; ++id) {
      SlabAllocator::SlotRef ref;
      std::vector<BlockKey> evicted;
      if (!allocator.Put(Key(id), 1, &ref, &evicted) || !evicted.empty()) {
        writer_ok = false;
        break;
      }
      if (id == 64) {
        std::unique_lock<std::mutex> lock(midpoint_mu);
        midpoint_ready = true;
        if (!midpoint_cv.wait_for(lock, std::chrono::seconds(5),
                                  [&] { return midpoint_sampled; })) {
          writer_ok = false;
          break;
        }
      }
      std::this_thread::yield();
    }
    done.store(true);
  });
  do {
    // Force a snapshot between two growth phases without holding allocator A
    // while waiting. The timeout also releases the writer after test failure.
    const std::lock_guard<std::mutex> lock(midpoint_mu);
    const SlabAllocator::LifetimeMemory memory =
        allocator.LifetimeMemoryUsage();
    EXPECT_EQ(memory.entries, config.bucket_count);
    EXPECT_GE(memory.bucket_bytes,
              memory.entries * SlabAllocatorTestPeer::BucketPageBytes());
    EXPECT_EQ(memory.token_bytes % sizeof(uint64_t), 0U);
    EXPECT_EQ(memory.identity_bytes % sizeof(uint64_t), 0U);
    EXPECT_EQ(memory.state_bytes % sizeof(std::optional<DiskLifetimeState>),
              0U);
    EXPECT_GE(memory.queue_bytes,
              (classes + 1) * (config.max_reads + 1) *
                  SlabAllocatorTestPeer::LifetimeQueueBytes());
    EXPECT_GE(memory.version_bytes, classes * sizeof(uint64_t));
    if (midpoint_ready) {
      midpoint_sampled = true;
      midpoint_cv.notify_one();
    }
  } while (!done.load());
  writer.join();
  ASSERT_TRUE(writer_ok);
  EXPECT_TRUE(midpoint_sampled);
  const SlabAllocator::LifetimeMemory memory = allocator.LifetimeMemoryUsage();
  ExpectBucketMemoryCompatibility(allocator, memory);
  EXPECT_GE(memory.token_bytes, 128 * sizeof(uint64_t));
  EXPECT_GE(memory.identity_bytes, 128 * sizeof(uint64_t));
  EXPECT_GE(memory.state_bytes, 128 * sizeof(std::optional<DiskLifetimeState>));
  EXPECT_GT(memory.link_bytes, 0U);
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeMemoryMatches(allocator, memory));
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, DisabledObservationDoesNotSampleOrAllocateSummary) {
  const DiskLifetimeConfig config = Config();
  for (const bool track_state : {false, true}) {
    SlabAllocator::Options options = Options(config);
    options.track_lifetime_state = track_state;
    options.lifetime_bucket_count = 0;
    options.lifetime_bucket_sec = 0;
    const SlabAllocator allocator(options);
    uint64_t source_calls = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&source_calls] {
      ++source_calls;
      return uint64_t{0};
    });
    const uint64_t before = source_calls;
    const SlabAllocator::LifetimeBucketObservation observation =
        allocator.ObserveLifetimeBuckets(clock);
    EXPECT_FALSE(observation.available);
    EXPECT_EQ(observation.protected_estimate, 0U);
    EXPECT_EQ(observation.summary_bytes, 0U);
    EXPECT_EQ(SlabAllocatorTestPeer::BucketSummaryCapacity(allocator), 0U);
    EXPECT_EQ(source_calls, before);
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeBuckets, ObservationAggregatesExtentsAndSilentlyExpires) {
  for (const uint64_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    DiskLifetimeConfig config = Config();
    config.max_reads = max_reads;
    config.bucket_count = max_reads == 63 ? 64 : 8;
    std::string error;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
    SlabAllocator allocator(Options(config, 8, 2));
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&seconds] { return seconds; });
    ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
    const DiskLifetimeState written = NewDiskLifetimeState(0);
    const DiskLifetimeState restored = RecoveredDiskLifetimeState(1);
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &written, config));
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, &restored, config));
    ASSERT_EQ(End(config, written), 16U);
    ASSERT_EQ(End(config, restored), 20U);
    // Two striped extent rows contribute to one bounded aggregate ring.
    EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 1U);
    EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 1, 0), 1U);
    for (const std::pair<uint32_t, uint64_t>& sample :
         {std::pair<uint32_t, uint64_t>{0, 2},
          {15, 2},
          {16, 1},
          {20, 1},
          {31, 1},
          {32, 0},
          {100000, 0}}) {
      seconds = sample.first;
      ASSERT_NO_FATAL_FAILURE(
          ExpectObservation(allocator, clock, sample.second));
      EXPECT_EQ(
          sample.second,
          SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, seconds) +
              SlabAllocatorTestPeer::ProtectedEstimate(allocator, 1, seconds));
    }
    EXPECT_EQ(allocator.Count(), 2U);  // Expiry is not physical reclamation.
    ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
    ASSERT_EQ(allocator.Remove(Key(2)), SlabAllocator::RemoveResult::kRemoved);
    ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
  }
}

TEST(SlabLifetimeBuckets, SummaryKeepsNewerGenerationAfterDelayedRegistration) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config, 8, 4));
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t extent = 0; extent < 4; ++extent)
    entries.push_back({Key(extent + 1), 4096, extent, 0, 1});
  ASSERT_TRUE(allocator.RestoreBulk(entries));
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  const DiskLifetimeState old = RecoveredDiskLifetimeState(1);
  DiskLifetimeState current = RecoveredDiskLifetimeState(1);
  current.anchor = 128;
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1, &old, &config,
                                          0);
  seconds = 128;
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(2), 2, &current,
                                          &config, 128);
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(1)), 2U);
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(2)), 10U);
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 1));
  // Original registration work can be delayed, but cannot replace a later
  // aggregate generation in another extent's same ring position.
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(3), 3, &old, &config,
                                          0);
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 1));
  ASSERT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 1));
  ASSERT_EQ(allocator.Remove(Key(2)), SlabAllocator::RemoveResult::kRemoved);
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
  // Retain that newest generation even after its count reaches zero.
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(4), 4, &old, &config,
                                          0);
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
  ASSERT_EQ(allocator.Remove(Key(3)), SlabAllocator::RemoveResult::kRemoved);
  ASSERT_EQ(allocator.Remove(Key(4)), SlabAllocator::RemoveResult::kRemoved);
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
}

TEST(SlabLifetimeBuckets, ObservationHonorsSoftHardAndRollbackClockBounds) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config));
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &state, config));
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 1));
  const uint64_t bytes = allocator.ObserveLifetimeBuckets(clock).summary_bytes;
  ASSERT_GT(bytes, 0U);
  seconds = UINT32_MAX - DiskLifetimeHorizon(config) + 1;
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
  uint32_t offset = 7;
  EXPECT_FALSE(clock.TryNow(&offset));
  EXPECT_EQ(offset, 7U);  // Observation must not reopen admission.
  seconds = UINT32_MAX;
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
  seconds = static_cast<uint64_t>(UINT32_MAX) + 1;
  const SlabAllocator::LifetimeBucketObservation hard =
      allocator.ObserveLifetimeBuckets(clock);
  EXPECT_FALSE(hard.available);
  EXPECT_EQ(hard.protected_estimate, 0U);  // No cached former estimate.
  EXPECT_EQ(hard.summary_bytes, bytes);
  seconds = 0;
  const SlabAllocator::LifetimeBucketObservation rollback =
      allocator.ObserveLifetimeBuckets(clock);
  EXPECT_FALSE(rollback.available);
  EXPECT_EQ(rollback.summary_bytes, bytes);
  EXPECT_FALSE(clock.TryNow(&offset));
  const DiskLifetimeClock empty(DiskLifetimeHorizon(config),
                                DiskLifetimeClock::NowFn{});
  const SlabAllocator::LifetimeBucketObservation unavailable =
      allocator.ObserveLifetimeBuckets(empty);
  EXPECT_FALSE(unavailable.available);
  EXPECT_EQ(unavailable.summary_bytes, bytes);
  EXPECT_EQ(allocator.Count(), 1U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets,
     ConcurrentObservationSeesPositiveResidentBeforeReuse) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config));
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  std::promise<void> first_seen;
  const std::future<void> seen = first_seen.get_future();
  std::atomic<bool> done{false};
  bool writer_ok = true;
  std::thread writer([&] {
    for (uint64_t generation = 1; generation <= 200; ++generation) {
      SlabAllocator::SlotRef ref;
      std::vector<BlockKey> evicted;
      if (!allocator.Put(Key(1), 1, &ref, &evicted) || !evicted.empty()) {
        writer_ok = false;
        break;
      }
      SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), generation,
                                              &state, &config, 0);
      if (generation == 1 &&
          seen.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        writer_ok = false;
        break;
      }
      if (!SlabAllocatorTestPeer::LifetimeBucketsValid(allocator) ||
          allocator.Remove(Key(1)) != SlabAllocator::RemoveResult::kRemoved) {
        writer_ok = false;
        break;
      }
    }
    done.store(true);
  });
  bool observed_positive = false;
  do {
    const SlabAllocator::LifetimeBucketObservation observation =
        allocator.ObserveLifetimeBuckets(clock);
    EXPECT_TRUE(observation.available);
    EXPECT_LE(observation.protected_estimate, 1U);
    EXPECT_GT(observation.summary_bytes, 0U);
    if (!observed_positive && observation.protected_estimate == 1) {
      observed_positive = true;
      first_seen.set_value();
    }
  } while (!done.load());
  writer.join();
  EXPECT_TRUE(writer_ok);
  EXPECT_TRUE(observed_positive);
  ASSERT_NO_FATAL_FAILURE(ExpectObservation(allocator, clock, 0));
}

TEST(SlabLifetimeBuckets, UnknownAndUncommittedResidentsDoNotRegister) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config));
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(1), 1, &ref, &evicted));
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(1)), 0U);
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(1), 1, nullptr,
                                          &config, 0);
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(1)), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::BucketPageCount(allocator),
            config.bucket_count);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, DeadlineBoundariesAndSilentExpiryAreConservative) {
  const DiskLifetimeConfig config = Config();
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  SlabAllocator allocator(Options(config));
  const DiskLifetimeState written = NewDiskLifetimeState(0);
  const DiskLifetimeState restored = RecoveredDiskLifetimeState(1);
  ASSERT_EQ(End(config, written), 16U);
  ASSERT_EQ(End(config, restored), 20U);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &written, config));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, &restored, config));
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 15), 2U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 16), 1U);
  // A non-boundary deadline may remain in the current coarse page, but never
  // after that page closes. No read or background sweep is required.
  EXPECT_EQ(DiskLifetimeScore(config, 0, 1, false, false, 20),
            static_cast<int64_t>(config.keep_score_sec));
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 20), 1U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 31), 1U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 32), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 100000), 0U);
  EXPECT_EQ(allocator.Count(), 2U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, UnregisteredRecoveredResidentCannotDebitItsPeer) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config));
  const DiskLifetimeState expired = RecoveredDiskLifetimeState(1);
  DiskLifetimeState protected_peer = RecoveredDiskLifetimeState(1);
  protected_peer.anchor = 5;
  ASSERT_EQ(TokenForEnd(config, End(config, expired)),
            TokenForEnd(config, End(config, protected_peer)));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &expired, config, 21));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, &protected_peer, config, 21));
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(1)), 0U);
  EXPECT_NE(SlabAllocatorTestPeer::BucketToken(allocator, Key(2)), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 21), 1U);
  EXPECT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 21), 1U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, StalePageTokenCannotDebitReusedGeneration) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config));
  const DiskLifetimeState old = RecoveredDiskLifetimeState(1);
  DiskLifetimeState current = RecoveredDiskLifetimeState(1);
  current.anchor = 128;
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &old, config));
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, &current, config, 128));
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(1)), 2U);
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(2)), 10U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 128), 1U);
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 128;
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 128), 2U);
  EXPECT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 128), 1U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 256), 0U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, SameSecondAndSaturatedReadsRegisterExactlyOnce) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config));
  const DiskLifetimeState initial = RecoveredDiskLifetimeState(1);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &initial, config));
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 7;
  for (unsigned read = 0; read < 70; ++read) {
    ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                       config, clock),
              SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
    EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 7), 1U);
  }
  const std::optional<DiskLifetimeState> state =
      SlabAllocatorTestPeer::LifetimeState(allocator, Key(1));
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value_or(DiskLifetimeState{}).scoring_reads(), 63);
  EXPECT_EQ(
      SlabAllocatorTestPeer::BucketToken(allocator, Key(1)),
      TokenForEnd(config, End(config, state.value_or(DiskLifetimeState{}))));
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, PhysicalReuseRejectsAnOldResidentRead) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config));
  const DiskLifetimeState old = NewDiskLifetimeState(0);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &old, config, 0, 11));
  EXPECT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  const DiskLifetimeState replacement = NewDiskLifetimeState(30);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &replacement, config, 30, 12));
  const uint64_t token = SlabAllocatorTestPeer::BucketToken(allocator, Key(1));
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  seconds = 31;
  EXPECT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 11,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kResidentChanged);
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(1)), token);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 31), 1U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, WidePagesKeepZeroAndFutureTimesUnambiguous) {
  for (const uint32_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    DiskLifetimeConfig reward_config = Config();
    reward_config.max_reads = max_reads;
    reward_config.read_bonus_sec = 1;
    reward_config.bucket_count = 64;
    std::string reward_error;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(reward_config, &reward_error))
        << reward_error;
    SlabAllocator rewarded(Options(reward_config));
    DiskLifetimeState capped = RecoveredDiskLifetimeState(1);
    capped.hstate = static_cast<uint8_t>(max_reads << 2);
    ASSERT_NO_FATAL_FAILURE(Seed(rewarded, 1, &capped, reward_config));
    const uint64_t reward_end = End(reward_config, capped);
    EXPECT_EQ(reward_end, 10U + max_reads);
    EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(rewarded, Key(1)),
              TokenForEnd(reward_config, reward_end));
    EXPECT_EQ(
        SlabAllocatorTestPeer::ProtectedEstimate(rewarded, 0, reward_end - 1),
        1U);
    EXPECT_EQ(
        SlabAllocatorTestPeer::ProtectedEstimate(
            rewarded, 0,
            TokenForEnd(reward_config, reward_end) * reward_config.bucket_sec),
        0U);
    ExpectValid(rewarded);

    DiskLifetimeConfig config = Config();
    config.base_sec = 10;
    config.read_bonus_sec = 0;
    config.keep_score_sec = 0;
    config.protect_sec = 1;
    config.slowdown = 1;
    config.max_reads = max_reads;
    config.bucket_sec = 1;
    config.bucket_count = 16;
    std::string error;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
    SlabAllocator allocator(Options(config));
    const uint32_t anchor = UINT32_MAX - 2;
    const DiskLifetimeState state = NewDiskLifetimeState(anchor);
    const uint64_t end = End(config, state);
    ASSERT_GT(end, UINT32_MAX);
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &state, config, anchor));
    EXPECT_GT(SlabAllocatorTestPeer::BucketToken(allocator, Key(1)),
              UINT32_MAX);
    EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 1U);
    EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, end - 1),
              1U);
    EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, end), 0U);
    EXPECT_EQ(
        SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, UINT64_MAX), 0U);
    ExpectValid(allocator);
  }
}

TEST(SlabLifetimeBuckets, CountSupportsMoreThan65535ResidentsInOneExtent) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config, 65536));
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  for (uint64_t id = 1; id <= 65536; ++id)
    ASSERT_NO_FATAL_FAILURE(Seed(allocator, id, &state, config));
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 65536U);
  EXPECT_EQ(allocator.Count(), 65536U);
  EXPECT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 65535U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, PersistenceFailureAndDeferredRemoveRetainAccounts) {
  const DiskLifetimeConfig config = Config();
  bool allow = false;
  SlabAllocator::Options options = Options(config);
  options.on_slot_evict = [&allow](const SlabAllocator::SlotRef&) {
    return allow;
  };
  SlabAllocator allocator(options);
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 1, &state, config));
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  std::vector<BlockKey> evicted;
  EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 256, config, clock, &evicted),
            0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 1U);
  ASSERT_TRUE(allocator.Pin(Key(1)));
  EXPECT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kDeferred);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 1U);
  ASSERT_TRUE(allocator.Unpin(Key(1)));
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 0U);
  allow = true;
  ASSERT_NO_FATAL_FAILURE(Seed(allocator, 2, &state, config));
  EXPECT_EQ(SlabAllocatorTestPeer::EvictLifetimeToTarget(
                allocator, 0, 256, config, clock, &evicted),
            1U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 0U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, SuccessfulReturnAndRebindResetEntireExtentRow) {
  const DiskLifetimeConfig config = Config();
  bool allow_bind = false;
  SlabAllocator::Options options = Options(config, 2, 9);
  options.on_extent_bind = [&allow_bind](uint32_t) { return allow_bind; };
  SlabAllocator allocator(options);
  std::vector<SlabAllocator::RestoreEntry> entries;
  for (uint32_t extent = 0; extent < 9; ++extent)
    entries.push_back({Key(extent + 1), 4096, extent, 0, 1});
  ASSERT_TRUE(allocator.RestoreBulk(entries));
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  for (uint32_t extent = 0; extent < 9; ++extent)
    SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(extent + 1),
                                            extent + 1, &state, &config, 0);
  std::vector<BlockKey> evicted;
  EXPECT_FALSE(allocator.StealFrom(0, 1, &evicted));
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 1U);
  EXPECT_EQ(allocator.Remove(Key(1)), SlabAllocator::RemoveResult::kRemoved);
  EXPECT_EQ(allocator.ExtentReturns(), 1U);
  EXPECT_TRUE(SlabAllocatorTestPeer::BucketRowEmpty(allocator, 0));
  allow_bind = true;
  ASSERT_TRUE(allocator.StealFrom(0, 1, &evicted));
  ASSERT_EQ(evicted.size(), 1U);
  EXPECT_EQ(evicted[0], Key(2));
  EXPECT_TRUE(SlabAllocatorTestPeer::BucketRowEmpty(allocator, 1));
  EXPECT_EQ(SlabAllocatorTestPeer::BucketToken(allocator, Key(2)), 0U);
  ExpectValid(allocator);
}

TEST(SlabLifetimeBuckets, ConcurrentOwnersMaintainPhysicalAccounts) {
  const DiskLifetimeConfig config = Config();
  SlabAllocator allocator(Options(config, 8, 2));
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  std::atomic<unsigned> failures{0};
  const auto run = [&](uint64_t owner) {
    for (uint64_t pass = 0; pass < 200; ++pass) {
      const BlockKey key = Key(owner * 1000 + pass + 1);
      SlabAllocator::SlotRef ref;
      std::vector<BlockKey> evicted;
      if (!allocator.Put(key, 1, &ref, &evicted) || !evicted.empty()) {
        ++failures;
        return;
      }
      SlabAllocatorTestPeer::CommitGeneration(
          &allocator, key, owner * 1000 + pass + 1, &state, &config, 0);
      if (SlabAllocatorTestPeer::ApplyLifetimeRead(
              allocator, key, owner * 1000 + pass + 1, config, clock) !=
          SlabAllocatorTestPeer::LifetimeReadResult::kApplied)
        ++failures;
      if (!allocator.Pin(key) ||
          allocator.Remove(key) != SlabAllocator::RemoveResult::kDeferred ||
          !allocator.Unpin(key) ||
          !SlabAllocatorTestPeer::LifetimeBucketsValid(allocator))
        ++failures;
    }
  };
  std::thread first([&] { run(1); });
  std::thread second([&] { run(2); });
  first.join();
  second.join();
  EXPECT_EQ(failures.load(), 0U);
  EXPECT_EQ(allocator.Count(), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 0, 0), 0U);
  EXPECT_EQ(SlabAllocatorTestPeer::ProtectedEstimate(allocator, 1, 0), 0U);
  ExpectValid(allocator);
}

class LifetimeBucketStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_buckets_" +
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
  void Create(bool enabled = true, bool scoring = true, uint32_t extents = 1) {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.extent_bytes = 8 * 4096;
    options.capacity_bytes = options.extent_bytes * extents;
    options.reclaim_interval_ms = 0;
    options.table_sync_ms = 0;
    options.slot_granularity = 4096;
    if (scoring) {
      options.lifetime_config = config_;
      options.lifetime_clock = clock_;
    }
    options.lifetime_class_eviction = enabled;
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  void Put(uint64_t id) {
    ASSERT_EQ(store_->Cache(Key(id), "old", 3), Status::kOk);
  }
  DiskLifetimeConfig config_ = Config();
  std::atomic<uint64_t> seconds_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::filesystem::path dir_;
  std::unique_ptr<DiskSlabStore> store_;
};

TEST_F(LifetimeBucketStoreTest, LegacyAndShadowDoNotAllocateBuckets) {
  for (const bool scoring : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(Create(false, scoring));
    ASSERT_NO_FATAL_FAILURE(Put(1));
    EXPECT_EQ(DiskSlabStoreTestPeer::Pages(*store_), 0U);
    const DiskSlabStore::Stats memory = store_->GetStats();
    EXPECT_EQ(memory.lifetime_bucket_entries, 0U);
    EXPECT_EQ(memory.lifetime_bucket_bytes, 0U);
    EXPECT_EQ(memory.lifetime_bucket_token_bytes, 0U);
    EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 0), 0U);
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
}

TEST_F(LifetimeBucketStoreTest,
       FailedMetadataRemovalKeepsResidentRegistration) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(Put(1));
  EXPECT_EQ(DiskSlabStoreTestPeer::Pages(*store_), config_.bucket_count);
  const DiskSlabStore::Stats memory = store_->GetStats();
  EXPECT_EQ(memory.lifetime_bucket_entries, config_.bucket_count);
  EXPECT_GE(memory.lifetime_bucket_bytes,
            config_.bucket_count * SlabAllocatorTestPeer::BucketPageBytes());
  EXPECT_GE(memory.lifetime_bucket_token_bytes, sizeof(uint64_t));
  EXPECT_GT(memory.lifetime_identity_bytes, 0U);
  EXPECT_GT(memory.lifetime_state_bytes, 0U);
  EXPECT_GT(memory.lifetime_link_bytes, 0U);
  EXPECT_GT(memory.lifetime_queue_bytes, 0U);
  EXPECT_GT(memory.lifetime_version_bytes, 0U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 0), 1U);
  const SlabAllocator::LifetimeBucketObservation before_observation =
      store_->ObserveLifetimeBuckets();
  EXPECT_TRUE(before_observation.available);
  EXPECT_EQ(before_observation.protected_estimate, 1U);
  const uint64_t token = DiskSlabStoreTestPeer::Token(*store_, Key(1));
  DiskSlabStoreTestPeer::FailMetadata(*store_);
  EXPECT_EQ(store_->Remove(Key(1)), Status::kIOError);
  EXPECT_FALSE(store_->Healthy());
  const DiskSlabStore::Stats failed = store_->GetStats();
  EXPECT_EQ(failed.lifetime_bucket_entries, memory.lifetime_bucket_entries);
  EXPECT_EQ(failed.lifetime_bucket_bytes, memory.lifetime_bucket_bytes);
  EXPECT_EQ(failed.lifetime_bucket_token_bytes,
            memory.lifetime_bucket_token_bytes);
  EXPECT_EQ(failed.lifetime_identity_bytes, memory.lifetime_identity_bytes);
  EXPECT_EQ(failed.lifetime_state_bytes, memory.lifetime_state_bytes);
  EXPECT_EQ(failed.lifetime_link_bytes, memory.lifetime_link_bytes);
  EXPECT_EQ(failed.lifetime_queue_bytes, memory.lifetime_queue_bytes);
  EXPECT_EQ(failed.lifetime_version_bytes, memory.lifetime_version_bytes);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Token(*store_, Key(1)), token);
  EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 0), 1U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 3U);
  const SlabAllocator::LifetimeBucketObservation after_observation =
      store_->ObserveLifetimeBuckets();
  EXPECT_TRUE(after_observation.available);
  EXPECT_EQ(after_observation.protected_estimate, 1U);
  EXPECT_EQ(after_observation.summary_bytes, before_observation.summary_bytes);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
}

TEST_F(LifetimeBucketStoreTest,
       DeferredRemoveFailureKeepsPhysicalNotVisibleAccount) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(Put(1));
  ReadLease lease;
  ASSERT_EQ(store_->RangeDirectPrep(Key(1), 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_EQ(store_->TenantUsedBytes(7), 0U);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 0), 1U);
  const DiskSlabStore::Stats hidden = store_->GetStats();
  EXPECT_EQ(hidden.payload_bytes, 0U);
  EXPECT_EQ(hidden.allocated_bytes, 4096U);
  EXPECT_EQ(hidden.allocator_payload_bytes, 3U);
  EXPECT_EQ(hidden.allocator_fragmentation_bytes, 4093U);
  EXPECT_EQ(store_->ObserveLifetimeBuckets().protected_estimate, 1U);
  DiskSlabStoreTestPeer::FailMetadata(*store_);
  lease = ReadLease{};
  EXPECT_FALSE(store_->Healthy());
  const DiskSlabStore::Stats failed = store_->GetStats();
  EXPECT_EQ(failed.payload_bytes, 0U);
  EXPECT_EQ(failed.allocator_payload_bytes, 3U);
  EXPECT_EQ(failed.allocator_fragmentation_bytes, 4093U);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 0U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 0), 1U);
  EXPECT_NE(DiskSlabStoreTestPeer::Token(*store_, Key(1)), 0U);
  const SlabAllocator::LifetimeBucketObservation retained =
      store_->ObserveLifetimeBuckets();
  EXPECT_TRUE(retained.available);
  EXPECT_EQ(retained.protected_estimate, 1U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
}

TEST_F(LifetimeBucketStoreTest, LastPinReleaseClearsHiddenAllocatorPayload) {
  for (const bool lifetime : {false, true}) {
    SCOPED_TRACE(lifetime);
    ASSERT_NO_FATAL_FAILURE(Create(lifetime, lifetime));
    ASSERT_NO_FATAL_FAILURE(Put(1));
    ReadLease first;
    ReadLease last;
    ASSERT_EQ(store_->RangeDirectPrep(Key(1), 0, 0, 4096, &first), Status::kOk);
    ASSERT_EQ(store_->RangeDirectPrep(Key(1), 0, 0, 4096, &last), Status::kOk);
    ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
    first = ReadLease{};
    const DiskSlabStore::Stats hidden = store_->GetStats();
    EXPECT_EQ(hidden.payload_bytes, 0U);
    EXPECT_EQ(hidden.committed_objects, 0U);
    EXPECT_EQ(hidden.allocator_objects, 1U);
    EXPECT_EQ(hidden.allocator_payload_bytes, 3U);
    EXPECT_EQ(hidden.allocator_fragmentation_bytes, 4093U);
    last = ReadLease{};
    const DiskSlabStore::Stats freed = store_->GetStats();
    EXPECT_EQ(freed.payload_bytes, 0U);
    EXPECT_EQ(freed.allocated_bytes, 0U);
    EXPECT_EQ(freed.allocator_payload_bytes, 0U);
    EXPECT_EQ(freed.allocator_fragmentation_bytes, 0U);
    EXPECT_EQ(freed.allocator_objects, 0U);
    EXPECT_EQ(store_->TenantUsedBytes(7), 0U);
    EXPECT_TRUE(store_->Healthy());
    last = ReadLease{};
    EXPECT_EQ(store_->GetStats().allocator_fragmentation_bytes, 0U);
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
}

TEST_F(LifetimeBucketStoreTest,
       AllocatorPayloadSumsClassesAndRebuildsOnRecovery) {
  ASSERT_NO_FATAL_FAILURE(Create(true, true, 2));
  ASSERT_NO_FATAL_FAILURE(Put(1));
  const std::string large(5000, 'l');
  const std::string small(1000, 's');
  ASSERT_EQ(store_->Cache(Key(2), large.data(), large.size()), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(3), small.data(), small.size()), Status::kOk);
  const DiskSlabStore::Stats before = store_->GetStats();
  EXPECT_EQ(before.payload_bytes, 6003U);
  EXPECT_EQ(before.allocated_bytes, 16384U);
  EXPECT_EQ(before.allocator_payload_bytes, 6003U);
  EXPECT_EQ(before.allocator_fragmentation_bytes, 10381U);
  store_.reset();
  ASSERT_NO_FATAL_FAILURE(Create(true, true, 2));
  const DiskSlabStore::Stats recovered = store_->GetStats();
  EXPECT_EQ(recovered.payload_bytes, 6003U);
  EXPECT_EQ(recovered.allocator_payload_bytes, 6003U);
  EXPECT_EQ(recovered.allocator_fragmentation_bytes, 10381U);
  std::string value;
  ASSERT_EQ(store_->Range(Key(2), 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, large);
  ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
  ASSERT_EQ(store_->Remove(Key(2)), Status::kOk);
  const DiskSlabStore::Stats remaining = store_->GetStats();
  EXPECT_EQ(remaining.payload_bytes, 1000U);
  EXPECT_EQ(remaining.allocator_payload_bytes, 1000U);
  EXPECT_EQ(remaining.allocator_fragmentation_bytes, 3096U);
}

TEST_F(LifetimeBucketStoreTest, RecoveryUsesActualNowRatherThanAnchorZero) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(Put(1));
  store_.reset();
  seconds_.store(21);
  ASSERT_NO_FATAL_FAILURE(Create());
  const std::optional<DiskLifetimeState> recovered =
      DiskSlabStoreTestPeer::State(*store_, Key(1));
  ASSERT_TRUE(recovered.has_value());
  EXPECT_EQ(recovered.value_or(DiskLifetimeState{}).anchor, 0U);
  EXPECT_EQ(recovered.value_or(DiskLifetimeState{}).scoring_reads(), 1);
  EXPECT_TRUE(recovered.value_or(DiskLifetimeState{}).history_unknown);
  EXPECT_EQ(DiskSlabStoreTestPeer::Token(*store_, Key(1)), 0U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 21), 0U);
  const SlabAllocator::LifetimeBucketObservation expired_recovery =
      store_->ObserveLifetimeBuckets();
  EXPECT_TRUE(expired_recovery.available);
  EXPECT_EQ(expired_recovery.protected_estimate, 0U);
  std::string value;
  DiskResidentIdentity identity;
  ASSERT_EQ(store_->RangeWithIdentity(Key(1), 0, 0, &value, &identity),
            Status::kOk);
  ASSERT_EQ(value, "old");
  store_->NotifyLifetimeReadSuccess(Key(1), identity, value.size());
  EXPECT_EQ(DiskSlabStoreTestPeer::Estimate(*store_, 21), 1U);
  EXPECT_EQ(store_->ObserveLifetimeBuckets().protected_estimate, 1U);
  EXPECT_NE(DiskSlabStoreTestPeer::Token(*store_, Key(1)), 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(*store_));
}
}  // namespace
}  // namespace dfkv
