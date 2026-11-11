#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
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
  static bool ObserveSlotEvict(DiskSlabStore& store,
                               std::function<void()> observer) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::ObserveSlotEvict(*store.alloc_,
                                                   std::move(observer));
  }
  static void FailInvalidation(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) close(store.table_fd_);
    store.table_fd_ = -1;
  }
  static void FailPayloadWrite(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    store.payload_write_hook_for_test_ = [] { return false; };
  }
  static bool PreserveWritePin(DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (!store.alloc_->Pin(key)) return false;
    return store.alloc_->Unpin(key);
  }
  static size_t Flights(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.put_flights_.size();
  }
  static long FlightReferences(const DiskSlabStore& store,
                               const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    const std::unordered_map<
        std::string, std::shared_ptr<DiskSlabStore::PutFlight>>::const_iterator
        flight = store.put_flights_.find(key.Filename());
    return flight == store.put_flights_.end() ? 0 : flight->second.use_count();
  }
  static bool IndexesValid(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeIndexesValid(*store.alloc_);
  }
};
namespace {
using Scan = SlabAllocatorTestPeer::LifetimeScan;
using ScanResult = SlabAllocatorTestPeer::LifetimeScanResult;
BlockKey Key(uint64_t id) { return {id, 0, 7}; }

SlabAllocator::Options AllocatorOptions(uint32_t slots) {
  SlabAllocator::Options options;
  options.extent_bytes = static_cast<uint64_t>(slots) * 4096;
  options.num_extents = 1;
  options.track_lifetime_state = true;
  return options;
}

void AddResident(SlabAllocator& allocator, uint64_t id,
                 const DiskLifetimeState* state, bool pin = false) {
  std::vector<BlockKey> evicted;
  SlabAllocator::SlotRef ref;
  ASSERT_TRUE(allocator.Put(Key(id), 1, &ref, &evicted));
  ASSERT_TRUE(evicted.empty());
  SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, state);
  if (pin) {
    ASSERT_TRUE(allocator.Pin(Key(id)));
  }
}

TEST(SlabLifetimeEviction, AgeUsesFixedDisplayBoundaries) {
  struct Case {
    uint32_t seconds;
    size_t bucket;
  };
  const std::array<Case, 14> cases = {{{0, 0},
                                       {1, 1},
                                       {2, 2},
                                       {60, 2},
                                       {61, 3},
                                       {3600, 3},
                                       {3601, 4},
                                       {86400, 4},
                                       {86401, 5},
                                       {604800, 5},
                                       {604801, 6},
                                       {2592000, 6},
                                       {2592001, 7},
                                       {UINT32_MAX, 7}}};
  for (const Case& test : cases) {
    SCOPED_TRACE(test.seconds);
    const DiskLifetimeConfig config;
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&] { return seconds; });
    SlabAllocator allocator(AllocatorOptions(2));
    const DiskLifetimeState state = NewDiskLifetimeState(0);
    ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
    seconds = test.seconds;
    Scan scan(config, clock);
    std::vector<BlockKey> evicted;
    bool denied = false;
    ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                         &evicted, &denied));
    EXPECT_FALSE(denied);
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    for (size_t bucket = 0; bucket < scan.age.buckets.size(); ++bucket) {
      EXPECT_EQ(scan.age.buckets[bucket], bucket == test.bucket ? 1U : 0U);
    }
    EXPECT_EQ(scan.age.total_sec, test.seconds);
    EXPECT_EQ(scan.age.history_unknown, 0U);
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

TEST(SlabLifetimeEviction, AgeUsesFinalPreWipeTime) {
  const DiskLifetimeConfig config;
  bool armed = false;
  unsigned samples = 0;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    const uint64_t sampled = seconds;
    if (armed) {
      ++samples;
      seconds = 20;
    }
    return sampled;
  });
  unsigned writes = 0;
  SlabAllocator::Options options = AllocatorOptions(2);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++writes;
    seconds = 1000;
    return true;
  };
  SlabAllocator allocator(options);
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
  seconds = 19;
  armed = true;
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_FALSE(denied);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_EQ(samples, 2U);  // Selection and the final pre-wipe recheck.
  EXPECT_EQ(writes, 1U);
  EXPECT_EQ(scan.visits, 1U);
  EXPECT_EQ(scan.age.buckets, (std::array<uint64_t, 8>{0, 0, 1}));
  EXPECT_EQ(scan.age.total_sec, 20U);
  EXPECT_EQ(scan.age.history_unknown, 0U);
}

TEST(SlabLifetimeEviction, AgeSaturatesFutureAnchorAtZero) {
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(AllocatorOptions(2));
  // Defensive synthetic state: production commits never lead their clock.
  const DiskLifetimeState state = NewDiskLifetimeState(30);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
  seconds = 20;
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_EQ(scan.age.buckets, (std::array<uint64_t, 8>{1}));
  EXPECT_EQ(scan.age.total_sec, 0U);
  EXPECT_EQ(scan.age.history_unknown, 0U);
}

TEST(SlabLifetimeEviction, AgeExcludesRecoveredHistoryAfterRealRead) {
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&] { return seconds; });
  SlabAllocator allocator(AllocatorOptions(4));
  const DiskLifetimeState state = RecoveredDiskLifetimeState(2);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 2, &state));
  seconds = 10;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(2), 2,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  seconds = 20;
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_EQ(scan.victims, 2U);
  EXPECT_EQ(scan.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(scan.age.total_sec, 0U);
  EXPECT_EQ(scan.age.history_unknown, 2U);
}

TEST(SlabLifetimeEviction, FinalClockFailureDeniesPutBeforeWipeOrFallback) {
  const DiskLifetimeConfig config;
  bool armed = false;
  unsigned samples = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    if (!armed) return uint64_t{0};
    return ++samples == 1 ? uint64_t{19} : uint64_t{UINT32_MAX} + 1;
  });
  unsigned guards = 0;
  unsigned writes = 0;
  SlabAllocator::Options options = AllocatorOptions(2);
  options.can_reclaim = [&] {
    ++guards;
    return true;
  };
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++writes;
    return true;
  };
  SlabAllocator allocator(options);
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 2, &state));
  Scan scan(config, clock);
  // Resume the class stage so pre-class growth/cold-donor probes are skipped.
  scan.result = ScanResult::kRetry;
  armed = true;
  guards = 0;
  std::vector<BlockKey> evicted;
  bool denied = false;
  SlabAllocator::SlotRef ref;
  EXPECT_FALSE(SlabAllocatorTestPeer::PutWithLifetimeScan(
      allocator, Key(3), 1, &ref, &evicted, scan, &denied));
  EXPECT_TRUE(denied);
  EXPECT_EQ(samples, 2U);
  EXPECT_EQ(guards, 2U);  // Class entry and pre-wipe only, no donor fallback.
  EXPECT_EQ(writes, 0U);
  EXPECT_EQ(scan.victims, 0U);
  EXPECT_EQ(scan.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(scan.age.total_sec, 0U);
  EXPECT_EQ(scan.age.history_unknown, 0U);
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_FALSE(allocator.Contains(Key(3)));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeEviction, FinalClockFailureKeepsEarlierSuccessfulAge) {
  const DiskLifetimeConfig config;
  bool armed = false;
  unsigned samples = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [&] {
    if (!armed) return uint64_t{0};
    ++samples;
    if (samples == 1) return uint64_t{19};
    if (samples == 2) return uint64_t{20};
    if (samples == 3) return uint64_t{30};
    return uint64_t{UINT32_MAX} + 1;
  });
  unsigned writes = 0;
  SlabAllocator::Options options = AllocatorOptions(2);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++writes;
    return true;
  };
  SlabAllocator allocator(options);
  const DiskLifetimeState state = NewDiskLifetimeState(0);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 2, &state));
  Scan scan(config, clock);
  armed = true;
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                        &evicted, &denied));
  EXPECT_TRUE(denied);
  EXPECT_EQ(samples, 4U);
  EXPECT_EQ(writes, 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_EQ(scan.victims, 1U);
  EXPECT_EQ(scan.age.buckets, (std::array<uint64_t, 8>{0, 0, 1}));
  EXPECT_EQ(scan.age.total_sec, 20U);
  EXPECT_EQ(scan.age.history_unknown, 0U);
  armed = false;  // Raw-clock rollback must not reopen the hard gate.
  denied = false;
  Scan next_scan(config, clock);
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, next_scan,
                                                        &evicted, &denied));
  EXPECT_EQ(writes, 1U);
  EXPECT_EQ(next_scan.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(next_scan.age.total_sec, 0U);
  EXPECT_EQ(next_scan.age.history_unknown, 0U);
}

TEST(SlabLifetimeEviction, SuccessfulEventCountersSurviveLaterFailure) {
  SlabAllocator::Options options = AllocatorOptions(2);
  unsigned attempts = 0;
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    return ++attempts == 1;
  };
  SlabAllocator allocator(options);
  for (uint64_t id = 1; id <= 2; ++id) {
    SlabAllocator::SlotRef ref;
    std::vector<BlockKey> evicted;
    ASSERT_TRUE(allocator.Put(Key(id), id == 1 ? 17 : 31, &ref, &evicted));
    ASSERT_TRUE(evicted.empty());
    const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
    SlabAllocatorTestPeer::CommitGeneration(&allocator, Key(id), id, &state);
  }
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  ASSERT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                        &evicted, &denied));
  EXPECT_EQ(scan.result, ScanResult::kPersistenceError);
  EXPECT_EQ(scan.victims, 1U);
  EXPECT_EQ(scan.freed_slot_bytes, 4096U);
  EXPECT_EQ(scan.freed_payload_bytes, 17U);
  EXPECT_EQ(scan.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(scan.age.total_sec, 0U);
  EXPECT_EQ(scan.age.history_unknown, 1U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
  EXPECT_TRUE(allocator.Contains(Key(2)));
}

TEST(SlabLifetimeEviction, ComparesDifferentBandsAndIgnoresSoftKeepAsPin) {
  SlabAllocator allocator(AllocatorOptions(4));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  const DiskLifetimeState new_write = NewDiskLifetimeState(0);
  const DiskLifetimeState once = RecoveredDiskLifetimeState(1);
  const DiskLifetimeState twice = RecoveredDiskLifetimeState(2);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &new_write));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 2, &twice));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 3, &once));
  ASSERT_GT(DiskLifetimeScore(config, 0, 1, false, false, 0),
            static_cast<int64_t>(config.keep_score_sec));
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_FALSE(denied);
  EXPECT_EQ(scan.result, ScanResult::kEvicted);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(3)});
  EXPECT_EQ(scan.visits, 3U);
  EXPECT_TRUE(allocator.Contains(Key(1)));
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeEviction, EqualScoresCompareAnchorThenGenerationAcrossBands) {
  // Zero BONUS makes the restored bands equal, but they remain distinct queues.
  DiskLifetimeConfig config;
  config.read_bonus_sec = 0;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  SlabAllocator allocator(AllocatorOptions(2));
  const DiskLifetimeState once = RecoveredDiskLifetimeState(1);
  const DiskLifetimeState twice = RecoveredDiskLifetimeState(2);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 20, &once));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 10, &twice));
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(10)});
}

TEST(SlabLifetimeEviction, EqualScorePrefersOlderAnchorBeforeGeneration) {
  DiskLifetimeConfig config;
  config.read_bonus_sec = 100;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  SlabAllocator allocator(AllocatorOptions(2));
  DiskLifetimeState newer = NewDiskLifetimeState(0);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 200, &newer));
  DiskLifetimeState older = RecoveredDiskLifetimeState(1);
  ASSERT_TRUE(ApplyDiskLifetimeRead(config, 100, &older));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 10, &newer));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 20, &older));
  seconds = 1000;
  ASSERT_EQ(DiskLifetimeScore(config, newer.anchor, newer.scoring_reads(),
                              newer.first_early_read(), false, seconds),
            DiskLifetimeScore(config, older.anchor, older.scoring_reads(),
                              older.first_early_read(), false, seconds));
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(20)});
}

TEST(SlabLifetimeEviction,
     UncommittedUninitializedAndPinnedNeverBecomeVictims) {
  SlabAllocator allocator(AllocatorOptions(4));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  SlabAllocator::SlotRef ref;
  std::vector<BlockKey> evicted;
  ASSERT_TRUE(allocator.Put(Key(1), 1, &ref, &evicted));  // Not committed.
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 2, nullptr));  // Hard-null.
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 3, &state, true));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 4, &state));
  Scan scan(config, clock);
  bool denied = false;
  ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                       &evicted, &denied));
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(4)});
  EXPECT_TRUE(allocator.Contains(Key(1)));
  EXPECT_TRUE(allocator.Contains(Key(2)));
  EXPECT_TRUE(allocator.Contains(Key(3)));
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeEviction,
     FailedInvalidationStopsAfterOneAttemptWithoutFreeing) {
  unsigned attempts = 0;
  SlabAllocator::Options options = AllocatorOptions(2);
  options.on_slot_evict = [&](const SlabAllocator::SlotRef&) {
    ++attempts;
    return false;
  };
  SlabAllocator allocator(options);
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
  ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 2, &state));
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  SlabAllocator::SlotRef ref;
  EXPECT_FALSE(SlabAllocatorTestPeer::PutWithLifetimeScan(
      allocator, Key(3), 1, &ref, &evicted, scan, &denied));
  EXPECT_EQ(scan.result, ScanResult::kPersistenceError);
  EXPECT_EQ(attempts, 1U);
  EXPECT_EQ(scan.age.buckets, (std::array<uint64_t, 8>{}));
  EXPECT_EQ(scan.age.total_sec, 0U);
  EXPECT_EQ(scan.age.history_unknown, 0U);
  EXPECT_EQ(allocator.Count(), 2U);
  EXPECT_TRUE(evicted.empty());
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeEviction,
     LongPinnedPrefixContinuesWithoutResettingOwnerBudget) {
  constexpr uint32_t kResidents = 1100;
  SlabAllocator allocator(AllocatorOptions(kResidents));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  for (uint64_t id = 1; id <= kResidents; ++id)
    ASSERT_NO_FATAL_FAILURE(
        AddResident(allocator, id, &state, id < kResidents));
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  SlabAllocator::SlotRef ref;
  EXPECT_FALSE(SlabAllocatorTestPeer::PutWithLifetimeScan(
      allocator, Key(2000), 1, &ref, &evicted, scan, &denied));
  ASSERT_EQ(scan.result, ScanResult::kRetry);
  EXPECT_EQ(scan.visits, 1024U);
  EXPECT_EQ(scan.slices, 1U);
  EXPECT_EQ(scan.visit_limit, 2 * kResidents + 2);
  EXPECT_EQ(scan.slice_limit, 4U);
  ASSERT_TRUE(SlabAllocatorTestPeer::PutWithLifetimeScan(
      allocator, Key(2000), 1, &ref, &evicted, scan, &denied));
  EXPECT_EQ(scan.visits, kResidents);
  EXPECT_EQ(scan.slices, 2U);
  EXPECT_EQ(evicted, std::vector<BlockKey>{Key(kResidents)});
  EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
}

TEST(SlabLifetimeEviction, IndependentOwnersCannotFakeEachOthersCoverage) {
  constexpr uint32_t kResidents = 1100;
  SlabAllocator allocator(AllocatorOptions(kResidents));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  for (uint64_t id = 1; id <= kResidents; ++id)
    ASSERT_NO_FATAL_FAILURE(AddResident(allocator, id, &state, true));
  Scan first(config, clock);
  Scan second(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, first,
                                                        &evicted, &denied));
  EXPECT_EQ(first.result, ScanResult::kRetry);
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, second,
                                                        &evicted, &denied));
  EXPECT_EQ(second.result, ScanResult::kRetry);
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, first,
                                                        &evicted, &denied));
  EXPECT_EQ(first.result, ScanResult::kNoCandidate);
  EXPECT_EQ(first.visits, kResidents);
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, second,
                                                        &evicted, &denied));
  EXPECT_EQ(second.result, ScanResult::kNoCandidate);
  EXPECT_EQ(second.visits, kResidents);
  EXPECT_TRUE(evicted.empty());
}

TEST(SlabLifetimeEviction,
     HeatChangesDoNotReplenishVisitsOrRetryOpportunities) {
  constexpr uint32_t kResidents = 1100;
  SlabAllocator allocator(AllocatorOptions(kResidents));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  for (uint64_t id = 1; id <= kResidents; ++id)
    ASSERT_NO_FATAL_FAILURE(AddResident(allocator, id, &state, true));
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  unsigned calls = 0;
  do {
    const uint64_t before = scan.visits;
    EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                          &evicted, &denied));
    EXPECT_LE(scan.visits - before, 1024U);
    EXPECT_LE(scan.visits, 2 * kResidents + 2);
    EXPECT_LE(scan.slices, 4U);
    ++calls;
    if (scan.result == ScanResult::kRetry) {
      ++seconds;
      ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                         config, clock),
                SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
    }
  } while (scan.result == ScanResult::kRetry && calls <= 5);
  EXPECT_EQ(scan.result, ScanResult::kBudgetExhausted);
  EXPECT_EQ(scan.visit_limit, 2 * kResidents + 2);
  EXPECT_TRUE(evicted.empty());
  EXPECT_EQ(allocator.Count(), kResidents);
}

TEST(SlabLifetimeEviction, EmptySlicesConsumeTheSameFiniteOpportunityBudget) {
  SlabAllocator allocator(AllocatorOptions(1));
  const DiskLifetimeConfig config;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config), [] { return 0; });
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  for (unsigned attempt = 0; attempt < 2; ++attempt) {
    EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                          &evicted, &denied));
    EXPECT_EQ(scan.result, ScanResult::kNoCandidate);
  }
  EXPECT_EQ(scan.visits, 0U);
  EXPECT_EQ(scan.slices, 2U);
  EXPECT_EQ(scan.visit_limit, 2U);
  EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                        &evicted, &denied));
  EXPECT_EQ(scan.result, ScanResult::kBudgetExhausted);
  EXPECT_EQ(scan.slices, 2U);
}

TEST(SlabLifetimeEviction, SaturatedVersionNeverWrapsOrReplenishesBudget) {
  constexpr uint32_t kResidents = 1100;
  SlabAllocator allocator(AllocatorOptions(kResidents));
  const DiskLifetimeConfig config;
  uint64_t seconds = 0;
  const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                [&seconds] { return seconds; });
  const DiskLifetimeState state = RecoveredDiskLifetimeState(1);
  for (uint64_t id = 1; id <= kResidents; ++id)
    ASSERT_NO_FATAL_FAILURE(AddResident(allocator, id, &state, true));
  SlabAllocatorTestPeer::SetLifetimeVersion(allocator, 0, UINT64_MAX);
  seconds = 1;
  ASSERT_EQ(SlabAllocatorTestPeer::ApplyLifetimeRead(allocator, Key(1), 1,
                                                     config, clock),
            SlabAllocatorTestPeer::LifetimeReadResult::kApplied);
  ASSERT_EQ(SlabAllocatorTestPeer::LifetimeVersion(allocator, 0), UINT64_MAX);
  Scan scan(config, clock);
  std::vector<BlockKey> evicted;
  bool denied = false;
  unsigned attempts = 0;
  do {
    EXPECT_FALSE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                          &evicted, &denied));
    ++attempts;
  } while (scan.result == ScanResult::kRetry && attempts <= 5);
  EXPECT_EQ(scan.result, ScanResult::kBudgetExhausted);
  EXPECT_LE(scan.visits, 2 * kResidents + 2);
  EXPECT_LE(scan.slices, 4U);
  EXPECT_EQ(SlabAllocatorTestPeer::LifetimeVersion(allocator, 0), UINT64_MAX);
}

TEST(SlabLifetimeEviction, ConfiguredCapsSelectTheHighestBandThrough63) {
  for (const uint32_t max_reads : {1U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    SlabAllocator::Options options = AllocatorOptions(1);
    options.lifetime_max_reads = max_reads;
    SlabAllocator allocator(options);
    DiskLifetimeConfig config;
    config.max_reads = max_reads;
    config.bucket_sec = 864000;
    std::string error;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
    uint64_t seconds = 0;
    const DiskLifetimeClock clock(DiskLifetimeHorizon(config),
                                  [&seconds] { return seconds; });
    seconds = 200;
    DiskLifetimeState state = RecoveredDiskLifetimeState(1);
    for (unsigned read = 0; read < 62; ++read)
      ASSERT_TRUE(ApplyDiskLifetimeRead(config, 200, &state));
    ASSERT_EQ(state.scoring_reads(), 63U);
    ASSERT_NO_FATAL_FAILURE(AddResident(allocator, 1, &state));
    EXPECT_EQ(SlabAllocatorTestPeer::LifetimeQueueKeys(allocator, 0, max_reads),
              std::vector<BlockKey>{Key(1)});
    Scan scan(config, clock);
    std::vector<BlockKey> evicted;
    bool denied = false;
    ASSERT_TRUE(SlabAllocatorTestPeer::ScanLifetimeClass(allocator, 0, scan,
                                                         &evicted, &denied));
    EXPECT_EQ(evicted, std::vector<BlockKey>{Key(1)});
    EXPECT_EQ(scan.visits, 1U);
    EXPECT_TRUE(SlabAllocatorTestPeer::LifetimeIndexesValid(allocator));
  }
}

class LifetimeClassStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_class_" +
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
  DiskSlabStore::Options Options(uint32_t slots) const {
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
    store_ = std::make_unique<DiskSlabStore>(Options(slots));
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  void Seed(uint32_t count, bool pin) {
    for (uint64_t id = 1; id <= count; ++id) {
      const DiskWriteResult result = store_->CacheWithResult(Key(id), "old", 3);
      ASSERT_EQ(result.status, Status::kOk);
      ASSERT_TRUE(result.identity.valid());
      if (pin) {
        ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(id)));
      }
    }
  }
  DiskWriteResult Write(const char* mode, const BlockKey& key) {
    if (std::string(mode) == "batch") {
      char bytes[] = "new";
      const std::vector<StoreEngine::CacheBatchItem> items{
          {key, bytes, 3, sizeof(bytes)}};
      return store_->CacheDirectBatchWithResults(items)[0];
    }
    if (std::string(mode) == "direct") {
      char bytes[] = "new";
      return store_->CacheDirectWithResult(key, bytes, 3, sizeof(bytes));
    }
    return store_->CacheWithResult(key, "new", 3);
  }
  void HeatPinnedFirst() {
    std::string bytes;
    DiskResidentIdentity source;
    EXPECT_EQ(store_->RangeWithIdentity(Key(1), 0, 0, &bytes, &source),
              Status::kOk);
    EXPECT_EQ(bytes, "old");
    seconds_.fetch_add(1);
    store_->NotifyLifetimeReadSuccess(Key(1), source, bytes.size());
  }
  DiskLifetimeConfig config_;
  std::atomic<uint64_t> seconds_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::filesystem::path dir_;
  std::unique_ptr<DiskSlabStore> store_;
};

TEST_F(LifetimeClassStoreTest, GateRejectsMissingScoringBeforeOpen) {
  DiskSlabStore::Options options = Options(2);
  options.lifetime_config.reset();
  const DiskSlabStore rejected(options);
  EXPECT_FALSE(rejected.Healthy());
  EXPECT_FALSE(rejected.StartupError().empty());
  EXPECT_FALSE(std::filesystem::exists(dir_));
}

TEST_F(LifetimeClassStoreTest, ScalarDirectAndBatchReclaimTheCheckedResident) {
  for (const char* mode : {"scalar", "direct", "batch"}) {
    SCOPED_TRACE(mode);
    store_.reset();
    std::filesystem::remove_all(dir_);
    ASSERT_NO_FATAL_FAILURE(Create(2));
    ASSERT_NO_FATAL_FAILURE(Seed(2, false));
    ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(*store_, Key(1)));
    const DiskWriteResult written = Write(mode, Key(3));
    ASSERT_EQ(written.status, Status::kOk);
    EXPECT_EQ(written.outcome, DiskWriteOutcome::kNew);
    EXPECT_TRUE(written.identity.valid());
    EXPECT_TRUE(store_->IsCached(Key(1)));
    EXPECT_FALSE(store_->IsCached(Key(2)));
    std::string bytes;
    ASSERT_EQ(store_->Range(Key(3), 0, 0, &bytes), Status::kOk);
    EXPECT_EQ(bytes, "new");
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_EQ(stats.lifetime_class_victims, 1U);
    EXPECT_EQ(stats.lifetime_class_age.buckets, (std::array<uint64_t, 8>{1}));
    EXPECT_EQ(stats.lifetime_class_age.total_sec, 0U);
    EXPECT_EQ(stats.lifetime_class_age.history_unknown, 0U);
    EXPECT_EQ(stats.lifetime_class_freed_slot_bytes, 4096U);
    EXPECT_EQ(stats.lifetime_class_freed_payload_bytes, 3U);
    EXPECT_EQ(stats.lifetime_class_scan_visits, 2U);
    EXPECT_EQ(stats.lifetime_scan_budget_exhausted, 0U);
    EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
    EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
  }
}

TEST_F(LifetimeClassStoreTest, FinalClockFailureReturnsTypedBackpressure) {
  constexpr uint32_t kResidents = 1100;
  for (const char* mode : {"scalar", "direct", "batch"}) {
    SCOPED_TRACE(mode);
    store_.reset();
    std::filesystem::remove_all(dir_);
    bool armed = false;
    unsigned samples = 0;
    unsigned writes = 0;
    const std::shared_ptr<DiskLifetimeClock> clock =
        std::make_shared<DiskLifetimeClock>(DiskLifetimeHorizon(config_), [&] {
          if (!armed) return uint64_t{0};
          // After the first pinned slice, the resumed class path reads entry
          // guard, selection, final guard, then the final pre-wipe timestamp.
          return ++samples == 4 ? uint64_t{UINT32_MAX} + 1 : uint64_t{19};
        });
    DiskSlabStore::Options options = Options(kResidents);
    options.lifetime_clock = clock;
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
    ASSERT_NO_FATAL_FAILURE(Seed(kResidents, true));
    ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(kResidents)));
    ASSERT_TRUE(
        DiskSlabStoreTestPeer::ObserveSlotEvict(*store_, [&] { ++writes; }));
    unsigned yields = 0;
    DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
      ++yields;
      armed = true;
    });
    const DiskSlabStore::Stats before = store_->GetStats();
    const DiskWriteResult result = Write(mode, Key(2000));
    armed = false;
    EXPECT_EQ(result.status, Status::kCacheFull);
    EXPECT_EQ(result.reason, DiskWriteReason::kLifetimeClockExhausted);
    EXPECT_FALSE(result.identity.valid());
    EXPECT_EQ(yields, 1U);
    EXPECT_EQ(samples, 4U);
    EXPECT_EQ(writes, 0U);
    EXPECT_TRUE(store_->Healthy());
    EXPECT_EQ(store_->Count(), kResidents);
    EXPECT_FALSE(store_->IsCached(Key(2000)));
    EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
    const DiskSlabStore::Stats after = store_->GetStats();
    EXPECT_EQ(after.lifetime_class_scan_visits, kResidents);
    EXPECT_EQ(after.lifetime_class_scan_slices, 2U);
    EXPECT_EQ(after.lifetime_class_victims, 0U);
    EXPECT_EQ(after.lifetime_class_age.buckets, (std::array<uint64_t, 8>{}));
    EXPECT_EQ(after.lifetime_class_age.total_sec, 0U);
    EXPECT_EQ(after.lifetime_class_age.history_unknown, 0U);
    EXPECT_EQ(after.lifetime_donor_released_residents, 0U);
    EXPECT_EQ(after.record_writes, before.record_writes);
    EXPECT_EQ(after.metadata_io_errors, 0U);
    EXPECT_EQ(after.committed_objects, kResidents);
    for (const uint64_t id : {uint64_t{1}, uint64_t{kResidents}}) {
      std::string bytes;
      EXPECT_EQ(store_->Range(Key(id), 0, 0, &bytes), Status::kOk);
      EXPECT_EQ(bytes, "old");
    }
    EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
    store_.reset();  // Destroy borrowed callbacks before their captured locals.
  }
}

TEST_F(LifetimeClassStoreTest, DonorRebindIsNotForegroundClassEviction) {
  ASSERT_NO_FATAL_FAILURE(Create(2));
  const std::string first(17, 'a');
  const std::string second(31, 'b');
  ASSERT_EQ(store_->Cache(Key(1), first.data(), first.size()), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(2), second.data(), second.size()), Status::kOk);
  const std::string large(5000, 'x');
  ASSERT_EQ(store_->Cache(Key(3), large.data(), large.size()), Status::kOk);
  const DiskSlabStore::Stats donor = store_->GetStats();
  EXPECT_EQ(donor.lifetime_donor_released_residents, 2U);
  EXPECT_EQ(donor.lifetime_donor_freed_slot_bytes, 8192U);
  EXPECT_EQ(donor.lifetime_donor_freed_payload_bytes, 48U);
  EXPECT_EQ(store_->EvictedBytes(), 48U);
  EXPECT_EQ(donor.lifetime_class_victims, 0U);
  EXPECT_EQ(donor.lifetime_class_freed_slot_bytes, 0U);
  EXPECT_EQ(donor.lifetime_class_freed_payload_bytes, 0U);
  EXPECT_EQ(store_->Count(), 1U);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_FALSE(store_->IsCached(Key(2)));
  const std::string next(5001, 'y');
  ASSERT_EQ(store_->Cache(Key(4), next.data(), next.size()), Status::kOk);
  const DiskSlabStore::Stats class_reclaim = store_->GetStats();
  EXPECT_EQ(class_reclaim.lifetime_donor_released_residents, 2U);
  EXPECT_EQ(class_reclaim.lifetime_donor_freed_slot_bytes, 8192U);
  EXPECT_EQ(class_reclaim.lifetime_donor_freed_payload_bytes, 48U);
  EXPECT_EQ(class_reclaim.lifetime_class_victims, 1U);
  EXPECT_EQ(class_reclaim.lifetime_class_freed_slot_bytes, 8192U);
  EXPECT_EQ(class_reclaim.lifetime_class_freed_payload_bytes, 5000U);
  EXPECT_EQ(store_->EvictedBytes(), 5048U);
  store_.reset();
  ASSERT_NO_FATAL_FAILURE(Create(2));
  EXPECT_EQ(store_->GetStats().lifetime_donor_released_residents, 0U);
  EXPECT_EQ(store_->GetStats().lifetime_donor_freed_slot_bytes, 0U);
  EXPECT_EQ(store_->GetStats().lifetime_donor_freed_payload_bytes, 0U);
  ASSERT_EQ(store_->Cache(Key(5), large.data(), large.size()), Status::kOk);
  EXPECT_EQ(store_->GetStats().lifetime_class_freed_slot_bytes, 8192U);
  EXPECT_EQ(store_->GetStats().lifetime_class_freed_payload_bytes, 5001U);
}

TEST_F(LifetimeClassStoreTest, FailedNewWriteRetainsSuccessfulDonorBytes) {
  for (const char* mode : {"scalar", "direct", "batch"}) {
    SCOPED_TRACE(mode);
    store_.reset();
    std::filesystem::remove_all(dir_);
    ASSERT_NO_FATAL_FAILURE(Create(2));
    ASSERT_EQ(store_->Cache(Key(1), "old", 3), Status::kOk);
    ASSERT_EQ(store_->Cache(Key(2), "prior", 5), Status::kOk);
    DiskSlabStoreTestPeer::FailPayloadWrite(*store_);
    std::string payload(5000, 'x');
    DiskWriteResult result;
    if (std::string(mode) == "batch") {
      const std::vector<StoreEngine::CacheBatchItem> items{
          {Key(3), payload.data(), 5000, 5000}};
      result = store_->CacheDirectBatchWithResults(items)[0];
    } else if (std::string(mode) == "direct") {
      result =
          store_->CacheDirectWithResult(Key(3), payload.data(), 5000, 5000);
    } else {
      result = store_->CacheWithResult(Key(3), payload.data(), 5000);
    }
    EXPECT_EQ(result.status, Status::kIOError);
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_EQ(stats.lifetime_donor_released_residents, 2U);
    EXPECT_EQ(stats.lifetime_donor_freed_slot_bytes, 8192U);
    EXPECT_EQ(stats.lifetime_donor_freed_payload_bytes, 8U);
    EXPECT_EQ(stats.lifetime_donor_protected_victims, 2U);
    EXPECT_EQ(stats.lifetime_donor_protected_payload_bytes, 8U);
    EXPECT_EQ(stats.lifetime_donor_unscored_victims, 0U);
    EXPECT_EQ(stats.lifetime_class_victims, 0U);
    EXPECT_EQ(stats.lifetime_background_victims, 0U);
    EXPECT_EQ(stats.watermark_slot_removals, 0U);
    EXPECT_EQ(store_->EvictedBytes(), 8U);
    EXPECT_EQ(store_->Count(), 0U);
    EXPECT_FALSE(store_->IsCached(Key(3)));
  }
}

TEST_F(LifetimeClassStoreTest, FailedNewWriteRetainsSuccessfulEvictionBytes) {
  for (const char* mode : {"scalar", "direct", "batch"}) {
    SCOPED_TRACE(mode);
    store_.reset();
    std::filesystem::remove_all(dir_);
    ASSERT_NO_FATAL_FAILURE(Create(2));
    ASSERT_NO_FATAL_FAILURE(Seed(2, false));
    seconds_.fetch_add(61);
    DiskSlabStoreTestPeer::FailPayloadWrite(*store_);
    EXPECT_EQ(Write(mode, Key(3)).status, Status::kIOError);
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_EQ(stats.lifetime_class_victims, 1U);
    EXPECT_EQ(stats.lifetime_class_age.buckets,
              (std::array<uint64_t, 8>{0, 0, 0, 1}));
    EXPECT_EQ(stats.lifetime_class_age.total_sec, 61U);
    EXPECT_EQ(stats.lifetime_class_age.history_unknown, 0U);
    EXPECT_EQ(stats.lifetime_class_freed_slot_bytes, 4096U);
    EXPECT_EQ(stats.lifetime_class_freed_payload_bytes, 3U);
    EXPECT_EQ(store_->EvictedBytes(), 3U);
    EXPECT_EQ(stats.payload_bytes, 3U);
    EXPECT_EQ(store_->Count(), 1U);
    EXPECT_FALSE(store_->IsCached(Key(3)));
  }
}

TEST_F(LifetimeClassStoreTest,
       LongPinnedPrefixSucceedsAfterUnlockingBothLocks) {
  constexpr uint32_t kResidents = 1100;
  ASSERT_NO_FATAL_FAILURE(Create(kResidents));
  ASSERT_NO_FATAL_FAILURE(Seed(kResidents, true));
  ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(kResidents)));
  unsigned yields = 0;
  DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
    ++yields;
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_EQ(stats.lifetime_class_scan_visits, 1024U);
    std::string bytes;
    EXPECT_EQ(store_->Range(Key(1), 0, 0, &bytes), Status::kOk);
    EXPECT_EQ(bytes, "old");
  });
  const DiskWriteResult written = Write("direct", Key(2000));
  ASSERT_EQ(written.status, Status::kOk);
  EXPECT_EQ(written.outcome, DiskWriteOutcome::kNew);
  EXPECT_EQ(yields, 1U);
  EXPECT_FALSE(store_->IsCached(Key(kResidents)));
  EXPECT_TRUE(store_->IsCached(Key(1)));
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(stats.lifetime_class_scan_visits, kResidents);
  EXPECT_EQ(stats.lifetime_class_scan_slices, 2U);
  EXPECT_EQ(stats.lifetime_class_victims, 1U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeClassStoreTest,
       PinChangeBetweenSlicesRechecksAnEarlierResident) {
  constexpr uint32_t kResidents = 1100;
  ASSERT_NO_FATAL_FAILURE(Create(kResidents));
  ASSERT_NO_FATAL_FAILURE(Seed(kResidents, true));
  unsigned yields = 0;
  DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
    ++yields;
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_EQ(stats.lifetime_class_scan_visits, 1024U);
    // The first slice already skipped key1. An actual pin-eligibility change
    // must invalidate its "fully checked" progress before choosing a victim.
    EXPECT_TRUE(DiskSlabStoreTestPeer::Unpin(*store_, Key(1)));
  });
  const DiskWriteResult written = Write("scalar", Key(2000));
  ASSERT_EQ(written.status, Status::kOk);
  EXPECT_EQ(written.outcome, DiskWriteOutcome::kNew);
  EXPECT_EQ(yields, 1U);
  EXPECT_FALSE(store_->IsCached(Key(1)));
  EXPECT_TRUE(store_->IsCached(Key(2)));
  const DiskSlabStore::Stats stats = store_->GetStats();
  EXPECT_EQ(stats.lifetime_class_scan_slices, 2U);
  EXPECT_LE(stats.lifetime_class_scan_visits, 2 * kResidents + 2);
  EXPECT_EQ(stats.lifetime_class_victims, 1U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeClassStoreTest, FailureDoesNotFreeOrReclassifyVictimAsBudget) {
  ASSERT_NO_FATAL_FAILURE(Create(2));
  ASSERT_NO_FATAL_FAILURE(Seed(2, false));
  DiskSlabStoreTestPeer::FailInvalidation(*store_);
  const DiskWriteResult written = Write("scalar", Key(3));
  EXPECT_EQ(written.status, Status::kIOError);
  EXPECT_EQ(written.reason, DiskWriteReason::kNone);
  EXPECT_FALSE(store_->Healthy());
  EXPECT_GT(store_->GetStats().metadata_io_errors, 0U);
  EXPECT_EQ(store_->Count(), 2U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
  EXPECT_EQ(store_->GetStats().lifetime_class_victims, 0U);
  EXPECT_EQ(store_->GetStats().lifetime_class_freed_slot_bytes, 0U);
  EXPECT_EQ(store_->GetStats().lifetime_class_freed_payload_bytes, 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeClassStoreTest, BudgetExhaustionPropagatesOnEveryWritePath) {
  constexpr uint32_t kResidents = 1100;
  for (const char* mode : {"scalar", "direct", "batch"}) {
    SCOPED_TRACE(mode);
    store_.reset();
    std::filesystem::remove_all(dir_);
    ASSERT_NO_FATAL_FAILURE(Create(kResidents));
    ASSERT_NO_FATAL_FAILURE(Seed(kResidents, true));
    uint64_t previous_visits = 0;
    unsigned yields = 0;
    DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
      // Both D and A must be released: these production calls acquire them.
      const DiskSlabStore::Stats stats = store_->GetStats();
      EXPECT_LE(stats.lifetime_class_scan_visits - previous_visits, 1024U);
      previous_visits = stats.lifetime_class_scan_visits;
      HeatPinnedFirst();
      ++yields;
    });
    const DiskWriteResult written = Write(mode, Key(2000));
    EXPECT_EQ(written.status, Status::kCacheFull);
    EXPECT_EQ(written.reason, DiskWriteReason::kScanBudgetExhausted);
    EXPECT_FALSE(written.identity.valid());
    EXPECT_TRUE(store_->Healthy());
    EXPECT_EQ(store_->Count(), kResidents);
    EXPECT_EQ(store_->TenantUsedBytes(7), 3 * kResidents);
    const DiskSlabStore::Stats stats = store_->GetStats();
    EXPECT_LE(stats.lifetime_class_scan_visits, 2 * kResidents + 2);
    EXPECT_LE(stats.lifetime_class_scan_slices, 4U);
    EXPECT_EQ(stats.lifetime_scan_budget_exhausted, 1U);
    EXPECT_EQ(stats.lifetime_class_victims, 0U);
    EXPECT_EQ(stats.lifetime_class_freed_slot_bytes, 0U);
    EXPECT_EQ(stats.lifetime_class_freed_payload_bytes, 0U);
    EXPECT_GE(yields, 1U);
    EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
    EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
  }
}

TEST_F(LifetimeClassStoreTest, BatchPreservesEarlierWritePinAcrossLaterBudget) {
  constexpr uint32_t kResidents = 1100;
  const BlockKey accepted = Key(2000);
  const BlockKey rejected = Key(2001);
  ASSERT_NO_FATAL_FAILURE(Create(kResidents + 1));
  ASSERT_NO_FATAL_FAILURE(Seed(kResidents, true));
  unsigned yields = 0;
  DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
    ++yields;
    EXPECT_TRUE(DiskSlabStoreTestPeer::PreserveWritePin(*store_, accepted));
    EXPECT_EQ(store_->GetStats().inflight, 1U);
    std::string bytes;
    EXPECT_EQ(store_->Range(accepted, 0, 0, &bytes), Status::kNotFound);
    HeatPinnedFirst();
  });
  char first[] = "one";
  char second[] = "two";
  char duplicate[] = "other";
  const std::vector<StoreEngine::CacheBatchItem> items{
      {accepted, first, 3, sizeof(first)},
      {rejected, second, 3, sizeof(second)},
      {accepted, duplicate, 5, sizeof(duplicate)}};
  const std::vector<DiskWriteResult> results =
      store_->CacheDirectBatchWithResults(items);
  ASSERT_EQ(results.size(), 3U);
  EXPECT_EQ(results[0].status, Status::kOk);
  EXPECT_EQ(results[0].outcome, DiskWriteOutcome::kNew);
  EXPECT_TRUE(results[0].identity.valid());
  EXPECT_EQ(results[1].status, Status::kCacheFull);
  EXPECT_EQ(results[1].reason, DiskWriteReason::kScanBudgetExhausted);
  EXPECT_FALSE(results[1].identity.valid());
  EXPECT_EQ(results[2].status, Status::kOk);
  EXPECT_EQ(results[2].outcome, DiskWriteOutcome::kKeptExisting);
  EXPECT_FALSE(results[2].identity.valid());
  std::string bytes;
  ASSERT_EQ(store_->Range(accepted, 0, 0, &bytes), Status::kOk);
  EXPECT_EQ(bytes, "one");
  EXPECT_FALSE(store_->IsCached(rejected));
  EXPECT_GT(yields, 0U);
  EXPECT_TRUE(store_->Healthy());
  EXPECT_EQ(store_->GetStats().inflight, 0U);
  EXPECT_EQ(store_->TenantUsedBytes(7), 3 * (kResidents + 1));
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
  EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(*store_));
}

TEST_F(LifetimeClassStoreTest, PausedOwnerKeepsOneFlightAndCompletesFollower) {
  constexpr uint32_t kResidents = 1100;
  const BlockKey incoming = Key(2000);
  ASSERT_NO_FATAL_FAILURE(Create(kResidents));
  ASSERT_NO_FATAL_FAILURE(Seed(kResidents, true));
  std::promise<void> paused;
  const std::future<void> pause_ready = paused.get_future();
  std::promise<void> resume;
  const std::shared_future<void> resume_ready = resume.get_future().share();
  std::atomic<unsigned> yields{0};
  DiskSlabStoreTestPeer::YieldHook(*store_, [&] {
    if (yields.fetch_add(1) == 0) {
      paused.set_value();
      resume_ready.wait();
    }
    HeatPinnedFirst();
  });
  std::promise<DiskWriteResult> owner_done;
  std::future<DiskWriteResult> owner_result = owner_done.get_future();
  std::thread owner([&] { owner_done.set_value(Write("scalar", incoming)); });
  const bool owner_paused = pause_ready.wait_for(std::chrono::seconds(3)) ==
                            std::future_status::ready;
  std::promise<DiskWriteResult> follower_done;
  std::future<DiskWriteResult> follower_result = follower_done.get_future();
  std::thread follower([&] {
    follower_done.set_value(store_->CacheWithResult(incoming, "different", 9));
  });
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (DiskSlabStoreTestPeer::FlightReferences(*store_, incoming) < 3 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool follower_attached =
      DiskSlabStoreTestPeer::FlightReferences(*store_, incoming) >= 3;
  resume.set_value();  // Always open the barrier before any fatal assertion.
  const bool owner_finished = owner_result.wait_for(std::chrono::seconds(3)) ==
                              std::future_status::ready;
  const bool follower_finished =
      follower_result.wait_for(std::chrono::seconds(3)) ==
      std::future_status::ready;
  owner.join();
  follower.join();
  ASSERT_TRUE(owner_paused);
  ASSERT_TRUE(follower_attached);
  ASSERT_TRUE(owner_finished);
  ASSERT_TRUE(follower_finished);
  const DiskWriteResult owner_write = owner_result.get();
  const DiskWriteResult follower_write = follower_result.get();
  EXPECT_EQ(owner_write.status, Status::kCacheFull);
  EXPECT_EQ(owner_write.reason, DiskWriteReason::kScanBudgetExhausted);
  EXPECT_EQ(follower_write.status, owner_write.status);
  EXPECT_EQ(follower_write.reason, owner_write.reason);
  EXPECT_FALSE(follower_write.identity.valid());
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(*store_), 0U);
  EXPECT_EQ(store_->GetStats().lifetime_scan_budget_exhausted, 1U);
  EXPECT_TRUE(store_->Healthy());
}

}  // namespace
}  // namespace dfkv
