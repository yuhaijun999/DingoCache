#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

// Third-party headers.
#include <gtest/gtest.h>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"

namespace dfkv {
namespace {

BlockKey Key(uint64_t id, uint64_t tenant = 7) { return {id, 0, tenant}; }

struct ReadStep {
  BlockKey key;
  uint64_t offset;
  uint64_t length;
  Status status;
  std::string bytes;
};

// Functional trace totals, not hit-rate or business/performance denominators.
struct ReadTotals {
  uint64_t nonempty = 0;
  uint64_t empty = 0;
  uint64_t missing = 0;
  uint64_t invalid = 0;
  uint64_t other = 0;
  uint64_t delivered_bytes = 0;
};

// Zero selects legacy; all other parameters select real lifetime score caps.
class EvictionWorkloadTest : public testing::TestWithParam<uint64_t> {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("dfkv_eviction_workload_" + std::to_string(getpid()) + "_" +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(dir_);
  }
  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(dir_);
  }
  void Create(uint64_t slots) {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.capacity_bytes = slots * 4096;
    options.extent_bytes = options.capacity_bytes;
    options.slot_granularity = 4096;
    options.reclaim_interval_ms = 0;
    options.table_sync_ms = 0;
    options.direct_writes = false;
    if (GetParam() != 0) {
      DiskLifetimeConfig config;
      config.max_reads = GetParam();
      // Cover the entire validated horizon even for M=63; no env overlay is
      // used by direct Store construction. Disable background pressure here.
      config.bucket_sec = 1000000;
      config.bucket_count = 64;
      config.high_pct = 0;
      std::string error;
      ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
      options.lifetime_config = config;
      options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
          DiskLifetimeHorizon(config),
          [this] { return seconds_.load(std::memory_order_relaxed); });
      options.lifetime_class_eviction = true;
    }
    store_ = std::make_unique<DiskSlabStore>(options);
    ASSERT_TRUE(store_->Healthy()) << store_->StartupError();
  }
  ReadTotals Replay(const std::vector<ReadStep>& trace) {
    ReadTotals totals;
    for (size_t index = 0; index < trace.size(); ++index) {
      SCOPED_TRACE(index);
      const ReadStep& step = trace[index];
      std::string bytes;
      DiskResidentIdentity identity;
      const Status status = store_->RangeWithIdentity(
          step.key, step.offset, step.length, &bytes, &identity);
      EXPECT_EQ(status, step.status);
      EXPECT_EQ(bytes, step.bytes);
      if (status == Status::kOk) {
        totals.delivered_bytes += bytes.size();
        if (bytes.empty()) {
          ++totals.empty;
        } else {
          ++totals.nonempty;
          // The logical caller owns success notification, not Range or prep.
          // Use only the provenance returned with these successfully read
          // bytes.
          if (GetParam() != 0) {
            EXPECT_TRUE(identity.valid());
            store_->NotifyLifetimeReadSuccess(step.key, identity, bytes.size());
          }
        }
      } else if (status == Status::kNotFound) {
        ++totals.missing;
      } else if (status == Status::kInvalid) {
        ++totals.invalid;
      } else {
        ++totals.other;
      }
    }
    return totals;
  }
  void ExpectApplied(uint64_t successful_reads) const {
    EXPECT_EQ(store_->GetStats().lifetime_reads.applied,
              GetParam() == 0 ? 0 : successful_reads);
  }
  std::filesystem::path dir_;
  std::atomic<uint64_t> seconds_{0};
  std::unique_ptr<DiskSlabStore> store_;
};

TEST_P(EvictionWorkloadTest, MixedResultsKeepTenantAndGenerationBytesSeparate) {
  Create(4);
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_EQ(store_->Cache(Key(1), "abcd", 4), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(1, 8), "UVWXYZ", 6), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(1), "replacement", 11), Status::kOk);
  EXPECT_EQ(store_->Count(), 2);
  EXPECT_EQ(store_->UsedBytes(), 8192);
  EXPECT_EQ(store_->GetStats().payload_bytes, 10);

  const ReadTotals first = Replay({{Key(1), 1, 2, Status::kOk, "bc"},
                                   {Key(1, 8), 0, 99, Status::kOk, "UVWXYZ"},
                                   {Key(1), 4, 1, Status::kOk, ""},
                                   {Key(99), 0, 1, Status::kNotFound, ""},
                                   {Key(1), 5, 1, Status::kInvalid, ""},
                                   {Key(1, 8), 2, 0, Status::kOk, "WXYZ"}});
  EXPECT_EQ(first.nonempty, 3);
  EXPECT_EQ(first.empty, 1);
  EXPECT_EQ(first.missing, 1);
  EXPECT_EQ(first.invalid, 1);
  EXPECT_EQ(first.other, 0);
  EXPECT_EQ(first.delivered_bytes, 12);
  ExpectApplied(3);

  ASSERT_EQ(store_->Remove(Key(1)), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(1), "newer", 5), Status::kOk);
  const ReadTotals second = Replay({{Key(1), 0, 0, Status::kOk, "newer"},
                                    {Key(1, 8), 0, 0, Status::kOk, "UVWXYZ"}});
  EXPECT_EQ(second.nonempty, 2);
  EXPECT_EQ(second.delivered_bytes, 11);
  EXPECT_EQ(store_->UsedBytes(), 8192);
  EXPECT_EQ(store_->GetStats().payload_bytes, 11);
  EXPECT_EQ(store_->Evictions(), 0);
  ExpectApplied(5);
}

TEST_P(EvictionWorkloadTest, AllPinnedRefusalCanRetryAfterOneLeaseIsReleased) {
  Create(2);
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_EQ(store_->Cache(Key(1), "one", 3), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(2), "two", 3), Status::kOk);
  // Leases are destroyed before the fixture's store even on a fatal assertion.
  std::array<ReadLease, 2> leases;
  ASSERT_EQ(store_->RangeDirectPrep(Key(1), 0, 0, 4096, leases.data()),
            Status::kOk);
  ASSERT_EQ(store_->RangeDirectPrep(Key(2), 0, 0, 4096, &leases[1]),
            Status::kOk);
  EXPECT_GE(leases[0].fd(), 0);
  EXPECT_GE(leases[1].fd(), 0);
  const Status refused = store_->Cache(Key(3), "three", 5);
  // No eligible victim is distinct from exhaustion of the scan/clock budget.
  // Preserve the existing no-candidate status for both policies.
  EXPECT_EQ(refused, Status::kIOError);
  EXPECT_TRUE(store_->Healthy());
  EXPECT_EQ(store_->Count(), 2);
  EXPECT_EQ(store_->Evictions(), 0);
  ExpectApplied(0);

  leases[0] = ReadLease{};
  ASSERT_EQ(store_->Cache(Key(3), "three", 5), Status::kOk);
  const ReadTotals totals = Replay({{Key(1), 0, 0, Status::kNotFound, ""},
                                    {Key(2), 0, 0, Status::kOk, "two"},
                                    {Key(3), 0, 0, Status::kOk, "three"}});
  EXPECT_EQ(totals.nonempty, 2);
  EXPECT_EQ(totals.missing, 1);
  EXPECT_EQ(totals.delivered_bytes, 8);
  EXPECT_EQ(store_->Count(), 2);
  EXPECT_EQ(store_->UsedBytes(), 8192);
  EXPECT_EQ(store_->GetStats().payload_bytes, 8);
  EXPECT_EQ(store_->Evictions(), 1);
  EXPECT_TRUE(store_->Healthy());
  ExpectApplied(2);
}

TEST_P(EvictionWorkloadTest, LogicalHotReadsSurviveTwoColdChurnSteps) {
  Create(3);
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_EQ(store_->Cache(Key(1), "alpha", 5), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(2), "bravo", 5), Status::kOk);
  ASSERT_EQ(store_->Cache(Key(3), "charlie", 7), Status::kOk);
  seconds_.store(200, std::memory_order_relaxed);  // Beyond NEW's P=180.
  const ReadTotals warm = Replay({{Key(1), 0, 0, Status::kOk, "alpha"},
                                  {Key(1), 0, 0, Status::kOk, "alpha"}});
  EXPECT_EQ(warm.nonempty, 2);
  ASSERT_EQ(store_->Cache(Key(4), "delta", 5), Status::kOk);
  const ReadTotals middle = Replay({{Key(1), 0, 0, Status::kOk, "alpha"}});
  EXPECT_EQ(middle.nonempty, 1);
  ASSERT_EQ(store_->Cache(Key(5), "echo", 4), Status::kOk);
  const ReadTotals final = Replay({{Key(1), 0, 0, Status::kOk, "alpha"},
                                   {Key(2), 0, 0, Status::kNotFound, ""},
                                   {Key(3), 0, 0, Status::kNotFound, ""},
                                   {Key(4), 0, 0, Status::kOk, "delta"},
                                   {Key(5), 0, 0, Status::kOk, "echo"}});
  EXPECT_EQ(final.nonempty, 3);
  EXPECT_EQ(final.missing, 2);
  EXPECT_EQ(final.delivered_bytes, 14);
  EXPECT_EQ(store_->Count(), 3);
  EXPECT_EQ(store_->UsedBytes(), 12288);
  EXPECT_EQ(store_->GetStats().payload_bytes, 14);
  EXPECT_EQ(store_->Evictions(), 2);
  ExpectApplied(6);
}

INSTANTIATE_TEST_SUITE_P(LegacyAndLifetime, EvictionWorkloadTest,
                         testing::Values(uint64_t{0}, uint64_t{1}, uint64_t{2},
                                         uint64_t{4}, uint64_t{63}));

}  // namespace
}  // namespace dfkv
