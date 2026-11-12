// POSIX environment mutation requires its C declaration, not only <cstdlib>.
#include <stdlib.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

// Third-party headers.
#include <gtest/gtest.h>

#include "cache/disk_cache_group.h"
#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_slab_store.h"
#include "cache/kv_node_server.h"
#include "cache/ram_tier.h"
#include "cache/read_coalescer.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "read_coalescer_test_peer.h"
#include "transport/wire.h"

namespace dfkv {
class KvNodeServerTestPeer {
 public:
  static DiskCacheGroup& Group(KvNodeServer& server) { return server.group_; }
  static RamTier& Ram(KvNodeServer& server) { return *server.ram_; }
  static uint64_t RegisterRead(KvNodeServer& server, const BlockKey& key) {
    return server.read_coalescer_.TryRegisterAsync(
        key, 0, 3, false, 0, false, server.CaptureReadEpoch(key));
  }
  static bool WaitForRead(KvNodeServer& server) {
    return ReadCoalescerTestPeer::WaitForCopyWaiter(server.read_coalescer_);
  }
  static bool FailRead(KvNodeServer& server, uint64_t token) {
    return server.read_coalescer_.CompleteAsync(token, Status::kIOError,
                                                nullptr, 0);
  }
  static size_t ReadTimeouts(const KvNodeServer& server) {
    return server.read_coalescer_.timeouts();
  }
};

class DiskCacheGroupTestPeer {
 public:
  static size_t Route(const DiskCacheGroup& group, const BlockKey& key) {
    return group.RouteIndex(key);
  }
  static DiskSlabStore& Slab(DiskCacheGroup& group, size_t disk) {
    return *group.slabs_.at(disk);
  }
};

class DiskSlabStoreTestPeer {
 public:
  static void FailMetadata(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    EXPECT_EQ(::close(store.table_fd_), 0);
    store.table_fd_ = -1;
  }
};

namespace {
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    const char* old = std::getenv(name);
    if (old != nullptr) {
      had_old_ = true;
      old_ = old;
    }
    EXPECT_EQ(::setenv(name, value, 1), 0);
  }
  ~ScopedEnv() {
    if (had_old_) {
      EXPECT_EQ(::setenv(name_.c_str(), old_.c_str(), 1), 0);
    } else {
      EXPECT_EQ(::unsetenv(name_.c_str()), 0);
    }
  }

 private:
  const std::string name_;
  std::string old_;
  bool had_old_ = false;
};

class DiskEvictionGhostReadTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("dfkv_ghost_read_" + std::to_string(::getpid()) + "_" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    config_.base_sec = 100;
    config_.read_bonus_sec = 10;
    config_.protect_sec = 8;
    config_.slowdown = 4;
    config_.keep_score_sec = 90;
    config_.bucket_sec = 16;
    config_.bucket_count = 8;
    config_.high_pct = 0;
  }
  void TearDown() override {
    server_.reset();
    std::filesystem::remove_all(dir_);
  }
  void Create(bool coalescing = false, size_t disks = 1, size_t capacity = 2,
              const char* policy = "lifetime", const char* engine = "slab") {
    server_.reset();
    seconds_ = 0;
    fail_clock_after_ = -1;
    clock_calls_ = 0;
    clock_ = std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(config_), [this] {
          ++clock_calls_;
          if (fail_clock_after_.load() >= 0 &&
              fail_clock_after_.fetch_sub(1) == 0) {
            return std::numeric_limits<uint64_t>::max();
          }
          return seconds_.load();
        });
    DiskCacheGroup::Options options;
    const std::filesystem::path instance = dir_ / std::to_string(instance_++);
    for (size_t disk = 0; disk < disks; ++disk)
      options.cache_dirs.push_back((instance / std::to_string(disk)).string());
    // Sparse 1-GiB geometry gives one slot per disk; payloads remain 3 bytes.
    // This is functional eviction control, not a memory/performance test.
    options.capacity_bytes = disks * (1ULL << 30);
    options.engine = engine;
    options.eviction_policy = policy;
    options.eviction_ghost_capacity_per_disk = capacity;
    if (std::string(policy) == "lifetime") {
      options.lifetime_config = config_;
      options.lifetime_clock = clock_;
    }
    const ScopedEnv coalesce("DFKV_READ_COALESCE", coalescing ? "1" : "0");
    server_ = std::make_unique<KvNodeServer>(options);
    ASSERT_TRUE(server_->Healthy()) << Group().StartupError();
    server_->set_identity("ghost\"node", "ghost\\group\n");
  }
  DiskCacheGroup& Group() { return KvNodeServerTestPeer::Group(*server_); }
  BlockKey Key(uint64_t first = 1, size_t disk = 0, uint64_t tenant = 7) {
    for (uint64_t id = first; id < first + 10000; ++id) {
      const BlockKey key{id, 0, tenant};
      if (DiskCacheGroupTestPeer::Route(Group(), key) == disk) return key;
    }
    ADD_FAILURE() << "no key routed to disk " << disk;
    return {};
  }
  Status Put(const BlockKey& key, const char* value = "old") {
    std::string ignored;
    return server_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                         key, 0, 0, value, 3, &ignored);
  }
  Status Operation(WireOp op, const BlockKey& key) {
    std::string ignored;
    return server_->ProcessRequestForKey(static_cast<uint8_t>(op), key, 0, 0,
                                         nullptr, 0, &ignored);
  }
  Status Read(bool into, const BlockKey& key, std::string* output = nullptr,
              uint64_t offset = 0) {
    std::string bytes;
    Status status;
    if (into) {
      std::array<char, 16> buffer{};
      size_t length = 0;
      status = server_->RangeIntoForKey(key, offset, 3, buffer.data(),
                                        buffer.size(), &length);
      if (status == Status::kOk) bytes.assign(buffer.data(), length);
    } else {
      status =
          server_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                        key, offset, 3, nullptr, 0, &bytes);
    }
    if (output != nullptr) *output = bytes;
    return status;
  }
  void Check(size_t disk, uint64_t misses, uint64_t matches, uint64_t age_sec,
             uint64_t unavailable = 0) {
    const DiskSlabStore::EvictionGhostObservation observation =
        Group().DiskEvictionGhostObservation(disk);
    EXPECT_EQ(observation.sync_read_misses, misses);
    EXPECT_EQ(observation.sync_read_history_matches, matches);
    EXPECT_EQ(observation.sync_read_lookup_unavailable, unavailable);
    EXPECT_EQ(observation.sync_read_entry_snapshot_age_sec, age_sec);
    EXPECT_GE(misses, matches + unavailable);
    const std::string text = server_->MetricsText();
    const std::string prefix = "dfkv_slab_disk_lifetime_ghost_";
    const std::string labels =
        "{disk=\"" + Group().DiskPath(disk) + "\",disk_index=\"" +
        std::to_string(disk) +
        R"(",node="ghost\"node",group="ghost\\group\n"} )";
    for (const std::pair<const char*, uint64_t>& field :
         {std::pair<const char*, uint64_t>{"sync_read_misses_total", misses},
          {"sync_read_history_matches_total", matches},
          {"sync_read_lookup_unavailable_total", unavailable},
          {"sync_read_entry_snapshot_age_seconds_total", age_sec}}) {
      const std::string name = prefix + field.first;
      EXPECT_NE(
          text.find("\n" + name + labels + std::to_string(field.second) + "\n"),
          std::string::npos)
          << name;
      EXPECT_NE(text.find("# TYPE " + name + " counter\n"), std::string::npos);
    }
    size_t samples = 0;
    for (size_t pos = text.find("\n" + prefix + "sync_read_");
         pos != std::string::npos;
         pos = text.find("\n" + prefix + "sync_read_", pos + 1)) {
      const std::string line =
          text.substr(pos + 1, text.find('\n', pos + 1) - pos - 1);
      if (line.find(",disk_index=\"" + std::to_string(disk) + "\"") ==
          std::string::npos)
        continue;
      EXPECT_NE(line.find(labels), std::string::npos) << line;
      ++samples;
    }
    EXPECT_EQ(samples, 4U);
    EXPECT_EQ(text.find("dfkv_slab_lifetime_ghost_sync_read_"),
              std::string::npos);
  }

  const ScopedEnv ram_{"DFKV_RAM_TIER", "0"};
  const ScopedEnv write_{"DFKV_SLAB_WRITE", "buffered"};
  const ScopedEnv granularity_{"DFKV_SLAB_GRANULARITY", "1073741824"};
  const ScopedEnv reclaim_{"DFKV_SLAB_RECLAIM_MS", "0"};
  const ScopedEnv sync_{"DFKV_SLAB_TABLE_SYNC_MS", "0"};
  std::filesystem::path dir_;
  DiskLifetimeConfig config_;
  std::atomic<uint64_t> seconds_{0};
  std::atomic<int> fail_clock_after_{-1};
  std::atomic<uint64_t> clock_calls_{0};
  std::shared_ptr<DiskLifetimeClock> clock_;
  std::unique_ptr<KvNodeServer> server_;
  size_t instance_ = 0;
};

TEST_F(DiskEvictionGhostReadTest, BothApisAssociateOnlyActualFinalMisses) {
  for (const bool coalescing : {false, true}) {
    for (const bool into : {false, true}) {
      SCOPED_TRACE(coalescing);
      SCOPED_TRACE(into);
      ASSERT_NO_FATAL_FAILURE(Create(coalescing));
      const BlockKey key = Key();
      ASSERT_EQ(Put(key), Status::kOk);
      ASSERT_EQ(Put(Key(key.digest_hi + 1)), Status::kOk);
      ASSERT_EQ(Read(into, key), Status::kNotFound);
      ASSERT_NO_FATAL_FAILURE(Check(0, 1, 1, 0));
      seconds_ = 7;
      ASSERT_EQ(Read(into, key), Status::kNotFound);
      EXPECT_EQ(server_->m_cache_miss(), 2U);
      ASSERT_NO_FATAL_FAILURE(Check(0, 2, 2, 7));
    }
  }
}

TEST_F(DiskEvictionGhostReadTest, CompleteTenantKeyAndUnmatchedRequests) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const BlockKey key = Key();
  ASSERT_EQ(Put(key), Status::kOk);
  ASSERT_EQ(Put(Key(key.digest_hi + 1)), Status::kOk);
  BlockKey other_tenant = key;
  other_tenant.tenant_hash = 8;
  ASSERT_EQ(Read(false, other_tenant), Status::kNotFound);
  ASSERT_EQ(Read(true, Key(100)), Status::kNotFound);
  ASSERT_EQ(Read(false, key), Status::kNotFound);
  EXPECT_EQ(server_->m_cache_miss(), 3U);
  ASSERT_NO_FATAL_FAILURE(Check(0, 3, 1, 0));
}

TEST_F(DiskEvictionGhostReadTest,
       HistoryDoesNotMakeSuccessfulReadsOrPutMisses) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const BlockKey key = Key();
  ASSERT_EQ(Put(key), Status::kOk);
  ASSERT_EQ(Put(Key(key.digest_hi + 1)), Status::kOk);
  ASSERT_EQ(Put(key, "new"), Status::kOk);
  ASSERT_EQ(Put(key, "dup"), Status::kOk);
  seconds_ = 10;
  for (const bool into : {false, true}) {
    std::string bytes;
    ASSERT_EQ(Read(into, key, &bytes), Status::kOk);
    EXPECT_EQ(bytes, "new");
    ASSERT_EQ(Read(into, key, &bytes, 3), Status::kOk);
    EXPECT_TRUE(bytes.empty());
    EXPECT_EQ(Read(into, key, nullptr, 4), Status::kInvalid);
  }
  ValueMetadata metadata;
  EXPECT_EQ(server_->LookupForKey(key, &metadata), Status::kOk);
  EXPECT_EQ(Operation(WireOp::kLookup, key), Status::kOk);
  EXPECT_EQ(Operation(WireOp::kExist, key), Status::kOk);
  EXPECT_EQ(Operation(WireOp::kRemove, key), Status::kOk);
  ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
  // A subsequent real miss still matches older history, even though Remove
  // (after reinsertion) explains the current absence. Association is not cause.
  EXPECT_EQ(Read(false, key), Status::kNotFound);
  ASSERT_NO_FATAL_FAILURE(Check(0, 1, 1, 10));
}

TEST_F(DiskEvictionGhostReadTest,
       PreparationAndMetadataMissesAreNotSyncMisses) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const BlockKey key = Key();
  ASSERT_EQ(Put(key), Status::kOk);
  ASSERT_EQ(Put(Key(key.digest_hi + 1)), Status::kOk);
  EXPECT_EQ(Operation(WireOp::kLookup, key), Status::kNotFound);
  EXPECT_EQ(Operation(WireOp::kExist, key), Status::kNotFound);
  EXPECT_EQ(Operation(WireOp::kRemove, key), Status::kNotFound);
  alignas(4096) std::array<char, 4096> staging{};
  const char* data = nullptr;
  size_t length = 0;
  EXPECT_EQ(server_->RangeDirectForKey(key, 0, 3, staging.data(),
                                       staging.size(), &data, &length),
            Status::kNotFound);
  const PreparedRead read =
      server_->PrepareReadForKey(key, 0, 3, staging.data(), staging.size());
  EXPECT_EQ(read.status(), Status::kNotFound);
  ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
}

TEST_F(DiskEvictionGhostReadTest, DisabledCaptureDoesNotSampleClock) {
  for (const char* mode : {"lifetime", "legacy", "file"}) {
    SCOPED_TRACE(mode);
    const bool file = std::string(mode) == "file";
    ASSERT_NO_FATAL_FAILURE(
        Create(false, 1, 0, file ? "legacy" : mode, file ? "file" : "slab"));
    const uint64_t before = clock_calls_.load();
    const DiskSlabStore::EvictionGhostReadWitness witness =
        Group().CaptureEvictionGhostRead(Key());
    EXPECT_EQ(witness.result,
              DiskSlabStore::EvictionGhostReadWitness::Result::kDisabled);
    EXPECT_EQ(clock_calls_.load(), before);
    EXPECT_EQ(Read(false, Key()), Status::kNotFound);
    EXPECT_EQ(Read(true, Key()), Status::kNotFound);
    if (file) {
      EXPECT_EQ(server_->MetricsText().find("dfkv_slab_disk_lifetime_ghost_"),
                std::string::npos);
    } else {
      ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
    }
  }
}

TEST_F(DiskEvictionGhostReadTest, LookupClockFailureAndAdmissionRefusalDiffer) {
  for (const bool into : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(Create());
    // One valid admission sample, then a hard failure at the entry witness.
    // A missing Store key needs no later clock sample to establish NotFound.
    fail_clock_after_ = 1;
    EXPECT_EQ(Read(into, Key()), Status::kNotFound);
    ASSERT_NO_FATAL_FAILURE(Check(0, 1, 0, 0, 1));
    EXPECT_EQ(Read(into, Key()), Status::kIOError);
    ASSERT_NO_FATAL_FAILURE(Check(0, 1, 0, 0, 1));
  }
}

TEST_F(DiskEvictionGhostReadTest, DiskFailuresAreNotSyncMisses) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const BlockKey key = Key();
  ASSERT_EQ(Put(key), Status::kOk);
  DiskSlabStoreTestPeer::FailMetadata(DiskCacheGroupTestPeer::Slab(Group(), 0));
  ASSERT_EQ(Operation(WireOp::kRemove, key), Status::kIOError);
  for (const bool into : {false, true})
    EXPECT_EQ(Read(into, key), Status::kIOError);
  ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
}

TEST_F(DiskEvictionGhostReadTest,
       TwoDisksRetainIndependentAssociationCounters) {
  ASSERT_NO_FATAL_FAILURE(Create(false, 2));
  for (size_t disk = 0; disk < 2; ++disk) {
    const uint64_t evicted_at = seconds_.load();
    const BlockKey key = Key(1, disk);
    ASSERT_EQ(Put(key), Status::kOk);
    ASSERT_EQ(Put(Key(key.digest_hi + 1, disk)), Status::kOk);
    seconds_ = evicted_at + 3;
    EXPECT_EQ(Read(disk != 0, key), Status::kNotFound);
    ASSERT_NO_FATAL_FAILURE(Check(disk, 1, 1, 3));
  }
  EXPECT_EQ(server_->m_cache_miss(), 2U);
  ASSERT_NO_FATAL_FAILURE(Check(0, 1, 1, 3));
}

TEST_F(DiskEvictionGhostReadTest,
       LaterSameSecondEvictionCannotCreatePriorMatch) {
  for (const bool into : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(Create(true));
    const BlockKey key = Key();
    ASSERT_EQ(Put(key), Status::kOk);
    const uint64_t flight = KvNodeServerTestPeer::RegisterRead(*server_, key);
    ASSERT_NE(flight, 0U);
    std::future<Status> pending =
        std::async(std::launch::async, [&, into] { return Read(into, key); });
    const bool joined = KvNodeServerTestPeer::WaitForRead(*server_);
    const Status evicted =
        joined ? Put(Key(key.digest_hi + 1)) : Status::kInvalid;
    const bool completed = KvNodeServerTestPeer::FailRead(*server_, flight);
    const Status status = pending.get();
    // Always release the flight and join before any fatal assertion.
    ASSERT_TRUE(joined);
    ASSERT_EQ(evicted, Status::kOk);
    ASSERT_TRUE(completed);
    ASSERT_EQ(status, Status::kNotFound);
    EXPECT_EQ(KvNodeServerTestPeer::ReadTimeouts(*server_), 0U);
    const DiskSlabStore::EvictionGhostReadWitness later =
        Group().CaptureEvictionGhostRead(key);
    EXPECT_EQ(later.result,
              DiskSlabStore::EvictionGhostReadWitness::Result::kHistory);
    EXPECT_EQ(later.age_sec, 0U);
    ASSERT_NO_FATAL_FAILURE(Check(0, 1, 0, 0));
  }
}

TEST_F(DiskEvictionGhostReadTest, CapturedMatchSurvivesLaterHistoryTruncation) {
  for (const bool into : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(Create(true));
    const BlockKey key = Key();
    const BlockKey second = Key(key.digest_hi + 1);
    ASSERT_EQ(Put(key), Status::kOk);
    ASSERT_EQ(Put(second), Status::kOk);
    ASSERT_EQ(Put(key), Status::kOk);
    seconds_ = 5;
    const uint64_t flight = KvNodeServerTestPeer::RegisterRead(*server_, key);
    ASSERT_NE(flight, 0U);
    std::future<Status> pending =
        std::async(std::launch::async, [&, into] { return Read(into, key); });
    const bool joined = KvNodeServerTestPeer::WaitForRead(*server_);
    std::array<Status, 3> writes{Status::kInvalid, Status::kInvalid,
                                 Status::kInvalid};
    if (joined) {
      seconds_ = 20;
      BlockKey next = second;
      for (Status& write : writes) {
        next = Key(next.digest_hi + 1);
        write = Put(next);
      }
    }
    const bool completed = KvNodeServerTestPeer::FailRead(*server_, flight);
    const Status status = pending.get();
    ASSERT_TRUE(joined);
    for (const Status write : writes) ASSERT_EQ(write, Status::kOk);
    ASSERT_TRUE(completed);
    ASSERT_EQ(status, Status::kNotFound);
    EXPECT_EQ(KvNodeServerTestPeer::ReadTimeouts(*server_), 0U);
    const DiskSlabStore::EvictionGhostReadWitness later =
        Group().CaptureEvictionGhostRead(key);
    EXPECT_EQ(later.result,
              DiskSlabStore::EvictionGhostReadWitness::Result::kNoHistory);
    EXPECT_TRUE(
        Group().DiskEvictionGhostObservation(0).history.capacity_limited);
    // The entry snapshot saw the older event at t=0, not its later refresh or
    // the final miss at t=20. The preserved witness contributes exactly 5.
    ASSERT_NO_FATAL_FAILURE(Check(0, 1, 1, 5));
  }
}

TEST_F(DiskEvictionGhostReadTest,
       RamHitsWithPriorGhostEvidenceDoNotCountMisses) {
  const ScopedEnv ram("DFKV_RAM_TIER", "1");
  const ScopedEnv bytes("DFKV_RAM_TIER_BYTES", "8388608");
  const ScopedEnv shards("DFKV_RAM_TIER_SHARDS", "1");
  const ScopedEnv writearound("DFKV_RAM_WRITE_MODE", "writearound");
  ASSERT_NO_FATAL_FAILURE(Create());
  const BlockKey key = Key();
  ASSERT_EQ(Put(key), Status::kOk);
  ASSERT_EQ(Put(Key(key.digest_hi + 1)), Status::kOk);
  ASSERT_TRUE(KvNodeServerTestPeer::Ram(*server_).PutDurable(key, "ram", 3));
  for (const bool into : {false, true}) {
    std::string output;
    ASSERT_EQ(Read(into, key, &output), Status::kOk);
    EXPECT_EQ(output, "ram");
  }
  EXPECT_EQ(server_->m_cache_miss(), 0U);
  ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
}
}  // namespace
}  // namespace dfkv
