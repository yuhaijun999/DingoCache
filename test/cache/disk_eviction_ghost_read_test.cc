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
#include <optional>
#include <string>
#include <utility>
#include <vector>

// Third-party headers.
#include <gtest/gtest.h>

#include "cache/disk_cache_group.h"
#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/disk_slab_store.h"
#include "cache/kv_node_server.h"
#include "cache/ram_tier.h"
#include "cache/read_coalescer.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "read_coalescer_test_peer.h"
#include "slab_allocator_test_peer.h"
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
  static std::optional<DiskLifetimeState> State(const DiskSlabStore& store,
                                                const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeState(*store.alloc_, key);
  }
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
              const char* policy = "lifetime", const char* engine = "slab",
              bool identify = true, size_t slots = 1) {
    server_.reset();
    ResetClock();
    DiskCacheGroup::Options options;
    const std::filesystem::path instance = dir_ / std::to_string(instance_++);
    for (size_t disk = 0; disk < disks; ++disk)
      options.cache_dirs.push_back((instance / std::to_string(disk)).string());
    // Sparse 1-GiB slots keep payloads tiny while controlling real capacity.
    // This is functional eviction control, not a memory/performance test.
    options.capacity_bytes = disks * slots * (1ULL << 30);
    options.engine = engine;
    options.eviction_policy = policy;
    options.eviction_ghost_capacity_per_disk = capacity;
    if (std::string(policy) == "lifetime") {
      options.lifetime_config = config_;
      options.lifetime_clock = clock_;
    }
    options_ = options;
    coalescing_ = coalescing;
    const ScopedEnv coalesce("DFKV_READ_COALESCE", coalescing_ ? "1" : "0");
    server_ = std::make_unique<KvNodeServer>(options_);
    ASSERT_TRUE(server_->Healthy()) << Group().StartupError();
    identified_ = identify;
    if (identified_) server_->set_identity("ghost\"node", "ghost\\group\n");
  }
  void ResetClock(uint64_t epoch = 0) {
    seconds_ = epoch;
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
  }
  void Reopen(uint64_t epoch) {
    // Finish the old owner's clean close before creating another clock origin.
    server_.reset();
    ResetClock(epoch);
    if (options_.lifetime_clock) options_.lifetime_clock = clock_;
    const ScopedEnv coalesce("DFKV_READ_COALESCE", coalescing_ ? "1" : "0");
    server_ = std::make_unique<KvNodeServer>(options_);
    ASSERT_TRUE(server_->Healthy()) << Group().StartupError();
    if (identified_) server_->set_identity("ghost\"node", "ghost\\group\n");
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
  void CheckSync(uint64_t nonempty, uint64_t empty, uint64_t not_found,
                 uint64_t invalid, uint64_t error, uint64_t admission_refused) {
    const std::string text = server_->MetricsText();
    const std::string prefix = "dfkv_sync_read_results_";
    const std::string labels =
        identified_ ? R"({node="ghost\"node",group="ghost\\group\n"})" : "";
    for (const std::pair<const char*, uint64_t>& result :
         {std::pair<const char*, uint64_t>{"nonempty_success_total", nonempty},
          {"empty_success_total", empty},
          {"not_found_total", not_found},
          {"invalid_total", invalid},
          {"error_total", error},
          {"admission_refused_total", admission_refused}}) {
      const std::string name = prefix + result.first;
      EXPECT_NE(text.find("\n" + name + labels + " " +
                          std::to_string(result.second) + "\n"),
                std::string::npos)
          << name;
      EXPECT_NE(text.find("# TYPE " + name + " counter\n"), std::string::npos)
          << name;
    }
    // Exactly six node-scoped samples: no extra disk/key labels, aggregates or
    // duplicate samples may conceal an incorrectly classified logical call.
    size_t samples = 0;
    for (size_t pos = text.find("\n" + prefix); pos != std::string::npos;
         pos = text.find("\n" + prefix, pos + 1)) {
      ++samples;
    }
    EXPECT_EQ(samples, 6U);
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
  DiskCacheGroup::Options options_;
  size_t instance_ = 0;
  bool identified_ = true;
  bool coalescing_ = false;
};

TEST_F(DiskEvictionGhostReadTest,
       CleanRestartForegroundTraceKeepsColdMajorityAndOwnerBoundaries) {
  // Formal lifetime startup overlays numeric environment values on Options.
  // Keep this functional oracle independent of the invoking shell's settings.
  const ScopedEnv base("DFKV_SLAB_LIFE_BASE_SEC", "100");
  const ScopedEnv bonus("DFKV_SLAB_LIFE_READ_BONUS_SEC", "10");
  const ScopedEnv max_reads("DFKV_SLAB_LIFE_MAX_READS", "2");
  const ScopedEnv protect("DFKV_SLAB_PROTECT_SEC", "8");
  const ScopedEnv slowdown("DFKV_SLAB_PROTECT_SLOWDOWN", "4");
  const ScopedEnv boost("DFKV_SLAB_PROTECT_BOOST_SEC", "120");
  const ScopedEnv keep("DFKV_SLAB_KEEP_SCORE_SEC", "90");
  const ScopedEnv bucket("DFKV_SLAB_BUCKET_SEC", "16");
  const ScopedEnv buckets("DFKV_SLAB_BUCKET_COUNT", "8");
  const ScopedEnv low("DFKV_SLAB_EVICT_LOW_PCT", "88");
  const ScopedEnv high("DFKV_SLAB_EVICT_HIGH_PCT", "0");
  config_.max_reads = 2;
  config_.boost_sec = 120;
  ASSERT_NO_FATAL_FAILURE(Create(false, 1, 2, "lifetime", "slab", true, 8));
  const std::string directory = Group().DiskPath(0);
  struct Resident {
    BlockKey key;
    std::string value;
  };
  std::vector<Resident> live;
  std::vector<BlockKey> gone;
  for (uint64_t id = 1; id <= 8; ++id) {
    const Resident resident{
        Key(id), std::string(id <= 2 ? "h0" : "o0") + std::to_string(id)};
    ASSERT_EQ(Put(resident.key, resident.value.c_str()), Status::kOk);
    live.push_back(resident);
  }
  for (size_t round = 0; round < 3; ++round) {
    SCOPED_TRACE(round);
    const uint64_t epoch = (round + 1) * 1000;
    ASSERT_NO_FATAL_FAILURE(Reopen(epoch));
    EXPECT_EQ(Group().DiskPath(0), directory);
    ASSERT_EQ(server_->Count(), 8U);
    ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 0, 0, 0, 0));
    ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
    const DiskSlabStore::Stats rebuilt = Group().DiskSlabStats(0);
    EXPECT_EQ(rebuilt.table_rebuilt, 8U);
    EXPECT_EQ(rebuilt.unclean_resets, 0U);
    EXPECT_EQ(rebuilt.lifetime_reads.successful, 0U);
    EXPECT_EQ(rebuilt.lifetime_reads.applied, 0U);
    const DiskSlabStore::EvictionGhostObservation empty =
        Group().DiskEvictionGhostObservation(0);
    EXPECT_EQ(empty.recorded, 0U);
    EXPECT_EQ(empty.dropped, 0U);
    EXPECT_EQ(empty.history.retained, 0U);
    EXPECT_FALSE(empty.history.capacity_limited);
    EXPECT_FALSE(empty.history.lost_through_sec.has_value());
    EXPECT_FALSE(empty.history.loss_time_unknown);
    EXPECT_EQ(empty.history.window_after_sec, std::optional<uint64_t>(0));
    EXPECT_EQ(empty.history.window_through_sec, std::optional<uint64_t>(0));
    const DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(Group(), 0);
    for (const Resident& resident : live) {
      const std::optional<DiskLifetimeState> state =
          DiskSlabStoreTestPeer::State(store, resident.key);
      if (!state.has_value()) {
        ADD_FAILURE() << "Committed resident has no lifetime state";
        return;
      }
      EXPECT_TRUE(state->history_unknown);
      EXPECT_EQ(state->scoring_reads(), 2U);
      EXPECT_EQ(state->actual_reads, 0U);
    }
    for (const BlockKey& key : gone) {
      EXPECT_EQ(Read(false, key), Status::kNotFound);
    }
    seconds_ = epoch + 30;
    for (size_t hot = 0; hot < 2; ++hot) {
      for (const bool into : {false, true}) {
        std::string bytes;
        ASSERT_EQ(Read(into, live[hot].key, &bytes), Status::kOk);
        EXPECT_EQ(bytes, live[hot].value);
      }
    }
    seconds_ = epoch + 40;
    for (size_t index = 0; index < live.size(); ++index) {
      const std::optional<DiskLifetimeState> state =
          DiskSlabStoreTestPeer::State(store, live[index].key);
      if (!state.has_value()) {
        ADD_FAILURE() << "Committed resident has no lifetime state";
        return;
      }
      EXPECT_EQ(state->actual_reads, index < 2 ? 2U : 0U);
      EXPECT_EQ(DiskLifetimeScore(
                    config_, state->anchor, state->scoring_reads(),
                    state->first_early_read(), state->write_armed(), 40),
                index < 2 ? 110 : 80);
    }
    const Resident added{Key(9 + round), "n0" + std::to_string(round)};
    ASSERT_EQ(Put(added.key, added.value.c_str()), Status::kOk);
    EXPECT_EQ(server_->Count(), 8U);
    // These post-selection reads validate bytes only; the next clean reopen
    // resets their heat before another pressure decision.
    std::vector<Resident> survivors;
    for (size_t index = 0; index < live.size(); ++index) {
      std::string bytes;
      const Status status = Read(index % 2 != 0, live[index].key, &bytes);
      if (status == Status::kNotFound) {
        ASSERT_GE(index, 2U);  // A cold member, never a particular tied key.
        gone.push_back(live[index].key);
      } else {
        ASSERT_EQ(status, Status::kOk);
        EXPECT_EQ(bytes, live[index].value);
        survivors.push_back(live[index]);
      }
    }
    ASSERT_EQ(gone.size(), round + 1);
    ASSERT_EQ(survivors.size(), 7U);
    std::string bytes;
    ASSERT_EQ(Read(false, added.key, &bytes), Status::kOk);
    EXPECT_EQ(bytes, added.value);
    survivors.push_back(added);
    live = std::move(survivors);
    ASSERT_NO_FATAL_FAILURE(CheckSync(12, 0, round + 1, 0, 0, 0));
    ASSERT_NO_FATAL_FAILURE(Check(0, round + 1, 1, 0));
    const DiskSlabStore::Stats completed = Group().DiskSlabStats(0);
    EXPECT_EQ(completed.lifetime_class_victims, 1U);
    EXPECT_EQ(completed.lifetime_reads.successful, 12U);
    EXPECT_EQ(completed.lifetime_reads.applied, 12U);
    const DiskSlabStore::EvictionGhostObservation settled =
        Group().DiskEvictionGhostObservation(0);
    EXPECT_EQ(settled.recorded, 1U);
    EXPECT_EQ(settled.history.retained, 1U);
    EXPECT_EQ(settled.history.oldest_evicted_at_sec,
              std::optional<uint64_t>(40));
    EXPECT_EQ(settled.history.newest_evicted_at_sec,
              std::optional<uint64_t>(40));
    EXPECT_EQ(settled.history.window_after_sec, std::optional<uint64_t>(0));
    EXPECT_EQ(settled.history.window_through_sec, std::optional<uint64_t>(40));
  }
  // A fourth clean reopen only validates the last victim and surviving bytes;
  // the three pressure rounds above do not splice time across owner clocks.
  ASSERT_NO_FATAL_FAILURE(Reopen(4000));
  EXPECT_EQ(Group().DiskPath(0), directory);
  EXPECT_EQ(server_->Count(), 8U);
  const DiskSlabStore::EvictionGhostObservation final_empty =
      Group().DiskEvictionGhostObservation(0);
  EXPECT_EQ(final_empty.recorded, 0U);
  EXPECT_EQ(final_empty.history.retained, 0U);
  EXPECT_EQ(final_empty.history.window_after_sec, std::optional<uint64_t>(0));
  EXPECT_EQ(final_empty.history.window_through_sec, std::optional<uint64_t>(0));
  ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 0, 0, 0, 0));
  for (const BlockKey& key : gone) {
    EXPECT_EQ(Read(true, key), Status::kNotFound);
  }
  for (const Resident& resident : live) {
    std::string bytes;
    ASSERT_EQ(Read(false, resident.key, &bytes), Status::kOk);
    EXPECT_EQ(bytes, resident.value);
  }
  ASSERT_NO_FATAL_FAILURE(CheckSync(8, 0, 3, 0, 0, 0));
  ASSERT_NO_FATAL_FAILURE(Check(0, 3, 0, 0));
}

TEST_F(DiskEvictionGhostReadTest,
       CleanRecoveryNewHotReadsCrossProtectionBoundaryThroughFifteenMinutes) {
  const ScopedEnv base("DFKV_SLAB_LIFE_BASE_SEC", "100");
  const ScopedEnv bonus("DFKV_SLAB_LIFE_READ_BONUS_SEC", "10");
  const ScopedEnv max_reads("DFKV_SLAB_LIFE_MAX_READS", "2");
  const ScopedEnv protect("DFKV_SLAB_PROTECT_SEC", "180");
  const ScopedEnv slowdown("DFKV_SLAB_PROTECT_SLOWDOWN", "4");
  const ScopedEnv boost("DFKV_SLAB_PROTECT_BOOST_SEC", "120");
  const ScopedEnv keep("DFKV_SLAB_KEEP_SCORE_SEC", "90");
  const ScopedEnv bucket("DFKV_SLAB_BUCKET_SEC", "32");
  const ScopedEnv buckets("DFKV_SLAB_BUCKET_COUNT", "8");
  const ScopedEnv low("DFKV_SLAB_EVICT_LOW_PCT", "88");
  const ScopedEnv high("DFKV_SLAB_EVICT_HIGH_PCT", "0");
  config_.protect_sec = 180;
  config_.bucket_sec = 32;
  config_.max_reads = 2;
  config_.boost_sec = 120;
  ASSERT_NO_FATAL_FAILURE(Create(false, 1, 8, "lifetime", "slab", true, 8));
  const std::string directory = Group().DiskPath(0);
  struct Resident {
    BlockKey key;
    std::string value;
  };
  std::vector<Resident> original;
  for (uint64_t id = 1; id <= 8; ++id) {
    const Resident resident{Key(id), "o0" + std::to_string(id)};
    ASSERT_EQ(Put(resident.key, resident.value.c_str()), Status::kOk);
    original.push_back(resident);
  }
  ASSERT_NO_FATAL_FAILURE(Reopen(1000));
  EXPECT_EQ(Group().DiskPath(0), directory);
  ASSERT_EQ(server_->Count(), 8U);
  ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 0, 0, 0, 0));
  ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
  const DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(Group(), 0);
  // Membership checks are read-only: they must not manufacture business heat
  // while identifying which tied recovered cold residents actually vanished.
  const auto check_cold = [&](size_t expected) {
    size_t retained = 0;
    for (const Resident& resident : original) {
      const std::optional<DiskLifetimeState> state =
          DiskSlabStoreTestPeer::State(store, resident.key);
      if (!state.has_value()) continue;
      ++retained;
      EXPECT_TRUE(state->history_unknown);
      EXPECT_EQ(state->scoring_reads(), 2U);
      EXPECT_EQ(state->actual_reads, 0U);
      EXPECT_EQ(state->anchor, 0U);
    }
    EXPECT_EQ(retained, expected);
  };
  check_cold(8);
  const std::array<Resident, 5> fresh{
      Resident{Key(9), "a09"}, Resident{Key(10), "b10"},
      Resident{Key(11), "c11"}, Resident{Key(12), "d12"},
      Resident{Key(13), "e13"}};
  for (size_t index = 0; index < 3; ++index) {
    ASSERT_EQ(Put(fresh[index].key, fresh[index].value.c_str()), Status::kOk);
  }
  check_cold(5);
  EXPECT_EQ(Group().DiskEvictionGhostObservation(0).recorded, 3U);
  std::string bytes;
  seconds_ = 1179;
  ASSERT_EQ(Read(false, fresh[0].key, &bytes), Status::kOk);
  EXPECT_EQ(bytes, fresh[0].value);
  seconds_ = 1180;
  ASSERT_EQ(Read(true, fresh[1].key, &bytes), Status::kOk);
  EXPECT_EQ(bytes, fresh[1].value);
  for (size_t index = 0; index < 2; ++index) {
    const std::optional<DiskLifetimeState> state =
        DiskSlabStoreTestPeer::State(store, fresh[index].key);
    if (!state.has_value()) {
      ADD_FAILURE() << "NEW resident vanished at the protection boundary";
      return;
    }
    EXPECT_EQ(state->put_at, 0U);
    EXPECT_EQ(state->anchor, 179U + index);
    EXPECT_EQ(state->scoring_reads(), 1U);
    EXPECT_EQ(state->actual_reads, 1U);
    EXPECT_FALSE(state->history_unknown);
    EXPECT_FALSE(state->write_armed());
    // Early-read reward uses age < P, whereas logical R includes age == P.
    EXPECT_EQ(state->first_early_read(), index == 0);
    EXPECT_EQ(ClassifyDiskLifetimeRead(config_, *state, 180),
              DiskLifetimeReadClass::kReadWithinNewWriteWindow);
    EXPECT_EQ(ClassifyDiskLifetimeRead(config_, *state, 181),
              DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
  }
  seconds_ = 1181;
  ASSERT_EQ(Read(false, fresh[2].key, &bytes), Status::kOk);
  EXPECT_EQ(bytes, fresh[2].value);
  const std::optional<DiskLifetimeState> outside =
      DiskSlabStoreTestPeer::State(store, fresh[2].key);
  if (!outside.has_value()) {
    ADD_FAILURE() << "NEW resident vanished after the protection boundary";
    return;
  }
  EXPECT_EQ(ClassifyDiskLifetimeRead(config_, *outside, 181),
            DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
  seconds_ = 1899;
  ASSERT_EQ(Put(fresh[3].key, fresh[3].value.c_str()), Status::kOk);
  ASSERT_EQ(Read(true, fresh[3].key, &bytes), Status::kOk);
  EXPECT_EQ(bytes, fresh[3].value);
  check_cold(4);
  seconds_ = 1900;
  ASSERT_EQ(Put(fresh[4].key, fresh[4].value.c_str()), Status::kOk);
  check_cold(3);
  ASSERT_EQ(server_->Count(), 8U);
  ASSERT_NO_FATAL_FAILURE(CheckSync(4, 0, 0, 0, 0, 0));
  const std::array<uint32_t, 5> anchors{179, 180, 181, 899, 900};
  for (size_t index = 0; index < fresh.size(); ++index) {
    const std::optional<DiskLifetimeState> state =
        DiskSlabStoreTestPeer::State(store, fresh[index].key);
    if (!state.has_value()) {
      ADD_FAILURE()
          << "A new hot resident was evicted instead of recovered cold";
      return;
    }
    EXPECT_FALSE(state->history_unknown);
    EXPECT_EQ(state->put_at, index < 3 ? 0U : anchors[index]);
    EXPECT_EQ(state->anchor, anchors[index]);
    EXPECT_EQ(state->scoring_reads(), index == 4 ? 0U : 1U);
    EXPECT_EQ(state->actual_reads, index == 4 ? 0U : 1U);
    EXPECT_EQ(state->first_early_read(), index == 0 || index == 3);
    EXPECT_EQ(state->write_armed(), index == 4);
  }
  // Only now validate every surviving value. These successful GETs are part of
  // the explicit 12-result oracle, not inputs to any later victim choice.
  for (const Resident& resident : original) {
    const bool retained =
        DiskSlabStoreTestPeer::State(store, resident.key).has_value();
    ASSERT_EQ(Read(false, resident.key, &bytes),
              retained ? Status::kOk : Status::kNotFound);
    if (retained) {
      EXPECT_EQ(bytes, resident.value);
    }
  }
  for (const Resident& resident : fresh) {
    ASSERT_EQ(Read(true, resident.key, &bytes), Status::kOk);
    EXPECT_EQ(bytes, resident.value);
  }
  ASSERT_NO_FATAL_FAILURE(CheckSync(12, 0, 5, 0, 0, 0));
  ASSERT_NO_FATAL_FAILURE(Check(0, 5, 5, 2701));
  const DiskSlabStore::Stats stats = Group().DiskSlabStats(0);
  EXPECT_EQ(stats.lifetime_reads.successful, 12U);
  EXPECT_EQ(stats.lifetime_reads.applied, 12U);
  EXPECT_EQ(stats.lifetime_class_victims, 5U);
  const DiskSlabStore::EvictionGhostObservation ghost =
      Group().DiskEvictionGhostObservation(0);
  EXPECT_EQ(ghost.recorded, 5U);
  EXPECT_EQ(ghost.dropped, 0U);
  EXPECT_EQ(ghost.history.retained, 5U);
  EXPECT_FALSE(ghost.history.capacity_limited);
  EXPECT_FALSE(ghost.history.loss_time_unknown);
  EXPECT_FALSE(ghost.history.lost_through_sec.has_value());
  EXPECT_EQ(ghost.history.window_after_sec, std::optional<uint64_t>(0));
  EXPECT_EQ(ghost.history.window_through_sec, std::optional<uint64_t>(900));
  // Three retained events settled at t=0 are outside exclusive (0,900]. Raw
  // miss association is not conditioned on that window: 3*900 + 1 + 0 = 2701.
}

TEST_F(DiskEvictionGhostReadTest,
       SyncResultCohortIsIndependentOfGhostAndPolicy) {
  for (const char* mode : {"file", "legacy", "lifetime_disabled", "lifetime"}) {
    for (const bool into : {false, true}) {
      SCOPED_TRACE(mode);
      SCOPED_TRACE(into);
      const bool file = std::string(mode) == "file";
      const bool legacy = std::string(mode) == "legacy";
      const size_t capacity = std::string(mode) == "lifetime" ? 2 : 0;
      ASSERT_NO_FATAL_FAILURE(Create(false, 1, capacity,
                                     file || legacy ? "legacy" : "lifetime",
                                     file ? "file" : "slab", !file));
      ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 0, 0, 0, 0));
      const BlockKey key = Key();
      ASSERT_EQ(Put(key), Status::kOk);
      std::string bytes;
      ASSERT_EQ(Read(into, key, &bytes), Status::kOk);
      EXPECT_EQ(bytes, "old");
      ASSERT_NO_FATAL_FAILURE(CheckSync(1, 0, 0, 0, 0, 0));
      ASSERT_EQ(Read(into, key, &bytes, 3), Status::kOk);
      EXPECT_TRUE(bytes.empty());
      ASSERT_NO_FATAL_FAILURE(CheckSync(1, 1, 0, 0, 0, 0));
      ASSERT_EQ(Read(into, key, nullptr, 4), Status::kInvalid);
      ASSERT_NO_FATAL_FAILURE(CheckSync(1, 1, 0, 1, 0, 0));
      ASSERT_EQ(Operation(WireOp::kRemove, key), Status::kOk);
      ASSERT_EQ(Read(into, key), Status::kNotFound);
      EXPECT_EQ(Operation(WireOp::kLookup, key), Status::kNotFound);
      EXPECT_EQ(Operation(WireOp::kExist, key), Status::kNotFound);
      std::string ignored;
      EXPECT_EQ(
          server_->ProcessRequestForKey(255, key, 0, 0, nullptr, 0, &ignored),
          Status::kInvalid);
      ASSERT_NO_FATAL_FAILURE(CheckSync(1, 1, 1, 1, 0, 0));
    }
  }
}

TEST_F(DiskEvictionGhostReadTest, IntoWithoutLengthOutputUsesActualByteCount) {
  ASSERT_NO_FATAL_FAILURE(Create());
  const BlockKey key = Key();
  ASSERT_EQ(Put(key), Status::kOk);
  std::array<char, 16> bytes{};
  size_t value_len = 0;
  EXPECT_EQ(server_->RangeIntoForKey(key, 0, 3, bytes.data(), bytes.size(),
                                     nullptr, &value_len),
            Status::kOk);
  EXPECT_EQ(std::string(bytes.data(), 3), "old");
  EXPECT_EQ(value_len, 3U);
  ASSERT_NO_FATAL_FAILURE(CheckSync(1, 0, 0, 0, 0, 0));
  EXPECT_EQ(server_->RangeIntoForKey(key, 3, 3, bytes.data(), bytes.size(),
                                     nullptr, &value_len),
            Status::kOk);
  EXPECT_EQ(value_len, 3U);
  ASSERT_NO_FATAL_FAILURE(CheckSync(1, 1, 0, 0, 0, 0));
  EXPECT_EQ(
      server_->RangeIntoForKey(key, 4, 3, bytes.data(), bytes.size(), nullptr),
      Status::kInvalid);
  ASSERT_NO_FATAL_FAILURE(CheckSync(1, 1, 0, 1, 0, 0));
  EXPECT_EQ(server_->RangeIntoForKey(Key(key.digest_hi + 1), 0, 3, bytes.data(),
                                     bytes.size(), nullptr),
            Status::kNotFound);
  ASSERT_NO_FATAL_FAILURE(CheckSync(1, 1, 1, 1, 0, 0));
}

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
      ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 1, 0, 0, 0));
      seconds_ = 7;
      ASSERT_EQ(Read(into, key), Status::kNotFound);
      EXPECT_EQ(server_->m_cache_miss(), 2U);
      ASSERT_NO_FATAL_FAILURE(Check(0, 2, 2, 7));
      ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 2, 0, 0, 0));
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
  ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 3, 0, 0, 0));
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
  ASSERT_NO_FATAL_FAILURE(CheckSync(2, 2, 0, 2, 0, 0));
  // A subsequent real miss still matches older history, even though Remove
  // (after reinsertion) explains the current absence. Association is not cause.
  EXPECT_EQ(Read(false, key), Status::kNotFound);
  ASSERT_NO_FATAL_FAILURE(Check(0, 1, 1, 10));
  ASSERT_NO_FATAL_FAILURE(CheckSync(2, 2, 1, 2, 0, 0));
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
  EXPECT_EQ(server_->m_cache_miss(), 2U);
  ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 0, 0, 0, 0));
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
    ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 2, 0, 0, 0));
    if (file) {
      EXPECT_EQ(server_->MetricsText().find("dfkv_slab_disk_lifetime_ghost_"),
                std::string::npos);
    } else {
      ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
    }
  }
}

TEST_F(DiskEvictionGhostReadTest, LookupClockFailureAndAdmissionRefusalDiffer) {
  for (const bool coalescing : {false, true}) {
    for (const bool into : {false, true}) {
      SCOPED_TRACE(coalescing);
      SCOPED_TRACE(into);
      ASSERT_NO_FATAL_FAILURE(Create(coalescing));
      // Admission succeeds before the witness sample hard-fails. Only the
      // coalesced string API performs a nested Lookup admission recheck; that
      // later refusal is an admitted-call error, not outer admission refusal.
      fail_clock_after_ = 1;
      const bool nested_refusal = coalescing && !into;
      EXPECT_EQ(Read(into, Key()),
                nested_refusal ? Status::kIOError : Status::kNotFound);
      const uint64_t misses = nested_refusal ? 0 : 1;
      const uint64_t errors = nested_refusal ? 1 : 0;
      ASSERT_NO_FATAL_FAILURE(Check(0, misses, 0, 0, misses));
      ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, misses, 0, errors, 0));
      EXPECT_EQ(Read(into, Key()), Status::kIOError);
      ASSERT_NO_FATAL_FAILURE(Check(0, misses, 0, 0, misses));
      ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, misses, 0, errors, 1));
    }
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
  ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 0, 0, 2, 0));
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
  ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 2, 0, 0, 0));
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
    ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 1, 0, 0, 0));
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
    ASSERT_NO_FATAL_FAILURE(CheckSync(0, 0, 1, 0, 0, 0));
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
    EXPECT_EQ(Read(into, key, &output, 3), Status::kOk);
    EXPECT_TRUE(output.empty());
  }
  EXPECT_EQ(server_->m_cache_miss(), 0U);
  ASSERT_NO_FATAL_FAILURE(Check(0, 0, 0, 0));
  ASSERT_NO_FATAL_FAILURE(CheckSync(2, 2, 0, 0, 0, 0));
}
}  // namespace
}  // namespace dfkv
