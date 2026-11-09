#include <gtest/gtest.h>
// POSIX setenv/unsetenv have no std:: equivalent and require this header.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cache/disk_cache_group.h"
#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/kv_node_server.h"
#include "cache/slab_allocator.h"
#include "cache/store_engine.h"
#include "common/config_dump.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"

namespace dfkv {

class DiskCacheGroupTestPeer {
 public:
  static DiskSlabStore& Slab(DiskCacheGroup& group, size_t index = 0) {
    return *group.slabs_[index];
  }
  static size_t Route(const DiskCacheGroup& group, const BlockKey& key) {
    return group.RouteIndex(key);
  }
};

class DiskSlabStoreTestPeer {
 public:
  static std::optional<DiskLifetimeState> State(DiskSlabStore& store,
                                                const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeState(*store.alloc_, key);
  }
  static bool Valid(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeIndexesValid(*store.alloc_) &&
           SlabAllocatorTestPeer::LifetimeBucketsValid(*store.alloc_);
  }
  static size_t Classes(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.alloc_->ClassCount();
  }
  static size_t Queues(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeQueueCount(*store.alloc_);
  }
  static size_t Pages(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::BucketPageCount(*store.alloc_);
  }
  static std::vector<BlockKey> Queue(DiskSlabStore& store, size_t row,
                                     uint32_t band) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeQueueKeys(*store.alloc_, row, band);
  }
  static size_t Class(DiskSlabStore& store, uint32_t size) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::ClassIndex(*store.alloc_, size);
  }
  static DiskLifetimeConfig Config(const DiskSlabStore& store) {
    if (!store.opt_.lifetime_config.has_value()) {
      ADD_FAILURE() << "The slab has no lifetime config";
      return {};
    }
    return *store.opt_.lifetime_config;
  }
  static const DiskLifetimeClock* Clock(const DiskSlabStore& store) {
    return store.opt_.lifetime_clock.get();
  }
  static bool FormalRecovery(const DiskSlabStore& store) {
    return store.opt_.lifetime_recovery_two_reads;
  }
  static void Tick(DiskSlabStore& store) { store.ReclaimTick(); }
  static void FailRecordWrites(DiskSlabStore& store) {
    store.record_write_hook_for_test_ = [] { return false; };
  }
};

namespace {

const char* const kNumericNames[] = {
    "DFKV_SLAB_LIFE_BASE_SEC",    "DFKV_SLAB_LIFE_READ_BONUS_SEC",
    "DFKV_SLAB_LIFE_MAX_READS",   "DFKV_SLAB_PROTECT_SEC",
    "DFKV_SLAB_PROTECT_SLOWDOWN", "DFKV_SLAB_PROTECT_BOOST_SEC",
    "DFKV_SLAB_KEEP_SCORE_SEC",   "DFKV_SLAB_BUCKET_SEC",
    "DFKV_SLAB_BUCKET_COUNT",     "DFKV_SLAB_EVICT_LOW_PCT",
    "DFKV_SLAB_EVICT_HIGH_PCT"};

class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    const char* previous = std::getenv(name);
    if (previous != nullptr) previous_ = previous;
    if (value != nullptr) {
      EXPECT_EQ(::setenv(name, value, 1), 0);
    } else {
      EXPECT_EQ(::unsetenv(name), 0);
    }
  }
  ~ScopedEnv() {
    if (previous_.has_value()) {
      ::setenv(name_.c_str(), previous_->c_str(), 1);
    } else {
      ::unsetenv(name_.c_str());
    }
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;

 private:
  std::string name_;
  std::optional<std::string> previous_;
};

using Snapshot = std::vector<std::pair<bool, std::string>>;

Snapshot Metadata(const std::string& directory) {
  Snapshot result;
  for (const char* name :
       {"slab_meta", "slab_state", "slots.tbl", "slab_initializing"}) {
    const std::filesystem::path path = std::filesystem::path(directory) / name;
    std::ifstream input(path, std::ios::binary);
    result.emplace_back(std::filesystem::exists(path),
                        std::string(std::istreambuf_iterator<char>(input),
                                    std::istreambuf_iterator<char>()));
  }
  return result;
}

class DiskLifetimePublicTest : public testing::Test {
 protected:
  void SetUp() override {
    base_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_public_" +
         std::string(
             testing::UnitTest::GetInstance()->current_test_info()->name()));
    std::filesystem::remove_all(base_);
    Env("DFKV_RAM_TIER", "0");
    Env("DFKV_RAM_WRITE_MODE", nullptr);
    Env("DFKV_PUT_ACK_MODE", nullptr);
    Env("DFKV_SLAB_EVICTION_POLICY", nullptr);
    Env("DFKV_SLAB_WRITE", "buffered");
    Env("DFKV_SLAB_GRANULARITY", "1048576");
    Env("DFKV_SLAB_TABLE_SYNC_MS", "0");
    Env("DFKV_SLAB_RECLAIM_MS", "0");
    Env("DFKV_SLAB_COLD_STEAL_WINDOW", "0");
    for (const char* name : kNumericNames) Env(name, nullptr);
  }
  void TearDown() override { std::filesystem::remove_all(base_); }
  void Env(const char* name, const char* value) {
    environment_.push_back(std::make_unique<ScopedEnv>(name, value));
  }
  DiskCacheGroup::Options GroupOptions(const std::string& suffix = "disk") {
    DiskCacheGroup::Options options;
    options.cache_dirs = {(base_ / suffix).string()};
    options.engine = "slab";
    options.eviction_policy = "lifetime";
    return options;
  }
  std::shared_ptr<DiskLifetimeClock> Clock(const DiskLifetimeConfig& config) {
    return std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(config), [this] { return seconds_.load(); });
  }
  DiskSlabStore::Options StoreOptions(const std::string& suffix = "small",
                                      uint64_t max_reads = 2) {
    DiskSlabStore::Options options;
    options.dir = (base_ / suffix).string();
    options.capacity_bytes = 8 * 4096;
    options.extent_bytes = options.capacity_bytes;
    options.slot_granularity = 4096;
    options.reclaim_interval_ms = 0;
    options.table_sync_ms = 0;
    options.track_resident_identity = true;
    DiskLifetimeConfig config;
    config.max_reads = max_reads;
    config.bucket_sec = 400000;
    config.bucket_count = 64;
    options.lifetime_config = config;
    options.lifetime_clock = Clock(config);
    options.lifetime_class_eviction = true;
    options.lifetime_recovery_two_reads = true;
    return options;
  }
  void Read(DiskSlabStore& store, const BlockKey& key,
            const std::string& expected = "bytes") {
    std::string bytes;
    DiskResidentIdentity identity;
    ASSERT_EQ(
        store.RangeWithIdentity(key, 0, expected.size(), &bytes, &identity),
        Status::kOk);
    ASSERT_EQ(bytes, expected);
    store.NotifyLifetimeReadSuccess(key, identity, bytes.size());
  }
  void CheckRecovered(DiskSlabStore& store, const BlockKey& key,
                      uint8_t virtual_reads = 2) {
    const std::optional<DiskLifetimeState> state =
        DiskSlabStoreTestPeer::State(store, key);
    if (!state.has_value()) {
      ADD_FAILURE() << "The committed lifetime state is missing";
      return;
    }
    EXPECT_EQ(state->hstate, virtual_reads << 2);
    EXPECT_EQ(state->anchor, 0U);
    EXPECT_EQ(state->put_at, 0U);
    EXPECT_EQ(state->actual_reads, 0U);
    EXPECT_TRUE(state->history_unknown);
    EXPECT_FALSE(state->write_armed());
    EXPECT_FALSE(state->first_early_read());
    EXPECT_EQ(ClassifyDiskLifetimeRead(DiskSlabStoreTestPeer::Config(store),
                                       *state, 0),
              DiskLifetimeReadClass::kUnread);
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(store));
  }
  void ExpectPopulation(const DiskSlabStore& store, uint64_t recovered,
                        uint64_t unread) {
    uint64_t total_recovered = 0;
    uint64_t total_unread = 0;
    const std::vector<SlabAllocator::ClassStat> classes = store.ClassStats();
    for (const SlabAllocator::ClassStat& cls : classes) {
      EXPECT_LE(cls.recovered_unread_residents, cls.recovered_residents);
      EXPECT_LE(cls.recovered_residents, cls.resident);
      total_recovered += cls.recovered_residents;
      total_unread += cls.recovered_unread_residents;
    }
    EXPECT_EQ(total_recovered, recovered);
    EXPECT_EQ(total_unread, unread);
  }
  std::filesystem::path base_;
  std::atomic<uint64_t> seconds_{0};
  std::vector<std::unique_ptr<ScopedEnv>> environment_;
};

TEST_F(DiskLifetimePublicTest, DefaultAndExplicitLegacyIgnoreInactiveNumbers) {
  const ScopedEnv invalid("DFKV_SLAB_LIFE_MAX_READS", "not-a-number");
  for (const std::string policy : {"default", "legacy"}) {
    DiskCacheGroup::Options options = GroupOptions(policy);
    options.eviction_policy = policy;
    if (policy == "default") options.eviction_policy.reset();
    DiskCacheGroup group(options);
    ASSERT_TRUE(group.Healthy()) << group.StartupError();
    EXPECT_FALSE(group.TracksLifetimeState());
    EXPECT_FALSE(group.HasLifetimeClock());
    ASSERT_EQ(group.Cache({1, 0}, "bytes", 5), Status::kOk);
  }
  DiskCacheGroup::Options file = GroupOptions("file");
  file.engine = "file";
  file.eviction_policy.reset();
  const DiskCacheGroup group(file);
  EXPECT_TRUE(group.Healthy()) << group.StartupError();
}

TEST_F(DiskLifetimePublicTest, ExplicitPolicyOverridesEnvironment) {
  const ScopedEnv environment_policy("DFKV_SLAB_EVICTION_POLICY", "lifetime");
  DiskCacheGroup::Options legacy = GroupOptions("legacy");
  legacy.eviction_policy = "LeGaCy";
  const DiskCacheGroup old_group(legacy);
  ASSERT_TRUE(old_group.Healthy()) << old_group.StartupError();
  EXPECT_FALSE(old_group.TracksLifetimeState());
  DiskCacheGroup::Options environment = GroupOptions("environment");
  environment.eviction_policy.reset();
  const DiskCacheGroup environment_group(environment);
  ASSERT_TRUE(environment_group.Healthy()) << environment_group.StartupError();
  EXPECT_TRUE(environment_group.TracksLifetimeState());
  const ScopedEnv override_policy("DFKV_SLAB_EVICTION_POLICY", "legacy");
  DiskCacheGroup::Options explicit_policy = GroupOptions("explicit");
  explicit_policy.eviction_policy = "LiFeTiMe";
  const DiskCacheGroup group(explicit_policy);
  ASSERT_TRUE(group.Healthy()) << group.StartupError();
  EXPECT_TRUE(group.TracksLifetimeState());
}

TEST_F(DiskLifetimePublicTest, ExplicitFileLifetimeRejectsBeforeOpening) {
  DiskCacheGroup::Options options = GroupOptions();
  options.engine = "file";
  const DiskCacheGroup group(options);
  EXPECT_FALSE(group.Healthy());
  EXPECT_NE(group.StartupError().find("lifetime"), std::string::npos);
  EXPECT_FALSE(std::filesystem::exists(base_));
}

TEST_F(DiskLifetimePublicTest, PublicCapsAllocateActualQueuesAndBuckets) {
  for (const uint64_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    const std::string text = std::to_string(max_reads);
    const ScopedEnv cap("DFKV_SLAB_LIFE_MAX_READS", text.c_str());
    const ScopedEnv width("DFKV_SLAB_BUCKET_SEC", "400000");
    const ScopedEnv count("DFKV_SLAB_BUCKET_COUNT", "64");
    DiskCacheGroup group(GroupOptions(text));
    ASSERT_TRUE(group.Healthy()) << group.StartupError();
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group);
    EXPECT_TRUE(DiskSlabStoreTestPeer::FormalRecovery(store));
    EXPECT_EQ(DiskSlabStoreTestPeer::Config(store).max_reads, max_reads);
    const size_t rows = DiskSlabStoreTestPeer::Classes(store) + 1;
    EXPECT_GT(rows, 1U);
    EXPECT_EQ(DiskSlabStoreTestPeer::Queues(store), rows * (max_reads + 1));
    EXPECT_EQ(DiskSlabStoreTestPeer::Pages(store), 64U);
    ASSERT_EQ(group.Cache({1, 0}, "bytes", 5), Status::kOk);
    ASSERT_NO_FATAL_FAILURE(Read(store, {1, 0}));
    const std::optional<DiskLifetimeState> state =
        DiskSlabStoreTestPeer::State(store, {1, 0});
    if (!state.has_value()) {
      ADD_FAILURE() << "The committed lifetime state is missing";
      return;
    }
    EXPECT_EQ(state->scoring_reads(), 1U);
    EXPECT_EQ(
        DiskLifetimeBand(DiskSlabStoreTestPeer::Config(store),
                         state->scoring_reads(), state->first_early_read()),
        std::min<uint64_t>(2, max_reads));
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(store));
  }
}

TEST_F(DiskLifetimePublicTest, MalformedApplicableNumbersRejectWithoutOpening) {
  for (const char* name : kNumericNames) {
    for (const char* raw : {"", "-1", "1x"}) {
      SCOPED_TRACE(std::string(name) + "='" + raw + "'");
      const ScopedEnv invalid(name, raw);
      const DiskCacheGroup group(GroupOptions());
      EXPECT_FALSE(group.Healthy());
      EXPECT_EQ(group.DiskCount(), 0U);
      EXPECT_NE(group.StartupError().find(name), std::string::npos);
      EXPECT_NE(group.StartupError().find("'" + std::string(raw) + "'"),
                std::string::npos);
      EXPECT_FALSE(std::filesystem::exists(base_));
    }
  }
}

TEST_F(DiskLifetimePublicTest, InvalidNumericConstraintsRejectWithoutOpening) {
  const std::pair<const char*, const char*> invalid[] = {
      {"DFKV_SLAB_LIFE_MAX_READS", "0"},
      {"DFKV_SLAB_LIFE_MAX_READS", "64"},
      {"DFKV_SLAB_BUCKET_COUNT", "0"},
      {"DFKV_SLAB_BUCKET_COUNT", "65"},
      {"DFKV_SLAB_BUCKET_SEC", "0"},
      {"DFKV_SLAB_KEEP_SCORE_SEC", "604800"},
      {"DFKV_SLAB_PROTECT_SLOWDOWN", "0"},
      {"DFKV_SLAB_LIFE_MAX_READS", "4"},
      {"DFKV_SLAB_LIFE_BASE_SEC", "2147483648"}};
  for (const std::pair<const char*, const char*>& entry : invalid) {
    SCOPED_TRACE(std::string(entry.first) + "=" + entry.second);
    const ScopedEnv setting(entry.first, entry.second);
    const DiskCacheGroup group(GroupOptions());
    EXPECT_FALSE(group.Healthy());
    EXPECT_FALSE(group.StartupError().empty());
    EXPECT_FALSE(std::filesystem::exists(base_));
  }
}

TEST_F(DiskLifetimePublicTest,
       FormalDumpReportsEffectivePolicyAndDerivedBoost) {
  const ScopedEnv bonus("DFKV_SLAB_LIFE_READ_BONUS_SEC", "100");
  config_dump::ResetForTest();
  testing::internal::CaptureStderr();
  const DiskCacheGroup group(GroupOptions());
  config_dump::Emit("public-lifetime-test");
  const std::string output = testing::internal::GetCapturedStderr();
  ASSERT_TRUE(group.Healthy()) << group.StartupError();
  const auto expect_entry = [&output](const char* name, const char* tail) {
    const size_t begin = output.find(name);
    ASSERT_NE(begin, std::string::npos) << output;
    EXPECT_NE(output.substr(begin, output.find('\n', begin) - begin).find(tail),
              std::string::npos)
        << output;
  };
  expect_entry("disk_eviction_effective", " = lifetime");
  expect_entry("disk_eviction_staged", " = false");
  expect_entry("disk_eviction_internal_test", " = off");
  expect_entry("DFKV_SLAB_LIFE_READ_BONUS_SEC", " = 100  (env)");
  expect_entry("DFKV_SLAB_PROTECT_BOOST_SEC", " = 605000");
}

TEST_F(DiskLifetimePublicTest, RamWritebackRequiresDiskAckBeforeDirectories) {
  const ScopedEnv ram("DFKV_RAM_TIER", "1");
  const ScopedEnv ram_bytes("DFKV_RAM_TIER_BYTES", "8388608");
  for (const char* acknowledgement : {"missing", "ram"}) {
    const ScopedEnv ack(
        "DFKV_PUT_ACK_MODE",
        std::string(acknowledgement) == "missing" ? nullptr : acknowledgement);
    const KvNodeServer node(GroupOptions());
    EXPECT_FALSE(node.Healthy());
    EXPECT_FALSE(std::filesystem::exists(base_));
  }
  const ScopedEnv disk_ack("DFKV_PUT_ACK_MODE", "disk");
  const KvNodeServer node(GroupOptions());
  EXPECT_TRUE(node.Healthy());
}

TEST_F(DiskLifetimePublicTest, LaterDiskFailurePreservesCleanAndDirtyMetadata) {
  for (const bool dirty : {false, true}) {
    SCOPED_TRACE(dirty);
    const std::string suffix = dirty ? "dirty" : "clean";
    DiskSlabStore::Options first_options;
    first_options.dir = (base_ / suffix / "a").string();
    first_options.reclaim_interval_ms = 0;
    first_options.table_sync_ms = 0;
    DiskSlabStore::Options second_options = first_options;
    second_options.dir = (base_ / suffix / "b").string();
    second_options.slot_granularity = 2U << 20;
    std::unique_ptr<DiskSlabStore> first =
        std::make_unique<DiskSlabStore>(first_options);
    ASSERT_TRUE(first->Healthy()) << first->StartupError();
    ASSERT_EQ(first->Cache({1, 0}, "bytes", 5), Status::kOk);
    {
      DiskSlabStore second(second_options);
      ASSERT_TRUE(second.Healthy()) << second.StartupError();
      ASSERT_EQ(second.Cache({2, 0}, "bytes", 5), Status::kOk);
    }
    if (!dirty) first.reset();
    const Snapshot before_first = Metadata(first_options.dir);
    const Snapshot before_second = Metadata(second_options.dir);
    DiskCacheGroup::Options options = GroupOptions();
    options.cache_dirs = {first_options.dir, second_options.dir};
    options.capacity_bytes = 2ull << 30;
    const DiskCacheGroup group(options);
    EXPECT_FALSE(group.Healthy());
    EXPECT_NE(group.StartupError().find(second_options.dir), std::string::npos);
    EXPECT_EQ(group.DiskCount(), 0U);
    EXPECT_EQ(Metadata(first_options.dir), before_first);
    EXPECT_EQ(Metadata(second_options.dir), before_second);
  }
}

TEST_F(DiskLifetimePublicTest,
       RecoveredBaselineAndFirstReadMoveBothQueueTails) {
  for (const uint64_t max_reads : {1U, 2U, 4U, 63U}) {
    SCOPED_TRACE(max_reads);
    DiskSlabStore::Options options =
        StoreOptions(std::to_string(max_reads), max_reads);
    {
      DiskSlabStore seed(options);
      ASSERT_TRUE(seed.Healthy()) << seed.StartupError();
      ASSERT_EQ(seed.Cache({1, 0}, "bytes", 5), Status::kOk);
      ASSERT_EQ(seed.Cache({2, 0}, "bytes", 5), Status::kOk);
      ExpectPopulation(seed, 0, 0);
    }
    DiskSlabStore recovered(options);
    ASSERT_TRUE(recovered.Healthy()) << recovered.StartupError();
    ASSERT_NO_FATAL_FAILURE(CheckRecovered(recovered, {1, 0}));
    ASSERT_NO_FATAL_FAILURE(CheckRecovered(recovered, {2, 0}));
    ExpectPopulation(recovered, 2, 2);
    const uint32_t band =
        static_cast<uint32_t>(std::min<uint64_t>(3, max_reads));
    seconds_.store(1);
    ASSERT_NO_FATAL_FAILURE(Read(recovered, {1, 0}));
    const std::optional<DiskLifetimeState> state =
        DiskSlabStoreTestPeer::State(recovered, {1, 0});
    if (!state.has_value()) {
      ADD_FAILURE() << "The committed lifetime state is missing";
      return;
    }
    EXPECT_EQ(state->scoring_reads(), 3U);
    EXPECT_EQ(state->actual_reads, 1U);
    EXPECT_EQ(state->anchor, 1U);
    EXPECT_FALSE(state->first_early_read());
    EXPECT_FALSE(state->write_armed());
    EXPECT_EQ(ClassifyDiskLifetimeRead(*options.lifetime_config, *state, 1),
              DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
    const size_t row = DiskSlabStoreTestPeer::Class(recovered, 4096) + 1;
    for (const size_t scope : {size_t{0}, row}) {
      const std::vector<BlockKey> keys =
          DiskSlabStoreTestPeer::Queue(recovered, scope, band);
      ASSERT_FALSE(keys.empty());
      EXPECT_EQ(keys.back(), (BlockKey{1, 0}));
    }
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(recovered));
    ExpectPopulation(recovered, 2, 1);
    ASSERT_NO_FATAL_FAILURE(Read(recovered, {1, 0}));
    ExpectPopulation(recovered, 2, 1);  // Only the first real read leaves W.
    seconds_.store(0);
  }
}

TEST_F(DiskLifetimePublicTest, InternalStagingKeepsOneVirtualRead) {
  DiskSlabStore::Options options = StoreOptions();
  options.lifetime_recovery_two_reads = false;
  {
    DiskSlabStore seed(options);
    ASSERT_TRUE(seed.Healthy());
    ASSERT_EQ(seed.Cache({1, 0}, "bytes", 5), Status::kOk);
  }
  DiskSlabStore recovered(options);
  ASSERT_TRUE(recovered.Healthy());
  ASSERT_NO_FATAL_FAILURE(CheckRecovered(recovered, {1, 0}, 1));
}

TEST_F(DiskLifetimePublicTest,
       ConsecutiveCleanRestartsResetOnlyVirtualHistory) {
  const DiskSlabStore::Options options = StoreOptions();
  {
    DiskSlabStore seed(options);
    ASSERT_TRUE(seed.Healthy());
    ASSERT_EQ(seed.Cache({1, 0}, "bytes", 5), Status::kOk);
  }
  for (int restart = 0; restart < 3; ++restart) {
    DiskSlabStore recovered(options);
    ASSERT_TRUE(recovered.Healthy());
    ASSERT_NO_FATAL_FAILURE(CheckRecovered(recovered, {1, 0}));
    ExpectPopulation(recovered, 1, 1);
    ASSERT_NO_FATAL_FAILURE(Read(recovered, {1, 0}));
    ExpectPopulation(recovered, 1, 0);
  }
}

TEST_F(DiskLifetimePublicTest, PublicLifetimeLegacyLifetimePreservesBytes) {
  DiskCacheGroup::Options options = GroupOptions();
  bool first_start = true;
  for (const std::string policy : {"lifetime", "legacy", "lifetime"}) {
    options.eviction_policy = policy;
    DiskCacheGroup group(options);
    ASSERT_TRUE(group.Healthy()) << group.StartupError();
    if (first_start) {
      first_start = false;
      ASSERT_EQ(group.Cache({1, 0}, "bytes", 5), Status::kOk);
    }
    ASSERT_TRUE(group.IsCached({1, 0}));
    std::string bytes;
    ASSERT_EQ(group.Range({1, 0}, 0, 5, &bytes), Status::kOk);
    EXPECT_EQ(bytes, "bytes");
    if (policy == "legacy") {
      EXPECT_FALSE(group.TracksLifetimeState());
      ExpectPopulation(DiskCacheGroupTestPeer::Slab(group), 0, 0);
    } else {
      EXPECT_TRUE(DiskSlabStoreTestPeer::FormalRecovery(
          DiskCacheGroupTestPeer::Slab(group)));
    }
  }
}

TEST_F(DiskLifetimePublicTest, DirtyFormalRestartClearsRecordsAndHeat) {
  const DiskSlabStore::Options options = StoreOptions();
  DiskSlabStore dirty(options);
  ASSERT_TRUE(dirty.Healthy());
  ASSERT_EQ(dirty.Cache({1, 0}, "bytes", 5), Status::kOk);
  DiskSlabStore recovered(options);
  ASSERT_TRUE(recovered.Healthy()) << recovered.StartupError();
  EXPECT_FALSE(recovered.IsCached({1, 0}));
  EXPECT_FALSE(DiskSlabStoreTestPeer::State(recovered, {1, 0}).has_value());
  ExpectPopulation(recovered, 0, 0);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(recovered));
}

TEST_F(DiskLifetimePublicTest, MultiplePublicSlabsShareOriginAndRecoverySeed) {
  DiskCacheGroup::Options options = GroupOptions();
  options.cache_dirs.push_back((base_ / "second").string());
  options.capacity_bytes = 2ull << 30;
  const DiskLifetimeConfig config;
  options.lifetime_clock = Clock(config);
  std::vector<BlockKey> keys(2);
  {
    DiskCacheGroup seed(options);
    ASSERT_TRUE(seed.Healthy()) << seed.StartupError();
    for (size_t disk = 0; disk < 2; ++disk) {
      uint64_t candidate = 1;
      while (DiskCacheGroupTestPeer::Route(seed, {candidate, 0}) != disk)
        ++candidate;
      keys[disk] = {candidate, 0};
      ASSERT_EQ(seed.Cache(keys[disk], "bytes", 5), Status::kOk);
    }
  }
  seconds_.store(10);
  DiskCacheGroup recovered(options);
  ASSERT_TRUE(recovered.Healthy()) << recovered.StartupError();
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(recovered, disk);
    EXPECT_EQ(DiskSlabStoreTestPeer::Clock(store),
              options.lifetime_clock.get());
    ASSERT_NO_FATAL_FAILURE(CheckRecovered(store, keys[disk]));
    ASSERT_NO_FATAL_FAILURE(Read(store, keys[disk]));
    const std::optional<DiskLifetimeState> state =
        DiskSlabStoreTestPeer::State(store, keys[disk]);
    if (!state.has_value()) {
      ADD_FAILURE() << "The committed lifetime state is missing";
      return;
    }
    EXPECT_EQ(state->anchor, 10U);
  }
}

TEST_F(DiskLifetimePublicTest,
       RecoveredPopulationTracksClassesAndMetadataReuse) {
  DiskSlabStore::Options options = StoreOptions();
  // Small writes stripe across extents. Leave a free extent for the larger
  // class so this recovery-accounting fixture never needs a donor victim.
  options.capacity_bytes = 4 * options.extent_bytes;
  const std::string large(5000, 'x');
  {
    DiskSlabStore seed(options);
    ASSERT_TRUE(seed.Healthy());
    ASSERT_EQ(seed.Cache({1, 0}, "bytes", 5), Status::kOk);
    ASSERT_EQ(seed.Cache({2, 0}, "bytes", 5), Status::kOk);
    ASSERT_EQ(seed.Cache({3, 0}, large.data(), large.size()), Status::kOk);
    ASSERT_EQ(seed.Count(), 3U);
    ASSERT_TRUE(seed.IsCached({1, 0}));
    ASSERT_TRUE(seed.IsCached({2, 0}));
    ASSERT_TRUE(seed.IsCached({3, 0}));
    ExpectPopulation(seed, 0, 0);
  }
  DiskSlabStore recovered(options);
  ASSERT_TRUE(recovered.Healthy());
  ExpectPopulation(recovered, 3, 3);
  std::string bytes;
  DiskResidentIdentity mismatched;
  ASSERT_EQ(recovered.RangeWithIdentity({1, 0}, 0, 5, &bytes, &mismatched),
            Status::kOk);
  ASSERT_EQ(bytes, "bytes");
  // A wrong-generation notification must not consume recovered unread state.
  ++mismatched.generation;
  recovered.NotifyLifetimeReadSuccess({1, 0}, mismatched, bytes.size());
  ExpectPopulation(recovered, 3, 3);
  EXPECT_EQ(recovered.GetStats().lifetime_reads.race_lost, 1U);
  ASSERT_NO_FATAL_FAILURE(Read(recovered, {1, 0}));
  ASSERT_NO_FATAL_FAILURE(Read(recovered, {1, 0}));
  ExpectPopulation(recovered, 3, 2);
  const std::vector<SlabAllocator::ClassStat> classes = recovered.ClassStats();
  for (const SlabAllocator::ClassStat& cls : classes) {
    EXPECT_EQ(cls.recovered_residents, cls.slot_size == 4096   ? 2U
                                       : cls.slot_size == 8192 ? 1U
                                                               : 0U);
    EXPECT_EQ(cls.recovered_unread_residents,
              cls.slot_size == 4096 || cls.slot_size == 8192 ? 1U : 0U);
  }
  ASSERT_EQ(recovered.Cache({4, 0}, "new", 3), Status::kOk);
  ExpectPopulation(recovered, 3, 2);  // NEW data has no recovered history.
  ASSERT_EQ(recovered.Remove({1, 0}), Status::kOk);
  ExpectPopulation(recovered, 2, 2);
  ASSERT_EQ(recovered.Remove({2, 0}), Status::kOk);
  ExpectPopulation(recovered, 1, 1);
  ASSERT_EQ(recovered.Cache({2, 0}, "replacement", 11), Status::kOk);
  ExpectPopulation(recovered, 1, 1);  // Recycled metadata starts as NEW.
  ASSERT_EQ(recovered.Remove({3, 0}), Status::kOk);
  ExpectPopulation(recovered, 0, 0);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(recovered));
}

TEST_F(DiskLifetimePublicTest,
       RecoveredPopulationWaitsForSuccessfulLastPinRelease) {
  for (const bool fail_invalidation : {false, true}) {
    const DiskSlabStore::Options options =
        StoreOptions(fail_invalidation ? "failed" : "success");
    {
      DiskSlabStore seed(options);
      ASSERT_TRUE(seed.Healthy());
      ASSERT_EQ(seed.Cache({1, 0}, "bytes", 5), Status::kOk);
    }
    DiskSlabStore recovered(options);
    ASSERT_TRUE(recovered.Healthy());
    ExpectPopulation(recovered, 1, 1);
    ReadLease first;
    ReadLease last;
    ASSERT_EQ(recovered.RangeDirectPrep({1, 0}, 0, 0, 4096, &first),
              Status::kOk);
    ASSERT_EQ(recovered.RangeDirectPrep({1, 0}, 0, 0, 4096, &last),
              Status::kOk);
    ASSERT_EQ(recovered.Remove({1, 0}), Status::kOk);
    EXPECT_FALSE(recovered.IsCached({1, 0}));
    ExpectPopulation(recovered, 1, 1);  // Hidden, but still physically owned.
    first = ReadLease{};
    ExpectPopulation(recovered, 1, 1);
    if (fail_invalidation) DiskSlabStoreTestPeer::FailRecordWrites(recovered);
    last = ReadLease{};
    EXPECT_EQ(recovered.Healthy(), !fail_invalidation);
    ExpectPopulation(recovered, fail_invalidation ? 1 : 0,
                     fail_invalidation ? 1 : 0);
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(recovered));
  }
}

TEST_F(DiskLifetimePublicTest, ConcurrentFirstReadsLeaveUnreadOnlyOnce) {
  const DiskSlabStore::Options options = StoreOptions();
  {
    DiskSlabStore seed(options);
    ASSERT_TRUE(seed.Healthy());
    ASSERT_EQ(seed.Cache({1, 0}, "bytes", 5), Status::kOk);
  }
  DiskSlabStore recovered(options);
  ASSERT_TRUE(recovered.Healthy());
  ExpectPopulation(recovered, 1, 1);
  constexpr size_t kReaders = 4;
  std::vector<Status> results(kReaders, Status::kInvalid);
  std::vector<std::string> values(kReaders);
  std::mutex start_mutex;
  std::condition_variable start_cv;
  bool start = false;
  std::vector<std::thread> readers;
  for (size_t index = 0; index < kReaders; ++index) {
    readers.emplace_back([&, index] {
      {
        std::unique_lock<std::mutex> lock(start_mutex);
        if (!start_cv.wait_for(lock, std::chrono::seconds(5),
                               [&] { return start; })) {
          return;
        }
      }
      DiskResidentIdentity identity;
      results[index] =
          recovered.RangeWithIdentity({1, 0}, 0, 5, &values[index], &identity);
      if (results[index] == Status::kOk) {
        recovered.NotifyLifetimeReadSuccess({1, 0}, identity,
                                            values[index].size());
      }
    });
  }
  {
    const std::lock_guard<std::mutex> lock(start_mutex);
    start = true;
  }
  start_cv.notify_all();
  for (std::thread& reader : readers) reader.join();
  for (size_t index = 0; index < kReaders; ++index) {
    EXPECT_EQ(results[index], Status::kOk);
    EXPECT_EQ(values[index], "bytes");
  }
  ExpectPopulation(recovered, 1, 0);
  EXPECT_EQ(recovered.GetStats().lifetime_reads.applied, kReaders);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(recovered));
}

TEST_F(DiskLifetimePublicTest, ImmediateWatermarkCannotResurrectRecoveredKeys) {
  DiskSlabStore::Options options = StoreOptions();
  if (!options.lifetime_config.has_value()) {
    FAIL() << "The test options must include lifetime config";
  }
  options.lifetime_config->low_pct = 50;
  options.lifetime_config->high_pct = 75;
  std::vector<uint64_t> survivors;
  {
    DiskSlabStore seed(options);
    ASSERT_TRUE(seed.Healthy());
    for (uint64_t key = 1; key <= 8; ++key)
      ASSERT_EQ(seed.Cache({key, 0}, "bytes", 5), Status::kOk);
  }
  {
    DiskSlabStore recovered(options);
    ASSERT_TRUE(recovered.Healthy());
    ExpectPopulation(recovered, 8, 8);
    DiskSlabStoreTestPeer::Tick(recovered);
    EXPECT_GT(recovered.Count(), 0U);
    EXPECT_LE(recovered.Count(), 4U);
    for (uint64_t key = 1; key <= 8; ++key) {
      if (recovered.IsCached({key, 0})) survivors.push_back(key);
    }
    EXPECT_EQ(recovered.Count(), survivors.size());
    ExpectPopulation(recovered, survivors.size(), survivors.size());
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(recovered));
  }
  DiskSlabStore reopened(options);
  ASSERT_TRUE(reopened.Healthy());
  EXPECT_EQ(reopened.Count(), survivors.size());
  ExpectPopulation(reopened, survivors.size(), survivors.size());
  for (uint64_t key = 1; key <= 8; ++key) {
    const bool survived =
        std::find(survivors.begin(), survivors.end(), key) != survivors.end();
    EXPECT_EQ(reopened.IsCached({key, 0}), survived) << "key=" << key;
    if (survived) {
      ASSERT_NO_FATAL_FAILURE(Read(reopened, {key, 0}));
    }
  }
  ExpectPopulation(reopened, survivors.size(), 0);
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(reopened));
}

TEST_F(DiskLifetimePublicTest, ImmediateDonorRebindDoesNotResurrectOldClass) {
  const DiskSlabStore::Options options = StoreOptions();
  {
    DiskSlabStore seed(options);
    ASSERT_TRUE(seed.Healthy());
    for (uint64_t key = 1; key <= 8; ++key)
      ASSERT_EQ(seed.Cache({key, 0}, "bytes", 5), Status::kOk);
  }
  const std::string large(5000, 'x');
  {
    DiskSlabStore recovered(options);
    ASSERT_TRUE(recovered.Healthy());
    ASSERT_NO_FATAL_FAILURE(CheckRecovered(recovered, {1, 0}));
    ExpectPopulation(recovered, 8, 8);
    ASSERT_EQ(recovered.Cache({100, 0}, large.data(), large.size()),
              Status::kOk);
    for (uint64_t key = 1; key <= 8; ++key)
      EXPECT_FALSE(recovered.IsCached({key, 0}));
    EXPECT_EQ(recovered.Count(), 1U);
    ExpectPopulation(recovered, 0, 0);
    EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(recovered));
  }
  DiskSlabStore reopened(options);
  ASSERT_TRUE(reopened.Healthy());
  EXPECT_EQ(reopened.Count(), 1U);
  ASSERT_NO_FATAL_FAILURE(CheckRecovered(reopened, {100, 0}));
  ExpectPopulation(reopened, 1, 1);
  ASSERT_NO_FATAL_FAILURE(Read(reopened, {100, 0}, large));
  ExpectPopulation(reopened, 1, 0);
  for (uint64_t key = 1; key <= 8; ++key)
    EXPECT_FALSE(reopened.IsCached({key, 0}));
  EXPECT_TRUE(DiskSlabStoreTestPeer::Valid(reopened));
}

}  // namespace
}  // namespace dfkv
