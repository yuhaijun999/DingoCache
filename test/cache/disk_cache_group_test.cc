// TDD R7 — DiskCacheGroup: one cache node spanning multiple NVMe SSDs, mirroring
// dingo-cache's intra-node Ketama across --cache_dir=d1,d2,d3 (each disk an
// independent KVStore with its own LRU; total capacity split across disks).
#include "cache/disk_cache_group.h"

#include <gtest/gtest.h>
// POSIX setenv/unsetenv require the C header, not the ISO C++ <cstdlib> API.
#include <stdlib.h>  // NOLINT(hicpp-deprecated-headers,modernize-deprecated-headers)

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "cache/kv_node_server.h"
#include "common/kv_types.h"
#include "common/status.h"

namespace fs = std::filesystem;
using namespace dfkv;  // NOLINT

TEST(DiskEvictionPolicyParsing, EnumHasExplicitByteValues) {
  static_assert(
      std::is_same_v<std::underlying_type_t<DiskEvictionPolicy>, unsigned char>,
      "DiskEvictionPolicy must use unsigned char");
  EXPECT_EQ(static_cast<unsigned char>(DiskEvictionPolicy::kInvalid), 0U);
  EXPECT_EQ(static_cast<unsigned char>(DiskEvictionPolicy::kLegacy), 1U);
  EXPECT_EQ(static_cast<unsigned char>(DiskEvictionPolicy::kLifetime), 2U);
}

TEST(DiskEvictionPolicyParsing, AcceptsAsciiCaseInsensitivePolicyNames) {
  for (const std::string_view text : {"legacy", "LEGACY", "Legacy", "lEgAcY"}) {
    SCOPED_TRACE(text);
    const DiskEvictionPolicy result = ParseDiskEvictionPolicy(text);
    EXPECT_EQ(result, DiskEvictionPolicy::kLegacy);
  }
  for (const std::string_view text :
       {"lifetime", "LIFETIME", "Lifetime", "lIfEtImE"}) {
    SCOPED_TRACE(text);
    const DiskEvictionPolicy result = ParseDiskEvictionPolicy(text);
    EXPECT_EQ(result, DiskEvictionPolicy::kLifetime);
  }
}

TEST(DiskEvictionPolicyParsing, RejectsExplicitEmptyAndUnknownNames) {
  for (const std::string_view text :
       {"", "unknown", "LEGAC", "LIFETIMES", " legacy", "lifetime ", "\t",
        "legacy,lifetime", "l\xFFgacy"}) {
    SCOPED_TRACE(text);
    const DiskEvictionPolicy result = ParseDiskEvictionPolicy(text);
    EXPECT_EQ(result, DiskEvictionPolicy::kInvalid);
  }
}

TEST(DiskEvictionPolicyParsing, DoesNotIgnoreEmbeddedNullOrTrailingBytes) {
  const char text[] = "legacy\0lifetime";
  const DiskEvictionPolicy result =
      ParseDiskEvictionPolicy(std::string_view(text, sizeof(text) - 1));
  EXPECT_EQ(result, DiskEvictionPolicy::kInvalid);
}

namespace {
struct BatchProbe {
  void Enter(size_t disk) {
    std::unique_lock<std::mutex> lock(mu);
    ++calls;
    ++calls_by_disk[disk];
    ++active;
    max_active = std::max(max_active, active);
    threads.push_back(std::this_thread::get_id());
    if (wait_for_overlap) {
      if (active >= 2) {
        release = true;
        cv.notify_all();
      } else {
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return release; });
      }
    }
    --active;
  }
  std::mutex mu;
  std::condition_variable cv;
  size_t calls = 0;
  size_t active = 0;
  size_t max_active = 0;
  bool wait_for_overlap = true;
  bool release = false;
  std::vector<std::thread::id> threads;
  std::vector<size_t> calls_by_disk;
};

class ScopedEnv {
 public:
  explicit ScopedEnv(const char* name) : ScopedEnv(name, std::string()) {
    ::unsetenv(name);
  }
  ScopedEnv(const char* name, const std::string& value) : name_(name) {
    if (const char* old = std::getenv(name)) {
      had_old_ = true;
      old_ = old;
    }
    ::setenv(name, value.c_str(), 1);
  }
  ~ScopedEnv() {
    if (had_old_)
      ::setenv(name_.c_str(), old_.c_str(), 1);
    else
      ::unsetenv(name_.c_str());
  }

 private:
  std::string name_;
  std::string old_;
  bool had_old_ = false;
};

TEST(DiskEvictionPolicyParsing, DoesNotReadEnvironment) {
  const ScopedEnv env("DFKV_SLAB_EVICTION_POLICY", "unknown");
  const DiskEvictionPolicy result = ParseDiskEvictionPolicy("legacy");
  EXPECT_EQ(result, DiskEvictionPolicy::kLegacy);
  EXPECT_STREQ(std::getenv("DFKV_SLAB_EVICTION_POLICY"), "unknown");
}

TEST(DiskEvictionConfig, DefaultAndExplicitLegacyAreDistinct) {
  const DiskEvictionConfigResult default_config = ResolveDiskEvictionConfig({});
  EXPECT_EQ(default_config.status, Status::kOk);
  EXPECT_EQ(default_config.policy, DiskEvictionPolicy::kLifetime);
  EXPECT_EQ(default_config.source, DiskEvictionPolicySource::kDefault);
  EXPECT_EQ(default_config.engine, DiskEvictionEngine::kSlab);

  DiskEvictionConfigInput explicit_input;
  explicit_input.cli_policy = "LEGACY";
  const DiskEvictionConfigResult explicit_config =
      ResolveDiskEvictionConfig(explicit_input);
  EXPECT_EQ(explicit_config.status, Status::kOk);
  EXPECT_EQ(explicit_config.policy, DiskEvictionPolicy::kLegacy);
  EXPECT_EQ(explicit_config.source, DiskEvictionPolicySource::kCommandLine);
}

TEST(DiskEvictionConfig, PolicyAndEngineSourcesHaveIndependentPriority) {
  DiskEvictionConfigInput input;
  input.cli_policy = "legacy";
  input.env_policy = "invalid";
  input.cli_engine = "slab";
  input.env_engine = "invalid";
  const DiskEvictionConfigResult cli = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(cli.status, Status::kOk);
  EXPECT_EQ(cli.source, DiskEvictionPolicySource::kCommandLine);
  EXPECT_EQ(cli.engine, DiskEvictionEngine::kSlab);

  input.cli_policy.reset();
  input.cli_engine.reset();
  input.env_policy = "legacy";
  input.env_engine = "file";
  const DiskEvictionConfigResult env = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(env.status, Status::kOk);
  EXPECT_EQ(env.source, DiskEvictionPolicySource::kEnvironment);
  EXPECT_EQ(env.engine, DiskEvictionEngine::kFile);

  input.cli_engine = "";  // Existing engine setting treats empty as absent.
  EXPECT_EQ(ResolveDiskEvictionConfig(input).engine, DiskEvictionEngine::kFile);
}

TEST(DiskEvictionConfig, FileAcceptsDefaultAndExplicitLegacy) {
  DiskEvictionConfigInput input;
  input.env_engine = "file";
  const DiskEvictionConfigResult default_file =
      ResolveDiskEvictionConfig(input);
  EXPECT_EQ(default_file.status, Status::kOk);
  EXPECT_EQ(default_file.policy, DiskEvictionPolicy::kLifetime);
  EXPECT_EQ(default_file.source, DiskEvictionPolicySource::kDefault);
  EXPECT_EQ(default_file.engine, DiskEvictionEngine::kFile);
  input.env_policy = "legacy";
  const DiskEvictionConfigResult legacy_file = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(legacy_file.status, Status::kOk);
  EXPECT_EQ(legacy_file.policy, DiskEvictionPolicy::kLegacy);
  EXPECT_EQ(legacy_file.engine, DiskEvictionEngine::kFile);
}

TEST(DiskEvictionConfig, RejectsInvalidNamesAndLifetimeOnFile) {
  DiskEvictionConfigInput input;
  input.env_policy = "legacy";
  for (const std::string_view invalid : {"", "unknown"}) {
    SCOPED_TRACE(invalid);
    input.cli_policy = invalid;
    const DiskEvictionConfigResult result = ResolveDiskEvictionConfig(input);
    EXPECT_EQ(result.status, Status::kInvalid);
    EXPECT_NE(result.error.find("--slab-eviction-policy"), std::string::npos);
    EXPECT_NE(result.error.find(std::string(invalid)), std::string::npos);
  }
  input.cli_policy = "LiFeTiMe";
  const DiskEvictionConfigResult slab_lifetime =
      ResolveDiskEvictionConfig(input);
  EXPECT_EQ(slab_lifetime.status, Status::kOk);
  EXPECT_TRUE(slab_lifetime.error.empty());
  EXPECT_EQ(slab_lifetime.policy, DiskEvictionPolicy::kLifetime);
  EXPECT_EQ(slab_lifetime.engine, DiskEvictionEngine::kSlab);
  EXPECT_EQ(slab_lifetime.source, DiskEvictionPolicySource::kCommandLine);
  input.cli_engine = "file";
  const DiskEvictionConfigResult file_lifetime =
      ResolveDiskEvictionConfig(input);
  EXPECT_EQ(file_lifetime.status, Status::kInvalid);
  EXPECT_NE(file_lifetime.error.find("LiFeTiMe"), std::string::npos);
  EXPECT_NE(file_lifetime.error.find("slab"), std::string::npos);
  input.cli_policy.reset();
  input.env_policy = "";
  const DiskEvictionConfigResult empty_env = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(empty_env.status, Status::kInvalid);
  EXPECT_NE(empty_env.error.find("DFKV_SLAB_EVICTION_POLICY"),
            std::string::npos);
  input.env_policy = "legacy";
  input.cli_engine = "unknown";
  const DiskEvictionConfigResult bad_engine = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(bad_engine.status, Status::kInvalid);
  EXPECT_NE(bad_engine.error.find("--store-engine='unknown'"),
            std::string::npos);
}

TEST(DiskEvictionConfig, EngineErrorsIdentifySelectedSource) {
  DiskEvictionConfigInput input;
  input.env_engine = "bad-env";
  const DiskEvictionConfigResult env = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(env.status, Status::kInvalid);
  EXPECT_EQ(env.error, "DFKV_STORE_ENGINE='bad-env' must be 'file' or 'slab'");

  input.env_engine = "file";
  input.cli_engine = "bad-cli";
  const DiskEvictionConfigResult cli = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(cli.status, Status::kInvalid);
  EXPECT_EQ(cli.error, "--store-engine='bad-cli' must be 'file' or 'slab'");
}

TEST(DiskEvictionConfig, InactiveLifetimeValuesAndRamAckDoNotAffectLegacy) {
  const ScopedEnv lifetime_value("DFKV_SLAB_LIFE_MAX_READS", "invalid");
  const ScopedEnv ack_mode("DFKV_PUT_ACK_MODE", "ram");
  const ScopedEnv policy_env("DFKV_SLAB_EVICTION_POLICY", "lifetime");
  DiskEvictionConfigInput input;
  input.cli_policy = "legacy";
  const DiskEvictionConfigResult result = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(result.status, Status::kOk);

  input.cli_engine = "file";
  const DiskEvictionConfigResult file_result = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(file_result.status, Status::kOk);
  EXPECT_EQ(file_result.engine, DiskEvictionEngine::kFile);
  EXPECT_EQ(file_result.policy, DiskEvictionPolicy::kLegacy);
}

TEST(DiskEvictionConfig, EnvironmentLifetimeAcceptsAndCliCanOverride) {
  DiskEvictionConfigInput input;
  input.env_policy = "LIFETIME";
  const DiskEvictionConfigResult accepted = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(accepted.status, Status::kOk);
  EXPECT_TRUE(accepted.error.empty());
  EXPECT_EQ(accepted.policy, DiskEvictionPolicy::kLifetime);
  EXPECT_EQ(accepted.engine, DiskEvictionEngine::kSlab);
  EXPECT_EQ(accepted.source, DiskEvictionPolicySource::kEnvironment);

  input.env_engine = "file";
  const DiskEvictionConfigResult file_lifetime =
      ResolveDiskEvictionConfig(input);
  EXPECT_EQ(file_lifetime.status, Status::kInvalid);
  EXPECT_NE(file_lifetime.error.find("DFKV_SLAB_EVICTION_POLICY"),
            std::string::npos);
  EXPECT_NE(file_lifetime.error.find("slab"), std::string::npos);

  input.cli_policy = "legacy";
  const DiskEvictionConfigResult overridden = ResolveDiskEvictionConfig(input);
  EXPECT_EQ(overridden.status, Status::kOk);
  EXPECT_TRUE(overridden.error.empty());
  EXPECT_EQ(overridden.policy, DiskEvictionPolicy::kLegacy);
  EXPECT_EQ(overridden.source, DiskEvictionPolicySource::kCommandLine);
}

TEST(DiskEvictionConfig, EmptyInputDoesNotReadProcessEnvironment) {
  const ScopedEnv policy("DFKV_SLAB_EVICTION_POLICY", "unknown");
  const ScopedEnv engine("DFKV_STORE_ENGINE", "unknown");
  const DiskEvictionConfigResult result = ResolveDiskEvictionConfig({});
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_TRUE(result.error.empty());
  EXPECT_EQ(result.policy, DiskEvictionPolicy::kLifetime);
  EXPECT_EQ(result.source, DiskEvictionPolicySource::kDefault);
  EXPECT_EQ(result.engine, DiskEvictionEngine::kSlab);
  EXPECT_STREQ(std::getenv("DFKV_SLAB_EVICTION_POLICY"), "unknown");
  EXPECT_STREQ(std::getenv("DFKV_STORE_ENGINE"), "unknown");
}

class ProbeEngine final : public StoreEngine {
 public:
  ProbeEngine(std::string dir, size_t disk, std::shared_ptr<BatchProbe> probe)
      : dir_(std::move(dir)), disk_(disk), probe_(std::move(probe)) {}
  bool Healthy() const override { return true; }
  const std::string& StartupError() const override { return error_; }
  Status Cache(const BlockKey&, const void*, size_t) override {
    return Status::kOk;
  }
  Status CacheDirect(const BlockKey&, char*, size_t, size_t) override {
    return Status::kOk;
  }
  std::vector<Status> CacheDirectBatch(
      const std::vector<CacheBatchItem>& items) override {
    probe_->Enter(disk_);
    std::vector<Status> out;
    out.reserve(items.size());
    for (const auto& item : items)
      out.push_back((item.key.digest_lo & 1) ? Status::kInvalid : Status::kOk);
    return out;
  }
  Status Range(const BlockKey&, uint64_t, uint64_t, std::string*,
               size_t*) override {
    return Status::kNotFound;
  }
  Status RangeInto(const BlockKey&, uint64_t, uint64_t, char*, size_t,
                   size_t*, size_t*) override {
    return Status::kNotFound;
  }
  Status RangeDirect(const BlockKey&, uint64_t, uint64_t, char*, size_t,
                     const char**, size_t*, size_t*) override {
    return Status::kNotFound;
  }
  Status RangeDirectPrep(const BlockKey&, uint64_t, uint64_t, size_t,
                         ReadLease*) override {
    return Status::kNotFound;
  }
  bool IsCached(const BlockKey&) const override { return false; }
  Status Lookup(const BlockKey&, ValueMetadata*) const override {
    return Status::kNotFound;
  }
  Status Remove(const BlockKey&) override { return Status::kNotFound; }
  uint64_t UsedBytes() const override { return 0; }
  size_t Count() const override { return 0; }
  uint64_t Evictions() const override { return 0; }
  uint64_t EvictedBytes() const override { return 0; }
  const std::string& Dir() const override { return dir_; }

 private:
  std::string dir_;
  size_t disk_ = 0;
  std::string error_;
  std::shared_ptr<BatchProbe> probe_;
};
}  // namespace

class DiskGroupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    base_ = fs::temp_directory_path() /
            ("dfkv_dg_" + std::to_string(::testing::UnitTest::GetInstance()
                                             ->current_test_info()->line()));
    fs::remove_all(base_);
  }
  void TearDown() override { fs::remove_all(base_); }

  std::vector<std::string> Dirs(int n) {
    std::vector<std::string> v;
    for (int i = 0; i < n; ++i) {
      auto d = (base_ / ("disk" + std::to_string(i))).string();
      fs::create_directories(d);
      v.push_back(d);
    }
    return v;
  }
  size_t CountIn(const std::string& dir) {
    size_t c = 0;
    auto blocks = fs::path(dir) / "blocks";
    if (!fs::exists(blocks)) return 0;
    for (auto it = fs::recursive_directory_iterator(blocks);
         it != fs::recursive_directory_iterator(); ++it)
      if (it->is_regular_file()) ++c;
    return c;
  }
  fs::path base_;
};

TEST_F(DiskGroupTest, InvalidPolicyRejectsBeforeAnyDiskFactory) {
  for (const std::string engine : {"slab", "file"}) {
    for (const std::string policy : {"", "unknown", "LiFeTiMe"}) {
      SCOPED_TRACE(engine + ":" + policy);
      const ScopedEnv policy_env("DFKV_SLAB_EVICTION_POLICY", policy);
      size_t constructions = 0;
      DiskCacheGroup::Options options;
      options.cache_dirs = {(base_ / "a").string(), (base_ / "b").string()};
      options.capacity_bytes = 2ull << 30;
      options.engine = engine;
      options.engine_factory = [&constructions](const std::string& dir,
                                                uint64_t) {
        ++constructions;
        return std::make_unique<ProbeEngine>(dir, 0,
                                             std::make_shared<BatchProbe>());
      };
      const DiskCacheGroup group(options);
      EXPECT_FALSE(group.Healthy());
      EXPECT_EQ(group.DiskCount(), 0U);
      EXPECT_EQ(constructions, 0U);
      EXPECT_NE(group.StartupError().find("DFKV_SLAB_EVICTION_POLICY"),
                std::string::npos);
      EXPECT_NE(group.StartupError().find("='" + policy + "'"),
                std::string::npos);
      EXPECT_FALSE(fs::exists(base_));
    }
  }
}

TEST_F(DiskGroupTest, ExplicitLegacyOverridesPolicyEnvironment) {
  const ScopedEnv policy_env("DFKV_SLAB_EVICTION_POLICY", "lifetime");
  DiskCacheGroup::Options options;
  options.cache_dirs = {base_.string()};
  options.engine = "file";
  options.eviction_policy = "LEGACY";
  DiskCacheGroup group(options);
  ASSERT_TRUE(group.Healthy()) << group.StartupError();
  const BlockKey key{1, 0};
  const std::string value = "legacy bytes";
  ASSERT_EQ(group.Cache(key, value.data(), value.size()), Status::kOk);
  std::string actual;
  ASSERT_EQ(group.Range(key, 0, value.size(), &actual), Status::kOk);
  EXPECT_EQ(actual, value);
}

TEST_F(DiskGroupTest, ExplicitEmptyOrUnreadyPolicyNeverOpensDirectory) {
  const ScopedEnv policy_env("DFKV_SLAB_EVICTION_POLICY", "legacy");
  for (const std::string policy : {"", "lifetime"}) {
    SCOPED_TRACE(policy);
    DiskCacheGroup::Options options;
    options.cache_dirs = {base_.string()};
    options.engine = "slab";
    options.eviction_policy = policy;
    const DiskCacheGroup group(options);
    EXPECT_FALSE(group.Healthy());
    EXPECT_EQ(group.DiskCount(), 0U);
    EXPECT_NE(group.StartupError().find("--slab-eviction-policy"),
              std::string::npos);
    EXPECT_NE(group.StartupError().find("='" + policy + "'"),
              std::string::npos);
    EXPECT_FALSE(fs::exists(base_));
  }
}

TEST_F(DiskGroupTest, DefaultSlabStillStoresDataWithInactiveLifetimeNumbers) {
  const ScopedEnv policy_env("DFKV_SLAB_EVICTION_POLICY");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv lifetime_number("DFKV_SLAB_LIFE_MAX_READS", "invalid");
  DiskCacheGroup::Options options;
  options.cache_dirs = {base_.string()};
  options.engine = "slab";
  DiskCacheGroup group(options);
  ASSERT_TRUE(group.Healthy()) << group.StartupError();
  EXPECT_EQ(group.EngineName(), "slab");
  EXPECT_TRUE(fs::exists(base_ / "slots.tbl"));
  const BlockKey key{2, 0};
  const std::string value = "default legacy bytes";
  ASSERT_EQ(group.Cache(key, value.data(), value.size()), Status::kOk);
  std::string actual;
  ASSERT_EQ(group.Range(key, 0, value.size(), &actual), Status::kOk);
  EXPECT_EQ(actual, value);
}

TEST_F(DiskGroupTest, KvNodeServerRejectsPolicyBeforeStorageInitialization) {
  const ScopedEnv policy_env("DFKV_SLAB_EVICTION_POLICY", "lifetime");
  const ScopedEnv engine("DFKV_STORE_ENGINE", "slab");
  KvNodeServer server(base_.string(), 1ull << 30);
  EXPECT_FALSE(server.Healthy());
  EXPECT_EQ(server.Start(0), Status::kIOError);
  EXPECT_FALSE(fs::exists(base_));
}

TEST_F(DiskGroupTest, KvNodeServerExplicitOptionsOverridePolicyEnvironment) {
  const ScopedEnv policy_env("DFKV_SLAB_EVICTION_POLICY", "legacy");
  DiskCacheGroup::Options options{{base_.string()}, 1ull << 30, "slab"};
  options.eviction_policy = "lifetime";
  KvNodeServer server(options);
  EXPECT_FALSE(server.Healthy());
  EXPECT_EQ(server.Start(0), Status::kIOError);
  EXPECT_FALSE(fs::exists(base_));
}

TEST_F(DiskGroupTest, SpreadsBlocksAcrossAllDisks) {
  auto dirs = Dirs(3);
  DiskCacheGroup g({dirs, 1ull << 30, "file"});
  EXPECT_EQ(g.DiskCount(), 3u);
  std::string v(500, 'x');
  for (int i = 0; i < 300; ++i)
    ASSERT_EQ(g.Cache(BlockKey{(uint64_t)i, 0}, v.data(), v.size()), Status::kOk);
  for (auto& d : dirs) EXPECT_GT(CountIn(d), 0u) << d;  // every disk used
  EXPECT_EQ(g.Count(), 300u);
}

TEST_F(DiskGroupTest, PutGetRoundTripAcrossDisks) {
  auto dirs = Dirs(3);
  DiskCacheGroup g({dirs, 1ull << 30, "file"});
  for (int i = 0; i < 100; ++i) {
    std::string v = "val_" + std::to_string(i);
    ASSERT_EQ(g.Cache(BlockKey{(uint64_t)i, 0}, v.data(), v.size()), Status::kOk);
  }
  for (int i = 0; i < 100; ++i) {
    std::string out;
    ASSERT_EQ(g.Range(BlockKey{(uint64_t)i, 0}, 0, 64, &out), Status::kOk);
    EXPECT_EQ(out, "val_" + std::to_string(i));
  }
}

TEST_F(DiskGroupTest, DeterministicRoutingSameKeySameDisk) {
  auto dirs = Dirs(3);
  DiskCacheGroup g({dirs, 1ull << 30, "file"});
  std::string v(10, 'a');
  ASSERT_EQ(g.Cache(BlockKey{77, 0}, v.data(), v.size()), Status::kOk);
  ASSERT_EQ(g.Cache(BlockKey{77, 0}, v.data(), v.size()), Status::kOk);  // idempotent, same disk
  size_t total = 0;
  for (auto& d : dirs) total += CountIn(d);
  EXPECT_EQ(total, 1u);  // exactly one disk holds it, not duplicated
}

TEST_F(DiskGroupTest, MissReturnsNotFound) {
  auto dirs = Dirs(2);
  DiskCacheGroup g({dirs, 1ull << 30, "file"});
  std::string out;
  EXPECT_EQ(g.Range(BlockKey{404, 0}, 0, 8, &out), Status::kNotFound);
  EXPECT_FALSE(g.IsCached(BlockKey{404, 0}));
}

TEST_F(DiskGroupTest, PerDiskCapacityKeepsTotalBounded) {
  auto dirs = Dirs(3);
  // total cap split evenly -> ~ (cap/3) per disk; write far more than capacity
  const uint64_t cap = 3 * 20 * 1000;  // ~20 objs/disk of 1000B
  DiskCacheGroup g({dirs, cap, "file"});
  std::string v(1000, 'y');
  for (int i = 0; i < 500; ++i)
    ASSERT_EQ(g.Cache(BlockKey{(uint64_t)i, 0}, v.data(), v.size()), Status::kOk);
  EXPECT_LE(g.UsedBytes(), cap);  // LRU per disk keeps total under the cap
  EXPECT_GT(g.Count(), 0u);
}

TEST_F(DiskGroupTest, ReloadFromDisksRebuilds) {
  auto dirs = Dirs(3);
  {
    DiskCacheGroup g({dirs, 1ull << 30, "file"});
    std::string v = "persisted";
    ASSERT_EQ(g.Cache(BlockKey{55, 0}, v.data(), v.size()), Status::kOk);
  }
  DiskCacheGroup g2({dirs, 1ull << 30, "file"});  // same disks, fresh instance
  EXPECT_TRUE(g2.IsCached(BlockKey{55, 0}));
  std::string out;
  ASSERT_EQ(g2.Range(BlockKey{55, 0}, 0, 9, &out), Status::kOk);
  EXPECT_EQ(out, "persisted");
}

TEST_F(DiskGroupTest, ZeroLengthWritesNeverAllocateObjects) {
  auto dirs = Dirs(1);
  DiskCacheGroup group({dirs, 1ull << 30, "file"});
  ASSERT_TRUE(group.Healthy());
  char marker = 0;

  EXPECT_EQ(group.Cache(BlockKey{1, 0}, &marker, 0), Status::kInvalid);
  EXPECT_EQ(group.CacheDirect(BlockKey{2, 0}, &marker, 0, 1),
            Status::kInvalid);
  std::vector<StoreEngine::CacheBatchItem> items;
  for (uint64_t i = 0; i < 64; ++i)
    items.push_back({BlockKey{3 + i, 0}, &marker, 0, 1});
  const auto statuses = group.CacheDirectBatch(items);
  ASSERT_EQ(statuses.size(), items.size());
  for (Status status : statuses) EXPECT_EQ(status, Status::kInvalid);
  EXPECT_EQ(group.Count(), 0u);
  EXPECT_EQ(group.UsedBytes(), 0u);
}

TEST_F(DiskGroupTest, CacheDirectBatchOverlapsDisksAndPreservesOrder) {
  auto dirs = Dirs(3);
  auto probe = std::make_shared<BatchProbe>();
  probe->calls_by_disk.resize(dirs.size());
  DiskCacheGroup::Options options;
  options.cache_dirs = dirs;
  options.capacity_bytes = 1ull << 30;
  options.engine = "file";
  options.engine_factory =
      [probe, dirs](const std::string& dir, uint64_t) {
        const size_t disk =
            static_cast<size_t>(std::find(dirs.begin(), dirs.end(), dir) -
                                dirs.begin());
        return std::make_unique<ProbeEngine>(dir, disk, probe);
      };
  DiskCacheGroup group(std::move(options));
  ASSERT_TRUE(group.Healthy());

  char value = 'x';
  std::vector<StoreEngine::CacheBatchItem> items;
  items.reserve(256);
  for (uint64_t i = 0; i < 256; ++i)
    items.push_back({BlockKey{0, i}, &value, 1, 1});
  const std::vector<Status> statuses = group.CacheDirectBatch(items);

  ASSERT_EQ(statuses.size(), items.size());
  for (size_t i = 0; i < statuses.size(); ++i) {
    EXPECT_EQ(statuses[i], (i & 1) ? Status::kInvalid : Status::kOk)
        << "result must remain at original input index " << i;
  }
  EXPECT_EQ(probe->calls, group.DiskCount());
  for (size_t disk = 0; disk < probe->calls_by_disk.size(); ++disk)
    EXPECT_EQ(probe->calls_by_disk[disk], 1u) << "disk " << disk;
  EXPECT_GE(probe->max_active, 2u)
      << "at least two disk groups must execute concurrently";
}

TEST_F(DiskGroupTest, SingleDiskBatchRunsInline) {
  auto dirs = Dirs(1);
  auto probe = std::make_shared<BatchProbe>();
  probe->wait_for_overlap = false;
  probe->calls_by_disk.resize(1);
  DiskCacheGroup::Options options;
  options.cache_dirs = dirs;
  options.capacity_bytes = 1ull << 30;
  options.engine = "file";
  options.engine_factory =
      [probe](const std::string& dir, uint64_t) {
        return std::make_unique<ProbeEngine>(dir, 0, probe);
      };
  DiskCacheGroup group(std::move(options));
  char value = 'x';
  std::vector<StoreEngine::CacheBatchItem> items{
      {BlockKey{0, 2}, &value, 1, 1},
      {BlockKey{0, 3}, &value, 1, 1}};
  const std::thread::id caller = std::this_thread::get_id();
  const auto statuses = group.CacheDirectBatch(items);
  ASSERT_EQ(statuses.size(), 2u);
  ASSERT_EQ(probe->threads.size(), 1u);
  EXPECT_EQ(probe->threads[0], caller);
}

TEST_F(DiskGroupTest, QuotaAdmissionSupportsBatchAndRemovalReuse) {
  const auto dirs = Dirs(1);
  const fs::path quotas = base_ / "quotas";
  std::ofstream(quotas) << "# explicit tenant\n"
                        << "0000000000001111 10\n";
  ScopedEnv quota_file("DFKV_TENANT_QUOTAS_FILE", quotas.string());
  ScopedEnv default_quota("DFKV_TENANT_DEFAULT_QUOTA_BYTES", "10");
  DiskCacheGroup group({dirs, 1ull << 30, "file"});
  ASSERT_TRUE(group.Healthy()) << group.StartupError();

  void* raw = std::aligned_alloc(4096, 4096);
  ASSERT_NE(raw, nullptr);
  std::unique_ptr<void, decltype(&std::free)> buffer(raw, &std::free);
  std::memset(raw, 'q', 4096);
  std::vector<StoreEngine::CacheBatchItem> items{
      {BlockKey{1, 0, 0x1111}, static_cast<char*>(raw), 6, 4096},
      {BlockKey{2, 0, 0x1111}, static_cast<char*>(raw), 6, 4096},
      {BlockKey{3, 0, 0x2222}, static_cast<char*>(raw), 6, 4096}};
  const auto statuses = group.CacheDirectBatch(items);
  ASSERT_EQ(statuses.size(), 3u);
  EXPECT_EQ(statuses[0], Status::kOk);
  EXPECT_EQ(statuses[1], Status::kQuotaExceeded);
  EXPECT_EQ(statuses[2], Status::kOk);
  EXPECT_EQ(group.TenantUsedBytes(0x1111), 6u);
  EXPECT_EQ(group.TenantQuotaRejections(), 1u);

  // Existing keys never double-charge, and Remove makes capacity reusable
  // before it releases the same tenant stripe.
  EXPECT_EQ(group.Cache(BlockKey{1, 0, 0x1111}, "ignored", 7), Status::kOk);
  EXPECT_EQ(group.TenantUsedBytes(0x1111), 6u);
  ASSERT_EQ(group.Remove(BlockKey{1, 0, 0x1111}), Status::kOk);
  EXPECT_EQ(group.Cache(BlockKey{2, 0, 0x1111}, "1234567890", 10),
            Status::kOk);
  EXPECT_EQ(group.TenantUsedBytes(0x1111), 10u);

  const auto metrics = group.ConfiguredTenantQuotaMetrics();
  ASSERT_EQ(metrics.size(), 1u);
  EXPECT_EQ(metrics[0].tenant_hash, 0x1111u);
  EXPECT_EQ(metrics[0].limit_bytes, 10u);
  EXPECT_EQ(metrics[0].used_bytes, 10u);
  EXPECT_EQ(metrics[0].rejections, 1u);
}

TEST_F(DiskGroupTest, ConcurrentQuotaAdmissionCannotOversubscribe) {
  ScopedEnv default_quota("DFKV_TENANT_DEFAULT_QUOTA_BYTES", "10");
  ScopedEnv quota_file("DFKV_TENANT_QUOTAS_FILE", "");
  DiskCacheGroup group({Dirs(1), 1ull << 30, "file"});
  ASSERT_TRUE(group.Healthy()) << group.StartupError();
  std::array<Status, 2> statuses{Status::kInvalid, Status::kInvalid};
  std::thread first([&] {

    statuses[0] =
        group.Cache(BlockKey{1, 0, 0x3333}, "123456", 6);
  });
  std::thread second([&] {
    statuses[1] =
        group.Cache(BlockKey{2, 0, 0x3333}, "123456", 6);
  });
  first.join();
  second.join();
  EXPECT_EQ(std::count(statuses.begin(), statuses.end(), Status::kOk), 1);
  EXPECT_EQ(std::count(statuses.begin(), statuses.end(),
                       Status::kQuotaExceeded), 1);
  EXPECT_EQ(group.TenantUsedBytes(0x3333), 6u);
}
TEST_F(DiskGroupTest, StoreEvictionReleasesTenantQuotaImmediately) {
  ScopedEnv default_quota("DFKV_TENANT_DEFAULT_QUOTA_BYTES", "10");
  ScopedEnv quota_file("DFKV_TENANT_QUOTAS_FILE", "");
  DiskCacheGroup group({Dirs(1), 10, "file"});
  ASSERT_TRUE(group.Healthy()) << group.StartupError();
  ASSERT_EQ(group.Cache(BlockKey{1, 0, 0x1111}, "123456", 6),
            Status::kOk);
  ASSERT_EQ(group.Cache(BlockKey{2, 0, 0x2222}, "123456", 6),
            Status::kOk);
  EXPECT_EQ(group.TenantUsedBytes(0x1111), 0u);
  EXPECT_EQ(group.Cache(BlockKey{3, 0, 0x1111}, "1234567890", 10),
            Status::kOk);
  EXPECT_EQ(group.TenantUsedBytes(0x1111), 10u);
}

TEST_F(DiskGroupTest, ConfiguredQuotaFileFailsClosed) {
  const fs::path missing = base_ / "missing";
  {
    ScopedEnv quota_file("DFKV_TENANT_QUOTAS_FILE", missing.string());
    DiskCacheGroup group({Dirs(1), 1ull << 30, "file"});
    EXPECT_FALSE(group.Healthy());
    EXPECT_NE(group.StartupError().find("cannot open"), std::string::npos);
  }
  const fs::path malformed = base_ / "malformed";
  std::ofstream(malformed) << "ABCDEF0123456789 10\n";
  {
    ScopedEnv quota_file("DFKV_TENANT_QUOTAS_FILE", malformed.string());
    DiskCacheGroup group({Dirs(1), 1ull << 30, "file"});
    EXPECT_FALSE(group.Healthy());
    EXPECT_NE(group.StartupError().find("malformed"), std::string::npos);
  }
}
