#include <gtest/gtest.h>
// POSIX setenv/unsetenv have no std:: equivalent and require this header.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <initializer_list>
#include <ios>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cache/disk_cache_group.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/disk_write_result.h"
#include "cache/kv_node_server.h"
#include "cache/ram_tier.h"
#include "cache/slab_allocator.h"
#include "common/config_dump.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "slab_allocator_test_peer.h"
#include "transport/wire.h"

namespace dfkv {
class DiskCacheGroupTestPeer {
 public:
  static DiskSlabStore& Slab(DiskCacheGroup& group, size_t disk = 0) {
    return *static_cast<DiskSlabStore*>(group.disks_[disk].get());
  }
  static size_t Route(const DiskCacheGroup& group, const BlockKey& key) {
    return group.RouteIndex(key);
  }
};
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
  static void FailMetadata(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) close(store.table_fd_);
    store.table_fd_ = -1;
  }
  static size_t Flights(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.put_flights_.size();
  }
  static bool IndexesValid(const DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeIndexesValid(*store.alloc_);
  }
};
class RamTierTestPeer {
 public:
  using Completion = std::shared_ptr<RamTier::PutCompletion>;
  using Admission = RamTier::Admission;
  static Admission Admit(RamTier& tier, const BlockKey& key,
                         const std::string& value, Completion* completion) {
    return tier.Admit(key, value.data(), value.size(), false, completion);
  }
  static Completion Writing(RamTier& tier, const BlockKey& key) {
    RamTier::Shard& shard = tier.ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mu);
    return shard.writing.count(key) == 0 ? Completion{} : shard.writing.at(key);
  }
  static Completion Pending(RamTier& tier, const BlockKey& key) {
    RamTier::Shard& shard = tier.ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mu);
    if (shard.writing.count(key) != 0) return shard.writing.at(key);
    return shard.index.count(key) == 0 ? Completion{}
                                       : shard.index.at(key).completion;
  }
  static bool AwaitDone(const Completion& completion) {
    if (!completion) return false;
    std::unique_lock<std::mutex> lock(completion->mu);
    return completion->cv.wait_for(lock, std::chrono::seconds(5),
                                   [&] { return completion->done; });
  }
  static RamTier::PutResult Result(const Completion& completion) {
    return RamTier::WaitPutResult(completion);
  }
  static bool AwaitCanceledOrHidden(RamTier& tier, const BlockKey& key,
                                    const Completion& completion) {
    const std::chrono::steady_clock::time_point until =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < until) {
      {
        const std::lock_guard<std::mutex> lock(completion->mu);
        if (completion->canceled) return true;
      }
      if (!tier.Contains(key)) return true;
      std::this_thread::yield();
    }
    return false;
  }
  static void UseScalarSink(RamTier& tier) { tier.flush_batch_results_ = {}; }
  static void WrapBatch(
      RamTier& tier,
      std::function<void(const std::vector<RamTier::FlushItem>&)> before) {
    const RamTier::FlushBatchResultFn batch = tier.flush_batch_results_;
    tier.flush_batch_results_ =
        [batch, before = std::move(before)](
            const std::vector<RamTier::FlushItem>& items) {
          before(items);
          return batch(items);
        };
  }
};
class KvNodeServerWiringTestPeer {
 public:
  static DiskCacheGroup& Group(KvNodeServer& server) { return server.group_; }
  static const DiskCacheGroup& Group(const KvNodeServer& server) {
    return server.group_;
  }
  static RamTier& Ram(KvNodeServer& server) { return *server.ram_; }
  static bool AwaitSecondPut(const KvNodeServer& server) {
    const std::chrono::steady_clock::time_point until =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < until) {
      if (server.put_inflight_.load(std::memory_order_relaxed) >= 2)
        return true;
      std::this_thread::yield();
    }
    return false;
  }
};
namespace {
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* old = std::getenv(name)) old_ = old;
    if (value != nullptr)
      ::setenv(name, value, 1);
    else
      ::unsetenv(name);
  }
  ~ScopedEnv() {
    if (old_.has_value())
      ::setenv(name_.c_str(), old_->c_str(), 1);
    else
      ::unsetenv(name_.c_str());
  }

 private:
  std::string name_;
  std::optional<std::string> old_;
};

// This gate only pauses an existing unlocked Store/flush boundary. A release
// guard is declared after futures so assertions cannot strand a worker.
class Gate {
 public:
  void EnterOnce() {
    std::unique_lock<std::mutex> lock(mu_);
    if (entered_) return;
    entered_ = true;
    cv_.notify_all();
    cv_.wait(lock, [&] { return open_; });
  }
  bool AwaitEntered() {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, std::chrono::seconds(5),
                        [&] { return entered_; });
  }
  void Open() {
    const std::lock_guard<std::mutex> lock(mu_);
    open_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool entered_ = false;
  bool open_ = false;
};
struct OpenGateOnExit {
  Gate& gate;
  ~OpenGateOnExit() { gate.Open(); }
};

BlockKey Key(uint64_t id) { return {id, 0, 7}; }
using Snapshot = std::map<std::string, std::string>;
Snapshot MetadataSnapshot(const std::filesystem::path& root) {
  Snapshot result;
  for (const char* name :
       {"slots.tbl", "slab_meta", "slab_state", "slab_initializing"}) {
    const std::filesystem::path path = root / name;
    if (!std::filesystem::exists(path)) continue;
    std::ifstream input(path, std::ios::binary);
    result.emplace(name, std::string(std::istreambuf_iterator<char>(input),
                                     std::istreambuf_iterator<char>()));
  }
  return result;
}

class LifetimeWritebackTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        std::filesystem::temp_directory_path() /
        ("dfkv_lifetime_writeback_" +
         std::string(
             ::testing::UnitTest::GetInstance()->current_test_info()->name()));
    std::filesystem::remove_all(dir_);
  }
  void TearDown() override {
    node_.reset();
    std::filesystem::remove_all(dir_);
  }
  DiskCacheGroup::Options Options(size_t disks = 1) const {
    DiskCacheGroup::Options options;
    for (size_t disk = 0; disk < disks; ++disk)
      options.cache_dirs.push_back((dir_ / std::to_string(disk)).string());
    options.capacity_bytes = (1ULL << 30) * disks;
    options.engine = "slab";
    options.eviction_policy = "legacy";
    options.lifetime_config = DiskLifetimeConfig{};
    options.lifetime_class_eviction = true;
    return options;
  }
  void Create(size_t disks = 1) {
    node_ = std::make_unique<KvNodeServer>(Options(disks));
    ASSERT_TRUE(node_->Healthy());
    ASSERT_TRUE(node_->ram_enabled());
    ASSERT_FALSE(node_->ram_ack_enabled());
  }
  DiskCacheGroup& Group() { return KvNodeServerWiringTestPeer::Group(*node_); }
  RamTier& Ram() { return KvNodeServerWiringTestPeer::Ram(*node_); }
  DiskSlabStore& Slab(size_t disk = 0) {
    return DiskCacheGroupTestPeer::Slab(Group(), disk);
  }
  void FillPinned(DiskSlabStore& store) {
    for (uint64_t id = 1; id <= 2048; ++id) {
      ASSERT_EQ(store.Cache(Key(id), "old", 3), Status::kOk);
      ASSERT_TRUE(DiskSlabStoreTestPeer::Pin(store, Key(id)));
    }
    ASSERT_EQ(store.Count(), 2048U);
    ASSERT_TRUE(DiskSlabStoreTestPeer::IndexesValid(store));
  }
  void KeepChangingHeat(DiskSlabStore& store, Gate* gate = nullptr) {
    DiskSlabStoreTestPeer::YieldHook(store, [&store, gate] {
      std::string value;
      DiskResidentIdentity identity;
      EXPECT_EQ(store.RangeWithIdentity(Key(1), 0, 0, &value, &identity),
                Status::kOk);
      EXPECT_EQ(value, "old");
      EXPECT_TRUE(identity.valid());
      store.NotifyLifetimeReadSuccess(Key(1), identity, value.size());
      if (gate != nullptr) gate->EnterOnce();
    });
  }
  BlockKey RoutedKey(size_t disk, uint64_t start) {
    for (uint64_t id = start;; ++id) {
      if (DiskCacheGroupTestPeer::Route(Group(), Key(id)) == disk)
        return Key(id);
    }
  }
  Status Put(const BlockKey& key, bool direct) {
    char value[] = "new";
    if (direct) return node_->CacheDirectForKey(key, value, 3, sizeof(value));
    std::string ignored;
    return node_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                       key, 0, 0, value, 3, &ignored);
  }
  void ExpectBudgetClean(const BlockKey& key, uint64_t attempts) {
    EXPECT_TRUE(node_->Healthy());
    EXPECT_TRUE(Ram().healthy());
    EXPECT_FALSE(Ram().Contains(key));
    EXPECT_FALSE(RamTierTestPeer::Writing(Ram(), key));
    EXPECT_FALSE(Group().IsCached(key));
    EXPECT_EQ(Ram().DirtyObjects(), 0U);
    EXPECT_EQ(Ram().PostAckFlushFailures(), 0U);
    EXPECT_EQ(Ram().RamAcks(), 0U);
    EXPECT_EQ(Slab().GetStats().lifetime_scan_budget_exhausted, attempts);
    EXPECT_EQ(Slab().GetStats().metadata_io_errors, 0U);
    EXPECT_EQ(DiskSlabStoreTestPeer::Flights(Slab()), 0U);
    EXPECT_TRUE(DiskSlabStoreTestPeer::IndexesValid(Slab()));
  }
  const ScopedEnv ram_on_{"DFKV_RAM_TIER", "1"};
  const ScopedEnv ram_bytes_{"DFKV_RAM_TIER_BYTES", "8388608"};
  const ScopedEnv ram_extent_{"DFKV_RAM_TIER_EXTENT_BYTES", "1048576"};
  const ScopedEnv reserve_{"DFKV_RAM_TIER_LARGE_RESERVE_BYTES", "0"};
  const ScopedEnv shards_{"DFKV_RAM_TIER_SHARDS", "1"};
  const ScopedEnv workers_{"DFKV_RAM_FLUSH_THREADS", "1"};
  const ScopedEnv admission_{"DFKV_PUT_INFLIGHT_LIMIT", "8"};
  const ScopedEnv writeback_{"DFKV_RAM_WRITE_MODE", "writeback"};
  const ScopedEnv ack_{"DFKV_PUT_ACK_MODE", "disk"};
  const ScopedEnv buffered_{"DFKV_SLAB_WRITE", "buffered"};
  const ScopedEnv granularity_{"DFKV_SLAB_GRANULARITY", "524288"};
  const ScopedEnv reclaim_{"DFKV_SLAB_RECLAIM_MS", "0"};
  const ScopedEnv ram_reclaim_{"DFKV_RAM_RECLAIM_MS", "0"};
  const ScopedEnv sync_{"DFKV_SLAB_TABLE_SYNC_MS", "0"};
  std::filesystem::path dir_;
  std::unique_ptr<KvNodeServer> node_;
};

TEST_F(LifetimeWritebackTest, MissingOrRamAckRejectsBeforeCreatingDirectories) {
  for (const char* enabled : {"1", "on", "true", "yes"}) {
    const ScopedEnv ram("DFKV_RAM_TIER", enabled);
    SCOPED_TRACE(enabled);
    for (const char* ack : std::initializer_list<const char*>{
             nullptr, "", "ram", "invalid", "DISK"}) {
      SCOPED_TRACE(ack == nullptr ? "unset" : ack);
      const ScopedEnv selected("DFKV_PUT_ACK_MODE", ack);
      KvNodeServer node(Options(2));
      EXPECT_FALSE(node.Healthy());
      EXPECT_FALSE(node.ram_enabled());
      EXPECT_EQ(node.Start(0), Status::kIOError);
      EXPECT_FALSE(std::filesystem::exists(dir_));
      EXPECT_NE(KvNodeServerWiringTestPeer::Group(node).StartupError().find(
                    "DFKV_PUT_ACK_MODE"),
                std::string::npos);
    }
  }
}

TEST_F(LifetimeWritebackTest, DefaultAndUnknownWriteModeCannotBypassAckGate) {
  const ScopedEnv ack("DFKV_PUT_ACK_MODE", nullptr);
  for (const char* mode : std::initializer_list<const char*>{
           nullptr, "", "writeback", "invalid"}) {
    const ScopedEnv selected("DFKV_RAM_WRITE_MODE", mode);
    const KvNodeServer node(Options());
    EXPECT_FALSE(node.Healthy());
    EXPECT_FALSE(std::filesystem::exists(dir_));
  }
}

TEST_F(LifetimeWritebackTest, InvalidActiveModesAreNotSilentlyNormalized) {
  {
    const ScopedEnv mode("DFKV_RAM_WRITE_MODE", "invalid");
    const KvNodeServer rejected(Options());
    EXPECT_FALSE(rejected.Healthy());
    EXPECT_NE(KvNodeServerWiringTestPeer::Group(rejected).StartupError().find(
                  "DFKV_RAM_WRITE_MODE"),
              std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(dir_));
  }
  {
    const ScopedEnv mode("DFKV_RAM_WRITE_MODE", "writearound");
    const ScopedEnv ack("DFKV_PUT_ACK_MODE", "invalid");
    const KvNodeServer rejected(Options());
    EXPECT_FALSE(rejected.Healthy());
    EXPECT_NE(KvNodeServerWiringTestPeer::Group(rejected).StartupError().find(
                  "DFKV_PUT_ACK_MODE"),
              std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(dir_));
  }
}

TEST_F(LifetimeWritebackTest, RejectionPreservesCleanAndDirtyDiskMetadata) {
  const DiskCacheGroup::Options options = Options(2);
  DiskSlabStore::Options store_options;
  store_options.dir = options.cache_dirs[0];
  store_options.capacity_bytes = 1ULL << 30;
  store_options.slot_granularity = 524288;
  store_options.reclaim_interval_ms = 0;
  store_options.table_sync_ms = 0;
  {
    DiskSlabStore clean(store_options);
    ASSERT_TRUE(clean.Healthy());
    ASSERT_EQ(clean.Cache(Key(1), "clean", 5), Status::kOk);
  }
  store_options.dir = options.cache_dirs[1];
  DiskSlabStore dirty(store_options);
  ASSERT_TRUE(dirty.Healthy());
  ASSERT_EQ(dirty.Cache(Key(2), "dirty", 5), Status::kOk);
  const Snapshot clean_before = MetadataSnapshot(options.cache_dirs[0]);
  const Snapshot dirty_before = MetadataSnapshot(options.cache_dirs[1]);
  ASSERT_EQ(clean_before.count("slots.tbl"), 1U);
  ASSERT_EQ(dirty_before.count("slab_state"), 1U);
  EXPECT_NE(clean_before.at("slab_state"), dirty_before.at("slab_state"));
  const ScopedEnv ack("DFKV_PUT_ACK_MODE", "ram");
  const KvNodeServer rejected(options);
  EXPECT_FALSE(rejected.Healthy());
  EXPECT_EQ(MetadataSnapshot(options.cache_dirs[0]), clean_before);
  EXPECT_EQ(MetadataSnapshot(options.cache_dirs[1]), dirty_before);
}

TEST_F(LifetimeWritebackTest, DiskAckAcceptsAndPublicLifetimeRemainsClosed) {
  ASSERT_NO_FATAL_FAILURE(Create());
  EXPECT_EQ(Put(Key(9000), false), Status::kOk);
  EXPECT_EQ(Ram().RamAcks(), 0U);
  node_.reset();
  const Snapshot before = MetadataSnapshot(Options().cache_dirs[0]);
  DiskCacheGroup::Options requested = Options();
  requested.eviction_policy = "lifetime";
  const KvNodeServer rejected(requested);
  EXPECT_FALSE(rejected.Healthy());
  EXPECT_EQ(MetadataSnapshot(requested.cache_dirs[0]), before);
}

TEST_F(LifetimeWritebackTest, InternalRunIsNotReportedAsLegacyBaseline) {
  config_dump::ResetForTest();
  ASSERT_NO_FATAL_FAILURE(Create());
  testing::internal::CaptureStderr();
  config_dump::Emit("internal-lifetime-writeback-test");
  const std::string output = testing::internal::GetCapturedStderr();
  const size_t effective = output.find("disk_eviction_effective");
  ASSERT_NE(effective, std::string::npos);
  EXPECT_NE(output.substr(effective, output.find('\n', effective) - effective)
                .find("internal_test_only"),
            std::string::npos);
  const size_t internal = output.find("disk_eviction_internal_test");
  ASSERT_NE(internal, std::string::npos);
  EXPECT_NE(output.substr(internal, output.find('\n', internal) - internal)
                .find("lifetime"),
            std::string::npos);
}

TEST_F(LifetimeWritebackTest, LegacyAndShadowRetainRamAckDefault) {
  const ScopedEnv ack("DFKV_PUT_ACK_MODE", nullptr);
  for (const bool shadow : {false, true}) {
    DiskCacheGroup::Options options = Options();
    options.lifetime_class_eviction = false;
    if (!shadow) options.lifetime_config.reset();
    const KvNodeServer node(options);
    ASSERT_TRUE(node.Healthy());
    EXPECT_TRUE(node.ram_ack_enabled());
    EXPECT_TRUE(node.ram_enabled());
  }
}

TEST_F(LifetimeWritebackTest, RamOffAndWriteAroundDoNotRequireDiskAck) {
  const ScopedEnv ack("DFKV_PUT_ACK_MODE", nullptr);
  {
    const ScopedEnv disabled("DFKV_RAM_TIER", "off");
    const KvNodeServer node(Options());
    EXPECT_TRUE(node.Healthy());
    EXPECT_FALSE(node.ram_enabled());
  }
  {
    const ScopedEnv around("DFKV_RAM_WRITE_MODE", "writearound");
    const KvNodeServer node(Options());
    EXPECT_TRUE(node.Healthy());
    EXPECT_TRUE(node.ram_enabled());
  }
}

TEST_F(LifetimeWritebackTest, InternalExecutionRequiresScoringAndRealSlabs) {
  DiskCacheGroup::Options options = Options();
  options.lifetime_config.reset();
  {
    const KvNodeServer node(options);
    EXPECT_FALSE(node.Healthy());
    EXPECT_FALSE(std::filesystem::exists(dir_));
  }
  options = Options();
  options.engine = "file";
  const KvNodeServer node(options);
  EXPECT_FALSE(node.Healthy());
  EXPECT_FALSE(std::filesystem::exists(dir_));
}

TEST_F(LifetimeWritebackTest, ScalarAndBatchSinksTerminateNodeAndRamFollowers) {
  for (const bool direct : {false, true}) {
    SCOPED_TRACE(direct);
    ASSERT_NO_FATAL_FAILURE(Create());
    if (!direct) RamTierTestPeer::UseScalarSink(Ram());
    ASSERT_NO_FATAL_FAILURE(FillPinned(Slab()));
    Gate gate;
    std::future<Status> owner;
    std::future<Status> foreground_waiter;
    const OpenGateOnExit release{gate};
    KeepChangingHeat(Slab(), &gate);
    const BlockKey key = Key(9001);
    owner = std::async(std::launch::async,
                       [this, key, direct] { return Put(key, direct); });
    ASSERT_TRUE(gate.AwaitEntered());
    const RamTierTestPeer::Completion first =
        RamTierTestPeer::Pending(Ram(), key);
    ASSERT_TRUE(first);
    RamTierTestPeer::Completion follower;
    ASSERT_EQ(RamTierTestPeer::Admit(Ram(), key, "different", &follower),
              RamTierTestPeer::Admission::kDuplicate);
    ASSERT_EQ(first, follower);
    // The Node's foreground key lock serializes this waiter. It becomes a
    // distinct logical owner, with its own three retries, after the first ends.
    foreground_waiter = std::async(
        std::launch::async, [this, key, direct] { return Put(key, direct); });
    ASSERT_TRUE(KvNodeServerWiringTestPeer::AwaitSecondPut(*node_));
    gate.Open();
    ASSERT_EQ(owner.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(owner.get(), Status::kCacheFull);
    ASSERT_EQ(foreground_waiter.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(foreground_waiter.get(), Status::kCacheFull);
    ASSERT_TRUE(RamTierTestPeer::AwaitDone(follower));
    const RamTier::PutResult result = RamTierTestPeer::Result(follower);
    EXPECT_EQ(result.status, Status::kCacheFull);
    EXPECT_EQ(result.reason, DiskWriteReason::kScanBudgetExhausted);
    EXPECT_EQ(result.disposition, RamTier::PutDisposition::kComplete);
    ExpectBudgetClean(key, 6);
    EXPECT_EQ(Slab().Count(), 2048U);
    EXPECT_LE(Slab().GetStats().lifetime_class_scan_visits, 6 * 4098U);
    // A new logical attempt gets its own finite retries, not a stale owner.
    EXPECT_EQ(Put(key, direct), Status::kCacheFull);
    ExpectBudgetClean(key, 9);
    EXPECT_EQ(Ram().FlushDropped(), 3U);
    DiskSlabStoreTestPeer::YieldHook(Slab(), {});
    ASSERT_TRUE(DiskSlabStoreTestPeer::Unpin(Slab(), Key(2)));
    EXPECT_EQ(Put(key, direct), Status::kOk);
    EXPECT_TRUE(Group().IsCached(key));
    std::string stored;
    EXPECT_EQ(Group().Range(key, 0, 0, &stored), Status::kOk);
    EXPECT_EQ(stored, "new");
    node_.reset();
    std::filesystem::remove_all(dir_);
  }
}

TEST_F(LifetimeWritebackTest, DirectBatchPreservesEachBudgetFailure) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(FillPinned(Slab()));
  KeepChangingHeat(Slab());
  char first[] = "first";
  char second[] = "second";
  const std::vector<Status> results =
      node_->CacheDirectBatchForKeys({{Key(9002), first, 5, sizeof(first)},
                                      {Key(9003), second, 6, sizeof(second)}});
  EXPECT_EQ(results,
            (std::vector<Status>{Status::kCacheFull, Status::kCacheFull}));
  ExpectBudgetClean(Key(9002), 2);
  EXPECT_FALSE(Group().IsCached(Key(9003)));
  EXPECT_EQ(Ram().FlushDropped(),
            0U);  // This frontend batches directly to disk.
}

TEST_F(LifetimeWritebackTest, CancelingPendingRamOwnerCompletesItsFollowers) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(FillPinned(Slab()));
  Gate gate;
  std::future<Status> owner;
  std::future<bool> removed;
  const OpenGateOnExit release{gate};
  KeepChangingHeat(Slab(), &gate);
  const BlockKey key = Key(9004);
  owner =
      std::async(std::launch::async, [this, key] { return Put(key, true); });
  ASSERT_TRUE(gate.AwaitEntered());
  const RamTierTestPeer::Completion completion =
      RamTierTestPeer::Pending(Ram(), key);
  ASSERT_TRUE(completion);
  // Remove hides/cancels the RAM owner while the real Store attempt is paused.
  removed =
      std::async(std::launch::async, [this, key] { return Ram().Remove(key); });
  ASSERT_TRUE(RamTierTestPeer::AwaitCanceledOrHidden(Ram(), key, completion));
  gate.Open();
  ASSERT_EQ(removed.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_TRUE(removed.get());
  ASSERT_EQ(owner.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_NE(owner.get(), Status::kOk);
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(completion));
  EXPECT_NE(RamTierTestPeer::Result(completion).status, Status::kOk);
  EXPECT_FALSE(Ram().Contains(key));
  EXPECT_FALSE(RamTierTestPeer::Writing(Ram(), key));
  EXPECT_TRUE(Ram().healthy());
  EXPECT_EQ(Ram().DirtyObjects(), 0U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(Slab()), 0U);
}

TEST_F(LifetimeWritebackTest, StopWaitsForAdmittedBudgetCompletion) {
  ASSERT_NO_FATAL_FAILURE(Create());
  ASSERT_NO_FATAL_FAILURE(FillPinned(Slab()));
  Gate gate;
  std::future<Status> owner;
  std::future<void> stopped;
  const OpenGateOnExit release{gate};
  KeepChangingHeat(Slab(), &gate);
  const BlockKey key = Key(9005);
  owner =
      std::async(std::launch::async, [this, key] { return Put(key, true); });
  ASSERT_TRUE(gate.AwaitEntered());
  ASSERT_EQ(node_->Start(0), Status::kOk);
  stopped = std::async(std::launch::async, [&] { node_->Stop(); });
  EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  gate.Open();
  ASSERT_EQ(owner.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(owner.get(), Status::kCacheFull);
  ASSERT_EQ(stopped.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  stopped.get();
  EXPECT_EQ(Ram().DirtyObjects(), 0U);
  EXPECT_EQ(Ram().PostAckFlushFailures(), 0U);
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(Slab()), 0U);
}

TEST_F(LifetimeWritebackTest, RealMixedFlushKeepsSuccessBudgetAndIoSeparate) {
  ASSERT_NO_FATAL_FAILURE(Create(3));
  ASSERT_NO_FATAL_FAILURE(FillPinned(Slab(0)));
  KeepChangingHeat(Slab(0));
  const BlockKey seed = RoutedKey(2, 10000);
  const BlockKey budget = RoutedKey(0, 20000);
  const BlockKey io = RoutedKey(1, 30000);
  const BlockKey success = RoutedKey(2, 40000);
  ASSERT_EQ(DiskCacheGroupTestPeer::Route(Group(), seed), 2U);
  ASSERT_EQ(DiskCacheGroupTestPeer::Route(Group(), budget), 0U);
  ASSERT_EQ(DiskCacheGroupTestPeer::Route(Group(), io), 1U);
  ASSERT_EQ(DiskCacheGroupTestPeer::Route(Group(), success), 2U);
  const std::shared_ptr<Gate> gate = std::make_shared<Gate>();
  const OpenGateOnExit release{*gate};
  const std::shared_ptr<std::vector<size_t>> batch_sizes =
      std::make_shared<std::vector<size_t>>();
  RamTierTestPeer::WrapBatch(
      Ram(), [gate, batch_sizes](const std::vector<RamTier::FlushItem>& items) {
        batch_sizes->push_back(items.size());
        gate->EnterOnce();
      });
  RamTierTestPeer::Completion seed_completion;
  ASSERT_EQ(RamTierTestPeer::Admit(Ram(), seed, "seed", &seed_completion),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(gate->AwaitEntered());
  RamTierTestPeer::Completion budget_completion;
  RamTierTestPeer::Completion io_completion;
  RamTierTestPeer::Completion success_completion;
  ASSERT_EQ(RamTierTestPeer::Admit(Ram(), budget, "budget", &budget_completion),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_EQ(RamTierTestPeer::Admit(Ram(), io, "io", &io_completion),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_EQ(
      RamTierTestPeer::Admit(Ram(), success, "success", &success_completion),
      RamTierTestPeer::Admission::kAccepted);
  DiskSlabStoreTestPeer::FailMetadata(Slab(1));
  gate->Open();
  for (const RamTierTestPeer::Completion& completion :
       {seed_completion, budget_completion, io_completion, success_completion})
    ASSERT_TRUE(RamTierTestPeer::AwaitDone(completion));
  const RamTier::PutResult budget_result =
      RamTierTestPeer::Result(budget_completion);
  const RamTier::PutResult io_result = RamTierTestPeer::Result(io_completion);
  const RamTier::PutResult success_result =
      RamTierTestPeer::Result(success_completion);
  EXPECT_EQ(budget_result.status, Status::kCacheFull);
  EXPECT_EQ(budget_result.reason, DiskWriteReason::kScanBudgetExhausted);
  EXPECT_EQ(io_result.status, Status::kIOError);
  EXPECT_EQ(io_result.reason, DiskWriteReason::kNone);
  EXPECT_EQ(success_result.status, Status::kOk);
  EXPECT_EQ(success_result.outcome, DiskWriteOutcome::kNew);
  EXPECT_EQ(*batch_sizes, (std::vector<size_t>{1, 3, 2, 2}));
  EXPECT_EQ(Slab(0).GetStats().lifetime_scan_budget_exhausted, 3U);
  EXPECT_EQ(Slab(1).GetStats().metadata_io_errors, 1U);
  EXPECT_FALSE(Ram().healthy());  // The actual metadata I/O error isolates RAM.
  EXPECT_FALSE(node_->Healthy());
  EXPECT_FALSE(Ram().Contains(budget));
  EXPECT_FALSE(Ram().Contains(io));
  EXPECT_TRUE(Ram().Contains(success));
  EXPECT_EQ(Ram().DirtyObjects(), 0U);
  EXPECT_EQ(Ram().PostAckFlushFailures(), 0U);
  std::string value;
  EXPECT_EQ(Slab(2).Range(success, 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, "success");
  EXPECT_EQ(DiskSlabStoreTestPeer::Flights(Slab(0)), 0U);
}
}  // namespace
}  // namespace dfkv
