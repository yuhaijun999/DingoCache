#include "cache/disk_write_result.h"

#include <gtest/gtest.h>
// POSIX setenv/unsetenv require their C header, not just the ISO C++ API.
#include <stdlib.h>  // NOLINT(hicpp-deprecated-headers,modernize-deprecated-headers)

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cache/disk_cache_group.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/kv_store.h"
#include "cache/ram_tier.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"

namespace fs = std::filesystem;
namespace dfkv {
class DiskSlabStoreTestPeer {
 public:
  static DiskResidentIdentity Identity(const DiskSlabStore& store,
                                       const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return store.ResidentIdentityLocked(key.Filename());
  }
  static void SetGenerationSequence(DiskSlabStore* store, uint64_t value) {
    const std::lock_guard<std::mutex> lock(store->mu_);
    store->resident_identity_->sequence = value;
  }
  static void SetPayloadWriteHook(DiskSlabStore* store,
                                  std::function<bool()> hook) {
    store->payload_write_hook_for_test_ = std::move(hook);
  }
  static void SetRecordWriteHook(DiskSlabStore* store,
                                 std::function<bool()> hook) {
    store->record_write_hook_for_test_ = std::move(hook);
  }
};
namespace {
// POSIX environment updates are confined to setup/teardown in this test
// process, before constructing group workers or after joining them.
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* previous = std::getenv(name)) previous_ = previous;
    if (value != nullptr)
      ::setenv(name, value, 1);
    else
      ::unsetenv(name);
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;
  ~ScopedEnv() {
    if (previous_)
      ::setenv(name_, previous_->c_str(), 1);
    else
      ::unsetenv(name_);
  }

 private:
  const char* name_;
  std::optional<std::string> previous_;
};

struct HookGate {
  std::mutex mutex;
  std::condition_variable cv;
  bool entered = false;
  bool released = false;
  bool result = false;
  bool Run() {
    std::unique_lock<std::mutex> lock(mutex);
    entered = true;
    cv.notify_all();
    cv.wait(lock, [&] { return released; });
    return result;
  }
  bool WaitEnteredFor() {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, std::chrono::seconds(5), [&] { return entered; });
  }
  void Release(bool success) {
    const std::lock_guard<std::mutex> lock(mutex);
    result = success;
    released = true;
    cv.notify_all();
  }
};

class DiskWriteResultTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        fs::temp_directory_path() /
        ("dfkv_write_result_" +
         std::string(
             ::testing::UnitTest::GetInstance()->current_test_info()->name()));
    fs::remove_all(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }
  static BlockKey K(uint64_t key) { return {key, 0}; }
  DiskSlabStore::Options Opts(uint64_t cap, uint64_t extent, uint64_t gran) {
    DiskSlabStore::Options options;
    options.dir = dir_.string();
    options.capacity_bytes = cap;
    options.extent_bytes = extent;
    options.slot_granularity = gran;
    return options;
  }
  DiskCacheGroup::Options GroupOptions(size_t count,
                                       std::vector<DiskSlabStore*>* stores) {
    DiskCacheGroup::Options options;
    for (size_t i = 0; i < count; ++i)
      options.cache_dirs.push_back((dir_ / std::to_string(i)).string());
    options.capacity_bytes = count * (1U << 20);
    options.engine = "slab";
    options.eviction_policy = "legacy";
    options.engine_factory = [stores](const std::string& path, uint64_t cap) {
      DiskSlabStore::Options slab;
      slab.dir = path;
      slab.capacity_bytes = cap;
      slab.extent_bytes = 1U << 20;
      slab.slot_granularity = 4096;
      slab.table_sync_ms = slab.reclaim_interval_ms = 0;
      slab.track_resident_identity = true;
      std::unique_ptr<DiskSlabStore> store =
          std::make_unique<DiskSlabStore>(slab);
      stores->push_back(store.get());
      return store;
    };
    return options;
  }
  fs::path dir_;
  ScopedEnv quota_file_{"DFKV_TENANT_QUOTAS_FILE", nullptr};
  ScopedEnv default_quota_{"DFKV_TENANT_DEFAULT_QUOTA_BYTES", nullptr};
};

TEST_F(DiskWriteResultTest,
       TypedWriteResultAllEntrypointsAndUntrackedFallback) {
  static_assert(std::is_same_v<std::underlying_type_t<dfkv::DiskWriteReason>,
                               unsigned char>);
  EXPECT_EQ(static_cast<unsigned char>(dfkv::DiskWriteReason::kNone), 0U);
  EXPECT_EQ(
      static_cast<unsigned char>(dfkv::DiskWriteReason::kIdentityExhausted),
      1U);
  for (const bool tracking : {false, true}) {
    SCOPED_TRACE(tracking);
    fs::remove_all(dir_);
    DiskSlabStore::Options options = Opts(8 * 4096, 8 * 4096, 4096);
    options.table_sync_ms = options.reclaim_interval_ms = 0;
    options.track_resident_identity = tracking;
    DiskSlabStore store(options);
    ASSERT_TRUE(store.Healthy());
    const dfkv::DiskWriteResult scalar =
        store.CacheWithResult(K(930), "first", 5);
    EXPECT_EQ(scalar.status, Status::kOk);
    EXPECT_EQ(scalar.reason, dfkv::DiskWriteReason::kNone);
    EXPECT_EQ(scalar.identity.valid(), tracking);
    alignas(4096) char first[4096] = "first";
    alignas(4096) char other[4096] = "other";
    const dfkv::DiskWriteResult direct =
        store.CacheDirectWithResult(K(931), first, 5, sizeof(first));
    EXPECT_EQ(direct.status, Status::kOk);
    EXPECT_EQ(direct.identity.valid(), tracking);
    const std::vector<dfkv::DiskWriteResult> batch =
        store.CacheDirectBatchWithResults({{K(932), first, 5, sizeof(first)},
                                           {K(932), other, 5, sizeof(other)},
                                           {K(933), nullptr, 1, 0}});
    ASSERT_EQ(batch.size(), 3U);
    EXPECT_EQ(batch[0].status, Status::kOk);
    EXPECT_EQ(batch[0].identity.valid(), tracking);
    EXPECT_EQ(batch[1].status, Status::kOk);
    EXPECT_FALSE(batch[1].identity.valid());
    EXPECT_EQ(batch[2].status, Status::kInvalid);
    EXPECT_FALSE(batch[2].identity.valid());
    for (const uint64_t key : {930U, 931U, 932U}) {
      const dfkv::DiskWriteResult duplicate =
          store.CacheWithResult(K(key), other, 5);
      EXPECT_EQ(duplicate.status, Status::kOk);
      EXPECT_EQ(duplicate.reason, dfkv::DiskWriteReason::kNone);
      EXPECT_FALSE(duplicate.identity.valid());
      std::string out;
      ASSERT_EQ(store.Range(K(key), 0, 0, &out), Status::kOk);
      EXPECT_EQ(out, "first");
    }
  }
}

TEST_F(DiskWriteResultTest,
       TypedWriteResultExhaustionIsAttachedToEachAdmission) {
  DiskSlabStore::Options options = Opts(4 * 4096, 4 * 4096, 4096);
  options.table_sync_ms = options.reclaim_interval_ms = 0;
  options.track_resident_identity = true;
  DiskSlabStore store(options);
  ASSERT_TRUE(store.Healthy());
  dfkv::DiskSlabStoreTestPeer::SetGenerationSequence(&store, UINT64_MAX - 1);
  alignas(4096) char value[4096] = "value";
  const std::vector<dfkv::DiskWriteResult> results =
      store.CacheDirectBatchWithResults({{K(934), value, 5, sizeof(value)},
                                         {K(935), value, 5, sizeof(value)},
                                         {K(934), value, 5, sizeof(value)},
                                         {K(936), nullptr, 1, 0}});
  ASSERT_EQ(results.size(), 4U);
  EXPECT_EQ(results[0].status, Status::kOk);
  EXPECT_EQ(results[0].reason, dfkv::DiskWriteReason::kNone);
  EXPECT_TRUE(results[0].identity.valid());
  EXPECT_EQ(results[0].identity.generation, UINT64_MAX);
  EXPECT_EQ(results[1].status, Status::kCacheFull);
  EXPECT_EQ(results[1].reason, dfkv::DiskWriteReason::kIdentityExhausted);
  EXPECT_FALSE(results[1].identity.valid());
  EXPECT_EQ(results[2].status, Status::kOk);
  EXPECT_EQ(results[2].reason, dfkv::DiskWriteReason::kNone);
  EXPECT_FALSE(results[2].identity.valid());
  EXPECT_EQ(results[3].status, Status::kInvalid);
  EXPECT_EQ(results[3].reason, dfkv::DiskWriteReason::kNone);
  const dfkv::DiskWriteResult scalar = store.CacheWithResult(K(937), value, 5);
  EXPECT_EQ(scalar.status, Status::kCacheFull);
  EXPECT_EQ(scalar.reason, dfkv::DiskWriteReason::kIdentityExhausted);
  const dfkv::DiskWriteResult direct =
      store.CacheDirectWithResult(K(938), value, 5, sizeof(value));
  EXPECT_EQ(direct.reason, dfkv::DiskWriteReason::kIdentityExhausted);
  EXPECT_EQ(store.GetStats().metadata_io_errors, 0U);
  EXPECT_TRUE(store.Healthy());
}

TEST_F(DiskWriteResultTest,
       TypedWriteResultDoesNotInferFailureFromStickyExhaustion) {
  for (const bool fail_payload : {false, true}) {
    SCOPED_TRACE(fail_payload);
    fs::remove_all(dir_);
    DiskSlabStore::Options options = Opts(2 * 4096, 2 * 4096, 4096);
    options.table_sync_ms = options.reclaim_interval_ms = 0;
    options.track_resident_identity = true;
    DiskSlabStore store(options);
    ASSERT_TRUE(store.Healthy());
    dfkv::DiskSlabStoreTestPeer::SetGenerationSequence(&store, UINT64_MAX - 1);
    HookGate gate;
    dfkv::DiskSlabStoreTestPeer::SetPayloadWriteHook(
        &store, [&] { return gate.Run(); });
    dfkv::DiskWriteResult original;
    std::thread writer(
        [&] { original = store.CacheWithResult(K(939), "x", 1); });
    if (!gate.WaitEnteredFor()) {
      gate.Release(false);
      writer.join();
      FAIL() << "writer did not reserve the final generation";
    }
    dfkv::DiskWriteResult rejected;
    std::promise<void> rejection_done;
    const std::future<void> rejection = rejection_done.get_future();
    std::thread other([&] {
      rejected = store.CacheWithResult(K(940), "y", 1);
      rejection_done.set_value();
    });
    EXPECT_EQ(rejection.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    // Release even a broken admission path before joining either writer.
    gate.Release(!fail_payload);
    writer.join();
    other.join();
    EXPECT_EQ(rejected.reason, dfkv::DiskWriteReason::kIdentityExhausted);
    EXPECT_EQ(original.status, fail_payload ? Status::kIOError : Status::kOk);
    EXPECT_EQ(original.reason, dfkv::DiskWriteReason::kNone);
    EXPECT_EQ(original.identity.valid(), !fail_payload);
    EXPECT_TRUE(store.Healthy());
  }
}

TEST_F(DiskWriteResultTest, TypedWriteResultRemovedCommitHasNoCredential) {
  DiskSlabStore::Options options = Opts(2 * 4096, 2 * 4096, 4096);
  options.table_sync_ms = options.reclaim_interval_ms = 0;
  options.track_resident_identity = true;
  DiskSlabStore store(options);
  ASSERT_TRUE(store.Healthy());
  HookGate gate;
  dfkv::DiskSlabStoreTestPeer::SetPayloadWriteHook(&store,
                                                   [&] { return gate.Run(); });
  dfkv::DiskWriteResult result;
  std::thread writer([&] { result = store.CacheWithResult(K(941), "x", 1); });
  if (!gate.WaitEnteredFor()) {
    gate.Release(false);
    writer.join();
    FAIL() << "writer did not enter payload gate";
  }
  EXPECT_EQ(store.Remove(K(941)), Status::kOk);
  gate.Release(true);
  writer.join();
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.reason, dfkv::DiskWriteReason::kNone);
  EXPECT_FALSE(result.identity.valid());
  EXPECT_FALSE(store.IsCached(K(941)));
}

TEST_F(DiskWriteResultTest, TypedWriteResultMetadataFailureRetainsIoStatus) {
  DiskSlabStore::Options options = Opts(4096, 4096, 4096);
  options.table_sync_ms = options.reclaim_interval_ms = 0;
  options.track_resident_identity = true;
  DiskSlabStore store(options);
  ASSERT_TRUE(store.Healthy());
  dfkv::DiskSlabStoreTestPeer::SetRecordWriteHook(&store, [] { return false; });
  const dfkv::DiskWriteResult result = store.CacheWithResult(K(942), "x", 1);
  EXPECT_EQ(result.status, Status::kIOError);
  EXPECT_EQ(result.reason, dfkv::DiskWriteReason::kNone);
  EXPECT_FALSE(result.identity.valid());
  EXPECT_FALSE(store.Healthy());
}

TEST_F(DiskWriteResultTest, GroupPreservesRealSlabResultsAcrossDisks) {
  std::vector<DiskSlabStore*> stores;
  DiskCacheGroup group(GroupOptions(3, &stores));
  ASSERT_TRUE(group.Healthy()) << group.StartupError();
  const DiskWriteResult scalar = group.CacheWithResult(K(1000), "first", 5);
  ASSERT_EQ(scalar.status, Status::kOk);
  ASSERT_TRUE(scalar.identity.valid());
  alignas(4096) char value[4096] = "first";
  const DiskWriteResult direct =
      group.CacheDirectWithResult(K(1001), value, 5, sizeof(value));
  ASSERT_EQ(direct.status, Status::kOk);
  EXPECT_TRUE(direct.identity.valid());
  std::vector<StoreEngine::CacheBatchItem> items;
  for (uint64_t key = 1100; key < 1228; ++key)
    items.push_back({K(key), value, 5, sizeof(value)});
  const std::vector<DiskWriteResult> results =
      group.CacheDirectBatchWithResults(items);
  ASSERT_EQ(results.size(), items.size());
  std::set<uint64_t> instances;
  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i].status, Status::kOk);
    EXPECT_EQ(results[i].reason, DiskWriteReason::kNone);
    EXPECT_TRUE(results[i].identity.valid());
    instances.insert(results[i].identity.store_instance);
    DiskResidentIdentity expected;
    for (const DiskSlabStore* store : stores) {
      if (store->IsCached(items[i].key))
        expected = DiskSlabStoreTestPeer::Identity(*store, items[i].key);
    }
    EXPECT_EQ(results[i].identity, expected) << i;
    std::string out;
    ASSERT_EQ(group.Range(items[i].key, 0, 0, &out), Status::kOk);
    EXPECT_EQ(out, "first");
  }
  EXPECT_EQ(instances.size(), 3U);
  const DiskWriteResult duplicate =
      group.CacheWithResult(K(1000), "different", 9);
  EXPECT_EQ(duplicate.status, Status::kOk);
  EXPECT_FALSE(duplicate.identity.valid());
  for (DiskSlabStore* store : stores)
    DiskSlabStoreTestPeer::SetGenerationSequence(store, UINT64_MAX);
  const DiskWriteResult exhausted = group.CacheWithResult(K(1300), value, 5);
  EXPECT_EQ(exhausted.status, Status::kCacheFull);
  EXPECT_EQ(exhausted.reason, DiskWriteReason::kIdentityExhausted);
  EXPECT_FALSE(exhausted.identity.valid());
  const std::vector<DiskWriteResult> mixed = group.CacheDirectBatchWithResults(
      {{K(1301), value, 5, sizeof(value)}, {K(1000), value, 5, sizeof(value)}});
  ASSERT_EQ(mixed.size(), 2U);
  EXPECT_EQ(mixed[0].reason, DiskWriteReason::kIdentityExhausted);
  EXPECT_EQ(mixed[1].status, Status::kOk);
  EXPECT_EQ(mixed[1].reason, DiskWriteReason::kNone);
  EXPECT_FALSE(mixed[1].identity.valid());
}

TEST_F(DiskWriteResultTest, GroupQuotaAndInvalidItemsNeverAcquireCredentials) {
  const ScopedEnv quota("DFKV_TENANT_DEFAULT_QUOTA_BYTES", "10");
  std::vector<DiskSlabStore*> stores;
  DiskCacheGroup group(GroupOptions(1, &stores));
  ASSERT_TRUE(group.Healthy());
  alignas(4096) char first[4096] = "123456";
  alignas(4096) char other[4096] = "different";
  const std::vector<DiskWriteResult> results =
      group.CacheDirectBatchWithResults({{K(1400), first, 6, sizeof(first)},
                                         {K(1401), first, 6, sizeof(first)},
                                         {K(1400), other, 9, sizeof(other)},
                                         {K(1402), nullptr, 1, 0}});
  ASSERT_EQ(results.size(), 4U);
  EXPECT_EQ(results[0].status, Status::kOk);
  EXPECT_TRUE(results[0].identity.valid());
  EXPECT_EQ(results[1].status, Status::kQuotaExceeded);
  EXPECT_EQ(results[2].status, Status::kOk);
  EXPECT_EQ(results[3].status, Status::kInvalid);
  for (size_t i = 1; i < results.size(); ++i) {
    EXPECT_EQ(results[i].reason, DiskWriteReason::kNone);
    EXPECT_FALSE(results[i].identity.valid());
  }
  const DiskWriteResult denied = group.CacheWithResult(K(1403), first, 6);
  EXPECT_EQ(denied.status, Status::kQuotaExceeded);
  EXPECT_FALSE(denied.identity.valid());
  const DiskWriteResult direct_denied =
      group.CacheDirectWithResult(K(1403), first, 6, sizeof(first));
  EXPECT_EQ(direct_denied.status, Status::kQuotaExceeded);
  EXPECT_EQ(group.TenantUsedBytes(0), 6U);
  std::string out;
  ASSERT_EQ(group.Range(K(1400), 0, 0, &out), Status::kOk);
  EXPECT_EQ(out, "123456");
}

struct ProbeCounts {
  std::atomic<unsigned> typed{0};
  std::atomic<unsigned> batches{0};
};

class LegacyBatchStore : public KVStore {
 public:
  LegacyBatchStore(const Options& options, std::shared_ptr<ProbeCounts> counts)
      : KVStore(options), counts_(std::move(counts)) {}
  std::vector<Status> CacheDirectBatch(
      const std::vector<CacheBatchItem>& items) override {
    counts_->batches.fetch_add(1, std::memory_order_relaxed);
    std::vector<Status> result;
    result.reserve(items.size());
    for (const CacheBatchItem& item : items) {
      result.push_back(
          (item.key.digest_hi & 1) != 0
              ? Status::kInvalid
              : KVStore::CacheDirect(item.key, item.data, item.len, item.cap));
    }
    return result;
  }

 private:
  std::shared_ptr<ProbeCounts> counts_;
};

class TypedCountingStore : public LegacyBatchStore {
 public:
  TypedCountingStore(const Options& options,
                     const std::shared_ptr<ProbeCounts>& counts)
      : LegacyBatchStore(options, counts), counts_(counts) {}
  DiskWriteResult CacheWithResult(const BlockKey& key, const void* data,
                                  size_t len) override {
    counts_->typed.fetch_add(1, std::memory_order_relaxed);
    return StoreEngine::CacheWithResult(key, data, len);
  }
  DiskWriteResult CacheDirectWithResult(const BlockKey& key, char* data,
                                        size_t len, size_t cap) override {
    counts_->typed.fetch_add(1, std::memory_order_relaxed);
    return StoreEngine::CacheDirectWithResult(key, data, len, cap);
  }
  std::vector<DiskWriteResult> CacheDirectBatchWithResults(
      const std::vector<CacheBatchItem>& items) override {
    counts_->typed.fetch_add(1, std::memory_order_relaxed);
    return StoreEngine::CacheDirectBatchWithResults(items);
  }

 private:
  std::shared_ptr<ProbeCounts> counts_;
};

TEST_F(DiskWriteResultTest, GroupOldInterfacesDoNotCallTypedMethods) {
  const std::shared_ptr<ProbeCounts> counts = std::make_shared<ProbeCounts>();
  DiskCacheGroup::Options options;
  options.cache_dirs = {dir_.string()};
  options.capacity_bytes = 1U << 20;
  options.engine = "file";
  options.eviction_policy = "legacy";
  options.engine_factory = [counts](const std::string& path, uint64_t cap) {
    return std::make_unique<TypedCountingStore>(KVStore::Options{path, cap},
                                                counts);
  };
  DiskCacheGroup group(options);
  ASSERT_TRUE(group.Healthy());
  alignas(4096) char value[4096] = "x";
  EXPECT_EQ(group.Cache(K(1500), value, 1), Status::kOk);
  EXPECT_EQ(group.CacheDirect(K(1502), value, 1, sizeof(value)), Status::kOk);
  EXPECT_EQ(group.CacheDirectBatch({{K(1504), value, 1, sizeof(value)}}),
            (std::vector<Status>{Status::kOk}));
  EXPECT_EQ(counts->typed.load(), 0U);
  EXPECT_EQ(group.CacheWithResult(K(1506), value, 1).status, Status::kOk);
  EXPECT_EQ(
      group.CacheDirectWithResult(K(1508), value, 1, sizeof(value)).status,
      Status::kOk);
  const std::vector<DiskWriteResult> results =
      group.CacheDirectBatchWithResults({{K(1510), value, 1, sizeof(value)},
                                         {K(1511), value, 1, sizeof(value)}});
  ASSERT_EQ(results.size(), 2U);
  EXPECT_EQ(results[0].status, Status::kOk);
  EXPECT_EQ(results[1].status,
            Status::kInvalid);  // Old batch, not scalar loop.
  EXPECT_EQ(counts->typed.load(), 3U);
  EXPECT_EQ(counts->batches.load(), 2U);
  for (const DiskWriteResult& result : results)
    EXPECT_FALSE(result.identity.valid());
}

TEST_F(DiskWriteResultTest, GroupFileBackendTypedResultsRemainUnbound) {
  DiskCacheGroup::Options options;
  options.cache_dirs = {dir_.string()};
  options.capacity_bytes = 1U << 20;
  options.engine = "file";
  options.eviction_policy = "legacy";
  DiskCacheGroup group(options);
  ASSERT_TRUE(group.Healthy());
  alignas(4096) char value[4096] = "x";
  EXPECT_EQ(group.CacheWithResult(K(1600), value, 1).status, Status::kOk);
  EXPECT_EQ(
      group.CacheDirectWithResult(K(1601), value, 1, sizeof(value)).status,
      Status::kOk);
  const std::vector<DiskWriteResult> results =
      group.CacheDirectBatchWithResults({{K(1602), value, 1, sizeof(value)}});
  ASSERT_EQ(results.size(), 1U);
  EXPECT_EQ(results[0].status, Status::kOk);
  EXPECT_EQ(results[0].reason, DiskWriteReason::kNone);
  EXPECT_FALSE(results[0].identity.valid());
  std::string out;
  ASSERT_EQ(group.Range(K(1602), 0, 0, &out), Status::kOk);
  EXPECT_EQ(out, "x");
}

enum class BrokenBatch : unsigned char { kShort = 0, kLong = 1, kThrow = 2 };
struct BrokenProbe {
  std::unordered_map<std::string, size_t> positions;
};
class BrokenBatchStore : public KVStore {
 public:
  BrokenBatchStore(const Options& options, BrokenBatch mode,
                   std::shared_ptr<BrokenProbe> probe)
      : KVStore(options), mode_(mode), probe_(std::move(probe)) {}
  std::vector<DiskWriteResult> CacheDirectBatchWithResults(
      const std::vector<CacheBatchItem>& items) override {
    for (size_t i = 0; i < items.size(); ++i)
      probe_->positions[items[i].key.Filename()] = i;
    if (mode_ == BrokenBatch::kThrow)
      throw std::runtime_error("injected engine failure");
    const size_t count = mode_ == BrokenBatch::kShort ? 1 : items.size() + 1;
    return std::vector<DiskWriteResult>(count, {Status::kCacheFull});
  }

 private:
  BrokenBatch mode_;
  std::shared_ptr<BrokenProbe> probe_;
};

TEST_F(DiskWriteResultTest,
       GroupMalformedBatchPreservesOtherDiskAndOriginalOrder) {
  for (const BrokenBatch mode :
       {BrokenBatch::kShort, BrokenBatch::kLong, BrokenBatch::kThrow}) {
    SCOPED_TRACE(static_cast<unsigned char>(mode));
    fs::remove_all(dir_);
    const std::shared_ptr<BrokenProbe> probe = std::make_shared<BrokenProbe>();
    const std::vector<std::string> dirs{(dir_ / "bad").string(),
                                        (dir_ / "good").string()};
    DiskCacheGroup::Options options;
    options.cache_dirs = dirs;
    options.capacity_bytes = 2U << 20;
    options.engine = "file";
    options.eviction_policy = "legacy";
    options.engine_factory = [probe, mode, dirs](
                                 const std::string& path,
                                 uint64_t cap) -> std::unique_ptr<StoreEngine> {
      if (path == dirs[0])
        return std::make_unique<BrokenBatchStore>(KVStore::Options{path, cap},
                                                  mode, probe);
      return std::make_unique<KVStore>(KVStore::Options{path, cap});
    };
    DiskCacheGroup group(options);
    ASSERT_TRUE(group.Healthy());
    alignas(4096) char value[4096] = "x";
    std::vector<StoreEngine::CacheBatchItem> items;
    for (uint64_t key = 1700; key < 1956; ++key)
      items.push_back({K(key), value, 1, sizeof(value)});
    std::atomic<size_t> failures{0};
    std::atomic<unsigned> observed_disks{0};
    const std::vector<DiskWriteResult> results =
        group.CacheDirectBatchWithResults(
            items, [&](size_t, size_t, uint64_t, size_t count, double) {
              failures.fetch_add(count, std::memory_order_relaxed);
              observed_disks.fetch_add(1, std::memory_order_relaxed);
            });
    ASSERT_EQ(results.size(), items.size());
    ASSERT_GT(probe->positions.size(), 1U);
    ASSERT_LT(probe->positions.size(), items.size());
    for (size_t i = 0; i < items.size(); ++i) {
      const std::unordered_map<std::string, size_t>::const_iterator bad =
          probe->positions.find(items[i].key.Filename());
      Status expected = Status::kOk;
      if (bad != probe->positions.end()) {
        expected = mode == BrokenBatch::kThrow ||
                           (mode == BrokenBatch::kShort && bad->second != 0)
                       ? Status::kIOError
                       : Status::kCacheFull;
      } else {
        std::string out;
        EXPECT_EQ(group.Range(items[i].key, 0, 0, &out), Status::kOk);
        EXPECT_EQ(out, "x");
      }
      EXPECT_EQ(results[i].status, expected) << i;
      EXPECT_EQ(results[i].reason, DiskWriteReason::kNone);
      EXPECT_FALSE(results[i].identity.valid());
    }
    EXPECT_EQ(failures.load(), probe->positions.size());
    EXPECT_EQ(observed_disks.load(), 2U);
  }
}

TEST_F(DiskWriteResultTest,
       RamCompletionPreservesRealGroupCommitAndExhaustion) {
  for (const bool batch : {false, true}) {
    SCOPED_TRACE(batch);
    fs::remove_all(dir_);
    std::vector<DiskSlabStore*> stores;
    DiskCacheGroup group(GroupOptions(1, &stores));
    ASSERT_TRUE(group.Healthy());
    RamTier::Options options;
    options.bytes = 64 * 4096;
    options.reclaim_interval_ms = 0;
    RamTier tier(options, nullptr);
    const RamTier::FlushResultFn single = [&](const BlockKey& key, char* data,
                                              size_t len, size_t cap) {
      return group.CacheDirectWithResult(key, data, len, cap);
    };
    RamTier::FlushBatchResultFn multiple;
    if (batch) {
      multiple = [&](const std::vector<RamTier::FlushItem>& items) {
        std::vector<StoreEngine::CacheBatchItem> writes;
        writes.reserve(items.size());
        for (const RamTier::FlushItem& item : items)
          writes.push_back({item.key, item.data, item.len, item.cap});
        return group.CacheDirectBatchWithResults(writes);
      };
    }
    tier.set_flush_results(single, multiple);
    const RamTier::PutResult committed = tier.PutCommitted(K(1800), "owner", 5);
    ASSERT_EQ(committed.status, Status::kOk);
    EXPECT_TRUE(committed.identity.valid());
    EXPECT_EQ(committed.identity,
              DiskSlabStoreTestPeer::Identity(*stores[0], K(1800)));
    const RamTier::PutResult duplicate =
        tier.PutCommitted(K(1800), "different", 9);
    EXPECT_EQ(duplicate.identity, committed.identity);
    std::string disk;
    ASSERT_EQ(group.Range(K(1800), 0, 0, &disk), Status::kOk);
    EXPECT_EQ(disk, "owner");
    RamTier::Hit hit;
    ASSERT_TRUE(tier.GetPrep(K(1800), 0, 5, &hit));
    EXPECT_EQ(std::string(hit.ptr, hit.len), disk);
    hit = RamTier::Hit{};
    DiskSlabStoreTestPeer::SetGenerationSequence(stores[0], UINT64_MAX);
    const RamTier::PutResult exhausted =
        tier.PutCommitted(K(1801), "blocked", 7);
    EXPECT_EQ(exhausted.status, Status::kCacheFull);
    EXPECT_EQ(exhausted.reason, DiskWriteReason::kIdentityExhausted);
    EXPECT_FALSE(exhausted.identity.valid());
    EXPECT_EQ(exhausted.disposition, RamTier::PutDisposition::kComplete);
    EXPECT_TRUE(tier.healthy());
    EXPECT_TRUE(group.Healthy());
    EXPECT_FALSE(tier.Contains(K(1801)));
    EXPECT_FALSE(group.IsCached(K(1801)));
  }
}

}  // namespace
}  // namespace dfkv
