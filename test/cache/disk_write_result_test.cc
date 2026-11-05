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
    return store.ResidentIdentityLocked(key, key.Filename());
  }
  static bool HasPutFollower(const DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    const std::unordered_map<
        std::string, std::shared_ptr<DiskSlabStore::PutFlight>>::const_iterator
        it = store.put_flights_.find(key.Filename());
    // Map + gated owner hold two references; a third proves attachment.
    return it != store.put_flights_.end() && it->second.use_count() >= 3;
  }
  static void SetGenerationSequence(DiskSlabStore* store, uint64_t value) {
    const std::lock_guard<std::mutex> lock(store->mu_);
    store->resident_identity_->sequence = value;
  }
  static void SetReadHook(DiskSlabStore* store, std::function<void()> hook) {
    store->read_acquired_hook_for_test_ = std::move(hook);
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

enum class ReadEntry : unsigned char { kString = 0, kInto = 1, kDirect = 2 };

Status ReadSource(DiskCacheGroup& group, ReadEntry entry, const BlockKey& key,
                  uint64_t offset, uint64_t length, std::string* out,
                  DiskResidentIdentity* identity, size_t cap = 4096,
                  size_t* value_len = nullptr) {
  if (entry == ReadEntry::kString)
    return group.RangeWithIdentity(key, offset, length, out, identity,
                                   value_len);
  alignas(4096) char buffer[4096] = {};
  const char* data = buffer;
  size_t got = 0;
  const Status status =
      entry == ReadEntry::kInto
          ? group.RangeIntoWithIdentity(key, offset, length, buffer, cap, &got,
                                        identity, value_len)
          : group.RangeDirectWithIdentity(key, offset, length, buffer, cap,
                                          &data, &got, identity, value_len);
  if (status == Status::kOk) out->assign(data, got);
  return status;
}

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
                                       std::vector<DiskSlabStore*>* stores,
                                       bool tracking = true,
                                       bool direct = false) {
    DiskCacheGroup::Options options;
    for (size_t i = 0; i < count; ++i)
      options.cache_dirs.push_back((dir_ / std::to_string(i)).string());
    options.capacity_bytes = count * (1U << 20);
    options.engine = "slab";
    options.eviction_policy = "legacy";
    options.engine_factory = [stores, tracking, direct](const std::string& path,
                                                        uint64_t cap) {
      DiskSlabStore::Options slab;
      slab.dir = path;
      slab.capacity_bytes = cap;
      slab.extent_bytes = 1U << 20;
      slab.slot_granularity = 4096;
      slab.table_sync_ms = slab.reclaim_interval_ms = 0;
      slab.track_resident_identity = tracking;
      slab.direct_writes = direct;
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
  static_assert(
      std::is_same_v<std::underlying_type_t<DiskWriteOutcome>, unsigned char>);
  EXPECT_EQ(static_cast<unsigned char>(DiskWriteOutcome::kUnknown), 0U);
  EXPECT_EQ(static_cast<unsigned char>(DiskWriteOutcome::kNew), 1U);
  EXPECT_EQ(static_cast<unsigned char>(DiskWriteOutcome::kKeptExisting), 2U);
  EXPECT_EQ(static_cast<unsigned char>(DiskWriteOutcome::kRemoved), 3U);
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
    EXPECT_EQ(scalar.outcome, DiskWriteOutcome::kNew);
    alignas(4096) char first[4096] = "first";
    alignas(4096) char other[4096] = "other";
    const dfkv::DiskWriteResult direct =
        store.CacheDirectWithResult(K(931), first, 5, sizeof(first));
    EXPECT_EQ(direct.status, Status::kOk);
    EXPECT_EQ(direct.identity.valid(), tracking);
    EXPECT_EQ(direct.outcome, DiskWriteOutcome::kNew);
    const std::vector<dfkv::DiskWriteResult> batch =
        store.CacheDirectBatchWithResults({{K(932), first, 5, sizeof(first)},
                                           {K(932), other, 5, sizeof(other)},
                                           {K(933), nullptr, 1, 0}});
    ASSERT_EQ(batch.size(), 3U);
    EXPECT_EQ(batch[0].status, Status::kOk);
    EXPECT_EQ(batch[0].identity.valid(), tracking);
    EXPECT_EQ(batch[0].outcome, DiskWriteOutcome::kNew);
    EXPECT_EQ(batch[1].status, Status::kOk);
    EXPECT_FALSE(batch[1].identity.valid());
    EXPECT_EQ(batch[1].outcome, DiskWriteOutcome::kKeptExisting);
    EXPECT_EQ(batch[2].status, Status::kInvalid);
    EXPECT_FALSE(batch[2].identity.valid());
    EXPECT_EQ(batch[2].outcome, DiskWriteOutcome::kUnknown);
    for (const uint64_t key : {930U, 931U, 932U}) {
      const dfkv::DiskWriteResult duplicate =
          store.CacheWithResult(K(key), other, 5);
      EXPECT_EQ(duplicate.status, Status::kOk);
      EXPECT_EQ(duplicate.reason, dfkv::DiskWriteReason::kNone);
      EXPECT_FALSE(duplicate.identity.valid());
      EXPECT_EQ(duplicate.outcome, DiskWriteOutcome::kKeptExisting);
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
  EXPECT_EQ(result.outcome, DiskWriteOutcome::kRemoved);
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
  EXPECT_EQ(result.outcome, DiskWriteOutcome::kUnknown);
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
    EXPECT_EQ(results[i].outcome, DiskWriteOutcome::kNew);
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
  EXPECT_EQ(duplicate.outcome, DiskWriteOutcome::kKeptExisting);
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
  EXPECT_EQ(mixed[0].outcome, DiskWriteOutcome::kUnknown);
  EXPECT_EQ(mixed[1].outcome, DiskWriteOutcome::kKeptExisting);
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
  EXPECT_EQ(results[0].outcome, DiskWriteOutcome::kUnknown);
  EXPECT_EQ(group.CacheWithResult(K(1600), value, 1).outcome,
            DiskWriteOutcome::kUnknown);
  EXPECT_EQ(
      group.CacheDirectWithResult(K(1601), value, 1, sizeof(value)).outcome,
      DiskWriteOutcome::kUnknown);
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
    HookGate gate;  // Outlives the tier whose flusher invokes its hook.
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
    DiskSlabStoreTestPeer::SetPayloadWriteHook(stores[0],
                                               [&] { return gate.Run(); });
    const bool admitted = tier.Put(K(1800), "owner", 5);
    const bool entered = admitted && gate.WaitEnteredFor();
    RamTier::Hit before;
    const bool hit_before = entered && tier.GetPrep(K(1800), 0, 0, &before);
    EXPECT_FALSE(before.identity.valid());
    // Release before any fatal assertion or waiting PUT, including on timeout.
    gate.Release(true);
    ASSERT_TRUE(admitted);
    ASSERT_TRUE(entered);
    ASSERT_TRUE(hit_before);
    const RamTier::PutResult committed = tier.PutCommitted(K(1800), "owner", 5);
    EXPECT_FALSE(before.identity.valid());
    before = RamTier::Hit{};
    ASSERT_EQ(committed.status, Status::kOk);
    EXPECT_TRUE(committed.identity.valid());
    EXPECT_EQ(committed.outcome, DiskWriteOutcome::kNew);
    EXPECT_EQ(committed.identity,
              DiskSlabStoreTestPeer::Identity(*stores[0], K(1800)));
    const RamTier::PutResult duplicate =
        tier.PutCommitted(K(1800), "different", 9);
    EXPECT_EQ(duplicate.identity, committed.identity);
    EXPECT_EQ(duplicate.outcome, DiskWriteOutcome::kNew);
    std::string disk;
    ASSERT_EQ(group.Range(K(1800), 0, 0, &disk), Status::kOk);
    EXPECT_EQ(disk, "owner");
    RamTier::Hit hit;
    ASSERT_TRUE(tier.GetPrep(K(1800), 0, 5, &hit));
    EXPECT_EQ(std::string(hit.ptr, hit.len), disk);
    EXPECT_EQ(hit.identity, committed.identity);
    hit = RamTier::Hit{};
    DiskSlabStoreTestPeer::SetGenerationSequence(stores[0], UINT64_MAX);
    const RamTier::PutResult exhausted =
        tier.PutCommitted(K(1801), "blocked", 7);
    EXPECT_EQ(exhausted.status, Status::kCacheFull);
    EXPECT_EQ(exhausted.reason, DiskWriteReason::kIdentityExhausted);
    EXPECT_FALSE(exhausted.identity.valid());
    EXPECT_EQ(exhausted.disposition, RamTier::PutDisposition::kComplete);
    EXPECT_EQ(exhausted.outcome, DiskWriteOutcome::kUnknown);
    EXPECT_TRUE(tier.healthy());
    EXPECT_TRUE(group.Healthy());
    EXPECT_FALSE(tier.Contains(K(1801)));
    EXPECT_FALSE(group.IsCached(K(1801)));
  }
}

TEST_F(DiskWriteResultTest, RamDoesNotPublishUncommittedAttemptAsDiskCopy) {
  for (const bool batch : {false, true}) {
    for (const bool remove_during_write : {false, true}) {
      SCOPED_TRACE(batch);
      SCOPED_TRACE(remove_during_write);
      fs::remove_all(dir_);
      std::vector<DiskSlabStore*> stores;
      DiskCacheGroup group(GroupOptions(1, &stores));
      ASSERT_TRUE(group.Healthy());
      const BlockKey key = K(1850);
      if (!remove_during_write) {
        ASSERT_EQ(group.CacheWithResult(key, "existing", 8).outcome,
                  DiskWriteOutcome::kNew);
      } else {
        // Actual slab payload I/O runs outside D. Remove wins this write,
        // whose final release must finish invalidation before returning Ok.
        DiskSlabStoreTestPeer::SetPayloadWriteHook(stores[0], [&] {
          EXPECT_EQ(stores[0]->Remove(key), Status::kOk);
          return true;
        });
      }
      RamTier::Options options;
      options.bytes = 64 * 4096;
      options.reclaim_interval_ms = 0;
      RamTier tier(options, nullptr);
      const RamTier::FlushResultFn single = [&](const BlockKey& k, char* data,
                                                size_t len, size_t cap) {
        return group.CacheDirectWithResult(k, data, len, cap);
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
      const RamTier::PutResult result = tier.PutCommitted(key, "attempt", 7);
      EXPECT_EQ(result.status, Status::kOk);
      EXPECT_EQ(result.outcome, remove_during_write
                                    ? DiskWriteOutcome::kRemoved
                                    : DiskWriteOutcome::kKeptExisting);
      EXPECT_FALSE(result.identity.valid());
      RamTier::Hit hit;
      EXPECT_FALSE(tier.GetPrep(key, 0, 0, &hit));
      EXPECT_FALSE(hit.identity.valid());
      EXPECT_EQ(tier.DirtyBytes(), 0U);
      EXPECT_EQ(tier.DirtyObjects(), 0U);
      EXPECT_EQ(tier.UsedBytes(), 0U);
      EXPECT_TRUE(tier.healthy());
      std::string disk;
      EXPECT_EQ(group.Range(key, 0, 0, &disk),
                remove_during_write ? Status::kNotFound : Status::kOk);
      if (!remove_during_write) {
        EXPECT_EQ(disk, "existing");
      }
    }
  }
}

TEST_F(DiskWriteResultTest, SyncReadSourceBindsRealRamPromotion) {
  for (const ReadEntry entry :
       {ReadEntry::kString, ReadEntry::kInto, ReadEntry::kDirect}) {
    SCOPED_TRACE(static_cast<int>(entry));
    fs::remove_all(dir_);
    std::vector<DiskSlabStore*> stores;
    DiskCacheGroup group(GroupOptions(1, &stores));
    const DiskWriteResult committed =
        group.CacheWithResult(K(2000), "owner", 5);
    ASSERT_EQ(committed.status, Status::kOk);
    ASSERT_TRUE(committed.identity.valid());
    DiskResidentIdentity identity{99, 99, 99};
    std::string data;
    size_t stored = 0;
    ASSERT_EQ(ReadSource(group, entry, K(2000), 0, 0, &data, &identity, 4096,
                         &stored),
              Status::kOk);
    EXPECT_EQ(data, "owner");
    EXPECT_EQ(stored, 5U);
    EXPECT_EQ(identity, committed.identity);
    RamTier::Options options;
    options.bytes = 64 * 4096;
    options.reclaim_interval_ms = 0;
    RamTier tier(options, nullptr);
    ASSERT_TRUE(tier.PutDurable(K(2000), data.data(), data.size(), identity));
    RamTier::Hit hit;
    ASSERT_TRUE(tier.GetPrep(K(2000), 0, 0, &hit));
    EXPECT_EQ(std::string(hit.ptr, hit.len), "owner");
    EXPECT_EQ(hit.identity, committed.identity);
    // An already visible RAM owner cannot be relabeled by a later promotion.
    ASSERT_TRUE(tier.PutDurable(K(2000), "different", 9, {99, 99, 99}));
    ASSERT_TRUE(tier.GetPrep(K(2000), 0, 0, &hit));
    EXPECT_EQ(hit.identity, committed.identity);
    EXPECT_EQ(std::string(hit.ptr, hit.len), "owner");
  }
}

TEST_F(DiskWriteResultTest, SyncReadSourceDoesNotGuessForEmptyOrFailedReads) {
  for (const bool tracking : {false, true}) {
    for (const ReadEntry entry :
         {ReadEntry::kString, ReadEntry::kInto, ReadEntry::kDirect}) {
      SCOPED_TRACE(tracking);
      SCOPED_TRACE(static_cast<int>(entry));
      fs::remove_all(dir_);
      std::vector<DiskSlabStore*> stores;
      DiskCacheGroup group(GroupOptions(1, &stores, tracking));
      const DiskWriteResult committed =
          group.CacheWithResult(K(2001), "owner", 5);
      ASSERT_EQ(committed.status, Status::kOk);
      for (const uint64_t offset : {0U, 5U, 6U}) {
        DiskResidentIdentity identity{99, 99, 99};
        std::string data;
        const Status status =
            ReadSource(group, entry, K(2001), offset, 2, &data, &identity);
        EXPECT_EQ(status, offset > 5 ? Status::kInvalid : Status::kOk);
        EXPECT_EQ(identity, tracking && offset == 0 ? committed.identity
                                                    : DiskResidentIdentity{});
        if (offset == 0) {
          EXPECT_EQ(data, "ow");
        }
      }
      DiskResidentIdentity identity{99, 99, 99};
      std::string data;
      EXPECT_EQ(ReadSource(group, entry, K(2002), 0, 0, &data, &identity),
                Status::kNotFound);
      EXPECT_FALSE(identity.valid());
    }
  }
}

TEST_F(DiskWriteResultTest, SyncReadSourceFinalReleaseControlsCredentials) {
  for (const bool fail_invalidation : {false, true}) {
    for (const ReadEntry entry :
         {ReadEntry::kString, ReadEntry::kInto, ReadEntry::kDirect}) {
      SCOPED_TRACE(fail_invalidation);
      SCOPED_TRACE(static_cast<int>(entry));
      fs::remove_all(dir_);
      HookGate gate;  // Outlives group and its read hook, even on timeout.
      std::vector<DiskSlabStore*> stores;
      DiskCacheGroup group(GroupOptions(1, &stores));
      const BlockKey key = K(2010);
      const DiskWriteResult committed = group.CacheWithResult(key, "owner", 5);
      ASSERT_TRUE(committed.identity.valid());
      DiskSlabStoreTestPeer::SetReadHook(stores[0], [&] { gate.Run(); });
      DiskSlabStoreTestPeer::SetRecordWriteHook(
          stores[0], [&] { return !fail_invalidation; });
      DiskResidentIdentity identity{99, 99, 99};
      std::string data;
      Status status = Status::kInvalid;
      std::thread reader([&] {
        status = ReadSource(group, entry, key, 0, 0, &data, &identity);
      });
      const bool entered = gate.WaitEnteredFor();
      if (entered) {
        EXPECT_EQ(group.Remove(key), Status::kOk);
      }
      gate.Release(true);
      reader.join();
      ASSERT_TRUE(entered);
      EXPECT_EQ(status, fail_invalidation ? Status::kIOError : Status::kOk);
      EXPECT_FALSE(identity.valid());
      EXPECT_FALSE(group.IsCached(key));
      if (!fail_invalidation) {
        EXPECT_EQ(data, "owner");
        const DiskWriteResult next = group.CacheWithResult(key, "new", 3);
        EXPECT_TRUE(next.identity.valid());
        EXPECT_FALSE(next.identity == committed.identity);
        EXPECT_FALSE(identity.valid());  // Do not relabel old bytes as next.
      }
    }
  }
}

TEST_F(DiskWriteResultTest, SyncReadPayloadFailureClearsCapturedSource) {
  for (const ReadEntry entry :
       {ReadEntry::kString, ReadEntry::kInto, ReadEntry::kDirect}) {
    fs::remove_all(dir_);
    std::vector<DiskSlabStore*> stores;
    DiskCacheGroup group(GroupOptions(1, &stores));
    ASSERT_TRUE(group.CacheWithResult(K(2011), "owner", 5).identity.valid());
    // Force a real short pread after acquisition, not an injected result.
    DiskSlabStoreTestPeer::SetReadHook(stores[0], [&] {
      fs::resize_file(dir_ / "0" / "extents" / "E00000", 0);
    });
    DiskResidentIdentity identity{99, 99, 99};
    std::string data;
    EXPECT_EQ(ReadSource(group, entry, K(2011), 0, 0, &data, &identity),
              Status::kIOError);
    EXPECT_FALSE(identity.valid());
  }
}

TEST_F(DiskWriteResultTest, SyncReadDirectSourceSurvivesDioAndFallbacks) {
  std::vector<DiskSlabStore*> stores;
  DiskCacheGroup group(GroupOptions(1, &stores, true, true));
  ASSERT_TRUE(group.Healthy());
  if (!stores[0]->DirectWritesActive())
    GTEST_SKIP() << "filesystem rejected O_DIRECT; DIO branch not exercised";
  const std::string value(3000, 'v');
  const DiskWriteResult committed =
      group.CacheWithResult(K(2012), value.data(), value.size());
  ASSERT_TRUE(committed.identity.valid());
  alignas(4096) char buffer[8192] = {};
  for (const bool unaligned : {false, true}) {
    for (const size_t capacity : {0U, 1U, 4096U}) {
      SCOPED_TRACE(unaligned);
      SCOPED_TRACE(capacity);
      char* const destination = buffer + (unaligned ? 1 : 0);
      const char* data = nullptr;
      size_t got = 99;
      DiskResidentIdentity identity{99, 99, 99};
      ASSERT_EQ(group.RangeDirectWithIdentity(K(2012), 1, 20, destination,
                                              capacity, &data, &got, &identity),
                Status::kOk);
      EXPECT_EQ(got, capacity == 0 ? 0U : capacity == 1 ? 1U : 20U);
      EXPECT_EQ(identity,
                got != 0 ? committed.identity : DiskResidentIdentity{});
      if (got != 0) {
        EXPECT_EQ(std::string(data, got), std::string(got, 'v'));
      }
      if (!unaligned && capacity == 4096) {
        EXPECT_EQ(data, destination + 1);  // Proves aligned DIO head path.
      }
    }
  }
}

TEST_F(DiskWriteResultTest, SyncReadFileBackendRemainsUnbound) {
  DiskCacheGroup::Options options;
  options.cache_dirs = {dir_.string()};
  options.capacity_bytes = 1U << 20;
  options.engine = "file";
  options.eviction_policy = "legacy";
  DiskCacheGroup group(options);
  ASSERT_TRUE(group.Healthy());
  ASSERT_EQ(group.Cache(K(2013), "owner", 5), Status::kOk);
  for (const ReadEntry entry :
       {ReadEntry::kString, ReadEntry::kInto, ReadEntry::kDirect}) {
    DiskResidentIdentity identity{99, 99, 99};
    std::string data;
    EXPECT_EQ(ReadSource(group, entry, K(2013), 0, 0, &data, &identity),
              Status::kOk);
    EXPECT_EQ(data, "owner");
    EXPECT_FALSE(identity.valid());
    EXPECT_EQ(ReadSource(group, entry, K(2013), 0, 0, &data, nullptr),
              Status::kInvalid);
  }
}

enum class OwnerAction : unsigned char {
  kCommit = 0,
  kRemove = 1,
  kPayloadFailure = 2,
  kRecordFailure = 3,
  kRemoveFailure = 4
};

TEST_F(DiskWriteResultTest, StoreFollowersKeepOwnerTerminalOutcomeNotIdentity) {
  for (const bool typed_owner : {false, true}) {
    for (const bool batch_owner : {false, true}) {
      for (const bool batch_follower : {false, true}) {
        for (const OwnerAction action :
             {OwnerAction::kCommit, OwnerAction::kRemove,
              OwnerAction::kPayloadFailure, OwnerAction::kRecordFailure,
              OwnerAction::kRemoveFailure}) {
          SCOPED_TRACE(typed_owner);
          SCOPED_TRACE(batch_owner);
          SCOPED_TRACE(batch_follower);
          SCOPED_TRACE(static_cast<int>(action));
          fs::remove_all(dir_);
          HookGate gate;
          std::atomic<unsigned> records{0};
          DiskSlabStore::Options options = Opts(4 * 4096, 4 * 4096, 4096);
          options.table_sync_ms = options.reclaim_interval_ms = 0;
          options.track_resident_identity = true;
          DiskSlabStore store(options);
          ASSERT_TRUE(store.Healthy());
          DiskSlabStoreTestPeer::SetPayloadWriteHook(
              &store, [&] { return gate.Run(); });
          DiskSlabStoreTestPeer::SetRecordWriteHook(&store, [&] {
            const unsigned call = ++records;
            return action != OwnerAction::kRecordFailure &&
                   (action != OwnerAction::kRemoveFailure || call == 1);
          });
          alignas(4096) char owner_bytes[4096] = "owner";
          alignas(4096) char follower_bytes[4096] = "different";
          DiskWriteResult owner;
          DiskWriteResult follower;
          std::thread leader([&] {
            if (batch_owner) {
              const std::vector<StoreEngine::CacheBatchItem> items{
                  {K(1900), owner_bytes, 5, sizeof(owner_bytes)}};
              if (typed_owner)
                owner = store.CacheDirectBatchWithResults(items)[0];
              else
                owner.status = store.CacheDirectBatch(items)[0];
            } else if (typed_owner) {
              owner = store.CacheWithResult(K(1900), owner_bytes, 5);
            } else {
              owner.status = store.Cache(K(1900), owner_bytes, 5);
            }
          });
          if (!gate.WaitEnteredFor()) {
            gate.Release(false);
            leader.join();
            FAIL() << "owner did not reach payload gate";
          }
          std::thread waiter([&] {
            if (batch_follower)
              follower = store.CacheDirectBatchWithResults(
                  {{K(1900), follower_bytes, 9, sizeof(follower_bytes)}})[0];
            else
              follower = store.CacheDirectWithResult(K(1900), follower_bytes, 9,
                                                     sizeof(follower_bytes));
          });
          const std::chrono::steady_clock::time_point deadline =
              std::chrono::steady_clock::now() + std::chrono::seconds(5);
          while (!DiskSlabStoreTestPeer::HasPutFollower(store, K(1900)) &&
                 std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          const bool attached =
              DiskSlabStoreTestPeer::HasPutFollower(store, K(1900));
          const bool removed = action == OwnerAction::kRemove ||
                               action == OwnerAction::kRemoveFailure;
          if (attached && removed) {
            EXPECT_EQ(store.Remove(K(1900)), Status::kOk);
          }
          // No fatal assertions until both controlled threads have joined.
          gate.Release(action != OwnerAction::kPayloadFailure);
          leader.join();
          waiter.join();
          ASSERT_TRUE(attached) << "follower was not attached to owner flight";
          const bool success =
              action == OwnerAction::kCommit || action == OwnerAction::kRemove;
          EXPECT_EQ(owner.status, success ? Status::kOk : Status::kIOError);
          EXPECT_EQ(follower.status, owner.status);
          EXPECT_EQ(follower.reason, DiskWriteReason::kNone);
          EXPECT_FALSE(follower.identity.valid());
          EXPECT_EQ(follower.outcome, !success ? DiskWriteOutcome::kUnknown
                                      : removed
                                          ? DiskWriteOutcome::kRemoved
                                          : DiskWriteOutcome::kKeptExisting);
          if (typed_owner) {
            EXPECT_EQ(owner.outcome, !success  ? DiskWriteOutcome::kUnknown
                                     : removed ? DiskWriteOutcome::kRemoved
                                               : DiskWriteOutcome::kNew);
            EXPECT_EQ(owner.identity.valid(), success && !removed);
          }
          if (action == OwnerAction::kCommit) {
            std::string value;
            ASSERT_EQ(store.Range(K(1900), 0, 0, &value), Status::kOk);
            EXPECT_EQ(value, "owner");
          } else {
            EXPECT_FALSE(store.IsCached(K(1900)));
          }
        }
      }
    }
  }
}

}  // namespace
}  // namespace dfkv
