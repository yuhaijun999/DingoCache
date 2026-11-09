// RamTier wiring into KvNodeServer: with DFKV_RAM_TIER=1, PUT is write-through
// to RAM (sync-visible, async-flushed) and GET is served from RAM; disabled by
// default the node behaves exactly as before. End-to-end over the TCP
// transport. POSIX setenv/unsetenv are not C++ standard-library functions.
// Include their declaring C header directly; keep the exception local to this
// header.
#include <stdlib.h>  // NOLINT(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// Third-party headers.
#include <gtest/gtest.h>

#include "cache/disk_cache_group.h"
#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/disk_write_result.h"
#include "cache/kv_node_server.h"
#include "cache/kv_store.h"
#include "cache/ram_tier.h"
#include "cache/store_engine.h"
#include "client/key_map.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "read_coalescer_test_peer.h"
#include "slab_allocator_test_peer.h"
#include "transport/tcp_transport.h"
#include "transport/wire.h"

namespace fs = std::filesystem;
using namespace dfkv;  // NOLINT

using namespace std::chrono_literals;

namespace dfkv {
class DiskCacheGroupTestPeer {
 public:
  static DiskSlabStore& Slab(DiskCacheGroup& group) {
    return *static_cast<DiskSlabStore*>(group.disks_.front().get());
  }
};
class RamTierTestPeer {
 public:
  static void GateTypedBatch(RamTier& tier, std::function<void()> gate) {
    const RamTier::FlushBatchResultFn batch = tier.flush_batch_results_;
    tier.flush_batch_results_ =
        [batch,
         gate = std::move(gate)](const std::vector<RamTier::FlushItem>& items) {
          gate();
          return batch(items);
        };
  }
  static bool HasTypedSingle(const RamTier& tier) {
    return static_cast<bool>(tier.flush_result_);
  }
  static bool HasTypedBatch(const RamTier& tier) {
    return static_cast<bool>(tier.flush_batch_results_);
  }
  static DiskWriteResult FlushSingle(RamTier& tier, const BlockKey& key,
                                     char* data, size_t len, size_t cap) {
    return tier.flush_result_(key, data, len, cap);
  }
  static std::vector<DiskWriteResult> FlushBatch(
      RamTier& tier, const std::vector<RamTier::FlushItem>& items) {
    return tier.flush_batch_results_(items);
  }
  static bool StoredResult(RamTier& tier, const BlockKey& key,
                           RamTier::PutResult* result) {
    RamTier::Shard& shard = tier.ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mu);
    if (shard.index.count(key) == 0) return false;
    const std::shared_ptr<RamTier::PutCompletion>& completion =
        shard.index.at(key).completion;
    if (!completion || !completion->result) return false;
    *result = *completion->result;
    return true;
  }
};
class DiskSlabStoreTestPeer {
 public:
  static std::optional<DiskLifetimeState> LifetimeState(
      const DiskSlabStore& store, const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    return SlabAllocatorTestPeer::LifetimeSnapshotFor(*store.alloc_, key)
        .second;
  }
  static void SetReadHook(DiskSlabStore& store, std::function<void()> hook) {
    store.read_acquired_hook_for_test_ = std::move(hook);
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
class KvNodeServerWiringTestPeer {
 public:
  static DiskSlabStore::LifetimeReadCounters LifetimeCounters(
      const KvNodeServer& server) {
    return server.group_.SlabStats().lifetime_reads;
  }
  static bool WaitForReadFollowers(KvNodeServer& server, int count = 1) {
    return ReadCoalescerTestPeer::WaitForCopyWaiter(server.read_coalescer_,
                                                    count);
  }
  static size_t ReadTimeouts(const KvNodeServer& server) {
    return server.read_coalescer_.timeouts();
  }
  static RamTier& Ram(KvNodeServer& server) { return *server.ram_; }
  static size_t MutationStripe(const BlockKey& key) {
    return KvNodeServer::MutationKeyStripe(key);
  }
  static DiskSlabStore& Slab(KvNodeServer& server) {
    return DiskCacheGroupTestPeer::Slab(server.group_);
  }
  static uint64_t FlushObjects(const KvNodeServer& server) {
    return server.ram_flush_disks_.front()->objects.load(
        std::memory_order_relaxed);
  }
  static std::unique_ptr<KvNodeServer> Create(DiskCacheGroup::Options options) {
    return std::unique_ptr<KvNodeServer>(new KvNodeServer(std::move(options)));
  }
  static void SetLimit(KvNodeServer& server, size_t limit) {
    server.put_busy_limit_ = limit;
  }
  static void SetCoalescing(KvNodeServer& server, bool enabled) {
    server.coalesce_enabled_ = enabled;
  }
  static Status ReadSharedSource(KvNodeServer& server, const BlockKey& key,
                                 uint64_t offset, uint64_t epoch,
                                 DiskResidentIdentity* identity,
                                 std::string* payload) {
    char bytes[16] = {};
    size_t length = 0;
    const Status result = server.read_coalescer_.ReadWithIdentity(
        key, offset, 0, bytes, sizeof(bytes), &length, nullptr, identity,
        [](char*, size_t, size_t*, size_t*, DiskResidentIdentity*) {
          return Status::kIOError;
        },
        nullptr, epoch);
    if (result == Status::kOk) payload->assign(bytes, length);
    return result;
  }
  static bool AwaitCopyWaiter(KvNodeServer& server) {
    return ReadCoalescerTestPeer::WaitForCopyWaiter(server.read_coalescer_);
  }
  static void SetReadEpoch(KvNodeServer& server, const BlockKey& key,
                           uint64_t epoch) {
    KvNodeServer::ReadPublicationFence& fence =
        server.read_publication_[server.MutationKeyStripe(key)];
    const std::lock_guard<std::mutex> lock(fence.mutex);
    fence.epoch = epoch;
  }
  static bool ReadFlight(KvNodeServer& server, const BlockKey& key,
                         uint64_t offset, uint64_t length, uint64_t epoch) {
    return server.read_coalescer_.InFlight(key, offset, length, epoch);
  }
  static uint64_t ReadEpoch(KvNodeServer& server, const BlockKey& key) {
    return server.CaptureReadEpoch(key);
  }
  static bool Begin(KvNodeServer& server) { return server.TryBeginPut(); }
  static void End(KvNodeServer& server) { server.EndPut(); }
  static void SetRam(KvNodeServer& server, RamTier::FlushFn flush,
                     bool ram_ack = false, uint32_t watermark_pct = 80) {
    RamTier::Options options;
    options.bytes = 16 * 4096;
    options.large_reserve_bytes = 0;
    options.reclaim_interval_ms = 0;
    options.ack_high_watermark_pct = watermark_pct;
    options.is_persisted = [&server](const BlockKey& key) {
      return server.group_.IsCached(key);
    };
    server.ram_ = std::make_unique<RamTier>(options, std::move(flush));
    server.ram_write_back_ = true;
    server.ram_ack_enabled_ = ram_ack;
  }
  static void SetBeforeDiskRemoveHook(KvNodeServer& server,
                                      std::function<void()> hook) {
    server.before_disk_remove_hook_for_test_ = std::move(hook);
  }
  static void SetRamAck(KvNodeServer& server, bool enabled) {
    server.ram_ack_enabled_ = enabled;
  }
  static void SetWriteBack(KvNodeServer& server, bool enabled) {
    server.ram_write_back_ = enabled;
  }
  static bool InstallDurable(KvNodeServer& server, const BlockKey& key,
                             const std::string& value) {
    return server.ram_->PutDurable(key, value.data(), value.size());
  }
  static bool MutationLockHeld(KvNodeServer& server, const BlockKey& key) {
    std::mutex& mutex = server.mutation_key_mu_[server.MutationKeyStripe(key)];
    if (!mutex.try_lock()) return true;
    mutex.unlock();
    return false;
  }
  static bool Resident(KvNodeServer& server, const BlockKey& key) {
    return server.ram_->Contains(key);
  }
  static DiskWriteResult PersistSource(KvNodeServer& server,
                                       const BlockKey& key, const char* data,
                                       size_t len) {
    return server.group_.CacheWithResult(key, data, len);
  }
  static DiskResidentIdentity RamSource(KvNodeServer& server,
                                        const BlockKey& key) {
    RamTier::Hit hit;
    return server.ram_->GetPrep(key, 0, 0, &hit) ? hit.identity
                                                 : DiskResidentIdentity{};
  }
  static Status Persist(KvNodeServer& server, const BlockKey& key,
                        const char* data, size_t len) {
    return server.group_.Cache(key, data, len);
  }
  static Status DiskRead(KvNodeServer& server, const BlockKey& key,
                         std::string* out) {
    return server.group_.Range(key, 0, 0, out);
  }
  static const std::string& DiskEngine(const KvNodeServer& server) {
    return server.group_.EngineName();
  }
  static Status DropDisk(KvNodeServer& server, const BlockKey& key) {
    return server.group_.Remove(key);
  }
  static uint64_t UncleanResets(const KvNodeServer& server) {
    return server.group_.SlabStats().unclean_resets;
  }
  static bool PersistenceHealthy(const KvNodeServer& server) {
    return server.group_.Healthy() && (!server.ram_ || server.ram_->healthy());
  }
  static uint64_t DiskReadLeases(const KvNodeServer& server) {
    return server.group_.SlabStats().prep_holds;
  }
  static bool Drain(KvNodeServer& server) {
    return server.ram_->WaitForDrain(2s);
  }
  static bool DropRam(KvNodeServer& server, const BlockKey& key) {
    return server.ram_->Remove(key);
  }
};
}  // namespace dfkv

namespace {
// Count actual backend entry, including failed writes; absence of a file alone
// would not prove a terminal rejection avoided the direct-disk handler.
class CountingStore : public KVStore {
 public:
  CountingStore(const std::string& path, uint64_t capacity,
                std::atomic<size_t>& calls)
      : KVStore(Options{path, capacity}), calls_(calls) {}
  Status Cache(const BlockKey& key, const void* data, size_t len) override {
    calls_.fetch_add(1, std::memory_order_relaxed);
    return KVStore::Cache(key, data, len);
  }
  Status CacheDirect(const BlockKey& key, char* data, size_t len,
                     size_t cap) override {
    calls_.fetch_add(1, std::memory_order_relaxed);
    return KVStore::CacheDirect(key, data, len, cap);
  }
  Status RangeDirect(const BlockKey& key, uint64_t offset, uint64_t length,
                     char* io_buf, size_t io_cap, const char** out_data,
                     size_t* out_len, size_t* value_len) override {
    direct_reads.fetch_add(1, std::memory_order_relaxed);
    return KVStore::RangeDirect(key, offset, length, io_buf, io_cap, out_data,
                                out_len, value_len);
  }
  std::atomic<size_t> direct_reads{0};

 private:
  std::atomic<size_t>& calls_;
};

// Restore the caller's configuration even when a fatal assertion exits a test.
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* old = std::getenv(name)) {
      had_old_ = true;
      old_ = old;
    }
    ::setenv(name, value, 1);
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

struct FlushBarrier {
  std::mutex mutex;
  std::condition_variable cv;
  bool entered = false;
  bool released = false;
  void Block() {
    std::unique_lock<std::mutex> lock(mutex);
    entered = true;
    cv.notify_all();
    cv.wait(lock, [&] { return released; });
  }
  bool AwaitEntry() {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, 2s, [&] { return entered; });
  }
  void Open() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      released = true;
    }
    cv.notify_all();
  }
};

struct OpenFlushOnExit {
  FlushBarrier& barrier;
  ~OpenFlushOnExit() { barrier.Open(); }
};

class TrackedReadBarrierStore : public DiskSlabStore {
 public:
  explicit TrackedReadBarrierStore(const Options& options)
      : DiskSlabStore(options) {}
  Status RangeDirectWithIdentity(const BlockKey& key, uint64_t offset,
                                 uint64_t length, char* buffer, size_t capacity,
                                 const char** data, size_t* size,
                                 DiskResidentIdentity* identity,
                                 size_t* value_len) override {
    const Status result = DiskSlabStore::RangeDirectWithIdentity(
        key, offset, length, buffer, capacity, data, size, identity, value_len);
    if (after_read_ != nullptr) after_read_->Block();
    return result;
  }
  Status RangeWithIdentity(const BlockKey& key, uint64_t offset,
                           uint64_t length, std::string* bytes,
                           DiskResidentIdentity* identity,
                           size_t* value_len) override {
    const Status result = DiskSlabStore::RangeWithIdentity(
        key, offset, length, bytes, identity, value_len);
    if (after_read_ != nullptr) after_read_->Block();
    return result;
  }
  Status RangeIntoWithIdentity(const BlockKey& key, uint64_t offset,
                               uint64_t length, char* buffer, size_t capacity,
                               size_t* size, DiskResidentIdentity* identity,
                               size_t* value_len) override {
    const Status result = DiskSlabStore::RangeIntoWithIdentity(
        key, offset, length, buffer, capacity, size, identity, value_len);
    if (after_read_ != nullptr) after_read_->Block();
    return result;
  }
  // Set before launching the reader; never changed while it is running.
  FlushBarrier* after_read_ = nullptr;
};

class BlockingBypassStore : public KVStore {
 public:
  BlockingBypassStore(const std::string& path, uint64_t capacity,
                      FlushBarrier& barrier)
      : KVStore(Options{path, capacity}), barrier_(barrier) {}
  Status Cache(const BlockKey& key, const void* data, size_t len) override {
    if (len > 64 * 1024) barrier_.Block();
    return KVStore::Cache(key, data, len);
  }
  Status CacheDirect(const BlockKey& key, char* data, size_t len,
                     size_t cap) override {
    if (len > 64 * 1024) barrier_.Block();
    return KVStore::CacheDirect(key, data, len, cap);
  }

 private:
  FlushBarrier& barrier_;
};

// Block outside the real store read, so Remove can finish while its old
// bytes are still awaiting publication; preparation blocks before acquiring
// the lease to expose reads whose epoch was captured before Remove.
class ReadBarrierStore : public KVStore {
 public:
  ReadBarrierStore(const std::string& path, uint64_t capacity,
                   FlushBarrier* after_read, FlushBarrier* before_prep)
      : KVStore(Options{path, capacity}),
        after_read_(after_read),
        before_prep_(before_prep) {}
  Status RangeDirect(const BlockKey& key, uint64_t offset, uint64_t length,
                     char* buffer, size_t capacity, const char** data,
                     size_t* size, size_t* value_len) override {
    const Status result = KVStore::RangeDirect(key, offset, length, buffer,
                                               capacity, data, size, value_len);
    if (after_read_ != nullptr) after_read_->Block();
    return result;
  }
  Status RangeDirectPrep(const BlockKey& key, uint64_t offset, uint64_t length,
                         size_t capacity, ReadLease* lease) override {
    if (before_prep_ != nullptr) before_prep_->Block();
    return KVStore::RangeDirectPrep(key, offset, length, capacity, lease);
  }

  Status Remove(const BlockKey& key) override {
    return fail_remove_ ? Status::kIOError : KVStore::Remove(key);
  }
  bool fail_remove_ = false;

 private:
  FlushBarrier* after_read_;
  FlushBarrier* before_prep_;
};

std::unique_ptr<KvNodeServer> ReadFenceServer(
    const fs::path& dir, bool coalesce, FlushBarrier* after_read = nullptr,
    FlushBarrier* before_prep = nullptr) {
  fs::remove_all(dir);
  fs::create_directories(dir);
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  options.engine_factory = [after_read, before_prep](const std::string& path,
                                                     uint64_t capacity) {
    return std::make_unique<ReadBarrierStore>(path, capacity, after_read,
                                              before_prep);
  };
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  KvNodeServerWiringTestPeer::SetRam(
      *server, [](const BlockKey&, char*, size_t, size_t) { return false; });
  KvNodeServerWiringTestPeer::SetWriteBack(*server, false);
  KvNodeServerWiringTestPeer::SetCoalescing(*server, coalesce);
  return server;
}

// Extract a single-sample counter/gauge value from Prometheus text (line
// "<name>{...} <v>" or "<name> <v>"); returns -1 if absent.
long MetricVal(const std::string& text, const std::string& name) {
  size_t ls = 0;
  while (ls < text.size()) {
    size_t le = text.find('\n', ls);
    if (le == std::string::npos) le = text.size();
    const std::string line = text.substr(ls, le - ls);
    ls = le + 1;
    if (line.empty() || line[0] == '#') continue;                 // skip HELP/TYPE
    if (line.compare(0, name.size(), name) != 0) continue;        // must start with name
    char after = line.size() > name.size() ? line[name.size()] : '\0';
    if (after != '{' && after != ' ') continue;                  // exact metric, not a prefix
    size_t sp = line.rfind(' ');
    if (sp != std::string::npos) return std::stol(line.substr(sp + 1));
  }
  return -1;
}

std::unique_ptr<KvNodeServer> Start(const fs::path& dir, std::string* addr) {
  fs::remove_all(dir);
  fs::create_directories(dir);
  auto s = std::make_unique<KvNodeServer>(dir.string(), 1ull << 30);
  EXPECT_EQ(s->Start(0), Status::kOk);
  *addr = "127.0.0.1:" + std::to_string(s->port());
  return s;
}

template <class F>
bool WaitFor(F pred) {
  for (int i = 0; i < 2000; ++i) {
    if (pred()) return true;
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}
}  // namespace

TEST(RamTierWiring, WriteThroughAndServeFromRam) {
  ::setenv("DFKV_RAM_TIER", "1", 1);
  ::setenv("DFKV_RAM_TIER_BYTES", "16777216", 1);  // 16 MiB arena
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_ramwire_a";
  auto s = Start(dir, &addr);
  TcpTransport t;

  // PUT 20 blocks, then GET each back -- read-after-write straight from RAM.
  for (int i = 0; i < 20; ++i) {
    std::string v = "ram-block-" + std::to_string(i) + std::string(200, 'z');
    ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "k" + std::to_string(i)), v.data(), v.size()),
              Status::kOk) << i;
  }
  for (int i = 0; i < 20; ++i) {
    std::string v = "ram-block-" + std::to_string(i) + std::string(200, 'z');
    std::string out;
    ASSERT_EQ(t.Range(addr, ToBlockKey("test/model", "k" + std::to_string(i)), 0, v.size(), &out),
              Status::kOk) << i;
    EXPECT_EQ(out, v) << i;
  }

  std::string m = s->MetricsText();
  EXPECT_GE(MetricVal(m, "dfkv_ram_put_total"), 20);
  EXPECT_GE(MetricVal(m, "dfkv_ram_hit_total"), 20);
  EXPECT_EQ(MetricVal(m, "dfkv_ram_miss_total"), 0);
  // The async flusher persists every block to disk in the background.
  EXPECT_TRUE(WaitFor([&] { return MetricVal(s->MetricsText(), "dfkv_ram_flushed_total") >= 20; }));

  s.reset();  // stop the flusher before removing the dir (no write race)
  ::unsetenv("DFKV_RAM_TIER");
  ::unsetenv("DFKV_RAM_TIER_BYTES");
  fs::remove_all(dir);
}

TEST(RamTierWiring, SurvivesAsMissWhenRangePastEnd) {
  ::setenv("DFKV_RAM_TIER", "1", 1);
  ::setenv("DFKV_RAM_TIER_BYTES", "8388608", 1);
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_ramwire_b";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v = "0123456789";
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "p"), v.data(), v.size()), Status::kOk);
  std::string out;
  ASSERT_EQ(t.Range(addr, ToBlockKey("test/model", "p"), 3, 4, &out), Status::kOk);  // RAM sub-range
  EXPECT_EQ(out, "3456");
  // absent key -> miss even with RAM on
  EXPECT_EQ(t.Range(addr, ToBlockKey("test/model", "absent"), 0, 4, &out), Status::kNotFound);
  EXPECT_GE(MetricVal(s->MetricsText(), "dfkv_ram_miss_total"), 1);

  s.reset();  // stop the flusher before removing the dir (no write race)
  ::unsetenv("DFKV_RAM_TIER");
  ::unsetenv("DFKV_RAM_TIER_BYTES");
  fs::remove_all(dir);
}

TEST(RamTierWiring, ExistAndRemoveSeeRam) {
  ::setenv("DFKV_RAM_TIER", "1", 1);
  ::setenv("DFKV_RAM_TIER_BYTES", "8388608", 1);
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_ramwire_d";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v(300, 'e');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "live"), v.data(), v.size()), Status::kOk);
  bool e = false;
  ASSERT_EQ(t.Exist(addr, ToBlockKey("test/model", "live"), &e), Status::kOk);
  EXPECT_TRUE(e) << "a RAM-resident block must report as existing";
  // Let it flush to disk so Remove targets a durable block, then remove it.
  EXPECT_TRUE(WaitFor([&] { return MetricVal(s->MetricsText(), "dfkv_ram_flushed_total") >= 1; }));
  ASSERT_EQ(t.Remove(addr, ToBlockKey("test/model", "live")), Status::kOk);
  ASSERT_EQ(t.Exist(addr, ToBlockKey("test/model", "live"), &e), Status::kOk);
  EXPECT_FALSE(e) << "after Remove the block is gone from both tiers";
  std::string out;
  EXPECT_EQ(t.Range(addr, ToBlockKey("test/model", "live"), 0, v.size(), &out), Status::kNotFound);

  s.reset();  // stop the flusher before removing the dir (no write race)
  ::unsetenv("DFKV_RAM_TIER");
  ::unsetenv("DFKV_RAM_TIER_BYTES");
  fs::remove_all(dir);
}

TEST(RamTierWiring, PersistedValueSurvivesConflictingPutAfterRamEviction) {
  ::setenv("DFKV_RAM_TIER", "1", 1);
  ::setenv("DFKV_RAM_TIER_BYTES", "16777216", 1);
  for (const char* ack : {"ram", "disk"}) {
    ::setenv("DFKV_PUT_ACK_MODE", ack, 1);
    std::string addr;
    auto dir = fs::temp_directory_path() /
               (std::string("dfkv_ramwire_retained_") + ack);
    auto server = Start(dir, &addr);
    TcpTransport tcp;
    alignas(4096) char original[4096] = "original";
    alignas(4096) char conflicting[4096] = "modified";
    for (bool direct : {false, true}) {
      const BlockKey key = ToBlockKey("retained-disk", direct ? "direct" : "tcp");
      const auto put = [&](char* data) {
        return direct ? server->CacheDirectForKey(key, data, 8, sizeof(original))
                      : tcp.Cache(addr, key, data, 8);
      };
      ASSERT_EQ(put(original), Status::kOk);
      ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
      std::string disk;
      ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &disk), Status::kOk);
      ASSERT_EQ(disk, "original");
      ASSERT_TRUE(KvNodeServerWiringTestPeer::DropRam(*server, key));
      ASSERT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));

      ASSERT_EQ(put(conflicting), Status::kOk);
      ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
      std::string visible;
      ASSERT_EQ(tcp.Range(addr, key, 0, 8, &visible), Status::kOk);
      EXPECT_EQ(visible, "original") << ack << " direct=" << direct;
      ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &disk), Status::kOk);
      EXPECT_EQ(disk, visible) << "RAM must not claim durability for different disk bytes";
    }
    server.reset();
    fs::remove_all(dir);
  }
  ::unsetenv("DFKV_PUT_ACK_MODE");
  ::unsetenv("DFKV_RAM_TIER_BYTES");
  ::unsetenv("DFKV_RAM_TIER");
}

TEST(RamTierWiring, CapacityBypassPreservesConcurrentDuplicateValue) {
  for (bool direct : {false, true}) {
    for (bool ram_ack : {false, true}) {
      auto dir = fs::temp_directory_path() /
          ("dfkv_ramwire_bypass_" + std::to_string(direct) +
           "_" + std::to_string(ram_ack));
      fs::remove_all(dir);
      fs::create_directories(dir);
      FlushBarrier disk, flush;
      DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
      options.engine_factory = [&](const std::string& path, uint64_t capacity) {
        return std::make_unique<BlockingBypassStore>(path, capacity, disk);
      };
      auto server = KvNodeServerWiringTestPeer::Create(std::move(options));
      auto* node = server.get();
      OpenFlushOnExit disk_cleanup{disk}, flush_cleanup{flush};
      KvNodeServerWiringTestPeer::SetLimit(*server, 0);
      KvNodeServerWiringTestPeer::SetRam(
          *server,
          [&, node](const BlockKey& key, char* data, size_t len, size_t) {
            flush.Block();
            return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
                   Status::kOk;
          }, ram_ack);
      const BlockKey key{8101, 0};
      alignas(4096) char original[128 * 1024], conflicting[4096];
      std::memset(original, 'a', sizeof(original));
      std::memset(conflicting, 'b', sizeof(conflicting));
      auto put = [&](char* value, size_t len) {
        if (direct)
          return server->CacheDirectForKey(key, value, len, len);
        std::string out;
        return server->ProcessRequestForKey(
            static_cast<uint8_t>(WireOp::kCache), key, 0, 0,
            value, len, &out);
      };
      Status first_status = Status::kInvalid, second_status = Status::kInvalid;
      std::thread first([&] { first_status = put(original, sizeof(original)); });
      const bool disk_entered = disk.AwaitEntry();
      EXPECT_TRUE(disk_entered);
      std::thread second([&] { second_status = put(conflicting, sizeof(conflicting)); });
      // Before the fix, the conflicting value is admitted while the oversized
      // owner is in disk I/O. Hold that flush until the owner commits first.
      // Correct serialization cannot enter this flush before disk is opened.
      flush.AwaitEntry();
      disk.Open();
      first.join();
      flush.Open();
      second.join();
      EXPECT_EQ(first_status, Status::kOk);
      EXPECT_EQ(second_status, Status::kOk);
      ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
      std::string persisted, visible;
      ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
                Status::kOk);
      ASSERT_EQ(server->ProcessRequestForKey(
                    static_cast<uint8_t>(WireOp::kRange), key, 0, 0,
                    nullptr, 0, &visible), Status::kOk);
      ASSERT_EQ(persisted.size(), sizeof(original));
      EXPECT_EQ(std::memcmp(persisted.data(), original, sizeof(original)), 0);
      EXPECT_EQ(visible, persisted) << "direct=" << direct
                                    << " ram_ack=" << ram_ack;
      server.reset();
      fs::remove_all(dir);
    }
  }
}

TEST(RamTierWiring, ColdDirectSliceDoesNotReplaceWholeValue) {
  std::string value(8192 + 317, '\0');
  for (size_t i = 0; i < value.size(); ++i)
    value[i] = static_cast<char>('!' + i % 90);
  for (const bool coalesce : {false, true}) {
    for (size_t offset : {size_t{0}, size_t{73}, value.size() - 121}) {
      SCOPED_TRACE("coalesce=" + std::to_string(coalesce) +
                   " offset=" + std::to_string(offset));
      const auto dir = fs::temp_directory_path() /
          ("dfkv_direct_slice_" + std::to_string(coalesce) +
           "_" + std::to_string(offset));
      fs::remove_all(dir);
      fs::create_directories(dir);
      std::atomic<size_t> disk_puts{0};
      CountingStore* store = nullptr;
      DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
      options.engine_factory = [&](const std::string& path, uint64_t capacity) {
        auto engine = std::make_unique<CountingStore>(path, capacity, disk_puts);
        store = engine.get();
        return engine;
      };
      auto server = KvNodeServerWiringTestPeer::Create(std::move(options));
      KvNodeServerWiringTestPeer::SetCoalescing(*server, coalesce);
      KvNodeServerWiringTestPeer::SetRam(
          *server, [](const BlockKey&, char*, size_t, size_t) { return false; });
      const BlockKey key{8201, 0};
      // Seed disk directly: the first range is deterministically a cold read.
      ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(
                    *server, key, value.data(), value.size()), Status::kOk);
      ASSERT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
      alignas(4096) char staging[16 * 1024];
      const char* data = nullptr;
      size_t bytes = 0, value_len = 0;
      const size_t length = offset == 0 ? 233 : 5001;
      ASSERT_EQ(server->RangeDirectForKey(
                    key, offset, length, staging, sizeof(staging),
                    &data, &bytes, &value_len), Status::kOk);
      ASSERT_NE(data, nullptr);
      EXPECT_EQ(std::string(data, bytes), value.substr(offset, length));
      EXPECT_EQ(value_len, value.size());
      EXPECT_EQ(store->direct_reads.load(), 1u);
      EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key))
          << "Before the fix, a cold slice was installed as the whole value";

      // A complete read must still return the original bytes and stored length,
      // including when the requested length is larger than the stored value.
      ASSERT_EQ(server->RangeDirectForKey(
                    key, 0, value.size() + 4096, staging, sizeof(staging),
                    &data, &bytes, &value_len), Status::kOk);
      ASSERT_NE(data, nullptr);
      EXPECT_EQ(bytes, value.size());
      EXPECT_EQ(std::string(data, bytes), value);
      EXPECT_EQ(value_len, value.size());
      EXPECT_EQ(store->direct_reads.load(), 2u);
      EXPECT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, key));

      // Behavior, not just membership: promotion exposes the complete pinned
      // arena value and subsequent direct reads do not enter the disk backend.
      {
        PreparedRead pinned;
        ASSERT_TRUE(server->RamPinnedHitForKey(key, 0, value.size(), &pinned));
        EXPECT_EQ(pinned.payload_len(), value.size());
        EXPECT_EQ(pinned.value_len(), value.size());
        EXPECT_EQ(std::string(pinned.data(), pinned.payload_len()), value);
      }
      ASSERT_EQ(server->RangeDirectForKey(
                    key, 0, value.size(), staging, sizeof(staging),
                    &data, &bytes, &value_len), Status::kOk);
      EXPECT_EQ(std::string(data, bytes), value);
      EXPECT_EQ(value_len, value.size());
      EXPECT_EQ(store->direct_reads.load(), 2u);
      server.reset();
      fs::remove_all(dir);
    }
  }
}

TEST(RamTierWiring, ColdWholeDirectReadPromotesWithoutLengthOutput) {
  for (const bool coalesce : {false, true}) {
    SCOPED_TRACE("coalesce=" + std::to_string(coalesce));
    const auto dir = fs::temp_directory_path() /
        ("dfkv_direct_whole_" + std::to_string(coalesce));
    fs::remove_all(dir);
    fs::create_directories(dir);
    std::atomic<size_t> disk_puts{0};
    CountingStore* store = nullptr;
    DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
    options.engine_factory = [&](const std::string& path, uint64_t capacity) {
      auto engine = std::make_unique<CountingStore>(path, capacity, disk_puts);
      store = engine.get();
      return engine;
    };
    auto server = KvNodeServerWiringTestPeer::Create(std::move(options));
    KvNodeServerWiringTestPeer::SetCoalescing(*server, coalesce);
    KvNodeServerWiringTestPeer::SetRam(
        *server, [](const BlockKey&, char*, size_t, size_t) { return false; });
    const BlockKey key{8202, 0};
    const std::string value(4096 + 111, 'w');
    ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(
                  *server, key, value.data(), value.size()), Status::kOk);
    ASSERT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
    alignas(4096) char staging[8192];
    const char* data = nullptr;
    size_t bytes = 0;
    // value_len is optional; omitting it must not suppress whole promotion.
    ASSERT_EQ(server->RangeDirectForKey(
                  key, 0, value.size(), staging, sizeof(staging),
                  &data, &bytes), Status::kOk);
    ASSERT_NE(data, nullptr);
    EXPECT_EQ(std::string(data, bytes), value);
    EXPECT_EQ(store->direct_reads.load(), 1u);
    EXPECT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, key));
    ASSERT_EQ(server->RangeDirectForKey(
                  key, 0, value.size(), staging, sizeof(staging),
                  &data, &bytes), Status::kOk);
    EXPECT_EQ(std::string(data, bytes), value);
    EXPECT_EQ(store->direct_reads.load(), 1u);
    server.reset();
    fs::remove_all(dir);
  }
}

TEST(RamTierWiring, DisabledByDefaultNoRamMetrics) {
  ::unsetenv("DFKV_RAM_TIER");  // ensure off
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_ramwire_c";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v(128, 'd');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "x"), v.data(), v.size()), Status::kOk);
  std::string out;
  ASSERT_EQ(t.Range(addr, ToBlockKey("test/model", "x"), 0, v.size(), &out), Status::kOk);  // via disk
  EXPECT_EQ(out, v);
  std::string m = s->MetricsText();
  EXPECT_EQ(MetricVal(m, "dfkv_ram_hit_total"), -1) << "no RAM metrics when disabled";
  EXPECT_EQ(MetricVal(m, "dfkv_ram_put_total"), -1);
  EXPECT_EQ(s->m_cache_put(), 1u);   // normal disk accounting intact
  EXPECT_EQ(s->m_cache_hit(), 1u);
  s.reset();
  fs::remove_all(dir);
}

// Hold all acquired permits until every contender has attempted admission.
// The winner count is exact, independent of disk latency or thread scheduling.
TEST(RamTierWiring, PutAdmissionGateReservesLimitAtomically) {
  ::unsetenv("DFKV_RAM_TIER");
  auto dir = fs::temp_directory_path() / "dfkv_admission_atomic";
  fs::remove_all(dir);
  fs::create_directories(dir);
  auto server = std::make_unique<KvNodeServer>(dir.string(), 1ull << 30);
  KvNodeServerWiringTestPeer::SetLimit(*server, 1);
  std::mutex mutex;
  std::condition_variable cv;
  int attempted = 0;
  int admitted = 0;
  bool release = false;
  std::vector<std::thread> writers;
  for (int i = 0; i < 8; ++i) {
    writers.emplace_back([&] {
      const bool acquired = KvNodeServerWiringTestPeer::Begin(*server);
      {
        std::unique_lock<std::mutex> lock(mutex);
        ++attempted;
        if (acquired) ++admitted;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
      }
      if (acquired) KvNodeServerWiringTestPeer::End(*server);
    });
  }
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, 2s, [&] { return attempted == 8; }));
    release = true;
  }
  cv.notify_all();
  for (auto& writer : writers) writer.join();
  EXPECT_EQ(admitted, 1);
  EXPECT_EQ(server->PutBusyTotal(), 7u);
  const bool acquired = KvNodeServerWiringTestPeer::Begin(*server);
  EXPECT_TRUE(acquired);
  if (acquired) KvNodeServerWiringTestPeer::End(*server);
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, PutGateRejectsBeforeRamAdmissionAndLeavesFlushUngated) {
  ::unsetenv("DFKV_RAM_TIER");
  auto dir = fs::temp_directory_path() / "dfkv_admission_ram";
  fs::remove_all(dir);
  fs::create_directories(dir);
  auto server = std::make_unique<KvNodeServer>(dir.string(), 1ull << 30);
  KvNodeServerWiringTestPeer::SetLimit(*server, 1);
  std::mutex mutex;
  std::condition_variable cv;
  bool flushing = false;
  bool release = false;
  int rejected_done = 0;
  KvNodeServerWiringTestPeer::SetRam(
      *server, [&](const BlockKey& key, char* data, size_t len, size_t) {
        {
          std::unique_lock<std::mutex> lock(mutex);
          flushing = true;
          cv.notify_all();
          cv.wait(lock, [&] { return release; });
        }
        return KvNodeServerWiringTestPeer::Persist(*server, key, data, len) ==
               Status::kOk;
      });
  const BlockKey first{7001, 0}, direct_key{7002, 0}, tcp_key{7003, 0};
  std::string original(500, 'a'), conflicting(1000, 'b');
  auto tcp_put = [&](const BlockKey& key, const std::string& value) {
    std::string out;
    return server->ProcessRequestForKey(
        static_cast<uint8_t>(WireOp::kCache), key, 0, 0, value.data(),
        value.size(), &out);
  };
  Status leader = Status::kInvalid;
  Status direct_rejected = Status::kInvalid;
  Status tcp_rejected = Status::kInvalid;
  std::thread writer([&] {
    leader = server->CacheDirectForKey(first, original.data(),
                                      original.size(), original.size());
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, 2s, [&] { return flushing; }));
  }
  std::thread direct([&] {
    direct_rejected = server->CacheDirectForKey(
        direct_key, conflicting.data(), conflicting.size(), conflicting.size());
    std::lock_guard<std::mutex> lock(mutex);
    ++rejected_done;
    cv.notify_all();
  });
  std::thread tcp([&] {
    tcp_rejected = tcp_put(tcp_key, conflicting);
    std::lock_guard<std::mutex> lock(mutex);
    ++rejected_done;
    cv.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    // This timeout is only a failure guard. The ordering is the explicit
    // flush/permit barrier, not a sleep chosen to catch a race window.
    EXPECT_TRUE(cv.wait_for(lock, 2s, [&] { return rejected_done == 2; }));
  }
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, direct_key));
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, tcp_key));
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  cv.notify_all();
  writer.join();
  direct.join();
  tcp.join();
  EXPECT_EQ(leader, Status::kOk);
  EXPECT_EQ(direct_rejected, Status::kCacheFull);
  EXPECT_EQ(tcp_rejected, Status::kCacheFull);
  EXPECT_EQ(server->PutBusyTotal(), 2u);
  // The accepted write retained its permit until flush completed, and flush
  // did not itself need a second permit. Both public paths are usable again.
  EXPECT_EQ(tcp_put(first, conflicting), Status::kOk);
  EXPECT_EQ(server->CacheDirectForKey(first, conflicting.data(),
                                      conflicting.size(), conflicting.size()),
            Status::kOk);
  std::string persisted;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, first, &persisted),
            Status::kOk);
  EXPECT_EQ(persisted, original);
  EXPECT_EQ(tcp_put(tcp_key, conflicting), Status::kOk);
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, DirtyWatermarkRejectsWithoutCallingDiskAndRecovers) {
  ::unsetenv("DFKV_RAM_TIER");
  const auto dir = fs::temp_directory_path() / "dfkv_watermark_terminal";
  fs::remove_all(dir);
  fs::create_directories(dir);
  std::atomic<size_t> disk_calls{0};
  FlushBarrier barrier;
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  options.engine_factory = [&](const std::string& path, uint64_t capacity) {
    return std::make_unique<CountingStore>(path, capacity, disk_calls);
  };
  auto server = KvNodeServerWiringTestPeer::Create(std::move(options));
  auto* node = server.get();
  OpenFlushOnExit cleanup{barrier};
  KvNodeServerWiringTestPeer::SetLimit(*server, 0);
  KvNodeServerWiringTestPeer::SetRam(
      *server,
      [&, node](const BlockKey& key, char* data, size_t len, size_t) {
        barrier.Block();
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      },
      /*ram_ack=*/true, /*watermark_pct=*/25);
  const BlockKey first{7101, 0}, tcp_key{7102, 0}, direct_key{7103, 0};
  std::string original(16 * 1024, 'a'), different(500, 'b');
  auto tcp_put = [&](const BlockKey& key, const std::string& value) {
    std::string out;
    return server->ProcessRequestForKey(
        static_cast<uint8_t>(WireOp::kCache), key, 0, 0, value.data(),
        value.size(), &out);
  };
  ASSERT_EQ(tcp_put(first, original), Status::kOk);  // RAM-ACK, no live PUT permit
  ASSERT_TRUE(barrier.AwaitEntry());
  ASSERT_EQ(disk_calls.load(), 0u);  // durability is deliberately stalled

  Status tcp_status = Status::kInvalid, direct_status = Status::kInvalid;
  int completed = 0;
  std::thread tcp([&] {
    tcp_status = tcp_put(tcp_key, different);
    std::lock_guard<std::mutex> lock(barrier.mutex);
    ++completed;
    barrier.cv.notify_all();
  });
  std::thread direct([&] {
    direct_status = server->CacheDirectForKey(
        direct_key, different.data(), different.size(), different.size());
    std::lock_guard<std::mutex> lock(barrier.mutex);
    ++completed;
    barrier.cv.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(barrier.mutex);
    // A failure guard, not a timing race: flush stays closed until both NEW
    // requests have had the opportunity to finish without queueing behind it.
    EXPECT_TRUE(barrier.cv.wait_for(lock, 2s, [&] { return completed == 2; }));
  }
  EXPECT_EQ(disk_calls.load(), 0u);
  EXPECT_EQ(server->Count(), 0u);
  EXPECT_EQ(server->PutBusyTotal(), 0u);  // not the foreground-count gate
  EXPECT_TRUE(server->Healthy());
  EXPECT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, first));
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, tcp_key));
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, direct_key));
  std::string visible;
  EXPECT_EQ(server->ProcessRequestForKey(
                static_cast<uint8_t>(WireOp::kRange), first, 0, 0,
                nullptr, 0, &visible),
            Status::kOk);
  EXPECT_EQ(visible, original);
  barrier.Open();
  tcp.join();
  direct.join();
  EXPECT_EQ(tcp_status, Status::kCacheFull);
  EXPECT_EQ(direct_status, Status::kCacheFull);
  ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
  EXPECT_EQ(disk_calls.load(), 1u);

  // Rejections never claimed these keys. Both entry points can admit them
  // after drain; conflicting duplicates still preserve the committed leader.
  EXPECT_EQ(tcp_put(first, different), Status::kOk);
  EXPECT_EQ(server->CacheDirectForKey(
                first, different.data(), different.size(), different.size()),
            Status::kOk);
  EXPECT_EQ(disk_calls.load(), 1u);
  std::string persisted;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, first, &persisted),
            Status::kOk);
  EXPECT_EQ(persisted, original);
  ASSERT_EQ(tcp_put(tcp_key, different), Status::kOk);
  ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
  ASSERT_EQ(server->CacheDirectForKey(
                direct_key, different.data(), different.size(), different.size()),
            Status::kOk);
  ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
  EXPECT_EQ(disk_calls.load(), 3u);
  EXPECT_TRUE(server->Healthy());
  server.reset();
  fs::remove_all(dir);
}

// PreparedRead unifies RAM and disk ownership. A RAM-resident value is returned
// ready with a send pin; an absent key proceeds to disk after exactly one RAM
// miss. Destroying the ready owner exercises connection-abort cleanup.
TEST(RamTierWiring, PreparedReadCountsRamMissesAndOwnsRamPin) {
  ::setenv("DFKV_RAM_TIER", "1", 1);
  ::setenv("DFKV_RAM_TIER_BYTES", "8388608", 1);
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_ramwire_prep";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v(4096, 'q');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "resident"), v.data(), v.size()), Status::kOk);

  const BlockKey rk = ToBlockKey("test/model", "resident");
  const BlockKey ak = ToBlockKey("test/model", "absent-key");
  std::vector<char> staging(1 << 20);

  // Consulted-and-present is not a miss and returns one live transaction.
  const long miss0 = MetricVal(s->MetricsText(), "dfkv_ram_miss_total");
  {
    PreparedRead prep =
        s->PrepareReadForKey(rk, 0, v.size(), staging.data(), staging.size());
    EXPECT_EQ(prep.status(), Status::kOk);
    EXPECT_TRUE(prep.owns_cleanup());
    EXPECT_FALSE(prep.needs_io());
    EXPECT_EQ(prep.payload_len(), v.size());
  }  // destructor-abort releases the RAM send pin
  EXPECT_EQ(MetricVal(s->MetricsText(), "dfkv_ram_miss_total"), miss0);

  // Failure to resolve the arena MR falls back to the owned staging buffer
  // without splitting cleanup ownership or releasing the pin early.
  PreparedRead staged =
      s->PrepareReadForKey(rk, 0, v.size(), staging.data(), staging.size());
  ASSERT_EQ(staged.status(), Status::kOk);
  ASSERT_TRUE(staged.source_registered());
  ASSERT_TRUE(staged.Stage());
  EXPECT_FALSE(staged.source_registered());
  EXPECT_EQ(std::memcmp(staged.data(), v.data(), v.size()), 0);
  EXPECT_TRUE(staged.Commit(Status::kOk, staged.payload_len(), 0.0));
  EXPECT_FALSE(staged.Abort());

  // Absent everywhere: RAM consulted and absent -> exactly one RAM miss.
  PreparedRead absent =
      s->PrepareReadForKey(ak, 0, 16, staging.data(), staging.size());
  EXPECT_EQ(absent.status(), Status::kNotFound);
  EXPECT_FALSE(absent.owns_cleanup());
  EXPECT_EQ(MetricVal(s->MetricsText(), "dfkv_ram_miss_total"), miss0 + 1);

  s.reset();
  ::unsetenv("DFKV_RAM_TIER");
  ::unsetenv("DFKV_RAM_TIER_BYTES");
  fs::remove_all(dir);
}

TEST(RamTierWiring, RemoveGapDoesNotDeleteFollowingPut) {
  for (int path = 0; path < 3; ++path) {
    SCOPED_TRACE(path);  // TCP single, direct single, direct batch.
    const fs::path dir =
        fs::temp_directory_path() / ("dfkv_remove_gap_" + std::to_string(path));
    fs::remove_all(dir);
    fs::create_directories(dir);
    FlushBarrier remove_gap, put_finished;
    DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
    std::unique_ptr<KvNodeServer> server =
        KvNodeServerWiringTestPeer::Create(std::move(options));
    KvNodeServer* const node = server.get();
    KvNodeServerWiringTestPeer::SetRam(
        *node, [node](const BlockKey& key, char* data, size_t len, size_t) {
          return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
                 Status::kOk;
        });
    const BlockKey key{9100, 0};
    // Persist, promote, then remove only the disk copy to construct a valid
    // RAM survivor. This is not an allocator eviction-policy test.
    ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*node, key, "old", 3),
              Status::kOk);
    ASSERT_TRUE(KvNodeServerWiringTestPeer::InstallDurable(*node, key, "old"));
    ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*node, key), Status::kOk);
    KvNodeServerWiringTestPeer::SetBeforeDiskRemoveHook(
        *node, [&] { remove_gap.Block(); });
    Status removed = Status::kInvalid;
    std::thread remover([&] {
      std::string ignored;
      removed =
          node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove), key,
                                     0, 0, nullptr, 0, &ignored);
    });
    EXPECT_TRUE(remove_gap.AwaitEntry());
    EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*node, key));
    EXPECT_TRUE(KvNodeServerWiringTestPeer::MutationLockHeld(*node, key));
    alignas(4096) char value[4096] = "new";
    Status written = Status::kInvalid;
    std::thread writer([&] {
      if (path == 0) {
        std::string ignored;
        written =
            node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                       key, 0, 0, value, 3, &ignored);
      } else if (path == 1) {
        written = node->CacheDirectForKey(key, value, 3, sizeof(value));
      } else {
        const std::vector<StoreEngine::CacheBatchItem> items{
            {key, value, 3, sizeof(value)}};
        const std::vector<Status> results =
            node->CacheDirectBatchForKeys(items);
        EXPECT_EQ(results.size(), 1u);
        if (!results.empty()) written = results[0];
      }
      put_finished.Block();
    });
    // Red implementation finishes the new PUT inside this held Remove gap;
    // the repaired one waits for Remove's mutation lock. This bounded wait is
    // not the sole oracle: lock ownership and actual RAM/disk bytes are
    // checked.
    (void)put_finished.AwaitEntry();
    remove_gap.Open();
    put_finished.Open();
    remover.join();
    writer.join();
    EXPECT_EQ(removed, Status::kOk);
    EXPECT_EQ(written, Status::kOk);
    std::string disk, visible;
    EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node, key, &disk),
              Status::kOk);
    EXPECT_EQ(node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                         key, 0, 0, nullptr, 0, &visible),
              Status::kOk);
    EXPECT_EQ(disk, "new");
    EXPECT_EQ(visible, disk);
    server.reset();
    fs::remove_all(dir);
  }
}

TEST(RamTierWiring, ForegroundDiskWritesHoldMutationLocksInEveryMode) {
  for (int mode = 0; mode < 3; ++mode) {
    for (int path = 0; path < 3; ++path) {
      SCOPED_TRACE("mode=" + std::to_string(mode) +
                   " path=" + std::to_string(path));
      const fs::path dir = fs::temp_directory_path() /
                           ("dfkv_mutation_modes_" + std::to_string(mode) +
                            "_" + std::to_string(path));
      fs::remove_all(dir);
      fs::create_directories(dir);
      FlushBarrier disk;
      DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
      options.engine_factory = [&](const std::string& name, uint64_t capacity) {
        return std::make_unique<BlockingBypassStore>(name, capacity, disk);
      };
      std::unique_ptr<KvNodeServer> server =
          KvNodeServerWiringTestPeer::Create(std::move(options));
      if (mode != 0) {
        KvNodeServerWiringTestPeer::SetRam(
            *server,
            [](const BlockKey&, char*, size_t, size_t) { return false; });
        KvNodeServerWiringTestPeer::SetWriteBack(*server, mode == 2);
      }
      // No RAM / writearound / writeback capacity bypass. Two keys collide in
      // the foreground stripe; duplicate keys must not recursively lock it.
      const BlockKey first{9200, 0}, second{9201, 0}, collision{10224, 0};
      alignas(4096) char value[128 * 1024];
      std::memset(value, 'v', sizeof(value));
      alignas(4096) char small[4096] = "abc";
      std::vector<Status> results;
      Status single = Status::kInvalid;
      std::thread writer([&] {
        if (path == 0) {
          std::string ignored;
          single = server->ProcessRequestForKey(
              static_cast<uint8_t>(WireOp::kCache), first, 0, 0, value,
              sizeof(value), &ignored);
        } else if (path == 1) {
          single = server->CacheDirectForKey(first, value, sizeof(value),
                                             sizeof(value));
        } else {
          const std::vector<StoreEngine::CacheBatchItem> items{
              {first, value, sizeof(value), sizeof(value)},
              {second, small, 3, sizeof(small)},
              {collision, small, 3, sizeof(small)},
              {first, small, 3, sizeof(small)},
              {second, small, 0, sizeof(small)}};
          results = server->CacheDirectBatchForKeys(items);
        }
      });
      EXPECT_TRUE(disk.AwaitEntry());
      EXPECT_TRUE(KvNodeServerWiringTestPeer::MutationLockHeld(*server, first));
      if (path == 2) {
        EXPECT_TRUE(
            KvNodeServerWiringTestPeer::MutationLockHeld(*server, second));
        EXPECT_TRUE(
            KvNodeServerWiringTestPeer::MutationLockHeld(*server, collision));
      }
      disk.Open();
      writer.join();
      if (path == 2) {
        ASSERT_EQ(results.size(), 5u);
        for (size_t i = 0; i < 4; ++i) EXPECT_EQ(results[i], Status::kOk);
        EXPECT_EQ(results[4], Status::kInvalid);
      } else {
        EXPECT_EQ(single, Status::kOk);
      }
      EXPECT_FALSE(
          KvNodeServerWiringTestPeer::MutationLockHeld(*server, first));
      std::string persisted;
      EXPECT_EQ(
          KvNodeServerWiringTestPeer::DiskRead(*server, first, &persisted),
          Status::kOk);
      ASSERT_EQ(persisted.size(), sizeof(value));
      EXPECT_EQ(std::memcmp(persisted.data(), value, sizeof(value)), 0);
      server.reset();
      fs::remove_all(dir);
    }
  }
}

TEST(RamTierWiring, DiskAckPutAndRemoveLeaveFlusherUngated) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_remove_flush_wait";
  fs::remove_all(dir);
  fs::create_directories(dir);
  FlushBarrier flush;
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  KvNodeServer* const node = server.get();
  KvNodeServerWiringTestPeer::SetRam(
      *node, [&, node](const BlockKey& key, char* data, size_t len, size_t) {
        flush.Block();
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      });
  const BlockKey key{9300, 0};
  const std::string value = "disk-ack";
  Status written = Status::kInvalid, removed = Status::kInvalid;
  std::thread writer([&] {
    std::string ignored;
    written =
        node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache), key, 0,
                                   0, value.data(), value.size(), &ignored);
  });
  EXPECT_TRUE(flush.AwaitEntry());
  EXPECT_TRUE(KvNodeServerWiringTestPeer::MutationLockHeld(*node, key));
  std::thread remover([&] {
    std::string ignored;
    removed = node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         key, 0, 0, nullptr, 0, &ignored);
  });
  flush.Open();
  writer.join();
  remover.join();
  EXPECT_EQ(written, Status::kOk);
  EXPECT_EQ(removed, Status::kOk);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*node, key));
  std::string out;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node, key, &out),
            Status::kNotFound);
  EXPECT_TRUE(node->Healthy());
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, RamAckRemoveWaitsForFlushWithoutHoldingRamShard) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_ram_ack_remove_wait";
  fs::remove_all(dir);
  fs::create_directories(dir);
  FlushBarrier flush;
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  KvNodeServer* const node = server.get();
  KvNodeServerWiringTestPeer::SetRam(
      *node,
      [&, node](const BlockKey& key, char* data, size_t len, size_t) {
        flush.Block();
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      },
      true);
  const BlockKey key{9400, 0};
  const std::string value = "ram-ack";
  std::string ignored;
  EXPECT_EQ(
      node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache), key, 0,
                                 0, value.data(), value.size(), &ignored),
      Status::kOk);
  EXPECT_TRUE(flush.AwaitEntry());
  Status removed = Status::kInvalid;
  std::atomic<bool> done{false};
  std::thread remover([&] {
    std::string output;
    removed = node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         key, 0, 0, nullptr, 0, &output);
    done.store(true, std::memory_order_release);
  });
  // This observes the actual visibility fence, not an assumed scheduling delay.
  EXPECT_TRUE(WaitFor(
      [&] { return !KvNodeServerWiringTestPeer::Resident(*node, key); }));
  EXPECT_TRUE(KvNodeServerWiringTestPeer::MutationLockHeld(*node, key));
  EXPECT_FALSE(done.load(std::memory_order_acquire));
  flush.Open();
  remover.join();
  EXPECT_EQ(removed, Status::kOk);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*node, key));
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node, key, &ignored),
            Status::kNotFound);
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, OverlappingBatchesAcceptOppositeInputOrder) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_mutation_batch_order";
  fs::remove_all(dir);
  fs::create_directories(dir);
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  const BlockKey first{9500, 0}, second{9501, 0};
  alignas(4096) char value[4096] = "same";
  const std::vector<StoreEngine::CacheBatchItem> forward{
      {first, value, 4, sizeof(value)}, {second, value, 4, sizeof(value)}};
  const std::vector<StoreEngine::CacheBatchItem> reverse{
      {second, value, 4, sizeof(value)}, {first, value, 4, sizeof(value)}};
  std::mutex mutex;
  std::condition_variable cv;
  size_t ready = 0;
  bool start = false;
  std::vector<Status> first_results, second_results;
  const auto run = [&](const std::vector<StoreEngine::CacheBatchItem>& items,
                       std::vector<Status>* results) {
    {
      std::unique_lock<std::mutex> lock(mutex);
      ++ready;
      cv.notify_all();
      cv.wait(lock, [&] { return start; });
    }
    *results = server->CacheDirectBatchForKeys(items);
  };
  std::thread one([&] { run(forward, &first_results); });
  std::thread two([&] { run(reverse, &second_results); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return ready == 2; }));
    start = true;
  }
  cv.notify_all();
  one.join();
  two.join();
  EXPECT_EQ(first_results, std::vector<Status>(2, Status::kOk));
  EXPECT_EQ(second_results, std::vector<Status>(2, Status::kOk));
  std::string persisted;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, first, &persisted),
            Status::kOk);
  EXPECT_EQ(persisted, "same");
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, second, &persisted),
            Status::kOk);
  EXPECT_EQ(persisted, "same");
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, LateSynchronousReadCannotRepublishAfterRemove) {
  for (const bool coalesce : {false, true}) {
    for (const bool replace : {false, true}) {
      SCOPED_TRACE("coalesce=" + std::to_string(coalesce) +
                   " replace=" + std::to_string(replace));
      const fs::path dir = fs::temp_directory_path() / "dfkv_late_sync_read";
      FlushBarrier old_read;
      std::unique_ptr<KvNodeServer> server =
          ReadFenceServer(dir, coalesce, &old_read);
      const BlockKey key{9600, 0};
      ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, "old", 3),
                Status::kOk);
      alignas(4096) char buffer[4096]{};
      Status result = Status::kInvalid;
      std::string bytes;
      std::thread reader([&] {
        const char* data = nullptr;
        size_t length = 0;
        result = server->RangeDirectForKey(key, 0, 3, buffer, sizeof(buffer),
                                           &data, &length);
        if (result == Status::kOk) bytes.assign(data, length);
      });
      EXPECT_TRUE(old_read.AwaitEntry());
      std::string ignored;
      EXPECT_EQ(
          server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                       key, 0, 0, nullptr, 0, &ignored),
          Status::kOk);
      if (replace) {
        EXPECT_EQ(
            server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                         key, 0, 0, "new", 3, &ignored),
            Status::kOk);
      }
      old_read.Open();
      reader.join();
      EXPECT_EQ(result, Status::kOk);
      EXPECT_EQ(bytes, "old");  // The already-read reply remains valid.
      EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
      EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &ignored),
                replace ? Status::kOk : Status::kNotFound);
      if (replace) {
        EXPECT_EQ(ignored, "new");
        const char* data = nullptr;
        size_t length = 0;
        EXPECT_EQ(server->RangeDirectForKey(key, 0, 3, buffer, sizeof(buffer),
                                            &data, &length),
                  Status::kOk);
        EXPECT_NE(data, nullptr);
        if (data != nullptr) {
          EXPECT_EQ(std::string(data, length), "new");
        }
        EXPECT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, key));
      }
      server.reset();
      fs::remove_all(dir);
    }
  }
}

TEST(RamTierWiring, ReadDuringRemoveGapDoesNotBecomeRamResident) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_read_remove_gap";
  FlushBarrier remove_gap;
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true);
  const BlockKey key{9601, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, "old", 3),
            Status::kOk);
  KvNodeServerWiringTestPeer::SetBeforeDiskRemoveHook(
      *server, [&] { remove_gap.Block(); });
  Status removed = Status::kInvalid;
  std::thread remover([&] {
    std::string ignored;
    removed = server->ProcessRequestForKey(
        static_cast<uint8_t>(WireOp::kRemove), key, 0, 0, nullptr, 0, &ignored);
  });
  EXPECT_TRUE(remove_gap.AwaitEntry());
  alignas(4096) char buffer[4096]{};
  const char* data = nullptr;
  size_t length = 0;
  EXPECT_EQ(server->RangeDirectForKey(key, 0, 3, buffer, sizeof(buffer), &data,
                                      &length),
            Status::kOk);
  EXPECT_NE(data, nullptr);
  if (data != nullptr) {
    EXPECT_EQ(std::string(data, length), "old");
  }
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  remove_gap.Open();
  remover.join();
  EXPECT_EQ(removed, Status::kOk);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  std::string out;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &out),
            Status::kNotFound);
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, RegisteredPromotionCompletesWhileRemoveWaits) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_remove_reserved_read";
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true);
  const BlockKey key{9602, 0};
  const std::string value(4096, 'r');
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, value.data(),
                                                value.size()),
            Status::kOk);
  alignas(4096) char staging[4096]{};
  PreparedRead read =
      server->PrepareReadForKey(key, 0, value.size(), staging, sizeof(staging));
  ASSERT_EQ(read.status(), Status::kOk);
  ASSERT_TRUE(read.source_registered());
  ASSERT_EQ(::pread(read.fd(), read.staging(), read.aligned_len(),
                    static_cast<off_t>(read.aligned_off())),
            static_cast<ssize_t>(read.aligned_len()));
  // Storage can finish before the grant/publication decision. Remove must
  // still be completed by Commit, not by destroying the retained SEND owner.
  ASSERT_EQ(read.FinishStorage(Status::kOk, read.payload_len()), Status::kOk);
  EXPECT_EQ(read.fd(), -1);
  EXPECT_GT(server->RamUsedBytes(), 0U);
  FlushBarrier disk_remove;
  KvNodeServerWiringTestPeer::SetBeforeDiskRemoveHook(
      *server, [&] { disk_remove.Block(); });
  std::atomic<bool> done{false};
  Status removed = Status::kInvalid;
  std::thread remover([&] {
    std::string ignored;
    removed = server->ProcessRequestForKey(
        static_cast<uint8_t>(WireOp::kRemove), key, 0, 0, nullptr, 0, &ignored);
    done.store(true, std::memory_order_release);
  });
  EXPECT_TRUE(WaitFor([&] {
    return KvNodeServerWiringTestPeer::MutationLockHeld(*server, key);
  }));
  // Remove owns its foreground stripe but cannot reach disk removal until
  // the registered promotion completes. The barrier is a phase oracle;
  // actual completion, deletion, and retained bytes are checked below.
  EXPECT_FALSE(disk_remove.AwaitEntry());
  EXPECT_FALSE(done.load(std::memory_order_acquire));
  EXPECT_TRUE(read.Commit(Status::kOk, read.payload_len(), 0.0));
  EXPECT_TRUE(disk_remove.AwaitEntry());
  disk_remove.Open();
  remover.join();
  EXPECT_EQ(removed, Status::kOk);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  std::string disk;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &disk),
            Status::kNotFound);
  // Logical deletion cannot invalidate the still-owned SEND source.
  EXPECT_EQ(std::string(read.data(), read.payload_len()), value);
  read = PreparedRead{};
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring,
     FailedPromotionReleasesReservationBeforeReadOwnerDestruction) {
  for (const bool coalesce : {false, true}) {
    SCOPED_TRACE(coalesce);
    const fs::path dir =
        fs::temp_directory_path() / "dfkv_failed_reserved_read";
    std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, coalesce);
    const BlockKey key{9607, 0};
    const std::string value(4096, 'f');
    ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, value.data(),
                                                  value.size()),
              Status::kOk);
    alignas(4096) char staging[4096]{};
    PreparedRead read = server->PrepareReadForKey(key, 0, value.size(), staging,
                                                  sizeof(staging));
    ASSERT_EQ(read.status(), Status::kOk);
    ASSERT_TRUE(read.needs_io());
    ASSERT_TRUE(read.source_registered());
    ASSERT_EQ(::pread(read.fd(), read.staging(), read.aligned_len(),
                      static_cast<off_t>(read.aligned_off())),
              static_cast<ssize_t>(read.aligned_len()));
    ASSERT_GT(server->RamUsedBytes(), 0u);

    // The storage operation has drained. Reporting its terminal failure must
    // finish the hidden RAM writer now, not when this queued read is destroyed.
    ASSERT_TRUE(read.Commit(Status::kIOError, 0, 0.0));
    EXPECT_FALSE(read.owns_cleanup());
    EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
    EXPECT_EQ(server->RamUsedBytes(), 0u);

    FlushBarrier removal_done;
    Status removed = Status::kInvalid;
    std::thread remover([&] {
      std::string ignored;
      removed =
          server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                       key, 0, 0, nullptr, 0, &ignored);
      removal_done.Block();
    });
    const bool completed_with_read_retained = removal_done.AwaitEntry();
    EXPECT_TRUE(completed_with_read_retained)
        << "Failed IO must not leave Remove waiting for the queued read owner";
    // Release a broken implementation's hidden completion before joining.
    // The successful path proves progress without this extra cleanup call.
    if (!completed_with_read_retained) read.Abort();
    removal_done.Open();
    remover.join();
    EXPECT_EQ(removed, Status::kOk);
    EXPECT_EQ(server->RamUsedBytes(), 0u);
    std::string disk;
    EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &disk),
              Status::kNotFound);
    EXPECT_FALSE(read.Abort());
    read = PreparedRead{};
    EXPECT_TRUE(server->Healthy());
    server.reset();
    fs::remove_all(dir);
  }
}

TEST(RamTierWiring, ReadCapturedBeforeRemoveCannotRegisterLatePromotion) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_late_read_prep";
  FlushBarrier preparation;
  std::unique_ptr<KvNodeServer> server =
      ReadFenceServer(dir, true, nullptr, &preparation);
  const BlockKey key{9603, 0};
  const std::string original(4096, 'o'), replacement(4096, 'n');
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, original.data(),
                                                original.size()),
            Status::kOk);
  alignas(4096) char staging[4096]{};
  PreparedRead read;
  std::thread reader([&] {
    read = server->PrepareReadForKey(key, 0, replacement.size(), staging,
                                     sizeof(staging));
  });
  EXPECT_TRUE(preparation.AwaitEntry());
  std::string ignored;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         key, 0, 0, nullptr, 0, &ignored),
            Status::kOk);
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                         key, 0, 0, replacement.data(),
                                         replacement.size(), &ignored),
            Status::kOk);
  preparation.Open();
  reader.join();
  ASSERT_EQ(read.status(), Status::kOk);
  EXPECT_FALSE(read.source_registered());
  ASSERT_EQ(::pread(read.fd(), read.staging(), read.aligned_len(),
                    static_cast<off_t>(read.aligned_off())),
            static_cast<ssize_t>(read.aligned_len()));
  EXPECT_TRUE(read.Commit(Status::kOk, read.payload_len(), 0.0));
  EXPECT_EQ(std::string(read.data(), read.payload_len()), replacement);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  read = PreparedRead{};
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, LateStagingCompletionCannotRepublishAfterRemove) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_late_staging_read";
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true);
  const BlockKey key{9604, 0}, filler{9605, 0};
  const std::string value(4096, 's'), full_arena(16 * 4096, 'f');
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, value.data(),
                                                value.size()),
            Status::kOk);
  ASSERT_TRUE(
      KvNodeServerWiringTestPeer::InstallDurable(*server, filler, full_arena));
  alignas(4096) char staging[4096]{};
  PreparedRead pinned =
      server->PrepareReadForKey(filler, 0, 4096, staging, sizeof(staging));
  ASSERT_EQ(pinned.status(), Status::kOk);
  PreparedRead read =
      server->PrepareReadForKey(key, 0, value.size(), staging, sizeof(staging));
  ASSERT_EQ(read.status(), Status::kOk);
  ASSERT_FALSE(read.source_registered());  // Pinned filler forces staging.
  ASSERT_EQ(::pread(read.fd(), read.staging(), read.aligned_len(),
                    static_cast<off_t>(read.aligned_off())),
            static_cast<ssize_t>(read.aligned_len()));
  std::string ignored;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         key, 0, 0, nullptr, 0, &ignored),
            Status::kOk);
  // A different thread avoids the coalescer's same-thread recursion fallback.
  // These reads must miss independently, not join the still-live old flight and
  // obtain NotFound only after its timeout fallback.
  const long timeouts =
      MetricVal(server->MetricsText(), "dfkv_read_coalesce_timeouts_total");
  std::thread following_reader([&] {
    alignas(4096) char following[4096]{};
    size_t following_length = 0;
    const char* following_data = nullptr;
    EXPECT_EQ(server->RangeIntoForKey(key, 0, value.size(), following,
                                      sizeof(following), &following_length),
              Status::kNotFound);
    EXPECT_EQ(server->RangeDirectForKey(key, 0, value.size(), following,
                                        sizeof(following), &following_data,
                                        &following_length),
              Status::kNotFound);
    std::string output;
    EXPECT_EQ(
        server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange), key,
                                     0, value.size(), nullptr, 0, &output),
        Status::kNotFound);
    const PreparedRead following_read = server->PrepareReadForKey(
        key, 0, value.size(), following, sizeof(following));
    EXPECT_EQ(following_read.status(), Status::kNotFound);
  });
  following_reader.join();
  EXPECT_EQ(
      MetricVal(server->MetricsText(), "dfkv_read_coalesce_timeouts_total"),
      timeouts);
  EXPECT_TRUE(KvNodeServerWiringTestPeer::DropRam(*server, filler));
  pinned = PreparedRead{};  // Space is available when the old read completes.
  EXPECT_TRUE(read.Commit(Status::kOk, read.payload_len(), 0.0));
  EXPECT_EQ(std::string(read.data(), read.payload_len()), value);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  read = PreparedRead{};
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, ExhaustedReadEpochNeverWrapsOrPromotes) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_read_epoch_limit";
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true);
  const BlockKey key{9606, 0};
  const std::string value(4096, 'e');
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, value.data(),
                                                value.size()),
            Status::kOk);
  const uint64_t exhausted = std::numeric_limits<uint64_t>::max();
  KvNodeServerWiringTestPeer::SetReadEpoch(*server, key, exhausted - 1);
  std::string ignored;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         key, 0, 0, nullptr, 0, &ignored),
            Status::kOk);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, key), exhausted);
  ASSERT_EQ(
      server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache), key, 0,
                                   0, value.data(), value.size(), &ignored),
      Status::kOk);
  alignas(4096) char staging[4096]{};
  const char* data = nullptr;
  size_t length = 0;
  ASSERT_EQ(server->RangeDirectForKey(key, 0, value.size(), staging,
                                      sizeof(staging), &data, &length),
            Status::kOk);
  EXPECT_EQ(std::string(data, length), value);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  PreparedRead read =
      server->PrepareReadForKey(key, 0, value.size(), staging, sizeof(staging));
  ASSERT_EQ(read.status(), Status::kOk);
  EXPECT_FALSE(read.source_registered());
  ASSERT_EQ(::pread(read.fd(), read.staging(), read.aligned_len(),
                    static_cast<off_t>(read.aligned_off())),
            static_cast<ssize_t>(read.aligned_len()));
  EXPECT_TRUE(read.Commit(Status::kOk, read.payload_len(), 0.0));
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  read = PreparedRead{};
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         key, 0, 0, nullptr, 0, &ignored),
            Status::kOk);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, key), exhausted);
  EXPECT_TRUE(server->Healthy());
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, FailedAndMissingRemoveCloseSharedStripeEpoch) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_remove_epoch_errors";
  fs::remove_all(dir);
  fs::create_directories(dir);
  ReadBarrierStore* store = nullptr;
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  options.engine_factory = [&](const std::string& path, uint64_t capacity) {
    std::unique_ptr<ReadBarrierStore> backend =
        std::make_unique<ReadBarrierStore>(path, capacity, nullptr, nullptr);
    store = backend.get();
    return backend;
  };
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  KvNodeServerWiringTestPeer::SetRam(
      *server, [](const BlockKey&, char*, size_t, size_t) { return false; });
  const BlockKey absent{9607, 0}, collision{10631, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, collision, "kept", 4),
            Status::kOk);
  std::string ignored;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         absent, 0, 0, nullptr, 0, &ignored),
            Status::kNotFound);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, collision), 2u);
  ASSERT_NE(store, nullptr);
  store->fail_remove_ = true;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                         collision, 0, 0, nullptr, 0, &ignored),
            Status::kIOError);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, absent), 4u);
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, collision, &ignored),
            Status::kOk);
  EXPECT_EQ(ignored, "kept");
  alignas(4096) char buffer[4096]{};
  const char* data = nullptr;
  size_t length = 0;
  ASSERT_EQ(server->RangeDirectForKey(collision, 0, 4, buffer, sizeof(buffer),
                                      &data, &length),
            Status::kOk);
  EXPECT_EQ(std::string(data, length), "kept");
  EXPECT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, collision));
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, ForegroundPutRetiresSurvivingDurableRamCopy) {
  for (int mode = 0; mode < 3; ++mode) {
    for (const bool direct : {false, true}) {
      for (const bool pinned : {false, true}) {
        for (const bool disk_retained : {false, true}) {
          SCOPED_TRACE("mode=" + std::to_string(mode) +
                       " direct=" + std::to_string(direct) +
                       " pinned=" + std::to_string(pinned) +
                       " disk_retained=" + std::to_string(disk_retained));
          const fs::path dir =
              fs::temp_directory_path() / "dfkv_put_surviving_durable_ram";
          fs::remove_all(dir);
          fs::create_directories(dir);
          DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
          std::unique_ptr<KvNodeServer> server =
              KvNodeServerWiringTestPeer::Create(std::move(options));
          KvNodeServer* const node = server.get();
          KvNodeServerWiringTestPeer::SetRam(
              *server,
              [node](const BlockKey& key, char* data, size_t len, size_t) {
                return KvNodeServerWiringTestPeer::Persist(*node, key, data,
                                                           len) == Status::kOk;
              },
              mode == 1);
          KvNodeServerWiringTestPeer::SetWriteBack(*server, mode != 2);
          const BlockKey key{9700, 0};
          const std::string original = "original", replacement = "modified";
          ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(
                        *server, key, original.data(), original.size()),
                    Status::kOk);
          // A real whole disk read creates the durable RAM copy. Disk-only
          // removal models its independently evictable backing copy; it is not
          // a claim that this test exercises allocator pressure itself.
          alignas(4096) char staging[4096]{};
          const char* data = nullptr;
          size_t length = 0;
          ASSERT_EQ(server->RangeDirectForKey(key, 0, original.size(), staging,
                                              sizeof(staging), &data, &length),
                    Status::kOk);
          ASSERT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, key));
          PreparedRead old_send;
          if (pinned) {
            old_send = server->PrepareReadForKey(key, 0, original.size(),
                                                 staging, sizeof(staging));
            ASSERT_EQ(old_send.status(), Status::kOk);
            ASSERT_TRUE(old_send.source_registered());
          }
          if (!disk_retained) {
            ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, key),
                      Status::kOk);
          }
          alignas(4096) char incoming[4096]{};
          std::memcpy(incoming, replacement.data(), replacement.size());
          FlushBarrier put_done;
          Status status = Status::kInvalid;
          std::thread writer([&] {
            std::string ignored;
            status = direct ? server->CacheDirectForKey(key, incoming,
                                                        replacement.size(),
                                                        sizeof(incoming))
                            : server->ProcessRequestForKey(
                                  static_cast<uint8_t>(WireOp::kCache), key, 0,
                                  0, incoming, replacement.size(), &ignored);
            put_done.Block();
          });
          const bool completed_with_send_pin = put_done.AwaitEntry();
          EXPECT_TRUE(completed_with_send_pin)
              << "A foreground PUT must not wait for an old SEND release";
          // Recover a broken implementation's wait before joining its thread.
          if (!completed_with_send_pin) old_send = PreparedRead{};
          put_done.Open();
          writer.join();
          EXPECT_EQ(status, Status::kOk);
          ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
          const std::string& expected = disk_retained ? original : replacement;
          std::string persisted, visible;
          EXPECT_EQ(
              KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
              Status::kOk);
          EXPECT_EQ(persisted, expected);
          EXPECT_EQ(
              server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                           key, 0, 0, nullptr, 0, &visible),
              Status::kOk);
          EXPECT_EQ(visible, expected);
          if (pinned && completed_with_send_pin) {
            EXPECT_EQ(std::string(old_send.data(), old_send.payload_len()),
                      original);
          }
          old_send = PreparedRead{};
          EXPECT_TRUE(server->Healthy());
          server.reset();
          fs::remove_all(dir);
        }
      }
    }
  }
}

TEST(RamTierWiring, SlabLegacyPutReplacesSurvivingDurableRamCopy) {
  const ScopedEnv ram("DFKV_RAM_TIER",
                      "0");  // Install the small RAM tier below.
  const ScopedEnv buffered("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "1048576");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  // Modes: disk ACK, RAM ACK, writearound. Paths: shared handler, direct,
  // batch.
  for (int mode = 0; mode < 3; ++mode) {
    for (int path = 0; path < 3; ++path) {
      for (const bool pinned : {false, true}) {
        SCOPED_TRACE("mode=" + std::to_string(mode) +
                     " path=" + std::to_string(path) +
                     " pinned=" + std::to_string(pinned));
        const fs::path dir = fs::temp_directory_path() / "dfkv_slab_ram_put";
        fs::remove_all(dir);
        fs::create_directories(dir);
        DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "slab"};
        options.eviction_policy = "legacy";
        std::atomic<size_t> flushes{
            0};  // Outlives the server's flush callback.
        std::unique_ptr<KvNodeServer> server =
            KvNodeServerWiringTestPeer::Create(std::move(options));
        ASSERT_TRUE(server->Healthy());
        ASSERT_EQ(KvNodeServerWiringTestPeer::DiskEngine(*server), "slab");
        ASSERT_TRUE(fs::exists(dir / "slab_meta"));
        ASSERT_TRUE(fs::exists(dir / "slots.tbl"));
        KvNodeServer* const node = server.get();
        KvNodeServerWiringTestPeer::SetRam(
            *server,
            [node, &flushes](const BlockKey& key, char* data, size_t len,
                             size_t) {
              flushes.fetch_add(1, std::memory_order_relaxed);
              return KvNodeServerWiringTestPeer::Persist(*node, key, data,
                                                         len) == Status::kOk;
            },
            mode == 1);
        KvNodeServerWiringTestPeer::SetWriteBack(*server, mode != 2);
        const BlockKey key{9900, 0};
        const std::string original(4096, 'o'), replacement(4096, 'n');
        ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(
                      *server, key, original.data(), original.size()),
                  Status::kOk);
        alignas(4096) char staging[4096]{};
        const char* data = nullptr;
        size_t length = 0;
        ASSERT_EQ(server->RangeDirectForKey(key, 0, original.size(), staging,
                                            sizeof(staging), &data, &length),
                  Status::kOk);
        ASSERT_NE(data, nullptr);
        ASSERT_EQ(std::string(data, length), original);
        ASSERT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, key));
        PreparedRead old_send;
        if (pinned) {
          old_send = server->PrepareReadForKey(key, 0, original.size(), staging,
                                               sizeof(staging));
          ASSERT_EQ(old_send.status(), Status::kOk);
          ASSERT_TRUE(old_send.source_registered());
          ASSERT_FALSE(old_send.needs_io());  // RAM pin, not a slab disk lease.
        }
        // Real slab removal models independent backing disappearance; this is
        // deliberately not a pressure-eviction/victim-selection assertion.
        ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, key),
                  Status::kOk);
        std::string persisted;
        ASSERT_EQ(
            KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
            Status::kNotFound);
        ASSERT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, key));
        alignas(4096) char incoming[4096]{};
        std::memcpy(incoming, replacement.data(), replacement.size());
        FlushBarrier done;
        Status status = Status::kInvalid;
        std::thread writer([&] {
          if (path == 0) {
            std::string ignored;
            status = server->ProcessRequestForKey(
                static_cast<uint8_t>(WireOp::kCache), key, 0, 0, incoming,
                replacement.size(), &ignored);
          } else if (path == 1) {
            status = server->CacheDirectForKey(
                key, incoming, replacement.size(), sizeof(incoming));
          } else {
            const std::vector<Status> results = server->CacheDirectBatchForKeys(
                {{key, incoming, replacement.size(), sizeof(incoming)}});
            if (results.size() == 1) status = results[0];
          }
          done.Block();
        });
        const bool completed_with_pin = done.AwaitEntry();
        EXPECT_TRUE(completed_with_pin) << "PUT must not wait for old RAM SEND";
        if (!completed_with_pin) old_send = PreparedRead{};
        done.Open();
        writer.join();
        EXPECT_EQ(status, Status::kOk);
        ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
        const bool new_ram_owner = mode != 2 && path != 2 && !pinned;
        EXPECT_EQ(flushes.load(std::memory_order_relaxed),
                  new_ram_owner ? 1u : 0u);
        EXPECT_EQ(KvNodeServerWiringTestPeer::Resident(*server, key),
                  new_ram_owner);
        EXPECT_EQ(
            KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
            Status::kOk);
        EXPECT_EQ(persisted, replacement);
        std::string visible;
        EXPECT_EQ(
            server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                         key, 0, 0, nullptr, 0, &visible),
            Status::kOk);
        EXPECT_EQ(visible, replacement);
        if (pinned && completed_with_pin) {
          EXPECT_EQ(std::string(old_send.data(), old_send.payload_len()),
                    original);
        }
        old_send = PreparedRead{};
        EXPECT_TRUE(server->Healthy());
        server.reset();
        fs::remove_all(dir);
      }
    }
  }
}

TEST(RamTierWiring, LateDiskReadCannotRepublishAcrossForegroundPut) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_put_late_disk_read";
  FlushBarrier old_read;
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true, &old_read);
  const BlockKey key{9701, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, "old", 3),
            Status::kOk);
  alignas(4096) char staging[4096]{};
  Status result = Status::kInvalid;
  std::string bytes;
  std::thread reader([&] {
    const char* data = nullptr;
    size_t length = 0;
    result = server->RangeDirectForKey(key, 0, 3, staging, sizeof(staging),
                                       &data, &length);
    if (result == Status::kOk) bytes.assign(data, length);
  });
  EXPECT_TRUE(old_read.AwaitEntry());
  EXPECT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, key), Status::kOk);
  std::string output;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                         key, 0, 0, "new", 3, &output),
            Status::kOk);
  old_read.Open();
  reader.join();
  EXPECT_EQ(result, Status::kOk);
  EXPECT_EQ(bytes, "old");
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                         key, 0, 0, nullptr, 0, &output),
            Status::kOk);
  EXPECT_EQ(output, "new");
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, ForegroundPutDoesNotJoinOldPromotionCompletion) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_put_old_promotion";
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true);
  KvNodeServer* const node = server.get();
  KvNodeServerWiringTestPeer::SetRam(
      *server, [node](const BlockKey& key, char* data, size_t len, size_t) {
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      });
  const BlockKey key{9702, 0};
  const std::string original(4096, 'o'), replacement(4096, 'n');
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, original.data(),
                                                original.size()),
            Status::kOk);
  alignas(4096) char staging[4096]{};
  PreparedRead old_read = server->PrepareReadForKey(key, 0, original.size(),
                                                    staging, sizeof(staging));
  ASSERT_EQ(old_read.status(), Status::kOk);
  ASSERT_TRUE(old_read.source_registered());
  ASSERT_EQ(::pread(old_read.fd(), old_read.staging(), old_read.aligned_len(),
                    static_cast<off_t>(old_read.aligned_off())),
            static_cast<ssize_t>(old_read.aligned_len()));
  ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, key), Status::kOk);
  FlushBarrier put_done;
  Status status = Status::kInvalid;
  std::thread writer([&] {
    std::string ignored;
    status = server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                          key, 0, 0, replacement.data(),
                                          replacement.size(), &ignored);
    put_done.Block();
  });
  EXPECT_TRUE(put_done.AwaitEntry())
      << "New PUT must not share a read-promotion completion";
  // Complete even after a failed progress assertion to release a broken waiter.
  EXPECT_TRUE(old_read.Commit(Status::kOk, old_read.payload_len(), 0.0));
  put_done.Open();
  writer.join();
  EXPECT_EQ(status, Status::kOk);
  std::string visible;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                         key, 0, 0, nullptr, 0, &visible),
            Status::kOk);
  EXPECT_EQ(visible, replacement);
  EXPECT_EQ(std::string(old_read.data(), old_read.payload_len()), original);
  old_read = PreparedRead{};
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, RetiredPinnedRamCannotBypassPositiveDirtyWatermark) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_put_retired_watermark";
  fs::remove_all(dir);
  fs::create_directories(dir);
  FlushBarrier flush;
  std::atomic<size_t> writes{0};
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  options.engine_factory = [&](const std::string& path, uint64_t capacity) {
    return std::make_unique<CountingStore>(path, capacity, writes);
  };
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  const OpenFlushOnExit cleanup{flush};
  KvNodeServer* const node = server.get();
  KvNodeServerWiringTestPeer::SetRam(
      *server,
      [node, &flush](const BlockKey& key, char* data, size_t len, size_t) {
        flush.Block();
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      },
      true, 6);
  const BlockKey retired{9703, 0}, dirty{9704, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, retired, "old", 3),
            Status::kOk);
  ASSERT_TRUE(
      KvNodeServerWiringTestPeer::InstallDurable(*server, retired, "old"));
  alignas(4096) char staging[4096]{};
  PreparedRead old_send =
      server->PrepareReadForKey(retired, 0, 3, staging, sizeof(staging));
  ASSERT_EQ(old_send.status(), Status::kOk);
  ASSERT_TRUE(old_send.source_registered());
  ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, retired),
            Status::kOk);
  const std::string dirty_value(4096, 'd');
  std::string ignored;
  ASSERT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                         dirty, 0, 0, dirty_value.data(),
                                         dirty_value.size(), &ignored),
            Status::kOk);
  ASSERT_TRUE(flush.AwaitEntry());
  const size_t writes_before = writes.load(std::memory_order_relaxed);
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                         retired, 0, 0, "new", 3, &ignored),
            Status::kCacheFull);
  EXPECT_EQ(writes.load(std::memory_order_relaxed), writes_before);
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, retired, &ignored),
            Status::kNotFound);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, retired));
  EXPECT_EQ(std::string(old_send.data(), old_send.payload_len()), "old");
  old_send = PreparedRead{};
  flush.Open();
  EXPECT_TRUE(KvNodeServerWiringTestPeer::Drain(*server));
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, BatchPutRetiresSurvivingDurableRamCopy) {
  for (int mode = 0; mode < 3; ++mode) {
    for (const bool pinned : {false, true}) {
      SCOPED_TRACE("mode=" + std::to_string(mode) +
                   " pinned=" + std::to_string(pinned));
      const fs::path dir = fs::temp_directory_path() / "dfkv_batch_old_ram";
      fs::remove_all(dir);
      fs::create_directories(dir);
      DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
      std::unique_ptr<KvNodeServer> server =
          KvNodeServerWiringTestPeer::Create(std::move(options));
      KvNodeServer* const node = server.get();
      KvNodeServerWiringTestPeer::SetRam(
          *server,
          [node](const BlockKey& key, char* data, size_t len, size_t) {
            return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
                   Status::kOk;
          },
          mode == 1);
      KvNodeServerWiringTestPeer::SetWriteBack(*server, mode != 2);
      const BlockKey key{9800, 0};
      ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, "old", 3),
                Status::kOk);
      alignas(4096) char staging[4096]{};
      const char* data = nullptr;
      size_t length = 0;
      ASSERT_EQ(server->RangeDirectForKey(key, 0, 3, staging, sizeof(staging),
                                          &data, &length),
                Status::kOk);
      ASSERT_TRUE(KvNodeServerWiringTestPeer::Resident(*server, key));
      PreparedRead old_send;
      if (pinned) {
        old_send =
            server->PrepareReadForKey(key, 0, 3, staging, sizeof(staging));
        ASSERT_EQ(old_send.status(), Status::kOk);
        ASSERT_TRUE(old_send.source_registered());
      }
      // Remove only the independent backing copy, not the RAM resident.
      ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, key),
                Status::kOk);
      alignas(4096) char incoming[4096] = "new";
      FlushBarrier done;
      std::vector<Status> results;
      std::thread writer([&] {
        results = server->CacheDirectBatchForKeys(
            {{key, incoming, 3, sizeof(incoming)}});
        done.Block();
      });
      const bool completed_with_pin = done.AwaitEntry();
      EXPECT_TRUE(completed_with_pin) << "Batch must not wait for an old SEND";
      if (!completed_with_pin) old_send = PreparedRead{};
      done.Open();
      writer.join();
      EXPECT_EQ(results, std::vector<Status>{Status::kOk});
      std::string persisted, visible;
      EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
                Status::kOk);
      EXPECT_EQ(persisted, "new");
      EXPECT_EQ(
          server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                       key, 0, 0, nullptr, 0, &visible),
          Status::kOk);
      EXPECT_EQ(visible, persisted);
      if (pinned && completed_with_pin) {
        EXPECT_EQ(std::string(old_send.data(), old_send.payload_len()), "old");
      }
      old_send = PreparedRead{};
      server.reset();
      fs::remove_all(dir);
    }
  }
}

TEST(RamTierWiring, BatchPutJoinsDirtyFirstOwnerAndWaitsForDisk) {
  for (const bool ram_ack : {false, true}) {
    for (const bool flush_success : {false, true}) {
      SCOPED_TRACE("ram_ack=" + std::to_string(ram_ack) +
                   " flush_success=" + std::to_string(flush_success));
      const fs::path dir = fs::temp_directory_path() / "dfkv_batch_dirty_owner";
      fs::remove_all(dir);
      fs::create_directories(dir);
      FlushBarrier flush, done;
      std::atomic<size_t> writes{0};
      DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
      options.engine_factory = [&](const std::string& path, uint64_t capacity) {
        return std::make_unique<CountingStore>(path, capacity, writes);
      };
      std::unique_ptr<KvNodeServer> server =
          KvNodeServerWiringTestPeer::Create(std::move(options));
      const OpenFlushOnExit cleanup{flush};
      KvNodeServer* const node = server.get();
      KvNodeServerWiringTestPeer::SetRam(
          *server,
          [node, &flush, flush_success](const BlockKey& key, char* data,
                                        size_t len, size_t) {
            flush.Block();
            return flush_success && KvNodeServerWiringTestPeer::Persist(
                                        *node, key, data, len) == Status::kOk;
          },
          true);
      const BlockKey key{9801, 0};
      std::string ignored;
      ASSERT_EQ(
          server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                       key, 0, 0, "first", 5, &ignored),
          Status::kOk);
      ASSERT_TRUE(flush.AwaitEntry());
      // Only the foreground policy changes, between requests. The already
      // accepted owner and its actual blocked flusher remain the same.
      KvNodeServerWiringTestPeer::SetRamAck(*server, ram_ack);
      alignas(4096) char conflicting[4096] = "second";
      std::vector<Status> results;
      std::thread writer([&] {
        results = server->CacheDirectBatchForKeys(
            {{key, conflicting, 6, sizeof(conflicting)}});
        done.Block();
      });
      // Batch completion means disk completion even when single PUTs use RAM
      // ACK.
      EXPECT_FALSE(done.AwaitEntry());
      EXPECT_EQ(writes.load(std::memory_order_relaxed), 0u);
      flush.Open();
      done.Open();
      writer.join();
      EXPECT_EQ(results, std::vector<Status>{flush_success ? Status::kOk
                                                           : Status::kIOError});
      EXPECT_EQ(writes.load(std::memory_order_relaxed),
                flush_success ? 1u : 0u);
      std::string persisted;
      EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
                flush_success ? Status::kOk : Status::kNotFound);
      if (flush_success) {
        EXPECT_EQ(persisted, "first");
      }
      EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, key) & 1, 0u);
      server.reset();
      fs::remove_all(dir);
    }
  }
}

TEST(RamTierWiring, BatchPutKeepsDiskCompletionAndOrderedItemResults) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_batch_item_results";
  fs::remove_all(dir);
  fs::create_directories(dir);
  FlushBarrier flush, done;
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  const OpenFlushOnExit cleanup{flush};
  KvNodeServer* const node = server.get();
  KvNodeServerWiringTestPeer::SetRam(
      *server,
      [node, &flush](const BlockKey& key, char* data, size_t len, size_t) {
        flush.Block();
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      },
      true, 6);
  const BlockKey dirty{9802, 0}, fresh{9803, 0}, collision{9803 + 1024, 0};
  const std::string first(4096, 'f');
  std::string ignored;
  ASSERT_EQ(
      server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache), dirty,
                                   0, 0, first.data(), first.size(), &ignored),
      Status::kOk);
  ASSERT_TRUE(flush.AwaitEntry());
  alignas(4096) char incoming[4096] = "new";
  alignas(4096) char duplicate[4096] = "duplicate";
  const std::vector<StoreEngine::CacheBatchItem> items{
      {fresh, incoming, 3, sizeof(incoming)},
      {fresh, nullptr, 3, 0},
      {dirty, incoming, 3, sizeof(incoming)},
      {collision, incoming, 3, sizeof(incoming)},
      {fresh, duplicate, 9, sizeof(duplicate)}};
  const uint64_t fresh_epoch =
      KvNodeServerWiringTestPeer::ReadEpoch(*server, fresh);
  const uint64_t dirty_epoch =
      KvNodeServerWiringTestPeer::ReadEpoch(*server, dirty);
  const uint64_t collision_epoch =
      KvNodeServerWiringTestPeer::ReadEpoch(*server, collision);
  std::vector<Status> results;
  std::thread writer([&] {
    results = server->CacheDirectBatchForKeys(items);
    done.Block();
  });
  EXPECT_FALSE(done.AwaitEntry())
      << "Batch must not early-ACK at the watermark";
  EXPECT_TRUE(KvNodeServerWiringTestPeer::MutationLockHeld(*server, fresh));
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, fresh),
            fresh_epoch + 1);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, dirty),
            dirty_epoch + 1);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, collision),
            collision_epoch + 1);
  flush.Open();
  done.Open();
  writer.join();
  EXPECT_EQ(results,
            (std::vector<Status>{Status::kOk, Status::kInvalid, Status::kOk,
                                 Status::kOk, Status::kOk}));
  for (const BlockKey& key : {fresh, dirty, collision}) {
    std::string persisted;
    EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
              Status::kOk);
    EXPECT_EQ(persisted, key == dirty ? first : "new");
    EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, key) & 1, 0u);
  }
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, fresh),
            fresh_epoch + 2);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, dirty),
            dirty_epoch + 2);
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, collision),
            collision_epoch + 2);
  EXPECT_TRUE(server->CacheDirectBatchForKeys({}).empty());
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, LateDiskReadCannotRepublishAcrossBatchPut) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_batch_late_read";
  FlushBarrier old_read;
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true, &old_read);
  const BlockKey key{9804, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, "old", 3),
            Status::kOk);
  alignas(4096) char staging[4096]{};
  Status result = Status::kInvalid;
  std::string bytes;
  std::thread reader([&] {
    const char* data = nullptr;
    size_t length = 0;
    result = server->RangeDirectForKey(key, 0, 3, staging, sizeof(staging),
                                       &data, &length);
    if (result == Status::kOk) bytes.assign(data, length);
  });
  EXPECT_TRUE(old_read.AwaitEntry());
  EXPECT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, key), Status::kOk);
  alignas(4096) char incoming[4096] = "new";
  EXPECT_EQ(
      server->CacheDirectBatchForKeys({{key, incoming, 3, sizeof(incoming)}}),
      std::vector<Status>{Status::kOk});
  old_read.Open();
  reader.join();
  EXPECT_EQ(result, Status::kOk);
  EXPECT_EQ(bytes, "old");
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
  std::string visible;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                         key, 0, 0, nullptr, 0, &visible),
            Status::kOk);
  EXPECT_EQ(visible, "new");
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, BatchPutDoesNotJoinOldPromotionCompletion) {
  const fs::path dir = fs::temp_directory_path() / "dfkv_batch_old_promotion";
  std::unique_ptr<KvNodeServer> server = ReadFenceServer(dir, true);
  KvNodeServerWiringTestPeer::SetWriteBack(*server, true);
  const BlockKey key{9805, 0};
  const std::string original(4096, 'o');
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server, key, original.data(),
                                                original.size()),
            Status::kOk);
  alignas(4096) char staging[4096]{};
  PreparedRead old_read = server->PrepareReadForKey(key, 0, original.size(),
                                                    staging, sizeof(staging));
  ASSERT_EQ(old_read.status(), Status::kOk);
  ASSERT_TRUE(old_read.source_registered());
  ASSERT_EQ(::pread(old_read.fd(), old_read.staging(), old_read.aligned_len(),
                    static_cast<off_t>(old_read.aligned_off())),
            static_cast<ssize_t>(old_read.aligned_len()));
  ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*server, key), Status::kOk);
  alignas(4096) char incoming[4096]{};
  std::memset(incoming, 'n', sizeof(incoming));
  FlushBarrier done;
  std::vector<Status> results;
  std::thread writer([&] {
    results = server->CacheDirectBatchForKeys(
        {{key, incoming, sizeof(incoming), sizeof(incoming)}});
    done.Block();
  });
  EXPECT_TRUE(done.AwaitEntry()) << "Batch must not wait for an old promotion";
  EXPECT_TRUE(old_read.Commit(Status::kOk, old_read.payload_len(), 0.0));
  done.Open();
  writer.join();
  EXPECT_EQ(results, std::vector<Status>{Status::kOk});
  std::string visible;
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                         key, 0, 0, nullptr, 0, &visible),
            Status::kOk);
  EXPECT_EQ(visible, std::string(sizeof(incoming), 'n'));
  EXPECT_EQ(std::string(old_read.data(), old_read.payload_len()), original);
  old_read = PreparedRead{};
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, BatchPutKeepsFirstPayloadAcrossCapacityBypass) {
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_batch_bypass_duplicate";
  fs::remove_all(dir);
  fs::create_directories(dir);
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  KvNodeServer* const node = server.get();
  KvNodeServerWiringTestPeer::SetRam(
      *server, [node](const BlockKey& key, char* data, size_t len, size_t) {
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      });
  const BlockKey key{9806, 0}, collision{9806 + 1024, 0};
  // The first payload exceeds the entire 64 KiB RAM budget. A later fitting
  // duplicate must not flush its different bytes ahead of this disk bypass.
  alignas(4096) char oversized[68 * 1024];
  std::memset(oversized, 'f', sizeof(oversized));
  alignas(4096) char conflicting[4096] = "second";
  const std::vector<StoreEngine::CacheBatchItem> items{
      {key, oversized, sizeof(oversized), sizeof(oversized)},
      {key, conflicting, 6, sizeof(conflicting)},
      {collision, conflicting, 6, sizeof(conflicting)}};
  EXPECT_EQ(server->CacheDirectBatchForKeys(items),
            std::vector<Status>(3, Status::kOk));
  std::string persisted, visible;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
            Status::kOk);
  EXPECT_EQ(persisted, std::string(sizeof(oversized), 'f'));
  EXPECT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                         key, 0, 0, nullptr, 0, &visible),
            Status::kOk);
  EXPECT_EQ(visible, persisted);
  EXPECT_EQ(
      KvNodeServerWiringTestPeer::DiskRead(*server, collision, &persisted),
      Status::kOk);
  EXPECT_EQ(persisted, "second");
  EXPECT_EQ(KvNodeServerWiringTestPeer::ReadEpoch(*server, key) & 1, 0u);
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, BatchPutPreservesTypedDiskFailuresWithoutRamFlush) {
  class FaultStore : public KVStore {
   public:
    using KVStore::KVStore;
    Status CacheDirect(const BlockKey& key, char* data, size_t len,
                       size_t cap) override {
      if (key.digest_hi == 9810) return Status::kIOError;
      if (key.digest_hi == 9811) return Status::kCacheFull;
      return KVStore::CacheDirect(key, data, len, cap);
    }
  };
  const fs::path dir = fs::temp_directory_path() / "dfkv_batch_typed_failures";
  fs::remove_all(dir);
  fs::create_directories(dir);
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  options.engine_factory = [](const std::string& path, uint64_t capacity) {
    return std::make_unique<FaultStore>(KVStore::Options{path, capacity});
  };
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  std::atomic<size_t> flushes{0};
  KvNodeServerWiringTestPeer::SetRam(
      *server,
      [&flushes](const BlockKey&, char*, size_t, size_t) {
        flushes.fetch_add(1, std::memory_order_relaxed);
        return false;
      },
      true);
  alignas(4096) char incoming[4096] = "new";
  const BlockKey io_error{9810, 0}, full{9811, 0}, invalid{9812, 0},
      ok{9813, 0};
  const std::vector<StoreEngine::CacheBatchItem> items{
      {io_error, incoming, 3, sizeof(incoming)},
      {ok, incoming, 3, sizeof(incoming)},
      {invalid, nullptr, 3, 0},
      {full, incoming, 3, sizeof(incoming)}};
  EXPECT_EQ(server->CacheDirectBatchForKeys(items),
            (std::vector<Status>{Status::kIOError, Status::kOk,
                                 Status::kInvalid, Status::kCacheFull}));
  std::string persisted;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, ok, &persisted),
            Status::kOk);
  EXPECT_EQ(persisted, "new");
  for (const BlockKey& key : {io_error, full, invalid})
    EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, key, &persisted),
              Status::kNotFound);
  EXPECT_EQ(flushes.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(server->RamUsedBytes(), 0u);
  EXPECT_EQ(MetricVal(server->MetricsText(), "dfkv_ram_flush_backlog"), 0);
  EXPECT_TRUE(server->Healthy());
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, BatchPutWithRamPreservesTenantQuotaAndDuplicateOrder) {
  const ScopedEnv quota("DFKV_TENANT_DEFAULT_QUOTA_BYTES", "3");
  const ScopedEnv overrides("DFKV_TENANT_QUOTAS_FILE", "");
  const fs::path dir = fs::temp_directory_path() / "dfkv_batch_ram_quota";
  fs::remove_all(dir);
  fs::create_directories(dir);
  DiskCacheGroup::Options options{{dir.string()}, 1ull << 30, "file"};
  std::unique_ptr<KvNodeServer> server =
      KvNodeServerWiringTestPeer::Create(std::move(options));
  KvNodeServerWiringTestPeer::SetRam(
      *server, [](const BlockKey&, char*, size_t, size_t) { return false; },
      true);
  const BlockKey first{9820, 0, 41}, rejected{9821, 0, 41};
  alignas(4096) char incoming[4096] = "new";
  alignas(4096) char conflicting[4096] = "other";
  EXPECT_EQ(
      server->CacheDirectBatchForKeys(
          {{first, incoming, 3, sizeof(incoming)},
           {rejected, incoming, 3, sizeof(incoming)},
           {first, conflicting, 5, sizeof(conflicting)}}),
      (std::vector<Status>{Status::kOk, Status::kQuotaExceeded, Status::kOk}));
  std::string persisted;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, first, &persisted),
            Status::kOk);
  EXPECT_EQ(persisted, "new");
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server, rejected, &persisted),
            Status::kNotFound);
  EXPECT_EQ(server->RamUsedBytes(), 0u);
  EXPECT_TRUE(server->Healthy());
  server.reset();
  fs::remove_all(dir);
}

TEST(RamTierWiring, TrackedSyncPromotionKeepsSourceAndRejectsLatePublication) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  for (const unsigned char scenario : {0, 1, 2, 3, 4, 5}) {
    const bool replace = (scenario & 1) != 0;
    const bool coalesce = scenario >= 2;
    SCOPED_TRACE(coalesce);
    SCOPED_TRACE(replace);
    const fs::path dir = fs::temp_directory_path() / "dfkv_tracked_sync_source";
    fs::remove_all(dir);
    FlushBarrier gate;  // Outlives the server and any controlled callback.
    FlushBarrier late_gate;
    TrackedReadBarrierStore* store = nullptr;
    DiskCacheGroup::Options options{{dir.string()}, 1U << 20, "slab"};
    options.eviction_policy = "legacy";
    options.engine_factory = [&](const std::string& path, uint64_t capacity) {
      DiskSlabStore::Options slab;
      slab.dir = path;
      slab.capacity_bytes = capacity;
      slab.extent_bytes = 1U << 20;
      slab.slot_granularity = 4096;
      slab.reclaim_interval_ms = slab.table_sync_ms = 0;
      slab.track_resident_identity = true;
      std::unique_ptr<TrackedReadBarrierStore> result =
          std::make_unique<TrackedReadBarrierStore>(slab);
      store = result.get();
      return result;
    };
    std::unique_ptr<KvNodeServer> server =
        KvNodeServerWiringTestPeer::Create(std::move(options));
    ASSERT_TRUE(server->Healthy());
    KvNodeServerWiringTestPeer::SetRam(
        *server, [](const BlockKey&, char*, size_t, size_t) { return false; });
    KvNodeServerWiringTestPeer::SetWriteBack(*server, false);
    KvNodeServerWiringTestPeer::SetCoalescing(*server, coalesce);
    const BlockKey key{9820, 0};
    const DiskWriteResult committed =
        KvNodeServerWiringTestPeer::PersistSource(*server, key, "old", 3);
    ASSERT_TRUE(committed.identity.valid());
    alignas(4096) char buffer[4096] = {};
    const char* data = nullptr;
    size_t length = 0;
    ASSERT_EQ(server->RangeDirectForKey(key, 0, 0, buffer, sizeof(buffer),
                                        &data, &length),
              Status::kOk);
    EXPECT_EQ(KvNodeServerWiringTestPeer::RamSource(*server, key),
              committed.identity);
    ASSERT_TRUE(KvNodeServerWiringTestPeer::DropRam(*server, key));
    if (coalesce) {
      // TCP/Into leaders and a direct follower share one real slab snapshot.
      store->after_read_ = &gate;
      std::string leader_bytes;
      std::thread leader([&] {
        if (scenario < 4) {
          EXPECT_EQ(server->ProcessRequestForKey(
                        static_cast<uint8_t>(WireOp::kRange), key, 0, 3,
                        nullptr, 0, &leader_bytes),
                    Status::kOk);
        } else {
          char target[4] = {};
          size_t n = 0;
          EXPECT_EQ(
              server->RangeIntoForKey(key, 0, 3, target, sizeof(target), &n),
              Status::kOk);
          leader_bytes.assign(target, n);
        }
      });
      const bool leader_entered = gate.AwaitEntry();
      std::thread follower([&] {
        EXPECT_EQ(server->RangeDirectForKey(key, 0, 3, buffer, sizeof(buffer),
                                            &data, &length),
                  Status::kOk);
      });
      const bool joined = KvNodeServerWiringTestPeer::AwaitCopyWaiter(*server);
      gate.Open();
      leader.join();
      follower.join();
      EXPECT_TRUE(leader_entered);
      EXPECT_TRUE(joined);
      EXPECT_EQ(leader_bytes, "old");
      EXPECT_EQ(std::string(data, length), "old");
      EXPECT_EQ(KvNodeServerWiringTestPeer::RamSource(*server, key),
                committed.identity);
      ASSERT_TRUE(KvNodeServerWiringTestPeer::DropRam(*server, key));
      store->after_read_ = nullptr;
    }
    store->after_read_ = &late_gate;
    Status status = Status::kInvalid;
    std::string bytes;
    std::thread reader([&] {
      status = server->RangeDirectForKey(key, 0, 0, buffer, sizeof(buffer),
                                         &data, &length);
      if (status == Status::kOk) bytes.assign(data, length);
    });
    const bool entered = late_gate.AwaitEntry();
    std::string ignored;
    if (entered) {
      EXPECT_EQ(
          server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                       key, 0, 0, nullptr, 0, &ignored),
          Status::kOk);
      if (replace) {
        EXPECT_EQ(
            server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                         key, 0, 0, "new", 3, &ignored),
            Status::kOk);
      }
    }
    late_gate.Open();
    reader.join();
    ASSERT_TRUE(entered);
    EXPECT_EQ(status, Status::kOk);
    EXPECT_EQ(bytes, "old");
    EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
    EXPECT_FALSE(KvNodeServerWiringTestPeer::RamSource(*server, key).valid());
    if (replace) {
      ASSERT_EQ(server->RangeDirectForKey(key, 0, 0, buffer, sizeof(buffer),
                                          &data, &length),
                Status::kOk);
      EXPECT_EQ(std::string(data, length), "new");
      const DiskResidentIdentity next =
          KvNodeServerWiringTestPeer::RamSource(*server, key);
      EXPECT_TRUE(next.valid());
      EXPECT_FALSE(next == committed.identity);
    }
    server.reset();
    fs::remove_all(dir);
  }
}

TEST(RamTierWiring, AsyncLastReleaseControlsStatusAndReservationSource) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  enum class Case : unsigned char {
    kSuccess = 0,
    kRemove = 1,
    kInvalidationFailure = 2,
    kIoFailure = 3,
    kAbort = 4,
    kEmpty = 5
  };
  for (const Case action :
       {Case::kSuccess, Case::kRemove, Case::kInvalidationFailure,
        Case::kIoFailure, Case::kAbort, Case::kEmpty}) {
    SCOPED_TRACE(static_cast<unsigned>(action));
    const fs::path dir =
        fs::temp_directory_path() / "dfkv_async_source_terminal";
    fs::remove_all(dir);
    DiskSlabStore* store = nullptr;
    DiskCacheGroup::Options options{{dir.string()}, 1U << 20, "slab"};
    options.eviction_policy = "legacy";
    options.engine_factory = [&](const std::string& path, uint64_t capacity) {
      DiskSlabStore::Options slab;
      slab.dir = path;
      slab.capacity_bytes = capacity;
      slab.extent_bytes = 1U << 20;
      slab.slot_granularity = 4096;
      slab.reclaim_interval_ms = slab.table_sync_ms = 0;
      slab.track_resident_identity = true;
      std::unique_ptr<DiskSlabStore> result =
          std::make_unique<DiskSlabStore>(slab);
      store = result.get();
      return result;
    };
    std::unique_ptr<KvNodeServer> server =
        KvNodeServerWiringTestPeer::Create(std::move(options));
    ASSERT_TRUE(server->Healthy());
    KvNodeServerWiringTestPeer::SetRam(
        *server, [](const BlockKey&, char*, size_t, size_t) { return false; });
    KvNodeServerWiringTestPeer::SetCoalescing(*server, true);
    const BlockKey key{9830, 0};
    const DiskWriteResult committed =
        KvNodeServerWiringTestPeer::PersistSource(*server, key, "owner", 5);
    ASSERT_TRUE(committed.identity.valid());
    alignas(4096) char staging[4096] = {};
    const bool partial =
        action == Case::kRemove || action == Case::kInvalidationFailure;
    const bool empty = action == Case::kEmpty;
    const uint64_t offset = empty ? 5 : partial ? 1 : 0;
    const uint64_t epoch = KvNodeServerWiringTestPeer::ReadEpoch(*server, key);
    PreparedRead read =
        server->PrepareReadForKey(key, offset, 0, staging, sizeof(staging));
    ASSERT_EQ(read.status(), Status::kOk);
    EXPECT_EQ(read.needs_io(), !empty);
    EXPECT_EQ(read.source_registered(), !partial && !empty);
    EXPECT_FALSE(read.source_identity().valid());
    if (!empty) {
      const ssize_t got = ::pread(read.fd(), read.staging(), read.aligned_len(),
                                  static_cast<off_t>(read.aligned_off()));
      ASSERT_GE(got, 0);
      ASSERT_GE(static_cast<size_t>(got), read.head() + read.payload_len());
      EXPECT_TRUE(KvNodeServerWiringTestPeer::ReadFlight(*server, key, offset,
                                                         0, epoch));
    }
    if (action == Case::kRemove || action == Case::kInvalidationFailure) {
      if (action == Case::kInvalidationFailure) {
        DiskSlabStoreTestPeer::SetRecordWriteHook(store, [] { return false; });
      }
      std::string ignored;
      ASSERT_EQ(
          server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                       key, 0, 0, nullptr, 0, &ignored),
          Status::kOk);
    }
    const Status io_status =
        action == Case::kIoFailure ? Status::kIOError : Status::kOk;
    const Status expected =
        action == Case::kInvalidationFailure || action == Case::kIoFailure
            ? Status::kIOError
            : Status::kOk;
    EXPECT_EQ(
        read.FinishStorage(io_status,
                           action == Case::kIoFailure ? 0 : read.payload_len()),
        expected);
    EXPECT_EQ(read.status(), expected);
    EXPECT_EQ(read.fd(), -1);
    EXPECT_EQ(server->RamUsedBytes() != 0, !partial && !empty);
    EXPECT_TRUE(read.owns_cleanup());  // Final I/O status is not grant success.
    EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
    if (action == Case::kSuccess) {
      EXPECT_EQ(read.source_identity(), committed.identity);
      PreparedRead moved(std::move(read));
      read =
          std::move(moved);  // Reinitialization preserves final storage state.
      EXPECT_EQ(read.source_identity(), committed.identity);
      EXPECT_TRUE(read.Stage());  // Fallback works after I/O, before grant.
      EXPECT_EQ(std::string(read.data(), read.payload_len()), "owner");
    }
    EXPECT_EQ(
        KvNodeServerWiringTestPeer::ReadFlight(*server, key, offset, 0, epoch),
        !empty);
    if (action == Case::kAbort) {
      EXPECT_TRUE(read.Abort());
      EXPECT_EQ(read.status(), Status::kIOError);
      EXPECT_EQ(server->RamUsedBytes(), 0U);
      EXPECT_FALSE(read.source_identity().valid());
      EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
      EXPECT_FALSE(KvNodeServerWiringTestPeer::ReadFlight(*server, key, offset,
                                                          0, epoch));
      read = PreparedRead{};
      server.reset();
      fs::remove_all(dir);
      continue;
    }
    DiskResidentIdentity shared_identity;
    std::string shared_bytes;
    Status shared_status = Status::kInvalid;
    std::thread follower;
    bool joined = false;
    if (!empty) {
      follower = std::thread([&] {
        shared_status = KvNodeServerWiringTestPeer::ReadSharedSource(
            *server, key, offset, epoch, &shared_identity, &shared_bytes);
      });
      joined = KvNodeServerWiringTestPeer::AwaitCopyWaiter(*server);
    }
    // A later Ok cannot turn a failed terminal back into a successful read.
    EXPECT_TRUE(read.Commit(Status::kOk, read.payload_len(), 0.0));
    if (!empty) {
      follower.join();
      EXPECT_TRUE(joined);
      EXPECT_EQ(shared_status, expected);
      EXPECT_EQ(shared_bytes, expected != Status::kOk ? ""
                              : partial               ? "wner"
                                                      : "owner");
      EXPECT_EQ(shared_identity, action == Case::kSuccess
                                     ? committed.identity
                                     : DiskResidentIdentity{});
    }
    EXPECT_EQ(read.status(), expected);
    EXPECT_FALSE(
        KvNodeServerWiringTestPeer::ReadFlight(*server, key, offset, 0, epoch));
    EXPECT_EQ(read.source_identity(), action == Case::kSuccess
                                          ? committed.identity
                                          : DiskResidentIdentity{});
    if (action == Case::kSuccess) {
      EXPECT_EQ(KvNodeServerWiringTestPeer::RamSource(*server, key),
                committed.identity);
      PreparedRead hit =
          server->PrepareReadForKey(key, 0, 0, staging, sizeof(staging));
      EXPECT_FALSE(hit.needs_io());
      EXPECT_TRUE(hit.Commit(Status::kOk, hit.payload_len(), 0.0));
      EXPECT_EQ(hit.source_identity(), committed.identity);
    } else if (action == Case::kInvalidationFailure ||
               action == Case::kIoFailure) {
      EXPECT_EQ(server->RamUsedBytes(), 0U);
      EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*server, key));
    }
    EXPECT_FALSE(read.Commit(Status::kOk, read.payload_len(), 0.0));
    EXPECT_FALSE(read.Abort());
    read = PreparedRead{};
    server.reset();
    fs::remove_all(dir);
  }
}

class LifetimeAdmissionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        fs::temp_directory_path() /
        ("dfkv_lifetime_admission_" +
         std::to_string(
             ::testing::UnitTest::GetInstance()->current_test_info()->line()));
    fs::remove_all(dir_);
    options_.cache_dirs = {dir_.string()};
    options_.capacity_bytes = 1ull << 30;
    options_.engine = "slab";
    options_.eviction_policy = "legacy";
    options_.lifetime_clock = std::make_shared<DiskLifetimeClock>(100, [this] {
      const uint64_t sample = seconds_.load(std::memory_order_relaxed);
      if (expire_after_sample_.exchange(false, std::memory_order_relaxed))
        seconds_.store(UINT32_MAX, std::memory_order_relaxed);
      return sample;
    });
    server_ = KvNodeServerWiringTestPeer::Create(options_);
    ASSERT_TRUE(server_->Healthy());
  }
  void TearDown() override {
    server_.reset();
    fs::remove_all(dir_);
  }
  void Expire() { seconds_.store(UINT32_MAX, std::memory_order_relaxed); }
  void InstallRam() {
    KvNodeServer* const node = server_.get();
    KvNodeServerWiringTestPeer::SetRam(
        *node, [node](const BlockKey& key, char* data, size_t len, size_t) {
          return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
                 Status::kOk;
        });
  }
  const ScopedEnv ram_off_{"DFKV_RAM_TIER", "0"};
  const ScopedEnv buffered_{"DFKV_SLAB_WRITE", "buffered"};
  const ScopedEnv granularity_{"DFKV_SLAB_GRANULARITY", "1048576"};
  const ScopedEnv sync_{"DFKV_SLAB_TABLE_SYNC_MS", "0"};
  const ScopedEnv reclaim_{"DFKV_SLAB_RECLAIM_MS", "0"};
  std::atomic<uint64_t> seconds_{0};
  std::atomic<bool> expire_after_sample_{false};
  DiskCacheGroup::Options options_;
  std::unique_ptr<KvNodeServer> server_;
  fs::path dir_;
};

TEST_F(LifetimeAdmissionTest, RejectsEveryNewBusinessEntryBeforeRamHit) {
  const BlockKey key{99001, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server_, key, "old", 3),
            Status::kOk);
  InstallRam();
  ASSERT_TRUE(KvNodeServerWiringTestPeer::InstallDurable(*server_, key, "old"));
  Expire();
  EXPECT_TRUE(server_->LifetimeRestartRequired());
  EXPECT_TRUE(server_->Healthy());  // Health is persistence, not readiness.
  for (const WireOp op : {WireOp::kRange, WireOp::kLookup, WireOp::kExist,
                          WireOp::kRemove, WireOp::kCache}) {
    std::string out = "stale";
    size_t total = 99;
    EXPECT_EQ(server_->ProcessRequestForKey(static_cast<uint8_t>(op), key, 0, 0,
                                            "new", 3, &out, &total),
              Status::kIOError);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(total, 0U);
  }
  ValueMetadata metadata;
  metadata.value_len = 99;
  EXPECT_EQ(server_->LookupForKey(key, &metadata), Status::kIOError);
  EXPECT_EQ(metadata.value_len, 0U);
  alignas(4096) char bytes[4096] = {};
  size_t length = 99;
  size_t total = 99;
  EXPECT_EQ(server_->RangeIntoForKey(key, 0, 0, bytes, sizeof(bytes), &length,
                                     &total),
            Status::kIOError);
  EXPECT_EQ(length, 0U);
  EXPECT_EQ(total, 0U);
  const char* data = bytes;
  length = total = 99;
  EXPECT_EQ(server_->RangeDirectForKey(key, 0, 0, bytes, sizeof(bytes), &data,
                                       &length, &total),
            Status::kIOError);
  EXPECT_EQ(data, nullptr);
  EXPECT_EQ(length, 0U);
  EXPECT_EQ(total, 0U);
  EXPECT_EQ(server_->CacheDirectForKey(key, bytes, 3, sizeof(bytes)),
            Status::kIOError);
  const std::vector<Status> results = server_->CacheDirectBatchForKeys(
      {{key, bytes, 3, sizeof(bytes)}, {{99002, 0}, bytes, 3, sizeof(bytes)}});
  ASSERT_EQ(results.size(), 2U);
  EXPECT_EQ(results[0], Status::kIOError);
  EXPECT_EQ(results[1], Status::kIOError);
  PreparedRead pinned;
  EXPECT_FALSE(server_->RamPinnedHitForKey(key, 0, 0, &pinned));
  EXPECT_FALSE(pinned.owns_cleanup());
  const PreparedRead prepared =
      server_->PrepareReadForKey(key, 0, 0, bytes, sizeof(bytes));
  EXPECT_EQ(prepared.status(), Status::kIOError);
  EXPECT_FALSE(prepared.owns_cleanup());
  std::string disk;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server_, key, &disk),
            Status::kOk);
  EXPECT_EQ(disk, "old");
  EXPECT_TRUE(KvNodeServerWiringTestPeer::Resident(*server_, key));
  EXPECT_TRUE(KvNodeServerWiringTestPeer::PersistenceHealthy(*server_));
  for (const WireOp op : {WireOp::kStats, WireOp::kMembers}) {
    std::string out;
    EXPECT_EQ(server_->ProcessRequestForKey(static_cast<uint8_t>(op), key, 0, 0,
                                            nullptr, 0, &out),
              Status::kOk);
  }
  seconds_.store(0, std::memory_order_relaxed);
  EXPECT_TRUE(server_->LifetimeRestartRequired());
}

TEST_F(LifetimeAdmissionTest, StartupRechecksClockBeforeOpeningListener) {
  Expire();
  EXPECT_EQ(server_->Start(0), Status::kIOError);
  EXPECT_TRUE(KvNodeServerWiringTestPeer::PersistenceHealthy(*server_));
}

TEST_F(LifetimeAdmissionTest, AdmittedPreparedReadStillReleasesDiskLease) {
  const BlockKey key{99003, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server_, key, "value", 5),
            Status::kOk);
  alignas(4096) char bytes[4096] = {};
  PreparedRead read =
      server_->PrepareReadForKey(key, 0, 0, bytes, sizeof(bytes));
  ASSERT_EQ(read.status(), Status::kOk);
  ASSERT_TRUE(read.needs_io());
  ASSERT_EQ(::pread(read.fd(), read.staging(), read.aligned_len(),
                    static_cast<off_t>(read.aligned_off())),
            static_cast<ssize_t>(read.aligned_len()));
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskReadLeases(*server_), 1U);
  Expire();
  const PreparedRead refused =
      server_->PrepareReadForKey(key, 0, 0, bytes, sizeof(bytes));
  EXPECT_EQ(refused.status(), Status::kIOError);
  EXPECT_TRUE(read.Commit(Status::kOk, read.payload_len(), 0));
  EXPECT_EQ(read.status(), Status::kOk);
  EXPECT_FALSE(read.owns_cleanup());
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskReadLeases(*server_), 0U);
}

TEST_F(LifetimeAdmissionTest, AdmittedPinnedRamReadStillCompletes) {
  InstallRam();
  const BlockKey key{99004, 0};
  ASSERT_TRUE(KvNodeServerWiringTestPeer::InstallDurable(*server_, key, "ram"));
  PreparedRead old;
  ASSERT_TRUE(server_->RamPinnedHitForKey(key, 0, 0, &old));
  Expire();
  PreparedRead refused;
  EXPECT_FALSE(server_->RamPinnedHitForKey(key, 0, 0, &refused));
  EXPECT_TRUE(old.Commit(Status::kOk, 3, 0));
  EXPECT_FALSE(old.owns_cleanup());
  EXPECT_TRUE(KvNodeServerWiringTestPeer::PersistenceHealthy(*server_));
}

TEST_F(LifetimeAdmissionTest, StopDrainsAdmittedWriteBackAndCleanlyReopens) {
  ASSERT_EQ(server_->Start(0), Status::kOk);
  FlushBarrier barrier;
  KvNodeServer* const node = server_.get();
  KvNodeServerWiringTestPeer::SetRam(
      *node, [&, node](const BlockKey& key, char* data, size_t len, size_t) {
        barrier.Block();
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      });
  const std::string address = "127.0.0.1:" + std::to_string(node->port());
  const BlockKey key{99005, 0};
  Status result = Status::kInvalid;
  std::thread client([&] {
    TcpTransport tcp;
    tcp.set_timeouts(1000, 5000);
    result = tcp.Cache(address, key, "accepted", 8);
  });
  EXPECT_TRUE(barrier.AwaitEntry());
  Expire();
  // New RAM admission must be refused while the old disk-ACK owner is waiting.
  alignas(4096) char fresh[4096] = {};
  EXPECT_EQ(node->CacheDirectForKey({99006, 0}, fresh, 3, sizeof(fresh)),
            Status::kIOError);
  std::atomic<bool> stopped{false};
  std::thread stopper([&] {
    node->Stop();
    stopped.store(true, std::memory_order_release);
  });
  client.join();  // Stop's socket shutdown cancels the waiting client reply.
  EXPECT_EQ(result, Status::kIOError);
  EXPECT_FALSE(stopped.load(std::memory_order_acquire));
  barrier.Open();
  stopper.join();
  EXPECT_TRUE(stopped.load(std::memory_order_acquire));
  EXPECT_TRUE(KvNodeServerWiringTestPeer::PersistenceHealthy(*node));
  server_.reset();  // Joins RAM flusher before disk writes its clean epoch.
  options_.lifetime_clock = std::make_shared<DiskLifetimeClock>(
      100, [this] { return seconds_.load(std::memory_order_relaxed); });
  server_ = KvNodeServerWiringTestPeer::Create(options_);
  ASSERT_TRUE(server_->Healthy());
  std::string value;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*server_, key, &value),
            Status::kOk);
  EXPECT_EQ(value, "accepted");
}

TEST_F(LifetimeAdmissionTest, TcpRamHitWorksAtBoundaryAndRejectsAfterIt) {
  InstallRam();
  const BlockKey key{99007, 0};
  ASSERT_TRUE(
      KvNodeServerWiringTestPeer::InstallDurable(*server_, key, "cached"));
  ASSERT_EQ(server_->Start(0), Status::kOk);
  const std::string address = "127.0.0.1:" + std::to_string(server_->port());
  TcpTransport tcp;
  tcp.set_timeouts(1000, 5000);
  seconds_.store(uint64_t{UINT32_MAX} - 100, std::memory_order_relaxed);
  std::string value;
  ASSERT_EQ(tcp.Range(address, key, 0, 6, &value), Status::kOk);
  EXPECT_EQ(value, "cached");
  seconds_.store(uint64_t{UINT32_MAX} - 99, std::memory_order_relaxed);
  EXPECT_EQ(tcp.Range(address, key, 0, 6, &value), Status::kIOError);
  EXPECT_TRUE(server_->Healthy());
  EXPECT_TRUE(server_->LifetimeRestartRequired());
}

TEST_F(LifetimeAdmissionTest, RamAckDirtyValueCanDrainAfterNewAdmissionCloses) {
  ASSERT_EQ(server_->Start(0), Status::kOk);
  FlushBarrier barrier;
  KvNodeServer* const node = server_.get();
  KvNodeServerWiringTestPeer::SetRam(
      *node,
      [&, node](const BlockKey& key, char* data, size_t len, size_t) {
        barrier.Block();
        return KvNodeServerWiringTestPeer::Persist(*node, key, data, len) ==
               Status::kOk;
      },
      true);
  const BlockKey key{99008, 0};
  std::string ignored;
  EXPECT_EQ(node->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kCache),
                                       key, 0, 0, "ram-ack", 7, &ignored),
            Status::kOk);
  EXPECT_TRUE(barrier.AwaitEntry());
  Expire();
  EXPECT_TRUE(node->LifetimeRestartRequired());
  barrier.Open();
  node->Stop();
  EXPECT_TRUE(KvNodeServerWiringTestPeer::PersistenceHealthy(*node));
  std::string disk;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node, key, &disk),
            Status::kOk);
  EXPECT_EQ(disk, "ram-ack");
  server_.reset();  // Join the flusher before its captured barrier dies.
}

TEST_F(LifetimeAdmissionTest, NestedLookupRejectionClearsOldResponse) {
  const BlockKey key{99009, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*server_, key, "old", 3),
            Status::kOk);
  KvNodeServerWiringTestPeer::SetCoalescing(*server_, true);
  expire_after_sample_.store(true, std::memory_order_relaxed);
  std::string value = "stale response";
  size_t total = 99;
  EXPECT_EQ(
      server_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange), key,
                                    0, 3, nullptr, 0, &value, &total),
      Status::kIOError);
  EXPECT_TRUE(value.empty());
  EXPECT_EQ(total, 0U);
  EXPECT_TRUE(server_->LifetimeRestartRequired());
  EXPECT_TRUE(server_->Healthy());
}

// Construct the real node/RAM tier: never replace its production flush sinks.
class ProductionFlushResultTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ =
        fs::temp_directory_path() /
        ("dfkv_production_flush_" +
         std::to_string(
             ::testing::UnitTest::GetInstance()->current_test_info()->line()));
    fs::remove_all(dir_);
  }
  void TearDown() override {
    node_.reset();
    fs::remove_all(dir_);
  }
  void Create(bool with_clock, const char* engine = "slab", size_t disks = 1) {
    node_.reset();
    DiskCacheGroup::Options options;
    options.cache_dirs = {(dir_ / engine).string()};
    for (size_t i = 1; i < disks; ++i)
      options.cache_dirs.push_back(
          (dir_ / (engine + std::to_string(i))).string());
    options.capacity_bytes = (1ull << 30) * disks;
    options.engine = engine;
    options.eviction_policy = "legacy";
    if (with_clock) {
      options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
          100, [this] { return seconds_.load(std::memory_order_relaxed); });
    }
    node_ = KvNodeServerWiringTestPeer::Create(std::move(options));
    ASSERT_TRUE(node_->Healthy());
    ASSERT_TRUE(node_->ram_enabled());
    ASSERT_EQ(KvNodeServerWiringTestPeer::DiskEngine(*node_), engine);
  }
  const ScopedEnv ram_on_{"DFKV_RAM_TIER", "1"};
  const ScopedEnv ram_bytes_{"DFKV_RAM_TIER_BYTES", "8388608"};
  const ScopedEnv extent_{"DFKV_RAM_TIER_EXTENT_BYTES", "1048576"};
  const ScopedEnv reserve_{"DFKV_RAM_TIER_LARGE_RESERVE_BYTES", "0"};
  const ScopedEnv shards_{"DFKV_RAM_TIER_SHARDS", "1"};
  const ScopedEnv workers_{"DFKV_RAM_FLUSH_THREADS", "1"};
  const ScopedEnv writeback_{"DFKV_RAM_WRITE_MODE", "writeback"};
  const ScopedEnv disk_ack_{"DFKV_PUT_ACK_MODE", "disk"};
  const ScopedEnv buffered_{"DFKV_SLAB_WRITE", "buffered"};
  const ScopedEnv granularity_{"DFKV_SLAB_GRANULARITY", "1048576"};
  const ScopedEnv sync_{"DFKV_SLAB_TABLE_SYNC_MS", "0"};
  const ScopedEnv reclaim_{"DFKV_SLAB_RECLAIM_MS", "0"};
  const ScopedEnv ram_reclaim_{"DFKV_RAM_RECLAIM_MS", "0"};
  fs::path dir_;
  std::atomic<uint64_t> seconds_{100};
  std::unique_ptr<KvNodeServer> node_;
};

TEST_F(ProductionFlushResultTest, NormalLegacyAndFileRetainBoolCompletion) {
  for (const char* engine : {"slab", "file"}) {
    SCOPED_TRACE(engine);
    ASSERT_NO_FATAL_FAILURE(Create(false, engine));
    RamTier& tier = KvNodeServerWiringTestPeer::Ram(*node_);
    EXPECT_FALSE(RamTierTestPeer::HasTypedSingle(tier));
    EXPECT_FALSE(RamTierTestPeer::HasTypedBatch(tier));
    const BlockKey key{99501, 0};
    char data[] = "legacy";
    ASSERT_EQ(node_->CacheDirectForKey(key, data, 6, sizeof(data)),
              Status::kOk);
    ASSERT_TRUE(tier.Contains(key));
    RamTier::PutResult completion{Status::kInvalid};
    EXPECT_FALSE(RamTierTestPeer::StoredResult(tier, key, &completion));
    std::string stored;
    ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, key, &stored),
              Status::kOk);
    EXPECT_EQ(stored, "legacy");
    EXPECT_EQ(KvNodeServerWiringTestPeer::FlushObjects(*node_), 1U);
  }
}

TEST_F(ProductionFlushResultTest, ClockPathPreservesActualWriteCompletion) {
  ASSERT_NO_FATAL_FAILURE(Create(true));
  RamTier& tier = KvNodeServerWiringTestPeer::Ram(*node_);
  const BlockKey key{99502, 0};
  char data[] = "clock";
  ASSERT_EQ(node_->CacheDirectForKey(key, data, 5, sizeof(data)), Status::kOk);
  RamTier::PutResult result{Status::kInvalid};
  ASSERT_TRUE(RamTierTestPeer::StoredResult(tier, key, &result));
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.reason, DiskWriteReason::kNone);
  EXPECT_EQ(result.outcome, DiskWriteOutcome::kNew);
  EXPECT_FALSE(result.identity.valid());  // Real slabs do not track it yet.
  EXPECT_EQ(KvNodeServerWiringTestPeer::FlushObjects(*node_), 1U);
  std::string stored;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, key, &stored),
            Status::kOk);
  EXPECT_EQ(stored, "clock");
}

TEST_F(ProductionFlushResultTest, ClockScalarSinkPersistsAndClassifies) {
  ASSERT_NO_FATAL_FAILURE(Create(true));
  RamTier& tier = KvNodeServerWiringTestPeer::Ram(*node_);
  ASSERT_TRUE(RamTierTestPeer::HasTypedSingle(tier));
  char data[] = "scalar";
  const BlockKey key{99503, 0};
  const DiskWriteResult result =
      RamTierTestPeer::FlushSingle(tier, key, data, 6, sizeof(data));
  ASSERT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.outcome, DiskWriteOutcome::kNew);
  EXPECT_FALSE(result.identity.valid());
  const DiskWriteResult duplicate =
      RamTierTestPeer::FlushSingle(tier, key, data, 6, sizeof(data));
  EXPECT_EQ(duplicate.status, Status::kOk);
  EXPECT_EQ(duplicate.outcome, DiskWriteOutcome::kKeptExisting);
  std::string stored;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, key, &stored),
            Status::kOk);
  EXPECT_EQ(stored, "scalar");
}

TEST_F(ProductionFlushResultTest, ClockBatchSinkKeepsOrderStatusAndObserver) {
  ASSERT_NO_FATAL_FAILURE(Create(true));
  RamTier& tier = KvNodeServerWiringTestPeer::Ram(*node_);
  ASSERT_TRUE(RamTierTestPeer::HasTypedBatch(tier));
  char data[] = "batch";
  char other[] = "other";
  const BlockKey key{99504, 0};
  const std::vector<RamTier::FlushItem> items{{{99505, 0}, nullptr, 0, 0},
                                              {key, data, 5, sizeof(data)},
                                              {key, other, 5, sizeof(other)}};
  const std::vector<DiskWriteResult> results =
      RamTierTestPeer::FlushBatch(tier, items);
  ASSERT_EQ(results.size(), 3U);
  EXPECT_EQ(results[0].status, Status::kInvalid);
  EXPECT_EQ(results[0].outcome, DiskWriteOutcome::kUnknown);
  EXPECT_EQ(results[1].status, Status::kOk);
  EXPECT_EQ(results[1].outcome, DiskWriteOutcome::kNew);
  EXPECT_EQ(results[2].status, Status::kOk);
  EXPECT_EQ(results[2].outcome, DiskWriteOutcome::kKeptExisting);
  EXPECT_EQ(KvNodeServerWiringTestPeer::FlushObjects(*node_), 2U);
  std::string stored;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, key, &stored),
            Status::kOk);
  EXPECT_EQ(stored, "batch");
}

TEST_F(ProductionFlushResultTest, RealRecordFailureStillIsolatesIo) {
  ASSERT_NO_FATAL_FAILURE(Create(true));
  DiskSlabStoreTestPeer::SetRecordWriteHook(
      &KvNodeServerWiringTestPeer::Slab(*node_), [] { return false; });
  char data[] = "failed";
  const Status result =
      node_->CacheDirectForKey({99506, 0}, data, 6, sizeof(data));
  EXPECT_EQ(result, Status::kIOError);
  EXPECT_FALSE(KvNodeServerWiringTestPeer::PersistenceHealthy(*node_));
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Resident(*node_, {99506, 0}));
  EXPECT_FALSE(node_->Healthy());
}

TEST_F(ProductionFlushResultTest, AcceptedQueuedPutStopsWithoutPoisoningIo) {
  // The real group keeps its 1-GiB extent, but one slot fills it. Buffered
  // payload I/O leaves the extent sparse; no 1-GiB test allocation is written.
  const ScopedEnv one_slot("DFKV_SLAB_GRANULARITY", "1073741824");
  ASSERT_NO_FATAL_FAILURE(Create(true));
  const RamTier& tier = KvNodeServerWiringTestPeer::Ram(*node_);
  const BlockKey first_key{99507, 0};
  BlockKey second_key{99508, 0};
  while (KvNodeServerWiringTestPeer::MutationStripe(first_key) ==
         KvNodeServerWiringTestPeer::MutationStripe(second_key))
    ++second_key.digest_lo;
  char first_data[] = "first";
  char second_data[] = "second";
  FlushBarrier barrier;
  DiskSlabStoreTestPeer::SetPayloadWriteHook(
      &KvNodeServerWiringTestPeer::Slab(*node_), [&] {
        barrier.Block();
        return true;
      });
  Status first = Status::kInvalid;
  Status second = Status::kInvalid;
  std::thread owner([&] {
    first =
        node_->CacheDirectForKey(first_key, first_data, 5, sizeof(first_data));
  });
  const bool entered = barrier.AwaitEntry();
  EXPECT_TRUE(entered);
  bool admitted = false;
  std::thread queued;
  if (entered) {
    queued = std::thread([&] {
      second = node_->CacheDirectForKey(second_key, second_data, 6,
                                        sizeof(second_data));
    });
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!(admitted = tier.Contains(second_key)) &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::yield();
    EXPECT_TRUE(admitted);  // Actual RAM visibility proves prior admission.
    if (admitted)
      seconds_.store(uint64_t{UINT32_MAX} + 101, std::memory_order_relaxed);
  }
  // No fatal assertions while workers are blocked; always release and join.
  barrier.Open();
  owner.join();
  if (queued.joinable()) queued.join();
  DiskSlabStoreTestPeer::SetPayloadWriteHook(
      &KvNodeServerWiringTestPeer::Slab(*node_), {});
  EXPECT_EQ(first, Status::kOk);  // Its disk reservation preceded exhaustion.
  if (admitted) {
    EXPECT_EQ(second, Status::kCacheFull);
    EXPECT_FALSE(tier.Contains(second_key));
    EXPECT_EQ(tier.DirtyObjects(), 0U);
    EXPECT_TRUE(KvNodeServerWiringTestPeer::PersistenceHealthy(*node_));
    EXPECT_TRUE(node_->LifetimeRestartRequired());
  }
  std::string stored;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, first_key, &stored),
            Status::kOk);
  EXPECT_EQ(stored, "first");
  // All accepted disk-ACK work is terminal before the old clock/store dies.
  node_.reset();
  ASSERT_NO_FATAL_FAILURE(Create(true));
  EXPECT_FALSE(node_->LifetimeRestartRequired());
  std::string restored;
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, first_key, &restored),
            Status::kOk);
  EXPECT_EQ(restored, "first");
}

TEST_F(ProductionFlushResultTest, UndrainedRamAckLeavesEveryDiskDirty) {
  const ScopedEnv ram_ack("DFKV_PUT_ACK_MODE", "ram");
  const ScopedEnv timeout("DFKV_RAM_SHUTDOWN_DRAIN_TIMEOUT_S", "0");
  const ScopedEnv one_slot("DFKV_SLAB_GRANULARITY", "1073741824");
  for (const bool started : {false, true}) {
    SCOPED_TRACE(started);
    ASSERT_NO_FATAL_FAILURE(Create(true, "slab", 2));
    if (started) {
      ASSERT_EQ(node_->Start(0), Status::kOk);
    }
    const BlockKey disk_key{99600, 0};
    ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*node_, disk_key, "disk", 4),
              Status::kOk);
    const std::shared_ptr<FlushBarrier> barrier =
        std::make_shared<FlushBarrier>();
    const OpenFlushOnExit release{*barrier};
    RamTierTestPeer::GateTypedBatch(KvNodeServerWiringTestPeer::Ram(*node_),
                                    [barrier] { barrier->Block(); });
    char data[] = "acked";
    ASSERT_EQ(node_->CacheDirectForKey({99601, 0}, data, 5, sizeof(data)),
              Status::kOk);
    ASSERT_TRUE(barrier->AwaitEntry());
    EXPECT_EQ(KvNodeServerWiringTestPeer::Ram(*node_).DirtyObjects(), 1U);
    seconds_.fetch_add(uint64_t{UINT32_MAX} + 1, std::memory_order_relaxed);
    if (started) {
      node_->Stop();
      node_->Stop();
    }
    barrier->Open();
    node_.reset();  // Final join must see the retained, unqueued dirty value.
    ASSERT_NO_FATAL_FAILURE(Create(true, "slab", 2));
    EXPECT_EQ(KvNodeServerWiringTestPeer::UncleanResets(*node_), 2U);
    EXPECT_TRUE(KvNodeServerWiringTestPeer::PersistenceHealthy(*node_));
    std::string value;
    EXPECT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, disk_key, &value),
              Status::kNotFound);
    // Dirty recovery deliberately cold-resets old disk cache, not RAM-ACK
    // recovery. This is the existing conservative restart contract.
    node_.reset();
  }
}

TEST_F(ProductionFlushResultTest,
       TimedOutStopCanStillFinishCleanlyAtDestruction) {
  const ScopedEnv ram_ack("DFKV_PUT_ACK_MODE", "ram");
  const ScopedEnv timeout("DFKV_RAM_SHUTDOWN_DRAIN_TIMEOUT_S", "0");
  ASSERT_NO_FATAL_FAILURE(Create(true));
  ASSERT_EQ(node_->Start(0), Status::kOk);
  const std::shared_ptr<FlushBarrier> barrier =
      std::make_shared<FlushBarrier>();
  const OpenFlushOnExit release{*barrier};
  RamTier& tier = KvNodeServerWiringTestPeer::Ram(*node_);
  RamTierTestPeer::GateTypedBatch(tier, [barrier] { barrier->Block(); });
  char data[] = "acked";
  const BlockKey key{99602, 0};
  ASSERT_EQ(node_->CacheDirectForKey(key, data, 5, sizeof(data)), Status::kOk);
  ASSERT_TRUE(barrier->AwaitEntry());
  node_->Stop();
  EXPECT_EQ(tier.ShutdownDrainTimeouts(), 1U);
  EXPECT_EQ(tier.DirtyObjects(), 1U);
  barrier->Open();
  node_.reset();  // The join, not Stop's temporary timeout, decides clean.
  ASSERT_NO_FATAL_FAILURE(Create(true));
  EXPECT_EQ(KvNodeServerWiringTestPeer::UncleanResets(*node_), 0U);
  std::string value;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, key, &value),
            Status::kOk);
  EXPECT_EQ(value, "acked");
}

TEST_F(ProductionFlushResultTest, RemovedRamAckDoesNotPreventCleanShutdown) {
  const ScopedEnv ram_ack("DFKV_PUT_ACK_MODE", "ram");
  const ScopedEnv one_slot("DFKV_SLAB_GRANULARITY", "1073741824");
  ASSERT_NO_FATAL_FAILURE(Create(true));
  const BlockKey disk_key{99603, 0};
  ASSERT_EQ(KvNodeServerWiringTestPeer::Persist(*node_, disk_key, "disk", 4),
            Status::kOk);
  const std::shared_ptr<FlushBarrier> barrier =
      std::make_shared<FlushBarrier>();
  const OpenFlushOnExit release{*barrier};
  RamTier& tier = KvNodeServerWiringTestPeer::Ram(*node_);
  RamTierTestPeer::GateTypedBatch(tier, [barrier] { barrier->Block(); });
  char data[] = "acked";
  const BlockKey pending{99604, 0};
  ASSERT_EQ(node_->CacheDirectForKey(pending, data, 5, sizeof(data)),
            Status::kOk);
  ASSERT_TRUE(barrier->AwaitEntry());
  seconds_.store(uint64_t{UINT32_MAX} + 101, std::memory_order_relaxed);
  barrier->Open();
  ASSERT_TRUE(WaitFor([&] {
    RamTier::PutResult result{Status::kInvalid};
    return RamTierTestPeer::StoredResult(tier, pending, &result) &&
           result.status == Status::kCacheFull &&
           result.reason == DiskWriteReason::kLifetimeClockExhausted;
  }));
  EXPECT_EQ(tier.DirtyObjects(), 1U);
  EXPECT_TRUE(tier.healthy());
  ASSERT_TRUE(
      tier.Remove(pending));  // Required cleanup after terminal refusal.
  node_.reset();
  ASSERT_NO_FATAL_FAILURE(Create(true));
  EXPECT_EQ(KvNodeServerWiringTestPeer::UncleanResets(*node_), 0U);
  std::string value;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, disk_key, &value),
            Status::kOk);
  EXPECT_EQ(value, "disk");
}

TEST_F(ProductionFlushResultTest, SuccessfulHardCrossingExposesInitSkipMetric) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir_ / "slab").string()};
  options.capacity_bytes = 1ull << 30;
  options.engine = "slab";
  options.eviction_policy = "legacy";
  options.lifetime_config = DiskLifetimeConfig{};
  options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
      DiskLifetimeHorizon(*options.lifetime_config),
      [this] { return seconds_.load(std::memory_order_relaxed); });
  node_ = KvNodeServerWiringTestPeer::Create(options);
  ASSERT_TRUE(node_->Healthy());
  DiskSlabStoreTestPeer::SetPayloadWriteHook(
      &KvNodeServerWiringTestPeer::Slab(*node_), [this] {
        seconds_.store(uint64_t{UINT32_MAX} + 101, std::memory_order_relaxed);
        return true;
      });
  char data[] = "committed";
  const BlockKey key{99800, 0};
  ASSERT_EQ(node_->CacheDirectForKey(key, data, 9, sizeof(data)), Status::kOk);
  EXPECT_TRUE(node_->LifetimeRestartRequired());
  EXPECT_TRUE(node_->Healthy());
  EXPECT_EQ(MetricVal(node_->MetricsText(),
                      "dfkv_slab_lifetime_init_clock_hard_exhausted_total"),
            1);
  EXPECT_EQ(
      MetricVal(node_->MetricsText(), "dfkv_slab_metadata_io_errors_total"), 0);
  std::string value;
  ASSERT_EQ(KvNodeServerWiringTestPeer::DiskRead(*node_, key, &value),
            Status::kOk);
  EXPECT_EQ(value, "committed");
}

class LifetimeSuccessReadTest : public ProductionFlushResultTest {
 protected:
  void CreateScored() {
    node_.reset();
    DiskCacheGroup::Options options;
    options.cache_dirs = {(dir_ / "slab").string()};
    options.capacity_bytes = 1ull << 30;
    options.engine = "slab";
    options.eviction_policy = "legacy";
    options.lifetime_config = DiskLifetimeConfig{};
    options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
        DiskLifetimeHorizon(*options.lifetime_config),
        [this] { return seconds_.load(std::memory_order_relaxed); });
    node_ = KvNodeServerWiringTestPeer::Create(options);
    ASSERT_TRUE(node_->Healthy());
    KvNodeServerWiringTestPeer::SetCoalescing(*node_, false);
  }
  DiskWriteResult Seed(const BlockKey& key,
                       const std::string& value = "value") {
    return KvNodeServerWiringTestPeer::PersistSource(*node_, key, value.data(),
                                                     value.size());
  }
  DiskLifetimeState State(const BlockKey& key) const {
    const std::optional<DiskLifetimeState> state =
        DiskSlabStoreTestPeer::LifetimeState(
            KvNodeServerWiringTestPeer::Slab(*node_), key);
    if (!state.has_value()) {
      ADD_FAILURE() << "the scored resident is missing";
      return {};
    }
    return *state;
  }
  DiskSlabStore::LifetimeReadCounters Counters() const {
    return KvNodeServerWiringTestPeer::LifetimeCounters(*node_);
  }
  Status Read(const BlockKey& key, std::string* out, uint64_t offset = 0,
              uint64_t length = 0) {
    return node_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                       key, offset, length, nullptr, 0, out);
  }
  void ExpectConservation(uint64_t successful) const {
    const DiskSlabStore::LifetimeReadCounters counters = Counters();
    EXPECT_EQ(counters.successful, successful);
    EXPECT_EQ(counters.successful,
              counters.applied + counters.no_binding + counters.race_lost +
                  counters.clock_hard_exhausted + counters.io_unhealthy +
                  counters.state_unavailable);
    EXPECT_EQ(counters.successful,
              counters.source_unclassified + counters.source_ram +
                  counters.source_disk + counters.source_copy_leader +
                  counters.source_copy_follower +
                  counters.source_transport_grant);
    // Callers have finished (or explicitly held) every logical completion.
    // Check the exported disk outcomes, not just the in-memory accounting.
    const std::string text = node_->MetricsText();
    const std::string labels =
        R"({disk=")" + (dir_ / "slab").string() + R"(",disk_index="0"} )";
    const std::pair<const char*, uint64_t> outcomes[] = {
        {"success", counters.successful},
        {"applied", counters.applied},
        {"no_binding", counters.no_binding},
        {"race_lost", counters.race_lost},
        {"clock_hard_exhausted", counters.clock_hard_exhausted},
        {"io_unhealthy", counters.io_unhealthy},
        {"state_unavailable", counters.state_unavailable},
        {"source_unclassified", counters.source_unclassified},
        {"source_ram", counters.source_ram},
        {"source_disk", counters.source_disk},
        {"source_copy_leader", counters.source_copy_leader},
        {"source_copy_follower", counters.source_copy_follower},
        {"source_transport_grant", counters.source_transport_grant}};
    for (const std::pair<const char*, uint64_t>& outcome : outcomes) {
      EXPECT_EQ(MetricVal(text, std::string("dfkv_slab_lifetime_read_") +
                                    outcome.first + "_total"),
                static_cast<long>(outcome.second));
      EXPECT_NE(text.find(std::string("dfkv_slab_disk_lifetime_read_") +
                          outcome.first + "_total" + labels +
                          std::to_string(outcome.second) + "\n"),
                std::string::npos)
          << outcome.first;
    }
  }
};

TEST_F(LifetimeSuccessReadTest, RequestAndIntoCountEachLogicalSuccess) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99900, 0};
  ASSERT_EQ(Seed(key).status, Status::kOk);
  seconds_.store(121, std::memory_order_relaxed);
  std::string value;
  ASSERT_EQ(Read(key, &value), Status::kOk);
  EXPECT_EQ(value, "value");
  EXPECT_EQ(State(key).actual_reads, 1U);
  EXPECT_EQ(State(key).anchor, 21U);
  char buffer[32] = {};
  size_t size = 0;
  ASSERT_EQ(node_->RangeIntoForKey(key, 0, 0, buffer, sizeof(buffer), &size),
            Status::kOk);
  EXPECT_EQ(std::string(buffer, size), "value");
  EXPECT_EQ(State(key).actual_reads, 2U);
  EXPECT_EQ(Counters().applied, 2U);
  EXPECT_EQ(Counters().source_disk, 2U);
  EXPECT_EQ(
      MetricVal(node_->MetricsText(), "dfkv_slab_lifetime_read_success_total"),
      2);
  EXPECT_EQ(
      MetricVal(node_->MetricsText(), "dfkv_slab_lifetime_read_applied_total"),
      2);
  ExpectConservation(2);
}

TEST_F(LifetimeSuccessReadTest, TcpSocketReadHeatsDiskAndRamSources) {
  for (const bool ram_hit : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(CreateScored());
    const BlockKey key{99910 + static_cast<uint64_t>(ram_hit), 0};
    const DiskWriteResult written = Seed(key);
    ASSERT_EQ(written.status, Status::kOk);
    ASSERT_TRUE(written.identity.valid());
    if (ram_hit) {
      ASSERT_TRUE(KvNodeServerWiringTestPeer::Ram(*node_).PutDurable(
          key, "value", 5, written.identity));
    }
    ASSERT_EQ(node_->Start(0), Status::kOk);
    TcpTransport tcp;
    tcp.set_timeouts(1000, 5000);
    std::string value;
    ASSERT_EQ(tcp.Range("127.0.0.1:" + std::to_string(node_->port()), key, 0, 5,
                        &value),
              Status::kOk);
    EXPECT_EQ(value, "value");
    EXPECT_EQ(State(key).actual_reads, 1U);
    EXPECT_EQ(Counters().applied, 1U);
    EXPECT_EQ(Counters().source_ram, ram_hit ? 1U : 0U);
    EXPECT_EQ(Counters().source_disk, ram_hit ? 0U : 1U);
    ExpectConservation(1);
  }
}

TEST_F(LifetimeSuccessReadTest, RamIntoUsesItsBoundSourceAfterReleasingHit) {
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99912, 0};
  const DiskWriteResult written = Seed(key);
  ASSERT_TRUE(written.identity.valid());
  ASSERT_TRUE(KvNodeServerWiringTestPeer::Ram(*node_).PutDurable(
      key, "value", 5, written.identity));
  char buffer[32] = {};
  ASSERT_EQ(node_->RangeIntoForKey(key, 0, 0, buffer, sizeof(buffer), nullptr),
            Status::kOk);
  EXPECT_EQ(std::string(buffer, 5), "value");
  EXPECT_EQ(State(key).actual_reads, 1U);
  EXPECT_EQ(Counters().applied, 1U);
  EXPECT_EQ(Counters().source_ram, 1U);
  ExpectConservation(1);
}

TEST_F(LifetimeSuccessReadTest, UnboundRamNeverDiscoversALaterDiskBinding) {
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99913, 0};
  ASSERT_TRUE(
      KvNodeServerWiringTestPeer::Ram(*node_).PutDurable(key, "ram", 3));
  ASSERT_EQ(Seed(key, "disk").status, Status::kOk);
  std::string value;
  ASSERT_EQ(Read(key, &value), Status::kOk);
  EXPECT_EQ(value, "ram");
  char buffer[32] = {};
  size_t size = 0;
  ASSERT_EQ(node_->RangeIntoForKey(key, 0, 0, buffer, sizeof(buffer), &size),
            Status::kOk);
  EXPECT_EQ(std::string(buffer, size), "ram");
  EXPECT_EQ(State(key).actual_reads, 0U);
  EXPECT_EQ(Counters().no_binding, 2U);
  EXPECT_EQ(Counters().source_ram, 2U);
  EXPECT_EQ(MetricVal(node_->MetricsText(),
                      "dfkv_slab_lifetime_read_no_binding_total"),
            2);
  ExpectConservation(2);
}

TEST_F(LifetimeSuccessReadTest, OldRamSourceNeverHeatsSameKeyReplacement) {
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99914, 0};
  const DiskWriteResult old = Seed(key, "old");
  ASSERT_TRUE(old.identity.valid());
  ASSERT_TRUE(KvNodeServerWiringTestPeer::Ram(*node_).PutDurable(key, "old", 3,
                                                                 old.identity));
  // Deliberately retain the old RAM copy across physical disk replacement.
  // This tests notification provenance, not bypassed Node PUT visibility.
  ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*node_, key), Status::kOk);
  const DiskWriteResult replacement = Seed(key, "new");
  ASSERT_TRUE(replacement.identity.valid());
  ASSERT_FALSE(old.identity == replacement.identity);
  std::string value;
  ASSERT_EQ(Read(key, &value), Status::kOk);
  EXPECT_EQ(value, "old");
  EXPECT_EQ(State(key).actual_reads, 0U);
  EXPECT_EQ(Counters().race_lost, 1U);
  EXPECT_EQ(MetricVal(node_->MetricsText(),
                      "dfkv_slab_lifetime_read_race_lost_total"),
            1);
  ExpectConservation(1);
}

TEST_F(LifetimeSuccessReadTest,
       ForegroundRewriteRetiresOldRamWithScoringEnabled) {
  for (const bool write_back : {false, true}) {
    for (const bool batch : {false, true}) {
      for (const bool pinned : {false, true}) {
        SCOPED_TRACE("write_back=" + std::to_string(write_back) +
                     " batch=" + std::to_string(batch) +
                     " pinned=" + std::to_string(pinned));
        node_.reset();
        fs::remove_all(dir_ / "slab");
        ASSERT_NO_FATAL_FAILURE(CreateScored());
        ASSERT_TRUE(node_->ram_enabled());
        ASSERT_FALSE(node_->ram_ack_enabled());
        KvNodeServerWiringTestPeer::SetWriteBack(*node_, write_back);
        RamTier& ram = KvNodeServerWiringTestPeer::Ram(*node_);
        // Keep the real InitRam typed sinks/workers, including disk-ACK
        // behavior; do not substitute a bool flush callback for this
        // scoring-mode test.
        ASSERT_TRUE(RamTierTestPeer::HasTypedSingle(ram));
        ASSERT_TRUE(RamTierTestPeer::HasTypedBatch(ram));
        const BlockKey key{99940 + uint64_t{write_back} * 4 +
                               uint64_t{batch} * 2 + uint64_t{pinned},
                           0};
        const DiskWriteResult original = Seed(key, "old");
        ASSERT_EQ(original.status, Status::kOk);
        ASSERT_TRUE(original.identity.valid());
        ASSERT_TRUE(ram.PutDurable(key, "old", 3, original.identity));
        alignas(4096) char staging[4096] = {};
        PreparedRead old_send =
            node_->PrepareReadForKey(key, 0, 3, staging, sizeof(staging));
        ASSERT_EQ(old_send.status(), Status::kOk);
        ASSERT_FALSE(old_send.needs_io());
        ASSERT_TRUE(old_send.source_registered());
        ASSERT_EQ(old_send.FinishStorage(Status::kOk, 3), Status::kOk);
        const DiskResidentIdentity old_source = old_send.source_identity();
        ASSERT_TRUE(old_source == original.identity);
        if (!pinned) {
          ASSERT_TRUE(old_send.Abort());
        }
        // Preparation/storage completion is not a grant.
        ExpectConservation(0);
        // Independent backing disappearance, not pressure/victim-selection
        // proof.
        ASSERT_EQ(KvNodeServerWiringTestPeer::DropDisk(*node_, key),
                  Status::kOk);
        ASSERT_TRUE(ram.Contains(key));
        alignas(4096) char incoming[4096] = {};
        std::memcpy(incoming, "new", 3);
        const uint64_t flushes_before =
            KvNodeServerWiringTestPeer::FlushObjects(*node_);
        FlushBarrier done;
        Status written = Status::kInvalid;
        std::thread writer([&] {
          if (batch) {
            const std::vector<Status> results = node_->CacheDirectBatchForKeys(
                {{key, incoming, 3, sizeof(incoming)}});
            if (results.size() == 1) written = results[0];
          } else {
            written =
                node_->CacheDirectForKey(key, incoming, 3, sizeof(incoming));
          }
          done.Block();
        });
        const bool completed_with_pin = done.AwaitEntry();
        EXPECT_TRUE(completed_with_pin) << "PUT must not wait for old RAM SEND";
        // On a regression, release the old pin before joining the writer. Never
        // let a fatal assertion strand a joinable writer or its completion
        // gate.
        if (!completed_with_pin) old_send.Abort();
        done.Open();
        writer.join();
        ASSERT_EQ(written, Status::kOk);
        ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*node_));
        if (pinned && completed_with_pin) {
          EXPECT_TRUE(old_send.owns_cleanup());
          EXPECT_EQ(std::string(old_send.data(), old_send.payload_len()),
                    "old");
        }
        if (write_back && !batch && !pinned) {
          // With no old send pin, this PUT must use a new real RAM owner and
          // complete its production typed flush before the disk ACK returns.
          EXPECT_GT(KvNodeServerWiringTestPeer::FlushObjects(*node_),
                    flushes_before);
          EXPECT_TRUE(ram.Contains(key));
        }
        // Authenticate the replacement by reading its bytes, not by treating
        // KEEP_EXISTING as proof that incoming PUT bytes were published.
        const char* new_data = nullptr;
        size_t new_size = 0;
        DiskResidentIdentity replacement_source;
        ASSERT_EQ(node_->RangeDirectForKey(key, 0, 3, staging, sizeof(staging),
                                           &new_data, &new_size, nullptr,
                                           &replacement_source),
                  Status::kOk);
        ASSERT_NE(new_data, nullptr);
        EXPECT_EQ(std::string(new_data, new_size), "new");
        ASSERT_TRUE(replacement_source.valid());
        EXPECT_FALSE(replacement_source == old_source);
        EXPECT_NE(replacement_source.generation, old_source.generation);
        EXPECT_EQ(State(key).actual_reads, 0U);
        std::string visible;
        ASSERT_EQ(Read(key, &visible), Status::kOk);
        EXPECT_EQ(visible, "new");
        const DiskLifetimeState fresh = State(key);
        EXPECT_EQ(fresh.actual_reads, 1U);
        EXPECT_EQ(Counters().applied, 1U);
        ExpectConservation(1);
        node_->NotifyLifetimeReadSuccessForKey(key, old_source, 3);
        const DiskLifetimeState after_delayed_grant = State(key);
        EXPECT_EQ(after_delayed_grant.anchor, fresh.anchor);
        EXPECT_EQ(after_delayed_grant.put_at, fresh.put_at);
        EXPECT_EQ(after_delayed_grant.hstate, fresh.hstate);
        EXPECT_EQ(after_delayed_grant.actual_reads, fresh.actual_reads);
        EXPECT_EQ(after_delayed_grant.history_unknown, fresh.history_unknown);
        EXPECT_EQ(Counters().race_lost, 1U);
        EXPECT_EQ(Counters().applied, 1U);
        EXPECT_EQ(Counters().source_transport_grant, 1U);
        ExpectConservation(2);
        if (pinned && completed_with_pin) {
          EXPECT_TRUE(old_send.Abort());
        }
        EXPECT_FALSE(old_send.owns_cleanup());
        // SEND-pin teardown is not another success event.
        ExpectConservation(2);
        EXPECT_TRUE(node_->Healthy());
      }
    }
  }
}

TEST_F(LifetimeSuccessReadTest, CopyFollowersCountRequestsRatherThanOneIo) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  for (const bool into : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(CreateScored());
    KvNodeServerWiringTestPeer::SetCoalescing(*node_, true);
    const BlockKey key{99915 + static_cast<uint64_t>(into), 0};
    ASSERT_EQ(Seed(key).status, Status::kOk);
    const std::shared_ptr<FlushBarrier> barrier =
        std::make_shared<FlushBarrier>();
    const OpenFlushOnExit release{*barrier};
    std::atomic<unsigned> reads{0};
    DiskSlabStore& store = KvNodeServerWiringTestPeer::Slab(*node_);
    DiskSlabStoreTestPeer::SetReadHook(store, [barrier, &reads] {
      reads.fetch_add(1, std::memory_order_relaxed);
      barrier->Block();
    });
    constexpr size_t kRequests = 4;
    std::vector<Status> results(kRequests, Status::kInvalid);
    std::vector<std::string> values(kRequests);
    const auto request = [&](size_t index) {
      if (into) {
        char buffer[32] = {};
        size_t size = 0;
        results[index] =
            node_->RangeIntoForKey(key, 0, 5, buffer, sizeof(buffer), &size);
        values[index].assign(buffer, size);
      } else {
        results[index] = Read(key, &values[index], 0, 5);
      }
    };
    std::thread leader([&] { request(0); });
    EXPECT_TRUE(barrier->AwaitEntry());
    std::vector<std::thread> followers;
    for (size_t i = 1; i < kRequests; ++i)
      followers.emplace_back([&, i] { request(i); });
    EXPECT_TRUE(KvNodeServerWiringTestPeer::WaitForReadFollowers(*node_, 3));
    barrier->Open();
    leader.join();
    for (std::thread& follower : followers) follower.join();
    DiskSlabStoreTestPeer::SetReadHook(store, {});
    for (size_t i = 0; i < kRequests; ++i) {
      EXPECT_EQ(results[i], Status::kOk);
      EXPECT_EQ(values[i], "value");
    }
    EXPECT_EQ(reads.load(std::memory_order_relaxed), 1U);
    EXPECT_EQ(State(key).scoring_reads(), kRequests);
    EXPECT_EQ(State(key).actual_reads, 3U);
    EXPECT_EQ(Counters().applied, kRequests);
    EXPECT_EQ(Counters().source_copy_leader, 1U);
    EXPECT_EQ(Counters().source_copy_follower, kRequests - 1);
    EXPECT_EQ(Counters().source_disk, 0U);
    ExpectConservation(kRequests);
  }
}

TEST_F(LifetimeSuccessReadTest, TimeoutFallbackHasOnlyItsOwnSuccessEvent) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  KvNodeServerWiringTestPeer::SetCoalescing(*node_, true);
  const BlockKey key{99917, 0};
  ASSERT_EQ(Seed(key).status, Status::kOk);
  const std::shared_ptr<FlushBarrier> barrier =
      std::make_shared<FlushBarrier>();
  const OpenFlushOnExit release{*barrier};
  std::atomic<unsigned> reads{0};
  DiskSlabStore& store = KvNodeServerWiringTestPeer::Slab(*node_);
  DiskSlabStoreTestPeer::SetReadHook(store, [barrier, &reads] {
    if (reads.fetch_add(1, std::memory_order_relaxed) == 0) barrier->Block();
  });
  Status leader_status = Status::kInvalid;
  Status follower_status = Status::kInvalid;
  std::string leader_value;
  std::string follower_value;
  std::thread leader([&] { leader_status = Read(key, &leader_value, 0, 5); });
  EXPECT_TRUE(barrier->AwaitEntry());
  std::promise<void> finished;
  const std::future<void> done = finished.get_future();
  std::thread follower([&] {
    follower_status = Read(key, &follower_value, 0, 5);
    finished.set_value();
  });
  const bool fallback_finished =
      done.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  EXPECT_TRUE(fallback_finished);
  if (fallback_finished) {
    EXPECT_EQ(Counters().applied, 1U);  // Leader's physical I/O is still held.
    EXPECT_GT(KvNodeServerWiringTestPeer::ReadTimeouts(*node_), 0U);
  }
  seconds_.store(129, std::memory_order_relaxed);
  barrier->Open();
  leader.join();
  follower.join();
  DiskSlabStoreTestPeer::SetReadHook(store, {});
  EXPECT_EQ(leader_status, Status::kOk);
  EXPECT_EQ(follower_status, Status::kOk);
  EXPECT_EQ(leader_value, "value");
  EXPECT_EQ(follower_value, "value");
  EXPECT_EQ(reads.load(std::memory_order_relaxed), 2U);
  EXPECT_EQ(State(key).scoring_reads(), 2U);
  EXPECT_EQ(State(key).anchor, 29U);
  EXPECT_EQ(Counters().applied, 2U);
  EXPECT_EQ(Counters().source_copy_leader, 1U);
  EXPECT_EQ(Counters().source_copy_follower, 0U);
  EXPECT_EQ(Counters().source_disk, 1U);  // Timeout executes its own fill.
  ExpectConservation(2);
}

TEST_F(LifetimeSuccessReadTest,
       LastReleaseRemoveUsesFinalStatusAndOriginalSource) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  for (const bool fail_invalidation : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(CreateScored());
    const BlockKey key{99918 + static_cast<uint64_t>(fail_invalidation), 0};
    ASSERT_EQ(Seed(key).status, Status::kOk);
    DiskSlabStore& store = KvNodeServerWiringTestPeer::Slab(*node_);
    DiskSlabStoreTestPeer::SetReadHook(store, [&] {
      EXPECT_EQ(store.Remove(key), Status::kOk);
      if (fail_invalidation)
        DiskSlabStoreTestPeer::SetRecordWriteHook(&store, [] { return false; });
    });
    std::string value;
    const Status result = Read(key, &value);
    DiskSlabStoreTestPeer::SetReadHook(store, {});
    if (fail_invalidation) {
      EXPECT_EQ(result, Status::kIOError);
      EXPECT_EQ(State(key).actual_reads, 0U);
      ExpectConservation(0);
    } else {
      EXPECT_EQ(result, Status::kOk);
      EXPECT_EQ(value, "value");
      EXPECT_EQ(Counters().race_lost, 1U);
      EXPECT_EQ(Counters().no_binding, 0U);
      ExpectConservation(1);
    }
  }
}

TEST_F(LifetimeSuccessReadTest,
       AdmittedReadUsesDrainClockWithoutReversingSuccess) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  for (const bool hard : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(CreateScored());
    const BlockKey key{99920 + static_cast<uint64_t>(hard), 0};
    ASSERT_EQ(Seed(key).status, Status::kOk);
    DiskSlabStore& store = KvNodeServerWiringTestPeer::Slab(*node_);
    DiskSlabStoreTestPeer::SetReadHook(store, [&, hard] {
      seconds_.store(uint64_t{UINT32_MAX} + (hard ? 101 : 100),
                     std::memory_order_relaxed);
    });
    std::string value;
    const Status result = Read(key, &value);
    DiskSlabStoreTestPeer::SetReadHook(store, {});
    EXPECT_EQ(result, Status::kOk);
    EXPECT_EQ(value, "value");
    EXPECT_TRUE(node_->Healthy());
    if (hard) {
      EXPECT_EQ(Counters().clock_hard_exhausted, 1U);
      EXPECT_EQ(MetricVal(node_->MetricsText(),
                          "dfkv_slab_lifetime_read_clock_hard_exhausted_total"),
                1);
      EXPECT_EQ(State(key).actual_reads, 0U);
    } else {
      EXPECT_EQ(Counters().applied, 1U);
      EXPECT_EQ(State(key).anchor, UINT32_MAX);
    }
    ExpectConservation(1);
    EXPECT_TRUE(node_->LifetimeRestartRequired());
    seconds_.store(100, std::memory_order_relaxed);
    EXPECT_EQ(Read(key, &value), Status::kIOError);
    ExpectConservation(1);
  }
}

TEST_F(LifetimeSuccessReadTest, EmptyFailedAndMetadataOnlyReadsDoNotHeat) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99922, 0};
  ASSERT_EQ(Seed(key).status, Status::kOk);
  std::string value;
  EXPECT_EQ(Read(key, &value, 5, 0), Status::kOk);
  EXPECT_TRUE(value.empty());
  EXPECT_EQ(Read(key, &value, 6, 0), Status::kInvalid);
  EXPECT_EQ(Read({99923, 0}, &value), Status::kNotFound);
  ValueMetadata metadata;
  EXPECT_EQ(node_->LookupForKey(key, &metadata), Status::kOk);
  EXPECT_EQ(node_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kExist),
                                        key, 0, 0, nullptr, 0, &value),
            Status::kOk);
  char buffer[32] = {};
  size_t size = 99;
  EXPECT_EQ(node_->RangeIntoForKey(key, 0, 5, buffer, 0, &size), Status::kOk);
  EXPECT_EQ(size, 0U);
  EXPECT_EQ(State(key).actual_reads, 0U);
  ExpectConservation(0);
}

TEST_F(LifetimeSuccessReadTest,
       PullPreparationAndResourceReleaseNeverNotifyHere) {
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99924, 0};
  const DiskWriteResult written = Seed(key);
  ASSERT_TRUE(written.identity.valid());
  alignas(4096) char buffer[4096] = {};
  const char* data = nullptr;
  size_t size = 0;
  ASSERT_EQ(
      node_->RangeDirectForKey(key, 0, 5, buffer, sizeof(buffer), &data, &size),
      Status::kOk);
  EXPECT_EQ(std::string(data, size), "value");
  ASSERT_TRUE(KvNodeServerWiringTestPeer::Ram(*node_).PutDurable(
      key, "value", 5, written.identity));
  PreparedRead prepared =
      node_->PrepareReadForKey(key, 0, 5, buffer, sizeof(buffer));
  ASSERT_EQ(prepared.status(), Status::kOk);
  EXPECT_TRUE(prepared.Commit(Status::kOk, 5, 0));
  PreparedRead pinned;
  ASSERT_TRUE(node_->RamPinnedHitForKey(key, 0, 5, &pinned));
  EXPECT_TRUE(pinned.Commit(Status::kOk, 5, 0));
  EXPECT_EQ(State(key).actual_reads, 0U);
  ExpectConservation(0);
}

TEST_F(LifetimeSuccessReadTest, ClockOnlyLegacyHasNoAddedCompletionSampling) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir_ / "slab").string()};
  options.capacity_bytes = 1ull << 30;
  options.engine = "slab";
  options.eviction_policy = "legacy";
  options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
      100, [this] { return seconds_.load(std::memory_order_relaxed); });
  node_ = KvNodeServerWiringTestPeer::Create(options);  // Clock-only.
  ASSERT_TRUE(node_->Healthy());
  KvNodeServerWiringTestPeer::SetCoalescing(*node_, false);
  const BlockKey key{99925, 0};
  ASSERT_EQ(Seed(key).status, Status::kOk);
  DiskSlabStore& store = KvNodeServerWiringTestPeer::Slab(*node_);
  DiskSlabStoreTestPeer::SetReadHook(store, [this] {
    seconds_.store(uint64_t{UINT32_MAX} + 101, std::memory_order_relaxed);
  });
  std::string value;
  EXPECT_EQ(Read(key, &value), Status::kOk);
  DiskSlabStoreTestPeer::SetReadHook(store, {});
  seconds_.store(100, std::memory_order_relaxed);
  EXPECT_FALSE(node_->LifetimeRestartRequired());
  ExpectConservation(0);
}

TEST_F(LifetimeSuccessReadTest, SourcePreservationDoesNotBypassPromotionFence) {
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99926, 0};
  ASSERT_EQ(Seed(key).status, Status::kOk);
  DiskSlabStore& store = KvNodeServerWiringTestPeer::Slab(*node_);
  DiskSlabStoreTestPeer::SetReadHook(store, [&] {
    std::string ignored;
    EXPECT_EQ(node_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove),
                                          key, 0, 0, nullptr, 0, &ignored),
              Status::kOk);
  });
  alignas(4096) char buffer[4096] = {};
  const char* data = nullptr;
  size_t size = 0;
  const Status result =
      node_->RangeDirectForKey(key, 0, 5, buffer, sizeof(buffer), &data, &size);
  DiskSlabStoreTestPeer::SetReadHook(store, {});
  ASSERT_EQ(result, Status::kOk);
  EXPECT_EQ(std::string(data, size), "value");
  EXPECT_FALSE(KvNodeServerWiringTestPeer::Ram(*node_).Contains(key));
  EXPECT_FALSE(store.IsCached(key));
  ExpectConservation(0);  // PULL preparation is still deferred to D.
}

TEST_F(LifetimeSuccessReadTest,
       ConcurrentRemoveFencesLatePromotionBeforeForegroundRewrite) {
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99948, 0};
  const DiskWriteResult original = Seed(key, "old");
  ASSERT_EQ(original.status, Status::kOk);
  DiskSlabStore& disk = KvNodeServerWiringTestPeer::Slab(*node_);
  const RamTier& ram = KvNodeServerWiringTestPeer::Ram(*node_);
  ASSERT_FALSE(ram.Contains(key));
  FlushBarrier acquired;
  DiskSlabStoreTestPeer::SetReadHook(disk, [&] { acquired.Block(); });
  alignas(4096) char staging[4096] = {};
  const char* old_data = nullptr;
  size_t old_size = 0;
  DiskResidentIdentity old_source;
  Status old_status = Status::kInvalid;
  std::thread reader([&] {
    old_status =
        node_->RangeDirectForKey(key, 0, 3, staging, sizeof(staging), &old_data,
                                 &old_size, nullptr, &old_source);
  });
  const bool read_paused = acquired.AwaitEntry();
  std::promise<Status> removal;
  std::future<Status> removed = removal.get_future();
  std::thread remover([&] {
    std::string ignored;
    removal.set_value(
        node_->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRemove), key,
                                    0, 0, nullptr, 0, &ignored));
  });
  const bool removed_while_paused =
      removed.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  // Open on success and timeout alike, then join both before fatal assertions.
  // The disk pin preserves old bytes, not permission to publish into RAM later.
  acquired.Open();
  reader.join();
  remover.join();
  DiskSlabStoreTestPeer::SetReadHook(disk, {});
  EXPECT_TRUE(read_paused);
  EXPECT_TRUE(removed_while_paused);
  ASSERT_EQ(removed.get(), Status::kOk);
  ASSERT_EQ(old_status, Status::kOk);
  ASSERT_NE(old_data, nullptr);
  EXPECT_EQ(std::string(old_data, old_size), "old");
  EXPECT_TRUE(old_source == original.identity);
  EXPECT_FALSE(ram.Contains(key));
  EXPECT_FALSE(disk.IsCached(key));
  EXPECT_EQ(KvNodeServerWiringTestPeer::DiskReadLeases(*node_), 0U);
  ExpectConservation(0);  // Direct preparation never publishes a heat event.
  char incoming[] = "new";
  ASSERT_EQ(node_->CacheDirectForKey(key, incoming, 3, sizeof(incoming)),
            Status::kOk);
  ASSERT_TRUE(KvNodeServerWiringTestPeer::Drain(*node_));
  const char* new_data = nullptr;
  size_t new_size = 0;
  DiskResidentIdentity replacement_source;
  ASSERT_EQ(
      node_->RangeDirectForKey(key, 0, 3, staging, sizeof(staging), &new_data,
                               &new_size, nullptr, &replacement_source),
      Status::kOk);
  ASSERT_NE(new_data, nullptr);
  EXPECT_EQ(std::string(new_data, new_size), "new");
  ASSERT_TRUE(replacement_source.valid());
  EXPECT_FALSE(replacement_source == old_source);
  EXPECT_NE(replacement_source.generation, old_source.generation);
  EXPECT_EQ(State(key).actual_reads, 0U);
  std::string visible;
  ASSERT_EQ(Read(key, &visible), Status::kOk);
  EXPECT_EQ(visible, "new");
  EXPECT_EQ(State(key).actual_reads, 1U);
  EXPECT_EQ(Counters().applied, 1U);
  EXPECT_EQ(Counters().race_lost, 0U);
  ExpectConservation(1);
}

TEST_F(LifetimeSuccessReadTest, CleanRecoveryFirstReadKeepsUnknownHistory) {
  const ScopedEnv ram_off("DFKV_RAM_TIER", "0");
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  const BlockKey key{99927, 0};
  ASSERT_EQ(Seed(key).status, Status::kOk);
  node_.reset();
  seconds_.store(135, std::memory_order_relaxed);
  ASSERT_NO_FATAL_FAILURE(CreateScored());
  EXPECT_EQ(State(key).scoring_reads(), 1U);  // Staged seed, not R45's n=2.
  EXPECT_EQ(State(key).actual_reads, 0U);
  EXPECT_TRUE(State(key).history_unknown);
  ExpectConservation(0);  // Recovery's virtual seed is not a business read.
  seconds_.store(140, std::memory_order_relaxed);
  std::string value;
  ASSERT_EQ(Read(key, &value), Status::kOk);
  EXPECT_EQ(value, "value");
  const DiskLifetimeState state = State(key);
  EXPECT_EQ(state.scoring_reads(), 2U);
  EXPECT_EQ(state.actual_reads, 1U);
  EXPECT_EQ(state.anchor, 5U);
  EXPECT_TRUE(state.history_unknown);
  EXPECT_FALSE(state.write_armed());
  EXPECT_FALSE(state.first_early_read());
  EXPECT_EQ(ClassifyDiskLifetimeRead(DiskLifetimeConfig{}, state, 5),
            DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow);
  ExpectConservation(1);
}
