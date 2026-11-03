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
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// Third-party headers.
#include <gtest/gtest.h>

#include "cache/disk_cache_group.h"
#include "cache/kv_node_server.h"
#include "cache/kv_store.h"
#include "cache/ram_tier.h"
#include "cache/store_engine.h"
#include "client/key_map.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/tcp_transport.h"
#include "transport/wire.h"

namespace fs = std::filesystem;
using namespace dfkv;  // NOLINT

using namespace std::chrono_literals;

namespace dfkv {
class KvNodeServerWiringTestPeer {
 public:
  static std::unique_ptr<KvNodeServer> Create(DiskCacheGroup::Options options) {
    return std::unique_ptr<KvNodeServer>(new KvNodeServer(std::move(options)));
  }
  static void SetLimit(KvNodeServer& server, size_t limit) {
    server.put_busy_limit_ = limit;
  }
  static void SetCoalescing(KvNodeServer& server, bool enabled) {
    server.coalesce_enabled_ = enabled;
  }
  static void SetReadEpoch(KvNodeServer& server, const BlockKey& key,
                           uint64_t epoch) {
    KvNodeServer::ReadPublicationFence& fence =
        server.read_publication_[server.MutationKeyStripe(key)];
    const std::lock_guard<std::mutex> lock(fence.mutex);
    fence.epoch = epoch;
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
