#include "client/kv_client.h"

// POSIX setenv/unsetenv have no std:: equivalent and require this header.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "cache/disk_cache_group.h"
#include "cache/kv_node_server.h"
#include "cache/kv_store.h"
#include "cache/store_engine.h"
#include "client/key_map.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/tcp_transport.h"
#include "transport/transport.h"
#include "utils/con_hash.h"

using namespace dfkv;  // NOLINT

namespace {
std::string Hdr() { return "test/model"; }
struct CountingTransport : Transport {
  std::string dead;
  std::mutex mu;
  std::map<std::string, int> range_calls, cache_calls;
  bool pipelined() const override { return false; }
  Status Range(const std::string& node, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    { std::lock_guard<std::mutex> lk(mu); range_calls[node]++; }
    return node == dead ? Status::kIOError : Status::kNotFound;
  }
  Status Cache(const std::string& node, const BlockKey&, const void*, size_t) override {
    { std::lock_guard<std::mutex> lk(mu); cache_calls[node]++; }
    return node == dead ? Status::kIOError : Status::kOk;
  }
  Status Lookup(const std::string& node, const BlockKey&,
                uint64_t* value_len) override {
    *value_len = 0;
    return node == dead ? Status::kIOError : Status::kNotFound;
  }
  Status Exist(const std::string& node, const BlockKey&, bool* e) override {
    *e = false;
    return node == dead ? Status::kIOError : Status::kOk;
  }
};

uint64_t MetricValue(const std::string& text, const std::string& family) {
  const std::string prefix = family + " ";
  size_t pos = text.find(prefix);
  while (pos != std::string::npos && pos != 0 && text[pos - 1] != '\n')
    pos = text.find(prefix, pos + 1);
  EXPECT_NE(pos, std::string::npos) << text;
  return pos == std::string::npos
             ? 0
             : std::stoull(text.substr(pos + prefix.size()));
}
}  // namespace

TEST(KvClientHealth, DeadPeerShortCircuitsAfterFirstFailure) {
  CountingTransport t; t.dead = "10.0.0.9:1";
  KVClient c({{"n", "10.0.0.9:1"}}, Hdr(), &t);
  char out[64] = {0};
  EXPECT_FALSE(c.Get("k1", out, 64));
  EXPECT_FALSE(c.Get("k2", out, 64));
  EXPECT_FALSE(c.Get("k3", out, 64));
  std::lock_guard<std::mutex> lk(t.mu);
  EXPECT_EQ(t.range_calls["10.0.0.9:1"], 1);
}

TEST(KvClientHealth, HealthyPeerNotShortCircuited) {
  CountingTransport t;
  KVClient c({{"n", "10.0.0.1:1"}}, Hdr(), &t);
  char out[64] = {0};
  EXPECT_FALSE(c.Get("k1", out, 64));
  EXPECT_FALSE(c.Get("k2", out, 64));
  std::lock_guard<std::mutex> lk(t.mu);
  EXPECT_EQ(t.range_calls["10.0.0.1:1"], 2);
}

TEST(KvClientHealth, DeadPeerShortCircuitsPut) {
  CountingTransport t; t.dead = "10.0.0.9:2";
  KVClient c({{"n", "10.0.0.9:2"}}, Hdr(), &t);
  std::string v(64, 'x');
  EXPECT_FALSE(c.Put("k1", v.data(), v.size()));
  EXPECT_FALSE(c.Put("k2", v.data(), v.size()));
  std::lock_guard<std::mutex> lk(t.mu);
  EXPECT_EQ(t.cache_calls["10.0.0.9:2"], 1);
}

namespace {
// Local budget starvation: the peer was never dialed, so unlike kIOError it
// must not engage the cooldown (0812-004: cooling an innocent peer blanket-
// fails every key routed to it and collapses cache hit rates).
struct StarvedTransport : CountingTransport {
  Status Range(const std::string& node, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    { std::lock_guard<std::mutex> lk(mu); range_calls[node]++; }
    return Status::kResourceExhausted;
  }
  Status Cache(const std::string& node, const BlockKey&, const void*,
               size_t) override {
    { std::lock_guard<std::mutex> lk(mu); cache_calls[node]++; }
    return Status::kResourceExhausted;
  }
};

struct NoCompatibleTransport : CountingTransport {
  Status Range(const std::string& node, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    { std::lock_guard<std::mutex> lk(mu); range_calls[node]++; }
    return Status::kNoCompatibleRail;
  }
  Status Cache(const std::string& node, const BlockKey&, const void*,
               size_t) override {
    { std::lock_guard<std::mutex> lk(mu); cache_calls[node]++; }
    return Status::kNoCompatibleRail;
  }
};
}  // namespace

TEST(KvClientHealth, ResourceExhaustedDoesNotCoolThePeer) {
  StarvedTransport t;
  KVClient c({{"n", "10.0.0.9:3"}}, Hdr(), &t);
  char out[64] = {0};
  std::string v(64, 'x');
  // Every op fails honestly, but the peer stays healthy: each retry reaches
  // the transport instead of being short-circuited by a cooldown.
  EXPECT_FALSE(c.Get("k1", out, 64));
  EXPECT_FALSE(c.Get("k2", out, 64));
  EXPECT_FALSE(c.Put("k1", v.data(), v.size()));
  EXPECT_FALSE(c.Put("k2", v.data(), v.size()));
  std::lock_guard<std::mutex> lk(t.mu);
  EXPECT_EQ(t.range_calls["10.0.0.9:3"], 2);
  EXPECT_EQ(t.cache_calls["10.0.0.9:3"], 2);
}

TEST(KvClientHealth, NoCompatibleRailIsPeerNeutral) {
  NoCompatibleTransport t;
  const std::string peer = "10.0.0.9:4";
  KVClient c({{"n", peer}}, Hdr(), &t);
  char out[64] = {};
  const std::string value(64, 'x');

  EXPECT_FALSE(c.Get("k1", out, sizeof(out)));
  EXPECT_FALSE(c.Get("k2", out, sizeof(out)));
  EXPECT_FALSE(c.Put("k1", value.data(), value.size()));
  EXPECT_FALSE(c.Put("k2", value.data(), value.size()));

  {
    std::lock_guard<std::mutex> lk(t.mu);
    EXPECT_EQ(t.range_calls[peer], 2)
        << "a local compatibility miss must not cool the peer";
    EXPECT_EQ(t.cache_calls[peer], 2)
        << "a local compatibility miss must not short-circuit later keys";
  }
  const std::string metrics = c.MetricsSnapshot();
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_ops_served_total"), 0u);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_io_errors_total"), 0u);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_unhealthy_skips_total"), 0u);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_peer_marked_bad_total"), 0u);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_busy_suppressed_total"), 0u);
}

namespace dfkv {
namespace {

class HealthScopedEnv {
 public:
  HealthScopedEnv(const char* name, const char* value) : name_(name) {
    const char* prior = std::getenv(name);
    had_prior_ = prior != nullptr;
    if (had_prior_) prior_ = prior;
    ::setenv(name, value, 1);
  }
  ~HealthScopedEnv() {
    if (had_prior_)
      ::setenv(name_.c_str(), prior_.c_str(), 1);
    else
      ::unsetenv(name_.c_str());
  }

 private:
  std::string name_;
  std::string prior_;
  bool had_prior_ = false;
};

// Inject only the remote response status; successful writes and all TCP
// framing/decoding use the real node and file backend. Store budget generation
// and RAM completion are covered separately by the lifetime wiring tests.
class HealthStatusStore final : public KVStore {
 public:
  HealthStatusStore(const std::string& dir, uint64_t capacity,
                    std::atomic<unsigned>& attempts)
      : KVStore(KVStore::Options{dir, capacity}), attempts_(attempts) {}

  Status Cache(const BlockKey& key, const void* data, size_t len) override {
    attempts_.fetch_add(1);
    const char first = len == 0 ? '\0' : static_cast<const char*>(data)[0];
    if (first == 'F') return Status::kCacheFull;
    if (first == 'I') return Status::kIOError;
    return KVStore::Cache(key, data, len);
  }
  Status CacheDirect(const BlockKey& key, char* data, size_t len,
                     size_t /*cap*/) override {
    return Cache(key, data, len);
  }

 private:
  std::atomic<unsigned>& attempts_;
};

class KvClientHealthTcpTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("dfkv_client_health_" + std::to_string(getpid()) + "_" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(dir_);
    ASSERT_NO_FATAL_FAILURE(Start("healthy", healthy_attempts_, &healthy_));
  }
  void TearDown() override {
    faulty_.reset();
    healthy_.reset();
    std::filesystem::remove_all(dir_);
  }
  void Start(const std::string& name, std::atomic<unsigned>& attempts,
             std::unique_ptr<KvNodeServer>* server) {
    DiskCacheGroup::Options options;
    options.cache_dirs = {(dir_ / name).string()};
    options.capacity_bytes = 1U << 20;
    options.engine = "file";
    options.eviction_policy = "legacy";
    options.engine_factory = [&attempts](const std::string& dir,
                                         uint64_t capacity) {
      return std::make_unique<HealthStatusStore>(dir, capacity, attempts);
    };
    *server = std::make_unique<KvNodeServer>(std::move(options));
    ASSERT_TRUE((*server)->Healthy());
    ASSERT_EQ((*server)->Start(0), Status::kOk);
  }
  static std::string Address(const KvNodeServer& server) {
    return "127.0.0.1:" + std::to_string(server.port());
  }
  static std::string KeyFor(const ConHash& ring, const std::string& owner,
                            const std::string& prefix) {
    for (unsigned i = 0; i < 10000; ++i) {
      const std::string key = prefix + std::to_string(i);
      std::string selected;
      if (ring.Lookup(key, &selected) && selected == owner) return key;
    }
    ADD_FAILURE() << "could not find a key for " << owner;
    return {};
  }
  const HealthScopedEnv ram_off_{"DFKV_RAM_TIER", "0"};
  const HealthScopedEnv probe_off_{"DFKV_PROBE_INTERVAL_MS", "0"};
  std::filesystem::path dir_;
  std::atomic<unsigned> healthy_attempts_{0};
  std::atomic<unsigned> faulty_attempts_{0};
  std::unique_ptr<KvNodeServer> healthy_;
  std::unique_ptr<KvNodeServer> faulty_;
};

TEST_F(KvClientHealthTcpTest, RepeatedCacheFullDoesNotCoolScalarTcpPeer) {
  TcpTransport transport;
  KVClient client({{"healthy", Address(*healthy_)}}, Hdr(), &transport, 1);
  for (unsigned i = 0; i < 6; ++i) EXPECT_FALSE(client.Put("retry", "Full", 4));
  EXPECT_EQ(healthy_attempts_.load(), 6U);
  EXPECT_EQ(MetricValue(client.MetricsSnapshot(),
                        "dfkv_client_peer_marked_bad_total"),
            0U);
  EXPECT_EQ(
      MetricValue(client.MetricsSnapshot(), "dfkv_client_io_errors_total"), 0U);
  EXPECT_EQ(MetricValue(client.MetricsSnapshot(),
                        "dfkv_client_unhealthy_skips_total"),
            0U);
  ASSERT_TRUE(client.Put("retry", "Success", 7));
  EXPECT_EQ(healthy_attempts_.load(), 7U);
  std::string value(7, '\0');
  EXPECT_TRUE(client.Get("retry", value.data(), value.size()));
  EXPECT_EQ(value, "Success");
}

TEST_F(KvClientHealthTcpTest, RepeatedCacheFullDoesNotCoolTcpBatchOrSg) {
  TcpTransport transport;
  KVClient client({{"healthy", Address(*healthy_)}}, Hdr(), &transport, 1);
  for (const bool scatter : {false, true}) {
    SCOPED_TRACE(scatter);
    const unsigned before = healthy_attempts_.load();
    for (unsigned i = 0; i < 4; ++i) {
      const std::vector<bool> result =
          scatter
              ? client.BatchPutSg(
                    {{"full-a", {"Full"}, {4}}, {"full-b", {"Full"}, {4}}})
              : client.BatchPut({{"full-a", "Full", 4}, {"full-b", "Full", 4}});
      EXPECT_EQ(result, (std::vector<bool>{false, false}));
    }
    EXPECT_EQ(healthy_attempts_.load() - before, 8U);
    const std::string key = scatter ? "sg-success" : "batch-success";
    const std::vector<bool> mixed =
        scatter ? client.BatchPutSg(
                      {{key, {"Success"}, {7}}, {"full", {"Full"}, {4}}})
                : client.BatchPut({{key, "Success", 7}, {"full", "Full", 4}});
    EXPECT_EQ(mixed, (std::vector<bool>{true, false}));
    std::string value(7, '\0');
    ASSERT_TRUE(client.Get(key, value.data(), value.size()));
    EXPECT_EQ(value, "Success");
  }
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_peer_marked_bad_total"), 0U);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_io_errors_total"), 0U);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_unhealthy_skips_total"), 0U);
}

TEST_F(KvClientHealthTcpTest, MixedTcpBatchIsolatesIoPeerNotCacheFullPeer) {
  ASSERT_NO_FATAL_FAILURE(Start("faulty", faulty_attempts_, &faulty_));
  const std::string healthy = Address(*healthy_);
  const std::string faulty = Address(*faulty_);
  TcpTransport transport;
  // Exercise the real response decoder before checking KVClient's bool API.
  EXPECT_EQ(transport.Cache(healthy, ToBlockKey(Hdr(), "raw-ok"), "Success", 7),
            Status::kOk);
  EXPECT_EQ(transport.Cache(healthy, ToBlockKey(Hdr(), "raw-full"), "Full", 4),
            Status::kCacheFull);
  EXPECT_EQ(transport.Cache(faulty, ToBlockKey(Hdr(), "raw-io"), "Io", 2),
            Status::kIOError);
  KVClient client({{"healthy", healthy}, {"faulty", faulty}}, Hdr(), &transport,
                  1);
  ConHash ring;
  ring.AddNode("healthy");
  ring.AddNode("faulty");
  ring.Build();
  const std::string good_key = KeyFor(ring, "healthy", "good-");
  const std::string full_key = KeyFor(ring, "healthy", "full-");
  const std::string io_key = KeyFor(ring, "faulty", "io-");
  ASSERT_FALSE(good_key.empty());
  ASSERT_FALSE(full_key.empty());
  ASSERT_FALSE(io_key.empty());
  EXPECT_EQ(
      client.BatchPut(
          {{good_key, "Success", 7}, {full_key, "Full", 4}, {io_key, "Io", 2}}),
      (std::vector<bool>{true, false, false}));
  EXPECT_EQ(healthy_attempts_.load(), 4U);
  EXPECT_EQ(faulty_attempts_.load(), 2U);
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_io_errors_total"), 1U);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_peer_marked_bad_total"), 1U);
  // A separate, never-served peer really cools down; the peer returning
  // CacheFull remains eligible, including a later successful retry.
  EXPECT_FALSE(client.Put(io_key, "Success", 7));
  EXPECT_EQ(faulty_attempts_.load(), 2U);
  EXPECT_EQ(MetricValue(client.MetricsSnapshot(),
                        "dfkv_client_unhealthy_skips_total"),
            1U);
  ASSERT_TRUE(client.Put(full_key, "Success", 7));
  EXPECT_EQ(healthy_attempts_.load(), 5U);
  std::string value(7, '\0');
  ASSERT_TRUE(client.Get(good_key, value.data(), value.size()));
  EXPECT_EQ(value, "Success");
}

}  // namespace
}  // namespace dfkv
