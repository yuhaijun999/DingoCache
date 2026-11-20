#include <gtest/gtest.h>
// POSIX environment and temporary-directory APIs require their C header.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "cache/disk_cache_group.h"
#include "cache/kv_node_server.h"
#include "client/key_map.h"
#include "client/kv_client.h"
#include "client/node_dedup.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/tcp_transport.h"
#include "transport/transport.h"

namespace dfkv {
namespace {

class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    const char* previous = std::getenv(name);
    if (previous != nullptr) {
      had_value_ = true;
      previous_ = previous;
    }
    EXPECT_EQ(::setenv(name, value, 1), 0);
  }
  ~ScopedEnv() {
    if (had_value_) {
      ::setenv(name_.c_str(), previous_.c_str(), 1);
    } else {
      ::unsetenv(name_.c_str());
    }
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;

 private:
  std::string name_;
  std::string previous_;
  bool had_value_ = false;
};

class DedupNamespace {
 public:
  DedupNamespace() {
    static std::atomic<uint64_t> sequence{0};
    value_ = "cpu-dedup-status-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1));
    segment_ = NodeDedup::EnvSegmentName(ToBlockKey(value_, "").digest_hi);
    ::shm_unlink(segment_.c_str());
  }
  ~DedupNamespace() { ::shm_unlink(segment_.c_str()); }
  const std::string& value() const { return value_; }
  std::unique_ptr<NodeDedup> OpenPeer() const {
    return NodeDedup::FromEnv(ToBlockKey(value_, "").digest_hi);
  }

 private:
  std::string value_;
  std::string segment_;
};

struct Reply {
  Status status = Status::kOk;
  std::string bytes = std::string("a\0c", 3);
};

class DedupStatusTransport final : public Transport {
 public:
  explicit DedupStatusTransport(std::string key_namespace)
      : key_namespace_(std::move(key_namespace)) {}
  void Set(const std::string& key, Reply reply) {
    replies_[ToBlockKey(key_namespace_, key).Filename()] = std::move(reply);
  }
  bool pipelined() const override {
    if (throw_on_pipeline_) throw std::runtime_error("dedup pipeline test");
    return true;
  }
  Status Cache(const std::string&, const BlockKey&, const void*,
               size_t) override {
    return Status::kInvalid;
  }
  Status Range(const std::string&, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    ADD_FAILURE() << "Status GET must retain RangeInto";
    return Status::kIOError;
  }
  Status Lookup(const std::string&, const BlockKey&, uint64_t*) override {
    return Status::kInvalid;
  }
  Status Exist(const std::string&, const BlockKey&, bool*) override {
    return Status::kInvalid;
  }
  std::vector<Status> RangeInto(const std::string&,
                                const std::vector<BlockKey>& keys,
                                const std::vector<RangeDst>& destinations,
                                std::vector<uint64_t>* lengths) override {
    calls_.fetch_add(1);
    submitted_.fetch_add(keys.size());
    if (blocked_) {
      std::unique_lock<std::mutex> lock(mutex_);
      entered_ = true;
      condition_.notify_all();
      if (!condition_.wait_for(lock, std::chrono::seconds(3),
                               [this] { return released_; })) {
        ADD_FAILURE() << "Test failed to release transport";
        return std::vector<Status>(keys.size(), Status::kIOError);
      }
    }
    if (throw_on_read_) throw std::runtime_error("dedup status test");
    std::vector<Status> statuses;
    lengths->clear();
    for (size_t i = 0; i < keys.size(); ++i) {
      const Reply& reply = replies_.at(keys[i].Filename());
      statuses.push_back(reply.status);
      lengths->push_back(reply.bytes.size());
      if (reply.status == Status::kOk && destinations[i].n != 0) {
        std::memcpy(destinations[i].payload, reply.bytes.data(),
                    std::min(reply.bytes.size(), destinations[i].n));
      }
    }
    if (malformed_) statuses.clear();
    return statuses;
  }
  void Block() { blocked_ = true; }
  bool WaitUntilEntered() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(2),
                               [this] { return entered_; });
  }
  void Release() {
    const std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    condition_.notify_all();
  }
  uint64_t calls() const { return calls_.load(); }
  uint64_t submitted() const { return submitted_.load(); }
  void set_throw_on_read(bool value) { throw_on_read_ = value; }
  void set_malformed(bool value) { malformed_ = value; }
  void set_throw_on_pipeline(bool value) { throw_on_pipeline_ = value; }

 private:
  std::string key_namespace_;
  std::map<std::string, Reply> replies_;
  std::atomic<uint64_t> calls_{0};
  std::atomic<uint64_t> submitted_{0};
  bool throw_on_read_ = false;
  bool malformed_ = false;
  bool throw_on_pipeline_ = false;
  bool blocked_ = false;
  bool entered_ = false;
  bool released_ = false;
  std::mutex mutex_;
  std::condition_variable condition_;
};

uint64_t Counter(const std::string& metrics, const std::string& name,
                 bool operation = true) {
  const std::string prefix = "\n" + name + (operation ? "{op=\"get\"} " : " ");
  const size_t position = metrics.find(prefix);
  EXPECT_NE(position, std::string::npos);
  return position == std::string::npos
             ? 0
             : std::stoull(metrics.substr(position + prefix.size()));
}

class BatchGetAutoDedupStatus : public ::testing::Test {
 protected:
  const ScopedEnv enabled_{"DFKV_CLIENT_NODE_DEDUP", "1"};
  const ScopedEnv arena_{"DFKV_NODE_DEDUP_ARENA_MB", "1"};
  const ScopedEnv slots_{"DFKV_NODE_DEDUP_SLOTS", "1024"};
  const ScopedEnv wait_{"DFKV_NODE_DEDUP_WAIT_MS", "20"};
  const ScopedEnv takeover_{"DFKV_NODE_DEDUP_TAKEOVER_MS", "2000"};
  const ScopedEnv ttl_{"DFKV_NODE_DEDUP_TTL_MS", "5000"};
  const ScopedEnv retries_{"DFKV_GET_MISS_RETRIES", "0"};
  DedupNamespace namespace_;
};

TEST_F(BatchGetAutoDedupStatus, DuplicateKeysFetchOnceAndCountEachLogicalRead) {
  DedupStatusTransport transport(namespace_.value());
  transport.Set("key", {});
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  std::array<char, 4> first{};
  std::array<char, 4> second{};
  const std::vector<KvGetItem> items{{"key", first.data(), first.size()},
                                     {"key", second.data(), second.size()}};
  std::vector<size_t> lengths{999};
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(items, &lengths),
            (std::vector<Status>{Status::kOk, Status::kOk}));
  EXPECT_EQ(lengths, (std::vector<size_t>{3, 3}));
  EXPECT_EQ(std::string(first.data(), 3), std::string("a\0c", 3));
  EXPECT_EQ(first, second);
  EXPECT_EQ(transport.submitted(), 1);
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 1);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 6);
  EXPECT_EQ(Counter(metrics, "dfkv_client_dedup_fetches_total", false), 1);
  EXPECT_EQ(Counter(metrics, "dfkv_client_dedup_wait_hits_total", false), 1);
  first.fill('x');
  second.fill('y');
  lengths = {999};
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(items, &lengths),
            (std::vector<Status>{Status::kOk, Status::kOk}));
  EXPECT_EQ(lengths, (std::vector<size_t>{3, 3}));
  EXPECT_EQ(std::string(first.data(), 3), std::string("a\0c", 3));
  EXPECT_EQ(std::string(second.data(), 3), std::string("a\0c", 3));
  EXPECT_EQ(transport.submitted(), 1);
  const std::string ready_metrics = client.MetricsSnapshot();
  EXPECT_EQ(Counter(ready_metrics, "dfkv_client_dedup_hits_total", false), 2);
  EXPECT_EQ(Counter(ready_metrics, "dfkv_client_op_requests_total"), 2);
  EXPECT_EQ(Counter(ready_metrics, "dfkv_client_op_keys_total"), 4);
  EXPECT_EQ(Counter(ready_metrics, "dfkv_client_op_hits_total"), 4);
  EXPECT_EQ(Counter(ready_metrics, "dfkv_client_op_bytes_total"), 12);
}

TEST_F(BatchGetAutoDedupStatus, DisabledUsesDirectStatusAndDoesNotMerge) {
  const ScopedEnv disabled("DFKV_CLIENT_NODE_DEDUP", "0");
  DedupStatusTransport transport(namespace_.value());
  transport.Set("key", {});
  transport.Set("missing", {Status::kNotFound, ""});
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  std::array<char, 4> first{};
  std::array<char, 4> second{};
  const std::vector<KvGetItem> items{{"key", first.data(), 4},
                                     {"key", second.data(), 4},
                                     {"missing", nullptr, 0}};
  std::vector<size_t> lengths{999};
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(items, &lengths),
            (std::vector<Status>{Status::kOk, Status::kOk, Status::kNotFound}));
  EXPECT_EQ(lengths, (std::vector<size_t>{3, 3, 0}));
  EXPECT_EQ(transport.submitted(), 3);
  EXPECT_EQ(Counter(client.MetricsSnapshot(), "dfkv_client_op_requests_total"),
            1);
}

TEST_F(BatchGetAutoDedupStatus, PreservesFailuresWithoutNegativeCaching) {
  const std::array<Status, 7> failures{
      Status::kNotFound,        Status::kCacheFull, Status::kQuotaExceeded,
      Status::kIOError,         Status::kInvalid,   Status::kResourceExhausted,
      Status::kNoCompatibleRail};
  for (const Status failure : failures) {
    SCOPED_TRACE(StatusName(failure));
    const DedupNamespace isolated;
    DedupStatusTransport failing_transport(isolated.value());
    failing_transport.Set("key", {failure, ""});
    KVClient failing({{"n", "unused:1"}}, isolated.value(), &failing_transport,
                     1);
    std::array<char, 4> output{};
    const std::vector<KvGetItem> items{{"key", output.data(), output.size()}};
    std::vector<size_t> lengths{999};
    EXPECT_EQ(failing.BatchGetAutoDeduplicatedWithStatus(items, &lengths),
              (std::vector<Status>{failure}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0}));
    // A fresh client avoids a deliberate I/O-error peer cooldown. Failure must
    // release ownership, rather than caching an absent/error result or leaving
    // the next reader stuck behind the abandoned generation.
    std::unique_ptr<NodeDedup> peer = isolated.OpenPeer();
    ASSERT_NE(peer, nullptr);
    uint64_t token = 0;
    size_t got = 999;
    const BlockKey key = ToBlockKey(isolated.value(), "key");
    EXPECT_EQ(peer->ClaimAuto(key, output.size(), output.data(), &got, &token),
              NodeDedup::Role::kFetch);
    EXPECT_NE(token, 0);
    peer->Abort(key, NodeDedup::Kind::kData, token);
    DedupStatusTransport recovered_transport(isolated.value());
    recovered_transport.Set("key", {});
    KVClient recovered({{"n", "unused:1"}}, isolated.value(),
                       &recovered_transport, 1);
    EXPECT_EQ(recovered.BatchGetAutoDeduplicatedWithStatus(items, &lengths),
              (std::vector<Status>{Status::kOk}));
    EXPECT_EQ(lengths, (std::vector<size_t>{3}));
    EXPECT_EQ(recovered_transport.submitted(), 1);
  }
}

TEST_F(BatchGetAutoDedupStatus, TwoClientsShareSuccessfulInFlightRead) {
  const ScopedEnv longer_wait("DFKV_NODE_DEDUP_WAIT_MS", "500");
  DedupStatusTransport owner_transport(namespace_.value());
  DedupStatusTransport waiter_transport(namespace_.value());
  owner_transport.Set("shared", {});
  waiter_transport.Set("marker", {Status::kOk, "mark"});
  owner_transport.Block();
  waiter_transport.Block();
  KVClient owner({{"n", "unused:1"}}, namespace_.value(), &owner_transport, 1);
  KVClient waiter({{"n", "unused:1"}}, namespace_.value(), &waiter_transport,
                  1);
  std::array<char, 4> owner_output{};
  std::array<char, 4> waiter_output{};
  std::array<char, 4> marker{};
  std::vector<size_t> owner_lengths;
  std::vector<size_t> waiter_lengths;
  std::future<std::vector<Status>> first = std::async(std::launch::async, [&] {
    return owner.BatchGetAutoDeduplicatedWithStatus(
        {{"shared", owner_output.data(), owner_output.size()}}, &owner_lengths);
  });
  EXPECT_TRUE(owner_transport.WaitUntilEntered());
  std::future<std::vector<Status>> second = std::async(std::launch::async, [&] {
    // Both claims occur before marker I/O. Entering the marker transport thus
    // proves that shared is already a waiter, without sleeps or test hooks.
    return waiter.BatchGetAutoDeduplicatedWithStatus(
        {{"marker", marker.data(), marker.size()},
         {"shared", waiter_output.data(), waiter_output.size()}},
        &waiter_lengths);
  });
  EXPECT_TRUE(waiter_transport.WaitUntilEntered());
  owner_transport.Release();
  EXPECT_EQ(first.get(), (std::vector<Status>{Status::kOk}));
  waiter_transport.Release();
  EXPECT_EQ(second.get(), (std::vector<Status>{Status::kOk, Status::kOk}));
  EXPECT_EQ(owner_output, waiter_output);
  EXPECT_EQ(owner_lengths, (std::vector<size_t>{3}));
  EXPECT_EQ(waiter_lengths, (std::vector<size_t>{4, 3}));
  EXPECT_EQ(owner_transport.submitted(), 1);
  EXPECT_EQ(waiter_transport.submitted(), 1);
  EXPECT_EQ(Counter(waiter.MetricsSnapshot(),
                    "dfkv_client_dedup_wait_hits_total", false),
            1);
  EXPECT_EQ(Counter(waiter.MetricsSnapshot(), "dfkv_client_op_requests_total"),
            1);
  EXPECT_EQ(Counter(waiter.MetricsSnapshot(), "dfkv_client_op_keys_total"), 2);
  EXPECT_EQ(Counter(waiter.MetricsSnapshot(), "dfkv_client_op_hits_total"), 2);
  EXPECT_EQ(Counter(waiter.MetricsSnapshot(), "dfkv_client_op_bytes_total"), 7);
}

TEST_F(BatchGetAutoDedupStatus, AbortedOwnerFallsBackToItsOwnNativeStatus) {
  for (const Status fallback :
       {Status::kNotFound, Status::kIOError, Status::kResourceExhausted}) {
    const DedupNamespace isolated;
    std::unique_ptr<NodeDedup> peer = isolated.OpenPeer();
    ASSERT_NE(peer, nullptr);
    std::array<char, 4> output{};
    size_t got = 0;
    uint64_t token = 0;
    const BlockKey key = ToBlockKey(isolated.value(), "shared");
    ASSERT_EQ(peer->ClaimAuto(key, output.size(), output.data(), &got, &token),
              NodeDedup::Role::kFetch);
    DedupStatusTransport transport(isolated.value());
    transport.Set("marker", {});
    transport.Set("shared", {fallback, ""});
    transport.Block();
    KVClient client({{"n", "unused:1"}}, isolated.value(), &transport, 1);
    std::array<char, 4> marker{};
    std::vector<size_t> lengths;
    std::future<std::vector<Status>> result =
        std::async(std::launch::async, [&] {
          return client.BatchGetAutoDeduplicatedWithStatus(
              {{"marker", marker.data(), marker.size()},
               {"shared", output.data(), output.size()}},
              &lengths);
        });
    EXPECT_TRUE(transport.WaitUntilEntered());
    peer->Abort(key, NodeDedup::Kind::kData, token);
    transport.Release();
    EXPECT_EQ(result.get(), (std::vector<Status>{Status::kOk, fallback}));
    EXPECT_EQ(lengths, (std::vector<size_t>{3, 0}));
    EXPECT_EQ(transport.submitted(), 2);
    const std::string metrics = client.MetricsSnapshot();
    EXPECT_EQ(Counter(metrics, "dfkv_client_dedup_wait_timeouts_total", false),
              1);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 1);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 2);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 1);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 3);
  }
}

TEST_F(BatchGetAutoDedupStatus, SlowOwnerTimesOutWithoutStealingItsToken) {
  std::unique_ptr<NodeDedup> peer = namespace_.OpenPeer();
  ASSERT_NE(peer, nullptr);
  std::array<char, 4> output{};
  size_t got = 0;
  uint64_t token = 0;
  const BlockKey key = ToBlockKey(namespace_.value(), "shared");
  ASSERT_EQ(peer->ClaimAuto(key, output.size(), output.data(), &got, &token),
            NodeDedup::Role::kFetch);
  DedupStatusTransport transport(namespace_.value());
  transport.Set("shared", {Status::kNotFound, ""});
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  std::vector<size_t> lengths{999};
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(
                {{"shared", output.data(), output.size()}}, &lengths),
            (std::vector<Status>{Status::kNotFound}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  EXPECT_EQ(transport.submitted(), 1);
  EXPECT_EQ(Counter(client.MetricsSnapshot(),
                    "dfkv_client_dedup_wait_timeouts_total", false),
            1);
  // An unclaimed fallback must not abort another owner's generation.
  peer->Publish(key, NodeDedup::Kind::kData, token, "late", 4);
  uint64_t ignored = 0;
  EXPECT_EQ(peer->ClaimAuto(key, output.size(), output.data(), &got, &ignored),
            NodeDedup::Role::kHit);
  EXPECT_EQ(std::string(output.data(), got), "late");
}

TEST_F(BatchGetAutoDedupStatus, KeysAndNamespacesDoNotSharePayloads) {
  DedupStatusTransport first_transport(namespace_.value());
  first_transport.Set("one", {Status::kOk, "one"});
  first_transport.Set("two", {Status::kOk, "two"});
  KVClient first({{"n", "unused:1"}}, namespace_.value(), &first_transport, 1);
  std::array<char, 4> first_output{};
  std::array<char, 4> second_output{};
  std::vector<size_t> lengths;
  EXPECT_EQ(
      first.BatchGetAutoDeduplicatedWithStatus(
          {{"one", first_output.data(), 4}, {"two", second_output.data(), 4}},
          &lengths),
      (std::vector<Status>{Status::kOk, Status::kOk}));
  EXPECT_EQ(std::string(first_output.data(), 3), "one");
  EXPECT_EQ(std::string(second_output.data(), 3), "two");
  const DedupNamespace other;
  DedupStatusTransport other_transport(other.value());
  other_transport.Set("one", {Status::kOk, "else"});
  KVClient second({{"n", "unused:1"}}, other.value(), &other_transport, 1);
  EXPECT_EQ(second.BatchGetAutoDeduplicatedWithStatus(
                {{"one", first_output.data(), 4}}, nullptr),
            (std::vector<Status>{Status::kOk}));
  EXPECT_EQ(std::string(first_output.data(), 4), "else");
  EXPECT_EQ(first_transport.submitted(), 2);
  EXPECT_EQ(other_transport.submitted(), 1);
}

TEST_F(BatchGetAutoDedupStatus, InvalidCapacityAndZeroLengthAreIndependent) {
  DedupStatusTransport transport(namespace_.value());
  transport.Set("valid", {});
  transport.Set("large", {Status::kOk, "large"});
  transport.Set("empty", {Status::kOk, ""});
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  std::array<char, 4> valid{};
  std::array<char, 4> large{};
  const std::vector<KvGetItem> items{{"", valid.data(), valid.size()},
                                     {"invalid", nullptr, 4},
                                     {"valid", valid.data(), valid.size()},
                                     {"large", large.data(), large.size()},
                                     {"empty", nullptr, 0}};
  std::vector<size_t> lengths{999};
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(items, &lengths),
            (std::vector<Status>{Status::kInvalid, Status::kInvalid,
                                 Status::kOk, Status::kInvalid, Status::kOk}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0, 0, 3, 0, 0}));
  EXPECT_EQ(transport.submitted(), 3);
  EXPECT_EQ(Counter(client.MetricsSnapshot(), "dfkv_client_op_requests_total"),
            1);
  EXPECT_EQ(Counter(client.MetricsSnapshot(), "dfkv_client_op_keys_total"), 5);
  EXPECT_EQ(Counter(client.MetricsSnapshot(), "dfkv_client_op_hits_total"), 2);
  EXPECT_EQ(Counter(client.MetricsSnapshot(), "dfkv_client_op_bytes_total"), 3);
  std::unique_ptr<NodeDedup> peer = namespace_.OpenPeer();
  ASSERT_NE(peer, nullptr);
  uint64_t token = 0;
  size_t got = 0;
  const BlockKey invalid_key = ToBlockKey(namespace_.value(), "invalid");
  EXPECT_EQ(
      peer->ClaimAuto(invalid_key, large.size(), large.data(), &got, &token),
      NodeDedup::Role::kFetch);
  EXPECT_NE(token, 0);
  peer->Abort(invalid_key, NodeDedup::Kind::kData, token);
}

TEST_F(BatchGetAutoDedupStatus, ExistingDirectAndLegacyMethodsRemainUnchanged) {
  DedupStatusTransport transport(namespace_.value());
  transport.Set("key", {});
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  std::array<char, 4> output{};
  const std::vector<KvGetItem> items{{"key", output.data(), output.size()}};
  std::vector<size_t> lengths;
  ASSERT_EQ(client.BatchGetAutoDeduplicatedWithStatus(items, &lengths),
            (std::vector<Status>{Status::kOk}));
  transport.Set("key", {Status::kOk, "new!"});
  EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths),
            (std::vector<Status>{Status::kOk}));
  EXPECT_EQ(std::string(output.data(), lengths[0]), "new!");
  EXPECT_EQ(transport.submitted(), 2);
  // Legacy rendezvous still sees the previously published success, proving
  // that the direct-only method neither claimed nor refreshed shared memory.
  EXPECT_EQ(client.BatchGetAuto(items, &lengths), (std::vector<bool>{true}));
  EXPECT_EQ(std::string(output.data(), lengths[0]), std::string("a\0c", 3));
  EXPECT_EQ(transport.submitted(), 2);
}

TEST_F(BatchGetAutoDedupStatus, MalformedAndThrownReadsReleaseClaims) {
  for (const bool malformed : {false, true}) {
    const DedupNamespace isolated;
    DedupStatusTransport transport(isolated.value());
    transport.Set("key", {});
    transport.set_throw_on_read(!malformed);
    transport.set_malformed(malformed);
    KVClient client({{"n", "unused:1"}}, isolated.value(), &transport, 1);
    std::array<char, 4> output{};
    std::vector<size_t> lengths{999};
    EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(
                  {{"key", output.data(), output.size()}}, &lengths),
              (std::vector<Status>{Status::kIOError}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0}));
    std::unique_ptr<NodeDedup> peer = isolated.OpenPeer();
    ASSERT_NE(peer, nullptr);
    size_t got = 0;
    uint64_t token = 0;
    const BlockKey key = ToBlockKey(isolated.value(), "key");
    EXPECT_EQ(peer->ClaimAuto(key, output.size(), output.data(), &got, &token),
              NodeDedup::Role::kFetch);
    EXPECT_NE(token, 0);
    peer->Abort(key, NodeDedup::Kind::kData, token);
  }
}

TEST_F(BatchGetAutoDedupStatus, EmptyBatchClearsLengthsAndRecordsOneRequest) {
  DedupStatusTransport transport(namespace_.value());
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  std::vector<size_t> lengths{999};
  EXPECT_TRUE(client.BatchGetAutoDeduplicatedWithStatus({}, &lengths).empty());
  EXPECT_TRUE(lengths.empty());
  EXPECT_EQ(transport.submitted(), 0);
  EXPECT_EQ(Counter(client.MetricsSnapshot(), "dfkv_client_op_requests_total"),
            1);
  EXPECT_EQ(Counter(client.MetricsSnapshot(), "dfkv_client_op_keys_total"), 0);
}

TEST_F(BatchGetAutoDedupStatus, PropagatedExceptionAbortsAllOwnedClaims) {
  DedupStatusTransport transport(namespace_.value());
  transport.Set("one", {});
  transport.Set("two", {});
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  transport.set_throw_on_pipeline(true);
  std::array<char, 4> first{};
  std::array<char, 4> second{};
  std::vector<size_t> lengths{999};
  EXPECT_THROW(client.BatchGetAutoDeduplicatedWithStatus(
                   {{"one", first.data(), first.size()},
                    {"two", second.data(), second.size()}},
                   &lengths),
               std::runtime_error);
  EXPECT_EQ(lengths, (std::vector<size_t>{0, 0}));
  EXPECT_EQ(transport.submitted(), 0);
  std::unique_ptr<NodeDedup> peer = namespace_.OpenPeer();
  ASSERT_NE(peer, nullptr);
  for (const std::string& name : {std::string("one"), std::string("two")}) {
    size_t got = 0;
    uint64_t token = 0;
    const BlockKey key = ToBlockKey(namespace_.value(), name);
    EXPECT_EQ(peer->ClaimAuto(key, first.size(), first.data(), &got, &token),
              NodeDedup::Role::kFetch);
    EXPECT_NE(token, 0);
    peer->Abort(key, NodeDedup::Kind::kData, token);
  }
  transport.set_throw_on_pipeline(false);
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 1);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 0);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 0);
}

TEST_F(BatchGetAutoDedupStatus, ReadyPayloadTooLargeStillRequiresNativeStatus) {
  std::unique_ptr<NodeDedup> peer = namespace_.OpenPeer();
  ASSERT_NE(peer, nullptr);
  std::array<char, 4> output{};
  uint64_t token = 0;
  size_t got = 0;
  const BlockKey key = ToBlockKey(namespace_.value(), "key");
  ASSERT_EQ(peer->ClaimAuto(key, output.size(), output.data(), &got, &token),
            NodeDedup::Role::kFetch);
  peer->Publish(key, NodeDedup::Kind::kData, token, "wide", 4);
  DedupStatusTransport transport(namespace_.value());
  transport.Set("key", {Status::kOk, "wide"});
  KVClient client({{"n", "unused:1"}}, namespace_.value(), &transport, 1);
  std::vector<size_t> lengths{999};
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(
                {{"key", output.data(), 2}}, &lengths),
            (std::vector<Status>{Status::kInvalid}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  EXPECT_EQ(transport.submitted(), 1);
  EXPECT_EQ(
      Counter(client.MetricsSnapshot(), "dfkv_client_dedup_hits_total", false),
      0);
}

class BatchGetAutoDedupStatusTcp : public BatchGetAutoDedupStatus {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-dedup-status-XXXXXX")
            .string();
    const char* directory = ::mkdtemp(pattern.data());
    ASSERT_NE(directory, nullptr);
    directory_ = directory;
    DiskCacheGroup::Options options;
    options.cache_dirs = {directory_};
    options.capacity_bytes = 1u << 20;
    options.engine = "file";
    server_ = std::make_unique<KvNodeServer>(options);
    ASSERT_EQ(server_->Start(0), Status::kOk);
  }
  void TearDown() override {
    server_.reset();
    if (!directory_.empty()) std::filesystem::remove_all(directory_);
  }
  std::string directory_;
  std::unique_ptr<KvNodeServer> server_;
};

TEST_F(BatchGetAutoDedupStatusTcp, RealBytesAbsenceAndRemoveInvalidation) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  namespace_.value(), &transport, 1);
  const std::string value("a\0c", 3);
  ASSERT_TRUE(client.Put("key", value.data(), value.size()));
  std::array<char, 4> output{};
  std::array<char, 4> duplicate{};
  std::array<char, 4> absent{};
  std::vector<size_t> lengths;
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(
                {{"key", output.data(), output.size()},
                 {"key", duplicate.data(), duplicate.size()},
                 {"absent", absent.data(), absent.size()}},
                &lengths),
            (std::vector<Status>{Status::kOk, Status::kOk, Status::kNotFound}));
  EXPECT_EQ(lengths, (std::vector<size_t>{3, 3, 0}));
  EXPECT_EQ(std::string(output.data(), 3), value);
  EXPECT_EQ(output, duplicate);
  ASSERT_TRUE(client.Remove("key"));
  EXPECT_EQ(client.BatchGetAutoDeduplicatedWithStatus(
                {{"key", output.data(), output.size()}}, &lengths),
            (std::vector<Status>{Status::kNotFound}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
}

}  // namespace
}  // namespace dfkv
