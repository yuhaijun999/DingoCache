#include <gtest/gtest.h>

#include "client/dfkv_c_api.h"
// POSIX environment and temporary-directory APIs require their C header.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
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

struct Reply {
  Status status = Status::kOk;
  std::string bytes = std::string("a\0c", 3);
};

class CApiDedupTransport final : public Transport {
 public:
  explicit CApiDedupTransport(std::string key_namespace)
      : key_namespace_(std::move(key_namespace)) {}
  void Set(const std::string& key, Reply reply) {
    replies_[ToBlockKey(key_namespace_, key).Filename()] = std::move(reply);
  }
  bool pipelined() const override {
    if (throw_on_pipeline_)
      throw std::runtime_error("C dedup pipeline failure");
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
    submitted_.fetch_add(keys.size(), std::memory_order_relaxed);
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
    return statuses;
  }
  size_t submitted() const {
    return submitted_.load(std::memory_order_relaxed);
  }
  void set_throw_on_pipeline(bool value) { throw_on_pipeline_ = value; }

 private:
  std::string key_namespace_;
  std::map<std::string, Reply> replies_;
  std::atomic<size_t> submitted_{0};
  bool throw_on_pipeline_ = false;
};

uint64_t Counter(const std::string& metrics, const std::string& name) {
  const std::string prefix = "\n" + name + "{op=\"get\"} ";
  const size_t position = metrics.find(prefix);
  EXPECT_NE(position, std::string::npos);
  return position == std::string::npos
             ? 0
             : std::stoull(metrics.substr(position + prefix.size()));
}

class CApiBatchGetAutoDedupStatus : public ::testing::Test {
 public:
  ~CApiBatchGetAutoDedupStatus() override { ::shm_unlink(segment_.c_str()); }

 protected:
  CApiBatchGetAutoDedupStatus() {
    static std::atomic<uint64_t> sequence{0};
    namespace_ = "c-api-cpu-dedup-" + std::to_string(::getpid()) + "-" +
                 std::to_string(sequence.fetch_add(1));
    segment_ = NodeDedup::EnvSegmentName(ToBlockKey(namespace_, "").digest_hi);
    ::shm_unlink(segment_.c_str());
  }
  const ScopedEnv enabled_{"DFKV_CLIENT_NODE_DEDUP", "1"};
  const ScopedEnv arena_{"DFKV_NODE_DEDUP_ARENA_MB", "1"};
  const ScopedEnv slots_{"DFKV_NODE_DEDUP_SLOTS", "1024"};
  const ScopedEnv wait_{"DFKV_NODE_DEDUP_WAIT_MS", "20"};
  const ScopedEnv takeover_{"DFKV_NODE_DEDUP_TAKEOVER_MS", "2000"};
  const ScopedEnv ttl_{"DFKV_NODE_DEDUP_TTL_MS", "5000"};
  const ScopedEnv retries_{"DFKV_GET_MISS_RETRIES", "0"};
  std::string namespace_;
  std::string segment_;
};

TEST_F(CApiBatchGetAutoDedupStatus, NegativeCountDoesNotTouchOutputs) {
  int status = 99;
  uint64_t length = 99;
  EXPECT_EQ(
      dfkv_batch_get_auto_deduplicated_status(
          nullptr, nullptr, nullptr, nullptr, nullptr, -1, &status, &length),
      -1);
  EXPECT_EQ(status, 99);
  EXPECT_EQ(length, 99);
}

TEST_F(CApiBatchGetAutoDedupStatus, RejectsGlobalArgumentsAndAllowsEmptyBatch) {
  CApiDedupTransport transport(namespace_);
  KVClient client({{"n", "unused:1"}}, namespace_, &transport, 1);
  const void* key = "key";
  const uint64_t key_length = 3;
  char buffer = 0;
  void* destination = &buffer;
  const uint64_t cap = 1;
  for (int missing = 0; missing < 6; ++missing) {
    SCOPED_TRACE(missing);
    int status = 99;
    uint64_t length = 99;
    EXPECT_EQ(
        dfkv_batch_get_auto_deduplicated_status(
            missing == 0 ? nullptr : &client, missing == 1 ? nullptr : &key,
            missing == 2 ? nullptr : &key_length,
            missing == 3 ? nullptr : &destination,
            missing == 4 ? nullptr : &cap, 1, missing == 5 ? nullptr : &status,
            &length),
        -1);
    EXPECT_EQ(status, missing == 5 ? 99 : DFKV_GET_INVALID);
    EXPECT_EQ(length, 0);
  }
  EXPECT_EQ(
      dfkv_batch_get_auto_deduplicated_status(
          &client, nullptr, nullptr, nullptr, nullptr, 0, nullptr, nullptr),
      0);
  EXPECT_EQ(
      dfkv_batch_get_auto_deduplicated_status(
          nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr, nullptr),
      -1);
  EXPECT_EQ(transport.submitted(), 0);
}

TEST_F(CApiBatchGetAutoDedupStatus,
       ReusesSuccessfulBytesAndKeepsDirectApiDirect) {
  CApiDedupTransport transport(namespace_);
  transport.Set("key", {});
  KVClient client({{"n", "unused:1"}}, namespace_, &transport, 1);
  const std::array<const void*, 2> keys{"key", "key"};
  const std::array<uint64_t, 2> key_lengths{3, 3};
  std::array<char, 4> first{};
  std::array<char, 4> second{};
  std::array<void*, 2> destinations{first.data(), second.data()};
  const std::array<uint64_t, 2> caps{4, 4};
  std::array<int, 2> statuses{};
  std::array<uint64_t, 2> lengths{99, 99};
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 2, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, (std::array<int, 2>{DFKV_GET_OK, DFKV_GET_OK}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 2>{3, 3}));
  EXPECT_EQ(std::string(first.data(), 3), std::string("a\0c", 3));
  EXPECT_EQ(first, second);
  EXPECT_EQ(transport.submitted(), 1);
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 1);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 6);

  transport.Set("key", {Status::kOk, "new"});
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lengths.data(),
                                       destinations.data(), caps.data(), 1,
                                       statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(std::string(first.data(), 3), "new");
  EXPECT_EQ(transport.submitted(), 2);
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 1, statuses.data(), nullptr),
            0);
  EXPECT_EQ(std::string(first.data(), 3), std::string("a\0c", 3));
  EXPECT_EQ(transport.submitted(), 2);
  int hit = 0;
  EXPECT_EQ(dfkv_batch_get_auto(&client, keys.data(), key_lengths.data(),
                                destinations.data(), caps.data(), 1, &hit,
                                lengths.data()),
            0);
  EXPECT_EQ(hit, 1);
  EXPECT_EQ(std::string(first.data(), 3), std::string("a\0c", 3));
  EXPECT_EQ(transport.submitted(), 2);
}

TEST_F(CApiBatchGetAutoDedupStatus, DisabledDoesNotMergeDuplicateReads) {
  const ScopedEnv disabled("DFKV_CLIENT_NODE_DEDUP", "0");
  CApiDedupTransport transport(namespace_);
  transport.Set("key", {});
  KVClient client({{"n", "unused:1"}}, namespace_, &transport, 1);
  const std::array<const void*, 2> keys{"key", "key"};
  const std::array<uint64_t, 2> key_lengths{3, 3};
  std::array<char, 4> first{};
  std::array<char, 4> second{};
  std::array<void*, 2> destinations{first.data(), second.data()};
  const std::array<uint64_t, 2> caps{4, 4};
  std::array<int, 2> statuses{};
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 2, statuses.data(), nullptr),
            0);
  EXPECT_EQ(statuses, (std::array<int, 2>{DFKV_GET_OK, DFKV_GET_OK}));
  EXPECT_EQ(transport.submitted(), 2);
}

TEST_F(CApiBatchGetAutoDedupStatus,
       PreservesAllNativeStatusesWithoutNegativeCache) {
  const std::array<Status, 9> native{Status::kOk,
                                     Status::kNotFound,
                                     Status::kCacheFull,
                                     Status::kQuotaExceeded,
                                     Status::kIOError,
                                     Status::kInvalid,
                                     Status::kResourceExhausted,
                                     Status::kNoCompatibleRail,
                                     static_cast<Status>(255)};
  const std::array<int, 9> expected{DFKV_GET_OK,
                                    DFKV_GET_NOT_FOUND,
                                    DFKV_GET_CACHE_FULL,
                                    DFKV_GET_QUOTA_EXCEEDED,
                                    DFKV_GET_IO_ERROR,
                                    DFKV_GET_INVALID,
                                    DFKV_GET_RESOURCE_EXHAUSTED,
                                    DFKV_GET_NO_COMPATIBLE_RAIL,
                                    DFKV_GET_INTERNAL_ERROR};
  CApiDedupTransport transport(namespace_);
  KVClient client({{"n", "unused:1"}}, namespace_, &transport, 1);
  std::array<std::string, 9> names{};
  std::array<const void*, 9> keys{};
  std::array<uint64_t, 9> key_lengths{};
  std::array<std::array<char, 4>, 9> buffers{};
  std::array<void*, 9> destinations{};
  std::array<uint64_t, 9> caps{};
  std::array<int, 9> statuses{};
  std::array<uint64_t, 9> lengths{};
  lengths.fill(99);
  caps.fill(4);
  for (size_t i = 0; i < names.size(); ++i) {
    names[i] = std::to_string(i);
    keys[i] = names[i].data();
    key_lengths[i] = names[i].size();
    destinations[i] = buffers[i].data();
    transport.Set(names[i], {native[i], std::string("a\0c", 3)});
  }
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 9, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, expected);
  EXPECT_EQ(lengths, (std::array<uint64_t, 9>{3, 0, 0, 0, 0, 0, 0, 0, 0}));
  transport.Set("1", {Status::kOk, "now"});
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data() + 1, key_lengths.data() + 1,
                destinations.data() + 1, caps.data() + 1, 1,
                statuses.data() + 1, lengths.data() + 1),
            0);
  EXPECT_EQ(statuses[1], DFKV_GET_OK);
  EXPECT_EQ(lengths[1], 3);
  EXPECT_EQ(std::string(buffers[1].data(), 3), "now");
  EXPECT_EQ(transport.submitted(), 10);
}

TEST_F(CApiBatchGetAutoDedupStatus,
       InvalidSlotsKeepValidBinaryAndEmptySiblings) {
  CApiDedupTransport transport(namespace_);
  const std::string binary_key("k\0x", 3);
  transport.Set(binary_key, {});
  transport.Set("empty", {Status::kOk, ""});
  KVClient client({{"n", "unused:1"}}, namespace_, &transport, 1);
  const std::array<const void*, 5> keys{nullptr, "zero", "bad",
                                        binary_key.data(), "empty"};
  const std::array<uint64_t, 5> key_lengths{1, 0, 3, 3, 5};
  std::array<char, 4> buffer{};
  std::array<void*, 5> destinations{nullptr, nullptr, nullptr, buffer.data(),
                                    nullptr};
  const std::array<uint64_t, 5> caps{4, 4, 1, 4, 0};
  std::array<int, 5> statuses{};
  std::array<uint64_t, 5> lengths{99, 99, 99, 99, 99};
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 5, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses,
            (std::array<int, 5>{DFKV_GET_INVALID, DFKV_GET_INVALID,
                                DFKV_GET_INVALID, DFKV_GET_OK, DFKV_GET_OK}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 5>{0, 0, 0, 3, 0}));
  EXPECT_EQ(std::string(buffer.data(), 3), std::string("a\0c", 3));
  EXPECT_EQ(transport.submitted(), 2);
}

TEST_F(CApiBatchGetAutoDedupStatus,
       WholeCallExceptionDiscardsAllAndAbortsClaims) {
  CApiDedupTransport transport(namespace_);
  transport.set_throw_on_pipeline(true);
  KVClient client({{"n", "unused:1"}}, namespace_, &transport, 1);
  const std::array<const void*, 2> keys{"one", "two"};
  const std::array<uint64_t, 2> key_lengths{3, 3};
  std::array<char, 4> first{};
  std::array<char, 4> second{};
  std::array<void*, 2> destinations{first.data(), second.data()};
  const std::array<uint64_t, 2> caps{4, 4};
  std::array<int, 2> statuses{};
  std::array<uint64_t, 2> lengths{99, 99};
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 2, statuses.data(), lengths.data()),
            -1);
  EXPECT_EQ(statuses, (std::array<int, 2>{DFKV_GET_INTERNAL_ERROR,
                                          DFKV_GET_INTERNAL_ERROR}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 2>{0, 0}));
  EXPECT_EQ(transport.submitted(), 0);
  transport.set_throw_on_pipeline(false);
  std::unique_ptr<NodeDedup> peer =
      NodeDedup::FromEnv(ToBlockKey(namespace_, "").digest_hi);
  ASSERT_NE(peer, nullptr);
  for (const std::string& name : {std::string("one"), std::string("two")}) {
    size_t got = 0;
    uint64_t token = 0;
    const BlockKey key = ToBlockKey(namespace_, name);
    EXPECT_EQ(peer->ClaimAuto(key, first.size(), first.data(), &got, &token),
              NodeDedup::Role::kFetch);
    EXPECT_NE(token, 0);
    peer->Abort(key, NodeDedup::Kind::kData, token);
  }
}

class CApiBatchGetAutoDedupStatusTcp : public CApiBatchGetAutoDedupStatus {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-c-api-dedup-XXXXXX")
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

TEST_F(CApiBatchGetAutoDedupStatusTcp, RealBinaryAbsenceAndRemoveInvalidation) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  namespace_, &transport, 1);
  const std::string key("k\0x", 3);
  const std::string value("a\0c", 3);
  ASSERT_TRUE(client.Put(key, value.data(), value.size()));
  const std::array<const void*, 3> keys{key.data(), key.data(), "absent"};
  const std::array<uint64_t, 3> key_lengths{3, 3, 6};
  std::array<char, 4> first{};
  std::array<char, 4> second{};
  std::array<char, 4> absent{};
  std::array<void*, 3> destinations{first.data(), second.data(), absent.data()};
  const std::array<uint64_t, 3> caps{4, 4, 4};
  std::array<int, 3> statuses{};
  std::array<uint64_t, 3> lengths{};
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 3, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses,
            (std::array<int, 3>{DFKV_GET_OK, DFKV_GET_OK, DFKV_GET_NOT_FOUND}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 3>{3, 3, 0}));
  EXPECT_EQ(std::string(first.data(), 3), value);
  EXPECT_EQ(first, second);
  ASSERT_EQ(dfkv_remove(&client, key.data(), key.size()), 1);
  EXPECT_EQ(dfkv_batch_get_auto_deduplicated_status(
                &client, keys.data(), key_lengths.data(), destinations.data(),
                caps.data(), 1, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses[0], DFKV_GET_NOT_FOUND);
  EXPECT_EQ(lengths[0], 0);
}

}  // namespace
}  // namespace dfkv
