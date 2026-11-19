#include <gtest/gtest.h>
// POSIX mkdtemp has no equivalent C++ declaration.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "cache/disk_cache_group.h"
#include "cache/kv_node_server.h"
#include "client/dfkv_c_api.h"
#include "client/key_map.h"
#include "client/kv_client.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/tcp_transport.h"
#include "transport/transport.h"

namespace dfkv {
namespace {

constexpr char kNamespace[] = "c-batch-status";

struct CBatchReply {
  Status status = Status::kOk;
  uint64_t length = 3;
};

struct CBatchStatusTransport final : Transport {
  bool pipeline = true;
  bool throw_on_read = false;
  std::string throw_key;
  std::atomic<int> calls{0};
  std::map<std::string, CBatchReply> replies;

  void Set(const std::string& key, CBatchReply reply) {
    replies[ToBlockKey(kNamespace, key).Filename()] = reply;
  }
  bool pipelined() const override { return pipeline; }
  Status Cache(const std::string&, const BlockKey&, const void*,
               size_t) override {
    return Status::kInvalid;
  }
  Status Range(const std::string&, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    ADD_FAILURE() << "Batch GET must retain RangeInto";
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
    ++calls;
    if (throw_on_read)
      throw std::runtime_error("C batch status test exception");
    EXPECT_EQ(keys.size(), destinations.size());
    std::vector<Status> statuses;
    lengths->clear();
    for (size_t i = 0; i < keys.size(); ++i) {
      if (!throw_key.empty() && keys[i].Filename() == throw_key) {
        throw std::runtime_error("C batch status partial completion exception");
      }
      const CBatchReply& reply = replies.at(keys[i].Filename());
      statuses.push_back(reply.status);
      lengths->push_back(reply.length);
      if (reply.status == Status::kOk && destinations[i].n != 0) {
        std::memcpy(destinations[i].payload, "abc",
                    std::min<size_t>(3, destinations[i].n));
      }
    }
    return statuses;
  }
};

uint64_t GetCounter(const std::string& metrics, const std::string& name) {
  const std::string prefix = "\n" + name + "{op=\"get\"} ";
  const size_t position = metrics.find(prefix);
  EXPECT_NE(position, std::string::npos);
  return position == std::string::npos
             ? 0
             : std::stoull(metrics.substr(position + prefix.size()));
}

TEST(CApiBatchGetStatus, MapsMixedStatusesInInputOrderAndPreservesLegacyBool) {
  const std::array<Status, 8> native{Status::kOk,
                                     Status::kNotFound,
                                     Status::kCacheFull,
                                     Status::kQuotaExceeded,
                                     Status::kIOError,
                                     Status::kInvalid,
                                     Status::kResourceExhausted,
                                     Status::kNoCompatibleRail};
  const std::array<int, 8> expected{DFKV_GET_OK,
                                    DFKV_GET_NOT_FOUND,
                                    DFKV_GET_CACHE_FULL,
                                    DFKV_GET_QUOTA_EXCEEDED,
                                    DFKV_GET_IO_ERROR,
                                    DFKV_GET_INVALID,
                                    DFKV_GET_RESOURCE_EXHAUSTED,
                                    DFKV_GET_NO_COMPATIBLE_RAIL};
  CBatchStatusTransport transport;
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const std::array<std::string, 8> names{"0", "1", "2", "3",
                                         "4", "5", "6", "7"};
  std::array<const void*, 8> keys{};
  const std::array<uint64_t, 8> key_lens{1, 1, 1, 1, 1, 1, 1, 1};
  std::array<std::array<char, 4>, 8> buffers{};
  std::array<void*, 8> destinations{};
  const std::array<uint64_t, 8> caps{4, 4, 4, 4, 4, 4, 4, 4};
  std::array<int, 8> statuses{};
  std::array<uint64_t, 8> lengths{};
  lengths.fill(999);
  for (size_t i = 0; i < names.size(); ++i) {
    keys[i] = names[i].data();
    destinations[i] = buffers[i].data();
    transport.Set(names[i], {native[i], 3});
  }
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lens.data(),
                                       destinations.data(), caps.data(), 8,
                                       statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, expected);
  EXPECT_EQ(lengths, (std::array<uint64_t, 8>{3, 0, 0, 0, 0, 0, 0, 0}));
  EXPECT_EQ(std::string(buffers[0].data(), 3), "abc");
  EXPECT_EQ(transport.calls.load(), 1);
  std::array<int, 8> hits{};
  EXPECT_EQ(dfkv_batch_get_auto(&client, keys.data(), key_lens.data(),
                                destinations.data(), caps.data(), 8,
                                hits.data(), lengths.data()),
            0);
  EXPECT_EQ(hits, (std::array<int, 8>{1, 0, 0, 0, 0, 0, 0, 0}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 8>{3, 0, 0, 0, 0, 0, 0, 0}));
}

TEST(CApiBatchGetStatus, InvalidItemsKeepValidBinaryAndZeroLengthSiblings) {
  for (const bool pipeline : {false, true}) {
    SCOPED_TRACE(pipeline);
    CBatchStatusTransport transport;
    transport.pipeline = pipeline;
    const std::string binary_key("k\0x", 3);
    transport.Set(binary_key, {});
    transport.Set("empty", {Status::kOk, 0});
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
    std::array<char, 4> output{};
    const std::array<const void*, 5> keys{nullptr, "zero", "bad",
                                          binary_key.data(), "empty"};
    const std::array<uint64_t, 5> key_lens{1, 0, 3, 3, 5};
    std::array<void*, 5> destinations{nullptr, nullptr, nullptr, output.data(),
                                      nullptr};
    const std::array<uint64_t, 5> caps{4, 4, 1, 4, 0};
    std::array<int, 5> statuses{};
    std::array<uint64_t, 5> lengths{999, 999, 999, 999, 999};
    EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lens.data(),
                                         destinations.data(), caps.data(), 5,
                                         statuses.data(), lengths.data()),
              0);
    EXPECT_EQ(statuses,
              (std::array<int, 5>{DFKV_GET_INVALID, DFKV_GET_INVALID,
                                  DFKV_GET_INVALID, DFKV_GET_OK, DFKV_GET_OK}));
    EXPECT_EQ(lengths, (std::array<uint64_t, 5>{0, 0, 0, 3, 0}));
    EXPECT_EQ(std::string(output.data(), 3), "abc");
    EXPECT_EQ(transport.calls.load(), 2);
    const std::string metrics = client.MetricsSnapshot();
    EXPECT_EQ(GetCounter(metrics, "dfkv_client_op_requests_total"), 1);
    EXPECT_EQ(GetCounter(metrics, "dfkv_client_op_keys_total"), 5);
    EXPECT_EQ(GetCounter(metrics, "dfkv_client_op_hits_total"), 2);
    EXPECT_EQ(GetCounter(metrics, "dfkv_client_op_bytes_total"), 3);
    EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lens.data(),
                                         destinations.data(), caps.data(), 5,
                                         statuses.data(), nullptr),
              0);
  }
}

TEST(CApiBatchGetStatus, RejectsWholeCallArgumentsAndClearsAvailableOutputs) {
  CBatchStatusTransport transport;
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const void* key = "key";
  const uint64_t key_len = 3;
  char output = 0;
  void* destination = &output;
  const uint64_t cap = 1;
  for (int missing = 0; missing < 6; ++missing) {
    SCOPED_TRACE(missing);
    int status = 999;
    uint64_t length = 999;
    EXPECT_EQ(
        dfkv_batch_get_auto_status(missing == 0 ? nullptr : &client,
                                   missing == 1 ? nullptr : &key,
                                   missing == 2 ? nullptr : &key_len,
                                   missing == 3 ? nullptr : &destination,
                                   missing == 4 ? nullptr : &cap, 1,
                                   missing == 5 ? nullptr : &status, &length),
        -1);
    EXPECT_EQ(status, missing == 5 ? 999 : DFKV_GET_INVALID);
    EXPECT_EQ(length, 0);
  }
  int status = 999;
  uint64_t length = 999;
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, nullptr, nullptr, nullptr,
                                       nullptr, -1, &status, &length),
            -1);
  EXPECT_EQ(status, 999);
  EXPECT_EQ(length, 999);
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, nullptr, nullptr, nullptr,
                                       nullptr, 0, nullptr, nullptr),
            0);
  EXPECT_EQ(dfkv_batch_get_auto_status(nullptr, nullptr, nullptr, nullptr,
                                       nullptr, 0, nullptr, nullptr),
            -1);
  EXPECT_EQ(transport.calls.load(), 0);
}

TEST(CApiBatchGetStatus, SeparatesPropagatedAndWorkerContainedExceptions) {
  for (const bool pipeline : {false, true}) {
    SCOPED_TRACE(pipeline);
    CBatchStatusTransport transport;
    transport.pipeline = pipeline;
    transport.throw_on_read = true;
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
    const std::array<const void*, 2> keys{"key", nullptr};
    const std::array<uint64_t, 2> key_lens{3, 1};
    std::array<char, 4> output{};
    std::array<void*, 2> destinations{output.data(), nullptr};
    const std::array<uint64_t, 2> caps{4, 4};
    std::array<int, 2> statuses{999, 999};
    std::array<uint64_t, 2> lengths{999, 999};
    EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lens.data(),
                                         destinations.data(), caps.data(), 2,
                                         statuses.data(), lengths.data()),
              pipeline ? 0 : -1);
    EXPECT_EQ(statuses,
              pipeline
                  ? (std::array<int, 2>{DFKV_GET_IO_ERROR, DFKV_GET_INVALID})
                  : (std::array<int, 2>{DFKV_GET_INTERNAL_ERROR,
                                        DFKV_GET_INTERNAL_ERROR}));
    EXPECT_EQ(lengths, (std::array<uint64_t, 2>{0, 0}));
    EXPECT_EQ(transport.calls.load(), 1);
  }
}

TEST(CApiBatchGetStatus, PropagatedExceptionDiscardsEarlierCompletedResults) {
  CBatchStatusTransport transport;
  transport.pipeline = false;
  transport.Set("ok", {});
  transport.throw_key = ToBlockKey(kNamespace, "bad").Filename();
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const std::array<const void*, 2> keys{"ok", "bad"};
  const std::array<uint64_t, 2> key_lens{2, 3};
  std::array<std::array<char, 4>, 2> buffers{};
  std::array<void*, 2> destinations{buffers[0].data(), buffers[1].data()};
  const std::array<uint64_t, 2> caps{4, 4};
  std::array<int, 2> statuses{999, 999};
  std::array<uint64_t, 2> lengths{999, 999};
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lens.data(),
                                       destinations.data(), caps.data(), 2,
                                       statuses.data(), lengths.data()),
            -1);
  EXPECT_EQ(statuses, (std::array<int, 2>{DFKV_GET_INTERNAL_ERROR,
                                          DFKV_GET_INTERNAL_ERROR}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 2>{0, 0}));
  EXPECT_EQ(transport.calls.load(), 2);
  // Bytes may have changed before an exception: discard even this former hit.
  EXPECT_EQ(std::string(buffers[0].data(), 3), "abc");
}

TEST(CApiBatchGetStatus, NoRouteIsNotAnAuthoritativeMiss) {
  CBatchStatusTransport transport;
  KVClient client({}, kNamespace, &transport, 1);
  const void* key = "key";
  const uint64_t key_len = 3;
  char output = 0;
  void* destination = &output;
  const uint64_t cap = 1;
  int status = 999;
  uint64_t length = 999;
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, &key, &key_len, &destination,
                                       &cap, 1, &status, &length),
            0);
  EXPECT_EQ(status, DFKV_GET_IO_ERROR);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls.load(), 0);
}

TEST(CApiBatchGetStatus, CooldownRefusalDoesNotPerformAnotherRead) {
  CBatchStatusTransport transport;
  transport.Set("key", {Status::kIOError, 999});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const void* key = "key";
  const uint64_t key_len = 3;
  char output = 0;
  void* destination = &output;
  const uint64_t cap = 1;
  int status = 999;
  uint64_t length = 999;
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, &key, &key_len, &destination,
                                       &cap, 1, &status, &length),
            0);
  EXPECT_EQ(status, DFKV_GET_IO_ERROR);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls.load(), 1);
  // An unconfigured key would throw if routing bypassed cooldown.
  key = "new";
  status = 999;
  length = 999;
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, &key, &key_len, &destination,
                                       &cap, 1, &status, &length),
            0);
  EXPECT_EQ(status, DFKV_GET_IO_ERROR);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls.load(), 1);
}

class CApiBatchGetStatusTcpTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-c-batch-status-XXXXXX")
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

TEST_F(CApiBatchGetStatusTcpTest, RealHitMissCapacityAndStoppedService) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  kNamespace, &transport, 1);
  const std::string key("k\0x", 3);
  ASSERT_EQ(dfkv_put(&client, key.data(), key.size(), "abc", 3), 0);
  const std::array<const void*, 3> keys{key.data(), "absent", key.data()};
  const std::array<uint64_t, 3> key_lens{3, 6, 3};
  std::array<std::array<char, 4>, 3> buffers{};
  std::array<void*, 3> destinations{buffers[0].data(), buffers[1].data(),
                                    buffers[2].data()};
  const std::array<uint64_t, 3> caps{4, 4, 1};
  std::array<int, 3> statuses{};
  std::array<uint64_t, 3> lengths{999, 999, 999};
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lens.data(),
                                       destinations.data(), caps.data(), 3,
                                       statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, (std::array<int, 3>{DFKV_GET_OK, DFKV_GET_NOT_FOUND,
                                          DFKV_GET_INVALID}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 3>{3, 0, 0}));
  EXPECT_EQ(std::string(buffers[0].data(), 3), "abc");
  server_->Stop();
  EXPECT_EQ(dfkv_batch_get_auto_status(&client, keys.data(), key_lens.data(),
                                       destinations.data(), caps.data(), 3,
                                       statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, (std::array<int, 3>{DFKV_GET_IO_ERROR, DFKV_GET_IO_ERROR,
                                          DFKV_GET_IO_ERROR}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 3>{0, 0, 0}));
}

}  // namespace
}  // namespace dfkv
