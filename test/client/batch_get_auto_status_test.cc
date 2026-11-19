#include <gtest/gtest.h>
// POSIX mkdtemp has no equivalent C++ declaration.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
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
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/tcp_transport.h"
#include "transport/transport.h"

namespace dfkv {
namespace {

constexpr char kNamespace[] = "batch-status-test";

struct Reply {
  Status status = Status::kOk;
  uint64_t length = 3;
  std::string bytes = "abc";
};

struct BatchCall {
  std::string node;
  size_t count;
  size_t capacity;
};

struct BatchStatusTransport final : Transport {
  bool use_pipeline = true;
  bool throw_on_read = false;
  int status_delta = 0;
  int length_delta = 0;
  bool overlap_required = false;
  bool overlap_observed = false;
  size_t active = 0;
  std::map<std::string, Reply> replies;
  std::mutex mutex;
  std::condition_variable entered;
  std::vector<BatchCall> calls;

  void Set(const std::string& key, Reply reply) {
    replies[ToBlockKey(kNamespace, key).Filename()] = std::move(reply);
  }
  bool pipelined() const override { return use_pipeline; }
  Status Cache(const std::string&, const BlockKey&, const void*,
               size_t) override {
    return Status::kInvalid;
  }
  Status Range(const std::string&, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    ADD_FAILURE() << "Batch auto GET must keep RangeInto";
    return Status::kIOError;
  }
  Status Lookup(const std::string&, const BlockKey&, uint64_t*) override {
    return Status::kInvalid;
  }
  Status Exist(const std::string&, const BlockKey&, bool*) override {
    return Status::kInvalid;
  }
  std::vector<Status> RangeInto(const std::string& node,
                                const std::vector<BlockKey>& keys,
                                const std::vector<RangeDst>& destinations,
                                std::vector<uint64_t>* lengths) override {
    {
      std::unique_lock<std::mutex> lock(mutex);
      calls.push_back({node, keys.size(),
                       destinations.empty() ? 0 : destinations.front().n});
      if (overlap_required) {
        ++active;
        if (active >= 2) overlap_observed = true;
        entered.notify_all();
        EXPECT_TRUE(entered.wait_for(lock, std::chrono::seconds(2),
                                     [this] { return overlap_observed; }));
        --active;
      }
    }
    if (throw_on_read) throw std::runtime_error("batch transport test failure");
    EXPECT_EQ(keys.size(), destinations.size());
    std::vector<Status> statuses;
    statuses.reserve(keys.size());
    lengths->clear();
    lengths->reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      const Reply& reply = replies.at(keys[i].Filename());
      statuses.push_back(reply.status);
      lengths->push_back(reply.length);
      if (reply.status == Status::kOk && destinations[i].n != 0) {
        std::memcpy(destinations[i].payload, reply.bytes.data(),
                    std::min(reply.bytes.size(), destinations[i].n));
      }
    }
    if (status_delta < 0) statuses.pop_back();
    if (status_delta > 0) statuses.push_back(Status::kNotFound);
    if (length_delta < 0) lengths->clear();
    if (length_delta > 0) lengths->push_back(0);
    return statuses;
  }
};

uint64_t Counter(const std::string& metrics, const std::string& name) {
  const std::string prefix = "\n" + name + "{op=\"get\"} ";
  const size_t position = metrics.find(prefix);
  EXPECT_NE(position, std::string::npos);
  return position == std::string::npos
             ? 0
             : std::stoull(metrics.substr(position + prefix.size()));
}

TEST(BatchGetAutoStatus, PreservesMixedStatusesAndInputOrderInBothModes) {
  const std::vector<Status> expected{Status::kOk,
                                     Status::kNotFound,
                                     Status::kCacheFull,
                                     Status::kQuotaExceeded,
                                     Status::kIOError,
                                     Status::kInvalid,
                                     Status::kResourceExhausted,
                                     Status::kNoCompatibleRail};
  for (const bool pipeline : {false, true}) {
    SCOPED_TRACE(pipeline);
    BatchStatusTransport transport;
    transport.use_pipeline = pipeline;
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
    std::array<std::array<char, 4>, 8> output{};
    std::vector<KvGetItem> items;
    items.reserve(expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      const std::string key = std::to_string(i);
      transport.Set(key, {expected[i], 3, "abc"});
      items.push_back({key, output[i].data(), output[i].size()});
    }
    std::vector<size_t> lengths{999};
    EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths), expected);
    EXPECT_EQ(lengths, (std::vector<size_t>{3, 0, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(std::string(output[0].data(), 3), "abc");
    const std::string metrics = client.MetricsSnapshot();
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 1);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 8);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 1);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 3);
    ASSERT_FALSE(transport.calls.empty());
    EXPECT_EQ(transport.calls.size(), pipeline ? 1 : 8);
  }
}

TEST(BatchGetAutoStatus, InvalidItemsDoNotAffectValidSiblingsOrZeroLengthHit) {
  for (const bool pipeline : {false, true}) {
    BatchStatusTransport transport;
    transport.use_pipeline = pipeline;
    transport.Set("valid", {});
    transport.Set("empty", {Status::kOk, 0, ""});
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
    std::array<char, 4> output{};
    const std::vector<KvGetItem> items{{"", output.data(), 4},
                                       {"invalid", nullptr, 4},
                                       {"valid", output.data(), 4},
                                       {"empty", nullptr, 0}};
    std::vector<size_t> lengths{999};
    EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths),
              (std::vector<Status>{Status::kInvalid, Status::kInvalid,
                                   Status::kOk, Status::kOk}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0, 0, 3, 0}));
    size_t submitted = 0;
    for (const BatchCall& call : transport.calls) submitted += call.count;
    EXPECT_EQ(submitted, 2);
  }
}

TEST(BatchGetAutoStatus, NoRouteAndCooldownAreErrorsWithoutNewIo) {
  BatchStatusTransport transport;
  transport.Set("key", {Status::kIOError, 0, ""});
  std::array<char, 4> output{};
  const std::vector<KvGetItem> items{{"key", output.data(), output.size()}};
  KVClient no_route({}, kNamespace, &transport);
  std::vector<size_t> lengths{999};
  EXPECT_EQ(no_route.BatchGetAutoWithStatus(items, &lengths),
            (std::vector<Status>{Status::kIOError}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  EXPECT_TRUE(transport.calls.empty());
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  for (int call = 0; call < 2; ++call) {
    EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths),
              (std::vector<Status>{Status::kIOError}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  }
  EXPECT_EQ(transport.calls.size(), 1);
}

TEST(BatchGetAutoStatus, LocalResourceAndGuardErrorsDoNotCoolHealthyRoute) {
  for (const Status status :
       {Status::kInvalid, Status::kCacheFull, Status::kQuotaExceeded,
        Status::kResourceExhausted, Status::kNoCompatibleRail}) {
    SCOPED_TRACE(StatusName(status));
    BatchStatusTransport transport;
    transport.Set("key", {status, 0, ""});
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<char, 4> output{};
    const std::vector<KvGetItem> items{{"key", output.data(), output.size()}};
    EXPECT_EQ(client.BatchGetAutoWithStatus(items, nullptr),
              (std::vector<Status>{status}));
    transport.Set("key", {});
    EXPECT_EQ(client.BatchGetAutoWithStatus(items, nullptr),
              (std::vector<Status>{Status::kOk}));
    EXPECT_EQ(transport.calls.size(), 2);
  }
}

TEST(BatchGetAutoStatus, RejectsMalformedStatusCardinalityWithoutFalseMiss) {
  for (const int delta : {-1, 1}) {
    BatchStatusTransport transport;
    transport.status_delta = delta;
    transport.Set("hit", {});
    transport.Set("absent", {Status::kNotFound, 0, ""});
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<std::array<char, 4>, 2> output{};
    const std::vector<KvGetItem> items{{"hit", output[0].data(), 4},
                                       {"absent", output[1].data(), 4}};
    std::vector<size_t> lengths;
    EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths),
              (std::vector<Status>{Status::kIOError, Status::kIOError}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0, 0}));
  }
}

TEST(BatchGetAutoStatus, BadLengthShapeFailsHitsButKeepsAuthoritativeAbsence) {
  for (const int delta : {-1, 1}) {
    BatchStatusTransport transport;
    transport.length_delta = delta;
    transport.Set("hit", {});
    transport.Set("absent", {Status::kNotFound, 0, ""});
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<std::array<char, 4>, 2> output{};
    const std::vector<KvGetItem> items{{"hit", output[0].data(), 4},
                                       {"absent", output[1].data(), 4}};
    std::vector<size_t> lengths;
    EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths),
              (std::vector<Status>{Status::kIOError, Status::kNotFound}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0, 0}));
  }
}

TEST(BatchGetAutoStatus, MalformedSuccessDoesNotRestoreOrPoisonPeerHealth) {
  for (const int delta : {-1, 1}) {
    BatchStatusTransport transport;
    transport.length_delta = delta;
    transport.Set("key", {});
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<char, 4> output{};
    EXPECT_EQ(
        client.BatchGetAutoWithStatus({{"key", output.data(), 4}}, nullptr),
        (std::vector<Status>{Status::kIOError}));
    const std::string metrics = client.MetricsSnapshot();
    EXPECT_NE(metrics.find("\ndfkv_client_ops_served_total 0\n"),
              std::string::npos);
    EXPECT_NE(metrics.find("\ndfkv_client_io_errors_total 0\n"),
              std::string::npos);
    transport.length_delta = 0;
    EXPECT_EQ(
        client.BatchGetAutoWithStatus({{"key", output.data(), 4}}, nullptr),
        (std::vector<Status>{Status::kOk}));
    EXPECT_EQ(transport.calls.size(), 2);
  }
}

TEST(BatchGetAutoStatus, OversizeIsInvalidAndOldBoolWrapperStillCountsOnce) {
  BatchStatusTransport transport;
  transport.Set("big", {Status::kOk, 7, "abcdefg"});
  transport.Set("hit", {});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  std::array<std::array<char, 4>, 2> output{};
  const std::vector<KvGetItem> items{{"big", output[0].data(), 4},
                                     {"hit", output[1].data(), 4}};
  std::vector<size_t> lengths;
  EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths),
            (std::vector<Status>{Status::kInvalid, Status::kOk}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0, 3}));
  EXPECT_EQ(client.BatchGetAuto(items, &lengths),
            (std::vector<bool>{false, true}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0, 3}));
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 4);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 6);
}

TEST(BatchGetAutoStatus, GroupsByNodeCapacityAndShardsWithoutChangingOrder) {
  BatchStatusTransport transport;
  KVClient client({{"a", "unused:1"}, {"b", "unused:2"}}, kNamespace,
                  &transport);
  std::array<std::array<char, 8>, 128> output{};
  std::vector<KvGetItem> items;
  items.reserve(output.size());
  for (size_t i = 0; i < output.size(); ++i) {
    const std::string key = std::to_string(i);
    const std::string bytes(3, static_cast<char>('a' + i % 26));
    transport.Set(key, {Status::kOk, 3, bytes});
    items.push_back({key, output[i].data(), i % 2 == 0 ? 4u : 8u});
  }
  std::vector<size_t> lengths;
  EXPECT_EQ(client.BatchGetAutoWithStatus(items, &lengths),
            std::vector<Status>(items.size(), Status::kOk));
  EXPECT_EQ(lengths, std::vector<size_t>(items.size(), 3));
  size_t submitted = 0;
  for (const BatchCall& call : transport.calls) {
    EXPECT_FALSE(call.node.empty());
    EXPECT_TRUE(call.capacity == 4 || call.capacity == 8);
    submitted += call.count;
  }
  EXPECT_EQ(submitted, items.size());
  EXPECT_GE(transport.calls.size(), 4);
  EXPECT_LT(transport.calls.size(), items.size());
  for (size_t i = 0; i < output.size(); ++i) {
    EXPECT_EQ(std::string(output[i].data(), 3),
              std::string(3, static_cast<char>('a' + i % 26)));
  }
  EXPECT_EQ(client.BatchGetAutoWithStatus(items, nullptr),
            std::vector<Status>(items.size(), Status::kOk));
}

TEST(BatchGetAutoStatus, TcpRetainsParallelFanoutRatherThanSerializingItems) {
  BatchStatusTransport transport;
  transport.use_pipeline = false;
  transport.overlap_required = true;
  transport.Set("a", {});
  transport.Set("b", {});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 2);
  std::array<std::array<char, 4>, 2> output{};
  EXPECT_EQ(
      client.BatchGetAutoWithStatus(
          {{"a", output[0].data(), 4}, {"b", output[1].data(), 4}}, nullptr),
      (std::vector<Status>{Status::kOk, Status::kOk}));
  EXPECT_TRUE(transport.overlap_observed);
}

TEST(BatchGetAutoStatus,
     WorkerExceptionRemainsIoErrorAndEmptyBatchClearsLengths) {
  BatchStatusTransport transport;
  transport.throw_on_read = true;
  transport.Set("key", {});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  std::array<char, 4> output{};
  std::vector<size_t> lengths{999};
  EXPECT_EQ(
      client.BatchGetAutoWithStatus({{"key", output.data(), 4}}, &lengths),
      (std::vector<Status>{Status::kIOError}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  EXPECT_TRUE(client.BatchGetAutoWithStatus({}, &lengths).empty());
  EXPECT_TRUE(lengths.empty());
}

TEST(BatchGetAutoStatus,
     SingleWorkerExceptionClearsOutputAndRecordsFailureOnce) {
  BatchStatusTransport transport;
  transport.use_pipeline = false;
  transport.throw_on_read = true;
  transport.Set("key", {});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  std::array<char, 4> output{};
  std::vector<size_t> lengths{999, 999};
  EXPECT_THROW(
      client.BatchGetAutoWithStatus({{"key", output.data(), 4}}, &lengths),
      std::runtime_error);
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 1);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 1);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 0);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 0);
}

class BatchGetAutoStatusTcpTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-batch-status-XXXXXX")
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

TEST_F(BatchGetAutoStatusTcpTest,
       RealHitAbsenceAndCapacityAreDifferentResults) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  kNamespace, &transport);
  ASSERT_TRUE(client.Put("key", "abc", 3));
  std::array<std::array<char, 4>, 3> output{};
  const std::vector<KvGetItem> items{{"key", output[0].data(), 4},
                                     {"absent", output[1].data(), 4},
                                     {"key", output[2].data(), 1}};
  std::vector<size_t> lengths;
  EXPECT_EQ(
      client.BatchGetAutoWithStatus(items, &lengths),
      (std::vector<Status>{Status::kOk, Status::kNotFound, Status::kInvalid}));
  EXPECT_EQ(lengths, (std::vector<size_t>{3, 0, 0}));
  EXPECT_EQ(std::string(output[0].data(), lengths[0]), "abc");
}

}  // namespace
}  // namespace dfkv
