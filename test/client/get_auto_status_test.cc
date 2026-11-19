#include <gtest/gtest.h>
// POSIX mkdtemp requires its declaring C system header.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cache/disk_cache_group.h"
#include "cache/kv_node_server.h"
#include "client/kv_client.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/tcp_transport.h"
#include "transport/transport.h"

namespace dfkv {
namespace {

struct StatusTransport final : Transport {
  std::vector<Status> statuses{Status::kOk};
  std::vector<uint64_t> lengths{3};
  bool use_pipeline = false;
  bool throw_on_read = false;
  std::atomic<int> calls{0};

  bool pipelined() const override { return use_pipeline; }
  Status Cache(const std::string&, const BlockKey&, const void*,
               size_t) override {
    return Status::kInvalid;
  }
  Status Range(const std::string&, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    ADD_FAILURE() << "GET must retain the RangeInto path";
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
                                std::vector<uint64_t>* value_lens) override {
    ++calls;
    if (throw_on_read) throw std::runtime_error("injected transport exception");
    EXPECT_EQ(keys.size(), 1);
    EXPECT_EQ(destinations.size(), 1);
    *value_lens = lengths;
    if (statuses.size() == 1 && statuses[0] == Status::kOk &&
        !destinations.empty() && destinations[0].n != 0) {
      std::memcpy(destinations[0].payload, "abc",
                  std::min<size_t>(3, destinations[0].n));
    }
    return statuses;
  }
};

uint64_t MetricValue(const std::string& text, const std::string& name) {
  const std::string prefix = "\n" + name + " ";
  const size_t position = text.find(prefix);
  EXPECT_NE(position, std::string::npos) << text;
  if (position == std::string::npos) return 0;
  return std::stoull(text.substr(position + prefix.size()));
}

TEST(GetAutoStatus, PreservesEachTransportStatusForBothTransportModes) {
  const std::array<Status, 8> statuses{Status::kOk,
                                       Status::kNotFound,
                                       Status::kIOError,
                                       Status::kInvalid,
                                       Status::kCacheFull,
                                       Status::kQuotaExceeded,
                                       Status::kResourceExhausted,
                                       Status::kNoCompatibleRail};
  for (const bool pipeline : {false, true}) {
    for (const Status status : statuses) {
      SCOPED_TRACE(StatusName(status));
      StatusTransport transport;
      transport.use_pipeline = pipeline;
      transport.statuses = {status};
      KVClient client({{"n", "unused:1"}}, "status-test", &transport);
      std::array<char, 4> output{};
      size_t length = 999;
      EXPECT_EQ(client.GetAutoWithStatus("key", output.data(), output.size(),
                                         &length),
                status);
      EXPECT_EQ(length, status == Status::kOk ? 3 : 0);
      EXPECT_EQ(transport.calls.load(), 1);
      if (status == Status::kOk) {
        EXPECT_EQ(std::string(output.data(), length), "abc");
      }
    }
  }
}

TEST(GetAutoStatus, NoRouteIsNotNotFoundAndDoesNotCallTransport) {
  StatusTransport transport;
  KVClient client({}, "status-test", &transport);
  char output = 0;
  size_t length = 999;
  EXPECT_EQ(client.GetAutoWithStatus("key", &output, 1, &length),
            Status::kIOError);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls.load(), 0);
  EXPECT_EQ(
      MetricValue(client.MetricsSnapshot(), "dfkv_client_io_errors_total"), 0);
}

TEST(GetAutoStatus, TransportExceptionPropagatesWithOutputLengthCleared) {
  StatusTransport transport;
  transport.throw_on_read = true;
  KVClient client({{"n", "unused:1"}}, "status-test", &transport);
  std::array<char, 4> output{};
  size_t length = 999;
  EXPECT_THROW(
      client.GetAutoWithStatus("key", output.data(), output.size(), &length),
      std::runtime_error);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls.load(), 1);
  EXPECT_EQ(MetricValue(client.MetricsSnapshot(),
                        "dfkv_client_op_requests_total{op=\"get\"}"),
            1);
  EXPECT_EQ(MetricValue(client.MetricsSnapshot(),
                        "dfkv_client_op_hits_total{op=\"get\"}"),
            0);
}

TEST(GetAutoStatus, CooldownRefusalIsNotASecondIoOrAMiss) {
  StatusTransport transport;
  transport.statuses = {Status::kIOError};
  KVClient client({{"n", "unused:1"}}, "status-test", &transport);
  char output = 0;
  size_t length = 999;
  ASSERT_EQ(client.GetAutoWithStatus("key", &output, 1, &length),
            Status::kIOError);
  EXPECT_EQ(client.GetAutoWithStatus("another-key", &output, 1, &length),
            Status::kIOError);
  EXPECT_EQ(transport.calls.load(), 1);
  EXPECT_EQ(length, 0);
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_io_errors_total"), 1);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_unhealthy_skips_total"), 1);
}

TEST(GetAutoStatus, InvalidAndLocalResourceResultsDoNotCoolPeer) {
  for (const Status status : {Status::kInvalid, Status::kResourceExhausted,
                              Status::kNoCompatibleRail}) {
    StatusTransport transport;
    transport.statuses = {status};
    KVClient client({{"n", "unused:1"}}, "status-test", &transport);
    std::array<char, 4> output{};
    size_t length = 999;
    EXPECT_EQ(
        client.GetAutoWithStatus("key", output.data(), output.size(), &length),
        status);
    transport.statuses = {Status::kOk};
    EXPECT_EQ(
        client.GetAutoWithStatus("key", output.data(), output.size(), &length),
        Status::kOk);
    EXPECT_EQ(transport.calls.load(), 2);
    EXPECT_EQ(length, 3);
    EXPECT_EQ(
        MetricValue(client.MetricsSnapshot(), "dfkv_client_io_errors_total"),
        0);
  }
}

TEST(GetAutoStatus, BadArgumentsAndSmallCapacityAreErrorsNotMisses) {
  StatusTransport transport;
  KVClient client({{"n", "unused:1"}}, "status-test", &transport);
  std::array<char, 2> output{};
  size_t length = 999;
  EXPECT_EQ(client.GetAutoWithStatus("", output.data(), output.size(), &length),
            Status::kInvalid);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(client.GetAutoWithStatus("key", nullptr, 1, &length),
            Status::kInvalid);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls.load(), 0);
  EXPECT_EQ(
      client.GetAutoWithStatus("key", output.data(), output.size(), &length),
      Status::kInvalid);
  EXPECT_EQ(length, 0);
  EXPECT_FALSE(client.GetAuto("key", output.data(), output.size(), &length));
  EXPECT_EQ(length, 0);
}

TEST(GetAutoStatus, MalformedStatusAndSuccessLengthsCannotBecomeHitsOrMisses) {
  const std::vector<std::vector<Status>> statuses{
      {}, {Status::kNotFound, Status::kOk}, {Status::kOk}, {Status::kOk}};
  const std::vector<std::vector<uint64_t>> lengths{{3}, {0, 3}, {}, {3, 3}};
  for (size_t i = 0; i < statuses.size(); ++i) {
    StatusTransport transport;
    transport.statuses = statuses[i];
    transport.lengths = lengths[i];
    KVClient client({{"n", "unused:1"}}, "status-test", &transport);
    std::array<char, 4> output{};
    size_t length = 999;
    EXPECT_EQ(
        client.GetAutoWithStatus("key", output.data(), output.size(), &length),
        Status::kIOError);
    EXPECT_EQ(length, 0);
    EXPECT_FALSE(client.GetAuto("key", output.data(), output.size(), &length));
    EXPECT_EQ(length, 0);
    EXPECT_EQ(
        MetricValue(client.MetricsSnapshot(), "dfkv_client_ops_served_total"),
        0);
  }
}

TEST(GetAutoStatus, ZeroLengthSuccessAndOptionalLengthRemainCompatible) {
  StatusTransport transport;
  transport.lengths = {0};
  KVClient client({{"n", "unused:1"}}, "status-test", &transport);
  size_t length = 999;
  EXPECT_EQ(client.GetAutoWithStatus("key", nullptr, 0, &length), Status::kOk);
  EXPECT_EQ(length, 0);
  EXPECT_TRUE(client.GetAuto("key", nullptr, 0, &length));
  transport.lengths = {3};
  std::array<char, 4> output{};
  EXPECT_EQ(
      client.GetAutoWithStatus("key", output.data(), output.size(), nullptr),
      Status::kOk);
}

TEST(GetAutoStatus, OldBoolAndTcpBatchCountEachPublicRequestOnce) {
  StatusTransport transport;
  KVClient client({{"n", "unused:1"}}, "status-test", &transport);
  std::array<char, 4> output{};
  size_t length = 0;
  EXPECT_EQ(
      client.GetAutoWithStatus("key", output.data(), output.size(), &length),
      Status::kOk);
  EXPECT_TRUE(client.GetAuto("key", output.data(), output.size(), &length));
  const std::vector<KvGetItem> items{{"key", output.data(), output.size()}};
  const std::vector<bool> hits = client.BatchGetAuto(items, nullptr);
  ASSERT_EQ(hits.size(), 1);
  EXPECT_TRUE(hits[0]);
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_op_requests_total{op=\"get\"}"),
            3);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_op_keys_total{op=\"get\"}"), 3);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_op_hits_total{op=\"get\"}"), 3);
  EXPECT_EQ(MetricValue(metrics, "dfkv_client_op_bytes_total{op=\"get\"}"), 9);
}

TEST(GetAutoStatus, ConcurrentCallsUseIndependentResultsAndCountOnce) {
  StatusTransport transport;
  KVClient client({{"n", "unused:1"}}, "status-test", &transport);
  std::array<std::array<char, 4>, 8> outputs{};
  std::array<size_t, 8> lengths{};
  std::array<Status, 8> statuses{};
  std::vector<std::thread> threads;
  for (size_t i = 0; i < outputs.size(); ++i) {
    threads.emplace_back([&client, &outputs, &lengths, &statuses, i] {
      statuses[i] = client.GetAutoWithStatus("key", outputs[i].data(),
                                             outputs[i].size(), &lengths[i]);
    });
  }
  for (std::thread& thread : threads) thread.join();
  for (size_t i = 0; i < outputs.size(); ++i) {
    EXPECT_EQ(statuses[i], Status::kOk);
    EXPECT_EQ(lengths[i], 3);
    EXPECT_EQ(std::string(outputs[i].data(), lengths[i]), "abc");
  }
  EXPECT_EQ(transport.calls.load(), 8);
  EXPECT_EQ(MetricValue(client.MetricsSnapshot(),
                        "dfkv_client_op_requests_total{op=\"get\"}"),
            8);
}

class GetAutoStatusTcpTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-status-XXXXXX")
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

TEST_F(GetAutoStatusTcpTest, RealHitMissCapacityAndStoppedServerStayDistinct) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  "status-test", &transport);
  const std::string value = "abc";
  ASSERT_TRUE(client.Put("key", value.data(), value.size()));
  std::array<char, 4> output{};
  size_t length = 999;
  EXPECT_EQ(
      client.GetAutoWithStatus("key", output.data(), output.size(), &length),
      Status::kOk);
  EXPECT_EQ(std::string(output.data(), length), value);
  EXPECT_EQ(
      client.GetAutoWithStatus("absent", output.data(), output.size(), &length),
      Status::kNotFound);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(client.GetAutoWithStatus("key", output.data(), 1, &length),
            Status::kInvalid);
  EXPECT_EQ(length, 0);
  EXPECT_FALSE(client.GetAuto("absent", output.data(), output.size(), &length));
  EXPECT_FALSE(client.GetAuto("key", output.data(), 1, &length));
  server_->Stop();
  EXPECT_EQ(
      client.GetAutoWithStatus("key", output.data(), output.size(), &length),
      Status::kIOError);
  EXPECT_EQ(length, 0);
}

}  // namespace
}  // namespace dfkv
