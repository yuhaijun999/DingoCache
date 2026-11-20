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
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
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

constexpr char kNamespace[] = "sg-status-test";

class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    const char* old = std::getenv(name);
    if (old != nullptr) {
      had_old_ = true;
      old_ = old;
    }
    EXPECT_EQ(::setenv(name, value, 1), 0);
  }
  ~ScopedEnv() {
    if (had_old_) {
      EXPECT_EQ(::setenv(name_.c_str(), old_.c_str(), 1), 0);
    } else {
      EXPECT_EQ(::unsetenv(name_.c_str()), 0);
    }
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;

 private:
  std::string name_;
  std::string old_;
  bool had_old_ = false;
};

struct SgReply {
  Status status = Status::kOk;
  size_t length = 3;
  std::string bytes = "abc";
};

struct SgCall {
  std::string node;
  size_t count;
  size_t capacity;
};

struct SgStatusTransport final : Transport {
  bool pipeline = true;
  bool throw_on_read = false;
  bool require_overlap = false;
  bool overlap = false;
  size_t active = 0;
  int status_delta = 0;
  int length_delta = 0;
  std::map<std::string, SgReply> replies;
  std::vector<SgCall> calls;
  std::mutex mutex;
  std::condition_variable entered;

  void Set(const std::string& key, SgReply reply = {}) {
    replies[ToBlockKey(kNamespace, key).Filename()] = std::move(reply);
  }
  bool pipelined() const override { return pipeline; }
  Status Cache(const std::string&, const BlockKey&, const void*,
               size_t) override {
    return Status::kInvalid;
  }
  Status Range(const std::string&, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    ADD_FAILURE() << "SG status GET must retain RangeIntoMulti";
    return Status::kIOError;
  }
  Status Lookup(const std::string&, const BlockKey&, uint64_t*) override {
    return Status::kInvalid;
  }
  Status Exist(const std::string&, const BlockKey&, bool*) override {
    return Status::kInvalid;
  }
  std::vector<Status> RangeIntoMulti(
      const std::string& node, const std::vector<BlockKey>& keys,
      const std::vector<RangeDstMulti>& destinations,
      std::vector<size_t>* lengths, std::string*) override {
    size_t capacity = 0;
    if (!destinations.empty()) {
      for (const RangeDstSegment& segment : destinations.front().payloads) {
        capacity += segment.second;
      }
    }
    {
      std::unique_lock<std::mutex> lock(mutex);
      calls.push_back({node, keys.size(), capacity});
      if (require_overlap) {
        ++active;
        if (active >= 2) overlap = true;
        entered.notify_all();
        EXPECT_TRUE(entered.wait_for(lock, std::chrono::seconds(2),
                                     [this] { return overlap; }));
        --active;
      }
    }
    if (throw_on_read) throw std::runtime_error("SG transport test failure");
    EXPECT_EQ(keys.size(), destinations.size());
    std::vector<Status> statuses;
    lengths->clear();
    for (size_t i = 0; i < keys.size(); ++i) {
      const SgReply& reply = replies.at(keys[i].Filename());
      statuses.push_back(reply.status);
      lengths->push_back(reply.length);
      if (reply.status != Status::kOk) continue;
      size_t offset = 0;
      for (const RangeDstSegment& segment : destinations[i].payloads) {
        const size_t copied =
            std::min(segment.second, reply.bytes.size() - offset);
        if (copied != 0) {
          std::memcpy(segment.first, reply.bytes.data() + offset, copied);
        }
        offset += copied;
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

TEST(BatchGetAutoSgStatus, PreservesMixedStatusesOrderAndMetricsInBothModes) {
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
    SgStatusTransport transport;
    transport.pipeline = pipeline;
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<std::array<char, 4>, 8> output{};
    std::vector<KvGetItemSg> items;
    for (size_t i = 0; i < expected.size(); ++i) {
      const std::string key = std::to_string(i);
      transport.Set(key, {expected[i], 3, "abc"});
      items.push_back({key, {output[i].data(), output[i].data() + 1}, {1, 3}});
    }
    std::vector<size_t> lengths{999};
    EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, &lengths), expected);
    EXPECT_EQ(lengths, (std::vector<size_t>{3, 0, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(std::string(output[0].data(), 3), "abc");
    ASSERT_EQ(transport.calls.size(), 1);
    EXPECT_EQ(transport.calls.front().count, 8);
    const std::string metrics = client.MetricsSnapshot();
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 1);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 8);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_hits_total"), 1);
    EXPECT_EQ(Counter(metrics, "dfkv_client_op_bytes_total"), 3);
  }
}

TEST(BatchGetAutoSgStatus, RejectsInvalidSlotsBeforeIoAndAllowsEmptyValues) {
  SgStatusTransport transport;
  transport.Set("valid");
  transport.Set("zero", {Status::kOk, 0, ""});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  std::array<char, 4> output{};
  const std::vector<KvGetItemSg> items{
      {"", {output.data()}, {4}},
      {"shape", {output.data()}, {}},
      {"null", {nullptr}, {1}},
      {"overflow",
       {output.data(), output.data()},
       {std::numeric_limits<size_t>::max(), 1}},
      {"valid", {output.data()}, {4}},
      {"zero", {}, {}},
      {"zero", {nullptr}, {0}}};
  std::vector<size_t> lengths{999};
  EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, &lengths),
            (std::vector<Status>{Status::kInvalid, Status::kInvalid,
                                 Status::kInvalid, Status::kInvalid,
                                 Status::kOk, Status::kOk, Status::kOk}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0, 0, 0, 0, 3, 0, 0}));
  size_t submitted = 0;
  for (const SgCall& call : transport.calls) submitted += call.count;
  EXPECT_EQ(submitted, 3);
}

TEST(BatchGetAutoSgStatus, ScattersBinaryBytesAcrossArbitrarySegmentCounts) {
  SgStatusTransport transport;
  const std::string bytes("a\0bc\0d", 6);
  transport.Set("binary", {Status::kOk, bytes.size(), bytes});
  const std::string large(1024, 'z');
  transport.Set("many", {Status::kOk, large.size(), large});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  std::array<char, 3> first{};
  std::array<char, 5> second{};
  std::vector<char> many(large.size(), '\0');
  KvGetItemSg wide;
  wide.key = "many";
  for (char& byte : many) {
    wide.dsts.push_back(&byte);
    wide.caps.push_back(1);
  }
  std::vector<size_t> lengths;
  EXPECT_EQ(
      client.BatchGetAutoSgWithStatus(
          {{"binary", {first.data(), nullptr, second.data()}, {3, 0, 5}}, wide},
          &lengths),
      (std::vector<Status>{Status::kOk, Status::kOk}));
  EXPECT_EQ(lengths, (std::vector<size_t>{6, 1024}));
  EXPECT_EQ(std::string(first.data(), 3) + std::string(second.data(), 3),
            bytes);
  EXPECT_EQ(second[3], '\0');
  EXPECT_EQ(std::string(many.data(), many.size()), large);
}

TEST(BatchGetAutoSgStatus, DoesNotRetryMinorityMissOrChangeOldApi) {
  const ScopedEnv retries("DFKV_GET_MISS_RETRIES", "1");
  SgStatusTransport transport;
  transport.Set("hit");
  transport.Set("miss", {Status::kNotFound, 0, ""});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  std::array<std::array<char, 4>, 2> output{};
  const std::vector<KvGetItemSg> items{{"hit", {output[0].data()}, {4}},
                                       {"miss", {output[1].data()}, {4}}};
  std::vector<size_t> lengths;
  EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, &lengths),
            (std::vector<Status>{Status::kOk, Status::kNotFound}));
  ASSERT_EQ(transport.calls.size(), 1);
  EXPECT_EQ(client.BatchGetAutoSg(items, &lengths),
            (std::vector<bool>{true, false}));
  ASSERT_EQ(transport.calls.size(), 3);
  EXPECT_EQ(transport.calls[1].count, 2);
  EXPECT_EQ(transport.calls[2].count, 1);
  EXPECT_EQ(lengths, (std::vector<size_t>{3, 0}));
  const std::string metrics = client.MetricsSnapshot();
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_requests_total"), 2);
  EXPECT_EQ(Counter(metrics, "dfkv_client_op_keys_total"), 4);
}

TEST(BatchGetAutoSgStatus, MissingRouteAndCooldownAreIoErrorsWithoutNewIo) {
  SgStatusTransport transport;
  transport.Set("key", {Status::kIOError, 0, ""});
  std::array<char, 4> output{};
  const std::vector<KvGetItemSg> items{{"key", {output.data()}, {4}}};
  KVClient unrouted({}, kNamespace, &transport);
  std::vector<size_t> lengths{999};
  EXPECT_EQ(unrouted.BatchGetAutoSgWithStatus(items, &lengths),
            (std::vector<Status>{Status::kIOError}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  EXPECT_TRUE(transport.calls.empty());
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  for (int call = 0; call < 2; ++call) {
    EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, &lengths),
              (std::vector<Status>{Status::kIOError}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  }
  EXPECT_EQ(transport.calls.size(), 1);
}

TEST(BatchGetAutoSgStatus, LocalErrorsDoNotCoolHealthyRoute) {
  for (const Status status :
       {Status::kInvalid, Status::kCacheFull, Status::kQuotaExceeded,
        Status::kResourceExhausted, Status::kNoCompatibleRail}) {
    SCOPED_TRACE(StatusName(status));
    SgStatusTransport transport;
    transport.Set("key", {status, 0, ""});
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<char, 4> output{};
    const std::vector<KvGetItemSg> items{{"key", {output.data()}, {4}}};
    EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, nullptr),
              (std::vector<Status>{status}));
    transport.Set("key");
    EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, nullptr),
              (std::vector<Status>{Status::kOk}));
    EXPECT_EQ(transport.calls.size(), 2);
  }
}

TEST(BatchGetAutoSgStatus, RejectsShortAndExtraStatusVectorsBeforePublishing) {
  for (const int delta : {-1, 1}) {
    SgStatusTransport transport;
    transport.Set("hit");
    transport.Set("miss", {Status::kNotFound, 0, ""});
    transport.status_delta = delta;
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<std::array<char, 4>, 2> output{};
    std::vector<size_t> lengths{999};
    EXPECT_EQ(
        client.BatchGetAutoSgWithStatus({{"hit", {output[0].data()}, {4}},
                                         {"miss", {output[1].data()}, {4}}},
                                        &lengths),
        (std::vector<Status>{Status::kIOError, Status::kIOError}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0, 0}));
    EXPECT_NE(
        client.MetricsSnapshot().find("\ndfkv_client_ops_served_total 0\n"),
        std::string::npos);
  }
}

TEST(BatchGetAutoSgStatus, MalformedLengthsPreserveExplicitErrorsNotSuccess) {
  for (const int delta : {-1, 1}) {
    SgStatusTransport transport;
    transport.Set("hit");
    transport.Set("miss", {Status::kNotFound, 42, ""});
    transport.Set("local", {Status::kResourceExhausted, 42, ""});
    transport.length_delta = delta;
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<std::array<char, 4>, 3> output{};
    std::vector<size_t> lengths;
    EXPECT_EQ(
        client.BatchGetAutoSgWithStatus({{"hit", {output[0].data()}, {4}},
                                         {"miss", {output[1].data()}, {4}},
                                         {"local", {output[2].data()}, {4}}},
                                        &lengths),
        (std::vector<Status>{Status::kIOError, Status::kNotFound,
                             Status::kResourceExhausted}));
    EXPECT_EQ(lengths, (std::vector<size_t>{0, 0, 0}));
  }
}

TEST(BatchGetAutoSgStatus, MalformedSuccessDoesNotMarkGoodOrBad) {
  for (const int delta : {-1, 1}) {
    SgStatusTransport transport;
    transport.Set("hit");
    transport.length_delta = delta;
    KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
    std::array<char, 4> output{};
    const std::vector<KvGetItemSg> items{{"hit", {output.data()}, {4}}};
    EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, nullptr),
              (std::vector<Status>{Status::kIOError}));
    const std::string metrics = client.MetricsSnapshot();
    EXPECT_NE(metrics.find("\ndfkv_client_ops_served_total 0\n"),
              std::string::npos);
    EXPECT_NE(metrics.find("\ndfkv_client_io_errors_total 0\n"),
              std::string::npos);
    transport.length_delta = 0;
    EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, nullptr),
              (std::vector<Status>{Status::kOk}));
    EXPECT_EQ(transport.calls.size(), 2);
  }
}

TEST(BatchGetAutoSgStatus, OversizeIsInvalidAndDoesNotHideValidSibling) {
  SgStatusTransport transport;
  transport.Set("big", {Status::kOk, 7, "abcdefg"});
  transport.Set("hit");
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  std::array<std::array<char, 4>, 2> output{};
  std::vector<size_t> lengths;
  EXPECT_EQ(client.BatchGetAutoSgWithStatus({{"big", {output[0].data()}, {4}},
                                             {"hit", {output[1].data()}, {4}}},
                                            &lengths),
            (std::vector<Status>{Status::kInvalid, Status::kOk}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0, 3}));
}

TEST(BatchGetAutoSgStatus, GroupsByNodeCapacityAndShardsInParallel) {
  SgStatusTransport transport;
  transport.require_overlap = true;
  KVClient client({{"a", "unused:1"}, {"b", "unused:2"}}, kNamespace,
                  &transport);
  std::array<std::array<char, 8>, 128> output{};
  std::vector<KvGetItemSg> items;
  for (size_t i = 0; i < output.size(); ++i) {
    const std::string key = std::to_string(i);
    const std::string bytes(3, static_cast<char>('a' + i % 26));
    transport.Set(key, {Status::kOk, 3, bytes});
    items.push_back({key,
                     {output[i].data(), output[i].data() + 1},
                     {1, i % 2 == 0 ? 3u : 7u}});
  }
  std::vector<size_t> lengths;
  EXPECT_EQ(client.BatchGetAutoSgWithStatus(items, &lengths),
            std::vector<Status>(items.size(), Status::kOk));
  EXPECT_EQ(lengths, std::vector<size_t>(items.size(), 3));
  EXPECT_TRUE(transport.overlap);
  size_t submitted = 0;
  for (const SgCall& call : transport.calls) {
    EXPECT_TRUE(call.capacity == 4 || call.capacity == 8);
    EXPECT_FALSE(call.node.empty());
    submitted += call.count;
  }
  EXPECT_EQ(submitted, items.size());
  EXPECT_GE(transport.calls.size(), 4);
  EXPECT_LT(transport.calls.size(), items.size());
  for (size_t i = 0; i < output.size(); ++i) {
    EXPECT_EQ(std::string(output[i].data(), 3),
              std::string(3, static_cast<char>('a' + i % 26)));
  }
}

TEST(BatchGetAutoSgStatus, WorkerExceptionLeavesIoAndEmptyBatchClearsLengths) {
  SgStatusTransport transport;
  transport.throw_on_read = true;
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport);
  std::array<char, 4> output{};
  std::vector<size_t> lengths{999};
  EXPECT_EQ(client.BatchGetAutoSgWithStatus({{"key", {output.data()}, {4}}},
                                            &lengths),
            (std::vector<Status>{Status::kIOError}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
  EXPECT_TRUE(client.BatchGetAutoSgWithStatus({}, &lengths).empty());
  EXPECT_TRUE(lengths.empty());
  EXPECT_TRUE(client.BatchGetAutoSgWithStatus({}, nullptr).empty());
}

class BatchGetAutoSgStatusTcpTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-sg-status-XXXXXX")
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

TEST_F(BatchGetAutoSgStatusTcpTest, RealScatterMissCapacityAndStoppedService) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  kNamespace, &transport);
  const std::string bytes("a\0bcd", 5);
  ASSERT_TRUE(client.Put("key", bytes.data(), bytes.size()));
  std::array<std::array<char, 8>, 3> output{};
  std::vector<size_t> lengths;
  EXPECT_EQ(
      client.BatchGetAutoSgWithStatus(
          {{"key", {output[0].data(), output[0].data() + 2}, {2, 6}},
           {"absent", {output[1].data()}, {8}},
           {"key", {output[2].data()}, {1}}},
          &lengths),
      (std::vector<Status>{Status::kOk, Status::kNotFound, Status::kInvalid}));
  EXPECT_EQ(lengths, (std::vector<size_t>{5, 0, 0}));
  EXPECT_EQ(std::string(output[0].data(), bytes.size()), bytes);
  server_.reset();
  EXPECT_EQ(client.BatchGetAutoSgWithStatus({{"key", {output[0].data()}, {8}}},
                                            &lengths),
            (std::vector<Status>{Status::kIOError}));
  EXPECT_EQ(lengths, (std::vector<size_t>{0}));
}

}  // namespace
}  // namespace dfkv
