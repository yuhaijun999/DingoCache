#include <gtest/gtest.h>

#include "client/dfkv_c_api.h"
// POSIX mkdtemp has no equivalent declaration in the C++ standard headers.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
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

struct CApiStatusTransport final : Transport {
  Status status = Status::kOk;
  uint64_t length = 3;
  bool throw_on_read = false;
  int calls = 0;
  std::vector<BlockKey> observed_keys;

  Status Cache(const std::string&, const BlockKey&, const void*,
               size_t) override {
    return Status::kInvalid;
  }
  Status Range(const std::string&, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    ADD_FAILURE() << "Scalar GET must retain the RangeInto path";
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
    if (throw_on_read) throw std::runtime_error("C ABI status test exception");
    observed_keys = keys;
    EXPECT_EQ(keys.size(), 1);
    EXPECT_EQ(destinations.size(), 1);
    *value_lens = {length};
    if (status == Status::kOk && !destinations.empty() &&
        destinations[0].n != 0) {
      std::memcpy(destinations[0].payload, "abc",
                  std::min<size_t>(3, destinations[0].n));
    }
    return {status};
  }
};

TEST(CApiGetStatus, MapsEveryExistingStatusToTheStableCResult) {
  struct Mapping {
    Status status;
    int result;
  };
  const std::array<Mapping, 8> mappings{{
      {Status::kOk, DFKV_GET_OK},
      {Status::kNotFound, DFKV_GET_NOT_FOUND},
      {Status::kCacheFull, DFKV_GET_CACHE_FULL},
      {Status::kQuotaExceeded, DFKV_GET_QUOTA_EXCEEDED},
      {Status::kIOError, DFKV_GET_IO_ERROR},
      {Status::kInvalid, DFKV_GET_INVALID},
      {Status::kResourceExhausted, DFKV_GET_RESOURCE_EXHAUSTED},
      {Status::kNoCompatibleRail, DFKV_GET_NO_COMPATIBLE_RAIL},
  }};
  for (const Mapping& mapping : mappings) {
    SCOPED_TRACE(StatusName(mapping.status));
    CApiStatusTransport transport;
    transport.status = mapping.status;
    KVClient client({{"n", "unused:1"}}, "c-status-test", &transport);
    std::array<char, 4> output{};
    uint64_t length = 999;
    EXPECT_EQ(dfkv_get_auto_status(&client, "key", 3, output.data(),
                                   output.size(), &length),
              mapping.result);
    EXPECT_EQ(length, mapping.status == Status::kOk ? 3 : 0);
    EXPECT_EQ(transport.calls, 1);
    if (mapping.status == Status::kOk) {
      EXPECT_EQ(std::string(output.data(), length), "abc");
    }
  }
}

TEST(CApiGetStatus, InvalidArgumentsClearLengthWithoutCallingTransport) {
  CApiStatusTransport transport;
  KVClient client({{"n", "unused:1"}}, "c-status-test", &transport);
  char output = 0;
  uint64_t length = 999;
  EXPECT_EQ(dfkv_get_auto_status(nullptr, "key", 3, &output, 1, &length),
            DFKV_GET_INVALID);
  EXPECT_EQ(length, 0);
  length = 999;
  EXPECT_EQ(dfkv_get_auto_status(&client, nullptr, 3, &output, 1, &length),
            DFKV_GET_INVALID);
  EXPECT_EQ(length, 0);
  length = 999;
  EXPECT_EQ(dfkv_get_auto_status(&client, "key", 0, &output, 1, &length),
            DFKV_GET_INVALID);
  EXPECT_EQ(length, 0);
  length = 999;
  EXPECT_EQ(dfkv_get_auto_status(&client, "key", 3, nullptr, 1, &length),
            DFKV_GET_INVALID);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls, 0);
}

TEST(CApiGetStatus, ContainsExceptionsAndDoesNotTurnThemIntoMisses) {
  CApiStatusTransport transport;
  transport.throw_on_read = true;
  KVClient client({{"n", "unused:1"}}, "c-status-test", &transport);
  std::array<char, 4> output{};
  uint64_t length = 999;
  EXPECT_EQ(dfkv_get_auto_status(&client, "key", 3, output.data(),
                                 output.size(), &length),
            DFKV_GET_INTERNAL_ERROR);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls, 1);
  // The old API still folds an exception into its historical false result.
  EXPECT_EQ(
      dfkv_get_auto(&client, "key", 3, output.data(), output.size(), &length),
      0);
  EXPECT_EQ(length, 0);
}

TEST(CApiGetStatus, AcceptsBinaryKeysAndOptionalLength) {
  CApiStatusTransport transport;
  const std::string key_namespace("ns\0x", 4);
  KVClient client({{"n", "unused:1"}}, key_namespace, &transport);
  const std::array<char, 3> key{'k', '\0', 'x'};
  std::array<char, 4> output{};
  EXPECT_EQ(dfkv_get_auto_status(&client, key.data(), key.size(), output.data(),
                                 output.size(), nullptr),
            DFKV_GET_OK);
  ASSERT_EQ(transport.observed_keys.size(), 1);
  const BlockKey expected =
      ToBlockKey(key_namespace, std::string(key.data(), key.size()));
  EXPECT_EQ(transport.observed_keys[0], expected);
  EXPECT_EQ(std::string(output.data(), 3), "abc");
  transport.length = 0;
  uint64_t length = 999;
  EXPECT_EQ(dfkv_get_auto_status(&client, key.data(), key.size(), nullptr, 0,
                                 &length),
            DFKV_GET_OK);
  EXPECT_EQ(length, 0);
}

TEST(CApiGetStatus, NoRouteAndCooldownRemainErrorsAndLegacyRemainsBoolean) {
  CApiStatusTransport transport;
  KVClient no_route({}, "c-status-test", &transport);
  char output = 0;
  uint64_t length = 999;
  EXPECT_EQ(dfkv_get_auto_status(&no_route, "key", 3, &output, 1, &length),
            DFKV_GET_IO_ERROR);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(transport.calls, 0);
  transport.status = Status::kIOError;
  KVClient client({{"n", "unused:1"}}, "c-status-test", &transport);
  EXPECT_EQ(dfkv_get_auto_status(&client, "key", 3, &output, 1, &length),
            DFKV_GET_IO_ERROR);
  EXPECT_EQ(dfkv_get_auto_status(&client, "other", 5, &output, 1, &length),
            DFKV_GET_IO_ERROR);
  EXPECT_EQ(transport.calls, 1);
  EXPECT_EQ(dfkv_get_auto(&client, "key", 3, &output, 1, &length), 0);
  EXPECT_EQ(length, 0);
}

class CApiGetStatusTcpTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-c-status-XXXXXX")
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

TEST_F(CApiGetStatusTcpTest, RealHitMissSmallCapacityAndIoFailureStayDistinct) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  "c-status-test", &transport);
  const std::array<char, 3> key{'k', '\0', 'x'};
  ASSERT_EQ(dfkv_put(&client, key.data(), key.size(), "abc", 3), 0);
  std::array<char, 4> output{};
  uint64_t length = 999;
  EXPECT_EQ(dfkv_get_auto_status(&client, key.data(), key.size(), output.data(),
                                 output.size(), &length),
            DFKV_GET_OK);
  EXPECT_EQ(length, 3);
  EXPECT_EQ(std::string(output.data(), length), "abc");
  EXPECT_EQ(dfkv_get_auto_status(&client, "absent", 6, output.data(),
                                 output.size(), &length),
            DFKV_GET_NOT_FOUND);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(dfkv_get_auto_status(&client, key.data(), key.size(), output.data(),
                                 1, &length),
            DFKV_GET_INVALID);
  EXPECT_EQ(length, 0);
  EXPECT_EQ(dfkv_get_auto(&client, "absent", 6, output.data(), output.size(),
                          &length),
            0);
  EXPECT_EQ(dfkv_get_auto(&client, key.data(), key.size(), output.data(),
                          output.size(), &length),
            1);
  EXPECT_EQ(length, 3);
  server_->Stop();
  EXPECT_EQ(dfkv_get_auto_status(&client, key.data(), key.size(), output.data(),
                                 output.size(), &length),
            DFKV_GET_IO_ERROR);
  EXPECT_EQ(length, 0);
}

}  // namespace
}  // namespace dfkv
