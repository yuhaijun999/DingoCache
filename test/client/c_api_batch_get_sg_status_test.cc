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
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
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

constexpr char kNamespace[] = "c-sg-status";

struct SgReply {
  Status status = Status::kOk;
  std::string value = std::string("a\0bc", 4);
};

struct SgStatusTransport final : Transport {
  std::atomic<int> calls{0};
  std::map<std::string, SgReply> replies;
  bool throw_on_read = false;

  void Set(const std::string& key, SgReply reply) {
    replies[ToBlockKey(kNamespace, key).Filename()] = std::move(reply);
  }
  Status Cache(const std::string&, const BlockKey&, const void*,
               size_t) override {
    return Status::kInvalid;
  }
  Status Range(const std::string&, const BlockKey&, uint64_t, uint64_t,
               std::string*, uint64_t*) override {
    ADD_FAILURE() << "SG status reads must use RangeIntoMulti";
    return Status::kIOError;
  }
  Status Lookup(const std::string&, const BlockKey&, uint64_t*) override {
    return Status::kInvalid;
  }
  Status Exist(const std::string&, const BlockKey&, bool*) override {
    return Status::kInvalid;
  }
  std::vector<Status> RangeIntoMulti(
      const std::string&, const std::vector<BlockKey>& keys,
      const std::vector<RangeDstMulti>& destinations,
      std::vector<size_t>* lengths, std::string*) override {
    ++calls;
    if (throw_on_read) throw std::runtime_error("SG status test exception");
    EXPECT_EQ(keys.size(), destinations.size());
    std::vector<Status> statuses;
    lengths->clear();
    for (size_t i = 0; i < keys.size(); ++i) {
      const SgReply& reply = replies.at(keys[i].Filename());
      statuses.push_back(reply.status);
      lengths->push_back(reply.value.size());
      if (reply.status != Status::kOk) continue;
      size_t offset = 0;
      for (const RangeDstSegment& segment : destinations[i].payloads) {
        const size_t copy =
            std::min(segment.second, reply.value.size() - offset);
        if (copy != 0) {
          std::memcpy(segment.first, reply.value.data() + offset, copy);
        }
        offset += copy;
      }
    }
    return statuses;
  }
};

TEST(CApiBatchGetSgStatus, PreservesMixedStatusesAndLegacyProjection) {
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
  SgStatusTransport transport;
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const std::array<std::string, 9> names{"0", "1", "2", "3", "4",
                                         "5", "6", "7", "8"};
  std::array<const void*, 9> keys{};
  std::array<uint64_t, 9> key_lens{};
  std::array<std::array<char, 4>, 9> outputs{};
  std::array<void*, 9> first_segments{};
  std::array<void**, 9> destinations{};
  const uint64_t cap = 4;
  std::array<const uint64_t*, 9> caps{};
  std::array<int, 9> counts{};
  std::array<int, 9> statuses{};
  std::array<uint64_t, 9> lengths{};
  for (size_t i = 0; i < names.size(); ++i) {
    keys[i] = names[i].data();
    key_lens[i] = names[i].size();
    first_segments[i] = outputs[i].data();
    destinations[i] = &first_segments[i];
    caps[i] = &cap;
    counts[i] = 1;
    transport.Set(names[i], {native[i], std::string("a\0bc", 4)});
  }
  EXPECT_EQ(dfkv_batch_get_auto_sg_status(
                &client, keys.data(), key_lens.data(), destinations.data(),
                caps.data(), counts.data(), 9, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, expected);
  EXPECT_EQ(lengths, (std::array<uint64_t, 9>{4, 0, 0, 0, 0, 0, 0, 0, 0}));
  EXPECT_EQ(std::string(outputs[0].data(), 4), std::string("a\0bc", 4));
  std::array<int, 9> hits{};
  EXPECT_EQ(dfkv_batch_get_auto_sg(
                &client, keys.data(), key_lens.data(), destinations.data(),
                caps.data(), counts.data(), 9, hits.data(), lengths.data()),
            0);
  EXPECT_EQ(hits, (std::array<int, 9>{1, 0, 0, 0, 0, 0, 0, 0, 0}));
}

TEST(CApiBatchGetSgStatus,
     RejectsGlobalArgumentsAndNegativeCountTouchesNothing) {
  SgStatusTransport transport;
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const void* key = "key";
  const uint64_t key_len = 3;
  char output = 0;
  void* segment = &output;
  void** destinations = &segment;
  const uint64_t cap = 1;
  const uint64_t* caps = &cap;
  const int count = 1;
  for (int missing = 0; missing < 7; ++missing) {
    SCOPED_TRACE(missing);
    int status = 999;
    uint64_t length = 999;
    EXPECT_EQ(
        dfkv_batch_get_auto_sg_status(
            missing == 0 ? nullptr : &client, missing == 1 ? nullptr : &key,
            missing == 2 ? nullptr : &key_len,
            missing == 3 ? nullptr : &destinations,
            missing == 4 ? nullptr : &caps, missing == 5 ? nullptr : &count, 1,
            missing == 6 ? nullptr : &status, &length),
        -1);
    EXPECT_EQ(status, missing == 6 ? 999 : DFKV_GET_INVALID);
    EXPECT_EQ(length, 0);
  }
  int status = 999;
  uint64_t length = 999;
  EXPECT_EQ(
      dfkv_batch_get_auto_sg_status(&client, nullptr, nullptr, nullptr, nullptr,
                                    nullptr, -1, &status, &length),
      -1);
  EXPECT_EQ(status, 999);
  EXPECT_EQ(length, 999);
  EXPECT_EQ(
      dfkv_batch_get_auto_sg_status(&client, nullptr, nullptr, nullptr, nullptr,
                                    nullptr, 0, nullptr, nullptr),
      0);
  EXPECT_EQ(
      dfkv_batch_get_auto_sg_status(nullptr, nullptr, nullptr, nullptr, nullptr,
                                    nullptr, 0, nullptr, nullptr),
      -1);
  EXPECT_EQ(transport.calls.load(), 0);
}

TEST(CApiBatchGetSgStatus,
     InvalidDescriptorsDoNotSuppressBinaryScatterSibling) {
  SgStatusTransport transport;
  const std::string key("k\0x", 3);
  transport.Set(key, {});
  transport.Set("empty", {Status::kOk, ""});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  char first = 0;
  std::array<char, 3> rest{};
  std::array<void*, 3> good{&first, nullptr, rest.data()};
  const std::array<uint64_t, 3> good_caps{1, 0, 3};
  void* bad = nullptr;
  const uint64_t bad_cap = 1;
  std::array<void*, 2> overflow{&first, rest.data()};
  const std::array<uint64_t, 2> overflow_caps{
      std::numeric_limits<uint64_t>::max(), 1};
  const std::array<const void*, 8> keys{nullptr, "key", "key",      "key",
                                        "key",   "key", key.data(), "empty"};
  const std::array<uint64_t, 8> key_lens{1, 0, 3, 3, 3, 3, 3, 5};
  std::array<void**, 8> destinations{nullptr,     nullptr, nullptr,
                                     nullptr,     &bad,    overflow.data(),
                                     good.data(), nullptr};
  std::array<const uint64_t*, 8> caps{nullptr,          nullptr,
                                      nullptr,          nullptr,
                                      &bad_cap,         overflow_caps.data(),
                                      good_caps.data(), nullptr};
  const std::array<int, 8> counts{0, 0, -1, 1, 1, 2, 3, 0};
  std::array<int, 8> statuses{};
  std::array<uint64_t, 8> lengths{};
  EXPECT_EQ(dfkv_batch_get_auto_sg_status(
                &client, keys.data(), key_lens.data(), destinations.data(),
                caps.data(), counts.data(), 8, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, (std::array<int, 8>{DFKV_GET_INVALID, DFKV_GET_INVALID,
                                          DFKV_GET_INVALID, DFKV_GET_INVALID,
                                          DFKV_GET_INVALID, DFKV_GET_INVALID,
                                          DFKV_GET_OK, DFKV_GET_OK}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 8>{0, 0, 0, 0, 0, 0, 4, 0}));
  EXPECT_EQ(first, 'a');
  EXPECT_EQ(std::string(rest.data(), 3), std::string("\0bc", 3));
  EXPECT_EQ(dfkv_batch_get_auto_sg_status(
                &client, keys.data(), key_lens.data(), destinations.data(),
                caps.data(), counts.data(), 8, statuses.data(), nullptr),
            0);
}

TEST(CApiBatchGetSgStatus, ArbitraryDescriptorCountIsNotLimitedToOneWr) {
  SgStatusTransport transport;
  transport.Set("key", {});
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const void* key = "key";
  const uint64_t key_len = 3;
  char output[4] = {};
  std::vector<void*> segments(129, nullptr);
  std::vector<uint64_t> capacities(129, 0);
  segments.back() = output;
  capacities.back() = 4;
  void** destinations = segments.data();
  const uint64_t* caps = capacities.data();
  const int count = static_cast<int>(segments.size());
  int status = 999;
  uint64_t length = 999;
  EXPECT_EQ(
      dfkv_batch_get_auto_sg_status(&client, &key, &key_len, &destinations,
                                    &caps, &count, 1, &status, &length),
      0);
  EXPECT_EQ(status, DFKV_GET_OK);
  EXPECT_EQ(length, 4);
  EXPECT_EQ(std::string(output, 4), std::string("a\0bc", 4));
}

TEST(CApiBatchGetSgStatus, ExecutorContainedExceptionIsIoErrorNotMiss) {
  SgStatusTransport transport;
  transport.throw_on_read = true;
  KVClient client({{"n", "unused:1"}}, kNamespace, &transport, 1);
  const void* key = "key";
  const uint64_t key_len = 3;
  char output = 0;
  void* segment = &output;
  void** destinations = &segment;
  const uint64_t cap = 1;
  const uint64_t* caps = &cap;
  const int count = 1;
  int status = 999;
  uint64_t length = 999;
  EXPECT_EQ(
      dfkv_batch_get_auto_sg_status(&client, &key, &key_len, &destinations,
                                    &caps, &count, 1, &status, &length),
      0);
  EXPECT_EQ(status, DFKV_GET_IO_ERROR);
  EXPECT_EQ(length, 0);
}

class CApiBatchGetSgStatusTcpTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-c-sg-status-XXXXXX")
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

TEST_F(CApiBatchGetSgStatusTcpTest,
       RealBinaryScatterMissCapacityAndStoppedService) {
  TcpTransport transport;
  transport.set_timeouts(100, 100);
  KVClient client({{"n", "127.0.0.1:" + std::to_string(server_->port())}},
                  kNamespace, &transport, 1);
  const std::string key("k\0x", 3);
  const std::string value("a\0bc", 4);
  ASSERT_EQ(
      dfkv_put(&client, key.data(), key.size(), value.data(), value.size()), 0);
  const std::array<const void*, 3> keys{key.data(), "absent", key.data()};
  const std::array<uint64_t, 3> key_lens{3, 6, 3};
  char first = 0;
  std::array<char, 3> rest{};
  char small = 0;
  std::array<void*, 2> good{&first, rest.data()};
  const std::array<uint64_t, 2> good_caps{1, 3};
  void* small_segment = &small;
  const uint64_t small_cap = 1;
  std::array<void**, 3> destinations{good.data(), nullptr, &small_segment};
  std::array<const uint64_t*, 3> caps{good_caps.data(), nullptr, &small_cap};
  const std::array<int, 3> counts{2, 0, 1};
  std::array<int, 3> statuses{};
  std::array<uint64_t, 3> lengths{999, 999, 999};
  EXPECT_EQ(dfkv_batch_get_auto_sg_status(
                &client, keys.data(), key_lens.data(), destinations.data(),
                caps.data(), counts.data(), 3, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, (std::array<int, 3>{DFKV_GET_OK, DFKV_GET_NOT_FOUND,
                                          DFKV_GET_INVALID}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 3>{4, 0, 0}));
  EXPECT_EQ(first, 'a');
  EXPECT_EQ(std::string(rest.data(), 3), std::string("\0bc", 3));
  server_->Stop();
  EXPECT_EQ(dfkv_batch_get_auto_sg_status(
                &client, keys.data(), key_lens.data(), destinations.data(),
                caps.data(), counts.data(), 3, statuses.data(), lengths.data()),
            0);
  EXPECT_EQ(statuses, (std::array<int, 3>{DFKV_GET_IO_ERROR, DFKV_GET_IO_ERROR,
                                          DFKV_GET_IO_ERROR}));
  EXPECT_EQ(lengths, (std::array<uint64_t, 3>{0, 0, 0}));
}

}  // namespace
}  // namespace dfkv
