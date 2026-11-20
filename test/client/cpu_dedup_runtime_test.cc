// The default path uses a tiny owned TCP/file server. The external mode uses
// the real transport selected by MakeClientTransport; only scheduling gates
// and the explicitly named local owner failure are injected by this test.
#include <gtest/gtest.h>

#include "client/dfkv_c_api.h"

// POSIX environment, temporary-directory and shared-memory APIs need C headers.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <regex>
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
#include "transport/transport.h"
#include "transport/transport_factory.h"

namespace dfkv {
namespace {

class ScopedEnvironment {
 public:
  ScopedEnvironment(const char* name, const char* value) : name_(name) {
    const char* previous = std::getenv(name);
    if (previous != nullptr) {
      had_value_ = true;
      previous_ = previous;
    }
    EXPECT_EQ(::setenv(name, value, 1), 0);
  }
  ~ScopedEnvironment() {
    if (had_value_) {
      ::setenv(name_.c_str(), previous_.c_str(), 1);
    } else {
      ::unsetenv(name_.c_str());
    }
  }
  ScopedEnvironment(const ScopedEnvironment&) = delete;
  ScopedEnvironment& operator=(const ScopedEnvironment&) = delete;

 private:
  std::string name_;
  std::string previous_;
  bool had_value_ = false;
};

// The gate runs after the public CPU API claimed its keys but before real I/O.
// A separate marker fetch proves the waiter's complete claim pass has run.
// Both gates are always released before future destructors join their threads.
class GatedTransport final : public Transport {
 public:
  explicit GatedTransport(std::unique_ptr<Transport> inner)
      : inner_(std::move(inner)) {}
  bool pipelined() const override { return inner_->pipelined(); }
  Status Cache(const std::string& node, const BlockKey& key, const void* data,
               size_t length) override {
    return inner_->Cache(node, key, data, length);
  }
  Status Range(const std::string& node, const BlockKey& key, uint64_t offset,
               uint64_t length, std::string* out,
               uint64_t* value_length) override {
    return inner_->Range(node, key, offset, length, out, value_length);
  }
  Status Lookup(const std::string& node, const BlockKey& key,
                uint64_t* value_length) override {
    return inner_->Lookup(node, key, value_length);
  }
  Status Exist(const std::string& node, const BlockKey& key,
               bool* exists) override {
    return inner_->Exist(node, key, exists);
  }
  std::vector<Status> RangeInto(const std::string& node,
                                const std::vector<BlockKey>& keys,
                                const std::vector<RangeDst>& destinations,
                                std::vector<uint64_t>* lengths) override {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      entered_ = true;
      condition_.notify_all();
      if (!condition_.wait_for(lock, std::chrono::seconds(5),
                               [this] { return released_; })) {
        ADD_FAILURE() << "Runtime test did not release its transport gate";
        lengths->assign(keys.size(), 0);
        return std::vector<Status>(keys.size(), Status::kIOError);
      }
    }
    if (inject_local_io_error_) {
      injected_keys_.fetch_add(keys.size(), std::memory_order_relaxed);
      lengths->assign(keys.size(), 0);
      return std::vector<Status>(keys.size(), Status::kIOError);
    }
    delegated_keys_.fetch_add(keys.size(), std::memory_order_relaxed);
    return inner_->RangeInto(node, keys, destinations, lengths);
  }
  bool WaitUntilEntered() {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, std::chrono::seconds(3),
                               [this] { return entered_; });
  }
  void Release() {
    const std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    condition_.notify_all();
  }
  void InjectLocalIoError() { inject_local_io_error_ = true; }
  uint64_t delegated_keys() const {
    return delegated_keys_.load(std::memory_order_relaxed);
  }
  uint64_t injected_keys() const {
    return injected_keys_.load(std::memory_order_relaxed);
  }

 private:
  std::unique_ptr<Transport> inner_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool entered_ = false;
  bool released_ = false;
  bool inject_local_io_error_ = false;
  std::atomic<uint64_t> delegated_keys_{0};
  std::atomic<uint64_t> injected_keys_{0};
};

struct ReleaseGates {
  GatedTransport* owner;
  GatedTransport* waiter;
  ~ReleaseGates() {
    owner->Release();
    waiter->Release();
  }
};

struct ReadResult {
  int code = -1;
  std::vector<int> statuses;
  std::vector<uint64_t> lengths;
  std::vector<std::string> bytes;
};

ReadResult Read(KVClient* client, const std::vector<std::string>& keys) {
  ReadResult result;
  result.statuses.assign(keys.size(), -999);
  result.lengths.assign(keys.size(), 999);
  result.bytes.assign(keys.size(), std::string(64, '\0'));
  std::vector<const void*> key_pointers;
  std::vector<uint64_t> key_lengths;
  std::vector<void*> destinations;
  const std::vector<uint64_t> capacities(keys.size(), 64);
  for (size_t i = 0; i < keys.size(); ++i) {
    key_pointers.push_back(keys[i].data());
    key_lengths.push_back(keys[i].size());
    destinations.push_back(result.bytes[i].data());
  }
  result.code = dfkv_batch_get_auto_deduplicated_status(
      client, key_pointers.data(), key_lengths.data(), destinations.data(),
      capacities.data(), static_cast<int>(keys.size()), result.statuses.data(),
      result.lengths.data());
  return result;
}

uint64_t Counter(const KVClient& client, const std::string& name,
                 bool operation = false) {
  const std::string text = client.MetricsSnapshot();
  const std::string prefix = "\n" + name + (operation ? "{op=\"get\"} " : " ");
  const size_t position = text.find(prefix);
  EXPECT_NE(position, std::string::npos) << name;
  return position == std::string::npos
             ? 0
             : std::stoull(text.substr(position + prefix.size()));
}

std::string Payload(unsigned int seed) {
  std::string result(64, '\0');
  for (size_t i = 1; i < result.size(); ++i) {
    result[i] = static_cast<char>((seed + i * 13) & 255);
  }
  return result;
}

class CpuDedupRuntime : public ::testing::Test {
 public:
  ~CpuDedupRuntime() override = default;

 protected:
  void SetUp() override {
    const char* external = std::getenv("DFKV_CPU_DEDUP_RUNTIME_MEMBERS");
    std::string tag = "local";
    if (external != nullptr) {
      const char* external_tag =
          std::getenv("DFKV_CPU_DEDUP_RUNTIME_NAMESPACE");
      ASSERT_NE(external_tag, nullptr);
      ASSERT_TRUE(
          std::regex_match(external_tag, std::regex("[A-Za-z0-9_-]{1,64}")));
      tag = external_tag;
      const std::string input(external);
      std::smatch match;
      ASSERT_TRUE(std::regex_match(
          input, match,
          std::regex("([A-Za-z0-9_-]+)=(127\\.0\\.0\\.1:([0-9]{1,5}))")));
      const int port = std::stoi(match[3]);
      ASSERT_GT(port, 0);
      ASSERT_LE(port, 65535);
      members_.push_back({match[1], match[2]});
    } else {
      Set("DFKV_RDMA", "0");
    }
    Set("DFKV_CLIENT_NODE_DEDUP", "1");
    Set("DFKV_NODE_DEDUP_ARENA_MB", "1");
    Set("DFKV_NODE_DEDUP_SLOTS", "1024");
    Set("DFKV_NODE_DEDUP_WAIT_MS", "1000");
    Set("DFKV_NODE_DEDUP_TAKEOVER_MS", "5000");
    Set("DFKV_NODE_DEDUP_TTL_MS", "5000");
    Set("DFKV_GET_MISS_RETRIES", "0");
    Set("DFKV_RDMA_OP_TIMEOUT_MS", "2000");
    Set("DFKV_RDMA_BATCH_OP_TIMEOUT_MS", "2000");
    Set("DFKV_RAM_TIER", "0");
    Set("DFKV_PUT_ACK_MODE", "disk");
    if (external == nullptr) {
      std::string pattern =
          (std::filesystem::temp_directory_path() / "dfkv-cpu-runtime-XXXXXX")
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
      members_.push_back({"n", "127.0.0.1:" + std::to_string(server_->port())});
    }
    static std::atomic<uint64_t> sequence{0};
    namespace_ =
        "cpu-runtime/" + tag + "/" +
        ::testing::UnitTest::GetInstance()->current_test_info()->name() + "/" +
        std::to_string(::getpid()) + "/" +
        std::to_string(sequence.fetch_add(1)) + "/" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    shm_name_ = NodeDedup::EnvSegmentName(ToBlockKey(namespace_, "").digest_hi);
    ASSERT_EQ(std::filesystem::symlink_status("/dev/shm" + shm_name_).type(),
              std::filesystem::file_type::not_found);
    std::string owner_mode;
    std::string waiter_mode;
    std::unique_ptr<Transport> owner = MakeClientTransport(&owner_mode);
    std::unique_ptr<Transport> waiter = MakeClientTransport(&waiter_mode);
    ASSERT_NE(owner, nullptr) << owner_mode;
    ASSERT_NE(waiter, nullptr) << waiter_mode;
    const char* rdma = std::getenv("DFKV_RDMA");
    const std::string expected = rdma != nullptr && std::string(rdma) == "1"
                                     ? "rdma"
                                     : "tcp(rdma-not-requested)";
    ASSERT_EQ(owner_mode, expected);
    ASSERT_EQ(waiter_mode, expected);
    RecordProperty("inner_transport", owner_mode);
    RecordProperty("key_namespace", namespace_);
    RecordProperty("shm_name", shm_name_);
    owner_transport_ = std::make_unique<GatedTransport>(std::move(owner));
    waiter_transport_ = std::make_unique<GatedTransport>(std::move(waiter));
  }
  void TearDown() override {
    waiter_transport_.reset();
    owner_transport_.reset();
    server_.reset();
    if (cleanup_shm_) {
      struct stat current {};
      const std::string path = "/dev/shm" + shm_name_;
      const int observed = ::lstat(path.c_str(), &current);
      EXPECT_EQ(observed, 0);
      if (observed == 0 && S_ISREG(current.st_mode) &&
          current.st_dev == shm_identity_.st_dev &&
          current.st_ino == shm_identity_.st_ino) {
        EXPECT_EQ(::shm_unlink(shm_name_.c_str()), 0);
        RecordProperty("shm_cleanup", "verified_dev_inode_unlinked");
      } else {
        ADD_FAILURE() << "Private test segment changed; do not unlink it";
      }
    }
    if (!directory_.empty()) std::filesystem::remove_all(directory_);
  }
  void Set(const char* name, const char* value) {
    environment_.push_back(std::make_unique<ScopedEnvironment>(name, value));
  }
  void Run(bool inject_local_error, bool absent) {
    KVClient owner(members_, namespace_, owner_transport_.get(), 1);
    KVClient waiter(members_, namespace_, waiter_transport_.get(), 1);
    // This fixture alone uses its fresh, private timestamped namespace. Capture
    // the regular segment only after client creation; cleanup rechecks identity
    // and never unlinks a preexisting/symlink/replaced path. The initial
    // absence check is not a claim of atomic exclusive creation by NodeDedup.
    const std::string path = "/dev/shm" + shm_name_;
    ASSERT_EQ(::lstat(path.c_str(), &shm_identity_), 0);
    ASSERT_TRUE(S_ISREG(shm_identity_.st_mode));
    ASSERT_EQ(shm_identity_.st_uid, ::getuid());
    cleanup_shm_ = true;
    const std::string shared_key("shared\0key", 10);
    const std::string marker_key("marker\0key", 10);
    const std::string shared_value = Payload(11);
    const std::string marker_value = Payload(29);
    if (!absent) {
      ASSERT_TRUE(
          owner.Put(shared_key, shared_value.data(), shared_value.size()));
    }
    ASSERT_TRUE(
        owner.Put(marker_key, marker_value.data(), marker_value.size()));
    EXPECT_EQ(Counter(owner, "dfkv_client_dedup_fetches_total"), 0);
    EXPECT_EQ(Counter(waiter, "dfkv_client_dedup_fetches_total"), 0);
    if (inject_local_error) owner_transport_->InjectLocalIoError();
    std::future<ReadResult> owner_call;
    std::future<ReadResult> waiter_call;
    const ReleaseGates release{owner_transport_.get(), waiter_transport_.get()};
    owner_call = std::async(std::launch::async,
                            [&] { return Read(&owner, {shared_key}); });
    ASSERT_TRUE(owner_transport_->WaitUntilEntered());
    waiter_call = std::async(std::launch::async, [&] {
      return Read(&waiter, {marker_key, shared_key});
    });
    ASSERT_TRUE(waiter_transport_->WaitUntilEntered());
    owner_transport_->Release();
    ASSERT_EQ(owner_call.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    const ReadResult first = owner_call.get();
    waiter_transport_->Release();
    ASSERT_EQ(waiter_call.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    const ReadResult second = waiter_call.get();
    EXPECT_EQ(first.code, 0);
    EXPECT_EQ(second.code, 0);
    EXPECT_EQ(first.statuses,
              (std::vector<int>{inject_local_error ? DFKV_GET_IO_ERROR
                                : absent           ? DFKV_GET_NOT_FOUND
                                                   : DFKV_GET_OK}));
    EXPECT_EQ(second.statuses,
              (std::vector<int>{DFKV_GET_OK,
                                absent ? DFKV_GET_NOT_FOUND : DFKV_GET_OK}));
    EXPECT_EQ(first.lengths,
              (std::vector<uint64_t>{inject_local_error || absent ? 0u : 64u}));
    EXPECT_EQ(second.lengths, (std::vector<uint64_t>{64, absent ? 0u : 64u}));
    EXPECT_EQ(second.bytes[0], marker_value);
    if (!absent) {
      EXPECT_EQ(second.bytes[1], shared_value);
    }
    if (!inject_local_error && !absent) {
      EXPECT_EQ(first.bytes[0], shared_value);
    }
    EXPECT_EQ(owner_transport_->injected_keys(), inject_local_error ? 1 : 0);
    EXPECT_EQ(owner_transport_->delegated_keys(), inject_local_error ? 0 : 1);
    const bool fallback = inject_local_error || absent;
    EXPECT_EQ(waiter_transport_->delegated_keys(), fallback ? 2 : 1);
    EXPECT_EQ(Counter(waiter, "dfkv_client_dedup_wait_hits_total"),
              fallback ? 0 : 1);
    EXPECT_EQ(Counter(waiter, "dfkv_client_dedup_wait_timeouts_total"),
              fallback ? 1 : 0);
    EXPECT_EQ(Counter(owner, "dfkv_client_op_requests_total", true), 1);
    EXPECT_EQ(Counter(waiter, "dfkv_client_op_requests_total", true), 1);
    EXPECT_EQ(Counter(owner, "dfkv_client_op_keys_total", true), 1);
    EXPECT_EQ(Counter(waiter, "dfkv_client_op_keys_total", true), 2);
    EXPECT_EQ(Counter(owner, "dfkv_client_op_hits_total", true),
              inject_local_error || absent ? 0 : 1);
    EXPECT_EQ(Counter(waiter, "dfkv_client_op_hits_total", true),
              absent ? 1 : 2);
    EXPECT_EQ(Counter(waiter, "dfkv_client_op_bytes_total", true),
              absent ? 64 : 128);
    RecordProperty("expected_server_puts", absent ? 1 : 2);
    RecordProperty("expected_server_put_bytes", absent ? 64 : 128);
    RecordProperty("expected_server_pulls", absent ? 3 : 2);
    RecordProperty("expected_server_grants", absent ? 1 : 2);
    RecordProperty("expected_server_not_found", absent ? 2 : 0);
    RecordProperty("injected_local_owner_errors", inject_local_error ? 1 : 0);
  }

 private:
  std::vector<std::unique_ptr<ScopedEnvironment>> environment_;
  std::unique_ptr<KvNodeServer> server_;
  std::string directory_;
  std::string namespace_;
  std::string shm_name_;
  bool cleanup_shm_ = false;
  struct stat shm_identity_ {};
  std::vector<std::pair<std::string, std::string>> members_;
  std::unique_ptr<GatedTransport> owner_transport_;
  std::unique_ptr<GatedTransport> waiter_transport_;
};

TEST_F(CpuDedupRuntime, InFlightSuccessSharesBytesWithoutSecondRemoteRead) {
  Run(false, false);
}

TEST_F(CpuDedupRuntime, InjectedOwnerLocalErrorUsesWaitersOwnRemoteSuccess) {
  Run(true, false);
}

TEST_F(CpuDedupRuntime, GenuineAbsentOwnerAndWaiterRetainNotFound) {
  Run(false, true);
}

}  // namespace
}  // namespace dfkv
