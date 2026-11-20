// Exec-based CPU rendezvous tests: no child inherits a live transport or CUDA
// context. Pipes gate only scheduling; reads use the selected real transport.
// Define POSIX process types before headers that provide optional aliases.
#include <sys/types.h>

// POSIX process, polling, and environment interfaces require these C headers.
#include <dirent.h>
#include <fcntl.h>
#include <linux/prctl.h>
#include <signal.h>  // NOLINT(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <spawn.h>
#include <stdlib.h>  // NOLINT(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// Third-party test framework.
#include <gtest/gtest.h>

#include "cache/disk_cache_group.h"
#include "cache/kv_node_server.h"
#include "client/dfkv_c_api.h"
#include "client/key_map.h"
#include "client/kv_client.h"
#include "client/node_dedup.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/transport.h"
#include "transport/transport_factory.h"

extern char** environ;

namespace dfkv {
namespace {
constexpr int kCommandFd = 3;
constexpr int kEventFd = 4;
constexpr int kPipeBudgetMs = 5000;

bool Transfer(int fd, void* buffer, size_t size, bool writing) {
  char* bytes = static_cast<char*>(buffer);
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(kPipeBudgetMs);
  while (size != 0) {
    const int remaining =
        static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
                             deadline - std::chrono::steady_clock::now())
                             .count());
    if (remaining <= 0) return false;
    struct pollfd descriptor {
      fd, static_cast<short>(writing ? POLLOUT : POLLIN), 0
    };
    const int ready = ::poll(&descriptor, 1, remaining);
    if (ready < 0 && errno == EINTR) continue;
    if (ready <= 0 ||
        (descriptor.revents & (writing ? POLLOUT : POLLIN)) == 0) {
      return false;
    }
    const ssize_t count =
        writing ? ::write(fd, bytes, size) : ::read(fd, bytes, size);
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) return false;
    bytes += count;
    size -= static_cast<size_t>(count);
  }
  return true;
}

bool Send(int fd, char value) { return Transfer(fd, &value, 1, true); }
bool Expect(int fd, char wanted) {
  char value = 0;
  return Transfer(fd, &value, 1, false) && value == wanted;
}

std::string Payload(unsigned int seed) {
  std::string result(64, '\0');
  for (size_t i = 1; i < result.size(); ++i) {
    result[i] = static_cast<char>((seed + i * 13) & 255);
  }
  return result;
}
const std::string kSharedKey("shared\0key", 10);
const std::string kMarkerKey("marker\0key", 10);

uint64_t Counter(const KVClient& client, const std::string& name) {
  const std::string text = client.MetricsSnapshot();
  const std::string prefix = "\n" + name + " ";
  const size_t position = text.find(prefix);
  return position == std::string::npos
             ? UINT64_MAX
             : std::stoull(text.substr(position + prefix.size()));
}

// POD IPC has a fixed local ABI: parent and exec children are the same binary.
// It contains no pointers and is never persisted or accepted from other
// binaries.
struct ChildResult {
  int code = -1;
  int statuses[2] = {-999, -999};
  uint64_t lengths[2] = {999, 999};
  char bytes[2][64] = {};
  uint64_t delegated = 0;
  uint64_t injected = 0;
  uint64_t fetches = 0;
  uint64_t hits = 0;
  uint64_t wait_hits = 0;
  uint64_t wait_timeouts = 0;
};

class ProcessTransport final : public Transport {
 public:
  ProcessTransport(std::unique_ptr<Transport> inner, bool gate, bool inject)
      : inner_(std::move(inner)), gate_(gate), inject_(inject) {}
  bool pipelined() const override { return inner_->pipelined(); }
  Status Cache(const std::string& node, const BlockKey& key, const void* data,
               size_t size) override {
    return inner_->Cache(node, key, data, size);
  }
  Status Range(const std::string& node, const BlockKey& key, uint64_t offset,
               uint64_t length, std::string* out, uint64_t* size) override {
    return inner_->Range(node, key, offset, length, out, size);
  }
  Status Lookup(const std::string& node, const BlockKey& key,
                uint64_t* size) override {
    return inner_->Lookup(node, key, size);
  }
  Status Exist(const std::string& node, const BlockKey& key,
               bool* exists) override {
    return inner_->Exist(node, key, exists);
  }
  std::vector<Status> RangeInto(const std::string& node,
                                const std::vector<BlockKey>& keys,
                                const std::vector<RangeDst>& destinations,
                                std::vector<uint64_t>* lengths) override {
    if (gate_) {
      gate_ = false;
      if (!Send(kEventFd, 'G') || !Expect(kCommandFd, 'R')) {
        lengths->assign(keys.size(), 0);
        return std::vector<Status>(keys.size(), Status::kIOError);
      }
    }
    if (inject_) {
      injected_ += keys.size();
      lengths->assign(keys.size(), 0);
      return std::vector<Status>(keys.size(), Status::kIOError);
    }
    delegated_ += keys.size();
    return inner_->RangeInto(node, keys, destinations, lengths);
  }
  uint64_t delegated() const { return delegated_; }
  uint64_t injected() const { return injected_; }

 private:
  std::unique_ptr<Transport> inner_;
  bool gate_;
  bool inject_;
  uint64_t delegated_ = 0;
  uint64_t injected_ = 0;
};

ChildResult Read(KVClient* client, const std::vector<std::string>& keys) {
  ChildResult result;
  const void* key_pointers[2] = {};
  uint64_t key_lengths[2] = {};
  void* destinations[2] = {result.bytes[0], result.bytes[1]};
  const uint64_t capacities[2] = {64, 64};
  for (size_t i = 0; i < keys.size(); ++i) {
    key_pointers[i] = keys[i].data();
    key_lengths[i] = keys[i].size();
  }
  result.code = dfkv_batch_get_auto_deduplicated_status(
      client, key_pointers, key_lengths, destinations, capacities,
      static_cast<int>(keys.size()), result.statuses, result.lengths);
  return result;
}

// The parent's tiny TCP server may have non-CLOEXEC descriptors. Exec drops
// its memory/context, and this allowlist drops unrelated inherited FDs before
// any client is created. Close only after enumeration, never its directory FD.
bool CloseInheritedDescriptors() {
  DIR* directory = ::opendir("/proc/self/fd");
  if (directory == nullptr) return false;
  const int iterator_fd = ::dirfd(directory);
  std::vector<int> descriptors;
  errno = 0;
  while (struct dirent* entry = ::readdir(directory)) {
    char* end = nullptr;
    const long descriptor = std::strtol(entry->d_name, &end, 10);
    if (*end == '\0' && descriptor >= 5 && descriptor != iterator_fd) {
      descriptors.push_back(static_cast<int>(descriptor));
    }
    errno = 0;
  }
  const bool complete = errno == 0;
  if (::closedir(directory) != 0 || !complete) return false;
  for (const int descriptor : descriptors) {
    if (::close(descriptor) != 0) return false;
  }
  return true;
}

int ChildMain(const std::string& role, const std::string& node,
              const std::string& address, const std::string& key_namespace) {
  std::string mode;
  std::unique_ptr<Transport> inner = MakeClientTransport(&mode);
  const char* rdma = std::getenv("DFKV_RDMA");
  const std::string expected = rdma != nullptr && std::string(rdma) == "1"
                                   ? "rdma"
                                   : "tcp(rdma-not-requested)";
  if (inner == nullptr || mode != expected) return 10;
  const bool owner = role == "owner" || role == "error";
  ProcessTransport transport(std::move(inner), role != "recovery",
                             role == "error");
  KVClient client({{node, address}}, key_namespace, &transport, 1);
  if (owner) {
    const std::string shared = Payload(11);
    const std::string marker = Payload(29);
    if (!client.Put(kSharedKey, shared.data(), shared.size()) ||
        !client.Put(kMarkerKey, marker.data(), marker.size())) {
      return 11;
    }
  }
  if (!Send(kEventFd, 'I') || !Expect(kCommandFd, 'S')) return 12;
  ChildResult result =
      Read(&client, role == "waiter"
                        ? std::vector<std::string>{kMarkerKey, kSharedKey}
                        : std::vector<std::string>{kSharedKey});
  if (role == "recovery") {
    // The first claim takes over the dead FETCHING generation and publishes;
    // the next read must be READY, not a second transport operation.
    const ChildResult ready = Read(&client, {kSharedKey});
    if (ready.code != 0 || ready.statuses[0] != DFKV_GET_OK ||
        ready.lengths[0] != 64 ||
        std::memcmp(ready.bytes[0], Payload(11).data(), 64) != 0) {
      return 13;
    }
  }
  result.delegated = transport.delegated();
  result.injected = transport.injected();
  result.fetches = Counter(client, "dfkv_client_dedup_fetches_total");
  result.hits = Counter(client, "dfkv_client_dedup_hits_total");
  result.wait_hits = Counter(client, "dfkv_client_dedup_wait_hits_total");
  result.wait_timeouts =
      Counter(client, "dfkv_client_dedup_wait_timeouts_total");
  if (!Send(kEventFd, 'D') ||
      !Transfer(kEventFd, &result, sizeof(result), true))
    return 14;
  return 0;
}

class ScopedEnvironment {
 public:
  ScopedEnvironment(const char* name, const char* value) : name_(name) {
    const char* previous = std::getenv(name);
    if (previous != nullptr) {
      previous_ = previous;
      had_value_ = true;
    }
    EXPECT_EQ(::setenv(name, value, 1), 0);
  }
  ~ScopedEnvironment() {
    if (had_value_)
      ::setenv(name_.c_str(), previous_.c_str(), 1);
    else
      ::unsetenv(name_.c_str());
  }
  ScopedEnvironment(const ScopedEnvironment&) = delete;
  ScopedEnvironment& operator=(const ScopedEnvironment&) = delete;

 private:
  std::string name_;
  std::string previous_;
  bool had_value_ = false;
};

// Owns one exact child PID until reaped. Never sends a signal after waitpid
// succeeds; POSIX keeps an unreaped child PID from being recycled.
class Child {
 public:
  ~Child() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      while (::waitpid(pid_, nullptr, 0) < 0 && errno == EINTR) {
      }
    }
    if (command_ >= 0) ::close(command_);
    if (events_ >= 0) ::close(events_);
  }
  Child() = default;
  Child(const Child&) = delete;
  Child& operator=(const Child&) = delete;
  bool Start(const std::string& role, const std::string& node,
             const std::string& address, const std::string& key_namespace) {
    int commands[2] = {-1, -1};
    int events[2] = {-1, -1};
    if (::pipe2(commands, O_CLOEXEC) != 0) return false;
    if (::pipe2(events, O_CLOEXEC) != 0) {
      ::close(commands[0]);
      ::close(commands[1]);
      return false;
    }
    // Move all source descriptors above fixed child FDs before dup2 actions.
    int sources[4] = {-1, -1, -1, -1};
    const int originals[4] = {commands[0], commands[1], events[0], events[1]};
    bool valid = true;
    for (size_t i = 0; i < 4; ++i) {
      sources[i] = ::fcntl(originals[i], F_DUPFD_CLOEXEC, 10);
      if (sources[i] < 0) valid = false;
      ::close(originals[i]);
    }
    posix_spawn_file_actions_t actions;
    const bool initialized = ::posix_spawn_file_actions_init(&actions) == 0;
    valid = valid && initialized;
    if (valid) {
      valid = ::posix_spawn_file_actions_adddup2(&actions, sources[0],
                                                 kCommandFd) == 0 &&
              ::posix_spawn_file_actions_adddup2(&actions, sources[3],
                                                 kEventFd) == 0;
    }
    std::vector<std::string> arguments = {
        "/proc/self/exe", "--cpu-process-child",     role, node, address,
        key_namespace,    std::to_string(::getpid())};
    std::vector<char*> pointers;
    pointers.reserve(arguments.size() + 1);
    for (std::string& argument : arguments) pointers.push_back(argument.data());
    pointers.push_back(nullptr);
    if (valid)
      valid = ::posix_spawn(&pid_, "/proc/self/exe", &actions, nullptr,
                            pointers.data(), environ) == 0;
    if (initialized) ::posix_spawn_file_actions_destroy(&actions);
    ::close(sources[0]);
    ::close(sources[3]);
    if (!valid) {
      if (sources[1] >= 0) ::close(sources[1]);
      if (sources[2] >= 0) ::close(sources[2]);
      pid_ = -1;
      return false;
    }
    command_ = sources[1];
    events_ = sources[2];
    return true;
  }
  bool Event(char value) const { return Expect(events_, value); }
  bool Command(char value) const { return Send(command_, value); }
  bool Result(ChildResult* result) const {
    return Event('D') && Transfer(events_, result, sizeof(*result), false);
  }
  bool Reap(bool killed = false) {
    if (killed && ::kill(pid_, SIGKILL) != 0) return false;
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t observed = ::waitpid(pid_, &status, WNOHANG);
      if (observed == pid_) {
        pid_ = -1;
        return killed ? WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL
                      : WIFEXITED(status) && WEXITSTATUS(status) == 0;
      }
      if (observed < 0 && errno != EINTR) {
        if (errno == ECHILD) pid_ = -1;
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
  }
  pid_t pid() const { return pid_; }

 private:
  pid_t pid_ = -1;
  int command_ = -1;
  int events_ = -1;
};

class CpuDedupProcessRuntime : public ::testing::Test {
 public:
  ~CpuDedupProcessRuntime() override = default;

 protected:
  void Set(const char* name, const char* value) {
    environment_.push_back(std::make_unique<ScopedEnvironment>(name, value));
  }
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
      std::smatch match;
      const std::string input(external);
      ASSERT_TRUE(std::regex_match(
          input, match,
          std::regex("([A-Za-z0-9_-]+)=(127\\.0\\.0\\.1:([0-9]{1,5}))")));
      ASSERT_GT(std::stoi(match[3]), 0);
      ASSERT_LE(std::stoi(match[3]), 65535);
      node_ = match[1];
      address_ = match[2];
    } else {
      Set("DFKV_RDMA", "0");
    }
    Set("DFKV_CLIENT_NODE_DEDUP", "1");
    Set("DFKV_NODE_DEDUP_ARENA_MB", "1");
    Set("DFKV_NODE_DEDUP_SLOTS", "1024");
    Set("DFKV_NODE_DEDUP_WAIT_MS", "150");
    Set("DFKV_NODE_DEDUP_TAKEOVER_MS", "5000");
    Set("DFKV_NODE_DEDUP_TTL_MS", "5000");
    Set("DFKV_GET_MISS_RETRIES", "0");
    Set("DFKV_RDMA_OP_TIMEOUT_MS", "2000");
    Set("DFKV_RDMA_BATCH_OP_TIMEOUT_MS", "2000");
    Set("DFKV_RAM_TIER", "0");
    Set("DFKV_PUT_ACK_MODE", "disk");
    if (external == nullptr) {
      std::string pattern =
          (std::filesystem::temp_directory_path() / "dfkv-cpu-process-XXXXXX")
              .string();
      ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
      directory_ = pattern;
      DiskCacheGroup::Options options;
      options.cache_dirs = {directory_};
      options.capacity_bytes = 1u << 20;
      options.engine = "file";
      server_ = std::make_unique<KvNodeServer>(options);
      ASSERT_EQ(server_->Start(0), Status::kOk);
      address_ = "127.0.0.1:" + std::to_string(server_->port());
    }
    namespace_ =
        "cpu-process/" + tag + "/" +
        ::testing::UnitTest::GetInstance()->current_test_info()->name() + "/" +
        std::to_string(::getpid()) + "/" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    shm_name_ = NodeDedup::EnvSegmentName(ToBlockKey(namespace_, "").digest_hi);
    ASSERT_EQ(std::filesystem::symlink_status("/dev/shm" + shm_name_).type(),
              std::filesystem::file_type::not_found);
    const char* rdma = std::getenv("DFKV_RDMA");
    RecordProperty("inner_transport",
                   rdma != nullptr && std::string(rdma) == "1"
                       ? "rdma"
                       : "tcp(rdma-not-requested)");
    RecordProperty("key_namespace", namespace_);
    RecordProperty("shm_name", shm_name_);
  }
  void TearDown() override {
    // Run() locals reap all children before this fixture removes shared memory.
    server_.reset();
    if (cleanup_shm_) {
      struct stat current {};
      const int observed = ::lstat(("/dev/shm" + shm_name_).c_str(), &current);
      EXPECT_EQ(observed, 0);
      if (observed == 0 && S_ISREG(current.st_mode) &&
          current.st_uid == ::getuid() && current.st_dev == identity_.st_dev &&
          current.st_ino == identity_.st_ino) {
        EXPECT_EQ(::shm_unlink(shm_name_.c_str()), 0);
        RecordProperty("shm_cleanup", "verified_dev_inode_unlinked");
      } else {
        ADD_FAILURE() << "Private shared-memory identity changed; retain it";
      }
    }
    if (!directory_.empty()) std::filesystem::remove_all(directory_);
  }
  void Run(bool local_error, bool killed) {
    Child owner;
    Child waiter;
    ASSERT_TRUE(owner.Start(local_error ? "error" : "owner", node_, address_,
                            namespace_));
    ASSERT_TRUE(owner.Event('I'));
    ASSERT_EQ(::lstat(("/dev/shm" + shm_name_).c_str(), &identity_), 0);
    ASSERT_TRUE(S_ISREG(identity_.st_mode));
    ASSERT_EQ(identity_.st_uid, ::getuid());
    cleanup_shm_ = true;
    ASSERT_TRUE(owner.Command('S'));
    ASSERT_TRUE(owner.Event('G'));
    const std::chrono::steady_clock::time_point claimed =
        std::chrono::steady_clock::now();
    ASSERT_TRUE(waiter.Start("waiter", node_, address_, namespace_));
    ASSERT_NE(owner.pid(), waiter.pid());
    ASSERT_NE(owner.pid(), ::getpid());
    ASSERT_NE(waiter.pid(), ::getpid());
    RecordProperty("parent_pid", static_cast<int>(::getpid()));
    RecordProperty("owner_pid", static_cast<int>(owner.pid()));
    RecordProperty("waiter_pid", static_cast<int>(waiter.pid()));
    ASSERT_TRUE(waiter.Event('I'));
    ASSERT_TRUE(waiter.Command('S'));
    // Waiter marker RangeInto is reached only after the entire batch claim
    // pass, proving shared_key saw the owner's in-flight generation.
    ASSERT_TRUE(waiter.Event('G'));
    if (killed) {
      ASSERT_TRUE(owner.Reap(true));
    } else {
      ASSERT_TRUE(owner.Command('R'));
      ChildResult first;
      ASSERT_TRUE(owner.Result(&first));
      ASSERT_TRUE(owner.Reap());
      EXPECT_EQ(first.code, 0);
      EXPECT_EQ(first.statuses[0],
                local_error ? DFKV_GET_IO_ERROR : DFKV_GET_OK);
      EXPECT_EQ(first.lengths[0], local_error ? 0 : 64);
      EXPECT_EQ(first.injected, local_error ? 1 : 0);
      EXPECT_EQ(first.delegated, local_error ? 0 : 1);
      EXPECT_EQ(first.fetches, 1);
      if (!local_error) {
        EXPECT_EQ(std::string(first.bytes[0], 64), Payload(11));
      }
    }
    ASSERT_TRUE(waiter.Command('R'));
    ChildResult second;
    ASSERT_TRUE(waiter.Result(&second));
    ASSERT_TRUE(waiter.Reap());
    EXPECT_EQ(second.code, 0);
    for (size_t i = 0; i < 2; ++i) {
      EXPECT_EQ(second.statuses[i], DFKV_GET_OK);
      EXPECT_EQ(second.lengths[i], 64);
    }
    EXPECT_EQ(std::string(second.bytes[0], 64), Payload(29));
    EXPECT_EQ(std::string(second.bytes[1], 64), Payload(11));
    EXPECT_EQ(second.fetches, 1);
    EXPECT_EQ(second.delegated, local_error || killed ? 2 : 1);
    EXPECT_EQ(second.wait_hits, local_error || killed ? 0 : 1);
    EXPECT_EQ(second.wait_timeouts, local_error || killed ? 1 : 0);
    if (killed) {
      // No sleep decides process ordering. Only this explicit elapsed-age wait
      // makes the existing 5000 ms takeover threshold eligible for a NEW claim.
      std::this_thread::sleep_until(claimed + std::chrono::milliseconds(5100));
      Child recovery;
      ASSERT_TRUE(recovery.Start("recovery", node_, address_, namespace_));
      RecordProperty("recovery_pid", static_cast<int>(recovery.pid()));
      ASSERT_TRUE(recovery.Event('I'));
      ASSERT_TRUE(recovery.Command('S'));
      ChildResult third;
      ASSERT_TRUE(recovery.Result(&third));
      ASSERT_TRUE(recovery.Reap());
      EXPECT_EQ(third.code, 0);
      EXPECT_EQ(third.statuses[0], DFKV_GET_OK);
      EXPECT_EQ(third.lengths[0], 64);
      EXPECT_EQ(std::string(third.bytes[0], 64), Payload(11));
      EXPECT_EQ(third.fetches, 1);
      EXPECT_EQ(third.hits, 1);
      EXPECT_EQ(third.delegated, 1);
      EXPECT_EQ(third.wait_timeouts, 0);
    }
    RecordProperty("expected_server_puts", 2);
    RecordProperty("expected_server_put_bytes", 128);
    RecordProperty("expected_server_pulls", killed ? 3 : 2);
    RecordProperty("expected_server_grants", killed ? 3 : 2);
    RecordProperty("expected_server_not_found", 0);
    RecordProperty("injected_local_owner_errors", local_error ? 1 : 0);
    RecordProperty("killed_owner_count", killed ? 1 : 0);
    RecordProperty("owned_children_reaped", killed ? 3 : 2);
    RecordProperty("child_launch", "posix_spawn_exec");
    RecordProperty("claim_schedule", "owner_then_waiter_marker");
  }

 private:
  std::vector<std::unique_ptr<ScopedEnvironment>> environment_;
  std::unique_ptr<KvNodeServer> server_;
  std::string directory_;
  std::string node_ = "n";
  std::string address_;
  std::string namespace_;
  std::string shm_name_;
  struct stat identity_ {};
  bool cleanup_shm_ = false;
};

TEST_F(CpuDedupProcessRuntime, CrossProcessWaiterSharesPublishedBytes) {
  Run(false, false);
}
TEST_F(CpuDedupProcessRuntime, OwnerLocalErrorFallsBackAcrossProcesses) {
  Run(true, false);
}
TEST_F(CpuDedupProcessRuntime, KilledOwnerFallsBackThenLaterClaimTakesOver) {
  Run(false, true);
}
}  // namespace
}  // namespace dfkv

int main(int argc, char** argv) {
  if (argc == 7 && std::string(argv[1]) == "--cpu-process-child") {
    // Configure before creating any client/context. The captured parent PID
    // also detects death between posix_spawn/exec and this prctl call.
    const pid_t expected_parent = static_cast<pid_t>(std::stol(argv[6]));
    if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 ||
        ::getppid() != expected_parent)
      return 15;
    // SIGPIPE must become an ordinary owned-child failure, not terminate the
    // parent test when a failed peer closes its scheduling pipe early.
    if (::signal(SIGPIPE, SIG_IGN) == SIG_ERR) return 16;
    if (!dfkv::CloseInheritedDescriptors()) return 17;
    return dfkv::ChildMain(argv[2], argv[3], argv[4], argv[5]);
  }
  if (::signal(SIGPIPE, SIG_IGN) == SIG_ERR) return 16;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
