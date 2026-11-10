#include <gtest/gtest.h>
// POSIX setenv/unsetenv require their direct C header, not C++ std functions.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "cache/disk_cache_group.h"
#include "cache/disk_slab_store.h"
#include "cache/kv_node_server.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "read_coalescer_test_peer.h"
#include "transport/wire.h"

namespace dfkv {
class KvNodeServerTestPeer {
 public:
  static DiskSlabStore::SyncPayloadReadCounters SyncPayloadReads(
      const KvNodeServer& server) {
    return server.group_.SlabStats().sync_payload_reads;
  }
  static bool WaitForCopyFollower(KvNodeServer& server) {
    return ReadCoalescerTestPeer::WaitForCopyWaiter(server.read_coalescer_);
  }
  static size_t CopiedFollowers(const KvNodeServer& server) {
    return server.read_coalescer_.coalesced();
  }
  static bool WaitForPublishedFollower(KvNodeServer& server) {
    return ReadCoalescerTestPeer::WaitForPublishedWaiter(
        server.read_coalescer_);
  }
};
}  // namespace dfkv

namespace fs = std::filesystem;
using namespace dfkv;  // NOLINT

namespace {

class AlignedBuffer {
 public:
  explicit AlignedBuffer(size_t size) : size_(size) {
    void* p = nullptr;
    EXPECT_EQ(::posix_memalign(&p, 4096, size), 0);
    data_.reset(static_cast<char*>(p));
  }
  char* data() const { return data_.get(); }
  size_t size() const { return size_; }

 private:
  struct Free {
    void operator()(char* p) const { std::free(p); }
  };
  std::unique_ptr<char, Free> data_;
  size_t size_;
};

BlockKey Key(uint64_t n) { return BlockKey{n, n + 1, 7}; }

Status Put(KvNodeServer* server, const BlockKey& key,
           const std::string& value) {
  std::string ignored;
  return server->ProcessRequestForKey(
      static_cast<uint8_t>(WireOp::kCache), key, 0, 0, value.data(),
      value.size(), &ignored);
}

TEST(PreparedRead, DiskSourceSurvivesMovesAndTerminalActions) {
  ::setenv("DFKV_READ_COALESCE", "0", 1);
  ::unsetenv("DFKV_RAM_TIER");
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_prepared_read_source_moves";
  fs::remove_all(dir);
  {
    KvNodeServer server(dir.string(), 1ULL << 30);
    ASSERT_TRUE(server.Healthy());
    const std::string value(4096, 's');
    ASSERT_EQ(Put(&server, Key(93), value), Status::kOk);
    const AlignedBuffer staging(8192);
    PreparedRead empty;
    EXPECT_EQ(empty.source(), PreparedRead::Source::kUnclassified);
    EXPECT_FALSE(empty.owns_cleanup());
    EXPECT_EQ(empty.async_read_counters(), nullptr);
    const PreparedRead missing = server.PrepareReadForKey(
        Key(94), 0, value.size(), staging.data(), staging.size());
    EXPECT_EQ(missing.status(), Status::kNotFound);
    EXPECT_EQ(missing.source(), PreparedRead::Source::kUnclassified);

    PreparedRead original = server.PrepareReadForKey(
        Key(93), 0, value.size(), staging.data(), staging.size());
    ASSERT_EQ(original.status(), Status::kOk);
    EXPECT_EQ(original.source(), PreparedRead::Source::kDiskStaging);
    PreparedRead moved(std::move(original));
    EXPECT_NE(moved.async_read_counters(), nullptr);
    // MoveFrom resets source_; this const getter verifies that post-move
    // contract.
    // NOLINTNEXTLINE(bugprone-use-after-move,hicpp-invalid-access-moved,clang-analyzer-cplusplus.Move)
    EXPECT_EQ(original.source(), PreparedRead::Source::kUnclassified);
    EXPECT_FALSE(original.owns_cleanup());
    EXPECT_EQ(moved.source(), PreparedRead::Source::kDiskStaging);
    const ssize_t got =
        ::pread(moved.fd(), moved.staging(), moved.aligned_len(),
                static_cast<off_t>(moved.aligned_off()));
    ASSERT_GE(got, 0);
    ASSERT_GE(static_cast<size_t>(got), moved.head() + moved.payload_len());
    ASSERT_EQ(moved.FinishStorage(Status::kOk, moved.payload_len()),
              Status::kOk);
    EXPECT_EQ(moved.async_read_counters(), nullptr);
    EXPECT_EQ(moved.source(), PreparedRead::Source::kDiskStaging);
    empty = std::move(moved);
    // MoveFrom resets source_; this const getter verifies that post-move
    // contract.
    // NOLINTNEXTLINE(bugprone-use-after-move,hicpp-invalid-access-moved,clang-analyzer-cplusplus.Move)
    EXPECT_EQ(moved.source(), PreparedRead::Source::kUnclassified);
    EXPECT_FALSE(moved.owns_cleanup());
    EXPECT_EQ(empty.source(), PreparedRead::Source::kDiskStaging);
    EXPECT_EQ(std::string(empty.data(), empty.payload_len()), value);
    EXPECT_TRUE(empty.Commit(Status::kOk, empty.payload_len(), 0.0));
    EXPECT_EQ(empty.source(), PreparedRead::Source::kDiskStaging);
    EXPECT_FALSE(empty.Commit(Status::kOk, empty.payload_len(), 0.0));
    empty = PreparedRead{};
    EXPECT_EQ(empty.source(), PreparedRead::Source::kUnclassified);

    PreparedRead failed = server.PrepareReadForKey(
        Key(93), 0, value.size(), staging.data(), staging.size());
    ASSERT_EQ(failed.status(), Status::kOk);
    EXPECT_EQ(failed.FinishStorage(Status::kIOError, 0), Status::kIOError);
    EXPECT_EQ(failed.source(), PreparedRead::Source::kDiskStaging);
    EXPECT_FALSE(failed.source_identity().valid());
    EXPECT_TRUE(failed.Abort());
    // Preparation provenance remains diagnostic, not a successful grant.
    EXPECT_EQ(failed.source(), PreparedRead::Source::kDiskStaging);
  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
}

TEST(PreparedRead, RangeDirectSourceResetsOnEmptyAndFailure) {
  ::setenv("DFKV_READ_COALESCE", "0", 1);
  ::unsetenv("DFKV_RAM_TIER");
  const fs::path dir = fs::temp_directory_path() / "dfkv_direct_source_reset";
  fs::remove_all(dir);
  {
    DiskCacheGroup::Options options;
    options.cache_dirs = {dir.string()};
    options.capacity_bytes = 1ULL << 30;
    options.engine = "slab";
    options.eviction_policy = "legacy";
    KvNodeServer server(options);
    ASSERT_TRUE(server.Healthy());
    const std::string value(4096, 'd');
    ASSERT_EQ(Put(&server, Key(95), value), Status::kOk);
    const AlignedBuffer buffer(8192);
    const char* data = nullptr;
    size_t bytes = 0;
    size_t value_len = 0;
    PreparedRead::Source source = PreparedRead::Source::kRamHit;
    ASSERT_EQ(server.RangeDirectForKey(Key(95), 0, value.size(), buffer.data(),
                                       buffer.size(), &data, &bytes, &value_len,
                                       nullptr, &source),
              Status::kOk);
    EXPECT_EQ(source, PreparedRead::Source::kSynchronousDisk);
    EXPECT_EQ(std::string(data, bytes), value);
    EXPECT_EQ(value_len, value.size());
    // A reused output must not retain the prior nonempty source.
    ASSERT_EQ(server.RangeDirectForKey(Key(95), value.size(), 1, buffer.data(),
                                       buffer.size(), &data, &bytes, &value_len,
                                       nullptr, &source),
              Status::kOk);
    EXPECT_EQ(bytes, 0U);
    EXPECT_EQ(source, PreparedRead::Source::kUnclassified);
    source = PreparedRead::Source::kSynchronousRamHit;
    EXPECT_EQ(
        server.RangeDirectForKey(Key(96), 0, 1, buffer.data(), buffer.size(),
                                 &data, &bytes, &value_len, nullptr, &source),
        Status::kNotFound);
    EXPECT_EQ(source, PreparedRead::Source::kUnclassified);
    source = PreparedRead::Source::kSynchronousCopyFollower;
    EXPECT_EQ(server.RangeDirectForKey(Key(95), value.size() + 1, 1,
                                       buffer.data(), buffer.size(), &data,
                                       &bytes, &value_len, nullptr, &source),
              Status::kInvalid);
    EXPECT_EQ(source, PreparedRead::Source::kUnclassified);
    const DiskSlabStore::SyncPayloadReadCounters io =
        KvNodeServerTestPeer::SyncPayloadReads(server);
    EXPECT_EQ(io.syscalls, 1U);
    EXPECT_EQ(io.bytes, value.size());
    EXPECT_EQ(io.errors, 0U);
    EXPECT_EQ(io.short_reads, 0U);
  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
}

TEST(PreparedRead, RangeDirectCoalescingReportsActualRole) {
  ::setenv("DFKV_READ_COALESCE", "1", 1);
  ::setenv("DFKV_READ_COALESCE_TIMEOUT_MS", "500", 1);
  ::unsetenv("DFKV_RAM_TIER");
  const fs::path dir = fs::temp_directory_path() / "dfkv_direct_source_roles";
  fs::remove_all(dir);
  {
    DiskCacheGroup::Options options;
    options.cache_dirs = {dir.string()};
    options.capacity_bytes = 1ULL << 30;
    options.engine = "slab";
    options.eviction_policy = "legacy";
    KvNodeServer server(options);
    ASSERT_TRUE(server.Healthy());
    const std::string value(4096, 'c');
    const BlockKey key = Key(97);
    ASSERT_EQ(Put(&server, key, value), Status::kOk);
    const AlignedBuffer leader_buffer(8192);
    const AlignedBuffer buffer(8192);
    const char* data = nullptr;
    size_t bytes = 0;
    size_t value_len = 0;
    PreparedRead::Source source = PreparedRead::Source::kUnclassified;
    ASSERT_EQ(server.RangeDirectForKey(key, 0, value.size(), buffer.data(),
                                       buffer.size(), &data, &bytes, &value_len,
                                       nullptr, &source),
              Status::kOk);
    EXPECT_EQ(source, PreparedRead::Source::kSynchronousCopyLeader);
    EXPECT_EQ(std::string(data, bytes), value);
    uint64_t expected_syscalls = 1;
    EXPECT_EQ(KvNodeServerTestPeer::SyncPayloadReads(server).syscalls,
              expected_syscalls);
    for (const bool fail_leader : {false, true}) {
      SCOPED_TRACE(fail_leader);
      PreparedRead leader = server.PrepareReadForKey(
          key, 0, value.size(), leader_buffer.data(), leader_buffer.size());
      ASSERT_EQ(leader.status(), Status::kOk);
      ASSERT_TRUE(leader.needs_io());
      // Same-thread recursion must independently read rather than deadlock or
      // report a follower merely because this request did not become leader.
      ASSERT_EQ(server.RangeDirectForKey(key, 0, value.size(), buffer.data(),
                                         buffer.size(), &data, &bytes,
                                         &value_len, nullptr, &source),
                Status::kOk);
      EXPECT_EQ(source, PreparedRead::Source::kSynchronousDisk);
      EXPECT_EQ(std::string(data, bytes), value);
      ++expected_syscalls;  // The same-thread fallback really reads storage.
      EXPECT_EQ(KvNodeServerTestPeer::SyncPayloadReads(server).syscalls,
                expected_syscalls);
      const size_t copied_before =
          KvNodeServerTestPeer::CopiedFollowers(server);
      Status follower_status = Status::kInvalid;
      std::thread follower([&] {
        follower_status = server.RangeDirectForKey(
            key, 0, value.size(), buffer.data(), buffer.size(), &data, &bytes,
            &value_len, nullptr, &source);
      });
      const bool waited = KvNodeServerTestPeer::WaitForCopyFollower(server);
      if (fail_leader) {
        EXPECT_TRUE(leader.Abort());
      } else {
        const ssize_t got =
            ::pread(leader.fd(), leader.staging(), leader.aligned_len(),
                    static_cast<off_t>(leader.aligned_off()));
        const bool io_ok = got >= 0 && static_cast<size_t>(got) >=
                                           leader.head() + leader.payload_len();
        EXPECT_TRUE(io_ok);
        EXPECT_TRUE(leader.Commit(io_ok ? Status::kOk : Status::kIOError,
                                  io_ok ? leader.payload_len() : 0, 0.0));
      }
      // Join before fatal assertions so failed handshakes cannot terminate
      // the test process with a live std::thread.
      follower.join();
      ASSERT_TRUE(waited);
      ASSERT_EQ(follower_status, Status::kOk);
      EXPECT_EQ(std::string(data, bytes), value);
      EXPECT_EQ(source, fail_leader
                            ? PreparedRead::Source::kSynchronousDisk
                            : PreparedRead::Source::kSynchronousCopyFollower);
      EXPECT_EQ(KvNodeServerTestPeer::CopiedFollowers(server),
                copied_before + (fail_leader ? 0 : 1));
      // The external prepared pread is excluded. A shared copy adds no Store
      // syscall; an aborted-flight fallback performs its own synchronous read.
      if (fail_leader) ++expected_syscalls;
      const DiskSlabStore::SyncPayloadReadCounters io =
          KvNodeServerTestPeer::SyncPayloadReads(server);
      EXPECT_EQ(io.syscalls, expected_syscalls);
      EXPECT_EQ(io.bytes, expected_syscalls * value.size());
      EXPECT_EQ(io.errors, 0U);
      EXPECT_EQ(io.short_reads, 0U);
    }
  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
  ::unsetenv("DFKV_READ_COALESCE_TIMEOUT_MS");
}

TEST(PreparedRead, CompletionAndFailureAreSingleTerminalActions) {
  ::setenv("DFKV_READ_COALESCE", "1", 1);
  ::setenv("DFKV_READ_COALESCE_TIMEOUT_MS", "25", 1);
  ::unsetenv("DFKV_RAM_TIER");
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_prepared_read_completion";
  fs::remove_all(dir);
  fs::create_directories(dir);
  {
  KvNodeServer server(dir.string(), 1ull << 30);
  ASSERT_TRUE(server.Healthy());

  const std::string value(8192, 'p');
  ASSERT_EQ(Put(&server, Key(1), value), Status::kOk);
  AlignedBuffer staging(1 << 20);

  PreparedRead read =
      server.PrepareReadForKey(Key(1), 0, value.size(), staging.data(),
                               staging.size());
  ASSERT_EQ(read.status(), Status::kOk);
  ASSERT_TRUE(read.owns_cleanup());
  ASSERT_TRUE(read.needs_io());
  const ssize_t got = ::pread(read.fd(), read.staging(), read.aligned_len(),
                              static_cast<off_t>(read.aligned_off()));
  ASSERT_GE(got, 0);
  ASSERT_GE(static_cast<size_t>(got), read.head() + read.payload_len());
  EXPECT_EQ(std::memcmp(read.data(), value.data(), value.size()), 0);
  EXPECT_TRUE(read.Commit(Status::kOk, read.payload_len(), 0.001));
  EXPECT_FALSE(read.Commit(Status::kOk, read.payload_len(), 0.001));
  EXPECT_FALSE(read.Abort());

  PreparedRead failed =
      server.PrepareReadForKey(Key(1), 0, value.size(), staging.data(),
                               staging.size());
  ASSERT_EQ(failed.status(), Status::kOk);
  EXPECT_TRUE(failed.Commit(Status::kIOError, 0, 0.001));
  EXPECT_EQ(failed.status(), Status::kIOError);
  EXPECT_FALSE(failed.source_identity().valid());
  EXPECT_FALSE(failed.Abort());

  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
  ::unsetenv("DFKV_READ_COALESCE_TIMEOUT_MS");
}

TEST(PreparedRead, FallbackAbortAndDestructorReleaseFlight) {
  ::setenv("DFKV_READ_COALESCE", "1", 1);
  ::unsetenv("DFKV_RAM_TIER");
  ::setenv("DFKV_READ_COALESCE_TIMEOUT_MS", "25", 1);
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_prepared_read_abort";
  fs::remove_all(dir);
  fs::create_directories(dir);
  {
    DiskCacheGroup::Options options;
    options.cache_dirs = {dir.string()};
    options.capacity_bytes = 1ULL << 30;
    options.engine = "slab";
    options.eviction_policy = "legacy";
    KvNodeServer server(options);
    ASSERT_TRUE(server.Healthy());

    const std::string value(4096, 'a');
    ASSERT_EQ(Put(&server, Key(2), value), Status::kOk);
    const AlignedBuffer first_staging(1 << 20);
    AlignedBuffer second_staging(1 << 20);

    PreparedRead leader = server.PrepareReadForKey(
        Key(2), 0, value.size(), first_staging.data(), first_staging.size());
    ASSERT_EQ(leader.status(), Status::kOk);
    const PreparedRead fallback = server.PrepareReadForKey(
        Key(2), 0, value.size(), second_staging.data(), second_staging.size());
    EXPECT_EQ(fallback.status(), Status::kInvalid);
    EXPECT_FALSE(fallback.owns_cleanup());

    // A follower times out defensively while the RAII leader remains alive,
    // then performs its own synchronous read without stealing cleanup
    // ownership.
    Status follower_status = Status::kIOError;
    PreparedRead::Source follower_source = PreparedRead::Source::kUnclassified;
    std::string follower_value;
    std::thread follower([&] {
      const char* data = nullptr;
      size_t len = 0;
      size_t value_len = 0;
      follower_status = server.RangeDirectForKey(
          Key(2), 0, value.size(), second_staging.data(), second_staging.size(),
          &data, &len, &value_len, nullptr, &follower_source);
      if (follower_status == Status::kOk && data != nullptr)
        follower_value.assign(data, len);
    });
    follower.join();
    EXPECT_EQ(follower_status, Status::kOk);
    EXPECT_EQ(follower_source, PreparedRead::Source::kSynchronousDisk);
    EXPECT_EQ(follower_value, value);
    EXPECT_EQ(KvNodeServerTestPeer::SyncPayloadReads(server).syscalls, 1U);
    EXPECT_EQ(KvNodeServerTestPeer::SyncPayloadReads(server).bytes,
              value.size());
    EXPECT_TRUE(leader.Abort());  // connection-abort path
    EXPECT_FALSE(leader.Abort());

    {
      const PreparedRead abandoned = server.PrepareReadForKey(
          Key(2), 0, value.size(), first_staging.data(), first_staging.size());
      ASSERT_EQ(abandoned.status(), Status::kOk);
      ASSERT_TRUE(abandoned.owns_cleanup());
    }  // destructor-abort must remove the coalescer flight

  PreparedRead retry =
      server.PrepareReadForKey(Key(2), 0, value.size(), second_staging.data(),
                               second_staging.size());
  EXPECT_EQ(retry.status(), Status::kOk);
  EXPECT_TRUE(retry.Abort());

  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
  ::unsetenv("DFKV_READ_COALESCE_TIMEOUT_MS");
}

// Regression (PR237 review #2): a coalesced TCP kRange must size its scratch
// from the REAL stored length, never from the client-declared range length --
// a forged multi-exabyte length used to resize the reply string before any
// storage lookup (one frame -> length_error/bad_alloc -> std::terminate, or
// OOM). Unknown keys must miss BEFORE any sized allocation, like the plain
// path. These asserts die on the unfixed code at the first hostile call.
TEST(PreparedRead, CoalescedRangeClampsHostileLength) {
  ::setenv("DFKV_READ_COALESCE", "1", 1);
  ::unsetenv("DFKV_RAM_TIER");
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_coalesce_range_clamp";
  fs::remove_all(dir);
  fs::create_directories(dir);
  {
  KvNodeServer server(dir.string(), 1ull << 30);
  ASSERT_TRUE(server.Healthy());

  std::string value(10000, '\0');
  for (size_t i = 0; i < value.size(); ++i)
    value[i] = static_cast<char>('a' + (i % 26));
  ASSERT_EQ(Put(&server, Key(7), value), Status::kOk);

  const uint64_t kHostile = std::numeric_limits<uint64_t>::max();
  auto range = [&](const BlockKey& key, uint64_t offset, uint64_t length,
                   std::string* out, size_t* value_len) {
    out->clear();
    return server.ProcessRequestForKey(
        static_cast<uint8_t>(WireOp::kRange), key, offset, length, nullptr, 0,
        out, value_len);
  };

  std::string out;
  size_t value_len = 0;
  // Whole value under a hostile length: served, clamped to the remainder.
  EXPECT_EQ(range(Key(7), 0, kHostile, &out, &value_len), Status::kOk);
  EXPECT_EQ(out, value);
  EXPECT_EQ(value_len, value.size());
  // Length beyond the remainder clamps to [offset, value_len).
  EXPECT_EQ(range(Key(7), 4000, kHostile, &out, &value_len), Status::kOk);
  EXPECT_EQ(out, value.substr(4000));
  EXPECT_EQ(value_len, value.size());
  // Store Range semantics survive: past-the-end offset is kInvalid, an
  // exact-end offset is an empty success with the authoritative length.
  EXPECT_EQ(range(Key(7), value.size() + 1, 16, &out, &value_len),
            Status::kInvalid);
  EXPECT_EQ(range(Key(7), value.size(), 16, &out, &value_len), Status::kOk);
  EXPECT_TRUE(out.empty());
  EXPECT_EQ(value_len, value.size());
  // The reported attack: an UNKNOWN key with a hostile length misses before
  // any sized allocation, exactly like the non-coalesced path.
  EXPECT_EQ(range(Key(404), 0, kHostile, &out, &value_len),
            Status::kNotFound);
  EXPECT_TRUE(out.empty());

  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
}

// Regression (PR237 review #3): the coalesced RangeDirect fill compacts the
// DIO superset in place (dst = shared scratch base, src = scratch + head with
// head in (0, 4095]) -- that forward overlap requires memmove; memcpy on it
// is UB and can hand shifted bytes to the leader and every follower. Assert
// byte-exact payload for non-4K-aligned offsets (sanitizer builds flag the
// overlap outright). Skips where the filesystem demotes the slab store to
// buffered I/O (tmpfs): there RangeDirect hands back io_buf itself and no
// head-shifted copy exists.
TEST(PreparedRead, CoalescedRangeDirectHeadShiftByteExact) {
  ::setenv("DFKV_READ_COALESCE", "1", 1);
  ::unsetenv("DFKV_RAM_TIER");
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_coalesce_direct_head";
  fs::remove_all(dir);
  fs::create_directories(dir);
  {
  KvNodeServer server(dir.string(), 1ull << 30);
  ASSERT_TRUE(server.Healthy());
  if (server.write_mode() != "direct")
    GTEST_SKIP() << "fs rejected O_DIRECT (tmpfs): no head-shifted read";

  std::string value(16384, '\0');
  for (size_t i = 0; i < value.size(); ++i)
    value[i] = static_cast<char>('a' + (i % 26));
  ASSERT_EQ(Put(&server, Key(8), value), Status::kOk);

  AlignedBuffer io(1 << 20);
  for (const uint64_t offset : {13ull, 4093ull}) {
    const uint64_t length = 5000;
    const char* data = nullptr;
    size_t len = 0;
    size_t value_len = 0;
    ASSERT_EQ(server.RangeDirectForKey(Key(8), offset, length, io.data(),
                                       io.size(), &data, &len, &value_len),
              Status::kOk);
    EXPECT_EQ(data, io.data());  // payload compacted back to the buffer base
    EXPECT_EQ(len, static_cast<size_t>(length));
    EXPECT_EQ(value_len, value.size());
    EXPECT_EQ(std::memcmp(data, value.data() + offset,
                          static_cast<size_t>(length)),
              0);
  }

  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
}

TEST(PreparedRead, WholeDiskMissReadsDirectlyIntoDurableRamSlot) {
  ::setenv("DFKV_READ_COALESCE", "1", 1);
  ::setenv("DFKV_READ_COALESCE_TIMEOUT_MS", "5000", 1);
  ::setenv("DFKV_RAM_TIER", "1", 1);
  ::setenv("DFKV_RAM_TIER_BYTES", "16777216", 1);
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  ::setenv("DFKV_RAM_WRITE_MODE", "writearound", 1);
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_prepared_read_direct_promotion";
  fs::remove_all(dir);
  fs::create_directories(dir);
  {
    DiskCacheGroup::Options options;
    options.cache_dirs = {dir.string()};
    options.capacity_bytes = 1ULL << 30;
    options.engine = "slab";
    options.eviction_policy = "legacy";
    KvNodeServer server(options);
    ASSERT_TRUE(server.Healthy());
    ASSERT_TRUE(server.ram_enabled());

    std::string value(8192, '\0');
    for (size_t i = 0; i < value.size(); ++i)
      value[i] = static_cast<char>('a' + (i % 23));
    AlignedBuffer seed(value.size());
    std::memcpy(seed.data(), value.data(), value.size());
    ASSERT_EQ(server.CacheDirectForKey(
                  Key(92), seed.data(), value.size(), seed.size()),
              Status::kOk);
    AlignedBuffer staging(1 << 20);

    PreparedRead read =
        server.PrepareReadForKey(Key(92), 0, value.size(), staging.data(),
                                 staging.size());
    ASSERT_EQ(read.status(), Status::kOk);
    ASSERT_TRUE(read.needs_io());
    EXPECT_EQ(read.source(), PreparedRead::Source::kDiskToRam);
    EXPECT_TRUE(read.source_registered());
    EXPECT_NE(read.staging(), staging.data());
    AlignedBuffer follower_staging(1 << 20);
    PreparedRead follower;
    std::thread follower_thread([&] {
      follower = server.PrepareReadForKey(
          Key(92), 0, value.size(), follower_staging.data(),
          follower_staging.size());
    });
    const bool follower_waited =
        KvNodeServerTestPeer::WaitForPublishedFollower(server);
    const ssize_t got = ::pread(read.fd(), read.staging(), read.aligned_len(),
                                static_cast<off_t>(read.aligned_off()));
    const bool io_ok =
        got >= 0 &&
        static_cast<size_t>(got) >= read.head() + read.payload_len();
    // MR fallback copies the bytes but must not turn this disk preparation
    // into a RAM hit just because its original destination is registered.
    if (io_ok) {
      EXPECT_TRUE(read.Stage());
      EXPECT_FALSE(read.source_registered());
      EXPECT_EQ(read.source(), PreparedRead::Source::kDiskToRam);
    }
    EXPECT_TRUE(read.Commit(io_ok ? Status::kOk : Status::kIOError,
                            io_ok ? read.payload_len() : 0, 0.001));
    follower_thread.join();
    ASSERT_TRUE(follower_waited);
    ASSERT_TRUE(io_ok);
    EXPECT_EQ(read.source(), PreparedRead::Source::kDiskToRam);
    EXPECT_EQ(std::memcmp(read.data(), value.data(), value.size()), 0);
    // The concurrent duplicate waited for disk completion, then acquired the
    // published arena entry directly: no follower payload copy or second IO.
    ASSERT_EQ(follower.status(), Status::kOk);
    EXPECT_FALSE(follower.needs_io());
    EXPECT_EQ(follower.source(), PreparedRead::Source::kPublishedFollower);
    EXPECT_TRUE(follower.source_registered());
    EXPECT_NE(follower.data(), follower_staging.data());
    EXPECT_EQ(std::memcmp(follower.data(), value.data(), value.size()), 0);

    // A second terminal call models SEND completion and releases the retained
    // post-read source pin; the follower releases its independent RAM pin.
    EXPECT_FALSE(read.Commit(Status::kOk, read.payload_len(), 0.0));
    EXPECT_TRUE(
        follower.Commit(Status::kOk, follower.payload_len(), 0.0));
    EXPECT_EQ(follower.source(), PreparedRead::Source::kPublishedFollower);

    PreparedRead ram_hit = server.PrepareReadForKey(Key(92), 0, value.size(),
                                                    follower_staging.data(),
                                                    follower_staging.size());
    ASSERT_EQ(ram_hit.status(), Status::kOk);
    EXPECT_FALSE(ram_hit.needs_io());
    EXPECT_EQ(ram_hit.source(), PreparedRead::Source::kRamHit);
    EXPECT_EQ(ram_hit.async_read_counters(), nullptr);
    EXPECT_TRUE(ram_hit.Stage());
    EXPECT_FALSE(ram_hit.source_registered());
    EXPECT_EQ(ram_hit.source(), PreparedRead::Source::kRamHit);
    EXPECT_EQ(std::string(ram_hit.data(), ram_hit.payload_len()), value);
    EXPECT_TRUE(ram_hit.Commit(Status::kOk, ram_hit.payload_len(), 0.0));
    EXPECT_EQ(ram_hit.source(), PreparedRead::Source::kRamHit);

    const char* direct_data = nullptr;
    size_t direct_bytes = 0;
    size_t direct_value_len = 0;
    PreparedRead::Source direct_source = PreparedRead::Source::kDiskStaging;
    ASSERT_EQ(server.RangeDirectForKey(
                  Key(92), 0, value.size(), follower_staging.data(),
                  follower_staging.size(), &direct_data, &direct_bytes,
                  &direct_value_len, nullptr, &direct_source),
              Status::kOk);
    EXPECT_EQ(direct_source, PreparedRead::Source::kSynchronousRamHit);
    EXPECT_EQ(std::string(direct_data, direct_bytes), value);
    ASSERT_EQ(server.RangeDirectForKey(
                  Key(92), value.size(), 1, follower_staging.data(),
                  follower_staging.size(), &direct_data, &direct_bytes,
                  &direct_value_len, nullptr, &direct_source),
              Status::kOk);
    EXPECT_EQ(direct_bytes, 0U);
    EXPECT_EQ(direct_source, PreparedRead::Source::kUnclassified);
    const DiskSlabStore::SyncPayloadReadCounters io =
        KvNodeServerTestPeer::SyncPayloadReads(server);
    EXPECT_EQ(io.syscalls, 0U);
    EXPECT_EQ(io.bytes, 0U);
    EXPECT_EQ(io.errors, 0U);
    EXPECT_EQ(io.short_reads, 0U);
  }
  fs::remove_all(dir);
  ::unsetenv("DFKV_READ_COALESCE");
  ::unsetenv("DFKV_READ_COALESCE_TIMEOUT_MS");
  ::unsetenv("DFKV_RAM_TIER");
  ::unsetenv("DFKV_RAM_TIER_BYTES");
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
  ::unsetenv("DFKV_RAM_WRITE_MODE");
}

}  // namespace
