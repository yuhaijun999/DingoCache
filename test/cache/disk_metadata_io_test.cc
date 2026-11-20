#include <gtest/gtest.h>
// SIGXFSZ is POSIX, not part of the C++ signal set.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <signal.h>
#include <sys/resource.h>
#include <unistd.h>

#include "cache/disk_slab_store.h"
// mkdtemp is a POSIX API without a std:: replacement.
// NOLINTNEXTLINE(hicpp-deprecated-headers,modernize-deprecated-headers)
#include <stdlib.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/slab_allocator.h"
#include "common/kv_types.h"
#include "common/status.h"

namespace dfkv {

class DiskSlabStoreTestPeer {
 public:
  static int ExchangeTableFd(DiskSlabStore& store, int fd) {
    const int previous = store.table_fd_;
    store.table_fd_ = fd;
    return previous;
  }
  static bool Sync(DiskSlabStore& store) { return store.SyncTable(0); }
  static void ReclaimClass(DiskSlabStore& store) {
    store.ReclaimLifetimeClass(0, 2, 8);
  }
  static bool WriteForPartialFailure(DiskSlabStore& store) {
    const char bytes[32] = {};
    return store.WriteTable(bytes, sizeof(bytes), 0,
                            DiskSlabStore::MetadataIoPath::kRecordWrite);
  }
  static void Tick(DiskSlabStore& store) {
    store.evict_high_bytes_ = 1;
    store.evict_low_bytes_ = 0;
    store.ReclaimTick();
  }
  static void FailBeforeWrite(DiskSlabStore& store) {
    store.record_write_hook_for_test_ = [] { return false; };
  }
};

namespace {

using IoPath = DiskSlabStore::MetadataIoPath;
using IoStats = DiskSlabStore::MetadataIoStats;

IoStats Io(const DiskSlabStore& store, IoPath path) {
  return store.GetStats().metadata_io[static_cast<size_t>(path)];
}

class DiskMetadataIoTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "dfkv-metadata-XXXXXX")
            .string();
    pattern.push_back('\0');
    const char* directory = ::mkdtemp(pattern.data());
    ASSERT_NE(directory, nullptr);
    directory_ = directory;
  }
  void TearDown() override { std::filesystem::remove_all(directory_); }
  DiskSlabStore::Options Options(bool lifetime = false) const {
    DiskSlabStore::Options options;
    options.dir = directory_.string();
    options.extent_bytes = 32768;
    options.capacity_bytes = options.extent_bytes;
    options.slot_granularity = 4096;
    options.table_sync_ms = 0;
    options.reclaim_interval_ms = 0;
    if (lifetime) {
      options.lifetime_config = DiskLifetimeConfig{};
      options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
          DiskLifetimeHorizon(*options.lifetime_config), [] { return 0; });
      options.lifetime_class_eviction = true;
    }
    return options;
  }
  std::filesystem::path directory_;
};

TEST_F(DiskMetadataIoTest, RuntimeWritesSeparateRecordsBindingsAndSync) {
  bool ok = false;
  DiskSlabStore store(Options(), &ok);
  ASSERT_TRUE(ok);
  for (const IoStats& io : store.GetStats().metadata_io)
    EXPECT_EQ(io.attempts, 0U);  // Startup writes are deliberately excluded.
  ASSERT_EQ(store.Cache(BlockKey{1, 0}, "abc", 3), Status::kOk);
  const IoStats binding = Io(store, IoPath::kExtentBind);
  EXPECT_EQ(binding.attempts, 1U);
  EXPECT_EQ(binding.completed, 1U);
  EXPECT_EQ(binding.errors, 0U);
  EXPECT_GT(binding.bytes, 0U);
  const IoStats record = Io(store, IoPath::kRecordWrite);
  EXPECT_EQ(record.attempts, 1U);
  EXPECT_EQ(record.completed, 1U);
  EXPECT_EQ(binding.bytes, 8 * record.bytes);
  EXPECT_EQ(Io(store, IoPath::kBindSync).completed, 1U);
  EXPECT_EQ(store.GetStats().table_syncs, 0U);
  ASSERT_EQ(store.Remove(BlockKey{1, 0}), Status::kOk);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).completed, 2U);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).bytes, 2 * record.bytes);
  ASSERT_TRUE(DiskSlabStoreTestPeer::Sync(store));
  EXPECT_EQ(Io(store, IoPath::kTableSync).completed, 1U);
  EXPECT_EQ(Io(store, IoPath::kTableSync).bytes, 0U);
  EXPECT_EQ(store.GetStats().table_syncs, 1U);
  EXPECT_EQ(store.GetStats().record_writes, 2U);
}

TEST_F(DiskMetadataIoTest, FailedRecordIoCountsFailureWithoutBytesOrSuccess) {
  bool ok = false;
  DiskSlabStore store(Options(), &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(store.Cache(BlockKey{1, 0}, "abc", 3), Status::kOk);
  const IoStats before = Io(store, IoPath::kRecordWrite);
  const int fd = DiskSlabStoreTestPeer::ExchangeTableFd(store, -1);
  const Status removed = store.Remove(BlockKey{1, 0});
  DiskSlabStoreTestPeer::ExchangeTableFd(store, fd);
  EXPECT_EQ(removed, Status::kIOError);
  const IoStats after = Io(store, IoPath::kRecordWrite);
  EXPECT_EQ(after.attempts, before.attempts + 1);
  EXPECT_EQ(after.completed, before.completed);
  EXPECT_EQ(after.bytes, before.bytes);
  EXPECT_EQ(after.errors, before.errors + 1);
  EXPECT_EQ(store.GetStats().metadata_io_errors, 1U);
  EXPECT_TRUE(store.GetStats().failed);
  // Fail-closed calls rejected before I/O must not invent more attempts.
  EXPECT_EQ(store.Remove(BlockKey{1, 0}), Status::kIOError);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).attempts, after.attempts);
}

TEST_F(DiskMetadataIoTest, PartialWriteFailureRetainsActualProgress) {
  // Only the child changes its signal disposition/file-size limit. The table
  // already exists; the first pwrite accepts 16 bytes, the retry hits EFBIG.
  ASSERT_EXIT(
      {
        bool ok = false;
        DiskSlabStore store(Options(), &ok);
        if (!ok) ::_exit(2);
        if (::signal(SIGXFSZ, SIG_IGN) == SIG_ERR) ::_exit(3);
        struct rlimit limit {};
        limit.rlim_cur = 16;
        limit.rlim_max = 16;
        if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) ::_exit(4);
        const bool written =
            DiskSlabStoreTestPeer::WriteForPartialFailure(store);
        const IoStats io = Io(store, IoPath::kRecordWrite);
        const bool correct = !written && io.attempts == 1 &&
                             io.completed == 0 && io.errors == 1 &&
                             io.bytes == 16 && store.GetStats().failed;
        ::_exit(correct ? 0 : 5);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST_F(DiskMetadataIoTest, FailedSyncHasOwnTerminalAndPreservesOldSyncCount) {
  bool ok = false;
  DiskSlabStore store(Options(), &ok);
  ASSERT_TRUE(ok);
  const int fd = DiskSlabStoreTestPeer::ExchangeTableFd(store, -1);
  const bool synced = DiskSlabStoreTestPeer::Sync(store);
  DiskSlabStoreTestPeer::ExchangeTableFd(store, fd);
  EXPECT_FALSE(synced);
  const IoStats io = Io(store, IoPath::kTableSync);
  EXPECT_EQ(io.attempts, 1U);
  EXPECT_EQ(io.completed, 0U);
  EXPECT_EQ(io.errors, 1U);
  EXPECT_EQ(io.bytes, 0U);
  EXPECT_EQ(store.GetStats().table_syncs, 0U);
  EXPECT_EQ(store.GetStats().metadata_io_errors, 1U);
}

TEST_F(DiskMetadataIoTest, PreIoHookFailureDoesNotInventTableWriteAttempt) {
  bool ok = false;
  DiskSlabStore store(Options(), &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(store.Cache(BlockKey{1, 0}, "abc", 3), Status::kOk);
  const IoStats before = Io(store, IoPath::kRecordWrite);
  DiskSlabStoreTestPeer::FailBeforeWrite(store);
  EXPECT_EQ(store.Remove(BlockKey{1, 0}), Status::kIOError);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).attempts, before.attempts);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).errors, before.errors);
  EXPECT_EQ(store.GetStats().metadata_io_errors, 1U);
}

TEST_F(DiskMetadataIoTest, LifetimeWatermarkWritesAreNotPutRecordWrites) {
  bool ok = false;
  DiskSlabStore store(Options(true), &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(store.Cache(BlockKey{1, 0}, "abc", 3), Status::kOk);
  ASSERT_EQ(store.Cache(BlockKey{2, 0}, "xyz", 3), Status::kOk);
  const IoStats records = Io(store, IoPath::kRecordWrite);
  DiskSlabStoreTestPeer::Tick(store);
  const IoStats slot = Io(store, IoPath::kWatermarkEvict);
  EXPECT_EQ(slot.completed, 2U);
  EXPECT_EQ(slot.attempts, slot.completed);
  EXPECT_EQ(slot.errors, 0U);
  EXPECT_EQ(slot.bytes, records.bytes);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).completed, records.completed);
  EXPECT_EQ(store.Count(), 0U);
  EXPECT_EQ(store.GetStats().watermark_record_clear_bytes, slot.bytes);
}

TEST_F(DiskMetadataIoTest, LegacyLastResidentUsesWholeExtentWrite) {
  bool ok = false;
  DiskSlabStore store(Options(), &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(store.Cache(BlockKey{1, 0}, "abc", 3), Status::kOk);
  ASSERT_EQ(store.Cache(BlockKey{2, 0}, "xyz", 3), Status::kOk);
  DiskSlabStoreTestPeer::Tick(store);
  EXPECT_EQ(Io(store, IoPath::kForegroundEvict).completed, 0U);
  const IoStats extent = Io(store, IoPath::kWatermarkEvict);
  EXPECT_EQ(extent.attempts, 2U);
  EXPECT_EQ(extent.completed, 2U);
  EXPECT_EQ(extent.errors, 0U);
  EXPECT_EQ(extent.bytes, Io(store, IoPath::kExtentBind).bytes +
                              Io(store, IoPath::kRecordWrite).bytes / 2);
  EXPECT_EQ(store.Count(), 0U);
}

TEST_F(DiskMetadataIoTest, FailedBindingDoesNotClaimSuccessfulWipeOrSync) {
  bool ok = false;
  DiskSlabStore store(Options(), &ok);
  ASSERT_TRUE(ok);
  const int fd = DiskSlabStoreTestPeer::ExchangeTableFd(store, -1);
  const Status cached = store.Cache(BlockKey{1, 0}, "abc", 3);
  DiskSlabStoreTestPeer::ExchangeTableFd(store, fd);
  EXPECT_EQ(cached, Status::kIOError);
  const IoStats binding = Io(store, IoPath::kExtentBind);
  EXPECT_EQ(binding.attempts, 1U);
  EXPECT_EQ(binding.completed, 0U);
  EXPECT_EQ(binding.bytes, 0U);
  EXPECT_EQ(binding.errors, 1U);
  EXPECT_EQ(Io(store, IoPath::kBindSync).attempts, 0U);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).attempts, 0U);
  EXPECT_EQ(store.GetStats().bind_wipes, 0U);
}

TEST_F(DiskMetadataIoTest, BackgroundOriginIsResetBeforeFollowingForegroundIo) {
  bool ok = false;
  DiskSlabStore store(Options(true), &ok);
  ASSERT_TRUE(ok);
  for (uint64_t id = 1; id <= 8; ++id)
    ASSERT_EQ(store.Cache(BlockKey{id, 0}, "abc", 3), Status::kOk);
  DiskSlabStoreTestPeer::ReclaimClass(store);
  EXPECT_EQ(Io(store, IoPath::kBackgroundEvict).completed, 2U);
  EXPECT_EQ(Io(store, IoPath::kForegroundEvict).attempts, 0U);
  for (uint64_t id = 9; id <= 11; ++id)
    ASSERT_EQ(store.Cache(BlockKey{id, 0}, "abc", 3), Status::kOk);
  EXPECT_EQ(Io(store, IoPath::kBackgroundEvict).completed, 2U);
  EXPECT_EQ(Io(store, IoPath::kForegroundEvict).completed, 1U);
}

TEST_F(DiskMetadataIoTest, DonorRebindSeparatesWipeAndSyncFromInitialBinding) {
  bool ok = false;
  DiskSlabStore store(Options(true), &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(store.Cache(BlockKey{1, 0}, "abc", 3), Status::kOk);
  ASSERT_EQ(store.Cache(BlockKey{2, 0}, "xyz", 3), Status::kOk);
  const std::string large(5000, 'd');
  ASSERT_EQ(store.Cache(BlockKey{3, 0}, large.data(), large.size()),
            Status::kOk);
  EXPECT_EQ(Io(store, IoPath::kExtentBind).completed, 1U);
  EXPECT_EQ(Io(store, IoPath::kBindSync).completed, 1U);
  EXPECT_EQ(Io(store, IoPath::kDonorBind).completed, 1U);
  EXPECT_EQ(Io(store, IoPath::kDonorSync).completed, 1U);
  EXPECT_EQ(Io(store, IoPath::kDonorBind).bytes,
            Io(store, IoPath::kExtentBind).bytes);
  EXPECT_EQ(store.GetStats().bind_wipes, 2U);
  std::string bytes;
  EXPECT_EQ(store.Range(BlockKey{1, 0}, 0, 0, &bytes), Status::kNotFound);
  ASSERT_EQ(store.Range(BlockKey{3, 0}, 0, 0, &bytes), Status::kOk);
  EXPECT_EQ(bytes, large);
}

TEST_F(DiskMetadataIoTest,
       FailedDonorWipeKeepsResidentsAndItsOwnFailureOrigin) {
  bool ok = false;
  DiskSlabStore store(Options(true), &ok);
  ASSERT_TRUE(ok);
  ASSERT_EQ(store.Cache(BlockKey{1, 0}, "abc", 3), Status::kOk);
  ASSERT_EQ(store.Cache(BlockKey{2, 0}, "xyz", 3), Status::kOk);
  const int fd = DiskSlabStoreTestPeer::ExchangeTableFd(store, -1);
  const std::string large(5000, 'd');
  const Status cached = store.Cache(BlockKey{3, 0}, large.data(), large.size());
  DiskSlabStoreTestPeer::ExchangeTableFd(store, fd);
  EXPECT_EQ(cached, Status::kIOError);
  const IoStats donor = Io(store, IoPath::kDonorBind);
  EXPECT_EQ(donor.attempts, 1U);
  EXPECT_EQ(donor.completed, 0U);
  EXPECT_EQ(donor.errors, 1U);
  EXPECT_EQ(donor.bytes, 0U);
  EXPECT_EQ(Io(store, IoPath::kDonorSync).attempts, 0U);
  EXPECT_EQ(Io(store, IoPath::kExtentBind).completed, 1U);
  EXPECT_EQ(store.Count(), 2U);
  EXPECT_EQ(store.GetStats().bind_wipes, 1U);
}

TEST_F(DiskMetadataIoTest, ClassEvictionCountsItsOwnRuntimeWrite) {
  bool ok = false;
  DiskSlabStore store(Options(true), &ok);
  ASSERT_TRUE(ok);
  for (uint64_t id = 1; id <= 9; ++id)
    ASSERT_EQ(store.Cache(BlockKey{id, 0}, "abc", 3), Status::kOk);
  EXPECT_EQ(Io(store, IoPath::kRecordWrite).completed, 9U);
  EXPECT_EQ(Io(store, IoPath::kForegroundEvict).completed, 1U);
  EXPECT_EQ(Io(store, IoPath::kForegroundEvict).errors, 0U);
  EXPECT_EQ(store.GetStats().watermark_record_clear_bytes, 0U);
}

}  // namespace
}  // namespace dfkv
