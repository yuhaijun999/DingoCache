// TDD R11 — server metrics counters + Prometheus text + remote Stats op.
#include "cache/kv_node_server.h"
#include "client/key_map.h"
#include "utils/net_util.h"
#include "transport/tcp_transport.h"

#include <unistd.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include <filesystem>
#include <fstream>
#include <map>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "cache/async_read_counters.h"
#include "cache/disk_cache_group.h"
#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_resident_identity.h"
#include "cache/disk_slab_store.h"
#include "cache/disk_write_result.h"
#include "cache/slab_allocator.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "transport/wire.h"

namespace dfkv {
class KvNodeServerTestPeer {
 public:
  static DiskCacheGroup& Group(KvNodeServer& server) { return server.group_; }
};

class DiskCacheGroupTestPeer {
 public:
  static size_t RouteIndex(const DiskCacheGroup& group, const BlockKey& key) {
    return group.RouteIndex(key);
  }
  static DiskSlabStore& Slab(DiskCacheGroup& group, size_t index) {
    return *group.slabs_.at(index);
  }
};

class DiskSlabStoreTestPeer {
 public:
  static std::pair<int, uint64_t> PayloadFile(DiskSlabStore& store,
                                              const BlockKey& key) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    SlabAllocator::SlotRef ref;
    if (!store.alloc_->Get(key, &ref)) return {-1, 0};
    return {store.extent_fds_.at(ref.extent), ref.offset};
  }
  static int ExchangePayloadFd(DiskSlabStore& store, const BlockKey& key,
                               int fd) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    SlabAllocator::SlotRef ref;
    if (!store.alloc_->Get(key, &ref)) return -1;
    return std::exchange(store.extent_fds_.at(ref.extent), fd);
  }

  static void FailMetadata(DiskSlabStore& store) {
    const std::lock_guard<std::mutex> lock(store.mu_);
    if (store.table_fd_ >= 0) ::close(store.table_fd_);
    store.table_fd_ = -1;
  }
  static void TickBackground(DiskSlabStore& store) {
    store.ReclaimLifetimeTick(1);
  }
  static void TickWatermark(DiskSlabStore& store) {
    // No background worker runs in this test. Isolate global reclamation from
    // the independent class headroom pass, without replacing its allocator.
    const std::vector<SlabAllocator::ClassStat> classes =
        store.alloc_->Classes();
    store.reclaim_last_puts_.resize(classes.size());
    store.lifetime_reclaim_targets_.assign(classes.size(), 0);
    for (size_t i = 0; i < classes.size(); ++i)
      store.reclaim_last_puts_[i] = classes[i].puts;
    store.evict_high_bytes_ = 2 * 1048576 - 1;
    store.evict_low_bytes_ = 1048576;
    store.evict_max_victims_per_tick_ = 1;
    store.ReclaimTick();
  }
};
}  // namespace dfkv

namespace fs = std::filesystem;
using namespace dfkv;  // NOLINT

namespace {
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    const char* old = ::getenv(name);
    if (old != nullptr) {
      had_old_ = true;
      old_ = old;
    }
    ::setenv(name, value, 1);
  }
  ~ScopedEnv() {
    if (had_old_)
      ::setenv(name_.c_str(), old_.c_str(), 1);
    else
      ::unsetenv(name_.c_str());
  }

 private:
  std::string name_;
  std::string old_;
  bool had_old_ = false;
};

DiskLifetimeConfig BucketMetricConfig() {
  DiskLifetimeConfig config;
  config.base_sec = 100;
  config.read_bonus_sec = 10;
  config.max_reads = 2;
  config.protect_sec = 8;
  config.slowdown = 4;
  config.keep_score_sec = 90;
  config.bucket_sec = 32;
  config.bucket_count = 8;
  config.high_pct = 0;
  return config;
}

std::unique_ptr<KvNodeServer> Start(fs::path dir, std::string* addr) {
  fs::remove_all(dir); fs::create_directories(dir);
  auto s = std::make_unique<KvNodeServer>(dir.string(), 1ull << 30);
  EXPECT_EQ(s->Start(0), Status::kOk);
  *addr = "127.0.0.1:" + std::to_string(s->port());
  return s;
}
}  // namespace

TEST(Metrics, CountersTrackOps) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_a";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v(100, 'm');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "a"), v.data(), v.size()), Status::kOk);
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "b"), v.data(), v.size()), Status::kOk);
  std::string out;
  ASSERT_EQ(t.Range(addr, ToBlockKey("test/model", "a"), 0, v.size(), &out), Status::kOk);   // hit
  ASSERT_EQ(t.Range(addr, ToBlockKey("test/model", "zzz"), 0, 8, &out), Status::kNotFound);  // miss
  bool e = false;
  ASSERT_EQ(t.Exist(addr, ToBlockKey("test/model", "a"), &e), Status::kOk); EXPECT_TRUE(e);   // exist hit
  ASSERT_EQ(t.Exist(addr, ToBlockKey("test/model", "nope"), &e), Status::kOk); EXPECT_FALSE(e); // exist miss

  EXPECT_EQ(s->m_cache_put(), 2u);
  EXPECT_EQ(s->m_cache_hit(), 1u);
  EXPECT_EQ(s->m_cache_miss(), 1u);
  EXPECT_EQ(s->m_exist_hit(), 1u);
  EXPECT_EQ(s->m_exist_miss(), 1u);

  std::string text = s->MetricsText();
  EXPECT_NE(text.find("dfkv_cache_hit_total 1"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_cache_put_total 2"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_objects 2"), std::string::npos) << text;
  s->Stop();
}

TEST(Metrics, TenantQuotaSeriesAreBoundedToConfiguredHashes) {
  const fs::path quota_file =
      fs::temp_directory_path() / "dfkv_metrics_tenant_quotas";
  std::ofstream(quota_file) << "fa1cde78082f951e 4\n";
  ScopedEnv quotas("DFKV_TENANT_QUOTAS_FILE", quota_file.c_str());
  ScopedEnv default_quota("DFKV_TENANT_DEFAULT_QUOTA_BYTES", "0");
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_quota";
  auto server = Start(dir, &addr);
  TcpTransport transport;
  const BlockKey key = ToBlockKey("test/model", "too-large");
  ASSERT_EQ(key.tenant_hash, 0xfa1cde78082f951eULL);
  EXPECT_EQ(transport.Cache(addr, key, "12345", 5),
            Status::kQuotaExceeded);
  const std::string text = server->MetricsText();
  EXPECT_NE(text.find("dfkv_tenant_default_quota_bytes 0"),
            std::string::npos) << text;
  EXPECT_NE(text.find(
                "dfkv_tenant_quota_limit_bytes{tenant_hash=\""
                "fa1cde78082f951e\"} 4"),
            std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_tenant_quota_rejections_total 1"),
            std::string::npos) << text;
  server->Stop();
  fs::remove(quota_file);
}

TEST(Metrics, SlabCapacityAndCorrectnessSeriesReflectCommittedState) {
  ScopedEnv engine("DFKV_STORE_ENGINE", "slab");
  ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_slab";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string value(3000, 's');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "slab-key"), value.data(), value.size()),
            Status::kOk);

  const std::string text = s->MetricsText();
  EXPECT_TRUE(
      KvNodeServerTestPeer::Group(*s).DiskLifetimeQueueStats(0).empty());
  EXPECT_TRUE(
      KvNodeServerTestPeer::Group(*s).DiskLifetimeQueueStats(1).empty());
  EXPECT_EQ(text.find("dfkv_slab_lifetime_queue_residents{"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_disk_lifetime_queue_residents{"),
            std::string::npos);
  EXPECT_NE(text.find("dfkv_slab_capacity_bytes 1073741824"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_allocated_bytes 4096"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_payload_bytes 3000"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_allocator_payload_bytes 3000\n"),
            std::string::npos);
  EXPECT_NE(text.find("dfkv_slab_allocator_fragmentation_bytes 1096\n"),
            std::string::npos);
  EXPECT_NE(text.find("dfkv_slab_internal_fragmentation_bytes 1096"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_allocator_objects 1"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_committed_objects 1"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_record_writes_total 1"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_bound_extents 1"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_pool_extents 0"), std::string::npos)
      << text;
  EXPECT_NE(text.find(
                "dfkv_slab_class_extents{slot_size=\"4096\"} 1"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find(
                "dfkv_slab_class_resident_objects{slot_size=\"4096\"} 1"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find(
                "dfkv_slab_class_fragmentation_bytes{slot_size=\"4096\"} 1096"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_failed_disks 0"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_healthy 1"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_storage_healthy 1"), std::string::npos) << text;
  // The foreground selector remains internally gated: ordinary slab traffic
  // exports these bounded-cardinality counters without recording new scans.
  for (const char* name :
       {"dfkv_slab_lifetime_class_scan_visits_total",
        "dfkv_slab_lifetime_class_scan_slices_total",
        "dfkv_slab_lifetime_scan_budget_exhausted_total",
        "dfkv_slab_lifetime_class_no_candidate_total",
        "dfkv_slab_lifetime_class_victims_total",
        "dfkv_slab_lifetime_class_freed_payload_bytes_total",
        "dfkv_slab_lifetime_class_freed_slot_bytes_total",
        "dfkv_slab_lifetime_background_scan_visits_total",
        "dfkv_slab_lifetime_background_scan_slices_total",
        "dfkv_slab_lifetime_background_budget_exhausted_total",
        "dfkv_slab_lifetime_background_no_candidate_total",
        "dfkv_slab_lifetime_background_victims_total",
        "dfkv_slab_lifetime_background_freed_payload_bytes_total",
        "dfkv_slab_lifetime_background_freed_slot_bytes_total",
        "dfkv_slab_lifetime_watermark_budget_exhausted_total",
        "dfkv_slab_lifetime_watermark_no_candidate_total",
        "dfkv_slab_lifetime_watermark_pin_skips_total",
        "dfkv_slab_lifetime_watermark_protected_victims_total",
        "dfkv_slab_lifetime_donor_coarse_extents_total",
        "dfkv_slab_lifetime_donor_precise_extents_total",
        "dfkv_slab_lifetime_donor_precise_residents_total",
        "dfkv_slab_lifetime_donor_budget_fallback_total",
        "dfkv_slab_lifetime_donor_unavailable_total",
        "dfkv_slab_lifetime_donor_persistence_errors_total",
        "dfkv_slab_lifetime_donor_selection_ns_total",
        "dfkv_slab_lifetime_donor_persistence_ns_total",
        "dfkv_slab_lifetime_donor_released_residents_total",
        "dfkv_slab_lifetime_donor_freed_slot_bytes_total",
        "dfkv_slab_lifetime_donor_freed_payload_bytes_total",
        "dfkv_slab_lifetime_donor_protected_victims_total",
        "dfkv_slab_lifetime_donor_protected_payload_bytes_total",
        "dfkv_slab_lifetime_donor_unscored_victims_total"}) {
    EXPECT_NE(text.find(std::string(name) + " 0\n"), std::string::npos);
  }
  for (const char* name :
       {"dfkv_slab_lifetime_bucket_entries", "dfkv_slab_lifetime_bucket_bytes",
        "dfkv_slab_lifetime_bucket_token_bytes",
        "dfkv_slab_lifetime_state_bytes", "dfkv_slab_lifetime_link_bytes",
        "dfkv_slab_lifetime_queue_bytes", "dfkv_slab_lifetime_version_bytes",
        "dfkv_slab_lifetime_recovered_objects",
        "dfkv_slab_lifetime_recovered_unread_objects"}) {
    EXPECT_NE(text.find(std::string(name) + " 0\n"), std::string::npos);
  }
  EXPECT_NE(text.find("\ndfkv_slab_lifetime_bucket_estimate_available 0\n"),
            std::string::npos);
  EXPECT_NE(text.find("\ndfkv_slab_lifetime_bucket_summary_bytes 0\n"),
            std::string::npos);
  EXPECT_EQ(text.find("\ndfkv_slab_lifetime_bucket_protected_estimate "),
            std::string::npos);
  EXPECT_FALSE(KvNodeServerTestPeer::Group(*s)
                   .DiskLifetimeBucketObservation(0)
                   .available);
  const std::string disk_labels =
      R"({disk=")" + dir.string() + R"(",disk_index="0"} )";
  EXPECT_NE(text.find("dfkv_slab_disk_allocator_payload_bytes" + disk_labels +
                      "3000\n"),
            std::string::npos);
  EXPECT_NE(text.find("dfkv_slab_disk_allocator_fragmentation_bytes" +
                      disk_labels + "1096\n"),
            std::string::npos);
  const std::string labels = disk_labels + "0\n";
  for (const char* suffix : {"success",
                             "applied",
                             "no_binding",
                             "race_lost",
                             "clock_hard_exhausted",
                             "io_unhealthy",
                             "state_unavailable",
                             "source_unclassified",
                             "source_ram",
                             "source_disk",
                             "source_copy_leader",
                             "source_copy_follower",
                             "source_transport_grant",
                             "transport_unclassified",
                             "transport_ram_hit",
                             "transport_disk_staging",
                             "transport_disk_to_ram",
                             "transport_published_follower",
                             "transport_synchronous_fallback",
                             "transport_fallback_unclassified",
                             "transport_fallback_ram_hit",
                             "transport_fallback_disk",
                             "transport_fallback_copy_leader",
                             "transport_fallback_copy_follower"}) {
    EXPECT_NE(text.find(std::string("dfkv_slab_disk_lifetime_read_") + suffix +
                        "_total" + labels),
              std::string::npos);
    EXPECT_NE(text.find(std::string("dfkv_slab_lifetime_read_") + suffix +
                        "_total 0\n"),
              std::string::npos);
  }
  for (const char* name :
       {"dfkv_slab_disk_lifetime_bucket_entries",
        "dfkv_slab_disk_lifetime_bucket_bytes",
        "dfkv_slab_disk_lifetime_bucket_token_bytes",
        "dfkv_slab_disk_lifetime_state_bytes",
        "dfkv_slab_disk_lifetime_link_bytes",
        "dfkv_slab_disk_lifetime_queue_bytes",
        "dfkv_slab_disk_lifetime_version_bytes",
        "dfkv_slab_disk_lifetime_recovered_objects",
        "dfkv_slab_disk_lifetime_recovered_unread_objects"}) {
    EXPECT_NE(text.find(std::string(name) + labels), std::string::npos);
  }
  for (const char* name :
       {"dfkv_slab_disk_lifetime_donor_released_residents_total",
        "dfkv_slab_disk_lifetime_donor_freed_slot_bytes_total",
        "dfkv_slab_disk_lifetime_donor_freed_payload_bytes_total",
        "dfkv_slab_disk_lifetime_donor_protected_victims_total",
        "dfkv_slab_disk_lifetime_donor_protected_payload_bytes_total",
        "dfkv_slab_disk_lifetime_donor_unscored_victims_total"}) {
    EXPECT_NE(text.find(std::string(name) + labels), std::string::npos);
  }
  s->Stop();
  s.reset();
  fs::remove_all(dir);
}

TEST(Metrics, WatermarkPayloadBytesAggregateAcrossDisksWithoutDoubleCounting) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "1048576");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_watermark";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
  ASSERT_EQ(group.DiskCount(), 2U);
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
    // Each disk has equal-size payloads but the disks differ, so aggregation
    // must sum their counters rather than report either one alone.
    const std::string value(disk == 0 ? 17 : 31, 'v');
    ASSERT_EQ(store.Cache({1, 0}, value.data(), value.size()), Status::kOk);
    ASSERT_EQ(store.Cache({2, 0}, value.data(), value.size()), Status::kOk);
    ASSERT_EQ(store.Count(), 2U);
    DiskSlabStoreTestPeer::TickWatermark(store);
    EXPECT_EQ(store.Count(), 1U);
  }
  const std::string text = server->MetricsText();
  EXPECT_NE(text.find("# TYPE dfkv_slab_watermark_freed_payload_bytes_total "
                      "counter\n"),
            std::string::npos);
  EXPECT_NE(text.find("dfkv_slab_watermark_freed_payload_bytes_total 48\n"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_evicted_bytes_total 48\n"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_watermark_freed_slot_bytes_total 2097152\n"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_payload_bytes 48\n"), std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_watermark_slot_removals_total 2\n"),
            std::string::npos)
      << text;
  for (size_t disk = 0; disk < 2; ++disk) {
    const std::string sample =
        "dfkv_slab_disk_watermark_freed_payload_bytes_total{disk=\"" +
        options.cache_dirs[disk] + "\",disk_index=\"" + std::to_string(disk) +
        "\"} " + (disk == 0 ? "17" : "31") + "\n";
    EXPECT_NE(text.find(sample), std::string::npos) << text;
  }
  for (size_t disk = 0; disk < 2; ++disk)
    DiskSlabStoreTestPeer::TickWatermark(
        DiskCacheGroupTestPeer::Slab(group, disk));
  const std::string repeated = server->MetricsText();
  EXPECT_NE(repeated.find("dfkv_slab_watermark_freed_payload_bytes_total 48\n"),
            std::string::npos)
      << repeated;
  EXPECT_NE(repeated.find("dfkv_evicted_bytes_total 48\n"), std::string::npos)
      << repeated;
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, BackgroundClassBytesAggregateAndResumeAcrossDisks) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "134217728");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_background";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
  ASSERT_EQ(group.DiskCount(), 2U);
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
    const std::string value(disk == 0 ? 17 : 31, 'v');
    // Eight 128 MiB slots fill the real 1 GiB extent. The two-slot headroom
    // target cannot fit in one successful-victim budget, so the next quiet
    // tick must resume it. Only the tiny payloads are written to sparse files.
    for (uint64_t key = 1; key <= 8; ++key) {
      ASSERT_EQ(store.Cache({key, 0}, value.data(), value.size()), Status::kOk);
    }
    ASSERT_EQ(store.Count(), 8U);
    DiskSlabStoreTestPeer::TickBackground(store);
    EXPECT_EQ(store.Count(), 7U);
  }
  const std::string first = server->MetricsText();
  EXPECT_NE(first.find("# TYPE dfkv_slab_lifetime_background_victims_total "
                       "counter\n"),
            std::string::npos);
  EXPECT_NE(first.find("dfkv_slab_lifetime_background_victims_total 2\n"),
            std::string::npos)
      << first;
  EXPECT_NE(
      first.find("dfkv_slab_lifetime_background_budget_exhausted_total 2\n"),
      std::string::npos)
      << first;
  EXPECT_NE(first.find(
                "dfkv_slab_lifetime_background_freed_payload_bytes_total 48\n"),
            std::string::npos)
      << first;
  EXPECT_NE(first.find("dfkv_slab_lifetime_background_freed_slot_bytes_total "
                       "268435456\n"),
            std::string::npos)
      << first;
  EXPECT_NE(first.find("dfkv_evicted_bytes_total 48\n"), std::string::npos);
  EXPECT_NE(first.find("dfkv_slab_lifetime_class_victims_total 0\n"),
            std::string::npos);
  EXPECT_NE(first.find("dfkv_slab_watermark_slot_removals_total 0\n"),
            std::string::npos);
  for (size_t disk = 0; disk < 2; ++disk) {
    const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                               "\",disk_index=\"" + std::to_string(disk) +
                               "\"} ";
    EXPECT_NE(first.find("dfkv_slab_disk_lifetime_background_victims_total" +
                         labels + "1\n"),
              std::string::npos)
        << first;
    EXPECT_NE(
        first.find(
            "dfkv_slab_disk_lifetime_background_freed_payload_bytes_total" +
            labels + (disk == 0 ? "17" : "31") + "\n"),
        std::string::npos)
        << first;
    EXPECT_NE(
        first.find("dfkv_slab_disk_lifetime_background_freed_slot_bytes_total" +
                   labels + "134217728\n"),
        std::string::npos)
        << first;
    DiskSlabStoreTestPeer::TickBackground(
        DiskCacheGroupTestPeer::Slab(group, disk));
    EXPECT_EQ(DiskCacheGroupTestPeer::Slab(group, disk).Count(), 6U);
  }
  for (unsigned tick = 0; tick < 2; ++tick) {
    const std::string completed = server->MetricsText();
    EXPECT_NE(completed.find("dfkv_slab_lifetime_background_victims_total 4\n"),
              std::string::npos);
    EXPECT_NE(
        completed.find(
            "dfkv_slab_lifetime_background_freed_payload_bytes_total 96\n"),
        std::string::npos);
    EXPECT_NE(
        completed.find("dfkv_slab_lifetime_background_freed_slot_bytes_total "
                       "536870912\n"),
        std::string::npos);
    EXPECT_NE(completed.find("dfkv_evicted_bytes_total 96\n"),
              std::string::npos);
    for (size_t disk = 0; disk < 2; ++disk) {
      DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
      DiskSlabStoreTestPeer::TickBackground(store);
      EXPECT_EQ(store.Count(), 6U);
    }
  }
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, ForegroundClassBytesExcludeIncomingWritesAndRepeatedPuts) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "134217728");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_foreground";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
  ASSERT_EQ(group.DiskCount(), 2U);
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
    const std::string old_value(disk == 0 ? 17 : 31, 'v');
    // Fill the real eight-slot extent; later PUTs cannot use spare capacity.
    for (uint64_t key = 1; key <= 8; ++key) {
      ASSERT_EQ(store.Cache({key, 0}, old_value.data(), old_value.size()),
                Status::kOk);
    }
    ASSERT_EQ(store.Count(), 8U);
  }
  const std::string new_value(101, 'n');
  for (unsigned stage = 0; stage < 3; ++stage) {
    SCOPED_TRACE(stage);
    const uint64_t key = stage == 2 ? 10 : 9;
    const uint64_t victims = stage == 2 ? 2 : 1;
    for (size_t disk = 0; disk < 2; ++disk) {
      DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
      ASSERT_EQ(store.Cache({key, 0}, new_value.data(), new_value.size()),
                Status::kOk);
      EXPECT_EQ(store.Count(), 8U);
    }
    // Stage one repeats the same resident. Victim payloads are 17/31 bytes,
    // not the incoming 101 bytes, and no background/watermark/donor ran.
    const std::string text = server->MetricsText();
    EXPECT_NE(text.find("dfkv_slab_lifetime_class_victims_total " +
                        std::to_string(2 * victims) + "\n"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("dfkv_slab_lifetime_class_freed_payload_bytes_total " +
                        std::to_string(48 * victims) + "\n"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("dfkv_slab_lifetime_class_freed_slot_bytes_total " +
                        std::to_string(268435456 * victims) + "\n"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("dfkv_evicted_bytes_total " +
                        std::to_string(48 * victims) + "\n"),
              std::string::npos);
    for (const char* name :
         {"dfkv_slab_lifetime_background_victims_total",
          "dfkv_slab_watermark_slot_removals_total",
          "dfkv_slab_lifetime_donor_released_residents_total"}) {
      EXPECT_NE(text.find(std::string(name) + " 0\n"), std::string::npos);
    }
    for (size_t disk = 0; disk < 2; ++disk) {
      const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                 "\",disk_index=\"" + std::to_string(disk) +
                                 "\"} ";
      EXPECT_NE(text.find("dfkv_slab_disk_lifetime_class_victims_total" +
                          labels + std::to_string(victims) + "\n"),
                std::string::npos);
      EXPECT_NE(
          text.find("dfkv_slab_disk_lifetime_class_freed_payload_bytes_total" +
                    labels + std::to_string((disk == 0 ? 17 : 31) * victims) +
                    "\n"),
          std::string::npos);
      EXPECT_NE(
          text.find("dfkv_slab_disk_lifetime_class_freed_slot_bytes_total" +
                    labels + std::to_string(134217728 * victims) + "\n"),
          std::string::npos);
    }
  }
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, DonorBytesAggregateAcrossDisksAndResetOnRecovery) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "1048576");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_donor";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
  ASSERT_EQ(group.DiskCount(), 2U);
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
    const std::string old_value(disk == 0 ? 17 : 31, 'v');
    for (uint64_t key = 1; key <= 2; ++key) {
      ASSERT_EQ(store.Cache({key, 0}, old_value.data(), old_value.size()),
                Status::kOk);
    }
    // A different class must steal the only extent, even though the donor
    // has many free slots. Count occupied slots, not the whole 1 GiB extent.
    const std::string incoming(1048577, 'n');
    ASSERT_EQ(store.Cache({3, 0}, incoming.data(), incoming.size()),
              Status::kOk);
    EXPECT_FALSE(store.IsCached({1, 0}));
    EXPECT_FALSE(store.IsCached({2, 0}));
    // Neither a repeated PUT nor a free slot in the new class is a donor.
    ASSERT_EQ(store.Cache({3, 0}, incoming.data(), incoming.size()),
              Status::kOk);
    ASSERT_EQ(store.Cache({4, 0}, incoming.data(), incoming.size()),
              Status::kOk);
    EXPECT_EQ(store.Count(), 2U);
  }
  const std::string text = server->MetricsText();
  EXPECT_NE(text.find("dfkv_slab_lifetime_donor_released_residents_total 4\n"),
            std::string::npos);
  EXPECT_NE(
      text.find("dfkv_slab_lifetime_donor_freed_payload_bytes_total 96\n"),
      std::string::npos);
  EXPECT_NE(
      text.find("dfkv_slab_lifetime_donor_freed_slot_bytes_total 4194304\n"),
      std::string::npos);
  EXPECT_NE(text.find("dfkv_evicted_bytes_total 96\n"), std::string::npos);
  // These NEW residents are still above KEEP. Donor release can evict them,
  // but must report their protection separately from missing scoring state.
  EXPECT_NE(text.find("dfkv_slab_lifetime_donor_protected_victims_total 4\n"),
            std::string::npos);
  EXPECT_NE(
      text.find("dfkv_slab_lifetime_donor_protected_payload_bytes_total 96\n"),
      std::string::npos);
  EXPECT_NE(text.find("dfkv_slab_lifetime_donor_unscored_victims_total 0\n"),
            std::string::npos);
  for (const char* name : {"dfkv_slab_lifetime_class_victims_total",
                           "dfkv_slab_lifetime_background_victims_total",
                           "dfkv_slab_watermark_slot_removals_total"}) {
    EXPECT_NE(text.find(std::string(name) + " 0\n"), std::string::npos);
  }
  for (size_t disk = 0; disk < 2; ++disk) {
    const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                               "\",disk_index=\"" + std::to_string(disk) +
                               "\"} ";
    EXPECT_NE(
        text.find("dfkv_slab_disk_lifetime_donor_released_residents_total" +
                  labels + "2\n"),
        std::string::npos);
    EXPECT_NE(
        text.find("dfkv_slab_disk_lifetime_donor_freed_payload_bytes_total" +
                  labels + std::to_string(disk == 0 ? 34 : 62) + "\n"),
        std::string::npos);
    EXPECT_NE(text.find("dfkv_slab_disk_lifetime_donor_freed_slot_bytes_total" +
                        labels + "2097152\n"),
              std::string::npos);
    EXPECT_NE(
        text.find("dfkv_slab_disk_lifetime_donor_protected_victims_total" +
                  labels + "2\n"),
        std::string::npos);
    EXPECT_NE(
        text.find(
            "dfkv_slab_disk_lifetime_donor_protected_payload_bytes_total" +
            labels + std::to_string(disk == 0 ? 34 : 62) + "\n"),
        std::string::npos);
    EXPECT_NE(text.find("dfkv_slab_disk_lifetime_donor_unscored_victims_total" +
                        labels + "0\n"),
              std::string::npos);
  }
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  const std::string recovered = server->MetricsText();
  for (const char* name :
       {"dfkv_slab_lifetime_donor_released_residents_total",
        "dfkv_slab_lifetime_donor_freed_payload_bytes_total",
        "dfkv_slab_lifetime_donor_freed_slot_bytes_total",
        "dfkv_slab_lifetime_donor_protected_victims_total",
        "dfkv_slab_lifetime_donor_protected_payload_bytes_total",
        "dfkv_slab_lifetime_donor_unscored_victims_total"}) {
    EXPECT_NE(recovered.find(std::string(name) + " 0\n"), std::string::npos);
  }
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(
        KvNodeServerTestPeer::Group(*server), disk);
    EXPECT_EQ(store.Count(), 2U);
    EXPECT_FALSE(store.IsCached({1, 0}));
    EXPECT_FALSE(store.IsCached({2, 0}));
    std::string value;
    EXPECT_EQ(store.Range({3, 0}, 0, 0, &value), Status::kOk);
    EXPECT_EQ(value, std::string(1048577, 'n'));
    const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                               "\",disk_index=\"" + std::to_string(disk) +
                               "\"} 0\n";
    for (const char* name :
         {"dfkv_slab_disk_lifetime_donor_released_residents_total",
          "dfkv_slab_disk_lifetime_donor_freed_payload_bytes_total",
          "dfkv_slab_disk_lifetime_donor_freed_slot_bytes_total",
          "dfkv_slab_disk_lifetime_donor_protected_victims_total",
          "dfkv_slab_disk_lifetime_donor_protected_payload_bytes_total",
          "dfkv_slab_disk_lifetime_donor_unscored_victims_total"}) {
      EXPECT_NE(recovered.find(std::string(name) + labels), std::string::npos);
    }
  }
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, AllocatorGaugesDistinguishPinnedRemovalFromSlotSlack) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_allocator";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
  ASSERT_EQ(group.DiskCount(), 2U);
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
    const std::string value(disk == 0 ? 17 : 31, 'v');
    ASSERT_EQ(store.Cache({1, 0}, value.data(), value.size()), Status::kOk);
  }
  // Check both accounting views against real pinned/removal transitions.
  // No synthetic Stats or resident mutation is used by this test.
  const auto expect_snapshot = [&](const std::string& text,
                                   const std::string& prefix,
                                   const std::string& labels, uint64_t visible,
                                   uint64_t resident, uint64_t allocated) {
    EXPECT_NE(text.find(prefix + "payload_bytes" + labels + " " +
                        std::to_string(visible) + "\n"),
              std::string::npos);
    EXPECT_NE(text.find(prefix + "allocated_bytes" + labels + " " +
                        std::to_string(allocated) + "\n"),
              std::string::npos);
    EXPECT_NE(text.find(prefix + "allocator_payload_bytes" + labels + " " +
                        std::to_string(resident) + "\n"),
              std::string::npos);
    EXPECT_NE(text.find(prefix + "allocator_fragmentation_bytes" + labels +
                        " " + std::to_string(allocated - resident) + "\n"),
              std::string::npos);
  };
  const auto labels = [&](size_t disk) {
    return "{disk=\"" + options.cache_dirs[disk] + "\",disk_index=\"" +
           std::to_string(disk) + "\"}";
  };
  const std::string initial = server->MetricsText();
  expect_snapshot(initial, "dfkv_slab_", "", 48, 48, 8192);
  expect_snapshot(initial, "dfkv_slab_disk_", labels(0), 17, 17, 4096);
  expect_snapshot(initial, "dfkv_slab_disk_", labels(1), 31, 31, 4096);
  DiskSlabStore& removed = DiskCacheGroupTestPeer::Slab(group, 0);
  ReadLease lease;
  ASSERT_EQ(removed.RangeDirectPrep({1, 0}, 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(removed.Remove({1, 0}), Status::kOk);
  EXPECT_FALSE(removed.IsCached({1, 0}));
  const std::string hidden = server->MetricsText();
  expect_snapshot(hidden, "dfkv_slab_", "", 31, 48, 8192);
  expect_snapshot(hidden, "dfkv_slab_disk_", labels(0), 0, 17, 4096);
  expect_snapshot(hidden, "dfkv_slab_disk_", labels(1), 31, 31, 4096);
  // Preserve the old allocated-visible gap: it includes the hidden 17 bytes,
  // unlike the new resident slot slack (8192 - 48 = 8144).
  EXPECT_NE(hidden.find("dfkv_slab_internal_fragmentation_bytes 8161\n"),
            std::string::npos);
  lease = ReadLease{};
  const std::string released = server->MetricsText();
  expect_snapshot(released, "dfkv_slab_", "", 31, 31, 4096);
  expect_snapshot(released, "dfkv_slab_disk_", labels(0), 0, 0, 0);
  expect_snapshot(released, "dfkv_slab_disk_", labels(1), 31, 31, 4096);
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  const std::string recovered = server->MetricsText();
  expect_snapshot(recovered, "dfkv_slab_", "", 31, 31, 4096);
  expect_snapshot(recovered, "dfkv_slab_disk_", labels(0), 0, 0, 0);
  expect_snapshot(recovered, "dfkv_slab_disk_", labels(1), 31, 31, 4096);
  DiskSlabStore& survivor =
      DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 1);
  std::string value;
  EXPECT_EQ(survivor.Range({1, 0}, 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, std::string(31, 'v'));
  ASSERT_EQ(survivor.Remove({1, 0}), Status::kOk);
  const std::string empty = server->MetricsText();
  expect_snapshot(empty, "dfkv_slab_", "", 0, 0, 0);
  expect_snapshot(empty, "dfkv_slab_disk_", labels(0), 0, 0, 0);
  expect_snapshot(empty, "dfkv_slab_disk_", labels(1), 0, 0, 0);
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, MetadataArrayBytesTrackRetentionAndRecoveryAcrossDisks) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_metadata";
  const std::array<const char*, 6> suffixes = {
      "slot_bytes",      "extent_bytes",         "class_bytes",
      "key_index_bytes", "class_rotation_bytes", "class_free_slot_bytes"};
  const auto bytes = [](const DiskSlabStore::Stats& stats) {
    return std::array<uint64_t, 6>{stats.metadata_slot_bytes,
                                   stats.metadata_extent_bytes,
                                   stats.metadata_class_bytes,
                                   stats.metadata_key_index_bytes,
                                   stats.metadata_class_rotation_bytes,
                                   stats.metadata_class_free_slot_bytes};
  };
  for (const char* policy : {"legacy", "lifetime"}) {
    SCOPED_TRACE(policy);
    fs::remove_all(dir);
    DiskCacheGroup::Options options;
    options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
    options.capacity_bytes = 4ULL << 30;
    options.engine = "slab";
    options.eviction_policy = policy;
    std::unique_ptr<KvNodeServer> server =
        std::make_unique<KvNodeServer>(options);
    ASSERT_TRUE(server->Healthy());
    std::vector<BlockKey> keys[2];
    for (uint64_t id = 1;
         id < 10000 && (keys[0].size() < 17 || keys[1].empty()); ++id) {
      const BlockKey key{id, 0};
      const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
          KvNodeServerTestPeer::Group(*server), key);
      ASSERT_LT(disk, 2U);
      if ((disk == 0 && keys[0].size() < 17) ||
          (disk == 1 && keys[1].empty())) {
        keys[disk].push_back(key);
      }
    }
    ASSERT_EQ(keys[0].size(), 17U);
    ASSERT_EQ(keys[1].size(), 1U);
    const auto expect_metrics = [&] {
      const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
      const std::string text = server->MetricsText();
      std::array<uint64_t, 6> totals{};
      for (size_t disk = 0; disk < 2; ++disk) {
        const std::array<uint64_t, 6> current =
            bytes(group.DiskSlabStats(disk));
        // Both base geometry arrays exist before the first resident, in
        // legacy as well as lifetime mode. Slot/index arrays grow on demand.
        EXPECT_GT(current[1], 0U);
        EXPECT_GT(current[2], 0U);
        const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                   "\",disk_index=\"" + std::to_string(disk) +
                                   "\"} ";
        for (size_t field = 0; field < suffixes.size(); ++field) {
          const std::string name =
              std::string("dfkv_slab_disk_metadata_") + suffixes[field];
          EXPECT_NE(text.find("# TYPE " + name + " gauge\n"),
                    std::string::npos);
          EXPECT_NE(text.find("\n" + name + labels +
                              std::to_string(current[field]) + "\n"),
                    std::string::npos);
          totals[field] += current[field];
        }
      }
      EXPECT_EQ(bytes(group.SlabStats()), totals);
      for (size_t field = 0; field < suffixes.size(); ++field) {
        const std::string name =
            std::string("dfkv_slab_metadata_") + suffixes[field];
        EXPECT_NE(text.find("# TYPE " + name + " gauge\n"), std::string::npos);
        EXPECT_NE(
            text.find("\n" + name + " " + std::to_string(totals[field]) + "\n"),
            std::string::npos);
      }
    };
    ASSERT_NO_FATAL_FAILURE(expect_metrics());
    EXPECT_EQ(KvNodeServerTestPeer::Group(*server)
                  .DiskSlabStats(0)
                  .metadata_slot_bytes,
              0U);
    for (size_t disk = 0; disk < 2; ++disk) {
      const DiskSlabStore::Stats empty =
          KvNodeServerTestPeer::Group(*server).DiskSlabStats(disk);
      EXPECT_EQ(empty.metadata_class_rotation_bytes, 0U);
      EXPECT_EQ(empty.metadata_class_free_slot_bytes, 0U);
    }
    const std::array<std::string, 2> payloads = {std::string(31, 'm'),
                                                 std::string(65537, 'n')};
    for (size_t disk = 0; disk < 2; ++disk) {
      const std::string& payload = payloads[disk];
      for (const BlockKey& key : keys[disk]) {
        ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(
                      key, payload.data(), payload.size()),
                  Status::kOk);
      }
    }
    ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Count(), 18U);
    ASSERT_NO_FATAL_FAILURE(expect_metrics());
    const std::array<uint64_t, 6> larger =
        bytes(KvNodeServerTestPeer::Group(*server).DiskSlabStats(0));
    const std::array<uint64_t, 6> smaller =
        bytes(KvNodeServerTestPeer::Group(*server).DiskSlabStats(1));
    EXPECT_GT(larger[0], smaller[0]);
    EXPECT_GT(smaller[0], 0U);
    EXPECT_GT(larger[3], smaller[3]);
    EXPECT_GT(smaller[3], 0U);
    EXPECT_GT(larger[4], 0U);
    EXPECT_GT(smaller[4], 0U);
    EXPECT_GT(larger[5], smaller[5]);
    EXPECT_GT(smaller[5], 0U);
    DiskSlabStore& removed =
        DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 0);
    for (const BlockKey& key : keys[0]) {
      ASSERT_EQ(removed.Remove(key), Status::kOk);
    }
    EXPECT_EQ(removed.Count(), 0U);
    EXPECT_EQ(bytes(removed.GetStats()), larger);
    ASSERT_NO_FATAL_FAILURE(expect_metrics());
    server.reset();
    server = std::make_unique<KvNodeServer>(options);
    ASSERT_TRUE(server->Healthy());
    ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Count(), 1U);
    ASSERT_NO_FATAL_FAILURE(expect_metrics());
    // Recovery reconstructs backing from surviving records. It does not
    // persist vector capacity or promise the previous allocation strategy.
    EXPECT_EQ(KvNodeServerTestPeer::Group(*server)
                  .DiskSlabStats(0)
                  .metadata_slot_bytes,
              0U);
    const DiskSlabStore::Stats rebuilt_empty =
        KvNodeServerTestPeer::Group(*server).DiskSlabStats(0);
    EXPECT_EQ(rebuilt_empty.metadata_class_rotation_bytes, 0U);
    EXPECT_EQ(rebuilt_empty.metadata_class_free_slot_bytes, 0U);
    DiskSlabStore& survivor =
        DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 1);
    const std::array<uint64_t, 6> recovered = bytes(survivor.GetStats());
    EXPECT_GT(recovered[0], 0U);
    EXPECT_GT(recovered[3], 0U);
    EXPECT_GT(recovered[4], 0U);
    EXPECT_GT(recovered[5], 0U);
    std::string value;
    ASSERT_EQ(survivor.Range(keys[1][0], 0, 0, &value), Status::kOk);
    EXPECT_EQ(value, payloads[1]);
    ReadLease lease;
    ASSERT_EQ(survivor.RangeDirectPrep(keys[1][0], 0, 1, 4096, &lease),
              Status::kOk);
    ASSERT_EQ(survivor.Remove(keys[1][0]), Status::kOk);
    EXPECT_FALSE(survivor.IsCached(keys[1][0]));
    EXPECT_EQ(bytes(survivor.GetStats()), recovered);
    DiskSlabStoreTestPeer::FailMetadata(survivor);
    lease = ReadLease{};
    ASSERT_FALSE(survivor.Healthy());
    EXPECT_EQ(survivor.Count(), 1U);
    EXPECT_EQ(bytes(survivor.GetStats()), recovered);
    ASSERT_NO_FATAL_FAILURE(expect_metrics());
    server.reset();
  }
  fs::remove_all(dir);
}

TEST(Metrics, LifetimeBucketGaugesIncludeEmptyExtentsAndRetainedCapacity) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const ScopedEnv buckets("DFKV_SLAB_BUCKET_COUNT", "32");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_buckets";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 4ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  const auto expect_resources = [&](bool tokens0, bool tokens1,
                                    uint64_t visible0, uint64_t visible1) {
    const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    ASSERT_EQ(group.DiskCount(), 2U);
    const std::string text = server->MetricsText();
    uint64_t bucket_bytes = 0;
    uint64_t token_bytes = 0;
    DiskSlabStore::Stats index_totals;
    const auto expect_index_memory = [&](const char* suffix, uint64_t bytes,
                                         const std::string& labels) {
      const std::string name =
          std::string(labels.empty() ? "dfkv_slab_lifetime_"
                                     : "dfkv_slab_disk_lifetime_") +
          suffix;
      EXPECT_NE(text.find("# TYPE " + name + " gauge\n"), std::string::npos);
      EXPECT_NE(text.find(name + (labels.empty() ? " " : labels) +
                          std::to_string(bytes) + "\n"),
                std::string::npos);
    };
    for (size_t disk = 0; disk < 2; ++disk) {
      const DiskSlabStore::Stats stats = group.DiskSlabStats(disk);
      // Two physical extents each reserve B=32 entries before any binding.
      EXPECT_EQ(stats.lifetime_bucket_entries, 64U);
      EXPECT_GT(stats.lifetime_bucket_bytes, 0U);
      EXPECT_EQ(stats.lifetime_bucket_token_bytes != 0,
                disk == 0 ? tokens0 : tokens1);
      const bool has_resident_capacity = disk == 0 ? tokens0 : tokens1;
      EXPECT_EQ(stats.lifetime_identity_bytes != 0, has_resident_capacity);
      EXPECT_EQ(stats.lifetime_state_bytes != 0, has_resident_capacity);
      EXPECT_EQ(stats.lifetime_link_bytes != 0, has_resident_capacity);
      // Queue heads and class versions exist even before the first resident.
      EXPECT_GT(stats.lifetime_queue_bytes, 0U);
      EXPECT_GT(stats.lifetime_version_bytes, 0U);
      const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                 "\",disk_index=\"" + std::to_string(disk) +
                                 "\"} ";
      EXPECT_NE(
          text.find("dfkv_slab_disk_lifetime_bucket_entries" + labels + "64\n"),
          std::string::npos);
      EXPECT_NE(text.find("dfkv_slab_disk_lifetime_bucket_bytes" + labels +
                          std::to_string(stats.lifetime_bucket_bytes) + "\n"),
                std::string::npos);
      EXPECT_NE(
          text.find("dfkv_slab_disk_lifetime_bucket_token_bytes" + labels +
                    std::to_string(stats.lifetime_bucket_token_bytes) + "\n"),
          std::string::npos);
      EXPECT_NE(
          text.find("dfkv_slab_disk_payload_bytes" + labels +
                    std::to_string(disk == 0 ? visible0 : visible1) + "\n"),
          std::string::npos);
      expect_index_memory("identity_bytes", stats.lifetime_identity_bytes,
                          labels);
      expect_index_memory("state_bytes", stats.lifetime_state_bytes, labels);
      expect_index_memory("link_bytes", stats.lifetime_link_bytes, labels);
      expect_index_memory("queue_bytes", stats.lifetime_queue_bytes, labels);
      expect_index_memory("version_bytes", stats.lifetime_version_bytes,
                          labels);
      index_totals.lifetime_identity_bytes += stats.lifetime_identity_bytes;
      index_totals.lifetime_state_bytes += stats.lifetime_state_bytes;
      index_totals.lifetime_link_bytes += stats.lifetime_link_bytes;
      index_totals.lifetime_queue_bytes += stats.lifetime_queue_bytes;
      index_totals.lifetime_version_bytes += stats.lifetime_version_bytes;
      bucket_bytes += stats.lifetime_bucket_bytes;
      token_bytes += stats.lifetime_bucket_token_bytes;
    }
    const DiskSlabStore::Stats group_stats = group.SlabStats();
    EXPECT_EQ(group_stats.lifetime_identity_bytes,
              index_totals.lifetime_identity_bytes);
    expect_index_memory("identity_bytes", index_totals.lifetime_identity_bytes,
                        "");
    EXPECT_EQ(group_stats.lifetime_state_bytes,
              index_totals.lifetime_state_bytes);
    expect_index_memory("state_bytes", index_totals.lifetime_state_bytes, "");
    EXPECT_EQ(group_stats.lifetime_link_bytes,
              index_totals.lifetime_link_bytes);
    expect_index_memory("link_bytes", index_totals.lifetime_link_bytes, "");
    EXPECT_EQ(group_stats.lifetime_queue_bytes,
              index_totals.lifetime_queue_bytes);
    expect_index_memory("queue_bytes", index_totals.lifetime_queue_bytes, "");
    EXPECT_EQ(group_stats.lifetime_version_bytes,
              index_totals.lifetime_version_bytes);
    expect_index_memory("version_bytes", index_totals.lifetime_version_bytes,
                        "");
    EXPECT_NE(text.find("dfkv_slab_lifetime_bucket_entries 128\n"),
              std::string::npos);
    EXPECT_NE(text.find("dfkv_slab_lifetime_bucket_bytes " +
                        std::to_string(bucket_bytes) + "\n"),
              std::string::npos);
    EXPECT_NE(text.find("dfkv_slab_lifetime_bucket_token_bytes " +
                        std::to_string(token_bytes) + "\n"),
              std::string::npos);
    EXPECT_NE(text.find("dfkv_slab_payload_bytes " +
                        std::to_string(visible0 + visible1) + "\n"),
              std::string::npos);
  };
  ASSERT_NO_FATAL_FAILURE(expect_resources(false, false, 0, 0));
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(
        KvNodeServerTestPeer::Group(*server), disk);
    const std::string value(disk == 0 ? 17 : 31, 'v');
    for (uint64_t key = 1; key <= 3; ++key) {
      ASSERT_EQ(store.Cache({key, 0}, value.data(), value.size()), Status::kOk);
    }
  }
  ASSERT_NO_FATAL_FAILURE(expect_resources(true, true, 51, 93));
  const DiskSlabStore::Stats before_remove =
      KvNodeServerTestPeer::Group(*server).DiskSlabStats(0);
  DiskSlabStore& removed =
      DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 0);
  for (uint64_t key = 1; key <= 3; ++key) {
    ASSERT_EQ(removed.Remove({key, 0}), Status::kOk);
  }
  // Removing residents clears registrations, not reserved vector capacity.
  ASSERT_NO_FATAL_FAILURE(expect_resources(true, true, 0, 93));
  const DiskSlabStore::Stats after_remove =
      KvNodeServerTestPeer::Group(*server).DiskSlabStats(0);
  EXPECT_EQ(after_remove.lifetime_bucket_bytes,
            before_remove.lifetime_bucket_bytes);
  EXPECT_EQ(after_remove.lifetime_bucket_token_bytes,
            before_remove.lifetime_bucket_token_bytes);
  EXPECT_EQ(after_remove.lifetime_identity_bytes,
            before_remove.lifetime_identity_bytes);
  EXPECT_EQ(after_remove.lifetime_state_bytes,
            before_remove.lifetime_state_bytes);
  EXPECT_EQ(after_remove.lifetime_link_bytes,
            before_remove.lifetime_link_bytes);
  EXPECT_EQ(after_remove.lifetime_queue_bytes,
            before_remove.lifetime_queue_bytes);
  EXPECT_EQ(after_remove.lifetime_version_bytes,
            before_remove.lifetime_version_bytes);
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  // A fresh empty allocator has no resident token backing. The live disk's
  // capacity is rebuilt, not persisted or promised to equal its former value.
  ASSERT_NO_FATAL_FAILURE(expect_resources(false, true, 0, 93));
  const DiskSlabStore::Stats rebuilt =
      KvNodeServerTestPeer::Group(*server).DiskSlabStats(1);
  DiskSlabStore& survivor =
      DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 1);
  for (uint64_t key = 1; key <= 3; ++key) {
    std::string value;
    EXPECT_EQ(survivor.Range({key, 0}, 0, 0, &value), Status::kOk);
    EXPECT_EQ(value, std::string(31, 'v'));
    ASSERT_EQ(survivor.Remove({key, 0}), Status::kOk);
  }
  ASSERT_NO_FATAL_FAILURE(expect_resources(false, true, 0, 0));
  const DiskSlabStore::Stats empty =
      KvNodeServerTestPeer::Group(*server).DiskSlabStats(1);
  EXPECT_EQ(empty.lifetime_bucket_bytes, rebuilt.lifetime_bucket_bytes);
  EXPECT_EQ(empty.lifetime_bucket_token_bytes,
            rebuilt.lifetime_bucket_token_bytes);
  EXPECT_EQ(empty.lifetime_identity_bytes, rebuilt.lifetime_identity_bytes);
  EXPECT_EQ(empty.lifetime_state_bytes, rebuilt.lifetime_state_bytes);
  EXPECT_EQ(empty.lifetime_link_bytes, rebuilt.lifetime_link_bytes);
  EXPECT_EQ(empty.lifetime_queue_bytes, rebuilt.lifetime_queue_bytes);
  EXPECT_EQ(empty.lifetime_version_bytes, rebuilt.lifetime_version_bytes);
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, LifetimeBucketEstimateTracksExpiryRetentionAndRecovery) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv coalesce("DFKV_READ_COALESCE", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const DiskLifetimeConfig config = BucketMetricConfig();
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  std::atomic<uint64_t> seconds{0};
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_metrics_bucket_estimate";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 4ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  options.lifetime_config = config;
  options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
      DiskLifetimeHorizon(config), [&] { return seconds.load(); });
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  std::vector<BlockKey> keys[2];
  for (uint64_t id = 1; id < 10000 && (keys[0].size() < 2 || keys[1].empty());
       ++id) {
    const BlockKey key{id, 0};
    const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
        KvNodeServerTestPeer::Group(*server), key);
    ASSERT_LT(disk, 2U);
    if ((disk == 0 && keys[0].size() < 2) || (disk == 1 && keys[1].empty()))
      keys[disk].push_back(key);
  }
  ASSERT_EQ(keys[0].size(), 2U);
  ASSERT_EQ(keys[1].size(), 1U);
  const auto expect_buckets = [&](uint64_t disk0, uint64_t disk1) {
    const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    const std::string text = server->MetricsText();
    uint64_t summary_bytes = 0;
    for (size_t disk = 0; disk < 2; ++disk) {
      const SlabAllocator::LifetimeBucketObservation observation =
          group.DiskLifetimeBucketObservation(disk);
      EXPECT_TRUE(observation.available);
      EXPECT_EQ(observation.protected_estimate, disk == 0 ? disk0 : disk1);
      EXPECT_GT(observation.summary_bytes, 0U);
      summary_bytes += observation.summary_bytes;
      const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                 "\",disk_index=\"" + std::to_string(disk) +
                                 "\"} ";
      EXPECT_NE(
          text.find("\ndfkv_slab_disk_lifetime_bucket_estimate_available" +
                    labels + "1\n"),
          std::string::npos);
      EXPECT_NE(
          text.find("\ndfkv_slab_disk_lifetime_bucket_summary_bytes" + labels +
                    std::to_string(observation.summary_bytes) + "\n"),
          std::string::npos);
      EXPECT_NE(
          text.find("\ndfkv_slab_disk_lifetime_bucket_protected_estimate" +
                    labels + std::to_string(disk == 0 ? disk0 : disk1) + "\n"),
          std::string::npos);
    }
    EXPECT_NE(text.find("\ndfkv_slab_lifetime_bucket_estimate_available 1\n"),
              std::string::npos);
    EXPECT_NE(text.find("\ndfkv_slab_lifetime_bucket_summary_bytes " +
                        std::to_string(summary_bytes) + "\n"),
              std::string::npos);
    EXPECT_NE(text.find("\ndfkv_slab_lifetime_bucket_protected_estimate " +
                        std::to_string(disk0 + disk1) + "\n"),
              std::string::npos);
    for (const char* name :
         {"dfkv_slab_lifetime_bucket_protected_estimate",
          "dfkv_slab_lifetime_bucket_estimate_available",
          "dfkv_slab_lifetime_bucket_summary_bytes",
          "dfkv_slab_disk_lifetime_bucket_protected_estimate",
          "dfkv_slab_disk_lifetime_bucket_estimate_available",
          "dfkv_slab_disk_lifetime_bucket_summary_bytes"}) {
      EXPECT_NE(text.find(std::string("# TYPE ") + name + " gauge\n"),
                std::string::npos);
    }
    EXPECT_FALSE(
        group.DiskLifetimeBucketObservation(group.DiskCount()).available);
  };
  ASSERT_NO_FATAL_FAILURE(expect_buckets(0, 0));
  for (const std::vector<BlockKey>& disk_keys : keys) {
    for (const BlockKey& key : disk_keys)
      ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(key, "value", 5),
                Status::kOk);
  }
  ASSERT_EQ(server->Count(), 3U);
  ASSERT_NO_FATAL_FAILURE(expect_buckets(2, 1));
  const uint64_t exact_end = DiskLifetimeProtectEnd(config, 0, 0, false, true);
  const uint64_t bucket_end =
      ((exact_end - 1) / config.bucket_sec + 1) * config.bucket_sec;
  ASSERT_GT(bucket_end, exact_end);
  seconds = exact_end;
  // Exact protection has ended, but the bucket remains conservatively live.
  ASSERT_NO_FATAL_FAILURE(expect_buckets(2, 1));
  seconds = bucket_end;
  ASSERT_NO_FATAL_FAILURE(expect_buckets(0, 0));
  const auto read = [&](const BlockKey& key) {
    std::string value;
    ASSERT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                           key, 0, 0, nullptr, 0, &value),
              Status::kOk);
    EXPECT_EQ(value, "value");
  };
  ASSERT_NO_FATAL_FAILURE(read(keys[0][0]));
  ASSERT_NO_FATAL_FAILURE(read(keys[1][0]));
  ASSERT_NO_FATAL_FAILURE(expect_buckets(1, 1));
  ReadLease lease;
  DiskSlabStore& disk0 =
      DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 0);
  ASSERT_EQ(disk0.RangeDirectPrep(keys[0][0], 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(disk0.Remove(keys[0][0]), Status::kOk);
  EXPECT_FALSE(disk0.IsCached(keys[0][0]));
  ASSERT_NO_FATAL_FAILURE(expect_buckets(1, 1));
  lease = ReadLease{};
  ASSERT_TRUE(disk0.Healthy());
  ASSERT_NO_FATAL_FAILURE(expect_buckets(0, 1));
  server.reset();
  seconds = 0;
  options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
      DiskLifetimeHorizon(config), [&] { return seconds.load(); });
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  ASSERT_EQ(server->Count(), 2U);
  // Fresh recovery seeds protection without inventing business reads.
  ASSERT_NO_FATAL_FAILURE(expect_buckets(1, 1));
  EXPECT_EQ(KvNodeServerTestPeer::Group(*server)
                .SlabStats()
                .lifetime_reads.successful,
            0U);
  DiskSlabStore& disk1 =
      DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 1);
  ASSERT_EQ(disk1.RangeDirectPrep(keys[1][0], 0, 0, 4096, &lease), Status::kOk);
  ASSERT_EQ(disk1.Remove(keys[1][0]), Status::kOk);
  DiskSlabStoreTestPeer::FailMetadata(disk1);
  lease = ReadLease{};
  EXPECT_FALSE(disk1.Healthy());
  ASSERT_NO_FATAL_FAILURE(expect_buckets(1, 1));
  KvNodeServerTestPeer::Group(*server).PreventCleanShutdown();
  server.reset();
  options.lifetime_clock = std::make_shared<DiskLifetimeClock>(
      DiskLifetimeHorizon(config), [&] { return seconds.load(); });
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  EXPECT_EQ(server->Count(), 0U);
  ASSERT_NO_FATAL_FAILURE(expect_buckets(0, 0));
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, LifetimeBucketEstimateOmitsUnknownAcrossClockBoundaries) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const DiskLifetimeConfig config = BucketMetricConfig();
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(config, &error)) << error;
  std::atomic<uint64_t> seconds{0};
  std::atomic<bool> cross_hard{false};
  std::atomic<unsigned int> samples{0};
  const uint64_t maximum = std::numeric_limits<uint32_t>::max();
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_bucket_clock";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 4ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  options.lifetime_config = config;
  options.lifetime_clock =
      std::make_shared<DiskLifetimeClock>(DiskLifetimeHorizon(config), [&] {
        if (cross_hard.load())
          return samples.fetch_add(1) == 0 ? maximum : maximum + 1;
        return seconds.load();
      });
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  const SlabAllocator::LifetimeBucketObservation initial =
      KvNodeServerTestPeer::Group(*server).DiskLifetimeBucketObservation(0);
  ASSERT_TRUE(initial.available);
  ASSERT_GT(initial.summary_bytes, 0U);
  const auto expect_text = [&](const std::string& text, bool first_available,
                               bool second_available) {
    for (size_t disk = 0; disk < 2; ++disk) {
      const bool available = disk == 0 ? first_available : second_available;
      const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                 "\",disk_index=\"" + std::to_string(disk) +
                                 "\"} ";
      EXPECT_NE(
          text.find("\ndfkv_slab_disk_lifetime_bucket_estimate_available" +
                    labels + (available ? "1\n" : "0\n")),
          std::string::npos);
      EXPECT_NE(
          text.find("\ndfkv_slab_disk_lifetime_bucket_summary_bytes" + labels +
                    std::to_string(initial.summary_bytes) + "\n"),
          std::string::npos);
      const std::string estimate =
          "\ndfkv_slab_disk_lifetime_bucket_protected_estimate" + labels;
      if (available)
        EXPECT_NE(text.find(estimate + "0\n"), std::string::npos);
      else
        EXPECT_EQ(text.find(estimate), std::string::npos);
    }
    const bool all_available = first_available && second_available;
    EXPECT_NE(text.find("\ndfkv_slab_lifetime_bucket_estimate_available " +
                        std::string(all_available ? "1\n" : "0\n")),
              std::string::npos);
    EXPECT_NE(text.find("\ndfkv_slab_lifetime_bucket_summary_bytes " +
                        std::to_string(2 * initial.summary_bytes) + "\n"),
              std::string::npos);
    const std::string estimate =
        "\ndfkv_slab_lifetime_bucket_protected_estimate ";
    if (all_available)
      EXPECT_NE(text.find(estimate + "0\n"), std::string::npos);
    else
      EXPECT_EQ(text.find(estimate), std::string::npos);
  };
  seconds = maximum - DiskLifetimeHorizon(config) + 1;
  ASSERT_NO_FATAL_FAILURE(expect_text(server->MetricsText(), true, true));
  uint32_t offset = 0;
  EXPECT_FALSE(options.lifetime_clock->TryNow(&offset));
  // A real shared-clock crossing occurs between the two disk observations.
  samples = 0;
  cross_hard = true;
  const std::string mixed = server->MetricsText();
  EXPECT_EQ(samples.load(), 2U);
  ASSERT_NO_FATAL_FAILURE(expect_text(mixed, true, false));
  cross_hard = false;
  seconds = 0;
  ASSERT_NO_FATAL_FAILURE(expect_text(server->MetricsText(), false, false));
  EXPECT_FALSE(options.lifetime_clock->TryNow(&offset));
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, LifetimeQueueGaugesTrackBothIndexesAcrossReadsAndRecovery) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv coalesce("DFKV_READ_COALESCE", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const ScopedEnv max_reads("DFKV_SLAB_LIFE_MAX_READS", "4");
  const ScopedEnv buckets("DFKV_SLAB_BUCKET_COUNT", "64");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_queues";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 4ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  std::vector<BlockKey> keys[2];
  for (uint64_t id = 1; id < 10000 && (keys[0].size() < 2 || keys[1].empty());
       ++id) {
    const BlockKey key{id, 0};
    const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
        KvNodeServerTestPeer::Group(*server), key);
    ASSERT_LT(disk, 2U);
    if ((disk == 0 && keys[0].size() < 2) || (disk == 1 && keys[1].empty()))
      keys[disk].push_back(key);
  }
  ASSERT_EQ(keys[0].size(), 2U);
  ASSERT_EQ(keys[1].size(), 1U);
  using QueueKey = std::pair<uint32_t, uint32_t>;
  using QueueCounts = std::map<QueueKey, uint64_t>;
  const auto labels = [](uint32_t slot_size, uint32_t band) {
    return std::string("index=\"") + (slot_size == 0 ? "global" : "class") +
           "\",slot_size=\"" + std::to_string(slot_size) + "\",band=\"" +
           std::to_string(band) + "\"";
  };
  const auto expect_queues = [&](const std::array<uint64_t, 5>& small0,
                                 const std::array<uint64_t, 5>& large0,
                                 const std::array<uint64_t, 5>& small1) {
    const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    const std::string text = server->MetricsText();
    QueueCounts totals;
    for (size_t disk = 0; disk < 2; ++disk) {
      const std::array<uint64_t, 5>& small = disk == 0 ? small0 : small1;
      uint64_t small_total = 0;
      uint64_t large_total = 0;
      for (uint32_t band = 0; band <= 4; ++band) {
        small_total += small[band];
        if (disk == 0) large_total += large0[band];
      }
      QueueCounts expected;
      for (uint32_t band = 0; band <= 4; ++band) {
        expected[{0, band}] = small[band] + (disk == 0 ? large0[band] : 0);
        if (small_total != 0) expected[{4096, band}] = small[band];
        if (large_total != 0) expected[{8192, band}] = large0[band];
      }
      const std::vector<SlabAllocator::LifetimeQueueStat> queues =
          group.DiskLifetimeQueueStats(disk);
      QueueCounts actual;
      for (const SlabAllocator::LifetimeQueueStat& queue : queues) {
        EXPECT_TRUE(
            actual
                .emplace(QueueKey{queue.slot_size, queue.band}, queue.residents)
                .second);
        const std::string disk_labels =
            "{disk=\"" + options.cache_dirs[disk] + "\",disk_index=\"" +
            std::to_string(disk) + "\"," + labels(queue.slot_size, queue.band) +
            "} ";
        EXPECT_NE(
            text.find("dfkv_slab_disk_lifetime_queue_residents" + disk_labels +
                      std::to_string(queue.residents) + "\n"),
            std::string::npos);
        totals[{queue.slot_size, queue.band}] += queue.residents;
      }
      EXPECT_EQ(actual, expected);
      for (uint32_t band = 0; band <= 4; ++band) {
        EXPECT_EQ(actual.at({0, band}),
                  (small_total == 0 ? 0 : actual.at({4096, band})) +
                      (large_total == 0 ? 0 : actual.at({8192, band})));
      }
    }
    for (const QueueCounts::value_type& entry : totals) {
      EXPECT_NE(text.find("dfkv_slab_lifetime_queue_residents{" +
                          labels(entry.first.first, entry.first.second) + "} " +
                          std::to_string(entry.second) + "\n"),
                std::string::npos);
    }
    for (const char* name : {"dfkv_slab_lifetime_queue_residents",
                             "dfkv_slab_disk_lifetime_queue_residents"}) {
      EXPECT_NE(text.find(std::string("# TYPE ") + name + " gauge\n"),
                std::string::npos);
    }
    EXPECT_EQ(text.find("band=\"5\""), std::string::npos);
  };
  const std::string small("small");
  const std::string large(5000, 'l');
  ASSERT_NO_FATAL_FAILURE(expect_queues({}, {}, {}));
  DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
  ASSERT_EQ(group.Cache(keys[0][0], small.data(), small.size()), Status::kOk);
  ASSERT_EQ(group.Cache(keys[0][1], large.data(), large.size()), Status::kOk);
  ASSERT_EQ(group.Cache(keys[1][0], small.data(), small.size()), Status::kOk);
  ASSERT_EQ(group.Count(), 3U);
  ASSERT_NO_FATAL_FAILURE(
      expect_queues({1, 0, 0, 0, 0}, {1, 0, 0, 0, 0}, {1, 0, 0, 0, 0}));
  const auto read_small = [&](const BlockKey& key) {
    std::string value;
    ASSERT_EQ(server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                           key, 0, 0, nullptr, 0, &value),
              Status::kOk);
    EXPECT_EQ(value, small);
  };
  ASSERT_NO_FATAL_FAILURE(read_small(keys[0][0]));
  // NEW's early-read bonus skips physical band 1; this is not logical O2.
  ASSERT_NO_FATAL_FAILURE(
      expect_queues({0, 0, 1, 0, 0}, {1, 0, 0, 0, 0}, {1, 0, 0, 0, 0}));
  ASSERT_NO_FATAL_FAILURE(read_small(keys[0][0]));
  ASSERT_NO_FATAL_FAILURE(
      expect_queues({0, 0, 0, 1, 0}, {1, 0, 0, 0, 0}, {1, 0, 0, 0, 0}));
  ASSERT_NO_FATAL_FAILURE(read_small(keys[0][0]));
  ASSERT_NO_FATAL_FAILURE(read_small(keys[0][0]));
  ASSERT_NO_FATAL_FAILURE(read_small(keys[1][0]));
  ASSERT_NO_FATAL_FAILURE(
      expect_queues({0, 0, 0, 0, 1}, {1, 0, 0, 0, 0}, {0, 0, 1, 0, 0}));
  DiskSlabStore& disk0 = DiskCacheGroupTestPeer::Slab(group, 0);
  ReadLease lease;
  ASSERT_EQ(disk0.RangeDirectPrep(keys[0][1], 0, 1, 4096, &lease), Status::kOk);
  ASSERT_EQ(disk0.Remove(keys[0][1]), Status::kOk);
  EXPECT_FALSE(disk0.IsCached(keys[0][1]));
  // Hidden pinned residents still belong to both physical indexes.
  ASSERT_NO_FATAL_FAILURE(
      expect_queues({0, 0, 0, 0, 1}, {1, 0, 0, 0, 0}, {0, 0, 1, 0, 0}));
  lease = ReadLease{};
  ASSERT_TRUE(disk0.Healthy());
  ASSERT_NO_FATAL_FAILURE(expect_queues({0, 0, 0, 0, 1}, {}, {0, 0, 1, 0, 0}));
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  // Clean recovery's virtual n=2 is band 2 but still zero real reads.
  ASSERT_NO_FATAL_FAILURE(expect_queues({0, 0, 1, 0, 0}, {}, {0, 0, 1, 0, 0}));
  EXPECT_EQ(KvNodeServerTestPeer::Group(*server)
                .SlabStats()
                .lifetime_reads.successful,
            0U);
  ASSERT_NO_FATAL_FAILURE(read_small(keys[0][0]));
  ASSERT_NO_FATAL_FAILURE(read_small(keys[1][0]));
  ASSERT_NO_FATAL_FAILURE(read_small(keys[1][0]));
  ASSERT_NO_FATAL_FAILURE(expect_queues({0, 0, 0, 1, 0}, {}, {0, 0, 0, 0, 1}));
  DiskSlabStore& disk1 =
      DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 1);
  ASSERT_EQ(disk1.RangeDirectPrep(keys[1][0], 0, 1, 4096, &lease), Status::kOk);
  ASSERT_EQ(disk1.Remove(keys[1][0]), Status::kOk);
  EXPECT_FALSE(disk1.IsCached(keys[1][0]));
  DiskSlabStoreTestPeer::FailMetadata(disk1);
  lease = ReadLease{};
  EXPECT_FALSE(disk1.Healthy());
  // Failed invalidation cannot debit the still-retained physical queues.
  ASSERT_NO_FATAL_FAILURE(expect_queues({0, 0, 0, 1, 0}, {}, {0, 0, 0, 0, 1}));
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, RecoveredGaugesTrackReadsRemovalAndRestarts) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv coalesce("DFKV_READ_COALESCE", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_recovered";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 4ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  std::vector<BlockKey> keys[2];
  for (uint64_t id = 1; id < 10000 && (keys[0].size() < 3 || keys[1].empty());
       ++id) {
    const BlockKey key{id, 0};
    const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
        KvNodeServerTestPeer::Group(*server), key);
    ASSERT_LT(disk, 2U);
    if ((disk == 0 && keys[0].size() < 3) || (disk == 1 && keys[1].empty()))
      keys[disk].push_back(key);
  }
  ASSERT_EQ(keys[0].size(), 3U);
  ASSERT_EQ(keys[1].size(), 1U);
  const auto expect_population = [&](uint64_t objects0, uint64_t unread0,
                                     uint64_t objects1, uint64_t unread1) {
    const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    const std::string text = server->MetricsText();
    const uint64_t objects[] = {objects0, objects1};
    const uint64_t unread[] = {unread0, unread1};
    uint64_t class_objects = 0;
    uint64_t class_unread = 0;
    const std::vector<SlabAllocator::ClassStat> classes = group.SlabClasses();
    for (const SlabAllocator::ClassStat& cls : classes) {
      class_objects += cls.recovered_residents;
      class_unread += cls.recovered_unread_residents;
    }
    EXPECT_EQ(class_objects, objects0 + objects1);
    EXPECT_EQ(class_unread, unread0 + unread1);
    for (size_t disk = 0; disk < 2; ++disk) {
      const DiskSlabStore::Stats stats = group.DiskSlabStats(disk);
      EXPECT_EQ(stats.lifetime_recovered_objects, objects[disk]);
      EXPECT_EQ(stats.lifetime_recovered_unread_objects, unread[disk]);
      EXPECT_LE(stats.lifetime_recovered_unread_objects,
                stats.lifetime_recovered_objects);
      const std::string labels = R"({disk=")" + options.cache_dirs[disk] +
                                 R"(",disk_index=")" + std::to_string(disk) +
                                 R"("} )";
      EXPECT_NE(text.find("dfkv_slab_disk_lifetime_recovered_objects" + labels +
                          std::to_string(objects[disk]) + "\n"),
                std::string::npos);
      EXPECT_NE(text.find("dfkv_slab_disk_lifetime_recovered_unread_objects" +
                          labels + std::to_string(unread[disk]) + "\n"),
                std::string::npos);
    }
    EXPECT_NE(text.find("dfkv_slab_lifetime_recovered_objects " +
                        std::to_string(objects0 + objects1) + "\n"),
              std::string::npos);
    EXPECT_NE(text.find("dfkv_slab_lifetime_recovered_unread_objects " +
                        std::to_string(unread0 + unread1) + "\n"),
              std::string::npos);
    for (const char* name :
         {"dfkv_slab_lifetime_recovered_objects",
          "dfkv_slab_lifetime_recovered_unread_objects",
          "dfkv_slab_disk_lifetime_recovered_objects",
          "dfkv_slab_disk_lifetime_recovered_unread_objects"}) {
      EXPECT_NE(text.find(std::string("# TYPE ") + name + " gauge\n"),
                std::string::npos);
    }
  };
  const auto read = [&](const BlockKey& key, std::string* value) {
    return server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                        key, 0, 0, nullptr, 0, value);
  };
  const std::string small(17, 's');
  const std::string large(5000, 'l');
  ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(keys[0][0], small.data(),
                                                       small.size()),
            Status::kOk);
  ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(keys[0][1], large.data(),
                                                       large.size()),
            Status::kOk);
  ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(keys[1][0], "other", 5),
            Status::kOk);
  // NEW residents, even unread ones, do not carry recovered history.
  ASSERT_NO_FATAL_FAILURE(expect_population(0, 0, 0, 0));
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  // Two size classes on disk 0 and a different population on disk 1.
  ASSERT_NO_FATAL_FAILURE(expect_population(2, 2, 1, 1));
  ASSERT_NO_FATAL_FAILURE(
      expect_population(2, 2, 1, 1));  // Scrapes are read-only.
  std::string value;
  ASSERT_EQ(read(keys[0][0], &value), Status::kOk);
  EXPECT_EQ(value, small);
  ASSERT_NO_FATAL_FAILURE(expect_population(2, 1, 1, 1));
  ASSERT_EQ(read(keys[0][0], &value), Status::kOk);
  ASSERT_NO_FATAL_FAILURE(expect_population(2, 1, 1, 1));
  char buffer[5] = {};
  size_t size = 0;
  ASSERT_EQ(
      server->RangeIntoForKey(keys[1][0], 0, 0, buffer, sizeof(buffer), &size),
      Status::kOk);
  EXPECT_EQ(std::string(buffer, size), "other");
  ASSERT_NO_FATAL_FAILURE(expect_population(2, 1, 1, 0));
  DiskSlabStore& disk0 =
      DiskCacheGroupTestPeer::Slab(KvNodeServerTestPeer::Group(*server), 0);
  // A storage-only read is not a logical notification and cannot consume
  // unread.
  ASSERT_EQ(disk0.Range(keys[0][1], 0, 0, &value), Status::kOk);
  EXPECT_EQ(value, large);
  ASSERT_NO_FATAL_FAILURE(expect_population(2, 1, 1, 0));
  ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(keys[0][2], small.data(),
                                                       small.size()),
            Status::kOk);
  ASSERT_NO_FATAL_FAILURE(expect_population(2, 1, 1, 0));
  ASSERT_EQ(disk0.Remove(keys[0][0]), Status::kOk);
  ASSERT_NO_FATAL_FAILURE(expect_population(1, 1, 1, 0));
  ReadLease lease;
  ASSERT_EQ(disk0.RangeDirectPrep(keys[0][1], 0, 0, 8192, &lease), Status::kOk);
  ASSERT_EQ(disk0.Remove(keys[0][1]), Status::kOk);
  EXPECT_FALSE(disk0.IsCached(keys[0][1]));
  // Hidden but pinned recovered residents remain in the allocator population.
  ASSERT_NO_FATAL_FAILURE(expect_population(1, 1, 1, 0));
  lease = ReadLease{};
  ASSERT_TRUE(server->Healthy());
  ASSERT_NO_FATAL_FAILURE(expect_population(0, 0, 1, 0));
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  // The prior NEW survivor is now recovered; real-read history resets on boot.
  ASSERT_NO_FATAL_FAILURE(expect_population(1, 1, 1, 1));
  ASSERT_EQ(read(keys[0][2], &value), Status::kOk);
  EXPECT_EQ(value, small);
  ASSERT_NO_FATAL_FAILURE(expect_population(1, 0, 1, 1));
  KvNodeServerTestPeer::Group(*server).PreventCleanShutdown();
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  EXPECT_EQ(KvNodeServerTestPeer::Group(*server).Count(), 0U);
  ASSERT_NO_FATAL_FAILURE(expect_population(0, 0, 0, 0));
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, SyncPayloadReadCountersAggregateRealIoAndReset) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_sync_payload";
  const std::array<const char*, 4> suffixes = {"syscalls", "bytes", "errors",
                                               "short_reads"};
  const auto fields = [](const DiskSlabStore::Stats& stats) {
    return std::array<uint64_t, 4>{
        stats.sync_payload_reads.syscalls, stats.sync_payload_reads.bytes,
        stats.sync_payload_reads.errors, stats.sync_payload_reads.short_reads};
  };
  // Each field has a distinct positive total, so swapped exports cannot pass.
  const std::array<std::array<uint64_t, 4>, 2> expected = {
      std::array<uint64_t, 4>{5, 13, 2, 2},
      std::array<uint64_t, 4>{9, 40, 3, 4}};
  for (const char* policy : {"legacy", "lifetime"}) {
    SCOPED_TRACE(policy);
    fs::remove_all(dir);
    DiskCacheGroup::Options options;
    options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
    options.capacity_bytes = 2ULL << 30;
    options.engine = "slab";
    options.eviction_policy = policy;
    std::unique_ptr<KvNodeServer> server =
        std::make_unique<KvNodeServer>(options);
    ASSERT_TRUE(server->Healthy());
    BlockKey keys[2]{};
    bool found[2]{};
    for (uint64_t id = 1; id < 10000 && !(found[0] && found[1]); ++id) {
      const BlockKey key{id, 0};
      const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
          KvNodeServerTestPeer::Group(*server), key);
      ASSERT_LT(disk, 2U);
      keys[disk] = key;
      found[disk] = true;
    }
    ASSERT_TRUE(found[0] && found[1]);
    const auto expect_counts = [&](bool empty) {
      const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
      const std::string text = server->MetricsText();
      const std::array<uint64_t, 4> total = fields(group.SlabStats());
      for (size_t field = 0; field < suffixes.size(); ++field) {
        uint64_t sum = 0;
        const std::string name = std::string("dfkv_slab_sync_payload_read_") +
                                 suffixes[field] + "_total";
        const std::string disk_name =
            std::string("dfkv_slab_disk_sync_payload_read_") + suffixes[field] +
            "_total";
        EXPECT_NE(text.find("# TYPE " + name + " counter\n"),
                  std::string::npos);
        EXPECT_NE(text.find("# TYPE " + disk_name + " counter\n"),
                  std::string::npos);
        for (size_t disk = 0; disk < 2; ++disk) {
          const uint64_t value = empty ? 0 : expected[disk][field];
          EXPECT_EQ(fields(group.DiskSlabStats(disk))[field], value);
          const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                     "\",disk_index=\"" + std::to_string(disk) +
                                     "\"} ";
          EXPECT_NE(text.find("\n" + disk_name + labels +
                              std::to_string(value) + "\n"),
                    std::string::npos);
          sum += value;
        }
        EXPECT_EQ(total[field], sum);
        EXPECT_NE(text.find("\n" + name + " " + std::to_string(sum) + "\n"),
                  std::string::npos);
      }
    };
    ASSERT_NO_FATAL_FAILURE(expect_counts(true));
    const std::string values[] = {std::string(11, 'a'), std::string(17, 'b')};
    for (size_t disk = 0; disk < 2; ++disk) {
      ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(
                    keys[disk], values[disk].data(), values[disk].size()),
                Status::kOk);
    }
    // PUT and metadata activity do not count as payload reads.
    ASSERT_NO_FATAL_FAILURE(expect_counts(true));
    for (size_t disk = 0; disk < 2; ++disk) {
      DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
      DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
      std::string output;
      for (size_t read = 0; read < disk + 1; ++read) {
        ASSERT_EQ(group.Range(keys[disk], 0, 0, &output), Status::kOk);
        EXPECT_EQ(output, values[disk]);
      }
      const std::pair<int, uint64_t> file =
          DiskSlabStoreTestPeer::PayloadFile(store, keys[disk]);
      ASSERT_GE(file.first, 0);
      // Only file length is needed; all Store I/O uses positional offsets,
      // so this test-only seek does not change any payload read/write offset.
      const off_t file_size = ::lseek(file.first, 0, SEEK_END);
      ASSERT_GE(file_size, 0);
      const uint64_t retained = disk + 2;
      ASSERT_EQ(
          ::ftruncate(file.first, static_cast<off_t>(file.second + retained)),
          0);
      for (size_t read = 0; read < disk + 1; ++read) {
        // An actual positive short pread is followed by an actual zero EOF.
        EXPECT_EQ(store.Range(keys[disk], 0, 0, &output), Status::kIOError);
        EXPECT_EQ(output.substr(0, retained), values[disk].substr(0, retained));
      }
      ASSERT_EQ(::ftruncate(file.first, file_size), 0);
      ASSERT_EQ(::pwrite(file.first, values[disk].data(), values[disk].size(),
                         static_cast<off_t>(file.second)),
                static_cast<ssize_t>(values[disk].size()));
      for (size_t read = 0; read < disk + 2; ++read) {
        // Retain ownership of the live descriptor, but make Store's real
        // pread use -1. Restore it before assertions or Store destruction.
        const int fd =
            DiskSlabStoreTestPeer::ExchangePayloadFd(store, keys[disk], -1);
        const Status status = store.Range(keys[disk], 0, 0, &output);
        const int replaced =
            DiskSlabStoreTestPeer::ExchangePayloadFd(store, keys[disk], fd);
        EXPECT_EQ(replaced, -1);
        ASSERT_EQ(fd, file.first);
        EXPECT_EQ(status, Status::kIOError);
      }
      EXPECT_TRUE(store.Healthy());
    }
    ASSERT_NO_FATAL_FAILURE(expect_counts(false));
    server.reset();
    server = std::make_unique<KvNodeServer>(options);
    ASSERT_TRUE(server->Healthy());
    EXPECT_EQ(KvNodeServerTestPeer::Group(*server).Count(), 2U);
    // Read-only preflight and clean table recovery do not populate these
    // counters. The restored extent bytes still serve the original content.
    ASSERT_NO_FATAL_FAILURE(expect_counts(true));
    std::string output;
    ASSERT_EQ(
        KvNodeServerTestPeer::Group(*server).Range(keys[0], 0, 0, &output),
        Status::kOk);
    EXPECT_EQ(output, values[0]);
    EXPECT_EQ(fields(KvNodeServerTestPeer::Group(*server).SlabStats()),
              (std::array<uint64_t, 4>{1, 11, 0, 0}));
    server.reset();
  }
  fs::remove_all(dir);
}

TEST(Metrics, UringPayloadReadCountersAggregateExplicitResultsAndReset) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv coalesce("DFKV_READ_COALESCE", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_uring_payload";
  const std::array<const char*, 4> suffixes = {"completions", "bytes", "errors",
                                               "short_reads"};
  const auto fields = [](const DiskSlabStore::Stats& stats) {
    return std::array<uint64_t, 4>{stats.uring_payload_reads.completions,
                                   stats.uring_payload_reads.bytes,
                                   stats.uring_payload_reads.errors,
                                   stats.uring_payload_reads.short_reads};
  };
  const std::array<std::array<uint64_t, 4>, 2> expected = {
      std::array<uint64_t, 4>{5, 4099, 2, 2},
      std::array<uint64_t, 4>{9, 8200, 3, 4}};
  for (const char* policy : {"legacy", "lifetime"}) {
    SCOPED_TRACE(policy);
    fs::remove_all(dir);
    DiskCacheGroup::Options options;
    options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
    options.capacity_bytes = 2ULL << 30;
    options.engine = "slab";
    options.eviction_policy = policy;
    std::unique_ptr<KvNodeServer> server =
        std::make_unique<KvNodeServer>(options);
    ASSERT_TRUE(server->Healthy());
    BlockKey keys[2]{};
    bool found[2]{};
    for (uint64_t id = 1; id < 10000 && !(found[0] && found[1]); ++id) {
      const BlockKey key{id, 0};
      const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
          KvNodeServerTestPeer::Group(*server), key);
      ASSERT_LT(disk, 2U);
      keys[disk] = key;
      found[disk] = true;
    }
    ASSERT_TRUE(found[0] && found[1]);
    const auto expect_counts = [&](bool empty) {
      const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
      const std::string text = server->MetricsText();
      const std::array<uint64_t, 4> total = fields(group.SlabStats());
      for (size_t field = 0; field < suffixes.size(); ++field) {
        uint64_t sum = 0;
        const std::string name = std::string("dfkv_slab_uring_payload_read_") +
                                 suffixes[field] + "_total";
        const std::string disk_name =
            std::string("dfkv_slab_disk_uring_payload_read_") +
            suffixes[field] + "_total";
        EXPECT_NE(text.find("# TYPE " + name + " counter\n"),
                  std::string::npos);
        EXPECT_NE(text.find("# TYPE " + disk_name + " counter\n"),
                  std::string::npos);
        for (size_t disk = 0; disk < 2; ++disk) {
          const uint64_t value = empty ? 0 : expected[disk][field];
          EXPECT_EQ(fields(group.DiskSlabStats(disk))[field], value);
          const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                     "\",disk_index=\"" + std::to_string(disk) +
                                     "\"} ";
          EXPECT_NE(text.find("\n" + disk_name + labels +
                              std::to_string(value) + "\n"),
                    std::string::npos);
          sum += value;
        }
        EXPECT_EQ(total[field], sum);
        EXPECT_NE(text.find("\n" + name + " " + std::to_string(sum) + "\n"),
                  std::string::npos);
      }
      EXPECT_EQ(group.SlabStats().sync_payload_reads.syscalls, 0U);
    };
    ASSERT_NO_FATAL_FAILURE(expect_counts(true));
    const std::string values[] = {std::string(11, 'a'), std::string(17, 'b')};
    for (size_t disk = 0; disk < 2; ++disk) {
      DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
      ASSERT_EQ(
          group.Cache(keys[disk], values[disk].data(), values[disk].size()),
          Status::kOk);
      DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
      std::vector<char> staging(4096);
      // This explicit executor seam uses actual pread returns, not kernel
      // io_uring CQEs. Ring->lease integration is tested in uring_reader_test.
      const auto execute = [&](unsigned phase) {
        PreparedRead read = server->PrepareReadForKey(
            keys[disk], 0, values[disk].size(), staging.data(), staging.size());
        ASSERT_EQ(read.status(), Status::kOk);
        ASSERT_TRUE(read.needs_io());
        ASSERT_NE(read.async_read_counters(), nullptr);
        ASSERT_EQ(read.aligned_len(), staging.size());
        ASSERT_EQ(read.head(), 0U);
        const std::array<uint64_t, 4> before = fields(store.GetStats());
        const ssize_t result =
            ::pread(read.fd(), read.staging(), read.aligned_len(),
                    phase == 2 ? static_cast<off_t>(-1)
                               : static_cast<off_t>(read.aligned_off()));
        const long completion = result < 0 ? -errno : result;
        read.async_read_counters()->Record(completion, read.aligned_len());
        if (phase == 0) {
          ASSERT_EQ(result, 4096);
          EXPECT_EQ(std::string(read.data(), read.payload_len()), values[disk]);
        } else if (phase == 1) {
          ASSERT_EQ(result, static_cast<ssize_t>(disk + 3));
          const ssize_t eof =
              ::pread(read.fd(), read.staging() + result,
                      read.aligned_len() - static_cast<size_t>(result),
                      static_cast<off_t>(read.aligned_off() +
                                         static_cast<uint64_t>(result)));
          ASSERT_EQ(eof, 0);
          read.async_read_counters()->Record(
              eof, read.aligned_len() - static_cast<size_t>(result));
        } else {
          ASSERT_EQ(result, -1);
          EXPECT_EQ(completion, -EINVAL);
        }
        // Recording is local until the original lease finalization lock.
        EXPECT_EQ(fields(store.GetStats()), before);
        const Status status = phase == 0 ? Status::kOk : Status::kIOError;
        const size_t bytes = phase == 0 ? read.payload_len() : 0;
        EXPECT_EQ(read.FinishStorage(status, bytes), status);
        EXPECT_EQ(read.async_read_counters(), nullptr);
        const std::array<uint64_t, 4> published = fields(store.GetStats());
        EXPECT_EQ(published[0], before[0] + (phase == 1 ? 2 : 1));
        EXPECT_EQ(read.FinishStorage(status, bytes), status);
        EXPECT_TRUE(read.Abort());
        EXPECT_FALSE(read.Abort());
        EXPECT_EQ(fields(store.GetStats()), published);
      };
      for (size_t count = 0; count < disk + 1; ++count) {
        ASSERT_NO_FATAL_FAILURE(execute(0));
      }
      const std::pair<int, uint64_t> file =
          DiskSlabStoreTestPeer::PayloadFile(store, keys[disk]);
      ASSERT_GE(file.first, 0);
      const off_t file_size = ::lseek(file.first, 0, SEEK_END);
      ASSERT_GE(file_size, 0);
      ASSERT_EQ(
          ::ftruncate(file.first, static_cast<off_t>(file.second + disk + 3)),
          0);
      for (size_t count = 0; count < disk + 1; ++count) {
        ASSERT_NO_FATAL_FAILURE(execute(1));
      }
      ASSERT_EQ(::ftruncate(file.first, file_size), 0);
      ASSERT_EQ(::pwrite(file.first, values[disk].data(), values[disk].size(),
                         static_cast<off_t>(file.second)),
                static_cast<ssize_t>(values[disk].size()));
      for (size_t count = 0; count < disk + 2; ++count) {
        ASSERT_NO_FATAL_FAILURE(execute(2));
      }
    }
    ASSERT_NO_FATAL_FAILURE(expect_counts(false));
    server.reset();
    server = std::make_unique<KvNodeServer>(options);
    ASSERT_TRUE(server->Healthy());
    ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Count(), 2U);
    ASSERT_NO_FATAL_FAILURE(expect_counts(true));
    const std::string text = server->MetricsText();
    EXPECT_NE(text.find("dfkv_slab_uring_payload_read_bytes_total 0\n"),
              std::string::npos);
    server.reset();
  }
  fs::remove_all(dir);
}

TEST(Metrics, TransportPreparationCountsAggregateAndReset) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_metrics_transport_source";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  BlockKey keys[2]{};
  bool found[2]{};
  for (uint64_t id = 1; id < 10000 && !(found[0] && found[1]); ++id) {
    const BlockKey key{id, 0};
    const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
        KvNodeServerTestPeer::Group(*server), key);
    ASSERT_LT(disk, 2U);
    keys[disk] = key;
    found[disk] = true;
  }
  ASSERT_TRUE(found[0] && found[1]);
  struct TransportMetric {
    PreparedRead::Source source;
    const char* suffix;
    uint64_t DiskSlabStore::LifetimeReadCounters::*field;
  };
  const TransportMetric metrics[] = {
      {PreparedRead::Source::kUnclassified, "unclassified",
       &DiskSlabStore::LifetimeReadCounters::transport_unclassified},
      {PreparedRead::Source::kRamHit, "ram_hit",
       &DiskSlabStore::LifetimeReadCounters::transport_ram_hit},
      {PreparedRead::Source::kDiskStaging, "disk_staging",
       &DiskSlabStore::LifetimeReadCounters::transport_disk_staging},
      {PreparedRead::Source::kDiskToRam, "disk_to_ram",
       &DiskSlabStore::LifetimeReadCounters::transport_disk_to_ram},
      {PreparedRead::Source::kPublishedFollower, "published_follower",
       &DiskSlabStore::LifetimeReadCounters::transport_published_follower},
      {PreparedRead::Source::kSynchronousFallback, "synchronous_fallback",
       &DiskSlabStore::LifetimeReadCounters::transport_synchronous_fallback}};
  const auto expect_counts = [&](uint64_t multiplier) {
    const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    const std::string text = server->MetricsText();
    const DiskSlabStore::LifetimeReadCounters total =
        group.SlabStats().lifetime_reads;
    EXPECT_EQ(total.source_transport_grant, 63 * multiplier);
    EXPECT_EQ(total.successful, total.source_transport_grant);
    size_t index = 0;
    for (const TransportMetric& metric : metrics) {
      const std::string name =
          std::string("dfkv_slab_lifetime_read_transport_") + metric.suffix +
          "_total";
      EXPECT_NE(text.find("# TYPE " + name + " counter\n"), std::string::npos);
      EXPECT_EQ(total.*(metric.field), (index + 1) * 3 * multiplier);
      EXPECT_NE(text.find("\n" + name + " " +
                          std::to_string(total.*(metric.field)) + "\n"),
                std::string::npos);
      const std::string disk_name =
          std::string("dfkv_slab_disk_lifetime_read_transport_") +
          metric.suffix + "_total";
      EXPECT_NE(text.find("# TYPE " + disk_name + " counter\n"),
                std::string::npos);
      for (size_t disk = 0; disk < 2; ++disk) {
        const DiskSlabStore::LifetimeReadCounters actual =
            group.DiskSlabStats(disk).lifetime_reads;
        EXPECT_EQ(actual.*(metric.field),
                  (index + 1) * (disk + 1) * multiplier);
        EXPECT_EQ(actual.source_transport_grant, 21 * (disk + 1) * multiplier);
        EXPECT_EQ(actual.source_transport_grant,
                  actual.transport_unclassified + actual.transport_ram_hit +
                      actual.transport_disk_staging +
                      actual.transport_disk_to_ram +
                      actual.transport_published_follower +
                      actual.transport_synchronous_fallback);
        const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                   "\",disk_index=\"" + std::to_string(disk) +
                                   "\"} ";
        EXPECT_NE(text.find("\n" + disk_name + labels +
                            std::to_string(actual.*(metric.field)) + "\n"),
                  std::string::npos);
      }
      ++index;
    }
  };
  ASSERT_NO_FATAL_FAILURE(expect_counts(0));
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    ASSERT_EQ(group.Cache(keys[disk], "value", 5), Status::kOk);
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
    std::string value;
    DiskResidentIdentity identity;
    ASSERT_EQ(store.RangeWithIdentity(keys[disk], 0, 0, &value, &identity),
              Status::kOk);
    EXPECT_EQ(value, "value");
    ASSERT_TRUE(identity.valid());
    EXPECT_EQ(store.GetStats().lifetime_reads.successful, 0U);
    // Exercise the observer boundary with explicit simulated grant events.
    // CPU transport tests separately prove real preparation-path attribution.
    size_t index = 0;
    for (const TransportMetric& metric : metrics) {
      for (size_t n = 0; n < (index + 1) * (disk + 1); ++n) {
        if (metric.source == PreparedRead::Source::kUnclassified)
          server->NotifyLifetimeReadSuccessForKey(keys[disk], identity,
                                                  value.size());
        else
          server->NotifyLifetimeReadSuccessForKey(keys[disk], identity,
                                                  value.size(), metric.source);
      }
      server->NotifyLifetimeReadSuccessForKey(keys[disk], identity, 0,
                                              metric.source);
      ++index;
    }
  }
  ASSERT_NO_FATAL_FAILURE(expect_counts(1));
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  EXPECT_EQ(KvNodeServerTestPeer::Group(*server).Count(), 2U);
  ASSERT_NO_FATAL_FAILURE(expect_counts(0));
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, SynchronousFallbackCountsAggregateAndReset) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir =
      fs::temp_directory_path() / "dfkv_metrics_fallback_source";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  BlockKey keys[2]{};
  bool found[2]{};
  for (uint64_t id = 1; id < 10000 && !(found[0] && found[1]); ++id) {
    const BlockKey key{id, 0};
    const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
        KvNodeServerTestPeer::Group(*server), key);
    ASSERT_LT(disk, 2U);
    keys[disk] = key;
    found[disk] = true;
  }
  ASSERT_TRUE(found[0] && found[1]);
  struct TransportMetric {
    PreparedRead::Source source;
    const char* suffix;
    uint64_t DiskSlabStore::LifetimeReadCounters::*field;
  };
  const TransportMetric metrics[] = {
      {PreparedRead::Source::kSynchronousFallback, "fallback_unclassified",
       &DiskSlabStore::LifetimeReadCounters::transport_fallback_unclassified},
      {PreparedRead::Source::kSynchronousRamHit, "fallback_ram_hit",
       &DiskSlabStore::LifetimeReadCounters::transport_fallback_ram_hit},
      {PreparedRead::Source::kSynchronousDisk, "fallback_disk",
       &DiskSlabStore::LifetimeReadCounters::transport_fallback_disk},
      {PreparedRead::Source::kSynchronousCopyLeader, "fallback_copy_leader",
       &DiskSlabStore::LifetimeReadCounters::transport_fallback_copy_leader},
      {PreparedRead::Source::kSynchronousCopyFollower, "fallback_copy_follower",
       &DiskSlabStore::LifetimeReadCounters::transport_fallback_copy_follower}};
  const auto expect_counts = [&](uint64_t multiplier) {
    const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    const std::string text = server->MetricsText();
    const DiskSlabStore::LifetimeReadCounters total =
        group.SlabStats().lifetime_reads;
    EXPECT_EQ(total.source_transport_grant, 45 * multiplier);
    EXPECT_EQ(total.successful, total.source_transport_grant);
    EXPECT_EQ(total.transport_synchronous_fallback,
              total.source_transport_grant);
    size_t index = 0;
    for (const TransportMetric& metric : metrics) {
      const std::string name =
          std::string("dfkv_slab_lifetime_read_transport_") + metric.suffix +
          "_total";
      EXPECT_NE(text.find("# TYPE " + name + " counter\n"), std::string::npos);
      EXPECT_EQ(total.*(metric.field), (index + 1) * 3 * multiplier);
      EXPECT_NE(text.find("\n" + name + " " +
                          std::to_string(total.*(metric.field)) + "\n"),
                std::string::npos);
      const std::string disk_name =
          std::string("dfkv_slab_disk_lifetime_read_transport_") +
          metric.suffix + "_total";
      EXPECT_NE(text.find("# TYPE " + disk_name + " counter\n"),
                std::string::npos);
      for (size_t disk = 0; disk < 2; ++disk) {
        const DiskSlabStore::LifetimeReadCounters actual =
            group.DiskSlabStats(disk).lifetime_reads;
        EXPECT_EQ(actual.*(metric.field),
                  (index + 1) * (disk + 1) * multiplier);
        EXPECT_EQ(actual.source_transport_grant, 15 * (disk + 1) * multiplier);
        EXPECT_EQ(actual.source_transport_grant,
                  actual.transport_unclassified + actual.transport_ram_hit +
                      actual.transport_disk_staging +
                      actual.transport_disk_to_ram +
                      actual.transport_published_follower +
                      actual.transport_synchronous_fallback);
        EXPECT_EQ(actual.transport_synchronous_fallback,
                  actual.transport_fallback_unclassified +
                      actual.transport_fallback_ram_hit +
                      actual.transport_fallback_disk +
                      actual.transport_fallback_copy_leader +
                      actual.transport_fallback_copy_follower);
        const std::string labels = "{disk=\"" + options.cache_dirs[disk] +
                                   "\",disk_index=\"" + std::to_string(disk) +
                                   "\"} ";
        EXPECT_NE(text.find("\n" + disk_name + labels +
                            std::to_string(actual.*(metric.field)) + "\n"),
                  std::string::npos);
      }
      ++index;
    }
  };
  ASSERT_NO_FATAL_FAILURE(expect_counts(0));
  for (size_t disk = 0; disk < 2; ++disk) {
    DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    ASSERT_EQ(group.Cache(keys[disk], "value", 5), Status::kOk);
    DiskSlabStore& store = DiskCacheGroupTestPeer::Slab(group, disk);
    std::string value;
    DiskResidentIdentity identity;
    ASSERT_EQ(store.RangeWithIdentity(keys[disk], 0, 0, &value, &identity),
              Status::kOk);
    EXPECT_EQ(value, "value");
    ASSERT_TRUE(identity.valid());
    EXPECT_EQ(store.GetStats().lifetime_reads.successful, 0U);
    // Exercise the observer boundary with explicit simulated grant events.
    // CPU transport tests separately prove real preparation-path attribution.
    size_t index = 0;
    for (const TransportMetric& metric : metrics) {
      for (size_t n = 0; n < (index + 1) * (disk + 1); ++n) {
        server->NotifyLifetimeReadSuccessForKey(keys[disk], identity,
                                                value.size(), metric.source);
      }
      server->NotifyLifetimeReadSuccessForKey(keys[disk], identity, 0,
                                              metric.source);
      ++index;
    }
  }
  ASSERT_NO_FATAL_FAILURE(expect_counts(1));
  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  EXPECT_EQ(KvNodeServerTestPeer::Group(*server).Count(), 2U);
  ASSERT_NO_FATAL_FAILURE(expect_counts(0));
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, LifetimeReadOutcomesArePerDiskAndResetOnRecovery) {
  const ScopedEnv ram("DFKV_RAM_TIER", "0");
  const ScopedEnv coalesce("DFKV_READ_COALESCE", "0");
  const ScopedEnv write_mode("DFKV_SLAB_WRITE", "buffered");
  const ScopedEnv granularity("DFKV_SLAB_GRANULARITY", "4096");
  const ScopedEnv reclaim("DFKV_SLAB_RECLAIM_MS", "0");
  const ScopedEnv table_sync("DFKV_SLAB_TABLE_SYNC_MS", "0");
  const fs::path dir = fs::temp_directory_path() / "dfkv_metrics_read_outcomes";
  fs::remove_all(dir);
  DiskCacheGroup::Options options;
  options.cache_dirs = {(dir / "a").string(), (dir / "b").string()};
  options.capacity_bytes = 2ULL << 30;
  options.engine = "slab";
  options.eviction_policy = "lifetime";
  std::unique_ptr<KvNodeServer> server =
      std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  server->set_identity("reads", "test");
  BlockKey keys[2]{};
  bool found[2]{};
  for (uint64_t id = 1; id < 10000 && !(found[0] && found[1]); ++id) {
    const BlockKey key{id, 0};
    const size_t disk = DiskCacheGroupTestPeer::RouteIndex(
        KvNodeServerTestPeer::Group(*server), key);
    ASSERT_LT(disk, 2U);
    keys[disk] = key;
    found[disk] = true;
  }
  ASSERT_TRUE(found[0] && found[1]);
  DiskResidentIdentity original[2]{};
  for (size_t disk = 0; disk < 2; ++disk) {
    const DiskWriteResult written =
        KvNodeServerTestPeer::Group(*server).CacheWithResult(keys[disk],
                                                             "value", 5);
    ASSERT_EQ(written.status, Status::kOk);
    ASSERT_TRUE(written.identity.valid());
    original[disk] = written.identity;
  }
  DiskSlabStore::LifetimeReadCounters expected[2]{};
  const auto expect_outcomes = [&] {
    const DiskCacheGroup& group = KvNodeServerTestPeer::Group(*server);
    const std::string text = server->MetricsText();
    struct ReadMetric {
      const char* suffix;
      uint64_t DiskSlabStore::LifetimeReadCounters::*field;
    };
    const ReadMetric metrics[] = {
        {"success", &DiskSlabStore::LifetimeReadCounters::successful},
        {"applied", &DiskSlabStore::LifetimeReadCounters::applied},
        {"no_binding", &DiskSlabStore::LifetimeReadCounters::no_binding},
        {"race_lost", &DiskSlabStore::LifetimeReadCounters::race_lost},
        {"clock_hard_exhausted",
         &DiskSlabStore::LifetimeReadCounters::clock_hard_exhausted},
        {"io_unhealthy", &DiskSlabStore::LifetimeReadCounters::io_unhealthy},
        {"state_unavailable",
         &DiskSlabStore::LifetimeReadCounters::state_unavailable},
        {"source_unclassified",
         &DiskSlabStore::LifetimeReadCounters::source_unclassified},
        {"source_ram", &DiskSlabStore::LifetimeReadCounters::source_ram},
        {"source_disk", &DiskSlabStore::LifetimeReadCounters::source_disk},
        {"source_copy_leader",
         &DiskSlabStore::LifetimeReadCounters::source_copy_leader},
        {"source_copy_follower",
         &DiskSlabStore::LifetimeReadCounters::source_copy_follower},
        {"source_transport_grant",
         &DiskSlabStore::LifetimeReadCounters::source_transport_grant}};
    for (const ReadMetric& metric : metrics) {
      const std::string name = std::string("dfkv_slab_disk_lifetime_read_") +
                               metric.suffix + "_total";
      EXPECT_NE(text.find("# HELP " + name + " "), std::string::npos);
      EXPECT_NE(text.find("# TYPE " + name + " counter\n"), std::string::npos);
      uint64_t sum = 0;
      for (size_t disk = 0; disk < 2; ++disk) {
        const DiskSlabStore::LifetimeReadCounters actual =
            group.DiskSlabStats(disk).lifetime_reads;
        EXPECT_EQ(actual.*metric.field, expected[disk].*metric.field);
        EXPECT_EQ(actual.successful,
                  actual.applied + actual.no_binding + actual.race_lost +
                      actual.clock_hard_exhausted + actual.io_unhealthy +
                      actual.state_unavailable);
        EXPECT_EQ(actual.successful,
                  actual.source_unclassified + actual.source_ram +
                      actual.source_disk + actual.source_copy_leader +
                      actual.source_copy_follower +
                      actual.source_transport_grant);
        const std::string labels = R"({disk=")" + options.cache_dirs[disk] +
                                   R"(",disk_index=")" + std::to_string(disk) +
                                   R"(",node="reads",group="test"} )";
        EXPECT_NE(
            text.find(name + labels +
                      std::to_string(expected[disk].*metric.field) + "\n"),
            std::string::npos);
        sum += expected[disk].*metric.field;
      }
      const std::string global =
          std::string("dfkv_slab_lifetime_read_") + metric.suffix + "_total";
      EXPECT_NE(text.find(global + "{node=\"reads\",group=\"test\"} " +
                          std::to_string(sum) + "\n"),
                std::string::npos);
    }
  };
  ASSERT_NO_FATAL_FAILURE(expect_outcomes());
  const auto read = [&](const BlockKey& key, uint64_t offset,
                        std::string* value) {
    return server->ProcessRequestForKey(static_cast<uint8_t>(WireOp::kRange),
                                        key, offset, 0, nullptr, 0, value);
  };
  for (size_t disk = 0; disk < 2; ++disk) {
    std::string value;
    ASSERT_EQ(read(keys[disk], 0, &value), Status::kOk);
    EXPECT_EQ(value, "value");
    ++expected[disk].successful;
    ++expected[disk].applied;
    ++expected[disk].source_disk;
    ValueMetadata metadata;
    ASSERT_EQ(server->LookupForKey(keys[disk], &metadata), Status::kOk);
    EXPECT_EQ(metadata.value_len, 5U);
    ASSERT_EQ(read(keys[disk], 5, &value), Status::kOk);
    EXPECT_TRUE(value.empty());
    ASSERT_EQ(read(keys[disk], 6, &value), Status::kInvalid);
  }
  char buffer[5]{};
  size_t size = 0;
  ASSERT_EQ(
      server->RangeIntoForKey(keys[1], 0, 0, buffer, sizeof(buffer), &size),
      Status::kOk);
  EXPECT_EQ(std::string(buffer, size), "value");
  ++expected[1].successful;
  ++expected[1].applied;
  ++expected[1].source_disk;
  std::string value;
  ASSERT_EQ(read({10001, 0}, 0, &value), Status::kNotFound);
  ASSERT_NO_FATAL_FAILURE(expect_outcomes());

  // Simulate a late successful transport notification with an actual old
  // credential. This checks export attribution, not a live transport grant.
  ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Remove(keys[0]), Status::kOk);
  ASSERT_EQ(KvNodeServerTestPeer::Group(*server).Cache(keys[0], "newer", 5),
            Status::kOk);
  server->NotifyLifetimeReadSuccessForKey(keys[0], original[0], 5);
  ++expected[0].successful;
  ++expected[0].race_lost;
  ++expected[0].source_transport_grant;
  // An unbound notification is attributed by key route, not physical source.
  server->NotifyLifetimeReadSuccessForKey(keys[1], {}, 5);
  ++expected[1].successful;
  ++expected[1].no_binding;
  ++expected[1].source_transport_grant;
  server->NotifyLifetimeReadSuccessForKey(keys[1], original[1], 0);
  ASSERT_NO_FATAL_FAILURE(expect_outcomes());

  server.reset();
  server = std::make_unique<KvNodeServer>(options);
  ASSERT_TRUE(server->Healthy());
  server->set_identity("reads", "test");
  expected[0] = {};
  expected[1] = {};
  // Recovery's virtual two reads rebuild protection but are not business reads.
  EXPECT_EQ(
      KvNodeServerTestPeer::Group(*server).DiskSlabStats(0).committed_objects,
      1U);
  EXPECT_EQ(
      KvNodeServerTestPeer::Group(*server).DiskSlabStats(1).committed_objects,
      1U);
  ASSERT_NO_FATAL_FAILURE(expect_outcomes());
  ASSERT_EQ(read(keys[1], 0, &value), Status::kOk);
  EXPECT_EQ(value, "value");
  ++expected[1].successful;
  ++expected[1].applied;
  ++expected[1].source_disk;
  ASSERT_NO_FATAL_FAILURE(expect_outcomes());
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, PrometheusFormatAndIdentity) {
  ScopedEnv engine("DFKV_STORE_ENGINE", "slab");
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_c";
  auto s = Start(dir, &addr);
  s->set_identity("n1", "g1");
  std::string text = s->MetricsText();
  // HELP/TYPE metadata present
  EXPECT_NE(text.find("# TYPE dfkv_cache_hit_total counter"), std::string::npos) << text;
  EXPECT_NE(text.find("# TYPE dfkv_used_bytes gauge"), std::string::npos) << text;
  // identity labels applied to series
  EXPECT_NE(text.find("dfkv_cache_hit_total{node=\"n1\",group=\"g1\"} 0"), std::string::npos) << text;
  // build_info + uptime present
  EXPECT_NE(text.find("dfkv_build_info{"), std::string::npos) << text;
  EXPECT_NE(text.find("version=\""), std::string::npos) << text;
  EXPECT_NE(text.find(",engine=\"slab\",write_mode=\""),
            std::string::npos) << text;
  EXPECT_EQ(text.find(",engine=\"file\""), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_uptime_seconds"), std::string::npos) << text;
  s->Stop();
}

TEST(Metrics, BuildInfoReportsExplicitFileDiagnosticBackend) {
  ScopedEnv engine("DFKV_STORE_ENGINE", "file");
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_file";
  auto server = Start(dir, &addr);
  const std::string text = server->MetricsText();
  EXPECT_NE(text.find(",engine=\"file\",write_mode=\"n/a\""),
            std::string::npos) << text;
  EXPECT_EQ(text.find("dfkv_slab_capacity_bytes"), std::string::npos) << text;
  for (const char* suffix : {"syscalls", "bytes", "errors", "short_reads"}) {
    EXPECT_EQ(text.find(std::string("dfkv_slab_sync_payload_read_") + suffix +
                        "_total"),
              std::string::npos);
    EXPECT_EQ(text.find(std::string("dfkv_slab_disk_sync_payload_read_") +
                        suffix + "_total"),
              std::string::npos);
  }

  for (const char* suffix : {"completions", "bytes", "errors", "short_reads"}) {
    EXPECT_EQ(text.find(std::string("dfkv_slab_uring_payload_read_") + suffix +
                        "_total"),
              std::string::npos);
    EXPECT_EQ(text.find(std::string("dfkv_slab_disk_uring_payload_read_") +
                        suffix + "_total"),
              std::string::npos);
  }

  for (const char* suffix :
       {"unclassified", "ram_hit", "disk_staging", "disk_to_ram",
        "published_follower", "synchronous_fallback", "fallback_unclassified",
        "fallback_ram_hit", "fallback_disk", "fallback_copy_leader",
        "fallback_copy_follower"}) {
    EXPECT_EQ(text.find(std::string("dfkv_slab_lifetime_read_transport_") +
                        suffix + "_total"),
              std::string::npos);
    EXPECT_EQ(text.find(std::string("dfkv_slab_disk_lifetime_read_transport_") +
                        suffix + "_total"),
              std::string::npos);
  }

  EXPECT_TRUE(
      KvNodeServerTestPeer::Group(*server).DiskLifetimeQueueStats(0).empty());
  EXPECT_FALSE(KvNodeServerTestPeer::Group(*server)
                   .DiskLifetimeBucketObservation(0)
                   .available);
  for (const char* name :
       {"dfkv_slab_metadata_slot_bytes",
        "dfkv_slab_metadata_extent_bytes",
        "dfkv_slab_metadata_class_bytes",
        "dfkv_slab_metadata_key_index_bytes",
        "dfkv_slab_metadata_class_rotation_bytes",
        "dfkv_slab_metadata_class_free_slot_bytes",
        "dfkv_slab_disk_metadata_slot_bytes",
        "dfkv_slab_disk_metadata_extent_bytes",
        "dfkv_slab_disk_metadata_class_bytes",
        "dfkv_slab_disk_metadata_key_index_bytes",
        "dfkv_slab_disk_metadata_class_rotation_bytes",
        "dfkv_slab_disk_metadata_class_free_slot_bytes",
        "dfkv_slab_lifetime_bucket_protected_estimate",
        "dfkv_slab_lifetime_bucket_estimate_available",
        "dfkv_slab_lifetime_bucket_summary_bytes",
        "dfkv_slab_disk_lifetime_bucket_protected_estimate",
        "dfkv_slab_disk_lifetime_bucket_estimate_available",
        "dfkv_slab_disk_lifetime_bucket_summary_bytes",
        "dfkv_slab_lifetime_queue_residents",
        "dfkv_slab_disk_lifetime_queue_residents",
        "dfkv_slab_allocator_payload_bytes",
        "dfkv_slab_allocator_fragmentation_bytes",
        "dfkv_slab_disk_allocator_payload_bytes",
        "dfkv_slab_disk_allocator_fragmentation_bytes",
        "dfkv_slab_disk_payload_bytes",
        "dfkv_slab_disk_allocated_bytes",
        "dfkv_slab_lifetime_bucket_entries",
        "dfkv_slab_lifetime_bucket_bytes",
        "dfkv_slab_lifetime_bucket_token_bytes",
        "dfkv_slab_lifetime_identity_bytes",
        "dfkv_slab_lifetime_state_bytes",
        "dfkv_slab_lifetime_link_bytes",
        "dfkv_slab_lifetime_queue_bytes",
        "dfkv_slab_lifetime_version_bytes",
        "dfkv_slab_disk_lifetime_identity_bytes",
        "dfkv_slab_disk_lifetime_state_bytes",
        "dfkv_slab_disk_lifetime_link_bytes",
        "dfkv_slab_disk_lifetime_queue_bytes",
        "dfkv_slab_disk_lifetime_version_bytes",
        "dfkv_slab_disk_lifetime_bucket_entries",
        "dfkv_slab_disk_lifetime_bucket_bytes",
        "dfkv_slab_disk_lifetime_bucket_token_bytes",
        "dfkv_slab_lifetime_recovered_objects",
        "dfkv_slab_lifetime_recovered_unread_objects",
        "dfkv_slab_disk_lifetime_recovered_objects",
        "dfkv_slab_disk_lifetime_recovered_unread_objects"}) {
    EXPECT_EQ(text.find(name), std::string::npos);
  }
  EXPECT_EQ(text.find("dfkv_slab_watermark_freed_payload_bytes_total"),
            std::string::npos)
      << text;
  EXPECT_EQ(text.find("dfkv_slab_disk_watermark_freed_payload_bytes_total"),
            std::string::npos)
      << text;
  EXPECT_EQ(text.find("dfkv_slab_lifetime_background_victims_total"),
            std::string::npos);
  EXPECT_EQ(
      text.find("dfkv_slab_lifetime_background_freed_payload_bytes_total"),
      std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_background_freed_slot_bytes_total"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_disk_lifetime_background_"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_class_freed_payload_bytes_total"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_class_freed_slot_bytes_total"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_disk_lifetime_class_"), std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_donor_freed_payload_bytes_total"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_donor_freed_slot_bytes_total"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_disk_lifetime_donor_"), std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_disk_lifetime_read_"), std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_donor_protected_victims_total"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_donor_protected_payload_bytes_total"),
            std::string::npos);
  EXPECT_EQ(text.find("dfkv_slab_lifetime_donor_unscored_victims_total"),
            std::string::npos);
  server->Stop();
  server.reset();
  fs::remove_all(dir);
}

TEST(Metrics, LabelValuesAreEscaped) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_esc";
  auto s = Start(dir, &addr);
  s->set_identity("n\"1", "g\\x");  // identity with a quote + backslash
  std::string text = s->MetricsText();
  // raw `n"1` would break the scrape; must appear escaped
  EXPECT_NE(text.find("node=\"n\\\"1\",group=\"g\\\\x\""), std::string::npos) << text;
  EXPECT_EQ(text.find("node=\"n\"1\""), std::string::npos) << "unescaped quote leaked";
  s->Stop();
}

TEST(Metrics, NoIdentityKeepsUnlabeledSeries) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_d";
  auto s = Start(dir, &addr);  // no set_identity
  std::string text = s->MetricsText();
  // back-compat: bare metric line, no label braces
  EXPECT_NE(text.find("dfkv_cache_hit_total 0"), std::string::npos) << text;
  s->Stop();
}

TEST(Metrics, DepthSeriesPresent) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_e";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v(100, 'q');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "a"), v.data(), v.size()), Status::kOk);
  std::string out;
  ASSERT_EQ(t.Range(addr, ToBlockKey("test/model", "a"), 0, v.size(), &out), Status::kOk);
  std::string text = s->MetricsText();
  EXPECT_NE(text.find("# TYPE dfkv_op_latency_seconds histogram"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_op_latency_seconds_count{op=\"get\"}"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_op_latency_seconds_count{op=\"put\"}"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_evictions_total"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_open_connections"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_errors_total{op=\"get\",status=\"io\"}"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_disk_used_bytes{disk="), std::string::npos) << text;
  s->Stop();
}

TEST(Metrics, OpenConnectionsTracksLiveConn) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_f";
  auto s = Start(dir, &addr);
  {
    TcpTransport t;  // pooled keep-alive connection held open for this scope
    std::string out;
    ASSERT_EQ(t.Range(addr, ToBlockKey("test/model", "x"), 0, 1, &out), Status::kNotFound);
    // a connection is open while the pooled fd lives
    EXPECT_NE(s->MetricsText().find("dfkv_open_connections"), std::string::npos);
  }  // transport destructor closes the pooled fd; server-side handler exits
  s->Stop();
}

TEST(Metrics, ReapsConnHandlerThreads) {
  // Short-lived client connections (connect + immediate close) each spawn a
  // handler thread that exits on peer-close. Reaping at accept time must keep the
  // unreaped handler-thread list bounded, not growing ~1 per connection.
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_reap";
  auto s = Start(dir, &addr);
  for (int i = 0; i < 80; ++i) {
    int fd = net::Dial(addr, 1000, 1000);
    ASSERT_GE(fd, 0);
    ::close(fd);  // peer close -> handler's ReadAll fails -> handler exits
  }
  // Reaping fires on accept; nudge a few more + poll until it drains.
  size_t live = 999;
  for (int r = 0; r < 40; ++r) {
    int fd = net::Dial(addr, 1000, 1000);
    if (fd >= 0) ::close(fd);
    live = s->live_conn_count();
    if (live <= 5) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  EXPECT_LE(live, 5u) << "conn handler threads not reaped";
  s->Stop();
}

TEST(Metrics, RemoteStatsOp) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_b";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v(50, 'x');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "k"), v.data(), v.size()), Status::kOk);
  std::string text;
  ASSERT_EQ(t.Stats(addr, &text), Status::kOk);
  EXPECT_NE(text.find("dfkv_cache_put_total 1"), std::string::npos) << text;
  s->Stop();
}

TEST(Metrics, RemoteStatsIncludesRuntimeExtension) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_extension";
  auto s = Start(dir, &addr);
  s->set_metrics_extension([] {
    return std::string(
        "# TYPE dfkv_server_ready gauge\n"
        "dfkv_server_ready 1\n"
        "# TYPE dfkv_rdma_v2_ready gauge\n"
        "dfkv_rdma_v2_ready 1\n");
  });
  EXPECT_EQ(s->MetricsText().find("dfkv_server_ready"), std::string::npos);
  EXPECT_NE(s->FullMetricsText().find("dfkv_server_ready 1"),
            std::string::npos);
  TcpTransport t;
  std::string text;
  ASSERT_EQ(t.Stats(addr, &text), Status::kOk);
  EXPECT_NE(text.find("dfkv_server_ready 1"), std::string::npos) << text;
  EXPECT_NE(text.find("dfkv_rdma_v2_ready 1"), std::string::npos) << text;
  s->Stop();
}

// exist latency histogram: the exist handler body is sampled into a distinct
// op="exist" series (separate from get/put). Its tail is the first signal to
// check when L3 prefetch stalls — before this it was invisible in metrics and
// only surfaced in the client access log by chance.
TEST(Metrics, ExistLatencyHistogramPresent) {
  std::string addr;
  auto dir = fs::temp_directory_path() / "dfkv_metrics_existlat";
  auto s = Start(dir, &addr);
  TcpTransport t;
  std::string v(100, 'x');
  ASSERT_EQ(t.Cache(addr, ToBlockKey("test/model", "k"), v.data(), v.size()), Status::kOk);
  // Drive enough exist probes that the sampler (1/64) records at least one.
  bool e = false;
  for (int i = 0; i < 4096; ++i) {
    ASSERT_EQ(t.Exist(addr, ToBlockKey("test/model", "k"), &e), Status::kOk);
    ASSERT_EQ(t.Exist(addr, ToBlockKey("test/model", "absent"), &e), Status::kOk);
  }
  std::string text = s->MetricsText();
  // The op="exist" latency series exists and is distinct from get/put.
  EXPECT_NE(text.find("dfkv_op_latency_seconds_count{op=\"exist\"}"),
            std::string::npos) << text;
  // Extract the exist count and assert the sampler recorded something.
  auto pos = text.find("dfkv_op_latency_seconds_count{op=\"exist\"}");
  ASSERT_NE(pos, std::string::npos);
  long cnt = std::stol(text.substr(pos + std::string(
      "dfkv_op_latency_seconds_count{op=\"exist\"}").size()));
  EXPECT_GT(cnt, 0) << "sampler recorded no exist latency over 8192 probes";
  s->Stop();
}
