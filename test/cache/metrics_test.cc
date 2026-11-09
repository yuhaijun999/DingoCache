// TDD R11 — server metrics counters + Prometheus text + remote Stats op.
#include "cache/kv_node_server.h"
#include "client/key_map.h"
#include "utils/net_util.h"
#include "transport/tcp_transport.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <thread>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "cache/disk_cache_group.h"
#include "cache/disk_slab_store.h"
#include "cache/slab_allocator.h"
#include "common/status.h"

namespace dfkv {
class KvNodeServerTestPeer {
 public:
  static DiskCacheGroup& Group(KvNodeServer& server) { return server.group_; }
};

class DiskCacheGroupTestPeer {
 public:
  static DiskSlabStore& Slab(DiskCacheGroup& group, size_t index) {
    return *group.slabs_.at(index);
  }
};

class DiskSlabStoreTestPeer {
 public:
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
  EXPECT_NE(text.find("dfkv_slab_capacity_bytes 1073741824"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_allocated_bytes 4096"),
            std::string::npos)
      << text;
  EXPECT_NE(text.find("dfkv_slab_payload_bytes 3000"), std::string::npos)
      << text;
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
        "dfkv_slab_lifetime_donor_released_residents_total"}) {
    EXPECT_NE(text.find(std::string(name) + " 0\n"), std::string::npos);
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
