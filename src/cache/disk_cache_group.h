/* DiskCacheGroup — one cache node spanning multiple NVMe SSDs, mirroring
 * dingo-cache's intra-node layout (--cache_dir=d1,d2,d3). Each disk is an
 * independent KVStore (own LRU); a block is routed to one disk by a second-level
 * Ketama hash on BlockKey.Filename(). Total capacity is split evenly across
 * disks. A single-disk group degenerates to one KVStore. */
#ifndef DFKV_DISK_CACHE_GROUP_H_
#define DFKV_DISK_CACHE_GROUP_H_

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "cache/disk_slab_store.h"
#include "cache/kv_store.h"
#include "cache/store_engine.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "utils/con_hash.h"

namespace dfkv {

enum class DiskEvictionPolicy : unsigned char {
  kInvalid = 0,  // Empty or unrecognized policy name.
  kLegacy = 1,   // Preserve the existing disk eviction strategy.
  kLifetime = 2  // Use lifetime scoring once the new strategy is enabled.
};

// Parses a policy name without reading environment or opening storage. ASCII
// letter case is ignored; whitespace is not trimmed and aliases are rejected.
// Empty/unknown input returns kInvalid. The caller handles missing input and
// defaults before calling; no input view is retained. Accepting "lifetime"
// here does not enable that storage strategy.
DiskEvictionPolicy ParseDiskEvictionPolicy(std::string_view text);

enum class DiskEvictionPolicySource : unsigned char {
  kDefault = 0,
  kEnvironment = 1,
  kCommandLine = 2
};
enum class DiskEvictionEngine : unsigned char { kSlab = 0, kFile = 1 };

struct DiskEvictionConfigInput {
  std::optional<std::string_view> cli_policy;
  std::optional<std::string_view> env_policy;
  std::optional<std::string_view> cli_engine;
  std::optional<std::string_view> env_engine;
};

struct DiskEvictionConfigResult {
  Status status = Status::kOk;
  std::string error;
  // Selected request, not the running strategy. Only use when status is kOk.
  DiskEvictionPolicy policy = DiskEvictionPolicy::kLifetime;
  DiskEvictionPolicySource source = DiskEvictionPolicySource::kDefault;
  DiskEvictionEngine engine = DiskEvictionEngine::kSlab;
};

// Resolves supplied values only; does not inspect the process environment or
// open storage. A missing policy requests lifetime by default; explicit
// lifetime is valid only with slab. File permits default/explicit legacy and
// has no effective disk eviction policy. This validates configuration, not
// algorithm readiness: startup must separately reject an unimplemented strategy
// before opening disks. Callers must reject a non-OK result before constructing
// disks.
DiskEvictionConfigResult ResolveDiskEvictionConfig(
    const DiskEvictionConfigInput& input);

class DiskCacheGroup {
 public:
  struct Options {
    std::vector<std::string> cache_dirs;   // one per NVMe SSD
    uint64_t capacity_bytes = (1ull << 30);  // TOTAL, split evenly across disks
    // Empty reads DFKV_STORE_ENGINE, then selects the production slab backend.
    // "file" remains available only when explicitly requested for diagnostics.
    std::string engine{};
    // Dependency injection for focused concurrency tests. Production leaves
    // this empty and constructs the selected file/slab engine normally.
    std::function<std::unique_ptr<StoreEngine>(const std::string&, uint64_t)>
        engine_factory{};
  };

  explicit DiskCacheGroup(Options opt);
  bool Healthy() const;
  const std::string& StartupError() const { return startup_error_; }

  Status Cache(const BlockKey& key, const void* data, size_t len);
  Status CacheDirect(const BlockKey& key, char* data, size_t len, size_t cap);
  // Batched CacheDirect: split by disk route, one engine batch per disk.
  // `observe` runs once per participating disk after its engine call.
  using DirectBatchObserver = std::function<void(
      size_t disk, size_t objects, uint64_t bytes, size_t failures,
      double elapsed_seconds)>;
  std::vector<Status> CacheDirectBatch(
      const std::vector<StoreEngine::CacheBatchItem>& items,
      const DirectBatchObserver& observe = {});
  Status Range(const BlockKey& key, uint64_t offset, uint64_t length,
               std::string* out, size_t* value_len = nullptr);
  Status RangeInto(const BlockKey& key, uint64_t offset, uint64_t length,
                   char* dst, size_t dst_cap, size_t* out_len,
                   size_t* value_len = nullptr);
  Status RangeDirect(const BlockKey& key, uint64_t offset, uint64_t length,
                     char* io_buf, size_t io_cap, const char** out_data,
                     size_t* out_len, size_t* value_len = nullptr);
  // Cheap prep half of RangeDirect (no disk read); the returned lease carries
  // descriptor and storage-pin ownership through asynchronous queues.
  Status RangeDirectPrep(const BlockKey& key, uint64_t offset, uint64_t length,
                         size_t io_cap, ReadLease* out);
  bool IsCached(const BlockKey& key) const;
  Status Lookup(const BlockKey& key, ValueMetadata* out) const;

  // Drop a cached block from its owning disk (routes like IsCached). kOk if
  // removed, kNotFound if absent. Backs the LMCache L2 eviction path.
  Status Remove(const BlockKey& key);

  uint64_t UsedBytes() const;   // summed across disks
  size_t Count() const;         // summed across disks
  size_t DiskCount() const { return disks_.size(); }
  uint64_t Evictions() const;     // summed across disks
  uint64_t EvictedBytes() const;  // summed across disks
  // The storage backend actually constructed ("file" | "slab") -- the RESOLVED
  // choice (Options.engine / DFKV_STORE_ENGINE / default), not the flag intent.
  const std::string& EngineName() const { return engine_; }
  // Slab I/O mode actually in effect ("direct" | "buffered"; empty for file
  // engine). Direct is the deployment default; a filesystem that rejects
  // O_DIRECT (tmpfs) resolves to buffered regardless of the env.
  const std::string& WriteMode() const { return write_mode_; }
  // Summed slab runtime counters across disks (all-zero for the file engine).
  DiskSlabStore::Stats SlabStats() const;
  // Live size-class layout, aggregated by slot size across all slab disks.
  std::vector<SlabAllocator::ClassStat> SlabClasses() const;
  // Per-disk views for fine-grained metrics (i in [0, DiskCount)).
  const std::string& DiskPath(size_t i) const { return disks_[i]->Dir(); }
  uint64_t DiskUsedBytes(size_t i) const { return disks_[i]->UsedBytes(); }
  size_t DiskObjects(size_t i) const { return disks_[i]->Count(); }
  // Slab-only, indexed identically to DiskPath; one coherent observation view.
  DiskSlabStore::WatermarkObservation DiskWatermarkObservation(size_t i) const {
    return slabs_[i]->GetWatermarkObservation();
  }
  uint64_t TenantUsedBytes(uint64_t tenant_hash) const;
  uint64_t TenantDefaultQuotaBytes() const { return tenant_default_quota_bytes_; }
  uint64_t TenantQuotaBytes(uint64_t tenant_hash) const {
    return TenantLimit(tenant_hash);
  }
  struct TenantQuotaMetric {
    uint64_t tenant_hash = 0;
    uint64_t limit_bytes = 0;
    uint64_t used_bytes = 0;
    uint64_t rejections = 0;
  };
  std::vector<TenantQuotaMetric> ConfiguredTenantQuotaMetrics() const;
  uint64_t TenantQuotaRejections() const {
    return tenant_quota_rejections_.load(std::memory_order_relaxed);
  }

 private:
  static constexpr size_t kInvalidDisk = std::numeric_limits<size_t>::max();

  size_t RouteIndex(const BlockKey& key) const;
  StoreEngine* Route(const BlockKey& key) const;
  static constexpr size_t kTenantStripes = 256;
  size_t TenantStripe(uint64_t tenant_hash) const {
    return static_cast<size_t>(tenant_hash) & (kTenantStripes - 1);
  }
  uint64_t TenantLimit(uint64_t tenant_hash) const;
  bool QuotaAllowsNewLocked(uint64_t tenant_hash, uint64_t current,
                            uint64_t additional) const;
  void RecordQuotaRejectionLocked(uint64_t tenant_hash);
  bool healthy_ = true;
  std::string startup_error_;
  std::string engine_;      // resolved backend name (see EngineName)
  std::string write_mode_;  // resolved slab I/O mode (see WriteMode)
  std::vector<std::unique_ptr<StoreEngine>> disks_;
  std::vector<const DiskSlabStore*> slabs_;  // typed view of disks_ (slab engine only)
  std::unordered_map<std::string, size_t> by_id_;  // disk id -> stable index
  ConHash ring_;
  mutable std::array<std::mutex, kTenantStripes> tenant_stripes_;
  std::map<uint64_t, uint64_t> tenant_quotas_;
  std::map<uint64_t, uint64_t> tenant_quota_rejections_by_hash_;
  uint64_t tenant_default_quota_bytes_ = 0;
  std::string tenant_quotas_file_;
  std::atomic<uint64_t> tenant_quota_rejections_{0};
};

}  // namespace dfkv

#endif  // DFKV_DISK_CACHE_GROUP_H_
