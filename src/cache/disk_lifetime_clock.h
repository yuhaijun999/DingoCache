#ifndef DFKV_CACHE_DISK_LIFETIME_CLOCK_H_
#define DFKV_CACHE_DISK_LIFETIME_CLOCK_H_

#include <atomic>
#include <cstdint>
#include <functional>

namespace dfkv {

// One shared time origin for all lifetime-enabled disks in a server instance.
// The future server owner must construct this before recovering any disk and
// share it, rather than creating a separate origin in each store. This helper
// does not itself install the server admission/drain gate or enable lifetime.
class DiskLifetimeClock {
 public:
  // Absolute monotonic whole seconds, not wall time. Injected sources must be
  // thread-safe when shared callers use TryNow concurrently. They must not
  // throw. Whole-second quantization can tick before a full second has elapsed
  // since construction; origin and samples always use the same units.
  using NowFn = std::function<uint64_t()>;

  // H is fixed for this clock's lifetime. Valid range: 1..INT32_MAX, normally
  // obtained from DiskLifetimeHorizon after validating the numeric config.
  explicit DiskLifetimeClock(uint64_t horizon_sec);
  DiskLifetimeClock(uint64_t horizon_sec, NowFn now);

  // Returns a relative second offset, including valid zero, only while it is
  // <= UINT32_MAX-H. Invalid H, an empty source, or exhaustion returns false
  // without changing *offset. offset must be non-null.
  // The observed age never decreases at its atomic linearization point, even
  // if a faulty injected source regresses. Exhaustion cannot clear by clock
  // rollback: only a new instance starts a new origin. Callers must react to
  // false BEFORE changing time metadata; this is not the full shutdown gate.
  bool TryNow(uint32_t* offset) const;

 private:
  const uint64_t horizon_sec_;
  const NowFn now_;
  uint64_t origin_sec_ = 0;
  mutable std::atomic<uint64_t> elapsed_sec_{0};
};

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_LIFETIME_CLOCK_H_
