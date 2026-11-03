#include "cache/disk_lifetime_clock.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <utility>

namespace dfkv {
namespace {
uint64_t SteadySeconds() {
  const int64_t seconds =
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  return seconds > 0 ? static_cast<uint64_t>(seconds) : 0;
}
}  // namespace

DiskLifetimeClock::DiskLifetimeClock(uint64_t horizon_sec)
    : DiskLifetimeClock(horizon_sec, SteadySeconds) {}

DiskLifetimeClock::DiskLifetimeClock(uint64_t horizon_sec, NowFn now)
    : horizon_sec_(horizon_sec), now_(std::move(now)) {
  if (now_) origin_sec_ = now_();
}

bool DiskLifetimeClock::TryNow(uint32_t* offset) const {
  if (horizon_sec_ == 0 ||
      horizon_sec_ >
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ||
      !now_) {
    return false;
  }
  const uint64_t sample = now_();
  const uint64_t elapsed = sample >= origin_sec_ ? sample - origin_sec_ : 0;
  uint64_t observed = elapsed_sec_.load(std::memory_order_relaxed);
  // Only advancing seconds need a CAS. The atomic protects this numeric floor,
  // not resident metadata; the caller must still update residents under the
  // store lock. No ordering of unrelated memory is required here.
  while (observed < elapsed &&
         !elapsed_sec_.compare_exchange_weak(observed, elapsed,
                                             std::memory_order_relaxed)) {
  }
  const uint64_t monotonic_elapsed = std::max(observed, elapsed);
  const uint64_t limit = std::numeric_limits<uint32_t>::max() - horizon_sec_;
  if (monotonic_elapsed > limit) return false;
  *offset = static_cast<uint32_t>(monotonic_elapsed);
  return true;
}

}  // namespace dfkv
