#ifndef DFKV_CACHE_DISK_LIFETIME_READ_COMPLETION_H_
#define DFKV_CACHE_DISK_LIFETIME_READ_COMPLETION_H_

#include <chrono>
#include <cstddef>

namespace dfkv {

// Ephemeral notification metadata, not a resident field or a heat timestamp.
struct DiskLifetimeReadCompletion {
  enum class Sampling : unsigned char {
    kMissing = 0,     // The producer supplied no completion sampling metadata.
    kNotSampled = 1,  // Eligible completion outside the selected cohort.
    kSampled = 2      // completed_at was captured at producer completion.
  };
  Sampling sampling = Sampling::kMissing;  // Separates missing from unsampled.
  // A same-process steady-clock sample, meaningful only for kSampled.
  std::chrono::steady_clock::time_point completed_at;
};

// First and every 64th enabled nonempty completion per thread, across stores.
// Disabled/empty events read no clock and do not advance this independent
// phase. A surviving thread keeps its phase across Store reconstruction.
DiskLifetimeReadCompletion CaptureDiskLifetimeReadCompletion(bool enabled,
                                                             size_t bytes);

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_LIFETIME_READ_COMPLETION_H_
