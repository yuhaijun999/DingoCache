#include "cache/disk_lifetime_read_completion.h"

#include <chrono>
#include <cstddef>

namespace dfkv {

DiskLifetimeReadCompletion CaptureDiskLifetimeReadCompletion(bool enabled,
                                                             size_t bytes) {
  if (!enabled || bytes == 0) return {};
  thread_local unsigned char phase = 0;
  const bool sampled = phase == 0;
  phase = static_cast<unsigned char>((phase + 1U) & 63U);
  DiskLifetimeReadCompletion completion;
  completion.sampling = sampled
                            ? DiskLifetimeReadCompletion::Sampling::kSampled
                            : DiskLifetimeReadCompletion::Sampling::kNotSampled;
  if (sampled) completion.completed_at = std::chrono::steady_clock::now();
  return completion;
}

}  // namespace dfkv
