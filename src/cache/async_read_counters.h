// Explicitly observed asynchronous payload read completions.
#ifndef DFKV_CACHE_ASYNC_READ_COUNTERS_H_
#define DFKV_CACHE_ASYNC_READ_COUNTERS_H_

#include <cstdint>

namespace dfkv {

// Request-local, owner-thread accounting. Only the executor that consumes an
// actual completion may record it; logical payload lengths are not I/O proof.
struct AsyncReadCounters {
  uint64_t completions = 0;  // Consumed completion results, including retries.
  uint64_t bytes = 0;        // Positive returned bytes, including DIO padding.
  uint64_t errors = 0;       // Negative completion results, including EINTR.
  uint64_t short_reads = 0;  // Nonnegative results below requested, EOF too.

  void Record(long result, uint64_t requested) noexcept {
    ++completions;
    if (result < 0) {
      ++errors;
      return;
    }
    bytes += static_cast<uint64_t>(result);
    if (static_cast<uint64_t>(result) < requested) ++short_reads;
  }
};

}  // namespace dfkv

#endif  // DFKV_CACHE_ASYNC_READ_COUNTERS_H_
