#ifndef DFKV_CACHE_DISK_WRITE_RESULT_H_
#define DFKV_CACHE_DISK_WRITE_RESULT_H_

#include <array>
#include <cstddef>
#include <cstdint>

#include "cache/disk_resident_identity.h"
#include "common/status.h"

namespace dfkv {

// Internal only; neither this enum nor resident credentials go on the wire.
// kNone leaves the existing Status meaning intact, including ordinary failures.
enum class DiskWriteReason : unsigned char {
  kNone = 0,
  // Permanent admission failure: eviction/retry cannot replenish generations.
  // It must not be treated as disk I/O corruption or an eviction scan budget.
  kIdentityExhausted = 1,
  // New reclaim/bind decisions stopped at the clock's hard uint32 bound.
  // Existing bound free slots and already-started persistence may finish.
  // This is not disk corruption; only the exact CacheFull pair is terminal.
  kLifetimeClockExhausted = 2,
  // This PUT consumed its finite class-scan budget without admission. Only the
  // exact CacheFull pair is backpressure, not proof of I/O corruption.
  kScanBudgetExhausted = 3
};

// A terminal event, not a live-residency query or proof of equal input bytes.
enum class DiskWriteOutcome : unsigned char {
  kUnknown = 0,  // Failure, or a backend without classified write results.
  kNew = 1,      // This store invocation committed its own new payload.
  // This invocation kept an existing value or joined another store owner.
  // It did not commit its own input; the resident may already be gone.
  kKeptExisting = 2,
  kRemoved = 3  // An in-flight Remove won and final release succeeded.
};

struct DiskWriteResult {
  Status status = Status::kInvalid;
  DiskWriteReason reason = DiskWriteReason::kNone;
  // Only a confirmed NEW commit may populate this. Ok may still have no
  // identity (legacy/file, duplicate/follower, or a write removed in flight).
  DiskResidentIdentity identity{};
  DiskWriteOutcome outcome = DiskWriteOutcome::kUnknown;
};

// Disjoint final-result categories. Reasons refine CacheFull only; an I/O
// failure carrying a stale admission reason is still an I/O failure.
enum class DiskWriteTerminal : unsigned char {
  kOk = 0,                 // Successful return, not proof of current residency.
  kCacheFull = 1,          // Capacity failure without a classified reason.
  kScanBudget = 2,         // Exact CacheFull + finite scan budget exhaustion.
  kIdentityExhausted = 3,  // Exact CacheFull + resident identity exhaustion.
  kClockExhausted = 4,     // Exact CacheFull + lifetime clock exhaustion.
  kIOError = 5,            // I/O or existing cancellation error contract.
  kInvalid = 6,            // Invalid input/result.
  kOther = 7               // Other existing Status values.
};

inline constexpr size_t kDiskWriteTerminalCount = 8;
using DiskWriteTerminalCounts = std::array<uint64_t, kDiskWriteTerminalCount>;

inline DiskWriteTerminal ClassifyDiskWriteTerminal(
    Status status, DiskWriteReason reason = DiskWriteReason::kNone) {
  if (status == Status::kOk) return DiskWriteTerminal::kOk;
  if (status == Status::kIOError) return DiskWriteTerminal::kIOError;
  if (status == Status::kInvalid) return DiskWriteTerminal::kInvalid;
  if (status != Status::kCacheFull) return DiskWriteTerminal::kOther;
  switch (reason) {
    case DiskWriteReason::kScanBudgetExhausted:
      return DiskWriteTerminal::kScanBudget;
    case DiskWriteReason::kIdentityExhausted:
      return DiskWriteTerminal::kIdentityExhausted;
    case DiskWriteReason::kLifetimeClockExhausted:
      return DiskWriteTerminal::kClockExhausted;
    default:
      return DiskWriteTerminal::kCacheFull;
  }
}

// Indexed by DiskWriteTerminal, also used by bounded metric exposition.
inline constexpr std::array<const char*, kDiskWriteTerminalCount>
    kDiskWriteTerminalNames = {"ok",
                               "cache_full",
                               "scan_budget",
                               "identity_exhausted",
                               "clock_exhausted",
                               "io_error",
                               "invalid",
                               "other"};

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_WRITE_RESULT_H_
