#ifndef DFKV_CACHE_DISK_LIFETIME_STATE_H_
#define DFKV_CACHE_DISK_LIFETIME_STATE_H_

#include <cstdint>

#include "cache/disk_lifetime_math.h"

namespace dfkv {

// Logical business-read classes, not physical eviction queues or score bands.
// Independent of MAX_READS: increasing score bands never adds enum values.
// Matches spec W/R/O1/O2/O3 respectively. Recovered history has no known NEW
// write window, so its successful reads use the outside-window classes.
enum class DiskLifetimeReadClass : unsigned char {
  kUnread =
      0,  // No successful business reads, including unread recovered data.
  // NEW resident with >=1 successful read and now - put_at <= P.
  kReadWithinNewWriteWindow = 1,
  // Outside the NEW write window: exactly one successful read; also recovered
  // data's first read.
  kReadOnceOutsideNewWriteWindow = 2,
  // Outside the NEW write window: exactly two successful reads; also recovered
  // data's second read.
  kReadTwiceOutsideNewWriteWindow = 3,
  // Outside the NEW write window: three OR MORE successful reads (third,
  // fourth, fifth, etc.).
  kReadThreeOrMoreOutsideNewWriteWindow = 4
};

// In-memory value only. Create through the factories below; the owner must
// serialize reads/updates with resident identity, queues and bucket accounting.
// No clock, locks, storage I/O or automatic heat notification lives here.
struct DiskLifetimeState {
  uint32_t anchor = 0;  // Last applied success; zero is a valid clock offset.
  uint32_t put_at =
      0;  // Original NEW commit, trusted only if !history_unknown.
  // bit0=write-armed (wa), bit1=first-early-read (fw), bits2..7=scoring n.
  uint8_t hstate = 0;
  uint8_t actual_reads = 0;      // Business reads: 0, 1, 2 or saturated >=3.
  bool history_unknown = false;  // Persists after a recovered resident is read.

  uint8_t scoring_reads() const { return static_cast<uint8_t>(hstate >> 2); }
  bool write_armed() const { return (hstate & 1) != 0; }
  bool first_early_read() const { return (hstate & 2) != 0; }
};

DiskLifetimeState NewDiskLifetimeState(uint32_t commit_at);
// Explicit virtual seed must be 1 (staged) or 2 (final clean recovery), never
// business reads. Anchor is zero: the common process startup origin, not the
// time each record was recovered. This does not wire recovery or enable policy.
DiskLifetimeState RecoveredDiskLifetimeState(uint8_t virtual_reads);

// Requires validated config and a non-null, well-formed state from a factory.
// Call ONLY for an accepted nonzero logical success after identity validation.
// applied_at comes from the shared bounded clock UNDER the resident owner lock,
// not completed_at. Rejects regressing time without changing state; equal-time
// independent requests still each count once. The caller handles deduplication,
// clock exhaustion and atomic publication with its indexes before unlocking.
bool ApplyDiskLifetimeRead(const DiskLifetimeConfig& config,
                           uint32_t applied_at, DiskLifetimeState* state);
DiskLifetimeReadClass ClassifyDiskLifetimeRead(const DiskLifetimeConfig& config,
                                               const DiskLifetimeState& state,
                                               uint64_t now);

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_LIFETIME_STATE_H_
