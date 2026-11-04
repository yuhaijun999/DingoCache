#ifndef DFKV_CACHE_DISK_WRITE_RESULT_H_
#define DFKV_CACHE_DISK_WRITE_RESULT_H_

#include "cache/disk_resident_identity.h"
#include "common/status.h"

namespace dfkv {

// Internal only; neither this enum nor resident credentials go on the wire.
// kNone leaves the existing Status meaning intact, including ordinary failures.
enum class DiskWriteReason : unsigned char {
  kNone = 0,
  // Permanent admission failure: eviction/retry cannot replenish generations.
  // It must not be treated as disk I/O corruption or an eviction scan budget.
  kIdentityExhausted = 1
};

struct DiskWriteResult {
  Status status = Status::kInvalid;
  DiskWriteReason reason = DiskWriteReason::kNone;
  // Only a confirmed NEW commit may populate this. Ok may still have no
  // identity (legacy/file, duplicate/follower, or a write removed in flight).
  DiskResidentIdentity identity{};
};

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_WRITE_RESULT_H_
