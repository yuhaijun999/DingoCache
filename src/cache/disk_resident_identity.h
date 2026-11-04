#ifndef DFKV_CACHE_DISK_RESIDENT_IDENTITY_H_
#define DFKV_CACHE_DISK_RESIDENT_IDENTITY_H_

#include <cstdint>

namespace dfkv {

// In-process provenance for one committed disk resident, not a key/slot handle
// or a wire/disk record. Only storage results may establish byte provenance;
// callers must not reconstruct this tuple by looking up a later same-key value.
struct DiskResidentIdentity {
  uint64_t store_instance =
      0;                    // Never reused by tracked stores in this process.
  uint64_t run_epoch = 0;   // Current persisted dirty-run epoch of the store.
  uint64_t generation = 0;  // Reserved once, never recycled within the store.

  // Only checks that the tuple is populated, not that it is still resident
  // or eligible for heat. Storage must revalidate under its own lock.
  bool valid() const {
    return store_instance != 0 && run_epoch != 0 && generation != 0;
  }
  bool operator==(const DiskResidentIdentity& other) const {
    return store_instance == other.store_instance &&
           run_epoch == other.run_epoch && generation == other.generation;
  }
};

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_RESIDENT_IDENTITY_H_
