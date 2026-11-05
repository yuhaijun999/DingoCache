#include "cache/disk_lifetime_state.h"

#include <cassert>
#include <cstdint>

#include "cache/disk_lifetime_math.h"

namespace dfkv {

DiskLifetimeState NewDiskLifetimeState(uint32_t commit_at) {
  return {commit_at, commit_at, 1, 0, false};
}

DiskLifetimeState RecoveredDiskLifetimeState(uint8_t virtual_reads) {
  assert(virtual_reads == 1 || virtual_reads == 2);
  return {0, 0, static_cast<uint8_t>(virtual_reads << 2), 0, true};
}

bool ApplyDiskLifetimeRead(const DiskLifetimeConfig& config,
                           uint32_t applied_at, DiskLifetimeState* state) {
  if (applied_at < state->anchor) return false;
  bool early = state->first_early_read();
  if (!state->history_unknown && state->actual_reads == 0) {
    early = DiskLifetimeEarlyRead(config, state->put_at, applied_at);
  }
  const uint8_t reads = state->scoring_reads();
  const uint8_t next_reads = static_cast<uint8_t>(reads < 63 ? reads + 1 : 63);
  // Rebuild all eight bits together: each successful read clears wa; fw is
  // fixed by the first actual read, independently of the virtual scoring seed.
  state->hstate = static_cast<uint8_t>((next_reads << 2) | (early ? 2 : 0));
  if (state->actual_reads < 3) ++state->actual_reads;
  state->anchor = applied_at;
  return true;
}

DiskLifetimeReadClass ClassifyDiskLifetimeRead(const DiskLifetimeConfig& config,
                                               const DiskLifetimeState& state,
                                               uint64_t now) {
  if (state.actual_reads == 0) return DiskLifetimeReadClass::kUnread;
  const uint64_t since_put = now >= state.put_at ? now - state.put_at : 0;
  // R uses original write age (inclusive P), not the refreshed anchor or fw.
  // A recovered resident has no trustworthy put_at and can never enter R.
  if (!state.history_unknown && since_put <= config.protect_sec)
    return DiskLifetimeReadClass::kReadWithinNewWriteWindow;
  if (state.actual_reads == 1)
    return DiskLifetimeReadClass::kReadOnceOutsideNewWriteWindow;
  if (state.actual_reads == 2)
    return DiskLifetimeReadClass::kReadTwiceOutsideNewWriteWindow;
  return DiskLifetimeReadClass::kReadThreeOrMoreOutsideNewWriteWindow;
}

}  // namespace dfkv
