#ifndef DFKV_CACHE_DISK_LIFETIME_MATH_H_
#define DFKV_CACHE_DISK_LIFETIME_MATH_H_

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace dfkv {

// Inert numeric inputs: no environment reads, storage access or policy switch.
struct DiskLifetimeConfig {
  // BASE: baseline score in seconds, before read rewards or aging (7 days).
  uint64_t base_sec = 604800;
  // BONUS: score added per effective read band (3 days 12 hours).
  uint64_t read_bonus_sec = 302400;
  // MAX_READS: cap on min(n + fw, MAX_READS), not on actual business reads.
  // Valid range: 1..63; the default provides score bands 0, 1 and 2.
  uint64_t max_reads = 2;
  // P: new-write slow-aging/BOOST window and first-early-read cutoff (3 min).
  // A successful read disarms write protection even before this window ends.
  uint64_t protect_sec = 180;
  // SLOW: while a write is armed and age < P, charge floor(age / SLOW).
  // Default: one second of score aging per four elapsed seconds.
  uint64_t slowdown = 4;
  // BOOST: extra score for an armed new write while age < P, in seconds.
  // Missing means BASE + BONUS * MAX_READS, including overrides (14 days
  // with all defaults). Withdrawn on a successful read or at age == P.
  std::optional<uint64_t> boost_sec;
  // KEEP: soft-protection threshold in score seconds (5 days 12 hours).
  // Only score > KEEP is protected; KEEP=0 is valid, not a disable switch.
  uint64_t keep_score_sec = 475200;
  // G: width of each protection-expiry time bucket (12 hours).
  uint64_t bucket_sec = 43200;
  // B: number of expiry buckets, valid range 1..64. Default B*G spans
  // 12 days; validation requires coverage of the protection horizon H.
  uint64_t bucket_count = 24;
  // Low watermark: target disk occupancy after pressure eviction (88%).
  uint64_t low_pct = 88;
  // High watermark: disk occupancy that triggers pressure eviction (92%).
  // Zero disables this watermark path, not foreground capacity eviction.
  uint64_t high_pct = 92;
};

// Strict unsigned decimal; no signs or whitespace. value must be non-null.
// Failure leaves value intact.
// Diagnostics include the parameter name, supplied value and constraint.
bool ParseDiskLifetimeUnsigned(std::string_view name, std::string_view text,
                               uint64_t* value, std::string* error);
bool ValidateDiskLifetimeConfig(const DiskLifetimeConfig& config,
                                std::string* error);

// All math below requires a validated config. No wall clock is read: callers
// supply monotonic seconds. Anchor zero is valid. Runtime clock-drain admission
// is a separate L12 obligation, not implemented by these pure functions.
uint64_t DiskLifetimeHorizon(const DiskLifetimeConfig& config);
uint8_t DiskLifetimeBand(const DiskLifetimeConfig& config, uint8_t reads,
                         bool first_early_read);
// Predicate only: the caller must restrict it to the first actual successful
// read of a new resident; restored history never earns this reward.
bool DiskLifetimeEarlyRead(const DiskLifetimeConfig& config, uint64_t put_at,
                           uint64_t applied_at);

// reads is the saturated scoring count (0..63), not the actual business count.
// write_armed implies reads==0 and !first_early_read. Only successful NEW PUTs
// arm BOOST; a successful read disarms it. Age subtraction saturates at zero,
// and scores below INT64_MIN saturate there (never wrap).
int64_t DiskLifetimeScore(const DiskLifetimeConfig& config, uint32_t anchor,
                          uint8_t reads, bool first_early_read,
                          bool write_armed, uint64_t now);
// First integer second at which score <= KEEP; protection uses now < end.
// A uint32 anchor plus the validated horizon fits uint64 without wrapping.
uint64_t DiskLifetimeProtectEnd(const DiskLifetimeConfig& config,
                                uint32_t anchor, uint8_t reads,
                                bool first_early_read, bool write_armed);

}  // namespace dfkv

#endif  // DFKV_CACHE_DISK_LIFETIME_MATH_H_
