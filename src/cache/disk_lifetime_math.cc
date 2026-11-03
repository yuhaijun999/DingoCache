#include "cache/disk_lifetime_math.h"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>

namespace dfkv {
namespace {
constexpr uint64_t kMaxScore = std::numeric_limits<int64_t>::max();

bool Reject(std::string_view name, std::string_view value,
            std::string_view constraint, std::string* error) {
  if (error != nullptr) {
    *error = std::string(name) + "='" + std::string(value) + "' " +
             std::string(constraint);
  }
  return false;
}

bool CheckRange(std::string_view name, uint64_t value, uint64_t low,
                uint64_t high, std::string* error) {
  return (value >= low && value <= high) ||
         Reject(
             name, std::to_string(value),
             "must be in " + std::to_string(low) + ".." + std::to_string(high),
             error);
}

uint64_t MaxLifetime(const DiskLifetimeConfig& config) {
  return config.base_sec + config.read_bonus_sec * config.max_reads;
}

uint64_t Boost(const DiskLifetimeConfig& config) {
  return config.boost_sec.value_or(MaxLifetime(config));
}

uint64_t Age(uint64_t anchor, uint64_t now) {
  return now >= anchor ? now - anchor : 0;
}

uint64_t Lifetime(const DiskLifetimeConfig& config, uint8_t reads,
                  bool first_early_read) {
  assert(reads <= 63);
  return config.base_sec +
         config.read_bonus_sec *
             DiskLifetimeBand(config, reads, first_early_read);
}
}  // namespace

bool ParseDiskLifetimeUnsigned(std::string_view name, std::string_view text,
                               uint64_t* value, std::string* error) {
  uint64_t parsed = 0;
  if (text.empty() || text.front() < '0' || text.front() > '9') {
    return Reject(name, text, "must be an unsigned decimal uint64", error);
  }
  const std::from_chars_result result =
      std::from_chars(text.data(), text.data() + text.size(), parsed);
  if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
    return Reject(name, text, "must be an unsigned decimal uint64", error);
  }
  *value = parsed;
  if (error != nullptr) error->clear();
  return true;
}

bool ValidateDiskLifetimeConfig(const DiskLifetimeConfig& c,
                                std::string* error) {
  if (error != nullptr) error->clear();
  if (!CheckRange("DFKV_SLAB_LIFE_BASE_SEC", c.base_sec, 1, kMaxScore, error) ||
      !CheckRange("DFKV_SLAB_LIFE_READ_BONUS_SEC", c.read_bonus_sec, 0,
                  kMaxScore, error) ||
      !CheckRange("DFKV_SLAB_LIFE_MAX_READS", c.max_reads, 1, 63, error) ||
      !CheckRange("DFKV_SLAB_PROTECT_SEC", c.protect_sec, 1, kMaxScore,
                  error) ||
      !CheckRange("DFKV_SLAB_PROTECT_SLOWDOWN", c.slowdown, 1, UINT64_MAX,
                  error) ||
      !CheckRange("DFKV_SLAB_BUCKET_SEC", c.bucket_sec, 1, kMaxScore, error) ||
      !CheckRange("DFKV_SLAB_BUCKET_COUNT", c.bucket_count, 1, 64, error) ||
      !CheckRange("DFKV_SLAB_KEEP_SCORE_SEC", c.keep_score_sec, 0,
                  c.base_sec - 1, error) ||
      !CheckRange("DFKV_SLAB_EVICT_LOW_PCT", c.low_pct, 0, 100, error) ||
      !CheckRange("DFKV_SLAB_EVICT_HIGH_PCT", c.high_pct, 0, 100, error)) {
    return false;
  }
  if (c.high_pct != 0 && c.low_pct >= c.high_pct) {
    return Reject(
        "DFKV_SLAB_EVICT_LOW_PCT", std::to_string(c.low_pct),
        "must be below DFKV_SLAB_EVICT_HIGH_PCT=" + std::to_string(c.high_pct),
        error);
  }
  // Check before multiplying or adding: validation itself must not overflow.
  if (c.read_bonus_sec > (kMaxScore - c.base_sec) / c.max_reads) {
    return Reject("DFKV_SLAB_LIFE_READ_BONUS_SEC",
                  std::to_string(c.read_bonus_sec),
                  "with BASE=" + std::to_string(c.base_sec) +
                      " and MAX_READS=" + std::to_string(c.max_reads) +
                      " must satisfy BASE + BONUS * MAX_READS <= INT64_MAX",
                  error);
  }
  const uint64_t boost = Boost(c);
  if (boost > kMaxScore - c.base_sec) {
    return Reject("DFKV_SLAB_PROTECT_BOOST_SEC", std::to_string(boost),
                  "with BASE=" + std::to_string(c.base_sec) +
                      " must satisfy BASE + BOOST <= INT64_MAX",
                  error);
  }
  const int64_t last_write =
      static_cast<int64_t>(c.base_sec + boost) -
      static_cast<int64_t>((c.protect_sec - 1) / c.slowdown);
  if (last_write <= static_cast<int64_t>(MaxLifetime(c))) {
    return Reject("DFKV_SLAB_PROTECT_BOOST_SEC", std::to_string(boost),
                  "must make BASE-floor((P-1)/SLOW)+BOOST > "
                  "BASE+BONUS*MAX_READS (BASE=" +
                      std::to_string(c.base_sec) +
                      ", BONUS=" + std::to_string(c.read_bonus_sec) +
                      ", MAX_READS=" + std::to_string(c.max_reads) +
                      ", P=" + std::to_string(c.protect_sec) +
                      ", SLOW=" + std::to_string(c.slowdown) + ")",
                  error);
  }
  // Reassociate P - floor(P/SLOW) first: nonnegative, and no overflowing
  // intermediate Lmax-KEEP+P even when the final expression would fit.
  const uint64_t tail = c.protect_sec - c.protect_sec / c.slowdown;
  const uint64_t remaining = MaxLifetime(c) - c.keep_score_sec;
  const uint64_t limit = std::numeric_limits<int32_t>::max();
  if (c.protect_sec > limit || c.bucket_sec > limit || remaining > limit ||
      tail > limit - remaining ||
      std::max(c.protect_sec, remaining + tail) > limit - c.bucket_sec) {
    return Reject("DFKV_SLAB_BUCKET_SEC", std::to_string(c.bucket_sec),
                  "with BASE=" + std::to_string(c.base_sec) +
                      ", BONUS=" + std::to_string(c.read_bonus_sec) +
                      ", MAX_READS=" + std::to_string(c.max_reads) +
                      ", KEEP=" + std::to_string(c.keep_score_sec) +
                      ", P=" + std::to_string(c.protect_sec) +
                      ", SLOW=" + std::to_string(c.slowdown) +
                      " must give H=max(P,Lmax-KEEP+P-floor(P/SLOW))+G "
                      "<= INT32_MAX",
                  error);
  }
  // H bounds G to INT32_MAX; B<=64, so this product now fits uint64.
  if (c.bucket_count * c.bucket_sec < DiskLifetimeHorizon(c)) {
    return Reject(
        "DFKV_SLAB_BUCKET_COUNT", std::to_string(c.bucket_count),
        "with G=" + std::to_string(c.bucket_sec) +
            " must satisfy B*G >= H=" + std::to_string(DiskLifetimeHorizon(c)),
        error);
  }
  return true;
}

uint64_t DiskLifetimeHorizon(const DiskLifetimeConfig& c) {
  return std::max(c.protect_sec,
                  MaxLifetime(c) - c.keep_score_sec +
                      (c.protect_sec - c.protect_sec / c.slowdown)) +
         c.bucket_sec;
}

uint8_t DiskLifetimeBand(const DiskLifetimeConfig& c, uint8_t reads,
                         bool first_early_read) {
  // Widen before adding: raw n=63 and fw=1 sum to 64, not a six-bit zero.
  return static_cast<uint8_t>(std::min<uint64_t>(
      static_cast<unsigned>(reads) + first_early_read, c.max_reads));
}

bool DiskLifetimeEarlyRead(const DiskLifetimeConfig& c, uint64_t put_at,
                           uint64_t applied_at) {
  return Age(put_at, applied_at) < c.protect_sec;
}

int64_t DiskLifetimeScore(const DiskLifetimeConfig& c, uint32_t anchor,
                          uint8_t reads, bool first_early_read,
                          bool write_armed, uint64_t now) {
  assert(!write_armed || (reads == 0 && !first_early_read));
  const uint64_t life = Lifetime(c, reads, first_early_read);
  const uint64_t age = Age(anchor, now);
  if (write_armed && age < c.protect_sec) {
    return static_cast<int64_t>(life + Boost(c)) -
           static_cast<int64_t>(age / c.slowdown);
  }
  const uint64_t aged =
      write_armed ? age - c.protect_sec + c.protect_sec / c.slowdown : age;
  if (aged <= life) return static_cast<int64_t>(life - aged);
  const uint64_t deficit = aged - life;
  // Defensive for synthetic uint64 times beyond the admitted runtime domain.
  // The real clock-drain gate must still reject updates before anchor wraps.
  return deficit > kMaxScore ? std::numeric_limits<int64_t>::min()
                             : -static_cast<int64_t>(deficit);
}

uint64_t DiskLifetimeProtectEnd(const DiskLifetimeConfig& c, uint32_t anchor,
                                uint8_t reads, bool first_early_read,
                                bool write_armed) {
  assert(!write_armed || (reads == 0 && !first_early_read));
  const uint64_t life = Lifetime(c, reads, first_early_read);
  const uint64_t duration =
      write_armed ? std::max(c.protect_sec,
                             life - c.keep_score_sec +
                                 (c.protect_sec - c.protect_sec / c.slowdown))
                  : life - c.keep_score_sec;
  return static_cast<uint64_t>(anchor) + duration;
}
}  // namespace dfkv
