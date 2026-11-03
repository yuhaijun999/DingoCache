#include "cache/disk_lifetime_math.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

namespace dfkv {
namespace {

TEST(DiskLifetimeMath, StrictDecimalParsingPreservesOutputOnFailure) {
  for (const std::string text :
       {"", "-1", "+1", " 1", "1 ", "1x", "1.0", "18446744073709551616"}) {
    SCOPED_TRACE(text);
    uint64_t value = 17;
    std::string error;
    EXPECT_FALSE(ParseDiskLifetimeUnsigned("PARAM", text, &value, &error));
    EXPECT_EQ(value, 17U);
    EXPECT_NE(error.find("PARAM='" + text + "'"), std::string::npos);
  }
  uint64_t value = 0;
  std::string error = "old";
  ASSERT_TRUE(ParseDiskLifetimeUnsigned("PARAM", "18446744073709551615", &value,
                                        &error));
  EXPECT_EQ(value, std::numeric_limits<uint64_t>::max());
  EXPECT_TRUE(error.empty());
  EXPECT_TRUE(ParseDiskLifetimeUnsigned("PARAM", "000", &value, nullptr));
  EXPECT_EQ(value, 0U);
}

TEST(DiskLifetimeMath, DefaultsAndDerivedBoost) {
  DiskLifetimeConfig c;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeHorizon(c), 777735U);
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, true, 1), 1814400);
  EXPECT_EQ(DiskLifetimeScore(c, 1, 1, true, false, 1), 1209600);
  EXPECT_EQ(DiskLifetimeProtectEnd(c, 0, 0, false, true), 129735U);
  EXPECT_EQ(DiskLifetimeProtectEnd(c, 0, 2, false, false), 734400U);
  c.base_sec += 100;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, true, 0), 1814600);
  c.boost_sec = 1209600;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, true, 0), 1814500);
}

TEST(DiskLifetimeMath, RejectsEveryNumericRangeAndWatermarkConflict) {
  struct Field {
    uint64_t DiskLifetimeConfig::*member;
    const char* name;
    uint64_t invalid;
  };
  const Field fields[] = {
      {&DiskLifetimeConfig::base_sec, "LIFE_BASE_SEC", 0},
      {&DiskLifetimeConfig::read_bonus_sec, "READ_BONUS_SEC", UINT64_MAX},
      {&DiskLifetimeConfig::max_reads, "MAX_READS", 0},
      {&DiskLifetimeConfig::max_reads, "MAX_READS", 64},
      {&DiskLifetimeConfig::protect_sec, "PROTECT_SEC", 0},
      {&DiskLifetimeConfig::slowdown, "SLOWDOWN", 0},
      {&DiskLifetimeConfig::keep_score_sec, "KEEP_SCORE_SEC", 604800},
      {&DiskLifetimeConfig::bucket_sec, "BUCKET_SEC", 0},
      {&DiskLifetimeConfig::bucket_count, "BUCKET_COUNT", 0},
      {&DiskLifetimeConfig::bucket_count, "BUCKET_COUNT", 65},
      {&DiskLifetimeConfig::low_pct, "LOW_PCT", 101},
      {&DiskLifetimeConfig::high_pct, "HIGH_PCT", 101}};
  DiskLifetimeConfig invalid_config;
  for (const Field& field : fields) {
    SCOPED_TRACE(field.name);
    invalid_config = DiskLifetimeConfig{};
    invalid_config.*(field.member) = field.invalid;
    std::string error;
    EXPECT_FALSE(ValidateDiskLifetimeConfig(invalid_config, &error));
    EXPECT_NE(error.find(field.name), std::string::npos);
    EXPECT_NE(error.find(std::to_string(field.invalid)), std::string::npos);
  }
  DiskLifetimeConfig c;
  c.low_pct = c.high_pct;
  std::string error;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
  c.high_pct = 0;  // Disable the watermark, not foreground eviction.
  EXPECT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  c.boost_sec = 0;
  EXPECT_FALSE(
      ValidateDiskLifetimeConfig(c, &error));  // Strict write priority.
  EXPECT_NE(error.find("BOOST"), std::string::npos);
}

TEST(DiskLifetimeMath, StrictPriorityAndZeroBonusBoundaries) {
  DiskLifetimeConfig c;
  std::string error;
  c.boost_sec = 604844;  // Equal to Lmax at the last protected second.
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
  c.boost_sec = 604845;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_GT(DiskLifetimeScore(c, 0, 0, false, true, 179), 1209600);
  c.read_bonus_sec = 0;
  c.protect_sec = c.slowdown = 1;
  c.boost_sec = 1;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  c.boost_sec = 0;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
}

TEST(DiskLifetimeMath, SlowdownIsAnUnsignedDivisorNotASignedScore) {
  DiskLifetimeConfig c;
  c.slowdown = UINT64_MAX;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeHorizon(c), 777780U);
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, true, 179), 1814400);
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, true, 180), 604800);
}

TEST(DiskLifetimeMath, ReadBandsAndBucketCoverageFollowMaxReads) {
  for (const uint64_t max_reads : {1U, 2U, 4U, 63U}) {
    DiskLifetimeConfig c;
    c.max_reads = max_reads;
    c.bucket_sec = 604800;
    c.bucket_count = 64;
    std::string error;
    ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
    EXPECT_EQ(DiskLifetimeBand(c, 63, true), max_reads);
    EXPECT_EQ(DiskLifetimeScore(c, 0, 63, true, false, 0),
              604800 + 302400 * max_reads);
    EXPECT_EQ(DiskLifetimeProtectEnd(c, 0, 63, true, false),
              129600 + 302400 * max_reads);
    EXPECT_EQ(DiskLifetimeBand(c, 0, false), 0U);
    EXPECT_EQ(DiskLifetimeBand(c, 1, true), max_reads == 1 ? 1U : 2U);
  }
  DiskLifetimeConfig c;
  c.max_reads = 4;
  std::string error;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
  EXPECT_NE(error.find("BUCKET_COUNT='24'"), std::string::npos);
  c.bucket_count = 33;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeHorizon(c), 1382535U);
}

TEST(DiskLifetimeMath, ChecksOverflowBeforeArithmetic) {
  const uint64_t max = std::numeric_limits<int64_t>::max();
  DiskLifetimeConfig c;
  c.base_sec = max;
  std::string error;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
  EXPECT_NE(error.find("BASE + BONUS * MAX_READS"), std::string::npos);
  c.base_sec = uint64_t{1} << 62;
  c.read_bonus_sec = 0;
  c.max_reads = 1;
  c.boost_sec = c.base_sec;
  c.keep_score_sec = c.base_sec - 1;
  c.protect_sec = c.slowdown = c.bucket_sec = 1;
  c.bucket_count = 2;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
  EXPECT_NE(error.find("BASE + BOOST"), std::string::npos);

  // A huge BASE is legal with a nearby KEEP. Do not reject the unreachable
  // combination of armed write BOOST and a nonzero read band.
  c.base_sec = max - 5;
  c.read_bonus_sec = 1;
  c.boost_sec = 5;
  c.keep_score_sec = c.base_sec - 1;
  c.bucket_count = 3;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, true, 0),
            std::numeric_limits<int64_t>::max());
  EXPECT_EQ(DiskLifetimeScore(c, 0, 1, true, false, 0),
            static_cast<int64_t>(max - 4));
  c.boost_sec = UINT64_MAX;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
}

TEST(DiskLifetimeMath, HorizonRejectsWideTimeAndAcceptsExactBoundary) {
  DiskLifetimeConfig c;
  c.base_sec = 4294967297;
  c.read_bonus_sec = 0;
  c.max_reads = 1;
  c.protect_sec = c.slowdown = 1;
  c.boost_sec = 1;
  c.keep_score_sec = 0;
  c.bucket_sec = 2147483649;
  c.bucket_count = 3;
  std::string error;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
  EXPECT_NE(error.find("INT32_MAX"), std::string::npos);
  c.bucket_sec = 1073741824;
  c.base_sec = 1073741823;
  c.bucket_count = 2;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeHorizon(c), 2147483647U);
  ++c.base_sec;
  EXPECT_FALSE(ValidateDiskLifetimeConfig(c, &error));
}

TEST(DiskLifetimeMath, BoostAndEarlyReadUseDifferentBoundaryRules) {
  const DiskLifetimeConfig c;
  EXPECT_TRUE(DiskLifetimeEarlyRead(c, 100, 279));
  EXPECT_FALSE(DiskLifetimeEarlyRead(c, 100, 280));
  EXPECT_FALSE(DiskLifetimeEarlyRead(c, 100, 281));
  EXPECT_EQ(DiskLifetimeScore(c, 100, 0, false, true, 279), 1814356);
  EXPECT_EQ(DiskLifetimeScore(c, 100, 0, false, true, 280), 604755);
  EXPECT_EQ(DiskLifetimeScore(c, 100, 0, false, true, 281), 604754);
  EXPECT_EQ(DiskLifetimeScore(c, 100, 1, true, false, 100), 1209600);
  // Earlier now cannot cause unsigned subtraction or negative age.
  EXPECT_EQ(DiskLifetimeScore(c, 100, 0, false, true, 99), 1814400);
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, false, UINT64_MAX),
            std::numeric_limits<int64_t>::min());
  EXPECT_LT(DiskLifetimeScore(c, 0, 0, false, false, 604801), 0);
  EXPECT_EQ(DiskLifetimeScore(c, 0, 1, false, false, 302400),
            DiskLifetimeScore(c, 0, 0, false, false, 0));
}

// Independent small-state oracle: accumulate aging one integer second at a
// time. Do not derive expected endpoints by calling ProtectEnd or Score.
int64_t Oracle(const DiskLifetimeConfig& c, unsigned band, bool write,
               uint64_t age) {
  EXPECT_TRUE(c.boost_sec.has_value());
  const int64_t boost = static_cast<int64_t>(c.boost_sec.value_or(0));
  int64_t score = static_cast<int64_t>(c.base_sec);
  for (unsigned i = 0; i < band; ++i) score += c.read_bonus_sec;
  if (write) score += boost;
  for (uint64_t tick = 1; tick <= age; ++tick) {
    if (!write || tick > c.protect_sec) {
      --score;
    } else {
      if (tick % c.slowdown == 0) --score;
      if (tick == c.protect_sec) score -= boost;
    }
  }
  return score;
}

TEST(DiskLifetimeMath, ProtectEndMatchesIndependentDiscreteOracle) {
  for (const uint64_t protect : {1U, 4U, 9U}) {
    for (const uint64_t slow : {1U, 2U, 4U}) {
      for (const uint64_t keep : {0U, 19U}) {
        DiskLifetimeConfig c;
        c.base_sec = 20;
        c.read_bonus_sec = 3;
        c.max_reads = 4;
        c.protect_sec = protect;
        c.slowdown = slow;
        c.boost_sec = 50;
        c.keep_score_sec = keep;
        c.bucket_sec = 64;
        c.bucket_count = 2;
        std::string error;
        ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
        for (const bool write : {false, true}) {
          for (unsigned reads = 0; reads <= (write ? 0U : 63U); ++reads) {
            for (const bool fw : {false, true}) {
              if (write && fw) continue;
              const unsigned sum = reads + static_cast<unsigned>(fw);
              const unsigned band = sum > 4 ? 4 : sum;
              uint64_t expected_end = 0;
              while (Oracle(c, band, write, expected_end) >
                     static_cast<int64_t>(keep)) {
                ++expected_end;
                ASSERT_LT(expected_end, 100U);
              }
              for (const uint32_t anchor : {0U, 17U, UINT32_MAX}) {
                const uint64_t end = DiskLifetimeProtectEnd(
                    c, anchor, static_cast<uint8_t>(reads), fw, write);
                ASSERT_EQ(end, uint64_t{anchor} + expected_end);
                for (uint64_t age = 0; age <= expected_end + 1; ++age) {
                  const uint64_t now = uint64_t{anchor} + age;
                  const int64_t score = DiskLifetimeScore(
                      c, anchor, static_cast<uint8_t>(reads), fw, write, now);
                  EXPECT_EQ(score, Oracle(c, band, write, age));
                  EXPECT_EQ(score > static_cast<int64_t>(keep), now < end);
                }
              }
            }
          }
        }
      }
    }
  }
}

TEST(DiskLifetimeMath, ProtectionCanEndExactlyWhenBoostWithdraws) {
  DiskLifetimeConfig c;
  c.base_sec = 2;
  c.read_bonus_sec = 0;
  c.max_reads = 1;
  c.protect_sec = 10;
  c.slowdown = 1;
  c.boost_sec = 20;
  c.keep_score_sec = 1;
  c.bucket_sec = 1;
  c.bucket_count = 16;
  std::string error;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeProtectEnd(c, 0, 0, false, true), 10U);
  EXPECT_GT(DiskLifetimeScore(c, 0, 0, false, true, 9), 1);
  EXPECT_LE(DiskLifetimeScore(c, 0, 0, false, true, 10), 1);
  // KEEP=0 still protects positive scores; it does not disable protection.
  c.keep_score_sec = 0;
  ASSERT_TRUE(ValidateDiskLifetimeConfig(c, &error)) << error;
  EXPECT_EQ(DiskLifetimeProtectEnd(c, 0, 0, false, false), 2U);
  EXPECT_GT(DiskLifetimeScore(c, 0, 0, false, false, 1), 0);
  EXPECT_EQ(DiskLifetimeScore(c, 0, 0, false, false, 2), 0);
}

}  // namespace
}  // namespace dfkv
