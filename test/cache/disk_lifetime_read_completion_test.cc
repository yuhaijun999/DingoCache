#include "cache/disk_lifetime_read_completion.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

namespace dfkv {
namespace {

using Sampling = DiskLifetimeReadCompletion::Sampling;

TEST(DiskLifetimeReadCompletion, DefaultMetadataIsMissing) {
  const DiskLifetimeReadCompletion completion;
  EXPECT_EQ(completion.sampling, Sampling::kMissing);
  EXPECT_EQ(completion.completed_at, std::chrono::steady_clock::time_point{});
  EXPECT_EQ(static_cast<unsigned char>(Sampling::kMissing), 0U);
  EXPECT_EQ(static_cast<unsigned char>(Sampling::kNotSampled), 1U);
  EXPECT_EQ(static_cast<unsigned char>(Sampling::kSampled), 2U);
}

TEST(DiskLifetimeReadCompletion,
     SamplerSkipsDisabledAndEmptyBeforeSelectingFirst) {
  std::vector<DiskLifetimeReadCompletion> ignored;
  DiskLifetimeReadCompletion first;
  std::thread producer([&] {
    for (size_t event = 0; event < 129; ++event) {
      ignored.push_back(CaptureDiskLifetimeReadCompletion(false, 5));
      ignored.push_back(CaptureDiskLifetimeReadCompletion(true, 0));
      ignored.push_back(CaptureDiskLifetimeReadCompletion(false, 0));
    }
    first = CaptureDiskLifetimeReadCompletion(true, 5);
  });
  producer.join();
  for (const DiskLifetimeReadCompletion& completion : ignored) {
    EXPECT_EQ(completion.sampling, Sampling::kMissing);
    EXPECT_EQ(completion.completed_at, std::chrono::steady_clock::time_point{});
  }
  EXPECT_EQ(first.sampling, Sampling::kSampled);
}

TEST(DiskLifetimeReadCompletion,
     SamplerSelectsFirstAndEvery64EligibleCompletion) {
  struct Observation {
    DiskLifetimeReadCompletion completion;
    std::chrono::steady_clock::time_point before;
    std::chrono::steady_clock::time_point after;
  };
  std::vector<Observation> observations;
  std::thread producer([&] {
    for (size_t event = 1; event <= 130; ++event) {
      const std::chrono::steady_clock::time_point before =
          std::chrono::steady_clock::now();
      const DiskLifetimeReadCompletion completion =
          CaptureDiskLifetimeReadCompletion(true, 5);
      const std::chrono::steady_clock::time_point after =
          std::chrono::steady_clock::now();
      observations.push_back({completion, before, after});
    }
  });
  producer.join();
  ASSERT_EQ(observations.size(), 130U);
  for (size_t index = 0; index < observations.size(); ++index) {
    SCOPED_TRACE(index + 1);
    const Observation& observed = observations[index];
    if (index % 64 == 0) {
      EXPECT_EQ(observed.completion.sampling, Sampling::kSampled);
      EXPECT_GE(observed.completion.completed_at, observed.before);
      EXPECT_LE(observed.completion.completed_at, observed.after);
    } else {
      EXPECT_EQ(observed.completion.sampling, Sampling::kNotSampled);
      EXPECT_EQ(observed.completion.completed_at,
                std::chrono::steady_clock::time_point{});
    }
  }
}

TEST(DiskLifetimeReadCompletion, SamplingPhasesBelongToIndependentThreads) {
  std::array<std::array<Sampling, 65>, 4> phases{};
  std::vector<std::thread> producers;
  producers.reserve(phases.size());
  for (std::array<Sampling, 65>& phase : phases) {
    producers.emplace_back([&phase] {
      for (Sampling& sample : phase) {
        sample = CaptureDiskLifetimeReadCompletion(true, 5).sampling;
      }
    });
  }
  for (std::thread& producer : producers) producer.join();
  for (const std::array<Sampling, 65>& phase : phases) {
    for (size_t index = 0; index < phase.size(); ++index) {
      EXPECT_EQ(phase[index],
                index % 64 == 0 ? Sampling::kSampled : Sampling::kNotSampled);
    }
  }
}

TEST(DiskLifetimeReadCompletion, ForwardedCopyRetainsOriginalTimestamp) {
  DiskLifetimeReadCompletion original;
  DiskLifetimeReadCompletion forwarded;
  std::thread producer([&] {
    original = CaptureDiskLifetimeReadCompletion(true, 5);
    // Passing the value through another layer must not recapture completion.
    forwarded = original;
    for (size_t event = 0; event < 64; ++event) {
      (void)CaptureDiskLifetimeReadCompletion(true, 5);
    }
  });
  producer.join();
  EXPECT_EQ(original.sampling, Sampling::kSampled);
  EXPECT_EQ(forwarded.sampling, original.sampling);
  EXPECT_EQ(forwarded.completed_at, original.completed_at);
}

}  // namespace
}  // namespace dfkv
