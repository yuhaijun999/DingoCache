#include "cache/rdma_pull_request_accounting.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "common/status.h"

namespace dfkv {
namespace {

using Path = RdmaPullRequestCounters::Path;
using Snapshot = RdmaPullRequestCounters::Snapshot;

uint64_t TerminalTotal(const Snapshot& snapshot, size_t path) {
  uint64_t total = 0;
  for (const uint64_t count : snapshot.terminal[path]) {
    total += count;
  }
  return total;
}

TEST(RdmaPullRequestAccounting, EmptyAndIndependentOwnersStartAtZero) {
  static_assert(!std::is_copy_constructible_v<RdmaPullRequestLedger>);
  static_assert(!std::is_move_constructible_v<RdmaPullRequestLedger>);
  RdmaPullRequestCounters first;
  {
    RdmaPullRequestLedger empty(first, 0);
    EXPECT_FALSE(empty.Begin(0, Path::kSync));
    EXPECT_FALSE(empty.Finish(0, Status::kOk, 5));
    EXPECT_FALSE(empty.ResponsePosted(0));
  }
  const Snapshot initial = first.Read();
  EXPECT_EQ(initial.dispatched, (std::array<uint64_t, 2>{0, 0}));
  EXPECT_EQ(initial.decided_unposted, (std::array<uint64_t, 2>{0, 0}));
  EXPECT_EQ(initial.terminal[0], (std::array<uint64_t, 9>{}));
  EXPECT_EQ(initial.terminal[1], (std::array<uint64_t, 9>{}));
  {
    RdmaPullRequestLedger request(first, 1);
    ASSERT_TRUE(request.Begin(0, Path::kSync));
  }
  EXPECT_EQ(first.Read().terminal[0][7], 1U);
  const RdmaPullRequestCounters second;
  EXPECT_EQ(second.Read().dispatched, initial.dispatched);
  EXPECT_EQ(second.Read().terminal, initial.terminal);
  EXPECT_EQ(second.Read().decided_unposted, initial.decided_unposted);
}

TEST(RdmaPullRequestAccounting, ClassifiesEveryFinalResultForBothPaths) {
  struct Case {
    Status status;
    size_t bytes;
    bool refused;
  };
  // Distinct counts expose swapped classifiers: each final status occurs one
  // more time than its predecessor. Nine undecided requests supply teardown
  // cancellation, whose result field precedes the eight "other" outcomes.
  constexpr std::array<Case, 8> kCases{{
      {Status::kOk, 19, false},
      {Status::kOk, 0, false},
      {Status::kNotFound, 99, false},
      {Status::kInvalid, 99, false},
      {Status::kCacheFull, 99, false},
      {Status::kIOError, 99, false},
      {Status::kIOError, 99, true},
      {Status::kQuotaExceeded, 99, false},
  }};
  RdmaPullRequestCounters counters;
  for (const Path path : {Path::kSync, Path::kUring}) {
    SCOPED_TRACE(static_cast<unsigned>(path));
    {
      RdmaPullRequestLedger ledger(counters, 9);
      for (size_t test_index = 0; test_index < kCases.size(); ++test_index) {
        const Case& test = kCases[test_index];
        for (size_t repetition = 0; repetition <= test_index; ++repetition) {
          ASSERT_TRUE(ledger.Begin(0, path));
          ASSERT_TRUE(ledger.Finish(0, test.status, test.bytes, test.refused));
          ASSERT_TRUE(ledger.ResponsePosted(0));
        }
      }
      for (size_t slot = 0; slot < 9; ++slot) {
        ASSERT_TRUE(ledger.Begin(slot, path));
      }
      const Snapshot pending = counters.Read();
      const size_t index = static_cast<size_t>(path);
      EXPECT_EQ(pending.dispatched[index], 45U);
      EXPECT_EQ(TerminalTotal(pending, index), 36U);
      EXPECT_EQ(pending.terminal[index][7], 0U);
    }
    const Snapshot final = counters.Read();
    const size_t index = static_cast<size_t>(path);
    EXPECT_EQ(final.terminal[index],
              (std::array<uint64_t, 9>{1, 2, 3, 4, 5, 6, 7, 9, 8}));
    EXPECT_EQ(final.dispatched[index], 45U);
    EXPECT_EQ(final.decided_unposted[index], 0U);
    EXPECT_EQ(TerminalTotal(final, index), 45U);
    EXPECT_EQ(TerminalTotal(final, index), final.dispatched[index]);
  }
}

TEST(RdmaPullRequestAccounting,
     DuplicateOperationsDoNotReplayOrReplaceARequest) {
  RdmaPullRequestCounters counters;
  {
    RdmaPullRequestLedger ledger(counters, 1);
    EXPECT_FALSE(ledger.Finish(0, Status::kIOError));
    EXPECT_FALSE(ledger.ResponsePosted(0));
    EXPECT_FALSE(ledger.Begin(1, Path::kSync));
    EXPECT_FALSE(ledger.Finish(1, Status::kOk));
    EXPECT_FALSE(ledger.ResponsePosted(1));
    ASSERT_TRUE(ledger.Begin(0, Path::kSync));
    EXPECT_FALSE(ledger.Begin(0, Path::kUring));
    EXPECT_FALSE(ledger.ResponsePosted(0));
    ASSERT_TRUE(ledger.Finish(0, Status::kOk, 37));
    EXPECT_FALSE(ledger.Finish(0, Status::kIOError));
    EXPECT_FALSE(ledger.Begin(0, Path::kUring));
    ASSERT_TRUE(ledger.ResponsePosted(0));
    EXPECT_FALSE(ledger.ResponsePosted(0));
    // Successful PostSend closes this accounting slot. Actual SEND-buffer
    // reuse still waits for its SEND CQE in Serve. A new request in the slot
    // can belong to another scheduler path without altering the old result.
    ASSERT_TRUE(ledger.Begin(0, Path::kUring));
    ASSERT_TRUE(ledger.Finish(0, Status::kIOError));
    ASSERT_TRUE(ledger.ResponsePosted(0));
  }
  const Snapshot final = counters.Read();
  EXPECT_EQ(final.dispatched, (std::array<uint64_t, 2>{1, 1}));
  EXPECT_EQ(final.terminal[0],
            (std::array<uint64_t, 9>{1, 0, 0, 0, 0, 0, 0, 0, 0}));
  EXPECT_EQ(final.terminal[1],
            (std::array<uint64_t, 9>{0, 0, 0, 0, 0, 1, 0, 0, 0}));
  EXPECT_EQ(final.decided_unposted, (std::array<uint64_t, 2>{0, 0}));
}

TEST(RdmaPullRequestAccounting, TeardownDoesNotUndoDecidedGrantsOrErrors) {
  RdmaPullRequestCounters counters;
  {
    RdmaPullRequestLedger ledger(counters, 4);
    ASSERT_TRUE(ledger.Begin(0, Path::kSync));
    ASSERT_TRUE(ledger.Finish(0, Status::kOk, 29));
    ASSERT_TRUE(ledger.Begin(1, Path::kSync));
    ASSERT_TRUE(ledger.Finish(1, Status::kIOError));
    ASSERT_TRUE(ledger.Begin(2, Path::kUring));
    ASSERT_TRUE(ledger.Begin(3, Path::kSync));
    ASSERT_TRUE(ledger.Finish(3, Status::kOk));
    ASSERT_TRUE(ledger.ResponsePosted(3));
    const Snapshot before_close = counters.Read();
    EXPECT_EQ(before_close.terminal[0],
              (std::array<uint64_t, 9>{1, 1, 0, 0, 0, 1, 0, 0, 0}));
    EXPECT_EQ(before_close.terminal[1], (std::array<uint64_t, 9>{}));
    EXPECT_EQ(before_close.decided_unposted, (std::array<uint64_t, 2>{0, 0}));
  }
  const Snapshot closed = counters.Read();
  EXPECT_EQ(closed.dispatched, (std::array<uint64_t, 2>{3, 1}));
  EXPECT_EQ(closed.terminal[0],
            (std::array<uint64_t, 9>{1, 1, 0, 0, 0, 1, 0, 0, 0}));
  EXPECT_EQ(closed.terminal[1],
            (std::array<uint64_t, 9>{0, 0, 0, 0, 0, 0, 0, 1, 0}));
  EXPECT_EQ(closed.decided_unposted, (std::array<uint64_t, 2>{2, 0}));
  EXPECT_EQ(TerminalTotal(closed, 0), 3U);
  EXPECT_EQ(TerminalTotal(closed, 1), 1U);
}

TEST(RdmaPullRequestAccounting, RefusalFlagRefinesOnlyAnIoError) {
  constexpr std::array<Status, 9> kStatuses{Status::kOk,
                                            Status::kNotFound,
                                            Status::kInvalid,
                                            Status::kCacheFull,
                                            Status::kIOError,
                                            Status::kQuotaExceeded,
                                            Status::kResourceExhausted,
                                            Status::kNoCompatibleRail,
                                            Status::kOk};
  RdmaPullRequestCounters counters;
  {
    RdmaPullRequestLedger ledger(counters, kStatuses.size());
    for (size_t slot = 0; slot < kStatuses.size(); ++slot) {
      ASSERT_TRUE(ledger.Begin(slot, Path::kUring));
      ASSERT_TRUE(ledger.Finish(slot, kStatuses[slot], slot, true));
      ASSERT_TRUE(ledger.ResponsePosted(slot));
    }
  }
  const Snapshot final = counters.Read();
  EXPECT_EQ(final.dispatched, (std::array<uint64_t, 2>{0, 9}));
  EXPECT_EQ(final.terminal[1],
            (std::array<uint64_t, 9>{1, 1, 1, 1, 1, 0, 1, 0, 3}));
  EXPECT_EQ(final.decided_unposted, (std::array<uint64_t, 2>{0, 0}));
}

TEST(RdmaPullRequestAccounting, MetricsExposeBoundedRowsAndScrapesAreNeutral) {
  RdmaPullRequestCounters counters;
  {
    RdmaPullRequestLedger ledger(counters, 2);
    ASSERT_TRUE(ledger.Begin(0, Path::kSync));
    ASSERT_TRUE(ledger.Finish(0, Status::kOk, 41));
    ASSERT_TRUE(ledger.Begin(1, Path::kUring));
  }
  const std::string text = counters.MetricsText();
  const std::array<const char*, 3> families{
      "dfkv_rdma_pull_requests_dispatched_total",
      "dfkv_rdma_pull_requests_terminal_total",
      "dfkv_rdma_pull_decided_responses_unposted_total"};
  for (const char* family : families) {
    const std::string declaration =
        std::string("# TYPE ") + family + " counter\n";
    EXPECT_NE(text.find(declaration), std::string::npos);
    EXPECT_EQ(text.find(declaration), text.rfind(declaration));
  }
  EXPECT_NE(
      text.find(
          "\ndfkv_rdma_pull_requests_dispatched_total{path=\"sync\"} 1\n"),
      std::string::npos);
  EXPECT_NE(
      text.find(
          "\ndfkv_rdma_pull_requests_dispatched_total{path=\"uring\"} 1\n"),
      std::string::npos);
  EXPECT_NE(text.find("\ndfkv_rdma_pull_decided_responses_unposted_total{path="
                      "\"sync\"} 1\n"),
            std::string::npos);
  EXPECT_NE(text.find("\ndfkv_rdma_pull_decided_responses_unposted_total{path="
                      "\"uring\"} 0\n"),
            std::string::npos);
  constexpr std::array<const char*, 9> kResults{
      "success_nonempty",  "success_empty", "not_found",
      "invalid",           "cache_full",    "io_error",
      "admission_refused", "cancelled",     "other"};
  for (const char* path : {"sync", "uring"}) {
    for (const char* result : kResults) {
      const bool observed =
          (std::string(path) == "sync" &&
           std::string(result) == "success_nonempty") ||
          (std::string(path) == "uring" && std::string(result) == "cancelled");
      const std::string sample =
          std::string("\ndfkv_rdma_pull_requests_terminal_total{path=\"") +
          path + "\",result=\"" + result + "\"} " + (observed ? "1\n" : "0\n");
      EXPECT_NE(text.find(sample), std::string::npos) << sample;
    }
  }
  size_t samples = 0;
  for (size_t pos = 0; pos < text.size();) {
    const size_t end = text.find('\n', pos);
    if (text.compare(pos, 1, "#") != 0) {
      ++samples;
    }
    ASSERT_NE(end, std::string::npos);
    pos = end + 1;
  }
  EXPECT_EQ(samples, 22U);
  EXPECT_EQ(counters.MetricsText(), text);
  EXPECT_EQ(counters.Read().dispatched, (std::array<uint64_t, 2>{1, 1}));
  EXPECT_EQ(counters.Read().decided_unposted, (std::array<uint64_t, 2>{1, 0}));
}

TEST(RdmaPullRequestAccounting, IndependentLedgersAndSnapshotsAreThreadSafe) {
  RdmaPullRequestCounters counters;
  std::promise<void> sampled;
  const std::future<void> sample = sampled.get_future();
  std::atomic<bool> stop{false};
  std::thread observer([&] {
    Snapshot previous;
    bool first = true;
    while (!stop.load(std::memory_order_relaxed)) {
      const Snapshot current = counters.Read();
      for (size_t path = 0; path < 2; ++path) {
        EXPECT_GE(current.dispatched[path], previous.dispatched[path]);
        EXPECT_GE(current.decided_unposted[path],
                  previous.decided_unposted[path]);
        for (size_t result = 0; result < 9; ++result) {
          EXPECT_GE(current.terminal[path][result],
                    previous.terminal[path][result]);
        }
      }
      previous = current;
      if (first) {
        sampled.set_value();
        first = false;
      }
      std::this_thread::yield();
    }
  });
  const bool observing =
      sample.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  std::vector<std::thread> producers;
  producers.reserve(4);
  for (size_t producer = 0; producer < 4; ++producer) {
    producers.emplace_back([&, producer] {
      const Path path = producer < 2 ? Path::kSync : Path::kUring;
      for (size_t request = 0; request < 100; ++request) {
        RdmaPullRequestLedger ledger(counters, 2);
        EXPECT_TRUE(ledger.Begin(0, path));
        EXPECT_TRUE(ledger.Finish(0, Status::kOk, 17));
        EXPECT_TRUE(ledger.ResponsePosted(0));
        EXPECT_TRUE(ledger.Begin(1, path));
        // Every second request finishes but loses its response; the others
        // leave no logical decision and cancel when their ledger is destroyed.
        if (request % 2 == 0) {
          EXPECT_TRUE(ledger.Finish(1, Status::kNotFound));
        }
      }
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }
  stop.store(true, std::memory_order_relaxed);
  observer.join();
  ASSERT_TRUE(observing);
  const Snapshot final = counters.Read();
  EXPECT_EQ(final.dispatched, (std::array<uint64_t, 2>{400, 400}));
  EXPECT_EQ(final.decided_unposted, (std::array<uint64_t, 2>{100, 100}));
  for (size_t path = 0; path < 2; ++path) {
    EXPECT_EQ(final.terminal[path],
              (std::array<uint64_t, 9>{200, 0, 100, 0, 0, 0, 0, 100, 0}));
    EXPECT_EQ(TerminalTotal(final, path), final.dispatched[path]);
  }
}

}  // namespace
}  // namespace dfkv
