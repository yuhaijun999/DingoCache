// ReadCoalescer: sync leader/follower collapse plus the async-flight protocol
// used by the io_uring serve path (TryRegisterAsync / CompleteAsync). Hermetic:
// "disk reads" are lambdas. Covers: async register -> follower join -> complete
// hands the payload; fan-in evidence (waiters) reporting; abort and failure ->
// followers fall back to their own read; waiter timeout fallback; same-thread
// pipelined duplicate never deadlocks; duplicate registration declines.
#include "cache/read_coalescer.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cache/disk_resident_identity.h"
#include "common/kv_types.h"
#include "common/status.h"
#include "read_coalescer_test_peer.h"

using dfkv::BlockKey;
using dfkv::ReadCoalescer;
using dfkv::Status;
using namespace std::chrono_literals;

namespace {

BlockKey K(uint64_t id) { return BlockKey{id, 0}; }

// The wait bound is latched on first use (function-local static); pin it to a
// test-friendly value before any test can latch the 500ms default.
[[maybe_unused]] const bool kEnvSet = [] {
  ::setenv("DFKV_READ_COALESCE_TIMEOUT_MS", "200", 1);
  ::setenv("DFKV_READ_COALESCE_RECUR_MS", "200", 1);  // recurrence-window tests
  return true;
}();

TEST(ReadCoalescer, AsyncFlightHandsPayloadToFollower) {
  ReadCoalescer c;
  const std::string v = "payload-bytes";
  uint64_t t = c.TryRegisterAsync(K(1), 0, v.size(),
                                  /*whole_value=*/true, /*value_len=*/97);
  ASSERT_NE(t, 0u);
  ASSERT_TRUE(c.InFlight(K(1), 0, v.size()));

  std::atomic<int> own_reads{0};
  std::string got(v.size(), '\0');
  size_t got_len = 0;
  size_t got_value_len = 0;
  std::thread follower([&] {
    Status st = c.Read(
        K(1), 0, v.size(), &got[0], got.size(), &got_len, &got_value_len,
        [&](char* buf, size_t, size_t* n, size_t* value_len) {
          own_reads.fetch_add(1);
          std::memcpy(buf, v.data(), v.size());
          *n = v.size();
          *value_len = 1;
          return Status::kOk;
        });
    EXPECT_EQ(st, Status::kOk);
  });
  // Give the follower time to join the flight, then complete it.
  std::this_thread::sleep_for(50ms);
  BlockKey key{};
  bool whole = false;
  EXPECT_TRUE(c.CompleteAsync(t, Status::kOk, v.data(), v.size(), &key, &whole));
  follower.join();
  EXPECT_EQ(own_reads.load(), 0);  // served from the flight, no duplicate read
  EXPECT_EQ(got, v);
  EXPECT_EQ(got_len, v.size());
  EXPECT_EQ(got_value_len, 97u);
  EXPECT_EQ(key.digest_hi, 1u);
  EXPECT_TRUE(whole);
  EXPECT_EQ(c.coalesced(), 1u);
}

TEST(ReadCoalescer, CompleteWithoutWaitersReportsNoFanIn) {
  ReadCoalescer c;
  uint64_t t = c.TryRegisterAsync(K(2), 0, 8, false);
  ASSERT_NE(t, 0u);
  EXPECT_FALSE(c.CompleteAsync(t, Status::kOk, "abcdefgh", 8));
  EXPECT_FALSE(c.InFlight(K(2), 0, 8));       // flight erased
  EXPECT_FALSE(c.CompleteAsync(t, Status::kOk, "abcdefgh", 8));  // idempotent
}

TEST(ReadCoalescer, DuplicateRegistrationDeclines) {
  ReadCoalescer c;
  uint64_t t1 = c.TryRegisterAsync(K(3), 0, 8, false);
  ASSERT_NE(t1, 0u);
  EXPECT_EQ(c.TryRegisterAsync(K(3), 0, 8, false), 0u);   // identical -> decline
  EXPECT_NE(c.TryRegisterAsync(K(3), 8, 8, false), 0u);   // other range -> own flight
  c.CompleteAsync(t1, Status::kIOError, nullptr, 0);
}

TEST(ReadCoalescer, AbortedFlightFollowerFallsBackToOwnRead) {
  ReadCoalescer c;
  uint64_t t = c.TryRegisterAsync(K(4), 0, 4, false);
  ASSERT_NE(t, 0u);
  std::atomic<int> own_reads{0};
  char buf[4];
  size_t n = 0;
  std::thread follower([&] {
    Status st = c.Read(K(4), 0, 4, buf, sizeof(buf), &n, nullptr,
                       [&](char* b, size_t, size_t* on, size_t*) {
                         own_reads.fetch_add(1);
                         std::memcpy(b, "sane", 4);
                         *on = 4;
                         return Status::kOk;
                       });
    EXPECT_EQ(st, Status::kOk);  // fallback read succeeded despite the abort
  });
  std::this_thread::sleep_for(50ms);
  c.CompleteAsync(t, Status::kIOError, nullptr, 0);  // teardown-path abort
  follower.join();
  EXPECT_EQ(own_reads.load(), 1);
  EXPECT_EQ(std::string(buf, 4), "sane");
}

TEST(ReadCoalescer, WaiterTimeoutFallsBackToOwnRead) {
  // The flight is NEVER completed: an abandoned flight (leader connection
  // died without reaching complete/abort) must not hang its waiters.
  ReadCoalescer c;
  uint64_t t = c.TryRegisterAsync(K(5), 0, 4, false);
  ASSERT_NE(t, 0u);
  std::atomic<int> own_reads{0};
  char buf[4];
  size_t n = 0;
  std::thread follower([&] {
    Status st = c.Read(K(5), 0, 4, buf, sizeof(buf), &n, nullptr,
                       [&](char* b, size_t, size_t* on, size_t*) {
                         own_reads.fetch_add(1);
                         std::memcpy(b, "wake", 4);
                         *on = 4;
                         return Status::kOk;
                       });
    EXPECT_EQ(st, Status::kOk);
  });
  follower.join();  // returns after the latched timeout, via its own read
  EXPECT_EQ(own_reads.load(), 1);
  EXPECT_EQ(c.timeouts(), 1u);
  (void)t;  // never completed on purpose: an abandoned flight must not hang waiters
}

TEST(ReadCoalescer, SameThreadDuplicateReadsItselfImmediately) {
  ReadCoalescer c;
  // Register on THIS thread (as the async prep does), then issue the duplicate
  // GET's sync fallback on the SAME thread: it must not wait (the "leader"
  // read would only run after this call returned -> deadlock-until-timeout).
  uint64_t t = c.TryRegisterAsync(K(6), 0, 4, false);
  ASSERT_NE(t, 0u);
  char buf[4];
  size_t n = 0;
  const auto t0 = std::chrono::steady_clock::now();
  Status st = c.Read(K(6), 0, 4, buf, sizeof(buf), &n, nullptr,
                     [&](char* b, size_t, size_t* on, size_t*) {
                       std::memcpy(b, "self", 4);
                       *on = 4;
                       return Status::kOk;
                     });
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0).count();
  EXPECT_EQ(st, Status::kOk);
  EXPECT_EQ(std::string(buf, 4), "self");
  EXPECT_LT(ms, 100);  // no wait: read executed inline
  c.CompleteAsync(t, Status::kIOError, nullptr, 0);
}

TEST(ReadCoalescer, SyncLeaderReportsFanIn) {
  ReadCoalescer c;
  const std::string v(8192, 'x');
  std::atomic<int> fills{0};
  std::atomic<bool> leader_entered{false};
  ReadCoalescer::Outcome oc;
  std::string lbuf(v.size(), '\0');
  size_t ln = 0;
  std::thread leader([&] {
    c.Read(K(7), 0, v.size(), &lbuf[0], lbuf.size(), &ln, nullptr,
           [&](char* b, size_t, size_t* on, size_t*) {
             leader_entered.store(true);
             std::this_thread::sleep_for(100ms);  // hold the flight open
             fills.fetch_add(1);
             std::memcpy(b, v.data(), v.size());
             *on = v.size();
             return Status::kOk;
           },
           &oc);
  });
  while (!leader_entered.load()) std::this_thread::sleep_for(1ms);
  std::string fbuf(v.size(), '\0');
  size_t fn = 0;
  Status st = c.Read(K(7), 0, v.size(), &fbuf[0], fbuf.size(), &fn, nullptr,
                     [&](char* b, size_t, size_t* on, size_t*) {
                       fills.fetch_add(1);
                       std::memcpy(b, v.data(), v.size());
                       *on = v.size();
                       return Status::kOk;
                     });
  leader.join();
  EXPECT_EQ(st, Status::kOk);
  EXPECT_EQ(fills.load(), 1);  // one disk read served both
  EXPECT_EQ(fbuf, v);
  EXPECT_EQ(lbuf, v);
  EXPECT_TRUE(oc.leader);
  EXPECT_TRUE(oc.had_waiters);
}

TEST(ReadCoalescer, AsyncFollowerWaitsWithoutPayloadCopy) {
  ReadCoalescer c;
  const std::string value(8192, 'z');
  const uint64_t token = c.TryRegisterAsync(
      K(8), 0, value.size(), /*whole_value=*/true, value.size(),
      /*publishes_to_ram=*/true);
  ASSERT_NE(token, 0u);

  std::atomic<bool> entered{false};
  ReadCoalescer::PublishedWait waited =
      ReadCoalescer::PublishedWait::kNotEligible;
  std::thread follower([&] {
    entered.store(true, std::memory_order_release);
    waited = c.WaitForPublished(K(8), 0, value.size());
  });
  while (!entered.load(std::memory_order_acquire))
    std::this_thread::sleep_for(1ms);
  std::this_thread::sleep_for(20ms);

  // No payload is supplied: a direct follower needs only the completion fence,
  // then obtains bytes through the separately published RAM reservation.
  EXPECT_TRUE(c.CompleteAsync(
      token, Status::kOk, nullptr, value.size()));
  follower.join();
  EXPECT_EQ(waited, ReadCoalescer::PublishedWait::kReady);
  EXPECT_EQ(c.coalesced(), 0u);
  c.RecordCoalesced();
  EXPECT_EQ(c.coalesced(), 1u);
}

TEST(ReadCoalescer, NoCopyWaitRequiresDirectRamPublication) {
  ReadCoalescer c;
  const std::string value(4096, 's');
  const uint64_t token = c.TryRegisterAsync(
      K(9), 0, value.size(), /*whole_value=*/true, value.size(),
      /*publishes_to_ram=*/false);
  ASSERT_NE(token, 0u);
  EXPECT_EQ(c.WaitForPublished(K(9), 0, value.size()),
            ReadCoalescer::PublishedWait::kNotEligible);
  EXPECT_TRUE(c.InFlight(K(9), 0, value.size()));
  EXPECT_FALSE(c.CompleteAsync(token, Status::kOk, value.data(), value.size()));
}

TEST(ReadCoalescer, DirectPublicationSkipsRecurrenceTombstone) {
  ReadCoalescer c;
  const uint64_t direct = c.TryRegisterAsync(
      K(10), 0, 4096, /*whole_value=*/true, 4096,
      /*publishes_to_ram=*/true);
  ASSERT_NE(direct, 0u);
  EXPECT_FALSE(c.CompleteAsync(direct, Status::kOk, nullptr, 4096));

  bool recurrent = true;
  const uint64_t staged =
      c.TryRegisterAsync(K(10), 0, 4096, /*whole_value=*/true);
  ASSERT_NE(staged, 0u);
  c.CompleteAsync(
      staged, Status::kOk, nullptr, 4096, nullptr, nullptr, &recurrent);
  EXPECT_FALSE(recurrent);
  EXPECT_EQ(c.recur_hits(), 0u);
}

}  // namespace

// v2 recurrence window: a lone whole-value read leaves a key fingerprint; a
// re-read within DFKV_READ_COALESCE_RECUR_MS reports recurrence evidence on
// completion (the drift-tolerant promotion gate). Env pinned to 200ms below.
TEST(ReadCoalescer, RecurrenceTombstoneReportsOnReRead) {
  ReadCoalescer c;
  const char v[8] = "payload";
  uint64_t t1 = c.TryRegisterAsync(K(20), 0, 8, /*whole=*/true);
  ASSERT_NE(t1, 0u);
  EXPECT_FALSE(c.CompleteAsync(t1, Status::kOk, v, 8));  // lone read: tombstone laid
  BlockKey key{};
  bool whole = false, recur = false;
  uint64_t t2 = c.TryRegisterAsync(K(20), 0, 8, true);   // re-read inside window
  ASSERT_NE(t2, 0u);
  EXPECT_EQ(c.recur_hits(), 1u);
  c.CompleteAsync(t2, Status::kOk, v, 8, &key, &whole, &recur);
  EXPECT_TRUE(recur);      // promotion evidence without any in-flight waiter
  EXPECT_TRUE(whole);
  EXPECT_EQ(key.digest_hi, 20u);
  // The hit consumed the tombstone, and a recurrent completion lays none
  // (promotion put the page in RAM): a third read reports no recurrence.
  uint64_t t3 = c.TryRegisterAsync(K(20), 0, 8, true);
  ASSERT_NE(t3, 0u);
  recur = false;
  c.CompleteAsync(t3, Status::kOk, v, 8, nullptr, nullptr, &recur);
  EXPECT_FALSE(recur);
}

TEST(ReadCoalescer, RecurrenceTombstoneExpires) {
  ReadCoalescer c;
  const char v[8] = "payload";
  uint64_t t1 = c.TryRegisterAsync(K(21), 0, 8, true);
  c.CompleteAsync(t1, Status::kOk, v, 8);
  std::this_thread::sleep_for(250ms);  // past the 200ms env-pinned window
  bool recur = true;
  uint64_t t2 = c.TryRegisterAsync(K(21), 0, 8, true);
  ASSERT_NE(t2, 0u);
  c.CompleteAsync(t2, Status::kOk, v, 8, nullptr, nullptr, &recur);
  EXPECT_FALSE(recur);
}

TEST(ReadCoalescer, NoTombstoneForPartialFailedOrFannedReads) {
  ReadCoalescer c;
  const char v[8] = "payload";
  // Partial (whole=false) completion: no tombstone.
  uint64_t t1 = c.TryRegisterAsync(K(22), 4, 4, false);
  c.CompleteAsync(t1, Status::kOk, v, 4);
  bool recur = true;
  uint64_t t2 = c.TryRegisterAsync(K(22), 4, 4, false);
  c.CompleteAsync(t2, Status::kOk, v, 4, nullptr, nullptr, &recur);
  EXPECT_FALSE(recur);
  // Failed/aborted completion: no tombstone.
  uint64_t t3 = c.TryRegisterAsync(K(23), 0, 8, true);
  c.CompleteAsync(t3, Status::kIOError, nullptr, 0);
  recur = true;
  uint64_t t4 = c.TryRegisterAsync(K(23), 0, 8, true);
  c.CompleteAsync(t4, Status::kOk, v, 8, nullptr, nullptr, &recur);
  EXPECT_FALSE(recur);
}

// The ordering tag belongs to the caller's cross-layer fence, not the disk
// resident identity. Keep an old read alive while a post-mutation read starts.
TEST(ReadCoalescer, FenceEpochSeparatesSynchronousFlights) {
  ReadCoalescer coalescer;
  const BlockKey key = K(800);
  std::mutex mutex;
  std::condition_variable cv;
  bool entered = false;
  bool released = false;
  char old_data[3] = {};
  size_t old_len = 0;
  Status old_status = Status::kInvalid;
  std::thread old_reader([&] {
    old_status = coalescer.Read(
        key, 0, 3, old_data, sizeof(old_data), &old_len, nullptr,
        [&](char* destination, size_t, size_t* length, size_t* value_len) {
          std::unique_lock<std::mutex> lock(mutex);
          entered = true;
          cv.notify_all();
          cv.wait(lock, [&] { return released; });
          std::memcpy(destination, "old", 3);
          *length = 3;
          if (value_len != nullptr) *value_len = 3;
          return Status::kOk;
        },
        nullptr, 10);
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered; }));
  }
  EXPECT_TRUE(coalescer.InFlight(key, 0, 3, 10));
  EXPECT_FALSE(coalescer.InFlight(key, 0, 3, 12));
  char new_data[3] = {};
  size_t new_len = 0;
  ReadCoalescer::Outcome outcome;
  EXPECT_EQ(
      coalescer.Read(
          key, 0, 3, new_data, sizeof(new_data), &new_len, nullptr,
          [](char* destination, size_t, size_t* length, size_t* value_len) {
            std::memcpy(destination, "new", 3);
            *length = 3;
            if (value_len != nullptr) *value_len = 3;
            return Status::kOk;
          },
          &outcome, 12),
      Status::kOk);
  EXPECT_TRUE(outcome.leader);
  EXPECT_EQ(coalescer.timeouts(), 0u);
  EXPECT_EQ(std::string(new_data, new_len), "new");
  EXPECT_TRUE(coalescer.InFlight(key, 0, 3, 10));
  {
    const std::lock_guard<std::mutex> lock(mutex);
    released = true;
  }
  cv.notify_all();
  old_reader.join();
  EXPECT_EQ(old_status, Status::kOk);
  EXPECT_EQ(std::string(old_data, old_len), "old");
  EXPECT_FALSE(coalescer.InFlight(key, 0, 3, 10));
}

TEST(ReadCoalescer, FenceEpochSeparatesAsyncTokensAndRecurrence) {
  ReadCoalescer coalescer;
  const BlockKey key = K(801);
  const uint64_t old_token =
      coalescer.TryRegisterAsync(key, 0, 3, true, 3, false, 20);
  ASSERT_NE(old_token, 0u);
  EXPECT_EQ(coalescer.TryRegisterAsync(key, 0, 3, true, 3, false, 20), 0u);
  const uint64_t new_token =
      coalescer.TryRegisterAsync(key, 0, 3, true, 3, false, 22);
  ASSERT_NE(new_token, 0u);
  EXPECT_FALSE(coalescer.CompleteAsync(old_token, Status::kOk, "old", 3));
  // Completing the old token must not erase the replacement generation.
  EXPECT_TRUE(coalescer.InFlight(key, 0, 3, 22));
  EXPECT_FALSE(coalescer.InFlight(key, 0, 3, 20));
  EXPECT_FALSE(coalescer.CompleteAsync(new_token, Status::kOk, "new", 3));
  const uint64_t next_token =
      coalescer.TryRegisterAsync(key, 0, 3, true, 3, false, 24);
  ASSERT_NE(next_token, 0u);
  bool recurrent = true;
  uint64_t completed_epoch = 0;
  EXPECT_FALSE(coalescer.CompleteAsync(next_token, Status::kOk, "now", 3,
                                       nullptr, nullptr, &recurrent,
                                       &completed_epoch));
  EXPECT_EQ(completed_epoch, 24u);
  EXPECT_FALSE(coalescer.CompleteAsync(next_token, Status::kOk, "now", 3,
                                       nullptr, nullptr, nullptr,
                                       &completed_epoch));
  EXPECT_EQ(completed_epoch, 24u);  // Retired tokens cannot rewrite identity.
  EXPECT_FALSE(recurrent) << "an old generation is not recurrence evidence";
  const uint64_t repeat_token =
      coalescer.TryRegisterAsync(key, 0, 3, true, 3, false, 24);
  ASSERT_NE(repeat_token, 0u);
  recurrent = false;
  EXPECT_FALSE(coalescer.CompleteAsync(repeat_token, Status::kOk, "now", 3,
                                       nullptr, nullptr, &recurrent));
  EXPECT_TRUE(recurrent) << "same-generation recurrence remains supported";
}

TEST(ReadCoalescer, FenceEpochSeparatesPublishedWaits) {
  ReadCoalescer coalescer;
  const BlockKey key = K(802);
  const uint64_t token =
      coalescer.TryRegisterAsync(key, 0, 3, true, 3, true, 30);
  ASSERT_NE(token, 0u);
  EXPECT_EQ(coalescer.WaitForPublished(key, 0, 3, 32),
            ReadCoalescer::PublishedWait::kNotEligible);
  // Same-thread followers still decline rather than waiting on themselves.
  EXPECT_EQ(coalescer.WaitForPublished(key, 0, 3, 30),
            ReadCoalescer::PublishedWait::kFallback);
  EXPECT_TRUE(coalescer.InFlight(key, 0, 3, 30));
  EXPECT_FALSE(coalescer.CompleteAsync(token, Status::kOk, "old", 3));
}

TEST(ReadCoalescer, DefaultFenceEpochPreservesExistingCallers) {
  ReadCoalescer coalescer;
  const BlockKey key = K(803);
  const uint64_t token = coalescer.TryRegisterAsync(key, 0, 3, false);
  ASSERT_NE(token, 0u);
  EXPECT_TRUE(coalescer.InFlight(key, 0, 3, 0));
  EXPECT_EQ(coalescer.TryRegisterAsync(key, 0, 3, false, 0, false, 0), 0u);
  EXPECT_FALSE(coalescer.CompleteAsync(token, Status::kIOError, nullptr, 0));
}

TEST(ReadCoalescer, NewEpochReadDoesNotJoinOldAsyncFlight) {
  ReadCoalescer coalescer;
  const BlockKey key = K(804);
  uint64_t old_token = 0;
  // Registration on another thread prevents the same-thread fallback from
  // masking an incorrect cross-epoch join. No physical I/O is mocked as heat.
  std::thread registrar([&] {
    old_token = coalescer.TryRegisterAsync(key, 0, 3, true, 77, false, 40);
  });
  registrar.join();
  ASSERT_NE(old_token, 0u);
  char data[3] = {};
  size_t length = 0;
  size_t value_len = 0;
  ReadCoalescer::Outcome outcome;
  EXPECT_EQ(
      coalescer.Read(
          key, 0, 3, data, sizeof(data), &length, &value_len,
          [](char* destination, size_t, size_t* bytes, size_t* stored_len) {
            std::memcpy(destination, "new", 3);
            *bytes = 3;
            *stored_len = 99;
            return Status::kOk;
          },
          &outcome, 42),
      Status::kOk);
  EXPECT_TRUE(outcome.leader);
  EXPECT_EQ(coalescer.timeouts(), 0u);
  EXPECT_EQ(std::string(data, length), "new");
  EXPECT_EQ(value_len, 99u);
  EXPECT_TRUE(coalescer.InFlight(key, 0, 3, 40));
  EXPECT_FALSE(coalescer.CompleteAsync(old_token, Status::kOk, "old", 3));
}

TEST(ReadCoalescer, AbortingOldEpochDoesNotRetireNewFlight) {
  ReadCoalescer coalescer;
  const BlockKey key = K(805);
  const uint64_t old_token =
      coalescer.TryRegisterAsync(key, 0, 3, true, 3, false, 50);
  const uint64_t new_token =
      coalescer.TryRegisterAsync(key, 0, 3, true, 3, false, 52);
  ASSERT_NE(old_token, 0u);
  ASSERT_NE(new_token, 0u);
  EXPECT_FALSE(
      coalescer.CompleteAsync(old_token, Status::kIOError, nullptr, 0));
  EXPECT_TRUE(coalescer.InFlight(key, 0, 3, 52));
  EXPECT_FALSE(coalescer.InFlight(key, 0, 3, 50));
  EXPECT_FALSE(coalescer.CompleteAsync(new_token, Status::kOk, "new", 3));
}

TEST(ReadCoalescer, SharedBytesCarryOriginalIdentity) {
  const dfkv::DiskResidentIdentity original{11, 12, 13};
  const dfkv::DiskResidentIdentity fallback{21, 22, 23};
  for (const bool asynchronous : {false, true}) {
    SCOPED_TRACE(asynchronous);
    ReadCoalescer coalescer;
    const BlockKey key = K(800);
    uint64_t token = 0;
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool finish = false;
    dfkv::DiskResidentIdentity leader_identity;
    dfkv::DiskResidentIdentity follower_identity = fallback;
    char leader_bytes[4] = {};
    char follower_bytes[4] = {};
    size_t leader_len = 0;
    size_t follower_len = 0;
    std::thread leader;
    if (asynchronous) {
      token = coalescer.TryRegisterAsync(key, 0, 4, true, 4);
      ASSERT_NE(token, 0U);
    } else {
      leader = std::thread([&] {
        EXPECT_EQ(coalescer.ReadWithIdentity(
                      key, 0, 4, leader_bytes, 4, &leader_len, nullptr,
                      &leader_identity,
                      [&](char* buffer, size_t, size_t* n, size_t* value_len,
                          dfkv::DiskResidentIdentity* identity) {
                        std::unique_lock<std::mutex> lock(mutex);
                        entered = true;
                        cv.notify_all();
                        cv.wait(lock, [&] { return finish; });
                        std::memcpy(buffer, "old!", 4);
                        *n = *value_len = 4;
                        *identity = original;
                        return Status::kOk;
                      }),
                  Status::kOk);
      });
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait(lock, [&] { return entered; });
    }
    int independent_reads = 0;
    std::thread follower([&] {
      EXPECT_EQ(
          coalescer.ReadWithIdentity(key, 0, 4, follower_bytes, 4,
                                     &follower_len, nullptr, &follower_identity,
                                     [&](char*, size_t, size_t*, size_t*,
                                         dfkv::DiskResidentIdentity*) {
                                       ++independent_reads;
                                       return Status::kIOError;
                                     }),
          Status::kOk);
    });
    const bool joined =
        dfkv::ReadCoalescerTestPeer::WaitForCopyWaiter(coalescer);
    if (asynchronous) {
      EXPECT_TRUE(coalescer.CompleteAsync(token, Status::kOk, "old!", 4,
                                          nullptr, nullptr, nullptr, nullptr,
                                          original));
    } else {
      {
        const std::lock_guard<std::mutex> lock(mutex);
        finish = true;
      }
      cv.notify_all();
      leader.join();
      EXPECT_EQ(leader_identity, original);
      EXPECT_EQ(std::string(leader_bytes, leader_len), "old!");
    }
    follower.join();
    EXPECT_TRUE(joined);
    EXPECT_EQ(independent_reads, 0);
    EXPECT_EQ(follower_identity, original);
    EXPECT_EQ(std::string(follower_bytes, follower_len), "old!");
  }
}

TEST(ReadCoalescer, FallbackUsesOnlyItsOwnIdentity) {
  const dfkv::DiskResidentIdentity original{31, 32, 33};
  const dfkv::DiskResidentIdentity replacement{41, 42, 43};
  // Self-recursion, timeout, failed leader, and successful empty leader.
  for (const unsigned char scenario : {0, 1, 2, 3}) {
    SCOPED_TRACE(scenario);
    ReadCoalescer coalescer;
    const BlockKey key = K(801);
    const uint64_t token = coalescer.TryRegisterAsync(key, 0, 4, true, 4);
    char buffer[4] = {};
    size_t length = 0;
    dfkv::DiskResidentIdentity identity = original;
    Status result = Status::kInvalid;
    auto read = [&] {
      result = coalescer.ReadWithIdentity(
          key, 0, 4, buffer, sizeof(buffer), &length, nullptr, &identity,
          [&](char* bytes, size_t, size_t* n, size_t*,
              dfkv::DiskResidentIdentity* source) {
            std::memcpy(bytes, "new!", 4);
            *n = 4;
            *source = replacement;
            return Status::kOk;
          });
    };
    if (scenario == 0) {
      read();
    } else {
      std::thread follower(read);
      const bool joined =
          dfkv::ReadCoalescerTestPeer::WaitForCopyWaiter(coalescer);
      if (scenario >= 2) {
        EXPECT_TRUE(coalescer.CompleteAsync(
            token, scenario == 2 ? Status::kIOError : Status::kOk, nullptr, 0,
            nullptr, nullptr, nullptr, nullptr, original));
      }
      follower.join();
      EXPECT_TRUE(joined);
    }
    EXPECT_EQ(result, Status::kOk);
    EXPECT_EQ(identity, replacement);
    EXPECT_EQ(std::string(buffer, length), "new!");
    coalescer.CompleteAsync(token, Status::kOk, "old!", 4, nullptr, nullptr,
                            nullptr, nullptr, original);
    EXPECT_EQ(identity, replacement);  // Late completion cannot relabel bytes.
  }
}

TEST(ReadCoalescer, FailedAndEmptyFillsClearIdentity) {
  const dfkv::DiskResidentIdentity stale{51, 52, 53};
  for (const bool failed : {false, true}) {
    for (const bool self_fallback : {false, true}) {
      ReadCoalescer coalescer;
      const BlockKey key = K(802);
      uint64_t token = 0;
      if (self_fallback) token = coalescer.TryRegisterAsync(key, 0, 4, true, 4);
      dfkv::DiskResidentIdentity identity = stale;
      char bytes[4] = {};
      size_t length = 99;
      const Status result = coalescer.ReadWithIdentity(
          key, 0, 4, bytes, sizeof(bytes), &length, nullptr, &identity,
          [&](char*, size_t, size_t* n, size_t*,
              dfkv::DiskResidentIdentity* source) {
            *source = stale;  // Even a misbehaving fill cannot export on error.
            *n = failed ? 4 : 0;
            return failed ? Status::kIOError : Status::kOk;
          });
      EXPECT_EQ(result, failed ? Status::kIOError : Status::kOk);
      EXPECT_FALSE(identity.valid());
      if (!failed) {
        EXPECT_EQ(length, 0U);
      }
      if (token) coalescer.CompleteAsync(token, Status::kIOError, nullptr, 0);
    }
  }
}

TEST(ReadCoalescer, SharedIdentityFollowsNonemptyCopiedSlice) {
  const dfkv::DiskResidentIdentity original{61, 62, 63};
  for (const size_t capacity : {0U, 2U, 4U}) {
    ReadCoalescer coalescer;
    const BlockKey key = K(803);
    const uint64_t token = coalescer.TryRegisterAsync(key, 0, 4, true, 4);
    char buffer[4] = {};
    size_t length = 99;
    dfkv::DiskResidentIdentity identity = original;
    std::thread follower([&] {
      EXPECT_EQ(
          coalescer.ReadWithIdentity(
              key, 0, 4, buffer, capacity, &length, nullptr, &identity,
              [](char*, size_t, size_t*, size_t*, dfkv::DiskResidentIdentity*) {
                return Status::kIOError;
              }),
          Status::kOk);
    });
    const bool joined =
        dfkv::ReadCoalescerTestPeer::WaitForCopyWaiter(coalescer);
    EXPECT_TRUE(coalescer.CompleteAsync(token, Status::kOk, "old!", 4, nullptr,
                                        nullptr, nullptr, nullptr, original));
    follower.join();
    EXPECT_TRUE(joined);
    EXPECT_EQ(length, capacity);
    EXPECT_EQ(std::string(buffer, length), std::string("old!", capacity));
    EXPECT_EQ(identity.valid(), capacity > 0);
    if (capacity > 0) {
      EXPECT_EQ(identity, original);
    }
  }
}
