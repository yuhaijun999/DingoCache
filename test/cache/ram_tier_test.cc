// RamTier: write-through RAM hot tier on SlabAllocator. Hermetic (no RDMA/disk;
// the flush sink is an injected callback). Covers read-after-write, the
// RAM_ONLY->DURABLE state machine via pins, send-in-flight pin blocking
// eviction, backpressure bypass when flush stalls, flush retry/drop, sub-range
// GetPrep, Remove semantics, and a concurrent TSan stress.
#include "cache/ram_tier.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cache/disk_resident_identity.h"
#include "cache/disk_write_result.h"
#include "common/kv_types.h"
#include "common/status.h"

using dfkv::BlockKey;
using dfkv::RamTier;
using dfkv::Status;
using namespace std::chrono_literals;

namespace dfkv {
// Exercise the admission/completion boundary without scheduling a duplicate
// into a sleep-sized race window.
class RamTierTestPeer {
 public:
  using Admission = RamTier::Admission;
  using Completion = std::shared_ptr<RamTier::PutCompletion>;
  using AdmitPhase = RamTier::AdmitTestPhase;

  static void SetAdmitHook(RamTier& tier,
                           std::function<void(AdmitPhase, bool)> hook) {
    tier.admit_test_hook_ = std::move(hook);
  }
  static Completion Writing(RamTier& tier, const BlockKey& key) {
    RamTier::Shard& shard = tier.ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mu);
    return shard.writing.count(key) == 0 ? Completion{} : shard.writing.at(key);
  }
  static bool Canceled(const Completion& completion) {
    const std::lock_guard<std::mutex> lock(completion->mu);
    return completion->canceled;
  }

  static Admission Admit(RamTier& tier, const BlockKey& key,
                         const std::string& value, Completion* completion,
                         bool client_ack = false) {
    return tier.Admit(key, value.data(), value.size(), client_ack, completion);
  }
  static bool Done(const Completion& completion) {
    std::lock_guard<std::mutex> lock(completion->mu);
    return completion->done;
  }
  static bool AwaitDone(const Completion& completion) {
    std::unique_lock<std::mutex> lock(completion->mu);
    return completion->cv.wait_for(lock, 2s, [&] { return completion->done; });
  }
  static RamTier::PutResult Result(const Completion& completion) {
    return RamTier::WaitPutResult(completion);
  }
  static Status Wait(const Completion& completion) {
    return RamTier::WaitPut(completion);
  }
};
}  // namespace dfkv
using dfkv::RamTierTestPeer;

namespace {
BlockKey K(uint64_t id) { return BlockKey{id, 0}; }

// A controllable flush sink: records flushed keys, can be gated (block until
// opened) and can be made to fail.
struct FlushSink {
  std::mutex m;
  std::condition_variable cv;
  std::set<std::string> flushed;
  std::unordered_map<std::string, std::string> values;
  bool gate_open = true;      // when false, flush blocks
  bool fail = false;          // when true, flush returns false
  int calls = 0;

  RamTier::FlushFn fn() {
    return [this](const BlockKey& k, char* data, size_t len, size_t) {
      std::unique_lock<std::mutex> lk(m);
      ++calls;
      cv.notify_all();
      cv.wait(lk, [this] { return gate_open; });
      if (fail) return false;
      flushed.insert(k.Filename());
      values.emplace(k.Filename(), std::string(data, len));
      cv.notify_all();
      return true;
    };
  }
  void open() { { std::lock_guard<std::mutex> lk(m); gate_open = true; } cv.notify_all(); }
  void close() { std::lock_guard<std::mutex> lk(m); gate_open = false; }
  bool await_calls(int count) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, 2s, [&] { return calls >= count; });
  }
};

// Declare after RamTier: fatal assertions must open the sink before the tier
// destructor joins its flush workers.
struct OpenFlushOnExit {
  FlushSink& sink;
  ~OpenFlushOnExit() { sink.open(); }
};

// Poll a predicate up to ~2s.
template <class F>
bool WaitFor(F pred) {
  for (int i = 0; i < 2000; ++i) {
    if (pred()) return true;
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}

RamTier::Options Opts(uint64_t bytes, uint32_t gran = 4096) {
  RamTier::Options o;
  o.bytes = bytes;
  o.slot_granularity = gran;
  return o;
}
}  // namespace

TEST(RamTier, ReadAfterWriteBeforeFlush) {
  FlushSink sink;
  sink.close();  // hold flushes so the slot stays RAM_ONLY
  RamTier rt(Opts(64 * 4096), sink.fn());
  ASSERT_TRUE(rt.ok());
  std::string v = "hot-kv-payload";
  ASSERT_TRUE(rt.Put(K(1), v.data(), v.size()));
  // Visible immediately, even though the flush hasn't run.
  RamTier::Hit h;
  ASSERT_TRUE(rt.GetPrep(K(1), 0, v.size(), &h));
  EXPECT_EQ(std::string(h.ptr, h.len), v);
  EXPECT_TRUE(h.in_arena);
  EXPECT_EQ(rt.Hits(), 1u);
  h = RamTier::Hit{};
  sink.open();
}

TEST(RamTier, ShutdownDrainTimeoutAndAckDurabilityLatencyAreObservable) {
  FlushSink sink;
  sink.close();
  RamTier rt(Opts(64 * 4096), sink.fn());
  const std::string value(4096, 'd');
  ASSERT_EQ(rt.PutWriteBack(K(2), value.data(), value.size()).status, Status::kOk);

  EXPECT_FALSE(rt.WaitForDrain(10ms));
  EXPECT_EQ(rt.ShutdownDrainTimeouts(), 1u);
  sink.open();
  ASSERT_TRUE(rt.WaitForDrain(2s));
  EXPECT_EQ(rt.DirtyObjects(), 0u);
  EXPECT_NE(rt.AckToDurableLatencyMetrics("").find(
                "dfkv_ram_ack_to_durable_latency_seconds_count 1"),
            std::string::npos);
}

TEST(RamTier, TenantIdentityIsPartOfResidentKey) {
  FlushSink sink;
  RamTier rt(Opts(64 * 4096), sink.fn());
  const BlockKey first{7, 11, 101};
  const BlockKey second{7, 11, 202};
  const std::string first_value = "tenant-a";
  const std::string second_value = "tenant-b";

  ASSERT_TRUE(
      rt.PutDurable(first, first_value.data(), first_value.size()));
  ASSERT_TRUE(
      rt.PutDurable(second, second_value.data(), second_value.size()));

  RamTier::Hit hit;
  ASSERT_TRUE(rt.GetPrep(first, 0, 0, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), first_value);
  hit = RamTier::Hit{};
  ASSERT_TRUE(rt.GetPrep(second, 0, 0, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), second_value);
  hit = RamTier::Hit{};

  EXPECT_TRUE(rt.Remove(first));
  EXPECT_FALSE(rt.Contains(first));
  EXPECT_TRUE(rt.Contains(second));
  ASSERT_TRUE(rt.GetPrep(second, 0, 0, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), second_value);
}

TEST(RamTier, WriteBackAcknowledgesVisiblePinnedRamBeforeFlush) {
  FlushSink sink;
  sink.close();
  RamTier rt(Opts(64 * 4096), sink.fn());
  const std::string value(4096, 'w');

  EXPECT_EQ(rt.PutWriteBack(K(101), value.data(), value.size()).status, Status::kOk);
  EXPECT_EQ(rt.RamAcks(), 1u);
  EXPECT_EQ(rt.DirtyObjects(), 1u);
  EXPECT_EQ(rt.DirtyBytes(), 4096u);
  RamTier::Hit hit;
  ASSERT_TRUE(rt.GetPrep(K(101), 0, 0, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), value);
  hit = RamTier::Hit{};

  sink.open();
  ASSERT_TRUE(WaitFor([&] { return rt.Flushed() == 1u; }));
  EXPECT_EQ(rt.DirtyObjects(), 0u);
  EXPECT_EQ(rt.DirtyBytes(), 0u);
}

TEST(RamTier, ZeroWatermarkWaitsForDiskCommit) {
  FlushSink sink;
  sink.close();
  RamTier::Options options = Opts(2 * 4096, 4096);
  options.large_reserve_bytes = 0;
  options.ack_high_watermark_pct = 0;
  RamTier rt(options, sink.fn());
  OpenFlushOnExit cleanup{sink};
  const std::string value(4096, 'b');
  Status result = Status::kInvalid;
  std::thread put([&] {
    result = rt.PutWriteBack(K(103), value.data(), value.size()).status;
  });
  // The flush callback is the barrier: this request has been admitted and
  // cannot be durable until we release it. No scheduling sleeps are needed.
  const bool entered = sink.await_calls(1);
  EXPECT_TRUE(entered);
  if (entered) {
    EXPECT_EQ(rt.RamAcks(), 0u);
    EXPECT_EQ(rt.DirtyObjects(), 1u);
  }
  sink.open();
  put.join();
  EXPECT_EQ(result, Status::kOk);
  EXPECT_EQ(rt.AckBackpressure(), 1u);
  EXPECT_EQ(rt.DirtyObjects(), 0u);
}

TEST(RamTier, PositiveWatermarkRejectsNewKeysButPreservesExistingOwners) {
  FlushSink sink;
  sink.close();
  RamTier::Options options = Opts(4 * 4096);
  options.large_reserve_bytes = 0;
  options.ack_high_watermark_pct = 25;
  RamTier rt(options, sink.fn());
  OpenFlushOnExit cleanup{sink};
  const std::string first(500, 'a'), different(1000, 'b');
  const auto accepted = rt.PutWriteBack(K(105), first.data(), first.size());
  ASSERT_EQ(accepted.status, Status::kOk);
  EXPECT_EQ(accepted.disposition, RamTier::PutDisposition::kComplete);
  ASSERT_TRUE(sink.await_calls(1));
  ASSERT_EQ(rt.DirtyBytes(), 4096u);  // exactly the positive watermark

  // Both payloads resolve to the same pending completion even at saturation.
  RamTierTestPeer::Completion leader, duplicate, rejected;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(105), first, &leader, true),
            RamTierTestPeer::Admission::kDuplicate);
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(105), different, &duplicate, true),
            RamTierTestPeer::Admission::kDuplicate);
  EXPECT_EQ(leader, duplicate);
  EXPECT_FALSE(RamTierTestPeer::Done(duplicate));
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(106), different, &rejected, true),
            RamTierTestPeer::Admission::kBackpressure);
  EXPECT_FALSE(rejected);  // rejection created no ownership or flush work
  EXPECT_FALSE(rt.Contains(K(106)));
  EXPECT_EQ(rt.DirtyBytes(), 4096u);
  EXPECT_EQ(rt.PutBypass(), 0u);
  EXPECT_EQ(rt.AckBackpressure(), 1u);

  // A removed durable copy is not a failed PUT owner. Its pinned allocation
  // must obey the same positive watermark as every other unadmitted request.
  ASSERT_TRUE(rt.PutDurable(K(107), first.data(), first.size()));
  RamTier::Hit pin;
  ASSERT_TRUE(rt.GetPrep(K(107), 0, 0, &pin));
  ASSERT_TRUE(rt.Remove(K(107)));
  const RamTier::PutResult removed =
      rt.PutWriteBack(K(107), different.data(), different.size());
  EXPECT_EQ(removed.status, Status::kCacheFull);
  EXPECT_EQ(removed.disposition, RamTier::PutDisposition::kBackpressure);
  EXPECT_EQ(rt.PutBypass(), 0u);
  EXPECT_EQ(rt.AckBackpressure(), 2u);
  EXPECT_EQ(std::string(pin.ptr, pin.len), first);
  pin = RamTier::Hit{};
  ASSERT_TRUE(rt.GetPrep(K(105), 0, 0, &pin));
  EXPECT_EQ(std::string(pin.ptr, pin.len), first);
  pin = RamTier::Hit{};

  sink.open();
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(leader));
  EXPECT_EQ(RamTierTestPeer::Wait(duplicate), Status::kOk);
  EXPECT_EQ(rt.DirtyBytes(), 0u);
  const auto recovered = rt.PutWriteBack(K(106), different.data(), different.size());
  EXPECT_EQ(recovered.status, Status::kOk);
  EXPECT_EQ(recovered.disposition, RamTier::PutDisposition::kComplete);
  ASSERT_EQ(rt.PutCommitted(K(106), first.data(), first.size()).status,
            Status::kOk);
  std::lock_guard<std::mutex> lock(sink.m);
  EXPECT_EQ(sink.values.at(K(105).Filename()), first);
  EXPECT_EQ(sink.values.at(K(106).Filename()), different);
}

TEST(RamTier, CapacityBypassRemainsDistinctFromTerminalBackpressure) {
  FlushSink sink;
  RamTier::Options options = Opts(4 * 4096);
  options.large_reserve_bytes = 0;
  RamTier rt(options, sink.fn());
  const std::string too_large(options.bytes + 4096, 'x');
  for (const auto result : {
           rt.PutWriteBack(K(108), too_large.data(), too_large.size()),
           rt.PutCommitted(K(109), too_large.data(), too_large.size())}) {
    EXPECT_EQ(result.status, Status::kCacheFull);
    EXPECT_EQ(result.disposition, RamTier::PutDisposition::kBypass);
  }
  EXPECT_EQ(rt.Count(), 0u);
  EXPECT_EQ(rt.DirtyBytes(), 0u);
  EXPECT_EQ(rt.AckBackpressure(), 0u);
}

TEST(RamTier, WriteBackFlushFailureIsPostAckAndUnhealthy) {
  FlushSink sink;
  sink.fail = true;
  RamTier::Options options = Opts(8 * 4096);
  options.flush_retries = 1;
  RamTier rt(options, sink.fn());
  const std::string value(500, 'f');

  EXPECT_EQ(rt.PutWriteBack(K(104), value.data(), value.size()).status, Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return rt.FlushDropped() == 1u; }));
  EXPECT_EQ(rt.PostAckFlushFailures(), 1u);
  EXPECT_EQ(rt.DirtyObjects(), 0u);
  EXPECT_FALSE(rt.healthy());
  EXPECT_FALSE(rt.Contains(K(104)));
}

TEST(RamTier, FlushMakesDurable) {
  FlushSink sink;
  RamTier rt(Opts(64 * 4096), sink.fn());
  std::string v(1000, 'd');
  ASSERT_TRUE(rt.Put(K(2), v.data(), v.size()));
  EXPECT_TRUE(WaitFor([&] { return rt.Flushed() == 1u; }));
  {
    std::lock_guard<std::mutex> lk(sink.m);
    EXPECT_TRUE(sink.flushed.count(K(2).Filename()));
  }
  EXPECT_EQ(rt.FlushBacklog(), 0u);
}

TEST(RamTier, FlushBacklogIncludesDequeuedInflightItem) {
  FlushSink sink;
  sink.close();
  RamTier rt(Opts(64 * 4096), sink.fn());
  std::string v(1000, 'q');
  ASSERT_TRUE(rt.Put(K(3), v.data(), v.size()));
  ASSERT_TRUE(WaitFor([&] {
    std::lock_guard<std::mutex> lk(sink.m);
    return sink.calls == 1;
  }));
  EXPECT_EQ(rt.FlushBacklog(), 1u);
  sink.open();
  ASSERT_TRUE(WaitFor([&] { return rt.Flushed() == 1u; }));
  EXPECT_EQ(rt.FlushBacklog(), 0u);
}

// Regression (hd04 prod, 2026-07): the RAM tier bound its whole arena to ONE
// SlabAllocator extent, so it could hold only a single size class at a time. A
// real KV working set has assorted value sizes (partial blocks, variable-length
// coalesced chunks); each cross-class Put stole the lone extent and evicted the
// whole tier (ram_objects collapsed to ~1-5 in a 16 GiB arena), so every reuse
// GET missed to slab and load ran at disk speed. With the arena partitioned into
// many extents, distinct size classes coexist and stay resident. Also exercises
// the global-arena-offset math (slots now live in extents > 0).
TEST(RamTier, VaryingSizesCoexistNoChurn) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "2097152", 1);  // 2 MiB extents
  FlushSink sink;
  RamTier rt(Opts(256ull << 20), sink.fn());             // 256 MiB arena -> ~128 extents
  ASSERT_TRUE(rt.ok());
  // 6 distinct size classes x 4 keys each (models a real KV mix: a handful of
  // block sizes, many blocks each). 6 classes * kStripeWays(8) = 48 extents of
  // headroom vs 128 available. Each value < the 2 MiB extent.
  const size_t sizes[6] = {40000, 90000, 200000, 380000, 560000, 730000};
  std::vector<std::pair<uint64_t, size_t>> items;
  for (int i = 0; i < 24; ++i) {
    size_t sz = sizes[i % 6];
    std::vector<char> v(sz, static_cast<char>(i + 1));
    ASSERT_TRUE(rt.Put(K(1000 + i), v.data(), v.size())) << "put i=" << i;
    items.emplace_back(1000 + i, sz);
  }
  // All resident (no cross-class eviction) and byte-correct after the global
  // offset resolves each slot's extent.
  EXPECT_EQ(rt.Count(), items.size());
  EXPECT_EQ(rt.Evictions(), 0u);
  for (auto& [id, sz] : items) {
    RamTier::Hit h;
    ASSERT_TRUE(rt.GetPrep(K(id), 0, sz, &h)) << "get id=" << id;
    EXPECT_EQ(h.len, sz);
    EXPECT_EQ(static_cast<unsigned char>(h.ptr[0]),
              static_cast<unsigned char>((id - 1000) + 1));
    h = RamTier::Hit{};
  }
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
}

TEST(RamTier, MissReturnsFalse) {
  FlushSink sink;
  RamTier rt(Opts(64 * 4096), sink.fn());
  RamTier::Hit h;
  EXPECT_FALSE(rt.GetPrep(K(99), 0, 10, &h));
  EXPECT_EQ(rt.Misses(), 1u);
}

TEST(RamTier, SharedRangeContract) {
  FlushSink sink;
  RamTier rt(Opts(64 * 4096), sink.fn());
  const std::string v = "0123456789";
  ASSERT_TRUE(rt.Put(K(3), v.data(), v.size()));
  RamTier::Hit h;

  ASSERT_TRUE(rt.GetPrep(K(3), 3, 0, &h));
  EXPECT_EQ(std::string(h.ptr, h.len), "3456789");
  EXPECT_EQ(h.value_len, v.size());
  h = RamTier::Hit{};

  ASSERT_TRUE(rt.GetPrep(K(3), v.size(), 4, &h));
  EXPECT_EQ(h.len, 0u);
  EXPECT_EQ(h.value_len, v.size());
  h = RamTier::Hit{};

  EXPECT_FALSE(rt.GetPrep(K(3), v.size() + 1, 4, &h));

  ASSERT_TRUE(rt.GetPrep(K(3), 3, 100, &h));
  EXPECT_EQ(std::string(h.ptr, h.len), "3456789");
  EXPECT_EQ(h.value_len, v.size());
  h = RamTier::Hit{};
}

TEST(RamTier, SendPinBlocksEvictionUntilRelease) {
  FlushSink sink;
  RamTier rt(Opts(2 * 4096, 4096), sink.fn());  // exactly 2 slots
  std::string v(4096, 'x');
  ASSERT_TRUE(rt.Put(K(10), v.data(), v.size()));
  ASSERT_TRUE(rt.Put(K(11), v.data(), v.size()));
  EXPECT_TRUE(WaitFor([&] { return rt.Flushed() == 2u; }));  // both durable

  RamTier::Hit h;
  ASSERT_TRUE(rt.GetPrep(K(10), 0, v.size(), &h));  // send-pin on K10
  // Putting a 3rd must evict the *unpinned* durable one (K11), never K10.
  ASSERT_TRUE(rt.Put(K(12), v.data(), v.size()));
  EXPECT_TRUE(WaitFor([&] { return rt.Flushed() == 3u; }));
  EXPECT_TRUE(rt.Contains(K(10))) << "send-pinned slot must survive";
  EXPECT_TRUE(rt.Contains(K(12)));
  EXPECT_FALSE(rt.Contains(K(11)));
  EXPECT_GE(rt.Evictions(), 1u);
  h = RamTier::Hit{};
}

TEST(RamTier, SendPinTokenSurvivesResidentIndexRehash) {
  FlushSink sink;
  RamTier rt(Opts(32ull << 20), sink.fn());
  const std::string value(4096, 'r');
  const BlockKey pinned = K(1'000'000);
  ASSERT_TRUE(rt.PutDurable(pinned, value.data(), value.size()));

  RamTier::Hit hit;
  ASSERT_TRUE(rt.GetPrep(pinned, 0, value.size(), &hit));
  ASSERT_EQ(std::string(hit.ptr, hit.len), value);
  for (uint64_t id = 1; id <= 4096; ++id)
    ASSERT_TRUE(rt.PutDurable(K(id), value.data(), value.size()));

  EXPECT_TRUE(rt.Contains(pinned));
  hit = RamTier::Hit{};
  EXPECT_TRUE(rt.Remove(pinned));
  EXPECT_FALSE(rt.Contains(pinned));
}

TEST(RamTier, BackpressureBypassWhenFlushStalls) {
  FlushSink sink;
  sink.close();  // stall all flushes -> slots stay RAM_ONLY (flush-pinned)
  RamTier rt(Opts(2 * 4096, 4096), sink.fn());
  std::string v(4096, 'p');
  ASSERT_TRUE(rt.Put(K(20), v.data(), v.size()));
  ASSERT_TRUE(rt.Put(K(21), v.data(), v.size()));
  // Arena full of non-evictable slots -> the next Put is declined (bypass).
  EXPECT_FALSE(rt.Put(K(22), v.data(), v.size()));
  EXPECT_EQ(rt.PutBypass(), 1u);
  // Drain: opening the gate lets flushes land, freeing capacity again.
  sink.open();
  EXPECT_TRUE(WaitFor([&] { return rt.Flushed() == 2u; }));
  EXPECT_TRUE(rt.Put(K(22), v.data(), v.size()));
}

TEST(RamTier, FlushFailureRetriesThenDrops) {
  FlushSink sink;
  sink.fail = true;  // every flush attempt fails
  RamTier::Options o = Opts(8 * 4096);
  o.flush_retries = 3;
  RamTier rt(o, sink.fn());
  std::string v(500, 'f');
  ASSERT_TRUE(rt.Put(K(30), v.data(), v.size()));
  // After flush_retries failed attempts the entry is dropped from RAM.
  EXPECT_TRUE(WaitFor([&] { return rt.FlushDropped() == 1u; }));
  EXPECT_FALSE(rt.Contains(K(30)));
  {
    std::lock_guard<std::mutex> lk(sink.m);
    EXPECT_GE(sink.calls, 3);
  }
}
TEST(RamTier, InflightConflictingDuplicateKeepsLeaderPayloadAndCompletion) {
  FlushSink sink;
  sink.close();
  RamTier::Options options = Opts(8 * 4096);
  options.large_reserve_bytes = 0;
  RamTier rt(options, sink.fn());
  OpenFlushOnExit cleanup{sink};
  const std::string leader(500, 'a');
  // A duplicate larger than the entire tier must still join the first value.
  const std::string follower(options.bytes + 4096, 'b');
  RamTierTestPeer::Completion first, second;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(31), leader, &first),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(sink.await_calls(1));
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(31), follower, &second),
            RamTierTestPeer::Admission::kDuplicate);
  ASSERT_EQ(first, second);
  EXPECT_FALSE(RamTierTestPeer::Done(first));
  EXPECT_EQ(rt.PutBypass(), 0u);
  RamTier::Hit hit;
  ASSERT_TRUE(rt.GetPrep(K(31), 0, 0, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), leader);
  hit = RamTier::Hit{};

  sink.open();
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(first));
  EXPECT_EQ(RamTierTestPeer::Wait(first), Status::kOk);
  EXPECT_EQ(RamTierTestPeer::Wait(second), Status::kOk);
  // Both public ACK modes preserve the leader after commit, too.
  EXPECT_EQ(rt.PutCommitted(K(31), follower.data(), follower.size()).status,
            Status::kOk);
  EXPECT_EQ(rt.PutWriteBack(K(31), follower.data(), follower.size()).status,
            Status::kOk);
  std::lock_guard<std::mutex> lock(sink.m);
  EXPECT_EQ(sink.calls, 1);
  EXPECT_EQ(sink.values.at(K(31).Filename()), leader);
}

TEST(RamTier, InflightDuplicateSharesTerminalLeaderFailure) {
  FlushSink sink;
  sink.close();
  sink.fail = true;
  RamTier::Options options = Opts(8 * 4096);
  options.large_reserve_bytes = 0;
  options.flush_retries = 1;
  RamTier rt(options, sink.fn());
  OpenFlushOnExit cleanup{sink};
  const std::string leader(500, 'a');
  const std::string follower(1000, 'b');
  RamTierTestPeer::Completion first, second;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(32), leader, &first),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(sink.await_calls(1));
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(32), follower, &second),
            RamTierTestPeer::Admission::kDuplicate);
  ASSERT_EQ(first, second);
  EXPECT_FALSE(RamTierTestPeer::Done(second));
  sink.open();
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(first));
  EXPECT_EQ(RamTierTestPeer::Wait(first), Status::kIOError);
  EXPECT_EQ(RamTierTestPeer::Wait(second), Status::kIOError);
  EXPECT_EQ(rt.PutBypass(), 0u);
  EXPECT_FALSE(rt.healthy());
  EXPECT_FALSE(rt.Contains(K(32)));
}

TEST(RamTier, RemoveCancelsQueuedGenerationBeforeReinsert) {
  FlushSink sink;
  sink.close();
  RamTier::Options options = Opts(8 * 4096);
  options.large_reserve_bytes = 0;
  RamTier rt(options, sink.fn());
  OpenFlushOnExit cleanup{sink};
  const std::string old_value(500, 'q');
  const std::string new_value(700, 'n');
  ASSERT_TRUE(rt.Put(K(33), old_value.data(), old_value.size()));
  ASSERT_TRUE(sink.await_calls(1));  // sole flusher owns the blocker

  RamTierTestPeer::Completion old, duplicate, replacement;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(34), old_value, &old),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(34), new_value, &duplicate),
            RamTierTestPeer::Admission::kDuplicate);
  ASSERT_EQ(old, duplicate);
  ASSERT_TRUE(rt.Remove(K(34)));  // queued, so cancellation is synchronous
  EXPECT_EQ(RamTierTestPeer::Wait(old), Status::kIOError);
  EXPECT_EQ(RamTierTestPeer::Wait(duplicate), Status::kIOError);
  EXPECT_FALSE(rt.Contains(K(34)));
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(34), new_value, &replacement),
            RamTierTestPeer::Admission::kAccepted);
  EXPECT_NE(old, replacement);
  EXPECT_FALSE(RamTierTestPeer::Done(replacement));
  sink.open();
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(replacement));
  EXPECT_EQ(RamTierTestPeer::Wait(replacement), Status::kOk);
  EXPECT_EQ(RamTierTestPeer::Wait(old), Status::kIOError);
  std::lock_guard<std::mutex> lock(sink.m);
  EXPECT_EQ(sink.calls, 2);
  EXPECT_EQ(sink.values.at(K(34).Filename()), new_value);
}

TEST(RamTier, RemovedPinnedDurableCopyAllowsBypass) {
  FlushSink sink;
  RamTier::Options options = Opts(8 * 4096);
  options.large_reserve_bytes = 0;
  RamTier rt(options, sink.fn());
  const std::string first(500, 'a');
  const std::string conflicting(options.bytes + 4096, 'b');
  ASSERT_TRUE(rt.PutDurable(K(35), first.data(), first.size()));
  RamTier::Hit pinned;
  ASSERT_TRUE(rt.GetPrep(K(35), 0, 0, &pinned));
  ASSERT_TRUE(rt.Remove(K(35)));
  EXPECT_FALSE(rt.Contains(K(35)));
  // A retired durable cache copy does not own a new PUT. It may bypass RAM
  // without waiting for SEND, but must not overwrite or release its old slot.
  const uint64_t used = rt.UsedBytes();
  const RamTier::PutResult disk_ack =
      rt.PutCommitted(K(35), conflicting.data(), conflicting.size());
  const RamTier::PutResult ram_ack =
      rt.PutWriteBack(K(35), conflicting.data(), conflicting.size());
  EXPECT_EQ(disk_ack.status, Status::kCacheFull);
  EXPECT_EQ(disk_ack.disposition, RamTier::PutDisposition::kBypass);
  EXPECT_EQ(ram_ack.status, Status::kCacheFull);
  EXPECT_EQ(ram_ack.disposition, RamTier::PutDisposition::kBypass);
  EXPECT_EQ(rt.PutBypass(), 2u);
  EXPECT_EQ(rt.UsedBytes(), used);
  {
    const std::lock_guard<std::mutex> lock(sink.m);
    EXPECT_EQ(sink.calls, 0);
  }
  EXPECT_EQ(std::string(pinned.ptr, pinned.len), first);
  pinned = RamTier::Hit{};
  EXPECT_EQ(rt.UsedBytes(), 0u);
  const std::string replacement(700, 'n');
  EXPECT_EQ(rt.PutCommitted(K(35), replacement.data(), replacement.size()).status,
            Status::kOk);
  ASSERT_TRUE(rt.GetPrep(K(35), 0, 0, &pinned));
  EXPECT_EQ(std::string(pinned.ptr, pinned.len), replacement);
}

TEST(RamTier, DuplicateOfAbortedHiddenReservationSharesFailure) {
  FlushSink sink;
  RamTier::Options options = Opts(8 * 4096);
  options.large_reserve_bytes = 0;
  RamTier rt(options, sink.fn());
  RamTier::DurableReservation reservation;
  ASSERT_TRUE(rt.ReserveDurable(K(36), 4096, &reservation));
  const std::string duplicate(1000, 'd');
  RamTierTestPeer::Completion completion;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(36), duplicate, &completion),
            RamTierTestPeer::Admission::kDuplicate);
  EXPECT_FALSE(RamTierTestPeer::Done(completion));
  EXPECT_FALSE(rt.Contains(K(36)));
  reservation = RamTier::DurableReservation{};
  ASSERT_TRUE(RamTierTestPeer::Done(completion));
  EXPECT_EQ(RamTierTestPeer::Wait(completion), Status::kIOError);
  EXPECT_FALSE(rt.Contains(K(36)));
  EXPECT_EQ(rt.UsedBytes(), 0u);
  EXPECT_EQ(rt.PutBypass(), 0u);
}

TEST(RamTier, RemoveHidesActiveFlushAndDefersPinnedRelease) {
  FlushSink sink;
  sink.close();
  RamTier rt(Opts(8 * 4096), sink.fn());
  std::string v(200, 'r');
  ASSERT_TRUE(rt.Put(K(40), v.data(), v.size()));
  EXPECT_TRUE(WaitFor([&] {
    std::lock_guard<std::mutex> lk(sink.m);
    return sink.calls == 1;
  }));

  RamTier::Hit h;
  ASSERT_TRUE(rt.GetPrep(K(40), 0, v.size(), &h));
  std::atomic<bool> removed{false};
  std::thread remover([&] {
    removed.store(rt.Remove(K(40)), std::memory_order_release);
  });
  EXPECT_TRUE(WaitFor([&] { return !rt.Contains(K(40)); }));
  EXPECT_FALSE(removed.load(std::memory_order_acquire));
  EXPECT_EQ(std::string(h.ptr, h.len), v);
  sink.open();
  remover.join();
  EXPECT_TRUE(removed.load(std::memory_order_acquire));
  EXPECT_FALSE(rt.Contains(K(40)));
  h = RamTier::Hit{};
  EXPECT_EQ(rt.Count(), 0u);
}

// Regression: Remove() racing an in-flight PUT must not resurrect the key.
// Admit used to read completion->canceled in one critical section and erase
// writing + install the index entry in another; a Remove landing in between
// flagged the canceled bit too late, the entry was installed untombstoned,
// and the key kept answering GETs after Remove returned true (the disk copy
// is deleted by the caller on remove, so the tier served a deleted value).
// Two synchronized windows cover cancellation before the check and removal
// after the check. Checking real lock ownership also catches split publication
// even when Remove's final tombstone/drop compensates for an incorrect install.
TEST(RamTier, RemoveRacingInFlightPutNeverResurrects) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  ::setenv("DFKV_RAM_TIER_SHARDS", "1", 1);
  FlushSink sink;
  RamTier::Options options = Opts(64ull << 20);
  options.large_reserve_bytes = 0;
  options.reclaim_interval_ms = 0;
  RamTier rt(options, sink.fn());
  ASSERT_TRUE(rt.ok());
  const std::string value(1ull << 20, 'v');
  using Phase = RamTierTestPeer::AdmitPhase;
  const Phase phases[] = {Phase::kBeforeCancelCheck, Phase::kAfterCancelCheck};
  int observed = 0;
  int resurrections = 0;
  for (const Phase pause_at : phases) {
    for (uint64_t round = 0; round < 3; ++round) {
      SCOPED_TRACE("phase=" + std::to_string(static_cast<int>(pause_at)) +
                   " round=" + std::to_string(round));
      const BlockKey key = K(3 * static_cast<uint64_t>(pause_at) + round);
      std::promise<void> paused_signal, release_signal, remove_signal;
      const std::future<void> paused = paused_signal.get_future();
      const std::future<void> release = release_signal.get_future();
      const std::future<void> remove_started = remove_signal.get_future();
      RamTierTestPeer::Completion completion;
      int hook_calls = 0;
      RamTierTestPeer::SetAdmitHook(rt, [&](Phase phase, bool lock_owned) {
        ++hook_calls;
        EXPECT_EQ(lock_owned, phase == Phase::kAfterCancelCheck);
        if (phase == Phase::kBeforeCancelCheck && !lock_owned)
          completion = RamTierTestPeer::Writing(rt, key);
        if (phase == pause_at) {
          paused_signal.set_value();
          release.wait();
        }
      });
      bool put = false, removed = false;
      std::thread writer(
          [&] { put = rt.Put(key, value.data(), value.size()); });
      const bool entered =
          paused.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
      EXPECT_TRUE(entered);
      // Never leave a failed assertion with a paused thread or callback that
      // refers to the next round's destroyed synchronization objects.
      if (!entered || !completion) {
        release_signal.set_value();
        writer.join();
        RamTierTestPeer::SetAdmitHook(rt, {});
        EXPECT_NE(completion, nullptr);  // Only inspect after writer joined.
        continue;
      }
      EXPECT_FALSE(RamTierTestPeer::Done(completion));
      std::thread remover([&] {
        remove_signal.set_value();
        removed = rt.Remove(key);
      });
      bool canceled_before_check = false;
      if (pause_at == Phase::kBeforeCancelCheck) {
        canceled_before_check =
            WaitFor([&] { return RamTierTestPeer::Canceled(completion); });
        EXPECT_TRUE(canceled_before_check);
      } else {
        // Do not infer exclusion from a timeout: the hook already checked the
        // actual lock held at the canceled read. Then exercise real Remove.
        EXPECT_EQ(remove_started.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
      }
      release_signal.set_value();
      writer.join();
      remover.join();
      RamTierTestPeer::SetAdmitHook(rt, {});
      EXPECT_EQ(hook_calls, 2);
      EXPECT_EQ(put, pause_at == Phase::kAfterCancelCheck);
      EXPECT_TRUE(removed);
      EXPECT_TRUE(RamTierTestPeer::Done(completion));
      if (canceled_before_check && removed) ++observed;
      if (rt.Contains(key)) ++resurrections;
      RamTier::Hit hit;
      if (rt.GetPrep(key, 0, 0, &hit)) ++resurrections;
      hit = RamTier::Hit{};
      rt.Remove(key);  // Also clean up a deliberately faulty mutation run.
      EXPECT_EQ(rt.Count(), 0u);
      EXPECT_EQ(rt.UsedBytes(), 0u);
      EXPECT_EQ(rt.DirtyObjects(), 0u);
      EXPECT_EQ(rt.FlushBacklog(), 0u);
    }
  }
  EXPECT_GE(observed, 3);  // Three actual writing cancellations, not sleeps.
  EXPECT_EQ(resurrections, 0);
}

// Regression: same cancel/install race through PutDurable (read-promotion),
// which installed the entry without ever consulting completion->canceled. The
// promotion's setup window spans a large posix_memalign + memcpy, so a Remove
// started inside it deterministically flags the shared completion; before the
// fix the entry was still installed (and completed) untombstoned.
TEST(RamTier, RemoveRacingInFlightPutDurableNeverResurrects) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  ::setenv("DFKV_RAM_TIER_SHARDS", "1", 1);
  FlushSink sink;  // unused: promoted entries are born durable, never flushed
  RamTier::Options o = Opts(96ull << 20);
  o.large_reserve_bytes = 48ull << 20;  // fit the 8 MiB dedicated values
  RamTier rt(o, sink.fn());
  ASSERT_TRUE(rt.ok());
  const std::string v(8ull << 20, 'p');  // dedicated: ms-wide install window

  int observed = 0;
  int resurrections = 0;
  for (uint64_t r = 0; r < 40; ++r) {
    std::atomic<bool> started{false};
    std::thread writer([&] {
      started.store(true, std::memory_order_release);
      rt.PutDurable(K(r), v.data(), v.size());
    });
    while (!started.load(std::memory_order_acquire))
      std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::microseconds(100 + (r % 9) * 100));
    const bool removed = rt.Remove(K(r));
    writer.join();
    if (removed) {
      ++observed;
      if (rt.Contains(K(r))) ++resurrections;
      RamTier::Hit h;
      if (rt.GetPrep(K(r), 0, 0, &h)) ++resurrections;
    }
    rt.Remove(K(r));  // cleanup
  }

  EXPECT_GE(observed, 3);  // prove the race regime was actually entered
  EXPECT_EQ(resurrections, 0);
}

TEST(RamTier, LargeValueReadRangeFlushAndRemoveIsByteExact) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  std::mutex m;
  std::string flushed;
  size_t flushed_len = 0;
  size_t flushed_cap = 0;
  RamTier rt(Opts(4ull << 20),
             [&](const BlockKey&, char* data, size_t len, size_t cap) {
               std::lock_guard<std::mutex> lk(m);
               flushed.assign(data, len);
               flushed_len = len;
               flushed_cap = cap;
               return true;
             });
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
  ASSERT_TRUE(rt.ok());
  EXPECT_EQ(rt.arena_bytes() + rt.large_budget_bytes(), 4ull << 20);
  EXPECT_EQ(rt.large_budget_bytes(), 2ull << 20);
  EXPECT_LE(rt.arena_bytes(), rt.budget_bytes());
  int fake_mr = 0;
  rt.SetArenaMr(&fake_mr);

  std::string value((1ull << 20) + 137, '\0');
  for (size_t i = 0; i < value.size(); ++i)
    value[i] = static_cast<char>((i * 131 + 17) & 0xff);
  ASSERT_TRUE(rt.Put(K(50), value.data(), value.size()));
  EXPECT_EQ(rt.UsedBytes(), ((value.size() + 4095) / 4096) * 4096);
  EXPECT_EQ(rt.large_used_bytes(), rt.UsedBytes());
  EXPECT_LE(rt.arena_bytes() + rt.large_used_bytes(), rt.budget_bytes());

  RamTier::Hit whole;
  ASSERT_TRUE(rt.GetPrep(K(50), 0, 0, &whole));
  EXPECT_FALSE(whole.in_arena);
  EXPECT_EQ(whole.mr, nullptr);
  EXPECT_EQ(whole.value_len, value.size());
  EXPECT_EQ(std::string(whole.ptr, whole.len), value);
  whole = RamTier::Hit{};

  RamTier::Hit range;
  ASSERT_TRUE(rt.GetPrep(K(50), (1ull << 20) - 19, 91, &range));
  EXPECT_FALSE(range.in_arena);
  EXPECT_EQ(std::string(range.ptr, range.len),
            value.substr((1ull << 20) - 19, 91));
  range = RamTier::Hit{};

  ASSERT_TRUE(WaitFor([&] { return rt.Flushed() == 1; }));
  {
    std::lock_guard<std::mutex> lk(m);
    EXPECT_EQ(flushed_len, value.size());
    EXPECT_GE(flushed_cap, flushed_len);
    EXPECT_EQ(flushed, value);
  }
  EXPECT_TRUE(rt.Remove(K(50)));
  EXPECT_FALSE(rt.Contains(K(50)));
  EXPECT_EQ(rt.UsedBytes(), 0u);
  EXPECT_EQ(rt.large_used_bytes(), 0u);
}

TEST(RamTier, LargeValueSendPinDefersPhysicalRemovalAndCapacityRelease) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  FlushSink sink;
  RamTier rt(Opts(2ull << 20), sink.fn());
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
  const std::string first((1ull << 20) + 1, 'a');
  const std::string second((1ull << 20) + 1, 'b');
  ASSERT_TRUE(rt.Put(K(51), first.data(), first.size()));
  ASSERT_TRUE(WaitFor([&] { return rt.Flushed() == 1; }));

  RamTier::Hit pinned;
  ASSERT_TRUE(rt.GetPrep(K(51), 0, 0, &pinned));
  ASSERT_FALSE(pinned.in_arena);
  EXPECT_TRUE(rt.Remove(K(51)));
  EXPECT_FALSE(rt.Contains(K(51)));
  EXPECT_FALSE(rt.Put(K(52), second.data(), second.size()))
      << "pinned durable large value must not be reclaimed";
  EXPECT_LE(rt.UsedBytes(), 2ull << 20);

  EXPECT_EQ(std::string(pinned.ptr, pinned.len), first)
      << "logical removal must not invalidate an in-flight pointer";
  pinned = RamTier::Hit{};
  EXPECT_TRUE(rt.Put(K(52), second.data(), second.size()))
      << "final send release must make the physical capacity reusable";
  EXPECT_FALSE(rt.Contains(K(51)));
  EXPECT_TRUE(rt.Contains(K(52)));
  EXPECT_LE(rt.UsedBytes(), 2ull << 20);
}

TEST(RamTier, LargeValueFlushFailureDropsAndReleasesBudget) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  FlushSink sink;
  sink.close();
  RamTier::Options o = Opts(2ull << 20);
  o.flush_retries = 1;
  RamTier rt(o, sink.fn());
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
  const std::string value((1ull << 20) + 1, 'f');
  ASSERT_TRUE(rt.Put(K(53), value.data(), value.size()));
  RamTier::Hit pinned;
  ASSERT_TRUE(rt.GetPrep(K(53), 0, 0, &pinned));
  ASSERT_FALSE(pinned.in_arena);
  EXPECT_FALSE(rt.Put(K(54), value.data(), value.size()))
      << "non-durable large allocation applies byte-budget backpressure";
  EXPECT_EQ(rt.PutBypass(), 1u);

  {
    std::lock_guard<std::mutex> lk(sink.m);
    sink.fail = true;
  }
  sink.open();
  ASSERT_TRUE(WaitFor([&] { return rt.FlushDropped() == 1; }));
  EXPECT_FALSE(rt.Contains(K(53)));
  EXPECT_GT(rt.UsedBytes(), 0u)
      << "remove-pending allocation stays owned while a reader holds it";
  EXPECT_EQ(std::string(pinned.ptr, pinned.len), value);
  pinned = RamTier::Hit{};
  EXPECT_EQ(rt.UsedBytes(), 0u);

  {
    std::lock_guard<std::mutex> lk(sink.m);
    sink.fail = false;
  }
  EXPECT_TRUE(rt.Put(K(54), value.data(), value.size()));
  EXPECT_LE(rt.UsedBytes(), 2ull << 20);
}

TEST(RamTier, ValueAboveTotalBudgetBypassesWithoutAccountingLeak) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  FlushSink sink;
  RamTier rt(Opts(2ull << 20), sink.fn());
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
  const std::string too_large((2ull << 20) + 1, 'x');
  EXPECT_FALSE(rt.Put(K(55), too_large.data(), too_large.size()));
  EXPECT_EQ(rt.PutBypass(), 1u);
  EXPECT_EQ(rt.UsedBytes(), 0u);
  EXPECT_EQ(rt.Count(), 0u);
}

TEST(RamTier, ConcurrentPutGetReleaseIsRaceFree) {
  FlushSink sink;
  RamTier rt(Opts(512 * 4096), sink.fn());
  constexpr int T = 8, N = 1500;
  std::atomic<int> hits{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < T; ++t) {
    ts.emplace_back([&, t] {
      std::string v(300, 'c');
      for (int i = 0; i < N; ++i) {
        BlockKey k = K((t * N + i) % 3000);
        rt.Put(k, v.data(), v.size());
        RamTier::Hit h;
        if (rt.GetPrep(k, 0, v.size(), &h)) {
          hits.fetch_add(1);
          h = RamTier::Hit{};
        }
      }
    });
  }
  for (auto& th : ts) th.join();
  EXPECT_GT(hits.load(), 0);
}

// Background reclaimer wiring: once the arena is full of DURABLE slots and new
// writes keep coming, the reclaimer (not the Put path) pre-frees slots, so
// admission rides pop-free-slot and never hits backpressure bypass.
TEST(RamTier, BackgroundReclaimerPreFreesDurableSlots) {
  FlushSink sink;
  auto o = Opts(16 * 4096);  // 16 slots, one extent (pool exhausted at first bind)
  o.reclaim_interval_ms = 1;
  RamTier rt(o, sink.fn());
  ASSERT_TRUE(rt.ok());
  std::string v(4000, 'r');
  for (uint64_t i = 0; i < 16; ++i) ASSERT_TRUE(rt.Put(K(100 + i), v.data(), v.size()));
  ASSERT_TRUE(WaitFor([&] { return rt.FlushBacklog() == 0; }));  // all durable
  // Sustained new-key writes: demand makes the reclaimer top up free slots.
  for (uint64_t i = 0; i < 32; ++i) {
    EXPECT_TRUE(rt.Put(K(200 + i), v.data(), v.size()));
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(WaitFor([&] { return rt.Reclaimed() > 0; }))
      << "reclaimer never pre-freed a slot";
  EXPECT_EQ(rt.PutBypass(), 0u);
}

// Class rebalance in the RAM tier: arena full of an idle class, then a burst of
// a NEW size larger than its first extents -- the tick must move extents from
// the idle donor so the burst stays resident (stock retained ~one extent).
TEST(RamTier, ReclaimTickGrowsHotClassFromColdDonor) {
  FlushSink sink;
  RamTier::Options o;
  o.bytes = 16ull << 20;  // 16 MiB arena
  o.large_reserve_bytes = 0;  // isolate fixed-slot rebalance capacity
  o.slot_granularity = 4096;
  o.reclaim_interval_ms = 1;
  setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);  // 16 extents x 1 MiB
  RamTier rt(o, sink.fn());
  unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
  ASSERT_TRUE(rt.ok());
  std::string a(4000, 'a');
  for (uint64_t i = 0; i < 4096; ++i) ASSERT_TRUE(rt.Put(K(30000 + i), a.data(), a.size()));
  ASSERT_TRUE(WaitFor([&] { return rt.FlushBacklog() == 0; }));  // donor durable
  std::string b(16000, 'b');
  for (uint64_t i = 0; i < 256; ++i) {  // 4 extents' worth of a new class
    ASSERT_TRUE(rt.Put(K(40000 + i), b.data(), b.size()));
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(WaitFor([&] { return rt.FlushBacklog() == 0; }));
  size_t resident = 0;
  for (uint64_t i = 0; i < 256; ++i) resident += rt.Contains(K(40000 + i));
  EXPECT_GT(rt.Rebalanced(), 0u) << "growth never kicked in";
  EXPECT_GE(resident, 250u) << "hot class must absorb capacity, not eat itself";
}

// Batched flush sink: a worker drains multiple queued items into ONE batch
// call; per-item retry/drop semantics survive partial batch failure.
TEST(RamTier, BatchFlushDrainsQueueAndRetriesPerItem) {
  std::mutex m;
  std::vector<size_t> batch_sizes;
  std::atomic<int> fail_key{-1};
  std::mutex gate_mu;
  std::condition_variable gate_cv;
  bool gate_open = false;
  RamTier::Options o = Opts(256 * 4096);
  o.flush_threads = 1;  // single worker => deterministic batching
  RamTier rt(o, nullptr);
  rt.set_flush_batch([&](const std::vector<RamTier::FlushItem>& items) {
    {
      std::unique_lock<std::mutex> lk(gate_mu);
      gate_cv.wait(lk, [&] { return gate_open; });
    }
    std::lock_guard<std::mutex> lk(m);
    batch_sizes.push_back(items.size());
    std::vector<bool> ok(items.size(), true);
    for (size_t i = 0; i < items.size(); ++i)
      if (fail_key >= 0 &&
          items[i].key.digest_hi ==
              static_cast<uint64_t>(fail_key.load()))
        ok[i] = false;
    return ok;
  });
  ASSERT_TRUE(rt.ok());
  std::string v(4000, 'b');
  bool queued = true;
  for (uint64_t i = 0; i < 40; ++i)
    queued = rt.Put(K(700 + i), v.data(), v.size()) && queued;
  {
    std::lock_guard<std::mutex> lk(gate_mu);
    gate_open = true;
  }
  gate_cv.notify_one();
  ASSERT_TRUE(queued);
  ASSERT_TRUE(WaitFor([&] { return rt.Flushed() == 40u; }));
  {
    std::lock_guard<std::mutex> lk(m);
    size_t mx = 0; for (size_t b : batch_sizes) mx = std::max(mx, b);
    EXPECT_GT(mx, 1u) << "worker never batched";
  }
  // Partial failure: one key keeps failing -> retried then dropped; others flush.
  fail_key = 900;
  ASSERT_TRUE(rt.Put(K(900), v.data(), v.size()));
  ASSERT_TRUE(rt.Put(K(901), v.data(), v.size()));
  ASSERT_TRUE(WaitFor([&] { return rt.FlushDropped() == 1u; }));
  EXPECT_TRUE(WaitFor([&] { return rt.Flushed() >= 41u; }));
  EXPECT_FALSE(rt.Contains(K(900))) << "dropped after retries";
  EXPECT_TRUE(rt.Contains(K(901)));
}


// Sharded tier: keys route by hash to independent shards; the whole lifecycle
// (put -> visible -> flush -> durable -> get/pin/release -> remove) must hold
// with shards > 1 exactly as with the single-lock layout.
TEST(RamTier, ShardedLifecycleAcrossShards) {
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);  // 1 MiB extents
  ::setenv("DFKV_RAM_TIER_SHARDS", "8", 1);
  FlushSink sink;  // gate open by default
  RamTier::Options o;
  o.bytes = 256ull << 20;  // 256 extents -> 32/shard, keeps 8 shards
  o.large_reserve_bytes = 0;  // this test exercises arena sharding only
  o.slot_granularity = 4096;
  o.flush_threads = 8;
  RamTier t(o, sink.fn());
  ASSERT_TRUE(t.ok());
  EXPECT_EQ(t.shards(), 8u);
  EXPECT_EQ(t.flusher_count(), 8u);

  const int N = 512;  // hash spreads these across all shards
  std::string v(8000, 'x');
  for (int i = 0; i < N; ++i) ASSERT_TRUE(t.Put(K(1000 + i), v.data(), v.size())) << i;
  EXPECT_EQ(t.Count(), static_cast<size_t>(N));
  // Wait for every shard's flushers to drain.
  for (int spin = 0; spin < 500 && t.Flushed() < static_cast<uint64_t>(N); ++spin)
    std::this_thread::sleep_for(10ms);
  EXPECT_EQ(t.Flushed(), static_cast<uint64_t>(N));
  EXPECT_EQ(t.FlushBacklog(), 0u);

  // Every key readable; tokens (shard-encoded) release correctly.
  for (int i = 0; i < N; ++i) {
    RamTier::Hit h;
    ASSERT_TRUE(t.GetPrep(K(1000 + i), 0, 0, &h)) << i;
    EXPECT_EQ(h.len, v.size());
    EXPECT_EQ(std::string(h.ptr, 16), v.substr(0, 16));
    h = RamTier::Hit{};
  }
  // Durable + idle -> removable, on whichever shard the key lives.
  for (int i = 0; i < N; i += 7) EXPECT_TRUE(t.Remove(K(1000 + i))) << i;
  ::unsetenv("DFKV_RAM_TIER_SHARDS");
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
}

TEST(RamTier, RejectsUnsupportedNumaMode) {
  ::setenv("DFKV_RAM_TIER_NUMA", "2", 1);
  FlushSink sink;
  RamTier rt(Opts(8 * 4096), sink.fn());
  EXPECT_FALSE(rt.ok());
  ::unsetenv("DFKV_RAM_TIER_NUMA");
}

TEST(RamTier, ShardCountCanExceedLegacyCapOf16) {
  // Phase 10: kTokenShardBits 4->6 lifts the cap 16->64 so read/write-heavy
  // multi-connection deployments can shard past the old ceiling. 32 shards must
  // be honored (not clamped to 16), and shard-encoded tokens still round-trip.
  // 1 MiB extents x 1 GiB = 1024 extents => 32 shards sustains the
  // >=32 extents/shard rule. Dedicated residency is disabled here so the test
  // isolates token routing and fixed-arena shard geometry.
  ::setenv("DFKV_RAM_TIER_EXTENT_BYTES", "1048576", 1);
  ::setenv("DFKV_RAM_TIER_SHARDS", "32", 1);
  FlushSink sink;
  RamTier::Options o;
  o.bytes = 1024ull << 20;   // 1024 x 1 MiB extents
  o.large_reserve_bytes = 0;  // preserve all 1024 arena extents for 32 shards
  o.slot_granularity = 4096;
  o.flush_threads = 8;
  RamTier t(o, sink.fn());
  ASSERT_TRUE(t.ok());
  EXPECT_EQ(t.shards(), 32u) << "32 shards must not be clamped to the old 16 cap";
  std::string v(8000, 'y');
  for (int i = 0; i < 256; ++i) ASSERT_TRUE(t.Put(K(5000 + i), v.data(), v.size())) << i;
  for (int i = 0; i < 256; ++i) {
    RamTier::Hit h;
    ASSERT_TRUE(t.GetPrep(K(5000 + i), 0, 0, &h)) << i;  // token from a high shard
    EXPECT_EQ(h.len, v.size());
    h = RamTier::Hit{};
  }
  ::unsetenv("DFKV_RAM_TIER_SHARDS");
  ::unsetenv("DFKV_RAM_TIER_EXTENT_BYTES");
}

// PutDurable: read-promotion entry. The slot is born durable — never enqueued
// for flush (zero DIO write cost) and immediately evictable/removable (no
// flush-pin). Dedup semantics match Put.
TEST(RamTier, PutDurableNoFlushIOAndImmediatelyEvictable) {
  FlushSink sink;
  sink.close();  // any enqueued flush would block forever -> test would hang
  RamTier rt(Opts(64 * 4096), sink.fn());
  ASSERT_TRUE(rt.ok());
  std::string v = "promoted-cold-page";
  ASSERT_TRUE(rt.PutDurable(K(90), v.data(), v.size()));
  EXPECT_EQ(rt.Promoted(), 1u);
  EXPECT_EQ(rt.FlushBacklog(), 0u);  // never touched the flush queue
  {
    std::lock_guard<std::mutex> lk(sink.m);
    EXPECT_EQ(sink.calls, 0);        // zero flush IO
  }
  RamTier::Hit h;
  ASSERT_TRUE(rt.GetPrep(K(90), 0, v.size(), &h));
  EXPECT_EQ(std::string(h.ptr, h.len), v);
  h = RamTier::Hit{};
  // No flush-pin held: Remove (which declines on any pin) succeeds at once.
  EXPECT_TRUE(rt.Remove(K(90)));
  EXPECT_FALSE(rt.Contains(K(90)));
}

TEST(RamTier, DirectDurableReservationPublishesAndPinsUntilRelease) {
  FlushSink sink;
  sink.close();
  RamTier rt(Opts(64 * 4096), sink.fn());
  ASSERT_TRUE(rt.ok());
  const std::string value(8192, 'r');

  RamTier::DurableReservation reservation;
  ASSERT_TRUE(rt.ReserveDurable(K(190), value.size(), &reservation));
  ASSERT_TRUE(reservation);
  ASSERT_GE(reservation.capacity(), value.size());
  EXPECT_FALSE(rt.Contains(K(190)));  // hidden while storage fills the slot
  EXPECT_EQ(rt.UsedBytes(), reservation.capacity());
  std::memcpy(reservation.data(), value.data(), value.size());

  ASSERT_TRUE(reservation.Commit());
  EXPECT_TRUE(rt.Contains(K(190)));
  EXPECT_EQ(rt.Promoted(), 1u);
  EXPECT_EQ(rt.FlushBacklog(), 0u);
  {
    std::lock_guard<std::mutex> lk(sink.m);
    EXPECT_EQ(sink.calls, 0);  // already durable: no write-back
  }

  RamTier::Hit hit;
  ASSERT_TRUE(rt.GetPrep(K(190), 0, value.size(), &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), value);
  hit = RamTier::Hit{};

  // Commit retains the reservation pin for an in-flight RDMA send. Remove
  // hides the key immediately, but physical capacity is released only when
  // the send owner drops the reservation.
  EXPECT_TRUE(rt.Remove(K(190)));
  EXPECT_FALSE(rt.Contains(K(190)));
  EXPECT_NE(rt.UsedBytes(), 0u);
  reservation = RamTier::DurableReservation{};
  EXPECT_EQ(rt.UsedBytes(), 0u);
}

TEST(RamTier, AbandonedDirectDurableReservationReleasesHiddenSlot) {
  FlushSink sink;
  RamTier rt(Opts(64 * 4096), sink.fn());
  ASSERT_TRUE(rt.ok());
  {
    RamTier::DurableReservation reservation;
    ASSERT_TRUE(rt.ReserveDurable(K(191), 4096, &reservation));
    std::memset(reservation.data(), 'x', 4096);
    EXPECT_FALSE(rt.Contains(K(191)));
    EXPECT_NE(rt.UsedBytes(), 0u);
  }
  EXPECT_FALSE(rt.Contains(K(191)));
  EXPECT_EQ(rt.UsedBytes(), 0u);
}

TEST(RamTier, PutDurableDedupsAgainstResident) {
  FlushSink sink;
  RamTier rt(Opts(64 * 4096), sink.fn());
  std::string v(500, 'p');
  ASSERT_TRUE(rt.Put(K(91), v.data(), v.size()));
  const uint64_t used = rt.UsedBytes();
  EXPECT_TRUE(rt.PutDurable(K(91), v.data(), v.size()));  // resident -> no-op true
  EXPECT_EQ(rt.UsedBytes(), used);                        // no second slot
  EXPECT_EQ(rt.Promoted(), 0u);                           // dedup is not a promotion
}

TEST(RamTier, RetireDurableForPutPreservesPinnedBytes) {
  for (const bool pinned : {false, true}) {
    SCOPED_TRACE(pinned);
    FlushSink sink;
    RamTier::Options options = Opts(16 * 4096);
    options.reclaim_interval_ms = 0;
    RamTier rt(options, sink.fn());
    const std::string value(4096, 'o');
    rt.RetireDurableForPut(K(200));  // Absent keys require no cleanup.
    ASSERT_TRUE(rt.PutDurable(K(200), value.data(), value.size()));
    RamTier::Hit hit;
    if (pinned) {
      ASSERT_TRUE(rt.GetPrep(K(200), 0, 0, &hit));
    }
    const uint64_t used = rt.UsedBytes();
    rt.RetireDurableForPut(K(200));
    rt.RetireDurableForPut(K(200));  // Retirement is idempotent.
    EXPECT_FALSE(rt.Contains(K(200)));
    EXPECT_EQ(rt.UsedBytes(), pinned ? used : 0u);
    if (pinned) {
      const RamTier::PutResult result =
          rt.PutCommitted(K(200), "replacement", 11);
      EXPECT_EQ(result.disposition, RamTier::PutDisposition::kBypass);
      EXPECT_EQ(result.status, Status::kCacheFull);
      EXPECT_EQ(std::string(hit.ptr, hit.len), value);
    }
    hit = RamTier::Hit{};
    EXPECT_EQ(rt.UsedBytes(), 0u);
  }
}

TEST(RamTier, RetireDurableForPutCancelsHiddenPromotionWithoutJoiningIt) {
  FlushSink sink;
  RamTier rt(Opts(16 * 4096), sink.fn());
  const std::string value(4096, 'p');
  RamTier::DurableReservation reservation;
  ASSERT_TRUE(rt.ReserveDurable(K(201), value.size(), &reservation));
  std::memcpy(reservation.data(), value.data(), value.size());
  const uint64_t used = rt.UsedBytes();
  rt.RetireDurableForPut(K(201));
  RamTierTestPeer::Completion completion;
  EXPECT_EQ(RamTierTestPeer::Admit(rt, K(201), "replacement", &completion),
            RamTierTestPeer::Admission::kBypass);
  EXPECT_EQ(completion, nullptr);
  EXPECT_FALSE(reservation.Commit());
  EXPECT_FALSE(rt.Contains(K(201)));
  EXPECT_EQ(rt.UsedBytes(), used);
  EXPECT_EQ(std::string(reservation.data(), value.size()), value);
  reservation = RamTier::DurableReservation{};
  EXPECT_EQ(rt.UsedBytes(), 0u);
}

TEST(RamTier, RetireDurableForPutKeepsDirtyFirstOwnerAndCompletion) {
  FlushSink sink;
  sink.close();
  RamTier rt(Opts(16 * 4096), sink.fn());
  const OpenFlushOnExit cleanup{sink};
  const std::string value(4096, 'd');
  RamTierTestPeer::Completion first, duplicate;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(202), value, &first),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(sink.await_calls(1));
  rt.RetireDurableForPut(K(202));
  EXPECT_EQ(RamTierTestPeer::Admit(rt, K(202), "different", &duplicate),
            RamTierTestPeer::Admission::kDuplicate);
  EXPECT_EQ(duplicate, first);
  EXPECT_FALSE(RamTierTestPeer::Done(first));
  RamTier::Hit hit;
  ASSERT_TRUE(rt.GetPrep(K(202), 0, 0, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), value);
  hit = RamTier::Hit{};
  sink.open();
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(first));
  EXPECT_EQ(RamTierTestPeer::Wait(first), Status::kOk);
  EXPECT_EQ(RamTierTestPeer::Wait(duplicate), Status::kOk);
}

TEST(RamTier, RetiredCopyCannotBypassPositiveDirtyWatermark) {
  for (const bool promotion : {false, true}) {
    SCOPED_TRACE(promotion);
    FlushSink sink;
    sink.close();
    RamTier::Options options = Opts(16 * 4096);
    options.ack_high_watermark_pct = 1;
    options.reclaim_interval_ms = 0;
    RamTier rt(options, sink.fn());
    const OpenFlushOnExit cleanup{sink};
    const std::string value(4096, 'w');
    RamTier::DurableReservation reservation;
    RamTier::Hit pinned;
    if (promotion) {
      ASSERT_TRUE(rt.ReserveDurable(K(203), value.size(), &reservation));
    } else {
      ASSERT_TRUE(rt.PutDurable(K(203), value.data(), value.size()));
      ASSERT_TRUE(rt.GetPrep(K(203), 0, 0, &pinned));
    }
    ASSERT_EQ(rt.PutWriteBack(K(204), value.data(), value.size()).status,
              Status::kOk);
    ASSERT_TRUE(sink.await_calls(1));
    rt.RetireDurableForPut(K(203));
    const RamTier::PutResult result =
        rt.PutWriteBack(K(203), value.data(), value.size());
    EXPECT_EQ(result.status, Status::kCacheFull);
    EXPECT_EQ(result.disposition, RamTier::PutDisposition::kBackpressure);
    EXPECT_EQ(rt.PutBypass(), 0u);
    EXPECT_EQ(rt.AckBackpressure(), 1u);
    EXPECT_FALSE(rt.Contains(K(203)));
    if (promotion) {
      EXPECT_FALSE(reservation.Commit());
    } else {
      EXPECT_EQ(std::string(pinned.ptr, pinned.len), value);
    }
    sink.open();
    ASSERT_TRUE(rt.WaitForDrain(std::chrono::seconds(2)));
  }
}

TEST(RamTier, RemovedPinnedDirtyOwnerRejectsRatherThanBypassing) {
  FlushSink sink;
  sink.close();
  RamTier::Options options = Opts(8 * 4096);
  options.large_reserve_bytes = 0;
  options.reclaim_interval_ms = 0;
  RamTier rt(options, sink.fn());
  const OpenFlushOnExit cleanup{sink};
  const std::string value(4096, 'd');
  ASSERT_TRUE(rt.Put(K(205), value.data(), value.size()));
  ASSERT_TRUE(sink.await_calls(1));  // Keep the only flusher off the next key.
  RamTierTestPeer::Completion completion;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(206), value, &completion),
            RamTierTestPeer::Admission::kAccepted);
  RamTier::Hit pinned;
  ASSERT_TRUE(rt.GetPrep(K(206), 0, 0, &pinned));
  ASSERT_TRUE(rt.Remove(K(206)));  // Queued cancellation completes immediately.
  EXPECT_EQ(RamTierTestPeer::Wait(completion), Status::kIOError);
  rt.RetireDurableForPut(K(206));
  const uint64_t used = rt.UsedBytes();
  const RamTier::PutResult disk_ack = rt.PutCommitted(K(206), "different", 9);
  const RamTier::PutResult ram_ack = rt.PutWriteBack(K(206), "different", 9);
  EXPECT_EQ(disk_ack.status, Status::kIOError);
  EXPECT_EQ(disk_ack.disposition, RamTier::PutDisposition::kComplete);
  EXPECT_EQ(ram_ack.status, Status::kIOError);
  EXPECT_EQ(ram_ack.disposition, RamTier::PutDisposition::kComplete);
  EXPECT_EQ(rt.PutBypass(), 0u);
  EXPECT_EQ(rt.UsedBytes(), used);
  EXPECT_EQ(std::string(pinned.ptr, pinned.len), value);
  pinned = RamTier::Hit{};
  EXPECT_LT(rt.UsedBytes(), used);
  EXPECT_FALSE(rt.Contains(K(206)));
  {
    const std::lock_guard<std::mutex> lock(sink.m);
    EXPECT_EQ(sink.calls, 1);  // The canceled value was never flushed.
  }
  sink.open();
  ASSERT_TRUE(rt.WaitForDrain(std::chrono::seconds(2)));
}

TEST(RamTier, PrepareDirectPutDoesNotAdmitOrProbeMissingKeys) {
  FlushSink sink;
  std::atomic<uint64_t> probes{0};
  RamTier::Options options = Opts(16 * 4096);
  options.reclaim_interval_ms = 0;
  options.is_persisted = [&probes](const BlockKey&) {
    probes.fetch_add(1, std::memory_order_relaxed);
    return true;
  };
  RamTier rt(options, sink.fn());
  ASSERT_TRUE(rt.ok());
  for (const uint64_t id : {207, 208, 207}) {
    rt.RetireDurableForPut(K(id));
    const RamTier::PutResult result = rt.PrepareDirectPut(K(id), "new", 3);
    EXPECT_EQ(result.status, Status::kCacheFull);
    EXPECT_EQ(result.disposition, RamTier::PutDisposition::kBypass);
    EXPECT_FALSE(rt.Contains(K(id)));
  }
  EXPECT_EQ(probes.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(rt.UsedBytes(), 0u);
  EXPECT_EQ(rt.FlushBacklog(), 0u);
  EXPECT_EQ(rt.PutBypass(), 0u);  // Direct routing is not capacity rejection.
  const std::lock_guard<std::mutex> lock(sink.m);
  EXPECT_EQ(sink.calls, 0);
}

TEST(RamTier, PrepareDirectPutBypassesCanceledPromotionWithoutReusingItsPin) {
  FlushSink sink;
  std::atomic<uint64_t> probes{0};
  RamTier::Options options = Opts(16 * 4096);
  options.reclaim_interval_ms = 0;
  options.is_persisted = [&probes](const BlockKey&) {
    probes.fetch_add(1, std::memory_order_relaxed);
    return true;
  };
  RamTier rt(options, sink.fn());
  RamTier::DurableReservation reservation;
  ASSERT_TRUE(rt.ReserveDurable(K(209), 4096, &reservation));
  std::memset(reservation.data(), 'o', 4096);
  const uint64_t used = rt.UsedBytes();
  rt.RetireDurableForPut(K(209));
  const RamTier::PutResult result = rt.PrepareDirectPut(K(209), "new", 3);
  EXPECT_EQ(result.status, Status::kCacheFull);
  EXPECT_EQ(result.disposition, RamTier::PutDisposition::kBypass);
  EXPECT_EQ(probes.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(rt.PutBypass(), 0u);
  EXPECT_EQ(rt.FlushBacklog(), 0u);
  EXPECT_EQ(rt.UsedBytes(), used);
  EXPECT_FALSE(reservation.Commit());
  EXPECT_FALSE(rt.Contains(K(209)));
  EXPECT_EQ(std::string(reservation.data(), 4096), std::string(4096, 'o'));
  reservation = RamTier::DurableReservation{};
  EXPECT_EQ(rt.UsedBytes(), 0u);
  const std::lock_guard<std::mutex> lock(sink.m);
  EXPECT_EQ(sink.calls, 0);
}

TEST(RamTier, BackingProbeDoesNotBlockRamReadsSendReleaseOrFlushCompletion) {
  FlushSink sink;
  sink.close();
  std::promise<void> entered, release, observed;
  const std::future<void> probe_entered = entered.get_future();
  const std::shared_future<void> probe_release = release.get_future().share();
  const std::future<void> observation_done = observed.get_future();
  RamTier::Options options = Opts(16 * 4096);
  options.reclaim_interval_ms = 0;
  options.is_persisted = [&](const BlockKey& key) {
    if (key == K(211)) {
      entered.set_value();
      probe_release.wait();
    }
    return false;
  };
  RamTier rt(options, sink.fn());
  const OpenFlushOnExit cleanup{sink};
  RamTierTestPeer::Completion dirty;
  ASSERT_EQ(RamTierTestPeer::Admit(rt, K(210), "old", &dirty),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(sink.await_calls(1));
  RamTier::Hit old_send;
  ASSERT_TRUE(rt.GetPrep(K(210), 0, 0, &old_send));
  Status status = Status::kInvalid;
  std::thread writer(
      [&] { status = rt.PutCommitted(K(211), "new", 3).status; });
  EXPECT_EQ(probe_entered.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  std::thread observer([&] {
    EXPECT_TRUE(rt.Contains(K(210)));
    size_t length = 0;
    EXPECT_TRUE(rt.Lookup(K(210), &length));
    EXPECT_EQ(length, 3u);
    RamTier::Hit hit;
    EXPECT_TRUE(rt.GetPrep(K(210), 0, 0, &hit));
    if (hit.ptr != nullptr) {
      EXPECT_EQ(std::string(hit.ptr, hit.len), "old");
    }
    hit = RamTier::Hit{};
    old_send = RamTier::Hit{};
    sink.open();
    EXPECT_TRUE(RamTierTestPeer::AwaitDone(dirty));
    observed.set_value();
  });
  // A failed implementation must still be released before joining threads.
  EXPECT_EQ(observation_done.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  release.set_value();
  observer.join();
  writer.join();
  EXPECT_EQ(status, Status::kOk);
  EXPECT_EQ(RamTierTestPeer::Wait(dirty), Status::kOk);
}

TEST(RamTier, BackingProbeKeepsFirstPutOwnershipAcrossUnlockedDiskCheck) {
  FlushSink sink;
  std::promise<void> entered, release, duplicate_started, duplicate_done;
  const std::future<void> probe_entered = entered.get_future();
  const std::shared_future<void> probe_release = release.get_future().share();
  const std::future<void> started = duplicate_started.get_future();
  const std::future<void> finished = duplicate_done.get_future();
  std::atomic<uint64_t> probes{0};
  RamTier::Options options = Opts(16 * 4096);
  options.reclaim_interval_ms = 0;
  options.is_persisted = [&](const BlockKey&) {
    if (probes.fetch_add(1, std::memory_order_relaxed) == 0) {
      entered.set_value();
      probe_release.wait();
    }
    return false;
  };
  RamTier rt(options, sink.fn());
  Status first = Status::kInvalid, second = Status::kInvalid;
  std::thread leader(
      [&] { first = rt.PutCommitted(K(212), "first", 5).status; });
  EXPECT_EQ(probe_entered.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  std::thread duplicate([&] {
    duplicate_started.set_value();
    second = rt.PutCommitted(K(212), "second", 6).status;
    duplicate_done.set_value();
  });
  EXPECT_EQ(started.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(finished.wait_for(std::chrono::seconds(2)),
            std::future_status::timeout);
  {
    const std::lock_guard<std::mutex> lock(sink.m);
    EXPECT_EQ(sink.calls, 0);
  }
  release.set_value();
  leader.join();
  duplicate.join();
  EXPECT_EQ(first, Status::kOk);
  EXPECT_EQ(second, Status::kOk);
  EXPECT_EQ(probes.load(std::memory_order_relaxed), 1u);
  const std::lock_guard<std::mutex> lock(sink.m);
  EXPECT_EQ(sink.calls, 1);
  EXPECT_EQ(sink.values.at(K(212).Filename()), "first");
}

TEST(RamTier, BackingProbeDeclinesCompetingPromotionWithoutWaitingForDisk) {
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    FlushSink sink;
    std::promise<void> entered, release, promotion_done;
    const std::future<void> probe_entered = entered.get_future();
    const std::shared_future<void> probe_release = release.get_future().share();
    const std::future<void> finished = promotion_done.get_future();
    RamTier::Options options = Opts(16 * 4096);
    options.reclaim_interval_ms = 0;
    // Leave a 16 KiB arena and a legal dedicated budget for the large-copy
    // path. Contention, not an invalid size/budget, must decline promotion.
    options.large_reserve_bytes = 48 * 1024;
    const std::string promoted_value(mode == 2 ? 20 * 1024 : 3, 'o');
    options.is_persisted = [&](const BlockKey&) {
      entered.set_value();
      probe_release.wait();
      return false;
    };
    RamTier rt(options, sink.fn());
    Status status = Status::kInvalid;
    std::thread writer(
        [&] { status = rt.PutCommitted(K(213), "first", 5).status; });
    EXPECT_EQ(probe_entered.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    bool promoted = true;
    std::thread promoter([&] {
      if (mode == 1) {
        RamTier::DurableReservation reservation;
        promoted = rt.ReserveDurable(K(213), 4096, &reservation);
      } else {
        promoted =
            rt.PutDurable(K(213), promoted_value.data(), promoted_value.size());
      }
      promotion_done.set_value();
    });
    EXPECT_EQ(finished.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    release.set_value();
    promoter.join();
    writer.join();
    EXPECT_FALSE(promoted);
    EXPECT_EQ(status, Status::kOk);
    RamTier::Hit hit;
    ASSERT_TRUE(rt.GetPrep(K(213), 0, 0, &hit));
    EXPECT_EQ(std::string(hit.ptr, hit.len), "first");
    if (mode == 2) {
      EXPECT_TRUE(
          rt.PutDurable(K(217), promoted_value.data(), promoted_value.size()));
    }
  }
}

TEST(RamTier, BackingProbePreservesPersistedSuccessAndPreAdmissionWatermark) {
  FlushSink sink;
  sink.close();
  RamTier::Options options = Opts(16 * 4096);
  options.reclaim_interval_ms = 0;
  options.ack_high_watermark_pct = 6;
  options.is_persisted = [](const BlockKey& key) { return key == K(215); };
  RamTier rt(options, sink.fn());
  const OpenFlushOnExit cleanup{sink};
  const std::string value(4096, 'd');
  ASSERT_EQ(rt.PutWriteBack(K(214), value.data(), value.size()).status,
            Status::kOk);
  ASSERT_TRUE(sink.await_calls(1));
  ASSERT_EQ(rt.DirtyBytes(), 4096u);  // Above the positive dirty watermark.
  const uint64_t used = rt.UsedBytes();
  const RamTier::PutResult persisted = rt.PutWriteBack(K(215), "known", 5);
  EXPECT_EQ(persisted.status, Status::kOk);
  EXPECT_FALSE(rt.Contains(K(215)));
  const RamTier::PutResult rejected = rt.PutWriteBack(K(216), "new", 3);
  EXPECT_EQ(rejected.status, Status::kCacheFull);
  EXPECT_EQ(rejected.disposition, RamTier::PutDisposition::kBackpressure);
  EXPECT_FALSE(rt.Contains(K(216)));
  // A rejected request must not leave a placeholder that a later direct PUT
  // mistakes for an accepted value or an unfinished completion.
  const RamTier::PutResult direct = rt.PrepareDirectPut(K(216), "other", 5);
  EXPECT_EQ(direct.disposition, RamTier::PutDisposition::kBypass);
  EXPECT_EQ(rt.UsedBytes(), used);
  EXPECT_EQ(rt.FlushBacklog(), 1u);
  EXPECT_EQ(rt.PutBypass(), 0u);
  EXPECT_EQ(rt.RamAcks(), 1u);
  EXPECT_EQ(rt.AckBackpressure(), 1u);
  const std::lock_guard<std::mutex> lock(sink.m);
  EXPECT_EQ(sink.calls, 1);
}

// Typed callbacks are opt-in. The production server still uses the bool sink.
TEST(RamTier, TypedFlushPreservesOwnerResultAndConflictingDuplicate) {
  FlushSink gate;
  gate.close();
  const dfkv::DiskResidentIdentity identity{17, 23, 41};
  RamTier tier(Opts(64 * 4096), nullptr);
  const OpenFlushOnExit release{gate};
  tier.set_flush_results(
      [&](const BlockKey& key, char* data, size_t len, size_t cap) {
        EXPECT_TRUE(gate.fn()(key, data, len, cap));
        return dfkv::DiskWriteResult{Status::kOk, dfkv::DiskWriteReason::kNone,
                                     identity};
      });
  RamTierTestPeer::Completion owner;
  RamTierTestPeer::Completion follower;
  ASSERT_EQ(RamTierTestPeer::Admit(tier, K(80000), "owner", &owner),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(gate.await_calls(1));
  ASSERT_EQ(RamTierTestPeer::Admit(tier, K(80000), "different", &follower),
            RamTierTestPeer::Admission::kDuplicate);
  EXPECT_EQ(owner, follower);
  gate.open();
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(owner));
  const RamTier::PutResult result = RamTierTestPeer::Result(follower);
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.reason, dfkv::DiskWriteReason::kNone);
  EXPECT_EQ(result.identity, identity);
  EXPECT_EQ(tier.PutCommitted(K(80000), "third", 5).identity, identity);
  EXPECT_EQ(tier.PrepareDirectPut(K(80000), "third", 5).identity, identity);
  EXPECT_EQ(tier.PutWriteBack(K(80000), "third", 5).identity, identity);
  RamTier::Hit hit;
  ASSERT_TRUE(tier.GetPrep(K(80000), 0, 5, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), "owner");
  EXPECT_EQ(gate.values.at(K(80000).Filename()), "owner");
}

TEST(RamTier, TypedFlushFailureReasonsControlRetryAndHealth) {
  for (const bool acknowledged : {false, true}) {
    for (const dfkv::DiskWriteResult failure :
         {dfkv::DiskWriteResult{Status::kCacheFull,
                                dfkv::DiskWriteReason::kIdentityExhausted},
          dfkv::DiskWriteResult{Status::kCacheFull},
          dfkv::DiskWriteResult{Status::kIOError},
          dfkv::DiskWriteResult{Status::kIOError,
                                dfkv::DiskWriteReason::kIdentityExhausted}}) {
      SCOPED_TRACE(acknowledged);
      SCOPED_TRACE(static_cast<int>(failure.reason));
      SCOPED_TRACE(static_cast<int>(failure.status));
      std::atomic<unsigned> calls{0};
      RamTier::Options options = Opts(64 * 4096);
      options.flush_retries = 3;
      RamTier tier(options, nullptr);
      tier.set_flush_results([&](const BlockKey&, char*, size_t, size_t) {
        ++calls;
        return failure;
      });
      RamTierTestPeer::Completion completion;
      ASSERT_EQ(RamTierTestPeer::Admit(tier, K(80001), "value", &completion,
                                       acknowledged),
                RamTierTestPeer::Admission::kAccepted);
      ASSERT_TRUE(RamTierTestPeer::AwaitDone(completion));
      const RamTier::PutResult result = RamTierTestPeer::Result(completion);
      EXPECT_EQ(result.status, failure.status);
      EXPECT_EQ(result.reason, failure.reason);
      EXPECT_FALSE(result.identity.valid());
      EXPECT_EQ(result.disposition, RamTier::PutDisposition::kComplete);
      const bool exhausted =
          failure.status == Status::kCacheFull &&
          failure.reason == dfkv::DiskWriteReason::kIdentityExhausted;
      EXPECT_EQ(calls.load(), exhausted ? 1U : 3U);
      EXPECT_EQ(tier.healthy(), exhausted && !acknowledged);
      EXPECT_EQ(tier.PostAckFlushFailures(), acknowledged ? 1U : 0U);
      EXPECT_EQ(tier.FlushDropped(), 1U);
      EXPECT_EQ(tier.DirtyObjects(), 0U);
      EXPECT_FALSE(tier.Contains(K(80001)));
    }
  }
}

TEST(RamTier, TypedLateSuccessCannotOverrideRemoveOrReinsert) {
  FlushSink gate;
  gate.close();
  std::atomic<uint64_t> generation{0};
  RamTier tier(Opts(64 * 4096), nullptr);
  const OpenFlushOnExit release{gate};
  tier.set_flush_results(
      [&](const BlockKey& key, char* data, size_t len, size_t cap) {
        gate.fn()(key, data, len, cap);
        return dfkv::DiskWriteResult{
            Status::kOk, dfkv::DiskWriteReason::kNone, {17, 23, ++generation}};
      });
  RamTierTestPeer::Completion completion;
  ASSERT_EQ(RamTierTestPeer::Admit(tier, K(80002), "old", &completion),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(gate.await_calls(1));
  std::thread remover([&] { EXPECT_TRUE(tier.Remove(K(80002))); });
  const bool hidden = WaitFor([&] { return !tier.Contains(K(80002)); });
  gate.open();
  remover.join();
  ASSERT_TRUE(hidden);
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(completion));
  const RamTier::PutResult removed = RamTierTestPeer::Result(completion);
  EXPECT_EQ(removed.status, Status::kIOError);
  EXPECT_EQ(removed.reason, dfkv::DiskWriteReason::kNone);
  EXPECT_FALSE(removed.identity.valid());
  const RamTier::PutResult fresh = tier.PutCommitted(K(80002), "new", 3);
  EXPECT_EQ(fresh.status, Status::kOk);
  EXPECT_EQ(fresh.identity.generation, 2U);
  EXPECT_TRUE(tier.healthy());
}

TEST(RamTier, TypedBatchKeepsRowsAndDefaultsMissingResultsToIoError) {
  FlushSink gate;
  gate.close();
  RamTier::Options options = Opts(64 * 4096);
  options.flush_retries = 1;
  std::atomic<unsigned> scalar_calls{0};
  std::vector<size_t> sizes;
  RamTier tier(options, nullptr);
  const OpenFlushOnExit release{gate};
  tier.set_flush_results(
      [&](const BlockKey&, char*, size_t, size_t) {
        ++scalar_calls;
        return dfkv::DiskWriteResult{Status::kIOError};
      },
      [&](const std::vector<RamTier::FlushItem>& items) {
        sizes.push_back(items.size());
        gate.fn()(items[0].key, items[0].data, items[0].len, items[0].cap);
        std::vector<dfkv::DiskWriteResult> results;
        results.reserve(items.size());
        for (const RamTier::FlushItem& item : items) {
          if (item.key == K(80006)) break;  // Last row deliberately omitted.
          if (item.key == K(80005))
            results.push_back({Status::kCacheFull,
                               dfkv::DiskWriteReason::kIdentityExhausted});
          else
            results.push_back({Status::kOk,
                               dfkv::DiskWriteReason::kNone,
                               {17, 23, item.key.digest_hi}});
        }
        return results;
      });
  RamTierTestPeer::Completion seed;
  ASSERT_EQ(RamTierTestPeer::Admit(tier, K(80003), "seed", &seed),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(gate.await_calls(1));
  std::vector<RamTierTestPeer::Completion> completions(3);
  for (size_t i = 0; i < completions.size(); ++i)
    ASSERT_EQ(
        RamTierTestPeer::Admit(tier, K(80004 + i), "value", &completions[i]),
        RamTierTestPeer::Admission::kAccepted);
  gate.open();
  for (const RamTierTestPeer::Completion& completion : completions)
    ASSERT_TRUE(RamTierTestPeer::AwaitDone(completion));
  const RamTier::PutResult success = RamTierTestPeer::Result(completions[0]);
  EXPECT_EQ(success.status, Status::kOk);
  EXPECT_EQ(success.reason, dfkv::DiskWriteReason::kNone);
  EXPECT_EQ(success.identity, (dfkv::DiskResidentIdentity{17, 23, 80004}));
  const RamTier::PutResult exhausted = RamTierTestPeer::Result(completions[1]);
  EXPECT_EQ(exhausted.status, Status::kCacheFull);
  EXPECT_EQ(exhausted.reason, dfkv::DiskWriteReason::kIdentityExhausted);
  EXPECT_FALSE(exhausted.identity.valid());
  const RamTier::PutResult missing = RamTierTestPeer::Result(completions[2]);
  EXPECT_EQ(missing.status, Status::kIOError);
  EXPECT_EQ(missing.reason, dfkv::DiskWriteReason::kNone);
  EXPECT_FALSE(missing.identity.valid());
  EXPECT_FALSE(tier.healthy());
  RamTier::Hit hit;
  ASSERT_TRUE(tier.GetPrep(K(80004), 0, 5, &hit));
  EXPECT_EQ(std::string(hit.ptr, hit.len), "value");
  EXPECT_EQ(scalar_calls.load(), 0U);
  EXPECT_EQ(sizes, (std::vector<size_t>{1, 3}));
}

TEST(RamTier, TypedExhaustionCompletesAttachedFollowersWithoutRetry) {
  FlushSink gate;
  gate.close();
  RamTier tier(Opts(64 * 4096), nullptr);
  const OpenFlushOnExit release{gate};
  tier.set_flush_results(
      [&](const BlockKey& key, char* data, size_t len, size_t cap) {
        gate.fn()(key, data, len, cap);
        return dfkv::DiskWriteResult{Status::kCacheFull,
                                     dfkv::DiskWriteReason::kIdentityExhausted};
      });
  RamTierTestPeer::Completion owner;
  RamTierTestPeer::Completion follower;
  ASSERT_EQ(RamTierTestPeer::Admit(tier, K(80007), "owner", &owner),
            RamTierTestPeer::Admission::kAccepted);
  ASSERT_TRUE(gate.await_calls(1));
  ASSERT_EQ(RamTierTestPeer::Admit(tier, K(80007), "different", &follower),
            RamTierTestPeer::Admission::kDuplicate);
  ASSERT_EQ(owner, follower);
  gate.open();
  ASSERT_TRUE(RamTierTestPeer::AwaitDone(owner));
  for (const RamTierTestPeer::Completion& completion : {owner, follower}) {
    const RamTier::PutResult result = RamTierTestPeer::Result(completion);
    EXPECT_EQ(result.status, Status::kCacheFull);
    EXPECT_EQ(result.reason, dfkv::DiskWriteReason::kIdentityExhausted);
    EXPECT_FALSE(result.identity.valid());
    EXPECT_EQ(result.disposition, RamTier::PutDisposition::kComplete);
  }
  EXPECT_EQ(gate.calls, 1);
  EXPECT_TRUE(tier.healthy());
}

TEST(RamTier, TypedRetryPublishesFinalSuccessAndIgnoresExtraBatchRows) {
  RamTier::Options options = Opts(64 * 4096);
  options.flush_retries = 3;
  std::atomic<unsigned> calls{0};
  RamTier tier(options, [](const BlockKey&, char*, size_t, size_t) {
    ADD_FAILURE() << "typed callback must override bool callbacks";
    return false;
  });
  tier.set_flush_batch([](const std::vector<RamTier::FlushItem>&) {
    ADD_FAILURE() << "typed callback must override bool batch";
    return std::vector<bool>{false};
  });
  tier.set_flush_results({}, [&](const std::vector<RamTier::FlushItem>& items) {
    EXPECT_EQ(items.size(), 1U);
    if (++calls == 1)
      return std::vector<dfkv::DiskWriteResult>{{Status::kIOError}};
    return std::vector<dfkv::DiskWriteResult>{
        {Status::kOk},
        {Status::kCacheFull, dfkv::DiskWriteReason::kIdentityExhausted}};
  });
  const RamTier::PutResult result = tier.PutCommitted(K(80008), "owner", 5);
  EXPECT_EQ(result.status, Status::kOk);
  EXPECT_EQ(result.reason, dfkv::DiskWriteReason::kNone);
  EXPECT_FALSE(
      result.identity.valid());  // An empty credential does not prove NEW.
  EXPECT_EQ(calls.load(), 2U);
  EXPECT_TRUE(WaitFor([&] { return tier.Flushed() == 1U; }));
  EXPECT_EQ(tier.FlushDropped(), 0U);
  EXPECT_TRUE(tier.healthy());
}
