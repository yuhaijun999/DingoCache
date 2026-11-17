#ifndef DFKV_CACHE_RDMA_PULL_REQUEST_ACCOUNTING_H_
#define DFKV_CACHE_RDMA_PULL_REQUEST_ACCOUNTING_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace dfkv {

class RdmaPullRequestLedger;

// Server-wide observations of decoded PULL requests, independent of grant
// notifications, SEND completions and client READ/RELEASE completion.
class RdmaPullRequestCounters {
 public:
  enum class Path : unsigned char {
    kSync = 0,  // Synchronous Serve, including ring initialization fallback.
    kUring = 1  // Serve with a successfully initialized asynchronous ring.
  };
  enum class Result : unsigned char {
    kSuccessNonempty = 0,   // A nonempty grant owns its registered bytes.
    kSuccessEmpty = 1,      // An empty grant owns its release identity.
    kNotFound = 2,          // The final storage result is a miss.
    kInvalid = 3,           // Decoded request has invalid control or geometry.
    kCacheFull = 4,         // Slot or staging capacity refused the request.
    kIOError = 5,           // Final I/O, storage release or MR failure.
    kAdmissionRefused = 6,  // Exact admission refusal, carried as IOError.
    kCancelled = 7,         // Connection ended before a logical decision.
    kOther = 8              // Another existing final Status value.
  };
  static constexpr size_t kPathCount = 2;
  static constexpr size_t kResultCount = 9;
  struct Snapshot {
    std::array<uint64_t, kPathCount> dispatched{};
    std::array<std::array<uint64_t, kResultCount>, kPathCount> terminal{};
    std::array<uint64_t, kPathCount> decided_unposted{};
  };

  // Relaxed per-cell observations, not a transaction across counter fields.
  Snapshot Read() const;
  std::string MetricsText() const;

 private:
  friend class RdmaPullRequestLedger;
  std::array<std::atomic<uint64_t>, kPathCount> dispatched_{};
  std::array<std::array<std::atomic<uint64_t>, kResultCount>, kPathCount>
      terminal_{};
  std::array<std::atomic<uint64_t>, kPathCount> decided_unposted_{};
};

// One Serve thread owns this bounded SEND-slot ledger. The counter owner must
// outlive it. It owns no payload, pin, MR, QP or disk-ring cleanup authority.
// Destroy only after the existing asynchronous ring drain/queue cleanup order.
class RdmaPullRequestLedger {
 public:
  RdmaPullRequestLedger(RdmaPullRequestCounters& counters, size_t depth);
  ~RdmaPullRequestLedger();
  RdmaPullRequestLedger(const RdmaPullRequestLedger&) = delete;
  RdmaPullRequestLedger& operator=(const RdmaPullRequestLedger&) = delete;
  RdmaPullRequestLedger(RdmaPullRequestLedger&&) = delete;
  RdmaPullRequestLedger& operator=(RdmaPullRequestLedger&&) = delete;

  // Begin only after successful PULL decoding and SEND-slot acquisition.
  // Reject occupied/out-of-range slots without changing observations.
  bool Begin(size_t slot, RdmaPullRequestCounters::Path path);
  // Finish once at the final status or established grant, before READY/SEND.
  // A later transport failure must not undo this decision or add cancellation.
  bool Finish(size_t slot, Status status, size_t bytes = 0,
              bool admission_refused = false);
  // Successful PostSend closes only the separate response-posting annotation;
  // this is not proof of a SEND CQE, client receipt or payload READ completion.
  bool ResponsePosted(size_t slot);

 private:
  enum class State : unsigned char {
    kIdle = 0,     // No admitted request owns this slot.
    kPending = 1,  // Admitted, without a final logical decision.
    kDecided = 2   // Logical terminal recorded, response not yet posted.
  };
  struct Slot {
    RdmaPullRequestCounters::Path path = RdmaPullRequestCounters::Path::kSync;
    State state = State::kIdle;
  };

  RdmaPullRequestCounters& counters_;
  std::vector<Slot> slots_;
};

}  // namespace dfkv

#endif  // DFKV_CACHE_RDMA_PULL_REQUEST_ACCOUNTING_H_
