#include "cache/rdma_pull_request_accounting.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <string>

#include "common/status.h"

namespace dfkv {
namespace {

using Path = RdmaPullRequestCounters::Path;
using Result = RdmaPullRequestCounters::Result;

Result Classify(Status status, size_t bytes, bool admission_refused) {
  switch (status) {
    case Status::kOk:
      return bytes == 0 ? Result::kSuccessEmpty : Result::kSuccessNonempty;
    case Status::kNotFound:
      return Result::kNotFound;
    case Status::kInvalid:
      return Result::kInvalid;
    case Status::kCacheFull:
      return Result::kCacheFull;
    case Status::kIOError:
      return admission_refused ? Result::kAdmissionRefused : Result::kIOError;
    default:
      return Result::kOther;
  }
}

}  // namespace

RdmaPullRequestCounters::Snapshot RdmaPullRequestCounters::Read() const {
  Snapshot snapshot;
  for (size_t path = 0; path < kPathCount; ++path) {
    snapshot.dispatched[path] =
        dispatched_[path].load(std::memory_order_relaxed);
    snapshot.decided_unposted[path] =
        decided_unposted_[path].load(std::memory_order_relaxed);
    for (size_t result = 0; result < kResultCount; ++result) {
      snapshot.terminal[path][result] =
          terminal_[path][result].load(std::memory_order_relaxed);
    }
  }
  return snapshot;
}

std::string RdmaPullRequestCounters::MetricsText() const {
  constexpr std::array<const char*, kPathCount> kPaths{"sync", "uring"};
  constexpr std::array<const char*, kResultCount> kResults{
      "success_nonempty",  "success_empty", "not_found",
      "invalid",           "cache_full",    "io_error",
      "admission_refused", "cancelled",     "other"};
  const Snapshot snapshot = Read();
  std::string text =
      "# HELP dfkv_rdma_pull_requests_dispatched_total Decoded PULL requests "
      "dispatched into Serve\n"
      "# TYPE dfkv_rdma_pull_requests_dispatched_total counter\n"
      "# HELP dfkv_rdma_pull_requests_terminal_total Final logical decisions "
      "of dispatched PULL requests\n"
      "# TYPE dfkv_rdma_pull_requests_terminal_total counter\n"
      "# HELP dfkv_rdma_pull_decided_responses_unposted_total Decided PULL "
      "responses not posted before connection teardown\n"
      "# TYPE dfkv_rdma_pull_decided_responses_unposted_total counter\n";
  for (size_t path = 0; path < kPathCount; ++path) {
    const std::string labels = std::string("{path=\"") + kPaths[path];
    text += "dfkv_rdma_pull_requests_dispatched_total" + labels + "\"} " +
            std::to_string(snapshot.dispatched[path]) + "\n";
    text += "dfkv_rdma_pull_decided_responses_unposted_total" + labels +
            "\"} " + std::to_string(snapshot.decided_unposted[path]) + "\n";
    for (size_t result = 0; result < kResultCount; ++result) {
      text += "dfkv_rdma_pull_requests_terminal_total" + labels +
              "\",result=\"" + kResults[result] + "\"} " +
              std::to_string(snapshot.terminal[path][result]) + "\n";
    }
  }
  return text;
}

RdmaPullRequestLedger::RdmaPullRequestLedger(RdmaPullRequestCounters& counters,
                                             size_t depth)
    : counters_(counters), slots_(depth) {}

RdmaPullRequestLedger::~RdmaPullRequestLedger() {
  for (const Slot& slot : slots_) {
    const size_t path = static_cast<size_t>(slot.path);
    if (slot.state == State::kPending) {
      counters_.terminal_[path][static_cast<size_t>(Result::kCancelled)]
          .fetch_add(1, std::memory_order_relaxed);
    } else if (slot.state == State::kDecided) {
      counters_.decided_unposted_[path].fetch_add(1, std::memory_order_relaxed);
    }
  }
}

bool RdmaPullRequestLedger::Begin(size_t slot, Path path) {
  const size_t path_index = static_cast<size_t>(path);
  if (slot >= slots_.size() ||
      path_index >= RdmaPullRequestCounters::kPathCount ||
      slots_[slot].state != State::kIdle) {
    return false;
  }
  slots_[slot].path = path;
  slots_[slot].state = State::kPending;
  counters_.dispatched_[path_index].fetch_add(1, std::memory_order_relaxed);
  return true;
}

bool RdmaPullRequestLedger::Finish(size_t slot, Status status, size_t bytes,
                                   bool admission_refused) {
  if (slot >= slots_.size() || slots_[slot].state != State::kPending) {
    return false;
  }
  Slot& state = slots_[slot];
  const size_t path = static_cast<size_t>(state.path);
  const size_t result =
      static_cast<size_t>(Classify(status, bytes, admission_refused));
  state.state = State::kDecided;
  counters_.terminal_[path][result].fetch_add(1, std::memory_order_relaxed);
  return true;
}

bool RdmaPullRequestLedger::ResponsePosted(size_t slot) {
  if (slot >= slots_.size() || slots_[slot].state != State::kDecided) {
    return false;
  }
  slots_[slot].state = State::kIdle;
  return true;
}

}  // namespace dfkv
