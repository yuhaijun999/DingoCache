#ifndef DFKV_TEST_CACHE_READ_COALESCER_TEST_PEER_H_
#define DFKV_TEST_CACHE_READ_COALESCER_TEST_PEER_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "cache/read_coalescer.h"

namespace dfkv {
class ReadCoalescerTestPeer {
 public:
  static bool WaitForPublishedWaiter(ReadCoalescer& coalescer) {
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
      {
        const std::lock_guard<std::mutex> lock(coalescer.mu_);
        for (const std::pair<const ReadCoalescer::Key,
                             std::shared_ptr<ReadCoalescer::Flight>>& entry :
             coalescer.map_) {
          if (entry.second->publishes_to_ram &&
              entry.second->waiters.load(std::memory_order_acquire) > 0)
            return true;
        }
      }
      std::this_thread::yield();
    }
    return false;
  }
  static bool WaitForCopyWaiter(ReadCoalescer& coalescer, int minimum = 1) {
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
      {
        const std::lock_guard<std::mutex> lock(coalescer.mu_);
        for (const std::pair<const ReadCoalescer::Key,
                             std::shared_ptr<ReadCoalescer::Flight>>& entry :
             coalescer.map_) {
          if (entry.second->copy_waiters.load(std::memory_order_acquire) >=
              minimum)
            return true;
        }
      }
      std::this_thread::yield();
    }
    return false;
  }
};
}  // namespace dfkv

#endif  // DFKV_TEST_CACHE_READ_COALESCER_TEST_PEER_H_
