#ifndef DFKV_CACHE_RDMA_REQUEST_ORDER_H_
#define DFKV_CACHE_RDMA_REQUEST_ORDER_H_

#include "transport/wire.h"

namespace dfkv {

// Admission to the asynchronous Serve queue, before any read preparation.
// Mutations can wait for hidden RAM writers, so they must not block the thread
// that completes those writers. A deferred request also fences later requests;
// releases alone may bypass it because they only retire existing grants.
inline bool ShouldDeferRdmaRequest(WireOp op, bool pending_disk_reads,
                                   bool prior_deferred) {
  if (op == WireOp::kPullRelease) return false;
  if (prior_deferred) return true;
  return pending_disk_reads && (op == WireOp::kCache || op == WireOp::kRemove);
}

}  // namespace dfkv

#endif  // DFKV_CACHE_RDMA_REQUEST_ORDER_H_
