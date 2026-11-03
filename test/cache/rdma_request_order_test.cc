#include "cache/rdma_request_order.h"

#include <gtest/gtest.h>

#include "transport/wire.h"

namespace dfkv {
namespace {

TEST(RdmaRequestOrder, MutationsCannotBlockTheirOwnDiskCompleter) {
  for (const WireOp op : {WireOp::kCache, WireOp::kRemove}) {
    EXPECT_TRUE(ShouldDeferRdmaRequest(op, true, false));
    EXPECT_FALSE(ShouldDeferRdmaRequest(op, false, false));
  }
  // Independent reads retain pipelining until a fallback establishes a fence.
  EXPECT_FALSE(ShouldDeferRdmaRequest(WireOp::kPullRange, true, false));
}

TEST(RdmaRequestOrder, DeferredBarrierPersistsAfterDiskCompletion) {
  for (const bool pending : {false, true}) {
    for (const WireOp op :
         {WireOp::kCache, WireOp::kRemove, WireOp::kPullRange, WireOp::kExist,
          WireOp::kLookup, WireOp::kStats, WireOp::kLeasePut}) {
      EXPECT_TRUE(ShouldDeferRdmaRequest(op, pending, true));
    }
  }
}

TEST(RdmaRequestOrder, ReleasesAlwaysProgressPastBarrier) {
  for (const bool pending : {false, true}) {
    for (const bool deferred : {false, true}) {
      EXPECT_FALSE(
          ShouldDeferRdmaRequest(WireOp::kPullRelease, pending, deferred));
    }
  }
}

}  // namespace
}  // namespace dfkv
