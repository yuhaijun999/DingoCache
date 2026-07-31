"""Unit tests for the @sg-group presence-probing fix.

Commit under test: "fix(vllm): probe every @sg group in dedup/lookup, not
just @sg0".

A block's payload is split into scatter-gather blobs ``<key>@sg{n}``
(``len(kv_caches_base_addr)`` segments coalesced ``max_segs`` at a time).
Before the fix the store dedup AND the rank-0 lookup used ``@sg0`` alone as
the whole-block-present proxy, while the load path fetches EVERY ``@sg``
group -- so a block with ``@sg0`` present but a later ``@sg{n}`` missing (a
partially-failed PUT or an independent eviction of one blob) was reported
present, and the following load of the missing group failed into a
recompute.  The fix probes *every* ``@sg`` group; a (block) counts present
only when all of its groups exist.

These tests drive the REAL ``DfkvStoreWorker.lookup`` against a synthetic
dfkv key-space (the client is faked; only pure-python key/lookup math runs),
plus direct tests of the new ``_sg_groups_for_db`` helper.  This mirrors the
harness in ``test_dcp_lookup_geometry.py``.

The store-side dedup uses the SAME ``_sg_groups_for_db`` core and the mirror
rule ("a block is missing if ANY of its @sg groups is absent"); it lives
inside ``KVCacheStoreSendingThread._handle_request``, wrapped in the send
thread's queue/lock machinery and the GPU PUT path, so it is not cleanly
unit-testable in isolation -- its shared core is covered here and its
end-to-end behaviour belongs in the engine harness.

Run (needs vllm+torch, i.e. the engine image; NOT the plain dev env)::

    cd integration/vllm && python -m pytest tests/test_sg_group_probing.py -v
"""

import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace

import torch  # noqa: F401  (spec dtype; provided by the runtime image)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vllm.v1.core.kv_cache_utils import BlockHash
from vllm.v1.kv_cache_interface import FullAttentionSpec, KVCacheGroupSpec

from dfkv_vllm.coordinator import DfkvStoreCoordinator
from dfkv_vllm.data import KeyMetadata, PoolKey
from dfkv_vllm.worker import (
    DfkvStoreWorker,
    SG_MAX_SEGS,
    _sg_group_key,
    _sg_groups_for_db,
)

BLOCK = 64
NCHUNK = 8
# len(kv_caches_base_addr) per db. With the default width (SG_MAX_SEGS=29):
#   43 segments  -> ceil(43/29) = 2 groups  (@sg0, @sg1)   -- the fix's target
#   20 segments  -> ceil(20/29) = 1 group   (@sg0 only)    -- backward-compat
N_SEGS_MULTI = 43
N_SEGS_SINGLE = 20


# --------------------------------------------------------------------------
# 1. _sg_groups_for_db  (pure function -- the shared core of dedup + lookup)
# --------------------------------------------------------------------------
def _db_with_segs(n_segs: int) -> SimpleNamespace:
    return SimpleNamespace(kv_caches_base_addr=list(range(n_segs)))


def test_sg_groups_single_group_within_max():
    # n_segs <= max_segs -> exactly one @sg group (bit-for-bit legacy shape).
    assert _sg_groups_for_db(_db_with_segs(1), 29) == 1
    assert _sg_groups_for_db(_db_with_segs(28), 29) == 1
    assert _sg_groups_for_db(_db_with_segs(29), 29) == 1


def test_sg_groups_multi_group_above_max():
    # ceil(n_segs / max_segs) once we cross a max_segs boundary.
    assert _sg_groups_for_db(_db_with_segs(30), 29) == 2
    assert _sg_groups_for_db(_db_with_segs(58), 29) == 2
    assert _sg_groups_for_db(_db_with_segs(59), 29) == 3


def test_sg_groups_empty_defaults_to_one():
    # No payload segments -> guard returns 1 (never 0, which would probe nothing).
    assert _sg_groups_for_db(_db_with_segs(0), 29) == 1


def test_sg_groups_respects_narrower_width():
    # The grouping is per (n_segs, max_segs); a narrower negotiated width
    # (e.g. max_sge yielding 2) splits the same block into more groups.
    assert _sg_groups_for_db(_db_with_segs(4), 2) == 2
    assert _sg_groups_for_db(_db_with_segs(5), 2) == 3


# --------------------------------------------------------------------------
# 2. lookup: probe every @sg group  (read-side of the fix)
# --------------------------------------------------------------------------
_METADATA = {
    "model_name": "m",
    "tp_rank": 0,
    "pcp_rank": 0,
    "pp_rank": 0,
    "group_id": 0,
    "dcp_rank": 0,
}


def _md() -> KeyMetadata:
    return KeyMetadata(**_METADATA)


def _sg_key(h: BlockHash, grp: int) -> str:
    # Same on-wire form the lookup probes and the save path stores.
    return _sg_group_key(PoolKey(_md(), h.hex()).to_string(), grp, SG_MAX_SEGS)


def _hashes() -> list[BlockHash]:
    return [
        BlockHash(hashlib.sha256(f"{i}".encode()).digest()) for i in range(NCHUNK)
    ]


class _FakeClient:
    """Presence-only dfkv client: batch_exist checks a synthetic key set."""

    _sg_segs_cache = SG_MAX_SEGS  # default width -> keys are "@sg{n}"

    def __init__(self, present: set[str]):
        self._present = present

    def batch_exist(self, keys):
        return [1 if k in self._present else 0 for k in keys]


def _lookup(store: set[str], hashes: list[BlockHash], n_segs: int) -> int:
    spec = FullAttentionSpec(
        block_size=BLOCK,
        num_kv_heads=1,
        head_size=576,
        dtype=torch.bfloat16,
    )
    groups = [KVCacheGroupSpec([f"layer{i}" for i in range(43)], spec)]
    coord = DfkvStoreCoordinator(
        groups, scheduler_block_size=BLOCK, hash_block_size=BLOCK
    )
    db = SimpleNamespace(
        metadata=_md(),
        block_size=BLOCK,
        kv_caches_base_addr=list(range(n_segs)),
    )
    self_ = SimpleNamespace(
        coord=coord,
        token_dbs=[db],
        client=_FakeClient(store),
        tp_size=8,
        num_kv_head=1,  # MLA -> tp_count = min(8, 1) = 1
        pp_size=1,
        _kv_cache_groups=groups,
        _record_kv_connector_operation=lambda *a, **k: None,
    )
    return DfkvStoreWorker.lookup(self_, NCHUNK * BLOCK, hashes)


def test_lookup_all_sg_groups_present_full_hit():
    """Every chunk has ALL of its @sg groups (@sg0 AND @sg1): the rank-0
    lookup reports the complete prefix."""
    hashes = _hashes()
    store: set[str] = set()
    for h in hashes:
        for grp in range(2):  # N_SEGS_MULTI -> 2 groups
            store.add(_sg_key(h, grp))
    hit = _lookup(store, hashes, N_SEGS_MULTI)
    assert hit == NCHUNK * BLOCK, f"expected full prefix, got {hit}"


def test_lookup_missing_one_sg_group_excludes_block():
    """THE fix. Every chunk has @sg0; chunk 3 is missing @sg1 (partial PUT /
    single-blob eviction). It must be excluded, truncating the contiguous
    prefix at chunk 3.

    Documented negative: the pre-fix @sg0-only proxy would see chunk 3's
    @sg0 and over-claim the FULL prefix, then fail the load of the missing
    @sg1 into a recompute. If this ever reports NCHUNK*BLOCK again the
    @sg1-load-failure incident class is back.
    """
    hashes = _hashes()
    store: set[str] = set()
    for i, h in enumerate(hashes):
        store.add(_sg_key(h, 0))  # @sg0 for every chunk
        if i != 3:
            store.add(_sg_key(h, 1))  # @sg1 for every chunk EXCEPT chunk 3
    hit = _lookup(store, hashes, N_SEGS_MULTI)
    assert hit == 3 * BLOCK, (
        f"chunk 3 (@sg1 missing) must be excluded -> hit=3*BLOCK; got {hit}. "
        f"A full {NCHUNK * BLOCK} means the @sg0-only over-claim regressed."
    )


def test_lookup_single_sg_group_unchanged():
    """Backward compat: a db whose payload fits one @sg group (n_sg=1) probes
    exactly @sg0, identical to the pre-fix behaviour -- present when @sg0 is."""
    hashes = _hashes()
    store = {_sg_key(h, 0) for h in hashes}  # only @sg0 exists
    hit = _lookup(store, hashes, N_SEGS_SINGLE)
    assert hit == NCHUNK * BLOCK, f"single-group full prefix expected, got {hit}"
