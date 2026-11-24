# Lifetime checked-candidate retention

This is a narrowly scoped follow-up to
[candidate and hot-refill diagnostics](DISK_LIFETIME_CANDIDATE_REFILL.md).
The prototype changed explicit-lifetime class/background selection, not the
scoring formula, legacy/RAM eviction, production policy default, ABI or disk
format. **It was tested but not adopted:** final production code is restored to
`ee005ff`; only baseline-compatible diagnostics and this report are delivered.
This is not complete queue-head fairness, formal A/B or deployment approval.

## Design decision and limitations

Class foreground and background share `EvictLifetimeFromLocked`. In the
experiment baseline, checking an eligible candidate advanced the shared band cursor even when another
band won. Continuous NEW appends could keep the next comparison near the tail,
preventing a previously checked, aging candidate from competing again.

The rejected prototype retained the shared cursor at each checked legal
candidate. It kept owner-local continuation, visit/slice budgets and version
validation unchanged. If that
candidate wins, existing unlink logic advances the shared cursor to a surviving
successor. Pinned/invalid slots still advance normally; no new lock, field,
resident visit, score cache or production test hook is added. Scores are still
recomputed under the existing lock. This directly matches the global watermark
path's existing retention of checked nonwinning candidates; that implementation
is reviewed and regression-tested but not modified.

Alternatives considered:

| Option | Reason not chosen for this iteration |
|---|---|
| Restart every selection at physical head | Can repeatedly consume the budget on a long pinned prefix and erase continuation progress. |
| Paid head probe plus continuation cursor | Could rediscover previously bypassed prefixes, but adds per-band state, budget work and cross-slice/cached-candidate rules. Requires separate design and validation. |
| Retain checked legal candidates | Chosen minimal change; prevents new loser-induced drift without extra visits, but is not globally fair. |

**Residual counterexample:** a legal non-head cursor can already have a colder
physical prefix behind it. Retaining its current candidate can delay that prefix
longer than the old implementation, which might otherwise wrap to the head.
Archived prototype tests retain this limitation over multiple selections. No claim of monotonic
improvement for every queue state, global minimum, starvation freedom or causal
identification of historical native victims is made. The specification still
only promises the minimum among actually checked eligible candidates.

## Test and native contract (before load)

RED first: naturally initialized unread head competes with a read candidate
inside P, then receives continuous NEW appends. When P ends, the older retained
candidate must compete again and win instead of another hot read candidate.
Exercise class and background entry points; the equivalent global scenario
must remain correct without changing global code. Preserve existing long-pin,
independent-owner, heat-version, 1024-slice, total-budget, metadata-failure,
read-relink and index-consistency regressions; do not weaken their budgets.

After build and tests, freeze source, binaries, tools and all effective options.
Repeat the previous six-case contract on the new build:

| Pair | Hot-miss refill | Order |
|---|---|---|
| 0 | off | legacy, lifetime |
| 1 | on | lifetime, legacy |
| 2 | on | legacy, lifetime |

Unchanged profile: default nine scores, three-store 12-GiB logical capacity
(nominal 12,288 slots), two namespaces, 4,096-byte values, 64 hot keys overall,
128 NEW objects/second, 240-second pre-aging and 1,800 measurement steps,
ACK-aged first reuse at 45/185/600 seconds with <=2-second lateness. RAM off,
disk ACK, class pressure only, RDMA/io_uring required; no TCP data fallback.
Each client deadline is 2,160 seconds, host pair deadline 4,600 seconds. Retain
all cases, fail-stop on correctness/infrastructure failure, no outcome-based
retries or parameter tuning. The prior round is a historical old-build reference,
not a newly randomized matched old/new experiment. Concurrent legacy cases on
the new build remain the policy reference. No business trace, weights or
numeric benefit acceptance threshold is invented.

Record scheduled hot hit/miss, distinct losses, repeated misses, refill
ACKs/payload bytes, verification and later scheduled outcomes separately;
verification reads add heat and do not become original scheduled hits. Compare
45/185/600 cohorts and full GET/PUT/metric denominators. Also extract existing
band/class visits per victim, total reclaim lock calls/wait/held time, scheduled
hot batch latency and step overrun from raw artifacts. Batch latency is not
per-key p99, total lock time is not a lock-tail guarantee, and hit/miss mix can
change cost. No new performance metric or hot-path logging is introduced.
Existing timing limits and safety budgets remain correctness gates; descriptive
cost comparisons are not formal production performance acceptance.

Only HOST disk2/disk3 may receive load. HOST `/mnt/disk1` stays prohibited;
GPU excluded. Require fresh exact VM PID/start/UID/netns/six-backing proof and a
fresh watchdog stopping on SMART warning, invalid reading, >=65 C sensor or
>=25-second stale sample. Guest `/data/disk1` is backed by HOST disk2; guest
`/data/disk2` and `/data/disk3` by HOST disk3. Preserve raw artifacts and terminate
only owned workloads. Never delete in-use backing images.

## Results

### Decision: tested prototype not adopted

All six prototype cases passed correctness/accounting, but did not establish a
benefit. The prototype removed the unit-level loser drift without resolving the
native hot losses, while the observed late-cohort advantage disappeared. The
non-head counterexample also remains. Internal consistency with global selection
is not sufficient reason to change production policy behavior: the old code did
not violate the specification's checked-minimum promise.

The four-line production patch and its dependent test expectations were safely
withdrawn **after** preserving the full patch, 323-input source snapshot, tests,
binary hashes and original raw results. Final diagnostics run on unchanged
production behavior. No score/P tuning, head-probe expansion, default flip or
outcome-based retry was performed. This is insufficient adoption evidence, not
an invented formal acceptance failure: no business weights or benefit threshold
were supplied, and old/new references are historical rather than matched samples.

### Actual prototype results (2026-11-24)

Six fresh cases completed in frozen AB / BA / AB order. Pair durations were
4102.083 / 4101.123 / 4101.437 seconds, totaling 12304.643 seconds (about 3.418
hours), excluding preparation/audit. All scheduled measurement hot GET
numerators below use the same 115,200 denominator; verification is excluded.

| Case / hot refill / policy | Hot hits / misses | Distinct keys lost | Repeat misses | Hot refill ACKs | Extra hot PUT payload bytes |
|---|---:|---:|---:|---:|---:|
| 0 / off / legacy | 115200 / 0 | 0 | 0 | 0 | 0 |
| 1 / off / lifetime prototype | 6053 / 109147 | 64 | 109083 | 0 | 0 |
| 2 / on / lifetime prototype | 6049 / 109151 | 64 | 109087 | 109151 | 447082496 |
| 3 / on / legacy | 115200 / 0 | 0 | 0 | 0 | 0 |
| 4 / on / legacy | 115200 / 0 | 0 | 0 | 0 | 0 |
| 5 / on / lifetime prototype | 6033 / 109167 | 64 | 109103 | 109167 | 447148032 |

Both prototype refill samples have **zero later scheduled hits after first hot
loss**: 0 hit / 109087 miss and 0 hit / 109103 miss. Each hot miss was refilled
and immediately verified successfully (109151 / 109167 verification hits).
Those verification reads are not successful scheduled hot probes. The added
hot PUT payloads are approximately 426.37 / 426.43 MiB, not NAND amplification.

All policies/cases have 45-second first reuse 64 hit / 0 miss, but 185- and
600-second first reuse each 0 hit / 64 miss. Every case separately refills these
128 delayed misses: 524288 extra payload bytes and 128 verification hits. The
final cooled cohort is 0 / 64 per case. First-read ages are actually
45.6356–45.7513, 185.6519–185.8694 and 600.7203–600.9634 seconds; each follows
positive foreground eviction (minimum delta5585). Pre-aging spans
240.0460–240.0507 seconds and measurement 1800.3702–1800.3909 seconds.

Full denominators, including conditional verification:

| Case | PUT ACKs / bytes | GET attempts / hits / misses | Foreground victims |
|---|---|---|---:|
| 0 | 242816 / 994574336 | 131264 / 131072 / 192 | 230528 |
| 1 | 242816 / 994574336 | 131264 / 21925 / 109339 | 230528 |
| 2 | 351967 / 1441656832 | 240415 / 131072 / 109343 | 339679 |
| 3 | 242816 / 994574336 | 131264 / 131072 / 192 | 230528 |
| 4 | 242816 / 994574336 | 131264 / 131072 / 192 | 230528 |
| 5 | 351983 / 1441722368 | 240431 / 131072 / 109359 | 339695 |

All watermark/background/donor counts remain zero in this class-only workload.
Byte/status checks, io_uring counters, effective configuration and raw pressure
scrapes agree with the complete client ledgers; native children and servers
exit normally and are reaped. Distinct-key counts are per case, not a promise
that exactly that many physical eviction events occurred. No per-victim trace
was collected.

### Historical comparison and interpretation

Previous unmodified lifetime behavior, under the same diagnostic profile:

| Mode | Historical scheduled hot hits / misses | Prototype |
|---|---|---|
| off | 6009 / 109191 | 6053 / 109147 |
| refill repeat1 | 27000 / 88200 | 6049 / 109151 |
| refill repeat2 | 28182 / 87018 | 6033 / 109167 |

Historical no-refill lifetime first185/600 cohorts both hit; historical refill
retained185 but lost600. The prototype loses both in all three cases. The
44-hit no-refill difference does not demonstrate meaningful improvement; the
refill samples instead show more repeated losses and writes. Alternating order
reproduced this direction, not proof of statistical independence.

A plausible explanation is that fixing drift makes the candidate comparison
more faithful without changing a high-arrival scoring tradeoff: nominal
capacity turnover is96 seconds while NEW protection P is180 seconds, so
`lambda*P/C=1.875`. Many protected NEW candidates can outrank read-hot objects.
Removing drift also changes which unread cohorts remain. This is a hypothesis
consistent with the fixture and aggregate observations, not captured victim
causality or proof that lifetime is universally worse. Day-scale aging, actual
business distributions/costs and formal A/B remain unvalidated. Do not tune P or
other scores after these results and call that the same frozen experiment.

### Descriptive scan and timing costs

Existing raw counters and hot batch timings were independently extracted; no
new instrumentation was added. Each row is a lifetime sample:

| Mode | Old visits/victim | Prototype visits/victim | Old mean allocation A-lock held ns/call | Prototype mean held ns/call | Old scheduled batch GET p99 ms | Prototype p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| off | 1.05292 | 1.05307 | 30384.9 | 29999.7 | 16.995 | 16.626 |
| refill1 | 1.45089 | 1.35750 | 37489.6 | 37165.9 | 16.962 | 17.238 |
| refill2 | 1.45484 | 1.35753 | 37453.7 | 36932.1 | 16.834 | 16.342 |

Lower visits/victim under refill does not imply a faster/better policy: the
prototype generated many more victims. Full numerator totals, wait/held totals,
all-hit batch subsets, maxima and step timing are retained in `cost-audit.json`.
Hot GET timings are client-observed wall time including payload preparation and
status validation. They cover 3600 batches of32 keys per sample, not per-key
p99, total PUT latency or a fixed-arrival throughput test. Mixes of hits/misses
and host/time effects differ. Reclaim-lock totals measure allocator A only,
not store D or request tails. Legacy does not collect these lifetime lock/band
metrics; its zero/absent samples must not be interpreted as zero work.

### Validation, artifacts and reproduction

Prototype verification before native load:
- RED on original production code: 14 candidate tests,11 pass/3 fail; two real
  class/background victim mismatches plus the changed cursor expectation.
- Prototype GREEN:15 candidate tests,347 lifetime/identity CTest tests and90
  DiskSlab tests, all pass0skip. TSan85 and ASan/UBSan85 pass0skip in new isolated
  builds. This includes pin/owner-budget/version/persistence/concurrency cases;
  it is not full repository/sanitizer/RDMA hardware acceptance.
- 48 existing workload/runner pure tests and293 related pure regressions pass0skip.
- Guest `/opt/dfkv-build` Debug RDMA/URING ON successfully rebuilt;6 selected
  Python CTest entries execute146 methods, plus15 candidate tests pass. These
  build/tests belong to the prototype source, not the final restored tree.
- LLVM17.0.6 candidate-test tidy/diff-format pass. Full production-file tidy
  reports37 existing diagnostics on unchanged lines; changed-line filtering
  passes. No wholesale cleanup or suppression was added, and full-file tidy
  cleanliness is **not** claimed.

Master evidence:
`/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/candidate-retention/20261124T013020Z-ee005ff/`

Original pre-run plan SHA:
`0d63b70bc7903272eae491ae31ab3c80ce8d928eaa141e7951b8cbb264ca1087`.
`frozen-contract.md` and `source.tar`/`source.sha256` describe the prototype;
`prototype.patch` preserves the original tracked diff before withdrawal. Raw
reports, snapshots, build hashes, tests and all-six audits remain untouched.
The updated report is not substituted for the immutable pre-run contract.

To reproduce the rejected prototype, start from ee005ff in a separate disposable
checkout and apply **only** `prototype.patch` from that evidence (never apply it
implicitly to the delivered working tree), or rebuild the exact archived source.
Overlay the archived source onto the same baseline checkout if using the archive.
Then follow the six-case invocation in the preceding diagnostic report using a
fresh tag and the same external host guard. Do not mistake a rebuilt source for
the exact historical binary without recording new hashes. This is an explicit
experiment, not an instruction to adopt the rejected change.

The prototype postflight checks source323, binaries, VM backings, rxe0,
io_uring, owned-process absence and ports12000/12001/12010. Its owned monitor
stopped normally/reaped;1266 samples peak41 C on both allowed devices,
SMART0/maxgap11.784 seconds. HOSTdisk1 was not loaded or sampled; GPU excluded.
Retained18 guest data directories total293.90625 MiB allocated:
`/data/disk{1,2,3}/cases/sg-long-20261124T013020Z-ee005ff-*`.
Exact paths/sizes are in `retained-data.json`; results are
`/data/disk1/results/retention-20261124T013020Z-ee005ff-p{0,1,2}`.
Host controls/archives:
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/candidate-retention-20261124T013020Z-ee005ff/`.
Do not delete in-use raw/qcow2 images. No native workload remains running.

### Final delivered tree (after rejecting the prototype)

Only three task files remain changed: this report, CHANGELOG and
`test/cache/slab_lifetime_band_scan_test.cc`. Production `slab_allocator.cc`
is byte-identical to ee005ff. All original12 test bodies remain byte-identical;
four new diagnostics characterize the existing class/background/global
cross-P selection difference and global retained-candidate pin/remove behavior.
They do not encode the rejected prototype as the required production behavior.

Fresh final commands from the repository root:

```bash
cmake --build build-test -j2
build-test/slab_lifetime_band_scan_test
ctest --test-dir build-test \
  -R '^(SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
python3 -m unittest discover -s test/python -p 'test_long_residence*.py' -v
```

Final host results:16/16 candidate tests;433/433 targeted CTest tests
(342 lifetime/allocator +91 DiskSlab including geometry),0fail/skip,5.40seconds.
Final diagnostic target rebuilt under TSan and ASan/UBSan separately:16/16 each,
0fail/skip. These final16 runs are distinct from the prototype85-test sanitizer
runs; neither is a full repository sanitizer acceptance.48 workload/runner pure
tests also freshly pass. Final test SHA-bound LLVM17.0.6 tidy and diff-format
pass; no production cleanup or new dependencies were introduced.

The guest was restored as well, using separately frozen
`delivery-source.tar`/`delivery-source.sha256`, never overwriting prototype
`source.tar`. Fresh RDMA/URING ON Debug full rebuild plus six exact Python
CTest entries146 methods and16 candidate tests pass,0skip. No native data load
was rerun after withdrawal. Restored server/library hashes exactly match the
original unmodified binary hashes; prototype binaries were first retained under
`/opt/validation-20261124T013020Z-ee005ff/prototype-build/`.
Restoration receipt, tests and source/binary proofs: evidence `delivery-guard/`.
A separate13-sample guard for restoration peaked40 C, SMART0; it too stopped
normally and was reaped after idle/port/VM proof. The existing guest is idle,
with no owned workload or thermal monitor running. HOSTdisk1 remains prohibited.

The final delivery is **diagnostic tests plus a documented rejected experiment**,
not an algorithm improvement. Formal A/B and overall policy benefit remain open.
