# Candidate selection and hot-miss refill diagnostics

This follow-up to [default-score delayed reuse](DISK_LIFETIME_DEFAULT_RESIDENCE.md)
separates bounded-selector behavior from the cost of repeatedly probing missing
hot keys. It is diagnostic evidence, not formal A/B, a business-weighted winner,
a production default change, or a deployment approval. GPU tests are excluded.

## Scope and frozen native comparison

Use the same frozen source and binaries for six fresh RDMA-only cases:

| Pair | Hot-miss refill | Order |
|---|---|---|
| 0 | disabled (control) | legacy, lifetime |
| 1 | enabled | lifetime, legacy |
| 2 | enabled | legacy, lifetime |

The control pair is rerun rather than treating historical no-refill results as
same-source paired evidence. Two alternating refill pairs reveal observed order
variation; one control pair and a single physical machine do not prove statistical
independence or eliminate all temporal effects. Retain every attempted case;
stop on infrastructure/correctness failure, not on an unfavorable strategy result.

Keep the default profile unchanged: 12 GiB logical slab capacity over three
stores (nominal 12,288 slots), two namespaces, 4,096-byte values, 32 hot keys per
namespace, 128 new objects per one-second closed-loop step. Pre-age 240 seconds;
measure 1,800 steps; probe first reuse at ACK-relative ages 45/185/600 seconds
with at most two seconds lateness. Initialization ACKs do not promise simultaneous
residence. This is 18.75 nominal arrival-capacity turnovers, not day-scale aging.
Default nine lifetime scoring settings remain unchanged. RAM is off, PUT ACK is
disk, io_uring is required, and class-capacity eviction is isolated from
watermark/background/donor activity. These diagnostic settings are not all
production defaults.

Delayed-cohort first misses still receive the existing refill and verification
in **both modes**. The only workload switch is optional refill of measurement
hot misses: write the same key/value after explicit NOT_FOUND and verify the
ACKed value. Record the original miss, not a manufactured hit. Verification
reads themselves add heat and refill writes add pressure; neither is hidden.
No assumption about real business recomputation cost or weight is supplied.

Report distinct hot identities first observed missing, repeated hot misses,
refill ACKs and extra PUT payload bytes, verification GETs, and later scheduled
hot outcomes separately from delayed-cohort refills. Payload-byte overhead is
not physical NAND write amplification. Full GET denominators must include
conditional verification; PUT denominators include every acknowledged refill.
Unknown/error/timeout is never NOT_FOUND or a fabricated zero counter.

## Candidate-selection tests

Use existing allocator test peers rather than production hot-path hooks.
Exercise a non-head cursor and old prefix, continuous append, capped-read
relinking, and pinned skips; assert candidate visits and actual victim identities.
The specification guarantees minimum score only among checked legal candidates,
not all queue members. A reproducible bypass/starvation-risk example is not by
itself a scoring defect or a specification violation. These unit fixtures cannot
identify the causal history of each victim in the native workload, which has no
per-victim trace. Diagnose before considering a separate algorithm change.

## Safety and reproducibility

HOST `/mnt/disk1` is prohibited. Only dedicated HOST `/mnt/disk2` and
`/mnt/disk3` paths may receive load. In the disposable guest, `/data/disk1` is
backed by HOST disk2; `/data/disk2` and `/data/disk3` by HOST disk3. Before load,
verify the exact VM PID/start/UID/network namespace and all six open backing
files against allowed host files. Require a fresh thermal watchdog: stop on any
allowed-device sensor >=65 C, any SMART warning, invalid reading, or sample age
>=25 seconds. No host kernel/network change, broad process kill, or deletion of
in-use backing images is authorized by this diagnostic.

Freeze the plan, source archive, effective configuration, binaries and tools
before native writes. Use a 2,160-second client deadline and a 4,600-second host
pair deadline. Six cases schedule about 3.4 hours plus setup and audit. Preserve
raw client ledgers, independent pressure scrapes, metrics, process exits and
hashes. HTTP metrics/SSH and RDMA bootstrap are control traffic, not TCP cache
GET/PUT fallback. Production algorithm, ABI, format and default stay unchanged.

## Execution and results

### Candidate behavior

Four new deterministic tests use reachable NEW/read transitions, not fabricated
recovery states, and require no production hooks:

- A legal non-head cursor can skip an older, lower-score unread prefix. At
  `now=500000`, the old unread score is 104935, the checked read candidate is
  427200, and the checked newer unread candidate is 504935. Only the latter two
  are compared, so the read candidate is evicted. These deliberately wide ages
  demonstrate the mechanism; they are not the native workload's victim ages.
- With one unread append and one read candidate added per round, eight rounds
  check two candidates each and evict the read candidate while bypassing the old
  prefix. After appends stop, the next two selections reach the tail then the old
  prefix. This proves finite repeated bypass, **not infinite starvation**.
- A real successful read at saturated raw count 63 still refreshes the anchor,
  relinks both capped scoring-band indexes and repairs both cursors. The raw
  count is not the configurable scoring cap (MAX_READS remains 2 here).
- A pinned prefix consumes checks and pin-skip accounting; the legal candidate
  behind it is selected without reclaiming the pinned resident.

The implementation matches the specification's checked-candidate minimum. These
fixtures expose a selection-quality risk, not a confirmed score calculation bug,
a proven specification violation, or the causal history of native victims.

### Actual six-case results (2026-11-23)

All six cases completed on the frozen source, without retries or parameter
changes. Pair execution took 4101.947 / 4101.741 / 4101.660 seconds, about
**3.418 hours**, excluding preparation/audit. Each case really pre-aged
240.0464–240.0479 seconds and measured 1800.3723–1800.3841 seconds.

Every row below has 115,200 **scheduled measurement hot GETs**. Verification
reads are excluded from this fixed denominator.

| Case / mode / policy | Hot hits / misses | Distinct hot keys lost | Repeat hot misses | Hot refill ACKs | Hot refill payload bytes |
|---|---:|---:|---:|---:|---:|
| 0 / off / legacy | 115200 / 0 | 0 | 0 | 0 | 0 |
| 1 / off / lifetime | 6009 / 109191 | 64 | 109127 | 0 | 0 |
| 2 / on / lifetime | 27000 / 88200 | 64 | 88136 | 88200 | 361267200 |
| 3 / on / legacy | 115200 / 0 | 0 | 0 | 0 | 0 |
| 4 / on / legacy | 115200 / 0 | 0 | 0 | 0 | 0 |
| 5 / on / lifetime | 28182 / 87018 | 64 | 86954 | 87018 | 356425728 |

Lifetime's two refill cases each lose all 64 distinct hot keys at least once.
Every explicit hot miss receives one acknowledged refill and one successful
same-byte verification: 88,200 / 87,018 verification hits respectively. Their
**later scheduled reads after an identity's first successful refill** are
21,014 hit / 88,136 miss and 22,183 hit / 86,954 miss, excluding immediate
verification. Thus refill improves scheduled hit retention relative to this
control, but repeated losses and substantial refill costs remain. The refill
cases add approximately 344.53 / 339.91 MiB of PUT payload, not physical device
write amplification or measured business recomputation cost.

First-reuse outcomes are separate: each delay has 64 reads per case.

| Mode / policy | 45-sec hit / miss | 185-sec hit / miss | 600-sec hit / miss | Delayed-cohort refill ACKs |
|---|---:|---:|---:|---:|
| off / legacy | 64 / 0 | 0 / 64 | 0 / 64 | 128 |
| off / lifetime | 64 / 0 | 64 / 0 | 64 / 0 | 0 |
| on / legacy, each repeat | 64 / 0 | 0 / 64 | 0 / 64 | 128 |
| on / lifetime, each repeat | 64 / 0 | 64 / 0 | 0 / 64 | 64 |

Delayed-cohort refills cost 4,096 bytes and one successful verification each,
and are **not included in the hot-refill table**. In both refill repeats,
lifetime's 600-second cohort advantage disappears; its 185-second advantage
remains. Extra refill writes change pressure and successful verification reads
add heat. This is the behavior of the explicit **refill-plus-verification**
workload, not a pure PUT-only refill or a causal single-factor proof about the
cursor. Reverse order reproduces the direction, with a 1,182-hot-hit variation;
it does not establish statistical order independence.

Actual first-read ages across all cases were 45.6393–45.7423,
185.6700–185.8337 and 600.7560–600.9024 seconds. Every first-read event followed
positive measured foreground eviction (minimum delta 5,499). Initialization,
pre-aging and ready checks all hit; the final cooled cohort is 0 hit / 64 miss
per case. Watermark/background/donor eviction remains zero.

Full denominators (including conditional verification) and foreground victims:

| Case | PUT ACKs / payload bytes | GET attempts / hits / misses | Foreground evictions |
|---|---|---|---:|
| 0 | 242816 / 994574336 | 131264 / 131072 / 192 | 230528 |
| 1 | 242688 / 994050048 | 131136 / 21881 / 109255 | 230400 |
| 2 | 330952 / 1355579392 | 219400 / 131072 / 88328 | 318664 |
| 3 | 242816 / 994574336 | 131264 / 131072 / 192 | 230528 |
| 4 | 242816 / 994574336 | 131264 / 131072 / 192 | 230528 |
| 5 | 329770 / 1350737920 | 218218 / 131072 / 87146 | 317482 |

Case 2 has 88,264 more foreground victims than the lifetime control: 88,200 hot
refills plus 64 delayed refills; case 5 adds 87,082 by the same accounting.
The identical 131,072 total GET hits in the refill policies are caused by
conditional verification traffic; **they do not show equivalent cache hit
benefit**. Compare the fixed scheduled denominators, not this mixed total.

**Correctness and accounting pass; the hotspot-loss risk is not resolved by
refill.** The chosen high-arrival diagnostic remains unfavorable for lifetime's
hot cohort. No business-weighted aggregate winner, universal algorithm verdict,
production default change or formal acceptance follows. Before formal A/B,
review selection fairness as a separate design decision, while retaining these
negative results. Do not silently change the algorithm inside this experiment.

### Reproduction, verification and retained data

With the host watchdog active and the proven disposable guest idle, use a fresh
ASCII tag and the same frozen source/build. In the guest root shell:

```bash
ulimit -l unlimited
TAG=YOUR_FRESH_TAG
for PAIR in 0 1 2; do
  MODE=()
  if [ "$PAIR" -ne 0 ]; then MODE=(--hot-refill); fi
  python3 /opt/dfkv/deploy/validation/run_long_residence.py \
    --build /opt/dfkv-build \
    --data-root /data/disk1/cases --data-root /data/disk2/cases \
    --data-root /data/disk3/cases \
    --output "/data/disk1/results/refill-$TAG-p$PAIR" \
    --tag "$TAG" --profile default --pairs 1 --start-pair "$PAIR" \
    "${MODE[@]}" || exit 1
  # Audit the completed pair before proceeding to the next.
done
```

Default without `--hot-refill` performs no extra hot PUT/verification, while
new reports add per-key hot loss evidence. Complete old no-refill reports remain
accepted; partially deleted new ledgers or refill-mode reports are rejected.
The loop above is the invocation pattern, not a substitute for the external
identity/thermal/timeout guard and per-pair audit.

Fresh validation from the repository root:

```bash
python3 -m unittest discover -s test/python -p 'test_long_residence*.py' -v
cmake --build build-test -j2
ctest --test-dir build-test -R '^(SlabLifetime|SlabAllocator|DiskLifetime)' \
  --no-tests=error --output-on-failure
```

- 48 Python methods passed (7 new), plus an explicit Native-free whitelist of
  293 related pure regressions, zero failures/skips. RED evidence records new
  tests failing against the original code before implementation.
- Host `build-test` RelWithDebInfo, RDMA/URING OFF: successful build; 338 targeted
  math/clock/allocator/lifetime CTest tests passed, zero failures/skips, 2.64 s.
  These are unit tests, not TCP cache-service validation. Candidate direct/CTest
  runs pass 12/12 (4 new).
- Guest `/opt/dfkv-build` Debug, RDMA/URING ON: successful source-checked build,
  rebuilt candidate test; server/library unchanged. Six exact Python CTest
  entries execute 146 methods (8+22+12+28+20+56), all pass, plus 12 candidate
  tests. Not full CTest, sanitizer, physical HCA, cross-host or GPU acceptance.
- LLVM 17.0.6 diff formatting and final-SHA-bound clang-tidy passed for the
  changed C++ test; external/non-user-header diagnostics were filtered, not a
  claim that the whole repository is warning-free. Python AST/bytecode and
  whitespace checks passed; Ruff/Mypy are unavailable, no full lint claim.
- Independent audits bind the original plan/contract, 323 frozen source inputs,
  14 tool hashes, binary hashes, actual effective configuration, per-key ledgers,
  ACK ages, separate raw pressure scrapes, metrics, full denominators, six unique
  PIDs/18 fresh directories, chronological AB/BA/AB execution and normal process
  exits/reaping. Actual service GET/PUT traffic is RDMA-only with io_uring.

The owned workload/servers finished, ports 12000/12001/12010 are released, source
and binary postflight checks pass, and the owned monitor stopped normally and
was reaped. All 1,261 thermal samples were healthy; allowed nvme2/nvme0 each
peaked at 41 C, maximum sample gap 10.514 seconds, zero SMART warnings. HOST
`/mnt/disk1` was neither loaded nor sampled. The original VM remains idle.

Evidence root:
`/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/hot-refill/20261123T094749Z-55e957b/`

Original pre-run plan SHA:
`4616c6a6c86d2e0ef7d0e53d847066b48fe1cb5dcf4076f4cfbc4ece8f022c55`.
The immutable pre-run contract is retained separately from this updated report.
See `all-six-audit.json`, raw pair archives/reports, tests/JUnit, safety and
retained-data records. Per-key ledgers make each report about 31 MiB; extraction
is bounded at 64 MiB/file and 256 MiB/pair, rather than reading arbitrary data.

Retained guest data (18 directories, 293.90625 MiB allocated):
`/data/disk{1,2,3}/cases/sg-long-20261123T094749Z-55e957b-*`.
Exact paths and allocated sizes: evidence `retained-data.json`. Guest results:
`/data/disk1/results/refill-20261123T094749Z-55e957b-p{0,1,2}`.
Host control/archive path:
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/hot-refill-20261123T094749Z-55e957b/`.
The existing data1.raw is on HOST disk2; data2.raw/data3.raw on HOST disk3.
Delete only enumerated guest case directories when cleanup is desired; do not
remove in-use raw/qcow2 images containing previous rounds.
