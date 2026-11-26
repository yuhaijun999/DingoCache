# RDMA prefix-chain delayed reuse and continuous turnover

Follow-up: [mixed-size prefix / global-watermark long-duration validation](DISK_LIFETIME_PREFIX_MIXED_VALIDATION.md) records twelve separately frozen due-first cases, including prior failures; do not pool those results with this earlier uniform-size experiment.

This is a bounded synthetic cache-service diagnostic, not formal production A/B.
It measures payload-validated usable-prefix proxies, **not inference-engine adoption
or TTFT**. The user excludes real inference-engine, TTFT and GPU validation.
Production scoring, defaults, ABI and disk format are unchanged.

## Frozen workload and interpretation

- Fresh legacy/lifetime/lifetime/legacy cases, same binary and immutable plan.
- Default scoring; RAM off, disk ACK, GET miss retries zero; RDMA RXE and
  io_uring only. No TCP cache fallback.
- Three guest ext4 volumes, 12 GiB logical slab capacity, 4 KiB values. This
  compact geometry has nominal 12,288 class slots (~48 MiB payload), **not**
  12 GiB of simultaneously resident business data.
- Initial 12,288 objects, shared hot blocks written last, then checked warm-up.
- 900 absolute one-second arrival slots, each 64 new blocks: 57,600 incoming
  objects, 225 MiB payload, 4.6875 nominal class-capacity turnovers. Actual
  foreground evictions must also be positive; incoming bytes alone do not prove
  eviction or full-device turnover.
- Each slot enrolls three distinct eight-block chains. First reads are due at
  45, 185 and 600 seconds after the entire arrival batch is confirmed ACKed.
  The batch-return time is a conservative chain-ready upper bound, not an
  individual block's exact device-commit timestamp.
- Three shared hot chains (eight-block long, shared-prefix branch, two-block
  short) every five slots. Prefix benefit freezes at the first miss before
  suffix refill; refill ACKs/bytes and recomputation-token proxies are separate.
  Subsequent residency is not guaranteed by successful refill.
- All planned chain identities, actual enrollment, ACK-derived due eligibility,
  right-censoring, failures and unattempted requests remain accounted for. Edge
  eligibility may differ between policies due to actual ACK timing; never
  silently intersect successful requests or claim equal denominators without
  checking raw identities.
- The fixed 900-second cutoff admits chains whose ACK-derived due time is
  within the window. The end drain may start those reads just after the cutoff,
  still subject to the two-second reuse-lateness guard. It is not a strict
  promise that every native call completes within 900 seconds.
- Maximum scheduling/reuse lateness is two seconds, a diagnostic validity guard,
  not a performance SLO. Serial arrival scheduling is not independent concurrent
  open-loop or latency-distribution evidence.

The three delay cohorts use distinct new objects; earlier reads cannot heat a
future 600-second sample. This is not an age curve obtained by repeatedly reading
the same chain. Hot requests have a fixed long → branch → short order with shared
blocks, so later requests can benefit from earlier reads/refills. Reversing policy
case order does not randomize that within-case business order. One namespace,
generation zero, uniform 4 KiB objects and single-reader serial execution remain
limits. The run covers minutes and several compact class turnovers, not a
terabyte live set, week-long residency or physical-HCA cross-host performance.

## Safety

HOST `/mnt/disk1` and `/dev/nvme1*` are prohibited for pressure, bulk reads,
results/backing placement and SMART sampling. Only host disk2/disk3 are allowed.
Guest `/data/disk1` is a separate volume backed by HOST `/mnt/disk2`, not the
prohibited host disk1. Fresh QEMU start/UID/network namespace and six backing-FD
identity checks precede native work. A live host watchdog samples only nvme2 and
nvme0, stops at >=65 C, warnings, invalid samples or >=25 s stale monitoring.
All owned client/server children must be reaped; cache directories are retained,
not deleted via live VM backing files.

## Entrypoints

- [`prefix_delayed.py`](../deploy/validation/prefix_delayed.py): immutable plan,
  ACK-aged scheduling, checked terminal outcomes and raw-ledger accounting.
- [`run_prefix_delayed.py`](../deploy/validation/run_prefix_delayed.py): guest-only
  fresh AB/BA cases, process bounds and exact native/Prometheus reconciliation.
- Previous short serial diagnostic:
  [RDMA prefix validation](DISK_LIFETIME_PREFIX_RDMA_VALIDATION.md).
- Business interpretation:
  [prefix acceptance](DISK_LIFETIME_PREFIX_BUSINESS_ACCEPTANCE.md).

## This execution

The frozen main ABBA run finished in **3,639.912 seconds**. All four native
cases passed correctness, payload, scheduling and metric reconciliation. The
tables below retain every case; they are not a benefit-acceptance verdict.

## Reproduction commands

Repository root, existing host OFF-feature build (pure tests, no TCP traffic):

```bash
cmake --build build-test -j2
(cd test/python && python3 -m unittest test_prefix_delayed test_prefix_delayed_runner -v)
ctest --test-dir build-test \
  -R '^(python_prefix_(chain|dynamic|rdma|rdma_runner|delayed|delayed_runner)|SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
```

Only inside the freshly proven, externally thermally guarded guest, with a new
tag/output directory each time (not an instruction to run on HOST disk1):

```bash
sudo -n prlimit --memlock=unlimited -- python3 /opt/dfkv/deploy/validation/run_prefix_delayed.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases --data-root /data/disk2/cases --data-root /data/disk3/cases \
  --output /data/disk1/results/prefix-delayed-NEW_TAG --tag NEW_TAG
```

Each client has a 1,100-second process limit. The supervisor kills/reaps a timed
out client and stops/reaps its exact server; later cases remain unattempted. A
whole-batch PUT exception means unknown partial mutation, not proof of zero
writes. Process death can lose an in-memory partial ledger: keep the frozen
plan, supervisor terminal state and logs, never fabricate a zero-error report.

The checker re-derives accounting using the same client state machine; it is not
an independent replacement-policy oracle. Payload hashes record checked claims,
not authenticated packet evidence. Independent raw/Prometheus review is listed
separately below.

## Main native results

All four cases enrolled the same 2,700 delayed chains plus 540 hot requests:
**3,240 planned requests**, target weight 493,920. Actual ACK-derived eligible
identities matched across all cases, not just successful-request intersections.
45/185/600-second cohorts had 855/715/300 due requests and 45/185/600 censored
requests respectively. Thus 2,410 requests with target weight 361,120 are
measurable; the 830 right-censored chains remain explicitly recorded and are
not counted as misses or silently discarded. Each due request completed with
known terminal outcomes, with no unknowns or failed requests.

Weights 8/16/24/32 per object are frozen synthetic token weights, not measured
tokens from a real inference engine. Each eight-block chain targets 160 and
each short hot chain 24. Full-chain rate and partial-prefix token reuse are
distinct metrics. `U/T` below counts only the continuous prefix before the
first gap, before suffix refill.

| Case | Policy | U / T | Reuse proxy | Full prefix / due requests | Refill ACKs | Refill payload bytes |
|---|---|---:|---:|---:|---:|---:|
| 00 | legacy | 201560 / 361120 | 55.82% | 1409 / 2410 | 7965 | 32624640 |
| 01 | lifetime | 166088 / 361120 | 45.99% | 1069 / 2410 | 9640 | 39485440 |
| 02 | lifetime | 159656 / 361120 | 44.21% | 1083 / 2410 | 9949 | 40751104 |
| 03 | legacy | 200432 / 361120 | 55.50% | 1401 / 2410 | 8020 | 32849920 |

Per cohort, cell is **usable-prefix weight; full-prefix count; refill ACKs**.
The fixed target denominator is in the first column.

| Cohort | 00 legacy | 01 lifetime | 02 lifetime | 03 legacy |
|---|---:|---:|---:|---:|
| 45s (T=136800, N=855) | 136800; 855; 0 | 136800; 855; 0 | 130560; 812; 298 | 136800; 855; 0 |
| 185s (T=114400, N=715) | 2840; 14; 5565 | 3368; 5; 5477 | 2552; 1; 5509 | 1712; 6; 5620 |
| 600s (T=48000, N=300) | 0; 0; 2400 | 1032; 0; 2317 | 616; 0; 2343 | 0; 0; 2400 |
| hot-long (T=28800, N=180) | 28800; 180; 0 | 1856; 6; 1317 | 0; 0; 1440 | 28800; 180; 0 |
| hot-branch (T=28800, N=180) | 28800; 180; 0 | 20000; 83; 428 | 21608; 90; 359 | 28800; 180; 0 |
| hot-short (T=4320, N=180) | 4320; 180; 0 | 3032; 120; 101 | 4320; 180; 0 | 4320; 180; 0 |

Both matched pairs show modest 185-second partial-prefix improvements for
lifetime, but fewer full eight-block chains: 14→5 and 6→1. At 600 seconds only
lifetime retains some partial prefix; **neither policy has a full eight-block
chain hit**. These gains do not offset hotspot losses in this fixed synthetic
mix: total prefix proxies are lower and refill writes higher for lifetime.
Relative to its paired legacy case, lifetime adds 1,675/1,929 application refill
ACKs, or 6,860,800/7,901,184 payload bytes. These are application writes, **not
measured NAND write amplification**.

The reverse lifetime case also loses some 45-second prefix reuse; do not state
that every short-delay chain is always protected. Two opposing-order pairs do
not prove statistical significance, absence of order effects, steady state or
universal strategy ranking. No parameters were tuned and no result was retried
or removed to improve the comparison. Production defaults remain unchanged.

The short hot chain's full hits may follow long/branch suffix refill restoring
shared blocks. They show the actual ordered request outcome, not independent
retention throughout the preceding five seconds. No per-victim eviction trace
was collected; do not invent exact eviction causality from these aggregate
results. Future representative business weights and acceptance thresholds remain
separate from this diagnostic.

### Actual ages and native reconciliation

- 45s first-request age: 45.000608833–46.038633116 seconds from confirmed batch return.
- 185s first-request age: 185.001291879–186.045171204 seconds from confirmed batch return.
- 600s first-request age: 600.004733389–601.047752437 seconds from confirmed batch return.

| Case | GET hit / miss | PUT ACKs | PUT payload bytes | Class foreground evictions |
|---|---:|---:|---:|---:|
| 00 | 10253 / 1001 | 77853 | 318885888 | 65462 |
| 01 | 8578 / 1341 | 79528 | 325746688 | 65450 |
| 02 | 8269 / 1327 | 79837 | 327012352 | 65595 |
| 03 | 10198 / 1009 | 77908 | 319111168 | 65465 |

Native counts include initialization, warmup and refill; they are not the
business-prefix denominator. All four cases have zero GET/PUT errors and
unknowns, exact dispatched/terminal/uring reconciliation, healthy storage, zero
fallback/inflight/error metrics, and zero watermark/background/donor evictions.
All three disks are populated. Positive class evictions confirm real capacity
pressure, not merely a theoretical incoming-byte ratio.

## Post-matrix diagnostic repair and evidence provenance

Review found two failure-only reason-label overwrites: a warmup native failure
was relabelled `warmup_miss`, and a late native GET failure was relabelled
`reuse_deadline`. They never turned an error into a hit/miss counter or a passing
case, but hid the original root cause. Two captured regression failures precede
the fix. The repair only preserves the first native failure; warmup's
unconditional stop and all successful-path behavior remain unchanged. Tracked
regressions cover error/unknown, exact call count/event index, genuine miss with
no refill, and combined lateness/native failure.

The 900-second ABBA runtime used client SHA
`86de69a96e1297287404a19c26d9767441ded1c77aa7ba0f21b70e32c6f5b1c0`.
The final repaired client SHA is
`f64e229e7729d2b8b29de44bae9cc2af8523259835e622ab9ff46b0552eb94ac`.
Original source/package/raw results remain retained. Checking those raw ledgers
with the repaired checker is not a claim that the old native run executed the
new SHA. A separately labelled short, fresh real-RDMA run validates final code
with 1/2/3-second diagnostic delays and unchanged server scoring; it does not
replace or add samples to the 45/185/600-second business comparison.

## Final verification and retained data

Final repaired-source verification (all zero failures/skips):

- Host `build-test`, RelWithDebInfo, RDMA/uring OFF: successful build; 33 new
  Python methods 10.204 s; 442 targeted CTest entries 20.31 s; 211 related Python
  methods 14.611 s. The OFF build is not a TCP cache-traffic test.
- Guest `/opt/dfkv-build`, Debug, RDMA/uring ON: successful build; 219 Python
  methods 22.701 s; 445 targeted CTest entries 30.46 s. CTest Python entries overlap
  these method counts; do not add them as independent tests.
- New tests cover actual fake-clock 900-slot accounting, ACK edges/censoring,
  first-gap/refill freeze, bad payload/status/identity, batch unknowns, stale
  schedules, bounded loader/CLI/native adapter, process failures/timeouts,
  native/Prometheus disagreement and the two diagnostic regressions.
- Final-code bounded native smoke: two fresh cases passed in 23.377 s, plan
  explicitly uses three slots and 1/2/3-second delays, not accelerated scoring.
  Due counts 2/1/0, censored 1/2/3. Legacy 96 GET hits/0 misses and 12,480 PUT ACKs;
  lifetime 71/6 and 12,505 PUT ACKs; class evictions 192/208. These are correctness
  observations, not another strategy-benefit comparison.
- The final checker accepts all four original raw ledgers unchanged, with
  runtime/source provenance separated as above. A read-only independent reviewer
  regenerated all identities/payload hashes and audited all four main plus both
  short native ledgers, first-gap/refill/ACK-age/full denominators and exact
  Prometheus terminal paths without importing the workload checker.
- AST/bytecode compilation, local document links/fences and `git diff --check`
  pass. Ruff/mypy are absent, not run. No full-repository CTest, fresh sanitizer,
  physical-HCA cross-host or production-performance claim is made. GPU, real
  inference-engine adoption and TTFT are explicitly excluded, not passed.

Evidence root (plans, both source manifests/packages, review/test logs, raw
results, analysis, safety and completion receipts):

```text
/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/prefix-delayed/20261125T090147Z-7693b8b
```

Main result logs are `matrix-results/prefix-delayed-20261125T090147Z-7693b8b/`;
final-code short logs are
`postpatch-smoke-results/prefix-delayed-smoke-20261125T090147Z-7693b8b/`.
`analysis.json`, `raw-input-hashes.json`, `postpatch-check-original-ledgers.json`,
`verification-summary.json` and `safety-closeout.json` bind the results.
The expected-negative watchdog marker test and the two pre-fix RED regressions
remain retained; they were not native benefit-run failures or discarded samples.

Host backing/control/archive/temperature files are only under allowed HOST disk2:

```text
/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/prefix-delayed-20261125T090147Z-7693b8b/
```

Guest result directories (guest `/data/disk1` is backed by HOST disk2):

```text
/data/disk1/results/prefix-delayed-20261125T090147Z-7693b8b/
/data/disk1/results/prefix-delayed-smoke-20261125T090147Z-7693b8b/
/data/disk1/results/prefix-tests-20261125T090147Z-7693b8b/
/data/disk1/results/prefix-tests-20261125T090147Z-7693b8b-postpatch/
```

Retained cache directories, on each of the three guest volumes:

```text
/data/disk{1,2,3}/cases/sg-prefix-delay-20261125T090147Z-7693b8b-{00-legacy,01-lifetime,02-lifetime,03-legacy}
/data/disk{1,2,3}/cases/sg-prefix-delay-smoke-20261125T090147Z-7693b8b-{00-legacy,01-lifetime}
```

All 18 exact paths and allocation sizes are in `retained-data.json`: 308,183,040
allocated bytes (293.90625 MiB). No automatic data cleanup occurred. Do not delete
in-use host raw/qcow2 backing files to remove guest cache directories.

Safety closeout verified source/binary hashes, the six backing identities, active
RXE, io_uring enabled, no owned server/client/launcher, and free service ports.
The exact owned thermal monitor stopped normally and was reaped with exit 0;
429 samples, maximum 42 C, maximum interval 10.358235 s, zero SMART warnings,
no alarm. Only nvme2/nvme0 were sampled; HOST disk1 was neither pressured nor
sampled. The existing VM is left idle; no load or monitor remains active.

The server/library hashes remain unchanged throughout the main and final runs:

```text
dfkv_server 04dfa682c67cb0e334f4bc2c4cc76f7bcc26d783d5edfa38ca3c2408f844d64c
libdfkv.so  d18edf7ba78dbe130c59b31290cc119e4937c5df9ad00f3cf2f2688febca9bd9
```

This closes the bounded prefix-delay diagnostic, not formal A/B or production
release. Mixed sizes/global watermark eviction, independent concurrency,
prefix-level recovery and parameter/resource boundaries remain separate work.
Real inference-engine adoption, TTFT and GPU are not pending gates in this scope.
