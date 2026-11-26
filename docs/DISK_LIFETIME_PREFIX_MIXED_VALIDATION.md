# Mixed-size prefix-chain and watermark validation

## Status and scope

R2 completed all twelve 1800-second measurement cases. Earlier failed experiments
remain failed and retained; they are not silently relabeled or merged with R2.
This is a synthetic, single-host RXE plus io_uring diagnostic, not formal A/B,
physical-HCA performance, inference-engine adoption or TTFT. GPU and engine/TTFT
validation are excluded by the user. Production scoring, defaults, ABI and disk
format are unchanged.

## Predeclared workload

- Two actual slab classes: payloads 4096 and 1048577 bytes map to 1 MiB and 2 MiB
  slots under the unchanged default 1 MiB slot granularity. Merely varying small
  payloads below 1 MiB would not exercise different classes.
- Three guest ext4 disks; 12 GiB aggregate logical capacity, four 1 GiB extents per
  disk. This is compact capacity pressure, not a claim of TB-scale live KV.
- Initialize 12288 immutable objects; record three shared hot-chain warmup reads.
  Mixed-class donor competition may make warmup miss: retain the observations,
  do not promise identical initial residency or refill during warmup.
- Measure 1800 absolute one-second arrival slots, 64 objects per slot, with the
  default scoring parameters. Three distinct eight-block chains per batch receive
  their first query 45, 185 and 600 seconds after whole-batch ACK confirmation.
  That timestamp is a conservative chain-ready upper bound, not device commit time.
- Query three shared hot-prefix chains every five slots. Freeze the original
  continuous usable prefix before suffix refill; do not credit refill as a hit.
- Keep every planned identity, target weight, due/censored state and failure.
  ACK-due admission cutoff is fixed; bounded terminal drain is not extra arrival
  time. A failed GET is not a miss. Synthetic token weights are not real LLM tokens.
- Each route has three fresh policy pairs AB/BA/AB, A=legacy, B=lifetime. No
  outcome-based tuning, successful-run selection or hidden replacement of failures.

## Route interpretation

1. `class-pressure`: watermark and background reclaim disabled. Donor extent
   rebinding can still occur with only four extents per disk. This is NOT proof
   of isolated foreground-only eviction when donor counters increase.
2. `watermark`: diagnostic low/high thresholds 40/50 percent, reclaim interval
   50 ms. These are explicit test settings, not production defaults 88/92 percent.
   Background class reclaim and donor competition can coexist. Require actual
   high-water observations, watermark removals and freed slot/payload progress;
   configuration alone is insufficient. Aggregate watermark victims do not identify
   their size class; class PUT counters prove admissions, not per-class victims.

Read back effective settings before writes. Reconcile native GET/PUT bytes and
terminal paths at completion; live metric snapshots are not atomic. Record periodic
raw Prometheus and owned-server CPU/RSS samples. CPU/RSS trends and 30-minute
windows do not establish absence of leaks, latency SLOs or steady-state convergence.
Enrollment-window business summaries must preserve right-censoring, particularly
for the 600-second cohort in the final windows; do not compare ratios without
reporting the eligible denominator.

## Safety and retained evidence

Only host `/mnt/disk2` and `/mnt/disk3` may back this workload. Host `/mnt/disk1`
and `/dev/nvme1*` are prohibited for pressure and SMART sampling. Guest
`/data/disk1` is backed by host disk2; its name does not grant safety authority.
Before native traffic, verify QEMU PID/start/UID/network namespace and all six
backing FDs against host devices/inodes/sizes, source/build hashes, guest mounts,
RXE state, io_uring setting and free ports. A live host thermal supervisor samples
only nvme2/nvme0, stops at 65 C or any SMART warning/invalid/stale sample, and bounds
owned processes. Preserve raw ledgers, periodic samples, original failures and
exact retained data paths. Never delete an active VM image to clean a test directory.

## Interpretation limits

Thirty-minute cases cover repeated capacity turnover and sub-hour reuse, not the
unchanged default 12-hour heat-bucket rollover, seven-day base lifetime, crash
recovery or proof of long-term memory-leak absence. The workload is serial,
clock-paced diagnostic traffic; payload preparation occurs between slot wakeup
and native calls. Record actual native arrival-start lag as well as guarded
scheduler lateness, and do not label this independent open-loop concurrency.
Resource samples cover only the owned server, not workload-client/host total CPU.

The first native class preflight exposed an oracle error: a successful refill
upsert may overwrite a still-resident suffix without a new allocator insertion.
`dfkv_slab_class_put_total` therefore need not equal client ACK PUT counts.
The failed matrix and raw ledgers are retained unchanged. The revised oracle
must separately retain distinct ACKed identities, allocator admissions and ACKed
upserts, requiring the first <= second <= third for each actual size class.
Native ACK bytes and GET/PUT terminal counts remain exact. No production scoring
or eviction implementation is changed by this diagnostic repair.

## Scheduling experiment provenance

The first long R1 matrix stopped in its fourth (legacy) case. An ACK ended
1.238109 ms before an enrolled 185-second query became eligible; the old
arrival-first polling snapshot did not refresh during subsequent reads/refill.
The next arrival preparation plus a 1.484242-second backend call envelope delayed
the first query to age 187.370621927 seconds. That GET hit valid data, but the
predeclared two-second lateness guard correctly failed. This is not a cache miss,
proven server-I/O defect or reason to mark the experiment passed. The backend
interval includes ctypes/setup and is not exclusively native C ABI latency.

R1 retains three full completed cases, a fourth timing-failed partial case, two
unattempted cases and no watermark long run. The failed case has no terminal
`after.prom`; periodic samples cannot fabricate final counter reconciliation.

R2 is a NEW explicitly configured `due_first=True` experiment, with a different
plan hash and GET/PUT ordering: service already-due queries before new arrivals,
refresh eligibility after each completed request/refill, and recheck the tick
budget/cutoff after draining. The same two-second guard remains; arbitrary long
blocking still fails. Old v1/v2 modes and reference ledger fingerprints remain
compatible. R2 must rerun all twelve fresh cases; never merge old and new samples
as a completed pair. Tick/post-drain guards precede payload generation, so actual
backend-call start lag is separately retained, not claimed as an RPC-start SLO.

## Completed R2 results (2026-11-25 UTC / 2026-11-26 China)

The class-pressure six-case controller took **11482.520994 seconds**; the global-
enabled six-case controller took **11411.276600 seconds**. Together: **22893.797594
seconds (6 h 21 min 33.8 s)**, including initialization and controller validation.
The measurement intervals themselves total exactly six hours. Archives and
independent review are additional time. Same guest binaries and frozen workload
were used across both routes; only the declared reclaim/watermark settings differ.

Every R2 case retained **6480 planned business requests / 987840 target proxy
tokens**, with **5650 eligible completed requests / T=855040**, and **830 censored
requests**. Independent audits confirmed identical due/censored identity sets
across all twelve cases, not an intersection of successful reads. Warmup is
separate: three more recorded requests per case, excluded from business benefit.

| First reuse | Planned | Eligible | Censored | Eligible T |
|---|---:|---:|---:|---:|
| 45 s | 1800 | 1755 | 45 | 280800 |
| 185 s | 1800 | 1615 | 185 | 258400 |
| 600 s | 1800 | 1200 | 600 | 192000 |
| Three hot groups | 1080 | 1080 | 0 | 123840 |

Each case admitted 115200 original arriving objects, **60633964800 payload bytes**.
Their rounded slot input is **181193932800 bytes / 12 GiB = 14.0625 nominal slot-
capacity turnovers**. This counts original arrivals only; it is not observed
physical write amplification, unique resident payload or exact eviction turnover.

### Aggregate continuous-prefix benefit and refill

`U` is the checked contiguous usable-prefix proxy before any refill. The full-hit
column uses the same eligible denominator N=5650. Refill bytes count acknowledged
application payload, including overwriting still-resident suffix objects; they
are not NAND writes. Case order is AB / BA / AB within each route.

#### Watermark/background disabled; donor-qualified class pressure

| Case | Policy | U / T=855040 | Reuse % | Full / 5650 | Refill ACK objects | Refill payload B |
|---|---|---|---|---|---|---|
| 00 | legacy | 183392 | 21.4484 | 1118 | 31906 | 18252432326 |
| 01 | lifetime | 103368 | 12.0893 | 569 | 36040 | 20252834609 |
| 02 | lifetime | 87192 | 10.1974 | 329 | 37077 | 20728143092 |
| 03 | legacy | 181208 | 21.1929 | 1114 | 32017 | 18309288956 |
| 04 | legacy | 185216 | 21.6617 | 1117 | 31813 | 18205049753 |
| 05 | lifetime | 93232 | 10.9038 | 469 | 36548 | 20540058690 |

#### Global enabled; background/donor qualified

| Case | Policy | U / T=855040 | Reuse % | Full / 5650 | Refill ACK objects | Refill payload B |
|---|---|---|---|---|---|---|
| 00 | legacy | 135408 | 15.8365 | 1080 | 35282 | 19179136816 |
| 01 | lifetime | 55672 | 6.5110 | 378 | 39548 | 21215592125 |
| 02 | lifetime | 62912 | 7.3578 | 302 | 38600 | 21356891976 |
| 03 | legacy | 136080 | 15.9150 | 1083 | 35225 | 19169503015 |
| 04 | legacy | 136176 | 15.9263 | 1082 | 35189 | 19177711407 |
| 05 | lifetime | 54720 | 6.3997 | 349 | 39609 | 21276421879 |

In these fixed synthetic profiles, lifetime has **lower aggregate continuous-
prefix reuse and more refill in every matched pair**. This does not establish a
universal policy ranking, statistical power or real-engine benefit. The results
are not a basis for a production-default flip. R1 and R2 numbers are not pooled.

### Cohort tradeoffs

Entries are usable-prefix proxy tokens, not individual KV hit counts. Each row
uses the fixed cohort denominators above. No case had a full eight-block hit in
the 185- or 600-second cohorts. Partial prefixes still receive their actual value.

| Route/case | Policy | 45 s U | 185 s U | 600 s U | Hot combined U |
|---|---|---|---|---|---|
| class-pressure/0 | legacy | 50936 | 8616 | 0 | 123840 |
| class-pressure/1 | lifetime | 40208 | 7104 | 632 | 55424 |
| class-pressure/2 | lifetime | 30720 | 7328 | 344 | 48800 |
| class-pressure/3 | legacy | 48808 | 8560 | 0 | 123840 |
| class-pressure/4 | legacy | 52744 | 8632 | 0 | 123840 |
| class-pressure/5 | lifetime | 34472 | 6592 | 552 | 51616 |
| watermark/0 | legacy | 11568 | 0 | 0 | 123840 |
| watermark/1 | lifetime | 15592 | 0 | 0 | 40080 |
| watermark/2 | lifetime | 14920 | 0 | 0 | 47992 |
| watermark/3 | legacy | 12240 | 0 | 0 | 123840 |
| watermark/4 | legacy | 12336 | 0 | 0 | 123840 |
| watermark/5 | lifetime | 16784 | 0 | 0 | 37936 |

Under disabled watermarks, lifetime retained a little partial 600-second prefix
where legacy retained none, but lost more hot-prefix value and did not improve
45/185-second totals. Under global-enabled pressure, lifetime's 45-second partial
value increased, but both policies had zero 185/600-second prefix value; lifetime's
larger hot loss dominated the aggregate. This is the observed tradeoff, not proof
of which individual victim caused it. No per-victim causal trace was collected.
The hot groups have fixed long -> branch -> short ordering and shared prefixes;
later requests can benefit from earlier suffix refill, not independent retention.

### Actual reclaim routes and sustained measurement coverage

Disabled cases had zero watermark/background activity in all retained snapshots,
but three donor extent steals per case. Enabled cases had three donor steals and
background reclaim as well. Counter units differ: watermark victims and background
slots are not donor extents or metadata operations. Do not add unlike counters or
infer per-class watermark victims from class-admission counters.

| WM case | Policy | WM victims (whole case) | BG reclaimed slots | Donor extents | WM victims in six measured 300 s windows |
|---|---|---|---|---|---|
| 0 | legacy | 102744 | 52877 | 3 | 13306/14768/16322/16242/16883/16248 |
| 1 | lifetime | 132643 | 26789 | 3 | 16640/19204/21064/21147/21211/21083 |
| 2 | lifetime | 75005 | 80709 | 3 | 10008/10724/11875/11875/11906/11878 |
| 3 | legacy | 101998 | 53705 | 3 | 13205/14667/16755/16140/16104/16110 |
| 4 | legacy | 100602 | 54788 | 3 | 13020/14466/15962/16090/15940/15978 |
| 5 | lifetime | 133014 | 26413 | 3 | 16879/19262/21257/21792/21233/21091 |

All **36** enabled-route measurement windows had positive watermark victim,
freed-slot and freed-payload deltas between two raw scrapes wholly inside that
window. Thus global activity was not merely an initialization artifact. The
conservative window deltas exclude edge gaps, so they need not sum to whole-case
counters. Effective per-disk capacity/high/low were 4294967296 / 2147483600 /
1717986880 bytes; reclaim interval 50 ms. New above-high observations and actual
metadata-completed progress were checked. Live scrapes are not transactional:
metadata attempts/completions are retained separately, not forced to equality
while reclaim can still run. Exact client native terminal counts are reconciled
after the client exits. Background scheduling varied between repeats; this is
not an isolated causal estimate of the watermark selector's benefit.

### Time windows and resources

Business windows group requests by enrollment step, not completion time. The first
four 300-second windows each have T=164640; the last two have T=116640 and 79840
because of right-censoring. Do not interpret changing mixture/denominators as
steady-state convergence. Examples below retain U and T rather than only rates.

| Route/case | Policy | U in enrollment windows 0/300/600/900/1200/1500 s |
|---|---|---|
| class-pressure/0 | legacy | 32512 / 30480 / 31376 / 30184 / 30112 / 28728 |
| class-pressure/1 | lifetime | 23264 / 20248 / 18808 / 14504 / 14432 / 12112 |
| class-pressure/2 | lifetime | 20560 / 18560 / 14816 / 14656 / 11272 / 7328 |
| class-pressure/3 | legacy | 32456 / 30784 / 29872 / 29696 / 29760 / 28640 |
| class-pressure/4 | legacy | 34440 / 29968 / 30720 / 30712 / 31376 / 28000 |
| class-pressure/5 | lifetime | 20920 / 18720 / 15688 / 14656 / 11464 / 11784 |
| watermark/0 | legacy | 24808 / 22176 / 22184 / 22128 / 22184 / 21928 |
| watermark/1 | lifetime | 18496 / 7560 / 7632 / 7216 / 7240 / 7528 |
| watermark/2 | lifetime | 10680 / 10544 / 10416 / 10536 / 10608 / 10128 |
| watermark/3 | legacy | 25192 / 22264 / 22232 / 22152 / 22240 / 22000 |
| watermark/4 | legacy | 25008 / 22224 / 22200 / 22288 / 22424 / 22032 |
| watermark/5 | lifetime | 19240 / 7144 / 7136 / 7160 / 6968 / 7072 |

Periodic snapshots cover **only the owned server** CPU ticks, VmRSS and threads.
They exclude workload-client SHAKE/copy CPU, guest kernel/page cache, QEMU and host
resources; this is not complete memory-budget or end-to-end performance acceptance.
First/last RSS below span the sampled process lifetime, including initialization,
not identical measurement-only endpoints. Five-minute raw resource-window records
and sampled CPU deltas are retained in the analysis JSON. No p99/SLO is inferred.

| Route/case | Policy | Samples | First / last / peak RSS KiB | Max sample gap s | Max backend arrival-start lag s |
|---|---|---|---|---|---|
| class-pressure/0 | legacy | 184 | 16288 / 38312 / 38312 | 10.357676 | 0.712731 |
| class-pressure/1 | lifetime | 183 | 16412 / 29996 / 29996 | 10.374045 | 0.754525 |
| class-pressure/2 | lifetime | 183 | 16464 / 30096 / 30096 | 10.349525 | 0.630852 |
| class-pressure/3 | legacy | 184 | 16392 / 29528 / 29528 | 10.380800 | 0.848489 |
| class-pressure/4 | legacy | 183 | 16300 / 29368 / 29368 | 10.389822 | 0.876632 |
| class-pressure/5 | lifetime | 183 | 16376 / 48520 / 48520 | 10.394271 | 0.619087 |
| watermark/0 | legacy | 183 | 16372 / 27404 / 27404 | 10.354389 | 0.768367 |
| watermark/1 | lifetime | 183 | 16480 / 36804 / 36804 | 10.358017 | 0.654086 |
| watermark/2 | lifetime | 183 | 16452 / 28044 / 28044 | 10.439020 | 0.715235 |
| watermark/3 | legacy | 183 | 16336 / 27360 / 27360 | 10.412933 | 0.851102 |
| watermark/4 | legacy | 183 | 16472 / 27700 / 27700 | 10.373160 | 0.727232 |
| watermark/5 | lifetime | 183 | 16384 / 27644 / 27644 | 10.371467 | 0.653285 |

No native GET/PUT error or unknown terminal, payload mismatch, unhealthy sample,
io_uring fallback or final unsettled native request was observed in R2. Maximum
first-query lateness was 0.811335002 s (class) / 0.850351206 s (enabled), below the
unchanged two-second guard. These bounded observations do not prove absence of
rare failures, memory leaks, CPU contention or default multi-day aging effects.

## Verification and provenance

Final source freeze: **339 files**, `freeze-r2.json` / `source-r2.sha256` /
`source-r2.tar`. Primary runtime SHA256:

```text
prefix_delayed.py  7ae0a0a2a9e1d372e97bd64ffee40087b226d49685451700d3e9782d5d4654b8
run_prefix_mixed.py e4982b2414a5c2cd1acc1be4e8f93645b0e6df58099aba7d227b7cc27af89920
main plan          1a2cdd40697bef6da0c65a9a8bb62dfa180f4bcca43db2259e422ed36a97c524
dfkv_server        04dfa682c67cb0e334f4bc2c4cc76f7bcc26d783d5edfa38ca3c2408f844d64c
libdfkv.so         d18edf7ba78dbe130c59b31290cc119e4937c5df9ad00f3cf2f2688febca9bd9
```

| Packet | Actual use |
|---|---|
| `source.tar` / `freeze.json` | Initial pre-review unit/build packet only |
| `source-final.tar` / `freeze-final.json` | Original two-case native smoke; incorrect class-upsert equality failed the controller |
| `source-r1.tar` / `freeze-r1.json` | Four fresh smokes and failed R1 long matrix; arrival-first ordering |
| `source-r2.tar` / `freeze-r2.json` | Four fresh smokes and all twelve final long cases; explicit due-first ordering |

All packets, failed matrices and raw ledgers remain unchanged. Rechecking old
ledgers with a newer semantic checker is not native execution at a newer SHA.
Genuine RED/GREEN evidence is retained for the class-admission counterexample
and scheduling revision. Client-cleanup receipt regressions were added and passed
after review; no retained pre-fix cleanup-specific RED is claimed. Production C++ source/default scoring/ABI/disk
format were not changed. Old v1 default plan digest and deterministic v1/v2 ledger
fingerprints are regression-tested; the accepted steps bound expanded to 1800.

Fresh final-code checks ran before R2 native loads; hashes were verified unchanged
at closeout. Counts below overlap (Python CTest entries execute some same methods):

- Host `build-test`: RelWithDebInfo, RDMA/uring OFF build passed; **68** client/
  runner methods passed in **28.107 s**; **443** selected CTest entries passed in
  **38.39 s** (436 C++ + seven Python entries); **211** related Python methods
  passed in **14.404 s**. Zero failures/skips in these selected suites.
- Ubuntu guest `/opt/dfkv-build`: Debug, RDMA/uring ON build passed; **254** Python
  methods passed in **42.838 s**; **446** selected CTest entries passed in **49.08 s**
  (439 C++ including three io_uring-specific entries + seven Python). Zero
  failures/skips. Host OFF build is not a TCP cache-traffic test.
- Independent native review audited all final twelve raw cases: **229342 events,
  77796 request rows, 2198 periodic samples**, checked payload hashes, identities,
  generation/order, actual ACK ages, prefix/refill denominators, cohort/enrollment
  summaries, native per-size/object/byte counters, raw Prometheus and snapshot
  hashes/PID/starttimes. This is not an independent implementation of production
  victim selection or a blind formal A/B acceptor.
- No fresh full-repository CTest, sanitizer, physical-HCA/cross-host, GPU or engine
  performance claim. GPU/inference-engine/TTFT are user-excluded, not pending
  blockers. Python AST/compile and documentation/static checks are recorded in
  the final evidence manifest; ruff/mypy were unavailable, not claimed passed.

### Exact test entry points

From the repository root (use absolute build paths when running elsewhere):

```bash
cmake --build build-test -j2
(cd test/python && python3 -m unittest test_prefix_delayed test_prefix_delayed_runner test_prefix_mixed_runner -v)
ctest --test-dir build-test --output-on-failure --no-tests=error \
  -R '^(python_prefix_(chain|dynamic|rdma|rdma_runner|delayed|delayed_runner|mixed_runner)|SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)'
(cd test/python && python3 -m unittest test_prefix_chain test_prefix_dynamic test_prefix_rdma test_prefix_rdma_runner test_long_residence test_long_residence_runner test_eviction_trace test_eviction_replay test_benefit_sensitivity -v)
```

The guest used the following eleven-module command (not the host sensitivity
suite), recorded in `guest-tests-r2.py`, plus the same CTest selector with build
path `/opt/dfkv-build`:

```bash
(cd /opt/dfkv/test/python && python3 -m unittest test_prefix_chain test_prefix_dynamic test_prefix_rdma test_prefix_rdma_runner test_prefix_delayed test_prefix_delayed_runner test_prefix_mixed_runner test_long_residence test_long_residence_runner test_eviction_trace test_eviction_replay -v)
```

Exact commands, XML, logs and per-stage watchdog results are retained under the
evidence root, including original failed runs; do not quote only successful logs.

## Repeat safely

First establish a fresh isolated guest and host backing/process/thermal proof;
see [quick validation environment](DISK_LIFETIME_QUICK_VALIDATION.md). This guest
controller does not create host authority, provision mounts or enforce an external
thermal limit. Do not reuse this run's PID, monitor or tag as permission.
The guest must have marker `/etc/dfkv-validation-guest`, distinct ext4 data roots,
active RXE `rxe0`, io_uring enabled, rebuilt matching RDMA/uring-ON binaries,
unlimited memlock and free ports 12000/12001/12010. No TCP fallback.

After those prerequisites, inside the verified guest:

```bash
cd /opt/dfkv
BUILD=/opt/dfkv-build
TAG=$(date -u +%Y%m%dT%H%M%SZ)
# Full six-case route: default1800s, delays45/185/600, due_first=True.
sudo -n prlimit --memlock=unlimited -- python3 deploy/validation/run_prefix_mixed.py \
  --build "$BUILD" --tag "$TAG-cp" --route class-pressure \
  --data-root /data/disk1/cases --data-root /data/disk2/cases --data-root /data/disk3/cases \
  --output "/data/disk1/results/prefix-mixed-$TAG-class-pressure"
# Run only after the previous matrix passes; keep a different fresh tag/directories.
sudo -n prlimit --memlock=unlimited -- python3 deploy/validation/run_prefix_mixed.py \
  --build "$BUILD" --tag "$TAG-wm" --route watermark \
  --data-root /data/disk1/cases --data-root /data/disk2/cases --data-root /data/disk3/cases \
  --output "/data/disk1/results/prefix-mixed-$TAG-watermark"
```

For a correctness-only preflight, use fresh output/tag and add
`--pairs 1 --steps 12 --delays-sec 1 3 7`. Initialization still writes over 6 GiB
of payload, so this is not a twelve-second wall-time promise. Short windows do not
replace the long 45/185/600-second experiment. Both modes fail-stop on timing/native
errors and preserve later cases as unattempted; do not silently retry with easier
settings. The backend-close/process bounds can leave only supervisor logs/plan if
killed before an in-memory ledger is written; missing results are not zero misses.

Every completed case retains `client/report.json`, before/after Prometheus,
`metrics-*.prom`, `metrics-samples.json` (hashes, times, server CPU/RSS), server and
client logs; the route retains sealed `plan.json` and `matrix.json`. Validate each
ledger from any matching source checkout:

```bash
python3 deploy/validation/prefix_delayed.py check --plan PLAN.json --ledger REPORT.json
```

The workload checker deterministically replays its own state machine; it is not
an independent production policy oracle. This run additionally used a separate
stdlib-only raw arithmetic/Prometheus auditor (`audit.py` in evidence) and native
reviewer raw inspection. Preserve raw files to reproduce that independent check.
CPU/RSS and measurement-only progress are derived from retained wholly-contained
sample intervals, not fabricated from cumulative initialization counters.

## Safety closeout and retained files

All owned servers/clients/launchers were reaped, ports rechecked, source339/binary
hashes and all six host backing FDs freshly verified. The existing VM remains
idle; no VM backing was deleted. The exact thermal monitor was normally stopped
and reaped (PID4172480/start423956584, exit0, `/proc` absent).
**3265** raw samples covered only nvme2/nvme0, maximum **42 C**,
maximum gap **11.129076 s**, zero SMART critical warnings/thermal
alarms. This task did not pressure or SMART-sample host disk1/nvme1.

**78** guest cache directories remain, allocated **58730008576 bytes**
(54.696583 GiB) for this round, including failed experiments.
Exact paths and sizes: `retained-data.json`. They are under guest
`/data/disk{1,2,3}/cases/sg-prefix-mixed-20261125T111743Z-97787ac*`.
Host runtime/archive/log location:

```text
/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/prefix-mixed-20261125T111743Z-97787ac
```

Guest volumes are backed by the already-verified host disk2 `data1.raw` and disk3
`data2.raw`/`data3.raw` under `dfkv-validation/20261118t063414z-c9bbdca`. To reclaim
space, remove only the exact task-owned guest directories after a new idle/ownership
check; do not delete active raw/qcow2 backing files. No cleanup was silently done.
Evidence, expanded raw archives, thermal copy, source packets, failed REDs, XML,
commands, safety receipts and hashes are retained at:

```text
/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/prefix-mixed/20261125T111743Z-97787ac
```

## Remaining scope

This closes the declared mixed-size prefix/global-enabled/thirty-minute stability
**diagnostic**. It does not close independent high-concurrency prefix workloads,
chain recovery, parameter/resource boundaries, multi-day aging or formal A/B
business thresholds and release approval. Reuse existing generic functional
coverage rather than re-labeling it as new prefix-chain evidence. No production
strategy/default flip is authorized by these observations.

Related: [delayed-prefix validation](DISK_LIFETIME_PREFIX_DELAYED_VALIDATION.md),
[business acceptance](DISK_LIFETIME_PREFIX_BUSINESS_ACCEPTANCE.md),
[quick multisize validation](DISK_LIFETIME_QUICK_VALIDATION.md).

Next diagnostic: [independent concurrent prefix chains](DISK_LIFETIME_PREFIX_CONCURRENT_VALIDATION.md).
