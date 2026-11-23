# Default-score residence and cross-turnover first reuse

This diagnostic follows the [accelerated specialty](DISK_LIFETIME_LONG_RESIDENCE.md). It does not change the production policy default, implement a new eviction algorithm, authorize deployment, or constitute formal A/B acceptance.

## Frozen contract (before native results)

Reuse `long_residence.py --profile default` and `run_long_residence.py --profile default` without changing their behavior. Keep the previous workload's capacity, seed, arrival batch size, payload size, namespaces and cohort layout. Change only the already-defined profile: default lifetime scoring, longer pre-aging/measurement, and 45/185/600-second first reuse.

| Item | Frozen value |
|---|---|
| Source baseline | e9e929cb44f3ccb1a8adec643bc3830e456789b7 |
| Transport / disk path | RXE/RDMA, kernel io_uring, buffered slab, disk PUT ACK, RAM tier off |
| Capacity | 12 GiB logical configuration, three 4-GiB stores; nominal 12,288 slots |
| Initial writes | 12,288 acknowledged objects, not a guarantee of simultaneous residence |
| Payload / namespaces | 4,096 bytes; two deterministic namespaces |
| Arrival schedule | 64 new objects per namespace per one-second closed-loop step: nominal 128/sec |
| Pre-aging | 240 seconds; keep hot reads active, leave previously hot cohort unread |
| Measurement | 1,800 steps, nominal 1,800 seconds |
| Measurement arrivals | 230,400 objects, 18.75 nominal capacity turnovers |
| Cohorts per namespace | 32 continuous-hot, 32 formerly-hot/cooled, 32 for each of three first-reuse delays |
| First reuse | 45 / 185 / 600 seconds after client-observed batch PUT ACK |
| First-reuse lateness | At most two seconds; 45-second cohort must remain inside P |
| New-write protection P | Default 180 seconds |
| Score defaults | BASE604800, BONUS302400, MAX_READS2, SLOW4, BOOST1209600, KEEP475200, G43200, B24 |
| Eviction scope | Class-capacity pressure only; watermark/reclaim disabled, donor activity rejected |
| Pair order | legacy→lifetime, lifetime→legacy, legacy→lifetime; six fresh samples |
| Child / pair watchdog | 2,160 / 4,600 seconds, with continuous thermal safeguards |

No score overrides are set for this profile. Effective values must be read back from server startup; legacy reports inactive lifetime fields. Zero watermarks/reclaim are explicit diagnostic overrides, so this is not a claim that all production settings are unchanged. No production implementation/ABI/disk-format/default-policy change is involved.

Nominal aggregate turnover is `12288 / 128 = 96 seconds`; the 185/600-second cohorts cross that nominal boundary. Hash/disk-local class imbalance can evict while other disks still have free slots, so this is not a per-key residence guarantee. The actual pre-first-reuse eviction checkpoints, ACK ages and full denominators remain mandatory.

The protection-pressure indicator `arrival_rate * P / capacity` is **1.875**, versus 0.125 in the accelerated run. Thus new arrivals over one protection window exceed nominal capacity. This intentionally preserves a high-arrival regime for comparison instead of lowering the rate after seeing prior hotspot losses. It is not a business-representative distribution or a promise that lifetime will win. Pre-aging 240 seconds exceeds P but does not reproduce seven-/fourteen-day score aging.

## Acceptance and reporting

Initialization/pre-aging/ready-state checks must be complete and byte-correct. All planned writes and reads must settle with validated states; only explicit NOT_FOUND counts as a miss. Require actual owned-server foreground eviction before each first-reuse event, RDMA/io_uring paths, healthy storage, no fallback, bounded timing and correctly reaped children/servers. Fail-stop on correctness/infrastructure failure; never retry merely to replace unfavorable policy outcomes.

Measure continuous-hot hits/misses, cooled final outcomes, first-hit/miss for each age, conditional first-miss refill ACKs/extra payload bytes/verification, eviction origins and all complete denominators separately. Hot misses are repeated read outcomes, not distinct evicted-key counts. First-miss refill is enabled by this fixed workload; continuous-hot misses are not refilled. Do not sum a made-up business-weight benefit score or call payload writes NAND write amplification.

All three alternating pairs must be retained. Alternation can reveal observed order differences but is not proof of statistical independence or absence of all cache/order bias on one host. No prescribed policy-benefit winner threshold is introduced.

## Safety and reproducibility

Only HOST `/mnt/disk2` and `/mnt/disk3` are allowed for test data. HOST `/mnt/disk1` remains prohibited; GPU is excluded. Reuse the authorized disposable guest only after fresh PID/start/namespace/six-open-backing-file proof. Guest `/data/disk1` is backed by HOST disk2; guest `/data/disk2` and `/data/disk3` by HOST disk3. Guest naming does not authorize use of the banned host disk.

Use a fresh watchdog: stop at any allowed-device sensor >=65°C, any SMART warning, invalid reading or sample age >=25 seconds. Stop only the verified owned workload/guest; do not broadly kill services or delete in-use raw/qcow2 images. Freeze source, tools, binaries, configuration and plan before writes; preserve original raw reports, metrics and process statuses. SSH/HTTP metrics/RDMA bootstrap are control traffic, not TCP cache-data fallback.

Inside the authorized guest root shell, with the host safety monitor active, run each pair serially with the same frozen tag and distinct outputs:

```bash
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/run_long_residence.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases \
  --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/default-NEW_TAG-p0 \
  --tag NEW_TAG --profile default --pairs 1 --start-pair 0
```

Then use pair/output suffix 1 and 2. Each sample schedules 34 minutes of phases; all six take about 3.4 hours plus setup/audit. A watchdog suitable for the accelerated run is too short and must not be silently reused.

Pure contract/regression tests (no native server):

```bash
python3 -m unittest discover -s test/python -p 'test_long_residence*.py' -v
```

## Actual result (2026-11-23)

All six default-profile samples completed in frozen AB / BA / AB order, with six distinct server processes and 18 fresh case directories. Pair durations were 4,095.830 / 4,095.262 / 4,095.403 seconds: approximately **3.413 hours** of paired execution, excluding preparation/tests/archive/audit. Per-sample pre-aging was 240.0449–240.0484 seconds and measurement 1,800.3675–1,800.3767 seconds. No shortened window, outcome-based retry, score override or workload change was substituted.

### Continuous-hot results

Each row contains the same 115,200 planned measurement hot GETs per policy. Initialization/pre-aging/ready reads are not included here.

| Pair / order | legacy hits / misses | lifetime hits / misses |
|---|---:|---:|
| 0: legacy → lifetime | 115,200 / 0 | 6,001 / 109,199 |
| 1: lifetime → legacy | 115,200 / 0 | 6,020 / 109,180 |
| 2: legacy → lifetime | 115,200 / 0 | 6,008 / 109,192 |
| Sum of three samples per policy | 345,600 / 0 | 18,029 / 327,571 |

There are 64 hot keys per sample, repeatedly read without miss refill. These miss counts are **not counts of distinct victims**. The 19-hit variation among lifetime samples does not change the observed tradeoff, but the six samples cannot prove absence of all order effects or physical-cache bias.

### First reuse, refill and eviction

The following sums cover three samples per policy, 192 first reads for each delay:

| First delay | legacy hits / misses | lifetime hits / misses |
|---|---:|---:|
| 45 seconds | 192 / 0 | 192 / 0 |
| 185 seconds | 0 / 192 | 192 / 0 |
| 600 seconds | 0 / 192 | 192 / 0 |

Actual ages across all six samples were 45.6391–45.7201, 185.6587–185.8149 and 600.7305–600.8904 seconds. Every first event followed positive measured eviction progress; the minimum early-event delta was 5,611. Both policies' initialization/pre-aging/ready reads all hit; final cooled reads were 0 hits / 64 misses per sample.

- legacy: 128 first-miss refill ACKs per sample, 524,288 extra payload bytes, and 128 successful verification GETs. Across three samples: 384 ACKs / 1,572,864 bytes / 384 verification hits.
- lifetime: zero first-miss refills or verification GETs. This is an observed late-first-reuse advantage, not a statement of overall policy benefit.
- Per-sample acknowledged PUTs/payload: legacy 242,816 / 994,574,336 bytes; lifetime 242,688 / 994,050,048 bytes. Foreground evictions: 230,528 / 230,400 respectively. The extra 128 legacy evictions accompany its 128 refill writes. Watermark, background and donor activity were zero throughout.
- Full GET counts, including initialization/pre-aging/first reuse/cooled/verification: legacy 131,264 per sample; lifetime 131,136. Verification accounts for the difference; compare the fixed hot and first-read denominators above instead of silently mixing them.
- Successful nonempty io_uring PULLs were 131,072 per legacy sample and 21,873 / 21,892 / 21,880 for lifetime. Terminal not-found counts were 192 per legacy sample and 109,263 / 109,244 / 109,256 for lifetime. Native status/byte checks and server counters agreed; errors were not counted as misses.

**Correctness passed; overall policy benefit was not established.** lifetime retained these late-first-reuse cohorts while losing most continuous-hot probes; legacy preserved the hot cohort and paid the late-cohort refill cost. Business weights/recompute costs/online refill behavior are not supplied, so no aggregate winner is invented. This high-arrival, no-hot-refill diagnostic is not an online workload or a reason to switch the production default. It neither erases the earlier hotspot-loss evidence nor proves the algorithm is universally worse.

### Selection interpretation and remaining risk

P=180 does **not** mean an unread object remains under its new-write BOOST at 185/600 seconds. The score implementation removes BOOST after P and retains the initial slow-aging credit; at mathematical age 600 with no reads its default score is `604800 - (600-180+180/4) = 604335`. A hypothetically resident, just-refreshed capped-read hot object scores approximately 1,209,600. The actual hot objects were already missing at the 600-second probes, so this is an explanation of score semantics, not a measured victim comparison.

Class selection is bounded: `src/cache/slab_allocator.cc` keeps anchor-sorted queues (`LinkLifetimeLocked`, lines 616–649), starts each band from the shared cursor rather than necessarily its physical head (1134–1139), advances cursors for inspected candidates (1156–1161), and compares each band's first eligible inspected candidate (1177–1190). New appends do not reset a nonempty cursor. Therefore a lower-score old prefix may not be among the compared candidates under continuous insertion. It is incorrect to infer that the retained late cohort must have had a higher score than every evicted hot object, or that the selector finds a global minimum.

The source behavior permits a bounded-candidate/selection-quality explanation, consistent with the specification's checked-candidate limit. **No per-victim trajectory was captured**, so this round does not prove that cursor behavior caused each observed outcome or establish a specification defect. `candidate-selection-note.md` in the evidence directory separates source facts from inference. A deterministic old-prefix/cursor-progress fixture is a recommended separate next step; no implementation or load was changed mid-run to force a winner.

Compared with the accelerated run, selecting the pre-existing default profile changes score inputs, time windows and phase durations; it is not a single-factor P-only experiment and does not modify production defaults. Default bucket/day-scale score aging, real physical HCA/cross-host behavior, online business trace weights and formal A/B signoff remain outside this validation.

## Verification and cleanup

Only two test files were modified, adding six contract regressions; the workload/runner/algorithm behavior is unchanged. The new full-default fake-clock test runs the complete schedule using a stateless adapter and tiny mocked payloads to avoid allocating approximately 994 MB; it proves schedule/denominator logic, **not native bytes or hardware behavior**. The six actual RDMA samples supply the independent native byte/status/path evidence.

Actual test/build commands:

```bash
# Repository root: 41 methods, including the six new contract cases.
python3 -m unittest discover -s test/python -p 'test_long_residence*.py' -v

# Inside the isolated guest: build succeeds; unchanged C++ means Ninja has no work.
cmake --build /opt/dfkv-build -j2
ctest --test-dir /opt/dfkv-build \
  -R '^(python_eviction_scheduler_backend|python_eviction_arrival|python_rdma_arrival_runner|python_long_residence|python_long_residence_runner|python_quick_eviction)$' \
  --no-tests=error --output-on-failure \
  --output-junit /data/disk1/results/default-tests-20261123T055657Z-e9e929c/ctest.xml
```

- Host contract tests: 41 passed; explicit related pure whitelist: 293 passed; zero failures/skips. No native TCP cache-data tests selected.
- Guest `/opt/dfkv-build`: Debug, RDMA/URING ON; successful build/recheck with unchanged server/library hashes. Six targeted CTest entries / 139 methods passed, zero failures/skips. Not a full CTest, sanitizer, GPU, physical-HCA or performance acceptance run.
- Frozen-plan SHA: `b31a45b2e926a2207f6e85b5d069c4463ad58f236e4392a003e5a0a08f1ef4e9`. Source snapshot has 323 inputs. Independent audits bind the original pre-run contract, plan, source/tools/binaries, fresh directories/order, ACK ages, full ledgers, raw pressure/metrics, native exits and cleanup.
- The owned monitor finished normally and was reaped after owned native processes ended and ports were released. 1,230 thermal samples had zero SMART warnings; nvme2/nvme0 each peaked at 41°C, maximum sample gap 10.525 seconds. HOST `/mnt/disk1` was neither loaded nor sampled.
- AST/bytecode, frozen-source identity, local document links and whitespace checks passed. Ruff/Pyflakes/Mypy were not installed; no full lint/type-analysis claim is made.

Master evidence:
`/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/default-residence/20261123T055657Z-e9e929c/`

It contains the immutable pre-run `frozen-plan.json`/SHA and `frozen-contract.md`, `source.tar`/`source.sha256`, all three raw pair archives, original client/metric reports, tests/JUnit, independent audits, candidate-selection note, thermal samples and source/backing/process postflight proofs.

Retained guest data:
`/data/disk{1,2,3}/cases/sg-long-20261123T055657Z-e9e929c-*`

These 18 directories use approximately **293.91 MiB allocated**, not their sparse logical capacity. Exact paths/sizes are in `retained-data.json`. Results are `/data/disk1/results/default-20261123T055657Z-e9e929c-p{0,1,2}`; host control/archive files are:
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/default-residence-20261123T055657Z-e9e929c/`

Guest data1.raw is backed by HOST `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/`; data2.raw/data3.raw by HOST `/mnt/disk3/dfkv-validation/20261118t063414z-c9bbdca/`. Delete only enumerated guest case directories if cleanup is later desired; do not delete in-use raw/qcow2 images containing earlier rounds. No native workload or monitor remains running; the existing idle guest is retained. Production defaults and formal A/B status remain unchanged.
