# Continuous arrival, delayed reuse and existing-hot-set diagnostics

Baseline `1f5ce63`; follows [benefit sensitivity](DISK_LIFETIME_BENEFIT_SENSITIVITY.md).
This is AB7 pre-formal-A/B diagnosis, not a production acceptance or a claim of
universal benefit. Production scoring, default policy, ABI, disk format and RAM
behavior remain unchanged. GPU is excluded. Actual cache traffic is RDMA/RXE
plus io_uring only, without TCP fallback.

## Frozen workload contract

The previous test repeatedly read hot keys but read each initial delayed cohort
only once. This test keeps those original observations separate and adds a
continuous stream of new keys that will each receive one delayed first read.
It is not repeated periodic reading of the same delayed key.

Every one-second step writes64 new objects per namespace, two namespaces.
Three distinct objects (arrival-batch offsets0,1,2) are assigned first-reuse
ACK-age targets45,185,600 seconds. They come from existing arrivals: no extra
new-object PUTs, duplicate identities or changed nominal128-new-objects/s rate.
Each delay therefore enrolls3600 keys across1800 steps. Actual terminal reads
and right-censoring, not enrollment, determine the observed denominator.

The final eligibility cutoff is sampled once before the final due sweep.
Each enrolled key must appear exactly once as a completed read when ACK+delay
is no later than cutoff, or as explicitly right-censored when it is later.
Final-sweep execution cannot extend eligibility. Censored objects are not
misses, failures, hypothetical hits or measured reuse outcomes. Read-start ACK ages
must satisfy target through target+2 seconds; a failed native call or invalid
status fails the case and is never converted into a cache miss.

Misses are refilled and immediately verified. Original delayed reads, delayed
refill ACKs/payload bytes and verification GETs stay separate. Verification hits
are not scheduled business hits. Existing hot reads/refills, initial one-time
45/185/600 cohorts and the formerly-hot cooled group remain separate too.
Initialization, warm-up, pre-aging and verification still enter complete native
GET/PUT/byte/terminal reconciliation, not just business-read denominators.

Four fresh pairs, in preregistered order: P180 lifetime/legacy, P60 legacy/lifetime,
P60 lifetime/legacy, P180 legacy/lifetime. Each uses240-second hot-only pre-aging,
then1800 one-second closed-loop measurement steps.32 hot and32 cooled keys per
namespace,32 per original delayed cohort,6144 initial ACKs per namespace,
4096-byte values, seed0xD1A60051. Nominal12288 class slots under12-GiB logical
capacity mean48MiB resident payload, not12GiB live data or physical-device fill.
Initial ACK counts do not certify simultaneous residency of every prefill key;
empty-store and strict warm/preage/ready hot-read checks establish the initial
state needed by this diagnostic.
RAM off, disk ACK, miss retries0, watermark/background/donor disabled. Only P60
changes `DFKV_SLAB_PROTECT_SEC`; legacy scoring inputs are inactive. Other eight
lifetime settings use production defaults. P affects protection, early-read
reward and retained aging credit, not only one mechanism.

Child budget2160s; pair watchdog4600s. Primary summaries cover the whole
measurement, with hot windows [0,660),[660,1200),[1200,1800) and60-step bins.
Delayed groups have different exposure windows: the600-second stream begins
later and is more heavily censored. Report both per-delay raw counts and actual
hit/read fractions; do not compare hit counts alone or pool cohorts. No invented
business weights, automatic scalar winner or post-hoc outcome retries. Two
opposite-order repeats are diagnostic replication, not formal statistical power
or proof that order effects never occur.

Raw pressure samples are retained, not deleted after outcomes. Each pair is
budgeted for at most2GiB compressed/8GiB expanded evidence,20000 archive members
and128MiB per member, with controlled extraction. Thermal samples cover only
HOST nvme2/nvme0, stopping at65C or any SMART warning; stale monitoring also
stops execution. HOST `/mnt/disk1` and `/dev/nvme1*` remain prohibited. Guest
`/data/disk1` is an isolated virtual disk backed by HOST `/mnt/disk2`, not the
prohibited physical disk.

## Results

All8 fresh cases passed complete ledger/native audits and independent raw
reconstruction:8 distinct server PIDs,24 fresh data directories,18,837 raw Prom
snapshots and the frozen actual start order. The workload, source and parameters stayed frozen; no
outcome-dependent retry occurred. Four pair runtimes were4187.135,4186.868,
4186.188,4191.990 seconds:16752.181 seconds total,4.653 hours excluding host
archive/audit overhead. Source325 and15 controller hashes remained frozen.

Every case enrolled3600 keys per delay, across both namespaces. Actual due/read
denominators were3510/3230/2400 for45/185/600 seconds; censored counts were
90/370/1200 respectively. Complete observed identity sets matched between both
strategies in every pair, with zero unmatched keys. Read-start ACK-age ranges
were45.257–46.234,185.229–186.103 and600.252–601.181 seconds. All original fixed-cohort and continuous delayed first reads had strictly
positive prior measured eviction pressure. No I/O/network/unknown
terminal was treated as a cache miss.

### Scheduled business reads

Every case had115200 scheduled hot reads. All legacy runs hit115200/115200,
with no hot refill. Their continuous groups hit3510/3510 at45 seconds,
0/3230 at185 and0/2400 at600. Lifetime results follow; all denominators include
both namespaces and exclude extra verification reads.

| Pair | Lifetime P and actual pair order | Hot hits / misses | Continuous45 hits /3510 | Continuous185 hits /3230 | Continuous600 hits /2400 |
|---|---|---:|---:|---:|---:|
|0|180, L→G|27737 /87463|2242|713|0|
|1|60, G→L|108336 /6864|3339|103|0|
|2|60, L→G|108452 /6748|3322|68|0|
|3|180, G→L|26103 /89097|2190|669|0|

All8 original fixed45 cohorts hit64/64. Fixed185 cohorts hit64/64 only in the
two P180 lifetime cases; all other fixed185 groups missed64/64. Fixed600 and
cooled groups missed64/64 in all8 cases. These fixed cohorts are not the
continuous streams and must not be silently pooled with them.

All lifetime cases lost all64 distinct hot keys at least once. Repeated hot
misses were87399,6800,6684,89033 respectively; distinct keys and repeated
misses are different measures. All refill verification reads hit, but this
proves immediate refill availability, not later sustained hot retention.

The preregistered hot windows expose late-emerging losses:

| Lifetime run | [0,660),42240 reads: misses | [660,1200),34560 reads: misses | [1200,1800),38400 reads: misses |
|---|---:|---:|---:|
|P180 pair0|29228|27681|30554|
|P60 pair1|0|6642|222|
|P60 pair2|0|5695|1053|
|P180 pair3|30484|27477|31136|

All legacy windows had zero misses. In particular, P60 initially looked
hot-loss-free for660 steps but lost hot reads later. The prior workload's
P60 zero-hot-loss observation does not generalize to this continuous-reuse
mix. Both reversed repeats reproduce the qualitative tradeoff; they are not
proof of statistical significance, exact stability or absence of order effects.

### Refills and complete transport accounting

Every refill ACK writes4096 application payload bytes and has one separately
counted verification GET. This is application write traffic, not measured NAND
amplification, block-device writes or business regeneration cost.

| Case | Hot refill ACKs | Fixed-first refill ACKs | Continuous refill ACKs | Extra total refill ACKs vs its legacy |
|---|---:|---:|---:|---:|
|P180 lifetime pair0|87463|64|6185|87954|
|P60 lifetime pair1|6864|128|5698|6932|
|P60 lifetime pair2|6748|128|5750|6868|
|P180 lifetime pair3|89097|64|6281|89684|
|Each legacy|0|128|5630|0|

Additional application payload versus the paired legacy was360259584,
28393472,28131328,367345664 bytes respectively. These net deltas include
avoided185-second refills; they are not simply hot-refill totals.

| Case | Full PUT ACKs | Full PUT payload bytes | Full GET hits / misses | Full GET attempts |
|---|---:|---:|---:|---:|
|P180 lifetime pair0|336400|1377894400|140212 /93776|233988|
|P60 lifetime pair1|255378|1046028288|140212 /12754|152966|
|P60 lifetime pair2|255314|1045766144|140212 /12690|152902|
|P180 lifetime pair3|338130|1384980480|140212 /95506|235718|
|Each legacy|248446|1017634816|140212 /5822|146034|

Equal full GET hits140212 are an accounting consequence of extra successful
refill verification replacing missed scheduled hits, **not equal business
benefit**. Initialization/warm/preage/ready and every verification are included
in these native totals. Startup, effective settings, io_uring terminals, bytes,
health and zero watermark/background/donor checks passed in every case.

Actual active-step NEW ACK rates were127.9616–127.9750/s, excluding endogenous
refill traffic. Hot-batch client p99 was17.329–19.204ms for32 keys/batch,
including client preparation/native reads/status/byte checks, not per-key RPC
or server PUT latency. Lifetime class visits/victim were1.6032/2.2952/2.1504/
1.5783 in pair order; legacy lacks this same instrument. These descriptive
costs do not establish throughput/CPU superiority or a formal performance gate.

### Stage conclusion and limits

The sustained stream changes the evidence: lifetime retains some185-second
objects that legacy loses, but also loses hot and45-second reads and generates
more refill traffic. P60 reduces hot loss relative to P180 here, but does not
eliminate it. Cross-P contrasts are sequential and descriptive, not isolated
causal parameter experiments. No600-second advantage appeared. Consequently this round does
not establish overall lifetime superiority, a production parameter recommendation
or a reason to change defaults. It also does not prove that every possible
large, persistent workload cannot benefit.

The offline analysis retains raw per-group denominators/censoring and an
outcome-blind intersection of observed identities for pairwise descriptive
miss deltas. Every intersection happened to equal both full observed sets.
Eligibility can depend on policy/timing in other runs, so this method is not
an unbiased causal estimate. No scalar business weights or winner are supplied;
full refill deltas remain separate from scheduled-read comparisons. Verification
hits are excluded from business groups. Do not reuse earlier fixed-cohort
break-even ratios for this different stream distribution.

Further benefit acceptance requires representative traffic/capacity/reuse
proportions, actual business miss/regeneration costs and the preregistered
formal A/B gates. Blindly extending the same test or selecting only185-second
hits would not resolve that requirement. Candidate-by-candidate victim causality
was not traced here; these ledgers alone cannot identify every eviction cause.

## Reproduction and evidence

Local logic tests do not connect a native cache client:

```bash
cmake --build build-test -j2
ctest --test-dir build-test -R '^(SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
python3 -m unittest discover -s test/python -p 'test_long_residence*.py' -v
python3 -m unittest discover -s test/python -p test_benefit_sensitivity.py -v
```

The guest runner's new opt-in flag is `--continuous-reuse`; omit it to retain
the old workload and report schema. The frozen runs also supplied
`--profile default --pressure-case high` (or `p60`), `--pressure-steps 1800`, `--hot-refill`,
`--pairs 1` and `--start-pair 0` or `1`. The checked `guest-launch.py` sets
unlimited memlock before invoking the runner. A repeat requires a new tag,
fresh directories and a new evidence/monitor identity, not overwriting this
round's data or reusing its stop file. The externally guarded matrix order is
recorded in `frozen-plan.json`; host `watch.py` supervises every pair.

Native guest runs additionally require a proven isolated VM, fresh data paths,
source/build hashes, effective-startup checks, active HOST thermal supervision
and a process timeout. The exact frozen plan, commands and results are retained
in the round-specific evidence directory, resolved by:

```
/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/continuous-reuse-current-path.txt
```

The existing fixed-cohort `benefit_sensitivity.py` rejects continuous-reuse
reports instead of silently ignoring their new business reads or applying its
fixed-denominator cost model. Actual business costs and formal A/B gates remain
separate. This experiment does not validate day-scale default aging, physical
HCA/cross-host behavior, production traffic, full-device pressure or GPU.


## Validation and safe closeout

Fresh host build `build-test` (RelWithDebInfo, RDMA/uring OFF) passed; the
focused family CTest selection passed436/436,5.15s. This build does not claim
actual RDMA validation. Residence tests passed77/77,8.703s; fixed-cohort
sensitivity tests25/25,0.596s. The293 explicitly selected native-free related
Python regressions also passed with no skips. AST checks covered all6 modified
Python files, and diff checks passed. No new C++ production code was changed;
no new full-repository, sanitizer or physical-HCA/GPU pass is claimed. Ruff and
mypy are unavailable.

Guest `/opt/dfkv-build` is a fresh-verified Debug RDMA/uring ON build:200 Python
test methods (six CTest entries plus the25-method sensitivity module) and19
actual C++ candidate-selection tests passed with no failures/skips. The two
production binary hashes stayed identical to the baseline. Fourteen new tests
(13 residence/runner,1 sensitivity guard) entered the existing test discovery.
Real RED/GREEN logs cover the new mode and unsupported-cost-model omission;
advancing-clock/cutoff regressions also passed. Fake clients do not constitute
hardware proof. An independent reviewer reconstructed the8 native raw ledgers and Prom
snapshots with stdlib/Decimal, without importing the workload/auditor helpers.

Thermal supervision retained1739 samples including preflight;1734 were in the
active monitor interval. Maximum temperature41C, SMART warnings0, maximum active
sample gap10.518s. A pre-load monitor launch with `-u` was rejected by the strict
argv identity guard; that exact monitor was normally stopped/reaped and relaunched
without `-u` before native execution. No native case was retried or discarded.
Only nvme2/nvme0 were sampled; HOST disk1 was neither pressured nor sampled.

Postflight verified original VM six-FD backing identity, unchanged source/binaries,
RXE active/io_uring enabled, no owned server/client/launcher and free ports
12000/12001/12010. The active monitor was then normally stopped and reaped;
its PID is absent. No current load/monitor remains. The original VM is idle,
not destroyed.24 cache directories allocate410951680 bytes (391.914MiB).
Exact paths are in `retained-data.json`; guest paths have the form:

```
/data/disk{1,2,3}/cases/sg-long-20261125T014751Z-1f5ce63-p{0,1,2,3}-*
```

This is a descriptive glob, not a deletion command. Guest results are under
`/data/disk1/results/continuous-20261125T014751Z-1f5ce63-p{0,1,2,3}`.
Host control/thermal/compressed archives are under:

```
/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/continuous-reuse-20261125T014751Z-1f5ce63
```

Expanded raw evidence, frozen plan/source, tests, command logs and reviews are:

```
/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/continuous-reuse/20261125T014751Z-1f5ce63
```

Do not delete in-use VM raw/qcow2 backing images: they contain retained results
from this and earlier rounds. No host networking/kernel change, deployment,
production default change, commit or push is part of this round.
