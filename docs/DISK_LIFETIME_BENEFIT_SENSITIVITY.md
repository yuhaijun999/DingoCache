# Lifetime benefit sensitivity and reversed long-window diagnostics

Baseline `fe7a3be`; follows [score-pressure diagnostics](DISK_LIFETIME_SCORE_PRESSURE.md).
This is pre-formal-A/B diagnosis, not a production recommendation. Production
scoring, default policy, ABI, disk format and RAM behavior are unchanged. GPU is
excluded. Cache data traffic uses RDMA/RXE plus io_uring, without TCP fallback.

## Preregistered conditions

Four fresh pairs: P180 lifetime/legacy, P60 legacy/lifetime, P60 lifetime/legacy,
P180 legacy/lifetime. Each uses 240-second hot-only pre-aging, then 1800
one-second closed-loop measurement steps, with first reuse at client-ACK ages
45/185/600 seconds (at most two seconds late). All use hot-miss refill and a
separately counted verification read. Seed, distribution, class geometry and
other eight scoring parameters are fixed. P is a single-parameter change, not
single-mechanism: protection, early-first-read reward and retained aging credit
all depend on it.

Two namespaces, initial6144 ACKs per namespace,32 hot and32 cooled keys per
namespace,32 per delayed cohort,4096-byte values.128 nominal new objects/s,
nominal12288 slots under12-GiB logical capacity. Nominal resident payload is
48MiB, not12GiB live data or full-device pressure. RAM off, disk ACK, miss
retries0, watermark/background/donor disabled. Only P60 overrides
`DFKV_SLAB_PROTECT_SEC`; legacy scoring inputs are inactive.

The exact plan,323-file native source snapshot, effective binary hashes,
commands, independent audits and raw ledgers are retained under:

```
/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/benefit-reversal/20261124T085236Z-fe7a3be
```

All outcomes are retained; no outcome-dependent retries or changes. Child
budget2160s, pair watchdog4600s. Whole1800 comparisons are primary. Scheduled
hot windows [0,660),[660,1200),[1200,1800) and60-step bins describe evolution;
they are not independent experimental replicates. The initial660 hot prefix
is historical context only: cooled keys are read at a different final age,
so their1800 endpoint is not a matched660 endpoint.

## Cost model and limits

The offline tool validates complete client ledgers before producing coefficients.
It does not replace native startup, server metrics, thermal or identity audits.
Business groups stay separate: scheduled hot, first45/185/600 and cooled.
Warm/preage/ready establish initial state; verification hits are not scheduled
business hits. Repeated hot misses and distinct lost keys are different measures.

For each cost category, savings coefficient = baseline cost-event count minus
candidate count. Positive means the candidate avoids events. Hypothetical net
savings are the sum of explicitly supplied nonnegative cost weights times these
coefficients. No actual business weights or automatic production winner exist.
Weights share an operator-chosen cost unit, per scheduled miss/group, refill ACK,
refill payload byte and verification read. Avoid double-counting: an inclusive
miss cost requires zero additional refill/verification weights. At fixed4096B,
ACK and payload coefficients are collinear; they are alternative/accounting
assumptions, not independent measurements of device cost. Application bytes
are not NAND amplification. Break-even thresholds apply only to this workload's
cohort proportions and finite measurement window, not a production threshold.

## Execution results

All8 fresh cases passed complete raw-ledger and native audits, with8 distinct
server PIDs,24 fresh directories and the frozen actual start order. Independent
review recounted raw rows/Prometheus snapshots without importing the diagnostic
helpers. Four pairs took16405.161 seconds total (4.557 hours), with no failed
case, outcome-based retry, source change or parameter adjustment. Actual first
ages were45.647–45.713,185.663–185.824 and600.738–600.920 seconds.

Each row below has115200 scheduled hot reads and64 first reads per delay.
All8 first45 groups hit64/64. All first600 groups missed64/64. Legacy hot
reads hit115200/115200 in every run; legacy cooled groups missed64/64.

| Pair | Lifetime P/order | Lifetime hot hits/misses | First185 hits L/G | Cooled hits L/G | Lifetime hot refill ACKs/payload bytes |
|---|---|---:|---:|---:|---:|
|0|180, L→G|28428 /86772|64 /0|0 /0|86772 /355418112|
|1|60, G→L|115200 /0|0 /0|64 /0|0 /0|
|2|60, L→G|115200 /0|0 /0|64 /0|0 /0|
|3|180, G→L|28384 /86816|64 /0|0 /0|86816 /355598336|

P180 lost all64 distinct hot keys, with86708/86752 repeated misses respectively.
Verification after each refill hit, but later scheduled hot outcomes after a
key's first refill were22417hit/86708miss and22398hit/86752miss. Immediate
refill success therefore does not mean sustained retention. P60 and legacy had
no hot losses/refills. Delayed-first-miss refills are separate:64 for P180,
128 for P60 and legacy,4096 bytes and one verification per ACK.

Predeclared scheduled-hot windows make the extended observation explicit:

| Lifetime run | [0,660),42240 reads: misses | [660,1200),34560 reads: misses | [1200,1800),38400 reads: misses |
|---|---:|---:|---:|
|P180 pair0|29492|27731|29549|
|P60 pair1|0|0|0|
|P60 pair2|0|0|0|
|P180 pair3|29870|27632|29314|

All corresponding legacy windows had zero misses. The P180 hot loss persists
past600 and1200 seconds; it is not only the first660 seconds in these tests.
P60's observed hot retention also persists across both longer repeats. Two
reversed repeats establish directional consistency here, not statistical
significance, no order effect in general, or steady state.

Full transport accounting retains initialization, pre-aging and verification:

| Case | PUT ACKs | PUT payload bytes | GET attempts | Full GET hits/misses |
|---|---:|---:|---:|---:|
|P180 lifetime pair0|329524|1349730304|217972|131072 /86900|
|P180 lifetime pair3|329568|1349910528|218016|131072 /86944|
|P60 lifetime, each|242816|994574336|131264|131136 /128|
|legacy, each|242816|994574336|131264|131072 /192|

Equal aggregate131072 hits for P180 and legacy are an extra-verification
artifact, not equal business benefit. No network/I/O/unknown status was
converted into a cache miss. All cases passed byte, io_uring terminal-count,
startup and health checks, with zero watermark/background/donor evictions.

Nominal new arrivals were128/s; actual active-step ACK rates were127.9739–
127.9751/s, excluding extra refill traffic. Hot-batch client p99 ranged
17.479–19.585ms (32 keys/batch), including preparation/native reads/status and
byte verification, not per-key RPC or server PUT p99. Lifetime class visits per
victim were1.45668/1.45690 for P180 and2 for P60; allocator allocate-path mean
held times37.38/37.79us versus31.11/30.19us. Those diagnostic aggregates do not
prove throughput superiority: victim counts differ, legacy lacks these same
lifetime lock instruments, and allocator-lock time is not the whole store lock
or request latency. Complete counters are retained in the audit.

## Explicit hypothetical break-even, not invented business value

Let `H` be cost of a scheduled hot miss, `D` cost of a first185 miss, `W` an
incremental refill ACK cost, `B` cost per refill payload byte, and `V` a
verification-read cost. All are nonnegative in a common chosen cost unit.
Costs must be disjoint; use W=B=V=0 if H/D already include all regeneration,
write and verification costs. First45/600 and cooled outcomes are identical
for P180 and its legacy baseline, so their coefficients are zero here.

For P180 versus its own legacy pair, hypothetical savings are exactly:

```
pair0: 64*D - 86772*H - 86708*W - 355155968*B - 86708*V
pair3: 64*D - 86816*H - 86752*W - 355336192*B - 86752*V
```

The net write deltas subtract64 delayed refills avoided by P180; they are not
simply the hot-refill totals. Fixed4096-byte payload makes ACK and byte
coefficients collinear. If only H=1 and other fixed costs are explicitly0,
P180 needs D>1355.8125 or D>1356.5 respectively to save hypothetical cost;
equality is a tie. This is a workload-specific ratio of repeated hot misses
to64 avoided first185 misses, **not an estimate of real D/H or a production
parameter threshold**. With the itemized, still hypothetical H=W=V=1,B=0,
thresholds instead become4065.4375/4067.5. These are examples of assumptions,
not measurements of equal event costs. The old660 point's459.0625 threshold
is historical only: extending the hot observation window changes the total
hot exposure while first185 is still read only once/key. Do not pool them.

For P60 versus its legacy baseline, only cooled differs:64 fewer misses per
run, all other listed business/refill coefficients zero. Consequently a model
that deliberately assigns cooled cost0 ties; positive cooled cost favors P60
within these observed categories. This does not include unmeasured CPU,
request-tail or real business costs and is not universal dominance.

Two additional descriptive contrasts compare lifetime P60 to lifetime P180
(pair1 vs0, pair2 vs3). P60 avoids86772/86816 hot misses,64 cooled misses and
86708/86752 net refills, but adds64 first185 misses. They are sequential
candidate comparisons, not new randomized/causal matched experiments. Both
full paths, raw hashes, coefficients and hypothetical fixed-weight maps are
retained in `benefit-analysis.json`. No scalar winner exists without explicit
weights; the tool uses exact rational arithmetic, with exact rational
break-even authoritative and rounded50-digit threshold display only.

## Reproduction

No new dependencies are required; the Python tests also enter the existing
Makefile unittest discovery. Local tests (no native cache transport):

```bash
cmake --build build-test -j2
ctest --test-dir build-test -R '^(SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
python3 -m unittest discover -s test/python -p 'test_long_residence*.py' -v
python3 -m unittest discover -s test/python -p test_benefit_sensitivity.py -v
```

After freshly proving isolated guest backing, correct build, RXE/io_uring,
free ports, fresh paths and a live HOST thermal watchdog, run INSIDE that guest:

```bash
sudo -n python3 /opt/dfkv/deploy/validation/run_long_residence.py \
  --build /opt/dfkv-build --profile default --pressure-case high \
  --pressure-steps 1800 --hot-refill --pairs 1 --start-pair 1 \
  --tag UNIQUE_TAG --output /data/disk1/results/UNIQUE_OUTPUT \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases
```

Choose pressure-case p60 for P60; start-pair0 is legacy/lifetime,1 is
lifetime/legacy. Use fresh tag/output for every pair. The explicit option
accepts only660/1800 and requires a pressure preset; omitting it preserves
prior behavior. The runner is not a replacement for the HOST guard.
Guest `/data/disk1` is backed by HOST `/mnt/disk2`, not prohibited HOST
`/mnt/disk1`. Stop on any allowed NVMe sensor>=65C, critical SMART warning,
invalid/stale>=25s sample, identity mismatch, correctness failure or timeout.

Offline analysis from two validated client reports, with a fresh output path:

```bash
python3 deploy/validation/benefit_sensitivity.py \
  --baseline /ABS/legacy/client/report.json \
  --candidate /ABS/lifetime/client/report.json --output /ABS/NEW-analysis.json
```

Without `--weights`, this emits coefficients only. For a threshold, provide a
JSON file containing every fixed weight explicitly, omitting only the named
variable. Example assumes hot1, all other fixed categories0:

```json
{"hot_miss":1,"cooled_miss":0,"first_45_miss":0,"first_600_miss":0,
 "refill_ack":0,"refill_payload_byte":0,"verify_read":0}
```

Add `--weights /ABS/weights.json --break-even first_185_miss` and use another
fresh output. No existing output or input report is overwritten. Invalid
ledgers, mismatched workloads, missing/extra/negative/nonfinite weights and
unbounded arithmetic inputs are rejected. Zero or negative benefit coefficients
are handled without inventing ratios or division-by-zero results.

## Validation and safe closeout

Fresh HOST full build passed;436 selected CTest cases,64 residence tests,
24 offline sensitivity tests and293 explicitly selected native-free related
regressions passed, with zero failures/skips. Guest RDMA/io_uring ON Debug full
build passed; six selected Python CTest entries covered162 methods, and19
actual C++ candidate tests passed. The offline sensitivity tool is HOST-only
and separately SHA-frozen; it is not part of the323-file native snapshot and
is not claimed as a guest hardware test. Test mocks validate accounting and
process contracts, not hardware or strategy benefit. New tests include true
RED/GREEN for unsupported window, arithmetic cancellation/display precision
and refusal to overwrite raw ledgers. No new C++ production or concurrency
code changed; fresh sanitizer/full-repository CTest runs are not claimed.
AST/diff checks passed. Ruff/mypy are not installed; no such check is claimed.

The HOST guard sampled1647 times, highest41C, all SMART critical warnings0,
maximum sample gap10.774s. It sampled only nvme2/nvme0, never nvme1. After all
owned server/client/launcher processes exited and ports12000/12001/12010 were
free, native source/binary hashes and the original VM's six backing-file
identities were rechecked. The monitor stopped normally, exit0 and reaped;
PID3083144 is absent. No current owned load/monitor remains; original VM is
idle, with its original rxe0 and io_uring settings. No HOST network/kernel or
GPU operations were performed.

Retained24 guest case directories total410959872 allocated bytes (391.922MiB):

```
/data/disk{1,2,3}/cases/sg-long-20261124T085236Z-fe7a3be-p{0,1,2,3}-*
```

This is a descriptive glob, not a deletion command; exact paths/sizes are in
`retained-data.json`. Guest results are under
`/data/disk1/results/benefit-20261124T085236Z-fe7a3be-p{0,1,2,3}`. HOST control
and result archives are under
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/benefit-reversal-20261124T085236Z-fe7a3be`.
Keep in-use VM qcow2/raw images: they also contain earlier rounds' evidence.

## Phase conclusion and remaining work

AB7 pre-formal-A/B diagnostic substep is complete. Within this high-arrival
synthetic class-pressure workload, reversed1800-second repeats preserve the
observed direction: P180 protects first185 reuse but repeatedly loses hot
objects; P60 keeps tested hot/cooled objects but does not protect first185 or
600. No tested candidate solves both goals here. This extends the short-window
evidence; it is not a universal algorithm defect or large-persistent-data win.

Keep production default/formula unchanged. P60 remains a candidate only for
further hot/short-reuse validation, not an automatic new default. If business
requires hot residents to beat every armed NEW inside P, that is an explicit
priority-contract choice; legal BOOST/BONUS/MAX_READS tuning does not reverse
the validated ordering. Do not make rate/capacity ratios a startup rejection.

The next useful decision needs actual reuse-arrival distribution and costs,
not another blind parameter sweep. A preregistered continuous delayed-reuse
stream is distinct from this one-time first-read cohort, and remains untested
here. Likewise day-scale/default12-hour buckets, larger live payloads,
production trace/geometry, independent high-concurrency resource costs,
physical HCA/cross-host and full formal A/B workload/gates/owners remain
separate. GPU is explicitly excluded, not an outstanding gate. Two repeats
are not significance; same-agent exploratory tests are not blinded formal A/B.
No deployment, production default change, formal acceptance or push is authorized.
