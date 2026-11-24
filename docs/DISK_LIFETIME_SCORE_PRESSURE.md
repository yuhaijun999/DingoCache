# Lifetime scoring and capacity-pressure diagnostics

This follows the [unadopted candidate-retention experiment](DISK_LIFETIME_CANDIDATE_RETENTION.md).
The baseline is `174ff32`, with the original production selector restored.
Only diagnostic tools/tests change; production scoring, policy defaults, ABI,
format and RAM behavior remain unchanged. This is not formal A/B or deployment
approval. GPU is excluded; cache data traffic is RDMA/RXE with io_uring only.

## A deliberate priority, not only a cursor issue

`ValidateDiskLifetimeConfig` requires:

```
BASE - floor((P - 1) / SLOW) + BOOST > BASE + BONUS * MAX_READS
```

`DiskLifetimeScore` consequently gives every armed, unread NEW resident inside
P a higher score than any already-read resident at a valid monotonic time.
When both are checked, eligible candidates, the minimum-score selector chooses
the read resident. It does not promise to keep a frequently read object ahead
of every untested new write. Adjusting BOOST/BONUS/MAX_READS while preserving
this validation contract cannot reverse that ordering.

Actual C++ score and allocator tests isolate two residents, one per band, and
check both once: at P-1 the hot resident loses; at P/P+1 the expired NEW loses.
This removes candidate-coverage ambiguity from that fixture, not from every
native eviction. Existing non-head/continuous-append diagnostics remain:
unchosen, unchecked colder residents can still exist. Aggregate native counters
are not a per-victim score trace or proof of a single cause.

Default examples, using score seconds rather than actual remaining residence:
NEW at age179 scores1814356, a just-refreshed capped read scores1209600,
NEW at age180 scores604755, and at age600 scores604335.
A successful first read at45 disarms BOOST; n=1 plus fw=1 already reaches the
default band2. It does not create a third uncapped read reward.

Changing P is a **single-parameter**, not single-mechanism experiment: it changes
BOOST expiry, early-first-read eligibility and the slow-aging credit retained
past expiry. At age600, unread NEW scores604245/604290/604335 for P60/120/180.
The same read at100 is early for P120/180 but not P60.

## Frozen representative native matrix

Three stores, 12-GiB logical cap, nominal12288 slots, two namespaces, 4096-byte
values; all points use the same distribution and seed. C is the tested class
geometry's nominal slot budget, not logical bytes divided by4096:12288 such
payloads total48 MiB, not12 GiB of resident payload or full-device pressure.
Initial6144 ACKs per
namespace, 32 hot, 32 cooled and 32 per delayed cohort. Pre-age240 seconds with
hot reads, then660 one-second measurement steps. Delayed cohorts are inserted
AFTER pre-aging and first read at client-ACK ages45/185/600, <=2 seconds late.
Hot misses refill and verify separately. Scheduled misses never become hits
merely because the extra verification succeeds.

| Point | NEW per namespace/step | Nominal NEW/s | P seconds | Nominal lambda*P/C | Fresh policy order |
|---|---:|---:|---:|---:|---|
| low |16|32|180|0.46875|legacy, lifetime|
| medium |32|64|180|0.9375|lifetime, legacy|
| high |64|128|180|1.875|legacy, lifetime|
| p60 |64|128|60|0.625|lifetime, legacy|
| p120 |64|128|120|1.25|legacy, lifetime|

All five points freeze before outcomes; the default-P high baseline precedes
P experiments. Only P60/P120 set `DFKV_SLAB_PROTECT_SEC`; other eight score inputs
stay default. Legacy reports these scoring settings inactive: P/ratio in its
pair is a reference to the paired lifetime configuration, not legacy behavior.
RAM off, disk ACK, miss retries0, background/watermark/donor eviction disabled.
Client deadline1020 seconds, host pair watchdog2400 seconds.

These are closed-loop scheduled steps, not fixed-arrival throughput tests.
Report actual ACK rates and extra endogenous hot-refill traffic separately.
Nominal lambda*P/C describes pressure, **not startup validity, a sharp threshold
at1, or a retention guarantee**. Capacity includes hot/pinned/other residents;
per-disk and per-size imbalance matters. Initial ACKs are not proof that all
initial keys remain resident simultaneously.

At low pressure660 seconds covers1.71875 nominal capacity turnovers and includes
the600-second first reuse with a60-second tail. This is not steady-state,
day-scale residency, default12-hour bucket coverage, or equivalent to earlier
1800-second runs. Each point has one policy pair, without repeated reversal at
the same point: running-order/host noise and significance are not resolved.
Historical long-window results are not fresh matched replicates. No business
weights, universal winner or numeric benefit gate is invented.

## Reproduction and safeguards

Existing entry points retain their behavior unless `--pressure-case` is explicit:

```bash
python3 -m unittest discover -s test/python -p 'test_long_residence*.py' -v
cmake --build build-test -j2
build-test/slab_lifetime_band_scan_test
ctest --test-dir build-test -R '^(SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
```

After proving the isolated guest/backing disks, build/hash, fresh directories,
RXE, io_uring, free ports and a live host thermal guard, run INSIDE that guest:

```bash
sudo -n python3 /opt/dfkv/deploy/validation/run_long_residence.py \
  --build /opt/dfkv-build --profile default --pressure-case low --hot-refill \
  --pairs 1 --start-pair 0 --tag UNIQUE_TAG \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases --output /data/disk1/results/UNIQUE_OUTPUT
```

Use a fresh tag/output for each point; select start-pair1 for reversed order.
The runner does not replace the mandatory HOST watchdog/backing proof. Guest
`/data/disk1` is a virtual disk backed by HOST `/mnt/disk2`, **not permission to
use HOST `/mnt/disk1`**, which remains prohibited. Stop on any allowed NVMe
sensor>=65C, SMART critical warning, invalid/stale>=25-second thermal sample,
identity mismatch, correctness/audit failure or timeout. No TCP data fallback,
no global cache/driver/network changes, no GPU operations, no outcome retries.

## Execution results

All10 fresh cases passed status/payload/configuration/full-denominator audits;
all PIDs were distinct, all30 data directories fresh, and actual server starts
matched the frozen order. Five pairs took9084.303 seconds total (2.523 hours).
No outcome-based retries, point substitutions, source or parameter changes.
Pre-aging was240.0455–240.0499 seconds and measurement660.1438–660.1743 seconds.
Actual ACK-aged first reuse across all cases was45.6439–45.7282,
185.6659–185.8439 and600.7457–600.9624 seconds; all stayed inside the frozen
lateness limits. The45-second cohort remained inside P even for P60.

Each policy/point has42240 scheduled hot GETs, 64 reads per first-reuse group
and64 cooled reads. Every45-second group hit64/64; every600-second group
hit0/64. All initial warm/preage/ready reads hit.

| Point | Lifetime hot hit/miss | Legacy hot hit/miss |185s hits L/G|Cooled hits L/G|Actual NEW/s L/G, active-step window|
|---|---:|---:|---:|---:|---:|
|low|42240/0|42240/0|64/64|64/64|31.99309/31.99358|
|medium|38207/4033|42240/0|49/43|40/0|63.98718/63.98725|
|high|12860/29380|42240/0|64/0|0/0|127.97474/127.97442|
|p60|42240/0|42240/0|0/0|64/0|127.97442/127.97418|
|p120|15124/27116|42240/0|64/0|0/0|127.97477/127.97535|

The cooled cohort was read twice during initial warmup, then not refreshed:
it is neither an unread NEW group nor the continuously read hot cohort.
Its observed retention must not be silently discarded or assumed to match
previous high-pressure runs. Its actual business value is not specified.

### Refill traffic, not hidden successful hits

Legacy has zero hot losses/refills in all five pairs. Lifetime low/P60 also
have none. For the other lifetime points:

| Point |Distinct lost hot keys|Repeat misses|Hot refill ACKs|Hot refill payload bytes|Later scheduled hit/miss after first refill|
|---|---:|---:|---:|---:|---:|
|medium|16|4017|4033|16519168|3122/4017|
|high|64|29316|29380|120340480|6846/29316|
|p120|64|27052|27116|111067136|9113/27052|

Every extra hot refill was ACKed and verified as a same-byte hit; those extra
verification reads add heat but are NOT original scheduled hits or evidence
that the next scheduled read will hit. Repeated misses are not distinct victims.
Payload bytes describe diagnostic application traffic, not NAND write
amplification, disk bytes or a complete recomputation cost.

Delayed-first-miss refills/verification remain a separate ledger: low64 each
policy; medium79 lifetime/85 legacy; high64 lifetime/128 legacy;
P60 128 each; P120 64 lifetime/128 legacy. Each ACK adds4096 payload bytes and
one successful verification GET. All these writes are additional to the
frozen external NEW stream, not counted in nominal lambda.

Full admitted/ACKed denominator cross-check (all GETs, including extra verifies):

|Point/policy|PUT ACKs|PUT payload bytes|GET attempts|GET hits|GET misses|
|---|---:|---:|---:|---:|---:|
|low/legacy|33472|137101312|58240|58176|64|
|low/lifetime|33472|137101312|58240|58176|64|
|medium/lifetime|58640|240189440|62288|58152|4136|
|medium/legacy|54613|223694848|58261|58112|149|
|high/legacy|96896|396886016|58304|58112|192|
|high/lifetime|126212|516964352|87620|58112|29508|
|p60/lifetime|96896|396886016|58304|58176|128|
|p60/legacy|96896|396886016|58304|58112|192|
|p120/legacy|96896|396886016|58304|58112|192|
|p120/lifetime|123948|507691008|85356|58112|27244|

Equal aggregate GET-hit counts in several rows result partly from conditional
verification traffic; they do not imply equal cache benefit. No native unknown
terminal, fallback, health failure or unresolved request was converted to a
cache miss or a zero.

### Descriptive cost checks

|Point|Lifetime visits/victim|Mean lifetime Allocate lock-held us|Hot batch GET p99 ms L/G|
|---|---:|---:|---:|
|low|2.71394|30.642|19.624/18.177|
|medium|2.33140|31.742|18.894/18.492|
|high|1.49014|35.134|18.543/18.558|
|p60|1.99879|29.277|18.879/18.625|
|p120|1.51089|34.796|17.758/19.466|

The1320 batch samples per case each contain32 keys. Client wall time includes
payload preparation, native call and status/byte validation; this is not an
individual-key/RPC/server p99, PUT tail or throughput acceptance. Lock time is
aggregate allocator A-lock time, not a per-request tail or the Store D lock.
Legacy lacks equivalent lifetime lock instrumentation: absent/zero counters
do not mean it did zero work. Fewer visits/victim can reflect more victims,
not better retention. All competing watermark/background/donor paths stayed0.
Lifetime final occupancy was4096 objects on each disk, but this does not prove
equal per-class pressure or balanced routing at every instant. Allocation
metrics, raw checkpoints and per-namespace ACK spans remain in the audit.

## Parameter guidance and remaining design decision

1. **Keep production defaults unchanged.** Neither this bounded synthetic
   matrix nor the earlier cursor prototype establishes a universal policy
   winner. No default P change or deployment is authorized by these results.
2. **Describe the desired retention objective before tuning.** P180/high kept
   all64 first185 reads but incurred29380 hot misses. P60/high kept all42240
   scheduled hot reads but lost all64 first185 reads; it also retained64 cooled
   keys. That is a concrete tradeoff, not a free improvement. P120 still lost
   all64 distinct hot keys repeatedly; it is not a demonstrated general cure.
3. **P60 is a candidate only for further validation of a hot/short-reuse
   objective like this one**, not a new recommended production default. Reads
   at100 lose the early reward compared with P180. If185/600-second reuse is
   expensive, these samples do not justify shortening P without business
   weights, a representative trace and repeated runs.
4. **Budget occupancy, not only elapsed time.** Estimate per disk/size class:
   recent still-unread NEW within P + desired hot occupancy + pinned/other
   required occupancy. Burstiness, size distribution and endogenous refill
   consume additional capacity. In the fixed-capacity rate sweep P180 changed
   from no hot loss at32/s to4033 at64/s and29380 at128/s. Even nominal
   lambda*P/C=0.9375 had losses: being below1 is not a guarantee. Lower offered NEW rates were tested only
   in this fixed setup; increasing capacity or implementing an admission-control
   mechanism was not tested.
5. **Do not add a startup rejection based on lambda*P/C.** It is workload
   dependent, not invalid numeric configuration. Preserve strict numerical
   validation; use measured pressure/retention as operational guidance.
6. **If the requirement is to keep hot objects ahead of unread NEW even within
   P under overload, this is a design-contract decision.** The current validator
   deliberately enforces the opposite priority. Legal BOOST/BONUS tweaks alone
   cannot reverse it. A priority/budget/admission change needs a separately
   reviewed specification and tests; no such mechanism is implemented here.

The definite finding is conditional scoring behavior plus a load-dependent
observed tradeoff. It is not proof that the algorithm is universally defective,
that large persistent datasets necessarily win, or that every native hot loss
has one identified cause. Candidate coverage remains a separate limitation.
Formal A/B still requires a real business trace/weights, frozen acceptance
thresholds, resource/baseline protocol and repetitions; GPU remains excluded.

## Validation, evidence and retained data

Fresh host RDMA/URING-OFF full build succeeded;19 candidate tests and436 selected
lifetime/allocator/DiskSlab CTest entries passed,0 failures/skips. Original16
candidate TEST bodies remain byte-identical;3 new tests exercise production
math/state/selector rather than a separate Python scoring model. An initial
fixture double-commit construction error was corrected; it is not evidence of
a production RED defect.

Fresh offline Python57/57 (9 new) and related Native-free293/293 passed,
0 skips. Preset API tests first failed against the missing implementation;
controlled memory/clock tests exercise report/dispatch/denominators, not RDMA.
LLVM17.0.6 C++ diff-format and clang-tidy passed for the changed test file;
Python AST and diffcheck passed. Ruff/mypy unavailable, no full lint claim.
No full-repository CTest or new sanitizer run is claimed: production shared
state/locks were not changed.

Guest Debug RDMA/URING-ON full build succeeded; the watched sync/build/test/
inventory operation took24.978 seconds in total. Six explicit Python CTest
entries (155 methods) and19 candidate tests passed with
0 failures/skips. Server/library hashes remained exactly baseline before and
after all10 native cases. Frozen323 source hashes, tool/configuration hashes,
raw report bytes/status/age ledgers and metrics all matched after execution.
Independent raw-ledger review is recorded separately from the runner verdict.

Evidence directory:
`.omx/evidence/score-pressure/20261124T054935Z-174ff32/`.
Frozen plan SHA256:
`9cb38afc4aff48e06daeb25631690966865e0d007fc0ba9c91645b48e9c505f6`.
See `frozen-contract.md`, `frozen-plan.json`, `source.sha256`, `audit.py`,
`audit-through-p4.json`, `reviewer-all-ten.json`, `host-ctest.xml`,
`cpp-boundary/`, `guest-method-counts.json`, `commands.md` and safety receipts.
Source/manifests are frozen before results; this completed public report is
not substituted retroactively for the frozen contract.

HOST control/archive directory:
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/score-pressure-20261124T054935Z-174ff32/`.
Guest outputs:
`/data/disk1/results/pressure-20261124T054935Z-174ff32-p{0,1,2,3,4}`.
Thirty retained guest case directories total489.875 MiB allocated:
`/data/disk{1,2,3}/cases/sg-long-20261124T054935Z-174ff32-p{0,1,2,3,4}-*`.
`retained-data.json` lists every exact path; the glob above is descriptive,
not a deletion command. Do not delete the live raw/qcow2 images containing
previous rounds. All guest data maps to HOSTdisk2/disk3, never HOSTdisk1.

Temperature monitoring collected927 allowed-NVMe samples, maximum41C,
SMART warnings0, largest gap10.4891 seconds. After owned servers/clients/
launchers exited and ports12000/12001/12010 were free, monitor1976105 exited0
and was reaped; exact VM six-FD proof, source/build hashes, rxe0 and io_uring
were rechecked. The original isolated VM is idle; no owned load/monitor remains.
HOSTdisk1 was neither loaded nor SMART-sampled; no host kernel/network change.
RXE/loopback is not physical HCA or cross-host proof. No commit/push is included.
