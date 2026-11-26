# Independent concurrent RDMA prefix-chain diagnostic

## Scope and status

This is an opt-in AB7 correctness and concurrency diagnostic, **not formal A/B
acceptance or a production default change**. GPU, inference-engine adoption and
TTFT are user-excluded, not passed hardware/business gates. A checked contiguous
prefix is a cache-service proxy; it is not measured inference token reuse.

This bounded round is complete and locally verified, not yet committed. The
next chain-recovery / parameter-resource / formal-A/B steps have not started.

## Why a separate executor

The previous [mixed-size diagnostic](DISK_LIFETIME_PREFIX_MIXED_VALIDATION.md)
executes requests serially. More Python threads or outstanding request objects
alone do not prove independent native calls. This diagnostic gives each worker
an exclusive RDMA handle and records time brackets around direct C ABI calls.
Within each chain, GET remains dependency ordered and stops at the first miss.

The workload uses **independent per-worker absolute paced streams**, not a
shared global open-loop queue. Blocking work delays that worker's next round;
expired rounds remain in the planned denominator. There is no queue-full metric,
no claim of independent arrival admission under overload, and no cancellation of
an in-flight C ABI operation. The parent process deadline is the final bound.

Concurrent requests may share immutable keys. A refill can improve another
request's later observation; eviction can remove an earlier prefix while another
block is being read. No atomic resident-chain snapshot, whole-chain pinning,
refill coalescing, serial-equivalent result or dedup saving is promised. Original
usable-prefix credit is frozen before that request's own suffix refill.

## Frozen comparison

- Workers: 1, 4, 16; three fresh pairs per count, AB / BA / AB.
- A = same-binary `legacy`; B = same-binary `lifetime`, default lifetime scoring.
- Main: 600 rounds, 100 ms per worker, 8 unique arrival objects per round,
  12,288 initialized objects, 4 KiB immutable payloads, five-second round deadline.
- Smoke: eight rounds and one pair per worker count; not main-duration evidence.
- One namespace, generation zero; hot long/branch/short chains and rotating
  initialized cold chains. Identity, request order and target weights are sealed.
- Initialization completes before the common measurement start. No post-refill
  verification GET is allowed to retroactively improve a request's first prefix.
- 12 GiB logical slab capacity across three guest filesystems, RAM disabled,
  disk-ACK PUT, buffered writes, RXE RDMA GET with io_uring enabled.
- Class-pressure environment: no global watermark/background reclaim override.
  Actual eviction/donor counters must be reported; nominal offered slot bytes
  are not device writes or proof of a particular victim.

The 1-worker case is a serial reference. The 4/16-worker correctness gate requires
actual overlapping C ABI brackets from different private handles. These are guest
monotonic wall-clock brackets around calls, including scheduling and ctypes
entry/return overhead, not isolated C++ service time or NIC packet timestamps.
They do not prove simultaneous physical device I/O or physical-HCA throughput.
Python buffer preparation and payload checking sit in wider backend envelopes.

Every planned business request retains its target tokens. Miss is only explicit
NOT_FOUND. Native errors, unknowns, deadline expiry, late completion and
unattempted rows are not converted into misses or excluded by a success-only
intersection. A timed-out PUT may still ACK physically: retain the physical
outcome, but do not turn the request into an on-time success.

## Safety prerequisites

Before every native invocation, prove the exact current isolated VM PID/start,
UID, namespace and six backing FD device/inode/path identities. Verify guest
mounts, source and binary hashes, RXE, io_uring, empty ports and no owned leftover
server/client. Path pointers and old summaries do not grant control authority.

**Never pressure or sample SMART on host `/mnt/disk1` / `/dev/nvme1*`.** Guest
`/data/disk1` is backed by an allowed host `/mnt/disk2` raw file; the names are
not interchangeable. All native VM/cache backing and runtime logs/archives stay on host disk2/3;
repository-local evidence copies use `.omx/evidence` on `/home`.
A live host watchdog samples only `/dev/nvme2` and `/dev/nvme0`; stop at 65 C,
any SMART warning/invalid sample or a sample older than 25 seconds. Do not change
host kernel/network configuration or delete active VM images. Stop and reap
owned clients/servers before normal watchdog shutdown; leave the VM idle.

## Reproduction and evidence

Commands, tested hashes, exact method/CTest counts, native results, failure
provenance, independent audit and retained-data locations are recorded below
only after execution. Reuse of an old result is never a fresh rerun.

### Commands

From the repository root (host OFF build is compilation/unit coverage only;
no TCP cache workload is executed):

```bash
cmake --build build-test -j2
(cd test/python && python3 -m unittest \
  test_prefix_concurrent test_prefix_concurrent_runner -v)
ctest --test-dir build-test --no-tests=error --output-on-failure \
  -R '^(python_prefix_(chain|dynamic|rdma|rdma_runner|delayed|delayed_runner|mixed_runner|concurrent|concurrent_runner)|SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)'
```

Inside the disposable guest, only after host safety prerequisites are live:

```bash
cmake --build /opt/dfkv-build -j2
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/run_prefix_concurrent.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/prefix-concurrent-UNIQUE_TAG-main \
  --tag UNIQUE_TAG --workers 1 4 16 --pairs 3 --rounds 600 \
  --interval-ns 100000000 --arrival-batch 8
```

Choose a new tag/output for every invocation; no overwrite, store reset or
cleanup is performed. A correctness-only smoke uses `--pairs 1 --rounds 8`,
with all three worker counts. The process timeout includes initialization,
handle setup, physical drain, report writing and checker work, not just the
nominal offered schedule. One worker's blocking call does not reserve another
worker's handle; no handle closes before all its actual calls have returned.

For each worker count, policies receive the same immutable identities and
planned target-token denominator. **Worker counts do not have equal aggregate
offered rates**: there are eight arrivals and one chain per worker-round. Compare
policies within a worker count; do not infer isolated concurrency scalability or
causal benefit from cross-count throughput/absolute token differences.

The four rotating chain kinds each occupy 150 of the 600 rounds per worker:
long/branch have 160 target proxy tokens, short has 24, initialized-rotating has
160. Thus each main case has `N = workers * 600` and
`T = workers * 75,600`. No row is removed because another worker refilled it or
because it failed to execute on time. Cold chains are previously initialized
objects, not ACK-aged newly arriving chains; long-delay/long-residence coverage
belongs to the earlier report, not this short concurrent measurement.

## Verification evidence and provenance

Evidence root (local, not committed):

```text
.omx/evidence/prefix-concurrent/20261126T014552Z-075d964
```

R1 implementation hashes: client `20dd7514`, controller `fde53347`, client tests
`cb43c850`, controller tests `5c0128df`. Full hashes for 343 source/build-input
files are in `freeze-r1.json` and `source-r1.sha256`; the source packet is
`source-r1.tar`. Neither C++ production sources nor scoring/defaults/ABI changed.
The initial packet and original failed matrix remain separately retained.

Fresh R1 verification, all zero failures/skips:

| Environment | Actual check | Result |
|---|---|---|
| Host `build-test`, RelWithDebInfo, RDMA/io_uring OFF | `cmake --build build-test -j2` | pass |
| Host | two new unittest modules | 43 methods, 1.810 s |
| Host | targeted CTest selector above | 445 entries, 43.36 s |
| Host | 13 related unittest modules | 288 methods, 55.763 s |
| Guest `/opt/dfkv-build`, Debug, RDMA/io_uring ON | `cmake --build /opt/dfkv-build -j2` | pass |
| Guest | 15 unittest modules | 331 methods, 55.781 s |
| Guest | same targeted CTest selector | 448 entries, 52.81 s |
| Independent code reviewer | two new unittest modules | 43 methods, 1.727 s |

CTest embeds Python methods already counted above; do not add the figures as
unique test methods. The guest has three extra native io_uring tests. The host's
OFF build does not execute TCP cache traffic. These are targeted checks, not a
fresh full-repository or sanitizer suite. No physical HCA/GPU claim is made.

Host logs are `r1-host-{build,new-tests,ctest,related}.log` and
`r1-host-ctest.xml`. Guest logs/XML and per-module counts are in
`tests-r1-results/prefix-concurrent-tests-20261126T014552Z-075d964-r1/`.
The host related command runs:

```bash
(cd test/python && python3 -m unittest \
  test_eviction_arrival test_eviction_realtime \
  test_prefix_chain test_prefix_dynamic test_prefix_rdma test_prefix_rdma_runner \
  test_prefix_delayed test_prefix_delayed_runner test_prefix_mixed_runner \
  test_long_residence test_long_residence_runner test_eviction_trace \
  test_eviction_replay -v)
```

The guest command adds `test_prefix_concurrent` and
`test_prefix_concurrent_runner` to exactly those 13 modules. The reproducible
command and XML checks are retained in `guest-tests-r1.py`. Static checks and
final raw-result review are recorded at closeout, separately from build success.

### Original smoke failure, retained rather than rewritten

The initial native controller stopped after the first legacy 1-worker case in
28.600236 s. The client completed correctly: 8/8 requests, U/T = 696/1008,
6 full prefixes, 15 refill ACKs / 61,440 bytes; native 12,367 PUT ACKs /
50,655,232 bytes and 49 GET hits plus 2 explicit misses, zero native errors or
unknowns. Client and server reaped with exit zero; five later cases were not
attempted.

The controller wrongly assumed `dfkv_errors_total` was an unlabeled scalar.
The service actually emits three labeled series (`put/io`, `get/io`,
`any/invalid`), all zero. A regression using that real metric shape first failed
against the original oracle (`metric-label-red.log`), then passed after the
controller-only fix (`metric-label-green.log`). Label identity, uniqueness,
finite integer values and zero errors remain required; fallback and inflight
still must be scalar zero. No scoring, workload or deadline change was made.

The original `smoke-results` matrix is still **FAILED**. Its 12,418 events,
8 requests and 3 raw samples were independently reconciled; that retrospective
raw audit is not an original controller PASS or a fresh native rerun.
`source.tar` / `freeze.json` bind the original controller; R1 has separate source,
output tags, logs and fresh runs. Earlier CMake registration and pre-load
16 MiB/checker fixes happened before native execution; their retained checks
are not fabricated hardware failures.

### Fresh R1 smoke

Six fresh cases passed in 139.780582 s. Both root and independent reviewer
stdlib-only audits reconciled **78,850 events, 336 planned/completed requests,
18 raw resource/Prom samples**, payload digests, original first-gap prefixes,
refills, all native byte/terminal denominators and owned process cleanup.

| Workers | Policy | U / T | Aggregate ABI peak | GET ABI peak |
|---:|---|---:|---:|---:|
| 1 | legacy | 712 / 1008 | 1 | 1 |
| 1 | lifetime | 616 / 1008 | 1 | 1 |
| 4 | legacy | 2760 / 4032 | 4 | 4 |
| 4 | lifetime | 1064 / 4032 | 4 | 4 |
| 16 | legacy | 11008 / 16128 | 16 | 12 |
| 16 | lifetime | 2808 / 16128 | 16 | 7 |

These eight-round smoke values are correctness evidence, not the 600-round
main comparison, an overload frontier or a benefit acceptance threshold.

## Main: 18 fresh comparisons

All 18 cases passed and owned clients/servers reaped with exit zero. The matrix
controller took **1770.628092 s (29 min 30.6 s)**, including initialization, handle
setup, physical calls, byte validation, report checking and cleanup. Each actual
measurement lasted approximately 59.918–60.023 s; the whole controller duration
is not one continuous 29-minute measurement. Main output is under
`main-r1-results/prefix-concurrent-20261126T014552Z-075d964-r1-main/`.

Both root and independent reviewer stdlib-only auditors reconciled
**1,355,916 events / 75,600 planned and completed requests / 175 raw samples**.
Across all calls, including initialization/warmup: 1,082,111 PUT ACKs /
4,432,326,656 logical payload bytes; 235,489 GET hits plus 38,316 explicit misses.
There were zero native errors, unknown outcomes, wrong payloads, late or expired
business requests. These are logical ACK/payload counters, not physical NAND
writes. The main business target denominator is 9,525,600 proxy tokens, with no
success-only denominator intersection or censored/error rows discarded.

Case IDs show AB / BA / AB independently within each worker count. Each case has
N/T = 600/75,600 (W1), 2400/302,400 (W4), or 9600/1,209,600 (W16).

| Case | Workers | Policy | Usable tokens | U/T | Full prefixes | Refill ACKs | GET ABI peak |
|---:|---:|---|---:|---:|---:|---:|---:|
| 00 | 1 | legacy | 51792 | 68.5079% | 450 | 1186 | 1 |
| 01 | 1 | lifetime | 15976 | 21.1323% | 146 | 2968 | 1 |
| 02 | 1 | lifetime | 17200 | 22.7513% | 158 | 2883 | 1 |
| 03 | 1 | legacy | 52072 | 68.8783% | 452 | 1174 | 1 |
| 04 | 1 | legacy | 51624 | 68.2857% | 450 | 1197 | 1 |
| 05 | 1 | lifetime | 15968 | 21.1217% | 144 | 2967 | 1 |
| 06 | 4 | legacy | 213640 | 70.6481% | 1844 | 4434 | 4 |
| 07 | 4 | lifetime | 75760 | 25.0529% | 632 | 11428 | 4 |
| 08 | 4 | lifetime | 71696 | 23.7090% | 593 | 11600 | 4 |
| 09 | 4 | legacy | 213480 | 70.5952% | 1843 | 4442 | 4 |
| 10 | 4 | legacy | 213432 | 70.5794% | 1841 | 4444 | 4 |
| 11 | 4 | lifetime | 76560 | 25.3175% | 624 | 11374 | 4 |
| 12 | 16 | legacy | 850520 | 70.3142% | 7353 | 17948 | 14 |
| 13 | 16 | lifetime | 250672 | 20.7235% | 1956 | 47948 | 11 |
| 14 | 16 | lifetime | 270536 | 22.3657% | 2062 | 46903 | 11 |
| 15 | 16 | legacy | 849784 | 70.2533% | 7348 | 17982 | 15 |
| 16 | 16 | legacy | 851136 | 70.3651% | 7358 | 17922 | 15 |
| 17 | 16 | lifetime | 264632 | 21.8776% | 2030 | 47327 | 12 |

Each refill payload is 4096 bytes, so refill payload bytes equal ACKs × 4096;
raw exact byte fields remain in the report. Same-generation concurrent upserts
are counted as ACK writes, not assumed new allocator admissions. Actual class
admissions satisfy distinct ACK identities ≤ admissions ≤ ACK upserts. No donor
steals, background reclaim or watermark victims were observed in these cases.

### Concurrency, clocks and actual measurement pressure

All 4-worker main cases observed four overlapping GET call brackets. The six
16-worker peaks were 14, 11, 11, 15, 15, 12; aggregate GET+PUT ABI peak was 16.
Do not claim every 16-worker case had 16 simultaneous GETs. Every successful
native event carries a valid direct ABI bracket inside its wider backend call.

Maximum per-request start lag was 0.003082 s at W1, 0.007108 s at W4 and
**0.370830 s at W16** (case 14). Thus some requests started more than one nominal
100 ms interval late, although all remained within their five-second deadline.
A late worker can catch up without sleeping on subsequent already-due rounds;
this tool does not enforce a minimum physical inter-arrival gap, offer an
unbounded independent producer, or establish an exact-100-ms arrival SLO.
Scheduling/ctypes/GIL entry-return time is not isolated C++/device latency.

Every main case has 5–6 raw scrapes fully contained between actual common
measurement start and the last business call completion: 97 snapshots / 79
adjacent windows in total. Each case's aggregate delta across those contained
samples is strictly positive for both capacity evictions and foreground eviction
metadata completions. Thus pressure was not confined to initialization. These
contained-window foreground completion deltas range from 4,120 to 86,400;
callback snapshots are not atomic with eviction counters, so small differences
are retained rather than forced equal. There is no per-victim chain-causality
or physical-I/O amplification proof.

Whole-case sampled server RSS peaks range from 30,020 to 42,132 KiB. Maximum
sampled server CPU within contained measurement intervals was 74.558% of one
core, not 74.558% of the six-vCPU guest. The synchronous ten-second sampler
covers only the owned server, not client Python, pinned client buffers, page
cache, QEMU or host CPU/RSS. No whole-system resource budget, leak absence,
lock-wait distribution or throughput frontier is accepted from these samples.

### Business interpretation

Within the same worker count and all matched policy pairs, this synthetic
workload gave legacy a larger usable-prefix proxy and lifetime more refill ACKs:

- W1: legacy 68.29–68.88%, lifetime 21.12–22.75%.
- W4: legacy 70.58–70.65%, lifetime 23.71–25.32%.
- W16: legacy 70.25–70.37%, lifetime 20.72–22.37%.

This is **a correctness/concurrency pass, not a lifetime benefit pass**. It does
not establish a universal policy ranking, independent key hit rate, production
business value or inference-engine/TTFT gain. Higher worker counts also offer
more new objects; do not attribute cross-count changes solely to concurrency.
Refill traffic and later requests' residency are endogenous to observed first
gaps. No per-victim score defect was demonstrated or scoring/default change
adopted. Default protected-new objects remain under their 180-second window in
this roughly 60-second concurrent measurement; long-delay usefulness belongs
to the separate prior experiments and representative business evaluation.

## Safety closeout and retained data

Fresh final six-FD, UID/start/netns, 343-source manifest, unchanged server/library
hashes, guest mounts, RXE/io_uring and free-port proofs passed. No owned server,
client or launcher remains. The existing VM is retained idle, not deleted.
Normal watchdog stop was requested only after all native work, archives, raw
audit and final identity/idle proofs; monitor PID 2549980/start429181928 reaped
exit zero and `/proc/2549980` was absent.

The watchdog retained **280 samples**, only host `/dev/nvme2` and `/dev/nvme0`,
maximum 42 C, all SMART warnings zero, maximum gap 10.333202 s (<25 s). No host
`/mnt/disk1` pressure or `/dev/nvme1*` SMART call was invoked by this task.
This does not assert that unrelated host services performed no I/O.

75 exact owned guest cache directories are retained, allocated size
**1,282,129,920 bytes (about 1.194 GiB)**, listed in `retained-data.json`:

```text
/data/disk{1,2,3}/cases/sg-prefix-concurrent-20261126T014552Z-075d964-s-*
/data/disk{1,2,3}/cases/sg-prefix-concurrent-20261126T014552Z-075d964-r1-s-*
/data/disk{1,2,3}/cases/sg-prefix-concurrent-20261126T014552Z-075d964-r1-m-*
```

The first pattern contains only the original failed-controller case's three
directories; the latter two contain 18 smoke and 54 main directories. Result
JSON/Prom/log/XML files remain in guest `/data/disk1/results/` and expanded
archives under the evidence root. Host runtime archives/logs are under:

```text
/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/prefix-concurrent-20261126T014552Z-075d964
```

Guest disk1 maps to host disk2, while guest disk2/3 raw backing maps to host
disk3. Do not delete active raw/qcow2 images to clean case files. No automatic
case cleanup was performed. Any later cleanup must use the exact owned list and
fresh idle/identity checks, not these globs as a deletion command.

`main-r1-analysis.json` and `smoke-r1-analysis.json` retain stdlib raw audits;
`independent-*-review-r1.json` are explicitly root transcriptions of independent
review messages, not reviewer-authored files or authentication signatures.
`safety-closeout.json`, `final-six-fd-proof.json`, `retained-data.json`, thermal
raw data, all freeze packets, original failure, RED/GREEN tests and watch receipts
remain separately retained. Source/plan hashes are provenance checks, not proof
against a malicious operator. The archive size ceiling is checked after capture,
not a live write quota; extraction rejects traversal/links/devices and has
separate member/count/expanded-byte bounds.

## Remaining scope

Next suggested round: prefix-chain clean/dirty recovery and policy-switch
compatibility. Then reuse existing generic coverage while filling parameter /
resource boundaries and numeric formal-A/B business/safety gates. Do not rebuild
already-proven generic behavior solely to inflate completion counts. GPU,
real inference-engine adoption and TTFT stay excluded, not pending blockers.
Formal A/B acceptance, representative business thresholds and production rollout
remain separate; neither this diagnostic nor a low error count flips defaults.
