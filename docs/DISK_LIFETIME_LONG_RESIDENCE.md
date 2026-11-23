# Long residence and delayed first reuse diagnostics

This specialty extends the RDMA diagnostics in [DISK_LIFETIME_RDMA_ARRIVAL.md](DISK_LIFETIME_RDMA_ARRIVAL.md). It is not formal A/B acceptance, a production-default change, or evidence of day-scale aging. Negative policy outcomes must remain visible.

## Frozen scope

- Compare legacy and lifetime with fresh directories and identical workloads, alternating AB / BA / AB.
- Isolate class-capacity eviction: watermark and background reclaim disabled. Reject competing donor/background/watermark activity. This is not coverage of all eviction modes.
- Fill compact three-disk slab geometry and continuously admit unread objects. Before measurement, explicitly pre-age the initial residents while continuing hot reads and leaving cooled objects unread; insert the three delayed-first-read cohorts only after this pre-aging phase. “Large” means capacity pressure and turnover, not a terabyte-scale working set or arbitrary absolute key count.
- Distinct cohorts: continuously read hot objects; formerly hot objects left to cool; three cohorts with delayed first reuse; continuous unread arrivals. Filler and cohorts have disjoint deterministic keys.
- Record PUT acknowledgements and actual first-read ages. Report first outcomes separately from conditional-on-first-miss refill and verification. Never reinterpret repeated misses as distinct victims, payload writes as NAND amplification, or successful validation as policy benefit.
- No forced winner or business-weight aggregate. Report all paired samples, including worse lifetime hotspot retention.

## Profiles

| Profile | Pre-aging / measurement per strategy | First-reuse delays | Lifetime parameters |
|---|---:|---|---|
| accelerated | 120 / 300 seconds | 3, 13, 24 seconds | BASE60, BONUS30, MAX_READS2, P12, SLOW4, BOOST120, KEEP47, G4, B24 |
| default | 240 / 1800 seconds | 45, 185, 600 seconds | production lifetime score defaults, explicitly selected lifetime policy |

Accelerated pre-aging spans the configured maximum read score (120 seconds); default pre-aging only exceeds the new-write protection period and does not reproduce day-scale cooling. Pre-aging hot reads and measured hot reads have separate denominators. These phases are frozen before native execution, not selected after seeing outcomes.

Accelerated parameters are a diagnostic model, **not an exact proportional simulation** of production scores. Rounding, bucket horizons, I/O rate, capacity and reuse rate do not scale equivalently. The default profile is an optional later step, not silently substituted for accelerated evidence. Neither profile proves seven-day residence or restart behavior.

## Safety

Actual service data uses RDMA/RXE and io_uring only. GPU is excluded. HOST `/mnt/disk1` is prohibited, including Docker's backing storage there. The retained disposable guest has `/data/disk1` backed by HOST `/mnt/disk2`, and `/data/disk2` and `/data/disk3` backed by HOST `/mnt/disk3`; the guest names do not denote the banned host disk.

Before any load, prove the existing VM's exact PID/start identity and all six open backing files against allowed host files. Use a fresh watchdog, not an old monitor PID. Stop on any allowed-device SMART warning, any sensor at or above 65°C, invalid readings, stale samples of 25 seconds or more, correctness failure, health failure, or transport fallback. This temperature is a conservative diagnostic stop, not a manufacturer rating.

Keep source/build/config hashes, raw client reports, before/after metrics, process exit status, and thermal samples. Bound and reap owned child/server processes; never broadly kill other services or delete in-use raw/qcow2 images. Fresh sample directories are retained for exact cleanup later.

## Execution and results

The external-client workload is `deploy/validation/long_residence.py`; the guest-only server/matrix supervisor is `deploy/validation/run_long_residence.py`. Both leave fresh data in place and refuse transport fallback. The supervisor verifies effective startup configuration, frozen binary/tool hashes, full client denominators, raw checkpoint metric hashes, real foreground eviction, healthy storage and io_uring path counters. Native children have a finite timeout; actual counters remain unknown if no valid child report was obtained.

Frozen accelerated workload per sample:
- 12,288 initial acknowledged PUTs of 4,096-byte objects, two namespaces, 32 objects per cohort per namespace.
- Pre-aging: 120 hot-read rounds, without new arrivals; formerly hot objects receive no further reads.
- Measurement: 300 paced steps, 128 newly arriving objects and 64 hot GETs per step. This admits 38,400 new objects, 3.125 times the compact geometry's nominal 12,288-slot budget; it is not terabyte-scale validation. Hash/disk-local class pressure can evict initial objects while other disks retain free slots, so 12,288 initial ACKs do not prove 12,288 simultaneous residents. The nominal aggregate turnover is about 96 seconds, not a per-key survival guarantee.
- First reuse: 64 objects at each of 3 / 13 / 24 seconds after their batch PUT ACK; at most two seconds of lateness, with the early group still inside P. Every first-read event requires actual new eviction since measurement start.
- Separate denominators for initialization, pre-aging, ready-state checks, measurement hot reads, first reuse, cooled final reads, refill ACKs and verification reads.
- Scheduling is closed-loop and never catches up by dropping planned operations. Continuity and lateness checks invalidate stalled samples; it is not a fixed-arrival throughput benchmark.

### Reproduction

Run only after the external host identity and thermal safeguards above are active. Inside the disposable guest, use a root shell (for example, `sudo -n -s` with the already-authorized guest account), a fresh tag/output every time and unlimited memlock. The unprivileged validator account cannot raise its hard memlock limit or write these root-owned data directories:

```bash
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/run_long_residence.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases \
  --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/long-NEW_TAG \
  --tag NEW_TAG --profile accelerated
```

The default is all three alternating pairs. For bounded separate dispatches, use `--pairs 1 --start-pair 0`, then 1 and 2, with distinct output paths but the same frozen tag/sources. Keep all six samples; do not rerun only unfavorable policy outcomes. The optional `--profile default` requires a separately budgeted longer host watchdog; it was not automatically scheduled by the accelerated run.

Pure tests, safe without any native service:

```bash
python3 -m unittest discover -s test/python -p test_long_residence.py -v
python3 -m unittest discover -s test/python -p test_long_residence_runner.py -v
```

Native results and their validation status are recorded below once all original artifacts have been inspected. A zero process exit alone is not acceptance.


## Actual accelerated result (2026-11-23)

Six accepted samples completed in frozen AB / BA / AB order with six distinct server processes and 18 fresh data directories. Each sample used 120 seconds of pre-aging and approximately 300.08 seconds of measurement. Total paired execution was approximately 42.7 minutes, excluding preparation/tests/audit. RXE and kernel io_uring were exercised; this is not physical-HCA or cross-host evidence. SSH, HTTP metrics and RDMA bootstrap control traffic are not TCP cache-data fallback.

Every accepted sample had the following result (all three repetitions for each policy agreed):

| Metric, per sample | legacy | lifetime |
|---|---:|---:|
| Measured hot hits / misses | 19,200 / 0 | 19,200 / 0 |
| First reuse at 3s, hits / misses | 64 / 0 | 64 / 0 |
| First reuse at 13s, hits / misses | 64 / 0 | 64 / 0 |
| First reuse at 24s, hits / misses | 64 / 0 | 64 / 0 |
| Final cooled hits / misses | 0 / 64 | 0 / 64 |
| Foreground evictions | 38,400 | 38,400 |
| Watermark / background / donor evictions | 0 / 0 / 0 | 0 / 0 / 0 |
| Conditional first-miss refills / extra payload bytes | 0 / 0 | 0 / 0 |
| Acknowledged PUTs / payload bytes | 50,688 / 207,618,048 | 50,688 / 207,618,048 |
| All GET attempts, including initialization/pre-aging | 27,456 | 27,456 |
| io_uring successful nonempty / not-found PULLs | 27,392 / 64 | 27,392 / 64 |

Observed first-read ages across all samples were 3.621–3.703, 13.621–13.673 and 24.606–24.676 seconds. Every first-read event followed positive measured eviction progress; the smallest early-event delta was 217. Pre-aging and ready-state hot checks all hit. Cooled misses were expected pressure outcomes, not network/I/O failures. All native children completed, all servers exited zero and were reaped, ports were released, and final source/binary/backing identities matched.

**Interpretation: correctness passed and the observed hit outcomes tied. This does not demonstrate superior lifetime policy benefit, throughput, latency, or absence of order effects outside these six samples.** First delays were shorter than nominal aggregate capacity turnover (~96 seconds), so this round did not test delayed reuse beyond that turnover. Per-disk hash imbalance means turnover is not an exact per-key lifetime. It also does not erase the earlier cold-start/default-score cases where lifetime lost more hotspot reads.

A further extrapolation limit is protection pressure: at 128 new objects/sec and nominal capacity 12,288, the simplified ratio `arrival_rate * P / capacity` is 0.125 for accelerated P=12, but 1.875 for default P=180. This is a workload indicator, not a guaranteed protected-resident count. Scaling the score parameters changes this regime; a default-parameter follow-up must freeze arrival rate/capacity and reuse windows explicitly rather than treating the accelerated tie as a production result. The default profile and its 45/185/600-second reuse windows were **not run** in this round.

### Tests and deviations

- New workload: 19 pure unit tests; runner: 16; related explicit pure whitelist: 293. All passed, zero skips/failures. No native TCP cache-data tests were selected.
- Guest build: `/opt/dfkv-build`, Debug, RDMA/URING ON. `cmake --build /opt/dfkv-build -j2` succeeded after CMake regeneration; no C++ source changed and Ninja had no compilation work.
- Guest targeted CTest: six entries, 133 unit methods, all passed, zero skips/failures. Not a full CTest, sanitizer, GPU or physical-HCA run.
- A first pre-run was deliberately interrupted during pre-aging after discovering that two final clock reads could falsely fail a one-microsecond consistency check. Its failed/interrupted raw matrix is retained and excluded, not relabeled as policy failure or success. A permanent regression first reproduced the error, then one shared final timestamp fixed it without relaxing the oracle. All six accepted samples used the separate v2 frozen source and fresh directories.
- An initial related-pure rerun invoked an old evidence-local script in place, rewriting its three pure-test output files. Provenance notes identify the rerun; current results were copied into this round. Historical native raw artifacts/source manifests were untouched. Later execution uses the current-round script copy.
- AST/bytecode and diff whitespace checks passed. Ruff/Pyflakes/Mypy were not installed; no full lint/type-analysis claim is made.

### Evidence, retained files and cleanup

Master evidence:
`/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/long-residence/20261123T032607Z-76ccc59/`

It contains `v2-source.tar`/`v2-source.sha256` (323 source inputs), accepted-pair raw archives, final combined and independent audits, commands/test logs/JUnit, the excluded interrupted pre-run, and thermal/postflight/cleanup proofs. Final accepted tag: `20261123T032607Z-76ccc59-v2`.

Retained guest case directories (18 accepted plus three interrupted pre-run directories):
`/data/disk{1,2,3}/cases/sg-long-20261123T032607Z-76ccc59*`

These 21 exact directories use approximately **342 MiB allocated**, not their sparse logical capacity. `retained-data.json` enumerates exact paths and sizes. Reports live under `/data/disk1/results/long-20261123T032607Z-76ccc59*`. Host control/archive files live in:
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/long-residence-20261123T032607Z-76ccc59/`

The existing guest data1.raw is backed by HOST `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/`; data2.raw/data3.raw by HOST `/mnt/disk3/dfkv-validation/20261118t063414z-c9bbdca/`. Do not delete in-use raw/qcow2 files containing other rounds. Cleanup, if later desired, should target the enumerated guest case directories, not the whole disk or backing image.

The owned thermal monitor finished normally and was reaped after all load/owned processes ended. 336 samples had zero SMART warnings; both allowed devices peaked at 41°C, maximum sample gap 10.555 seconds. HOST `/mnt/disk1` was neither loaded nor sampled. No native workload or monitor remains running; the existing idle guest is retained. Production defaults, algorithm, ABI and disk format remain unchanged. Formal A/B has not started.
