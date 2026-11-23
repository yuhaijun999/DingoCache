# RDMA fixed-arrival diagnostics and expanded regression

This validation lane uses RXE/RDMA only for cache data. TCP fallback is a failure,
not a substitute result. SSH administration and HTTP metrics are control-plane
traffic, not TCP cache-data tests. RDMA bootstrap control connections may also
use TCP; the cache data transport must still report `rdma`. GPU tests are excluded. This is diagnostic
coverage, not formal business/performance A/B acceptance.

## Prerequisites and safety

- Run the native matrix inside the disposable validation guest. Require active
  `rxe0`, RDMA/io_uring ON binaries and usable kernel io_uring.
- Before any guest load, the host operator must freshly prove the exact QEMU
  process/starttime, network namespace and all backing-file FD/inode identities.
- **Never load host `/mnt/disk1`.** Guest `/data/disk1` is a different path: its
  backing must be proved to reside on authorized host disk2. Guest disks2/3 are
  backed by authorized host disk3. Do not infer this mapping from names.
- Use a fresh host thermal watchdog for `/dev/nvme2` and `/dev/nvme0` only.
  Stop at any valid sensor >=65 C, any SMART critical warning, invalid sample or
  sample age >=25 seconds. Recheck process ownership before stopping work.
- Unlimited memlock is set only for this guest validation process and children.
  No host driver/module/sysctl changes, raw-device writes, global Docker cleanup,
  drop_caches or automatic deletion of sample directories.

## Fixed-arrival matrix

Inside the verified guest, after source SHA verification and a successful build:

```bash
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/run_rdma_arrival.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases \
  --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/rdma-arrival-NEW_TAG \
  --tag NEW_TAG
```

The three data roots and results parent must already exist. Output and every
sample data directory must be new. `NEW_TAG` uses 1..40 ASCII letters/digits/_/-.
Default: workers **1,4,16**, each **A/B, B/A, A/B** (A=legacy, B=lifetime),
18 fresh services. For a smaller correctness probe use `--workers 1 --pairs 1`;
that is not evidence of the complete default matrix.

Each sample uses the same immutable synthetic trace: 8 initial objects, 4 rounds,
4096-byte payload, 20ms source arrival interval; 24 planned requests, 12 PUTs and
12 GETs, 49152 ACK bytes and 49152 expected hit bytes. Existing staged scheduler
separates init, warmup and measure. Queue capacity16, request deadline5 seconds,
whole client process budget30 seconds including setup and close. Every worker
has private pre-opened namespace handles, all required to report exact `rdma`.

This is deliberately **below capacity**, not an eviction or saturation workload.
Configured worker count is not actual simultaneous I/O: inspect `peak_running`
and row intervals before claiming concurrency. No forced catch-up or silently
reduced planned denominator is permitted. Exceptions/failed reads are not misses;
late PUT completion does not retroactively become an on-time logical ACK.

The runner freezes binaries, trace and relevant runner/client/helper tool hashes
across the matrix; each child is independently audited. It verifies fresh disk
identity, effective requested/actual policy, exact PUT bytes, disk object count,
RDMA PULL terminal paths, io_uring reads/completions, lifetime heat (legacy0),
health and zero fallback/error/inflight/eviction. Stop on the first failure and
retain later cases as `not_attempted`; do not overwrite/retry a failed sample.
Source identity is not independent proof of source-to-binary provenance.

## Isolated class eviction (separate from watermark pressure)

A default `quick-pressure` result can contain only watermark evictions. It does
**not** by itself prove the class-head selection branch ran. To isolate it, use
this existing supervisor inside the same verified/monitored guest:

```bash
python3 /opt/dfkv/deploy/validation/quick_eviction.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases \
  --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/class-NEW_TAG-legacy \
  --data-name class-NEW_TAG-legacy \
  --policy legacy --scenario quick-pressure --class-only
```

Repeat with `lifetime` and distinct output/data names after the owned service
stops. This diagnostic sets low/high watermarks and background reclaim interval
to zero, verifies their effective per-disk identities **before writes**, and
requires positive foreground metadata evictions exactly matching the total
victim count. Watermark/background/donor activity and competing eviction metadata
must remain zero. All ordinary RDMA bytes/status/GET/heat checks still apply.
Only `quick-pressure` accepts `--class-only`. Production defaults are unchanged;
this isolated configuration is not interchangeable with default-watermark data.

## Expanded RDMA coverage

Reuse existing entrypoints under the same host safety guard, serially (they use
fixed ports12000/12001/12010):

- `run_sg_validation.py`: both policies, full SG bytes and per-item states,
  oversize/zero-cap boundaries and stopped-service failures.
- `run_cpu_dedup_validation.py --cross-process`: both policies, C/Python/report
  chain, shared READY data, owner-local error, killed-owner recovery, explicit
  failed remote reads and TTL boundaries. CPU only, no GPU substitute claim.
- `quick_eviction.py`: fresh two-policy `quick-pressure`,
  `multisize-pressure --multisize-watermark`, `concurrent-pressure`, and
  `arrivals-pressure --refill-on-miss`. Preserve the original scenario schedules:
  arrivals uses 200 writer steps at 1 second, independent concurrency uses 200
  writer steps at 0.1 second; multisize is per-batch interleaving, not a claimed
  200-second clock. Retain actual first-outcome/refill denominators; do not
  shorten a schedule and call the result equivalent. Low40/high50 applies only
  to the explicit watermark
  diagnostic, not production defaults.
- Required real-QP tests: pin lifetime across Remove/eviction, busy-QP admission,
  queued connection close, failed submitted read/PUT, exact read bounds,
  coalesced mutation ordering, per-depth concurrent reuse and release cleanup.
  Controlled CQE tests prove scheduling/error control flow, not kernel reads.
- Five `UringReader` real kernel-ring cases are recorded separately. A required
  hardware-path skip is a missing proof, never a pass.

Small fixed-arrival samples and closed-loop pressure scenarios have different
arrival/denominator contracts; never merge their rates into one performance A/B
number. Preserve lifetime hot-loss tradeoffs rather than adjusting thresholds
or choosing only favorable runs after observing results.

## Unit tests (no native TCP service)

```bash
python3 -m unittest discover -s test/python -p test_eviction_arrival.py -v
python3 -m unittest discover -s test/python -p test_rdma_arrival_runner.py -v
```

The controlled adapters are unit fixtures, not RDMA hardware evidence. Native
validation must use the ON guest build and retain raw XML/client ledger/metrics.

## Report and remaining gates

Retain `matrix.json`, frozen `trace.json`, per-case server log, before/after
Prometheus metrics, complete child ledger and supervisor reports, report SHA,
source/build identities, actual commands, process exit/reap proofs and thermal
history. Data paths appear in each case's `data_dirs`; retained failures count.

Formal AB7 is still gated on target workload/resources, independent M/O baseline,
roles/visibility audit and numerical preregistration. RXE loopback does not prove
physical HCA/cross-host behavior or production latency/throughput. Production
policy defaults are unchanged; no formal A/B signoff or automatic commit here.

## This round's execution record

本轮基线 `c30c70c`；主证据目录：
`.omx/evidence/rdma-arrival/20261122T125908Z-c30c70c/`。
最新源码为 `final3-source.sha256`，最终矩阵是 `checked-results/rdma-matrix-20261122T125908Z-c30c70c-checked`；
前两轮开发矩阵及其源码快照保留为历史，不重新标记为最终样本。

### 实际执行与结果

- Guest：Ubuntu24.04.5 / kernel6.8.0-147、GCC13.3.0、Python3.12.3，6vCPU/8GiB；
  Debug、RDMA=ON、io_uring=ON。全目标327步重编译成功（421.46秒）；后续仅Python工具修复，
  同步319个源码摘要并再次构建成功，无C++待编译。无生产C++/ABI/算法/磁盘格式变更。
- 最终18组固定到达：432/432请求，216 PUT ACK＋216 GET hit，各884736有效字节；
  252个RDMA句柄全部关闭、54个独立新数据目录，全部服务正常退出。
  配置1/4/16worker，**实际并发峰值分别为1、3～4、3～4**，不宣称16路同时I/O。
- 40个必需QP测试首轮39通过、1因缺第二ACTIVE设备跳过；随后在guest现有enp0s3上临时增加
  第二RXE，原跳过项实际通过，校验身份后删除新增设备。原skip记录保留。另5项真实kernel-ring通过。
  其中32线程×512次×1MiB READ是软件RXE原语诊断，不是物理HCA或策略性能对比。
- SG、CPU dedup与跨进程owner失败/死亡恢复，两策略均通过完整原账、字节/状态及服务端指标校验。
- 10个压力样本通过诊断契约：默认quick2、多尺寸LOW40/HIGH50水位2、8个独立读者2、
  持续新到达＋缺失回填2、class-only2。每种压力配置每策略仅1次，不冒充正式重复配对A/B。
  class-only两策略均为616次foreground淘汰，水位/后台/donor均0，metadata attempts/completed与616一致、errors0。
  独立读者实际观察到8路调用重叠及中段水位进展。
- 真实客户端进程50ms总预算负例：legacy24计划请求、实际计数unknown（不是0），TERM并回收子进程，
  正常关闭服务；lifetime按fail-stop保持not_attempted。**不推断超时发生在setup还是在途RDMA I/O**。
  该故障样本的matrix仍为failed，`expected-fault.json`只表明保护符合预期，不将失败改写为业务成功。
- 最终host显式白名单纯回归293方法通过、0失败/跳过。Guest定向CTest4项通过、0失败/跳过，
  内部方法数为arrival22、runner12、scheduler8、quick56（共98）；不与293或QP项数相加。
  本轮未运行全量CTest、额外sanitizer或GPU验证。Python编译/AST、diff与链接检查通过；
  Ruff/Pyflakes/Mypy未安装，不冒报lint/typecheck全绿。

实际构建与定向CTest命令（在guest执行）：

```bash
cmake --build /opt/dfkv-build -j2
ctest --test-dir /opt/dfkv-build \
  -R '^(python_eviction_arrival|python_rdma_arrival_runner|python_eviction_scheduler_backend|python_quick_eviction)$' \
  --no-tests=error --output-on-failure --output-junit NEW_OUTPUT/ctest.xml
```

### 不应掩盖的策略取舍

以下是诊断样本，不是生产验收阈值，也未据此调参或改变默认：

| 场景 | legacy热点命中/未命中 | lifetime热点命中/未命中 |
|---|---:|---:|
| class-only | 7552 / 0 | 6184 / 1368 |
| 默认quick水位压力 | 7552 / 0 | 4322 / 3230 |
| 多尺寸水位 | 22448 / 1104 | 6852 / 16700 |
| 独立并发 | 178560 / 0 | 54339 / 211805 |

并发是闭环，两个策略实际读取分母不同，不能按请求总数推断吞吐优劣。
持续到达回填：legacy首次384命中/0未命中，无回填；lifetime首次256命中/128未命中，
128次回填ACK及128次验证命中，额外524288有效PUT字节。有效PUT字节不是设备/NAND写放大。
**正确性通过不代表lifetime热点保留更好或总体业务收益为正。**目标业务权重与正式数值门禁仍待确定。

### 修复与执行偏差记录

- 新RDMA配置曾共享`CLIENT_ENV`字典，回归先RED后以独立dict副本修复；最终18组重新使用新目录验证。
- 独立审查补齐whole-HTTP预算、跨样本工具冻结、逗号路径拒绝，以及class-only的外来磁盘gauge/
  竞争metadata活动反例。所有最终代码审查问题已解决。
- 扩展host回归误选并执行了5个历史TCP NativeBatch/NativeCpuDedup用例；已向用户说明，
  原记录留在`selection-deviation.json`，**不纳入本轮RDMA证据或293纯测试**。后续白名单未再包含Native类。
- 第一条复杂SSH CTest命令仅返回MOTD与exit0、无完成标记，未计通过；改用上传的Python argv脚本，
  验证4项目录与JUnit内部98方法后才采信。失败归档尝试同样保留，未用空测试伪造通过。

### 安全、落盘和收尾

两块允许盘温控278次采样，最高nvme2=41°C、nvme0=41°C，warning0，最大间隔10.371秒。
宿主disk1未被压测或采样。原VM保持空闲，测试服务/子进程已回收，临时第二RXE删除，
新温控会话正常退出0且PID消失，无本轮后台负载遗留。

本轮207个guest数据目录共约3204MiB实际分配，均保留，可按本轮TAG筛选后人工清理：

- Guest `/data/disk1/cases/*20261122T125908Z-c30c70c*` → HOST disk2上的`data1.raw`。
- Guest `/data/disk2/cases/*20261122T125908Z-c30c70c*` → HOST disk3上的`data2.raw`。
- Guest `/data/disk3/cases/*20261122T125908Z-c30c70c*` → HOST disk3上的`data3.raw`。
- Guest报告：`/data/disk1/results/rdma-*-20261122T125908Z-c30c70c*`。
- HOST控制日志/归档：`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/rdma-arrival-20261122T125908Z-c30c70c/`。
- 既有镜像backing：HOST disk2 `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/`，
  HOST disk3 `/mnt/disk3/dfkv-validation/20261118t063414z-c9bbdca/`。

不要直接删除仍由VM使用、还包含其他轮次数据的raw/qcow2镜像；清理本轮数据应在guest内操作精确case目录。
主证据目录已保存原账副本、源码/工具身份、独立审计、温控及收尾证明；未自动删除样本、提交代码或推送。
正式A/B仍未开始，生产默认未变。
