# RDMA 前缀链恢复与策略切换验证

本轮属于 AB7 正式 A/B 前专项诊断，不修改生产评分、默认值、C ABI 或磁盘格式。
GPU、真实推理引擎采用及 TTFT 按用户要求排除；连续前缀只是经字节校验的缓存代理。

## 冻结合同与门槛

- clean：健康正常退出会同步 payload/table，再标记 clean；重启应精确保留退出前仍驻留的探针身份和字节。
- dirty：既有实现无条件清空脏表。磁盘 ACK 不等于缓存崩溃持久承诺，重启应全 miss；不能将主动冷恢复当成数据损坏，也不能将此测试当成断电持久性证明。
- 策略切换是停进程后使用同一数据目录重启，不是运行时热切换。两个方向都要覆盖，恢复虚拟读历史不等于历史真实业务读。
- 探针 32 个独立身份、四条共享前缀链，每链 10 块、每块 16 代理 token；payload 4096 / 1048577 字节，映射两个真实 slot 档。
- 并发压力使用既有四 worker 独占句柄工具、独立命名空间；压力可能淘汰探针。停止前明确构造新的检查点 cohort，PUT-only 准备后逐字节快照并要求 32 hit / 640 代理 token，防止全 miss 的空洞“保留成功”。压力与旧 cohort 回填结果单列，不能将新检查点冒称压力幸存者。
- 所有正常阶段 errors/unknown 为零；dirty 主动断连的客户端失败必须单独记账，不能转成 miss 或并入正常成功分母。
- 重启前后采集原始 Prometheus，核对三盘身份、dirty reset=3（clean=0）、lifetime visible recovered、实际策略、磁盘 ACK、RAM off、RDMA/uring 路径与逐客户端终局计数。
- SHA 固定工具、二进制和输入，完整保存所有计划 case、未执行项、失败与原始账；出现失败停止后续 case，不事后放宽门槛。

源码合同：`src/cache/disk_slab_store.cc` 的析构同步、`ResetUncleanEpoch`；既有回归见
`test/cache/disk_lifetime_public_test.cc`、`test/cache/disk_slab_store_test.cc`。

## 本轮计划

1. 单测和独立审查，隔离环境 RDMA ON / io_uring ON 构建。
2. 八 case 带压力冒烟：clean/dirty × same/switch × 初始 legacy/lifetime，各一次重启、每进程 8 轮压力。
3. 同矩阵每 case 三次重启，每个进程寿命 600 轮、200ms 四 worker 压力，另驻留 10 秒；共 24 次请求重启、32 个服务进程寿命，不冒称单进程连续长跑。
4. 单列四 case 活跃客户端 SIGKILL，双向 same/switch；只证明杀进程时客户端仍活跃，不保证杀中某条内核 I/O，也不模拟掉电。
5. 全账审计、资源和恢复耗时统计、最终源/进程/端口/落盘身份核验；停止并回收本轮温控守护后交付。

## 安全范围

仅真实 RDMA 缓存流量，无 TCP fallback。宿主 `/mnt/disk1` 与 `/dev/nvme1*` 禁压、禁 SMART 采样。
宿主 backing/runtime 只在 `/mnt/disk2`、`/mnt/disk3`；guest `/data/disk1` 映射宿主 disk2，不能混淆。
每次原生任务前重新证明 VM PID/start/UID/netns/六个 FD、backing dev/inode、源和二进制、guest mounts/RXE/uring、端口与进程空闲。
新温控仅采 `/dev/nvme2`、`/dev/nvme0`：65°C、SMART warning、无效样本或超过 25 秒无新样本停止；不重用旧监控状态。
仓库证据副本在 `.omx/evidence/`（`/home`），不是声称全部日志都在 disk2/3。

## 初次冒烟与 R1 初始化修复

初次八场矩阵在第一场 legacy 的退出前门槛失败，尚未请求任何重启，其余七场未执行。
32 PUT ACK / 16,842,768 逻辑字节；首次快照 29 hit / 3 miss（对象 1、5、9），连续前缀 352/640。
三个 donor steal 与三个 capacity eviction 均在 seed 阶段，客户端无 I/O 错误或未知结果，服务进程正常回收。

这是四个 1GiB extent/盘的小几何初始化效应：`kStripeWays=8`，首个小尺寸档先绑定所有四个可用 extent；
第二尺寸档首次出现没有 free extent，需要借走一个已有小尺寸 extent，逐盘产生住户淘汰。
这是现有 extent 所有权行为，不是重启数据丢失，不能因总 payload 很小就断言不会淘汰。

R1 在 seed 或恢复后的首次回填之后，固定增加**一次** `stabilize` 回填，让两个尺寸档建档后恢复探针；
不使用“直到成功”的循环，不修改 allocator、评分或原来的 32 hit / 640 门槛。
真实 fake-backend 回归模拟首次大对象写入淘汰一个小对象，证明单次稳定化能修复快照；控制器测试覆盖有界次数与初始/恢复快照的严格失败门槛。
原失败、旧源和原始账保留；R1 使用独立 source freeze、新标签、新目录，完整重跑八场，不混作首次矩阵通过。

## R1 压力矩阵失败与 R2

R1 的八场无压力冒烟通过；随后主矩阵第一场的 120 秒压力本身也通过：2400 请求、35939 PUT ACK、11161 hit / 557 miss、无错误或未知，23650 次 capacity eviction。
但是，单尺寸压力把 1MiB/2MiB 档 extent 从 9/3 变成 12/0，夺走全部大档 extent；恢复回填再把它们变回 9/3，产生 3088 次住户淘汰。
首轮回填的小块复读发生在大档重新绑定之前，因此最终快照仍有五个小块缺失（2、4、6、9、10），27 hit、384/640 前缀代理。
矩阵在**任何重启前**正确失败，其余七场未执行；所有正常客户端无错误或未知，服务进程正常回收。原始失败保留，不冒称恢复损坏或主矩阵通过。

R2 仅在压力后的 `restore` 后固定增加一次 `restore-stabilize`，用第二次有界回填修复大档重建时淘汰的小块。
这不是失败后无限重试；每阶段固定执行、全部调用记账、门槛保持 32 hit / 640，恢复验证仍在任何回填之前。
R2 冒烟也明确加入每阶段 8 轮压力，覆盖这条重新建档路径；主矩阵参数不变。所有源/标签/目录重新冻结，不混用 R1 主测结论。

## R2 回填语义失败与 R3 检查点协议

带压力的 R2 冒烟中，legacy case 0 完成一次正常重启；lifetime case 1 在自身首次重启前回填复读失败：
16 PUT ACK、14 hit / 3 miss，没有 native error/unknown；后六场未执行。
先恢复链 0 并成功读过，再写分支 1 的后缀，随后复读共享头对象 0 已 miss。此时尚未触碰大尺寸块，所以不是该场大尺寸重新绑定导致。

源码支持的解释是：成功读会撤掉 NEW 的写保护 BOOST，而后续新对象仍受保护；满容量下较弱的已读块可以被淘汰。
原账没有逐 victim 身份/分数，故这是**源码支持的推断，不是直接测得的评分因果证明**。此外 PUT 对既有 key 是 keep-first，不重置热度，不能声称重复 PUT 会重新保护既有住户。

因此不能用“旧热点在满容量持续写入后仍必须同时驻留”作为恢复测试的准备前提，也不能用重复回填循环掩盖这个策略取舍。
R3 把两件事分开：

1. 保留此前压力与回填失败，仍标记失败；不宣称修好了热点收益。
2. 预先冻结每一阶段的新检查点 cohort，改变实际 key，而非只改客户端 generation。
3. 重启后先用**上一 cohort 的 manifest 和快照**完整验证 clean 保留或 dirty 清空，任何写入均在其后。
4. 本阶段压力完成后，用全新 cohort 做两次固定 PUT-only 准备，再做 GET-only 的严格 32 hit / 640 快照；快照后不再写入才停机。
5. 新检查点是明确的恢复探针准备，不是压力幸存者、热点回填收益或业务命中率证据。两次准备也不承诺必然成功，快照不满足仍 fail-stop。

默认 checkpoint=0 的旧工具 manifest/身份保持兼容，历史失败可复核；新 cohort 有单独 manifest、hash 和 previous→current 链接。

## 结果

最终 **R3 冻结协议完成并通过 scoped 正确性验证**；下面时长是控制器墙钟，包含启动、预填、冷却、校验和退出，不能当作纯压测时长或单进程连续运行时间。

| 组别 | cases / 进程寿命 / 实际重启 | 控制器时长 | 压力与恢复结果 |
|---|---:|---:|---|
| 带压力冒烟 | 8 / 16 / 8 | 773.810 秒 | 512 压力业务请求；16 段都观察到淘汰，GET ABI 重叠峰值 15 段为 4、一段为 3 |
| 三次连续恢复主测 | 8 / 32 / 24 | 5681.667 秒（94 分 41.7 秒） | 76,800 压力业务请求；32 段约 120 秒计时压力，全部完成、GET ABI 峰值均为 4、容量淘汰合计 866,105 |
| 活跃客户端中断 | 4 / 8 / 4 | 207.448 秒 | 四次 server SIGKILL，客户端当时仍活跃；四次 dirty reset 后 32 miss，回填/稳定化后 32 hit / 640 |

主测 24 次重启为 **12 clean + 12 dirty**；20 clean stop 包含八个最终停止，不是 20 次 clean restart。
全部最终 R3 组合为 56 个进程寿命、36 次重启（16 clean / 20 dirty）。不把旧失败实验或无重启的初始进程加作重启。

主测普通客户端终局计量为：1,240,042 PUT ACK / 6,482,994,496 **逻辑 ACK 字节**，275,350 hit / 34,660 miss，GET/PUT error 与 unknown 都为零。
每客户端的 ACK/字节、RDMA dispatched/terminal 及其 uring result/path、uring read/completion、lifetime 真读增量逐项对账。
容量淘汰包括 donor 的影响，不将总数冒称纯 class 单住户选择或全局水位淘汰；watermark/background 路径关闭。
ACK upsert 不等于物理写入次数，逻辑字节不是 NAND 写放大。

活跃中断的 **28 份普通报告**有 331 ACK / 135,049,344 逻辑字节、1013 hit / 156 miss、零 error/unknown。
另外四份压力报告按预期保持 **FAILED**：客户端均 exit 1 并回收、没有 timeout；四个 server 都 exit -9。
中断压力共 51,944 已知 ACK、1264 hit / 97 miss、**16 PUT unknown**（每客户端 4 个），不能记成 miss 或正常成功。
全部 9600 个计划业务请求保留：276 completed、16 failed、9308 not_attempted，因此 unknown 为 9324，而不是丢弃未完成请求后计算命中率。
kill 前 70 份活动观察保留健康、无 fallback、合法终局路径和实际进展；这些只证明进程活跃，不证明击中了特定内核 I/O 窗口。

### 独立原账复核

原生 code-reviewer 使用标准库独立重算、未导入作者验收函数：

- 冒烟：104 reports / 576 Prom / 208,447 events。
- 主测：216 reports / 1568 Prom / 1,550,052 events，1104 资源样本的 PID/start/CPU 单调性和 hash 有效。
- active：32 reports（含四份预期中断失败）/ 54,821 events；上一 cohort 链接、SHAKE payload、完整行分母、SIGKILL/回收和 cold reset 全部复核。
- 原始三组失败保留为失败；不以新检查点结果改写旧热点回填或策略收益。

主测从 Popen 到启动就绪为 0.488–1.049 秒；owned server 样本峰 RSS 51,092KiB、threads 29，最大相邻样本 CPU 约 35.012%（一个核为 100%）。这些是周期采样观察，不是生产恢复/资源 SLO 通过证明。

父审计脚本曾在主测读取压力大报告时误选 16MiB 通用 loader；只修正 `.omx/evidence/` 脚本为既有的 pressure 专用 128MiB loader，探针仍为 16MiB。
该问题发生在原生矩阵通过之后，**工作负载源没有变化、没有声称重新做了原生长测**；修正后父审计和独立标准库重算均通过。

### 构建与单测

- 本机 `build-test`：RelWithDebInfo、RDMA OFF / uring OFF，`cmake --build build-test -j2` 成功；43 新方法 17.985 秒，448 项定向 CTest 63.38 秒，零失败/跳过。
- Guest `/opt/dfkv-build`：Debug、RDMA ON / uring ON，重新构建成功；18 模块 **374 方法** 76.651 秒，451 项定向 CTest 71.59 秒，零失败/跳过。
- 主长测后本机构建再次成功，43 方法 16.126 秒，三个 recovery CTest 18.40 秒；全部原生验证后构建与三个 recovery CTest 又通过（19.79 秒）；CTest 含 Python 方法，不能将入口数与方法数相加当作独立用例数。
- 43 方法覆盖固定身份、keep-first、跨 cohort 拒绝、wrong payload/未知、donor 初始化、完整分母、错误指标、提前退出、进程超时/清理、错误检查点和严格快照失败；最终源与真实运行固定的 349 个输入 hash 一致。
- 六个 Python 文件 AST/py_compile、十个任务文件空白检查、CMake 三个真实入口和 Markdown 链接/围栏检查执行并记录。Ruff/Pylint 不可用；没有新 C++ 代码，不冒称本轮重跑 clang-tidy、全仓 CTest 或 sanitizer。

本轮只有验证工具/单测/报告变动，没有改生产 C++、评分、默认开关、C ABI 或磁盘格式。
**旧热点在满容量回填中出现的复读 miss 仍是保留的实际观察；新检查点恢复通过不代表它已被修复，更不代表 lifetime 业务收益胜出。**
正式 A/B 和生产默认切换均未开始。GPU、真实推理引擎采用和 TTFT 为用户排除项，不记作已通过，也不列为本轮阻塞。

### 安全收尾与落盘

本轮全部 owned cache/client/controller 已回收，端口 12000/12001/12010 空闲；349 源、二进制、VM 六 FD/backing 身份和 guest mounts/RXE/uring 最终重新核验。
96 个本轮 guest case 目录的 `slab_state` 64 字节 header 均 CRC 正确、clean marker=1；历史失败的测试结果仍保持 FAILED，clean marker 只表示进程最终正常关闭。
guest case 合计 allocated **2,255,675,392 字节（约 2.101GiB）**保留，没有删除共享 VM raw/qcow2。
温控正常停止并回收（exit 0，PID 已不存在）：1246 个样本只含 `/dev/nvme2`、`/dev/nvme0`，最高 42°C、critical_warning 全零，最大间隔 10.667 秒，无 alarm。
本轮未压测或 SMART 采样宿主 `/mnt/disk1`/`/dev/nvme1*`；不宣称其他宿主进程也没有 I/O。

本轮唯一证据根目录：
`/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/prefix-recovery/20261126T031016Z-878d3f1/`。
`freeze-r3.json`、三个 `*-r3-analysis.json`、独立审查、XML、`final-six-fd-proof.json`、`safety-closeout.json` 及 `retained-data.json` 保存全部索引。
运行时日志与压缩归档在宿主：
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/prefix-recovery-20261126T031016Z-878d3f1/`；仓库证据副本在 `/home`。

Guest 结果为 `/data/disk1/results/prefix-recovery-20261126T031016Z-878d3f1{-smoke,-r1-smoke,-r1-main,-r2-smoke,-r3-smoke,-r3-main,-r3-active}`。
单测报告另在 `/data/disk1/results/prefix-recovery-tests-20261126T031016Z-878d3f1{,-r1,-r2,-r3}`；本轮控制脚本在 `/opt/validation-recovery-20261126T031016Z-878d3f1{,-r1,-r2,-r3}`。
`/opt/dfkv` 与 `/opt/dfkv-build` 是共享隔离源/构建目录，不属于可随本轮缓存一并删除的清单。
缓存数据在 `/data/disk{1,2,3}/cases/sg-prefix-recovery-*20261126T031016Z-878d3f1*`，**精确 96 项路径/dev/inode/allocated/clean header 见 `retained-data.json`**。
清理时重新证明目标 VM、无活跃使用者和这份逐项目录归属，只清理本轮已确认 case；不要用宽泛通配符删除，不要为了清理这些目录而删除共享 raw/qcow2。

当前处于 AB7，恢复闭环完成；后续参数/资源边界和正式 A/B 门槛另轮处理，不自动扩展为生产切换。
前一轮见 [独立并发验证](DISK_LIFETIME_PREFIX_CONCURRENT_VALIDATION.md)，配置与恢复合同见 [集中操作入口](DISK_LIFETIME_R45.md)。

## 复跑入口（新标签，不覆盖旧结果）

先核验主机 backing 归属和温控，再进入可丢弃 guest；以下不是省略安全预检的宿主机压测命令。

```bash
# 仓库根目录，本机不连接缓存服务。
cmake --build build-test -j2
python3 -m unittest discover -s test/python -p 'test_prefix_recovery*.py' -v
ctest --test-dir build-test --no-tests=error --output-on-failure -R '^python_prefix_recovery'
```

Guest 中需 RDMA ON / io_uring ON 构建、rxe0 ACTIVE、RAM off/disk ACK、可用三挂载、端口 12000/12001/12010 空闲和 unlimited memlock。
`TAG` 每次取新值（最多 40 个 ASCII 字母数字/`_`/`-`）；输出和 case 目录必须不存在：

```bash
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/run_prefix_recovery.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases --data-root /data/disk2/cases --data-root /data/disk3/cases \
  --output "/data/disk1/results/prefix-recovery-$TAG" --tag "$TAG" \
  --cycles 3 --dwell-sec 10 --pressure-rounds 600 --pressure-interval-ms 200
```

带压力冒烟将 `cycles=1 / pressure-rounds=8`，其余主路径不变；active 单独用 `run_prefix_recovery_active.py` 的同一 build/data-root/output/tag 参数。
工具主动拒绝已有目录、非 disposable guest、非三独立挂载；但不自行证明宿主 backing 或建立温控，这些仍由外部监督承担。
不要直接重用 `.omx/evidence/` 中旧的 VM PID、monitor PID、停止标记和实验标签；它们是本次证据，不是下一次授权。

## 环境与结论边界

Ubuntu disposable VM，6 vCPU / 8GiB，Debug RDMA ON / io_uring ON；RXE loopback 不是物理 HCA 或跨机吞吐验证。
三 guest ext4 盘各四个 1GiB extent、总逻辑容量 12GiB；实际 slot 档为 1MiB/2MiB。
三 guest 盘只依托两块允许的宿主 NVMe：data1.raw 在宿主 disk2，data2.raw/data3.raw 在 disk3；不能冒称三个独立物理故障域。
RAM 关闭，PUT ACK=disk，payload write=buffered；watermark/background reclaim 关闭；生产评分九项默认值保持不变，诊断 override 与生产默认值分开。
同一 VM/文件系统/page cache 的进程重启，不是宿主重启、断电或冷物理盘测试；启动就绪耗时也不是独立恢复 I/O 或生产恢复 SLO。
资源样本仅 owned server CPU/RSS/threads；不代表客户端、VM、宿主、page cache、注册内存或锁等待的完整资源预算。
