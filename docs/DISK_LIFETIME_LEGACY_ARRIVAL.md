# 真实固定到达回放与 legacy 诊断基线采集

状态：**DIAGNOSTIC_ONLY / FORMAL_RUN_BLOCKED**。CPU 纳入，GPU 全部排除。
本轮把已有真实客户端和固定到达调度接起来，不再用无节奏 smoke 代替真实调度。
这仍是有界合成正确性样本，不是目标业务/生产性能基线；不运行 lifetime 性能探索，
不代定数值门槛、不启动正式 A/B、不改变生产默认。

依据：[正式协议](DISK_LIFETIME_AB_PROTOCOL_V1.md)、[准备包](DISK_LIFETIME_AB_PREPARATION.md)、
[阈值登记](DISK_LIFETIME_THRESHOLDS_TEMPLATE.md)、[验收索引](DISK_LIFETIME_ACCEPTANCE.md)。

## 1. 入口与复跑

在仓库根目录，使用已有构建；输出必须是父目录已存在的新目录，不覆盖、不续跑、不重试。
新入口复用既有 server launcher，但只启动 **显式 slab/legacy/disk ACK** 一个策略。
RAM/合并/RDMA关闭，buffered，fresh 独立数据目录；服务器与库摘要前后核对。

```bash
cmake --build build-test -j2
OUT="$(mktemp -d /tmp/dfkv-legacy-arrival.XXXXXX)"
python3 deploy/validation/eviction_baseline_smoke.py --build build-test \
  --fixed-arrival-legacy "$OUT/run"
echo "原始数据：$OUT"
```

默认 trace：2个初始化PUT、2个预热GET，测量阶段2轮“新对象PUT＋旧对象GET”，
各128字节，原计划间隔80ms，每次8请求。默认2 worker、等待队列16、
请求deadline1秒、客户端整个进程预算10秒。它们是小样本工具参数，不是生产推荐或验收阈值。

也可先生成/校验一次不可变 trace，再使用独立 fresh 目录复跑相同样本：

```bash
python3 deploy/validation/eviction_trace.py generate "$OUT/trace.json" \
  --objects 4 --rounds 4 --length 1024 --interval-ns 80000000
python3 deploy/validation/eviction_trace.py check "$OUT/trace.json"
python3 deploy/validation/eviction_baseline_smoke.py --build build-test \
  --fixed-arrival-legacy --trace "$OUT/trace.json" --workers 2 \
  --queue-capacity 16 --timeout-ns 1000000000 --process-timeout-sec 10 "$OUT/replay-2"
```

上限沿用有界客户端工具：初态对象≤8、轮次≤4、payload≤4096B，原计划跨度≤5秒；
不是服务器几何、业务负载或正式采样窗口限制。只支持既定唯一generation0写入与GET，
不假称已覆盖重启、回填、长保护窗口、目标几何或真实业务权重。
每策略1GiB稀疏容量、1MiB slot；默认payload写入仅512B，元数据、对齐与初始化I/O另计。
**不施加淘汰压力**；全部命中不证明淘汰收益或容量效率。

## 2. 调度、分母与时间口径

复用 `eviction_realtime.run`，没有第二套调度器：

- 客户端库加载和 worker×namespace 独立句柄全部预建后，才开始调度单调时钟原点。
  初始化、预热、测量各自保留原 trace 相对间隔，前一阶段物理调用排空后才开启下一阶段。
  阶段相对原点是显式协议，不宣称三个阶段共享不间断的全局到达轴。
- 同阶段计划到达固定，不因命中/缺失或慢调用缩小 offered trace。满队列、错过到达槽、
  等待超时都有逐请求终局；不静默丢弃、不补发追赶、不延长某一组计划窗口。
- deadline 从 effective scheduled time 起算，包含排队。记录原scheduled、实际arrival、
  admission、submission、started、completed、terminal/settled及排队/调度延迟。
  latency 是客户端调度包络，不是服务端处理或磁盘I/O耗时；service同样含适配器与计量开销。
- 同步 native 调用在 scheduler Condition 锁外运行。每个执行线程使用自己的预建句柄；
  一次请求逻辑超时不等于取消 native 调用，也不释放其物理槽、不关闭在途缓冲区。
  迟到PUT可能真正写入，但不追补逻辑ACK/ACK字节；物理完成另记。
- init必须全部ACK，warmup必须真实逐项完整字节命中。失败会阻断下一阶段，未执行项
  显式phase_blocked；planned、offered、started、completed与terminal分别报告，不混成一个分母。
- 测量账允许记录明确miss，以观察真实状态；本地 below-capacity 正确性收集器额外要求
  所有预期GET均hit，并核验服务器PUT/hit/miss/bytes/live objects/错误/淘汰计数的精确增量。

关闭句柄发生在所有已启动调用排空之后；setup、native调用、排空和close都受父进程总预算监督。
超时/崩溃/坏报告时保留计划总数，实际计数unknown，不能填0或伪造miss；不自动重试。

## 3. 初态与状态证明边界

真实 backend 的字段为 **acknowledged_objects**，不是 fake 的residents。
它是成功PUT的逻辑影子与字节身份账，不是磁盘驻留快照；晚于deadline的物理成功PUT也
可能更新影子，但不会产生逻辑ACK。只有init完全成功才能进入下一阶段。

实际 empty 初态来自专属 fresh slab目录＋运行前dfkv_disk_objects=0且磁盘标签匹配。
预热实际GET证明当时读取到期望字节；不据此推断后续持续驻留、未来命中或真实业务覆盖率。
独立句柄与适配器调用的线程重叠由受控事件单测验证，不冒充native I/O重叠或吞吐证明；
正常低负载真实样本不保证恰好观察到并发峰值2。

只用 additive scalar状态接口：status1才能是get_miss；其他状态异常失败，缺新符号则setup失败，
不走旧bool、EXIST探测、SG/dedup或自动重试。错误字节与部分native错误当前都可抛异常归入
backend_error并保留原错误文本；**wrong_bytes=0不能脱离backend_error及全样本成功单独证明安全**。

## 4. 身份、报告与复核

冻结输入/canonical trace、server/lib SHA、生效策略日志、显式环境、工具源码摘要、
调度参数与原始逐请求时间。更换源码、库、trace或配置须使用新目录；不修改旧样本续跑。
源码commit不是由binary反推的构建证明；目标资源/编译依赖/业务输入仍须正式登记。

保留：顶层report，legacy/data，server/stdout日志，前后metrics，client目录中的
trace/config、child-result、父report及stdout/stderr。真实child-result是envelope，
其中ledger保存调度器原账；状态域明确为acknowledged-logical-shadow-not-residency。

底层 `eviction_arrival.py` 可向**调用者明确指定且已获授权**的单个loopback服务回放：
它不证明服务策略、初态或ACK，不能单独把结果命名为legacy基线。推荐通过上方自建服务入口。
真实客户端父进程审计使用独立保存的外部report SHA，例如在服务退出后：

```bash
sha256sum "$OUT/run/legacy/client/report.json" > "$OUT/client-report.sha256"
REPORT_SHA="$(cut -d' ' -f1 "$OUT/client-report.sha256")"
python3 deploy/validation/eviction_arrival.py check "$OUT/run/legacy/client" \
  --report-sha256 "$REPORT_SHA"
```

SHA不是签名或独立审批；审计为失败样本返回非零，不能因原始失败账完整就视作通过。
父独立校验模型/配置/句柄关闭/完整逐项账、阶段与分母，不能只相信passed或汇总。

## 5. 安全与剩余正式门禁

本地 tiny launcher 拒绝全部宿主/mnt输出，并拒绝读取或执行/mnt/disk1构建/输入资产。
禁止压测/mnt/disk1继续有效；不调用Docker/VM、裸盘写入、全局drop_caches或内核调整。
仅创建自己的子进程/目录，退出有界TERM/KILL/reap；未知故障样本保留，不替换为好样本。
本机仓库与/tmp位于mapper home/root，本轮仅小规模单元/合成I/O，非真实盘压力运行。

尚缺：真实业务trace与权重、目标资源、真实目标A基线、独立M/O对照、职责/可见性审计、
F01–F05逐子项数值冻结及正式全场景/多轮交替采样。已有tiny trace窗口不满足W01–W09。
本工具的数据不能事后签为正式样本；GPU排除也不解除其他门禁。

## 6. 测试入口

```bash
cmake --build build-test -j2
ctest --test-dir build-test -E '^Gpu(NodeDedup|DedupChild)\.' --output-on-failure
# 定向回归，包含新调度接缝、真实适配器与旧fake/阶段/客户端契约：
ctest --test-dir build-test -R '^python_eviction_' --output-on-failure
# 本地实际服务用例（含原双策略正确性及新增legacy固定到达，不是性能探索）：
DFKV_BUILD="$PWD/build-test" python3 -m unittest discover \
  -s test/python -p test_eviction_baseline_smoke.py -v
```

新纯测试由python_eviction_scheduler_backend/python_eviction_arrival收集；native ctypes用例
沿用python_plugin既有sanitizer排除规则。测试覆盖完整成功、事件控制并发、排队/满队列、
超时后在途槽/迟到PUT、明确miss与unresolved/异常、setup/close失败、异常退出与报告伪造。
初次真实服务验证发现transport mode文本不是裸tcp而是tcp(rdma-not-requested)，已用实际
C ABI契约修正并增加反例，不通过放宽完整分母或取消真实用例来解决。

最终原始证据位于仓库根目录 `.omx/evidence/legacy-arrival/20261122-b08ea6a/`。
实际重复验证使用同一原始trace：objects4/rounds4/1024B/80ms；两个fresh legacy样本
各16/16请求完成、8192B PUT ACK与8192B GET完整命中字节，四句柄全部关闭、server/child正常回收。
两次实际peak_running均为1，不能据此宣称已测到native并行峰值或饱和吞吐。
`raw-audit.json` 与两份外部client-report SHA用于重建审核；配置/trace/库/工具摘要一致。

落盘数据：该证据目录的 `legacy-1/legacy/data`、`legacy-2/legacy/data`；
初次失败诊断为 `probe-first/legacy/data`，其client在transport标签检查阶段停止、零请求执行，
报告保留planned8/known completed0及失败原因，未改为成功样本，未用它参与正式统计。
初次native单测错误与源码修正记录也保留在launcher-native-first.log与review记录中。
源码修正后不覆盖旧报告；最终冻结样本使用新的legacy-1/legacy-2目录。

本轮新增30个测试方法；独立针对相关三个模块执行41方法全部通过、无跳过（8接缝＋17适配器＋16launcher，
含新增真实legacy固定到达与既有双策略正确性用例）。最终完整非GPU CTest结果另在下段登记。
Python编译/AST、差异空白与本地文档目标检查通过；Ruff/Pyflakes未安装，未冒报lint/typecheck。
本轮无C++改动、无额外sanitizer/ON-HCA/io_uring/压力或性能证明。

### 最终完整回归（本轮源码冻结后）

GCC13.3.1 / RelWithDebInfo，`build-test`，RDMA=OFF、io_uring=OFF。
构建成功（CMake重新生成；无C++待编译）。完整非GPU CTest：
**1543选择＝1518通过＋25跳过，0失败，251.43秒**；11个GPU条目显式排除。
25项跳过为24项etcd/MDS环境条件及1项buffered配置不适用的direct专用项，不计通过。
新增纯测试与实际native测试均无跳过；没有把Ctest条目数与Python方法数相加。
独立代码审查无未解决项，最终7个Python源码/测试文件与冻结摘要一致。
