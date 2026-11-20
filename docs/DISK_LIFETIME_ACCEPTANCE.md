# 磁盘寿命淘汰：验收收敛与剩余实施清单

最新增量：SG 批量 GET 新增 C++ → C ABI 的 direct-only、single-pass 状态链路；
旧 SG bool/dedup/重试路径保留，Python SG 与真实设备验证未包含。
契约与本轮验证见[状态接口第11节](CLIENT_GET_STATUS_COMPATIBILITY.md)，不构成正式 A/B 放行。

最新增量：新批量C ABI/Python链路已完成隔离RXE/io_uring复核，完整ON构建、44目标C++、
56个Python方法及两策略8组场景预期符合。direct-only链路证据已补齐，SG/dedup和正式A/B仍后续处理。
见[状态接口第10节](CLIENT_GET_STATUS_COMPATIBILITY.md)及[冷盘说明第15节](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。

最新增量：批量 C ABI → 验证用 Python → 受监督报告闭环已接通，显式batch模式保留完整逐项分母，
旧库hit0仍unresolved、原scalar默认不变；新TCP模式仅用于明确选择的正确性冒烟。
见[状态兼容方案第9节](CLIENT_GET_STATUS_COMPATIBILITY.md)，不扩大为SG/dedup/RXE或正式A/B放行。

最新增量：新增兼容的批量 GET C ABI 状态接口，调用级失败与逐项结果分离，旧接口保持不变。
Python 本轮尚未接入，SG/dedup 与正式 A/B 仍后续处理，见[状态兼容方案第8节](CLIENT_GET_STATUS_COMPATIBILITY.md)。

最新增量：C++直接批量GET状态契约/共享核心已完成，132相关CTest与两策略真实RXE/uring混合批次通过。
批量C ABI/Python及SG/dedup状态仍未接通；新接口明确绕过去重，不宣称旧dedup性能等价。
见[状态兼容方案第7节](CLIENT_GET_STATUS_COMPATIBILITY.md)。

最新增量：已完成新C ABI＋验证用Python单键接口的隔离RXE/io_uring小规模复核，
23项C++、31个Python方法及8个场景判定通过；批量/SG/dedup和正式A/B仍未放行。
见[状态兼容方案第6节](CLIENT_GET_STATUS_COMPATIBILITY.md)及[冷盘说明第13节](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。

最新增量：O5/AB7前置已接通新增C ABI＋验证用Python单键GET状态，
仅明确NOT_FOUND计miss；旧库保留unresolved。严格冒烟仍hit-only，
批量/SG/dedup及正式A/B未放行，见[状态兼容方案第5节](CLIENT_GET_STATUS_COMPATIBILITY.md)。

核查版本：2026-11-17，源码基线 `0cde375`；阶段 **DOC6 操作说明收口**。
O5bd 原清单由 O5be/O5bf/O5bg 增量更新；不将历史日志视为本轮重新执行。
本轮用户已授权隔离 Docker/VM 内实际 RXE＋io_uring 诊断，解除此前该项后置安排；
当前执行状态及复跑边界见[Docker 验证说明](DISK_LIFETIME_DOCKER_VALIDATION.md)。
首轮诊断发现两项全 ON 连接关闭失败；后续最小修复已通过实际RXE回归及全量ON/OFF，
详见验证说明第9节；修复后的36＋6组端到端场景已实际复跑通过，见第10节。未因此关闭 O5/O12 或正式 A/B 门禁；下文旧包的后置/skip记录仍是历史证据。
新增[快速淘汰冒烟配方](DISK_LIFETIME_QUICK_VALIDATION.md)：默认时间参数不变，
小容量预填充约19秒实证水位淘汰，热点缺失差异单列，不作性能放行。
固定路由的即时/185秒延后施压四组诊断已完成，阶段证据及解释边界见配方第5节。
三轮持续交错重复已完成：本轨迹默认lifetime热点缺失率约65%、legacy为0，详见第6节；
这是保留性风险证据，不是lifetime收益或默认切换放行。
保持公式的P180/60/30隔离首筛已完成，热点缺失率64.89%/21.64%/0，见第7节；
仅支持候选继续验证，默认仍180秒。新增90秒首次复用反向代价验证（第8节）：
P180首次64对象全命中，P60/P30/legacy全缺失；该90秒结果仍是单轮固定顺序。
后续45/185秒正反序16组实测完成（第9节）：45秒四配置均全命中，185秒均全缺失，
正反结果一致；未重跑90秒，仍不支持缩短默认窗口或宣称最优P。
新增已有热点＋三档首次复用混合首筛（第10节）：四配置热点12928/0，45秒组均命中、
90秒组仅P180命中、185秒组均缺失（各32对象）。背景预热/分组/探测频率不同，
不与此前轨迹假装单变量比较，也不据此批准默认切换或宣称全业务最佳参数。
同一混合场景反序P30→P60→legacy→P180已完成（第10.4节）：四配置热点、三档首次
复用及末扫结果均与历史正序一致，未观察到本轮命中结果的顺序影响；正反各一组，
不证明普遍顺序无关或性能等价。仍不翻默认，业务混合分布与正式A/B门禁保持。
持续新到达对象的早段抽样现已接入supervisor并完成真实四配置首筛（第11.3节）：
四配置热点全命中，legacy三档首次全命中；P180的185秒组全缺失，P60/P30的90/185秒
组全缺失，P30的45秒组额外缺失13/128。仅前12批抽样及单顺序各一组，保留该风险，
不宣称全业务优劣、最佳P、正式A/B通过或O5整体验收完成。
因disk1温度告警，后续测试永久排除该盘直到用户明确变更；系统/backing/数据/日志
改用disk2/disk3的独立rootless环境，旧三盘VM/容器已停止、样本保留。新布局下已
重新运行新到达正序＋反序8组（快速配方第12节），逐配置各cohort汇总及热点/末扫
计数一致；只证明本轮未观察到计数的顺序变化，不是全业务/性能等价或唯一根因结论。
冷盘映射、温度停止线、独立socket及复跑边界见[冷盘说明](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。
缺失后回填已接入自动验收并完成冷盘四配置首轮（快速配方第14节）：回填ACK依次
P180128、legacy0、P60256、P30269，校验均命中，四组热点12928/0；逐对象/完整GET/
PUT/payload/计热对账过。逻辑payload放大约1.0082/1/1.0164/1.0172，不冒充物理写放大。
73工具单测及host/guest目标CTest、真实RXE1+ring5+Serve9通过，独立原始审计PASS。
业务权重未假设；单顺序早段抽样不关闭O5或正式A/B门禁，默认不变。
双尺寸紧凑几何首轮已完成（快速配方第15节）：legacy/lifetime各36544GET/12800PUT，
两真实class实际计数、字节和终局对账通过；80工具单测及目标CTest/真实RXE/ring检查过。
保留不利结果：大档压力probe legacy11776/0、lifetime1696/10080。水位0，实际覆盖后台
class回收5056slot和extent steal3事件，不能称多尺寸水位通过或默认策略收益/上线放行。
另完成独立40/50双尺寸水位诊断档（快速配方第16节）：WM4197/4237，原始三盘阈值、
完整分母及两实际class对账过；86工具单测与目标CTest、真实RXE/ring检查通过。
这不证明默认92/88表现；本轮lifetime两档热点均更差，风险保留，不改默认/评分。
独立8读流＋主写流首轮已完成（快速配方第17节）：8独立reader handle/2writer handle、
实际C ABI包络peak8、中点WM1596/1575、最终两组WM4804，410112GET完整对账通过。
96工具单测、目标CTest/真实RXE/ring与独立审计过；lifetime仍有176981次压力miss，
闭环请求总量不同，不当成正式策略/性能A/B；不是极限压测、长期竞态或O5放行。
正式A/B尚未启动，业务权重未假设。
正式 A/B 准备协议见[协议 V1 与冻结前置清单](DISK_LIFETIME_AB_PROTOCOL_V1.md)：
固定对照/回放/场景/资源边界/交替与证据规则；旧 baseline、独立职责及逐项数值仍缺，
不是“阈值已冻结”，不授权正式采样。
协议第8节有离线合成trace，第9节新增fake backend虚拟时间回放及排队/超时/终局测试；
仅模型计量。第10节新增实际monotonic时钟/线程fake回放，验证调度排队；
第11节新增父进程超时/异常退出监护与失败报告（直接fake子进程），仍无真实服务、
业务trace或正式冻结包，不作native/磁盘性能结论。第12节新增显式fake三阶段：
物理排空barrier、严格ACK/hit与payload初态校验，planned/offered及blocked分开；不证明真实热度初态。
本文是验收索引，不修改原规格，不是放行报告，也不将测试覆盖率换算为完成百分比。

## 1. 权威输入与状态口径

- 工作树根：`/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1`。
- 原规格：`/home/server/work/DingoCache/dfkv-磁盘淘汰算法改造-spec-定稿版-v2.33.3.md`，重点 §10、§11、§11.1。
- 批准计划：上述工作树的 `.omx/plans/prd-disk-lifetime-eviction.md`。
- 测试矩阵：上述工作树的 `.omx/plans/test-spec-disk-lifetime-eviction.md`，U0–U10、I0a–I6、E0–E6。
- 交接入口：`/home/server/work/DingoCache/dfkv-磁盘淘汰改造-文档导航与实施交接说明-v2.33.3.md`。
- 下文代码、测试和证据路径均相对工作树根；`.omx/` 是本地证据，通常不随 Git 分发。
  其他模型或验收者缺这些文件时必须取得原证据包或重跑，不能仅凭本文接受通过结论。

状态分类：**已有功能/证据**仅覆盖所列范围；**实现缺口**需改代码；**验证缺口**不等于代码缺失；
**外部条件**需要真实环境、业务输入或独立人员。复合状态不应压成一个“通过”。
测试名称/源码存在不证明执行；历史日志只证明当时版本。O5bd清单轮未重跑全套C++/sanitizer，O5be证据单列。

当前结论：P0/S1/S2/L12/Q3/D4/R45 已有实现及回归积累；显式 slab/lifetime 可用，
默认仍 staged legacy。O5 的观测主体已落地，但 **O5/E4 尚未整体验收**；
DOC6 操作说明已集中整理至 [操作入口](DISK_LIFETIME_R45.md)；这只完成文档包，
不解除 O5 外部验收门禁。AB7、FLIP7 不能由本文代为完成，生产部署另需授权。

## 2. §10 逐项观测收敛

入口中 `allocator` 指 `src/cache/slab_allocator.{h,cc}`，`Store` 指
`src/cache/disk_slab_store.{h,cc}`，`Node` 指 `src/cache/kv_node_server.{h,cc}`。
指标的精确定义仍以 [METRICS.md](METRICS.md) 为准，不能将表中的概括当作新口径。

| ID / 原要求 | 当前入口与真实证据范围 | 状态与关闭条件 |
|---|---|---|
| O01 生效 engine/requested/effective、适用配置、无高基数标签 | `ResolveDiskEvictionConfig` (`src/cache/disk_cache_group.cc`) 相关配置入口、Node 指标；`test/cache/disk_lifetime_public_test.cc` 的 `DefaultAndExplicitLegacyIgnoreInactiveNumbers`、`ExplicitFileLifetimeRejectsBeforeOpening`、`MalformedApplicableNumbersRejectWithoutOpening` | 已有配置/拒启测试；保持 file N/A、legacy inactive，不以 lifetime 字段存在宣称生效。新指标继续检查标签，不加入 key/request ID。 |
| O02 成功 victim、slot/payload 字节、互斥路径 | allocator class/watermark/donor/background 统计→Store/Group→Node；`LifetimeWatermarkStoreTest.PayloadAndSlotBytesConserveInBothPolicies`，各路径指标测试 | 已有功能/确定性守恒；正式路径占比、回收速率仍待 E1/E5 同负载验收，不能把互相重叠的统计直接相加。 |
| O03 可见 payload/tenant 与仍占槽/保护桶分离 | Store 延期 Remove/失效回调与 allocator 物理账；`test/cache/disk_slab_store_test.cc`、`disk_lifetime_public_test.cc` 的 `VisibleRecoverySurvivesImmediateRemoveFailure` | 已有故障回归；可见移除不等于持久失效或 slot 可复用。故障必须维持 unhealthy，安全项零容忍。 |
| O04 类内片/单请求累计/后台整轮/水位/全 E donor 扫描 | allocator 预算、Store PUT owner/后台循环；`slab_lifetime_background_test.cc` 的 `EntireAllocatorLockHoldVisitsAtMost1024`、`FourTimesTickCapPersistsAcrossAllocatorCalls` | 已有预算测试；扫描上限不是 I/O 墙钟上限，锁成本和持续压力分布尚未验收。 |
| O05 水位跨档公平、pin、双索引/游标 | `slab_lifetime_watermark_test.cc` 的 `LongPinnedBandDoesNotStarveAnotherBand`、`WinnerIsReplenishedBeforeNextCrossBandChoice`、`HeatMigrationRepairsBothIndexesBetweenTicks`，类内队列测试 | O5be补充path×band visits/candidates/pin_skips公开账，按现有付费检查点计数，跨class聚合；新 `slab_lifetime_band_scan_test.cc` 和真实双盘metrics用例验证。仍不证明逐class公平或持续压力进度分布；最多256件不是每tick必达量。 |
| O06 真实 W/R/O1/O2/3+ 与 R 到期滞后 | allocator read-class ledger、Store 观察时排空；`slab_lifetime_read_class_test.cc`、`disk_lifetime_read_class_test.cc`；METRICS 的 Logical read-class observations | 已有功能；R 逻辑截止含等号，桶宽导致有界观测滞后，不物理迁评分链。恢复虚拟读不算实际读，饱和分类不能证明无重复应用。 |
| O07 budget→前台 CacheFull / 同key完成、fallback/零进展 | typed Cache/flush 结果，Store PutFlight，RAM retry；`ram_tier_test.cc`、`ram_tier_wiring_test.cc`、`disk_cache_group_test.cc` | **O5bf命名观测接缝**：后台RAM已dispatch item的single/batch终局（completed/cancelled/retained）、前台三个RAM等待API的owner/duplicate返回、Store真实flight follower attached/completed公开账分别分区，保留CacheFull细因与IOError。三者分母不可相加；不是完整Node PUT或所有传输分母，正式分母/阈值仍未登记。 |
| O08 跨class整条、受保护损失、归还完整extent | donor/bucket/reclaim 统计；`slab_lifetime_donor_test.cc`、`slab_lifetime_background_test.cc`；精确选择最多8条/8192 resident | 已有有界选择/失败不释放测试；全 E 粗扫、选中后整条清表与sync成本仍需分别测量，不能藏入8192预算。 |
| O09 元数据I/O/回收耗时/锁等待与持有 | Store table I/O、allocator 路径统计；Node notification wait/body/held 与 sampled completion；METRICS 对应小节 | **部分实现/需定界**：已有record_writes/table_syncs/bind_wipes/metadata_io_errors及部分路径耗时，不是所有互斥路径完整syscall/byte/lock账，也不代表成本验收。采样/嵌套口径不能累加为总耗时；GET/PUT p99、全 E/整条sync锁成本仍 open。 |
| O10 RAM前台/后台、单/批预算与真实IO、ACK/最终丢弃 | `src/cache/ram_tier.cc` typed flush；Node `dfkv_ram_post_ack_flush_failures_total`；`RamTier.TypedFlushFailureReasonsControlRetryAndHealth`、`WriteBackFlushFailureIsPostAckAndUnhealthy` | 已有真实 publisher/健康回归；预算失败不是IO故障、未ACK失败不是ACK后丢失。新模式 RAM write-back 必须显式disk ACK，告警不能替代闸门。 |
| O11 ACK后丢失立即可见且保留通用告警 | `deploy/observability/alerts.yml` 的 `DfkvRamPostAckDataLoss`：counter>0、无for；`alerts.test.yml` 与 Python contract，O5bc 原始日志 | 已有专用规则与真实promtool测试；“立即”是成功scrape后的首次匹配eval。外部Alertmanager通知/事件留存未验收，reset/stale不等于丢失恢复。 |
| O12 RAM/disk、合并leader/follower、传输完成路径的逻辑读 | Node `RecordSyncReadResult` 两API终局6分类；真实TCP两新case及refusal增强见O5bb；PreparedSource/PULL grant子分区，物理pread/uring独立统计 | **O5bg实现已解码/进入Serve调度的PULL独立终局账，整体验收仍未关闭**：sync/uring分区，授权成功/最终错误/未决定取消一次结算，决定后未PostSend独立注记。排除未解码原始WC FIFO/非法frame/RELEASE/客户端完成；不是所有传输分母。CPU生产账本测试不证明实际Serve；真实Serve回归硬件后置，不将skip或grant helper计为通过，见METRICS终局表。 |
| O13 通知接受/无副本/未绑定/错代/race_lost/重复 | Store `NotifyLifetimeReadSuccess` 相关核验/原因分区；`disk_lifetime_read_test.cc`、`ram_tier_wiring_test.cc`、`prepared_read_test.cc`、CPU PULL测试 | 已有身份/时序回归；unbound不证明无副本，applied/success不证明所有适用请求都发通知。需独立完成账+同代来源资格联核，真实PULL硬件仍后置。 |
| O14 completed→applied、通知锁成本 | `disk_lifetime_read_completion_test.cc`、wiring；METRICS 的 Logical completion to lifetime application / Sampled notification processing cost | 已有采样及排除原因；没有形成通知开销的性能放行结论。锁耗时与该延迟不是同一变量，禁止拼接均值/max。 |
| O15 恢复数量、真实零读W、评分档、连续重启存活 | `disk_lifetime_public_test.cc` 的 `ConsecutiveCleanRestartsResetOnlyVirtualHistory`、`ImmediateWatermarkCannotResurrectRecoveredKeys`、`ImmediateDonorRebindDoesNotResurrectOldClass`；O5ax/O5ay回放与恢复boot-age | 已有状态/短轨迹验证；boot-age不是跨进程连续生存年龄。没有全负载连续存活/过度保留业务验收，虚拟两读绝不加入真实读数。 |
| O16 成功受害者年龄/unknown、正负分、KEEP保护比例 | class/watermark/donor/background final pre-wipe快照；`SlabLifetimeWatermark.AgeExcludesRecoveredHistoryEvenAfterRealRead`、`ProtectedCountUsesFinalPreWipeTime`、`ScoreBinsSeparateSignsIncludingZero` | 已有功能与边界；失败不计成功，unknown不伪造历史年龄，正负分分开；显示bin不是放行阈值。 |
| O17 首读/再读间隔及幸存偏差 | allocator accepted application interval→Store/Group→Node；`LifetimeGhostMetricsTest.ReadIntervalsExportBothDisksAndResetAfterCleanReopen`，O5az/O5ba | 已有区间/真实多盘非零聚合与重启unknown测试；间隔是applied时刻，非客户端端到端时间。不能只用存活样本推导最佳寿命，需真实trace/重访。 |
| O18 bounded ghost覆盖/有效窗/后续真实重访 | `disk_eviction_ghost.{h,cc}`、Store Drain/entry witness、`disk_eviction_ghost_read_test.cc`；O5ap–O5av | 已有容量/丢失边界/同一entry witness终局miss关联；窗为成功settlement前缀且左开右闭，不是所有物理驱逐的完整日志，也不是因果损失率分母。容量0禁用。 |
| O19 重复PUT、重算/业务损失关联 | 已有ghost实际miss关联，但不提供推理引擎重算/前缀/TTFT数据 | **外部数据+验证缺口**：重复PUT不得直接算驱逐miss；取得同请求trace、缓存代际/驱逐关联、真实重算token/TTFT后，按冻结口径出结果。不为缺业务数据新增伪业务counter。 |

## 3. 安全硬闸门：不能被性能收益抵消

以下把 §11.1 的安全长段映射到原测试矩阵；不是宣布所有组合已经验收。
表中路径均在 `test/cache/`，I3 另含 `test/transport/`。

| ID / 要求 | 已有入口/测试范围 | 仍须守住的关闭条件 |
|---|---|---|
| S01 不返回错误bytes；同步GET最后pin失效修订结果再计热 | Store release/延期Remove，`disk_slab_store_test.cc`、`disk_lifetime_read_test.cc`；I0a/I0b | 成功Remove原住户消失时可逻辑成功但不追补热度；失效失败终局IOError不热，旧slot不可复用。 |
| S02 Remove/promotion/重写跨层可见序；write-around/noRAM不可假定Put锁；diskACK/flusher无环 | Node per-key fence、coalescer epoch、RAM durable identity；`ram_tier_wiring_test.cc`，I1/S2 | 每种写路径按可控交错核对bytes/最终可见，错代跳热不能代替返回值正确。旧故障证据按版本归档，不凭字段存在证明无环。 |
| S03 同代通知先拿盘锁不漏/错热；淘汰先行单列race_lost | Store D→A验证、resident identity；`disk_lifetime_read_test.cc`、wiring；U4/I2 | RAM无盘副本排除必须有来源资格，unbound不等于无副本；未提交/错代不能加热，真实读与虚拟恢复分开。 |
| S04 I/O pin不复用，持久化失败停止；幸存记录恢复正确/旧class不复活 | allocator callback、Store health/recovery；`disk_slab_store_test.cc`、`disk_lifetime_reclaim_test.cc`、`disk_lifetime_public_test.cc`；I5 | 异步取消/释放也保持所有权；失败不得换donor继续，部分盘表改变按原脏启动契约处置，不承诺盘上无变化。 |
| S05 类内预算/全pin/并发竞争有界，owner/followers终结 | `slab_lifetime_eviction_test.cc`、`slab_lifetime_background_test.cc`、Store PutFlight及wiring；U6/I4 | 累计≤2N+2访问、每锁片≤1024、机会计0访问重试；预算终局不留下flight。同步pwrite不可被该访问预算抢占。 |
| S06 非法适用配置/几何先于任何盘构造；全目录只读预检 | Group startup + `DiskSlabStore::ValidateExisting*`；`disk_lifetime_public_test.cc` 双盘clean/dirty快照测试；U0/E0 | 首次无文件可建，拒启时所有目录slots/epoch/marker保持不变；预检不防并发改盘，构造复检不是无理由重复。 |
| S07 KEEP0合法、H>INT32_MAX拒启、anchor运行上界安全停接排空 | lifetime config/math/clock；`disk_lifetime_math_test.cc`、`disk_lifetime_clock_test.cc`、public/admission tests；U1–U3/E0 | 不wrap、不静默切legacy；H与UINT32_MAX-H边界、统一boot origin、干净重启归0必须共同满足。 |
| S08 legacy/inactive/file隔离；新模式RAMwriteback显式diskACK | config/parser/startup + `DiskLifetimePublicTest.RamWritebackRequiresDiskAckBeforeDirectories`；U0/I6 | legacy非法LIFE数值不阻断，file N/A，不把仅lifetime ACK闸门扩散；默认仍legacy。 |
| S09 单/批budget与IO分项；纯budget CacheFull不虚假ACK、不unhealthy/MarkBad、不丢ACK脏数据 | RAM typed结果、Group/Node/client status；`ram_tier_test.cc`、wiring、`disk_cache_group_test.cc`、`test/client/kv_client_health_test.cc`；I4 | 连续超旧重试次数、混合IO、同keyfollowers、关闭取消均需真实终局；IO仍按自己的失败契约隔离并告警。 |
| S10 格式/旧策略/RAM CLOCK兼容；全部成功路径安全 | public模式重启、legacy allocator/RAM套件；I3/I5/I6 | CPU功能与回归已有证据，但真实RDMA/RXE＋io_uring授权/取消/READY/断连/重复RELEASE仍需实际路径验证，缺硬件保持open。 |

## 4. §11 的完整实验与资源要求

| ID / 要求 | 当前证据边界 / 未闭合项 | 关闭条件 |
|---|---|---|
| A01 可比二进制/ACK/容量/几何/RAM/并发/预算/trace/热度/时长 | 有显式policy/diskACK启动和模式重启功能测试；没有正式旧基线报告 | 同一新二进制legacy与lifetime都显式disk ACK；现役RAM ACK另作迁移参考；另保存v2.33.0旧二进制行为对照，不混入算法收益。 |
| A02 冷启动/稳定热点/scan/长前缀/早读vs未读/混合尺寸/持续与突发写/nearfull/allpin/IO | 功能套件覆盖若干安全场景，不是整个性能矩阵 | 冻结trace/config与初态；每场景独立原始样本、故障/零进展分母，不能用总体平均遮盖首次早读撤BOOST。 |
| A03 clean后0–15min、dirty、连续restart、切换、旧冷多数+新热 | O5ax三次干净restart短轨迹；O5ay179/180/181及899/900秒竞争；public dirty/切换 | 已有隔离恢复功能证据；完整连续生存/过度保留/业务损失及正式负载回放仍未测，不能将boot-age当跨进程存活。 |
| A04 两索引同档/队头同now；cursor仅已检最低；pin影响与256/1024 | Q3/D4单测 + M1/2/4/63 public实际队列/桶 | 继续保留确定性断言；持续水位压力下进展分布、低水位/预算/IO成本需完整实验。 |
| A05 同逻辑trace×RAM开关×合并开关×M1/2/4/63及通知竞争 | wiring参数矩阵、真实4TCP convoy；CPU PULL通知helper | 不等于实际RDMA Serve/hardware；独立请求终局分母、适用同代副本资格、真实RAM通知竞争成本仍须闭合。 |
| A06 key/byte命中、前缀token、空间/slot载荷、驱逐重访/重算/TTFT | Node6类终局只覆盖两API；ghost部分窗；空间账/bytes已有 | 明确有效请求与字节粒度（含empty/invalid/截断/失败）；引擎前缀/重算/TTFT缺数据写未测，不用applied或重复PUT替代。 |
| A07 GET/PUT分位/吞吐/回收率/锁/CPU/内存/元数据IO | 观测计数及静态布局已有，不是已测性能 | 先落实§11.1人员隔离；合法4KiB与目标几何、M1/2/4/63核算slot/链头cursor/桶token/identity/RAM/coalescer凭据、动态容器、恢复峰值RSS。 |
| A08 完整资源与全E/大extent成本 | `MetadataMemoryUsage()`六类backing及lifetime向量只是局部；map节点、ghost分配、heap开销、fixed对象与恢复峰值未完整核算 | 不以sizeof微指标/向量容量之和关闭；另列全E粗扫、8192精查与选中后整条失效sync。不得通过缩小合法几何规避。 |
| A09 ≥3轮交替、样本数/离散度/相对变化 | 尚无正式同ACK A/B；功能repeat/随机/故障注入不属于正式样本 | O5旧基线和数值冻结后，独立完整正式样本≥3轮交替；探索样本不能复用，改阈值须新版本重采。 |
| A10 DOC6、FLIP7与部署 | 默认未翻转，ACK/重启回滚说明已集中整理，实际迁移与完整验收仍待完成 | 配置/README/DEPLOY/ARCHITECTURE/METRICS交叉核对；回滚显式legacy且重启，不能承诺在线无损。AB通过后单独默认切换+单测；部署另授权。 |

## 5. §11.1 登记表（版本0，未冻结，禁止据此放行）

可复制的完整交付格式：[版本化阈值登记模板](DISK_LIFETIME_THRESHOLDS_TEMPLATE.md)、
[正式 A/B 报告模板](DISK_LIFETIME_AB_REPORT_TEMPLATE.md)。两者仍为 DRAFT，无人员/
数值/冻结/采样证据，不能因模板已提交就开始试跑或声称已放行。本节汇总状态不替代
逐指标登记；实际登记表及原始证据必须另存不可变版本。

不填写猜测阈值。以下每行必须在独立登记版本中填写全部字段，原始数据不可仅留摘要。
本表既不分配人员，也不授权开始探索性能试跑。

| 登记项 | 当前值 | 完成条件 |
|---|---|---|
| 探索样本保管者 / 阈值制定者 / 独立验收者 | 未登记 | 实际姓名/职责、分离约束与可访问数据版本；保管者与阈值角色分离。 |
| 样本位置/权限/访问日志、配置和trace哈希、硬件版本 | 未登记 | 首次新模式性能探索前落实隔离；O5冻结前阈值制定者不得接触新模式待验收指标。 |
| 旧baseline：disk ACK可比组、现役RAM ACK迁移参考、v2.33.0行为 | 无可供本清单验收的完整报告 | 先取得目标负载/容量/恢复轨迹旧baseline，记录版本、flags、resolvedpolicy/ACK和原始输出。 |
| 探索/正式边界、冻结版本、可见数据清单 | 未登记 | 泄露或阈值变化登记新版本；独立重采全套正式样本，不能事后放宽。 |

以下五行分别对应原规格五个数值闸门，**全部未冻结**。
每行必填：指标、数值阈值、单位、分母、窗口、允许波动、统计判据、负责人、
可见数据版本、配置/trace哈希、冻结版本、探索/正式边界。

| ID / 闸门 | 必须覆盖的子项 | 数值状态 |
|---|---|---|
| F01 业务收益/回退 | 前缀复用、驱逐后miss、重算token/TTFT；至少一项独立业务收益达标，其余回退受限 | 全部待旧基线后登记，阻断正式验收 |
| F02 性能与锁 | GET/PUT p99/吞吐；稳态/高压/重启窗；RAM→disk通知额外p99与锁等待/持有 | 同上 |
| F03 读覆盖与内存 | 适用同代RAM/leader/follower读；盘锁接受后漏计/错代零容忍；race_lost/unbound比例；常驻增量/恢复RSS | 同上；安全零容忍不因数值表空白而豁免 |
| F04 恢复/利用率 | clean/dirty误伤、连续重启过保留、真实零读恢复W、评分档分布、整条归还/空间 | 同上 |
| F05 donor/预算/零进展/IO | fallback、每请求访问/重试、flight完成率、失败比例/持续时间；前后台flush、CacheFull/IOError、ACK后drop、预算背压和落盘耗时 | 同上 |

## 6. 后续小步：不再无限拆可选指标

1. **先按三个真实缺口收口，不继续添可选微指标：**
   - **O5be：逐评分档检查机会/候选/跳pin账（代码包已实现）。** 复用现有M+1档与有界扫描，
     不创建另一套链；以长pin前缀和tiny budget正向握手证明轮转，公开账与总量一致。
     这是功能观测缺口，不是要求本轮先跑性能。类内/水位字段作用域须在实施前明确。
   - **O5bf：typed flush与follower终局公开账（代码包已实现）。** 区分前后台、single/batch、CacheFull/IOError，
     只在真实终局处计数，重试/提交不算完成，混批与同key等待者各自守恒。
     已有typed语义不重写，避免将观测修复扩大成写回算法重构。
   - **O5bg：已解码PULL Serve 独立请求终局账（代码包已实现）。** 先列同步/异步完成、队列取消、gate拒绝、MR失败、
     授权后取消的终局表；统一一次性终局接缝，不复用grant通知计数冒充请求分母。
     新增真实Serve路径回归；硬件后置时明确未执行部分，不能用只调用helper的CPU测试
     宣称整包PULL完成。若需拆包，按可证明的完整终局分区拆，不按“新增一个字段”拆。
   每个代码包同提交含真实单测、构建与适用sanitizer；不是一轮合并实现三组。
2. **并行前置准备（不跑性能）：** 确认人员/隔离/旧baseline方案，准备完整资源核算表和业务trace字段；
   未有外部条件不虚构完成。实际RDMA/RXE＋io_uring继续后置，单列I3矩阵。
3. **DOC6文档包已集中收口：** 参数/双索引/恢复/ACK迁移/背压/受控重启/回滚说明
   见操作入口，README/ARCHITECTURE/DEPLOY/METRICS 互链。文档齐备不是迁移或 O5 放行。
4. **条件具备后：** 旧baseline→登记冻结→独立≥3轮交替AB7→独立FLIP7代码/单测。
   没有条件时保持明确未放行，不自动试跑、不默认切换、不部署。

O5bd清单轮只整理清单与修正文档口径，不更改算法、配置、指标producer、磁盘格式或默认值，
不提交代码、不推送。后续候选不是自动授权扩大本轮范围。

## 7. O5bd证据索引与该轮验证范围

| 证据包（工作树根下） | 实际覆盖 / 不代表什么 |
|---|---|
| `.omx/evidence/o5bc-post-ack-alert/` | d972da1同源码历史：OFF全CTest 1391项=1357通过/34既有跳过；ON/TSan/ASan+UBSan仅各2条publisher fresh。真实pinned promtool 27 rules、3组9评估，Python7/7，三个错误规则负控。不是全ON/sanitizer或通知送达。 |
| `.omx/evidence/o5bb-tcp-read-results/` | c4dd014历史：OFF同样1357/34；三enabled配置各26相关suite合计792通过/4旧跳过；TCP两新case+增强case四配置实际执行，repeat240次。CPU PULL28和direct1不是硬件验收。 |
| `.omx/evidence/o5ba-read-interval-metrics/` | 222d2eb历史：两盘真实非零interval/重启unknown聚合、legacy零与file省略；不证明全路径请求分母或业务收益。 |
| `.omx/evidence/o5az-read-intervals/` | e7a5699历史：accepted interval分桶、recovered unknown、并发Stats守恒。固定对象sizeof变化不替代完整内存。 |
| `.omx/evidence/o5bd-acceptance-reconciliation/` | **本轮**文档引用/覆盖检查、原合同SHA验证、build-test命令成功（Ninja no-op，并非重新编译），观测Python7/7。没有新增C++、未重跑全CTest、sanitizer、promtool或C++静态检查。 |

O5bd命令（从工作树根）：

```bash
cmake --build build-test -j2
python3 -m unittest discover -s test/python -p test_observability_contract.py -v
sha256sum -c .omx/evidence/l12-gamma17a-typed-flush/contract-approved.sha256
git diff --check
```

纯文档交付不新增虚假单测；沿用真实既有用例并明确执行版本。
独立审查只读，不把阅读测试名当成重新运行结果。详细行号可能随后续代码移动，
以测试名/函数名定位，并在每个后续代码包更新本页的具体状态，不能把本页永久视作最新放行证据。

### O5be增量说明

本轮在清单核查后落实O05的path×band观测：allocator在原付费点增加计数，
Store/Group只转发快照，Node同一per-disk样本导出并聚合。不改变淘汰选择/预算/默认。
O5bd“本轮纯文档/无新C++测试”等仅描述此前清单包；O5be新增真实单测与构建证据
单独保存在 `.omx/evidence/o5be-band-scans/`，未完成验证前不宣称通过。
O07命名flush/follower账由O5bf补充，完整Node分母不在该范围；O12由O5bg补充decoded/dispatched账，但实际Serve硬件与原始接收/全传输边界仍open，硬件继续后置。


### DOC6 交接范围

本轮只整理七份文档，无运行时代码/默认/格式修改，无新增伪单测；配置实例与
`disk_lifetime_public_test`、时间边界与 `disk_lifetime_clock_test` 对照。
新鲜构建/测试及独立只读审查证据单列 `.omx/evidence/doc6-operations/`，不借用历史
计数充当本轮执行。O5be/bf/bg 代码均已提交，O12 的实际 Serve 硬件仍按约定后置。

接下来需要落实 §5 的职责、隔离与旧 baseline 准备条件，以及真实硬件/完整资源/
业务数据；没有条件不自动试跑。然后旧 baseline→冻结→独立交替 AB7→单独 FLIP7。
操作文档收口不改变这条顺序，不提供生产部署授权。

### AB7-prep1：确定性功能回放（非正式 A/B）

`test/cache/eviction_workload_test.cc` 在真实 Store 上复用固定小轨迹，分别运行
legacy 与 lifetime M=1/2/4/63：跨 tenant/partial/empty/missing/invalid 读、
keep-first 与 Remove 后新值、全 pin 拒绝后释放一份 lease 再重试，以及两轮冷写
压力下热点逻辑读。逐项断言字面量 status/bytes/驻留数/驱逐数和实际成功应用数；
不复制淘汰算法作为 oracle，不用预期结果计算实测计数。

这是批准计划中回放入口的 CPU 功能准备，不是正式 AB7 开始或完成。直接 Store
调用在非空成功读后用返回的 identity 通知生产接缝，不替代 Node/RAM/传输/ACK
路径测试；prep/pin 本身不计热。不给命中率/TTFT/性能或策略优劣结论，不记录
吞吐/耗时/资源样本，不绕过人员隔离、旧 baseline、冻结阈值与硬件后置约定。
本包证据单列 `.omx/evidence/ab7-prep1-replay/`；正式验收门禁保持不变。

最新前置小步：真实客户端最小适配接口及硬件无关单测完成，见
[正式A/B协议第13节](DISK_LIFETIME_AB_PROTOCOL_V1.md#13-真实客户端最小适配接口单测完成未接服务)。
仅复用既有Client，不连接服务；真实监护/阶段入口和小规模正确性冒烟仍待接入，正式A/B未开始。

最新前置小步：真实客户端进程保护＋串行有界冒烟已完成，详见
[协议第14节](DISK_LIFETIME_AB_PROTOCOL_V1.md#14-有界真实客户端正确性冒烟已实测非正式-ab)。
新16单测/guest同16、9目标CTest通过；实际legacy/lifetime各12 PUT/12 GET正确、
RXE＋io_uring指标闭合，停服负例失败。无淘汰压力，不当性能A/B或默认切换依据；
真实GET no-hit仍不可判定，旧baseline/业务输入/独立职责/数值门槛及正式A/B门禁未解除。

最新前置小步：[真实GET状态兼容方案](CLIENT_GET_STATUS_COMPATIBILITY.md)与C++单键
状态入口已落地，明确区分权威NotFound和非miss错误，旧bool保留。
C ABI/Python/批量去重仍待分步接入，不能宣称真实A/B miss计量全链路已修复。
