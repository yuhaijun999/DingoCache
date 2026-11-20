# 磁盘淘汰正式 A/B：协议 V1 与冻结前置清单

日期：2026-11-18；准备基线：`6428c52`。

**状态：PREPARATION_ONLY / FORMAL_RUN_BLOCKED。**
协议已整理；数值/执行包未冻结；正式 A/B 未开始，不可放行。
本轮仅文档，不运行负载、不改生产参数，也不授权默认切换或部署。
本协议从属于原规格 §11/§11.1，不替代其要求；权威路径见
[验收索引](DISK_LIFETIME_ACCEPTANCE.md#1-权威输入与状态口径)。

## 1. 本轮固定什么，尚缺什么

固定：对照关系、必需场景、回放与计量规则、交替规则、安全否决、证据交付及变更流程。
尚缺：旧基线原始报告、目标业务 trace/资源参数、独立角色与隔离审计、逐指标数值及签署。
这些缺项不是“采用默认值”，也不能由自动化代理代签。

使用已有[阈值登记模板](DISK_LIFETIME_THRESHOLDS_TEMPLATE.md)创建实际登记版本；
正式结果使用[报告模板](DISK_LIFETIME_AB_REPORT_TEMPLATE.md)。不再创建一套重复指标框架。
只有下列条件全部满足，才把状态改为“冻结完成/允许正式采样”：

1. 独立职责与可见数据清单、访问审计落实。
2. 本文场景逐行有可执行配置、trace/初态/回放器哈希及旧 baseline。
3. F01–F05 **每个子指标**数值、分母、窗口、统计判据及异常规则齐全。
4. 外部 manifest、冻结时间、制定者和独立验收者确认齐全。
5. 负载工具的确定性、时间计量和负控测试通过；不能仅凭本文开始测量。

## 2. 基线和比较对象

| ID | 固定用途 | 不能混算的内容 |
|---|---|---|
| A | 冻结的新二进制，显式 slab/legacy，disk ACK | 同二进制 legacy 不是 v2.33.0 旧版本 |
| B | 与 A 完全相同二进制，显式 slab/lifetime，disk ACK | 仅策略不同；默认评分与 P180，不筛选 P30/P60 最佳结果 |
| M | 现役 legacy/RAM ACK 迁移参考，适用时单列 | 只估计 ACK 迁移代价，不计为算法收益 |
| O | v2.33.0 旧二进制、依赖和行为/兼容对照 | 独立新目录，兼容性未证前不打开新版盘目录 |

`6428c52` 是准备参考，不是已经取得正式 binary 的 SHA。正式包固定源码提交、
构建参数、编译器/依赖版本、binary SHA；采样期间不能重编后沿用旧版本号。
先取 A/M/O 的目标负载旧基线，再由合资格人员制定数值门槛；不是先跑 B 再定阈值。
M 不适用必须提供事实与独立确认，不能无说明省略原规格的迁移参考。

## 3. 负载与初态协议

### 3.1 回放规则

- 生成一次不可变逻辑 trace：请求 ID、相对计划时间、操作、namespace/key、代际、
  payload 长度/摘要及原始生成种子、读期望、首次/再次复用组、重启事件和业务关联字段。
  这些属于离线原始证据，不加到 Prometheus 高基数标签。
- A/B 用相同 trace、初始逻辑数据、预热操作/时序和测量窗口，不根据命中结果跳过后续
  请求。策略可使驻留状态不同，不能为了达到相同命中率给某一组额外预热。
- 主策略比较采用固定计划到达序列及固定并发上限；同时记录计划、实际提交、完成时间。
  排队计入端到端延迟，另报服务调用耗时，避免 coordinated omission。
- 满队列不能静默丢请求、加速追赶或延长一组窗口。队列容量、超时、迟到/拒绝的终局
  计量、允许偏差由旧 baseline 后逐项冻结；未定义则不能开始正式采样。
- 饱和吞吐闭环作为**单独场景**，报告每组实际请求分母；不冒充相同 offered trace。
  现有 concurrent-pressure 闭环诊断不能直接改名为正式策略 A/B。
- 每轮 fresh 独立目录、相同磁盘逻辑顺序/路由与 namespace/key；记录路径对路由的影响。
  恢复场景从各自相同逻辑初始化流程构造，不能把 A 运行后的目录当作 B 的初态。
- 回填单列：同一既定回源/写回协议允许 miss 导致额外 PUT，逐请求关联，基础请求 trace
  不变；额外操作完整计量，不强行让两组回填次数相等，也不计入无回填组。
- 时间用真实单调时钟，不缩短 P/KEEP/G 或修改系统时钟。计划/ACK/读开始/完成分别保留。

### 3.2 必需场景清单（逐行待填最终规模、速率、时长和哈希）

| ID | 场景和比较重点 | 冻结包必须补齐 |
|---|---|---|
| W01 | 冷启动、稳定热点、一次性 scan 污染 | 工作集/容量比例、冷热分布、读写率、预热与稳态窗口 |
| W02 | 首次早读与未读竞争、持续新到达、已有热点 | 首次/再次复用分布；保留 45/90/185 秒诊断分层但不代表真实分布 |
| W03 | 混合尺寸、持续/突发写、near-full | 每尺寸对象/字节权重、slot/extent 几何、突发计划；不能只报总 key 命中 |
| W04 | 缺失后回填、长前缀与业务代价 | 回源/回填规则、真实连续前缀 token/重算/TTFT 字段和权重来源 |
| W05 | 独立读写并发及饱和容量 | 独立连接数、并发上限、固定到达与闭环分表；延迟含排队 |
| W06 | clean 后 0–15 分钟、dirty、连续重启、模式切换 | 重启时间表、旧冷多数/新热竞争、代际连续生存轨迹及恢复初态 |
| W07 | RAM 开/关、合并开/关、M=1/2/4/63 | 全部适用组合/排除依据，同代盘副本来源资格和通知竞争 |
| W08 | 全 pin、预算、I/O 故障、长 pinned 前缀和游标 | 可控故障时间/预期终局、owner/follower 无残留、256/1024 进展分布 |
| W09 | 合法 4 KiB 与目标几何、M63、全 E 与大 extent | 全资源账/峰值 RSS、粗扫/精查/整条清表同步分项成本 |

W08 安全实验不与正常性能样本混算；已有功能测试可以作对应安全证据，但不能替代
本次版本/配置下需要的正式测量。每项可拆为多个 trace，不能拿一个 20 秒轨迹覆盖全表。
W06 必须覆盖重启后完整 0–900 秒；其他窗口由旧 baseline 决定，必须跨保护窗并有稳定样本。
缺业务 trace 时可生成**合成验证**，不虚构线上权重/TTFT；W04 业务收益缺口仍阻断完整放行。

## 4. 资源与隔离

- 仅用宿主 `/mnt/disk2`、`/mnt/disk3` 的专属目录，**禁止压测 `/mnt/disk1`**，包括
  VM root/backing、Docker data-root、数据、临时文件、日志与结果。不得回退默认 Docker daemon。
- 现有冷盘 VM 可作本机模拟环境候选：6 vCPU/8 GiB、Ubuntu 24、软件 RXE/io_uring。
  运行前重新核验身份与绑定；历史 PID/socket 不是可复用授权证明。
- 三块 guest 盘仅映射两块宿主物理盘，软件 RXE 不是真实 HCA；此结果最多代表该冻结
  本机模拟环境，不自动外推线上硬件、三独立物理盘或生产服务等级。
- 精确容量/extent/slot、RAM、合并、ACK、并发、预算、guest 内核/库/文件系统、CPU/内存
  限额或无硬限额事实、磁盘映射/剩余空间及宿主干扰都写进 manifest。12 GiB 紧凑档
  可以保留为诊断附件，不能替代目标几何和合法 4 KiB/M63 资源验收。
- A/B 串行，不同时争用同机；预热、页缓存策略、静置、温度起始条件和宿主竞争采样
  按同一预注册规则执行。不擅自全局 drop_caches、改宿主网络/内核/daemon 或裸盘写入。
- 每次负载前启动**新拥有的**温度监控，覆盖前置测试到退出。仅采 nvme2/nvme0，
  沿用 65°C 保守停止线、SMART warning 非零/无效采样/采样失败停止规则；这不是厂商额定值。
  新执行器须测试监控失联/过期也能停止，不能仅因为没有 alarm 文件就继续。
  超温/环境漂移的样本保留标记中断，不用来选择更优结果；不自动高压重试。

## 5. 指标及门槛

| 闸门 | 固定口径 | 当前数值状态 |
|---|---|---|
| S01–S10 | 错 bytes/错代可见、漏热/错热、复用在途 slot、错误 ACK、故障不隔离等安全项零容忍 | 规格既定硬闸门；任一失败否决 |
| F01 | key/bytes 命中、连续前缀、同请求代际驱逐重访、重算/TTFT；至少一项独立业务收益达到预设幅度，其余回退受限 | 待旧 baseline 与业务输入，不填猜测百分比 |
| F02 | GET/PUT p50/p95/p99、吞吐；稳态/高压/恢复、RAM 通知额外 p99、锁等待/持有分别统计 | 待逐项登记，不能用 case 总时长或 C ABI 包络 peak 替代 |
| F03 | 适用同代读覆盖、race_lost/unbound；全部新增常驻/动态内存和恢复峰值 | 安全零容忍照常；比例/RSS 上限待登记 |
| F04 | clean/dirty 误伤、连续恢复过保留、真实零读 W、评分档、extent 归还、slot/payload 利用率 | 待登记；boot-age 不当作连续存活年龄 |
| F05 | donor、访问/重试预算、flight 终局、零进展、前后台 single/batch CacheFull/IOError、元数据 I/O | 待登记；不混合预算/物理无候选/持久化失败 |

每个子项必须填写单位、分子/分母、窗口、允许波动、样本数、方向/绝对或相对判据、
统计方法、责任人和原始数据源。分母为零、缺数据、skip 为不可判定；baseline 为零
不计算相对百分比，另设事前绝对判据。每轮单列样本和离散度，不平均各轮 p99。
吞吐分别报有效完成、命中/缺失、失败及实际 bytes；miss 快不能冒充业务收益。
逻辑回填 payload 放大不是设备/NAND 写放大；ghost/重复 PUT/计热数不能代替业务损失。

**既有探索已公开：**本会话及诊断文档已看到 lifetime 热点 miss 和耗时等结果。
本代理不能自称盲态阈值制定者；另起一个能读取同一工作区的模型也不自动成立隔离。
保留探索版本/可见记录，不删除不利结果、不补签“事前冻结”。独立人员仅接触其获准
数据（旧 baseline 等）；实际制定者的可见性/泄露范围须如实登记和审查。
历史探索不计正式样本；后续改门槛或发现泄露必须新登记、新的完整独立正式采样。

## 6. 轮次、证据与变更

- 每个适用场景/参数组合至少三轮完整配对，固定顺序 **第1轮 A→B，第2轮 B→A，
  第3轮 A→B**；增加轮次数及统计判据必须在看正式结果前注册。每对同一 trace，
  配对间种子列表事前冻结。三轮是最低重复数，不自动满足每项统计功效。
- 全部运行 ID 预分配；中断、失败、无效样本与原因全部保留。安全失败不可当环境
  无效丢弃。替补整对采样规则/最多次数须预注册，不能只补较差一组或挑最好三次。
- 每组交付启动命令、完整生效配置、时间/版本、请求终局和原始时间样本、全量 metrics、
  资源/温度/宿主干扰、重启事件、数据字节校验、退出及清理记录，外部 SHA256 manifest。
  `.omx` 摘要/文档截图不是可分发的原始包，也不证明人员权限隔离。
- 冻结包只读另存；文档不自填自身 SHA。修订 workload/资源/阈值/统计规则则新版本，
  保留旧版本与原因，重新满足开始门禁；不事后改同一版本解释失败。
- 结果仅能为未执行、不可判定、失败或明确范围通过。AB7、O5 整体验收、FLIP7、生产
  部署分别判定；本机模拟范围通过不等于全部放行。

## 7. 接下来按小步实施

1. **本轮：**交付协议和缺口清单；无压测/无生产代码/无提交授权。
2. **下一代码小步建议：**实现不可变 trace 的离线生成/校验与单测（不连接服务），
   检查完整分母、计划时间、代际/字节、确定性哈希和拒绝坏 trace；先审阅再接执行器。
3. 再接固定到达回放与原始计时，用 fake backend 验证排队/超时/清理/分母，不先跑 B 性能。
4. 与准备工作并行补齐实际角色隔离、业务 trace/权重、目标资源和旧 baseline 的登记。
   旧 baseline 实跑前仍先固定它的负载/资源包，不拿旧诊断日志充数。
5. 独立数值冻结和执行包审查完成后，再安排正式 ≥3 轮交替采样与独立验收。

需要用户/指定负责人提供或确认的是：业务 trace 或明确的合成范围、目标部署资源/
现役 ACK 配置、合资格阈值制定者与独立验收者及权限安排。普通离线编码/构建/测试由
代理完成；这些外部输入缺失不阻止离线工具开发，但阻止声称正式冻结完成和正式 A/B 启动。

## 8. 已实现的离线合成 trace 小步（非正式负载）

入口 `deploy/validation/eviction_trace.py`，仅 Python 标准库，不导入 C ABI、不加载
libdfkv、不连接或启动服务。格式 `dfkv-offline-trace-v1` 是有界的单场景样本，
**不是** W01–W09 的完整覆盖、真实业务 trace 导入器或冻结后的正式执行包。

```bash
# 在仓库根目录；选择自己的新文件，不覆盖已有文件。
python3 deploy/validation/eviction_trace.py generate /tmp/dfkv-trace-new.json
python3 deploy/validation/eviction_trace.py check /tmp/dfkv-trace-new.json
python3 -m unittest discover -s test/python -p test_eviction_trace.py
```

默认 seed=1、objects=16、rounds=4、length=4096、interval-ns=1000000。
`objects/rounds` 各 1–256、payload 1–65536 字节、间隔 1–1000000000 纳秒、seed
为 uint64。上限仅限制本离线工具开销，不是服务端合法尺寸/几何的新限制。

请求按顺序：objects 个 unique PUT 初始化、objects 个 GET 预热、rounds 轮
“写入一个新对象＋读取一个原对象”。仅 generation=0，不支持重写/Remove/重启/
回填/业务权重；这些应另一个明确版本实现，不能篡改本 schema 的含义。
两个 namespace 包含 NUL；key 由 seed 和对象序号编码，payload 是按 namespace/key/
generation/length 分隔的 SHAKE256 字节流。每条保留 payload SHA256，GET 摘要是
“命中时必须匹配”的期望，不宣称缓存一定命中，也不把逻辑预热当实际预热已成功。

所有请求含连续 request_id、phase、op、scheduled_ns、namespace/key 十六进制、
generation、length、payload_sha256。scheduled_ns 是**计划 trace 起点**起算的整型
纳秒，不是实际发送/完成或 ACK 时间；没有执行器，也没有实测计时。
完整 PUT/GET 分母各 objects+rounds、总请求 2×(objects+rounds)，测量段 2×rounds；
输出 JSON 汇总全阶段计数和逻辑请求字节，不冒充成功/ACK/实际传输字节。
默认各20 PUT/GET、共40请求、测量段8请求。原始字节可用同模块 payload(row) 重建。

文件采用 canonical JSON+末尾换行，sha256 是排除自身字段后的 canonical 内容摘要；
它不是签名，也不是自己的完整文件 SHA，正式外部 manifest 仍需另算文件 SHA。
校验时根据配置**重建整个既定计划**，因此修改请求并重算 sha256 也不能通过。
拒绝重复 JSON 字段、非有限数字、非法类型/范围、未知字段、遗漏/重复/重排请求、
计划时间/代际/摘要不一致及超过16 MiB文件。不同 seed/合法配置会生成另一个合法
样本，因此正式采样仍必须比对**外部冻结的配置与文件哈希**，校验成功不等于已获注册。

生成仅独占创建文件，不创建父目录，不覆盖文件/符号链接，不设只读权限或保证防篡改。
写入中断可能留下部分文件；保留该文件和错误，check 应失败，不自动删除或覆盖重试。
trace 工具本身不回放；第9节为虚拟时间模型，第10节为实际时钟fake回放，
两者均不能用于取得正式 A/B 性能样本。

## 9. fake backend 虚拟回放小步（不是实际执行器）

入口 `deploy/validation/eviction_replay.py`，复用第8节 trace 校验与 payload 生成，
纯标准库、内存数据、整数虚拟时钟。**不睡眠、不启动线程、不连接真实服务、不发服务 I/O
请求**；输出 ns 全为模型时间，不是实测延迟、吞吐、锁时间或调度能力。这里只锁定
排队/超时/终局计量语义，不宣称已有生产可用的异步回放器。

```bash
python3 deploy/validation/eviction_replay.py /tmp/dfkv-trace-new.json /tmp/dfkv-replay-new.json
python3 -m unittest discover -s test/python -p test_eviction_replay.py
```

CLI 可指定 `--workers`（1–64，默认1）、`--queue-capacity`（等待队列0–1024，默认16）、
`--timeout-ns`（1–10^12，默认10^9）、`--service-ns`（1–10^12，默认1）。
workers 是模拟执行槽数，不是真实线程数；超时、队列和服务时长均为离线测试参数，
不是正式性能阈值。每次 run 要求 fresh FakeBackend，不能沿用上一组缓存状态。

- 所有计划请求仍按原 scheduled_ns 到达，FIFO 排队，不因慢请求而减少 offered 分母。
  容量只限制等待队列，不包含执行槽；等待队满记 queue_full，保留请求 ID、不调用 backend。
- deadline=scheduled_ns+timeout_ns，包括排队时间。执行中在截止时刻恰好完成则完成优先；
  已排队请求在截止时刻不能再启动，即使该刻恰好有槽释放。完成同刻按 request_id 排序。
- 执行中超时只结算一次逻辑 timeout，**不提前释放执行槽**。模型继续到所有已启动调用
  完成；迟到 PUT 仍可能写入内存，记录 backend_outcome，但绝不追补逻辑 ACK/ACK 字节。
  完整排空时刻 drained_ns 不改变原到达计划，也不当作性能测量窗口。
- 内存 PUT/GET 在完成时写入/观察状态（非真实系统一致性模型）。GET 在其 PUT 完成前
  可能 miss；这不是严格预热已成立。后续真实执行器仍需定义阶段 barrier、预热资格与
  实际观测时间，不能直接用本模型当正式初态构造器。
- 单测可按 request_id 注入有界服务时长、backend_error、GET miss 或错误 bytes。
  错误 PUT 不提交；miss 合法、错误 bytes 否决。迟到错 bytes 同样计入独立安全计数，
  不因请求早已 timeout 而被覆盖；继续模拟其余请求是为了保留完整分母，不表示安全放行。

每请求保留 trace 身份、计划时间、deadline、唯一 terminal/terminal_ns/latency_ns。
仅真正启动者有 started_ns/queue_wait_ns，仅完成者有 completed_ns/service_ns/
backend_outcome/late_completion。未开始/未完成时间不填0，不伪造调用记录。
latency_ns 是从计划到逻辑终局的时间；service_ns 是完整模拟调用时间，迟到时可超过
逻辑 latency；不能把这两种延迟互相替代。summary 的 offered、terminal、started、
completed、late_completions、wrong_bytes、acked_put_bytes、hit_get_bytes 均从原始行汇总。
trace_sha256 是原 trace 的内容摘要；输出保存模型参数/注入配置，不是正式外部冻结 manifest。

terminal 为 put_ack/get_hit/get_miss/queue_full/timeout/backend_error/wrong_bytes。
只有前三者组成的完整模拟才 passed；这里的 passed 只表示本模型没有模拟失败，
不是 A/B 通过。失败模型也落完整结果且 CLI 返回1；输入/配置非法返回1且不创建结果。
输出独占创建，不覆盖文件/符号链接、不创建父目录；写入中断可能留下部分文件，保留错误。

**本虚拟模型的边界：**不验证实际时钟或线程调度；实际fake回放另见第10节。
真实服务/不可取消调用的进程级兜底、网络/ACK/取消、容量淘汰与恢复仍未实现/验证。
模型所有服务时长有界，不能据此声称真实 native 卡死可排空；正式门禁仍未完成。

## 10. 实际单调时钟＋线程 fake 回放（仍不连接服务）

入口 `deploy/validation/eviction_realtime.py`，复用既有 trace 和内存 FakeBackend，
`time.monotonic_ns()` 记时、真实线程等待/并发。与第9节虚拟模型分别输出，不混称
同一种实测。没有 C ABI、服务、网络、磁盘缓存读写或真实淘汰；仅读trace/写结果文件。

```bash
# 4个请求、80ms间隔；新文件名，不能覆盖原样本。
python3 deploy/validation/eviction_trace.py generate /tmp/dfkv-real-trace-new.json --objects 1 --rounds 1 --interval-ns 80000000
python3 deploy/validation/eviction_realtime.py /tmp/dfkv-real-trace-new.json /tmp/dfkv-real-result-new.json --workers 2 --service-ns 1000000
python3 -m unittest discover -s test/python -p test_eviction_realtime.py
```

本小步限制 workers=1–16、等待队列0–1024、timeout=1–5×10^9 ns；默认分别为1/16/10^9。
service默认1ns（只是假等待参数），单调用配置等待≤1秒、全部配置等待之和≤10秒、
trace最后计划到达≤5秒。超出拒绝且不启动；这些是诊断开销上限，不是服务合法范围
或正式放行阈值，也不是 OS 的硬实时/强制停止保证。

调度规则：

- 原 trace 的 scheduled_ns/请求 ID 不改，起点为本轮 monotonic origin。调度线程独立
  于有界 worker；主线程只给已预留的执行槽提交任务，executor 不接管无界等待队列。
- `arrival_observed_ns` 为调度器实际看到到达的时刻，`schedule_lag_ns` 单列。若已经
  达到deadline记timeout；尚未超时但已错过下一到达槽（scheduled+interval）记
  **arrival_late**，不集中补发。所有迟到/拒绝/超时仍保留原请求分母，并使本轮失败。
- 接受后记录 admitted_ns；排队FIFO，过期等待不提交。submitted_ns 仅证明任务提交，
  不等于已启动；worker拿锁后再次核验deadline，已过期不调用backend、不伪造started。
- 真实 started_ns 后才调用假等待；内存访问、完成时间记录与结算在同一Condition下。
  `peak_reserved` 包含已提交未启动及超时但未结束者，`peak_running` 才是同时在假调用
  包络内的线程数；不等于物理I/O、NIC或native在飞数。store锁成本/调度/GIL可计入时间。
- deadline始终从**计划到达**起算。active timeout不撤销线程/释放槽；迟到完成及PUT
  可能已提交的结果保留，逻辑timeout不补ACK。调用恰好在deadline完成可成功；等待或
  尚未启动的任务到deadline不再启动。真实时钟测试不伪造纳秒级精确相等，边界数学由虚拟模型另证。
- `terminal_ns` 对timeout为有效deadline，`settled_ns` 是实际发现/结算时刻，必须分别
  保存。`latency_ns` 是前者减scheduled；**实际观察延迟是 observed_latency_ns**，不能
  把截止时间裁剪后的latency当p99。服务时间从started到completed，包含假等待及本地开销；
  queue_wait从admitted到started，dispatch_lag从scheduled到started，不能丢掉schedule_lag。
- 捕获假等待/完成异常为backend_error并保留异常类型/信息；迟到错bytes仍计安全失败。
  所有提交的future均取结果，线程退出/join后才返回报告，不在后台继续改结果或访问状态。

输出 `model=monotonic-threaded-fake-only`，保留配置/注入参数及逐请求原始账，新增
arrival_late终局。仅put_ack/get_hit/get_miss且全量终结才passed；miss仍不证明严格预热。
summary的offered/submitted/started/completed、终局计数、late/wrong_bytes及ACK/hit逻辑
字节从原始行汇总。输入无效不创建输出；完整失败报告仍保存并返回1，成功返回0；
输出独占创建、不覆盖原文件/符号链接，不自动删除partial或重试。

验证用Event握手证明两个假调用确实重叠，延迟worker入口证明“submitted不等于started”；
人工拖慢调度器检查不追赶，假调用跨过真实到达/截止窗口检查排队与超时。断言比较
原始时间先后/计数，不要求真实调度达到精确纳秒或给出性能达标结论。

**仍缺：**真实服务适配器、真实ACK/网络/磁盘淘汰、正式阶段barrier与热度初态、
实际native调用的取消/隔离契约。当前线程等待仅针对有界fake，不能推广为
native卡死可安全join；第11节新增fake子进程监护，不替代生产服务的进程/资源隔离。
正式业务trace、旧baseline、人员隔离及逐项数值门槛仍缺，正式A/B未启动，默认/评分不改。

## 11. fake 回放的父进程监护与失败报告

入口 `deploy/validation/eviction_guard.py`。父进程只启动固定的 `eviction_realtime.py`，
不提供任意命令/真实服务参数；仍不连接服务、不压盘、不作正式A/B。

```bash
# 输入可用第10节生成的trace，输出必须是尚不存在的目录。
python3 deploy/validation/eviction_guard.py /tmp/dfkv-real-trace-new.json /tmp/dfkv-guard-new --process-timeout-sec 5
python3 -m unittest discover -s test/python -p test_eviction_guard.py
```

输出目录以独占mkdir取得，不建父目录、不复用文件/符号链接、不自动删除或重试。
先校验trace，在新目录保存canonical `trace.json`，子进程只读此副本；report同时登记
内容摘要和这份文件的SHA，防止把源文件后续修改当成同一输入。其余线程/队列/请求
超时/服务假等待参数沿用第10节，进程时限额外传 `--process-timeout-sec`（有限数值
0.05–60秒、默认30秒），从启动子进程前起算，包含解释器启动，不是仅请求执行时间。等待使用扣除启动耗时后的剩余预算；启动返回或观察到退出时已过
预算，即使已有有效passed文件也失败。Popen本身在内核中阻塞时，当前父进程不能强行
抢占它，只能返回后识别超时；因此不宣称整个函数是硬墙钟上限。终止宽限另计，
报告elapsed包含清理，但不把这些时间当作成功执行预算。

监护规则：

- 使用参数列表、stdin=DEVNULL，无shell；stdout/stderr直接写各自日志文件，不用可能
  填满的PIPE。固定fake输出有界，不因此承诺任意程序日志有配额/截断保护。
- 到进程时限仍未退出，给**本次Popen对象代表的直接子进程**请求TERM，宽限1秒；
  仍未结束则请求KILL，再有界等待1秒并核对回收。记录请求动作、错误和returncode/
  reaped，不把“发了KILL”当作已经回收。只操作自有句柄，不按进程名、目录或PID扫描杀进程。
- 若有界等待后仍不能确认回收，保存cleanup_failed、原失败原因及PID，保持失败，
  不转为成功、不自动重跑。该极端分支用mock验证，不冒充真实D-state故障注入。
- 正常退出0也必须核对child-result：固定model/trace/config、完整原请求身份与顺序、
  唯一终局/启动完成、基本时间顺序、PUT/GET与成功类型对应、成功结算等于完成时刻、
  admitted/submitted/started凭据、原始行与终局/字节汇总守恒，以及passed与退出码一致。
  这里只是最小整体一致性核验，不是独立性能验收器，也不证明不可信程序不能伪造整份报告。
- 超时优先于已有结果：即使子进程写了passed后卡住，也记process_timeout，不放行。
  崩溃/信号/退出1无有效结果、缺失/截断/重复字段/矛盾结果分别保留原文件与日志。
- 对完整且正常返回的成功/工作负载失败账，可以登记已知完成数和ACK字节；对超时、
  异常、无效/不完整结果，一律 `request_outcomes=unknown`、完成数/ACK字节=null。
  planned_requests是计划数，不冒充实际已发送数；不把未知调用补成逐请求timeout或0次ACK。
- 启动失败也保存父报告。父进程可捕获的异常/KeyboardInterrupt会先有界清理自有子进程、
  保存错误；KeyboardInterrupt再向上抛出，不吞掉停止请求。不会声称可拦截父进程SIGKILL。

目录保留 `trace.json`、`stdout.log`、`stderr.log`、可能存在/不完整的`child-result.json`、
父 `report.json`。父报告记child_pid、command、process时限、elapsed、动作、returncode、
reaped、结果状态。只有正常退出0+完整一致的passed结果才整体passed，CLI返回0；
其余返回1或继续传播父中断。失败不覆盖/删改子报告，不用stdout的“成功”字样代替结果。
输入/参数非法或目录已存在在启动前拒绝，不创建新的输出目录。

**边界：**只验证直接fake子进程，不清理未知孙进程、不修改宿主进程组/内核/daemon，
也不证明真实native/磁盘I/O在KILL后安全取消或数据已持久化。父被SIGKILL/未处理的终止信号、磁盘满/权限
故障等可能使报告不存在/不完整；报告写失败不得返回成功，保留已有文件并明确报错。
output目录/log没有系统级硬配额。fake阶段初态验证见第12节；真实服务适配、真实热度初态
和A/B冻结门禁仍未完成。

## 12. 显式三阶段与初态校验（fake-only，默认平铺模式不变）

第10/11节新增 `--staged`，API对应 `staged=True`；默认不设置时仍是原来的平铺
调度，不改变旧诊断的允许miss规则、时间轴和分母。第9节纯虚拟模型不变。

```bash
python3 deploy/validation/eviction_guard.py /tmp/dfkv-real-trace-new.json /tmp/dfkv-staged-new --staged --process-timeout-sec 5
python3 -m unittest discover -s test/python -p test_eviction_stages.py
```

这个模式是**显式的新回放时间协议**，不是悄悄重定时原trace：源 `scheduled_ns`、
request_id、phase、对象/字节和trace SHA保持不变；输出另记
`schedule_mode=phase-relative-v1`、各阶段 `source_offset_ns`/实际 `origin_ns` 及逐请求
`effective_scheduled_ns = origin_ns + scheduled_ns - source_offset_ns`。
同一阶段内部间隔保持原计划；请求deadline/到达滞后/端到端延迟基于effective时间。
初始化与预热耗时另报，不混进测量请求；不可拿这个模式与旧平铺结果当同负载A/B。
未来正式注册还须冻结模式、phase计划与成本口径，当前不是正式冻结包。

| 阶段 | 入场/关闭条件 | 失败处理 |
|---|---|---|
| init | fresh且空fake store；所有初始化PUT及时ACK，结束后key集合、长度和payload摘要严格等于计划 | 失败后不进入warmup/measure |
| warmup | init物理调用全部排空；每条预热GET必须get_hit，结束时payload驻留快照仍完全一致 | miss即失败，不当成功预热；measure不发出 |
| measure | warmup物理调用排空且校验通过后才定新origin；新PUT/原对象GET按阶段计划到达 | 测量miss仍可作为正常缓存结果；错bytes/超时等仍失败 |

切换依据不只是逻辑请求终局：reserved/等待队列都清空，已启动假调用实际completed，
才取结束快照和开启下一阶段。初始化超时即使随后迟到PUT提交成功，也必须排空后
判失败，不允许把迟到提交当作预热资格。快照计算先于下阶段origin，计算成本仍体现在
整个回放耗时/阶段转换间隔中，不让取快照自身提前耗尽下一阶段的到达槽。

`start_state/end_state` 为实际fake store的resident数、payload字节及排序后
`[namespace_hex,key_hex,length,payload_sha256]` 清单摘要。init前必须为空；init后、
warmup前后及measure前必须是计划初始化集合。额外对象、缺对象、长度/字节不符均
失败，不能只比较对象数量或只相信PUT返回ACK。父监护器按trace元数据独立核对摘要。
**这不是实际磁盘/RAM的热度/评分初态**：fake没有真实读热度、pin、淘汰链和恢复状态，
也不能证明真实cache在相同逻辑预热下有相同驻留/热度。当前只验证payload初态及阶段资格。

每阶段保留status（passed/failed/blocked）、实际origin/finished或blocked时刻、
原始请求账与独立summary。父进程核验阶段顺序/排空时序、effective时间映射、状态摘要、
严格ACK/hit条件及计数，不能仅凭passed/preparation布尔值接纳子报告。

前置失败的后续请求保留 `phase_blocked` 和实际阻止决策时间；没有实际到达、deadline、
启动/完成或伪造latency，也不生成一个假的阶段origin。各阶段/root同时报告
**planned（计划数）与offered（实际被调度观察到的到达数）**，blocked不进入offered。
terminal计数包含全部计划行以便审计，因此可能大于offered；不能把terminal总数用作
实际读分母。已开始阶段每行必须有真实arrival观察凭据；父进程拒绝删凭据后重算分母。
root/phase的ACK和hit字节均是各自原始行汇总，测量分母不得包含初始化/预热请求。
完整失败账允许明确报告“测量offered=0、blocked”，不是宣称测量成功率或业务收益为0。

本小步只改fake调度/父验证和单测，默认策略/评分/保护窗口、trace schema及生产C++不变。
无服务连接/磁盘压力；正式trace/业务权重、旧baseline、人员隔离与数值门槛仍缺，正式A/B未开始。

## 13. 真实客户端最小适配接口（单测完成，未接服务）

`deploy/validation/eviction_client.py` 的 `TraceClient(document, lib, members,
namespace)` 复用既有 `disk_scenarios.Client`。构造前严格校验整个trace和二进制namespace，
然后打开一个该namespace专属句柄；`lib` 由调用者用既有 `load_library` 加载。
导入模块不连接服务；构造会连接。调用时需将 `deploy/validation` 与
`integration/common/src` 加入Python搜索路径，沿用现有诊断工具环境。

- `execute(request_id)`：只执行trace中对应的一个请求，拒绝其他namespace和非法ID；
  PUT必须返回成功才报 `put_ack`，GET复用现有完整长度/字节校验。
  返回 `{outcome: put_ack|get_hit|get_unresolved}`；异常直接传播，
  不把错误转成miss/ACK。trace源对象不会被修改。
- **当前C ABI不能可靠区分缓存miss和服务/网络错误**：GET_AUTO的hit=0也可能来自
  IO错误、无路由/冷却或容量不足；EXIST也将部分错误压成0，多探测不能修复信息丢失。
  因此no-hit只返回 `get_unresolved`，绝不能计为正常缓存miss或成功测量终局。
  不增加GET/EXIST探测，不用重复读伪造证据；未来需要携带明确错误状态的接口，
  才能支持可信miss计量。本步不修改C ABI。根因位置见 `src/client/dfkv_c_api.cc`
  的 `dfkv_batch_get_auto/dfkv_exist` 与 `src/client/kv_client.cc` 的GET/ExistDirect路径。
- 一个句柄的execute/close由锁串行化；独立并发使用每worker/namespace独立句柄，
  不将锁等待称为网络并发。`close`/上下文退出等待当前调用返回，幂等且禁止复用；
  关闭异常仍传播并禁止复用，不声称资源已成功释放。
- 无请求调度、阶段barrier、deadline、取消或自动重试；native调用挂住时close也可能等待。
  `put_ack`仅表示现有API返回成功，不能独自证明磁盘ACK、RXE或io_uring路径。
  外层进程监护和服务配置/指标证明仍必须另行接入，不能把fake guard直接当真实监护器。
- 没有真实resident/热度/评分快照接口，不能套用fake payload快照为真实初态证明。

硬件无关单测通过既有Client的ctypes缓冲区读写、假C ABI返回值检查二进制key/value、
成功/错误、no-hit歧义/过大对象/错字节、关闭与在途调用、非法trace/ID/namespace和默认工厂复用。
这里C ABI库是测试替身，**不是已连接真实lib/服务的冒烟**。

```bash
python3 -m unittest discover -s test/python -p test_eviction_client.py
cmake --build build-test -j2
ctest --test-dir build-test -R '^python_eviction_client$' --output-on-failure
```

下一小步安排：先补真实适配器专属的进程隔离/超时失败账与有界init→warm→measure冒烟入口，
再在重新核验归属的隔离VM内跑少量对象的字节正确性冒烟；新建冷盘目录并先启温度监控。
禁止宿主 `/mnt/disk1`，不重用默认Docker daemon，不自动开始压力或正式A/B。
当前未启动任何真实服务/VM/测试盘负载；正式业务输入、旧baseline、独立职责和数值门槛仍未齐。

## 14. 有界真实客户端正确性冒烟（已实测，非正式 A/B）

入口 `deploy/validation/eviction_smoke.py`；工具**只连接已有的专用scratch服务**，
不创建/重置/停止服务，不用于线上实例。参数限制单个 `name=127.0.0.1:port`，
最多8个初始化对象、4个新增对象、每对象4096B，即最多24条计划请求/49152B PUT。
复用§13适配器，先全部init PUT，再全部warm GET，最后measure交错PUT/GET；
任一异常/no-hit立即失败，后续（包括当前阶段剩余）标为 `not_attempted`。
这里no-hit只能为 `get_unresolved`，不能当普通miss通过。没有真正驻留/热度快照。

这是**串行、无节奏的正确性检查**：源计划时间/ID/字节/SHA保留作身份，
`schedule_mode=sequential-unpaced`，不按scheduled_ns调度、无追赶、不形成性能到达率对照。
GET检查全部返回字节；当前trace的测量GET仍访问初始对象，新增测量PUT只检查ACK，
不声称所有新写对象都做过回读。started/completed是整个适配器调用的墙钟边界，
不是精确native/NIC区间；没有逐请求deadline或取消承诺。

父进程独占新输出目录，冻结trace、库绝对路径/SHA、members及固定客户端环境。
清除继承的 `DFKV_*`，显式使用RDMA=1/rxe0/depth4、8MiB recv、2MiB payload/block。
加载库、打开句柄、执行及关闭全部放在自有直接child；默认总预算10秒，可设0.05–30秒。
预算含启动成本，超时先TERM宽限1秒、再KILL宽限1秒；复用fake guard的exact-child
停止函数，但不复用fake结果验证。无任意command CLI、无进程枚举/进程组批量信号。
父核对完整trace/config/逐请求op与终局、顺序起止、首次失败后禁执行、句柄清单、
汇总和退出码，而不只信passed。完整失败账可给已知计数；杀死/异常退出/坏账则完成数、
ACK字节为null未知，不能填0。stdout/stderr及原始child账、父report均保留。

边界：TERM/KILL及reaped只证明直接child处理，不代表远端PUT撤销或落盘回滚；
未证明后代清理。Popen自身内核阻塞不可硬抢占，父SIGKILL/落盘失败可能缺报告，
日志无系统硬配额。`--child`仅内部使用，不能当绕过监护的公开入口。
服务健康/磁盘ACK/实际路径和空闲回收由外层独立核验，不能仅凭客户端transport字符串证明。

专用隔离guest内，**先按冷盘说明核验归属、fresh目录/服务与温控**，再执行：

```bash
# 每轮换新seed/文件名/结果目录；只针对本轮专用scratch服务。
python3 /opt/dfkv/deploy/validation/eviction_trace.py generate /tmp/smoke-trace-NEW.json \
  --seed 2026111901 --objects 8 --rounds 4 --length 4096
python3 /opt/dfkv/deploy/validation/eviction_smoke.py /tmp/smoke-trace-NEW.json \
  /data/disk1/results/smoke-NEW --lib /opt/dfkv-build/libdfkv.so \
  --members n=127.0.0.1:12001 --process-timeout-sec 10
```

上例guest `/data/disk1`必须已证明由宿主disk2承载，**不允许映射宿主 `/mnt/disk1`**。
服务启动/停止与温控归属门禁见[冷盘说明](DISK_LIFETIME_COOL_DISK_VALIDATION.md)；
不能对未核验的本地端口盲目重跑。失败后保留原目录/原账，不续跑、不自动重试/覆盖。

2026-11-19实测：同一冻结trace、同一既有ON二进制，fresh slab/legacy与slab/lifetime
各24计划/24尝试/24完成：12 PUT ACK、12 GET命中，各49152B写入及读取。
RAM关闭、disk ACK、buffered；每例3个新guest盘目录，初始对象数为0且metrics归属匹配。
两例均实际uring读取/完成12，PULL终局12 success_nonempty，错误/fallback/inflight/淘汰均0；
lifetime read_success=12。两例均正常退出0、端口释放。停服后额外负例第一PUT失败，
1尝试/1返回、ACK0，余23不执行；不是正常miss，也不是已撤销写入证明。
两例不是正式A/B样本，不比较约0.9秒运行时间，不证明大容量/长时间淘汰效果。

新16硬件无关单测、guest同16方法均0失败/跳过；本机OFF/OFF构建及9目标CTest通过。
另保留一次SSH返回0但只给MOTD的无执行尝试：无guest结果目录，已判无效，不计成功样本；
实际执行改为直接sudo python并要求唯一完成标记，再独立校验原始账/指标，不能只看SSH退出码。
冷盘温控及保留文件位置见冷盘说明第12节。正式A/B仍未开始，生产默认/评分/保护参数不变。

## 15. 真实GET状态兼容方案：先C++单键

[状态接口方案与实施第一步](CLIENT_GET_STATUS_COMPATIBILITY.md)明确状态丢失层、
仅权威NotFound才计miss、错误/本地拒绝/容量不足不能当miss，以及旧ABI/wire兼容路线。
本小步新增C++ `GetAutoWithStatus` 和共享buffer核心，保留旧bool外观及指标一次；
修正坏结果形状的越界/假成功。**C ABI、Python和批量去重状态尚未接通**，
当前Python仍保持 `get_unresolved` / 严格命中冒烟，不因此开始正式A/B或改变默认。

## 16. C ABI与验证用Python单键状态贯通

第15节记录首步；后续已新增独立 `dfkv_get_auto_status`，以稳定C常量显式映射结果。
验证用TraceClient自动探测新符号，只有明确NOT_FOUND计get_miss；失败保留错误，
旧库no-hit仍unresolved。生产连接器及并发batch/SG/dedup没有切换成串行状态读取。
严格hit-only smoke认识get_miss完整失败记录，但不因此放宽门槛。
接口/验证/后续边界详见[状态兼容方案第5节](CLIENT_GET_STATUS_COMPATIBILITY.md)。
仅在tiny file/TCP服务验证本轮新增接口；此前RXE/uring二进制未含该新接口，
不能沿用旧guest结果声称新接口已过真实RDMA。正式A/B仍未开始、默认不变。

## 17. 新单键状态接口的隔离RXE/io_uring验证

源码 `5abc009` 已在既有冷盘VM中完整ON重建，143文件SHA匹配、新库符号/哈希核验。
23项C++及Python14＋17方法通过，legacy/lifetime各正常24请求和3类真实故障诊断符合预期。
只计明确NOT_FOUND为miss；容量不足/停服均错误，父报告保留完整已知失败账、停止余请求。
服务端grant成功与客户端最终hit分别核验，不混用两者分母；注入PUT/REMOVE单列。
详见[状态接口第6节](CLIENT_GET_STATUS_COMPATIBILITY.md)及[冷盘说明第13节](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。
这补齐单键接口的RXE/uring正确性证据，不是物理HCA/批量/SG/dedup或正式A/B放行。

## 18. C++直接批量GET状态核心：兼容契约已落地

新增BatchGetAutoWithStatus，返回N项原序Status，保留TCP并行和RDMA分组/分片调度；
明确direct-only绕过opt-in dedup，旧bool/dedup/SG契约不变，异常响应先校验再索引。
本机完整OFF构建及132相关CTest通过，ON相关目标与新13用例通过；
真实RXE/uring两策略各一个64项新状态批次和一个64项旧bool投影批次通过。
这不是新增批量C ABI/Python或SG/dedup状态传播，也不是整套ON/全量CTest、性能或正式A/B。
详见[状态兼容方案第7节](CLIENT_GET_STATUS_COMPATIBILITY.md)。


## 19. 批量 GET C ABI：调用失败与逐项结果分离

新增 dfkv_batch_get_auto_status，沿用固定 DFKV_GET_* 结果；返回0表示完整逐项结果，
不表示全命中。-1整体失败不得当作miss，异常无回滚保证，输出长度仅OK有效。
复用C++直接批量核心并绕过去重，旧C符号不变；Python/SG/dedup状态接入另行处理。
完整契约及本轮验证见[状态兼容方案第8节](CLIENT_GET_STATUS_COMPATIBILITY.md)。
这不是正式A/B、性能、物理HCA或生产默认切换放行。


## 20. 批量状态的 Python 与受监督报告闭环

新增验证用read_outcomes/TraceClient.execute_batch和显式--batch-gets；缺新符号才使用旧batch，
旧hit0保留unresolved，不使用scalar/EXIST重试。新模式按连续GET窗口/namespace分组，
同组结果全保留后才停止后续组，父进程从冻结配置选择独立组计划验收器。
报告区分明确miss、逐项错误、整组失败、未尝试，超时/坏报告计数仍unknown；严格hit门槛不放宽。
--tcp显式冻结TCP环境，默认仍RDMA/rxe0，不失败自动回退；实际调用顺序不同于scalar模式，
共享组时间不等于单项传输延迟，不用于固定到达或性能对比。
完整契约、复跑和验证见[状态兼容方案第9节](CLIENT_GET_STATUS_COMPATIBILITY.md)。
本轮是tiny file/TCP正确性闭环，不证明新Python路径RXE/uring、SG/dedup或正式A/B已通过。

## 21. 新批量 C ABI/Python 状态链路的 RXE/io_uring 证据

源码4709a1f已在重新取证且fresh温控的既有冷盘VM中完整ON构建，386文件SHA一致。
44项C++与56个Python方法通过；两策略各正常/混合/隐藏新符号/停服，共8组预期符合。
正常使用公共--batch-gets，负例使用明确记录的证据wrapper与同一父验收器，保留完整失败账、
不缩分母、不把旧hit0/IO_ERROR当miss；server grant热度与client最终hit继续分开。
本轮VM backing与guest诊断落盘仅宿主disk2/3，服务/子进程回收、端口空闲；未修改生产默认配置。
详见[状态接口第10节](CLIENT_GET_STATUS_COMPATIBILITY.md)与[冷盘说明第15节](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。
这是direct-only批量状态正确性证据；SG/dedup、物理HCA、性能和正式A/B仍未放行。


## 22. SG C++ / C ABI 状态接口的前置边界

本轮补 `BatchGetAutoSgWithStatus` / `dfkv_batch_get_auto_sg_status`：保持原序逐项状态、
合法 sibling、完整长度和调用级失败区分。新接口 direct-only/single-pass，不参与 CPU/GPU
rendezvous，不走旧 SG 少数失败重试；旧接口保留。不能将新旧 I/O、命中率、时延直接视为等价。
见[状态兼容方案第11节](CLIENT_GET_STATUS_COMPATIBILITY.md)。Python SG、dedup、
真实设备 SG 状态验证及正式 A/B 仍需单独安排；本轮不改变冻结负载、数值门槛和生产默认配置。
