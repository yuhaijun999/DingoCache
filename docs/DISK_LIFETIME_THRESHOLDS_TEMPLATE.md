# 磁盘寿命淘汰：阈值登记模板

**模板状态：DRAFT / 未登记 / 未冻结 / 不可放行。**
本文件没有实际人员、实验数据或数值阈值。复制为独立版本的登记表后填写；模板本身
不是冻结版本，不证明职责隔离已成立，不授权性能探索、正式 A/B、默认切换或部署。
依据原规格 §11/§11.1；要求总表见[验收索引](DISK_LIFETIME_ACCEPTANCE.md)，
结果填写[正式报告模板](DISK_LIFETIME_AB_REPORT_TEMPLATE.md)。

## 1. 版本、职责与样本隔离（先于首次新模式性能探索）

以下“待填”均为未完成，不是默认值；实际登记必须替换并提供证据位置。

| 登记项 | 当前值 / 必填内容 |
|---|---|
| 登记版本、创建日期、适用源码提交 | 待填；冻结后不得原地改同一版本 |
| 探索样本保管者 | 待填：实际身份、职责、权限和确认记录 |
| 阈值制定负责人 | 待填：实际身份、职责及其可见数据版本 |
| 独立验收者 | 待填：实际身份、独立性及签署职责 |
| 实验执行者、环境维护者 | 待填；写明与上述角色的关系，不自采、自定后自验 |
| 旧 baseline / 探索 / 正式样本位置 | 待填：三个独立数据集 ID、位置、保管及保留策略 |
| 访问权限及访问日志 | 待填：读写主体、授权时间、可审计日志位置；路径不同不等于隔离 |
| 首次探索前隔离确认 | 待填：确认人、时间、权限核验与日志证据 |
| 冻结前阈值制定者可见清单 | 待填：仅允许旧基线的具体版本；不留“全部可见”之类歧义 |
| 探索/正式边界 | 待填：样本 ID 范围、产生时间、配置/trace 版本和隔离规则 |
| 泄露/例外记录 | 待填：包括确认为无的审计证据；发现泄露须停止相关放行、重注册和独立重采 |

GET/PUT p99、RSS/恢复峰值、锁成本、预算失败等即使不比较业务收益，也属于需封存
的新模式性能数据。冻结前不能让阈值制定者接触；探索样本不能参与门槛制定或充当
正式样本。正确性/恢复功能测试不因此延后，功能 repeat 也不是性能基线。
角色或隔离未成立时，不开始性能试跑；自动生成的模板内容不能代替角色确认或独立签署。

## 2. 旧基线与可比配置（数值冻结前取得）

| 组别 | 必须登记的证据 | 当前状态 |
|---|---|---|
| 目标 disk-ACK legacy 基线 | 新二进制 legacy；目标负载、容量、重启轨迹；RAM write-back 显式 disk ACK | 未提供 |
| 现役迁移参考 | 现役 legacy/RAM-ACK（适用时），单列 ACK 迁移代价，不作为算法 A/B 对照 | 未提供 |
| v2.33.0 行为对照 | 旧 binary/依赖/参数/兼容与行为记录；不能拿新 binary 的 legacy 冒充旧 binary | 未提供 |

每组保存：binary/source SHA、构建特性、依赖和硬件/内核/文件系统版本、磁盘路径及
几何、容量、RAM/合并开关、ACK、生效配置 dump、并发、预算、尺寸/热度分布、trace
及初始逻辑数据 SHA、预热方法、观察时长、重启方式和原始数据清单。明确 engine 与
requested/effective/staged；只有请求 lifetime 不是已生效证据。

正式 A/B 的 A/B 两组使用同一新 binary，分别显式 legacy/lifetime，保持 ACK 和
其余可比条件一致。RAM write-back 两组都显式 disk ACK。旧 binary 的格式兼容
另外验证，不把当前 v3 目录直接交给未验证兼容的版本；旧数据路径不复用为新实验
工作目录。不因基线难取得而换成较小的合法几何或不同初态掩盖资源/恢复成本。

## 3. 每个指标的完整登记记录（复制此块逐项填写）

不能只给 F01–F05 每组一个模糊总阈值。每个子指标、方向、路径、场景窗口需要可
判定的记录；向量阈值必须逐维列明，不可用总均值遮盖失败子项。

```text
record_id / gate_id / registration_version: 待填
metric / definition / source or business trace field: 待填
comparison direction and absolute or relative rule: 待填
numeric threshold / unit: 待填
numerator / denominator / eligible cohort / exclusions: 待填
scenario / path / disk / API / observation window: 待填
allowed variation / sample size requirement / statistical decision rule: 待填
zero denominator or zero baseline / missing or skipped data rule: 待填
responsible person / independent reviewer: 待填
old-baseline evidence IDs / visible-data versions at freeze: 待填
binary / resolved-config / initial-state / trace hashes: 待填
exploratory sample boundary / fresh formal sample boundary: 待填
freeze version / timestamp / approval evidence: 待填
```

### 必须展开的五组指标（下列均未填数值）

| gate_id | 逐项覆盖要求 | 数值状态 |
|---|---|---|
| F01 业务收益与回退 | 有效前缀复用、驱逐后重访 miss、重算 token、TTFT；至少一项独立业务收益的事前增益幅度，其余各项回退上限 | 未登记 |
| F02 性能与锁 | GET/PUT p99、吞吐、锁等待/持有；稳态/高压/重启窗口分别登记；RAM→disk 通知额外 GET p99 和锁成本单列 | 未登记 |
| F03 同代读覆盖与内存 | RAM 命中、合并 leader/follower 的适用同代已提交副本分母；盘锁接受后漏热/错代零容忍；race_lost、unbound 比例；合法几何增量常驻内存及恢复峰值 RSS | 未登记；既定安全零容忍不豁免 |
| F04 恢复与利用率 | clean/dirty 误伤、连续重启过保留、恢复零真实读 W/虚拟 n2、两套评分档驻留分布、整条归还、空间/slot payload 利用率 | 未登记 |
| F05 donor/预算/无进展/I/O | fallback、请求访问量/重试、同 key flight 完成率、失败比例/持续时间；按盘/路径与前后台 single/batch 分开，physical no-candidate/all-pin/budget/persistence error 不混算；预算背压/落盘耗时、未 ACK 失败/ACK 后丢弃分开 | 未登记 |

盘锁已接受的同代通知不得漏计/错代、不得返回错 value、不得复用在途 slot 等，是
原规格既定安全硬闸门，不是等待基线后可以放宽的性能数值。逐项安全关闭证据使用
[验收索引 S01–S10](DISK_LIFETIME_ACCEPTANCE.md#3-安全硬闸门不能被性能收益抵消)；
任一硬失败即否决，不能由业务收益抵消。缺业务引擎字段写“未测”，不能拿 ghost
命中、重复 PUT、applied 次数或评分改善替代业务收益。

## 4. 冻结包、变更和执行门禁

| 冻结包内容 | 当前状态 |
|---|---|
| 全部指标记录与场景/交替顺序/统计方案齐全 | 未提供 |
| 旧 baseline、可见数据清单与权限/访问审计 | 未提供 |
| 配置/trace/初态/binary 清单及 SHA256 manifest | 未提供 |
| 制定负责人和独立验收者的确认、时间和签署位置 | 未提供 |
| 外部保存的冻结表文件 SHA 与不可变版本位置 | 未提供；不在表中自填自身 SHA 造成循环引用 |
| 对正式样本 ID 和新采集边界的约束 | 未提供 |

保存只读冻结副本与外部 manifest。填完表或计算 SHA 本身不是权限审计或独立放行。
冻结后发现定义/数值/可见样本泄露或需要改门槛，保留旧版本与原因，登记新版本，
重新采集独立完整正式样本；不能回填旧结果、只挑更好的轮次或复用探索样本。

顺序：职责/隔离确认 → 旧基线齐备 → 全部阈值冻结 → 独立正式采样/验收 →
独立 FLIP7 代码和单测。已有隔离 RXE＋io_uring 诊断见验收索引；正式覆盖仍按注册矩阵逐项验收，
未执行项保持 open，不代替真实 HCA 或完整放行证据。生产部署另需授权，模板不提供自动执行器。
