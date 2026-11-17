# R45：显式启用磁盘 lifetime 淘汰

本文说明开发分支当前已实现的启动与恢复行为，不替代已批准的算法规格，也不宣称
性能、默认策略翻转或 RDMA/RXE＋io_uring 验收已经完成。

## 选择策略

```bash
# TCP、RAM 关闭；容量保持为 slab extent 大小的整数倍。
DFKV_RAM_TIER=0 ./build/dfkv_server \
  --dir /path/to/cache --cap 1073741824 --store-engine slab \
  --slab-write buffered --slab-eviction-policy lifetime
```

也可设置 `DFKV_SLAB_EVICTION_POLICY=lifetime`。命令行优先于环境变量，策略值
忽略 ASCII 大小写；显式空值及未知值报错，不视为缺省。显式 `legacy` 保留旧策略。

| 输入 | requested | effective | staged |
|---|---|---|---|
| slab，未指定 | lifetime | legacy | true |
| slab，显式 lifetime | lifetime | lifetime | false |
| slab，显式 legacy | legacy | legacy | false |
| file，未指定或显式 legacy | lifetime 或 legacy | not_applicable | false |
| file，显式 lifetime | 开盘前拒绝 | — | — |

以启动日志中的 `disk_eviction_requested`、`disk_eviction_effective`、
`disk_eviction_staged` 为准；请求 lifetime 不等于已经生效。**默认仍是 staged
legacy，本次没有默认翻转。**

## RAM 写回门禁

显式 lifetime 且 RAM write-back 时，必须显式设置 `DFKV_PUT_ACK_MODE=disk`。
ACK 缺省或设置为 `ram` 均在打开磁盘、分配 RAM tier 前拒绝启动；未指定 RAM
写模式时仍按既有默认 write-back 判断。RAM 关闭、write-around 不受此 disk-ACK
门禁限制。旧策略及 file 的既有 ACK 行为不变。

## 数值参数

显式 lifetime 会在开盘前严格解析下列十一项。仅接受无符号十进制数字，不接受
空值、符号、空白或非数字。范围错误、算术溢出及参数间关系不成立均打印含参数名、
用户原始值和约束的错误，以非零状态退出；不会降级为 legacy 或先创建缓存目录。

| 环境变量 | 默认值 | 含义 / 基本范围 |
|---|---|---|
| `DFKV_SLAB_LIFE_BASE_SEC` | 604800（7 天） | BASE，基础分秒数，1..INT64_MAX |
| `DFKV_SLAB_LIFE_READ_BONUS_SEC` | 302400（3 天 12 小时） | BONUS，每读档奖励秒数，0..INT64_MAX |
| `DFKV_SLAB_LIFE_MAX_READS` | 2 | M，评分读档上限，1..63 |
| `DFKV_SLAB_PROTECT_SEC` | 180（3 分钟） | P，新写保护及首次早读窗口，1..INT64_MAX |
| `DFKV_SLAB_PROTECT_SLOWDOWN` | 4 | SLOW，新写保护期内每 4 秒计 1 秒衰减，1..UINT64_MAX |
| `DFKV_SLAB_PROTECT_BOOST_SEC` | BASE + BONUS × M（默认 1209600，14 天） | BOOST，新写临时加分，另受下述关系约束 |
| `DFKV_SLAB_KEEP_SCORE_SEC` | 475200（5 天 12 小时） | KEEP，分数严格大于此值才受软保护，0..BASE−1 |
| `DFKV_SLAB_BUCKET_SEC` | 43200（12 小时） | G，保护到期桶宽，1..INT64_MAX，另受 H 约束 |
| `DFKV_SLAB_BUCKET_COUNT` | 24 | B，桶数，1..64 |
| `DFKV_SLAB_EVICT_LOW_PCT` | 88 | 回收到的低水位百分比，0..100 |
| `DFKV_SLAB_EVICT_HIGH_PCT` | 92 | 触发高水位百分比，0..100；0 仅关闭水位回收路径 |

参数不是独立旋钮。校验还要求：

- `Lmax = BASE + BONUS × M <= INT64_MAX`，`BASE + BOOST <= INT64_MAX`。
- `BASE - floor((P-1)/SLOW) + BOOST > Lmax`。
- `H = max(P, Lmax-KEEP+P-floor(P/SLOW)) + G <= INT32_MAX`，且 `B × G >= H`。
- HIGH 非零时，`LOW < HIGH`。

因此修改 M、BASE、BONUS 或 KEEP 时可能也需要调整 BOOST、G、B；不能仅凭各字段
各自落在基本范围内就认定整套参数可用。M 使用动态 `M+1` 个评分档，不需要修改
枚举或重新编译；M=1、2、4、63 使用同一套动态机制。

在未指定策略的 staged legacy、显式 legacy 或 file 下，前九项 lifetime-only
参数为 `inactive`，即使残留非法值也不解析、不阻止启动。LOW/HIGH **不是**
lifetime-only 参数，在旧模式下仍按原有规则适用，不能把它们也当作 inactive。

## 恢复边界

- 显式 lifetime 的 clean 恢复给现有住户虚拟评分读数 `n=2`，保留历史未知标识；
  这不代表发生过两次真实业务读，也不恢复旧的首次早读奖励。
- 恢复后的第一次实际成功读推进到 `n=3`，刷新读锚点，但不获得 `fw` 首次早读
  奖励；评分档仍按 M 封顶，例如 M=1 时仍落在档 1。真实读分类与评分档分开记录。
- dirty 恢复继续执行既有冷恢复重置合同，不因为启用 lifetime 而恢复原先会被
  清除的缓存住户。缓存不是持久数据库，不能将此策略切换视为数据持久化承诺。

本次不修改已批准规格，不增加性能试验或硬件验收结论。启动字段的完整上下文见
[部署配置](DEPLOY.md#当前磁盘淘汰策略请求值不等于生效值)。

## DOC6：结构、计热与保护桶

本文也是当前分支的集中操作入口；Q3/D4 等阶段说明中的“尚未开放”、临时恢复值及
对象大小是当时版本记录，不能覆盖本文和当前源码。完整未验收项见
[验收索引](DISK_LIFETIME_ACCEPTANCE.md)。

- 每个尺寸档有 `M+1` 条评分链，全盘另有 `M+1` 条跨尺寸档评分链；同一住户在
  两种索引中各有一个位置，评分档都是 `min(n+fw,M)`。类内淘汰和水位淘汰各用
  对应索引，比较已检查的有效候选；不是一条共享链，也不是五条全局物理链。
  M=2/4/63 时每组分别为 3/5/64 档，配置改变需要重启，不是动态扩容接口。
- W/R/O1/O2/O3 是五种**逻辑真实读分类**，最后一类表示三次及以上；与 M 无关，
  不随评分档数扩展枚举。恢复虚拟读不增加真实读数。非空成功读在原住户身份核验后
  应用；失败、空读、EXIST/元信息查询不计热。RAM 命中和合并 follower 各按适用
  同代副本通知，不能拿物理 I/O 次数代替逻辑请求数。
- 首次早读奖励 `fw` 要求 NEW 的首次实际成功应用距写入 **严格小于 P**；
  R 分类窗口则含等号 `<=P`。成功读撤下新写 BOOST，KEEP=0 仍表示 `score>0`
  受软保护，不是关闭保护。pin 是不能淘汰的硬约束，不会因分值低而绕过。
- donor 粗估保护数用 G/B 到期桶，必须满足上文 H 覆盖关系；KEEP、M、BONUS
  改动都会影响覆盖需求。桶是有界近似，不替代最终逐住户评分。精查每次最多
  8 个完整 extent、累计 8192 个住户。只要存在完整 exact 候选，就在已完整检查
  集合内择优；若没有可完成的 exact 候选（例如单条超过 8192 住户），可走全池
  粗排 fallback，并单列
  `dfkv_slab_lifetime_donor_budget_fallback_total`；这不保证全盘精确最小。
  全池 E 粗扫和选中后整条清表/sync 不在 8192 内，因此这不是耗时或锁持有上限。
- R 到期观察使用独立桶宽 `Q=ceil(P/(B-1))`（B>1）；P=0 或 B=1 时此观察机制
  不启用。默认 Q=8 秒，R 到期观测最多滞后 7 秒；B=64 时 Q=3、最多 2 秒。
  这不是评分桶宽 G，也不改变精确计热/评分。公开配置 P 必须大于 0。

评分可直接对照 `src/cache/disk_lifetime_math.cc`：令
`L=BASE+BONUS*min(n+fw,M)`、`age=max(now-anchor,0)`，分数单位均为秒：

| 状态 | score |
|---|---|
| 新写仍 armed 且 age<P | `L + BOOST - floor(age/SLOW)` |
| 新写仍 armed 且 age>=P | `L - (age-P+floor(P/SLOW))` |
| 已读/恢复、非 armed | `L - age` |

应用成功读刷新 anchor；未读 armed 在 P 到期时直接撤 BOOST。上述分段不是
`age/SLOW` 永远生效，恢复也不能重新享有 NEW 的 BOOST。

## 启动预检与 ACK 迁移清单

1. 保存旧二进制、启动环境/参数、resolved policy/ACK、几何及目录对应关系。
   缓存可重算不等于允许静默丢失已确认写入；切换前先停止新写并等待在途请求和
   写回结算。遇到 unhealthy 或失败结算不能当作正常排空。
2. 若启用 lifetime+RAM write-back，先显式配置 `DFKV_PUT_ACK_MODE=disk`。
   不能沿用缺省 RAM ACK 直接升级；write-around/关闭 RAM 不受此专门门禁限制。
   disk ACK 与旧 RAM ACK 的延迟差不能归为算法收益或回退。
3. 启动前校验全部适用数值、每盘几何和现有元数据。Group 在构造任何盘前对所有盘
   依次做只读 `ValidateExistingMeta/InitMarker/Epoch/DataFiles`；首次缺少文件可以
   创建，不会仅因不存在就拒启。已有损坏/不匹配则拒启。构造阶段复检防止直接
   Store 调用绕过校验；这不提供并发改目录的事务保证，禁止两进程共用缓存目录。
4. 启动后核对 engine、requested/effective/staged 和 ACK；有任何拒启先修正
   配置或处理数据/格式原因，**不要删除元数据来绕过校验**。不要拿 legacy 的
   inactive 参数展示当作已启用新策略。

## 背压、时间上界与受控重启

- 前台预算耗尽按 typed 结果保留 CacheFull 细因；后台预算暂缓不是客户端 IO
  故障。纯预算失败不应 MarkBad/unhealthy，也不能虚假 disk ACK。实际元数据
  I/O 故障仍按失效/健康契约处置，不应为了继续淘汰而换 donor 掩盖故障。
- RAM 后台已派发结算、前台等待返回、Store PUT follower 是三个不同分母；重试
  不算多次完成。已 ACK 的预算/时钟背压保留脏数据，不能当作成功落盘或最终丢弃。
  `DfkvRamPostAckDataLoss` 告警不替代 disk-ACK 启动门禁；通知送达/事件留存须另验。
- 所有 lifetime 盘共享同一单调启动原点。设 `T=UINT32_MAX-H`，新请求仅在
  elapsed `<=T` 时准入；越界后不因时钟回退恢复准入。已准入工作可用剩余时间
  排空至 `UINT32_MAX`，不是无限延期，也不是磁盘 I/O 的超时保证。
- 触发 restart-required 后 readiness 关闭、新请求拒绝，daemon 停止注册和服务并
  排空后以非零状态退出；进程管理器必须记录原因、检查故障并安排重启。不要只
  不断重试请求或改系统墙钟。超过硬上界不能 wrap、复用旧时间或静默切 legacy。
- 正常运维重启先撤流量、停止新请求、等待写回与 lease/pin/异步清理，再正常停止
  进程；只有正常关闭完成才依赖 clean 恢复。强杀/断电仍按 dirty 冷重置处理。
  新进程原点归零，clean 虚拟 n=2；这不是跨重启真实热度或业务存活年龄持久化。

## 回滚与尚未放行的事项

策略回滚使用**当前兼容二进制**显式 `--slab-eviction-policy legacy`，并将 ghost
容量设为 0（同时清除或覆盖环境中的非零值）；按上述排空流程重启，不在线改链。
legacy 不解析九项 inactive 评分参数，但几何、水位及其他旧校验仍适用。
政策切换不是降级任意旧二进制：同格式策略切换与 v3 以前格式升级/降级不同，
不能推定旧 binary 能读当前目录。先验证对应版本格式，保留回退方案；不承诺在线
无损、所有缓存必存或强杀可恢复。已有 lifetime↔legacy 同格式恢复回归不等于
任意版本兼容认证。

真实 RDMA/RXE＋io_uring 测试按约定后置；CPU helper/账本通过及硬件测试 skip 不
证明实际 Serve 完成/取消安全。完整资源/恢复峰值 RSS、业务重算/TTFT、连续重启
存活和正式性能仍未验收。先登记职责与样本隔离，再旧 baseline、冻结阈值、独立
至少三轮交替 AB7；通过后才单独实施 FLIP7。默认仍 legacy，部署需另行授权。

## 可迁移的验证命令

从任意源码副本的仓库根执行；构建目录可自行命名，无需依赖 `.omx` 指针文件。
以下是关闭可选硬件的 CPU 验证，不是性能测试或硬件验收；依赖须预先具备。

```bash
cmake -S . -B build-cpu -DDFKV_BUILD_TESTS=ON \
  -DDFKV_WITH_RDMA=OFF -DDFKV_WITH_URING=OFF
cmake --build build-cpu -j2
DFKV_STORE_ENGINE=file DFKV_SLAB_WRITE=buffered \
  ./build-cpu/disk_lifetime_public_test
./build-cpu/disk_lifetime_clock_test
DFKV_STORE_ENGINE=file DFKV_SLAB_WRITE=buffered \
  ctest --test-dir build-cpu --output-on-failure
```

`disk_lifetime_public_test` 自己隔离相关环境并构建真实 slab 场景，覆盖默认/显式
选择、非法值拒启、ACK 门禁、clean/dirty 和模式往返；clock suite 覆盖准入等号、
排空及硬上界。测试存在不等于当前构建执行过：交付时另附实际构建目录、版本、
命令、通过/失败/跳过数和原始日志。ON/sanitizer/硬件验证范围见
[验收索引](DISK_LIFETIME_ACCEPTANCE.md)，不能用上述 OFF 命令替代。
