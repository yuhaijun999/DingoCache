# Q3a：双索引与内部前台类内选择

当前已接两套评分索引，以及**内部显式门控**的前台选择与预算。
公开 lifetime、Group/Node 新策略激活和 disk-ACK 启动门禁尚未开放。
普通 shadow 配置仍使用 legacy victim；RAM CLOCK 不变。

## 两套索引

- 每个 class 和全盘分别拥有 MAX_READS+1 档，头数为
  `(class_count+1)*(MAX_READS+1)`，不是固定五链。
- 有效档为 `min(n+fw, MAX_READS)`；fw 的首次早读奖励也影响档位。
- 已提交且评分状态存在才入两套链；硬时钟后的空状态不伪造热度。
- generation/state/双挂链、成功读双迁档均在同一 allocator 锁内完成；
  即使有效档封顶或同秒成功读也摘下重插。不因 R 逻辑到期搬链。
- 按 anchor 非递减排列；相同 anchor 保持插入顺序，不冒称身份有序。
  正常时间顺序尾插 O(1)，旧 anchor 交错提交从尾向前定位，最坏 O(N)。
- 物理释放才摘两套链并修复 cursor；延期 Remove 和失败失效不提前
  释放成员。挂链不是可淘汰的证明，还需核验代际、可见性与 pin。

## 本轮前台选择与预算

- `DiskSlabStore::Options::lifetime_class_eviction` 默认 false，Group/CLI
  不转发该内部测试开关。开启要求有效 scoring/clock 及后台周期为0；
  不允许混入尚未接通的 legacy 后台 reclaim。
- 一片 allocator 锁内最多访问1024个 resident，使用同一 now。
  各档轮转检查本片首个合法候选，按 score、anchor、住户 generation
  比较；KEEP 是软线，pin 不能淘汰。只保证实际检查候选中的最低分。
- 候选评分不跨锁保存，选中后在同一 A 临界区完成一次持久失效。
  失效失败立即停止、不释放住户、不继续 donor。
- 唯一 PUT owner 首次进入类内尝试时固定 N，用安全64位计算
  `B=2*N+2` 次访问和 `ceil(B/1024)+1` 次机会；零访问也计机会。
- owner 保存自己的遍历位置。class 成员/热度或 pin 资格变化时，
  重建进度但不重置预算；版本饱和不回绕。其他 owner 推动共享游标
  不能冒充本请求已完整覆盖。续片不能重新执行前置 donor 升级。
- 单件、direct、batch 共用 Store 分配 helper，先清已失效可见账，
  再同时释放 D/A，续片核验健康和容量。PutFlight 保留唯一 owner，
  不重新进入自己的 follower 分支；所有终局写结果并唤醒等待者。
- 只有 `CacheFull + kScanBudgetExhausted` 是预算背压；没有新增 wire
  状态，也不把普通分配失败、全 pin 或真实 I/O 一律改成 CacheFull。
- RAM typed 写回对精确预算原因有限重试，未 ACK 写最终完成 CacheFull
  且不单独标 unhealthy；真实 I/O 保留隔离。防御性遇到已 RAM-ACK 的
  预算失败保留唯一 dirty/flush-pin 副本，不能丢弃或宣称完成 drain。
  这不代替将来 Group/Node 激活前必须实施的显式 disk-ACK 启动闸门。

## 内存与验收边界

当前 GCC13/x86_64：allocator544→568B（固定+24，含RAM），Store1592→1672B
（含选项、5个计数及测试同步hook）；SlotMeta104、links20、queue12字节不变。
启用的链接按 metadata 高水位保留容量，另有每 class 8字节 mutation version。
`sizeof(LifetimeScan)=640`，仅由唯一 owner 的 helper 持有，不是逐住户字段。
禁用时不分配这些版本/链接/队头；不以结构成本冒充 RSS、锁延迟或 p99 验收。

- 生命周期：`test/cache/slab_lifetime_storage_test.cc`。
- 类内选择与真实 Store：`test/cache/slab_lifetime_eviction_test.cc`，包含
  M=1/4/63、长 pin 前缀、变热耗尽、pin 变化、版本饱和、真正失效故障、
  owner/follower及三项批次部分成功；不是只验证独立容器。
- typed RAM 终局与混批：`test/cache/ram_tier_test.cc`。
- 固定维度扫描/预算指标见 `docs/METRICS.md`。

**尚未完成整个Q3a**：后台 class reclaim 的同片/每 tick 预算还未接通。
Q3b全盘水位公平选择、完整Q3c端到端激活/ACK门禁及D4桶/donor仍待后续。
L12真实MR/READY/RXE/kernel io_uring数据面继续按用户约定后置，不算通过。
