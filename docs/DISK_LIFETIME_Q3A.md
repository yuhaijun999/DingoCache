# Q3a：双索引与内部类内回收

当前已接两套评分索引，以及**内部显式门控**的前台选择、后台回收及预算。
公开lifetime仍未开放；Q3c已接通Group/Node内部执行及开盘前disk-ACK门禁。
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

- `DiskSlabStore::Options::lifetime_class_eviction` 默认 false。Q3c增加Group
  同名内部选项显式转发，CLI不设置；shadow config/clock不会自动启用。
  开启要求有效scoring/clock；可设置后台周期。
  新后台路径先运行Q3b评分水位，再运行class回收，不混入legacy donor。
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
  新内部Group/Node路径由Q3c实施显式disk-ACK启动门禁，不依赖此防御分支。

## 本轮后台 class reclaim

- 复用同一评分/失效函数，但不为每个 victim 重新领取前台 PUT 预算。
  每 class 每 tick 默认最多4096成功 victim、累计最多16384次访问；
  访问上限始终为本 tick 配置成功上限的4倍，不随剩余 batch 缩减重算。
- 单次 allocator 持锁的所有 victim 扫描合计最多1024次访问、128个
  成功 victim；不是每个 victim 单独最多1024后再无限累加。
- 每片先清所有已完成失效的 payload/tenant 账，释放 D/A 后再续片。
  真失效失败、硬时钟耗尽立即停止；先前成功失效仍保留，不回滚复活。
  保留 target、unbound pool 和整条归还的停止条件，避免级联缩容。
- 共享队列 cursor 保留跨 tick 位置；单独保存待完成的 headroom target，
  即使没有新 PUT，安静 tick 仍会续扫。达到目标后清掉任务，不空转扫描。
- 全 pin/当前无候选与访问或成功件数到限分开计数；保留待做任务到后续
  tick，不在同一 tick 无限重试。后台暂缓不变成某个客户端的 CacheFull。
- ReclaimTick 在内部开关下先运行Q3b评分水位，再运行新class路径；
  不运行旧全局水位或donor GROW，普通shadow/legacy保持原行为。
- 四个 background 计数与前台计数分开，`reclaimed_slots` 仍统计真实
  后台成功删除量。扫描计数不是 p99 的硬界，失效 I/O 仍遵守既有契约。

## 内存与验收边界

Q3a提交时 GCC13/x86_64：allocator568B不变，Store1672→1728B（固定+56，含4个
后台计数和pending vector）；StoreOptions200B不变，Stats496→528B。
SlotMeta104、links20、queue12字节不变；既有 enabled版本8B/class保留。
新增 pending target 每class一个size_t（本ABI为8B），只在新后台路径分配。
`sizeof(LifetimeScan)=640`，前台按owner、后台按当前class/tick持有，不是逐住户
字段。不以结构成本冒充 RSS、锁延迟或 p99 验收。

- 生命周期：`test/cache/slab_lifetime_storage_test.cc`。
- 类内选择与真实 Store：`test/cache/slab_lifetime_eviction_test.cc`，包含
  M=1/4/63、长 pin 前缀、变热耗尽、pin 变化、版本饱和、真正失效故障、
  owner/follower及三项批次部分成功；不是只验证独立容器。
- typed RAM 终局与混批：`test/cache/ram_tier_test.cc`。
- 后台回收：`test/cache/slab_lifetime_background_test.cc`，涵盖完整持锁片
  访问上界、安静tick续扫、全pin、部分成功后失效失败、时钟硬终局、两层锁
  释放及真实线程运行/析构join/干净重启。
- 固定维度扫描/预算指标见 `docs/METRICS.md`。

Q3a 的内部类内实现本轮收口；不冒称完整策略、性能或公开激活已验收。
Q3b全盘水位公平选择见 `docs/DISK_LIFETIME_Q3B.md`；
Q3c写回接线/ACK门禁见 `docs/DISK_LIFETIME_Q3C.md`，D4桶/donor待后续。
L12真实MR/READY/RXE/kernel io_uring数据面继续按用户约定后置，不算通过。
