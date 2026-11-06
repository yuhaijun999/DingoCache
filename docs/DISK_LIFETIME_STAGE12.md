# 磁盘寿命评分：L12 阶段回归边界

本页对应 A → B+C → D → E 的 CPU/API 收口。评分仅由内部测试配置启用；
**公开 lifetime 策略仍未开放，legacy victim 与 RAM CLOCK 未替换。**
测试通过不等于 Q3 双索引、D4 保护桶/donor 或正式性能验收通过。

## 成功口径与不变量

- 同步读：最终状态为成功且有效字节非零，按每个逻辑请求通知。
- PULL：有效 READ MR 与授权所有权就绪后、READY 编码前通知一次；
  不等 READY 发送或客户端消费。准备、授权前失败/Abort、零字节不计。
- 授权后取消、发送失败、RELEASE 只回收资源，不倒扣、不重计。
- 凭据来自该读实际返回的字节；KEEP_EXISTING 不证明输入字节成为住户，
  不能用其空 identity 冒充新的来源。绝不按当前同名 key 补绑定。
- 通知不持 RAM 锁跨磁盘锁；在 D → A 锁序下核验同代并采实际应用时间。
  迟到或反序通知各计一次，anchor 不回退，首读资格不使用旧完成时间。
- hard 时钟耗尽后保留原请求/授权成功，明确统计未计热原因；
  hard NEW 的空评分状态优先记 clock，而非 state_unavailable。
- 通知计数互斥守恒：successful = applied + no_binding + race_lost +
  clock_hard_exhausted + io_unhealthy + state_unavailable。

## 验收矩阵

下列测试位于仓库的真实 GoogleTest/CTest 入口；不是仅审阅伪代码。

| 项目 | 当前证据入口 | 边界 |
|---|---|---|
| U1 参数、溢出、MAX_READS、KEEP、H | [disk_lifetime_math_test.cc](../test/cache/disk_lifetime_math_test.cc) | 纯数学/配置验证；不代表实际多链数量已实现 |
| U2 状态、保护边界、真实/虚拟读 | [disk_lifetime_state_test.cc](../test/cache/disk_lifetime_state_test.cc)、[disk_lifetime_read_test.cc](../test/cache/disk_lifetime_read_test.cc) | 覆盖实际 Store 应用时间、反序完成、持 D 的受控并发跨 P/hard（并核对源码锁序）；不验证新 victim |
| U3 准入、排空、硬终局 | [disk_lifetime_clock_test.cc](../test/cache/disk_lifetime_clock_test.cc)、[disk_lifetime_resident_test.cc](../test/cache/disk_lifetime_resident_test.cc)、[disk_lifetime_reclaim_test.cc](../test/cache/disk_lifetime_reclaim_test.cc) | hard NEW 保留数据/身份但无热状态，真实后续通知仍分类 clock；不承诺 H 内一定排空 |
| U4 同源凭据与迟到通知 | [disk_lifetime_read_test.cc](../test/cache/disk_lifetime_read_test.cc)、[ram_tier_wiring_test.cc](../test/cache/ram_tier_wiring_test.cc) | 实例/epoch/generation/可见性不匹配跳过；不热新住户 |
| I0b 同步最后 pin 与 Remove | wiring 的 LastReleaseRemoveUsesFinalStatusAndOriginalSource；Store 的 LastPinRemovePreservesSourceButRejectsHeat | 失效成功后可返回旧字节但记 race；失效失败最终 IOError 不热 |
| I1 跨层同 key 与 promotion | wiring 的 ForegroundRewriteRetiresOldRamWithScoringEnabled、ConcurrentRemoveFencesLatePromotionBeforeForegroundRewrite | 使用真实 InitRam typed sinks；覆盖 write-around/write-back disk-ACK、direct/batch、旧 pin；实际无 pin 的 write-back 验证 flush 增量；DropDisk 仅模拟副本消失，不冒称压力淘汰 |
| I2 RAM/coalescer 与恢复 | wiring 的 CopyFollowersCountRequestsRatherThanOneIo、CleanRecoveryFirstReadKeepsUnknownHistory；[rdma_loopback_test.cc](../test/transport/rdma_loopback_test.cc) 的 PublishedFollowerHasItsOwnLogicalGrant | leader/follower 各一次；阶段恢复 seed=1、未知历史，不冒充 R45 正式 seed=2 |
| I3 PULL CPU 接缝 | RdmaPullHeatTest、RdmaPullAdmissionTest；[prepared_read_test.cc](../test/cache/prepared_read_test.cc) | 真实 pread/RAM pin/共享 flight；显式模拟 MR 成功之后调用生产 helper，不进行真实 MR 注册 |
| legacy/RAM 隔离 | SlabLifetimeStorage.DisabledStateStorageNeverAllocatesPerSlot、DiskLifetimeResidentTest.ClockOnlyAddsNoCommitSampling、RdmaPullHeatTest.ClockOnlyLegacyGrantNeverSamplesCompletionTime | 无评分配置不分配 state，不采新的应用时钟、不记热；既有 admission 的采样不是新增 completion 采样 |

I3 的真实 Serve/MR 注册成功与失败、Stage 注册重试、READY/PostSend 异常、
RXE/HCA 远端 READ，以及真实内核 io_uring 数据面，按用户约定后置，
**尚未验收，skip 不算通过**。当前 RDMA responder-WRITE GET 已退役，
旧 kRange 只适用于 TCP；不能虚构一条普通 RDMA SEND GET 路径的通过结果。

## 内存与锁成本

当前 GCC 13 / x86-64 构建测得 State=12B、optional State=16B、
SlotMeta=104B；这些数值不是跨 ABI 承诺。启用路径的额外 dense 数据应按
generation.capacity() × 8B + state.capacity() × 16B 估算，
**不是 live 数量 × 单项大小**；删除后仍保留 metadata 高水位。
ReadHeatKeepsMetadataCapacityAtItsHighWaterMark 在真实 Store 上核对读热更新
不扩张这两个数组，并在 XML 中登记实际 capacity 字节。

A 增加 allocator 固定 32B（RAM 的每个 shard 同样承担）；A 的 Store 固定
增加 112B，B+C 再增加 56B 计数器；D 的 RdmaServer 固定增加 32B callback，
Node/PreparedRead 不变。E 仅补测试/文档，不新增生产字段、锁或统计框架。
这些是结构成本，不是进程 RSS、恢复峰值或吞吐/p99 验收结论。

同代有效通知同步获得 D、再获得 A，并采一次 TryDrainNow；缺绑定、错代或
I/O 隔离可提前返回，默认关闭和零字节不进入新的热度锁/采样。
RAM pin 可以仍存活，但 RAM mutex 必须已释放。PULL 通知发生在 Serve 线程，
等待 D/A 可能延长 CQ 推进间隔。E 的持锁回归只验证正确性，不测量或承诺锁 p99；
正式成本/性能阈值与实验仍属于 O5/AB7。

## 执行与放行

基础构建使用 RDMA/URING=OFF；PULL CPU 用例另在 ON 构建中执行：

~~~bash
cmake --build build-test -j2
DFKV_STORE_ENGINE=file DFKV_SLAB_WRITE=buffered \
  ctest --test-dir build-test --output-on-failure
cmake --build build -j2
DFKV_STORE_ENGINE=file DFKV_SLAB_WRITE=buffered \
  ctest --test-dir build --output-on-failure \
  -R 'RdmaPull(Heat|Admission)Test\.'
~~~

构建前须分别正确配置这两个目录；构建/测试的 TMPDIR 放在工作盘，
不要默认使用空间紧张的系统 /tmp。共享状态回归另跑 TSan 与 ASan/UBSan；
新并发场景重复执行，原始失败与跳过原因单列。每次报告保留实际命令、
XML、退出码、源码 SHA 和改动行静态诊断；不把旧文件警告写成全文件 clean。

E 不负责新链/桶/victim、R45 正式恢复/开放或默认切换。
完整 transport 硬件验收仍待后置验证，不因本文汇总而自动放行。
