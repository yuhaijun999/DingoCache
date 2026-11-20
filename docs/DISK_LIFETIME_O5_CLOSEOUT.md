# 磁盘寿命淘汰 O5 功能收口报告

状态：**本轮 O5 功能与回归收尾完成；整体正式验收未通过**。基线 `8fba940`；本轮未提交。
GPU 全部排除，不是通过；正式 A/B、默认切换和部署均未执行。
权威契约及阈值见[验收清单](DISK_LIFETIME_ACCEPTANCE.md)、
[正式 A/B 协议](DISK_LIFETIME_AB_PROTOCOL_V1.md)。

## 1. 本轮边界

CPU 实现与验证纳入，GPU 实现及硬件验证全部排除，不算通过。
不更改公式、保护优先级、盘上格式、ABI 或生产默认；不新增可选客户端扩展。
禁止压测宿主 `/mnt/disk1`，真实 RXE/io_uring 仅复用经身份和 backing 重新核验的隔离环境。
不启动正式 A/B、不翻默认、不部署、不自动提交或推送。

## 2. O01–O19 逐项核对

“功能已有”不等于持续负载、性能数值或业务收益验收；测试新鲜执行结果另见第5节。

| 项目 | 功能范围与本轮处置 | 不能据此关闭的事项 |
|---|---|---|
| O01 | engine/requested/effective、适用配置与拒启已有 | 不虚报 file/legacy 为 lifetime |
| O02 | victim/slot/payload/互斥路径与守恒已有 | 正式路径占比/回收速率 |
| O03 | 可见字节与物理占槽分账、延期 Remove 故障已有 | 不承诺故障前 Remove 已持久化 |
| O04 | 类内片/单请求/后台/水位/donor 预算已有 | 扫描界不是墙钟上限 |
| O05 | path×band visits/candidates/pin_skips、双索引游标已有 | 持续负载公平性，不是每 tick 必达256 |
| O06 | 真实读分类、恢复虚拟读、R 截止账已有 | 分桶滞后不是物理链迁移 |
| O07 | typed flush/follower 独立终局账已有 | 分母不能相加成完整 Node PUT |
| O08 | donor 有界选择、持久化失败停止已有 | 全 E 与选中整条清表成本仍需测量 |
| O09 | 本轮补 runtime 元数据 I/O 与回收 A 锁计量；专项及完整回归通过 | 完整成本/分位/资源阈值不是计数器通过 |
| O10 | 前后台/单批/预算与 IO 分项、disk ACK 闸门已有 | RAM ACK 不等于抗永久故障 |
| O11 | 专用立即评估告警及规则测试已有 | 实际 Alertmanager 送达/事件留存 |
| O12 | 逻辑读、decoded/dispatched PULL 终局账已有；本轮实际 lifetime Serve 联核通过 | 不是未解码 WC/所有传输/客户端交付分母 |
| O13 | 通知身份、来源资格、重复/错代/race_lost 已有；本轮 Serve 接线联核通过 | applied/success 不能代替来源资格分母 |
| O14 | 完成到应用、采样通知锁成本已有 | 通知开销性能放行 |
| O15 | 恢复状态/虚拟两读/连续短轨迹已有 | 全负载连续生存/过度保留业务代价 |
| O16 | victim 年龄/unknown/符号评分/保护比例已有 | unknown 不伪造历史年龄 |
| O17 | accepted 应用时刻的首读/再读间隔已有 | 幸存样本不能推出最优寿命 |
| O18 | bounded ghost/settlement 窗/同 entry witness 已有 | 不等于完整因果驱逐损失率 |
| O19 | 真实回填/重访与逻辑 payload 放大诊断已有 | 业务权重/真实重算 token/TTFT 缺外部数据 |

## 3. 安全硬闸门核对

| 项目 | 必须守住的范围 / 本轮回归入口 |
|---|---|
| S01 | 最后 pin 失效后的最终状态决定计热；disk_slab_store / disk_lifetime_read |
| S02 | Remove/PUT/promotion 同 key 围栏、disk ACK/flusher 无环；ram_tier_wiring |
| S03 | 同代来源资格、通知先行/淘汰先行、重复；read/wiring 与新增 Serve 联核 |
| S04 | I/O pin 不复用、持久化失败停止、取消清理；store/reclaim 与新增 Serve 联核 |
| S05 | 单请求累计预算、长 pin 前缀、owner/follower 终结；allocator/background/RAM |
| S06 | 全目录只读拒启、fresh 缺文件合法；public/group |
| S07 | H/KEEP/32位运行时钟边界与安全停接排空；math/clock/public/wiring |
| S08 | legacy/file/inactive 隔离、lifetime RAM write-back 显式 disk ACK；public/config |
| S09 | 纯预算不假 ACK/不 IO 隔离、混合故障逐项守恒；RAM/group/client health |
| S10 | legacy/RAM CLOCK/格式/重启兼容；本轮实际 lifetime Serve 边界回归通过 |

测试名存在不证明执行；旧 sanitizer、实际 RXE 和全量结果不得伪装为本轮重跑。

## 4. 外部门禁与范围排除

- E3：合法几何/布局已有功能测试，完整动态容器、恢复峰值 RSS、锁成本与预算待测。
- E4/O19：业务 trace、复用/前缀/重算/TTFT、业务权重缺失，不用合成 trace 或重复 PUT 冒充。
- 正式旧基线：同 ACK 可比 legacy、现役 RAM ACK 迁移参考、v2.33.0 行为基线未齐。
- 独立职责/数据可见范围、F01–F05 数值冻结、独立 >=3 轮交替正式样本未齐。
- Alertmanager 实际送达与事件留存未验收。
- GPU：用户排除，不是通过；HCA/跨物理机性能不能由单机 RXE 代替。

上述项目不能通过“收尾”标题、工具测试数量或任意百分比自动关闭。
保留既有探索负面结果：默认 lifetime 在部分持续/多尺寸负载有明显热点损失，
见[快速诊断配方](DISK_LIFETIME_QUICK_VALIDATION.md)，不得只保留有利场景。

## 5. 本轮实现与验证

### 实现

1. runtime `slots.tbl` 新增9种固定来源：record_write、foreground/background/watermark_evict、
   extent_bind/donor_bind、table_sync/bind_sync/donor_sync。分别记录尝试、完成、实际写字节、
   失败及耗时；部分写入后失败仍保留已写字节，旧指标语义不变。不是 syscall/NAND/持久化字节。
2. allocator A 锁新增 allocate/class_slice/watermark/donor 四种入口的次数、等待/持有纳秒累计。
   不增加锁或逐 slot 状态；allocate 包含普通分配，嵌套 donor 不重复算一段新锁。
   disabled legacy/RAM 不读计时时钟；不代表 Store D 锁、所有互斥锁或请求 p99。
3. donor 专用可选持久化回调，在释放住户前执行；未提供时仍调用原 bind 回调。
   Store 用它区分首次绑定与 donor 失效/sync，失败仍 fail-closed，不修改选 victim 的算法。
4. 两类 selector 标记在持有 D 的作用域内自动恢复，不能跨 yield/unlock 将后续前台 IO
   误标为后台/水位。全局/每盘指标分别聚合，固定标签无 key/request 高基数。
5. 三个显式 slab/lifetime/disk-ACK Serve 回归联核：READY 授权后无 READ 仍一次热度、
   重复 RELEASE 不重热；short-read→EOF 错误不热；队列停止只给已授权且来源同代的 RAM
   请求计热，未决定请求取消不热。后两项为真实 RXE Serve + **受控 CQE**，不冒充内核 ring 读。

### 已取得的本轮专项证据

- 12 个实际 Store 元数据用例与1个实际双盘指标用例通过，无 skip；含进程内实际坏 fd、
  子进程 RLIMIT_FSIZE 导致的16字节部分写失败、后台归属恢复、donor 失效失败保留住户。
- 新5个 allocator 用例覆盖早退、持久化失败、400次并发调用、donor 原回调兼容与失败隔离、
  legacy/RAM 零计时。Release 后又以 Debug/sanitizer 重跑，不能用 Release 跳过断言冒充合法夹具。
- TSan：5项×3轮 +12项 =27次执行，无失败/skip；ASan+UBSan：17项，无失败/skip。
  都是专项而非整个 sanitizer 套件，不等于共享内存/真实 RDMA 全面竞态证明。
  这27/17项覆盖 allocator/Store及对应测试，不证明Node指标命名导出经sanitizer检查；
  命名兼容由实际双盘红→绿、完整ON/OFF回归与独立16指标族冲突检查验证。
- GCC13 x86_64 同编译条件下固定布局实测：allocator 1352→1480B（含 Options 208→240B），
  Store 3280→3640B，Stats 快照2576→3032B。没有逐 slot 增量；这不是完整动态内存或恢复峰值 RSS。
  新计时有开销，未借此宣称零性能回退，旧模式可比基线与正式成本门槛仍保留。
- 告警契约 Python 7项通过；告警规则/阈值未修改。不是 Alertmanager 通知送达证明。

### 失败账与修复

- 审查发现9项路径数组只填6个旧名字，实际指标测试两次 SIGSEGV139；已替换正确9标签，
  使用推导大小数组和 static_assert 防止静默补空指针，修复后真实指标测试通过。
- Debug TSan 揭示新锁夹具未填桶配置，构造断言失败134。修复夹具为同一合法配置派生
  max_reads/桶宽/桶数/读窗口，不删生产断言。修复后的 TSan 和 ASan/UBSan 均通过。
  首次 ASan 构建已捕获旧夹具，故也保留134并顺序重编重跑；不是两个生产缺陷。
- 最初指标源变量作用域编译错误已修复；一次重叠的自有 Ninja 被精确停止，未拿中断构建作成功证据。

### 完整回归额外揭示的测试夹具竞争

最终 guest 完整回归曾失败：1796项计划、1769项实际执行，1738通过/1失败/30跳过，
另27项未执行；31个必选核心用例均通过。失败为纯内存 Python 并发夹具的
`test_full_fixed_schedule_uses_ten_handles_and_real_thread_overlap`，不是新 C++ 库的错误。

原夹具把 writer 压缩成200×1ms，却只在读线程发布后同步，不能保证8个fake GET都已进入。
writer 可能先完成并设置 stop，迟到线程不进入首读，其他线程在夹具自有8方barrier报错。
仅修测试：首读 barrier action 发布 ready，正常/全miss模式的首次冷写等待 ready；
原3秒超时不变，故障模式不新增等待，生产 driver/节拍/验收 oracle 全部不变。

新增第45个方法用 Event 正向扣住最后一个 reader、观察 writer 实际等待而未 ACK，
最终释放并 join，保留10句柄/8读流/200步断言。只在内存中移除此等待的负控会失败，
复现 writer 先结束而最后 reader 不能进入；修复后45方法通过，新受控用例预定5次均通过。
不靠“重试到绿”、放大超时或删除失败断言。失败原账保留，最终完整回归对新快照重新执行。

### 最终宿主 OFF 回归

构建目录为工作树下 `build-test`，GCC13.3.1、RelWithDebInfo、RDMA=OFF、io_uring=OFF。
最终 C++ 源快照已完整重编及回归；随后仅修 Python 夹具，构建入口退出0（Ninja no-op，不冒充重新编译）。执行以下命令（XML/日志存本轮证据目录）：

```bash
cmake --build build-test -j2
ctest --test-dir build-test -E '^Gpu(NodeDedup|DedupChild)\.' --output-on-failure
DFKV_STORE_ENGINE=slab DFKV_SLAB_WRITE=direct build-test/prepared_read_test \
  --gtest_filter=PreparedRead.CoalescedRangeDirectHeadShiftByteExact
```

最终 CTest **1539条目：1514通过、0失败、25跳过，220.68秒**，另有11个 GPU 条目在选择阶段排除。
25跳过为24个未启用真实etcd/MDS集成和1个默认file/buffered下的direct专用用例；
该direct用例另以显式slab/direct执行1项通过，不篡改原CTest的skip数。
CTest条目包含Python模块入口，不把入口数冒充展开后的Python方法数。

### 风格、静态检查与审查边界

- LLVM17.0.6：两个新增 C++ 文件完整格式检查通过；旧文件仅检查/调整本次差异，
  `git diff --check` 通过；局部说明 POSIX SIGXFSZ/mkdtemp 必须用系统头，不扩大 NOLINT。
- 新增两个测试文件完整 clang-tidy 通过；allocator、Store、Node 的修改行按完整配置检查通过。
  allocator 曾报缺少直接 `<functional>` 引用，已补齐并重查通过；对最终快照重新构建和回归。
- Group 完整配置300秒超时；只排除 `bugprone-unchecked-optional-access` 的重查通过。
  metrics 测试 TU 的完整检查在 LLVM17 该检查中崩溃139，限定重试超时；RDMA 大测试 TU
  完整检查300秒超时。**这些不是完整静态检查通过**，原日志/退出码保留；未改全仓规则、
  禁用全部检查或用 runtime 测试冒充静态分析。
- 独立只读审查覆盖11个功能源/测试文件及收口范围，修复指标标签 HIGH 后无未解决问题。
  编译仍有旧测试文件的既有警告，未借全文件格式化清理，也未新增编译警告。
- 本轮为必要功能与可观测性补齐，不新增依赖；未改变淘汰评分/默认、盘格式、C ABI 或协议。

### 最终隔离 ON 回归与证据归属

Ubuntu24.04/kernel6.8、GCC13.3 Debug、RDMA=ON/io_uring=ON、实际 RXE；
源码 `/opt/dfkv`，构建 `/opt/dfkv-build`。405项源码 SHA 与本轮11文件覆盖 SHA 前后核验一致。

- 前序完整构建200步及修复后131步保留为历史；最终指标命名修复的完整增量构建
  **132步、63.507秒、退出0**，两处源实际重新编译/链接。不是 fresh clean build。
- 最终非 GPU CTest **1796条目：1766通过、0失败、30跳过，381.69秒**；
  14个 GPU/CUDA 条目在选择阶段排除，不算跳过或通过。按大小写敏感的精确名称过滤，
  不误删 `FollowingPut`/`ConflictingPut` 等 CPU 安全用例。
- 31个必选用例全部实际通过、无 skip：新元数据12、新 allocator5、两项指标、
  新旧 Serve6、RXE1、真实 kernel ring5。后两项新增 Serve 的 CQE 受控边界仍按上文分列。
- Python 诊断模块实际执行45方法通过（36.449秒），不是仅CTest收集；日志保存完整输出。
- 30个 skip 保留为24个etcd/MDS环境条件、1个默认backend专用direct用例、5个多rail/
  CUDA相邻诊断条件，均不算通过。direct另以slab/direct单独执行1/1通过，无skip。
- 最初纯Python失败的完整计划/执行/未执行分母和原XML/log均保留；修复后在相同1796
  选择集/14排除/31必选口径上重新完整执行，不只重跑一个失败用例。

完整 ON 调度由最终命名修复证据目录的 `guest-regression.py` / `regression.sh`
记录：顺序 CTest、120秒每项及840秒进程界、900秒外层监护；先冻结 inventory/selection，
再执行并独立复核完整 JUnit。实际使用的等价测试入口为：

```bash
: "${GPU_EXACT_REGEX:?先按冻结selection.json设置并核对14项精确排除名单}"
cmake --build /opt/dfkv-build -j2
# GPU_EXACT_REGEX 来自冻结 selection.json 的14个精确排除名称，不用忽略大小写的gpu子串。
ctest --test-dir /opt/dfkv-build -j1 -E "$GPU_EXACT_REGEX" \
  --timeout 120 --stop-on-failure --output-on-failure
DFKV_STORE_ENGINE=slab DFKV_SLAB_WRITE=direct /opt/dfkv-build/prepared_read_test \
  --gtest_filter=PreparedRead.CoalescedRangeDirectHeadShiftByteExact
```

不要在宿主照搬 guest 路径/历史PID；真实负载前必须重新核验隔离环境和全部 backing，
并有新监控、温度/告警/样本新鲜度与自有进程回收监护。原始审计可只读复跑 `audit-raw.py`。

### 资源与清理

- 最终命名修复 runtime 原账：
  `.omx/evidence/o5-metadata-family-runtime/20261120T100309Z-8fba940/`，
  raw 为 `results/o5-family-20261120T100309Z-8fba940/`，归档9 entries / 3555736B。
  当前Node SHA `94044b8a…`、metrics测试 `f6744545…` 与405源清单及编译产物前后核验一致。
- 前序原账 `.omx/evidence/o5-closeout-runtime/20261120T085637Z-8fba940/` 保留较早
  成功/失败/夹具修复快照；其21 entries / 10693911B归档不代替最终命名修复证据。
- 最后新宿主落盘：
  `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/o5-family-20261120T100309Z-8fba940/`。
  前序目录：`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/o5-closeout-20261120T085637Z-8fba940/`。
  guest 结果前缀 `/data/disk1/results/o5-family-20261120T100309Z-8fba940`；guest disk1
  backing 为**宿主disk2**，不是被禁止的宿主 `/mnt/disk1`。
- 两轮都先后核验六FD/UID/start/netns/backing仅宿主disk2/3。前序330个SMART样本最高
  42/41°C、warning全0、最大间隔10.267秒；最终新监控89样本最高**42/40°C**、warning全0、
  最大间隔10.184秒。>=65°C、非零warning或非法/过期样本须停止自有验证。
  未实际注入温控/监控失活故障，不冒充该项动态验证。
- 两个自有监控均正常退出0、回收并确认PID不存在；测试服务退出，
  12000/12001/12010端口空闲，VM空闲。无待处理自有负载或监控。
  没有默认Docker或宿主disk1压力，没有宿主网络/模块/全局sysctl改动。
  保留原账供复核；不要删除仍承载空闲VM的父目录或旧磁盘1资产。

### 最终命名兼容复核（完成）

复核发现新增 `metadata_io_errors_total{path}` 与旧无标签 broad counter 同名，
重复 HELP/TYPE 并混合旧查询分母。已补真实回归：旧HELP/TYPE各一次、旧指标无路径标签、
原无标签值保留；实际旧代码1用例失败3项断言，独立证实冲突，不是只读推测。

新 ledger 已统一命名 `dfkv_slab_metadata_ops_*` / `dfkv_slab_disk_metadata_ops_*`；
内部计量不变，旧 `dfkv_slab_metadata_io_errors_total` 的调用/HELP/TYPE/取值与HEAD完全一致，
旧Grafana查询不用改。新增9条ops错误样本及双盘字节断言实际1用例通过，无skip。
独立检查16个新增指标族无HEAD名称碰撞；代码审查恢复APPROVE，新快照完整OFF/ON回归均通过。

命名修复的最终回归使用新证据目录
`.omx/evidence/o5-metadata-family-runtime/20261120T100309Z-8fba940/`，独立新监控与原账。
最终两处源SHA：Node `94044b8a…`，metrics测试 `f6744545…`；11功能文件列表仍在本轮主证据。
新快照最终OFF1514通过/25skip、ON1766通过/30skip，均0失败；31必选实际通过无skip，direct另1。
较早的成功快照不冒充命名修复后的最终验证，命名红→绿及先前失败全部保留。

## 6. 交付与停止点

本工作树15个任务文件：6个生产源/头（allocator/Store/Group/Node）、5个C++/Python测试、
3个文档和CHANGELOG。用户原有 AGENTS.md 与个人工具文件未纳入。
没有新增配置旋钮、依赖、线上指标阈值或默认切换；保留性能/完整资源与业务门禁。

当前可以称 **O5 本轮功能收尾完成**，不能称 O5/E4整体正式验收、AB7完成或上线获准。
下一阶段需第4节的外部输入和独立冻结/正式验证，不再无限追加可选客户端或GPU功能。
本轮不自动提交/推送，修改留给用户审阅；工作树外的文档导航与本地进度索引已同步。
最终独立原账审计PASS，证据为主目录 `independent-final-audit.{py,json,md,log}`。
本地证据目录 `.omx/evidence/o5-closeout/20261120-8fba940/` 不随 Git 分发；
外部复跑者需要取得原账或重跑。历史证据按验收页和运行时说明索引，不换名为本轮证据。
