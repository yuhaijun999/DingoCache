# 真实 GET 状态接口：兼容方案与第一小步

基线 `752d1eb`，2026-11-19。属于 **O5 / AB7前置准备**，不是正式A/B放行。
首步实现C++单键buffer接口；随后新增C ABI和验证用Python单键适配（见第5节）。
下文第1–4节保留首步设计/验证记录，批量去重状态仍待后续小步接入。

## 1. 状态在哪里丢失

| 层 | 现有信息与问题 |
|---|---|
| TCP / RDMA `Transport::RangeInto` | 保留每项 `Status` 与完整stored size；连接/读写/解码失败不是 `NotFound` |
| C++单键buffer / TCP批量内部 | 原 `GetAutoDirect` 将NotFound、IO、无路由、冷却、容量不足压成false |
| C++批量RDMA、batch rendezvous | 返回bool/长度，失败Fetch只Abort；Wait失败直接重读，不保存失败类别 |
| 现有C ABI / Python | `out_hit=0`不是可信miss；正常C调用rc=0也不代表读取成功 |

相关实现：`src/transport/tcp_transport.cc` 的 `RoundTripInto/RangeInto`、
`src/transport/rdma_transport.cc` 的 `RangeInto/PullInto`、
`src/client/kv_client.cc` 的 `GetAutoDirect/BatchGetAutoDirect/BatchGetAuto`，
以及 `src/client/dfkv_c_api.cc` 的 `dfkv_batch_get_auto`。
EXIST同样会丢失失败类别，不能通过重复GET/EXIST把不确定结果证明成缓存miss。

## 2. 本轮兼容决策

新增方法，无复杂结果结构体、无新状态枚举：

```cpp
Status KVClient::GetAutoWithStatus(const std::string& key, void* out,
                                  size_t cap, size_t* out_len);
```

| 情况 | 返回 | 可计缓存miss？ |
|---|---|---|
| 明确成功、单项长度可信且放得下 | `kOk` | 否，命中 |
| transport明确返回不存在 | `kNotFound` | **是，仅此情况** |
| 无路由、peer冷却 | `kIOError` | 否；本地未发新I/O，不证明本次网络失败 |
| 空key、非零容量配空buffer、完整对象超过容量 | `kInvalid` | 否 |
| transport I/O / 资源 / rail / 其他失败 | 原 `Status` | 否 |
| 返回空/多项status，或kOk缺失/多项长度 | `kIOError` | 否；不能索引越界或伪造0字节成功 |

`out_len`可为空；非空则入口清零，仅kOk写实际完整长度。保留旧的kOk+长度0语义，
不能凭长度0推导miss。失败目标buffer可能已被部分写入，应全部丢弃，不承诺回滚。
异常仍按既有C++边界传播，不伪造NotFound；即使transport抛异常，外部长度也已清零，
公开请求指标仍由RAII计一次。这不是新增异常式业务控制流。

共享私有 `GetAutoStatusDirect` 保留RangeInto路径和目标内存分类：
- 旧buffer bool公开方法仅以状态==kOk返回true；新公开方法统一记一次GET指标。
- 私有bool包装供TCP batch使用，不再记一次逐项公开指标，避免重复分母。
- 仅实际transport的IOError调用MarkBad；无路由/冷却/本地结果形状异常不补记peer错误。
  原有效Ok/NotFound健康更新保持；资源不足、无兼容rail不冷却peer。
- string overload、批量RDMA分组、SG与dedup实现暂不改，不冒充全路径状态已贯通。

旧C符号/签名/返回值、Status数值/底层类型、wire字节、磁盘格式、生产默认均不改。
旧bool对正常有效输入的成功/失败含义保留。异常结果形状现在安全失败，
修正原来的 `[0]`越界或缺长度假成功；非法空key/nullbuffer提前拒绝。
**不宣称这些非法输入/异常transport行为也与旧版本逐字节一致。**

## 3. 分小步实施顺序

1. **本轮完成：C++单键buffer状态核心＋兼容包装＋单测。**
2. 后续：新增独立C ABI符号；显式定义稳定C结果常量/参数及调用级错误，
   通过显式映射保持与C++枚举布局解耦。不能改旧函数或直接转换内部枚举数值当新ABI契约。
3. 再接Python适配器：显式探测新符号；旧库继续返回 `get_unresolved`，
   不允许bool假推miss。新旧库混用、坏参数、真实服务错误均须回归。
4. 最后独立处理批量/SG/dedup：保留失败Abort、不把IO发布成缓存不存在；
   再逐项保真、测试混合成功/缺失/错误与等待回退。不能假装串行循环等价原批量并发。

第2–4步尚未实现，也不自动扩大为本轮改动。当前Python真实冒烟仍保持严格hit-only检查；
C++新接口通过不代表正式trace已具备可信miss计量，更不解除业务/旧baseline/独立职责/数值门槛。

## 4. 验证与范围

新增 `test/client/get_auto_status_test.cc` 自动由CMake收集：覆盖全部8种已有transport状态、
两种pipeline标志、无路由、冷却、资源/rail中立性、容量/参数、坏结果形状、零长度兼容、
可选长度、旧bool/TCP batch指标一次，以及8线程独立输出。
另以独占临时目录、1MiB显式file后端和真实TCP loopback验证PUT→命中、
不存在→NotFound、容量小→Invalid、停服→IOError，非仅mock状态自洽。

先写测试得到新API缺失的编译RED；审阅又发现初稿在transport抛异常时未提前清除公开
out_len，新增回归真实得到999未清零的RED，再补入口清零。测试中的抛异常仅故障替身，
不是声称真实socket错误通过异常传递。后续测试/构建证据见本地
`.omx/evidence/get-status/20261119T030526Z-752d1eb/`。

```bash
cmake --build build-test -j2
DFKV_STORE_ENGINE=file DFKV_SLAB_WRITE=buffered build-test/get_auto_status_test
ctest --test-dir build-test \
  -R '^(GetAuto|KvClientHealth|KVClientTest|Client|CApi|Batch|AutoBatchWorkers)' \
  --output-on-failure
```

本轮不运行VM、真实RXE/io_uring、压力或正式A/B，不将fake pipeline=true当真实RDMA证明。
临时文件在root文件系统 `/tmp`；继续禁止压测宿主 `/mnt/disk1`。

最终验证：新增11个GoogleTest用例通过，相关81项CTest全部通过（含新增项），
0失败/0跳过，1.28秒；本机GCC13/RelWithDebInfo、RDMA=OFF/URING=OFF完整构建成功。
独立审阅APPROVE。LLVM17.0.6：新测试全clang-tidy检查与默认format检查通过；
核心本次diff范围全clang-tidy检查通过，整文件范围外既有诊断未整改。
初轮新测试const/直接头依赖诊断及本轮核心直接依赖已修复，不把初轮失败说成通过。
旧文件保留原include块顺序，diff格式检查关闭include重排以避免无关整块格式化；
默认检查的排序建议另留证据，没有改项目格式配置或批量压制lint规则。
POSIX mkdtemp直接包含stdlib.h，针对无C++等价声明的deprecated-header检查给出局部技术理由。
未做新ON构建/真实RDMA、全量CTest或sanitizer，未改变共享锁/状态布局；
8线程用例不是TSan或高并发性能证明。

## 5. 第二步：兼容C ABI＋验证用Python单键读取

源码基线 `f5e0ded`。新增 `dfkv_get_auto_status`，参数与旧scalar auto相同，
直接返回稳定的int常量 `DFKV_GET_*`，不新增结果struct/枚举，不依赖内部Status布局：

| 常量后缀 | 固定值 | 含义 |
|---|---:|---|
| OK | 0 | 完整读取成功，长度0仍是命中 |
| NOT_FOUND | 1 | 明确不存在，唯一可信miss |
| CACHE_FULL / QUOTA_EXCEEDED | 2 / 3 | 相应失败，非miss |
| IO_ERROR | 4 | I/O失败或本地无路由/冷却拒绝，非miss |
| INVALID | 5 | 参数/容量等错误，包括null handle |
| RESOURCE_EXHAUSTED / NO_COMPATIBLE_RAIL | 6 / 7 | 客户端资源/rail失败 |
| INTERNAL_ERROR | 8 | C边界捕获异常或未识别内部状态 |

C头使用宏常量保持C兼容，没有C++ enum ABI依赖；实现逐项switch显式映射。
这是单键结果，不再叠加“调用rc＋out_status”双层结构。非法参数直接INVALID，
既有NoThrow边界把异常变为INTERNAL_ERROR；不把异常当NOT_FOUND或抛出C边界。
可选out_len入口清零、仅OK设置；失败buffer可能部分写入，调用方必须丢弃。
有效指针/有效handle生命周期仍由调用方负责，不能检查伪造/悬空地址。
旧函数/符号/返回值、wire、Status数值、批量/SG/dedup均不改。

`deploy/validation/disk_scenarios.py` loader显式探测并配置可选ctypes符号。
新增单独 `Client.read_outcome`，仅供trace单键适配：
- 新库OK须通过原有完整字节及长度oracle，返回get_hit；
- 仅NOT_FOUND且长度0返回get_miss；其余失败/未知码/矛盾长度抛错，不回退重读；
- 旧库无符号时仍走原batch单项读取，no-hit是get_unresolved，绝不靠EXIST假推miss。

原并发batch诊断及生产 `dfkv_common`/连接器没有改成逐项串行循环。
这不是宣称全部Python或批量路径已状态保真。TraceClient保留原close互斥/缓冲区生命期。
严格hit-only smoke收到get_miss仍失败、停止后续请求；父验收器现在认识该完整失败终局，
保留已知尝试/完成计数，而非误判invalid_result或允许miss通过。

新增6个C ABI用例（含真实tiny file/TCP服务），Python adapter14个、smoke17个方法。
C ABI首次编译时实现已存在，不声称有API缺失RED；审阅发现smoke未识别get_miss，
先补回归得到真实ValueError RED，再修复并通过。另用本轮libdfkv.so＋真实TCP服务运行
Python ctypes全链路：显式不存在、PUT后完整命中、先Remove再写大对象→INVALID、停服→IO_ERROR。
脚本初次把日志放在cache目录导致启动被正确拒绝；另一次对已有key重复PUT未改变旧值，
改为先Remove再PUT后得到预期大对象错误。保留失败尝试，不计成功样本，不算生产缺陷。

验证命令（仓库根目录）：

```bash
cmake --build build-test -j2
ctest --test-dir build-test --output-on-failure \
  -R '^(GetAuto|KvClientHealth|KVClientTest|Client|CApi|Batch|AutoBatchWorkers|python_eviction_|python_disk_validation|python_quick_eviction)'
```

本轮证据：`.omx/evidence/cabi-get-status/20261119T034633Z/`。
未运行VM/RXE/io_uring、全量CTest、sanitizer或压力；继续禁止宿主/mnt/disk1压测。
正式A/B与生产默认不变。下一步应先核验新状态接口在有界真实服务冒烟中的终局及报告，
再单独设计批量/SG/dedup状态传播；不自动扩大本轮范围。

本轮最终验证：build-test OFF/OFF完整构建成功；96项相关CTest全部通过、0失败/跳过，
58.69秒。新C ABI6例、Python adapter14方法、smoke17方法均通过。
独立审阅APPROVE；C11头/稳定常量/签名编译、Python编译、diff/本地链接检查通过。
LLVM17.0.6新测试全配置clang-tidy与默认format通过；核心diff范围clang-tidy通过，
范围外旧诊断未整改；旧include块保留，diff format关闭include重排。
初轮缺少直接cstdint依赖已补齐。一次CTest与目标重链接竞争，6例permission denied未执行，
保留该无效验证账；最终在完整构建结束后顺序重跑全部96项通过，不把未执行算通过。

## 6. 第三步：隔离RXE＋io_uring状态与报告实测

2026-11-19，基线 `5abc009`，仅验证已提交实现，没有新增生产代码。
143个源码/构建/绑定/工具/测试文件与宿主冻结SHA逐项一致；guest GCC13.3、Debug、
RDMA=ON/URING=ON完整重建成功，23项目标C++测试与Python14＋17方法全部通过，0跳过。
包含实际RXE loopback1例、内核io_uring5例、新旧scalar状态17例。
新库SHA256为 `402a5028e366747371245a145e41eec6ccafc5ac97fa15c356e9ca058666741d`，
新C符号已导出；server重建后的SHA与上一轮相同，不能把它描述成服务端行为变更。

legacy/lifetime各启动一个fresh slab服务（RAM关闭、disk ACK、buffered、uring开启）：

| 场景（每种策略各一例） | 实际结果 | 父报告 |
|---|---|---|
| 正常trace：8对象/4轮/4096B | 24计划/24尝试/24完成，12 PUT ACK＋12完整GET命中，写读各49152B | passed、rc0、已回收 |
| 首个预热GET前显式REMOVE | 实际C结果NOT_FOUND=1、长度0；get_miss | workload_failed、rc1、完整子账 |
| REMOVE后额外PUT4098B，GET容量4097B | 实际C结果INVALID=5、长度0；client_error | workload_failed、rc1、完整子账 |
| 首个预热GET前停止唯一owned服务 | 实际C结果IO_ERROR=4、长度0；client_error | workload_failed、rc1、完整子账 |

每个负例6计划，仅前2 PUT ACK与1 GET被尝试/完成，余3不执行；父报告准确保留3/3、
ACK8192B，不是invalid_result/未知，更不把miss或错误判成功。每次GET仅调用一次真实C符号。
负例以**证据目录内的诊断child**做受控注入，复用提交的run/TraceClient/父验收器；
不是修改公开smoke CLI或伪造后端结果。父报告记录实际child命令。
每种策略内四场景使用不同seed，两种策略复用同组seed；两种策略累计84计划、
66尝试/完成，trace PUT ACK36次/147456B。
额外注入的2次PUT/8196B及4次REMOVE单列，不混入trace分母；两台服务累计PUT38次/155652B。

每种策略PULL终局：正常uring/success_nonempty12、缺失uring/not_found1、
容量不足uring/success_nonempty1；其他路径/结果增量0。uring读/完成各13，无回退/残留在途/淘汰。
**容量不足的客户端失败不等于服务端授权失败**：按定稿spec第3节口径，MR/有效grant成立即可计热，
客户端收到READY后才因完整长度不足拒绝。因此lifetime服务端热度成功/应用各13、客户端命中12，
不是将失败客户端请求伪记为命中，也不改成仅凭RELEASE补热；legacy热度0。
停服后的GET没有活服务端可抓取最终计数，保留停服前快照，不声称服务端io_error增量或远端取消。

两台服务正常退出0、所有直接child回收、端口空闲；VM保留空闲，温控已正常结束。
有效monitor期间60次SMART：nvme2最高41°C、nvme0最高40°C、warning均0，最大间隔10.208秒。
全部VM资产与数据只由宿主disk2/3承载，继续禁止宿主/mnt/disk1压测。
源码/二进制、原始账/指标、独立审计、重建/测试及保留位置见
[冷盘说明第13节](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。

本轮未做压力、真实物理HCA、全量CTest、sanitizer或正式A/B，不依据约1秒或停服耗时比较性能。
下一小步应独立梳理批量GET状态传播契约，不能将当前单键证明扩大到SG/dedup/生产连接器。

## 7. 批量GET契约与第一小步：C++直接批量核心

基线 `a05b9be`。新增接口，无新结果struct/枚举：

```cpp
std::vector<Status> KVClient::BatchGetAutoWithStatus(
    const std::vector<KvGetItem>& items, std::vector<size_t>* out_lens);
```

### 兼容边界

- 返回恰好N项，按原输入顺序；仅kNotFound可计miss。optional out_lens正常返回时为N项，
  失败项长度0、仅Ok写完整长度；Ok＋长度0仍保留命中语义，不凭0推断miss。
- 这是**稳定的direct-only接口**：即使启用了opt-in dedup也不claim/publish/wait，
  与scalar状态接口一致；相比旧dedup接口可能产生额外真实I/O、观察到不同即时结果。
  不能宣称同等去重或性能；后续dedup状态设计不能悄悄改掉这一明确契约。
- 原公开BatchGetAuto保持bool结果及原Claim/Publish/Abort/Wait/回退流程；
  只将私有Direct改为共享状态核心的 `status == kOk` 投影，避免维护两套批量传输逻辑。
- TCP仍RunParallel逐项并行调用**私有**状态核心；RDMA仍按(node,cap)分组、
  ShardReadGroups分片及RunReadScheduled调度，不是串行调用公开scalar接口。
- 公开指标一次请求/N个提交key，命中数与字节仅来自最终Ok，不重复计逐项公开GET。
  输出buffer由调用方保证有效且并发项之间不重叠；失败buffer可能部分写入，必须丢弃。
- 空批次返回空status/长度；空key或非零容量配null为Invalid，合法sibling继续执行。
  无路由/冷却为IOError且无新I/O，不为本地拒绝额外MarkBad；容量不足为Invalid。
- status数量不等于整个shard请求数：整shard本地IOError，先拒绝再索引，不假推NotFound，
  不改变peer健康。status数量正确而长度数量不正确：原Ok项改本地IOError，
  明确NotFound/其他错误保留；坏Ok元数据本身不能MarkGood或MarkBad。
- 合法原Ok（包括完整长度可信但容量不足）或明确NotFound是已服务响应；
  同组有响应优先MarkGood，否则实际IOError才MarkBad，保留旧混合组规则。
  Invalid/resource/rail保持中立。既有无效参数/异常transport的安全修复不承诺旧版UB兼容。
- 输出status用非bitpacked vector，先设IOError；worker/scheduler吞掉的任务异常留下失败槽，
  不变成miss。**不承诺noexcept或统一异常状态**：现有TCP单worker路径可传播异常；
  公开入口先清外部长度，RAII仍记一次失败请求。异常向调用方传播时，整个调用无返回status，丢弃输出。

### 本轮验证与范围

13个新增用例覆盖8种混合状态/两类transport、参数/容量、status与长度缺项/多项、
坏Ok健康中立、顺序及128项多节点/容量/分片、TCP实际并行重叠、空批次、两种异常边界、
旧bool投影与指标，以及tiny真实TCP file服务。先取得新API缺失编译RED，再接入实现通过。
启用dedup后的绕过契约本轮为源码核查，没有新增共享内存fixture；不冒充dedup状态已贯通。

GCC13本机OFF/OFF完整构建成功；132项相关CTest全部通过，0失败/跳过，60.00秒。
LLVM17新测试全配置clang-tidy/默认format、核心diff范围clang-tidy/format通过，
范围外旧诊断未改；旧include顺序保留，diff format关闭include重排。独立代码审查APPROVE。

另在重新证明归属并启动fresh温控的既有冷盘VM中，133个生产/绑定/构建/新增测试文件SHA匹配，
完成ON相关目标（dfkv、server、新测试）重建，新13测试同样通过。
**不是整套ON测试二进制全部重新构建或全量CTest。**
真实RXE＋io_uring C++探针分别验证legacy/lifetime：每种策略先写63个16B对象，
新API一个64项混合批次返回62 Ok、1 NotFound、1 Invalid，再以旧bool接口读同64项并核验投影/字节。
共256逻辑GET、248命中/3968B读取、4明确miss、4容量错误；126 PUT/2016B。
每种策略客户端GET公开请求恰好2、keys128、hits124、bytes1984；服务端PULL128终局闭合，
uring成功grant126＋not_found2，读/完成各126，无回退/在途/淘汰；lifetime热度126，legacy0。
容量不足的有效grant不等于客户端hit，继续遵守spec第3节服务端成功口径。
探针不证明物理HCA、性能收益或高并发上限，启用dedup的探针路径明确关闭。

本轮证据：`.omx/evidence/batch-get-status/20261119T094456Z-a05b9be/`。
真实盘安全边界与保留路径见[冷盘说明第14节](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。
尚未新增**批量C ABI/Python状态接口**，旧C batch hit0仍不能当可信miss；
定长BatchGet、SG、dedup均未做新状态传播。下一小步再定义批量C ABI的调用级错误与逐项结果，
不能把本轮C++直接批量完成说成整个客户端栈或正式A/B已完成。

## 8. 第二小步：兼容的批量 C ABI 状态接口

基线 `934d595`，新增符号，不改变旧 batch、scalar、SG、wire 或 ABI version：

```c
int dfkv_batch_get_auto_status(
    dfkv_client_t c, const void* const* keys, const uint64_t* key_lens,
    void** ptrs, const uint64_t* caps, int n, int* out_status,
    uint64_t* out_len);
```

- 返回 **0** 表示已获得完整的 N 项结果，并非全命中。`out_status[i]` 使用现有
  `DFKV_GET_*` 固定值，仅 `NOT_FOUND` 是可信 miss；`OK` 长度 0 仍是命中。
  返回 **-1** 表示调用整体被拒绝或捕获到向外传播的异常，全部 buffer/result 应丢弃，
  不能计作逐项 miss/hit。不是事务：此前 I/O 不会回滚，也不保证远端取消。
- `n < 0` 不触碰任何输出。否则在参数校验前将提供的 N 项 status 置 `INVALID`、
  长度置 0。client 必须有效且非 null；`n == 0` 时所有数组均可 null，合法 client
  仍调用一次 C++ 空批次接口。`n > 0` 时 keys/key_lens/ptrs/caps/out_status 必填；
  `out_len` 可选，不要求为了接收状态而额外分配长度数组。
- 完成调用级参数校验后、任何分配或 I/O 前，将全部 status 置 `INTERNAL_ERROR`。
  C++ 完整返回、确认两组结果均为 N 项后才发布 C 输出；因此捕获的异常返回 -1，
  所有 status 保持 `INTERNAL_ERROR`、长度 0，即使部分 I/O 已完成。
  C++ executor 内部吞掉的任务异常仍可能正常返回 0＋逐项 `IO_ERROR`；不能混淆两种边界。
- 二进制 key/namespace 不变。空/null/长度溢出的 key、非零 cap 配 null 或容量溢出，
  只让对应项 `INVALID`，合法 sibling 继续读取；cap 0 配 null 是合法输入。
  无路由/peer 冷却为 `IO_ERROR`，未必有新网络 I/O；容量不足为 `INVALID`。
  失败 buffer 可能部分写入，必须丢弃；仅 OK 输出完整长度。
- 复用一个 `BatchGetAutoWithStatus` 调用，不循环调用公开 scalar，不把批量退化为串行。
  非法项用空 key 占位，使 C++ 公开指标仍记 N 个提交项/一次请求；调用级拒绝未进入 C++，
  不新增请求指标。稳定 C 状态转换与 scalar 共享显式 switch，不依赖内部 enum 布局。
- **direct-only**：即使用户开启 opt-in dedup 也绕过 claim/publish/wait，可能比旧
  dedup 接口产生额外 I/O，不承诺同等去重/性能。旧 `dfkv_batch_get_auto` bool 行为不变。
  所有输入/输出指针和句柄须有效并存活到调用结束；输出数组、输入描述符和并发目标 buffer
  不得重叠。C 无法校验指针实际可访问范围，非 null 不等于有效内存。

Python 本轮不绑定新符号；旧 Python batch 的 hit0 仍是未命中或失败，不能据此可靠计 miss。
SG/dedup 状态传播、真实服务新 C 符号的 RXE/io_uring 运行验证和正式 A/B 另行处理；
不把上一轮 C++ 探针当作本轮 C 符号实测，不修改生产默认配置。

### 本轮验证

- 新 API 未声明时先取得目标编译 RED；实现后新增8例通过，最后完整构建后再次执行8例，
  0失败/跳过。覆盖8种状态及旧bool投影、二进制key/零长度hit/optional长度、
  无效sibling隔离/N项指标、全局参数/负数/空批次、传播异常/worker吞异常、
  部分I/O完成后整体异常不发布部分成功、无路由/冷却，以及真实tiny file/TCP服务的
  hit/NotFound/容量不足/停服。64位宿主未运行32位size_t溢出分支。
- GCC13 RelWithDebInfo OFF/OFF完整构建成功；其后顺序执行140项相关CTest，
  全部通过，0失败/跳过，62.97秒。不是全量CTest，也不是本轮真实RDMA新C符号验证。
- LLVM17.0.6新测试全配置clang-tidy/默认format、生产diff范围tidy/format通过；
  旧范围外20条诊断未改。初次test tidy的unused `<utility>` 已移除并重验，失败日志保留。
  C11头文件/新旧函数签名/固定状态值编译通过；重建共享库的新旧符号存在，
  独立C程序动态链接后验证整体拒绝/负数计数契约通过；这不是网络负载测试。
- 独立审查未发现阻断问题。证据：
  `.omx/evidence/batch-cabi-status/20261119T101203Z-934d595/`；
  `commands.md`记录完整命令，`off-ctest.log/xml`、`final-direct.log/xml`为最终版本证据。
  未运行sanitizer、压力或正式A/B，未连接VM或对宿主`/mnt/disk1`施压。

## 9. 第三小步闭环：批量 C ABI → Python → 受监督报告

基线 `5e3b125`，仅验证工具接入；生产连接器、旧 `Client.read/read_outcome`、旧 C 符号、
SG/dedup、淘汰默认参数及线协议均不改变。

### Python 结果与兼容

`disk_scenarios.Client.read_outcomes(keys, expected)` 一次调用一个真实 batch，返回原序 N 个
`{outcome, optional status/error}`。新符号独立于 scalar 可选绑定：只有符号缺失才调用旧
batch，一次调用不进行 scalar/EXIST/重试。新符号继承direct-only契约，
即使开启opt-in dedup也绕过去重，可能比旧batch增加I/O，不宣称同等去重或性能。

- 新接口仅 `NOT_FOUND=1` 且长度0为 `get_miss`；OK=0须完整长度/字节匹配，长度0仍是hit。
- 错误/未知状态、非OK却长度非0、错误字节/长度为该项 `client_error`，保留数字状态，
  不丢弃合法 sibling。初始化 sentinel 防止未写入的状态槽被默认0误判为OK。
- C返回非0或异常为整批失败，不使用部分buffer推断结果；smoke记整个GET组为
  `get_batch_error`。可能已有I/O，无回滚/远端取消保证。
- 旧库的hit0仍为 `get_unresolved`，不能计明确miss；旧hit也要完整字节验证。
  空输入不发起native调用；向量长度、bytes/key、关闭句柄先校验。
- `TraceClient.execute_batch(ids)` 先校验全部ID唯一、GET且同namespace；共用原close锁，
  同一次native调用和所有backing buffer都结束后才能close。先验证完整返回形状再发布整组，
  缺项/坏项不会泄露“前几项已成功”的不完整结果。

### 显式批量模式与验收

原smoke默认scalar模型/顺序/报告保持不变。新增 `--batch-gets`，冻结config后选择独立
`namespace-batched-real-client-correctness-only` 模型及独立验收器。
GET连续窗口不跨PUT/phase，窗口内按namespace首次出现顺序分组；PUT仍单项。
这会改变不同namespace之间的实际读取顺序，**不是固定到达/性能回放**，原scheduled_ns只作身份。
初始objects=8时warmup确有两个4项组；measurement原轨迹交替PUT/GET，仍是singleton。

所有组和逻辑请求都保留在账中：同组结果全部收齐后再执行严格门槛，任何miss/unresolved/error
阻断后续组/阶段；同组合法hit仍保留，但不能据此宣布整轮通过。共享start/end只表示适配器
整组调用区间，不是各项网络延迟；`get_adapter_calls` 不冒充native调用或NIC并发数。
父验收器从冻结trace独立重建组计划，核对身份/组序/全组形状/共享时间/停止边界与计数。

批量summary与父报告新增 `get_outcomes`：planned/attempted/completed，及get_hit/get_miss/
get_unresolved/client_error/get_batch_error/not_attempted六类完整分母；另记GET适配器调用数及
整组失败调用数。失败调用数与受影响逻辑项数不能相加。completed表示适配器终局记录，
不证明远端操作全部终结。超时、崩溃、坏报告仍保留None/unknown，不能填0或伪计miss。
strict smoke仍要求所有PUT ACK/GET hit；明确miss是完整的失败账，不是验收放宽。

新增 `--tcp` 显式选择TCP并冻结 `DFKV_RDMA=0`，便于OFF库正确性复跑；原默认仍请求RDMA/rxe0，
不允许失败后自动回退。父子config/命令/环境一致，仍限制单个loopback服务、fresh输出、
8初始对象/4轮/4096B、精确子进程超时和回收，不连接生产服务。

### 复跑入口

在仓库根目录，已有专用scratch服务时（PORT替换为其TCP端口，输出路径必须不存在）：

```bash
python3 deploy/validation/eviction_trace.py generate /tmp/batch-trace-NEW.json \
  --objects 8 --rounds 2 --length 17
python3 deploy/validation/eviction_smoke.py /tmp/batch-trace-NEW.json /tmp/batch-result-NEW \
  --lib "$PWD/build-test/libdfkv.so" --members n=127.0.0.1:PORT \
  --batch-gets --tcp --process-timeout-sec 10
```

自动创建并清理tiny file/TCP服务的测试，无需准备服务或真实盘：

```bash
cmake --build build-test -j2
DFKV_BUILD="$PWD/build-test" DFKV_LIB="$PWD/build-test/libdfkv.so" \
  python3 -m unittest discover -s test/python -p test_eviction_batch.py
```

CMake将纯ledger/adapter测试单列 `python_eviction_batch`；真实ctypes三例并入原 `python_plugin`，
继承现有ASan/TSan对stock Python dlopen的排除，不假称已在sanitizer验证native路径。
缺构建产物/新符号时native测试明确skip；本轮提供当前OFF构建实际运行，不把skip当通过。

### 本轮实际证据

- 新method/optional binding缺失RED、批量报告module缺失RED均保留，随后通过。
  Python adapter23例（新增9）、原scalar smoke17例、新batch16例（纯13＋native3）全部通过，
  0失败/跳过；新batch独立复核同样16例通过。
- 当前GCC13 RelWithDebInfo OFF/OFF完整build入口成功（C++未改，CMake重新配置后无待编译项）。
  随后顺序142项相关CTest通过，0失败/跳过，68.19秒，包含新纯测试入口及原python_plugin中的
  三个native测试。不是全量CTest或本轮ON/RXE验证。
- 实际公共 `--tcp --batch-gets` 父子入口：20/20逻辑请求、10PUT ACK/170B、10GET hit/170B，
  4次GET适配器调用，warmup确有两个4项批次，父报告校验通过并回收子进程。
- 实际混合错误在**进程内run＋同一父验收器**验证，不冒充受监督负例子进程实测：
  首namespace的4项为 `[OK, NOT_FOUND, INVALID, OK]`，完整保留2hit/1miss/1error，
  下一namespace及measurement全部阻断。20计划/12尝试/12完成，24B trace ACK、6B hit，
  GET完整分母10=2hit＋1miss＋1error＋6未尝试。fixture额外2REMOVE＋1PUT/5B独立记录，
  不计trace写入；只是容量错误/丢失注入，不是淘汰收益证据。
- 真实旧bool路径通过屏蔽当前库的新符号验证，其hit0仍unresolved；不是旧发行二进制实测。
  真实停服返回逐项IO_ERROR4，不计miss。整批rc失败/异常、未知状态及坏报告为mock/受控子进程测试，
  不冒充真实网络故障注入全部覆盖。
- 初次OFF库配原默认RDMA=1得到预期setup failure，推动显式TCP选项；失败账保留。
  测试自身None哨兵fixture修复、临时TCP bootstrap的历史green也保留，不把它当最终公共入口证明。
  最终公共入口验证使用未mock的--tcp命令。所有owned tiny服务回收、临时数据清理；未使用VM/真实盘压力，
  宿主`/mnt/disk1`继续禁止施压。
- Python语法编译、CMake配置、diff及文档链接检查通过；本机无ruff/pyflakes/pylint，
  未声称通过这些lint/typecheck，也未运行sanitizer。新C++无改动，不伪报本轮clang-tidy结果。

证据 `.omx/evidence/python-batch-status/20261119T105424Z-5e3b125/`：
`off-ctest.log/xml`、`report-green.log`、`binding-green.log`、`native/guarded-positive`和
`native/mixed-failure`保留实际报告。SG/dedup状态传播、新Python模式RXE/uring实测及正式A/B仍后续处理。
