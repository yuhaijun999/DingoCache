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
