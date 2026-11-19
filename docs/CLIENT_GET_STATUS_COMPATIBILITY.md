# 真实 GET 状态接口：兼容方案与第一小步

基线 `752d1eb`，2026-11-19。属于 **O5 / AB7前置准备**，不是正式A/B放行。
本轮只实现C++单键buffer接口；C ABI、Python和批量去重状态仍待后续小步接入。

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
