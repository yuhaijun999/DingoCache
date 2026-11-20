# 显式 CPU dedup：C ABI → Python → 验证报告

本轮基线 `239831a`，属于 **O5 / AB7 前置正确性验证**，不启动正式 A/B、不改变生产默认值。
`/mnt/disk1` 继续禁止压测；本轮仅小型 TCP/file 测试，没有 VM/Docker 或真实盘压力。

## 1. 接口与兼容边界

- C++：上轮新增 `BatchGetAutoDeduplicatedWithStatus`，本轮不改该实现。
- C：新增 `dfkv_batch_get_auto_deduplicated_status`，参数/状态码与直读
  `dfkv_batch_get_auto_status` 相同。共用小型参数校验/异常/结果发布 helper，两个符号
  分别显式选择 direct 与 CPU dedup；旧直读符号不受 dedup 开关改变。
- 验证用 Python：新增 `disk_scenarios.Client.read_deduplicated_outcomes`；缺少新符号
  或调用级失败就失败，**不回退直读、旧 bool 或 EXIST**。原 `read_outcomes` 及其旧库
  symbol-absent fallback 保持原样。不是所有公共 connector 的 dedup 集成。
- `TraceClient` 只在显式 token 模式下对批量 GET 调用新方法；PUT 和原 trace ID/载荷不变。

新C/Python入口仍沿用C++在CPU开关关闭或初始化失败时的direct降级契约；
“不回退”指Python不悄悄改用其他C符号。严格冒烟的启用观察阻止把这种原生降级当作CPU验证通过。

调用返回0代表N项状态可用，不代表全部命中。只有明确 `NOT_FOUND=1` 是 miss；
未知状态、容量不足、I/O、错误字节/长度保持逐项错误，合法 sibling 结果保留。
调用返回-1则整批作废，不采信任何部分状态/缓冲，无回滚或取消承诺。
CPU 共享内存仍只存成功 payload，不广播失败；waiter有界等待后保留自己回源的真实状态。
多项等待可累积，不是batch总截止；TTL内可能在服务停止后命中本地成功数据，Ok不证明
此次服务端仍可达。GPU/SG去重及旧bool不改，无wire/Status/shm版本修改。

## 2. 显式报告模式

`eviction_smoke.py --batch-gets --cpu-dedup` 才启用该模式；缺少 `--batch-gets` 拒绝。
`--tcp` 可显式选择TCP；不指定仍请求RXE/rxe0，但本轮没有实际RXE去重运行证据。
默认直读命令、环境、报告模型和判定规则保留。

新模式为 `namespace-batched-cpu-dedup-correctness-only`，不是到达率回放或性能模型。
父进程生成一次性随机token，冻结配置/库SHA/子命令；不能由父CLI指定token重放。
子环境清除外部DFKV变量后固定：CPU开关1、arena1MiB、slots1024、wait20ms、
takeover2000ms、TTL5000ms、miss-retries0。后两项只是本次诊断值，不是生产建议。
每个原namespace映射到带token的新后端namespace，并在原账记录两者。
**映射会改变tenant identity、对象身份及路由，不是原正式A/B trace的原路由复跑。**

父进程按key_map.h/v4算法计算UID作用域的shm路径，发现已有文件或悬空symlink就拒绝；
这只是precheck，不是原子独占创建，也不是共享段创建者所有权证明。
ctypes快照签名与其他函数一样在load_library时绑定一次，不在各handle并发调用期间反复修改。
每个handle在任何请求前读取有界完整 `dfkv_stats_snapshot`，必须观察到四个精确的
CPU dedup counters（hits/fetches/wait_hits/wait_timeouts），否则setup失败：
**不能仅凭开关或新函数存在就宣称功能启用**。每个handle前快照全0，后快照原样保留。

父验收器独立重建namespace映射、shm名、分组、完整请求分母、终局状态、时序和summary；
检查快照精确字段、整数类型、非负有界、基线0及单调性。计数不得超过该namespace已完成
GET数；本有界host-buffer/小对象/少key的全hit冒烟还要求计数和等于GET分母，避免实际
走直读却声称CPU路径。**这不是通用NodeDedup不变量**：device、cap0、arena/索引无法
承接等unclaimed路径可不计这些counter，不能把此规则直接推广到生产或任意trace。

本轮真实Python冒烟每namespace一个handle，证明启用和READY成功数据复用，
不是本轮新增的跨进程在途waiter合并证明；确定性等待者回归仍由既有C++用例覆盖。

`cpu_dedup_observations` 是进程本地rendezvous观察，不是网络请求数、服务器grant或计热。
缺失snapshot的已知失败保留unknown观察；超时、崩溃、伪造报告保持观测计数unknown、计划分母不变，
不能伪造0失败。错误/miss仍使严格业务冒烟失败，不放宽旧全ACK/全hit门槛。

## 3. 最简单的复跑

在本工作树根目录，使用已有OFF构建：

```bash
cmake --build build-test -j2
ctest --test-dir build-test \
  -R '^(CApiBatchGetAutoDedupStatus|python_eviction_dedup$|python_plugin$)' \
  --output-on-failure
```

`python_plugin` 包含新真实 C ABI/Python/guarded-report 测试，由测试创建临时小型file/TCP
服务，自动等待、停止并回收；它也包含原有其他原生客户端测试。纯报告入口为
`python_eviction_dedup`，sanitizer中仍运行；原生ctypes入口沿用stock-Python sanitizer排除。
全部新测试应实际执行，无硬件skip不能当作通过。构建目录不同就替换 `build-test`。

仅当你已有**专用可丢弃的本地测试服务**时，可直接调用：

```bash
python3 deploy/validation/eviction_trace.py generate /tmp/NEW-cpu-trace.json \
  --objects 8 --rounds 2 --length 17
python3 deploy/validation/eviction_smoke.py /tmp/NEW-cpu-trace.json /tmp/NEW-cpu-report \
  --lib "$PWD/build-test/libdfkv.so" --members n1=127.0.0.1:12000 \
  --tcp --batch-gets --cpu-dedup --process-timeout-sec 10
```

每次换NEW文件/目录；不复用服务端生产目录，不自动创建/清空服务，不压真实盘。
父进程只控制自己的单个客户端子进程；原生调用本身不可取消，超时不能宣称服务端回滚。

## 4. 产物、保留段与清理边界

输出含canonical trace、child-result/report、stdout/stderr。父report列出
`possible_retained_shm_paths`；子原账列出原namespace、后端namespace、shm name和前后计数。
没有独占创建者证明，程序**不自动unlink共享段**，`shm_cleanup`明确标注原因。
本有界trace最多两段，每段arena1MiB＋64KiB索引＋header（合计约2.13MiB），在 `/dev/shm`
内存文件系统，不落 `/mnt/disk1`。setup早失败时列出的路径可能尚未创建，不能说全部已创建。
可重复运行会累积这些段；需要清理时应核对对应report以及所有附着进程已退出，再逐项处理，
不要按全局前缀批量删除，也不要unlink其他客户端的段。本轮测试/证据保留路径单独登记。

实际RXE/uring CPU去重、物理HCA/GPU、全量CTest、性能与正式A/B另行；不把TCP或fake
观察外推成上述证据。正式A/B外部输入/独立职责/数值门槛仍未放行。

## 5. 本轮实测记录（2026-11-20）

- GCC13 OFF/OFF完整构建通过；最终185相关CTest全部通过、无失败/跳过，74.75秒，非全量CTest。
- 最终Python四模块78方法全部通过、无跳过（31 binding/adapter＋17原smoke＋16原batch＋14新CPU模块）；
  新CPU模块为12纯测试＋2真实TCP方法，原生入口未被错误地作为纯测试或硬件证据。
- ON/ON目标重编译通过，同22项fake/TCP通过；不是完整ON构建或实际RXE/uring去重证明。
- TSan OFF/OFF重编译，22项×3轮＝66执行通过；不等于跨进程共享内存竞态完全被证明。
- C11签名/固定状态数值/动态新旧符号与边界检查通过；LLVM17新Cpp测试完整format/tidy及
  生产diff格式/修改行tidy通过。旧/系统诊断过滤，非全仓clean；Python编译/AST通过，
  Ruff/Pyflakes/Pylint未安装，未声称外部lint/typecheck通过。独立审查16文件、0问题。
- 冻结源码真实TCP二次审计：guarded20/20操作，10GET全hit，观察到8fetch＋2READY hit，
  4次GET adapter call；mixed状态[0,1,5]，服务停止后未读过的key返回IO4、不是miss。
  父客户端子进程已回收，临时TCP服务/目录由fixture清理。shared-memory数据按第4节保留。

证据 `.omx/evidence/cpu-dedup-chain/20261120-239831a/`：commands.md、最终XML/日志、
source.sha256、review.md、native-audit-frozen.json及native-results-frozen原始报告。
finalaudit精确三段路径/每段1114144字节列于该JSON；重复测试另有保留段，
shm-observed-inventory.json只列当前同UID可观察文件元数据，**不推断所有权或删除权限**。
缺失接口RED、metrics绑定RED、静态夹具诊断均保留；首次新模块16测试包含3个导入旧用例的
重复收集，已修正，最终新模块明确14项。未用重复收集虚增覆盖，也未放宽严格成功门槛。

## 6. 后续隔离实测与自动复跑

本链路已通过实际RXE＋io_uring两策略验证，补充受控在途owner/waiter、失败回源、
namespace隔离、停服后的同handle TTL与冷keyIO边界；新增可重复guest矩阵和独立验收。
完整分母、server grant与client hit差异、宿主安全条件、资源保留和实测局限见
[CPU运行时验证说明](CPU_DEDUP_RUNTIME_VALIDATION.md)。不是正式A/B或生产默认切换依据。
