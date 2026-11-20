# CPU dedup 新链路：隔离 RXE/io_uring 验证与自动复跑

基线 `0471f59`，2026-11-20，仍是 **O5 / AB7 前置正确性验证**，不是正式A/B。
本轮只增加验证工具、测试及文档；不修改生产算法、默认配置、wire、Status或shm v4格式。
宿主 `/mnt/disk1` / `/dev/nvme1n1` 继续禁压，默认rootful Docker的根目录在该盘，不能回退使用。

## 1. 本轮交付与可信边界

| 文件 | 用途 |
|---|---|
| `test/client/cpu_dedup_runtime_test.cc` | 默认自建小型file/TCP服务；可显式接入隔离RXE服务。测试专用委托Transport在CPU claim后门控，再调用真正后端；不新增生产hook |
| `deploy/validation/cpu_dedup_validation.py` | live/stopped固定C ABI→Python探针，单次实际调用期间捕获状态/长度/完整字节，父进程独立验收 |
| `deploy/validation/cpu_dedup_boundary.py` | 固定客户端子进程，保持同一handle跨越停服，用ready/ack协议验证TTL本地命中和从未读取key的IO失败；不接受PID/任意命令，不停止服务 |
| `deploy/validation/run_cpu_dedup_validation.py` | guest-only两策略完整矩阵；只创建/回收自己的Popen服务及客户端，核验固定server oracle、XML、报告和build SHA |
| 两个新Python测试模块 | 探针14方法＋runner/boundary10方法；无真实盘/服务的纯测试 |
| `run_sg_validation.guest_paths` | 复用既有guest三独立挂载检查；新增cpu前缀，默认sg不变，lexists同时拒绝已有路径和悬空symlink |

native在同一进程内使用两个独立client与POSIX共享内存、真正传输后端；
不是两个独立OS进程在实际RXE上的waiter调度证明，旧NodeDedup fork单测是另一类证据。

三个native场景在C ABI上验证：
1. owner已claim且正在等待真实读取，waiter也已claim等待；放行owner后，waiter使用完整发布数据，
   不为shared对象再次发远程读。marker对象独立读取，门控不用sleep或生产测试钩子。
2. 明确注入owner**本地**IO错误，随后waiter超时回退并真正远程读取成功。这不是网络故障注入证明。
3. owner与waiter真正回源读取缺失对象，均保留NotFound，不把其他错误猜成miss。

门控只改变测试时序，实际成功/NF都委托已选中的Transport；外部模式必须实际为rdma。
默认本机TCP也是实际服务，不是fake成功。外部selector只允许一个显式loopback成员，namespace tag合法有界。

## 2. 两策略矩阵与固定分母

fresh legacy、fresh lifetime各使用三块guest数据文件系统、12GiB slab、buffered、RAM off、
disk-ACK、reclaim50ms、rxe0/depth4/recv8MiB/maxpayload及block2MiB、server io_uring。
顺序：native3 → live探针 → 原有完整trace报告 → boundary暖读 → 保存指标 → 停自己的服务并reap0
→ ack客户端 → 同handle本地命中/冷keyIO → fresh stopped探针。两策略间检查端口空闲。

| 每策略组成 | PUT/字节 | GET终局 |
|---|---:|---|
| native3 | 5 / 320B | 9项：6hit/384B、2NF、1注入本地IO |
| live Python探针 | 3 / 192B | 6项：4hit/256B、1NF、1容量Invalid |
| 原trace的C ABI→Python→父报告 | 10 / 170B | 10项全hit；CPU观察8fetch＋2READY hit |
| same-handle停服边界 | 1 / 64B | 3项：暖读hit、本地hit、never-read IO |
| fresh stopped探针 | 0 | 2项IO，不是miss；第二项可能在peer冷却中本地拒绝 |
| 合计 | **19 / 746B** | **30项：22hit/938B、3NF、5error** |

两策略共 **98操作＝38PUT/1492B＋60GET**，44hit/1876B、6NF、10error。
错误在固定诊断中是预声明预期，diagnostic passed不替代业务全ACK/全hit门槛。
所有原生/探针分母完整保留；请求失败、半批失败、未知状态、坏字节和进程未知不冒充miss。

每策略服务端独立oracle：**20PULL＝17uring有效grant＋3NF；ring read/completion17；
L lifetime-read-success17，G0；存储健康1、error/fallback/inflight/eviction0。**
XML只能证明受控测试执行，不能由它的属性自行提供server oracle；runner独立冻结3个名字和期望。
Client hit22不等于server grant17：每策略6个本地成功、1个容量失败仍获有效grant。
不改原规格server成功计热点，不把本地cache hit加到磁盘热度。

## 3. 停服与TTL边界

boundary先实际PUT/暖读，原handle保持打开；父持有服务Popen并正常停止/reap0后才写ack。
同handle暖key仍可通过CPU共享内存命中，never-read key必须为IO4，不能为NF1。
CPU诊断TTL固定30秒，安全观测窗口小于25秒；本次两策略warm→本地返回分别0.184/0.245秒。
这是**CPU共享缓存TTL**，不是磁盘180秒新写保护或KEEP，不调整磁盘评分参数。
边界子进程总预算30秒，warm-ready10秒，等待stop-ack20秒；超时/异常只TERM/KILL/reap自己的子进程。
多项CPU wait仍是逐项有界等待，不是一个batch总deadline；原生调用无取消/回滚承诺。

父冻结库SHA、token、实际namespace与完整客户端环境，严格核验JSON类型，False不能冒充状态0。
new live探针证明跨namespace同binary key的数据隔离/READY复用；native门控另证明在途waiter。
不能把自然调度下的READY hit计作IPC等待命中，不能把注入本地IO称为真实断网故障。

## 4. 复跑方式

### 本地快速单测（不用RDMA设备）

```bash
cmake --build build-test -j2
ctest --test-dir build-test \
  -R '^(CpuDedupRuntime|python_cpu_dedup_runtime$|python_sg_validation$)' \
  --output-on-failure
```

本地不要设置 `DFKV_CPU_DEDUP_RUNTIME_MEMBERS/NAMESPACE`，native测试会自建/清理临时file/TCP服务。
3native测试由CMake glob收集；24个新纯Python方法进入python_cpu_dedup_runtime，旧SG14同样回归。
本轮无新生产Cpp，无需新生产tidy；新增native测试执行LLVM17完整format/tidy。

### 已核验的可丢弃guest内

先在宿主重新证明**当前**QEMU PID/starttime/UID/netns和六个FD对应的backing dev/inode，确保全在
允许的disk2/3；启动独立SMART watchdog（仅nvme2/nvme0，65°C/非零warning/无效样本停止，
监控身份和样本0≤age<25秒），再经受监控入口执行。guest marker不能替代宿主proof/温控。
不复用历史PID/monitor.stop/旧tag，不用默认Docker daemon，不读写旧disk1镜像。

在guest中预先有ON/ON构建和rxe0/uring可用条件，按自己的新tag执行（最长55字符，留policy suffix）：

```bash
sudo -n bash -c 'ulimit -l unlimited; exec python3 /opt/dfkv/deploy/validation/run_cpu_dedup_validation.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases --data-root /data/disk2/cases --data-root /data/disk3/cases \
  --output /data/disk1/results/cpu-runtime-NEW_TAG --tag NEW_TAG'
```

memlock仅调整此guest进程及后代，不修改宿主limit/sysctl。guest `/data/disk1` 实际backing在**宿主disk2**，
不是被禁止的宿主`/mnt/disk1`。输出及case必须fresh，不覆盖、不删除旧数据。
runner不做宿主环境配置、mount、全局kill、共享数据清理或旧数据复用。

## 5. 实测证据与资源位置

源码基线0471f59：guest396已提交源文件SHA核验，GCC13.3 Debug ON/ON完整319步构建通过400.88秒；
9个工作文件覆盖后组合402文件SHA前后全部一致。新lib
`e92c6482ed4f74d818502fb0ce472fd5d245b2a4f3aeeb1a4c56bf97f226d878`，server仍
`280bd55b880db94ceeefb9a9e7d7c376ef9d5bb9fc1c83895a1b533e5675bf54`。
两策略矩阵第一次实际执行全部符合固定预期（25.303秒，非性能指标）；6个native case实际rdma且无skip。
宿主OFF完整build＋189相关CTest全pass/no skip77.98秒；ON目标3个TCP case、TSan3项×3轮9执行通过。
Guest88Cpp（含actualRXE1/kerneluring5）、126Python方法全pass/no skip。非全量CTest、非HCA/GPU或性能放行。

fresh宿主六FD/backing与after proof通过；83个SMART样本最高nvme2 42°C/nvme0 41°C，warning全0，
max gap10.421秒；两个服务正常exit0，客户端回收、端口空闲，归档后fresh样本再正常停止watchdog，VM留空闲。
没有实际注入热故障/监控失活，不声称该故障路径被动态证明。

- 证据：`.omx/evidence/cpu-dedup-runtime/20261120-0471f59/`，含commands、原账/XML、SHA、失败尝试与独立审查。
- 宿主新增产物：`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/cpu-dedup-20261120T072802Z-0471f59/`。
- guest结果：`/data/disk1/results/cpu-runtime-20261120T072802Z-0471f59/`、`cpu-tests-20261120T072802Z-0471f59/`。
- guest六个数据目录：`/data/disk{1,2,3}/cases/cpu-20261120T072802Z-0471f59-{legacy,lifetime}`。
- native fixture只在确认dev/inode仍一致时unlink自己的测试共享段；Python探针/trace/boundary无独占创建证明，
  不自动unlink，精确possible路径在各report/config中，重复单测也有少量保留段。不能按全局前缀删除其他客户端数据。

数据/results保留。尤其不要删除包含活动VM镜像的disk2/3父目录；只按精确本轮子目录处理。
首次同步脚本误用了不存在的/opt/dfkv-src，失败后查询CMake确认/opt/dfkv再同步，日志保留；
runner首轮mock PID不可JSON序列化属fixture问题，修正后38纯测通过，未放宽runtime oracle。
当前仍需GPU dedup、正式A/B的外部输入/独立职责/数值门槛与运行；生产默认配置未变。

## 6. 独立 OS 进程扩展（基线5f7b276）

新增 `test/client/cpu_dedup_process_runtime_test.cc`：用 `posix_spawn` **exec 同一测试二进制的固定 helper 模式**，
每个子进程重新创建 client/transport，不继承父进程的连接/RDMA上下文。
默认仍为实际自建 TCP/file 服务；外部模式要求实际 RDMA。原第1–5节的同进程证据保留。

父通过双向 pipe 依次确认 owner claim 后到达真实读取前的 gate，再确认 waiter 的 marker 已到达 gate；
后者发生在完整批次 claim pass 之后。只有此后才放行 owner/终止 owner，再放行 waiter。
没有用 sleep 猜测是否进入等待；dead-owner 后为满足既有5000ms takeover年龄的等待另行标注。

| 新场景 | PUT/字节 | 真实远程读 | GET逻辑终局 |
|---|---:|---:|---|
| 正常 owner 发布、跨进程 waiter 复用 | 2/128B | shared1＋marker1 | 3hit/192B，wait_hit1 |
| owner显式本地IO注入，waiter自己回源 | 2/128B | marker1＋shared回源1 | 2hit/128B＋1IO；不称网络故障 |
| claim后SIGKILL owner，waiter超时回源；新进程接管并READY再读 | 2/128B | marker1＋waiter回源1＋recovery1 | 4hit/256B＋1被终止未返回项，不能猜成IO或NF |
| 新suite合计 | **6/384B** | **7grant，无NF** | **11项：9hit/576B、1IO、1未返回** |

waiter的无token回源不发布，之后新的claim才能接管旧FETCHING；recovery首次远程读取后第二次为READY。
测试核验完整binary字节、C ABI逐项状态/长度、CPU counters及委托远程读次数，而非只看环境开关。
被终止项计入已发起GET分母，不伪造返回状态、payload或完成时刻；诊断符合预期不等于全业务成功。

父独占持有spawn返回的子PID，正常等待或精确SIGKILL后waitpid回收；不扫描前缀杀进程。
子在创建client前设置parent-death SIGKILL并复核父PID，避免测试父超时退出后留下活客户端。
调度pipe的源FD使用CLOEXEC，只显式dup两个childFD；helper在创建client前关闭其他继承FD，
避免持有父测试服务的监听socket。不从活client fork后继续使用继承状态。
共享段fresh检查及相同UID/dev/inode复核后清理，且先回收子进程；不按全局shm前缀删除。

### 6.1 自动复跑与独立验收

现有 guest runner 增加 **opt-in `--cross-process`**，不指定时原矩阵及分母不变。
打开后须有 `cpu_dedup_process_runtime_test`；原native3之后运行新suite3，再执行原probe/trace/TTL/停服链路。
新suite父进程预算45秒，失败/超时写独立report并回收自己持有的Popen；失败停止后续策略。
XML必须三项精确名字/类名/run/completed/no skip，固定gate/exec/计数/cleanup属性，
实际父PID匹配Popen，owner/waiter/recovery PID合法且互不相同。不能由XML自己决定验收期望。
新二进制同server/lib一起做运行前后SHA核验。

每策略固定总数为 **25PUT/1130B、41GET（31hit/1514B、3NF、6error、1被终止未返回）**，66操作；
两策略 **132项已发起操作＝50PUT/2260B＋82GET（62hit/3028B、6NF、12error、2未返回）**，
其中130项完成返回；被终止的2项不算已完成。
每策略服务端 **27PULL＝24uring有效grant＋3NF，ring24，L计热24/G0**；
client hit与grant仍不相等，不改变磁盘计热点。全场景是正确性诊断，不是正式A/B或性能测试。

本地新增测试入口（原CMake glob自动发现）：

```bash
cmake --build build-test -j2
ctest --test-dir build-test \
  -R '^(CpuDedupProcessRuntime|CpuDedupRuntime|python_cpu_dedup_runtime$)' \
  --output-on-failure
```

完成宿主 backing/SMART 安全前置证明后，在原guest命令末尾增加 `--cross-process` 即可；
仍使用新tag/fresh输出及三盘case，不复用历史目录。不应把此选项加到未满足前置条件的宿主命令。

GPU dedup仍只做[兼容契约梳理](GPU_DEDUP_STATUS_CONTRACT.md)，没有实现新GPU入口或执行硬件验证。

### 6.2 本轮实测与保留位置（2026-11-20）

冻结3个runtime源文件后第一次实际扩展矩阵通过，外层监控入口耗时36.154秒（含SSH/采集，非性能指标）。
两策略各原native3＋新跨进程3均实际RDMA/no skip；25PUT/1130B、27PULL/24grant/3NF的独立oracle全部匹配，
两个owner按计划SIGKILL并reap，其余子进程及两个服务正常回收。逻辑分母符合第6.1节，未返回不冒充错误。
`process-native-report.json` 仅在exit0且严格XML通过后填写 `logical_counts`；失败/超时/坏XML保持null。
同handle停服边界warm→local为0.1841/0.1847秒，仍在CPU TTL安全窗口内。

- 宿主OFF完整build入口通过，最终 **192相关CTest通过，0失败/跳过，79.19秒**；不是全量CTest。
- ON目标重新构建及3个默认真实TCP用例通过；独立Debug TSan目标3轮/9项执行通过，
  不等于跨进程共享内存竞态完全证明。
- Guest复核旧402源文件、库/server SHA，再增量同步并构建新目标，最终403组合源文件SHA前后相同；
  不是一次新的clean/full重编译。新测试binary SHA为
  `9686d928a20b469deb8bf1cfc3025444a56a94c572add39a919d39f468f874e5`；
  lib仍e92c648...、server仍280bd55...，生产源码未变。
- Guest **93 C++＋136 Python方法通过，0失败/跳过**；含真实RXE1/kerneluring5及GPU无硬件控制测试2。
  GPU的2项只核对名字/开关，不证明CUDA复制/IPC或未来状态接口。
- 新C++ LLVM17完整format/tidy通过；两个Python文件compile/AST、diff及文档链接检查通过。
  没有外部Python lint/typecheck工具，不将py_compile冒充它们。
- 宿主fresh六FD/start/UID/netns/backing前后证明均通过；72个SMART样本最高nvme2 **41°C**、
  nvme0 **41°C**，warning全0，max gap10.219秒。监视PID2469977/start379623958正常停止/reap0；
  归档后端口空闲、无测试二进制/服务遗留，VM留空闲。未实际注入热故障/监控失活/父进程总超时；
  总超时报告路径为mock测试，owner被终止及恢复是实际动态测试。

证据 `.omx/evidence/cpu-dedup-process/20261120-5f7b276/` 包含命令、原始XML/报告/metrics、
85条目安全归档、SHA、审查、早期失败尝试；第一次OFF build与并行worker重链接冲突ETXTBSY，
保留原日志，后续串行冻结构建/测试通过。tidy的header/const诊断及继承监听FD审查修复都在首次真实矩阵前完成。

新增落盘位置：
- HOST `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/cpu-process-20261120T081159Z-5f7b276/`。
- Guest `/data/disk1/results/cpu-process-20261120T081159Z-5f7b276/` 和
  `/data/disk1/results/cpu-process-tests-20261120T081159Z-5f7b276/`。
- Guest六个fresh数据目录 `/data/disk{1,2,3}/cases/cpu-20261120T081159Z-5f7b276-{legacy,lifetime}`。
- 新native段仅在UID/dev/inode复核后删除；原Pythonprobe/trace/boundary无独占创建证明，仍保留报告中的精确possible shm路径。

不得删除包含活动VM镜像的HOST父目录，不得按shm前缀全局删除；HOST `/mnt/disk1` 继续禁压。
本轮没有修改生产API、默认或评分；GPU状态实现、正式A/B及默认切换仍待后续独立安排。
