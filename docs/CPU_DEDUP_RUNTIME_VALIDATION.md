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
