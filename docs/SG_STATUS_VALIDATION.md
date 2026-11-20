# SG 状态链路：隔离验证与复跑

2026-11-20，生产代码基线 `f35df95`。本轮新增验证工具，不改生产客户端、服务端、
wire、磁盘格式或默认配置；属于 **O5 / AB7 前置准备**，不是正式 A/B。

## 1. 两个入口的作用

- `deploy/validation/sg_validation.py`：针对一个**已有、自有的 loopback 服务**，冻结库SHA、
  namespace、逐项计划及传输配置，在独立子进程调用真实SG C ABI；父进程独立重建计划，
  校验完整状态、长度、重构字节及完整分母。仅为验证用ctypes，不是公共Python SG绑定。
- `deploy/validation/run_sg_validation.py`：**仅限现有可丢弃验证guest**，顺序启动legacy/lifetime
  两个fresh三虚拟盘slab服务，分别跑live→保存指标→停止自有服务→stopped，验收指标并回收进程。
  固定12GiB逻辑几何、RAM off、disk ACK、buffered、rxe0、uring；没有淘汰压力或性能结论。
- `test/python/test_sg_validation.py`：9个纯测试＋1个真实tiny TCP测试。
  `test/python/test_sg_validation_runner.py`：14个纯测试，覆盖路径/挂载隔离、两策略排序、
  停服先回收、指标失败、构建SHA变化、TERM/KILL/不可回收失败等。

新工具的 `passed` 指**预先冻结的诊断预期全部匹配**，包括刻意制造的Invalid、NotFound、
旧接口unresolved及停服IO_ERROR；绝不代表这些业务读取成功。既有smoke的严格全ACK/hit门槛未改。

## 2. 冻结的 RDMA 矩阵

每策略先PUT 4个二进制key/确定性带NUL payload，必须全部ACK，共321B。
然后6次SG C调用、17个逻辑GET，输入/目的内存保持至同步调用返回：

| 调用 | 描述 | 预期原序结果 |
|---|---|---|
| binary | 64B分到3/0/61B段，零段空指针 | status `[0]`，完整字节 |
| mixed | 命中、缺失、容量63B不足、合法sibling | `[0,1,5,0]`，两命中完整字节 |
| many | 129个**非空**1B段，重构129B | `[0]`；捕获runtime宽度并要求 `<129` |
| zero | 非空对象无段/零容量段、缺失对象无段 | `[5,5,5]`，RDMA本地拒绝，不能证明miss |
| invalid | 负段数、缺嵌套数组、容量溢出、合法sibling | `[5,5,5,0]` |
| legacy | 当前库的旧SG C符号，命中/缺失/不足/命中 | bool `[1,0,0,1]`；两个0保留unresolved |

live：7个hit/513B、1个明确miss、7个错误、2个旧bool unresolved；所有17项均尝试/完成。
旧接口仅验证当前库的兼容符号，不是旧发行二进制。验证子进程显式冻结
`DFKV_GET_MISS_RETRIES=0`，避免诊断中的旧接口重试改变分母；生产默认值未修改。

自有服务正常退出后，再以同tag创建新client，一次4项新SG调用必须返回 `[4,4,4,4]`，
无PUT、无miss。IO_ERROR可以含连接拒绝或本地冷却，不宣称每项都发起了新网络I/O。
整次C调用rc失败时不采信逐项输出；超时、崩溃、坏报告的计数为unknown/None，不伪造0或miss。
计划项全部保留，异常后的未尝试项明确标识。

### TCP 的既有零容量限制

RDMA whole-object容量0在 `RdmaTransport::PullInto` 本地Invalid，连缺失对象也不查询。
TCP长度0表示读whole-object；非空回复在零容量接收解码处被拒绝，现有实现返回IO_ERROR，
**不是可靠的容量Invalid或miss**。描述符合法不保证所有传输能完成零容量读取。

因此显式 `--tcp` 的zero正常组是三个缺失对象，预期 `[1,1,1]`；另外在**全部正常调用之后**
单列非空零容量诊断，冻结 `[4,4]`，避免冷却干扰后续用例。TCP live为19GET/7SG调用：
7hit/513B、4miss、2unresolved、6error；与RDMA分母不同，不能当成同负载A/B。
此边界及首次错误预期的失败账保留；没有修改生产行为，也没有把它藏成成功读取。
129段在TCP只能证明fallback/scatter正确，不证明RDMA单WR布局或零拷贝。

## 3. 复跑前置：禁止直接在宿主机执行矩阵

1. 先按[冷盘说明](DISK_LIFETIME_COOL_DISK_VALIDATION.md)重新证明VM的PID/start/UID/netns、
   六个真实FD到backing dev/inode/size，及宿主挂载设备；**禁止宿主 `/mnt/disk1` / nvme1n1**。
   只允许宿主disk2/nvme2n1与disk3/nvme0n1。本轮guest `/data/disk1` 实际backing在宿主disk2。
2. 使用新的宿主disk2专属结果目录和fresh温控监控，SMART仅nvme2/nvme0；诊断停止线65℃，
   任意warning非0、非法/超时样本或样本失活停止。watch校验精确monitor身份，样本年龄在[0,25)秒；
   异常时只处理重新证明的自有VM/子进程，不使用默认Docker、pkill或全局清理。
3. 同步当前源码并逐项核验SHA；确认ON/ON构建、新SG符号、guest rxe0 ACTIVE、uring可用、
   三guest数据挂载及端口12000/12001/12010空闲。宿主本身无真实RDMA，不在宿主装驱动或改sysctl。
4. guest进程必须有足够memlock（可用guest sudo，仅调当前进程限制），所有路径/tag新建且不复用。
   `run_sg_validation.py` 会拒绝无guest标记、非三独立挂载、跨嵌套挂载、已有数据/结果目录。
   **guest标记不是宿主安全证明，矩阵不会代替宿主温控/watchdog。**

当前guest内示例（已同步源码和构建后；每次新TAG）：

```bash
cd /opt/dfkv
TAG="$(date -u +%Y%m%dT%H%M%SZ)-sg"
sudo -n bash -c 'ulimit -l unlimited && exec python3 deploy/validation/run_sg_validation.py "$@"' -- \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases --data-root /data/disk2/cases --data-root /data/disk3/cases \
  --output "/data/disk1/results/sg-runtime-$TAG" --tag "$TAG"
```

该命令须由宿主fresh watchdog包围；不得盲目重用历史watch中的PID/start/tag。
矩阵每个client子进程20秒预算，超时精确TERM/KILL/reap；服务readiness30秒、TERM15秒/KILL后3秒。
OS不可中断调用/不可回收均不能宣称通过；宿主watch提供外层边界。只停止自身Popen服务，不停止任意PID。
报告写在输出目录 `matrix.json`；每策略包含live/stopped的config、父report、child-result、stdout/stderr、
server.log及before/before-stop.prom。停服后不捏造server指标。数据不自动删除，禁止删VM backing父目录。

只验证已有自有TCP服务时，可在其他目录调用绝对脚本路径：

```bash
python3 /path/to/DingoCache/deploy/validation/sg_validation.py \
  --lib /path/to/build/libdfkv.so --members n=127.0.0.1:PORT \
  --tag fresh-tag --output /new/exclusive/result-directory --tcp
```

端点必须显式loopback，无DNS/远端目标；live仅4PUT/321B，stopped仅读且由调用方先停自有服务。
既有数据不会删除，fresh tag隔离namespace，不允许重复执行live覆盖同名结果目录。

## 4. 本轮实测与范围

- 源码：`f35df95`的388个已提交文件＋5项工作树覆盖（CMake及四个新Python文件），
  guest结束392项组合SHA全部匹配。完整GCC13.3 Debug ON/ON 315步构建通过400.56秒；
  5覆盖SHA/增量重配置通过。lib SHA `4ac19c8248b4782c8660b79e1a19cfc2e5f552b4e90274df2b19d364dd0955b2`；
  server仍为 `280bd55b880db94ceeefb9a9e7d7c376ef9d5bb9fc1c83895a1b533e5675bf54`。
  17个既有cache/transport测试告警保留，不声称warning-clean。
- guest **63项C++0失败/跳过**，包括19个SG新接口测试、实际RXE loopback及5个kernel uring测试；
  **80个Python方法0失败/跳过**（新工具10＋runner14＋既有23/17/16）。这些Python native单测使用TCP，
  实际新SG RXE证据来自下面四组矩阵，不从TCP单测推断。
- 两策略各live/stopped四组诊断均匹配预期，约6.03秒，实际transport均rdma、runtime width=29。
  两策略合计50个计划/尝试/完成逻辑操作＝8PUT ACK/642B＋42GET；GET为14hit/1026B、2miss、
  14Invalid、4legacy unresolved、8停服IO_ERROR。14次SG native调用，整次rc失败0，wrong-bytes0。
  没有删分母或将错误当miss。129非空段超过29宽度，证明有序scatter字节正确，不证明单WR打包。
- 每策略server PULL11＝9个uring有效grant＋2NF，ring read/completion各9；健康1、error/fallback/
  inflight/eviction0。lifetime read_success/applied均9，legacy均0；9grant≠7hit，因为两个容量不足
  读取仍有有效grant，维持spec计热口径，不改为客户端消费成功。
- 两服务正常exit0、4探针child回收，端口空闲，无遗留runner；fresh结束FD证明、392SHA检查通过。
  42个结果archive条目安全校验提取；归档后fresh温控样本再正常停止监控，VM留空闲。
  94个SMART样本nvme2最高42℃/nvme0最高41℃，warning全0，最大间隔10.219秒。
  没有触发真实过热或监控失活，不冒充这些故障注入证明。
- 宿主OFF完整build入口成功（本轮无C++待编译）；**162相关CTest0失败/跳过72.03秒**。
  新24方法单独通过，独立纯测试23/23。四Python文件syntax/AST、diff及独立审查通过；
  Ruff/Pyflakes/Pylint缺失，不声称外部lint/typecheck通过。没有C++改动，不伪报新clang检查。

初版验收器曾允许completed setup携带被忽略的error/未知字段，已通过实际RED补严格字段回归；
noPUT的stopped-open失败账也已修正。初次测试greeting/TCP模式字符串假设错误、TCP零容量预期错误、
0项discovery（不是RED）及真正missing-module RED均分开保留，不归咎生产算法。

证据：`.omx/evidence/sg-runtime/20261120T034220Z-f35df95/`，含完整命令、source SHA、build/unit日志、
四组父子报告/原始字节、metrics、进程/端口/温控与失败尝试；落盘位置见[冷盘说明第16节](DISK_LIFETIME_COOL_DISK_VALIDATION.md)。
本轮不证明物理HCA/GPU、性能或淘汰效果，不包括公共Python SG、dedup状态、全量CTest、sanitizer、
正式A/B或默认切换。单机两宿主盘承载三虚拟盘，不是三块独立物理盘。
