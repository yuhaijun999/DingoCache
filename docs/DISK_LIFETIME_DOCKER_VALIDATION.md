# 磁盘淘汰：Docker 隔离诊断与复跑

本文对应开发分支的**合成数据功能诊断**，不是原规格 §11.1 的正式业务 A/B 放行。
用户已授权创建隔离 Docker、使用三块盘的专用目录，并解除本轮 RXE/io_uring 后置。
不改生产默认、不部署、不清理其他数据。真实模型的前缀 token、重算/TTFT、物理 HCA
性能仍不能由这些合成样本证明。

## 1. 为什么容器内还需要虚拟机

本机宿主是 EL8 4.18，实际 `io_uring_setup` 返回 ENOSYS；已有 `/dev/kvm` API=12。
普通 Docker 共用宿主内核，仅换 Ubuntu 不能解决。采用 Ubuntu 22.04 容器包装 QEMU，
启动 Ubuntu 24.04 的独立内核；只透传已有 `/dev/kvm`，不用 privileged、host network、
宿主 RDMA 模块、sysctl 或宿主块设备。RXE 两端均在 guest 内，SSH 转发不是 RDMA 证据。

参考：[Docker 内核边界](https://docs.docker.com/learn/content/what-is-a-container/)、
[QEMU 网络](https://www.qemu.org/docs/master/system/devices/net.html)、
[rdma-core 软件 RDMA](https://github.com/linux-rdma/rdma-core#usage)、
[Canonical cloud-init](https://ubuntu.com/docs/public-images/public-images-how-to/use-local-cloud-init-ds/)。

## 2. 目录和权限边界

每次使用全新的 `RUN_ID`，数据只放：

```text
/mnt/disk1/dfkv-validation/<RUN_ID>/   # base、guest.qcow2、seed/SSH key、data.raw、结果
/mnt/disk2/dfkv-validation/<RUN_ID>/   # data.raw
/mnt/disk3/dfkv-validation/<RUN_ID>/   # data.raw
```

各目录有 `.dfkv-validation-owned` 标记。虚拟数据盘是三个独立宿主挂载点上的文件，
每个虚拟盘 64 GiB；guest 系统盘 32 GiB 为稀疏 qcow2，实际占用另记，不是预占总量。初次8GiB盘已在guest闲置时安全扩容至16/64GiB并保留日志。
不把 `/dev/nvme*` 交给容器/VM。格式化命令只在 guest 内对 `virtio-dfkv-data1/2/3`
执行，先核对 root serial=`dfkv-root`。不要在宿主执行 guest 格式化脚本。

## 3. 建立环境（从仓库根；无须改宿主内核）

逐步可复制命令见[建立与复跑配方](DISK_LIFETIME_DOCKER_SETUP.md)，包括本轮 VM 快速复用和全新 RUN_ID 建立。

```bash
docker build -t dfkv-validation-vm:ubuntu22.04 \
  -f deploy/validation/Dockerfile deploy/validation
```

镜像 pin Ubuntu 22.04 digest；apt 包版本会随仓库更新，保存 image ID 和 `dpkg-query`
输出，不把重建称为逐字节可复现。备份目录的旧发布脚本关闭测试并写 dist，不用于本轮。

创建三个专用目录并分别 bind 为 `/disk1/2/3`，源码只读 bind 为 `/src`。容器使用：
`--device /dev/kvm --security-opt label=disable --memory 12g --cpus 8`，不发布宿主端口。
若用宿主 UID/GID 运行，先在容器内创建同 UID 的用户，否则 OpenSSH 会拒绝未知 UID。
`label=disable` 不重标记源码/宿主数据目录。没有 KVM 时可用 TCG 做功能尝试，但不报性能。

从 Canonical 下载 `noble/current/noble-server-cloudimg-amd64.img` 与同目录 `SHA256SUMS`，
核对**准确文件名的 SHA256**再启动。保留已验证 base；`current` URL 不是不可变版本。
中断下载必须续传并最终核 SHA；不能把退出 0、文件长度或某一分块当完整校验。

容器内生成无密码临时 SSH key，cloud-init 用户 `validator` 仅授权该公钥、禁用密码，
并由 `cloud-localds` 生成 seed。密钥仅放本轮 seed 目录，不提交、不输出私钥。创建：

```bash
# 容器内；noble-base.img 已完整校验。
qemu-img create -f qcow2 -F qcow2 -b /disk1/noble-base.img /disk1/guest.qcow2
qemu-img resize /disk1/guest.qcow2 32G
for i in 1 2 3; do qemu-img create -f raw /disk$i/data.raw 64G; done
```

QEMU 使用 `q35,accel=kvm`、`-cpu host -m 8192 -smp 6`，所有 drive 用 `aio=threads`
（宿主不要求 io_uring）。root drive 用 `if=none,id=root`，再附
`virtio-blk-pci,drive=root,serial=dfkv-root,bootindex=1`；三个 data drive 分别明确 serial
`dfkv-data1/2/3`。**不能省 root bootindex**，否则 firmware 可能先尝试空数据盘。
管理网用 `user,id=net0,hostfwd=tcp:127.0.0.1:2222-:22` 与 virtio-net；seed 只读 CDROM。
控制台、pid 和 monitor socket 均放 `/disk1`。保存完整 docker/QEMU 参数和 cloud image SHA。

SSH 在容器内连 `validator@127.0.0.1:2222`；known_hosts 也放本轮 seed 目录。
等待 `cloud-init status --wait`，guest 内安装：

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  cmake ninja-build g++ git ca-certificates libibverbs-dev liburing-dev \
  rdma-core ibverbs-providers ibverbs-utils iproute2 python3 python3-numpy \
  python3-psutil python3-pytest pkg-config linux-modules-extra-$(uname -r)
```

只在 guest 内格式化/挂载带上述 serial 的新盘到 `/data/disk1/2/3`，写 volume 标记，
然后 `sudo modprobe rdma_rxe; sudo rdma link add rxe0 type rxe netdev lo`。
记录 `uname -a`、`rdma link show`、`ibv_devinfo -d rxe0`；要求 PORT_ACTIVE。
还要实际调用 `io_uring_setup`，随后执行下面五个真实 ring 用例，不以版本号猜测可用。

## 4. 编译及分层验证

guest编译产物面向Ubuntu24/glibc环境，仅用于诊断，不拿来替换Ubuntu22/EL8发版包。
把本次实际源码快照复制至 guest `/opt/dfkv`，保存逐文件 SHA 和 Git 基线；不要复制
宿主旧 build 作为新 guest 产物。网络可用时 CMake 自取 GoogleTest；离线时用已核对的
GoogleTest 源码并显式传 `FETCHCONTENT_SOURCE_DIR_GOOGLETEST`，记录该源码 SHA。

```bash
cmake -S /opt/dfkv -B /opt/dfkv-build -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DDFKV_BUILD_TESTS=ON \
  -DDFKV_WITH_RDMA=ON -DDFKV_WITH_URING=ON
cmake --build /opt/dfkv-build -j4
sudo bash /opt/dfkv/deploy/validation/guest_checks.sh \
  /opt/dfkv-build "/data/disk1/results/check-$(date -u +%Y%m%dT%H%M%S)-$$"
```

另建 OFF/OFF 构建，保留无 RDMA/uring 时可编译及 CPU 回归。各配置使用独立 TMPDIR、
同一测试 UID（本轮 guest 内用 sudo/root），结果目录须可写；不要将 root 创建的固定
/tmp fixture 交给另一个 UID 清理。CMake/CTest 即使报告退出 0 也须核对新 XML 的完整
条数/失败/跳过；不得读取上一轮旧 XML。新工具的无硬件单测：

```bash
python3 -m unittest discover -s test/python -p test_disk_validation.py -v
ctest --test-dir build-test -R '^python_disk_validation$' --output-on-failure
```

`guest_checks.sh` 要求新证据目录，拒绝复用旧 XML，并对全量发现条数进行核验。
它要求 RXE probe=1、真实 ring=5、指定 PULL Serve=9，全部实际执行且
零 skip/失败；XML 使用显式失败检查，`PYTHONOPTIMIZE=1` 不能跳过。全量 CTest 则逐项
记录 skip，不把 CUDA/其他环境缺失算通过。外层 timeout/非零退出也不能称通过。

| 层级 | 可以证明 | 不能替代 |
|---|---|---|
| 无硬件工具单测 | 生成器、字节/长度/缺失 oracle、trace 与 XML 校验的反例 | 真 C ABI/RDMA |
| CPU allocator/Store/Node | 评分/预算/identity/恢复与故障轨迹 | 实际 Serve/内核 ring |
| PULL Serve 9 | 真 RXE/QP/Serve；异步故障例使用可控 CQE | 真内核磁盘读 |
| UringReader 5 | 真内核 ring/read/completion | 应用端到端 RDMA+uring |
| 外部 C ABI + server | 实际 transport、三盘与成功 bytes | 未实际参与的路径/业务 TTFT |

## 5. 合成样本与应用层运行

固定 seed `0xD1A60001`，两个含 NUL 的 binary namespace，相同 logical key；SHAKE256
按 seed/namespace/key/generation/length 分离，生成偏移变化的字节。工具先写 trace/hash，
再连客户端；不把预期值当实际计数。并发到达顺序不保证确定，因此输入trace/hash
不等于服务端精确执行序；错误记录带key/index/长度与expected/actual SHA256，不输出原值。C ABI GET 的容量不足也可能报告 miss，所以已知
缺失同时核 metadata EXIST；故障与 miss 的 ABI 歧义仍需联核服务健康/错误指标。

服务显式设置真实 slab/lifetime（legacy 对照须显式 legacy），RAM write-back 必须
显式 disk ACK。三目录依次为 `/data/disk1/<case>,/data/disk2/<case>,/data/disk3/<case>`。
无驱逐混合尺寸基线总容量48GiB、每盘16个1GiB extent；设置 G=1000000/B=64 供
M=1/2/4/63 的合法覆盖，并在此基线设置 `DFKV_SLAB_RECLAIM_MS=0`、核对实际驱逐为0。
这是两个活跃尺寸档×8条非满extent轮转目标所需的余量，不是 MAX_READS+1链数；
依据 `SlabAllocator::kStripeWays=8`，不是用payload容量猜测。压力用例仍开启后台。
原12GiB/每盘4个、24GiB/每盘8个 extent 的紧凑
几何保留为 donor 重绑诊断：混合档并发申请可能合法驱逐，不能单凭总占用低就断言
所有对象都应命中，或把已失去盘副本的 RAM 读 race_lost 当成盘锁接受后漏热。
case 必须独立初态，不复用上一 case 的缓存热度；初态和 ACK 相同才具可比性。

```bash
PYTHONPATH=/opt/dfkv/integration/common/src DFKV_RDMA=1 DFKV_RDMA_DEV=rxe0 \
  python3 /opt/dfkv/deploy/validation/disk_scenarios.py \
  --lib /opt/dfkv-build/libdfkv.so --members n=127.0.0.1:12001 \
  --output "/data/disk1/results/client-$(date -u +%Y%m%dT%H%M%S)-$$" --scenario roundtrip --readers 8
```

`roundtrip` 验证九个长度边界、同 key 跨 tenant、keep-first、Remove 后重写及稳定并发读；
`write/verify` 使用相同确定性 dataset 做 clean restart；dirty restart 按原冷重置合同。
`pressure --count N` 的 N 是**每 namespace**数，两 namespace 合计 2N。默认 1 MiB
slot 的单尺寸压力用12 GiB容量约12288个最小档槽；可按 0.75/1.05/2 倍配置数量，但按实际盘路由/
class 分配核对，不能只按 payload 字节判断压力。压力后允许 miss，不允许错 bytes；
必须有成功读取且联核服务健康、实际驱逐/终局账，不能把所有错误都称合法驱逐。

实际 io_uring 应用路径需 metrics 的 `dfkv_uring_reads_total`、completions 增量为正，
`dfkv_uring_init_fallbacks_total` 增量为 0，且真实 PULL/transport 有参与。
`DFKV_SERVER_URING=1`、编译 ON 或受控 CQE 单测都不够。RAM 命中场景不强求磁盘读，
应单列其通知资格与分母。按 case 归档原日志、XML、resolved config、trace/hash 和 metrics。

## 6. 诊断阈值与结果边界

事前建议：错 bytes/长度/namespace/代际、pin 复用、错 ACK、意外崩溃/sanitizer 为零容忍；
声称真实 ring 时 fallback 必须为 0，必测 case 不许 skip。建议待处理请求 10 秒无成功则报警、30 秒抓现场后终止。
这两项是待实现的进度 watchdog 建议，当前脚本仅有 120/300 秒整体 timeout，不冒充已实现。donor fallback 在至少100次选择中超过1%、
配对诊断 p99>1.25倍/吞吐<0.8倍、重复周期后 drained RSS>1.2倍仅是**调查建议**，
不是正式放行数字，也不自动启动性能比较。首次 no-fault/below-capacity 错误就保留现场。

本轮实测结果登记在下方第 8 节，不能把其中的已通过子集解释为完整验收通过。
正式业务阈值仍按[登记模板](DISK_LIFETIME_THRESHOLDS_TEMPLATE.md)独立冻结，不能拿已看过
的探索数据事后调门槛。物理 HCA/GPU、真实推理业务与完整性能/资源放行保持明确未验收。

## 7. 清理

先正常关闭 guest 服务，保存结果到宿主证据目录；再正常关闭 VM、停止并删除本轮带
`dfkv.validation.run=<RUN_ID>` 标签且 ID 匹配的容器。核对路径和 ownership 标记后，
才删除三个**完整、精确 RUN_ID 子目录**；不要删除 `/mnt/diskX` 或共享父目录，不运行
Docker prune。停止容器不会自动删除 bind 文件；删除某 run 的 backing image 前，确认
没有其他 VM 使用它。源码、其他容器/镜像及原有磁盘数据不清理。

## 8. 本轮实际结果（2026-11-17）

Git 基线 `d69e126dfefe399247cdbec2360604ff19c2ea0a`，本轮只改诊断工具、CTest 注册和文档，
未改生产 C++。容器 `dfkv-validation-20261117t035613z-d69e126`；guest Ubuntu 24、
内核 `6.8.0-147-generic`，RXE `rxe0`；ON/OFF 均从源码重新构建成功。
镜像 ID、下载 SHA、源文件 SHA、软件包版本、命令/日志及失败试验均保留在本地证据中。

| 项目 | 实测结果 / 边界 |
|---|---|
| 新工具无硬件单测 | 9/9，通过；含容量不足假 miss、错误 bytes、XML 失败/skip/旧结果等反例 |
| 真 RXE / 真 kernel ring / PULL Serve | 1/1、5/5、9/9，全部零 skip；Serve 异步 CQE 受控，与真 kernel ring 分开证明 |
| 全量 RDMA=ON、URING=ON CTest | **1645 通过、2 失败、41 跳过，总数 1688** |
| 全量 OFF/OFF CTest | 1401 通过、0 失败、34 跳过，总数 1435 |
| 应用端到端矩阵 | 36/36；M=1/2/4/63，RAM off/write-around/write-back，coalesce 0/1、sync、六个压力组合、legacy、direct |
| M 饱和与恢复 | 6/6；四个 M 各 1260 次真实成功读，队列数分别 2/3/5/64，18 个对象均到顶档；clean 三次重启校验 17 个存活对象/删除/代际；SIGKILL 后三盘冷重置、18 个对象确认为缺失 |
| direct 合并偏移补充 | PreparedRead.CoalescedRangeDirectHeadShiftByteExact，1/1 实际执行 |

独立应用 smoke：三个盘对象数 7/7/4；实际 kernel reads/completions=94/94、
ring fallback=0，PULL dispatch/terminal=98/98（94 非空、4 missing），热度通知/应用=94/94。
矩阵不只看客户端退出码，还核对三盘参与、服务健康、错误/终局、ring 与适用读分母。
RAM 命中读不能虚报成物理磁盘读。不同 case 数据目录参与一致性哈希路由，因此这里是
功能诊断，不是配对性能 A/B。应用压力数量指每 namespace，二者合计后计算占槽压力。

### 8.1 首轮发现的真实失败（后续修复见第9节）

`RdmaLoopback.RealFifoQueuedConnectionCloseSync`、
`RdmaLoopback.RealFifoQueuedConnectionCloseUring` 均未在 10 秒内回收连接
（ActiveConns=1，期望0），各额外重复两轮仍失败；不是 skip，也没有通过延长等待掩盖。
初步静态定位是等待 CQ/在途 SEND 的短轮询可能绕过 elapsed idle budget；这仍是待验证假设，
不能仅凭 RXE 特定行为断言根因。下一小步建议先加状态快照与无 hook 的 stalled-SEND 回归，
再最小修复连接关闭/空闲超时，并重跑两个配置及 QP/pin 生命周期回归。
**该首轮诊断不是全绿。后续修复结果单列于第9节，仍不构成 O5/O12、翻默认或部署放行。**

### 8.2 失败试验与覆盖缺口

- 原 12/24 GiB 混合尺寸基线出现 donor 重绑/失去盘副本；原始失败保留在
  `app-matrix`、`app-matrix-wide`，未删除或换名为通过。24 GiB 的一个 keep-first
  用例曾有同长度 bytes 不符，但当时没有实际值摘要，且存在驱逐，不能确定是数据损坏
  还是基线不再具备原住户前提；应保留为未定诊断。新工具已补 expected/actual SHA256。
  最终无驱逐基线按两档×8个非满 extent 预留 48 GiB，并硬核对实际驱逐为0，36项通过。
- 初次 OFF 用不同 UID 访问 root fixture、缺 pytest 的失败记录仍保留；修正环境后才计入上表。
- SIGKILL 只证明进程崩溃合同，不是断电持久化；有限合成样本不等于穷尽故障/并发调度。
- 没有物理 HCA/GPU/真实推理业务、性能 AB 或本轮硬件路径 sanitizer 通过记录。
  合成 TTFT/重算不能替代正式业务证据；调查阈值未用于倒推放行结论。

### 8.3 证据、保留与复跑

本轮 RUN_ID=`20261117t035613z-d69e126`。仓库证据目录：
`.omx/evidence/container-validation/20261117t035613z-d69e126/`，含执行日志、run.json、
版本/SHA、矩阵和恢复 supervisor 脚本；它们不是默认 Git 交付文件。
宿主 disk1 的 `results-complete.tgz` 保存 guest `/data/disk1/results` 全部可读结果，
其中 `hardware-final/ctest.xml`、`cpu-final.xml`、`app-matrix-final/summary.json`、
`lifecycle/summary.json` 是上述最终结果入口；旧失败试验同包保存。

三个宿主目录均为 `/mnt/diskN/dfkv-validation/20261117t035613z-d69e126/`（N=1/2/3）。
64 GiB 是每个 data.raw 的虚拟容量；实际占用见证据 `disk-usage.txt`。
为复跑保留镜像/数据和**仍运行的空闲 VM**；应用服务和测试进程已退出，不自动删测试盘。
默认256MiB接收分块在长时运行后的guest曾出现RXE高阶页分配ENOMEM，启动按合同失败；
保留日志后，配方显式用8MiB分块复跑成功，94次实际读/计热、三盘及终局均核对。
这只改复跑配置，不追溯改变原矩阵条件，也未改宿主内存设置。
本轮操作已删除42个完整SHA校验后的下载临时分块，保留可复跑镜像/样本。
`evidence-complete.tgz` 保存supervisor/本轮日志/SHA，不含SSH私钥。
私钥仅保留 disk1/seed，未提交或打入结果证据包。复跑选配方 A+D，用新的 case 目录；
全量矩阵 supervisor 及原命令保存在证据包，固定目录须改为新的 run/case，不能覆写旧结果。
清理按第7节先关闭 VM，再核 ID/标签/ownership 后仅删除本轮三个子目录。

## 9. 后续小迭代：修复 RDMA 空闲期限绕过（2026-11-17）

基线 `c15682f`，只修复第8.1节连接回收，不修改磁盘评分/参数、协议或重试次数。
原两项失败再次复现；原 FIFO 的失败现场为 held=0、queued=0、free=0，uring 两次
reply posted 仅一次 SEND fence，说明 B 已出队、回复 SEND 尚未完成。同步测试的 CQ
重排钩子和生产异步路径分别使短轮询绕过空闲期限；生产同步路径有 pending receive
时也存在相同的无限豁免。

修复只在空 verbs CQ 等待后，用已有 `last_active_us_` 判断实际经过的空闲期限。
继续使用 **RDMA CQ 活动**时间，不声称磁盘 CQ 会重置此时钟；不在磁盘读 in-flight
阶段新增超时，不改变 ring drain、QP先销毁再释放 MR/pin 的顺序。idle<=0仍关闭回收。

新增四项不使用 CQ 钩子的真实 RXE 回归：不投递对端接收缓冲，使回复 SEND 真正
停在 RNR；覆盖 sync/uring 的200ms回收、lease归零/新连接，以及 idle=0 时保持连接
并在补接收后收到原回复。修复前六项组合为3通过/3失败；修复后最终7项（含原FIFO
两项及keepalive）连续三轮通过，零skip。仅guest内临时禁用uring验证不支持环境：
sync两项通过、uring两项显式skip，随后恢复原值0；这不是uring路径通过证据。

最终 ON/OFF 源码构建成功。最终 XML 对齐 CTest discovery：ON总1692，**1651通过、
0失败、41跳过**；OFF总1435，**1401通过、0失败、34跳过**。跳过主要涉及缺少的
GPU/多rail/etcd等条件，不算通过。生产文件的差异行 clang-tidy 无新增诊断、exit0；
大测试文件的全检查超300秒预算后停止，只有编译诊断及有限检查完成，不能宣称其
全检查已通过。差异行 clang-format 和 diff whitespace 检查通过。

证据：仓库 `.omx/evidence/rdma-idle-close/20261117t072000z-c15682f/`，包括 red/green、
最终全量XML、原始日志、逐文件SHA和独立审查。实际构建目录为guest `/opt/dfkv-build`
和 `/opt/dfkv-cpu`；新结果目录由每份日志的 `E=` 指明。复跑使用建立配方A进入同一
私有VM，在guest以同一UID/独立TMPDIR执行：

```bash
cmake --build /opt/dfkv-build -j4
DFKV_RDMA_DEV=rxe0 DFKV_RDMA_RECV_CHUNK_BYTES=8388608   /opt/dfkv-build/rdma_loopback_test   --gtest_filter='RdmaLoopback.StalledReplyIdle*:RdmaLoopback.RealFifoQueuedConnectionClose*:RdmaLoopback.KeepalivePreservesIdleDataConnection'
ctest --test-dir /opt/dfkv-build --output-on-failure --timeout 120
ctest --test-dir /opt/dfkv-cpu --output-on-failure --timeout 120
```

本轮未重新跑全部36+6合成矩阵，不能将首轮矩阵追溯为新版本实跑；也未新增物理HCA、
GPU、硬件路径sanitizer或正式性能AB通过记录。本次缺陷收敛完成，不关闭正式业务门禁。


## 10. 修复后端到端复跑：36＋6 场景（2026-11-17）

源码基线 `d5025f3`。本轮没有修改生产代码、参数默认值或测试预期；在同一私有
Ubuntu guest（kernel 6.8.0-147、RXE rxe0、io_uring 可用）中重新执行全部合成功能
场景，而非沿用第8节历史结果。135个构建/运行相关源码文件的 SHA256 与工作树一致，
`cmake --build /opt/dfkv-build -j4` 成功（Ninja 无待编译工作，不冒充重新编译）。

| 本轮实际执行 | 结果 |
|---|---|
| 36组应用矩阵 | 36通过、0失败、0跳过；24个 M×RAM×coalesce、4同步、6压力、legacy、direct |
| 6组生命周期 | 6通过、0失败、0跳过；M=1/2/4/63各1260次读、clean三次重启、dirty SIGKILL重启 |
| 额外结果审计 | 外层退出0；36个服务退出0；PULL终局与缺失分母一致；25份生命周期快照健康、无错误/fallback/遗留在途 |
| 审计器反例 | 5/5正确拒绝：外层非零、服务非零、缺失零值指标、隐藏IO终局、错误场景集合；不是产品功能测试数 |

四种 M 的全局队列数分别2/3/5/64，18个住户均到顶档；clean校验17个存活对象、
两个更新代际和一个删除对象，恢复虚拟读不计实际热度。dirty重启三盘冷重置，
18次缺失均为 `not_found` 终局，不以 C ABI 的 miss 独自推断无 I/O 错误。
无驱逐基线保持实际驱逐0；所有成功读仍核对预期字节。六组压力的实测如下，数量
按每个 namespace 计，两个 namespace 合计施压；缺失属于预期驱逐，不代表错误字节可接受。

| 压力场景 | 驱逐数 | 命中 / 缺失 |
|---|---:|---:|
| M2，每namespace 4608对象 | 0 | 9216 / 0 |
| M2，每namespace 6452对象 | 2092 | 10812 / 2092 |
| M2，每namespace 12288对象 | 13764 | 10812 / 13764 |
| M1/M4/M63，各每namespace 12288对象 | 各13764 | 各10812 / 13764 |

使用全新目录，不覆盖原失败记录。沿用已验证的显式8MiB RXE接收分块，避免长期
运行guest的高阶页分配问题；这是诊断环境配置，不改变生产默认。生命周期 supervisor
只额外保存每阶段退出前的指标快照，原有正确性断言没有放宽。不同case目录参与路由，
因此结果不能当作严格配对性能 A/B。退出后无应用服务/本轮测试进程，guest的uring设置仍为0。

### 10.1 命令与证据

同一root身份、memlock unlimited，在配方A的私有guest内顺序执行：

```bash
ulimit -l unlimited
cmake --build /opt/dfkv-build -j4
PYTHONUNBUFFERED=1 python3 /opt/dfkv-rerun/matrix.py
PYTHONUNBUFFERED=1 python3 /opt/dfkv-rerun/lifecycle.py
```

这两个脚本是本轮证据副本，不是安装产物；再次执行必须先按新 RUN_ID 改写结果及
三个盘的case路径，否则其拒绝复用目录。完整脚本、逐case参数/环境、客户端字节校验
结果、原始metrics、源SHA、执行命令、外层退出码和审计器保存在：
`.omx/evidence/post-fix-matrix/20261117t075643z-d5025f3/`。
`results-summary.json` 为汇总入口，`run.log`/`run.exit` 为真实执行结果，
`guest-results.tgz` 为原始结果包，`audit.log`/`audit-negative.log` 为结果审计。
这些本地证据通常不随Git分发；缺少证据包不能仅凭本文确认通过。

新落盘数据仍限于原授权的三个宿主专用目录：
`/mnt/diskN/dfkv-validation/20261117t035613z-d69e126/`（N=1/2/3）的虚拟盘文件内。
guest新case路径为 `/data/diskN/cases/{matrix,lifecycle}-20261117t075643z-d5025f3/`，
结果在 `/data/disk1/results/` 下的同名目录；不直接操作宿主块设备。保留样本和空闲VM
供复跑，各guest数据盘目前约剩16GiB，下一轮应先检查空间，不能无限新增完整矩阵。

本轮没有重新执行全量CTest或sanitizer；第9节的ON/OFF结果仍为同代码历史证据。
也未覆盖物理HCA/GPU、真实业务TTFT、正式性能A/B或断电持久化。修复后的合成功能
回归已完成，**不据此关闭O5正式验收、翻转默认或部署**。
