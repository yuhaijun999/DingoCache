# 冷盘隔离验证：排除 disk1 的环境与复跑边界

> **本机当前任务禁止向 `/mnt/disk1` 施加测试负载。**
> 用户报告 `/dev/nvme1` 温度告警；系统盘、backing image、数据、日志和结果均绕开它。
> 只使用 `/mnt/disk2`、`/mnt/disk3` 的专用测试目录，不透传或写入宿主裸设备。
> 此限制持续有效，除非用户明确变更；不能因稍后温度下降自动恢复 disk1 测试。

本文记录2026-11-18建立的冷盘环境。它不是正式性能A/B或O5整体验收报告。
生产评分、校验、P180及 staged legacy 策略默认均不改变。

## 1. 为什么不能只换应用数据目录

宿主原 Docker 数据根是 `/mnt/disk1/docker`，旧VM的系统盘、backing和结果也在disk1。
因此不在原daemon下新建容器/构建镜像，也不从旧盘大规模复制或清理样本。
旧VM已正常关机，旧容器 `dfkv-validation-20261117t035613z-d69e126` 已停止，样本保留。
不修改原daemon配置，不停止其他容器，不执行全局Docker prune。

本轮使用独立 **rootless Docker 26.1.3 + RootlessKit/slirp4netns + vfs**。
配置、data-root、exec-root、socket、临时目录和日志均使用disk2专用路径。
不安装常驻systemd服务，不修改宿主模块、sysctl、iptables或物理宿主网络。
直接启动rootless daemon及独立目录的官方说明见
[Docker rootless tips](https://docs.docker.com/engine/security/rootless/tips/)和
[dockerd多实例边界](https://docs.docker.com/reference/cli/dockerd/#run-multiple-daemons)。

宿主为cgroup v1，本轮rootless容器**没有可宣称生效的CPU/内存硬限制**。
QEMU的 `-m 8192 -smp 6` 配置guest内存/vCPU，不等于宿主进程硬资源上限。
不能照搬原rootful环境“8CPU/12GiB容器限额”的结论，见
[rootless资源限制](https://docs.docker.com/engine/security/rootless/tips/#limiting-resources)。

## 2. 实际落盘映射

本轮标识：`20261118t063414z-c9bbdca`，源码基线 `c9bbdca`。

| 内容 | 宿主专用路径 |
|---|---|
| Docker数据、base、guest.qcow2、seed、控制台与结果 | `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/` |
| 独立daemon运行目录/socket | `/mnt/disk2/dfkv-run-063755-3532361/` |
| 初次试启遗留运行目录（停用、不复用） | `/mnt/disk2/dfkv-run-063639-3522037/` |
| guest `/data/disk1`，64GiB虚拟盘 | disk2专用目录内 `data1.raw` |
| guest `/data/disk2`，64GiB虚拟盘 | `/mnt/disk3/dfkv-validation/20261118t063414z-c9bbdca/data2.raw` |
| guest `/data/disk3`，64GiB虚拟盘 | 同一disk3专用目录内 `data3.raw` |

container把两个专用目录分别bind为 `/cool2`、`/cool3`；源码只读bind为 `/src`。
容器 `dfkv-cool-20261118t063414z-c9bbdca` 只透传 `/dev/kvm`，不使用privileged。
KVM API已实际核对为12，guest已启动Ubuntu24内核 `6.8.0-147-generic`。
三个guest路径仍在，但现在只有**两个物理宿主盘**，不能声称三物理盘独立I/O。

Ubuntu22 wrapper在独立daemon内新建；noble云镜像重新下载，未从disk1复制。
下载文件同时核对官方 `SHA256SUMS` 的准确文件名及冻结SHA：
`04e8f2461a16a44e78a370284471f29616cdd9baf1893df25807b71f985b6c94`。
`current` URL可变化；不同SHA须另登记新环境，不能跳过校验。

## 3. 安全复用现有冷盘环境

以下命令只核查/连接，不建盘、不格式化、不启动第二个QEMU；已在本轮环境验证。
容器或socket不存在就停止复用，不自动回退到默认Docker daemon。

```bash
set -euo pipefail
RUN_ID=20261118t063414z-c9bbdca
D2=/mnt/disk2/dfkv-validation/$RUN_ID
D3=/mnt/disk3/dfkv-validation/$RUN_ID
R=/mnt/disk2/dfkv-run-063755-3532361
C=dfkv-cool-$RUN_ID
SOCKET=unix://$R/docker.sock
dh() { docker --host "$SOCKET" "$@"; }
test "$(dh info --format '{{.DockerRootDir}}')" = "$D2/docker"
dh info --format '{{json .SecurityOptions}}' | grep -q '"name=rootless"'
test "$(dh inspect -f '{{.State.Running}}' "$C")" = true
test "$(dh inspect -f '{{index .Config.Labels "dfkv.validation.run"}}' "$C")" = "$RUN_ID"
vmssh() {
  dh exec -i "$C" ssh -i /cool2/seed/id_ed25519 -p2222 \
    -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    -o UserKnownHostsFile=/cool2/seed/known_hosts \
    -o ConnectTimeout=10 validator@127.0.0.1 "$@"
}
vmssh 'test -f /etc/dfkv-validation-guest; uname -r'
```

Docker命令始终显式指定独立socket，不更改用户默认context。
SSH私钥只保留在本轮 `seed/`，不提交或打包分发；不要输出私钥内容。
仓库 `.omx/evidence/` 留有小型证据副本，位于 `/home` 逻辑卷，不是受测盘或disk1。
证据包中的 `ssh.sh` 实现同一连接方式，但复用不依赖该私有指针。

本轮容器使用 `--network host`，**这里的host是Docker26的RootlessKit网络namespace**，
不是物理宿主网络。实际inode为guest wrapper `net:[4026533842]`、物理宿主
`net:[4026532008]`，已核对不同；不得将该参数复制到rootful daemon。
版本升级后重新核对，不能只凭参数名推断边界，见
[rootless网络历史限制](https://docs.docker.com/engine/security/rootless/troubleshoot/)。

## 4. 新建环境与首次格式化的边界

新一轮先创建不同RUN_ID及owned标记；不复用旧盘或现有VM的文件锁。
独立daemon启动使用专用HOME/XDG目录、空配置、`vfs`和slirp4netns，显式指定
`--data-root`、`--exec-root`、`--pidfile`、`--host`，以及
`--bridge=none --iptables=false --ip-forward=false --ip-masq=false`。
这些是本轮实际参数，不是可在运行环境重复执行的初始化命令。
临时daemon须保持受监督的前台进程，不能因一次短命工具命令返回就假定后台daemon
仍在运行；复用前实际检查独立socket和rootless归属。rootless/cgroup v1的
`docker top` 本轮返回PID枚举错误，改用宿主唯一QEMU命令及只读 `/proc` 核验，
不将收集工具限制算成产品通过或负载失败。

完整初始化记录随证据保留：`new-vm.sh`、`guest-install.sh`、daemon日志及run.json。
缺少证据包时不能冒称已复现全部初始化；先重新冻结路径/命令与镜像校验记录。
[原建立配方](DISK_LIFETIME_DOCKER_SETUP.md)的依赖和验证分层仍可参考，
但其中disk1命令仅是历史记录，**不能直接执行**。

格式化仅在新guest内：先核root serial=`dfkv-root`，再核三个数据盘明确serial
`dfkv-data1/2/3`、块设备存在且无文件系统，任何检查失败立即停止。
保留VM重跑不再格式化；不把宿主 `/dev/nvme*` 传入容器或当作guest盘操作。

## 5. 温度门禁与停止条件

宿主监控仅查询 `/dev/nvme2`、`/dev/nvme0`，约每10秒记录一次SMART JSON。
出现任一非零critical warning、有效温度字段显示达到65°C、采样超时/失败或字段异常，
写 `thermal-stop.json`，核对精确私有RootlessKit PID及运行路径后停止该进程树。
归属核验失败则保留告警并报错，不扩大为停止其他Docker daemon。

65°C是本次**保守诊断停止线**，不是厂商安全上限，也未修改设备温度阈值。
正常复跑前确认监控仍运行、记录持续更新、没有告警文件；不能仅查看一份旧温度。
监控的正常/六类负例fixture已验证，这些不是产品实测用例，也不证明硬件绝不升温。

## 6. 正反序对照与结果口径

物理布局已经改变，旧三宿主盘正序只作为历史参考，不作为本轮反序的配对基线。
已在同一新环境实际执行正序P180→legacy→P60→P30，再反序P30→P60→legacy→P180，
共8组；固定seed/路径/空初始化/预热，正常退出后唯一归档，全部结果保留。
同配置的热点、各首次复用组和末扫汇总命中/缺失在正反各一次中一致。P30两次
均在step9的45秒组缺失13/32；不表示能够识别两次是否正好丢了同13个key。
封存的258份SMART采样覆盖全部8组实跑，并含部分环境建立阶段；两块允许盘
均40–41°C、无告警，
最大采样间隔10.424秒；私有监控/VM仍保留，不承诺采样间绝无温度瞬变。
新guest ON从源码完整构建，RXE1＋真实ring5＋PULL Serve9通过，无skip；
工具65单测与host/guest各2CTest通过。详细表格/年龄/证据见
[冷盘正反序结果](DISK_LIFETIME_QUICK_VALIDATION.md#12-仅两块冷盘的新到达正反序复测2026-11-18)。

场景仍是前12批新到达对象抽样，不是完整到达分布或稳态业务覆盖；
业务权重/回填、多尺寸、独立高并发和正式A/B继续分步处理。
分母及自动门禁见[新到达场景说明](DISK_LIFETIME_QUICK_VALIDATION.md#113-自动校验接入与四配置实测2026-11-18)。
本轮证据目录：`.omx/evidence/arrival-cool-repeat/20261118t063414z-c9bbdca/`。

## 7. 保留与后续清理

样本和环境暂时保留，后续清理只针对上表准确RUN_ID目录及其owned标记。
先正常关闭guest，再停止经名称/ID/标签/socket核验的本轮容器和私有daemon；
确认没有QEMU引用backing后，才处理对应文件。不要删除共享父目录或执行全局prune。
这不授权清理disk1旧数据，不修改其他服务；源代码、密钥和证据按各自边界保留。

## 8. 回填轮次的存活VM恢复（2026-11-18）

回填轮次开始前实查：原独立Docker socket已无daemon响应，旧温度monitor也不存在；
但原唯一QEMU仍运行，应用进程不存在。不能继续声称daemon/monitor仍存活，也没有
回退默认Docker（其root在disk1）、盲目重启旧容器、启动第二个QEMU或重新格式化。

本轮采用有证据的维护恢复，不替代第3节正常复用门禁：
1. 在任何负载前冻结现有QEMU的PID、`/proc/<pid>/stat` starttime和cmdline。
2. 检查`/proc/<pid>/root/cool2/{guest.qcow2,noble-base.img,data1.raw}`与宿主disk2
   文件是同inode，cool3的data2/3与宿主disk3同inode；核对实际打开FD和backing链。
3. 比较该进程netns与物理宿主不同；通过`sudo nsenter -t <已核验PID> -n -- ssh ...`
   仅进入既有隔离网络进行本地转发SSH。保留已有host-key校验，不输出或复制私钥。
4. guest再次核对标记、内核、三盘mount、应用/端口空闲、RXE、真实ring前置检查，
   新温度monitor采样持续更新且无告警后才开始负载。

不把历史PID或文本标记当作新执行授权/实例归属；实例退出/PID复用/文件或namespace
不一致就停止，不能绕过检查。这里没有新增特权容器、host模块/sysctl或物理网络变更。
VM仍是原6vCPU/8GiB/两个物理盘的隔离诊断布局，不因此获得容器硬资源上限。

新monitor每约10秒仅采nvme2/nvme0。warning非零、有效温度>=65°C或采样/字段异常，
先写本轮专用告警，再核对PID/starttime/cmdline与root镜像同inode后SIGTERM唯一QEMU；
不再尝试终止已不存在的旧RootlessKit。归属无法证明时保留失败，不杀其他实例。
真实温度告警未人为注入，不宣称硬件热故障关停耗时已经验证。

本轮恢复、命令、source/binary指纹及新monitor记录：
`.omx/evidence/refill-validation/20261118t081007z-582ea1d/`。
宿主新日志/归档专用目录：
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/refill-20261118t081007z-582ea1d/`。
新日志不覆盖旧258采样历史证据，旧disk1文件不复制/清理。后续每次复跑仍需新鲜
monitor和同样的精确归属检查，不能依赖“上轮曾有监控”。


本轮四配置回填实测均正常退出，结果见快速验证第14节；封存116份SMART，两块允许盘
composite温度40–41°C/告警0，最大间隔10.192968秒，覆盖前置测试及四组负载。确认应用和端口清空后
已正常停止新monitor（专用stop marker保留），VM空闲保留，旧私有daemon仍不可用。
后续复跑必须重新验证实例和启动监控；不要把历史“保留运行中”的描述当作当前存活证据。


## 9. 双尺寸首轮的冷盘复用（2026-11-18）

沿用第8节精确QEMU PID/starttime/root/backing/data inode与netns核验，通过既有隔离
网络SSH复用，没有回退默认daemon、新开VM或格式化。新monitor日志/证据归档在：
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/multisize-20261118t084806z-4c8645a/`。
监控代码SHA与回填轮相同；本轮封存88份SMART，composite40–41°C/warn0，最长间隔
10.190078秒，覆盖前置测试与legacy/lifetime两组。负载后端口/应用清空，monitor正常
停止，VM空闲保留，daemon仍不可用。下轮重新核验并启动新monitor，不能据旧日志假定存活。

虚拟盘/物理盘映射仍为disk2承载root/backing/data1/results、disk3承载data2/3，
仍只有两个物理盘。无宿主disk1测试压力，也未复制/清理它的旧大样本。
结果见快速验证第15节：双档实际参与且账目通过，但默认lifetime大档热点丢失明显，
水位计数0，不能冒充水位覆盖、性能收益或正式A/B通过。实际数据子目录按tag+policy
唯一归档并保留owned marker，清理仍只针对核验后的本轮准确路径，不执行全局prune。


## 10. 双尺寸水位诊断档的冷盘复用（2026-11-18）

仍按第8节对原QEMU的PID/starttime/cmdline、六个root/backing/seed/data打开FD的
同inode及独立netns逐项重验，才经netns SSH复用。未新开VM、格式化或fallback默认
Docker daemon；本轮private socket实查ConnectionRefused，不能假设daemon存活。

新monitor日志/证据归档专用目录：
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/multisize-watermark-20261118t091551z-9d83242/`。
同一monitor代码，7项纯mock控制分支重跑过；封存75份SMART，允许盘composite40–41°C、
warning0、最长间隔10.291038秒，覆盖前置检查及两组实际负载。65°C仍仅诊断停止线，
非厂家限值；不声称实测热故障关停耗时或采样间无瞬变。
应用结束、listener/进程清空并按应用SO_REUSEADDR端口bind过后正常停止monitor，
VM空闲保留，下轮必须重新核验/启新监控。首次未设SO_REUSEADDR的收尾bind失败保留，
复核无listener后修正检查方法，不杀其他进程、不放宽产品门禁。

虚拟盘仍只在宿主disk2/disk3，**继续禁止宿主/mnt/disk1测试压力**，不动旧大文件。
实际数据与结果的准确路径见快速验证第16节及summary.json的archived_data_dirs。
仅同轮固定路由legacy→lifetime首轮诊断；40/50水位覆盖不等于默认92/88表现，
本轮lifetime两尺寸热点均更差。无独立高并发/正式A/B或生产默认变更，O5未整体放行。


## 11. 独立并发首轮的冷盘复用（2026-11-18）

仍按第8节重验唯一原QEMU的PID/starttime/cmdline、六个实际FD/root/backing/seed/data
同inode与独立netns后才复用；无新VM/格式化或默认Docker回退。private socket本轮
实查ConnectionRefused。8个reader独立handle＋2writer handle都在原guest内运行，
不改宿主模块/sysctl/网络、容器硬资源预算或生产配置。三guest盘仍只有两块物理盘。

新monitor日志/证据归档目录：
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/concurrent-pressure-20261118t094212z-6ad2cbb/`。
7项纯mock停止控制分支重跑通过；57份SMART覆盖前置检查及两组负载，nvme2的
composite40–42°C、nvme0为40–41°C，warning均0、最长间隔10.399660秒。
65°C仍仅诊断停止线，不证明采样间无瞬变或真实过热关停耗时。应用正常退出、
listener/server清空并SO_REUSEADDR端口bind通过后，monitor正常停止，原VM空闲保留。
下轮必须重新核验/启新monitor，不能凭历史PID、pointer或旧日志假定环境/监控存活。

**继续禁止宿主/mnt/disk1测试压力**，不动旧热盘大文件。结果与保留data准确路径见
快速验证第17节和summary.json。真实并发分母/字节/淘汰核验通过，不包装成默认lifetime
保留收益（本轮仍大量热点miss）、性能扩展性、长期稳定性或正式A/B/O5整体验收。

## 12. 有界真实客户端首次小规模冒烟（2026-11-19）

基线 `c7ef424`，O5/AB7前置小步；入口与复跑边界见
[正式协议第14节](DISK_LIFETIME_AB_PROTOCOL_V1.md#14-有界真实客户端正确性冒烟已实测非正式-ab)。
本轮复用原QEMU，重新校验PID/start/cmdline、6实际FD/root/宿主资产inode和独立netns，
未使用已失效私有Docker socket，也未回退默认daemon/新建VM/格式化。
仅显式同步5个eviction工具和新单测；既有driver/CABI源及server/lib SHA前后相同。

两例fresh legacy/lifetime各12 PUT/12 GET，字节一致；确认RDMA＋实际io_uring，
无错误/回退/在途/淘汰，server正常退出0。停服负例失败且阻止余请求。
这不是水位压力、热度初态对等、性能对照或正式A/B；无可靠miss状态能力不因本轮通过而解除。

保留文件（没有自动删除）：
- 宿主新增目录及archive/monitor：
  `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/real-smoke-20261119T024347Z-c7ef424/`
  ，其中 `real-smoke-results.tgz` 为仅结果包，不含虚拟盘/密钥。
- guest结果：`/data/disk1/results/real-smoke-20261119T024347Z-c7ef424/`。
- guest数据：`/data/disk{1,2,3}/cases/real-smoke-20261119T024347Z-c7ef424-{legacy,lifetime}`，
  共6个本轮新目录。它们经既有虚拟盘映射到宿主disk2/3，guest disk1不是宿主disk1。
- 仓库本地证据：`.omx/evidence/real-smoke/20261119T024347Z-c7ef424/`，
  含原始账/指标、测试、hash/归属、审查、无效SSH尝试与完成标记证据。

新monitor仅采nvme2/nvme0，35次SMART均warning0，所有有效传感器最高41°C，
最大采样间隔10.180秒；覆盖同步/guest单测/实际执行与结束后，未触发>=65°C停止线。
外层watchdog监视monitor归属/样本新鲜度，死亡/超25秒陈旧则重新证明并停止唯一ownedVM，
不对其他进程发送信号。该兜底本轮未触发，不冒充实际热故障注入。
所有服务/直接客户端和本轮monitor已结束；原VM留存空闲。继续禁止宿主 `/mnt/disk1` 压测。

## 13. 新C ABI＋Python单键状态：小规模RXE/io_uring复核

2026-11-19，源码 `5abc009`。本轮不改生产代码或默认值，不运行水位/容量压力或正式A/B。
继续使用第12节同一已重新核验的VM，未启动默认Docker daemon、未重建/格式化虚拟盘。
六个QEMU实际FD、进程starttime/uid/netns、宿主文件dev/inode/size均重新核对，
根盘/seed/data1由宿主disk2承载，data2/3由宿主disk3承载；guest `/data/disk1`不是宿主disk1。

### 构建、用例及口径

同步提交的源码与新增测试后，143文件SHA逐项核验；完整ON/ON Debug构建成功。
新库包含 `dfkv_get_auto_status`；SHA为
`402a5028e366747371245a145e41eec6ccafc5ac97fa15c356e9ca058666741d`。
server重新链接后字节SHA仍为上一轮值，本轮没有服务端实现变更。
实际guest命令包括：

```bash
cmake --build /opt/dfkv-build -j2
# 17个scalar状态用例；另运行实际RXE1例、内核io_uring5例。
ctest --test-dir /opt/dfkv-build --output-on-failure -R '^(CApiGetStatus|GetAutoStatus)'
python3 -m unittest discover -s /opt/dfkv/test/python -p test_eviction_client.py
python3 -m unittest discover -s /opt/dfkv/test/python -p test_eviction_smoke.py
```

实际使用有界root guest脚本设置本进程memlock上限，未改宿主限制。完整23项C++过滤器/命令/XML
在证据 `guest-units.py` 及结果目录中；Python分别14/17方法，全部0失败/跳过。
两种策略各正常24请求＋缺失/容量不足/停服3负例，合计8例均符合预设判定。
负例各6计划、3尝试/完成、trace ACK8192B；实际C状态依次1/5/4且长度0，
父报告均workload_failed、完整已知计数、rc1/已回收，余请求不执行。
trace累计84计划、66尝试/完成、36 PUT ACK/147456B；另有受控注入2 PUT/8196B、4 REMOVE。
注入脚本调用真实库/服务，单次GET无预探测，不是fake backend；它不是公开CLI新增模式。
每种策略实际PULL终局14（成功grant13、明确not_found1），uring读/完成13，无同步回退/残留/淘汰。
每种策略客户端正常命中12、服务端成功grant13；lifetime热度13、legacy热度0。
该差异按spec已明确的服务端成功口径单列，
不能将容量不足的成功grant算客户端hit。细表见[状态接口第6节](CLIENT_GET_STATUS_COMPATIBILITY.md)。

### 复跑顺序与保留位置

1. 重新证明唯一owned VM与六个backing资产归属；不要只复用历史PID或目录标签。
2. 用新TAG、新host结果目录启动**持续存活**monitor，只采nvme2/nvme0；
   warning非0、任一有效温度>=65°C、样本不合法/陈旧或monitor死亡立即失败并按归属停止唯一VM。
3. 同步冻结源码，比较SHA，顺序执行构建→目标单测→二进制SHA/符号检查。
4. 每种策略仅一个fresh scratch服务，3个新guest数据目录，核验空初态/端口/metrics归属。
   正常例使用公开smoke入口；负例使用本轮证据中的受控child，父账保存真实命令。
   改新TAG/seed/目录后重新冻结脚本SHA，不对旧目录续跑或覆盖，也不盲目复制旧watch的PID。
5. 独立审核原始状态/字节/分母/终局标签，正常退出并回收服务及child，验证端口空闲；
   fresh结束后SMART样本出现后正常结束monitor，结果归档，不清理旧样本。

本轮文件未自动删除：
- 宿主新增结果包/monitor目录：
  `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/rxe-get-status-20261119T092144Z-5abc009/`，
  `results.tgz`仅含本轮结果/日志/指标/测试，不含密钥或虚拟盘。
- guest结果：`/data/disk1/results/rxe-status-20261119T092144Z-5abc009/`；
  测试结果：`/data/disk1/results/rxe-status-tests-20261119T092144Z-5abc009/`。
- guest数据：`/data/disk{1,2,3}/cases/rxe-status-20261119T092144Z-5abc009-{legacy,lifetime}`，共6新目录。
- guest诊断脚本：`/tmp/dfkv-rxe-status-20261119T092144Z-5abc009.py`、
  `/tmp/dfkv-rxe-status-fault-20261119T092144Z-5abc009.py`、
  `/tmp/dfkv-rxe-status-units-20261119T092144Z-5abc009.py`。
- 仓库证据：`.omx/evidence/rxe-get-status/20261119T092144Z-5abc009/`，含脚本/完整命令、
  143文件SHA、二进制SHA、父子原账、故障注入操作账、指标与测试/独立审计。

有效monitor期间60样本，nvme2最高41°C、nvme0最高40°C，warning均0，最大间隔10.208秒。
最初两次普通shell后台monitor未跨工具调用存活，尚未运行guest工作；
正式操作前改为持久前台执行会话并验证PID/starttime/新鲜样本，原2样本保留但不计连续有效监护。
结束复核的首次复杂SSH命令又出现rc0仅MOTD，无完成标记，判无效、不计成功；
改为简单python stdin后实际确认无server、端口空闲、二进制SHA未变。不得只看SSH退出码。
两台服务正常退出0，所有直接child回收、monitor正常结束，VM留存空闲；未触发实际热故障，
不把未触发的兜底说成热故障注入证明。无物理HCA/压力/全量测试/正式A/B或默认切换放行。

## 14. C++直接批量状态核心的小规模RXE/uring验证

2026-11-19，`a05b9be`＋本轮未提交C++改动；详细契约见
[状态兼容方案第7节](CLIENT_GET_STATUS_COMPATIBILITY.md)。重新证明第13节同一VM六个FD/backing、
PID/starttime/uid/netns，仅宿主disk2/3；未使用默认Docker或宿主/mnt/disk1。
fresh持久monitor覆盖源码同步/ON构建/测试/结束复核；37样本，两设备所有有效传感器最高41°C、
warning均0，最大间隔10.210秒；fresh结束样本后正常停止，未触发热故障兜底。

133个生产/绑定/构建/新增测试文件SHA核对一致。仅重建ON相关目标
`cmake --build /opt/dfkv-build --target dfkv dfkv_server batch_get_auto_status_test -j2`，
不是整套ON测试目标全部重编。新库SHA
`53e92f50ad018e15034dda9020acf372729b6efb5a0c5f8da108f63b709f60da`，
新C++ BatchGetAutoWithStatus符号已导出；server字节SHA仍未改变。
ON新13测试通过；另编译显式链接本轮libdfkv.so的有界C++探针并执行，非mock/pipeline标志推测。

legacy/lifetime各一个fresh slab服务，RAM关闭、disk ACK、buffered/uring开启，
容量12GiB只用于稳定几何，**不是容量压力**：每例仅PUT63个16B对象（1008B）。
新接口一个64项批次：62 Ok/1 NotFound/1 Invalid；旧bool接口再读相同64项，
逐项长度和完整字节均验证，旧bool与状态投影相符。两例共126 PUT/2016B、256 GET，
248命中/3968B、4明确miss、4容量错误；每例公开GET指标恰好2请求/128key/124hit/1984B。
读分片配置显式16key/最多8连接，scheduler requeue累计8，结束pending/active0；
不据此宣称8路真实并发或吞吐收益。dedup显式关闭，不冒充dedup状态验证。
服务端每例128终局=uring有效grant126＋not_found2，uring读取/完成126，健康1、
错误/回退/在途/淘汰0；lifetime热度126、legacy0。容量错误的有效grant仍按spec服务端口径计热，
不能改计客户端命中。两台服务正常退出0、child回收、端口空闲；VM留空闲。

本轮文件保留，无自动删除：
- 宿主新目录：
  `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/batch-get-status-20261119T094456Z-a05b9be/`，
  `results.tgz`仅结果，不含VM资产/密钥。
- guest结果：`/data/disk1/results/batch-status-20261119T094456Z-a05b9be/`。
- guest数据：`/data/disk{1,2,3}/cases/batch-status-20261119T094456Z-a05b9be-{legacy,lifetime}`，共6新目录。
- guest探针源/程序：`/tmp/dfkv-batch-status-20261119T094456Z-a05b9be.cc`及去掉`.cc`后的同名程序；
  编排脚本 `/tmp/dfkv-batch-status-on-20261119T094456Z-a05b9be.py`。
- 本地证据：`.omx/evidence/batch-get-status/20261119T094456Z-a05b9be/`，
  含源码与二进制SHA、完整命令、OFF132相关CTest、ON13测试及原始探针状态/字节oracle/指标/退出账。

第一次结束复核脚本以guest普通用户向root结果目录写symbol文件，被PermissionError拒绝，
该次无完成标记、判失败并保留；改为只读nm输出后重新确认无server、端口空闲、新库符号/哈希。
不把该脚本权限错误算生产缺陷或已通过验证。复跑仍按第13节顺序先证明归属、换新TAG/目录，
冻结source/probe并启动fresh温控，不续跑/覆盖旧数据。未做压力、物理HCA、全量CTest、sanitizer或正式A/B。

## 15. 新批量 C ABI → Python → 报告的 RXE/io_uring 复核

本轮源提交 `4709a1f`；全量ON构建和44目标C++、56个Python方法通过，
legacy/lifetime各正常/混合/旧符号/停服4组共8场景预期符合。
仅正常场景passed；负例保留完整已知失败账且阻断后续组。
数值、wrapper与公共入口的边界见[状态接口第10节](CLIENT_GET_STATUS_COMPATIBILITY.md)。
不涉及生产代码/默认切换，也不是正式A/B、物理HCA、性能或真实盘压力。

本轮复用了重新取证的独立VM（6vCPU/8GiB，Ubuntu24.04.5/kernel6.8.0-147/GCC13.3），
没有启动默认Docker，也没有拷贝/读取旧宿主disk1资产。guest RXE是软件设备，不是真实HCA。
本轮六个backing与前文同一套：root/base/seed/data1在宿主disk2目录，data2/data3在disk3。
**guest `/data/disk1` 映射宿主 `/mnt/disk2/.../data1.raw`，不是禁用的宿主 `/mnt/disk1`。**
温控73样本nvme2最高42℃、nvme0最高41℃、warning0、最大间隔10.221秒，
归档/结束核验后取得fresh样本才正常停止；两服务exit0、所有case子进程回收、端口空闲，VM留空闲。
没有实际过热或监控故障注入。用户的宿主disk1禁压约束继续有效。

本轮文件保留，未自动删除：
- 宿主结果目录：
  `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/rxe-python-batch-20261120T014037Z-4709a1f/`，
  `results.tgz`仅两个新结果目录，不包含VM资产或密钥；含温控与各operation原始log。
- guest场景结果：`/data/disk1/results/rxe-python-batch-20261120T014037Z-4709a1f/`。
- guest单测结果：`/data/disk1/results/rxe-python-batch-tests-20261120T014037Z-4709a1f/`。
- guest六个数据目录：
  `/data/disk{1,2,3}/cases/rxe-python-batch-20261120T014037Z-4709a1f-{legacy,lifetime}`。
- guest证据专用脚本：
  `/tmp/dfkv-rxe-python-batch-fault-20261120T014037Z-4709a1f.py`、
  `/tmp/dfkv-rxe-python-batch-units-20261120T014037Z-4709a1f.py`、
  `/tmp/dfkv-rxe-python-batch-experiment-20261120T014037Z-4709a1f.py`。
- 本地证据：`.omx/evidence/rxe-python-batch/20261120T014037Z-4709a1f/`；
  `results/`解包原始结果，archive.sha256校验宿主归档，commands.md列实际顺序。

复跑步骤仍以第13节的安全流程为准：
1. 重新证明当前VM实际PID/start/uid/netns和全部FD/backing，不把旧PID或本页历史归档当当前权威。
   不使用默认Docker或宿主disk1；VM不存在/归属不能证明时先停止，不盲用其他环境。
2. 选fresh TAG/宿主结果/guest六目录与两结果目录，不覆盖或续跑旧文件。
   证据脚本是单次使用，**不能原样执行本轮TAG**；同步修改全部脚本、传输目标及proof标签后冻结SHA。
3. 启动fresh温控，监测两允许盘的所有有效sensor、warning；>=65℃、warning非0、采集失败停止，
   监控失活/样本>25秒的watchdog先证明同一自有VM再终止，不按进程名广杀。
4. 冻结提交并逐文件校验guest源码；完整ON构建完成后，再顺序运行目标单测及两策略场景。
   已有build由root所有时使用guest sudo，不修改宿主权限/ulimit；进程memlock只在guest runner内设定。
5. 正常场景用公共`--batch-gets`、不传`--tcp`；混合/旧符号/停服的wrapper只用于专用scratch服务，
   记录额外动作及native返回，不用于业务流量。即使ssh rc0也必须核验完成marker与原始账。
6. 核对完整逻辑分母与server PULL/grant，停服仅用停前snapshot；不把客户端hit、server热度混算。
   最后回收精确子进程、核验端口/runner、归档、fresh结束温控样本，再正常停止monitor。

后续如清理，仅删除确认无在途服务的本轮guest cases/results/证据脚本；
**不要在VM运行时删除宿主qcow/raw文件或整层父目录**，其中包含正在使用的VM资产。
本轮保留全部失败账：初次构建权限、runner传输shell误解释、一次未启动单测的FD一致性预检失败，
按状态接口第10节说明恢复，不把无完成标记或失败预检算作通过。


## 16. SG 新链路隔离矩阵与保留路径（2026-11-20）

基线f35df95＋5工作树覆盖，392组合源SHA匹配；ON315步完整构建成功，63项C++与80个Python方法
无失败/跳过。新sg_validation/run_sg_validation两工具和单测提供可复跑入口，详见
[SG验证说明](SG_STATUS_VALIDATION.md)，宿主禁压disk1约束不变。
两策略live/stopped四组实际rdma预期符合，合计50逻辑操作、8PUT642B、42GET完整账；
每策略runtime width29，129非空段字节正确，11PULL=9uringgrant+2NF，ring9，L热成功/applied9而G0。
两个capacity失败grant与client hit7不同；停服IO不计miss，旧符号false仍unresolved。
两个服务exit0、4探针child回收、端口空闲、无runner；VM留空闲、监控正常停止。
94SMART样本最高42/41℃，warning0/maxgap10.219s；非压力/性能或热故障注入。

本轮宿主结果（仅此新子目录，不得删除其VM资产父目录）：
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/sg-runtime-20261120T034220Z-f35df95/`。
results.tgz只含两guest结果目录，不含镜像、密钥或backing；42条目安全校验、SHA保留。

Guest保留：
- `/data/disk1/results/sg-runtime-20261120T034220Z-f35df95/`（matrix、两策略各live/stopped、server日志/指标）。
- `/data/disk1/results/sg-tests-20261120T034220Z-f35df95/`（63 C++ XML/日志与五个Python测试日志）。
- `/data/disk1/cases/sg-20261120T034220Z-f35df95-legacy`、`/data/disk1/cases/sg-20261120T034220Z-f35df95-lifetime`。
- `/data/disk2/cases/sg-20261120T034220Z-f35df95-legacy`、`/data/disk2/cases/sg-20261120T034220Z-f35df95-lifetime`。
- `/data/disk3/cases/sg-20261120T034220Z-f35df95-legacy`、`/data/disk3/cases/sg-20261120T034220Z-f35df95-lifetime`。
- `/tmp/dfkv-sg-units-20261120T034220Z-f35df95.py`、`/tmp/dfkv-sg-matrix-20261120T034220Z-f35df95.py`（本轮证据包装脚本）。

Guest/data/disk1的backing实际在宿主disk2，另两个在宿主disk3；不是宿主禁压的/mnt/disk1。
数据/报告不自动删除；以后使用新TAG与新目录，先重新证明VM/挂载/monitor身份及fresh样本，
再用版本化guest矩阵工具，不盲目复用历史PID/start/watch脚本。不得fallback默认Docker daemon。
证据 `.omx/evidence/sg-runtime/20261120T034220Z-f35df95/`，原始失败尝试和TCP零容量边界同样保留。

## 17. CPU dedup新链路实际隔离验证

基线0471f59＋验证工作区，使用此前已证明全在宿主disk2/3的可丢弃VM，不创建默认Docker资源。
fresh两策略各native3/live探针/完整trace/停服TTL边界/stopped探针，固定oracle第一次实际运行通过；
每策略19PUT746B、30GET（22hit938B/3NF/5预期error），20PULL＝17uringgrant＋3NF，Lheat17/G0。
6个受控native case实际rdma，owner本地IO注入不是网络故障，native本地默认TCP可单测重跑。
Guest88Cpp/126Python无skip，宿主189相关CTest通过；3×3 TSan通过。83温控样本最高42/41°C、
warning0；after402源文件SHA/六FD/空闲端口通过，服务/客户端回收，monitor正常停止，VM留空闲。
说明、精确目录/清理边界及复跑方法见[CPU运行时验证说明](CPU_DEDUP_RUNTIME_VALIDATION.md)。
未进行真实盘压力/热故障注入/HCA/GPU/正式A/B，继续禁止压测宿主/mnt/disk1，默认配置未变。
