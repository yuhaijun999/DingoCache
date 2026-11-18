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
