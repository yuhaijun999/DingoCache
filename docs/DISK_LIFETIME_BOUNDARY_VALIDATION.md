# 磁盘淘汰：参数、资源、背压故障与长时混合验证

## 状态与范围

2026-11-26，基线 `fc89a03`。本轮已完成所列诊断及原账复核，**不是正式 A/B 或生产放行**。
生产默认、评分公式、C ABI 与磁盘格式不变；只用 RDMA/RXE＋io_uring。
GPU、真实推理引擎采用及 TTFT 按用户要求排除，不列为待补硬件验证。

本轮沿用[正式协议](DISK_LIFETIME_AB_PROTOCOL_V1.md)的 W07/W08/W09 分类，
不把既有机制误判为尚未实现，不把局部测试称作全矩阵验收。

## 冻结矩阵

| 项目 | 本轮配置 |
|---|---|
| 策略／读上限 | 当前二进制 legacy 参考；lifetime M=1/2/4/63 |
| 淘汰路径 | class-pressure；watermark 两条，各五配置，共十场 |
| 冒烟 | 每场 8 步，首次复用 1/2/3 秒，只验证工具与路径 |
| 长测 | 每场 1800 步、约 30 分钟；首次复用 45/185/600 秒 |
| 业务形状 | 4096/1048577 字节对象，多尺寸前缀链、持续新到达、已有热点、缺失回填 |
| 桶配置 | 全配置显式 G=400000 秒、B=64；legacy 中 inactive |
| 评分 | BASE=604800、BONUS=302400、KEEP=475200、P=180、SLOW=4；BOOST 自动随 M 派生 |
| 资源 | 6 vCPU/8 GiB guest，三 guest 盘各 4 GiB，1 GiB extent／1 MiB granularity |
| 写入 | RAM off，PUT ACK=disk，payload write=buffered |

M=4/63 使用默认 G=43200、B=24 时，桶时间覆盖不足，应拒绝启动。
合法矩阵统一 G/B，避免无效配置被当作策略失败；不声称是完整默认配置、
单独 M 的因果实验或按测试结果选出的参数。自动 BOOST 也随 M 改变。
M+1 是配置导出的每档链数；实际队列分配由已有公共接口测试核验，不能用配置值冒充实测。

watermark 使用诊断水位 40/50、reclaim 50ms；class-pressure 禁用水位和后台回收。
donor、后台 class 和水位活动分别保留，不把聚合淘汰都归因于单一分支。
顺序为各路径 legacy、M1、M2、M4、M63；不是正式交替三配对收益实验。

## 安全与中止

- 宿主 `/mnt/disk1` 与 `/dev/nvme1*` 禁止本任务压力及 SMART 采样。
  guest `/data/disk1` 实际映射宿主 `/mnt/disk2`，不能按 guest 名字误判。
- 只用宿主 `/mnt/disk2`、`/mnt/disk3` 专属 backing；先重新证明 VM PID/starttime、
  UID、netns、六个 FD 的 dev/inode 与 guest mount、RXE、io_uring、空闲端口及二进制。
- 每轮新建温控 guard，只采 nvme2/nvme0；任一传感器 ≥65°C、SMART warning 非零、
  无效采样、采样失败或心跳 ≥25 秒，停止本轮。65°C 是保守诊断线，不是厂商额定值。
- 任一语义、字节、计量或进程终局失败，保留 FAILED 原账并中止后续矩阵；
  不放宽门槛、不选最好结果、不偷偷重跑。修工具须新版本及独立新输出。
- 不改宿主内核、网络、daemon，不 drop_caches，不删除活跃 raw/qcow2；只回收精确拥有的进程。

## 验证层次与仍开放的证据

### 参数拒绝与数据边界

重新执行公共参数范围、耦合、打开任一盘前失败，以及已有元数据不变测试。
额外真实进程负例保存错误中的参数名／原值、非零退出、超时与 reap，
对已有盘的必要元数据做哈希并比较全部目录项；未创建盘仍须不存在。
该目录快照不是全部 payload 字节的证明；成功读回／已有字节测试另列。

### pin、预算及故障

复用 allocator 的全 pin、长 pinned 前缀、跨 tick 游标、M63、256/1024 预算、
全 E 粗筛／8192 精查与选中失效，以及 Store、writeback 的 I/O 错误、延期 Remove、
owner/follower 完成及取消等真实回归。记录本次构建与 fresh 结果。
这是可控测试接口故障，不冒称真实盘掉电、设备故障或 native 压力中的故障注入。
安全故障结果不与正常性能样本混算。

### 资源与分项成本

新增 `/proc` 采样在每次读取前后核验 PID/starttime，分别记录 server/client 的
RSS、VmHWM、线程、FD、VmLck、CPU ticks 和单调时钟；进程消失、身份变化或缺字段不是零占用。
RSS 峰值只代表采样观察；VmHWM 是包含预热的进程寿命高水位，不是测量窗口独占峰值。
VmLck 不是完整 RDMA 注册内存账，独立进程峰值不能相加当同时峰值。
原始 Prom 保留 MR 声明／注册 rail、scan、pin skip、budget、donor、九类 slots.tbl 操作
与四个 allocator A 锁入口的计数／时间；client 未导出的 MR 数量不补造。
粗扫、精查、选中清表／sync、锁 wait/held 分开解释；嵌套时间不能相加冒充互斥成本。

本轮紧凑 12 GiB 几何不覆盖合法 4 KiB／目标满规模的全部内存、恢复峰值与 MR 资源。
已有 capacity/sizeof 指标及 full-E 功能测试都不能补成完整资源性能验收。
独立高并发证明见[前轮](DISK_LIFETIME_PREFIX_CONCURRENT_VALIDATION.md)；
本轮长测为串行混合业务流，不冒称独立高并发新证据。

## 验收准备交接

1. 安全硬闸门仍零容忍，错误／unknown 不计为 miss；连续前缀在原请求回填前冻结。
2. 使用[既有阈值表](DISK_LIFETIME_THRESHOLDS_TEMPLATE.md)与
   [正式报告模板](DISK_LIFETIME_AB_REPORT_TEMPLATE.md)，不再建一套重复门槛。
3. 当前二进制 legacy 不等于 v2.33.0；旧 binary、目标资源、适用 RAM ACK 迁移参考、
   业务 trace／权重、独立职责与每子指标数值／签署未齐，保持正式 A/B blocked。
4. 本代理已见探索结果，不代签盲态制定者；不能从本轮 lifetime 结果倒推正式阈值。
5. 合成前缀代理可以验证缓存语义及容量取舍，不证明推理引擎采用或 TTFT。

## 实际命令、结果与数据位置

### 构建与单测（本轮新鲜执行）

仓库根目录可直接执行，不依赖 `.omx` 指针：

```bash
cmake --build build-test -j2
python3 -m unittest discover -s test/python -p 'test_boundary*.py' -v
ctest --test-dir build-test --no-tests=error --output-on-failure -R '^python_boundary'
```

`build-test` 是本工作树 RelWithDebInfo、RDMA OFF／URING OFF 的已有构建目录；
换目录使用自己的构建目录，不把此名字当成通用已配置环境。
本轮完整相关集实际命令：

```bash
ctest --test-dir build-test --no-tests=error --output-on-failure \
  -R '^(python_boundary|python_prefix_|SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab|SharedLifetimeClockTest|DiskGroupTest|DiskEviction)'
```

- 新增 38 个单测（15 resource、13 startup、10 runner）：host 5.179 秒、guest 5.824 秒，均 0 失败／跳过。
- host 572 项相关 CTest，68.39 秒；guest 575 项，87.82 秒，均 0 失败／跳过。
  Python 的三个 CTest 条目包含上述 38 方法，不能把方法数和入口数相加当不同用例。
- 原生结束后再次 host build、38 方法（5.108 秒）、3 个注册入口（6.13 秒），均通过。
- guest `/opt/dfkv-build` 为 Debug、RDMA ON／URING ON；host OFF 与 guest ON 的构建命令均成功。
  无生产 C++ 修改，增量构建二进制 SHA 保持不变，不冒称重新做了 clean build。
- 六 Python 文件 AST／`py_compile` 和任务 diff 空白检查通过。Ruff/Pylint 不可用；
  没有新的 sanitizer、完整 lint/typecheck 或真实 HCA 性能通过声明。

### 真实场景与计量

- 十场冒烟：884.153332 秒；3854 个 API 事件、128152 个逐对象 PUT ACK，
  1788 hit／37 miss，error／unknown 均零。300 个计划业务请求含 61 个 censored，
  不是 300 个都完成；冒烟 1/2/3 秒不替代长测。
- 24 个真实二进制启动负例：12 配置各测现存三盘和从未创建的三盘。
  全部 exit=1、包含 ERROR 及所需参数原值、已 reap；元数据／目录清单不变，缺失盘仍缺失。
- 十场主长测：控制器 19076.901669 秒，约 **5 小时 17 分 57 秒**。
  每场 30 分钟测量，串行总计约 5 小时测量；不是同一 server 连续跑了五小时。
- 主长测 182829 个 API 事件、1645233 个逐对象 PUT ACK、875968496340 逻辑 payload 字节；
  GET 60203 hit＋51356 miss＝111559 次，GET／PUT error 与 unknown 均零。
  batch API 事件与其中的对象 ACK 分母不同；逻辑写字节不是设备／NAND 写入或物理写放大。
- 64800 个计划业务请求＝56500 完成＋8300 censored；每场 6480／5650／830。
  每场 45 秒组完成 1755、censor 45；185 秒组 1615／185；600 秒组 1200／600。
  实际首次复用最大年龄分别约 46.202／186.799／601.174 秒，保持原 +2 秒调度约束。
- 独立逐项复算 payload SHA、回填前连续前缀、实际年龄、全部终局与 Prom 分母；
  主长测 1832 份指标快照、3654 条 server/client 资源记录均核对。
  每份指标中三种错误标签和九类元数据路径错误均为零；两条路径均有实际淘汰进展。

### 业务与资源结果（不作为正式排名或门槛）

| 路径 | 策略／M | 已知可用前缀 token | 合格目标 token | 比率 | 回填 PUT ACK | server RSS 峰值 KiB | client RSS 峰值 KiB |
|---|---|---:|---:|---:|---:|---:|---:|
| class-pressure | legacy | 180968 | 855040 | 21.165% | 32026 | 39064 | 126356 |
| class-pressure | lifetime/1 | 92544 | 855040 | 10.823% | 36558 | 29808 | 119280 |
| class-pressure | lifetime/2 | 82240 | 855040 | 9.618% | 37123 | 38796 | 121984 |
| class-pressure | lifetime/4 | 96680 | 855040 | 11.307% | 36363 | 29840 | 117792 |
| class-pressure | lifetime/63 | 84456 | 855040 | 9.877% | 37154 | 40140 | 125544 |
| watermark | legacy | 135984 | 855040 | 15.904% | 35564 | 27316 | 123564 |
| watermark | lifetime/1 | 65720 | 855040 | 7.686% | 38720 | 27904 | 122256 |
| watermark | lifetime/2 | 64528 | 855040 | 7.547% | 38501 | 36900 | 123788 |
| watermark | lifetime/4 | 64960 | 855040 | 7.597% | 39190 | 27764 | 121396 |
| watermark | lifetime/63 | 57464 | 855040 | 6.721% | 39154 | 38148 | 122908 |

本轮每场合格目标 token 恰好相同；但顺序、路径路由、page cache、后台活动及
BOOST 联动仍需保留，不是三轮反序正式 A/B。这里的 token 是合成前缀代理。

**所有 lifetime 配置的综合前缀复用在本负载中仍低于 legacy，回填更多。**
class 场景 lifetime 在 600 秒组保留 376–648 个部分前缀 token，而 legacy 为零；
但热点损失抵消了这一局部收益。两策略在 185/600 秒均未取得完整长链命中。
watermark 场景 lifetime 的 45 秒组部分前缀 token 高于 legacy，但 185/600 秒组均为零。
不能用这些局部优势宣布整体胜出，也不能从一次合成矩阵推导普遍策略排序。

server 观察 RSS 峰值 40140 KiB、线程最多 13、FD 最多 41；client 观察 RSS 峰值
126356 KiB、进程寿命 VmHWM 126624 KiB、线程 2、FD 最多 14。
这些极值可能来自不同场次和时间，不可相加；资源样本含准备／预热。
客户端账本累积、连接生命周期和采样窗口均是趋势解释的混杂因素，不能据此诊断泄漏、
宣称无泄漏、登记 RSS SLO 或确认 M 对资源的单独因果影响。VmLck=0 不代表未注册 RDMA 内存。

### 扫描、锁与持久化成本的证据边界

`main-costs.json` 从每场 before/after 与周期 Prom 提取计数和 server MR gauge；
保留全部标签、单位及分母，没有用缺失指标补零。原始指标为最终依据。

- class 的 lifetime 粗筛 extent 访问总量约 115.6–116.1 万次／场，
  前台 class resident 访问量 177241／200161／222643／240818（M1/2/4/63）。
  这是累计访问次数，不是每次选择扫描百万 extent；本几何每盘只有四个 extent。
- 实际 donor 精查 resident 总量很小（每场 3–7）；**全部 lifetime 场 donor 释放居民为零**。
  因而即便持久化计时非零，也不能宣称测到了满 extent 清表／sync 或 8192 精查的成本上界。
  full-E／8192 功能边界来自本轮重新执行的已有 donor 回归，不能替代对应资源成本测量。
- A 锁四入口、九类 slots.tbl 操作、donor selection 与 persistence/rebind 分项保留；
  锁内外及跨盘并发时间并不互斥，不能相加变成单请求延迟或设备 I/O 时间。
- 30 分钟／场跨过 P=180 秒，但没有自然跑完 G=400000 秒或数日 KEEP／评分 horizon；
  桶轮转及极限时钟的功能证据是受控时钟单测，不冒称物理经过数日。

### 复跑入口（仅已证明环境并启动新 guard 后）

在对应 disposable guest 内，以已配置 ON 构建为例：

```bash
cd /opt/dfkv
python3 deploy/validation/run_boundary_validation.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/boundary-UNIQUE-smoke --tag UNIQUE-s --preset smoke
# 全部冒烟和原账通过后，另用新目录／新 tag，--preset long。
```

真实 RDMA 客户端需要已登记的 memlock 及依赖；本轮受监督 launcher 显式设置
RLIMIT_MEMLOCK 为 infinity。入口只校验 guest 路径，**不会代替宿主 backing 身份证明和温控**。
`boundary_startup.run(command, env, directories, new_output)` 是负例 API；
调用方必须提供显式 slab/lifetime 命令、完整环境及自己专属的现存／缺失盘目录。
它不创建缓存目录、不删除数据；`ProcessSampler` 也不会 signal 进程。

### 原始证据与保留文件

本轮唯一 E：
`/home/server/work/DingoCache.omx-worktrees/launch-feat-wip-evict-v1/.omx/evidence/boundary-validation/20261126T072357Z-fc89a03`。

- `source-freeze-r1.json`／`source-r1.tar` 固定 355 个源／构建／测试文件。
  provisional 源和测试保留：最后三份 helper/test 在原生开始前补齐后，已重新冻结并完整测试；
  原生全部使用 r1，不混用早期 37 方法结果。
- `smoke-results`、`main-results`、`negative-results`、`tests-results` 保存完整归档展开结果；
  `host-ctest-r1.xml`、`final-boundary-ctest.xml` 与 guest XML 保存测试账。
- `independent-smoke-raw-review.json`／`independent-main-raw-review.json` 为无 author helper 导入的
  独立 stdlib 原账复核；不是人员权限隔离、认证签名或正式验收签署。
- 宿主 runtime／归档：
  `/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/boundary-validation-20261126T072357Z-fc89a03`。
- guest 结果：`/data/disk1/results/boundary-validation-20261126T072357Z-fc89a03-r1-{smoke,main,negative,tests}`；
  provisional 测试另在不含 `-r1` 的 tests 目录，控制脚本在
  `/opt/validation-boundary-20261126T072357Z-fc89a03`。共享 `/opt/dfkv{,-build}` 不是可随意删除的本轮缓存。
- 精确 **60 个本轮缓存目录**见 `retained-data.json`，分散在 `/data/disk{1,2,3}/cases`；
  已记录 dev/inode、du、clean marker 与 CRC，共 46962606080 分配字节，约 43.737 GiB。
  最终均 clean=1、CRC 正确，服务／客户端／本轮 launcher 已退出、端口空闲。
  剩余 guest 空间约 14.018／22.103／20.710 GB。未删除本轮或历史缓存。
  后续清理须按精确清单重新核对空闲与归属；不要删除仍运行 VM 的 raw/qcow2，也不要宽泛 glob 清理历史轮次。
- 温控 2086 条样本，仅 nvme2/nvme0，最高 42°C，最大间隔 11.231 秒，无 SMART warning／alarm。
  guard 已正常停止并 reap=0，PID 消失；`safety-closeout.json`、`final-prestop-six-fd-proof.json`
  和 `host-logs/thermal.jsonl` 保留证明。现在没有本轮 native 压力或 monitor。

**阶段结论：**本轮所列配置合法性／功能故障回归／紧凑几何稳定性诊断完成；
完整资源、实际 RAM×coalescer 矩阵、目标业务及正式 A/B 数值／职责门槛继续开放。
保持现有生产默认，不因 M63 合法或稳定性通过就推荐扩大读上限、切换默认或部署。
