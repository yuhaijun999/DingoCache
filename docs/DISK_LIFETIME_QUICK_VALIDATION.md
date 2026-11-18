# 快速触发磁盘淘汰：隔离环境冒烟验证

目标是在已有 Docker/VM 环境中，用小容量和预填充快速制造压力，不等待默认寿命
参数自然老化。只验证有限合成场景的正确性与实际淘汰，不是性能 A/B、默认配置长期
收益、物理 HCA/GPU 或线上放行证明。环境创建/连接见
[Docker/VM 建立配方](DISK_LIFETIME_DOCKER_SETUP.md)。

## 1. 固定配置和负载

- 三个盘的合计逻辑容量12GiB（每盘4个1GiB extent），仅4KiB对象、一个尺寸档。
  不能将这个几何直接用于混合尺寸基线；混合尺寸会触发额外 extent 分配/重绑。
- 显式 slab/lifetime、buffered、RAM关闭、disk ACK、真实RXE＋kernel io_uring；
  RXE接收分块8MiB，后台回收周期50ms。分块是诊断配置，不改变生产默认。
- **不缩短寿命、保护期或桶时间**，保留默认评分/水位/时间参数；隔离继承的 `DFKV_*`
  环境变量，不修改系统时间、磁盘格式或生产默认值；表同步也保留默认值。
- 两个二进制 namespace 使用相同逻辑key、不同确定性内容。每个namespace预填4608个
  对象，选前32个作热点，严格读取两轮，然后按批继续冷写至6452个对象，批间探测热点，
  最后扫描所有对象。每次命中都核对长度和完整字节，失败保留key与摘要。
- 空间不足时保护是软保护，不能理解为“保护期内绝对不能淘汰”。首次早读还会撤掉新写
  BOOST，所以本场景只记录热点命中/缺失，不断言热点一定胜过刚写入的冷数据。

## 2. 执行

先完成建立配方A，确认guest源码包含本轮两个工具且已构建。以下通过 `vmssh`
进入隔离guest执行，**不是在宿主机运行sudo或修改宿主设置**：

```bash
vmssh 'sudo bash -se' <<'GUEST'
set -euo pipefail
ulimit -l unlimited
cmake --build /opt/dfkv-build -j4
CASE=quick-$(date -u +%Y%m%dT%H%M%S)-$$
mkdir -p /data/disk{1,2,3}/cases /data/disk1/results
python3 /opt/dfkv/deploy/validation/quick_eviction.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases \
  --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/$CASE
GUEST
```

对照旧模式可加 `--policy legacy`，仍使用新的 `--output`。这只是两种策略分别冒烟，
不是配对性能A/B：UUID目录参与路由且两次初态路径不同。工具可从任意工作目录调用，
`--build` 和数据根目录应指向实际guest位置。

固定端口12000/12001/12010必须空闲，只能串行运行；工具拒绝忙端口、不停止其他服务。
启动最多等待30秒，负载子进程最多120秒，服务退出最多等待15秒；再加指标请求及
本机调度开销，单场景目标是几分钟。环境建立、首次编译不计入该目标。超时或无实际
淘汰均失败，不能靠延长保护期或放宽字节校验将失败改成通过。

## 3. 成功标准和结果

- 客户端全部使用RDMA，三个盘实际有对象；真实ring有读和完成，fallback及遗留在途为0。
- 实际淘汰计数必须大于0。记录水位、后台回收、extent steal计数以辨别路径，
  这些计数不可直接相加充当总淘汰；本场景不保证覆盖所有淘汰入口。
- 服务健康，错误计数为0；PULL派发/终局/客户端成功与缺失分母对齐，
  仅允许成功非空与not_found，不把C ABI miss单独当成驱逐证明。
- 预热阶段不允许缺失，施压后允许真实缺失，但命中仍必须完整字节正确。
- lifetime逻辑成功计数与客户端成功读一致；适用热度应用数单独记录，
  允许施压时原住户已被驱逐而无法应用通知，不能把应用数当作逻辑成功分母。
- 服务正常退出。被迫SIGKILL清理、非零退出、启动/负载失败均保留失败结果。

`--output/result.json` 是总结果，退出码0才通过；`case.json` 记录命令、显式环境及
三个新建数据目录，`before.prom`/`after.prom`、`server.log`/`client.log` 和
`trace/{trace,result}.json` 保留证据。失败不删除目录、不自动重试掩盖原失败。
若负载失败，after.prom可能不存在，必须按失败结果处理。

工具只在指定的现有根目录内新建UUID子目录，拒绝复用输出，不清理旧数据。
本环境真实数据仍落在宿主授权的 `/mnt/diskN/dfkv-validation/20261117t035613z-d69e126/`
虚拟盘文件中。清理时根据case.json定位本次目录，先确认应用已退出；停止VM或删除
宿主镜像的规则仍见建立配方，不能删除共享目录或仍被使用的backing文件。

## 4. 首次实际执行结果（2026-11-17）

基线 `3c64fd1` 加本轮诊断工具修改。复用同一已授权的Ubuntu24 guest，内核
6.8.0-147，RXE rxe0，uring开启。两场景串行、独立新目录，**没有改短默认时间参数**。

| 模式 | 全场景耗时（含启动/退出） | 实际水位淘汰 | 最终扫描命中/缺失 | 热点探测命中/缺失 |
|---|---:|---:|---:|---:|
| lifetime | 18.93秒 | 2092 | 10812 / 2092 | 4531 / 3021 |
| legacy | 18.70秒 | 2092 | 10812 / 2092 | 7552 / 0 |

两者7552次热点探测均包括128次严格预热；派发/终局总数各20456。
lifetime的15343次成功读与热度通知/应用一致，kernel读/完成15343；legacy的kernel
读/完成18364。两场景均wrong_bytes=0，fallback=0，错误终局=0，三盘有对象且健康，
服务退出0；后台class回收与extent steal均0。**这里只实证水位入口，不声称覆盖所有淘汰路径。**

热点缺失差异如实保留，不调整参数“优化”本次结果。这种短时新写压力下，首次早读
撤BOOST与新写保护规则可能使热点相对新冷写不占优；此方向与当前规则一致，但本轮
未逐victim追踪证明全部原因，也不能用一个场景判定算法优劣或收益。后续需要固定
初态/路由、独立交替轮次，以及跨保护期的持续观测，不能直接当作正式性能结论。

无硬件单测12＋9共21通过，覆盖正常流程、预热缺失、热点损坏、无淘汰、缺失/非法指标、
隐藏错误、终局分母和退出失败。host `build-test` 与guest `/opt/dfkv-build` 构建成功
（CMake重新生成，Ninja无待编译工作）；两处均通过CTest的
`python_disk_validation`、`python_quick_eviction` 两个入口，无跳过。未重跑全量CTest
或sanitizer；本轮无生产C++修改，所以没有新的C++ clang-format/clang-tidy执行记录。

证据：`.omx/evidence/quick-eviction/20261117t082057z-3c64fd1/`，含源码SHA、单测/
构建/CTest日志和XML、真实guest结果包及逐case指标。guest结果目录为
`/data/disk1/results/quick-20261117t082057z-3c64fd1-{lifetime,legacy,tests}`。
六个新数据目录的准确UUID路径见各case.json；全部位于既有授权虚拟盘，应用已退出，
VM及样本保留。以后每轮须重新生成输出和数据目录，不能覆盖本轮证据。

## 5. 固定初态的180秒BOOST窗口对照

`--pressure-delay-sec 185` 在严格预热后真实等待185秒，再执行同一冷写/热点探测轨迹；
范围0–300，默认0。工具按等待时长延长客户端总超时，不改系统时钟或寿命参数。
**180秒是新写BOOST窗口，不是所有KEEP软保护到期，也不是TTL。**

默认评分中，新写未读住户在窗口内可能比已读热点分数更高；跨窗会撤掉该批未读住户
的BOOST。这提供一个待检验的解释，不预设热点保留必然改善。既有数学单测验证179/
180边界；本轮用真实185秒等待，只证明跨窗，不冒充精确边界测量。

### 5.1 四组设计与复跑

四组：lifetime/legacy，各执行即时施压与等待185秒后施压。保持同一固定目录字符串、
磁盘顺序/权重、seed、key、容量及负载；每组必须从全新空目录启动，不能恢复上一组。
`--data-name` 可指定安全的单个目录名；仍拒绝复用已有目录。正常退出后，只归档本轮
新建目录、重新创建相同路径，才能固定以路径建环的路由。不能只用不同UUID目录声称
路由一致，也不能删除不明来源的数据。

建立配方A环境已就绪后，可在guest执行下列流程；这是复跑配方，不是新的通过记录。
它保留四组原始数据，不执行删除。完整阶段审计仍须满足5.2，不能仅看四个退出码：

```bash
vmssh 'sudo bash -se' <<'GUEST'
set -euo pipefail
ulimit -l unlimited
RUN_ID=$(date -u +%Y%m%dT%H%M%S)-$$
FIXED=age-$RUN_ID-active
mkdir -p /data/disk{1,2,3}/cases /data/disk1/results
for label in lifetime-early legacy-early legacy-aged lifetime-aged; do
  policy=${label%-*}; delay=0
  [[ $label != *-aged ]] || delay=185
  OUT=/data/disk1/results/age-$RUN_ID-$label
  python3 /opt/dfkv/deploy/validation/quick_eviction.py \
    --build /opt/dfkv-build --policy "$policy" --pressure-delay-sec "$delay" \
    --data-root /data/disk1/cases --data-root /data/disk2/cases \
    --data-root /data/disk3/cases --data-name "$FIXED" --output "$OUT"
  python3 - "$OUT" "$FIXED" "$label" <<'ARCHIVE'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1]);name,label=sys.argv[2:]
r=json.loads((out/'result.json').read_text());case=json.loads((out/'case.json').read_text())
if r['status']!='passed' or r['server_exit']!=0:raise RuntimeError('not cleanly stopped')
expected=[pathlib.Path(f'/data/disk{i}/cases')/name for i in (1,2,3)]
if case['data_dirs']!=list(map(str,expected)):raise RuntimeError('unexpected store paths')
for p in expected:
    archive=p.with_name(name+'--'+label)
    if p.is_symlink() or not p.is_dir() or archive.exists():raise RuntimeError('unsafe archive')
    p.rename(archive)
ARCHIVE
done
GUEST
```

### 5.2 需要核对的阶段证据

客户端保存五份 `trace/phase-{prefill,warm,before-pressure,after-pressure,after-scan}.prom`
及 `phases.json`，采集失败会使测试失败。HTTP指标读取不是业务GET，不给住户加热。

- early组施压前的客户端累计时间必须小于180秒，它是最早预填充写入年龄的上界。
- aged组实测等待至少185秒，施压前与预填充完成阶段的间隔也至少185秒；
  不能把“参数传了185”当成实际等待完成。
- 四组prefill/warm/before-pressure均9216个对象、淘汰0；逐盘对象数和slot占用字节一致。
  若不成立，应判实验初态无效，不可选择性忽略。
- warm和before-pressure的PULL派发/终局均128；lifetime逻辑成功/应用也保持128，
  排除等待期额外读热。默认keepalive允许继续，但它不能被计成业务读取。
- 每组严格7552次热点探测、20456次业务GET；命中字节、错误终局、健康、ring和退出
  继续按冒烟门禁核对。热点命中/缺失按阶段展示，不预设胜负。

延后施压组不是早期组在原地等待后的“热点恢复”：四组独立初始化，已被淘汰的对象
不会因时间经过而自动回来。

这是单轮2×2“初始未读住户跨窗”诊断。等待时热点是静默的，不代表持续热读或持续冷写
负载；不证明每个victim的因果、断电持久化或正式性能A/B。后续持续业务负载与交替重复
轮次需要另行设计，不能拿该短轨迹宣称线上收益。

### 5.3 固定路由的首次四组实测（2026-11-17）

基线 `0577e6f` 加本轮工具增量，仍用同一私有RXE＋kernel ring guest。四组施压前
逐盘对象数均3079/3079/3058，总9216；对应slot字节也完全一致，淘汰0。
本几何每个4KiB对象占用1MiB slot；水位按分配占用计算，不能按payload总量推算。

| 模式/施压时机 | 真实等待 | 总耗时 | 热点探测命中/缺失 | 最终扫描命中/缺失 | 水位淘汰 |
|---|---:|---:|---:|---:|---:|
| lifetime，即时 | 0秒 | 18.45秒 | 4380 / 3172 | 10812 / 2092 | 2092 |
| legacy，即时 | 0秒 | 18.75秒 | 7552 / 0 | 10812 / 2092 | 2092 |
| legacy，跨窗 | 185.0003秒 | 201.79秒 | 7552 / 0 | 10812 / 2092 | 2092 |
| lifetime，跨窗 | 185.0003秒 | 202.55秒 | 7552 / 0 | 10812 / 2092 | 2092 |

热点数字是**探测请求数，不是不同对象数**；热点集合只有64个对象。
两组即时施压时的客户端累计时间分别约2.95/2.90秒，小于180；两组跨窗从最后预填充
完成到施压前均超过185秒。warm与before-pressure的业务读均保持128，等待期间没有
额外读热。四组共20份阶段快照通过健康、错误、ring和分母审计，各20456次业务GET，
命中字节全部正确、服务退出0；原始数据仅在退出后归档，没有复用恢复初态。

**观察结论：本轨迹中，延后施压的lifetime组不再出现热点探测缺失。** 结合生产评分
和已有边界单测，证据支持初始冷住户BOOST状态变化会影响该现象的解释，但没有追踪
每个victim，不作唯一因果断言。它不抹掉即时施压组的真实缺失，也不证明持续在线
流量、长期收益或默认策略应该切换。每格只有一轮，不满足正式交替A/B验收。

本轮无硬件单测15＋13＝28通过，已有评分默认/179–180边界单测2通过；host OFF与
guest ON构建成功（Ninja无待编译工作），各两个CTest入口通过。没有全量CTest/
sanitizer或新C++静态检查执行；本轮没有生产C++修改。原有7552次热点轨迹计数检查
也已纳入运行时门禁，不再仅依赖事后审计。

证据：`.omx/evidence/age-window/20261117t084028z-0577e6f/`，含阶段指标、实际等待、
固定路径/参数/源码SHA、四组supervisor、XML及独立审查。guest结果：
`/data/disk1/results/age-window-20261117t084028z-0577e6f/`。归档数据位于各盘
`/data/diskN/cases/age-20261117t084028z-0577e6f-active--{policy}-{early,aged}`，准确路径
见summary.json；四组均已退出，VM与样本保留。底层仍仅使用原授权宿主虚拟盘文件。

## 6. 持续交错热点/冷写的三轮成对重复

`--scenario sustained-pressure` 复用启动和数据正确性门禁，默认仍为quick-pressure。
这一场景不是独立并发读线程压测，也没有自动回填；热点丢失后继续探测缺失，不将
同key重新PUT。它用于观察保留行为，不能代表真实推理业务的重算/回填效果。

### 6.1 冻结轨迹与实验有效性

- 默认时间/评分/水位不变，RAM关闭、disk ACK、12GiB三盘单档、4KiB载荷。
- 两namespace各预填4608个对象、各32个热点严格读取两轮；之后200步，每步先给
  两namespace各写16个全新冷key，再各读取32个热点；最终扫描每namespace7808个key。
- 下一步以实际步骤起点加1秒安排，不在暂停后密集追赶。每组12928次热点读（含128次
  预热）、15616次最终扫描，共28544次业务GET，冷写key不与热点重叠。
- 实测节拍至少覆盖199秒；相邻步骤起点间隔须在1–5秒内（浮点误差允许1微秒）。
  5秒是本轮预先冻结的连续性有效门槛，不是性能发布阈值；超出即场景节拍无效，
  不把暂停后的结果归因于算法。记录最大间隔、超期和步骤耗时，不当作GET/PUT分位延迟。
- 按实际时间选取距末次预填充ACK至少185秒后的完整步骤，至少10步，单列命中/缺失
  分母。过渡步骤不能伪装为精确180秒边界样本。
- prefills无淘汰，逐盘初态一致；固定路径、顺序、权重、seed、负载和ACK，每组
  从空目录/新进程开始。服务正常退出后只归档本轮数据，不复用恢复状态。

三轮顺序预先固定为：legacy→lifetime、lifetime→legacy、legacy→lifetime，共六组。
这只是交替先后的固定轨迹保留性重复，不因轮数达到三就成为正式性能A/B。
完整六组约20分钟；不能缩短默认180秒窗口来冒充跨窗持续流量。

### 6.2 执行与证据

在建立配方A的guest中执行（端口空闲、源码已含本轮工具）；下面保留六组数据，
不会删除既有目录。只跑一组时把label列表改成 `p1-lifetime` 即可：

```bash
vmssh 'sudo bash -se' <<'GUEST'
set -euo pipefail
ulimit -l unlimited
RUN_ID=$(date -u +%Y%m%dT%H%M%S)-$$
FIXED=sustain-$RUN_ID-active
mkdir -p /data/disk{1,2,3}/cases /data/disk1/results
for label in p1-legacy p1-lifetime p2-lifetime p2-legacy p3-legacy p3-lifetime; do
  policy=${label#*-}
  OUT=/data/disk1/results/sustain-$RUN_ID-$label
  python3 /opt/dfkv/deploy/validation/quick_eviction.py \
    --build /opt/dfkv-build --scenario sustained-pressure --policy "$policy" \
    --data-root /data/disk1/cases --data-root /data/disk2/cases \
    --data-root /data/disk3/cases --data-name "$FIXED" --output "$OUT"
  python3 - "$OUT" "$FIXED" "$label" <<'ARCHIVE'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1]);name,label=sys.argv[2:]
r=json.loads((out/'result.json').read_text());case=json.loads((out/'case.json').read_text())
if r['status']!='passed' or r['server_exit']!=0:raise RuntimeError('not cleanly stopped')
expected=[pathlib.Path(f'/data/disk{i}/cases')/name for i in (1,2,3)]
if case['data_dirs']!=list(map(str,expected)):raise RuntimeError('unexpected store paths')
for p in expected:
    archive=p.with_name(name+'--'+label)
    if p.is_symlink() or not p.is_dir() or archive.exists():raise RuntimeError('unsafe archive')
    p.rename(archive)
ARCHIVE
done
GUEST
```

持续模式拒绝额外pressure-delay，负载子进程上限320秒（另有启动30秒/退出15秒），
不能为了延长运行而吞掉超时。完整阶段审计仍须满足6.1：六个退出码不能替代初态一致
和完整post185样本的核对。实际六组supervisor、冻结run.json与完整命令另存本轮证据包，
再次运行必须使用新的RUN_ID，不能覆盖已保留结果。

每步 `trace/steps.jsonl` 及时落盘，result.json保留同一轨迹；11份阶段快照为prefill、
warm、before-pressure、step-040/080/120/160/180/200、after-pressure、after-scan。
持续探测在阶段边界前完成，HTTP指标采集不增加业务热度。仍需核对每次命中字节、
终局、ring、健康、实际淘汰及退出码；缺失不单凭C ABI返回值认定为合法驱逐。

### 6.3 三轮实测结果（2026-11-17）

基线 `80793be` 加本轮工具增量，真实RXE/kernel ring，同一固定路由及默认时间参数。
三轮六组全部完成，**正确性门禁通过不等于lifetime保留效果通过**：

| 轮次 | 先后顺序 | legacy热点命中/缺失 | lifetime热点命中/缺失 | lifetime热点缺失率 |
|---|---|---:|---:|---:|
| 1 | legacy→lifetime | 12928 / 0 | 4537 / 8391 | 64.91% |
| 2 | lifetime→legacy | 12928 / 0 | 4519 / 8409 | 65.04% |
| 3 | legacy→lifetime | 12928 / 0 | 4523 / 8405 | 65.01% |

分母为每组12928次热点探测，含128次严格预热，不是不同对象数。lifetime三轮平均
缺失率64.99%，范围64.91%–65.04%，只是该轨迹的三轮描述，不是置信区间或线上收益。
三轮首次出现热点缺失的步骤均为step52（从0开始），该步骤起点距预填充完成约52.2秒；
这是步骤级定位，不冒充单个GET或victim的精确时刻。

每组185秒后均有15个完整步骤、960次探测：legacy三轮分别960命中/0缺失，
lifetime三轮分别0命中/960缺失。窗口分类相对末次预填充ACK，并非逐key精确年龄；
跨边界的5个步骤单列，不混入post185。等待窗口不是一个暂停：200步持续交错，
实测节拍跨度199.0685–199.0847秒，最大相邻步间隔1.001462秒，小于预先冻结的5秒门槛。
每组总耗时212.19–213.60秒；这些数字只验证运行/节拍，不是GET/PUT分位或吞吐收益。

六组施压前逐盘对象均3181/3122/2913，slot字节一致、淘汰0；每组最终扫描10812命中/
4804缺失，实际水位淘汰4804，后台class回收/extent steal均0。共1200步、66份阶段
指标、171264次业务GET；命中字节、终局分母、对象守恒、健康、ring与正常退出均通过。
没有把合法缺失混成I/O错误，也没有把热点缺失当成数据损坏或把它从结果中删去。

**当前默认参数在这类“持续新冷写、热点不回填”的轨迹下，热点保留明显弱于legacy，
且三轮重复出现。** 默认新写BOOST与首次读撤BOOST提供合理解释，但未逐victim追踪
唯一原因，不改规格或调参来掩盖结果。后续应先讨论这种取舍是否符合目标业务，再
决定是否测试参数候选；不能仅凭功能门禁通过推进默认切换。

这与5.3不矛盾：5.3是在压力开始前静默等待185秒，热点尚未丢失；本节在约52秒即
开始出现缺失，且不回填，故时间跨窗不会令已丢失热点自动复活。本节不覆盖真实业务
回填/重算、高并发独立读线程、多尺寸或正式性能A/B。

工具单测19＋16＝35通过，host OFF与guest ON构建及各两个CTest入口通过（Ninja
无待编译工作）。审计还正确拒绝伪造跨窗原点、已淘汰的prefill和缺少第六组的三类
反例；这不是额外产品用例数。未重跑全量CTest/sanitizer，本轮无生产C++修改。

证据：`.omx/evidence/sustained-retention/20261117t090611z-80793be/`，包括冻结run.json、
逐步JSONL/66份指标、完整supervisor、源码SHA、XML、原始结果包、audit与negative证据。
guest结果在 `/data/disk1/results/sustained-20261117t090611z-80793be/`；各盘数据归档为
`/data/diskN/cases/sustain-20261117t090611z-80793be-active--p{1,2,3}-{legacy,lifetime}`，
准确路径见summary.json。六组均已退出，VM和样本保留，仍只使用原授权宿主虚拟盘文件。

## 7. 保留现有优先原则：P＝180／60／30秒首轮筛选

本轮选择保留“窗口内新写未读优先”的公式和校验，仅在隔离进程显式测试不同P。
默认P仍180秒；不修改BOOST、MAX_READS、KEEP、桶、水位或生产代码。合法参数始终
满足窗口内新写优先，因此调低合法BOOST或增加读档不能直接逆转这一优先关系。

P同时控制新写BOOST窗口和首次早读奖励的截止时间，不是缓存TTL。本轨迹的初始
热点在30秒内已完成预热，三档的初始早读资格相同；但这不量化真实冷数据首次复用
延迟的损失。短窗口可能改善热点保留，也可能减少新写等待首次复用的机会。

工具新增 `--protect-sec {30,60,180}`，只允许lifetime；不指定时为None，既不继承
外部DFKV参数也不覆盖默认P。它是诊断预设，不缩小生产参数的合法范围。
`case.json` 记录覆盖值和实际子进程环境，服务器启动日志须确认生效P来自env；不能
把命令行里写了一个数当作实际配置证明。

### 7.1 复跑

要做固定路由的三组对照，复用6.2的完整运行/归档流程，只替换三处：

1. label列表改为 `p180 p60 p30`。
2. 原policy赋值改为 `policy=lifetime; protect=${label#p}`。
3. 工具调用增加 `--protect-sec "$protect"`，其余固定路径、新输出和正常退出后归档
   检查保留。不要只更换输出路径而省略归档/空初始化。

单组实际命令示例（在guest中，先设好无限memlock）：

```bash
python3 /opt/dfkv/deploy/validation/quick_eviction.py \
  --build /opt/dfkv-build --policy lifetime --scenario sustained-pressure --protect-sec 30 \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases --output /data/disk1/results/p30-$(date -u +%Y%m%dT%H%M%S)-$$
```

单组使用新UUID目录，不能直接拿它与另一UUID目录的结果宣称严格同路由对照。
本轮实际三组复用6.3的相同固定路径，每组空初始化，正常退出后使用新的归档名保留
数据。完整supervisor和冻结run.json在证据包，重复执行须换新的run/output/归档名。

每组继续执行200步、12928次热点读与15616次最终扫描，按1–5秒连续性门禁和全部
字节/健康/终局/ring/退出门禁审计；候选P改变不放宽检查。各档只运行一组，顺序为
180→60→30，用于首轮筛选，不是交替重复验收、最优值选择或正式性能A/B。

### 7.2 首轮筛选结果（2026-11-17）

基线 `22cf638` 加诊断覆盖参数；顺序180→60→30，每档只跑一组。与第6节使用相同
固定路径、seed、初态和200步轨迹；初态逐盘3181/3122/2913对象、slot字节一致，
施压前淘汰0，初始热点均在30秒内完成预热。启动日志确认三组P分别为180/60/30，
来源为env；BASE、BONUS、MAX_READS、BOOST、SLOW、KEEP、G、B逐项核对为原默认值。

| P（秒） | 热点命中/缺失 | 热点缺失率 | 185秒后命中/缺失 |
|---|---:|---:|---:|
| 180 | 4539 / 8389 | 64.89% | 0 / 960 |
| 60 | 10131 / 2797 | 21.64% | 675 / 285 |
| 30 | 12928 / 0 | 0 | 960 / 0 |

每组热点分母12928（含128预热），185秒后为15个完整步骤、960次探测；数字是请求数，
不是唯一对象数。三组均实际水位淘汰4804、末扫10812命中/4804缺失，字节、终局、
健康、ring、对象守恒与正常退出全部通过。600步、33份阶段快照审计通过，最大步间
间隔1.001300秒，总耗时每组约212–213秒，没有用跳时钟或密集追赶缩短验证。

**本轨迹中缩短P改善了热点保留，P30值得进入下一轮候选验证；生产默认仍为180。**
这不是最佳参数或收益放行：只有一轮、固定顺序，而且初态是刚完成预填充的新写池，
并非老冷数据占多数的稳态。没有量化新写首次复用延迟变长时的损失，也没有真实回填。
P60在窗口结束后仍缺失，不说明窗口未撤BOOST；此前被淘汰的热点没有回填，不会自动
回来。不能以本组结果批准改默认或跳过新写首次复用代价、混合尺寸和重复轮次验证。

本轮仅新增工具的显式P预设及对应单测，未改生产C++、评分公式/校验、磁盘格式或
参数默认值。工具单测18＋原驱动19＝37通过，host OFF/guest ON构建及各两个CTest
入口通过（Ninja无待编译工作）。额外四类审计反例均正确拒绝：伪造跨窗原点、错误
prefill初态、缺少第三组、启动日志生效P不符；它们不是额外产品用例数。无全量CTest/
sanitizer或正式性能A/B的新执行。

证据：`.omx/evidence/protect-window-screen/20261117t095539z-22cf638/`，含冻结run.json、
源SHA、启动生效配置、原始指标、supervisor、XML和审计。guest结果在
`/data/disk1/results/protect-screen-20261117t095539z-22cf638/`；数据归档为各盘
`/data/diskN/cases/sustain-20261117t090611z-80793be-active--20261117t095539z-22cf638--p{180,60,30}`。
旧轮次归档未覆盖；三组应用已退出，VM和样本保留，仍只落原授权宿主虚拟盘。

## 8. 首次复用延迟：缩短P的代价检查

`--first-reuse-sec {0,45,90,185}` 是驱动与supervisor的隔离诊断选项，默认0保持
第6/7节原有轨迹；非零仅允许 `sustained-pressure`。它不是生产参数，也不缩短服务器
时钟。不等同于第5节的静默等待：延迟期间仍按一秒一步持续写冷数据。

非零时，两namespace各预填4608对象，将各前32对象作为“等待首次复用”的64对象
集合；**只预热其余9152对象两遍**，严格要求18304次命中，待复用集合不做任何预读。
之后200步每步先写32个新冷对象；从实际压力原点起达到指定延迟，才开始每步读64个
待复用对象，不回填缺失对象，不追赶突发，最终完整扫描15616对象。延迟之前步骤的
cohort读数为0；首读全缺失也是有效风险结果，不可因命中率低删去该组。

`first_reuse` 逐namespace记录首次业务GET起点、命中/缺失及距该namespace cohort
最后一个PUT ACK的实测时间。该年龄不是每个对象的精确写入年龄：前一批16对象更早
写入，因而是以最后批ACK为基准的下界。指定90秒是距压力原点的门槛，**不是**距写入
正好90秒，预填充/预热时间也计入实测年龄。缺失对象仍在原始64对象分母中，避免
只统计幸存对象的首次读间隔。后续总命中率还受首次成功读的早读奖励资格影响，不能
把它全部解释成BOOST窗口的单一效果。

### 8.1 复跑与检查

在第7节相同guest环境中运行；单组示例（不是严格同路由多组对照）：

```bash
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/quick_eviction.py \
  --build /opt/dfkv-build --policy lifetime --scenario sustained-pressure \
  --protect-sec 30 --first-reuse-sec 90 \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/first-reuse-$(date -u +%Y%m%dT%H%M%S)-$$
```

固定路由多组对照仍遵循6.2的固定 `--data-name`、空初始化和正常退出后唯一归档流程；
legacy组去掉 `--protect-sec`，保留相同 `--first-reuse-sec 90`。不能复用已有数据目录，
不能将不同目录哈希导致的路由变化当成纯参数差异。建议先单轮P180→legacy→P60→P30，
再决定是否扩展45/185秒和交替重复；工具支持某预设不表示该预设已实测。

检查 `trace/steps.jsonl`、`first_reuse`、阶段指标和 `case.json`。总业务GET分母应为
18304次filler预热＋实际cohort探测次数＋15616次末扫，不能沿用12928的原热点分母。
审计要求200步、1–5秒连续性、延迟前无cohort GET、首读时刻/年龄一致、首次64对象
结果与首个探测步骤守恒，以及原字节/终局/健康/真实ring/正常退出门禁。首读之后的
重复缺失不是多个唯一对象损失，也不能证明此前被淘汰的对象会在跨窗时自动恢复。

### 8.2 90秒首次复用首轮实测（2026-11-17）

基线 `8b690fa` 加本轮工具/单测；顺序P180→legacy→P60→P30，各一组，非交替重复。
延迟90秒从压力原点计，实际首次读取距cohort最后批PUT ACK约103.23–106.82秒。
四组在施压前逐盘3181/3122/2913对象、slot字节一致，淘汰0；filler预热均18304次
命中、待复用集合预读0。三档lifetime启动日志确认生效P来自env，其余八项评分/桶
配置保持默认；legacy不传P覆盖值。

| 策略/候选P | 首次64对象命中/缺失 | 后续含首读的7040次探测命中/缺失 |
|---|---:|---:|
| lifetime / 180 | 64 / 0 | 7040 / 0 |
| legacy | 0 / 64 | 0 / 7040 |
| lifetime / 60 | 0 / 64 | 0 / 7040 |
| lifetime / 30 | 0 / 64 | 0 / 7040 |

每组110个实际探测步骤，最后扫描10812命中/4804缺失、水位淘汰4804，后台class回收/
extent steal为0；每组总GET40960（18304＋7040＋15616）。四组共800步、44份阶段
快照，最大步间1.000524秒，实际运行约15分钟。字节、终局、对象守恒、健康、真实
io_uring、正常退出均通过。缺失组没有回填，所以7040次缺失不是7040个唯一对象损失。

**已观察到短P的反向代价：此前改善已预热热点保留的P30/60，在本轮等待首次复用的
64对象上全部缺失，而P180全部保留。** 这是两种合成轨迹中的明确取舍，不支持直接
把默认P从180改成30，也不证明180是全业务最佳值。该场景只有一个90秒门槛、固定
顺序、一组样本和4KiB尺寸；当轮45/185秒仅有工具单测（后续实测见第9节），混合延迟分布、
回填、多尺寸和交替重复仍未覆盖，不关闭正式A/B或O5验收。

驱动23＋supervisor22＝45单测通过，无失败/跳过；host OFF `build-test` 和guest ON
`/opt/dfkv-build` 构建成功（Ninja无待编译工作），各两个CTest入口通过。独立代码
审查修复一处“跳过判定后记录时间跨过截止点”的误拒绝，并加边界回归；最终校验器
重新核对四组原始结果通过。运行中的驱动未改变，证据保留运行初始和最终源码SHA。
额外五类审计负控均拒绝：提前探测、伪造年龄、缺组、预读待复用集合、缺filler分母；
它们不是新增产品场景。未重跑全量CTest、sanitizer或物理HCA/GPU验证；无生产C++修改。

证据：`.omx/evidence/first-reuse/20261117t102034z-8b690fa/`，包含冻结计划/配置、
原始日志/指标/逐步轨迹、源码SHA、supervisor、实际命令和审计。guest结果路径沿用
原supervisor的前缀：`/data/disk1/results/protect-screen-20261117t102034z-8b690fa/`。
数据归档为各盘 `/data/diskN/cases/sustain-20261117t090611z-80793be-active--20261117t102034z-8b690fa--{p180,legacy,p60,p30}`。
旧数据未覆盖；应用已退出，VM和样本保留，仍仅落原授权三块宿主盘的虚拟盘文件。

## 9. 45／185秒首次复用与正反序重复

本轮复用第8节已提交的工具，不修改生产或诊断运行时代码。冻结16组清单：先45秒
正序、185秒正序，再45秒反序、185秒反序。正序为P180→legacy→P60→P30，反序为
P30→P60→legacy→P180。每个延迟/配置两次，独立空初始化；90秒沿用8.2的历史结果，
**本轮未重跑90秒**。这是顺序平衡的两轮合成诊断，不是随机化、三轮统计或正式A/B。

仍使用同一固定数据目录字符串、seed、容量、尺寸、预填充、filler预热与200步轨迹；
每组正常退出、端口空闲后才将本轮目录改为唯一归档名，禁止覆盖旧样本或复用恢复
初态。每组开始前检查三盘至少6GiB空闲。冻结计划、实际命令/环境、逐步轨迹与阶段
指标必须保留，不能仅保留平均命中率；任何门禁失败都保留并报告，不删掉失败组。

### 9.1 复跑边界

单组命令沿用8.1，仅替换 `--first-reuse-sec 45` 或 `185`。完整对照须使用6.2的
固定 `--data-name`、新输出、空初始化和唯一归档流程；不能把不同UUID路由的结果
当作严格同路由对照。16组实际清单及supervisor随本轮证据包保存，重跑需换新的
run/output/归档标识。原样复跑前检查路径和磁盘标记，切勿重新格式化保留的虚拟盘。

延迟仍从**压力原点**计，不是精确的单对象写入年龄。尤其45秒加上预填充/预热耗时，
实际首读可能落在P60边界附近；须逐namespace列出实际末批PUT ACK到首GET的年龄，
不能看到“45<60”就断言全部对象获得早读奖励。客户端ACK年龄是下界，也不是服务器
内部每个resident的资格判定证据；不能由它单独反推每个受害者的唯一淘汰原因。

每组业务GET分母为18304次严格filler预热＋实际cohort探测＋15616次末扫。按实际
时间决定何时开始探测，未强制把所有延迟套用7040次；连续性仍为1–5秒，至少10个
完整探测步骤，首次64对象结果与首个探测步骤守恒。保留缺失对象在分母中，后续重复
缺失不重复计作唯一对象损失；无回填，也不表示跨过P后对象会重新出现。

### 9.2 实测结果（2026-11-17）

基线 `2f39a89`，16组全部实际执行。每个表格单元是**每组首次64对象的命中/缺失**；
对应正反两组的结果一致，不把两组相加冒充更大的独立对象集合。

| 压力原点后首次探测门槛 | lifetime P180 | lifetime P60 | lifetime P30 | legacy |
|---|---:|---:|---:|---:|
| 45秒，正/反各一组 | 64 / 0 | 64 / 0 | 64 / 0 | 64 / 0 |
| 185秒，正/反各一组 | 0 / 64 | 0 / 64 | 0 / 64 | 0 / 64 |

45秒组实际末批PUT ACK到首GET为60.061551–62.410632秒，每组155个探测步骤、
9920次探测全部命中。185秒组实测199.477109–202.769010秒，每组15步、960次
探测全部缺失。前者不能被称为“实际首读年龄45秒、必在P60内”，后者也不表示
P180保证任意对象至少存活180秒：P是软评分窗口，不是TTL或pin。

16组施压前逐盘均3181/3122/2913对象，slot字节一致，初始淘汰0；filler预热严格
18304次命中、cohort预读0。每组末扫10812命中/4804缺失、水位淘汰4804，后台
class回收及extent steal均0。三档生效P来自env，其余八项评分/桶配置保持默认。
每个45秒组业务GET43840、每个185秒组34880，合计629760次；不能混用原90秒的
40960分母。共3200步、176份阶段快照，最大相邻步间隔1.001349秒，实测总耗时
3624.524秒（约60分钟），没有跳时钟、压缩时间或丢弃缺失组。

全部字节、健康、终局、真实RXE/kernel io_uring、对象守恒及正常退出检查通过。
额外八类审计负控正确拒绝：缺少反序组、标签重复、延迟不符、生效P不符、伪造年龄、
预读cohort、初态丢对象、JSONL被改；它们不是额外产品场景。

**本轮顺序反转未改变45/185秒两端结果；结合8.2的90秒历史结果，保护窗口的取舍
仍成立，但没有得出全业务最佳P。** 90秒只保留上一轮单组证据，不能称为本轮反序
重复通过。这里没有混合首次复用分布、业务重算/回填、多尺寸或独立高并发读流量，
也没有正式A/B、吞吐/p99或生产放行。生产默认P仍180，默认策略未切换。

本轮只更新验证文档与验收索引，未修改运行时代码或新增伪单测；重新执行既有
驱动23＋supervisor22＝45单测，无失败/跳过。host OFF `build-test` 与guest ON
`/opt/dfkv-build` 构建成功（Ninja无待编译工作），各两个CTest入口通过。125个生产
源码文件、四个工具/测试文件SHA与guest一致，服务器/客户端二进制SHA前后未变。
未重跑全量CTest、sanitizer、C++静态检查或物理HCA/GPU验证。

证据：`.omx/evidence/first-reuse-repeat/20261117t110513z-2f39a89/`，含运行前冻结
计划/逐组清单/源码SHA、完整supervisor、逐步结果/指标、命令、XML及审计。
guest结果：`/data/disk1/results/first-reuse-repeat-20261117t110513z-2f39a89/`。
三盘样本分别归档到 `/data/diskN/cases/sustain-20261117t090611z-80793be-active--20261117t110513z-2f39a89--d{45,185}-{forward,reverse}-{p180,legacy,p60,p30}`；
完整实际路径见summary.json。旧样本未覆盖，应用均已退出，VM和样本保留，仍仅落
原授权三块宿主盘的虚拟盘文件。

## 10. 已有热点＋不同首次复用延迟的混合首筛

新增 `mixed-pressure`，仅为外部诊断场景，不修改生产C++、评分公式/校验或默认值。
本轮先比较P180→legacy→P60→P30各一组，固定同路径、seed、容量和空初始化；不是
交替重复、正式性能A/B或全业务最佳参数选择。

### 10.1 冻结轨迹与分母

每namespace预填4608对象，布局固定为：

| 索引区间（左闭右开） | 用途 | 预热及施压期间读取 |
|---|---|---|
| `[0,32)` | 已有热点 | 严格预热两遍，随后每步读取 |
| `[32,48)` | 45秒待复用组 | 不预热，到期首次读取一次 |
| `[48,64)` | 90秒待复用组 | 不预热，到期首次读取一次 |
| `[64,80)` | 185秒待复用组 | 不预热，到期首次读取一次 |
| `[80,4608)` | 背景filler | 严格预热两遍，施压期间不再读 |

先完成全部PUT，再严格预热已有热点两遍（128次GET），随后严格预热filler两遍
（18112次GET）。三组待复用对象共96个，不能预读。压力阶段200步，每步按真实
一秒节拍先冷写32对象，再读已有热点64对象，最后检查三组是否首次到期；达到实际
压力原点45/90/185秒的组仅首次读取一次，每组32对象，之后直到末扫不再探测。
不回填缺失对象、不追赶突发、不使用独立并发读线程；末扫15616对象。

业务GET分母固定为 **46752＝12928热点（含128预热）＋18112 filler预热＋96首次
复用＋15616末扫**。热点是重复请求分母，每组首次复用是32个对象分母，必须分别
报告命中/缺失，不能用综合平均率掩盖一类对象损失。首次复用全缺失是有效风险结果；
字节错误、I/O失败、严格预热缺失、健康/终局或退出异常仍为验证失败。

待复用对象是本轮**新鲜预填充的住户**，不是压力期间持续到达的新对象分布。
延迟从压力原点算，逐namespace保留末批PUT ACK到实际首GET的年龄；不是每对象精确
写入年龄或服务器内部早读资格的直接记录。相比第8/9节，还改变了预热集合和待复用
探测频率，不能将跨场景差异全部归因于“加入混合延迟”。

每组仍检查200步、1–5秒连续性、11份阶段快照、cohort无提前/重复/遗漏/索引重叠、
每namespace16对象结果与实际年龄、PULL及计热完整分母，以及字节/对象守恒/健康/
真实ring/正常退出门禁。失败时保留此前完整步骤和已完成namespace的首次读取记录，
不将部分失败轨迹伪装成完整46752次GET的成功结果。

### 10.2 复跑

在原隔离guest中，单组示例：

```bash
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/quick_eviction.py \
  --build /opt/dfkv-build --policy lifetime --scenario mixed-pressure --protect-sec 180 \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/mixed-$(date -u +%Y%m%dT%H%M%S)-$$
```

三组延迟固定，不能再指定非零 `--first-reuse-sec` 或 `--pressure-delay-sec`。
legacy组去掉P覆盖。完整四组比较须保留6.2的固定 `--data-name`、新输出、空初始化、
正常退出后唯一归档及端口/空间检查；不能把默认UUID路径的不同路由当作纯配置差异。
实际清单、supervisor和审计脚本随证据包保存，重跑时换新的run/output/归档标识，
不要覆盖或删除旧样本，也不要重新格式化保留的虚拟盘。

### 10.3 首轮实测（2026-11-18）

基线 `0f966fb` 加本轮工具/单测；顺序P180→legacy→P60→P30，各一组，未反序重复。
热点列为12928次重复请求（包含128次预热）；每个首次复用列是**32个对象**的单次
结果，不沿用第8/9节的64对象分母。

| 配置 | 已有热点命中/缺失 | 45秒首次命中/缺失 | 90秒首次命中/缺失 | 185秒首次命中/缺失 |
|---|---:|---:|---:|---:|
| lifetime P180 | 12928 / 0 | 32 / 0 | 32 / 0 | 0 / 32 |
| legacy | 12928 / 0 | 32 / 0 | 0 / 32 | 0 / 32 |
| lifetime P60 | 12928 / 0 | 32 / 0 | 0 / 32 | 0 / 32 |
| lifetime P30 | 12928 / 0 | 32 / 0 | 0 / 32 | 0 / 32 |

45/90/185秒组的实际末批PUT ACK到首GET年龄范围分别为59.993764–62.270772、
104.986968–107.274848、200.006158–202.285420秒。45秒组仍处于实际P60边界附近，
不能仅根据名义延迟判断服务器内每个对象的早读资格。

施压前四组逐盘3181/3122/2913对象，slot字节一致，淘汰0；严格预热共18240次GET，
待复用集合无预读。每组末扫10812命中/4804缺失、水位淘汰4804，后台class回收/
extent steal均0。各档P生效与其余八项评分/桶默认字段核对通过。四组实际GET共
187008次、800步、44份阶段快照；最大相邻步间隔约1.010252秒，总耗时908.140秒
（约15分钟）。字节、终局、计热分母、对象守恒、健康、真实RXE/kernel io_uring及
正常退出全部通过，没有将首次复用缺失过滤成“未参与对象”。

**在本轮冻结轨迹中，P180多保留了90秒待复用组，且未观察到已有热点的命中损失；
缩短P没有带来热点命中收益。** 这不与第6/7节矛盾：那里的初始背景池包含大量新写
未读对象；本轮背景filler已严格预热两遍，待复用组的数量、索引和探测频率也改变。
背景竞争状态不同，不能假装是相同负载或将跨场景差异全部归因于P；没有逐victim
追踪唯一淘汰原因。结果再次说明参数收益依赖负载，不能从单个合成场景选择全业务
最优P或批准默认切换。生产P仍180，默认策略、评分公式/校验、磁盘格式均未修改。

本轮新增5个驱动和4个supervisor单测方法，工具单测28＋26＝54通过，无失败/跳过；
覆盖无预读/重叠、一次性实时时机、停顿跨阈值、不回填、严格预热缺失、字节/I/O
错误及部分namespace失败证据，以及伪造事件/年龄/分母拒绝。主机OFF `build-test`
和guest ON `/opt/dfkv-build` 构建成功（Ninja无待编译工作），各两个CTest入口通过。
125个生产源码与guest一致，服务器/客户端二进制SHA前后未变；四个工具/测试文件
SHA与guest一致。独立代码审查无遗留问题。七类额外审计负控均拒绝：提前/重复复用、
假年龄、索引重叠、错误复用分母、缺组、预热计数被改；它们不是额外产品用例。
未重跑全量CTest、sanitizer、C++静态检查、物理HCA/GPU或正式性能A/B。

仍未覆盖持续新到达对象的混合延迟分布、业务权重/重算/回填、多尺寸、独立高并发
读线程或交替重复。只比较了本轮明确的初态和一次性复用，不能声称整体O5验收完成。

证据：`.omx/evidence/mixed-retention/20261118t021429z-0f966fb/`，含冻结计划/清单、
源码SHA、完整supervisor、原始指标/逐步轨迹、命令、XML和审计。
guest结果：`/data/disk1/results/mixed-retention-20261118t021429z-0f966fb/`。
数据保留在 `/data/diskN/cases/sustain-20261117t090611z-80793be-active--20261118t021429z-0f966fb--{p180,legacy,p60,p30}`；
准确路径见summary.json。旧数据未覆盖，应用已退出，VM和样本保留，仍只落原授权
三块宿主盘的虚拟盘文件。

### 10.4 混合场景反序确认（2026-11-18）

复用已提交的 `6b0cc86` 工具，未修改生产或诊断运行时代码；本轮新跑
**P30 → P60 → legacy → P180** 四组。对照10.3的历史正序四组，本轮没有重新运行
正序。每配置累计正/反各一组，不称为多轮随机化实验或正式性能A/B。

保持相同固定数据路径、seed、二进制、容量/尺寸、预填充与预热、200步真实节拍、
三档一次性首次复用及不回填策略。每组空初始化，正常退出且端口释放后唯一归档，
不覆盖旧数据。逐配置完整trace配置及服务端参数/环境一致，仅客户端输出路径改变；
正反两轮逐盘3181/3122/2913对象及slot字节相同，初始淘汰均0。

| 反序配置 | 已有热点命中/缺失 | 45秒首次命中/缺失 | 90秒首次命中/缺失 | 185秒首次命中/缺失 |
|---|---:|---:|---:|---:|
| lifetime P30 | 12928 / 0 | 32 / 0 | 0 / 32 | 0 / 32 |
| lifetime P60 | 12928 / 0 | 32 / 0 | 0 / 32 | 0 / 32 |
| legacy | 12928 / 0 | 32 / 0 | 0 / 32 | 0 / 32 |
| lifetime P180 | 12928 / 0 | 32 / 0 | 32 / 0 | 0 / 32 |

**本次反序与历史正序的热点、首次复用和末扫命中/缺失结果逐配置相同，未观察到
这些结果随运行顺序改变。** 不能由每配置两次观测证明所有运行均顺序无关，也没有
据此作延迟/吞吐性能等价性判断。P180仍是本冻结混合轨迹唯一保住90秒组的配置，
且没有观察到热点损失；不推导全业务最佳P，不翻生产P180或默认策略。

反序45/90/185秒组实际末批PUT ACK到首GET年龄范围分别为60.673752–62.797168、
105.668428–107.799532、200.679968–202.816139秒；名义延迟不是对象实际写龄，
不能用“45秒”直接证明处于P60内。每组末扫10812/4804、水位淘汰4804，
后台class回收/extent steal均0。新四组共187008次业务GET、800步、44份阶段快照，
最大步间隔1.001388秒，总耗时908.593秒；分母仍为10.1的46752/组。
字节、完整PULL终局/计热分母、对象守恒、健康、真实RXE/kernel io_uring和正常退出
门禁全部通过；命中结果不是硬编码的放行条件，全缺失仍按实际结果保留。

既有工具单测28＋26＝54本轮重跑通过，无失败/跳过；没有新增或修改代码，因此
不新增测试方法。主机OFF `build-test` 与guest ON `/opt/dfkv-build` 均构建成功
（Ninja无待编译工作），各两个Python CTest入口通过。125个生产源码及4个工具/
测试文件SHA与guest一致，二进制SHA前后不变。八项审计负控拒绝提前/重复复用、
假年龄、索引重叠、错误复用分母、缺组、预热计数改写及错误顺序；负控不是额外
产品场景。未重跑全量CTest、sanitizer、C++静态检查、物理HCA/GPU或正式性能A/B。

实际命令：
```bash
python3 -m unittest discover -s test/python -p test_disk_validation.py -v
python3 -m unittest discover -s test/python -p test_quick_eviction.py -v
cmake --build build-test -j2
ctest --test-dir build-test --no-tests=error \
  -R '^python_(disk_validation|quick_eviction)$' --output-on-failure
```

guest构建/CTest使用 `/opt/dfkv-build`，负载复用10.2入口及冻结supervisor，
逐组具体命令见证据的 `results/<配置>/case.json`。证据目录：
`.omx/evidence/mixed-retention-reverse/20261118t024812z-6b0cc86/`，
包含冻结清单、原始结果/指标/步骤、正反对照、SHA、测试XML与独立终审。
guest结果：`/data/disk1/results/mixed-retention-20261118t024812z-6b0cc86/`。
数据目录：
`/data/diskN/cases/sustain-20261117t090611z-80793be-active--20261118t024812z-6b0cc86--{p30,p60,legacy,p180}`。
应用/测试已退出，VM及样本保留，三盘各约15GiB可用；仅使用原授权虚拟盘。

该混合场景的正反序确认已完成；持续新到达对象、业务权重/回填、多尺寸、
独立高并发、正式A/B及整体O5验收仍未关闭。

## 11. 持续新到达对象：早段抽样与真实后端首筛

第10节待复用对象都在施压前预填充，不能代表施压期间新到达的对象。本小步新增
外部客户端场景 `arrivals-pressure`。驱动准备步骤（`f6cbdb7`）仅有无硬件回归，
当时尚未接入supervisor；本轮已接入自动校验并运行真实四配置对照，详见11.3。
两个步骤均未修改生产代码、评分、参数校验或默认配置，不将内存测试当作磁盘验证。

### 11.1 冻结的早段抽样口径

- 每namespace初始预填4608个对象；已有热点 `[0,32)` 严格预热两遍，其余
  `[32,4608)` filler严格预热两遍。没有初始待复用组。
- 持续200步，每步真实一秒节拍，新写16对象/namespace，再读已有热点64对象。
  无追赶突发、缺失回填或独立读线程；每个新key只写一次。
- 只标记**前12步**新到达的批次（step 0–11），延迟按45/90/185秒轮转：
  每档4组，每组16对象/namespace，共384个对象。后188步仍持续新写，
  但不纳入首次复用抽样。这不是全部到达对象或稳态到达分布的完整覆盖。
- 延迟从该组两个namespace中**较晚的PUT ACK**开始算，不从压力原点算。
  到期后每组只读取一次；保留admission step、索引、两个ACK时间、
  实际首次GET时间和逐namespace写龄/命中/缺失。相同delay的不同组不能合并身份。
- 不另加停写排空尾部来强行等待存活；200步结束若任何抽样组尚未完成首次读取，
  明确失败并保留部分证据，不过滤未完成组或声称获得完整384个对象结果。
- 最后全扫15616对象。成功完整轨迹业务GET分母应为
  **47232＝12928热点（含128预热）＋18304 filler预热＋384首次复用＋15616末扫**。
  首次复用按每档128个不同对象统计，热点按重复请求统计，不能混用分母。
  首次全缺失是有效观测；字节错误、I/O失败和严格预热缺失不是有效淘汰结果。

驱动准备步骤只锁定以上工具行为；后续supervisor接入及真实结果见11.3。不能套用
第10节命中表；业务权重、重算/回填、多尺寸、独立高并发和正式A/B仍分别后续处理。

### 11.2 驱动准备验证记录（f6cbdb7，2026-11-18）

新增5个测试方法，驱动33项及既有supervisor 26项单测全部通过，无失败/跳过。
新增场景在实现前的红测因CLI尚不识别该值而失败；实现后使用MemoryClient与
假时钟，正常用例完整执行4608预填＋200步＋7808对象/namespace，实际47232次GET。
覆盖两个namespace ACK不同、一次性/无预读/无回填、停顿不追赶、首次全缺失、
严格预热缺失、字节损坏、PUT/GET I/O失败、部分namespace成功及未完成轨迹失败。
这证明驱动契约，不证明真实磁盘淘汰结果。

独立审查无遗留问题，并重跑59项单测；相同MemoryClient/假时钟下直接对照
`ef61e35` 的旧mixed/sustained/quick驱动，完整trace及结果逐项一致。
主机OFF `build-test`、guest ON `/opt/dfkv-build` 的构建及两个Python CTest入口
均通过（Ninja无待编译工作）；guest同步4个工具/测试文件SHA核对，125个生产
源码保持一致，二进制SHA前后未变。未跑全量CTest、sanitizer或新的真实RXE/
io_uring负载，也没有新增真实后端放行结论。Python编译及diff检查通过。

单元测试及构建命令沿用10.4；本轮驱动单测33项，不再是此前28项。
证据：`.omx/evidence/arrivals-driver/20261118t031026z-ef61e35/`，
包括红/绿测试日志、完整规模回归、旧场景等价性、主机/guest CTest XML及审查报告。

### 11.3 自动校验接入与四配置实测（2026-11-18）

基线 `f6cbdb7`，本轮只修改supervisor及对应测试，未改驱动/生产代码或默认配置。
独立 `check_arrivals` 校验12组身份/固定索引、ACK位于对应admission step且两
namespace有序、较晚ACK起算due、实际逐namespace年龄、事件/namespace先后关系、
每组恰好一次及384个对象分母。两个组拥有相同delay不代表同一身份。
跳过到期组按row开始时间判断，避免将决策后跨deadline的行尾错误判为提前遗漏。

CLI现在支持 `--scenario arrivals-pressure`，固定7808对象/namespace和320秒客户端
超时，拒绝非零单次延迟/预压等待；完整PULL终局、计热账按47232次GET核对。
全首次缺失或施压阶段热点缺失可以是有效保留性结果；严格预热缺失、字节/I/O、
缺组/假年龄/分母错误、健康、ring fallback及非正常退出仍然失败。

在原隔离guest中，单组复跑：
```bash
ulimit -l unlimited
python3 /opt/dfkv/deploy/validation/quick_eviction.py \
  --build /opt/dfkv-build --policy lifetime --scenario arrivals-pressure --protect-sec 180 \
  --data-root /data/disk1/cases --data-root /data/disk2/cases \
  --data-root /data/disk3/cases \
  --output /data/disk1/results/arrivals-$(date -u +%Y%m%dT%H%M%S)-$$
```

legacy去掉P覆盖；完整四组须沿用6.2的固定新空 `--data-name`、唯一输出/归档、
正常退出、端口与空间检查。默认UUID示例不保证跨组路由一致，不能直接当纯参数对照。

实际顺序 **P180 → legacy → P60 → P30**，各一组，无反序/随机化重复。
热点列为12928次重复GET（含128预热）；每档首次复用是4组共**128个不同对象**，
不同于第10节每档32个对象，分母不能混用。

| 配置 | 已有热点命中/缺失 | ACK后45秒首次命中/缺失 | ACK后90秒首次命中/缺失 | ACK后185秒首次命中/缺失 |
|---|---:|---:|---:|---:|
| lifetime P180 | 12928 / 0 | 128 / 0 | 128 / 0 | 0 / 128 |
| legacy | 12928 / 0 | 128 / 0 | 128 / 0 | 128 / 0 |
| lifetime P60 | 12928 / 0 | 128 / 0 | 0 / 128 | 0 / 128 |
| lifetime P30 | 12928 / 0 | 115 / 13 | 0 / 128 | 0 / 128 |

实际逐namespace ACK到首GET年龄范围：
45秒组45.040714–45.057602秒，90秒组90.045760–90.064328秒，
185秒组185.067123–185.078544秒。这次是真实写入ACK相对延迟，不是之前的
压力原点相对延迟。P30的13个45秒缺失集中在最后一个45秒组（admission step 9），
该组19命中/13缺失；前三个45秒组均32命中。保留全部12组明细，不用合并率隐藏差异。

四组初态逐盘3181/3122/2913对象、slot字节相同、淘汰0；严格预热18432次GET。
每组末扫10812命中/4804缺失、水位淘汰4804，后台class回收/extent steal均0。
P覆盖及八项评分/桶默认字段核对通过。四组共188928次业务GET、800步、44份阶段
快照，最大步间隔1.000260秒，总耗时910.645秒。字节、终局/计热分母、对象守恒、
健康、真实RXE/kernel io_uring、正常退出均通过。没有删除全缺失样本或将其算作skip。

**本冻结轨迹中，P180的185秒首次复用保留性低于legacy；缩短P又损失90秒组，
P30还损失部分45秒对象，但没有观察到已有热点收益。** 这是需要保留和复测的
负载相关风险，不证明唯一victim原因或全业务策略优劣。相比第10节，本轮不仅改变
对象到达时机，还改变filler预热集合、抽样数量/索引及延迟原点，不能伪装单变量
跨场景比较。没有逐victim原因追踪，不将差异直接命名为代码错误或已证实的根因。

新增6个supervisor测试方法，驱动33＋supervisor32＝65项本轮重跑通过，无失败/
跳过；包含34类身份/ACK/年龄/事件/计数篡改、同row多组到期的身份/namespace顺序、
跨deadline有效边界、全缺失及完整supervisor模拟路径。红测留档。独立代码审查
无遗留，并把真实默认规模MemoryClient驱动输出直接接入新校验器联核通过。

主机OFF `build-test`、guest ON `/opt/dfkv-build` 均构建成功（Ninja无待编译工作），
各两个Python CTest入口通过。125生产源码及4个工具/测试文件SHA与guest一致，
服务器/客户端二进制SHA前后未变。九项独立审计负控全部拒绝：缺配置、重复身份、
假ACK、假年龄、提前探测、索引重叠、错误复用分母、错误PULL分母和遗漏事件。
负控脚本初版因假设指标不带label而在fixture断言停止，修正为实际uring label后
九项重跑通过；保留初次日志，未改产品门禁或实测结果。负控不是额外产品场景。
未重跑全量CTest、sanitizer、C++静态检查、物理HCA/GPU或正式性能A/B。

实际单测/构建命令：
```bash
python3 -m unittest discover -s test/python -p test_disk_validation.py -v
python3 -m unittest discover -s test/python -p test_quick_eviction.py -v
cmake --build build-test -j2
ctest --test-dir build-test --no-tests=error \
  -R '^python_(disk_validation|quick_eviction)$' --output-on-failure
```

guest构建/CTest使用 `/opt/dfkv-build`；四组实际命令/环境逐组保存在
`results/<配置>/case.json`。证据：
`.omx/evidence/arrival-validation/20261118t032854z-f6cbdb7/`，
含冻结清单、源码SHA、supervisor、原始指标/逐步轨迹、XML、负控与独立终审。
guest结果：`/data/disk1/results/arrival-retention-20261118t032854z-f6cbdb7/`。
数据目录：
`/data/diskN/cases/sustain-20261117t090611z-80793be-active--20261118t032854z-f6cbdb7--{p180,legacy,p60,p30}`。
应用及测试均退出，VM与样本保留，三盘各约15GiB可用，旧数据未覆盖。

仅完成前12批早段新到达抽样的单轮四配置首筛；还需反序/重复确认及更晚到达、
业务权重/回填、多尺寸、独立高并发和正式A/B。生产P180、策略默认、公式/校验及
磁盘格式均未改变，O5未整体验收完成。
