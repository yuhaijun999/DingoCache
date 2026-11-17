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
