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
