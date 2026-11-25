# 前缀链真实 RDMA 回放与双策略正确性验证

基线 `a7934cf`；阶段 **AB7：正式 A/B 前业务收益诊断**。
本轮承接[动态离线模型](DISK_LIFETIME_PREFIX_DYNAMIC_VALIDATION.md)，不改变生产算法、
默认参数、ABI 或磁盘格式，不启动正式 A/B，不以测试结果直接放行生产。

## 范围与口径

真实数据路径仅使用隔离 Ubuntu guest 的 RXE＋io_uring，RAM 关闭、disk ACK、
重试 0；不测 TCP，不测 GPU。RXE 回环不代表真实 HCA 跨主机性能。
HTTP metrics/SSH 只用于控制与核对，不承载业务 KV 流量。

真实 GET 通过状态接口可靠区分 not-found 与 I/O/网络失败，并校验返回长度和内容。
从链首依次 GET，首个缺失处停止；读取成功的连续前缀只叫 **usable-prefix proxy**，
不是推理引擎实际采用 token、重算节省或 TTFT 收益，也不强制修改真实批量客户端。
本轮是直接 GET 的单阶段代理，没有额外 EXISTS 或引擎 LOAD 采用证据。底层 Client
逐字节校验后，账本记录其已验证内容的摘要，不是带签名/认证的原始数据抓包。
链身份以有序 object_ids 表达，固定单 namespace、generation=0；不宣称验证多代际
覆盖或跨 namespace 行为。调用起止单调时间仅供序列/证据核验，不给出性能验收结论。

原始读取先冻结，随后才进行可选后缀回填；回填及额外读取不得增加首次请求收益。
初始化、预热、正式测量、回填分别记录，同时全部纳入 native GET/PUT/字节对账。
业务请求和目标 token 分母由固定计划决定，失败/未知/未尝试不可悄悄删除。
正常可捕获失败会保留部分原始账和后续未尝试请求；进程被超时强杀时不虚构内存中
尚未落盘的账，保留冻结计划、进程终局与日志并判失败，不能报零错误或核算通过。

使用同一个冻结负载和同一构建，独立空目录运行 legacy/lifetime，再反序运行。
这是有界正确性及诊断对照，不是正式统计显著性或生产策略胜出证明。
不使用 FIFO fake 的回收顺序去要求真实策略；真实结果来自实际状态与原始服务指标。

## 安全边界

运行前重新核实 QEMU PID/starttime/UID/网络命名空间及六个 backing FD 的完整绑定，
确认所有 guest 盘实际只落在宿主 `/mnt/disk2`、`/mnt/disk3`。
宿主 `/mnt/disk1`、`/dev/nvme1*` 继续禁止压力和 SMART 采样。

仅采宿主 nvme2/nvme0，约 10 秒一次；任一 SMART warning、温度 >=65°C 或
监控陈旧 >=25 秒即停止。65°C 是本诊断保守停止线，不是厂家硬件极限。
进程预算、异常退出和精确子进程回收保持 fail-closed；不全局 kill、不删在用镜像。

## 计划、复跑与结果

### 工具与冻结负载

- `deploy/validation/prefix_rdma.py`：generate/run/check、严格计划、原始调用账、
  状态接口和字节校验；checker 重用请求状态机，不是独立执行证明。
- `deploy/validation/run_prefix_rdma.py`：只允许专属 guest、三个独立挂载盘和新目录，
  同构建/计划默认 AB→BA，逐 case 校验启动配置、空初态、native 指标和有界回收。
- 对应 `test/python/test_prefix_rdma*.py`：fake 单测不会冒充真实 RDMA；真实 runner
  必须拒绝 `fake-rdma-test`，只接受 `rdma`。

本轮固定 seed `0xD1A60071`，12,288 次初始化 PUT（填充对象在前，12 个共享热点
对象在后），两条完整链预热；随后 8 步，每步 1,024 个新对象，并读取热点长链、
共享分支、短链、新链首次/重访以及上一批新链。长链 8 块，短链 2 块；每个对象
4,096 字节，token 权重按对象序号循环 8/16/24/32。长短链不是多尺寸 slab 验证。
这是有限串行交错，无线程并发、固定到达速率或延迟复用时长保证。

最后构造三个独立新链，分别提前写入 0/4/7 块，确保首/中/尾缺口，再逐项回填及
重访。首次边界必须为 0/4/7、可用 token 必须为 0/80/128；回填 ACK 分别 8/4/1，
重访按实际 GET 结果报告完整/部分/缺失，不要求一律 160 token：容量已满时，
后缀回填可能回收此前采用的前缀。它们验证真实短路和首次收益冻结，不靠压力
偶然产生初次缺口，也不把“ACK 成功”误解成“之后整链持续驻留”。

固定计划 20,546 动作，**53 个测量业务请求、7,392 个目标 token**；初始化、预热
不进入业务分母，但全部进入 native 对账。所有策略与顺序使用同一计划摘要：
`39c629efdd9216bc5322db4ab1268343f8dd969f4a5a6372967dcc39fbd233ee`。

逻辑总容量 12 GiB（每 guest 盘 4 GiB），沿用压缩容量几何，名义 12,288 个 4 KiB
class slot，即约 48 MiB 驻留 payload，**不是 12 GiB 实际业务数据或线上规模**。
watermark/background/donor 关闭，本轮只验 class 压力；保留生产默认 lifetime 参数。
300 秒每 client、宿主全操作 1,800 秒上限，异常不转成 miss，不为收益重试或调参。
本轮仅因验证器错误假设进行 R1 修订，原始失败与修订后运行全部保留，见下文。

### 可复跑入口

先重新建立宿主 backing 六 FD、当前 PID/starttime 和温控监控证据，不能直接沿用
旧 PID、旧温度或本轮 pass。构建新源码并核验 manifest、RXE ACTIVE、io_uring=0、
初态端口空闲；下列命令只能在已被宿主保护器监护的专属 guest 内执行。
每轮换新 TAG，已有目录拒绝，不清理后复用：

```bash
TAG="prefix-$(date -u +%Y%m%dT%H%M%SZ)"
sudo -n prlimit --memlock=unlimited -- python3 /opt/dfkv/deploy/validation/run_prefix_rdma.py \
  --build /opt/dfkv-build \
  --data-root /data/disk1/cases --data-root /data/disk2/cases --data-root /data/disk3/cases \
  --output "/data/disk1/results/prefix-rdma-$TAG" --tag "$TAG"
```

宿主保护器、source/plan 冻结与源码上传/构建编排实例留在本轮 evidence，
并非自带任意宿主运行权限的通用部署脚本。无保护器、映射不明、温控异常时不运行。
输出保留 matrix、各 case 原始 report、server/client 日志与前后 Prom，不只保留汇总。

### 本轮结果

#### 原始尝试与验证器修订，均保留

首次 ABBA 尝试在 case 02 lifetime 的验证器断言处停止：case 00 legacy、01 lifetime
通过，case 02 原生客户端完成且无错误/未知，但 runner 要求“回填后重访必须整链
命中”，与容量竞争合同不符。case 03 未尝试，**原矩阵仍是失败**。case 02 没有
`after.prom`，不能声称其服务指标完成对账。

实际反例：middle 首次 80 token、4 个回填 ACK，重访链首缺失 0 token；tail 首次
128 token、1 ACK，重访 8 token。符合容量下可能丢失前缀的行为，**没有逐 victim
trace，不能由此断言具体回收因果或生产实现错误**。初次三份客户端账及前两份
Prom 经独立复核，可用前缀代理 token 分别 7120/4400/4088，原始数据未删除或挑选。

增加能复现错误断言的 RED 后，仅修 runner 和对应测试：首次 0/4/7 不变，重访
按实际连续前缀、token、未尝试后缀及后续 ACK 核验；不放宽错误、未知、健康或
指标门槛。不改客户端、计划、构建产物、生产策略或收益阈值。使用全新 `-r1`
目录重新执行完整 ABBA，所有结果连同原失败保留。

#### R1 四 case 原生结果

每 case 固定 53 请求/7392 token，均真实 RDMA，无 I/O 错误或未知：

| 顺序 | 策略 | 可用前缀 token/目标 | 完整前缀请求 | 回填 ACK / 载荷字节 | GET hit/miss | class 回收 |
|---|---|---|---|---|---|---:|
| 00 | legacy | 7120/7392 | 50/53 | 13 / 53248 | 379/3 | 8216 |
| 01 | lifetime | 3704/7392 | 24/53 | 183 / 749568 | 209/29 | 8328 |
| 02 | lifetime | 4024/7392 | 26/53 | 166 / 679936 | 226/27 | 8319 |
| 03 | legacy | 7120/7392 | 50/53 | 13 / 53248 | 379/3 | 8216 |

全业务分母包含 6 个明确人为缺口 fixture（960 token），它们用于正确性，不代表
真实业务分布；另外 47 请求/6432 token 是压力 cohort。两者预先由计划 kind 区分，
不要事后删除 fixture 来另选“更好看的主指标”。

分组描述：两次 legacy 热点长/分支/短链分别为 1280/1280、1280/1280、192/192；
lifetime 分别为 72/1280、560 或 584/1280、120/192。上一批新链重访 legacy
1120/1120、lifetime 0/1120；这只是上一批动作后的重访，**没有 45/185/600 秒
实际年龄合同**。当批新链首次及立即重访，所有策略均为 1280/1280。
R1 lifetime 比 legacy 多 696320 或 626688 个应用回填字节，不是 NAND 写放大。

R1 的强制首次缺口均为 0/80/128 token；case 01 lifetime 的 middle/tail 重访为
24/0 token（ACK 6/8），case 02 为 160/160；如实反映，不把它们删掉或当 I/O 失败。

**结论：本轮验证了真实状态、短路、回填和对账机制，但该短时合成容量竞争下
lifetime 前缀代理收益低于 legacy，回填成本更高。** 不能宣布 lifetime 更优秀，
也不能据此否定其所有长期大批量场景。未测引擎采用/TTFT，不能把这些 token
称为实测客户端缓存复用收益。两次顺序对照不证明无顺序影响或统计显著性。

#### 控制器问题透明记录

- 初次 guest 预检错误照搬 OFF-host 的 440 项数量；ON-guest 实际多三个 io_uring
  存储测试，共 443 项且全部通过。保留原 XML/失败断言，在新输出目录纠正预检。
- R1 宿主 watch 的步骤名为 `matrix_r1`，冻结 guest 脚本打印 `COMPLETE_matrix`，
  导致 watch 标记不匹配（实际子进程 exit 0）。问题在结果出来前已记录，未改运行中
  冻结文件、未注入标记、未为此重跑原生负载；**该 watch 保持 operation_failed**。
  独立核验实际 matrix 四 case、完整原始账和 Prom、源码/计划/构建与回收状态后，
  另一次正确命名的 guarded closeout 成功（13.268 秒）。不冒称旧 watch 通过。

这两项是本地编排错误，不是生产算法修复；所有失败证据保留。

### 构建、单测与独立审查

仓库根目录（真实服务不在宿主启动）：

```bash
cmake --build build-test -j2
(cd test/python && python3 -m unittest test_prefix_rdma test_prefix_rdma_runner -v)
ctest --test-dir build-test \
  -R '^(python_prefix_(chain|dynamic|rdma|rdma_runner)|SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
(cd test/python && python3 -m unittest test_prefix_chain test_prefix_dynamic \
  test_eviction_trace test_eviction_replay test_long_residence \
  test_long_residence_runner test_benefit_sensitivity -v)
python3 -m py_compile deploy/validation/prefix_rdma.py deploy/validation/run_prefix_rdma.py \
  test/python/test_prefix_rdma.py test/python/test_prefix_rdma_runner.py
git diff --check
```

- 宿主 `build-test`，RelWithDebInfo、RDMA/io_uring OFF：构建成功；**31 新方法通过**
  （16 client＋15 runner，0 失败/跳过，0.964 秒）；**440 CTest 通过**（10.25 秒）；
  **180 相关 Python 通过**（13.823 秒）。440 含 436 C++ 与 4 个 Python 入口，
  不把入口中的 30/26/16/15 方法再重复加总。
- guest `/opt/dfkv-build`，Debug、RDMA/io_uring ON：新源码 hash 核验及构建成功，
  server/lib 二进制 SHA 与本轮初态一致；**186 Python 通过**（14.701 秒）、
  **443 CTest 通过**（22.21 秒），均 0 失败/跳过。比宿主多三项 io_uring 存储测试。
- 存在真实缺模块 RED，以及本轮容量反例 RED。fake adapter、坏 payload、未命中/
  错误/未知、部分回填、后续未尝试完整分母、摘要重算后篡改、状态 ABI 缺失、TCP
  回退拒绝、超时/异常/cleanup 失败均有单测。没有为了失败而调用真实 TCP。
- 独立 code-reviewer 用 stdlib 从原始计划/调用账重新算身份、payload 摘要、调用顺序、
  53/7392 分母、首次冻结/回填、各类计数，核对 R1 四组 Prom 的 native/uring 终局、
  class 压力及健康/错误/回退；未调用 workload/check helper。初次 case 02 Prom 缺口明确保留。
- AST/py_compile、文档本地链接/围栏、diff 检查通过；ruff/mypy 缺失，不声称运行。
  未运行全仓测试、sanitizer、真实 HCA、GPU 或推理引擎性能验证。

### 证据与落盘位置

本地 evidence（不提交）：
`.omx/evidence/prefix-rdma/20261125T081757Z-a7934cf/`。
冻结清单：`freeze.json`、`freeze-r1.json`（333 源码文件，R1 只变 runner/test）、
`controller-freeze*.json`、`predeclared-contract.json`；结果见 `matrix-results/`、
`matrix-r1-results/`、`r1-analysis.json`、`initial-counterexample.json`；
编排问题见 `guest-preflight-failure.json`、`r1-marker-predeclared-controller-error.json`、
`matrix_r1-watch.json`、`closeout-watch.json`。原生矩阵初次 84.897 秒、R1 115.094 秒，
不把它们当 QPS/TTFT 或同等工作量的性能比较。

宿主运行日志/压缩原账：
`/mnt/disk2/dfkv-validation/20261118t063414z-c9bbdca/prefix-rdma-20261125T081757Z-a7934cf/`。
guest 结果：`/data/disk1/results/prefix-rdma-20261125T081757Z-a7934cf{,-r1}/`。
缓存目录分别在 guest `/data/disk{1,2,3}/cases/sg-prefix-20261125T081757Z-a7934cf-...`
及 `...a7934cf-r1-...`，初次 9 个、R1 12 个，共 **21 个目录/359546880 字节**
（约 342.89 MiB，按 du 分配量），精确路径见 `retained-data.json`。
保留未删；这些是 guest 路径，宿主 backing 只在允许的 disk2/3，不能直接删除在用
`data*.raw` 或 `guest.qcow2` 来清理本轮数据。

安全收尾：原 VM 六 FD/源码/二进制/端口/无 owned server-client-launcher 核验通过，
RXE ACTIVE、io_uring=0。113 次仅 nvme2/nvme0 样本，最高 42°C、SMART warning 0、
最大间隔 10.264 秒；监控正常停止并 reap（exit 0，原 PID 已不存在），无当前负载。
未压宿主 disk1、未采其 SMART，guest 空闲保留。

## 尚未完成的业务门槛

仍缺代表性真实业务 trace、引擎采用/重算/TTFT、业务成本与正式数值门槛、
真实资源预算及独立签署。短时压缩容量验证不代表长时驻留收益或线上资源规模。

## Follow-up

Timed chain reuse and continuous turnover are tracked separately in
[the delayed-prefix diagnostic](DISK_LIFETIME_PREFIX_DELAYED_VALIDATION.md).
For this follow-up, real inference-engine adoption, TTFT and GPU validation are
user-excluded, not pending gates. The earlier missing-evidence list records the
scope of the historical run above.
