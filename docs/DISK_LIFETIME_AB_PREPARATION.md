# 磁盘淘汰 A/B 准备包与可复跑正确性基线

状态：**PREPARATION_ONLY / FORMAL_RUN_BLOCKED**。CPU 纳入，GPU 全部排除。
本工具不切换生产默认、不启动正式 A/B、不决定数值门槛，也不宣称性能验收完成。
依据：[协议](DISK_LIFETIME_AB_PROTOCOL_V1.md)、[阈值登记](DISK_LIFETIME_THRESHOLDS_TEMPLATE.md)、
[报告模板](DISK_LIFETIME_AB_REPORT_TEMPLATE.md)、[功能收口](DISK_LIFETIME_O5_CLOSEOUT.md)。

## 1. 本轮交付边界

准备工作分成两条可运行链路，不能混称“正式基线已取得”：

- **离线准备包与 fake 复跑**：固定 trace、共同配置、二进制身份及工具摘要，核验
  A/B 仅策略不同，按 A→B、B→A、A→B 编排六次受监督 fake 回放，再审计完整原账。
  fake 没有磁盘策略实现，A/B 是预登记的组身份，不产生真实策略优劣数据。
- **真实 TCP/slab 小冒烟**：同一服务器和客户端库，显式 legacy/lifetime、disk ACK、
  RAM 关闭、独立 fresh 目录。逐项核验 ACK、命中、完整字节、进程退出及摘要，并核验 lifetime 读计热增量4、legacy为0。
  不施加淘汰压力、不跨保护窗口、不测固定到达性能，不能由全部命中推出算法有效。

复用 `eviction_trace.py`、`eviction_guard.py`、`eviction_realtime.py` 和
`eviction_smoke.py` 的既有校验与子进程保护，不引入依赖或第二套客户端。
其中 `_verify` 等是仓库内协作契约，不承诺外部稳定 API；修改后必须重跑关联测试并创建新包。
使用外部保存的 plan SHA；包内自填摘要不是签名或独立审批。
源代码或工具改变后，应创建新准备包，不修改旧包后沿用旧摘要。

## 2. 一次准备、多次复跑

在仓库根目录执行，`build-test` 可换成任意同源构建目录。选择尚不存在的包和结果目录；
工具不覆盖、不续跑、不自动重试。以下 `/tmp` 小文件位于本机 mapper root，不是宿主 disk1。
同机当前工作区位于 mapper home；这些离线/单元规模操作不等于 disk2/3 压力测试。

```bash
WORK="$(mktemp -d /tmp/dfkv-ab-prep.XXXXXX)"
cat > "$WORK/config.json" <<'JSON'
{"capacity_bytes":null,"extent_bytes":null,"slot_sizes":null,
 "ram_bytes":null,"coalescing":null,"workers":1,"queue_capacity":16,
 "timeout_ns":1000000000,"process_timeout_sec":10}
JSON
python3 deploy/validation/eviction_ab_prepare.py create "$WORK/package" \
  --binary "$(realpath build-test/dfkv_server)" \
  --source-revision "$(git rev-parse HEAD)" --config "$WORK/config.json" \
  > "$WORK/created.json"
PLAN_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["plan_sha256"])' "$WORK/created.json")"
python3 deploy/validation/eviction_ab_prepare.py check "$WORK/package" --plan-sha256 "$PLAN_SHA"
python3 deploy/validation/eviction_ab_prepare.py run-fake "$WORK/package" "$WORK/fake-1" \
  --plan-sha256 "$PLAN_SHA" > "$WORK/run.json"
REPORT_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["report_sha256"])' "$WORK/run.json")"
python3 deploy/validation/eviction_ab_prepare.py check-results "$WORK/package" "$WORK/fake-1" \
  --plan-sha256 "$PLAN_SHA" --report-sha256 "$REPORT_SHA"
# 再执行使用新目录 fake-2；不覆盖 fake-1，不挑选最好结果。
python3 deploy/validation/eviction_baseline_smoke.py --build build-test "$WORK/tcp-smoke"
echo "保留原始证据位置：$WORK"
```

`null` 明确表示资源待登记，不是默认容量；后续有真实值时创建新包。
source revision 是调用者的来源声明，**没有证明该 binary 由该提交生成**；编译命令、
依赖、库与源码映射仍需独立构建证据。tiny TCP 入口另冻结实际 server/lib 摘要和环境。
输出 SHA 请独立保存；自行改包后换一个 SHA 是新包，不是原冻结包通过。
`formal` 子命令始终失败，填满声明字段也不能把本工具变成正式执行器。
`check-results` 既检查文件摘要，也重新核验逐项原账；成功只表示该准备链路正确。

主要产物：`package/{plan,trace}.json`；fake 结果的 `report.json` 和六个预登记 run
目录，每个保留 supervisor/child 原账与 stdout/stderr；真实冒烟保留每策略 data、
server 日志、前后 metrics 及受监督 client 报告。失败产物也保留供定位。
CLI 不自动删除目录；确认服务和子进程已经回收后，只删除本次自己的 `$WORK` 即可。
真实 slab 每策略逻辑容量 1 GiB、稀疏文件；四个 PUT 合计仅 512 字节 payload，
初始化/元数据写入另计，不把 payload 字节等同所有文件 I/O。

## 3. 对照身份与旧版本兼容

| 组 | 本轮处理 | 未证明事项 |
|---|---|---|
| A | 新 binary、显式 slab/legacy/disk ACK | 不是 v2.33.0 |
| B | 同一 binary、显式 slab/lifetime/disk ACK | fake 不执行策略；tiny smoke 不测淘汰收益 |
| M | 现役 legacy/RAM ACK 迁移参考，单独保留缺口 | 未给出现役配置，不能擅自标不适用 |
| O | v2.33.0 旧行为/兼容对照，单列缺口 | 未构建或运行旧版，不能打开新版盘目录 |

只读核对本仓库 `v2.33.0` 标签指向 `dbdd6bb5a0a5e1d0925d0a254a7de0af97666446`：
其 `src/cache/dfkv_server_main.cc` 不声明新策略与显式 PUT ACK 选择参数。
这是该版本入口的静态事实，不是完整旧 API、协议或磁盘格式兼容证明。
**不能把新版命令行直接套给旧版，不能用同 binary legacy 冒充 O。**
后续 O 应使用隔离源码/构建和独立数据目录，登记当时实际 ACK 与能力；M 也单表记录。

## 4. 数据、资源与失败规则

离线包中的容量、extent、slot、RAM、合并等是**声明**，不是服务的生效 dump。
缺项保持 pending；即使填齐，也不解除正式门禁。正式采样仍需验证实际生效配置、
目标几何、CPU/内存限制、磁盘映射、温度、初态及 W01–W09 场景。

fake 六次运行串行且每次 fresh；首个失败停止后续执行，保留失败和 not_attempted，
不自动重试、不从失败组挑选成功数据。超时、异常和不完整账的实际计数保持 unknown，
完整计划分母仍保留；未知不等于零。成功也必须重新校验原始逐请求账和各阶段初态。

真实小冒烟是有界本地单元规模，不接业务服务；仅使用新拥有的进程和目录。
禁止将输出放到宿主 `/mnt`；禁止回退 Docker 默认 daemon。
**继续禁止压测宿主 `/mnt/disk1`。** 此入口不授权真实盘压力、全局清缓存或内核改动。
后续 RXE/io_uring 压力运行仍需重新核验 VM backing、专属 disk2/3 目录与新温控监护。

## 5. 正式放行仍缺什么

1. 真实业务 trace/权重、连续前缀/重算/TTFT 字段与目标部署资源。
2. 目标 A 的真实旧基线、现役 M 与旧版 O 的独立证据；本轮 tiny smoke 不能替代。
3. 有资格的阈值制定者、独立验收者、样本隔离和访问审计；代理不代签。
4. F01–F05 **每个子指标**的数值、单位、分母、窗口、零基线规则、方差/统计判据、
   样本数、冻结版本及签署。工具不根据探索数据推荐“易通过”数值。
5. 目标规模实际固定到达回放与全资源采样，以及全部必需场景；现有真实 smoke 是
   sequential-unpaced，不能改名为正式 open-loop 执行器。

无需用户代做普通编码、构建和单测；上述业务/权限输入未到位时继续保持正式阻断。
本轮可交付的是准备工具和正确性证据，不是线上收益承诺或默认切换授权。

## 6. 本轮验证与证据

构建目录 `build-test`，GCC 13.3.1 / RelWithDebInfo，RDMA=OFF、io_uring=OFF。
本轮没有 C++/生产配置改动；GPU 不纳入。

```bash
cmake --build build-test -j2
ctest --test-dir build-test -E '^Gpu(NodeDedup|DedupChild)\.' --output-on-failure
# 仅复跑本轮30个方法（19准备＋10纯冒烟＋1真实native）：
DFKV_BUILD="$PWD/build-test" python3 -m unittest discover -s test/python -p test_eviction_ab_prepare.py -v
DFKV_BUILD="$PWD/build-test" python3 -m unittest discover -s test/python -p test_eviction_baseline_smoke.py -v
```

最终重新执行：构建成功（无 C++ 待编译）；1541 个非 GPU CTest 条目中
**1516 通过、25 跳过、0 失败，238.97秒**。11个GPU条目显式排除；跳过不算通过，
25项包含etcd/MDS环境条件及当前buffered模式不适用的direct专用项。
本轮新增30个单测方法（19准备＋11冒烟，含1个真实native）均实际执行通过。
额外 CLI 验证：fake六次共48/48请求，双策略真实TCP共16/16请求；两者分别核验，
不合并计算策略收益。独立review无未解决问题。

两个新 CTest 条目收集准备/纯冒烟；真实 ctypes 测试加入既有 `python_plugin`，
沿用其 sanitizer 排除规则，避免 stock Python 误加载 TSan 库。
独立审查发现的失败报告、禁止路径先读后验、部分启动输出和慢速 HTTP 响应问题
均已补回归；HTTP 全请求超时会回收精确拥有的查询子进程。

证据目录：仓库根目录 `.omx/evidence/ab-preparation/20261120-22e3ac3/`，包含
`final-ctest.xml/log`、构建日志、CLI命令数组、plan/trace、fake六次原账与审计、
`tcp-smoke` 双策略原始报告、前后 metrics、二进制/工具摘要及独立 review 收据。
数据位置为该目录的 `tcp-smoke/{legacy,lifetime}/data`；初始诊断另保留
`/tmp/dfkv-baseline-smoke-probe-1795172320`，不把旧诊断当作最终源码证明。
后者与每次单元测试的临时目录不同；单测自建临时目录在子进程回收后清理。

Python编译、AST、差异空白与本轮本地文档目标检查通过。Ruff/Pyflakes未安装，
不声称完成其 lint/typecheck；未运行本轮 ON/HCA/性能/正式 A/B。
