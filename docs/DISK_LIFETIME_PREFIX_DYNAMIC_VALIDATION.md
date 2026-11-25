# 前缀链动态状态、回填与多请求竞争：离线验证

本轮属于 **AB7：正式 A/B 前业务收益诊断**，基线 `510ba4a`。
这是可人工核算的合成模型，不是线上样本、生产淘汰实现或正式策略对照。

## 1. 与上轮的边界

[脚本 V1](DISK_LIFETIME_PREFIX_OFFLINE_VALIDATION.md) 保持不变。本轮另加
`deploy/validation/prefix_dynamic.py`，让命中由先前写入、删除和容量回收的状态推导，
而不是逐次直接指定命中结果。故障刺激仍是明确的测试输入，不是运行事实。

仍限定：单逻辑块一个对象、正 token 长度、线性有序依赖、无本地缓存，
没有 TP/PP/CP、多分片、真实引擎采用、真实 RDMA、GPU 或性能测试。
多个请求共享同一 fake 缓存，按固定动作序列竞争容量；**不是线程并发验证**。

容量回收只用 **测试专用 FIFO**，不实现 legacy/lifetime，不读取生产参数，
不能用本轮结果判断哪种生产策略更优秀。对象字节数是应用有效载荷代理，
不包括 extent、元数据、对齐、设备写入和物理写放大。

## 2. 核算合同

- 身份包含 namespace/key/generation；同一身份的 token/字节元数据必须一致。
- 查到对象与采用对象分开；首个缺失/失败后，未尝试后缀不能冒充命中。
- 每次请求首先冻结当次实际采用的连续前缀，再做可选后缀重算代理与回填。
  后续回填成功和重访不能倒改首次结果。重算 token 只是合成成本代理，不是 TTFT。
- 回填逐对象记录尝试、ACK/失败和有效载荷字节；初始化、普通写入与回填分开。
  失败写入不回收对象、不改变状态。未知采用不能假设成功，也不能伪装成未命中。
- 每次重复业务请求都保留分母；共享对象只共享存储，不合并请求收益。
- 使用有限动作列表固定分母，不按成功挑交集，也不声称实现时间窗口或真实调度。
- 原始动作、观测、回收和占用状态可对账；摘要是误改检查，不是签名。

检查器会从计划重新执行同一状态模型并逐项比较账本，能够发现错身份、漏项、
虚报命中和汇总篡改；**它与回放共享实现，不能独立证明模型正确**。
因此另以人工示例及不调用该模型的独立参考计算检查关键状态与算术。

## 3. 复跑与验证

### 状态与故障输入

计划 schema 为 `dfkv-prefix-dynamic-plan-v1`，包含 `objects` 目录、按顺序执行的
`actions`、独立的 `faults` 刺激和摘要。目录项包括 `object_id`、namespace/key/
generation、tokens 和 bytes。动作支持 `seed`、`put`、`remove`、`request`；
request 明确 request_id、chain_id、有序 object_ids 和是否 refill。
同一请求 ID 不可重复；同物理身份不得通过不同目录 ID 建别名；共享对象的有序
依赖前缀必须相同，不允许把依赖 A 的 B 当作另一条以 C 开头的链的同一个 B。
空链单列，不进入 token/非空请求分母。

FIFO 合同：按 ACK 写入顺序排列，读命中不刷新位置，重复 ACK 写入移至队尾；
写入前淘汰队首直至字节容量足够。单对象超容量返回 `oversize`，不改变状态；
`io_error` 写入不改变状态。remove 只删除指定的完整身份，缺失删除不报命中。
**不同 generation 可以同时驻留**：这是版本限定对象模型，不模拟生产同 key
覆盖/代际失效协议。也不模拟 pin、在途回收或崩溃持久化。

故障按 action_index/block_index/phase 定位，仅能注入明确支持的查询、采用、
回填失败状态；非法、重复、越界位置及最终未实际执行的故障均拒绝。
LOAD `unknown`/`timeout` 保留已确认采用下界，整请求精确采用量为 null，并禁止
回填；读 I/O/未知/超时也不触发假装成功的回填。确定 `rejected` 可以走合成重算。
不支持结果未知的写入，因为那需要额外的状态分支/核实协议，不能假装没有写入。
合成重算后逐项写回整个未采用后缀，即使部分后缀原本驻留也会重写；单项回填失败
不取消其余项。重算完成与写回成功是两件事，后续请求仍可能在失败块处截断。
容量小于链长时，回填甚至可以淘汰本次刚采用的链首；本次采用量仍冻结，下一次
请求则可能从链首缺失。这是模型显式暴露的竞争，不是保证“回填必然保住整链”。

账本包含逐次 events、逐动作 action_states、每次 requests、final_residents 与
summary；check 对完整内容核对，而非只信汇总或自填摘要。
`evicted_previously_adopted_objects` 是“历史上曾采用对象被回收”的**事件次数**，
包括重复回收，不是独立热点数或热点未命中次数。真实缺失应看当次 request 与
lookup 账。`recompute_proxy_tokens` 仅统计实际启用且未被失败抑制的回填分支所
合成重算的后缀，不是全部未复用 token，更不是引擎实测重算。

### 人工可核算的默认样例

容量 3 字节，A/B/C/D 各 1 字节、10 token。先 seed A/B：

| 动作 | 当次采用 token | 后续状态/成本 |
|---|---:|---|
| 请求 ABC，启用回填 | 20 | 首次 C 缺失；合成重算 10，回填 C ACK 1 字节 |
| 请求 ABD，不回填 | 20 | D 缺失；不改变缓存 |
| 再次请求 ABC | 30 | 使用之前回填的 C，不能倒改首次 20 |
| PUT D | — | FIFO 回收 A，驻留 B/C/D |
| 再次请求 ABC | 0 | 链首 A 缺失，即使 B/C 驻留也不构成前缀收益 |

4 个业务请求、目标 120 token、采用 70，token 比例 **7/12**、均值 **35/2**，
完整率 **1/4**；回填尝试/ACK 各 1 字节，回收 1 次。它说明核算和状态转换，
不说明该 FIFO 合理，更不代表 legacy 或 lifetime 的业务收益。

### 最短复跑命令

仓库根目录执行，结果只在新的 `/tmp` 目录，不连服务：

```bash
WORK="$(mktemp -d /tmp/dfkv-prefix-dynamic.XXXXXX)"
python3 deploy/validation/prefix_dynamic.py generate --output "$WORK/plan.json"
python3 deploy/validation/prefix_dynamic.py replay \
  --plan "$WORK/plan.json" --output "$WORK/ledger.json" > "$WORK/replay.stdout.json"
python3 deploy/validation/prefix_dynamic.py check \
  --plan "$WORK/plan.json" --ledger "$WORK/ledger.json" --output "$WORK/summary.json"
python3 -m unittest discover -s test/python -p test_prefix_dynamic.py -v
```

所有输出独占新建，已有文件/输入别名/符号链接不可覆盖。输入 JSON 上限 16 MiB，
计划最多 1024 对象、4096 动作、每请求 128 块、总请求块数 16384；超限拒绝。
校验成功仅表示该合成模型账本一致。无请求为 `not_applicable`，有未知采用为
`inconclusive`，可精确计量为 `measured`；没有任何正式策略验收的 pass 含义。

### 本轮验证证据

本轮实际构建目录为仓库 `build-test`：RelWithDebInfo，RDMA/io_uring 均 OFF。
这是离线工具与 C++ 存储回归构建，不运行 TCP 客户端，也不冒充 RDMA 数据路径验证。

```bash
cmake --build build-test -j2
python3 -m unittest discover -s test/python -p test_prefix_dynamic.py -v
ctest --test-dir build-test \
  -R '^(python_prefix_(chain|dynamic)|SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
(cd test/python && python3 -m unittest test_prefix_chain test_eviction_trace \
  test_eviction_replay test_long_residence test_long_residence_runner \
  test_benefit_sensitivity -v)
python3 -m py_compile deploy/validation/prefix_dynamic.py test/python/test_prefix_dynamic.py
git diff --check
```

- 真实 RED：新增测试先因缺少 `prefix_dynamic` 模块失败，后实现修复。
- 最终构建成功；新增 **26 方法通过，0 失败/跳过，1.596 秒**。
  内含 **648** 个独立 list 模型串行动作组合、**30** 个驻留组合、人工算术，
  以及首/中/尾缺口、共享分支、重访、回填逐项失败、自身前缀被回收、未知 LOAD、
  查询/采用 I/O 区分、代际/namespace、不同 token/字节、空分母、严格输入、
  重算摘要后的账本篡改与 `/tmp` 下 `python -I` 真实 CLI。
- **438 项 CTest 通过，0 失败/跳过，9.01 秒**：436 项 C++ 存储族测试，
  加两个 Python 入口，分别包含原 30 方法与本轮 26 方法；不要重复相加成新增总数。
- **154 项相关 Python 通过，0 失败/跳过，13.011 秒**，包括原前缀 30 方法。
- 独立审查之外，主代理另写不调用 fake 状态/汇总函数的 list/tuple 参考：
  **5,184 组**容量 1～4、四动作序列、AB/AC 分支及 put/remove/refill 对照通过；
  核验最终 FIFO 顺序、每请求采用、逐动作占用、完整 token 分母、回填 ACK/尝试与回收数。
- 真实 generate/replay/check 样例账与上述人工结果一致；两文件 py_compile/AST、
  本地文档链接、围栏与 diff 检查通过。环境没有 ruff/mypy，不声称完成其检查。

日志与样例（本地 evidence，不进入提交）：
`.omx/evidence/prefix-dynamic/20261125T075737Z-510ba4a/`，
主要文件为 `final-build.log`、`final-dynamic-tests.log`、`final-ctest.log/xml`、
`related-regression.log`、`independent_oracle.py`、`independent-oracle.json`、
`plan.json`、`ledger.json`、`summary.json`。独立参考也是合成模型检查，不是策略性能证据。
本轮没有运行全仓测试、sanitizer、真实服务、真实硬件、真实引擎或压测。

## 4. 仍未关闭的门槛

本轮不改变生产默认值，不放行部署，也不完成正式 A/B。
后续仍需有代表性的业务前缀 trace、真实引擎采用证据、真实容量与资源预算、
业务成本与预登记阈值，以及独立验收职责。状态模型不能替代 RDMA 服务验证、
生产持久化失败测试、真实并发或磁盘性能。

本轮不启动服务/VM/压测，不触碰 HOST `/mnt/disk1` 的压力或 SMART 采样；
后续真实服务继续仅允许 RDMA，GPU 排除。
