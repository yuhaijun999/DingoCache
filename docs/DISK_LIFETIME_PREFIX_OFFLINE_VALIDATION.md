# 前缀链离线验证：trace、计量器与 fake 回放

日期：2026-11-25；源码基线：`e2bf16e`。
本轮落实[前缀业务口径](DISK_LIFETIME_PREFIX_BUSINESS_ACCEPTANCE.md)的首个可核算子集，
不代表全部业务场景已覆盖，不属于正式 A/B 或策略收益验收。

另见后续[动态状态、回填与容量竞争模型](DISK_LIFETIME_PREFIX_DYNAMIC_VALIDATION.md)：
它使用独立 schema 和测试专用 FIFO，不改变本文 V1 脚本合同，也不代表生产策略。

## 1. 范围先于结果

本工具只支持**合成、线性依赖、每逻辑块一个对象、无本地缓存**。对象由 namespace、
key、generation 明确标识，逻辑块 token 长度可以不同；请求有各自的 ID、链身份和
计划时刻。跨请求共享对象不消除业务请求分母，批次/链标签本身不证明对象等价。

fake backend 只执行独立脚本规定的查询/采用结果，不实现 legacy 或 lifetime，
不模拟容量竞争或生产淘汰。因此任何人为脚本的高命中都**不是某个策略更优秀的证据**。
没有 native client、socket、VM、磁盘压力或 GPU，也没有新的生产指标。
脚本中的时间仅是计划/资格时间，不是实际调度延迟、TTFT 或设备性能。

以下能力不属于 V1：本地与外部复用叠加、多分片/TP/PP/CP、混合模型 checkpoint、
真实引擎采用、回填/verify、ACK 生命周期、排队超时执行器、实际缓存状态的并发演变。
不支持的输入字段必须拒绝，不能静默忽略后按“已覆盖”报告。

## 2. 输入与原始账分开

- **trace**：不可变业务请求及有序对象/token/代际，固定资格窗口；不含命中结果。
- **scenario**：绑定 trace 摘要的独立 fake 结果脚本。它是测试刺激，不是线上样本、
  真实驻留状态或已发生事实。不能通过改 trace 来迎合命中结果。
- **ledger**：实际调用 fake 方法得到的逐请求/逐块结果和身份、未尝试状态与终局。
- **calculator**：从 trace 与原始 ledger 核验身份、数量、顺序和语义后重算指标，
  不把 ledger 里自填的汇总数字当作事实。

摘要使用规范化 JSON 的 SHA256，供版本/误改核对；不是签名或可信执行证明。
即使摘要匹配，也不能证明脚本来自真实服务；离线结构校验不可能替代原始来源审计。
输出使用新文件/目录，禁止覆盖输入、已有文件或符号链接。不清理旧证据后重用路径。

## 3. fake 客户端合同与业务计算

这是一个明确的**顺序查询、顺序采用**客户端子模型，不强制真实批量客户端改成短路：

1. 按 trace 的原有请求顺序回放；按计划时刻和固定窗口判断资格。
2. 每条链从链首查询，首个非 hit 后不继续探测后缀。后缀记 not_attempted，不额外
   GET 观察后缀并改变热度。miss、error、timeout、unknown 分开，不相互替代。
3. 已发现的连续候选仍需通过采用阶段；lookup hit 不等于采用 hit。采用在首个
   不能确认成功处截断。可靠失败与采用证据未知分开，未知不能猜作成功或零收益。
4. 当前请求的收益由当次结果确定，不用后来的回填/下一次请求倒改本次状态。
5. 资格按计划时间固定，不按实际完成结果挑成功交集；窗口外请求显式记录而非消失。
   空目标请求单列，不进入前缀收益分母，但保留在整个计划请求账。

V1 无本地缓存，因此已证实采用的连续前缀就是该合成请求的复用量。
请求平均 token、token 加权比例、请求平均比例和完整前缀率分别报告，使用精确有理数
避免浮点误差；零分母不可判定。未知实际采用时不报伪造的精确主指标，另报已证实
采用贡献/完整分母的保守下界，不能把下界当作真实均值。
缺少真实引擎时，只能称“fake 采用/合成重算代理”，不能称实测重算或 TTFT 改善。

## 4. 人工核算目标

四个请求各有四个 16-token 块，三种缺口位置和一条完整链：

| 查询命中位置 | 连续候选与成功采用 token | 主业务解释 |
|---|---|---|
| A/B/C，D 缺失 | 48 | 尾部缺口仍能复用前三块 |
| A，B 缺失 | 16 | C/D 即使脚本规定可存在，也不会增加连续收益 |
| A 缺失 | 0 | 后缀不构成可用前缀 |
| A/B/C/D | 64 | 完整目标前缀可复用 |

在该顺序查询模型下，未查询后缀不能被报告成已命中。因此“相同对象驻留命中数”
可以作为静态示意前提，但不是这些回放的实际 GET hit 数。不能为了证明同样 3/4
对象命中而增加真实查询。四请求合计 128/256 token、请求均值 32 token、完整率
1/4；这是人工设定的计算示例，不是 lifetime/legacy 对照。

## 5. 复跑、验证与剩余缺口

在仓库根目录执行；每轮使用新目录，不覆盖旧证据，不连接真实服务：

```bash
WORK="$(mktemp -d /tmp/dfkv-prefix-offline.XXXXXX)"
python3 deploy/validation/prefix_chain.py generate --output "$WORK/sample"
python3 deploy/validation/prefix_chain.py replay \
  --trace "$WORK/sample/trace.json" --scenario "$WORK/sample/scenario.json" \
  --output "$WORK/ledger.json" > "$WORK/replay.stdout.json"
python3 deploy/validation/prefix_chain.py check \
  --trace "$WORK/sample/trace.json" --scenario "$WORK/sample/scenario.json" \
  --ledger "$WORK/ledger.json" --output "$WORK/summary.json"
echo "本轮产物：$WORK"
```

`generate` 写固定的四请求教学样例，不生成随机线上分布；自定义输入仍须符合 V1 严格
schema 并重新计算摘要。`replay` 需要新的 fake backend，不能复用有状态 backend；
`check` 必须同时取得对应 trace、scenario 和 ledger，逐项重算、拒绝错误身份、
多/缺/乱序行、虚报后缀命中及汇总篡改。修改脚本并重算所有摘要意味着新实验，不是
原冻结证据仍然通过。CLI 成功退出仅说明该合成工具校验完成，不是策略收益通过。

比例在 JSON 中表示为 `{numerator, denominator}`，例如 `1/2` 输出 `1,2`，不是
浮点百分比。无请求/零目标输出 `not_applicable`；采用未知输出 `inconclusive`，
精确主指标为 null；可确定实际采用时 `measured` 仅表示可计量，不表示正式闸门通过。
候选观测未知单列 `candidate_incomplete_requests`，不自动抹掉已证实采用前缀；
fake 明确未调用 LOAD 的后缀不算采用。采用阶段 unknown/timeout 则保留整请求未知
与已确认前缀下界。`unreused_target_tokens` 只是合成目标未复用量，不是引擎实测。

最短测试命令（不需要服务）：

```bash
python3 -m unittest discover -s test/python -p test_prefix_chain.py -v
```

已有 `build-test` 也可使用 CTest，注册名是 `python_prefix_chain`：

```bash
cmake --build build-test -j2
ctest --test-dir build-test -R '^python_prefix_chain$' --output-on-failure
```

只运行这一 CTest 项，不选择会启动 native/TCP 客户端的全量 Python 入口。
构建目录首次配置/选项与依赖应按项目规则准备，不能把上述 build 命令当自动配置。

下一步如扩展，应先选择一项明确的业务合同（如共享前缀状态演变与缺失后回填），
补齐独立单测和原账，再考虑真实 RDMA 链级对照。不能把本工具直接挂到 native client
后就称多分片、实际采用和收益都已验证。真实链长、共享率、复用间隔/容量比例仍需
业务样本；假数据不能代替真实分布或正式阈值登记。

正式 A/B、F01–F05、独立职责/隔离及安全硬闸门仍按既有协议，不在本轮放宽。
禁止压测宿主 `/mnt/disk1` 和 `/dev/nvme1*`，后续真实服务只走 RDMA；本轮不使用任何
真实盘压测、不测 TCP/GPU，不改变生产算法和默认配置。

## 6. 本轮完成与验证记录

已实现 `deploy/validation/prefix_chain.py`、`test/python/test_prefix_chain.py`，并注册
CTest `python_prefix_chain`。没有修改 `src/`、生产策略/default、ABI 或磁盘格式，
没有新增依赖。此前未提交的前缀业务文档仍保留，并补充了本轮实施范围和入口。

本轮重新执行（不是沿用历史计数）：

| 验证 | 结果/边界 |
|---|---|
| `cmake --build build-test -j2` | 成功；已有 C++ 产物无需重编，配置/新测试入口已生成 |
| `python3 -m unittest discover -s test/python -p test_prefix_chain.py -v` | 30 方法通过，0 失败/跳过，1.594 秒 |
| 定向 CTest（命令见下方） | 437 项通过，0 失败/跳过，7.33 秒；其中 436 C++ 项＋1 个包含上述 30 方法的 Python 项，不重复计为新增 30 项 |
| 相关纯 Python 回归 | trace 9＋replay 13＋residence/runner 77＋sensitivity 25＝124 方法通过，0 失败/跳过，12.142 秒 |
| 独立算术/调用数穷举 | 1–4 块、token 长度 3/5/7/11，340 种 lookup hit/miss × load adopted/rejected 组合通过；不是真实服务或策略测试 |
| 真实 CLI 子进程 | 从 `/tmp` 使用 `python3 -I`，generate → replay → check 成功；人工结果 48/16/0/64，均值 32、比例 1/2、完整率 1/4 |
| 静态与文档检查 | 两新 Python 文件 `py_compile`、本轮 diff whitespace/链接检查通过；ruff/mypy 未安装，未声称完成这些检查 |

完整定向 CTest 和相关纯 Python 回归命令（在仓库根目录）：

```bash
ctest --test-dir build-test \
  -R '^(python_prefix_chain|SlabLifetime|SlabAllocator|DiskLifetime|DiskSlab)' \
  --no-tests=error --output-on-failure
(cd test/python && python3 -m unittest \
  test_eviction_trace test_eviction_replay test_long_residence \
  test_long_residence_runner test_benefit_sensitivity -v)
python3 -m py_compile deploy/validation/prefix_chain.py test/python/test_prefix_chain.py
```

`build-test` 对应本仓库，RelWithDebInfo、RDMA/OFF、io_uring/OFF；本轮不运行 native
网络客户端/TCP 测试。相关回归只选择上述五个纯 Python 模块，不使用全量 native
Python 入口。没有新 sanitizer、真实 HCA/GPU、真实引擎或生产收益证明。

新功能先有缺模块 RED；另用真实行为 RED 复现并修复了 fake backend 复用、错 scenario
导致 KeyError，以及隔离 `python -I` 导入失败。修复保持简单：fresh-only 标记、scenario
摘要绑定、同目录 helper 导入。独立代码审查覆盖输入/身份、未知状态和分母，不是正式
独立阈值验收；哈希不构成真实性签名。

本轮证据目录（本地开发文件，不作为已签署正式证据包）：
`.omx/evidence/prefix-offline/20261125T072820Z-e2bf16e/`。
包含 `final-build.log`、`final-ctest.log/xml`、`final-prefix-tests.log`、
`final-regression.log`、`exhaustive_oracle.py`、`exhaustive-oracle.json`、
`sample/{trace,scenario}.json`、`ledger.json`、`summary.json` 及 RED/审查记录。
全部是小型本地合成输入和结果；本轮未操作 VM、未启动真实服务或温度监控，
也未新增真实盘落盘测试目录。未提交、未推送，正式 A/B 尚未开始。
