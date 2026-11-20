# GPU dedup：状态传播现状与下一步兼容契约

2026-11-20，基线 `5f7b276`。本文件为**仓库实现核对及拟实施契约**，不是已实现的新接口，
不改变生产代码、连接器、CUDA 调用、共享内存格式或默认配置。当前阶段仍是 O5 / AB7 前置准备。

## 1. 三条现有路径不能混用

| 路径 | 当前实现 | 可信边界 |
|---|---|---|
| GPU-capable 旧 SG bool | `KVClient::BatchGetAutoSg` → GPU claim/publish/wait → bool direct/fallback | false 不能区分 NF、I/O、参数或复制失败 |
| 保留状态的新 SG | `BatchGetAutoSgWithStatus` → `RangeIntoMulti` | 固定 direct-only，不参与 CPU/GPU rendezvous |
| 显式 CPU 状态 | `BatchGetAutoDeduplicatedWithStatus` | device destination 绕过 CPU shm，不是 GPU 状态链路 |

代码入口：[`kv_client.cc`](../src/client/kv_client.cc) 的上述方法及 `GpuDedup`；
[`dfkv_c_api.cc`](../src/client/dfkv_c_api.cc) 的 `dfkv_batch_get_auto_sg`（旧 bool）和
`dfkv_batch_get_auto_sg_status`（direct-only）。
验证用 Python SG probe 不等于公共 Python GPU 接口。
生产 [`vLLM client`](../integration/vllm/src/dfkv_vllm/dfkv_client.py) 和
[`HiCache connector`](../integration/hicache/dfkv_hicache.py) 仍通过旧 SG bool ABI 读取；
本轮不迁移它们，也不将它们的零 hit 猜成 `kNotFound`。

**结论：尚不存在显式 GPU-deduplicated、逐项保留状态的 C++ → C ABI → Python/报告完整链路。**
不能通过给现有 direct-only API 暗中打开 GPU dedup 来填补这一缺口。

## 2. 现有 GPU rendezvous 的边界

以 [`node_dedup_gpu.h`](../src/client/node_dedup_gpu.h) /
[`node_dedup_gpu.cc`](../src/client/node_dedup_gpu.cc) 为准：

- 需同时打开 `DFKV_CLIENT_NODE_DEDUP=1` 与 `DFKV_CLIENT_NODE_DEDUP_GPU=1`。
  driver/context 不可用或初始化失败可降级直读；设置开关不证明实际启用。
- 只把 shape 合法、全 device 的 SG item 送入 GPU dedup；host/mixed item 走 direct。
  `KVClient::GpuDedup` 在首次全 device hint 时初始化一次，不按每个请求重新选择上下文。
- 索引共享段版本为 `/dfkv-dedup-gpuv3-<uid>-<namespace_hash>`，与 CPU shm v4 不同；
  默认 GPU staging arena 为**每进程 512 MiB**，16384 slots；wait500ms / takeover2000ms / TTL5000ms。
  这些是现有默认值，不是本轮资源预算或调参建议。
- 共享的是成功数据与长度，不含权威 `Status`。owner 失败 Abort；wait 失败由调用方自己回源，
  不是负缓存、失败状态广播或即时唤醒。复制/优化失败本身不能证明后端未命中。
- `WaitManySg` 对整个等待列表使用一个等待预算，按轮摊销同步；不可照搬 CPU 逐项等待而放大尾延迟。
- `PublishSg` 完成私有 CUDA stream 同步后才发布 READY；复制后还检查 generation/lap。
  不能把复制提交成功等同于字节已可安全消费。
- 零 payload 或超过半 arena 的数据不发布到 GPU cache；后端空对象的成功语义仍需保留，
  不应据此制造 NF。TTL 缓存不是强一致缓存，命中不证明服务当下可达。

以上是对现有仓库代码的描述；本轮没有调用或验证外部 CUDA API 签名/驱动行为。

## 3. 下一步拟实施的 additive 状态契约

名称在实现前最终确认；先独立增加显式 GPU/SG 状态入口，保留现有 direct-only 与旧 bool API。
不得在本轮文档中宣称符号已导出或 Python 已绑定。

| 终局 | 拟定逐项结果及责任 |
|---|---|
| direct owner 成功 | 保留 Ok/完整长度；仅持有非零 generation token 的 owner 尝试发布 |
| direct owner 明确 NF 或其他错误 | 保留其原始状态、长度0；Abort 自有 claim，不广播该失败 |
| READY / wait 复制成功并复核通过 | Ok / 完整长度；logical hit 计一次，不伪造远程读取 |
| wait/复制/优化失败 | 批量 direct 状态回退，保留**此次回退**的真实状态，不能继承 owner 错误 |
| host/mixed / dedup 不可用 | 原 direct 状态路径；报告实际模式，不宣称 GPU 合并通过 |
| 参数非法/长度溢出/状态 shape 错误 | 按已有状态接口 fail-closed，不采信不完整数组，不影响合法 sibling |
| 异常逃逸或整个 C 调用失败 | RAII 清理自有 generation；C ABI 初始化输出并返回调用级失败，调用方丢弃整批输出 |

实施还必须满足：
1. 原 `Status` 数值、C ABI 数值映射、wire 和 GPU shm v3 布局保持不变；失败目标 buffer 丢弃。
2. 共享 direct 状态核心而不是重新调用 public 方法导致逻辑 GET/hit/bytes 重复计量。
3. 保留整批 `WaitManySg` 等待和 direct 批量回退，明确 wait 不包含所有传输/复制的总截止时间。
4. claim guard 只 Abort 自己取得且尚未发布的 token；不能清除接管者/其他进程的 generation。
   现有旧 GPU bool 循环没有新 CPU 方法式 RAII guard；这是新入口的设计要求，
   不是本轮已复现并修复的旧路径生产故障。
5. SG buffer、device context、IPC handle、owner arena 与同步完成边界都要纳入生命周期审查；
   不能在异步复制尚未完成时清理 arena 或发布可复用状态。
6. Python 显式选择新符号；符号缺失、调用级失败不能悄悄回退旧 bool 或 EXIST 猜 miss。
   真实设备报告要有实际 GPU 启用/复制证据，并分别登记远程 grant 与客户端命中。

## 4. 验证分层与缺口

现有 [`node_dedup_gpu_test.cc`](../test/client/node_dedup_gpu_test.cc) 有无 GPU 的开关测试，
也有需要 CUDA 的复制、失效及 fork+exec IPC 用例；直接测试 `GpuNodeDedup` 不能证明未来
KVClient/C ABI/Python/报告链路。缺硬件时 skip 必须明示，不能用 CPU/RXE 或容器模拟成 GPU。

未来分层：
- 无硬件：参数/shape/状态映射、旧符号兼容、owner 成功/错误与 wait 回退、异常 token 清理、
  一次计量、调用级失败及报告伪造拒绝；mock 只证明控制流。
- 真 GPU：真实 device SG、分段完整字节、批量 wait、复制/同步失败、arena 环绕和代次 fencing、
  owner 退出/接管及独立进程 CUDA IPC，之后验证 C++ → C ABI → Python → 报告。
- 资源/性能：另冻结每进程 arena、进程数、GPU/驱动版本、负载和指标；不借用 CPU 正确性阈值。

本轮只完成契约梳理，**没有新增 GPU 状态代码或执行 GPU 硬件测试**；
正式 A/B 的业务输入、基线、目标资源、独立职责、数值门槛及默认切换仍是独立门禁。
