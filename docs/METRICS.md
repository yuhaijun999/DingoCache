# dfkv 可观测性（Metrics / 集群视图）

> 本页是 **v2.11.1** 的稳定指标契约；表内列出本版本全部公开 metric family，
> 并标明按构建、引擎或开关才出现的条件族。旧版本可能缺少后续增加的指标，
> 不得把“序列不存在”直接解释为 0。`/metrics` 生成仍在独立端口/线程，
> 热路径计数为 relaxed 原子、延迟按 1/64 采样；不开端口时不启动 HTTP。

> **分布式追踪（traces）** 是独立的连接器侧能力（OTLP `/v1/traces`），
> 见 [tracing.md](tracing.md)。本文只覆盖 metrics。

## 1. 抓取端点

| 进程 | 开关 | 端点 |
|---|---|---|
| `dfkv_server` | `--metrics-port <p>`（缺省=关） | `GET /metrics`、`GET /healthz`、`GET /readyz` |
| `dfkv_mds` | `--metrics-port <p>`（缺省=关） | `GET /metrics`、`GET /healthz`、`GET /readyz` |

身份标签：`dfkv_server --id <node> --group <g>` → 节点数据面序列带
`{node,group}`；registrar 健康序列显式带 `{node,group}`。Prometheus 抓取自带
`instance`/`job`。`/healthz` 反映当前 store/RAM terminal health；`/readyz`
还要求本地 startup 完成以及（配置 MDS 时）首次注册成功。首次注册 latch 后，
瞬时 heartbeat 丢失不会清 readiness；用下述 heartbeat 指标单独告警。未配置
MDS 的 node 仅等待本地 startup 和动态 storage health。
IB placement eligibility is independent of these HTTP checks. Only an
RDMA-enabled binary with an active listener emits the server rail families.
At least one healthy initialized rail keeps the node eligible: a partial loss
reports fewer active rails but `/healthz`, `/readyz`, and placement remain
online. Losing the final healthy rail makes `dfkv_server_ring_eligible=0`
immediately; 0→nonzero recovery waits the configured sample streak.
`dfkv_mds` 的语义不同：`/healthz` 是纯进程 liveness，**不 probe etcd**——
否则一次秒级 etcd 抖动会让 kubelet 同时重启全部 MDS 副本，CrashLoop 比抖动
更久并引发 lease 集体过期。`/readyz` 保留 etcd 依赖检查，但经 TTL-debounced
probe（`DFKV_MDS_PROBE_CACHE_MS`，默认 2500 ms 内复用上次结果，0=恢复逐请求
现场 probe）：not-ready 副本每 TTL 最多重探一次，kubelet 抓取节奏不会放大成
etcd 读负载。运行期依赖丢失时 `/readyz` 从 200 变 503，scheduler 必须用它
摘流；etcd 恢复后同一 MDS 进程自动回到 200。

监听加固 knobs（均 env，resolved 后进 config dump；两进程的 metrics HTTP 还
支持 `--metrics-bind <addr>` 绑定监听地址，缺省 0.0.0.0）：

| knob | 默认 | 作用 |
|---|---|---|
| `DFKV_METRICS_FIRST_REQ_MS` | 30000（0=关，硬上限 3600000） | metrics HTTP 首行请求 absolute deadline（accept 起算），防 drip/silent 连接占 handler |
| `DFKV_METRICS_MAX_CONNS` | 64（硬上限 4096） | metrics HTTP 并发连接上限，超限直接关闭 |
| `DFKV_MDS_FIRST_REQ_MS` | 30000（0=关，硬上限 3600000） | `dfkv_mds` 控制面首帧 absolute deadline |
| `DFKV_MDS_MAX_CONNS` | 4096 | `dfkv_mds` 并发连接上限 |
| `DFKV_MDS_PROBE_CACHE_MS` | 2500（0=逐请求现场 probe，钳制上限 600000） | `dfkv_mds` `/readyz` etcd probe TTL 去抖 |
| `DFKV_TCP_FIRST_REQ_MS` | 30000（0=关，硬上限 3600000） | `dfkv_server` 数据面首帧 absolute deadline；resolved 值见 `dfkv_tcp_first_req_ms` |

```bash
dfkv_server --dir /mnt/d1,/mnt/d2 --port 12000 --rdma-port 12001 \
            --id gpu1-0001 --group glm --mds 10.0.0.1:9400 --advertise 10.0.0.11:12000 \
            --metrics-port 9100
curl -s 127.0.0.1:9100/metrics
```

## 2. 集群 / 环视图（CLI）

> **v1.8.0 起**：`dfkvctl ring` 多一列 `INFO` = 各节点在注册/心跳时自报的
> `ver=…,engine=…,wr=…,disks=…,cap=…,ram=…,rdma=…,qd=…`（运行时 resolved
> 真相，非 flag 意图；`wr=` 是 slab 的 direct/buffered resolved 写模式，`qd=`
> 是 server 端协商后的 pipeline depth）。
> 全环版本/引擎审计一条命令完成（抓"静默跳升级/引擎不一致"）。`-` = 节点还是
> 旧版未上报（本身就是版本信号）。信息不参与环 epoch（变更不会触发客户端重建环）。
> 生效条件：server 与 MDS 都 ≥ v1.8.0（老 MDS 会丢弃该扩展字段）。

```bash
dfkvctl ring     --mds <ep,...> --group <g>   # 成员表 + 一致性哈希环 vnode 分布/占比
dfkvctl stat --all --mds <ep,...> --group <g> # 逐节点指标 + 集群聚合（容量/对象/命中率）
dfkvctl stat <ip:port>                        # 单节点原始 /metrics 文本（旧用法不变）
```

## 3. 指标清单

### 3.1 cache 节点（`dfkv_server` /metrics）
下表及其条件子表构成 v2.11.1 的完整公开 family 清单。带 `{disk}`、`{dev}`、
`{tenant_hash}`、`{slot_size}` 的序列，其 label 值来自启动时有界集合，不能由请求输入扩张。

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_build_info{version,transport,engine,write_mode[,node,group]}` | gauge | 版本 + 构建传输（rdma/tcp）+ resolved 存储引擎；slab 的 `write_mode` 为 `direct`/`buffered`，显式 file 为 `n/a`；设置 `--id`/`--group` 时追加 `node`/`group` label；恒为 1 |
| `dfkv_uptime_seconds` | gauge | 启动至今秒数 |
| `dfkv_storage_healthy` | gauge | 当前 disk group 与显式请求 RAM tier 的 terminal health；0 时 `/healthz`、`/readyz` 均为 503 |
| `dfkv_server_startup_complete` / `dfkv_server_mds_registration_ready` / `dfkv_server_healthy` / `dfkv_server_ready` | gauge | startup、本次首次 MDS 注册门、本地动态 health；ready 还要求未触发 lifetime 时钟重启信号（正常旧模式无此信号），不将该信号当磁盘 I/O 故障；`dfkvctl stat --all` 用这组稳定 gauge 判定节点健康 |
| `dfkv_cache_put_total` / `dfkv_cache_hit_total` / `dfkv_cache_miss_total` | counter | PUT / GET 命中 / GET 未命中 |
| `dfkv_exist_hit_total` / `dfkv_exist_miss_total` | counter | Exist 命中 / 未命中 |
| `dfkv_remove_ok_total` / `dfkv_remove_miss_total` | counter | Remove 删掉了块 / 目标本就不存在（partial save 清理等路径的诊断分型） |
| `dfkv_bytes_written_total` / `dfkv_bytes_read_total` | counter | 读写字节 |
| `dfkv_accepts_total` | counter | 累计 TCP accept |
| `dfkv_open_connections` | gauge | 当前打开连接数 |
| `dfkv_tcp_max_connections` / `dfkv_tcp_io_timeout_seconds` | gauge | cache TCP handler 硬准入上限 / socket I/O 超时的 resolved 配置 |
| `dfkv_tcp_first_req_ms` | gauge | 首帧 absolute deadline 的 resolved 配置（`DFKV_TCP_FIRST_REQ_MS`，默认 30000，0=关；防 drip 连接占 handler 槽位） |
| `dfkv_tcp_rejected_connections_total` | counter | 达 handler 上限后拒绝的新 TCP 连接；增长表示 silent/flood 或连接池规模超过预算 |
| `dfkv_mds_registration_latched{group,node}` | gauge | server 首次 MDS 注册是否完成；成功后保持 1 |
| `dfkv_mds_first_registration_timeout_ms{group,node}` | gauge | 首次注册 deadline 的 resolved 值；cache daemon 默认 60000，合法配置范围 1000–600000 |
| `dfkv_mds_first_registration_timed_out{group,node}` | gauge | deadline 是否已到期；到期后进程立即进入 fail-closed shutdown 并退出 1 |
| `dfkv_mds_heartbeat_healthy{group,node}` | gauge | 首次注册后最近一次 heartbeat 是否成功 |
| `dfkv_mds_heartbeat_failures_consecutive{group,node}` / `dfkv_mds_heartbeat_failures_total{group,node}` | gauge / counter | 连续 / 累计 heartbeat 失败 |
| `dfkv_mds_last_success_age_seconds{group,node}` | gauge | 最近成功注册或 heartbeat 的年龄 |
| `dfkv_evictions_total` / `dfkv_evicted_bytes_total` | counter | 淘汰对象数 / payload 字节；slab 字节包含成功水位淘汰，不是 slot 容量或元数据写入字节 |
| `dfkv_errors_total{op,status}` | counter | 失败 op（put/get io、invalid） |
| `dfkv_objects` / `dfkv_used_bytes` / `dfkv_disks` | gauge | 对象数 / 占用 / 盘数 |
| `dfkv_disk_used_bytes{disk}` / `dfkv_disk_objects{disk}` | gauge | 每盘占用 / 对象 |
| `dfkv_tenant_default_quota_bytes` | gauge | 本节点未显式列出 tenant 的 quota；0=无限 |
| `dfkv_tenant_quota_rejections_total` | counter | 本节点所有 tenant 的 `kQuotaExceeded` item 总数 |
| `dfkv_tenant_quota_limit_bytes{tenant_hash}` | gauge | 配置文件中该 16 位小写 hex tenant hash 的 per-node limit；0=无限 |
| `dfkv_tenant_used_bytes{tenant_hash}` | gauge | 该显式配置 tenant 在本节点的 committed payload bytes |
| `dfkv_tenant_quota_rejections_by_hash_total{tenant_hash}` | counter | 该显式配置 tenant 在本节点的 quota rejection |
| `dfkv_op_latency_seconds{op}` | histogram | **1/64 采样**的 **get / put / exist** 服务端延迟（50µs–60s 桶，另有 `+Inf`）。`op="exist"` 是 exist handler 体延迟（Contains + IsCached 的锁），L3 预取停滞时先查它的尾；serve-loop 排队（大 GET 挡在同连接的 exist 前）是另一回事，靠客户端 control-lane QP 隔离规避 |

Tenant 标签**只**来自启动时有界配置文件中的 16-hex hash；默认 quota 命中的
未列 tenant 不生成动态 label。容量余量为
`dfkv_tenant_quota_limit_bytes - dfkv_tenant_used_bytes`（limit=0 时不告警）。
`rate(dfkv_tenant_quota_rejections_total[5m]) > 0` 表示 quota 拒绝；不要与
`dfkv_put_busy_total`（写入门 `kCacheFull`）混为一类。

建议告警：cache daemon 的首次注册由
`--mds-registration-timeout-ms` / `DFKV_MDS_REGISTRATION_TIMEOUT_MS`
（默认 60000 ms，1000–600000，严格解析）有界；到期退出 1 而不是无限 unready。
`dfkv_mds_heartbeat_healthy == 0` 持续 2 个 heartbeat 或
`dfkv_mds_last_success_age_seconds > 30` 用于**成功注册后的** MDS 降级；
此时 cache `/readyz` 保持 200，恢复后续租。`rate(dfkv_tcp_rejected_connections_total[5m]) > 0`
触发时同步看 `dfkv_open_connections / dfkv_tcp_max_connections`。

IB rail 告警先看 active cardinality，再看 eligibility：
`dfkv_server_rdma_rails_active < dfkv_rdma_rails_initialized` 表示 `PARTIAL`
带宽；只要 active ≥1，节点仍在 placement。`dfkv_server_ring_eligible == 0`
表示最后健康 rail 已丢失、该节点全部 cache capacity 已摘除。逐轨定位用
`dfkv_server_ib_device_healthy{device}`。这些查询在 TCP-only server 或未启动
RDMA listener 的 RDMA build 上不会返回序列，这是预期行为；不得用
`or vector(0)` 把缺失补成 unhealthy。告警“应有但缺失”必须关联明确的
RDMA-listener scrape inventory。

默认 `DFKV_RDMA_HEALTH_RECOVERY_SAMPLES=3`。从非零健康轨降到零立即清 eligibility；
从零恢复到非零才要求连续成功的本地健康采样机会。通过后，全部轨健康为
`ACTIVE`，部分健康为 `PARTIAL`。heartbeat 名义间隔 10 s 时三次机会通常约
20–30 s（取决于相位），还要叠加 registrar/MDS 发布延迟；这不是上界，也无需
重启。部分掉轨 9→8 不走该恢复门，因为节点从未失去最后健康轨。

四种 rail 口径不能混用：

| 口径 | 权威来源 | 解释 |
|---|---|---|
| configured input / monitored topology | startup log `configured=N`; `dfkv_server_rdma_rails_configured` | log 值是显式列表去重数（auto 为 0）；gauge 是 health monitor 的固定 resolved rail 数（auto 含选出的 rail） |
| initialized/resolved | startup log `initialized=N`; `dfkv_rdma_rails_initialized`; `dfkv_rdma_recv_segment_registered_rails` | 完成 anchor、共享 segment 和全部启动 MR 的固定 topology，auto 含选出的 rail |
| startup-ACTIVE | startup log `ACTIVE=N` / `inactive=...` | 仅启动瞬间快照 |
| current healthy | `dfkv_server_rdma_rails_active`; `sum(dfkv_server_ib_device_healthy)` | 当前 ACTIVE+LinkUp 数；至少 1 即可 placement |

仅在 **RDMA-enabled binary 实际启动 RDMA listener** 时额外输出（折叠进同一
`/metrics`）；TCP-only binary 或未启动 RDMA listener 时下列 family 不存在：

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_server_ring_eligible` | gauge | 当前是否有至少一条通过门控的 initialized healthy rail；最后一条丢失立即为 0，0→非 0 需 recovery streak |
| `dfkv_server_rdma_rails_configured` / `dfkv_rdma_rails_initialized` / `dfkv_server_rdma_rails_active` | gauge | health monitor 固定 resolved 数 / 成功 anchor 初始化数 / 当前健康数；auto 模式均包含选出的 rail，无 `device` label |
| `dfkv_server_ib_device_healthy{device}` | gauge | 每个固定 initialized device 当前是否 `query_ok && ACTIVE && LinkUp`；部分为 0 不再等价于整节点退环 |
| `dfkv_rdma_completions_total` / `dfkv_rdma_completion_errors_total` | counter | RDMA 请求完成 / 错误完成 |
| `dfkv_rdma_pull_read_notification_notified_total` | counter | PULL once-boundary observer invocations, before callback entry; not applied heat, READY delivery or remote consumption. |
| `dfkv_rdma_pull_read_notification_duplicate_total` | counter | Repeated once-boundary calls suppressed before invoking the observer. |
| `dfkv_rdma_pull_read_notification_empty_total` | counter | First calls consumed with zero payload bytes, even if no observer exists. |
| `dfkv_rdma_pull_read_notification_no_observer_total` | counter | First nonempty calls consumed without an installed observer. |
| `dfkv_rdma_active_conns` | gauge | 当前服务中的 RDMA 连接 |
| `dfkv_rdma_v2_conns_opened_total` | counter | server 累计打开的 v2 连接 |
| `dfkv_rdma_v2_put_writes_total` | counter | server 实际收到的 `WRITE_WITH_IMM` PUT |
| `dfkv_rdma_recv_segment_bytes` / `max_bytes` / `chunks` | gauge | receive-pool 当前提交量 / hard budget / 已提交 chunk 数 |
| `dfkv_rdma_recv_segment_used_bytes` / `free_bytes` | gauge | 已提交 chunk 中所有 lease 占用 / 全部空闲字节，**含 staging 专用 chunk**；后者不等于连接可分配字节 |
| `dfkv_rdma_recv_segment_largest_free_range_bytes` | gauge | 任一 chunk 最大连续 free range，含 staging 专用 chunk |
| `dfkv_rdma_recv_segment_connection_free_bytes` / `connection_largest_free_range_bytes` | gauge | 排除 staging 专用 chunk 后，连接可用的全部 / 单块最大连续空闲字节；排查 bootstrap 连接拒绝时看此组 |
| `dfkv_rdma_recv_segment_staging_reserved_bytes` / `staging_reserved_free_bytes` | gauge | 初始 chunk 专供动态 GET 和 leased PUT 暂存的总额 / 当前空闲；已计入上述 hard budget，并非新增预算 |
| `dfkv_rdma_recv_segment_growths_total` / `growth_failures_total` | counter | 启动后 chunk 增长成功 / 因预算或分配失败 |
| `dfkv_rdma_recv_segment_shrinks_total` / `released_bytes_total` / `chunk_idle_ms` | counter / gauge | 空闲缩容次数 / 已返还字节 / 非初始chunk空闲保留期 |
| `dfkv_rdma_recv_segment_allocation_failures_total` | counter | grow 后仍无法满足的最终 allocation |
| `dfkv_rdma_pull_connections` | gauge | 当前 dynamic pull 连接数，包含新握手和 v2.28 dynamic 过渡连接 |
| `dfkv_rdma_pull_zerocopy_served_total` | counter | 为 RAM arena 命中准备的 zero-copy dynamic-pull READY 数，不等于客户端成功完成的 READ 数 |
| `dfkv_rdma_dynamic_get_mr_active` | gauge | 当前进程存活的 exact dynamic READ MR；PullRelease 先撤销 MR，再释放 RAM pin，断连由 QP 销毁完成 fencing |
| `dfkv_op_latency_seconds{op="get"}`（arena pull） | histogram | 记录 RAM handler 准备耗时，与 staged range handler 的计时边界一致；成功 PullRelease 后才提交样本，不包含客户端持有 grant、RDMA READ 或 MR 撤销时间。端到端延迟看客户端 bench，不能用此序列替代 |
| `dfkv_rdma_connection_bytes{class=\"data|control\"}` | gauge | data/control connection 当前 lease 字节；应随 adaptive class 而非 logical max 增长 |
| `dfkv_rdma_recv_segment_registered_rails` | gauge | 成功注册初始 receive chunk 的 rail 数；后续 chunk 按使用 rail 惰性注册 |
| `dfkv_rdma_v2_ready` | gauge | 初始 receive chunk 与 rail anchor 是否就绪 |
| `dfkv_rdma_segment_evictions_total` / `dfkv_rdma_idle_reclaims_total` | counter | hard budget 压力淘汰 / idle timeout 回收连接 |
| `dfkv_uring_reads_total` / `dfkv_uring_init_fallbacks_total` | counter | io_uring 路径实际读 / 初始化失败回退同步连接数 |
| `dfkv_rdma_rail_active_conns{dev}` | gauge | 每个本地 HCA 上当前连接数 |
| `dfkv_rdma_rail_completions_total{dev}` / `dfkv_rdma_rail_completion_errors_total{dev}` | counter | 每 rail 请求完成 / 错误完成 |
| `dfkv_rdma_rail_put_writes_total{dev}` / `dfkv_rdma_rail_put_bytes_total{dev}` | counter | 每 rail 收到的 PUT one-sided writes / payload bytes |

RAM zero-copy pull 的 cache-hit、read-bytes 和采样 GET 延迟在有效 PullRelease
完成时计入。准备阶段探测和失败后 staged fallback 不重复计数；断连中止不当作成功读取。

v2.31 删除 server/client GET-WRITE、固定 pull MR/MW、legacy connection 和
ambiguous GET quarantine 的专属序列；不保留值为零的兼容别名。GET 传输看
client pull READ/release 与 server exact-grant/pin 指标。

> **v2 上线判据**：显式 topology 必须
> `configured == initialized == dfkv_rdma_recv_segment_registered_rails`。
> 启动 `ACTIVE` 只是快照；运行期以
> `dfkv_server_rdma_rails_active` 和逐设备 healthy 为准。active≥1 时
> `dfkv_server_ring_eligible=1`。同时要求 `dfkv_rdma_v2_ready=1`、
> `committed <= max`、growth/allocation failures 不增长。

读侧 convoy 合并与直读晋升（`DFKV_READ_COALESCE=1` 时才有增量；恒零 = 开关没生效）：
| `dfkv_read_coalesce_leaders_total` | counter | 经 coalescer 登记并完成的读（同步 leader + io_uring flight 各计一次；>0 = 合并路径确实在环内） |
| `dfkv_read_coalesced_total` | counter | 被在途同键读吸收的 follower 数；直读晋升 follower 等发布后从 RAM 获取独立 send pin，不复制 leader payload |
| `dfkv_read_coalesce_recur_total` | counter | staged fallback flight 命中复现指纹的诊断计数（`DFKV_READ_COALESCE_RECUR_MS`，默认 1000ms）；direct-to-RAM flight 不创建指纹 |
| `dfkv_read_coalesce_timeouts_total` | counter | follower 等待超时回退自读的次数（`DFKV_READ_COALESCE_TIMEOUT_MS`，默认 500ms；**健康态应恒 0**，持续非零 = leader 连接异常死亡或盘读时延超阈） |

slab 引擎内部（**resolved engine=slab 时输出**——按运行时实际引擎判定，不设 flag/env 的默认也命中；file 引擎无此系列）：
| 指标 | 类型 | 含义 |
|------|------|------|
| `dfkv_slab_dio_write_fallback_total` / `dfkv_slab_dio_read_fallback_total` | counter | direct 模式下回退 buffered 的写/读——**非零升高 = page cache 悄悄回来了**（对齐条件被破坏），direct 部署重点盯 |
| `dfkv_slab_table_sync_total` | counter | slots.tbl fdatasync 周期数（`DFKV_SLAB_TABLE_SYNC_MS`，默认 100ms；限定崩溃复活毒化窗口） |
| `dfkv_slab_extent_steals_total` / `dfkv_slab_extent_returns_total` | counter | 跨 class extent 抢占（伴随驱逐，容量失衡信号）/ 全空 extent 主动回池（无损再平衡） |
| `dfkv_slab_cold_steals_total` | counter | 命中"全局最冷 extent"护栏的跨 class 抢占（donor 全部内容都冷于 `DFKV_SLAB_COLD_STEAL_WINDOW` 窗口才走此路，满环自噬护栏；0=窗口关闭） |
| `dfkv_slab_watermark_evictions_total` / `dfkv_slab_watermark_slot_removals_total` | counter | 全局水位回收移除的对象数，包含最后一个 resident 经整 extent 回调清除的对象；不再以 extent 为单位 |
| `dfkv_slab_watermark_extent_clears_total` | counter | 成功的整 extent 元数据清除写入数，不等于对象数或 fdatasync 次数 |
| `dfkv_slab_watermark_ticks_total` / `dfkv_slab_watermark_active` | counter / gauge | bounded eviction tick 数 / hysteresis 是否仍在 drain |
| `dfkv_slab_watermark_last_tick_us` / `max_tick_us` | gauge | 最近 / 进程最大 watermark 锁区间微秒数 |
| `dfkv_slab_watermark_max_victims_per_tick` / `dfkv_slab_watermark_max_visits_per_tick` | gauge | 每盘每 tick 的 slot 移除上限 / 扫描上限；替代旧 max-extents gauge |
| `dfkv_slab_watermark_slot_record_clears_total` / `dfkv_slab_watermark_record_clear_bytes_total` | counter | 全局回收成功的单 slot tombstone 写入数 / 单 slot 与整 extent 元数据写入字节；不代表同步持久化或物理写放大 |
| `dfkv_slab_watermark_freed_slot_bytes_total` / `dfkv_slab_watermark_scan_visits_total` | counter | 全局回收释放的分配字节 / 扫描位置次数 |
| `dfkv_slab_watermark_freed_payload_bytes_total` | counter | 水位成功淘汰的逻辑已提交 payload 字节，各盘求和；与 rounded slot 字节分开，pin 跳过、失效失败、普通 Remove 不增加 |
| `dfkv_slab_disk_watermark_freed_payload_bytes_total` | counter | 同一口径的每盘计数，沿用 `disk` / `disk_index` 标签，无 key/request 标签 |
| `dfkv_slab_disk_run_epoch` / `dfkv_slab_disk_capacity_bytes` | gauge | 按 `disk`、`disk_index` 标记的运行 epoch / 容量 |
| `dfkv_slab_disk_watermark_high_bytes` / `dfkv_slab_disk_watermark_low_bytes` / `dfkv_slab_disk_reclaim_interval_ms` / `dfkv_slab_disk_watermark_active` | gauge | 每盘解析后的水位、回收周期及活跃状态 |
| `dfkv_slab_disk_watermark_high_observations_total` | counter | 每盘回收线程实际观察到 used > high 的次数；不等于回收次数 |
| `dfkv_slab_disk_watermark_last_high_bytes` / `dfkv_slab_disk_watermark_peak_high_bytes` | gauge | 最近 / 本次运行最大超高水位观察值；历史 peak 不能单独证明新阶段跨水位 |
| `dfkv_slab_table_rebuilt_objects` / `dfkv_slab_rebuild_scanned_bytes` / `dfkv_slab_rebuild_scan_chunks` / `dfkv_slab_rebuild_sparse_ranges` / `dfkv_slab_rebuild_mmap_scans` | gauge | 上次启动冷升从 slots.tbl 恢复的对象数 / 扫描字节 / chunk / sparse range / mmap 的表数（冷升目录规模诊断） |
| `dfkv_slab_rebuild_corrupt_records_total` / `dfkv_slab_rebuild_rejected_records_total` / `dfkv_slab_rebuild_sequential_fallbacks_total` | counter | 启动 rebuild 丢弃的损坏记录 / 因几何不安全被清除的合法记录 / sparse seek 退化为顺序扫描的次数（任一非零都值得看日志） |
| `dfkv_slab_deferred_removes_total` | counter | 被在飞 I/O 延迟执行的 Remove |
| `dfkv_slab_inflight_keys` / `dfkv_slab_prep_holds` | gauge | 锁外 I/O 在飞 key 数 / 未释放的异步 prep 持有数（持续增长 = 泄漏） |
| `dfkv_slab_reclaimed_total` | counter | 后台回收线程预驱逐的 slot 数（`DFKV_SLAB_RECLAIM_MS`，默认 50ms）——持续为 0 且 PUT 延迟高 = 回收被关或池未满，先查 `--slab-reclaim-ms` |
| `dfkv_slab_rebalanced_total` | counter | 回收线程从冷 class 搬给热 class 的 extent 数（类再平衡）——换模型/尺寸迁移期应看到增长，稳态应静止 |
| `dfkv_slab_batched_writes_total` / `dfkv_slab_uring_write_batches_total` | counter | 进入 batched store visit 的 payload 写数 / io_uring 单次 submit 轮数 |
| `dfkv_slab_bind_wipes_total` | counter | extent 绑定新 class 前完成的旧 slot-grid 清理 |
| `dfkv_slab_uring_payload_read_completions_total` / `dfkv_slab_disk_uring_payload_read_completions_total` | counter | Consumed CQEs from participating prepared payload reads, including residual and drained completions. Not syscalls or logical GETs. |
| `dfkv_slab_uring_payload_read_bytes_total` / `dfkv_slab_disk_uring_payload_read_bytes_total` | counter | Positive CQE-returned bytes, including DIO padding and partial bytes before later failure. |
| `dfkv_slab_uring_payload_read_errors_total` / `dfkv_slab_disk_uring_payload_read_errors_total` | counter | Negative read CQE results, not submission/wait errors or final GET failures. |
| `dfkv_slab_uring_payload_read_short_reads_total` / `dfkv_slab_disk_uring_payload_read_short_reads_total` | counter | Nonnegative CQEs below the current residual request length, including EOF. |
| `dfkv_slab_sync_payload_read_syscalls_total` / `dfkv_slab_disk_sync_payload_read_syscalls_total` | counter | Every actual Store-owned synchronous payload pread call, including retries. Not logical GETs or device reads. |
| `dfkv_slab_sync_payload_read_bytes_total` / `dfkv_slab_disk_sync_payload_read_bytes_total` | counter | Positive kernel-returned bytes for those calls, including aligned DIO excess and partial bytes before failure. Not logical payload bytes. |
| `dfkv_slab_sync_payload_read_errors_total` / `dfkv_slab_disk_sync_payload_read_errors_total` | counter | Negative pread returns, including retried EINTR. Not final GET failures; EOF is a short read instead. |
| `dfkv_slab_sync_payload_read_short_reads_total` / `dfkv_slab_disk_sync_payload_read_short_reads_total` | counter | Nonnegative returns smaller than that syscall request, including zero EOF; positive partial reads and a following EOF each count. |
| `dfkv_slab_metadata_io_errors_total` / `dfkv_slab_unclean_resets_total` | counter | slots.tbl/epoch 持久化失败 / 冷升时丢弃 dirty epoch |
| `dfkv_slab_lifetime_class_scan_visits_total` | counter | Resident visits in staged foreground lifetime class scans. |
| `dfkv_slab_lifetime_class_scan_slices_total` | counter | Staged foreground lifetime class scan opportunities. |
| `dfkv_slab_lifetime_scan_budget_exhausted_total` | counter | PUT owners rejected at their lifetime scan budget. |
| `dfkv_slab_lifetime_class_no_candidate_total` | counter | Stable class traversals with no eligible lifetime candidate. |
| `dfkv_slab_lifetime_class_victims_total` | counter | Residents durably removed by foreground lifetime class scans. |
| `dfkv_slab_lifetime_class_freed_payload_bytes_total` / `dfkv_slab_lifetime_class_freed_slot_bytes_total` | counter | 前台 lifetime 类内成功淘汰的旧住户 payload / slot 字节，不是新 PUT 长度或 direct padded 容量；成功事件在释放前快照、按 owner 调用差额计入 |
| `dfkv_slab_disk_lifetime_class_victims_total` / `dfkv_slab_disk_lifetime_class_freed_payload_bytes_total` / `dfkv_slab_disk_lifetime_class_freed_slot_bytes_total` | counter | 上述对象数与两种字节口径的每盘值，复用 `disk` / `disk_index` 标签；不混入 donor、水位或后台 class |
| `dfkv_slab_lifetime_background_scan_visits_total` | counter | Resident visits in internal background class reclaim. |
| `dfkv_slab_lifetime_background_scan_slices_total` | counter | Background class allocator lock slices. |
| `dfkv_slab_lifetime_background_budget_exhausted_total` | counter | Class ticks deferred at a scan or successful-victim cap. |
| `dfkv_slab_lifetime_background_no_candidate_total` | counter | Background class scans with no eligible candidate. |
| `dfkv_slab_lifetime_background_victims_total` | counter | 后台 lifetime 类内成功淘汰对象数；只计失效与释放成功的住户，不混入前台、水位或 donor |
| `dfkv_slab_lifetime_background_freed_payload_bytes_total` / `dfkv_slab_lifetime_background_freed_slot_bytes_total` | counter | 同一路径成功移除的已提交 payload / 分配 slot 字节；两种口径分开，部分失败保留此前成功量，pin/失败/无进展不增加 |
| `dfkv_slab_disk_lifetime_background_victims_total` / `dfkv_slab_disk_lifetime_background_freed_payload_bytes_total` / `dfkv_slab_disk_lifetime_background_freed_slot_bytes_total` | counter | 上述三项的每盘值，复用 `disk` / `disk_index` 标签；无 key/request 标签 |
| `dfkv_slab_lifetime_watermark_budget_exhausted_total` | counter | Internal lifetime watermark ticks stopped at the shared resident-visit cap; does not imply no eligible victim. |
| `dfkv_slab_lifetime_watermark_no_candidate_total` | counter | Internal lifetime watermark scans that completed their stable traversal without an eligible candidate; separate from budget and metadata failure. |
| `dfkv_slab_lifetime_watermark_pin_skips_total` | counter | Pinned resident checks in internal lifetime watermark scans; repeated checks count again. |
| `dfkv_slab_lifetime_watermark_protected_victims_total` | counter | Successfully invalidated watermark victims with score strictly above effective KEEP at the final pre-wipe clock recheck (soft-protection fallback). Score equal to KEEP is not protected. Earlier candidate-selection scores and time spent in persistence do not classify the loss; failed invalidation contributes zero. Per-disk `dfkv_slab_disk_lifetime_watermark_protected_victims_total` uses the same rule and existing disk labels; global values sum per-disk counts. |
| `dfkv_slab_lifetime_donor_coarse_extents_total` | counter | Extents visited in full-pool coarse passes, including rejected entries. |
| `dfkv_slab_lifetime_donor_precise_extents_total` | counter | Extents completely examined for exact loss; at most 8 per selection. |
| `dfkv_slab_lifetime_donor_precise_residents_total` | counter | Resident checks during precise selection; at most 8192 per selection. |
| `dfkv_slab_lifetime_donor_budget_fallback_total` | counter | Selections using coarse ranking because no exact candidate exists. |
| `dfkv_slab_lifetime_donor_unavailable_total` | counter | Selections with no eligible donor (not metadata failures). |
| `dfkv_slab_lifetime_donor_persistence_errors_total` | counter | Selected donor persistence/rebind failures; no alternate donor is tried. |
| `dfkv_slab_lifetime_donor_selection_ns_total` | counter | Cumulative selection nanoseconds, excluding donor persistence and release. |
| `dfkv_slab_lifetime_donor_persistence_ns_total` | counter | Cumulative selected donor persistence AND release/rebind nanoseconds; not bounded by the 8192 selection cap. |
| `dfkv_slab_lifetime_donor_released_residents_total` | counter | Residents actually released after successful selected-donor persistence; separate from selection work. |
| `dfkv_slab_lifetime_donor_freed_payload_bytes_total` / `dfkv_slab_lifetime_donor_freed_slot_bytes_total` | counter | Successfully removed donor residents' old payload / occupied slot bytes, not incoming PUT bytes or the whole extent capacity. Empty donors contribute zero; failed invalidation and skipped pins contribute zero. |
| `dfkv_slab_disk_lifetime_donor_released_residents_total` / `dfkv_slab_disk_lifetime_donor_freed_payload_bytes_total` / `dfkv_slab_disk_lifetime_donor_freed_slot_bytes_total` | counter | Per-disk values with existing `disk` / `disk_index` labels, aggregated across foreground, cold-steal and background growth donor entries. Mutually exclusive with foreground class, background class and watermark victims. Runtime counters reset on store reconstruction; recovery does not replay old eviction events. |
| `dfkv_slab_lifetime_donor_protected_victims_total` / `dfkv_slab_lifetime_donor_protected_payload_bytes_total` | counter | Successful donor releases whose pre-wipe, rechecked-time score is strictly greater than KEEP, and their old payload bytes. Score equal to KEEP is not protected. Use released residents as the denominator; when unscored victims are nonzero, this fraction is only a known-protected lower bound, not evidence that all remaining victims were unprotected. All three donor entry paths are combined. |
| `dfkv_slab_lifetime_donor_unscored_victims_total` | counter | Successfully released donor residents without valid lifetime scoring state. Restored residents with unknown history but valid virtual-read state are scored, not unscored. |
| `dfkv_slab_lifetime_class_score_nonnegative_seconds` / `dfkv_slab_disk_lifetime_class_score_nonnegative_seconds` | histogram | Nonnegative final pre-wipe scores (including zero) of successful class lifetime invalidations only. Reuses the existing final clock sample, not earlier selection time or time after I/O. Fixed score-second display bounds 0, 1, 60, 3600, 86400, 604800, 2592000, then `+Inf`; not acceptance thresholds or remaining wall-clock TTL. Valid recovered virtual scoring is included even after real reads and when age history is unknown. |
| `dfkv_slab_lifetime_class_score_negative_magnitude_seconds` / `dfkv_slab_disk_lifetime_class_score_negative_magnitude_seconds` | histogram | Absolute magnitudes of strictly negative successful class scores, same bounds. Zero appears only in the nonnegative family. Separate paths exclude donor, watermark and explicit Remove. Failed invalidation contributes zero; later allocation/payload failure does not undo completed invalidation. Each allocator call contributes its own score observations before Store return/yield, without subtracting cumulative floating sums or recounting prior pieces. In the same Store snapshot both score counts sum to class victims. Global bins/sums add disks; sums are approximate doubles to avoid overflow from legal large scores and can lose small increments at very large totals. Counts/bins and eviction decisions remain integer. Runtime reset does not replay recovered losses; legacy observes zero and file omits these families. |
| `dfkv_slab_lifetime_background_score_nonnegative_seconds` / `dfkv_slab_disk_lifetime_background_score_nonnegative_seconds` | histogram | Nonnegative final pre-wipe scores (including zero) of successful background lifetime invalidations only. Reuses the existing final clock sample, not earlier selection time or time after I/O. Fixed score-second display bounds 0, 1, 60, 3600, 86400, 604800, 2592000, then `+Inf`; not acceptance thresholds or remaining wall-clock TTL. Valid recovered virtual scoring is included even after real reads and when age history is unknown. |
| `dfkv_slab_lifetime_background_score_negative_magnitude_seconds` / `dfkv_slab_disk_lifetime_background_score_negative_magnitude_seconds` | histogram | Absolute magnitudes of strictly negative successful background scores, same bounds. Zero appears only in the nonnegative family. Separate paths exclude donor, watermark and explicit Remove. Failed invalidation contributes zero; later allocation/payload failure does not undo completed invalidation. Each allocator call contributes its own score observations before Store return/yield, without subtracting cumulative floating sums or recounting prior pieces. In the same Store snapshot both score counts sum to background victims. Global bins/sums add disks; sums are approximate doubles to avoid overflow from legal large scores and can lose small increments at very large totals. Counts/bins and eviction decisions remain integer. Runtime reset does not replay recovered losses; legacy observes zero and file omits these families. |
| `dfkv_slab_lifetime_class_age_seconds` / `dfkv_slab_disk_lifetime_class_age_seconds` | histogram | Known-history ages `max(0, final_pre_wipe_now - anchor)` for successful foreground lifetime class invalidations only. Separate from background, donor, watermark and explicit Remove. A later allocation or payload-write failure does not undo an already successful invalidation. Uses the existing display bounds 0, 1, 60, 3600, 86400, 604800, 2592000 seconds, then `+Inf`; cumulative buckets, known-age count and integer-seconds sum. NEW unread anchors are commit times, not GET times. Global bins/sums add disks, not quantiles. Display bins are not acceptance thresholds. |
| `dfkv_slab_lifetime_class_age_history_unknown_total` / `dfkv_slab_disk_lifetime_class_age_history_unknown_total` | counter | Successful foreground class victims with unknown recovered history, even after real reads. Excluded from known-age histogram and sum. In the same Store Stats snapshot, known-age count + unknown = foreground class victims; those values share D. Legacy observes zero; file omits these families. |
| `dfkv_slab_lifetime_background_age_seconds` / `dfkv_slab_disk_lifetime_background_age_seconds` | histogram | Known-history ages of successful background lifetime class victims only, using the same final pre-wipe rule and display bounds. Each allocator call contributes only its new events, even across multiple pieces or later failure; no cumulative-scan double counting. No foreground/donor/watermark/Remove events. |
| `dfkv_slab_lifetime_background_age_history_unknown_total` / `dfkv_slab_disk_lifetime_background_age_history_unknown_total` | counter | Successful background class victims with unknown recovered history, including after actual reads, excluded from the histogram. In the same Store Stats snapshot known-age count + unknown = background victims, including at unlocked piece boundaries. Legacy observes zero; file omits these families. |
| `dfkv_slab_lifetime_watermark_age_seconds` / `dfkv_slab_disk_lifetime_watermark_age_seconds` | histogram | Known-history ages `max(0, final_pre_wipe_now - anchor)` of successfully invalidated lifetime watermark victims only. NEW unread anchors are commit times, not last GET times. Uses the existing display bounds 0, 1, 60, 3600, 86400 (1 day), 604800 (7 days), 2592000 (30 days), then `+Inf`; cumulative buckets, integer-seconds sum, and known-age count. These are display bins, not acceptance thresholds. Selection time and time spent in persistence do not determine age. Failed invalidation contributes nothing; completed victims before a later failure remain counted. |
| `dfkv_slab_lifetime_watermark_age_history_unknown_total` / `dfkv_slab_disk_lifetime_watermark_age_history_unknown_total` | counter | Successful lifetime watermark victims with unknown recovered history, including after real reads. Excluded from the age histogram and sum. At quiescence, known-age count + unknown-history count = successful lifetime watermark victims; concurrent scrapes are not an atomic snapshot of allocator victim counts and Store age counters. Global series sum disks; legacy leaves these observations zero, and file omits the families. |
| `dfkv_slab_lifetime_donor_age_seconds` / `dfkv_slab_disk_lifetime_donor_age_seconds` | histogram | Known-history resident ages `max(0, pre_wipe_now - anchor)` for successful lifetime cross-class donor releases only. NEW unread anchors are commit times, not last GET times. Fixed finite `le` bounds in seconds: 0, 1, 60, 3600, 86400 (1 day), 604800 (7 days), 2592000 (30 days), followed by `+Inf`. Export cumulative `_bucket`, known-age `_count` and integer-seconds `_sum`; aggregate disjoint bins and sums across disks, not per-disk quantiles. These are display bins, not acceptance thresholds. |
| `dfkv_slab_lifetime_donor_age_history_unknown_total` / `dfkv_slab_disk_lifetime_donor_age_history_unknown_total` | counter | Successful donor releases with valid scoring state but unknown recovered history, including after real reads. Excluded from age histogram and sum: a recovery seed or subsequent anchor must not masquerade as known historical age. Missing state or zero generation uses existing `donor_unscored_victims_total`, not another count. In one allocator snapshot, histogram count + history unknown + unscored = donor released residents. |
| `dfkv_slab_lifetime_watermark_score_nonnegative_seconds` / `dfkv_slab_disk_lifetime_watermark_score_nonnegative_seconds` | histogram | Nonnegative scores (including zero) of successful lifetime watermark victims, sampled at the existing final pre-wipe clock recheck. Fixed finite display bounds in score-seconds: 0, 1, 60, 3600, 86400, 604800, 2592000, then `+Inf`. The same integer score determines the existing strict KEEP protection test; selection scores and time spent in I/O do not classify the loss. Valid recovered virtual scoring state is included, even when age history is unknown. Not a remaining wall-clock TTL or acceptance threshold. |
| `dfkv_slab_lifetime_watermark_score_negative_magnitude_seconds` / `dfkv_slab_disk_lifetime_watermark_score_negative_magnitude_seconds` | histogram | Absolute magnitudes of strictly negative scores of successful lifetime watermark victims, with the same display bounds. Split signs prevent cancellation and retain nondecreasing sums; zero belongs only to the nonnegative family. Failed invalidation contributes nothing; completed victims before a later failure remain counted. At quiescence the two histogram counts sum to successful lifetime watermark victims. Existing allocator/Store cross-counter scrapes are not one atomic live snapshot. Both families use approximate floating sums because legal large scores can overflow integer sums; bins and KEEP comparisons remain integer. Global bins and sums add disks; legacy remains zero and file omits these families. |
| `dfkv_slab_lifetime_donor_score_nonnegative_seconds` / `dfkv_slab_disk_lifetime_donor_score_nonnegative_seconds` | histogram | Successful lifetime donor victims with pre-wipe `DiskLifetimeScore >= 0`, in score seconds. Zero belongs only to this distribution. Cumulative `_bucket`, `_count` and nonnegative floating-point `_sum`; finite `le` bounds 0, 1, 60, 3600, 86400, 604800, 2592000, then `+Inf`. Fixed display bins, not acceptance thresholds. |
| `dfkv_slab_lifetime_donor_score_negative_magnitude_seconds` / `dfkv_slab_disk_lifetime_donor_score_negative_magnitude_seconds` | histogram | Absolute magnitudes of strictly negative successful-donor scores, with the same bounds and exposition. Its zero bucket stays empty. Kept separate from nonnegative scores so cancellation cannot hide loss or make the combined signed `_sum` unsuitable for rate queries. |
| `dfkv_slab_disk_lifetime_donor_protected_victims_total` / `dfkv_slab_disk_lifetime_donor_protected_payload_bytes_total` / `dfkv_slab_disk_lifetime_donor_unscored_victims_total` | counter | Per-disk protection/unscored values with existing `disk` / `disk_index` labels. Empty donors, failed invalidation and skipped pins add zero; counters reset on store reconstruction. Scoring is fused into the existing O(N) selected-resident release loop, with no extra traversal; this release work is separate from the 8192-resident selector budget, not a latency bound or performance result. |
| `dfkv_slab_lifetime_init_clock_hard_exhausted_total` | counter | 内部评分路径的 NEW 或恢复住户因超过32位时钟硬界限而跳过评分初始化的次数；数据提交和身份仍有效，不是 PUT 失败或 I/O 错误。未初始化状态不可用于评分。未注入评分配置的 legacy 及仅注入 clock 的路径恒为0，file 不输出此 slab 指标；此计数不代表外部 lifetime 已开放 |
| `dfkv_slab_lifetime_read_success_total` / `dfkv_slab_lifetime_read_applied_total` | counter | 内部评分路径收到的非零逻辑成功读通知数 / 已对原住户应用的通知数；不是物理 I/O 数，不含失败、零字节、Lookup/Exist。同步请求/Into及其RAM、copy follower和PULL已成立授权均按逻辑请求计数；PULL在有效READ注册与授权所有权建立后、READY编码前通知，准备/RELEASE不补计；不是READY发送成功或客户端消费证明 |
| `dfkv_slab_lifetime_read_no_binding_total` / `dfkv_slab_lifetime_read_race_lost_total` | counter | 成功字节没有盘来源凭据 / 原来源实例、epoch、可见性或generation已不匹配；绝不按当前同名key补绑定或补热 |
| `dfkv_slab_lifetime_read_clock_hard_exhausted_total` | counter | 原住户仍匹配，但实际应用时超过32位时钟硬界限；保留逻辑读成功，不伪报已计热或I/O失败。软门禁后的可表示排空阶段仍正常计热 |
| `dfkv_slab_lifetime_read_io_unhealthy_total` / `dfkv_slab_lifetime_read_state_unavailable_total` | counter | 已成功的读在通知时遇盘I/O隔离 / 匹配住户的评分state缺失或时间不变量错误（正常应为0）；均不改原读状态。失败GET本身不发送通知 |
| `dfkv_slab_disk_lifetime_read_success_total` / `dfkv_slab_disk_lifetime_read_applied_total` | counter | Per-disk successful nonempty logical read notifications / notifications applied to the original resident, using existing `disk` / `disk_index` and optional node identity labels. These reuse existing Store counters, not physical payload I/O counts, transport delivery/client-consumption proof, or proof of deduplication. |
| `dfkv_slab_disk_lifetime_read_no_binding_total` / `dfkv_slab_disk_lifetime_read_race_lost_total` | counter | Per-disk notifications without a disk credential / with an obsolete or removed original resident. Attribution follows the key's configured disk route even for an unbound source; it does not prove that the bytes came from that disk. |
| `dfkv_slab_lifetime_read_lock_held_samples_total` / `dfkv_slab_disk_lifetime_read_lock_held_samples_total` | counter | Lock-held observations paired with the thread-local acquisition cohort; counts equal `lock_wait_samples_total` in the same Store snapshot, including rejected heat. The cross-Store phase, reconstruction and sampling-bias caveats below also apply. |
| `dfkv_slab_lifetime_read_lock_held_ns_total` / `dfkv_slab_disk_lifetime_read_lock_held_ns_total` | counter | Sampled elapsed wall nanoseconds from the existing acquisition endpoint through notification completion before the held ledger update and mutex unlock. Includes wait-ledger updates, allocator wait, business processing, descheduling and old processing-timer bookkeeping. Excludes its own ledger update, actual unlock and delivery lag: not literally the complete lock hold or CPU time. Paired wait/held sums and means may be added from the same snapshot/count, unlike the independent processing-body cohort. |
| `dfkv_slab_lifetime_read_lock_held_max_ns` / `dfkv_slab_disk_lifetime_read_lock_held_max_ns` | gauge | Largest sampled lock-held interval since Store reconstruction; aggregate is max across disks, not a sum or percentile. Do not add wait and held maxima as an observed total maximum: their peaks may come from different notifications. Zero samples is unobserved. Only selected calls add one end-clock read; timing still has nonzero overhead and does not establish a performance budget. |
| `dfkv_slab_lifetime_read_lock_wait_samples_total` / `dfkv_slab_disk_lifetime_read_lock_wait_samples_total` | counter | Observed Store-lock acquisitions for enabled nonempty read notifications, including rejected heat. Select first/65th/129th... eligible call per executing thread across all Stores, independently of processing-body samples. |
| `dfkv_slab_lifetime_read_lock_wait_ns_total` / `dfkv_slab_disk_lifetime_read_lock_wait_ns_total` | counter | Sum of sampled elapsed wall nanoseconds immediately around the existing Store mutex acquisition, including acquisition overhead and descheduling; not pure blocked time, CPU time, lock hold or completed-to-applied delivery lag. |
| `dfkv_slab_lifetime_read_lock_wait_max_ns` / `dfkv_slab_disk_lifetime_read_lock_wait_max_ns` | gauge | Largest sampled acquisition duration since Store reconstruction; aggregate is max across disks, not sum or percentile. Store reset does not reset surviving threads' sampling phases. Short-lived threads are oversampled; periodic cross-disk routing may leave a disk unsampled indefinitely. Zero samples means unobserved, not no contention. New wait and existing body cohorts differ: do not add their means/maxima into end-to-end latency, or multiply sampled totals by 64 as an exact total. TLS access/increment has cost even though unsampled calls read no timing clock and introduce no shared atomic RMW. |
| `dfkv_slab_lifetime_read_processing_samples_total` / `dfkv_slab_disk_lifetime_read_processing_samples_total` | counter | Sampled processing bodies of enabled nonempty successful-read notifications, first/65th/129th... in Store lock acquisition order; includes notifications whose heat is rejected. |
| `dfkv_slab_lifetime_read_processing_ns_total` / `dfkv_slab_disk_lifetime_read_processing_ns_total` | counter | Sum of sampled notification-body elapsed wall nanoseconds after obtaining the Store lock; includes allocator lock wait and descheduling, excludes Store lock wait and timing-ledger updates. Not all-notification elapsed time. |
| `dfkv_slab_lifetime_read_processing_max_ns` / `dfkv_slab_disk_lifetime_read_processing_max_ns` | gauge | Maximum observed sampled body duration since Store reconstruction; aggregate is max across disks, NOT a sum, window maximum or percentile. Samples=0 means unobserved, not zero latency. |
| `dfkv_slab_lifetime_read_race_identity_scope_total` / `dfkv_slab_disk_lifetime_read_race_identity_scope_total` | counter | Receiving Store identity context is absent, or credential store instance/run epoch differs. Checked before payload visibility. |
| `dfkv_slab_lifetime_read_race_not_visible_total` / `dfkv_slab_disk_lifetime_read_race_not_visible_total` | counter | Identity scope matches, but no reader-visible payload remains under the Store lifecycle lock. Does not prove the physical slot is already freed or distinguish Remove from eviction. |
| `dfkv_slab_lifetime_read_race_generation_mismatch_total` / `dfkv_slab_disk_lifetime_read_race_generation_mismatch_total` | counter | Existing allocator lookup found a live, nonpending resident with a different current generation. The current generation may be reserved/uncommitted (zero); no replacement-commit claim. |
| `dfkv_slab_lifetime_read_race_allocator_unavailable_total` / `dfkv_slab_disk_lifetime_read_race_allocator_unavailable_total` | counter | Remaining original allocator identity/resident guards rejected the notification (tracking disabled, missing/non-live/pending resident or zero supplied generation). Not allocation failure, I/O isolation or missing lifetime state. |
| `dfkv_slab_lifetime_read_transport_unclassified_total` / `dfkv_slab_disk_lifetime_read_transport_unclassified_total` | counter | No preparation-route tag was supplied (including the legacy three-argument callback). A subpartition of successful nonempty PULL grants, not an additional term in the original six-source sum. |
| `dfkv_slab_lifetime_read_transport_ram_hit_total` / `dfkv_slab_disk_lifetime_read_transport_ram_hit_total` | counter | An already resident RAM value supplied the read, including pinned arena hits and copies into staging. A subpartition of successful nonempty PULL grants, not an additional term in the original six-source sum. |
| `dfkv_slab_lifetime_read_transport_disk_staging_total` / `dfkv_slab_disk_lifetime_read_transport_disk_staging_total` | counter | The prepared storage request used staging, including an async coalescer owner. Not a physical-I/O counter. A subpartition of successful nonempty PULL grants, not an additional term in the original six-source sum. |
| `dfkv_slab_lifetime_read_transport_disk_to_ram_total` / `dfkv_slab_disk_lifetime_read_transport_disk_to_ram_total` | counter | The prepared storage request targeted a hidden RAM reservation. This is not a RAM hit or proof of successful RAM publication. A subpartition of successful nonempty PULL grants, not an additional term in the original six-source sum. |
| `dfkv_slab_lifetime_read_transport_published_follower_total` / `dfkv_slab_disk_lifetime_read_transport_published_follower_total` | counter | RAM preparation after a successful publication wait. Not proof the returned generation still belongs to that exact leader after concurrent mutation. A subpartition of successful nonempty PULL grants, not an additional term in the original six-source sum. |
| `dfkv_slab_lifetime_read_transport_synchronous_fallback_total` / `dfkv_slab_disk_lifetime_read_transport_synchronous_fallback_total` | counter | The transport used the synchronous range handler. Includes all five fallback_* children; old handlers remain unclassified. A subpartition of successful nonempty PULL grants, not an additional term in the original six-source sum. |
| `dfkv_slab_disk_lifetime_read_clock_hard_exhausted_total` / `dfkv_slab_disk_lifetime_read_io_unhealthy_total` / `dfkv_slab_disk_lifetime_read_state_unavailable_total` | counter | Per-disk hard-clock / isolated-disk / unavailable-state outcomes, with the same precedence as their aggregate counterparts. Within one Store snapshot, success equals applied plus these five skip outcomes. Per-disk and aggregate snapshots are separately sampled: compare exact sums only when activity is quiescent. All seven counters reset on Store reconstruction; recovery's virtual reads add zero, as do failed/empty reads and payload-free lookups. File engines omit these series; legacy without lifetime tracking exports zero. This export adds no read-path state or locks and does not complete origin/latency/physical-I/O coverage. |
| `dfkv_slab_lifetime_read_transport_fallback_unclassified_total` / `dfkv_slab_disk_lifetime_read_transport_fallback_unclassified_total` | counter | A legacy range handler provided no route. A nested subpartition of synchronous_fallback, not an additional transport/source term. |
| `dfkv_slab_lifetime_read_transport_fallback_ram_hit_total` / `dfkv_slab_disk_lifetime_read_transport_fallback_ram_hit_total` | counter | The synchronous handler copied a RAM hit. A nested subpartition of synchronous_fallback, not an additional transport/source term. |
| `dfkv_slab_lifetime_read_transport_fallback_disk_total` / `dfkv_slab_disk_lifetime_read_transport_fallback_disk_total` | counter | An independent synchronous storage read, including coalescer timeout/self/failure fallback. A nested subpartition of synchronous_fallback, not an additional transport/source term. |
| `dfkv_slab_lifetime_read_transport_fallback_copy_leader_total` / `dfkv_slab_disk_lifetime_read_transport_fallback_copy_leader_total` | counter | The synchronous handler led a copy-coalesced storage read. A nested subpartition of synchronous_fallback, not an additional transport/source term. |
| `dfkv_slab_lifetime_read_transport_fallback_copy_follower_total` / `dfkv_slab_disk_lifetime_read_transport_fallback_copy_follower_total` | counter | The synchronous handler actually copied successful shared-flight bytes; not inferred from merely being a nonleader. A nested subpartition of synchronous_fallback, not an additional transport/source term. |
| `dfkv_slab_lifetime_read_source_unclassified_total` / `dfkv_slab_disk_lifetime_read_source_unclassified_total` | counter | Successful nonempty notifications from internal callers that do not supply a completion-path classification. Not an inference from credential validity; production Request/Into and transport-grant callbacks supply explicit paths. |
| `dfkv_slab_lifetime_read_source_ram_total` / `dfkv_slab_disk_lifetime_read_source_ram_total` | counter | Successful nonempty synchronous Request/Into reads copied from RAM, including RAM without a disk credential. Does not count physical disk I/O. |
| `dfkv_slab_lifetime_read_source_disk_total` / `dfkv_slab_disk_lifetime_read_source_disk_total` | counter | Successful nonempty independent synchronous storage reads, including coalescer timeout, failed-flight, missing-shared-buffer and self-thread fallbacks. A logical completion, not a syscall count or bytes read from hardware. |
| `dfkv_slab_lifetime_read_source_copy_leader_total` / `dfkv_slab_disk_lifetime_read_source_copy_leader_total` | counter | Successful nonempty synchronous coalescer leaders, even when no follower joined. Failed or empty leaders contribute zero. |
| `dfkv_slab_lifetime_read_source_copy_follower_total` / `dfkv_slab_disk_lifetime_read_source_copy_follower_total` | counter | Successful nonempty requests that actually copied shared flight bytes. Independent fallbacks are not followers; a source disk identity or a false leader flag alone cannot establish this role. |
| `dfkv_slab_lifetime_read_source_transport_grant_total` / `dfkv_slab_disk_lifetime_read_source_transport_grant_total` | counter | Successful nonempty transport-grant notifications at the existing PULL authorization point, not preparation, READY delivery, client consumption or RELEASE. This category intentionally combines RAM/disk and transport coalescer origins. This remains the coarse total; transport_* children subdivide preparation routes, not physical I/O or exact shared-leader provenance. |
| `dfkv_slab_eviction_record_clears_total` / `dfkv_slab_record_writes_total` | counter | allocator 复用前持久清除记录 / 已提交 slots.tbl record 写数 |
| `dfkv_slab_capacity_bytes` / `dfkv_slab_allocated_bytes` / `dfkv_slab_payload_bytes` / `dfkv_slab_internal_fragmentation_bytes` | gauge | 物理 payload 容量 / 已占 slot / reader-visible 已提交逻辑 payload / 旧口径 allocated-visible 差额。保留旧公式不变；该差额可能包含隐藏、在途或等待最后一个 pin 释放的住户 payload，不能只解释为 slot 碎片 |
| `dfkv_slab_allocator_payload_bytes` / `dfkv_slab_allocator_fragmentation_bytes` | gauge | All allocator residents' useful bytes (including hidden/in-flight residents) / resident slot slack. Computed by summing useful bytes and allocated-minus-useful bytes from one existing per-class allocator snapshot, not reader-visible payload, filesystem fragmentation or durable I/O byte counts. |
| `dfkv_slab_disk_allocated_bytes` / `dfkv_slab_disk_payload_bytes` / `dfkv_slab_disk_allocator_payload_bytes` / `dfkv_slab_disk_allocator_fragmentation_bytes` | gauge | Per-disk occupied slots / visible logical payload / resident useful bytes / resident slot slack, using existing `disk` / `disk_index` labels and per-disk Stats snapshot. Gauges recompute from current residents after recovery; unlike eviction counters, they are not reset to zero if valid residents recover. |
| `dfkv_slab_lifetime_bucket_entries` / `dfkv_slab_disk_lifetime_bucket_entries` | gauge | Logical protection-expiry bucket vector size, B × all physical extents including unbound/empty extents; these are entries, not OS memory pages or currently protected residents. Global / per-disk values, with existing `disk` / `disk_index` labels on the latter. |
| `dfkv_slab_lifetime_bucket_bytes` / `dfkv_slab_disk_lifetime_bucket_bytes` | gauge | Retained expiry bucket vector backing: capacity × sizeof(entry), currently 16 bytes per entry, not vector size × entry size or an allocation/RSS peak. |
| `dfkv_slab_lifetime_bucket_token_bytes` / `dfkv_slab_disk_lifetime_bucket_token_bytes` | gauge | Retained resident expiry-token vector backing: capacity × sizeof(uint64_t). Removing residents can clear registrations without releasing backing; clean recovery constructs fresh vectors whose capacity can differ, including zero for an empty rebuilt store. |
| `dfkv_slab_metadata_slot_bytes` / `dfkv_slab_disk_metadata_slot_bytes` | gauge | Retained SlotMeta array capacity × actual sizeof(SlotMeta), including unused capacity. Base allocator storage in all slab modes, not lifetime-only incremental memory or RSS. |
| `dfkv_slab_metadata_extent_bytes` / `dfkv_slab_disk_metadata_extent_bytes` | gauge | Retained ExtentMeta array capacity × actual sizeof(ExtentMeta), including unused capacity. Base allocator storage in all slab modes, not lifetime-only incremental memory or RSS. |
| `dfkv_slab_metadata_class_bytes` / `dfkv_slab_disk_metadata_class_bytes` | gauge | Retained Class array capacity × actual sizeof(Class), including unused capacity. Base allocator storage in all slab modes, not lifetime-only incremental memory or RSS. |
| `dfkv_slab_metadata_key_index_bytes` / `dfkv_slab_disk_metadata_key_index_bytes` | gauge | Retained FlatKeyIndex::Entry array capacity × actual sizeof(FlatKeyIndex::Entry), including unused capacity. Base allocator storage in all slab modes, not lifetime-only incremental memory or RSS. |
| `dfkv_slab_metadata_class_rotation_bytes` / `dfkv_slab_disk_metadata_class_rotation_bytes` | gauge | Sum of retained Class free-extent rotation vector capacity × sizeof(uint32_t). Includes unused/empty retained capacity, separately from the Class array. |
| `dfkv_slab_metadata_class_free_slot_bytes` / `dfkv_slab_disk_metadata_class_free_slot_bytes` | gauge | Sum of retained per-extent free-slot vector capacity × sizeof(uint32_t), including exhausted vectors with zero size. Drops when the extent's vector is erased, not when its final free slot is consumed. Excludes unordered_map nodes/buckets and heap bookkeeping. |
| `dfkv_slab_lifetime_identity_bytes` / `dfkv_slab_disk_lifetime_identity_bytes` | gauge | Resident generation vector capacity × sizeof(uint64_t). Reports actual enabled identity storage, including identity-only configurations; not an estimate of live keys. |
| `dfkv_slab_lifetime_state_bytes` / `dfkv_slab_disk_lifetime_state_bytes` | gauge | Optional lifetime-state vector capacity × sizeof(optional<DiskLifetimeState>), including alignment and empty entries. |
| `dfkv_slab_lifetime_link_bytes` / `dfkv_slab_disk_lifetime_link_bytes` | gauge | Dual-index link vector capacity × sizeof(LifetimeLinks); both global and class links belong to each element. |
| `dfkv_slab_lifetime_queue_bytes` / `dfkv_slab_disk_lifetime_queue_bytes` | gauge | Score-queue vector capacity × sizeof(LifetimeQueue). Logical queue count is (all canonical size classes + 1 global row) × (MAX_READS + 1), even before any resident is written. |
| `dfkv_slab_lifetime_version_bytes` / `dfkv_slab_disk_lifetime_version_bytes` | gauge | Class mutation-version vector capacity × sizeof(uint64_t), covering all canonical size classes, not only bound classes. |
| `dfkv_slab_lifetime_queue_residents` / `dfkv_slab_disk_lifetime_queue_residents` | gauge | Current retained residents in each physical lifetime scoring queue. Labels: `index="global"` with reserved `slot_size="0"`, or `index="class"` with actual slot size; `band="0"` through `MAX_READS`. Per-disk series also use existing `disk` / `disk_index`. Aggregate values sum the exact per-disk queue snapshots taken during that scrape, not a second sampling pass. |
| `dfkv_slab_lifetime_bucket_protected_estimate` / `dfkv_slab_disk_lifetime_bucket_protected_estimate` | gauge | Current conservative bucket-based protected-resident estimate, not exact `score > KEEP`, unpinned candidates or bucket/token capacity. Numeric sample is omitted when unavailable. Aggregate sums the same per-disk observations only when all are available. |
| `dfkv_slab_lifetime_bucket_estimate_available` / `dfkv_slab_disk_lifetime_bucket_estimate_available` | gauge | 1 when enabled bucket accounting has a usable bounded owner-clock observation, 0 for disabled buckets or hard clock exhaustion. 0 does not mean an empty cache. Aggregate is 1 only when all contributing disks are available. |
| `dfkv_slab_lifetime_bucket_summary_bytes` / `dfkv_slab_disk_lifetime_bucket_summary_bytes` | gauge | Retained global expiry-summary ring capacity × sizeof(bucket), allocated once for B entries on enabled disks. Reported even when time is unavailable; separate from the existing per-extent bucket backing and token gauges. |
| `dfkv_slab_lifetime_visible_recovered_objects` / `dfkv_slab_disk_lifetime_visible_recovered_objects` | gauge | Current lifetime-enabled recovered records still in the Store commit map, globally / per disk with existing labels. Origin persists after applied reads. Deferred Remove subtracts immediately on hiding, before physical release; ordinary invalidation failure leaves the map entry counted even though the Store is unhealthy. This is commit-gated membership, NOT a guarantee that GET/IsCached can currently serve it. Recovery without initialized scoring state still counts, so this is not necessarily a subset of the allocator initialized-state gauge below. NEW/keep-first duplicates do not invent or reset origin; reconstruction rebuilds it. |
| `dfkv_slab_lifetime_visible_recovered_unread_objects` / `dfkv_slab_disk_lifetime_visible_recovered_unread_objects` | gauge | The visible recovered population above without an applied nonempty heat notification since this recovery. Only the first accepted application removes it from unread; storage-only reads, failed/empty reads, wrong identities, health/clock rejection, virtual scoring seeds and metric scrapes do not consume it. Not proof that no successful logical GET occurred. Same Store snapshot obeys unread <= visible recovered <= committed_objects; not an availability or cumulative-read counter. Cached under the existing Store lock, not a resident scan during scrape. |
| `dfkv_slab_lifetime_recovered_objects` / `dfkv_slab_disk_lifetime_recovered_objects` | gauge | Current allocator residents whose initialized lifetime state has `history_unknown=true`, globally / per disk with existing `disk` and `disk_index` labels. Reading them does not clear their recovered-history marker. Not the cumulative startup `table_rebuilt_objects`, reader-visible-only population, payload bytes or all records restored when scoring initialization was skipped. |
| `dfkv_slab_lifetime_recovered_unread_objects` / `dfkv_slab_disk_lifetime_recovered_unread_objects` | gauge | The recovered population above whose in-memory `actual_reads` is zero. The first applied nonempty logical read removes one from this subset, but later reads cannot subtract it again. Virtual recovery scoring reads, payload preparation, physical storage reads alone, heat-rejected notifications and metric scrapes do not consume unread population. NEW unread residents are excluded. This state-based gauge is not proof that no logical GET succeeded if its metadata update was skipped. |
| `dfkv_slab_allocator_objects` / `dfkv_slab_committed_objects` | gauge | allocator resident（含未提交写）/ reader-visible 已提交对象 |
| `dfkv_slab_classes` / `dfkv_slab_bound_extents` / `dfkv_slab_pool_extents` | gauge | live class 数 / 已绑定 extent / 未绑定共享池 extent |
| `dfkv_slab_class_extents{slot_size}` / `dfkv_slab_class_resident_objects{slot_size}` / `dfkv_slab_class_free_slots{slot_size}` | gauge | 每个 live size class 的 extent、resident、可立即分配 slot |
| `dfkv_slab_class_allocated_bytes{slot_size}` / `dfkv_slab_class_useful_bytes{slot_size}` / `dfkv_slab_class_fragmentation_bytes{slot_size}` | gauge | 每 class 的占用、有效 payload、内部碎片 |
| `dfkv_slab_class_read_heat{slot_size}` / `dfkv_slab_class_put_total{slot_size}` | gauge / counter | 每 class 的衰减读热度 / 累计 PUT |
| `dfkv_slab_failed_disks` / `dfkv_slab_healthy` | gauge | fail-closed disk 数 / 全部 slab disk 是否健康 |

Donor age uses the already-rechecked final pre-wipe clock, not the earlier
selection estimate or a new post-I/O clock. State stays under the existing
allocator lock through persistence; publication follows successful extent
invalidation and precedes resident release. A failed wipe, pinned/skipped donor,
empty extent, ordinary Remove, class/background reclaim, watermark eviction or
legacy eviction adds no donor age sample. Store reconstruction resets these
runtime counters; clean recovery does not restore old observations. Global
series retain node/group labels; per-disk series also retain disk/disk_index,
and only histogram buckets add `le`. File-engine scrapes omit these families.
No key/request labels, per-resident age fields, extra locks or clock calls are
added. Recording does add bounded plain accounting work to the existing donor
release loop; this is not a latency or full memory-budget acceptance result.
Other eviction paths' ages, score histograms and recovered survival remain
pending; the new histogram does not represent all eviction ages.

前台和后台 lifetime 成功计数在 staged/explicit legacy 或 file 下不代表新策略生效：
legacy slab 值为零，file 不导出 slab 系列。计数随进程/Store 重建重置，不恢复历史
淘汰量；普通 Remove 不计作后台回收。每盘与汇总为分别取样，并发回收时不保证同一
抓取中所有样本原子一致。每盘完整 stats 抓取会读取 allocator/class 信息，仅用于
观测，不进入业务读写热路径。

前台只消费 class 成功事件快照，不能把 PUT 返回的混合 `evicted` 列表全算作 class，
也不以最终扫描结果枚举代替累计事件。旧住户已经成功淘汰后，即使新 payload 写入
失败，仍保留该淘汰量；反之，旧住户失效失败、pin/预算阻止淘汰、重复 PUT 保留
已有值时不增。恢复后的计数从零开始，恢复 payload 使用已提交记录的真实长度。

上述通知原因互斥：success = applied + no_binding + race_lost + clock_hard_exhausted + io_unhealthy + state_unavailable。优先级为I/O隔离、无绑定、旧实例/epoch/可见性/代际、硬时钟、状态可用性，再应用。硬界限造成的未初始化state按clock原因记，不混成state_unavailable。无评分配置的legacy及仅clock注入不采新应用时间、不加新热锁、计数为0；file不输出slab指标。所有新指标不带key/request标签。


The bucket backing gauges include only the two vectors' retained element
storage, excluding vector objects/allocator overhead, resident identity/scoring
state, lifetime queues, other metadata, RSS and recovery peaks. Retained capacity
is not a registration/protected-resident count; legacy slab reports zero and
file omits these series. Capacity snapshots are O(1), with no resident scan or
clock update; their values are not latency bounds or performance measurements.

Allocator useful bytes and resident slot slack share one per-class snapshot.
The separately observed visible payload, per-disk snapshots and aggregate scrape
are not an atomic cross-disk transaction under concurrent mutation; do not infer
an exact instantaneous cross-series identity from an active scrape. At quiescence,
allocated bytes equal allocator useful bytes plus resident slot slack.

PUT 准入门（**仅 `--put-inflight-limit > 0` 时输出**）：
| `dfkv_put_busy_total` | counter | 被准入门以 kCacheFull 快速拒绝的 PUT（受控 miss，替代深队列尾延迟） |

Recovered population gauges follow allocator ownership: a hidden pinned resident, including one retained after metadata invalidation failure, remains counted until physical release succeeds. Clean reconstruction rebuilds the counts and marks all initialized recovered residents unread again; dirty-reset recovery produces zero. They can differ from visible committed-object gauges and from the cumulative startup restored-record count. The latter may include a resident whose admitted recovery initialization crossed the hard clock limit and left lifetime state absent. Counts are folded from the existing per-class snapshot, with no additional resident traversal or lock acquisition. Each allocator class and ClassStat snapshot adds two uint64 fields (16 bytes), including shared RAM/legacy allocator instances where these counts remain zero; Store Stats adds 16 snapshot bytes. No per-resident field is added. Per-disk and aggregate samples are separate, so exact sums apply at quiescence, not atomically during reads/reclaim. Legacy without lifetime tracking exports zero; file omits these gauges. These counts do not close scoring-band/read-class distribution, lifetime retention, complete-memory/RSS, recovery-peak or performance acceptance.

The six source counters are a completion-path partition, separate from the six heat-application outcomes: each partition sums to `read_success_total` in one Store snapshot, including successes skipped by race, clock or I/O isolation. They reuse the existing notification lock, adding one fixed counter update per enabled nonempty notification, not a new lock, clock or per-resident field. Per Store, the six counters add 48 bytes of fixed state (and 48 bytes to each read-counter snapshot); the coalescer's optional Outcome adds one boolean. Scrapes reuse existing snapshots with six additional series per disk and six aggregate series. Counters reset on reconstruction, and virtual recovery reads/failed or empty GET/Lookup add zero. Legacy without lifetime tracking exports zero; file omits these series. Compare disk/global sums only at quiescence. This does not complete physical-I/O, transport-origin, deduplication, latency or lock-cost acceptance.

RAM 热层（**仅 `DFKV_RAM_TIER=1` 时输出**；关时无此系列，向后兼容）：
| 指标 | 类型 | 含义 |
|------|------|------|
| `dfkv_ram_hit_total` / `dfkv_ram_miss_total` | counter | GET 命中 RAM / 未命中落盘（命中率 = hit/(hit+miss)） |
| `dfkv_ram_put_total` | counter | 写直通进 RAM 的 PUT 数 |
| `dfkv_ram_put_bypass_total` | counter | **背压**：arena 满（flush 落后）→ PUT 旁路直写盘，非零即 flush 跟不上 |
| `dfkv_ram_ack_backpressure_total` | counter | 正的 RAM-ACK dirty 水位触发的新 key 接纳拒绝；水位为 0 时计数强制等待落盘的请求。已有 key 先解析 owner，不因水位切换覆盖首写者 |
| `dfkv_ram_promoted_total` | counter | 整值冷读晋升：优先让 O_DIRECT 直接读入隐藏 arena reservation，成功后以 born-durable 身份发布；staged fallback 则读后复制晋升。不进 flushq、零重复刷盘、随时可驱逐；健康冷→热运行中首次读增长，随后应转为 `dfkv_ram_hit_total` 增长且磁盘字节不再增加 |
| `dfkv_ram_flushed_total` / `dfkv_ram_flush_dropped_total` | counter | RAM slot 落盘转 DURABLE / flush 多次失败后丢弃 |
| `dfkv_ram_healthy` | gauge | RAM flusher 未发生 terminal failure 时为 1；0 会动态摘除 readiness |
| `dfkv_ram_flush_threads` | gauge | shard 最小值调整后的实际 flusher 数（不是原始请求值） |
| `dfkv_ram_evictions_total` | counter | RAM slot 容量压力淘汰数（含内联与后台回收两路） |
| `dfkv_ram_reclaimed_total` | counter | 其中由后台回收线程预驱逐的数量（`DFKV_RAM_RECLAIM_MS`，默认 10ms；flush 积压 >4096 时自动歇拍，此计数暂停属预期） |
| `dfkv_ram_rebalanced_total` | counter | RAM 层类再平衡搬动的 extent 数（增长阶段不受 flush 积压歇拍影响——从冷 donor 搬 durable extent 恰是 flush-gated 时唯一能扩收速的动作） |
| `dfkv_ram_objects` / `dfkv_ram_flush_backlog` | gauge | 当前 RAM 常驻块 / 待 flush（含已出队、正在 flush，均尚未 DURABLE）的块数 |
| `dfkv_ram_budget_bytes` / `dfkv_ram_arena_bytes` / `dfkv_ram_large_budget_bytes` | gauge | RAM 层总预算（arena+dedicated）/ arena 固定预留 / 总预算内大对象 dedicated 预留 |
| `dfkv_ram_used_bytes` / `dfkv_ram_large_used_bytes` | gauge | 已用（常驻 slot + dedicated 分配）/ 其中 dedicated 大对象已用——与上组预算行做水位比 |

> 关键运维信号：COLD `load_get_avg_ms` 骤降 + `dfkv_ram_hit_total` 上升 = RAM 热层生效；`dfkv_ram_put_bypass_total` 或 `dfkv_ram_flush_backlog` 持续升高 = flush 落盘带宽不足，需扩 flush 或降 PUT 速率（见 [docs/ARCHITECTURE.md](ARCHITECTURE.md) §6 背压）。

### 3.2 MDS（`dfkv_mds` /metrics）
`dfkv_mds` 自身的 scheduler readiness 不是启动 latch：`/readyz` 经
TTL-debounced etcd probe 判定（`DFKV_MDS_PROBE_CACHE_MS`，默认 2500 ms 内复用
上次结果，0=逐请求现场 probe），probe 频率由 TTL 去抖决定而非 kubelet 抓取
节奏；`/healthz` 不探 etcd，恒反映进程存活。因此支持 200 → 503 → 200 原地
恢复。


每环汇总（**v1.10.0 起**；scrape 时 MDS 现场 range 一次 etcd，数值来自各节点心跳携带的 STA1 统计，新鲜度≈心跳周期 10s。全部为 **gauge 语义**——节点重启会使 `_sum` 回落，速率分析请用节点级 counter）：
| 指标 | 含义 |
|------|------|
| `dfkv_mds_group_nodes{group}` | 该环成员数（带标签版；旧无标签 `dfkv_mds_members` 保留不动） |
| `dfkv_mds_group_ring_eligible_nodes{group}` / `dfkv_mds_group_degraded_nodes{group}` | 当前可参与 placement 的节点数 / 因 IB health 被排除但 lease 仍在线的节点数 |
| `dfkv_mds_group_capacity_bytes{group}` / `dfkv_mds_group_used_bytes{group}` | 环总容量 / **环水位**（direct 模式下 df 已失真，此为唯一真值） |
| `dfkv_mds_group_objects{group}` | 环内常驻块数 |
| `dfkv_mds_group_hits_sum{group}` / `dfkv_mds_group_misses_sum{group}` | 环级命中率 = hits/(hits+misses) |
| `dfkv_mds_group_evictions_sum{group}` / `dfkv_mds_group_puts_sum{group}` | 容量压力 / 写入量 |
| `dfkv_mds_group_put_busy_sum{group}` | 准入门拒绝总数（过载信号） |
| `dfkv_mds_group_dio_fallbacks_sum{group}` | direct 模式 buffered 回退总数（**>0 = page cache 悄悄回来了**，舰队级告警位） |
| `dfkv_mds_group_ram_used_bytes{group}` / `dfkv_mds_group_ram_hits_sum{group}` | RAM 热层水位 / 命中 |
| `dfkv_mds_group_stats_missing{group}` | 无 STA1 上报的成员数（滚动升级进度/掉队检测） |
| `dfkv_mds_group_version_skew{group}` | 去重版本数，**>1 = 版本漂移** |
| `dfkv_mds_group_clients{group}` | 当前注册的 inference connector/consumer 实例数；client lease 到期后自动下降 |

对应 CLI：`dfkvctl topology --mds <eps> --group <g>` 通过 `kListTopology` 展示全部在线节点和 HLT1 逐设备健康。HLT1 是 Members 的已有可选扩展（placement eligibility + repeated device name/port_state/phys_state/query_ok），不是独立 rail protocol。`MembersTopologyEpoch` 对 member id/device name 稳定排序并包含地址/placement binding 与全部 health 字段；因此 rail health 变化不必重建 Ketama 环，仍会更新 transport。`dfkvctl stats` 只聚合 eligible placement；`PARTIAL` 节点仍在其中，只有零 active 的 `DEGRADED` 节点进入 degraded 计数。

原有计数：
| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_mds_ready` | gauge | TTL-debounced etcd probe；与 `/readyz` 使用同一 cache，1=可服务 membership |
| `dfkv_build_info{version,role="mds"}` | gauge | MDS 二进制版本；恒为 1 |
| `dfkv_mds_register_requests_total` / `dfkv_mds_keepalives_total` | counter | 注册 / 心跳 |
| `dfkv_mds_list_requests_total` | counter | ListMembers 次数 |
| `dfkv_mds_lease_grants_total` | counter | etcd lease 授予 |
| `dfkv_mds_etcd_errors_total` | counter | etcd I/O 失败 |
| `dfkv_mds_members` | gauge | 上次 List 返回的成员数 |
| `dfkv_mds_client_register_requests_total` / `dfkv_mds_client_keepalives_total` / `dfkv_mds_client_list_requests_total` | counter | consumer 注册 / 心跳 / ListClients |
| `dfkv_mds_clients` | gauge | 上次 ListClients 返回的 consumer 数 |
| `dfkv_mds_etcd_requests_total{op}` / `dfkv_mds_etcd_request_errors_total{op}` | counter | etcd HTTP 请求 / 传输或 HTTP status 失败，`op=lease_grant|lease_keepalive|put|range|metrics_range`；`metrics_range` 是每次 MDS scrape 同时用于 readiness 与 group 聚合的唯一 range，和业务控制面流量分开计数 |
| `dfkv_mds_etcd_request_duration_seconds{op}` | histogram | 同五类 etcd HTTP 请求延迟（50µs–60s + `+Inf`）；用于区分本地控制面排队、常规 etcd 慢响应和 scrape 自身的聚合 range |
| `dfkv_mds_local_member_leases` / `dfkv_mds_local_client_leases` | gauge | 本 MDS 进程当前缓存的 member/client lease shortcut 数；只是 etcd 权威状态的有界优化 |
| `dfkv_mds_local_member_leases_pruned_total` / `dfkv_mds_local_client_leases_pruned_total` | counter | 因多个 TTL 未在本 MDS 使用而丢弃的本地 shortcut；churn 下增长正常，下次心跳会 fresh grant+Put |
| `dfkv_mds_legacy_frames_total` | counter | 以 v1.x 旧控制面帧（42/10 字节帧）服务的请求数；仅在 `DFKV_MDS_ACCEPT_LEGACY=1` 的混合代际迁移期出现，**归零后应撤掉该 env** |

### 3.3 客户端（SGLang 插件 `/metrics` 与原生 C 快照）
SGLang 后台线程每 `client_stats_poll_s`（默认 10s，0=关）读取一次 C 客户端快照，
把下列有界 family 镜像到插件 `/metrics` 并追加 `{tp_rank}`；request path 不做
Prometheus 调用。vLLM 使用同一 allowlist，区别只是在 family 前加 `vllm:`，见 §3.5。

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_client_stats_snapshot_success` / `dfkv_client_stats_snapshot_timestamp_seconds` / `dfkv_client_stats_snapshot_errors_total` | gauge / gauge / counter | 最新 poll 是否成功 / 最后成功 Unix 时间 / 失败累计；失败时保留 last-good 数据并显式置 success=0 |
| `dfkv_client_transport_info{transport}` | gauge | live transport identity（`rdma`/`tcp`），恒为 1 |
| `dfkv_client_ops_served_total` / `dfkv_client_io_errors_total` / `dfkv_client_health_checks_total` | counter | 收到响应 / 传输失败 / endpoint health check |
| `dfkv_client_unhealthy_skips_total` / `dfkv_client_busy_suppressed_total` | counter | peer 熔断短路 / busy 后主动抑制请求 |
| `dfkv_client_peer_marked_bad_total` / `dfkv_client_peer_recovered_total` | counter | peer 熔断 / 恢复切换 |
| `dfkv_client_peer_errors_total{peer}` | counter | 逐 endpoint 错误；peer 集合在原生层硬限 4096 |
| `dfkv_client_ring_members` / `dfkv_client_mds_reachable` / `dfkv_client_mds_unreachable_polls_total` | gauge / gauge / counter | 当前环节点数 / 最近发现轮询是否可达 MDS / 不可达轮询累计 |
| `dfkv_client_dedup_hits_total` / `dfkv_client_dedup_fetches_total` / `dfkv_client_dedup_wait_hits_total` / `dfkv_client_dedup_wait_timeouts_total` | counter | host rendezvous 命中、真实 fetch、等待后命中、等待超时 |
| `dfkv_client_gpu_dedup_hits_total` / `dfkv_client_gpu_dedup_fetches_total` / `dfkv_client_gpu_dedup_wait_hits_total` / `dfkv_client_gpu_dedup_wait_timeouts_total` | counter | 同上 CUDA IPC rendezvous |

原生 `KVClient` 的收敛操作指标（`op="put|get|exist|remove"`）：

| 指标 | 含义 |
|---|---|
| `dfkv_client_op_requests_total{op}` | 公开 scalar/batch/SG 调用次数；每次调用严格加 1 |
| `dfkv_client_op_keys_total{op}` | 调用提交的 key 数；TCP fan-out、RDMA pipeline、重试和 rendezvous 不重复计数 |
| `dfkv_client_op_hits_total{op}` | put/remove 确认成功数、get 命中数、exist 存在数；shm/CUDA rendezvous 命中也计入 |
| `dfkv_client_op_bytes_total{op}` | 成功移动的 payload 字节 |
| `dfkv_client_op_latency_seconds{op}`（C 快照 histogram）/ `dfkv_client_op_max_seconds{op}`（镜像 gauge） | 完整公开调用延迟（含 rendezvous 等待和有界重试）/ lifetime max；prometheus_client 不重复注册外部累计 histogram，插件另用下述 per-batch histogram |

路由为空、peer cooldown、I/O 失败等早退仍计入 request/key（hit 为 0）。
因此这些指标可直接校验请求守恒，不应与 transport 请求数或 dedup fetch 数相等。

插件直接暴露的调用层 family（全部带 `{tp_rank}`）：

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_client_set_calls_total` / `dfkv_client_set_pages_total` / `dfkv_client_set_ok_pages_total` / `dfkv_client_set_bytes_total` | counter | v1 set 调用、提交页、成功页、字节 |
| `dfkv_client_get_calls_total` / `dfkv_client_get_pages_total` / `dfkv_client_get_hit_pages_total` / `dfkv_client_get_bytes_total` | counter | v1 get 调用、请求页、命中页、字节 |
| `dfkv_client_set_seconds` / `dfkv_client_get_seconds` | histogram | v1 batch set/get 延迟 |
| `dfkv_client_set_v2_calls_total` / `dfkv_client_set_v2_pages_total` / `dfkv_client_set_v2_ok_pages_total` / `dfkv_client_set_v2_bytes_total` | counter | side-pool v2 set 调用、提交页、成功页、字节 |
| `dfkv_client_get_v2_calls_total` / `dfkv_client_get_v2_pages_total` / `dfkv_client_get_v2_hit_pages_total` / `dfkv_client_get_v2_bytes_total` | counter | side-pool v2 get 调用、请求页、命中页、字节 |
| `dfkv_client_set_v2_seconds` / `dfkv_client_get_v2_seconds` | histogram | v2 batch set/get I/O 延迟 |
| `dfkv_client_exist_v2_calls_total` / `dfkv_client_exist_v2_probe_pages_total` / `dfkv_client_exist_v2_hit_pages_total` | counter | v2 prefix probe 调用、候选页、可用 hit-prefix 页 |

C 客户端快照还含传输级指标（RDMA 构建）：

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_rdma_client_conns_opened_total` | counter | 累计打开的 RDMA client QP |
| `dfkv_rdma_client_v2_put_writes_total` | counter | 实际发出的 v2 one-sided PUT |
| `dfkv_rdma_client_mr_regions` / `dfkv_rdma_client_mr_registered_bytes` | gauge | 所有 active rail 已成功 anchor 后才发布的 host pool MR 区域数 / 声明字节数；任一 rail 失败时两者保持上次成功值 |
| `dfkv_rdma_client_mr_registration_rejections_total` | counter | 非法 range、rail anchor 或 `ibv_reg_mr` 失败而未发布（已回滚）的声明次数 |
| `dfkv_rdma_client_adhoc_user_mr_total` / `dfkv_rdma_client_transient_user_mr_active` | counter / gauge | pool 外实际注册累计 / 当前仍存活的一次性 MR；公开调用返回后 active 必须回到调用前基线 |
| `dfkv_rdma_client_pool_mr_registrations_total` / `dfkv_rdma_client_pool_mr_registration_failures_total` | counter | shared-PD 上真实 `ibv_reg_mr` 次数 / 单 rail 显式 pool 注册失败；包含最终回滚的尝试 |
| `dfkv_rdma_client_pool_mr_active_registrations` | gauge | 进程内仍有 endpoint 引用的 shared-PD MR generations；扩容成功后 anchor/空闲 endpoint 立即释放旧代，在飞旧 endpoint 到下次 acquire/close 才释放，确保旧 range 不中断 |
| `dfkv_rdma_client_max_block_seen_bytes` / `dfkv_rdma_client_declared_max_block_bytes` / `dfkv_rdma_client_connection_min_block_bytes` | gauge | 实际请求高水位 / 逻辑安全上限 / adaptive QP 最小 class |
| `dfkv_rdma_client_connection_class_opened_total{block_bytes,depth}` | counter | adaptive二维class累计新建data QP |
| `dfkv_rdma_client_connection_class_active{block_bytes,depth}` / `connection_class_idle{block_bytes,depth}` | gauge | 当前执行中 / idle pool内的二维class分布；label集合受power-of-two class限制 |
| `dfkv_rdma_client_oversize_rejects_total` | counter | 分配、注册或发帖前因超过声明上限而拒绝的操作 |
| `dfkv_rdma_client_v2_probe_attempts_total` / `dfkv_rdma_client_v2_probe_failures_total` | counter | 必选 v2 bootstrap probe 尝试 / 失败 |
| `dfkv_rdma_client_stale_pool_retries_total` | counter | pooled QP 失败后改用 fresh connection 的重试 |
| `dfkv_rdma_client_cross_rail_retries_total` | counter | 无 label；client-local `kRailFailure` 后真正启动的 cross-rail logical retry。GET 可重试未完成 work；PUT 只有 request 未 post 才计入 |
| `dfkv_rdma_client_cross_rail_retry_successes_total` | counter | 无 label；上述 retry 最终成功的 logical operation 数 |
| `dfkv_rdma_client_cross_rail_retry_exhausted_total` | counter | 无 label；上述 retry 的第二次物理尝试仍失败 |
| `dfkv_rdma_client_completion_timeouts_total` | counter | 消耗完一次绝对 completion-window deadline 的窗口；部分完成不重置预算 |
| `dfkv_rdma_client_endpoint_budget{kind}` / `qp_budget{kind}` / `wr_slot_budget{kind}` / `registered_slot_bytes_budget{kind}` | gauge | `kind=used|limit` 的进程级资源占用/上限；registered bytes 按每 endpoint 的 server receive + pull 两个 arena 计量 |
| `dfkv_rdma_client_resource_budget_raises_total` | counter | 预算自适应放大次数（随 ring 采纳，只增不减；任一预算 env 显式设置时恒为 0） |
| `dfkv_rdma_client_admission_failures_total` | counter | 本进程预算饥饿导致的操作失败（返回 kResourceExhausted，对端从未被拨号、不进冷却）；应恒为 0，>0 说明预算仍小于扇出需求 |
| `dfkv_rdma_client_depth_refunds_total` | counter | 服务端 clamp depth 后按协商值退还 WR/注册字节预算的连接数（首连按上限探测，后续按学习值建连） |
| `dfkv_rdma_client_topology_nodes` | gauge | 最近一次喂给预算自适应的 ring 规模（0=从未提示或自适应关闭） |
| `dfkv_rdma_client_peer_topology_updates_total` | counter | 无 label；接纳的新 peer immutable topology generation 数；同 generation 重复通知不增加 |
| `dfkv_rdma_client_no_compatible_rail_total` | counter | 无 label；configured tiers 因 absent/incomplete peer topology 或最高可用交集为空而返回 `kNoCompatibleRail` 的次数；不计 peer health failure |
| `dfkv_rdma_client_stale_generation_reaps_total` | counter | 无 label；因 Conn 的 peer generation 落后而拒绝 take/repool/keepalive、并经 single-owner lifecycle 回收的 endpoint 数 |
| `dfkv_rdma_client_keepalive_attempts_total` / `dfkv_rdma_client_keepalive_successes_total` / `dfkv_rdma_client_keepalive_failures_total` | counter | idle pooled QP 的保活尝试 / 成功 / 失败并退役连接；仅 `DFKV_RDMA_KEEPALIVE_MS>0` 时增长 |
| `dfkv_rdma_client_rail_conns_total{dev}` / `dfkv_rdma_client_rail_selections_total{dev}` | counter | 每 rail 新连接 / 准入分布 |
| `dfkv_rdma_client_rail_put_ops_total{dev}` / `dfkv_rdma_client_rail_put_bytes_total{dev}` | counter | 每条本地rail成功完成的逻辑PUT对象数/有效payload字节；跨多个WR的SG对象只计一个逻辑op |
| `dfkv_rdma_client_rail_get_ops_total{dev}` / `dfkv_rdma_client_rail_get_bytes_total{dev}` | counter | 每条本地rail成功完成的逻辑GET对象数/实际返回payload字节；用于验证rank-local affinity是否真正承载业务流量 |
| `dfkv_rdma_client_rail_inflight{dev}` / `dfkv_rdma_client_rail_credits_available{dev}` | gauge | 当前已租 / 可用 request credits |
| `dfkv_rdma_client_rail_credits_exhausted_total{dev}` | counter | 因 local candidate credit 不足跳过次数 |
| `dfkv_rdma_client_rail_errors_total{dev}` / `dfkv_rdma_client_rail_consecutive_errors{dev}` | counter / gauge | **仅本地** device open、QP transition、MR register/refresh、post/CQ verbs API 失败和 local WC 累计 / 当前连续值 |
| `dfkv_rdma_client_endpoint_errors_total{dev}` | counter | peer bootstrap/不可达/probe/protocol/receive-segment/decode、remote/RNR/retry/timeout/flush WC；credit 已归还，**不惩罚或恢复 rail**。没有 local-error WC 的 completion deadline 也属 endpoint，不触发 cross-rail retry |
| `dfkv_rdma_client_rail_quarantines_total{dev}` / `dfkv_rdma_client_rail_quarantined{dev}` / `dfkv_rdma_client_rail_recovery_probe{dev}` / `dfkv_rdma_client_rail_recoveries_total{dev}` | counter / gauge / gauge / counter | 连续 3 次 client-local failure 后隔离（默认 cooldown 5000 ms）/ 隔离状态 / cooldown 到期后唯一真实 probe / probe 成功恢复；probe local failure 重启 cooldown，endpoint failure 只释放 probe ownership |
| `dfkv_rdma_client_numa_fallbacks_total{reason="caller_unknown\|no_local_rail"}` | counter | `DFKV_RDMA_NUMA=1` 无法建立 local mask 而回退全部 enabled rails |
| `dfkv_rdma_cq_completions_total` / `dfkv_rdma_cq_errors_total` | counter | client CQ 完成 / 错误完成 |
| `dfkv_rdma_client_pipeline_depth` | gauge | 握手解析后的有效 pipeline depth |
| `dfkv_rdma_client_pool_connections{lane,state=\"idle|active\",dev}` | gauge | endpoint 按最后/当前 operation lane、ownership state 和 rail 分解；active 不含已隔离连接 |
| `dfkv_rdma_client_peer_connections{peer,dev}` | gauge | 当前 live endpoint 按 MDS stable peer identity 与本地 rail 分解；连接销毁后 series 删除，cardinality 受 endpoint budget 上限约束 |
| `dfkv_rdma_client_pool_limit` | gauge | 每 peer/pool 的 idle retention 基线（默认 8）；未显式配置 POOL_MAX 时 data pool 可短期保留至 64，超过基线的应用空闲连接在 60 秒后退休，仍受进程资源预算限制 |

TCP 构建或 TCP fallback 的 C 快照 family（同样由插件镜像）：

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_transport_pool_connections` / `dfkv_transport_pool_inflight` | gauge | 当前 pooled 连接 / 在飞 acquire |
| `dfkv_transport_pool_selections_total` | counter | routed endpoint 的 pooled connection 选择数 |
| `dfkv_transport_pool_retirements_total{reason}` | counter | 连接退休数；`reason=idle|error` |
| `dfkv_transport_pool_backoff_endpoints` | gauge | 当前阻止连接增长的 endpoint 数 |
| `dfkv_transport_pool_backoff_events_total` / `dfkv_transport_pool_backoff_suppressed_total` | counter | 进入 backoff / backoff 中被 fast-fail 的 acquire |

RDMA client transport family 只在 RDMA runtime 使用相应传输并由 client snapshot/
connector poller 导出时可见；TCP-only 进程缺失是预期行为。所有新 peer-topology
family 均是无 label process counter，避免 peer 地址造成无界时序；`{dev}` 仍只
来自进程启动时的固定 local rail 集合。`kNoCompatibleRail` 是本地兼容性结果，
不增加 `dfkv_client_ops_served_total`、`dfkv_client_io_errors_total`、
`dfkv_client_peer_marked_bad_total` 或 cooldown。endpoint 和 local-rail health
仍按各自既有 family 分开告警。

SGLang HiCache插件另导出：

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_hicache_parallel_identity{tp_rank,physical_rank,pcp_rank,dcp_rank,storage_pcp_rank,primary_dev}` | gauge | 每个connector进程的并行坐标、node-local物理rank、存储PCP writer坐标和primary rail；`physical_rank=-1`表示affinity关闭 |
| `dfkv_hicache_exist_probes_total{tp_rank,result}` | counter | `result=full_miss|partial_prefix|full_hit`的exist probe次数 |
| `dfkv_hicache_exist_probe_pages_total{tp_rank}` | counter | 候选page总数 |
| `dfkv_hicache_exist_present_pages_total{tp_rank}` | counter | probe批内任意位置存在的page总数 |
| `dfkv_hicache_exist_contiguous_pages_total{tp_rank}` | counter | 从首页开始连续、可供SGLang使用的page总数 |
| `dfkv_hicache_exist_seconds{tp_rank}` | histogram | connector整次`batch_exists`耗时 |

`present_pages - contiguous_pages`直接表达“后页存在但前缀有洞”；不要用
`prefetched_tokens_total`反推物理RDMA字节。轨道真实读写用上述per-rail bytes。

**retry 语义与告警**：

- `cross_rail` counter 只统计 client-local `kRailFailure` 后真正启动的跨轨 retry；
  pooled endpoint staleness仍由 `stale_pool_retries_total` 单独计数。每个 logical
  operation 捕获一个 peer generation，物理尝试最多 2 次。
- GET/batch/SG 保留已完成 item/window 的 byte-exact staged 结果，fresh 第二次只
  提交未完成集合。PUT 仅在明确 request 尚未 post 时允许换轨；post 后 completion
  不明的 PUT 不 cross-rail replay，避免扩大 ambiguous write 语义。
- Conn 绑定 peer generation；pool 中旧代 endpoint 永不复用或回池，并通过既有
  single-owner lifecycle 销毁。topology update 不在别的线程直接双重 teardown。
- `Status::kNoCompatibleRail` 是 client-local（不上 wire）、peer-health neutral：
  不增加 served、IO error、marked-bad 或 cooldown。configured tiers 遇 absent/
  incomplete HLT1 或空健康交集时返回它；未配置 tiers 的 legacy homogeneous 模式
  不需要完整 peer topology。
- `rate(dfkv_rdma_client_cross_rail_retry_exhausted_total[5m]) > 0` 或持续
  `rail_quarantined{dev} == 1` 需要处置。workload drain 后 inflight 或 transient
  MR 不归零按资源泄漏告警。

该能力不新增 rail wire protocol：MDS 复用 Members HLT1，transport 数据面仍是
RDMA v2。新 client + 当前 MDS + legacy member 在未配置 tiers 时保持
homogeneous 兼容；configured tiers 对该 member fail closed。legacy MDS 不识别
`kListTopology`，modern poll 失败并保留 last-good ring，rollout 必须 MDS-first，
不能把这类 poll failure 解释为 homogeneous fallback。

### 3.4 连接器车队指标（三连接器 OTLP **push**，opt-in）
§3.3 是进程本地 Prometheus **pull**。vLLM、LMCache、SGLang HiCache 还可把
同一份聚合状态经 OTLP push 到中心 Collector，用一个 dashboard 按实例、引擎、
模型、部署、版本和节点过滤。

- 开关：`DFKV_METRICS_ENABLED=1`；关闭时 recorder 是冻结 no-op singleton，
  不 import OTel，不启动线程。
- 默认 `DFKV_METRICS_EXPORTER=stdlib` 使用纯标准库 **OTLP/HTTP JSON**，
  endpoint 必须是 HTTP receiver（通常 `http://collector:4318`）；自动补
  `/v1/metrics`。把标准 gRPC 端口 `4317` 交给 stdlib exporter 会启动告警，
  Collector 也会因协议不匹配拒绝请求。
- 显式 `DFKV_METRICS_EXPORTER=otel` 才使用 OpenTelemetry SDK，可按 SDK 配置
  gRPC `4317` 或 HTTP `4318`。详细接法见
  [`../deploy/observability/CONNECTOR-USAGE.md`](../deploy/observability/CONNECTOR-USAGE.md)。
- OTLP resource attribute 是 `dfkv.connector_type`、`dfkv.connector_id`、
  `dfkv.host`、`dfkv.pid`、`dfkv.tp_rank`，以及有值时的 `dfkv.version`、
  `dfkv.native_version`、`dfkv.model`、`dfkv.deployment`、
  `dfkv.cache_role`、`dfkv.team`。示例 Collector 开启
  `resource_to_telemetry_conversion` 后，Prometheus label 对应为
  `dfkv_connector_type`、`dfkv_connector_id` 等。
- 同进程多个 connector 共享 recorder/tracer 并按实例引用计数；最后一个 close
  停止 native poller 后同步 final export。失败的窗口最大值和 native counter
  delta 保留到下一次成功推送，不会因 snapshot/reset 顺序丢失。

| 指标 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `dfkv_connector_ops_total` / `dfkv_connector_keys_total` / `dfkv_connector_bytes_total` | counter | `op` | Python connector 完成的调用、key、payload bytes |
| `dfkv_connector_op_seconds` | histogram | `op` | connector 调用延迟 |
| `dfkv_connector_op_max_seconds` | gauge | `op` | 上一导出窗口观察到的最大 connector 调用延迟 |
| `dfkv_connector_info` | gauge | — | connector 活跃心跳，值为 1；身份在 resource labels |
| `dfkv_connector_client_stats_poll_success` / `dfkv_connector_client_stats_poll_errors_total` / `dfkv_connector_client_stats_last_success_unixtime` | gauge / counter / gauge | — | 最新原生快照 poll 是否成功 / 失败累计 / 最后成功时间；失败保留 last-good |
| `dfkv_connector_op_requests_total` / `dfkv_connector_op_keys_total` / `dfkv_connector_op_hits_total` / `dfkv_connector_op_bytes_total` | counter | `op` | C++ KVClient 收敛 chokepoint 的公开调用计数 |
| `dfkv_connector_op_latency_seconds` / `dfkv_connector_op_lifetime_max_seconds` | histogram / gauge | `op` | C++ 完整调用延迟 / lifetime max |
| `dfkv_client_peer_latency_avg_seconds` / `dfkv_client_peer_latency_max_seconds` | gauge | `peer` | 当前 poll 窗口平均值 / lifetime max |
| `dfkv_client_peer_latency_seconds_count` / `dfkv_client_peer_latency_seconds_sum` | counter | `peer` | C++ 累计 peer probe 样本数 / 时长 |
| `dfkv_connector_dedup_hits_total` / `dfkv_connector_dedup_fetches_total` / `dfkv_connector_dedup_wait_hits_total` / `dfkv_connector_dedup_wait_timeouts_total` | counter | — | host rendezvous dedup 漏斗 |
| `dfkv_connector_gpu_dedup_hits_total` / `dfkv_connector_gpu_dedup_fetches_total` / `dfkv_connector_gpu_dedup_wait_hits_total` / `dfkv_connector_gpu_dedup_wait_timeouts_total` | counter | — | CUDA IPC rendezvous 漏斗 |

其余原生状态/传输 family 由 §3.3 的 allowlist **一一**导出，名称映射是公开契约：

| C 快照前缀 | OTLP/Prometheus 前缀 | 范围 |
|---|---|---|
| `dfkv_client_` | `dfkv_connector_` | health、ring、MDS；不含由上表单独转换的 op/dedup/peer |
| `dfkv_rdma_client_` | `dfkv_connector_rdma_` | QP、MR、rail、NUMA、timeout、pipeline |
| `dfkv_rdma_cq_` | `dfkv_connector_rdma_cq_` | CQ completion/error |
| `dfkv_transport_pool_` | `dfkv_connector_transport_pool_` | TCP pool、retirement、backoff |

例如 §3.3 的 `dfkv_rdma_client_completion_timeouts_total` 对应
`dfkv_connector_rdma_completion_timeouts_total`。逐 endpoint
`dfkv_client_peer_errors_total{peer}` 不进入 OTLP：动态 peer 集合虽在 C++ 内有
4096 上限，仍不适合作为全车队中心序列；中心诊断使用固定 rail family 和聚合
peer health counter。

逐 peer 延迟由 C++ 主动探测（`DFKV_PROBE_INTERVAL_MS`）：SGLang HiCache 开
metrics 时自动启用；vLLM/LMCache 需显式配置非零 interval。

### 3.5 vLLM 本地客户端指标（`dfkv-vllm` `/metrics`，pull）
vLLM 的 `DFKV_CLIENT_STATS_POLL_S`（默认 15s，0=关）镜像 §3.3 中**全部**
allow-listed counter/gauge，而不是只镜像 ring/MDS。名称固定为
`vllm:<C快照原名>`，并追加 `{tp_rank}` 与源 family 的 `{op|peer|dev|reason|transport}`。
例如：

| 指标 | 类型 | 含义 |
|---|---|---|
| `vllm:dfkv_client_ring_members` / `vllm:dfkv_client_mds_reachable` | gauge | 空环 / MDS 可达性 |
| `vllm:dfkv_rdma_client_rail_errors_total{tp_rank,dev}` | counter | 每 rank、每 rail 本地错误 |
| `vllm:dfkv_transport_pool_connections{tp_rank}` | gauge | TCP pool 当前连接 |
| `vllm:dfkv_client_stats_snapshot_success` / `vllm:dfkv_client_stats_snapshot_timestamp_seconds` / `vllm:dfkv_client_stats_snapshot_errors_total` | gauge / gauge / counter | poll last-good 健康；无需开启 OTLP telemetry |

### 3.6 成员一致性巡检 textfile 指标

`deploy/dfkv_membership_audit.py --prom-output <path>` 原子写 node_exporter textfile
格式。它是巡检结果，不是 daemon 热路径指标：

| 指标 | 类型 | 含义 |
|---|---|---|
| `dfkv_membership_audit_ok{group}` | gauge | 每个 MDS placement view、etcd 注册、自报 INFO、ring epoch 全部一致时为 1 |
| `dfkv_membership_audit_issues{group,severity}` | gauge | 最近一次巡检的 critical / warning 数 |
| `dfkv_membership_ring_epoch{group}` | gauge | 按 placement 内容计算的 FNV-1a ring epoch（主要用于变更关联；Prometheus float 可能舍入 64-bit 低位） |
| `dfkv_membership_registration_revision{group,node}` | gauge | etcd 中该节点注册 key 的最新 `mod_revision`，可关联停止更新/大面积重注册事件 |

建议 cron/systemd timer 每分钟运行一次，命令本身用 `--timeout 5`；同时对
`dfkv_membership_audit_ok == 0` 或脚本退出非零告警。textfile 有上一次成功文件不代表
本次 timer 成功，必须另外监控 timer exit/文件 mtime；不要只看陈旧的 `ok=1`。
split-brain、MDS/etcd 漂移和自报不一致均由脚本以 `critical` + exit `2` 给出，
JSON `issues[].code/message` 包含具体 endpoint/node 和处置方向。

## 4. 性能保证

- 热路径计数：`std::atomic` relaxed `fetch_add`，零锁零分配（与既有模式一致）。
- 延迟：1/64 采样（`Sampler` 掩码），仅采样到的 op 读 vDSO 时钟。
- HTTP `/metrics`：独立端口 + 线程，仅 scrape 时把原子读成文本。
- 客户端计数搭在 `PeerHealth` 既有锁段上（无新增锁）；per-peer 仅错误路径（罕见）。
- 插件轮询线程睡眠态、按间隔触发，不在 per-batch/per-page 路径。
- 验证：每 PR 跑 RDMA Soft-RoCE loopback + ThreadSanitizer 全绿。

Lifetime vector backing gauges use one O(1) allocator-locked snapshot shared
with the existing bucket/token gauges. They include unused retained capacity:
Remove/rebind clears ownership but does not promise to release vector backing.
Clean/dirty recovery constructs fresh vectors; capacity is not persisted or
promised equal across restarts. Identity-only and internal shadow configurations
report their actual allocations rather than policy-based synthetic zeroes.
Global values sum disks; independently sampled global/per-disk scrapes are not
an atomic cross-disk snapshot. Existing bucket series retain their meanings.
These gauges exclude vector objects, allocator metadata, fixed counters, base
Class/ExtentMeta/SlotMeta/key-index storage, transient reallocation copies and
recovery/process peaks. Their sum is not complete lifetime incremental memory
or RSS, and does not satisfy the full layout/peak acceptance gate. No key or
request labels are added; existing dashboards and alerts keep their series.

Base metadata array gauges separately report slots, extents, canonical classes
and the flat key-index table through one O(1), allocator-locked
`MetadataMemoryUsage()` snapshot. They use vector capacity, not live count or
logical hash-bucket count. Deleting residents leaves retained backing visible;
recovery rebuilds arrays and does not preserve previous high-water capacities.
Legacy slab modes also report actual nonzero base storage; file engines omit
these families. Global values sum disks through existing Stats aggregation;
per-disk and global Stats are separate samples, not a cross-disk transaction.
The four arrays are disjoint from the lifetime-vector backing above. However,
`sizeof(Class)` covers the contained map/vector objects, not their allocations:
free-slot map nodes/buckets remain excluded, while per-extent free-slot and
rotation vector backing is reported separately by the two nested-vector gauges. Fixed allocator objects, heap overhead/fragmentation, temporary
reallocation copies, peak/recovery memory and RSS are also excluded. Even the
combined gauges therefore do not prove complete allocator or incremental
lifetime memory. The original four-array snapshot added one O(1) A-lock
acquisition per GetStats and 32 bytes to Store Stats. The two nested-vector
gauges reuse that acquisition, add 16 bytes to Store Stats and the metadata
snapshot, and maintain 16 fixed bytes per allocator at existing vector
growth/erase sites. No per-resident state is added. These costs have not been
declared within performance budgets. Existing alerts and
dashboards do not depend on the new families and are not renamed.

The original three-field `SlabAllocator::LifetimeBucketMemory` and
`BucketMemory()` remain available; the expanded `LifetimeMemoryUsage()` is a
separate API. This does not promise binary compatibility for all internal C++
cache types: `DiskSlabStore::Stats` grows with these metrics. C++ consumers of
cache internals must rebuild with matching headers and libraries; do not mix old
Stats-returning callers with a new library. The public C API and disk format do
not change.

### Lifetime queue populations

These are physical index memberships: `band = min(scoring_reads + fw,
MAX_READS)`, not W/R/O logical classes, actual read counts, current scores or
protected populations. Time passing alone does not move a resident's physical
band. Clean-recovered virtual-two-read residents are still logically unread
but occupy band `min(2, MAX_READS)` until a real event changes membership.

Each enabled disk emits all `MAX_READS + 1` global bands including zeros, plus
all bands for classes with allocator residents. Class rows disappear when that
class has no residents; no fixed five-band assumption is made. Uncommitted or
uninitialized/null-state residents are not indexed, so indexed totals can be
less than allocator resident count. Hidden pinned or isolated residents retain
membership until physical release, including failed metadata invalidation.
At one disk snapshot, each global band equals the sum of that band's class
rows; do not add both indexes together as if they represented different keys.
The disk snapshots are coherent under A, but not simultaneous across disks or
atomic with separately sampled payload/population metrics.

Counters are maintained only at existing link/unlink sites under A. Metrics
use a separate A-locked queue snapshot, costing O(canonical classes + emitted
queues), with up to `(active resident classes + 1) * (MAX_READS + 1)` rows per
disk. No resident traversal, clock read or deadline processing is introduced.
This is not O(1) or zero overhead: each queue gains a uint64 count (including
alignment effects in actual sizeof), link/unlink does extra fixed arithmetic,
and a scrape adds an A-lock acquisition, temporary rows/aggregation and output.
The existing queue-backing byte gauge uses actual sizeof and reflects this
allocation change. Shared ClassStat/Stats, ordinary Classes(), RAM reclaim and
business snapshot paths are not expanded to carry band arrays. Disabled legacy
slab has no queue samples; file omits the slab families. Internal shadow
allocators return actual enabled index populations without claiming production
policy activation. Internal C++ cache consumers still require matching headers
and libraries; the C API, disk format and existing snapshot interfaces remain.

### Current protection-bucket estimates

An enabled disk keeps a B-entry summary across its per-extent expiry rings.
Observations sum only generations strictly newer than `now / G`, matching the
existing donor coarse estimator at current nondecreasing time; this is not a
historical arbitrary-time query. Expiry rounding is conservative: a resident
whose exact score has fallen to KEEP may remain estimated protected until its
bucket boundary. Counts include pinned/hidden/isolated residents while their
physical registrations remain; ordinary time expiry can still remove them from
the current estimate without freeing their slots. Only exact resident checks
can establish actual protected loss or victim eligibility.

Registration, unregistration and extent reset update the summary under the
existing A lock. Older delayed generations never replace newer summaries;
stale tokens/rows subtract only matching generations. The estimate query is
O(B), B<=64, not O(extents*B) or a resident scan. It acquires A and samples the
owner's shared bounded clock once. Sampling advances that clock's sticky
monotonic floor and can close future admission; it does not admit requests,
heat residents, rotate expiry pages or change donor selection.

The estimate remains available through the soft admission boundary, including
valid already-admitted drain time. Beyond UINT32_MAX it becomes unavailable;
clock rollback cannot restore availability or reopen admission. Summary backing
bytes remain visible, but numeric estimates are omitted rather than emitted as
zero or cached stale values. Disabled legacy/shadow buckets are unavailable;
file omits slab metrics. Each aggregate uses the same per-disk observations,
not a second sampling pass, but disks and other gauges are not simultaneous.

The global summary adds an empty vector object even to disabled allocators and
B actual bucket elements only to enabled ones, with fixed extra arithmetic at
registration/removal/reset. This is not zero memory/latency cost. Existing
bucket bytes still cover only per-extent backing; summary_bytes is separate.
Neither includes allocator overhead or recovery/RSS peaks. Ordinary
Classes/ClassStat/Stats and all previous snapshot return layouts are unchanged;
internal C++ consumers still need matching headers/library builds.


### PULL grant preparation-path subpartition

The original six `lifetime_read_transport_*_total` counters subdivide the existing
`lifetime_read_source_transport_grant_total`; within a Store snapshot their
sum equals that coarse counter. The original six-source sum still equals
successful logical notifications, and the independent heat-outcome partition
is unchanged. Do not add transport children to the original source sum again.
All preparation counters, including nested fallback children, reset on Store
reconstruction; virtual recovery reads add nothing.
Legacy without lifetime tracking exports zero and file engines omit them.
Per-disk series use only the existing disk/disk_index labels. Global/per-disk
samples are separate; compare cross-disk sums at quiescence.

`PreparedRead::Source` records the preparation route, not registered-memory
state or physical byte provenance. A disk read directly targeting a RAM
reservation stays disk_to_ram after MR fallback copies its bytes into staging.
Ordinary RAM hits stay ram_hit when copied to a nonregistered buffer. A
published_follower means the request waited for publication then acquired a
RAM hit; a concurrent new PUT may have replaced the original leader resident,
so this tag is not proof of exact shared-leader identity. Existing resident
credentials remain authoritative. Synchronous fallback has its own category,
subdivided by five `lifetime_read_transport_fallback_*_total` counters. Their
sum equals synchronous_fallback; never add these nested children to the
six-way transport sum. RAM/disk/copy leader/copied-follower labels come from
the actual synchronous handler result, not identity validity, promotion or
registered-memory state. Timeout, self-recursion, failed-flight and no-shared-
bytes fallbacks are independent disk reads, not copied followers. The Node's
RangeDirectForKey resets its source output to Unclassified for empty or failed
reads. Legacy adapters retain their unknown-route fallback tag regardless of
status; neither failed nor empty grants count. Old eight/nine-argument range
setters still work through adapters and use fallback_unclassified;
production uses the source-aware range setter. The new fallback counters share
the existing reset/disabled/empty/health-rejection semantics.
Move transfers the tag, while Stage and terminal operations preserve it.

Only the established nonempty grant's existing once-only callback counts the
event before READY encoding. Preparation, physical storage completion alone,
empty or failed grants, repeated grant-recording attempts for the same grant, RELEASE
and teardown do not add events. Grant establishment does not prove READY
was delivered or data consumed remotely. The old three-argument success
handler remains source-compatible through a small adapter that discards the
tag; production uses the source-aware setter. Internal C++ cache types still
require matching rebuilt headers/libraries; no C ABI or wire value changes.

The new partition adds six uint64 counters (48 bytes per Store and its Stats
snapshot) and fixed arithmetic inside the existing D lock, with no new
resident field, read-path lock, clock or scan. PreparedRead adds one
per-request enum field with platform-dependent alignment cost, not a
per-resident allocation. Twelve new fixed metric families add scrape/output
cost. None of this establishes latency, RSS, recovery-peak or complete-memory
budget compliance. Existing dashboards and alerts retain their names.


### Store-owned synchronous payload pread accounting

The four `sync_payload_read_*_total` counters describe syscall activity owned by
Store Range/RangeInto/RangeDirect, in both legacy and lifetime slab modes.
They reset when the Store is reconstructed and are absent for file engines.
They are not an outcome partition: bytes has a different unit; a short positive
return adds both bytes and short_reads, and a retried EINTR adds both a syscall
and an error even if the read eventually succeeds. EOF adds a syscall and a
short read, but not a negative-result error. Zero-length, missing or invalid
ranges that issue no pread add nothing.

A direct read reports the kernel's aligned-window return bytes, which can
exceed the logical range. Capacity/alignment fallback chooses buffered I/O
before a DIO syscall, not after a failed DIO read; count only calls actually
made. Buffered and direct calls share these counters. A successful payload
syscall still counts if final-pin deferred invalidation later turns the GET
into IOError; such a metadata failure does not become a payload syscall error.

Only the four payload callsites provide an accumulator. Startup preflight,
meta/init-marker/epoch/table reads, recovery scans, writes, RAM hits and shared
copy followers do not add payload syscalls. A synchronous coalescer leader or
independent fallback does. RangeDirectPrep/PreparedRead preparation, release,
external descriptor pread and io_uring CQ completions are excluded, even if an
external descriptor owner executes pread synchronously. This is a deliberately
bounded synchronous series, not complete asynchronous payload I/O, device I/O,
page-cache misses or storage amplification. Async accounting remains pending.

Each request accumulates locally and merges all four fields under its existing
Store finalization lock before releasing the last pin. No new per-syscall lock,
atomics, clock read or per-resident fields are added. Pending reads do not appear
until finalization; the four new fields are copied together under the same
Store lock, not a claim that every existing Stats field shares that snapshot.
Global/per-disk series use only existing disk/disk_index labels and are sampled
separately; compare sums at quiescence. Fixed counters, request-local work and
additional metrics output have nonzero costs; this is not a memory/latency
budget or full RSS/recovery-peak acceptance result.

### io_uring reader-local completion diagnostics

`UringReader::read_counters()` provides an owner-thread-only cumulative snapshot
of consumed read CQEs: `completions`, positive returned `bytes`, negative-result
`errors`, and nonnegative `short_reads` (including EOF). Shortness is compared
with the current residual request, before advancing its completed byte count.
One short CQE followed by EOF therefore counts two completions and two shorts;
partial bytes remain counted if a later CQE fails. These fields are not a
mutually exclusive outcome partition.

Both normal `Reap` and shutdown/error `Drain` count consumed CQEs. Peeking,
waiting, submission retries/errors, and dormant unsubmitted SQEs do not count.
Partial Drain timeouts retain already consumed counts without counting them
again on retry. Drain still discards results without retrying shorts. An unknown
token contributes completions/bytes/errors, but cannot contribute short reads
because its request length is unavailable. Such a token remains an existing
infrastructure failure on the normal Reap path.

These local diagnostics are **not exported Prometheus metrics**, per-disk Store
counters, syscall counts, device I/O counts, or proof of RDMA/RXE hardware
acceptance. They reset with each reader instance; destructor-only drain totals
are not externally published. The existing `dfkv_uring_reads_total` and the
Store-owned synchronous payload counters retain their previous meanings.
Per-disk attribution for participating readers is described below. Arbitrary
externally executed PreparedRead I/O remains outside that accounting. No I/O is
inferred from logical lease-release bytes.

### Prepared io_uring payload attribution

The eight `dfkv_slab[_disk]_uring_payload_read_*_total` families attribute explicit
read CQEs to the original owning slab through its storage lease. Normal Reap and
Drain update request-local counters at each consumed CQE. The lease publishes
all four fields under the Store's existing finalization lock before releasing
the final disk pin. Reads still in flight or completed but not yet released are
not yet visible in these Store metrics. Partial Drain timeouts retain ownership;
later Drain/destructor completion precedes lease release. Even if deferred Remove
invalidation changes the final GET result to an error, completed I/O is retained.
Moves and repeated terminal calls do not duplicate publication.

Only descriptors explicitly attached to the prepared lease's counter sink are
covered. The RDMA io_uring pipeline binds that sink after stable queue insertion
and before submission, including batches that are only partly admitted. Dormant
unsubmitted SQEs contribute zero. Unknown CQE tokens cannot be guessed into a
disk. Ordinary external fd pread and independently implemented io_uring executors
remain excluded unless they explicitly participate; logical lease bytes never
substitute for actual I/O. These metrics are not total device I/O, grants, logical
read success, or proof of hardware acceptance.

Both legacy and lifetime slabs report actual participation; file engines do not
export these slab families. Store reconstruction starts them at zero, including
clean recovery. Global and per-disk series are sampled separately, so equality
of sums is guaranteed only when quiescent. They use existing disk/disk_index
labels, not per-request or per-key labels. Existing synchronous payload,
`dfkv_uring_reads_total`, heat and grant counters retain their meanings.

Internal counter pointers are borrowed, owner-thread-only, and invalidated by
moving or releasing their PreparedRead. The owner must remain stable until all
kernel operations drain. This is not a cross-thread mutable metrics API. Internal
C++ lease/descriptor layouts grow; matching headers and libraries must be rebuilt.
No wire/disk format or policy default changes are implied.

### PULL read-notification once-boundary outcomes

The four `dfkv_rdma_pull_read_notification_*_total` counters classify calls to the
existing post-validation/MR/grant `RecordPullGrant` boundary. Each call contributes
exactly once: an already-consumed generation is `duplicate`; a fresh generation
is consumed at the original point and classified as `empty` before checking
`no_observer`, otherwise `notified` before invoking the callback. Empty/missing
observer generations cannot later be replayed when an observer is installed.
Canceled or failed preparation that never reaches this boundary contributes zero.
The callback's existing thread-safe/nonthrowing and before-Start setup contract
is unchanged; these metrics do not add live handler reconfiguration support.

In a quiescent server, the sum of all four is the number of helper calls, and
`notified + empty + no_observer` counts fresh generations consumed there. The
fields are sampled by independent relaxed loads, not a coherent cross-counter
snapshot. They are scoped to each RdmaServer instance, start at zero on new
construction, and have no key/request/disk labels. They are not all GETs, unique
keys, all transport attempts, cross-protocol dedup coverage or per-disk heat.

`notified` records an observer invocation, not successful application or callback
completion. Legacy/no-binding/changed-resident/clock/IO conditions can leave the
Store unheated; READY send failure never retracts an invocation. The once bool
is still connection-owner state: atomic metrics do not make concurrent sharing
of that bool legal. Each thread/request must retain its original ownership.

Instrumentation adds one shared relaxed atomic RMW per helper call, including
normal nonempty PULLs. There is no new clock or lock, but contention/cost is not
zero and no latency/throughput budget acceptance is implied. Notification delay,
lock time and non-PULL once-boundary coverage remain separate O5 work. Real
RDMA/RXE acceptance is not established by CPU post-MR-boundary tests.


### Nested class vector backing

The two `metadata_class_*` vector gauges above apply to all slab policies. The
allocator maintains capacity deltas under its existing lock when vectors grow
or are erased, so the existing metadata snapshot remains O(1), without scanning
slots/extents at scrape time. Ordinary pops/removes do not shrink vectors;
extent rebinding can release free-slot backing while retaining rotation backing.
Clean recovery recomputes capacities from reconstructed state, not persisted
capacity values. Global values sum disk snapshots; compare separately collected
snapshots only at quiescence. File mode omits the slab families. No key/request
labels or new hot-path locks, atomics or clocks are added.

The counters add two fixed uint64_t fields per allocator (including RAM) and two
fields per metadata/Store snapshot, not per resident. Existing Class-array bytes
already include vector/map objects, but not their allocations; the new gauges
cover vector elements only, avoiding double counting those objects. Map nodes,
buckets, heap overhead, other fixed objects, RSS and recovery peaks remain
outside this partial accounting. This is not a full-memory/incremental lifetime
budget or performance acceptance result.


### Successful-read residency rejection reasons

The four `read_race_*` children form a nested partition of the existing
`read_race_lost_total`, not extra terms in the six-outcome success sum:

```
race_lost = race_identity_scope + race_not_visible
          + race_generation_mismatch + race_allocator_unavailable
```

Rejection priority remains I/O isolation, missing/invalid binding, identity
scope, visible payload, allocator identity/live/pending guards, generation,
then clock/state and application. Disabled lifetime tracking and empty reads
still add zero. Rejected credentials do not sample the application clock,
change heat, move scoring queues or alter expiry buckets. Coarse/source counts
and caller GET status are unchanged. These diagnostics do not establish that a
concurrent race occurred: supplied foreign credentials also fail validation.
They do not measure notification coverage, deduplication, latency or lock cost.

All fields share the existing Store lifecycle lock and snapshot, with the
existing single allocator lookup under its original lock. A rejection adds one
plain integer increment; the applied path adds no new counter update. Four
uint64_t fields add 32 fixed bytes per Store and read-counter/Stats snapshot,
not per resident or request. Disk labels identify the receiving/validating
configured key route, not proof of the physical source of returned bytes.
Global values sum disk snapshots; compare separate samples only at quiescence.
Counters reset on Store reconstruction; recovery virtual reads do not increment
them. Legacy without lifetime scoring exports zero and file mode omits slab
families. Existing alerts/dashboards keep the coarse family unchanged; do not
sum its children with their parent or treat these as completed E2 acceptance.


### Sampled notification processing cost

Sampling reuses the existing D-protected successful-notification count: first,
65th, 129th, etc. Empty or disabled notifications do not advance it. A local
noncopyable timer is constructed after the Store lock and successful increment;
it records before that lock is released, including all normal early returns.
Only selected notifications read `steady_clock` twice. No new shared sampler,
atomic RMW, lock, request/resident field or scoring-clock read is introduced.
Three plain counters share the existing D lock and Stats snapshot; reset on
reconstruction, recovery virtual reads add zero. Legacy without lifetime
tracking reports zero; file mode omits these slab families. Existing disk/node
labels remain, with no key/request labels.

The interval is processing-body elapsed wall time, not full D hold time, D
acquisition wait, completed-to-applied delay, CPU time, READY/client consumption
or GET latency. Fixed-period sampling can correlate with periodic workloads;
it is not random/unbiased. Do not multiply the sampled sum by 64 as an exact
total, divide it by all successes as a mean, or use sampled max as p99. A sampled
mean is sum/samples (nanoseconds; divide by 1e9 for seconds), with zero samples
undefined. Global samples/sum add across disks; max takes their maximum.
Separate scrapes are not a cross-disk transaction.

Store and Stats each gain 24 fixed bytes; sampled calls add two monotonic clock
reads plus bookkeeping under D, and unsampled calls still have a branch/local
timer. Its static size is not a complete stack-frame or RSS budget. This is
not zero overhead or completed lock/latency acceptance: D wait and full
completed-to-applied instrumentation remain separate work. No new alert/query
migration is needed; existing metric names and heat rules are unchanged.

### Successful donor score distributions

Score observations use the same final pre-wipe time and successful extent
invalidation boundary as donor age/protection accounting. Score is calculated
once and reused for the unchanged strict `score > KEEP` comparison. Sign,
bucket and protection decisions remain integer arithmetic. These are **score
seconds**, not guaranteed remaining wall-clock lifetime or time until eviction.
Recovered residents with valid virtual-read state are included, even when their
history is unknown and their ages are excluded from known-history age bins.
Missing state or zero generation uses the existing unscored counter; in one
allocator snapshot, nonnegative count + negative-magnitude count + unscored
victims equals released donor residents. Unknown-age count is not an additional
term in this score partition.

Both score `_sum` values deliberately use plain floating-point accumulation.
A valid huge-BASE configuration can produce INT64_MAX scores, so only three
victims would overflow a uint64 sum. Floating sums remain nonnegative and
approximate (integer unit precision is not promised above 2^53); Prometheus also
represents samples as floating point. Group sums merge each sign independently,
while bucket counts stay uint64. Do not subtract these sums to infer a signed
policy benefit or average age, or combine per-disk quantiles. Existing age sums
keep their integer representation and are not converted to floating point by
the shared renderer.

Global/per-disk labels and runtime-reset semantics match donor age. Failed
invalidation, skipped pins/guards, empty donors, ordinary Remove, class reclaim,
watermark and legacy eviction add no score observations; file omits the
families. No new key/request labels, clocks, locks, atomics, resident fields,
maps or hooks. There is new fixed accounting state and bounded release-loop
work, not zero cost or full RSS/performance-budget acceptance. Other eviction
paths' score distributions and the rest of O5 remain pending.

### Watermark protected-victim timestamp correction

Watermark selection already refreshes the bounded clock immediately before
invalidating the selected slot. Protection accounting uses that saved final
pre-wipe time, not the earlier candidate-comparison score or a new post-I/O
sample. A winner that crosses KEEP between selection and this recheck is not
reported protected; a winner protected at recheck remains counted if persistence
subsequently crosses KEEP. Publication still requires successful invalidation
and physical release. Partial success retains only completed victims.

This corrects observation only: victim choice, selection scores, scan limits,
cursors, pin handling, failure/health propagation and legacy behavior are
unchanged. No reranking or added clock/lock/atomic/state/configuration/hook.
When statistics are requested, there is one extra pure score evaluation for
the selected victim; a null statistics pointer skips that work. This is not
zero CPU cost or a performance acceptance claim. The existing global counter name/type/labels and
runtime reset behavior are unchanged. The previously missing per-disk family
`dfkv_slab_disk_lifetime_watermark_protected_victims_total` now exports the
existing per-store counter with the standard disk/disk_index labels. Other O5 observation and acceptance work
remains pending; no thresholds or default-policy changes follow from this fix.

### Class eviction age sampling and final clock guard

Lifetime foreground and background class selection now perform one additional
`TryDrainNow` after the existing final health/reclaim guard, immediately before
slot invalidation. This is one extra time-source read and existing atomic-clock
floor access per selected, guard-permitted attempt (including subsequent I/O
failure), not per visited resident or all-pin/empty scan. Advancing time may use
CAS. It is not free instrumentation: if that sample crosses `UINT32_MAX`, the
not-yet-started invalidation is refused through the existing clock-exhausted
`kCacheFull` path, or the background remainder stops. Earlier completed work
stays counted. No selection-time fallback, zero-age substitute or post-I/O
reclassification is used. Valid-time victim ranking, pin/scan budgets and legacy
selection remain unchanged. No extra lock or atomic variable is introduced.

The class/background score observations reuse that final sample with one extra
pure score calculation per selected, guard-permitted attempt (also when the
subsequent invalidation fails); only successful wipes enter the histogram.
They do not add another time-source read, lock, atomic or per-resident field.
The per-call score block is cleared once at each allocator call entry, then
merged under Store D before return/yield. Age remains cumulative and uses integer
call deltas; score uses a fresh call block rather than subtracting cumulative
floating totals. This avoids losing an entire small later piece after a large
prior score, but Store/Group double sums still have ordinary rounding limits.

The two persistent score blocks add 288 fixed bytes each to Store and Stats;
Scan gains 144 bytes. Actual GCC sizes in this iteration: Store 2512 to 2800,
Stats 1792 to 2080, Scan 744 to 888; Allocator remains 1032 bytes. Legacy layouts
also pay fixed object/possible frame costs even though they record no scores.
Reset, score arithmetic, bin updates, Store/Group merges and text rendering have
nonzero cost; these sizes are not full stack, RSS, recovery-peak or performance
budget acceptance evidence.

### Logical read-class observations

The internal `ObserveLifetimeReadClasses` Store/allocator API observes retained,
initialized lifetime states, including pinned and hidden deferred-Remove data.
It is not a visible-payload or readability gauge. State-less hard-drain commits
are excluded. The five estimates follow the existing business classes W/R/O1/
O2/O3 (last means three or more actual reads), independently of scoring MAX_READS.
Recovery's virtual reads remain logical W until a real accepted read, then O1;
recovered residents never enter the NEW write-window class R.

W is exact. R is an upper-bound estimate, and each outside-window O class can be
underestimated until pending expiry settles. All five sum exactly to the retained
initialized population in the same allocator snapshot. Exact R ends strictly
after original `put_at + P`, not after the refreshed anchor. A separate fixed B
ring rounds this deadline to quantum `Q = ceil(P / (B - 1))`, so extra expiry lag
is at most Q-1 seconds even without another GET. Default P=180/B=24 gives Q=8 and
maximum lag 7 seconds. This quantum is NOT protection bucket G (default 12 hours), a new
user setting, or an acceptance threshold. The pure exact classifier is unchanged.

The snapshot returns availability, sampled shared-clock offset, Q, lag bound and
actual R-ring vector backing bytes. It takes one allocator lock, samples the
shared clock once (advancing its monotonic floor), and visits at most B <= 64 pages;
there is no resident enumeration or snapshot heap allocation. Hard exhaustion
returns unavailable while retaining backing/resolution facts; numeric zero then
is not an empty-cache observation. Disabled legacy/RAM/shadow paths allocate no
R ring and read no clock through this observation API.

Bookkeeping reuses accepted commit/read/final-release lifecycle points, adding
plain count updates and bounded ring arithmetic but no per-resident fields,
clocks, locks or atomics. Failed reads/invalidation and deferred visibility loss
do not debit a still-retained resident. Rebuild reconstructs counts rather than
persisting or replaying historical events. Fixed allocator layout and ring costs
are nonzero, including fixed layout in disabled modes. The API's `ring_bytes`
accounts for the new backing; older memory gauges remain partial and exclude
this ring. The independent ring gauge below supplies that component without
changing older gauge definitions. No existing dashboard/alert is switched to
these estimates and no complete memory/performance budget is established.

Measured GCC layout for this foundation: Allocator 1032 to 1104 bytes (+72,
including Options 200 to 208); Store 2800, Stats 2080, state 12 and Scan 888 bytes
remain unchanged. The observation value is 80 bytes. R-ring backing is 32 bytes
per retained bucket (default 24 means 768 bytes, maximum 64 means 2048 bytes), once
per enabled allocator, not per resident or extent. Disabled allocators still pay
the fixed 72-byte layout increase. These are component sizes, not heap overhead,
full stack usage, incremental RSS or recovery-peak acceptance measurements.

The Group delegates `DiskLifetimeReadClassObservation(disk_index)` to the same
Store API. Prometheus exports these **per-disk gauges** for the slab engine:

| Name (prefix `dfkv_slab_disk_lifetime_read_class_`) | Meaning |
| --- | --- |
| `resident_estimate` | Five estimates from one allocator snapshot, additionally labeled `read_class`. |
| `available` | 1 for a usable snapshot, 0 for disabled tracking or hard clock exhaustion. |
| `sampled_at_sec` | Snapshot's shared lifetime-clock offset in seconds, not Unix time. |
| `expiry_quantum_sec` | R expiry rounding quantum Q (not protection bucket G). |
| `expiry_lag_bound_sec` | Maximum extra R expiry lag Q-1, not measured observed lag. |
| `ring_bytes` | Actual retained R-ring vector backing bytes, excluding heap metadata. |

All six carry existing `disk` and `disk_index` labels. `resident_estimate` has
exactly five fixed label values: `unread`, `read_within_new_write_window`,
`read_once_outside_new_write_window`, `read_twice_outside_new_write_window`, and
`read_three_or_more_outside_new_write_window`. These mean W/R/O1/O2/O3+, not
MAX_READS bands. No key/request labels or dynamic read-count series are added.

An enabled empty disk emits five population zeros with `available=1`. When
unavailable, population **and sampled time samples are omitted**, not exported
as zeros; `available=0` and ring/Q/lag facts remain. Disabled legacy slab emits
availability and those three facts as zero; the file engine omits all six
families. A hard-exhausted clock retains ring/Q/lag facts and cannot be reopened
by a source-clock rollback. Failed metadata persistence does not make retained
population disappear or claim readability. Restart rebuilds the ledger rather
than restoring historical metric samples.

No global population/time family is emitted: each disk has its own allocator
lock and sampling point. For a deliberately non-atomic operational aggregate,
`sum by (read_class) (dfkv_slab_disk_lifetime_read_class_resident_estimate)`
sums only available disks; check availability for **all intended disks** before
treating it as complete. Summed R is still an upper bound and O classes lower
bounds only for the contributing observations, not an exact common-time
classification. Never sum `sampled_at_sec` into a timestamp. Do not add population
estimates to queue/visible/recovered counts: these are overlapping views.

Each metrics scrape adds one enabled observation per slab disk: one allocator
lock, one shared-clock read/floor update and O(B) bounded work, plus six-family
text rendering. It reuses one observation for all five populations and facts;
there is no extra resident scan, permanent field, hot-path clock or per-event
allocation in this export step. The observation value is 80 bytes; output string
growth has ordinary allocation costs. This is not a lock-contention, RSS, latency
or performance-budget acceptance claim. Existing alerts and dashboards do not
depend on the new families.

### Recovered eviction boot-age histograms

For each lifetime eviction path (`class`, `background`, `watermark`, `donor`),
`dfkv_slab_lifetime_<path>_recovered_boot_age_seconds` and its
`dfkv_slab_disk_lifetime_<path>_recovered_boot_age_seconds` counterpart record
successful recovery-origin victims' **current shared-clock boot offset at final
pre-wipe time**. Per-disk labels are the existing `disk`/`disk_index`; optional
node/group labels follow the existing renderer. There are no key/request labels.
The fixed histogram bounds are 0, 1, 60, 3600, 86400, 604800, 2592000 and +Inf
seconds, not new lifetime configuration or acceptance thresholds.

These observations use initialized `history_unknown` states with valid resident
identity. Recovery's virtual reads are not business reads; a later real read
refreshes `anchor` but does not reset this boot-age measure. For example, a
recovered resident read at 59 and successfully evicted at 61 contributes 61,
not the refreshed-anchor age 2. The existing ordinary age histogram still omits
it, and its existing `age_history_unknown_total` counter still increments.
For each path, the new histogram count equals that same unknown-history count;
known-history NEW residents never enter the recovered histogram.

Only successful invalidation contributes. Failed persistence, skipped pins,
refused hard-clock selections, uninitialized/unscored states and work not
performed do not add a sample; completed victims remain recorded if a later
piece or payload write fails. Selection-time and post-I/O times are not used.
Donor victims share the already established final whole-extent pre-wipe time.
Explicit Remove, rewrite and shutdown are **not** these eviction events and do
not contribute. Watermark calls without a stats output do not collect samples.

This is an **eviction-selected cohort exit distribution**, not true age before
restart, time since the last read, duration since each object's individual
reconstruction, or a full survival curve. The origin is the shared bounded
clock's current-run origin used by recovered state initialization. Still-retained
recovered objects are censored observations, separately exposed by the existing
retained recovery gauges; evicted-only samples must not predict their lifetime.
A clean restart rebuilds a fresh cohort and resets runtime counters. Reusing a
clock in a test is not proof of a fresh process origin. Never add boot offsets
across restarts or infer per-key cross-restart survival from these aggregates.
Continuous-restart retention/replay acceptance remains unfinished.

All eight families retain existing slab conventions: legacy emits zero
histograms, while file omits them. Integer counts/sums merge across disks and
owner slices using the existing snapshot protocols, without floating cumulative
subtraction. They have finite uint64 counter range: exact integer accounting is
conditional on no counter/sum overflow, including Group aggregation; a bounded
32-bit timestamp alone is not a proof that arbitrary multi-disk sums cannot
overflow. Existing cumulative-counter reset/wrap caveats apply. Global and
per-disk snapshots are taken separately; cross-view equality is a quiescent
invariant, not one atomic multi-disk instant. Each path's histogram count and
unknown-history count are copied from the same owning snapshot.

No new hot-path clock, lock, atomic, resident field, expiry ring or persistent
record is introduced. Successful recovered victims add bounded bin arithmetic;
existing age snapshots, per-call copies/deltas, Store/Group merges and metric
rendering now carry larger fixed blocks. Disabled RAM/legacy layouts also pay
shared fixed-type growth. This is nonzero instrumentation cost, not a completed
heap/RSS/recovery-peak, contention, throughput or p99 budget assessment. Existing
alerts/dashboards are not switched to the new families.

Measured GCC component sizes for this addition: age statistics 80 to 152 bytes
(+72), DonorStats 336 to 408, Scan 888 to 960, and watermark output 256 to 328.
Allocator grows 1104 to 1176 (+72); Store 2800 to 3016 (+216); Stats snapshot
2080 to 2368 (+288). Existing per-call age-before snapshots also copy 72 more
bytes, including calls that evict only NEW residents. Options 208, resident
state 12 and read-class observation 80 bytes are unchanged. These are component
layouts, not measurements of complete stack frames, heap/RSS or recovery peak.

### Logical completion to lifetime application

The `dfkv_slab_lifetime_read_completion_to_apply_*` and corresponding
`dfkv_slab_disk_lifetime_read_completion_to_apply_*` families observe a sampled
**Node synchronous completion or PULL grant publication → successful owner update**
interval. Suffixes `samples_total` and `ns_total` are counters; `max_ns` is a
gauge. The per-disk families use the existing disk/disk_index labels; global
sums/counts add and maxima take max. No key/request labels are introduced.

The synchronous request RANGE boundary is its common success/ownership-ended
point immediately before notification, after the existing branch hit/byte
bookkeeping. RangeInto captures after RAM Hit release or final disk/coalescer
result/output completion, before success counters where practical. This is
neither physical storage completion nor client receipt. Every copy follower
has its own logical completion after its output is ready. Read failure, empty
success and preparation alone do not create an eligible sampled notification.
In particular, a disk read later changed to I/O error by final deferred-Remove
invalidation is not a successful producer event.

Capture selects the first and every 64th eligible nonempty logical success
**per thread across Nodes/stores**. Disabled or empty calls do not touch the
phase or a clock. Synchronous and completion-aware PULL producers share this phase on a thread.
The producer carries one transient completion token through
Group/Store; downstream code does not resample. A selected successful update
captures its endpoint only after lifetime state, both indexes and bucket
bookkeeping are complete, while allocator A is still held. The interval includes
producer-to-Store dispatch, disk routing, D/A contention and accepted update
work, not time before the declared producer boundary. It cannot be reconstructed
from the existing independently sampled wait/body/held timers.

The bounded lifetime `applied_at` used for heat remains sampled under the owner
lock as before. Completion timestamps never backdate heat or revoke logical
success. Rejected heat does not produce an applied-delay sample. Tokens contain
actual same-process steady-clock readings, not Unix time or externally supplied
wire values. A selected timestamp later than the application endpoint is counted
invalid rather than wrapping into a huge unsigned latency or reporting a false
zero; the already applied heat is preserved. Arbitrary forged clock epochs or
unrepresentable-duration values are not supported timestamp inputs.

Coverage is exported by four counter families, globally and per disk:
`dfkv_slab[_disk]_lifetime_read_completion_missing_total`,
`..._not_sampled_total`, `..._not_applied_total` and `..._invalid_total`.
For each eligible Store notification snapshot under D:

```
successful = missing + not_sampled + not_applied + invalid + delay.samples
```

Missing means the caller did not supply completion provenance (unrecognized
internal sampling metadata is also treated as missing); not_sampled means the
producer participated but deliberately skipped this sample. not_applied means
selected but rejected by the existing heat-result checks; invalid means applied
with unusable timestamp ordering. These are an independent partition, not more
logical successes, sources or heat outcomes. The production PULL observer now
forwards completion metadata unchanged. Old three/four-argument transport
observers and internal direct-notification callers still default to **Missing**.

PULL captures at the once-eligible grant-recording boundary, after final
preparation success, valid MR registration and installed grant ownership, but
before the notified counter and observer invocation. Thus it includes observer
dispatch/delay, routing and accepted update work, not preceding MR registration
or ownership bookkeeping. It is not preparation, CQE completion, READY send,
client receipt or RELEASE. Duplicate, empty and no-observer grants do not advance
the sampler or read its clock. Once consumed, a grant is not replayed on observer
replacement, READY failure, release or teardown; rejected heat remains a valid
grant with no applied-delay sample. The bounded heat timestamp remains the
owner's current time, not the earlier grant timestamp.

The completion-aware handler and its sampling flag are configured before Start;
handler/owner must outlive Stop and callbacks remain synchronous, thread-safe
and nonthrowing. Production enables sampling from immutable lifetime tracking,
not merely clock presence or dynamic health. Old handler setters always disable
sampling, including replacement/clear; no observer also disables capture. This
preserves ordinary legacy/file behavior. Internal metadata-only opt-in follows
the same tracking rule as synchronous reads. CPU tests exercise this post-MR
helper boundary and real Node/Store application, not actual registration or
READY delivery; real RDMA/RXE plus io_uring hardware acceptance remains deferred.

The elapsed values are sampled sum/count/max, not a histogram, percentile, p99,
all-request elapsed total or a latency acceptance result. Short-lived threads
can oversample; TLS phase can correlate with routing and survives Store
reconstruction on a surviving thread. Zero observed samples means unobserved,
not zero latency. Restart resets Store counters but not another live thread's
phase. Do not add means or maxima from the independent processing/wait/held
cohorts. Counter ranges are finite uint64, including Group aggregation;
cross-disk/global snapshots are not one atomic instant. The same-Store coverage
partition is coherent under D. File omits the slab families; normal legacy slab
emits zeros without completion sampling or lifetime D/A activity.

Costs are nonzero: transient token construction/forwarding, per-thread phase and
branches on eligible unsampled successes, one additional steady-clock read for
selected rejected events, and two for selected applied events. Existing wait,
processing and held timers retain their own reads; two is not a total-request
clock-cost claim. Plain counters and larger fixed Store/Stats snapshots and text
rendering have costs, including shared disabled layouts. No resident field, new
lock, shared sampling atomic, persistent record or user parameter is added.
The PULL adapter adds a setup-only bool, unified callback forwarding and a
transient token; legacy adapters and disabled token construction are not free.
Existing per-request grant notification atomics are unchanged.
Complete heap/RSS, lock contention, p99 and strategy performance acceptance remain
unfinished; no existing alert/dashboard is switched to these new samples.

Measured GCC sizes: completion token 16 bytes; Store 3016 to 3072 (+56),
Stats 2368 to 2424 (+56). Allocator 1176, Options 208, resident state 12,
Scan 960 and read-class observation 80 bytes are unchanged. These are component
layouts, not total stack-frame, heap/RSS, recovery-peak or performance-budget
measurements. Passing a default token is source-compatible for existing internal
C++ callers; it does not add a C/transport wire field.

Measured GCC RDMA+io_uring layouts for PULL forwarding: RdmaServer 1432 to
1440 bytes (+8 for the flag/alignment); Node 102232, Store 3072, Stats 2424 and
completion token 16 bytes are unchanged. This is fixed object layout, not a
measurement of callback heap allocation, complete stack/RSS or latency budget.
