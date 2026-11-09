# R45：显式启用磁盘 lifetime 淘汰

本文说明开发分支当前已实现的启动与恢复行为，不替代已批准的算法规格，也不宣称
性能、默认策略翻转或 RDMA/RXE＋io_uring 验收已经完成。

## 选择策略

```bash
# TCP、RAM 关闭；容量保持为 slab extent 大小的整数倍。
DFKV_RAM_TIER=0 ./build/dfkv_server \
  --dir /path/to/cache --cap 1073741824 --store-engine slab \
  --slab-write buffered --slab-eviction-policy lifetime
```

也可设置 `DFKV_SLAB_EVICTION_POLICY=lifetime`。命令行优先于环境变量，策略值
忽略 ASCII 大小写；显式空值及未知值报错，不视为缺省。显式 `legacy` 保留旧策略。

| 输入 | requested | effective | staged |
|---|---|---|---|
| slab，未指定 | lifetime | legacy | true |
| slab，显式 lifetime | lifetime | lifetime | false |
| slab，显式 legacy | legacy | legacy | false |
| file，未指定或显式 legacy | lifetime 或 legacy | not_applicable | false |
| file，显式 lifetime | 开盘前拒绝 | — | — |

以启动日志中的 `disk_eviction_requested`、`disk_eviction_effective`、
`disk_eviction_staged` 为准；请求 lifetime 不等于已经生效。**默认仍是 staged
legacy，本次没有默认翻转。**

## RAM 写回门禁

显式 lifetime 且 RAM write-back 时，必须显式设置 `DFKV_PUT_ACK_MODE=disk`。
ACK 缺省或设置为 `ram` 均在打开磁盘、分配 RAM tier 前拒绝启动；未指定 RAM
写模式时仍按既有默认 write-back 判断。RAM 关闭、write-around 不受此 disk-ACK
门禁限制。旧策略及 file 的既有 ACK 行为不变。

## 数值参数

显式 lifetime 会在开盘前严格解析下列十一项。仅接受无符号十进制数字，不接受
空值、符号、空白或非数字。范围错误、算术溢出及参数间关系不成立均打印含参数名、
用户原始值和约束的错误，以非零状态退出；不会降级为 legacy 或先创建缓存目录。

| 环境变量 | 默认值 | 含义 / 基本范围 |
|---|---|---|
| `DFKV_SLAB_LIFE_BASE_SEC` | 604800（7 天） | BASE，基础分秒数，1..INT64_MAX |
| `DFKV_SLAB_LIFE_READ_BONUS_SEC` | 302400（3 天 12 小时） | BONUS，每读档奖励秒数，0..INT64_MAX |
| `DFKV_SLAB_LIFE_MAX_READS` | 2 | M，评分读档上限，1..63 |
| `DFKV_SLAB_PROTECT_SEC` | 180（3 分钟） | P，新写保护及首次早读窗口，1..INT64_MAX |
| `DFKV_SLAB_PROTECT_SLOWDOWN` | 4 | SLOW，新写保护期内每 4 秒计 1 秒衰减，1..UINT64_MAX |
| `DFKV_SLAB_PROTECT_BOOST_SEC` | BASE + BONUS × M（默认 1209600，14 天） | BOOST，新写临时加分，另受下述关系约束 |
| `DFKV_SLAB_KEEP_SCORE_SEC` | 475200（5 天 12 小时） | KEEP，分数严格大于此值才受软保护，0..BASE−1 |
| `DFKV_SLAB_BUCKET_SEC` | 43200（12 小时） | G，保护到期桶宽，1..INT64_MAX，另受 H 约束 |
| `DFKV_SLAB_BUCKET_COUNT` | 24 | B，桶数，1..64 |
| `DFKV_SLAB_EVICT_LOW_PCT` | 88 | 回收到的低水位百分比，0..100 |
| `DFKV_SLAB_EVICT_HIGH_PCT` | 92 | 触发高水位百分比，0..100；0 仅关闭水位回收路径 |

参数不是独立旋钮。校验还要求：

- `Lmax = BASE + BONUS × M <= INT64_MAX`，`BASE + BOOST <= INT64_MAX`。
- `BASE - floor((P-1)/SLOW) + BOOST > Lmax`。
- `H = max(P, Lmax-KEEP+P-floor(P/SLOW)) + G <= INT32_MAX`，且 `B × G >= H`。
- HIGH 非零时，`LOW < HIGH`。

因此修改 M、BASE、BONUS 或 KEEP 时可能也需要调整 BOOST、G、B；不能仅凭各字段
各自落在基本范围内就认定整套参数可用。M 使用动态 `M+1` 个评分档，不需要修改
枚举或重新编译；M=1、2、4、63 使用同一套动态机制。

在未指定策略的 staged legacy、显式 legacy 或 file 下，前九项 lifetime-only
参数为 `inactive`，即使残留非法值也不解析、不阻止启动。LOW/HIGH **不是**
lifetime-only 参数，在旧模式下仍按原有规则适用，不能把它们也当作 inactive。

## 恢复边界

- 显式 lifetime 的 clean 恢复给现有住户虚拟评分读数 `n=2`，保留历史未知标识；
  这不代表发生过两次真实业务读，也不恢复旧的首次早读奖励。
- 恢复后的第一次实际成功读推进到 `n=3`，刷新读锚点，但不获得 `fw` 首次早读
  奖励；评分档仍按 M 封顶，例如 M=1 时仍落在档 1。真实读分类与评分档分开记录。
- dirty 恢复继续执行既有冷恢复重置合同，不因为启用 lifetime 而恢复原先会被
  清除的缓存住户。缓存不是持久数据库，不能将此策略切换视为数据持久化承诺。

本次不修改已批准规格，不增加性能试验或硬件验收结论。启动字段的完整上下文见
[部署配置](DEPLOY.md#当前磁盘淘汰策略请求值不等于生效值)。
