# D4a：保护到期桶与代际账务

本轮只为后续extent donor粗筛准备保护数量估计，不改变任何donor、水位或
类内受害者排序。公开lifetime仍未放行；恢复仍使用临时虚拟n=1，不提前切n=2。

## 开启与数据来源

- 仅Store内部实际执行开关启用桶。正常legacy/RAM及shadow配置不分配
  桶页或逐住户token，也不计算保护到期；没有新增用户环境参数。
- 复用已校验的config：ProtectEnd由Score>KEEP推导，G/B覆盖H，包含
  MAX_READS及KEEP变化后的保护期限；不按最后访问时刻直接分桶。
- NEW和恢复提交都传入已采集的实际盘时钟offset。恢复anchor=0不是登记
  时刻；恢复时已经过期者不登记，时钟硬界后未知状态也不制造保护账。

## 页、凭据与查询

- 每extent有B个页，每页{uint64绝对generation, uint64 count}。
  每个实际slot-meta位置另有8B token，0代表从未登记；不挤入SlotMeta。
- deadline>now才登记。页号=(deadline-1)/G，token=页号+1，即向上取整
  deadline/G，不用deadline+G的潜在溢出写法。第0页可用，0仍能表示未登记。
- 查询仅累计generation>now/G的页，最多查B<=64页，且查询不修改账。
  精确桶边界立即过期，非边界可保守多算0..G-1秒；只用于粗筛，不能冒充
  精确Score>KEEP损失或证明没有冷donor。未来页不误作过期，旧页不因取模复活。
- 登记允许惰性重置同环位置的过期页。有效配置保证同一当前时刻的活动
  截止页最多占B个连续页，不会互相覆盖；配置不可变、登记时间不回退。

## 物理生命周期

- 提交在同一A锁内安装generation、state、双链和token。
- 成功读先核对原住户generation，再撤旧token、更新state/双链并登记；
  同秒或读数封顶也撤旧入新。拒绝/错代读不改变账。
- 撤账只认真实token及对应页的绝对generation，不根据旧state反推：
  未登记的过期恢复A即使与已登记B位于同页，删除A也不会扣B；过期旧token
  也不能扣同一环位置的新代页。
- token隐式绑定当前dense resident；物理Free/reuse清零，并与已有住户
  generation校验共用A锁，因此不重复存另一份住户generation。
- pending Remove/隐藏payload、失败持久失效不撤物理账。成功失效后的
  Free才撤账；成功绑定/重绑/整条归还才重置页。盘格式和磁盘保护字段不变。

## 成本及验证边界

有效元素为每个slot-meta位置8B、每个extent的B页各16B；实际辅助堆容量
按 `8*token.capacity() + 16*pages.capacity()` 计，另有分配器开销。RestoreBulk同步reserve token，避免恢复时
反复扩容。disabled的两辅助vector size/capacity均为0。
GCC13/x86_64实测：allocator576→640B（RAM实例也承担固定64B），Options
168→184B；SlotMeta104B、ExtentMeta56B及Store1760B不变。默认B24、单extent、
65536个slot-meta时，两辅助vector容量合计524672B，shadow为0。
单extent256MiB、4KiB粒度、65536条有效记录的一次独立恢复测量：shadow峰值
RSS38540KiB，启用桶为39224KiB（/usr/bin/time -v）。这不是差值归因、全几何
内存放行或吞吐/p99基准；其他几何与多盘恢复仍须后续预算验收。

测试：`test/cache/slab_lifetime_buckets_test.cc`，含65536同桶住户、跨环旧token、
KEEP0/M1/2/4/63、宽代际、并发、真实元数据失败和恢复时钟。
测量与日志：`.omx/evidence/d4a-buckets/`；阶段报告：
`.omx/plans/implementation-d4a-buckets.md`。本轮不宣称吞吐、p99或真实硬件验收。

D4b三处donor统一选择与失败分流见 `docs/DISK_LIFETIME_D4B.md`；
正式恢复初值和公开显式lifetime仍待R45。
