# D4b：统一有界 donor 选择与失败分流

仅内部实际执行路径使用新选择器。公开lifetime仍在R45前拒启，恢复仍是临时
n=1。legacy/RAM/shadow的选择分支、ACK与正常候选顺序保留。

## 三个入口和资格

StealExtentFor、StealColdExtentFor、StealFromResult共用同一个A锁内选择器。
原bool StealFrom保持兼容；Store后台使用typed结果，不再把元数据故障当无候选。
Store拥有不可变config/clock，其生命周期覆盖allocator，Options仅借用两指针。

- donor必须已绑定、不是target、无pin，并满足入口的指定class/stripe下限。
- Cold保留启用状态、原动态/显式window和youngest_seq截止条件；新路径允许
  满足窗口的空bound donor零slot完成精查，legacy原非空限制不改。
- 后台保留冷class需求限制、donor实际bound>8、每热class每tick最多2条。
  新Grow只读取上轮puts快照，随后class pass仍能消费同一批需求；新模式对
  2×delta和冷热比较使用宽/安全计算，不借机重构旧分支。

## 完整粗查与完整精查

1. 每次选择检查完整配置池E条extent一次，先应用硬资格，再查D4a桶估计。
   粗序为：估计保护数、总payload字节、空槽数降序、extent编号。
2. 保留全池粗排fallback，以及单条resident<=8192者中粗排最前8个候选。
   固定数组插入维护，O(8E)，不分配随E增长的候选数组或做全池ElogE排序。
3. 按该顺序完整检查候选，累计最多8192 resident、最多8完整extent。
   若下一条放不进剩余预算即可停；不把半条扫描当exact。不能精确评分的
   未知住户也不进入exact（正常Store未知状态伴随hardclock拒绝新reclaim）。
4. exact按五级字典序择最小：保护数、保护payload、总payload、空槽降序、
   extent编号。保护由同一now快照下的Score>KEEP逐件计算，不用旧read_heat。
5. 只要有一个exact，就在已完整检查集合内选择；后续预算到界不能改走
   fallback。只有没有exact时才选全池粗排fallback，单独计数，绝不声称
   全盘精确最小。单条>8192的合法几何仍可前进。

## 持久化、锁与失败

所有资格、扫描、选择和动作在同一个A锁内，候选不跨锁。启动选定动作前再
复核health/clock。只让选中donor经过原on_extent_bind清零该extent对应表记录及fdatasync；
成功后才Free全部住户、撤双链/桶账并重绑。失败立即返回PersistenceError，
不撤原物理账、不换另一donor；已写出的部分记录不能声称未改变。

Store水位后运行新Grow再运行新class回收，仍不进入旧GROW/CLOCK分支。
Unavailable可尝试其他合格class，PersistenceError/ReclaimDenied立即结束
该次后台过程。清成功evicted的payload/tenant账仍在D锁事务中完成。

## 统计与成本边界

九个固定donor计数见 `docs/METRICS.md`，分开统计全E粗查、完整精查、resident
检查、无exact兜底、无候选、持久化错误、selection_ns、persistence_ns和实际
released_residents。legacy计数为0。

8192是**每次donor选择**的精查预算，不是整个PUT或tick的累计工作上限。
selection_ns在已取得A后计选择工作；persistence_ns另计选中者的持久化、
释放和重绑，不包含随后Store payload/tenant清账，也不等于锁等待/完整PUT。
完整donor的清表、Free、evicted输出和绑定新格可超过8192，不能推导p99硬界。

GCC13/x86_64：allocator640→728B（共享RAM实例也承担固定88B），Options
184→200B；SlotMeta104、ExtentMeta56、Store1760B不变。统计对象72B，Store
Stats560→632B；没有新逐住户字段/辅助堆，选择候选数组192B。Store Grow另有
按class数计的临时快照/需求数组；完整evicted输出仍按实际donor住户数增长。

一次并行验证期间的成本记录（不是吞吐/p99基准）：
- 4096 extent、唯一末尾空donor：粗查4096、exact1/0resident，选择30754ns。
- 真实2×64MiB、4KiB几何：一次PUT两轮各E2粗查，fallback1，实际清表同步1次、
  释放16384住户；选择2212ns，选中动作5847154ns，完整PUT25960148ns。
  最终Count8193、健康正常，幸存样本及新4097B字节一致。生成数据已清理。

测试：`test/cache/slab_lifetime_donor_test.cc`；日志/测量生成器：
`.omx/evidence/d4b-donor/`。下一步R45正式恢复初值与开放显式lifetime，尚未开始；
O5/AB性能标定、默认切换和真实RDMA/RXE/kernel io_uring验收继续后置。
