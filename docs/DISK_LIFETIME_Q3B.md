# Q3b：内部全盘水位公平调度

仅通过既有 `DiskSlabStore::Options::lifetime_class_eviction` 内部门控运行。
默认false，Q3c增加Group同名内部选项显式转发，CLI不设置；
公开lifetime仍未开放，内部write-back显式disk-ACK门禁见 `docs/DISK_LIFETIME_Q3C.md`。

## 算法与预算

- 复用两套索引的全盘row0，每档保留安全cursor，另存下一档号。
  每轮每个非空档至多检查一个resident；跳pin同样推进档内及档间位置，
  长pinned前缀不能独占一轮。位置跨tick保留，候选不跨tick保存。
- 一个调用持同一allocator锁；每轮统一now，核验已提交状态、generation、
  可见性、pin及有效评分档。对实际检查候选按score、anchor、generation
  比较；续扫不声称整盘最低。锁内没有其他线程可迁档、换代或重新pin。
- 每成功一件只补winner档，下轮重新付访问成本核验其他缓存候选并算分。
  例如A[1,100]、B[2,3]逐件归并为1、2、3，而非连清A。
- 单tick默认最多256次成功、总共1024次resident访问；访问预算为已配置
  成功上限的4倍，不因档/受害者重新领取。缓存复核与比较归入该次访问；
  到访问界后不再启动持久失效，因此成功数可能低于上限。
  有已检查winner且旧缓存已全部复核时，可提前结束本轮，保留一个未使用
  访问额度后启动失效，使小预算也能前进；不能为此忽略已知较冷的旧候选。
  未成功清除的候选在退出前交还本档cursor，最后一次访问发现的合法住户
  下一tick优先复查，不因wrap重复穿越同一pinned前缀而永久错过。
- 每次下一件前检查低水位、件数与预算；最后一个完整slot允许有限超出
  字节目标，不拆value。KEEP为软保护，全部已检查候选仍受保护时可清最低者。
- 同锁持久清除一个slot记录成功后才Free；绝不以整extent回调替代单件清表。
  失败立即停止，仅保留之前已成功失效的结果。硬时钟/健康闸门在开始扫描
  及启动失效前复核；不把时钟耗尽或真实I/O误归类为全pin/预算耗尽。

## Store集成与计数

- 沿用原有高低水位和NewPutDemandLatch（成功NEW需求、安静观察到期及
  active drain）的触发语义；内部运行从已验证config快照取高低百分比。
  legacy仍按原方式取环境参数，旧全局选择器和RAM CLOCK保持不变。
- 水位后运行Q3a类内回收，再返回；不混入尚未实施的新donor或旧GROW。
  因此Store整tick删除量包含两种不同用途，不能拿总Count差冒称水位件数。
- 复用watermark成功数、访问数、slot记录清理数、物理slot字节及清表字节。
  新增lifetime水位budget/no-candidate/pin-skip/protected-victim四个固定计数，
  指标口径见 `docs/METRICS.md`。失败不计成功，跳过不等于受害者。

## 验收边界

全盘暂存最多64个candidate索引和64个遍历起点，按调用栈持有，非逐resident
字段；仅新增固定next-band及四个Store计数，不增加队列或生产测试hook。
单个A锁内最多访问配置的4×成功上限（默认1024；已有参数最大1024件即4096访问）。
预算不抢占已开始的同步I/O，不能用此证明墙钟p99上界。真实硬件、内存/RSS、
吞吐和p99仍是后续验收，不在本轮宣称通过。

本轮只闭环Q3b；Q3c写回背压/ACK门禁及D4a保护桶账务见对应文档，
D4b donor见 `docs/DISK_LIFETIME_D4B.md`，R45仍后置。

GCC13/x86_64静态布局：allocator568→576B（含对齐，RAM实例也承担固定8B），
Store1728→1760B（四个计数32B），Stats528→560B；SlotMeta104B、links20B、
queue12B及StoreOptions200B保持不变。未新增逐resident字段或额外堆队列。
