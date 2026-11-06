# Q3c：内部写回背压与启动ACK门禁

## 范围

`DiskCacheGroup::Options::lifetime_class_eviction` 默认false，仅用于内部集成。
显式开启才将同名选项传给真实slab；要求有效scoring config，不能用于file
或自定义engine factory。shadow config/clock自身不改变淘汰算法。
CLI/环境策略解析不设置此开关，公开lifetime仍在R45之前拒启；本轮没有
实施D4保护桶/donor，也不将内部测试称为公开策略放行。
配置日志以effective=internal_test_only、disk_eviction_internal_test=lifetime
标识内部执行，不能将其当成legacy对照组；原legacy/file标识保持不变。
寿命环境参数仍标inactive，因为此路径使用显式注入的config快照、不解析它们。

## 开盘前安全检查

Node先构造Group、再初始化RAM。因此新策略的ACK组合必须由Group在任何
磁盘构造/恢复/epoch改写前检查，不能等InitRamTier才拒绝。

- 按Node既有语义，RAM_TIER的1/on/true/yes表示开启；WRITE_MODE仅
  writearound免于写回限制，未设置默认writeback；新内部路径拒绝适用的
  非空未知WRITE_MODE或ACK值，不沿用legacy的告警后归一化。
- 内部实际新策略+RAM write-back必须显式设置 `DFKV_PUT_ACK_MODE=disk`。
  未设置、空、ram或其他值均ERROR拒启，错误包含输入和配置指引。
- 不静默改ACK、关闭RAM或降级算法；RAM关闭、writearound、legacy及
  仅shadow注入不受此新增组合限制。旧模式原有默认ACK行为不改。
- 公共显式lifetime仍走既有未就绪拒启；此开关不是配置解析的旁路。

## 数据与失败语义

复用现有typed single/batch flusher及RAM有限重试，不再建立一套状态机：

`Node → RamTier → Group typed write → Store → per-item completion → Node status`。

只有精确的 `CacheFull + kScanBudgetExhausted` 才按预算背压分类。一次Store
owner保持自身固定扫描预算；RAM整项重试另有固定上限，终局返回CacheFull，
完成owner/follower并清在途状态，不误ACK、不单独置RAM unhealthy或MarkBad。
真实I/O仍逐项返回IOError并按既有健康契约隔离，混批不能被其他项预算原因
掩盖。已RAM-ACK的防御性驻留规则保留，但新路径的启动门禁禁止制造此组合。

前台Node批写保持原有disk-complete路径，不人为绕入RAM；flusher本身不取
等待其完成的前台mutation锁。Remove/Stop仍遵守原有完成和析构顺序。

本轮测试/验证证据及阶段记录位于 `.omx/plans/implementation-q3c-writeback.md`
和 `.omx/evidence/q3c-writeback/`；真实硬件及性能验收继续后置。
