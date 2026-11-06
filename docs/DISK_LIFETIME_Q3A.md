# Q3a 第一轮：两套寿命评分索引

本轮只连接索引生命周期，**尚未替换淘汰选择器或开放 lifetime**。
现有 legacy victim/RAM CLOCK 继续生效；类内有界扫描、请求预算及
owner/follower 终结留在 Q3a 下一轮。真实 RDMA/RXE/io_uring 验收仍后置。

## 结构与维护

- 每个 class 和全盘分别拥有 MAX_READS+1 档，头数为
  `(class_count+1)*(MAX_READS+1)`，不是固定五链。
- 有效档为 `min(n+fw, MAX_READS)`；fw 的首次早读奖励也影响档位。
- 仅已提交且评分状态存在的住户入两套索引。未提交及硬时钟后
  无可用评分状态的住户不伪造默认热度。
- generation/state 发布、两套挂链及成功读后的双链迁移都在同一
  allocator 锁内完成；同档封顶成功读也摘下重插。
- 按 anchor 非递减排列；相同 anchor 保持插入顺序，不冒称身份有序。
  正常时间顺序尾插 O(1)，旧 anchor 的交错恢复/提交从尾向前定位，
  最坏 O(N)，不能把它称为有界扫描或性能验收。
- 物理释放同时摘两套索引，并让 cursor 前进到存活后继（尾部回绕）。
  延期 Remove 与持久失效失败不提前释放物理成员；后续选择器需核验
  可见性、代际与 pin，而不是把挂链等同于可淘汰。
- 不因 R 逻辑到期搬链；不在低层物理 Get/Touch 中更新新索引。

## 内存与范围

采用 enabled-only 并行链接表，SlotMeta 不增加字段。新表按 metadata
高水位保留容量；每项20字节（四个32位链接及档位），每队列12字节
（head/tail/cursor）。两 vector 本身也有固定成本，包括 RAM allocator；
禁用时没有新表的堆分配。GCC13/x86_64 实测 sizeof(allocator) 从496
变为544字节（固定+48），Options仍168、SlotMeta仍104字节。
此处是当前 ABI 的结构成本，不是 RSS 实测。

## 回归入口

- `test/cache/slab_lifetime_storage_test.cc`：两套成员/同档、链接与游标、
  anchor 顺序、MAX_READS=1/2/4/63、迁档/封顶、恢复、失败失效和并发。
- `test/cache/disk_lifetime_resident_test.cc`：实际 Store 配置传递、真实
  提交和读通知、封顶及 Remove；不只验证独立容器。

本轮通过不代表 U5/U6 或整个 Q3a 已通过：尚无新受害者顺序、
1024 访问片预算、跨锁续试、单 flight 总预算及 follower 终结验收。
