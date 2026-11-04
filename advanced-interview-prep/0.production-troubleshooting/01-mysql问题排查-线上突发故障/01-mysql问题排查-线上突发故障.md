# 面经总结 01：MySQL 问题排查——线上突发故障，怎么办？

> 来源：极客时间《后端工程师的高阶面经》邓明
> 整理日期：2026-11-01

---

## 一、文章总结

**核心框架**：一切"XXX 问题怎么排查"都按三步曲回答——**发现问题 → 定位问题 → 修复问题**。

**排查工具箱**（按使用频率）：

| 工具 | 用途 | 关键命令/表 |
|---|---|---|
| SHOW 系列 | 看数据库实时状态 | `SHOW PROCESSLIST`（在跑什么）、`SHOW ENGINE INNODB STATUS`（死锁/事务/锁细节）、`SHOW STATUS`（整体指标） |
| PERFORMANCE_SCHEMA | 性能数据系统库 | `data_locks` / `data_lock_waits`（锁与锁等待）、`events_transactions_*`（长事务）、`events_errors_*`（错误上下文） |
| 日志 | 事后追溯 | 慢查询日志（配 `long_query_time`、`log_queries_not_using_indexes`）、错误日志（`innodb_print_all_deadlocks` 打印死锁） |
| mysql 前缀工具 | 管理与日志解析 | `mysqladmin`（与 SHOW 重叠）、`mysqlbinlog`（解析 binlog） |

**补充工具（文章未提，面试加分）**：
- `sys` schema（P_S 的人性化封装）：`sys.innodb_lock_waits`、`sys.statements_with_full_table_scans`
- `information_schema.innodb_trx`：定位长事务（存活时长、持锁行数）
- `EXPLAIN ANALYZE`（8.0）：真实执行并给出每步实际耗时
- `pt-query-digest`：慢日志聚合分析，抓偶发慢查询

**面试策略要点**：
1. 案例一类只准备一个，越高端越好（年限越长越要复杂案例）
2. 排查过程要讲成"A 命令→初步猜测→B 命令验证"的推理链，不能像无头苍蝇
3. 可信度来自数字：指标前后值、配置值都要记住
4. 可结合 AI（告警→AI 分析日志→定位代码段），刷技术敏感度印象分

---

## 二、Q&A（问题 + 标准答案）

### Q1：线上 MySQL 突发故障，你的排查思路是什么？

按三步曲：先**止血恢复业务**（保留现场），再定位根因，最后修复并复盘。
- 发现：监控告警（RT/QPS/慢查询数/连接数）+ 应用日志
- 定位：`SHOW PROCESSLIST` 看当前在跑什么 → 按症状分流（慢查询堆积 / 锁等待 / 连接打满 / CPU 飙高）→ 用对应工具验证猜测
- 修复：止血手段（kill 长事务、加索引、调参数）+ 根治（SQL 优化、容量模型、上线 Review 制度）

### Q2：死锁是怎么监控和发现的？

三代做法（按推荐程度从高到低）：

1. **日志推送（主流做法）**：开启 `innodb_print_all_deadlocks=ON`，死锁信息写入错误日志 → 日志采集（Filebeat/ELK/Loki）→ 日志平台配关键字告警。优点：不丢死锁、不占轮询连接、复用公司日志告警链路。云 RDS 控制台一般也自带死锁监控。
2. **工具轮询**：Percona Toolkit 的 `pt-deadlock-logger` 守护进程，定期轮询 InnoDB Status 并把死锁完整落库/落文件。
3. **cron 脚本轮询（最原始）**：定时 `mysql -e "SHOW ENGINE INNODB STATUS"` + grep `LATEST DETECTED DEADLOCK`。**硬伤：该命令只保留最近一次死锁**，两次轮询之间发生多个死锁会丢记录。

> 面试话术：先答日志方案，再补一句"早期用轮询 SHOW ENGINE INNODB STATUS，但只保留最近一次死锁容易丢，后来切换到了错误日志方案"——有演进史更可信。

另外应用侧也能感知：捕获 `ERROR 1213 (40001): Deadlock found`（InnoDB 死锁检测默认开启，发现 wait-for graph 成环后回滚代价小的事务）。

### Q3：SELECT ... FOR UPDATE 查不到记录时加什么锁？为什么强调 RR？

RR 隔离级别下加 **Next-Key Lock（临键锁）**，锁住 `(上一条, 下一条]` 区间。空表时锁 `(-∞, supremum]`，即整个区间。

强调 RR 的两个原因：

1. **RR 是 MySQL InnoDB 的默认隔离级别**（Oracle/PostgreSQL 默认是 RC，转过来的人容易忽略），没动过配置的系统都在 RR 上，这类死锁"开箱即得"。
2. **Gap Lock / Next-Key Lock 只存在于 RR**：RR 要防幻读，InnoDB 的方案就是锁"记录 + 间隙"挡住插入；RC 允许幻读所以 Gap Lock 基本禁用，该死锁案例在 RC 下不会发生。

因果链：默认 RR → RR 有 Next-Key Lock → 查不到记录也锁间隙 → 双方 INSERT 互相等待 → 死锁。被问"为什么会出这种死锁"时从默认隔离级别讲起，把事故讲成机制理解。

### Q4：为什么两个事务都持有 Gap Lock 却不互相阻塞，最后却死锁了？

**Gap Lock 之间互相兼容**（设计目的是防止插入，允许共存）。死锁发生在 INSERT 时：插入需要**插入意向锁（Insert Intention Lock）**，它与对方持有的 Gap Lock 冲突——A 等 B 释放，B 等 A 释放，环路形成。

### Q5：上述死锁怎么解决？

1. 首选 `INSERT ... ON DUPLICATE KEY UPDATE`，消除"先查后插"的窗口（原子单语句）
2. 或 `INSERT IGNORE` + 判断影响行数
3. 评估降级到 RC 隔离级别（Gap Lock 基本禁用，需确认可接受幻读）
4. 兜底：应用捕获死锁异常重试 2~3 次（死锁检测已牺牲一个事务，重试几乎必成功；Spring 侧捕获 `DeadlockLoserDataAccessException`）

### Q6：InnoDB 死锁检测机制了解吗？有什么坑？

默认 `innodb_deadlock_detect=ON`，后台线程构建 wait-for graph 找环，发现环回滚**修改行数少**的事务。坑：高并发锁竞争下检测本身是热点（等待图构建开销大），MySQL 8.0.18+ 有优化；极端场景可关闭检测，用 `innodb_lock_wait_timeout`（默认 50s）超时兜底。

### Q7：Too many connections 怎么排查和止血？

- 确认：`SHOW VARIABLES LIKE 'max_connections'` vs `SHOW STATUS LIKE 'Threads_connected'`
- 止血（文章精髓——渐进式）：先调小应用限流 → `SET GLOBAL max_connections` 分步上调（151→200→250→350）→ 每步观察 MySQL 负载 → 同步放开限流
- 不敢一把梭的原因：连接有 per-connection 内存开销（thread stack、sort_buffer、join_buffer），且线程过多 CPU 耗在上下文切换

### Q8：数据库连接数应该怎么规划？（容量公式）

```
DB max_connections ≥ 应用实例数 × 单实例连接池上限 + 管理余量
```
案例：11 实例 × 池 30 = 330 需求 > DB 151 → 必然爆。调整后：11 × 20 = 220 < DB 230，留 10 个冗余。

### Q9：max_connections 打满后，DBA 怎么连进 MySQL 处理？

- SUPER 权限用户有 **+1 保留连接**（max_connections + 1）
- MySQL 8.0.14+：专用 administrative 连接（默认端口 33062），不受 max_connections 限制

### Q10：监控连接数该看哪些指标？

- `Threads_connected`：当前连接总数（含空闲）
- `Threads_running`：真正在执行查询的连接数——connected 高 running 低 = 大量空闲连接，查 `wait_timeout` 和连接池空闲回收
- `Max_used_connections`：历史峰值，评估 max_connections 是否合理的直接依据

### Q11：慢查询怎么系统化排查？

1. 慢日志开启：`slow_query_log=ON`、`long_query_time`（生产常设 0.1~1s）、`log_queries_not_using_indexes=ON`
2. `pt-query-digest` 聚合分析：按 SQL 指纹归类，按总耗时排序（偶发慢查询尤其需要）
3. 嫌疑 SQL 用 `EXPLAIN ANALYZE`（8.0）看真实执行计划与每步耗时
4. 优化手段：加索引（大表用 gh-ost/pt-osc 或 8.0 `ALGORITHM=INSTANT`）、改写 SQL、业务侧预计算/缓存

### Q12：长事务有什么危害？怎么定位？

危害：持有锁阻塞别人、undo log 膨胀（purge 不掉）、MVCC 读视图不释放。
定位：`information_schema.innodb_trx` 按 `trx_started` 排序；或 P_S 的 `events_transactions_*` 系列表。处理：`KILL` 掉源头连接 + 代码层把长事务拆成短事务。

---

## 三、简历可用案例

> ⚠️ 使用前请把数字、业务背景替换成你的真实场景，并确保能扛住连环追问。

### 专业技能栏（可加 1 条）

- 精通 MySQL 问题排查与性能优化，深入理解 InnoDB 锁机制（Next-Key Lock / Gap Lock / 插入意向锁），具备线上死锁、连接风暴等突发故障的应急止血与根因治理能力

### 项目经历栏·案例一：死锁治理

**版本 A（简洁版，一行）**
- 排查并根治订单系统 INSERT 死锁：基于 `innodb_print_all_deadlocks` 死锁日志与 `SHOW ENGINE INNODB STATUS` 定位到"FOR UPDATE 未命中加 Next-Key Lock，INSERT 插入意向锁互相冲突"的死锁链路，改造为 `INSERT ... ON DUPLICATE KEY UPDATE` 原子写入并增加死锁自动重试，上线后死锁告警清零

**版本 B（详细版，供面试口述）**
> 我在 XX 公司解决过数据库死锁问题。死锁监控上，早期我们是定期执行 `SHOW ENGINE INNODB STATUS` 抓取 `LATEST DETECTED DEADLOCK`，但它只保留最近一次死锁容易丢，后来切换为开启 `innodb_print_all_deadlocks` 把死锁写入错误日志，接入日志平台做关键字告警。某次新功能上线后死锁告警频发，从死锁日志中 `insert intention waiting` 关键字定位到是一个"存在则 SELECT FOR UPDATE 更新、不存在则 INSERT"的逻辑：RR 级别下记录不存在时 FOR UPDATE 会对间隙加 Next-Key Lock，两个事务各持 Gap Lock（互相兼容）后同时 INSERT，插入意向锁与对方的 Gap Lock 冲突形成等待环。解决方案是改为 `INSERT ... ON DUPLICATE KEY UPDATE` 消除先查后插窗口，并在应用层对 1213 死锁错误做有限重试。后续还推动把死锁日志接入 AI 告警分析，自动关联代码提交定位问题代码段。

### 项目经历栏·案例二：连接风暴应急

**版本 A（简洁版，一行）**
- 主导大促期间数据库 Too many connections 故障应急：采用"限流收缩 + 渐进上调 max_connections（151→350）"保守止血保住活动，根因定位为突发流量 + 偶发慢查询 + 连接容量规划缺失（11 实例 × 池 30 > DB 151），通过慢查询预计算改造与"实例数 × 池上限"容量模型重建（池 20 / 库 230），后续大促零故障

**版本 B（详细版，供面试口述）**
> 一次促销活动开始瞬间爆发大量 Too many connections 错误。我先用 `SHOW STATUS LIKE 'Threads_connected'` 确认连接数已达上限 151。活动不能中断，采取保守止血：先收紧应用限流，再把 max_connections 从 151 逐步调到 200、250、350，每步观察 MySQL 负载确认稳定后再放开限流——不敢一步到位是担心连接内存开销和线程调度把库打崩。事后根因排查发现四个因素叠加：突发流量、资格校验 SQL 对老用户偶发慢查询、连接池上限 30 是初始化时复制的拍脑袋值、DB max_connections 151 也是默认值，而 11 实例 × 30 = 330 的理论需求远超 151。根治措施：资格校验改为活动前预计算；按"实例数 × 池上限 + 余量"公式重建容量模型（池调 20、库调 230）；补充 Threads_running、Max_used_connections 监控告警。

---

## 四、面试官连环追问清单（含参考答案）

### Q1：RR 和 RC 下上述死锁分别会不会发生？为什么？

- **RR：会发生。** FOR UPDATE 未命中记录时加 Next-Key Lock（锁间隙），双方持 Gap Lock 后 INSERT，插入意向锁冲突成环。
- **RC：本案例不会发生。** RC 下 Gap Lock 基本被禁用（仅外键检查和唯一键重复检查时保留），FOR UPDATE 未命中不会持有间隙锁，INSERT 不会因为别人的 Gap Lock 而等待。
- 追问防坑：RC 不是"没有锁等待"——唯一键冲突检测时 INSERT 仍会加 S 锁等待，只是场景不同。

### Q2：插入意向锁是什么？和 Gap Lock、X 锁的关系？

- Insert Intention Lock 是 **INSERT 前加在间隙上的特殊 Gap Lock**，表示"我要往这个区间插入"。
- 设计目的：提高并发插入能力——**多个事务往同一间隙的不同位置插入时，彼此的插入意向锁不冲突**。
- 冲突规则：插入意向锁与**已存在的 Gap Lock / Next-Key Lock 冲突**（别人锁了区间你就不能往里插）；获得插入意向锁后，真正插入的行再加 X 记录锁。

### Q3：死锁后应用收到什么错误码？代码怎么处理？

- 错误：`ERROR 1213 (40001): Deadlock found when trying to get lock; try restarting transaction`。Spring 体系翻译为 `DeadlockLoserDataAccessException`。
- 处理：**有限重试 2~3 次 + 小间隔退避**。因为死锁检测已回滚了对方或己方，重试几乎必成功；前提是事务幂等。
- 防坑：别和 `ERROR 1205 (Lock wait timeout exceeded)` 混淆——1213 是死锁被杀，1205 是等待超时，重试语义相同但根因不同。

### Q4：死锁检测的原理？什么场景下它自己会成为瓶颈？

- 原理：`innodb_deadlock_detect=ON`（默认），锁等待发生时构建 **wait-for graph（等待有向图）并检测环**；发现环后选择**事务权重小（修改行数少）的回滚**，代价最小。
- 瓶颈场景：热点行/热点间隙上大量并发事务互相等待时，每次新增等待边都要跑环检测，等待事务多时开销近似 O(n²)，CPU 被检测逻辑吃满。
- 应对：MySQL 8.0.18+ 对检测算法有优化；极端热点场景可**关闭死锁检测**，把 `innodb_lock_wait_timeout`（默认 50s）调小到 5~10s，用超时兜底。

### Q5：连接打满后怎么登进 MySQL kill 连接？（8.0 前后答案不同）

- **8.0 之前**：MySQL 为 SUPER 权限用户保留 **1 个额外连接**（实际允许 max_connections + 1），用 root 登录后 `KILL <thread_id>`。所以生产库务必留一个不对外使用的管理账号。
- **8.0.14+**：专用 **administrative 管理连接**（`admin_address` + `admin_port`，默认 33062），不受 max_connections 限制，专门给 DBA 救急用。
- 云 RDS 场景：控制台一般有"会话管理/kill 会话"功能，不依赖 SQL 登录。

### Q6：你们连接池配多大？依据是什么？

两个维度，缺一不可：
- **对下不超数据库容量**：`实例数 × 池上限 ≤ DB max_connections − 管理余量`（如 11 × 20 = 220 < 230）。
- **对上不超数据库算力**：HikariCP 官方建议的经验公式 `连接数 ≈ CPU 核数 × 2 + 有效磁盘数`——连接多 ≠ 吞吐高，超过 DB 算力后只是排队还徒增上下文切换。
- 落地方法：从小配起（如 10~20），压测观察 `Threads_running`、RT、连接池等待队列（HikariCP 的 pending 指标）逐步上调。

### Q7：`long_query_time` 设多少？偶发慢查询怎么抓？

- 生产常见 0.1s~1s（高敏业务 0.1s，一般业务 0.5s~1s）；配套 `log_queries_not_using_indexes=ON`。
- **偶发慢查询的抓法**（重点）：
  1. 慢日志攒够时间窗后用 `pt-query-digest` 按 SQL 指纹聚合，按总耗时排序——偶发的单看日志肉眼找不到规律；
  2. 实时方案用 P_S 的 `events_statements_summary_by_digest`，按 digest 统计 avg/max latency，偶发 SQL 的 max 值会暴露；
  3. 抓到后 `EXPLAIN ANALYZE` 验证真实执行计划（偶发常与**数据分布**有关，如老用户数据量大、统计信息过期导致走错索引）。

### Q8：线上加索引怎么做才不锁表？

- **5.6+ Online DDL**：`ADD INDEX` 默认 `ALGORITHM=INPLACE, LOCK=NONE`，执行期间允许读写——**但不是完全无锁**：开始和结束阶段需要短暂的 **MDL 元数据锁**，若此时有长事务/慢查询未结束，DDL 排队，后续所有该表查询全堵（MDL 雪崩）。执行前务必先查 `innodb_trx` 确认无长事务。
- **大表/老版本/主从延迟敏感场景**：用 **gh-ost**（基于 binlog，可暂停可限速）或 **pt-online-schema-change**（基于触发器，注意触发器开销和已有触发器的冲突）。
- MySQL **8.0**：部分 DDL 支持 `ALGORITHM=INSTANT`（秒级加列）；加二级索引仍是 INPLACE。
- 通用纪律：低峰期执行、提前评估磁盘空间（INPLACE 需要额外空间）、关注主从延迟。
