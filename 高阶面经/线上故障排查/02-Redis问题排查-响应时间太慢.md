# 面经总结 02：Redis 问题排查——响应时间太慢了，怎么办？

> 来源：极客时间《后端工程师的高阶面经》邓明
> 整理日期：2026-11-02

---

## 一、文章总结

**核心框架**：Redis 排查与 MySQL 排查同宗——**知道有哪些工具，以及这些工具怎么用**。线上故障最高频就两类：**性能问题**（对缓存响应时间最敏感）和**连接问题**（多与网络、客户端强相关）。

**故障分类**：

| 大类 | 症状 | 常见原因 |
|---|---|---|
| 性能问题 | 响应时间长 / 慢查询 / 吞吐量下降 | 高并发、大键值操作、网络延迟；资源瓶颈（CPU/内存/磁盘 IO）或配置不当 |
| 连接问题 | 连接超时 / 连接数过多 / 频繁断连 | 网络不稳、防火墙、maxclients 打满、少数是 Redis 负载过高 |
| 其他（偏运维） | 数据一致性、持久化、内存、主从复制、集群问题 | 一般由运维处理，业务开发了解即可 |

> 热点问题（hotkey）和数据倾斜单独成篇（下一讲），本篇不涉及。

**排查工具箱**（按使用频率）：

| 工具 | 用途 | 关键点 |
|---|---|---|
| SLOWLOG | 慢查询日志 | 性能问题**第一站** |
| INFO | 服务器全景信息 | 内存、连接数、持久化状态；连接问题直接看它 |
| CLUSTER INFO | 集群状态 | 集群问题专用 |
| MONITOR | 实时监控全部命令 | 性能杀手，**线上禁用**，仅开发环境 DEBUG |
| PUBSUB | 发布订阅监控 | 同 MONITOR，生产少用 |
| redis-cli --bigkeys | 大 key 扫描 | 案例二定位大 Hash 的关键命令 |
| Redis Insight | 官方图形化工具 | 日常监控 + 故障排查 |
| Prometheus + Grafana | 监控可视化 | 自定义指标和告警 |
| redis-benchmark / redis-stat / redis-faina | 压测 / 实时指标 / 慢日志分析 | 第三方补充 |

**排查分流口诀**：性能问题 → 先 SLOWLOG，再 INFO/CLUSTER INFO 确认状态；连接问题 → SLOWLOG 无有效信息，直接查 INFO/CLUSTER INFO。

**补充工具（文章未提，面试加分）**：
- `redis-cli --latency` / `--latency-history` / `--intrinsic-latency`：区分延迟来自网络/系统层还是 Redis 本身（intrinsic 是基线延迟，对比出真凶在哪一层）
- `MEMORY USAGE key`：精确看单个 key 占多少内存（`--bigkeys` 只是按元素个数采样）
- `INFO persistence` 的 `latest_fork_usec`：fork（RDB/AOF rewrite）耗时，排查主线程被 fork 阻塞
- `redis-cli --hotkeys`：热点 key 扫描（需 maxmemory-policy 为 LFU 类）
- 4.0+ `UNLINK` 异步删除大 key、`lazyfree-lazy-expire` 配置：治本层面缓解大 key 删除/过期阻塞

**面试策略要点**：
1. **慢查询案例必须准备**，可融入整体性能优化方案中讲，作为一个环节
2. 案例复杂度与工作年限、目标岗位匹配——年限越高案例越要复杂，不匹配反而被看扁
3. 讲案例**一定要解释到底层原因**（如 malloc_trim 阻塞主线程），用原理深度证明自己
4. 排查过程讲成"监控 → SLOWLOG → bigkeys → 抽样验证"的排除法推理链

---

## 二、Q&A（问题 + 标准答案）

### Q1：线上 Redis 响应突然变慢，你的排查思路是什么？

按三步曲：先确认**是 Redis 慢还是链路慢**（应用 RT、网络、客户端连接池等待），再按性能问题套路定位，最后修复。
- 发现：监控告警（Redis RT/QPS/慢查询数/连接数）+ 应用侧 command timeout 报错
- 定位：`SLOWLOG GET` 看有没有慢命令 → 有则按慢命令类型治理；没有则 `INFO` 检查内存（used_memory vs maxmemory、是否触发淘汰）、CPU、连接数、持久化状态 → 再查大 key（`--bigkeys`）
- 修复：止血（拆分大 key、限流）+ 根治（数据结构改造、容量规划、监控补齐）

### Q2：Redis 线上故障分哪几类？

两大类最高频：
1. **性能问题**：响应时间长、慢查询（响应时间长的特例/极端例子）、吞吐量下降
2. **连接问题**：连接超时、连接数过多（超 maxclients 拒绝新连接）、频繁断连——多与网络、客户端相关，少数是 Redis 负载过高

其余（数据一致性、持久化、内存、主从、集群）一般运维主导。热点 key 和数据倾斜是两类特殊问题，单列讨论。

### Q3：Redis 慢查询怎么发现和治理？

- 发现：`SLOWLOG GET` 查看慢查询记录；生产用慢查询监控（采集 SLOWLOG 或 Redis Insight）做告警
- 典型根因：**单条命令返回/操作大量数据**——大 hash 的 HGETALL、大 zset 的 ZRANGEBYSCORE 命中几十万元素
- 治理两板斧：
  1. **拆**：大 hash 按业务维度拆成多个 key（key1~key10 各放一部分 field），业务按需访问
  2. **分页**：ZRANGEBYSCORE 改游标式分页——前端每次带 `minScore`，命令带 `LIMIT 100`，等价于 `score >= minScore LIMIT 100`，每次只取前 100 名

> bigkey 为什么这么多灾多难（删除、持久化、复制、集群各路径的机制），见文末"五、专题深挖"。

### Q4：MONITOR 命令能在线上用吗？为什么？

**不能。** MONITOR 实时输出 Redis 收到的所有命令，高并发下输出量巨大，会显著拖慢主线程（有测试显示吞吐下降可达 50% 以上）。只在开发/测试环境 DEBUG 用。生产环境要看命令分布，用慢查询日志或采样分析工具（如 redis-faina）。

### Q5：Redis 连接问题怎么排查？

SLOWLOG 对连接问题基本无效，直接看 `INFO clients`：
- `connected_clients` vs `maxclients`（默认 10000）：打满则新连接报 `ERR max number of clients reached`
- `CLIENT LIST`：看连接来源 IP、空闲时长（idle）、状态，定位是哪个应用/实例泄漏连接
- 应用侧：检查连接池配置（最大连接数、空闲回收策略）、是否未关闭连接

### Q6：凌晨 1 点周期性 command timeout，怎么排查？（文章核心案例）

排除法推理链：
1. **查监控**：CPU、内存、磁盘 IO 全部正常 → 排除资源瓶颈和持久化影响
2. **查 SLOWLOG**：无慢查询记录 → 排除"某条慢命令堵住其他命令"
3. **查大 key**：`redis-cli --bigkeys` 发现大量超过 1MB 的 Hash 结构 → 开始怀疑
4. **抽样验证**：抽这些 Hash 的 TTL，发现**过期时间全部集中在凌晨**；从 key 命名反查业务，确认是新上线定时任务产生，任务凌晨运行、过期时间恰好一天
5. **指标佐证**：监控中 `expired_time_cap_reached_count` 在该时段飙高 → 确认是大量 key 短时间集中过期引发

### Q7：为什么大量大 key 集中过期会导致主线程阻塞？

关键链路：大量 key 同一时刻过期 → Redis 执行过期删除 → 释放内存时触发 **glibc 的 malloc_trim（归还内存给 OS）阻塞主线程** → 所有命令超时。
根因不在 Redis 逻辑本身，而在**内存分配器**：大 key 释放的内存块大，glibc 归还内存时的整理操作耗时长，且发生在主线程路径上。

### Q8：这个周期性故障怎么解决？

1. **过期时间加随机数**：避免 key 大量集中过期（顺带防缓存雪崩）
2. **优化定时任务**：把一个大 Hash 拆成多个 key 存储，单个 key 变小，过期处理压力分散
3. 理论方案（动静大，一般不采用）：换内存分配器（glibc → jemalloc/tcmalloc）；调 glibc `M_TRIM_THRESHOLD` 环境变量减少 malloc_trim 触发频率

### Q9：怎么设计监控提前预防这类问题？

- 大 key 巡检：定期 `--bigkeys` 或 MEMORY USAGE 采样，纳入巡检报表
- 过期风暴预警：监控 `expired_keys` 速率与 `expired_time_cap_reached_count`，突增告警
- 慢查询监控：SLOWLOG 采集告警（或 Redis Insight）
- 容量监控：used_memory / maxmemory、connected_clients / maxclients 水位线告警

### Q10：面试讲 Redis 排查案例，有什么讲究？

- 慢查询案例必备，融进整体性能优化方案里讲
- 必须讲透底层原因（如 malloc_trim），案例只是引子，**原理深度才是得分点**
- 案例难度匹配年限：初中级岗位讲 hash 拆分够用；高年限/专家岗要讲周期性故障这种有完整推理链的复杂案例
- 结尾加反思：如"后来把大 key 和过期策略监控前置，防患于未然"——展现体系化意识

---

## 三、简历可用案例

> ⚠️ 使用前请把业务背景、数字替换成你的真实场景，并确保能扛住连环追问（尤其是 malloc_trim 的细节）。

### 专业技能栏（可加 1 条）

- 精通 Redis 问题排查与性能优化，熟悉 SLOWLOG / INFO / bigkeys 等诊断手段，深入理解大 key、集中过期引发的阻塞原理（glibc malloc_trim），具备慢查询治理、周期性故障定位与容量治理能力

### 项目经历栏·案例一：慢查询治理

**版本 A（简洁版，一行）**
- 治理 Redis 慢查询：通过慢查询监控定位到大 hash 全量读取与 ZRANGEBYSCORE 大范围命中两类慢命令，主导 hash 结构按业务维度拆分多 key、zset 查询改造为"minScore 游标 + LIMIT" 的分页模式，慢查询告警清零

**版本 B（详细版，供面试口述）**
> 我们的业务有两类慢查询。第一类是全局配置放在一个 hash 里，随着系统演进这个 hash 越来越大，很多业务全量读取导致慢查询。我通过慢查询监控发现后，主导把这个 hash 按业务维度拆分成多个 key，业务方按需访问自己那部分，慢查询消失。第二类是某业务的有序数据放在 zset 里，数据量大的业务有上十万元素，ZRANGEBYSCORE 偶发命中大量元素引发慢查询。我把查询改成游标式分页：前端每次带 minScore，命令执行等价于 score >= minScore LIMIT 100，每次只取前 100 名，彻底解决了偶发慢查询。

### 项目经历栏·案例二：周期性 timeout 排查

**版本 A（简洁版，一行）**
- 定位并根治 Redis 凌晨周期性超时故障：通过"监控排除资源瓶颈 → SLOWLOG 排除慢命令 → --bigkeys 定位大 Hash → 抽样 TTL 确认集中过期 → expired_time_cap_reached_count 佐证"的完整推理链，确认根因为大量大 key 集中过期触发 glibc malloc_trim 阻塞主线程，通过过期时间加随机扰动 + 定时任务拆 key 根治，故障清零

**版本 B（详细版，供面试口述）**
> 我负责的项目遇到过一个有趣的挑战：Redis 每天凌晨 1 点周期性出现问题，业务频繁收到 command timeout，持续一段时间，其他时段完全正常。我按排除法排查：首先查监控，CPU、内存、磁盘 IO 都正常，排除资源瓶颈；然后查 SLOWLOG，没有慢查询记录，排除单条慢命令堵塞；接着用 redis-cli --bigkeys 扫描，发现大量超过 1MB 的 Hash 结构；抽样检查这些大 key 的过期时间，发现全部集中在凌晨过期，从 key 命名反查确认是新上线的定时任务产生的，任务凌晨跑、过期时间恰好一天；最后看监控，expired_time_cap_reached_count 在该时段非常高，坐实了大量 key 短时间集中过期。底层原因是：大量大 key 集中过期后，Redis 删除释放内存时触发 glibc 的 malloc_trim 归还内存，阻塞了主线程，导致所有命令超时。解决方案有两个：一是过期时间加随机数，避免集中过期（顺带防缓存雪崩）；二是优化定时任务，把大 Hash 拆成多个小 key。实施后问题彻底解决。后续我还推动把大 key 和过期策略的监控前置，防患于未然。

---

## 四、面试官连环追问清单（含参考答案）

### Q1：SLOWLOG 的两个核心配置参数是什么？有什么坑？

- `slowlog-log-slower-than`：慢查询阈值，**单位是微秒**，默认 10000（即 10ms）；生产高敏业务常设 1000（1ms）。设为 0 记录所有命令，-1 关闭
- `slowlog-max-len`：慢日志队列最大长度，默认 128 条，生产建议调大（如 1000），否则高频慢查询会冲掉旧记录
- **坑**：SLOWLOG 只记录命令**执行时间**，不包含网络传输和排队等待时间——所以"SLOWLOG 没有慢查询但客户端感觉慢"完全可能，这正是案例二的剧情

### Q2：malloc_trim 为什么会阻塞主线程？还有什么方案？

- glibc 的 ptmalloc 在 free 大块内存时会触发 malloc_trim，把空闲内存归还 OS，该操作涉及堆的整理和系统调用（brk/mmap），大内存场景耗时可达毫秒级以上，而过期删除发生在主线程路径上，直接阻塞所有命令
- 治理方向：
  1. 业务侧（文章方案）：随机过期时间 + 拆小 key，避免瞬时大量大内存释放
  2. Redis 侧：4.0+ 用 `UNLINK` 异步删除；配置 `lazyfree-lazy-expire yes`（6.0+）让过期 key 的内存释放交给后台线程
  3. 系统侧：换 jemalloc/tcmalloc（不主动 trim，Redis 官方默认编译 jemalloc）；或调 glibc `M_TRIM_THRESHOLD`
- 防坑：面试官若问"为什么官方用 jemalloc 还会遇到"——自行编译或某些发行版用 glibc 才会踩到，先确认部署环境

### Q3：`redis-cli --bigkeys` 的原理是什么？线上执行安全吗？

- 原理：基于 **SCAN 游标遍历**全库（非 KEYS，不会一次阻塞），对每个 key 采样统计，输出每种数据类型中最大的 key
- 安全性：SCAN 是增量迭代，单次执行时间短，**不会阻塞主线程**，线上可用；但注意它只报"每种类型最大的几个"，且按元素个数而非字节数衡量，精确大小要用 `MEMORY USAGE key` 补查
- 对比防坑：`KEYS *` 线上禁用（O(N) 全量阻塞）；生产查 key 一律 SCAN 系

### Q4：zset 分页为什么用 minScore 游标，而不用 LIMIT offset？

- `ZRANGEBYSCORE key -inf +inf LIMIT 100000 100` 这种 offset 分页，Redis 仍需**先扫描跳过前 offset 个元素**，复杂度 O(logN + offset)，页越深越慢，治标不治本
- minScore 游标式：直接按 score 定位，复杂度 O(logN + count)，每页代价恒定
- 防坑细节：score 可能重复，翻页条件要用**开区间** `(minScore`（排除上一页最后一个 score）配合第二排序键，否则可能丢数据或重复——被追问时主动提这点加分

### Q5：hash 拆成多个 key 后，会不会引入新问题？

- 跨 key 原子性丢失：原来一个 HGETALL/事务能搞定的，拆完后要么多次命令要么 pipeline；集群模式下多 key 还涉及跨 slot，需要用 hash tag（`{biz}:key1`）保证同 slot，或用 Lua 脚本
- 读取放大：需要聚合全量数据的业务要 mget/pipeline 多次，网络 RTT 增加——所以拆分要**按业务访问维度**拆，让大多数业务只访问一个 key
- 这正是文章话术里"业务方根据自己的需要访问拆分后的 key"的深意

### Q6：`expired_time_cap_reached_count` 是什么指标？

- Redis 主动过期（active expire cycle）每轮执行有 CPU 时间上限（默认不超过单轮的 25%），该指标统计**过期循环因超时被强制中断的次数**
- 飙升含义：短时间内要过期的 key 太多，一轮处理不完——是"集中过期风暴"的直接证据指标
- 关联指标：`expired_keys`（累计过期 key 数）看总量，`expired_time_cap_reached_count` 看瞬时压力

### Q7：怎么区分"过期删除阻塞"和"持久化 fork 阻塞"？

两者都会让主线程卡住，区分方法：
- fork 阻塞：`INFO persistence` 的 `latest_fork_usec`（最近一次 fork 耗时，微秒）飙高；RDB bgsave / AOF rewrite 有明确触发时间点（`rdb_last_bgsave_time_sec` 等）；实例内存越大 fork 越慢
- 过期删除阻塞：与 key 过期时间点强相关（周期性），`expired_time_cap_reached_count` 同步飙高
- 案例二中"CPU、内存、磁盘 IO 正常"第一步就排除了持久化方向——**监控先行排除是推理链可信度的关键**

### Q8：maxclients 打满怎么办？和 MySQL 的连接打满处理有什么异同？

- 确认：`INFO clients` 的 `connected_clients` vs `CONFIG GET maxclients`（默认 10000）
- 止血：`CLIENT KILL` 按来源 IP/空闲时长批量踢连接；应用侧收紧连接池、重启泄漏实例
- 根治：连接池容量规划（实例数 × 池上限 ≤ maxclients × 安全水位）、排查连接泄漏（idle 长的连接）
- 异同：思路与 MySQL 一致（容量公式通用）；不同在于 Redis 单连接开销远小于 MySQL 线程模型，maxclients 默认值就上万，打满多半是**客户端连接泄漏**而非容量不足——先查泄漏再谈扩容

---

## 五、专题深挖：大 key 为什么麻烦（不止查询慢）

> 本篇 Q3/Q6-Q8 的案例都与 bigkey 有关，但 bigkey 的危害远不止查询慢——它在 Redis 的**每一条关键路径上都埋了雷**。以下机制按层展开，是应对深度追问的素材。

**先明确 bigkey 的两种形态**（麻烦点不同）：
- String 型：value 本身大（如 5MB 的 JSON）
- 集合型：单元素不大，但元素多（hash 几十万 field、zset 上百万成员）

### 1. 查询慢：单线程模型把线性开销放大成全局排队

**集合型**：`HGETALL` 10 万 field 的 hash，主线程要遍历整个 hashtable → 逐个协议编码 → 拷贝进客户端输出缓冲区，约 5MB 数据的遍历+编码+拷贝 = 几十毫秒。**这期间实例上所有其他命令全部排队**——Redis 单机 10W QPS 的前提是每条命令都足够便宜（微秒级），bigkey 直接破坏该前提。

**String 型**：GET 查找是 O(1)，慢在搬运——5MB 的内存拷贝 + 网卡发送（千兆网卡 ~40ms/次，几十 QPS 就打满带宽）+ 客户端反序列化（5MB JSON 解析几十毫秒 + 大对象可能直进老年代触发 GC）。**SLOWLOG 看不到**（只记命令执行时间，不含网络传输）——经典追问坑。

### 2. 删除/过期：释放内存比申请内存贵得多

- `DEL` 大集合不是标记删除，而是 O(N)：遍历 N 个元素逐个释放 sds → 释放桶数组 → 每步都经过内存分配器 free，毫秒~几十毫秒级主线程阻塞
- **malloc_trim 雪上加霜**：glibc free 大块内存后触发 `malloc_trim` 归还 OS——堆空闲块整理合并 + `brk`/`madvise` 系统调用 + 内核摘页表（后续访问还会缺页中断，有余震）
- 集中爆发场景：大量大 key 同一时刻过期 → 主动过期循环在主线程逐个 O(N) 删除 → 释放触发 malloc_trim → 主线程卡死 → 全体 timeout（即案例二完整因果链）
- 修复手段与机制的对应关系：随机过期时间 = 把集中的释放成本**摊平到时间轴**；拆 key = 把单次 O(N) 的 N **降数量级**；`UNLINK`/`lazyfree-lazy-expire` = 把释放**移出主线程**

### 3. 持久化：fork 的 COW 写放大被引爆

bgsave/AOF rewrite 的 fork 本身只复制页表，之后靠 Copy-On-Write：父进程每修改一个页就先拷贝一份（4KB，THP 下 2MB）。一个大 hash **横跨成千上万个内存页**，fork 期间业务持续更新它 → 每个被碰到的页都拷贝 → 极端情况 COW 拷贝量接近整个数据集，**物理内存翻倍，直接 OOM/swap**。"32G 实例 bgsave 时内存不足"的事故背后常是它。

### 4. 主从复制：全量同步灾难

从库重连后 `repl_backlog` 覆盖不住断线期间的写入 → 全量同步（fork RDB → 传输 → 加载）。bigkey 越多 RDB 越大、同步越久，期间主节点写入又撑爆 backlog → 同步完追不上 → 再来一次全量（恶性循环）；slave 侧 `client-output-buffer-limit` 超限也会断连重同步。

### 5. 集群：迁移阻塞 + 数据倾斜

- 扩缩容 `MIGRATE` 搬 key 旧版本**同步阻塞**——迁移一个大 key，源和目标实例双卡
- bigkey 落在固定 slot 无法拆分均衡：该节点内存/CPU 先爆，其他节点再空闲也没用；bigkey 常同时是 hotkey，两个绝症叠加

### 6. 内存碎片：慢性损耗

大对象反复分配释放制造碎片：`mem_fragmentation_ratio`（RSS/used_memory）持续 > 1.5 要警惕——数据没变多但机器内存快用完，且 jemalloc 不积极 trim，内存"只进不出"。

### 7. 客户端：输出缓冲区把你踢下线

HGETALL 返回 5MB + 客户端消费慢 → 服务端输出缓冲区堆积 → 超过 `client-output-buffer-limit` **强制断连** → 客户端重连再全量拉取 → 循环放大，雪崩剧本。

### 代价速查表

| 路径 | bigkey 的代价 | 根因机制 |
|---|---|---|
| 查询 | 单命令几十 ms，全实例排队 | O(N) 遍历 + 单线程串行 |
| 网络 | 带宽打满，全体变慢 | 大 value 传输 + 输出缓冲区拷贝 |
| 删除/过期 | 主线程阻塞、周期性 timeout | O(N) free + glibc malloc_trim |
| 持久化 | fork COW 写放大，内存翻倍 | bigkey 横跨大量内存页，修改触发整页拷贝 |
| 复制 | 全量同步风暴 | backlog 溢出 → 大 RDB 传输加载 |
| 集群 | 迁移阻塞、数据倾斜 | MIGRATE 同步搬运 + key 不可拆分 |
| 内存 | 碎片率高，只进不出 | 大对象反复分配释放 |
| 客户端 | 连接被 kill、反序列化 GC | 输出缓冲区超限 + 大对象进老年代 |

**一句话总结**：Redis 所有的设计红利（单线程无锁、内存操作、COW fork、增量复制）都建立在"key 小而均匀"的假设上，bigkey 恰好把这个假设打穿了。面试时能讲透其中两三层机制（而非只会说"拆 key"），就与背八股的候选人拉开了差距。
