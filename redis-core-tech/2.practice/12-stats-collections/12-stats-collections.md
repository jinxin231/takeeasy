# 12 有一亿个 keys 要统计，应该用哪种集合？

> 来源：极客时间《Redis 核心技术与实战》蒋德钧（实践篇 12）
> 整理日期：2026-11-13

---

## 一、内容总结

**四种统计模式 × 集合选型**（本讲核心框架）：

### ① 聚合统计（交并差）——Set

场景：统计每日新增用户、留存用户。
- 每天登录用户存一个 Set（`user:id:20200803`），新增 = 当天 Set；**留存 = 两天 Set 求交集**（`SINTERSTORE`）
- ⚠️ **风险**：差/并/交集计算复杂度高，数据量大时阻塞实例。对策：**选专用从库做聚合，或读回客户端计算**——别在主库上算
- Kaito 纠错：`SINTERSTORE/SUNIONSTORE/SDIFFSTORE` 会**生成新 key**，从库默认 readonly 写不了——只能在主库执行；从库用不落盘的 `SINTER/SUNION/SDIFF`

### ② 排序统计——Sorted Set（别用 List 分页）

场景：商品最新评论列表分页。
- **List 的坑**：按**位置**排序，新元素 LPUSH 到队头后所有元素后移——翻页间隙来了新评论，下一页会**重复读到**上一页的元素（{A..F} 翻页间隙插入 G，第二页 LRANGE 3 5 读到的是 C、D、E 而不是 D、E、F）
- **Sorted Set 按权重（score）排序**：评论时间戳做 score，`ZRANGEBYSCORE` 按权重范围取——新元素插入不影响已读范围的准确性
- 结论：**List 只当队列用；需要稳定排序分页用 Sorted Set**（02 讲"因地制宜"的实战版）

### ③ 二值状态统计——Bitmap

场景：用户签到（签/没签）。
- Bitmap = String 底层的 bit 数组，**一个元素只占 1 bit**
- 操作：`SETBIT key offset 1`（签到）、`GETBIT`（查某天）、`BITCOUNT`（统计次数）、`BITOP`（多个 Bitmap 按位与/或/异或）
- 大杀器：**1 亿用户连续 10 天签到统计**——每天一个 1 亿位 Bitmap（≈12MB），10 天 ≈120MB；`BITOP AND` 10 个 Bitmap → `BITCOUNT` 即得连续签到总数
- 实践：签到类 Bitmap **设过期时间**，自动清理

### ④ 基数统计（去重计数）——HyperLogLog

场景：网页 UV（同一用户多次访问只算一次）。
- Set（`SADD`+`SCARD`）或 Hash（`HSET`+`HLEN`）能精确去重，但**千万 UV × 上千页面**时内存爆炸
- **HyperLogLog**：`PFADD` + `PFCOUNT`，**每个 HLL 固定 12KB**，可统计近 2^64 个元素的基数
- 代价：**概率算法，标准误差 0.81%**（UV 报 100 万实际可能 101 万）——要精确数字回 Set/Hash
- Kaito 补充：`PFCOUNT key1 key2` / `PFMERGE` 可算多页面合并 UV

**选型总表（作者建议保存）**：

| 统计模式 | 首选 | 备注 |
|---|---|---|
| 聚合（交并差） | Set | 差集只有 Set 支持；Bitmap 间也能与/或/异或；大计算量去从库/客户端 |
| 排序 | Sorted Set | List 位置排序翻页会重复 |
| 二值状态 | Bitmap | 1 bit/元素，海量二值场景省内存 |
| 基数（去重） | HyperLogLog | 亿级 + 容忍 0.81% 误差；精确用 Set/Hash |

**评论区要点**：
- Kaito：ZSet 统计时间段在线用户（`ZADD ts + ZCOUNT start end`）；`ZINTERSTORE aggregate max` 求多天都在线的用户
- 集群模式警告：多 key 聚合命令要求 key 同槽，跨实例无法聚合（报错或结果错误）；大统计与在线业务**实例隔离部署**
- 现实派评论：海量 PV/UV 业界主流走数仓/实时流（Hive/Flink），Redis 方案适合中等规模快速落地

---

## 二、设计问答

### Q1：一亿用户的签到统计怎么设计？内存多少？

每天一个 Bitmap：key 含日期（`sign:20200803`），offset = 用户 ID，bit = 是否签到。1 亿位 ≈ 12MB/天。查连续 N 天签到：`BITOP AND result key1...keyN` + `BITCOUNT`。对比 Set 方案：每个签到记录至少几十字节（dictEntry+SDS），1 亿用户一天就是 GB 级——**Bitmap 省了两个数量级**。

### Q2：Set 的聚合操作为什么危险？

SINTER/SUNION/SDIFF 要遍历参与集合，复杂度 O(N)；SINTERSTORE 等还要写新 key。大集合（千万级）上执行就是秒级阻塞主线程。对策：①从库只读版命令（SINTER 不带 STORE）；②专用实例/客户端计算；③集群下注意多 key 必须同槽（hash tag），否则跨实例无法聚合。

### Q3：UV 统计，Set vs Hash vs HyperLogLog？

| | 精确度 | 内存 | 适用 |
|---|---|---|---|
| Set | 精确 | 每元素几十字节 | 万~百万级 UV |
| Hash | 精确（HSET 幂等覆盖） | 同 Set 量级 | 同 Set，可顺带存额外信息 |
| HyperLogLog | 0.81% 误差 | **固定 12KB** | 亿级 UV，允许近似 |

### Q4：最新列表分页为什么 Sorted Set 比 List 准？

List 按位置取（LRANGE offset count），位置是**相对**的——新元素插入把所有位置平移，翻页读到重复。Sorted Set 按 score 取（ZRANGEBYSCORE min max），score 是**绝对**的（时间戳），新元素插入不改变已有元素的 score 边界——只要记住上一页最后一条的 score 作为游标，永远不重不漏。**游标式分页 vs 位置式分页**，这个原则在 DB 分页同样成立。

### Q5：HyperLogLog 误差不影响业务吗？

看业务：报表展示、趋势分析、AB 统计 → 0.81% 完全无感；计费、风控、发奖 → 必须精确，回 Set/Hash 或上数仓。面试话术："HLL 是用 0.81% 误差换固定 12KB 内存，我先问业务能不能容忍误差再选型。"

---

## 三、实践要点

- **统计类大 key 操作（SINTERSTORE/BITOP）别在在线主库跑**——单独部署统计实例或从库只读命令
- Bitmap 的 offset 上限：String 最大 512MB = 42 亿 bit，够用但注意单 key 也是大 key（BITOP 输出同样大）
- 集群模式：聚合类多 key 命令加 **hash tag** 保证同槽，或干脆统计流量走独立单实例
- 签到/在线状态类 Bitmap、每日 UV 的 HLL 都**按周期设过期**，防内存无限增长
- 超大数据量 + 复杂分析 → 别硬用 Redis，数仓/Flink 是正解（评论区现实派观点，面试承认边界更可信）

---

## 四、深入追问

### Q1：Bitmap 为什么是按"天"分 key 而不是一个 key 全包？

单 key 上限 512MB（42 亿 bit）是硬约束；按天分 key 还带来三个好处：①按天过期回收内存；②BITOP 跨天聚合灵活；③避免单 key 成为热点/大 key。分 key 的维度（天/业务线）按查询模式定。

### Q2：HyperLogLog 的 12KB 是怎么做到固定的？（原理简述）

分桶 + 调和平均：16384 个桶（2^14），每个桶记录"哈希值前导零的最大长度"。所有元素哈希后按高位分桶、低位数前导零——基数越大，观察到的最大前导零越大。桶数固定所以内存固定；估的是统计量不是逐元素，所以有误差。Redis 的 HLL 实际是稀疏/稠密双表示，稀疏时更省。面试讲到"分桶 + 前导零最大长度 + 调和平均"即可。

### Q3：和面试缓存模块的串联？

Bitmap 和布隆过滤器是亲兄弟：BF ≈ Bitmap + k 个哈希函数（面经 35 讲穿透）。Kaito 评论也点了：BF 底层就是 String + 位运算。面试问"Bitmap 有什么用"，答完签到顺手带到 BF，两条知识线就织起来了。

### Q4：Kaito 的 ZSet 在线用户统计为什么优雅？

`ZADD online_users <上线时间戳> <user_id>` 天然双功能：去重（同 user_id 重复 ZADD 只更新时间）+ 按时间段计数（ZCOUNT start end）+ 查询最近上线时间（score 即信息）。一个结构同时解决"在不在、多少、啥时候在"——比 Set + 额外时间戳字段省一层。
