# 13 GEO 是什么？还可以定义新的数据类型吗？

> 来源：极客时间《Redis 核心技术与实战》蒋德钧（实践篇 13）
> 整理日期：2026-11-13

---

## 一、内容总结

**GEO——面向 LBS（位置服务）的扩展类型**，典型场景：叫车应用匹配附近车辆。

**GEO 的底层：没有新结构，就是 Sorted Set**
- 元素 = 车辆 ID，score（权重分数）= **经纬度经 GeoHash 编码后的值**
- Sorted Set 提供"按权重有序范围查找" → 天然支持"搜索附近"

**GeoHash 编码——"二分区间，区间编码"**：
1. **经度**（范围 [-180,180]）做 N 次二分区：每次看目标值落左半（记 0）还是右半（记 1），得 N bit 编码。例：116.37 做 5 次 → 定位到 [112.5, 123.75]，编码 11010
2. **纬度**（范围 [-90,90]）同样编码，例：39.86 → 10111
3. **组合**：**偶数位放经度编码、奇数位放纬度编码**（偶数位从 0 起）→ 1110011101——一个值就能当 Sorted Set 的 score 了
- 效果：地理空间被切成方格，**相邻方格的编码值基本接近**（一维映射保局部性），N 越大越精准
- 注意：**编码值相近不一定位置相近**（跨方格边界的两点编码可能差很远）——所以 GEORADIUS 内部要查目标方格 + 8 个邻居方格再过滤，保证准确

**GEO 的两个命令**：
```bash
GEOADD cars:locations 116.034579 39.030452 33          # 存车辆 33 的位置
GEORADIUS cars:locations 116.054579 39.030452 5 km ASC COUNT 10
# 以用户位置为中心查 5km 内车辆，按距离升序，最多返回 10 辆（控带宽）
```

**如何自定义数据类型（扩展类型的终极版）**：

**RedisObject 结构**（一切扩展的地基）：
- `type`（值的类型，五大基本类型）、`encoding`（底层编码：SDS/ziplist/哈希表/跳表）、`lru`（最后访问时间，淘汰用）、`refcount`（引用计数）、`*ptr`（指向实际数据）
- 扩展原理：定义新 type/encoding，`*ptr` 指向新结构的实现即可

**开发新类型四步**（以 NewTypeObject = Long 单链表为例）：
1. **定义底层结构**：newtype.h 里定义 NewTypeObject（head 指针 + len）和 NewTypeNode（value + next）——想高效就换 B+ 树
2. **注册 type**：server.h 加宏 `#define OBJ_NEWTYPE 7`
3. **实现创建/释放函数**：object.c 加 createNewTypeObject（zmalloc 分配 + createObject(OBJ_NEWTYPE, ptr)）；类型专属函数放 t_newtype.c（命名惯例 t_string.c/t_list.c）；释放用 zfree
4. **实现命令**：t_newtype.c 写 ntinsertCommand → server.h 声明 → server.c 的 redisCommandTable 注册 `{"ntinsert", ntinsertCommand, ...}`
- 要持久化还得在 RDB/AOF 模块加对应代码

**扩展类型的两条路**（总结）：
- **基于现有类型 + 编码/新操作**：GEO（Sorted Set + GeoHash）、Bitmap（String + 位操作）——多数场景够用
- **开发自定义类型**：改源码，彻底自由

**评论区要点**：
- Kaito：List 当队列（无 ack、丢数据，适合不敏感场景）；Pub/Sub（多消费者但只发在线者、不持久化）；**Stream（5.0，借鉴 Kafka：持久化、ack、多消费者、回溯消费）**——队列需求的演进线（15 讲展开）
- GEO 的纬度范围实际限制：±85.05112878（文档规定，近两极不可索引）
- ES 的 geo_distance 也能做距离计算——Redis 性能更好，ES 搜索功能更全

---

## 二、设计问答

### Q1：GEO 为什么用 Sorted Set 实现，不专门设计新结构？

"搜索附近"的本质是**按编码值做范围查询**——Sorted Set 的跳表天然支持 O(logN) 范围检索（02 讲）。GeoHash 把二维坐标压成一维数值后，剩下的活全是 Sorted Set 的老本行。**复用成熟结构 + 加一层编码**，比新造轮子风险小、维护成本低——这是"扩展类型两条路"里第一条的典范。

### Q2：GeoHash 为什么要把经纬度交叉组合？

经纬度各自编码后还是两个数，存不进单个 score。交叉组合（偶经奇纬）的关键性质：**编码的高位段决定了大范围的方格，低位段细化位置**——前缀相同的编码在地理上相邻（Z 阶曲线特性）。这样 Sorted Set 的范围查询才能圈出"一片区域"而不是一堆散点。

### Q3：GeoHash 编码值相近一定位置近吗？反过来呢？

位置近 ⇒ 编码值**通常**相近；编码值相近 ⇏ 位置**一定**近——两个相邻点可能落在方格边界的两侧，编码差异巨大（如同一区相邻两户分属不同邮编段）。所以 GEORADIUS 不能只查一个方格，要查目标方格 + 8 邻居再按真实距离过滤。

### Q4：Redis 的"扩展数据类型"有哪些实现途径？

①**组合复用**：现有类型 + 编码约定（GEO、Bitmap、布隆过滤器）——零源码改动，业务层就能做；②**源码扩展**：定义 RedisObject 的 type/encoding，实现创建/释放函数和命令表注册，还要处理 RDB/AOF 持久化——自由但维护成本高（升级 Redis 要 rebase）。工程上优先①，①满足不了再②。

### Q5：设计一个新数据类型的最小 checklist？

底层结构定义（t_xxx.c/h）→ type 宏注册（server.h）→ 创建函数（zmalloc + createObject）→ 释放函数（zfree）→ 命令实现 → 命令表注册（redisCommandTable）→（可选）RDB/AOF 持久化支持 →（可选）集群迁移支持（MIGRATE/DUMP 序列化）。漏掉持久化和迁移，类型就只能当玩具。

---

## 三、实践要点

- GEO 适用：静态/低频更新的位置点（门店、充电桩）；**车辆这种高频移动目标，GEOADD 更新频率要控制**（批量 + 降频），否则写压力大
- GEORADIUS 一定带 `COUNT` 限制返回量，防带宽和客户端压力
- 生产 Redis 4.0+ 注意 GEO 命令演变：GEORADIUS 已标记废弃，用 `GEOSEARCH`/`GEOSEARCHSTORE`（BYRADIUS/BYBOX）
- 自定义类型之前先问自己：现有类型 + 编码能不能解决？（90% 的情况能）改源码意味着自己扛升级
- RedisModule（4.0+ 模块系统）是官方推荐的扩展方式——不用改 Redis 源码，编译成 .so 动态加载，AGGREGATE/持久化都有 API

---

## 四、深入追问

### Q1：GEO 范围查询的内部流程完整版？

中心点经纬度 → GeoHash → 定位中心方格 → 取中心 + 8 邻居方格的 score 范围 → 在 Sorted Set 里 ZRANGEBYSCORE 捞出候选 → 逐点计算真实球面距离（haversine）→ 按半径过滤 → ASC/DESC 排序 → COUNT 截断。理解了这个流程就明白：方格邻居数是固定的，**查询成本与半径无关，与范围内点数相关**。

### Q2：RedisObject 的 lru 字段在这里有什么用？

24bit 的 lru 存的是 LRU 时钟秒数（或 LFU 的 16bit 衰减时间 + 8bit 计数）——面经 32 讲的淘汰策略就是读这个字段决定谁被淘汰。RedisObject 是"对象生命周期管理"的挂载点：类型、编码、淘汰、共享全在这一个壳里。

### Q3：和面经 33 讲"装饰器模式实现缓存模式"的呼应？

GEO 的设计哲学和装饰器一致：**不修改核心，在外层叠加能力**。Sorted Set 是被装饰的核心，GeoHash 编码是叠加的行为。自研缓存组件、自研数据类型，高手做"扩展"的思路都是一样的——找稳定的内核，在外面加一层。

### Q4：Stream 为什么放在这里提？（承接 Kaito 评论）

GEO/Bitmap/HLL 是"扩展类型三兄弟"（本课程的官方分类），Stream（5.0）是第四个也是最重要的扩展——它解决了 List 无 ack、Pub/Sub 丢消息两个硬伤，是 15 讲"Redis 消息队列方案"的主角。学到这里先记住结论：**简单队列用 List 凑合，认真做 MQ 用 Stream，但专业的活还是交给 Kafka/RocketMQ**（mq-deep-dive 的选型原则）。
