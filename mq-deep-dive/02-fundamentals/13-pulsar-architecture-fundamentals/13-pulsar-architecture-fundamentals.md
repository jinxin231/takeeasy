# 13：从基础功能拆解 Pulsar 的架构设计与实现

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

**定位**：Kafka 升级版——**存算分离、多租户、多协议、百万 Topic**，消息 + 流一体。解决 Kafka 痛点：计算层弹性差、超大分区支持弱。

**最大区别：计算存储分离**——"Pulsar" = **计算层 Broker 集群 + 存储层 BookKeeper 集群**（独立开源日志存储项目：Bookies + ZK）。对比 Kafka：Broker 收到数据写本地文件；Pulsar：Broker 调 BookKeeper SDK 写远程集群。部署上两层独立，**多套 Pulsar 集群可共享一套 BookKeeper**。ZK 通常 Pulsar 与 BookKeeper **共用一套**（省资源）。

**① 协议**：自定义私有协议，**行格式解析**（4 字节总长 → 4 字节命令长 → 命令内容），命令和元数据用 **Protobuf** 组织。两类命令：**SimpleCommands**（连接、心跳，不带消息体）/ **MessageCommands**（生产等，带 Broker Entry Metadata、Message Metadata 等）。

**② 网络**：Netty（行业标准姿势）。

**③ 元数据**：核心是 ZK；**Pulsar 元数据更多**（比 Kafka 压力大）→ 新版支持**可插拔元数据框架**（etcd、本地内存、RocksDB）缓解 ZK 瓶颈。ZK 的集群规模/分区数问题与 Kafka 相同。

**④ 消息存储**：
- Broker 侧按分区组织；底层数据以 **Ledger 段**组织，每 Ledger 含一批 **Entry**（≈一条消息）；**一个分区同时只有一个 Ledger 可写**，超长/超量切新 Ledger
- Ledger 按 **QW（写副本数）** 分布到多个 Bookie；一个 Bookie 可存多个不连续 Ledger
- **弹性的来源**：某 Bookie 负载高/容量满 → **禁写该节点**，负载快速转移——存储弹性就是 Ledger 的可调度性
- 写入性能主要依赖 BookKeeper 底层（**WAL + 批量写 + 写缓存**）
- **TTL vs Retention**（易混）：TTL = 时间内未被 ACK 的消息被 Broker **主动 ACK**（不删数据）；Retention = 已 ACK 消息在 Bookie **继续保留多久**，以 Ledger 为最小单元整段删。**TTL 管确认、Retention 管删除**

**⑤ 生产端**：
- **访问模式**（独有）：**Shared**（多生产者写同 Topic）/ **Exclusive**（独占，别人写报错）/ **WaitForExclusive**（灾备，别人挂起等待，类似 ZK 观察者）
- 路由模式：**RoundRobinPartition**（轮询）/ **SinglePartition**（随机选一个分区集中写）/ **CustomPartition**（实现 MessageRouter）；指定 Key 则优先按 Key Hash
- 协议支持 Batch 语义 → 客户端批量发送（攒批：最大条数/最大延迟）；同步/异步写

**⑥ 消费端**：Pull 模型；**订阅（Subscription）≈ Kafka 消费分组**。四种订阅类型：
| 类型 | 语义 |
|---|---|
| Exclusive 独占 | 订阅只关联一个消费者，挂了就停 |
| Failover 灾备 | 多消费者关联，只有一个消费，挂了切换 |
| Shared 共享 | 多消费者轮询接收（不保序） |
| **Key_Shared** | 同 Key 消息固定到同消费者（**保序 + 并行**，Kafka 做不到） |

- **持久化 vs 非持久化订阅**：游标是否持久化（Broker 重启是否保留进度）。Kafka/RocketMQ 的分组都是持久化
- 确认：**累积确认**（确认最后一条，之前全不再投——性能好）/ **单条确认**（每条 ACK——会造成**消息空洞**，给 Broker 压力大）；**取消确认**（unack 后可重新消费，单条/批量，不同订阅类型支持度不同）

**⑦ 管控与 HTTP**：**数据流与管控分离**——私有协议端口跑数据，独立 HTTP Server 跑管控 REST API（CLI 底层也是 HTTP Client）；HTTP 也提供简单生产消费接口，但**不建议大流量用**（大流量走 Proxy 方案）。

---

## 二、设计问答（Q&A）

### Q1：存算分离给 Pulsar 带来什么？

①计算层无状态化——Broker 崩溃/扩缩容不搬数据，秒级弹性；②存储独立扩容、独立优化（WAL 盘等）；③多套计算集群共享存储（多租户/多环境省资源）。代价：多一跳网络写、架构复杂度（Broker + BK + ZK 三套）、当前稳定性不如 Kafka 成熟。

### Q2：Key_Shared 为什么是个亮点？

Kafka 世界里"保序"和"并行"互斥（独占保序但单线程，共享并行但乱序）。Key_Shared 按 Key 哈希把消息粘到消费者：**同 Key 有序 + 不同 Key 并行**——业务内有序的最优解直接内建，不用自己算分区（对比 3.mq 24 笔记的手工方案）。

### Q3：TTL 和 Retention 为什么分成两个概念？

因为有两个不同的诉求：①"消息太久没人理就当他死了"（TTL，业务语义，动作是 ACK——影响的是**可消费性**）；②"已消费的消息还要留多久"（Retention，存储语义，动作是删 Ledger——影响的是**可回溯性**）。分开才能表达"ACK 了但留 7 天供重放"这种组合。Kafka 只有 retention（没 ACK 概念），RabbitMQ 只有 TTL/ACK 删除——Pulsar 的二元模型表达力最强。

### Q4：思考题——Pulsar 为什么对 ZK 压力大？

Pulsar 元数据**量和变更频率都更高**：①多租户层级（Tenant/Namespace/Topic 三层）；②订阅游标状态（持久化订阅的进度在 ZK）；③Bundle（Topic 到分区的动态映射，弹性调度时频繁变更）；④Ledger 元数据（BookKeeper 侧也写 ZK）。Kafka 的 ZK 痛点（写瓶颈、规模上限）在 Pulsar 被放大——所以 Pulsar 搞可插拔元数据（RocksDB/etcd），Kafka 搞 KRaft。

### Q5：消息空洞是什么？为什么单条确认会导致它？

单条确认下，位点只能推进到"连续已确认"的边界——前面有一条没 ACK，后面全确认了也不能提交进度，形成"空洞"：空洞消息占着 redelivery 状态，Broker 要持续追踪它们的超时重投，内存和调度压力随空洞数膨胀。累积确认没有空洞但粒度粗（中间失败的消息会被连带重投）。**确认粒度 = 精确性 vs Broker 状态的权衡**。

---

## 三、实践要点

1. Pulsar 部署清单：ZK + BookKeeper（Bookies）+ Broker 三集群起步——容量规划分别算
2. 订阅类型决策：要顺序 → Exclusive/Failover；要并行 → Shared；**同 Key 有序 + 并行 → Key_Shared**
3. 生产端确认机制选型：吞吐优先用累积确认 + 批量 ACK；精确重投用单条确认（警惕空洞）
4. 生产可靠性：Retention 设长 + 持久化订阅，TTL 慎用（会把未消费消息标记已读）

---

## 四、深入追问

### Q1：Pulsar 的"百万 Topic"是怎么做到的？Kafka 为什么不行？

Kafka 的 Topic 数瓶颈在**分区即文件**（FD、page cache 碎片、刷盘风暴、rebalance 成本随分区线性涨）。Pulsar 的 Topic/分区是**逻辑概念 + Ledger 动态创建**：分区数据在 BK 里是 Ledger 段，不独占文件/fd；计算层无状态，Topic 创建只是元数据注册。所以百万 Topic 的本质是**把"分区=物理资源"解耦成"分区=逻辑视图"**。

### Q2：Bundle 是什么？为什么为弹性而生？

Bundle 是 Topic 与分区的**中间映射层**：Topic 先哈希到 Bundle，Bundle 再分配到 Broker。Broker 负载高时把部分 Bundle **卸载（unload）转移**到其他 Broker——不用迁移数据（数据在 BK），只转移"服务权"。这是计算层弹性的实现机制：**调度单元从"分区+数据"变成"Bundle 路由权"**，轻到毫秒级。

### Q3：WaitForExclusive 和 ZK 观察者模式像在哪？

都是"备胎挂起等转正"：Exclusive 是第二个写入者直接报错（快速失败），WaitForExclusive 让备胎连接挂着等，主挂自动接管——写入者的**主动/被动灾备**。类比 ZK 的 watch 等通知。差别：Pulsar 这个语义在 Broker 层内建，业务无感。

### Q4：存算分离下，读路径会发生什么变化？

读也要过网络（Broker → BK）——所以 Pulsar 在 Broker 加了**读缓存**（ManagedLedger 的 entry cache）和预读。追尾读命中率高（刚写入的在缓存），历史读走 BK——BK 自身也有读缓存 +  ledger 分散多 Bookie 并行读。对比 Kafka 的 page cache + sendfile 零拷贝：Kafka 读路径更短（本地），Pulsar 读路径多一跳但可并行分散。**存算分离用网络换弹性，缓存机制补偿读性能**。
