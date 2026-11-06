# 12：从基础功能拆解 Kafka 的架构设计与实现

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

**架构**：Producer / Broker / **ZooKeeper** / Consumer。Kafka 与 RocketMQ 架构极像（RocketMQ 早期参考过 Kafka）：分区 ≈ MessageQueue，ZooKeeper ≈ NameServer——但实现细节差异大。**元数据演进**：ZK 有额外维护成本、一致性、分区数规模限制 → RocketMQ 用 NameServer 替代，**Kafka 3.0 用内置 Raft（KRaft）替代 ZK**。

**① 协议**：私有 Kafka Protocol（V0/V1/V2，稳定 V2）。官方不支持 HTTP，商业版用代理补（如 Confluent Kafka Rest）。

**② 网络**：原生 Java NIO + Reactor（多线程多 Selector，见 04 讲）。

**③ 元数据存储**：
- ZK 持久化全量元数据，**Broker 不存集群元数据**，启动连 ZK 读取；ZK 基于 Zab（Leader 写 / Slave 读），自带组网/一致性/持久化/监听
- **巧妙设计：内置 Topic 存元数据**——消费进度（`__consumer_offsets`）、事务状态（`__transaction_state`）、分层存储元数据、KRaft 元数据都存在内置 Topic。用 MQ 自己的存储复制机制管理自己的元数据（自举），值得借鉴

**④ 消息存储**（05 讲"每分区一文件"的标杆）：
- **一个分区一个目录**（TopicName-分区号），目录下 `.log`（消息数据，**分段默认 1G**）+ `.index`（偏移量索引）+ `.timeindex`（时间索引）+ leader-epoch-checkpoint
- 清理：**Topic 维度过期时间 / 分区维度大小**（分区超大小即触发）——比 RocketMQ 的节点维度精细；**延时清理以段为单位**（整段过期才删）
- 性能核心：**每分区顺序读写**；写靠顺序写+写缓存+批量写，读靠批量读+顺序读+热数据读缓存+零拷贝

**⑤ 生产端**：寻址演进——**老版本连 ZK 寻址**（ZK 被客户端打爆）→ **新版本直连 Broker 拿元数据**（Broker 缓存全量元数据，客户端不与 ZK 交互）。写 Topic 走客户端分区分配（**轮询/KeyHash**）；**协议原生 Batch 语义**：本地按分区聚合成批再发，吞吐大增。
- **两种"批量"的本质区别**（亮点）：①协议级 Batch——批里**每条消息各有 offset**，Broker 视角仍是多条，消费端无感；②业务层聚合——**整批就是一条消息只有一个 offset**，消费者要自己解包逐条处理。选型时区分！

**⑥ 消费端**：**只支持 Pull**。消费分组（重平衡 + RangeAssignor/RoundRobin/StickyAssignor/自定义）或**直连分区**（免分配、自管进度）。位点存 `__consumer_offsets` 内置 Topic；自动/手动提交；重启按位点续消费防重复。

**⑦ 管控**：Kafka Protocol over TCP（Broker 封装后转发 ZK，避免客户端直连 ZK 施压）；老命令行残留直连 ZK 操作；命令行底层 = 客户端 SDK。

**Kafka vs RocketMQ 异同**（面试高频）：架构概念同构（都是参考关系），但——存储（分区文件 vs CommitLog 共享）、协议（Batch 语义有无）、元数据（ZK/KRaft vs NameServer）、消费（仅 Pull vs Pull/Push/Pop）、定位（流 vs 业务消息）。

---

## 二、设计问答（Q&A）

### Q1：Kafka 为什么早期用 ZK 寻址后来改 Broker 寻址？

大量客户端直连 ZK 拉元数据会把 ZK 打超载（ZK 是低 QPS 协调服务，不是数据服务）。改由 Broker 缓存全量元数据 + 客户端问 Broker——**元数据的读流量从 ZK 卸载到 Broker 集群**。这是"协调服务不能当数据服务用"的经典教训。

### Q2：内置 Topic 存元数据妙在哪？

元数据（offset、事务状态）获得与业务消息**相同的复制、持久化、读写路径**——不需要额外的一致性组件，运维零新增。本质是"元数据即日志"（log as metadata）——KRaft 把这条路线走到极致，连集群元数据都成了 Raft 复制的日志。

### Q3：协议级 Batch 和业务层聚合一条大消息有什么区别？

协议级 Batch：批内每条消息**各有 offset**，Broker 存成多条，消费端无感知逐条消费——吞吐收益且语义无损。业务聚合：整批一条消息一个 offset，消费者要**感知并解包**——少一次网络交互但消费语义变重、单条重试粒度丢失。RocketMQ 无 Batch 协议，所以只能走业务聚合——这是协议设计的连锁反应。

### Q4：Kafka 清理机制为什么"时间按 Topic、大小按分区"？

时间过期以段为单位整段删，Topic 维度配置符合业务直觉（这个业务的日志留 7 天）；大小限制保护的是**磁盘**——磁盘是按分区文件占用的，所以按分区卡上限（防单个热点分区写爆盘）。配置维度的差异背后是**"业务语义"与"物理资源"的分层**。

### Q5：Kafka 为什么不支持 Push？

流场景吞吐优先：Pull 让客户端按自身节奏批量拉（fetch.min.bytes/max.wait.ms 攒批），Broker 不用维护推送状态和客户端健康度。Push 的背压/连接维护成本与 Kafka 的高吞吐定位冲突。消费及时性靠"长轮询"（hold 请求）已足够——证明**伪实时用拉模型就够，不用真推**。

---

## 三、实践要点

1. 看懂 Kafka 数据目录：`.log/.index/.timeindex` 三件套 + 分段命名（起始 offset）——排查消息存储问题的地图
2. 老系统检查：是否还有客户端直连 ZK 的寻址/管控残留——ZK 超载的隐形炸弹
3. 生产调优组合拳：Batch（linger.ms + batch.size）+ acks + 压缩，三者联动（见 3.mq 30 笔记）

---

## 四、深入追问

### Q1：Kafka 为什么不把索引做成全局哈希索引（像 IndexFile）？

Kafka 的设计假设是**按 offset 顺序消费**（流场景），消息查询不是主路径——offset 索引 + 时间索引两个稀疏索引已够用，且保持每分区自包含（目录独立、迁移简单、删除整段即清）。RocketMQ 需要按 key 查消息（业务消息的对账/重投场景）才要 IndexFile 哈希索引。**索引设计的差异 = 消费语义的差异**。

### Q2：ZK 时代 Kafka 分区数上限的瓶颈在哪？

ZK 的写入走 Leader + 半数确认，元数据写 QPS 低（千级）；而分区数膨胀时，**状态变更（Leader 选举、ISR 变更）次数 ~ 分区数 × 故障频率**——大集群一次抖动产生海量 ZK 写入，恢复时间失控。KRaft 后元数据走内部 Raft 日志（批量、顺序写），单集群百万分区成为可能。

### Q3：每分区一文件 + 分区数很多时，除了写性能还有什么问题？（串联 3.mq 29 笔记）

①FD 占用（每段文件一个 fd × 分区数 × 段数）；②page cache 碎片化（写流分散降低缓存命中）；③**刷盘风暴**（flush 时要遍历所有分区文件）；④rebalance/迁移的数据搬迁量大。所以 Kafka 官方也在推 KIP-405 分层存储来解耦——分区数据归档远端，本地只留热段。

### Q4：Kafka 的事务状态为什么也存内置 Topic？

事务协调器的状态（pending/committed transaction）必须**比 Broker 更长寿**——协调器故障转移时新协调器要能重建事务状态。存 `__transaction_state` Topic = 状态获得分区复制 + 故障转移时重放日志恢复。又是"元数据即日志"：一切需要容错的状态都写成日志，恢复就是重放。
