# 25：从集群角度拆解 Kafka 的架构设计与实现

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

集群构建（ZK/KRaft，见 16 笔记）不重复。本讲核心：**ISR 协议** + 安全 + 可观测。

**ISR 协议**（为流场景吞吐而生的一致性协议变种），三大思想：

**① 副本拉取（性能优先）**：Raft/多数派是"Leader 收到立即分发"（QPS 大时分发交互爆炸）。ISR 让 **Follower 批量拉取**：Leader 收 1000 次数据，推送方案要 2000 次交互（2 个 Follower），拉取方案每批 50 条只要 20 次。一致性确认的等待用**时间轮**（Leader 本地队列等 Follower 同步完成再回复客户端）。

**② 动态维护可用副本集合（ISR）**：副本异常（节点宕机——心跳探活；或**拉取进度跟不上**——按条数判断不准，改为**按落后时间**，可配置）→ 踢出 ISR → 强一致的"全部"按 ISR 内成员算，**有问题的节点不再拖累强一致**。[A,B,C] → [A,B]，两副本成功即返回。

**③ Leader 切换与数据截断**：
- 强一致集群切换简单无丢失（RabbitMQ 镜像队列、RocketMQ 同步双写）但性能差
- ISR 是**最终一致**，极端场景：A、B 写入 100 条成功返回 → A、B 同时挂 → C（只有 90 条）能当 Leader 则**数据截断**（A、B 回来后 90~100 冲突丢弃 = 丢数据）；C 不能当 Leader 则**服务不可用**（不丢数据）——Kafka **Topic 维度可配**两种行为（`unclean.leader.election.enable`）
- 所以 ISR 协议的大量工作在代码层面处理 Leader 切换和数据截断

**安全**：
- TLS/SSL 加密（同 RocketMQ 标准用法）
- **认证五种**（SASL 框架下）：GSSAPI（对接 Kerberos）、PLAIN（明文，配置在 Broker 文件不可动态改）、**SCRAM**（存 ZK 可动态修改）、OAUTHBEARER（OAuth，第三方不碰用户账号）、Delegation Token（轻量 Token，存 ZK）
- **鉴权极细**：6 种资源（Topic/Group/Cluster/TransactionalId/DelegationToken/User）× **13 种权限**（Read/Write/Create/Delete/Alter/Describe/ClusterAction/DescribeConfigs/AlterConfigs/IdempotentWrite/CreateTokens/DescribeTokens/All）+ 来源 IP 限制。存 ZK `/kafka-acl`，Broker 启动全量加载内存、ZK 变更通知重载。工具：`kafka-acls.sh`

**可观测性**：
- 指标定义：**Yammer Metrics**（Histogram/Meters/Gauges）
- 暴露：**只有 JMX**——Broker 开 JMX Server，三条监控路线：JMX Client 自采 / **JMX Exporter** → Prometheus / **Kafka Exporter**（走 Kafka 协议拿消费进度、Topic 信息）→ Prometheus
- 日志：Log4j，分模块分文件（Controller/Coordinator/GC…）
- **无消息轨迹**：流场景消息量太大（一条消息至少 3 条轨迹，成本收益比低）；采样又不符合"重要消息全要轨迹"的诉求。要支持得改内核 + SDK（技术不难）

---

## 二、设计问答（Q&A）

### Q1：ISR 协议为什么适合流场景？

吞吐优先：Follower 批量拉取代 Leader 推送（交互次数数量级下降）；ISR 动态集合把故障节点移出"强一致等待集"（坏节点不拖累集群）；acks 三档客户端自选一致性级别。代价：最终一致底色 → Leader 切换有截断/丢失窗口，要靠 HW/Leader Epoch/unclean 配置精细管理。

### Q2：ISR 怎么判定副本"跟不上"？

早期按条数（不同 Topic 流量差异大，判断不准）；后改**按时间**（`replica.lag.time.max.ms`，Follower 落后 Leader 多久）——时间是流量无关的度量。节点宕机另由心跳探活判定。

### Q3：unclean 选举开关的语义？

`unclean.leader.election.enable=true`：ISR 全空时从 OSR 选主——**保可用、可能丢数据**（流场景日志可接受）；false：**保数据、分区不可用**直到 ISR 恢复（业务消息场景）。配合 acks=all + min.insync.replicas=2 才是完整的"不丢"配置（同 3.mq 26 笔记）。

### Q4：Kafka 为什么没有消息轨迹？

流场景消息量巨大（轨迹 3 倍放大成本爆炸）且轨迹价值低（日志丢了重采）；业务消息要轨迹但 Kafka 不想为此付内核成本。采样也不成立：重要消息要全量轨迹、不重要的不用采。要用 Kafka 当业务总线的公司可自改内核 + SDK 拦截器实现（不难）。

### Q5：SCRAM vs PLAIN 的实际差别？

PLAIN：账号密码写 Broker 配置文件，改密码要重启——运营噩梦。SCRAM：凭证存 ZK 动态增删改，且挑战-应答机制**密码不上网络**（防窃听）+ 加盐迭代哈希存储（防库泄）。生产只用 SCRAM 或更强。

---

## 三、实践要点

1. 可靠性基线配置：acks=all + min.insync.replicas=2 + 3 副本 + unclean 选举按业务选
2. ISR 监控：`UnderReplicatedPartitions`、replica lag（按时间）、Controller 切换频率
3. ACL 治理：按"资源 × 权限"矩阵定期审计，生产 Topic 写权限最小化
4. 监控搭建：JMX Exporter + Prometheus 是标配；消费进度靠 Kafka Exporter

---

## 四、深入追问

### Q1：ISR 和 Raft 的本质区别是什么？

Raft：**固定多数派**（N/2+1）+ 日志匹配 + 任期。ISR：**动态集合**（健康副本才计入"全部"）+ 拉取模型 + 客户端可选一致性级别。ISR 放弃强一致换吞吐与故障弹性——Raft 故障节点会阻塞写入，ISR 把它踢出去继续跑。**Raft 保正确性，ISR 保吞吐**。

### Q2：思考题——Leader 主动分发时，节点异常怎么设计退避？

①**指数退避重试**（100ms→200ms→…→上限）避免故障节点被高频无效调用；②**熔断器**：连续 N 次失败断开一段时间，半开试探恢复；③把节点从分发列表临时摘除（类似 ISR 剔除），恢复后回补数据追平再回列表；④退避期间异步落盘/内存队列防数据滞留丢失。核心：**失败要变贵（退避），别让故障节点消耗健康路径的资源**。

### Q3：时间轮在 Kafka 里还用在哪？

①生产端 acks 等待确认的超时；②延迟操作（DelayedProduce/DelayedFetch——拉取攒批的 max.wait）；③Purgatory 炼狱队列统一承载延迟操作。时间轮的优势：**O(1) 插入删除**，海量定时任务（百万级 in-flight 请求）下远胜堆/队列（O(log n)）。Netty HashedWheelTimer 同款。

### Q4：Kafka ACL 为什么存 ZK 而不是 Broker 本地？

ZK 是集群级单一事实源：ACL 一处改、全集群 Hook 通知生效；Broker 本地存（RocketMQ 方式）则多 Broker 同步靠文件监听各自加载——一致性和即时性差。代价：ZK 多一类读写（ACL 变更低频，可接受）。**配置类数据的存储选型看变更频率**：低频高一致 → ZK/KRaft；高频大流量 → Broker 本地缓存 + 推送。
