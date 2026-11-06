# 24：从集群角度拆解 RocketMQ 的架构设计与实现

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

**集群构建纠偏**：元数据**实际存在 Broker**（本地持久化文件），NameServer 只是**内存缓存 + 分发**（无持久化、节点间无通信无一致性——各节点数据靠 Broker 上报维护）。节点发现 = Broker 向 NameServer 集群注册上报；探活 = Broker 与 NameServer 定期心跳。

**三种部署模式（演进主线，理解可靠性的前提）**：

| 模式 | 机制 | 解决问题 | 代价 |
|---|---|---|---|
| **Master/Slave** | 经典主从（Master 写、Slave 备份读）；**早期不支持主从切换**——Master 挂则写入/Offset/事务停摆 | 应对：**Topic 在所有 Master 上建全量分区**（一个 Master 挂不影响新写入） | 不保序；挂节点未消费数据不可消费；Topic/分区数放大（每节点全量） |
| **DLedger** | **Raft Commitlog** 替代原 Commitlog——存储层获得选主切换能力 | 自动主从切换 | 最少 3 节点；**不兼容原生存储复制**；维护难 |
| **Controller（DLedger Controller）** | Raft 选主组件**上移为独立层**，与存储解耦（可嵌 NameServer 或独立部署） | Master/Slave 部署也能获得容灾切换 | 多一个组件 |

Controller 四部件：Active Controller（Raft 选出的主）/ **SyncStateSet**（允许选主的副本集合 ≈ Kafka ISR）/ Elect Master / Replication。**与 Kafka 架构神似**（分区粒度副本集 + Controller 选主 + 复制模块）。

**数据可靠性（按模式）**：
- Master/Slave：**异步复制**（性能高、最终一致）/ **同步双写**（性能低、强一致）——性能：异步 > 同步；可靠性反之
- DLedger：Raft **多数原则**（3 副本写 2 成功），同步写入，最终一致
- Controller：`inSyncReplicas=N` 自定义成功副本数（=Pulsar Qa 思路），`allAckInSyncStateSet` 全写成功（强一致）。同步写入；inSyncReplicas=最终一致，allAck=强一致

**安全**：
- TLS：Broker 标准 Java TLS 集成（证书/密钥/端口配置，客户端对应配置）
- 认证：**仅明文用户名/密码**（AccessKey/SecretKey，可动态申请）；管理员账户全权限、普通账户需授权
- 鉴权：Topic/Group 两资源，**DENY/ANY/PUB/SUB** 四权限 + IP 白名单；mqadmin 动态管理。**凭证存 Broker 文件**（非中央服务，监听文件变化重载——与元数据存储思路一致）
- 校验顺序：全局 IP 白名单 → 用户 IP 白名单 → 签名 → 资源权限（特殊操作如 UPDATE_AND_CREATE_TOPIC 仅 Admin；无显式配置用默认权限）

**可观测性**：
- **指标**：5.0 前自研 BrokerStatsManager（内存 Map 记指标）+ Export（Remoting Pull → HTTP /Metrics）——缺点：不规范、RPC 压 Broker、改指标要改 Admin 接口。**5.0 重构为 OpenTelemetry 规范**：Counter/Gauge/Histogram + Prometheus 命名规范，暴露三模式：**Pull（Prometheus /metrics）/ Push（OTel Collector）/ Export 兼容**（Export 改收 Push）
- 日志：Logback + SLF4J，分级/滚动/保留全具备
- **消息轨迹（业界最好）**：**客户端 SDK 内嵌轨迹上报**（生产端 + 消费端）→ 内置/自定义 Topic → Broker 存全链路轨迹；每条消息有唯一 ID，可按 ID 查（mqadmin 或导 ES）

---

## 二、设计问答（Q&A）

### Q1：RocketMQ 为什么演进三种部署模式？（思考题）

业务规模与可靠性要求的递进：Master/Slave（最简，够用但有单点）→ DLedger（要自动容灾，但存储层强绑 Raft 代价大）→ Controller（**选主能力与存储引擎解耦**——Raft 只做选主，Commitlog 保持原生高性能）。演进逻辑：每一步都在"可靠性收益"和"架构侵入性"之间找新平衡。也反映 RocketMQ 从"阿里内部系统"到"Apache 顶级开源"的普适化过程。

### Q2：SyncStateSet 和 Kafka ISR 是不是一个东西？

概念同构：都是"当前与主保持同步、允许选主的副本集合"。差异：ISR 动态进出（lag 超阈值踢出），SyncStateSet 由 Controller 维护且与 `inSyncReplicas` 参数配合决定写成功副本数。**本质都是"副本健康度 + 选举资格"的集合管理**——RocketMQ Controller 模式是向 Kafka ISR 模型的看齐。

### Q3：Master/Slave 模式下"Topic 全量分区建在所有 Master"有什么问题？

①分区数 = 节点数 × 配置分区数（放大效应，元数据和文件句柄膨胀）；②某 Master 挂 → 它上面的未消费数据**不可消费**（Slave 只备份不接管读）；③写入倾斜时单 Master 承压。这是"无切换能力"的代偿设计——用空间换可用性，很糙但有效。DLedger/Controller 就是来消灭这个代偿的。

### Q4：RocketMQ 消息轨迹为什么业界最好？

**全链路覆盖**：别家大多只记 Broker 段，RocketMQ 在**生产端和消费端 SDK 都内置了轨迹上报**（发到内置 Topic）。加上每条消息唯一 ID（客户端生成），实现"消息 ID → 全链路轨迹"的完整查询（mqadmin 直查）。对比 22 讲：RocketMQ 是"轨迹设计要点"的教科书实现。

### Q5：5.0 指标重构为什么选 OpenTelemetry 而不是只兼容 Prometheus？

Prometheus 是**产品**（Pull 模型 + 自家格式），OTel 是**规范**（指标定义 + 传输协议 + Collector 生态）。选规范 = 一次实现同时兼容 Prometheus（Pull）、云厂商监控（Collector Push）、自建系统——长期不被单一产品绑定。这是可观测性建设的趋势判断：**绑定规范，不绑定产品**。

---

## 三、实践要点

1. 部署模式选型：新集群直接 Controller 模式（内嵌 NameServer 最简）；存量 Master/Slave 加 Controller 组件即获容灾切换
2. 可靠性配置矩阵：同步双写（强一致低性能）/ 异步复制（最终一致高性能）/ Controller 模式 inSyncReplicas=2（平衡）
3. ACL 凭证在 Broker 文件——变更靠 mqadmin，注意多 Broker 一致性
4. 轨迹排查三板斧：按消息 ID 查 mqadmin → 导 ES 做查询平台 → 客户端开启轨迹上报开关

---

## 四、深入追问

### Q1：DLedger 模式为什么"维护困难"而失败？

Raft 写进存储层意味着：①Commitlog 的一切优化（PageCache 策略、批量写、冷热分离）都要和 Raft 日志复制耦合调优；②存储层 bug 与共识 bug 纠缠，排查困难；③与原生主从复制不兼容，老集群迁移要重建。**教训：共识协议应该是控制面组件，不该侵入数据面存储格式**——Controller 模式的"上移"正是纠错。

### Q2：Controller 模式 vs Kafka 架构的细微差别？

同：Controller 选主 + 副本集（SyncStateSet ≈ ISR）+ 复制模块。异：①Kafka 元数据在 ZK/KRaft（独立服务/内嵌 Raft），RocketMQ Controller 可嵌 NameServer（更轻）；②Kafka ISR 动态伸缩灵敏（lag 踢出），RocketMQ SyncStateSet 变更经 Controller；③Kafka 分层：Controller 只管选举，RocketMQ Controller 还管元数据分发。**趋同是分布式 MQ 的收敛**。

### Q3：RocketMQ 为什么认证只有明文 AK/SK？够用吗？

够用但有边界：AK/SK 本质是预共享密钥，签名防篡改（HMAC）防重放靠时间戳。缺失：①无凭证轮换机制（硬编码风险）；②无细粒度身份联邦（对接企业 IAM 要自研）；③明文传输必须配 TLS（否则密钥裸奔）。企业级场景要外接网关做认证增强。这反映 RocketMQ 的安全投入优先级低于功能——开源社区补短板的方向。

### Q4：Export 模式的缺陷为什么值得单独记？

它是"指标适配器"反例的教科书：①Remoting 高频 Pull 给 Broker 加压（监控影响被监控对象——海森堡bug）；②指标口径私有，与 Prometheus 生态不通；③改指标要改 Admin 接口（监控与内核耦合）。**监控接入的正确姿势：内核按规范自暴露，而不是外围桥接**。
