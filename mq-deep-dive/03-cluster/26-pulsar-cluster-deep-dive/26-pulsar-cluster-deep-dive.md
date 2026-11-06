# 26：从集群角度拆解 Pulsar 的架构设计与实现

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

**集群构建**：Broker 集群构建与 Kafka 一致——ZK 节点注册（`/loadbalance/brokers` 下 IP:Port 子节点）+ 元数据全量存 ZK（管控、Bookie/Ledger、LoadBalance/Bundle、Schema/Stream/Function……目录：admin/bookies/counters/ledgers/loadbalance/managed-ledgers/namespace/pulsar/schemas/stream）。
- **主节点（Master Node ≈ Kafka Controller）**：管元数据与状态（主题/订阅/消费者）、协调节点（选副本、分配分区）。选举：向 ZK 注册争抢，主失效则重新竞争——同 Kafka Controller 抢注机制
- **ZK 瓶颈根源**：分层树结构读需多层检索，数据落硬盘读性能低 → **ZK 必须全量加载内存才有性能** → 单机容量上限 = 集群上限。Pulsar 存算分离 + 计算层弹性要存更多元数据 → ZK 瓶颈放大
- **弱 ZK 化（不是去 ZK）**：**可插拔元数据存储框架**——支持 ZK/**etcd（集群推荐，B 树硬盘读取性能好、容量不受单机限）**/RocksDB（单机，同 RabbitMQ Mnesia 思路）/内存（集成测试）。实现：定义 `MetadataStore` 接口（get/getChildren/exists/put/delete…），各引擎实现接口

**数据可靠性**：
- Pulsar 的 **Topic 没有副本概念**——可靠性靠 **Ledger 多副本**（Qw 总副本数 / Qa 写成功副本数，见 17 笔记；可强一致可最终一致）
- 结构：消息 = Entry，一批 Entry = Ledger，一批 Ledger = 分区；Ledger 按固定长度或写流断开时切新
- **放置策略**（Ledger 分布到哪些 Bookie）：**机架感知 / 区域感知 / 可用区感知**三种 EnsemblePlacementPolicy——跨机架/AZ/Region 容灾
- Bookie 挂 → Quorum 机制副本切换（写前/写后确认保一致完整）

**安全控制**（四手段）：
- TLS：netty-tcnative 实现；Broker 和 **Proxy（自带代理 ≈ LB）**都可开
- **端到端加密（别家少有）**：生产者定期（4h/定量）生成对称会话密钥（AES）加密消息 + 用消费者公钥（RSA）加密会话密钥放消息头；消费者私钥解密会话密钥再解密消息——Broker 存密文。金融/快递场景刚需
- **认证 7 种**：mTLS、JWT、Athenz、Kerberos、OAuth2.0、OIDC、HTTP Basic。可插拔框架（客户端 `AuthenticationDataProvider` + 服务端 `AuthenticationProvider` 两段插件）。**链式认证**：配多个 provider 依次尝试（JWT 不过试 OAuth2）——认证方式平滑升级
- **鉴权**：插件化（AuthorizationProvider / MultiRolesTokenAuthorizationProvider 配 JWT）。**Role Token**（认证后关联 Role，按 Role 鉴权）；角色分**超级用户**（配置写死，全权限）/ 租户管理员（Tenant Admin）。四维度：Broker、Tenant（管控）/ Namespace、Topic（生产消费 + Function）

**可观测性**：Prometheus 指标库（Counter/Gauge/Histogram/Summary）+ 全组件（Broker/BookKeeper/ZK/Proxy/Function/IO）指标 + `/metrics` HTTP 接口——云原生标配姿势，同 RocketMQ 5.0。社区版**无消息轨迹**（商业版有）。

**作者提醒**：Pulsar 社区迭代快，本文内容可能过期——**掌握原理，变化只是变化**。

---

## 二、设计问答（Q&A）

### Q1：Pulsar 为什么"弱 ZK"而不是"去 ZK"（KRaft 路线）？（思考题）

生态现实：Pulsar = Broker + BookKeeper + ZK 三层，BK 也重度依赖 ZK（Ledger 元数据）——去 ZK 要重写两套系统的协调层，工作量巨大且割裂社区。可插拔框架（MetadataStore 接口）是**最小侵入的务实解**：集群化推荐 etcd（B 树硬盘读好、容量不受单机内存限），单机 RocksDB，测试内存。Kafka 去 ZK 是因为 ZK 只有它一家依赖者，可以整体替换。**改造范围取决于依赖面**。

### Q2：ZK 为什么"容量上限 = 单机内存"？

ZK 的 Zab 要求全量数据在内存（DataTree）才能提供低延迟读；落盘的快照/事务日志只为恢复。分层树结构在磁盘上读取要多次随机 IO——所以"全内存"是性能前提。副作用：集群能存的数据总量 = 单节点内存。**所有协调服务的容量瓶颈都是这个内存约束**（etcd 用 bbolt 落盘 + mmap，容量宽松但读慢）。

### Q3：Ledger 放置策略解决什么？

Qw 副本写哪些 Bookie 决定容灾半径：机架感知防同机架断电、区域/AZ 感知防机房级故障。类似 HDFS 的块放置。**写路径的容灾是"放置"出来的，不是"切换"出来的**——故障发生前副本已在安全分布，故障时只需补副本。

### Q4：端到端加密的密钥管理怎么做？

双层密钥：**会话密钥**（AES，对称，快——加密消息本体，定期/定量轮换）+ **消费者公钥**（RSA，非对称，安全——加密会话密钥随消息头携带）。Broker 只见密文和加密后的密钥。这就是 HTTPS/数字信封思想在 MQ 的应用：**对称加密扛性能，非对称加密扛分发**。

### Q5：Pulsar 的认证为什么要"Role Token"中转一层？

认证方式多（7 种）但鉴权要统一——Role 是**认证与鉴权之间的解耦层**：任何认证方式通过后都归一到 Role，鉴权只认 Role。加新认证方式不用动鉴权逻辑。和微服务网关中"认证转身份头"同构。

---

## 三、实践要点

1. 新建 Pulsar 集群：元数据用 etcd（弱 ZK 推荐），Broker/BK/ZK（或 etcd）三层规划
2. 金融级数据：TLS（传输）+ 端到端加密（存储）+ mTLS（认证）三件套
3. Ledger 放置策略按机房拓扑配置（机架/AZ 感知）
4. 读最新文档——Pulsar 版本迭代快，笔记内容要按版本核对

---

## 四、深入追问

### Q1：Pulsar 没有 Topic 副本概念，主从切换在哪发生？

Topic 的高可用 = **Ledger 多副本（存储层，BK 负责）+ Broker 无状态切换（计算层）**。Broker 挂了：Bundle 卸载重新分配，新 Broker 接管（不搬数据——Ledger 在 BK）；Bookie 挂了：Quorum 补副本。两层各自独立容错——这就是存算分离的容灾红利：**计算故障和存储故障解耦**。

### Q2：四款 MQ 集群篇的收敛总结（本文附表精神）

| 维度 | RabbitMQ | RocketMQ | Kafka | Pulsar |
|---|---|---|---|---|
| 元数据 | Mnesia 内嵌 | Broker+NameServer 轻量 | ZK → KRaft | ZK → 可插拔（etcd） |
| 副本一致性 | 镜像 → Quorum(Raft) | M/S 双模式 → DLedger(Raft) → Controller | ISR（拉取+动态集合） | BK Ledger Qw/Qa |
| 认证 | 用户密码+链式 | AK/SK 明文 | SASL 五机制 | 7 种+链式+Role |
| 鉴权 | Vhost/Exchange | Topic/Group | 6 资源×13 权限 | 4 维度+租户 |
| 指标 | Prometheus | 自研→OTel | Yammer+JMX | Prometheus 原生 |
| 轨迹 | Firehose 插件 | **SDK 全链路内建（最佳）** | 无（要改内核） | 商业版 |

**共性收敛**：元数据自研化（KRaft/可插拔）、一致性可配置化、观测 Prometheus/OTel 化、认证插件化——**四家在答案上殊途同归**。

### Q3：Pulsar 的商业化版本轨迹为什么存在而社区版没有？

轨迹是**企业刚需但社区优先级低**的功能（排障/审计/合规付费意愿高）。商业版（StreamNative 等）把它作为差异化卖点。这揭示开源商业化的常见模式：**核心引擎开源，企业级运维能力收费**（轨迹、审计、多活）。

### Q4：Bundle 机制在集群视角怎么理解？

Bundle = Topic/分区到 Broker 的**间接映射层**：Topic → 哈希到 Bundle → Bundle 分配到 Broker。Broker 扩缩容时只迁移 Bundle 的"服务权"（元数据变更），不搬数据（数据在 BK）。没有 Bundle，Topic 直接绑 Broker，扩容就要搬分区。**间接层是弹性的通用手段**（对比：Redis Cluster 的槽、分库分表的逻辑表）。
