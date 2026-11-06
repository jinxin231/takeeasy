# 33：Schema——如何设计实现 Schema 模块？

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

**Schema 是什么**：数据结构定义（模式）。解决的问题：默认 MQ 不校验消息格式，上下游**线下对齐**格式——生产端改格式没通知下游 → 消费解析异常。Schema 保证**全流程按指定格式传递**（生产端规范、Broker 校验、消费端按格式解析）。类比 MySQL 表结构。

**五问拆解**：怎么存？客户端/Broker 怎么拿？什么维度生效？能否变更、变更后各方怎么办？三方怎么用？

**核心组件：Schema Register（注册中心）**——存 Schema + 增删改查接口。运行流程：①建 Schema（客户端自动注册或运营端录入，分配唯一 ID）；②生产/消费启动加载缓存；③生产时 SDK 校验 Schema 存在性（不存在且允许则自动注册，ID 写入消息属性）；④Broker 缓存全量 Schema，按消息里 **SchemaID 校验**；⑤消费端按 SchemaID 解析，不符则丢弃/报错。

**Register 两形态**：
| 形态 | 机制 | 优劣 | 使用者 |
|---|---|---|---|
| **独立部署（主流）** | 独立 Server（Spring Boot + 存储），**一套服务多集群** | 不浪费；但多一个组件 | Kafka/RocketMQ/Pulsar |
| Broker 内核集成 | Broker 提供 CRUD 接口 + 内部 Topic（__schema）存 | 部署简；**内核改 Register 要发版，成本高** | 少用 |

存储选型：避免引入 MySQL（多部署一个组件）→ **存 MQ 自己的 Topic**（分布式可靠存储复用；RocketMQ 用内置 Compact Topic）。

**Schema 格式**：id + **version**（一 ID 多版本，**id+version 才唯一**）+ cluster/tenant/topic + name + **properties**（字段名/类型/约束的 JSON 数组——DDL 核心）。Topic 的 Schema 会演进 → 消息带 SchemaID+Version，各方按版本校验解析。协议要加这两个字段。

**集成**：
- Broker：配 Register 地址 → 启动加载缓存 → 按维度（集群/租户/Topic）开关 → 收消息解析 SchemaID 校验。工作量不大
- 客户端：生产/消费端流程对称（配置→加载→校验/注册→带 ID 发送/按 ID 解析）
- **序列化格式**：Avro/JSON/Protobuf/Thrift（生产消费配置序列化器，Kafka 的 `VALUE_SERIALIZER_CLASS_CONFIG`）

**RocketMQ Schema 实证**：Schema Registry 独立部署（Spring Boot HTTP），存 Compact Topic；**客户端缓存 Schema**（降 Register 访问频率，更新少所以可行）；**Broker 用 RocksDB 做 Schema 缓存**；发送前校验兼容性，不过则拒发。

**思考题观点**：统一通用 Schema Register 有意义（一套服务所有 MQ/DB）——难点在各 MQ 的 Schema 语义和兼容策略差异。

---

## 二、设计问答（Q&A）

### Q1：Schema 解决什么痛点？

上下游数据格式的**契约问题**：生产端改字段/类型没同步下游 → 消费端解析炸。Schema 把线下口头契约变成**线上强校验**：生产规范、Broker 校验拦截、消费按格式解析。本质是**数据治理**——让消息管道里的数据有 DDL。

### Q2：Schema Register 为什么独立部署？

①一套服务多集群（每集群内嵌太浪费）；②Register 是独立服务，内嵌 Broker 后改 Register 要发内核版——成本高。所以主流都独立。存储反用 MQ Topic（避免引入 MySQL）——**自己的存储自己最可靠**。

### Q3：Schema 演进（兼容性）怎么处理？

Topic 的 Schema 会变（加字段/删字段/改类型）。机制：id+version 多版本共存；消息带版本；Broker 校验用**兼容性策略**（向后兼容：新读旧；向前：旧读新；全兼容）。Avro/Protobuf 的字段编号和默认值机制是兼容性的基础（加字段有默认值，老代码不炸）。

### Q4：为什么客户端要缓存 Schema？

Register 交互频繁会拖垮性能和稳定性。Schema **更新频率低**（DDL 不常变）→ 缓存安全。失效策略：校验失败时回源刷新。同理 Broker 用 RocksDB 缓存——**低频元数据 + 本地缓存 = 性能和稳定双赢**。

### Q5：MQ Schema 和 DB Schema 的异同？

同：都是 DDL（字段/类型/约束）。异：DB Schema 是**存储的物理结构**（改要 DDL 迁移数据）；MQ Schema 是**消息的传输契约**（多版本共存于管道，不改存储）。DB 改表是动过去，MQ 改 Schema 只管将来。

---

## 三、实践要点

1. 上 Schema 的时机：多团队协作 + 数据跨系统流转（数据集成/CDC 管道）——单团队紧耦合服务 ROI 低
2. 兼容性策略默认**向后兼容**（新消费者读旧数据），升级消费者先行
3. 序列化选型：性能选 Protobuf/Avro，可读性调试选 JSON

---

## 四、深入追问

### Q1：统一通用 Schema Register 有意义吗？（思考题）

有——数据集成时代（CDC、流计算、多 MQ 并存）需要统一的"数据契约中心"。好处：一处定义多处用、跨系统兼容性检查、数据血缘。难点：①各 MQ Schema 语义差异（Pulsar 内建 vs Kafka 外挂）；②兼容策略差异；③性能（统一服务要扛所有集群的校验压力）。Confluent Schema Registry 已是 Kafka 生态事实标准，跨 MQ 统一还没人做成。

### Q2：Schema 校验在 Broker 会不会成为性能瓶颈？

校验 = 解析消息体比对结构——CPU 开销与消息大小成正比。缓解：①抽样校验/灰度开启；②Schema 简单时校验很快（字段数少）；③高吞吐集群可只在**生产端 SDK 校验**（Broker 信任客户端）。这是"Broker 校验"与"客户端校验"的信任边界问题——金融级全校验，互联网高吞吐可只在客户端。

### Q3：Schema 和消息压缩/batch 的关系？

顺序：先按 Schema 序列化结构体 → 再压缩/攒批。Broker 校验要**先解批解压再校验**（与 20 讲服务端加密的"先还原格式"同问题）。代价：Broker 校验要求它能解开批量压缩——这就是为什么有的实现把校验放客户端/Proxy。

### Q4：Schema Registry 挂了怎么办？

各方缓存兜底：生产/消费/Broker 都有本地缓存，Register 短暂故障不影响存量消息的收发校验（只影响新 Schema 注册）。设计原则同配置中心：**读路径缓存化，写路径（注册）可短暂降级**。
