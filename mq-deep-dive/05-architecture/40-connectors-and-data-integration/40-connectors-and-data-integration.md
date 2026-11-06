# 40：连接器——如何以 MQ 为核心搭建数据集成架构？

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

**连接器（Connector）**：把数据从源端搬到目标端的组件。MQ 连接器的特殊之处：**其中一端一定是 MQ**。两类：**Source**（数据源 → MQ）/ **Sink**（MQ → 下游引擎）。三家实现：Kafka Connector、RocketMQ Connector、Pulsar IO。

**连接器 vs 典型数据集成组件（Flink CDC/DataX/SeaTunnel）**：核心差别在**是否经 MQ 缓存**：
- 典型数据集成：源 → 直连下游，一对一，链路短
- MQ 连接器：源 → MQ 缓存 → 下游。**看似多存一份浪费，但一对多场景订阅一次源即可多次分发**（典型组件要多次订阅压垮源端）
- **选型结论：一对一用典型集成，一对多用 MQ 连接器**

**连接器架构 = 分布式任务调度平台（Runtime）+ Source/Sink 连接器 + Transforms**。

**Runtime（分布式任务调度平台）**：
- 概念：**Connector**（逻辑概念，一个订阅任务）→ **Task**（执行实体，Runtime 调度的最小单位）；一个 Connector 拆多个 Task 并行
- 四功能：**HTTP API**（增删改查/启停/扩缩 Connector）、**元数据存储**（集群/Connector/Task 状态——可存内置 Topic/ZK/etcd）、**调度模块**（拆分、调度、启停、扩缩容）、**任务执行节点**（Worker，跑 Task 线程）
- 集群启动：节点发现 + 元数据 + 主节点 + 探活（15/16 讲的通用框架）+ 内置 HTTP Server 暴露管控
- 任务运行流程（订阅 MySQL Binlog 例）：POST 创建 Connector → 存元数据（源/目标地址、任务数）→ 按任务数在多节点建 Task → Task 订阅 Binlog 处理写入 MQ → **节点异常时 Task 调度迁移** → 扩缩 Task 时 Runtime 通知执行节点增删

**Source/Sink 连接器实现**（插件化）：三段式 **Read（主动读：TCP 拉 Binlog、HTTP API 拉钉钉）→ Local Transforms（格式/类型转换）→ Write（下游 SDK 写入）**。代码骨架：`Connector{start/stop}` + `Task{start/execute/stop}`，execute 是核心（读 → transforms → 写）。打包提交 Runtime → 反射加载运行。**任务数与分区数一致时性能最佳**（Sink 场景）。

**Transforms（配置化数据清洗）**：把常见转换内置成 Runtime 的算子（add_field/replace/rename/uppercase/remove_field 等基础操作）——避免每个插件重复写清洗逻辑。按配置顺序链式执行。实现同样是 Interface + 反射。

**Kafka Connector 实证**：
- 架构：Cluster（Runtime）/ Connector / Task / **Worker** / Transform / **Config & Status Storage（存 Kafka 内置 Topic）**
- Source（外部 → Kafka）/ Sink（Kafka → 目标）各可拆多 Task 并行
- **RESTful API**：`POST /connectors` 创建、`GET /connectors` 列表等
- 示例（File→Topic）：`connector.class=FileStreamSource` + `tasks.max=1` + transforms 链（HoistField 加 key、InsertField 加固定字段）——输入 `"foo"` → 输出 `{"line":"foo","data_source":"test-file-source"}`

**思考题（为什么 MQ 自己开发 Runtime 而不用 Spark/Mesos）**：①**轻量**——连接器任务模型简单（Read→Transform→Write 的管道），Spark 是通用计算引擎，杀鸡用牛刀且部署重；②**语义内建**——位点管理、重试、与 MQ 分区/位点模型的对齐，通用平台没有；③**运维内聚**——MQ 用户不想为搬数据再养一套调度系统。**专用 Runtime 是为数据集成语义定制的最小调度器**。

**上节闭环（事件源存储模型）**：混存一个 Topic 时，大事件源积压拖累其他源且无法根治 → **每个事件源独立 Topic**（量小单分区，量大扩分区）；积压时扩分区 + 调消费模型。缓存层再按容量/客户级别拆集群。

---

## 二、设计问答（Q&A）

### Q1：MQ 连接器和 Flink CDC/DataX 的本质区别？

是否经 MQ 缓存。直连组件：源 → 下游，适合**一对一**（链路短、少一跳）。MQ 连接器：源 → MQ → 下游，适合**一对多**（订阅一次源，MQ 缓存分发多下游）+ 削峰 + 可回溯重放。**多存一份数据换来分发能力和源端保护**。

### Q2：Connector 和 Task 的关系？

Connector 是**逻辑订阅任务**（配置：源、目标、任务数、Transforms 链）；Task 是**执行实体**（线程，跑在 Worker 上）。一个 Connector 拆 N 个 Task 并行（Sink 场景通常 N = 分区数）。Runtime 负责 Task 的创建、分布、迁移、启停。

### Q3：Transforms 为什么要内置到 Runtime 而不是插件自己写？

数据清洗是**高频共性需求**（改字段名/加字段/过滤/格式转换）。每个插件重写 = 重复劳动 + 不一致。内置成配置化算子链（`transforms=MakeMap,InsertSource`），插件只写 Read/Write 差异逻辑。**把共性收进平台，把个性留给插件**。

### Q4：为什么主流 MQ 都自研 Runtime 而不用 Spark/Mesos？（思考题）

连接器任务模型是固定的"读-转-写"管道，要的是轻量调度 + **与 MQ 语义深度对齐**（位点管理、重试、分区对应、事务）。Spark/Mesos 是通用计算调度：部署重、语义通用（没有 MQ 位点概念）、杀鸡用牛刀。自研 Runtime 是**为数据集成语义定制的最小调度器**——这也解释 Kafka Connect 元数据存内置 Topic（自举，见 27 讲）。

### Q5：连接器的位点怎么管？（进阶）

Source 要记录"读到哪了"（Binlog 位点、文件 offset），存元数据存储（内置 Topic）。故障迁移时新 Task 从位点续读。Sink 的位点 = MQ 消费 offset。Exactly-once 难（Sink 写入外部系统非事务）——**至少一次 + 目标端幂等/去重**（同 MQ 可靠性铁律）。

---

## 三、实践要点

1. 选型：一对一同步用 CDC/DataX；一对多分发/要缓存重放用 MQ 连接器
2. 性能基线：Sink 任务数 = 分区数；Source 按源端可承受并发定
3. 清洗优先用 Transforms 配置化算子，别在插件里硬编码
4. 元数据存内置 Topic（自举），监控 Task 迁移和位点进度

---

## 四、深入追问

### Q1：连接器集群的"调度"和 MQ 消费组的 rebalance 异同？

同构：都是"N 个执行单元 ↔ M 个任务分片"的分配与再平衡（Task ↔ 分区，Worker ↔ 消费者）。连接器 Runtime 的调度更通用（任务可以不是分区）。Kafka Connect 的 rebalance 直接复用了消费组协调机制。**分布式任务分配的原理是相通的**（09 讲消费分配）。

### Q2：Source 连接器的位点为什么不能存在 Worker 本地？

Worker 是故障域——Task 迁移到新 Worker 要能从位点续读。位点存本地 = 绑定节点，迁移即断点。所以位点必须**外置到共享存储**（内置 Topic/KV）——"执行无状态化，状态集中"（与 Controller 无状态同原则，16 讲）。

### Q3：为什么 Transforms 要链式组合而不是一个大算子？

Unix 哲学：每个算子做一件事（HoistField/InsertField/Filter），组合出任意复杂度。单一大算子要枚举所有需求（爆炸）。链式组合 = **配置即编排**，零代码实现 80% 清洗需求；剩下 20% 才写自定义算子。Fluentd/Logstash 的 filter 链也是同一思想。

### Q4：数据集成场景下 Schema 和连接器怎么配合？（33 讲联动）

Schema 让连接器做到**结构化搬运**：Source 端按 Schema 解析源数据 → 类型映射到 MQ Schema → Sink 端按 Schema 写入下游（类型校验）。没有 Schema：连接器搬的是字节流，类型错配到下游才炸。CDC 场景的 Schema 推断 + 演进兼容（加列、改类型）是连接器最复杂的部分——也是 Debezium 等组件的核心价值。
