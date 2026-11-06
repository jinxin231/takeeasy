# 21：可观测性——如何设计实现一个好用的分布式监控体系？

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

可观测性 = **Metrics + Logs + Traces**（OpenTelemetry 统一规范——定位类似 MQ 界的 AMQP/OpenMessaging）。本讲：指标 + 日志。

**指标维度**：单机三层（OS：CPU/内存/网卡/硬盘；语言虚拟机：GC/线程池/堆；**应用进程**：各阶段耗时、接口请求数、句柄数——最重点）+ 集群维度（Topic/分区/节点数量、Controller 负载）。

**MQ 通用五维指标**：集群、节点、Topic、分区、消费分组：
- **集群**：资源**数量**类指标——Topic/分区过多会拖垮各家（RabbitMQ 全量同步 Queue、Kafka Controller 压力、Pulsar ZK 压力）
- **节点**：吞吐、耗时、消息数、接口请求数/错误码/耗时、GC 次数、TCP 连接数——排查顺序：节点耗时 → GC/接口/连接 → IaaS 层
- **Topic/分区**：吞吐、条数、生产消费耗时——定位特定主题异常
- **消费分组**：消费速度、**堆积数**——消费异常的入口指标
- 各家有独特指标：Kafka Controller/协调器、Pulsar Ledger、RabbitMQ Exchange——需懂架构才能读懂

**指标类型与指标库**（需求实例：消息数量=累加、流量=累加、耗时=分布、连接数=可增可减、启动状态=瞬时）：

| 库 | 类型 | 使用者 |
|---|---|---|
| Java Metrics | Gauge/Counter/Meter/Histogram/Timer（Timer=Meter+Histogram） | Kafka（自定义 KafkaMetrics） |
| **Prometheus** | Gauge/Counter（只增）/Histogram/Summary | RabbitMQ、Pulsar、RocketMQ 4.x |
| OpenTelemetry | Counter/UpDownCounter（+异步 Observer 版）/Histogram/GaugeObserver | RocketMQ 5.0+ |

**指标暴露四方案**：
| 方案 | 机制 | 优劣 |
|---|---|---|
| 自定义 TCP/HTTP | 私有协议/HTTP Server | TCP 不便集成、改协议成本高；HTTP 数据大有瓶颈 |
| JMX | Java 标准代理服务 | 仅 Java、私有协议，渐淘汰（Kafka 早期） |
| **Prometheus /metrics** | 内嵌 HTTP Server 或**外挂 Exporter**（老组件不改内核：拉旧接口转格式再暴露） | **中小规模主流**；几万节点秒级采集有瓶颈 → 大厂自研分布式采集 |
| OpenTelemetry | **Push 模型**上报 Collector（其余皆 Pull） | 无厂商绑定，长期推荐新组件用 |

大规模集群：Prometheus 性能不足 → 自研分布式指标采集（Pull + 汇总计算）。

**日志四原则**（作者运营经验）：
1. **内容完整**——带齐排查所需信息（参考 OpenTelemetry Logs：事件、ID、级别、内容、额外信息 + 来源/作用域）
2. **该打打该不打不打**——关键节点都打（请求收发、进出延时/死信队列），Trace/Debug 承载细节
3. **级别与保留策略可动态调整**——平时 Info，出事动态调低级别；异常时日志爆量会滚没，保留策略也要能动态调
4. **分类分文件**——不同模块/功能独立文件（参考 Kafka 日志配置），防穿插干扰

---

## 二、设计问答（Q&A）

### Q1：MQ 监控体系的指标怎么分层？

单机三层（OS/JVM/应用进程，重点应用进程）× 集群一层；业务视角五维（集群/节点/Topic/分区/消费分组）。排查套路：业务报障 → 先看分组堆积和速度 → 再看 Topic/分区耗时 → 再看节点耗时/GC/连接 → 最后看 IaaS。从下往上定位，从上往下感知。

### Q2：Counter/Gauge/Histogram 怎么选？

只增累计（消息数、流量）→ Counter；可增可减瞬态量（连接数、线程池余量）→ Gauge；**分布类（耗时、大小）→ Histogram/Summary**（要 TP99 就别用平均值——平均掩盖长尾）；频率 → Meter。

### Q3：Prometheus 暴露和 OTel 上报的本质区别？

**Pull vs Push**。Pull（Prometheus）：监控系统定时来拉，指标生命周期由采集器控制（目标发现、频率、保留），适合固定基础设施。Push（OTel Collector）：服务主动上报，天然适配**短生命周期/弹性实例**（Serverless、K8s Job）——实例没了也能收到它最后的数据。云原生时代 Push 是方向。

### Q4：老组件怎么低成本接入 Prometheus？

外挂 **Exporter**：不动内核 Metrics 模块，单独组件拉老接口（JMX/TCP）→ 转 Prometheus 格式 → 暴露 /metrics。Kafka、RocketMQ 4.x 都是这路子。**重构内核不如加个适配器**——成熟的工程折中。

### Q5：日志"出问题找不到"的两个原因怎么治？

太少 → 内容完整 + 关键节点都埋点（参考 OTel Logs 规范）；太多 → ①分级（Trace/Debug 承载细节，平时 Info）②**动态调级别**（免重启）③分文件防穿插 ④保留策略可动态调（异常爆量防滚没）。

---

## 三、实践要点

1. 新组件指标模块：Prometheus 起步（生态成熟），长期向 OTel 迁移
2. MQ 告警最小集：消费 lag、生产/消费耗时 TP99、节点 GC、磁盘水位、连接数、rebalance 次数
3. 日志系统验收：不重启能否调级别？异常风暴时关键日志还在不在？两个"是"才算合格

---

## 四、深入追问

### Q1：思考题——怎么给现有服务做透明监控（无侵入埋点）？

四层路径：①**JavaAgent/字节码增强**（接口耗时、调用链全自动埋点，SkyWalking/Pinpoint 方式）；②**AOP/拦截器**（框架层埋点，Spring Interceptor、Dubbo Filter）；③**JMX/内置 Metrics** 直读（JVM、连接池自有指标）；④**日志采集**（Filebeat 采日志 → 解析成指标）。优先级：能 Agent 不手写，能框架层不业务层——**透明监控的核心是埋点与业务代码解耦**。

### Q2：Prometheus 在大规模下瓶颈在哪？

①拉模式：N 万目标 × 15s 间隔，采集器自身 CPU/网络扛不住 → 要分片/联邦；②**基数爆炸**（高基数标签如 user_id 当 label，时间序列数 = 指标数 × 标签组合数，内存爆炸）；③单机存储。应对：联邦集群、VictoriaMetrics/Thanos、标签纪律（禁止无界基数标签）。这也是大厂自研分布式采集的原因。

### Q3：为什么耗时指标不能用平均值？

平均值掩盖分布：TP50=10ms 平均=50ms 时可能 TP99=2s——长尾请求（恰恰是出问题的那些）被平均掉了。MQ 场景堆积时少量请求延迟飙高，平均值只轻微上升，Histogram 的 TP99 立即报警。**分布类指标必配分位数**。

### Q4：监控自身的高可用怎么做？（元监控问题）

监控是故障时最需要的东西，但它自己也可能挂：①监控系统独立部署（不和业务集群共命运）；②采集链路冗余（Push + Pull 双通道关键指标）；③**对监控系统本身的监控**（采集成功率、延迟——meta-monitoring）；④降级预案：监控挂了时的手工排查手册。别让"监控不可用时发生故障"成为盲区。
