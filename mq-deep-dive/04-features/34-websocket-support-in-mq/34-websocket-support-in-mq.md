# 34：WebSocket——如何在消息队列内核中支持 WebSocket？

> 来源：极客时间《深入拆解消息队列47讲》许文强
> 整理日期：2026-11-06

---

## 一、内容总结

**WebSocket 是什么**：基于 TCP 的应用层标准公有协议，**可在浏览器使用的双向通信**。关键记忆点：**浏览器可用的全双工**。

**通信方式基础**：单工（广播，只一方发）vs 双工（电话，双方同时收发）。HTTP 是单向（服务端不能主动给浏览器发）；TCP/IP 是双向。**WebSocket 五特点**：持久化连接（少握手）、全双工、低带宽开销（紧凑二进制帧）、与 HTTP 兼容（同 80/443 端口）、允许跨域（配置后）。场景：IM、在线游戏、实时行情、IoT、协作工具。

**MQ 为什么需要 WebSocket**：08 讲的 Push 模型要消费者起 Server（有成本）——**WebSocket 让 Broker 直接推数据给客户端，客户端不用维护 Server**，开发量大降。案例：IoT 传感器 → MQTT Broker → WebSocket → 浏览器监控面板（双向：浏览器也可经 WebSocket 发消息回 IoT 终端）。

**内核支持四步**：
1. **定功能**：WebSocket 用于数据流（生产/消费/位点）
2. **协议设计**：WebSocket 兼容 HTTP → **协议设计 ≈ HTTP API 设计**（URI + JSON Body）。生产示例：`wss://host/send/ns1/tp1`，Body 带 key/value/properties；返回 result/messageID/errorMsg
3. **WebSocket Server**：各语言有库。Netty 示例：pipeline 加 HttpServerCodec（握手）+ HttpObjectAggregator + WebSocket 压缩 + 自定义 Handler；**writeAndFlush 回写 Channel = 主动推送的关键**
4. **主动推送**：数据写 Topic → Broker 经 WebSocket 直接推客户端；内核维护**异步线程回写**（回顾 08 讲 Push 模型）

**Pulsar 实现**：内核启动 or 独立组件部署（同一份代码）；基于 **Jetty WebSocket**；支持生产/消费/读位点三功能。URI 含版本/接口/租户/命名空间/Topic；Body 带 payload（**Base64 编码**）+ properties + context。客户端流程：建 URI → 连接 → 组参 → 发送 → 处理返回 → 关连接。

**业界现状**：只有 Pulsar 和 RabbitMQ 支持。其实**可自研 WebSocket Server 消费 MQ 再推客户端**（成本不高）——所以 WebSocket 不是 MQ 核心功能，属场景增强。

**思考题要点**：WebSocket 握手是 HTTP（Upgrade 头）→ 升级后走 TCP 全双工帧——"基于 HTTP"指**握手阶段走 HTTP**，数据交互阶段是独立帧协议。

---

## 二、设计问答（Q&A）

### Q1：MQ 为什么要支持 WebSocket？

双向通信：Broker 可主动推送，**客户端不用起 Server**（真 Push 的轻量替代）。场景：浏览器实时监控面板、告警推送、IM。浏览器无法用裸 TCP——WebSocket 是浏览器能用的唯一双向协议。

### Q2：WebSocket 与 HTTP 的关系？（思考题）

**握手基于 HTTP**：客户端发 HTTP 请求带 `Upgrade: websocket` 头，服务端 101 切换协议。之后脱离 HTTP：全双工帧通信（自己的帧格式：FIN/opcode/mask/payload）。所以"基于 HTTP"仅指建立连接阶段——**用 HTTP 敲门，用 TCP 长聊**。

### Q3：内核支持 vs Proxy 模式？

实现差别不大（WebSocket Server 代码同一份）。内核：少一跳、低延迟、Broker 负担重（长连接维护）。Proxy：独立部署可伸缩、隔离 Broker。大规模连接选 Proxy（连接管理专业化，同 20 讲限流的网关层）。

### Q4：WebSocket 推送的背压怎么办？

客户端慢 → Broker 推送堆积 → 内存涨。解法：①Channel 水位线（Netty `writeBufferHighWaterMark`，超线暂停推送）；②按客户端 ACK 节奏推（类 basicQos 预取计数）；③慢客户端断开重连转 Pull。真 Push 的宿命问题：**服务端必须感知客户端消费能力**。

### Q5：WebSocket 的消费位点怎么管？

Pulsar 模式：WebSocket 消费接口返回消息 + 提供"读取位点/确认"接口（HTTP 式调用）。本质是**把消费协议翻译成 HTTP 语义**：receive = 拉/推消息、ack = 提交位点、位点查询 = 读接口。客户端无状态化（重连后续传位点）——比私有协议的瘦客户端还瘦。

---

## 三、实践要点

1. 监控大屏/告警场景：别自研轮询——MQ + WebSocket（或 Kafka + 自建 WS 网关）是标准答案
2. WebSocket 连接数大时走 Proxy/网关层，别让 Broker 背海量长连接
3. 生产环境必开 wss（TLS），ws 明文只在内网

---

## 四、深入追问

### Q1：WebSocket vs MQTT vs 私有 TCP 长连接，怎么选？

MQTT：IoT 专用（极轻协议头、QoS 分级、遗嘱消息），浏览器不支持。WebSocket：浏览器唯一选项，通用实时通信。私有 TCP：性能极致但要客户端库（App/服务端）。**看客户端形态**：浏览器 → WebSocket；IoT 设备 → MQTT；自有 App/服务间 → 私有 TCP。

### Q2：WebSocket 和 Server-Sent Events（SSE）？

SSE：HTTP 单向推送（服务端→客户端），更轻（自动重连、文本流）。WebSocket 双向。监控面板**只读场景 SSE 更简单**；要双向（浏览器也要发）才 WebSocket。很多"WebSocket 项目"其实 SSE 就够。

### Q3：WebSocket 长连接的运维成本在哪？

①连接保活（ping/pong 心跳防 NAT 超时和假死）；②LB/网关的超时配置（很多中间层默认 60s 无流量断连——要调）；③扩缩容时的连接迁移（graceful：stop accepting + 等排空）；④内存（每连接缓冲区）。百万连接级要专职接入层（Go 的 push 网关/Netty 集群）。

### Q4：为什么"自研 WS Server + 消费 MQ"常常比内核支持更好？

关注点分离：WS 网关管海量连接和推送（连接型负载），MQ 管存储分发（IO 型负载）——两者的扩缩容节奏和资源画像完全不同。内核支持把两种负载耦合在 Broker 上。**接入层与存储层分离**是大型实时系统的通用结构（IM 架构也是：接入层 → 逻辑层 → 存储层）。
