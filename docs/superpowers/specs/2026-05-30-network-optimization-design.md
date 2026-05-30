# PFRP 网络传输性能优化设计文档

## 背景

PFRP 是一个 Python 编写的多连接 FRP (Fast Reverse Proxy) 客户端/服务器。在 **5000km 跨国链路、300Mbps 理论带宽** 的场景下，当前单条 TCP 流下载速度远低于预期，存在明显的传输瓶颈。

## 目标

- 单流下载速度从当前受限水平提升到接近理论带宽（300Mbps ≈ 37.5MB/s）
- 支持自适应网络质量调整，无需手动调参
- 优化代码风格一致性，提升可维护性

## 当前瓶颈分析

### 瓶颈 1：每次发送都 `await drain()`（最严重）

`DataChannel.send()` 每次写入后都调用 `await self.writer.drain()`，在高延迟链路上（RTT 150-200ms）导致流水线频繁停顿。

### 瓶颈 2：单条流绑定单条通道

`stream_to_channel[stream_id]` 固定绑定后，单用户下载只能利用 1/16 的通道带宽，单条 TCP 拥塞窗口在高延迟网络下增长极慢。

### 瓶颈 3：读写粒度小，无流水线

读取 512KB → 发送 → drain → 再读取，没有预读/批量发送机制。

## 架构设计

在现有 PFRP 架构上叠加两层优化：

```
应用层（保持不变）
  └── 端口扫描、心跳、UDP 转发、服务管理

流量调度层（新增）
  ├── 流量分类器（自适应判断单/多通道）
  ├── 单通道队列（小流量）
  └── 多通道队列（大流量）

传输优化层（改造）
  ├── 批量发送器（去 drain 阻塞）
  ├── 通道质量监控（带宽/延迟自适应）
  └── 序列号重组器（多流合并）

网络层（保持不变）
  └── 16 条 TCP 数据通道 + 1 条控制通道
```

## 帧格式变更

### 新帧格式（兼容单/多通道）

采用统一带序列号的格式，单通道时序列号填 0：

```
┌──────────┬────────────────┬──────────┬──────────┐
│ StreamID │ SequenceNumber │ Length   │ Data     │
│ 4 bytes  │ 8 bytes        │ 4 bytes  │ N bytes  │
│ uint32   │ uint64         │ uint32   │ bytes    │
└──────────┴────────────────┴──────────┴──────────┘
总头部开销：16 字节（原 8 字节 → 新增 8 字节）
```

### 序列号规则

| 模式 | SequenceNumber 值 | 说明 |
|------|-------------------|------|
| 单通道传输 | `0` | 表示无需排序，直接转发 |
| 多通道传输 | `1, 2, 3, ...` | 从 1 开始递增，接收端按 Seq 顺序重组 |

### 控制通道命令扩展

```python
CMD_ENABLE_MULTI_CHANNEL = 10  # 服务器通知客户端启用多通道
CMD_MULTI_CHANNEL_ACK = 11     # 客户端确认
CMD_MULTI_CHANNEL_NACK = 12    # 序列号缺失，请求重传
```

帧载荷：`[StreamID:4]` —— 仅 4 字节

### 兼容性

- 旧版本连接新版本时，序列号始终为 0，退化为单通道模式
- 版本协商在控制通道建立时完成

## 核心组件设计

### 1. BatchSender（自适应批量发送器）

替代 `DataChannel.send()` 中每次 `await drain()` 的阻塞调用。

**核心方法**：

```python
class BatchSender:
    def write(self, data: bytes):
        """非阻塞写入，数据缓冲等待批量 flush。"""

    async def flush(self):
        """强制刷新所有待发送数据。"""

    def _adapt_thresholds(self):
        """
        根据通道带宽自适应调整：
        - 带宽高 → 阈值更大（减少 drain 频率）
        - RTT 高 → 间隔更长（避免 ACK 等待阻塞）

        阈值 = max(64KB, min(2MB, BDP/4))
        BDP = bandwidth(bps) * RTT(s) / 8
        间隔 = RTT / 2，范围 [5ms, 50ms]
        """
```

| 网络状况 | RTT | 带宽 | drain_threshold | drain_interval |
|---------|-----|------|-----------------|----------------|
| 强网络 | 50ms | 300Mbps | ~470KB | 25ms |
| 弱网络 | 200ms | 50Mbps | ~313KB | 50ms |

### 2. FlowClassifier（自适应流量分类器）

决定某条流走单通道还是多通道模式。

**核心方法**：

```python
class FlowClassifier:
    MODE_SINGLE = 1
    MODE_MULTI = 2

    def record_bytes(self, stream_id: int, bytes_count: int, timestamp: float) -> bool:
        """
        记录流传输数据。
        返回 True 表示该流被提升为多通道模式。
        """

    def _adapt_thresholds(self):
        """
        根据可用总带宽自适应：
        - 总带宽高 → 提高阈值（避免小流走多通道）
        - 总带宽低 → 降低阈值（让更多流走多通道聚合带宽）

        rate_threshold = 总带宽的 5%，范围 [2MB/s, 50MB/s]
        bytes_threshold = rate_threshold * 10s，范围 [20MB, 500MB]
        """

    def get_mode(self, stream_id: int) -> int:
        """获取流的当前模式。"""
```

| 总带宽 | rate_threshold | bytes_threshold |
|--------|---------------|-----------------|
| 300Mbps | 15MB/s | 150MB |
| 100Mbps | 5MB/s | 50MB |
| 30Mbps | 2MB/s | 20MB |

### 3. MultiChannelScheduler（自适应多通道调度器）

将单条流的数据拆分到多条通道并行传输。

**核心方法**：

```python
class MultiChannelScheduler:
    def get_chunks(self, stream_id: int, data: bytes) -> List[Tuple[int, int, bytes]]:
        """
        将数据拆分为 chunk，按通道质量加权分配。
        返回: List of (channel_id, sequence_number, chunk_data)
        """

    def _adapt_chunk_size(self):
        """
        根据 RTT 和带宽自适应 chunk 大小：
        - RTT 高 → chunk 更大（减少 ACK 往返次数）
        - 带宽高 → chunk 更大（摊平头部开销）

        chunk_size = BDP / num_channels / 4
        范围: [16KB, 256KB]
        """

    def _select_channel(self) -> int:
        """基于通道质量权重的加权选择。"""
```

| RTT | 带宽 | chunk_size |
|-----|------|-----------|
| 50ms | 300Mbps | ~117KB |
| 150ms | 300Mbps | ~176KB |
| 200ms | 50Mbps | ~26KB |

### 4. ChannelQualityMonitor（通道质量监控）

实时测量每条通道的有效带宽和延迟。

**核心方法**：

```python
class ChannelQualityMonitor:
    def record_sent(self, channel_id: int, bytes_count: int, timestamp: float):
        """记录某通道发送数据。"""

    def record_ack(self, channel_id: int, bytes_count: int, timestamp: float):
        """记录 ACK 到达（用于吞吐量估算）。"""

    def update_weights(self):
        """根据观测吞吐量重新计算通道权重。"""

    def get_weighted_channel(self) -> int:
        """加权随机选择通道。"""
```

### 5. SequenceReassembler（序列号重组器）

接收多通道乱序到达的数据，按序列号排序后顺序输出。

**核心方法**：

```python
class SequenceReassembler:
    def receive(self, seq: int, data: bytes) -> List[bytes]:
        """
        接收一个 chunk。
        返回从 next_seq 开始的连续 chunk 列表。
        """

    def is_stalled(self, timeout_ms: float = 500) -> bool:
        """检查是否等待缺失序列号过久。"""

    def reset(self):
        """重置重组器状态。"""
```

## 数据流

### 上传方向（客户端 → 服务器）

```
本地服务 reader
    → FlowClassifier（自适应分类）
        → 单通道? → 单通道发送
        → 多通道? → MultiChannelScheduler（chunk + 加权分配）
            → BatchSender x N（批量缓冲 + 自适应 flush）
                → TCP Data Channels x 16
```

### 下载方向（服务器 → 客户端）

```
TCP Data Channels x 16（接收带 Seq 的帧）
    → SequenceReassembler（按 Seq 排序）
        → 本地服务 writer
```

## 错误处理

| 错误场景 | 处理策略 |
|---------|---------|
| 序列号缺失（乱序/丢包） | 重组器缓冲最多 500ms，超时后发送 `CMD_MULTI_CHANNEL_NACK` 请求重传；持续失败降级为单通道 |
| 某通道断开 | 调度器将该通道权重置 0，流量自动转移；5 秒后尝试重连 |
| 多通道协商失败 | 回退单通道模式（Seq=0），完全兼容旧行为 |
| 重组器内存超限（>16MB） | 强制 flush 已缓存数据，清空缓冲区，日志告警 |
| 自适应参数异常 | 使用安全默认值，标记监控异常，不中断传输 |
| 通道质量全部劣化 | 自动降低 drain_threshold 和 chunk_size，优先保证稳定性 |

## 代码风格统一

### 修复规则

| 问题 | 修复规则 |
|------|---------|
| 缩进混合 | 全部统一为 4 空格 |
| 字符串引号混用 | 全部使用单引号（docstring 用双引号） |
| 命名风格不一致 | 全部使用 `snake_case` |
| 类型注解缺失 | 所有公共方法补全类型注解 |
| 魔术数字 | 提取为 `constants.py` 中的命名常量 |
| 日志格式不统一 | 统一为 `logger.info(f'[TAG] message: {var}')` |
| 裸 `except:` | 改为 `except Exception:` 并记录日志 |
| 行长度 | 限制在 100 字符以内 |
| 导入排序 | 按标准库、第三方库、本地模块分组，字母排序 |

## 性能预期

| 场景 | 优化前（预估） | 优化后（预期） |
|------|---------------|---------------|
| 单流大文件下载（300Mbps, 150ms RTT） | 5-10 MB/s | 30-37 MB/s |
| 多流并发下载 | 20-30 MB/s | 接近线速 |
| 高延迟弱网（50Mbps, 200ms RTT） | 2-5 MB/s | 15-20 MB/s |
| 小流量（< 阈值） | 正常 | 保持不变，低延迟 |

## 风险评估

| 风险 | 缓解措施 |
|------|---------|
| 多通道乱序重组引入延迟 | 500ms 超时 + 降级机制 |
| 序列号 64-bit 溢出 | 单次会话内不可能溢出 |
| 自适应参数震荡 | 平滑窗口 + 变化率限制 |
| 代码改动量大引入 bug | 保持旧代码兼容路径，单通道模式始终可用 |

---

*设计日期: 2026-05-30*
*版本: v1.0*
