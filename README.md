# PFRP - 高性能多连接 FRP 客户端/服务器

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows-lightgrey.svg)](https://github.com/jeff2009wang/frp_python)
[![Python](https://img.shields.io/badge/python-3.7+-green.svg)](https://www.python.org/downloads/)

PFRP 是一个用 Python 编写的高性能 FRP (Fast Reverse Proxy) 客户端/服务器，支持多连接并行传输，最大化网络吞吐量。

## ✨ 特性

- 🚀 **多连接并行传输** - 使用多个 TCP 连接实现最大吞吐量
- 🔍 **自动端口扫描** - 自动检测并注册本地开放端口
- 🌐 **自适应网络质量** - 根据 RTT 和抖动自动调整缓冲区大小
- 📦 **独立可执行文件** - 无需安装 Python 或任何依赖
- 🎯 **跨平台支持** - 支持 Linux 和 Windows
- 🔧 **服务管理** - Linux 下支持 systemd 服务注册
- 📊 **性能监控** - 实时显示传输速率和性能指标
- 🌍 **UDP 转发** - 支持 UDP 端口转发（实验性）

## 🚀 快速开始

### Linux

#### 一键安装客户端

```bash
curl -fsSL https://raw.githubusercontent.com/jeff2009wang/frp_python/main/install.sh | sudo bash
```

#### 一键安装服务器

```bash
curl -fsSL https://raw.githubusercontent.com/jeff2009wang/frp_python/main/install.sh | sudo bash -s -- --server
```

#### 下载预编译版本

从 [GitHub Releases](https://github.com/jeff2009wang/frp_python/tree/main/dist/linux) 下载二进制文件：

```bash
wget https://github.com/jeff2009wang/frp_python/raw/main/dist/linux/frpc_multi -O pfrpc
wget https://github.com/jeff2009wang/frp_python/raw/main/dist/linux/frps_multi -O pfrps
chmod +x pfrpc pfrps
```

### Windows

从 [Releases](https://github.com/jeff2009wang/frp_python/releases) 下载 `.exe` 文件，直接运行即可。

## 📖 使用方法

### 客户端 (pfrpc)

```bash
pfrpc <server_host> <server_port> [options]
```

**示例:**

```bash
# 基本使用（TCP 端口自动扫描）
pfrpc 192.168.1.100 7000

# 自定义参数
pfrpc example.com 7000 --channels 16 --target 127.0.0.1

# 监控特定 TCP 端口
pfrpc 192.168.1.100 7000 --ports 80,443,8080,3306

# 转发 UDP 端口（实验性功能）
pfrpc 192.168.1.100 7000 --udp-ports 53,1194

# TCP + UDP 混合转发
pfrpc 192.168.1.100 7000 --ports 80,443 --udp-ports 53

# 调整扫描间隔
pfrpc 192.168.1.100 7000 --interval 30
```

**参数:**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `server_host` | 服务器地址 | - |
| `server_port` | 服务器端口 | - |
| `--channels` | 数据通道数量 | 16 |
| `--target` | 目标主机 | 127.0.0.1 |
| `--interval` | 端口扫描间隔(秒) | 20 |
| `--ports` | 监控的 TCP 端口列表(逗号分隔) | 自动扫描 |
| `--udp-ports` | 转发的 UDP 端口列表(逗号分隔) | 空 |

### 服务器 (pfrps)

```bash
pfrps <port> [options]
```

**示例:**

```bash
# 基本使用
pfrps 7000

# 自定义通道数
pfrps 7000 --channels 8

# 监听特定地址
pfrps 7000 --host 192.168.1.100
```

**参数:**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `port` | 监听端口 | - |
| `--channels` | 数据通道数量 | 16 |
| `--host` | 监听地址 | 0.0.0.0 |

## 🛠️ Linux 服务管理

安装后，使用 `pfrp` 命令管理服务：

```bash
pfrp status              # 查看服务状态和日志
pfrp start client        # 启动客户端
pfrp stop client         # 停止客户端
pfrp restart client      # 重启客户端
pfrp enable client       # 开机自启动
pfrp logs client         # 查看实时日志

pfrp start server        # 启动服务器
pfrp stop server         # 停止服务器
pfrp logs server         # 查看服务器日志
```

或使用 systemctl：

```bash
sudo systemctl start pfrpc          # 启动客户端
sudo systemctl stop pfrpc           # 停止客户端
sudo systemctl restart pfrpc        # 重启客户端
sudo systemctl enable pfrpc         # 开机自启
sudo systemctl status pfrpc         # 查看状态
```

## 📂 项目结构

```
frp_python/
├── frpc_multi.py           # 客户端源代码
├── frps_multi.py           # 服务器源代码
├── install.sh              # Linux 安装脚本
├── build_windows.bat       # Windows 编译脚本
├── build_linux.sh          # Linux 编译脚本
├── Dockerfile.linux        # Docker 编译文件
├── requirements.txt        # Python 依赖
└── dist/                   # 编译输出
    ├── windows/            # Windows 可执行文件
    │   ├── frpc_multi.exe
    │   └── frps_multi.exe
    └── linux/              # Linux 可执行文件
        ├── frpc_multi
        └── frps_multi
```

## 🔧 编译

### Windows

```cmd
cd frp_python
build_windows.bat
```

### Linux

```bash
cd frp_python
chmod +x build_linux.sh
./build_linux.sh
```

### Docker

```bash
cd frp_python
docker build -f Dockerfile.linux -t pfrp-linux .
docker cp pfrp-linux:/build/dist/linux ./dist/
```

## 🎯 工作原理

PFRP 使用多个并行的 TCP 连接来最大化网络吞吐量：

1. **控制连接** - 处理命令、端口注册和心跳
2. **数据通道** - 多个并行连接传输实际数据
3. **自动端口扫描** - 客户端自动检测本地开放端口并注册到服务器
4. **自适应优化** - 根据网络质量 (RTT, 抖动) 动态调整缓冲区大小

```
客户端                          服务器
┌─────────┐                   ┌─────────┐
│  本地    │                   │  公网    │
│         │                   │         │
│ ┌─────┐ │◄─── 控制连接 ─────►│ ┌─────┐ │
│ │App A│ │                   │ │用户A│ │
│ └─────┘ │                   │ └─────┘ │
│         │                   │         │
│ ┌─────┐ │◄─数据通道 1───────►│ ┌─────┐ │
│ │App B│ │◄─数据通道 2───────►│ │用户B│ │
│ └─────┘ │   ...             │ └─────┘ │
│         │◄─数据通道 N───────►│         │
└─────────┘                   └─────────┘
```

## 📊 性能优化

### 通道数量选择

根据网络带宽调整通道数：

| 带宽 | 推荐通道数 |
|------|-----------|
| < 10 Mbps | 4-8 |
| 10-100 Mbps | 8-16 |
| > 100 Mbps | 16-32 |

### 自适应网络检测

PFRP 自动监测网络质量：

- **强网络模式**: RTT < 300ms 且抖动 < 150ms
  - 缓冲区: 16MB
  - 适合: 有线网络、5G

- **弱网络模式**: RTT ≥ 300ms 或抖动 ≥ 150ms
  - 缓冲区: 4MB
  - 适合: 3G、高延迟网络

## 🔍 故障排除

### 服务无法启动

```bash
# 查看日志
sudo journalctl -u pfrpc -n 50

# 检查配置
sudo systemd-analyze verify /etc/systemd/system/pfrpc.service

# 手动测试
sudo /usr/local/bin/pfrpc 127.0.0.1 7000
```

### 连接失败

```bash
# 检查防火墙
sudo ufw status
sudo ufw allow 7000/tcp

# 检查端口
sudo netstat -tlnp | grep 7000

# 测试网络
ping <server_host>
telnet <server_host> <server_port>
```

### 性能问题

1. 调整通道数量: `--channels`
2. 检查网络延迟和抖动
3. 查看性能统计: 日志中的 `[PERF]` 信息

## 📝 更新日志

### v1.0 (2024-02-03)
- ✨ 初始版本
- 🚀 多连接并行传输
- 🔍 自动端口扫描和注册
- 🌐 自适应网络质量调整
- 📦 独立可执行文件
- 🎯 跨平台支持 (Linux/Windows)
- 🔧 Linux systemd 服务集成
- 📊 性能监控和日志

## 📄 许可证

[MIT License](LICENSE)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

GitHub: https://github.com/jeff2009wang/frp_python

## ⭐ Star History

如果这个项目对你有帮助，请给个 Star！

---

**注意**: 请确保在使用前遵守当地法律法规。
