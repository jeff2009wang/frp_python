# PFRP Linux 安装指南

## 概述

PFRP (Python FRP) 是一个高性能的多连接 FRP 客户端/服务器，支持自动端口扫描和转发。

## 特性

- ✅ 多连接并行传输，最大化吞吐量
- ✅ 自动端口扫描和注册
- ✅ 自适应网络质量调整
- ✅ 一键安装为系统服务
- ✅ 便捷的 `pfrp` 管理命令

## 安装

### 快速安装

```bash
curl -fsSL https://raw.githubusercontent.com/jeff2009wang/frp_python/main/install.sh -o install.sh
chmod +x install.sh
sudo ./install.sh
```

### 交互式安装

安装程序会显示以下选项：

```
==========================================
    PFRP Installer v1.0
    Multi-Connection FRP Client/Server
==========================================

Please select an option:

  1) Install PFRP Client (frpc)
  2) Install PFRP Server (frps)
  3) Install Both
  4) Uninstall PFRP
  5) Exit
```

## 使用

### pfrp 管理命令

安装完成后，可以使用 `pfrp` 命令管理服务：

```bash
pfrp status              # 查看服务状态和日志
pfrp start client        # 启动客户端
pfrp stop client         # 停止客户端
pfrp restart client      # 重启客户端
pfrp enable client       # 开机自启动
pfrp disable client      # 禁用开机自启动
pfrp logs client         # 查看实时日志

pfrp start server        # 启动服务器
pfrp stop server         # 停止服务器
pfrp logs server         # 查看服务器日志

pfrp config              # 查看配置文件位置
pfrp version             # 查看版本信息
pfrp help                # 查看帮助
```

### systemctl 命令

也可以直接使用 systemctl 管理服务：

```bash
sudo systemctl start pfrpc          # 启动客户端
sudo systemctl stop pfrpc           # 停止客户端
sudo systemctl restart pfrpc        # 重启客户端
sudo systemctl enable pfrpc         # 开机自启
sudo systemctl status pfrpc         # 查看状态

sudo systemctl start pfrps          # 启动服务器
sudo systemctl stop pfrps           # 停止服务器
sudo systemctl status pfrps         # 查看状态
```

## 配置

### 修改服务配置

编辑 systemd 服务文件：

**客户端配置:**
```bash
sudo nano /etc/systemd/system/pfrpc.service
```

**服务器配置:**
```bash
sudo nano /etc/systemd/system/pfrps.service
```

修改 `ExecStart` 行的参数：

**客户端示例:**
```ini
ExecStart=/usr/local/bin/pfrpc 127.0.0.1 7000 --channels 16 --target 192.168.1.100
```

**服务器示例:**
```ini
ExecStart=/usr/local/bin/pfrps 7000 --channels 16 --host 0.0.0.0
```

修改后重新加载配置：

```bash
sudo systemctl daemon-reload
sudo systemctl restart pfrpc  # 或 pfrps
```

## 命令行参数

### 客户端 (pfrpc)

```bash
pfrpc <server_host> <server_port> [options]
```

参数:
- `server_host`: 服务器地址
- `server_port`: 服务器端口
- `--channels NUM`: 数据通道数量 (默认: 16)
- `--target HOST`: 目标主机 (默认: 127.0.0.1)
- `--interval SECS`: 端口扫描间隔 (默认: 20)
- `--ports PORTS`: 监控的端口列表，逗号分隔 (例: 80,443,8080)

示例:
```bash
pfrpc 192.168.1.100 7000 --channels 16
pfrpc example.com 7000 --channels 8 --target 192.168.1.50
pfrpc 192.168.1.100 7000 --ports 80,443,8080,3306
```

### 服务器 (pfrps)

```bash
pfrps <port> [options]
```

参数:
- `port`: 监听端口
- `--channels NUM`: 数据通道数量 (默认: 16)
- `--host HOST`: 监听地址 (默认: 0.0.0.0)

示例:
```bash
pfrps 7000
pfrps 7000 --channels 8
pfrps 7000 --host 192.168.1.100
```

## 日志

### 查看日志

```bash
# 使用 pfrp 命令
pfrp logs client    # 查看客户端日志（实时）
pfrp logs server    # 查看服务器日志（实时）

# 使用 journalctl
sudo journalctl -u pfrpc -n 50           # 查看最近 50 行
sudo journalctl -u pfrpc -f              # 实时跟踪
sudo journalctl -u pfrpc --since "1 hour ago"  # 最近 1 小时

sudo journalctl -u pfrps -n 50
sudo journalctl -u pfrps -f
```

### 日志文件位置

- 系统日志: `journalctl -u pfrpc` / `journalctl -u pfrps`
- 应用日志: `/var/log/pfrp/`

## 卸载

```bash
sudo ./install.sh
# 选择选项 4) Uninstall PFRP
```

或手动卸载：

```bash
sudo systemctl stop pfrpc pfrps
sudo systemctl disable pfrpc pfrps
sudo rm -f /etc/systemd/system/pfrpc.service
sudo rm -f /etc/systemd/system/pfrps.service
sudo rm -f /usr/local/bin/pfrpc
sudo rm -f /usr/local/bin/pfrps
sudo rm -f /usr/local/bin/pfrp
sudo systemctl daemon-reload
```

## 故障排除

### 服务无法启动

1. 检查日志：
```bash
sudo journalctl -u pfrpc -n 50
```

2. 检查配置文件语法：
```bash
sudo systemd-analyze verify /etc/systemd/system/pfrpc.service
```

3. 手动测试：
```bash
sudo /usr/local/bin/pfrpc 127.0.0.1 7000
```

### 连接失败

1. 检查防火墙：
```bash
sudo ufw status
sudo ufw allow 7000/tcp
```

2. 检查端口是否被占用：
```bash
sudo netstat -tlnp | grep 7000
```

3. 测试网络连接：
```bash
ping <server_host>
telnet <server_host> <server_port>
```

## 性能优化

### 调整数据通道数量

根据网络带宽调整通道数：

- 低速网络 (< 10 Mbps): 4-8 通道
- 中速网络 (10-100 Mbps): 8-16 通道
- 高速网络 (> 100 Mbps): 16-32 通道

修改服务配置中的 `--channels` 参数。

### 端口扫描间隔

默认每 20 秒扫描一次端口。可根据需要调整：

```bash
--interval 30    # 每 30 秒扫描一次
--interval 10    # 每 10 秒扫描一次
```

## 系统要求

- Linux 系统 (支持 systemd)
- x86_64 或 ARM64 架构
- Python 3.7+ (已编译为二进制，无需安装)

## 许可证

MIT License

## 支持

GitHub: https://github.com/jeff2009wang/frp_python

## 更新日志

### v1.0
- 初始版本
- 支持多连接并行传输
- 自动端口扫描和注册
- 自适应网络质量调整
- systemd 服务集成
- pfrp 管理命令
