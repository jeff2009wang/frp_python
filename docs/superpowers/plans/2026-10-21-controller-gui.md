# 总控与网页穿透 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 做一个总控网页和设备代理，让用户只填参数就能建立「给外网访问」和「设备互访」两种穿透，并且字节不经过总控。

**Architecture:** 新代码放在 `pfrp/control/`，不接入现有多通道传输。总控用一个 HTTP 端口做网页和信令；被选中的设备运行 `EntryBridge`，用一条会话连接通知新访问，每个访问再开一条数据连接做双向拷贝。设备登记、公网判断和穿透记录先写成可单测的纯逻辑，再接到套接字。

**Tech Stack:** Python 3.7+，产品代码只用标准库（`ipaddress`、`hashlib`、`asyncio`、`http`、`json`、`ctypes`）。测试沿用仓库里的 pytest 和 pytest-asyncio。

**Spec:** `docs/superpowers/specs/2026-10-21-controller-gui-design.md`

## Global Constraints

- 产品代码只使用 Python 标准库，不新增第三方依赖。
- 不修改 `frpc_multi.py`、`frps_multi.py`、`pfrp/constants.py`、`pfrp/batch_sender.py`、`pfrp/channel_monitor.py`、`pfrp/flow_classifier.py`、`pfrp/scheduler.py`、`pfrp/reassembler.py`。
- 公网判断只看 IPv4。非公网网段是 `0.0.0.0/8`、`10.0.0.0/8`、`127.0.0.0/8`、`169.254.0.0/16`、`172.16.0.0/12`、`192.168.0.0/16`、`100.64.0.0/10`、`255.255.255.255/32`。IPv6 不是公网可达的证据。
- 未设置 `PFRP_DATA_DIR` 时，数据目录是 Windows `%LOCALAPPDATA%\pfrp`，Linux `$XDG_DATA_HOME/pfrp`，未设置 `XDG_DATA_HOME` 时为 `~/.local/share/pfrp`。测试必须设置 `PFRP_DATA_DIR`。
- 口令用 PBKDF2-HMAC-SHA256 保存，迭代次数 `200000`，盐 16 字节，摘要 32 字节。存储串格式 `pbkdf2_sha256$200000$<salt_hex>$<digest_hex>`。
- 数值：网页端口预填 `7500`；探测超时 `3` 秒；重测间隔 `180` 秒；建立超时 `10` 秒；长轮询 `25` 秒；重试从 `1` 秒起加倍，上限 `30` 秒。
- 可达性字符串只使用 `公网可达`、`仅内网可达`、`检测中`。穿透状态只使用 `建立中`、`正常`、`重连中`、`入口不可用`、`建立失败`。
- 规格原文错误句：`口令不正确`、`端口已被占用`、`没能读到本机监听端口`、`目标端口没有服务`。本计划补上的句子：`当前没有可用入口`、`端口无效`、`名称已被使用`、`总控地址必须是 http://主机:端口`。
- Cookie 名 `pfrp_session`，属性 `HttpOnly`、`SameSite=Lax`、`Path=/`，不加 `Secure`。会话只放内存。
- 首次页面只绑 `127.0.0.1`。总控确认后网页和信令绑 `0.0.0.0` 的同一个端口。
- 类型注解使用 `typing` 里的 `List`、`Dict`、`Optional`、`Tuple`，以兼容 Python 3.7。
- 不做带宽验收，不写多通道调度。

## Review Focus

- 来源地址 `127.0.0.1` 不能把总控标成公网可达；结果必须是 `仅内网可达`。测试在 Task 4。
- `external_port` 为 `0` 或 `65536` 时不创建穿透，错误句是 `端口无效`。测试在 Task 3。
- 移除正在当入口的设备后，相关穿透变为 `入口不可用`，对外监听关掉。测试在 Task 9。
- 外部连接已经进来，但目标在建立超时内没有完成数据握手：只关闭这一次访问，穿透保持 `正常`，下一次访问仍然可以成功。测试在 Task 6。
- 总控地址写成 `https://203.0.113.10:7500` 或 `http://203.0.113.10:7500/api` 时拒绝，代理状态文件不变。测试在 Task 10。

---

## File Structure

```
pfrp/control/__init__.py       # 包标记
pfrp/control/constants.py      # 状态字符串、错误句、超时数值
pfrp/control/timing.py         # 返回上述数值的函数，供测试打桩
pfrp/control/addresses.py      # is_public_ipv4、normalize_controller_url
pfrp/control/passphrase.py     # hash_passphrase、verify_passphrase
pfrp/control/models.py         # Device、Tunnel
pfrp/control/state.py          # 数据目录、ControllerStore、AgentStore
pfrp/control/registry.py       # 登记、口令、穿透校验
pfrp/control/reachability.py   # 总控/设备可达性和入口候选
pfrp/control/scanner.py        # 本机 127.0.0.1 能连上的监听端口
pfrp/control/protocol.py       # 一行一个 JSON
pfrp/control/bridge.py         # 入口会话、数据连接、双向拷贝
pfrp/control/queue.py          # 每设备指令队列
pfrp/control/api.py            # 与套接字无关的 HTTP 行为
pfrp/control/server.py         # 总控 HTTP、探测、下发穿透
pfrp/control/agent.py          # 设备代理：信令、扫描、接桥
pfrp/control/pages.py          # 首次页面和管理页面 HTML
pfrp/control/__main__.py       # python -m pfrp.control
tests/test_control_constants.py
tests/test_control_state.py
tests/test_control_registry.py
tests/test_control_reachability.py
tests/test_control_scanner.py
tests/test_control_bridge.py
tests/test_control_api.py
tests/test_control_server.py
tests/test_control_flow.py
tests/test_control_pages.py
```

`ControllerStore` 写入 `<数据目录>/controller.json`。`AgentStore` 写入 `<数据目录>/agent.json`。

会话和数据连接都拨入入口的 `session_port`。对外端口只接受外部用户。设备互访不为该服务再绑公网端口。流编号从 1 起，按穿透分别递增。

## Task 1: 常量、计时和 IPv4 分类

**Files:**
- Create: `pfrp/control/__init__.py`
- Create: `pfrp/control/constants.py`
- Create: `pfrp/control/timing.py`
- Create: `pfrp/control/addresses.py`
- Test: `tests/test_control_constants.py`

**Interfaces:**
- Consumes: 无
- Produces:
  - `is_public_ipv4(ip: str) -> bool`
  - `normalize_controller_url(url: str) -> str`，失败抛 `ControlError`
  - `class ControlError(Exception)`，属性 `message: str`
  - `hash` 用的常量，以及 `REACH_PUBLIC`、`REACH_PRIVATE`、`REACH_CHECKING`、`STATUS_ESTABLISHING`、`STATUS_UP`、`STATUS_RECONNECTING`、`STATUS_ENTRY_DOWN`、`STATUS_FAILED`
  - `ERR_BAD_PASSPHRASE = "口令不正确"`、`ERR_PORT_IN_USE = "端口已被占用"`、`ERR_SCAN_FAILED = "没能读到本机监听端口"`、`ERR_TARGET_DOWN = "目标端口没有服务"`、`ERR_NO_ENTRY = "当前没有可用入口"`、`ERR_BAD_PORT = "端口无效"`、`ERR_NAME_TAKEN = "名称已被使用"`、`ERR_BAD_URL = "总控地址必须是 http://主机:端口"`
  - `DEFAULT_WEB_PORT = 7500`、`PROBE_TIMEOUT_S = 3.0`、`REPROBE_INTERVAL_S = 180.0`、`SETUP_TIMEOUT_S = 10.0`、`LONG_POLL_S = 25.0`、`RETRY_INITIAL_S = 1.0`、`RETRY_MAX_S = 30.0`、`PBKDF2_ITERATIONS = 200000`、`COOKIE_NAME = "pfrp_session"`
  - `probe_timeout() -> float`、`reprobe_interval() -> float`、`setup_timeout() -> float`、`long_poll_timeout() -> float`、`retry_delay(attempt: int) -> float`

- [ ] **Step 1: Write the failing test**

```python
from pfrp.control.addresses import is_public_ipv4, normalize_controller_url
from pfrp.control.constants import (
    ERR_BAD_URL, PROBE_TIMEOUT_S, REPROBE_INTERVAL_S, SETUP_TIMEOUT_S, LONG_POLL_S,
)
from pfrp.control.timing import (
    probe_timeout, reprobe_interval, setup_timeout, long_poll_timeout, retry_delay,
)

def test_public_and_non_public_ipv4():
    assert is_public_ipv4("8.8.8.8") is True
    assert is_public_ipv4("172.15.255.255") is True
    assert is_public_ipv4("100.128.0.1") is True
    for ip in ("10.0.0.1", "127.0.0.1", "172.16.0.1", "172.31.255.255",
               "192.168.1.1", "100.64.0.1", "100.127.255.255", "169.254.1.1",
               "0.1.2.3", "255.255.255.255", "2001:db8::1", "", "not-an-ip"):
        assert is_public_ipv4(ip) is False

def test_timing_matches_spec():
    assert probe_timeout() == PROBE_TIMEOUT_S == 3.0
    assert reprobe_interval() == REPROBE_INTERVAL_S == 180.0
    assert setup_timeout() == SETUP_TIMEOUT_S == 10.0
    assert long_poll_timeout() == LONG_POLL_S == 25.0
    assert retry_delay(0) == 1.0
    assert retry_delay(1) == 2.0
    assert retry_delay(4) == 16.0
    assert retry_delay(5) == 30.0

def test_controller_url_normalized_or_rejected():
    assert normalize_controller_url("http://203.0.113.10:7500/") == "http://203.0.113.10:7500"
    for bad in ("https://203.0.113.10:7500", "http://203.0.113.10:7500/api", "http://203.0.113.10"):
        try:
            normalize_controller_url(bad)
        except Exception as exc:
            assert exc.message == ERR_BAD_URL
        else:
            raise AssertionError(bad)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_constants.py -v`

Expected: FAIL，`pfrp.control.addresses` 无法导入。

- [ ] **Step 3: Implement the signatures listed above**

`is_public_ipv4` 用 `ipaddress`。先拒绝带 `:` 的字符串和非 IPv4，再拒绝 Global Constraints 里的八个网段。`retry_delay` 为 `min(RETRY_MAX_S, RETRY_INITIAL_S * (2 ** attempt))`。`normalize_controller_url` 只接受 scheme `http`、显式端口、路径为空或 `/`，并且没有用户信息、query、fragment。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_constants.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/__init__.py pfrp/control/constants.py pfrp/control/timing.py pfrp/control/addresses.py tests/test_control_constants.py
git commit -m "feat: add control-plane address and timing rules"
```

## Task 2: 口令哈希和状态文件

**Files:**
- Create: `pfrp/control/passphrase.py`
- Create: `pfrp/control/models.py`
- Create: `pfrp/control/state.py`
- Test: `tests/test_control_state.py`

**Interfaces:**
- Consumes: `PBKDF2_ITERATIONS`、`ControlError`
- Produces:
  - `hash_passphrase(passphrase: str) -> str`
  - `verify_passphrase(passphrase: str, stored: str) -> bool`
  - `class Device`，字段 `device_id: str`、`name: str`、`online: bool`、`reachability: str`、`announce_ip: Optional[str]`、`observed_ip: Optional[str]`、`local_ips: List[str]`
  - `class Tunnel`，字段 `tunnel_id: str`、`kind: str`、`target_device_id: str`、`target_port: int`、`entry_device_id: str`、`external_port: Optional[int]`、`accessor_device_id: Optional[str]`、`accessor_port: Optional[int]`、`token: str`、`status: str`、`last_error: str`、`session_port: Optional[int]`
  - `data_dir() -> str`
  - `class ControllerStore`，`save(self) -> None`，`load(cls, directory: str) -> ControllerStore`；字段 `passphrase_hash: str`、`listen_port: int`、`controller_device_id: str`、`devices: Dict[str, Device]`、`tunnels: Dict[str, Tunnel]`、`revoked_device_ids: List[str]`
  - `class AgentStore`，字段 `device_id: str`、`name: str`、`controller_url: str`、`role: str`；`save` / `load` 同上

- [ ] **Step 1: Write the failing test**

```python
import os
from pfrp.control.passphrase import hash_passphrase, verify_passphrase
from pfrp.control.state import AgentStore, ControllerStore, data_dir

def test_passphrase_roundtrip_uses_pbkdf2():
    stored = hash_passphrase("secret")
    assert stored.startswith("pbkdf2_sha256$200000$")
    assert verify_passphrase("secret", stored) is True
    assert verify_passphrase("other", stored) is False

def test_data_dir_honors_env_then_platform(monkeypatch, tmp_path):
    monkeypatch.setenv("PFRP_DATA_DIR", str(tmp_path))
    assert data_dir() == str(tmp_path)
    monkeypatch.delenv("PFRP_DATA_DIR")
    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "app"))
    assert data_dir() == str(tmp_path / "app" / "pfrp")

def test_stores_roundtrip_without_sessions(tmp_path):
    controller = ControllerStore(str(tmp_path))
    controller.passphrase_hash = hash_passphrase("secret")
    controller.listen_port = 7500
    controller.controller_device_id = "dev-controller"
    controller.save()
    loaded = ControllerStore.load(str(tmp_path))
    assert loaded.passphrase_hash == controller.passphrase_hash
    assert loaded.listen_port == 7500
    text = open(os.path.join(str(tmp_path), "controller.json"), encoding="utf-8").read()
    assert "secret" not in text
    assert "pfrp_session" not in text
    agent = AgentStore(str(tmp_path))
    agent.device_id = "dev-1"
    agent.name = "office"
    agent.controller_url = "http://203.0.113.10:7500"
    agent.role = "device"
    agent.save()
    assert AgentStore.load(str(tmp_path)).device_id == "dev-1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_state.py -v`

Expected: FAIL，`pfrp.control.passphrase` 无法导入。

- [ ] **Step 3: Implement `hash_passphrase`, `verify_passphrase`, `data_dir`, `ControllerStore`, `AgentStore`**

哈希用 `hashlib.pbkdf2_hmac("sha256", ...)`。Linux 分支在 `os.name != "nt"` 时使用 `XDG_DATA_HOME` 或 `~/.local/share/pfrp`。`Device` 和 `Tunnel` 提供 `to_dict` / `from_dict`，供两个 Store 使用。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_state.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/passphrase.py pfrp/control/models.py pfrp/control/state.py tests/test_control_state.py
git commit -m "feat: persist controller and agent state"
```

## Task 3: 设备登记和穿透校验

**Files:**
- Create: `pfrp/control/registry.py`
- Test: `tests/test_control_registry.py`

**Interfaces:**
- Consumes: `ControllerStore`、`Device`、`Tunnel`、`hash_passphrase`、`ControlError`、本计划里的 `ERR_*` 和 `STATUS_*`、`REACH_CHECKING`
- Produces:
  - `class Registry`，`open(cls, directory: str) -> Registry`
  - `set_passphrase(self, passphrase: str) -> None`
  - `check_passphrase(self, passphrase: str) -> bool`
  - `change_passphrase(self, passphrase: str) -> None`
  - `register_device(self, device_id: str, name: str, passphrase: str, observed_ip: str, local_ips: List[str]) -> Device`
  - `remove_device(self, device_id: str) -> List[str]`，返回受影响的 `tunnel_id`
  - `create_tunnel(self, kind: str, target_device_id: str, target_port: int, entry_device_id: str, external_port: Optional[int] = None, accessor_device_id: Optional[str] = None, accessor_port: Optional[int] = None) -> Tunnel`
  - `close_tunnel(self, tunnel_id: str) -> Tunnel`
  - `set_reachability(self, device_id: str, reachability: str, announce_ip: Optional[str]) -> None`
  - `set_online(self, device_id: str, online: bool) -> List[str]`
  - `set_tunnel_status(self, tunnel_id: str, status: str, last_error: str = "") -> None`
  - `entry_candidates(self) -> List[Device]`
  - 属性 `store: ControllerStore`。没有 `controller.json` 时 `open` 创建空登记，不抛异常。`entry_candidates` 通过 `pfrp.control.addresses.is_public_ipv4` 这个模块属性判断可宣布地址，方便测试替换。

- [ ] **Step 1: Write the failing test**

```python
import pytest
from pfrp.control.constants import (
    ERR_BAD_PASSPHRASE, ERR_BAD_PORT, ERR_NAME_TAKEN, ERR_NO_ENTRY,
    REACH_PUBLIC, STATUS_ENTRY_DOWN, STATUS_ESTABLISHING,
)
from pfrp.control.registry import Registry

def test_register_rejects_bad_passphrase_and_duplicate_name(tmp_path):
    registry = Registry.open(str(tmp_path))
    registry.set_passphrase("secret")
    with pytest.raises(Exception) as bad:
        registry.register_device("d1", "office", "nope", "203.0.113.8", [])
    assert bad.value.message == ERR_BAD_PASSPHRASE
    assert registry.store.devices == {}
    registry.register_device("d1", "office", "secret", "203.0.113.8", [])
    with pytest.raises(Exception) as taken:
        registry.register_device("d2", "office", "secret", "203.0.113.9", [])
    assert taken.value.message == ERR_NAME_TAKEN

def test_reconnect_keeps_name_and_ignores_passphrase_change(tmp_path):
    registry = Registry.open(str(tmp_path))
    registry.set_passphrase("secret")
    registry.register_device("d1", "office", "secret", "203.0.113.8", [])
    registry.register_device("d2", "home", "secret", "198.51.100.8", [])
    registry.change_passphrase("next")
    again = registry.register_device("d1", "home", "wrong", "203.0.113.8", [])
    assert again.name == "office"
    assert again.online is True

def test_removed_device_must_use_current_passphrase(tmp_path):
    registry = Registry.open(str(tmp_path))
    registry.set_passphrase("secret")
    registry.register_device("d1", "office", "secret", "203.0.113.8", [])
    registry.remove_device("d1")
    with pytest.raises(Exception) as denied:
        registry.register_device("d1", "office", "secret", "203.0.113.8", [])
    assert denied.value.message == ERR_BAD_PASSPHRASE
    registry.change_passphrase("next")
    revived = registry.register_device("d1", "office", "next", "203.0.113.8", [])
    assert revived.device_id == "d1"

def test_tunnel_requires_candidate_and_valid_port(tmp_path):
    registry = Registry.open(str(tmp_path))
    registry.set_passphrase("secret")
    registry.register_device("entry", "entry", "secret", "203.0.113.10", [])
    registry.register_device("target", "target", "secret", "198.51.100.10", [])
    with pytest.raises(Exception) as no_entry:
        registry.create_tunnel("public", "target", 80, "entry", external_port=8080)
    assert no_entry.value.message == ERR_NO_ENTRY
    registry.set_reachability("entry", REACH_PUBLIC, "203.0.113.10")
    registry.set_online("entry", True)
    for port in (0, 65536):
        with pytest.raises(Exception) as bad_port:
            registry.create_tunnel("public", "target", 80, "entry", external_port=port)
        assert bad_port.value.message == ERR_BAD_PORT
        assert registry.store.tunnels == {}
    tunnel = registry.create_tunnel("public", "target", 80, "entry", external_port=8080)
    assert tunnel.status == STATUS_ESTABLISHING
    with pytest.raises(Exception) as conflict:
        registry.create_tunnel("public", "target", 81, "entry", external_port=8080)
    assert conflict.value.message == "端口已被占用"
    registry.set_online("entry", False)
    assert registry.store.tunnels[tunnel.tunnel_id].status == STATUS_ENTRY_DOWN
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_registry.py -v`

Expected: FAIL，`pfrp.control.registry` 无法导入。

- [ ] **Step 3: Implement `Registry` with the signatures above**

新设备必须口令正确且名称未被占用，初始 `reachability` 为 `检测中`，`online` 为 `True`。已登记且未移除的设备再次 `register_device` 时忽略口令和请求中的名称，只更新 `observed_ip`、`local_ips` 和在线状态。`entry_candidates` 只返回在线、`公网可达`、且 `announce_ip` 为公网 IPv4 的设备。`create_tunnel` 生成 `token`，`kind` 只接受 `public` 和 `private`；`private` 还要校验 `accessor_port`。端口必须在 1 到 65535。同一入口的同一 `external_port`，或同一访问方的同一 `accessor_port`，二次创建抛 `ERR_PORT_IN_USE`。入口变为离线时，把它参与的穿透设为 `STATUS_ENTRY_DOWN`，并作为 `set_online(False)` 的返回值。`remove_device` 把编号写入 `revoked_device_ids` 并做同样的状态更新。每次修改后 `save`。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_registry.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/registry.py tests/test_control_registry.py
git commit -m "feat: add device registry and tunnel validation"
```

## Task 4: 公网可达判断

**Files:**
- Create: `pfrp/control/reachability.py`
- Test: `tests/test_control_reachability.py`

**Interfaces:**
- Consumes: `is_public_ipv4`、`REACH_PUBLIC`、`REACH_PRIVATE`、`REACH_CHECKING`
- Produces:
  - `classify_controller(source_ips: List[str]) -> str`
  - `initial_device_reachability(controller_reach: str) -> str`
  - `reachability_after_probe(ok: bool) -> str`
  - `announce_ip_for_device(observed_ip: str, probe_ok: bool) -> Optional[str]`
  - `announce_ip_for_controller(resolved_ips: List[str]) -> Optional[str]`
  - `is_entry_candidate(online: bool, reachability: str, announce_ip: Optional[str]) -> bool`

- [ ] **Step 1: Write the failing test**

```python
from pfrp.control.constants import REACH_CHECKING, REACH_PRIVATE, REACH_PUBLIC
from pfrp.control.reachability import (
    announce_ip_for_controller, announce_ip_for_device, classify_controller,
    initial_device_reachability, is_entry_candidate, reachability_after_probe,
)

def test_loopback_source_does_not_make_controller_public():
    assert classify_controller(["127.0.0.1"]) == REACH_PRIVATE
    assert classify_controller(["10.0.0.4", "2001:db8::1"]) == REACH_PRIVATE
    assert classify_controller(["10.0.0.4", "203.0.113.8"]) == REACH_PUBLIC

def test_devices_are_not_probed_until_controller_is_public():
    assert initial_device_reachability(REACH_PRIVATE) == REACH_PRIVATE
    assert initial_device_reachability(REACH_PUBLIC) == REACH_CHECKING
    assert reachability_after_probe(True) == REACH_PUBLIC
    assert reachability_after_probe(False) == REACH_PRIVATE
    assert announce_ip_for_device("203.0.113.8", True) == "203.0.113.8"
    assert announce_ip_for_device("10.0.0.4", True) is None
    assert announce_ip_for_device("203.0.113.8", False) is None

def test_controller_announce_ip_is_first_public_result():
    assert announce_ip_for_controller(["10.0.0.4", "203.0.113.10", "198.51.100.10"]) == "203.0.113.10"
    assert is_entry_candidate(True, REACH_PUBLIC, "203.0.113.10") is True
    assert is_entry_candidate(False, REACH_PUBLIC, "203.0.113.10") is False
    assert is_entry_candidate(True, REACH_CHECKING, "203.0.113.10") is False
    assert is_entry_candidate(True, REACH_PUBLIC, None) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_reachability.py -v`

Expected: FAIL，`pfrp.control.reachability` 无法导入。

- [ ] **Step 3: Implement the six functions**

`classify_controller` 在任一来源使 `addresses.is_public_ipv4` 为真时返回 `REACH_PUBLIC`，空列表也返回 `REACH_PRIVATE`。`announce_ip_for_device` 和 `is_entry_candidate` 同样通过 `pfrp.control.addresses.is_public_ipv4` 这个模块属性判断，不要把函数复制到本地别名。探测成功且来源地址是公网 IPv4 时才返回可宣布地址。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_reachability.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/reachability.py tests/test_control_reachability.py
git commit -m "feat: classify public reachability"
```

## Task 5: 本机端口推荐

**Files:**
- Create: `pfrp/control/scanner.py`
- Test: `tests/test_control_scanner.py`

**Interfaces:**
- Consumes: `ERR_SCAN_FAILED`
- Produces: `class ScanResult`，字段 `ports: List[int]`、`error: str`；`scan_local_ports() -> ScanResult`

- [ ] **Step 1: Write the failing test**

```python
import socket
from pfrp.control.constants import ERR_SCAN_FAILED
from pfrp.control.scanner import scan_local_ports

def test_scan_includes_loopback_listener_only():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    try:
        result = scan_local_ports()
        assert port in result.ports
        assert result.error == ""
    finally:
        listener.close()

def test_scan_failure_has_spec_message(monkeypatch):
    monkeypatch.setattr("pfrp.control.scanner._listening_ports", lambda: (_ for _ in ()).throw(OSError("no table")))
    result = scan_local_ports()
    assert result.ports == []
    assert result.error == ERR_SCAN_FAILED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_scanner.py -v`

Expected: FAIL，`pfrp.control.scanner` 无法导入。

- [ ] **Step 3: Implement `scan_local_ports`**

`_listening_ports()` 在 Windows 上用 `ctypes` 调 `iphlpapi.GetExtendedTcpTable`，其他系统解析 `/proc/net/tcp` 里状态为 `0A` 的端口。然后对每个端口连接 `127.0.0.1`，超时 0.2 秒，只保留能连上的端口。枚举抛 `OSError` 时返回空列表和 `ERR_SCAN_FAILED`。这个函数不接触 `Registry`。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_scanner.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/scanner.py tests/test_control_scanner.py
git commit -m "feat: recommend loopback-reachable listen ports"
```

## Task 6: 入口桥

**Files:**
- Create: `pfrp/control/protocol.py`
- Create: `pfrp/control/bridge.py`
- Test: `tests/test_control_bridge.py`

**Interfaces:**
- Consumes: `setup_timeout`、`ERR_PORT_IN_USE`、`ControlError`
- Produces:
  - `dumps(message: dict) -> bytes`，以 `\n` 结尾
  - `loads(line: bytes) -> dict`
  - `enable_keepalive(sock: socket.socket) -> None`
  - `class EntryBridge`
  - `start(self) -> None`（async）
  - `session_port(self) -> int`
  - `allow_tunnel(self, tunnel_id: str, token: str, target_port: int, public_port: Optional[int] = None) -> None`（async）
  - `close_tunnel(self, tunnel_id: str) -> None`（async）
  - `stop(self) -> None`（async）
  - `public_port(self, tunnel_id: str) -> int`，仅在 `public_port=0` 绑到临时端口之后可用

会话第一行是 `{"type":"hello","tunnel_id","token","role","device_id"}`。成功回复 `{"type":"hello_ok"}`。令牌不对则关闭且不写字节。访问方会话发送 `{"type":"accept"}` 后，入口向访问方和目标都写 `{"type":"open","stream_id":int,"target_port":int}`。给外网访问时，外部连接到达后只向目标写 `open`。数据连接的第一行是 `{"type":"data","tunnel_id","token","stream_id","role"}`，之后是原始字节。数据连接也拨入 `session_port`。

- [ ] **Step 1: Write the failing test**

```python
import asyncio
import json
import pytest
from pfrp.control.bridge import EntryBridge
from pfrp.control.constants import ERR_PORT_IN_USE
from pfrp.control.protocol import dumps

async def _hello(port, tunnel_id, token, role):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(dumps({"type": "hello", "tunnel_id": tunnel_id, "token": token, "role": role, "device_id": role}))
    await writer.drain()
    return reader, writer

@pytest.mark.asyncio
async def test_bad_token_and_port_conflict():
    bridge = EntryBridge()
    await bridge.start()
    await bridge.allow_tunnel("tun", "secret", 9, public_port=0)
    reader, writer = await _hello(bridge.session_port(), "tun", "nope", "target")
    assert await reader.read() == b""
    with pytest.raises(Exception) as occupied:
        await bridge.allow_tunnel("other", "secret", 9, public_port=bridge.session_port())
    assert occupied.value.message == ERR_PORT_IN_USE
    await bridge.stop()
```

下面三个测试和 `test_bad_token_and_port_conflict` 放在同一个文件里。

```python
@pytest.mark.asyncio
async def test_late_data_hello_closes_only_that_visitor(monkeypatch):
    monkeypatch.setattr("pfrp.control.bridge.setup_timeout", lambda: 0.2)
    bridge = EntryBridge()
    await bridge.start()
    await bridge.allow_tunnel("tun", "secret", 80, public_port=0)
    target_reader, _target_writer = await _hello(bridge.session_port(), "tun", "secret", "target")
    assert b"hello_ok" in await target_reader.readline()
    late_reader, _late_writer = await asyncio.open_connection("127.0.0.1", bridge.public_port("tun"))
    late_open = json.loads((await target_reader.readline()).decode("utf-8"))
    await asyncio.sleep(0.5)
    assert await late_reader.read() == b""
    good_reader, good_writer = await asyncio.open_connection("127.0.0.1", bridge.public_port("tun"))
    good_open = json.loads((await target_reader.readline()).decode("utf-8"))
    assert good_open["stream_id"] != late_open["stream_id"]
    data_reader, data_writer = await asyncio.open_connection("127.0.0.1", bridge.session_port())
    data_writer.write(dumps({"type": "data", "tunnel_id": "tun", "token": "secret", "stream_id": good_open["stream_id"], "role": "target"}))
    await data_writer.drain()
    good_writer.write(b"OK")
    await good_writer.drain()
    assert await data_reader.readexactly(2) == b"OK"
    await bridge.stop()

@pytest.mark.asyncio
async def test_two_public_streams_do_not_mix():
    bridge = EntryBridge()
    await bridge.start()
    await bridge.allow_tunnel("tun", "secret", 80, public_port=0)
    target_reader, _target_writer = await _hello(bridge.session_port(), "tun", "secret", "target")
    await target_reader.readline()
    pairs = []
    for _ in range(2):
        visitor = await asyncio.open_connection("127.0.0.1", bridge.public_port("tun"))
        opened = json.loads((await target_reader.readline()).decode("utf-8"))
        data = await asyncio.open_connection("127.0.0.1", bridge.session_port())
        data[1].write(dumps({"type": "data", "tunnel_id": "tun", "token": "secret", "stream_id": opened["stream_id"], "role": "target"}))
        await data[1].drain()
        pairs.append((visitor, data))
    pairs[0][0][1].write(b"AAA")
    pairs[1][0][1].write(b"BBB")
    await pairs[0][0][1].drain()
    await pairs[1][0][1].drain()
    assert await pairs[0][1][0].readexactly(3) == b"AAA"
    assert await pairs[1][1][0].readexactly(3) == b"BBB"
    await bridge.stop()

@pytest.mark.asyncio
async def test_data_socket_keepalive(monkeypatch):
    seen = []
    import pfrp.control.bridge as bridge_mod
    original = bridge_mod.enable_keepalive
    monkeypatch.setattr(bridge_mod, "enable_keepalive", lambda sock: seen.append(sock) or original(sock))
    bridge = EntryBridge()
    await bridge.start()
    await bridge.allow_tunnel("tun", "secret", 80, public_port=0)
    target_reader, _target_writer = await _hello(bridge.session_port(), "tun", "secret", "target")
    await target_reader.readline()
    _visitor_reader, visitor_writer = await asyncio.open_connection("127.0.0.1", bridge.public_port("tun"))
    opened = json.loads((await target_reader.readline()).decode("utf-8"))
    _data_reader, data_writer = await asyncio.open_connection("127.0.0.1", bridge.session_port())
    data_writer.write(dumps({"type": "data", "tunnel_id": "tun", "token": "secret", "stream_id": opened["stream_id"], "role": "target"}))
    await data_writer.drain()
    visitor_writer.write(b"Z")
    await visitor_writer.drain()
    assert seen
    await bridge.stop()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_bridge.py -v`

Expected: FAIL，`pfrp.control.bridge` 无法导入。

- [ ] **Step 3: Implement `dumps`, `loads`, `enable_keepalive`, and `EntryBridge`**

`bridge.py` 里写 `from pfrp.control.timing import setup_timeout`，让测试能补丁 `pfrp.control.bridge.setup_timeout`。`enable_keepalive` 设置 `SO_KEEPALIVE`，并在桥接开始时调用它。`allow_tunnel` 的 `public_port is None` 表示设备互访，不绑对外端口；`0` 表示绑 `0.0.0.0` 的临时端口。绑定失败抛 `ControlError(ERR_PORT_IN_USE)`。会话和数据连接都由 `session_port` 接入，按第一行的 `type` 区分。数据握手超过 `setup_timeout()` 时只关这一次访问。一端读到 EOF 时关闭另一端。`close_tunnel` 关闭该穿透的监听和已接入连接。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_bridge.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/protocol.py pfrp/control/bridge.py tests/test_control_bridge.py
git commit -m "feat: bridge one TCP stream per visit"
```

## Task 7: 网页 API 和指令队列

**Files:**
- Create: `pfrp/control/queue.py`
- Create: `pfrp/control/api.py`
- Test: `tests/test_control_api.py`

**Interfaces:**
- Consumes: `Registry`、`CommandQueue` 由本任务产生、`COOKIE_NAME`、`ERR_BAD_PASSPHRASE`、`ERR_SCAN_FAILED`
- Produces:
  - `class CommandQueue`，`push(self, device_id: str, command: dict) -> None`，`async wait(self, device_id: str, timeout: float) -> List[dict]`，`pending_count(self, device_id: str) -> int`
  - `class Api`，`__init__(self, registry: Registry, queue: CommandQueue) -> None`
  - `login(self, passphrase: str) -> Tuple[int, dict, Optional[str]]`，第三项是要写入 cookie 的令牌
  - `authorized(self, cookie_header: str) -> bool`
  - `cookie_header(self, token: str) -> str`
  - `devices(self) -> List[dict]`
  - `recheck(self, device_id: str) -> None`
  - `remove_device(self, device_id: str) -> None`
  - `report_scan(self, device_id: str, ports: List[int], error: str) -> None`
  - `ports(self, device_id: str) -> dict`
  - `create_tunnel(self, body: dict) -> Tuple[int, dict]`
  - `delete_tunnel(self, tunnel_id: str) -> None`
  - `change_passphrase(self, passphrase: str) -> None`
  - `tunnels(self) -> List[dict]`

`GET /api/tunnels` 的字典含 `status` 和 `last_error`，不含 `token`。`recheck` 向该设备 `push({"type":"bind_probe"})`。`delete_tunnel` 向入口、目标和访问方 `push({"type":"close_tunnel","tunnel_id":...})`。`ports` 不调用 `create_tunnel`。

- [ ] **Step 1: Write the failing test**

```python
import pytest
from pfrp.control.api import Api
from pfrp.control.constants import ERR_BAD_PASSPHRASE, ERR_SCAN_FAILED, REACH_PUBLIC
from pfrp.control.queue import CommandQueue
from pfrp.control.registry import Registry

def _api(tmp_path):
    registry = Registry.open(str(tmp_path))
    registry.set_passphrase("secret")
    return Api(registry, CommandQueue()), registry

def test_login_cookie_and_rejected_passphrase(tmp_path):
    api, _registry = _api(tmp_path)
    status, body, token = api.login("nope")
    assert status == 401 and body["error"] == ERR_BAD_PASSPHRASE and token is None
    status, _body, token = api.login("secret")
    header = api.cookie_header(token)
    assert "pfrp_session=" in header and "HttpOnly" in header and "SameSite=Lax" in header
    assert "Secure" not in header
    assert api.authorized(header) is True
    assert api.authorized("") is False

def test_ports_do_not_create_tunnels_and_tokens_stay_hidden(tmp_path):
    api, registry = _api(tmp_path)
    registry.register_device("entry", "entry", "secret", "203.0.113.10", [])
    registry.register_device("target", "target", "secret", "198.51.100.10", [])
    registry.set_reachability("entry", REACH_PUBLIC, "203.0.113.10")
    api.report_scan("target", [], ERR_SCAN_FAILED)
    assert api.ports("target") == {"ports": [], "error": ERR_SCAN_FAILED}
    status, body = api.create_tunnel({
        "kind": "public", "target_device_id": "target", "target_port": 80,
        "entry_device_id": "entry", "external_port": 8080,
    })
    assert status == 201 and "token" not in body
    assert registry.store.tunnels[body["tunnel_id"]].token
    assert api.tunnels()[0]["status"] == "建立中"
    api.delete_tunnel(body["tunnel_id"])
    assert body["tunnel_id"] not in registry.store.tunnels

@pytest.mark.asyncio
async def test_recheck_pushes_probe_command(tmp_path):
    api, registry = _api(tmp_path)
    registry.register_device("entry", "entry", "secret", "203.0.113.10", [])
    api.recheck("entry")
    commands = await api.queue.wait("entry", timeout=0.05)
    assert commands == [{"type": "bind_probe"}]
```

`Api` 把构造时的队列放在 `self.queue`。未登录的保护由 `authorized` 表达；HTTP 层在 Task 8 对未授权返回 401。

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_api.py -v`

Expected: FAIL，`pfrp.control.api` 无法导入。

- [ ] **Step 3: Implement `CommandQueue` and `Api`**

`wait` 在超时内没有指令时返回空列表，并取走已到达的指令。`pending_count` 返回尚未取走的指令数。`cookie_header` 生成 `pfrp_session=<token>; HttpOnly; SameSite=Lax; Path=/`。登录成功的令牌只留在 `Api` 内存里。`create_tunnel` 捕获 `ControlError` 时返回 `400` 和 `{"error": message}`，成功返回 `201` 和去掉 `token`、`session_port` 的隧道字典。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_api.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/queue.py pfrp/control/api.py tests/test_control_api.py
git commit -m "feat: add tunnel API and device command queue"
```

## Task 8: 总控 HTTP、登记和探测

**Files:**
- Create: `pfrp/control/server.py`
- Test: `tests/test_control_server.py`

**Interfaces:**
- Consumes: `Api`、`Registry`、`CommandQueue`、`classify_controller`、`initial_device_reachability`、`reachability_after_probe`、`announce_ip_for_device`、`announce_ip_for_controller`、`long_poll_timeout`、`probe_timeout`、`reprobe_interval`
- Produces:
  - `class ControllerServer`，`__init__(self, directory: str) -> None`
  - `async start(self, port: Optional[int] = None) -> None`，`None` 用存储里的端口，`0` 绑临时端口
  - `async stop(self) -> None`
  - `base_url(self) -> str`，形如 `http://127.0.0.1:<port>`
  - `async tcp_probe(ip: str, port: int) -> bool`，超时取 `probe_timeout()`
  - `add_observed_source(self, ip: str) -> None`，HTTP 登记时用来源地址调用它；测试也可以直接调用
  - 属性 `queue: CommandQueue`、`registry: Registry`

路由：`POST /api/login`、`GET /api/devices`、`POST /api/devices/<id>/recheck`、`DELETE /api/devices/<id>`、`GET /api/devices/<id>/ports`、`GET /api/tunnels`、`POST /api/tunnels`、`DELETE /api/tunnels/<id>`、`POST /api/passphrase`、`POST /api/agent/register`、`GET /api/agent/wait?device_id=`、`POST /api/agent/event`。除 login 和 agent register 外，浏览器路由要求 `authorized`。`register` 的 `observed_ip` 取这支 HTTP 连接的来源地址。

- [ ] **Step 1: Write the failing test**

```python
import json
import urllib.error
import urllib.request
import pytest
from pfrp.control.server import ControllerServer
from pfrp.control.timing import probe_timeout, reprobe_interval, long_poll_timeout

def _post(url, payload, cookie=""):
    request = urllib.request.Request(url, json.dumps(payload).encode("utf-8"), {"Content-Type": "application/json", "Cookie": cookie})
    with urllib.request.urlopen(request) as response:
        return response.status, json.loads(response.read().decode("utf-8")), response.headers

@pytest.mark.asyncio
async def test_register_observes_peer_ip_and_probe_timeout(tmp_path, monkeypatch):
    assert probe_timeout() == 3.0 and reprobe_interval() == 180.0 and long_poll_timeout() == 25.0
    monkeypatch.setattr("pfrp.control.server.probe_timeout", lambda: 0.2)
    server = ControllerServer(str(tmp_path))
    server.registry.set_passphrase("secret")
    await server.start(port=0)
    status, _body, headers = _post(server.base_url() + "/api/login", {"passphrase": "secret"})
    cookie = headers["Set-Cookie"]
    status, body, _headers = _post(server.base_url() + "/api/agent/register", {
        "device_id": "d1", "name": "office", "passphrase": "secret", "local_ips": ["127.0.0.1"],
    })
    assert status == 200
    device = server.registry.store.devices["d1"]
    assert device.observed_ip == "127.0.0.1"
    assert device.reachability == "仅内网可达"
    ok = await server.tcp_probe("127.0.0.1", 1)
    assert ok is False
    await server.stop()

@pytest.mark.asyncio
async def test_unauthorized_devices_are_401(tmp_path):
    server = ControllerServer(str(tmp_path))
    server.registry.set_passphrase("secret")
    await server.start(port=0)
    request = urllib.request.Request(server.base_url() + "/api/devices")
    with pytest.raises(urllib.error.HTTPError) as caught:
        urllib.request.urlopen(request)
    assert caught.value.code == 401
    await server.stop()

@pytest.mark.asyncio
async def test_wait_returns_pushed_command(tmp_path):
    server = ControllerServer(str(tmp_path))
    server.registry.set_passphrase("secret")
    await server.start(port=0)
    _post(server.base_url() + "/api/agent/register", {
        "device_id": "d1", "name": "office", "passphrase": "secret", "local_ips": [],
    })
    server.queue.push("d1", {"type": "bind_probe"})
    with urllib.request.urlopen(server.base_url() + "/api/agent/wait?device_id=d1") as response:
        body = json.loads(response.read().decode("utf-8"))
    assert body["commands"] == [{"type": "bind_probe"}]
    await server.stop()

@pytest.mark.asyncio
async def test_probe_event_marks_public_device(tmp_path, monkeypatch):
    async def allow_probe(self, ip, port):
        return True
    monkeypatch.setattr(ControllerServer, "tcp_probe", allow_probe)
    server = ControllerServer(str(tmp_path))
    server.registry.set_passphrase("secret")
    await server.start(port=0)
    server.add_observed_source("203.0.113.9")
    _post(server.base_url() + "/api/agent/register", {
        "device_id": "d1", "name": "office", "passphrase": "secret", "local_ips": [],
    })
    server.registry.store.devices["d1"].observed_ip = "203.0.113.8"
    status, body, _headers = _post(server.base_url() + "/api/agent/event", {
        "device_id": "d1", "event": {"type": "probe_bound", "port": 9},
    })
    assert status == 200
    device = server.registry.store.devices["d1"]
    assert device.reachability == "公网可达"
    assert device.announce_ip == "203.0.113.8"
    await server.stop()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_server.py -v`

Expected: FAIL，`pfrp.control.server` 无法导入。

- [ ] **Step 3: Implement `ControllerServer` and `tcp_probe`**

用 `asyncio.start_server` 解析 HTTP/1.1 请求行、头和 `Content-Length` 正文。`add_observed_source` 把来源地址记下来，`classify_controller` 用这份列表。来源地址全部不是公网 IPv4 时，新设备用 `initial_device_reachability` 的结果，不发探测。总控已是公网可达时，登记后 `push bind_probe`；收到 `{"type":"probe_bound","port":int}` 事件后调用 `tcp_probe(observed_ip, port)`，再用 `reachability_after_probe` 和 `announce_ip_for_device` 写回。探测失败不修改穿透状态。`start` 另起一个任务，循环 `await asyncio.sleep(reprobe_interval())`，仅当总控是公网可达时给每个在线设备再 `push bind_probe`。长轮询的超时参数使用 `long_poll_timeout()`，不要写死别的数字。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_server.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/server.py tests/test_control_server.py
git commit -m "feat: serve controller API and reachability probes"
```

## Task 9: 两种穿透的转发和恢复

**Files:**
- Create: `pfrp/control/agent.py`
- Test: `tests/test_control_flow.py`

**Interfaces:**
- Consumes: `ControllerServer`、`EntryBridge`、`scan_local_ports`、`retry_delay`、`setup_timeout`、`enable_keepalive`、Task 6 的会话帧、Task 7 的 `close_tunnel` / `bind_probe` 指令
- Produces:
  - `class DeviceAgent`，`__init__(self, directory: str) -> None`
  - `async start(self) -> None`，开始登记并循环领取指令，直到 `stop`
  - `async stop(self) -> None`

`open_tunnel` 的字段固定为 `tunnel_id`、`token`、`kind`、`role`、`external_port`、`accessor_port`、`target_port`、`entry_host`、`session_port`。入口角色调用 `EntryBridge.allow_tunnel`，成功后 `POST /api/agent/event` 发送 `{"type":"tunnel_ready","tunnel_id","session_port"}`；绑定失败发送 `{"type":"tunnel_error","tunnel_id","error":"端口已被占用"}`。目标角色拨 `entry_host:session_port`，收到 `open` 后连接 `127.0.0.1:target_port`，再拨一条 `data`。本地端口连不上时发送 `visit_failed`，错误句 `目标端口没有服务`。访问方角色只绑 `127.0.0.1:accessor_port`。收到 `{"type":"scan"}` 时调用 `scan_local_ports` 并上报 `scan_result`。会话断开后按 `retry_delay(attempt)` 重连，成功后把 attempt 归零。代理和入口桥调用 `pfrp.control.bridge.enable_keepalive` 这个模块属性，不把函数复制到别的全局名。

本任务的远程代理只走 HTTP。总控进程内的嵌入代理留到 Task 10。`POST /api/tunnels` 在 Task 7 的即时登记之后继续等待：端口冲突返回 400，且 registry 里没有这条记录；会话超时返回 201，`status` 为 `建立失败`；成功返回 201，`status` 为 `正常`。`entry_host` 使用入口的 `announce_ip`。设备再次 `register` 时，总控对它仍参与的穿透重新推送 `open_tunnel`。`GET /api/devices/<id>/ports` 先推送 `{"type":"scan"}`，等到 `scan_result` 或 `setup_timeout()`，再返回已上报的端口；这不会创建穿透。

- [ ] **Step 1: Write the failing test**

```python
import asyncio
import json
import socket
import urllib.error
import urllib.request
import pytest
import pfrp.control.addresses as addresses
import pfrp.control.bridge as bridge_mod
from pfrp.control.agent import DeviceAgent
from pfrp.control.constants import ERR_PORT_IN_USE, ERR_TARGET_DOWN, STATUS_ENTRY_DOWN, STATUS_FAILED, STATUS_UP
from pfrp.control.server import ControllerServer
from pfrp.control.state import AgentStore

def _patch(monkeypatch):
    real = addresses.is_public_ipv4
    monkeypatch.setattr(addresses, "is_public_ipv4", lambda ip: ip == "127.0.0.1" or real(ip))
    monkeypatch.setattr("pfrp.control.server.probe_timeout", lambda: 0.2)
    monkeypatch.setattr("pfrp.control.server.setup_timeout", lambda: 0.5)
    monkeypatch.setattr("pfrp.control.agent.setup_timeout", lambda: 0.5)
    monkeypatch.setattr("pfrp.control.agent.retry_delay", lambda attempt: 0.05)

async def _server(tmp_path, monkeypatch):
    _patch(monkeypatch)
    server = ControllerServer(str(tmp_path / "controller"))
    server.registry.set_passphrase("secret")
    await server.start(0)
    return server

async def _agent(tmp_path, server, device_id):
    directory = tmp_path / device_id
    store = AgentStore(str(directory))
    store.device_id = device_id
    store.name = device_id
    store.controller_url = server.base_url()
    store.role = "device"
    store.save()
    agent = DeviceAgent(str(directory))
    await agent.start()
    return agent

async def _wait(server, device_id, public=False):
    for _ in range(50):
        device = server.registry.store.devices.get(device_id)
        if device and device.online and (not public or (device.reachability == "公网可达" and device.announce_ip == "127.0.0.1")):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(device_id)

def _login(server):
    request = urllib.request.Request(server.base_url() + "/api/login", json.dumps({"passphrase": "secret"}).encode("utf-8"), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request) as response:
        return response.headers["Set-Cookie"].split(";", 1)[0]

def _call(server, method, path, payload, cookie):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(server.base_url() + path, data, {"Content-Type": "application/json", "Cookie": cookie}, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))

def _free_port():
    sock = socket.socket()
    sock.bind(("0.0.0.0", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

async def _echo(total):
    async def handle(reader, writer):
        buf = b""
        while len(buf) < total:
            chunk = await reader.read(total - len(buf))
            if not chunk:
                break
            buf += chunk
        writer.write(buf)
        await writer.drain()
        writer.close()
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]

@pytest.mark.asyncio
async def test_public_echo(tmp_path, monkeypatch):
    seen = []
    original = bridge_mod.enable_keepalive
    monkeypatch.setattr(bridge_mod, "enable_keepalive", lambda sock: seen.append(sock) or original(sock))
    server = await _server(tmp_path, monkeypatch)
    entry = await _agent(tmp_path, server, "entry")
    target = await _agent(tmp_path, server, "target")
    await _wait(server, "entry", public=True)
    await _wait(server, "target")
    echo, echo_port = await _echo(4)
    cookie = _login(server)
    external = _free_port()
    status, body = _call(server, "POST", "/api/tunnels", {
        "kind": "public", "target_device_id": "target", "target_port": echo_port,
        "entry_device_id": "entry", "external_port": external,
    }, cookie)
    assert status == 201 and body["status"] == STATUS_UP
    assert external != int(server.base_url().rsplit(":", 1)[1])
    reader, writer = await asyncio.open_connection("127.0.0.1", external)
    writer.write(b"ping")
    await writer.drain()
    assert await reader.readexactly(4) == b"ping"
    assert seen
    await entry.stop(); await target.stop(); echo.close(); await server.stop()

@pytest.mark.asyncio
async def test_private_echo_has_no_public_service_port(tmp_path, monkeypatch):
    server = await _server(tmp_path, monkeypatch)
    entry = await _agent(tmp_path, server, "entry")
    target = await _agent(tmp_path, server, "target")
    accessor = await _agent(tmp_path, server, "accessor")
    await _wait(server, "entry", public=True)
    await _wait(server, "target")
    await _wait(server, "accessor")
    echo, echo_port = await _echo(4)
    cookie = _login(server)
    local_port = _free_port()
    status, body = _call(server, "POST", "/api/tunnels", {
        "kind": "private", "target_device_id": "target", "target_port": echo_port,
        "entry_device_id": "entry", "accessor_device_id": "accessor", "accessor_port": local_port,
    }, cookie)
    assert status == 201 and body["status"] == STATUS_UP and body["external_port"] is None
    reader, writer = await asyncio.open_connection("127.0.0.1", local_port)
    writer.write(b"ping")
    await writer.drain()
    assert await reader.readexactly(4) == b"ping"
    await entry.stop(); await target.stop(); await accessor.stop(); echo.close(); await server.stop()
```

同文件再写这些测试，每个都自己启动和关闭 server、agent：

- `test_port_in_use`：先把一个套接字绑在 `0.0.0.0` 的某个端口上并保持打开，再创建使用该 `external_port` 的公网穿透。断言 HTTP 400，`error == ERR_PORT_IN_USE`，`GET /api/tunnels` 为空。
- `test_target_session_timeout`：只启动入口代理，不启动目标。断言响应 201 且 `status == STATUS_FAILED`，随后连接 `external_port` 失败。
- `test_accessor_session_timeout`：启动入口和目标，不启动访问方。私有穿透的结果同样是 `STATUS_FAILED`。
- `test_target_port_down_keeps_tunnel`：目标代理在线，但 `target_port` 没有服务。断言穿透 `status == STATUS_UP`。向对外端口写 `b"x"` 后读到 EOF，轮询 `GET /api/tunnels` 直到 `last_error == ERR_TARGET_DOWN`。
- `test_pause_mid_transfer`：echo 要收满 2048 字节才回写。客户端先写 1024 字节，`await asyncio.sleep(2)`，再写 1024 字节，然后读满 2048 字节。
- `test_session_recovers`：公网 echo 成功后 `await target.stop()`，再 `start` 同一个目标代理。新的连接仍能完成一次 4 字节 echo。
- `test_remove_entry_closes_listener`：公网 echo 成功后 `DELETE /api/devices/entry`。断言该穿透 `status == STATUS_ENTRY_DOWN`，再连接原来的对外端口失败。
- `test_restart_restores_tunnel`：公网 echo 成功后停止入口、目标和总控，保留 echo 服务器。用同一 `tmp_path / "controller"` 重新 `start`，再启动两个代理。不新建穿透，原 `external_port` 能再次完成 4 字节 echo。
- `test_scan_does_not_create_tunnel`：目标代理在线，并在 `127.0.0.1` 上监听一个端口。`GET /api/devices/target/ports` 的 `ports` 包含该端口，`GET /api/tunnels` 仍为空。

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_flow.py -v`

Expected: FAIL，`pfrp.control.agent` 无法导入。

- [ ] **Step 3: Implement `DeviceAgent` and the server-side tunnel dispatcher**

远程代理用 `GET /api/agent/wait` 和 `POST /api/agent/register`、`POST /api/agent/event`。建立超时调用 `pfrp.control.server.setup_timeout` 和 `pfrp.control.agent.setup_timeout` 这两个模块属性。代理重连调用 `pfrp.control.agent.retry_delay`。`GET /api/devices/<id>/ports` 和 `POST /api/tunnels` 按本任务接口里写的等待规则实现，不要让 Task 7 的 `Api.create_tunnel` 自己去等套接字。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_flow.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pfrp/control/agent.py pfrp/control/server.py tests/test_control_flow.py
git commit -m "feat: forward public and private tunnels"
```

## Task 10: 首次页面和启动入口

**Files:**
- Create: `pfrp/control/pages.py`
- Create: `pfrp/control/__main__.py`
- Modify: `pfrp/control/server.py`
- Test: `tests/test_control_pages.py`

**Interfaces:**
- Consumes: `normalize_controller_url`、`DEFAULT_WEB_PORT`、`ControllerStore`、`AgentStore`、`ControllerServer`、`DeviceAgent`、`setup_page`、`app_page`
- Produces:
  - `setup_page() -> str`
  - `app_page() -> str`
  - `class SetupServer`，`async start(self, directory: str) -> None`，`async stop(self) -> None`，`base_url(self) -> str`
  - `main() -> None`

`SetupServer` 只绑 `127.0.0.1` 端口 `0`，并把实际 URL 写到 `<数据目录>/setup.url`。`POST /setup/controller` 正文是 `port`、`passphrase`、`name`。`POST /setup/device` 正文是 `controller_url`、`passphrase`、`name`。来源地址不是 `127.0.0.1` 时返回 403。空口令返回 `口令不正确`。端口不在 1 到 65535 时返回 `端口无效`。设备地址先过 `normalize_controller_url`。成功后总控目录同时有 `controller.json` 和 `role=="controller"` 的 `agent.json`；纯设备目录只有 `agent.json`。修改已保存的总控地址时保留 `device_id`。`main` 在有 `controller.json` 时启动 `ControllerServer` 和内嵌代理，只有 `agent.json` 时只启动 `DeviceAgent`，都没有时启动 `SetupServer`。

- [ ] **Step 1: Write the failing test**

```python
import json
import urllib.error
import urllib.request
import pytest
from pfrp.control.constants import ERR_BAD_PASSPHRASE, ERR_BAD_URL
from pfrp.control.pages import app_page, setup_page
from pfrp.control.__main__ import SetupServer
from pfrp.control.state import AgentStore

def test_pages_name_both_tunnel_kinds():
    assert "给外网访问" in app_page()
    assert "设备互访" in app_page()
    assert "访问口令" in setup_page()

@pytest.mark.asyncio
async def test_setup_rejects_https_and_keeps_device_id(tmp_path):
    setup = SetupServer()
    await setup.start(str(tmp_path))
    assert setup.base_url().startswith("http://127.0.0.1:")
    bad = json.dumps({"controller_url": "https://203.0.113.10:7500", "passphrase": "secret", "name": "office"}).encode("utf-8")
    request = urllib.request.Request(setup.base_url() + "/setup/device", bad, {"Content-Type": "application/json"})
    with pytest.raises(urllib.error.HTTPError) as response:
        urllib.request.urlopen(request)
    assert response.value.code == 400
    assert ERR_BAD_URL.encode("utf-8") in response.value.read()
    assert not (tmp_path / "agent.json").exists()
    empty = json.dumps({"port": 7500, "passphrase": "", "name": "ctrl"}).encode("utf-8")
    request = urllib.request.Request(setup.base_url() + "/setup/controller", empty, {"Content-Type": "application/json"})
    with pytest.raises(urllib.error.HTTPError) as response:
        urllib.request.urlopen(request)
    assert ERR_BAD_PASSPHRASE.encode("utf-8") in response.value.read()
    ok = json.dumps({"controller_url": "http://203.0.113.10:7500/api", "passphrase": "secret", "name": "office"}).encode("utf-8")
    request = urllib.request.Request(setup.base_url() + "/setup/device", ok, {"Content-Type": "application/json"})
    with pytest.raises(urllib.error.HTTPError):
        urllib.request.urlopen(request)
    good = json.dumps({"controller_url": "http://203.0.113.10:7500", "passphrase": "secret", "name": "office"}).encode("utf-8")
    urllib.request.urlopen(urllib.request.Request(setup.base_url() + "/setup/device", good, {"Content-Type": "application/json"}))
    first = AgentStore.load(str(tmp_path)).device_id
    changed = json.dumps({"controller_url": "http://198.51.100.10:7500", "passphrase": "secret", "name": "office"}).encode("utf-8")
    urllib.request.urlopen(urllib.request.Request(setup.base_url() + "/setup/device", changed, {"Content-Type": "application/json"}))
    assert AgentStore.load(str(tmp_path)).device_id == first
    await setup.stop()

@pytest.mark.asyncio
async def test_controller_setup_writes_matching_ids(tmp_path):
    setup = SetupServer()
    await setup.start(str(tmp_path))
    body = json.dumps({"port": 7500, "passphrase": "secret", "name": "ctrl"}).encode("utf-8")
    urllib.request.urlopen(urllib.request.Request(setup.base_url() + "/setup/controller", body, {"Content-Type": "application/json"}))
    from pfrp.control.state import ControllerStore
    controller = ControllerStore.load(str(tmp_path))
    agent = AgentStore.load(str(tmp_path))
    assert controller.controller_device_id == agent.device_id
    assert agent.role == "controller"
    assert controller.listen_port == 7500
    await setup.stop()
```

`SetupServer` 在设备已经存在时，`POST /setup/device` 只更新 `controller_url`。页面测试不要求像素，只要两段 HTML 含上述中文。

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_control_pages.py -v`

Expected: FAIL，`pfrp.control.pages` 无法导入。

- [ ] **Step 3: Implement `setup_page`, `app_page`, `SetupServer`, and `main`**

管理页提供设备列表、重测、移除、两种穿透表单、端口推荐和关闭穿透，调用 Task 8 的路由。首次页面提供「总控」和「设备」两种提交。校验失败返回 400。`GET /` 在总控上返回 `app_page()`。导入 `pfrp.control.__main__` 时不要调用 `main`。`main` 读取 `data_dir()`。总控首次保存成功后，同一目录里有 `controller.json`，以及 `role=="controller"` 且 `device_id` 与 `controller_device_id` 相同的 `agent.json`。Task 10 再把这个内嵌代理接到总控的 `CommandQueue`，不要让它再去 HTTP 长轮询自己。

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_control_pages.py tests -q`

Expected: PASS。`tests -q` 含原有多通道测试。

- [ ] **Step 5: 在浏览器里走通页面**

启动 `python -m pfrp.control`，打开 `setup.url` 里的地址。用空口令提交，页面出现 `口令不正确`。用合法口令建成总控后，打开网页，确认能看到「给外网访问」和「设备互访」，并在选择目标设备后看到端口推荐或 `没能读到本机监听端口`。没有浏览器工具时，写明没做这一步，不要把它记成已通过。

- [ ] **Step 6: Commit**

```bash
git add pfrp/control/pages.py pfrp/control/__main__.py pfrp/control/server.py tests/test_control_pages.py
git commit -m "feat: add first-run and management pages"
```
