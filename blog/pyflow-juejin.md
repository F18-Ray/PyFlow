# 50 行 Python 代码，跑通一个"默认加密"的消息 / 文件传输 / 远程命令通道

前段时间做内网自动化，需要几台机器之间互传配置文件，顺带跑几条远程命令，还要把收到的文件转发给别的机器。起初觉得用标准库 `socket` 直写就行，真写起来才发现问题远不止"发个字节流"：

- 加密怎么办？用 `ssl` 套一层，证书分发和信任又变成新麻烦；
- 文件要分片、要确认、要断线重连，还要和消息复用同一个通道；
- 消息、文件、命令三种语义混在一个连接里；
- 后面想加"把文件转发给多个目标"，`socket` 原语层根本不够用。

FTP 太重而且明文；SCP 只管传文件；HTTP 自建得自己补加密和鉴权。找了一圈，没有把"加密消息 + 文件传输 + 扩展接口"收敛到一套 API 的库，就自己写了一个：PyFlow。下面用一个 50 行上下、**实测可跑**的示例，把它最核心的用法过一遍。

## 三个关键设计（决定了它怎么用）

**1. 加密默认开启，密钥交换走 TOFU。**

`TCP_Server_Base` / `TCP_Client_Base` 默认 `is_enable_encrypto=True`：连接建立后先交换 RSA 公钥——优先用 `~/.ssh/id_rsa`，没有就在 `.Flow/pvt_key/` 下生成一对 2048 位密钥——之后控制通道上的每条消息都加密。

对端公钥按 `(ip, port)` 记在 `.Flow/pub_key/pub_key.json`，存的是 PEM 文本的 SHA-256。这就是 TOFU（Trust On First Use）：

- 第一次连接默认信任，直接记下；
- 同一个地址再连时公钥变了 → **拒绝连接**（防换钥中间人）；
- 公钥没变、只是换了地址/端口 → 接受，并按新地址重新登记。

收益是零配置：不需要 CA、不需要手动发证书；代价是首次连接必须可信。内网工具、设备配对、临时协作很合适，公网严格场景要换证书或预共享密钥。

**2. 防重放不是可选项，还要有断路器。**

加密只解决"别人看不到内容"，不解决"别人把旧消息重放一遍"。PyFlow 的加密线格式是：

```text
<nonce>|<seq>|<ciphertext>
```

16 字节随机 session nonce + 每条消息递增的序号 + RSA-OAEP 密文（超长自动分块再拼接）。nonce 不符、序号不等于期望值（重放/乱序），都会被静默丢弃——不是报警，是直接不进业务逻辑。

另一个坑是"解密失败风暴"：密钥不匹配或有人灌垃圾数据时，双方会不停重协商。所以加了个断路器——**连续解密失败 3 次**（`MAX_DECODE_FAILURES`）就先丢弃加密状态、重载本端密钥、重新交换公钥；如果还失败，直接断开连接，避免两个进程把 CPU 打满。

**3. 扩展协议是一等公民。**

远程命令、文件转发都不写死在 TCP 层里，而是通过 `register_command(name, handler, where_to_run=...)` 注册的扩展命令。随库内置两个：

- `command_control_extension_tcp.py`：服务端把命令下发给指定客户端执行，执行结果（JSON 日志）回传并合并；
- `forward_extension_tcp.py`：文件/文件夹"先上传到服务端、再推送给多个目标"。

你也可以照这套接口写自己的协议（数据库查询、日志收集……）挂到同一个控制通道上。

## 完整示例：加密消息 + 文件 + 远程命令

先安装。注意：PyPI 上只发布源码包，安装时会在本地编译 C 加密库，所以需要 C 编译器和 OpenSSL 开发头文件（Python ≥ 3.10）：

```bash
# Debian / Ubuntu
sudo apt-get install -y build-essential libssl-dev
pip install pyflow-net
```

`server.py`：注册扩展、打印收到的消息和文件、阻塞运行。

```python
"""PyFlow demo server: encrypted messages/files, remote commands, forwarding."""

from PyFlow.command_control_extension_tcp import setup_server_commands
from PyFlow.forward_extension_tcp import setup_server_commands as setup_forward_commands
from PyFlow.network_api.connect_tcp import TCP_Server_Base


def on_message(client_id, message):
    """Print every plain message a client sends (forwarded ones included)."""
    print(f"[server] message from {client_id}: {message}")


def on_file(client_id, full_path, name, size, command):
    """Print every uploaded file, skipping the crypto key-exchange pushes."""
    if command.startswith("/crypto"):
        return
    print(f"[server] file from {client_id}: {name} ({size} B) -> {full_path}")


server = TCP_Server_Base(host="127.0.0.1", port=12345, is_extend_command=True)
server.add_message_listener(on_message)
server.add_file_listener(on_file)
setup_server_commands(server)  # 远程命令扩展：控制台 /command 下发 + 日志回传
setup_forward_commands(server)  # 转发扩展：接收客户端的 /forward_file、/forward_folder 请求
server.start_TCP_Server()  # 阻塞运行；控制台可输入 /command、/stop
```

`client.py`：连上之后发一条加密消息、传一个文件，然后保持在线等待服务端下发命令。

```python
"""PyFlow demo client: encrypted message, file transfer, remote command handling."""

import os
import threading
import time

from PyFlow.command_control_extension_tcp import setup_client_commands
from PyFlow.network_api.connect_tcp import TCP_Client_Base

client = TCP_Client_Base(
    host="127.0.0.1",
    port=12345,
    client_port=23456,  # 固定本地端口，便于服务端 /command 指名
    is_input_command_in_console=False,  # 不进入交互模式
    is_extend_command=True,  # 先注册扩展，再手动启动
    is_wait_server=False,
)
setup_client_commands(client)  # 响应服务端下发的 /command：本机执行并把 JSON 日志回传

threading.Thread(target=client.start_TCP_client, daemon=True).start()
for _ in range(100):  # 等待连接建立（最多 10 秒）
    if client.running:
        break
    time.sleep(0.1)
time.sleep(1)  # 等 RSA 公钥交换完成，再开始发消息

client.send_message_to_server('/send_msg "hello from PyFlow"')  # 1. 加密消息
client.file_transfer_client_recv_client_start_thread(  # 2. 传文件（走独立传输连接）
    f'/file "{os.path.abspath("payload.txt")}"'
)

time.sleep(20)  # 保持在线，等待服务端控制台下发 /command
client.close()
```

两个文件加起来 58 行（含注释和空行；去掉注释和空行约 45 行）。再准备一个待传的文件，然后把三个文件放在同一目录，开两个终端：

```bash
echo "PyFlow demo payload." > payload.txt
python server.py   # 终端 1
python client.py   # 终端 2
```

服务端实际输出（实测，省略了密钥交换的过程日志）：

```text
TCP server deployed on 127.0.0.1:12345
input '/stop' to stop the server

crypto: accepted public key from 127.0.0.1:23456 (known key, endpoint updated)  # 首次运行会显示 first connection (TOFU)
hello from PyFlow
[server] message from 127.0.0.1:23456: hello from PyFlow
[18:59:05] 127.0.0.1:23456: hello from PyFlow
[server] file from 127.0.0.1:23456: payload.txt (21 B) -> .../network_api/received_files/payload.txt
file payload.txt received from 127.0.0.1:23456, size 21 bytes
```

消息走的是加密控制通道；文件落在服务端的 `PyFlow/network_api/received_files/` 目录。

然后是"远程命令"。趁客户端还在线，在**服务端控制台**（终端 1）输入：

```text
/command "whoami" "('127.0.0.1', 23456)"
```

客户端会执行 `whoami`，把结果写成 JSON 日志回传；服务端在 `PyFlow/logs/merged_logs.json` 里合并出：

```json
{
  "whoami": [
    {
      "timestamp": "2026-10-08T19:00:30.467818",
      "command": "whoami",
      "cmd_id": "1",
      "client_id": "0",
      "client": "('127.0.0.1', 23456)",
      "from": "127.0.0.1:23456",
      "output": "rayxu",
      "error": "",
      "returncode": 0
    }
  ]
}
```

注意方向：**下发命令的是服务端控制台，执行命令的是客户端**；`"('127.0.0.1', 23456)"` 是目标客户端地址（服务端控制台 `/clients` 可以列出）。

> 小提示：如果服务端和客户端在同一台机器、同一份代码目录里跑，日志目录是同一个，合并后可能出现重复条目；分机器或分目录部署不会有这个问题。

## 转发：消息一条路，文件两条路

**消息转发是 TCP 层原生能力，不用装扩展。** 在客户端控制台：

```text
/forward_send_msg "hello via relay" "('127.0.0.1', 23457)"
```

服务端会把消息包成 `/send_msg_from` 信封转发给目标客户端，接收端能看出原始发送者是谁（实测接收端打印：`message from 127.0.0.1:23456: hello via relay`）。代码里等价写法就一行：

```python
client.forward_messages(["hello via relay"], [("127.0.0.1", 23457)])
```

**文件/文件夹转发有两套实现，按场景选：**

- **原生（不落盘）**：客户端控制台 `/forward_file "payload.txt" "('127.0.0.1', 23457)"`（文件夹用 `/forward_folder`）。文件流经服务端内存直接中继给所有目标，服务端不碰磁盘，还带暂停/恢复流控；
- **扩展（上传-推送）**：`/file_forward`、`/multiple_file_forward`、`/folder_forward`、`/multiple_folder_forward`。先把文件上传到服务端的传输目录，再由服务端推送给每个目标，适合"服务端当分发站"的场景。

两者的注册要求不一样：原生 `/forward_file`、`/forward_folder` 是 TCP 层内置的（客户端控制台直接用，服务端无需任何注册）；扩展的 `/file_forward` 系列需要服务端注册中继处理器（`setup_forward_commands(server)`，就是上面 server.py 里那一行），客户端侧也要注册——用启动器会自动加载，手写代码时加一行 `forward_extension_tcp.setup_client_commands(client)` 即可。

## 不想写代码：启动器 + 浏览器 UI

- `python -m PyFlow`：交互式启动器，问几个问题生成 `setup.json` 并起实例；也可以命令行直接来：`python -m PyFlow --type 0 --setup_addr_port 127.0.0.1:12345`；扩展文件用 `--add path/to/my_extension.py` 注册后，之后每次启动都会自动加载（注册表在 `PyFlow/added_extensions.json`）；
- `python -m PyFlow --web_server` / `--web_client`（或直接跑 `PyFlow/transfer_web/setup_server.py`）：Flask 包出来的浏览器 UI。服务端首启的管理员是 `admin/admin`，登录时会弹警告提醒先改密码再暴露到公网；注册、改密码、客户端登录走邮箱验证码（管理员先在配置页填好 SMTP）；客户端登录后按联系人列表互传消息、文件、文件夹，也可以把文件转发给其他已连接的客户端。`/api/server_info` 是公开接口，供客户端查询 TCP 地址和加密开关。
- 顺手还带了些常用件：UDP 端点（`send` / `broadcast` / `listen`，适合局域网广播与发现）、`add_message_listener` / `add_file_listener` 事件回调、消息与事件自动落盘（`.Flow/messages_log.json`、`.Flow/events_log.json`）、实例间端口分配池。

## 加密实现细节与边界（诚实版）

- C 库 `libcrypto_api` 用 OpenSSL 实现，通过 ctypes 加载，暴露 `pf_*` 的 C API：RSA-OAEP（SHA-256）、ECDH（P-256/384/521）、HKDF-SHA256、AES-256-GCM，也可以脱离 Python 直接在 C/CMake 项目里用；
- TCP 控制通道目前是 **RSA-OAEP 逐消息加密**：默认 2048 位密钥下每块明文上限 190 字节，超长消息自动分块再 base64 拼接。所以控制通道适合消息、命令这类中小负载；
- **文件不走控制通道**：文件/文件夹传输会另开临时 TCP 连接（端口从分配池里取），分块收发、不阻塞消息。要注意：RSA 逐消息加密只覆盖控制通道，文件数据在独立连接上是明文字节流（只做分块与错误校验）——敏感文件请先自行加密，或把整个通道放进 VPN/内网；
- 每次（重）连接都会重新交换公钥并做一次 TOFU 校验，这也是断路器存在的意义：让"重连-换钥"这件事有明确的上限。

实现里有两个值得单独说的点：

- 序号必须严格连续，所以"分配序号"和"写进 socket"必须在同一把锁里完成——否则两个线程并发发送时帧会写反，对端一律按乱序丢弃。这也是这条加密通道的一个隐含约束：发送是串行的。
- 收到公钥推送（`/crypto_pub_key`）时会检查这条连接确实处在握手中，否则直接忽略——不然任何人都能往 TOFU 注册表里塞公钥。

## FAQ

**Q：TOFU 够安全吗？**

TOFU 的前提是"第一次连接是可信的"。它能防"同一个地址换了公钥"的中间人，防不了"第一次就被冒充"。要严格首次身份验证（比如公网服务端），请上证书或预共享密钥。适用边界在仓库 `docs/source/Crypto/Crypto.rst` 里写得很清楚。

**Q：Windows 上安装编译失败？**

装两样东西：Visual Studio Build Tools（C++ 工作负载）和 Win64 OpenSSL 安装器（选非 Light 版，Light 不含开发头文件）。OpenSSL 装在非默认路径时，设置环境变量 `OPENSSL_ROOT_DIR` 指过去。

**Q：我只想传文件，能关掉加密吗？**

可以，创建实例时传 `is_enable_encrypto=False`（默认是 `True`）。本地回环测试无所谓，只要跨机器，建议保持默认开启。

**Q：和 `socket` + `ssl` 自建相比，我图什么？**

图一套 API 覆盖"消息/文件/文件夹/转发/远程命令 + 加密 + TOFU + 防重放 + 端口分配 + 事件监听"，外加启动器和 Web UI。代价是：安装时要本地编译 C 库（没有预编译 wheel），许可是 GPL-3.0（商用请先确认许可兼容性）。

## 项目信息

- GitHub：`F18-Maverick/PyFlow`
- 安装：`pip install pyflow-net`（当前 1.0.1，Python ≥ 3.10）
- 文档：https://pyflow-net.readthedocs.io （仓库 `docs/source/` 为 Sphinx 源，含中/日/韩/俄/繁体多语言目录）
- CI：Ubuntu + Windows，Python 3.10–3.14（含免 GIL 的 3.14 构建）

如果这个项目的思路对你有帮助，或者你正好需要一个能统一处理"加密消息 + 文件传输 + 扩展协议"的 Python 库，欢迎 star 支持；使用上的问题直接提 Issue，会尽快回。
