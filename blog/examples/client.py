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
