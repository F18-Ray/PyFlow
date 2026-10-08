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
