"""Tests for the file/folder forward extension (forward_extension_tcp.py).

Plain-message forwarding (the client-only command ``/forward_send_msg``)
is native to the TCP protocol and covered by
``test_forward_msg_attribution.py``; this file covers the extension's
file and folder transfers plus the internal message command's placement.
"""

import os
import socket
import threading
import time
from types import SimpleNamespace

import pytest

import PyFlow.forward_extension_tcp as fwd


class DummySocket:
    """Minimal socket stand-in capturing what the server would send."""

    def __init__(self):
        self.data = b""

    def sendall(self, data):
        self.data += data

    def close(self):
        pass


@pytest.fixture
def dummy_client_socket():
    return DummySocket()


def test_parse_items_and_addrs():
    tokens = ["111", "222", "('127.0.0.1', 3000)", "('127.0.0.1', 3500)", "plain"]
    items, addrs = fwd._parse_items_and_addrs(tokens)
    assert items == ["111", "222", "plain"]
    assert addrs == [("127.0.0.1", 3000), ("127.0.0.1", 3500)]


def test_parse_items_and_addrs_rejects_malformed_tuple():
    items, addrs = fwd._parse_items_and_addrs(["('127.0.0.1', notaport)", "ok"])
    assert items == ["('127.0.0.1', notaport)", "ok"]
    assert addrs == []


def test_message_forward_command_is_internal(client, server):
    """The single ``/forward_send_msg`` command is internal, not an extension:
    not registered in the handler registry on either side, and the deleted
    ``/send_msg_forward`` name is gone."""
    assert "/forward_send_msg" not in server._custom_handlers[0]
    assert "/forward_send_msg" not in server._custom_handlers[1]
    assert "/forward_send_msg" not in client._custom_handlers[0]
    assert "/forward_send_msg" not in client._custom_handlers[1]
    assert "/send_msg_forward" not in client._custom_handlers[1]
    assert "/send_msg_forward" not in client._custom_handlers[0]


def test_file_forward_relay_skips_unreachable(server, monkeypatch, capsys, tmp_path):
    fwd.server_instance = server
    server.running = True
    server.file_transfer_dir = str(tmp_path / "received")
    os.makedirs(server.file_transfer_dir, exist_ok=True)
    (tmp_path / "received" / "a.txt").write_text("data")
    reachable = ("127.0.0.1", 12345)
    server.clients[reachable] = {"socket": DummySocket()}
    pushed = []

    def record_push(message, file_folder_abspath=None):
        pushed.append(message)

    monkeypatch.setattr(server, "file_transfer_server_recv_client_start", record_push)
    fwd._forward_files_handler(
        None,
        None,
        "/forward_file \"a.txt\" \"('127.0.0.1', 12345)\" \"('127.0.0.1', 99999)\"",
    )
    assert len(pushed) == 1
    assert "/file" in pushed[0] and "a.txt" in pushed[0]
    assert "skipped" in capsys.readouterr().out


def test_folder_forward_relay_skips_unreachable(server, monkeypatch, capsys, tmp_path):
    fwd.server_instance = server
    server.running = True
    server.file_transfer_dir = str(tmp_path / "received")
    os.makedirs(server.file_transfer_dir, exist_ok=True)
    os.makedirs(tmp_path / "received" / "folder")
    reachable = ("127.0.0.1", 12345)
    server.clients[reachable] = {"socket": DummySocket()}
    pushed = []
    monkeypatch.setattr(
        server,
        "folder_file_transfer_server_recv_client_start",
        pushed.append,
    )
    fwd._forward_folders_handler(
        None,
        None,
        "/forward_folder \"folder\" \"('127.0.0.1', 12345)\" \"('127.0.0.1', 99999)\"",
    )
    assert len(pushed) == 1
    assert "/file_folder" in pushed[0] and "folder" in pushed[0]
    assert "skipped" in capsys.readouterr().out


def test_forward_commands_are_client_only(client, server):
    fwd.setup_client_commands(client)
    fwd.setup_server_commands(server)
    for cmd in fwd._FORWARD_COMMANDS:
        assert cmd in client._custom_handlers[1]  # client console triggers it
        assert cmd not in server._custom_handlers[1]  # server console rejects it
        assert cmd not in client._custom_handlers[0]
    for relay in ("/forward_file", "/forward_folder"):
        assert relay in server._custom_handlers[0]  # client requests reach it
    assert "/send_msg_forward" not in fwd._FORWARD_COMMANDS


def test_forward_messages_public_api(client, monkeypatch):
    """forward_messages() builds the /forward_send_msg relay request."""
    client.client_socket = DummySocket()
    sent = []
    monkeypatch.setattr(client, "send_message", lambda sock, msg: sent.append(msg) or True)
    client.forward_messages(["111", "222"], [("127.0.0.1", 3000)])
    assert len(sent) == 1
    request = sent[0]
    assert request.startswith("/forward_send_msg")
    assert "111" in request and "222" in request
    assert "127.0.0.1" in request and "3000" in request


def test_forward_send_msg_relays_to_reachable_only(server, dummy_client_socket, capsys):
    """The native /forward_send_msg relay wraps each message with the
    originator's address and records it under the originator's socket."""
    server.running = True
    origin_sock = DummySocket()
    origin_addr = ("127.0.0.1", 54321)
    reachable = ("127.0.0.1", 12345)
    server.clients[reachable] = {"socket": dummy_client_socket}
    server._handle_forward_send_msg(
        origin_sock,
        origin_addr,
        "/forward_send_msg \"111\" \"222\" \"('127.0.0.1', 12345)\" \"('127.0.0.1', 99999)\"",
    )
    sent = dummy_client_socket.data.decode("utf-8")
    assert "111" in sent and "222" in sent
    assert "99999" not in sent
    assert "skipped" in capsys.readouterr().out
    recorded = [e[0] for e in server.messages_dict.get(origin_sock, [])]
    assert "111" in recorded and "222" in recorded


def test_file_forward_uploads_then_asks_server(client, monkeypatch, tmp_path):
    fwd.setup_client_commands(client)
    client.client_socket = DummySocket()
    payload = tmp_path / "payload.txt"
    payload.write_text("data")
    uploaded = []
    sent = []
    monkeypatch.setattr(
        client,
        "file_transfer_client_recv_client_start",
        lambda message, file_folder_abspath=None: uploaded.append(message),
    )
    monkeypatch.setattr(client, "send_message", lambda sock, msg: sent.append(msg) or True)
    handler = client._custom_handlers[1]["/file_forward"]
    handler(None, None, '/file_forward "{}" "(\'127.0.0.1\', 3000)"'.format(payload))
    assert any("payload.txt" in m for m in uploaded)
    assert sent[-1].startswith("/forward_file")
    assert "payload.txt" in sent[-1]
    assert "127.0.0.1" in sent[-1] and "3000" in sent[-1]


def test_folder_forward_uploads_sync_then_asks_server(client, monkeypatch, tmp_path):
    fwd.setup_client_commands(client)
    client.client_socket = DummySocket()
    folder = tmp_path / "data_folder"
    (folder / "sub").mkdir(parents=True)
    (folder / "a.txt").write_text("a")
    (folder / "sub" / "b.txt").write_text("b")
    uploaded = []
    sent = []
    monkeypatch.setattr(
        client,
        "file_transfer_client_recv_client_start",
        lambda message, file_folder_abspath=None: uploaded.append(message),
    )
    monkeypatch.setattr(client, "send_message", lambda sock, msg: sent.append(msg) or True)
    handler = client._custom_handlers[1]["/folder_forward"]
    handler(None, None, '/folder_forward "{}" "(\'127.0.0.1\', 3000)"'.format(folder))
    # root and sub-directory commands, then one file transfer per file
    dir_commands = [m for m in sent if m.startswith("/file_folder") and "(" not in m]
    expected_dirs = 2  # root + sub dir
    assert len(dir_commands) == expected_dirs
    file_uploads = [m for m in uploaded if m.startswith("/file_folder")]
    assert len(file_uploads) == expected_dirs  # a.txt + sub/b.txt
    assert sent[-1].startswith("/forward_folder")
    assert "data_folder" in sent[-1]


def test_console_forward_without_items_or_addrs_prints_usage(client, monkeypatch, capsys):
    client.client_socket = DummySocket()
    sent = []
    monkeypatch.setattr(client, "send_message", lambda sock, msg: sent.append(msg) or True)
    client._console_forward_send_msg("/forward_send_msg")
    assert sent == []
    assert "need at least one message" in capsys.readouterr().out


def test_single_variant_rejects_multiple_items(client, monkeypatch, capsys):
    fwd.setup_client_commands(client)
    client.client_socket = DummySocket()
    sent = []
    monkeypatch.setattr(client, "send_message", lambda sock, msg: sent.append(msg) or True)
    handler = client._custom_handlers[1]["/file_forward"]
    handler(None, None, '/file_forward "a.txt" "b.txt" "(\'127.0.0.1\', 3000)"')
    assert sent == []
    assert "exactly one item" in capsys.readouterr().out


def test_server_setup_creates_instance(monkeypatch, tmp_path, capsys):
    """server_setup() builds a real server instance with relays registered."""
    original = fwd.connect_tcp.TCP_Server_Base
    created = []

    def fake_server(**kwargs):
        created.append(kwargs)
        s = original(**kwargs)
        s.start_TCP_Server = lambda: None
        return s

    monkeypatch.setattr(fwd.connect_tcp, "TCP_Server_Base", fake_server)
    fwd.server_setup()
    assert created and created[0]["is_extend_command"] is True
    assert fwd.server_instance is not None
    for relay in ("/forward_file", "/forward_folder"):
        assert relay in fwd.server_instance._custom_handlers[0]
    # the message relay is internal and needs no extension setup
    assert "/forward_send_msg" not in fwd.server_instance._custom_handlers[0]


def test_server_dispatches_forward_send_msg_internally(server, capsys):
    """/forward_send_msg arriving over the wire is routed by handle_command's
    built-in chain (no extension registration involved)."""
    from test_util import wait_until

    server.running = True
    dest = DummySocket()
    reachable = ("127.0.0.1", 12345)
    server.clients[reachable] = {"socket": dest}
    origin_sock = DummySocket()
    try:
        ack = server.handle_command(
            origin_sock,
            ("127.0.0.1", 54321),
            "/forward_send_msg \"111\" \"('127.0.0.1', 12345)\" \"('127.0.0.1', 99999)\"",
        )
        assert ack == "Command received, processing in background.\n"
        assert wait_until(lambda: b"111" in dest.data), f"never relayed: {dest.data!r}"
        assert "skipped" in capsys.readouterr().out
    finally:
        server.running = False


def test_server_setup_with_existing_instance_threaded(monkeypatch):
    """server_setup(instance=..., is_input_command_in_console=False) registers
    the relays on the given instance and really starts it in a background
    thread: the accept loop is live and accepts a plain connection."""
    with socket.socket() as s0:
        s0.bind(("127.0.0.1", 0))
        port = s0.getsockname()[1]
    s = fwd.connect_tcp.TCP_Server_Base(
        host="127.0.0.1",
        port=port,
        is_extend_command=True,
        is_input_command_in_console=False,
        is_enable_encrypto=False,
    )
    try:
        fwd.server_setup(instance=s, is_input_command_in_console=False)
        assert fwd.server_instance is s
        for relay in ("/forward_file", "/forward_folder"):
            assert relay in s._custom_handlers[0]
        for _ in range(50):
            if s.running:
                break
            time.sleep(0.02)
        assert s.running  # the background thread really started the server
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.settimeout(2)
        probe.connect(("127.0.0.1", port))
        probe.close()
    finally:
        s.stop()
