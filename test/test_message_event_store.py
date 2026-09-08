"""Tests for the inbound message/event stores (messages_dict / events_dict).

External code reads these dicts (or the JSON logs they flush into) instead
of registering message/file listeners. The dicts are keyed by the sender's
socket; each value is a list of [content, timestamp] pairs. When a store's
total size reaches max_dict_size (64 KiB) it is flushed to its JSON log and
cleared.
"""

import json
import os
import threading

import pytest

from test_util import server_ready, wait_until

from PyFlow.network_api.connect_tcp import TCP_Client_Base, TCP_Server_Base

_PORT_COUNTER = 65510


def _next_port():
    global _PORT_COUNTER
    _PORT_COUNTER += 1
    return _PORT_COUNTER


@pytest.fixture
def pair(tmp_path):
    port = _next_port()
    server = TCP_Server_Base(
        host="127.0.0.1",
        port=port,
        is_extend_command=True,
        is_input_command_in_console=False,
        is_enable_encrypto=False,
    )
    server.messages_log_file = str(tmp_path / "server_messages_log.json")
    server.events_log_file = str(tmp_path / "server_events_log.json")
    threading.Thread(target=server.start_TCP_Server, daemon=True).start()
    assert server_ready(server), "server did not start"
    client = TCP_Client_Base(
        host="127.0.0.1",
        port=port,
        client_host="127.0.0.1",
        is_extend_command=True,
        is_input_command_in_console=False,
        is_enable_encrypto=False,
    )
    client.messages_log_file = str(tmp_path / "client_messages_log.json")
    client.events_log_file = str(tmp_path / "client_events_log.json")
    assert client.connect()
    yield server, client
    client.close()
    server.stop()


def _server_sock(server, client):
    """The server-side socket for a connected client."""
    return server.clients[client.client_socket.getsockname()]["socket"]


def _client_addr_key(client):
    """The JSON-log key for the server's store (the client's address)."""
    ip, port = client.client_socket.getsockname()
    return f"{ip}:{port}"


def test_server_records_client_message(pair):
    server, client = pair
    client.send_message(client.client_socket, "hello from client")
    assert wait_until(
        lambda: any(
            e[0] == "hello from client"
            for e in server.messages_dict.get(_server_sock(server, client), [])
        )
    ), "message not recorded"
    entries = server.messages_dict[_server_sock(server, client)]
    assert any(e[0] == "hello from client" for e in entries)
    assert all(e[1] for e in entries)  # every entry carries a timestamp


def test_server_records_client_event(pair):
    server, client = pair
    client.send_message(client.client_socket, "/time")
    assert wait_until(
        lambda: any(
            e[0] == "/time" for e in server.events_dict.get(_server_sock(server, client), [])
        )
    ), "event not recorded"
    entries = server.events_dict[_server_sock(server, client)]
    assert any(e[0] == "/time" for e in entries)
    assert all(e[1] for e in entries)


def test_client_records_server_message(pair):
    server, client = pair
    server.send_message(_server_sock(server, client), "hello from server")
    assert wait_until(
        lambda: any(
            e[0] == "hello from server"
            for e in client.messages_dict.get(client.client_socket, [])
        )
    ), "message not recorded"
    entries = client.messages_dict[client.client_socket]
    assert any(e[0] == "hello from server" for e in entries)


def test_client_records_server_event(pair):
    server, client = pair
    server.send_message(_server_sock(server, client), "/custom_cmd arg1")
    assert wait_until(
        lambda: any(
            e[0] == "/custom_cmd arg1"
            for e in client.events_dict.get(client.client_socket, [])
        )
    ), "event not recorded"
    entries = client.events_dict[client.client_socket]
    assert any(e[0] == "/custom_cmd arg1" for e in entries)


def test_server_records_file_transfer_event(pair, tmp_path):
    """A /file command over the wire is recorded as an event with the
    transfer's completion timestamp."""
    server, client = pair
    server_recv = tmp_path / "server_recv"
    server_recv.mkdir()
    server.file_transfer_dir = str(server_recv)
    src = tmp_path / "upload.bin"
    payload = os.urandom(8192)
    src.write_bytes(payload)

    client.file_transfer_client_recv_client_start_thread(f"/file {src}", None)
    assert wait_until(lambda: any(server_recv.iterdir()), timeout=10), "file not received"
    assert list(server_recv.iterdir())[0].read_bytes() == payload

    server_sock = _server_sock(server, client)
    assert wait_until(
        lambda: any(e[0].startswith("/file ") for e in server.events_dict.get(server_sock, []))
    ), f"no /file event recorded: {server.events_dict.get(server_sock)}"
    file_events = [e for e in server.events_dict[server_sock] if e[0].startswith("/file ")]
    assert file_events[-1][1]  # completion timestamp present


def test_update_event_timestamp_refreshes_entry(pair):
    """The completion-time refresh replaces the receipt timestamp of the
    matching event entry."""
    server, client = pair
    server_sock = _server_sock(server, client)
    server._record_event(server_sock, "/file a.txt 0")
    server._update_event_timestamp(server_sock, "/file a.txt 0", "2026-01-01 00:00:00")
    assert server.events_dict[server_sock][-1] == ["/file a.txt 0", "2026-01-01 00:00:00"]


def test_flush_writes_json_and_clears_dict(pair, tmp_path):
    """When a store reaches max_dict_size it is written to its JSON log and
    cleared; the log accumulates across flushes."""
    server, client = pair
    server.max_dict_size = 200
    key = _client_addr_key(client)
    for i in range(30):
        client.send_message(client.client_socket, f"msg {i} " + "x" * 40)

    def all_flushed():
        if not os.path.exists(server.messages_log_file):
            return False
        with open(server.messages_log_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        in_file = len(data.get(key, []))
        in_dict = len(server.messages_dict.get(_server_sock(server, client), []))
        return in_file + in_dict >= 30

    assert wait_until(all_flushed, timeout=10), "messages were not flushed to the JSON log"
    with open(server.messages_log_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    in_dict = len(server.messages_dict.get(_server_sock(server, client), []))
    assert len(data[key]) + in_dict == 30
    assert all(len(e) == 2 and e[1] for e in data[key])
    assert server._messages_dict_size < server.max_dict_size


def test_flush_on_close_persists_remaining(pair, tmp_path):
    """close()/stop() flush whatever is still buffered so nothing is lost."""
    server, client = pair
    client.send_message(client.client_socket, "persist me")
    assert wait_until(
        lambda: any(
            e[0] == "persist me"
            for e in server.messages_dict.get(_server_sock(server, client), [])
        )
    ), "message not recorded"
    server.stop()
    with open(server.messages_log_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    key = _client_addr_key(client)
    assert any(e[0] == "persist me" for e in data.get(key, []))


def test_splice_event_command(pair):
    """The wire command is kept verbatim; a missing command is spliced from
    the available parts (forwarded transfers)."""
    server, client = pair
    assert server._splice_event_command("/file a.txt 1") == "/file a.txt 1"
    assert server._splice_event_command("", fname="a.txt") == "/file a.txt"
    assert (
        server._splice_event_command("", kind="folder", rel_dir="d", fname="a.txt")
        == "/file_folder d a.txt"
    )
    assert server._splice_event_command("", kind="folder", fname="a.txt") == "/file_folder a.txt"
    assert server._splice_event_command("") == "/unknown"
