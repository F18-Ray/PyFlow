"""Tests for native message forwarding and sender attribution.

Plain-message forwarding is native to the TCP protocol: the client-only
console command ``/forward_send_msg`` asks the server (which runs the
same-named relay) to push messages to other connected clients. The server
sends each message wrapped in a ``/send_msg_from <addr> <payload>``
envelope, so the receiving client can attribute it to the sender's
conversation instead of surfacing under every sidebar entry.

Records of a forwarded message belong to the originator on both ends:

- the server records the payload under the originator's socket, as if the
  originator had sent it directly;
- the receiving client cannot hold the originator's socket (it only has
  its own link to the server), so it records the payload and the envelope
  event under the originator's ``"ip:port"`` address string.

Message listeners on both classes share one contract,
``listener(sender_id, message)``; on the client ``sender_id`` is ``None``
for direct pushes from the server.
"""

import shlex
import threading
import time

import pytest

from test_util import server_ready, wait_until

from PyFlow.network_api.connect_tcp import (
    TCP_Client_Base,
    TCP_Server_Base,
    parse_forward_originator,
    parse_forwarded_message,
)

_PORT_COUNTER = 65500


def _next_port():
    global _PORT_COUNTER
    _PORT_COUNTER += 1
    return _PORT_COUNTER


@pytest.fixture
def trio():
    """Server + two clients. Message forwarding is native, no extension setup."""
    port = _next_port()
    server = TCP_Server_Base(
        host="127.0.0.1",
        port=port,
        is_extend_command=True,
        is_input_command_in_console=False,
        is_enable_encrypto=False,
    )
    threading.Thread(target=server.start_TCP_Server, daemon=True).start()
    assert server_ready(server), "server did not start"

    clients = []
    for _ in range(2):
        c = TCP_Client_Base(
            host="127.0.0.1",
            port=port,
            client_host="127.0.0.1",
            is_extend_command=True,
            is_input_command_in_console=False,
            is_enable_encrypto=False,
        )
        assert c.connect()
        clients.append(c)

    yield server, *clients
    for c in clients:
        c.close()
    server.stop()


def _addr(client):
    return client.client_socket.getsockname()


def _client_key(client):
    ip, port = client.client_socket.getsockname()
    return f"{ip}:{port}"


def _server_sock(server, client):
    """The server-side socket object for a connected client."""
    return server.clients[client.client_socket.getsockname()]["socket"]


def _forward(sender, payload, dest):
    """Mimic the web backend: forward via the native public API."""
    sender.forward_messages([payload], [_addr(dest)])


def _envelope(addr, payload):
    # the exact wire form produced by the server relay
    return f"/send_msg_from {shlex.quote(repr(addr))} {shlex.quote(payload)}"


def test_parse_forwarded_message():
    assert parse_forwarded_message(_envelope(("127.0.0.1", 3000), "hi")) == (
        "127.0.0.1:3000",
        "hi",
    )
    assert parse_forwarded_message(_envelope(("127.0.0.1", 3000), "hello world")) == (
        "127.0.0.1:3000",
        "hello world",
    )
    assert parse_forwarded_message(_envelope(("::1", 3000), "x")) == ("::1:3000", "x")
    assert parse_forwarded_message(_envelope(("127.0.0.1", 3000), "it's fine")) == (
        "127.0.0.1:3000",
        "it's fine",
    )
    # malformed or unrelated lines are not envelopes
    assert parse_forwarded_message("/send_msg_from nope hi") is None
    assert parse_forwarded_message("/send_msg_from") is None
    assert parse_forwarded_message("/other ('127.0.0.1', 3000) hi") is None
    assert parse_forwarded_message("plain message") is None


def test_forward_commands_are_internal():
    """Message forwarding is internal, not an extension. The single command
    ``/forward_send_msg`` is NOT registered in the extension handler registry
    on either side, and the deleted ``/send_msg_forward`` name is gone."""
    server = TCP_Server_Base(
        host="127.0.0.1", port=_next_port(), is_extend_command=True, is_enable_encrypto=False
    )
    client = TCP_Client_Base(
        host="127.0.0.1",
        port=server.port,
        client_host="127.0.0.1",
        is_extend_command=True,
        is_enable_encrypto=False,
    )
    try:
        assert "/forward_send_msg" not in server._custom_handlers[0]
        assert "/forward_send_msg" not in server._custom_handlers[1]
        assert "/forward_send_msg" not in client._custom_handlers[0]
        assert "/forward_send_msg" not in client._custom_handlers[1]
        assert "/send_msg_forward" not in server._custom_handlers[0]
        assert "/send_msg_forward" not in server._custom_handlers[1]
        assert "/send_msg_forward" not in client._custom_handlers[0]
        assert "/send_msg_forward" not in client._custom_handlers[1]
    finally:
        client.close()
        server.stop()


def test_listener_gets_sender_for_forward_and_none_for_direct_push(trio):
    """The merged message listener receives (sender_id, message): the
    forwarding client's id for forwards, None for direct server pushes."""
    server, a, b = trio
    received = []
    b.add_message_listener(lambda sender, text: received.append((sender, text)))
    _forward(a, "hello", b)
    server.send_message(_server_sock(server, b), "direct from server")
    assert wait_until(lambda: len(received) == 2), f"messages never arrived {received=}"
    assert (_client_key(a), "hello") in received
    assert (None, "direct from server") in received


def test_forwarded_message_reaches_destination_as_plain(trio):
    """The payload reaches the destination's listener and message store,
    exactly as a plain inbound message would."""
    server, a, b = trio
    received = []
    b.add_message_listener(lambda sender, text: received.append((sender, text)))
    _forward(a, "hello from a", b)
    assert wait_until(lambda: any(t == "hello from a" for _, t in received)), (
        f"payload never reached {received=}"
    )
    assert received[0][0] == _client_key(a)
    assert wait_until(
        lambda: any(
            e[0] == "hello from a" for e in b.messages_dict.get(_client_key(a), [])
        )
    ), "payload not recorded under the originator in the destination store"


def test_forward_records_keyed_by_originator(trio):
    """Part of the store contract: forwarded content is recorded under the
    ORIGINAL client on both ends.

    - server.messages_dict[origin_socket] gains the payload (as if the
      originator had sent it directly);
    - the receiving client records the payload and the envelope event under
      the originator's "ip:port" (its socket exists only on the server);
    - the receiving client's own server-link socket key stays clean.
    """
    server, a, b = trio
    origin_key = _client_key(a)
    origin_sock = _server_sock(server, a)
    _forward(a, "rec me", b)
    assert wait_until(
        lambda: any(
            e[0] == "rec me" for e in server.messages_dict.get(origin_sock, [])
        )
    ), "server did not record the forwarded message under the originator's socket"
    assert wait_until(
        lambda: any(e[0] == "rec me" for e in b.messages_dict.get(origin_key, []))
    ), "receiver did not record the payload under the originator's address"
    assert wait_until(
        lambda: any(
            e[0].startswith("/send_msg_from")
            for e in b.events_dict.get(origin_key, [])
        )
    ), "receiver did not record the envelope event under the originator's address"
    # the client's own link to the server must not hold forwarded content
    assert all(
        e[0] != "rec me" for e in b.messages_dict.get(b.client_socket, [])
    )


def test_public_forward_api(trio):
    """The packaged forward API for extension authors: forward_message_to
    tags the envelope, forward_target_command builds the tagged transfer
    command, and unreachable targets are reported."""
    server, a, b = trio
    received = []
    b.add_message_listener(lambda sender, text: received.append((sender, text)))
    assert server.forward_message_to(_addr(b), "via api", _addr(a))
    assert wait_until(lambda: len(received) == 1), f"never arrived {received=}"
    assert received[0] == (_client_key(a), "via api")
    cmd = server.forward_target_command("file", "", "a.txt", _addr(a), 7)
    assert parse_forward_originator(cmd) == _client_key(a)
    assert server.forward_message_to(("127.0.0.1", 1), "x", _addr(a)) is False  # unreachable


def test_extension_command_cannot_hijack_envelope(trio):
    """/send_msg_from is unwrapped before command dispatch: an extension
    registering a command with that name must not swallow forwarded messages."""
    server, a, b = trio
    hijacked = []
    b.register_command(
        "/send_msg_from",
        lambda sock, addr, cmd: hijacked.append(cmd),
        where_to_run="server",
        run_in_thread=False,
    )
    received = []
    b.add_message_listener(lambda sender, text: received.append((sender, text)))
    _forward(a, "hi", b)
    assert wait_until(lambda: len(received) == 1), f"payload never reached {received=}"
    assert received[0] == (_client_key(a), "hi")
    time.sleep(0.3)
    assert hijacked == []
