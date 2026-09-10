"""End-to-end file-transfer tests (plain channel, no crypto)."""

import os
import threading
import time
import shlex

import pytest


from test_util import server_ready, wait_until

from PyFlow.network_api.connect_tcp import (
    TCP_Client_Base,
    TCP_Server_Base,
    parse_forward_originator,
)
from unittest.mock import MagicMock

_PORT_COUNTER = 65420


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
    recv_dir = tmp_path / "recv"
    recv_dir.mkdir()
    client.file_transfer_dir = str(recv_dir)
    assert client.connect()
    yield server, client, recv_dir
    client.close()
    server.stop()


def _wait_port(client, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with client.file_transfer_server_port_lock:
            ports = list(client.file_server_port_list)
        if ports:
            return ports[0][0]
        time.sleep(0.05)
    return None


def test_client_receives_file_from_server(pair, tmp_path):
    server, client, recv_dir = pair
    src = tmp_path / "payload.bin"
    payload = os.urandom(2048)
    src.write_bytes(payload)

    # the client side of the transfer: it believes the server sent a
    # "/file" command, so it opens a temporary server and advertises the
    # port to the server over the control channel
    client.file_transfer_client_recv_server_start_thread(
        "cid", client.client_socket, f"/file {src} 0"
    )
    server_port = _wait_port(server)
    assert server_port is not None

    server.file_transfer_mode(str(src), "127.0.0.1", server_port, 0)

    assert wait_until(lambda: any(recv_dir.iterdir()), timeout=10), "file was not received"
    files = list(recv_dir.iterdir())
    assert files[0].read_bytes() == payload


def test_client_receives_large_file_from_server(pair, tmp_path):
    """A payload larger than one TCP chunk is reassembled correctly."""
    server, client, recv_dir = pair
    src = tmp_path / "big.bin"
    payload = os.urandom(300 * 1024)  # ~300 KB, many chunks
    src.write_bytes(payload)

    client.file_transfer_client_recv_server_start_thread(
        "cid", client.client_socket, f"/file {src} 1"
    )
    server_port = _wait_port(server)
    assert server_port is not None

    server.file_transfer_mode(str(src), "127.0.0.1", server_port, 0)

    assert wait_until(lambda: any(recv_dir.iterdir()), timeout=15), "file was not received"
    files = list(recv_dir.iterdir())
    assert files[0].read_bytes() == payload


def test_server_receives_file_from_client(pair, tmp_path):
    """Client pushes a file to the server (server-side receive path)."""
    server, client, _ = pair
    server_recv = tmp_path / "server_recv"
    server_recv.mkdir()
    server.file_transfer_dir = str(server_recv)
    src = tmp_path / "upload.bin"
    payload = os.urandom(8192)
    src.write_bytes(payload)

    # the server believes the client sent a "/file" command: it opens a
    # temporary server and advertises the port back to the client
    server_sock = server.clients[client.client_socket.getsockname()]["socket"]
    server.file_transfer_server_recv_server_start_thread(
        "cid", server_sock, f"/file {src} 0"
    )
    server_port = _wait_port(client)
    assert server_port is not None

    client.file_transfer_mode(str(src), "127.0.0.1", server_port, 0)

    assert wait_until(lambda: any(server_recv.iterdir()), timeout=10), (
        "server did not receive the file"
    )
    files = list(server_recv.iterdir())
    assert files[0].read_bytes() == payload


def test_server_pushes_file_with_quoted_client_addr(pair, tmp_path):
    """The console-style command with quoted parameters works end to end:
    /file "<path>" "('127.0.0.1', <port>)" - the quoted tuple survives
    shlex and ast.literal_eval.
    """
    server, client, recv_dir = pair
    src = tmp_path / "quoted.bin"
    payload = os.urandom(4096)
    src.write_bytes(payload)
    client_addr = client.client_socket.getsockname()
    # handle_client already registered the client with the server-side socket
    assert client_addr in server.clients
    cmd = '/file "{}" "({}, {})"'.format(src, repr(client_addr[0]), client_addr[1])
    server.file_transfer_server_recv_client_start(cmd, file_folder_abspath=None)
    assert wait_until(lambda: any(recv_dir.iterdir()), timeout=10), (
        "client did not receive the quoted-address transfer"
    )
    files = list(recv_dir.iterdir())
    assert files[0].read_bytes() == payload


def test_server_receives_file_at_destination(pair, tmp_path):
    """Client /file with a destination dir: the server writes the file there
    instead of its default transfer directory."""
    server, client, _ = pair
    server_recv = tmp_path / "server_recv"
    server_recv.mkdir()
    server.file_transfer_dir = str(server_recv)
    src = tmp_path / "upload.bin"
    payload = os.urandom(8192)
    src.write_bytes(payload)
    dest = tmp_path / "dest"
    dest.mkdir()

    # paths must be quoted: shlex treats an unquoted backslash as an escape,
    # which mangles Windows paths like C:\Users\...
    server_sock = server.clients[client.client_socket.getsockname()]["socket"]
    server.file_transfer_server_recv_server_start_thread(
        "cid", server_sock, f'/file "{src}" "{dest}" 0'
    )
    server_port = _wait_port(client)
    assert server_port is not None

    client.file_transfer_mode(str(src), "127.0.0.1", server_port, 0)

    assert wait_until(lambda: any(dest.iterdir()), timeout=10), (
        "file was not written to the destination directory"
    )
    assert not any(server_recv.iterdir()), "file must not land in the default directory"
    files = list(dest.iterdir())
    assert files[0].read_bytes() == payload


def test_server_pushes_file_to_destination(pair, tmp_path):
    """Server /file with a destination dir: the client writes the file there
    instead of its default transfer directory."""
    server, client, _ = pair
    src = tmp_path / "payload.bin"
    payload = os.urandom(4096)
    src.write_bytes(payload)
    client_addr = client.client_socket.getsockname()
    dest = tmp_path / "dest"
    dest.mkdir()

    cmd = '/file "{}" "({}, {})" "{}"'.format(src, repr(client_addr[0]), client_addr[1], dest)
    server.file_transfer_server_recv_client_start(cmd, file_folder_abspath=None)

    assert wait_until(lambda: any(dest.iterdir()), timeout=10), (
        "client did not write the file to the destination directory"
    )
    files = list(dest.iterdir())
    assert files[0].read_bytes() == payload


def test_client_uploads_folder_to_destination(pair, tmp_path):
    """Client /file_folder with a destination dir: the folder structure is
    recreated under the destination on the server."""
    server, client, _ = pair
    server_recv = tmp_path / "server_recv"
    server_recv.mkdir()
    server.file_transfer_dir = str(server_recv)
    dest = tmp_path / "dest"
    dest.mkdir()
    folder = tmp_path / "data"
    (folder / "sub").mkdir(parents=True)
    (folder / "a.txt").write_text("hello")
    (folder / "sub" / "b.txt").write_text("world")

    client.folder_file_transfer_client_recv_client_start(
        f'/file_folder "{folder}" "{dest}"'
    )

    # the folder keeps its own name under the destination (same structure as
    # the default receive directory, with the destination as its root)
    assert wait_until(lambda: (dest / "data" / "a.txt").exists(), timeout=10), (
        "a.txt not at destination"
    )
    assert wait_until(lambda: (dest / "data" / "sub" / "b.txt").exists(), timeout=10), (
        "b.txt not at destination"
    )
    assert (dest / "data" / "a.txt").read_text() == "hello"
    assert (dest / "data" / "sub" / "b.txt").read_text() == "world"
    assert not any(server_recv.iterdir()), "folder must not land in the default directory"


def test_server_pushes_folder_to_destination(pair, tmp_path):
    """Server /file_folder with a destination dir: the folder structure is
    recreated under the destination on the client."""
    server, client, _ = pair
    folder = tmp_path / "data"
    (folder / "sub").mkdir(parents=True)
    (folder / "a.txt").write_text("hello")
    (folder / "sub" / "b.txt").write_text("world")
    client_addr = client.client_socket.getsockname()
    dest = tmp_path / "dest"
    dest.mkdir()

    cmd = '/file_folder "{}" "({}, {})" "{}"'.format(
        folder, repr(client_addr[0]), client_addr[1], dest
    )
    server.folder_file_transfer_server_recv_client_start(cmd)

    assert wait_until(lambda: (dest / "data" / "a.txt").exists(), timeout=10), (
        "a.txt not at destination"
    )
    assert wait_until(lambda: (dest / "data" / "sub" / "b.txt").exists(), timeout=10), (
        "b.txt not at destination"
    )
    assert (dest / "data" / "a.txt").read_text() == "hello"
    assert (dest / "data" / "sub" / "b.txt").read_text() == "world"


def test_client_uploads_multiple_files_to_destination(pair, tmp_path):
    """Client /multiple_file with a destination dir: every file is written
    under the destination on the server."""
    server, client, _ = pair
    server_recv = tmp_path / "server_recv"
    server_recv.mkdir()
    server.file_transfer_dir = str(server_recv)
    dest = tmp_path / "dest"
    dest.mkdir()
    f1 = tmp_path / "one.bin"
    f1.write_bytes(b"one")
    f2 = tmp_path / "two.bin"
    f2.write_bytes(b"two")

    client.multiple_file_transfer_client_recv_client_start(
        f'/multiple_file "{f1}" "{f2}" "{dest}"'
    )

    assert wait_until(lambda: (dest / "one.bin").exists(), timeout=10), (
        "one.bin not at destination"
    )
    assert wait_until(lambda: (dest / "two.bin").exists(), timeout=10), (
        "two.bin not at destination"
    )
    assert (dest / "one.bin").read_bytes() == b"one"
    assert (dest / "two.bin").read_bytes() == b"two"


def test_client_uploads_multiple_folders_to_destination(pair, tmp_path):
    """Client /multiple_file_folder with a destination dir: every folder is
    recreated under the destination on the server."""
    server, client, _ = pair
    server_recv = tmp_path / "server_recv"
    server_recv.mkdir()
    server.file_transfer_dir = str(server_recv)
    # destination is a remote-only path: it must not exist locally, or the
    # multiple-folder parser would treat it as another folder to transfer
    dest = tmp_path / "dest"
    folder_a = tmp_path / "data_a"
    folder_a.mkdir()
    (folder_a / "a.txt").write_text("hello")
    folder_b = tmp_path / "data_b"
    folder_b.mkdir()
    (folder_b / "b.txt").write_text("world")

    client.multiple_folder_file_transfer_client_recv_client_start(
        f'/multiple_file_folder "{folder_a}" "{folder_b}" "{dest}"'
    )

    assert wait_until(lambda: (dest / "data_a" / "a.txt").exists(), timeout=10), (
        "a.txt not at destination"
    )
    assert wait_until(lambda: (dest / "data_b" / "b.txt").exists(), timeout=10), (
        "b.txt not at destination"
    )
    assert (dest / "data_a" / "a.txt").read_text() == "hello"
    assert (dest / "data_b" / "b.txt").read_text() == "world"

def test_windows_paths_need_quoting_through_shlex():
    """Unquoted ``C:\\dir`` is mangled by shlex (backslash = escape), so
    transfer commands must quote Windows paths; the destination parser then
    sees the path intact. Runs on every OS: shlex semantics are POSIX."""
    from PyFlow.network_api.connect_tcp import _parse_destination_path

    # quoted paths survive shlex, and the trailing client id is skipped
    tokens = shlex.split('/file "C:\\src.bin" "C:\\dest" 0')
    assert tokens == ["/file", "C:\\src.bin", "C:\\dest", "0"]
    assert _parse_destination_path(tokens) == "C:\\dest"
    # unquoted backslashes are eaten -> a mangled, non-absolute path
    tokens = shlex.split("/file C:\\src.bin C:\\dest 0")
    assert tokens == ["/file", "C:src.bin", "C:dest", "0"]
    assert _parse_destination_path(tokens) == "C:dest"
    # folder creation with a quoted Windows destination
    tokens = shlex.split('/file_folder "/data" "C:\\dest"')
    assert _parse_destination_path(tokens) == "C:\\dest"
    # the folder command name is matched case-insensitively: an uppercase
    # /FILE_FOLDER from a console must still deliver the destination
    tokens = shlex.split('/FILE_FOLDER "/data" "C:\\dest"')
    assert _parse_destination_path(tokens) == "C:\\dest"

def test_server_console_file_command_preserves_path_case(pair, tmp_path, monkeypatch):
    """The server console matches command names case-insensitively (an
    uppercase /FILE must dispatch) while the file path reaches the transfer
    unchanged (POSIX paths are case-sensitive). A mixed-case path typed on
    the console used to fail with FileNotFoundError."""
    server, client, recv_dir = pair
    src = tmp_path / "MiXeD_Case.bin"
    payload = os.urandom(2048)
    src.write_bytes(payload)
    client_addr = client.client_socket.getsockname()
    cmd = '/FILE "{}" "({}, {})"'.format(src, repr(client_addr[0]), client_addr[1])
    inputs = iter([cmd, "/stop"])

    def fake_input():
        # "/stop" flips server.running False, which makes send_message refuse
        # the still-running transfer; only stop the console after the file
        # has arrived
        value = next(inputs)
        if value == "/stop":
            assert wait_until(lambda: any(recv_dir.iterdir()), timeout=10), (
                "client did not receive the mixed-case-path transfer"
            )
        return value

    monkeypatch.setattr("builtins.input", fake_input)
    monkeypatch.setattr(server, "stop", MagicMock())
    server.console_input()
    files = list(recv_dir.iterdir())
    assert files[0].name == "MiXeD_Case.bin"
    assert files[0].read_bytes() == payload


def _forward_client(server, recv_dir):
    client = TCP_Client_Base(
        host="127.0.0.1",
        port=server.port,
        client_host="127.0.0.1",
        is_extend_command=True,
        is_input_command_in_console=False,
        is_enable_encrypto=False,
    )
    client.file_transfer_dir = str(recv_dir)
    assert client.connect()
    return client


def test_parse_forward_originator():
    """The originator tuple in a pushed /file or /file_folder command is
    recovered; a direct send (the receiver's own address) is filtered out."""
    # the wire form produced by forward_target_command (tuple shlex-quoted)
    cmd = lambda *a: TCP_Server_Base.forward_target_command(None, *a)
    assert parse_forward_originator(cmd("file", "", "a.txt", ("127.0.0.1", 3000), 7)) == (
        "127.0.0.1:3000"
    )
    assert parse_forward_originator(
        cmd("file", "", "a.txt", ("127.0.0.1", 3000), 7, "dest")
    ) == "127.0.0.1:3000"
    assert parse_forward_originator(
        cmd("folder", "d", "a.txt", ("127.0.0.1", 3000), 7)
    ) == "127.0.0.1:3000"
    assert parse_forward_originator(
        cmd("folder", "d", "a.txt", ("127.0.0.1", 3000), 7, "dest")
    ) == "127.0.0.1:3000"
    # direct send: the tuple is the receiver's own address -> filtered
    assert (
        parse_forward_originator(
            cmd("file", "", "a.txt", ("127.0.0.1", 3000), 7),
            own_address="127.0.0.1:3000",
        )
        is None
    )
    # no tuple: plain direct send or a non-transfer command
    assert parse_forward_originator("/file 'a.txt' 7") is None
    assert parse_forward_originator("/pause_trans 3") is None


def test_forwarded_file_carries_originator(pair, tmp_path):
    """A streamed forward tags the pushed /file command with the originator's
    address: the receiver's file listener can attribute the transfer."""
    server, target, recv_dir = pair
    src = tmp_path / "payload.bin"
    payload = os.urandom(4096)
    src.write_bytes(payload)
    forwarder = _forward_client(server, tmp_path / "fwd")
    commands = []
    target.add_file_listener(
        lambda full_path, name, size, command: commands.append(command)
    )
    t = target.client_socket.getsockname()
    cmd = '/forward_file "{}" "({}, {})"'.format(src, repr(t[0]), t[1])
    forwarder.forward_file_console(cmd)
    assert wait_until(lambda: (recv_dir / "payload.bin").exists(), timeout=15), (
        "forwarded file not received"
    )
    assert wait_until(lambda: len(commands) >= 1), f"no file event {commands=}"
    a = forwarder.client_socket.getsockname()
    own = target.client_socket.getsockname()
    originator = parse_forward_originator(commands[0], own_address=f"{own[0]}:{own[1]}")
    assert originator == f"{a[0]}:{a[1]}"
    forwarder.close()


def test_forward_file_to_multiple_clients(pair, tmp_path):
    """/forward_file relays one file from a client to several clients, with
    the server acting as an in-memory relay (no server-side disk write)."""
    server, target1, recv1 = pair
    recv2 = tmp_path / "recv2"
    recv2.mkdir()
    target2 = _forward_client(server, recv2)
    src = tmp_path / "payload.bin"
    payload = os.urandom(64 * 1024 + 123)
    src.write_bytes(payload)
    forwarder = _forward_client(server, tmp_path / "fwd")

    a1 = target1.client_socket.getsockname()
    a2 = target2.client_socket.getsockname()
    cmd = '/forward_file "{}" "({}, {})" "({}, {})"'.format(
        src, repr(a1[0]), a1[1], repr(a2[0]), a2[1]
    )
    forwarder.forward_file_console(cmd)

    # size-aware wait: exists() can be true while the receiver is still
    # writing; reading mid-write would see a truncated file
    def got(path):
        return path.exists() and path.stat().st_size == len(payload)

    assert wait_until(lambda: got(recv1 / "payload.bin"), timeout=15), (
        "target 1 did not receive the forwarded file"
    )
    assert wait_until(lambda: got(recv2 / "payload.bin"), timeout=15), (
        "target 2 did not receive the forwarded file"
    )
    assert (recv1 / "payload.bin").read_bytes() == payload
    assert (recv2 / "payload.bin").read_bytes() == payload
    target2.close()
    forwarder.close()


def test_forward_folder_to_client(pair, tmp_path):
    """/forward_folder relays a folder (structure preserved) from a client to
    another client through the in-memory relay."""
    server, target, recv_dir = pair
    folder = tmp_path / "data"
    (folder / "sub").mkdir(parents=True)
    (folder / "a.txt").write_text("hello")
    (folder / "sub" / "b.txt").write_text("world")
    forwarder = _forward_client(server, tmp_path / "fwd")

    a = target.client_socket.getsockname()
    cmd = '/forward_folder "{}" "({}, {})"'.format(folder, repr(a[0]), a[1])
    forwarder.forward_folder_console(cmd)

    assert wait_until(lambda: (recv_dir / "data" / "a.txt").exists(), timeout=15), (
        "a.txt not forwarded"
    )
    assert wait_until(lambda: (recv_dir / "data" / "sub" / "b.txt").exists(), timeout=15), (
        "sub/b.txt not forwarded"
    )
    assert (recv_dir / "data" / "a.txt").read_text() == "hello"
    assert (recv_dir / "data" / "sub" / "b.txt").read_text() == "world"
    forwarder.close()


def test_forward_skips_unreachable_and_server_targets(pair, tmp_path):
    """Unreachable addresses and the server itself are skipped; the reachable
    target still receives the file."""
    server, target, recv_dir = pair
    src = tmp_path / "payload.bin"
    payload = os.urandom(2048)
    src.write_bytes(payload)
    forwarder = _forward_client(server, tmp_path / "fwd")

    a = target.client_socket.getsockname()
    bad = ("127.0.0.1", 39999)  # not connected
    server_addr = (server.host, server.port)  # the server itself
    cmd = '/forward_file "{}" "({}, {})" "({}, {})" "({}, {})"'.format(
        src,
        repr(a[0]), a[1],
        repr(bad[0]), bad[1],
        repr(server_addr[0]), server_addr[1],
    )
    forwarder.forward_file_console(cmd)

    def got(path):
        return path.exists() and path.stat().st_size == len(payload)

    assert wait_until(lambda: got(recv_dir / "payload.bin"), timeout=15), (
        "reachable target did not receive the forwarded file"
    )
    assert (recv_dir / "payload.bin").read_bytes() == payload
    forwarder.close()


def test_forward_flow_control_pauses_uploader(pair, tmp_path, monkeypatch):
    """When the server's in-memory buffer exceeds max_mem_buff it pauses the
    uploader with /pause_trans and resumes it with /start_trans; the file is
    still delivered intact."""
    server, target, recv_dir = pair
    # strictly below one 64 KiB relay chunk, so every chunk overflows the
    # buffer and a /pause_trans -> /start_trans cycle is guaranteed (a limit
    # of exactly one chunk sits at the > comparison boundary and makes the
    # pause depend on thread scheduling)
    server.max_mem_buff = 32 * 1024
    src = tmp_path / "big.bin"
    payload = os.urandom(300 * 1024)
    src.write_bytes(payload)
    forwarder = _forward_client(server, tmp_path / "fwd")

    sent = []
    real_send = server.send_message

    def spy(sock, message):
        sent.append(message if isinstance(message, str) else "")
        return real_send(sock, message)

    monkeypatch.setattr(server, "send_message", spy)
    a = target.client_socket.getsockname()
    cmd = '/forward_file "{}" "({}, {})"'.format(src, repr(a[0]), a[1])
    forwarder.forward_file_console(cmd)

    def got(path):
        return path.exists() and path.stat().st_size == len(payload)

    assert wait_until(lambda: got(recv_dir / "big.bin"), timeout=25), (
        "forwarded file was not received"
    )
    assert (recv_dir / "big.bin").read_bytes() == payload
    assert any(str(m).startswith("/pause_trans") for m in sent), "expected a /pause_trans"
    assert any(str(m).startswith("/start_trans") for m in sent), "expected a /start_trans"
    forwarder.close()

def test_forward_file_with_destination(pair, tmp_path):
    """/forward_file with a trailing destination dir: targets write the
    forwarded file under the destination instead of their default dir."""
    server, target, recv_dir = pair
    src = tmp_path / "payload.bin"
    payload = os.urandom(8192)
    src.write_bytes(payload)
    dest = tmp_path / "dest"
    forwarder = _forward_client(server, tmp_path / "fwd")

    a = target.client_socket.getsockname()
    cmd = '/forward_file "{}" "({}, {})" "{}"'.format(src, repr(a[0]), a[1], dest)
    forwarder.forward_file_console(cmd)

    def got(path):
        return path.exists() and path.stat().st_size == len(payload)

    assert wait_until(lambda: got(dest / "payload.bin"), timeout=15), (
        "file not written to the destination"
    )
    assert (dest / "payload.bin").read_bytes() == payload
    assert not any(recv_dir.iterdir()), "default directory must stay empty"
    forwarder.close()


def test_forward_folder_with_destination(pair, tmp_path):
    """/forward_folder with a trailing destination dir: the folder structure
    is recreated under the destination on the target."""
    server, target, recv_dir = pair
    folder = tmp_path / "data"
    (folder / "sub").mkdir(parents=True)
    (folder / "a.txt").write_text("hello")
    (folder / "sub" / "b.txt").write_text("world")
    dest = tmp_path / "dest"
    forwarder = _forward_client(server, tmp_path / "fwd")

    a = target.client_socket.getsockname()
    cmd = '/forward_folder "{}" "({}, {})" "{}"'.format(folder, repr(a[0]), a[1], dest)
    forwarder.forward_folder_console(cmd)

    assert wait_until(lambda: (dest / "data" / "a.txt").exists(), timeout=15), (
        "a.txt not at the destination"
    )
    assert wait_until(lambda: (dest / "data" / "sub" / "b.txt").exists(), timeout=15), (
        "sub/b.txt not at the destination"
    )
    assert (dest / "data" / "a.txt").read_text() == "hello"
    assert (dest / "data" / "sub" / "b.txt").read_text() == "world"
    assert not any(recv_dir.iterdir()), "default directory must stay empty"
    forwarder.close()

def test_multiple_file_multiple_client_console_command(pair, tmp_path):
    """/multiple_file_multiple_client from the server console sends one file to
    every listed client (the console form used to be a silent no-op because
    the handler only dispatched /file- and /file_folder-prefixed messages)."""
    server, client, recv_dir = pair
    recv2 = tmp_path / "recv2"
    recv2.mkdir()
    client2 = TCP_Client_Base(
        host="127.0.0.1",
        port=server.port,
        client_host="127.0.0.1",
        is_extend_command=True,
        is_input_command_in_console=False,
        is_enable_encrypto=False,
    )
    client2.file_transfer_dir = str(recv2)
    assert client2.connect()
    src = tmp_path / "broadcast.bin"
    payload = os.urandom(4096)
    src.write_bytes(payload)
    addr1 = client.client_socket.getsockname()
    addr2 = client2.client_socket.getsockname()
    dest = tmp_path / "dest"
    dest.mkdir()
    cmd = '/multiple_file_multiple_client "{}" "({}, {})" "({}, {})" "{}"'.format(
        src, repr(addr1[0]), addr1[1], repr(addr2[0]), addr2[1], dest
    )
    server.multiple_file_multiple_client_transfer_server_recv_client_start(cmd)
    assert wait_until(lambda: (dest / "broadcast.bin").exists(), timeout=10), (
        "client 1 did not receive the broadcast file at the destination"
    )
    assert not any(recv_dir.iterdir()), "default directory must stay empty on client 1"
    assert not any(recv2.iterdir()), "default directory must stay empty on client 2"
    assert (dest / "broadcast.bin").read_bytes() == payload
    client2.close()
