"""Forward extension for the TCP protocol.

Disk-based, upload-then-push forwarding of files and folders to a list of
destination clients. This is deliberately a second implementation of file
forwarding: the native TCP protocol already streams files and folders in
memory (``/forward_file`` / ``/forward_folder`` on a client console,
relayed by the server as ``/forward_item`` with no disk I/O on the
server), while this extension uploads the data to the server's transfer
directory first and then asks the server to push the stored copies.
Plain-message forwarding is native as well (the client-only command
``/forward_send_msg``, relayed by the server), so no string forwarding
lives here.

Transfer families added by this extension:

  /file_forward <file_path> <(ip, port)> ...
      forward one file to every listed destination
  /multiple_file_forward <file1> <file2> ... <(ip, port)> ...
      forward several files to every listed destination
  /folder_forward <folder_path> <(ip, port)> ...
      forward one folder (structure preserved) to every destination
  /multiple_folder_forward <folder1> <folder2> ... <(ip, port)> ...
      forward several folders to every listed destination

Items come first, destinations last; every destination is written as a
Python address tuple, e.g. ``"('127.0.0.1', 3000)"``. There is no limit
on the number or size of items or destinations.

The commands are only available on the client console: they are
registered in the "client" handler group, so typing them on the server
console is rejected as an unrecognized command. Forwarding goes through
the server - the client uploads the data over the normal transfer
channel (the server stores it in its transfer directory) and then asks
the server to push it to the destinations, which receive it through the
main protocol's own receive paths. Destinations that are unreachable
(not connected to the server, or the server itself, which is never in
the client table) are skipped and the remaining destinations are still
served.
"""

import functools
import os
import shlex
import threading

from .network_api import connect_tcp
from .network_api.connect_tcp import (
    forward_skip_message as _server_skip_message,
    parse_forward_items_and_addrs as _parse_items_and_addrs,
)

server_instance = None
client_instance = None

_FORWARD_COMMANDS = (
    "/file_forward",
    "/multiple_file_forward",
    "/folder_forward",
    "/multiple_folder_forward",
)

# client command kind -> the relay command the server receives
_RELAY_FOR_KIND = {
    "file": "/forward_file",
    "folder": "/forward_folder",
}


def _server_send(message):
    """Send one protocol request to the server through the client socket."""
    if client_instance is None or client_instance.client_socket is None:
        print("forward: client instance is not set up")
        return False
    return client_instance.send_message(client_instance.client_socket, message)


def _forward_request(command, names, addrs):
    """Ask the server to push ``names`` (stored paths) to ``addrs``."""
    request = command + " " + " ".join(shlex.quote(n) for n in names)
    request += " " + " ".join(shlex.quote(str(a)) for a in addrs)
    return _server_send(request)


def _upload_files_sync(paths):
    """Upload every existing file to the server over the normal channel."""
    if client_instance is None:
        print("forward: client instance is not set up")
        return
    for path in paths:
        if not os.path.isfile(path):
            print(f"forward: {path} is not a valid file, skipped")
            continue
        client_instance.file_transfer_client_recv_client_start(f"/file {shlex.quote(path)}", None)


def _upload_folder_sync(folder_path):
    """Upload one folder to the server, preserving its structure.

    Mirrors the main protocol's folder transfer, but synchronous: every
    file transfer finishes before this returns, so the follow-up forward
    request can never overtake the upload.
    """
    base_path = os.path.dirname(folder_path)

    def get_relative_path(base, abs_path):
        rel = os.path.relpath(abs_path, base)
        if rel == ".":
            return ""
        return rel.replace(os.sep, "/")

    transfer_path = get_relative_path(base_path, folder_path)
    if client_instance is None:
        print("forward: client instance is not set up")
        return
    _server_send(f"/file_folder {shlex.quote(transfer_path)}")
    for root, dirs, files in os.walk(folder_path):
        rel_dir = get_relative_path(base_path, root)
        if root != folder_path:
            _server_send(f"/file_folder {shlex.quote(rel_dir)}")
        for file in files:
            client_instance.file_transfer_client_recv_client_start(
                f"/file_folder {shlex.quote(rel_dir)} {shlex.quote(file)}", root
            )


def _upload_folders_sync(paths):
    for path in paths:
        if not os.path.isdir(path):
            print(f"forward: {path} is not a valid folder, skipped")
            continue
        _upload_folder_sync(path)


def _client_forward_handler(kind, allow_multiple, sock, addr, cmd):
    """Console entry point for the /xxx_forward commands (client only).

    The command name never matters here: ``kind`` ("file" or "folder") and
    ``allow_multiple`` are bound at registration time with
    functools.partial. Registered with where_to_run="client", so it only
    fires from console input (interactive_mode), never from messages sent
    by other instances.
    """
    parts = shlex.split(cmd)
    items, addrs = _parse_items_and_addrs(parts[1:])
    if not items or not addrs:
        print(
            f"{kind}: need at least one item and one destination, "
            'e.g. /file_forward "file.txt" "(\'127.0.0.1\', 3000)"'
        )
        return None
    if not allow_multiple and len(items) != 1:
        print(f"{kind}: expects exactly one item; use the multiple variant")
        return None
    relay = _RELAY_FOR_KIND[kind]
    if kind == "file":
        _upload_files_sync(items)
        _forward_request(relay, [os.path.basename(p) for p in items], addrs)
    elif kind == "folder":
        _upload_folders_sync(items)
        _forward_request(relay, [os.path.basename(p) for p in items], addrs)
    return None


def _forward_files_handler(sock, addr, cmd):
    """Server-side relay: push server-side files to every reachable destination."""
    if server_instance is None:
        print("forward: server instance is not set up")
        return None
    parts = shlex.split(cmd)
    names, addrs = _parse_items_and_addrs(parts[1:])
    for target in addrs:
        client_info = server_instance.clients.get(target)
        if client_info is None:
            print(_server_skip_message(target))
            continue
        for name in names:
            path = os.path.join(server_instance.file_transfer_dir, name)
            if not os.path.isfile(path):
                print(f"forward: {path} is not on the server, skipped")
                continue
            server_instance.file_transfer_server_recv_client_start(
                f"/file {shlex.quote(path)} {shlex.quote(str(target))}", None
            )
    return None


def _forward_folders_handler(sock, addr, cmd):
    """Server-side relay: push server-side folders to every reachable destination."""
    if server_instance is None:
        print("forward: server instance is not set up")
        return None
    parts = shlex.split(cmd)
    names, addrs = _parse_items_and_addrs(parts[1:])
    for target in addrs:
        client_info = server_instance.clients.get(target)
        if client_info is None:
            print(_server_skip_message(target))
            continue
        for name in names:
            path = os.path.join(server_instance.file_transfer_dir, name)
            if not os.path.isdir(path):
                print(f"forward: {path} is not on the server, skipped")
                continue
            server_instance.folder_file_transfer_server_recv_client_start(
                f"/file_folder {shlex.quote(path)} {shlex.quote(str(target))}"
            )
    return None


def setup_client_commands(client):  # noqa: PLW0603
    """Register the file/folder forward commands on a client instance.

    Message forwarding (``/forward_send_msg``) is native and needs no setup.
    Each command binds its transfer kind and single/multiple policy into
    the shared handler via functools.partial; where_to_run="client" makes
    them fire from console input only.
    """
    global client_instance  # noqa: PLW0603
    client_instance = client
    command_specs = [
        ("/file_forward", "file", False),
        ("/multiple_file_forward", "file", True),
        ("/folder_forward", "folder", False),
        ("/multiple_folder_forward", "folder", True),
    ]
    for cmd_name, kind, allow_multiple in command_specs:
        client.register_command(
            cmd_name,
            functools.partial(_client_forward_handler, kind, allow_multiple),
            where_to_run="client",
            run_in_thread=True,
        )


def setup_server_commands(server):  # noqa: PLW0603
    """Register the file/folder forward relays on a server instance.

    The message relay (``/forward_send_msg``) is native and needs no setup.
    These handlers are triggered by relay requests sent by clients, i.e.
    they live in the "server" group: messages coming in from other
    instances are dispatched there. The /xxx_forward commands themselves
    stay in the client group, so typing them on the server console is
    rejected as unrecognized.
    """
    global server_instance  # noqa: PLW0603
    server_instance = server
    server.register_command(
        "/forward_file", _forward_files_handler, where_to_run="server", run_in_thread=True
    )
    server.register_command(
        "/forward_folder", _forward_folders_handler, where_to_run="server", run_in_thread=True
    )


def client_setup(instance=None, is_input_command_in_console=True):
    """Create and start a forwarding-capable client (mirrors the control extension)."""
    global client_instance  # noqa: PLW0603
    if instance is None:
        client_instance = connect_tcp.TCP_Client_Base(
            host="127.0.0.1",
            port=65000,
            client_host="127.0.0.1",
            is_input_command_in_console=is_input_command_in_console,
            is_extend_command=True,
        )
    else:
        client_instance = instance
    setup_client_commands(client_instance)
    if is_input_command_in_console:
        client_instance.start_TCP_client()
    else:
        threading.Thread(target=client_instance.start_TCP_client, daemon=True).start()


def server_setup(instance=None, is_input_command_in_console=True):
    """Create and start a forwarding-capable server (mirrors the control extension)."""
    global server_instance  # noqa: PLW0603
    if instance is None:
        server_instance = connect_tcp.TCP_Server_Base(
            host="127.0.0.1",
            port=65000,
            max_clients=10,
            is_input_command_in_console=is_input_command_in_console,
            is_extend_command=True,
        )
    else:
        server_instance = instance
    setup_server_commands(server_instance)
    if is_input_command_in_console:
        server_instance.start_TCP_Server()
    else:
        threading.Thread(target=server_instance.start_TCP_Server, daemon=True).start()
