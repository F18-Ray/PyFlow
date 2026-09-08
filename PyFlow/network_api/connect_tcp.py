import os
import ast
import sys
import time
import copy
import shlex
import socket
import secrets
import traceback
import threading
import uuid
import errno
import queue
from . import rsa_crypto
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


def _is_closed_socket_error(exc):
    """True when the error means a socket that is already closed
    (EBADF / ENOTSOCK, Windows winsock WSAENOTSOCK 10038, ECONNRESET /
    EPIPE from a peer that dropped the connection, or the "connection
    error" RuntimeError raised by send_message after the instance
    stopped): the normal teardown signal between a closing thread and a
    receive thread, not a fault worth a traceback."""
    if isinstance(exc, RuntimeError) and str(exc) == "connection error":
        return True
    return isinstance(exc, OSError) and exc.errno in (
        errno.EBADF,
        errno.ENOTSOCK,
        errno.ECONNRESET,
        errno.EPIPE,
        10038,  # WSAENOTSOCK (Windows)
    )

MAX_DECODE_FAILURES = 3  # circuit breaker: close after N consecutive decode failures (re-exchange storm / garbage injection)

def _parse_destination_path(command_part):
    """Extract the trailing destination directory from a received transfer
    command, or None when the command carries no destination.

    ``<id>`` is the file-client id appended by the sender; ``<addr>`` is a
    quoted client address tuple. The destination (when present) is the token
    before ``<id>``, except for folder-creation commands, which carry no id
    and put the destination last:

      /file <src> <id>
      /file <src> <dest> <id>
      /file <src> <addr> <id>
      /file <src> <addr> <dest> <id>
      /file_folder <rel>            folder creation, default location
      /file_folder <rel> <dest>     folder creation with destination
      /file_folder <rel> <file> <id>
      /file_folder <rel> <file> <dest> <id>
    """
    if len(command_part) < 3:
        return None
    if command_part[0].lower() == "/file_folder":
        if len(command_part) == 3:
            return command_part[-1]  # creation + destination, no client id
        if len(command_part) >= 4 and command_part[-2] != command_part[2]:
            if not (
                command_part[-2].startswith("(") and command_part[-2].endswith(")")
            ):
                return command_part[-2]
    elif len(command_part) >= 4:
        if not (
            command_part[-2].startswith("(") and command_part[-2].endswith(")")
        ):
            return command_part[-2]
    return None



class TCP_Server_Base:  # TCP server class
    def __init__(
        self,
        host="127.0.0.1",
        port=65432,
        max_clients=10,
        port_add_step=1,
        port_range_num=100,
        max_file_transfer_thread_num=10,
        is_hand_alloc_port=False,
        is_input_command_in_console=True,
        max_custom_workers=10,
        is_extend_command=False,
        is_enable_encrypto=True,
        is_custom_keys=None,
        max_mem_buff=2048,
    ):
        self.max_mem_buff = max_mem_buff * 1024 * 1024
        self._forward_fid = 0
        self._forward_fid_lock = threading.Lock()
        self._forward_relays = {}
        self._forward_relays_lock = threading.Lock()
        self.project_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_info_dir = os.path.join(self.project_dir, ".Flow")
        if os.path.exists(self.project_info_dir) == False:
            os.mkdir(self.project_info_dir)
        self.project_temp_info_dir = os.path.join(self.project_info_dir, "temp_info")
        if os.path.exists(self.project_temp_info_dir) == False:
            os.mkdir(self.project_temp_info_dir)
        self.server_port_lock_file = os.path.join(
            self.project_temp_info_dir, "server_port_lock.lock"
        )
        self.decode_command_table_file_path = os.path.join(
            self.project_dir, "decode_command_table.json"
        )
        self.file_transfer_dir = os.path.join(self.project_dir, "received_files")
        self.host = host
        self.port = port
        self.all_allocated_ports_list = [self.port]
        self.max_clients = max_clients
        self.is_hand_alloc_port = is_hand_alloc_port
        self.is_input_command_in_console = is_input_command_in_console
        self.alloc_port(port_add_step, port_range_num)
        self.server_socket = None
        self.clients = {}  # store client info
        self.file_client_id_lock = threading.Lock()
        self.file_transfer_server_port_lock = threading.Lock()
        self.file_server_port_list = []
        self.file_client_id = 0
        self.running = False
        self.client_lock = threading.Lock()  # add the threading lock
        self.max_file_transfer_thread_num = max_file_transfer_thread_num
        MAX_CONCURRENT_FILES = self.max_file_transfer_thread_num
        self.file_semaphore = threading.Semaphore(MAX_CONCURRENT_FILES)
        self.command_decode_table_str = None
        with open(self.decode_command_table_file_path, "r", encoding="utf-8") as f:
            self.command_decode_table_str = f.read()
        self.command_decode_table = ast.literal_eval(self.command_decode_table_str)
        self.send_file_header_sign = self.command_decode_table[0]["file_send_server_header"]
        self.send_file_data_sign = self.command_decode_table[0]["file_send_server_data"]
        self.server_received_file_header_sign = self.command_decode_table[0][
            "file_receive_client_header"
        ]
        self.server_received_file_data_sign = self.command_decode_table[0][
            "file_receive_client_data"
        ]
        self.server_start_file_transfer_sign = self.command_decode_table[0][
            "file_send_server_start_file_transfer"
        ]
        self.error_sign = self.command_decode_table[0]["file_send_receive_error"]
        self._custom_handlers = [{}, {}]
        self._custom_handler_threaded = [{}, {}]
        self._custom_executor = ThreadPoolExecutor(max_workers=max_custom_workers)
        self._task_semaphore = threading.Semaphore(max_custom_workers)
        # Inbound-event listeners (see add_message_listener/add_file_listener).
        # They run on the receive thread, so a listener must not block and must
        # never raise (exceptions are swallowed by the notify helpers).
        self._message_listeners = []
        self._file_listeners = []
        self._event_listeners_lock = threading.Lock()
        self.is_extend_command = is_extend_command
        self.is_enable_encrypto = is_enable_encrypto
        self.is_custom_keys = is_custom_keys
        self._crypto_lock = threading.RLock()  # serialises the crypto collections below (no-GIL safe); held only around short ops, never across I/O
        self._encrypted_sockets = set()
        self._encrypted_recv_buffers = {}
        self._crypto_peer = {}  # socket -> (peer_role, peer_pem_path)
        self._crypto_send_locks = {}  # socket -> Lock: wire order must match seq allocation order
        self._crypto_state = {}  # client_address -> crypto handshake state
        self._crypto_sock_addr = {}  # socket -> client_address
        self._crypto_push_active = set()  # sockets with an in-flight key push
        self.crypto = (
            rsa_crypto.RsaCrypto("server", self.project_dir, custom_keys=is_custom_keys)
            if is_enable_encrypto
            else None
        )
        if self.is_extend_command:
            pass
        else:
            self.start_TCP_Server()

    def alloc_port(self, port_add_step, port_range_num):
        if self.is_hand_alloc_port == True:
            while self.is_server_port_temp_info_file_locked():
                time.sleep(0.1)
            self.server_port_temp_info_file_lock()
            self.hand_alloc_port(port_add_step, port_range_num)
            self.server_port_temp_info_file_unlock()

    def free_port(self):
        if self.is_hand_alloc_port == True:
            while self.is_server_port_temp_info_file_locked():
                time.sleep(0.1)
            self.server_port_temp_info_file_lock()
            self.hand_free_port()
            self.server_port_temp_info_file_unlock()

    def server_port_temp_info_file_lock(self):
        with open(self.server_port_lock_file, "w", encoding="utf-8") as f:
            f.write("locked")

    def is_server_port_temp_info_file_locked(self):
        if os.path.exists(self.server_port_lock_file):
            return True
        else:
            return False

    def server_port_temp_info_file_unlock(self):
        if os.path.exists(self.server_port_lock_file):
            os.remove(self.server_port_lock_file)

    def hand_alloc_port(self, port_add_step, port_range_num):
        self.port_temp_info_path = os.path.join(self.project_temp_info_dir, "server_port_info.log")
        client_port_temp_info_file_path = os.path.join(
            self.project_temp_info_dir, "clients_port_info.log"
        )
        if os.path.exists(client_port_temp_info_file_path):
            print(
                "Warning: client port info file exists, means the client has already allocated a port, may cause port conflict!"
            )
        self.port_add_step = port_add_step
        self.port_range_num = port_range_num
        self.add_latest_port = self.port + 1
        self.minus_latest_port = self.port
        self.alloc_add_port_lock = threading.Lock()
        self.alloc_minus_port_lock = threading.Lock()
        self.each_client_port_range = int(self.port_range_num / self.max_clients)
        if os.path.exists(self.port_temp_info_path) == False:
            self.server_num = 0
            self.server_port_info = []
            self.min_port = self.port - self.port_add_step * self.port_range_num
            self.max_port = self.port + 1 + self.port_add_step * self.port_range_num
            each_server_info = {
                "server_id": self.server_num,
                "host": self.host,
                "port": self.port,
                "min_port": self.min_port,
                "max_port": self.max_port,
                "is_running": self.running,
            }
            self.server_port_info.append(each_server_info)
            with open(self.port_temp_info_path, "w", encoding="utf-8") as f:
                f.write(str(self.server_port_info))
        else:
            with open(self.port_temp_info_path, "r", encoding="utf-8") as f:
                self.server_port_info = ast.literal_eval(f.read())
            self.server_num = self.server_port_info[len(self.server_port_info) - 1]["server_id"] + 1
            auto_port_add = (
                self.server_port_info[len(self.server_port_info) - 1]["max_port"]
                + self.port_add_step * self.port_range_num
                + 1
            )
            auto_port_minus = (
                self.server_port_info[len(self.server_port_info) - 1]["min_port"]
                - self.port_add_step * self.port_range_num
                - 1
            )
            if self.port > auto_port_minus and self.port < auto_port_add:
                self.port = auto_port_add
            self.min_port = self.port - self.port_add_step * self.port_range_num
            self.max_port = self.port + 1 + self.port_add_step * self.port_range_num
            each_server_info = {
                "server_id": self.server_num,
                "host": self.host,
                "port": self.port,
                "min_port": self.min_port,
                "max_port": self.max_port,
                "is_running": self.running,
            }
            self.server_port_info.append(each_server_info)
            for is_running in range(len(self.server_port_info) - 1, -1, -1):
                if self.server_port_info[is_running]["is_running"] == False:
                    del self.server_port_info[is_running]
            with open(self.port_temp_info_path, "w", encoding="utf-8") as f:
                f.write(str(self.server_port_info))

    def hand_free_port(self):
        self.port_temp_info_path = os.path.join(self.project_temp_info_dir, "server_port_info.log")
        if os.path.exists(self.port_temp_info_path):
            with open(self.port_temp_info_path, "r", encoding="utf-8") as f:
                self.server_port_info = ast.literal_eval(f.read())
            for server_num in range(len(self.server_port_info)):
                if self.server_port_info[server_num]["server_id"] == self.server_num:
                    del self.server_port_info[server_num]
            if len(self.server_port_info) == 0:
                os.remove(self.port_temp_info_path)
            else:
                with open(self.port_temp_info_path, "w", encoding="utf-8") as f:
                    f.write(str(self.server_port_info))

    def palloc(self):
        alloc_port = 0
        while True:
            alloc_port = self.file_palloc()
            time.sleep(0.1)
            if alloc_port is not None:
                return alloc_port
            else:
                alloc_port = self.spy_palloc()
                if alloc_port is not None:
                    return alloc_port
                else:
                    pass

    def pfree(self, port):
        self.file_pfree(port)
        self.spy_pfree(port)

    def file_palloc(self):
        if self.is_hand_alloc_port:
            with self.alloc_add_port_lock:
                if self.add_latest_port + self.port_add_step > self.max_port:
                    for step in range(self.port + 1, self.max_port, self.port_add_step):
                        if step in self.all_allocated_ports_list:
                            pass
                        else:
                            allocated_port = step
                            self.all_allocated_ports_list.append(allocated_port)
                            return allocated_port
                    return None
                self.add_latest_port += self.port_add_step
                self.all_allocated_ports_list.append(self.add_latest_port)
                return self.add_latest_port
        else:
            return 0

    def file_pfree(self, port):
        if self.is_hand_alloc_port:
            with self.alloc_add_port_lock:
                if port in self.all_allocated_ports_list:
                    self.all_allocated_ports_list.remove(port)
                    print("releasing file transfer port, current latest port:", port)
                self.add_latest_port -= self.port_add_step
        else:
            pass

    def spy_palloc(self):
        if self.is_hand_alloc_port:
            with self.alloc_minus_port_lock:
                if self.minus_latest_port - self.port_add_step < self.min_port:
                    for step in range(self.port, self.min_port, -self.port_add_step):
                        if step in self.all_allocated_ports_list:
                            pass
                        else:
                            allocated_port = step
                            self.all_allocated_ports_list.append(allocated_port)
                            return allocated_port
                    return None
                self.minus_latest_port -= self.port_add_step
                self.all_allocated_ports_list.append(self.minus_latest_port)
                return self.minus_latest_port
        else:
            return 0

    def spy_pfree(self, port):
        if self.is_hand_alloc_port:
            with self.alloc_minus_port_lock:
                if port in self.all_allocated_ports_list:
                    self.all_allocated_ports_list.remove(port)
                self.minus_latest_port += self.port_add_step
        else:
            pass

    def register_command(self, command_name, handler, where_to_run, run_in_thread=False):
        registe_index = None
        if where_to_run == "server":
            registe_index = 0
        elif where_to_run == "client":
            registe_index = 1
        else:
            print(f"Invalid where_to_run value: {where_to_run}, must be 'server' or 'client'")
            return False
        self._custom_handlers[registe_index][command_name] = handler
        self._custom_handler_threaded[registe_index][command_name] = run_in_thread

    def add_message_listener(self, listener):
        """Register ``listener(client_id, message)`` for every inbound plain-text message.

        Plain messages are the chat/data lines received from a client that do
        not start with ``/``.  ``client_id`` is the sender's ``"ip:port"``.
        Commands are not reported here; they go through the registered
        command handlers.
        """
        with self._event_listeners_lock:
            if listener not in self._message_listeners:
                self._message_listeners.append(listener)

    def remove_message_listener(self, listener):
        """Unregister a listener previously added by ``add_message_listener``."""
        with self._event_listeners_lock:
            try:
                self._message_listeners.remove(listener)
            except ValueError:
                pass

    def add_file_listener(self, listener):
        """Register ``listener(client_id, full_path, name, size, command)`` for each saved inbound file.

        Fired after a file uploaded by a client (a direct send or a forwarded
        file/folder item staged on the server) has been fully written to
        ``file_transfer_dir``.  ``client_id`` is the uploader's ``"ip:port"``
        and ``command`` is the wire command that triggered the transfer, so a
        listener can recognise protocol pushes such as ``/crypto_pub_key``.
        """
        with self._event_listeners_lock:
            if listener not in self._file_listeners:
                self._file_listeners.append(listener)

    def remove_file_listener(self, listener):
        """Unregister a listener previously added by ``add_file_listener``."""
        with self._event_listeners_lock:
            try:
                self._file_listeners.remove(listener)
            except ValueError:
                pass

    def _notify_message_received(self, client_id, message):
        with self._event_listeners_lock:
            listeners = list(self._message_listeners)
        for listener in listeners:
            try:
                listener(client_id, message)
            except Exception:
                traceback.print_exc()

    def _notify_file_received(self, client_id, full_path, name, size, command):
        with self._event_listeners_lock:
            listeners = list(self._file_listeners)
        for listener in listeners:
            try:
                listener(client_id, full_path, name, size, command)
            except Exception:
                traceback.print_exc()

    def submit_task(self, func, *args, **kwargs):
        self._task_semaphore.acquire()
        future = self._custom_executor.submit(func, *args, **kwargs)
        future.add_done_callback(lambda f: self._task_semaphore.release())
        return future

    def create_temporary_server(self, handler, port=None, max_connections=1):
        if port is None:
            port = self.palloc()
            if port is None:
                raise RuntimeError("No available port for temporary server")
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, port))
        server_socket.listen(max_connections)
        stop_event = threading.Event()

        def server_loop():
            while not stop_event.is_set():
                try:
                    server_socket.settimeout(1.0)
                    client_sock, addr = server_socket.accept()
                    threading.Thread(target=handler, args=(client_sock, addr), daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if not stop_event.is_set():
                        print(f"Temporary server error: {e}")
                    break
            server_socket.close()
            self.pfree(port)

        server_thread = threading.Thread(target=server_loop, daemon=True)
        server_thread.start()
        return port, server_thread, stop_event

    def create_temporary_client(self, server_host, server_port, bind_port=None, on_data=None):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if bind_port is not None:
            client_sock.bind((self.host, bind_port))
        client_sock.connect((server_host, server_port))
        stop_event = threading.Event()

        def receiver():
            while not stop_event.is_set():
                try:
                    client_sock.settimeout(1.0)
                    data = self.receive_message(client_sock, 4096)
                    if not data:
                        break
                    if on_data:
                        on_data(data, client_sock)
                except socket.timeout:
                    continue
                except:
                    break
            client_sock.close()

        recv_thread = threading.Thread(target=receiver, daemon=True)
        recv_thread.start()
        return client_sock, recv_thread, stop_event

    def broadcast(
        self, message, exclude_client=None
    ):  # broadcast message to all clients except exclude_client
        with self.client_lock:
            disconnected_clients = []
            for addr, client_info in self.clients.items():
                if exclude_client and addr == exclude_client:
                    continue
                try:
                    self.send_message(client_info["socket"], message)
                except:
                    disconnected_clients.append(addr)
                    traceback.print_exc()
            for addr in disconnected_clients:  # del disconnected clients
                if addr in self.clients:
                    print(f"deleting the disconnected client: {addr}")
                    self.clients[addr]["socket"].close()
                    del self.clients[addr]

    def send_msg_to_specific_client(
        self, message
    ):  # send message to specific client by client address
        command_part = shlex.split(message)
        del command_part[0]
        client_message_pair_list = []
        client_list = []
        msg_list = []
        client_addr_times = 0
        for command_part_index in range(len(command_part)):
            part = command_part[command_part_index]
            if part.startswith("(") and part.endswith(")"):
                try:
                    client_addr = ast.literal_eval(part)
                    client_list.append(client_addr)
                    client_addr_times += 1
                    if command_part_index == len(command_part) - 1:
                        client_message_pair = [client_list, msg_list]
                        client_message_pair_list.append(client_message_pair)
                except:
                    traceback.print_exc()
                    print(
                        f"ErrorWhileParsingClientAddress: {part} is not a valid client address, skipped"
                    )
            else:
                if client_addr_times != 0:
                    client_message_pair = [client_list, msg_list]
                    client_message_pair_list.append(client_message_pair)
                    client_list = []
                    msg_list = []
                    msg_list.append(part)
                    client_addr_times = 0
                else:
                    msg_list.append(part)
        for each_client_message_pair in client_message_pair_list:
            for client_addr in each_client_message_pair[0]:
                for msg in each_client_message_pair[1]:
                    if client_addr in self.clients:
                        client_socket = self.clients[client_addr]["socket"]
                        self.send_message(client_socket, msg)
                    else:
                        print(f"Client {client_addr} not found, cannot send message: {msg}")

    def _crypto_get_send_lock(self, client_socket):
        """Per-socket lock serialising encrypt+send for one connection.

        Sequence numbers are allocated inside ``_crypto_encrypt_message``,
        so the wire write must happen under the same lock or a concurrent
        sender could overtake it and the peer would drop the frame as
        out-of-order (no-GIL safe)."""
        with self._crypto_lock:
            lock = self._crypto_send_locks.get(client_socket)
            if lock is None:
                lock = threading.Lock()
                self._crypto_send_locks[client_socket] = lock
            return lock

    def send_message(self, client_socket, message):  # send message to specific client
        if not self.running or not client_socket:
            print("disable the connect to server")
            raise RuntimeError("connection error")
        with self._crypto_get_send_lock(client_socket):  # alloc+encrypt+sendall stay ordered per socket
            try:  # add newline character for server to distinguish messages
                with self._crypto_lock:
                    encrypted = client_socket in self._encrypted_sockets
                if encrypted:
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")
                    if not isinstance(message, str):
                        print(f"Unsupported message type: {type(message)}")
                        return False
                    wire = self._crypto_encrypt_message(client_socket, message)
                    data = wire.encode("ascii") + b"\n"
                elif isinstance(message, str):
                    deal_msg = message.strip()
                    if not deal_msg.endswith("\n"):
                        deal_msg += "\n"
                    data = deal_msg.encode("utf-8")
                elif isinstance(message, bytes):
                    data = message
                else:
                    print(f"Unsupported message type: {type(message)}")
                    return False
                client_socket.sendall(data)
                return True
            except Exception as e:
                if not _is_closed_socket_error(e):
                    print(f"send msg error: {e}")
                    traceback.print_exc()
                raise

    def _send_raw(self, client_socket, text):
        """Send a plaintext crypto-protocol message, bypassing encryption."""
        data = text.strip()
        if not data.endswith("\n"):
            data += "\n"
        client_socket.sendall(data.encode("utf-8"))

    def _crypto_mark_encrypted(self, client_socket, peer_role, peer_pem_path):
        with self._crypto_lock:
            self._encrypted_sockets.add(client_socket)
            self._crypto_peer[client_socket] = (peer_role, peer_pem_path)

    @staticmethod
    def _new_crypto_state(force=False):
        """Fresh per-connection crypto handshake state."""
        return {
            "force": force,
            "peer_nonce": None,
            "my_nonce": None,
            "peer_pub_event": threading.Event(),
            "peer_pub_ok": None,
            "peer_pem_path": None,
            "ready_sent": False,
            "peer_ready": False,
            "send_seq": 0,
            "recv_seq": 0,
            "decode_failures": 0,
        }

    def _crypto_encrypt_message(self, client_socket, message):
        """Encrypt ``message`` for the peer, prefixed with the session
        nonce and an incrementing sequence number (replay protection)."""
        with self._crypto_lock:
            peer = self._crypto_peer.get(client_socket)
            if peer is None or self.crypto is None:
                raise RuntimeError("socket not bound to a crypto peer")
            addr = self._crypto_sock_addr.get(client_socket)
            state = self._crypto_state.get(addr) if addr is not None else None
            if state is None:
                raise RuntimeError("no crypto state for socket")
            nonce = state.get("my_nonce") or ""
            seq = state.get("send_seq", 0)
            state["send_seq"] = seq + 1
        body = self.crypto.encrypt_for_peer(peer[1], message.strip())
        return f"{nonce}|{seq}|{body}"

    def receive_message(self, client_socket, msg_length):  # receive message
        data = client_socket.recv(msg_length)
        return data

    def _crypto_process_line(self, client_socket, line):
        """Decrypt a received line when the socket is in encrypted mode.

        Wire format: ``<sender_nonce>|<seq>|<ciphertext>``. Returns
        ``(True, plaintext)`` on success; a line that fails the nonce or
        sequence check (replay / out-of-order) is silently dropped; a
        line that cannot be decrypted at all drops the socket out of the
        encrypted set and triggers the (circuit-broken) re-exchange.
        """
        with self._crypto_lock:
            encrypted = client_socket in self._encrypted_sockets
        if not encrypted:
            return False, line
        parts = line.split("|", 2)
        if len(parts) != 3:
            if line.startswith("/"):
                return False, line  # plaintext crypto command (handshake/re-exchange), not ciphertext
            return True, ""  # malformed ciphertext frame: not a key problem, drop silently
        nonce, seq_str, cipher = parts
        if len(nonce) != 32 or not all(c in "0123456789abcdef" for c in nonce):  # shape check: a plaintext message with "|" must not look like a ciphertext frame
            return False, line
        try:
            int(seq_str)
        except ValueError:
            return False, line
        try:
            ok, plain = self.crypto.decrypt_with_own(cipher)
            if ok:
                with self._crypto_lock:
                    addr = self._crypto_sock_addr.get(client_socket)
                    state = self._crypto_state.get(addr) if addr is not None else None
                    if state is None:
                        return True, ""
                    if nonce != state.get("peer_nonce"):
                        print("crypto: replay dropped (nonce mismatch)")
                        return True, ""
                    try:
                        seq = int(seq_str)
                    except ValueError:
                        return True, ""
                    expected = state.get("recv_seq", 0)
                    if seq != expected:
                        print(
                            f"crypto: replay/out-of-order dropped (seq {seq}, expected {expected})"
                        )
                        return True, ""
                    state["recv_seq"] = expected + 1
                    state["decode_failures"] = 0
                return True, plain
        except Exception:
            pass
        if line.startswith("/"):
            return False, line  # plaintext crypto command (handshake/re-exchange), not ciphertext
        self._crypto_on_decode_failure(client_socket)
        return False, line

    def _crypto_on_decode_failure(self, client_socket):
        """Decryption failed (stale/wrong key): drop encryption, reload
        our own key and ask the peer to re-exchange public keys.

        A circuit breaker closes the connection after
        ``MAX_DECODE_FAILURES`` consecutive failures on one connection,
        so a garbage-injection or persistent-mismatch storm cannot spin
        the re-exchange loop forever.
        """
        with self._crypto_lock:
            self._encrypted_sockets.discard(client_socket)
            self._crypto_peer.pop(client_socket, None)
            addr = self._crypto_sock_addr.get(client_socket)
            state = self._crypto_state.get(addr) if addr is not None else None
            if state is None:
                state = self._new_crypto_state(force=True)
                if addr is not None:
                    self._crypto_state[addr] = state
            state["decode_failures"] = state.get("decode_failures", 0) + 1
            if state["decode_failures"] >= MAX_DECODE_FAILURES:
                do_close = True
                do_reload = False
                do_exchange = False
            else:
                do_close = False
                do_reload = True
                do_exchange = True
                state["force"] = True
                state["my_nonce"] = self._crypto_fresh_nonce()
                state["peer_pub_event"] = threading.Event()
                state["peer_pub_ok"] = None
        if do_close:
            print("crypto: too many decode failures, closing connection")
            try:
                client_socket.close()
            except Exception:
                traceback.print_exc()
            return
        if do_reload and self.crypto is not None:
            try:
                self.crypto.reload_own_key()
            except Exception:
                traceback.print_exc()
        print("crypto: decode failure, re-exchanging public keys")
        if do_exchange:
            if addr is not None:
                threading.Thread(
                    target=self._crypto_wait_server_ready,
                    args=(client_socket, addr, state),
                    daemon=True,
                ).start()
            try:
                with self._crypto_lock:
                    nonce = state.get("my_nonce") or self._crypto_fresh_nonce()
                self._send_raw(client_socket, f"/crypto_key_exchange {nonce} 1")
            except Exception:
                traceback.print_exc()

    @staticmethod
    def _crypto_fresh_nonce():
        return secrets.token_hex(16)

    def _crypto_on_client_hello(self, client_socket, client_address, client_nonce, force):
        """Server side of /crypto_key_exchange: record the client's
        session nonce, reply that we need its public key (every
        connection re-exchanges), then wait for it and announce
        readiness."""
        self.crypto.ensure_keys()
        my_nonce = self._crypto_fresh_nonce()
        with self._crypto_lock:
            state = self._crypto_state.setdefault(client_address, self._new_crypto_state(force))
            state["force"] = force
            state["peer_nonce"] = client_nonce
            state["my_nonce"] = my_nonce
            state["peer_pub_event"] = threading.Event()
            state["peer_pub_ok"] = None
            state["send_seq"] = 0
            state["recv_seq"] = 0
        self._send_raw(client_socket, f"/crypto_key_exchange_ack {my_nonce} 1 {1 if force else 0}")
        threading.Thread(
            target=self._crypto_wait_server_ready,
            args=(client_socket, client_address, state),
            daemon=True,
        ).start()

    def _crypto_try_server_flip(self, client_socket, state):
        """Flip the socket to encrypted mode only when the peer key
        passed the TOFU check and both sides announced readiness (no
        plaintext/encrypted interleaving). Resets the sequence counters
        so the fresh session starts at seq 0."""
        # no send may interleave with the seq reset / flip
        with self._crypto_get_send_lock(client_socket):
            with self._crypto_lock:
                if (
                    state.get("peer_pub_ok") is True
                    and state.get("peer_pem_path")
                    and state.get("ready_sent")
                    and state.get("peer_ready")
                    and client_socket not in self._encrypted_sockets
                ):
                    state["send_seq"] = 0
                    state["recv_seq"] = 0
                    self._crypto_mark_encrypted(client_socket, "client", state["peer_pem_path"])

    def _crypto_wait_server_ready(self, client_socket, client_address, state):
        """Wait until the client's public key arrived and passed the
        TOFU check, then announce readiness. The socket flips to
        encrypted mode only when the client's /crypto_ready arrives too."""
        with self._crypto_lock:
            peer_pub_event = state.get("peer_pub_event")
        if peer_pub_event is None or not peer_pub_event.wait(timeout=60):
            print("crypto: waiting for client public key timed out")
            return
        with self._crypto_lock:
            pub_ok = state.get("peer_pub_ok")
        if pub_ok is not True:
            print("crypto: client public key rejected, connection dropped")
            return
        with self._crypto_lock:
            state["ready_sent"] = True
        try:
            self._send_raw(client_socket, "/crypto_ready")
        except Exception:
            traceback.print_exc()
        self._crypto_try_server_flip(client_socket, state)

    def _crypto_push_pub_to_client(self, client_socket, client_address):
        """Push our public key file to the client via the file-transfer
        mechanism (server is the sender)."""
        while True:  # serialise pushes: wait for an in-flight push instead of silently skipping it
            with self._crypto_lock:
                if client_socket not in self._crypto_push_active:
                    self._crypto_push_active.add(client_socket)
                    break
            time.sleep(0.05)
        try:
            with self.file_client_id_lock:
                client_id = copy.copy(self.file_client_id)
                self.file_client_id += 1
            send_msg = f"/crypto_pub_key {client_id}"
            self._send_raw(client_socket, send_msg)
            file_transfer_client_port = self.palloc()
            file_server_port = None
            waiting_time = 0
            while True:
                time.sleep(0.05)
                with self.file_transfer_server_port_lock:
                    for port_info in list(self.file_server_port_list):
                        if port_info[1] == client_id:
                            file_server_port = port_info[0]
                            self.file_server_port_list.remove(port_info)
                            break
                if file_server_port is not None:
                    break
                waiting_time += 1
                if waiting_time >= 200:
                    print("crypto: transfer port waiting timeout, public key push failed")
                    return False
            self.file_transfer_mode(
                self.crypto.pub_path, client_address[0], file_server_port, file_transfer_client_port
            )
            self.pfree(file_transfer_client_port)
        except Exception:
            traceback.print_exc()
            return False
        finally:
            with self._crypto_lock:
                self._crypto_push_active.discard(client_socket)
        return True

    def _crypto_store_received_pub(self, full_path, peer_role, client_address):
        """TOFU-check a public key received via the file-transfer
        mechanism and cache it under the peer endpoint.

        The check runs ``verify_peer_pub`` against ``pub_key.json``; on
        failure the peer connection is rejected and dropped.
        """
        if self.crypto is None:
            return
        peer_ip, peer_port = client_address
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                pem = f.read()
        except OSError:
            pem = ""
        ok, reason = self.crypto.verify_peer_pub(peer_ip, peer_port, pem, peer_role)
        with self._crypto_lock:
            state = self._crypto_state.get(client_address)
            if ok:
                self.crypto.store_peer_pem(peer_role, peer_ip, peer_port, full_path)
                if state is not None:
                    state["peer_pem_path"] = self.crypto.peer_pem_path(
                        peer_role, peer_ip, peer_port
                    )
                    state["peer_pub_ok"] = True
                    state["peer_pub_event"].set()
                print(f"crypto: accepted public key from {peer_ip}:{peer_port} ({reason})")
            else:
                if state is not None:
                    state["peer_pub_ok"] = False
                    state["peer_pub_event"].set()
                print(f"crypto: REJECTED public key from {peer_ip}:{peer_port}: {reason}")
                try:
                    os.remove(full_path)  # rejected key: do not leave it in received_files/
                except OSError:
                    pass
                peer_socket = self.clients.get(client_address, {}).get("socket")
        if not ok and peer_socket is not None:
            try:
                self._send_raw(peer_socket, f"/crypto_reject {reason}")
            except Exception:
                traceback.print_exc()
            try:
                peer_socket.close()
            except Exception:
                traceback.print_exc()

    def handle_client(self, client_socket, client_address):  # deal with each client
        client_id = f"{client_address[0]}:{client_address[1]}"
        with self.client_lock:  # add new client
            self.clients[client_address] = {
                "socket": client_socket,
                "address": client_address,
                "id": client_id,
                "connected_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        if self.is_enable_encrypto and self.crypto is not None:
            with self._crypto_lock:
                self._crypto_sock_addr[client_socket] = client_address
        print(f"new connection: {client_id}")
        print(f"connection count mount: {len(self.clients)}")
        welcome_msg = f"Welcome!: {client_id}\n"  # send welcome message
        try:
            self.send_message(client_socket, welcome_msg)
        except Exception as e:
            # the server is stopping (or the peer vanished): the finally
            # block below cleans up; never let this escape the thread
            if not _is_closed_socket_error(e):
                print(f"error while welcoming client {client_id} : {e}")
            return
        # announce our encryption mode; a mismatched peer is disconnected in handle_command
        self._send_raw(client_socket, f"/crypto_mode {1 if self.is_enable_encrypto else 0}")
        if self.is_hand_alloc_port == True:
            broadcast_clients_port_alloc_range_msg = "/client_alloc_port_range {}".format(
                self.each_client_port_range
            )
            self.broadcast(broadcast_clients_port_alloc_range_msg)
        else:
            broadcast_clients_port_alloc_range_msg = "/client_alloc_port_range NO_LIMIT"
            self.broadcast(broadcast_clients_port_alloc_range_msg)
        print(self.clients)
        buffer = ""
        try:
            while True:
                data = self.receive_message(client_socket, 4096)  # get msg from client
                print(data)
                if not data:
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:  # deal with multiple messages in buffer
                    line, buffer = buffer.split("\n", 1)
                    message = line.strip()
                    if not message:
                        continue
                    ok, plain = self._crypto_process_line(client_socket, message)
                    if ok:
                        message = plain.strip()
                    print(message)
                    if message.startswith("/"):  # deal with special command
                        response = self.handle_command(client_socket, client_address, message)
                    else:
                        self._notify_message_received(client_id, message)
                        timestamp = datetime.now().strftime("%H:%M:%S")  # deal with normal message
                        log_msg = f"[{timestamp}] {client_id}: {message}"
                        print(log_msg)
                        response = f"msg send: {message}"
                    if response:  # send response to client
                        self.send_message(client_socket, response)
        except ConnectionResetError:
            pass  # peer dropped with RST; the finally block reports the disconnect once
        except Exception as e:
            if not _is_closed_socket_error(e):
                print(f"error while deal with client {client_id} : {e}")
                traceback.print_exc()
        finally:
            with self.client_lock:
                if client_address in self.clients:
                    del self.clients[client_address]
            with self._crypto_lock:
                self._crypto_state.pop(client_address, None)
                self._crypto_sock_addr.pop(client_socket, None)
                self._crypto_peer.pop(client_socket, None)
                self._crypto_push_active.discard(client_socket)
                self._encrypted_sockets.discard(client_socket)
                self._encrypted_recv_buffers.pop(client_socket, None)
                self._crypto_send_locks.pop(client_socket, None)
            client_socket.close()
            print(f"client disconnected: {client_id}")
            print(f"current connection count: {len(self.clients)}")

    def handle_command(
        self, client_socket, client_address, command
    ):  # deal with special commands from client
        print(client_socket, client_address, command)
        client_id = f"{client_address[0]}:{client_address[1]}"
        send_str = None
        if command == "/help":
            help_text = [
                "avalable commands:",
                "/help - print help meg\n",
                "\t/time - display server time\n",
                "\t/clients - display connected clients\n",
                "\t/file <file_path> [destination_file_path] - send file to server\n",
                "\t/multiple_file <file1> <file2> ... [destination_file_path] ",
                "- send multiple files to server\n",
                "\t/file_folder <folder_path> [destination_file_path] - send folder to server\n",
                "\t/multiple_file_folder <folder1> <folder2> ... [destination_file_path] ",
                "- send multiple folders to server\n",
                "\t/quit - disconnect",
            ]
            send_str = " ".join(help_text) + "\n"
            return send_str
        elif command == "/time":
            send_str = f"server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + "\n"
            return send_str
        elif command == "/clients":
            with self.client_lock:
                client_list = [info["id"] for info in self.clients.values()]
                send_str = f"online clients ({len(client_list)}): {', '.join(client_list)}" + "\n"
                return send_str
        elif command == "/quit":
            send_str = "Bye!" + "\n"
            return send_str
        elif command.lower().split(" ")[0] == "/crypto_mode":
            try:
                client_crypto = int(command.split(" ")[1])
            except (IndexError, ValueError):
                client_crypto = -1
            if client_crypto != (1 if self.is_enable_encrypto else 0):
                print(
                    f"crypto: encryption mode mismatch with client {client_id} "
                    f"(client={client_crypto}, server={1 if self.is_enable_encrypto else 0}), "
                    f"disconnecting"
                )
                try:
                    client_socket.close()
                except Exception:
                    traceback.print_exc()
            return None
        elif shlex.split(command.lower())[0] == "/file":
            self.file_transfer_server_recv_server_start_thread(client_id, client_socket, command)
        elif shlex.split(command.lower())[0] == "/file_folder":
            self.file_folder_transfer_server_recv_server_start_thread(
                command, client_id, client_socket
            )
        elif shlex.split(command.lower())[0] == "/server_file_transfer_port":
            with self.file_transfer_server_port_lock:
                self.file_transfer_server_port = int(command.split(" ")[1])
                try:
                    file_client_id = int(command.split(" ")[2])
                    self.file_server_port_list.append(
                        [self.file_transfer_server_port, file_client_id]
                    )
                except:
                    traceback.print_exc()
                    pass
        elif shlex.split(command.lower())[0] == "/forward_item":
            threading.Thread(
                target=self._forward_item_handler,
                args=(client_socket, client_address, command),
                daemon=True,
            ).start()
            return None
        elif shlex.split(command.lower())[0] == "/pause_trans":
            self._forward_pause_target(client_address, command)
            return None
        elif shlex.split(command.lower())[0] == "/start_trans":
            self._forward_resume_target(client_address, command)
            return None
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_key_exchange"
        ):
            parts = command.split(" ")
            client_nonce = parts[1] if len(parts) > 1 else ""
            force = len(parts) > 2 and parts[2] == "1"
            self._crypto_on_client_hello(client_socket, client_address, client_nonce, force)
            return None
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_pub_key"
        ):
            with self._crypto_lock:  # only accept pushes from a connection that started the handshake (else the TOFU registry could be poisoned)
                handshaking = client_address in self._crypto_state
            if not handshaking:
                print(f"crypto: ignoring /crypto_pub_key from non-handshaking peer {client_id}")
                return None
            self.file_transfer_server_recv_server_start_thread(client_id, client_socket, command)
            return None
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_pub_key_request"
        ):
            threading.Thread(  # client asks the server to push its public key
                target=self._crypto_push_pub_to_client,
                args=(client_socket, client_address),
                daemon=True,
            ).start()
            return None
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_key_exchange_ack"
        ):
            parts = command.split(" ")
            if len(parts) < 3:
                return None
            client_nonce = parts[1]
            client_needs_server_pub = parts[2]
            force = len(parts) > 3 and parts[3] == "1"
            with self._crypto_lock:
                state = self._crypto_state.get(client_address)
                if state is not None:
                    state["peer_nonce"] = client_nonce  # reply to a server-initiated hello: fresh session nonce from the client's ack
            if force or client_needs_server_pub == "1":
                threading.Thread(
                    target=self._crypto_push_pub_to_client,
                    args=(client_socket, client_address),
                    daemon=True,
                ).start()
            return None
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_ready"
        ):
            with self._crypto_lock:
                state = self._crypto_state.setdefault(
                    client_address, self._new_crypto_state(force=False)
                )
                state["peer_ready"] = True
            self._crypto_try_server_flip(client_socket, state)
            return None
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_reject"
        ):
            reason = command[len("/crypto_reject") :].strip()  # the peer rejected our public key (TOFU mismatch)
            print(f"crypto: connection rejected by peer: {reason}")
            try:
                client_socket.close()
            except Exception:
                traceback.print_exc()
            return None
        else:
            cmd_parts = shlex.split(command.strip())
            if not cmd_parts:
                return None
            cmd_name = cmd_parts[0].lower()
            if cmd_name in self._custom_handlers[0]:
                handler = self._custom_handlers[0][cmd_name]
                run_in_thread = self._custom_handler_threaded[0].get(cmd_name, False)
                if run_in_thread:
                    self.submit_task(
                        self._execute_custom_handler,
                        handler,
                        command,
                        client_socket,
                        client_address,
                    )
                    return "Command received, processing in background.\n"
                else:
                    response = self._execute_custom_handler(
                        handler, command, client_socket, client_address
                    )
                    return response
            else:
                print(f"Unknown command: {command}")

    def _execute_custom_handler(self, handler, command, client_socket=None, client_address=None):
        try:
            result = handler(client_socket, client_address, command)
            if result is not None:
                if isinstance(result, str) and not result.endswith("\n"):
                    result += "\n"
                try:
                    self.send_message(client_socket, result)
                except Exception as e:
                    print(f"Error sending message: {e}")
                return result
            return None
        except Exception as e:
            error_msg = f"Error in custom command handler: {e}\n"
            traceback.print_exc()
            try:
                self.send_message(client_socket, error_msg)
            except Exception as e:
                print(f"Error sending error message: {e}")
            return error_msg

    def file_folder_transfer_server_recv_server_start_thread(  # start a file folder server thread on server
        self, command, client_id, client_socket
    ):
        command_part = shlex.split(command)
        relative_folder_path = command_part[1]
        # >= 4 tokens means a file transfer (/file_folder <rel> <file> [<dest>] <id>);
        # 2-3 tokens is folder creation (optional destination, no client id)
        file_name = command_part[2] if len(command_part) >= 4 else None
        folder_transfer_server_recv_server_start_thread = threading.Thread(
            target=self.file_transfer_server_recv_server_start,
            args=(client_id, client_socket, command, relative_folder_path, file_name),
            daemon=True,
        )
        folder_transfer_server_recv_server_start_thread.start()

    def file_transfer_server_recv_server_start_thread(  # start a file server thread on server
        self, client_id, client_socket, command
    ):
        file_transfer_server_recv_server_start_thread = threading.Thread(
            target=self.file_transfer_server_recv_server_start,
            args=(client_id, client_socket, command),
            daemon=True,
        )
        file_transfer_server_recv_server_start_thread.start()

    def file_transfer_server_recv_server_start(  # deal with file transfer request server on server from client and receive file from client
        self, client_id, client_socket, command, new_save_path=None, file_name=None
    ):
        file_transfer_server_port = self.palloc()
        self.file_transfer_mode_recv(
            self.host,
            file_transfer_server_port,
            client_socket,
            client_id,
            new_save_path,
            file_name,
            command,
        )
        self.pfree(file_transfer_server_port)

    def file_transfer_mode_recv(
        self,
        server_file_address,
        server_file_port,
        client_socket,
        client_id,
        new_save_path,
        file_name,
        command,
    ):
        file_running = True
        client_file_socket = None
        server_file_socket = None
        save_path = None

        def close_socket():
            nonlocal file_running
            nonlocal client_file_socket
            nonlocal server_file_socket
            file_running = False
            client_file_socket.close()
            server_file_socket.close()

        def setting_file_save_path():
            nonlocal save_path
            save_path = self.file_transfer_dir
            if new_save_path or destination_path:
                if destination_path:
                    if os.path.isabs(destination_path):
                        save_path = destination_path
                    else:
                        save_path = os.path.join(save_path, destination_path)
                if new_save_path:
                    path_list = new_save_path.split("/")
                    if path_list and path_list[0] == "":
                        del path_list[0]
                    for node in path_list:
                        save_path = os.path.join(save_path, node)
                os.makedirs(save_path, exist_ok=True)
            if file_name or new_save_path == None:
                return save_path
            close_socket()
            return None

        def file_transfer_client_recv(client_id):
            nonlocal file_running
            nonlocal client_file_socket
            nonlocal server_file_socket
            nonlocal save_path
            filename = None
            full_path = None
            self.send_message(client_file_socket, self.server_start_file_transfer_sign)
            try:
                name_len_bytes = b""
                while len(name_len_bytes) < 4:
                    chunk = self.receive_message(client_file_socket, 4 - len(name_len_bytes))
                    print(chunk)
                    if not chunk:
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        raise ConnectionError(
                            "ErrorWhileReceivingFileNameLength: client disconnected"
                        )
                    name_len_bytes += chunk
                    if name_len_bytes.strip() == self.error_sign.encode("utf-8"):
                        close_socket()
                        raise ConnectionError(
                            "ErrorSignReceivedWhileReceivingFileNameLength: client reported error and disconnected"
                        )
                name_len = int.from_bytes(name_len_bytes, "big")
                file_name_encoded = b""
                while len(file_name_encoded) < name_len:
                    chunk = self.receive_message(
                        client_file_socket, name_len - len(file_name_encoded)
                    )
                    if not chunk:
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        raise ConnectionError("ErrorWhileReceivingFileName: client disconnected")
                    file_name_encoded += chunk
                    if file_name_encoded.strip() == self.error_sign.encode("utf-8"):
                        close_socket()
                        raise ConnectionError(
                            "ErrorSignReceivedWhileReceivingFileName: client reported error and disconnected"
                        )
                filename = file_name_encoded.decode("utf-8")
                filename = filename.strip()
                filename = os.path.basename(filename)
                size_bytes = b""
                while len(size_bytes) < 8:
                    chunk = self.receive_message(client_file_socket, 8 - len(size_bytes))
                    if not chunk:
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        raise ConnectionError("ErrorWhileReceivingFileSize: client disconnected")
                    size_bytes += chunk
                    if size_bytes.strip() == self.error_sign.encode("utf-8"):
                        close_socket()
                        raise ConnectionError(
                            "ErrorSignReceivedWhileReceivingFileSize: client reported error and disconnected"
                        )
                file_size = int.from_bytes(size_bytes, "big")
                self.send_message(client_file_socket, self.server_received_file_header_sign)
                original_filename = filename
                if file_name:
                    final_filename = file_name.strip()
                elif command_part[0] == "/crypto_pub_key":
                    # concurrent key pushes share received_files/: a fixed name would be
                    # overwritten/moved by another connection before this one consumes it
                    final_filename = "{}.pem".format(uuid.uuid4().hex)
                else:
                    final_filename = os.path.basename(original_filename)
                full_path = os.path.join(save_path, final_filename).strip()
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                with open(full_path.strip(), "wb") as f:
                    remaining = file_size
                    while remaining > 0:
                        chunk = self.receive_message(client_file_socket, min(65536, remaining))
                        if not chunk:
                            try:
                                self.send_message(client_file_socket, self.error_sign)
                            except:
                                traceback.print_exc()
                                pass
                            close_socket()
                            raise ConnectionError(
                                "ErrorWhileReceivingFileData: client disconnected"
                            )
                        f.write(chunk)
                        remaining -= len(chunk)
                # TOFU first: the ack is only a notification, and a failed
                # ack send must not skip the key registration (the server
                # would never announce readiness and the handshake hangs)
                self._notify_file_received(client_id, full_path, final_filename, file_size, command)
                print(f"file {filename} received from {client_id}, size {file_size} bytes")
                if command_part[0] == "/crypto_pub_key":
                    with self._crypto_lock:
                        peer_addr = self._crypto_sock_addr.get(client_socket)
                    if peer_addr:
                        self._crypto_store_received_pub(full_path, "client", peer_addr)
                    else:
                        # the peer mapping is already gone (connection torn
                        # down mid-handshake): store nothing, leave nothing
                        try:
                            os.remove(full_path)
                        except OSError:
                            pass
                try:
                    self.send_message(client_file_socket, self.server_received_file_data_sign)
                except Exception:
                    traceback.print_exc()
                close_socket()
            except Exception as e:
                traceback.print_exc()
                if full_path is not None and os.path.exists(full_path):
                    try:
                        os.remove(full_path)  # partial transfer: no half-written leftovers
                    except OSError:
                        pass
                try:
                    self.send_message(client_file_socket, self.error_sign)
                except:
                    traceback.print_exc()
                    pass
                close_socket()
                print(f"ErrorWhileReceiveFile: {e}")
                return False
            else:
                close_socket()
                return None

        server_file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_file_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_file_socket.bind((server_file_address, server_file_port))
        file_transfer_server_port = server_file_socket.getsockname()[1]
        command_part = shlex.split(command)
        file_client_id = command_part[len(command_part) - 1]
        destination_path = _parse_destination_path(command_part)
        transfer_server_port_msg = "/server_file_transfer_port {} {}\n".format(
            file_transfer_server_port, file_client_id
        )
        self._send_raw(client_socket, transfer_server_port_msg)  # protocol control message: always plaintext (like the handshake)
        server_file_socket.listen(1)
        try:
            client_file_socket, client_file_address = server_file_socket.accept()
            is_open_file_transfer = setting_file_save_path()
            if is_open_file_transfer == None:
                pass
            else:
                threading.Thread(
                    target=file_transfer_client_recv, args=(client_id,), daemon=True
                ).start()
        except Exception as e:
            print(f"\nget file transfer msg error: {e}")
            traceback.print_exc()
            close_socket()
        finally:
            server_file_socket.close()

    def diff_multiple_file_diff_multiple_client_transfer_server_recv_client_start(self, message):
        command_part = shlex.split(message)
        del command_part[0]
        # optional destination directory is the last argument, after the
        # client addresses (which are always tuples)
        destination_path = None
        if command_part and not (
            command_part[-1].startswith("(") and command_part[-1].endswith(")")
        ):
            destination_path = command_part[-1]
            del command_part[-1]
        file_client_pair_list = []
        file_list = []
        client_list = []
        command_part_addr_times = 0
        for command_part_index in range(len(command_part)):
            part = command_part[command_part_index]
            if part.startswith("(") and part.endswith(")"):
                try:
                    client_addr = ast.literal_eval(part)
                    client_list.append(client_addr)
                    command_part_addr_times += 1
                    if command_part_index == len(command_part) - 1:
                        file_client_pair = [client_list, file_list]
                        file_client_pair_list.append(file_client_pair)
                except:
                    traceback.print_exc()
            else:
                if command_part_addr_times != 0:
                    file_client_pair = [client_list, file_list]
                    file_client_pair_list.append(file_client_pair)
                    file_list = []
                    client_list = []
                    file_list.append(part)
                    command_part_addr_times = 0
                else:
                    file_list.append(part)
        for each_file_client_pair in file_client_pair_list:
            file_transfer_command_message = ""
            file_folder_transfer_command_message = ""
            for file in each_file_client_pair[1]:
                try:
                    if os.path.isfile(file):
                        if file_transfer_command_message == "":
                            file_transfer_command_message = "/file {}".format(shlex.quote(file))
                        else:
                            file_transfer_command_message += " {}".format(shlex.quote(file))
                    elif os.path.isdir(file):
                        if file_folder_transfer_command_message == "":
                            file_folder_transfer_command_message = "/file_folder {}".format(
                                shlex.quote(file)
                            )
                        else:
                            file_folder_transfer_command_message += " {}".format(shlex.quote(file))
                except:
                    traceback.print_exc()
                    print(
                        f"ErrorWhileParsingFilePath: {file} is not a valid file or folder path, skipped"
                    )
                    pass
            for client_addr in each_file_client_pair[0]:
                if file_transfer_command_message != "":
                    file_transfer_command_message += " {}".format(shlex.quote(str(client_addr)))
                if file_folder_transfer_command_message != "":
                    file_folder_transfer_command_message += " {}".format(
                        shlex.quote(str(client_addr))
                    )
            if destination_path:
                if file_transfer_command_message != "":
                    file_transfer_command_message += " {}".format(shlex.quote(destination_path))
                if file_folder_transfer_command_message != "":
                    file_folder_transfer_command_message += " {}".format(
                        shlex.quote(destination_path)
                    )
            if file_transfer_command_message != "":
                self.multiple_file_multiple_client_transfer_server_recv_client_start(
                    file_transfer_command_message
                )
            if file_folder_transfer_command_message != "":
                self.multiple_file_multiple_client_transfer_server_recv_client_start(
                    file_folder_transfer_command_message
                )

    def multiple_file_multiple_client_transfer_server_recv_client_start(self, message):
        command_part = shlex.split(message)
        command_type = command_part[0]
        del command_part[0]
        # optional destination directory is the last argument, after the
        # client addresses (which are always tuples)
        destination_path = None
        if command_part and not (
            command_part[-1].startswith("(") and command_part[-1].endswith(")")
        ):
            destination_path = command_part[-1]
            del command_part[-1]
        transfer_file_list = []
        client_addr_list = []
        for part in command_part:
            if part.startswith("(") and part.endswith(")"):
                try:
                    client_addr = ast.literal_eval(part)
                    client_addr_list.append(client_addr)
                except:
                    traceback.print_exc()
            else:
                transfer_file_list.append(part)
        for client_addr in client_addr_list:
            for transfer_file in transfer_file_list:
                if command_type == "/multiple_file_multiple_client":
                    # console form: classify each item by its own type
                    if os.path.isfile(transfer_file):
                        item_type = "/file"
                    elif os.path.isdir(transfer_file):
                        item_type = "/file_folder"
                    else:
                        print(
                            f"ErrorWhileParsingFilePath: {transfer_file} is not "
                            "a valid file or folder path, skipped"
                        )
                        continue
                else:
                    item_type = command_type
                if item_type == "/file":
                    file_transfer_command_message = "/file {} {}".format(
                        shlex.quote(transfer_file), shlex.quote(str(client_addr))
                    )
                    if destination_path:
                        file_transfer_command_message += " {}".format(
                            shlex.quote(destination_path)
                        )
                    self.file_transfer_server_recv_client_start_thread(
                        file_transfer_command_message
                    )
                    print(f"start to send file command: {file_transfer_command_message}")
                elif item_type == "/file_folder":
                    folder_transfer_command_message = "/file_folder {} {}".format(
                        shlex.quote(transfer_file), shlex.quote(str(client_addr))
                    )
                    if destination_path:
                        folder_transfer_command_message += " {}".format(
                            shlex.quote(destination_path)
                        )
                    self.folder_file_transfer_server_recv_client_start(
                        folder_transfer_command_message
                    )
                    print(f"start to send folder command: {folder_transfer_command_message}")

    def folder_file_transfer_server_recv_client_start(self, message):
        command_part = shlex.split(message)
        folder_path = command_part[1]
        if command_part[-1].startswith("(") and command_part[-1].endswith(")"):
            destination_path = None
            client_addr = ast.literal_eval(command_part[-1])
        else:
            destination_path = command_part[-1]
            client_addr = ast.literal_eval(command_part[-2])
        client_socket = self.clients[client_addr]["socket"]
        if os.path.isdir(folder_path) == False:
            print(f"{folder_path} is not a valid folder path")
            return False
        base_path = os.path.dirname(folder_path)

        def get_relative_path(base_path, abs_path):
            base = os.path.normpath(base_path)
            abs_ = os.path.normpath(abs_path)
            common = os.path.commonpath([base, abs_])
            if common != base:
                raise ValueError(f"'{abs_path}' is not a subpath of '{base_path}'")
            rel = os.path.relpath(abs_, base)
            if rel == ".":
                return ""
            rel = rel.replace(os.sep, "/")
            return "/" + rel

        def send_folder_transfer_command(folder_path, file_name=None, abspath=None):
            folder_transfer_command_message = "/file_folder {}".format(shlex.quote(folder_path))
            if file_name:
                each_file_transfer_command_message = "/file_folder {} {} {}".format(
                    shlex.quote(folder_path), shlex.quote(file_name), shlex.quote(str(client_addr))
                )
                if destination_path:
                    each_file_transfer_command_message += " {}".format(
                        shlex.quote(destination_path)
                    )
                self.file_transfer_server_recv_client_start_thread(
                    each_file_transfer_command_message, abspath
                )
                print(f"start to send folder command: {each_file_transfer_command_message}")
            else:
                if destination_path:
                    folder_transfer_command_message += " {}".format(
                        shlex.quote(destination_path)
                    )
                self.send_message(client_socket, folder_transfer_command_message.strip())
                print(f"start to send folder command: {folder_transfer_command_message}")

        def start_file_transfer_with_limit(rel_dir, file, root):
            cmd = "/file_folder {} {} {}".format(
                shlex.quote(rel_dir), shlex.quote(file), shlex.quote(str(client_addr))
            )
            if destination_path:
                cmd += " {}".format(shlex.quote(destination_path))

            def limited_transfer():
                self.file_semaphore.acquire()
                try:
                    self.file_transfer_server_recv_client_start(cmd, root)
                finally:
                    self.file_semaphore.release()

            thread = threading.Thread(target=limited_transfer, daemon=True)
            thread.start()
            print(f"start to send file: {cmd} (limit {self.max_file_transfer_thread_num})")

        def get_all_files_in_folder():
            for root, dirs, files in os.walk(folder_path):
                rel_dir = get_relative_path(base_path, root)
                if root != folder_path:
                    send_folder_transfer_command(rel_dir)
                for file in files:
                    start_file_transfer_with_limit(rel_dir, file, root)
            print(f"finished sending all files in folder {folder_path}")

        transfer_path = get_relative_path(base_path, folder_path)
        send_folder_transfer_command(transfer_path)
        get_all_files_in_folder()

    def file_transfer_server_recv_client_start_thread(self, message, file_folder_abspath=None):
        file_transfer_server_recv_client_start_thread = threading.Thread(
            target=self.file_transfer_server_recv_client_start,
            args=(message, file_folder_abspath),
            daemon=True,
        )
        file_transfer_server_recv_client_start_thread.start()

    def file_transfer_server_recv_client_start(self, message, file_folder_abspath):
        client_id = None
        command_part = shlex.split(message)
        # optional destination directory comes after the client address tuple
        if command_part[-1].startswith("(") and command_part[-1].endswith(")"):
            client_ip = ast.literal_eval(command_part[-1])
        else:
            client_ip = ast.literal_eval(command_part[-2])
        client_address = client_ip[0]
        try:
            client_socket = self.clients[client_ip]["socket"]
        except:
            print(
                "ErrorWhileSearchingClientSocket: can not find the client socket, file sending failed"
            )
            traceback.print_exc()
            return False
        print(client_socket, client_address, message)
        with self.file_client_id_lock:
            client_id = copy.copy(self.file_client_id)
            send_msg = message.strip() + " " + str(self.file_client_id) + "\n"
            self.file_client_id += 1
        try:
            waiting_time = 0
            if shlex.split(message.lower())[0] == "/file_folder":
                filename = os.path.join(file_folder_abspath, shlex.split(message)[2])
            else:
                filename = shlex.split(message)[1]
            self.send_message(client_socket, send_msg)
            file_transfer_client_port = self.palloc()
            file_server_port = None
            is_find_port = True
            while is_find_port:
                time.sleep(1)
                if len(self.file_server_port_list) > 0:
                    with self.file_transfer_server_port_lock:
                        for port_info in self.file_server_port_list:
                            if port_info[1] == client_id:
                                file_server_port = port_info[0]
                                self.file_server_port_list.remove(port_info)
                                is_find_port = False
                                break
                    pass
                waiting_time += 1
                if waiting_time >= 20:
                    print(
                        "ErrorWhileReceiveFileServerPort: transfer port waiting timeout, file sending failed"
                    )
                    return False
            self.file_transfer_mode(
                filename, client_address, file_server_port, file_transfer_client_port
            )
            self.pfree(file_transfer_client_port)
        except IndexError:
            print("invalid command, please use '/file <filename>'")
            traceback.print_exc()

    def file_transfer_mode(  # noqa: PLR0911 - peer-close and timeout exits are distinct outcomes
        self, filename, server_address, server_port, client_port, pause_fid=None
    ):
        print(f"start to send file: {filename}")
        client_file_socket = None
        reset_time = 0

        def close_socket():
            nonlocal file_running
            nonlocal client_file_socket
            file_running = False
            client_file_socket.close()
            with self.file_client_id_lock:
                self.file_client_id -= 1

        while True:
            try:
                client_file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_file_socket.bind((self.host, client_port))
                client_file_socket.connect((server_address, server_port))
                break
            except Exception as e:
                print(f"file transfer connection error: {e}")
                traceback.print_exc()
                if reset_time >= 20:
                    close_socket()
                    print("unable to connect to file transfer server, file sending failed")
                    return False
                reset_time += 1
                time.sleep(1)
        file_running = True
        file_receive_data_from_server = ""

        def receive_file_transfer_messages():
            nonlocal file_running
            nonlocal client_file_socket
            nonlocal file_receive_data_from_server
            while file_running:
                try:
                    data = self.receive_message(client_file_socket, 4096)
                    if not data:
                        print("\nbreak the file transfer connection from server")
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        break
                    file_receive_data_from_server = data.decode("utf-8").strip()
                    if file_receive_data_from_server == self.error_sign:
                        print("\nError sign received from server, file transfer may have failed")
                        close_socket()
                        break
                except Exception as e:
                    print(f"\nget file transfer msg error: {e}")
                    traceback.print_exc()
                    try:
                        self.send_message(client_file_socket, self.error_sign)
                    except:
                        traceback.print_exc()
                        pass
                    close_socket()
                    break

        receive_thread = threading.Thread(target=receive_file_transfer_messages, daemon=True)
        receive_thread.start()
        waiting_time = 0
        try:
            while True:
                if file_receive_data_from_server == self.server_start_file_transfer_sign:
                    break
                if file_receive_data_from_server == self.error_sign:
                    close_socket()
                    break
                if not file_running:
                    # peer closed the file connection: the receive thread
                    # already cleaned up; nothing more to wait for
                    return False
                time.sleep(1)
                waiting_time += 1
                if waiting_time >= 10:
                    try:
                        self.send_message(client_file_socket, self.error_sign)
                    except:
                        traceback.print_exc()
                        pass
                    print(
                        f"ErrorWhileSendFile: \
                          Wait file transfer function start sign timeout, \
                          file {filename} sending failed"
                    )
                    close_socket()
                    return False
            waiting_time = 0
            file_size = os.path.getsize(filename)
            file_name_encoded = os.path.basename(filename).encode("utf-8")
            name_len = len(file_name_encoded)
            self.send_message(client_file_socket, name_len.to_bytes(4, "big"))
            self.send_message(client_file_socket, file_name_encoded)
            self.send_message(client_file_socket, file_size.to_bytes(8, "big"))
            with open(filename, "rb") as f:
                while True:
                    if pause_fid is not None:
                        with self._forward_pause_cond:
                            while self._forward_pause.get(pause_fid, False):
                                self._forward_pause_cond.wait()
                    file_data = f.read(65536)
                    if not file_data:
                        break
                    self.send_message(client_file_socket, file_data)
            extra_time = (file_size // (100 * 1024 * 1024)) * 10
            timeout = int(30 + extra_time)
            while True:
                if file_receive_data_from_server == self.server_received_file_data_sign:
                    break
                if file_receive_data_from_server == self.error_sign:
                    close_socket()
                    break
                if not file_running:
                    # peer closed the file connection: the receive thread
                    # already cleaned up; nothing more to wait for
                    return False
                time.sleep(1)
                waiting_time += 1
                if waiting_time >= timeout:
                    try:
                        self.send_message(client_file_socket, self.error_sign)
                    except:
                        traceback.print_exc()
                        pass
                    close_socket()
                    print(
                        f"ErrorWhileSendFileData: \
                          wait file transfer confirmation sign timeout, \
                          file {filename} sending may have failed"
                    )
                    return False
            print(f"Success: file {filename} sent successfully")
            close_socket()
            return True
        except FileNotFoundError:
            traceback.print_exc()
            try:
                self.send_message(client_file_socket, self.error_sign)
            except:
                traceback.print_exc()
                pass
            close_socket()
            print(f"file {filename} not exist")
            return False
        except Exception as e:
            traceback.print_exc()
            try:
                self.send_message(client_file_socket, self.error_sign)
            except:
                traceback.print_exc()
                pass
            close_socket()
            print(f"send error: {e}")
            return False

    # ---- native in-memory forward relay (server side) ----------------------

    def _forward_alloc_fid(self):
        with self._forward_fid_lock:
            self._forward_fid += 1
            return self._forward_fid

    def _forward_notify_error(self, sock, msg):
        try:
            self.send_message(sock, f"/forward_error {msg}")
        except Exception:
            pass

    def _forward_parse_items_addrs(self, tokens):
        items = []
        addrs = []
        for token in tokens:
            if token.startswith("(") and token.endswith(")"):
                try:
                    addr = ast.literal_eval(token)
                except (ValueError, SyntaxError):
                    items.append(token)
                    continue
                if isinstance(addr, tuple) and len(addr) == 2 and isinstance(addr[0], str):
                    addrs.append(addr)
                else:
                    items.append(token)
            else:
                items.append(token)
        return items, addrs

    def _forward_item_handler(self, sock, addr, cmd):
        """Server-side entry for /forward_item <kind> <...> <addr...>.

        The server never touches disk: it only relays the uploaded byte
        stream to every reachable target client from memory.
        """
        try:
            parts = shlex.split(cmd)
        except Exception:
            self._forward_notify_error(sock, "invalid forward command")
            return
        if len(parts) < 3:
            self._forward_notify_error(sock, "invalid forward command")
            return
        kind = parts[1].lower()
        rest = parts[2:]
        # optional destination directory is the very last argument, after the
        # client address tuples
        destination_path = None
        if rest and not (rest[-1].startswith("(") and rest[-1].endswith(")")):
            destination_path = rest[-1]
            rest = rest[:-1]
        items, addrs = self._forward_parse_items_addrs(rest)
        valid_targets = []
        for target in addrs:
            if target == (self.host, self.port):
                print(f"forward: target {target} is the server itself, skipped")
                continue
            if target not in self.clients:
                print(f"forward: target {target} is unreachable, skipped")
                continue
            valid_targets.append(target)
        if not valid_targets:
            self._forward_notify_error(sock, "no reachable targets to forward to")
            return
        if kind == "file":
            if not items:
                self._forward_notify_error(sock, "missing file name")
                return
            rel_dir = ""
            fname = items[0]
        elif kind == "folder":
            if len(items) < 2:
                self._forward_notify_error(sock, "missing folder file")
                return
            rel_dir = items[0]
            fname = items[1]
        else:
            self._forward_notify_error(sock, f"unknown forward kind: {kind}")
            return
        threading.Thread(
            target=self._forward_relay,
            args=(sock, kind, rel_dir, fname, valid_targets, destination_path),
            daemon=True,
        ).start()

    def _forward_read_sign(self, sock, sign, timeout=15):
        """Read from a target transfer socket until ``sign`` is seen."""
        buf = b""
        target = sign.encode("utf-8")
        err = self.error_sign.encode("utf-8")
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                data = sock.recv(4096)
            except Exception:
                return False
            if not data:
                return False
            buf += data
            if target in buf:
                return True
            if err in buf:
                return False
        return False

    def _forward_recv_exact(self, sock, n):
        buf = b""
        while len(buf) < n:
            data = sock.recv(n - len(buf))
            if not data:
                raise ConnectionError("forward: uploader closed the stream")
            buf += data
        return buf

    def _forward_pause_target(self, client_address, command):
        try:
            fid = int(shlex.split(command)[1])
        except Exception:
            return
        with self._forward_relays_lock:
            relay = self._forward_relays.get(fid)
        if relay is None:
            return
        with relay["cond"]:
            if client_address in relay["writer_pause"]:
                relay["writer_pause"][client_address] = True

    def _forward_resume_target(self, client_address, command):
        try:
            fid = int(shlex.split(command)[1])
        except Exception:
            return
        with self._forward_relays_lock:
            relay = self._forward_relays.get(fid)
        if relay is None:
            return
        with relay["cond"]:
            if client_address in relay["writer_pause"]:
                relay["writer_pause"][client_address] = False
                relay["cond"].notify_all()

    def _forward_relay(self, forwarder_sock, kind, rel_dir, fname, targets, destination_path=None):
        """Relay one file/folder item to every target, streaming from the
        uploader's transfer connection with bounded in-memory buffering."""
        fid = self._forward_alloc_fid()
        tfids = [self._forward_alloc_fid() for _ in targets]
        relay = {
            "cond": threading.Condition(),
            "writer_pause": {target: False for target in targets},
        }
        with self._forward_relays_lock:
            for key in [fid] + tfids:
                self._forward_relays[key] = relay
        try:
            # ask every target to open a receive transfer socket; the
            # destination directory (if any) goes before the target id so
            # the receiver's own destination parsing sees it
            for target, tfid in zip(targets, tfids):
                try:
                    t_sock = self.clients[target]["socket"]
                except Exception as e:
                    print(f"forward: target {target} missing, skipped: {e}")
                    continue
                if kind == "file":
                    if destination_path:
                        self.send_message(
                            t_sock, f"/file {shlex.quote(fname)} {shlex.quote(destination_path)} {tfid}"
                        )
                    else:
                        self.send_message(t_sock, f"/file {shlex.quote(fname)} {tfid}")
                else:
                    if destination_path:
                        self.send_message(
                            t_sock,
                            f"/file_folder {shlex.quote(rel_dir)} {shlex.quote(fname)} "
                            f"{shlex.quote(destination_path)} {tfid}",
                        )
                    else:
                        self.send_message(
                            t_sock,
                            f"/file_folder {shlex.quote(rel_dir)} {shlex.quote(fname)} {tfid}",
                        )
            # collect every target's advertised transfer port
            ports = {}
            deadline = time.time() + 20
            while len(ports) < len(tfids) and time.time() < deadline:
                with self.file_transfer_server_port_lock:
                    for info in list(self.file_server_port_list):
                        if info[1] in tfids and info[1] not in ports:
                            ports[info[1]] = info[0]
                            self.file_server_port_list.remove(info)
                if len(ports) < len(tfids):
                    time.sleep(0.05)
            if len(ports) < len(tfids):
                self._forward_notify_error(forwarder_sock, "target port setup timed out")
                return
            # connect to each target and wait for its start sign
            target_conns = []
            for target, tfid in zip(targets, tfids):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.connect((target[0], ports[tfid]))
                except Exception as e:
                    print(f"forward: cannot connect to target {target}: {e}")
                    sock.close()
                    continue
                if not self._forward_read_sign(sock, self.server_start_file_transfer_sign):
                    print(f"forward: target {target} did not start, skipped")
                    sock.close()
                    continue
                target_conns.append((tfid, target, sock))
            if not target_conns:
                self._forward_notify_error(forwarder_sock, "no target accepted the transfer")
                return
            # open the upload listener and hand it to the forwarder
            up_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            up_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            up_sock.bind((self.host, self.palloc()))
            sport = up_sock.getsockname()[1]
            up_sock.listen(1)
            self.send_message(forwarder_sock, f"/forward_upload {fid} {sport}")
            up_client, _ = up_sock.accept()
            self.send_message(up_client, self.server_start_file_transfer_sign)
            self._forward_pump(up_client, target_conns, forwarder_sock, fid, fname, relay)
            up_client.close()
            up_sock.close()
            self.pfree(sport)
        finally:
            with self._forward_relays_lock:
                for key in [fid] + tfids:
                    self._forward_relays.pop(key, None)

    def _forward_pump(self, up_sock, target_conns, forwarder_sock, fid, fname, relay):
        """Pump the uploader's byte stream to every target from memory.

        The total bytes held in the per-target queues never exceeds
        ``max_mem_buff``: once it does, the uploader is told to pause
        (``/pause_trans``) and the reader stops pulling; as the writers
        drain below the low-water mark the uploader is resumed
        (``/start_trans``).
        """
        max_bytes = max(1, self.max_mem_buff)
        low_water = max(1, max_bytes // 2)
        cond = relay["cond"]
        writer_pause = relay["writer_pause"]
        state = {"buffered": 0, "paused": False}
        queues = {tfid: queue.Queue() for tfid, _, _ in target_conns}
        END = object()
        result = {"ok": 0}

        def enqueue(chunk):
            with cond:
                state["buffered"] += len(chunk) * len(target_conns)
                for q in queues.values():
                    q.put(chunk)
                if not state["paused"] and state["buffered"] > max_bytes:
                    state["paused"] = True
                    try:
                        self.send_message(forwarder_sock, f"/pause_trans {fid}")
                    except Exception:
                        pass

        def writer(tfid, target, sock):
            q = queues[tfid]
            while True:
                with cond:
                    while writer_pause.get(target, False):
                        cond.wait()
                item = q.get()
                if item is END:
                    break
                try:
                    sock.sendall(item)
                except Exception:
                    with cond:
                        while not q.empty():
                            state["buffered"] -= len(q.get())
                    break
                with cond:
                    state["buffered"] -= len(item)
                    if state["paused"] and state["buffered"] <= low_water:
                        state["paused"] = False
                        try:
                            self.send_message(forwarder_sock, f"/start_trans {fid}")
                        except Exception:
                            pass
                        cond.notify_all()
            if self._forward_read_sign(sock, self.server_received_file_data_sign):
                result["ok"] += 1
            try:
                sock.close()
            except Exception:
                pass

        def reader():
            try:
                name_len_b = self._forward_recv_exact(up_sock, 4)
                name_len = int.from_bytes(name_len_b, "big")
                name_b = self._forward_recv_exact(up_sock, name_len)
                size_b = self._forward_recv_exact(up_sock, 8)
                file_size = int.from_bytes(size_b, "big")
            except Exception as e:
                print(f"forward: uploader header read failed: {e}")
                for q in queues.values():
                    q.put(END)
                return
            for chunk in (name_len_b, name_b, size_b):
                enqueue(chunk)
            remaining = file_size
            while remaining > 0:
                with cond:
                    while state["paused"]:
                        cond.wait()
                chunk = self._forward_recv_exact(up_sock, min(65536, remaining))
                remaining -= len(chunk)
                enqueue(chunk)
            for q in queues.values():
                q.put(END)

        threads = [
            threading.Thread(target=writer, args=(tfid, target, sock), daemon=True)
            for tfid, target, sock in target_conns
        ]
        for t in threads:
            t.start()
        reader()
        for t in threads:
            t.join(timeout=120)
        try:
            self.send_message(up_sock, self.server_received_file_data_sign)
        except Exception:
            pass
        print(f"forward: relayed {fname} to {result['ok']}/{len(target_conns)} targets")

    def start_TCP_Server(self):  # set up server socket
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(self.max_clients)
            self.running = True
            print(f"TCP server deployed on {self.host}:{self.port}")
            print(f"max clients mount: {self.max_clients}")
            print("input '/stop' to stop the server\n")
            if self.is_input_command_in_console:
                input_thread = threading.Thread(
                    target=self.console_input, daemon=True
                )  # set up console input thread
                input_thread.start()
            else:
                pass
            while self.running:  # main loop to accept clients
                try:
                    client_socket, client_address = self.server_socket.accept()
                    if len(self.clients) >= self.max_clients:
                        self.send_message(client_socket, "Max connection mount, try latter")
                        client_socket.close()
                        continue
                    client_thread = threading.Thread(  # set up client handling thread
                        target=self.handle_client, args=(client_socket, client_address), daemon=True
                    )
                    client_thread.start()
                except OSError as e:
                    if not _is_closed_socket_error(e):
                        traceback.print_exc()
                    break  # server socket closed, exit loop
        except Exception as e:
            print(f"Server error: {e}")
            traceback.print_exc()
        finally:
            self.stop()

    def console_input(self):  # deal consule input
        while self.running:
            try:
                cmd = input()
                deal_cmd = cmd.strip()
                if deal_cmd.lower() == "/stop":
                    print("shutting down...")
                    self.running = False
                    self.stop()
                elif deal_cmd.lower() == "/status":
                    print(f"current connection count: {len(self.clients)}")
                    print(f"server running: {self.running}")
                elif deal_cmd.lower() == "/clients":
                    with self.client_lock:
                        for addr, info in self.clients.items():
                            print(f"  {info['id']} - connection time: {info['connected_time']}")
                elif shlex.split(deal_cmd)[0].lower() == "/send_msg":
                    self.send_msg_to_specific_client(deal_cmd)
                elif shlex.split(deal_cmd)[0].lower() == "/file":
                    self.file_transfer_server_recv_client_start_thread(deal_cmd)
                elif shlex.split(deal_cmd)[0].lower() == "/file_folder":
                    self.folder_file_transfer_server_recv_client_start(deal_cmd)
                elif shlex.split(deal_cmd)[0].lower() == "/multiple_file_multiple_client":
                    self.multiple_file_multiple_client_transfer_server_recv_client_start(
                        deal_cmd
                    )
                elif shlex.split(deal_cmd)[0].lower() == "/diff_multiple_file_diff_multiple_client":
                    self.diff_multiple_file_diff_multiple_client_transfer_server_recv_client_start(
                        deal_cmd
                    )
                elif shlex.split(deal_cmd)[0].lower() in ("/forward_file", "/forward_folder"):
                    print(
                        "forward commands are client-only; "
                        "run them on a client console, not on the server"
                    )
                elif shlex.split(deal_cmd)[0].lower() == "/help":
                    help_text = [
                        "avalable commands:",
                        "/stop - stop the server\n",
                        "\t/status - display server status\n",
                        "\t/clients - display connected clients\n",
                        "\t/send_msg <message1> <message2> ... ",
                        "<client_id1> <client_id2> ... <messageN>",
                        " ... <client_idN> ... - send message or ",
                        "messages to specific client or clients\n",
                        "\t/file <file_path> <client_id> [destination_file_path] - ",
                        "send file to specific client\n",
                        "\t/file_folder <folder_path> <client_id> [destination_file_path] - ",
                        "send folder to specific client\n",
                        "\t/multiple_file_multiple_client <file1> <file2>",
                        " ... <client_id1> <client_id2> ... <fileN> ...",
                        " <client_idN> ... - send multiple files to multiple",
                        " clients, files should be before clients, ",
                        "and clients should be in format of (ip, port)\n",
                        "\t/diff_multiple_file_diff_multiple_client <file1> <file2>",
                        " ... <client_id1> <client_id2> ... <fileN> ...",
                        " <client_idN> ... - send multiple files to multiple clients",
                        " with different file list for each client, files and clients",
                        " should be in pairs, and clients should be in format of (ip, port)",
                    ]
                    print("\n" + " ".join(help_text) + "\n")
                else:
                    cmd_parts = shlex.split(deal_cmd)
                    if not cmd_parts:
                        return None
                    cmd_name = cmd_parts[0].lower()
                    if cmd_name in self._custom_handlers[1]:
                        handler = self._custom_handlers[1][cmd_name]
                        run_in_thread = self._custom_handler_threaded[1].get(cmd_name, False)
                        if run_in_thread:
                            self.submit_task(self._execute_custom_handler, handler, deal_cmd)
                            pass
                        else:
                            response = self._execute_custom_handler(handler, deal_cmd)
                            pass
                    else:
                        print("Unrecognized command, input '/help' for available commands")
            except KeyboardInterrupt:
                print("\nKeyboardInterrupt received, shutting down...")
                self.running = False
                self.stop()
                break
            except EOFError:
                print("EOF received, shutting down...")
                self.running = False
                self.stop()
                break
            except:
                traceback.print_exc()
                pass

    def stop(self):  # shutting down the server
        self.running = False
        self.free_port()
        with self.client_lock:  # close all clients connections
            for client_info in self.clients.values():
                try:
                    client_info["socket"].close()
                except:
                    traceback.print_exc()
                    pass
            self.clients.clear()
        if self.server_socket:  # close server socket
            self.server_socket.close()
            print("server stopped")


class TCP_Client_Base:  # TCP client class
    def __init__(
        self,
        host=None,
        client_host="127.0.0.1",
        port=65432,
        client_port=None,
        timeout=None,
        port_add_step=1,
        max_thread_num=10,
        is_input_command_in_console=True,
        is_wait_server=True,
        max_custom_workers=10,
        is_extend_command=False,
        is_enable_encrypto=True,
        is_custom_keys=None,
        max_mem_buff=2048,
    ):
        self.max_mem_buff = max_mem_buff * 1024 * 1024
        self._forward_upload_queue = queue.Queue()
        self._forward_pause = {}
        self._forward_pause_cond = threading.Condition()
        self._forward_error = None
        self.project_dir = os.path.dirname(os.path.abspath(__file__))
        self.file_transfer_dir = os.path.join(self.project_dir, "received_files")
        self.decode_command_table_file_path = os.path.join(
            self.project_dir, "decode_command_table.json"
        )
        self.project_info_dir = os.path.join(self.project_dir, ".Flow")
        if os.path.exists(self.project_info_dir) == False:
            os.mkdir(self.project_info_dir)
        self.project_temp_info_dir = os.path.join(self.project_info_dir, "temp_info")
        self.client_port_lock_file = os.path.join(
            self.project_temp_info_dir, "client_port_lock.lock"
        )
        if os.path.exists(self.project_temp_info_dir) == False:
            os.mkdir(self.project_temp_info_dir)
        self.all_allocated_ports_list = []
        self.is_hand_alloc_port = None
        self.file_transfer_server_port = None
        self.file_server_port_list = []
        self.file_transfer_server_port_lock = threading.Lock()
        self.client_ports_list = []
        self.client_port = client_port
        self.host = host
        self.client_host = client_host
        self.port = port
        self.port_add_step = port_add_step
        self.latest_port = None
        self.timeout = timeout
        self.client_socket = None
        self.running = False
        self.receive_thread = None
        self.command_decode_table_str = None
        self.max_thread_num = max_thread_num
        self.is_input_command_in_console = is_input_command_in_console
        self.is_wait_server = is_wait_server
        if self.is_wait_server and self.timeout != None:
            error_msg = [
                "wait_server_timeout is not applicable when is_wait_server is True,",
                "please set wait_server_timeout to None or set is_wait_server to False",
            ]
            raise ValueError(" ".join(error_msg))
        self.file_client_id = 0
        self.file_client_id_lock = threading.Lock()
        MAX_CONCURRENT_FILES = self.max_thread_num
        self.file_semaphore = threading.Semaphore(MAX_CONCURRENT_FILES)
        with open(self.decode_command_table_file_path, "r", encoding="utf-8") as f:
            self.command_decode_table_str = f.read()
        self.command_decode_table = ast.literal_eval(self.command_decode_table_str)
        self.send_file_header_sign = self.command_decode_table[0]["file_send_server_header"]
        self.send_file_data_sign = self.command_decode_table[0]["file_send_server_data"]
        self.server_received_file_header_sign = self.command_decode_table[0][
            "file_receive_client_header"
        ]
        self.server_received_file_data_sign = self.command_decode_table[0][
            "file_receive_client_data"
        ]
        self.server_start_file_transfer_sign = self.command_decode_table[0][
            "file_send_server_start_file_transfer"
        ]
        self.error_sign = self.command_decode_table[0]["file_send_receive_error"]
        self._custom_handlers = [{}, {}]
        self._custom_handler_threaded = [{}, {}]
        self._custom_executor = ThreadPoolExecutor(max_workers=max_custom_workers)
        self._task_semaphore = threading.Semaphore(max_custom_workers)
        # Inbound-event listeners (see add_message_listener/add_file_listener).
        # They run on the receive thread, so a listener must not block and must
        # never raise (exceptions are swallowed by the notify helpers).
        self._message_listeners = []
        self._file_listeners = []
        self._event_listeners_lock = threading.Lock()
        self.is_extend_command = is_extend_command
        self.is_enable_encrypto = is_enable_encrypto
        self.is_custom_keys = is_custom_keys
        self._crypto_lock = threading.RLock()  # serialises the crypto collections (no-GIL safe); held only around short ops, never across I/O
        self._encrypted_sockets = set()
        self._encrypted_recv_buffers = {}
        self._crypto_peer = {}  # socket -> (peer_role, peer_pem_path)
        self.crypto = (
            rsa_crypto.RsaCrypto("client", self.project_dir, custom_keys=is_custom_keys)
            if is_enable_encrypto
            else None
        )
        self._crypto_ack_event = threading.Event()
        self._crypto_exchange_done = threading.Event()
        self._crypto_server_pub_event = threading.Event()
        self._crypto_force = False
        self._crypto_push_active = set()
        self._crypto_send_lock = threading.Lock()  # send+encrypt ordered: wire order == seq order
        self._crypto_handshake_gen = 0  # bumped on handshake start; stale flows act/close no-op
        self._crypto_mode_event = threading.Event()  # set once the server announced its crypto mode
        self._crypto_mode_ok = False  # server mode matches ours (negotiated at connect)
        self._crypto_mode_timeout = 10  # seconds to wait for the server's mode announcement

        self._crypto_requested_server_pub = False
        self._crypto_server_ready = False
        self._crypto_own_ready_sent = False
        self._crypto_server_pub_ok = None  # None/True/False: TOFU check of server key
        self._crypto_server_pem_path = None  # cached server public key file
        self._crypto_handshake_started = False  # a hello was sent/received
        self._crypto_my_nonce = None  # session nonce we advertise
        self._crypto_server_nonce = None  # session nonce the server advertises
        self._crypto_send_seq = 0  # outbound encrypted-message counter
        self._crypto_recv_seq = 0  # inbound encrypted-message counter
        self._crypto_decode_failures = 0  # consecutive decode failures
        if self.is_extend_command:
            pass
        else:
            self.start_TCP_client()

    def register_command(self, command_name, handler, where_to_run, run_in_thread=False):
        registe_index = None
        if where_to_run == "server":
            registe_index = 0
        elif where_to_run == "client":
            registe_index = 1
        else:
            print(f"Invalid where_to_run value: {where_to_run}, must be 'server' or 'client'")
            return False
        self._custom_handlers[registe_index][command_name] = handler
        self._custom_handler_threaded[registe_index][command_name] = run_in_thread

    def add_message_listener(self, listener):
        """Register ``listener(message: str)`` for every inbound plain-text message.

        Plain messages are the chat/data lines received from the server that
        do not start with ``/`` (direct sends from the server, messages
        forwarded from other clients, protocol replies).  Commands are not
        reported here; they go through the registered command handlers.
        """
        with self._event_listeners_lock:
            if listener not in self._message_listeners:
                self._message_listeners.append(listener)

    def remove_message_listener(self, listener):
        """Unregister a listener previously added by ``add_message_listener``."""
        with self._event_listeners_lock:
            try:
                self._message_listeners.remove(listener)
            except ValueError:
                pass

    def add_file_listener(self, listener):
        """Register ``listener(full_path, name, size, command)`` for each saved inbound file.

        Fired after a file pushed by the server (a direct send, a forwarded
        file or folder item) has been fully written to ``file_transfer_dir``.
        ``command`` is the wire command that triggered the transfer, so a
        listener can recognise protocol pushes such as ``/crypto_pub_key``.
        """
        with self._event_listeners_lock:
            if listener not in self._file_listeners:
                self._file_listeners.append(listener)

    def remove_file_listener(self, listener):
        """Unregister a listener previously added by ``add_file_listener``."""
        with self._event_listeners_lock:
            try:
                self._file_listeners.remove(listener)
            except ValueError:
                pass

    def _notify_message_received(self, message):
        with self._event_listeners_lock:
            listeners = list(self._message_listeners)
        for listener in listeners:
            try:
                listener(message)
            except Exception:
                traceback.print_exc()

    def _notify_file_received(self, full_path, name, size, command):
        with self._event_listeners_lock:
            listeners = list(self._file_listeners)
        for listener in listeners:
            try:
                listener(full_path, name, size, command)
            except Exception:
                traceback.print_exc()

    def submit_task(self, func, *args, **kwargs):
        self._task_semaphore.acquire()
        future = self._custom_executor.submit(func, *args, **kwargs)
        future.add_done_callback(lambda f: self._task_semaphore.release())
        return future

    def alloc_port(self, port_add_step, port_range_num):
        if self.is_hand_alloc_port == True:
            while self.is_client_port_temp_info_file_locked():
                time.sleep(0.1)
            self.client_port_temp_info_file_lock()
            self.hand_alloc_port(port_add_step, port_range_num)
            self.client_port_temp_info_file_unlock()

    def free_port(self):
        if self.is_hand_alloc_port == True:
            while self.is_client_port_temp_info_file_locked():
                time.sleep(0.1)
            self.client_port_temp_info_file_lock()
            self.hand_free_port()
            self.client_port_temp_info_file_unlock()

    def client_port_temp_info_file_lock(self):
        with open(self.client_port_lock_file, "w", encoding="utf-8") as f:
            f.write("locked")

    def is_client_port_temp_info_file_locked(self):
        if os.path.exists(self.client_port_lock_file):
            return True
        else:
            return False

    def client_port_temp_info_file_unlock(self):
        if os.path.exists(self.client_port_lock_file):
            os.remove(self.client_port_lock_file)

    def hand_alloc_port(self, port_add_step, port_range_num):
        self.port_temp_info_path = os.path.join(self.project_temp_info_dir, "clients_port_info.log")
        server_port_temp_info_file_path = os.path.join(
            self.project_temp_info_dir, "server_port_info.log"
        )
        if os.path.exists(server_port_temp_info_file_path) == True:
            print(
                "Warning: server port info file exists, means the server has already allocated a port, may cause port conflict!"
            )
        self.port_add_step = port_add_step
        self.port_range_num = port_range_num
        self.add_latest_port = self.port + 1
        self.minus_latest_port = self.port
        self.alloc_add_port_lock = threading.Lock()
        self.alloc_minus_port_lock = threading.Lock()
        if os.path.exists(self.port_temp_info_path) == False:
            self.client_num = 0
            self.client_port_info = []
            self.min_port = self.port - self.port_add_step * self.port_range_num
            self.max_port = self.port + 1 + self.port_add_step * self.port_range_num
            each_client_info = {
                "client_id": self.client_num,
                "host": self.host,
                "port": self.port,
                "min_port": self.min_port,
                "max_port": self.max_port,
                "is_running": self.running,
            }
            self.client_port_info.append(each_client_info)
            with open(self.port_temp_info_path, "w", encoding="utf-8") as f:
                f.write(str(self.client_port_info))
        else:
            with open(self.port_temp_info_path, "r", encoding="utf-8") as f:
                self.client_port_info = ast.literal_eval(f.read())
            self.client_num = self.client_port_info[len(self.client_port_info) - 1]["client_id"] + 1
            auto_port_add = (
                self.client_port_info[len(self.client_port_info) - 1]["max_port"]
                + self.port_add_step * self.port_range_num
                + 1
            )
            auto_port_minus = (
                self.client_port_info[len(self.client_port_info) - 1]["min_port"]
                - self.port_add_step * self.port_range_num
                - 1
            )
            if self.port > auto_port_minus and self.port < auto_port_add:
                self.port = auto_port_add
            self.min_port = self.port - self.port_add_step * self.port_range_num
            self.max_port = self.port + 1 + self.port_add_step * self.port_range_num
            each_client_info = {
                "client_id": self.client_num,
                "host": self.host,
                "port": self.port,
                "min_port": self.min_port,
                "max_port": self.max_port,
                "is_running": self.running,
            }
            self.client_port_info.append(each_client_info)
            for is_running in range(len(self.client_port_info) - 1, -1, -1):
                if self.client_port_info[is_running]["is_running"] == False:
                    del self.client_port_info[is_running]
            with open(self.port_temp_info_path, "w", encoding="utf-8") as f:
                f.write(str(self.client_port_info))

    def hand_free_port(self):
        self.port_temp_info_path = os.path.join(self.project_temp_info_dir, "clients_port_info.log")
        if os.path.exists(self.port_temp_info_path):
            with open(self.port_temp_info_path, "r", encoding="utf-8") as f:
                self.client_port_info = ast.literal_eval(f.read())
            for client_num in range(len(self.client_port_info)):
                if self.client_port_info[client_num]["client_id"] == self.client_num:
                    del self.client_port_info[client_num]
            if len(self.client_port_info) == 0:
                os.remove(self.port_temp_info_path)
            else:
                with open(self.port_temp_info_path, "w", encoding="utf-8") as f:
                    f.write(str(self.client_port_info))

    def palloc(self):
        alloc_port = 0
        while True:
            alloc_port = self.file_palloc()
            time.sleep(0.1)
            if alloc_port is not None:
                return alloc_port
            else:
                alloc_port = self.spy_palloc()
                if alloc_port is not None:
                    return alloc_port
                else:
                    pass

    def pfree(self, port):
        self.file_pfree(port)
        self.spy_pfree(port)

    def file_palloc(self):
        if self.is_hand_alloc_port:
            with self.alloc_add_port_lock:
                if self.add_latest_port + self.port_add_step > self.max_port:
                    for step in range(self.port + 1, self.max_port, self.port_add_step):
                        if step in self.all_allocated_ports_list:
                            pass
                        else:
                            allocated_port = step
                            self.all_allocated_ports_list.append(allocated_port)
                            return allocated_port
                    return None
                self.add_latest_port += self.port_add_step
                self.all_allocated_ports_list.append(self.add_latest_port)
                return self.add_latest_port
        else:
            return 0

    def file_pfree(self, port):
        if self.is_hand_alloc_port:
            with self.alloc_add_port_lock:
                if port in self.all_allocated_ports_list:
                    self.all_allocated_ports_list.remove(port)
                    print("releasing file transfer port, current latest port:", port)
                self.add_latest_port -= self.port_add_step
        else:
            pass

    def spy_palloc(self):
        if self.is_hand_alloc_port:
            with self.alloc_minus_port_lock:
                if self.minus_latest_port - self.port_add_step < self.min_port:
                    for step in range(self.port, self.min_port, -self.port_add_step):
                        if step in self.all_allocated_ports_list:
                            pass
                        else:
                            allocated_port = step
                            self.all_allocated_ports_list.append(allocated_port)
                            return allocated_port
                    return None
                self.minus_latest_port -= self.port_add_step
                self.all_allocated_ports_list.append(self.minus_latest_port)
                return self.minus_latest_port
        else:
            return 0

    def spy_pfree(self, port):
        if self.is_hand_alloc_port:
            with self.alloc_minus_port_lock:
                if port in self.all_allocated_ports_list:
                    self.all_allocated_ports_list.remove(port)
                    print("releasing file transfer port, current latest port:", port)
                self.minus_latest_port += self.port_add_step
        else:
            pass

    def create_temporary_server(self, handler, port=None, max_connections=1):
        if port is None:
            port = self.palloc()
            if port is None:
                raise RuntimeError("No available port for temporary server")
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.client_host, port))
        server_socket.listen(max_connections)
        stop_event = threading.Event()

        def server_loop():
            while not stop_event.is_set():
                try:
                    server_socket.settimeout(1.0)
                    client_sock, addr = server_socket.accept()
                    threading.Thread(target=handler, args=(client_sock, addr), daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if not stop_event.is_set():
                        print(f"Temporary server error: {e}")
                    break
            server_socket.close()
            self.pfree(port)

        server_thread = threading.Thread(target=server_loop, daemon=True)
        server_thread.start()
        return port, server_thread, stop_event

    def create_temporary_client(self, server_host, server_port, bind_port=None, on_data=None):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if bind_port is not None:
            client_sock.bind((self.client_host, bind_port))
        else:
            bind_port = self.palloc()
            client_sock.bind((self.client_host, bind_port))
        client_sock.connect((server_host, server_port))
        stop_event = threading.Event()

        def receiver():
            while not stop_event.is_set():
                try:
                    client_sock.settimeout(1.0)
                    data = self.receive_message(client_sock, 4096)
                    if not data:
                        break
                    if on_data:
                        on_data(data, client_sock)
                except socket.timeout:
                    continue
                except:
                    break
            client_sock.close()

        recv_thread = threading.Thread(target=receiver, daemon=True)
        recv_thread.start()
        return client_sock, recv_thread, stop_event

    def _crypto_negotiate_mode(self):
        """Announce our encryption mode and wait for the server's.

        Returns True only when both sides agree on ``is_enable_encrypto``.
        On a mismatch (or a missing announcement) the connection is
        closed and False is returned.
        """
        self._send_raw(self.client_socket, f"/crypto_mode {1 if self.is_enable_encrypto else 0}")
        if not self._crypto_mode_event.wait(timeout=self._crypto_mode_timeout):
            print("crypto: server did not announce its encryption mode, disconnecting")
            self.close()
            return False
        if not self._crypto_mode_ok:
            self.close()  # mismatch: the receive thread signalled the refusal; close idempotently
            return False
        return True

    def connect(self):  # connect to server
        while True:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if self.timeout != None:
                    self.client_socket.settimeout(self.timeout)  # connect over 5 seconds timeout
                else:
                    self.client_socket.settimeout(5)
                print(f"connecting to {self.host}:{self.port}...")
                if self.client_port == None:
                    pass
                else:
                    self.local_address = (self.client_host, self.client_port)
                    self.client_socket.bind(self.local_address)
                self.client_socket.connect((self.host, self.port))
                self.running = True
                self.receive_thread = threading.Thread(
                    target=self.receive_messages
                )  # set up get msg thread
                with self._crypto_lock:  # reset before the recv thread starts
                    self._crypto_mode_event.clear()
                    self._crypto_mode_ok = False
                self.receive_thread.daemon = True
                self.receive_thread.start()
                if not self._crypto_negotiate_mode():
                    return False
                if self.is_enable_encrypto and self.crypto is not None:
                    with self._crypto_lock:
                        self._crypto_decode_failures = 0  # fresh connection, fresh breaker
                    self._crypto_start_exchange()
                print("connect success! type '/help' to get help.\n")
                return True
            except socket.timeout:
                if self.is_wait_server:
                    print("waiting for server to start...")
                    pass
                else:
                    print("timeout, unable to connect to server")
                    traceback.print_exc()
                    return False
            except ConnectionRefusedError:
                if self.is_wait_server:
                    print("waiting for server to start...")
                    pass
                else:
                    print("connection rejected by server, please ensure the server is running")
                    traceback.print_exc()
                    return False
            except Exception as e:
                print(f"connection error: {e}")
                traceback.print_exc()
                return False

    def receive_messages(self):  # get server msg
        buffer = ""
        while self.running:
            try:
                data = self.receive_message(self.client_socket, 4096)
                if not data:
                    print("\nbreak the connection from server")
                    self.running = False
                    self.free_port()
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:  # deal with multiple messages in buffer
                    line, buffer = buffer.split("\n", 1)
                    message = line.strip()
                    if not message:
                        continue
                    ok, plain = self._crypto_process_line(self.client_socket, message)
                    if ok:
                        message = plain.strip()
                    if message.startswith("/"):
                        self.handle_server_command(message)
                    if message:
                        if not message.startswith("/"):
                            self._notify_message_received(message)
                        print(f"\n[server] {message}")
            except socket.timeout:
                continue
            except ConnectionResetError:
                print("\nReset by server, connection closed")
                traceback.print_exc()
                self.running = False
                self.free_port()
                break
            except Exception as e:
                if not _is_closed_socket_error(e):
                    print(f"\nget msg error: {e}")
                    traceback.print_exc()
                self.running = False
                self.free_port()
                break

    def send_message(self, client_socket, message):  # send msg to server
        if not self.running or not self.client_socket:
            print("disable the connect to server")
            return False
        with self._crypto_send_lock:  # alloc+encrypt+sendall stay ordered (no-GIL safe)
            try:  # add newline character for server to distinguish messages
                with self._crypto_lock:
                    encrypted = client_socket in self._encrypted_sockets
                if encrypted:
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")
                    if not isinstance(message, str):
                        print(f"Unsupported message type: {type(message)}")
                        return False
                    wire = self._crypto_encrypt_message(client_socket, message)
                    data = wire.encode("ascii") + b"\n"
                elif isinstance(message, str):
                    deal_msg = message.strip()
                    if not deal_msg.endswith("\n"):
                        deal_msg += "\n"
                    data = deal_msg.encode("utf-8")
                elif isinstance(message, bytes):
                    data = message
                else:
                    print(f"Unsupported message type: {type(message)}")
                    return False
                client_socket.sendall(data)
                return True
            except Exception as e:
                if not _is_closed_socket_error(e):
                    print(f"send msg error: {e}")
                    traceback.print_exc()
                return False
        
    def send_message_to_server(self, message):
        self.send_message(self.client_socket, shlex.split(message)[1])

    def receive_message(self, client_socket, msg_length):  # receive msg
        data = client_socket.recv(msg_length)
        return data

    def _send_raw(self, client_socket, text):
        """Send a plaintext crypto-protocol message, bypassing encryption."""
        data = text.strip()
        if not data.endswith("\n"):
            data += "\n"
        client_socket.sendall(data.encode("utf-8"))

    def _crypto_mark_encrypted(self, client_socket, peer_role, peer_pem_path):
        with self._crypto_lock:
            self._encrypted_sockets.add(client_socket)
            self._crypto_peer[client_socket] = (peer_role, peer_pem_path)

    def _crypto_encrypt_message(self, client_socket, message):
        """Encrypt ``message`` for the server, prefixed with the session
        nonce and an incrementing sequence number (replay protection)."""
        with self._crypto_lock:
            peer = self._crypto_peer.get(client_socket)
            if peer is None or self.crypto is None:
                raise RuntimeError("socket not bound to a crypto peer")
            nonce = self._crypto_my_nonce or ""
            seq = self._crypto_send_seq
            self._crypto_send_seq = seq + 1
        body = self.crypto.encrypt_for_peer(peer[1], message.strip())
        return f"{nonce}|{seq}|{body}"

    def _crypto_process_line(self, client_socket, line):
        """Decrypt a received line when the socket is in encrypted mode.

        Wire format: ``<sender_nonce>|<seq>|<ciphertext>``. Returns
        ``(True, plaintext)`` on success; a line that fails the nonce or
        sequence check (replay / out-of-order) is silently dropped; a
        line that cannot be decrypted at all drops the socket out of the
        encrypted set and triggers the (circuit-broken) re-exchange.
        """
        with self._crypto_lock:
            encrypted = client_socket in self._encrypted_sockets
        if not encrypted:
            return False, line
        parts = line.split("|", 2)
        if len(parts) != 3:
            if line.startswith("/"):
                return False, line  # plaintext crypto command (handshake/re-exchange), not ciphertext
            return True, ""  # malformed ciphertext frame: not a key problem, drop silently
        nonce, seq_str, cipher = parts
        if len(nonce) != 32 or not all(c in "0123456789abcdef" for c in nonce):  # shape check: a plaintext message with "|" must not look like a ciphertext frame
            return False, line
        try:
            int(seq_str)
        except ValueError:
            return False, line
        try:
            ok, plain = self.crypto.decrypt_with_own(cipher)
            if ok:
                with self._crypto_lock:
                    if nonce != self._crypto_server_nonce:
                        print("crypto: replay dropped (nonce mismatch)")
                        return True, ""
                    try:
                        seq = int(seq_str)
                    except ValueError:
                        return True, ""
                    expected = self._crypto_recv_seq
                    if seq != expected:
                        print(
                            f"crypto: replay/out-of-order dropped (seq {seq}, expected {expected})"
                        )
                        return True, ""
                    self._crypto_recv_seq = expected + 1
                    self._crypto_decode_failures = 0
                return True, plain
        except Exception:
            pass
        if line.startswith("/"):
            return False, line  # plaintext crypto command (handshake/re-exchange), not ciphertext
        self._crypto_on_decode_failure(client_socket)
        return False, line

    def _crypto_on_decode_failure(self, client_socket):
        """Decryption failed (stale/wrong key): drop encryption, reload
        our own key and re-run the public-key exchange (forced).

        A circuit breaker closes the connection after
        ``MAX_DECODE_FAILURES`` consecutive failures, so a garbage or
        persistent-mismatch storm cannot spin the loop forever."""
        with self._crypto_lock:
            self._encrypted_sockets.discard(client_socket)
            self._crypto_peer.pop(client_socket, None)
            self._crypto_decode_failures += 1
            if self._crypto_decode_failures >= MAX_DECODE_FAILURES:
                do_close = True
                do_exchange = False
            else:
                do_close = False
                do_exchange = True
                self._crypto_my_nonce = self._crypto_fresh_nonce()
        if do_close:
            print("crypto: too many decode failures, closing connection")
            try:
                self.close()
            except Exception:
                traceback.print_exc()
            return
        if self.crypto is not None:
            try:
                self.crypto.reload_own_key()
            except Exception:
                traceback.print_exc()
        print("crypto: decode failure, re-exchanging public keys")
        if do_exchange:
            try:
                threading.Thread(
                    target=self._crypto_exchange_thread, args=(True,), daemon=True
                ).start()
            except Exception:
                traceback.print_exc()

    def _crypto_start_exchange(self):
        """Start the client side of the public-key handshake (daemon)."""
        threading.Thread(target=self._crypto_exchange_thread, daemon=True).start()

    def _crypto_still_current(self, gen):
        """True while ``gen`` is still the latest handshake generation."""
        with self._crypto_lock:
            return self._crypto_handshake_gen == gen

    def _crypto_exchange_thread(self, force=False):
        """Drive the client half of the handshake to completion."""
        try:
            self.crypto.ensure_keys()
            with self._crypto_lock:
                ack_event = threading.Event()
                done_event = threading.Event()
                pub_event = threading.Event()
                self._crypto_ack_event = ack_event
                self._crypto_exchange_done = done_event
                self._crypto_server_pub_event = pub_event
                self._crypto_requested_server_pub = False
                self._crypto_server_pub_ok = None
                self._crypto_server_pem_path = None
                self._crypto_my_nonce = self._crypto_fresh_nonce()
                self._crypto_send_seq = 0
                self._crypto_recv_seq = 0
                self._crypto_handshake_started = True
                self._crypto_handshake_gen += 1
                gen = self._crypto_handshake_gen
                hello = f"/crypto_key_exchange {self._crypto_my_nonce}"
                if force:
                    hello += " 1"
            self._send_raw(self.client_socket, hello)
            if not ack_event.wait(timeout=30):
                if not self._crypto_still_current(gen):
                    return  # superseded by a newer handshake; that flow owns the connection
                raise TimeoutError("crypto handshake: no ack from server")
            if not done_event.wait(timeout=90):
                if not self._crypto_still_current(gen):
                    return
                raise TimeoutError("crypto handshake: key exchange did not complete")
            self._crypto_try_flip()  # readiness was announced by _wait_done; wait here until the socket actually flipped
            for _ in range(600):
                with self._crypto_lock:
                    flipped = self.client_socket in self._encrypted_sockets
                if flipped:
                    break
                time.sleep(0.1)
            else:
                if self._crypto_still_current(gen):
                    raise TimeoutError("crypto handshake: server /crypto_ready never arrived")
        except Exception as e:
            print(f"crypto handshake failed: {e}")
            traceback.print_exc()
            try:
                self.close()
            except Exception:
                pass

    def _crypto_on_server_hello(self, server_nonce, force):
        """Server initiated the handshake (re-exchange after a decode
        failure): record its session nonce, ack that we need its key
        (every connection re-exchanges), then run our half."""
        with self._crypto_lock:
            self._crypto_force = force
            self._crypto_server_nonce = server_nonce
            self._crypto_handshake_started = True
            self._crypto_my_nonce = self._crypto_fresh_nonce()
            self._crypto_send_seq = 0
            self._crypto_recv_seq = 0
            self._crypto_handshake_gen += 1
            my_nonce = self._crypto_my_nonce
        ack = f"/crypto_key_exchange_ack {my_nonce} 1 {1 if force else 0}"
        self._send_raw(self.client_socket, ack)
        self._crypto_after_ack("1" if force else "0", force)

    def _crypto_try_flip(self):
        """Flip to encrypted mode only when the server key passed the
        TOFU check and both sides announced readiness. Resets the
        sequence counters so the fresh session starts at seq 0."""
        with self._crypto_send_lock:  # no send may interleave with the seq reset / flip
            with self._crypto_lock:
                if (
                    self._crypto_server_ready
                    and self._crypto_own_ready_sent
                    and self._crypto_server_pub_ok is True
                    and self._crypto_server_pem_path
                    and self.client_socket not in self._encrypted_sockets
                ):
                    self._crypto_send_seq = 0
                    self._crypto_recv_seq = 0
                    self._crypto_mark_encrypted(
                        self.client_socket, "server", self._crypto_server_pem_path
                    )

    @staticmethod
    def _crypto_fresh_nonce():
        return secrets.token_hex(16)

    def _crypto_after_ack(self, server_needs, force):
        """Server replied to our hello: push our key and request the
        server's key (if it needs us to), then signal the exchange
        thread. Runs under ``_crypto_lock`` so a concurrent handshake
        cannot interleave (no-GIL safe)."""
        with self._crypto_lock:
            gen = self._crypto_handshake_gen
            need_push = force or server_needs == "1"
            push_thread = None
            if need_push:
                push_thread = threading.Thread(target=self._crypto_push_pub_key, daemon=True)
                push_thread.start()
            if not self._crypto_requested_server_pub:
                self._crypto_requested_server_pub = True
                request_pub = True
            else:
                request_pub = False
        if request_pub:
            self._send_raw(self.client_socket, "/crypto_pub_key_request")

        def _wait_done():
            with self._crypto_lock:
                pub_event = self._crypto_server_pub_event
            if not pub_event.wait(timeout=90):
                print("crypto: waiting for server public key timed out")
                return
            if push_thread is not None:
                # the push is best-effort (its ack can lag behind a slow file
                # transfer): never gate our readiness on it
                push_thread.join(timeout=5)
            with self._crypto_lock:
                if self._crypto_handshake_gen != gen or self._crypto_server_pub_ok is not True:
                    stale = True
                else:
                    stale = False
                    self._crypto_own_ready_sent = True  # announce readiness: flip only once the server's /crypto_ready arrived too; sent on every handshake path
            if stale:
                return
            try:
                self._send_raw(self.client_socket, "/crypto_ready")
            except OSError:
                pass  # connection already closed (reject/breaker): nothing to announce
            with self._crypto_lock:
                self._crypto_exchange_done.set()

        threading.Thread(target=_wait_done, daemon=True).start()
        with self._crypto_lock:
            self._crypto_ack_event.set()

    def _crypto_push_pub_key(self):
        """Push our public key file to the server via the file-transfer
        mechanism (client is the sender)."""
        while True:  # serialise pushes: wait for a concurrent push (double handshake) instead of skipping it
            with self._crypto_lock:
                if self.client_socket not in self._crypto_push_active:
                    self._crypto_push_active.add(self.client_socket)
                    break
            time.sleep(0.05)
        try:
            with self.file_client_id_lock:
                client_id = copy.copy(self.file_client_id)
                self.file_client_id += 1
            send_msg = f"/crypto_pub_key {client_id}"
            self._send_raw(self.client_socket, send_msg)
            file_transfer_client_port = self.palloc()
            file_server_port = None
            waiting_time = 0
            while True:
                time.sleep(0.05)
                with self.file_transfer_server_port_lock:
                    for port_info in list(self.file_server_port_list):
                        if port_info[1] == client_id:
                            file_server_port = port_info[0]
                            self.file_server_port_list.remove(port_info)
                            break
                if file_server_port is not None:
                    break
                waiting_time += 1
                if waiting_time >= 200:
                    print("crypto: transfer port waiting timeout, public key push failed")
                    return False
            self.file_transfer_mode(
                self.crypto.pub_path, self.host, file_server_port, file_transfer_client_port
            )
            self.pfree(file_transfer_client_port)
        except Exception:
            traceback.print_exc()
            return False
        finally:
            with self._crypto_lock:
                self._crypto_push_active.discard(self.client_socket)
        return True

    def _crypto_store_received_pub(self, full_path, peer_role, server_addr):
        """TOFU-check the server's public key received via the file
        transfer mechanism and cache it under the server endpoint.

        On failure the connection is rejected and closed.
        """
        if self.crypto is None:
            return
        peer_ip, peer_port = server_addr
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                pem = f.read()
        except OSError:
            pem = ""
        ok, reason = self.crypto.verify_peer_pub(peer_ip, peer_port, pem, peer_role)
        if ok:
            self.crypto.store_peer_pem(peer_role, peer_ip, peer_port, full_path)
            with self._crypto_lock:
                self._crypto_server_pem_path = self.crypto.peer_pem_path(
                    peer_role, peer_ip, peer_port
                )
                self._crypto_server_pub_ok = True
            print(f"crypto: accepted server public key ({reason})")
        else:
            with self._crypto_lock:
                self._crypto_server_pub_ok = False
            print(f"crypto: REJECTED server public key: {reason}")
            try:
                os.remove(full_path)  # rejected key: do not leave it in received_files/
            except OSError:
                pass
            try:
                self._send_raw(self.client_socket, f"/crypto_reject {reason}")
            except Exception:
                traceback.print_exc()
            try:
                self.close()
            except Exception:
                traceback.print_exc()
            return
        with self._crypto_lock:
            self._crypto_server_pub_event.set()

    def handle_server_command(self, command):  # deal with special command from server
        client_id = f"{self.client_host}:{self.client_port}"
        if command.lower().split(" ")[0] == "/crypto_mode":
            try:
                peer_mode = int(command.split(" ")[1])
            except (IndexError, ValueError):
                peer_mode = -1
            expected = 1 if self.is_enable_encrypto else 0
            with self._crypto_lock:
                self._crypto_mode_ok = peer_mode == expected
                self._crypto_mode_event.set()
            if not self._crypto_mode_ok:
                print(
                    f"crypto: encryption mode mismatch with server "
                    f"(server={peer_mode}, client={expected}), disconnecting"
                )
                self.close()
            return
        if command.lower().split(" ")[0] == "/client_alloc_port_range":
            if command.lower().split(" ")[1] == "no_limit":
                self.is_hand_alloc_port = False
                print("server has no limit on client port allocation")
            else:
                self.is_hand_alloc_port = True
                self.each_client_port_range = int(command.split(" ")[1])
                self.alloc_port(self.port_add_step, self.each_client_port_range)
                print(f"server allocated port range for each client: {self.each_client_port_range}")
        elif shlex.split(command.lower())[0] == "/server_file_transfer_port":
            with self.file_transfer_server_port_lock:
                self.file_transfer_server_port = int(command.split(" ")[1])
                try:
                    file_client_id = int(command.split(" ")[2])
                    self.file_server_port_list.append(
                        [self.file_transfer_server_port, file_client_id]
                    )
                except:
                    traceback.print_exc()
                    pass
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_key_exchange"
        ):
            parts = command.split(" ")
            server_nonce = parts[1] if len(parts) > 1 else ""
            force = len(parts) > 2 and parts[2] == "1"
            self._crypto_on_server_hello(server_nonce, force)
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_key_exchange_ack"
        ):
            parts = command.split(" ")
            if len(parts) < 3:
                return
            with self._crypto_lock:
                self._crypto_server_nonce = parts[1]
            server_needs = parts[2]
            force = len(parts) > 3 and parts[3] == "1"
            self._crypto_after_ack(server_needs, force)
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_reject"
        ):
            reason = command[len("/crypto_reject") :].strip()
            print(f"crypto: connection rejected by server: {reason}")
            self.close()
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_pub_key"
        ):
            with self._crypto_lock:
                handshake_started = self._crypto_handshake_started
            if not handshake_started:  # only accept key pushes while a handshake is in progress
                print("crypto: ignoring /crypto_pub_key outside a handshake")
                return
            self.file_transfer_client_recv_server_start_thread(  # server pushes its key file; the receive hook TOFU-checks it
                client_id, self.client_socket, command
            )
        elif (
            self.is_enable_encrypto
            and self.crypto is not None
            and command.lower().split(" ")[0] == "/crypto_ready"
        ):
            with self._crypto_lock:
                self._crypto_server_ready = True
            self._crypto_try_flip()
        elif shlex.split(command.lower())[0] == "/file":
            self.file_transfer_client_recv_server_start_thread(
                client_id, self.client_socket, command
            )
        elif shlex.split(command.lower())[0] == "/file_folder":
            self.file_folder_transfer_client_recv_server_start_thread(
                command, client_id, self.client_socket
            )
        elif shlex.split(command.lower())[0] == "/forward_upload":
            try:
                parts = shlex.split(command)
                fid = int(parts[1])
                sport = int(parts[2])
            except (IndexError, ValueError):
                return
            self._forward_upload_queue.put((fid, sport))
        elif shlex.split(command.lower())[0] == "/pause_trans":
            try:
                fid = int(shlex.split(command)[1])
            except (IndexError, ValueError):
                return
            with self._forward_pause_cond:
                self._forward_pause[fid] = True
                self._forward_pause_cond.notify_all()
        elif shlex.split(command.lower())[0] == "/start_trans":
            try:
                fid = int(shlex.split(command)[1])
            except (IndexError, ValueError):
                return
            with self._forward_pause_cond:
                self._forward_pause[fid] = False
                self._forward_pause_cond.notify_all()
        elif shlex.split(command.lower())[0] == "/forward_error":
            self._forward_error = command
        else:
            cmd_parts = shlex.split(command.strip())
            if not cmd_parts:
                return
            cmd_name = cmd_parts[0].lower()
            if cmd_name in self._custom_handlers[0]:
                handler = self._custom_handlers[0][cmd_name]
                run_in_thread = self._custom_handler_threaded[0].get(cmd_name, False)
                if run_in_thread:
                    self.submit_task(
                        self._execute_custom_handler,
                        handler,
                        command,
                        self.client_socket,
                        client_id,
                    )
                else:
                    self._execute_custom_handler(handler, command, self.client_socket, client_id)
            else:
                print(f"Unknown server command: {command}")

    def _execute_custom_handler(self, handler, command, client_socket=None, client_address=None):
        try:
            result = handler(client_socket, client_address, command)
            if result is not None:
                if isinstance(result, str) and not result.endswith("\n"):
                    result += "\n"
                try:
                    self.send_message(client_socket, result)
                except Exception as e:
                    print(f"Error sending message: {e}")
                return result
            return None
        except Exception as e:
            error_msg = f"Error in custom command handler: {e}\n"
            traceback.print_exc()
            try:
                self.send_message(client_socket, error_msg)
            except Exception as e:
                print(f"Error sending error message: {e}")
            return error_msg

    def interactive_mode(self):  # Interactive mode
        client_id = f"{self.client_host}:{self.client_port}"
        try:
            while self.running:
                try:  # get user input
                    message = input()
                    if not self.running:
                        break
                    if message.strip():
                        if message.lower() == "/quit":
                            self.send_message(self.client_socket, "/quit")
                            time.sleep(0.5)
                            break
                        elif shlex.split(message.lower())[0]=="/send_msg":
                            self.send_message_to_server(message)
                        elif shlex.split(message.lower())[0] == "/file":
                            self.file_transfer_client_recv_client_start_thread(message)
                        elif shlex.split(message.lower())[0] == "/multiple_file":
                            self.multiple_file_transfer_client_recv_client_start(message)
                        elif shlex.split(message.lower())[0] == "/file_folder":
                            self.folder_file_transfer_client_recv_client_start(message)
                        elif shlex.split(message.lower())[0] == "/multiple_file_folder":
                            self.multiple_folder_file_transfer_client_recv_client_start(message)
                        elif shlex.split(message.lower())[0] == "/forward_file":
                            self.forward_file_console(message)
                        elif shlex.split(message.lower())[0] == "/forward_folder":
                            self.forward_folder_console(message)
                        else:
                            cmd_name = message[0].lower()
                            if cmd_name in self._custom_handlers[1]:
                                handler = self._custom_handlers[1][cmd_name]
                                run_in_thread = self._custom_handler_threaded[1].get(
                                    cmd_name, False
                                )
                                if run_in_thread:
                                    self.submit_task(
                                        self._execute_custom_handler,
                                        handler,
                                        message,
                                        self.client_socket,
                                        client_id,
                                    )
                                else:
                                    self._execute_custom_handler(
                                        handler, message, self.client_socket, client_id
                                    )
                            else:
                                self.send_message(self.client_socket, message)
                                print(f"Unknown server command: {message}")
                except KeyboardInterrupt:
                    self.close()
                    print("\nshutting down...")
                    traceback.print_exc()
                    self.send_message(self.client_socket, "/quit")
                    time.sleep(0.5)
                    break
                except EOFError:
                    self.close()
                    print("\nshutting down...")
                    traceback.print_exc()
                    self.send_message(self.client_socket, "/quit")
                    time.sleep(0.5)
                    break
                except:
                    traceback.print_exc()
                    pass
        finally:
            self.close()

    def multiple_folder_file_transfer_client_recv_client_start(self, message):
        transfer_folder_file_list = shlex.split(message)[1:]
        destination_path = None
        if len(transfer_folder_file_list) >= 2 and not os.path.isdir(
            transfer_folder_file_list[-1]
        ):
            destination_path = transfer_folder_file_list.pop()
        for transfer_folder_file in transfer_folder_file_list:
            command = "/file_folder {}".format(shlex.quote(transfer_folder_file))
            if destination_path:
                command += " {}".format(shlex.quote(destination_path))
            folder_file_transfer_client_recv_client_start_thread = threading.Thread(
                target=self.folder_file_transfer_client_recv_client_start,
                args=(command,),
                daemon=True,
            )
            folder_file_transfer_client_recv_client_start_thread.start()

    def folder_file_transfer_client_recv_client_start(self, message):
        command_part = shlex.split(message)
        folder_path = command_part[1]
        destination_path = command_part[2] if len(command_part) >= 3 else None
        if os.path.isdir(folder_path) == False:
            print(f"{folder_path} is not a valid folder path")
            return False
        base_path = os.path.dirname(folder_path)

        def get_relative_path(base_path, abs_path):
            base = os.path.normpath(base_path)
            abs_ = os.path.normpath(abs_path)
            common = os.path.commonpath([base, abs_])
            if common != base:
                raise ValueError(f"'{abs_path}' is not a subpath of '{base_path}'")
            rel = os.path.relpath(abs_, base)
            if rel == ".":
                return ""
            rel = rel.replace(os.sep, "/")
            return "/" + rel

        def send_folder_transfer_command(folder_path, file_name=None, abspath=None):
            folder_transfer_command_message = "/file_folder {}".format(shlex.quote(folder_path))
            if file_name:
                each_file_transfer_command_message = "/file_folder {} {}".format(
                    shlex.quote(folder_path), shlex.quote(file_name)
                )
                if destination_path:
                    each_file_transfer_command_message += " {}".format(
                        shlex.quote(destination_path)
                    )
                self.file_transfer_client_recv_client_start_thread(
                    each_file_transfer_command_message, abspath
                )
                print(f"start to send folder command: {each_file_transfer_command_message}")
            else:
                if destination_path:
                    folder_transfer_command_message += " {}".format(
                        shlex.quote(destination_path)
                    )
                self.send_message(self.client_socket, folder_transfer_command_message.strip())
                print(f"start to send folder command: {folder_transfer_command_message}")

        def start_file_transfer_with_limit(rel_dir, file, root):
            cmd = f"/file_folder {shlex.quote(rel_dir)} {shlex.quote(file)}"
            if destination_path:
                cmd += " {}".format(shlex.quote(destination_path))

            def limited_transfer():
                self.file_semaphore.acquire()
                try:
                    self.file_transfer_client_recv_client_start(cmd, root)
                finally:
                    self.file_semaphore.release()

            thread = threading.Thread(target=limited_transfer, daemon=True)
            thread.start()
            print(f"start to send file: {cmd} (limit {self.max_thread_num})")

        def get_all_files_in_folder():
            for root, dirs, files in os.walk(folder_path):
                rel_dir = get_relative_path(base_path, root)
                if root != folder_path:
                    send_folder_transfer_command(rel_dir)
                for file in files:
                    start_file_transfer_with_limit(rel_dir, file, root)
            print(f"finished sending all files in folder {folder_path}")

        transfer_path = get_relative_path(base_path, folder_path)
        send_folder_transfer_command(transfer_path)
        get_all_files_in_folder()

    def multiple_file_transfer_client_recv_client_start(self, message):
        file_list = shlex.split(message)[1:]
        destination_path = None
        if len(file_list) >= 2 and not os.path.isfile(file_list[-1]):
            destination_path = file_list.pop()
        for file in file_list:
            self.file_semaphore.acquire()
            try:
                each_file_transfer_command_message = "/file {}".format(shlex.quote(file))
                if destination_path:
                    each_file_transfer_command_message += " {}".format(
                        shlex.quote(destination_path)
                    )
                self.file_transfer_client_recv_client_start_thread(
                    each_file_transfer_command_message
                )
                print(f"start to send file command: {each_file_transfer_command_message}")
            finally:
                self.file_semaphore.release()

    def file_transfer_client_recv_client_start_thread(self, message, file_folder_abspath=None):
        file_transfer_client_recv_client_start_thread = threading.Thread(
            target=self.file_transfer_client_recv_client_start,
            args=(message, file_folder_abspath),
            daemon=True,
        )
        file_transfer_client_recv_client_start_thread.start()

    def file_transfer_client_recv_client_start(self, message, file_folder_abspath):
        client_id = None
        with self.file_client_id_lock:
            client_id = copy.copy(self.file_client_id)
            send_msg = message.strip() + " " + str(self.file_client_id)
            self.file_client_id += 1
        try:
            waiting_time = 0
            if shlex.split(message.lower())[0] == "/file_folder":
                filename = os.path.join(file_folder_abspath, shlex.split(message)[2])
            else:
                filename = shlex.split(message)[1]
            self.send_message(self.client_socket, send_msg)
            file_transfer_client_port = self.palloc()
            file_server_port = None
            is_find_port = True
            while is_find_port:
                time.sleep(1)
                if len(self.file_server_port_list) > 0:
                    with self.file_transfer_server_port_lock:
                        for port_info in self.file_server_port_list:
                            if port_info[1] == client_id:
                                file_server_port = port_info[0]
                                self.file_server_port_list.remove(port_info)
                                is_find_port = False
                                break
                    pass
                waiting_time += 1
                if waiting_time >= 20:
                    print(
                        "ErrorWhileReceiveFileServerPort: transfer port waiting timeout, file sending failed"
                    )
                    return False
            self.file_transfer_mode(
                filename, self.host, file_server_port, file_transfer_client_port
            )
            self.pfree(file_transfer_client_port)
        except IndexError:
            traceback.print_exc()
            print("invalid command, please use '/file <filename>'")

    def file_transfer_mode(  # noqa: PLR0911 - peer-close and timeout exits are distinct outcomes
        self, filename, server_address, server_port, client_port, pause_fid=None
    ):
        print(f"start to send file: {filename}")
        client_file_socket = None
        reset_time = 0

        def close_socket():
            nonlocal file_running
            nonlocal client_file_socket
            file_running = False
            client_file_socket.close()
            with self.file_client_id_lock:
                self.file_client_id -= 1

        while True:
            try:
                client_file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_file_socket.bind((self.client_host, client_port))
                client_file_socket.connect((server_address, server_port))
                break
            except Exception as e:
                print(f"file transfer connection error: {e}")
                traceback.print_exc()
                if reset_time >= 20:
                    close_socket()
                    print("unable to connect to file transfer server, file sending failed")
                    return False
                reset_time += 1
                time.sleep(1)
        file_running = True
        file_receive_data_from_server = ""

        def receive_file_transfer_messages():
            nonlocal file_running
            nonlocal client_file_socket
            nonlocal file_receive_data_from_server
            while file_running:
                try:
                    data = self.receive_message(client_file_socket, 4096)
                    if not data:
                        print("\nbreak the file transfer connection from server")
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        break
                    file_receive_data_from_server = data.decode("utf-8").strip()
                    if file_receive_data_from_server == self.error_sign:
                        print("\nError sign received from server, file transfer may have failed")
                        close_socket()
                        break
                except Exception as e:
                    print(f"\nget file transfer msg error: {e}")
                    traceback.print_exc()
                    try:
                        self.send_message(client_file_socket, self.error_sign)
                    except:
                        traceback.print_exc()
                        pass
                    close_socket()
                    break

        receive_thread = threading.Thread(target=receive_file_transfer_messages, daemon=True)
        receive_thread.start()
        waiting_time = 0
        try:
            while True:
                if file_receive_data_from_server == self.server_start_file_transfer_sign:
                    break
                if file_receive_data_from_server == self.error_sign:
                    close_socket()
                    break
                if not file_running:
                    # peer closed the file connection: the receive thread
                    # already cleaned up; nothing more to wait for
                    return False
                time.sleep(1)
                waiting_time += 1
                if waiting_time >= 10:
                    try:
                        self.send_message(client_file_socket, self.error_sign)
                    except:
                        traceback.print_exc()
                        pass
                    print(
                        f"ErrorWhileSendFile: \
                          Wait file transfer function start sign timeout, \
                          file {filename} sending failed"
                    )
                    close_socket()
                    return False
            waiting_time = 0
            file_size = os.path.getsize(filename)
            file_name_encoded = os.path.basename(filename).encode("utf-8")
            name_len = len(file_name_encoded)
            self.send_message(client_file_socket, name_len.to_bytes(4, "big"))
            self.send_message(client_file_socket, file_name_encoded)
            self.send_message(client_file_socket, file_size.to_bytes(8, "big"))
            with open(filename, "rb") as f:
                while True:
                    if pause_fid is not None:
                        with self._forward_pause_cond:
                            while self._forward_pause.get(pause_fid, False):
                                self._forward_pause_cond.wait()
                    file_data = f.read(65536)
                    if not file_data:
                        break
                    self.send_message(client_file_socket, file_data)
            extra_time = (file_size // (100 * 1024 * 1024)) * 10
            timeout = int(30 + extra_time)
            while True:
                if file_receive_data_from_server == self.server_received_file_data_sign:
                    break
                if file_receive_data_from_server == self.error_sign:
                    close_socket()
                    break
                if not file_running:
                    # peer closed the file connection: the receive thread
                    # already cleaned up; nothing more to wait for
                    return False
                time.sleep(1)
                waiting_time += 1
                if waiting_time >= timeout:
                    try:
                        self.send_message(client_file_socket, self.error_sign)
                    except:
                        traceback.print_exc()
                        pass
                    close_socket()
                    print(
                        f"ErrorWhileSendFileData: \
                          wait file transfer confirmation sign timeout, \
                          file {filename} sending may have failed"
                    )
                    return False
            print(f"Success: file {filename} sent successfully")
            close_socket()
            return True
        except FileNotFoundError:
            traceback.print_exc()
            try:
                self.send_message(client_file_socket, self.error_sign)
            except:
                traceback.print_exc()
                pass
            close_socket()
            print(f"file {filename} not exist")
            return False
        except Exception as e:
            traceback.print_exc()
            try:
                self.send_message(client_file_socket, self.error_sign)
            except:
                traceback.print_exc()
                pass
            close_socket()
            print(f"send error: {e}")
            return False

    # ---- native in-memory forward (client side) ----------------------------

    def _forward_parse(self, tokens):
        items = []
        addrs = []
        for token in tokens:
            if token.startswith("(") and token.endswith(")"):
                try:
                    addr = ast.literal_eval(token)
                except (ValueError, SyntaxError):
                    items.append(token)
                    continue
                if isinstance(addr, tuple) and len(addr) == 2 and isinstance(addr[0], str):
                    addrs.append(addr)
                else:
                    items.append(token)
            else:
                items.append(token)
        return items, addrs

    def _forward_parse_command(self, tokens):
        """Split forward console tokens into (items, addrs, destination_path).

        Files/folders come first and address tuples last; an optional
        destination directory, when present, is the very last argument.
        """
        destination_path = None
        if tokens and not (tokens[-1].startswith("(") and tokens[-1].endswith(")")):
            destination_path = tokens[-1]
            tokens = tokens[:-1]
        items, addrs = self._forward_parse(tokens)
        return items, addrs, destination_path

    def forward_file_console(self, message):
        """/forward_file <file1> <file2> ... <addr1> <addr2> ... [dest] (client only)."""
        parts = shlex.split(message)
        items, addrs, destination_path = self._forward_parse_command(parts[1:])
        files = [p for p in items if os.path.isfile(p)]
        for p in items:
            if not os.path.isfile(p):
                print(f"forward: {p} is not a valid file, skipped")
        if not files:
            return
        if not addrs:
            print("forward: no target clients given")
            return
        threading.Thread(
            target=self._forward_driver,
            args=(files, addrs, False, destination_path),
            daemon=True,
        ).start()

    def forward_folder_console(self, message):
        """/forward_folder <folder1> <folder2> ... <addr1> <addr2> ... [dest] (client only)."""
        parts = shlex.split(message)
        items, addrs, destination_path = self._forward_parse_command(parts[1:])
        folders = [p for p in items if os.path.isdir(p)]
        for p in items:
            if not os.path.isdir(p):
                print(f"forward: {p} is not a valid folder, skipped")
        if not folders:
            return
        if not addrs:
            print("forward: no target clients given")
            return
        threading.Thread(
            target=self._forward_driver,
            args=(folders, addrs, True, destination_path),
            daemon=True,
        ).start()

    def _forward_driver(self, items, addrs, folder_mode, destination_path=None):
        try:
            if not folder_mode:
                for path in items:
                    self._forward_one(
                        "file", "", os.path.basename(path), path, addrs, destination_path
                    )
            else:
                for folder in items:
                    base_path = os.path.dirname(folder)
                    for root, _dirs, files in os.walk(folder):
                        rel_dir = self._forward_rel_path(base_path, root)
                        for file in files:
                            self._forward_one(
                                "folder",
                                rel_dir,
                                file,
                                os.path.join(root, file),
                                addrs,
                                destination_path,
                            )
        except Exception:
            traceback.print_exc()

    def _forward_rel_path(self, base_path, abs_path):
        base = os.path.normpath(base_path)
        abs_ = os.path.normpath(abs_path)
        common = os.path.commonpath([base, abs_])
        if common != base:
            return ""
        rel = os.path.relpath(abs_, base)
        if rel == ".":
            return ""
        return "/" + rel.replace(os.sep, "/")

    def _forward_one(self, kind, rel_dir, fname, abspath, addrs, destination_path=None):
        if not os.path.isfile(abspath):
            print(f"forward: {abspath} is not a valid file, skipped")
            return
        msg = f"/forward_item {kind}"
        if kind == "folder":
            msg += f" {shlex.quote(rel_dir)}"
        msg += f" {shlex.quote(fname)} " + " ".join(shlex.quote(str(a)) for a in addrs)
        if destination_path:
            msg += f" {shlex.quote(destination_path)}"
        self._forward_error = None
        self.send_message(self.client_socket, msg)
        upload = None
        deadline = time.time() + 30
        while time.time() < deadline:
            if self._forward_error is not None:
                print(f"forward: server error: {self._forward_error}")
                self._forward_error = None
                return
            try:
                upload = self._forward_upload_queue.get(timeout=0.2)
                break
            except queue.Empty:
                continue
        if upload is None:
            print(f"forward: timed out waiting for an upload slot for {fname}")
            return
        fid, sport = upload
        client_port = self.palloc()
        try:
            self.file_transfer_mode(abspath, self.host, sport, client_port, pause_fid=fid)
        finally:
            self.pfree(client_port)
            with self._forward_pause_cond:
                self._forward_pause.pop(fid, None)

    def file_folder_transfer_client_recv_server_start_thread(
        self, command, client_id, client_socket
    ):
        command_part = shlex.split(command)
        relative_folder_path = command_part[1]
        # >= 4 tokens means a file transfer (/file_folder <rel> <file> [<dest>] <id>);
        # 2-3 tokens is folder creation (optional destination, no client id)
        file_name = command_part[2] if len(command_part) >= 4 else None
        folder_transfer_client_recv_server_start_thread = threading.Thread(
            target=self.file_transfer_client_recv_server_start,
            args=(client_id, client_socket, command, relative_folder_path, file_name),
            daemon=True,
        )
        folder_transfer_client_recv_server_start_thread.start()

    def file_transfer_client_recv_server_start_thread(self, client_id, client_socket, command):
        file_transfer_client_recv_server_start_thread = threading.Thread(
            target=self.file_transfer_client_recv_server_start,
            args=(client_id, client_socket, command),
            daemon=True,
        )
        file_transfer_client_recv_server_start_thread.start()

    def file_transfer_client_recv_server_start(
        self, client_id, client_socket, command, new_save_path=None, file_name=None
    ):
        file_transfer_server_port = self.palloc()
        self.file_transfer_mode_recv(
            self.host,
            file_transfer_server_port,
            client_socket,
            client_id,
            new_save_path,
            file_name,
            command,
        )
        self.pfree(file_transfer_server_port)

    def file_transfer_mode_recv(
        self,
        server_file_address,
        server_file_port,
        client_socket,
        client_id,
        new_save_path,
        file_name,
        command,
    ):
        file_running = True
        client_file_socket = None
        server_file_socket = None
        save_path = None

        def close_socket():
            nonlocal file_running
            nonlocal client_file_socket
            nonlocal server_file_socket
            file_running = False
            client_file_socket.close()
            server_file_socket.close()

        def setting_file_save_path():
            nonlocal save_path
            save_path = self.file_transfer_dir
            if new_save_path or destination_path:
                if destination_path:
                    if os.path.isabs(destination_path):
                        save_path = destination_path
                    else:
                        save_path = os.path.join(save_path, destination_path)
                if new_save_path:
                    path_list = new_save_path.split("/")
                    if path_list and path_list[0] == "":
                        del path_list[0]
                    for node in path_list:
                        save_path = os.path.join(save_path, node)
                os.makedirs(save_path, exist_ok=True)
            if file_name or new_save_path == None:
                return save_path
            close_socket()
            return None

        def file_transfer_client_recv(client_id):
            nonlocal file_running
            nonlocal client_file_socket
            nonlocal server_file_socket
            nonlocal save_path
            filename = None
            full_path = None
            self.send_message(client_file_socket, self.server_start_file_transfer_sign)
            try:
                name_len_bytes = b""
                while len(name_len_bytes) < 4:
                    chunk = self.receive_message(client_file_socket, 4 - len(name_len_bytes))
                    print(chunk)
                    if not chunk:
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        raise ConnectionError(
                            "ErrorWhileReceivingFileNameLength: client disconnected"
                        )
                    name_len_bytes += chunk
                    if name_len_bytes.strip() == self.error_sign.encode("utf-8"):
                        close_socket()
                        raise ConnectionError(
                            "ErrorSignReceivedWhileReceivingFileNameLength: client reported error and disconnected"
                        )
                name_len = int.from_bytes(name_len_bytes, "big")
                file_name_encoded = b""
                while len(file_name_encoded) < name_len:
                    chunk = self.receive_message(
                        client_file_socket, name_len - len(file_name_encoded)
                    )
                    if not chunk:
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        raise ConnectionError("ErrorWhileReceivingFileName: client disconnected")
                    file_name_encoded += chunk
                    if file_name_encoded.strip() == self.error_sign.encode("utf-8"):
                        close_socket()
                        raise ConnectionError(
                            "ErrorSignReceivedWhileReceivingFileName: client reported error and disconnected"
                        )
                filename = file_name_encoded.decode("utf-8")
                filename = filename.strip()
                filename = os.path.basename(filename)
                size_bytes = b""
                while len(size_bytes) < 8:
                    chunk = self.receive_message(client_file_socket, 8 - len(size_bytes))
                    if not chunk:
                        try:
                            self.send_message(client_file_socket, self.error_sign)
                        except:
                            traceback.print_exc()
                            pass
                        close_socket()
                        raise ConnectionError("ErrorWhileReceivingFileSize: client disconnected")
                    size_bytes += chunk
                    if size_bytes.strip() == self.error_sign.encode("utf-8"):
                        close_socket()
                        raise ConnectionError(
                            "ErrorSignReceivedWhileReceivingFileSize: client reported error and disconnected"
                        )
                file_size = int.from_bytes(size_bytes, "big")
                self.send_message(client_file_socket, self.server_received_file_header_sign)
                original_filename = filename
                if file_name:
                    final_filename = file_name.strip()
                elif command_part[0] == "/crypto_pub_key":
                    # concurrent key pushes share received_files/: a fixed name would be
                    # overwritten/moved by another connection before this one consumes it
                    final_filename = "{}.pem".format(uuid.uuid4().hex)
                else:
                    final_filename = os.path.basename(original_filename)
                full_path = os.path.join(save_path, final_filename).strip()
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                with open(full_path.strip(), "wb") as f:
                    remaining = file_size
                    while remaining > 0:
                        chunk = self.receive_message(client_file_socket, min(65536, remaining))
                        if not chunk:
                            try:
                                self.send_message(client_file_socket, self.error_sign)
                            except:
                                traceback.print_exc()
                                pass
                            close_socket()
                            raise ConnectionError(
                                "ErrorWhileReceivingFileData: client disconnected"
                            )
                        f.write(chunk)
                        remaining -= len(chunk)
                # TOFU first: the ack is only a notification, and a failed
                # ack send must not skip the key registration (readiness
                # would never be announced and the handshake hangs)
                self._notify_file_received(full_path, final_filename, file_size, command)
                print(f"file {filename} received from {client_id}, size {file_size} bytes")
                if command_part[0] == "/crypto_pub_key":
                    self._crypto_store_received_pub(full_path, "server", (self.host, self.port))
                try:
                    self.send_message(client_file_socket, self.server_received_file_data_sign)
                except Exception:
                    traceback.print_exc()
                close_socket()
            except Exception as e:
                traceback.print_exc()
                if full_path is not None and os.path.exists(full_path):
                    try:
                        os.remove(full_path)  # partial transfer: no half-written leftovers
                    except OSError:
                        pass
                try:
                    self.send_message(client_file_socket, self.error_sign)
                except:
                    traceback.print_exc()
                    pass
                close_socket()
                print(f"ErrorWhileReceiveFile: {e}")
                return False
            else:
                close_socket()
                return None

        server_file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_file_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_file_socket.bind((server_file_address, server_file_port))
        file_transfer_server_port = server_file_socket.getsockname()[1]
        command_part = shlex.split(command)
        file_client_id = command_part[len(command_part) - 1]
        destination_path = _parse_destination_path(command_part)
        transfer_server_port_msg = "/server_file_transfer_port {} {}\n".format(
            file_transfer_server_port, file_client_id
        )
        self._send_raw(client_socket, transfer_server_port_msg)  # protocol control message: always plaintext (like the handshake)
        server_file_socket.listen(1)
        try:
            client_file_socket, client_file_address = server_file_socket.accept()
            is_open_file_transfer = setting_file_save_path()
            if is_open_file_transfer == None:
                pass
            else:
                threading.Thread(
                    target=file_transfer_client_recv, args=(client_id,), daemon=True
                ).start()
        except Exception as e:
            print(f"\nget file transfer msg error: {e}")
            traceback.print_exc()
            close_socket()
        finally:
            server_file_socket.close()

    def close(self):  # close connection
        self.running = False
        self.free_port()
        if self.client_socket:
            self.client_socket.close()
        print("connection closed")

    def start_TCP_client(self):  # start client
        if not self.connect():
            sys.exit(1)
        try:
            if self.is_input_command_in_console:
                self.interactive_mode()
            else:
                while self.running:
                    time.sleep(1)
        except KeyboardInterrupt:
            print("\nclient shutting down...")
            traceback.print_exc()
        finally:
            self.close()
