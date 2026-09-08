"""Flask backend wrapping the PyFlow TCP server for the web tool.

Two modes, one process:

- ``config`` mode: serves the server startup-configuration UI.  The UI
  shows every ``TCP_Server_Base`` parameter with its default value; on
  submit the config is written to ``.Flow_Web/setup_server.json`` (same
  shape as ``flow_setup``'s ``setup.json``) and the TCP server class is
  started.
- ``status`` mode: serves the minimal status page plus the same
  sidebar/input UI as the client frontend (forwarding disabled; native
  sends to connected clients allowed).  Also exposes the HTTP API that
  clients use to discover the TCP server address/port.

The backend monitors ``server.clients``: whenever a client connects or
disconnects it broadcasts the current instance list to every connected
client (``/web_clients_update``), and it re-checks the list every
minute.

Inbound events (plain-text messages and file uploads arriving from
clients) are captured on the TCP server's receive threads through
``TCP_Server_Base``'s ``add_message_listener``/``add_file_listener``
APIs, queued here, and polled by the frontend via ``/api/events``.
"""

import json
import os
import shlex
import socket
import sys
import threading
import time
import traceback

from flask import Flask, jsonify, render_template, request

from PyFlow import add_extension
from PyFlow import forward_extension_tcp
from PyFlow.network_api.connect_tcp import TCP_Server_Base

WEB_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLOW_WEB_DIR = os.path.join(WEB_ROOT, ".Flow_Web")
SERVER_CONFIG_FILE = os.path.join(FLOW_WEB_DIR, "setup_server.json")
SERVER_EXTENSIONS_UI_FILE = os.path.join(FLOW_WEB_DIR, "server_extensions_ui.json")
UPLOAD_DIR = os.path.join(FLOW_WEB_DIR, "uploads")
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
STATIC_DIR = os.path.join(WEB_ROOT, "static")

DEFAULT_WEB_PORT = 5000

# Ordered (key, label, type, default, help) for every TCP_Server_Base
# parameter shown in the startup-configuration UI.
SERVER_PARAM_FIELDS = [
    ("host", "Host", "text", "127.0.0.1", "IP address the TCP server binds to."),
    ("port", "Port", "number", 65432, "TCP port the server listens on."),
    ("max_clients", "Max clients", "number", 10, "Maximum number of concurrent clients."),
    ("port_add_step", "Port add step", "number", 1, "Step size for port allocation."),
    ("port_range_num", "Port range num", "number", 100, "Number of ports in the allocation range."),
    (
        "max_file_transfer_thread_num",
        "Max file transfer threads",
        "number",
        10,
        "Maximum concurrent file-transfer threads.",
    ),
    ("is_hand_alloc_port", "Hand-allocated ports", "bool", False, "Manually allocate transfer ports."),
    (
        "is_input_command_in_console",
        "Console input",
        "bool",
        False,
        "Forced False by the web architecture (the web UI is the input).",
    ),
    ("max_custom_workers", "Max custom workers", "number", 10, "Maximum custom-command worker threads."),
    (
        "is_extend_command",
        "Extend command",
        "bool",
        True,
        "Forced True by the web architecture (extensions are registered before start).",
    ),
    ("is_enable_encrypto", "Enable encryption", "bool", True, "RSA-encrypt the TCP channel."),
    ("is_custom_keys", "Custom keys", "text", "", "Optional [pub_key_path, pvt_key_path] pair."),
    ("max_mem_buff", "Max memory buffer (MB)", "number", 2048, "In-memory transfer buffer in MB."),
]

# Web-only settings (not TCP_Server_Base parameters).
WEB_FIELDS = [
    ("web_port", "Web port", "number", DEFAULT_WEB_PORT, "Port of this web backend (clients query it)."),
]


def _public_host(host):
    """Resolve a wildcard bind address to an address clients can reach."""
    if host not in ("0.0.0.0", "::", ""):
        return host
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("8.8.8.8", 80))
            return probe.getsockname()[0]
        finally:
            probe.close()
    except Exception:
        return "127.0.0.1"


def _find_free_port(base):
    """Return ``base`` if free, otherwise the next free port."""
    port = base
    while port < base + 100:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                port += 1
    return base


def _load_json_list(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


class ServerWebApp:
    """Flask app + TCP_Server_Base wrapper for the web tool."""

    def __init__(self, web_port=None):
        self.web_port = web_port or DEFAULT_WEB_PORT
        self.server = None
        self.mode = "config"  # "config" | "status"
        self._bound_port = None
        self._last_clients = set()
        self._monitor_stop = threading.Event()
        self._monitor_thread = None
        self._events = []  # inbound events surfaced to the frontend (/api/events)
        self._events_lock = threading.Lock()
        self._event_seq = 0
        self.app = Flask(
            __name__,
            template_folder=TEMPLATE_DIR,
            static_folder=STATIC_DIR,
            static_url_path="/static",
        )
        self._register_routes()

    # ------------------------------------------------------------------ setup

    def start_from_config(self):
        """Read ``.Flow_Web/setup_server.json`` and start the TCP server."""
        if not os.path.exists(SERVER_CONFIG_FILE):
            self.mode = "config"
            return
        with open(SERVER_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        servers = data.get("servers", [])
        if not servers:
            self.mode = "config"
            return
        web = data.get("web", {}) or {}
        self.web_port = int(web.get("port", DEFAULT_WEB_PORT))
        self._start_server(servers[0])

    def _start_server(self, config):
        """Create, register and start the TCP_Server_Base instance."""
        params = dict(config)
        # Web architecture constraints: extensions must be registered
        # before start, and the web UI replaces the console input.
        params["is_extend_command"] = True
        params["is_input_command_in_console"] = False
        if params.get("is_custom_keys") in (None, ""):
            params["is_custom_keys"] = None
        elif isinstance(params["is_custom_keys"], str):
            try:
                parsed = json.loads(params["is_custom_keys"])
                params["is_custom_keys"] = parsed if isinstance(parsed, list) else None
            except Exception:
                params["is_custom_keys"] = None
        self.server = TCP_Server_Base(**params)
        forward_extension_tcp.setup_server_commands(self.server)
        self.server.register_command(
            "/web_sync_clients", self._on_sync_clients, where_to_run="server", run_in_thread=True
        )
        self.server.add_message_listener(self._on_incoming_message)
        self.server.add_file_listener(self._on_incoming_file)
        try:
            add_extension.load_registered_extensions(self.server, "server")
        except ImportError as e:
            print(f"Failed to load registered extensions: {e}")
        threading.Thread(target=self.server.start_TCP_Server, daemon=True).start()
        self.mode = "status"
        self._last_clients = set()
        self._monitor_stop.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        print(f"TCP server started: {self.server.host}:{self.server.port}")

    # ------------------------------------------------------------- monitoring

    def _monitor_loop(self):
        """Broadcast the client list on connect/disconnect; re-check every minute."""
        last_check = time.time()
        while not self._monitor_stop.is_set():
            time.sleep(1)
            if self.server is None or not self.server.running:
                continue
            with self.server.client_lock:
                current = set(self.server.clients.keys())
            if current != self._last_clients:
                self._last_clients = current
                self._broadcast_clients()
            if time.time() - last_check >= 60:
                last_check = time.time()
                self._broadcast_clients()

    def _client_list(self):
        if self.server is None:
            return []
        with self.server.client_lock:
            return [
                {"ip": addr[0], "port": addr[1], "id": info["id"]}
                for addr, info in self.server.clients.items()
            ]

    def _server_info_payload(self):
        return {
            "host": _public_host(self.server.host),
            "port": self.server.port,
            "is_enable_encrypto": self.server.is_enable_encrypto,
        }

    def _broadcast_clients(self):
        """Push the current instance list to every connected client."""
        if self.server is None or not self.server.running:
            return
        payload = json.dumps(self._client_list(), separators=(",", ":"))
        message = f"/web_clients_update {payload}"
        with self.server.client_lock:
            for info in list(self.server.clients.values()):
                try:
                    self.server.send_message(info["socket"], message)
                except Exception:
                    pass

    def _on_sync_clients(self, sock, addr, cmd):
        """A client asked for a fresh instance list: broadcast it."""
        self._broadcast_clients()
        return None

    # ------------------------------------------------ inbound event handling

    def _push_event(self, event):
        """Record an inbound event with a monotonically increasing id."""
        with self._events_lock:
            self._event_seq += 1
            event["id"] = self._event_seq
            self._events.append(event)
            if len(self._events) > 1000:
                del self._events[: len(self._events) - 1000]
        return self._event_seq

    def _on_incoming_message(self, client_id, text):
        """Server receive thread: a plain-text message arrived from a client."""
        text = (text or "").strip()
        if not text:
            return
        self._push_event(
            {"type": "msg", "from": client_id, "text": text, "at": time.strftime("%H:%M:%S")}
        )

    def _on_incoming_file(self, client_id, full_path, name, size, command):
        """Server receive thread: a file uploaded by a client was saved."""
        try:
            cmd_name = (command or "").strip().split(" ", 1)[0].lower()
        except Exception:
            cmd_name = ""
        if cmd_name == "/crypto_pub_key":  # handshake keys are not user data
            return
        rel = full_path
        if self.server is not None:
            try:
                candidate = os.path.relpath(full_path, self.server.file_transfer_dir)
                if not candidate.startswith(".."):
                    rel = candidate
            except Exception:
                pass
        self._push_event(
            {
                "type": "file",
                "name": name,
                "path": rel,
                "size": size,
                "from": client_id,
                "at": time.strftime("%H:%M:%S"),
            }
        )

    # ---------------------------------------------------------------- helpers

    def _require_server(self):
        if self.server is None or not self.server.running:
            return jsonify({"ok": False, "error": "TCP server is not running"}), 503
        return None

    def _target_info(self, target):
        addr = (target[0], int(target[1]))
        with self.server.client_lock:
            return self.server.clients.get(addr)

    def _run_extension(self, handler, command):
        try:
            self.server._execute_custom_handler(handler, command)
        except Exception:
            traceback.print_exc()

    def _send_file_to_client(self, target, path):
        try:
            self.server.file_transfer_server_recv_client_start(
                f"/file {shlex.quote(path)} {shlex.quote(str(target))}", None
            )
        except Exception:
            traceback.print_exc()

    def _send_folder_to_client(self, target, path):
        try:
            self.server.folder_file_transfer_server_recv_client_start(
                f"/file_folder {shlex.quote(path)} {shlex.quote(str(target))}"
            )
        except Exception:
            traceback.print_exc()

    def _restart(self):
        time.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    # ------------------------------------------------------------------ routes

    def _register_routes(self):
        app = self.app

        @app.get("/")
        def index():
            if self.mode == "status":
                return render_template("server_status.html", mode="server")
            return render_template(
                "server_config.html", fields=SERVER_PARAM_FIELDS, web_fields=WEB_FIELDS
            )

        @app.get("/api/status")
        def api_status():
            return jsonify(
                {
                    "mode": self.mode,
                    "running": self.server is not None and self.server.running,
                    "server_info": self._server_info_payload() if self.server is not None else None,
                    "clients": self._client_list(),
                }
            )

        @app.post("/api/save_config")
        def api_save_config():
            data = request.get_json(force=True)
            params = data.get("params", {})
            web_port = int(data.get("web_port", DEFAULT_WEB_PORT))
            os.makedirs(FLOW_WEB_DIR, exist_ok=True)
            config = {"servers": [params], "clients": [], "web": {"port": web_port}}
            with open(SERVER_CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            self.web_port = web_port
            try:
                self._start_server(params)
            except Exception as e:
                traceback.print_exc()
                return jsonify({"ok": False, "error": f"failed to start TCP server: {e}"}), 500
            if self._bound_port is not None and web_port != self._bound_port:
                # the web port only takes effect on the next start
                threading.Thread(target=self._restart, daemon=True).start()
                return jsonify({"ok": True, "restarting": True})
            return jsonify({"ok": True, "server_info": self._server_info_payload()})

        @app.get("/api/server_info")
        def api_server_info():
            """TCP server address/port discovery for web clients."""
            if self.server is None or not self.server.running:
                return jsonify({"ok": False, "error": "TCP server is not running"}), 503
            return jsonify(self._server_info_payload())

        @app.get("/api/clients")
        def api_clients():
            return jsonify({"clients": self._client_list()})

        @app.get("/api/events")
        def api_events():
            since = request.args.get("since", 0, type=int)
            with self._events_lock:
                events = [e for e in self._events if e["id"] > since]
                latest = events[-1]["id"] if events else since
            return jsonify({"events": events, "latest": latest})

        @app.post("/api/send_msg")
        def api_send_msg():
            err = self._require_server()
            if err:
                return err
            data = request.get_json(force=True)
            target = data.get("target")
            message = data.get("message", "")
            if target == "server":
                return jsonify({"ok": False, "error": "the server cannot send to itself"}), 400
            info = self._target_info(target)
            if info is None:
                return jsonify({"ok": False, "error": "target client is not connected"}), 404
            try:
                self.server.send_message(info["socket"], message)
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500
            return jsonify({"ok": True})

        @app.post("/api/send_file")
        def api_send_file():
            err = self._require_server()
            if err:
                return err
            target = json.loads(request.form.get("target", "null"))
            files = request.files.getlist("files")
            if not files:
                return jsonify({"ok": False, "error": "no files uploaded"}), 400
            if target == "server":
                return jsonify({"ok": False, "error": "the server cannot send to itself"}), 400
            info = self._target_info(target)
            if info is None:
                return jsonify({"ok": False, "error": "target client is not connected"}), 404
            os.makedirs(UPLOAD_DIR, exist_ok=True)
            saved = []
            for f in files:
                path = os.path.join(UPLOAD_DIR, os.path.basename(f.filename))
                f.save(path)
                saved.append(path)
            for path in saved:
                threading.Thread(
                    target=self._send_file_to_client, args=(tuple(target), path), daemon=True
                ).start()
            return jsonify({"ok": True, "paths": saved})

        @app.post("/api/send_folder")
        def api_send_folder():
            err = self._require_server()
            if err:
                return err
            target = json.loads(request.form.get("target", "null"))
            files = request.files.getlist("files")
            if not files:
                return jsonify({"ok": False, "error": "no files uploaded"}), 400
            if target == "server":
                return jsonify({"ok": False, "error": "the server cannot send to itself"}), 400
            info = self._target_info(target)
            if info is None:
                return jsonify({"ok": False, "error": "target client is not connected"}), 404
            os.makedirs(UPLOAD_DIR, exist_ok=True)
            root = None
            for f in files:
                rel = f.filename  # webkitRelativePath, e.g. "folder/sub/file.txt"
                path = os.path.join(UPLOAD_DIR, rel)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                f.save(path)
                if root is None:
                    root = os.path.join(UPLOAD_DIR, rel.split("/")[0])
            if root is None or not os.path.isdir(root):
                return jsonify({"ok": False, "error": "folder upload failed"}), 500
            threading.Thread(
                target=self._send_folder_to_client, args=(tuple(target), root), daemon=True
            ).start()
            return jsonify({"ok": True, "path": root})

        @app.post("/api/run_extension")
        def api_run_extension():
            err = self._require_server()
            if err:
                return err
            data = request.get_json(force=True)
            command = data.get("command", "")
            parts = shlex.split(command)
            if not parts:
                return jsonify({"ok": False, "error": "empty command"}), 400
            handler = self.server._custom_handlers[1].get(parts[0].lower())
            if handler is None:
                return jsonify({"ok": False, "error": f"command {parts[0]} is not registered"}), 404
            threading.Thread(target=self._run_extension, args=(handler, command), daemon=True).start()
            return jsonify({"ok": True})

        @app.get("/api/available_commands")
        def api_available_commands():
            err = self._require_server()
            if err:
                return err
            return jsonify({"commands": sorted(self.server._custom_handlers[1].keys())})

        @app.post("/api/sync_clients")
        def api_sync_clients():
            self._broadcast_clients()
            return jsonify({"ok": True})

        @app.get("/api/extensions_ui")
        def api_get_extensions_ui():
            return jsonify({"extensions": _load_json_list(SERVER_EXTENSIONS_UI_FILE)})

        @app.get("/api/registered_extensions")
        def api_registered_extensions():
            return jsonify({"extensions": _load_json_list(add_extension.added_extensions_log_file)})

        @app.post("/api/extensions_ui")
        def api_save_extensions_ui():
            data = request.get_json(force=True)
            entries = data.get("extensions", [])
            os.makedirs(FLOW_WEB_DIR, exist_ok=True)
            with open(SERVER_EXTENSIONS_UI_FILE, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=4, ensure_ascii=False)
            return jsonify({"ok": True})

        @app.post("/api/add_extension")
        def api_add_extension():
            data = request.get_json(force=True)
            paths = data.get("paths", [])
            try:
                add_extension.add_extension(paths)
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 400
            threading.Thread(target=self._restart, daemon=True).start()
            return jsonify({"ok": True, "restarting": True})

        @app.post("/api/remove_extension")
        def api_remove_extension():
            data = request.get_json(force=True)
            paths = data.get("paths", [])
            try:
                add_extension.remove_extension(paths)
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 400
            threading.Thread(target=self._restart, daemon=True).start()
            return jsonify({"ok": True, "restarting": True})

    # ------------------------------------------------------------------- run

    def run(self):
        host = "127.0.0.1" if self.mode == "config" else self.server.host
        port = _find_free_port(self.web_port)
        if port != self.web_port:
            print(f"Web port {self.web_port} busy, using {port}")
        self._bound_port = port
        threading.Thread(target=self._open_browser, args=(port,), daemon=True).start()
        self.app.run(host=host, port=port, threaded=True, use_reloader=False)

    def _open_browser(self, port):
        time.sleep(1.5)
        try:
            import webbrowser

            webbrowser.open(f"http://127.0.0.1:{port}/")
        except Exception:
            pass
