"""Flask backend wrapping the PyFlow TCP client for the web tool.

The launcher (``setup_client.py``) starts this backend and opens the
connect UI in the browser.  The user enters the server address (an
``http``/``https`` domain or a bare IP); the backend queries the
server's web backend ``/api/server_info`` for the TCP server address
and port, then starts the ``TCP_Client_Base`` instance.  The backend
stays up to relay the user's frontend actions:

- messages/files/folders to the server use the native transfer methods;
- messages to other clients are forwarded through the built-in
  ``forward_extension_tcp`` extension (string forwarding lives there);
- files/folders to other clients use the native forward methods.

The sidebar instance list is kept fresh by the server's
``/web_clients_update`` broadcasts; a reload button re-requests the
list via ``/web_sync_clients``.
"""

import json
import os
import shlex
import socket
import sys
import threading
import time
import traceback
import urllib.request
from urllib.parse import urlparse

from flask import Flask, jsonify, render_template, request

from PyFlow import add_extension
from PyFlow import forward_extension_tcp
from PyFlow.network_api.connect_tcp import TCP_Client_Base

WEB_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLOW_WEB_DIR = os.path.join(WEB_ROOT, ".Flow_Web")
CLIENT_EXTENSIONS_UI_FILE = os.path.join(FLOW_WEB_DIR, "client_extensions_ui.json")
CLIENT_LAST_SERVER_FILE = os.path.join(FLOW_WEB_DIR, "client_last_server.json")
UPLOAD_DIR = os.path.join(FLOW_WEB_DIR, "uploads")
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
STATIC_DIR = os.path.join(WEB_ROOT, "static")

DEFAULT_CLIENT_WEB_PORT = 5001
DEFAULT_SERVER_WEB_PORT = 5000


def _find_free_port(base):
    port = base
    while port < base + 100:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                port += 1
    return base


def _normalize_address(address):
    """Turn user input into ``scheme://host:port`` for the server web backend."""
    address = address.strip()
    if not address:
        raise ValueError("empty server address")
    if "://" not in address:
        address = "http://" + address
    parts = urlparse(address)
    if not parts.hostname:
        raise ValueError(f"invalid server address: {address}")
    port = parts.port or (443 if parts.scheme == "https" else DEFAULT_SERVER_WEB_PORT)
    return f"{parts.scheme}://{parts.hostname}:{port}"


def _load_json_list(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


class ClientWebApp:
    """Flask app + TCP_Client_Base wrapper for the web tool."""

    def __init__(self, web_port=None):
        self.web_port = web_port or DEFAULT_CLIENT_WEB_PORT
        self.client = None
        self.server_info = None
        self.connected = False
        self._last_address = ""
        self._clients = []
        self._clients_lock = threading.Lock()
        self.app = Flask(
            __name__,
            template_folder=TEMPLATE_DIR,
            static_folder=STATIC_DIR,
            static_url_path="/static",
        )
        self._register_routes()

    # ---------------------------------------------------------------- helpers

    def _own_address(self):
        if self.client is None or self.client.client_socket is None:
            return None
        try:
            ip, port = self.client.client_socket.getsockname()[:2]
            return {"ip": ip, "port": port, "id": f"{ip}:{port}"}
        except Exception:
            return None

    def _client_id(self):
        own = self._own_address()
        if own:
            return own["id"]
        return f"{self.client.client_host}:{self.client.client_port}"

    def _run_client_command(self, handler, command):
        try:
            self.client._execute_custom_handler(
                handler, command, self.client.client_socket, self._client_id()
            )
        except Exception:
            traceback.print_exc()

    def _send_file_to_server(self, path):
        try:
            self.client.file_transfer_client_recv_client_start(f"/file {shlex.quote(path)}", None)
        except Exception:
            traceback.print_exc()

    def _send_folder_to_server(self, path):
        try:
            self.client.folder_file_transfer_client_recv_client_start(
                f"/file_folder {shlex.quote(path)}"
            )
        except Exception:
            traceback.print_exc()

    def _forward_file(self, path, addr):
        try:
            self.client.forward_file_console(
                f"/forward_file {shlex.quote(path)} {shlex.quote(str(addr))}"
            )
        except Exception:
            traceback.print_exc()

    def _forward_folder(self, path, addr):
        try:
            self.client.forward_folder_console(
                f"/forward_folder {shlex.quote(path)} {shlex.quote(str(addr))}"
            )
        except Exception:
            traceback.print_exc()

    def _restart(self):
        time.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def _on_clients_update(self, sock, addr, cmd):
        """Server broadcast: refresh the sidebar instance list."""
        payload = cmd[len("/web_clients_update") :].strip()
        try:
            clients = json.loads(payload)
        except Exception:
            return None
        if isinstance(clients, list):
            with self._clients_lock:
                self._clients = clients
        return None

    # ---------------------------------------------------------------- connect

    def _start_client(self, host, port, is_enable_encrypto):
        self.client = TCP_Client_Base(
            host=host,
            port=port,
            client_host="127.0.0.1",
            client_port=None,
            timeout=None,
            port_add_step=1,
            max_thread_num=10,
            is_input_command_in_console=False,
            is_wait_server=True,
            max_custom_workers=10,
            is_extend_command=True,
            is_enable_encrypto=is_enable_encrypto,
            is_custom_keys=None,
            max_mem_buff=2048,
        )
        forward_extension_tcp.setup_client_commands(self.client)
        self.client.register_command(
            "/web_clients_update", self._on_clients_update, where_to_run="server", run_in_thread=True
        )
        try:
            add_extension.load_registered_extensions(self.client, "client")
        except ImportError as e:
            print(f"Failed to load registered extensions: {e}")
        threading.Thread(target=self.client.start_TCP_client, daemon=True).start()
        self.connected = True
        self.server_info = {
            "host": host,
            "port": port,
            "is_enable_encrypto": is_enable_encrypto,
        }
        with self._clients_lock:
            self._clients = []
        os.makedirs(FLOW_WEB_DIR, exist_ok=True)
        with open(CLIENT_LAST_SERVER_FILE, "w", encoding="utf-8") as f:
            json.dump({"address": self._last_address}, f, indent=4, ensure_ascii=False)

    # ------------------------------------------------------------------ routes

    def _register_routes(self):
        app = self.app

        @app.get("/")
        def index():
            if self.connected:
                return render_template("client_main.html", mode="client")
            last = ""
            if os.path.exists(CLIENT_LAST_SERVER_FILE):
                try:
                    with open(CLIENT_LAST_SERVER_FILE, "r", encoding="utf-8") as f:
                        last = json.load(f).get("address", "")
                except Exception:
                    last = ""
            return render_template("client_connect.html", last_address=last)

        @app.post("/api/connect")
        def api_connect():
            data = request.get_json(force=True)
            address = data.get("address", "").strip()
            try:
                base = _normalize_address(address)
            except ValueError as e:
                return jsonify({"ok": False, "error": str(e)}), 400
            try:
                with urllib.request.urlopen(f"{base}/api/server_info", timeout=10) as resp:
                    info = json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": f"cannot reach the server web backend at {base}: {e}",
                        }
                    ),
                    502,
                )
            if not info.get("ok", True):
                return jsonify({"ok": False, "error": info.get("error", "server not ready")}), 503
            host = info.get("host")
            port = int(info.get("port"))
            is_enable_encrypto = bool(info.get("is_enable_encrypto", True))
            self._last_address = address
            try:
                self._start_client(host, port, is_enable_encrypto)
            except Exception as e:
                traceback.print_exc()
                return jsonify({"ok": False, "error": f"failed to start TCP client: {e}"}), 500
            return jsonify({"ok": True, "server_info": self.server_info})

        @app.get("/api/status")
        def api_status():
            return jsonify(
                {
                    "connected": self.connected
                    and self.client is not None
                    and self.client.running,
                    "server_info": self.server_info,
                    "clients": self._clients_snapshot(),
                    "own_address": self._own_address(),
                }
            )

        @app.post("/api/send_msg")
        def api_send_msg():
            if not self.connected or self.client is None:
                return jsonify({"ok": False, "error": "not connected"}), 400
            data = request.get_json(force=True)
            target = data.get("target")
            message = data.get("message", "")
            if target == "server":
                ok = self.client.send_message(self.client.client_socket, message)
                if not ok:
                    return jsonify({"ok": False, "error": "send failed"}), 500
                return jsonify({"ok": True})
            addr = (target[0], int(target[1]))
            handler = self.client._custom_handlers[1].get("/send_msg_forward")
            if handler is None:
                return jsonify({"ok": False, "error": "forward extension is not loaded"}), 500
            command = f"/send_msg_forward {shlex.quote(message)} {shlex.quote(str(addr))}"
            threading.Thread(
                target=self._run_client_command, args=(handler, command), daemon=True
            ).start()
            return jsonify({"ok": True})

        @app.post("/api/send_file")
        def api_send_file():
            if not self.connected or self.client is None:
                return jsonify({"ok": False, "error": "not connected"}), 400
            target = request.form.get("target")
            files = request.files.getlist("files")
            if not files:
                return jsonify({"ok": False, "error": "no files uploaded"}), 400
            os.makedirs(UPLOAD_DIR, exist_ok=True)
            saved = []
            for f in files:
                path = os.path.join(UPLOAD_DIR, os.path.basename(f.filename))
                f.save(path)
                saved.append(path)
            if target == "server":
                for path in saved:
                    threading.Thread(
                        target=self._send_file_to_server, args=(path,), daemon=True
                    ).start()
            else:
                addr = tuple(json.loads(target))
                for path in saved:
                    threading.Thread(target=self._forward_file, args=(path, addr), daemon=True).start()
            return jsonify({"ok": True})

        @app.post("/api/send_folder")
        def api_send_folder():
            if not self.connected or self.client is None:
                return jsonify({"ok": False, "error": "not connected"}), 400
            target = request.form.get("target")
            files = request.files.getlist("files")
            if not files:
                return jsonify({"ok": False, "error": "no files uploaded"}), 400
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
            if target == "server":
                threading.Thread(
                    target=self._send_folder_to_server, args=(root,), daemon=True
                ).start()
            else:
                addr = tuple(json.loads(target))
                threading.Thread(target=self._forward_folder, args=(root, addr), daemon=True).start()
            return jsonify({"ok": True})

        @app.post("/api/run_extension")
        def api_run_extension():
            if not self.connected or self.client is None:
                return jsonify({"ok": False, "error": "not connected"}), 400
            data = request.get_json(force=True)
            command = data.get("command", "")
            parts = shlex.split(command)
            if not parts:
                return jsonify({"ok": False, "error": "empty command"}), 400
            handler = self.client._custom_handlers[1].get(parts[0].lower())
            if handler is None:
                return jsonify({"ok": False, "error": f"command {parts[0]} is not registered"}), 404
            threading.Thread(
                target=self._run_client_command, args=(handler, command), daemon=True
            ).start()
            return jsonify({"ok": True})

        @app.get("/api/available_commands")
        def api_available_commands():
            if not self.connected or self.client is None:
                return jsonify({"ok": False, "error": "not connected"}), 400
            return jsonify({"commands": sorted(self.client._custom_handlers[1].keys())})

        @app.post("/api/sync_clients")
        def api_sync_clients():
            if not self.connected or self.client is None:
                return jsonify({"ok": False, "error": "not connected"}), 400
            self.client.send_message(self.client.client_socket, "/web_sync_clients")
            return jsonify({"ok": True})

        @app.get("/api/extensions_ui")
        def api_get_extensions_ui():
            return jsonify({"extensions": _load_json_list(CLIENT_EXTENSIONS_UI_FILE)})

        @app.get("/api/registered_extensions")
        def api_registered_extensions():
            return jsonify({"extensions": _load_json_list(add_extension.added_extensions_log_file)})

        @app.post("/api/extensions_ui")
        def api_save_extensions_ui():
            data = request.get_json(force=True)
            entries = data.get("extensions", [])
            os.makedirs(FLOW_WEB_DIR, exist_ok=True)
            with open(CLIENT_EXTENSIONS_UI_FILE, "w", encoding="utf-8") as f:
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

    def _clients_snapshot(self):
        with self._clients_lock:
            return list(self._clients)

    def run(self):
        port = _find_free_port(self.web_port)
        if port != self.web_port:
            print(f"Web port {self.web_port} busy, using {port}")
        threading.Thread(target=self._open_browser, args=(port,), daemon=True).start()
        self.app.run(host="127.0.0.1", port=port, threaded=True, use_reloader=False)

    def _open_browser(self, port):
        time.sleep(1.5)
        try:
            import webbrowser

            webbrowser.open(f"http://127.0.0.1:{port}/")
        except Exception:
            pass
