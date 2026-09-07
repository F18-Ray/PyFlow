import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import traceback

try:
    import msvcrt as _msvcrt
except ImportError:  # Windows only
    _msvcrt = None
try:
    import select as _select
except ImportError:  # POSIX only
    _select = None  # type: ignore[invalid-assignment]

try:
    import termios as _termios
except ImportError:  # POSIX only
    _termios = None  # type: ignore[invalid-assignment]

try:
    import tty as _tty
except ImportError:  # POSIX only
    _tty = None
from . import add_extension
from . import command_control_extension_tcp, forward_extension_tcp
from .network_api.connect_tcp import TCP_Client_Base, TCP_Server_Base

config_file_name="setup.json"
flow_setup_root=os.path.dirname(os.path.abspath(__file__))
config_file_dir=os.path.join(flow_setup_root, config_file_name)
project_root = os.path.dirname(flow_setup_root)

SERVER_DEFAULTS = {
    "host": "127.0.0.1",
    "port": 65432,
    "max_clients": 10,
    "port_add_step": 1,
    "port_range_num": 100,
    "max_file_transfer_thread_num": 10,
    "is_hand_alloc_port": False,
    "is_input_command_in_console": True,
    "max_custom_workers": 10,
    "is_extend_command": False,
    "is_enable_encrypto": True,
    "is_custom_keys": None,
    "max_mem_buff": 2048,
}

CLIENT_DEFAULTS = {
    "host": None,
    "client_host": "127.0.0.1",
    "port": 65432,
    "client_port": None,
    "timeout": None,
    "port_add_step": 1,
    "max_thread_num": 10,
    "is_input_command_in_console": True,
    "is_wait_server": True,
    "max_custom_workers": 10,
    "is_extend_command": False,
    "is_enable_encrypto": True,
    "is_custom_keys": None,
    "max_mem_buff": 2048,
}


def parse_addr_port(addr_port):
    host, port_str = addr_port.strip().split(":")
    return host, int(port_str)


def load_existing_config():
    if os.path.exists(config_file_dir):
        with open(config_file_dir, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"servers": [], "clients": []}


def complete_server_config(cfg):
    full = SERVER_DEFAULTS.copy()
    full.update(cfg)
    return full


def complete_client_config(cfg):
    full = CLIENT_DEFAULTS.copy()
    full.update(cfg)
    return full


def save_config(servers, clients):
    server_config=[]
    client_config=[]
    if servers:
        pass
    else:
        servers = []
    if clients:
        pass
    else:
        clients = []
    for server in servers:
        server_config.append(complete_server_config(server))
    for client in clients:
        client_config.append(complete_client_config(client))
    data = {
        "servers": server_config,
        "clients": client_config,
    }
    with open(config_file_dir, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def launch_instance(config, instance_type):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(config, f)
        config_file_path = f.name
    module = "PyFlow.flow_setup"
    python = sys.executable
    launch_arg = f"--launch_{instance_type}"
    system = platform.system()
    try:
        if system == "Windows":
            cmd = (
                f'start cmd /k {python} -m {module} {launch_arg} '
                f'--config-file "{config_file_path}"'
            )
            subprocess.Popen(cmd, shell=True, cwd=project_root)
        elif system == "Linux":
            terminals = ["gnome-terminal", "xterm", "x-terminal-emulator"]
            launched = False
            for term in terminals:
                if shutil.which(term):
                    cmd = (
                        f'{term} -- {python} -m {module} {launch_arg} '
                        f'--config-file "{config_file_path}"'
                    )
                    subprocess.Popen(cmd, shell=True, cwd=project_root)
                    launched = True
                    break
            if not launched:
                subprocess.Popen(
                    [python, "-m", module, launch_arg, "--config-file", config_file_path],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                    cwd=project_root,
                )
        elif system == "Darwin":
            cmd = (
                f'open -a Terminal.app {python} -m {module} {launch_arg} '
                f'--config-file "{config_file_path}"'
            )
            subprocess.Popen(cmd, shell=True, cwd=project_root)
        else:
            subprocess.Popen(
                [python, "-m", module, launch_arg, "--config-file", config_file_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                cwd=project_root,
            )
    except Exception as e:
        print(f"Failed to launch instance: {e}")
        traceback.print_exc()
        try:
            os.unlink(config_file_path)
        except:
            pass


# ---- web tool launchers (transfer_web) --------------------------------------

def launch_web_tool(kind):
    """Launch the transfer_web launcher (``kind`` = "server" or "client").

    The web tool is a Flask app that opens a browser UI, so it runs in
    its own process (a terminal window when one is available, otherwise
    detached) and the launcher returns immediately.
    """
    module = (
        "PyFlow.transfer_web.setup_server" if kind == "server" else "PyFlow.transfer_web.setup_client"
    )
    python = sys.executable
    system = platform.system()
    try:
        if system == "Windows":
            cmd = f"start cmd /k {python} -m {module}"
            subprocess.Popen(cmd, shell=True, cwd=project_root)
        elif system == "Linux":
            terminals = ["gnome-terminal", "xterm", "x-terminal-emulator"]
            launched = False
            for term in terminals:
                if shutil.which(term):
                    cmd = f"{term} -- {python} -m {module}"
                    subprocess.Popen(cmd, shell=True, cwd=project_root)
                    launched = True
                    break
            if not launched:
                subprocess.Popen(
                    [python, "-m", module],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                    cwd=project_root,
                )
        elif system == "Darwin":
            cmd = f"open -a Terminal.app {python} -m {module}"
            subprocess.Popen(cmd, shell=True, cwd=project_root)
        else:
            subprocess.Popen(
                [python, "-m", module],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                cwd=project_root,
            )
    except Exception as e:
        print(f"Failed to launch web {kind}: {e}")
        traceback.print_exc()


# ---- instance editor: reduce/change existing instances ----------------------

_MOUSE_WHEEL_UP = 64
_MOUSE_WHEEL_DOWN = 65
_DOUBLE_CLICK_SECONDS = 0.3
_ESC_SEQUENCE_WAIT = 0.08

EDITOR_USAGE = """\
Instance editor usage (instance menu):
  up/down arrows or j/k ......... move the selection (first instance selected
                                  by default, works without a UI)
  mouse click ................... select the clicked instance
  double click / Enter .......... open the selected instance's config editor
  Delete / Backspace, dd ........ delete the selected instance
  :w ............................ write setup.json (save without exiting)
  :wq ........................... write setup.json and exit the editor
  :q ............................ exit (refused while there are unsaved changes)
  :q! ........................... exit and discard all changes
  Fix_Config ................... write setup.json and exit (same as :wq);
                              at field-value prompts it returns to the list
  Setup ......................... launch every instance from setup.json and
                              exit (works at every prompt)
  Quit .......................... exit the setup program immediately
  Help .......................... show this help
Config editor (opened with Enter): type a field index or press Enter to edit
the selected field, j/k move the selection, Esc/back returns to the menu.
Config fields (empty input keeps the current value):
  server: host, port, max_clients, port_add_step, port_range_num,
          max_file_transfer_thread_num, is_hand_alloc_port,
          is_input_command_in_console, max_custom_workers, is_extend_command,
          is_enable_encrypto, is_custom_keys, max_mem_buff
  client: host, client_host, port, client_port, timeout, port_add_step,
          max_thread_num, is_input_command_in_console, is_wait_server,
          max_custom_workers, is_extend_command, is_enable_encrypto,
          is_custom_keys, max_mem_buff
Booleans accept true/false/1/0/y/n; integers are parsed with int(). Type
"none" to reset a nullable field (host/client_port/timeout/is_custom_keys).
Help, Fix_Config, Setup and Quit work at every prompt, including field
values.
"""

COLLECT_USAGE = """\
Setup flow usage:
  0/1 ......... instance type (0 = server, 1 = client)
  host:port ... bind address, e.g. 127.0.0.1:65432; clients are also asked
                for the server host:port they connect to
  Y/N ......... confirmations: y = yes, n = no (case-insensitive)
  Help ........ show this help
  Fix_Config .. jump to the instance editor, where startup parameters can
                be changed (works at every prompt)
  Setup ....... launch every instance from setup.json and exit (works at
                every prompt)
  Add_Extension ... add extension protocol files to the project (works at
                every prompt)
  Delete_Extension ... remove registered extension files from the project
                (works at every prompt)
  Quit ........ exit the setup program immediately
Help, Fix_Config, Add_Extension, Delete_Extension, Setup and Quit work at
every prompt. In the instance editor (reached with Y on the reduce question)
'Help' prints all config fields and the vim-style commands.
"""


class _EnterEditor(Exception):
    """Entering Fix_Config at any prompt jumps to the instance editor."""


class _LaunchFromSetup(Exception):
    """Entering Setup at any prompt launches every instance from setup.json."""


class _QuitFlow(Exception):
    """Entering Quit at any prompt exits the setup program immediately."""


def _launch_all_from_setup():
    """Start every instance configured in setup.json; True when launched.

    Used by the global ``Setup`` command: the current interactive flow is
    abandoned and the configured instances are started as-is. Returns False
    (with a message) when setup.json is missing, unreadable or empty, so the
    caller can keep asking instead of silently doing nothing.
    """
    if not os.path.exists(config_file_dir):
        print("setup.json not found - nothing to launch")
        return False
    try:
        with open(config_file_dir, "r", encoding="utf-8") as f:
            config_data = json.load(f)
    except Exception as e:  # noqa: BLE001
        print(f"setup.json could not be read: {e}")
        return False
    servers = config_data.get("servers", [])
    clients = config_data.get("clients", [])
    if not servers and not clients:
        print("setup.json contains no instances - nothing to launch")
        return False
    for cfg in servers:
        launch_instance(cfg, "server")
    for cfg in clients:
        launch_instance(cfg, "client")
    return True


def _input(prompt, help_text=COLLECT_USAGE):
    """Ask one question; 'Help' reprints usage, 'Fix_Config' enters the
    instance editor, 'Setup' launches every instance from setup.json,
    'Add_Extension' adds extension files, 'Delete_Extension' removes
    registered extension files, 'Quit' exits the program.

    The returned line has surrounding whitespace stripped; the caller
    decides case handling for its own answers.
    """
    while True:
        answer = input(prompt).strip()
        low = answer.lower()
        if low == "help":
            print(help_text, flush=True)
            continue
        if low == "fix_config":
            raise _EnterEditor()
        if low == "setup":
            if _launch_all_from_setup():
                raise _LaunchFromSetup()
            continue  # nothing usable in setup.json: ask again
        if low == "quit":
            raise _QuitFlow()
        if low == "add_extension":
            paths_input = input(
                "Enter extension path(s) to add (separated by space): "
            ).strip()
            if paths_input:
                try:
                    add_extension.add_extension(paths_input.split())
                    print("Extension(s) added successfully.")
                except Exception as e:
                    print(f"Failed to add extension(s): {e}")
            continue
        if low == "delete_extension":
            if not os.path.exists(add_extension.added_extensions_log_file):
                print("No extension registration file found - nothing to delete.")
                continue
            paths_input = input(
                "Enter extension path(s) to delete (separated by space): "
            ).strip()
            if paths_input:
                try:
                    add_extension.remove_extension(paths_input.split())
                    print("Extension(s) deleted successfully.")
                except Exception as e:
                    print(f"Failed to delete extension(s): {e}")
            continue
        return answer


def _stdin_is_tty():
    try:
        return sys.stdin.isatty()
    except Exception:
        return False


def _raw_mode_active():
    # Interactive terminals get real keys; pipes, CI and tests (which set
    # PYTEST_CURRENT_TEST) fall back to line commands (plain input()).
    if not _stdin_is_tty():
        return False
    if "PYTEST_CURRENT_TEST" in os.environ:
        return False
    return _termios is not None or _msvcrt is not None


def _enter_raw():
    """Switch stdin to raw byte mode with mouse tracking; return saved attrs."""
    if _termios is None or _tty is None or not _stdin_is_tty():
        return None
    fd = sys.stdin.fileno()
    saved_attr = _termios.tcgetattr(fd)
    _tty.setraw(fd)
    sys.stdout.write("\x1b[?1000h\x1b[?1006h\x1b[?25l")
    sys.stdout.flush()
    return saved_attr


_pending_bytes = bytearray()


def _leave_raw(saved_attr):
    if _termios is None or saved_attr is None:
        return
    sys.stdout.write("\x1b[?1000l\x1b[?1006l\x1b[?25h")
    sys.stdout.flush()
    _termios.tcsetattr(sys.stdin.fileno(), _termios.TCSADRAIN, saved_attr)


def _read_posix_key():
    fd = sys.stdin.fileno()
    first = os.read(fd, 1)
    if first != b"\x1b":
        return first
    seq = first
    while True:
        assert _select is not None
        ready, _, _ = _select.select([fd], [], [], _ESC_SEQUENCE_WAIT)
        if not ready:
            break
        chunk = os.read(fd, 4096)
        if not chunk:
            break
        seq += chunk
    return seq


def _decode_posix_seq(seq):  # noqa: PLR0911
    if not seq:
        return ("eof",)
    if seq == b"\x1b[A":
        return ("up",)
    if seq == b"\x1b[B":
        return ("down",)
    if seq in (b"\x1b[3~", b"\x1b[3;2~", b"\x1b[3;5~"):
        return ("delete",)
    if seq in (b"\r", b"\n"):
        return ("enter",)
    if seq in (b"\x7f", b"\x08"):
        return ("backspace",)
    if seq == b"\x1b":
        return ("esc",)
    if seq == b"\x03":
        raise KeyboardInterrupt
    if seq.startswith(b"\x1b[<") and seq[-1:] in (b"M", b"m"):
        try:
            button, x, y = (int(p) for p in seq[3:-1].decode("ascii", "replace").split(";"))
            return ("mouse", button, x, y)
        except ValueError:
            pass
    if len(seq) == 1:
        return ("char", seq.decode("utf-8", "replace"))
    if seq.startswith(b"\x1b"):
        # Unknown escape sequence: keep the trailing bytes for the next reads
        # and surface the leading ESC, so pasted text after ESC is not lost.
        _pending_bytes[0:0] = seq[1:]
        return ("esc",)
    return None


def _read_win_key():
    if _msvcrt is None:
        return None
    ch = _msvcrt.getwch()  # ty: ignore[unresolved-attribute]
    if ch in ("\x00", "\xe0"):
        second = _msvcrt.getwch()  # ty: ignore[unresolved-attribute]
        return ({"H": "up", "P": "down", "S": "delete"}.get(second, "unknown"),)
    if ch == "\r":
        return ("enter",)
    if ch == "\x08":
        return ("backspace",)
    if ch == "\x1b":
        return ("esc",)
    if ch == "\x03":
        raise KeyboardInterrupt
    return ("char", ch)


def _read_key():
    if _pending_bytes:
        return ("char", bytes([_pending_bytes.pop(0)]).decode("utf-8", "replace"))
    if _termios is not None and _stdin_is_tty():
        return _decode_posix_seq(_read_posix_key())
    if _msvcrt is not None and _stdin_is_tty():
        return _read_win_key()
    return None


def _menu_action_from_line(command):  # noqa: PLR0911, PLR0912
    low = command.lower()
    if command == "":
        return ("edit", None)
    if low in ("j", "down", "+"):
        return ("select", 1)
    if low in ("k", "up", "-"):
        return ("select", -1)
    if low in ("dd", "del", "delete", "backspace"):
        return ("delete", None)
    if low == ":wq":
        return ("write_quit", None)
    if low == ":w":
        return ("write", None)
    if low == ":q!":
        return ("abort", None)
    if low == ":q":
        return ("quit", None)
    if low in (":help", ":h", "help", "h"):
        return ("help", None)
    if low == "fix_config":
        return ("write_quit", None)
    if low == "setup":
        return ("setup", None)
    if low == "quit":
        return ("quit_flow", None)
    if command.isdigit():
        return ("select_abs", int(command))
    return ("noop", command)


def _detail_command(command, num_fields):  # noqa: PLR0911
    low = command.lower()
    if command == "":
        return ("edit", None)
    if command.isdigit():
        index = int(command)
        if 0 <= index < num_fields:
            return ("edit", index)
        return ("noop", command)
    if low in ("j", "down"):
        return ("select", 1)
    if low in ("k", "up"):
        return ("select", -1)
    if low in ("b", "back", "esc"):
        return ("back", None)
    if low == "fix_config":
        return ("back", None)
    if low == "setup":
        return ("setup", None)
    if low == "quit":
        return ("quit_flow", None)
    if low in ("help", "h"):
        return ("help", None)
    return ("noop", command)


def _instance_label(kind, cfg):
    if kind == "server":
        return f"{cfg.get('host', '?')}:{cfg.get('port', '?')}"
    return (
        f"{cfg.get('client_host', '?')}:{cfg.get('client_port', '?')}"
        f" -> {cfg.get('host', '?')}:{cfg.get('port', '?')}"
    )


def _instance_entries(servers, clients):
    entries = [("server", s) for s in servers]
    entries.extend(("client", c) for c in clients)
    return entries


def _terminal_width():
    """Current terminal width in columns (80 when it cannot be determined)."""
    try:
        return shutil.get_terminal_size().columns
    except Exception:
        return 80


def _row_to_entry(y, entry_rows):
    """Map a 1-based screen row back to the entry index it belongs to."""
    for index, start in enumerate(entry_rows):
        end = entry_rows[index + 1] if index + 1 < len(entry_rows) else start + 1
        if start <= y < end:
            return index
    return None


def _print_menu(servers, clients, selection, raw, typed, status_msg):  # noqa: PLR0913, PLR0917
    """Render the instance menu with dynamically aligned columns.

    Column widths (index, kind, label) are derived from the current
    content; rows wider than the terminal wrap with the continuation
    indented to the label column. Returns the 1-based screen row where
    every entry starts, used to map mouse clicks back to entries.
    """
    entries = _instance_entries(servers, clients)
    width = _terminal_width()
    index_w = len(str(max(len(entries) - 1, 0))) if entries else 1
    kind_w = max((len(k) for k, _ in entries), default=6)
    lines = []
    if raw:
        lines.append("\x1b[2J\x1b[H")
    lines.append("=== Instance Manager === (:wq save&exit | :q! / Fix_Config | Help)")
    entry_rows = []
    screen_row = 2  # row 1 is the title
    for index, (kind, cfg) in enumerate(entries):
        marker = "> " if index == selection else "  "
        label = _instance_label(kind, cfg)
        prefix = f"{marker}[{index:>{index_w}}] [{kind:<{kind_w}}] "
        entry_rows.append(screen_row)
        pieces = textwrap.wrap(label, max(width - len(prefix), 10)) or [""]
        lines.append(prefix + pieces[0])
        for extra in pieces[1:]:
            lines.append(" " * len(prefix) + extra)
        screen_row += len(pieces)
    if not entries:
        lines.append("  (no instances)")
    if typed:
        lines.append(f"command: {typed}")
    if status_msg:
        lines.append(status_msg)
    for line in lines:
        print(line, flush=True)
    return entry_rows


def _delete_at(servers, clients, selection):
    entries = _instance_entries(servers, clients)
    if not entries or not (0 <= selection < len(entries)):
        return None
    kind, cfg = entries[selection]
    if kind == "server":
        del servers[selection]
    else:
        del clients[selection - len(servers)]
    return kind, cfg


def _parse_field_value(default, raw_value):  # noqa: PLR0911
    if isinstance(default, bool):
        low = raw_value.lower()
        if low in ("true", "1", "y", "yes"):
            return True, None
        if low in ("false", "0", "n", "no"):
            return False, None
        return None, f"invalid boolean {raw_value!r} (use true/false/1/0/y/n)"
    if isinstance(default, int):
        try:
            return int(raw_value), None
        except ValueError:
            return None, f"invalid integer {raw_value!r}"
    if default is None and raw_value.lower() in ("none", "null"):
        return None, None
    return raw_value, None


def _edit_field_value(cfg, field):
    key, default = field
    current = cfg.get(key, default)
    while True:
        raw_value = _input(
            f"{key} (current={current!r}, empty keeps): ", help_text=EDITOR_USAGE
        )
        if raw_value == "":
            return False
        value, error = _parse_field_value(default, raw_value)
        if error:
            print(error, flush=True)
            continue
        cfg[key] = value
        print(f"{key} = {value!r}", flush=True)
        return True


def _edit_instance_detail(kind, cfg, raw, saved_attr):  # noqa: PLR0912, PLR0915
    """Edit every startup field of one instance; returns True when changed."""
    defaults = SERVER_DEFAULTS if kind == "server" else CLIENT_DEFAULTS
    fields = list(defaults.items())
    selection = 0
    changed = False
    typed = ""
    status_msg = ""
    while True:
        if selection >= len(fields):
            selection = len(fields) - 1
        width = _terminal_width()
        idx_w = len(str(len(fields) - 1))
        key_w = max(len(key) for key, _ in fields)
        lines = []
        if raw:
            lines.append("\x1b[2J\x1b[H")
        lines.append(f"=== Editing {kind}: {_instance_label(kind, cfg)} ===")
        lines.append("(field index/Enter = edit, j/k = move, Esc/back = return, Fix_Config)")
        for index, (key, default) in enumerate(fields):
            marker = "> " if index == selection else "  "
            value = str(cfg.get(key, default))
            prefix = f"{marker}[{index:>{idx_w}}] {key:<{key_w}} = "
            pieces = textwrap.wrap(value, max(width - len(prefix), 10)) or [""]
            lines.append(prefix + pieces[0])
            for extra in pieces[1:]:
                lines.append(" " * len(prefix) + extra)
        if typed:
            lines.append(f"command: {typed}")
        if status_msg:
            lines.append(status_msg)
        for line in lines:
            print(line, flush=True)
        status_msg = ""
        if raw:
            key = None
            while key is None:
                key = _read_key()
            kind_key = key[0]
            if kind_key == "eof":
                return changed  # stdin closed: leave the field editor
            if kind_key == "char":
                typed += key[1]
                continue
            if kind_key == "backspace":
                typed = typed[:-1]
                continue
            if kind_key == "esc":
                if typed:
                    typed = ""
                    continue
                return changed
            if kind_key == "enter":
                command, typed = typed, ""
            else:
                typed = ""
                if kind_key == "up":
                    selection = (selection - 1) % len(fields)
                elif kind_key == "down":
                    selection = (selection + 1) % len(fields)
                continue
        else:
            command = input("Field> ").strip()
        name, arg = _detail_command(command, len(fields))
        if name == "edit":
            index = selection if arg is None else arg
            _leave_raw(saved_attr)
            try:
                if _edit_field_value(cfg, fields[index]):
                    changed = True
            except _EnterEditor:
                # Fix_Config at the value prompt: keep the edits made so far
                # and return to the instance menu.
                return changed
            finally:
                saved_attr = _enter_raw()
            selection = index
        elif name == "select":
            selection = (selection + arg) % len(fields)
        elif name == "back":
            return changed
        elif name == "setup":
            if _launch_all_from_setup():
                raise _LaunchFromSetup()
            status_msg = "nothing to launch (setup.json missing or empty)"
        elif name == "quit_flow":
            raise _QuitFlow()
        elif name == "help":
            print(EDITOR_USAGE, flush=True)
            if raw:
                _read_key()  # let the user read the help before the redraw
        else:
            status_msg = f"unknown command {command!r} (field index, j/k, back)"


def _menu_driver(servers, clients, raw, saved_attr):  # noqa: PLR0912, PLR0915
    """Selection menu over existing instances; returns 'saved' or 'discarded'."""
    selection = 0
    modified = False
    status_msg = ""
    typed = ""
    last_click = (None, 0.0)
    while True:
        entries = _instance_entries(servers, clients)
        if entries and selection >= len(entries):
            selection = len(entries) - 1
        entry_rows = _print_menu(servers, clients, selection, raw, typed, status_msg)
        status_msg = ""
        if raw:
            key = None
            while key is None:
                key = _read_key()
            kind = key[0]
            if kind == "eof":
                return "discarded"  # stdin closed: leave the editor
            if kind == "char":
                typed += key[1]
                action = None
            elif kind == "enter":
                action = _menu_action_from_line(typed) if typed else ("edit", None)
                typed = ""
            else:
                if kind == "esc" and typed:
                    status_msg = "command cancelled"
                typed = ""
                if kind in ("up", "down"):
                    action = ("select", 1 if kind == "down" else -1)
                elif kind in ("delete", "backspace"):
                    action = ("delete", None)
                elif kind == "mouse":
                    button, _, y = key[1], key[2], key[3]
                    if button == _MOUSE_WHEEL_UP:
                        action = ("select", -1)
                    elif button == _MOUSE_WHEEL_DOWN:
                        action = ("select", 1)
                    elif button == 0:
                        row_index = _row_to_entry(y, entry_rows)
                        if row_index is None:
                            action = None
                        else:
                            now = time.monotonic()
                            if (
                                row_index == last_click[0]
                                and now - last_click[1] < _DOUBLE_CLICK_SECONDS
                            ):
                                selection = row_index
                                action = ("edit", None)  # double click opens the editor
                            else:
                                selection = row_index
                                action = ("select_abs", row_index)
                            last_click = (row_index, now)
                    else:
                        action = None
                else:
                    action = None
        else:
            action = _menu_action_from_line(input("Menu> ").strip())
        if action is None:
            continue
        name, arg = action
        if name == "select":
            if entries and arg is not None:
                selection = (selection + arg) % len(entries)
        elif name == "select_abs":
            if arg is not None and 0 <= arg < len(entries):
                selection = arg
            else:
                status_msg = f"invalid index: {arg}"
        elif name == "delete":
            deleted = _delete_at(servers, clients, selection)
            if deleted:
                modified = True
                status_msg = f"deleted [{deleted[0]}] {_instance_label(*deleted)}"
        elif name == "edit":
            if entries:
                kind, cfg = entries[selection]
                if _edit_instance_detail(kind, cfg, raw, saved_attr):
                    modified = True
        elif name == "help":
            print(EDITOR_USAGE, flush=True)
            if raw:
                _read_key()  # let the user read the help before the redraw
        elif name == "setup":
            if _launch_all_from_setup():
                raise _LaunchFromSetup()
            status_msg = "nothing to launch (setup.json missing or empty)"
        elif name == "quit_flow":
            raise _QuitFlow()
        elif name == "write":
            save_config(servers, clients)
            modified = False
            status_msg = "saved"
        elif name == "write_quit":
            save_config(servers, clients)
            return "saved"
        elif name == "quit":
            if modified:
                status_msg = "No write since last change (use :q! to discard)"
            else:
                return "discarded"
        elif name == "abort":
            return "discarded"
        elif name == "noop":
            status_msg = f"unknown command: {arg}"


def edit_existing_instances(servers, clients):
    """Vim-style editor to delete/change existing instances.

    Returns (status, servers, clients):
      status == "saved"     -> setup.json was written (:w / :wq); keep the
                               returned edited lists
      status == "discarded" -> the editor was exited without saving
                               (:q! / :q) and the original lists are
                               returned unchanged
    """
    edited_servers = [dict(s) for s in servers]
    edited_clients = [dict(c) for c in clients]
    raw = _raw_mode_active()
    saved_attr = _enter_raw() if raw else None
    try:
        status = _menu_driver(edited_servers, edited_clients, raw, saved_attr)
    finally:
        _leave_raw(saved_attr)
    if status == "saved":
        return "saved", edited_servers, edited_clients
    return "discarded", servers, clients


def _add_instances_loop(servers, clients):
    while True:
        print("\n--- Add New Instance ---")
        while True:
            type_choice = _input("Select type (0=Server, 1=Client): ")
            if type_choice in ("0", "1"):
                break
            print("Invalid input, please enter 0 or 1")
        is_server = type_choice == "0"
        while True:
            setup_addr = _input("Enter bind address and port (format host:port): ")
            try:
                host, port = parse_addr_port(setup_addr)
                break
            except:
                print("Invalid format, please retry")
        if is_server:
            server_len=len(servers)
            server_index=0
            config = {"host": host, "port": port}
            for server in servers:
                if config==server:
                    break
                server_index+=1
            if server_len==server_index:
                servers.append(config)
            print(f"Server config set to: {host}:{port}")
        else:
            while True:
                conn_addr = _input(
                    "Enter server address and port to connect (format host:port): "
                )
                try:
                    srv_host, srv_port = parse_addr_port(conn_addr)
                    break
                except:
                    print("Invalid format, please retry")
            config = {"client_host": host, "client_port": port, "host": srv_host, "port": srv_port}
            client_len=len(clients)
            client_index=0
            for client in clients:
                if config==client:
                    break
                client_index+=1
            if client_len==client_index:
                clients.append(config)
            print(f"Client config set to: local {host}:{port} -> server {srv_host}:{srv_port}")
        cont = _input("Continue adding more instances? (Y/N): ")
        if cont.lower() != "y":
            break
    return servers, clients


def _collect_after_editor(servers, clients):
    """Open the instance editor over existing configs, then keep collecting.

    Fix_Config entered at any prompt funnels here. After the editor is left
    with :wq / Fix_Config the user is asked whether new instances are wanted.
    """
    while True:
        try:
            status, servers, clients = edit_existing_instances(servers, clients)
            if status == "saved":
                choice = _input("Add new instances? (Y/N): ")
                if choice.lower() != "y":
                    return servers, clients
            return _add_instances_loop(servers, clients)
        except _EnterEditor:
            continue  # Fix_Config at the follow-up prompt: edit again


def interactive_collect():
    config_data = load_existing_config()
    servers = config_data["servers"]
    clients = config_data["clients"]
    try:
        if servers or clients:
            # Instances already exist: first ask whether to delete/change
            # them. An empty config skips this question and goes straight to
            # the add flow. Help/Fix_Config work at every prompt via _input().
            choice = _input("Delete or change existing instances or configs? (Y/N): ")
            if choice.lower() == "y":
                return _collect_after_editor(servers, clients)
        return _add_instances_loop(servers, clients)
    except _EnterEditor:
        # Fix_Config entered at any prompt: jump to the instance editor,
        # where startup parameters can be changed, then keep collecting.
        return _collect_after_editor(servers, clients)


def generate_configs_from_args(args):
    if args.setup_num > 1:
        print("Warning: --setup_num is ignored because only one instance per type is allowed.")
    host, port = parse_addr_port(args.setup_addr_port)
    if args.type == 0:
        config = {"host": host, "port": port}
        return [config], []
    else:
        srv_host, srv_port = parse_addr_port(args.connect_addr_port)
        config = {"client_host": host, "client_port": port, "host": srv_host, "port": srv_port}
        return [], [config]


def _load_extensions(instance, instance_type):
    """Register the extension protocols on an instance (is_extend_command)."""
    if instance_type == "server":
        command_control_extension_tcp.setup_server_commands(instance)
        forward_extension_tcp.setup_server_commands(instance)
    else:
        command_control_extension_tcp.setup_client_commands(instance)
        forward_extension_tcp.setup_client_commands(instance)


def _keep_alive(instance):
    """Keep the process alive while the instance runs in a background thread.

    Used when is_input_command_in_console=False: the instance runs in a
    daemon thread and the main thread only waits for it to stop.
    """
    try:
        while instance.running:
            time.sleep(1)
    except KeyboardInterrupt:
        stop = getattr(instance, "stop", None) or getattr(instance, "close", None)
        if stop is not None:
            try:
                stop()
            except Exception:
                traceback.print_exc()


def run_launched_instance(instance_type, config_file_path):
    try:
        with open(config_file_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        try:
            os.unlink(config_file_path)
        except:
            pass
        if instance_type == "server":
            server = TCP_Server_Base(**config)
            if config.get("is_extend_command", False):
                _load_extensions(server, "server")
            try:
                add_extension.load_registered_extensions(server, "server")
            except ImportError as e:
                print(f"Failed to load registered extensions: {e}")
            if config.get("is_input_command_in_console", True):
                server.start_TCP_Server()
            else:
                threading.Thread(target=server.start_TCP_Server, daemon=True).start()
                _keep_alive(server)
        else:
            client = TCP_Client_Base(**config)
            if config.get("is_extend_command", False):
                _load_extensions(client, "client")
            try:
                add_extension.load_registered_extensions(client, "client")
            except ImportError as e:
                print(f"Failed to load registered extensions: {e}")
            if config.get("is_input_command_in_console", True):
                client.start_TCP_client()
            else:
                threading.Thread(target=client.start_TCP_client, daemon=True).start()
                _keep_alive(client)
    except Exception as e:
        print(f"Failed to start instance: {e}")
        traceback.print_exc()
        input("Press any key to exit...")
        sys.exit(1)


def main():  # noqa: PLR0911, PLR0912, PLR0915
    if "--launch_server" in sys.argv:
        idx = sys.argv.index("--launch_server")
        try:
            cfg_idx = sys.argv.index("--config-file", idx)
            if cfg_idx + 1 < len(sys.argv):
                config_file = sys.argv[cfg_idx + 1]
                run_launched_instance("server", config_file)
            else:
                print("Error: missing --config-file argument")
                sys.exit(1)
        except ValueError:
            print("Error: missing --config-file argument")
            sys.exit(1)
        return
    if "--launch_client" in sys.argv:
        idx = sys.argv.index("--launch_client")
        try:
            cfg_idx = sys.argv.index("--config-file", idx)
            if cfg_idx + 1 < len(sys.argv):
                config_file = sys.argv[cfg_idx + 1]
                run_launched_instance("client", config_file)
            else:
                print("Error: missing --config-file argument")
                sys.exit(1)
        except ValueError:
            print("Error: missing --config-file argument")
            sys.exit(1)
        return
    parser = argparse.ArgumentParser(description="Flow Setup Launcher")
    parser.add_argument("--type", type=int, choices=[0, 1], help="0=Server, 1=Client")
    parser.add_argument(
        "--web_server", action="store_true", help="Launch the web TCP server tool (browser UI)"
    )
    parser.add_argument(
        "--web_client", action="store_true", help="Launch the web TCP client tool (browser UI)"
    )
    parser.add_argument("--setup_addr_port", type=str, help="Bind address and port (host:port)")
    parser.add_argument(
        "--connect_addr_port", type=str, help="Server address and port to connect (client required)"
    )
    parser.add_argument("--delete", type=str, nargs="+", help="Remove registered extension protocol file(s) by path")
    parser.add_argument("--add", type=str, nargs="+", help="Add extension protocol file(s) by path")
    parser.add_argument(
        "--setup_num", type=int, default=1, help="Number of instances to launch (only 1 is allowed)"
    )
    args = parser.parse_args()
    if args.web_server or args.web_client:
        if args.web_server:
            launch_web_tool("server")
        if args.web_client:
            launch_web_tool("client")
        return
    if args.add is not None:
        try:
            add_extension.add_extension(args.add)
            print("Extension(s) added successfully.")
        except Exception as e:
            print(f"Failed to add extension(s): {e}")
        return
    if args.delete is not None:
        if not os.path.exists(add_extension.added_extensions_log_file):
            print("Warning: extension registration file not found - nothing to delete.")
            return
        try:
            add_extension.remove_extension(args.delete)
            print("Extension(s) deleted successfully.")
        except Exception as e:
            print(f"Failed to delete extension(s): {e}")
        return
    if args.type is not None:
        if args.type == 0 and args.connect_addr_port is not None:
            print("Error: --connect_addr_port cannot be used in Server mode")
            sys.exit(1)
        if args.type == 1 and (args.setup_addr_port is None or args.connect_addr_port is None):
            print("Error: Client mode requires both --setup_addr_port and --connect_addr_port")
            sys.exit(1)
        if args.type == 0 and args.setup_addr_port is None:
            print("Error: Server mode requires --setup_addr_port")
            sys.exit(1)
        servers, clients = generate_configs_from_args(args)
        save_config(servers, clients)
        for cfg in servers:
            launch_instance(cfg, "server")
        for cfg in clients:
            launch_instance(cfg, "client")
        return
    if os.path.exists(config_file_dir):
        try:
            choice = _input("setup.json exists. Overwrite configuration data? (Y/N): ")
        except _EnterEditor:
            # Fix_Config: skip the question and edit the existing instances
            # (startup parameters) before launching them.
            config_data = load_existing_config()
            try:
                servers, clients = _collect_after_editor(
                    config_data["servers"], config_data["clients"]
                )
            except (_LaunchFromSetup, _QuitFlow):
                return  # Setup/Quit typed inside the editor
            save_config(servers, clients)
            for cfg in servers:
                launch_instance(cfg, "server")
            for cfg in clients:
                launch_instance(cfg, "client")
            return
        except _LaunchFromSetup:
            return  # every configured instance was launched from setup.json
        except _QuitFlow:
            return  # user asked to exit the setup program
        if choice.lower() == "n":
            config_data = load_existing_config()
            for cfg in config_data.get("servers", []):
                launch_instance(cfg, "server")
            for cfg in config_data.get("clients", []):
                launch_instance(cfg, "client")
            return
    else:
        choice = "y"
    try:
        servers, clients = interactive_collect()
    except _LaunchFromSetup:
        return  # every configured instance was launched from setup.json
    except _QuitFlow:
        return  # user asked to exit the setup program
    save_config(servers, clients)
    for cfg in servers:
        launch_instance(cfg, "server")
    for cfg in clients:
        launch_instance(cfg, "client")


if __name__ == "__main__":
    main()
