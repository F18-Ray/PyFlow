import argparse
import json
import os
import re
import sys
import tempfile
import threading
import time
from unittest.mock import MagicMock

import pytest


import PyFlow.flow_setup as fs

TEST_PORT = 9000


@pytest.fixture
def tmp_config(monkeypatch, tmp_path):
    """Point flow_setup's setup.json at a temp path and return that path."""
    config_path = tmp_path / "setup.json"
    monkeypatch.setattr(fs, "config_file_dir", str(config_path))
    return config_path


@pytest.fixture
def mocked_popen(monkeypatch):
    """Mock subprocess.Popen (default Windows); auto-cleanup temp files."""
    popen = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(fs.subprocess, "Popen", popen)
    monkeypatch.setattr(fs.platform, "system", lambda: "Windows")
    yield popen
    if popen.call_args is None:
        return
    cmd = popen.call_args.args[0]
    if isinstance(cmd, (list, tuple)):
        try:
            path = cmd[cmd.index("--config-file") + 1]
        except (ValueError, IndexError):
            return
    else:
        m = re.search(r'--config-file\s+"([^"]+)"', cmd)
        if not m:
            m = re.search(r"--config-file\s+(\S+)", cmd)
        path = m.group(1) if m else None
    if path and os.path.exists(path):
        os.unlink(path)


@pytest.mark.parametrize(
    "addr,expected",
    [
        (" 127.0.0.1:8080 ", ("127.0.0.1", 8080)),
        ("10.0.0.1:443", ("10.0.0.1", 443)),
    ],
)
def test_parse_addr_port(addr, expected):
    assert fs.parse_addr_port(addr) == expected


def test_parse_addr_port_invalid():
    with pytest.raises(ValueError):
        fs.parse_addr_port("not-a-port")


@pytest.mark.parametrize(
    "complete_fn,defaults,key",
    [
        (fs.complete_server_config, fs.SERVER_DEFAULTS, "max_clients"),
        (fs.complete_client_config, fs.CLIENT_DEFAULTS, "max_custom_workers"),
    ],
)
def test_complete_config_overrides_and_fills(complete_fn, defaults, key):
    cfg = complete_fn({"host": "0.0.0.0", "port": TEST_PORT})
    assert cfg["host"] == "0.0.0.0"
    assert cfg["port"] == TEST_PORT
    assert cfg[key] == defaults[key]


@pytest.mark.parametrize(
    "complete_fn,defaults_dict",
    [
        (fs.complete_server_config, fs.SERVER_DEFAULTS),
        (fs.complete_client_config, fs.CLIENT_DEFAULTS),
    ],
)
def test_complete_config_does_not_mutate_defaults(complete_fn, defaults_dict):
    before = dict(defaults_dict)
    complete_fn({"port": 9999})
    assert defaults_dict == before


def test_generate_configs_server():
    args = argparse.Namespace(
        type=0, setup_addr_port="127.0.0.1:8080", connect_addr_port=None, setup_num=1
    )
    servers, clients = fs.generate_configs_from_args(args)
    assert servers == [{"host": "127.0.0.1", "port": 8080}]
    assert clients == []


def test_generate_configs_client():
    args = argparse.Namespace(
        type=1, setup_addr_port="127.0.0.1:8080", connect_addr_port="10.0.0.1:9090", setup_num=1
    )
    servers, clients = fs.generate_configs_from_args(args)
    assert servers == []
    assert clients == [
        {"client_host": "127.0.0.1", "client_port": 8080, "host": "10.0.0.1", "port": 9090}
    ]


def test_generate_configs_ignores_setup_num(capsys):
    args = argparse.Namespace(
        type=0, setup_addr_port="127.0.0.1:8080", connect_addr_port=None, setup_num=3
    )
    servers, _ = fs.generate_configs_from_args(args)
    assert servers == [{"host": "127.0.0.1", "port": 8080}]
    assert "ignored" in capsys.readouterr().out


def test_save_config_writes_completed_single(tmp_config):
    fs.save_config([{"host": "0.0.0.0", "port": TEST_PORT}], [])
    data = json.loads(tmp_config.read_text(encoding="utf-8"))
    assert len(data["servers"]) == 1
    assert data["servers"][0]["host"] == "0.0.0.0"
    assert data["servers"][0]["port"] == TEST_PORT
    assert "max_clients" in data["servers"][0]


def test_save_config_preserves_all_configs(tmp_config):
    # Current behavior: every supplied server/client config is persisted.
    servers = [{"host": "a", "port": 1}, {"host": "b", "port": 2}]
    clients = [{"host": "c", "port": 3}, {"host": "d", "port": 4}]
    fs.save_config(servers, clients)
    data = json.loads(tmp_config.read_text(encoding="utf-8"))
    assert [s["host"] for s in data["servers"]] == ["a", "b"]
    assert [c["host"] for c in data["clients"]] == ["c", "d"]


def test_save_config_empty_lists(tmp_config):
    fs.save_config([], [])
    data = json.loads(tmp_config.read_text(encoding="utf-8"))
    assert data == {"servers": [], "clients": []}


def test_load_existing_config_missing(tmp_config):
    assert fs.load_existing_config() == {"servers": [], "clients": []}


def test_load_existing_config_present(tmp_config):
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "x", "port": 1}], "clients": []}), encoding="utf-8"
    )
    data = fs.load_existing_config()
    assert data["servers"][0]["host"] == "x"


@pytest.mark.parametrize(
    "system,which_return,instance_type,expected_arg",
    [
        ("Windows", None, "server", "--launch_server"),
        ("Linux", "xterm", "client", "--launch_client"),
        ("Darwin", None, "server", "--launch_server"),
        ("Linux", None, "server", "--launch_server"),
        ("FreeBSD", None, "client", "--launch_client"),
    ],
)
def test_launch_instance_os_branches(  # noqa: PLR0913
    monkeypatch, mocked_popen, system, which_return, instance_type, expected_arg
):
    monkeypatch.setattr(fs.platform, "system", lambda: system)
    if which_return is not None:
        monkeypatch.setattr(fs.shutil, "which", lambda t: which_return)
    elif system == "Linux":
        monkeypatch.setattr(fs.shutil, "which", lambda t: None)

    fs.launch_instance({"host": "127.0.0.1", "port": 65000}, instance_type)
    mocked_popen.assert_called_once()
    cmd = mocked_popen.call_args.args[0]
    if system in ("Windows", "Darwin") or (system == "Linux" and which_return):
        assert isinstance(cmd, str)
    else:
        assert isinstance(cmd, list)
    cmd_repr = cmd if isinstance(cmd, str) else " ".join(cmd)
    assert expected_arg in cmd_repr
    assert "--config-file" in cmd_repr
    # the child must be started as a package (python -m), not as the
    # script file, so the relative imports in flow_setup.py resolve
    assert "-m PyFlow.flow_setup" in cmd_repr
    assert "flow_setup.py" not in cmd_repr
    # and from the project root, so `PyFlow` is importable
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(fs.__file__)))
    assert mocked_popen.call_args.kwargs.get("cwd") == project_root


@pytest.mark.parametrize(
    "system,which_return,kind,expected_module",
    [
        ("Windows", None, "server", "PyFlow.transfer_web.setup_server"),
        ("Linux", "xterm", "client", "PyFlow.transfer_web.setup_client"),
        ("Darwin", None, "server", "PyFlow.transfer_web.setup_server"),
        ("Linux", None, "client", "PyFlow.transfer_web.setup_client"),
        ("FreeBSD", None, "server", "PyFlow.transfer_web.setup_server"),
    ],
)
def test_launch_web_tool_os_branches(  # noqa: PLR0913
    monkeypatch, mocked_popen, system, which_return, kind, expected_module
):
    monkeypatch.setattr(fs.platform, "system", lambda: system)
    if which_return is not None:
        monkeypatch.setattr(fs.shutil, "which", lambda t: which_return)
    elif system == "Linux":
        monkeypatch.setattr(fs.shutil, "which", lambda t: None)

    fs.launch_web_tool(kind)
    mocked_popen.assert_called_once()
    cmd = mocked_popen.call_args.args[0]
    if system in ("Windows", "Darwin") or (system == "Linux" and which_return):
        assert isinstance(cmd, str)
    else:
        assert isinstance(cmd, list)
    cmd_repr = cmd if isinstance(cmd, str) else " ".join(cmd)
    assert expected_module in cmd_repr
    # the child must be started as a package (python -m), not as the
    # script file, so the relative imports in the launcher resolve
    assert "-m PyFlow.transfer_web.setup_" in cmd_repr
    assert "setup_server.py" not in cmd_repr
    assert "setup_client.py" not in cmd_repr
    # and from the project root, so `PyFlow` is importable
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(fs.__file__)))
    assert mocked_popen.call_args.kwargs.get("cwd") == project_root


def test_launch_web_tool_popen_failure(monkeypatch, capsys):
    """Exception in Popen is caught and an error is printed."""

    def failing_popen(*args, **kwargs):
        raise OSError("mock failure")

    monkeypatch.setattr(fs.subprocess, "Popen", failing_popen)
    monkeypatch.setattr(fs.platform, "system", lambda: "Windows")

    fs.launch_web_tool("server")

    captured = capsys.readouterr()
    assert "Failed to launch web server" in captured.out
    assert "mock failure" in captured.out


def test_run_launched_instance_server_constructs(monkeypatch, tmp_path):
    server_mock = MagicMock()
    monkeypatch.setattr(fs, "TCP_Server_Base", server_mock)
    cfg = {"host": "127.0.0.1", "port": 65000}
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    fs.run_launched_instance("server", str(path))
    server_mock.assert_called_once_with(**cfg)
    assert not path.exists()


def test_run_launched_instance_client_constructs(monkeypatch, tmp_path):
    client_mock = MagicMock()
    monkeypatch.setattr(fs, "TCP_Client_Base", client_mock)
    cfg = {"host": "127.0.0.1", "port": 65000}
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    fs.run_launched_instance("client", str(path))
    client_mock.assert_called_once_with(**cfg)


def test_run_launched_instance_unlink_cleanup_fails(monkeypatch, tmp_path):
    """Inner except: pass catches os.unlink failure."""
    server_mock = MagicMock()
    monkeypatch.setattr(fs, "TCP_Server_Base", server_mock)
    monkeypatch.setattr(fs.os, "unlink", MagicMock(side_effect=PermissionError("denied")))
    cfg = {"host": "127.0.0.1", "port": 65000}
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    fs.run_launched_instance("server", str(path))
    server_mock.assert_called_once_with(**cfg)


def test_run_launched_instance_missing_file_exits(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda *a, **k: "")
    with pytest.raises(SystemExit):
        fs.run_launched_instance("server", "/no/such/file.json")


def test_run_launched_instance_extensions_loaded_and_threaded(monkeypatch, tmp_path):
    """is_extend_command=True loads the extensions; is_input_command_in_console=False
    starts the instance in a background thread and keeps the process alive."""
    server_mock = MagicMock()
    monkeypatch.setattr(fs, "TCP_Server_Base", server_mock)
    loaded = []
    monkeypatch.setattr(
        fs.command_control_extension_tcp,
        "setup_server_commands",
        lambda s: loaded.append("ctl"),
    )
    monkeypatch.setattr(
        fs.forward_extension_tcp, "setup_server_commands", lambda s: loaded.append("fwd")
    )
    monkeypatch.setattr(fs, "_keep_alive", lambda inst: None)
    cfg = {
        "host": "127.0.0.1",
        "port": 65000,
        "is_extend_command": True,
        "is_input_command_in_console": False,
    }
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    fs.run_launched_instance("server", str(path))
    assert loaded == ["ctl", "fwd"]
    for _ in range(50):  # the instance starts in a background thread
        if server_mock.return_value.start_TCP_Server.called:
            break
        time.sleep(0.01)
    assert server_mock.return_value.start_TCP_Server.called
    assert not path.exists()


def test_run_launched_instance_no_extensions_without_flag(monkeypatch, tmp_path):
    """is_extend_command=False (or missing) starts the raw protocol only."""
    server_mock = MagicMock()
    monkeypatch.setattr(fs, "TCP_Server_Base", server_mock)
    loaded = []
    monkeypatch.setattr(
        fs.command_control_extension_tcp,
        "setup_server_commands",
        lambda s: loaded.append("ctl"),
    )
    monkeypatch.setattr(
        fs.forward_extension_tcp, "setup_server_commands", lambda s: loaded.append("fwd")
    )
    cfg = {"host": "127.0.0.1", "port": 65000}  # no extension flag
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    fs.run_launched_instance("server", str(path))
    assert loaded == []
    for _ in range(50):  # the instance starts in a background thread
        if server_mock.return_value.start_TCP_Server.called:
            break
        time.sleep(0.01)
    assert server_mock.return_value.start_TCP_Server.called


def test_run_launched_instance_console_mode_starts_inline(monkeypatch, tmp_path):
    """is_input_command_in_console=True calls start directly (blocking)."""
    client_mock = MagicMock()
    monkeypatch.setattr(fs, "TCP_Client_Base", client_mock)
    started = []
    real_thread = threading.Thread

    def spy_thread(*args, **kwargs):
        started.append(kwargs.get("target"))
        return real_thread(*args, **kwargs)

    monkeypatch.setattr(fs.threading, "Thread", spy_thread)
    cfg = {"host": "127.0.0.1", "port": 65000, "is_input_command_in_console": True}
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    fs.run_launched_instance("client", str(path))
    assert started == []  # no background thread: started inline
    assert client_mock.return_value.start_TCP_client.called


def test_keep_alive_waits_while_running(monkeypatch):
    class FakeInstance:
        running = True

    fake = FakeInstance()

    def stop_after_first_sleep(_):
        fake.running = False

    monkeypatch.setattr(fs.time, "sleep", stop_after_first_sleep)
    fs._keep_alive(fake)  # returns after one wait round


def test_keep_alive_stops_on_keyboard_interrupt(monkeypatch):
    class FakeInstance:
        running = True
        stopped = False

        def stop(self):
            self.stopped = True

    fake = FakeInstance()
    monkeypatch.setattr(fs.time, "sleep", lambda _: (_ for _ in ()).throw(KeyboardInterrupt()))
    fs._keep_alive(fake)
    assert fake.stopped


@pytest.mark.parametrize(
    "inputs_seq,expected_servers,expected_clients",
    [
        (["0", "127.0.0.1:8080", "n"], [{"host": "127.0.0.1", "port": 8080}], []),
        (
            ["1", "127.0.0.1:8080", "10.0.0.1:9090", "n"],
            [],
            [{"client_host": "127.0.0.1", "client_port": 8080, "host": "10.0.0.1", "port": 9090}],
        ),
        # Retry paths: invalid type, invalid bind addr
        (["2", "0", "bad", "127.0.0.1:8080", "n"], [{"host": "127.0.0.1", "port": 8080}], []),
        # Retry paths: invalid type, invalid bind addr, invalid connect addr
        (
            ["x", "1", "bad", "127.0.0.1:8080", "bad2", "10.0.0.1:9090", "n"],
            [],
            [{"client_host": "127.0.0.1", "client_port": 8080, "host": "10.0.0.1", "port": 9090}],
        ),
    ],
)
def test_interactive_collect(
    monkeypatch, inputs_seq, expected_servers, expected_clients, tmp_config
):
    # interactive_collect() seeds from the existing config file.
    tmp_config.write_text(json.dumps({"servers": [], "clients": []}), encoding="utf-8")
    inputs = iter(inputs_seq)
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == expected_servers
    assert clients == expected_clients


# ---- main() CLI entry point ------------------------------------------------

@pytest.fixture
def cli_cleanup(monkeypatch, tmp_path):
    """Isolate main() from the real filesystem and launch side effects."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fs, "launch_instance", MagicMock())
    monkeypatch.setattr(fs, "save_config", MagicMock())
    monkeypatch.setattr(fs, "interactive_collect", MagicMock(return_value=([], [])))
    monkeypatch.setattr(fs, "run_launched_instance", MagicMock())


def test_main_launch_server_with_config_file(monkeypatch, cli_cleanup):
    tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump({"host": "127.0.0.1", "port": 1}, tmp)
    tmp.close()
    monkeypatch.setattr(sys, "argv", ["flow_setup", "--launch_server", "--config-file", tmp.name])
    fs.main()
    fs.run_launched_instance.assert_called_once_with("server", tmp.name)
    os.unlink(tmp.name)


def test_main_launch_client_with_config_file(monkeypatch, tmp_path, cli_cleanup):
    cfg = tmp_path / "c.json"
    cfg.write_text(json.dumps({"host": "127.0.0.1", "port": 1}))
    monkeypatch.setattr(
        sys, "argv", ["flow_setup", "--launch_client", "--config-file", str(cfg)]
    )
    fs.main()
    fs.run_launched_instance.assert_called_once_with("client", str(cfg))


def test_main_launch_server_missing_config_file(monkeypatch, cli_cleanup):
    monkeypatch.setattr(sys, "argv", ["flow_setup", "--launch_server"])
    with pytest.raises(SystemExit):
        fs.main()


def test_main_type_server(monkeypatch, cli_cleanup):
    """--type 0 with a bind address saves config and launches a server."""
    monkeypatch.setattr(
        sys, "argv", ["flow_setup", "--type", "0", "--setup_addr_port", "127.0.0.1:12345"]
    )
    fs.main()
    servers, clients = fs.save_config.call_args.args
    assert servers == [{"host": "127.0.0.1", "port": 12345}]
    assert clients == []
    fs.launch_instance.assert_called_once_with({"host": "127.0.0.1", "port": 12345}, "server")


def test_main_type_client(monkeypatch, cli_cleanup):
    """--type 1 requires both addresses; launches a client."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "flow_setup",
            "--type",
            "1",
            "--setup_addr_port",
            "127.0.0.1:23456",
            "--connect_addr_port",
            "127.0.0.1:12345",
        ],
    )
    fs.main()
    servers, clients = fs.save_config.call_args.args
    assert servers == []
    assert clients == [{"client_host": "127.0.0.1", "client_port": 23456,
                        "host": "127.0.0.1", "port": 12345}]
    fs.launch_instance.assert_called_once_with(
        {"client_host": "127.0.0.1", "client_port": 23456, "host": "127.0.0.1", "port": 12345},
        "client",
    )


def test_main_type_server_with_connect_addr_rejected(monkeypatch, cli_cleanup):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "flow_setup",
            "--type",
            "0",
            "--setup_addr_port",
            "127.0.0.1:12345",
            "--connect_addr_port",
            "127.0.0.1:9999",
        ],
    )
    with pytest.raises(SystemExit):
        fs.main()


def test_main_type_client_missing_args_rejected(monkeypatch, cli_cleanup):
    monkeypatch.setattr(
        sys, "argv", ["flow_setup", "--type", "1", "--setup_addr_port", "127.0.0.1:1"]
    )
    with pytest.raises(SystemExit):
        fs.main()


def test_main_web_server_flag(monkeypatch, cli_cleanup):
    """--web_server launches the web TCP server tool and returns."""
    monkeypatch.setattr(fs, "launch_web_tool", MagicMock())
    monkeypatch.setattr(sys, "argv", ["flow_setup", "--web_server"])
    fs.main()
    fs.launch_web_tool.assert_called_once_with("server")


def test_main_web_client_flag(monkeypatch, cli_cleanup):
    """--web_client launches the web TCP client tool and returns."""
    monkeypatch.setattr(fs, "launch_web_tool", MagicMock())
    monkeypatch.setattr(sys, "argv", ["flow_setup", "--web_client"])
    fs.main()
    fs.launch_web_tool.assert_called_once_with("client")


def test_main_web_both_flags_launch_both(monkeypatch, cli_cleanup):
    """Both web flags together launch both tools."""
    monkeypatch.setattr(fs, "launch_web_tool", MagicMock())
    monkeypatch.setattr(sys, "argv", ["flow_setup", "--web_server", "--web_client"])
    fs.main()
    assert fs.launch_web_tool.call_args_list == [
        (("server",),),
        (("client",),),
    ]


def test_main_existing_setup_json_keep_existing(monkeypatch, cli_cleanup, tmp_config):
    """setup.json exists and user answers 'n': load and launch existing config."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []})
    )
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    monkeypatch.setattr("builtins.input", lambda _: "n")
    fs.main()
    fs.launch_instance.assert_called_once_with({"host": "127.0.0.1", "port": 7000}, "server")
    fs.interactive_collect.assert_not_called()


def test_main_existing_setup_json_overwrite(monkeypatch, cli_cleanup, tmp_config):
    """setup.json exists and user answers 'y': interactive collection replaces it."""
    tmp_config.write_text(json.dumps({"servers": [], "clients": []}))
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    monkeypatch.setattr("builtins.input", lambda _: "y")
    fs.main()
    fs.interactive_collect.assert_called_once()


# ---- instance editor: reduce/change existing instances ---------------------


@pytest.fixture
def line_mode(monkeypatch):
    """Force the instance editor into line-command mode (no raw terminal)."""
    monkeypatch.setattr(fs, "_raw_mode_active", lambda: False)


def test_reduce_prompt_no_continues_add_flow(monkeypatch, line_mode, tmp_config):
    """N on the reduce question keeps everything and continues the add flow."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["n", "0", "127.0.0.1:8080", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [
        {"host": "127.0.0.1", "port": 7000},
        {"host": "127.0.0.1", "port": 8080},
    ]
    assert clients == []


def test_reduce_prompt_yes_delete_default_selection(monkeypatch, line_mode, tmp_config):
    """Y: dd deletes the first (default-selected) instance, :wq saves."""
    tmp_config.write_text(
        json.dumps(
            {
                "servers": [
                    {"host": "127.0.0.1", "port": 7000},
                    {"host": "127.0.0.1", "port": 7001},
                ],
                "clients": [{"client_host": "127.0.0.1", "client_port": 7002}],
            }
        ),
        encoding="utf-8",
    )
    inputs = iter(["y", "dd", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [{"host": "127.0.0.1", "port": 7001}]
    assert clients == [{"client_host": "127.0.0.1", "client_port": 7002}]


def test_reduce_prompt_yes_wq_keeps_all_without_edit(monkeypatch, line_mode, tmp_config):
    """:wq with no edits keeps all instances and asks about new ones."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "a", "port": 1}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["y", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [{"host": "a", "port": 1}]
    assert clients == []


def test_reduce_prompt_yes_wq_then_add_new_instances(monkeypatch, line_mode, tmp_config):
    """After :wq the add loop is skipped and 'add new instances' is asked."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "a", "port": 1}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["y", ":wq", "y", "0", "127.0.0.1:8080", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert [s["host"] for s in servers] == ["a", "127.0.0.1"]
    assert clients == []


def test_reduce_prompt_yes_fix_config_saves_and_exits(monkeypatch, line_mode, tmp_config):
    """Fix_Config in the menu saves like :wq and exits the editor."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["y", "dd", "Fix_Config", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == []
    assert clients == []


def test_reduce_prompt_yes_qbang_discards_changes(monkeypatch, line_mode, tmp_config):
    """:q! also discards changes and returns to the normal add flow."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["y", "dd", ":q!", "0", "127.0.0.1:8080", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [
        {"host": "127.0.0.1", "port": 7000},
        {"host": "127.0.0.1", "port": 8080},
    ]
    assert clients == []


def test_reduce_prompt_yes_help_prints_usage(monkeypatch, line_mode, tmp_config, capsys):
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["y", "Help", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    captured = capsys.readouterr().out
    assert ":wq" in captured
    assert "Fix_Config" in captured
    assert "Delete" in captured
    assert servers == [{"host": "127.0.0.1", "port": 7000}]
    assert clients == []


def test_reduce_prompt_yes_enter_edits_selected_field(monkeypatch, line_mode, tmp_config):
    """Enter opens the full config; a field can be changed and saved with :wq."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    # menu -> Enter -> edit field 0 (host) -> new value -> back -> :wq -> n
    inputs = iter(["y", "", "0", "10.0.0.1", "back", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [{"host": "10.0.0.1", "port": 7000}]
    assert clients == []


def test_reduce_prompt_yes_edit_rejects_invalid_value(monkeypatch, line_mode, tmp_config, capsys):
    """Invalid typed values are rejected and the field keeps its old value."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    # edit field 1 (port): first try a bad value, then a good one
    inputs = iter(["y", "", "1", "not-a-port", "9000", "back", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [{"host": "127.0.0.1", "port": 9000}]
    assert "invalid integer" in capsys.readouterr().out


# ---- instance editor: raw key decoding (terminal mode) ---------------------


def test_decode_posix_seq_keys():
    assert fs._decode_posix_seq(b"\x1b[A") == ("up",)
    assert fs._decode_posix_seq(b"\x1b[B") == ("down",)
    assert fs._decode_posix_seq(b"\x1b[3~") == ("delete",)
    assert fs._decode_posix_seq(b"\r") == ("enter",)
    assert fs._decode_posix_seq(b"\x7f") == ("backspace",)
    assert fs._decode_posix_seq(b"\x1b") == ("esc",)
    assert fs._decode_posix_seq(b"x") == ("char", "x")
    assert fs._decode_posix_seq(b"") == ("eof",)
    with pytest.raises(KeyboardInterrupt):
        fs._decode_posix_seq(b"\x03")


def test_decode_posix_seq_mouse():
    assert fs._decode_posix_seq(b"\x1b[<0;10;5M") == ("mouse", 0, 10, 5)
    assert fs._decode_posix_seq(b"\x1b[<64;3;2M") == ("mouse", 64, 3, 2)  # wheel up
    assert fs._decode_posix_seq(b"\x1b[<65;3;2M") == ("mouse", 65, 3, 2)  # wheel down


def test_decode_posix_seq_unknown_escape_buffers_tail():
    """Unknown escape sequences keep trailing bytes for the next reads."""
    try:
        assert fs._decode_posix_seq(b"\x1b[99~rst") == ("esc",)
        assert fs._pending_bytes == b"[99~rst"
        assert fs._read_key() == ("char", "[")
    finally:
        fs._pending_bytes = b""


def test_menu_action_from_line_commands():
    assert fs._menu_action_from_line("") == ("edit", None)
    assert fs._menu_action_from_line("j") == ("select", 1)
    assert fs._menu_action_from_line("k") == ("select", -1)
    assert fs._menu_action_from_line("dd") == ("delete", None)
    assert fs._menu_action_from_line("backspace") == ("delete", None)
    assert fs._menu_action_from_line(":wq") == ("write_quit", None)
    assert fs._menu_action_from_line(":w") == ("write", None)
    assert fs._menu_action_from_line(":q") == ("quit", None)
    assert fs._menu_action_from_line(":q!") == ("abort", None)
    assert fs._menu_action_from_line("Fix_Config") == ("write_quit", None)
    assert fs._menu_action_from_line("Setup") == ("setup", None)
    assert fs._menu_action_from_line("Quit") == ("quit_flow", None)
    assert fs._menu_action_from_line("Help") == ("help", None)
    assert fs._menu_action_from_line("2") == ("select_abs", 2)
    assert fs._menu_action_from_line("nonsense")[0] == "noop"


def test_detail_command():
    assert fs._detail_command("", 12) == ("edit", None)
    assert fs._detail_command("4", 12) == ("edit", 4)
    assert fs._detail_command("j", 12) == ("select", 1)
    assert fs._detail_command("back", 12) == ("back", None)
    assert fs._detail_command("Fix_Config", 12) == ("back", None)
    assert fs._detail_command("Setup", 12) == ("setup", None)
    assert fs._detail_command("Quit", 12) == ("quit_flow", None)


def test_print_menu_aligns_columns(capsys):
    """Menu rows share aligned columns: index is right-aligned, kind and
    label start at the same column on every row."""
    servers = [{"host": "10.0.0.1", "port": 1000 + i} for i in range(10)]
    clients = [{"client_host": "10.0.0.1", "client_port": 2000}]
    fs._print_menu(servers, clients, 0, False, "", "")
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("=== Instance Manager ===")
    entry_lines = lines[1:]
    expected_entries = 11  # 10 servers + 1 client
    assert len(entry_lines) == expected_entries
    # index column: every ']' sits at the same column (right-aligned index)
    close_cols = {line.index("]") for line in entry_lines}
    assert len(close_cols) == 1
    # kind column: [server]/[client] start at the same column
    kind_cols = {
        line.index("[server]") if "[server]" in line else line.index("[client]")
        for line in entry_lines
    }
    assert len(kind_cols) == 1


def test_print_menu_wraps_long_labels(capsys, monkeypatch):
    """Rows wider than the terminal wrap, indented to the label column."""
    monkeypatch.setattr(fs, "_terminal_width", lambda: 25)
    servers = [{"host": "127.0.0.1", "port": 65432}]
    fs._print_menu(servers, [], 0, False, "", "")
    lines = capsys.readouterr().out.splitlines()
    wrapped_rows = 3  # title + wrapped entry (two rows)
    assert len(lines) == wrapped_rows
    prefix = "> [0] [server] "  # selected marker + aligned columns
    assert lines[1].startswith(prefix)
    # continuation line is indented to the label column
    assert len(lines[2]) - len(lines[2].lstrip()) == len(prefix)


def test_detail_rows_align_key_column(monkeypatch, capsys):
    """Field rows align the '=' sign: keys are padded to the longest key."""
    monkeypatch.setattr("builtins.input", lambda *a, **k: "back")
    cfg = {"host": "127.0.0.1", "port": 65432}
    fs._edit_instance_detail("server", cfg, False, None)
    lines = capsys.readouterr().out.splitlines()
    eq_cols = {line.index(" =") for line in lines[2:] if "=" in line}
    assert len(eq_cols) == 1
    assert fs._detail_command("99", 12)[0] == "noop"  # out of range field index


# ---- Help / Fix_Config at any prompt ---------------------------------------


def test_input_help_at_reduce_prompt_reasks(monkeypatch, line_mode, tmp_config, capsys):
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "a", "port": 1}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["Help", "n", "0", "127.0.0.1:8080", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert "Fix_Config" in capsys.readouterr().out
    assert [s["host"] for s in servers] == ["a", "127.0.0.1"]
    assert clients == []


def test_input_fix_config_at_reduce_prompt_opens_editor(monkeypatch, line_mode, tmp_config):
    """Fix_Config at the reduce prompt opens the editor; :wq keeps config."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "a", "port": 1}], "clients": []}),
        encoding="utf-8",
    )
    inputs = iter(["Fix_Config", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [{"host": "a", "port": 1}]
    assert clients == []


def test_input_help_at_type_prompt_reasks(monkeypatch, line_mode, tmp_config, capsys):
    tmp_config.write_text(json.dumps({"servers": [], "clients": []}), encoding="utf-8")
    inputs = iter(["Help", "0", "127.0.0.1:8080", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert "Fix_Config" in capsys.readouterr().out
    assert servers == [{"host": "127.0.0.1", "port": 8080}]
    assert clients == []


def test_input_fix_config_mid_add_loop_opens_editor(monkeypatch, line_mode, tmp_config):
    """Fix_Config in the add loop opens the editor over the collection."""
    tmp_config.write_text(json.dumps({"servers": [], "clients": []}), encoding="utf-8")
    inputs = iter(["0", "127.0.0.1:8080", "Fix_Config", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [{"host": "127.0.0.1", "port": 8080}]
    assert clients == []


def test_input_fix_config_at_field_value_returns_to_menu(monkeypatch, line_mode, tmp_config):
    """Fix_Config at a value prompt returns to the instance menu, no launch."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    # reduce y -> menu Enter -> field 0 -> Fix_Config at the value prompt
    inputs = iter(["y", "", "0", "Fix_Config", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    servers, clients = fs.interactive_collect()
    assert servers == [{"host": "127.0.0.1", "port": 7000}]
    assert clients == []


def test_main_overwrite_fix_config_opens_editor(monkeypatch, cli_cleanup, tmp_config):
    """Fix_Config at the overwrite prompt opens the editor, then launches."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []})
    )
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    inputs = iter(["Fix_Config", ":wq", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    fs.main()
    fs.save_config.assert_called()
    fs.launch_instance.assert_called_once_with({"host": "127.0.0.1", "port": 7000}, "server")
    fs.interactive_collect.assert_not_called()


def test_main_overwrite_help_reprints_usage(monkeypatch, cli_cleanup, tmp_config, capsys):
    tmp_config.write_text(json.dumps({"servers": [], "clients": []}))
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    inputs = iter(["Help", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    fs.main()
    assert "Fix_Config" in capsys.readouterr().out
    fs.launch_instance.assert_not_called()
    fs.interactive_collect.assert_not_called()


# ---- Setup: launch every instance from setup.json ---------------------------


def test_main_overwrite_setup_launches_all_instances(monkeypatch, cli_cleanup, tmp_config):
    """Setup at the overwrite prompt starts every instance from setup.json."""
    tmp_config.write_text(
        json.dumps(
            {
                "servers": [{"host": "127.0.0.1", "port": 7000}],
                "clients": [{"client_host": "127.0.0.1", "client_port": 7002}],
            }
        )
    )
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    monkeypatch.setattr("builtins.input", lambda _: "Setup")
    fs.main()
    expected_launches = 2  # one server + one client
    assert fs.launch_instance.call_count == expected_launches
    fs.launch_instance.assert_any_call({"host": "127.0.0.1", "port": 7000}, "server")
    fs.launch_instance.assert_any_call(
        {"client_host": "127.0.0.1", "client_port": 7002}, "client"
    )
    fs.interactive_collect.assert_not_called()


def test_main_setup_with_empty_setup_json_asks_again(monkeypatch, cli_cleanup, tmp_config, capsys):
    """Setup with nothing configured prints a message and re-asks."""
    tmp_config.write_text(json.dumps({"servers": [], "clients": []}))
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    inputs = iter(["Setup", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    fs.main()
    captured = capsys.readouterr().out
    assert "nothing to launch" in captured
    fs.launch_instance.assert_not_called()
    fs.interactive_collect.assert_not_called()


def test_input_setup_in_add_loop_launches_all(monkeypatch, line_mode, tmp_config):
    """Setup typed in the add loop launches setup.json instances and stops."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    launched = []
    monkeypatch.setattr(fs, "launch_instance", lambda cfg, kind: launched.append((cfg, kind)))
    # reduce prompt: n -> add loop -> Setup at the type prompt
    inputs = iter(["n", "Setup"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    with pytest.raises(fs._LaunchFromSetup):
        fs.interactive_collect()
    assert launched == [({"host": "127.0.0.1", "port": 7000}, "server")]


def test_setup_in_instance_menu_launches_all(monkeypatch, line_mode, tmp_config):
    """Setup typed in the instance menu launches setup.json and stops."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    launched = []
    monkeypatch.setattr(fs, "launch_instance", lambda cfg, kind: launched.append((cfg, kind)))
    # reduce y -> menu -> Setup
    inputs = iter(["y", "Setup"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    with pytest.raises(fs._LaunchFromSetup):
        fs.interactive_collect()
    assert launched == [({"host": "127.0.0.1", "port": 7000}, "server")]


# ---- Quit: exit the setup program -------------------------------------------


def test_main_overwrite_quit_exits(monkeypatch, cli_cleanup, tmp_config):
    """Quit at the overwrite prompt ends the program without launching."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []})
    )
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    monkeypatch.setattr("builtins.input", lambda _: "Quit")
    fs.main()  # returns normally: the program ends right here
    fs.launch_instance.assert_not_called()
    fs.save_config.assert_not_called()
    fs.interactive_collect.assert_not_called()


def test_input_quit_in_add_loop_exits(monkeypatch, line_mode, tmp_config):
    """Quit typed in the add loop aborts the whole flow without launching."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    launched = []
    monkeypatch.setattr(fs, "launch_instance", lambda cfg, kind: launched.append((cfg, kind)))
    inputs = iter(["n", "Quit"])  # reduce: n -> add loop -> Quit
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    with pytest.raises(fs._QuitFlow):
        fs.interactive_collect()
    assert launched == []


def test_quit_in_instance_menu_exits(monkeypatch, line_mode, tmp_config):
    """Quit typed in the instance menu exits without saving or launching."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []}),
        encoding="utf-8",
    )
    launched = []
    monkeypatch.setattr(fs, "launch_instance", lambda cfg, kind: launched.append((cfg, kind)))
    inputs = iter(["y", "Quit"])  # reduce y -> menu -> Quit
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    with pytest.raises(fs._QuitFlow):
        fs.interactive_collect()
    assert launched == []


def test_main_fix_config_then_quit_in_editor_exits(monkeypatch, cli_cleanup, tmp_config):
    """Quit inside the editor reached via Fix_Config ends the program cleanly."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []})
    )
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    inputs = iter(["Fix_Config", "Quit"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    fs.main()  # must return normally, no traceback
    fs.launch_instance.assert_not_called()
    fs.save_config.assert_not_called()


def test_main_fix_config_then_setup_in_editor_launches(monkeypatch, cli_cleanup, tmp_config):
    """Setup inside the editor reached via Fix_Config launches and ends."""
    tmp_config.write_text(
        json.dumps({"servers": [{"host": "127.0.0.1", "port": 7000}], "clients": []})
    )
    monkeypatch.setattr(sys, "argv", ["flow_setup"])
    inputs = iter(["Fix_Config", "Setup"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    fs.main()  # must return normally, no traceback
    fs.launch_instance.assert_called_once_with({"host": "127.0.0.1", "port": 7000}, "server")
