# PyFlow

[![CI](https://github.com/F18-Maverick/PyFlow/workflows/CI/badge.svg)](https://github.com/F18-Maverick/PyFlow/actions)  [![readthedocs](https://img.shields.io/readthedocs/PyFlow)](https://pyflow.readthedocs.io/en/stable/)  [![coverage](https://img.shields.io/codecov/c/github/F18-Maverick/PyFlow)](https://app.codecov.io/gh/F18-Maverick/PyFlow)  [![Pypi](https://img.shields.io/pypi/v/pyflow-net.svg)](https://pypi.org/project/pyflow-net/)  [![supported_version](https://img.shields.io/pypi/pyversions/pyflow-net)](https://img.shields.io/pypi/pyversions/pyflow-net)  [![lisence](https://img.shields.io/github/license/F18-Maverick/PyFlow)](https://github.com/F18-Maverick/PyFlow/blob/main/LICENSE)  [![commit](https://img.shields.io/github/last-commit/F18-Maverick/PyFlow)](https://github.com/F18-Maverick/PyFlow/commits/main/)

PyFlow is a high-level network protocol with APIs for transferring messages, files, and folders, plus extensible interfaces etc..

## Features

- **TCP server / client** — message exchange, custom commands, file transfer, and port allocation over a single control channel (`PyFlow/network_api/connect_tcp.py`).
- **UDP communication** — connectionless messaging (`PyFlow/network_api/connect_udp.py`).
- **Encrypted TCP channel** — RSA-OAEP message encryption with a TOFU (trust-on-first-use) peer-key registry, session nonces and sequence numbers against replay, and a circuit breaker against re-exchange storms. See [docs/Crypto](docs/Crypto/Crypto.rst) and the encrypted-channel sections of the TCP API docs.
- **C/OpenSSL cryptography library** — `libcrypto_api` provides RSA-OAEP, ECDH (P-256/384/521), HKDF-SHA256 and AES-256-GCM with a stable C API (`pf_*` prefix) usable from C, CMake or pkg-config.
- **Multi-instance launcher** — `python -m PyFlow` (package entry point backed by `PyFlow/flow_setup.py`) starts one or more server/client instances from a CLI, an interactive prompt, or a `setup.json` configuration file.
- **Extension protocols** — `command_control_extension_tcp.py` (remote command execution with log collection) and `forward_extension_tcp.py` (forwarding messages/files/folders to multiple destinations) plug into any instance via `setup_*_commands()`; `flow_setup.py` loads them automatically for every instance whose `setup.json` config sets `is_extend_command=True`, and starts instances in a background thread when `is_input_command_in_console=False`.
- **Web tool** — `PyFlow/transfer_web/` wraps the TCP protocol in a browser UI for non-library use: `setup_server.py` opens a startup-configuration page (saved to `.Flow_Web/setup_server.json`, same shape as `setup.json`) and then serves a status page plus a client-facing API; `setup_client.py` connects to a server by address, and both pages offer a sidebar of connected instances, message/file/folder sending (with forwarding to other clients), and extension loading. Backed by Flask.

## Architecture

```
PyFlow/
├── crypto_api/              C/OpenSSL library (pf_crypto, pf_rsa, pf_ecdh)
│   └── include/             public headers: pf_crypto.h, pf_rsa.h, pf_ecdh.h
├── network_api/
│   ├── connect_tcp.py       TCP_Server_Base / TCP_Client_Base
│   ├── connect_udp.py       UDP communication
│   ├── rsa_crypto.py        ctypes binding to libcrypto_api + TOFU key registry
│   └── decode_command_table.json   wire-format table for the file-transfer protocol
├── command_control_extension_tcp.py  command-control extension over TCP
├── forward_extension_tcp.py          forward extension over TCP (messages/files/folders to multiple destinations)
├── transfer_web/                     web tool: setup_server.py / setup_client.py launchers,
│   │                                 web_backend/ (Flask + TCP server wrapper),
│   │                                 web_front/ (Flask + TCP client wrapper), static/ (shared UI)
├── __init__.py / __main__.py         package launcher entry (`python -m PyFlow`)
├── flow_setup.py                     launcher implementation
└── setup.json                        default launcher configuration (generated)
test/                        C tests (test_hkdf/test_rsa/test_ecdh) + Python tests
docs/                        Sphinx documentation (multi-language)
CMakeLists.txt               top-level build for the C library and C tests
```

The Python layer runs on the standard library plus Flask (used only by the `transfer_web` web tool); the C library is loaded at runtime via `ctypes`.

## Requirements

- Python 3.10 or newer
- Pip 25.1 or newer
- CMake 3.16 or newer
- OpenSSL 1.1.1 or newer (development headers, e.g. `libssl-dev` on Debian/Ubuntu)
- A C compiler (gcc/clang on Linux/macOS, MSVC on Windows)

## Build

### 1. Build the C library (required for the encrypted channel)

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure   # optional: run the C test suite
```

This produces `build/libcrypto_api.so` (or `.dylib` / `.dll`), which `rsa_crypto.py` locates automatically.

### 2. Set up the Python environment

With [uv](https://docs.astral.sh/uv/) (the project uses `pyproject.toml` + `uv.lock`):

```bash
uv sync --group dev
```

Or with pip:

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate
pip install --group dev -e .
```

## Quick start

The examples below use `uv run` for uv users; if you installed with pip instead, drop the `uv run` prefix and use `python -m` directly.

### Interactive launcher

```bash
uv run python -m PyFlow
```

Prompts for server/client configuration, writes `setup.json`, and launches the instances.

### Command-line launcher

Start a server listening on `127.0.0.1:12345`:

```bash
uv run python -m PyFlow --type 0 --setup_addr_port 127.0.0.1:12345
```

Start a client that connects to that server (and binds its own local address/port):

```bash
uv run python -m PyFlow --type 1 --setup_addr_port 127.0.0.1:23456 --connect_addr_port 127.0.0.1:12345
```

### Web tool (browser UI)

The web tool wraps the TCP protocol in a browser UI for non-library use
(requires Flask, installed by `uv sync`). Launch it through the package
launcher or directly:

```bash
uv run python -m PyFlow --web_server   # server: config UI -> status page + API
uv run python -m PyFlow --web_client   # client: connect UI -> main UI
```

or directly:

```bash
uv run python PyFlow/transfer_web/setup_server.py
uv run python PyFlow/transfer_web/setup_client.py
```

On first run the server launcher opens a startup-configuration page
showing every `TCP_Server_Base` parameter with its default; the saved
config lives in `PyFlow/transfer_web/.Flow_Web/setup_server.json` (same
shape as `setup.json`). Once the TCP server is up, the server's web
backend serves a status page and a client-facing API
(`/api/server_info` returns the TCP address/port). The client launcher
asks for the server address (an `http`/`https` domain or a bare IP) and
connects through the server's web backend. Both pages show a sidebar of
connected instances, message/file/folder sending (client-to-client
sends are forwarded through the server), and extension loading.

### `setup.json`

A pre-written `setup.json` is honoured by the launcher:

```json
{
  "servers": [
    { "host": "127.0.0.1", "port": 12345, "max_clients": 10,
      "is_extend_command": false, "is_input_command_in_console": true }
  ],
  "clients": []
}
```

### Programmatic use

```python
from PyFlow.network_api.connect_tcp import TCP_Server_Base, TCP_Client_Base

server = TCP_Server_Base(host="127.0.0.1", port=12345, is_extend_command=True)
client = TCP_Client_Base(host="127.0.0.1", port=12345, is_extend_command=True)
```

By default both ends enable the encrypted channel (`is_enable_encrypto=True`): keys come from `~/.ssh/id_rsa` when parseable, otherwise an RSA-2048 pair is generated into `PyFlow/network_api/.Flow/pvt_key/`, and peer keys are exchanged and TOFU-checked on every connection (`PyFlow/network_api/.Flow/pub_key/pub_key.json`). See the TCP API docs for `is_custom_keys` and the full handshake.

## Testing

```bash
uv run pytest                       # full Python suite
ctest --test-dir build       # C library tests
```

The encrypted-channel tests (`test/test_crypto_rsa.py`, `test/test_crypto_tcp.py`) are skipped automatically when `libcrypto_api` has not been built; everything else runs regardless. The suite passes on Python 3.10–3.14, including the free-threaded (no-GIL) 3.14 build.

## Documentation

Sphinx sources live in `docs/` (English source with `ja`/`ko`/`ru`/`zh_CN`/`zh_TW` translations). Build the HTML docs with:

```bash
uv run python -m sphinx -b html docs build/sphinx_doc
```

Rebuild the translations (extract gettext, machine-translate new strings, compile `.mo`) with `docs/reBuild.sh`; it needs the documentation/translation dependencies from `pyproject.toml` (`sphinx`, `sphinx-intl`, `polib`, `deep-translator`).

## License

[GPL-3.0](LICENSE)
