#!/usr/bin/env python3
"""PyFlow TCP server web launcher.

Checks ``transfer_web/.Flow_Web/setup_server.json``:

- missing  -> opens the server startup-configuration UI in the browser;
  the UI saves the config (same shape as ``flow_setup``'s ``setup.json``)
  and starts the TCP server class;
- present  -> starts the TCP server class directly from the saved config.

After the TCP server is up, the lightweight Flask backend serves the
status page and the client-facing API (``/api/server_info`` etc.) on
the server's address.
"""

import os
import sys

WEB_ROOT = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(WEB_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from transfer_web.web_backend.server_backend import (  # noqa: E402
    FLOW_WEB_DIR,
    SERVER_CONFIG_FILE,
    ServerWebApp,
)


def main():
    os.makedirs(FLOW_WEB_DIR, exist_ok=True)
    app = ServerWebApp()
    if os.path.exists(SERVER_CONFIG_FILE):
        app.start_from_config()
    # else: stays in config mode, the config UI is served at /
    app.run()


if __name__ == "__main__":
    main()
