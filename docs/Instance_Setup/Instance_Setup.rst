========================
Flow Setup Launcher
========================

The ``flow_setup.py`` script is a launcher for the TCP 
server/client framework defined in ``connect_tcp.py``. 
It allows you to quickly spawn a single server or client 
instance either interactively or via command‑line 
arguments. Each launched instance runs in a separate 
terminal window (or background process on headless systems).

**Note:** This launcher supports only **one server** 
and **one client** instance at a time. Adding a new 
server or client configuration will completely overwrite 
any previous configuration of the same type.

Features
========

- **Interactive mode** – step‑by‑step creation of a 
  server or client instance.
- **Command‑line mode** – launch with all parameters 
  in one command.
- **Persistent configuration** – stores the latest 
  instance definitions in ``setup.json`` 
  (same directory as the script). Each type 
  (server/client) holds **only one** configuration, 
  which is overwritten on each update.
- **Cross‑platform** – supports Windows (cmd), Linux 
  (gnome‑terminal, xterm, or background), 
  and macOS (Terminal.app).
- **Complete parameter support** – all parameters 
  accepted by ``TCP_Server_Base`` and ``TCP_Client_Base`` 
  can be stored in ``setup.json`` for fine‑tuning.

Usage
=====

Interactive Mode
-----------------

Run the script without any arguments:

.. code-block:: bash

    python -m PyFlow

The script will ask you to:

1. Choose the type (0 for Server, 1 for Client).
2. Enter the bind address and port (``host:port``).
3. If Client, also enter the server address and port to connect to.
4. Decide whether to add another instance (if you add the same type again, the previous configuration of that type is overwritten).

If ``setup.json`` already exists, you will be prompted 
to either reuse the existing configuration 
(launch the stored instances) or overwrite it with new 
definitions.

**Important:** When you choose to overwrite, the old 
server/client configuration is **completely replaced** 
by the new one. There is no merging.

Command‑line Mode
------------------

Use the following options:

+----------------------+-------------------------------------------------------+
| Option               | Description                                           |
+======================+=======================================================+
| ``--type {0,1}``     | **Required.** 0 = Server, 1 = Client.                |
+----------------------+-------------------------------------------------------+
| ``--setup_addr_port``| **Required.** Bind address and port (e.g. ``127.0.0.1:8000``). |
+----------------------+-------------------------------------------------------+
| ``--connect_addr_port``| Required for Client only. Server address and port to connect to. |
+----------------------+-------------------------------------------------------+
| ``--setup_num``      | *Ignored.* The script always launches a single instance. This flag is accepted for compatibility but has no effect. |
+----------------------+-------------------------------------------------------+

Examples
--------

**Launch a single server** on ``127.0.0.1:8000``:

.. code-block:: bash

    python -m PyFlow --type 0 --setup_addr_port 127.0.0.1:8000

**Launch a client** bound to port ``9000``, connecting 
to a server at ``127.0.0.1:8000``:

.. code-block:: bash

    python -m PyFlow --type 1 --setup_addr_port 127.0.0.1:9000 --connect_addr_port 127.0.0.1:8000

**Launch from an existing configuration** 
(if ``setup.json`` is present):

.. code-block:: bash

    python -m PyFlow   # then answer 'N' when asked to overwrite

Configuration File
==================

The script writes a file named ``setup.json`` in the 
same directory. Its structure is:

.. code-block:: json

    {
      "servers": [
        {
          "host": "127.0.0.1",
          "port": 8000,
          // any other custom parameter for TCP_Server_Base
        }
      ],
      "clients": [
        {
          "client_host": "127.0.0.1",
          "client_port": 9000,
          "host": "127.0.0.1",
          "port": 8000,
          // any other custom parameter for TCP_Client_Base
        }
      ]
    }

**Each list contains at most one object.** When a new 
server or client configuration is added, the entire 
list for that type is replaced.

Custom Parameters
-----------------

You can manually edit ``setup.json`` to include any 
parameter accepted by ``TCP_Server_Base`` or ``TCP_Client_Base`` 
(see the source code for the full list). These custom 
values are retained when the launcher overwrites the 
configuration (since the script reads the existing config 
and updates it with user‑provided values, but if you 
choose to overwrite, the old config is discarded and
only the new fields are saved – so if you want custom 
parameters, you should add them after the first launch 
or edit the file manually).

Extension Protocols and Startup Mode
-------------------------------------

Two extension protocols ship with the launcher and are
loaded automatically for every instance whose ``setup.json``
entry sets ``is_extend_command=True``:

- ``command_control_extension_tcp.py`` – remote command
execution with per-client log collection (``/command``).
- ``forward_extension_tcp.py`` – forwarding files,
multiple files, folders and multiple folders to any
number of destination clients (``/file_forward``,
``/multiple_file_forward``, ``/folder_forward``,
``/multiple_folder_forward``).

Plain-message forwarding is native to the TCP protocol
(no extension needed): the client-only command
``/forward_send_msg`` relays messages to the listed
destination clients through the server.

With ``is_extend_command=False`` (the default) only the
raw TCP protocol is started.

The ``is_input_command_in_console`` flag selects how the
instance is started:

- ``True`` (default) – ``start_TCP_Server()`` /
``start_TCP_client()`` is called directly and the
console input loop runs in its own thread.
- ``False`` – the instance runs in a background thread
and the launcher keeps the process alive until the
instance stops (useful for headless deployments).

Both extensions also expose injectable registration
(``setup_server_commands(instance)`` /
``setup_client_commands(instance)``) and a convenience
``client_setup(instance=None, is_input_command_in_console=True)`` /
``server_setup(instance=None, is_input_command_in_console=True)``
that accepts an existing instance, so several extensions
can be loaded onto the same instance from code.

Internal Operation
==================

- Each instance is launched in a new terminal window 
- (or background process).
- The configuration is passed via a temporary JSON 
- file to avoid shell escaping issues.
- If an instance fails to start, the error is 
- displayed and the window pauses for inspection.

Requirements
============

- Python 3.6+
- The ``network_api.connect_tcp`` module must be 
- importable (the script imports ``TCP_Server_Base`` 
- and ``TCP_Client_Base`` from there).
