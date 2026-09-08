/* PyFlow transfer_web shared frontend logic.
 * window.WEB_MODE is "server" or "client" (set by the template).
 */
(function () {
  "use strict";

  const MODE = window.WEB_MODE || "client";
  const $ = (id) => document.getElementById(id);

  const state = {
    serverInfo: null,
    clients: [],
    ownAddress: null,
    connected: false,
    target: null, // "server" | [ip, port]
    extensions: [], // [{name, icon, command}]
    messages: [], // [{dir: "out"|"sys", text}]
    eventId: 0, // last inbound event id consumed from /api/events
  };

  /* ---------------- helpers ---------------- */

  function toast(text, kind) {
    const el = document.createElement("div");
    el.className = "toast" + (kind ? " " + kind : "");
    el.textContent = text;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3500);
  }

  async function api(path, options) {
    const resp = await fetch(path, options);
    let data = null;
    try {
      data = await resp.json();
    } catch (e) {
      data = {};
    }
    if (!resp.ok || data.ok === false) {
      throw new Error(data.error || ("HTTP " + resp.status));
    }
    return data;
  }

  function esc(s) {
    const div = document.createElement("div");
    div.textContent = s == null ? "" : String(s);
    return div.innerHTML;
  }

  function targetLabel(target) {
    if (target === "server") {
      return "Server " + (state.serverInfo ? state.serverInfo.host + ":" + state.serverInfo.port : "");
    }
    return target[0] + ":" + target[1];
  }

  function fmtSize(n) {
    if (n == null || isNaN(n)) return "?";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let v = Number(n);
    let i = 0;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
    }
    return (v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)) + " " + units[i];
  }

  function isSelf(entry) {
    return (
      state.ownAddress &&
      entry.ip === state.ownAddress.ip &&
      entry.port === state.ownAddress.port
    );
  }

  /* ---------------- sidebar ---------------- */

  function renderSidebar() {
    const list = $("instance-list");
    list.innerHTML = "";
    const serverEntry = document.createElement("div");
    serverEntry.className = "instance" + (state.target === "server" ? " active" : "");
    serverEntry.innerHTML =
      '<span class="dot server"></span><span class="name">' +
      esc("Server " + (state.serverInfo ? state.serverInfo.host + ":" + state.serverInfo.port : "")) +
      '</span><span class="tag">server</span>';
    serverEntry.addEventListener("click", () => selectTarget("server"));
    list.appendChild(serverEntry);

    let shown = 0;
    state.clients.forEach((c) => {
      // a client never lists itself; the server is never in the client list
      if (MODE === "client" && isSelf(c)) return;
      shown++;
      const el = document.createElement("div");
      const key = c.ip + ":" + c.port;
      const active =
        state.target !== "server" &&
        state.target &&
        state.target[0] === c.ip &&
        state.target[1] === c.port;
      el.className = "instance" + (active ? " active" : "");
      el.innerHTML =
        '<span class="dot client"></span>' +
        '<span class="name">' + esc(key) + "</span>" +
        '<span class="tag">client</span>';
      el.addEventListener("click", () => selectTarget([c.ip, c.port]));
      list.appendChild(el);
    });
    if (!shown) {
      const empty = document.createElement("div");
      empty.className = "empty-hint";
      empty.style.padding = "16px 8px";
      empty.textContent = "No clients connected";
      list.appendChild(empty);
    }
  }

  function selectTarget(target) {
    state.target = target;
    renderSidebar();
    const title = $("target-title");
    if (MODE === "server" && target === "server") {
      title.textContent = "This is the server";
      $("empty-hint").style.display = "block";
      $("empty-hint").textContent =
        "This is the server. Select a connected client on the left to send data.";
      $("input").disabled = true;
      $("send-btn").disabled = true;
      $("icon-bar").style.opacity = "0.4";
      $("icon-bar").style.pointerEvents = "none";
      return;
    }
    title.textContent = "Sending to " + targetLabel(target);
    $("empty-hint").style.display = "none";
    $("input").disabled = false;
    $("send-btn").disabled = false;
    $("icon-bar").style.opacity = "1";
    $("icon-bar").style.pointerEvents = "auto";
    $("input").focus();
  }

  /* ---------------- status polling ---------------- */

  async function refreshStatus() {
    try {
      const data = await api("/api/status");
      state.serverInfo = data.server_info;
      state.clients = data.clients || [];
      state.ownAddress = data.own_address || null;
      state.connected = MODE === "server" ? !!data.running : !!data.connected;
      const conn = $("conn-indicator");
      conn.textContent = state.connected ? "connected" : "disconnected";
      conn.className = "conn " + (state.connected ? "on" : "off");
      const meta = $("server-meta");
      meta.textContent = state.serverInfo
        ? state.serverInfo.host + ":" + state.serverInfo.port
        : "no server info";
      renderSidebar();
    } catch (e) {
      const conn = $("conn-indicator");
      conn.textContent = "offline";
      conn.className = "conn off";
    }
  }

  /* ---------------- inbound events ---------------- */

  async function refreshEvents() {
    let data;
    try {
      data = await api("/api/events?since=" + state.eventId);
    } catch (e) {
      return; // offline or a backend without the endpoint
    }
    let rendered = 0;
    (data.events || []).forEach((ev) => {
      if (ev.id) state.eventId = Math.max(state.eventId, ev.id);
      const from = ev.from ? ev.from + ": " : "";
      if (ev.type === "msg") {
        addMessage("in", from + ev.text);
        rendered++;
      } else if (ev.type === "file") {
        const size = fmtSize(ev.size);
        addMessage("in", from + "file received: " + ev.name + " (" + size + ") -> " + ev.path);
        toast("File received: " + ev.name + " (" + size + ")", "ok");
        rendered++;
      }
    });
    if (rendered && MODE === "server") {
      const hint = $("empty-hint");
      if (hint) hint.style.display = "none";
    }
  }

  /* ---------------- sending ---------------- */

  function currentTarget() {
    if (!state.target) {
      toast("Select a target on the left first", "err");
      return null;
    }
    if (MODE === "server" && state.target === "server") {
      toast("The server cannot send to itself", "err");
      return null;
    }
    return state.target;
  }

  async function sendMessage() {
    const target = currentTarget();
    if (!target) return;
    const input = $("input");
    const text = input.value.trim();
    if (!text) return;
    try {
      await api("/api/send_msg", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target, message: text }),
      });
      addMessage("out", text);
      input.value = "";
      input.style.height = "auto";
    } catch (e) {
      toast("Send failed: " + e.message, "err");
    }
  }

  function addMessage(dir, text) {
    const area = $("chat-area");
    const el = document.createElement("div");
    el.className = "msg " + dir;
    el.textContent = text;
    area.appendChild(el);
    area.scrollTop = area.scrollHeight;
  }

  /* ---------------- modals ---------------- */

  function openModal(html) {
    const backdrop = document.createElement("div");
    backdrop.className = "modal-backdrop";
    backdrop.innerHTML = '<div class="modal">' + html + "</div>";
    backdrop.addEventListener("click", (e) => {
      if (e.target === backdrop) backdrop.remove();
    });
    document.body.appendChild(backdrop);
    return backdrop;
  }

  function closeModal(backdrop) {
    backdrop.remove();
  }

  /* file transfer modal */
  function openFileModal(folderMode) {
    const backdrop = openModal(
      '<h2>' + (folderMode ? "Send folder" : "Send file(s)") + "</h2>" +
      '<div class="drop-zone" id="drop-zone">' +
      (folderMode
        ? "Click to choose a folder, or drop a folder here"
        : "Click to choose file(s), or drop them here") +
      '<input type="file" id="file-input" ' +
      (folderMode ? 'webkitdirectory directory multiple' : "multiple") + "></div>" +
      '<div class="file-list" id="file-list"></div>' +
      '<div class="actions"><button class="btn" id="confirm-btn">Send</button> ' +
      '<button class="btn btn-ghost" id="cancel-btn">Cancel</button></div>'
    );
    const zone = backdrop.querySelector("#drop-zone");
    const input = backdrop.querySelector("#file-input");
    const list = backdrop.querySelector("#file-list");
    let files = [];

    function showFiles() {
      list.innerHTML = "";
      files.forEach((f) => {
        const div = document.createElement("div");
        div.textContent = folderMode ? f.webkitRelativePath || f.name : f.name;
        list.appendChild(div);
      });
    }

    zone.addEventListener("click", () => input.click());
    zone.addEventListener("dragover", (e) => {
      e.preventDefault();
      zone.classList.add("drag");
    });
    zone.addEventListener("dragleave", () => zone.classList.remove("drag"));
    zone.addEventListener("drop", (e) => {
      e.preventDefault();
      zone.classList.remove("drag");
      files = Array.from(e.dataTransfer.files);
      showFiles();
    });
    input.addEventListener("change", () => {
      files = Array.from(input.files);
      showFiles();
    });
    backdrop.querySelector("#cancel-btn").addEventListener("click", () => closeModal(backdrop));
    backdrop.querySelector("#confirm-btn").addEventListener("click", async () => {
      if (!files.length) {
        toast("No files selected", "err");
        return;
      }
      const target = currentTarget();
      if (!target) return;
      const fd = new FormData();
      fd.append("target", JSON.stringify(target));
      files.forEach((f) => {
        fd.append("files", f, folderMode ? f.webkitRelativePath || f.name : f.name);
      });
      try {
        await api("/api/" + (folderMode ? "send_folder" : "send_file"), { method: "POST", body: fd });
        toast("Transfer started", "ok");
        closeModal(backdrop);
      } catch (e) {
        toast("Transfer failed: " + e.message, "err");
      }
    });
  }

  /* extension protocol add modal (the "+" button) */
  async function openAddProtocolModal() {
    let commands = [];
    try {
      commands = (await api("/api/available_commands")).commands || [];
    } catch (e) {
      toast("Cannot list commands: " + e.message, "err");
      return;
    }
    if (!commands.length) {
      toast("No extension commands registered. Add an extension first.", "err");
      return;
    }
    const backdrop = openModal(
      '<h2>Add an extension protocol</h2>' +
      '<div class="field"><label>Protocol name</label><input type="text" id="ext-name" placeholder="e.g. Remote Command"></div>' +
      '<div class="field"><label>Custom icon (emoji or short text)</label><input type="text" id="ext-icon" placeholder="e.g. &#9881;"></div>' +
      '<div class="field"><label>Extension method (registered command)</label>' +
      '<select id="ext-command">' + commands.map((c) => '<option value="' + esc(c) + '">' + esc(c) + "</option>").join("") +
      "</select></div>" +
      '<div class="actions"><button class="btn" id="ext-add-btn">Add</button> ' +
      '<button class="btn btn-ghost" id="ext-cancel-btn">Cancel</button></div>'
    );
    backdrop.querySelector("#ext-cancel-btn").addEventListener("click", () => closeModal(backdrop));
    backdrop.querySelector("#ext-add-btn").addEventListener("click", async () => {
      const name = backdrop.querySelector("#ext-name").value.trim();
      const icon = backdrop.querySelector("#ext-icon").value.trim() || "&#9881;";
      const command = backdrop.querySelector("#ext-command").value;
      if (!name) {
        toast("Protocol name is required", "err");
        return;
      }
      state.extensions.push({ name, icon, command });
      try {
        await api("/api/extensions_ui", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ extensions: state.extensions }),
        });
        renderExtensionIcons();
        closeModal(backdrop);
        toast("Protocol added", "ok");
      } catch (e) {
        toast("Failed: " + e.message, "err");
      }
    });
  }

  /* extension command execution modal */
  function openRunExtensionModal(entry) {
    const backdrop = openModal(
      "<h2>" + esc(entry.name) + "</h2>" +
      '<div class="field"><label>Command (' + esc(entry.command) + ")</label>" +
      '<input type="text" id="ext-cmd-input" placeholder="' + esc(entry.command) + ' ..."></div>' +
      '<div class="actions"><button class="btn" id="ext-run-btn">Execute</button> ' +
      '<button class="btn btn-ghost" id="ext-run-cancel-btn">Cancel</button></div>'
    );
    backdrop.querySelector("#ext-run-cancel-btn").addEventListener("click", () => closeModal(backdrop));
    backdrop.querySelector("#ext-run-btn").addEventListener("click", async () => {
      const cmd = backdrop.querySelector("#ext-cmd-input").value.trim();
      if (!cmd) {
        toast("Command is empty", "err");
        return;
      }
      try {
        await api("/api/run_extension", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ command: cmd }),
        });
        addMessage("sys", "extension executed: " + cmd);
        closeModal(backdrop);
        toast("Command executed", "ok");
      } catch (e) {
        toast("Failed: " + e.message, "err");
      }
    });
  }

  function renderExtensionIcons() {
    const bar = $("icon-bar");
    bar.querySelectorAll(".icon-btn.ext").forEach((el) => el.remove());
    state.extensions.forEach((entry) => {
      const btn = document.createElement("button");
      btn.className = "icon-btn ext";
      btn.title = entry.name + " (" + entry.command + ")";
      btn.textContent = entry.icon;
      btn.addEventListener("click", () => openRunExtensionModal(entry));
      bar.insertBefore(btn, $("plus-btn"));
    });
  }

  /* add/remove extension modal (top-right "+ Extension") */
  async function openExtensionManager() {
    let registered = [];
    try {
      registered = (await api("/api/registered_extensions")).extensions || [];
    } catch (e) {
      /* backend without the endpoint: ignore */
    }
    const rows = registered
      .map(
        (p, i) =>
          '<div class="ext-entry"><div class="info"><div class="name">' + esc(p) +
          '</div></div><button class="btn btn-danger" data-remove="' + i + '">Remove</button></div>'
      )
      .join("");
    const backdrop = openModal(
      "<h2>Extension files</h2>" +
      '<div class="field"><label>Add extension file path(s) (one per line)</label>' +
      '<textarea id="ext-paths" rows="3" style="width:100%;background:var(--bg);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:8px 10px;font-family:inherit;resize:vertical;"></textarea></div>' +
      '<div class="actions"><button class="btn" id="ext-add-file-btn">Add &amp; Restart</button> ' +
      '<button class="btn btn-ghost" id="ext-mgr-close-btn">Close</button></div>' +
      (rows ? '<div style="margin-top:14px;"><div style="font-size:12px;color:var(--text-dim);margin-bottom:6px;">Registered extensions (loaded on every start):</div>' + rows + "</div>" : "")
    );
    backdrop.querySelector("#ext-mgr-close-btn").addEventListener("click", () => closeModal(backdrop));
    backdrop.querySelector("#ext-add-file-btn").addEventListener("click", async () => {
      const raw = backdrop.querySelector("#ext-paths").value.trim();
      const paths = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
      if (!paths.length) {
        toast("Enter at least one extension file path", "err");
        return;
      }
      try {
        await api("/api/add_extension", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ paths }),
        });
        toast("Extension added, restarting...", "ok");
        setTimeout(() => { location.href = "/"; }, 1500);
      } catch (e) {
        toast("Failed: " + e.message, "err");
      }
    });
    backdrop.querySelectorAll("[data-remove]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const path = registered[Number(btn.dataset.remove)];
        try {
          await api("/api/remove_extension", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ paths: [path] }),
          });
          toast("Extension removed, restarting...", "ok");
          setTimeout(() => { location.href = "/"; }, 1500);
        } catch (e) {
          toast("Failed: " + e.message, "err");
        }
      });
    });
  }

  /* ---------------- init ---------------- */

  async function loadExtensions() {
    try {
      state.extensions = (await api("/api/extensions_ui")).extensions || [];
      renderExtensionIcons();
    } catch (e) {
      /* ignore */
    }
  }

  function init() {
    $("send-btn").addEventListener("click", sendMessage);
    $("input").addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    });
    $("input").addEventListener("input", () => {
      $("input").style.height = "auto";
      $("input").style.height = Math.min($("input").scrollHeight, 160) + "px";
    });
    $("file-btn").addEventListener("click", () => openFileModal(false));
    $("folder-btn").addEventListener("click", () => openFileModal(true));
    $("plus-btn").addEventListener("click", openAddProtocolModal);
    $("reload-btn").addEventListener("click", async () => {
      try {
        if (MODE === "client") {
          await api("/api/sync_clients", { method: "POST" });
        }
        await refreshStatus();
        toast("Instance list refreshed", "ok");
      } catch (e) {
        toast("Reload failed: " + e.message, "err");
      }
    });
    $("add-ext-btn").addEventListener("click", openExtensionManager);
    selectTarget("server");
    loadExtensions();
    refreshStatus();
    refreshEvents();
    setInterval(refreshStatus, 2000);
    setInterval(refreshEvents, 2000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
