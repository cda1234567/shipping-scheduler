import { apiJson, apiPost, apiPut, showToast, esc } from "./api.js";

let _reloadApp = null;
let _selectedBackupName = "";
let _overview = null;
let _busy = false;
let _initialized = false;

export async function initDbBackup({ reloadApp, autoLoad = false } = {}) {
  _reloadApp = typeof reloadApp === "function" ? reloadApp : null;
  if (_initialized) {
    if (autoLoad) await refreshDbBackupPanel();
    return;
  }

  document.getElementById("btn-run-db-backup")?.addEventListener("click", handleRunBackup);
  document.getElementById("btn-save-db-backup-settings")?.addEventListener("click", handleSaveSettings);
  document.getElementById("btn-db-backup-refresh")?.addEventListener("click", refreshDbBackupPanel);
  document.getElementById("btn-db-backup-restore")?.addEventListener("click", handleRestoreBackup);
  document.getElementById("db-backup-list")?.addEventListener("change", event => {
    const radio = event.target?.closest?.("input[name='db-backup-item']");
    if (!radio) return;
    _selectedBackupName = String(radio.value || "").trim();
    renderBackupList(_overview?.backups || []);
  });

  _initialized = true;
  if (autoLoad) {
    await refreshDbBackupPanel();
  }
}

export async function refreshDbBackupPanel() {
  try {
    const overview = await apiJson("/api/system/db-backups");
    _overview = overview;
    renderBackupSummary(overview);
    renderBackupList(overview.backups || []);
  } catch (error) {
    renderBackupError(error);
  }
}

function setBusy(nextBusy) {
  _busy = Boolean(nextBusy);
  [
    "btn-run-db-backup",
    "btn-save-db-backup-settings",
    "btn-db-backup-refresh",
    "btn-db-backup-restore",
  ].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.disabled = _busy;
  });
}

function renderBackupSummary(overview) {
  const enabledEl = document.getElementById("db-backup-enabled");
  const timeEl = document.getElementById("db-backup-time");
  const keepCountEl = document.getElementById("db-backup-keep-count");
  const statusEl = document.getElementById("db-backup-status");
  const hintEl = document.getElementById("db-backup-hint");

  if (enabledEl) enabledEl.checked = Boolean(overview.enabled);
  if (timeEl) timeEl.value = `${String(overview.hour).padStart(2, "0")}:${String(overview.minute).padStart(2, "0")}`;
  if (keepCountEl) keepCountEl.value = String(overview.keep_count || 14);

  if (statusEl) {
    const lines = [
      overview.enabled
        ? `每日 ${String(overview.hour).padStart(2, "0")}:${String(overview.minute).padStart(2, "0")} 自動備份`
        : "自動備份已停用",
      `保留最近 ${overview.keep_count || 14} 份`,
      overview.last_backup_at
        ? `最近一次：${formatDateTime(overview.last_backup_at)}`
        : "最近一次：尚無備份",
    ];
    if (overview.due_now && overview.enabled) {
      lines.push("本次排程待執行");
    } else if (overview.next_run_at && overview.enabled) {
      lines.push(`下次排程：${formatDateTime(overview.next_run_at)}`);
    }
    if (overview.last_error) {
      lines.push(`上次錯誤：${overview.last_error}`);
    }

    const tone = overview.last_error ? " warn" : overview.last_backup_at ? " ok" : "";
    statusEl.className = `file-status${tone}`;
    statusEl.innerHTML = lines.map(line => `<span>${esc(line)}</span>`).join("<br>");
  }

  if (hintEl) {
    hintEl.textContent = overview.last_restore_at
      ? `最近還原：${formatDateTime(overview.last_restore_at)}`
      : "還原前會先自動建立保底備份";
  }
}

function renderBackupError(error) {
  const statusEl = document.getElementById("db-backup-status");
  const listEl = document.getElementById("db-backup-list");
  const emptyEl = document.getElementById("db-backup-empty");
  const restoreBtn = document.getElementById("btn-db-backup-restore");
  const message = `讀取資料庫備份狀態失敗：${error.message}`;

  if (statusEl) {
    statusEl.className = "file-status warn";
    statusEl.innerHTML = `<span>${esc(message)}</span>`;
  }
  if (listEl) listEl.innerHTML = "";
  if (emptyEl) {
    emptyEl.textContent = message;
    emptyEl.style.display = "block";
  }
  if (restoreBtn) restoreBtn.disabled = true;
}

function renderBackupList(backups) {
  const listEl = document.getElementById("db-backup-list");
  const emptyEl = document.getElementById("db-backup-empty");
  const restoreBtn = document.getElementById("btn-db-backup-restore");
  if (!listEl || !emptyEl) return;

  const items = Array.isArray(backups) ? backups : [];
  const exists = items.some(item => item?.name === _selectedBackupName);
  if (!exists) {
    _selectedBackupName = items[0]?.name || "";
  }

  if (!items.length) {
    listEl.innerHTML = "";
    emptyEl.textContent = "目前還沒有任何資料庫備份。";
    emptyEl.style.display = "block";
    if (restoreBtn) restoreBtn.disabled = true;
    return;
  }

  emptyEl.style.display = "none";
  listEl.innerHTML = items.map(item => {
    const selected = item.name === _selectedBackupName;
    const latest = item.name === _overview?.last_backup_name;
    const restored = item.name === _overview?.last_restore_file;
    const meta = [formatDateTime(item.created_at), formatBytes(item.size_bytes)];
    return `
      <label class="db-backup-item${selected ? " is-selected" : ""}">
        <input type="radio" name="db-backup-item" value="${esc(item.name)}" ${selected ? "checked" : ""}>
        <div class="db-backup-item-body">
          <div class="db-backup-item-head">
            <span class="db-backup-item-name">${esc(item.name)}</span>
            <span class="db-backup-item-badges">
              ${latest ? '<span class="db-backup-badge">最新</span>' : ""}
              ${restored ? '<span class="db-backup-badge db-backup-badge-restore">已還原</span>' : ""}
            </span>
          </div>
          <div class="db-backup-item-meta">${esc(meta.join(" · "))}</div>
          <div class="db-backup-item-path">${esc(item.path)}</div>
        </div>
      </label>
    `;
  }).join("");
  if (restoreBtn) restoreBtn.disabled = !_selectedBackupName || _busy;
}

async function handleRunBackup() {
  if (_busy) return;
  setBusy(true);
  try {
    const result = await apiPost("/api/system/db-backups/run");
    showToast(`資料庫備份完成：${result.backup?.name || ""}`);
    await refreshDbBackupPanel();
  } catch (error) {
    showToast(`建立資料庫備份失敗：${error.message}`);
  } finally {
    setBusy(false);
  }
}

async function handleSaveSettings() {
  if (_busy) return;

  const enabled = Boolean(document.getElementById("db-backup-enabled")?.checked);
  const timeValue = String(document.getElementById("db-backup-time")?.value || "02:00");
  const keepCount = Number(document.getElementById("db-backup-keep-count")?.value || 14);
  const [hourText, minuteText] = timeValue.split(":");
  const hour = Number(hourText);
  const minute = Number(minuteText);

  if (!Number.isInteger(hour) || hour < 0 || hour > 23 || !Number.isInteger(minute) || minute < 0 || minute > 59) {
    showToast("請輸入正確的備份時間");
    return;
  }
  if (!Number.isInteger(keepCount) || keepCount < 1 || keepCount > 365) {
    showToast("保留份數請輸入 1 到 365");
    return;
  }

  setBusy(true);
  try {
    await apiPut("/api/system/db-backups/settings", {
      enabled,
      hour,
      minute,
      keep_count: keepCount,
    });
    showToast("資料庫備份排程已更新");
    await refreshDbBackupPanel();
  } catch (error) {
    showToast(`儲存排程失敗：${error.message}`);
  } finally {
    setBusy(false);
  }
}

async function handleRestoreBackup() {
  if (_busy || !_selectedBackupName) return;

  const confirmed = confirm(
    `確定要還原資料庫備份？\n\n${_selectedBackupName}\n\n系統會先自動備份目前的 system.db，再用這份備份覆蓋目前資料庫。`
  );
  if (!confirmed) return;

  setBusy(true);
  try {
    await apiPost("/api/system/db-backups/restore", { backup_name: _selectedBackupName });
    showToast("資料庫還原完成，系統將重新整理");
    if (_reloadApp) {
      setTimeout(() => { void _reloadApp(); }, 400);
    } else {
      setTimeout(() => window.location.reload(), 400);
    }
  } catch (error) {
    showToast(`還原資料庫備份失敗：${error.message}`);
    setBusy(false);
  }
}

function formatDateTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const normalized = text.includes("T") ? text : text.replace(" ", "T");
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return text.replace("T", " ").slice(0, 16);
  const y = parsed.getFullYear();
  const m = String(parsed.getMonth() + 1).padStart(2, "0");
  const d = String(parsed.getDate()).padStart(2, "0");
  const hh = String(parsed.getHours()).padStart(2, "0");
  const mm = String(parsed.getMinutes()).padStart(2, "0");
  return `${y}/${m}/${d} ${hh}:${mm}`;
}

function formatBytes(value) {
  const size = Number(value || 0);
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}
