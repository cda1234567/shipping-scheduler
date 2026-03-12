import { showToast } from "./api.js";

let _desktopState = null;
let _initialized = false;

function hasDesktopApi() {
  return Boolean(window.pywebview?.api);
}

function parseFilenameFromContentDisposition(headerValue) {
  const text = String(headerValue || "");
  const utfMatch = text.match(/filename\*=UTF-8''([^;]+)/i);
  if (utfMatch) return decodeURIComponent(utfMatch[1].replace(/"/g, ""));
  const plainMatch = text.match(/filename="?([^";]+)"?/i);
  return plainMatch ? plainMatch[1] : "";
}

function normalizeDownloadText(value) {
  return String(value || "").trim();
}

function applyDesktopTheme() {
  document.body.classList.toggle("desktop-dark", Boolean(_desktopState?.dark_mode_enabled));
}

async function readDesktopState() {
  if (!hasDesktopApi()) return null;
  _desktopState = await window.pywebview.api.get_state();
  applyDesktopTheme();
  return _desktopState;
}

function renderDesktopState() {
  const statusEl = document.getElementById("desktop-status");
  const urlEl = document.getElementById("desktop-url");
  const startupEl = document.getElementById("desktop-startup-note");
  const checkbox = document.getElementById("desktop-autostart");
  const folderEl = document.getElementById("desktop-download-folder");
  const darkModeEl = document.getElementById("desktop-dark-mode");
  if (!statusEl || !urlEl || !startupEl || !checkbox || !folderEl || !darkModeEl) return;

  if (!_desktopState) {
    statusEl.textContent = "桌面版未連線";
    urlEl.textContent = "";
    startupEl.textContent = "";
    folderEl.textContent = "尚未指定下載資料夾";
    checkbox.checked = false;
    checkbox.disabled = true;
    darkModeEl.checked = false;
    darkModeEl.disabled = true;
    applyDesktopTheme();
    return;
  }

  statusEl.textContent = _desktopState.server_started_here
    ? "桌面版已啟動本機服務"
    : "桌面版已連到既有服務";
  urlEl.textContent = _desktopState.app_url || "";
  folderEl.textContent = _desktopState.download_directory_set
    ? (_desktopState.download_directory || "尚未指定下載資料夾")
    : "尚未指定，首次下載時會詢問資料夾";
  darkModeEl.checked = Boolean(_desktopState.dark_mode_enabled);
  darkModeEl.disabled = false;
  if (_desktopState.autostart_managed) {
    startupEl.textContent = _desktopState.autostart_enabled
      ? "開機後會自動啟動，並縮小在工作列"
      : "目前不會隨 Windows 開機自動啟動";
    checkbox.disabled = false;
  } else {
    startupEl.textContent = "目前環境不支援自動啟動設定";
    checkbox.disabled = true;
  }
  checkbox.checked = Boolean(_desktopState.autostart_enabled);
  applyDesktopTheme();
}

function openDesktopModal() {
  document.getElementById("desktop-modal").style.display = "flex";
}

function closeDesktopModal() {
  document.getElementById("desktop-modal").style.display = "none";
}

async function handleAutostartChange(event) {
  const checkbox = event.currentTarget;
  const enabled = checkbox.checked;
  checkbox.disabled = true;
  try {
    const nextState = await window.pywebview.api.set_autostart(enabled);
    if (!nextState?.ok && nextState?.message) {
      throw new Error(nextState.message);
    }
    _desktopState = nextState;
    renderDesktopState();
    showToast(enabled ? "已啟用開機自動啟動" : "已關閉開機自動啟動");
  } catch (error) {
    checkbox.checked = !enabled;
    checkbox.disabled = false;
    showToast("自動啟動設定失敗: " + error.message);
  }
}

async function handleChooseDownloadDirectory() {
  try {
    const nextState = await window.pywebview.api.choose_download_directory();
    if (nextState?.cancelled) return;
    if (!nextState?.ok && nextState?.message) {
      throw new Error(nextState.message);
    }
    _desktopState = nextState;
    renderDesktopState();
    showToast("已更新桌面版下載資料夾");
  } catch (error) {
    showToast("設定下載資料夾失敗: " + error.message);
  }
}

async function handleDarkModeChange(event) {
  const checkbox = event.currentTarget;
  const enabled = checkbox.checked;
  checkbox.disabled = true;
  try {
    const nextState = await window.pywebview.api.set_dark_mode(enabled);
    if (!nextState?.ok && nextState?.message) {
      throw new Error(nextState.message);
    }
    _desktopState = nextState;
    renderDesktopState();
    showToast(enabled ? "已切換為黑暗模式" : "已切換為淺色模式");
  } catch (error) {
    checkbox.checked = !enabled;
    checkbox.disabled = false;
    showToast("主題切換失敗: " + error.message);
  }
}

async function handleMinimize() {
  await window.pywebview.api.minimize_window();
  closeDesktopModal();
}

async function handleOpenBrowser() {
  await window.pywebview.api.open_in_browser();
  showToast("已在外部瀏覽器開啟");
}

async function handleQuitDesktop() {
  const confirmed = window.confirm("要結束桌面版嗎？結束後桌面視窗和內建服務都會關閉。");
  if (!confirmed) return;
  await window.pywebview.api.quit_app();
}

function bindDesktopEvents() {
  document.getElementById("btn-desktop-controls").addEventListener("click", openDesktopModal);
  document.getElementById("desktop-close").addEventListener("click", closeDesktopModal);
  document.getElementById("desktop-cancel").addEventListener("click", closeDesktopModal);
  document.getElementById("desktop-minimize").addEventListener("click", handleMinimize);
  document.getElementById("desktop-open-browser").addEventListener("click", handleOpenBrowser);
  document.getElementById("desktop-quit").addEventListener("click", handleQuitDesktop);
  document.getElementById("desktop-autostart").addEventListener("change", handleAutostartChange);
  document.getElementById("desktop-choose-download-dir").addEventListener("click", handleChooseDownloadDirectory);
  document.getElementById("desktop-dark-mode").addEventListener("change", handleDarkModeChange);
  document.getElementById("desktop-modal").addEventListener("click", event => {
    if (event.target.id === "desktop-modal") closeDesktopModal();
  });
}

async function fallbackBrowserDownload({ path, method = "GET", body = null, filename = "" }) {
  const response = await fetch(path, {
    method,
    headers: body == null ? undefined : { "Content-Type": "application/json" },
    body: body == null ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      message = (await response.json()).detail || message;
    } catch (_) {}
    throw new Error(message);
  }
  const blob = await response.blob();
  const headerName = parseFilenameFromContentDisposition(response.headers.get("content-disposition"));
  const outputName = filename || headerName || "download.bin";
  const blobUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = blobUrl;
  anchor.download = outputName;
  anchor.click();
  URL.revokeObjectURL(blobUrl);
  return { ok: true, filename: outputName, path: outputName, directory: "" };
}

export function buildDownloadToastMessage(result, noun = "檔案") {
  const label = normalizeDownloadText(noun) || "檔案";
  const filename = normalizeDownloadText(result?.filename);
  const directory = normalizeDownloadText(result?.directory);
  const path = normalizeDownloadText(result?.path);

  if (filename && directory) {
    return `${label}已下載：${filename}\n儲存位置：${directory}`;
  }
  if (filename && path && path !== filename) {
    return `${label}已下載：${filename}\n儲存位置：${path}`;
  }
  if (filename) {
    return `${label}已下載：${filename}`;
  }
  if (path) {
    return `${label}已下載：${path}`;
  }
  return `${label}已下載`;
}

export function showDownloadToast(result, noun = "檔案") {
  showToast(buildDownloadToastMessage(result, noun));
}

export async function desktopDownload({ path, method = "GET", body = null, filename = "" }) {
  if (!hasDesktopApi()) {
    return fallbackBrowserDownload({ path, method, body, filename });
  }

  const result = await window.pywebview.api.download_from_app({ path, method, body, filename });
  if (result?.cancelled) {
    throw new Error("已取消選擇下載資料夾");
  }
  if (!result?.ok) {
    throw new Error(result?.message || "桌面版下載失敗");
  }
  return result;
}

async function bootDesktopBridge() {
  if (_initialized || !hasDesktopApi()) return;
  _initialized = true;
  bindDesktopEvents();
  document.getElementById("btn-desktop-controls").style.display = "inline-flex";
  await readDesktopState();
  renderDesktopState();
}

export async function initDesktopBridge() {
  if (hasDesktopApi()) {
    await bootDesktopBridge();
    return;
  }

  window.addEventListener("pywebviewready", () => {
    bootDesktopBridge().catch(error => {
      showToast("桌面版初始化失敗: " + error.message);
    });
  }, { once: true });
}
