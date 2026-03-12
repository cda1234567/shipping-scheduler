import { showToast } from "./api.js";

let _desktopState = null;
let _initialized = false;

function hasDesktopApi() {
  return Boolean(window.pywebview?.api);
}

async function readDesktopState() {
  if (!hasDesktopApi()) return null;
  _desktopState = await window.pywebview.api.get_state();
  return _desktopState;
}

function renderDesktopState() {
  const statusEl = document.getElementById("desktop-status");
  const urlEl = document.getElementById("desktop-url");
  const startupEl = document.getElementById("desktop-startup-note");
  const checkbox = document.getElementById("desktop-autostart");
  if (!statusEl || !urlEl || !startupEl || !checkbox) return;

  if (!_desktopState) {
    statusEl.textContent = "桌面版未連線";
    urlEl.textContent = "";
    startupEl.textContent = "";
    checkbox.checked = false;
    checkbox.disabled = true;
    return;
  }

  statusEl.textContent = _desktopState.server_started_here
    ? "桌面版已啟動本機服務"
    : "桌面版已連到既有服務";
  urlEl.textContent = _desktopState.app_url || "";
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
  document.getElementById("desktop-modal").addEventListener("click", event => {
    if (event.target.id === "desktop-modal") closeDesktopModal();
  });
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
