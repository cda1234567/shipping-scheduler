import { showToast } from "./api.js";

let _desktopState = null;
let _browserDownloadState = null;
let _initialized = false;
let _stateHydrationStarted = false;
let _stateHydrationStatus = "idle";
let _browserStateHydrationStarted = false;
const BROWSER_DOWNLOAD_DB_NAME = "shipping-scheduler-browser-downloads";
const BROWSER_DOWNLOAD_DB_VERSION = 1;
const BROWSER_DOWNLOAD_STORE = "handles";
const BROWSER_DOWNLOAD_KEY = "preferred-download-directory";

function getDesktopApi() {
  const api = window.pywebview?.api;
  if (!api) return null;
  const requiredMethods = [
    "get_state",
    "set_autostart",
    "set_dark_mode",
    "choose_download_directory",
    "minimize_window",
    "open_in_browser",
    "quit_app",
    "download_from_app",
    "reload_app",
  ];
  return requiredMethods.every(name => typeof api[name] === "function") ? api : null;
}

function hasDesktopApi() {
  return Boolean(getDesktopApi());
}

async function waitForDesktopApi(timeoutMs = 8000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const api = getDesktopApi();
    if (api) return api;
    await new Promise(resolve => window.setTimeout(resolve, 120));
  }
  return null;
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

function supportsBrowserSavePicker() {
  return typeof window.showSaveFilePicker === "function" && window.isSecureContext;
}

function supportsBrowserDirectoryPicker() {
  return typeof window.showDirectoryPicker === "function" && window.isSecureContext;
}

function createDefaultBrowserDownloadState() {
  return {
    supported: supportsBrowserDirectoryPicker(),
    save_picker_supported: supportsBrowserSavePicker(),
    secure_context: Boolean(window.isSecureContext),
    download_directory_set: false,
    download_directory: "",
    permission_state: "prompt",
    handle: null,
  };
}

function buildPickerTypes(filename, contentType = "") {
  const safeName = normalizeDownloadText(filename) || "download.bin";
  const ext = safeName.includes(".") ? `.${safeName.split(".").pop().toLowerCase()}` : "";
  const normalizedType = normalizeDownloadText(contentType).split(";")[0].trim().toLowerCase();
  const typeMap = {
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xlsm": "application/vnd.ms-excel.sheet.macroEnabled.12",
    ".xls": "application/vnd.ms-excel",
    ".zip": "application/zip",
    ".csv": "text/csv",
    ".json": "application/json",
    ".txt": "text/plain",
    ".db": "application/octet-stream",
    ".sqlite": "application/octet-stream",
  };
  const resolvedType = normalizedType || typeMap[ext] || "application/octet-stream";
  const descriptionMap = {
    ".xlsx": "Excel 檔案",
    ".xlsm": "Excel 巨集檔",
    ".xls": "Excel 97-2003 檔",
    ".zip": "ZIP 壓縮檔",
    ".csv": "CSV 檔案",
    ".json": "JSON 檔案",
    ".txt": "文字檔",
    ".db": "資料庫檔案",
    ".sqlite": "SQLite 資料庫",
  };
  const acceptExt = ext || ".bin";
  return [{
    description: descriptionMap[ext] || "下載檔案",
    accept: {
      [resolvedType]: [acceptExt],
    },
  }];
}

async function saveBlobWithBrowserPicker(blob, filename, contentType = "") {
  let fileHandle = null;
  try {
    fileHandle = await window.showSaveFilePicker({
      suggestedName: filename || "download.bin",
      types: buildPickerTypes(filename, contentType),
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("已取消選擇下載位置");
    }
    throw error;
  }

  const writable = await fileHandle.createWritable();
  await writable.write(blob);
  await writable.close();

  return {
    ok: true,
    filename: fileHandle.name || filename || "download.bin",
    path: fileHandle.name || filename || "download.bin",
    directory: "",
    saved_with_picker: true,
  };
}

function openBrowserDownloadDb() {
  return new Promise((resolve, reject) => {
    if (!("indexedDB" in window)) {
      reject(new Error("瀏覽器不支援 IndexedDB"));
      return;
    }
    const request = window.indexedDB.open(BROWSER_DOWNLOAD_DB_NAME, BROWSER_DOWNLOAD_DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(BROWSER_DOWNLOAD_STORE)) {
        db.createObjectStore(BROWSER_DOWNLOAD_STORE);
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("開啟瀏覽器下載設定失敗"));
  });
}

async function loadBrowserDownloadDirectoryHandle() {
  if (!supportsBrowserDirectoryPicker()) return null;
  try {
    const db = await openBrowserDownloadDb();
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(BROWSER_DOWNLOAD_STORE, "readonly");
      const store = tx.objectStore(BROWSER_DOWNLOAD_STORE);
      const request = store.get(BROWSER_DOWNLOAD_KEY);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error || new Error("讀取網頁版下載資料夾失敗"));
      tx.oncomplete = () => db.close();
      tx.onerror = () => db.close();
      tx.onabort = () => db.close();
    });
  } catch (error) {
    console.warn("loadBrowserDownloadDirectoryHandle failed", error);
    return null;
  }
}

async function saveBrowserDownloadDirectoryHandle(handle) {
  if (!supportsBrowserDirectoryPicker()) return false;
  try {
    const db = await openBrowserDownloadDb();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(BROWSER_DOWNLOAD_STORE, "readwrite");
      const store = tx.objectStore(BROWSER_DOWNLOAD_STORE);
      const request = store.put(handle, BROWSER_DOWNLOAD_KEY);
      request.onsuccess = () => resolve(true);
      request.onerror = () => reject(request.error || new Error("儲存網頁版下載資料夾失敗"));
      tx.oncomplete = () => db.close();
      tx.onerror = () => db.close();
      tx.onabort = () => db.close();
    });
    return true;
  } catch (error) {
    console.warn("saveBrowserDownloadDirectoryHandle failed", error);
    return false;
  }
}

async function clearBrowserDownloadDirectoryHandle() {
  if (!supportsBrowserDirectoryPicker()) return false;
  try {
    const db = await openBrowserDownloadDb();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(BROWSER_DOWNLOAD_STORE, "readwrite");
      const store = tx.objectStore(BROWSER_DOWNLOAD_STORE);
      const request = store.delete(BROWSER_DOWNLOAD_KEY);
      request.onsuccess = () => resolve(true);
      request.onerror = () => reject(request.error || new Error("清除網頁版下載資料夾失敗"));
      tx.oncomplete = () => db.close();
      tx.onerror = () => db.close();
      tx.onabort = () => db.close();
    });
    return true;
  } catch (error) {
    console.warn("clearBrowserDownloadDirectoryHandle failed", error);
    return false;
  }
}

async function getBrowserHandlePermissionState(handle) {
  if (!handle) return "prompt";
  if (typeof handle.queryPermission !== "function") return "granted";
  try {
    return await handle.queryPermission({ mode: "readwrite" });
  } catch (_) {
    return "prompt";
  }
}

async function ensureBrowserHandlePermission(handle) {
  if (!handle) return false;
  let permission = await getBrowserHandlePermissionState(handle);
  if (permission === "granted") return true;
  if (typeof handle.requestPermission !== "function") return false;
  try {
    permission = await handle.requestPermission({ mode: "readwrite" });
  } catch (_) {
    return false;
  }
  return permission === "granted";
}

async function hydrateBrowserDownloadState(force = false) {
  if (_browserStateHydrationStarted && !force) return;
  _browserStateHydrationStarted = true;
  const state = createDefaultBrowserDownloadState();
  if (supportsBrowserDirectoryPicker()) {
    const handle = await loadBrowserDownloadDirectoryHandle();
    if (handle) {
      state.handle = handle;
      state.download_directory_set = true;
      state.download_directory = String(handle.name || "").trim();
      state.permission_state = await getBrowserHandlePermissionState(handle);
    }
  }
  _browserDownloadState = state;
  renderDesktopState();
}

async function findAvailableFilenameInDirectory(directoryHandle, filename) {
  const safeName = normalizeDownloadText(filename) || "download.bin";
  const dotIndex = safeName.lastIndexOf(".");
  const stem = dotIndex > 0 ? safeName.slice(0, dotIndex) : safeName;
  const suffix = dotIndex > 0 ? safeName.slice(dotIndex) : "";
  let candidate = safeName;
  let counter = 1;
  while (true) {
    try {
      await directoryHandle.getFileHandle(candidate);
      candidate = `${stem}_${counter}${suffix}`;
      counter += 1;
    } catch (error) {
      if (error?.name === "NotFoundError") {
        return candidate;
      }
      throw error;
    }
  }
}

async function saveBlobWithConfiguredBrowserDirectory(blob, filename) {
  const handle = _browserDownloadState?.handle;
  if (!handle) return null;

  const granted = await ensureBrowserHandlePermission(handle);
  if (!granted) {
    await clearBrowserDownloadDirectoryHandle();
    _browserDownloadState = createDefaultBrowserDownloadState();
    renderDesktopState();
    showToast("網頁版下載資料夾權限已失效，這次改為重新選擇下載位置");
    return null;
  }

  const availableName = await findAvailableFilenameInDirectory(handle, filename || "download.bin");
  const fileHandle = await handle.getFileHandle(availableName, { create: true });
  const writable = await fileHandle.createWritable();
  await writable.write(blob);
  await writable.close();

  _browserDownloadState = {
    ...(_browserDownloadState || createDefaultBrowserDownloadState()),
    handle,
    download_directory_set: true,
    download_directory: String(handle.name || "").trim(),
    permission_state: "granted",
  };
  renderDesktopState();

  return {
    ok: true,
    filename: availableName,
    path: availableName,
    directory: String(handle.name || "").trim(),
    saved_with_browser_directory: true,
  };
}

function applyDesktopTheme() {
  document.body.classList.toggle("desktop-dark", Boolean(_desktopState?.dark_mode_enabled));
}

async function readDesktopState() {
  const api = await waitForDesktopApi();
  if (!api) return null;
  _desktopState = await api.get_state();
  applyDesktopTheme();
  return _desktopState;
}

async function hydrateDesktopState(force = false) {
  if (_stateHydrationStarted && !force) return;
  _stateHydrationStarted = true;
  _stateHydrationStatus = "loading";
  renderDesktopState();
  try {
    await readDesktopState();
    _stateHydrationStatus = _desktopState ? "ready" : "idle";
    renderDesktopState();
  } catch (error) {
    _stateHydrationStatus = "error";
    _stateHydrationStarted = false;
    console.warn("desktop bridge state hydration failed", error);
    renderDesktopState();
  }
}

function renderDesktopState() {
  const statusEl = document.getElementById("desktop-status");
  const urlEl = document.getElementById("desktop-url");
  const startupEl = document.getElementById("desktop-startup-note");
  const checkbox = document.getElementById("desktop-autostart");
  const folderEl = document.getElementById("desktop-download-folder");
  const folderNoteEl = document.getElementById("desktop-download-note");
  const darkModeEl = document.getElementById("desktop-dark-mode");
  const desktopChooseBtn = document.getElementById("desktop-choose-download-dir");
  const desktopClearBtn = document.getElementById("desktop-clear-download-dir");
  const desktopMinimizeBtn = document.getElementById("desktop-minimize");
  const desktopOpenBrowserBtn = document.getElementById("desktop-open-browser");
  const desktopQuitBtn = document.getElementById("desktop-quit");
  const modalTitle = document.getElementById("desktop-modal-title");
  const modalSubtitle = document.getElementById("desktop-modal-subtitle");
  const controlBtn = document.getElementById("btn-desktop-controls");
  if (!statusEl || !urlEl || !startupEl || !checkbox || !folderEl || !folderNoteEl || !darkModeEl || !desktopChooseBtn || !desktopClearBtn || !desktopMinimizeBtn || !desktopOpenBrowserBtn || !desktopQuitBtn || !modalTitle || !modalSubtitle || !controlBtn) return;

  const desktopAvailable = hasDesktopApi();
  controlBtn.style.display = "inline-flex";
  controlBtn.textContent = "下載設定";
  controlBtn.title = "下載設定";
  modalTitle.textContent = "下載設定";
  modalSubtitle.textContent = desktopAvailable
    ? "下載位置會依目前模式套用；桌面版另外支援本機服務與開機自啟。"
    : "網頁版下載會交給瀏覽器處理；如要變更位置，請使用瀏覽器本身的下載設定。";
  desktopMinimizeBtn.disabled = !desktopAvailable;
  desktopOpenBrowserBtn.disabled = !desktopAvailable;
  desktopQuitBtn.disabled = !desktopAvailable;
  desktopChooseBtn.style.display = desktopAvailable ? "inline-flex" : "none";
  desktopClearBtn.style.display = desktopAvailable ? "inline-flex" : "none";

  if (!_desktopState) {
    statusEl.textContent = desktopAvailable
      ? (_stateHydrationStatus === "loading" ? "桌面版連線中..." : "桌面版未連線")
      : "網頁版模式";
    urlEl.textContent = desktopAvailable ? "" : window.location.origin;
    startupEl.textContent = desktopAvailable
      ? (_stateHydrationStatus === "error" ? "桌面橋接尚未就緒，請稍後再試一次。" : "")
      : "網頁版不提供開機自動啟動";
    folderEl.textContent = "尚未指定下載資料夾";
    folderNoteEl.textContent = desktopAvailable
      ? "桌面版下載完成後，會存進這個資料夾。"
      : "目前會依瀏覽器能力決定下載方式。";
    checkbox.checked = false;
    checkbox.disabled = true;
    darkModeEl.checked = false;
    darkModeEl.disabled = true;
  } else {
    statusEl.textContent = _desktopState.remote_server
      ? "桌面版已連到遠端服務"
      : _desktopState.server_started_here
        ? "桌面版已啟動本機服務"
        : "桌面版已連到既有本機服務";
    urlEl.textContent = _desktopState.app_url || "";
    folderEl.textContent = _desktopState.download_directory_set
      ? (_desktopState.download_directory || "尚未指定下載資料夾")
      : "尚未指定，首次下載時會詢問資料夾";
    folderNoteEl.textContent = _desktopState.download_directory_set
      ? "桌面版下載會直接存進這個資料夾。"
      : "桌面版下載時，若還沒設定資料夾，會先詢問一次。";
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
  }

  if (!desktopAvailable) {
    folderEl.textContent = "使用瀏覽器預設下載位置";
    folderNoteEl.textContent = "如要每次自行選位置，請在瀏覽器開啟「下載前一律詢問儲存位置」。";
  } else {
    desktopChooseBtn.disabled = false;
    desktopClearBtn.disabled = true;
  }

  applyDesktopTheme();
}

function openDesktopModal() {
  document.getElementById("desktop-modal").style.display = "flex";
  void hydrateBrowserDownloadState(true);
  void hydrateDesktopState(true);
}

function closeDesktopModal() {
  document.getElementById("desktop-modal").style.display = "none";
}

async function handleChooseBrowserDownloadDirectory() {
  try {
    if (!supportsBrowserDirectoryPicker()) {
      if (!window.isSecureContext) {
        throw new Error("目前是一般 HTTP 網址；瀏覽器只有在 HTTPS 或 localhost 才允許網站選擇下載位置。若要固定下載資料夾，請改用 HTTPS、localhost，或改用桌面版。");
      }
      if (supportsBrowserSavePicker()) {
        throw new Error("目前只能每次下載時自行選位置，還不能固定資料夾");
      }
      throw new Error("目前瀏覽器或連線環境不支援固定下載資料夾");
    }
    const handle = await window.showDirectoryPicker({
      id: "shipping-scheduler-downloads",
      mode: "readwrite",
    });
    const granted = await ensureBrowserHandlePermission(handle);
    if (!granted) {
      throw new Error("未取得這個資料夾的寫入權限");
    }
    const persisted = await saveBrowserDownloadDirectoryHandle(handle);
    _browserDownloadState = {
      ...createDefaultBrowserDownloadState(),
      supported: true,
      save_picker_supported: supportsBrowserSavePicker(),
      secure_context: Boolean(window.isSecureContext),
      handle,
      download_directory_set: true,
      download_directory: String(handle.name || "").trim(),
      permission_state: "granted",
    };
    renderDesktopState();
    showToast(persisted ? "已更新網頁版下載資料夾" : "已更新本次工作階段的網頁版下載資料夾");
  } catch (error) {
    if (error?.name === "AbortError") return;
    showToast("設定網頁版下載資料夾失敗: " + error.message);
  }
}

async function handleClearBrowserDownloadDirectory() {
  await clearBrowserDownloadDirectoryHandle();
  _browserDownloadState = createDefaultBrowserDownloadState();
  renderDesktopState();
  showToast("已清除網頁版下載資料夾設定");
}

async function handleChooseDownloadDirectory() {
  if (hasDesktopApi()) {
    return handleChooseDesktopDownloadDirectory();
  }
  showToast("網頁版下載交由瀏覽器處理；如要每次選位置，請在瀏覽器開啟「下載前一律詢問儲存位置」。");
}

async function handleClearDownloadDirectory() {
  if (hasDesktopApi()) {
    showToast("桌面版目前不支援清除下載資料夾設定");
    return;
  }
  showToast("網頁版沒有系統內的下載資料夾設定可清除。");
}

async function handleAutostartChange(event) {
  const checkbox = event.currentTarget;
  const enabled = checkbox.checked;
  checkbox.disabled = true;
  try {
    const api = await waitForDesktopApi();
    if (!api) throw new Error("桌面版尚未連線完成");
    const nextState = await api.set_autostart(enabled);
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

async function handleChooseDesktopDownloadDirectory() {
  try {
    const api = await waitForDesktopApi();
    if (!api) throw new Error("桌面版尚未連線完成");
    const nextState = await api.choose_download_directory();
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
    const api = await waitForDesktopApi();
    if (!api) throw new Error("桌面版尚未連線完成");
    const nextState = await api.set_dark_mode(enabled);
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
  const api = await waitForDesktopApi();
  if (!api) throw new Error("桌面版尚未連線完成");
  await api.minimize_window();
  closeDesktopModal();
}

async function handleOpenBrowser() {
  const api = await waitForDesktopApi();
  if (!api) throw new Error("桌面版尚未連線完成");
  await api.open_in_browser();
  showToast("已在外部瀏覽器開啟");
}

async function handleQuitDesktop() {
  const confirmed = window.confirm("要結束桌面版嗎？結束後桌面視窗和內建服務都會關閉。");
  if (!confirmed) return;
  const api = await waitForDesktopApi();
  if (!api) throw new Error("桌面版尚未連線完成");
  await api.quit_app();
}

function bindDesktopEvents() {
  if (_initialized) return;
  _initialized = true;
  document.getElementById("btn-desktop-controls").addEventListener("click", openDesktopModal);
  document.getElementById("desktop-close").addEventListener("click", closeDesktopModal);
  document.getElementById("desktop-cancel").addEventListener("click", closeDesktopModal);
  document.getElementById("desktop-minimize").addEventListener("click", handleMinimize);
  document.getElementById("desktop-open-browser").addEventListener("click", handleOpenBrowser);
  document.getElementById("desktop-quit").addEventListener("click", handleQuitDesktop);
  document.getElementById("desktop-autostart").addEventListener("change", handleAutostartChange);
  document.getElementById("desktop-choose-download-dir").addEventListener("click", handleChooseDownloadDirectory);
  document.getElementById("desktop-clear-download-dir").addEventListener("click", handleClearDownloadDirectory);
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
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  window.setTimeout(() => {
    URL.revokeObjectURL(blobUrl);
    anchor.remove();
  }, 1200);
  return { ok: true, filename: outputName, path: outputName, directory: "", browser_download_started: true };
}

export function buildDownloadToastMessage(result, noun = "檔案") {
  const label = normalizeDownloadText(noun) || "檔案";
  const filename = normalizeDownloadText(result?.filename);
  const directory = normalizeDownloadText(result?.directory);
  const path = normalizeDownloadText(result?.path);
  const savedWithPicker = Boolean(result?.saved_with_picker);

  if (filename && directory) {
    return `${label}已下載：${filename}\n儲存位置：${directory}`;
  }
  if (filename && savedWithPicker) {
    return `${label}已下載：${filename}\n儲存位置：你剛剛選擇的位置`;
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

  const api = await waitForDesktopApi();
  if (!api) {
    throw new Error("桌面版尚未連線完成");
  }
  const result = await api.download_from_app({ path, method, body, filename });
  if (result?.cancelled) {
    throw new Error("已取消選擇下載資料夾");
  }
  if (!result?.ok) {
    throw new Error(result?.message || "桌面版下載失敗");
  }
  return result;
}

async function bootDesktopBridge() {
  bindDesktopEvents();
  if (!_browserDownloadState) {
    _browserDownloadState = createDefaultBrowserDownloadState();
    renderDesktopState();
    void hydrateBrowserDownloadState();
  }
  if (!hasDesktopApi()) {
    renderDesktopState();
    return;
  }
  if (_stateHydrationStarted || _desktopState) {
    renderDesktopState();
    return;
  }
  _stateHydrationStatus = "loading";
  renderDesktopState();
  window.setTimeout(() => {
    void hydrateDesktopState();
  }, 300);
}

export async function initDesktopBridge() {
  await bootDesktopBridge();
  if (hasDesktopApi()) {
    return;
  }

  window.addEventListener("pywebviewready", () => {
    bootDesktopBridge().catch(error => {
      showToast("桌面版初始化失敗: " + error.message);
    });
  }, { once: true });

  let attempts = 0;
  const pollTimer = window.setInterval(() => {
    attempts += 1;
    if (hasDesktopApi()) {
      window.clearInterval(pollTimer);
      bootDesktopBridge().catch(error => {
        showToast("桌面版初始化失敗: " + error.message);
      });
      return;
    }
    if (attempts >= 50) {
      window.clearInterval(pollTimer);
    }
  }, 120);
}
