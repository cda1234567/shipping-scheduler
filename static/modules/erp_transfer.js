// 一鍵調撥分頁（倉庫 → 一鍵調撥）。寫入公司只由伺服器環境設定決定，瀏覽器不傳 target。
import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";
import { INTRO, batchState, confirmText, renderDetails, renderHistory, renderPicker, resultHtml, submitLabel } from "/static/modules/transfer_view.js";

const PREFIX = "/api/shortage-po/transfer";

let _initialized = false;
let _config = null;
let _data = null; // 最近一次預覽
let _clientToken = "";
let _checked = new Set(); // 右邊勾到的批次 key；預設不勾
let _loading = false;
let _posting = false;

function el(id) { return document.getElementById(id); }

function newToken() {
  return globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`;
}

/** 只送右邊勾到、且還可開的批次（照預覽順序）。 */
function checkedKeys() {
  if (!_data) return [];
  return _data.batches.filter(b => _checked.has(b.key) && batchState(b) === "ready").map(b => b.key);
}

function renderDetail() {
  const box = el("transfer-detail");
  if (box && _data) box.innerHTML = renderDetails(_data, _checked);
}

function codeOf(key) {
  return _data?.batches.find(b => b.key === key)?.code || key;
}

function updateSubmit() {
  const button = el("btn-transfer-submit");
  if (!button) return;
  const count = _data ? checkedKeys().length : 0;
  button.textContent = submitLabel(count);
  button.disabled = _posting || _loading || !count || !_config?.available;
}

/** 狀態訊息放左邊明細區，右邊清單與警告清空。 */
function setBusy(html) {
  const detail = el("transfer-detail");
  if (detail) detail.innerHTML = `<p class="transfer-empty">${html}</p>`;
  for (const id of ["transfer-batches", "transfer-warnings"]) {
    const box = el(id);
    if (box) box.innerHTML = "";
  }
}

async function loadConfig() {
  _config = await apiJson(`${PREFIX}/config`);
  const badge = el("transfer-target");
  if (badge) {
    badge.textContent = _config.available ? `寫入：${_config.label}` : `無法寫入：${_config.reason}`;
    badge.classList.toggle("transfer-target-prod", !!_config.is_production);
  }
  const shortcut = el("btn-shortage-transfer");
  if (shortcut) shortcut.title = _config.available ? `寫入 ${_config.label}` : _config.reason;
  return _config;
}

async function loadHistory() {
  const box = el("transfer-history");
  if (!box) return;
  try {
    const { records } = await apiJson(`${PREFIX}/history`);
    box.innerHTML = renderHistory(records);
  } catch (error) {
    box.textContent = error.message;
  }
}

/** 讀取預覽（只讀）；每次預覽換一個送出識別碼。 */
export async function refreshTransfer() {
  if (_loading || _posting) return;
  if (!isEditAuthenticated()) {
    _data = null;
    setBusy(`開調撥單要先登入編輯。 <button type="button" class="btn btn-sm" id="btn-transfer-login">登入編輯</button>`);
    el("btn-transfer-login").onclick = () => openEditAuthModal();
    updateSubmit();
    return;
  }
  _loading = true;
  _data = null;
  _checked = new Set();
  updateSubmit();
  setBusy("正在整理調撥單…");
  try {
    await loadConfig();
    if (!_config.available) throw new Error(_config.reason);
    _data = await apiPost(`${PREFIX}/preview`);
    _clientToken = newToken();
    el("transfer-warnings").innerHTML = (_data.warnings || []).map(w => `<p class="transfer-warning">${esc(w)}</p>`).join("");
    el("transfer-batches").innerHTML = renderPicker(_data, _checked);
    renderDetail();
  } catch (error) {
    setBusy(`讀不到調撥資料：${esc(error.message)}`);
  } finally {
    _loading = false;
    updateSubmit();
  }
}

async function submit() {
  const keys = checkedKeys();
  if (!_data || !keys.length || _posting) return;
  if (!globalThis.confirm(confirmText(_config.label, keys.map(codeOf)))) return;
  _posting = true;
  updateSubmit();
  el("btn-transfer-refresh").disabled = true;
  document.querySelectorAll("#transfer-batches input").forEach(input => { input.disabled = true; });
  const result = el("transfer-result");
  result.hidden = false;
  result.textContent = "正在寫入正航，請勿關閉或重新整理頁面…";
  try {
    const response = await apiPost(`${PREFIX}/create`, { batches: keys, client_token: _clientToken, preview_token: _data.preview_token });
    result.innerHTML = resultHtml(response, codeOf);
    const ok = response.results.filter(r => r.ok).length;
    if (ok) showToast(`已開 ${ok} 張調撥單`);
  } catch (error) {
    result.textContent = `沒有寫入：${error.message}`;
  } finally {
    _posting = false;
    el("btn-transfer-refresh").disabled = false;
  }
  // 寫完一定重新預覽：已開的批次會變成「已開 RM-…」，不能拿舊預覽再送。
  await refreshTransfer();
  await loadHistory();
}

async function recheck(button) {
  button.disabled = true;
  try {
    const rec = await apiPost(`${PREFIX}/recheck/${button.dataset.recheck}`);
    showToast(rec.status === "deleted" ? "正航單據已刪除，已更新紀錄" : "正航單據還在");
    await loadHistory();
    if (rec.status === "deleted") await refreshTransfer();
  } catch (error) {
    showToast(error.message);
    button.disabled = false;
  }
}

export function initTransfer() {
  if (_initialized) return;
  _initialized = true;
  const intro = el("transfer-intro");
  if (intro) intro.textContent = INTRO;
  el("btn-transfer-refresh")?.addEventListener("click", () => { void refreshTransfer(); });
  el("btn-transfer-submit")?.addEventListener("click", () => { void submit(); });
  el("transfer-batches")?.addEventListener("change", event => {
    const input = event.target.closest("[data-transfer-batch]");
    if (!input) return;
    if (input.checked) _checked.add(input.dataset.transferBatch);
    else _checked.delete(input.dataset.transferBatch);
    renderDetail();
    updateSubmit();
  });
  el("transfer-history")?.addEventListener("click", event => {
    const button = event.target.closest("[data-recheck]");
    if (button) void recheck(button);
  });
  // 缺料採購裡的捷徑：直接切到倉庫 → 一鍵調撥
  el("btn-shortage-transfer")?.addEventListener("click", () => {
    document.querySelector('.tab-btn[data-tab="transfer-tab"]')?.click();
  });
  loadConfig().catch(() => {});
}

export async function onTransferTabActivated() {
  initTransfer();
  await Promise.all([refreshTransfer(), loadHistory()]);
}
