import { apiFetch, apiJson, apiPatch, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";

// 鈞洋 後段加工對應管理（缺料採購頁籤內的可收合區塊）：
// 已確認且啟用的對應才會在缺料採購自動帶出加工行；自動建議的候選為「待確認」，確認後才生效。

const STATUS_LABELS = { confirmed: "已確認", pending: "待確認", rejected: "已駁回" };
const SOURCE_LABELS = { seed: "預設", auto_bom: "BOM 建議", auto_po: "採購單建議", manual: "手動" };

let _links = [];
let _showRejected = false;
let _initialized = false;
let _busy = false;

function el(id) { return document.getElementById(id); }

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

function render() {
  const box = el("pp-links-table");
  if (!box) return;
  const rows = _links.filter(l => _showRejected || l.status !== "rejected");
  const pending = _links.filter(l => l.status === "pending").length;
  const summary = el("pp-links-summary");
  if (summary) summary.textContent = `${rows.length} 筆${pending ? `，待確認 ${pending} 筆` : ""}`;
  box.innerHTML = rows.length ? `<table class="erp-po-table"><thead><tr>
      <th>狀態</th><th>基礎件（鈞洋）</th><th>加工件</th><th class="num">比例</th><th>啟用</th><th>依據</th><th>備註</th><th></th>
    </tr></thead><tbody>${rows.map(l => `<tr data-id="${l.id}" class="${l.status === "pending" ? "pp-row-pending" : ""}">
      <td>${esc(STATUS_LABELS[l.status] || l.status)}<div class="erp-po-hint">${esc(SOURCE_LABELS[l.source] || l.source)}</div></td>
      <td class="mono">${esc(l.base_part)}</td>
      <td><input type="text" class="mono" data-f="process_part" value="${esc(l.process_part)}" autocomplete="off"></td>
      <td><input type="number" class="num" data-f="ratio" value="${esc(l.ratio)}" min="0" step="any"></td>
      <td><input type="checkbox" data-f="enabled" ${l.enabled ? "checked" : ""} ${l.status === "confirmed" ? "" : "disabled"}></td>
      <td class="erp-po-hint pp-evidence">${esc(l.evidence)}</td>
      <td><input type="text" data-f="note" value="${esc(l.note)}" autocomplete="off"></td>
      <td class="pp-actions">
        ${l.status !== "confirmed" ? '<button class="btn btn-primary btn-sm" data-act="confirm" type="button">確認</button>' : ""}
        ${l.status === "pending" ? '<button class="btn btn-secondary btn-sm" data-act="reject" type="button">駁回</button>' : ""}
        <button class="btn btn-secondary btn-sm" data-act="save" type="button">儲存</button>
        <button class="btn btn-danger btn-sm" data-act="delete" type="button">刪除</button>
      </td></tr>`).join("")}</tbody></table>` : '<div class="erp-po-hint">尚無對應</div>';
}

export async function loadPostprocessLinks() {
  try {
    const data = await apiJson(`/api/postprocess/links?include_rejected=${_showRejected ? "true" : "false"}`);
    _links = data.links || [];
  } catch (error) {
    _links = [];
    showToast(`讀取後段加工對應失敗：${error.message}`, { tone: "error" });
  }
  render();
}

async function act(fn, okText) {
  if (_busy || !requireEdit()) return;
  _busy = true;
  try {
    await fn();
    if (okText) showToast(okText, { tone: "success" });
    await loadPostprocessLinks();
  } catch (error) {
    showToast(error.message, { tone: "error" });
  } finally {
    _busy = false;
  }
}

function rowValues(tr) {
  const get = f => tr.querySelector(`[data-f="${f}"]`);
  return {
    process_part: get("process_part").value.trim(),
    ratio: Number(get("ratio").value),
    enabled: get("enabled").checked,
    note: get("note").value,
  };
}

async function onTableClick(event) {
  const btn = event.target.closest("[data-act]");
  const tr = event.target.closest("tr[data-id]");
  if (!btn || !tr) return;
  const id = Number(tr.dataset.id);
  const kind = btn.dataset.act;
  if (kind === "save") {
    await act(() => apiPatch(`/api/postprocess/links/${id}`, rowValues(tr)), "已儲存");
  } else if (kind === "confirm") {
    await act(async () => {
      await apiPatch(`/api/postprocess/links/${id}`, rowValues(tr)); // 先存使用者在列上改的比例 / 加工件
      await apiPost(`/api/postprocess/links/${id}/confirm`, {});
    }, "已確認，之後缺料採購會自動帶出加工行");
  } else if (kind === "reject") {
    await act(() => apiPost(`/api/postprocess/links/${id}/reject`, {}), "已駁回");
  } else if (kind === "delete") {
    if (!window.confirm("確定刪除這筆對應嗎？")) return;
    await act(() => apiFetch(`/api/postprocess/links/${id}`, { method: "DELETE" }), "已刪除");
  }
}

async function onAdd() {
  const base = el("pp-add-base")?.value.trim();
  const process = el("pp-add-process")?.value.trim();
  const ratio = Number(el("pp-add-ratio")?.value || 1);
  if (!base || !process) { showToast("請填基礎件與加工件料號", { tone: "error" }); return; }
  await act(async () => {
    await apiPost("/api/postprocess/links", { base_part: base, process_part: process, ratio });
    el("pp-add-base").value = "";
    el("pp-add-process").value = "";
    el("pp-add-ratio").value = "1";
  }, "已新增");
}

async function onSuggest() {
  if (_busy || !requireEdit()) return;
  _busy = true;
  const btn = el("btn-pp-suggest");
  if (btn) btn.disabled = true;
  try {
    const r = await apiPost("/api/postprocess/suggest", { use_erp: true });
    (r.warnings || []).forEach(w => showToast(w, { tone: "error" }));
    showToast(`新增 ${r.created_count} 筆待確認候選`, { tone: "success" });
    await loadPostprocessLinks();
  } catch (error) {
    showToast(`自動建議失敗：${error.message}`, { tone: "error" });
  } finally {
    _busy = false;
    if (btn) btn.disabled = false;
  }
}

export function initPostprocessLinks() {
  if (_initialized) return;
  _initialized = true;
  const panel = el("pp-links-panel");
  panel?.addEventListener("toggle", () => { if (panel.open) void loadPostprocessLinks(); });
  el("pp-links-table")?.addEventListener("click", ev => { void onTableClick(ev); });
  el("btn-pp-add")?.addEventListener("click", () => { void onAdd(); });
  el("btn-pp-suggest")?.addEventListener("click", () => { void onSuggest(); });
  el("pp-show-rejected")?.addEventListener("change", ev => {
    _showRejected = ev.target.checked;
    void loadPostprocessLinks();
  });
}
