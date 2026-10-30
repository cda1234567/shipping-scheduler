import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";

let _preview = null; // /api/part-conversion/preview 回應
let _previewKey = "";
let _busy = false;
let _initialized = false;

function el(id) { return document.getElementById(id); }

function fmtQty(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toLocaleString("zh-TW", { maximumFractionDigits: 4 }) : "—";
}

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

function inputs() {
  return { a: el("pc-part-a").value.trim().toUpperCase(), b: el("pc-part-b").value.trim().toUpperCase() };
}

function updateButtons() {
  const { a, b } = inputs();
  const ready = _preview && _previewKey === `${a}|${b}` && _preview.count > 0;
  el("btn-pc-preview").disabled = _busy || !a || !b;
  el("btn-pc-confirm").disabled = _busy || !ready;
}

function renderPreview() {
  const box = el("pc-preview");
  if (!_preview) { box.hidden = true; return; }
  const p = _preview;
  const stockA = p.stock[p.part_a] || {};
  const stockB = p.stock[p.part_b] || {};
  const warnings = (p.warnings || []).map(w => `<div class="erp-po-warn">${esc(w)}</div>`).join("");
  const notes = [];
  if (p.merge_count) notes.push(`${p.merge_count} 份 BOM 已含 ${esc(p.part_b)}：視為互換料，保留 ${esc(p.part_b)} 原本的用量與需求量、刪除 ${esc(p.part_a)} 那列（用量不相加）`);
  if (p.db_only_count) notes.push(`${p.db_only_count} 份 BOM 沒有可編輯的 xlsx 檔（.xls 或檔案遺失），只更新資料庫`);
  const rows = p.affected.map(x => `<tr>
    <td>${esc(x.filename)}</td>
    <td>${esc(x.model)}</td>
    <td>${esc(x.group_model)}</td>
    <td class="num">${fmtQty(x.qty_per_board)}</td>
    <td class="num">${fmtQty(x.needed_qty)}</td>
    <td class="num">${fmtQty(x.needed_qty_after ?? x.needed_qty)}</td>
    <td>${x.merge ? `<span class="pc-flag-merge${x.qty_mismatch ? " pc-flag-mismatch" : ""}">互換料，保留 ${esc(p.part_b)} 用量 ${fmtQty(x.b_qty_per_board_before)}（刪除 ${esc(p.part_a)}）</span>${x.qty_mismatch ? `<div class="erp-po-warn">用量不同：${esc(p.part_a)} ${fmtQty(x.qty_per_board)}、${esc(p.part_b)} ${fmtQty(x.b_qty_per_board_before)}</div>` : ""}` : "改料號"}</td>
    <td>${x.file_mode === "xlsx" ? "xlsx + 資料庫" : "僅資料庫"}</td>
  </tr>`).join("");
  box.hidden = false;
  box.innerHTML = `
    <div class="erp-po-preview-head"><strong>${esc(p.part_a)} → ${esc(p.part_b)}</strong>：共 ${p.count} 份 BOM 使用 ${esc(p.part_a)}</div>
    ${warnings}
    ${notes.map(n => `<div class="erp-po-hint">${n}</div>`).join("")}
    <div class="erp-po-hint">庫存：${esc(p.part_a)} ST ${fmtQty(stockA.st_stock)}｜${esc(p.part_b)} ST ${fmtQty(stockB.st_stock)}｜轉換後 ${esc(p.part_b)} 有效庫存 ${fmtQty(p.effective_stock_b)}
      （主檔快照：${esc(p.part_a)} ${stockA.main_snapshot_stock == null ? "—" : fmtQty(stockA.main_snapshot_stock)}、${esc(p.part_b)} ${stockB.main_snapshot_stock == null ? "—" : fmtQty(stockB.main_snapshot_stock)}，主檔不會被修改）</div>
    ${p.count ? `<div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>BOM 檔案</th><th>機種</th><th>群組</th><th class="num">A 每板用量</th><th class="num">A 需求量</th><th class="num">轉換後 B 需求量</th><th>處理</th><th>寫入</th>
    </tr></thead><tbody>${rows}</tbody></table></div>` : ""}`;
}

async function runPreview() {
  const { a, b } = inputs();
  if (!a || !b) return;
  _busy = true; updateButtons();
  try {
    _preview = await apiJson(`/api/part-conversion/preview?part_a=${encodeURIComponent(a)}&part_b=${encodeURIComponent(b)}`);
    _previewKey = `${a}|${b}`;
  } catch (error) {
    _preview = null;
    showToast(error.message, { tone: "error" });
  } finally {
    _busy = false;
    renderPreview();
    updateButtons();
  }
}

async function runConvert() {
  if (!requireEdit() || !_preview) return;
  const p = _preview;
  if (!confirm(`確定要把 ${p.count} 份 BOM 裡的 ${p.part_a} 全部換成 ${p.part_b} 嗎？\n轉換後 ${p.part_a} 會停用，可在下方歷史紀錄還原。`)) return;
  _busy = true; updateButtons();
  try {
    const r = await apiPost("/api/part-conversion/convert", { part_a: p.part_a, part_b: p.part_b });
    showToast(`已轉換 ${r.count} 份 BOM（${p.part_a} → ${p.part_b}）`);
    _preview = null;
    renderPreview();
    await loadHistory();
  } catch (error) {
    showToast(`轉換失敗：${error.message}`, { tone: "error" });
  } finally {
    _busy = false;
    updateButtons();
  }
}

async function restoreConversion(id, force = false) {
  if (!requireEdit()) return;
  if (!force && !confirm("確定要還原這次轉換嗎？BOM 會回到轉換前的內容，A 會重新啟用。")) return;
  try {
    const r = await apiPost(`/api/part-conversion/${id}/restore`, { force });
    showToast(`已還原 ${r.restored_boms} 份 BOM（${r.part_a} 重新啟用）${(r.warnings || []).join("；")}`);
    await loadHistory();
  } catch (error) {
    if (!force && /又被修改過/.test(error.message) && confirm(`${error.message}\n\n仍要強制還原嗎？`)) {
      return restoreConversion(id, true);
    }
    showToast(`還原失敗：${error.message}`, { tone: "error" });
  }
}

async function loadHistory() {
  const box = el("pc-history");
  if (!box) return;
  try {
    const data = await apiJson("/api/part-conversion/list");
    const rows = data.conversions || [];
    if (!rows.length) { box.innerHTML = '<div class="erp-po-hint">尚無轉換紀錄</div>'; return; }
    box.innerHTML = `<div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>#</th><th>A → B</th><th>時間</th><th>操作者</th><th class="num">影響 BOM</th><th>狀態</th><th></th>
    </tr></thead><tbody>${rows.map(c => `<tr>
      <td>${c.id}</td>
      <td class="mono">${esc(c.part_a)} → ${esc(c.part_b)}</td>
      <td>${esc(String(c.created_at || "").slice(0, 16).replace("T", " "))}</td>
      <td>${esc(c.actor)}</td>
      <td class="num" title="${esc((c.affected || []).map(x => x.filename).join("\n"))}">${(c.affected || []).length}</td>
      <td>${c.status === "active" ? "生效中（A 已停用）" : `已還原 ${esc(String(c.restored_at || "").slice(0, 16).replace("T", " "))}`}</td>
      <td>${c.status === "active" ? `<button class="btn btn-secondary btn-sm" data-restore="${c.id}" type="button">還原</button>` : ""}</td>
    </tr>`).join("")}</tbody></table></div>`;
  } catch (error) {
    box.innerHTML = `<div class="erp-po-warn">讀取紀錄失敗：${esc(error.message)}</div>`;
  }
}

export async function initPartConversion() {
  if (_initialized) return;
  _initialized = true;
  ["pc-part-a", "pc-part-b"].forEach(id => el(id)?.addEventListener("input", () => {
    _preview = null; renderPreview(); updateButtons();
  }));
  el("btn-pc-preview")?.addEventListener("click", runPreview);
  el("btn-pc-confirm")?.addEventListener("click", runConvert);
  el("pc-history")?.addEventListener("click", ev => {
    const btn = ev.target.closest("[data-restore]");
    if (btn) void restoreConversion(Number(btn.dataset.restore));
  });
  updateButtons();
}

export async function onPartConversionTabActivated() {
  await loadHistory();
}
