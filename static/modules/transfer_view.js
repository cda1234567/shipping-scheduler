// 一鍵調撥畫面（純函式，不呼叫 API；node 測試可直接 import）。

export const WARE_LABELS = { "001": "一般倉", "002": "客供倉", "015": "庚霖倉" };
export const COLUMNS = ["料號", "品名", "從哪倉", "要撥", "公司倉現有", "本次撥", "短少", "成本"];
export const INTRO = "一個批次開一張調撥單：一般料從一般倉（001）、客供料從客供倉（002）撥到庚霖倉（015）。公司倉不夠時能撥多少撥多少，短少的留到下次。";

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

export function fmtQty(value) {
  return Number(value || 0).toLocaleString("zh-TW", { maximumFractionDigits: 4 });
}

function fmtCost(value) {
  return Number(value || 0).toLocaleString("zh-TW", { maximumFractionDigits: 10 });
}

export function wareLabel(ware) {
  return `${ware} ${WARE_LABELS[ware] || ""}`.trim();
}

/** 批次狀態：done 已開過、empty 沒料可撥、ready 可開。 */
export function batchState(batch) {
  if (batch.already) return "done";
  if (!batch.totals?.transfer) return "empty";
  return "ready";
}

function batchNote(batch) {
  const state = batchState(batch);
  if (state === "done") return `已開 ${esc(batch.already.move_no)}`;
  if (!batch.rows.length) return "沒有要撥的料";
  if (state === "empty") return `公司倉都沒貨，短少 ${fmtQty(batch.totals.short)}`;
  const short = batch.totals.short ? `，短少 ${fmtQty(batch.totals.short)}` : "";
  return `${batch.rows.filter(r => r.transfer > 0).length} 項，撥 ${fmtQty(batch.totals.transfer)}${short}`;
}

function rowHtml(r) {
  // stock_src = 公司3 該來源倉現有；stock_target 是目標公司的同一個來源倉，只在不同時放提示。
  const tip = r.stock_target !== undefined && r.stock_target !== r.stock_src ? ` title="寫入公司的 ${esc(r.ware_out)} 倉現有 ${fmtQty(r.stock_target)}"` : "";
  return `<tr class="${r.transfer > 0 ? "" : "transfer-row-skip"}">
    <td>${esc(r.part)}</td><td>${esc(r.name)}</td><td>${esc(wareLabel(r.ware_out))}</td>
    <td class="num">${fmtQty(r.pending)}</td><td class="num"${tip}>${fmtQty(r.stock_src)}</td>
    <td class="num"><strong>${fmtQty(r.transfer)}</strong></td>
    <td class="num${r.short > 0 ? " transfer-short" : ""}">${r.short > 0 ? fmtQty(r.short) : "—"}</td>
    <td class="num">${fmtCost(r.cost)}</td></tr>`;
}

export function renderBatch(batch, checked = true) {
  const state = batchState(batch);
  const disabled = state !== "ready";
  const table = state === "done" || !batch.rows.length ? "" : `<table class="transfer-table">
      <thead><tr>${COLUMNS.map((c, i) => `<th${i > 2 ? ' class="num"' : ""}>${c}</th>`).join("")}</tr></thead>
      <tbody>${batch.rows.map(rowHtml).join("")}</tbody></table>`;
  return `<section class="transfer-batch transfer-batch-${state}" data-batch-key="${esc(batch.key)}">
    <label class="transfer-batch-head">
      <input type="checkbox" data-transfer-batch="${esc(batch.key)}" ${disabled ? "disabled" : checked ? "checked" : ""}>
      <span class="transfer-batch-title">${esc(batch.code)} · 一張調撥單</span>
      <span class="transfer-batch-note">${batchNote(batch)}</span>
    </label>${table}</section>`;
}

export function renderBatches(data) {
  if (!data.batches.length) return `<p class="transfer-empty">目前沒有要調撥的批次。</p>`;
  return data.batches.map(b => renderBatch(b)).join("");
}

export function submitLabel(count) {
  return count ? `開 ${count} 張調撥單` : "請勾選要開的批次";
}

export function confirmText(label, codes) {
  return `確定在「${label}」開 ${codes.length} 張調撥單？\n${codes.join("、")}\n\n開了之後要到正航才能刪。`;
}

export function resultHtml(response, codeOf) {
  const lines = response.results.map(r => r.ok
    ? `<li class="transfer-ok">${esc(codeOf(r.batch_key))} → ${esc(r.move_no)}${r.warning ? `<div>${esc(r.warning)}</div>` : ""}</li>`
    : `<li class="transfer-fail">${esc(codeOf(r.batch_key))} 沒開成：${esc(r.error)}</li>`);
  if (response.stopped.length) lines.push(`<li class="transfer-fail">前一張失敗，以下沒有開：${response.stopped.map(k => esc(codeOf(k))).join("、")}</li>`);
  return `<ul>${lines.join("")}</ul>`;
}

const STATUS = { ok: "已開", error: "失敗", deleted: "正航已刪" };

export function renderHistory(records) {
  if (!records.length) return "<p>還沒有調撥紀錄。</p>";
  return `<table class="transfer-table"><thead><tr><th>批次</th><th>調撥單號</th><th>公司</th><th>狀態</th><th>時間</th><th></th></tr></thead>
    <tbody>${records.map(r => `<tr><td>${esc(r.batch_code)}</td><td>${esc(r.move_no)}</td><td>${esc(r.target_db)}</td>
      <td>${esc(STATUS[r.status] || r.status)}${r.error ? `：${esc(r.error)}` : ""}</td><td>${esc(r.created_at)}</td>
      <td>${r.status === "ok" ? `<button type="button" class="btn btn-sm" data-recheck="${esc(r.id)}">重新檢查</button>` : ""}</td></tr>`).join("")}</tbody></table>`;
}
