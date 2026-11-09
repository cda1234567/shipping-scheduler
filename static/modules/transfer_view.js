// 一鍵調撥畫面（純函式，不呼叫 API；node 測試可直接 import）。

export const WARE_LABELS = { "001": "一般倉", "002": "客供倉", "015": "庚霖倉" };
export const COLUMNS = ["料號", "品名", "本次撥"];
export const INTRO = "一個批次開一張調撥單：一般料從一般倉（001）、客供料從客供倉（002）撥到庚霖倉（015）。公司倉不夠時能撥多少撥多少，短少的留到下次。";

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

export function fmtQty(value) {
  return Number(value || 0).toLocaleString("zh-TW", { maximumFractionDigits: 4 });
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
  return `${batch.rows.filter(r => r.transfer > 0).length} 項，撥 ${fmtQty(batch.totals.transfer)}`;
}

/** 右邊清單：還沒開的可勾（沒貨的不能勾），已開過的另外一區、沒有 checkbox。 */
export function renderPicker(data, checked = new Set()) {
  if (!data.batches.length) return `<p class="transfer-empty">目前沒有要調撥的批次。</p>`;
  const open = data.batches.filter(b => batchState(b) !== "done");
  const done = data.batches.filter(b => batchState(b) === "done");
  const openHtml = open.length ? open.map(b => {
    const state = batchState(b);
    const input = state === "ready"
      ? `<input type="checkbox" data-transfer-batch="${esc(b.key)}"${checked.has(b.key) ? " checked" : ""}>`
      : `<input type="checkbox" disabled title="公司倉沒貨，不能開">`;
    return `<label class="transfer-pick transfer-pick-${state}">${input}
      <span class="transfer-pick-code">${esc(b.code)}</span>
      <span class="transfer-pick-note">${batchNote(b)}</span></label>`;
  }).join("") : `<p class="transfer-empty">沒有還沒開的批次。</p>`;
  const doneHtml = done.length ? `<div class="transfer-side-group transfer-side-done">
      <h3>已開過（${done.length}）</h3>
      <ul>${done.map(b => `<li><span class="transfer-pick-code">${esc(b.code)}</span><span class="transfer-done-no">${esc(b.already.move_no)}</span></li>`).join("")}</ul>
    </div>` : "";
  return `<div class="transfer-side-group transfer-side-open"><h3>還沒開（${open.length}）</h3>${openHtml}</div>${doneHtml}`;
}

/** 左邊明細：只畫右邊勾到、可開的批次；每批只列料號／品名／本次撥。 */
export function renderDetails(data, checked = new Set()) {
  const picked = data.batches.filter(b => checked.has(b.key) && batchState(b) === "ready");
  if (!picked.length) return `<p class="transfer-empty transfer-detail-empty">右邊勾選要開的批次，明細會出現在這裡</p>`;
  return picked.map(b => `<section class="transfer-batch" data-batch-key="${esc(b.key)}">
    <div class="transfer-batch-head">
      <span class="transfer-batch-title">${esc(b.code)} · 一張調撥單</span>
      <span class="transfer-batch-note">${batchNote(b)}</span>
    </div>
    <table class="transfer-detail-table">
      <colgroup><col class="transfer-col-part"><col><col class="transfer-col-qty"></colgroup>
      <thead><tr><th>${COLUMNS[0]}</th><th>${COLUMNS[1]}</th><th class="num">${COLUMNS[2]}</th></tr></thead>
      <tbody>${b.rows.filter(r => r.transfer > 0).map(r => `<tr><td>${esc(r.part)}</td><td>${esc(r.name)}</td><td class="num"><strong>${fmtQty(r.transfer)}</strong></td></tr>`).join("")}</tbody>
    </table></section>`).join("");
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
