/**
 * 通用「未結明細挑選器」（不含任何收料專屬邏輯；之後開 invoice 可用 customer 當分組）。
 *
 * const picker = createOpenLinePicker(containerEl, {
 *   fetchLines: async (query) => lines[],      // query = { q }；回傳扁平明細陣列
 *   lineKey:    (line) => string,              // 每筆明細的唯一鍵（必填）
 *   groupKey:   (line) => string,              // 分組鍵（例：廠商代號）；第一筆勾選後鎖定該組
 *   groupLabel: (line) => string,              // 鎖定橫幅顯示的名稱
 *   columns: [{ label, render: (line) => htmlString(已跳脫), align?: "num" }],
 *   qtyOf:   (line) => number,                 // 可編輯數量的預設值（例：剩餘量）
 *   priceOf: (line) => number | null,          // 回 null 或省略 = 不顯示可編輯單價欄
 *   searchPlaceholder?: string,
 *   emptyText?: string,
 *   onChange?: (selection) => void,            // 勾選、數量、單價、鎖定變動時呼叫
 * });
 *
 * picker.getSelection() → [{ line, qty, price }]   依勾選順序；price 沒有 priceOf 時為 null
 * picker.reload()       → Promise                  重新抓明細（已不存在的勾選會被移除）
 * picker.clear()        → void                     取消所有勾選並解除鎖定
 * picker.lockedGroup()  → string | null
 *
 * 鎖定：勾第一筆後，其他分組的列變灰、不可勾；橫幅有「解除」鈕（會取消所有勾選）。
 */
import { esc } from "/static/modules/api.js";

export function createOpenLinePicker(root, options) {
  const opts = { searchPlaceholder: "搜尋廠商 / 料號 / 單號", emptyText: "沒有可選的明細", ...options };
  const hasPrice = typeof opts.priceOf === "function";
  let lines = [];
  let truncated = false;
  let lockKey = null;
  let lockLabel = "";
  const order = []; // 勾選順序（lineKey）
  const edits = new Map(); // lineKey → { qty, price }
  let timer = null;

  root.classList.add("olp");
  root.innerHTML = `
    <div class="olp-bar">
      <input type="search" class="olp-search" placeholder="${esc(opts.searchPlaceholder)}" autocomplete="off">
      <span class="olp-lock" hidden><span class="olp-lock-text"></span> <button type="button" class="btn btn-secondary btn-sm olp-unlock">解除</button></span>
      <span class="olp-count"></span>
    </div>
    <div class="olp-wrap"><table class="olp-table"><thead></thead><tbody></tbody></table></div>`;
  const search = root.querySelector(".olp-search");
  const lockEl = root.querySelector(".olp-lock");
  const countEl = root.querySelector(".olp-count");
  const head = root.querySelector("thead");
  const body = root.querySelector("tbody");

  const byKey = key => lines.find(l => opts.lineKey(l) === key);
  const fmtNum = v => (Number.isFinite(Number(v)) ? Number(v).toLocaleString("zh-TW", { maximumFractionDigits: 4 }) : "");

  function selection() {
    return order.map(key => byKey(key)).filter(Boolean).map(line => {
      const e = edits.get(opts.lineKey(line)) || {};
      return { line, qty: Number(e.qty), price: hasPrice ? Number(e.price) : null };
    });
  }

  function notify() {
    countEl.textContent = order.length ? `已選 ${order.length} 筆` : "";
    if (typeof opts.onChange === "function") opts.onChange(selection());
  }

  function renderHead() {
    head.innerHTML = `<tr><th></th>${opts.columns.map(c => `<th class="${c.align === "num" ? "num" : ""}">${esc(c.label)}</th>`).join("")}<th class="num">本次數量</th>${hasPrice ? '<th class="num">單價</th>' : ""}</tr>`;
  }

  function renderLock() {
    lockEl.hidden = lockKey === null;
    if (lockKey !== null) lockEl.querySelector(".olp-lock-text").textContent = `已鎖定：${lockLabel || lockKey}`;
  }

  function renderBody() {
    const colspan = opts.columns.length + 2 + (hasPrice ? 1 : 0);
    if (!lines.length) {
      body.innerHTML = `<tr><td colspan="${colspan}" class="olp-empty">${esc(opts.emptyText)}</td></tr>`;
      return;
    }
    body.innerHTML = lines.map(line => {
      const key = opts.lineKey(line);
      const checked = order.includes(key);
      const blocked = lockKey !== null && opts.groupKey(line) !== lockKey;
      const e = edits.get(key);
      const cells = opts.columns.map(c => `<td class="${c.align === "num" ? "num" : ""}">${c.render(line)}</td>`).join("");
      const qty = `<td class="num"><input type="number" class="olp-qty num" min="0" step="any" value="${esc(e ? e.qty : opts.qtyOf(line))}" ${checked ? "" : "disabled"}></td>`;
      const price = hasPrice ? `<td class="num"><input type="number" class="olp-price num" min="0" step="any" value="${esc(e ? e.price : opts.priceOf(line))}" ${checked ? "" : "disabled"}></td>` : "";
      return `<tr data-key="${esc(key)}" class="${blocked ? "olp-blocked" : ""}${checked ? " olp-picked" : ""}"><td><input type="checkbox" class="olp-check" ${checked ? "checked" : ""} ${blocked ? "disabled" : ""}></td>${cells}${qty}${price}</tr>`;
    }).join("") + (truncated ? `<tr><td colspan="${colspan}" class="olp-empty">結果過多，只顯示前面一部分，請用搜尋縮小範圍</td></tr>` : "");
  }

  function render() {
    renderHead();
    renderLock();
    renderBody();
  }

  function pick(key, on) {
    const line = byKey(key);
    if (!line) return;
    if (on) {
      if (lockKey === null) { lockKey = opts.groupKey(line); lockLabel = opts.groupLabel ? opts.groupLabel(line) : lockKey; }
      if (!order.includes(key)) order.push(key);
      if (!edits.has(key)) edits.set(key, { qty: opts.qtyOf(line), price: hasPrice ? opts.priceOf(line) : null });
    } else {
      const i = order.indexOf(key);
      if (i >= 0) order.splice(i, 1);
    }
  }

  body.addEventListener("change", event => {
    const tr = event.target.closest("tr[data-key]");
    if (!tr) return;
    const key = tr.dataset.key;
    if (event.target.classList.contains("olp-check")) {
      pick(key, event.target.checked);
      render();
    } else {
      const e = edits.get(key);
      if (e) {
        if (event.target.classList.contains("olp-qty")) e.qty = event.target.value;
        if (event.target.classList.contains("olp-price")) e.price = event.target.value;
      }
    }
    notify();
  });

  root.querySelector(".olp-unlock").addEventListener("click", () => api.clear());

  search.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(() => { void api.reload(); }, 250);
  });

  const api = {
    getSelection: selection,
    lockedGroup: () => lockKey,
    clear() {
      order.length = 0;
      edits.clear();
      lockKey = null;
      lockLabel = "";
      render();
      notify();
    },
    async reload() {
      const result = await opts.fetchLines({ q: search.value.trim() });
      lines = Array.isArray(result) ? result : (result.lines || []);
      truncated = !Array.isArray(result) && Boolean(result.truncated);
      const keys = new Set(lines.map(opts.lineKey));
      for (let i = order.length - 1; i >= 0; i--) if (!keys.has(order[i])) order.splice(i, 1);
      render();
      notify();
    },
  };
  render();
  return api;
}
