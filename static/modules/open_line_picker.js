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
 *   otherGroupText?: string,                   // 鎖定時隱藏其他分組的提示，{n} = 隱藏筆數；預設「已隱藏其他分組 {n} 筆」
 *   multiGroupText?: string,                   // 全選時畫面上有多個分組的提示；預設「目前顯示多個分組，請先搜尋縮小到單一分組再全選」
 *   onRender?: (rootEl) => void,               // 表格重畫後呼叫（例：補上徽章）
 *   onChange?: (selection) => void,            // 勾選、數量、單價、鎖定變動時呼叫
 * });
 *
 * picker.getSelection() → [{ line, qty, price }]   依勾選順序；price 沒有 priceOf 時為 null
 * picker.reload()       → Promise                  重新抓明細（已不存在的勾選會被移除）
 * picker.clear()        → void                     取消所有勾選並解除鎖定
 * picker.lockedGroup()  → string | null
 * picker.selectAll()    → { ok, message? }       勾選目前畫面上（搜尋後、鎖定後）看得到的全部明細
 * picker.selectNone()   → void                   全不選（= clear()）
 *
 * 全選（v2026.10.30.14）：只勾「目前顯示」的列；已鎖定時就是鎖定分組內看得到的列。
 * 未鎖定且畫面上有多個分組 → 不勾任何列，顯示 multiGroupText 請使用者先搜尋縮小到單一分組（絕不混分組）。
 * 表頭勾選框 = 全選 / 全不選切換。
 *
 * 鎖定：勾第一筆後，其他分組的列整個隱藏（不渲染），並在計數處顯示「已隱藏其他廠商 N 筆」之類的提示；
 * 取消最後一筆勾選、按橫幅「解除」或 clear() 後全部列重新顯示。搜尋仍在鎖定分組內運作。
 * （送出時的同廠商檢查仍由後端負責。）
 */
import { esc } from "/static/modules/api.js";

/** 鎖定某分組時只顯示該組的列；未鎖定（lockKey === null）時全部顯示。 */
export function visibleLines(lines, lockKey, groupKey) {
  return lockKey === null ? lines : lines.filter(line => groupKey(line) === lockKey);
}

/**
 * 全選要勾哪些列（純函式）。visible = 目前畫面上的列。
 * 回傳 { ok: true, keys, group } 或 { ok: false, reason: "empty" | "multi_group" }。
 */
export function planSelectAll(visible, lockKey, groupKey, lineKey) {
  if (!visible.length) return { ok: false, reason: "empty" };
  const groups = new Set(visible.map(groupKey));
  if (lockKey === null && groups.size > 1) return { ok: false, reason: "multi_group", groups: groups.size };
  const group = lockKey === null ? [...groups][0] : lockKey;
  return { ok: true, group, keys: visible.filter(l => groupKey(l) === group).map(lineKey) };
}

export function createOpenLinePicker(root, options) {
  const opts = { searchPlaceholder: "搜尋廠商 / 料號 / 單號", emptyText: "沒有可選的明細", otherGroupText: "已隱藏其他分組 {n} 筆",
    multiGroupText: "目前顯示多個分組，請先搜尋縮小到單一分組再全選", ...options };
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
      <button type="button" class="btn btn-secondary btn-sm olp-all">全選</button>
      <button type="button" class="btn btn-secondary btn-sm olp-none">全不選</button>
      <span class="olp-count"></span>
      <span class="olp-msg" role="status"></span>
    </div>
    <div class="olp-wrap"><table class="olp-table"><thead></thead><tbody></tbody></table></div>`;
  const search = root.querySelector(".olp-search");
  const lockEl = root.querySelector(".olp-lock");
  const countEl = root.querySelector(".olp-count");
  const head = root.querySelector("thead");
  const body = root.querySelector("tbody");
  const msgEl = root.querySelector(".olp-msg");
  const setMsg = text => { if (msgEl) msgEl.textContent = text || ""; };

  const byKey = key => lines.find(l => opts.lineKey(l) === key);
  const fmtNum = v => (Number.isFinite(Number(v)) ? Number(v).toLocaleString("zh-TW", { maximumFractionDigits: 4 }) : "");

  function selection() {
    return order.map(key => byKey(key)).filter(Boolean).map(line => {
      const e = edits.get(opts.lineKey(line)) || {};
      return { line, qty: Number(e.qty), price: hasPrice ? Number(e.price) : null };
    });
  }

  function notify() {
    const hidden = lines.length - visibleLines(lines, lockKey, opts.groupKey).length;
    const parts = [];
    if (order.length) parts.push(`已選 ${order.length} 筆`);
    if (hidden > 0) parts.push(opts.otherGroupText.replace("{n}", String(hidden)));
    countEl.textContent = parts.join("　");
    if (typeof opts.onChange === "function") opts.onChange(selection());
  }

  function renderHead() {
    const vis = visibleLines(lines, lockKey, opts.groupKey);
    const allOn = vis.length > 0 && vis.every(l => order.includes(opts.lineKey(l)));
    head.innerHTML = `<tr><th><input type="checkbox" class="olp-check-all" title="全選 / 全不選（目前顯示的列）" ${allOn ? "checked" : ""} ${vis.length ? "" : "disabled"}></th>${opts.columns.map(c => `<th class="${c.align === "num" ? "num" : ""}">${esc(c.label)}</th>`).join("")}<th class="num">本次數量</th>${hasPrice ? '<th class="num">單價</th>' : ""}</tr>`;
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
    body.innerHTML = visibleLines(lines, lockKey, opts.groupKey).map(line => {
      const key = opts.lineKey(line);
      const checked = order.includes(key);
      const e = edits.get(key);
      const cells = opts.columns.map(c => `<td class="${c.align === "num" ? "num" : ""}">${c.render(line)}</td>`).join("");
      const qty = `<td class="num"><input type="number" class="olp-qty num" min="0" step="any" value="${esc(e ? e.qty : opts.qtyOf(line))}" ${checked ? "" : "disabled"}></td>`;
      const price = hasPrice ? `<td class="num"><input type="number" class="olp-price num" min="0" step="any" value="${esc(e ? e.price : opts.priceOf(line))}" ${checked ? "" : "disabled"}></td>` : "";
      return `<tr data-key="${esc(key)}" class="${checked ? "olp-picked" : ""}"><td><input type="checkbox" class="olp-check" ${checked ? "checked" : ""}></td>${cells}${qty}${price}</tr>`;
    }).join("") + (truncated ? `<tr><td colspan="${colspan}" class="olp-empty">結果過多，只顯示前面一部分，請用搜尋縮小範圍</td></tr>` : "");
  }

  function render() {
    renderHead();
    renderLock();
    renderBody();
    if (typeof opts.onRender === "function") opts.onRender(root);
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
      if (!order.length) { lockKey = null; lockLabel = ""; } // 最後一筆取消 → 解除鎖定，其他分組重新顯示
    }
  }

  body.addEventListener("change", event => {
    const tr = event.target.closest("tr[data-key]");
    if (!tr) return;
    const key = tr.dataset.key;
    if (event.target.classList.contains("olp-check")) {
      setMsg("");
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
  root.querySelector(".olp-all")?.addEventListener("click", () => { api.selectAll(); });
  root.querySelector(".olp-none")?.addEventListener("click", () => api.selectNone());
  head.addEventListener("change", event => {
    if (!event.target.classList.contains("olp-check-all")) return;
    if (event.target.checked) api.selectAll(); else api.selectNone();
  });

  search.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(() => { void api.reload(); }, 250);
  });

  const api = {
    getSelection: selection,
    lockedGroup: () => lockKey,
    selectAll() {
      const plan = planSelectAll(visibleLines(lines, lockKey, opts.groupKey), lockKey, opts.groupKey, opts.lineKey);
      if (!plan.ok) {
        const message = plan.reason === "multi_group" ? opts.multiGroupText : opts.emptyText;
        setMsg(message);
        render(); // 表頭勾選框回到實際狀態
        notify();
        return { ok: false, message };
      }
      setMsg("");
      for (const key of plan.keys) pick(key, true);
      render();
      notify();
      return { ok: true };
    },
    selectNone() {
      setMsg("");
      api.clear();
    },
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
      if (!order.length) { lockKey = null; lockLabel = ""; }
      render();
      notify();
    },
  };
  render();
  return api;
}
