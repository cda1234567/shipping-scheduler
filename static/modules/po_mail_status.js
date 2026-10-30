/**
 * 採購單寄信狀態徽章（資料來自 automail，經 /api/automail/po-status；本站不寄信）。
 *
 * 各處只要輸出 `<span class="po-mail" data-po-mail="PO-..."></span>`，渲染後呼叫 decoratePoMailBadges(root) 即可。
 */
import { apiJson, apiPost, esc } from "/static/modules/api.js";

const CACHE_MS = 60 * 1000;
let _cache = null; // { at, result }
let _pending = null;

export function normalizePo(po) {
  return String(po ?? "").replace(/\s+/g, "").toUpperCase();
}

export async function loadPoMailStatus({ force = false } = {}) {
  if (!force && _cache && Date.now() - _cache.at < CACHE_MS) return _cache.result;
  if (_pending) return _pending;
  _pending = (async () => {
    try {
      const result = force ? await apiPost("/api/automail/refresh", {}) : await apiJson("/api/automail/po-status");
      _cache = { at: Date.now(), result };
      return result;
    } catch (error) {
      return { enabled: false, ok: false, error: error?.message || String(error), pos: {} };
    } finally {
      _pending = null;
    }
  })();
  return _pending;
}

export function poMailBadgeHtml(poNo, status) {
  if (!status || !status.enabled || !status.ok) return "";
  const key = normalizePo(poNo);
  if (!key) return "";
  const rec = (status.pos || {})[key];
  let html;
  if (rec && rec.sent) {
    const to = (rec.to || []).join("、");
    const title = `${to ? `收件：${to}` : ""}${rec.sent_count > 1 ? `${to ? "；" : ""}寄出 ${rec.sent_count} 次` : ""}`;
    html = `<span class="po-mail-sent" title="${esc(title)}">已寄 ${esc(rec.last_sent_local)}</span>`;
  } else {
    html = '<span class="po-mail-unsent">未寄</span>';
  }
  if (rec && rec.replied_count > 0) {
    const title = `${rec.last_reply_from || ""}${rec.last_reply_subject ? `：${rec.last_reply_subject}` : ""}`;
    html += ` <span class="po-mail-replied" title="${esc(title)}">廠商已回 ${esc(String(rec.last_reply_local || "").slice(0, 5))}</span>`;
  }
  return html;
}

export async function decoratePoMailBadges(root = document, { force = false } = {}) {
  const nodes = Array.from(root.querySelectorAll("[data-po-mail]"));
  if (!nodes.length) return;
  const status = await loadPoMailStatus({ force });
  for (const node of nodes) node.innerHTML = poMailBadgeHtml(node.dataset.poMail, status);
}

export function poMailSlot(poNo) {
  return `<span class="po-mail" data-po-mail="${esc(poNo)}"></span>`;
}

export async function poMailFailedHint() {
  const status = await loadPoMailStatus();
  return status.error || (status.enabled && !status.ok) ? "寄信狀態讀取失敗" : "";
}
