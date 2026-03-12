import { apiJson, esc } from "./api.js";

export async function initLogs() {
  await refreshLogs();
}

export async function refreshLogs() {
  const container = document.getElementById("log-list");
  if (!container) return;

  try {
    const d = await apiJson("/api/logs?limit=100");
    const logs = d.logs || [];

    if (!logs.length) {
      container.innerHTML = '<div class="empty-state">無操作紀錄</div>';
      return;
    }

    container.innerHTML = logs.map(l => {
      const time = (l.created_at || "").slice(5, 16).replace("T", " ");
      return `<div class="log-item">
        <span class="log-time">${time}</span>
        <span class="log-action">${esc(l.action)}</span>
        <span class="log-detail">${esc(l.detail)}</span>
      </div>`;
    }).join("");
  } catch (_) {
    container.innerHTML = '<div class="empty-state">載入失敗</div>';
  }
}
