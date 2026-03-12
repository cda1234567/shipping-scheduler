import { apiJson, apiPost, esc } from "./api.js";

let _alerts = [];
let _unreadCount = 0;

export async function initAlerts() {
  document.getElementById("btn-mark-all-read")?.addEventListener("click", markAllRead);
  await refreshAlerts();
  // 每 30 秒自動檢查
  setInterval(refreshAlerts, 30000);
}

export async function refreshAlerts() {
  try {
    const d = await apiJson("/api/alerts?unread_only=false");
    _alerts = d.alerts || [];
    _unreadCount = d.unread_count || 0;
  } catch (_) { _alerts = []; _unreadCount = 0; }
  renderAlertBadge();
  renderAlertPanel();
}

function renderAlertBadge() {
  const badge = document.getElementById("alert-badge");
  if (!badge) return;
  if (_unreadCount > 0) {
    badge.textContent = _unreadCount;
    badge.style.display = "inline";
  } else {
    badge.style.display = "none";
  }
}

function renderAlertPanel() {
  const container = document.getElementById("alert-list");
  if (!container) return;

  if (!_alerts.length) {
    container.innerHTML = '<div class="empty-state">無提醒</div>';
    return;
  }

  container.innerHTML = _alerts.slice(0, 50).map(a => {
    const typeIcon = {
      "customer_material": "📦",
      "delivery_change": "📅",
      "cancellation": "🚫",
      "shortage_warning": "⚠️",
      "batch_merge_done": "✅",
    }[a.alert_type] || "🔔";

    const readClass = a.is_read ? "alert-read" : "alert-unread";
    const time = (a.created_at || "").slice(5, 16).replace("T", " ");

    return `<div class="alert-item ${readClass}" data-alert-id="${a.id}">
      <span class="alert-icon">${typeIcon}</span>
      <div class="alert-content">
        <div class="alert-msg">${esc(a.message)}</div>
        <div class="alert-time">${time}</div>
      </div>
      ${!a.is_read ? `<button class="alert-mark-btn" data-alert-id="${a.id}">已讀</button>` : ""}
    </div>`;
  }).join("");

  container.querySelectorAll(".alert-mark-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const alertId = parseInt(btn.dataset.alertId);
      try {
        await apiPost(`/api/alerts/${alertId}/read`);
        await refreshAlerts();
      } catch (_) {}
    });
  });
}

async function markAllRead() {
  try {
    await apiPost("/api/alerts/read-all");
    await refreshAlerts();
  } catch (_) {}
}
