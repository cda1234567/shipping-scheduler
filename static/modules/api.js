// ── API helpers ──────────────────────────────────────────────────────────────
export const BASE = "";
let _apiAuthRequiredHandler = null;

export function setApiAuthRequiredHandler(handler) {
  _apiAuthRequiredHandler = typeof handler === "function" ? handler : null;
}

export async function apiFetch(path, opts = {}) {
  const res = await fetch(BASE + path, opts);
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    let payload = null;
    try {
      payload = await res.clone().json();
      msg = payload?.detail || msg;
    } catch (_) {}
    if (res.status === 403 && payload?.code === "edit_auth_required" && _apiAuthRequiredHandler) {
      try {
        const loggedIn = await _apiAuthRequiredHandler(payload);
        if (loggedIn) return apiFetch(path, opts);
      } catch (_) {}
    }
    throw new Error(msg);
  }
  return res;
}

export async function apiJson(path, opts = {}) {
  const res = await apiFetch(path, opts);
  return res.json();
}

export async function apiPost(path, body = {}) {
  return apiJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function apiPatch(path, body = {}) {
  return apiJson(path, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function apiPut(path, body = {}) {
  return apiJson(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

// ── Toast ────────────────────────────────────────────────────────────────────
let _toastTimer = null;

export function hideToast() {
  const t = document.getElementById("toast");
  if (!t) return;
  if (_toastTimer) {
    clearTimeout(_toastTimer);
    _toastTimer = null;
  }
  t.classList.remove("show", "is-sticky", "tone-error", "tone-success");
  t.dataset.tone = "default";
}

export function showToast(msg, options = {}) {
  const t = document.getElementById("toast");
  if (!t) return;

  const config = typeof options === "number"
    ? { duration: options }
    : { duration: 2500, sticky: false, tone: "default", ...options };

  if (_toastTimer) {
    clearTimeout(_toastTimer);
    _toastTimer = null;
  }

  const messageEl = t.querySelector(".toast-message");
  const closeBtn = t.querySelector(".toast-close");
  if (messageEl) {
    messageEl.textContent = msg;
  } else {
    t.textContent = msg;
  }

  t.classList.remove("tone-error", "tone-success");
  if (config.tone === "error") t.classList.add("tone-error");
  if (config.tone === "success") t.classList.add("tone-success");
  t.classList.toggle("is-sticky", Boolean(config.sticky));
  t.classList.add("show");

  if (closeBtn) {
    closeBtn.style.display = config.sticky ? "inline-flex" : "none";
    closeBtn.onclick = () => hideToast();
  }

  if (!config.sticky && Number(config.duration) > 0) {
    _toastTimer = setTimeout(() => hideToast(), Number(config.duration));
  }
}

// ── Escape HTML ──────────────────────────────────────────────────────────────
export function esc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

export function fmt(n) {
  return Number.isInteger(n) ? n : n.toFixed(1);
}
