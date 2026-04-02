import { apiJson, apiPost, setApiAuthRequiredHandler, showToast } from "/static/modules/api.js";

let _editAuthStatus = {
  authenticated: false,
  readonly: true,
  expires_at: "",
};

const STATIC_EDIT_LOCK_SELECTORS = [
  "#btn-upload-main",
  "#btn-upload-st-inventory",
  "#btn-upload-schedule",
  "#btn-save-order",
  "#btn-batch-merge",
  "#btn-batch-dispatch",
  "#btn-dedup-schedule",
  "#btn-create-folder",
  "#btn-save-bom-order",
];

function setLockedElementState(element, locked) {
  if (!element) return;
  if ("disabled" in element) {
    element.disabled = locked;
  }
  element.classList.toggle("edit-auth-locked", locked);
  if (locked) {
    element.setAttribute("title", "目前為唯讀模式，請先登入編輯。");
  } else if (element.getAttribute("title") === "目前為唯讀模式，請先登入編輯。") {
    element.removeAttribute("title");
  }
}

function closeEditAuthModal() {
  const modal = document.getElementById("edit-auth-modal");
  if (modal) modal.style.display = "none";
}

export function openEditAuthModal() {
  const modal = document.getElementById("edit-auth-modal");
  const passwordInput = document.getElementById("edit-auth-password");
  const note = document.getElementById("edit-auth-note");
  if (!modal || !passwordInput || !note) return;
  note.textContent = _editAuthStatus?.authenticated
    ? "目前已登入編輯模式。"
    : "目前為唯讀模式，登入後才可編輯。";
  passwordInput.value = "";
  modal.style.display = "flex";
  passwordInput.focus();
}

function applyReadonlyUi() {
  const authenticated = Boolean(_editAuthStatus?.authenticated);
  document.body.classList.toggle("edit-auth-readonly", !authenticated);

  const button = document.getElementById("btn-edit-auth");
  if (button) {
    button.textContent = authenticated ? "登出編輯" : "登入編輯";
    button.classList.remove("btn-secondary", "btn-success");
    button.classList.add(authenticated ? "btn-success" : "btn-secondary");
    button.title = authenticated
      ? `已登入編輯${_editAuthStatus?.expires_at ? `，到期 ${String(_editAuthStatus.expires_at).slice(5, 16).replace("T", " ")}` : ""}`
      : "目前為唯讀模式，登入後才可編輯";
  }

  const chip = document.getElementById("edit-auth-status-chip");
  if (chip) {
    chip.textContent = authenticated ? "可編輯" : "唯讀";
    chip.classList.toggle("is-authenticated", authenticated);
  }

  STATIC_EDIT_LOCK_SELECTORS.forEach(selector => {
    document.querySelectorAll(selector).forEach(node => setLockedElementState(node, !authenticated));
  });
}

export function isEditAuthenticated() {
  return Boolean(_editAuthStatus?.authenticated);
}

export async function refreshEditAuthStatus() {
  _editAuthStatus = await apiJson("/api/system/edit-auth/status");
  applyReadonlyUi();
  return _editAuthStatus;
}

export async function initEditAuth() {
  document.getElementById("btn-edit-auth")?.addEventListener("click", async () => {
    try {
      if (isEditAuthenticated()) {
        await apiPost("/api/system/edit-auth/logout");
        await refreshEditAuthStatus();
        showToast("已切回唯讀模式");
        return;
      }
      openEditAuthModal();
    } catch (error) {
      showToast(`編輯模式切換失敗：${error.message}`);
    }
  });
  document.getElementById("edit-auth-close")?.addEventListener("click", closeEditAuthModal);
  document.getElementById("edit-auth-cancel")?.addEventListener("click", closeEditAuthModal);
  document.getElementById("edit-auth-modal")?.addEventListener("click", event => {
    if (event.target?.id === "edit-auth-modal") closeEditAuthModal();
  });
  document.getElementById("edit-auth-submit")?.addEventListener("click", async () => {
    const passwordInput = document.getElementById("edit-auth-password");
    const note = document.getElementById("edit-auth-note");
    const password = String(passwordInput?.value || "");
    if (!password.trim()) {
      note.textContent = "請輸入密碼。";
      passwordInput?.focus();
      return;
    }
    try {
      await apiPost("/api/system/edit-auth/login", { password });
      await refreshEditAuthStatus();
      closeEditAuthModal();
      showToast("已登入編輯模式", { tone: "success" });
    } catch (error) {
      note.textContent = error.message || "登入失敗，請稍後再試。";
      passwordInput?.focus();
      passwordInput?.select();
    }
  });
  document.getElementById("edit-auth-password")?.addEventListener("keydown", event => {
    if (event.key === "Enter") {
      event.preventDefault();
      document.getElementById("edit-auth-submit")?.click();
    }
  });
  setApiAuthRequiredHandler(() => {
    openEditAuthModal();
    showToast("目前為唯讀模式，請先登入編輯。");
  });
  await refreshEditAuthStatus();
}
