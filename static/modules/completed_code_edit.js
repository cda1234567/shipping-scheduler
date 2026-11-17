// ── 已發料列：雙擊批次代號標籤改代號（例：12-13 → 12-7）────────────────────────
import { apiPatch, showToast } from "./api.js";

const BATCH_CODE_RE = /^\d+-\d+$/;

export function normalizeBatchCode(value) {
  return String(value ?? "").trim();
}

// 回傳錯誤訊息；空字串 = 格式正確
export function validateBatchCode(value) {
  const code = normalizeBatchCode(value);
  if (!code) return "批次代號不可空白";
  if (!BATCH_CODE_RE.test(code)) return "批次代號格式需為「數字-數字」，例如 12-7";
  return "";
}

// Enter 存檔、Esc 取消，其他鍵不處理
export function batchCodeKeyAction(key) {
  if (key === "Enter") return "save";
  if (key === "Escape" || key === "Esc") return "cancel";
  return "";
}

// 輸入框寬度：字數剛好（至少 5 字寬，多 1 字給游標）
export function batchCodeInputWidth(value) {
  return `${Math.max(normalizeBatchCode(value).length, 5) + 1}ch`;
}

export function isDuplicateCodeError(error) {
  return error?.status === 409 && error?.payload?.detail?.code === "duplicate_code";
}

function defaultRequest(orderId, code, force) {
  return apiPatch(`/api/schedule/completed/${orderId}/code`, { code, force });
}

// 沒有編輯權限時照其他寫入功能：toast + 開登入視窗（動態載入，純函式測試不需要 DOM）
async function defaultRequireEdit() {
  const { isEditAuthenticated, openEditAuthModal } = await import("/static/modules/edit_auth.js");
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

// 送出改代號；409 重複代號時 confirm 後帶 force 重送。回傳 { status: "saved" | "unchanged" | "cancelled", result }
export async function submitBatchCodeChange(orderId, original, value, {
  request = defaultRequest,
  confirmFn = message => window.confirm(message),
} = {}) {
  const code = normalizeBatchCode(value);
  if (code === normalizeBatchCode(original)) return { status: "unchanged", result: null };
  const invalid = validateBatchCode(code);
  if (invalid) throw new Error(invalid);
  try {
    return { status: "saved", result: await request(orderId, code, false) };
  } catch (error) {
    if (!isDuplicateCodeError(error)) throw error;
    if (!confirmFn(error.message)) return { status: "cancelled", result: null };
    return { status: "saved", result: await request(orderId, code, true) };
  }
}

export function attachCompletedCodeEditor(tag, {
  orderId,
  code,
  onSaved = async () => {},
  request = defaultRequest,
  confirmFn = message => window.confirm(message),
  toast = showToast,
  requireEdit = defaultRequireEdit,
  doc = document,
} = {}) {
  if (!tag) return;
  const original = normalizeBatchCode(code);
  tag.title = "雙擊修改批次代號";
  tag.classList.add("completed-code-tag");

  tag.addEventListener("dblclick", async event => {
    event.preventDefault();
    event.stopPropagation();
    if (tag.dataset.editing === "1") return;
    tag.dataset.editing = "1";
    if (!(await requireEdit())) {
      delete tag.dataset.editing;
      return;
    }

    const input = doc.createElement("input");
    input.type = "text";
    input.className = "completed-code-input";
    input.value = original;
    input.style.width = batchCodeInputWidth(original);
    input.setAttribute("aria-label", "批次代號");
    let finished = false;

    const restore = () => {
      tag.textContent = original;
      delete tag.dataset.editing;
    };
    const finish = async action => {
      if (finished) return;
      finished = true;
      if (action === "cancel") {
        restore();
        return;
      }
      const value = input.value;
      const invalid = normalizeBatchCode(value) === original ? "" : validateBatchCode(value);
      if (invalid) {
        toast(invalid, { tone: "error" });
        restore();
        return;
      }
      input.disabled = true;
      let outcome;
      try {
        outcome = await submitBatchCodeChange(orderId, original, value, { request, confirmFn });
        if (outcome.status !== "saved") {
          restore();
          return;
        }
        const result = outcome.result || {};
        tag.textContent = normalizeBatchCode(result.code || value);
        delete tag.dataset.editing;
        toast(
          result.unchanged
            ? "批次代號未變更"
            : `批次代號已改為 ${result.code}（主檔標頭 ${(result.columns || []).length} 欄一起更新）`,
          { tone: "success" },
        );
      } catch (error) {
        restore();
        toast(`批次代號未修改：${error.message}`, { tone: "error", sticky: true });
        return;
      }
      try {
        await onSaved(outcome.result);
      } catch (error) {
        toast(`批次代號已修改，但重新載入已發料列表失敗：${error.message}`, { tone: "error" });
      }
    };

    input.addEventListener("input", () => {
      input.style.width = batchCodeInputWidth(input.value);
    });
    input.addEventListener("keydown", keyEvent => {
      keyEvent.stopPropagation();
      const action = batchCodeKeyAction(keyEvent.key);
      if (!action) return;
      keyEvent.preventDefault();
      void finish(action);
    });
    input.addEventListener("blur", () => { void finish("save"); });
    input.addEventListener("click", clickEvent => clickEvent.stopPropagation());
    input.addEventListener("dblclick", clickEvent => clickEvent.stopPropagation());

    tag.textContent = "";
    tag.appendChild(input);
    input.focus();
    input.select();
  });
}
