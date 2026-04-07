import { apiJson, apiPut, esc, showToast } from "./api.js";

let _initialized = false;
let _rows = [];

export async function initStPackages({ autoLoad = true } = {}) {
  if (!_initialized) {
    document.getElementById("btn-st-packages-refresh")?.addEventListener("click", () => {
      void refreshStPackages();
    });
    document.getElementById("st-packages-list")?.addEventListener("input", event => {
      const input = event.target?.closest(".st-package-input");
      if (!input) return;
      applyRowState(input.closest(".st-package-card"));
    });
    document.getElementById("st-packages-list")?.addEventListener("keydown", event => {
      const input = event.target?.closest(".st-package-input");
      if (!input || event.key !== "Enter") return;
      event.preventDefault();
      const button = input.closest(".st-package-card")?.querySelector("[data-save-st-package]");
      if (button) void saveRow(button);
    });
    document.getElementById("st-packages-list")?.addEventListener("click", event => {
      const button = event.target?.closest("[data-save-st-package]");
      if (!button) return;
      void saveRow(button);
    });
    _initialized = true;
  }

  if (autoLoad) {
    await refreshStPackages();
  }
}

export async function refreshStPackages() {
  const list = document.getElementById("st-packages-list");
  const status = document.getElementById("st-packages-status");
  if (!list || !status) return;

  status.className = "file-status";
  status.innerHTML = "<span>讀取無 MOQ 包裝資料中...</span>";

  try {
    const result = await apiJson("/api/system/st-packages/missing-moq");
    _rows = Array.isArray(result?.rows) ? result.rows : [];
    renderRows(_rows);

    if (!_rows.length) {
      status.className = "file-status ok";
      status.innerHTML = "<span>目前沒有需要管理的無 MOQ 料。</span>";
      return;
    }

    const mismatchCount = _rows.filter(row => !row.matches_stock).length;
    status.className = mismatchCount ? "file-status warn" : "file-status ok";
    status.innerHTML = mismatchCount
      ? `<span>共 ${_rows.length} 筆，${mismatchCount} 筆包裝合計和 ST 庫存不一致。</span>`
      : `<span>共 ${_rows.length} 筆，目前都已和 ST 庫存對齊。</span>`;
  } catch (error) {
    list.innerHTML = "";
    status.className = "file-status warn";
    status.innerHTML = `<span>${esc(`讀取無 MOQ 包裝資料失敗：${error.message}`)}</span>`;
  }
}

function renderRows(rows) {
  const list = document.getElementById("st-packages-list");
  if (!list) return;
  if (!rows.length) {
    list.innerHTML = '<div class="st-package-empty">目前沒有需要管理的無 MOQ 料。</div>';
    return;
  }

  list.innerHTML = rows.map(buildRowHtml).join("");
  list.querySelectorAll(".st-package-card").forEach(card => applyRowState(card));
}

function buildRowHtml(row) {
  const updatedAt = formatDateTime(row.updated_at);
  const packageText = String(row.package_text || "");
  return `
    <section class="st-package-card ${row.matches_stock ? "is-match" : "is-mismatch"}"
      data-part-number="${esc(row.part_number)}"
      data-stock-qty="${esc(row.stock_qty)}">
      <div class="st-package-card-head">
        <div>
          <div class="st-package-part">${esc(row.part_number)}</div>
          <div class="st-package-desc">${esc(row.description || "—")}</div>
        </div>
        <div class="st-package-badge">${row.matches_stock ? "已對齊" : `差 ${esc(formatQty(row.diff_qty))}`}</div>
      </div>
      <div class="st-package-meta">
        <span>ST 庫存 ${esc(formatQty(row.stock_qty))}</span>
        <span>包裝合計 <strong class="st-package-sum">${esc(formatQty(row.package_sum))}</strong></span>
        <span class="st-package-diff">差額 ${esc(formatQty(row.diff_qty))}</span>
        <span>最後修改 ${esc(updatedAt || "尚未設定")}</span>
      </div>
      <div class="st-package-editor">
        <input class="st-package-input" type="text" value="${esc(packageText)}" placeholder="例如：200,300,500">
        <button class="btn btn-primary btn-sm" type="button" data-save-st-package>保存</button>
      </div>
      <div class="st-package-help">料號清單來自主檔 MOQ 空白料；包裝合計只跟 ST 庫存比對。寫入主檔後會先找整包相等的數量扣除，沒有才由左到右拆包。</div>
      <div class="st-package-warning"></div>
    </section>
  `;
}

function parsePackageInput(text) {
  const raw = String(text || "").replaceAll("，", ",").trim();
  if (!raw) return [];
  const values = [];
  for (const token of raw.split(",")) {
    const item = String(token || "").trim();
    if (!item) continue;
    const amount = Number(item);
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new Error(`包裝數量格式不正確：${item}`);
    }
    values.push(amount);
  }
  return values;
}

function applyRowState(card) {
  if (!card) return;
  const input = card.querySelector(".st-package-input");
  const warning = card.querySelector(".st-package-warning");
  const sumEl = card.querySelector(".st-package-sum");
  const diffEl = card.querySelector(".st-package-diff");
  const badge = card.querySelector(".st-package-badge");
  const saveButton = card.querySelector("[data-save-st-package]");
  const stockQty = Number(card.dataset.stockQty || 0);

  try {
    const values = parsePackageInput(input?.value || "");
    const total = values.reduce((sum, value) => sum + value, 0);
    const diff = total - stockQty;
    const matches = Math.abs(diff) < 0.000001;

    if (sumEl) sumEl.textContent = formatQty(total);
    if (diffEl) diffEl.textContent = `差額 ${formatQty(diff)}`;
    if (warning) {
      warning.textContent = matches
        ? "包裝合計已和 ST 庫存對齊。"
        : `包裝合計 ${formatQty(total)} 與 ST 庫存 ${formatQty(stockQty)} 不一致。`;
    }
    if (badge) badge.textContent = matches ? "已對齊" : `差 ${formatQty(diff)}`;
    card.classList.toggle("is-match", matches);
    card.classList.toggle("is-mismatch", !matches);
    if (saveButton) saveButton.disabled = false;
  } catch (error) {
    if (sumEl) sumEl.textContent = "格式錯誤";
    if (diffEl) diffEl.textContent = "差額 —";
    if (warning) warning.textContent = error.message;
    if (badge) badge.textContent = "格式錯誤";
    card.classList.remove("is-match");
    card.classList.add("is-mismatch");
    if (saveButton) saveButton.disabled = true;
  }
}

async function saveRow(button) {
  const card = button?.closest(".st-package-card");
  const input = card?.querySelector(".st-package-input");
  const partNumber = String(card?.dataset.partNumber || "").trim();
  if (!card || !input || !partNumber) return;

  try {
    parsePackageInput(input.value);
  } catch (error) {
    showToast(error.message, { tone: "error", sticky: true });
    applyRowState(card);
    return;
  }

  const idleText = button.textContent;
  button.disabled = true;
  button.textContent = "保存中...";
  try {
    const result = await apiPut(`/api/system/st-packages/${encodeURIComponent(partNumber)}`, {
      package_text: input.value,
    });
    const row = result?.row;
    if (row) {
      input.value = row.package_text || "";
      card.outerHTML = buildRowHtml(row);
      const replaced = document.querySelector(`.st-package-card[data-part-number="${cssEscape(partNumber)}"]`);
      applyRowState(replaced);
    } else {
      await refreshStPackages();
    }
    showToast(`${partNumber} 包裝已保存`, { tone: "success" });
    await refreshStPackages();
  } catch (error) {
    showToast(`保存失敗：${error.message}`, { tone: "error", sticky: true });
    button.disabled = false;
    button.textContent = idleText;
  }
}

function formatQty(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return "0";
  if (Math.abs(amount - Math.round(amount)) < 0.000001) {
    return String(Math.round(amount));
  }
  return amount.toFixed(3).replace(/\.?0+$/, "");
}

function formatDateTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const normalized = text.includes("T") ? text : text.replace(" ", "T");
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return text.replace("T", " ").slice(0, 16);
  const y = parsed.getFullYear();
  const m = String(parsed.getMonth() + 1).padStart(2, "0");
  const d = String(parsed.getDate()).padStart(2, "0");
  const hh = String(parsed.getHours()).padStart(2, "0");
  const mm = String(parsed.getMinutes()).padStart(2, "0");
  return `${y}/${m}/${d} ${hh}:${mm}`;
}

function cssEscape(value) {
  if (typeof window.CSS?.escape === "function") return window.CSS.escape(value);
  return String(value).replace(/["\\]/g, "\\$&");
}
