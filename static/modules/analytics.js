import { apiJson, apiFetch, showToast, esc, fmt } from "./api.js";

let _historyChart = null;
let _historyGroupBy = "model";
let _diffResult = null;
let _partHistoryRequestId = 0;

export async function initAnalytics() {
  document.getElementById("part-history-form")?.addEventListener("submit", loadPartDispatchHistory);
  document.getElementById("frequent-zero-months")?.addEventListener("change", loadFrequentZeroStock);
  document.getElementById("frequent-zero-min-orders")?.addEventListener("change", loadFrequentZeroStock);
  document.getElementById("btn-refresh-frequent-zero")?.addEventListener("click", loadFrequentZeroStock);
  document.getElementById("history-group-select")?.addEventListener("change", (e) => {
    _historyGroupBy = e.target.value;
    loadHistory();
  });
  document.getElementById("btn-schedule-diff")?.addEventListener("click", handleScheduleDiff);
  document.getElementById("btn-diff-apply")?.addEventListener("click", handleDiffApply);
}

export async function refreshAnalytics() {
  await Promise.all([loadFrequentZeroStock(), loadHistory()]);
}

// ── 單一料號發料查詢 ──────────────────────────────────────────────────────────

async function loadPartDispatchHistory(event) {
  event?.preventDefault();
  const input = document.getElementById("part-history-input");
  const result = document.getElementById("part-history-result");
  const button = document.getElementById("btn-part-history");
  const partNumber = String(input?.value || "").trim();
  const days = document.getElementById("part-history-days")?.value || "30";
  if (!result) return;
  if (!partNumber) {
    input?.focus();
    showToast("請先輸入完整料號");
    return;
  }

  const requestId = ++_partHistoryRequestId;
  result.innerHTML = '<div class="no-shortage-msg">正在查詢發料紀錄...</div>';
  if (button) {
    button.disabled = true;
    button.textContent = "查詢中...";
  }
  try {
    const data = await apiJson(`/api/analytics/part-dispatch-history?part_number=${encodeURIComponent(partNumber)}&days=${encodeURIComponent(days)}`);
    if (requestId !== _partHistoryRequestId) return;
    renderPartDispatchHistory(data);
  } catch (error) {
    if (requestId !== _partHistoryRequestId) return;
    result.innerHTML = `<div class="part-history-empty is-error">查詢失敗：${esc(error.message || "未知錯誤")}</div>`;
  } finally {
    if (button && requestId === _partHistoryRequestId) {
      button.disabled = false;
      button.textContent = "查詢";
    }
  }
}

function renderPartDispatchHistory(data) {
  const container = document.getElementById("part-history-result");
  if (!container) return;
  const rows = Array.isArray(data?.rows) ? data.rows : [];
  const lastDispatched = formatAnalyticsDateTime(data?.last_dispatched_at);
  const summary = `
    <div class="part-history-query-label">料號 <strong>${esc(data?.part_number || "")}</strong></div>
    <div class="part-history-summary">
      <div class="part-history-stat">
        <span>查詢期間</span>
        <strong>${esc(data?.period_label || "—")}</strong>
      </div>
      <div class="part-history-stat is-primary">
        <span>發料總用量（BOM）</span>
        <strong>${fmt(Number(data?.total_qty || 0))}</strong>
      </div>
      <div class="part-history-stat">
        <span>發料訂單</span>
        <strong>${fmt(Number(data?.order_count || 0))} 筆</strong>
      </div>
      <div class="part-history-stat">
        <span>最近發料</span>
        <strong>${esc(lastDispatched || "—")}</strong>
      </div>
    </div>`;

  if (!rows.length) {
    container.innerHTML = `${summary}<div class="part-history-empty">這段期間沒有符合條件的發料紀錄。可確認完整料號，或改查全部紀錄。</div>`;
    return;
  }

  container.innerHTML = `${summary}
    <div class="part-history-table-wrap">
      <table class="analytics-table part-history-table">
        <thead><tr>
          <th>發料時間</th>
          <th>批次</th>
          <th>PO</th>
          <th>機種</th>
          <th>出貨日</th>
          <th>發料用量</th>
        </tr></thead>
        <tbody>${rows.map(row => `<tr>
          <td>${esc(formatAnalyticsDateTime(row.dispatched_at) || "—")}</td>
          <td>${esc(row.code || "—")}</td>
          <td>${esc(row.po_number || "—")}</td>
          <td>${esc(row.model || "—")}</td>
          <td>${esc(row.ship_date || "—")}</td>
          <td class="part-history-qty">${fmt(Number(row.issued_qty || 0))}</td>
        </tr>`).join("")}</tbody>
      </table>
    </div>
    <div class="part-history-note">以發料當時保存的 BOM 生產用量加總，同一訂單相同料號合併顯示；排除標記缺料的紀錄。最近天數從查詢當下往前計算。</div>`;
}

function formatAnalyticsDateTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.replace("T", " ").slice(0, 16);
}

// ── 常用料零庫存 ──────────────────────────────────────────────────────────────

async function loadFrequentZeroStock() {
  const container = document.getElementById("frequent-zero-table");
  const summaryEl = document.getElementById("frequent-zero-summary");
  const warningEl = document.getElementById("frequent-zero-source-warning");
  if (!container) return;
  const months = document.getElementById("frequent-zero-months")?.value || "6";
  const minOrders = document.getElementById("frequent-zero-min-orders")?.value || "3";
  container.innerHTML = '<div class="no-shortage-msg">正在分析近期用料與庫存...</div>';
  if (summaryEl) summaryEl.innerHTML = "";
  if (warningEl) warningEl.innerHTML = "";
  try {
    const data = await apiJson(`/api/analytics/frequent-zero-stock?months=${encodeURIComponent(months)}&min_orders=${encodeURIComponent(minOrders)}`);
    renderFrequentZeroStock(data);
  } catch (error) {
    container.innerHTML = `<div class="no-shortage-msg">分析載入失敗：${esc(error.message || "未知錯誤")}</div>`;
  }
}

function renderFrequentZeroStock(data) {
  const container = document.getElementById("frequent-zero-table");
  const summaryEl = document.getElementById("frequent-zero-summary");
  const warningEl = document.getElementById("frequent-zero-source-warning");
  if (!container) return;
  const items = Array.isArray(data?.items) ? data.items : [];
  const summary = data?.summary || {};
  const sources = data?.sources || {};

  if (warningEl) {
    const missing = [];
    if (!sources.main_loaded) missing.push("主檔庫存");
    if (!sources.st_loaded) missing.push("ST 庫存");
    warningEl.innerHTML = missing.length
      ? `<div class="frequent-zero-warning">⚠ 尚未載入${esc(missing.join("、"))}，目前結果只能依已載入的庫存判斷，請先補齊資料。</div>`
      : "";
  }

  if (summaryEl) {
    summaryEl.innerHTML = `
      <div class="frequent-zero-stat">
        <span>符合常用門檻</span>
        <strong>${fmt(summary.common_part_count || 0)}</strong>
      </div>
      <div class="frequent-zero-stat is-zero">
        <span>庫存已歸零</span>
        <strong>${fmt(summary.zero_stock_count || 0)}</strong>
      </div>
      <div class="frequent-zero-stat is-urgent">
        <span>目前排程會用到</span>
        <strong>${fmt(summary.urgent_count || 0)}</strong>
      </div>
      <div class="frequent-zero-stat">
        <span>暫無目前排程</span>
        <strong>${fmt(summary.watch_count || 0)}</strong>
      </div>`;
  }

  if (!items.length) {
    container.innerHTML = '<div class="frequent-zero-empty">目前沒有符合條件的常用零庫存料號。</div>';
    return;
  }

  container.innerHTML = `
    <div class="frequent-zero-table-wrap">
      <table class="analytics-table frequent-zero-table">
        <thead><tr>
          <th>狀態</th>
          <th>料號／說明</th>
          <th>廠商</th>
          <th>近期使用</th>
          <th>主檔庫存</th>
          <th>ST 庫存</th>
          <th>目前排程</th>
          <th>最近使用</th>
        </tr></thead>
        <tbody>${items.map(renderFrequentZeroRow).join("")}</tbody>
      </table>
    </div>`;
}

function renderFrequentZeroRow(item) {
  const active = Boolean(item.active_demand);
  const status = active
    ? '<span class="frequent-zero-badge is-urgent">排程會用到</span>'
    : '<span class="frequent-zero-badge">庫存歸零</span>';
  const description = item.description
    ? `<div class="frequent-zero-description">${esc(item.description)}</div>`
    : "";
  const lastUsed = String(item.last_used_at || "").slice(0, 10) || "—";
  return `<tr class="${active ? "frequent-zero-row is-urgent" : "frequent-zero-row"}">
    <td>${status}</td>
    <td><strong class="frequent-zero-part">${esc(item.part_number)}</strong>${description}</td>
    <td>${esc(item.vendor || "未分類廠商")}</td>
    <td>
      <strong>${fmt(item.history_order_count || 0)} 筆訂單</strong>
      <div class="frequent-zero-muted">總用量 ${fmt(item.history_total_qty || 0)}</div>
    </td>
    <td><span class="frequent-zero-stock">${fmt(item.main_stock_qty || 0)}</span></td>
    <td><span class="frequent-zero-stock">${fmt(item.st_stock_qty || 0)}</span></td>
    <td>${renderFrequentZeroActiveUsage(item)}</td>
    <td>${esc(lastUsed)}</td>
  </tr>`;
}

function renderFrequentZeroActiveUsage(item) {
  const rows = Array.isArray(item.used_by) ? item.used_by : [];
  if (!rows.length) return '<span class="frequent-zero-muted">目前無排程</span>';
  const details = rows.map(row => {
    const model = String(row.model || "").trim() || "未指定機種";
    const meta = [
      row.code ? `批次 ${row.code}` : "",
      row.po_number ? `PO ${row.po_number}` : "",
      row.ship_date || "",
    ].filter(Boolean).join("／");
    return `<li><strong>${esc(model)}</strong> 用量 ${fmt(row.used_qty || 0)}${meta ? `<span>${esc(meta)}</span>` : ""}</li>`;
  }).join("");
  return `<details class="frequent-zero-usage">
    <summary>${fmt(item.active_order_count || 0)} 筆／需求 ${fmt(item.active_demand_qty || 0)}</summary>
    <ul>${details}</ul>
  </details>`;
}

// ── 發料歷史 ──────────────────────────────────────────────────────────────────

async function loadHistory() {
  try {
    const d = await apiJson(`/api/analytics/dispatch-history?group_by=${_historyGroupBy}`);
    renderHistoryChart(d.chart_data);
    renderHistoryTable(d.rows);
  } catch (_) {}
}

function renderHistoryChart(chartData) {
  const canvas = document.getElementById("history-chart");
  if (!canvas || typeof Chart === "undefined") return;

  if (_historyChart) _historyChart.destroy();

  const qtyDataset = chartData.datasets?.find(ds => ds.label === "總數量");
  _historyChart = new Chart(canvas, {
    type: "bar",
    data: {
      labels: chartData.labels || [],
      datasets: [{
        label: "總數量",
        data: qtyDataset?.data || [],
        backgroundColor: "#2563eb",
        borderWidth: 0,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { font: { size: 11 } } },
        y: { beginAtZero: true, ticks: { font: { size: 11 } } },
      },
    },
  });
}

function renderHistoryTable(rows) {
  const container = document.getElementById("history-table");
  if (!container) return;
  if (!rows?.length) {
    container.innerHTML = '<div class="no-shortage-msg">尚無發料紀錄</div>';
    return;
  }
  const labelHeader = _historyGroupBy === "month" ? "月份" : "機種";
  container.innerHTML = `
    <table class="analytics-table">
      <thead><tr><th>${labelHeader}</th><th>訂單數</th><th>總數量</th></tr></thead>
      <tbody>${rows.map(r => `
        <tr>
          <td>${esc(r.period || r.label || "")}</td>
          <td>${r.order_count}</td>
          <td>${fmt(r.total_qty || 0)}</td>
        </tr>`).join("")}
      </tbody>
    </table>`;
}

// ── 排程差異比對 ──────────────────────────────────────────────────────────────

async function handleScheduleDiff() {
  const input = document.getElementById("schedule-diff-input");
  if (!input) return;
  input.value = "";
  input.onchange = async () => {
    const file = input.files?.[0];
    if (!file) return;
    const formData = new FormData();
    formData.append("file", file);
    try {
      const resp = await apiFetch("/api/analytics/schedule-diff", {
        method: "POST",
        body: formData,
      });
      _diffResult = await resp.json();
      renderDiffResult(_diffResult);
    } catch (e) {
      showToast("比對失敗：" + e.message);
    }
  };
  input.click();
}

const DIFF_TYPE_LABELS = {
  added: { text: "新增", cls: "diff-added" },
  removed: { text: "移除", cls: "diff-removed" },
  changed: { text: "變更", cls: "diff-changed" },
};

function renderDiffResult(result) {
  const container = document.getElementById("schedule-diff-result");
  const applyBtn = document.getElementById("btn-diff-apply");
  if (!container) return;

  const diffs = result.diffs || [];
  const summary = result.summary || {};

  if (!diffs.length) {
    container.innerHTML = '<div class="no-shortage-msg">排程表無差異</div>';
    if (applyBtn) applyBtn.style.display = "none";
    return;
  }

  let html = `<div class="diff-summary">
    <span class="diff-badge diff-added">新增 ${summary.added || 0}</span>
    <span class="diff-badge diff-removed">移除 ${summary.removed || 0}</span>
    <span class="diff-badge diff-changed">變更 ${summary.changed || 0}</span>
    <span class="diff-badge diff-unchanged">不變 ${summary.unchanged || 0}</span>
  </div>`;

  html += '<div class="diff-table-wrap"><table class="analytics-table"><thead><tr>';
  html += "<th>狀態</th><th>PO</th><th>機種</th><th>PCB</th><th>異動內容</th>";
  html += "</tr></thead><tbody>";

  for (const diff of diffs) {
    const typeInfo = DIFF_TYPE_LABELS[diff.type] || { text: diff.type, cls: "" };
    const badge = `<span class="diff-type-badge ${typeInfo.cls}">${typeInfo.text}</span>`;
    let detail = "";

    if (diff.type === "added") {
      detail = `數量 ${fmt(diff.new_qty || 0)}，交期 ${esc(diff.new_date || "—")}`;
    } else if (diff.type === "removed") {
      detail = `原數量 ${fmt(diff.old_qty || 0)}，原交期 ${esc(diff.old_date || "—")}`;
    } else if (diff.type === "changed") {
      detail = (diff.changes || []).map(c =>
        `${esc(c.label)}: ${esc(String(c.old || "—"))} → ${esc(String(c.new || "—"))}`
      ).join("、");
    }

    html += `<tr>
      <td>${badge}</td>
      <td>${diff.po_number}</td>
      <td>${esc(diff.model || "")}</td>
      <td>${esc(diff.pcb || "")}</td>
      <td>${detail}</td>
    </tr>`;
  }

  html += "</tbody></table></div>";
  container.innerHTML = html;
  if (applyBtn) applyBtn.style.display = "inline-flex";
}

async function handleDiffApply() {
  if (!confirm("確定要套用新排程表？這會取代目前待處理的排程。")) return;

  const input = document.getElementById("schedule-diff-input");
  const file = input?.files?.[0];
  if (!file) {
    showToast("找不到排程檔案，請重新比對");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);
  try {
    await apiFetch("/api/schedule/upload", { method: "POST", body: formData });
    showToast("排程已更新", { tone: "success" });
    _diffResult = null;
    const container = document.getElementById("schedule-diff-result");
    if (container) container.innerHTML = '<div class="no-shortage-msg">已套用，請回排程頁確認</div>';
    document.getElementById("btn-diff-apply").style.display = "none";
  } catch (e) {
    showToast("套用失敗：" + e.message);
  }
}
