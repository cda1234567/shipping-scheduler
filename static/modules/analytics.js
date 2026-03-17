import { apiJson, apiFetch, showToast, esc, fmt } from "./api.js";

let _trendChart = null;
let _historyChart = null;
let _trendPeriod = "month";
let _historyGroupBy = "model";
let _diffResult = null;

const CHART_COLORS = [
  "#2563eb", "#dc2626", "#16a34a", "#f59e0b", "#8b5cf6",
  "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1",
  "#14b8a6", "#e11d48", "#a855f7", "#0ea5e9", "#eab308",
];

export async function initAnalytics() {
  document.getElementById("trend-period-select")?.addEventListener("change", (e) => {
    _trendPeriod = e.target.value;
    loadTrend();
  });
  document.getElementById("history-group-select")?.addEventListener("change", (e) => {
    _historyGroupBy = e.target.value;
    loadHistory();
  });
  document.getElementById("btn-schedule-diff")?.addEventListener("click", handleScheduleDiff);
  document.getElementById("btn-diff-apply")?.addEventListener("click", handleDiffApply);
}

export async function refreshAnalytics() {
  await Promise.all([loadTrend(), loadTopParts(), loadHistory()]);
}

// ── 發料趨勢 ──────────────────────────────────────────────────────────────────

async function loadTrend() {
  try {
    const d = await apiJson(`/api/analytics/dispatch-trend?period=${_trendPeriod}`);
    renderTrendChart(d.chart_data);
  } catch (_) {}
}

function renderTrendChart(chartData) {
  const canvas = document.getElementById("trend-chart");
  if (!canvas || typeof Chart === "undefined") return;

  if (_trendChart) _trendChart.destroy();

  const datasets = (chartData.datasets || []).map((ds, i) => ({
    label: ds.label,
    data: ds.data,
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
    borderWidth: 0,
  }));

  _trendChart = new Chart(canvas, {
    type: "bar",
    data: { labels: chartData.labels || [], datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "bottom", labels: { boxWidth: 12, font: { size: 11 } } },
      },
      scales: {
        x: { stacked: true, ticks: { font: { size: 11 } } },
        y: { stacked: true, beginAtZero: true, ticks: { font: { size: 11 } } },
      },
    },
  });
}

async function loadTopParts() {
  const container = document.getElementById("top-parts-table");
  if (!container) return;
  try {
    const d = await apiJson("/api/analytics/top-parts?limit=20&months=6");
    const parts = d.parts || [];
    if (!parts.length) {
      container.innerHTML = '<div class="no-shortage-msg">尚無發料紀錄</div>';
      return;
    }
    container.innerHTML = `
      <table class="analytics-table">
        <thead><tr>
          <th>#</th><th>料號</th><th>總用量</th><th>訂單數</th><th>ST 庫存</th>
        </tr></thead>
        <tbody>${parts.map((p, i) => `
          <tr>
            <td>${i + 1}</td>
            <td>${esc(p.part_number)}</td>
            <td>${fmt(p.total_qty)}</td>
            <td>${p.order_count}</td>
            <td>${p.has_st_stock
              ? `<span class="badge-ok" style="padding:2px 8px;border-radius:10px;font-size:11px">${fmt(p.st_stock_qty)}</span>`
              : '<span class="badge-shortage" style="padding:2px 8px;border-radius:10px;font-size:11px">無</span>'
            }</td>
          </tr>`).join("")}
        </tbody>
      </table>`;
  } catch (_) {
    container.innerHTML = '<div class="no-shortage-msg">載入失敗</div>';
  }
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
