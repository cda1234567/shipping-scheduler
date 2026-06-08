import { apiFetch, apiJson, apiPut, esc, fmt, showToast } from "./api.js";
import { desktopDownload, showDownloadToast } from "./desktop_bridge.js";

let _shipments = [];
let _current = null;
let _packingSpecs = [];
let _packingLoaded = false;
let _hscCodes = [];
let _hscLoaded = false;

export async function initSeaFreight() {
  document.getElementById("btn-sea-refresh")?.addEventListener("click", () => refreshSeaFreight());
  document.getElementById("btn-sea-upload")?.addEventListener("click", () => {
    document.getElementById("sea-file-input")?.click();
  });
  document.getElementById("sea-file-input")?.addEventListener("change", handleUpload);
  document.getElementById("btn-sea-save")?.addEventListener("click", handleSave);
  document.getElementById("btn-sea-export")?.addEventListener("click", handleExport);
  document.getElementById("btn-sea-packing")?.addEventListener("click", openPackingPanel);
  document.getElementById("btn-sea-packing-close")?.addEventListener("click", closePackingPanel);
  document.getElementById("btn-sea-packing-add")?.addEventListener("click", addPackingRow);
  document.getElementById("sea-packing-search")?.addEventListener("input", renderPackingSpecs);
  document.getElementById("btn-sea-hsc")?.addEventListener("click", openHscPanel);
  document.getElementById("btn-sea-hsc-close")?.addEventListener("click", closeHscPanel);
  document.getElementById("btn-sea-hsc-add")?.addEventListener("click", addHscRow);
  document.getElementById("sea-hsc-search")?.addEventListener("input", renderHscCodes);
}

export async function refreshSeaFreight() {
  const list = document.getElementById("sea-shipment-list");
  if (!list) return;
  try {
    const data = await apiJson("/api/sea-freight/shipments");
    _shipments = data.shipments || [];
    renderShipmentList();
    if (_current?.id) {
      await loadShipment(_current.id, { quiet: true });
    }
  } catch (error) {
    list.innerHTML = '<div class="no-shortage-msg">海運批次載入失敗</div>';
  }
}

async function handleUpload(event) {
  const file = event.target.files?.[0];
  if (!file) return;
  const form = new FormData();
  form.append("file", file);
  try {
    showToast("海運檔案上傳中...");
    const resp = await apiFetch("/api/sea-freight/upload", { method: "POST", body: form });
    const data = await resp.json();
    showToast(`已匯入海運 ${data.item_count || 0} 筆`, { tone: "success" });
    await refreshSeaFreight();
    if (data.shipment_id) await loadShipment(data.shipment_id);
  } catch (error) {
    showToast(`海運匯入失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    event.target.value = "";
  }
}

function renderShipmentList() {
  const list = document.getElementById("sea-shipment-list");
  if (!list) return;
  if (!_shipments.length) {
    list.innerHTML = '<div class="no-shortage-msg">尚無海運批次</div>';
    return;
  }
  list.innerHTML = _shipments.map(row => `
    <button class="sea-shipment-card ${_current?.id === row.id ? "active" : ""}" type="button" data-id="${row.id}">
      <span class="sea-shipment-main">${esc(row.filename || `海運批次 #${row.id}`)}</span>
      <span class="sea-shipment-meta">${esc(row.customer || "-")} · ${esc(row.cust_po || "-")}</span>
      <span class="sea-shipment-meta">${row.item_count || 0} 筆 · ${fmt(Number(row.total_boxes || 0))} 箱</span>
    </button>
  `).join("");
  list.querySelectorAll(".sea-shipment-card").forEach(btn => {
    btn.addEventListener("click", () => loadShipment(Number(btn.dataset.id || 0)));
  });
}

async function loadShipment(id, options = {}) {
  if (!id) return;
  try {
    const data = await apiJson(`/api/sea-freight/shipments/${id}`);
    _current = data.shipment;
    renderShipmentList();
    renderEditor();
  } catch (error) {
    if (!options.quiet) showToast(`載入海運批次失敗：${error.message}`);
  }
}

function renderEditor() {
  const empty = document.getElementById("sea-empty");
  const editor = document.getElementById("sea-editor");
  if (!_current || !editor) {
    if (empty) empty.style.display = "";
    if (editor) editor.style.display = "none";
    return;
  }
  if (empty) empty.style.display = "none";
  editor.style.display = "";

  setValue("sea-customer", _current.customer || "");
  setValue("sea-cust-po", _current.cust_po || "");
  setValue("sea-shipment-date", _current.shipment_date || "");
  setValue("sea-delivery-date", _current.delivery_date || "");
  setValue("sea-invoice-no", _current.invoice_no || "");
  setValue("sea-mark-text", _current.mark_text || "HILLIARD");

  const items = _current.items || [];
  const totalBoxes = items.reduce((sum, item) => sum + Number(item.box_count || 0), 0);
  const totalAmount = items.reduce((sum, item) => sum + Number(item.qty || 0) * Number(item.price || 0), 0);
  const missing = items.filter(item => item.match_status !== "matched").length;
  const noHsc = items.filter(item => !(item.harmonized_code || "").trim()).length;
  document.getElementById("sea-summary").innerHTML = `
    <span>品項 ${items.length}</span>
    <span>箱數 ${fmt(totalBoxes)}</span>
    <span>金額 ${fmt(totalAmount)}</span>
    <span class="${missing ? "sea-warn" : ""}">未匹配 ${missing}</span>
    <span class="${noHsc ? "sea-warn" : ""}">未填 HSC ${noHsc}</span>
  `;

  const body = document.getElementById("sea-items-body");
  body.innerHTML = items.map((item, index) => `
    <tr data-index="${index}">
      <td>${index + 1}</td>
      <td>${esc(item.item_no || "")}</td>
      <td><input data-field="packing_name" type="text" value="${esc(item.packing_name || "")}"></td>
      <td><input data-field="cust_po" type="text" value="${esc(item.cust_po || "")}"></td>
      <td><input data-field="qty" type="number" step="any" value="${Number(item.qty || 0)}"></td>
      <td><input data-field="price" type="number" step="any" value="${Number(item.price || 0)}"></td>
      <td><input data-field="carton_no" type="text" value="${esc(item.carton_no || "")}" placeholder="手動輸入"></td>
      <td><input data-field="box_count" type="number" step="1" min="0" value="${Number(item.box_count || 0)}"></td>
      <td><input data-field="per_box_qty" type="number" step="any" value="${Number(item.per_box_qty || 0)}"></td>
      <td><input data-field="net_weight" type="number" step="any" value="${Number(item.net_weight || 0)}"></td>
      <td><input data-field="gross_weight" type="number" step="any" value="${Number(item.gross_weight || 0)}"></td>
      <td><input data-field="volume" type="number" step="any" value="${Number(item.volume || 0)}"></td>
      <td><input data-field="harmonized_code" type="text" value="${esc(item.harmonized_code || "")}"></td>
      <td>${item.match_status === "matched" ? '<span class="badge-ok">已匹配</span>' : '<span class="badge-shortage">未匹配</span>'}</td>
    </tr>
  `).join("");
  body.querySelectorAll("input").forEach(input => {
    input.addEventListener("change", syncEditorFromDom);
  });
}

function setValue(id, value) {
  const el = document.getElementById(id);
  if (el) el.value = value;
}

function getValue(id) {
  return document.getElementById(id)?.value || "";
}

function syncEditorFromDom() {
  if (!_current) return;
  const rows = document.querySelectorAll("#sea-items-body tr[data-index]");
  rows.forEach(row => {
    const item = _current.items[Number(row.dataset.index || 0)];
    if (!item) return;
    row.querySelectorAll("input[data-field]").forEach(input => {
      const field = input.dataset.field;
      const numeric = ["qty", "price", "box_count", "per_box_qty", "net_weight", "gross_weight", "volume"].includes(field);
      item[field] = numeric ? Number(input.value || 0) : input.value;
    });
    const perBox = Number(item.per_box_qty || 0);
    const qty = Number(item.qty || 0);
    item.tail_qty = perBox > 0 ? qty % perBox : 0;
  });
  renderEditor();
}

function buildPayload() {
  syncEditorFromDom();
  return {
    customer: getValue("sea-customer"),
    cust_po: getValue("sea-cust-po"),
    shipment_date: getValue("sea-shipment-date"),
    delivery_date: getValue("sea-delivery-date"),
    invoice_no: getValue("sea-invoice-no"),
    mark_text: getValue("sea-mark-text") || "HILLIARD",
    maker: "Andy",
    items: _current?.items || [],
  };
}

async function handleSave() {
  if (!_current?.id) return;
  try {
    const data = await apiPut(`/api/sea-freight/shipments/${_current.id}`, buildPayload());
    _current = data.shipment;
    showToast("海運批次已儲存", { tone: "success" });
    await refreshSeaFreight();
    renderEditor();
  } catch (error) {
    showToast(`儲存失敗：${error.message}`, { tone: "error" });
  }
}

async function handleExport() {
  if (!_current?.id) return;
  try {
    await handleSave();
    const result = await desktopDownload({
      path: `/api/sea-freight/shipments/${_current.id}/export`,
      method: "POST",
    });
    showDownloadToast(result, "海運出貨單");
  } catch (error) {
    showToast(`匯出失敗：${error.message}`, { tone: "error", sticky: true });
  }
}

async function openPackingPanel() {
  const panel = document.getElementById("sea-packing-panel");
  if (panel) panel.style.display = "";
  if (!_packingLoaded) await loadPackingSpecs();
}

function closePackingPanel() {
  const panel = document.getElementById("sea-packing-panel");
  if (panel) panel.style.display = "none";
}

async function loadPackingSpecs() {
  const body = document.getElementById("sea-packing-body");
  if (body) body.innerHTML = '<tr><td colspan="8">載入中...</td></tr>';
  try {
    const data = await apiJson("/api/sea-freight/packing-specs");
    _packingSpecs = data.specs || [];
    _packingLoaded = true;
    renderPackingSpecs();
  } catch (error) {
    if (body) body.innerHTML = '<tr><td colspan="8">包裝主檔載入失敗</td></tr>';
    showToast(`包裝主檔載入失敗：${error.message}`, { tone: "error" });
  }
}

function renderPackingSpecs() {
  const body = document.getElementById("sea-packing-body");
  if (!body) return;
  const query = (document.getElementById("sea-packing-search")?.value || "").trim().toLowerCase();
  const specs = _packingSpecs.filter(spec => {
    if (!query) return true;
    return [spec.item_no, spec.packing_name, spec.vendor].some(value => String(value || "").toLowerCase().includes(query));
  });
  if (!specs.length) {
    body.innerHTML = '<tr><td colspan="8">沒有符合的包裝資料</td></tr>';
    return;
  }
  body.innerHTML = specs.map((spec, index) => `
    <tr data-index="${_packingSpecs.indexOf(spec)}">
      <td><input data-field="item_no" type="text" value="${esc(spec.item_no || "")}" ${spec._isNew ? "" : "readonly"}></td>
      <td><input data-field="packing_name" type="text" value="${esc(spec.packing_name || "")}"></td>
      <td><input data-field="per_box_qty" type="number" step="any" value="${Number(spec.per_box_qty || 0)}"></td>
      <td><input data-field="net_weight" type="number" step="any" value="${Number(spec.net_weight || 0)}"></td>
      <td><input data-field="gross_weight" type="number" step="any" value="${Number(spec.gross_weight || 0)}"></td>
      <td><input data-field="volume" type="number" step="any" value="${Number(spec.volume || 0)}"></td>
      <td><input data-field="vendor" type="text" value="${esc(spec.vendor || "")}"></td>
      <td>
        <button class="btn btn-primary btn-xs" type="button" data-action="save">存</button>
        <button class="btn btn-danger btn-xs" type="button" data-action="delete">刪</button>
      </td>
    </tr>
  `).join("");
  body.querySelectorAll("button[data-action='save']").forEach(btn => {
    btn.addEventListener("click", () => savePackingRow(btn.closest("tr")));
  });
  body.querySelectorAll("button[data-action='delete']").forEach(btn => {
    btn.addEventListener("click", () => deletePackingRow(btn.closest("tr")));
  });
}

function addPackingRow() {
  _packingSpecs.unshift({
    item_no: "",
    packing_name: "",
    per_box_qty: 0,
    net_weight: 0,
    gross_weight: 0,
    volume: 0,
    vendor: "",
    _isNew: true,
  });
  const search = document.getElementById("sea-packing-search");
  if (search) search.value = "";
  renderPackingSpecs();
}

function readPackingRow(row) {
  const spec = {};
  row.querySelectorAll("input[data-field]").forEach(input => {
    const field = input.dataset.field;
    const numeric = ["per_box_qty", "net_weight", "gross_weight", "volume"].includes(field);
    spec[field] = numeric ? Number(input.value || 0) : input.value.trim();
  });
  return spec;
}

async function savePackingRow(row) {
  if (!row) return;
  const index = Number(row.dataset.index || 0);
  const spec = readPackingRow(row);
  if (!spec.item_no) {
    showToast("請輸入 ITEM NO", { tone: "error" });
    return;
  }
  try {
    await apiPut(`/api/sea-freight/packing-specs/${encodeURIComponent(spec.item_no)}`, spec);
    _packingSpecs[index] = { ...spec };
    showToast(`${spec.item_no} 包裝主檔已儲存`, { tone: "success" });
    await loadPackingSpecs();
  } catch (error) {
    showToast(`儲存包裝主檔失敗：${error.message}`, { tone: "error" });
  }
}

async function deletePackingRow(row) {
  if (!row) return;
  const spec = readPackingRow(row);
  if (!spec.item_no) {
    _packingSpecs.splice(Number(row.dataset.index || 0), 1);
    renderPackingSpecs();
    return;
  }
  if (!confirm(`刪除 ${spec.item_no} 的包裝主檔？`)) return;
  try {
    await apiFetch(`/api/sea-freight/packing-specs/${encodeURIComponent(spec.item_no)}`, { method: "DELETE" });
    showToast(`${spec.item_no} 包裝主檔已刪除`, { tone: "success" });
    await loadPackingSpecs();
  } catch (error) {
    showToast(`刪除包裝主檔失敗：${error.message}`, { tone: "error" });
  }
}

async function openHscPanel() {
  const panel = document.getElementById("sea-hsc-panel");
  if (panel) panel.style.display = "";
  if (!_hscLoaded) await loadHscCodes();
}

function closeHscPanel() {
  const panel = document.getElementById("sea-hsc-panel");
  if (panel) panel.style.display = "none";
}

async function loadHscCodes() {
  const body = document.getElementById("sea-hsc-body");
  if (body) body.innerHTML = '<tr><td colspan="4">載入中...</td></tr>';
  try {
    const data = await apiJson("/api/sea-freight/hsc-codes");
    _hscCodes = data.codes || [];
    _hscLoaded = true;
    renderHscCodes();
  } catch (error) {
    if (body) body.innerHTML = '<tr><td colspan="4">HSC 主檔載入失敗</td></tr>';
    showToast(`HSC 主檔載入失敗：${error.message}`, { tone: "error" });
  }
}

function renderHscCodes() {
  const body = document.getElementById("sea-hsc-body");
  if (!body) return;
  const query = (document.getElementById("sea-hsc-search")?.value || "").trim().toLowerCase();
  const codes = _hscCodes.filter(row => {
    if (!query) return true;
    return [row.item_no, row.harmonized_code, row.note].some(value => String(value || "").toLowerCase().includes(query));
  });
  if (!codes.length) {
    body.innerHTML = '<tr><td colspan="4">沒有符合的 HSC 資料</td></tr>';
    return;
  }
  body.innerHTML = codes.map(row => `
    <tr data-index="${_hscCodes.indexOf(row)}">
      <td><input data-field="item_no" type="text" value="${esc(row.item_no || "")}" ${row._isNew ? "" : "readonly"}></td>
      <td><input data-field="harmonized_code" type="text" value="${esc(row.harmonized_code || "")}"></td>
      <td><input data-field="note" type="text" value="${esc(row.note || "")}"></td>
      <td>
        <button class="btn btn-primary btn-xs" type="button" data-action="save">存</button>
        <button class="btn btn-danger btn-xs" type="button" data-action="delete">刪</button>
      </td>
    </tr>
  `).join("");
  body.querySelectorAll("button[data-action='save']").forEach(btn => {
    btn.addEventListener("click", () => saveHscRow(btn.closest("tr")));
  });
  body.querySelectorAll("button[data-action='delete']").forEach(btn => {
    btn.addEventListener("click", () => deleteHscRow(btn.closest("tr")));
  });
}

function addHscRow() {
  _hscCodes.unshift({ item_no: "", harmonized_code: "", note: "", _isNew: true });
  const search = document.getElementById("sea-hsc-search");
  if (search) search.value = "";
  renderHscCodes();
}

function readHscRow(row) {
  const data = {};
  row.querySelectorAll("input[data-field]").forEach(input => {
    data[input.dataset.field] = input.value.trim();
  });
  return data;
}

async function saveHscRow(row) {
  if (!row) return;
  const index = Number(row.dataset.index || 0);
  const data = readHscRow(row);
  if (!data.item_no) {
    showToast("請輸入 ITEM NO", { tone: "error" });
    return;
  }
  try {
    await apiPut(`/api/sea-freight/hsc-codes/${encodeURIComponent(data.item_no)}`, data);
    _hscCodes[index] = { ...data };
    showToast(`${data.item_no} HSC 已儲存`, { tone: "success" });
    await loadHscCodes();
  } catch (error) {
    showToast(`儲存 HSC 失敗：${error.message}`, { tone: "error" });
  }
}

async function deleteHscRow(row) {
  if (!row) return;
  const data = readHscRow(row);
  if (!data.item_no) {
    _hscCodes.splice(Number(row.dataset.index || 0), 1);
    renderHscCodes();
    return;
  }
  if (!confirm(`刪除 ${data.item_no} 的 HSC？`)) return;
  try {
    await apiFetch(`/api/sea-freight/hsc-codes/${encodeURIComponent(data.item_no)}`, { method: "DELETE" });
    showToast(`${data.item_no} HSC 已刪除`, { tone: "success" });
    await loadHscCodes();
  } catch (error) {
    showToast(`刪除 HSC 失敗：${error.message}`, { tone: "error" });
  }
}
