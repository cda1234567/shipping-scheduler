import { apiJson, apiFetch, showToast, esc } from "./api.js";

let _onRefresh = null;
let _outerSortable = null;
const _innerSortables = [];

export function initBomManager(onRefreshCallback) {
  _onRefresh = onRefreshCallback;

  const uploadArea = document.getElementById("bom-upload-area");
  const bomInput = document.getElementById("bom-file-input");

  uploadArea.addEventListener("click", () => bomInput.click());
  uploadArea.addEventListener("dragover", e => { e.preventDefault(); uploadArea.style.borderColor = "#007aff"; });
  uploadArea.addEventListener("dragleave", () => { uploadArea.style.borderColor = ""; });
  uploadArea.addEventListener("drop", async e => {
    e.preventDefault();
    uploadArea.style.borderColor = "";
    if (e.dataTransfer.files.length) await uploadBom(e.dataTransfer.files);
  });
  bomInput.addEventListener("change", async e => {
    if (e.target.files.length) await uploadBom(e.target.files);
    bomInput.value = "";
  });

  renderBomGroups();
}

async function uploadBom(files) {
  const fd = new FormData();
  for (const f of files) fd.append("files", f);
  const groupModel = (document.getElementById("bom-group-model")?.value || "").trim();
  if (groupModel) fd.append("group_model", groupModel);
  try {
    const d = await (await fetch("/api/bom/upload", { method: "POST", body: fd })).json();
    const saved = d.saved || [];
    const errors = d.errors || [];
    let msg = `已上傳 ${saved.length} 個 BOM`;
    const csCount = saved.reduce((s, b) => s + (b.customer_supplied_count || 0), 0);
    if (csCount) msg += `（含 ${csCount} 個客供料）`;
    if (errors.length) {
      msg += `，${errors.length} 個失敗`;
      alert("BOM 上傳失敗：\n" + errors.join("\n"));
    }
    showToast(msg);
    await renderBomGroups();
    if (_onRefresh) await _onRefresh();
  } catch (e) { showToast("上傳失敗：" + e.message); }
}

export async function renderBomGroups() {
  const container = document.getElementById("bom-group-list");
  if (!container) return;

  if (_outerSortable) { _outerSortable.destroy(); _outerSortable = null; }
  _innerSortables.forEach(s => s.destroy());
  _innerSortables.length = 0;

  try {
    const d = await apiJson("/api/bom/list");
    const groups = d.groups || [];

    if (!groups.length) {
      container.innerHTML = '<div class="empty-state">尚未上傳 BOM 檔案</div>';
      return;
    }

    container.innerHTML = groups.map(g => `
      <div class="bom-model-group" data-model="${esc(g.model)}">
        <div class="bom-model-header">
          <span class="drag-handle-model" title="拖曳調整機種順序">⠿</span>
          <span class="bom-model-name">${esc(g.model)}</span>
          <span class="bom-model-count">${g.items.length} 份 BOM</span>
        </div>
        <ul class="bom-items-list">
          ${g.items.map(b => _itemHtml(b)).join("")}
        </ul>
      </div>`).join("");

    if (typeof Sortable !== "undefined") {
      _outerSortable = Sortable.create(container, {
        animation: 150, handle: ".drag-handle-model", ghostClass: "sortable-ghost",
      });

      container.querySelectorAll(".bom-items-list").forEach(ul => {
        const s = Sortable.create(ul, {
          animation: 150, handle: ".drag-handle-item", ghostClass: "sortable-ghost", group: "bom-items",
        });
        _innerSortables.push(s);
      });
    }

    container.querySelectorAll(".bom-del").forEach(btn => {
      btn.addEventListener("click", () => deleteBom(btn.dataset.id));
    });
  } catch (_) {}
}

function _itemHtml(b) {
  const date = (b.uploaded_at || "").slice(5, 10);
  const csInfo = b.customer_supplied_count ? `<span class="tag tag-cs" style="font-size:9px;padding:0 4px">${b.customer_supplied_count}客供</span>` : "";
  return `<li class="bom-item-row" data-id="${b.id}">
    <span class="drag-handle-item" title="拖曳調整順序">⠿</span>
    <span class="tag tag-pcb">${esc(b.pcb)}</span>
    <span class="bom-item-filename" title="${esc(b.filename)}">${esc(b.filename)}</span>
    ${csInfo}
    <span class="bom-item-info">${b.components} 料號${date ? " · " + date : ""}</span>
    <button class="bom-del" data-id="${b.id}" title="刪除">✕</button>
  </li>`;
}

async function deleteBom(id) {
  if (!confirm("確定刪除此 BOM？")) return;
  try {
    await apiFetch(`/api/bom/${id}`, { method: "DELETE" });
    await renderBomGroups();
    if (_onRefresh) await _onRefresh();
    showToast("已刪除");
  } catch (e) { showToast("刪除失敗：" + e.message); }
}

export async function renderBomSidebar() {
  // 保留向後相容，但不再使用
}
