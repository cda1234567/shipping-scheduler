import { apiJson, esc } from "./api.js";

let _bound = false;
let _appMeta = null;

function formatReleaseDate(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.replaceAll("-", "/");
}

function buildSectionHtml(section) {
  const title = esc(section?.title || "");
  const items = Array.isArray(section?.items) ? section.items : [];
  const itemHtml = items.map(item => `<li>${esc(item || "")}</li>`).join("");
  return `
    <section class="version-section">
      <h4>${title}</h4>
      <ul>${itemHtml}</ul>
    </section>`;
}

function renderVersionInfo(meta) {
  const versionButton = document.getElementById("btn-app-version");
  const versionLabel = document.getElementById("app-version-label");
  const modal = document.getElementById("version-info-modal");
  const title = document.getElementById("version-info-title");
  const subhead = document.getElementById("version-info-subhead");
  const content = document.getElementById("version-info-content");

  if (!versionButton || !versionLabel || !modal || !title || !subhead || !content) return;

  const version = String(meta?.version || "").trim();
  const appName = String(meta?.app_name || "").trim() || "OpenText 出貨排程系統";
  const releaseDate = formatReleaseDate(meta?.released_at);
  const headline = String(meta?.headline || "").trim();
  const sections = Array.isArray(meta?.sections) ? meta.sections : [];

  versionLabel.textContent = version || "版本資訊";
  versionButton.style.display = "inline-flex";
  versionButton.title = version ? `${version} 更新內容` : "更新內容";

  title.textContent = version ? `${appName} ${version}` : appName;
  subhead.textContent = [releaseDate, headline].filter(Boolean).join("  |  ");
  content.innerHTML = sections.map(buildSectionHtml).join("");

  if (version) {
    document.title = `${appName} ${version}`;
  } else {
    document.title = appName;
  }
}

function openVersionInfoModal() {
  const modal = document.getElementById("version-info-modal");
  if (!modal || !_appMeta) return;
  modal.style.display = "flex";
}

function closeVersionInfoModal() {
  const modal = document.getElementById("version-info-modal");
  if (!modal) return;
  modal.style.display = "none";
}

function bindVersionInfoEvents() {
  if (_bound) return;
  _bound = true;

  document.getElementById("btn-app-version")?.addEventListener("click", openVersionInfoModal);
  document.getElementById("version-info-close")?.addEventListener("click", closeVersionInfoModal);
  document.getElementById("version-info-ok")?.addEventListener("click", closeVersionInfoModal);
  document.getElementById("version-info-modal")?.addEventListener("click", event => {
    if (event.target?.id === "version-info-modal") {
      closeVersionInfoModal();
    }
  });
}

export async function initVersionInfo() {
  bindVersionInfoEvents();
  _appMeta = await apiJson("/api/system/app-meta");
  renderVersionInfo(_appMeta);
  return _appMeta;
}
