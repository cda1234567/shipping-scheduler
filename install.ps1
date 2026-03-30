# ═══════════════════════════════════════════════════════════
#   出貨排程系統 — Windows 一鍵安裝（含 Docker 自動安裝）
#
#   對方只需在 PowerShell 執行一行：
#   irm https://raw.githubusercontent.com/cda1234567/shipping-scheduler/main/install.ps1 | iex
# ═══════════════════════════════════════════════════════════

$ErrorActionPreference = "Stop"
$APP_DIR = Join-Path $HOME "dispatch-scheduler"

function Write-Step($msg) { Write-Host "  ✔ $msg" -ForegroundColor Green }
function Write-Info($msg) { Write-Host "  $msg" -ForegroundColor White }
function Write-Warn($msg) { Write-Host "  ⚠ $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "  ══════════════════════════════════════" -ForegroundColor Cyan
Write-Host "    出貨排程系統 — 一鍵安裝" -ForegroundColor Cyan
Write-Host "  ══════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── 1. 檢查 / 安裝 Docker Desktop ──
$dockerReady = $false
try {
    $null = & docker version 2>&1
    if ($LASTEXITCODE -eq 0) { $dockerReady = $true }
} catch {}

if (-not $dockerReady) {
    Write-Warn "找不到 Docker，開始自動安裝 Docker Desktop..."
    Write-Host ""

    # 優先用 winget
    $useWinget = $false
    try {
        $null = & winget --version 2>&1
        if ($LASTEXITCODE -eq 0) { $useWinget = $true }
    } catch {}

    if ($useWinget) {
        Write-Info "透過 winget 安裝 Docker Desktop..."
        winget install -e --id Docker.DockerDesktop --accept-source-agreements --accept-package-agreements
    } else {
        # 手動下載安裝
        $installerUrl = "https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe"
        $installerPath = Join-Path $env:TEMP "DockerDesktopInstaller.exe"
        Write-Info "下載 Docker Desktop 安裝檔..."
        Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath -UseBasicParsing
        Write-Info "執行安裝（可能需要幾分鐘）..."
        Start-Process -FilePath $installerPath -ArgumentList "install", "--quiet", "--accept-license" -Wait
        Remove-Item $installerPath -ErrorAction SilentlyContinue
    }

    Write-Host ""
    Write-Host "  ══════════════════════════════════════" -ForegroundColor Yellow
    Write-Host "    Docker Desktop 已安裝！" -ForegroundColor Yellow
    Write-Host "    請完成以下步驟：" -ForegroundColor Yellow
    Write-Host "    1. 重新啟動電腦" -ForegroundColor White
    Write-Host "    2. 開啟 Docker Desktop 並等待啟動完成" -ForegroundColor White
    Write-Host "    3. 重新執行此安裝腳本" -ForegroundColor White
    Write-Host "  ══════════════════════════════════════" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "  按 Enter 結束"
    exit 0
}

Write-Step "Docker 已就緒"

# ── 2. 檢查 Docker Compose ──
try {
    $null = & docker compose version 2>&1
    if ($LASTEXITCODE -ne 0) { throw "no compose" }
} catch {
    Write-Host "  ❌ 找不到 Docker Compose，請更新 Docker Desktop" -ForegroundColor Red
    Read-Host "  按 Enter 結束"
    exit 1
}

Write-Step "Docker Compose 已就緒"

# ── 3. 建立目錄 ──
if (-not (Test-Path $APP_DIR)) {
    New-Item -ItemType Directory -Path $APP_DIR | Out-Null
}
if (-not (Test-Path "$APP_DIR\data")) {
    New-Item -ItemType Directory -Path "$APP_DIR\data" | Out-Null
}
Set-Location $APP_DIR
Write-Step "目錄: $APP_DIR"

# ── 4. 寫入 docker-compose.yml ──
@'
name: dispatch-scheduler

services:
  dispatch-scheduler:
    image: ${APP_IMAGE:-ghcr.io/cda1234567/dispatch-scheduler:latest}
    container_name: ${APP_CONTAINER_NAME:-dispatch-scheduler}
    restart: unless-stopped
    ports:
      - "${APP_PORT:-8765}:8765"
    volumes:
      - ./data:/app/data
    labels:
      - "com.centurylinklabs.watchtower.enable=true"
      - "com.centurylinklabs.watchtower.scope=${WATCHTOWER_SCOPE:-dispatch-scheduler}"

  watchtower:
    image: containrrr/watchtower:latest
    container_name: ${WATCHTOWER_CONTAINER_NAME:-dispatch-watchtower}
    restart: unless-stopped
    environment:
      DOCKER_API_VERSION: "${WATCHTOWER_DOCKER_API_VERSION:-1.44}"
    command:
      - "--label-enable"
      - "--cleanup"
      - "--rolling-restart"
      - "--scope"
      - "${WATCHTOWER_SCOPE:-dispatch-scheduler}"
      - "--interval"
      - "${WATCHTOWER_POLL_INTERVAL:-300}"
    volumes:
      - //var/run/docker.sock:/var/run/docker.sock
'@ | Set-Content -Path "docker-compose.yml" -Encoding UTF8
Write-Step "docker-compose.yml 已建立"

# ── 5. 寫入 .env（如果不存在）──
if (-not (Test-Path ".env")) {
@'
APP_IMAGE=ghcr.io/cda1234567/dispatch-scheduler:latest
APP_PORT=8765
APP_CONTAINER_NAME=dispatch-scheduler
WATCHTOWER_CONTAINER_NAME=dispatch-watchtower
WATCHTOWER_SCOPE=dispatch-scheduler
WATCHTOWER_POLL_INTERVAL=300
WATCHTOWER_DOCKER_API_VERSION=1.44
'@ | Set-Content -Path ".env" -Encoding UTF8
    Write-Step ".env 已建立"
} else {
    Write-Step ".env 已存在，跳過"
}

# ── 6. 建立桌面捷徑 ──
$desktopPath = [Environment]::GetFolderPath("Desktop")
$shortcutPath = Join-Path $desktopPath "出貨排程系統.url"
if (-not (Test-Path $shortcutPath)) {
@"
[InternetShortcut]
URL=http://localhost:8765
IconIndex=0
"@ | Set-Content -Path $shortcutPath -Encoding UTF8
    Write-Step "桌面捷徑已建立"
}

# ── 7. 啟動服務 ──
Write-Host ""
Write-Host "  🚀 拉取映像並啟動..." -ForegroundColor Yellow
docker compose pull
docker compose up -d

Write-Host ""
Write-Host "  ══════════════════════════════════════" -ForegroundColor Green
Write-Host "    ✅ 安裝完成！" -ForegroundColor Green
Write-Host "  ══════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  🌐 開啟瀏覽器: http://localhost:8765" -ForegroundColor White
Write-Host "  📁 資料目錄:   $APP_DIR\data" -ForegroundColor White
Write-Host ""
Write-Host "  🔄 系統會自動檢查更新（每 5 分鐘）" -ForegroundColor White
Write-Host ""
Write-Host "  常用指令（在 $APP_DIR 目錄下執行）：" -ForegroundColor Gray
Write-Host "    查看狀態:  docker compose ps" -ForegroundColor Gray
Write-Host "    查看日誌:  docker compose logs -f" -ForegroundColor Gray
Write-Host "    重新啟動:  docker compose restart" -ForegroundColor Gray
Write-Host "    停止服務:  docker compose down" -ForegroundColor Gray
Write-Host ""

# 自動開啟瀏覽器
Start-Sleep -Seconds 3
Start-Process "http://localhost:8765"

Read-Host "  按 Enter 結束"
