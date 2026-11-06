# 進貨單列印小幫手（Windows 主機）

網站（Docker 容器 `dispatch-scheduler-localserver`，Andy-STNB:8765）產生正航版面的進貨單 PDF：
- 單據（明細 + 合計 + 簽核列）能放進 A4 上半張（≤148.5mm）→ `NPI975BEF (HP LaserJet M111w)`（裝半張紙，A4 頁面只印上半）
- 否則 → `HP Color LaserJet M652 [7D2006]`（A4，多頁時表頭每頁重複、頁碼 n/m）

預設 **不啟用** 自動送印：按「🖨 列印」會開 PDF 分頁並提示要選哪台印表機（列印時縮放請選「實際大小」）。

## 啟用步驟（尚未在任何機器上安裝）
1. 主機安裝 Python 3 與 [SumatraPDF](https://www.sumatrapdfreader.org/)（預設路徑 `C:\Program Files\SumatraPDF\SumatraPDF.exe`）。
2. 把本資料夾複製到主機，例如 `C:\rm_print_helper\`。
3. 產生一組隨機 token（例如 PowerShell：`[guid]::NewGuid().ToString("N")`），只放在主機環境變數與容器 `.env`，不要提交到 git。
4. 主機啟動（建議用「工作排程器」設定開機 / 登入時執行，使用者需能存取這兩台印表機）：
   ```powershell
   $env:PRINT_HELPER_TOKEN = "<token>"
   python C:\rm_print_helper\print_helper.py
   ```
5. 防火牆：允許 TCP 8766 **只從 Docker/WSL 虛擬網段**連入（不要對整個 LAN 開放）。
6. 測試（主機上）：`curl http://127.0.0.1:8766/health` → `sumatra_found: true`；
   先用 `PRINT_HELPER_DRY_RUN=1` 確認容器能連到再真的印。
7. 容器 `.env` 加上後重建/重啟容器：
   ```
   RM_PRINT_HELPER_ENABLED=1
   RM_PRINT_HELPER_URL=http://host.docker.internal:8766
   RM_PRINT_HELPER_TOKEN=<token>
   ```
   （docker compose 若非 Docker Desktop，需加 `extra_hosts: ["host.docker.internal:host-gateway"]`）

## 其他可調環境變數（容器）
- `RM_PRINT_PRINTER_HALF` / `RM_PRINT_PRINTER_FULL`：印表機名稱（需與小幫手 `PRINT_HELPER_PRINTERS` 一致）
- `RM_PRINT_HALF_PAGE_MM`：半張判斷上限（預設 148.5）
- `RM_PRINT_FONT_PATH`：指定 mingliu.ttc（有授權時用，版面最接近正航；未設定時用 AR PL UMing TW + Liberation Serif 依新細明體字寬縮放）

## v2026.11.04.7 補充

- 一律 A4：`SUMATRA_SETTINGS` 預設改為 `noscale,paper=A4`。
- 來源限制：`PRINT_HELPER_ALLOW`（預設 `127.0.0.0/8,::1/128,192.168.65.0/24,172.16.0.0/12`），其他來源回 403。
  Docker Desktop 會把容器連到 `host.docker.internal` 的流量從本機 127.0.0.1 轉入，所以可設 `PRINT_HELPER_BIND=127.0.0.1`，區網其他電腦連不到。
- 開機自動啟動：用工作排程器「登入時」執行 `py -3 print_helper.py`（環境變數由啟動用的 .cmd 設定，token 不要寫進 git）。

## 按需重啟（restart_listener.py）

- 埠 `8767`，只聽 `127.0.0.1`
- `GET /health`、`POST /restart`（需 `X-Print-Token`）
- **沒有定時輪詢**；網站按列印連不到小幫手時才呼叫
- 主機排程：`ShippingScheduler-PrintHelper-RestartListener`（登入觸發）
- 啟動腳本：`run_restart_listener.ps1`（主機 `D:\tools\print_helper\`）
