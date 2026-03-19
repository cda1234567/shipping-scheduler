# OpenText 出貨排程系統

PCB/電子製造業的出貨排程管理系統，整合 BOM 領料、庫存計算、缺料預警與發料管理功能。

---

## 技術架構

| 層級 | 技術 |
|------|------|
| 後端 | FastAPI + uvicorn |
| 資料庫 | SQLite（`data/system.db`） |
| 前端 | Vanilla JS（ES modules）+ HTML/CSS |
| Excel 處理 | openpyxl + xlrd |
| 拖曳排序 | Sortable.js |
| Pydantic | v1（1.10.x） |

---

## 快速啟動

```bash
# 安裝相依套件
pip install -r requirements.txt

# 啟動伺服器
python main.py
# 或使用
run.bat
```

伺服器啟動後瀏覽 `http://localhost:8000`。

專案的 `data/`、`__pycache__/` 與本機設定不會提交到 GitHub；首次啟動時會自動建立需要的資料夾。

---

## 驗證

發布前建議至少跑一次：

```powershell
powershell -ExecutionPolicy Bypass -File .\verify.ps1
```

這會依序執行：

- Python 單元測試：`python -m unittest discover -s tests -p test_*.py`
- 前端 JavaScript 語法檢查：`node tools\check_js.mjs`

如果是在新的 terminal / 重新開啟的 Codex 視窗中，`node` 會直接可用。
GitHub 端也有 `.github/workflows/checks.yml` 會自動執行同一批檢查。
如果要從檔案總管或 `cmd` 執行，也可以用 `verify.bat`。

---

## 打包與部署

### Windows 桌面版 `.exe`

先安裝一般相依套件，再執行：

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\build_desktop_exe.ps1
```

打包完成後會輸出：

- `dist/OpenTextDesktop/`：可直接執行的桌面版資料夾
- `dist/OpenTextDesktop-win64.zip`：方便發給其他 Windows 電腦的壓縮包

桌面版會把 `data/` 建在 `.exe` 所在資料夾旁邊，適合做可攜式部署。

如果你已經把系統改成跑在 Docker / server，上述桌面版也可以改成「只連遠端、不開本機 Python」：

1. 在 `.exe` 同層放一份 `desktop_client.json`
2. 內容可參考 [desktop_client.json.example](./desktop_client.json.example)
3. 填入 server 位址，例如：

```json
{
  "server_url": "http://192.168.1.10:8765/"
}
```

之後桌面版會直接連到遠端 Docker server，不再本機啟動 FastAPI。
也可以用命令列覆蓋：

```powershell
OpenTextDesktop.exe --server-url http://192.168.1.10:8765/
```

### Docker / Server 版

如果你要丟到 server，可以直接用 Docker：

```bash
docker compose up -d --build
```

預設會：

- 對外開 `8765`
- 把本機 `./data` 掛到 container 內的 `/app/data`
- 用 `requirements-server.txt`，不會安裝桌面版的 `pywebview`

如果只想手動 build image：

```bash
docker build -t dispatch-scheduler .
docker run -d --name dispatch-scheduler -p 8765:8765 -v $(pwd)/data:/app/data dispatch-scheduler
```

### Server 自動更新版

如果 server 不方便常常手動登入更新，建議改用：

```bash
cp .env.server.example .env
docker compose -f docker-compose.server.yml up -d
```

這條線會做兩件事：

- GitHub Actions 在 `main` push 後自動發佈 GHCR image
- `watchtower` 每隔一段時間自動檢查並更新 `dispatch-scheduler`

預設設定：

- image：`ghcr.io/cda1234567/dispatch-scheduler:latest`
- port：`8765`
- 更新檢查間隔：`300` 秒

對應檔案：

- server compose：[docker-compose.server.yml](./docker-compose.server.yml)
- 環境變數範本：[.env.server.example](./.env.server.example)
- 自動發佈 workflow：[.github/workflows/docker-publish.yml](./.github/workflows/docker-publish.yml)

注意：

- `watchtower` 只能更新「registry 上的 image」，不能更新本機 `build:` 出來的 container。
- 如果 GHCR package 是 private，server 端要先 `docker login ghcr.io`。
- 如果要讓更新完全免登入，建議把 GHCR package 設成 public。

---

## 開發流程

本專案後續修改一律遵守以下流程，避免功能互相干擾或改壞後難以回退：

1. 先照規劃文件的 `Phase 1 → Phase 2 → Phase 3 → Phase 4` 順序執行，不跳 phase。
2. 需要隔離風險或準備 PR 時再開 branch，branch 名稱建議使用 `codex/<feature-name>`；若不需要，可直接在目前工作線上開發。
3. 每完成一個明確功能就先 `commit` 一次，commit message 必須清楚描述這次改動。
4. 每次改完都要先跑驗證，至少執行 `powershell -ExecutionPolicy Bypass -File .\verify.ps1`。
5. 有新邏輯就補對應測試，確認結果正確後再進下一個功能。
6. 設計規則或流程有變動時，要同步更新 repo 內文件，避免後續開發依據過期。

建議節奏：

```text
做單一功能 -> 跑測試 -> commit -> 再進下一步
```

---

## 功能模組

### 1. 主檔管理（庫存快照）

- 上傳主檔 xlsx（包含料號、庫存量、MOQ）
- 設定庫存快照作為計算基準
- 發料後自動 merge 消耗量回主檔

### 2. 排程管理

- 上傳排程表 xlsx，自動解析訂單
- 訂單卡片式 UI，支援拖曳排序
- 四階段狀態：`pending` → `merged` → `dispatched` → `completed`
- 批次 merge 與自動 merge（依交期門檻）
- 單筆發料鎖定（✓ 按鈕）
- 交期修改（已發料訂單改交期會產生提醒）

### 3. BOM 管理

- 上傳 BOM 領料單（支援 .xls / .xlsx / .xlsm）
- 自動解析料號、用量、客供料標記
- 以機種分組顯示
- 支援 `group_model` 逗號分隔多機種共用

### 4. 缺料計算（Running Balance）

- 快照庫存為基準
- 已發料消耗先扣除（隔離）
- 對 merged 訂單逐行計算 running balance
- 缺料自動建議採購量（依 MOQ 倍數向上取整）

### 5. 補料與 BOM 下載

- 批次 merge 後開啟補料彈窗
- 依機種分組顯示缺料明細
- 填入補料數量後下載 BOM 副本（補料量寫入 H 欄）

### 6. 發料單生成

- 選擇訂單後生成發料單 xlsx
- 包含料號、用量、決策等資訊

### 7. 已發料歸檔

- 已發料訂單可建立資料夾分類
- 支援拖曳/下拉移動訂單到資料夾
- 資料夾可收合、刪除（訂單移回未歸檔）

### 8. 提醒系統

- 交期變更提醒
- 客供料提醒
- 缺料警告
- 支援已讀/全部已讀

### 9. 操作日誌

- 自動記錄所有重要操作
- 含時間戳與操作細節

---

## 專案結構

```
opentext大改版/
├── main.py                  # FastAPI 應用啟動
├── config.yaml              # Excel 欄位設定
├── requirements.txt         # Python 套件
├── app/
│   ├── config.py            # 設定載入
│   ├── database.py          # SQLite CRUD
│   ├── models.py            # Pydantic models
│   ├── storage.py           # metadata 管理
│   ├── routers/             # API 路由
│   │   ├── main_file.py     # 主檔 API
│   │   ├── schedule.py      # 排程 API
│   │   ├── bom.py           # BOM API
│   │   ├── dispatch.py      # 發料單 API
│   │   ├── alerts.py        # 提醒 API
│   │   └── logs.py          # 日誌 API
│   └── services/            # 業務邏輯
│       ├── main_reader.py       # 主檔讀取
│       ├── schedule_parser.py   # 排程表解析
│       ├── bom_parser.py        # BOM 解析
│       ├── calculator.py        # 缺料計算
│       ├── merge_to_main.py     # 主檔 merge
│       ├── xls_reader.py        # xls/xlsx 統一讀取
│       └── dispatch_form_generator.py
├── static/
│   ├── index.html           # SPA 主頁
│   ├── style.css            # 樣式
│   └── modules/             # JS 模組
│       ├── api.js           # fetch wrapper
│       ├── schedule.js      # 排程 UI
│       ├── bom_manager.js   # BOM UI
│       ├── calculator.js    # 前端計算
│       ├── alerts.js        # 提醒 UI
│       └── logs.js          # 日誌 UI
├── tests/                   # 單元測試
├── tools/
│   └── check_js.mjs         # 前端語法檢查
├── verify.ps1               # PowerShell 一鍵驗證
├── verify.bat               # 本機一鍵驗證
└── .github/workflows/       # GitHub Actions
    ...

data/
    ├── system.db            # SQLite DB
    ├── main_file/           # 主檔存放
    ├── schedule/            # 排程表存放
    ├── bom/                 # BOM 檔案（UUID 命名）
    └── backups/             # 主檔備份
```

---

## 資料庫表

| 表名 | 說明 |
|------|------|
| `orders` | 訂單（含 status, folder, code, sort_order） |
| `inventory_snapshot` | 庫存快照（料號 → 庫存量 + MOQ） |
| `bom_files` | BOM 檔案 metadata |
| `bom_components` | BOM 零件明細 |
| `dispatch_records` | 已發料紀錄（不可逆） |
| `decisions` | 缺料決策 |
| `alerts` | 系統提醒 |
| `activity_logs` | 操作日誌 |
| `settings` | 系統設定 KV |

---

## 相依套件

- `fastapi` >= 0.110.0
- `uvicorn[standard]` >= 0.29.0
- `openpyxl` >= 3.1.2
- `xlrd` >= 2.0.1
- `python-multipart` >= 0.0.9
- `pyyaml` >= 6.0
- `python-dateutil` >= 2.8.0
