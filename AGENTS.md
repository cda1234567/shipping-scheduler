# AGENTS.md — AI Coding Agent 開發指南

> 本文件專為 AI coding agent（Claude、Cursor、Copilot 等）撰寫，目的是讓任何 agent 快速理解本專案並安全地進行修改。

---

## 專案概述

**OpenText 出貨排程系統** — 用於 PCB/電子製造業的出貨排程、BOM 領料、庫存計算、缺料預警與發料管理。

- **後端**: FastAPI + SQLite（單一 `system.db`）
- **前端**: 純 vanilla JS（ES modules）+ HTML/CSS，無框架
- **Pydantic 版本**: **v1**（1.10.x）— 使用 `.dict()` 而非 `.model_dump()`
- **部署環境**: Windows + UNC 路徑（`\\St-nas\...`）
- **啟動方式**: `python main.py` 或 `run.bat`（uvicorn 預設 port 8000）

---

## 目錄結構

```
opentext大改版/
├── main.py                  # FastAPI app 啟動點
├── config.yaml              # Excel 欄位對應設定
├── requirements.txt         # Python 相依套件
├── run.bat / restart.bat    # Windows 啟動/重啟腳本
├── app/
│   ├── config.py            # 設定載入（cfg() 用 dot notation）
│   ├── database.py          # SQLite 所有 CRUD（含 migration）
│   ├── models.py            # Pydantic models + Enums
│   ├── storage.py           # metadata.json 讀寫
│   ├── routers/
│   │   ├── main_file.py     # 主檔（庫存）上傳/下載/快照
│   │   ├── schedule.py      # 排程管理（訂單 CRUD、merge、dispatch）
│   │   ├── bom.py           # BOM 上傳/查詢/下載/補料寫入
│   │   ├── dispatch.py      # 發料單 xlsx 生成
│   │   ├── alerts.py        # 提醒系統
│   │   └── logs.py          # 操作日誌
│   └── services/
│       ├── main_reader.py       # 讀主檔庫存/MOQ
│       ├── schedule_parser.py   # 解析排程表 xlsx
│       ├── bom_parser.py        # 解析 BOM 領料單
│       ├── calculator.py        # 缺料 running balance 計算
│       ├── merge_to_main.py     # 發料 merge 寫入主檔
│       ├── xls_reader.py        # .xls/.xlsx 統一開啟
│       └── dispatch_form_generator.py  # 發料單生成
├── static/
│   ├── index.html           # SPA 主頁
│   ├── style.css            # 全域樣式
│   └── modules/
│       ├── api.js           # fetch wrapper（apiFetch, apiJson, apiPost）
│       ├── schedule.js      # 排程 UI 主模組（最大，~780 行）
│       ├── bom_manager.js   # BOM 管理 UI
│       ├── calculator.js    # 前端 running balance 計算
│       ├── alerts.js        # 提醒 UI
│       └── logs.js          # 操作日誌 UI
└── data/
    ├── system.db            # SQLite 資料庫
    ├── main_file/main.xlsx  # 當前主檔
    ├── schedule/            # 排程表
    ├── bom/                 # BOM 檔案（以 UUID 命名）
    └── backups/             # 主檔備份（發料時自動生成）
```

---

## 核心業務邏輯

### 訂單狀態流程（四階段）

```
pending（未排程）
   ↓  批次 merge / 自動 merge
merged（已 merge）
   ↓  ✓ 按鈕 → dispatch（鎖死、merge 到主檔）
dispatched（已發料）
   ↓
completed（已完成）

   ╲  手動取消
cancelled（已取消，可恢復）
```

**關鍵工作流：**

| 操作 | 觸發方式 | 行為 |
|------|---------|------|
| 批次 Merge | toolbar 按鈕 | 勾選的訂單 status → merged，開啟補料彈窗 |
| 補料彈窗「確認補料並下載 BOM」 | 彈窗內按鈕 | **只下載 BOM（寫入補料量到 H 欄），不 dispatch** |
| 發料鎖定 | 訂單卡片 ✓ 按鈕 | confirm → dispatch → merge 到主檔 → 移入已發料 |

### 缺料計算（Running Balance）

1. 讀取庫存快照（固定截止點的起始庫存）
2. 扣除所有已發料訂單的消耗（鎖死不動）
3. 對 merged 訂單依排序逐行跑 running balance
4. 庫存變負 → 缺料，建議採購量 = `ceil(shortage / MOQ) * MOQ`

### 發料 Merge 到主檔

每份 BOM 寫入主檔 3 欄：
- 欄位標頭格式：`batch_code | PO# | BOM model`
- H 欄（增添料）、F 欄（生產用量）、J 欄（結存）
- 寫入前自動備份主檔到 `data/backups/`

---

## 資料庫 Schema（SQLite）

| 表名 | 用途 | 主鍵 |
|------|------|------|
| `orders` | 訂單主表（含 status, folder, code） | id (自增) |
| `inventory_snapshot` | 起始庫存快照 | part_number |
| `bom_files` | BOM 檔案 metadata | id (UUID) |
| `bom_components` | BOM 零件明細（FK → bom_files） | id (自增) |
| `dispatch_records` | 已發料紀錄（鎖死用） | id (自增) |
| `decisions` | 缺料決策（UNIQUE: order_id + part_number） | id (自增) |
| `alerts` | 提醒（交期變更、缺料警告等） | id (自增) |
| `activity_logs` | 操作日誌 | id (自增) |
| `settings` | 系統設定 KV | key |

**Migration 模式**：在 `init_db()` 中使用 `PRAGMA table_info` 檢查欄位是否存在，不存在就 `ALTER TABLE ADD COLUMN`。

---

## Excel 欄位對應（0-based）

所有欄位位置定義在 `config.yaml`，透過 `cfg("excel.xxx", default)` 讀取。

| 設定鍵 | 預設值 | 說明 |
|--------|--------|------|
| `excel.bom_part_col` | 2 | BOM 料號欄（C 欄） |
| `excel.bom_desc_col` | 3 | BOM 說明欄（D 欄） |
| `excel.bom_qty_per_board` | 1 | 每板用量欄（B 欄） |
| `excel.bom_needed_col` | 5 | 需求量欄（F 欄） |
| `excel.bom_g_col` | 6 | G 欄 |
| `excel.bom_h_col` | 7 | H 欄（補料/上批餘料） |
| `excel.bom_data_start_row` | 5 | 資料起始列（1-based） |

> **重要**：`bom_parser.py` 用 0-based index 讀取 `all_rows`，`_write_supplements_to_ws` 用 openpyxl 的 1-based index（所以 +1）。

---

## API 端點總覽

### 主檔（/api/main-file）

| 方法 | 路徑 | 說明 |
|------|------|------|
| POST | /upload | 上傳主檔 xlsx |
| POST | /snapshot | 設定庫存快照 |
| GET | /data | 取得快照庫存 + MOQ |
| GET | /download | 下載主檔 |
| GET | /info | 主檔基本資訊 |

### 排程（/api/schedule）

| 方法 | 路徑 | 說明 |
|------|------|------|
| POST | /upload | 上傳排程表 |
| GET | /rows | 取得待處理訂單 |
| GET | /completed | 取得已發料訂單 + folders |
| GET | /calculate | 計算缺料 |
| POST | /batch-merge | 批次 merge 訂單 |
| POST | /auto-merge | 自動 merge（交期門檻） |
| POST | /reorder | 儲存訂單排序 |
| POST | /auto-sort | 依出貨日排序 |
| PATCH | /orders/{id}/delivery | 改交期 |
| PATCH | /orders/{id}/code | 改訂單編號 |
| PATCH | /orders/{id}/model | 改機種 |
| POST | /orders/{id}/cancel | 取消訂單 |
| POST | /orders/{id}/restore | 恢復訂單 |
| POST | /orders/{id}/dispatch | 發料（merge 到主檔） |
| POST | /orders/{id}/decisions | 儲存缺料決策 |
| POST | /orders/move-folder | 移動訂單到資料夾 |
| DELETE | /folders/{name} | 刪除資料夾 |

### BOM（/api/bom）

| 方法 | 路徑 | 說明 |
|------|------|------|
| POST | /upload | 上傳 BOM（支援 .xls/.xlsx/.xlsm） |
| GET | /list | BOM 機種分組清單 |
| DELETE | /{bom_id} | 刪除 BOM |
| GET | /data | 以機種為 key 的合併 BOM |
| GET | /{bom_id}/file | 下載單一 BOM |
| POST | /lookup | 依機種查 BOM metadata |
| POST | /download | 依機種下載（多檔 zip） |
| POST | /dispatch-download | 寫入補料量後下載 BOM 副本 |

### 其他

| 方法 | 路徑 | 說明 |
|------|------|------|
| POST | /api/dispatch/generate | 生成發料單 xlsx |
| GET | /api/alerts | 取得提醒 |
| POST | /api/alerts/{id}/read | 標記已讀 |
| POST | /api/alerts/read-all | 全部已讀 |
| GET | /api/logs | 取得操作日誌 |

---

## 開發注意事項

### 絕對不能做的事

1. **不要用 Pydantic v2 語法** — 本專案是 v1，用 `.dict()` 不是 `.model_dump()`
2. **不要在寫入 BOM 時用 `data_only=True`** — 會丟失所有公式
3. **不要隨意改訂單狀態** — `dispatch` 會 merge 到主檔，不可逆
4. **不要假設 BOM 的 group_model 只對應單一機種** — 可以是逗號分隔（如 `"T356789IU,T356789IU-U/A"`）

### 常見陷阱

- **Excel 欄位 index**：parser 用 0-based（`all_rows[row][col]`），openpyxl 寫入用 1-based（`.cell(row=r, column=c)`），轉換時記得 +1
- **.xls 檔案**：必須用 `xls_reader.py` 的 `open_workbook_any()` 處理，不能直接用 openpyxl 開
- **.xlsm 檔案**：`openpyxl.load_workbook(path, keep_vba=True)` 才能保留巨集
- **前端 fetch**：所有 API 呼叫都透過 `api.js` 的 `apiFetch`/`apiJson`/`apiPost`，不要直接 `fetch`
- **資料夾 folder 欄位**：空字串 `""` = 未歸檔，有名稱 = 在資料夾內
- **CSS/HTML**：無 framework、無 utility class，全是手寫 CSS。修改樣式只改 `style.css`

### 新增功能的標準流程

1. **Database**：在 `database.py` 新增 CRUD 函式；若需新欄位，在 `init_db()` 加 migration
2. **Models**：在 `models.py` 新增 Pydantic model（Request/Response）
3. **Router**：在對應的 `routers/xxx.py` 新增端點
4. **Service**：業務邏輯抽到 `services/` 下的對應模組
5. **Frontend**：在 `static/modules/` 對應的 JS 模組新增函式，用 `api.js` 呼叫 API
6. **HTML**：如需新 UI 元素，加在 `index.html` 對應的 section

### 程式碼風格

- Python：4 格縮排，type hints，中文註解
- JavaScript：2 格縮排，ES module（`import/export`），底線前綴表私有（`_rows`）
- 命名：後端 snake_case，前端 camelCase
- 所有使用者可見的文字用**繁體中文**
