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
└── data/
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
