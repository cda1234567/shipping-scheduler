# 交接文件 — 出貨排程系統（2026-07-13，由 Claude 交接給 Codex）

> 讀者：接手的 AI agent 與 Andy。搭配 `AGENTS.md`（程式結構）與 `.omc/plans/`、`.omc/research/`（設計與診斷報告）服用。

---

## 0. 一句話現狀

工作目錄版本 **v2026.07.13.8**，算料手填值保留與寫主檔前置加速已完成，**尚未部署**。有一件**待 Andy 操作的資料修復**（見 §2 第 1 條），和一排已排序的待辦（§4）。

## 1. 開發循環 SOP（每一版都照這個走，血淚換來的）

1. **改完先跑全套測試**：`C:/Users/Andy-STNB/AppData/Local/Programs/Python/Python313/python.exe -m unittest discover -s tests`
   —— 一定用這個絕對路徑；這台機器的 `python` 會隨機解析到 uv/hermes venv（缺 openpyxl）。
2. **前端改動**加跑 `node --check static/modules/schedule.js`。
3. **commit 前掃漏檔**：`git status` 檢查有沒有新增的 .py/.js 還在 untracked——曾因漏 commit `app/constants.py` 造成 production 當機十分鐘。
4. **每個使用者可見的改動要 bump 版本**：`app/version_info.py` 的 APP_VERSION（格式 `v年.月.日.流水號`）＋白話 changelog（Andy 會看「紀錄」分頁）。
5. **出貨＝push main**：GitHub Actions 建映像（約 90 秒）→ 本機 watchtower 每 5 分鐘拉新版。驗證：`curl http://127.0.0.1:8765/api/health` 看 version。**CI 的 Checks workflow 紅燈＝停手查明**，publish 成功不代表測試過。
6. **獨立審查**：非瑣碎改動出貨前，找另一個 fresh agent 審 diff（寫的人不審自己的）。高風險寫入（主檔/庫存）開嚴格清單：冪等、空值、正負號、邊界時間戳。
7. **部署後驗證**：至少 health＋針對改動的實測（API 探針或 Playwright 腳本，`tools/verify_*.mjs` 有現成模式）。改動涉及計算時，拿真實資料驗一個具體數字。
8. **動資料前先備份**：整包 `docker cp dispatch-scheduler-localserver:/app/data <目的地>`（約 500MB）；快速 DB 快照 `POST /api/system/db-backups/run`（需編輯登入）。

## 2. ⚠️ 目前資料狀態（接手先讀）

1. **主檔 EC 料大片負數，等 Andy 修復**：7/13 早上那批 20 筆寫入時副檔補料是空的（前置故障連鎖），EC-10029A 帳上 −21865 等。修復路徑已備好：退回那 20 筆 → 勾「重算補料」（v13.7 起會自動帶入建議補料）→ 重新寫入。**接手後若 Andy 還沒做，提醒他。**
2. **主檔歷史守恆掃描**：`.omc/research/conservation-scan-20260713.md`（130 處跳動、85 支料淨差延續至今，多為手帳時代整卷進出未入帳）。已與 Andy 共識：不考古，靠盤點停損歸零。主檔要不要也以盤點數重設＝**未決策**。
3. **盤點停損還沒正式執行過**（他不敢按 → 已給乾跑報告 `.omc/research/stoploss-dryrun-20260713.md`，125 支料的完整前後對照）。撤銷按鈕還沒做（見 §4）。
4. 幽靈不良品批次（7/3 重複匯入）已手術清除；防連點雙扣、退回警告日期彙總都已上線。

## 3. 本週版本大事記（詳見 version_info.py changelog）

- v07.08.x：發料單批次排序、已發料全選/Shift 範圍勾、巢狀資料夾、打樣（EC 免補到 100，照常扣帳）
- v07.09.x：退回沖不良品的警告＋一鍵補回（防雙扣）、資料夾重影修正、算料工作區（廢彈窗改分頁）、EC 補料跨列污染止血、缺料計算統一後端一份（一料一列、整批補一次）、批次 Merge 一顆＋兩勾選、打樣全路徑
- v07.10.x：寫主檔真進度（背景 job＋輪詢＋斷線接回＋single-flight）、寫入後三層自動核對、盤點對帳（庚霖格式試算＋分批停損＋批次截止點）、副檔 BOM 期初修正
- v07.13.x：未盤點覆蓋清單（自備料盲區）、退回警告日期彙總、批次 Merge 進度顯示、訂單卡列回共用缺料、主檔預覽重算修正（空白表頭前結存）、**calc-preview 事件迴圈解凍＋快取**（全站卡死元兇）、核對層同單同料誤報修正、重算補料自動帶建議值
- v07.13.8：算料工作區手動補料／缺料狀態改由前端依訂單與料號保留；改 MOQ 不再重建並重開整個工作區；calc-preview 與寫主檔前置重算不再產生整批 BOM Excel

## 4. 排隊中的工作（依優先序）

1. **儲存副檔／下載副檔改背景 job**（現為同步重活，Cloudflare 100 秒會 524；比照 update-and-commit-drafts 的 job 模式，`app/routers/schedule.py` 有完整範本）
2. **決策儲存改批次端點**（現在前端點一次缺料決策＝對 `/orders/{id}/decisions` 連發 N 個請求，SQLite database-is-locked；收成一個批次 POST）
3. **停損撤銷按鈕**（st_reconcile_adjustments 有完整 adjust_qty 留痕，倒扣回去＋alignment 標記 reverted＋anchor 查詢排除 reverted）
4. **幽靈批次 UI 刪除選項**（批次的主檔欄位已被退回沖掉時，刪除被 guard 擋死；給「只刪紀錄不動主檔」的明確選項）
5. 待 Andy 拍板：庚霖盤點表改一列一料（B）或系統自動拆（C）；主檔歷史跳動要不要以盤點數重設
6. 低優先：F 欄同單同料多筆只顯示最後一筆的量（帳面顯示與實扣不一致，實扣是對的）；commit 原子性（alignment 三段非同交易）；`.omc/research/review-*.md` 各報告內未修的 minor 清單

## 5. 地雷清單（改碼前必讀，每一條都炸過）

1. **主檔約 8.8% 舊批次組的批次碼表頭是空白**：任何「靠 row-1 表頭找欄位」的邏輯都會漏看它們。已炸三次（副檔 BOM 期初、主檔預覽重算、下載重建）。正解＝跟寫入路徑一致的「由右往左找數值」（`merge_to_main._read_latest_stock` 語意）。
2. **FastAPI async def ＋ 重計算 ＝ 凍住整個 event loop**：重活端點一律 sync `def`（自動進 threadpool）。calc-preview 曾因此把全站（含寫主檔、health）拖到逾時。
3. **同一張單 BOM 內同料號多筆**：寫入端逐筆覆寫同組欄位、結存累積到最後一筆（結存正確、F 欄顯示只剩最後一筆的量）。核對/比對邏輯必須拿「最後一筆」期望值（`main_reconcile._dedupe_plan_rows`）。
4. **退回＝整份主檔備份覆蓋**：備份時點之後寫入的一切（不良品、多打）都會消失；已有警告＋補回機制，但改退回邏輯時要記得這個語意。
5. **Cloudflare 100 秒斷頭**：任何可能超過 100 秒的同步端點都會 524。長活一律 job＋輪詢。
6. **寫主檔 job 存記憶體**：容器重啟（watchtower 部署）job 就消失，前端會優雅 404；部署時間點注意別跟 Andy 寫入撞車。
7. **批次代碼（如 6-1）≠ 時間順序**；批次的 dispatched_at 早於它自己的庫存 audit 時間戳——切時間點用「該批最後一筆 consumed_at」（cutoff_at），用 dispatched_at 會雙扣（已炸過，有回歸測試）。
8. **Codex CLI 在這台機器的沙箱跑不了測試**（Temp 權限），跑了必卡死。若用 codex exec 派工，prompt 一定加「不要自己跑任何測試，寫完即回報」；測試由外部跑。
9. Pydantic 版本注意：AGENTS.md 寫 v1，但 runtime 對 `.dict()`/`utcnow` 出 v2 式棄用警告——動 models 前先實際確認。
10. 這台 Docker 偶發整個卡死（CLI 全 timeout、watchtower 停擺、容器照常服務）：`wsl --shutdown` 連 VM 重開才有效，重啟後容器會自己回來。

## 6. 環境與憑證

- 服務：`http://127.0.0.1:8765`（container `dispatch-scheduler-localserver`；對外 app.cda1234567.com 走 cloudflared）
- 編輯登入密碼：`123`（`POST /api/system/edit-auth/login`）；寫入類 API 需帶 cookie
- 庚霖季度盤點檔：`\\St-nas\個人資料夾\Andy\Job\123\`（範例已入庫 `templates/庚霖實際庫存2026Q1_2026-6-29.xlsx`）
- 下載落地資料夾（container 掛載）：`D:\Download\excel`
- GitHub：`cda1234567/shipping-scheduler`；映像 ghcr.io

### 正航採購單寫入（開採購單頁籤）

- 目標公司：`ERP_WRITE_TARGET`（預設 `CHIComp02` 測試；正式為 `CHIComp03`）。頁面徽章會顯示目前寫入哪一家。
- 測試公司由環境變數 `ERP_TEST_TARGETS`（逗號分隔，預設 `CHIComp01,CHIComp02,CHIComp13`，每次呼叫即時讀取；`erp_purchase.test_targets()` / `is_test()`）決定，目前 = 公司1、公司2、公司13（公司13 為正航範例公司，沒有我們的供應商 / 料號、倉庫只有 01-05、沒有人員 017，全靠補主檔）。項目須符合 `CHIComp\d\d`；**公司3（正式）與來源庫 `ERP_SQL_DATABASE` 即使列入也一律不算測試公司**（不給測試帳密、不複製主檔）。測試公司共用 `ERP_SQL_TEST_USER / ERP_SQL_TEST_PASSWORD / ERP_SQL_TEST_SERVER`；**資料庫一律是 `ERP_WRITE_TARGET`**，`ERP_SQL_TEST_DB` / `ERP_SQL_TEST_DB_ALT` 已不使用。測試帳號絕不會用在公司3（或任何非測試公司）。
- 帳密查找順序：`ERP_SQL_WRITE_USER_/PASSWORD_/SERVER_<TARGET>`（例 `..._CHICOMP03`，server 格式 `host,port`）→ 僅測試公司才退回 `ERP_SQL_TEST_*`。公司3：另建 `D:/secrets/shipping-erp-comp03-write.env` 放公司3專用帳密。
- **切換到公司3**：設 `ERP_WRITE_TARGET=CHIComp03` 後重建容器；先用預覽確認徽章為紅色「寫入：公司3（正式）」。寫入公司3時缺主檔不會自動複製（直接報錯）；寫入只新增、不改不刪，且本系統無法修改 / 刪除已開的單。
- **補主檔（只限測試公司，從公司3複製）**：依序 comCustomer（供應商）→ comProductClass（各料號的 ClassID）→ comProduct → comWareHouse（`ERP_ST_WARE_ID`，如 001）→ comDepartment（範本單 DepartID，以及人員自己的 DepartID，缺才補）→ comPerson（採購員 SalesMan、製單人 MakerID，如 017）。類別 / 部門來源也沒有就略過；供應商 / 料號 / 倉庫 / 人員來源沒有則報錯。只有目標庫會被寫入，來源連線唯讀，且每次寫入前都核對 `DB_NAME()`。預覽的 `masters_to_copy` 會列出將複製的主檔。INSERT 欄位取自目標庫 `sys.columns`。
- 本機紀錄在 SQLite `erp_purchase_orders`；帳密一律不進 repo。

### 缺料採購（缺料採購頁籤）

- 流程：寫入主檔 / 生成發料單後 → 「缺料採購」頁籤（或已發料頁籤的「缺料採購」按鈕、完成後 toast 的「前往缺料採購」）→ 選訂單（預設最近一次寫入主檔那批：發料時間距最新一筆 10 分鐘內）→ 產生缺料清單 → 勾選 → 預覽採購單 → 確認寫入正航。
- 需求量來自 `dispatch.build_dispatch_groups`（與發料單同一套計算，item 多帶 `need_qty`、group 多帶 `order_id` / `ship_date`）。缺料 = `shortage_rules.summarize_requested_supply(需求, ST)['purchase_needed_qty']`（需求 − ST，與發料單同一套；v2026.10.29.9 起**不再扣在途**，使用者明說「不減在途」）；在途量仍用低庫存提醒的 `_query_on_order` 查出，僅供參考顯示，ERP 連不上時當 0 並在畫面出現警告。建議數量 = ceil(缺)。
- 採購明細與訂單的對應（v2026.10.29.9，**只記錄 / 顯示，不參與任何缺料或扣在途計算**）：成功寫入 ERP 後，在寫 `erp_purchase_orders` 的同一步把每行存進 SQLite `erp_po_line_allocations`（kind = order / extra / manual；source = shortage_po / manual_po；`dispatch_session_id` = 當時該訂單最近一次未退回的發料批次）。`shortage_purchase.allocate_purchase_to_orders` 依出貨日（空日期最後）先用 max(ST,0) 抵需求，再填採購量，多買的記 extra；preview / create 由 router 的 `prepare_groups` 依前端送的 `order_ids` 以最終數量重算（忽略用戶端送的 allocations；料號不在這批訂單內或沒給 order_ids → manual）。開採購單可選帶 `allocations`（驗證 qty>0、合計 ≤ 行數量，不足補 manual）。歷史 API `/api/erp/po/history` 每張單附 `allocations`，舊單為空 → 前端顯示「—（舊單無紀錄）」。日後「只扣對應在途」開關可讀這張表，但本版沒有任何地方用它扣減。
- 供應商：正航料號主檔 `MajorSupplier`（須存在於 comCustomer Flag=2）→ 最近一張採購單的供應商 → 未設定（需手動填，未填不能勾）。單價預設最近採購價。同供應商勾選的料合併成一張採購單，一次最多 20 張。
- 預計進貨日以最早需要該料的訂單出貨日為準：打件 = 出貨日 −1 個月（取月底）後，取不晚於該日的最近一個 26 日；後段 = 出貨日 − 21 天。早於今天標「遲了」（仍可寫）。
- 打件 / 後段自動判斷（`shortage_purchase.classify_part`）：料號前兩碼 EC / IC / UC / PB，或品名含 0201 0402 0603 0805 1206 SMD SMT SOT QFN QFP BGA SOP TSSOP PCB 電容 電阻 電感 → 打件；其餘（OC / PK / SP / AC / RC / CM / CF / SS / GH …）→ 後段。人工指定存在 SQLite `part_purchase_categories`，優先於自動判斷。
- API（`/api/shortage-po`）：`GET /list?order_ids=1,2` 缺料清單、`GET /orders` 可選訂單 + `latest_batch_ids`、`POST /category` 存類別、`POST /preview`（`{groups}`，預估單號依序遞增）、`POST /create`（需 `confirm:true`，逐張寫入、單張失敗不中斷，活動日誌記成功 / 失敗張數）。POST 皆需編輯登入。

### 採購 MOQ（v2026.10.30.4，`app/services/moq.py`）

- 開採購單 / 缺料採購 / 低庫存「開採購單」的預設數量 = `max(需求, MOQ)`（`moq.adjust_qty`）；沒有 MOQ 就用需求。使用者可手動改，建立時後端照送出的數量寫入（不再調整）。
- MOQ 來源（`moq.lookup_moq`）：① 本站有效 MOQ = `dispatch_pipeline.get_effective_moq(主檔路徑)`（主檔 MOQ 欄，被庫存快照 / 手動編輯覆蓋；標示「主檔」）；② 本站沒有或為 0 才查正航 `comProduct.MinPurch`（最低採購量，唯讀 SELECT + NOLOCK；標示「ERP 最低採購量」），正航連不上靜默略過。低庫存清單 API 不連 ERP，只用 ① ；ERP 補查發生在開採購單的 `part-defaults` 與缺料採購清單。
- **沒有可靠的訂購倍數 / SPQ**：`TakeMatMultiple` 是領料倍數（不是採購）、`MinPackQty` 全為 0、`PackAmt` / `InPackAmt` 是單位換算。所以目前不進位到倍數；`adjust_qty(need, moq, multiple=0)` 已預留 `multiple`，之後有可靠來源再接。

### 重複開單保護、無年份日期、低庫存統計窗口（v2026.10.30.4）

- **重複開單**（`app/services/po_duplicates.py`）：只看本站 `erp_purchase_orders`（status=ok、同 `target_db`）+ `erp_po_line_allocations`。缺料採購清單每行有 `already_ordered`；`create_po` 偵測到已開過且沒帶 `allow_duplicate` → 丟 `DuplicateWarning`，router 回 200 + `duplicate_warning`（不寫入）；缺料採購 `/create` 在 router 先整批檢查，通過後各組以 `allow_duplicate=True` 建立。防連點 `fingerprint`（60 秒、含建立中 in-flight）不能用 allow_duplicate 繞過，回 409。
- **無年份日期**（`app/services/date_infer.py`）：排程表 m/d 格式儲存格依今天推年（過去超過 60 天 → 明年）。舊資料修復：`python -m app.tools.fix_yearless_dates [--apply]`，手動執行、預設 dry-run，不在啟動時跑，不碰正航。
- **低庫存**：`lookback_days`（平均日用量，預設 365，env `LOW_STOCK_LOOKBACK_DAYS`）與 `suggest_lookback_days`（建議清單候選窗口，預設 90）分開。

## 7. 跟 Andy 的合作方式

- 全程繁中白話，講結論不寫論文；選項用「三選一＋推薦哪個」。他說「幾支幾」＝批次代碼（6-1 這種）。
- 可逆的直接做不用問；不可逆（刪資料、動庫存基準、對外）先講清楚後果拿確認。**動資料前先備份**是他明定的規矩。
- 他會自己直接改主檔預覽的格子（合法操作），帳務邏輯改動要考慮這件事。
- 他回報 bug 通常一句話＋截圖；先查真實資料再下結論，修完要拿實際數字驗證給他看。
