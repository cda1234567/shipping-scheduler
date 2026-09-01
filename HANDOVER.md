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

## 7. 跟 Andy 的合作方式

- 全程繁中白話，講結論不寫論文；選項用「三選一＋推薦哪個」。他說「幾支幾」＝批次代碼（6-1 這種）。
- 可逆的直接做不用問；不可逆（刪資料、動庫存基準、對外）先講清楚後果拿確認。**動資料前先備份**是他明定的規矩。
- 他會自己直接改主檔預覽的格子（合法操作），帳務邏輯改動要考慮這件事。
- 他回報 bug 通常一句話＋截圖；先查真實資料再下結論，修完要拿實際數字驗證給他看。
