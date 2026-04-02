from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.02.2"
APP_RELEASED_AT = "2026-04-02"
APP_HEADLINE = "這版聚焦在 modal 編輯效率、BOM 上傳檢查與副檔操作整理。"
APP_CHANGELOG = [
    {
        "title": "Modal 與副檔操作",
        "items": [
            "補料 / 副檔編輯 modal 最上方新增搜尋欄，搜尋時採前後萬用字元式的包含比對。",
            "副檔工作台拿掉額外的預覽按鈕，保留修改、下載、刪除與寫入主檔。",
            "同一顆料在 modal 內編輯時，現在可以先搜尋再調整，不用整頁捲動找料號。",
        ],
    },
    {
        "title": "BOM 上傳檢查",
        "items": [
            "上傳 BOM 時會先檢查資料列的 G / H 欄是否為空白。",
            "上傳 BOM 時也會檢查 I / J 欄是否仍為公式，避免把壞掉的副檔直接收進系統。",
            "檢查失敗會直接在上傳結果顯示錯列與料號，不會先寫進資料庫。",
        ],
    },
    {
        "title": "穩定性",
        "items": [
            "這次變更已補上前端資產測試、BOM 編輯測試與 API 上傳檢查測試。",
            "modal 搜尋與 BOM 檢查都走同一路徑，不再各自維護不同邏輯。",
        ],
    },
]


def get_app_meta() -> dict:
    return {
        "app_name": APP_NAME,
        "version": APP_VERSION,
        "released_at": APP_RELEASED_AT,
        "headline": APP_HEADLINE,
        "sections": APP_CHANGELOG,
    }
