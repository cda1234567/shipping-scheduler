from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.07.2"
APP_RELEASED_AT = "2026-04-07"
APP_HEADLINE = "這版把無 MOQ 包裝管理頁改成直接跟主檔庫存對標，畫面和資料來源不再混用 ST 庫存。"
APP_CHANGELOG = [
    {
        "title": "無 MOQ 包裝管理",
        "items": [
            "無 MOQ 包裝頁改成直接讀主檔快照，現在列出的料號與對標數量都只跟主檔走。",
            "每顆料可直接輸入包裝數量清單，系統會即時計算包裝合計與主檔庫存差額。",
            "寫入主檔後，系統會優先整包扣除；沒有剛好相等時，才改用由左到右拆包。",
        ],
    },
    {
        "title": "同步與驗證",
        "items": [
            "無 MOQ 包裝頁的 API、前端顯示與測試已一起改成主檔來源，避免畫面和資料來源對不上。",
            "補了後端與前端測試，避免無 MOQ 管理頁、主檔對標與自動拆包規則之後再跑掉。",
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
