from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.07.4"
APP_RELEASED_AT = "2026-04-07"
APP_HEADLINE = "這版把無 MOQ 包裝頁收成『料號看主檔、數量看 ST』，來源更符合你現在的作業方式。"
APP_CHANGELOG = [
    {
        "title": "無 MOQ 包裝管理",
        "items": [
            "無 MOQ 包裝頁現在改成：料號清單只看主檔快照，數量則改讀 ST 庫存。",
            "每顆料可直接輸入包裝數量清單，系統會即時計算包裝合計與 ST 數量差額。",
            "寫入主檔後，系統會優先整包扣除；沒有剛好相等時，才改用由左到右拆包。",
            "無 MOQ 包裝頁外層改成可捲動容器，現在滑鼠滾輪可以直接往下查看全部料號。",
        ],
    },
    {
        "title": "同步與驗證",
        "items": [
            "無 MOQ 包裝頁的 API、前端顯示與測試已一起改成主檔 / ST 混合來源，避免料號和數量讀錯邊。",
            "補了後端與前端測試，避免無 MOQ 管理頁、料號來源、ST 數量對標與自動拆包規則之後再跑掉。",
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
