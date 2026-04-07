from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.07.1"
APP_RELEASED_AT = "2026-04-07"
APP_HEADLINE = "這版新增無 MOQ 包裝管理頁，會跟 ST 庫存對標，寫入主檔後也會自動扣掉已用掉的包裝數量。"
APP_CHANGELOG = [
    {
        "title": "無 MOQ 包裝管理",
        "items": [
            "新增「無 MOQ 包裝」頁面，會自動列出目前 MOQ 空白、且 ST 庫存仍有數量的料號。",
            "每顆料可直接輸入包裝數量清單，系統會即時計算包裝合計與 ST 庫存差額。",
            "寫入主檔後，系統會優先整包扣除；沒有剛好相等時，才改用由左到右拆包。",
        ],
    },
    {
        "title": "同步與驗證",
        "items": [
            "寫入主檔成功後，ST 庫存與無 MOQ 包裝清單都會一起同步更新。",
            "補了後端與前端測試，避免無 MOQ 管理頁、ST 扣包裝與自動拆包規則之後再跑掉。",
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
