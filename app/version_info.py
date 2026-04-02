from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.02.8"
APP_RELEASED_AT = "2026-04-02"
APP_HEADLINE = "這版新增編輯登入模式，預設唯讀，登入後才允許寫入操作。"
APP_CHANGELOG = [
    {
        "title": "編輯登入",
        "items": [
            "系統現在預設唯讀，右上角要先按登入編輯，後端才允許 POST/PUT/PATCH/DELETE 寫入操作。",
            "未登入時如果直接點到編輯功能，前端會自動跳出登入視窗，不會默默失敗。",
        ],
    },
    {
        "title": "唯讀提示",
        "items": [
            "右上角新增唯讀/可編輯狀態提示，主要靜態編輯按鈕在唯讀模式下會先鎖住。",
            "登入成功後會寫入 HttpOnly cookie，重新整理後在有效期限內不需要每次重登。",
        ],
    },
    {
        "title": "穩定性",
        "items": [
            "這版補了 API 與前端資產測試，確認未登入真的會被後端擋下，登入後才可寫入。",
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
