from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.02.10"
APP_RELEASED_AT = "2026-04-02"
APP_HEADLINE = "這版把主檔扣帳樣式收乾淨，扣帳數量不再上色，第一列標頭也會自動換行。"
APP_CHANGELOG = [
    {
        "title": "主檔樣式整理",
        "items": [
            "不良品扣帳、加工多打扣帳與回復寫進主檔時，扣帳數量欄不再沿用舊欄位的底色。",
            "主檔第 1 列現在會統一套用自動換行，新增欄位和舊欄位標頭都會一起整理。",
        ],
    },
    {
        "title": "穩定性",
        "items": [
            "補了主檔寫入測試，避免之後欄位樣式又被舊的樣板 fill 複製回來。",
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
