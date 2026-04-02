from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.02.6"
APP_RELEASED_AT = "2026-04-02"
APP_HEADLINE = "這版補上右側補料備註與最後修改時間，方便追補料變更。"
APP_CHANGELOG = [
    {
        "title": "補料變更紀錄",
        "items": [
            "右側補料卡片現在可以直接輸入備註，保存後會一起記進正式補料資料。",
            "每筆補料都會顯示最後修改時間，重新整理後仍會保留。",
            "只有數量或備註真的變更時才會刷新 updated_at，不會每次重建都把時間洗掉。",
        ],
    },
    {
        "title": "資料同步",
        "items": [
            "排程列資料現在除了補料數量，也會一起回傳備註與 updated_at 給前端。",
            "其他只改數量的流程會保留原本備註，不會因為沒有傳 note 就被清空。",
        ],
    },
    {
        "title": "穩定性",
        "items": [
            "這版補了資料庫測試、API 測試與前端資產測試，避免補料備註和時間之後又掉回去。",
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
