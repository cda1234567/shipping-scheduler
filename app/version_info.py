from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.07.6"
APP_RELEASED_AT = "2026-04-07"
APP_HEADLINE = "這版把缺料補料改成真正照前後順序往下帶，前一筆補平後，後一筆不會再自己跳成缺料。"
APP_CHANGELOG = [
    {
        "title": "缺料補料串接",
        "items": [
            "缺料 modal 會依同料號的訂單順序往後重算 running balance，不再只改當前卡片。",
            "像 2-6 先補料後，2-7 同料號如果已經被補平，就不會再自己勾成缺料。",
            "後面訂單真的還缺時才會留下，補平的卡片會直接隱藏，畫面判讀會跟實際扣帳一致。",
        ],
    },
    {
        "title": "操作一致性",
        "items": [
            "保留逐筆補料的方式，但不再把同料號的補料值硬同步到所有卡片。",
            "缺料勾選與補料輸入改成一起觸發後續卡片重算，避免畫面顯示和實際結果不同步。",
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
