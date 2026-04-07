from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.07.5"
APP_RELEASED_AT = "2026-04-07"
APP_HEADLINE = "這版把缺料清單改成照排程順序顯示，並明確標出機種 + x-x，避免同機種不同單混在一起。"
APP_CHANGELOG = [
    {
        "title": "缺料顯示與扣帳順序",
        "items": [
            "右側缺料清單改成以每筆訂單為單位顯示，會直接看到「機種 + x-x」而不是只顯示機種。",
            "同機種不同編號現在會分開列出，例如 TA7-2 2-5、TA7-2 2-7 不會再被折成同一組。",
            "缺料卡片與補料 modal 會依排程實際順序往下顯示，讓扣帳過程更容易對照。",
        ],
    },
    {
        "title": "搜尋與判讀",
        "items": [
            "缺料清單搜尋會一起吃機種、x-x 與 PO，追查是哪一筆扣到哪一筆時比較直覺。",
            "保留逐筆補料的方式，補這筆之後，後面的訂單仍會沿用剩餘量繼續扣帳。",
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
