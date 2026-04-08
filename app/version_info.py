from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.08.1"
APP_RELEASED_AT = "2026-04-08"
APP_HEADLINE = "這版把同料號的建議補料改成會往後看累積缺口，前一筆就會直接帶出足夠的建議量。"
APP_CHANGELOG = [
    {
        "title": "建議補料",
        "items": [
            "同料號若後面訂單還會繼續吃料，前一張卡的「建議補」會直接帶入後續累積缺口，不再只看自己這一筆。",
            "像 2-5 的 EC-20131A-TAB，現在會直接建議補 2000，不會只先開 1000 又讓 2-6 再跳一次。",
            "右側缺料區和缺料 modal 都會套用同一套往後看的建議量算法，畫面判讀會更一致。",
        ],
    },
    {
        "title": "順序扣帳",
        "items": [
            "缺料 modal 仍然保留逐筆補料，但同料號會照訂單順序往後重算 running balance。",
            "前面一筆已經補到足夠時，後面同料號會自動被補平並隱藏，不會再讓人誤以為要重複補。",
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
