from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.02.7"
APP_RELEASED_AT = "2026-04-02"
APP_HEADLINE = "這版修正副檔修改 modal 搜尋，避免搜中一筆時整份副檔都被誤判符合。"
APP_CHANGELOG = [
    {
        "title": "Modal 搜尋修正",
        "items": [
            "副檔修改 modal 的區塊搜尋現在只用副檔名比對，不會再把整份副檔所有料號都串成區塊關鍵字。",
            "搜尋某一筆料號時，現在會只顯示真的符合的列，不會因為同份副檔內另一筆命中就整塊都跳出來。",
        ],
    },
    {
        "title": "補料變更紀錄",
        "items": [
            "右側補料卡片現在可以直接輸入備註，保存後會一起記進正式補料資料。",
            "每筆補料都會顯示最後修改時間，重新整理後仍會保留。",
        ],
    },
    {
        "title": "穩定性",
        "items": [
            "這版補了前端資產測試，避免 modal 搜尋之後又回到整區塊誤命中的舊行為。",
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
