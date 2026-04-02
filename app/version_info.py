from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.02.9"
APP_RELEASED_AT = "2026-04-02"
APP_HEADLINE = "這版修正 modal 搜尋，把數字搜尋從說明欄的封裝尺寸誤命中中分開。"
APP_CHANGELOG = [
    {
        "title": "Modal 搜尋修正",
        "items": [
            "副檔修改與補料決策 modal 現在把料號/機種和說明分開比對，數字搜尋不會再被 0201 這類封裝尺寸誤命中。",
            "像輸入 201 這類數字時，會優先對料號與機種做包含比對；說明欄則改成看詞首，避免結果太雜。",
        ],
    },
    {
        "title": "編輯登入",
        "items": [
            "系統現在預設唯讀，右上角要先按登入編輯，後端才允許 POST/PUT/PATCH/DELETE 寫入操作。",
            "登入成功後會寫入 HttpOnly cookie，重新整理後在有效期限內不需要每次重登。",
        ],
    },
    {
        "title": "穩定性",
        "items": [
            "這版補了前端資產測試，避免 modal 搜尋之後又回到數字誤命中的舊行為。",
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
