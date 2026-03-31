from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.03.31.10"
APP_RELEASED_AT = "2026-03-31"
APP_HEADLINE = "這版聚焦在發料流程、補料可視性、主檔寫入保護與系統維護。"
APP_CHANGELOG = [
    {
        "title": "發料與寫入主檔",
        "items": [
            "工具列按鈕改成「寫入主檔」，送出前會先整理預覽，不再直接批次發料。",
            "批次 Merge 與寫入主檔都有 loading 遮罩、進度條與更清楚的錯誤提示。",
            "若仍有缺料會禁止寫入主檔；EC 料低於 100 視為需補料，但只有寫入後會變負數時才會擋下。",
        ],
    },
    {
        "title": "副檔工作台",
        "items": [
            "副檔工作台預設收起，可展開後做單一副檔的預覽、修改、下載與寫入主檔。",
            "預覽與修改改成只針對目前點到的副檔，不會一次把全部攤開。",
            "修正舊版已 merge 副檔在寫入主檔預覽時補料顯示為 0 的問題。",
        ],
    },
    {
        "title": "缺料與 BOM",
        "items": [
            "右側補料明細改成只顯示 merge 後真正還缺的料，不再把已補掉的料一起列出。",
            "主檔預覽支援凍結第 1 列與 A/B/C 欄，比對料號時更接近 Excel。",
            "副檔預覽與修改畫面移除 CS 欄，需求、上批餘料、補料改成更精簡的顯示方式。",
        ],
    },
    {
        "title": "系統維護",
        "items": [
            "新增資料庫每日自動備份、保留最近 N 份、以及人工還原功能。",
            "已發料退回時會保留最近 30 天的副檔工作台，方便回復後續改。",
            "桌面版圖示與前端資產已整理，並補上更多前後端回歸測試。",
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
