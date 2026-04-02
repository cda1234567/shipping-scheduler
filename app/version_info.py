from __future__ import annotations

APP_NAME = "出貨排程系統"
APP_VERSION = "v2026.04.02.5"
APP_RELEASED_AT = "2026-04-02"
APP_HEADLINE = "這版聚焦在下載副檔公式預先重算與補料檔案開啟穩定度。"
APP_CHANGELOG = [
    {
        "title": "下載副檔重算",
        "items": [
            "副檔與補料 BOM 下載前，現在會先標記為完整重算，避免打開後還要手動按編輯才顯示正確結果。",
            "有公式的下載檔案會優先嘗試用 Excel / LibreOffice 預先刷新計算快取，減少公式延遲顯示。",
            "沒有公式的工作簿則維持原本快速輸出，不會多做外部重算。",
        ],
    },
    {
        "title": "副檔與補料 BOM",
        "items": [
            "副檔生成與 BOM dispatch-download 現在共用同一套儲存 helper，不再各自直接 wb.save()。",
            "下載路徑會自動保留 Excel 的重新計算旗標，讓公式依賴欄位更新後更穩定。",
            "這版也補了回歸測試，避免之後又把下載路徑改回單純儲存。",
        ],
    },
    {
        "title": "穩定性",
        "items": [
            "這次變更已補上 workbook 重算測試、副檔生成測試與 BOM 下載 API 測試。",
            "若執行環境沒有 Excel 或 LibreOffice，仍會保留 fullCalcOnLoad 旗標做最佳努力處理。",
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
