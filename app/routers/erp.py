from __future__ import annotations

from fastapi import APIRouter, HTTPException

from .. import database as db
from ..services import erp_stock

router = APIRouter()


@router.get("/erp/health")
async def erp_health():
    """ERP 連線狀態（不回傳帳號 / 密碼）。"""
    result = {**erp_stock.public_settings(), "connected": False}
    if not result["configured"]:
        result["error"] = "尚未設定 ERP 連線（請設定 ERP_SQL_USER 與 ERP_SQL_PASSWORD 環境變數）"
        return result
    try:
        erp_stock.check_connection()
        result["connected"] = True
    except erp_stock.ErpError as exc:
        result["error"] = str(exc)
    return result


@router.get("/erp/st-stock")
async def erp_st_stock(refresh: bool = False):
    """ST 倉庫存（來自 ERP，唯讀）。失敗時回 503，不影響本機任何資料。"""
    try:
        data = erp_stock.get_st_stock(force_refresh=refresh)
    except erp_stock.ErpError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    # 用主檔補 MOQ / 是否在主檔（與 /st-inventory/in-main 同來源）；不修改快取內容
    main_snapshot = {
        str(part).strip().upper(): row for part, row in db.get_snapshot().items() if str(part).strip()
    }
    rows = []
    for row in data["rows"]:
        main_row = main_snapshot.get(row["part_number"].upper())
        rows.append({
            **row,
            "in_main": main_row is not None,
            "moq": float((main_row or {}).get("moq") or 0),
            "description": row["name"] or str((main_row or {}).get("description") or ""),
        })
    return {**data, "rows": rows}
