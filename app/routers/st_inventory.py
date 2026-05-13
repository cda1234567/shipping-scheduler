from __future__ import annotations

from fastapi import APIRouter

from .. import database as db

router = APIRouter()


@router.get("/st-inventory/in-main")
async def get_st_inventory_in_main():
    st_stock = {
        str(part).strip().upper(): float(qty or 0)
        for part, qty in db.get_st_inventory_stock().items()
        if str(part).strip()
    }
    st_snapshot = db.get_st_inventory_snapshot()
    main_snapshot = db.get_snapshot()
    main_parts = {str(part).strip().upper() for part in main_snapshot if str(part).strip()}

    rows = []
    for part_number in sorted(part for part in st_stock if part in main_parts):
        st_row = st_snapshot.get(part_number) or {}
        main_row = main_snapshot.get(part_number) or {}
        rows.append({
            "part_number": part_number,
            "description": str(st_row.get("description") or main_row.get("description") or ""),
            "stock_qty": st_stock[part_number],
            "moq": float(main_row.get("moq") or 0),
        })
    return rows
