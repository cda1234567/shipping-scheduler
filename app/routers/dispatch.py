from __future__ import annotations
import tempfile, os
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from ..services.dispatch_form_generator import generate_dispatch_form
from .. import database as db

router = APIRouter()


class DispatchRequest(BaseModel):
    decisions: dict[str, str] = {}


@router.post("/dispatch/generate")
async def generate(req: DispatchRequest):
    bom_map = db.get_all_bom_components_by_model()
    if not bom_map:
        raise HTTPException(400, "請先上傳 BOM 檔案")

    orders = db.get_orders(["pending", "merged"])
    if not orders:
        raise HTTPException(400, "沒有待處理的訂單")

    today = datetime.now().strftime("%Y/%m/%d")
    groups = []
    for order in orders:
        model_key = (order.get("model") or "").upper()
        comps = bom_map.get(model_key, [])
        if not comps:
            continue

        items = []
        for comp in comps:
            if comp.get("is_dash") or comp.get("needed_qty", 0) <= 0:
                continue
            part = comp.get("part_number", "")
            decision = req.decisions.get(part, "None")
            items.append({
                "part":        part,
                "desc":        comp.get("description", ""),
                "qty":         int(comp.get("needed_qty", 0)),
                "fill_color":  "FFC000" if decision == "CreateRequirement" else None,
                "is_shortage": decision == "Shortage",
                "is_customer_supplied": comp.get("is_customer_supplied", False),
            })

        if not items:
            continue

        ship_date = order.get("delivery_date") or order.get("ship_date") or ""
        groups.append({
            "batch_code": order.get("code") or str(order.get("id", "")),
            "po_number":  str(order.get("po_number", "")),
            "model":      order.get("model", ""),
            "date":       ship_date.replace("-", "/") if ship_date else today,
            "order_qty":  int(order.get("order_qty", 0)),
            "items":      items,
        })

    if not groups:
        raise HTTPException(400, "沒有可生成發料單的訂單（請確認 BOM 機種名稱與排程表一致）")

    tmp_dir = tempfile.mkdtemp()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(tmp_dir, f"發料單_{ts}.xlsx")
    generate_dispatch_form(groups, out_path)
    db.log_activity("dispatch_generated", f"生成發料單，{len(groups)} 個工單")

    return FileResponse(
        out_path,
        filename=f"發料單_{ts}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
