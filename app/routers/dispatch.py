from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .. import database as db
from ..services.calculator import run as calc_run
from ..services.dispatch_form_generator import generate_dispatch_form
from ..services.download_names import build_generated_filename
from ..services.main_reader import find_legacy_snapshot_stock_fixes, read_moq, read_stock

router = APIRouter()


class DispatchRequest(BaseModel):
    order_ids: list[int] = Field(default_factory=list)
    decisions: dict[str, str] = Field(default_factory=dict)


def _normalize_part_key(value) -> str:
    return str(value or "").strip().upper()


def _normalize_decision_overrides(decisions: dict[str, str] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for part_number, decision in (decisions or {}).items():
        key = _normalize_part_key(part_number)
        if key and decision:
            normalized[key] = decision
    return normalized


def _get_selected_orders(order_ids: list[int]) -> list[dict]:
    selected_orders: list[dict] = []
    seen_ids: set[int] = set()
    for order_id in order_ids:
        try:
            normalized_id = int(order_id)
        except (TypeError, ValueError):
            continue
        if normalized_id in seen_ids:
            continue
        seen_ids.add(normalized_id)

        order = db.get_order(normalized_id)
        if not order:
            continue
        if order.get("status") not in ("pending", "merged"):
            continue
        selected_orders.append(order)
    return selected_orders


def _load_shortage_inputs() -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    main_path = str(db.get_setting("main_file_path") or "").strip()
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先上傳主檔")

    snapshot = db.get_snapshot()
    if snapshot:
        fixes = find_legacy_snapshot_stock_fixes(main_path, snapshot)
        if fixes:
            db.update_snapshot_stock(fixes)
            for part, qty in fixes.items():
                if part in snapshot:
                    snapshot[part]["stock_qty"] = qty

        stock = {
            _normalize_part_key(part): float((values or {}).get("stock_qty") or 0)
            for part, values in snapshot.items()
            if _normalize_part_key(part)
        }
        moq = {
            _normalize_part_key(part): float((values or {}).get("moq") or 0)
            for part, values in snapshot.items()
            if _normalize_part_key(part)
        }
        live_moq = {
            _normalize_part_key(part): float(qty or 0)
            for part, qty in read_moq(main_path).items()
            if _normalize_part_key(part)
        }
        live_moq.update(moq)
        moq = live_moq
    else:
        stock = {
            _normalize_part_key(part): float(qty or 0)
            for part, qty in read_stock(main_path).items()
            if _normalize_part_key(part)
        }
        moq = {
            _normalize_part_key(part): float(qty or 0)
            for part, qty in read_moq(main_path).items()
            if _normalize_part_key(part)
        }

    dispatched_consumption = db.get_all_dispatched_consumption(db.get_snapshot_taken_at())
    return stock, moq, dispatched_consumption


def _build_component_description_map(components: list[dict]) -> dict[str, str]:
    descriptions: dict[str, str] = {}
    for component in components:
        part = _normalize_part_key(component.get("part_number"))
        if not part:
            continue
        descriptions.setdefault(part, component.get("description", ""))
    return descriptions


@router.post("/dispatch/generate")
async def generate(req: DispatchRequest):
    bom_map = db.get_all_bom_components_by_model()
    if not bom_map:
        raise HTTPException(400, "請先上傳 BOM 檔案")

    requested_ids = list(dict.fromkeys(req.order_ids))
    if not requested_ids:
        raise HTTPException(400, "請先勾選要生成發料單的訂單")

    orders = _get_selected_orders(requested_ids)
    if not orders:
        raise HTTPException(400, "勾選的訂單沒有可生成的待處理內容")

    stock, moq, dispatched_consumption = _load_shortage_inputs()
    calc_results = calc_run(orders, bom_map, stock, moq, dispatched_consumption, db.get_st_inventory_stock())
    result_by_order = {int(result.get("order_id")): result for result in calc_results if result.get("order_id") is not None}
    saved_supplements = db.get_order_supplements([int(order["id"]) for order in orders])
    decision_overrides = _normalize_decision_overrides(req.decisions)

    today = datetime.now().strftime("%Y/%m/%d")
    groups = []

    for order in orders:
        order_id = int(order["id"])
        model_key = _normalize_part_key(order.get("model"))
        components = bom_map.get(model_key, [])
        if not components:
            continue

        descriptions = _build_component_description_map(components)
        saved_decisions = db.get_decisions_for_order(order_id)
        decisions = {**saved_decisions, **decision_overrides}
        stored_order_supplements = saved_supplements.get(order_id, {})

        result = result_by_order.get(order_id) or {}
        shortage_items = [
            *(result.get("shortages") or []),
            *(result.get("customer_material_shortages") or []),
        ]
        shortages_by_part = {
            _normalize_part_key(item.get("part_number")): item
            for item in shortage_items
            if _normalize_part_key(item.get("part_number"))
        }

        candidate_parts = set(shortages_by_part) | set(stored_order_supplements)
        candidate_parts.update(
            part_number
            for part_number, decision in decisions.items()
            if decision in {"CreateRequirement", "Shortage"}
        )

        items = []
        for part in sorted(candidate_parts):
            decision = decisions.get(part, "None")
            if decision in {"MarkHasPO", "IgnoreOnce"}:
                continue

            shortage_item = shortages_by_part.get(part)
            purchase_needed_qty = float((shortage_item or {}).get("purchase_needed_qty") or 0)
            fill_color = "FFFFC000" if purchase_needed_qty > 0 else None
            description = descriptions.get(part) or (shortage_item or {}).get("description", "")
            display_part = (shortage_item or {}).get("part_number") or part

            if decision == "Shortage":
                items.append({
                    "part": display_part,
                    "desc": description,
                    "qty": "缺",
                    "fill_color": None,
                    "is_shortage": True,
                })
                continue

            supplement_qty = float(stored_order_supplements.get(part, 0) or 0)
            if supplement_qty > 0:
                items.append({
                    "part": display_part,
                    "desc": description,
                    "qty": round(supplement_qty),
                    "fill_color": fill_color,
                    "is_shortage": False,
                })
                continue

            if not shortage_item:
                continue

            suggested_qty = float(shortage_item.get("suggested_qty") or shortage_item.get("shortage_amount") or 0)
            if suggested_qty <= 0:
                continue

            items.append({
                "part": display_part,
                "desc": description,
                "qty": round(suggested_qty),
                "fill_color": fill_color,
                "is_shortage": False,
            })

        if not items:
            continue

        ship_date = order.get("delivery_date") or order.get("ship_date") or ""
        groups.append({
            "batch_code": order.get("code") or str(order.get("id", "")),
            "po_number": str(order.get("po_number", "")),
            "model": order.get("model", ""),
            "date": ship_date.replace("-", "/") if ship_date else today,
            "items": items,
        })

    if not groups:
        raise HTTPException(400, "勾選的訂單目前沒有可生成的發料內容")

    tmp_dir = tempfile.mkdtemp()
    filename = build_generated_filename("發料單", ".xlsx")
    out_path = os.path.join(tmp_dir, filename)
    generate_dispatch_form(groups, out_path)
    db.log_activity("dispatch_generated", f"生成發料單，{len(groups)} 筆訂單")

    return FileResponse(
        out_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
