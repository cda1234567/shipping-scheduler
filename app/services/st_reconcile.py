"""加工廠盤點對帳試算。"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from .reconcile_core import theoretical_stock_with_details
from .xls_reader import open_workbook_any

CUSTOMER_HEADER = "客戶編號"
PART_HEADER = "汎翊國際料號"
DESC_HEADER = "品名規格"
PHYSICAL_HEADER = "辰尚填寫"

CATEGORY_HAVE_OURS_NOT_THEIRS = "我有單他沒有"
CATEGORY_HAVE_THEIRS_NOT_OURS = "他有單我沒入"
CATEGORY_QTY_MISMATCH = "同單數量不符"
CATEGORY_UNATTRIBUTED = "無法歸因淨差"
CATEGORY_MATCHED = "無差異"

ASSUMPTIONS = [
    "本試算只讀取上傳盤點表，不會寫入 ST 庫存，也不會建立對齊點。",
    "盤點表沒有良品 / 不良品分欄，本版先用盤點數與系統理論良品庫存比對；差額需人工再對照未報廢不良品單。",
    "盤點表沒有工單或 MO 號，本版只能做料號級淨差歸因；同單數量不符需等盤點表提供單號後才能精準判定。",
    "H 欄視為客戶群組總盤點數；同一群組若有多個汎翊料號，會標示需人工拆分並歸入無法歸因淨差。",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_part(value: Any) -> str:
    return _normalize_text(value).upper()


def _try_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _find_header_row(ws) -> tuple[int, dict[str, int]]:
    required = {CUSTOMER_HEADER, PART_HEADER, PHYSICAL_HEADER}
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=min(12, ws.max_row), values_only=True), start=1):
        values = [_normalize_text(cell) for cell in row]
        if not required.issubset(set(values)):
            continue
        return row_idx, {
            "customer": values.index(CUSTOMER_HEADER),
            "part": values.index(PART_HEADER),
            "desc": values.index(DESC_HEADER) if DESC_HEADER in values else -1,
            "physical": values.index(PHYSICAL_HEADER),
        }
    raise ValueError("找不到加工廠盤點表頭，需包含「客戶編號」、「汎翊國際料號」與「辰尚填寫」")


def _cell(row: tuple[Any, ...], column: int) -> Any:
    if column < 0 or len(row) <= column:
        return None
    return row[column]


def _finish_group(group: dict[str, Any] | None, rows: list[dict]) -> None:
    if not group:
        return
    unique_parts = list(dict.fromkeys(group["parts"]))
    if not unique_parts:
        return

    descriptions = group["descriptions"]
    if len(unique_parts) == 1:
        part = unique_parts[0]
        rows.append({
            "part_number": part,
            "description": descriptions.get(part, ""),
            "physical": float(group["physical"]),
            "customer_code": group["customer_code"],
            "group_part_count": 1,
            "group_parts": unique_parts,
            "needs_manual_split": False,
        })
        return

    for part in unique_parts:
        rows.append({
            "part_number": part,
            "description": descriptions.get(part, ""),
            "physical": None,
            "group_physical": float(group["physical"]),
            "customer_code": group["customer_code"],
            "group_part_count": len(unique_parts),
            "group_parts": unique_parts,
            "needs_manual_split": True,
        })


def parse_st_reconcile_file(path: str) -> dict[str, Any]:
    """解析辰尚盤點表，保留群組 forward-fill 語意。"""
    source_path = Path(path)
    workbook = open_workbook_any(str(source_path), read_only=True, data_only=True)
    try:
        ws = workbook.worksheets[0]
        header_row, columns = _find_header_row(ws)
        current_group: dict[str, Any] | None = None
        parsed_rows: list[dict] = []

        for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
            customer = _normalize_text(_cell(row, columns["customer"]))
            raw_physical = _cell(row, columns["physical"])
            starts_group = bool(customer or raw_physical not in (None, ""))
            if starts_group:
                _finish_group(current_group, parsed_rows)
                current_group = {
                    "customer_code": customer,
                    "physical": _try_float(raw_physical),
                    "parts": [],
                    "descriptions": {},
                }

            part = _normalize_part(_cell(row, columns["part"]))
            if not part:
                continue
            if current_group is None:
                current_group = {
                    "customer_code": "",
                    "physical": 0.0,
                    "parts": [],
                    "descriptions": {},
                }
            current_group["parts"].append(part)
            desc = _normalize_text(_cell(row, columns["desc"]))
            if desc and not current_group["descriptions"].get(part):
                current_group["descriptions"][part] = desc

        _finish_group(current_group, parsed_rows)

        return {
            "sheet_name": ws.title,
            "rows": parsed_rows,
            "part_count": len(parsed_rows),
            "manual_split_count": sum(1 for row in parsed_rows if row.get("needs_manual_split")),
        }
    finally:
        workbook.close()


def _normalize_cutoff_for_query(cutoff_date: str) -> str:
    text = str(cutoff_date or "").strip()
    if not text:
        raise ValueError("cutoff_date 為必填")
    if "T" not in text and len(text) == 10:
        return f"{text}T23:59:59.999999"
    return text


def _build_summary() -> dict[str, int]:
    return {
        CATEGORY_HAVE_OURS_NOT_THEIRS: 0,
        CATEGORY_HAVE_THEIRS_NOT_OURS: 0,
        CATEGORY_QTY_MISMATCH: 0,
        CATEGORY_UNATTRIBUTED: 0,
        CATEGORY_MATCHED: 0,
    }


def _classify(diff: float, has_ours_event: bool, tol: float) -> str:
    if abs(diff) <= tol:
        return CATEGORY_MATCHED
    if diff > tol and has_ours_event:
        return CATEGORY_HAVE_OURS_NOT_THEIRS
    if diff < -tol and not has_ours_event:
        return CATEGORY_HAVE_THEIRS_NOT_OURS
    return CATEGORY_UNATTRIBUTED


def build_st_reconcile_preview(path: str, cutoff_date: str, *, tol: float = 1e-6) -> dict[str, Any]:
    parsed = parse_st_reconcile_file(path)
    part_numbers = [str(row.get("part_number") or "") for row in parsed["rows"] if row.get("part_number")]
    cutoff_for_query = _normalize_cutoff_for_query(cutoff_date)
    theoretical = theoretical_stock_with_details(cutoff_for_query, part_numbers=part_numbers)
    stock_by_part = theoretical.get("stock") or {}
    details_by_part = theoretical.get("order_details") or {}

    combined: dict[str, dict] = {}
    for row in parsed["rows"]:
        part = str(row.get("part_number") or "").strip().upper()
        if not part:
            continue
        existing = combined.setdefault(part, {
            "part_number": part,
            "description": row.get("description") or "",
            "physical": 0.0,
            "group_physical": 0.0,
            "needs_manual_split": False,
            "manual_split_notes": [],
        })
        if row.get("description") and not existing.get("description"):
            existing["description"] = row.get("description")
        if row.get("needs_manual_split"):
            existing["needs_manual_split"] = True
            existing["group_physical"] += float(row.get("group_physical") or 0)
            existing["manual_split_notes"].append(
                f"{row.get('customer_code') or '未填客戶編號'} 群組共 {row.get('group_part_count')} 個料號，總盤點數 {row.get('group_physical') or 0:g}"
            )
        else:
            existing["physical"] += float(row.get("physical") or 0)

    rows: list[dict] = []
    summary = _build_summary()
    for part in sorted(combined):
        item = combined[part]
        theoretical_qty = float(stock_by_part.get(part, 0.0))
        notes: list[str] = []
        if item.get("needs_manual_split"):
            category = CATEGORY_UNATTRIBUTED
            physical: float | None = None
            diff: float | None = None
            notes.append("群組多料號需人工拆分")
            notes.extend(item.get("manual_split_notes") or [])
        else:
            physical = float(item.get("physical") or 0)
            diff = round(physical - theoretical_qty, 6)
            has_ours_event = bool(details_by_part.get(part))
            category = _classify(diff, has_ours_event, tol)
            if has_ours_event:
                notes.append(f"截止日前有效 ST 領用事件 {len(details_by_part.get(part) or [])} 筆")
            if category == CATEGORY_QTY_MISMATCH:
                notes.append("盤點表未提供單號，本版不做單對單數量比對")
        summary[category] = int(summary.get(category, 0)) + 1
        rows.append({
            "part_number": part,
            "description": item.get("description") or "",
            "physical": physical,
            "theoretical": theoretical_qty,
            "diff": diff,
            "category": category,
            "notes": notes,
        })

    categories = defaultdict(list)
    for row in rows:
        categories[row["category"]].append(row)

    return {
        "cutoff_date": str(cutoff_date or "").strip(),
        "sheet_name": parsed["sheet_name"],
        "parts": rows,
        "summary": summary,
        "categories": dict(categories),
        "assumptions": ASSUMPTIONS,
    }
