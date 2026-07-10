"""加工廠盤點對帳試算。"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from app import database as db
from app.constants import ST_RECONCILE_ADJUSTMENT_REASON

from .reconcile_core import theoretical_stock_with_details
from .xls_reader import open_workbook_any

CUSTOMER_HEADER = "客戶編號"
PART_HEADER = "汎翊國際料號"
DESC_HEADER = "品名規格"
PHYSICAL_HEADER = "辰尚填寫"
GENLIN_PART_HEADER = "consign invoice NO"
GENLIN_BOOK_HEADER = "辰尚庫存"
GENLIN_PHYSICAL_HEADER = "庚霖庫存 當下實際"
GENLIN_DESC_HEADER = "Parts No/Description"

CATEGORY_HAVE_OURS_NOT_THEIRS = "我有單他沒有"
CATEGORY_HAVE_THEIRS_NOT_OURS = "他有單我沒入"
CATEGORY_QTY_MISMATCH = "同單數量不符"
CATEGORY_UNATTRIBUTED = "無法歸因淨差"
CATEGORY_MATCHED = "無差異"
CATEGORY_STOP_LOSS = "停損吸收"
CATEGORY_GENLIN_BLANK_PHYSICAL = "未填實盤，跳過"

ASSUMPTIONS = [
    "本試算只讀取上傳盤點表，不會寫入 ST 庫存，也不會建立對齊點。",
    "盤點表沒有良品 / 不良品分欄，本版先用盤點數與系統理論良品庫存比對；差額需人工再對照未報廢不良品單。",
    "盤點表沒有工單或 MO 號，本版只能做料號級淨差歸因；同單數量不符需等盤點表提供單號後才能精準判定。",
    "H 欄視為客戶群組總盤點數；同一群組若有多個汎翊料號，會標示需人工拆分並歸入無法歸因淨差。",
]
GENLIN_ASSUMPTIONS = [
    "本試算讀取庚霖實際庫存格式：F 欄為我方帳面，G 欄為庚霖實盤。",
    "停損點模式只分「無差異」與「停損吸收」；F 與 G 的差額不再逐筆歸因。",
    "按下設為停損點後，系統會以庚霖實盤 G 欄重設每個料號的 ST 庫存基準，差額自動寫入調帳紀錄。",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_header_text(value: Any) -> str:
    return " ".join(_normalize_text(value).split())


def _normalize_part(value: Any) -> str:
    return _normalize_text(value).upper()


def _normalize_genlin_part(value: Any) -> str:
    text = _normalize_part(value)
    if text.endswith("-TAB"):
        text = text[:-4]
    return text.strip().upper()


def _normalize_part_numbers(values: list[str] | None) -> list[str] | None:
    if values is None:
        return None
    normalized = [
        part
        for value in values
        for part in [_normalize_part(value)]
        if part and part != "[]"
    ]
    return list(dict.fromkeys(normalized))


def _try_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _try_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _find_chenshang_header_row(ws) -> tuple[int, dict[str, int]]:
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


def _find_genlin_header_row(ws) -> tuple[int, dict[str, int]]:
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=min(12, ws.max_row), values_only=True), start=1):
        values = [_normalize_header_text(cell) for cell in row]
        part_col = 3 if len(values) > 3 and values[3].lower().startswith("consign") else -1
        book_col = values.index(GENLIN_BOOK_HEADER) if GENLIN_BOOK_HEADER in values else -1
        physical_col = next((idx for idx, value in enumerate(values) if value.startswith("庚霖庫存")), -1)
        if part_col < 0 or book_col < 0 or physical_col < 0:
            continue
        desc_col = next(
            (idx for idx, value in enumerate(values) if value in {GENLIN_DESC_HEADER, "Parts No/Parts Description"}),
            -1,
        )
        return row_idx, {
            "part": part_col,
            "desc": desc_col,
            "book": book_col,
            "physical": physical_col,
        }
    raise ValueError("找不到庚霖實際庫存表頭，需包含 D 欄「consign」開頭、「辰尚庫存」與「庚霖庫存」開頭")


def _detect_format(ws) -> tuple[str, int, dict[str, int]]:
    header_errors: list[str] = []
    try:
        header_row, columns = _find_genlin_header_row(ws)
        return "genlin", header_row, columns
    except ValueError as error:
        header_errors.append(str(error))
    try:
        header_row, columns = _find_chenshang_header_row(ws)
        return "chenshang", header_row, columns
    except ValueError as error:
        header_errors.append(str(error))
    raise ValueError("無法辨識盤點表格式；" + "；".join(header_errors))


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


def _parse_genlin_sheet(ws, header_row: int, columns: dict[str, int]) -> list[dict]:
    rows: list[dict] = []
    for row in ws.iter_rows(min_row=header_row + 2, values_only=True):
        part = _normalize_genlin_part(_cell(row, columns["part"]))
        if not part:
            continue
        rows.append({
            "part_number": part,
            "description": _normalize_text(_cell(row, columns["desc"])),
            "book_qty": _try_float(_cell(row, columns["book"])),
            "physical_qty": _try_optional_float(_cell(row, columns["physical"])),
        })
    return rows


def parse_st_reconcile_file(path: str) -> dict[str, Any]:
    """解析盤點表；依表頭自動偵測辰尚舊格式或庚霖實際庫存格式。"""
    source_path = Path(path)
    workbook = open_workbook_any(str(source_path), read_only=True, data_only=True)
    try:
        sheet_by_stripped_name = {str(name).strip(): name for name in workbook.sheetnames}
        sheet_name = sheet_by_stripped_name.get("實際庫存-生產結餘")
        ws = workbook[sheet_name] if sheet_name else workbook.worksheets[0]
        file_format, header_row, columns = _detect_format(ws)
        if file_format == "genlin":
            parsed_rows = _parse_genlin_sheet(ws, header_row, columns)
            return {
                "format": "genlin",
                "sheet_name": ws.title.strip(),
                "rows": parsed_rows,
                "part_count": len(parsed_rows),
                "manual_split_count": 0,
            }

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
            "format": "chenshang",
            "sheet_name": ws.title.strip(),
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


def _build_genlin_summary() -> dict[str, int]:
    return {
        CATEGORY_MATCHED: 0,
        CATEGORY_STOP_LOSS: 0,
        CATEGORY_GENLIN_BLANK_PHYSICAL: 0,
    }


def _classify(diff: float, has_ours_event: bool, tol: float) -> str:
    if abs(diff) <= tol:
        return CATEGORY_MATCHED
    if diff > tol and has_ours_event:
        return CATEGORY_HAVE_OURS_NOT_THEIRS
    if diff < -tol and not has_ours_event:
        return CATEGORY_HAVE_THEIRS_NOT_OURS
    return CATEGORY_UNATTRIBUTED


def _build_genlin_preview(parsed: dict[str, Any], cutoff_date: str, cutoff_for_query: str, tol: float) -> dict[str, Any]:
    part_numbers = [str(row.get("part_number") or "") for row in parsed["rows"] if row.get("part_number")]
    theoretical = theoretical_stock_with_details(cutoff_for_query, part_numbers=part_numbers)
    stock_by_part = theoretical.get("stock") or {}

    combined: dict[str, dict] = {}
    for row in parsed["rows"]:
        part = str(row.get("part_number") or "").strip().upper()
        if not part:
            continue
        existing = combined.setdefault(part, {
            "part_number": part,
            "description": row.get("description") or "",
            "book_qty": 0.0,
            "physical_qty": 0.0,
            "has_physical": False,
        })
        if row.get("description") and not existing.get("description"):
            existing["description"] = row.get("description")
        existing["book_qty"] += float(row.get("book_qty") or 0)
        if row.get("physical_qty") is not None:
            existing["physical_qty"] += float(row.get("physical_qty") or 0)
            existing["has_physical"] = True

    rows: list[dict] = []
    summary = _build_genlin_summary()
    for part in sorted(combined):
        item = combined[part]
        book_qty = float(item.get("book_qty") or 0)
        theoretical_qty = float(stock_by_part.get(part, 0.0))
        if item.get("has_physical"):
            physical_qty: float | None = float(item.get("physical_qty") or 0)
            book_vs_physical_diff: float | None = round(book_qty - physical_qty, 6)
            diff: float | None = round(physical_qty - theoretical_qty, 6)
            category = CATEGORY_MATCHED if abs(book_vs_physical_diff) <= tol else CATEGORY_STOP_LOSS
            notes: list[str] = []
        else:
            physical_qty = None
            book_vs_physical_diff = None
            diff = None
            category = CATEGORY_GENLIN_BLANK_PHYSICAL
            notes = ["G 欄未填實盤，commit 時不更新 ST 庫存，也不建立停損點基準"]
        summary[category] = int(summary.get(category, 0)) + 1
        rows.append({
            "part_number": part,
            "description": item.get("description") or "",
            "book_qty": book_qty,
            "physical_qty": physical_qty,
            "theoretical": theoretical_qty,
            "book_vs_physical_diff": book_vs_physical_diff,
            "diff": diff,
            "category": category,
            "notes": notes,
        })

    return {
        "format": "genlin",
        "mode": "stop_loss",
        "cutoff_date": str(cutoff_date or "").strip(),
        "sheet_name": parsed["sheet_name"],
        "parts": rows,
        "summary": summary,
        "categories": {
            CATEGORY_MATCHED: [row for row in rows if row["category"] == CATEGORY_MATCHED],
            CATEGORY_STOP_LOSS: [row for row in rows if row["category"] == CATEGORY_STOP_LOSS],
            CATEGORY_GENLIN_BLANK_PHYSICAL: [
                row for row in rows if row["category"] == CATEGORY_GENLIN_BLANK_PHYSICAL
            ],
        },
        "assumptions": GENLIN_ASSUMPTIONS,
    }


def build_st_reconcile_preview(path: str, cutoff_date: str, *, tol: float = 1e-6) -> dict[str, Any]:
    parsed = parse_st_reconcile_file(path)
    part_numbers = [str(row.get("part_number") or "") for row in parsed["rows"] if row.get("part_number")]
    cutoff_for_query = _normalize_cutoff_for_query(cutoff_date)
    if parsed.get("format") == "genlin":
        return _build_genlin_preview(parsed, cutoff_date, cutoff_for_query, tol)

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
        "format": parsed.get("format") or "chenshang",
        "mode": "attribution",
        "cutoff_date": str(cutoff_date or "").strip(),
        "sheet_name": parsed["sheet_name"],
        "parts": rows,
        "summary": summary,
        "categories": dict(categories),
        "assumptions": ASSUMPTIONS,
    }


def commit_st_reconcile_stop_loss(
    path: str,
    cutoff_date: str,
    *,
    source_filename: str = "",
    part_numbers: list[str] | None = None,
) -> dict[str, Any]:
    preview = build_st_reconcile_preview(path, cutoff_date)
    if preview.get("format") != "genlin":
        raise ValueError("停損點 commit 目前只支援庚霖實際庫存格式")
    selected_parts = _normalize_part_numbers(part_numbers)
    if part_numbers is not None and not selected_parts:
        raise ValueError("請至少勾選 1 支料號再建立停損點")
    selected_part_set = set(selected_parts or [])

    cutoff_for_anchor = _normalize_cutoff_for_query(cutoff_date)
    current_stock = db.get_st_inventory_stock()
    stock_updates: dict[str, float] = {}
    alignment_parts: list[dict] = []
    adjustments: list[dict] = []

    for row in preview.get("parts") or []:
        part = str(row.get("part_number") or "").strip().upper()
        if not part:
            continue
        if selected_parts is not None and part not in selected_part_set:
            continue
        if row.get("physical_qty") is None:
            continue
        aligned_qty = float(row.get("physical_qty") or 0)
        current_qty = float(current_stock.get(part, 0.0))
        adjust_qty = round(aligned_qty - current_qty, 6)
        stock_updates[part] = aligned_qty
        alignment_parts.append({
            "part_number": part,
            "theoretical_qty": float(row.get("theoretical") or 0),
            "physical_qty": aligned_qty,
            "diff": float(row.get("diff") or 0),
            "category": str(row.get("category") or ""),
            "aligned_qty": aligned_qty,
        })
        adjustments.append({
            "part_number": part,
            "adjust_qty": adjust_qty,
            "reason": ST_RECONCILE_ADJUSTMENT_REASON,
            "actor": "reconcile",
        })

    updated_count = db.update_st_inventory_stock(
        stock_updates,
        reason=ST_RECONCILE_ADJUSTMENT_REASON,
        actor="reconcile",
    )
    alignment_id = db.create_st_reconcile_alignment(
        aligned_at=cutoff_for_anchor,
        source_filename=source_filename or Path(path).name,
        note="停損點模式：以庚霖實盤 G 欄重設 ST 庫存基準",
        parts=alignment_parts,
        adjustments=adjustments,
    )
    summary = {
        "alignment_id": alignment_id,
        "aligned_at": cutoff_for_anchor,
        "part_count": len(alignment_parts),
        "updated_count": updated_count,
        "adjusted_count": sum(1 for row in adjustments if abs(float(row.get("adjust_qty") or 0)) > 1e-6),
        "total_abs_adjust_qty": round(sum(abs(float(row.get("adjust_qty") or 0)) for row in adjustments), 6),
    }
    return {
        "ok": True,
        "format": "genlin",
        "mode": "stop_loss",
        "summary": summary,
        "preview_summary": preview.get("summary") or {},
        "parts": alignment_parts,
        "adjustments": adjustments,
    }
