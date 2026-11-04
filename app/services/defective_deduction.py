"""
不良品 Excel 匯入 + 主檔扣帳邏輯。

副檔格式（BOM 領料單）：
  Row 1: PO# (col H)
  Row 2: 機種 (col C), PCB (col D)
  Row 5+: 料號 (col C), 說明 (col D), 不良數量 (col F)
"""
from __future__ import annotations

from copy import copy
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill

from ..config import cfg
from .. import database as db
from ..snapshot_sync import refresh_snapshot_from_main
from .local_time import local_now
from .xls_reader import open_workbook_any
from .merge_to_main import (
    PART_COL,
    RED_FILL,
    STOCK_SEARCH_START_COL,
    _build_part_row_map,
    _read_latest_stock,
    _round_away,
    backup_main_file,
    _save_workbook_atomically,
    clear_cell_fill,
    ensure_main_header_wrap,
)


def _try_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def parse_defective_excel(path: str) -> list[dict]:
    """解析副檔格式的不良品 Excel，回傳 [{part_number, description, defective_qty}]。"""
    part_col = cfg("excel.bom_part_col", 2)
    desc_col = cfg("excel.bom_desc_col", 3)
    qty_col = cfg("excel.bom_needed_col", 5)
    data_start = cfg("excel.bom_data_start_row", 5)

    wb = open_workbook_any(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    all_rows = list(ws.iter_rows(min_row=1, values_only=True))
    wb.close()

    items: list[dict] = []
    for offset, row_vals in enumerate(all_rows[data_start - 1:], start=data_start):
        if not row_vals or len(row_vals) <= qty_col:
            continue

        part = str(row_vals[part_col] or "").strip().upper()
        if not part:
            continue

        qty = _try_float(row_vals[qty_col])
        if not qty or qty <= 0:
            continue

        desc = str(row_vals[desc_col] or "").strip() if len(row_vals) > desc_col else ""
        items.append({
            "source_row": offset,
            "part_number": part,
            "description": desc,
            "defective_qty": qty,
        })

    return items


HEADER_FONT = Font(bold=True, size=9)
HEADER_FILL = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
REVERSE_HEADER_FILL = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
CENTER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _get_main_worksheet(workbook):
    if getattr(workbook, "worksheets", None):
        return workbook.worksheets[0]
    return workbook.active


def _copy_column_layout(ws, source_col: int, target_col: int):
    if source_col <= 0 or target_col <= 0 or source_col == target_col:
        return

    source_letter = openpyxl.utils.get_column_letter(source_col)
    target_letter = openpyxl.utils.get_column_letter(target_col)
    source_dimension = ws.column_dimensions[source_letter]
    target_dimension = ws.column_dimensions[target_letter]

    for attr in ("width", "hidden", "bestFit", "style", "outlineLevel", "collapsed"):
        try:
            setattr(target_dimension, attr, copy(getattr(source_dimension, attr)))
        except Exception:
            continue

    for row_idx in range(1, ws.max_row + 1):
        source_cell = ws.cell(row=row_idx, column=source_col)
        target_cell = ws.cell(row=row_idx, column=target_col)
        target_cell._style = copy(source_cell._style)
        target_cell.number_format = source_cell.number_format
        target_cell.protection = copy(source_cell.protection)
        target_cell.alignment = copy(source_cell.alignment)
        target_cell.font = copy(source_cell.font)
        target_cell.fill = copy(source_cell.fill)
        target_cell.border = copy(source_cell.border)


def _prepare_new_entry_columns(ws, max_col: int, col_supply: int, col_usage: int, col_stock: int):
    source_stock_col = max_col
    source_delta_col = max_col - 1 if max_col - 1 >= STOCK_SEARCH_START_COL else source_stock_col
    _copy_column_layout(ws, source_delta_col, col_supply)
    _copy_column_layout(ws, source_delta_col, col_usage)
    _copy_column_layout(ws, source_stock_col, col_stock)


def deduct_defectives_from_main(
    main_path: str,
    items: list[dict],
    backup_dir: str | None = None,
    entry_header: str = "不良品扣帳",
    batch_marker: str = "",
) -> dict:
    """
    在主檔新增 3 欄扣帳：補料/加回 + 使用/扣帳 + 扣帳後庫存。

    batch_marker：有給時在扣帳表頭加註解（例 defective-batch:29），方便日後定位這批扣帳欄位。

    回傳 {"backup_path", "deducted_count", "skipped_parts", "results"}。
    """
    if not items:
        return {"backup_path": None, "deducted_count": 0, "skipped_parts": [], "results": []}

    backup_path = backup_main_file(main_path, backup_dir) if backup_dir else None

    is_xlsm = Path(main_path).suffix.lower() == ".xlsm"
    wb = openpyxl.load_workbook(main_path, keep_vba=is_xlsm)
    ws = _get_main_worksheet(wb)
    part_row_map = _build_part_row_map(ws)
    max_col = ws.max_column

    col_supply = max_col + 1
    col_deduct = max_col + 2
    col_stock = max_col + 3
    _prepare_new_entry_columns(ws, max_col, col_supply, col_deduct, col_stock)

    # 寫表頭
    ts_label = local_now().strftime("%m/%d %H:%M")
    deduct_header = ws.cell(row=1, column=col_supply)
    deduct_header.value = str(entry_header or "不良品扣帳").strip() or "不良品扣帳"
    if batch_marker:
        from openpyxl.comments import Comment
        deduct_header.comment = Comment(str(batch_marker), "shipping-scheduler")
    deduct_header.font = HEADER_FONT
    deduct_header.fill = HEADER_FILL
    deduct_header.alignment = CENTER_ALIGN

    usage_header = ws.cell(row=1, column=col_deduct)
    usage_header.value = "使用數量"
    usage_header.font = HEADER_FONT
    usage_header.fill = HEADER_FILL
    usage_header.alignment = CENTER_ALIGN

    stock_header = ws.cell(row=1, column=col_stock)
    stock_header.value = ts_label
    stock_header.font = HEADER_FONT
    stock_header.fill = HEADER_FILL
    stock_header.alignment = CENTER_ALIGN

    results: list[dict] = []
    skipped: list[str] = []

    for item in items:
        part_upper = item["part_number"].upper()
        row_idx = part_row_map.get(part_upper)
        if row_idx is None:
            skipped.append(part_upper)
            continue

        current_stock = _read_latest_stock(ws, row_idx, max_col)
        deduct_qty = float(item["defective_qty"])
        new_stock = _round_away(current_stock - deduct_qty)

        supply_cell = ws.cell(row=row_idx, column=col_supply)
        supply_cell.value = None
        clear_cell_fill(supply_cell)
        deduct_cell = ws.cell(row=row_idx, column=col_deduct)
        deduct_cell.value = _round_away(deduct_qty)
        clear_cell_fill(deduct_cell)
        stock_cell = ws.cell(row=row_idx, column=col_stock)
        stock_cell.value = new_stock
        if new_stock < 0:
            stock_cell.fill = RED_FILL
        else:
            clear_cell_fill(stock_cell)

        results.append({
            "part_number": part_upper,
            "description": item.get("description", ""),
            "defective_qty": deduct_qty,
            "stock_before": current_stock,
            "stock_after": new_stock,
        })

    ensure_main_header_wrap(ws)
    wb.save(main_path)
    wb.close()

    return {
        "backup_path": backup_path,
        "deducted_count": len(results),
        "skipped_parts": skipped,
        "results": results,
    }


def reverse_defectives_from_main(
    main_path: str,
    items: list[dict],
    backup_dir: str | None = None,
    entry_header: str = "不良品回復",
    reversal_id: str = "",
) -> dict:
    """
    將已扣帳的數量加回主檔（刪除批次時用）。

    items: [{part_number, defective_qty}]
    在主檔新增 3 欄：補料/加回 + 使用/扣帳 + 回復後庫存。
    回傳 {"backup_path", "reversed_count", "skipped_parts", "results"}。
    """
    if not items:
        return {"backup_path": None, "reversed_count": 0, "skipped_parts": [], "results": []}

    backup_path = backup_main_file(main_path, backup_dir) if backup_dir else None

    is_xlsm = Path(main_path).suffix.lower() == ".xlsm"
    wb = openpyxl.load_workbook(main_path, keep_vba=is_xlsm)
    try:
        return _reverse_defectives_workbook(wb, main_path, items, entry_header, reversal_id, backup_path)
    finally:
        wb.close()


def _reverse_defectives_workbook(wb, main_path: str, items: list[dict], entry_header: str,
                                reversal_id: str, backup_path: str | None) -> dict:
    ws = _get_main_worksheet(wb)
    part_row_map = _build_part_row_map(ws)
    max_col = ws.max_column

    col_reverse = max_col + 1
    col_usage = max_col + 2
    col_stock = max_col + 3
    _prepare_new_entry_columns(ws, max_col, col_reverse, col_usage, col_stock)

    ts_label = local_now().strftime("%m/%d %H:%M")
    reverse_header = ws.cell(row=1, column=col_reverse)
    reverse_header.value = str(entry_header or "不良品回復").strip() or "不良品回復"
    if reversal_id:
        from openpyxl.comments import Comment
        reverse_header.comment = Comment(f"defective-reversal:{reversal_id}", "shipping-scheduler")
    reverse_header.font = HEADER_FONT
    reverse_header.fill = REVERSE_HEADER_FILL
    reverse_header.alignment = CENTER_ALIGN

    usage_header = ws.cell(row=1, column=col_usage)
    usage_header.value = "使用數量"
    usage_header.font = HEADER_FONT
    usage_header.fill = REVERSE_HEADER_FILL
    usage_header.alignment = CENTER_ALIGN

    stock_header = ws.cell(row=1, column=col_stock)
    stock_header.value = ts_label
    stock_header.font = HEADER_FONT
    stock_header.fill = REVERSE_HEADER_FILL
    stock_header.alignment = CENTER_ALIGN

    results: list[dict] = []
    skipped: list[str] = []

    for item in items:
        part_upper = item["part_number"].upper()
        row_idx = part_row_map.get(part_upper)
        if row_idx is None:
            skipped.append(part_upper)
            continue

        current_stock = _read_latest_stock(ws, row_idx, max_col)
        reverse_qty = float(item["defective_qty"])
        new_stock = _round_away(current_stock + reverse_qty)

        reverse_cell = ws.cell(row=row_idx, column=col_reverse)
        reverse_cell.value = _round_away(reverse_qty)
        clear_cell_fill(reverse_cell)
        usage_cell = ws.cell(row=row_idx, column=col_usage)
        usage_cell.value = None
        clear_cell_fill(usage_cell)
        stock_cell = ws.cell(row=row_idx, column=col_stock)
        stock_cell.value = new_stock
        if new_stock < 0:
            stock_cell.fill = RED_FILL
        else:
            clear_cell_fill(stock_cell)

        results.append({
            "part_number": part_upper,
            "reverse_qty": reverse_qty,
            "stock_before": current_stock,
            "stock_after": new_stock,
        })

    ensure_main_header_wrap(ws)
    _save_workbook_atomically(wb, main_path)

    return {
        "backup_path": backup_path,
        "reversed_count": len(results),
        "skipped_parts": skipped,
        "results": results,
    }


OVERRUN_DEDUCT_HEADER = "加工多打扣帳"
DEFECTIVE_DEDUCT_HEADER = "不良品扣帳"


def replay_defectives_after(main_path: str, cutoff: str) -> dict:
    records = db.get_defective_records_after(cutoff)
    from .inventory_restore_guard import is_count_protected_record, COUNT_HISTORY_MESSAGE
    if any(is_count_protected_record(record) for record in records):
        raise ValueError(COUNT_HISTORY_MESSAGE)
    if not records:
        refresh_snapshot_from_main(main_path)
        return {"replayed_batches": 0, "replayed_records": 0, "skipped_parts": []}

    grouped: list[dict] = []
    batch_map: dict[int, dict] = {}
    for row in records:
        batch_id = int(row.get("batch_id") or 0)
        if batch_id not in batch_map:
            batch_map[batch_id] = {"batch_id": batch_id, "records": []}
            grouped.append(batch_map[batch_id])
        batch_map[batch_id]["records"].append(row)

    replayed_batches = 0
    replayed_records = 0
    skipped_parts: list[str] = []
    replayed_batch_ids: list[int] = []
    for group in grouped:
        items = [
            {
                "part_number": row.get("part_number"),
                "description": row.get("description", ""),
                "defective_qty": row.get("defective_qty"),
            }
            for row in group["records"]
            if float(row.get("defective_qty") or 0) > 0 and str(row.get("part_number") or "").strip()
        ]
        if not items:
            continue

        is_overrun = any(
            str(row.get("action_taken") or "").strip() == OVERRUN_DEDUCT_HEADER
            for row in group["records"]
        )
        result = deduct_defectives_from_main(
            main_path,
            items,
            backup_dir=None,
            entry_header=OVERRUN_DEDUCT_HEADER if is_overrun else DEFECTIVE_DEDUCT_HEADER,
            batch_marker=f"{BATCH_MARKER_PREFIX}{group['batch_id']}" if group["batch_id"] > 0 else "",
        )
        if group["batch_id"] > 0 and int(result.get("deducted_count") or 0) > 0:
            replayed_batch_ids.append(group["batch_id"])
        replayed_batches += 1
        replayed_records += int(result.get("deducted_count") or 0)
        skipped_parts.extend(result.get("skipped_parts") or [])

    if replayed_batch_ids:
        # 補回後主檔已換成「退回備份 + 重新扣帳」，把批次的主檔時間戳記更新成補回後的主檔，
        # 之後刪除批次時才會回寫庫存，不會誤判成「主檔已更換」。
        db.update_defective_batches_main_file_mtime(replayed_batch_ids, Path(main_path).stat().st_mtime)
    refresh_snapshot_from_main(main_path)
    return {
        "replayed_batches": replayed_batches,
        "replayed_records": replayed_records,
        "skipped_parts": skipped_parts,
        "replayed_batch_ids": replayed_batch_ids,
    }


BATCH_MARKER_PREFIX = "defective-batch:"


def _numeric(value) -> float | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def batch_expected_deductions(batch: dict) -> dict[str, float]:
    expected: dict[str, float] = {}
    for record in batch.get("items") or []:
        qty = float(record.get("defective_qty") or 0)
        part = str(record.get("part_number") or "").strip().upper()
        if qty > 0 and part:
            expected[part] = expected.get(part, 0.0) + qty
    return {part: _round_away(qty) for part, qty in expected.items()}


def locate_applied_batch_deduction(main_path: str, batch: dict) -> dict:
    """在目前主檔找出這批不良品 / 加工多打扣帳是否仍在（唯讀）。

    以內容比對：表頭為「…扣帳」的欄組，其「使用數量」欄的非空值（料號 → 數量）必須
    和批次明細完全相同；之後若有一組「…回復」欄的加回值也完全相同，視為已回復。
    回傳 {"status": "applied" | "missing" | "ambiguous", "columns": [...], "parts": n, "qty": total}；
    columns 為扣帳欄組第一欄（1 起算）。只有「恰好一組扣帳仍有效」時才是 applied。
    """
    expected_all = batch_expected_deductions(batch)
    result = {"status": "missing", "columns": [], "parts": 0, "qty": 0.0}
    if not main_path or not Path(main_path).is_file() or not expected_all:
        return result

    wb = openpyxl.load_workbook(main_path, read_only=True, data_only=False)
    try:
        ws = _get_main_worksheet(wb)
        rows = [tuple(values) for values in ws.iter_rows(min_row=1, values_only=True)]
    finally:
        wb.close()
    if not rows:
        return result

    header = list(rows[0])
    part_idx = PART_COL - 1
    row_parts: list[str] = []
    for values in rows[1:]:
        raw = values[part_idx] if len(values) > part_idx else None
        row_parts.append(str(raw or "").strip().upper())
    present = set(row_parts)
    expected = {part: qty for part, qty in expected_all.items() if part in present}
    if not expected:
        return result

    def column_values(col_idx: int) -> dict[str, float]:
        values_map: dict[str, float] = {}
        for part, values in zip(row_parts, rows[1:]):
            if not part or col_idx >= len(values):
                continue
            number = _numeric(values[col_idx])
            if number is None or number == 0:
                continue
            values_map[part] = values_map.get(part, 0.0) + number
        return values_map

    def same(found: dict[str, float]) -> bool:
        return set(found) == set(expected) and all(abs(found[p] - expected[p]) < 1e-6 for p in expected)

    applied: list[int] = []
    for idx, title in enumerate(header):
        text = str(title or "").strip()
        if text.endswith("扣帳") and same(column_values(idx + 1)):
            applied.append(idx + 1)
        elif text.endswith("回復") and applied and same(column_values(idx)):
            applied.pop()

    result["columns"] = applied
    result["parts"] = len(expected)
    result["qty"] = sum(expected.values())
    if len(applied) == 1:
        result["status"] = "applied"
    elif len(applied) > 1:
        result["status"] = "ambiguous"
    return result
