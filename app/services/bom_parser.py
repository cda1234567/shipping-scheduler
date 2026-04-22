from __future__ import annotations
from pathlib import Path
from ..config import cfg
from .xls_reader import open_workbook_any
from .bom_quantity import calculate_effective_needed_qty, coerce_scrap_factor
from ..models import BomFile, BomComponent

_DASH_LIKE = {"-", "—", "－", "–", "x", "X", "n", "N", "?", "N/A", "n/a", "無"}


def _try_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _is_dash(v) -> bool:
    return v is not None and str(v).strip() in _DASH_LIKE


def _is_formula(v) -> bool:
    return isinstance(v, str) and v.lstrip().startswith("=")


def _row_value(row_vals, col_idx: int):
    return row_vals[col_idx] if len(row_vals) > col_idx else None


def _extract_number(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    # 嘗試直接轉 float
    try:
        return float(s)
    except (ValueError, TypeError):
        pass
    # 嘗試從字串中擷取數字（例如 "訂單數量: 100" -> 100）
    import re
    match = re.search(r"(\d+(\.\d+)?)", s)
    if match:
        try:
            return float(match.group(1))
        except (ValueError, TypeError):
            pass
    return None


def parse_bom(path: str, bom_id: str, filename: str, uploaded_at: str) -> BomFile:
    """
    解析 BOM 副檔（領料單）：
      Row 1：PO#、訂單數量
      Row 2：機種、PCB型號
      Row 5+：components
    """
    part_col = cfg("excel.bom_part_col", 2)
    desc_col = cfg("excel.bom_desc_col", 3)
    scrap_col = cfg("excel.bom_scrap_col", 4)
    qty_col = cfg("excel.bom_qty_per_board", 1)
    f_col = cfg("excel.bom_needed_col", 5)
    g_col = cfg("excel.bom_g_col", 6)
    h_col = cfg("excel.bom_h_col", 7)
    po_col = cfg("excel.bom_po_col", 7)
    oq_col = cfg("excel.bom_order_qty_col", 10)
    model_col = cfg("excel.bom_model_col", 2)
    pcb_col = cfg("excel.bom_pcb_col", 3)
    data_start = cfg("excel.bom_data_start_row", 5)

    wb = open_workbook_any(path, read_only=True, data_only=True)
    formula_wb = open_workbook_any(path, read_only=True, data_only=False)
    ws = wb.worksheets[0]
    formula_ws = formula_wb.worksheets[0]
    all_rows = list(ws.iter_rows(min_row=1, values_only=True))
    formula_rows = list(formula_ws.iter_rows(min_row=1, values_only=True))
    wb.close()
    formula_wb.close()

    if len(all_rows) < 2:
        raise ValueError("BOM 檔案格式錯誤：行數不足")

    # Row 1（index 0）
    row1 = all_rows[0]
    po_raw = row1[po_col] if len(row1) > po_col else None
    po_extracted = _extract_number(po_raw)
    po = int(po_extracted) if po_extracted is not None else 0
    
    order_qty_raw = row1[oq_col] if len(row1) > oq_col else None
    order_qty = _extract_number(order_qty_raw) or 0.0

    # Row 2（index 1）
    row2 = all_rows[1]
    model = str(row2[model_col] or "").strip() if len(row2) > model_col else ""
    pcb = str(row2[pcb_col] or "").strip() if len(row2) > pcb_col else ""
    if not order_qty:
        order_qty = _extract_number(row2[6]) or 0.0

    # Row data_start+（index data_start-1）
    components: list[BomComponent] = []
    for row_number, row_vals in enumerate(all_rows[data_start - 1:], start=data_start):
        if not row_vals or len(row_vals) <= f_col:
            continue

        part = str(row_vals[part_col] or "").strip() if len(row_vals) > part_col else ""
        if not part:
            continue

        formula_row_vals = formula_rows[row_number - 1] if len(formula_rows) >= row_number else row_vals
        g_raw = _row_value(row_vals, g_col)
        h_raw = _row_value(row_vals, h_col)
        is_dash_flag = _is_dash(g_raw) or _is_dash(h_raw)

        qty_per = _try_float(_row_value(row_vals, qty_col)) or 0.0
        scrap_factor = coerce_scrap_factor(
            _row_value(formula_row_vals, scrap_col)
            if _row_value(formula_row_vals, scrap_col) not in (None, "")
            else _row_value(row_vals, scrap_col)
        )
        needed_raw = _row_value(row_vals, f_col)
        formula_needed_raw = _row_value(formula_row_vals, f_col)
        needed_qty = _try_float(needed_raw)
        if _is_formula(formula_needed_raw) and qty_per > 0 and order_qty > 0 and scrap_factor > 0:
            needed_qty = calculate_effective_needed_qty(
                needed_qty=0,
                qty_per_board=qty_per,
                scrap_factor=scrap_factor,
                schedule_order_qty=order_qty,
            )
        elif needed_qty is None:
            needed_qty = calculate_effective_needed_qty(
                needed_qty=0,
                qty_per_board=qty_per,
                scrap_factor=scrap_factor,
                schedule_order_qty=order_qty,
            )
        prev_cs = _try_float(h_raw) or 0.0
        desc = str(row_vals[desc_col] or "").strip() if len(row_vals) > desc_col else ""

        components.append(BomComponent(
            part_number=part,
            description=desc,
            qty_per_board=qty_per,
            scrap_factor=scrap_factor,
            needed_qty=needed_qty or 0.0,
            prev_qty_cs=prev_cs,
            is_dash=is_dash_flag,
            is_customer_supplied=False,
            source_row=row_number,
            source_sheet=ws.title,
        ))

    return BomFile(
        id=bom_id,
        filename=filename,
        path=path,
        po_number=po,
        model=model,
        pcb=pcb,
        order_qty=order_qty,
        components=components,
        uploaded_at=uploaded_at,
    )
