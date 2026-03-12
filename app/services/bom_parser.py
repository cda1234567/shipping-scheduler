from __future__ import annotations
from pathlib import Path
from ..config import cfg
from .xls_reader import open_workbook_any
from ..models import BomFile, BomComponent

_DASH_LIKE = {"-", "—", "－", "–", "x", "X", "n", "N", "?", "N/A", "n/a", "無"}

# 客供料關鍵字（在說明或備註中出現表示客供料）
_CUSTOMER_SUPPLIED_KEYWORDS = {"客供", "客供料", "CUST", "customer supplied", "CS"}


def _try_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _is_dash(v) -> bool:
    return v is not None and str(v).strip() in _DASH_LIKE


def _is_customer_supplied(desc: str) -> bool:
    """根據說明欄判斷是否為客供料。"""
    upper = desc.upper()
    for kw in _CUSTOMER_SUPPLIED_KEYWORDS:
        if kw.upper() in upper:
            return True
    return False


def parse_bom(path: str, bom_id: str, filename: str, uploaded_at: str) -> BomFile:
    """
    解析 BOM 副檔（領料單）：
      Row 1：PO#、訂單數量
      Row 2：機種、PCB型號
      Row 5+：components
    """
    part_col = cfg("excel.bom_part_col", 2)
    desc_col = cfg("excel.bom_desc_col", 3)
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
    ws = wb.worksheets[0]
    all_rows = list(ws.iter_rows(min_row=1, values_only=True))
    wb.close()

    if len(all_rows) < 2:
        raise ValueError("BOM 檔案格式錯誤：行數不足")

    # Row 1（index 0）
    row1 = all_rows[0]
    po_raw = row1[po_col] if len(row1) > po_col else None
    try:
        po = int(float(po_raw)) if po_raw is not None and str(po_raw).strip() else 0
    except (ValueError, TypeError):
        po = 0
    order_qty_raw = row1[oq_col] if len(row1) > oq_col else None
    order_qty = _try_float(order_qty_raw) or 0.0

    # Row 2（index 1）
    row2 = all_rows[1]
    model = str(row2[model_col] or "").strip() if len(row2) > model_col else ""
    pcb = str(row2[pcb_col] or "").strip() if len(row2) > pcb_col else ""
    if not order_qty:
        order_qty = _try_float(row2[6]) or 0.0

    # Row data_start+（index data_start-1）
    components: list[BomComponent] = []
    for row_number, row_vals in enumerate(all_rows[data_start - 1:], start=data_start):
        if not row_vals or len(row_vals) <= f_col:
            continue

        part = str(row_vals[part_col] or "").strip() if len(row_vals) > part_col else ""
        if not part:
            continue

        g_raw = row_vals[g_col] if len(row_vals) > g_col else None
        h_raw = row_vals[h_col] if len(row_vals) > h_col else None
        is_dash_flag = _is_dash(g_raw) or _is_dash(h_raw)

        needed_qty = _try_float(row_vals[f_col]) or 0.0
        prev_cs = _try_float(h_raw) or 0.0
        qty_per = _try_float(row_vals[qty_col] if len(row_vals) > qty_col else None) or 0.0
        desc = str(row_vals[desc_col] or "").strip() if len(row_vals) > desc_col else ""

        components.append(BomComponent(
            part_number=part,
            description=desc,
            qty_per_board=qty_per,
            needed_qty=needed_qty,
            prev_qty_cs=prev_cs,
            is_dash=is_dash_flag,
            is_customer_supplied=_is_customer_supplied(desc),
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
