from __future__ import annotations
from datetime import date, datetime
from ..config import cfg
from .date_infer import apply_yearless, parse_yearless_text
from .local_time import local_now
from .xls_reader import open_workbook_any
from ..models import ScheduleRow


def _to_date_str(v, number_format: str = "", today: date | None = None) -> str | None:
    """number_format 是儲存格的數字格式；沒有年份的格式（m/d）或文字（1/29）依今天推斷年份，明確年份不動。"""
    if v is None:
        return None
    if isinstance(v, datetime):
        return apply_yearless(v, number_format, today).strftime("%Y-%m-%d")
    try:
        s = str(v).strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        inferred = parse_yearless_text(s, today)
        if inferred is not None:
            return inferred.strftime("%Y-%m-%d")
    except Exception:
        pass
    return None


def _try_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(float(str(v).strip()))
    except (ValueError, TypeError):
        return None


def _try_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def parse_schedule(path: str, today: date | None = None) -> list[ScheduleRow]:
    """
    解析排程表 PCB sheet。
    表頭在 row 3（1-based），資料從 config 設定行開始。
    """
    po_col = cfg("excel.sch_po_col", 0)
    model_col = cfg("excel.sch_model_col", 1)
    pcb_col = cfg("excel.sch_pcb_col", 2)
    qty_col = cfg("excel.sch_qty_col", 3)
    balance_col = cfg("excel.sch_balance_col", 5)
    ship_date_col = cfg("excel.sch_ship_date_col", 6)
    remark_col = cfg("excel.sch_remark_col", 8)
    data_start = cfg("excel.sch_data_start_row", 4)
    sheet_name = cfg("excel.sch_sheet_name", "PCB")

    today = today or local_now().date()
    wb = open_workbook_any(path, read_only=True, data_only=True)

    ws = None
    for sheet in wb.worksheets:
        if sheet.title.strip().upper() == sheet_name.upper():
            ws = sheet
            break
    if ws is None:
        ws = wb.worksheets[0]

    rows: list[ScheduleRow] = []
    for r_idx, row_cells in enumerate(ws.iter_rows(min_row=data_start), start=data_start):
        # 逐格讀取（不用 values_only）才拿得到出貨日儲存格的 number_format；EmptyCell 沒有該屬性
        row_vals = tuple(getattr(c, "value", None) for c in row_cells)
        if not row_vals or len(row_vals) < ship_date_col + 1:
            continue

        po = _try_int(row_vals[po_col])
        if not po:
            continue

        model = str(row_vals[model_col] or "").strip()
        pcb = str(row_vals[pcb_col] or "").strip()
        order_qty = _try_float(row_vals[qty_col]) or 0.0
        balance = _try_float(row_vals[balance_col])
        ship_date = _to_date_str(
            row_vals[ship_date_col], getattr(row_cells[ship_date_col], "number_format", ""), today
        )
        remark = str(row_vals[remark_col] or "").strip() if len(row_vals) > remark_col else ""

        if not pcb:
            continue

        rows.append(ScheduleRow(
            po_number=po,
            model=model,
            pcb=pcb,
            order_qty=order_qty,
            balance_qty=balance,
            ship_date=ship_date,
            remark=remark,
            row_index=r_idx,
        ))

    wb.close()
    return rows
