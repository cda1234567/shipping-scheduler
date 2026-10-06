"""將已核對的主檔不良／多打併入盤點；原始紀錄由主檔註記封存。"""
from __future__ import annotations

from collections import Counter, defaultdict
import math

from .main_file_recalc import _stock_events

COUNT_DEFECT_HEADERS = {"不良品扣帳", "加工多打扣帳", "不良品回復", "加工多打回復"}


def plan_count_absorption(ws, start_col: int, parts: list[str], records: list[dict], *,
                          cutoff_at: str, max_record_id: int, excluded_record_ids=(),
                          end_col: int | None = None, current_period_id: int | None = None) -> dict:
    """只規劃指定範圍；逐料／類型／數量完整對上才標示 DB 紀錄已封存。"""
    selected = set(parts)
    rows = {str(ws.cell(r, 1).value or '').strip().upper(): r for r in range(2, ws.max_row + 1)}
    by_part = {part: {'delta': 0.0, 'cells': []} for part in selected}
    quantities = defaultdict(Counter)
    for event in _stock_events(ws):
        col, balance = int(event['start_col']), int(event['balance_col'])
        header = str(ws.cell(1, col).value or '').strip()
        if col < start_col or (end_col is not None and balance > end_col) or header not in COUNT_DEFECT_HEADERS:
            continue
        for part in selected:
            row = rows.get(part)
            if row is None:
                raise ValueError(f'主檔找不到盤點料號 {part}')
            supply = ws.cell(row, col).value
            usage = ws.cell(row, col + 1).value if balance == col + 2 else None
            if supply is None and usage is None:
                continue
            if any(value is not None and (not isinstance(value, (int, float)) or not math.isfinite(value))
                   for value in (supply, usage)):
                raise ValueError(f'主檔料號 {part} 不良品／多打數量無法安全併入盤點')
            if balance == col + 2:
                delta = float(supply or 0) - float(usage or 0)
                deduction = float(usage or 0)
            else:
                deduction = float(supply or 0)
                delta = deduction if event['kind'] == 'reverse' else -deduction
            item = by_part[part]
            item['delta'] += delta
            item['cells'].extend((row, c, ws.cell(row, c).value) for c in range(col, balance + 1))
            if event['kind'] == 'deduct' and deduction:
                kind = 'overrun' if '多打' in header else 'defect'
                quantities[(part, kind)][round(deduction, 6)] += 1

    excluded = set(excluded_record_ids)
    candidates = defaultdict(list)
    for record in records:
        part = str(record.get('part_number') or '').strip().upper()
        record_id = int(record.get('id') or 0)
        if record.get('status', 'open') not in {'open', 'confirmed', 'closed'}:
            continue
        if (current_period_id is not None and record.get('main_period_id') is not None
                and int(record['main_period_id']) != current_period_id):
            continue
        if (part not in selected or record_id in excluded or not 0 < record_id <= max_record_id
                or str(record.get('created_at') or '') <= cutoff_at):
            continue
        kind = 'overrun' if record.get('action_taken') == '加工多打扣帳' else 'defect'
        candidates[(part, kind)].append(record)
    absorbed_ids = []
    for key, ledger in quantities.items():
        items = candidates.get(key, [])
        # 已刪除或回復的歷史扣帳仍可由主檔確認數量，但不虛構 DB 對應。
        if not items:
            continue
        actual = Counter(round(float(item.get('defective_qty') or 0), 6) for item in items)
        if ledger != actual:
            raise ValueError(f'料號 {key[0]} 的不良品／多打明細與主檔不一致，請先核對，未寫入任何資料')
        absorbed_ids.extend(int(item['id']) for item in items)
    return {'parts': by_part, 'record_ids': sorted(absorbed_ids),
            'cleared_cells': sum(len(item['cells']) for item in by_part.values())}


def clear_absorbed_cells(ws, plan: dict) -> None:
    for item in plan['parts'].values():
        for row, col, _ in item['cells']:
            ws.cell(row, col).value = None
