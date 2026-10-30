"""一次性修正：排程表出貨日「m/d」被存成輸入當年（實際應為隔年）的訂單。

判斷準則（ship_date、delivery_date 各自獨立）：
- 訂單狀態不是 cancelled / completed；
- 該日期比訂單建立日（created_at）早超過 60 天 → 加一年，仍超過 60 天就繼續加。
真正的舊單（出貨日只比建立日早幾天）不會被動到。只改本站 SQLite，不碰正航。

用法（預設只列出會改什麼，不寫入）：
    python -m app.tools.fix_yearless_dates
    python -m app.tools.fix_yearless_dates --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from .. import database as db
from ..services.date_infer import WINDOW_DAYS

SKIP_STATUSES = ("cancelled", "completed")
DATE_FIELDS = ("ship_date", "delivery_date")


def _parse(text: Any) -> Optional[date]:
    try:
        return datetime.strptime(str(text or "").strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def shifted_date(value: Any, created_at: Any, window_days: int = WINDOW_DAYS) -> Optional[str]:
    """需要修正時回傳新的 YYYY-MM-DD，否則回 None。"""
    d, created = _parse(value), _parse(created_at)
    if d is None or created is None:
        return None
    limit = created - timedelta(days=window_days)
    shifted = d
    while shifted < limit:
        try:
            shifted = shifted.replace(year=shifted.year + 1)
        except ValueError:  # 2/29 → 非閏年，無法表示就不動
            return None
    return shifted.isoformat() if shifted != d else None


def plan_fixes() -> List[Dict[str, Any]]:
    with db.get_conn() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT id, code, po_number, model, status, ship_date, delivery_date, created_at FROM orders ORDER BY id"
        ).fetchall()]
    changes = []
    for row in rows:
        if row["status"] in SKIP_STATUSES:
            continue
        fields = {}
        for field in DATE_FIELDS:
            new = shifted_date(row.get(field), row.get("created_at"))
            if new:
                fields[field] = {"old": row[field], "new": new}
        if fields:
            changes.append({
                "id": row["id"], "code": row["code"], "po_number": row["po_number"], "model": row["model"],
                "status": row["status"], "created_at": row["created_at"], "fields": fields,
            })
    return changes


def run(apply: bool = False) -> List[Dict[str, Any]]:
    """dry-run（預設）只回傳變更清單；apply=True 才寫入並記錄 activity_logs。"""
    changes = plan_fixes()
    if apply and changes:
        for change in changes:
            db.update_order(change["id"], **{f: v["new"] for f, v in change["fields"].items()})
        detail = "；".join(
            f"{c['code'] or c['id']}：" + "、".join(f"{f} {v['old']}→{v['new']}" for f, v in c["fields"].items())
            for c in changes
        )
        db.log_activity("fix_yearless_dates", f"修正無年份出貨日 {len(changes)} 張訂單：{detail}"[:2000])
    return changes


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="修正被存成當年的無年份出貨日（預設只列出，不寫入）")
    parser.add_argument("--apply", action="store_true", help="實際寫入資料庫")
    args = parser.parse_args(argv)
    changes = run(apply=args.apply)
    if not changes:
        print("沒有需要修正的訂單。")
        return 0
    for c in changes:
        detail = "、".join(f"{f} {v['old']} → {v['new']}" for f, v in c["fields"].items())
        print(f"#{c['id']} {c['code']} PO {c['po_number']} {c['model']}（{c['status']}，建立 {str(c['created_at'])[:10]}）：{detail}")
    print(f"共 {len(changes)} 張訂單{'已修正' if args.apply else '需要修正（dry-run，加 --apply 才會寫入）'}。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
