"""本站紀錄的單據是否仍存在於正航（正航為準：Andy 在正航刪掉的單，本站不可再當成已開 / 已收）。

只做唯讀 SELECT，連到本機紀錄當初寫入的那家公司；結束一律 rollback / close，不寫任何東西。
查詢失敗（連不上、沒帳密、資料庫名稱不符等）回 None，呼叫端維持原行為（照本機紀錄算）。
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

from . import erp_purchase as ep, erp_stock

logger = logging.getLogger(__name__)

CHUNK = 200  # IN (...) 一次最多幾個單號

# 單據種類 → (表頭表, Flag)
_MAIN = {"po": ("ordBillMain", 4), "receipt": ("stkBillMain", 100)}


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


@contextmanager
def _read_session(target: str) -> Iterator[Any]:
    """目標公司唯讀連線：目標就是唯讀來源公司 → 唯讀帳號；否則用目標公司的寫入帳密（只讀，結束 rollback）。"""
    settings = erp_stock.get_settings()
    if settings["database"] == target and erp_stock.is_configured(settings):
        conn = erp_stock._connect(settings)
    else:
        creds = ep.write_credentials(target)
        if creds is None:
            raise erp_stock.ErpNotConfigured(ep.UNAVAILABLE_REASON)
        conn = ep._connect_write(creds)
    try:
        ep._check_db_name(conn.cursor(), target)
        yield conn
    finally:
        ep._close_quietly(conn, rollback=True)


def _chunks(items: List[str]) -> Iterator[List[str]]:
    for i in range(0, len(items), CHUNK):
        yield items[i:i + CHUNK]


def _select(target: str, sql: str, bill_nos: Iterable[Any]) -> Optional[List[Tuple[Any, ...]]]:
    bills = sorted({str(b).strip() for b in bill_nos if str(b or "").strip()})
    if not bills:
        return []
    if not target:
        return None
    try:
        rows: List[Tuple[Any, ...]] = []
        with _read_session(target) as conn:
            cur = conn.cursor()
            for part in _chunks(bills):
                cur.execute(sql.format(marks=", ".join(["%s"] * len(part))), tuple(part))
                rows.extend(tuple(r) for r in cur.fetchall())
        return rows
    except Exception as exc:
        logger.warning("查正航單據是否存在失敗（%s），照本機紀錄算：%s", target, type(exc).__name__)
        return None


def po_line_keys(target: str, bill_nos: Iterable[Any]) -> Optional[Set[Tuple[str, str, str]]]:
    """正航目前還在的採購單明細 {(單號, 供應商, 料號)}（皆去空白、大寫）；查詢失敗回 None。"""
    rows = _select(
        target,
        "SELECT s.BillNO, m.CustomerID, s.ProdID FROM ordBillSub s "
        "JOIN ordBillMain m ON m.Flag = 4 AND m.BillNO = s.BillNO "
        "WHERE s.Flag = 4 AND s.BillNO IN ({marks})",
        bill_nos,
    )
    return None if rows is None else {(_norm(b), _norm(c), _norm(p)) for b, c, p in rows}


def bill_customers(target: str, kind: str, bill_nos: Iterable[Any]) -> Optional[Dict[str, Set[str]]]:
    """正航目前還在的單據 {單號: {供應商 / 客戶}}（kind: po / receipt）；查詢失敗回 None。
    進貨單表頭 stkBillMain 沒有 CustomerID 欄，只查單號，供應商集合為空（只能用單號比對）。"""
    table, flag = _MAIN[kind]
    cols = "BillNO" if kind == "receipt" else "BillNO, CustomerID"
    rows = _select(target, f"SELECT {cols} FROM {table} WHERE Flag = {flag} AND BillNO IN ({{marks}})", bill_nos)
    if rows is None:
        return None
    out: Dict[str, Set[str]] = {}
    for bill, *customer in rows:
        owners = out.setdefault(_norm(bill), set())
        if customer:
            owners.add(_norm(customer[0]))
    return out


def mark_deleted(records: List[Dict[str, Any]], kind: str, bill_key: str, customer_key: str = "") -> List[Dict[str, Any]]:
    """本機紀錄中 status=ok 但正航已查無此單者加上 erp_deleted=True（customer_key 有給時供應商不同也算已刪）。
    只改回傳的 dict，不動本機 SQLite；某家公司查不到（失敗）就不加旗標。"""
    by_target: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        if r.get("status") == "ok" and str(r.get(bill_key) or "").strip():
            by_target.setdefault(str(r.get("target_db") or ""), []).append(r)
    for target, rows in by_target.items():
        found = bill_customers(target, kind, [r[bill_key] for r in rows])
        if found is None:
            continue
        for r in rows:
            owners = found.get(_norm(r[bill_key]))
            if owners is None or (customer_key and _norm(r.get(customer_key)) not in owners):
                r["erp_deleted"] = True
    return records
