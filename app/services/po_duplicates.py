"""重複開採購單保護：用本站自己的採購單紀錄（erp_purchase_orders + erp_po_line_allocations，限同一個寫入公司）。

- 已開過的判斷（find_matches）：
  * 這行綁了訂單、之前那張採購單的同料號也綁了訂單 → 訂單有交集才算已開；
  * 這行綁了訂單、之前那張只有 manual / 備品 → 只看料號，且限 PART_ALONE_DAYS 天內（避免舊的手動單永遠擋住）；
  * 這行沒綁訂單（手動開採購單）→ 同供應商、同料號、「今天」已開過才算。
- 防連點（fingerprint）：同公司、同供應商、同明細（料號 / 數量 / 單價 / 預計進貨日）在 IDEMPOTENCY_SECONDS 秒內
  已建立成功，或相同內容正在建立中，一律拒絕，即使 allow_duplicate 也一樣。
本模組只讀寫本機 SQLite，不碰正航。
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

from .. import database as db
from .local_time import local_now

logger = logging.getLogger(__name__)

PART_ALONE_DAYS = 30
IDEMPOTENCY_SECONDS = 60
RECENT_LIMIT = 500

_INFLIGHT: set = set()
_INFLIGHT_LOCK = threading.Lock()


def _norm(part: Any) -> str:
    return str(part or "").strip().upper()


def _day(text: Any) -> Optional[date]:
    try:
        return datetime.fromisoformat(str(text)).date()
    except ValueError:
        return None


def load_prior(target: str, parts: Iterable[str]) -> Dict[str, List[Dict[str, Any]]]:
    """同一寫入公司、狀態成功的本站採購單中，含指定料號的明細：{料號: [{po_id, bill_no, supplier_id, date, created_at, qty, order_ids}]}。
    讀取失敗回空（保護只是提醒，不可因此擋住開單）。"""
    keys = {_norm(p) for p in parts if _norm(p)}
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not keys or not target:
        return out
    try:
        with db.get_conn() as conn:
            pos = [dict(r) for r in conn.execute(
                "SELECT id, bill_no, supplier_id, created_at, lines_json FROM erp_purchase_orders "
                "WHERE status='ok' AND target_db=? ORDER BY id DESC LIMIT ?", (target, RECENT_LIMIT)).fetchall()]
        hits: Dict[int, Dict[str, float]] = {}
        for po in pos:
            try:
                lines = json.loads(po["lines_json"] or "[]")
            except ValueError:
                continue
            for line in lines:
                part = _norm(line.get("part_number"))
                if part in keys:
                    hits.setdefault(po["id"], {})
                    hits[po["id"]][part] = hits[po["id"]].get(part, 0.0) + float(line.get("qty") or 0)
        if not hits:
            return out
        order_ids: Dict[tuple, set] = {}
        for a in db.get_po_line_allocations(list(hits)):
            if a.get("kind") == "order" and int(a.get("order_id") or 0) > 0:
                order_ids.setdefault((int(a["erp_po_id"]), _norm(a["part_number"])), set()).add(int(a["order_id"]))
        for po in pos:
            for part, qty in (hits.get(po["id"]) or {}).items():
                out.setdefault(part, []).append({
                    "po_id": po["id"], "bill_no": po["bill_no"], "supplier_id": po["supplier_id"],
                    "created_at": po["created_at"], "date": str(po["created_at"])[:10], "qty": qty,
                    "order_ids": sorted(order_ids.get((po["id"], part), set())),
                })
    except Exception as exc:
        logger.warning("讀取本站採購單紀錄失敗，略過重複檢查：%s", type(exc).__name__)
        return {}
    return out


def find_matches(
    prior: List[Dict[str, Any]], order_ids: Iterable[int], supplier_id: str, today: Optional[date] = None
) -> List[Dict[str, Any]]:
    today = today or local_now().date()
    current = {int(i) for i in order_ids if i and int(i) > 0}
    supplier = str(supplier_id or "").strip()
    matches = []
    for e in prior:
        prior_ids = set(e.get("order_ids") or [])
        if current and prior_ids:
            ok = bool(current & prior_ids)
        elif current:
            d = _day(e.get("date"))
            ok = d is not None and 0 <= (today - d).days <= PART_ALONE_DAYS
        else:
            ok = str(e.get("supplier_id") or "").strip() == supplier and e.get("date") == today.isoformat()
        if ok:
            matches.append({k: e[k] for k in ("bill_no", "date", "qty", "supplier_id")})
    return matches


def find_duplicates(req: Dict[str, Any], target: str, today: Optional[date] = None) -> List[Dict[str, Any]]:
    """req 為 erp_purchase.normalize_request 的結果。回傳已開過的明細 [{line_no, part_number, qty, supplier_id, matches}]。"""
    prior = load_prior(target, [l["part_number"] for l in req["lines"]])
    found = []
    for i, line in enumerate(req["lines"], 1):
        ids = [a["order_id"] for a in line.get("allocations") or [] if a.get("order_id")]
        matches = find_matches(prior.get(_norm(line["part_number"]), []), ids, req["supplier_id"], today)
        if matches:
            found.append({
                "line_no": i, "part_number": line["part_number"], "qty": float(line["qty"]),
                "supplier_id": req["supplier_id"], "matches": matches,
            })
    return found


# ── 防連點 / 冪等 ─────────────────────────────────────────────────────────

def _line_key(part: Any, qty: Any, price: Any, pre_in: Any) -> list:
    return [_norm(part), round(float(qty), 6), round(float(price), 6), str(pre_in)]


def fingerprint(target: str, req: Dict[str, Any]) -> str:
    """req 為 normalize_request 結果（預計進貨日為 YYYYMMDD 整數）。"""
    lines = sorted(
        _line_key(l["part_number"], l["qty"], l["price"], _fmt_int_date(l["pre_in_date"])) for l in req["lines"]
    )
    raw = json.dumps([target, str(req["supplier_id"]).strip(), lines], ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _fmt_int_date(value: Any) -> str:
    try:
        v = int(value)
        return f"{v // 10000:04d}-{v // 100 % 100:02d}-{v % 100:02d}"
    except (TypeError, ValueError):
        return str(value)


def recent_identical(target: str, req: Dict[str, Any], now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """IDEMPOTENCY_SECONDS 秒內是否已成功建立內容完全相同的採購單；有則回 {bill_no, seconds_ago}。"""
    now = now or local_now()
    want = fingerprint(target, req)
    try:
        with db.get_conn() as conn:
            rows = [dict(r) for r in conn.execute(
                "SELECT bill_no, supplier_id, created_at, lines_json FROM erp_purchase_orders "
                "WHERE status='ok' AND target_db=? ORDER BY id DESC LIMIT 20", (target,)).fetchall()]
        for r in rows:
            try:
                created = datetime.fromisoformat(str(r["created_at"]))
            except ValueError:
                continue
            age = (now - created).total_seconds()
            if age > IDEMPOTENCY_SECONDS:
                break  # 依 id 由新到舊，更早的不用看
            lines = json.loads(r["lines_json"] or "[]")
            got = sorted(_line_key(l["part_number"], l["qty"], l["price"], l["pre_in_date"]) for l in lines)
            raw = json.dumps([target, str(r["supplier_id"]).strip(), got], ensure_ascii=False)
            if hashlib.sha256(raw.encode("utf-8")).hexdigest() == want:
                return {"bill_no": r["bill_no"], "seconds_ago": max(0, int(age))}
    except Exception as exc:
        logger.warning("冪等檢查失敗，略過：%s", type(exc).__name__)
    return None


def claim(fp: str) -> bool:
    """相同內容正在建立中則回 False。"""
    with _INFLIGHT_LOCK:
        if fp in _INFLIGHT:
            return False
        _INFLIGHT.add(fp)
        return True


def release(fp: str) -> None:
    with _INFLIGHT_LOCK:
        _INFLIGHT.discard(fp)
