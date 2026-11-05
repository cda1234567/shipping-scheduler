"""缺料採購的「待調撥」：網站已扣 ST（補料給庚霖）、但正航還沒做 001→015 調撥的量。

背景：寫入主檔時網站會扣本機 ST，但每小時從正航同步 ST 時會被正航 001 的數字覆蓋；
正航的 ST 只有在人員打調撥單（Flag 400，001 → 015 庚霖倉）時才會減少。所以
「已發料但還沒調撥」的料，正航 ST 看起來還在，缺料採購會少算。

規則（B1，v2026.11.04.9）：
- 截止批次（設定 shortage_transfer_cutoff_batch，預設 10-3）：該批次（含）以前的都視為已調撥完成。
- 待調撥（每個料號）= 截止批次之後、已發料訂單的網站扣 ST 量（st_dispatch_consumptions，未退回）
  − 對應期間正航 001 → 015 的調撥量（015 → 001 退回的扣回來）。
  * 對應期間 = 「截止批次的料實際調撥的那天」之後（不含當天）。那天自動判斷：截止批次扣過的料號，
    在該批次首次發料日（含）以後，001→015 調撥單上出現最多料號的日期（同數取較晚）。
    找不到時改用截止批次首次發料日，並附警告。
  * 只算 ST 倉（ERP_ST_WARE_ID，預設 001）與庚霖倉（SHORTAGE_GL_WARE_ID，預設 015）之間；其他倉不算。
  * 網站扣量只算 ST 快照最後更新時間（st_inventory_loaded_at）以前的；之後的扣量本機 ST 已經扣過。
  * 本次勾選的訂單不算在待調撥（它們本身就是「需求」）。
- 淨值 < 0（調撥比扣的多）：待調撥當 0，多出來的顯示成「超轉」，只供參考。
- 轉換料：扣量與調撥的料號都先轉成最終料號（part_conversion.resolve）再加總。
- 批次排序：批次代碼「月-序」，年份由該批首次發料時間（沒有則建立時間）推估：
  取離那個時間最近的「該月」所在年份（例：3 月建立的 12-1 → 前一年 12 月；10 月發料的 1-2 → 隔年 1 月）。
不會寫入正航，只用唯讀帳號查 comProdRec。
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .. import database as db
from . import erp_stock, low_stock, part_conversion

logger = logging.getLogger(__name__)

SETTING_KEY = "shortage_transfer_cutoff_batch"
DEFAULT_CUTOFF = "10-3"
ACTIVE_STATUSES = ("dispatched", "completed")
IN_CHUNK = 200
_CODE_RE = re.compile(r"^(\d{1,2})-(\d+)$")


# ── 批次排序（純函式）────────────────────────────────────────────────────────

def parse_code(code: Any) -> Optional[Tuple[int, int]]:
    m = _CODE_RE.match(str(code or "").strip())
    if not m:
        return None
    month, seq = int(m.group(1)), int(m.group(2))
    return (month, seq) if 1 <= month <= 12 else None


def _parse_dt(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "")[:19]).date()
    except ValueError:
        try:
            return datetime.strptime(text[:10].replace("/", "-"), "%Y-%m-%d").date()
        except ValueError:
            return None


def batch_year(month: int, anchor: Optional[date]) -> int:
    """批次月份 + 參考日期 → 推估年份：取離參考日最近的那個月（各月以 15 日計）。"""
    if anchor is None:
        return 0
    best = None
    for year in (anchor.year - 1, anchor.year, anchor.year + 1):
        gap = abs((date(year, month, 15) - anchor).days)
        if best is None or gap < best[0]:
            best = (gap, year)
    return best[1]


def batch_key(code: Any, anchor: Optional[date]) -> Optional[Tuple[int, int, int]]:
    parsed = parse_code(code)
    if not parsed:
        return None
    return (batch_year(parsed[0], anchor), parsed[0], parsed[1])


def key_text(key: Tuple[int, int, int]) -> str:
    return f"{key[0]}/{key[1]}-{key[2]}"


def parse_setting(value: Any) -> Tuple[Optional[int], Optional[Tuple[int, int]]]:
    """'2026/10-3' → (2026, (10, 3))；'10-3' → (None, (10, 3))；格式錯誤 → (None, None)。"""
    text = str(value or "").strip()
    year = None
    if "/" in text:
        y, text = text.split("/", 1)
        if not y.strip().isdigit():
            return None, None
        year = int(y)
    return year, parse_code(text)


# ── 批次清單 ─────────────────────────────────────────────────────────────────

def _load_orders() -> List[Dict[str, Any]]:
    with db.get_conn() as conn:
        rows = conn.execute(
            f"""SELECT o.id, o.code, o.status, o.created_at,
                       COALESCE((SELECT MIN(ds.dispatched_at) FROM dispatch_sessions ds
                                 WHERE ds.order_id = o.id AND ds.rolled_back_at = ''), '') AS first_dispatched_at
                FROM orders o
                WHERE o.archived_at='' AND o.status IN ({','.join('?' * len(ACTIVE_STATUSES))})""",
            ACTIVE_STATUSES,
        ).fetchall()
    return [dict(r) for r in rows]


def list_batches(orders: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """已發料 / 已完成訂單的批次，依（推估年, 月, 序）排序。代碼不是「月-序」的不列。"""
    orders = _load_orders() if orders is None else orders
    batches: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
    for o in orders:
        if o.get("archived_at"):
            continue
        anchor = _parse_dt(o.get("first_dispatched_at")) or _parse_dt(o.get("created_at"))
        key = batch_key(o.get("code"), anchor)
        if not key:
            continue
        b = batches.setdefault(key, {
            "key": key_text(key), "code": f"{key[1]}-{key[2]}", "year": key[0],
            "order_ids": [], "first_dispatched_at": "",
        })
        b["order_ids"].append(int(o["id"]))
        first = str(o.get("first_dispatched_at") or "")
        if first and (not b["first_dispatched_at"] or first < b["first_dispatched_at"]):
            b["first_dispatched_at"] = first
    ordered = [batches[k] for k in sorted(batches)]
    codes = [b["code"] for b in ordered]
    for b in ordered:
        d = _parse_dt(b["first_dispatched_at"])
        label = b["code"]
        if codes.count(b["code"]) > 1:
            label += f"（{b['year']}）"
        if d:
            label += f"　發料 {d.month:02d}/{d.day:02d}"
        b["label"] = label
        b["sort"] = [b["year"], *map(int, b["code"].split("-"))]
    return ordered


def resolve_cutoff(value: Any, batches: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """設定值 → 批次。沒寫年份時取同代碼中最新的一批（只看排序上的位置）。"""
    year, code = parse_setting(value)
    if not code:
        return None
    hits = [b for b in batches if b["sort"][1:] == list(code) and (year is None or b["year"] == year)]
    return hits[-1] if hits else None


def get_cutoff_setting() -> str:
    return str(db.get_setting(SETTING_KEY, "") or "").strip() or DEFAULT_CUTOFF


def set_cutoff_setting(value: str) -> Dict[str, Any]:
    batches = list_batches()
    hit = resolve_cutoff(value, batches)
    if not hit:
        raise ValueError(f"找不到批次「{value}」（需為已發料 / 已完成的批次，例如 10-3）")
    db.set_setting(SETTING_KEY, hit["key"])
    return config(batches)


def config(batches: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    batches = list_batches() if batches is None else batches
    value = get_cutoff_setting()
    hit = resolve_cutoff(value, batches)
    return {
        "cutoff": hit["key"] if hit else value,
        "cutoff_code": hit["code"] if hit else value,
        "cutoff_found": bool(hit),
        "default": DEFAULT_CUTOFF,
        "batches": [{k: b[k] for k in ("key", "code", "year", "label", "first_dispatched_at")} for b in batches],
    }


# ── 網站扣量 / 正航調撥 ──────────────────────────────────────────────────────

def _consumptions(order_ids: List[int], loaded_at: str) -> List[Tuple[str, float]]:
    if not order_ids:
        return []
    out: List[Tuple[str, float]] = []
    with db.get_conn() as conn:
        for i in range(0, len(order_ids), 500):
            chunk = order_ids[i:i + 500]
            sql = (f"SELECT part_number, SUM(used_qty) AS qty FROM st_dispatch_consumptions "
                   f"WHERE rolled_back_at = '' AND order_id IN ({','.join('?' * len(chunk))})")
            params: List[Any] = list(chunk)
            if loaded_at:
                sql += " AND consumed_at <= ?"
                params.append(loaded_at)
            rows = conn.execute(sql + " GROUP BY part_number", params).fetchall()
            out.extend((str(r["part_number"] or ""), float(r["qty"] or 0)) for r in rows)
    return out


def ware_ids() -> Tuple[str, str]:
    st = str(erp_stock.get_settings().get("ware_id") or "001").strip() or "001"
    gl = str(os.environ.get("SHORTAGE_GL_WARE_ID") or "015").strip() or "015"
    return st, gl


def _yyyymmdd(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def _from_yyyymmdd(v: Any) -> Optional[date]:
    try:
        return datetime.strptime(str(int(v)), "%Y%m%d").date()
    except (TypeError, ValueError):
        return None


def detect_settle_date(conn, parts: List[str], start: date, st: str, gl: str) -> Optional[date]:
    """截止批次的料「實際調撥的那天」：start（含）以後 001→015 調撥單上出現最多這些料號的日期（同數取較晚）。"""
    counts: Dict[int, set] = {}
    cur = conn.cursor()
    for i in range(0, len(parts), IN_CHUNK):
        chunk = parts[i:i + IN_CHUNK]
        cur.execute(
            "SELECT BillDate, ProdID FROM comProdRec WITH (NOLOCK) "
            "WHERE Flag = 400 AND WareOut = %s AND WareID = %s AND BillDate >= %s "
            f"AND ProdID IN ({','.join(['%s'] * len(chunk))}) GROUP BY BillDate, ProdID",
            (st, gl, _yyyymmdd(start), *chunk),
        )
        for bill_date, prod in cur.fetchall():
            counts.setdefault(int(bill_date), set()).add(str(prod or "").strip().upper())
    if not counts:
        return None
    best = max(counts, key=lambda d: (len(counts[d]), d))
    return _from_yyyymmdd(best)


def query_transfers(conn, after: date, st: str, gl: str) -> Dict[str, float]:
    """after（不含）以後的淨調撥：001→015 為正、015→001 退回為負。{正航料號大寫: 量}"""
    cur = conn.cursor()
    cur.execute(
        "SELECT ProdID, WareOut, SUM(Quantity) FROM comProdRec WITH (NOLOCK) "
        "WHERE Flag = 400 AND BillDate > %s "
        "AND ((WareOut = %s AND WareID = %s) OR (WareOut = %s AND WareID = %s)) "
        "GROUP BY ProdID, WareOut",
        (_yyyymmdd(after), st, gl, gl, st),
    )
    out: Dict[str, float] = {}
    for prod, ware_out, qty in cur.fetchall():
        part = str(prod or "").strip().upper()
        if not part:
            continue
        sign = 1.0 if str(ware_out or "").strip() == st else -1.0
        out[part] = out.get(part, 0.0) + sign * float(qty or 0)
    return out


def net_pending(deducted: float, transferred: float) -> Dict[str, float]:
    """純函式：淨值 = 扣量 − 調撥；負的不歸零而是記成超轉（待調撥當 0）。"""
    net = float(deducted or 0) - float(transferred or 0)
    return {
        "deducted": float(deducted or 0),
        "transferred": float(transferred or 0),
        "pending": max(0.0, net),
        "over": max(0.0, -net),
    }


def compute(exclude_order_ids: Iterable[int] = (), conversions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """回傳 {"parts": {最終料號: net_pending(...)}, "info": {...}, "warnings": [...]}。

    失敗時 parts 為空（= 待調撥 0，跟舊版一樣）並附警告，不影響缺料清單產生。
    """
    conversions = part_conversion.active_map() if conversions is None else conversions
    exclude = {int(i) for i in exclude_order_ids if i}
    info: Dict[str, Any] = {"cutoff": "", "cutoff_code": "", "settle_date": "", "settle_source": "",
                            "transfer_after": "", "batches": [], "order_count": 0, "st_loaded_at": ""}
    result: Dict[str, Any] = {"parts": {}, "info": info, "warnings": []}
    batches = list_batches()
    setting = get_cutoff_setting()
    cutoff = resolve_cutoff(setting, batches)
    if not cutoff:
        if batches and _consumptions([i for b in batches for i in b["order_ids"]], ""):
            result["warnings"].append(f"待調撥：找不到截止批次「{setting}」，本次未計算待調撥（請在上方重新選擇）")
        return result
    info.update(cutoff=cutoff["key"], cutoff_code=cutoff["code"])
    idx = batches.index(cutoff)
    after = batches[idx + 1:]
    info["batches"] = [b["code"] for b in after]
    order_ids = [i for b in after for i in b["order_ids"] if i not in exclude]
    info["order_count"] = len(order_ids)
    loaded_at = str(db.get_setting("st_inventory_loaded_at", "") or "")
    info["st_loaded_at"] = loaded_at
    deducted: Dict[str, float] = {}
    for part, qty in _consumptions(order_ids, loaded_at):
        key = part_conversion.resolve(part, conversions)
        if key:
            deducted[key] = deducted.get(key, 0.0) + qty
    if not any(abs(v) > 1e-9 for v in deducted.values()):
        return result

    st, gl = ware_ids()
    start = _parse_dt(cutoff["first_dispatched_at"])
    cutoff_parts = sorted({p for p, _ in _consumptions(cutoff["order_ids"], "")})
    try:
        with low_stock._erp_session() as conn:
            settle = detect_settle_date(conn, cutoff_parts, start, st, gl) if (start and cutoff_parts) else None
            if settle:
                info["settle_source"] = "auto"
            else:
                settle = start or (date.today() - timedelta(days=1))
                info["settle_source"] = "fallback"
                result["warnings"].append(
                    f"待調撥：找不到 {cutoff['code']} 的調撥紀錄，改從 {settle.month:02d}/{settle.day:02d} 之後的調撥起算")
            raw_transfers = query_transfers(conn, settle, st, gl)
    except Exception as exc:
        logger.warning("待調撥：正航調撥查詢失敗：%s", type(exc).__name__)
        result["warnings"].append(f"待調撥：正航調撥查詢失敗，本次未計算待調撥（{type(exc).__name__}）")
        return result
    info["settle_date"] = settle.isoformat()
    info["transfer_after"] = settle.isoformat()
    transfers: Dict[str, float] = {}
    for part, qty in raw_transfers.items():
        key = part_conversion.resolve(part, conversions)
        transfers[key] = transfers.get(key, 0.0) + qty
    for key in set(deducted) | set(transfers):
        result["parts"][key] = net_pending(deducted.get(key, 0.0), transfers.get(key, 0.0))
    return result


def allocate_batches(batches: List[Dict[str, Any]], transfers: Dict[str, float],
                     website: List[Dict[str, Any]], conversions: Dict[str, str]) -> List[Dict[str, Any]]:
    """正航量 FIFO；正式網站單已包含於正航量，先移除再按原批次扣除，避免重扣。"""
    import json
    from . import erp_purchase as ep
    remaining: Dict[str, float] = {}
    for part, qty in transfers.items():
        key = part_conversion.resolve(part, conversions)
        remaining[key] = remaining.get(key, 0) + qty
    own: Dict[str, Dict[str, float]] = {}
    batch_keys = {b['key'] for b in batches}
    for rec in website:
        for line in json.loads(rec['lines_json']):
            part = part_conversion.resolve(line['part'], conversions)
            qty = float(line['qty'])
            own.setdefault(rec['batch_key'], {})
            own[rec['batch_key']][part] = own[rec['batch_key']].get(part, 0) + qty
            if rec['target_db'] == 'CHIComp03' and rec.get('_in_source', True) and rec['batch_key'] in batch_keys:
                remaining[part] = remaining.get(part, 0) - qty
    out = []
    for batch in batches:
        deducted: Dict[str, float] = {}
        for part, qty in batch['deductions']:
            key = part_conversion.resolve(part, conversions)
            if ep.customer_supplied_reason(part) or ep.customer_supplied_reason(key):
                continue
            deducted[key] = deducted.get(key, 0) + qty
        rows = []
        for part, qty in sorted(deducted.items()):
            qty = max(0, qty - own.get(batch['key'], {}).get(part, 0))
            # 淨退回量為負時，先補回最早批次的待調撥，與 compute 的淨量一致。
            if remaining.get(part, 0) < 0:
                qty -= remaining[part]
                remaining[part] = 0
            used = min(qty, max(0, remaining.get(part, 0)))
            remaining[part] = remaining.get(part, 0) - used
            if qty > used:
                rows.append({'part': part, 'pending': qty - used})
        out.append({k: v for k, v in batch.items() if k != 'deductions'} | {'rows': rows})
    return out


def batch_breakdown(target: str) -> Dict[str, Any]:
    """一鍵調撥使用，失敗即拒絕預覽；不改既有 compute 結果。"""
    conversions = part_conversion.active_map()
    batches = list_batches()
    cutoff = resolve_cutoff(get_cutoff_setting(), batches)
    if not cutoff:
        raise ValueError('找不到調撥截止批次，請重新選擇')
    loaded = str(db.get_setting('st_inventory_loaded_at', '') or '')
    start = _parse_dt(cutoff['first_dispatched_at'])
    if not start:
        raise ValueError('截止批次缺少首次發料日期')
    parts = sorted({p for p, _ in _consumptions(cutoff['order_ids'], '')})
    warnings = []
    with low_stock._erp_session() as conn:
        settle = detect_settle_date(conn, parts, start, '001', '015') if parts else None
        if not settle:
            settle = start
            warnings.append('找不到截止批次的調撥日，改從首次發料日之後起算')
        transfers = query_transfers(conn, settle, '001', '015')
    after = [{**b, 'deductions': _consumptions(b['order_ids'], loaded)} for b in batches[batches.index(cutoff)+1:]]
    website = db.successful_erp_transfers(target)
    if target != 'CHIComp03':
        website += db.successful_erp_transfers('CHIComp03')
    for rec in website:
        rec['_in_source'] = int(rec.get('move_date') or 0) > _yyyymmdd(settle)
    # 正式紀錄只用於消除 FIFO 重扣；測試公司只扣自身紀錄。
    return {'batches': allocate_batches(after, transfers, website, conversions), 'warnings': warnings}
