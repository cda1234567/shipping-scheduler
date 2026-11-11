"""低庫存可用量（總指揮 2026-11-11 19:24／19:30／19:47 定案，v2026.11.11.7）。

可用 = 自由 ST + 待調撥 + 015 − 未領需求
- 待調撥 = min(st_pending_transfer 該料 pending, ST)：正航 001 中尚未調撥的量（超過 ST 的標資料待確認）。
- 自由 ST = max(0, ST − 待調撥原值)：總指揮原式沒有這項，畫面標「待總指揮確認」；
  不含自由 ST 的值另存 available_without_free_st（= 總指揮原式），自由 ST = 0 時兩者相同。
- 015 = 正航 015 現有量。
- 未領需求 = Σ 已發料（orders.status='dispatched'）各 PO：max(0, Σ 該 PO 各批需求 − 該 PO 015 已領)。
  * 每個批次只算一次需求，不管它是由哪一次 ST 補料供應的。
  * 需求：有派工紀錄（dispatch_records.needed_qty，發料時寫進主檔 PO 欄的用量）用派工紀錄；
    沒有派工紀錄才退回現行 BOM（單機用量 × 台數，含損耗），兩者都沒有標「缺 BOM」資料待確認，不猜。
    不用批次碼欄的整包補料量。BOM 版另外算出來並列對照。
  * 015 已領：正航 prdProdRec Flag=331 → prdMKOrdMain.CustBill 去掉「-後綴」= 客戶 PO；
    只算 015 倉（comProdRec Flag=331 同 BillNO/RowNO/ProdID、WareID=015），全部年度。
  * 轉換料兩邊都轉成最終料號再比；料號含 -TAB 的一律排除。
- 待排程（status='pending'）的訂單不算在這裡（低庫存另用「已排程需求」判斷）。
正航全程唯讀 SELECT。這個可用量只用在低庫存，缺料採購公式不變。
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import part_conversion
from .bom_quantity import coerce_qty, get_component_effective_needed_qty

SOURCE_DISPATCH = "dispatch_records"
SOURCE_BOM = "bom"
SOURCE_NO_BOM = "no_bom"
IN_CHUNK = 500


def norm_part(part: Any) -> str:
    return str(part or "").strip().upper()


def is_tab(part: Any) -> bool:
    """料號含 -TAB（不分大小寫，含 OC-10935B-TAB-C 這種）→ 客供料，低庫存一律排除。"""
    return "-TAB" in norm_part(part)


def po_key(value: Any) -> str:
    """製令 CustBill / 訂單 PO → 比對用 PO：去空白、去掉第一個「-」之後的後綴（4500059418-B → 4500059418）。"""
    return str(value or "").strip().split("-", 1)[0].strip().upper()


def _round(value: float) -> float:
    return round(float(value or 0), 4)


# ── 每張已發料訂單的需求（純函式） ─────────────────────────────────────────

def bom_need_by_part(order: Dict[str, Any], bom_map: Dict[str, List[dict]],
                     conversions: Dict[str, str]) -> Optional[Dict[str, float]]:
    """現行 BOM 需求（單機用量 × 台數 × (1 + 損耗)，跟發料時一致）；找不到 BOM → None。"""
    components = bom_map.get(norm_part(order.get("model")))
    if components is None:
        return None
    order_qty = coerce_qty(order.get("order_qty"))
    out: Dict[str, float] = {}
    for comp in components:
        if comp.get("is_dash"):
            continue
        qty = get_component_effective_needed_qty(comp, order_qty)
        key = part_conversion.resolve(str(comp.get("part_number") or ""), conversions)
        if key and qty > 0:
            out[key] = out.get(key, 0.0) + float(qty)
    return out


def record_need_by_part(records: Iterable[dict], conversions: Dict[str, str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for r in records or []:
        key = part_conversion.resolve(str(r.get("part_number") or ""), conversions)
        qty = float(r.get("needed_qty") or 0)
        if key and qty > 0:
            out[key] = out.get(key, 0.0) + qty
    return out


def order_demands(orders: List[Dict[str, Any]], records_by_order: Dict[int, List[dict]],
                  bom_map: Dict[str, List[dict]], conversions: Dict[str, str]) -> List[Dict[str, Any]]:
    """每張已發料訂單：{order_id, code, po, model, order_qty, source, parts, bom_parts}。

    parts = 用來算未領需求的量（派工紀錄優先，沒有才用 BOM）；bom_parts = 現行 BOM 版（對照用，沒有 BOM 為 None）。
    """
    out = []
    for o in orders:
        oid = int(o.get("id") or 0)
        bom = bom_need_by_part(o, bom_map, conversions)
        records = records_by_order.get(oid) or []
        if records:
            parts, source = record_need_by_part(records, conversions), SOURCE_DISPATCH
        elif bom is not None:
            parts, source = dict(bom), SOURCE_BOM
        else:
            parts, source = {}, SOURCE_NO_BOM
        out.append({
            "order_id": oid, "code": str(o.get("code") or "").strip(), "po": po_key(o.get("po_number")),
            "po_number": str(o.get("po_number") or "").strip(), "model": str(o.get("model") or "").strip(),
            "order_qty": coerce_qty(o.get("order_qty")), "source": source, "parts": parts, "bom_parts": bom,
        })
    return out


def convert_issued(raw: Dict[Tuple[str, str], float], conversions: Dict[str, str]) -> Dict[Tuple[str, str], float]:
    """{(PO, 正航料號): 已領} → 料號轉成最終料號後加總。"""
    out: Dict[Tuple[str, str], float] = {}
    for (po, part), qty in raw.items():
        key = (po_key(po), part_conversion.resolve(part, conversions))
        if key[0] and key[1]:
            out[key] = out.get(key, 0.0) + float(qty or 0)
    return out


def unissued_by_part(demands: List[Dict[str, Any]], issued: Dict[Tuple[str, str], float]) -> Dict[str, Dict[str, Any]]:
    """PO 層級：同 PO 多批次需求先加總，再減該 PO 015 已領（每批只算一次）。

    回傳 {料號: {"unissued": 合計, "detail": [{po, codes, need, bom_need, issued, unissued, sources}]}}；
    -TAB 料不列。issued 的料號須已轉成最終料號（convert_issued）。
    """
    by_po: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for d in demands:
        if not d["po"]:
            continue
        parts = set(d["parts"]) | set(d["bom_parts"] or {})
        for part in parts:
            if is_tab(part):
                continue
            e = by_po.setdefault((d["po"], part), {"po": d["po"], "codes": [], "need": 0.0, "bom_need": 0.0,
                                                   "bom_missing": False, "sources": []})
            e["need"] += float(d["parts"].get(part, 0.0))
            if d["bom_parts"] is None:
                e["bom_missing"] = True
            else:
                e["bom_need"] += float(d["bom_parts"].get(part, 0.0))
            if d["code"] and d["code"] not in e["codes"]:
                e["codes"].append(d["code"])
            if d["source"] not in e["sources"]:
                e["sources"].append(d["source"])
    out: Dict[str, Dict[str, Any]] = {}
    for (po, part), e in sorted(by_po.items()):
        got = float(issued.get((po, part), 0.0))
        row = {
            "po": po, "codes": e["codes"], "need": _round(e["need"]),
            "bom_need": None if e["bom_missing"] else _round(e["bom_need"]),
            "issued": _round(got), "unissued": _round(max(0.0, e["need"] - got)), "sources": e["sources"],
        }
        entry = out.setdefault(part, {"unissued": 0.0, "detail": []})
        entry["unissued"] = _round(entry["unissued"] + row["unissued"])
        entry["detail"].append(row)
    return out


def calc_available(st: float, pending_raw: float, gl015: float, unissued: float) -> Dict[str, Any]:
    """單一料號的可用量；可以是負數，照實回傳。"""
    st = float(st or 0)
    pending_raw = max(0.0, float(pending_raw or 0))
    pending = min(pending_raw, max(st, 0.0))
    free_st = max(0.0, st - pending_raw)
    gl015 = float(gl015 or 0)
    unissued = float(unissued or 0)
    return {
        "st": _round(st), "pending_raw": _round(pending_raw), "pending": _round(pending), "free_st": _round(free_st),
        "gl015": _round(gl015), "unissued": _round(unissued),
        "available": _round(free_st + pending + gl015 - unissued),
        "available_without_free_st": _round(pending + gl015 - unissued),
        "pending_over_st": pending_raw > max(st, 0.0) + 1e-9,
    }


# ── 正航唯讀查詢 ─────────────────────────────────────────────────────────

def _chunks(items: List[str], size: int = IN_CHUNK):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def query_issued_by_po(conn, gl_ware: str, parts: Iterable[str]) -> Dict[Tuple[str, str], float]:
    """015 已領（全部年度）：{(客戶 PO, 正航料號): 已領量}。comProdRec Flag 331 出庫為負數，取正。"""
    out: Dict[Tuple[str, str], float] = {}
    cur = conn.cursor()
    for batch in _chunks(sorted({norm_part(p) for p in parts if norm_part(p)})):
        cur.execute(
            "SELECT m.CustBill, c.ProdID, SUM(c.Quantity) FROM prdProdRec p "
            "JOIN (SELECT DISTINCT MKOrdNO, CustBill FROM prdMKOrdMain) m ON m.MKOrdNO = p.MkOrdNO "
            "JOIN comProdRec c ON c.Flag = 331 AND c.BillNO = p.BillNO AND c.RowNO = p.RowNO AND c.ProdID = p.ProdID "
            f"WHERE p.Flag = 331 AND c.WareID = %s AND c.ProdID IN ({','.join(['%s'] * len(batch))}) "
            "GROUP BY m.CustBill, c.ProdID",
            (gl_ware, *batch),
        )
        for cust_bill, prod_id, qty in cur.fetchall():
            key = (po_key(cust_bill), norm_part(prod_id))
            if key[0] and key[1]:
                out[key] = out.get(key, 0.0) - float(qty or 0)
    return out


def issued_query_parts(parts: Iterable[str], demands: List[Dict[str, Any]], conversions: Dict[str, str]) -> List[str]:
    """查已領要帶的正航料號：母體料號 + 需求料號 + 會轉成這些料號的舊料號（轉換料兩邊一致）。"""
    targets = {norm_part(p) for p in parts}
    for d in demands:
        targets |= set(d["parts"]) | set(d["bom_parts"] or {})
    targets = {p for p in targets if p}
    olds = {norm_part(a) for a in conversions if part_conversion.resolve(a, conversions) in targets}
    return sorted(targets | olds)
