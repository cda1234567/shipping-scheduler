"""鈞洋 後段加工對應：鈞洋（P0372）賣塑膠外殼 OC-109xxB，印刷由加工廠（聖佑 / 皆祥…）另外做 SP-100xxB。

BOM 裡加工件是緊接在鈞洋件後面的虛線行（is_dash=1），缺料計算會略過虛線行，所以加工件從來不會被需求。
這裡維護 postprocess_links（基礎件 → 加工件 + 比例）：
- confirmed 且 enabled 才生效（缺料採購自動帶出加工行、後端開單檢查）；
- 自動建議一律是 pending（待確認），確認後才生效；駁回的存成 rejected，之後不再重複建議。
建議來源：(a) BOM 內緊接在鈞洋件之後的虛線行；(b) 正航採購單歷史：鈞洋與加工廠同一天、單號連號、數量相同。
"""
from __future__ import annotations

import logging
import math
import os
import re
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .. import database as db
from . import erp_purchase, tw_holidays

logger = logging.getLogger(__name__)

JUNYANG_VENDOR = "P0372"
DEFAULT_PROCESSOR_VENDORS = "P0332,P0020,P0334,P0350,P0352,W0046,W0049,W0245"
DEFAULT_PROCESS_PART_PREFIXES = "SP-"
BASE_BEFORE_PROCESS_DAYS = 4  # 鈞洋件預交日 = 加工件預交日 - 4 天
HISTORY_DAYS = 730
MAX_EVIDENCE_BILLS = 3
_BILL_RE = re.compile(r"^(.*?)(\d{3})$")


def junyang_vendor() -> str:
    return (os.environ.get("POSTPROCESS_BASE_VENDOR") or JUNYANG_VENDOR).strip()


def processor_vendors() -> List[str]:
    raw = os.environ.get("POSTPROCESS_VENDORS") or DEFAULT_PROCESSOR_VENDORS
    return list(dict.fromkeys(v.strip() for v in raw.split(",") if v.strip()))


def process_part_prefixes() -> Tuple[str, ...]:
    raw = os.environ.get("POSTPROCESS_PART_PREFIXES")
    raw = DEFAULT_PROCESS_PART_PREFIXES if raw is None else raw
    return tuple(p.strip().upper() for p in raw.split(",") if p.strip())


# ── 純函式 ───────────────────────────────────────────────────────────────────

def linked_qty(base_qty: Any, ratio: Any) -> int:
    """加工件數量 = 基礎件數量 × 比例，無條件進位成整數（不看加工件自己的庫存 / MOQ）。"""
    try:
        value = float(base_qty or 0) * float(ratio or 0)
    except (TypeError, ValueError):
        return 0
    return int(math.ceil(round(value, 6))) if value > 0 else 0


def linked_dates(ship: Optional[date], today: date) -> Optional[Dict[str, Any]]:
    """加工件 / 基礎件的預交日。

    加工件 = 出貨日 - 21 天（早於今天用今天）；基礎件 = 加工件 - 4 天（早於今天用今天）；兩者再各自套用假日規則。
    回傳 raw（未套用假日前、已 clamp）與 shift 結果；沒有出貨日回 None。
    """
    if ship is None:
        return None
    process_raw = ship - timedelta(days=21)
    process_clamped = max(process_raw, today)
    base_raw = process_clamped - timedelta(days=BASE_BEFORE_PROCESS_DAYS)
    return {
        "process_raw": process_raw,
        "base_raw": base_raw,
        "process": tw_holidays.shift_due_date(process_raw, today),
        "base": tw_holidays.shift_due_date(base_raw, today),
    }


def missing_postprocess(lines: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """請求（可跨多張採購單）內有生效對應的基礎件，但加工件不在同一批 → 回警告清單；不查 ERP。"""
    items = [(str(l.get("part_number") or "").strip().upper(), l.get("qty")) for l in lines]
    parts = {p for p, _ in items if p}
    if not parts:
        return []
    links = db.active_postprocess_links(sorted(parts))
    out: List[Dict[str, Any]] = []
    seen = set()
    for part, qty in items:
        for link in links.get(part, []):
            key = (part, link["process_part"])
            if link["process_part"] in parts or key in seen:
                continue
            seen.add(key)
            try:
                base_qty = float(qty or 0)
            except (TypeError, ValueError):
                base_qty = 0.0
            out.append({
                "base_part": part, "process_part": link["process_part"], "ratio": link["ratio"],
                "base_qty": base_qty, "suggested_qty": linked_qty(base_qty, link["ratio"]),
            })
    return out


def missing_postprocess_for_groups(groups: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lines: List[Dict[str, Any]] = []
    for g in groups:
        lines.extend(g.get("lines") or [])
    return missing_postprocess(lines)


def postprocess_warning_text(items: List[Dict[str, Any]]) -> str:
    return "；".join(f"{i['base_part']} 需搭配後段加工 {i['process_part']}（建議 {i['suggested_qty']:,}）" for i in items)


# ── 正航採購單歷史配對 ───────────────────────────────────────────────────────

def _bill_parts(bill_no: str) -> Optional[Tuple[str, int]]:
    m = _BILL_RE.match(str(bill_no or "").strip())
    return (m.group(1), int(m.group(2))) if m else None


def pair_po_lines(rows: List[Tuple[Any, ...]], base_vendor: str, processors: Iterable[str],
                  prefixes: Tuple[str, ...] = ()) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """rows = (BillNO, BillDate, CustomerID, ProdID, Quantity)。

    同一天、單號連號（尾端 3 碼流水號差 1、前綴相同）、一邊是基礎件廠商、一邊是加工廠、數量相同 → 一組配對。
    回傳 {(基礎件, 加工件): {"count": 次數, "bills": ["基礎單號/加工單號", ...]}}。
    """
    procs = set(processors)
    prefixes = tuple(p.upper() for p in prefixes)
    by_date: Dict[int, List[Dict[str, Any]]] = {}
    for bill_no, bill_date, vendor, prod, qty in rows:
        parsed = _bill_parts(bill_no)
        if parsed is None:
            continue
        try:
            q = float(qty or 0)
        except (TypeError, ValueError):
            continue
        by_date.setdefault(int(bill_date or 0), []).append({
            "bill": str(bill_no).strip(), "prefix": parsed[0], "seq": parsed[1],
            "vendor": str(vendor or "").strip(), "part": str(prod or "").strip().upper(), "qty": q,
        })
    found: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for lines in by_date.values():
        bases = [l for l in lines if l["vendor"] == base_vendor]
        others = [l for l in lines if l["vendor"] in procs]
        for b in bases:
            for o in others:
                if b["prefix"] != o["prefix"] or abs(b["seq"] - o["seq"]) != 1:
                    continue
                if b["qty"] <= 0 or abs(b["qty"] - o["qty"]) > 1e-6 or b["part"] == o["part"]:
                    continue
                if prefixes and not o["part"].startswith(prefixes):
                    continue
                entry = found.setdefault((b["part"], o["part"]), {"count": 0, "bills": []})
                entry["count"] += 1
                text = f"{b['bill']}/{o['bill']}"
                if text not in entry["bills"] and len(entry["bills"]) < MAX_EVIDENCE_BILLS:
                    entry["bills"].append(text)
    return found


def _query_po_history(since: int) -> List[Tuple[Any, ...]]:
    vendors = [junyang_vendor()] + processor_vendors()
    marks = ",".join(["%s"] * len(vendors))
    with erp_purchase._source_session() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT m.BillNO, m.BillDate, m.CustomerID, s.ProdID, s.Quantity "
            "FROM ordBillMain m WITH (NOLOCK) "
            "JOIN ordBillSub s WITH (NOLOCK) ON s.Flag = m.Flag AND s.BillNO = m.BillNO "
            f"WHERE m.Flag = 4 AND m.BillDate >= %s AND m.CustomerID IN ({marks})",
            (since, *vendors),
        )
        return [tuple(r) for r in cur.fetchall()]


def _resolve_base_vendors(parts: List[str]) -> Tuple[Dict[str, str], str]:
    """基礎件 → 供應商（主檔慣用供應商 > 最近採購）；ERP 連不上回 ({}, 錯誤訊息)。"""
    from . import shortage_purchase as sp  # 延遲載入避免循環相依

    if not parts:
        return {}, ""
    try:
        with erp_purchase._source_session() as conn:
            products = sp._lookup_products(conn, parts)
            last = sp._lookup_last_po(conn, parts)
    except Exception as exc:
        return {}, erp_purchase._safe_message(exc)
    out: Dict[str, str] = {}
    for p in parts:
        major = (products.get(p) or {}).get("major_supplier", "")
        out[p] = major or (last.get(p) or {}).get("supplier_id", "")
    return out, ""


def _add_pending(base: str, process: str, source: str, evidence: str) -> bool:
    if db.postprocess_pair_exists(base, process):
        return False
    try:
        db.add_postprocess_link(base, process, 1, status="pending", source=source, evidence=evidence)
    except ValueError:
        return False
    return True


def suggest(use_erp: bool = True) -> Dict[str, Any]:
    """產生待確認的候選對應（不會動既有任何一筆，也不會讓候選生效）；ERP 連不上時只做 BOM 來源。"""
    warnings: List[str] = []
    created: List[Dict[str, str]] = []
    base_vendor = junyang_vendor()

    # (a) BOM：緊接在鈞洋件之後的虛線行
    followers = db.list_bom_dash_followers()
    pairs: Dict[Tuple[str, str], List[str]] = {}
    for f in followers:
        pairs.setdefault((f["base_part"], f["process_part"]), []).append(f["model"])
    vendors: Dict[str, str] = {}
    erp_ok = False
    if use_erp and pairs:
        vendors, err = _resolve_base_vendors(sorted({b for b, _ in pairs}))
        erp_ok = not err
        if err:
            warnings.append(f"正航查詢失敗，BOM 候選改用 {base_vendor} 料號前綴 OC- 推測（{err}）")
    elif not use_erp:
        warnings.append("未查詢正航，BOM 候選改用 OC- 前綴推測")
    for (base, process), models in sorted(pairs.items()):
        vendor = vendors.get(base, "")
        if vendor:
            if vendor != base_vendor:
                continue
            how = f"正航供應商 {vendor}"
        elif base.startswith("OC-"):
            how = "無法由正航確認供應商，依 OC- 前綴推測為鈞洋件" if not erp_ok else "正航查無慣用供應商，依 OC- 前綴推測為鈞洋件"
        else:
            continue
        shown = "、".join(dict.fromkeys(m for m in models if m))[:80]
        evidence = f"BOM 內 {process} 為緊接在 {base} 之後的虛線行（{len(models)} 份 BOM{('：' + shown) if shown else ''}）；{how}"
        if _add_pending(base, process, "auto_bom", evidence):
            created.append({"base_part": base, "process_part": process, "source": "auto_bom"})

    # (b) 正航採購單歷史
    po_ok = False
    if use_erp:
        since = erp_purchase.date_to_int(erp_purchase.local_now().date() - timedelta(days=HISTORY_DAYS))
        try:
            rows = _query_po_history(since)
            po_ok = True
        except Exception as exc:
            rows = []
            warnings.append(f"正航採購單歷史查詢失敗，只產生 BOM 候選（{erp_purchase._safe_message(exc)}）")
        found = pair_po_lines(rows, base_vendor, processor_vendors(), process_part_prefixes())
        for (base, process), info in sorted(found.items()):
            evidence = (f"正航採購單：{base_vendor} 與加工廠同日連號、數量相同，共 {info['count']} 次"
                        f"（{'、'.join(info['bills'])}）")
            if _add_pending(base, process, "auto_po", evidence):
                created.append({"base_part": base, "process_part": process, "source": "auto_po"})
    try:
        db.log_activity("postprocess_suggest", f"後段加工對應：新增 {len(created)} 筆待確認候選")
    except Exception:
        pass
    return {"created": created, "created_count": len(created), "bom_pairs": len(pairs),
            "erp_ok": (erp_ok or not pairs) and (po_ok or not use_erp), "warnings": warnings}
