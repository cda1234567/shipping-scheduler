"""補齊測試公司（如 CHIComp13）已複製的供應商 / 料號缺少的關聯主檔（舊版只複製了 comCustomer / comProduct）。

用法（容器內；預設 dry-run，只印計畫）：
    python -m app.tools.erp_master_backfill --target CHIComp13 --auto --backup-dir /app/data/backfill_c13
    python -m app.tools.erp_master_backfill --target CHIComp13 --vendors P0011,P0372 --products AC-40827B \\
        --backup-dir /app/data/backfill_c13 --apply

規則：
- 只允許測試公司（ERP_TEST_TARGETS）；正式公司、非測試公司、與來源相同一律拒絕。
- --auto：目標內缺 comCustDesc 且來源有的供應商（Flag=2）、缺 comProdLLC 且來源有的料號。
- --apply 之前先把目標「目前」的相關列匯出到 --backup-dir（每表 JSON + CSV，UTF-8）與 plan.json；
  之後在同一個 transaction 只 INSERT 缺少的列（絕不 UPDATE / DELETE），讀回確認筆數、DB_NAME() 等於目標才 commit。
- 與網站開採購單共用同一組複製函式（erp_purchase.related_*_specs / _copy_masters），帳款金額歸 0、不碰 comWareAmount。
- 不印任何帳號密碼。
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..services import erp_purchase as ep
from ..services import erp_stock
from ..services.erp_stock import ErpError

VENDOR_TABLES = (
    ("comCustomer", "Flag = 2 AND ID IN ({m})"),
    ("comCustDesc", "Flag = 2 AND ID IN ({m})"),
    ("comCustTrade", "Flag = 2 AND ID IN ({m})"),
    ("comCustAddress", "Flag = 2 AND ID IN ({m})"),
    ("comCustAccounts", "Flag = 2 AND ID IN ({m})"),
)
PRODUCT_TABLES = (
    ("comProduct", "ProdID IN ({m})"),
    ("comProdLLC", "ProdID IN ({m})"),
    ("ComCommoditySet", "ProdID IN ({m})"),
    ("ComProdctCheckProject", "ProdID IN ({m})"),
)
IN_CHUNK = 500


class BackfillError(Exception):
    pass


# ── 檢查 ─────────────────────────────────────────────────────────────────────

def check_target(target: str) -> None:
    """只允許測試公司；正式公司 / 非測試公司 / 與來源相同一律拒絕。"""
    source_db = erp_stock.get_settings()["database"]
    if not ep.TARGET_RE.match(target or ""):
        raise BackfillError(f"目標公司格式不正確：{target}")
    if target == ep.PRODUCTION_TARGET or target == source_db:
        raise BackfillError(f"拒絕：{target} 是正式公司 / 來源公司，不可寫入")
    if not ep.is_test(target):
        raise BackfillError(f"拒絕：{target} 不在測試公司清單（ERP_TEST_TARGETS）")
    if not ep._can_copy_masters(target, source_db):
        raise BackfillError(f"拒絕：{target} 不可從來源複製主檔")


def _split(text: str) -> List[str]:
    return list(dict.fromkeys(t.strip() for t in (text or "").split(",") if t.strip()))


def _exists(cur, table: str, where: str, params: Tuple[Any, ...]) -> bool:
    cur.execute(f"SELECT 1 FROM {table} WHERE {where}", params)
    return cur.fetchone() is not None


# ── 自動挑選 ─────────────────────────────────────────────────────────────────

def auto_vendors(scur, wcur) -> List[str]:
    wcur.execute(
        "SELECT c.ID FROM comCustomer c WHERE c.Flag = 2 AND NOT EXISTS "
        "(SELECT 1 FROM comCustDesc d WHERE d.Flag = c.Flag AND d.ID = c.ID) ORDER BY c.ID"
    )
    ids = [ep._text(r[0]) for r in wcur.fetchall()]
    return [i for i in ids if i and _exists(scur, "comCustomer", "Flag = 2 AND ID = %s", (i,))]


def auto_products(scur, wcur) -> List[str]:
    wcur.execute(
        "SELECT p.ProdID FROM comProduct p WHERE NOT EXISTS "
        "(SELECT 1 FROM comProdLLC l WHERE l.ProdID = p.ProdID) ORDER BY p.ProdID"
    )
    ids = [ep._text(r[0]) for r in wcur.fetchall()]
    return [i for i in ids if i and _exists(scur, "comProduct", "ProdID = %s", (i,))]


# ── 計畫 ─────────────────────────────────────────────────────────────────────

def build_plan(scur, wcur, vendors: List[str], products: List[str]) -> Dict[str, Any]:
    """回傳 {"specs": 依複製順序的 spec 清單, "vendors": [...], "products": [...], "skipped": [...]}；只讀。"""
    skipped: List[str] = []
    pre: List[Dict[str, Any]] = []
    vendor_after: List[Dict[str, Any]] = []
    product_after: List[Dict[str, Any]] = []
    ok_vendors, ok_products = [], []
    for vid in vendors:
        if not _exists(wcur, "comCustomer", "Flag = 2 AND ID = %s", (vid,)):
            skipped.append(f"供應商 {vid}：目標公司沒有此供應商，略過（本工具只補關聯列）")
            continue
        scur.execute("SELECT * FROM comCustomer WHERE Flag = 2 AND ID = %s", (vid,))
        row = ep._one(scur)
        if row is None:
            skipped.append(f"供應商 {vid}：來源公司沒有此供應商，略過")
            continue
        p, a = ep.related_vendor_specs(scur, wcur, vid, row)
        pre += p
        vendor_after += a
        ok_vendors.append(vid)
    for pid in products:
        if not _exists(wcur, "comProduct", "ProdID = %s", (pid,)):
            skipped.append(f"料號 {pid}：目標公司沒有此料號，略過")
            continue
        if not _exists(scur, "comProduct", "ProdID = %s", (pid,)):
            skipped.append(f"料號 {pid}：來源公司沒有此料號，略過")
            continue
        product_after += ep.related_product_specs(scur, wcur, pid)
        ok_products.append(pid)
    seen = set()
    specs = []
    for spec in ep.order_master_specs([], pre, vendor_after, product_after):
        sig = (spec["table"], spec["where"], tuple(spec["params"]))
        if sig not in seen:
            seen.add(sig)
            specs.append(spec)
    return {"specs": specs, "vendors": ok_vendors, "products": ok_products, "skipped": skipped}


def plan_by_table(specs: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for spec in specs:
        out.setdefault(spec["table"], []).append(spec["key"])
    return out


# ── 匯出（寫入前備份目標目前的列）────────────────────────────────────────────

def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _fetch_in(cur, table: str, where_tpl: str, keys: List[str]) -> Tuple[List[str], List[List[Any]]]:
    names: List[str] = []
    rows: List[List[Any]] = []
    for i in range(0, len(keys), IN_CHUNK):
        chunk = keys[i:i + IN_CHUNK]
        cur.execute(f"SELECT * FROM {table} WHERE " + where_tpl.format(m=",".join(["%s"] * len(chunk))), tuple(chunk))
        names = [d[0] for d in cur.description]
        rows += [list(r) for r in cur.fetchall()]
    return names, rows


def export_target_rows(wcur, backup_dir: Path, vendors: List[str], products: List[str], plan: Dict[str, Any],
                       target: str) -> Dict[str, int]:
    """把目標目前的相關列（供應商 / 料號鍵集合 + 參照的類別、地區）寫成每表 JSON + CSV，並存 plan.json。"""
    backup_dir.mkdir(parents=True, exist_ok=True)
    counts: Dict[str, int] = {}

    def dump(table: str, names: List[str], rows: List[List[Any]]) -> None:
        records = [{n: _jsonable(v) for n, v in zip(names, r)} for r in rows]
        (backup_dir / f"{table}.json").write_text(json.dumps(records, ensure_ascii=False, indent=1), encoding="utf-8")
        with open(backup_dir / f"{table}.csv", "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(names)
            for r in rows:
                w.writerow(["" if v is None else _jsonable(v) for v in r])
        counts[table] = len(rows)

    class_ids: List[str] = []
    area_ids: List[str] = []
    for table, tpl in VENDOR_TABLES:
        if not vendors:
            break
        names, rows = _fetch_in(wcur, table, tpl, vendors)
        dump(table, names, rows)
        if table == "comCustomer":
            low = [n.lower() for n in names]
            for col, bucket in (("classid", class_ids), ("areaid", area_ids)):
                if col in low:
                    idx = low.index(col)
                    bucket += [ep._text(r[idx]) for r in rows if ep._text(r[idx])]
    # 類別 / 地區：目標內「參照到的」＋計畫內要新增的
    for spec in plan["specs"]:
        if spec["table"] == "comCustClass":
            class_ids.append(ep._text(spec["params"][-1]))
        elif spec["table"] == "comArea":
            area_ids.append(ep._text(spec["params"][-1]))
    class_ids, area_ids = list(dict.fromkeys(class_ids)), list(dict.fromkeys(area_ids))
    if class_ids:
        names, rows = _fetch_in(wcur, "comCustClass", "Flag = 2 AND ClassID IN ({m})", class_ids)
        dump("comCustClass", names, rows)
    if area_ids:
        names, rows = _fetch_in(wcur, "comArea", "AreaID IN ({m})", area_ids)
        dump("comArea", names, rows)
    for table, tpl in PRODUCT_TABLES:
        if not products:
            break
        names, rows = _fetch_in(wcur, table, tpl, products)
        dump(table, names, rows)
    (backup_dir / "plan.json").write_text(json.dumps({
        "target": target, "created_at": datetime.now().isoformat(timespec="seconds"),
        "vendors": vendors, "products": products, "skipped": plan["skipped"],
        "to_insert": plan_by_table(plan["specs"]), "exported_row_counts": counts,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    return counts


# ── 執行 ─────────────────────────────────────────────────────────────────────

def format_plan(target: str, plan: Dict[str, Any]) -> str:
    lines = [f"目標公司：{target}（{ep.target_label(target)}）",
             f"供應商 {len(plan['vendors'])} 家：{', '.join(plan['vendors']) or '—'}",
             f"料號 {len(plan['products'])} 個：{', '.join(plan['products']) or '—'}"]
    lines += [f"略過：{s}" for s in plan["skipped"]]
    by_table = plan_by_table(plan["specs"])
    if not by_table:
        lines.append("沒有需要補的資料列。")
    for table, keys in by_table.items():
        lines.append(f"  {table}：將新增 {len(keys)} 列　{', '.join(keys[:12])}{' …' if len(keys) > 12 else ''}")
    return "\n".join(lines)


def count_present(wcur, specs: List[Dict[str, Any]]) -> Dict[str, int]:
    """每個 spec 在目標是否已有對應列，按表統計。"""
    out: Dict[str, int] = {}
    for spec in specs:
        if _exists(wcur, spec["table"], spec["where"], tuple(spec["params"])):
            out[spec["table"]] = out.get(spec["table"], 0) + 1
    return out


def run(target: str, vendors: List[str], products: List[str], auto: bool, backup_dir: Optional[str],
        apply: bool, out=print) -> int:
    check_target(target)
    creds = ep.write_credentials(target)
    if creds is None:
        raise BackfillError(f"{target} 未設定寫入帳號（ERP_SQL_TEST_USER / ERP_SQL_TEST_PASSWORD）")
    if apply and not backup_dir:
        raise BackfillError("--apply 需要 --backup-dir（寫入前先備份目標目前的列）")
    if not (vendors or products or auto):
        raise BackfillError("請指定 --vendors / --products 或 --auto")
    with ep._write_lock, ep._source_session() as src:
        wconn = ep._connect_write(creds)
        committed = False
        try:
            wcur, scur = wconn.cursor(), src.cursor()
            ep._check_db_name(wcur, target)
            if auto:
                vendors = list(dict.fromkeys(vendors + auto_vendors(scur, wcur)))
                products = list(dict.fromkeys(products + auto_products(scur, wcur)))
            plan = build_plan(scur, wcur, vendors, products)
            out(format_plan(target, plan))
            specs = plan["specs"]
            if not apply:
                out("\n這是 dry-run，沒有寫入任何資料。確認計畫後加 --apply 執行。")
                return 0
            if not specs:
                return 0
            counts = export_target_rows(wcur, Path(backup_dir), plan["vendors"], plan["products"], plan, target)
            out(f"\n已備份目標目前的列到 {backup_dir}（{sum(counts.values())} 列，{len(counts)} 張表）")
            before = count_present(wcur, specs)
            ep._copy_masters(scur, wcur, specs, target)
            after = count_present(wcur, specs)
            inserted: Dict[str, int] = {}
            for spec in specs:
                inserted[spec["table"]] = inserted.get(spec["table"], 0) + 1
            for table, n in inserted.items():
                if after.get(table, 0) - before.get(table, 0) != n:
                    raise BackfillError(f"讀回確認失敗：{table} 預期新增 {n} 列，實際 {after.get(table, 0) - before.get(table, 0)}，已 rollback")
            ep._check_db_name(wcur, target)
            wconn.commit()
            committed = True
            out("\n已 commit。各表新增筆數：")
            for table, n in inserted.items():
                out(f"  {table}：+{n}")
            return 0
        except (BackfillError, ErpError):
            raise
        except Exception as exc:
            raise BackfillError(f"執行失敗，已 rollback：{ep._safe_message(exc, creds)}") from None
        finally:
            ep._close_quietly(wconn, rollback=not committed)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="補齊測試公司缺少的供應商 / 料號關聯主檔（預設 dry-run）")
    parser.add_argument("--target", required=True, help="測試公司資料庫，如 CHIComp13")
    parser.add_argument("--vendors", default="", help="供應商代號，逗號分隔")
    parser.add_argument("--products", default="", help="料號，逗號分隔")
    parser.add_argument("--auto", action="store_true", help="自動挑選缺 comCustDesc 的供應商與缺 comProdLLC 的料號")
    parser.add_argument("--backup-dir", default="", help="寫入前匯出目標目前列的目錄（--apply 必填）")
    parser.add_argument("--apply", action="store_true", help="實際寫入（預設只 dry-run）")
    args = parser.parse_args(argv)
    try:
        return run(args.target.strip(), _split(args.vendors), _split(args.products), args.auto,
                   args.backup_dir or None, args.apply)
    except (BackfillError, ErpError) as exc:
        print(f"錯誤：{ep._safe_message(exc)}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
