"""上線前對帳報表（唯讀）：低庫存新可用量 vs 主檔最右結存。

用法（容器內，repo 在 /app）：
    python tools/available_reconcile_report.py [--out-dir /tmp] [--today 2026-11-11]

輸出 available_reconcile_<時間>.json 與 .csv：全部主檔料號（排除 -TAB），每顆料
ST、001 現有、待調撥（原值 / 以 001 現有封頂）、自由 ST、015、未領需求（逐 PO 明細）、available、available_without_free_st、
主檔最右結存（main_reader.read_stock）、差異；依 |available_without_free_st − 主檔結存| 由大到小排，全部列出不加門檻。
「推測原因」只是標示類別供總指揮核對：不清帳、不改主檔、不寫正航。
正航只用唯讀 SELECT；本機只讀（不寫快照、不寫設定、不寫操作紀錄）。
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import database as db  # noqa: E402
from app.services import available_qty as aq  # noqa: E402
from app.services import low_stock, low_stock_plan, main_reader  # noqa: E402
from app.services.local_time import local_now  # noqa: E402

MATCH_TOLERANCE = 0.5
CSV_FIELDS = (
    "part", "name", "st", "wh001", "pending_raw", "pending", "free_st", "gl015", "unissued",
    "available", "available_without_free_st", "main_balance", "diff", "diff_with_free_st", "match",
    "causes", "unissued_detail",
)


def causes_for(row: dict, issued_pos: dict, match: bool) -> list:
    """推測原因類別（僅供參考）。"""
    out = []
    detail = row.get("unissued_detail") or []
    if row.get("pending_over_001"):
        out.append("待調撥 > 001 現有（資料待確認）")
    if row.get("free_st", 0) > MATCH_TOLERANCE:
        out.append("B 自由 ST > 0（主檔結存不含自由 ST）")
    if any(d.get("bom_need") is not None and abs(d["bom_need"] - d["need"]) > MATCH_TOLERANCE for d in detail):
        out.append("BOM 版 ≠ 派工紀錄版（以派工紀錄為準）")
    if any(d.get("bom_need") is None for d in detail):
        out.append("H 有已發料訂單缺 BOM（資料待確認）")
    if any(aq.SOURCE_BOM in (d.get("sources") or []) for d in detail):
        out.append("有已發料訂單沒有派工紀錄，用現行 BOM")
    if any(d["issued"] > d["need"] + MATCH_TOLERANCE for d in detail):
        out.append("M 015 已領 > 需求（超領／進位）")
    demand_pos = {d["po"] for d in detail}
    orphan = sorted(po for po, qty in issued_pos.items() if po not in demand_pos and abs(qty) > MATCH_TOLERANCE)
    if orphan:
        out.append("D 正航已領的 PO 不在網站已發料訂單：" + "、".join(orphan[:10]) + ("…" if len(orphan) > 10 else ""))
    if not match and row.get("unissued", 0) <= MATCH_TOLERANCE and not orphan:
        out.append("C 未領需求 0 仍不一致（推測主檔手動扣帳／盤點／手改結存，正航無對應）")
    if not match and not out:
        out.append("A／其他：主檔補料或扣帳與正航 ST／調撥不一致（待人工核對）")
    return out


def build(today: date) -> dict:
    main_path = str(db.get_setting("main_file_path", "") or "").strip()
    if not main_path or not Path(main_path).is_file():
        raise SystemExit("找不到主檔（settings.main_file_path）")
    main_stock = {aq.norm_part(k): float(v or 0) for k, v in main_reader.read_stock(main_path).items()}
    parts = low_stock_plan.population_parts(set(main_stock))
    data = low_stock_plan.collect(parts, today)
    issued_by_part: dict = {}
    for (po, part), qty in data["issued"].items():
        issued_by_part.setdefault(part, {})[po] = qty
    rows = []
    for part in parts:
        r = data["rows"][part]
        main_balance = main_stock.get(part, 0.0)
        diff = round(r["available_without_free_st"] - main_balance, 4)
        match = abs(diff) <= MATCH_TOLERANCE
        rows.append({
            "part": part, "name": r["name"], "st": r["st"], "wh001": r["wh001"], "pending_raw": r["pending_raw"], "pending": r["pending"],
            "free_st": r["free_st"], "gl015": r["gl015"], "unissued": r["unissued"],
            "available": r["available"], "available_without_free_st": r["available_without_free_st"],
            "main_balance": main_balance, "diff": diff, "diff_with_free_st": round(r["available"] - main_balance, 4),
            "match": match, "pending_over_001": r["pending_over_001"],
            "unissued_detail": r["unissued_detail"],
            "causes": causes_for(r, issued_by_part.get(part, {}), match),
        })
    rows.sort(key=lambda x: (-abs(x["diff"]), x["part"]))
    mismatch = [x for x in rows if not x["match"]]
    cause_counts: dict = {}
    for x in mismatch:
        for c in x["causes"]:
            key = c.split("：", 1)[0]
            cause_counts[key] = cause_counts.get(key, 0) + 1
    return {
        "generated_at": local_now().strftime("%Y-%m-%d %H:%M:%S"),
        "formula": "可用 = max(0, ST − 待調撥) + min(待調撥, 001 現有) + 015 − 未領需求；"
                   "available_without_free_st = 待調撥 + 015 − 未領需求（總指揮原式），對帳用這個值",
        "main_file": main_path,
        "pending_info": data["pending_info"],
        "warnings": data["warnings"],
        "no_bom_orders": data["no_bom_orders"],
        "no_record_orders": data["no_record_orders"],
        "summary": {
            "total": len(rows), "match": len(rows) - len(mismatch), "mismatch": len(mismatch),
            "free_st_parts": sum(1 for x in rows if x["free_st"] > MATCH_TOLERANCE),
            "pending_over_001_parts": sum(1 for x in rows if x["pending_over_001"]),
            "cause_counts": dict(sorted(cause_counts.items(), key=lambda kv: -kv[1])),
        },
        "rows": rows,
    }


def write(report: dict, out_dir: Path) -> tuple:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = local_now().strftime("%Y%m%d-%H%M%S")
    json_path = out_dir / f"available_reconcile_{stamp}.json"
    csv_path = out_dir / f"available_reconcile_{stamp}.csv"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for x in report["rows"]:
            w.writerow({
                **{k: x.get(k) for k in CSV_FIELDS},
                "causes": "；".join(x["causes"]),
                "unissued_detail": "；".join(
                    f"{d['po']}（{'/'.join(d['codes'])}）需求 {d['need']:g} 已領 {d['issued']:g} 未領 {d['unissued']:g}"
                    for d in x["unissued_detail"] if d["need"] or d["issued"]),
            })
    return json_path, csv_path


def main() -> None:
    ap = argparse.ArgumentParser(description="低庫存新可用量對帳報表（唯讀）")
    ap.add_argument("--out-dir", default="/tmp")
    ap.add_argument("--today", default="")
    args = ap.parse_args()
    today = date.fromisoformat(args.today) if args.today else local_now().date()
    with low_stock._erp_session():
        pass  # 先確認正航連得上，連不上直接報錯
    report = build(today)
    json_path, csv_path = write(report, Path(args.out_dir))
    s = report["summary"]
    print(f"總數 {s['total']}、一致 {s['match']}、不一致 {s['mismatch']}；自由 ST > 0 {s['free_st_parts']}；"
          f"待調撥 > 001 現有 {s['pending_over_001_parts']}")
    print(json_path)
    print(csv_path)


if __name__ == "__main__":
    main()
