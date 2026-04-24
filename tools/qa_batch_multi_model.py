"""
QA: batch numeric consistency check across all merged orders (multi-model).
Pure API — no Playwright needed.

Checks:
  1. Integer-truncation drift: needed has fractional part > 1e-3 (UI rounds to int -> drift)
  2. Logic consistency: shortage_amount == needed - current_stock - prev_qty_cs
  3. Running balance carry-over: current_stock for part in order N+1 ==
     current_stock[N] + prev_qty_cs[N] - needed[N]  (within tol 0.1)
"""
from __future__ import annotations
import requests
from typing import Any, Dict, List

BASE_URL = "http://127.0.0.1:8765"
TOL = 1e-3
RB_TOL = 0.1


def _approx(a: Any, b: Any, tol: float = TOL) -> bool:
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False


def _has_fraction(v: Any, tol: float = TOL) -> bool:
    """True when v has a fractional part > tol (UI int-display would lose precision)."""
    try:
        f = float(v)
        return abs(f - round(f)) > tol
    except Exception:
        return False


def main() -> None:
    drafts: Dict[str, Any] = (
        requests.get(f"{BASE_URL}/api/schedule/drafts", timeout=15).json().get("drafts", {})
    )
    if not drafts:
        print("No drafts found — nothing to test.")
        return

    # part -> list of shortage dicts in order processed (for running-balance check)
    part_seq: Dict[str, List[Dict]] = {}

    rows_summary = []
    all_drifts: List[str] = []

    for oid_str in sorted(drafts, key=int):
        draft = drafts[oid_str]
        oid = int(oid_str)
        model = draft.get("model", "?")
        shortages: List[Dict] = draft.get("shortages") or []

        order_drifts: List[str] = []
        logic_ok = True

        for s in shortages:
            part = s.get("part_number", "?")
            needed = s.get("needed")
            cur_stock = s.get("current_stock")
            shortage_amt = s.get("shortage_amount")
            prev_qty_cs = float(s.get("prev_qty_cs") or 0)

            # Check 1 — integer-truncation drift
            if needed is not None and _has_fraction(needed):
                ui_int = round(float(needed))
                diff = abs(float(needed) - ui_int)
                msg = f"Order{oid}/{part}: UI={ui_int}, API={needed} (diff {diff:.6f})"
                order_drifts.append(msg)
                all_drifts.append(msg)

            # Check 2 — logic: shortage_amount == needed - current_stock - prev_qty_cs
            if None not in (needed, cur_stock, shortage_amt):
                expected = float(needed) - float(cur_stock) - prev_qty_cs
                if not _approx(expected, float(shortage_amt)):
                    logic_ok = False
                    msg = (
                        f"Order{oid}/{part}: LOGIC FAIL "
                        f"needed({needed})-cur({cur_stock})-prev({prev_qty_cs})"
                        f"={expected:.4f} != shortage_amt({shortage_amt})"
                    )
                    order_drifts.append(msg)
                    all_drifts.append(msg)

            # Accumulate for running-balance
            part_seq.setdefault(part, []).append(
                dict(order_id=oid, model=model, needed=needed,
                     current_stock=cur_stock, prev_qty_cs=prev_qty_cs)
            )

        rows_summary.append(dict(
            order_id=oid, model=model, parts=len(shortages),
            drift_count=len(order_drifts), logic_ok=logic_ok,
        ))

    # Check 3 — running balance across orders for shared parts
    rb_fails: List[str] = []
    for part, entries in part_seq.items():
        if len(entries) < 2:
            continue
        for i in range(1, len(entries)):
            prev, curr = entries[i - 1], entries[i]
            try:
                expected_next = (float(prev["current_stock"] or 0)
                                 + float(prev["prev_qty_cs"] or 0)
                                 - float(prev["needed"] or 0))
                actual_next = float(curr["current_stock"] or 0)
            except Exception:
                continue
            if not _approx(expected_next, actual_next, tol=RB_TOL):
                rb_fails.append(
                    f"{part}: order {prev['order_id']}→{curr['order_id']} "
                    f"expected_next_stock={expected_next:.4f} actual={actual_next:.4f}"
                )

    # ---- Output ----
    total_parts = sum(r["parts"] for r in rows_summary)
    print(f"\n## 測試結果：{len(rows_summary)} 張 orders / {total_parts} 料號\n")
    print("| order_id | model | parts | ui_api_drifts | running_bal_ok |")
    print("|----------|-------|-------|---------------|----------------|")
    for r in rows_summary:
        rb_for = [f for f in rb_fails
                  if f"order {r['order_id']}→" in f or f"→{r['order_id']} " in f]
        rb_status = "OK" if not rb_for else "FAIL"
        drift_col = str(r["drift_count"]) if r["drift_count"] == 0 else f"{r['drift_count']} drift(s)"
        print(f"| {r['order_id']} | {r['model']} | {r['parts']} | {drift_col} | {rb_status} |")

    print("\n### Drift 詳情")
    if all_drifts:
        for d in all_drifts:
            print(f"- {d}")
    else:
        print("(無 drift)")

    print("\n### Running Balance")
    if rb_fails:
        for f in rb_fails:
            print(f"- {f}")
    else:
        print("全部跨訂單 carry-over 正常")

    total_issues = len(all_drifts) + len(rb_fails)
    print("\n### 結論")
    if total_issues == 0:
        print("全部 PASS")
    else:
        print(f"發現 {len(all_drifts)} 個 drift，{len(rb_fails)} 個 running-balance 異常")


if __name__ == "__main__":
    main()
