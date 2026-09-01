"""
扣帳三階段比對 v2 — orders 74/75/76/77/78/79/85 (batch 4-1~4-7)
重點：新公式 needed = qty_per_board × schedule × (1 + scrap_factor)，scrap_factor 校正後驗證

比對規則：
- calc:   不套補料，回 needed/current_stock/shortage_amount
- drafts: 套補料，current_stock=原始庫存，supplement_qty=補料量
- mwp:    套補料，current_stock=原始庫存+supplement_qty（折入庫存）

情境 1: DB supplement 不動（order_supplements 不傳）
情境 2: full supplement = shortage_amount（驗證 resulting_stock 歸 0 or ≥ 0）
"""
from __future__ import annotations
import json
import requests

BASE_URL = "http://127.0.0.1:8765"
ORDER_IDS = [74, 75, 76, 77, 78, 79, 85]
TOL = 0.01

session = requests.Session()


def login():
    r = session.post(f"{BASE_URL}/api/system/edit-auth/login",
                     json={"password": "123"}, timeout=15)
    r.raise_for_status()
    assert r.json().get("ok"), f"Login failed: {r.text}"


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_calculate(order_ids: list[int], order_supplements: dict | None = None) -> dict[int, list[dict]]:
    params: dict = {"ids": ",".join(map(str, order_ids))}
    if order_supplements:
        params["order_supplements"] = json.dumps(order_supplements)
    r = session.get(f"{BASE_URL}/api/schedule/calculate", params=params, timeout=60)
    r.raise_for_status()
    out: dict[int, list[dict]] = {}
    for row in r.json().get("results", []):
        oid = int(row.get("order_id", 0))
        out[oid] = row.get("shortages", [])
    return out


def fetch_drafts(order_ids: list[int], order_supplements: dict | None = None) -> dict[int, list[dict]]:
    body: dict = {"order_ids": order_ids}
    if order_supplements is not None:
        body["order_supplements"] = order_supplements
    r = session.put(f"{BASE_URL}/api/schedule/drafts", json=body, timeout=60)
    r.raise_for_status()
    out: dict[int, list[dict]] = {}
    for k, v in r.json().get("drafts", {}).items():
        oid = int(v.get("order_id", k))
        out[oid] = v.get("shortages", [])
    return out


def fetch_mwp(order_ids: list[int], order_supplements: dict | None = None) -> dict[int, list[dict]]:
    body: dict = {"order_ids": order_ids}
    if order_supplements is not None:
        body["order_supplements"] = order_supplements
    r = session.post(f"{BASE_URL}/api/schedule/main-write-preview", json=body, timeout=60)
    r.raise_for_status()
    out: dict[int, list[dict]] = {}
    for s in r.json().get("shortages", []):
        oid = int(s.get("order_id", 0))
        out.setdefault(oid, []).append(s)
    return out


def fetch_bom_parts(order_ids: list[int]) -> dict[int, list[dict]]:
    """Fetch BOM-level part info including qty_per_board and scrap_factor per order."""
    params: dict = {"ids": ",".join(map(str, order_ids))}
    r = session.get(f"{BASE_URL}/api/schedule/calculate", params=params, timeout=60)
    r.raise_for_status()
    out: dict[int, list[dict]] = {}
    for row in r.json().get("results", []):
        oid = int(row.get("order_id", 0))
        out[oid] = row.get("shortages", [])
    return out


# ---------------------------------------------------------------------------
# Supplement builder
# ---------------------------------------------------------------------------

def build_full_supplement(calc: dict[int, list[dict]]) -> dict[str, dict[str, float]]:
    supp: dict[str, dict[str, float]] = {}
    for oid, shortages in calc.items():
        parts: dict[str, float] = {}
        for s in shortages:
            amt = float(s.get("shortage_amount") or 0)
            if amt > TOL:
                parts[s["part_number"]] = amt
        if parts:
            supp[str(oid)] = parts
    return supp


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------

def idx(shortages: list[dict]) -> dict[str, dict]:
    return {s["part_number"]: s for s in shortages}


def fval(d: dict, field: str) -> float:
    v = d.get(field)
    return float(v) if v is not None else 0.0


def compare_scenario1(
    calc: dict[int, list[dict]],
    drafts: dict[int, list[dict]],
    mwp: dict[int, list[dict]],
) -> list[str]:
    """Scenario 1: DB supplement unchanged. Checks needed consistency, shortage cross-check, math."""
    fails: list[str] = []
    all_oids = set(calc) | set(drafts) | set(mwp)

    for oid in sorted(all_oids):
        ic = idx(calc.get(oid, []))
        id_ = idx(drafts.get(oid, []))
        im = idx(mwp.get(oid, []))
        all_parts = set(ic) | set(id_) | set(im)

        for part in sorted(all_parts):
            sc = ic.get(part, {})
            sd = id_.get(part, {})
            sm = im.get(part, {})

            # A) needed: all three should agree
            pairs = [("calc", sc, "drafts", sd),
                     ("calc", sc, "mwp", sm),
                     ("drafts", sd, "mwp", sm)]
            for la, sa, lb, sb in pairs:
                if sa and sb:
                    va = fval(sa, "needed")
                    vb = fval(sb, "needed")
                    if abs(va - vb) > TOL:
                        fails.append(f"order={oid} part={part} field=needed "
                                     f"{la}={va:.4f} {lb}={vb:.4f} diff={abs(va-vb):.4f}")

            # B) needed math: calc should carry qty_per_board, order_qty, scrap_factor
            # Verify via: needed == qty_per_board * order_qty * (1 + scrap_factor)
            if sc:
                qpb = fval(sc, "qty_per_board")
                scrap = fval(sc, "scrap_factor")
                order_qty = fval(sc, "order_qty")
                needed = fval(sc, "needed")
                if qpb > 0 and order_qty > 0:
                    expected = qpb * order_qty * (1 + scrap)
                    if abs(needed - expected) > TOL:
                        fails.append(f"order={oid} part={part} field=needed_formula "
                                     f"expected=(qty_per_board={qpb}×order_qty={order_qty}×(1+scrap={scrap}))={expected:.4f} "
                                     f"actual={needed:.4f} diff={abs(needed-expected):.4f}")

            # C) shortage_amount: calc == drafts (no supplement scenario)
            if sc and sd:
                vc = fval(sc, "shortage_amount")
                vd = fval(sd, "shortage_amount")
                if abs(vc - vd) > TOL:
                    fails.append(f"order={oid} part={part} field=shortage_amount "
                                 f"calc={vc:.4f} drafts={vd:.4f} diff={abs(vc-vd):.4f}")

            # D) shortage_amount: drafts == mwp
            if sd and sm:
                va = fval(sd, "shortage_amount")
                vb = fval(sm, "shortage_amount")
                if abs(va - vb) > TOL:
                    fails.append(f"order={oid} part={part} field=shortage_amount "
                                 f"drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

            # E) supplement_qty: drafts == mwp
            if sd and sm:
                va = fval(sd, "supplement_qty")
                vb = fval(sm, "supplement_qty")
                if abs(va - vb) > TOL:
                    fails.append(f"order={oid} part={part} field=supplement_qty "
                                 f"drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

            # F) resulting_stock: drafts == mwp
            if "resulting_stock" in sd and "resulting_stock" in sm:
                va = fval(sd, "resulting_stock")
                vb = fval(sm, "resulting_stock")
                if abs(va - vb) > TOL:
                    fails.append(f"order={oid} part={part} field=resulting_stock "
                                 f"drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

            # G) mwp current_stock = drafts current_stock + drafts supplement_qty
            if sd and sm:
                cs_d = fval(sd, "current_stock")
                sq_d = fval(sd, "supplement_qty")
                cs_m = fval(sm, "current_stock")
                expected_cs_m = cs_d + sq_d
                if abs(cs_m - expected_cs_m) > TOL:
                    fails.append(f"order={oid} part={part} field=mwp_current_stock "
                                 f"mwp_cs={cs_m:.4f} expected(drafts_cs+supp)={expected_cs_m:.4f} "
                                 f"diff={abs(cs_m-expected_cs_m):.4f}")

            # H) resulting_stock internal math
            if "resulting_stock" in sd:
                cs = fval(sd, "current_stock")
                sq = fval(sd, "supplement_qty")
                nd = fval(sd, "needed")
                rs = fval(sd, "resulting_stock")
                expected = cs + sq - nd
                if abs(rs - expected) > TOL:
                    fails.append(f"order={oid} part={part} field=rs_math "
                                 f"drafts rs={rs:.4f} cs+supp-needed={expected:.4f} "
                                 f"diff={abs(rs-expected):.4f}")
            if "resulting_stock" in sm:
                cs = fval(sm, "current_stock")
                nd = fval(sm, "needed")
                rs = fval(sm, "resulting_stock")
                expected = cs - nd
                if abs(rs - expected) > TOL:
                    fails.append(f"order={oid} part={part} field=rs_math "
                                 f"mwp rs={rs:.4f} cs-needed={expected:.4f} "
                                 f"diff={abs(rs-expected):.4f}")

    return fails


def compare_scenario2(
    calc: dict[int, list[dict]],
    drafts: dict[int, list[dict]],
    mwp: dict[int, list[dict]],
) -> list[str]:
    """Scenario 2: full supplement = shortage_amount. Verify resulting_stock >= 0."""
    fails: list[str] = []
    all_oids = set(calc) | set(drafts) | set(mwp)

    for oid in sorted(all_oids):
        ic = idx(calc.get(oid, []))
        id_ = idx(drafts.get(oid, []))
        im = idx(mwp.get(oid, []))
        all_parts = set(ic) | set(id_) | set(im)

        for part in sorted(all_parts):
            sc = ic.get(part, {})
            sd = id_.get(part, {})
            sm = im.get(part, {})

            # needed: all three agree
            pairs = [("calc", sc, "drafts", sd),
                     ("calc", sc, "mwp", sm),
                     ("drafts", sd, "mwp", sm)]
            for la, sa, lb, sb in pairs:
                if sa and sb:
                    va = fval(sa, "needed")
                    vb = fval(sb, "needed")
                    if abs(va - vb) > TOL:
                        fails.append(f"order={oid} part={part} field=needed "
                                     f"{la}={va:.4f} {lb}={vb:.4f} diff={abs(va-vb):.4f}")

            # resulting_stock should be >= 0 when full supplement supplied
            for label, sd_ in [("drafts", sd), ("mwp", sm)]:
                if "resulting_stock" in sd_:
                    rs = fval(sd_, "resulting_stock")
                    if rs < -TOL:
                        fails.append(f"order={oid} part={part} field=resulting_stock "
                                     f"{label}={rs:.4f} expected>=0 (full supplement)")

            # shortage_amount should be 0 after full supplement
            for label, sd_ in [("drafts", sd), ("mwp", sm)]:
                if sd_:
                    sa_val = fval(sd_, "shortage_amount")
                    if sa_val > TOL:
                        fails.append(f"order={oid} part={part} field=shortage_amount "
                                     f"{label}={sa_val:.4f} expected=0 after full supplement")

    return fails


def check_scrap_factors(calc: dict[int, list[dict]]) -> list[str]:
    """Find any parts with scrap_factor >= 1.0 (should have been corrected)."""
    anomalies: list[str] = []
    seen: set[str] = set()
    for oid, shortages in calc.items():
        for s in shortages:
            part = s.get("part_number", "")
            scrap = s.get("scrap_factor")
            if scrap is not None:
                sf = float(scrap)
                key = f"{part}:{sf}"
                if sf >= 1.0 and key not in seen:
                    seen.add(key)
                    anomalies.append(f"part={part} scrap_factor={sf} (order={oid})")
    return anomalies


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    login()

    print("Scenario 1: DB supplement unchanged...", flush=True)
    calc1 = fetch_calculate(ORDER_IDS)
    drafts1 = fetch_drafts(ORDER_IDS)
    mwp1 = fetch_mwp(ORDER_IDS)
    fails1 = compare_scenario1(calc1, drafts1, mwp1)
    status1 = "PASS" if not fails1 else f"FAIL ({len(fails1)} fails)"

    print("Scenario 2: full supplement = shortage_amount...", flush=True)
    full_supp = build_full_supplement(calc1)
    calc2 = fetch_calculate(ORDER_IDS, full_supp)
    drafts2 = fetch_drafts(ORDER_IDS, full_supp)
    mwp2 = fetch_mwp(ORDER_IDS, full_supp)
    fails2 = compare_scenario2(calc2, drafts2, mwp2)
    status2 = "PASS" if not fails2 else f"FAIL ({len(fails2)} fails)"

    scrap_anomalies = check_scrap_factors(calc1)

    print()
    print("## Agent A 扣帳稽核 v2 (v2026.04.24.3+)")
    print()
    print(f"### 情境 1: {status1}")
    print(f"### 情境 2: {status2}")

    all_fails = (
        [f"[S1] {f}" for f in fails1] +
        [f"[S2] {f}" for f in fails2]
    )

    print()
    print("### FAIL 詳情")
    if all_fails:
        for f in all_fails[:60]:
            print(f"- {f}")
        if len(all_fails) > 60:
            print(f"... ({len(all_fails) - 60} more)")
    else:
        print("無 — 全部通過")

    print()
    print("### 異常 scrap_factor（>= 1.0）")
    if scrap_anomalies:
        for a in scrap_anomalies[:20]:
            print(f"- {a}")
    else:
        print("無 — scrap 校正已全部生效")

    total_fails = len(all_fails)
    print()
    print("### 結論")
    if total_fails == 0 and not scrap_anomalies:
        print("全部對齊，新公式與 scrap 校正均已生效")
    else:
        issues = []
        if total_fails:
            issues.append(f"{total_fails} 個比對失敗")
        if scrap_anomalies:
            issues.append(f"{len(scrap_anomalies)} 個異常 scrap_factor")
        print("仍有問題：" + "，".join(issues))

    print()
    print("AGENT_A_DONE")


if __name__ == "__main__":
    main()
