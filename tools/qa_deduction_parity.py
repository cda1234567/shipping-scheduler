"""
扣帳數量三階段比對 — orders 74/75/76/77/78/79/85 (batch 4-1~4-7)

比對規則（根據各 endpoint 實際語義）：
- calc:   不套補料，只回 needed/current_stock/shortage_amount
- drafts: 套補料，current_stock=原始庫存，supplement_qty=補料量
- mwp:    套補料，current_stock=原始庫存+supplement_qty（折入庫存）

比對項目：
  A) needed:         calc == drafts == mwp（應全等）
  B) shortage_amount: drafts == mwp（補料後應相等）
  C) supplement_qty:  drafts == mwp（應全等）
  D) resulting_stock: drafts == mwp（應全等）
  E) mwp current_stock = drafts current_stock + drafts supplement_qty（數學一致）
  F) resulting_stock math: cs + supp - needed（各自內部一致）

情境1不傳 supplement → 比 calc shortage_amount == drafts/mwp shortage_amount
情境2/3 傳 supplement → 只比 drafts vs mwp
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


# ---------------------------------------------------------------------------
# Supplement builders
# ---------------------------------------------------------------------------

def build_half_supplement(calc: dict[int, list[dict]]) -> dict[str, dict[str, float]]:
    supp: dict[str, dict[str, float]] = {}
    for oid, shortages in calc.items():
        parts: dict[str, float] = {}
        for s in shortages:
            amt = float(s.get("shortage_amount") or 0)
            if amt > TOL:
                parts[s["part_number"]] = amt / 2
        if parts:
            supp[str(oid)] = parts
    return supp


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
# Comparison
# ---------------------------------------------------------------------------

def idx(shortages: list[dict]) -> dict[str, dict]:
    return {s["part_number"]: s for s in shortages}


def fval(d: dict, field: str) -> float:
    v = d.get(field)
    return float(v) if v is not None else 0.0


def compare(
    calc: dict[int, list[dict]],
    drafts: dict[int, list[dict]],
    mwp: dict[int, list[dict]],
    has_supplement: bool,
) -> list[str]:
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
            for la, sa, lb, sb in [("calc", sc, "drafts", sd),
                                    ("calc", sc, "mwp", sm),
                                    ("drafts", sd, "mwp", sm)]:
                va = fval(sa, "needed")
                vb = fval(sb, "needed")
                if sa and sb and abs(va - vb) > TOL:
                    fails.append(f"order={oid} part={part} field=needed "
                                 f"{la}={va:.4f} {lb}={vb:.4f} diff={abs(va-vb):.4f}")

            # B) shortage_amount: drafts == mwp
            va = fval(sd, "shortage_amount")
            vb = fval(sm, "shortage_amount")
            if sd and sm and abs(va - vb) > TOL:
                fails.append(f"order={oid} part={part} field=shortage_amount "
                             f"drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

            # In scenario 1 (no supplement), calc shortage_amount should also match
            if not has_supplement and sc and sd:
                vc = fval(sc, "shortage_amount")
                vd = fval(sd, "shortage_amount")
                if abs(vc - vd) > TOL:
                    fails.append(f"order={oid} part={part} field=shortage_amount "
                                 f"calc={vc:.4f} drafts={vd:.4f} diff={abs(vc-vd):.4f}")

            # C) supplement_qty: drafts == mwp
            va = fval(sd, "supplement_qty")
            vb = fval(sm, "supplement_qty")
            if sd and sm and abs(va - vb) > TOL:
                fails.append(f"order={oid} part={part} field=supplement_qty "
                             f"drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

            # D) resulting_stock: drafts == mwp
            if "resulting_stock" in sd and "resulting_stock" in sm:
                va = fval(sd, "resulting_stock")
                vb = fval(sm, "resulting_stock")
                if abs(va - vb) > TOL:
                    fails.append(f"order={oid} part={part} field=resulting_stock "
                                 f"drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

            # E) mwp current_stock = drafts current_stock + drafts supplement_qty
            if sd and sm:
                cs_d = fval(sd, "current_stock")
                sq_d = fval(sd, "supplement_qty")
                cs_m = fval(sm, "current_stock")
                expected_cs_m = cs_d + sq_d
                if abs(cs_m - expected_cs_m) > TOL:
                    fails.append(f"order={oid} part={part} field=mwp_current_stock "
                                 f"mwp={cs_m:.4f} expected(drafts_cs+supp)={expected_cs_m:.4f} "
                                 f"diff={abs(cs_m-expected_cs_m):.4f}")

            # F) resulting_stock internal math
            # drafts: cs + supp - needed  (supplement NOT folded into cs)
            # mwp:    cs - needed          (supplement IS already folded into cs)
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_scenario(order_supplements: dict | None) -> tuple[str, list[str]]:
    has_supp = order_supplements is not None
    calc = fetch_calculate(ORDER_IDS, order_supplements)
    drafts = fetch_drafts(ORDER_IDS, order_supplements)
    mwp = fetch_mwp(ORDER_IDS, order_supplements)
    fails = compare(calc, drafts, mwp, has_supplement=has_supp)
    return ("PASS" if not fails else "FAIL"), fails


def main():
    login()

    print("Running scenario 1 (DB supplement)...", flush=True)
    status1, fails1 = run_scenario(None)

    print("Running scenario 2 (half supplement)...", flush=True)
    calc_base = fetch_calculate(ORDER_IDS, None)
    half_supp = build_half_supplement(calc_base)
    status2, fails2 = run_scenario(half_supp)

    print("Running scenario 3 (full supplement)...", flush=True)
    full_supp = build_full_supplement(calc_base)
    status3, fails3 = run_scenario(full_supp)

    print()
    print("## Agent A 扣帳稽核")
    print()
    print(f"### 情境 1（DB supplement）: {status1}")
    print(f"### 情境 2（half）: {status2}")
    print(f"### 情境 3（full）: {status3}")

    all_fails = (
        [f"[情境1] {f}" for f in fails1] +
        [f"[情境2] {f}" for f in fails2] +
        [f"[情境3] {f}" for f in fails3]
    )

    print()
    print("### FAIL 詳情")
    if all_fails:
        for f in all_fails[:50]:
            print(f"- {f}")
        if len(all_fails) > 50:
            print(f"... ({len(all_fails) - 50} more failures)")
    else:
        print("無 — 全部通過")

    print()
    print("AGENT_A_DONE")


if __name__ == "__main__":
    main()
