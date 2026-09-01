"""Final QA deduction audit v2 — batch 4-1~4-7"""
import json
import requests

BASE = "http://127.0.0.1:8765"
ORDER_IDS = [74, 75, 76, 77, 78, 79, 85]
TOL = 0.01

s = requests.Session()
s.post(f"{BASE}/api/system/edit-auth/login", json={"password": "123"}, timeout=15)


def fval(d, f):
    v = d.get(f)
    return float(v) if v is not None else 0.0


# ── order info ──────────────────────────────────────────────────────────────
r0 = s.get(f"{BASE}/api/schedule/rows", timeout=15)
raw = r0.json()
rows = raw if isinstance(raw, list) else raw.get("orders", raw.get("rows", []))
order_info = {}
for o in rows:
    oid = o.get("id") or o.get("order_id")
    if oid in ORDER_IDS:
        order_info[oid] = {"model": o.get("model", ""), "order_qty": float(o.get("order_qty") or 0)}

# ── BOM data (one call returns all models) ───────────────────────────────────
r_bom = s.get(f"{BASE}/api/bom/data", params={"order_id": 74}, timeout=30)
all_bom = r_bom.json()

bom_by_order = {}   # oid -> {pn: {qpb, sf, order_qty, expected}}
scrap_anomalies = {}  # pn -> (sf, oid, model)

for oid in ORDER_IDS:
    model = order_info[oid]["model"]
    order_qty = order_info[oid]["order_qty"]
    mdata = all_bom.get(model, {})
    parts = {}
    for comp in mdata.get("components", []):
        pn = comp["part_number"]
        qpb = float(comp.get("qty_per_board") or 0)
        sf = float(comp.get("scrap_factor") or 0)
        if sf >= 1.0 and pn not in scrap_anomalies:
            scrap_anomalies[pn] = (sf, oid, model)
        if qpb > 0 and order_qty > 0:
            parts[pn] = {"qpb": qpb, "sf": sf, "order_qty": order_qty,
                         "expected": qpb * order_qty * (1 + sf)}
    bom_by_order[oid] = parts

# ── fetch three endpoints ────────────────────────────────────────────────────
r_calc = s.get(f"{BASE}/api/schedule/calculate",
               params={"ids": ",".join(map(str, ORDER_IDS))}, timeout=60)
calc_rows = {int(row["order_id"]): row for row in r_calc.json().get("results", [])}

r_drafts = s.put(f"{BASE}/api/schedule/drafts", json={"order_ids": ORDER_IDS}, timeout=60)
drafts_rows = {}
for k, v in r_drafts.json().get("drafts", {}).items():
    drafts_rows[int(v.get("order_id", k))] = v

r_mwp = s.post(f"{BASE}/api/schedule/main-write-preview",
               json={"order_ids": ORDER_IDS}, timeout=60)
mwp_rows = {}
for sh in r_mwp.json().get("shortages", []):
    oid = int(sh.get("order_id", 0))
    mwp_rows.setdefault(oid, {})[sh["part_number"]] = sh


def idx_sh(shortages):
    return {sh["part_number"]: sh for sh in shortages}


# ── S1 checks ────────────────────────────────────────────────────────────────
s1_fails = []

for oid in ORDER_IDS:
    calc_sh = idx_sh(calc_rows.get(oid, {}).get("shortages", []))
    drafts_sh = idx_sh(drafts_rows.get(oid, {}).get("shortages", []))
    mwp_sh = mwp_rows.get(oid, {})
    all_parts = set(calc_sh) | set(drafts_sh) | set(mwp_sh)

    for pn in sorted(all_parts):
        sc = calc_sh.get(pn, {})
        sd = drafts_sh.get(pn, {})
        sm = mwp_sh.get(pn, {})

        # A: needed 3-way
        for la, sa, lb, sb in [("calc", sc, "drafts", sd),
                                ("calc", sc, "mwp", sm),
                                ("drafts", sd, "mwp", sm)]:
            if sa and sb:
                va, vb = fval(sa, "needed"), fval(sb, "needed")
                if abs(va - vb) > TOL:
                    s1_fails.append(f"needed order={oid} part={pn} {la}={va:.4f} {lb}={vb:.4f} diff={abs(va-vb):.4f}")

        # B: formula check (only parts in BOM)
        if pn in bom_by_order.get(oid, {}) and sc:
            info = bom_by_order[oid][pn]
            actual = fval(sc, "needed")
            if abs(actual - info["expected"]) > TOL:
                s1_fails.append(
                    f"needed_formula order={oid} part={pn} "
                    f"expected=(qpb={info['qpb']}*qty={info['order_qty']}*(1+sf={info['sf']:.6f}))={info['expected']:.4f} "
                    f"actual={actual:.4f} diff={abs(actual-info['expected']):.4f}"
                )

        # C: shortage calc==drafts
        if sc and sd:
            vc, vd = fval(sc, "shortage_amount"), fval(sd, "shortage_amount")
            if abs(vc - vd) > TOL:
                s1_fails.append(f"shortage_amount order={oid} part={pn} calc={vc:.4f} drafts={vd:.4f} diff={abs(vc-vd):.4f}")

        # D: shortage drafts==mwp
        if sd and sm:
            va, vb = fval(sd, "shortage_amount"), fval(sm, "shortage_amount")
            if abs(va - vb) > TOL:
                s1_fails.append(f"shortage_amount order={oid} part={pn} drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

        # F: resulting_stock drafts==mwp
        if "resulting_stock" in sd and "resulting_stock" in sm:
            va, vb = fval(sd, "resulting_stock"), fval(sm, "resulting_stock")
            if abs(va - vb) > TOL:
                s1_fails.append(f"resulting_stock order={oid} part={pn} drafts={va:.4f} mwp={vb:.4f} diff={abs(va-vb):.4f}")

        # G: mwp cs = drafts cs + supp
        if sd and sm:
            cs_d, sq_d, cs_m = fval(sd, "current_stock"), fval(sd, "supplement_qty"), fval(sm, "current_stock")
            if abs(cs_m - (cs_d + sq_d)) > TOL:
                s1_fails.append(f"mwp_cs order={oid} part={pn} mwp_cs={cs_m:.4f} expected={cs_d+sq_d:.4f} diff={abs(cs_m-(cs_d+sq_d)):.4f}")

        # H: rs math drafts
        if "resulting_stock" in sd:
            cs, sq, nd, rs = fval(sd, "current_stock"), fval(sd, "supplement_qty"), fval(sd, "needed"), fval(sd, "resulting_stock")
            if abs(rs - (cs + sq - nd)) > TOL:
                s1_fails.append(f"rs_math_drafts order={oid} part={pn} rs={rs:.4f} cs+supp-needed={cs+sq-nd:.4f} diff={abs(rs-(cs+sq-nd)):.4f}")

        # H: rs math mwp
        if "resulting_stock" in sm:
            cs, nd, rs = fval(sm, "current_stock"), fval(sm, "needed"), fval(sm, "resulting_stock")
            if abs(rs - (cs - nd)) > TOL:
                s1_fails.append(f"rs_math_mwp order={oid} part={pn} rs={rs:.4f} cs-needed={cs-nd:.4f} diff={abs(rs-(cs-nd)):.4f}")

# ── S2: full supplement ───────────────────────────────────────────────────────
full_supp = {}
for oid in ORDER_IDS:
    parts = {}
    for sh in calc_rows.get(oid, {}).get("shortages", []):
        amt = float(sh.get("shortage_amount") or 0)
        if amt > TOL:
            parts[sh["part_number"]] = amt
    if parts:
        full_supp[str(oid)] = parts

r_d2 = s.put(f"{BASE}/api/schedule/drafts",
             json={"order_ids": ORDER_IDS, "order_supplements": full_supp}, timeout=60)
r_m2 = s.post(f"{BASE}/api/schedule/main-write-preview",
              json={"order_ids": ORDER_IDS, "order_supplements": full_supp}, timeout=60)

mwp2 = {}
for sh in r_m2.json().get("shortages", []):
    oid = int(sh.get("order_id", 0))
    mwp2.setdefault(oid, {})[sh["part_number"]] = sh

s2_fails = []
for k, v in r_d2.json().get("drafts", {}).items():
    oid = int(v.get("order_id", k))
    for sh in v.get("shortages", []):
        pn = sh["part_number"]
        rs = fval(sh, "resulting_stock")
        if rs < -TOL:
            s2_fails.append(f"rs_neg order={oid} part={pn} drafts_rs={rs:.4f}")
        sm = mwp2.get(oid, {}).get(pn, {})
        if sm:
            rs_m = fval(sm, "resulting_stock")
            if rs_m < -TOL:
                s2_fails.append(f"rs_neg order={oid} part={pn} mwp_rs={rs_m:.4f}")

# ── output ────────────────────────────────────────────────────────────────────
s1_status = "PASS" if not s1_fails else f"FAIL ({len(s1_fails)} fails)"
s2_status = "PASS" if not s2_fails else f"FAIL ({len(s2_fails)} fails)"

print()
print("## Agent A 扣帳稽核 v2 (v2026.04.24.4)")
print()
print(f"### 情境 1: {s1_status}")
print(f"### 情境 2: {s2_status}")
print()
print("### FAIL 詳情")
all_fails = [f"[S1] {f}" for f in s1_fails] + [f"[S2] {f}" for f in s2_fails]
if all_fails:
    for f in all_fails[:40]:
        print(f"- {f}")
    if len(all_fails) > 40:
        print(f"... ({len(all_fails)-40} more)")
else:
    print("無 — 全部通過")

print()
print("### 異常 scrap_factor（>= 1.0）")
if scrap_anomalies:
    for pn, (sf, oid, model) in sorted(scrap_anomalies.items()):
        print(f"- part={pn} scrap_factor={sf} (order={oid} model={model})")
else:
    print("無 — scrap 校正已全部生效")

print()
print("### 結論")
total = len(all_fails)
if total == 0 and not scrap_anomalies:
    print("全部對齊，新公式與 scrap 校正均已生效")
else:
    issues = []
    if total:
        issues.append(f"{total} 個比對失敗")
    if scrap_anomalies:
        issues.append(f"{len(scrap_anomalies)} 個異常 scrap_factor（見上）")
    print("仍有問題：" + "，".join(issues))

print()
print("AGENT_A_DONE")
