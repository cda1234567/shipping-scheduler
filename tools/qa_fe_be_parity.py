"""
QA: FE/BE parity check — 反向比對前端 calculate() 與後端 /api/schedule/calculate 的缺料清單。

比對項目：
  1. missed_by_fe  : API 有但前端沒有的料（前端漏算缺料）
  2. phantom_in_fe : 前端有但 API 沒有的料（前端多算缺料）
  3. needed_drift  : 兩邊都有、但 needed 數量不同（>0.01 才算 drift）

使用方式：
  python -X utf8 tools/qa_fe_be_parity.py
"""
from __future__ import annotations
import json
import os
import subprocess
import sys
import tempfile
from typing import Any

import requests

BASE_URL = "http://127.0.0.1:8765"
NEEDED_TOL = 0.01
SHORTAGE_TOL = 0.01

# calculator.js 的路徑（相對於此腳本所在的 tools/ 目錄）
CALC_JS = os.path.join(os.path.dirname(__file__), "..", "static", "modules", "calculator.js")


# ---------------------------------------------------------------------------
# 資料擷取
# ---------------------------------------------------------------------------

def fetch_orders() -> list[dict]:
    resp = requests.get(f"{BASE_URL}/api/schedule/rows", timeout=15)
    resp.raise_for_status()
    d = resp.json()
    rows = d.get("rows", d) if isinstance(d, dict) else d
    return rows


def fetch_be_calculate(order_ids: list[int]) -> dict[int, list[dict]]:
    """後端計算結果：{order_id: [shortage, ...]}"""
    ids_str = ",".join(map(str, order_ids))
    resp = requests.get(f"{BASE_URL}/api/schedule/calculate", params={"ids": ids_str}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", data) if isinstance(data, dict) else data
    be: dict[int, list[dict]] = {}
    for row in results:
        oid = int(row.get("order_id", 0))
        be[oid] = row.get("shortages", [])
    return be


def fetch_bom_data() -> dict:
    resp = requests.get(f"{BASE_URL}/api/bom/data", timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_main_data() -> dict:
    resp = requests.get(f"{BASE_URL}/api/main-file/data", timeout=15)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# 前端 calculate() via Node
# ---------------------------------------------------------------------------

NODE_WRAPPER = r"""
import { calculate } from '{CALC_PATH}';
import { readFileSync } from 'fs';
const input = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const results = calculate(
  input.orders,
  input.bom_data,
  input.stock,
  input.moq,
  {},  // dispatchedConsumption — 傳空，與後端 baseline 對齊
  {},  // stStock
  {}   // orderSupplementsByOrder
);
process.stdout.write(JSON.stringify(results));
"""


def run_fe_calculate(orders: list[dict], bom_data: dict, stock: dict, moq: dict) -> list[dict]:
    """呼叫前端 calculator.js，回傳 results 陣列。"""
    abs_path = os.path.abspath(CALC_JS).replace("\\", "/")
    # Windows ESM import 需要 file:// URL
    calc_path = f"file:///{abs_path}"
    wrapper_code = NODE_WRAPPER.replace("{CALC_PATH}", calc_path)

    payload = json.dumps({
        "orders": orders,
        "bom_data": bom_data,
        "stock": stock,
        "moq": moq,
    }, ensure_ascii=False)

    # 用兩個暫存檔：一個給 wrapper .mjs，一個給 payload JSON
    with tempfile.NamedTemporaryFile(mode="w", suffix=".mjs", delete=False, encoding="utf-8") as f:
        f.write(wrapper_code)
        tmp_script = f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        f.write(payload)
        tmp_data = f.name

    try:
        result = subprocess.run(
            ["node", tmp_script, tmp_data],
            capture_output=True, text=True, encoding="utf-8", timeout=30
        )
        if result.returncode != 0:
            print(f"[ERROR] Node stderr:\n{result.stderr}", file=sys.stderr)
            sys.exit(1)
        return json.loads(result.stdout)
    finally:
        os.unlink(tmp_script)
        os.unlink(tmp_data)


# ---------------------------------------------------------------------------
# 比對邏輯
# ---------------------------------------------------------------------------

def compare_order(
    order_id: int,
    model: str,
    be_shortages: list[dict],
    fe_shortages: list[dict],
) -> dict:
    be_map = {s["part_number"].upper(): s for s in be_shortages}
    fe_map = {s["part_number"].upper(): s for s in fe_shortages}

    be_parts = set(be_map)
    fe_parts = set(fe_map)

    missed_by_fe = be_parts - fe_parts       # API 有但前端沒有
    phantom_in_fe = fe_parts - be_parts      # 前端有但 API 沒有
    common = be_parts & fe_parts

    needed_drifts = []
    for part in sorted(common):
        be_needed = float(be_map[part].get("needed", 0) or 0)
        fe_needed = float(fe_map[part].get("needed", 0) or 0)
        diff = abs(be_needed - fe_needed)
        if diff > NEEDED_TOL:
            needed_drifts.append({
                "part": part,
                "be_needed": be_needed,
                "fe_needed": fe_needed,
                "diff": diff,
                "be_shortage": float(be_map[part].get("shortage_amount", 0) or 0),
                "fe_shortage": float(fe_map[part].get("shortage_amount", 0) or 0),
                "be_stock": float(be_map[part].get("current_stock", 0) or 0),
            })

    missed_details = []
    for part in sorted(missed_by_fe):
        s = be_map[part]
        missed_details.append({
            "part": part,
            "be_needed": float(s.get("needed", 0) or 0),
            "be_shortage": float(s.get("shortage_amount", 0) or 0),
            "be_stock": float(s.get("current_stock", 0) or 0),
        })

    phantom_details = []
    for part in sorted(phantom_in_fe):
        s = fe_map[part]
        phantom_details.append({
            "part": part,
            "fe_needed": float(s.get("needed", 0) or 0),
            "fe_shortage": float(s.get("shortage_amount", 0) or 0),
        })

    return {
        "order_id": order_id,
        "model": model,
        "be_parts": len(be_parts),
        "fe_parts": len(fe_parts),
        "missed_by_fe": missed_details,
        "phantom_in_fe": phantom_details,
        "needed_drifts": needed_drifts,
    }


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    # 確認版號
    health = requests.get(f"{BASE_URL}/api/health", timeout=5).json()
    version = health.get("version", "?")
    print(f"Service version: {version}")

    orders = fetch_orders()
    if not orders:
        print("No orders found.")
        return

    order_ids = [int(o["id"]) for o in orders]

    # 後端結果
    be_by_order = fetch_be_calculate(order_ids)

    # 前端所需資料
    bom_data = fetch_bom_data()
    main_data = fetch_main_data()
    stock = main_data.get("stock", {})
    moq = main_data.get("moq", {})

    # 前端計算
    fe_results_list = run_fe_calculate(orders, bom_data, stock, moq)
    fe_by_order: dict[int, list[dict]] = {}
    for r in fe_results_list:
        oid = int(r.get("order_id", 0))
        fe_by_order[oid] = r.get("shortages", [])

    # 逐張比對
    comparisons = []
    for order in orders:
        oid = int(order["id"])
        model = order.get("model", "?")
        be_shortages = be_by_order.get(oid, [])
        fe_shortages = fe_by_order.get(oid, [])
        comparisons.append(compare_order(oid, model, be_shortages, fe_shortages))

    # ---- 輸出 ----
    print(f"\n## FE/BE Parity 驗證結果（{version}）\n")
    print("| order | model | BE parts | FE parts | missed by FE | phantom in FE | needed drifts |")
    print("|-------|-------|----------|----------|--------------|---------------|---------------|")
    for c in comparisons:
        missed = len(c["missed_by_fe"])
        phantom = len(c["phantom_in_fe"])
        drifts = len(c["needed_drifts"])
        m_col = str(missed) if missed == 0 else f"**{missed}**"
        p_col = str(phantom) if phantom == 0 else f"**{phantom}**"
        d_col = str(drifts) if drifts == 0 else f"**{drifts}**"
        print(f"| {c['order_id']} | {c['model']} | {c['be_parts']} | {c['fe_parts']} | {m_col} | {p_col} | {d_col} |")

    # Drift 詳情
    all_missed: list[str] = []
    all_phantom: list[str] = []
    all_drifts: list[str] = []

    for c in comparisons:
        oid = c["order_id"]
        model = c["model"]
        for item in c["missed_by_fe"]:
            all_missed.append(
                f"order {oid}/{model} / {item['part']}: "
                f"BE needed={item['be_needed']:.2f}, shortage={item['be_shortage']:.2f}, stock={item['be_stock']:.0f}"
            )
        for item in c["phantom_in_fe"]:
            all_phantom.append(
                f"order {oid}/{model} / {item['part']}: "
                f"FE needed={item['fe_needed']:.2f}, shortage={item['fe_shortage']:.2f}"
            )
        for item in c["needed_drifts"]:
            direction = "前端少算" if item["fe_needed"] < item["be_needed"] else "前端多算"
            all_drifts.append(
                f"order {oid}/{model} / {item['part']}: "
                f"BE needed={item['be_needed']:.4f}, FE needed={item['fe_needed']:.4f} "
                f"(diff={item['diff']:.4f}, {direction})"
            )

    print("\n### Missed by FE（API 有、前端沒有）")
    if all_missed:
        for m in all_missed:
            print(f"- {m}")
    else:
        print("(無)")

    print("\n### Phantom in FE（前端有、API 沒有）")
    if all_phantom:
        for p in all_phantom:
            print(f"- {p}")
    else:
        print("(無)")

    print("\n### Needed Drift 詳情（needed 數量不同）")
    if all_drifts:
        for d in all_drifts:
            print(f"- {d}")
    else:
        print("(無 drift)")

    total_issues = len(all_missed) + len(all_phantom) + len(all_drifts)
    print("\n### 結論")
    if total_issues == 0:
        print("全部對齊 PASS")
    else:
        print(
            f"發現 {len(all_missed)} 個 missed_by_fe、"
            f"{len(all_phantom)} 個 phantom_in_fe、"
            f"{len(all_drifts)} 個 needed drift，需追查"
        )
    sys.exit(0 if total_issues == 0 else 1)


if __name__ == "__main__":
    main()
