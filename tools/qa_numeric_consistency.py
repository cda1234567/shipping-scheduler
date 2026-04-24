"""
QA numeric consistency check for: batch merge modal -> draft (副檔) -> main-write (寫入主檔).

Validates that numbers (needed/current_stock/shortage/supplement/resulting_stock) stay
consistent across the three views. Drives the UI with Playwright AND cross-checks via HTTP.

Run:
    python D:/claude/shipping-scheduler/tools/qa_numeric_consistency.py
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import httpx
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

BASE_URL = "http://127.0.0.1:8765"
EDIT_PASSWORD = "123"
SCREENSHOT_DIR = Path("/tmp")
INITIAL_PNG = SCREENSHOT_DIR / "dispatch-scheduler-initial.png"
MODAL_PNG = SCREENSHOT_DIR / "dispatch-scheduler-merge-modal.png"
MAIN_WRITE_PNG = SCREENSHOT_DIR / "dispatch-scheduler-main-write.png"


# ---- numeric helpers ----------------------------------------------------------

def _round6(v: Any) -> float:
    try:
        return round(float(v), 6)
    except Exception:
        return float("nan")


def _approx_equal(a: Any, b: Any, tol: float = 1e-3) -> bool:
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False


# ---- main QA ------------------------------------------------------------------

async def fetch_drafts(client: httpx.AsyncClient) -> Dict[str, Any]:
    r = await client.get(f"{BASE_URL}/api/schedule/drafts")
    r.raise_for_status()
    return r.json().get("drafts", {})


async def fetch_calculate(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    r = await client.get(f"{BASE_URL}/api/schedule/calculate")
    r.raise_for_status()
    return r.json().get("results", [])


async def fetch_rows(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    r = await client.get(f"{BASE_URL}/api/schedule/rows")
    r.raise_for_status()
    return r.json().get("rows", [])


def pick_target_draft(drafts: Dict[str, Any]) -> Dict[str, Any]:
    """Prefer a draft that has both supplements AND shortages (most interesting)."""
    best = None
    best_score = -1
    for oid, d in drafts.items():
        shortages = d.get("shortages") or []
        sup = d.get("supplements") or {}
        score = len(sup) + 3 * len(shortages)
        if score > best_score:
            best_score = score
            best = d
    return best or {}


async def run_qa():
    results: Dict[str, Any] = {
        "server_version": None,
        "draft_count": 0,
        "target_order": None,
        "stage_A_merge_modal": {},
        "stage_B_draft_api": {},
        "stage_C_main_write_preview": {},
        "pass_items": [],
        "fail_items": [],
        "warnings": [],
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        # --- Stage 0: sanity check ----------------------------------------
        v = (await client.get(f"{BASE_URL}/version")).json()
        results["server_version"] = v.get("version")
        drafts = await fetch_drafts(client)
        results["draft_count"] = len(drafts)
        calc = await fetch_calculate(client)
        rows = await fetch_rows(client)
        row_by_id = {r["id"]: r for r in rows}
        calc_by_id = {c["order_id"]: c for c in calc}

        if not drafts:
            results["fail_items"].append("No drafts exist on server — cannot test flow")
            print(json.dumps(results, ensure_ascii=False, indent=2))
            return

        target_draft = pick_target_draft(drafts)
        oid = target_draft.get("order_id")
        model = target_draft.get("model")
        results["target_order"] = {"order_id": oid, "model": model, "po": target_draft.get("po_number")}
        print(f"[INFO] target draft order_id={oid} model={model} "
              f"sup={len(target_draft.get('supplements') or {})} "
              f"shortages={len(target_draft.get('shortages') or [])}")

    # --- Stage 1: drive UI with Playwright -------------------------------
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1600, "height": 1000})
        page = await context.new_page()

        console_errs: List[str] = []
        page.on("console", lambda msg: console_errs.append(msg.text) if msg.type == "error" else None)

        try:
            await page.goto(BASE_URL, wait_until="networkidle")
            # wait for app to render schedule rows
            try:
                await page.wait_for_selector(".row-check", timeout=30000)
            except PwTimeout:
                results["warnings"].append("schedule rows not rendered within 30s")
            await page.screenshot(path=str(INITIAL_PNG))
            print(f"[INFO] initial screenshot -> {INITIAL_PNG}")

            # ---- login edit mode ----
            await page.click("#btn-edit-auth")
            # the modal may have an input; try common selectors
            pwd_sel = 'input[type="password"]'
            try:
                await page.wait_for_selector(pwd_sel, timeout=5000)
                await page.fill(pwd_sel, EDIT_PASSWORD)
                # try pressing Enter, then click a confirm button
                await page.keyboard.press("Enter")
            except PwTimeout:
                # maybe prompt() — fall back to HTTP login
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(f"{BASE_URL}/api/system/edit-auth/login", json={"password": EDIT_PASSWORD})
                results["warnings"].append("used HTTP login fallback for edit auth")
            # verify login chip
            await page.wait_for_timeout(800)
            chip_txt = await page.text_content("#edit-auth-status-chip") or ""
            print(f"[INFO] edit-auth chip: {chip_txt!r}")

            # Also set cookie in playwright from HTTP client if needed; easiest: do HTTP login via page
            await page.evaluate(
                """async (pwd) => {
                  await fetch('/api/system/edit-auth/login', {
                    method: 'POST',
                    headers: {'Content-Type':'application/json'},
                    body: JSON.stringify({password: pwd})
                  });
                }""",
                EDIT_PASSWORD,
            )

            # ---- select the target order's checkbox ----
            await page.wait_for_selector(f'.row-check[data-order-id="{oid}"]', timeout=15000)
            is_checked = await page.is_checked(f'.row-check[data-order-id="{oid}"]')
            if not is_checked:
                await page.check(f'.row-check[data-order-id="{oid}"]')
            # also pick one more order with draft to test running balance
            other_oids = [int(k) for k in (await page.evaluate("Object.keys(window._draftsByOrderId||{})"))] if False else []
            # simpler: pick 2 more draft order ids from HTTP data
            async with httpx.AsyncClient(timeout=10.0) as client:
                drafts_now = await fetch_drafts(client)
            extra = [int(k) for k in drafts_now.keys() if int(k) != oid][:2]
            for other in extra:
                try:
                    await page.check(f'.row-check[data-order-id="{other}"]')
                except Exception:
                    pass

            # ---- open batch merge ----
            await page.click("#btn-batch-merge")

            # wait for modal to appear
            await page.wait_for_selector("#shortage-modal", state="visible", timeout=30000)
            # wait for shortage items rendered
            try:
                await page.wait_for_selector("#shortage-modal .shortage-item[data-part]", timeout=15000)
            except PwTimeout:
                results["warnings"].append("no .shortage-item[data-part] found in modal — maybe no shortages to review")
            await page.wait_for_timeout(500)
            await page.screenshot(path=str(MODAL_PNG))
            print(f"[INFO] merge modal screenshot -> {MODAL_PNG}")

            # ---- Stage A: read every shortage card in modal ----
            modal_cards = await page.evaluate(
                """() => {
                  const out = [];
                  const cards = document.querySelectorAll('#shortage-modal .shortage-item[data-part]');
                  cards.forEach(card => {
                    const input = card.querySelector('.supplement-input');
                    const markCb = card.querySelector('.shortage-mark');
                    out.push({
                      part: card.dataset.part,
                      order_id: card.dataset.orderId ? Number(card.dataset.orderId) : null,
                      needed: Number(card.dataset.needed),
                      base_current_stock: Number(card.dataset.baseCurrentStock),
                      current_stock: Number(card.dataset.currentStock),
                      prev_qty_cs: Number(card.dataset.prevQtyCs || 0),
                      moq: Number(card.dataset.moq || 0),
                      supplement_input: input ? Number(input.value || 0) : null,
                      input_disabled: input ? input.disabled : null,
                      shortage_marked: markCb ? markCb.checked : false,
                    });
                  });
                  return out;
                }"""
            )
            results["stage_A_merge_modal"]["card_count"] = len(modal_cards)
            results["stage_A_merge_modal"]["cards_sample"] = modal_cards[:6]
            print(f"[STAGE-A] modal shortage-item cards: {len(modal_cards)}")
            for c in modal_cards[:6]:
                print(f"   part={c['part']} order={c['order_id']} "
                      f"needed={c['needed']} cur={c['current_stock']} "
                      f"base_cur={c['base_current_stock']} prev_cs={c['prev_qty_cs']} "
                      f"supp_input={c['supplement_input']}")

            # ---- Stage B: reopen modal and confirm numbers stable ----
            # close
            close_btn = await page.query_selector('#shortage-modal .close, #shortage-modal [data-action="close"], #shortage-modal button:has-text("關閉"), #shortage-modal button:has-text("取消")')
            if close_btn:
                try:
                    await close_btn.click()
                    await page.wait_for_selector("#shortage-modal", state="hidden", timeout=5000)
                except Exception:
                    pass
            # reopen
            await page.wait_for_timeout(400)
            try:
                await page.click("#btn-batch-merge")
                await page.wait_for_selector("#shortage-modal", state="visible", timeout=15000)
                await page.wait_for_selector("#shortage-modal .shortage-item[data-part]", timeout=10000)
            except Exception as e:
                results["warnings"].append(f"reopen modal failed: {e}")

            modal_cards_2 = await page.evaluate(
                """() => {
                  const out = [];
                  document.querySelectorAll('#shortage-modal .shortage-item[data-part]').forEach(card => {
                    const input = card.querySelector('.supplement-input');
                    out.push({
                      part: card.dataset.part,
                      order_id: card.dataset.orderId ? Number(card.dataset.orderId) : null,
                      needed: Number(card.dataset.needed),
                      current_stock: Number(card.dataset.currentStock),
                      supplement_input: input ? Number(input.value || 0) : null,
                    });
                  });
                  return out;
                }"""
            )
            # --- consistency: stage A vs reopen ---
            drift = []
            by_key = {(c["part"], c["order_id"]): c for c in modal_cards}
            for c2 in modal_cards_2:
                key = (c2["part"], c2["order_id"])
                a = by_key.get(key)
                if not a:
                    continue
                for f in ("needed", "current_stock", "supplement_input"):
                    if not _approx_equal(a.get(f), c2.get(f)):
                        drift.append({"key": key, "field": f, "before": a.get(f), "after": c2.get(f)})
            if drift:
                results["fail_items"].append({
                    "type": "modal_reopen_drift",
                    "drift_count": len(drift),
                    "samples": drift[:5],
                })
            else:
                results["pass_items"].append(f"Modal reopen: all {len(modal_cards_2)} cards unchanged")

            # ---- Stage D: running balance across duplicate part numbers ----
            # Group cards by part; if same part appears in multiple orders, later row
            # should have current_stock reduced by earlier supplement_input.
            by_part: Dict[str, List[Dict]] = {}
            for c in modal_cards:
                by_part.setdefault(c["part"], []).append(c)
            running_checks: List[Dict] = []
            for part, cards in by_part.items():
                if len(cards) < 2:
                    continue
                # they should already be in modal order; check adjacent
                for i in range(1, len(cards)):
                    prev, curr = cards[i - 1], cards[i]
                    expected = _round6(prev["current_stock"] + (prev["supplement_input"] or 0) - prev["needed"])
                    actual = _round6(curr["current_stock"])
                    running_checks.append({
                        "part": part,
                        "prev_order": prev["order_id"],
                        "curr_order": curr["order_id"],
                        "expected_curr_cur_stock": expected,
                        "actual_curr_cur_stock": actual,
                        "ok": _approx_equal(expected, actual, tol=0.01),
                    })
            bad_running = [r for r in running_checks if not r["ok"]]
            results["stage_A_merge_modal"]["running_balance_checks"] = len(running_checks)
            results["stage_A_merge_modal"]["running_balance_failures"] = len(bad_running)
            if running_checks:
                if bad_running:
                    results["fail_items"].append({
                        "type": "running_balance_drift",
                        "samples": bad_running[:5],
                    })
                    print(f"[STAGE-D] running balance FAIL: {len(bad_running)}/{len(running_checks)}")
                else:
                    results["pass_items"].append(
                        f"Running balance consistent across {len(running_checks)} shared-part pairs"
                    )
                    print(f"[STAGE-D] running balance PASS: {len(running_checks)}/{len(running_checks)}")
            else:
                results["warnings"].append("No cross-order duplicate parts to verify running balance")

            # Leave modal open and read one draft via API to cross-check
            async with httpx.AsyncClient(timeout=15.0) as client:
                drafts_after = await fetch_drafts(client)
            draft = drafts_after.get(str(oid)) or {}
            draft_supp = draft.get("supplements") or {}
            draft_short = draft.get("shortages") or []
            results["stage_B_draft_api"] = {
                "order_id": oid,
                "supplement_count": len(draft_supp),
                "shortage_count": len(draft_short),
                "shortages_sample": draft_short[:3],
            }
            print(f"[STAGE-B] draft api order={oid} supplements={len(draft_supp)} "
                  f"shortages={len(draft_short)}")

            # cross-check: every shortage in API should match modal card (for this order)
            modal_for_order = [c for c in modal_cards if c["order_id"] == oid]
            modal_parts = {c["part"]: c for c in modal_for_order}
            mismatches = []
            for s in draft_short:
                mc = modal_parts.get(s.get("part_number"))
                if not mc:
                    mismatches.append({"part": s.get("part_number"), "reason": "no matching modal card"})
                    continue
                for src, dst, key in (
                    ("needed", "needed", "needed"),
                    ("current_stock", "current_stock", "current_stock"),
                ):
                    if not _approx_equal(s.get(src), mc.get(dst), tol=0.01):
                        mismatches.append({"part": s.get("part_number"), "field": key,
                                           "draft_api": s.get(src), "modal_ui": mc.get(dst)})
            if mismatches:
                results["fail_items"].append({"type": "modal_vs_draftapi_mismatch", "samples": mismatches[:5]})
            else:
                results["pass_items"].append(f"Modal vs draft API agree for order {oid} ({len(draft_short)} shortages)")

            # ---- Stage E+F: close modal, then call main-write-preview API ----
            # This is exactly what the "寫入主檔" button triggers for preview
            try:
                esc_btn = await page.query_selector('#shortage-modal button:has-text("取消")')
                if esc_btn:
                    await esc_btn.click()
            except Exception:
                pass

            async with httpx.AsyncClient(timeout=30.0) as client:
                # send edit-auth cookie
                await client.post(f"{BASE_URL}/api/system/edit-auth/login", json={"password": EDIT_PASSWORD})
                preview_ids = [oid] + extra
                pv = await client.post(
                    f"{BASE_URL}/api/schedule/main-write-preview",
                    json={"order_ids": preview_ids},
                )
                if pv.status_code == 200:
                    pv_data = pv.json()
                else:
                    pv_data = {"error": pv.status_code, "body": pv.text[:400]}
            results["stage_C_main_write_preview"]["status"] = pv.status_code
            results["stage_C_main_write_preview"]["keys"] = list(pv_data.keys())[:15] if isinstance(pv_data, dict) else None

            # Extract rows/shortages from preview
            preview_rows = []
            if isinstance(pv_data, dict):
                for maybe_key in ("rows", "plan", "previews", "order_previews", "items"):
                    v = pv_data.get(maybe_key)
                    if isinstance(v, list) and v:
                        preview_rows = v
                        break
            results["stage_C_main_write_preview"]["preview_row_count"] = len(preview_rows)
            results["stage_C_main_write_preview"]["preview_sample"] = preview_rows[:1] if preview_rows else pv_data

            # Cross-check: preview shortages for target order vs draft api shortages
            pv_short = []
            if isinstance(pv_data, dict):
                # try common keys
                for maybe_key in ("shortages", "all_shortages", "aggregate_shortages"):
                    v = pv_data.get(maybe_key)
                    if isinstance(v, list):
                        pv_short = v
                        break
                if not pv_short and preview_rows:
                    for row in preview_rows:
                        if row.get("order_id") == oid and isinstance(row.get("shortages"), list):
                            pv_short = row["shortages"]
                            break
            results["stage_C_main_write_preview"]["pv_shortage_count"] = len(pv_short)

            # Compare: draft shortage supplement_qty vs preview supplement_qty (same part)
            cross = []
            draft_short_by_part = {s.get("part_number"): s for s in draft_short}
            for s in pv_short:
                part = s.get("part_number")
                ds = draft_short_by_part.get(part)
                if not ds:
                    continue
                for key in ("supplement_qty", "shortage_amount", "resulting_stock"):
                    if key in s and key in ds and not _approx_equal(s.get(key), ds.get(key), tol=0.01):
                        cross.append({"part": part, "field": key,
                                      "draft": ds.get(key), "preview": s.get(key)})
            if cross:
                results["fail_items"].append({"type": "draft_vs_preview_mismatch", "samples": cross[:5]})
            elif pv_short:
                results["pass_items"].append(
                    f"Draft vs main-write-preview agree on {len(pv_short)} shortages for order {oid}"
                )
            else:
                results["warnings"].append("main-write-preview returned no shortage rows to compare")

            await page.screenshot(path=str(MAIN_WRITE_PNG))

        finally:
            await context.close()
            await browser.close()

    # ---- report ---------------------------------------------------------------
    print("\n================== QA REPORT ==================")
    print(json.dumps(
        {
            "server_version": results["server_version"],
            "target": results["target_order"],
            "stage_A_card_count": results["stage_A_merge_modal"].get("card_count"),
            "stage_A_running_checks": results["stage_A_merge_modal"].get("running_balance_checks"),
            "stage_A_running_failures": results["stage_A_merge_modal"].get("running_balance_failures"),
            "stage_B_shortage_count": results["stage_B_draft_api"].get("shortage_count"),
            "stage_C_status": results["stage_C_main_write_preview"].get("status"),
            "stage_C_keys": results["stage_C_main_write_preview"].get("keys"),
            "stage_C_preview_row_count": results["stage_C_main_write_preview"].get("preview_row_count"),
            "stage_C_pv_shortage_count": results["stage_C_main_write_preview"].get("pv_shortage_count"),
        },
        ensure_ascii=False, indent=2,
    ))
    print("\n---- PASS ----")
    for p in results["pass_items"]:
        print(" -", p)
    print("\n---- FAIL ----")
    for f in results["fail_items"]:
        print(" -", json.dumps(f, ensure_ascii=False))
    print("\n---- WARNINGS ----")
    for w in results["warnings"]:
        print(" -", w)
    # also dump stage-A sample
    print("\n---- Stage A sample (first 6 cards) ----")
    for c in results["stage_A_merge_modal"].get("cards_sample", []):
        print(f"   part={c['part']:<14} order={c['order_id']} "
              f"needed={c['needed']:<8} cur={c['current_stock']:<8} "
              f"supp={c['supplement_input']}")
    print("\n---- Stage C preview sample ----")
    sample = results["stage_C_main_write_preview"].get("preview_sample")
    print(json.dumps(sample, ensure_ascii=False, indent=2, default=str)[:800])


if __name__ == "__main__":
    asyncio.run(run_qa())
