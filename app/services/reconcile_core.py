"""ST 對帳核心：以 audit log 計算截止日理論庫存。"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from app import database as db
from app.constants import ST_RECONCILE_ADJUSTMENT_REASON


def _normalize_parts(part_numbers: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    parts = [
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ]
    return list(dict.fromkeys(parts))


def _normalize_anchor(anchor: dict | None) -> tuple[str, dict[str, float]]:
    if not anchor:
        return "", {}
    aligned_at = str(anchor.get("aligned_at") or "").strip()
    raw_baseline = anchor.get("baseline_qty") or anchor.get("baseline_qty_by_part") or {}
    baseline = {
        str(part).strip().upper(): float(qty or 0)
        for part, qty in raw_baseline.items()
        if str(part).strip()
    }
    return aligned_at, baseline


def _candidate_parts(
    part_numbers: list[str],
    baseline_by_part: dict[str, float],
    upload_baselines: dict[str, dict],
    delta_rows: list[dict],
) -> list[str]:
    if part_numbers:
        return part_numbers
    parts = set(baseline_by_part) | set(upload_baselines)
    parts.update(str(row.get("part_number") or "").strip().upper() for row in delta_rows)
    return sorted(part for part in parts if part)


def theoretical_stock(
    cutoff_T: str,
    anchor: dict | None = None,
    part_numbers: list[str] | tuple[str, ...] | set[str] | None = None,
) -> dict[str, float]:
    """計算截止日 T 的 ST 理論庫存。

    anchor 可傳 None；None 時每個料號會以 cutoff 前最近一次
    st_inventory_upload 的 new_qty 作為 baseline，沒有上傳紀錄則從 0 起算。
    """
    cutoff = str(cutoff_T or "").strip()
    if not cutoff:
        return {}

    parts = _normalize_parts(part_numbers)
    explicit_anchor_at, explicit_anchor_baseline = _normalize_anchor(anchor)
    if anchor is None:
        anchors_by_part = db.get_latest_st_reconcile_anchors(cutoff, parts or None)
    else:
        anchors_by_part = {
            part: {
                "aligned_at": explicit_anchor_at,
                "baseline_qty": qty,
            }
            for part, qty in explicit_anchor_baseline.items()
        }
    upload_baselines = db.get_st_inventory_upload_baselines(cutoff, parts or None)
    delta_rows = db.get_st_inventory_audit_deltas(
        cutoff,
        part_numbers=parts or None,
        exclude_reason=ST_RECONCILE_ADJUSTMENT_REASON,
    )

    anchor_baseline = {
        part: float((values or {}).get("baseline_qty") or 0)
        for part, values in anchors_by_part.items()
    }
    result: dict[str, float] = {}
    baseline_at_by_part: dict[str, str] = {}
    for part in _candidate_parts(parts, anchor_baseline, upload_baselines, delta_rows):
        part_anchor = anchors_by_part.get(part) or {}
        if part_anchor:
            result[part] = float(part_anchor.get("baseline_qty") or 0.0)
            baseline_at_by_part[part] = str(part_anchor.get("aligned_at") or "")
        else:
            upload = upload_baselines.get(part) or {}
            result[part] = float(upload.get("baseline_qty") or 0.0)
            baseline_at_by_part[part] = str(upload.get("aligned_at") or "")

    for row in delta_rows:
        part = str(row.get("part_number") or "").strip().upper()
        if not part:
            continue
        baseline_at = baseline_at_by_part.get(part, "")
        if baseline_at and str(row.get("changed_at") or "") <= baseline_at:
            continue
        if parts and part not in result:
            continue
        result[part] = round(float(result.get(part, 0.0)) + float(row.get("delta") or 0), 6)

    return result


def theoretical_stock_with_details(
    cutoff_T: str,
    anchor: dict | None = None,
    part_numbers: list[str] | tuple[str, ...] | set[str] | None = None,
) -> dict[str, Any]:
    """回傳理論庫存與截止日有效的訂單級 ST 消耗明細。"""
    stock = theoretical_stock(cutoff_T, anchor=anchor, part_numbers=part_numbers)
    requested_parts = _normalize_parts(part_numbers)
    consumption_rows = db.get_st_dispatch_consumptions_as_of(
        str(cutoff_T or "").strip(),
        requested_parts or None,
    )

    by_part: dict[str, list[dict]] = defaultdict(list)
    for row in consumption_rows:
        part = str(row.get("part_number") or "").strip().upper()
        if not part:
            continue
        by_part[part].append(row)
        stock.setdefault(part, 0.0)

    return {
        "stock": stock,
        "order_details": dict(by_part),
    }
