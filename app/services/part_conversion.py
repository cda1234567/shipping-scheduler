"""轉換料 A→B：把本網站所有 BOM 裡的料號 A 換成 B，可預覽、可還原。

- 只改本網站自己的資料（bom_files / bom_components 與 BOM xlsx 檔），完全不碰正航 / ERP。
- B 已存在於同一份 BOM 時：每板用量相加、需求量相加，A 那列清空（xlsx）/ 移除（資料庫），預覽中標示「合併」。
- 確認時先把受影響 BOM 的元件與 xlsx 備份存成快照（part_conversions.snapshot_json），
  資料庫部分在單一 transaction 內完成；任何一份 BOM 失敗，已改動的 xlsx 一律從備份還原。
- 轉換後 A 記入 part_conversion_active：缺料採購不再列出 A，A 的需求與庫存併入 B
  （B 有效庫存 = B 庫存 + A 庫存，A 可為負數）。
- .xls 或檔案不存在的 BOM 只更新資料庫元件（file_mode = db_only），預覽會標示。
"""
from __future__ import annotations

import hashlib
import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import openpyxl

from .. import database as db
from ..config import cfg
from . import bom_editor
from .bom_revision import ensure_bom_revision_history, snapshot_bom_revision

logger = logging.getLogger(__name__)

_COMPONENT_KEYS = (
    "part_number", "description", "qty_per_board", "scrap_factor", "needed_qty",
    "prev_qty_cs", "is_dash", "is_customer_supplied",
)


class ConversionError(ValueError):
    """轉換 / 還原的使用者可理解錯誤（API 轉成 400 / 409）。"""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def norm(value: Any) -> str:
    return str(value or "").strip().upper()


# ── 生效中的轉換（缺料計算使用）──────────────────────────────────────────────

def active_map() -> Dict[str, str]:
    """{A: B}；讀取失敗時視為沒有轉換，不影響缺料清單。"""
    try:
        return db.get_active_part_conversions()
    except Exception as exc:
        logger.warning("讀取轉換料失敗：%s", type(exc).__name__)
        return {}


def resolve(part: str, mapping: Dict[str, str]) -> str:
    """沿著 A→B→C 找到最終料號（有環時停在第一次重複處）。"""
    key = norm(part)
    seen = {key}
    while key in mapping:
        nxt = norm(mapping[key])
        if not nxt or nxt in seen:
            break
        seen.add(nxt)
        key = nxt
    return key


def sources_by_target(mapping: Dict[str, str]) -> Dict[str, List[str]]:
    """{最終料號: [所有併入它的舊料號]}，依舊料號排序。"""
    out: Dict[str, List[str]] = {}
    for a in mapping:
        out.setdefault(resolve(a, mapping), []).append(norm(a))
    return {k: sorted(v) for k, v in out.items()}


def effective_stock(
    part: str, st_stock: Dict[str, float], sources: Dict[str, List[str]]
) -> Tuple[float, float, List[Dict[str, Any]]]:
    """(有效庫存, 自己的庫存, merged_from)；有效庫存 = 自己 + 所有併入料號（可為負）。"""
    key = norm(part)
    own = float(st_stock.get(key, 0.0) or 0.0)
    merged = [{"part": a, "stock": float(st_stock.get(a, 0.0) or 0.0)} for a in sources.get(key, [])]
    return own + sum(m["stock"] for m in merged), own, merged


# ── 純函式：元件轉換 ─────────────────────────────────────────────────────────

def transform_components(components: List[Dict[str, Any]], part_a: str, part_b: str) -> Tuple[List[Dict[str, Any]], bool]:
    """回傳 (轉換後元件, 是否合併進既有 B)。不修改傳入的清單。

    同一份 BOM 已有 B：第一個 B 列加上所有 A 列的每板用量與需求量，A 列移除。
    沒有 B：A 列直接改成 B 料號（其餘欄位不變）。
    """
    a, b = norm(part_a), norm(part_b)
    out = [dict(c) for c in components]
    a_rows = [c for c in out if norm(c.get("part_number")) == a]
    if not a_rows:
        return out, False
    b_row = next((c for c in out if norm(c.get("part_number")) == b), None)
    if b_row is None:
        for c in a_rows:
            c["part_number"] = b
        return out, False
    for c in a_rows:
        b_row["qty_per_board"] = float(b_row.get("qty_per_board") or 0) + float(c.get("qty_per_board") or 0)
        b_row["needed_qty"] = float(b_row.get("needed_qty") or 0) + float(c.get("needed_qty") or 0)
    return [c for c in out if norm(c.get("part_number")) != a], True


def _clean_component(c: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "part_number": str(c.get("part_number") or ""),
        "description": str(c.get("description") or ""),
        "qty_per_board": float(c.get("qty_per_board") or 0),
        "scrap_factor": float(c.get("scrap_factor") or 0),
        "needed_qty": float(c.get("needed_qty") or 0),
        "prev_qty_cs": float(c.get("prev_qty_cs") or 0),
        "is_dash": bool(c.get("is_dash")),
        "is_customer_supplied": bool(c.get("is_customer_supplied")),
    }


def components_signature(components: List[Dict[str, Any]]) -> str:
    """元件清單內容指紋（與順序 / 列 id 無關），用於還原前確認 BOM 沒有被改過。"""
    rows = sorted(
        (norm(c.get("part_number")), round(float(c.get("qty_per_board") or 0), 6),
         round(float(c.get("needed_qty") or 0), 6))
        for c in components
    )
    return hashlib.sha1(json.dumps(rows).encode("utf-8")).hexdigest()


# ── 預覽 ─────────────────────────────────────────────────────────────────────

def _validate_pair(part_a: str, part_b: str, mapping: Dict[str, str]) -> Tuple[str, str]:
    a, b = norm(part_a), norm(part_b)
    if not a or not b:
        raise ConversionError("請輸入料號 A 與料號 B")
    if a == b:
        raise ConversionError("料號 A 與 B 不可相同")
    if a in mapping:
        raise ConversionError(f"{a} 已轉換為 {mapping[a]}，請先還原再重新轉換")
    if b in mapping:
        raise ConversionError(f"{b} 已被停用（已轉換為 {mapping[b]}），不可作為轉換目標")
    return a, b


def _bom_label(bom: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "bom_id": bom["id"],
        "filename": bom.get("filename") or "",
        "model": bom.get("model") or "",
        "group_model": bom.get("group_model") or "",
        "po_number": bom.get("po_number") or "",
    }


def _file_mode(bom: Dict[str, Any]) -> str:
    path = Path(str(bom.get("filepath") or ""))
    return "xlsx" if path.suffix.lower() in (".xlsx", ".xlsm") and path.exists() else "db_only"


def _stock_info(parts: List[str]) -> Dict[str, Dict[str, Any]]:
    st = {norm(p): float(q or 0) for p, q in db.get_st_inventory_stock().items()}
    with db.get_conn() as conn:
        marks = ",".join("?" * len(parts))
        snap = {
            norm(r["part_number"]): float(r["stock_qty"] or 0)
            for r in conn.execute(
                f"SELECT part_number, stock_qty FROM inventory_snapshot WHERE UPPER(part_number) IN ({marks})",
                parts,
            ).fetchall()
        }
    return {p: {"st_stock": st.get(p, 0.0), "main_snapshot_stock": snap.get(p)} for p in parts}


def preview(part_a: str, part_b: str) -> Dict[str, Any]:
    """唯讀 dry-run：列出所有使用 A 的 BOM（含合併標記）；不改任何資料。"""
    mapping = active_map()
    a, b = _validate_pair(part_a, part_b, mapping)
    affected: List[Dict[str, Any]] = []
    for bom in db.get_bom_files():
        comps = db.get_bom_components(bom["id"])
        a_rows = [c for c in comps if norm(c["part_number"]) == a]
        if not a_rows:
            continue
        b_rows = [c for c in comps if norm(c["part_number"]) == b]
        qty_a = sum(float(c["qty_per_board"] or 0) for c in a_rows)
        qty_b = sum(float(c["qty_per_board"] or 0) for c in b_rows)
        affected.append({
            **_bom_label(bom),
            "file_mode": _file_mode(bom),
            "rows": len(a_rows),
            "qty_per_board": qty_a,
            "needed_qty": sum(float(c["needed_qty"] or 0) for c in a_rows),
            "description": a_rows[0].get("description") or "",
            "merge": bool(b_rows),
            "b_qty_per_board_before": qty_b,
            "qty_per_board_after": qty_a + qty_b if b_rows else qty_a,
        })
    stock = _stock_info([a, b])
    return {
        "part_a": a,
        "part_b": b,
        "count": len(affected),
        "merge_count": sum(1 for x in affected if x["merge"]),
        "db_only_count": sum(1 for x in affected if x["file_mode"] == "db_only"),
        "affected": affected,
        "stock": stock,
        "effective_stock_b": stock[a]["st_stock"] + stock[b]["st_stock"],
        "warnings": ([] if affected else [f"本網站的 BOM 中找不到料號 {a}"]),
    }


# ── xlsx 編輯 ────────────────────────────────────────────────────────────────

def _cell_number(ws, row: int, col: int, label: str) -> float:
    cell = bom_editor._resolve_cell_for_write(ws, row, col)
    if bom_editor._is_formula_excel_cell(cell):
        raise ConversionError(f"{label} 第 {row} 列的用量為公式，無法自動合併，請先手動調整")
    try:
        return float(cell.value or 0)
    except (TypeError, ValueError):
        raise ConversionError(f"{label} 第 {row} 列的用量不是數字") from None


def _edit_workbook(path: str, a: str, b: str, a_needed: float, label: str) -> None:
    """就地修改 xlsx：沒有 B 就把 A 列料號改成 B；有 B 就把用量併入 B、清空 A 列。"""
    suffix = Path(path).suffix.lower()
    wb = openpyxl.load_workbook(path, keep_vba=(suffix == ".xlsm"))
    try:
        ws = wb.worksheets[0]
        part_col = cfg("excel.bom_part_col", 2) + 1
        desc_col = cfg("excel.bom_desc_col", 3) + 1
        qty_col = cfg("excel.bom_qty_per_board", 1) + 1
        needed_col = cfg("excel.bom_needed_col", 5) + 1
        start = cfg("excel.bom_data_start_row", 5)
        rows_a, rows_b = [], []
        for r in range(start, ws.max_row + 1):
            value = norm(bom_editor._get_cell_value(ws, r, part_col))
            if value == a:
                rows_a.append(r)
            elif value == b:
                rows_b.append(r)
        if not rows_a:
            raise ConversionError(f"{label} 的檔案中找不到 {a}（資料庫與檔案不一致）")
        if not rows_b:
            for r in rows_a:
                bom_editor._set_cell_value(ws, r, part_col, b)
        else:
            target = rows_b[0]
            qty = _cell_number(ws, target, qty_col, label)
            needed_cell = bom_editor._resolve_cell_for_write(ws, target, needed_col)
            for r in rows_a:
                qty += _cell_number(ws, r, qty_col, label)
                bom_editor._set_cell_value(ws, r, part_col, None)
                bom_editor._set_cell_value(ws, r, desc_col, None)
                bom_editor._set_cell_value(ws, r, qty_col, 0)
                if not bom_editor._is_formula_excel_cell(bom_editor._resolve_cell_for_write(ws, r, needed_col)):
                    bom_editor._set_cell_value(ws, r, needed_col, 0)
            bom_editor._set_cell_value(ws, target, qty_col, qty)
            if not bom_editor._is_formula_excel_cell(needed_cell):
                needed_cell.value = float(needed_cell.value or 0) + a_needed
        bom_editor.mark_workbook_for_recalc(wb)
        wb.save(path)
    finally:
        wb.close()
        if getattr(wb, "vba_archive", None) is not None:
            wb.vba_archive.close()


def _reparse(bom: Dict[str, Any]) -> List[Dict[str, Any]]:
    parsed = bom_editor.parse_bom_for_storage(
        path=str(bom["filepath"]),
        bom_id=bom["id"],
        filename=bom["filename"],
        uploaded_at=bom.get("uploaded_at") or "",
        group_model=bom.get("group_model") or "",
        source_filename=bom.get("source_filename") or "",
        source_format=bom.get("source_format") or "",
        is_converted=bool(bom.get("is_converted")),
    )
    return [_clean_component(c.dict()) for c in parsed.components]


def _restore_files(backups: List[Tuple[str, str]]) -> None:
    for filepath, backup in backups:
        try:
            shutil.copy2(backup, filepath)
        except OSError as exc:
            logger.error("還原 BOM 檔案失敗 %s：%s", filepath, type(exc).__name__)


# ── 確認轉換 ─────────────────────────────────────────────────────────────────

def convert(part_a: str, part_b: str, actor: str = "web", dry_run: bool = False) -> Dict[str, Any]:
    """執行轉換。dry_run=True 只回傳預覽，不改任何資料。"""
    info = preview(part_a, part_b)
    if dry_run:
        return {**info, "dry_run": True}
    if not info["affected"]:
        raise ConversionError(f"本網站的 BOM 中找不到料號 {info['part_a']}，無需轉換")
    a, b = info["part_a"], info["part_b"]
    boms = {x["bom_id"]: db.get_bom_file(x["bom_id"]) for x in info["affected"]}

    backups: List[Tuple[str, str]] = []
    snapshot_boms: List[Dict[str, Any]] = []
    states: List[Dict[str, Any]] = []
    try:
        for entry in info["affected"]:
            bom = boms[entry["bom_id"]]
            before = [_clean_component(c) for c in db.get_bom_components(bom["id"])]
            mode = entry["file_mode"]
            backup_path = ""
            if mode == "xlsx":
                try:
                    ensure_bom_revision_history(bom)
                except Exception as exc:  # 歷史版本只是輔助，失敗不擋轉換
                    logger.warning("BOM 歷史版本建立失敗：%s", type(exc).__name__)
                backup_path = bom_editor.backup_bom_file(bom["filepath"])
                backups.append((bom["filepath"], backup_path))
                a_needed = sum(float(c["needed_qty"] or 0) for c in before if norm(c["part_number"]) == a)
                _edit_workbook(bom["filepath"], a, b, a_needed, bom.get("filename") or bom["id"])
                after = _reparse(bom)
            else:
                after, _ = transform_components(before, a, b)
            snapshot_boms.append({
                "bom_id": bom["id"], "filepath": bom.get("filepath") or "", "backup_path": backup_path,
                "file_mode": mode, "components_before": before,
                "after_signature": components_signature(after),
            })
            states.append({"bom_id": bom["id"], "components": after})
        conversion_id = db.apply_part_conversion(
            a, b, states, {"boms": snapshot_boms},
            [{k: x[k] for k in ("bom_id", "filename", "model", "group_model", "merge", "file_mode",
                                 "qty_per_board", "needed_qty")} for x in info["affected"]],
            actor,
        )
    except Exception:
        _restore_files(backups)
        raise
    for entry in info["affected"]:
        bom = db.get_bom_file(entry["bom_id"])
        if entry["file_mode"] == "xlsx" and bom:
            try:
                snapshot_bom_revision(bom, "part_convert", f"轉換料 {a} → {b}")
            except Exception as exc:
                logger.warning("BOM 歷史版本建立失敗：%s", type(exc).__name__)
    return {**info, "dry_run": False, "conversion_id": conversion_id}


# ── 還原 ─────────────────────────────────────────────────────────────────────

def restore(conversion_id: int, actor: str = "web", force: bool = False) -> Dict[str, Any]:
    conv = db.get_part_conversion(int(conversion_id))
    if not conv:
        raise ConversionError("找不到此轉換紀錄", 404)
    if conv["status"] != "active":
        raise ConversionError("此轉換已經還原過了", 409)
    snap_boms = (conv.get("snapshot") or {}).get("boms") or []
    bom_ids = {s["bom_id"] for s in snap_boms}

    # 後做的轉換若動過同一批 BOM，必須先還原後面的（否則會蓋掉它的結果）
    for other in db.list_part_conversions(200, with_snapshot=True):
        if other["id"] > conv["id"] and other["status"] == "active":
            other_ids = {s["bom_id"] for s in (other.get("snapshot") or {}).get("boms") or []}
            if bom_ids & other_ids:
                raise ConversionError(
                    f"較新的轉換 #{other['id']}（{other['part_a']} → {other['part_b']}）也改了相同的 BOM，請先還原它", 409
                )

    # 轉換之後 BOM 若又被編輯過，直接還原會蓋掉那些修改
    changed = []
    for s in snap_boms:
        current = [_clean_component(c) for c in db.get_bom_components(s["bom_id"])]
        if components_signature(current) != s.get("after_signature"):
            bom = db.get_bom_file(s["bom_id"]) or {}
            changed.append(bom.get("filename") or s["bom_id"])
    if changed and not force:
        raise ConversionError("轉換後這些 BOM 又被修改過，還原會覆蓋修改：" + "、".join(changed), 409)

    stash_dir = Path(tempfile.mkdtemp(prefix="part_convert_restore_"))
    stashed: List[Tuple[str, str]] = []
    warnings: List[str] = []
    try:
        for i, s in enumerate(snap_boms):
            backup = Path(str(s.get("backup_path") or ""))
            target = Path(str(s.get("filepath") or ""))
            if s.get("file_mode") != "xlsx":
                continue
            if not backup.exists() or not str(target):
                warnings.append(f"找不到 {target.name or s['bom_id']} 的備份檔，只還原資料庫內容")
                continue
            if target.exists():
                stash = stash_dir / f"{i}{target.suffix}"
                shutil.copy2(target, stash)
                stashed.append((str(target), str(stash)))
            shutil.copy2(backup, target)
        try:
            db.restore_part_conversion(
                conv["id"], {s["bom_id"]: s["components_before"] for s in snap_boms}, actor
            )
        except Exception:
            _restore_files(stashed)
            raise
    finally:
        shutil.rmtree(stash_dir, ignore_errors=True)
    for s in snap_boms:
        bom = db.get_bom_file(s["bom_id"])
        if s.get("file_mode") == "xlsx" and bom:
            try:
                snapshot_bom_revision(bom, "part_convert_restore", f"還原轉換料 {conv['part_a']} → {conv['part_b']}")
            except Exception as exc:
                logger.warning("BOM 歷史版本建立失敗：%s", type(exc).__name__)
    return {"ok": True, "conversion_id": conv["id"], "part_a": conv["part_a"], "part_b": conv["part_b"],
            "restored_boms": len(snap_boms), "warnings": warnings}


def list_conversions() -> Dict[str, Any]:
    return {"conversions": db.list_part_conversions(50), "active": db.get_active_part_conversions()}
