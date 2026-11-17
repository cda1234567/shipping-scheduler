"""已發料單改批次代號：orders.code 與主檔第 1 列批次欄標頭一起改（例：12-13 → 12-7）。

批次代號同時是主檔三欄組（批次｜PO｜機種）的批次欄標頭；副檔重抓、發料單、精準退回、
主檔預覽封存比對都用 orders.code 對主檔標頭，所以只改 DB 會對不上，必須兩邊一起改。

安全規則（任一不符就 409，主檔與資料庫都不動）：
- 只改 status=dispatched、未封存、不是插單的單。
- 主檔欄位：標頭＝舊代號且右側 PO 標頭屬於這張單（規則同 dispatch._cols_for_order：只看數字，
  完全相同或標頭 ≥5 碼且為 PO 結尾）。同一單多組（A/B 板）全部改；找不到、或同欄也可能屬於
  別張同代號同 PO 的已發料單、或跟這張單的發料欄位紀錄對不起來 → 409。
- 一鍵調撥用「年/批次代號」當批次 key，已調撥的批次改代號會被當成未調撥（重複調撥），
  改代號跨過調撥截止批次會讓待調撥算錯 → 409。
- 有待插入的單以舊／新代號當插入位置 → 409。
- 新代號已被別張未封存訂單（不同 PO）使用 → 409（force=True 才改；跨年同代號是正常的）。
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

import openpyxl
from openpyxl.utils import get_column_letter

from .. import database as db
from ..config import BACKUP_DIR
from .dispatch_precise_rollback import norm_header
from .local_time import local_now
from .main_file_lock import serialized_main_file_write

CODE_CHANGE_LOG_ACTION = "completed_code_changed"
_CODE_RE = re.compile(r"^[0-9]+-[0-9]+$")


class CompletedCodeEditError(Exception):
    def __init__(self, status: int, message: str, *, code: str = "", extra: dict | None = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.code = code
        self.extra = extra or {}

    @property
    def detail(self):
        if not self.code:
            return self.message
        return {"code": self.code, "message": self.message, **self.extra}


def normalize_batch_code(value) -> str:
    return str(value or "").strip()


def is_valid_batch_code(value) -> bool:
    return bool(_CODE_RE.fullmatch(normalize_batch_code(value)))


def _digits(value) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def po_header_matches(header_po, order_po) -> bool:
    """批次欄右側 PO 標頭是否屬於這張訂單（同 dispatch._cols_for_order）。"""
    header = _digits(header_po)
    po = _digits(order_po)
    if not header or not po:
        return False
    return header == po or (len(header) >= 5 and po.endswith(header))


def locate_batch_columns(ws, code: str, po_number: str) -> list[int]:
    """主檔第 1 列：標頭＝code 且右側 PO 標頭屬於 po_number 的批次欄（1-based）。"""
    code = normalize_batch_code(code)
    if not code:
        return []
    max_col = ws.max_column
    return [
        col for col in range(1, max_col + 1)
        if norm_header(ws.cell(1, col).value) == code
        and po_header_matches(ws.cell(1, col + 1).value, po_number)
    ]


def _find_duplicate_orders(order: dict, new_code: str) -> list[dict]:
    """別張未封存訂單已用同一代號（同 PO 的另一組不算）。"""
    po = _digits(order.get("po_number"))
    duplicates = []
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT id, code, po_number, model, status FROM orders "
            "WHERE archived_at='' AND id<>? AND TRIM(IFNULL(code,''))=?",
            (int(order["id"]), new_code),
        ).fetchall()
    for row in rows:
        if po and _digits(row["po_number"]) == po:
            continue
        duplicates.append({
            "id": int(row["id"]),
            "po_number": str(row["po_number"] or ""),
            "model": str(row["model"] or ""),
            "status": str(row["status"] or ""),
        })
    return duplicates


def _find_competing_owners(order: dict, ws, columns: list[int], old_code: str) -> list[dict]:
    """同代號、PO 標頭也對得上的別張已發料單（含封存）——欄位無法確定屬於哪一張。"""
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT id, po_number, model, archived_at FROM orders "
            "WHERE id<>? AND TRIM(IFNULL(code,''))=? AND status IN ('dispatched','completed')",
            (int(order["id"]), old_code),
        ).fetchall()
    owners = []
    for row in rows:
        if any(po_header_matches(ws.cell(1, col + 1).value, row["po_number"]) for col in columns):
            owners.append({"id": int(row["id"]), "po_number": str(row["po_number"] or ""),
                           "model": str(row["model"] or ""), "archived": bool(row["archived_at"])})
    return owners


def _check_against_dispatch_record(order: dict, ws, columns: list[int]) -> str:
    """有發料欄位紀錄時，主檔找到的組數與 PO／機種要和紀錄一致；回傳錯誤訊息（空字串＝OK）。"""
    session = db.get_active_dispatch_session(int(order["id"]))
    if not session:
        return ""
    record = db.get_dispatch_column_record(int(session["id"]))
    groups = (record or {}).get("groups") or []
    if not groups:
        return ""
    expected = Counter((_digits(g.get("po_number")), norm_header(g.get("bom_model"))) for g in groups)
    actual = Counter((_digits(ws.cell(1, col + 1).value), norm_header(ws.cell(1, col + 2).value)) for col in columns)
    if expected == actual:
        return ""
    return (f"主檔找到 {len(columns)} 組批次欄，和這張單的發料紀錄（{len(groups)} 組："
            + "、".join(sorted({norm_header(g.get('bom_model')) for g in groups}))
            + "）對不起來，無法確定哪幾欄屬於這張單")


def _pending_insert_conflicts(old_code: str, new_code: str) -> list[dict]:
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT id, po_number, model, insert_before_code FROM orders "
            "WHERE archived_at='' AND status IN ('pending','merged') AND TRIM(IFNULL(insert_before_code,'')) IN (?,?)",
            (old_code, new_code),
        ).fetchall()
    return [dict(row) for row in rows]


def _transfer_conflict(order: dict, old_code: str, new_code: str) -> str:
    """一鍵調撥 / 待調撥依「年/批次代號」分批；改代號會讓調撥紀錄或截止批次對不上就回傳原因。"""
    from . import st_pending_transfer as stp

    order_id = int(order["id"])
    orders = stp._load_orders()
    me = next((item for item in orders if int(item["id"]) == order_id), None)
    if me is None:
        return ""
    anchor = stp._parse_dt(me.get("first_dispatched_at")) or stp._parse_dt(me.get("created_at"))
    old_key = stp.batch_key(old_code, anchor)
    new_key = stp.batch_key(new_code, anchor)
    if not old_key or not new_key:
        return ""

    with db.get_conn() as conn:
        records = [dict(r) for r in conn.execute(
            "SELECT batch_key, move_no, target_db FROM erp_transfers WHERE status='ok'"
        ).fetchall()]
    for key, label in ((old_key, old_code), (new_key, new_code)):
        text = stp.key_text(key)
        hit = next((rec for rec in records if str(rec.get("batch_key") or "") == text), None)
        if hit:
            return (f"批次 {label}（{text}）已有一鍵調撥紀錄（{hit.get('move_no') or '無單號'}，{hit.get('target_db')}），"
                    "改代號會讓調撥紀錄對不上、可能重複調撥或漏調撥")

    batches = stp.list_batches(orders)
    cutoffs = []
    try:
        cutoffs.append(stp.effective_cutoff(batches).get("batch"))
    except Exception:
        pass
    cutoffs.append(stp.resolve_cutoff(stp.get_cutoff_setting(), batches))
    for cut in cutoffs:
        if not cut:
            continue
        cut_key = stp.parse_key(cut.get("key"))
        if not cut_key:
            continue
        if old_key == cut_key and list(cut.get("order_ids") or []) == [order_id]:
            return f"批次 {old_code} 是目前的調撥截止批次，改代號後截止批次會不見"
        if (old_key <= cut_key) != (new_key <= cut_key):
            return (f"改成 {new_code} 會跨過調撥截止批次 {cut.get('code')}，"
                    "待調撥 / 一鍵調撥會把這批的扣料算錯")
    return ""


def _conflict(message: str) -> CompletedCodeEditError:
    return CompletedCodeEditError(409, message + "，未做任何修改。")


def _restore_main(backup_path: str, main_path: str) -> None:
    from .st_reconcile import _restore_main_from_backup

    _restore_main_from_backup(backup_path, main_path)


@serialized_main_file_write
def change_completed_order_code(order_id: int, new_code, *, force: bool = False,
                                backup_dir: str | None = None) -> dict:
    order = db.get_order(int(order_id))
    if not order:
        raise CompletedCodeEditError(404, "找不到此訂單")
    new_code = normalize_batch_code(new_code)
    if not is_valid_batch_code(new_code):
        raise CompletedCodeEditError(400, "批次代號格式需為「數字-數字」，例如 12-7")
    old_code = normalize_batch_code(order.get("code"))
    if new_code == old_code:
        return {"ok": True, "unchanged": True, "order_id": int(order["id"]), "code": old_code, "columns": []}
    if order.get("status") != "dispatched":
        raise _conflict("只有「已發料」的單可以在這裡改批次代號")
    if order.get("archived_at"):
        raise _conflict("這張單已封存")
    if normalize_batch_code(order.get("insert_before_code")):
        raise _conflict("插入單據的編號須與插入位置一致，不能單獨修改")
    if not is_valid_batch_code(old_code):
        raise _conflict(f"目前批次代號「{old_code}」不是「數字-數字」格式，主檔無法對應")
    if not _digits(order.get("po_number")):
        raise _conflict("這張單沒有 PO，無法確定主檔哪幾欄屬於這張單")

    inserts = _pending_insert_conflicts(old_code, new_code)
    if inserts:
        names = "、".join(f"{item['po_number']} {item['model']}（插在 {item['insert_before_code']} 前）" for item in inserts)
        raise _conflict(f"有待發料的插單以 {old_code} / {new_code} 當插入位置：{names}，請先處理插單")

    transfer_reason = _transfer_conflict(order, old_code, new_code)
    if transfer_reason:
        raise _conflict(transfer_reason)

    duplicates = _find_duplicate_orders(order, new_code)
    if duplicates and not force:
        names = "、".join(f"{item['po_number']} {item['model']}" for item in duplicates)
        raise CompletedCodeEditError(
            409,
            f"批次代號 {new_code} 已被其他訂單使用：{names}。主檔可能有舊循環同名批次，確定仍要改嗎？",
            code="duplicate_code",
            extra={"duplicates": duplicates},
        )

    main_path = db.resolve_managed_path(str(db.get_setting("main_file_path") or "").strip(), "main_file_path")
    if not main_path or not Path(main_path).exists():
        raise _conflict("找不到主檔")
    is_xlsm = Path(main_path).suffix.lower() == ".xlsm"
    workbook = openpyxl.load_workbook(main_path, keep_vba=is_xlsm)
    try:
        ws = workbook.worksheets[0]
        columns = locate_batch_columns(ws, old_code, order.get("po_number"))
        if not columns:
            raise _conflict(f"主檔找不到批次 {old_code}（PO {order.get('po_number')}）的欄位")
        owners = _find_competing_owners(order, ws, columns, old_code)
        if owners:
            names = "、".join(f"#{item['id']} {item['po_number']} {item['model']}" for item in owners)
            raise _conflict(f"主檔批次 {old_code} 的欄位也可能屬於其他已發料單（{names}），無法確定哪幾欄屬於這張單")
        record_reason = _check_against_dispatch_record(order, ws, columns)
        if record_reason:
            raise _conflict(record_reason)

        column_info = [
            {
                "col": col,
                "po_header": norm_header(ws.cell(1, col + 1).value),
                "model_header": norm_header(ws.cell(1, col + 2).value),
            }
            for col in columns
        ]
        for col in columns:
            ws.cell(1, col).value = new_code

        from .merge_to_main import _save_workbook_atomically, backup_main_file

        backup_path = backup_main_file(main_path, str(backup_dir or BACKUP_DIR))
        changed_at = local_now().isoformat()
        main_touched = False
        try:
            with db.get_conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute("SELECT code, status, archived_at FROM orders WHERE id=?",
                                   (int(order["id"]),)).fetchone()
                if (not row or normalize_batch_code(row["code"]) != old_code
                        or row["status"] != "dispatched" or row["archived_at"]):
                    raise _conflict("訂單狀態或代號已變更，請重新整理")
                conn.execute("UPDATE orders SET code=?, updated_at=? WHERE id=?",
                             (new_code, changed_at, int(order["id"])))
                conn.execute(
                    "INSERT INTO order_code_changes(order_id, po_number, old_code, new_code, columns_json, "
                    "main_backup_path, changed_at) VALUES(?,?,?,?,?,?,?)",
                    (int(order["id"]), str(order.get("po_number") or ""), old_code, new_code,
                     json.dumps(column_info, ensure_ascii=False), backup_path, changed_at),
                )
                letters = "、".join(get_column_letter(col) for col in columns)
                conn.execute(
                    "INSERT INTO activity_logs(action, detail, created_at) VALUES(?,?,?)",
                    (CODE_CHANGE_LOG_ACTION,
                     f"已發料訂單 #{order['id']} PO {order.get('po_number')} ({order.get('model')}) 批次代號 "
                     f"{old_code} → {new_code}{'（重複代號已確認）' if duplicates else ''}；"
                     f"主檔標頭 {len(columns)} 欄（{letters}）；備份 {Path(backup_path).name}",
                     changed_at),
                )
                # 存檔放在交易內：存檔失敗交易不 commit；commit 失敗就用剛才的備份還原主檔
                main_touched = True
                _save_workbook_atomically(workbook, main_path)
        except Exception:
            if main_touched:
                _restore_main(backup_path, main_path)
            raise
    finally:
        workbook.close()

    _after_main_changed(main_path)
    return {
        "ok": True,
        "unchanged": False,
        "order_id": int(order["id"]),
        "old_code": old_code,
        "code": new_code,
        "columns": columns,
        "column_letters": [get_column_letter(col) for col in columns],
        "forced": bool(duplicates),
        "main_backup_path": backup_path,
    }


def _after_main_changed(main_path: str) -> None:
    """主檔只改標頭；仍走主檔寫入後的標準收尾，讓快照、快取、待發料副檔跟主檔 mtime 一致。"""
    import logging

    from ..snapshot_sync import refresh_snapshot_from_main
    from .main_preview import clear_main_preview_response_cache

    log = logging.getLogger(__name__)
    refresh_snapshot_from_main(main_path)
    clear_main_preview_response_cache()
    try:
        from ..routers.main_file import invalidate_main_data_cache
        invalidate_main_data_cache()
    except Exception:
        pass
    try:
        from .merge_drafts import rebuild_merge_drafts

        active_order_ids = [int(item["order_id"]) for item in db.get_active_merge_drafts()
                            if item.get("order_id") is not None]
        if active_order_ids:
            rebuild_merge_drafts(active_order_ids)
    except Exception as exc:
        log.warning("改批次代號後更新待發料副檔失敗：%s", exc)


def code_changes_restored_by_full_rollback(sessions: list[dict]) -> list[dict]:
    """整檔還原會把主檔換回 sessions[0] 發料前的備份（並重播同批前面的訂單）。

    在那之後改過批次代號、且不在這次還原範圍內的訂單，主檔標頭會被還原成舊代號，
    orders.code 卻還是新代號 → 回傳這些改動（同一張單改來改回就不算）。
    """
    if not sessions:
        return []
    cutoff = str(sessions[0].get("dispatched_at") or "").strip()
    if not cutoff:
        return []
    rolled_back = {int(item["order_id"]) for item in sessions if item.get("order_id") is not None}
    first_old: dict[int, dict] = {}
    for change in db.get_order_code_changes_after(cutoff):
        order_id = int(change.get("order_id") or 0)
        if order_id in rolled_back:
            continue
        first_old.setdefault(order_id, change)
    result = []
    for order_id, change in first_old.items():
        order = db.get_order(order_id) or {}
        if normalize_batch_code(order.get("code")) == normalize_batch_code(change.get("old_code")):
            continue
        result.append({**change, "current_code": normalize_batch_code(order.get("code")),
                       "model": str(order.get("model") or "")})
    return result
