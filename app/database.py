"""
SQLite 資料庫管理模組 — 所有持久化資料都存在這裡。

表結構：
  inventory_snapshot  — 起始庫存快照（截止點）
  orders              — 訂單（排程行），四階段狀態
  bom_files           — BOM 副檔 metadata
  bom_components      — BOM 料件明細
  dispatch_records    — 已發料紀錄（鎖死）
  decisions           — 補貨決策（持久化）
  alerts              — 提醒
  activity_logs       — 操作日誌
  settings            — 系統設定（主檔路徑等）
"""
from __future__ import annotations
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

from .config import DATA_DIR

DB_PATH = DATA_DIR / "system.db"

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_snapshot (
    part_number TEXT PRIMARY KEY,
    stock_qty   REAL NOT NULL DEFAULT 0,
    moq         REAL NOT NULL DEFAULT 0,
    moq_manual  INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    snapshot_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS orders (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    po_number     TEXT    NOT NULL DEFAULT '',
    model         TEXT    NOT NULL DEFAULT '',
    pcb           TEXT    NOT NULL DEFAULT '',
    order_qty     REAL    NOT NULL DEFAULT 0,
    balance_qty   REAL,
    delivery_date TEXT,
    ship_date     TEXT,
    status        TEXT    NOT NULL DEFAULT 'pending',
    code          TEXT    NOT NULL DEFAULT '',
    remark        TEXT    NOT NULL DEFAULT '',
    sort_order    INTEGER NOT NULL DEFAULT 0,
    row_index     INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL DEFAULT '',
    updated_at    TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS bom_files (
    id          TEXT PRIMARY KEY,
    filename    TEXT NOT NULL DEFAULT '',
    filepath    TEXT NOT NULL DEFAULT '',
    source_filename TEXT NOT NULL DEFAULT '',
    source_format   TEXT NOT NULL DEFAULT '',
    is_converted    INTEGER NOT NULL DEFAULT 0,
    po_number   TEXT NOT NULL DEFAULT '',
    model       TEXT NOT NULL DEFAULT '',
    pcb         TEXT NOT NULL DEFAULT '',
    group_model TEXT NOT NULL DEFAULT '',
    order_qty   REAL NOT NULL DEFAULT 0,
    uploaded_at TEXT NOT NULL DEFAULT '',
    sort_order  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bom_components (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    bom_file_id          TEXT    NOT NULL REFERENCES bom_files(id) ON DELETE CASCADE,
    part_number          TEXT    NOT NULL DEFAULT '',
    description          TEXT    NOT NULL DEFAULT '',
    qty_per_board        REAL    NOT NULL DEFAULT 0,
    needed_qty           REAL    NOT NULL DEFAULT 0,
    prev_qty_cs          REAL    NOT NULL DEFAULT 0,
    is_dash              INTEGER NOT NULL DEFAULT 0,
    is_customer_supplied INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bom_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bom_file_id     TEXT    NOT NULL REFERENCES bom_files(id) ON DELETE CASCADE,
    revision_number INTEGER NOT NULL DEFAULT 0,
    filename        TEXT    NOT NULL DEFAULT '',
    filepath        TEXT    NOT NULL DEFAULT '',
    source_action   TEXT    NOT NULL DEFAULT '',
    note            TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL DEFAULT '',
    UNIQUE(bom_file_id, revision_number)
);

CREATE TABLE IF NOT EXISTS dispatch_records (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id      INTEGER NOT NULL REFERENCES orders(id),
    part_number   TEXT    NOT NULL DEFAULT '',
    needed_qty    REAL    NOT NULL DEFAULT 0,
    prev_qty_cs   REAL    NOT NULL DEFAULT 0,
    decision      TEXT    NOT NULL DEFAULT 'None',
    dispatched_at TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS dispatch_sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    previous_status TEXT    NOT NULL DEFAULT 'pending',
    backup_path     TEXT    NOT NULL DEFAULT '',
    main_file_path  TEXT    NOT NULL DEFAULT '',
    dispatched_at   TEXT    NOT NULL DEFAULT '',
    rolled_back_at  TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS decisions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id     INTEGER NOT NULL REFERENCES orders(id),
    part_number  TEXT    NOT NULL DEFAULT '',
    decision     TEXT    NOT NULL DEFAULT 'None',
    decided_at   TEXT    NOT NULL DEFAULT '',
    UNIQUE(order_id, part_number)
);

CREATE TABLE IF NOT EXISTS order_supplements (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id       INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    part_number    TEXT    NOT NULL DEFAULT '',
    supplement_qty REAL    NOT NULL DEFAULT 0,
    updated_at     TEXT    NOT NULL DEFAULT '',
    UNIQUE(order_id, part_number)
);

CREATE TABLE IF NOT EXISTS merge_drafts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id          INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    status            TEXT    NOT NULL DEFAULT 'active',
    main_file_path    TEXT    NOT NULL DEFAULT '',
    main_file_mtime_ns TEXT   NOT NULL DEFAULT '',
    main_loaded_at    TEXT    NOT NULL DEFAULT '',
    decisions_json    TEXT    NOT NULL DEFAULT '{}',
    supplements_json  TEXT    NOT NULL DEFAULT '{}',
    shortages_json    TEXT    NOT NULL DEFAULT '[]',
    created_at        TEXT    NOT NULL DEFAULT '',
    updated_at        TEXT    NOT NULL DEFAULT '',
    committed_at      TEXT    NOT NULL DEFAULT '',
    deleted_at        TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS merge_draft_files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id         INTEGER NOT NULL REFERENCES merge_drafts(id) ON DELETE CASCADE,
    bom_file_id      TEXT    NOT NULL DEFAULT '',
    filename         TEXT    NOT NULL DEFAULT '',
    filepath         TEXT    NOT NULL DEFAULT '',
    source_filename  TEXT    NOT NULL DEFAULT '',
    source_format    TEXT    NOT NULL DEFAULT '',
    model            TEXT    NOT NULL DEFAULT '',
    group_model      TEXT    NOT NULL DEFAULT '',
    carry_overs_json TEXT    NOT NULL DEFAULT '{}',
    supplements_json TEXT    NOT NULL DEFAULT '{}',
    created_at       TEXT    NOT NULL DEFAULT '',
    updated_at       TEXT    NOT NULL DEFAULT '',
    UNIQUE(draft_id, bom_file_id)
);

CREATE TABLE IF NOT EXISTS alerts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type TEXT    NOT NULL DEFAULT '',
    order_id   INTEGER,
    message    TEXT    NOT NULL DEFAULT '',
    is_read    INTEGER NOT NULL DEFAULT 0,
    created_at TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS activity_logs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    action     TEXT NOT NULL DEFAULT '',
    detail     TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_delivery ON orders(delivery_date);
CREATE INDEX IF NOT EXISTS idx_bom_comp_file ON bom_components(bom_file_id);
CREATE INDEX IF NOT EXISTS idx_bom_revisions_file ON bom_revisions(bom_file_id, revision_number);
CREATE INDEX IF NOT EXISTS idx_dispatch_order ON dispatch_records(order_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_sessions_order ON dispatch_sessions(order_id, rolled_back_at, id);
CREATE INDEX IF NOT EXISTS idx_decisions_order ON decisions(order_id);
CREATE INDEX IF NOT EXISTS idx_order_supplements_order ON order_supplements(order_id);
CREATE INDEX IF NOT EXISTS idx_merge_drafts_order_status ON merge_drafts(order_id, status, id);
CREATE INDEX IF NOT EXISTS idx_merge_draft_files_draft ON merge_draft_files(draft_id, id);
CREATE INDEX IF NOT EXISTS idx_alerts_read ON alerts(is_read);
"""


def _now() -> str:
    return datetime.now().isoformat()


def _json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: str, default):
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def init_db():
    """建立所有表（若不存在）+ migration。"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(_CREATE_SQL)
        # migration: orders 加 folder 欄位
        cols = [r[1] for r in conn.execute("PRAGMA table_info(orders)").fetchall()]
        if "folder" not in cols:
            conn.execute("ALTER TABLE orders ADD COLUMN folder TEXT NOT NULL DEFAULT ''")
        snapshot_cols = [r[1] for r in conn.execute("PRAGMA table_info(inventory_snapshot)").fetchall()]
        if "moq_manual" not in snapshot_cols:
            conn.execute("ALTER TABLE inventory_snapshot ADD COLUMN moq_manual INTEGER NOT NULL DEFAULT 0")
        bom_cols = [r[1] for r in conn.execute("PRAGMA table_info(bom_files)").fetchall()]
        if "source_filename" not in bom_cols:
            conn.execute("ALTER TABLE bom_files ADD COLUMN source_filename TEXT NOT NULL DEFAULT ''")
        if "source_format" not in bom_cols:
            conn.execute("ALTER TABLE bom_files ADD COLUMN source_format TEXT NOT NULL DEFAULT ''")
        if "is_converted" not in bom_cols:
            conn.execute("ALTER TABLE bom_files ADD COLUMN is_converted INTEGER NOT NULL DEFAULT 0")
        if "sort_order" not in bom_cols:
            conn.execute("ALTER TABLE bom_files ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0")
        draft_cols = [r[1] for r in conn.execute("PRAGMA table_info(merge_drafts)").fetchall()]
        if draft_cols and "main_file_mtime_ns" not in draft_cols:
            conn.execute("ALTER TABLE merge_drafts ADD COLUMN main_file_mtime_ns TEXT NOT NULL DEFAULT ''")


@contextmanager
def get_conn():
    """取得 SQLite 連線（WAL mode, foreign keys ON）。"""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Settings ──────────────────────────────────────────────────────────────────

def get_setting(key: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key: str, value: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


# ── Inventory Snapshot ────────────────────────────────────────────────────────

def save_snapshot(
    stock: dict[str, float],
    moq: dict[str, float] | None = None,
    manual_moq_parts: set[str] | None = None,
):
    """儲存起始庫存快照（截止點）。"""
    norm_stock = {
        str(part).strip().upper(): qty
        for part, qty in stock.items()
        if str(part).strip()
    }
    norm_moq = {
        str(part).strip().upper(): qty
        for part, qty in (moq or {}).items()
        if str(part).strip()
    }
    norm_manual_parts = {
        str(part).strip().upper()
        for part in (manual_moq_parts or set())
        if str(part).strip()
    }
    all_parts = sorted(set(norm_stock) | set(norm_moq))
    now = _now()
    with get_conn() as conn:
        conn.execute("DELETE FROM inventory_snapshot")
        for part in all_parts:
            conn.execute(
                "INSERT INTO inventory_snapshot(part_number, stock_qty, moq, moq_manual, snapshot_at) VALUES(?,?,?,?,?)",
                (part, norm_stock.get(part, 0), norm_moq.get(part, 0), 1 if part in norm_manual_parts else 0, now),
            )


def update_snapshot_stock(stock_updates: dict[str, float]) -> int:
    """只更新快照中的庫存值，用於修正舊版讀檔 bug。"""
    normalized = {
        str(part).strip().upper(): qty
        for part, qty in (stock_updates or {}).items()
        if str(part).strip()
    }
    if not normalized:
        return 0

    updated = 0
    with get_conn() as conn:
        for part, qty in normalized.items():
            cur = conn.execute(
                "UPDATE inventory_snapshot SET stock_qty=? WHERE part_number=?",
                (qty, part),
            )
            updated += cur.rowcount or 0
    return updated


def upsert_snapshot_moq(part_number: str, moq: float) -> str:
    normalized = str(part_number or "").strip().upper()
    if not normalized:
        return ""

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT stock_qty FROM inventory_snapshot WHERE part_number=?",
            (normalized,),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE inventory_snapshot SET moq=?, moq_manual=1 WHERE part_number=?",
                (moq, normalized),
            )
            return normalized

        snapshot_row = conn.execute(
            "SELECT MAX(snapshot_at) AS snapshot_at FROM inventory_snapshot"
        ).fetchone()
        snapshot_at = (snapshot_row["snapshot_at"] if snapshot_row and snapshot_row["snapshot_at"] else "") or _now()
        conn.execute(
            "INSERT INTO inventory_snapshot(part_number, stock_qty, moq, moq_manual, snapshot_at) VALUES(?,?,?,?,?)",
            (normalized, 0, moq, 1, snapshot_at),
        )
    return normalized


def get_snapshot() -> dict[str, dict]:
    """取得快照 {PART: {stock_qty, moq}}"""
    with get_conn() as conn:
        rows = conn.execute("SELECT part_number, stock_qty, moq, moq_manual FROM inventory_snapshot").fetchall()
    return {
        r["part_number"]: {
            "stock_qty": r["stock_qty"],
            "moq": r["moq"],
            "moq_manual": bool(r["moq_manual"]),
        }
        for r in rows
    }


def get_snapshot_taken_at() -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT MAX(snapshot_at) AS snapshot_at FROM inventory_snapshot").fetchone()
    return (row["snapshot_at"] if row and row["snapshot_at"] else "") or ""


def get_snapshot_stock() -> dict[str, float]:
    snap = get_snapshot()
    return {k: v["stock_qty"] for k, v in snap.items()}


def get_snapshot_moq() -> dict[str, float]:
    snap = get_snapshot()
    return {k: v["moq"] for k, v in snap.items()}


def get_manual_snapshot_moq() -> dict[str, float]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT part_number, moq FROM inventory_snapshot WHERE moq_manual=1"
        ).fetchall()
    return {r["part_number"]: r["moq"] for r in rows}


# ── Orders ────────────────────────────────────────────────────────────────────

def _get_bom_model_groups() -> dict[str, str]:
    """取得 BOM group_model 的合併對照表 { MODEL_UPPER: PRIMARY_MODEL_UPPER }。
    例如 group_model='T356789IU,T356789IU-U/A' → {'T356789IU-U/A': 'T356789IU'}
    """
    alias_map: dict[str, str] = {}
    try:
        with get_conn() as conn:
            rows = conn.execute("SELECT group_model FROM bom_files WHERE group_model LIKE '%,%'").fetchall()
        for row in rows:
            models = [m.strip().upper() for m in row["group_model"].split(",") if m.strip()]
            if len(models) >= 2:
                primary = models[0]
                for secondary in models[1:]:
                    alias_map[secondary] = primary
    except Exception:
        pass
    return alias_map


def upsert_orders_from_schedule(rows: list[dict]):
    """從排程表解析結果批次寫入 orders 表。
    自動合併共用 BOM 的行（如 T356789IU + T356789IU-U/A）。
    status=pending/merged 的會被清除重建。
    """
    now = _now()
    alias_map = _get_bom_model_groups()

    # 合併共用 BOM 的行：相同 PO + 次要機種合入主要機種
    merged_rows: list[dict] = []
    merge_targets: dict[str, int] = {}  # key="PO|PRIMARY_MODEL" → index in merged_rows

    for r in rows:
        model_upper = (r.get("model") or "").strip().upper()
        po = str(r.get("po_number", ""))
        primary = alias_map.get(model_upper)

        if primary:
            merge_key = f"{po}|{primary}"
            if merge_key in merge_targets:
                idx = merge_targets[merge_key]
                target = merged_rows[idx]
                target["order_qty"] = (target.get("order_qty") or 0) + (r.get("order_qty") or 0)
                target["pcb"] = f"{target['pcb']} / {r.get('pcb', '')}"
                continue

        merge_key = f"{po}|{model_upper}"
        merge_targets[merge_key] = len(merged_rows)
        merged_rows.append(dict(r))

    with get_conn() as conn:
        # 先刪掉待處理訂單的決策，避免外鍵擋住 orders 重建
        conn.execute(
            "DELETE FROM decisions WHERE order_id IN "
            "(SELECT id FROM orders WHERE status IN ('pending','merged'))"
        )
        conn.execute(
            "DELETE FROM order_supplements WHERE order_id IN "
            "(SELECT id FROM orders WHERE status IN ('pending','merged'))"
        )
        conn.execute("DELETE FROM orders WHERE status IN ('pending','merged')")
        for i, r in enumerate(merged_rows):
            conn.execute(
                "INSERT INTO orders(po_number, model, pcb, order_qty, balance_qty, "
                "delivery_date, ship_date, status, code, remark, sort_order, row_index, "
                "created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    str(r.get("po_number", "")),
                    r.get("model", ""),
                    r.get("pcb", ""),
                    r.get("order_qty", 0),
                    r.get("balance_qty"),
                    r.get("ship_date"),
                    r.get("ship_date"),
                    "pending",
                    r.get("code", ""),
                    r.get("remark", ""),
                    i,
                    r.get("row_index", 0),
                    now, now,
                ),
            )


def get_orders(statuses: list[str] | None = None) -> list[dict]:
    """取得訂單列表。statuses=None 取全部。"""
    with get_conn() as conn:
        if statuses:
            placeholders = ",".join("?" * len(statuses))
            rows = conn.execute(
                f"SELECT * FROM orders WHERE status IN ({placeholders}) ORDER BY sort_order, delivery_date, row_index",
                statuses,
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM orders ORDER BY sort_order, delivery_date, row_index").fetchall()
    return [dict(r) for r in rows]


def get_order(order_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    return dict(row) if row else None


def update_order(order_id: int, **fields):
    fields["updated_at"] = _now()
    sets = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [order_id]
    with get_conn() as conn:
        conn.execute(f"UPDATE orders SET {sets} WHERE id=?", vals)


def get_dispatch_folders() -> list[str]:
    """取得已發料訂單中所有不重複的 folder 名稱（不含空字串）。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT folder FROM orders WHERE status IN ('dispatched','completed') AND folder != '' ORDER BY folder"
        ).fetchall()
    return [r["folder"] for r in rows]


def move_orders_to_folder(order_ids: list[int], folder: str):
    if not order_ids:
        return
    placeholders = ",".join("?" * len(order_ids))
    with get_conn() as conn:
        conn.execute(
            f"UPDATE orders SET folder=?, updated_at=? WHERE id IN ({placeholders})",
            [folder, _now()] + order_ids,
        )


def move_orders_to_folder_by_name(folder_name: str):
    """將指定資料夾的訂單全部移回未歸檔（folder=''）。"""
    with get_conn() as conn:
        conn.execute(
            "UPDATE orders SET folder='', updated_at=? WHERE folder=?",
            [_now(), folder_name],
        )


def update_orders_sort(order_ids: list[int]):
    """按 order_ids 順序更新 sort_order。"""
    with get_conn() as conn:
        for i, oid in enumerate(order_ids):
            conn.execute("UPDATE orders SET sort_order=?, updated_at=? WHERE id=?", (i, _now(), oid))


def batch_merge_orders(order_ids: list[int]):
    """批次將 pending/merged 訂單改為 merged。"""
    now = _now()
    with get_conn() as conn:
        placeholders = ",".join("?" * len(order_ids))
        conn.execute(
            f"UPDATE orders SET status='merged', updated_at=? WHERE id IN ({placeholders}) AND status IN ('pending','merged')",
            [now] + order_ids,
        )


# ── BOM Files ─────────────────────────────────────────────────────────────────

def save_bom_file(bom: dict):
    """儲存一個 BOM 檔案及其 components。"""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT sort_order FROM bom_files WHERE id=?",
            (bom["id"],),
        ).fetchone()
        if bom.get("sort_order") is not None:
            sort_order = int(bom.get("sort_order", 0))
        elif existing:
            sort_order = int(existing["sort_order"])
        else:
            row = conn.execute("SELECT COALESCE(MAX(sort_order), -1) AS max_sort FROM bom_files").fetchone()
            max_sort = row["max_sort"] if row and row["max_sort"] is not None else -1
            sort_order = int(max_sort) + 1

        conn.execute(
            "INSERT INTO bom_files("
            "id, filename, filepath, source_filename, source_format, is_converted, "
            "po_number, model, pcb, group_model, order_qty, uploaded_at, sort_order"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "filename=excluded.filename, "
            "filepath=excluded.filepath, "
            "source_filename=excluded.source_filename, "
            "source_format=excluded.source_format, "
            "is_converted=excluded.is_converted, "
            "po_number=excluded.po_number, "
            "model=excluded.model, "
            "pcb=excluded.pcb, "
            "group_model=excluded.group_model, "
            "order_qty=excluded.order_qty, "
            "uploaded_at=excluded.uploaded_at, "
            "sort_order=excluded.sort_order",
            (
                bom["id"],
                bom["filename"],
                bom["filepath"],
                bom.get("source_filename", bom.get("filename", "")),
                bom.get("source_format", Path(str(bom.get("filename", ""))).suffix.lower()),
                int(bool(bom.get("is_converted", False))),
                str(bom.get("po_number", "")),
                bom["model"],
                bom.get("pcb", ""),
                bom.get("group_model", ""),
                bom.get("order_qty", 0),
                bom.get("uploaded_at", ""),
                sort_order,
            ),
        )
        # 清除舊 components
        conn.execute("DELETE FROM bom_components WHERE bom_file_id=?", (bom["id"],))
        for c in bom.get("components", []):
            conn.execute(
                "INSERT INTO bom_components(bom_file_id, part_number, description, qty_per_board, "
                "needed_qty, prev_qty_cs, is_dash, is_customer_supplied) VALUES(?,?,?,?,?,?,?,?)",
                (bom["id"], c["part_number"], c.get("description", ""),
                 c.get("qty_per_board", 0), c.get("needed_qty", 0),
                 c.get("prev_qty_cs", 0), int(c.get("is_dash", False)),
                 int(c.get("is_customer_supplied", False))),
            )


def get_bom_file(bom_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bom_files WHERE id=?", (bom_id,)).fetchone()
    return dict(row) if row else None


def get_bom_files() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM bom_files ORDER BY sort_order, uploaded_at, filename").fetchall()
    return [dict(r) for r in rows]


def save_bom_order(groups: list[dict]) -> int:
    normalized_groups = []
    for group in groups or []:
        model = str(group.get("model", "") or "").strip()
        item_ids = [
            str(item_id).strip()
            for item_id in group.get("item_ids", [])
            if str(item_id).strip()
        ]
        if not item_ids:
            continue
        normalized_groups.append({"model": model, "item_ids": item_ids})

    if not normalized_groups:
        return 0

    updated = 0
    with get_conn() as conn:
        rows = conn.execute("SELECT id, group_model, sort_order FROM bom_files ORDER BY sort_order, uploaded_at, filename").fetchall()
        existing = {r["id"]: dict(r) for r in rows}
        seen_ids: set[str] = set()
        next_sort = 0

        for group in normalized_groups:
            for item_id in group["item_ids"]:
                if item_id not in existing or item_id in seen_ids:
                    continue
                conn.execute(
                    "UPDATE bom_files SET group_model=?, sort_order=? WHERE id=?",
                    (group["model"], next_sort, item_id),
                )
                seen_ids.add(item_id)
                updated += 1
                next_sort += 1

        for row in rows:
            if row["id"] in seen_ids:
                continue
            conn.execute(
                "UPDATE bom_files SET sort_order=? WHERE id=?",
                (next_sort, row["id"]),
            )
            updated += 1
            next_sort += 1

    return updated


def get_bom_components(bom_file_id: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bom_components WHERE bom_file_id=?", (bom_file_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_bom_components_by_model() -> dict[str, list[dict]]:
    """回傳 { MODEL_UPPER: [components] }，單次 JOIN 查詢。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT bf.group_model, bf.model, "
            "bc.part_number, bc.description, bc.qty_per_board, "
            "bc.needed_qty, bc.prev_qty_cs, bc.is_dash, bc.is_customer_supplied "
            "FROM bom_files bf JOIN bom_components bc ON bc.bom_file_id = bf.id "
            "ORDER BY bf.sort_order, bf.uploaded_at, bf.filename, bc.id"
        ).fetchall()
    result: dict[str, list[dict]] = {}
    for r in rows:
        comp = {
            "part_number": r["part_number"], "description": r["description"],
            "qty_per_board": r["qty_per_board"], "needed_qty": r["needed_qty"],
            "prev_qty_cs": r["prev_qty_cs"], "is_dash": bool(r["is_dash"]),
            "is_customer_supplied": bool(r["is_customer_supplied"]),
        }
        raw_model = r["group_model"] or r["model"]
        for key in [k.strip().upper() for k in raw_model.split(",") if k.strip()]:
            if key not in result:
                result[key] = []
            result[key].append(comp)
    return result


def get_bom_files_by_models(models: list[str]) -> list[dict]:
    """依機種名稱查詢對應的 BOM 檔案（支援 group_model 逗號分隔）。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bom_files ORDER BY sort_order, uploaded_at, filename"
        ).fetchall()
    matched = []
    seen_ids = set()
    upper_models = {m.upper() for m in models}
    for r in rows:
        raw = r["group_model"] or r["model"]
        keys = {k.strip().upper() for k in raw.split(",") if k.strip()}
        if keys & upper_models and r["id"] not in seen_ids:
            matched.append(dict(r))
            seen_ids.add(r["id"])
    return matched


def save_bom_revision(revision: dict) -> dict:
    created_at = revision.get("created_at") or _now()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(revision_number), 0) AS max_revision FROM bom_revisions WHERE bom_file_id=?",
            (revision["bom_file_id"],),
        ).fetchone()
        next_revision = int(row["max_revision"] or 0) + 1
        cur = conn.execute(
            "INSERT INTO bom_revisions("
            "bom_file_id, revision_number, filename, filepath, source_action, note, created_at"
            ") VALUES(?,?,?,?,?,?,?)",
            (
                revision["bom_file_id"],
                next_revision,
                revision["filename"],
                revision["filepath"],
                revision.get("source_action", ""),
                revision.get("note", ""),
                created_at,
            ),
        )
        revision_id = cur.lastrowid
    return {
        "id": revision_id,
        "bom_file_id": revision["bom_file_id"],
        "revision_number": next_revision,
        "filename": revision["filename"],
        "filepath": revision["filepath"],
        "source_action": revision.get("source_action", ""),
        "note": revision.get("note", ""),
        "created_at": created_at,
    }


def get_bom_revisions(bom_file_id: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bom_revisions WHERE bom_file_id=? ORDER BY revision_number DESC, id DESC",
            (bom_file_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_bom_revision(revision_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bom_revisions WHERE id=?", (revision_id,)).fetchone()
    return dict(row) if row else None


def delete_bom_file(bom_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM bom_revisions WHERE bom_file_id=?", (bom_id,))
        conn.execute("DELETE FROM bom_components WHERE bom_file_id=?", (bom_id,))
        conn.execute("DELETE FROM bom_files WHERE id=?", (bom_id,))


# ── Dispatch Records ──────────────────────────────────────────────────────────

def save_dispatch_records(order_id: int, records: list[dict]):
    """儲存發料紀錄（鎖死不再重算）。"""
    now = _now()
    with get_conn() as conn:
        for r in records:
            conn.execute(
                "INSERT INTO dispatch_records(order_id, part_number, needed_qty, prev_qty_cs, decision, dispatched_at) "
                "VALUES(?,?,?,?,?,?)",
                (order_id, r["part_number"], r["needed_qty"], r.get("prev_qty_cs", 0),
                 r.get("decision", "None"), now),
            )


def get_dispatch_records(order_id: int | None = None) -> list[dict]:
    with get_conn() as conn:
        if order_id:
            rows = conn.execute("SELECT * FROM dispatch_records WHERE order_id=?", (order_id,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM dispatch_records").fetchall()
    return [dict(r) for r in rows]


def save_dispatch_session(
    order_id: int,
    previous_status: str,
    backup_path: str,
    main_file_path: str,
    dispatched_at: str | None = None,
) -> dict:
    dispatched_at = dispatched_at or _now()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO dispatch_sessions(order_id, previous_status, backup_path, main_file_path, dispatched_at, rolled_back_at) "
            "VALUES(?,?,?,?,?, '')",
            (order_id, previous_status, backup_path, main_file_path, dispatched_at),
        )
        session_id = cur.lastrowid
    return {
        "id": session_id,
        "order_id": order_id,
        "previous_status": previous_status,
        "backup_path": backup_path,
        "main_file_path": main_file_path,
        "dispatched_at": dispatched_at,
        "rolled_back_at": "",
    }


def get_active_dispatch_session(order_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM dispatch_sessions "
            "WHERE order_id=? AND rolled_back_at='' "
            "ORDER BY id DESC LIMIT 1",
            (order_id,),
        ).fetchone()
    return dict(row) if row else None


def get_active_dispatch_sessions() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM dispatch_sessions WHERE rolled_back_at='' ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def get_dispatch_session_tail(session_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM dispatch_sessions WHERE rolled_back_at='' AND id>=? ORDER BY id",
            (session_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_dispatch_sessions_rolled_back(session_ids: list[int]) -> int:
    normalized_ids = []
    for session_id in session_ids or []:
        try:
            normalized_ids.append(int(session_id))
        except (TypeError, ValueError):
            continue
    if not normalized_ids:
        return 0

    placeholders = ",".join("?" * len(normalized_ids))
    with get_conn() as conn:
        cur = conn.execute(
            f"UPDATE dispatch_sessions SET rolled_back_at=? WHERE id IN ({placeholders}) AND rolled_back_at=''",
            [_now()] + normalized_ids,
        )
    return int(cur.rowcount or 0)


def delete_dispatch_records_for_orders(order_ids: list[int]) -> int:
    normalized_ids = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    if not normalized_ids:
        return 0

    placeholders = ",".join("?" * len(normalized_ids))
    with get_conn() as conn:
        cur = conn.execute(
            f"DELETE FROM dispatch_records WHERE order_id IN ({placeholders})",
            normalized_ids,
        )
    return int(cur.rowcount or 0)


def get_all_dispatched_consumption(after_snapshot_at: str = "") -> dict[str, float]:
    """取得所有已發料的消耗量加總 { PART: total_needed }。"""
    sql = (
        "SELECT part_number, SUM(needed_qty) as total FROM dispatch_records "
        "WHERE decision != 'Shortage'"
    )
    params: list[str] = []
    if after_snapshot_at:
        sql += " AND dispatched_at > ?"
        params.append(after_snapshot_at)
    sql += " GROUP BY part_number"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {r["part_number"].upper(): r["total"] for r in rows}


# ── Decisions ─────────────────────────────────────────────────────────────────

def save_decision(order_id: int, part_number: str, decision: str):
    now = _now()
    part = str(part_number).strip().upper()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO decisions(order_id, part_number, decision, decided_at) VALUES(?,?,?,?) "
            "ON CONFLICT(order_id, part_number) DO UPDATE SET decision=excluded.decision, decided_at=excluded.decided_at",
            (order_id, part, decision, now),
        )


def get_decisions_for_order(order_id: int) -> dict[str, str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT part_number, decision FROM decisions WHERE order_id=?", (order_id,)).fetchall()
    return {str(r["part_number"]).strip().upper(): r["decision"] for r in rows}


def get_all_decisions() -> dict[str, str]:
    """取得所有待處理訂單的決策 { part_number: decision }。較新的決策會覆蓋較舊的。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT d.part_number, d.decision FROM decisions d "
            "JOIN orders o ON o.id = d.order_id "
            "WHERE o.status IN ('pending','merged') "
            "ORDER BY d.part_number, d.decided_at, d.id"
        ).fetchall()
    decisions: dict[str, str] = {}
    for r in rows:
        decisions[str(r["part_number"]).strip().upper()] = r["decision"]
    return decisions


def replace_order_supplements(order_ids: list[int], allocations: dict[int, dict[str, float]] | None = None):
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return

    now = _now()
    with get_conn() as conn:
        placeholders = ",".join("?" * len(normalized_ids))
        conn.execute(
            f"DELETE FROM order_supplements WHERE order_id IN ({placeholders})",
            normalized_ids,
        )

        for order_id in normalized_ids:
            for part_number, supplement_qty in (allocations or {}).get(order_id, {}).items():
                part = str(part_number or "").strip().upper()
                qty = float(supplement_qty or 0)
                if not part or qty <= 0:
                    continue
                conn.execute(
                    "INSERT INTO order_supplements(order_id, part_number, supplement_qty, updated_at) "
                    "VALUES(?,?,?,?)",
                    (order_id, part, qty, now),
                )


def get_order_supplements(order_ids: list[int] | None = None) -> dict[int, dict[str, float]]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))

    sql = "SELECT order_id, part_number, supplement_qty FROM order_supplements"
    params: list[int] = []
    if normalized_ids:
        placeholders = ",".join("?" * len(normalized_ids))
        sql += f" WHERE order_id IN ({placeholders})"
        params.extend(normalized_ids)
    sql += " ORDER BY order_id, part_number"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    result: dict[int, dict[str, float]] = {}
    for row in rows:
        order_id = int(row["order_id"])
        result.setdefault(order_id, {})
        result[order_id][str(row["part_number"]).strip().upper()] = float(row["supplement_qty"] or 0)
    return result


def replace_merge_draft(
    *,
    order_id: int,
    main_file_path: str,
    main_file_mtime_ns: str,
    main_loaded_at: str,
    decisions: dict[str, str] | None = None,
    supplements: dict[str, float] | None = None,
    shortages: list[dict] | None = None,
) -> dict:
    now = _now()
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id, created_at FROM merge_drafts WHERE order_id=? AND status='active' ORDER BY id DESC LIMIT 1",
            (order_id,),
        ).fetchone()
        if existing:
            draft_id = int(existing["id"])
            created_at = str(existing["created_at"] or now)
            conn.execute(
                "UPDATE merge_drafts SET "
                "main_file_path=?, main_file_mtime_ns=?, main_loaded_at=?, "
                "decisions_json=?, supplements_json=?, shortages_json=?, updated_at=?, deleted_at='' "
                "WHERE id=?",
                (
                    main_file_path,
                    str(main_file_mtime_ns or ""),
                    main_loaded_at,
                    _json_dumps(decisions or {}),
                    _json_dumps(supplements or {}),
                    _json_dumps(shortages or []),
                    now,
                    draft_id,
                ),
            )
        else:
            cur = conn.execute(
                "INSERT INTO merge_drafts("
                "order_id, status, main_file_path, main_file_mtime_ns, main_loaded_at, "
                "decisions_json, supplements_json, shortages_json, created_at, updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    order_id,
                    "active",
                    main_file_path,
                    str(main_file_mtime_ns or ""),
                    main_loaded_at,
                    _json_dumps(decisions or {}),
                    _json_dumps(supplements or {}),
                    _json_dumps(shortages or []),
                    now,
                    now,
                ),
            )
            draft_id = int(cur.lastrowid)
            created_at = now

    return {
        "id": draft_id,
        "order_id": int(order_id),
        "status": "active",
        "main_file_path": main_file_path,
        "main_file_mtime_ns": str(main_file_mtime_ns or ""),
        "main_loaded_at": main_loaded_at,
        "decisions": dict(decisions or {}),
        "supplements": dict(supplements or {}),
        "shortages": list(shortages or []),
        "created_at": created_at,
        "updated_at": now,
        "committed_at": "",
        "deleted_at": "",
    }


def replace_merge_draft_files(draft_id: int, files: list[dict]):
    now = _now()
    with get_conn() as conn:
        conn.execute("DELETE FROM merge_draft_files WHERE draft_id=?", (draft_id,))
        for item in files or []:
            conn.execute(
                "INSERT INTO merge_draft_files("
                "draft_id, bom_file_id, filename, filepath, source_filename, source_format, "
                "model, group_model, carry_overs_json, supplements_json, created_at, updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    draft_id,
                    str(item.get("bom_file_id") or ""),
                    str(item.get("filename") or ""),
                    str(item.get("filepath") or ""),
                    str(item.get("source_filename") or ""),
                    str(item.get("source_format") or ""),
                    str(item.get("model") or ""),
                    str(item.get("group_model") or ""),
                    _json_dumps(item.get("carry_overs") or {}),
                    _json_dumps(item.get("supplements") or {}),
                    now,
                    now,
                ),
            )


def get_merge_draft(draft_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM merge_drafts WHERE id=? ORDER BY id DESC LIMIT 1",
            (draft_id,),
        ).fetchone()
    if not row:
        return None
    result = dict(row)
    result["decisions"] = _json_loads(result.get("decisions_json", ""), {})
    result["supplements"] = _json_loads(result.get("supplements_json", ""), {})
    result["shortages"] = _json_loads(result.get("shortages_json", ""), [])
    return result


def get_active_merge_draft_for_order(order_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM merge_drafts WHERE order_id=? AND status='active' ORDER BY id DESC LIMIT 1",
            (order_id,),
        ).fetchone()
    if not row:
        return None
    result = dict(row)
    result["decisions"] = _json_loads(result.get("decisions_json", ""), {})
    result["supplements"] = _json_loads(result.get("supplements_json", ""), {})
    result["shortages"] = _json_loads(result.get("shortages_json", ""), [])
    return result


def get_merge_draft_files(draft_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM merge_draft_files WHERE draft_id=? ORDER BY id",
            (draft_id,),
        ).fetchall()
    items = [dict(row) for row in rows]
    for item in items:
        item["carry_overs"] = _json_loads(item.get("carry_overs_json", ""), {})
        item["supplements"] = _json_loads(item.get("supplements_json", ""), {})
    return items


def get_active_merge_drafts(order_ids: list[int] | None = None) -> list[dict]:
    normalized_ids = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))

    sql = (
        "SELECT md.*, o.po_number, o.model, o.pcb, o.order_qty, o.delivery_date, o.ship_date, o.sort_order, o.row_index "
        "FROM merge_drafts md "
        "JOIN orders o ON o.id = md.order_id "
        "WHERE md.status='active'"
    )
    params: list[int] = []
    if normalized_ids:
        placeholders = ",".join("?" * len(normalized_ids))
        sql += f" AND md.order_id IN ({placeholders})"
        params.extend(normalized_ids)
    sql += " ORDER BY o.sort_order, o.delivery_date, o.row_index, md.id"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    results = [dict(row) for row in rows]
    for item in results:
        item["decisions"] = _json_loads(item.get("decisions_json", ""), {})
        item["supplements"] = _json_loads(item.get("supplements_json", ""), {})
        item["shortages"] = _json_loads(item.get("shortages_json", ""), [])
        item["files"] = get_merge_draft_files(int(item["id"]))
    return results


def get_active_merge_draft_ids_by_order_ids(order_ids: list[int]) -> dict[int, int]:
    normalized_ids = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return {}

    placeholders = ",".join("?" * len(normalized_ids))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT order_id, id FROM merge_drafts WHERE status='active' AND order_id IN ({placeholders}) ORDER BY id DESC",
            normalized_ids,
        ).fetchall()

    mapping: dict[int, int] = {}
    for row in rows:
        order_id = int(row["order_id"])
        if order_id not in mapping:
            mapping[order_id] = int(row["id"])
    return mapping


def mark_merge_draft_committed(draft_id: int) -> int:
    now = _now()
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE merge_drafts SET status='committed', committed_at=?, updated_at=? "
            "WHERE id=? AND status='active'",
            (now, now, draft_id),
        )
    return int(cur.rowcount or 0)


def delete_merge_draft(draft_id: int) -> int:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM merge_drafts WHERE id=?", (draft_id,))
    return int(cur.rowcount or 0)


# ── Alerts ────────────────────────────────────────────────────────────────────

def create_alert(alert_type: str, message: str, order_id: int | None = None):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO alerts(alert_type, order_id, message, created_at) VALUES(?,?,?,?)",
            (alert_type, order_id, message, _now()),
        )


def get_alerts(unread_only: bool = False) -> list[dict]:
    with get_conn() as conn:
        if unread_only:
            rows = conn.execute("SELECT * FROM alerts WHERE is_read=0 ORDER BY created_at DESC").fetchall()
        else:
            rows = conn.execute("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 200").fetchall()
    return [dict(r) for r in rows]


def mark_alert_read(alert_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE alerts SET is_read=1 WHERE id=?", (alert_id,))


def mark_all_alerts_read():
    with get_conn() as conn:
        conn.execute("UPDATE alerts SET is_read=1 WHERE is_read=0")


# ── Activity Logs ─────────────────────────────────────────────────────────────

def log_activity(action: str, detail: str = ""):
    with get_conn() as conn:
        conn.execute("INSERT INTO activity_logs(action, detail, created_at) VALUES(?,?,?)",
                     (action, detail, _now()))
        # 保留最新 2000 筆
        conn.execute(
            "DELETE FROM activity_logs WHERE id NOT IN "
            "(SELECT id FROM activity_logs ORDER BY id DESC LIMIT 2000)"
        )


def get_activity_logs(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM activity_logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]
