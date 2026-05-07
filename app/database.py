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
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath, PureWindowsPath
from contextlib import contextmanager

from .config import DATA_DIR, MAIN_FILE_DIR, SCHEDULE_DIR, BOM_DIR, BOM_HISTORY_DIR, MERGE_DRAFT_DIR

DB_PATH = DATA_DIR / "system.db"
_MANAGED_PATH_FALLBACKS = {
    "main_file_path": MAIN_FILE_DIR,
    "schedule_file_path": SCHEDULE_DIR,
}
_BOM_FILE_FALLBACK_DIRS = (BOM_DIR,)
_BOM_REVISION_FALLBACK_DIRS = (BOM_HISTORY_DIR,)
_MERGE_DRAFT_FILE_FALLBACK_DIRS = (MERGE_DRAFT_DIR,)

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

CREATE TABLE IF NOT EXISTS st_inventory_snapshot (
    part_number TEXT PRIMARY KEY,
    stock_qty   REAL NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    loaded_at   TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS st_package_breakdowns (
    part_number  TEXT PRIMARY KEY,
    package_text TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL DEFAULT ''
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
    scrap_factor         REAL    NOT NULL DEFAULT 0,
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
    note           TEXT    NOT NULL DEFAULT '',
    updated_at     TEXT    NOT NULL DEFAULT '',
    UNIQUE(order_id, part_number)
);

CREATE TABLE IF NOT EXISTS purchase_reminder_statuses (
    part_number TEXT PRIMARY KEY,
    notified    INTEGER NOT NULL DEFAULT 0,
    notified_at TEXT    NOT NULL DEFAULT '',
    note        TEXT    NOT NULL DEFAULT '',
    ignored     INTEGER NOT NULL DEFAULT 0,
    ignored_at  TEXT    NOT NULL DEFAULT '',
    updated_at  TEXT    NOT NULL DEFAULT ''
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
CREATE INDEX IF NOT EXISTS idx_purchase_reminder_notified ON purchase_reminder_statuses(notified, updated_at);
CREATE INDEX IF NOT EXISTS idx_merge_drafts_order_status ON merge_drafts(order_id, status, id);
CREATE INDEX IF NOT EXISTS idx_merge_draft_files_draft ON merge_draft_files(draft_id, id);
CREATE INDEX IF NOT EXISTS idx_alerts_read ON alerts(is_read);

CREATE TABLE IF NOT EXISTS defective_batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT    NOT NULL DEFAULT '',
    imported_at     TEXT    NOT NULL DEFAULT '',
    note            TEXT    NOT NULL DEFAULT '',
    main_file_mtime REAL    NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS defective_records (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      INTEGER REFERENCES defective_batches(id),
    order_id      INTEGER REFERENCES orders(id),
    part_number   TEXT    NOT NULL DEFAULT '',
    description   TEXT    NOT NULL DEFAULT '',
    defective_qty REAL    NOT NULL DEFAULT 0,
    stock_before  REAL    NOT NULL DEFAULT 0,
    stock_after   REAL    NOT NULL DEFAULT 0,
    action_taken  TEXT    NOT NULL DEFAULT '',
    action_note   TEXT    NOT NULL DEFAULT '',
    status        TEXT    NOT NULL DEFAULT 'open',
    reported_by   TEXT    NOT NULL DEFAULT '',
    created_at    TEXT    NOT NULL DEFAULT '',
    confirmed_at  TEXT    NOT NULL DEFAULT '',
    closed_at     TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_defective_status ON defective_records(status);
CREATE INDEX IF NOT EXISTS idx_defective_order ON defective_records(order_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_records_at ON dispatch_records(dispatched_at);
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


def _repair_managed_path_setting(conn: sqlite3.Connection, key: str) -> int:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if not row:
        return 0

    original = str(row["value"] or "").strip()
    repaired = resolve_managed_path(original, setting_key=key)
    if not repaired or repaired == original:
        return 0

    conn.execute("UPDATE settings SET value=? WHERE key=?", (repaired, key))
    return 1


def _repair_managed_path_column(
    conn: sqlite3.Connection,
    *,
    table: str,
    column: str,
    setting_key: str = "",
    fallback_dirs: tuple[Path, ...] | list[Path] | None = None,
    recursive: bool = False,
) -> int:
    rows = conn.execute(
        f"SELECT id, {column} AS path_value FROM {table} WHERE {column} != ''"
    ).fetchall()
    repaired_rows = 0
    for row in rows:
        original = str(row["path_value"] or "").strip()
        repaired = resolve_managed_path(
            original,
            setting_key=setting_key,
            fallback_dirs=fallback_dirs,
            recursive=recursive,
        )
        if not repaired or repaired == original:
            continue
        conn.execute(
            f"UPDATE {table} SET {column}=? WHERE id=?",
            (repaired, row["id"]),
        )
        repaired_rows += 1
    return repaired_rows


def _repair_managed_paths(conn: sqlite3.Connection) -> int:
    repaired = 0
    for key in _MANAGED_PATH_FALLBACKS:
        repaired += _repair_managed_path_setting(conn, key)

    repaired += _repair_managed_path_column(
        conn,
        table="dispatch_sessions",
        column="main_file_path",
        setting_key="main_file_path",
    )
    repaired += _repair_managed_path_column(
        conn,
        table="merge_drafts",
        column="main_file_path",
        setting_key="main_file_path",
    )
    repaired += _repair_managed_path_column(
        conn,
        table="bom_files",
        column="filepath",
        fallback_dirs=_BOM_FILE_FALLBACK_DIRS,
    )
    repaired += _repair_managed_path_column(
        conn,
        table="bom_revisions",
        column="filepath",
        fallback_dirs=_BOM_REVISION_FALLBACK_DIRS,
        recursive=True,
    )
    repaired += _repair_managed_path_column(
        conn,
        table="merge_draft_files",
        column="filepath",
        fallback_dirs=_MERGE_DRAFT_FILE_FALLBACK_DIRS,
        recursive=True,
    )
    return repaired


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
        bom_component_cols = [r[1] for r in conn.execute("PRAGMA table_info(bom_components)").fetchall()]
        if bom_component_cols and "scrap_factor" not in bom_component_cols:
            conn.execute("ALTER TABLE bom_components ADD COLUMN scrap_factor REAL NOT NULL DEFAULT 0")
        draft_cols = [r[1] for r in conn.execute("PRAGMA table_info(merge_drafts)").fetchall()]
        if draft_cols and "main_file_mtime_ns" not in draft_cols:
            conn.execute("ALTER TABLE merge_drafts ADD COLUMN main_file_mtime_ns TEXT NOT NULL DEFAULT ''")
        supplement_cols = [r[1] for r in conn.execute("PRAGMA table_info(order_supplements)").fetchall()]
        if supplement_cols and "note" not in supplement_cols:
            conn.execute("ALTER TABLE order_supplements ADD COLUMN note TEXT NOT NULL DEFAULT ''")
        purchase_reminder_cols = [r[1] for r in conn.execute("PRAGMA table_info(purchase_reminder_statuses)").fetchall()]
        if purchase_reminder_cols and "ignored" not in purchase_reminder_cols:
            conn.execute("ALTER TABLE purchase_reminder_statuses ADD COLUMN ignored INTEGER NOT NULL DEFAULT 0")
        if purchase_reminder_cols and "ignored_at" not in purchase_reminder_cols:
            conn.execute("ALTER TABLE purchase_reminder_statuses ADD COLUMN ignored_at TEXT NOT NULL DEFAULT ''")
        # migration: defective_records 加 batch_id / stock 欄位
        def_cols = [r[1] for r in conn.execute("PRAGMA table_info(defective_records)").fetchall()]
        if def_cols and "batch_id" not in def_cols:
            conn.execute("ALTER TABLE defective_records ADD COLUMN batch_id INTEGER REFERENCES defective_batches(id)")
        if def_cols and "stock_before" not in def_cols:
            conn.execute("ALTER TABLE defective_records ADD COLUMN stock_before REAL NOT NULL DEFAULT 0")
        if def_cols and "stock_after" not in def_cols:
            conn.execute("ALTER TABLE defective_records ADD COLUMN stock_after REAL NOT NULL DEFAULT 0")
        if def_cols and "batch_id" in [r[1] for r in conn.execute("PRAGMA table_info(defective_records)").fetchall()]:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_defective_batch ON defective_records(batch_id)")

        # defective_batches: 加 main_file_mtime 欄位
        batch_cols = [r[1] for r in conn.execute("PRAGMA table_info(defective_batches)").fetchall()]
        if batch_cols and "main_file_mtime" not in batch_cols:
            conn.execute("ALTER TABLE defective_batches ADD COLUMN main_file_mtime REAL NOT NULL DEFAULT 0")

        _repair_managed_paths(conn)


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

def resolve_managed_path(
    path_value: str,
    setting_key: str = "",
    *,
    fallback_dirs: tuple[Path, ...] | list[Path] | None = None,
    recursive: bool = False,
) -> str:
    normalized = str(path_value or "").strip()
    if not normalized:
        return ""

    path = Path(normalized).expanduser()
    if path.exists():
        return str(path)

    candidate_names: list[str] = []
    for name in (
        path.name,
        PureWindowsPath(normalized).name,
        PurePosixPath(normalized).name,
    ):
        normalized_name = str(name or "").strip()
        if normalized_name and normalized_name not in candidate_names:
            candidate_names.append(normalized_name)
    if not candidate_names:
        return normalized

    candidate_dirs: list[Path] = []
    if fallback_dirs:
        candidate_dirs.extend(Path(item) for item in fallback_dirs)
    elif setting_key and setting_key in _MANAGED_PATH_FALLBACKS:
        candidate_dirs.append(_MANAGED_PATH_FALLBACKS[setting_key])
    else:
        candidate_dirs.extend(_MANAGED_PATH_FALLBACKS.values())

    for fallback_dir in candidate_dirs:
        for candidate_name in candidate_names:
            candidate = fallback_dir / candidate_name
            if candidate.exists():
                return str(candidate)
            if recursive and fallback_dir.exists():
                try:
                    nested_candidate = next(fallback_dir.rglob(candidate_name))
                except StopIteration:
                    nested_candidate = None
                if nested_candidate and nested_candidate.exists():
                    return str(nested_candidate)

    return normalized


def _repair_row_path(
    conn: sqlite3.Connection,
    row: sqlite3.Row | dict | None,
    *,
    table: str,
    column: str,
    fallback_dirs: tuple[Path, ...] | list[Path] | None = None,
    recursive: bool = False,
) -> dict | None:
    if not row:
        return None

    data = dict(row)
    original = str(data.get(column) or "").strip()
    repaired = resolve_managed_path(
        original,
        fallback_dirs=fallback_dirs,
        recursive=recursive,
    )
    if repaired and repaired != original:
        conn.execute(f"UPDATE {table} SET {column}=? WHERE id=?", (repaired, data["id"]))
        data[column] = repaired
    return data


def _repair_row_paths(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
    *,
    table: str,
    column: str,
    fallback_dirs: tuple[Path, ...] | list[Path] | None = None,
    recursive: bool = False,
) -> list[dict]:
    repaired_rows: list[dict] = []
    for row in rows:
        repaired = _repair_row_path(
            conn,
            row,
            table=table,
            column=column,
            fallback_dirs=fallback_dirs,
            recursive=recursive,
        )
        if repaired:
            repaired_rows.append(repaired)
    return repaired_rows


def get_setting(key: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        value = row["value"] if row else default
        if key in _MANAGED_PATH_FALLBACKS:
            repaired = resolve_managed_path(value, setting_key=key)
            if row and repaired and repaired != value:
                conn.execute("UPDATE settings SET value=? WHERE key=?", (repaired, key))
                return repaired
        return value


def set_setting(key: str, value: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


# ── Purchase reminder statuses ───────────────────────────────────────────────

def get_purchase_reminder_statuses() -> dict[str, dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT part_number, notified, notified_at, note, ignored, ignored_at, updated_at "
            "FROM purchase_reminder_statuses"
        ).fetchall()
    return {
        str(row["part_number"] or "").strip().upper(): {
            "notified": bool(row["notified"]),
            "notified_at": row["notified_at"] or "",
            "note": row["note"] or "",
            "ignored": bool(row["ignored"]),
            "ignored_at": row["ignored_at"] or "",
            "updated_at": row["updated_at"] or "",
        }
        for row in rows
        if str(row["part_number"] or "").strip()
    }


def set_purchase_reminder_status(part_number: str, notified: bool, note: str = "") -> dict:
    normalized = str(part_number or "").strip().upper()
    if not normalized:
        return {
            "part_number": "",
            "notified": False,
            "notified_at": "",
            "note": "",
            "ignored": False,
            "ignored_at": "",
            "updated_at": "",
        }

    now = _now()
    if not notified:
        with get_conn() as conn:
            conn.execute("DELETE FROM purchase_reminder_statuses WHERE part_number=?", (normalized,))
        return {
            "part_number": normalized,
            "notified": False,
            "notified_at": "",
            "note": "",
            "ignored": False,
            "ignored_at": "",
            "updated_at": now,
        }

    cleaned_note = str(note or "").strip()
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT notified_at FROM purchase_reminder_statuses WHERE part_number=?",
            (normalized,),
        ).fetchone()
        notified_at = existing["notified_at"] if existing and existing["notified_at"] else now
        conn.execute(
            """
            INSERT INTO purchase_reminder_statuses(part_number, notified, notified_at, note, updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(part_number) DO UPDATE SET
                notified=excluded.notified,
                notified_at=excluded.notified_at,
                note=excluded.note,
                ignored=0,
                ignored_at='',
                updated_at=excluded.updated_at
            """,
            (normalized, 1, notified_at, cleaned_note, now),
        )
    return {
        "part_number": normalized,
        "notified": True,
        "notified_at": notified_at,
        "note": cleaned_note,
        "ignored": False,
        "ignored_at": "",
        "updated_at": now,
    }


def set_purchase_reminder_ignored(part_number: str, ignored: bool) -> dict:
    normalized = str(part_number or "").strip().upper()
    if not normalized:
        return {
            "part_number": "",
            "notified": False,
            "notified_at": "",
            "note": "",
            "ignored": False,
            "ignored_at": "",
            "updated_at": "",
        }

    now = _now()
    if not ignored:
        with get_conn() as conn:
            existing = conn.execute(
                "SELECT notified, notified_at, note FROM purchase_reminder_statuses WHERE part_number=?",
                (normalized,),
            ).fetchone()
            if not existing or not existing["notified"]:
                conn.execute("DELETE FROM purchase_reminder_statuses WHERE part_number=?", (normalized,))
                return {
                    "part_number": normalized,
                    "notified": False,
                    "notified_at": "",
                    "note": "",
                    "ignored": False,
                    "ignored_at": "",
                    "updated_at": now,
                }
            conn.execute(
                "UPDATE purchase_reminder_statuses SET ignored=0, ignored_at='', updated_at=? WHERE part_number=?",
                (now, normalized),
            )
            return {
                "part_number": normalized,
                "notified": True,
                "notified_at": existing["notified_at"] or "",
                "note": existing["note"] or "",
                "ignored": False,
                "ignored_at": "",
                "updated_at": now,
            }

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO purchase_reminder_statuses(part_number, notified, notified_at, note, ignored, ignored_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(part_number) DO UPDATE SET
                notified=0,
                notified_at='',
                note='',
                ignored=excluded.ignored,
                ignored_at=excluded.ignored_at,
                updated_at=excluded.updated_at
            """,
            (normalized, 0, "", "", 1, now, now),
        )
    return {
        "part_number": normalized,
        "notified": False,
        "notified_at": "",
        "note": "",
        "ignored": True,
        "ignored_at": now,
        "updated_at": now,
    }


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
        rows = conn.execute(
            "SELECT part_number, stock_qty, moq, moq_manual, description FROM inventory_snapshot"
        ).fetchall()
    return {
        r["part_number"]: {
            "stock_qty": r["stock_qty"],
            "moq": r["moq"],
            "moq_manual": bool(r["moq_manual"]),
            "description": str(r["description"] or ""),
        }
        for r in rows
    }


def save_st_inventory_snapshot(stock: dict[str, float], descriptions: dict[str, str] | None = None):
    normalized_stock = {
        str(part).strip().upper(): float(qty or 0)
        for part, qty in (stock or {}).items()
        if str(part).strip()
    }
    normalized_desc = {
        str(part).strip().upper(): str(description or "").strip()
        for part, description in (descriptions or {}).items()
        if str(part).strip()
    }
    all_parts = sorted(set(normalized_stock) | set(normalized_desc))
    loaded_at = _now()

    with get_conn() as conn:
        conn.execute("DELETE FROM st_inventory_snapshot")
        for part in all_parts:
            conn.execute(
                "INSERT INTO st_inventory_snapshot(part_number, stock_qty, description, loaded_at) VALUES(?,?,?,?)",
                (part, normalized_stock.get(part, 0.0), normalized_desc.get(part, ""), loaded_at),
            )


def get_st_inventory_snapshot() -> dict[str, dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT part_number, stock_qty, description FROM st_inventory_snapshot").fetchall()
    return {
        str(row["part_number"]): {
            "stock_qty": float(row["stock_qty"] or 0),
            "description": str(row["description"] or ""),
        }
        for row in rows
    }


def get_st_inventory_stock() -> dict[str, float]:
    snapshot = get_st_inventory_snapshot()
    return {part: float(item.get("stock_qty") or 0) for part, item in snapshot.items()}


def update_st_inventory_stock(stock_updates: dict[str, float]) -> int:
    normalized = {
        str(part).strip().upper(): float(qty or 0)
        for part, qty in (stock_updates or {}).items()
        if str(part).strip()
    }
    if not normalized:
        return 0

    updated = 0
    loaded_at = _now()
    with get_conn() as conn:
        for part, qty in normalized.items():
            existing = conn.execute(
                "SELECT description FROM st_inventory_snapshot WHERE part_number=?",
                (part,),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE st_inventory_snapshot SET stock_qty=?, loaded_at=? WHERE part_number=?",
                    (qty, loaded_at, part),
                )
            else:
                conn.execute(
                    "INSERT INTO st_inventory_snapshot(part_number, stock_qty, description, loaded_at) VALUES(?,?,?,?)",
                    (part, qty, "", loaded_at),
                )
            updated += 1
    return updated


def get_st_package_breakdowns(part_numbers: list[str] | None = None) -> dict[str, dict]:
    normalized_parts = [
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ]
    normalized_parts = list(dict.fromkeys(normalized_parts))

    sql = "SELECT part_number, package_text, updated_at FROM st_package_breakdowns"
    params: list[str] = []
    if normalized_parts:
        placeholders = ",".join("?" * len(normalized_parts))
        sql += f" WHERE part_number IN ({placeholders})"
        params.extend(normalized_parts)
    sql += " ORDER BY part_number"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    return {
        str(row["part_number"]).strip().upper(): {
            "package_text": str(row["package_text"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
        for row in rows
    }


def save_st_package_breakdown(part_number: str, package_text: str, updated_at: str | None = None) -> str:
    part = str(part_number or "").strip().upper()
    if not part:
        return ""
    now = str(updated_at or _now()).strip() or _now()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO st_package_breakdowns(part_number, package_text, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(part_number) DO UPDATE SET package_text=excluded.package_text, updated_at=excluded.updated_at",
            (part, str(package_text or ""), now),
        )
    return part


def get_st_inventory_taken_at() -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT MAX(loaded_at) AS loaded_at FROM st_inventory_snapshot").fetchone()
    return (row["loaded_at"] if row and row["loaded_at"] else "") or ""


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
            models = _split_model_keys(row["group_model"])
            if len(models) >= 2:
                primary = models[0]
                for secondary in models[1:]:
                    if secondary == primary:
                        continue
                    alias_map[secondary] = primary
    except Exception:
        pass
    return alias_map


def _split_model_keys(value: str) -> list[str]:
    return list(dict.fromkeys(
        item.strip().upper()
        for item in str(value or "").split(",")
        if item.strip()
    ))


def _merge_schedule_rows(rows: list[dict]) -> list[dict]:
    """合併共用 BOM 的行（如 T356789IU + T356789IU-U/A）。"""
    alias_map = _get_bom_model_groups()
    merged_rows: list[dict] = []
    merge_targets: dict[str, int] = {}

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
        if merge_key in merge_targets:
            # 同 PO+機種重複行 → 合併數量
            idx = merge_targets[merge_key]
            target = merged_rows[idx]
            target["order_qty"] = (target.get("order_qty") or 0) + (r.get("order_qty") or 0)
            if r.get("pcb") and r["pcb"] not in (target.get("pcb") or ""):
                target["pcb"] = f"{target['pcb']} / {r['pcb']}"
            continue
        merge_targets[merge_key] = len(merged_rows)
        merged_rows.append(dict(r))

    return merged_rows


def _order_key(po, model) -> str:
    """用 PO+機種 當唯一 key（同 PO 可有不同機種）。"""
    return f"{str(po).strip()}|{str(model).strip().upper()}"


def upsert_orders_from_schedule(rows: list[dict]) -> dict:
    """從排程表解析結果批次寫入 orders 表。
    自動比對已存在的 PO+機種（含已發料），不重複新增。
    保留使用者手動輸入的 code（編號）。
    回傳差異摘要 {added, updated, skipped, removed, diffs}。
    """
    now = _now()
    merged_rows = _merge_schedule_rows(rows)

    with get_conn() as conn:
        all_orders = conn.execute(
            "SELECT id, po_number, model, pcb, order_qty, ship_date, status, code FROM orders"
        ).fetchall()

        # 用 PO+機種 當 key，同 PO 不同機種分開處理
        existing_by_key: dict[str, dict] = {}
        for o in all_orders:
            key = _order_key(o["po_number"], o["model"])
            existing_by_key[key] = dict(o)

        new_key_set = {
            _order_key(r.get("po_number", ""), r.get("model", ""))
            for r in merged_rows
        }

        added_count = 0
        updated_count = 0
        skipped_count = 0
        diffs: list[dict] = []

        # 刪掉「新排程表中不存在」的 pending/merged 訂單
        removed_count = 0
        for key, existing in list(existing_by_key.items()):
            if key not in new_key_set and existing["status"] in ("pending", "merged"):
                conn.execute("DELETE FROM decisions WHERE order_id = ?", (existing["id"],))
                conn.execute("DELETE FROM order_supplements WHERE order_id = ?", (existing["id"],))
                conn.execute("DELETE FROM orders WHERE id = ?", (existing["id"],))
                diffs.append({
                    "type": "removed",
                    "po_number": existing.get("po_number", ""),
                    "model": existing.get("model", ""),
                })
                removed_count += 1

        for i, r in enumerate(merged_rows):
            po = str(r.get("po_number", "")).strip()
            model = str(r.get("model", "")).strip()
            if not po:
                continue

            key = _order_key(po, model)
            existing = existing_by_key.get(key)

            if existing and existing["status"] in ("dispatched", "completed"):
                changes = _compare_order_fields(existing, r)
                if changes:
                    diffs.append({
                        "type": "skipped_changed",
                        "po_number": po, "model": model,
                        "status": existing["status"],
                        "changes": changes,
                    })
                skipped_count += 1
                continue

            if existing and existing["status"] in ("pending", "merged"):
                changes = _compare_order_fields(existing, r)
                # 保留使用者手動輸入的 code
                preserved_code = existing.get("code", "") or ""
                conn.execute(
                    "UPDATE orders SET pcb=?, order_qty=?, balance_qty=?, "
                    "delivery_date=?, ship_date=?, remark=?, "
                    "sort_order=?, row_index=?, updated_at=? WHERE id=?",
                    (
                        r.get("pcb", ""),
                        r.get("order_qty", 0),
                        r.get("balance_qty"),
                        r.get("ship_date"),
                        r.get("ship_date"),
                        r.get("remark", ""),
                        i, r.get("row_index", 0),
                        now, existing["id"],
                    ),
                )
                if changes:
                    diffs.append({
                        "type": "updated",
                        "po_number": po, "model": model,
                        "changes": changes,
                    })
                    updated_count += 1
                continue

            # 全新 PO+機種 → 新增
            conn.execute(
                "INSERT INTO orders(po_number, model, pcb, order_qty, balance_qty, "
                "delivery_date, ship_date, status, code, remark, sort_order, row_index, "
                "created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    po, model,
                    r.get("pcb", ""),
                    r.get("order_qty", 0),
                    r.get("balance_qty"),
                    r.get("ship_date"),
                    r.get("ship_date"),
                    "pending",
                    r.get("code", ""),
                    r.get("remark", ""),
                    i, r.get("row_index", 0),
                    now, now,
                ),
            )
            diffs.append({
                "type": "added",
                "po_number": po, "model": model,
            })
            added_count += 1

    return {
        "added": added_count,
        "updated": updated_count,
        "skipped": skipped_count,
        "removed": removed_count,
        "diffs": diffs,
    }


def _compare_order_fields(existing: dict, new_row: dict) -> list[dict]:
    """比對訂單欄位差異，回傳 [{field, label, old, new}]。"""
    changes: list[dict] = []

    old_qty = float(existing.get("order_qty") or 0)
    new_qty = float(new_row.get("order_qty") or 0)
    if old_qty != new_qty:
        changes.append({"field": "order_qty", "label": "數量", "old": old_qty, "new": new_qty})

    old_date = str(existing.get("ship_date") or "")
    new_date = str(new_row.get("ship_date") or "")
    if old_date != new_date:
        changes.append({"field": "ship_date", "label": "交期", "old": old_date, "new": new_date})

    old_model = str(existing.get("model") or "")
    new_model = str(new_row.get("model") or "")
    if old_model != new_model:
        changes.append({"field": "model", "label": "機種", "old": old_model, "new": new_model})

    return changes


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


def get_order_by_code(code: str) -> dict | None:
    normalized_code = str(code or "").strip()
    if not normalized_code:
        return None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM orders WHERE code=? ORDER BY id DESC LIMIT 1",
            (normalized_code,),
        ).fetchone()
    return dict(row) if row else None


def remove_duplicate_pending_orders() -> dict:
    """移除 pending/merged 中與已發料 PO+機種 完全重複的訂單。回傳 {removed, duplicates}。"""
    with get_conn() as conn:
        dispatched = conn.execute(
            "SELECT po_number, model FROM orders WHERE status IN ('dispatched','completed')"
        ).fetchall()
        dispatched_keys = {_order_key(r["po_number"], r["model"]) for r in dispatched}

        pending_orders = conn.execute(
            "SELECT id, po_number, model FROM orders WHERE status IN ('pending','merged')"
        ).fetchall()

        duplicates: list[dict] = []
        for order in pending_orders:
            key = _order_key(order["po_number"], order["model"])
            if key in dispatched_keys:
                duplicates.append({"id": order["id"], "po_number": str(order["po_number"]).strip(), "model": order["model"]})

        if duplicates:
            dup_ids = [d["id"] for d in duplicates]
            placeholders = ",".join("?" * len(dup_ids))
            conn.execute(f"DELETE FROM decisions WHERE order_id IN ({placeholders})", dup_ids)
            conn.execute(f"DELETE FROM order_supplements WHERE order_id IN ({placeholders})", dup_ids)
            conn.execute(f"DELETE FROM orders WHERE id IN ({placeholders})", dup_ids)

    return {"removed": len(duplicates), "duplicates": duplicates}


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
                "scrap_factor, needed_qty, prev_qty_cs, is_dash, is_customer_supplied) VALUES(?,?,?,?,?,?,?,?,?)",
                (bom["id"], c["part_number"], c.get("description", ""),
                 c.get("qty_per_board", 0), c.get("scrap_factor", 0), c.get("needed_qty", 0),
                 c.get("prev_qty_cs", 0), int(c.get("is_dash", False)),
                 int(c.get("is_customer_supplied", False))),
            )


def get_bom_file(bom_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bom_files WHERE id=?", (bom_id,)).fetchone()
        return _repair_row_path(
            conn,
            row,
            table="bom_files",
            column="filepath",
            fallback_dirs=_BOM_FILE_FALLBACK_DIRS,
        )


def get_bom_files() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM bom_files ORDER BY sort_order, uploaded_at, filename").fetchall()
        return _repair_row_paths(
            conn,
            rows,
            table="bom_files",
            column="filepath",
            fallback_dirs=_BOM_FILE_FALLBACK_DIRS,
        )


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
            "SELECT bc.*, bf.order_qty AS bom_order_qty "
            "FROM bom_components bc "
            "JOIN bom_files bf ON bf.id = bc.bom_file_id "
            "WHERE bc.bom_file_id=?",
            (bom_file_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_bom_components_by_model() -> dict[str, list[dict]]:
    """回傳 { MODEL_UPPER: [components] }，單次 JOIN 查詢。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT bf.group_model, bf.model, "
            "bc.part_number, bc.description, bc.qty_per_board, "
            "bc.scrap_factor, bc.needed_qty, bc.prev_qty_cs, bc.is_dash, bc.is_customer_supplied, "
            "bf.order_qty AS bom_order_qty "
            "FROM bom_files bf JOIN bom_components bc ON bc.bom_file_id = bf.id "
            "ORDER BY bf.sort_order, bf.uploaded_at, bf.filename, bc.id"
        ).fetchall()
    result: dict[str, list[dict]] = {}
    for r in rows:
        comp = {
            "part_number": r["part_number"], "description": r["description"],
            "qty_per_board": r["qty_per_board"], "needed_qty": r["needed_qty"],
            "scrap_factor": r["scrap_factor"],
            "bom_order_qty": r["bom_order_qty"],
            "prev_qty_cs": r["prev_qty_cs"], "is_dash": bool(r["is_dash"]),
            "is_customer_supplied": bool(r["is_customer_supplied"]),
        }
        raw_model = r["group_model"] or r["model"]
        for key in _split_model_keys(raw_model):
            if key not in result:
                result[key] = []
            result[key].append(comp)
    return result


def get_bom_files_by_models(models: list[str]) -> list[dict]:
    """依機種名稱查詢對應的 BOM 檔案（支援 group_model 逗號分隔）。"""
    rows = get_bom_files()
    matched = []
    seen_ids = set()
    upper_models = {m.upper() for m in models}
    for r in rows:
        raw = r["group_model"] or r["model"]
        keys = set(_split_model_keys(raw))
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
        return _repair_row_paths(
            conn,
            rows,
            table="bom_revisions",
            column="filepath",
            fallback_dirs=_BOM_REVISION_FALLBACK_DIRS,
            recursive=True,
        )


def get_bom_revision(revision_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bom_revisions WHERE id=?", (revision_id,)).fetchone()
        return _repair_row_path(
            conn,
            row,
            table="bom_revisions",
            column="filepath",
            fallback_dirs=_BOM_REVISION_FALLBACK_DIRS,
            recursive=True,
        )


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


def get_active_dispatch_sessions_after(dispatched_after: str) -> list[dict]:
    cutoff = str(dispatched_after or "").strip()
    if not cutoff:
        return []
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM dispatch_sessions "
            "WHERE rolled_back_at='' AND dispatched_at>? "
            "ORDER BY dispatched_at, id",
            (cutoff,),
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


def get_part_first_dispatched_order_code() -> dict[str, str]:
    """每個料件最早 dispatched 的訂單 code，用來標『X-X 開始缺料』。"""
    sql = (
        "SELECT dr.part_number, o.code "
        "FROM dispatch_records dr JOIN orders o ON o.id = dr.order_id "
        "WHERE dr.part_number IS NOT NULL AND dr.part_number <> '' "
        "ORDER BY dr.dispatched_at ASC, dr.id ASC"
    )
    result: dict[str, str] = {}
    with get_conn() as conn:
        for row in conn.execute(sql):
            key = str(row["part_number"] or "").strip().upper()
            code = str(row["code"] or "").strip()
            if not key or not code:
                continue
            if key not in result:
                result[key] = code
    return result


# ── Decisions ─────────────────────────────────────────────────────────────────

def save_decision(order_id: int, part_number: str, decision: str):
    part = str(part_number).strip().upper()
    if not part:
        return

    normalized_decision = str(decision or "").strip() or "None"
    with get_conn() as conn:
        if normalized_decision == "None":
            conn.execute(
                "DELETE FROM decisions WHERE order_id=? AND part_number=?",
                (order_id, part),
            )
            return

        now = _now()
        conn.execute(
            "INSERT INTO decisions(order_id, part_number, decision, decided_at) VALUES(?,?,?,?) "
            "ON CONFLICT(order_id, part_number) DO UPDATE SET decision=excluded.decision, decided_at=excluded.decided_at",
            (order_id, part, normalized_decision, now),
        )


def get_decisions_for_order(order_id: int) -> dict[str, str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT part_number, decision FROM decisions WHERE order_id=? AND decision != 'None'",
            (order_id,),
        ).fetchall()
    return {str(r["part_number"]).strip().upper(): r["decision"] for r in rows}


def get_all_decisions() -> dict[str, str]:
    """取得所有待處理訂單的決策 { part_number: decision }。較新的決策會覆蓋較舊的。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT d.part_number, d.decision FROM decisions d "
            "JOIN orders o ON o.id = d.order_id "
            "WHERE o.status IN ('pending','merged') AND d.decision != 'None' "
            "ORDER BY d.part_number, d.decided_at, d.id"
        ).fetchall()
    decisions: dict[str, str] = {}
    for r in rows:
        decisions[str(r["part_number"]).strip().upper()] = r["decision"]
    return decisions


def _filter_existing_order_ids(conn, order_ids: list[int]) -> list[int]:
    if not order_ids:
        return []
    placeholders = ",".join("?" * len(order_ids))
    rows = conn.execute(
        f"SELECT id FROM orders WHERE id IN ({placeholders})",
        order_ids,
    ).fetchall()
    existing = {int(row["id"]) for row in rows}
    return [order_id for order_id in order_ids if order_id in existing]


def replace_order_decisions(order_ids: list[int], allocations: dict[int, dict[str, str]] | None = None):
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
        normalized_ids = _filter_existing_order_ids(conn, normalized_ids)
        if not normalized_ids:
            return
        placeholders = ",".join("?" * len(normalized_ids))
        conn.execute(
            f"DELETE FROM decisions WHERE order_id IN ({placeholders})",
            normalized_ids,
        )

        for order_id in normalized_ids:
            for part_number, decision in (allocations or {}).get(order_id, {}).items():
                part = str(part_number or "").strip().upper()
                normalized_decision = str(decision or "").strip()
                if not part or not normalized_decision or normalized_decision == "None":
                    continue
                conn.execute(
                    "INSERT INTO decisions(order_id, part_number, decision, decided_at) VALUES(?,?,?,?)",
                    (order_id, part, normalized_decision, now),
                )


def get_order_decisions(order_ids: list[int] | None = None) -> dict[int, dict[str, str]]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))

    sql = "SELECT order_id, part_number, decision FROM decisions"
    params: list[int] = []
    if normalized_ids:
        placeholders = ",".join("?" * len(normalized_ids))
        sql += f" WHERE order_id IN ({placeholders})"
        params.extend(normalized_ids)
    sql += " ORDER BY order_id, part_number"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    result: dict[int, dict[str, str]] = {}
    for row in rows:
        order_id = int(row["order_id"])
        result.setdefault(order_id, {})
        decision = str(row["decision"] or "").strip()
        if decision and decision != "None":
            result[order_id][str(row["part_number"]).strip().upper()] = decision
    return result


def _normalize_supplement_note(value) -> str:
    return str(value or "").strip()


def get_order_supplement_details(order_ids: list[int] | None = None) -> dict[int, dict[str, dict]]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))

    sql = "SELECT order_id, part_number, supplement_qty, note, updated_at FROM order_supplements"
    params: list[int] = []
    if normalized_ids:
        placeholders = ",".join("?" * len(normalized_ids))
        sql += f" WHERE order_id IN ({placeholders})"
        params.extend(normalized_ids)
    sql += " ORDER BY order_id, part_number"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    result: dict[int, dict[str, dict]] = {}
    for row in rows:
        order_id = int(row["order_id"])
        part_number = str(row["part_number"]).strip().upper()
        if not part_number:
            continue
        result.setdefault(order_id, {})
        result[order_id][part_number] = {
            "supplement_qty": float(row["supplement_qty"] or 0),
            "note": _normalize_supplement_note(row["note"]),
            "updated_at": str(row["updated_at"] or "").strip(),
        }
    return result


def replace_order_supplements(
    order_ids: list[int],
    allocations: dict[int, dict[str, float]] | None = None,
    note_updates: dict[int, dict[str, str]] | None = None,
):
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
        normalized_ids = _filter_existing_order_ids(conn, normalized_ids)
        if not normalized_ids:
            return
        existing_details = get_order_supplement_details(normalized_ids)
        placeholders = ",".join("?" * len(normalized_ids))
        conn.execute(
            f"DELETE FROM order_supplements WHERE order_id IN ({placeholders})",
            normalized_ids,
        )

        for order_id in normalized_ids:
            order_note_updates = {
                str(part or "").strip().upper(): _normalize_supplement_note(note)
                for part, note in (note_updates or {}).get(order_id, {}).items()
                if str(part or "").strip()
            }
            for part_number, supplement_qty in (allocations or {}).get(order_id, {}).items():
                part = str(part_number or "").strip().upper()
                qty = float(supplement_qty or 0)
                if not part or qty <= 0:
                    continue
                previous = ((existing_details.get(order_id) or {}).get(part) or {})
                note = order_note_updates.get(part)
                if note is None:
                    note = _normalize_supplement_note(previous.get("note"))
                previous_qty = float(previous.get("supplement_qty") or 0)
                previous_note = _normalize_supplement_note(previous.get("note"))
                updated_at = str(previous.get("updated_at") or "").strip()
                if qty != previous_qty or note != previous_note or not updated_at:
                    updated_at = now
                conn.execute(
                    "INSERT INTO order_supplements(order_id, part_number, supplement_qty, note, updated_at) "
                    "VALUES(?,?,?,?,?)",
                    (order_id, part, qty, note, updated_at),
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
        items = _repair_row_paths(
            conn,
            rows,
            table="merge_draft_files",
            column="filepath",
            fallback_dirs=_MERGE_DRAFT_FILE_FALLBACK_DIRS,
            recursive=True,
        )
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


def reactivate_merge_draft(draft_id: int) -> int:
    now = _now()
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE merge_drafts SET status='active', committed_at='', updated_at=?, deleted_at='' "
            "WHERE id=? AND status='committed'",
            (now, draft_id),
        )
    return int(cur.rowcount or 0)


def get_latest_committed_merge_draft_for_order(order_id: int, committed_after: str = "") -> dict | None:
    sql = (
        "SELECT * FROM merge_drafts "
        "WHERE order_id=? AND status='committed'"
    )
    params: list[object] = [order_id]
    if str(committed_after or "").strip():
        sql += " AND committed_at>=?"
        params.append(str(committed_after).strip())
    sql += " ORDER BY committed_at DESC, id DESC LIMIT 1"
    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    result = dict(row)
    result["decisions"] = _json_loads(result.get("decisions_json", ""), {})
    result["supplements"] = _json_loads(result.get("supplements_json", ""), {})
    result["shortages"] = _json_loads(result.get("shortages_json", ""), [])
    return result


def get_expired_committed_merge_drafts(retention_days: int = 365) -> list[dict]:
    try:
        days = max(int(retention_days), 1)
    except (TypeError, ValueError):
        days = 365
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM merge_drafts "
            "WHERE status='committed' AND committed_at<>'' AND committed_at<? "
            "ORDER BY committed_at, id",
            (cutoff,),
        ).fetchall()
    results = [dict(row) for row in rows]
    for item in results:
        item["decisions"] = _json_loads(item.get("decisions_json", ""), {})
        item["supplements"] = _json_loads(item.get("supplements_json", ""), {})
        item["shortages"] = _json_loads(item.get("shortages_json", ""), [])
    return results


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


def get_activity_logs_by_action(action: str, limit: int = 50) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM activity_logs WHERE action=? ORDER BY id DESC LIMIT ?",
            (action, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def get_activity_logs_after(
    created_after: str,
    *,
    actions: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
) -> list[dict]:
    cutoff = str(created_after or "").strip()
    if not cutoff:
        return []

    normalized_actions = [str(action or "").strip() for action in (actions or []) if str(action or "").strip()]
    try:
        normalized_limit = max(int(limit), 1)
    except (TypeError, ValueError):
        normalized_limit = 100

    sql = "SELECT * FROM activity_logs WHERE created_at>?"
    params: list[object] = [cutoff]
    if normalized_actions:
        placeholders = ",".join("?" * len(normalized_actions))
        sql += f" AND action IN ({placeholders})"
        params.extend(normalized_actions)
    sql += " ORDER BY created_at, id LIMIT ?"
    params.append(normalized_limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# ── Defective Records ────────────────────────────────────────────────────────

def create_defective_batch(filename: str, note: str = "", main_file_mtime: float = 0) -> int:
    """建立不良品匯入批次，回傳 batch_id。"""
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO defective_batches(filename, imported_at, note, main_file_mtime) VALUES(?,?,?,?)",
            (filename, _now(), note, main_file_mtime),
        )
        return cur.lastrowid


def create_defective_record(data: dict) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO defective_records
               (batch_id, order_id, part_number, description, defective_qty,
                stock_before, stock_after,
                action_taken, action_note, status, reported_by, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                data.get("batch_id"),
                data.get("order_id"),
                str(data.get("part_number") or "").strip(),
                str(data.get("description") or "").strip(),
                float(data.get("defective_qty") or 0),
                float(data.get("stock_before") or 0),
                float(data.get("stock_after") or 0),
                str(data.get("action_taken") or "").strip(),
                str(data.get("action_note") or "").strip(),
                data.get("status", "open"),
                str(data.get("reported_by") or "").strip(),
                _now(),
            ),
        )
        return cur.lastrowid


def get_defective_batches() -> list[dict]:
    """取得所有批次，含每批次的項目。"""
    with get_conn() as conn:
        batches = conn.execute(
            "SELECT * FROM defective_batches ORDER BY id DESC"
        ).fetchall()
        result = []
        for b in batches:
            batch = dict(b)
            items = conn.execute(
                "SELECT * FROM defective_records WHERE batch_id=? ORDER BY id",
                (b["id"],),
            ).fetchall()
            batch["items"] = [dict(i) for i in items]
            result.append(batch)
    return result


def get_defective_batch_summaries_after(imported_after: str) -> list[dict]:
    cutoff = str(imported_after or "").strip()
    if not cutoff:
        return []
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM defective_batches WHERE imported_at>? ORDER BY imported_at, id",
            (cutoff,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_defective_batch_summaries_after_id(batch_id: int) -> list[dict]:
    try:
        normalized_id = int(batch_id)
    except (TypeError, ValueError):
        return []
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM defective_batches WHERE id>? ORDER BY id",
            (normalized_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_defective_records(status: str = "all") -> list[dict]:
    with get_conn() as conn:
        if status and status != "all":
            rows = conn.execute(
                "SELECT * FROM defective_records WHERE status=? ORDER BY id DESC", (status,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM defective_records ORDER BY id DESC"
            ).fetchall()
    return [dict(r) for r in rows]


def get_defective_record(record_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM defective_records WHERE id=?", (record_id,)).fetchone()
    return dict(row) if row else None


def update_defective_record(record_id: int, data: dict) -> bool:
    fields = []
    values = []
    for key in ("part_number", "description", "defective_qty", "action_taken", "action_note", "reported_by"):
        if key in data:
            fields.append(f"{key}=?")
            values.append(data[key])
    if not fields:
        return False
    values.append(record_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE defective_records SET {','.join(fields)} WHERE id=?", values)
    return True


def delete_defective_record(record_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM defective_records WHERE id=?", (record_id,))
    return cur.rowcount > 0


def delete_defective_batch(batch_id: int) -> bool:
    with get_conn() as conn:
        conn.execute("DELETE FROM defective_records WHERE batch_id=?", (batch_id,))
        cur = conn.execute("DELETE FROM defective_batches WHERE id=?", (batch_id,))
    return cur.rowcount > 0


def confirm_defective_record(record_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE defective_records SET status='confirmed', confirmed_at=? WHERE id=? AND status='open'",
            (_now(), record_id),
        )
    return cur.rowcount > 0


def close_defective_record(record_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE defective_records SET status='closed', closed_at=? WHERE id=? AND status IN ('open','confirmed')",
            (_now(), record_id),
        )
    return cur.rowcount > 0


# ── Analytics Queries ────────────────────────────────────────────────────────

def get_dispatch_trend(period: str = "month") -> list[dict]:
    fmt = "%Y-%m" if period == "month" else "%Y-%W"
    with get_conn() as conn:
        rows = conn.execute(
            f"""SELECT strftime('{fmt}', dispatched_at) AS period,
                       part_number,
                       SUM(needed_qty) AS total_qty,
                       COUNT(*) AS record_count,
                       decision
                FROM dispatch_records
                WHERE dispatched_at != ''
                GROUP BY period, part_number, decision
                ORDER BY period DESC, total_qty DESC""",
        ).fetchall()
    return [dict(r) for r in rows]


def get_top_dispatched_parts(limit: int = 20, months: int = 6) -> list[dict]:
    cutoff = (datetime.now() - timedelta(days=months * 30)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT part_number,
                      SUM(needed_qty) AS total_qty,
                      COUNT(DISTINCT order_id) AS order_count,
                      COUNT(*) AS record_count
               FROM dispatch_records
               WHERE dispatched_at >= ? AND decision != 'Shortage'
               GROUP BY part_number
               ORDER BY total_qty DESC
               LIMIT ?""",
            (cutoff, limit),
        ).fetchall()
    results = [dict(r) for r in rows]
    st_stock = get_st_inventory_stock()
    for item in results:
        part = str(item["part_number"]).strip().upper()
        item["st_stock_qty"] = st_stock.get(part, 0.0)
        item["has_st_stock"] = st_stock.get(part, 0.0) > 0
    return results


def get_dispatch_history(group_by: str = "model") -> list[dict]:
    if group_by == "month":
        sql = """SELECT strftime('%Y-%m', ds.dispatched_at) AS period,
                        COUNT(DISTINCT ds.order_id) AS order_count,
                        SUM(o.order_qty) AS total_qty
                 FROM dispatch_sessions ds
                 JOIN orders o ON o.id = ds.order_id
                 WHERE ds.dispatched_at != '' AND ds.rolled_back_at = ''
                 GROUP BY period
                 ORDER BY period DESC"""
    else:
        sql = """SELECT o.model AS label,
                        COUNT(DISTINCT ds.order_id) AS order_count,
                        SUM(o.order_qty) AS total_qty
                 FROM dispatch_sessions ds
                 JOIN orders o ON o.id = ds.order_id
                 WHERE ds.dispatched_at != '' AND ds.rolled_back_at = ''
                 GROUP BY o.model
                 ORDER BY total_qty DESC"""
    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]
