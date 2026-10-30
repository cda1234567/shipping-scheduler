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

from .constants import ST_RECONCILE_ADJUSTMENT_REASON
from .config import DATA_DIR, MAIN_FILE_DIR, SCHEDULE_DIR, BOM_DIR, BOM_HISTORY_DIR, MERGE_DRAFT_DIR
from .services.local_time import local_now

DB_PATH = DATA_DIR / "system.db"
_MANAGED_PATH_FALLBACKS = {
    "main_file_path": MAIN_FILE_DIR,
    "schedule_file_path": SCHEDULE_DIR,
}
_BOM_FILE_FALLBACK_DIRS = (BOM_DIR,)
_BOM_REVISION_FALLBACK_DIRS = (BOM_HISTORY_DIR,)
_MERGE_DRAFT_FILE_FALLBACK_DIRS = (MERGE_DRAFT_DIR,)

# 常用料號低庫存提醒：關注清單（part_number 一律 UPPER(TRIM())）
LOW_STOCK_WATCH_SQL = """
CREATE TABLE IF NOT EXISTS low_stock_watch (
    part_number            TEXT PRIMARY KEY,
    name                   TEXT NOT NULL DEFAULT '',
    source                 TEXT NOT NULL DEFAULT 'auto',
    enabled                INTEGER NOT NULL DEFAULT 1,
    dismissed              INTEGER NOT NULL DEFAULT 0,
    note                   TEXT NOT NULL DEFAULT '',
    threshold_qty          REAL,
    threshold_source       TEXT NOT NULL DEFAULT 'auto',
    lead_time_days         REAL,
    lead_time_source       TEXT NOT NULL DEFAULT 'default',
    planned_lead_days      REAL,
    actual_lead_days       REAL,
    delay_days             REAL,
    delay_samples          INTEGER NOT NULL DEFAULT 0,
    avg_daily_usage        REAL,
    usage_qty              REAL,
    bill_count             INTEGER,
    last_purchase_date     TEXT NOT NULL DEFAULT '',
    last_purchase_supplier TEXT NOT NULL DEFAULT '',
    suggested_at           TEXT NOT NULL DEFAULT '',
    updated_at             TEXT NOT NULL DEFAULT '',
    updated_by             TEXT NOT NULL DEFAULT '',
    on_order_qty           REAL,
    next_expected_date     TEXT,
    on_order_synced_at     TEXT,
    on_order_note          TEXT
);
"""

# 舊 DB 補欄位用（欄位名, 型別）
LOW_STOCK_MIGRATION_COLUMNS = (
    ("on_order_qty", "REAL"),
    ("next_expected_date", "TEXT"),
    ("on_order_synced_at", "TEXT"),
    ("on_order_note", "TEXT"),
)

_CREATE_SQL = LOW_STOCK_WATCH_SQL + """
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

CREATE TABLE IF NOT EXISTS st_inventory_audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    part_number TEXT NOT NULL,
    old_qty     REAL,
    new_qty     REAL NOT NULL,
    delta       REAL,
    reason      TEXT NOT NULL DEFAULT '',
    actor       TEXT NOT NULL DEFAULT '',
    changed_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS st_package_breakdowns (
    part_number  TEXT PRIMARY KEY,
    package_text TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS st_dispatch_consumptions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    dispatch_session_id INTEGER NOT NULL REFERENCES dispatch_sessions(id) ON DELETE CASCADE,
    order_id            INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    part_number         TEXT    NOT NULL DEFAULT '',
    used_qty            REAL    NOT NULL DEFAULT 0,
    stock_before        REAL    NOT NULL DEFAULT 0,
    stock_after         REAL    NOT NULL DEFAULT 0,
    package_before      TEXT    NOT NULL DEFAULT '',
    package_after       TEXT    NOT NULL DEFAULT '',
    consumed_at         TEXT    NOT NULL DEFAULT '',
    rolled_back_at      TEXT    NOT NULL DEFAULT '',
    UNIQUE(dispatch_session_id, part_number)
);

CREATE TABLE IF NOT EXISTS st_reconcile_alignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aligned_at TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    source_filename TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    part_count INTEGER NOT NULL DEFAULT 0,
    diff_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS st_reconcile_alignment_parts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alignment_id INTEGER NOT NULL REFERENCES st_reconcile_alignments(id) ON DELETE CASCADE,
    part_number TEXT NOT NULL DEFAULT '',
    theoretical_qty REAL NOT NULL DEFAULT 0,
    physical_qty REAL NOT NULL DEFAULT 0,
    diff REAL NOT NULL DEFAULT 0,
    category TEXT NOT NULL DEFAULT '',
    aligned_qty REAL NOT NULL DEFAULT 0,
    UNIQUE(alignment_id, part_number)
);

CREATE TABLE IF NOT EXISTS st_reconcile_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alignment_id INTEGER NOT NULL REFERENCES st_reconcile_alignments(id) ON DELETE CASCADE,
    part_number TEXT NOT NULL DEFAULT '',
    adjust_qty REAL NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    actor TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS inventory_count_sessions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scope          TEXT NOT NULL DEFAULT 'st',
    cutoff_code    TEXT NOT NULL DEFAULT '',
    cutoff_at      TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'active',
    started_at     TEXT NOT NULL DEFAULT '',
    completed_at   TEXT NOT NULL DEFAULT '',
    cancelled_at   TEXT NOT NULL DEFAULT '',
    alignment_id   INTEGER REFERENCES st_reconcile_alignments(id),
    source_filename TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS main_file_periods (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    label                TEXT NOT NULL DEFAULT '',
    source_filename      TEXT NOT NULL DEFAULT '',
    main_file_path       TEXT NOT NULL DEFAULT '',
    previous_label       TEXT NOT NULL DEFAULT '',
    archived_main_path   TEXT NOT NULL DEFAULT '',
    database_backup_path TEXT NOT NULL DEFAULT '',
    started_at           TEXT NOT NULL DEFAULT ''
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
    is_manual     INTEGER NOT NULL DEFAULT 0,
    insert_before_code TEXT NOT NULL DEFAULT '',
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

CREATE TABLE IF NOT EXISTS bom_substitution_rules (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    model               TEXT NOT NULL DEFAULT '',
    old_part_number     TEXT NOT NULL DEFAULT '',
    new_part_number     TEXT NOT NULL DEFAULT '',
    new_per_old_ratio   REAL NOT NULL DEFAULT 1,
    strategy            TEXT NOT NULL DEFAULT 'old_first',
    effective_from_code TEXT NOT NULL DEFAULT '',
    note                TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'active',
    created_at          TEXT NOT NULL DEFAULT '',
    updated_at          TEXT NOT NULL DEFAULT '',
    ended_at            TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS order_substitution_allocations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    rule_id     INTEGER NOT NULL REFERENCES bom_substitution_rules(id) ON DELETE CASCADE,
    old_qty     REAL NOT NULL DEFAULT 0,
    new_qty     REAL NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL DEFAULT '',
    UNIQUE(order_id, rule_id)
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
    rolled_back_at  TEXT    NOT NULL DEFAULT '',
    main_period_id  INTEGER NOT NULL DEFAULT 0
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
    absorbed_by_alignment_id INTEGER NOT NULL DEFAULT 0,
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
    is_sample          INTEGER NOT NULL DEFAULT 0,
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
    effective_components_json TEXT NOT NULL DEFAULT '[]',
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

-- 從網站開到正航 ERP 的採購單本機紀錄（成功與失敗嘗試都記）
CREATE TABLE IF NOT EXISTS erp_purchase_orders (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_no       TEXT NOT NULL DEFAULT '',
    target_db     TEXT NOT NULL DEFAULT '',
    supplier_id   TEXT NOT NULL DEFAULT '',
    supplier_name TEXT NOT NULL DEFAULT '',
    remark        TEXT NOT NULL DEFAULT '',
    lines_json    TEXT NOT NULL DEFAULT '[]',
    sum_qty       REAL NOT NULL DEFAULT 0,
    sum_amount    REAL NOT NULL DEFAULT 0,
    sum_tax       REAL NOT NULL DEFAULT 0,
    total         REAL NOT NULL DEFAULT 0,
    actor         TEXT NOT NULL DEFAULT '',
    client_ip     TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'ok',
    error         TEXT NOT NULL DEFAULT ''
);

-- 網站開出的採購單每一行是為哪些訂單買的（僅記錄 / 顯示，不參與任何缺料扣減）
CREATE TABLE IF NOT EXISTS erp_po_line_allocations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    erp_po_id           INTEGER NOT NULL DEFAULT 0,
    bill_no             TEXT NOT NULL DEFAULT '',
    target_db           TEXT NOT NULL DEFAULT '',
    row_no              INTEGER NOT NULL DEFAULT 0,
    part_number         TEXT NOT NULL DEFAULT '',
    order_id            INTEGER NOT NULL DEFAULT 0,
    order_code          TEXT NOT NULL DEFAULT '',
    model               TEXT NOT NULL DEFAULT '',
    customer_po         TEXT NOT NULL DEFAULT '',
    ship_date           TEXT NOT NULL DEFAULT '',
    dispatch_session_id INTEGER NOT NULL DEFAULT 0,
    qty                 REAL NOT NULL DEFAULT 0,
    kind                TEXT NOT NULL DEFAULT 'manual',
    source              TEXT NOT NULL DEFAULT 'manual_po',
    created_at          TEXT NOT NULL DEFAULT ''
);

-- 缺料採購：料號「打件 / 後段」類別的人工指定（優先於自動判斷）
CREATE TABLE IF NOT EXISTS part_purchase_categories (
    part_number TEXT PRIMARY KEY,
    category    TEXT NOT NULL CHECK (category IN ('smt', 'backend')),
    updated_at  TEXT NOT NULL DEFAULT '',
    actor       TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sea_shipments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT NOT NULL DEFAULT '',
    source_path     TEXT NOT NULL DEFAULT '',
    customer        TEXT NOT NULL DEFAULT '',
    cust_po         TEXT NOT NULL DEFAULT '',
    shipment_date   TEXT NOT NULL DEFAULT '',
    delivery_date   TEXT NOT NULL DEFAULT '',
    maker           TEXT NOT NULL DEFAULT 'Andy',
    mark_text       TEXT NOT NULL DEFAULT 'HILLIARD',
    invoice_no      TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sea_shipment_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    shipment_id     INTEGER NOT NULL REFERENCES sea_shipments(id) ON DELETE CASCADE,
    line_no         INTEGER NOT NULL DEFAULT 0,
    order_date      TEXT NOT NULL DEFAULT '',
    customer        TEXT NOT NULL DEFAULT '',
    cust_po         TEXT NOT NULL DEFAULT '',
    item_no         TEXT NOT NULL DEFAULT '',
    description     TEXT NOT NULL DEFAULT '',
    price           REAL NOT NULL DEFAULT 0,
    qty             REAL NOT NULL DEFAULT 0,
    delivery_date   TEXT NOT NULL DEFAULT '',
    packing_name    TEXT NOT NULL DEFAULT '',
    per_box_qty     REAL NOT NULL DEFAULT 0,
    net_weight      REAL NOT NULL DEFAULT 0,
    gross_weight    REAL NOT NULL DEFAULT 0,
    volume          REAL NOT NULL DEFAULT 0,
    box_count       INTEGER NOT NULL DEFAULT 0,
    tail_qty        REAL NOT NULL DEFAULT 0,
    carton_no       TEXT NOT NULL DEFAULT '',
    harmonized_code TEXT NOT NULL DEFAULT '',
    match_status    TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sea_harmonized_codes (
    item_no          TEXT PRIMARY KEY,
    harmonized_code TEXT NOT NULL DEFAULT '',
    note            TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sea_packing_specs (
    item_no       TEXT PRIMARY KEY,
    packing_name  TEXT NOT NULL DEFAULT '',
    per_box_qty   REAL NOT NULL DEFAULT 0,
    net_weight    REAL NOT NULL DEFAULT 0,
    gross_weight  REAL NOT NULL DEFAULT 0,
    volume        REAL NOT NULL DEFAULT 0,
    vendor        TEXT NOT NULL DEFAULT '',
    updated_at    TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_delivery ON orders(delivery_date);
CREATE INDEX IF NOT EXISTS idx_st_inventory_audit_part_changed ON st_inventory_audit_log(part_number, changed_at);
CREATE INDEX IF NOT EXISTS idx_st_dispatch_consumptions_session ON st_dispatch_consumptions(dispatch_session_id, rolled_back_at);
CREATE INDEX IF NOT EXISTS idx_st_dispatch_consumptions_order ON st_dispatch_consumptions(order_id, rolled_back_at);
CREATE INDEX IF NOT EXISTS idx_st_reconcile_align_at ON st_reconcile_alignments(aligned_at);
CREATE INDEX IF NOT EXISTS idx_st_reconcile_parts_align ON st_reconcile_alignment_parts(alignment_id, part_number);
CREATE INDEX IF NOT EXISTS idx_bom_comp_file ON bom_components(bom_file_id);
CREATE INDEX IF NOT EXISTS idx_bom_revisions_file ON bom_revisions(bom_file_id, revision_number);
CREATE INDEX IF NOT EXISTS idx_bom_substitution_active ON bom_substitution_rules(status, model, old_part_number);
CREATE INDEX IF NOT EXISTS idx_order_substitution_order ON order_substitution_allocations(order_id, rule_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_order ON dispatch_records(order_id);
CREATE INDEX IF NOT EXISTS idx_erp_po_alloc_po ON erp_po_line_allocations(erp_po_id, row_no);
CREATE INDEX IF NOT EXISTS idx_erp_po_alloc_bill ON erp_po_line_allocations(bill_no, target_db);
CREATE INDEX IF NOT EXISTS idx_erp_po_alloc_order ON erp_po_line_allocations(order_id, part_number);
CREATE INDEX IF NOT EXISTS idx_dispatch_records_part_at ON dispatch_records(UPPER(TRIM(part_number)), dispatched_at);
CREATE INDEX IF NOT EXISTS idx_dispatch_sessions_order ON dispatch_sessions(order_id, rolled_back_at, id);
CREATE INDEX IF NOT EXISTS idx_decisions_order ON decisions(order_id);
CREATE INDEX IF NOT EXISTS idx_order_supplements_order ON order_supplements(order_id);
CREATE INDEX IF NOT EXISTS idx_purchase_reminder_notified ON purchase_reminder_statuses(notified, updated_at);
CREATE INDEX IF NOT EXISTS idx_merge_drafts_order_status ON merge_drafts(order_id, status, id);
CREATE INDEX IF NOT EXISTS idx_merge_draft_files_draft ON merge_draft_files(draft_id, id);
CREATE INDEX IF NOT EXISTS idx_alerts_read ON alerts(is_read);
CREATE INDEX IF NOT EXISTS idx_sea_items_shipment ON sea_shipment_items(shipment_id, line_no);
CREATE INDEX IF NOT EXISTS idx_sea_packing_name ON sea_packing_specs(packing_name);

CREATE TABLE IF NOT EXISTS defective_batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT    NOT NULL DEFAULT '',
    imported_at     TEXT    NOT NULL DEFAULT '',
    note            TEXT    NOT NULL DEFAULT '',
    main_file_mtime REAL    NOT NULL DEFAULT 0,
    main_period_id  INTEGER NOT NULL DEFAULT 0
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
    ,absorbed_by_alignment_id INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_defective_status ON defective_records(status);
CREATE INDEX IF NOT EXISTS idx_defective_order ON defective_records(order_id);
CREATE TABLE IF NOT EXISTS defective_deleted_history (
    record_id INTEGER PRIMARY KEY,
    part_number TEXT NOT NULL,
    defective_qty REAL NOT NULL,
    created_at TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    reversed_at TEXT NOT NULL DEFAULT '',
    reversal_id TEXT NOT NULL DEFAULT '',
    main_period_id INTEGER NOT NULL DEFAULT 0
);
-- 轉換料 A→B：每次轉換的紀錄與還原用快照（只改本網站 BOM，不碰正航）
CREATE TABLE IF NOT EXISTS part_conversions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    part_a        TEXT NOT NULL DEFAULT '',
    part_b        TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'active',
    affected_json TEXT NOT NULL DEFAULT '[]',
    snapshot_json TEXT NOT NULL DEFAULT '{}',
    actor         TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT '',
    restored_at   TEXT NOT NULL DEFAULT '',
    restored_by   TEXT NOT NULL DEFAULT ''
);
-- 目前生效中的轉換（A 已停用，需求與庫存併入 B）
CREATE TABLE IF NOT EXISTS part_conversion_active (
    part_a        TEXT PRIMARY KEY,
    part_b        TEXT NOT NULL DEFAULT '',
    conversion_id INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL DEFAULT ''
);

-- 客人改交期 → 廠商提前清單：每一行（採購單項次 / 未下單料號）的詢問狀態
CREATE TABLE IF NOT EXISTS po_pullin_items (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    line_key       TEXT NOT NULL UNIQUE,
    supplier_id    TEXT NOT NULL DEFAULT '',
    supplier_name  TEXT NOT NULL DEFAULT '',
    bill_no        TEXT NOT NULL DEFAULT '',
    row_no         INTEGER NOT NULL DEFAULT 0,
    part_number    TEXT NOT NULL DEFAULT '',
    qty            REAL NOT NULL DEFAULT 0,
    po_date        TEXT NOT NULL DEFAULT '',
    requested_date TEXT NOT NULL DEFAULT '',
    days_earlier   INTEGER NOT NULL DEFAULT 0,
    order_ids_json TEXT NOT NULL DEFAULT '[]',
    status         TEXT NOT NULL DEFAULT '未詢問',
    note           TEXT NOT NULL DEFAULT '',
    actor          TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL DEFAULT '',
    updated_at     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_po_pullin_status ON po_pullin_items(status);
-- 鈞洋 後段加工對應：基礎件（如 OC-10988B）買了之後要另外給加工廠印刷的加工件（如 SP-10024B）
-- status: confirmed（已確認，enabled=1 時生效）/ pending（自動建議，待確認，不生效）/ rejected（已駁回，避免重複建議）
CREATE TABLE IF NOT EXISTS postprocess_links (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    base_part    TEXT NOT NULL,
    process_part TEXT NOT NULL,
    ratio        REAL NOT NULL DEFAULT 1,
    enabled      INTEGER NOT NULL DEFAULT 1,
    status       TEXT NOT NULL DEFAULT 'confirmed',
    source       TEXT NOT NULL DEFAULT 'manual',
    evidence     TEXT NOT NULL DEFAULT '',
    note         TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL DEFAULT '',
    UNIQUE(base_part, process_part)
);
CREATE INDEX IF NOT EXISTS idx_postprocess_links_base ON postprocess_links(base_part);
CREATE INDEX IF NOT EXISTS idx_dispatch_records_at ON dispatch_records(dispatched_at);
CREATE INDEX IF NOT EXISTS idx_inventory_count_sessions_status ON inventory_count_sessions(scope, status, id);
"""


def _now() -> str:
    return local_now().isoformat()


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


def _migrate_low_stock_columns(conn) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(low_stock_watch)").fetchall()}
    for name, col_type in LOW_STOCK_MIGRATION_COLUMNS:
        if name not in cols:
            conn.execute(f"ALTER TABLE low_stock_watch ADD COLUMN {name} {col_type}")


def ensure_low_stock_table() -> None:
    """低庫存關注清單表（冪等；供 init_db 以外的一次性腳本呼叫）。"""
    with get_conn() as conn:
        conn.executescript(LOW_STOCK_WATCH_SQL)
        _migrate_low_stock_columns(conn)


def init_db():
    """建立所有表（若不存在）+ migration。"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(_CREATE_SQL)
        _migrate_low_stock_columns(conn)
        # migration: orders 加 folder 欄位
        cols = [r[1] for r in conn.execute("PRAGMA table_info(orders)").fetchall()]
        if "is_manual" not in cols:
            conn.execute("ALTER TABLE orders ADD COLUMN is_manual INTEGER NOT NULL DEFAULT 0")
        if "insert_before_code" not in cols:
            conn.execute("ALTER TABLE orders ADD COLUMN insert_before_code TEXT NOT NULL DEFAULT ''")
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
        if draft_cols and "is_sample" not in draft_cols:
            conn.execute("ALTER TABLE merge_drafts ADD COLUMN is_sample INTEGER NOT NULL DEFAULT 0")
        draft_file_cols = [r[1] for r in conn.execute("PRAGMA table_info(merge_draft_files)").fetchall()]
        if draft_file_cols and "effective_components_json" not in draft_file_cols:
            conn.execute("ALTER TABLE merge_draft_files ADD COLUMN effective_components_json TEXT NOT NULL DEFAULT '[]'")
        dispatch_session_cols = [r[1] for r in conn.execute("PRAGMA table_info(dispatch_sessions)").fetchall()]
        if dispatch_session_cols and "main_period_id" not in dispatch_session_cols:
            conn.execute("ALTER TABLE dispatch_sessions ADD COLUMN main_period_id INTEGER NOT NULL DEFAULT 0")
        supplement_cols = [r[1] for r in conn.execute("PRAGMA table_info(order_supplements)").fetchall()]
        if supplement_cols and "note" not in supplement_cols:
            conn.execute("ALTER TABLE order_supplements ADD COLUMN note TEXT NOT NULL DEFAULT ''")
        if supplement_cols and "absorbed_by_alignment_id" not in supplement_cols:
            conn.execute("ALTER TABLE order_supplements ADD COLUMN absorbed_by_alignment_id INTEGER NOT NULL DEFAULT 0")
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
        if def_cols and "absorbed_by_alignment_id" not in def_cols:
            conn.execute("ALTER TABLE defective_records ADD COLUMN absorbed_by_alignment_id INTEGER NOT NULL DEFAULT 0")
        if def_cols and "batch_id" in [r[1] for r in conn.execute("PRAGMA table_info(defective_records)").fetchall()]:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_defective_batch ON defective_records(batch_id)")

        # defective_batches: 加 main_file_mtime 欄位
        batch_cols = [r[1] for r in conn.execute("PRAGMA table_info(defective_batches)").fetchall()]
        if batch_cols and "main_file_mtime" not in batch_cols:
            conn.execute("ALTER TABLE defective_batches ADD COLUMN main_file_mtime REAL NOT NULL DEFAULT 0")
        if batch_cols and "main_period_id" not in batch_cols:
            conn.execute("ALTER TABLE defective_batches ADD COLUMN main_period_id INTEGER NOT NULL DEFAULT 0")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_order_supplements_absorbed ON order_supplements(absorbed_by_alignment_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_defective_absorbed ON defective_records(absorbed_by_alignment_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_sessions_period ON dispatch_sessions(main_period_id, rolled_back_at, id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_defective_batches_period ON defective_batches(main_period_id, id)")

        seed_postprocess_links(conn)

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


def save_st_inventory_snapshot(
    stock: dict[str, float],
    descriptions: dict[str, str] | None = None,
    actor: str = "system",
):
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
        existing_rows = conn.execute("SELECT part_number, stock_qty FROM st_inventory_snapshot").fetchall()
        old_stock = {str(row["part_number"]): float(row["stock_qty"] or 0) for row in existing_rows}
        conn.execute("DELETE FROM st_inventory_snapshot")
        for part in all_parts:
            conn.execute(
                "INSERT INTO st_inventory_snapshot(part_number, stock_qty, description, loaded_at) VALUES(?,?,?,?)",
                (part, normalized_stock.get(part, 0.0), normalized_desc.get(part, ""), loaded_at),
            )
        audit_parts = sorted(set(old_stock) | set(normalized_stock))
        for part in audit_parts:
            old_qty = old_stock.get(part)
            new_qty = normalized_stock.get(part, 0.0)
            if old_qty is None or old_qty != new_qty:
                _insert_st_inventory_audit_log(
                    conn,
                    part_number=part,
                    old_qty=old_qty,
                    new_qty=new_qty,
                    reason="st_inventory_upload",
                    actor=actor,
                    changed_at=loaded_at,
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


def get_latest_st_reconcile_anchor(
    cutoff_at: str,
    part_numbers: list[str] | None = None,
) -> dict | None:
    """取得 cutoff 前最近一次 ST 停損點對齊 baseline。"""
    cutoff = str(cutoff_at or "").strip()
    if not cutoff:
        return None
    normalized_parts = [
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ]
    normalized_parts = list(dict.fromkeys(normalized_parts))

    with get_conn() as conn:
        alignment = conn.execute(
            """
            SELECT id, aligned_at, committed_at, source_filename, note, part_count, diff_count
            FROM st_reconcile_alignments
            WHERE aligned_at<=?
            ORDER BY aligned_at DESC, id DESC
            LIMIT 1
            """,
            (cutoff,),
        ).fetchone()
        if not alignment:
            return None

        sql = """
            SELECT part_number, aligned_qty, theoretical_qty, physical_qty, diff, category
            FROM st_reconcile_alignment_parts
            WHERE alignment_id=?
        """
        params: list[object] = [int(alignment["id"])]
        if normalized_parts:
            placeholders = ",".join("?" * len(normalized_parts))
            sql += f" AND part_number IN ({placeholders})"
            params.extend(normalized_parts)
        rows = conn.execute(sql, params).fetchall()

    baseline = {
        str(row["part_number"] or "").strip().upper(): float(row["aligned_qty"] or 0)
        for row in rows
        if str(row["part_number"] or "").strip()
    }
    return {
        "id": int(alignment["id"]),
        "aligned_at": str(alignment["aligned_at"] or ""),
        "committed_at": str(alignment["committed_at"] or ""),
        "source_filename": str(alignment["source_filename"] or ""),
        "note": str(alignment["note"] or ""),
        "part_count": int(alignment["part_count"] or 0),
        "diff_count": int(alignment["diff_count"] or 0),
        "baseline_qty": baseline,
    }


def capture_inventory_snapshot_state() -> dict:
    """完整保存主檔快照，供跨檔案/DB 寫入失敗時精確補償。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT part_number, stock_qty, moq, moq_manual, description, snapshot_at "
            "FROM inventory_snapshot ORDER BY part_number"
        ).fetchall()
        setting = conn.execute(
            "SELECT value FROM settings WHERE key='main_part_count'"
        ).fetchone()
    return {
        "rows": [dict(row) for row in rows],
        "main_part_count_exists": setting is not None,
        "main_part_count": str(setting["value"] or "") if setting else "",
    }


def restore_inventory_snapshot_state(state: dict) -> None:
    """逐欄還原 capture_inventory_snapshot_state 的內容與相關設定。"""
    rows = list((state or {}).get("rows") or [])
    with get_conn() as conn:
        conn.execute("DELETE FROM inventory_snapshot")
        conn.executemany(
            "INSERT INTO inventory_snapshot("
            "part_number, stock_qty, moq, moq_manual, description, snapshot_at"
            ") VALUES(?,?,?,?,?,?)",
            [
                (
                    str(row.get("part_number") or ""),
                    float(row.get("stock_qty") or 0),
                    float(row.get("moq") or 0),
                    int(row.get("moq_manual") or 0),
                    str(row.get("description") or ""),
                    str(row.get("snapshot_at") or ""),
                )
                for row in rows
            ],
        )
        if (state or {}).get("main_part_count_exists"):
            conn.execute(
                "INSERT INTO settings(key, value) VALUES('main_part_count', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str((state or {}).get("main_part_count") or ""),),
            )
        else:
            conn.execute("DELETE FROM settings WHERE key='main_part_count'")


def get_latest_st_reconcile_anchors(
    cutoff_at: str,
    part_numbers: list[str] | None = None,
) -> dict[str, dict]:
    """逐料號取得 cutoff 前最近一次停損基準，避免局部盤點截斷其他料號。"""
    cutoff = str(cutoff_at or "").strip()
    if not cutoff:
        return {}
    normalized_parts = list(dict.fromkeys(
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ))
    sql = """
        SELECT a.id AS alignment_id, a.aligned_at, a.committed_at,
               p.part_number, p.aligned_qty
        FROM st_reconcile_alignment_parts p
        JOIN st_reconcile_alignments a ON a.id=p.alignment_id
        WHERE a.aligned_at<=?
    """
    params: list[object] = [cutoff]
    if normalized_parts:
        placeholders = ",".join("?" * len(normalized_parts))
        sql += f" AND p.part_number IN ({placeholders})"
        params.extend(normalized_parts)
    sql += " ORDER BY p.part_number, a.aligned_at DESC, a.id DESC"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    result: dict[str, dict] = {}
    for row in rows:
        part = str(row["part_number"] or "").strip().upper()
        if not part or part in result:
            continue
        result[part] = {
            "alignment_id": int(row["alignment_id"]),
            "aligned_at": str(row["aligned_at"] or ""),
            "committed_at": str(row["committed_at"] or ""),
            "baseline_qty": float(row["aligned_qty"] or 0),
        }
    return result


def get_active_inventory_count_session(scope: str = "st") -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM inventory_count_sessions WHERE scope=? AND status='active' ORDER BY id DESC LIMIT 1",
            (str(scope or "st"),),
        ).fetchone()
    return dict(row) if row else None


def start_inventory_count_session(*, cutoff_at: str, cutoff_code: str = "", scope: str = "st") -> dict:
    normalized_scope = str(scope or "st").strip() or "st"
    existing = get_active_inventory_count_session(normalized_scope)
    if existing:
        return existing
    now = _now()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO inventory_count_sessions(scope, cutoff_code, cutoff_at, status, started_at) "
            "VALUES(?,?,?,'active',?)",
            (normalized_scope, str(cutoff_code or "").strip(), str(cutoff_at or "").strip(), now),
        )
        session_id = int(cur.lastrowid)
    return {
        "id": session_id,
        "scope": normalized_scope,
        "cutoff_code": str(cutoff_code or "").strip(),
        "cutoff_at": str(cutoff_at or "").strip(),
        "status": "active",
        "started_at": now,
        "completed_at": "",
        "cancelled_at": "",
        "alignment_id": None,
        "source_filename": "",
    }


def get_current_main_period_id() -> int:
    try:
        return max(0, int(get_setting("main_file_period_id", "0") or 0))
    except (TypeError, ValueError):
        return 0


def create_main_file_period(
    *,
    label: str,
    source_filename: str,
    main_file_path: str,
    previous_label: str = "",
    archived_main_path: str = "",
    database_backup_path: str = "",
    started_at: str | None = None,
) -> dict:
    normalized_label = str(label or "").strip()
    if not normalized_label:
        raise ValueError("年度標籤不可空白")
    timestamp = started_at or _now()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO main_file_periods(label, source_filename, main_file_path, previous_label, "
            "archived_main_path, database_backup_path, started_at) VALUES(?,?,?,?,?,?,?)",
            (
                normalized_label,
                str(source_filename or "").strip(),
                str(main_file_path or "").strip(),
                str(previous_label or "").strip(),
                str(archived_main_path or "").strip(),
                str(database_backup_path or "").strip(),
                timestamp,
            ),
        )
        period_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO settings(key, value) VALUES('main_file_period_id', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(period_id),),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES('main_file_period_label', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (normalized_label,),
        )
        row = conn.execute("SELECT * FROM main_file_periods WHERE id=?", (period_id,)).fetchone()
    return dict(row) if row else {"id": period_id, "label": normalized_label}


def list_main_file_periods() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM main_file_periods ORDER BY id DESC").fetchall()
    return [dict(row) for row in rows]


def get_main_file_period(period_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM main_file_periods WHERE id=?", (int(period_id),)).fetchone()
    return dict(row) if row else None


def finish_inventory_count_session(
    session_id: int,
    *,
    status: str,
    alignment_id: int | None = None,
    source_filename: str = "",
    undo_receipt: dict | None = None,
) -> bool:
    normalized_status = str(status or "").strip()
    if normalized_status not in {"completed", "cancelled"}:
        raise ValueError("盤點工作階段狀態不正確")
    now = _now()
    time_column = "completed_at" if normalized_status == "completed" else "cancelled_at"
    with get_conn() as conn:
        cur = conn.execute(
            f"UPDATE inventory_count_sessions SET status=?, {time_column}=?, alignment_id=?, source_filename=? "
            "WHERE id=? AND status='active'",
            (normalized_status, now, alignment_id, str(source_filename or ""), int(session_id)),
        )
        if cur.rowcount and undo_receipt is not None:
            conn.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                         (f"main_count_undo_{session_id}", json.dumps(undo_receipt, ensure_ascii=False)))
    return bool(cur.rowcount)


def main_count_undo_state(conn) -> dict:
    """盤點撤回的 DB 異動指紋來源；不含快照與唯讀操作。"""
    return {table: [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY {'record_id' if table == 'defective_deleted_history' else 'id'}")]
            for table in ('defective_records', 'defective_batches', 'dispatch_sessions', 'dispatch_records', 'defective_deleted_history')}


def apply_main_count_undo(conn, session_id: int, receipt: dict, stock: dict, moq: dict) -> None:
    """與呼叫端同一交易更新快照、撤回憑證及稽核，不修改原盤點歷史。"""
    old = {r['part_number']: dict(r) for r in conn.execute('SELECT * FROM inventory_snapshot')}
    now = _now()
    conn.execute('DELETE FROM inventory_snapshot')
    for part in sorted(set(stock) | set(moq)):
        prior = old.get(part, {})
        manual = int(prior.get('moq_manual') or 0)
        conn.execute('INSERT INTO inventory_snapshot(part_number,stock_qty,moq,moq_manual,description,snapshot_at) VALUES(?,?,?,?,?,?)',
                     (part, stock.get(part, 0), prior.get('moq', 0) if manual else moq.get(part, 0), manual, prior.get('description', ''), now))
    receipt['restored_at'] = now
    for key, value in ((f'main_count_undo_{session_id}', json.dumps(receipt, ensure_ascii=False)), ('main_part_count', str(len(stock)))):
        conn.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))
    conn.execute('INSERT INTO activity_logs(action,detail,created_at) VALUES(?,?,?)',
                 ('inventory_count_undone', f"撤回主檔盤點 #{session_id}；備份={receipt['backup_name']}；撤回前備份={receipt['safety_backup_name']}；還原SHA256={receipt['backup_sha256']}", now))


def mark_inventory_history_absorbed(
    alignment_id: int,
    cutoff_at: str,
    part_numbers: list[str],
    *,
    defective_record_ids: list[int] | None = None,
) -> dict[str, int]:
    """將已完成且落在盤點切點前的異動封存；未完成缺料不會被吸收。"""
    parts = list(dict.fromkeys(
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ))
    if not parts:
        return {"defective_records": 0, "supplements": 0}
    placeholders = ",".join("?" * len(parts))
    with get_conn() as conn:
        if defective_record_ids is None:
            defective = conn.execute(
                f"UPDATE defective_records SET absorbed_by_alignment_id=? "
                f"WHERE absorbed_by_alignment_id=0 AND created_at<=? AND UPPER(TRIM(part_number)) IN ({placeholders})",
                [int(alignment_id), str(cutoff_at or "")] + parts,
            )
        else:
            ids = list(dict.fromkeys(int(value) for value in defective_record_ids))
            id_placeholders = ','.join('?' for _ in ids) or 'NULL'
            defective = conn.execute(
                f"UPDATE defective_records SET absorbed_by_alignment_id=? "
                f"WHERE absorbed_by_alignment_id=0 AND id IN ({id_placeholders}) "
                f"AND UPPER(TRIM(part_number)) IN ({placeholders})",
                [int(alignment_id)] + ids + parts,
            )
        supplements = conn.execute(
            f"UPDATE order_supplements SET absorbed_by_alignment_id=? "
            f"WHERE absorbed_by_alignment_id=0 AND updated_at<=? "
            f"AND UPPER(TRIM(part_number)) IN ({placeholders}) "
            "AND order_id IN (SELECT id FROM orders WHERE status IN ('dispatched','completed'))",
            [int(alignment_id), str(cutoff_at or "")] + parts,
        )
    return {
        "defective_records": int(defective.rowcount or 0),
        "supplements": int(supplements.rowcount or 0),
    }


def get_latest_merge_draft_updated_at() -> str:
    """最近一次副檔異動時間（calc-preview 快取失效判斷用）。"""
    with get_conn() as conn:
        row = conn.execute("SELECT MAX(updated_at) AS latest FROM merge_drafts").fetchone()
    return str(row["latest"] or "") if row else ""


def get_defective_part_totals(cutoff_at: str, after_at: str = "") -> list[dict]:
    """統計時間窗內不良品/加工多打扣帳的料號彙總（盤點覆蓋盲區偵測用）。"""
    cutoff = str(cutoff_at or "").strip()
    if not cutoff:
        return []
    sql = """
        SELECT UPPER(TRIM(part_number)) AS part_number,
               SUM(defective_qty) AS total_qty,
               COUNT(*) AS record_count,
               MAX(created_at) AS last_at
        FROM defective_records
        WHERE created_at<=? AND TRIM(IFNULL(part_number,'')) <> ''
    """
    params: list[object] = [cutoff]
    window_start = str(after_at or "").strip()
    if window_start:
        sql += " AND created_at>?"
        params.append(window_start)
    from .services.inventory_restore_guard import get_count_absorbed_record_ids
    absorbed_ids = sorted(get_count_absorbed_record_ids())
    if absorbed_ids:
        sql += f" AND id NOT IN ({','.join('?' for _ in absorbed_ids)})"
        params.extend(absorbed_ids)
    sql += " GROUP BY UPPER(TRIM(part_number)) ORDER BY SUM(defective_qty) DESC, part_number"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        {
            "part_number": str(row["part_number"] or ""),
            "total_qty": float(row["total_qty"] or 0),
            "record_count": int(row["record_count"] or 0),
            "last_at": str(row["last_at"] or ""),
        }
        for row in rows
    ]


def get_defective_interval_parts(cutoff_at: str, count_at: str, *, reversal_ids: set[str] | None = None) -> dict[str, dict]:
    """盤點日期內有效扣帳；含已吸收列，讓同一盤點重算結果保持一致。"""
    period_id = get_current_main_period_id()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, part_number, defective_qty FROM defective_records "
            "WHERE created_at>? AND created_at<=? AND status IN ('open','confirmed','closed') "
            "AND defective_qty>0 ORDER BY id",
            (cutoff_at, count_at),
        ).fetchall()
        history = conn.execute(
            "SELECT * FROM defective_deleted_history WHERE main_period_id=?",
            (period_id,),
        ).fetchall()
    result: dict[str, dict] = {}
    from .services.inventory_restore_guard import get_count_absorbed_record_ids
    absorbed_ids = get_count_absorbed_record_ids()
    for row in rows:
        if int(row['id']) in absorbed_ids:
            continue
        part = str(row['part_number'] or '').strip().upper()
        item = result.setdefault(part, {'defect_delta': 0.0, 'defective_record_ids': []})
        item['defect_delta'] -= float(row['defective_qty'])
        item['defective_record_ids'].append(int(row['id']))
    for row in history:
        part = str(row['part_number'] or '').strip().upper()
        item = result.setdefault(part, {'defect_delta': 0.0, 'defective_record_ids': []})
        if cutoff_at < row['created_at'] <= count_at:
            item['defect_delta'] -= float(row['defective_qty'])
            item['defective_record_ids'].append(int(row['record_id']))
        if (row['reversal_id'] in (reversal_ids or set())
                and cutoff_at < row['reversed_at'] <= count_at):
            item['defect_delta'] += float(row['defective_qty'])
    return result


def get_defective_reversal_history() -> dict[str, dict[str, float]]:
    """供主檔回復事件核對；未留歷史的舊回復不能猜測原扣帳日期。"""
    period_id = get_current_main_period_id()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT reversal_id, part_number, SUM(defective_qty) AS qty "
            "FROM defective_deleted_history WHERE reversal_id<>'' AND main_period_id=? "
            "GROUP BY reversal_id, part_number", (period_id,),
        ).fetchall()
    result: dict[str, dict[str, float]] = {}
    for row in rows:
        result.setdefault(row['reversal_id'], {})[row['part_number']] = float(row['qty'])
    return result


def create_st_reconcile_alignment(
    *,
    aligned_at: str,
    source_filename: str,
    note: str,
    parts: list[dict],
    adjustments: list[dict],
) -> int:
    committed_at = _now()
    safe_parts = parts or []
    diff_count = sum(1 for row in safe_parts if abs(float(row.get("diff") or 0)) > 1e-6)
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO st_reconcile_alignments(
                aligned_at, committed_at, source_filename, note, part_count, diff_count
            )
            VALUES(?,?,?,?,?,?)
            """,
            (
                str(aligned_at or ""),
                committed_at,
                str(source_filename or ""),
                str(note or ""),
                len(safe_parts),
                diff_count,
            ),
        )
        alignment_id = int(cur.lastrowid)
        for row in safe_parts:
            conn.execute(
                """
                INSERT INTO st_reconcile_alignment_parts(
                    alignment_id, part_number, theoretical_qty, physical_qty, diff, category, aligned_qty
                )
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    alignment_id,
                    str(row.get("part_number") or "").strip().upper(),
                    float(row.get("theoretical_qty") or row.get("theoretical") or 0),
                    float(row.get("physical_qty") or row.get("physical") or 0),
                    float(row.get("diff") or 0),
                    str(row.get("category") or ""),
                    float(row["aligned_qty"] if row.get("aligned_qty") is not None else row.get("physical_qty") or row.get("physical") or 0),
                ),
            )
        for row in adjustments or []:
            conn.execute(
                """
                INSERT INTO st_reconcile_adjustments(
                    alignment_id, part_number, adjust_qty, reason, actor, created_at
                )
                VALUES(?,?,?,?,?,?)
                """,
                (
                    alignment_id,
                    str(row.get("part_number") or "").strip().upper(),
                    float(row.get("adjust_qty") or 0),
                    str(row.get("reason") or ""),
                    str(row.get("actor") or ""),
                    committed_at,
                ),
            )
    return alignment_id


def _insert_st_inventory_audit_log(
    conn: sqlite3.Connection,
    *,
    part_number: str,
    old_qty: float | None,
    new_qty: float,
    reason: str,
    actor: str,
    changed_at: str,
):
    delta = None if old_qty is None else round(float(new_qty) - float(old_qty), 6)
    conn.execute(
        """
        INSERT INTO st_inventory_audit_log(part_number, old_qty, new_qty, delta, reason, actor, changed_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            part_number,
            old_qty,
            new_qty,
            delta,
            str(reason or ""),
            str(actor or ""),
            changed_at,
        ),
    )


def update_st_inventory_stock(stock_updates: dict[str, float], reason: str = "", actor: str = "system") -> int:
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
                "SELECT stock_qty, description FROM st_inventory_snapshot WHERE part_number=?",
                (part,),
            ).fetchone()
            old_qty = float(existing["stock_qty"]) if existing else None
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
            if old_qty is None or old_qty != qty:
                _insert_st_inventory_audit_log(
                    conn,
                    part_number=part,
                    old_qty=old_qty,
                    new_qty=qty,
                    reason=reason,
                    actor=actor,
                    changed_at=loaded_at,
                )
            updated += 1
    return updated


def get_st_inventory_audit_log(part_number: str | None = None, limit: int = 200) -> list[dict]:
    normalized_part = str(part_number or "").strip().upper()
    safe_limit = max(1, min(int(limit or 200), 1000))
    sql = """
        SELECT id, part_number, old_qty, new_qty, delta, reason, actor, changed_at
        FROM st_inventory_audit_log
    """
    params: list[object] = []
    if normalized_part:
        sql += " WHERE part_number=?"
        params.append(normalized_part)
    sql += " ORDER BY changed_at DESC, id DESC LIMIT ?"
    params.append(safe_limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        {
            "id": int(row["id"]),
            "part_number": str(row["part_number"] or ""),
            "old_qty": None if row["old_qty"] is None else float(row["old_qty"]),
            "new_qty": float(row["new_qty"] or 0),
            "delta": None if row["delta"] is None else float(row["delta"]),
            "reason": str(row["reason"] or ""),
            "actor": str(row["actor"] or ""),
            "changed_at": str(row["changed_at"] or ""),
        }
        for row in rows
    ]


def get_st_inventory_upload_baselines(
    cutoff_at: str,
    part_numbers: list[str] | None = None,
) -> dict[str, dict]:
    """取得每個料號在 cutoff 前最近一次 ST 盤點上傳數量。"""
    cutoff = str(cutoff_at or "").strip()
    if not cutoff:
        return {}
    normalized_parts = [
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ]
    normalized_parts = list(dict.fromkeys(normalized_parts))

    sql = """
        SELECT id, part_number, new_qty, changed_at
        FROM st_inventory_audit_log
        WHERE reason='st_inventory_upload'
          AND changed_at<=?
    """
    params: list[object] = [cutoff]
    if normalized_parts:
        placeholders = ",".join("?" * len(normalized_parts))
        sql += f" AND part_number IN ({placeholders})"
        params.extend(normalized_parts)
    sql += " ORDER BY part_number, changed_at DESC, id DESC"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    baselines: dict[str, dict] = {}
    for row in rows:
        part = str(row["part_number"] or "").strip().upper()
        if not part or part in baselines:
            continue
        baselines[part] = {
            "part_number": part,
            "baseline_qty": float(row["new_qty"] or 0),
            "aligned_at": str(row["changed_at"] or ""),
            "audit_log_id": int(row["id"]),
        }
    return baselines


def get_st_inventory_audit_deltas(
    cutoff_at: str,
    *,
    after_at: str | None = None,
    part_numbers: list[str] | None = None,
    exclude_reason: str = ST_RECONCILE_ADJUSTMENT_REASON,
) -> list[dict]:
    """取得截止日前 ST audit delta；after_at 為開區間。"""
    cutoff = str(cutoff_at or "").strip()
    if not cutoff:
        return []
    normalized_parts = [
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ]
    normalized_parts = list(dict.fromkeys(normalized_parts))

    sql = """
        SELECT id, part_number, old_qty, new_qty, delta, reason, actor, changed_at
        FROM st_inventory_audit_log
        WHERE changed_at<=?
          AND delta IS NOT NULL
    """
    params: list[object] = [cutoff]
    if after_at:
        sql += " AND changed_at>?"
        params.append(str(after_at))
    if exclude_reason:
        sql += " AND reason<>?"
        params.append(str(exclude_reason))
    if normalized_parts:
        placeholders = ",".join("?" * len(normalized_parts))
        sql += f" AND part_number IN ({placeholders})"
        params.extend(normalized_parts)
    sql += " ORDER BY changed_at, id"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        {
            "id": int(row["id"]),
            "part_number": str(row["part_number"] or "").strip().upper(),
            "old_qty": None if row["old_qty"] is None else float(row["old_qty"]),
            "new_qty": float(row["new_qty"] or 0),
            "delta": float(row["delta"] or 0),
            "reason": str(row["reason"] or ""),
            "actor": str(row["actor"] or ""),
            "changed_at": str(row["changed_at"] or ""),
        }
        for row in rows
    ]


def get_st_dispatch_consumptions_as_of(
    cutoff_at: str,
    part_numbers: list[str] | None = None,
) -> list[dict]:
    """取得截止日時仍有效的 ST 發料消耗明細。"""
    cutoff = str(cutoff_at or "").strip()
    if not cutoff:
        return []
    normalized_parts = [
        str(part).strip().upper()
        for part in (part_numbers or [])
        if str(part).strip()
    ]
    normalized_parts = list(dict.fromkeys(normalized_parts))

    sql = """
        SELECT
            id, dispatch_session_id, order_id, part_number, used_qty,
            stock_before, stock_after, package_before, package_after,
            consumed_at, rolled_back_at
        FROM st_dispatch_consumptions
        WHERE consumed_at<=?
          AND (rolled_back_at='' OR rolled_back_at>?)
    """
    params: list[object] = [cutoff, cutoff]
    if normalized_parts:
        placeholders = ",".join("?" * len(normalized_parts))
        sql += f" AND part_number IN ({placeholders})"
        params.extend(normalized_parts)
    sql += " ORDER BY part_number, consumed_at, id"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        {
            "id": int(row["id"]),
            "dispatch_session_id": int(row["dispatch_session_id"] or 0),
            "order_id": int(row["order_id"] or 0),
            "part_number": str(row["part_number"] or "").strip().upper(),
            "used_qty": float(row["used_qty"] or 0),
            "stock_before": float(row["stock_before"] or 0),
            "stock_after": float(row["stock_after"] or 0),
            "package_before": str(row["package_before"] or ""),
            "package_after": str(row["package_after"] or ""),
            "consumed_at": str(row["consumed_at"] or ""),
            "rolled_back_at": str(row["rolled_back_at"] or ""),
        }
        for row in rows
    ]


def get_st_reconcile_cutoff_batch_options(limit: int = 200) -> list[dict]:
    """取得可當盤點停損截止點的批次清單（未回滾的發料批次，新到舊）。

    cutoff_at 用「該批最後一筆有效 ST 消耗的 consumed_at」（無消耗才退回 dispatched_at）：
    同一批的 audit changed_at 恆早於 consumed_at、晚於 dispatched_at，
    這樣該批自己的扣帳會落在停損基準側，不會被重放雙扣。
    """
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT TRIM(o.code) AS code,
                   MAX(s.dispatched_at) AS dispatched_at,
                   MAX(COALESCE(c.consumed_at, s.dispatched_at)) AS cutoff_at
            FROM dispatch_sessions s
            JOIN orders o ON o.id = s.order_id
            LEFT JOIN st_dispatch_consumptions c
                   ON c.dispatch_session_id = s.id AND c.rolled_back_at=''
            WHERE s.rolled_back_at='' AND TRIM(IFNULL(o.code,'')) <> ''
            GROUP BY TRIM(o.code)
            ORDER BY MAX(s.dispatched_at) DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [
        {
            "code": str(row["code"] or "").strip(),
            "dispatched_at": str(row["dispatched_at"] or ""),
            "cutoff_at": str(row["cutoff_at"] or row["dispatched_at"] or ""),
        }
        for row in rows
        if str(row["code"] or "").strip() and str(row["dispatched_at"] or "").strip()
    ]


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


def save_st_dispatch_consumptions(records: list[dict]) -> int:
    normalized: list[dict] = []
    now = _now()
    for record in records or []:
        try:
            session_id = int(record.get("dispatch_session_id") or 0)
            order_id = int(record.get("order_id") or 0)
            used_qty = float(record.get("used_qty") or 0)
        except (TypeError, ValueError):
            continue
        part = str(record.get("part_number") or "").strip().upper()
        if session_id <= 0 or order_id <= 0 or not part or used_qty <= 0:
            continue
        normalized.append({
            "dispatch_session_id": session_id,
            "order_id": order_id,
            "part_number": part,
            "used_qty": used_qty,
            "stock_before": float(record.get("stock_before") or 0),
            "stock_after": float(record.get("stock_after") or 0),
            "package_before": str(record.get("package_before") or ""),
            "package_after": str(record.get("package_after") or ""),
            "consumed_at": str(record.get("consumed_at") or now),
        })
    if not normalized:
        return 0

    with get_conn() as conn:
        for record in normalized:
            conn.execute(
                """
                INSERT INTO st_dispatch_consumptions(
                    dispatch_session_id, order_id, part_number, used_qty,
                    stock_before, stock_after, package_before, package_after,
                    consumed_at, rolled_back_at
                )
                VALUES(?,?,?,?,?,?,?,?,?, '')
                ON CONFLICT(dispatch_session_id, part_number) DO UPDATE SET
                    used_qty=excluded.used_qty,
                    stock_before=excluded.stock_before,
                    stock_after=excluded.stock_after,
                    package_before=excluded.package_before,
                    package_after=excluded.package_after,
                    consumed_at=excluded.consumed_at,
                    rolled_back_at=''
                """,
                (
                    record["dispatch_session_id"],
                    record["order_id"],
                    record["part_number"],
                    record["used_qty"],
                    record["stock_before"],
                    record["stock_after"],
                    record["package_before"],
                    record["package_after"],
                    record["consumed_at"],
                ),
            )
    return len(normalized)


def get_st_dispatch_consumptions_for_sessions(
    session_ids: list[int],
    *,
    active_only: bool = True,
) -> list[dict]:
    normalized_ids: list[int] = []
    for session_id in session_ids or []:
        try:
            normalized_ids.append(int(session_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return []

    placeholders = ",".join("?" * len(normalized_ids))
    sql = f"SELECT * FROM st_dispatch_consumptions WHERE dispatch_session_id IN ({placeholders})"
    params: list[object] = normalized_ids
    if active_only:
        sql += " AND rolled_back_at=''"
    sql += " ORDER BY id"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def mark_st_dispatch_consumptions_rolled_back(consumption_ids: list[int]) -> int:
    normalized_ids: list[int] = []
    for consumption_id in consumption_ids or []:
        try:
            normalized_ids.append(int(consumption_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return 0

    placeholders = ",".join("?" * len(normalized_ids))
    with get_conn() as conn:
        cur = conn.execute(
            f"UPDATE st_dispatch_consumptions SET rolled_back_at=? WHERE id IN ({placeholders}) AND rolled_back_at=''",
            [_now()] + normalized_ids,
        )
    return int(cur.rowcount or 0)


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
    """合併共用 BOM 的行（如 T356789IU + T356789IU-U/A）。

    同 PO、同機種但不同出貨日要保留成不同訂單，避免分批出貨被合併。
    """
    alias_map = _get_bom_model_groups()
    merged_rows: list[dict] = []
    merge_targets: dict[str, int] = {}

    for r in rows:
        model_upper = (r.get("model") or "").strip().upper()
        po = str(r.get("po_number", ""))
        ship_date = _order_date_key(r.get("ship_date"))
        primary = alias_map.get(model_upper)

        if primary:
            merge_key = f"{po}|{primary}|{ship_date}"
            if merge_key in merge_targets:
                idx = merge_targets[merge_key]
                target = merged_rows[idx]
                target["order_qty"] = (target.get("order_qty") or 0) + (r.get("order_qty") or 0)
                target["pcb"] = f"{target['pcb']} / {r.get('pcb', '')}"
                continue

        merge_key = f"{po}|{model_upper}|{ship_date}"
        if merge_key in merge_targets:
            # 同 PO+機種+出貨日重複行 → 合併數量
            idx = merge_targets[merge_key]
            target = merged_rows[idx]
            target["order_qty"] = (target.get("order_qty") or 0) + (r.get("order_qty") or 0)
            if r.get("pcb") and r["pcb"] not in (target.get("pcb") or ""):
                target["pcb"] = f"{target['pcb']} / {r['pcb']}"
            continue
        merge_targets[merge_key] = len(merged_rows)
        merged_rows.append(dict(r))

    return merged_rows


def _order_date_key(value) -> str:
    return str(value or "").strip()


def _order_key(po, model, ship_date=None) -> str:
    """用 PO+機種+出貨日 當排程唯一 key（同 PO/機種可分批出貨）。"""
    return f"{str(po).strip()}|{str(model).strip().upper()}|{_order_date_key(ship_date)}"


def _order_base_key(po, model) -> str:
    """用 PO+機種 比對已發料訂單的交期變更。"""
    return f"{str(po).strip()}|{str(model).strip().upper()}"


def apply_dispatch_code_shifts(code_shifts: dict[str, str], exclude_order_ids: list[int]) -> list[dict]:
    """依發料主檔的編號對照同步訂單；以原始快照避免連續重編。"""
    changes = []
    excluded = set(exclude_order_ids)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        for row in conn.execute("SELECT id, code, insert_before_code, status FROM orders").fetchall():
            old_anchor = row["insert_before_code"]
            new_anchor = code_shifts.get(old_anchor, old_anchor) if row["status"] in ("pending", "merged") else old_anchor
            if row["id"] in excluded or (row["code"] not in code_shifts and new_anchor == old_anchor):
                continue
            new_code = code_shifts.get(row["code"], row["code"])
            changes.append({"id": row["id"], "old_code": row["code"], "new_code": new_code,
                            "old_insert_before_code": old_anchor, "new_insert_before_code": new_anchor})
            conn.execute("UPDATE orders SET code=?, insert_before_code=?, updated_at=? WHERE id=?", (new_code, new_anchor, _now(), row["id"]))
    return changes


def validate_dispatch_code_shift_restores(change_batches: list[list[dict]]):
    """按呼叫順序模擬多批還原，先確認全部編號仍可安全復原。"""
    with get_conn() as conn:
        current = {row["id"]: (row["code"], row["insert_before_code"])
                   for row in conn.execute("SELECT id, code, insert_before_code FROM orders")}
    for changes in change_batches:
        for change in changes:
            if current.get(change["id"]) != (change["new_code"], change["new_insert_before_code"]):
                raise ValueError("單據編號已變更，無法安全還原插入")
        for change in changes:
            current[change["id"]] = (change["old_code"], change["old_insert_before_code"])


def restore_dispatch_code_shifts(changes: list[dict]):
    """還原本次插入造成的編號異動，遇到其他修改則中止。"""
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        for change in changes:
            row = conn.execute("SELECT code, insert_before_code FROM orders WHERE id=?", (change["id"],)).fetchone()
            if not row or row["code"] != change["new_code"] or row["insert_before_code"] != change["new_insert_before_code"]:
                raise ValueError("單據編號已變更，無法安全還原插入")
        for change in changes:
            conn.execute("UPDATE orders SET code=?, insert_before_code=?, updated_at=? WHERE id=?", (change["old_code"], change["old_insert_before_code"], _now(), change["id"]))


def create_manual_order(data: dict) -> dict:
    """手動建立未排程單據；業務欄位完全採用輸入值。"""
    now = _now()
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        insert_before_code = data.get("insert_before_code", "")
        if insert_before_code and data["code"] != insert_before_code:
            raise ValueError("插入單據的編號必須與插入位置編號相同")
        if not insert_before_code and conn.execute("SELECT 1 FROM orders WHERE code=?", (data["code"],)).fetchone():
            raise ValueError("此單據編號已存在，請使用其他編號")
        sort_order = conn.execute("SELECT COALESCE(MAX(sort_order), -1) + 1 FROM orders").fetchone()[0]
        fields = ("code", "po_number", "model", "pcb", "order_qty", "balance_qty", "ship_date", "delivery_date", "remark")
        values = [str(data[key]) if key.endswith("_date") else data[key] for key in fields]
        cursor = conn.execute(
            "INSERT INTO orders(code, po_number, model, pcb, order_qty, balance_qty, ship_date, "
            "delivery_date, remark, sort_order, is_manual, insert_before_code, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,1,?,?,?)",
            (*values, sort_order, insert_before_code, now, now),
        )
        return dict(conn.execute("SELECT * FROM orders WHERE id=?", (cursor.lastrowid,)).fetchone())


def upsert_orders_from_schedule(rows: list[dict]) -> dict:
    """從排程表解析結果批次寫入 orders 表。
    自動比對已存在的 PO+機種+出貨日（含已發料），不重複新增。
    保留使用者手動輸入的 code（編號）。
    回傳差異摘要 {added, updated, skipped, removed, diffs}。
    """
    now = _now()
    merged_rows = _merge_schedule_rows(rows)

    with get_conn() as conn:
        all_orders = conn.execute(
            "SELECT id, po_number, model, pcb, order_qty, ship_date, delivery_date, status, code, is_manual FROM orders"
        ).fetchall()

        # 用 PO+機種+出貨日 當 key，同 PO/機種不同出貨日分開處理
        existing_by_key: dict[str, dict] = {}
        dispatched_by_base_key: dict[str, dict] = {}
        for o in all_orders:
            key = _order_key(o["po_number"], o["model"], o["ship_date"] or o["delivery_date"])
            existing = dict(o)
            existing_by_key[key] = existing
            if existing["status"] in ("dispatched", "completed"):
                dispatched_by_base_key.setdefault(
                    _order_base_key(existing["po_number"], existing["model"]),
                    existing,
                )

        new_key_set = {
            _order_key(r.get("po_number", ""), r.get("model", ""), r.get("ship_date"))
            for r in merged_rows
        }
        new_base_key_counts: dict[str, int] = {}
        for r in merged_rows:
            base_key = _order_base_key(r.get("po_number", ""), r.get("model", ""))
            new_base_key_counts[base_key] = new_base_key_counts.get(base_key, 0) + 1

        added_count = 0
        updated_count = 0
        skipped_count = 0
        diffs: list[dict] = []

        # 刪掉「新排程表中不存在」的 pending/merged 訂單
        removed_count = 0
        for key, existing in list(existing_by_key.items()):
            if not existing["is_manual"] and key not in new_key_set and existing["status"] in ("pending", "merged"):
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

            key = _order_key(po, model, r.get("ship_date"))
            existing = existing_by_key.get(key)
            if existing and existing["is_manual"]:
                skipped_count += 1
                continue
            base_key = _order_base_key(po, model)
            dispatched_existing = dispatched_by_base_key.get(base_key)
            if (
                (not existing or existing["status"] not in ("dispatched", "completed"))
                and new_base_key_counts.get(base_key, 0) == 1
                and dispatched_existing
            ):
                if existing and existing["status"] in ("pending", "merged"):
                    conn.execute("DELETE FROM decisions WHERE order_id = ?", (existing["id"],))
                    conn.execute("DELETE FROM order_supplements WHERE order_id = ?", (existing["id"],))
                    conn.execute("DELETE FROM orders WHERE id = ?", (existing["id"],))
                existing = dispatched_existing

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

            # 全新 PO+機種+出貨日 → 新增
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
    """移除 pending/merged 中與已發料 PO+機種+出貨日 完全重複的訂單。回傳 {removed, duplicates}。"""
    with get_conn() as conn:
        dispatched = conn.execute(
            "SELECT po_number, model, ship_date, delivery_date FROM orders WHERE status IN ('dispatched','completed')"
        ).fetchall()
        dispatched_keys = {
            _order_key(r["po_number"], r["model"], r["ship_date"] or r["delivery_date"])
            for r in dispatched
        }

        pending_orders = conn.execute(
            "SELECT id, po_number, model, ship_date, delivery_date FROM orders WHERE status IN ('pending','merged') AND is_manual=0"
        ).fetchall()

        duplicates: list[dict] = []
        for order in pending_orders:
            key = _order_key(order["po_number"], order["model"], order["ship_date"] or order["delivery_date"])
            if key in dispatched_keys:
                duplicates.append({
                    "id": order["id"],
                    "po_number": str(order["po_number"]).strip(),
                    "model": order["model"],
                    "ship_date": order["ship_date"] or order["delivery_date"] or "",
                })

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


def move_completed_folder_tree(folder: str, new_folder: str) -> int:
    """搬移已發料資料夾整棵子樹，將 folder 前綴改寫成 new_folder。"""
    old_prefix = str(folder or "").strip().strip("/")
    new_prefix = str(new_folder or "").strip().strip("/")
    if not old_prefix:
        return 0

    old_child_prefix = f"{old_prefix}/"
    old_child_like = (
        old_child_prefix
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
        + "%"
    )
    now = _now()
    updated = 0
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, folder FROM orders
            WHERE status IN ('dispatched','completed')
              AND (folder=? OR folder LIKE ? ESCAPE '\\')
            """,
            (old_prefix, old_child_like),
        ).fetchall()
        for row in rows:
            current = str(row["folder"] or "")
            suffix = current[len(old_prefix):].lstrip("/")
            target = "/".join(part for part in (new_prefix, suffix) if part)
            conn.execute(
                "UPDATE orders SET folder=?, updated_at=? WHERE id=?",
                (target, now, row["id"]),
            )
            updated += 1
    return updated


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


def list_bom_substitution_rules(*, active_only: bool = False) -> list[dict]:
    sql = "SELECT * FROM bom_substitution_rules"
    params: list[object] = []
    if active_only:
        sql += " WHERE status='active'"
    sql += " ORDER BY status='active' DESC, model, old_part_number, id"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def get_bom_substitution_rule(rule_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bom_substitution_rules WHERE id=?", (int(rule_id),)).fetchone()
    return dict(row) if row else None


def save_bom_substitution_rule(payload: dict, rule_id: int | None = None) -> dict:
    model = str(payload.get("model") or "").strip().upper()
    old_part = str(payload.get("old_part_number") or "").strip().upper()
    new_part = str(payload.get("new_part_number") or "").strip().upper()
    ratio = float(payload.get("new_per_old_ratio") or 1)
    strategy = str(payload.get("strategy") or "old_first").strip()
    effective_code = str(payload.get("effective_from_code") or "").strip()
    note = str(payload.get("note") or "").strip()
    if not model or not old_part or not new_part:
        raise ValueError("機種、舊料號與新料號皆為必填")
    if old_part == new_part:
        raise ValueError("新舊料號不可相同")
    if ratio <= 0:
        raise ValueError("替代比例必須大於 0")
    if strategy not in {"old_first", "new_first"}:
        raise ValueError("替代策略不正確")
    now = _now()
    with get_conn() as conn:
        duplicate = conn.execute(
            "SELECT id FROM bom_substitution_rules "
            "WHERE status='active' AND model=? AND old_part_number=? AND id<>? LIMIT 1",
            (model, old_part, int(rule_id or 0)),
        ).fetchone()
        if duplicate:
            raise ValueError(f"{model} 的 {old_part} 已有進行中的替代設定")
        if rule_id is None:
            cur = conn.execute(
                "INSERT INTO bom_substitution_rules("
                "model, old_part_number, new_part_number, new_per_old_ratio, strategy, "
                "effective_from_code, note, status, created_at, updated_at"
                ") VALUES(?,?,?,?,?,?,?,'active',?,?)",
                (model, old_part, new_part, ratio, strategy, effective_code, note, now, now),
            )
            saved_id = int(cur.lastrowid)
        else:
            cur = conn.execute(
                "UPDATE bom_substitution_rules SET model=?, old_part_number=?, new_part_number=?, "
                "new_per_old_ratio=?, strategy=?, effective_from_code=?, note=?, updated_at=? "
                "WHERE id=? AND status='active'",
                (model, old_part, new_part, ratio, strategy, effective_code, note, now, int(rule_id)),
            )
            if not cur.rowcount:
                raise ValueError("找不到進行中的替代設定")
            saved_id = int(rule_id)
    return get_bom_substitution_rule(saved_id) or {}


def end_bom_substitution_rule(rule_id: int) -> bool:
    now = _now()
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE bom_substitution_rules SET status='ended', ended_at=?, updated_at=? "
            "WHERE id=? AND status='active'",
            (now, now, int(rule_id)),
        )
    return bool(cur.rowcount)


def get_order_substitution_allocations(order_ids: list[int] | None = None) -> dict[int, dict[int, dict]]:
    normalized_ids = list(dict.fromkeys(int(order_id) for order_id in (order_ids or []) if int(order_id) > 0))
    sql = "SELECT order_id, rule_id, old_qty, new_qty, updated_at FROM order_substitution_allocations"
    params: list[object] = []
    if normalized_ids:
        placeholders = ",".join("?" * len(normalized_ids))
        sql += f" WHERE order_id IN ({placeholders})"
        params.extend(normalized_ids)
    sql += " ORDER BY order_id, rule_id"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    result: dict[int, dict[int, dict]] = {}
    for row in rows:
        order_id = int(row["order_id"])
        rule_id = int(row["rule_id"])
        result.setdefault(order_id, {})[rule_id] = {
            "old_qty": float(row["old_qty"] or 0),
            "new_qty": float(row["new_qty"] or 0),
            "updated_at": str(row["updated_at"] or ""),
        }
    return result


def save_order_substitution_allocation(order_id: int, rule_id: int, old_qty: float, new_qty: float) -> dict:
    now = _now()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO order_substitution_allocations(order_id, rule_id, old_qty, new_qty, updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(order_id, rule_id) DO UPDATE SET "
            "old_qty=excluded.old_qty, new_qty=excluded.new_qty, updated_at=excluded.updated_at",
            (int(order_id), int(rule_id), float(old_qty or 0), float(new_qty or 0), now),
        )
    return {"order_id": int(order_id), "rule_id": int(rule_id), "old_qty": float(old_qty or 0), "new_qty": float(new_qty or 0), "updated_at": now}


def delete_order_substitution_allocation(order_id: int, rule_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM order_substitution_allocations WHERE order_id=? AND rule_id=?",
            (int(order_id), int(rule_id)),
        )
    return bool(cur.rowcount)


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
        main_period_id = get_current_main_period_id()
        cur = conn.execute(
            "INSERT INTO dispatch_sessions(order_id, previous_status, backup_path, main_file_path, dispatched_at, rolled_back_at, main_period_id) "
            "VALUES(?,?,?,?,?, '', ?)",
            (order_id, previous_status, backup_path, main_file_path, dispatched_at, main_period_id),
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
        "main_period_id": main_period_id,
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

    sql = (
        "SELECT order_id, part_number, supplement_qty, note, updated_at "
        "FROM order_supplements WHERE absorbed_by_alignment_id=0"
    )
    params: list[int] = []
    if normalized_ids:
        placeholders = ",".join("?" * len(normalized_ids))
        sql += f" AND order_id IN ({placeholders})"
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

    sql = (
        "SELECT order_id, part_number, supplement_qty FROM order_supplements "
        "WHERE absorbed_by_alignment_id=0"
    )
    params: list[int] = []
    if normalized_ids:
        placeholders = ",".join("?" * len(normalized_ids))
        sql += f" AND order_id IN ({placeholders})"
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
    is_sample: bool = False,
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
                "decisions_json=?, supplements_json=?, shortages_json=?, is_sample=?, updated_at=?, deleted_at='' "
                "WHERE id=?",
                (
                    main_file_path,
                    str(main_file_mtime_ns or ""),
                    main_loaded_at,
                    _json_dumps(decisions or {}),
                    _json_dumps(supplements or {}),
                    _json_dumps(shortages or []),
                    1 if is_sample else 0,
                    now,
                    draft_id,
                ),
            )
        else:
            cur = conn.execute(
                "INSERT INTO merge_drafts("
                "order_id, status, main_file_path, main_file_mtime_ns, main_loaded_at, "
                "decisions_json, supplements_json, shortages_json, is_sample, created_at, updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    order_id,
                    "active",
                    main_file_path,
                    str(main_file_mtime_ns or ""),
                    main_loaded_at,
                    _json_dumps(decisions or {}),
                    _json_dumps(supplements or {}),
                    _json_dumps(shortages or []),
                    1 if is_sample else 0,
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
        "is_sample": bool(is_sample),
        "created_at": created_at,
        "updated_at": now,
        "committed_at": "",
        "deleted_at": "",
    }


def set_merge_draft_sample_flags(order_ids: list[int], sample_order_ids: set[int] | list[int]) -> int:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return 0

    sample_ids = set()
    for order_id in sample_order_ids or []:
        try:
            sample_ids.add(int(order_id))
        except (TypeError, ValueError):
            continue

    updated = 0
    now = _now()
    with get_conn() as conn:
        for order_id in normalized_ids:
            cur = conn.execute(
                "UPDATE merge_drafts SET is_sample=?, updated_at=? WHERE order_id=? AND status='active'",
                (1 if order_id in sample_ids else 0, now, order_id),
            )
            updated += int(cur.rowcount or 0)
    return updated


def replace_merge_draft_files(draft_id: int, files: list[dict]):
    now = _now()
    with get_conn() as conn:
        conn.execute("DELETE FROM merge_draft_files WHERE draft_id=?", (draft_id,))
        for item in files or []:
            conn.execute(
                "INSERT INTO merge_draft_files("
                "draft_id, bom_file_id, filename, filepath, source_filename, source_format, "
                "model, group_model, carry_overs_json, supplements_json, effective_components_json, created_at, updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                    _json_dumps(item.get("effective_components") or []),
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
        item["effective_components"] = _json_loads(item.get("effective_components_json", ""), [])
    return items


def get_merge_draft_files_for_drafts(draft_ids: list[int]) -> dict[int, list[dict]]:
    normalized_ids: list[int] = []
    for draft_id in draft_ids or []:
        try:
            normalized_ids.append(int(draft_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return {}

    placeholders = ",".join("?" * len(normalized_ids))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM merge_draft_files WHERE draft_id IN ({placeholders}) ORDER BY draft_id, id",
            normalized_ids,
        ).fetchall()
        items = _repair_row_paths(
            conn,
            rows,
            table="merge_draft_files",
            column="filepath",
            fallback_dirs=_MERGE_DRAFT_FILE_FALLBACK_DIRS,
            recursive=True,
        )

    result: dict[int, list[dict]] = {draft_id: [] for draft_id in normalized_ids}
    for item in items:
        try:
            draft_id = int(item.get("draft_id"))
        except (TypeError, ValueError):
            continue
        item["carry_overs"] = _json_loads(item.get("carry_overs_json", ""), {})
        item["supplements"] = _json_loads(item.get("supplements_json", ""), {})
        item["effective_components"] = _json_loads(item.get("effective_components_json", ""), [])
        result.setdefault(draft_id, []).append(item)
    return result


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


def get_latest_committed_merge_drafts_for_orders(
    order_ids: list[int],
    committed_after: str = "",
) -> dict[int, dict]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return {}

    placeholders = ",".join("?" * len(normalized_ids))
    params: list[object] = list(normalized_ids)
    where = f"order_id IN ({placeholders}) AND status='committed'"
    if str(committed_after or "").strip():
        where += " AND committed_at>=?"
        params.append(str(committed_after).strip())

    sql = (
        "SELECT * FROM ("
        "SELECT md.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY committed_at DESC, id DESC) AS rn "
        "FROM merge_drafts md "
        f"WHERE {where}"
        ") WHERE rn=1"
    )
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    result: dict[int, dict] = {}
    for row in rows:
        item = dict(row)
        item.pop("rn", None)
        item["decisions"] = _json_loads(item.get("decisions_json", ""), {})
        item["supplements"] = _json_loads(item.get("supplements_json", ""), {})
        item["shortages"] = _json_loads(item.get("shortages_json", ""), [])
        try:
            result[int(item["order_id"])] = item
        except (TypeError, ValueError):
            continue
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


def add_erp_purchase_order(rec: dict) -> int:
    """寫入一筆正航採購單本機紀錄（status=ok 或 error）。"""
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO erp_purchase_orders(
                   bill_no, target_db, supplier_id, supplier_name, remark, lines_json,
                   sum_qty, sum_amount, sum_tax, total, actor, client_ip, created_at, status, error)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                rec.get("bill_no", ""), rec.get("target_db", ""), rec.get("supplier_id", ""),
                rec.get("supplier_name", ""), rec.get("remark", ""), rec.get("lines_json", "[]"),
                float(rec.get("sum_qty") or 0), float(rec.get("sum_amount") or 0),
                float(rec.get("sum_tax") or 0), float(rec.get("total") or 0),
                rec.get("actor", ""), rec.get("client_ip", ""), _now(),
                rec.get("status", "ok"), rec.get("error", ""),
            ),
        )
        return int(cur.lastrowid)


def list_erp_purchase_orders(limit: int = 50) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM erp_purchase_orders ORDER BY id DESC LIMIT ?", (int(limit),)
        ).fetchall()
        return [dict(r) for r in rows]


def get_latest_active_dispatch_session_ids(order_ids: list[int]) -> dict[int, int]:
    """每張訂單最近一次未退回的 dispatch_sessions.id；沒有的訂單不在結果內。"""
    ids = list(dict.fromkeys(int(i) for i in order_ids if i))
    result: dict[int, int] = {}
    with get_conn() as conn:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            marks = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT order_id, MAX(id) AS sid FROM dispatch_sessions "
                f"WHERE rolled_back_at='' AND order_id IN ({marks}) GROUP BY order_id",
                chunk,
            ).fetchall()
            result.update({int(r["order_id"]): int(r["sid"]) for r in rows})
    return result


def save_po_line_allocations(erp_po_id: int, bill_no: str, target_db: str, rows: list[dict]) -> int:
    """寫入採購單明細的「為哪些訂單而買」紀錄；只做記錄，不參與缺料計算。回傳寫入筆數。"""
    now = _now()
    data = [
        (
            int(erp_po_id), str(bill_no or ""), str(target_db or ""), int(r.get("row_no") or 0),
            str(r.get("part_number") or "").strip().upper(), int(r.get("order_id") or 0),
            str(r.get("order_code") or ""), str(r.get("model") or ""), str(r.get("customer_po") or ""),
            str(r.get("ship_date") or ""), int(r.get("dispatch_session_id") or 0),
            float(r.get("qty") or 0), str(r.get("kind") or "manual"),
            str(r.get("source") or "manual_po"), now,
        )
        for r in rows
    ]
    if not data:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """INSERT INTO erp_po_line_allocations(
                   erp_po_id, bill_no, target_db, row_no, part_number, order_id, order_code, model,
                   customer_po, ship_date, dispatch_session_id, qty, kind, source, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            data,
        )
    return len(data)


def get_po_line_allocations(erp_po_ids: list[int] | None = None, bill_no: str | None = None) -> list[dict]:
    """依採購單本機 id 清單或單號查明細分配，依 erp_po_id、row_no、id 排序。"""
    where, params = [], []
    if erp_po_ids is not None:
        ids = [int(i) for i in erp_po_ids]
        if not ids:
            return []
        where.append(f"erp_po_id IN ({','.join('?' * len(ids))})")
        params.extend(ids)
    if bill_no is not None:
        where.append("bill_no = ?")
        params.append(str(bill_no))
    sql = "SELECT * FROM erp_po_line_allocations"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY erp_po_id, row_no, id"
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def list_dispatched_orders_for_purchase(limit: int = 300) -> list[dict]:
    """缺料採購可選的訂單：已發料 / 已完成，最新發料的在前；dispatched_at 取未退回的最近一次。"""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT o.id, o.code, o.model, o.po_number, o.status,
                      COALESCE(NULLIF(o.delivery_date, ''), o.ship_date, '') AS ship_date,
                      COALESCE((SELECT MAX(ds.dispatched_at) FROM dispatch_sessions ds
                                WHERE ds.order_id = o.id AND ds.rolled_back_at = ''), '') AS dispatched_at
               FROM orders o
               WHERE o.status IN ('dispatched', 'completed')
               ORDER BY dispatched_at DESC, o.id DESC
               LIMIT ?""",
            (int(limit),),
        ).fetchall()
    return [dict(r) for r in rows]


def get_part_purchase_categories(parts: list[str]) -> dict[str, str]:
    """人工指定的採購類別 {料號大寫: 'smt'|'backend'}；只回有指定的料號。"""
    keys = list(dict.fromkeys(str(p or "").strip().upper() for p in parts if str(p or "").strip()))
    result: dict[str, str] = {}
    with get_conn() as conn:
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            marks = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT part_number, category FROM part_purchase_categories WHERE part_number IN ({marks})",
                chunk,
            ).fetchall()
            result.update({str(r["part_number"]): str(r["category"]) for r in rows})
    return result


def set_part_purchase_category(part_number: str, category: str, actor: str = "") -> None:
    part = str(part_number or "").strip().upper()
    if not part:
        raise ValueError("料號不可為空")
    if category not in ("smt", "backend"):
        raise ValueError("類別需為 smt 或 backend")
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO part_purchase_categories(part_number, category, updated_at, actor)
               VALUES(?,?,?,?)
               ON CONFLICT(part_number) DO UPDATE SET
                   category=excluded.category, updated_at=excluded.updated_at, actor=excluded.actor""",
            (part, category, _now(), actor),
        )


# ── 鈞洋 後段加工對應 ─────────────────────────────────────────────────────────

POSTPROCESS_STATUSES = ("confirmed", "pending", "rejected")
POSTPROCESS_SOURCES = ("seed", "auto_bom", "auto_po", "manual")
POSTPROCESS_SEED = (
    ("OC-10988B", "SP-10024B"), ("OC-10989B", "SP-10025B"), ("OC-10990B", "SP-10026B"),
    ("OC-10991B", "SP-10027B"), ("OC-10992B", "SP-10028B"), ("OC-10993B", "SP-10029B"),
    ("OC-10978B", "SP-10022B"), ("OC-10986B", "SP-10023B"),
)


def seed_postprocess_links(conn) -> int:
    """預設已確認的對應（冪等：已存在的組合不動，包含使用者改過 / 停用的）；回傳新增筆數。"""
    added = 0
    now = _now()
    for base, process in POSTPROCESS_SEED:
        cur = conn.execute(
            "INSERT OR IGNORE INTO postprocess_links(base_part, process_part, ratio, enabled, status, source, "
            "evidence, note, created_at, updated_at) VALUES(?,?,1,1,'confirmed','seed','預設對應（Andy 提供）','',?,?)",
            (base, process, now, now),
        )
        added += cur.rowcount or 0
    return added


def _postprocess_row(row) -> dict:
    d = dict(row)
    d["enabled"] = bool(d.get("enabled"))
    d["active"] = d["enabled"] and d.get("status") == "confirmed"
    return d


def list_postprocess_links(include_rejected: bool = True) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM postprocess_links "
            + ("" if include_rejected else "WHERE status != 'rejected' ")
            + "ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'confirmed' THEN 1 ELSE 2 END, base_part, process_part"
        ).fetchall()
    return [_postprocess_row(r) for r in rows]


def get_postprocess_link(link_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM postprocess_links WHERE id=?", (int(link_id),)).fetchone()
    return _postprocess_row(row) if row else None


def active_postprocess_links(bases: list[str] | None = None) -> dict[str, list[dict]]:
    """生效中（confirmed 且 enabled）的對應，依基礎件分組：{base: [{id, process_part, ratio}]}。"""
    sql = "SELECT id, base_part, process_part, ratio FROM postprocess_links WHERE status='confirmed' AND enabled=1"
    params: list = []
    if bases is not None:
        keys = list(dict.fromkeys(str(b or "").strip().upper() for b in bases if str(b or "").strip()))
        if not keys:
            return {}
        sql += f" AND base_part IN ({','.join('?' * len(keys))})"
        params = keys
    with get_conn() as conn:
        rows = conn.execute(sql + " ORDER BY id", params).fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(str(r["base_part"]), []).append(
            {"id": r["id"], "process_part": str(r["process_part"]), "ratio": float(r["ratio"])}
        )
    return out


def _postprocess_clean(base_part, process_part, ratio, status, source) -> tuple[str, str, float, str, str]:
    base = str(base_part or "").strip().upper()
    process = str(process_part or "").strip().upper()
    if not base or not process:
        raise ValueError("基礎件與加工件料號不可為空")
    if base == process:
        raise ValueError("基礎件與加工件不可相同")
    try:
        ratio_value = float(ratio)
    except (TypeError, ValueError):
        raise ValueError("比例需為數字") from None
    if not (ratio_value > 0) or ratio_value > 1000:
        raise ValueError("比例需大於 0 且不超過 1000")
    if status not in POSTPROCESS_STATUSES:
        raise ValueError("狀態不正確")
    if source not in POSTPROCESS_SOURCES:
        raise ValueError("來源不正確")
    return base, process, ratio_value, status, source


def add_postprocess_link(
    base_part: str, process_part: str, ratio: float = 1, *, status: str = "confirmed", source: str = "manual",
    evidence: str = "", note: str = "", enabled: bool = True,
) -> int:
    """新增一筆對應；同一組 (基礎件, 加工件) 已存在（任何狀態）就丟 ValueError。"""
    base, process, ratio_value, status, source = _postprocess_clean(base_part, process_part, ratio, status, source)
    now = _now()
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO postprocess_links(base_part, process_part, ratio, enabled, status, source, evidence, note, "
                "created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (base, process, ratio_value, 1 if enabled else 0, status, source, evidence, note, now, now),
            )
        except sqlite3.IntegrityError:
            raise ValueError(f"{base} → {process} 已存在") from None
        return int(cur.lastrowid)


def update_postprocess_link(link_id: int, **fields) -> dict | None:
    """可改 process_part / ratio / enabled / status / note；沒有這筆回 None。"""
    current = get_postprocess_link(link_id)
    if current is None:
        return None
    process = fields.get("process_part", current["process_part"])
    ratio = fields.get("ratio", current["ratio"])
    status = fields.get("status", current["status"])
    _b, process, ratio_value, status, _s = _postprocess_clean(
        current["base_part"], process, ratio, status, current["source"]
    )
    enabled = current["enabled"] if fields.get("enabled") is None else bool(fields["enabled"])
    note = current["note"] if fields.get("note") is None else str(fields["note"])
    with get_conn() as conn:
        try:
            conn.execute(
                "UPDATE postprocess_links SET process_part=?, ratio=?, enabled=?, status=?, note=?, updated_at=? WHERE id=?",
                (process, ratio_value, 1 if enabled else 0, status, note, _now(), int(link_id)),
            )
        except sqlite3.IntegrityError:
            raise ValueError(f"{current['base_part']} → {process} 已存在") from None
    return get_postprocess_link(link_id)


def delete_postprocess_link(link_id: int) -> bool:
    with get_conn() as conn:
        return conn.execute("DELETE FROM postprocess_links WHERE id=?", (int(link_id),)).rowcount > 0


def postprocess_pair_exists(base_part: str, process_part: str) -> bool:
    with get_conn() as conn:
        return conn.execute(
            "SELECT 1 FROM postprocess_links WHERE base_part=? AND process_part=?",
            (str(base_part).strip().upper(), str(process_part).strip().upper()),
        ).fetchone() is not None


def list_bom_dash_followers() -> list[dict]:
    """BOM 內「緊接在非虛線行之後的虛線行」：[{bom_file_id, base_part, process_part, model}]（供後段加工自動建議）。"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT c.bom_file_id, c.id, c.part_number, c.is_dash, f.group_model "
            "FROM bom_components c JOIN bom_files f ON f.id = c.bom_file_id "
            "ORDER BY c.bom_file_id, c.id"
        ).fetchall()
    out: list[dict] = []
    prev = None
    for r in rows:
        if prev is not None and prev["bom_file_id"] == r["bom_file_id"] and not prev["is_dash"] and r["is_dash"]:
            base = str(prev["part_number"] or "").strip().upper()
            process = str(r["part_number"] or "").strip().upper()
            if base and process and base != process:
                out.append({"bom_file_id": r["bom_file_id"], "base_part": base, "process_part": process,
                            "model": str(r["group_model"] or "")})
        prev = r
    return out


# ── 轉換料 A→B ────────────────────────────────────────────────────────────────

def get_active_part_conversions() -> dict[str, str]:
    """生效中的轉換 {A: B}（料號皆為大寫）。"""
    with get_conn() as conn:
        rows = conn.execute("SELECT part_a, part_b FROM part_conversion_active").fetchall()
    return {str(r["part_a"]).upper(): str(r["part_b"]).upper() for r in rows}


def get_part_conversion(conversion_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM part_conversions WHERE id=?", (int(conversion_id),)).fetchone()
    return _decode_part_conversion(row) if row else None


def list_part_conversions(limit: int = 50, *, with_snapshot: bool = False) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM part_conversions ORDER BY id DESC LIMIT ?", (int(limit),)
        ).fetchall()
    items = [_decode_part_conversion(r) for r in rows]
    if not with_snapshot:
        for item in items:
            item.pop("snapshot", None)
    return items


def _decode_part_conversion(row) -> dict:
    data = dict(row)
    data["affected"] = _json_loads(data.pop("affected_json", ""), [])
    data["snapshot"] = _json_loads(data.pop("snapshot_json", ""), {})
    return data


def _replace_components(conn, bom_id: str, components: list[dict]) -> None:
    conn.execute("DELETE FROM bom_components WHERE bom_file_id=?", (bom_id,))
    for c in components:
        conn.execute(
            "INSERT INTO bom_components(bom_file_id, part_number, description, qty_per_board, "
            "scrap_factor, needed_qty, prev_qty_cs, is_dash, is_customer_supplied) VALUES(?,?,?,?,?,?,?,?,?)",
            (bom_id, c["part_number"], c.get("description", ""),
             c.get("qty_per_board", 0), c.get("scrap_factor", 0), c.get("needed_qty", 0),
             c.get("prev_qty_cs", 0), int(bool(c.get("is_dash", False))),
             int(bool(c.get("is_customer_supplied", False)))),
        )


def apply_part_conversion(
    part_a: str, part_b: str, bom_states: list[dict], snapshot: dict, affected: list[dict], actor: str = ""
) -> int:
    """單一 transaction：寫入還原快照 + 轉換紀錄 + 生效標記，並以新元件取代各 BOM 的元件。

    bom_states 每項 {bom_id, components}（轉換後的完整元件清單）。任何一步失敗整筆回復。
    """
    a, b = str(part_a or "").strip().upper(), str(part_b or "").strip().upper()
    now = _now()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO part_conversions(part_a, part_b, status, affected_json, snapshot_json, actor, created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (a, b, "active", _json_dumps(affected), _json_dumps(snapshot), actor, now),
        )
        conversion_id = int(cur.lastrowid)
        for state in bom_states:
            _replace_components(conn, str(state["bom_id"]), state["components"])
        conn.execute(
            "INSERT INTO part_conversion_active(part_a, part_b, conversion_id, created_at) VALUES(?,?,?,?)",
            (a, b, conversion_id, now),
        )
        conn.execute(
            "INSERT INTO activity_logs(action, detail, created_at) VALUES(?,?,?)",
            ("part_convert", f"轉換料 {a} → {b}，影響 {len(affected)} 份 BOM（#{conversion_id}，{actor}）", now),
        )
    return conversion_id


def restore_part_conversion(conversion_id: int, bom_components: dict[str, list[dict]], actor: str = "") -> None:
    """單一 transaction：還原各 BOM 元件、移除生效標記（A 重新啟用）、標記紀錄為已還原。"""
    now = _now()
    with get_conn() as conn:
        row = conn.execute("SELECT part_a, part_b, status FROM part_conversions WHERE id=?",
                           (int(conversion_id),)).fetchone()
        if not row or row["status"] != "active":
            raise ValueError("此轉換已還原或不存在")
        for bom_id, components in bom_components.items():
            _replace_components(conn, str(bom_id), components)
        conn.execute("DELETE FROM part_conversion_active WHERE conversion_id=?", (int(conversion_id),))
        conn.execute(
            "UPDATE part_conversions SET status='restored', restored_at=?, restored_by=? WHERE id=?",
            (now, actor, int(conversion_id)),
        )
        conn.execute(
            "INSERT INTO activity_logs(action, detail, created_at) VALUES(?,?,?)",
            ("part_convert_restore",
             f"還原轉換料 {row['part_a']} → {row['part_b']}（#{conversion_id}，{actor}）", now),
        )


# ── 客人改交期 → 廠商提前清單 ─────────────────────────────────────────────────

PULLIN_STATUSES = ("未詢問", "已詢問", "可提前", "不能提前")
PULLIN_UNRESOLVED = ("未詢問", "已詢問")


def _pullin_params(line: dict) -> tuple:
    return (
        str(line.get("supplier_id") or ""), str(line.get("supplier_name") or ""),
        str(line.get("bill_no") or ""), int(line.get("row_no") or 0),
        str(line.get("part_number") or "").strip().upper(), float(line.get("qty") or 0),
        str(line.get("po_date") or ""), str(line.get("requested_date") or ""),
        int(line.get("days_earlier") or 0), _json_dumps(list(line.get("order_ids") or [])),
    )


def upsert_pullin_items(lines: list[dict], status: str | None = None, actor: str = "", note: str | None = None) -> int:
    """寫入 / 更新提前清單的行。status=None 時新行為「未詢問」、既有行保留原狀態；
    指定 status 時一律設為該狀態。以 line_key 為鍵，重跑清單不會洗掉狀態。"""
    if status is not None and status not in PULLIN_STATUSES:
        raise ValueError(f"狀態需為 {' / '.join(PULLIN_STATUSES)}")
    now = _now()
    count = 0
    with get_conn() as conn:
        for line in lines:
            key = str(line.get("line_key") or "").strip()
            if not key:
                continue
            params = _pullin_params(line)
            exists = conn.execute("SELECT id FROM po_pullin_items WHERE line_key=?", (key,)).fetchone()
            if exists:
                conn.execute(
                    "UPDATE po_pullin_items SET supplier_id=?, supplier_name=?, bill_no=?, row_no=?, "
                    "part_number=?, qty=?, po_date=?, requested_date=?, days_earlier=?, "
                    "order_ids_json=COALESCE(?, order_ids_json), updated_at=? WHERE line_key=?",
                    (*params[:-1], params[-1] if line.get("order_ids") else None, now, key),
                )
                if status is not None:
                    conn.execute("UPDATE po_pullin_items SET status=?, actor=? WHERE line_key=?",
                                 (status, actor, key))
                if note is not None:
                    conn.execute("UPDATE po_pullin_items SET note=? WHERE line_key=?", (note, key))
            else:
                conn.execute(
                    "INSERT INTO po_pullin_items(line_key, supplier_id, supplier_name, bill_no, row_no, "
                    "part_number, qty, po_date, requested_date, days_earlier, order_ids_json, status, note, "
                    "actor, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (key, *params, status or "未詢問", note or "", actor, now, now),
                )
            count += 1
    return count


def get_pullin_statuses(line_keys: list[str]) -> dict[str, dict]:
    keys = list(dict.fromkeys(str(k) for k in line_keys if k))
    result: dict[str, dict] = {}
    with get_conn() as conn:
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            rows = conn.execute(
                f"SELECT line_key, status, note, updated_at FROM po_pullin_items "
                f"WHERE line_key IN ({','.join('?' * len(chunk))})", chunk,
            ).fetchall()
            result.update({r["line_key"]: dict(r) for r in rows})
    return result


def count_unresolved_pullin_items() -> int:
    with get_conn() as conn:
        row = conn.execute(
            f"SELECT COUNT(*) AS n FROM po_pullin_items WHERE status IN ({','.join('?' * len(PULLIN_UNRESOLVED))})",
            PULLIN_UNRESOLVED,
        ).fetchone()
    return int(row["n"])


def list_pullin_items(unresolved_only: bool = False) -> list[dict]:
    sql = "SELECT * FROM po_pullin_items"
    params: tuple = ()
    if unresolved_only:
        sql += f" WHERE status IN ({','.join('?' * len(PULLIN_UNRESOLVED))})"
        params = PULLIN_UNRESOLVED
    with get_conn() as conn:
        rows = conn.execute(sql + " ORDER BY supplier_id, bill_no, row_no, id", params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["order_ids"] = _json_loads(d.pop("order_ids_json", ""), [])
        out.append(d)
    return out


def get_activity_logs(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM activity_logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


# ── Sea freight ───────────────────────────────────────────────────────────────

def create_sea_shipment(meta: dict, items: list[dict]) -> int:
    now = _now()
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO sea_shipments(
                filename, source_path, customer, cust_po, shipment_date, delivery_date,
                maker, mark_text, invoice_no, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                meta.get("filename", ""),
                meta.get("source_path", ""),
                meta.get("customer", ""),
                meta.get("cust_po", ""),
                meta.get("shipment_date", ""),
                meta.get("delivery_date", ""),
                meta.get("maker", "Andy"),
                meta.get("mark_text", "HILLIARD"),
                meta.get("invoice_no", ""),
                now,
                now,
            ),
        )
        shipment_id = int(cur.lastrowid)
        for i, item in enumerate(items, start=1):
            conn.execute(
                """
                INSERT INTO sea_shipment_items(
                    shipment_id, line_no, order_date, customer, cust_po, item_no,
                    description, price, qty, delivery_date, packing_name, per_box_qty,
                    net_weight, gross_weight, volume, box_count, tail_qty, carton_no,
                    harmonized_code, match_status, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    shipment_id,
                    i,
                    item.get("order_date", ""),
                    item.get("customer", ""),
                    item.get("cust_po", ""),
                    item.get("item_no", ""),
                    item.get("description", ""),
                    float(item.get("price") or 0),
                    float(item.get("qty") or 0),
                    item.get("delivery_date", ""),
                    item.get("packing_name", ""),
                    float(item.get("per_box_qty") or 0),
                    float(item.get("net_weight") or 0),
                    float(item.get("gross_weight") or 0),
                    float(item.get("volume") or 0),
                    int(item.get("box_count") or 0),
                    float(item.get("tail_qty") or 0),
                    item.get("carton_no", ""),
                    item.get("harmonized_code", ""),
                    item.get("match_status", ""),
                    now,
                ),
            )
        return shipment_id


def list_sea_shipments(limit: int = 30) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT s.*,
                   COUNT(i.id) AS item_count,
                   COALESCE(SUM(i.box_count), 0) AS total_boxes,
                   COALESCE(SUM((i.box_count - CASE WHEN i.tail_qty > 0 THEN 1 ELSE 0 END) * i.net_weight
                       + CASE WHEN i.tail_qty > 0 AND i.per_box_qty > 0 THEN (i.net_weight * i.tail_qty / i.per_box_qty) ELSE 0 END), 0) AS total_net_weight,
                   COALESCE(SUM((i.box_count - CASE WHEN i.tail_qty > 0 THEN 1 ELSE 0 END) * i.gross_weight
                       + CASE WHEN i.tail_qty > 0 AND i.per_box_qty > 0 THEN (i.gross_weight * i.tail_qty / i.per_box_qty) ELSE 0 END), 0) AS total_gross_weight,
                   COALESCE(SUM((i.box_count - CASE WHEN i.tail_qty > 0 THEN 1 ELSE 0 END) * i.volume
                       + CASE WHEN i.tail_qty > 0 THEN i.volume ELSE 0 END), 0) AS total_volume
            FROM sea_shipments s
            LEFT JOIN sea_shipment_items i ON i.shipment_id = s.id
            GROUP BY s.id
            ORDER BY s.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_sea_shipment(shipment_id: int) -> dict | None:
    with get_conn() as conn:
        shipment = conn.execute("SELECT * FROM sea_shipments WHERE id=?", (shipment_id,)).fetchone()
        if not shipment:
            return None
        items = conn.execute(
            "SELECT * FROM sea_shipment_items WHERE shipment_id=? ORDER BY line_no, id",
            (shipment_id,),
        ).fetchall()
        data = dict(shipment)
        data["items"] = [dict(r) for r in items]
        return data


def update_sea_shipment(shipment_id: int, meta: dict, items: list[dict]) -> bool:
    now = _now()
    with get_conn() as conn:
        existing = conn.execute("SELECT id FROM sea_shipments WHERE id=?", (shipment_id,)).fetchone()
        if not existing:
            return False
        conn.execute(
            """
            UPDATE sea_shipments
            SET customer=?, cust_po=?, shipment_date=?, delivery_date=?,
                maker=?, mark_text=?, invoice_no=?, updated_at=?
            WHERE id=?
            """,
            (
                meta.get("customer", ""),
                meta.get("cust_po", ""),
                meta.get("shipment_date", ""),
                meta.get("delivery_date", ""),
                meta.get("maker", "Andy"),
                meta.get("mark_text", "HILLIARD"),
                meta.get("invoice_no", ""),
                now,
                shipment_id,
            ),
        )
        for i, item in enumerate(items, start=1):
            conn.execute(
                """
                UPDATE sea_shipment_items
                SET line_no=?, cust_po=?, qty=?, price=?, packing_name=?, per_box_qty=?,
                    net_weight=?, gross_weight=?, volume=?, box_count=?, tail_qty=?,
                    carton_no=?, harmonized_code=?, match_status=?, updated_at=?
                WHERE id=? AND shipment_id=?
                """,
                (
                    i,
                    item.get("cust_po", ""),
                    float(item.get("qty") or 0),
                    float(item.get("price") or 0),
                    item.get("packing_name", ""),
                    float(item.get("per_box_qty") or 0),
                    float(item.get("net_weight") or 0),
                    float(item.get("gross_weight") or 0),
                    float(item.get("volume") or 0),
                    int(item.get("box_count") or 0),
                    float(item.get("tail_qty") or 0),
                    item.get("carton_no", ""),
                    item.get("harmonized_code", ""),
                    item.get("match_status", ""),
                    now,
                    int(item.get("id") or 0),
                    shipment_id,
                ),
            )
        return True


def delete_sea_shipment(shipment_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM sea_shipments WHERE id=?", (shipment_id,))
        return cur.rowcount > 0


def get_sea_harmonized_codes() -> dict[str, str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT item_no, harmonized_code FROM sea_harmonized_codes").fetchall()
        return {str(r["item_no"]): str(r["harmonized_code"] or "") for r in rows}


def list_sea_harmonized_codes() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT item_no, harmonized_code, note, updated_at
            FROM sea_harmonized_codes
            ORDER BY item_no COLLATE NOCASE
            """
        ).fetchall()
        return [dict(r) for r in rows]


def upsert_sea_harmonized_code(item_no: str, harmonized_code: str, note: str = ""):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO sea_harmonized_codes(item_no, harmonized_code, note, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(item_no) DO UPDATE SET
              harmonized_code=excluded.harmonized_code,
              note=excluded.note,
              updated_at=excluded.updated_at
            """,
            (item_no, harmonized_code, note, _now()),
        )


def delete_sea_harmonized_code(item_no: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM sea_harmonized_codes WHERE item_no=?", (item_no,))
        return cur.rowcount > 0


def get_sea_packing_specs() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM sea_packing_specs
            ORDER BY item_no COLLATE NOCASE
            """
        ).fetchall()
        return [dict(r) for r in rows]


def upsert_sea_packing_spec(spec: dict):
    item_no = str(spec.get("item_no") or "").strip()
    if not item_no:
        return
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO sea_packing_specs(
                item_no, packing_name, per_box_qty, net_weight, gross_weight,
                volume, vendor, updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(item_no) DO UPDATE SET
              packing_name=excluded.packing_name,
              per_box_qty=excluded.per_box_qty,
              net_weight=excluded.net_weight,
              gross_weight=excluded.gross_weight,
              volume=excluded.volume,
              vendor=excluded.vendor,
              updated_at=excluded.updated_at
            """,
            (
                item_no,
                str(spec.get("packing_name") or "").strip(),
                float(spec.get("per_box_qty") or 0),
                float(spec.get("net_weight") or 0),
                float(spec.get("gross_weight") or 0),
                float(spec.get("volume") or 0),
                str(spec.get("vendor") or "").strip(),
                _now(),
            ),
        )


def upsert_sea_packing_specs(specs: list[dict]) -> int:
    count = 0
    for spec in specs:
        if str(spec.get("item_no") or "").strip():
            upsert_sea_packing_spec(spec)
            count += 1
    return count


def delete_sea_packing_spec(item_no: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM sea_packing_specs WHERE item_no=?", (item_no,))
        return cur.rowcount > 0


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
            "INSERT INTO defective_batches(filename, imported_at, note, main_file_mtime, main_period_id) VALUES(?,?,?,?,?)",
            (filename, _now(), note, main_file_mtime, get_current_main_period_id()),
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


def get_defective_records_after(created_after: str, *, include_count_absorbed: bool = False) -> list[dict]:
    cutoff = str(created_after or "").strip()
    if not cutoff:
        return []
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                r.*,
                b.filename AS batch_filename,
                b.imported_at AS batch_imported_at,
                b.note AS batch_note
            FROM defective_records r
            JOIN defective_batches b ON b.id = r.batch_id
            WHERE r.created_at>? AND r.status IN ('open', 'confirmed')
            ORDER BY b.imported_at, r.created_at, r.id
            """,
            (cutoff,),
        ).fetchall()
    from .services.inventory_restore_guard import get_count_absorbed_record_ids
    absorbed_ids = set() if include_count_absorbed else get_count_absorbed_record_ids()
    return [dict(row) for row in rows if int(row['id']) not in absorbed_ids]


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
    period_id = get_current_main_period_id()
    with get_conn() as conn:
        _archive_deleted_defectives(conn, 'id=?', record_id, period_id=period_id)
        cur = conn.execute("DELETE FROM defective_records WHERE id=?", (record_id,))
    return cur.rowcount > 0


def _archive_deleted_defectives(conn, predicate: str, identifier: int, *,
                               period_id: int,
                               reversed_parts: set[str] | None = None,
                               reversed_at: str = '', reversal_id: str = '') -> None:
    rows = conn.execute(f"SELECT * FROM defective_records WHERE {predicate}", (identifier,)).fetchall()
    for row in rows:
        if float(row['defective_qty'] or 0) <= 0 or row['status'] not in ('open', 'confirmed', 'closed'):
            continue
        part = str(row['part_number']).strip().upper()
        reversed_part = part in (reversed_parts or set())
        conn.execute(
            "INSERT INTO defective_deleted_history "
            "(record_id,part_number,defective_qty,created_at,deleted_at,reversed_at,reversal_id,main_period_id) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (row['id'], part, row['defective_qty'], row['created_at'], _now(),
             reversed_at if reversed_part else '', reversal_id if reversed_part else '', period_id),
        )


def delete_defective_batch(batch_id: int, *, reversed_parts: set[str] | None = None,
                          reversed_at: str = '', reversal_id: str = '') -> bool:
    period_id = get_current_main_period_id()
    with get_conn() as conn:
        _archive_deleted_defectives(conn, 'batch_id=?', batch_id, period_id=period_id, reversed_parts=reversed_parts,
                                   reversed_at=reversed_at, reversal_id=reversal_id)
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
                  AND UPPER(TRIM(part_number)) NOT LIKE 'EC-1%'
                  AND UPPER(TRIM(part_number)) NOT LIKE 'EC-2%'
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
                 AND UPPER(TRIM(part_number)) NOT LIKE 'EC-1%'
                 AND UPPER(TRIM(part_number)) NOT LIKE 'EC-2%'
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


def get_recent_dispatched_part_usage(months: int = 6) -> list[dict]:
    """彙整近期各料號的已發料訂單數、用量與最後使用時間。"""
    normalized_months = max(1, min(int(months or 6), 24))
    cutoff = (local_now() - timedelta(days=normalized_months * 30)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT part_number,
                      SUM(needed_qty) AS total_qty,
                      COUNT(DISTINCT order_id) AS order_count,
                      MAX(dispatched_at) AS last_used_at
               FROM dispatch_records
               WHERE dispatched_at >= ?
                 AND decision != 'Shortage'
                 AND TRIM(part_number) != ''
               GROUP BY UPPER(TRIM(part_number))
               ORDER BY order_count DESC, total_qty DESC, part_number""",
            (cutoff,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_dispatch_part_numbers() -> list[str]:
    """取得可查詢發料歷史的料號清單（沿用 get_part_dispatch_history 的口徑）。"""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT DISTINCT UPPER(TRIM(dr.part_number)) AS part_number
               FROM dispatch_records dr
               JOIN orders o ON o.id = dr.order_id
               WHERE o.status IN ('dispatched', 'completed')
                 AND dr.dispatched_at != ''
                 AND dr.decision != 'Shortage'
                 AND TRIM(dr.part_number) != ''"""
        ).fetchall()
    return [row["part_number"] for row in rows]


def get_part_dispatch_history(part_number: str, days: int = 30) -> list[dict]:
    """彙整已發料／已完成訂單的 BOM 用量，沿用分析頁排除缺料的口徑。"""
    normalized_part = str(part_number or "").strip().upper()
    if not normalized_part:
        return []

    normalized_days = max(0, min(int(days or 0), 3650))
    params: list[object] = [normalized_part]
    cutoff_sql = ""
    if normalized_days:
        cutoff_at = (local_now() - timedelta(days=normalized_days)).isoformat()
        cutoff_sql = " AND dr.dispatched_at >= ?"
        params.append(cutoff_at)

    with get_conn() as conn:
        rows = conn.execute(
            f"""SELECT o.id AS order_id,
                       o.code,
                       o.po_number,
                       o.model,
                       o.ship_date,
                       MAX(dr.dispatched_at) AS dispatched_at,
                       SUM(dr.needed_qty) AS issued_qty,
                       COUNT(*) AS record_count
                FROM dispatch_records dr
                JOIN orders o ON o.id = dr.order_id
                WHERE UPPER(TRIM(dr.part_number)) = ?
                  AND o.status IN ('dispatched', 'completed')
                  AND dr.dispatched_at != ''
                  AND dr.decision != 'Shortage'
                  {cutoff_sql}
                GROUP BY o.id, o.code, o.po_number, o.model, o.ship_date
                ORDER BY dispatched_at DESC, o.id DESC""",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


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
