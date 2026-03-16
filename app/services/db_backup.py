from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path

from .. import database as db
from ..config import BACKUP_DIR, cfg

DATABASE_BACKUP_DIRNAME = "database"

SETTING_ENABLED = "db_backup_enabled"
SETTING_HOUR = "db_backup_hour"
SETTING_MINUTE = "db_backup_minute"
SETTING_KEEP_COUNT = "db_backup_keep_count"
SETTING_LAST_BACKUP_AT = "db_backup_last_backup_at"
SETTING_LAST_BACKUP_NAME = "db_backup_last_backup_name"
SETTING_LAST_BACKUP_REASON = "db_backup_last_backup_reason"
SETTING_LAST_SCHEDULED_RUN_AT = "db_backup_last_scheduled_run_at"
SETTING_LAST_RESTORE_AT = "db_backup_last_restore_at"
SETTING_LAST_RESTORE_FILE = "db_backup_last_restore_file"
SETTING_LAST_ERROR = "db_backup_last_error"

_DEFAULT_ENABLED = True
_DEFAULT_HOUR = 2
_DEFAULT_MINUTE = 0
_DEFAULT_KEEP_COUNT = 14
_DEFAULT_CHECK_INTERVAL_SECONDS = 60

_operation_lock = threading.RLock()


def _as_bool(value, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _as_int(value, default: int, *, minimum: int, maximum: int) -> int:
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(maximum, number))


def _parse_iso_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def get_database_backup_dir(backup_dir: Path | str | None = None) -> Path:
    target = Path(backup_dir) if backup_dir else BACKUP_DIR / DATABASE_BACKUP_DIRNAME
    target.mkdir(parents=True, exist_ok=True)
    return target


def _get_database_path(database_path: Path | str | None = None) -> Path:
    return Path(database_path) if database_path else db.DB_PATH


def get_database_backup_settings() -> dict:
    enabled_default = _as_bool(cfg("database_backup.enabled", _DEFAULT_ENABLED), _DEFAULT_ENABLED)
    hour_default = _as_int(cfg("database_backup.hour", _DEFAULT_HOUR), _DEFAULT_HOUR, minimum=0, maximum=23)
    minute_default = _as_int(cfg("database_backup.minute", _DEFAULT_MINUTE), _DEFAULT_MINUTE, minimum=0, maximum=59)
    keep_count_default = _as_int(
        cfg("database_backup.keep_count", _DEFAULT_KEEP_COUNT),
        _DEFAULT_KEEP_COUNT,
        minimum=1,
        maximum=365,
    )

    enabled = _as_bool(db.get_setting(SETTING_ENABLED, str(enabled_default)), enabled_default)
    hour = _as_int(db.get_setting(SETTING_HOUR, str(hour_default)), hour_default, minimum=0, maximum=23)
    minute = _as_int(db.get_setting(SETTING_MINUTE, str(minute_default)), minute_default, minimum=0, maximum=59)
    keep_count = _as_int(
        db.get_setting(SETTING_KEEP_COUNT, str(keep_count_default)),
        keep_count_default,
        minimum=1,
        maximum=365,
    )

    last_backup_at = db.get_setting(SETTING_LAST_BACKUP_AT)
    last_backup_name = db.get_setting(SETTING_LAST_BACKUP_NAME)
    last_backup_reason = db.get_setting(SETTING_LAST_BACKUP_REASON)
    last_scheduled_run_at = db.get_setting(SETTING_LAST_SCHEDULED_RUN_AT)
    last_restore_at = db.get_setting(SETTING_LAST_RESTORE_AT)
    last_restore_file = db.get_setting(SETTING_LAST_RESTORE_FILE)
    last_error = db.get_setting(SETTING_LAST_ERROR)

    due_now = is_database_backup_due(
        datetime.now(),
        {
            "enabled": enabled,
            "hour": hour,
            "minute": minute,
            "last_scheduled_run_at": last_scheduled_run_at,
        },
    )

    return {
        "enabled": enabled,
        "hour": hour,
        "minute": minute,
        "keep_count": keep_count,
        "backup_dir": str(get_database_backup_dir()),
        "last_backup_at": last_backup_at,
        "last_backup_name": last_backup_name,
        "last_backup_reason": last_backup_reason,
        "last_scheduled_run_at": last_scheduled_run_at,
        "last_restore_at": last_restore_at,
        "last_restore_file": last_restore_file,
        "last_error": last_error,
        "due_now": due_now,
        "next_run_at": compute_next_database_backup_run(
            {
                "enabled": enabled,
                "hour": hour,
                "minute": minute,
                "last_scheduled_run_at": last_scheduled_run_at,
            }
        ),
    }


def update_database_backup_settings(*, enabled: bool, hour: int, minute: int, keep_count: int) -> dict:
    normalized_hour = _as_int(hour, _DEFAULT_HOUR, minimum=0, maximum=23)
    normalized_minute = _as_int(minute, _DEFAULT_MINUTE, minimum=0, maximum=59)
    normalized_keep_count = _as_int(keep_count, _DEFAULT_KEEP_COUNT, minimum=1, maximum=365)

    db.set_setting(SETTING_ENABLED, "1" if enabled else "0")
    db.set_setting(SETTING_HOUR, str(normalized_hour))
    db.set_setting(SETTING_MINUTE, str(normalized_minute))
    db.set_setting(SETTING_KEEP_COUNT, str(normalized_keep_count))
    db.log_activity(
        "database_backup_settings_updated",
        f"enabled={int(bool(enabled))}, time={normalized_hour:02d}:{normalized_minute:02d}, keep={normalized_keep_count}",
    )

    prune_database_backups(keep_count=normalized_keep_count)
    return get_database_backup_settings()


def get_database_backup_filename(now: datetime | None = None) -> str:
    current = now or datetime.now()
    return f"system_backup_{current.strftime('%Y%m%d_%H%M%S')}.db"


def list_database_backups(
    *,
    backup_dir: Path | str | None = None,
    limit: int | None = None,
) -> list[dict]:
    root = get_database_backup_dir(backup_dir)
    backups: list[dict] = []
    for path in root.glob("system_backup_*.db"):
        if not path.is_file():
            continue
        stat = path.stat()
        backups.append(
            {
                "name": path.name,
                "path": str(path),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            }
        )
    backups.sort(key=lambda item: (item["created_at"], item["name"]), reverse=True)
    if limit is not None:
        return backups[: max(0, int(limit))]
    return backups


def prune_database_backups(
    *,
    keep_count: int,
    backup_dir: Path | str | None = None,
) -> int:
    normalized_keep_count = _as_int(keep_count, _DEFAULT_KEEP_COUNT, minimum=1, maximum=365)
    backups = list_database_backups(backup_dir=backup_dir)
    removed = 0
    for item in backups[normalized_keep_count:]:
        path = Path(item["path"])
        if not path.exists():
            continue
        path.unlink()
        removed += 1
    return removed


def _ensure_unique_backup_path(target_path: Path) -> Path:
    if not target_path.exists():
        return target_path
    stem = target_path.stem
    suffix = target_path.suffix
    parent = target_path.parent
    index = 1
    while True:
        candidate = parent / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def _copy_database(source_path: Path, target_path: Path, *, target_wal: bool) -> None:
    source_conn = sqlite3.connect(str(source_path), timeout=30)
    target_conn = sqlite3.connect(str(target_path), timeout=30)
    try:
        target_conn.execute(f"PRAGMA journal_mode={'WAL' if target_wal else 'DELETE'}")
        if target_wal:
            target_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        source_conn.backup(target_conn)
        target_conn.commit()
        if target_wal:
            target_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        target_conn.close()
        source_conn.close()


def create_database_backup(
    *,
    reason: str = "manual",
    keep_count: int | None = None,
    backup_dir: Path | str | None = None,
    database_path: Path | str | None = None,
    now: datetime | None = None,
    prune: bool = True,
) -> dict:
    backup_root = get_database_backup_dir(backup_dir)
    live_db_path = _get_database_path(database_path)
    timestamp = now or datetime.now()
    target_path = _ensure_unique_backup_path(backup_root / get_database_backup_filename(timestamp))
    normalized_keep_count = keep_count
    if normalized_keep_count is None:
        normalized_keep_count = get_database_backup_settings()["keep_count"]

    with _operation_lock:
        _copy_database(live_db_path, target_path, target_wal=False)
        removed = prune_database_backups(
            keep_count=_as_int(normalized_keep_count, _DEFAULT_KEEP_COUNT, minimum=1, maximum=365),
            backup_dir=backup_root,
        ) if prune else 0

    backup_info = {
        "name": target_path.name,
        "path": str(target_path),
        "size_bytes": target_path.stat().st_size,
        "created_at": timestamp.isoformat(timespec="seconds"),
        "reason": reason,
        "removed_count": removed,
    }
    _remember_backup_success(backup_info)
    return backup_info


def _resolve_backup_path(backup_name: str, *, backup_dir: Path | str | None = None) -> Path:
    name = Path(str(backup_name or "").strip()).name
    if not name:
        raise FileNotFoundError("未指定備份檔案")
    path = get_database_backup_dir(backup_dir) / name
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"找不到備份檔：{name}")
    return path


def restore_database_backup(
    backup_name: str,
    *,
    backup_dir: Path | str | None = None,
    database_path: Path | str | None = None,
    now: datetime | None = None,
) -> dict:
    backup_root = get_database_backup_dir(backup_dir)
    live_db_path = _get_database_path(database_path)
    source_path = _resolve_backup_path(backup_name, backup_dir=backup_root)
    restore_time = now or datetime.now()

    with _operation_lock:
        safety_backup = create_database_backup(
            reason="pre_restore",
            keep_count=get_database_backup_settings()["keep_count"],
            backup_dir=backup_root,
            database_path=live_db_path,
            now=restore_time,
            prune=False,
        )
        _copy_database(source_path, live_db_path, target_wal=True)
        db.init_db()
        pruned = prune_database_backups(
            keep_count=get_database_backup_settings()["keep_count"],
            backup_dir=backup_root,
        )

    db.set_setting(SETTING_LAST_RESTORE_AT, restore_time.isoformat(timespec="seconds"))
    db.set_setting(SETTING_LAST_RESTORE_FILE, source_path.name)
    db.set_setting(SETTING_LAST_BACKUP_AT, safety_backup["created_at"])
    db.set_setting(SETTING_LAST_BACKUP_NAME, safety_backup["name"])
    db.set_setting(SETTING_LAST_BACKUP_REASON, safety_backup["reason"])
    db.set_setting(SETTING_LAST_ERROR, "")
    db.log_activity(
        "database_backup_restored",
        f"restore={source_path.name}, safety={safety_backup['name']}, removed={pruned}",
    )

    return {
        "restored_backup": {
            "name": source_path.name,
            "path": str(source_path),
        },
        "safety_backup": safety_backup,
        "restored_at": restore_time.isoformat(timespec="seconds"),
        "removed_count": pruned,
    }


def _remember_backup_success(backup_info: dict) -> None:
    db.set_setting(SETTING_LAST_BACKUP_AT, backup_info["created_at"])
    db.set_setting(SETTING_LAST_BACKUP_NAME, backup_info["name"])
    db.set_setting(SETTING_LAST_BACKUP_REASON, backup_info["reason"])
    if backup_info["reason"] == "scheduled":
        db.set_setting(SETTING_LAST_SCHEDULED_RUN_AT, backup_info["created_at"])
    db.set_setting(SETTING_LAST_ERROR, "")
    db.log_activity(
        "database_backup_created",
        f"{backup_info['name']} ({backup_info['reason']})",
    )


def _remember_backup_failure(error: Exception) -> None:
    message = str(error).strip() or "未知錯誤"
    db.set_setting(SETTING_LAST_ERROR, message)
    db.log_activity("database_backup_failed", message)


def is_database_backup_due(now: datetime, settings: dict) -> bool:
    if not settings.get("enabled"):
        return False

    scheduled_at = now.replace(
        hour=_as_int(settings.get("hour"), _DEFAULT_HOUR, minimum=0, maximum=23),
        minute=_as_int(settings.get("minute"), _DEFAULT_MINUTE, minimum=0, maximum=59),
        second=0,
        microsecond=0,
    )
    if now < scheduled_at:
        return False

    last_scheduled_run = _parse_iso_datetime(settings.get("last_scheduled_run_at", ""))
    if last_scheduled_run and last_scheduled_run.date() == now.date():
        return False
    return True


def compute_next_database_backup_run(settings: dict, now: datetime | None = None) -> str:
    current = now or datetime.now()
    if not settings.get("enabled"):
        return ""

    scheduled_today = current.replace(
        hour=_as_int(settings.get("hour"), _DEFAULT_HOUR, minimum=0, maximum=23),
        minute=_as_int(settings.get("minute"), _DEFAULT_MINUTE, minimum=0, maximum=59),
        second=0,
        microsecond=0,
    )
    last_scheduled_run = _parse_iso_datetime(settings.get("last_scheduled_run_at", ""))

    if current < scheduled_today:
        return scheduled_today.isoformat(timespec="seconds")
    if last_scheduled_run and last_scheduled_run.date() == current.date():
        return (scheduled_today + timedelta(days=1)).isoformat(timespec="seconds")
    return scheduled_today.isoformat(timespec="seconds")


def maybe_run_scheduled_database_backup(now: datetime | None = None) -> dict | None:
    current = now or datetime.now()
    settings = get_database_backup_settings()
    if not is_database_backup_due(current, settings):
        return None

    try:
        return create_database_backup(
            reason="scheduled",
            keep_count=settings["keep_count"],
            now=current,
        )
    except Exception as error:  # pragma: no cover - scheduler path only
        _remember_backup_failure(error)
        return None


def get_database_backup_overview() -> dict:
    settings = get_database_backup_settings()
    backups = list_database_backups()
    return {
        **settings,
        "backups": backups,
        "backup_count": len(backups),
    }


def get_database_backup_check_interval_seconds() -> int:
    return _as_int(
        cfg("database_backup.check_interval_seconds", _DEFAULT_CHECK_INTERVAL_SECONDS),
        _DEFAULT_CHECK_INTERVAL_SECONDS,
        minimum=15,
        maximum=3600,
    )


class DatabaseBackupScheduler:
    def __init__(self):
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="database-backup-scheduler",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def _run_loop(self) -> None:
        maybe_run_scheduled_database_backup()
        while not self._stop_event.wait(get_database_backup_check_interval_seconds()):
            maybe_run_scheduled_database_backup()


database_backup_scheduler = DatabaseBackupScheduler()
