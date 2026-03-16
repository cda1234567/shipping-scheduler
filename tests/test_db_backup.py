from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from app.services import db_backup


def _write_demo_db(path: Path, value: str) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS demo (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("DELETE FROM demo")
        conn.execute("INSERT INTO demo(id, value) VALUES(1, ?)", (value,))
        conn.commit()
    finally:
        conn.close()


def _read_demo_db(path: Path) -> str:
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute("SELECT value FROM demo WHERE id=1").fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


class DatabaseBackupServiceTests(unittest.TestCase):
    def test_create_database_backup_prunes_old_backups(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "system.db"
            backup_dir = temp_path / "backups"
            _write_demo_db(db_path, "v1")

            with patch("app.services.db_backup.db.set_setting"), \
                 patch("app.services.db_backup.db.log_activity"):
                backup_one = db_backup.create_database_backup(
                    reason="manual",
                    keep_count=2,
                    backup_dir=backup_dir,
                    database_path=db_path,
                    now=datetime(2026, 3, 13, 1, 0, 0),
                )
                backup_two = db_backup.create_database_backup(
                    reason="manual",
                    keep_count=2,
                    backup_dir=backup_dir,
                    database_path=db_path,
                    now=datetime(2026, 3, 13, 2, 0, 0),
                )
                backup_three = db_backup.create_database_backup(
                    reason="manual",
                    keep_count=2,
                    backup_dir=backup_dir,
                    database_path=db_path,
                    now=datetime(2026, 3, 13, 3, 0, 0),
                )

            backups = db_backup.list_database_backups(backup_dir=backup_dir)
            names = [item["name"] for item in backups]

            self.assertEqual(len(backups), 2)
            self.assertNotIn(backup_one["name"], names)
            self.assertIn(backup_two["name"], names)
            self.assertIn(backup_three["name"], names)

    def test_restore_database_backup_creates_safety_backup_before_restore(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "system.db"
            backup_dir = temp_path / "backups"
            _write_demo_db(db_path, "v1")

            def fake_get_setting(key: str, default: str = "") -> str:
                defaults = {
                    db_backup.SETTING_KEEP_COUNT: "5",
                    db_backup.SETTING_ENABLED: "1",
                    db_backup.SETTING_HOUR: "2",
                    db_backup.SETTING_MINUTE: "0",
                }
                return defaults.get(key, default)

            with patch("app.services.db_backup.db.set_setting"), \
                 patch("app.services.db_backup.db.log_activity"), \
                 patch("app.services.db_backup.db.init_db"), \
                 patch("app.services.db_backup.db.get_setting", side_effect=fake_get_setting):
                backup_one = db_backup.create_database_backup(
                    reason="manual",
                    keep_count=5,
                    backup_dir=backup_dir,
                    database_path=db_path,
                    now=datetime(2026, 3, 13, 1, 0, 0),
                )

                _write_demo_db(db_path, "v2")
                db_backup.create_database_backup(
                    reason="manual",
                    keep_count=5,
                    backup_dir=backup_dir,
                    database_path=db_path,
                    now=datetime(2026, 3, 13, 2, 0, 0),
                )

                _write_demo_db(db_path, "v3")
                result = db_backup.restore_database_backup(
                    backup_one["name"],
                    backup_dir=backup_dir,
                    database_path=db_path,
                    now=datetime(2026, 3, 13, 3, 0, 0),
                )

            self.assertEqual(_read_demo_db(db_path), "v1")
            self.assertEqual(_read_demo_db(Path(result["safety_backup"]["path"])), "v3")
            self.assertEqual(result["restored_backup"]["name"], backup_one["name"])

    def test_is_database_backup_due_only_runs_once_per_day_after_scheduled_time(self):
        settings = {
            "enabled": True,
            "hour": 2,
            "minute": 0,
            "last_scheduled_run_at": "2026-03-13T02:05:00",
        }

        self.assertFalse(db_backup.is_database_backup_due(datetime(2026, 3, 13, 4, 0, 0), settings))
        self.assertFalse(db_backup.is_database_backup_due(datetime(2026, 3, 13, 1, 59, 0), {**settings, "last_scheduled_run_at": ""}))
        self.assertTrue(db_backup.is_database_backup_due(datetime(2026, 3, 13, 2, 1, 0), {**settings, "last_scheduled_run_at": ""}))
