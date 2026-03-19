from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.runtime_paths import get_app_base_dir, get_config_file_path, get_resource_base_dir


class RuntimePathTests(unittest.TestCase):
    def test_get_app_base_dir_uses_repo_root_in_script_mode(self):
        expected = Path(__file__).resolve().parents[1]
        self.assertEqual(get_app_base_dir(), expected)

    def test_get_resource_base_dir_prefers_meipass_when_present(self):
        with patch.object(sys, "_MEIPASS", "C:/bundle-temp", create=True):
            self.assertEqual(get_resource_base_dir(), Path("C:/bundle-temp").resolve())

    def test_get_config_file_path_prefers_external_config_next_to_frozen_executable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app_dir = Path(temp_dir) / "portable-app"
            bundle_dir = Path(temp_dir) / "bundle"
            app_dir.mkdir()
            bundle_dir.mkdir()
            (app_dir / "config.yaml").write_text("external: true", encoding="utf-8")
            (bundle_dir / "config.yaml").write_text("bundled: true", encoding="utf-8")

            with patch.object(sys, "frozen", True, create=True), \
                 patch.object(sys, "executable", str(app_dir / "OpenTextDesktop.exe"), create=True), \
                 patch.object(sys, "_MEIPASS", str(bundle_dir), create=True):
                self.assertEqual(get_config_file_path().resolve(), (app_dir / "config.yaml").resolve())

    def test_get_config_file_path_falls_back_to_bundled_config_when_external_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app_dir = Path(temp_dir) / "portable-app"
            bundle_dir = Path(temp_dir) / "bundle"
            app_dir.mkdir()
            bundle_dir.mkdir()
            (bundle_dir / "config.yaml").write_text("bundled: true", encoding="utf-8")

            with patch.object(sys, "frozen", True, create=True), \
                 patch.object(sys, "executable", str(app_dir / "OpenTextDesktop.exe"), create=True), \
                 patch.object(sys, "_MEIPASS", str(bundle_dir), create=True):
                self.assertEqual(get_config_file_path().resolve(), (bundle_dir / "config.yaml").resolve())


if __name__ == "__main__":
    unittest.main()
