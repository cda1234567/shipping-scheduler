from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.desktop_launcher import (
    DOWNLOAD_MODE_ASK_EACH_TIME,
    DOWNLOAD_MODE_FIXED,
    build_autostart_command,
    build_unique_download_path,
    format_bool_setting,
    get_desktop_app_icon_path,
    get_default_download_directory,
    get_startup_shortcut_path,
    is_autostart_enabled,
    normalize_download_directory,
    normalize_download_mode,
    parse_bool_setting,
    parse_content_disposition_filename,
    resolve_pythonw_executable,
    set_autostart_enabled,
)


class DesktopLauncherTests(unittest.TestCase):
    def test_resolve_pythonw_executable_prefers_pythonw(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            python_exe = Path(temp_dir) / "python.exe"
            pythonw_exe = Path(temp_dir) / "pythonw.exe"
            python_exe.write_text("", encoding="utf-8")
            pythonw_exe.write_text("", encoding="utf-8")

            resolved = resolve_pythonw_executable(str(python_exe))

        self.assertEqual(resolved, str(pythonw_exe))

    def test_build_autostart_command_for_script_mode_uses_pythonw(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            python_exe = Path(temp_dir) / "python.exe"
            pythonw_exe = Path(temp_dir) / "pythonw.exe"
            script_path = Path(temp_dir) / "desktop_app.py"
            python_exe.write_text("", encoding="utf-8")
            pythonw_exe.write_text("", encoding="utf-8")
            script_path.write_text("", encoding="utf-8")

            target, arguments, working_directory = build_autostart_command(
                entry_script=str(script_path),
                current_executable=str(python_exe),
                frozen=False,
            )

            self.assertEqual(Path(target).name.lower(), "pythonw.exe")
            self.assertEqual(Path(target).parent.name, pythonw_exe.parent.name)
            self.assertIn("desktop_app.py", arguments)
            self.assertIn("--autostart", arguments)
            self.assertIn("--minimized", arguments)
            self.assertEqual(Path(working_directory).name, script_path.parent.name)

    def test_build_autostart_command_for_frozen_mode_uses_current_executable(self):
        target, arguments, working_directory = build_autostart_command(
            current_executable="C:/DispatchScheduler/DispatchSchedulerDesktop.exe",
            frozen=True,
        )

        self.assertEqual(target, str(Path("C:/DispatchScheduler/DispatchSchedulerDesktop.exe").resolve()))
        self.assertEqual(arguments, "--autostart --minimized")
        self.assertEqual(working_directory, str(Path("C:/DispatchScheduler").resolve()))

    def test_get_startup_shortcut_path_uses_appdata(self):
        shortcut_path = get_startup_shortcut_path(
            appdata=r"C:\Users\Andy\AppData\Roaming",
            shortcut_name="App.lnk",
        )

        self.assertEqual(
            shortcut_path,
            Path(r"C:\Users\Andy\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\App.lnk"),
        )

    def test_get_desktop_app_icon_path_points_to_static_asset(self):
        icon_path = get_desktop_app_icon_path(r"C:\DispatchScheduler")
        self.assertEqual(icon_path, Path(r"C:\DispatchScheduler\static\assets\dispatch_app_icon.ico"))

    def test_parse_bool_setting(self):
        self.assertTrue(parse_bool_setting("1"))
        self.assertTrue(parse_bool_setting("true"))
        self.assertFalse(parse_bool_setting("0"))
        self.assertFalse(parse_bool_setting(""))
        self.assertEqual(format_bool_setting(True), "1")
        self.assertEqual(format_bool_setting(False), "0")

    def test_default_and_normalized_download_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            home = Path(temp_dir)
            downloads = home / "Downloads"
            downloads.mkdir()

            self.assertEqual(get_default_download_directory(temp_dir), downloads)
            self.assertEqual(normalize_download_directory(str(downloads), temp_dir), downloads)
            self.assertEqual(normalize_download_directory(str(home / "missing"), temp_dir), downloads)

    def test_build_unique_download_path_appends_suffix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            first = directory / "report.xlsx"
            first.write_text("x", encoding="utf-8")

            candidate = build_unique_download_path(directory, "report.xlsx")

        self.assertEqual(candidate.name, "report_1.xlsx")

    def test_parse_content_disposition_filename(self):
        self.assertEqual(
            parse_content_disposition_filename("attachment; filename*=UTF-8''BOM%20zip.zip"),
            "BOM zip.zip",
        )
        self.assertEqual(
            parse_content_disposition_filename('attachment; filename="dispatch.xlsx"'),
            "dispatch.xlsx",
        )
        self.assertEqual(parse_content_disposition_filename(""), "")

    def test_set_autostart_enabled_removes_shortcut_when_disabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            shortcut_path = get_startup_shortcut_path(temp_dir)
            shortcut_path.parent.mkdir(parents=True, exist_ok=True)
            shortcut_path.write_text("old", encoding="utf-8")

            result = set_autostart_enabled(False, appdata=temp_dir)

        self.assertEqual(result, shortcut_path)
        self.assertFalse(shortcut_path.exists())

    def test_set_autostart_enabled_creates_shortcut_when_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = Path(temp_dir) / "desktop_app.py"
            python_exe = Path(temp_dir) / "python.exe"
            pythonw_exe = Path(temp_dir) / "pythonw.exe"
            script_path.write_text("", encoding="utf-8")
            python_exe.write_text("", encoding="utf-8")
            pythonw_exe.write_text("", encoding="utf-8")

            with patch("app.services.desktop_launcher.create_windows_shortcut") as mock_shortcut:
                shortcut_path = set_autostart_enabled(
                    True,
                    entry_script=str(script_path),
                    current_executable=str(python_exe),
                    frozen=False,
                    appdata=temp_dir,
                )

                expected_path = get_startup_shortcut_path(temp_dir)
                self.assertEqual(shortcut_path, expected_path)
                mock_shortcut.assert_called_once()
                args, kwargs = mock_shortcut.call_args
                self.assertEqual(args[0], str(expected_path))
                self.assertEqual(Path(args[1]).name.lower(), "pythonw.exe")
                self.assertEqual(Path(args[1]).parent.name, pythonw_exe.parent.name)
                self.assertIn("desktop_app.py", kwargs["arguments"])
                self.assertIn("--autostart", kwargs["arguments"])
                self.assertIn("--minimized", kwargs["arguments"])
                self.assertEqual(Path(kwargs["working_directory"]).name, script_path.parent.name)
                self.assertEqual(kwargs["icon_path"], "")

    def test_set_autostart_enabled_passes_icon_path_to_shortcut(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = Path(temp_dir) / "desktop_app.py"
            python_exe = Path(temp_dir) / "python.exe"
            pythonw_exe = Path(temp_dir) / "pythonw.exe"
            icon_path = Path(temp_dir) / "static" / "assets" / "dispatch_app_icon.ico"
            script_path.write_text("", encoding="utf-8")
            python_exe.write_text("", encoding="utf-8")
            pythonw_exe.write_text("", encoding="utf-8")
            icon_path.parent.mkdir(parents=True, exist_ok=True)
            icon_path.write_text("", encoding="utf-8")

            with patch("app.services.desktop_launcher.create_windows_shortcut") as mock_shortcut:
                set_autostart_enabled(
                    True,
                    entry_script=str(script_path),
                    current_executable=str(python_exe),
                    frozen=False,
                    appdata=temp_dir,
                    icon_path=str(icon_path),
                )

                _, kwargs = mock_shortcut.call_args
                self.assertEqual(kwargs["icon_path"], str(icon_path.resolve()))

    def test_is_autostart_enabled_checks_shortcut_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertFalse(is_autostart_enabled(temp_dir))
            shortcut_path = get_startup_shortcut_path(temp_dir)
            shortcut_path.parent.mkdir(parents=True, exist_ok=True)
            shortcut_path.write_text("", encoding="utf-8")
            self.assertTrue(is_autostart_enabled(temp_dir))

    def test_normalize_download_mode_defaults_to_fixed(self):
        self.assertEqual(normalize_download_mode(None), DOWNLOAD_MODE_FIXED)
        self.assertEqual(normalize_download_mode(""), DOWNLOAD_MODE_FIXED)
        self.assertEqual(normalize_download_mode("something-else"), DOWNLOAD_MODE_FIXED)

    def test_normalize_download_mode_accepts_prompt_mode(self):
        self.assertEqual(normalize_download_mode("ask_each_time"), DOWNLOAD_MODE_ASK_EACH_TIME)
        self.assertEqual(normalize_download_mode("ASK_EACH_TIME"), DOWNLOAD_MODE_ASK_EACH_TIME)


if __name__ == "__main__":
    unittest.main()
