from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

SHORTCUT_NAME = "OpenText Shipping Scheduler.lnk"
DOWNLOAD_DIR_SETTING = "desktop_download_dir"
DARK_MODE_SETTING = "desktop_dark_mode"
_TRUE_VALUES = {"1", "true", "yes", "on"}


def _ps_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_bool_setting(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in _TRUE_VALUES


def format_bool_setting(value: bool) -> str:
    return "1" if value else "0"


def get_startup_folder(appdata: str | None = None) -> Path:
    appdata_root = appdata or os.environ.get("APPDATA")
    if appdata_root:
        return Path(appdata_root) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
    return Path.home() / "AppData" / "Roaming" / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"


def get_startup_shortcut_path(appdata: str | None = None, shortcut_name: str = SHORTCUT_NAME) -> Path:
    return get_startup_folder(appdata) / shortcut_name


def get_default_download_directory(home: str | None = None) -> Path:
    home_path = Path(home).expanduser() if home else Path.home()
    downloads = home_path / "Downloads"
    if downloads.exists():
        return downloads
    return home_path


def normalize_download_directory(path: str | None, home: str | None = None) -> Path:
    candidate = Path(str(path or "")).expanduser()
    if candidate.exists() and candidate.is_dir():
        return candidate
    return get_default_download_directory(home)


def build_unique_download_path(directory: str | Path, filename: str) -> Path:
    safe_name = Path(filename or "download.bin").name or "download.bin"
    base_path = Path(directory) / safe_name
    if not base_path.exists():
        return base_path

    stem = base_path.stem
    suffix = base_path.suffix
    counter = 1
    while True:
        candidate = base_path.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def parse_content_disposition_filename(header_value: str | None) -> str:
    text = str(header_value or "").strip()
    if not text:
        return ""

    match = re.search(r"filename\*=UTF-8''([^;]+)", text, flags=re.IGNORECASE)
    if match:
        from urllib.parse import unquote

        return Path(unquote(match.group(1).strip().strip('"'))).name

    match = re.search(r'filename="?([^";]+)"?', text, flags=re.IGNORECASE)
    if match:
        return Path(match.group(1).strip()).name

    return ""


def resolve_pythonw_executable(current_executable: str | None = None) -> str:
    executable = Path(current_executable or sys.executable)
    pythonw = executable.with_name("pythonw.exe")
    return str(pythonw if pythonw.exists() else executable)


def build_autostart_command(
    *,
    entry_script: str | None = None,
    current_executable: str | None = None,
    frozen: bool | None = None,
) -> tuple[str, str, str]:
    executable = Path(current_executable or sys.executable).resolve()
    if frozen is None:
        frozen = bool(getattr(sys, "frozen", False))

    if frozen:
        return str(executable), "--autostart --minimized", str(executable.parent)

    script_path = Path(entry_script or "desktop_app.py").resolve()
    return (
        resolve_pythonw_executable(str(executable)),
        f'"{script_path}" --autostart --minimized',
        str(script_path.parent),
    )


def create_windows_shortcut(shortcut_path: str, target_path: str, arguments: str = "", working_directory: str = ""):
    script = f"""
$ErrorActionPreference = 'Stop'
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut({_ps_quote(shortcut_path)})
$shortcut.TargetPath = {_ps_quote(target_path)}
$shortcut.Arguments = {_ps_quote(arguments)}
$shortcut.WorkingDirectory = {_ps_quote(working_directory)}
$shortcut.IconLocation = {_ps_quote(target_path)}
$shortcut.Save()
"""
    subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        check=True,
        capture_output=True,
        text=True,
    )


def is_autostart_enabled(appdata: str | None = None, shortcut_name: str = SHORTCUT_NAME) -> bool:
    return get_startup_shortcut_path(appdata, shortcut_name).exists()


def set_autostart_enabled(
    enabled: bool,
    *,
    entry_script: str | None = None,
    current_executable: str | None = None,
    frozen: bool | None = None,
    appdata: str | None = None,
    shortcut_name: str = SHORTCUT_NAME,
):
    shortcut_path = get_startup_shortcut_path(appdata, shortcut_name)
    shortcut_path.parent.mkdir(parents=True, exist_ok=True)

    if not enabled:
        shortcut_path.unlink(missing_ok=True)
        return shortcut_path

    target_path, arguments, working_directory = build_autostart_command(
        entry_script=entry_script,
        current_executable=current_executable,
        frozen=frozen,
    )
    create_windows_shortcut(
        str(shortcut_path),
        target_path,
        arguments=arguments,
        working_directory=working_directory,
    )
    return shortcut_path
