from __future__ import annotations

import argparse
import ctypes
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

import uvicorn
import webview

from app import database as db
from app.runtime_paths import get_app_base_dir, get_resource_base_dir
from app.services.desktop_connection import resolve_remote_server_url
from app.services.desktop_launcher import (
    DARK_MODE_SETTING,
    DOWNLOAD_DIR_SETTING,
    build_unique_download_path,
    format_bool_setting,
    get_default_download_directory,
    get_desktop_app_icon_path,
    is_autostart_enabled,
    normalize_download_directory,
    parse_bool_setting,
    parse_content_disposition_filename,
    set_autostart_enabled,
)

BASE_DIR = get_app_base_dir()
RESOURCE_DIR = get_resource_base_dir()
APP_ICON_PATH = RESOURCE_DIR / "static" / "assets" / "opentext_app_icon.ico"
APP_USER_MODEL_ID = "OpenText.ShippingScheduler.Desktop"
APP_HOST = "127.0.0.1"
APP_PORT = 8765
LOCAL_APP_URL = f"http://{APP_HOST}:{APP_PORT}/"


def build_app_url(base_url: str) -> str:
    # Force a fresh page load on every desktop launch to avoid stale WebView caches.
    return f"{base_url}?_desktop_shell=1&_desktop={int(time.time() * 1000)}"


def hide_console_window():
    """隱藏 python.exe 的 console 視窗（VBS 用 python 而非 pythonw 啟動時需要）。"""
    if os.name != "nt":
        return
    try:
        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            SW_HIDE = 0
            ctypes.windll.user32.ShowWindow(hwnd, SW_HIDE)
    except Exception:
        pass


def apply_windows_app_identity():
    if os.name != "nt":
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(APP_USER_MODEL_ID)
    except Exception:
        pass


def apply_windows_window_icon(window: webview.Window):
    if os.name != "nt" or not APP_ICON_PATH.exists():
        return
    try:
        native = getattr(window, "native", None)
        hwnd = int(getattr(native, "Handle", 0))
        if not hwnd:
            return

        image_icon = 1
        load_from_file = 0x0010
        default_size = 0x0040
        icon_small = 0
        icon_big = 1
        wm_seticon = 0x0080
        hicon = ctypes.windll.user32.LoadImageW(
            None,
            str(APP_ICON_PATH),
            image_icon,
            0,
            0,
            load_from_file | default_size,
        )
        if not hicon:
            return
        ctypes.windll.user32.SendMessageW(hwnd, wm_seticon, icon_small, hicon)
        ctypes.windll.user32.SendMessageW(hwnd, wm_seticon, icon_big, hicon)
    except Exception:
        pass


def _kill_stale_server(port: int):
    """找到佔用指定 port 的程序並強制結束。"""
    try:
        import subprocess
        result = subprocess.run(
            ["powershell", "-Command",
             f"(Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue).OwningProcess"],
            capture_output=True, text=True, timeout=5,
        )
        pids = set()
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if line.isdigit() and int(line) > 0:
                pids.add(int(line))

        my_pid = os.getpid()
        for pid in pids:
            if pid == my_pid:
                continue
            try:
                subprocess.run(
                    ["powershell", "-Command", f"Stop-Process -Id {pid} -Force -ErrorAction SilentlyContinue"],
                    timeout=5,
                )
            except Exception:
                pass

        if pids:
            time.sleep(0.5)
    except Exception:
        pass


class AppServer:
    def __init__(self, app_url: str, *, managed: bool):
        self.app_url = app_url.rstrip("/") + "/"
        self.managed = bool(managed)
        self.server: uvicorn.Server | None = None
        self.thread: threading.Thread | None = None
        self.started_here = False

    @property
    def is_remote(self) -> bool:
        return not self.managed

    def request_url(self, path: str = "") -> str:
        return urllib.parse.urljoin(self.app_url, str(path or "").lstrip("/"))

    @staticmethod
    def _url_ready(url: str) -> bool:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                return 200 <= response.status < 500
        except Exception:
            return False

    def _health_ok(self) -> bool:
        """確認 server 真的健康（不只是 port 有回應）。"""
        try:
            health_url = self.request_url("/api/health")
            with urllib.request.urlopen(health_url, timeout=3.0) as response:
                if response.status == 200:
                    data = json.loads(response.read())
                    return data.get("ok") is True
        except Exception:
            pass
        return False

    def ensure_started(self):
        if self._health_ok():
            return

        if not self.managed:
            raise RuntimeError(f"找不到遠端服務：{self.app_url}")

        # port 有佔用但不健康 → 殺掉舊程序
        if self._url_ready(self.app_url):
            _kill_stale_server(APP_PORT)
            time.sleep(0.3)

        self._start_server()
        self._wait_until_ready()

    def _start_server(self):
        config = uvicorn.Config(
            "main:app",
            host=APP_HOST,
            port=APP_PORT,
            log_level="warning",
            access_log=False,
            reload=False,
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        self.started_here = True

    def _wait_until_ready(self):
        deadline = time.time() + 20
        while time.time() < deadline:
            if self._health_ok():
                return
            time.sleep(0.3)
        raise RuntimeError("Desktop app failed to start the local server in time.")

    def stop(self):
        if not self.started_here or not self.server:
            return
        self.server.should_exit = True
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

    def restart(self):
        if not self.managed:
            return
        if self.started_here:
            self.stop()
            self.server = None
            self.thread = None
            self.started_here = False
        self.ensure_started()


class DesktopBridge:
    def __init__(self, server: AppServer, args: argparse.Namespace):
        self.server = server
        self.args = args
        self.window: webview.Window | None = None
        self.allow_close = False

    def bind_window(self, window: webview.Window):
        self.window = window

    def _get_download_directory(self) -> Path:
        return normalize_download_directory(db.get_setting(DOWNLOAD_DIR_SETTING, ""))

    def _get_dark_mode(self) -> bool:
        return parse_bool_setting(db.get_setting(DARK_MODE_SETTING, "0"))

    def _build_state(self) -> dict:
        directory = self._get_download_directory()
        stored_value = db.get_setting(DOWNLOAD_DIR_SETTING, "")
        return {
            "desktop_mode": True,
            "autostart_enabled": is_autostart_enabled(),
            "autostart_managed": os.name == "nt",
            "app_url": self.server.app_url,
            "server_started_here": self.server.started_here,
            "remote_server": self.server.is_remote,
            "download_directory": str(directory),
            "download_directory_set": bool(stored_value),
            "dark_mode_enabled": self._get_dark_mode(),
        }

    def get_state(self):
        return self._build_state()

    def set_autostart(self, enabled: bool):
        if os.name != "nt":
            return {"ok": False, "message": "Autostart is only supported on Windows."}
        set_autostart_enabled(
            bool(enabled),
            entry_script=str(BASE_DIR / "desktop_app.py"),
            current_executable=sys.executable,
            frozen=bool(getattr(sys, "frozen", False)),
            icon_path=str(get_desktop_app_icon_path(BASE_DIR)),
        )
        state = self._build_state()
        state["ok"] = True
        return state

    def set_dark_mode(self, enabled: bool):
        db.set_setting(DARK_MODE_SETTING, format_bool_setting(bool(enabled)))
        state = self._build_state()
        state["ok"] = True
        return state

    def choose_download_directory(self):
        if not self.window:
            return {"ok": False, "message": "Desktop window is not ready yet."}

        initial_directory = str(self._get_download_directory())
        selection = self.window.create_file_dialog(webview.FOLDER_DIALOG, directory=initial_directory)
        if not selection:
            state = self._build_state()
            state["ok"] = False
            state["cancelled"] = True
            return state

        chosen_directory = Path(selection[0]).expanduser()
        chosen_directory.mkdir(parents=True, exist_ok=True)
        db.set_setting(DOWNLOAD_DIR_SETTING, str(chosen_directory))
        state = self._build_state()
        state["ok"] = True
        return state

    def _ensure_download_directory(self) -> Path | None:
        stored_value = db.get_setting(DOWNLOAD_DIR_SETTING, "")
        if stored_value:
            stored_directory = Path(stored_value).expanduser()
            if stored_directory.exists() and stored_directory.is_dir():
                stored_directory.mkdir(parents=True, exist_ok=True)
                return stored_directory
            db.set_setting(DOWNLOAD_DIR_SETTING, "")

        chosen_state = self.choose_download_directory()
        if not chosen_state.get("ok"):
            return None
        directory = Path(chosen_state["download_directory"])
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    @staticmethod
    def _read_http_error(error: urllib.error.HTTPError) -> str:
        try:
            payload = json.loads(error.read().decode("utf-8", errors="replace"))
            return str(payload.get("detail") or payload)
        except Exception:
            return f"HTTP {error.code}"

    def download_from_app(self, payload: dict | None = None):
        payload = payload or {}
        directory = self._ensure_download_directory()
        if directory is None:
            return {"ok": False, "cancelled": True, "message": "Download folder selection was cancelled."}

        path = str(payload.get("path") or "").strip()
        if not path:
            return {"ok": False, "message": "Download path is required."}

        method = str(payload.get("method") or "GET").upper()
        body = payload.get("body")
        data = None
        headers: dict[str, str] = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(
            self.server.request_url(path),
            data=data,
            headers=headers,
            method=method,
        )

        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                content = response.read()
                filename = (
                    str(payload.get("filename") or "").strip()
                    or parse_content_disposition_filename(response.headers.get("Content-Disposition"))
                    or Path(urllib.parse.urlparse(path).path).name
                    or "download.bin"
                )
        except urllib.error.HTTPError as error:
            return {"ok": False, "message": self._read_http_error(error)}

        target_path = build_unique_download_path(directory, filename)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(content)
        return {
            "ok": True,
            "filename": target_path.name,
            "path": str(target_path),
            "directory": str(target_path.parent),
        }

    def minimize_window(self):
        if self.window:
            self.window.minimize()
        return {"ok": True}

    def open_in_browser(self):
        webbrowser.open(self.server.app_url)
        return {"ok": True}

    def reload_app(self):
        if self.server.started_here:
            self.server.restart()
        if self.window:
            refresh_url = (
                f"{self.server.app_url}?_desktop_shell=1&_reload={int(time.time() * 1000)}"
            )
            self.window.load_url(refresh_url)
        return {"ok": True}

    def quit_app(self):
        self.allow_close = True
        if self.window:
            threading.Thread(target=self.window.destroy, daemon=True).start()
        return {"ok": True}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OpenText Shipping Scheduler desktop app")
    parser.add_argument("--autostart", action="store_true", help="started from the Windows startup shortcut")
    parser.add_argument("--minimized", action="store_true", help="start minimized to the taskbar")
    parser.add_argument("--server-url", help="connect desktop shell to a remote Docker/server URL instead of starting local Python")
    return parser


def main():
    os.chdir(BASE_DIR)
    apply_windows_app_identity()
    args = build_parser().parse_args()
    remote_server_url = resolve_remote_server_url(
        BASE_DIR,
        cli_url=args.server_url,
        env_url=os.environ.get("OPENTEXT_REMOTE_URL"),
    )
    server = AppServer(remote_server_url or LOCAL_APP_URL, managed=not bool(remote_server_url))
    server.ensure_started()

    bridge = DesktopBridge(server, args)
    window = webview.create_window(
        "OpenText 出貨排程系統",
        build_app_url(server.app_url),
        js_api=bridge,
        width=1480,
        height=920,
        min_size=(1180, 760),
        minimized=bool(args.minimized or args.autostart),
    )
    bridge.bind_window(window)

    def on_closing(window: webview.Window):
        if bridge.allow_close:
            return True
        window.minimize()
        return False

    window.events.closing += on_closing

    def on_webview_ready():
        hide_console_window()
        apply_windows_window_icon(window)

    try:
        webview.start(
            on_webview_ready,
            debug=False,
            private_mode=True,
            icon=str(APP_ICON_PATH) if APP_ICON_PATH.exists() else None,
        )
    finally:
        server.stop()


if __name__ == "__main__":
    main()
