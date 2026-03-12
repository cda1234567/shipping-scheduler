from __future__ import annotations

import argparse
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
from app.services.desktop_launcher import (
    DARK_MODE_SETTING,
    DOWNLOAD_DIR_SETTING,
    build_unique_download_path,
    format_bool_setting,
    get_default_download_directory,
    is_autostart_enabled,
    normalize_download_directory,
    parse_bool_setting,
    parse_content_disposition_filename,
    set_autostart_enabled,
)

BASE_DIR = Path(__file__).resolve().parent
APP_HOST = "127.0.0.1"
APP_PORT = 8765
APP_URL = f"http://{APP_HOST}:{APP_PORT}/"


class LocalServer:
    def __init__(self, app_url: str):
        self.app_url = app_url
        self.server: uvicorn.Server | None = None
        self.thread: threading.Thread | None = None
        self.started_here = False

    @staticmethod
    def _url_ready(url: str) -> bool:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                return 200 <= response.status < 500
        except Exception:
            return False

    def ensure_started(self):
        if self._url_ready(self.app_url):
            return

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
            if self._url_ready(self.app_url):
                return
            time.sleep(0.2)
        raise RuntimeError("Desktop app failed to start the local server in time.")

    def stop(self):
        if not self.started_here or not self.server:
            return
        self.server.should_exit = True
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

    def restart(self):
        if self.started_here:
            self.stop()
            self.server = None
            self.thread = None
            self.started_here = False
        self.ensure_started()


class DesktopBridge:
    def __init__(self, server: LocalServer, args: argparse.Namespace):
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
            "app_url": APP_URL,
            "server_started_here": self.server.started_here,
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
    def _request_url(path: str) -> str:
        return urllib.parse.urljoin(APP_URL, str(path or "").lstrip("/"))

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
            self._request_url(path),
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
        webbrowser.open(APP_URL)
        return {"ok": True}

    def reload_app(self):
        if self.server.started_here:
            self.server.restart()
        if self.window:
            refresh_url = f"{APP_URL}?_reload={int(time.time() * 1000)}"
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
    return parser


def main():
    os.chdir(BASE_DIR)
    args = build_parser().parse_args()
    server = LocalServer(APP_URL)
    server.ensure_started()

    bridge = DesktopBridge(server, args)
    window = webview.create_window(
        "OpenText 出貨排程系統",
        APP_URL,
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

    try:
        webview.start(debug=False, private_mode=False)
    finally:
        server.stop()


if __name__ == "__main__":
    main()
