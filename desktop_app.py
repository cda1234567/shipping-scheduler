from __future__ import annotations

import argparse
import os
import sys
import threading
import time
import urllib.request
import webbrowser
from pathlib import Path

import uvicorn
import webview

from app.services.desktop_launcher import is_autostart_enabled, set_autostart_enabled

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

        deadline = time.time() + 20
        while time.time() < deadline:
            if self._url_ready(self.app_url):
                return
            time.sleep(0.2)
        raise RuntimeError("桌面版啟動失敗：後端服務沒有在預期時間內就緒")

    def stop(self):
        if not self.started_here or not self.server:
            return
        self.server.should_exit = True
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)


class DesktopBridge:
    def __init__(self, server: LocalServer, args: argparse.Namespace):
        self.server = server
        self.args = args
        self.window: webview.Window | None = None
        self.allow_close = False

    def bind_window(self, window: webview.Window):
        self.window = window

    def get_state(self):
        return {
            "desktop_mode": True,
            "autostart_enabled": is_autostart_enabled(),
            "autostart_managed": os.name == "nt",
            "app_url": APP_URL,
            "server_started_here": self.server.started_here,
        }

    def set_autostart(self, enabled: bool):
        if os.name != "nt":
            return {"ok": False, "message": "目前只支援 Windows 開機自動啟動"}
        set_autostart_enabled(
            bool(enabled),
            entry_script=str(BASE_DIR / "desktop_app.py"),
            current_executable=sys.executable,
            frozen=bool(getattr(sys, "frozen", False)),
        )
        state = self.get_state()
        state["ok"] = True
        return state

    def minimize_window(self):
        if self.window:
            self.window.minimize()
        return {"ok": True}

    def open_in_browser(self):
        webbrowser.open(APP_URL)
        return {"ok": True}

    def quit_app(self):
        self.allow_close = True
        if self.window:
            threading.Thread(target=self.window.destroy, daemon=True).start()
        return {"ok": True}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OpenText 出貨排程系統桌面版")
    parser.add_argument("--autostart", action="store_true", help="由開機自動啟動捷徑呼叫")
    parser.add_argument("--minimized", action="store_true", help="啟動時縮小到工作列")
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
