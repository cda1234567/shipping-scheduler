"""Print helper on-demand restart listener (no polling).

GET  /health  -> {"ok": true}
POST /restart -> restart ShippingScheduler-PrintHelper scheduled task, wait until :8766/health is up.

Env:
  PRINT_HELPER_TOKEN   required (same as print helper)
  RESTART_LISTEN_PORT  default 8767
  RESTART_LISTEN_BIND  default 127.0.0.1
  PRINT_HELPER_TASK    default ShippingScheduler-PrintHelper
  PRINT_HELPER_HEALTH  default http://127.0.0.1:8766/health
  RESTART_WAIT_SEC     default 15
"""
from __future__ import annotations

import hmac
import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from urllib.request import urlopen

TASK = os.environ.get("PRINT_HELPER_TASK", "ShippingScheduler-PrintHelper")
HEALTH = os.environ.get("PRINT_HELPER_HEALTH", "http://127.0.0.1:8766/health")
WAIT_SEC = float(os.environ.get("RESTART_WAIT_SEC", "15"))


def _token() -> str:
    return os.environ.get("PRINT_HELPER_TOKEN", "").strip()


def _json(handler: BaseHTTPRequestHandler, code: int, body: dict) -> None:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _ps(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
        capture_output=True, text=True, timeout=60,
    )


def _helper_ok() -> bool:
    try:
        with urlopen(HEALTH, timeout=3) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return resp.status == 200 and '"ok"' in body and "true" in body
    except Exception:
        return False


def restart_helper() -> dict:
    started = time.time()
    state = _ps(f"(Get-ScheduledTask -TaskName '{TASK}').State").stdout.strip()
    if state == "Running":
        _ps(f"Stop-ScheduledTask -TaskName '{TASK}' -ErrorAction SilentlyContinue")
        time.sleep(1.5)
    start = _ps(f"Start-ScheduledTask -TaskName '{TASK}'; 'STARTED'")
    if start.returncode != 0:
        return {"ok": False, "error": f"Start-ScheduledTask 失敗：{(start.stderr or start.stdout).strip()[:200]}"}
    deadline = time.time() + WAIT_SEC
    while time.time() < deadline:
        if _helper_ok():
            return {"ok": True, "restarted": True, "elapsed_ms": int((time.time() - started) * 1000)}
        time.sleep(0.8)
    return {"ok": False, "error": f"已重啟但 {WAIT_SEC:.0f} 秒內連不到列印小幫手 /health",
            "elapsed_ms": int((time.time() - started) * 1000)}


class Handler(BaseHTTPRequestHandler):
    server_version = "RmPrintHelperRestart/1"

    def _auth(self) -> bool:
        token = _token()
        if not token or not hmac.compare_digest(self.headers.get("X-Print-Token", ""), token):
            _json(self, 401, {"ok": False, "error": "token 錯誤"})
            return False
        return True

    def do_GET(self):
        if urlparse(self.path).path == "/health":
            return _json(self, 200, {"ok": True, "service": "print-helper-restart"})
        _json(self, 404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if urlparse(self.path).path != "/restart":
            return _json(self, 404, {"ok": False, "error": "not found"})
        if not self._auth():
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        result = restart_helper()
        _json(self, 200 if result.get("ok") else 503, result)

    def log_message(self, fmt, *args):
        print("[restart_listener] " + (fmt % args))


def main():
    if not _token():
        raise SystemExit("請先設定 PRINT_HELPER_TOKEN")
    port = int(os.environ.get("RESTART_LISTEN_PORT", "8767"))
    bind = os.environ.get("RESTART_LISTEN_BIND", "127.0.0.1")
    print(f"[restart_listener] listening on {bind}:{port}")
    ThreadingHTTPServer((bind, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
