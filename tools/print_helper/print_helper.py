"""進貨單列印小幫手（Windows 主機端，Python 3 標準函式庫，免安裝套件）。

容器 dispatch-scheduler-localserver 以 POST http://host.docker.internal:8766/print?printer=<名稱>&name=<單號>
送 PDF（Content-Type: application/pdf，標頭 X-Print-Token），本程式用 SumatraPDF 靜默列印到指定的 Windows 印表機。

環境變數：
  PRINT_HELPER_TOKEN    必填，與容器的 RM_PRINT_HELPER_TOKEN 相同
  PRINT_HELPER_PORT     預設 8766
  PRINT_HELPER_BIND     預設 0.0.0.0（容器經 host.docker.internal 連入；請用防火牆限制來源）
  PRINT_HELPER_PRINTERS 允許的印表機（以 ; 分隔），預設 "NPI975BEF (HP LaserJet M111w);HP Color LaserJet M652 [7D2006]"
  SUMATRA_PDF           SumatraPDF.exe 路徑，預設 C:\\Program Files\\SumatraPDF\\SumatraPDF.exe
  SUMATRA_SETTINGS      -print-settings，預設 "noscale"（實際大小，不可縮放，否則半張位置會跑掉）
  PRINT_HELPER_DRY_RUN  1 = 不真的列印，只存檔並回 ok（測試用）
"""
from __future__ import annotations

import hmac
import json
import os
import subprocess
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

MAX_BYTES = 20 * 1024 * 1024
DEFAULT_PRINTERS = "NPI975BEF (HP LaserJet M111w);HP Color LaserJet M652 [7D2006]"


def _cfg():
    return {
        "token": os.environ.get("PRINT_HELPER_TOKEN", "").strip(),
        "printers": [p.strip() for p in os.environ.get("PRINT_HELPER_PRINTERS", DEFAULT_PRINTERS).split(";") if p.strip()],
        "sumatra": os.environ.get("SUMATRA_PDF", r"C:\Program Files\SumatraPDF\SumatraPDF.exe"),
        "settings": os.environ.get("SUMATRA_SETTINGS", "noscale"),
        "dry_run": os.environ.get("PRINT_HELPER_DRY_RUN", "") == "1",
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "RmPrintHelper/1"

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if urlparse(self.path).path == "/health":
            cfg = _cfg()
            return self._send(200, {"ok": True, "printers": cfg["printers"], "sumatra_found": os.path.exists(cfg["sumatra"])})
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        cfg = _cfg()
        url = urlparse(self.path)
        if url.path != "/print":
            return self._send(404, {"ok": False, "error": "not found"})
        if not cfg["token"] or not hmac.compare_digest(self.headers.get("X-Print-Token", ""), cfg["token"]):
            return self._send(401, {"ok": False, "error": "token 錯誤"})
        q = parse_qs(url.query)
        printer = (q.get("printer") or [""])[0]
        if printer not in cfg["printers"]:
            return self._send(400, {"ok": False, "error": f"不允許的印表機：{printer}"})
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BYTES:
            return self._send(400, {"ok": False, "error": "PDF 大小不正確"})
        pdf = self.rfile.read(length)
        if not pdf.startswith(b"%PDF"):
            return self._send(400, {"ok": False, "error": "不是 PDF"})
        name = "".join(ch for ch in (q.get("name") or ["RM"])[0] if ch.isalnum() or ch in "-_")[:40] or "RM"
        fd, path = tempfile.mkstemp(prefix=f"{name}_", suffix=".pdf")
        with os.fdopen(fd, "wb") as fh:
            fh.write(pdf)
        started = time.time()
        try:
            if not cfg["dry_run"]:
                cmd = [cfg["sumatra"], "-print-to", printer, "-print-settings", cfg["settings"], "-silent", "-exit-when-done", path]
                proc = subprocess.run(cmd, capture_output=True, timeout=60)
                if proc.returncode != 0:
                    return self._send(500, {"ok": False, "error": f"SumatraPDF 結束碼 {proc.returncode}"})
            self._send(200, {"ok": True, "job": name, "printer": printer, "elapsed_ms": int((time.time() - started) * 1000)})
        except Exception as exc:
            self._send(500, {"ok": False, "error": type(exc).__name__})
        finally:
            if not cfg["dry_run"]:
                try:
                    os.remove(path)
                except OSError:
                    pass

    def log_message(self, fmt, *args):
        print("[print_helper] " + (fmt % args))


def main():
    if not _cfg()["token"]:
        raise SystemExit("請先設定 PRINT_HELPER_TOKEN")
    port = int(os.environ.get("PRINT_HELPER_PORT", "8766"))
    bind = os.environ.get("PRINT_HELPER_BIND", "0.0.0.0")
    print(f"[print_helper] listening on {bind}:{port}")
    ThreadingHTTPServer((bind, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
