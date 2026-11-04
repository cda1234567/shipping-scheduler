"""進貨單送印：容器 → Windows 主機上的列印小幫手（tools/print_helper）。預設關閉。

RM_PRINT_HELPER_ENABLED=1 才會真的送；RM_PRINT_HELPER_URL（預設 http://host.docker.internal:8766）、
RM_PRINT_HELPER_TOKEN（與小幫手相同的共用密鑰，必填）。未啟用時前端改開 PDF，並提示要選哪台印表機。
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict

DEFAULT_HELPER_URL = "http://host.docker.internal:8766"


class PrintHelperError(Exception):
    pass


def helper_enabled() -> bool:
    return os.environ.get("RM_PRINT_HELPER_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")


def helper_config() -> Dict[str, Any]:
    """前端顯示用；不含 token。"""
    return {"enabled": helper_enabled() and bool(os.environ.get("RM_PRINT_HELPER_TOKEN", "").strip()),
            "flag": helper_enabled(), "url": os.environ.get("RM_PRINT_HELPER_URL", "").strip() or DEFAULT_HELPER_URL}


def send_pdf(pdf: bytes, printer: str, job_name: str, timeout: float = 20.0) -> Dict[str, Any]:
    if not helper_enabled():
        raise PrintHelperError("尚未啟用自動送印（RM_PRINT_HELPER_ENABLED）")
    token = os.environ.get("RM_PRINT_HELPER_TOKEN", "").strip()
    if not token:
        raise PrintHelperError("尚未設定 RM_PRINT_HELPER_TOKEN")
    base = helper_config()["url"].rstrip("/")
    url = f"{base}/print?" + urllib.parse.urlencode({"printer": printer, "name": job_name})
    req = urllib.request.Request(url, data=pdf, method="POST",
                                 headers={"Content-Type": "application/pdf", "X-Print-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8")).get("error", "")
        except Exception:
            detail = ""
        raise PrintHelperError(f"列印小幫手回應 {exc.code}{('：' + detail) if detail else ''}") from None
    except Exception as exc:
        raise PrintHelperError(f"連不到列印小幫手（{base}）：{type(exc).__name__}") from None
    if not body.get("ok"):
        raise PrintHelperError(f"列印失敗：{body.get('error') or '未知原因'}")
    return {"ok": True, "printer": printer, **{k: v for k, v in body.items() if k in ("job", "elapsed_ms")}}
