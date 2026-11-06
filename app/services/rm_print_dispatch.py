"""進貨單送印：容器 → Windows 主機上的列印小幫手（tools/print_helper）。預設關閉。

RM_PRINT_HELPER_ENABLED=1 才會真的送；RM_PRINT_HELPER_URL（預設 http://host.docker.internal:8766）、
RM_PRINT_HELPER_TOKEN（與小幫手相同的共用密鑰，必填）。未啟用時前端改開 PDF，並提示要選哪台印表機。
"""
from __future__ import annotations

import json
import os
import time
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
            "restart_url": os.environ.get("RM_PRINT_HELPER_RESTART_URL", "").strip() or "http://host.docker.internal:8767",
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
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        raise PrintHelperError(f"連不到列印小幫手（{base}）：{type(exc).__name__}") from None
    except Exception:
        raise PrintHelperError("列印小幫手回傳格式不正確") from None
    if not isinstance(body, dict):
        raise PrintHelperError("列印小幫手回傳格式不正確")
    if not body.get("ok"):
        raise PrintHelperError(f"列印失敗：{body.get('error') or '未知原因'}")
    return {"ok": True, "printer": printer, **{k: v for k, v in body.items() if k in ("job", "elapsed_ms")}}


def restart_helper(timeout: float = 15) -> Dict[str, Any]:
    """要求主機重啟小幫手，不顯示密鑰。"""
    token = os.environ.get("RM_PRINT_HELPER_TOKEN", "").strip()
    if not token:
        raise PrintHelperError("尚未設定 RM_PRINT_HELPER_TOKEN")
    url = helper_config()["restart_url"].rstrip("/") + "/restart"
    req = urllib.request.Request(url, data=b"", method="POST",
                                 headers={"X-Print-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        raise PrintHelperError(f"列印重啟服務回應 {exc.code}，無法重啟小幫手") from None
    except (urllib.error.URLError, ConnectionError, TimeoutError):
        raise PrintHelperError("連不到列印重啟服務，請確認主機重啟服務已啟動") from None
    except Exception:
        raise PrintHelperError("列印重啟服務回傳格式不正確") from None
    if not isinstance(body, dict) or not body.get("ok"):
        raise PrintHelperError("列印重啟服務未成功重啟小幫手")
    return {"ok": True}


def helper_reachable(timeout: float = 1.0) -> bool:
    """短暫探測小幫手是否恢復服務。"""
    req = urllib.request.Request(helper_config()["url"].rstrip("/") + "/health", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8") or "{}")
        return isinstance(body, dict) and bool(body.get("ok"))
    except Exception:
        return False


def send_pdf_with_auto_restart(pdf: bytes, printer: str, job_name: str,
                               timeout: float = 20.0, health_timeout: float = 15.0) -> Dict[str, Any]:
    """僅連線失敗時重啟，恢復健康後最多再送印一次。"""
    try:
        return send_pdf(pdf, printer, job_name, timeout=timeout)
    except PrintHelperError as exc:
        message = str(exc).lower()
        # HTTP 與列印工作錯誤不可因內容提到連線而觸發重啟。
        if message.startswith(("列印小幫手回應", "列印失敗")) or not any(
                marker in message for marker in ("連不到", "urlerror", "connection", "timeout", "timed out")):
            raise
    try:
        restart_helper()
    except PrintHelperError as exc:
        raise PrintHelperError(f"列印連線失敗，自動重啟失敗：{exc}") from None
    deadline = time.monotonic() + health_timeout
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if helper_reachable(timeout=min(1.0, max(0.001, remaining))):
            break
        time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))
    else:
        raise PrintHelperError("已要求重啟，但仍連不到列印小幫手（等待約 15 秒），請確認主機服務")
    try:
        return send_pdf(pdf, printer, job_name, timeout=timeout)
    except PrintHelperError as exc:
        raise PrintHelperError(f"小幫手重啟後再次列印仍失敗：{exc}") from None
