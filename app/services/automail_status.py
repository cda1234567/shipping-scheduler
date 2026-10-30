"""讀取 automail（Andy 的寄信網站）的寄信紀錄與廠商回信，讓採購單顯示「已寄 / 未寄 / 廠商已回」。

本模組只對 automail 發出 GET /api/sendlog 與 GET /api/replies，唯讀；本站絕不寄信（不使用 SMTP）。
設定（環境變數）：
- AUTOMAIL_BASE_URL：預設 https://automail.cda.tw；空字串或 "off" = 停用此功能
- AUTOMAIL_REFRESH_SECONDS：快取秒數，預設 180，最小 30
- AUTOMAIL_TIMEOUT_SECONDS：單次請求逾時，預設 8
- AUTOMAIL_API_KEY：automail 機器 API 金鑰，以 X-Api-Key 標頭送出（放 env_file，不可提交到 git；不寫入 log / 回應）
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

DEFAULT_BASE_URL = "https://automail.cda.tw"
SENDLOG_PATH = "/api/sendlog"
REPLIES_PATH = "/api/replies"

_lock = threading.Lock()  # 保護 _state
_fetch_lock = threading.Lock()  # 同一時間只允許一個抓取
_state: Dict[str, Any] = {
    "fetched_ts": 0.0, "fetched_at": "", "ok": False, "error": "", "last_error_at": "",
    "sendlog": None, "replies": None, "replies_enabled": False, "replies_error": "",
}

try:
    from zoneinfo import ZoneInfo
    _TAIPEI = ZoneInfo("Asia/Taipei")
except Exception:  # tzdata 缺少時退回固定 +08:00
    _TAIPEI = timezone(timedelta(hours=8))


def base_url() -> str:
    raw = os.environ.get("AUTOMAIL_BASE_URL")
    value = DEFAULT_BASE_URL if raw is None else raw.strip()
    if not value or value.lower() == "off":
        return ""
    return value.rstrip("/")


def _int_env(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, "") or default))
    except ValueError:
        return default


def refresh_seconds() -> int:
    return _int_env("AUTOMAIL_REFRESH_SECONDS", 180, 30)


def timeout_seconds() -> int:
    return _int_env("AUTOMAIL_TIMEOUT_SECONDS", 8)


def normalize_po(value: Any) -> str:
    return "".join(str(value or "").split()).upper()


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def local_text(value: Any) -> str:
    """ISO（UTC）→ 台北時間 "MM/DD HH:MM"；無法解析回空字串。"""
    dt = _parse_iso(value)
    return dt.astimezone(_TAIPEI).strftime("%m/%d %H:%M") if dt else ""


def _headers() -> Dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "shipping-scheduler-automail-status/1.0"}
    key = (os.environ.get("AUTOMAIL_API_KEY") or "").strip()
    if key:
        headers["X-Api-Key"] = key
    return headers


def _get_json(path: str) -> Any:
    request = urllib.request.Request(base_url() + path, method="GET", headers=_headers())
    with urllib.request.urlopen(request, timeout=timeout_seconds()) as response:  # noqa: S310 - 設定的 automail 位址
        return json.loads(response.read().decode("utf-8"))


def _safe_error(exc: Exception) -> str:
    # 只留例外類別與狀態碼，不含網址或回應內容
    code = getattr(exc, "code", None)
    return f"{type(exc).__name__}{f' {code}' if code else ''}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch() -> None:
    sendlog, error = None, ""
    try:
        data = _get_json(SENDLOG_PATH)
        if not isinstance(data, list):
            raise ValueError("unexpected sendlog format")
        sendlog = data
    except Exception as exc:  # noqa: BLE001 - 絕不丟給呼叫端
        error = _safe_error(exc)
    replies, replies_enabled, replies_error = None, False, ""
    try:
        data = _get_json(REPLIES_PATH)
        if isinstance(data, dict) and data.get("enabled") and isinstance(data.get("replies"), list):
            replies, replies_enabled = data["replies"], True
            replies_error = str(data.get("error") or "")[:200]
        elif isinstance(data, dict) and data.get("enabled") is False:
            replies_error = ""  # 對方未啟用回信功能：不算錯誤
    except Exception as exc:  # noqa: BLE001 - 舊版 404 等
        replies_error = _safe_error(exc)
    with _lock:
        _state["fetched_ts"] = time.time()
        if sendlog is not None:
            _state.update(sendlog=sendlog, ok=True, error="", fetched_at=_now_iso())
        else:
            _state.update(ok=False, error=error, last_error_at=_now_iso())
        # 回信獨立：成功或明確停用才覆蓋；失敗時保留上次資料
        if replies_enabled or not replies_error:
            _state.update(replies=replies, replies_enabled=replies_enabled, replies_error="")
        else:
            _state["replies_error"] = replies_error
            if _state["replies"] is None:
                _state["replies_enabled"] = False


def _aggregate() -> Dict[str, Dict[str, Any]]:
    pos: Dict[str, Dict[str, Any]] = {}
    for item in _state["sendlog"] or []:
        if not isinstance(item, dict):
            continue
        key = normalize_po(item.get("orderNumber"))
        dt = _parse_iso(item.get("sentAt"))
        if not key or dt is None:
            continue
        rec = pos.setdefault(key, {"sent": True, "sent_count": 0, "_first": None, "_last": None, "vendor": "", "to": [],
                                   "replied_count": 0, "_reply": None})
        rec["sent_count"] += 1
        if rec["_first"] is None or dt < rec["_first"][0]:
            rec["_first"] = (dt, item.get("sentAt"))
        if rec["_last"] is None or dt >= rec["_last"][0]:
            rec["_last"] = (dt, item.get("sentAt"))
            rec["vendor"] = str(item.get("vendor") or "")
            rec["to"] = [str(x) for x in (item.get("to") or []) if x]
    for item in _state["replies"] or []:
        if not isinstance(item, dict):
            continue
        key = normalize_po(item.get("orderNumber"))
        if not key:
            continue
        dt = _parse_iso(item.get("receivedAt")) or datetime.min.replace(tzinfo=timezone.utc)
        rec = pos.setdefault(key, _empty(False))
        rec["replied_count"] += 1
        if rec["_reply"] is None or dt >= rec["_reply"][0]:
            rec["_reply"] = (dt, item)
    return {key: _finish(rec) for key, rec in pos.items()}


def _empty(sent: bool) -> Dict[str, Any]:
    return {"sent": sent, "sent_count": 0, "_first": None, "_last": None, "vendor": "", "to": [],
            "replied_count": 0, "_reply": None}


def _finish(rec: Dict[str, Any]) -> Dict[str, Any]:
    first, last, reply = rec.pop("_first"), rec.pop("_last"), rec.pop("_reply")
    rec["first_sent_at"] = first[1] if first else ""
    rec["last_sent_at"] = last[1] if last else ""
    rec["last_sent_local"] = local_text(last[1]) if last else ""
    item = reply[1] if reply else {}
    rec["last_reply_at"] = str(item.get("receivedAt") or "") if reply else ""
    rec["last_reply_local"] = local_text(item.get("receivedAt")) if reply else ""
    rec["last_reply_from"] = str(item.get("fromName") or item.get("from") or "") if reply else ""
    rec["last_reply_subject"] = str(item.get("subject") or "") if reply else ""
    return rec


def _fresh() -> bool:
    return time.time() - _state["fetched_ts"] < refresh_seconds()


def get_status(force: bool = False, pos: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    if not base_url():
        return {"enabled": False, "ok": False, "fetched_at": "", "error": "", "replies_enabled": False,
                "replies_error": "", "pos": {}}
    if force or not _fresh():
        if _fetch_lock.acquire(blocking=False):
            try:
                if force or not _fresh():
                    _fetch()
            finally:
                _fetch_lock.release()
        # 取不到鎖 = 別人正在抓，直接用舊快取
    with _lock:
        result = {
            "enabled": True, "ok": bool(_state["ok"]), "fetched_at": _state["fetched_at"],
            "error": _state["error"], "last_error_at": _state["last_error_at"],
            "replies_enabled": bool(_state["replies_enabled"]), "replies_error": _state["replies_error"],
            "pos": _aggregate(),
        }
    if pos is not None:
        wanted: List[str] = []
        for p in pos:
            key = normalize_po(p)
            if key and key not in wanted:
                wanted.append(key)
        result["pos"] = {k: result["pos"].get(k) or _finish(_empty(False)) for k in wanted}
    return result


def reset_cache() -> None:
    """測試用：清空快取。"""
    with _lock:
        _state.update(fetched_ts=0.0, fetched_at="", ok=False, error="", last_error_at="", sendlog=None,
                      replies=None, replies_enabled=False, replies_error="")
