from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import timedelta

from fastapi import Request, Response

from .. import database as db
from .local_time import local_fromtimestamp, local_now


EDIT_AUTH_COOKIE = "dispatch_edit_auth"
EDIT_AUTH_REQUIRED_MESSAGE = "目前為唯讀模式，請先登入編輯。"
_DEFAULT_PASSWORD = "123"
_PASSWORD_HASH_KEY = "edit_auth_password_hash"
_SECRET_KEY = "edit_auth_secret"
_SESSION_HOURS = 12
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
_EXEMPT_PATHS = {
    "/api/system/edit-auth/status",
    "/api/system/edit-auth/login",
    "/api/system/edit-auth/logout",
    "/api/health",
}


@dataclass(frozen=True)
class EditAuthSession:
    authenticated: bool
    expires_at: str = ""


def _hash_text(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _ensure_password_hash() -> str:
    saved = str(db.get_setting(_PASSWORD_HASH_KEY) or "").strip()
    if saved:
        return saved
    default_hash = _hash_text(_DEFAULT_PASSWORD)
    db.set_setting(_PASSWORD_HASH_KEY, default_hash)
    return default_hash


def _ensure_secret() -> str:
    saved = str(db.get_setting(_SECRET_KEY) or "").strip()
    if saved:
        return saved
    secret = secrets.token_hex(32)
    db.set_setting(_SECRET_KEY, secret)
    return secret


def verify_edit_password(password: str) -> bool:
    attempted = _hash_text(str(password or ""))
    return hmac.compare_digest(attempted, _ensure_password_hash())


def _sign_payload(payload: str) -> str:
    secret = _ensure_secret().encode("utf-8")
    return hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()


def create_edit_session_cookie_value() -> tuple[str, str, int]:
    expires_at = local_now() + timedelta(hours=_SESSION_HOURS)
    expires_ts = str(int(expires_at.timestamp()))
    signature = _sign_payload(expires_ts)
    return f"{expires_ts}.{signature}", expires_at.isoformat(timespec="seconds"), _SESSION_HOURS * 3600


def parse_edit_session(cookie_value: str) -> EditAuthSession:
    raw = str(cookie_value or "").strip()
    if not raw or "." not in raw:
        return EditAuthSession(authenticated=False)
    expires_ts, provided_signature = raw.split(".", 1)
    if not expires_ts.isdigit():
        return EditAuthSession(authenticated=False)
    expected_signature = _sign_payload(expires_ts)
    if not hmac.compare_digest(provided_signature, expected_signature):
        return EditAuthSession(authenticated=False)
    expires_dt = local_fromtimestamp(float(expires_ts))
    if expires_dt <= local_now():
        return EditAuthSession(authenticated=False)
    return EditAuthSession(authenticated=True, expires_at=expires_dt.isoformat(timespec="seconds"))


def get_edit_auth_status(request: Request) -> EditAuthSession:
    return parse_edit_session(request.cookies.get(EDIT_AUTH_COOKIE, ""))


def apply_edit_auth_cookie(response: Response, request: Request) -> str:
    cookie_value, expires_at, max_age = create_edit_session_cookie_value()
    response.set_cookie(
        EDIT_AUTH_COOKIE,
        cookie_value,
        max_age=max_age,
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
        path="/",
    )
    return expires_at


def clear_edit_auth_cookie(response: Response) -> None:
    response.delete_cookie(EDIT_AUTH_COOKIE, path="/")


def request_requires_edit_auth(request: Request) -> bool:
    if request.method.upper() in _SAFE_METHODS:
        return False
    path = request.url.path.rstrip("/") or "/"
    if not path.startswith("/api"):
        return False
    if path in _EXEMPT_PATHS:
        return False
    return True
