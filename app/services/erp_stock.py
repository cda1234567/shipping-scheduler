"""正航 ERP（SQL Server）ST 倉庫存讀取。

只做唯讀 SELECT，連線設定一律來自環境變數，不寫死帳密。
注意：此模組僅供「ST 庫存」頁籤顯示；缺料 / 發料 / 排程計算仍使用
原本的 Excel 快照（st_inventory_snapshot），不受此處影響。
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

CACHE_TTL_SECONDS = 60.0

_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {"key": None, "data": None, "expires": 0.0}


class ErpError(Exception):
    """ERP 相關錯誤基底類別（訊息為繁體中文，且不含密碼）。"""


class ErpNotConfigured(ErpError):
    """尚未設定 ERP 連線環境變數。"""


class ErpUnavailable(ErpError):
    """ERP 無法連線或查詢失敗。"""


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or "").strip() or default


def _int_env(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError:
        return default


def get_settings() -> Dict[str, Any]:
    """讀取連線設定（含密碼，僅供內部連線使用，不可回傳給前端）。"""
    return {
        "server": _env("ERP_SQL_SERVER", "10.0.0.1"),
        "port": _int_env("ERP_SQL_PORT", 1433),
        "database": _env("ERP_SQL_DATABASE", "CHIComp03"),
        "user": _env("ERP_SQL_USER"),
        "password": os.environ.get("ERP_SQL_PASSWORD") or "",
        "ware_id": _env("ERP_ST_WARE_ID", "001"),
        "timeout": max(1.0, _float_env("ERP_SQL_TIMEOUT", 5.0)),
        "encryption": _env("ERP_SQL_ENCRYPTION", "off").lower(),
        "tds_version": _env("ERP_SQL_TDS_VERSION", "7.3"),
    }


def public_settings() -> Dict[str, Any]:
    s = get_settings()
    return {
        "configured": is_configured(s),
        "server": s["server"],
        "database": s["database"],
        "ware_id": s["ware_id"],
    }


def is_configured(settings: Optional[Dict[str, Any]] = None) -> bool:
    s = settings or get_settings()
    return bool(s["user"] and s["password"])


def _connect(settings: Dict[str, Any]):
    if not is_configured(settings):
        raise ErpNotConfigured("尚未設定 ERP 連線（請設定 ERP_SQL_USER 與 ERP_SQL_PASSWORD 環境變數）")
    try:
        import pymssql  # 延遲載入，未安裝時 app 仍可啟動
    except ImportError as exc:
        raise ErpUnavailable("缺少 pymssql 套件，無法連線 ERP") from exc
    timeout = int(settings["timeout"])
    try:
        return pymssql.connect(
            server=settings["server"],
            port=settings["port"],
            user=settings["user"],
            password=settings["password"],
            database=settings["database"],
            login_timeout=timeout,
            timeout=timeout,
            as_dict=False,
            encryption=settings["encryption"],
            tds_version=settings["tds_version"],
        )
    except Exception as exc:
        # 只輸出例外類型，避免連線字串 / 密碼外洩
        raise ErpUnavailable(
            f"無法連線 ERP（{settings['server']}:{settings['port']}）：{type(exc).__name__}"
        ) from None


def check_connection() -> None:
    """測試連線，失敗時丟出 ErpError。"""
    conn = _connect(get_settings())
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{type(exc).__name__}") from None
    finally:
        conn.close()


_ST_STOCK_SQL = """
SELECT w.ProdID, p.ProdName, w.Quantity
FROM comWareAmount AS w
LEFT JOIN comProduct AS p ON p.ProdID = w.ProdID
WHERE w.WareID = %s
ORDER BY w.ProdID
"""

_WARE_NAME_SQL = "SELECT WareHouseName FROM comWareHouse WHERE WareHouseID = %s"


def _query_st_stock(settings: Dict[str, Any]) -> Dict[str, Any]:
    conn = _connect(settings)
    try:
        cur = conn.cursor()
        cur.execute(_ST_STOCK_SQL, (settings["ware_id"],))
        rows = [
            {
                "part_number": str(prod_id or "").strip(),
                "name": str(prod_name or "").strip(),
                "quantity": float(qty or 0),
            }
            for prod_id, prod_name, qty in cur.fetchall()
            if str(prod_id or "").strip()
        ]
        ware_name = ""
        try:
            cur.execute(_WARE_NAME_SQL, (settings["ware_id"],))
            found = cur.fetchone()
            ware_name = str(found[0] or "").strip() if found else ""
        except Exception:
            pass  # 倉庫名稱僅為顯示用，取不到不影響庫存
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{type(exc).__name__}") from None
    finally:
        conn.close()
    return {
        "source": "erp",
        "ware_id": settings["ware_id"],
        "ware_name": ware_name,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": rows,
    }


def get_st_stock(force_refresh: bool = False) -> Dict[str, Any]:
    """取得 ERP ST 倉庫存（約 60 秒快取；force_refresh=True 強制重查）。"""
    settings = get_settings()
    if not is_configured(settings):
        raise ErpNotConfigured("尚未設定 ERP 連線（請設定 ERP_SQL_USER 與 ERP_SQL_PASSWORD 環境變數）")
    key = (settings["server"], settings["port"], settings["database"], settings["ware_id"])
    with _cache_lock:
        if (
            not force_refresh
            and _cache["key"] == key
            and _cache["data"] is not None
            and time.monotonic() < _cache["expires"]
        ):
            return _cache["data"]
    data = _query_st_stock(settings)
    with _cache_lock:
        _cache.update(key=key, data=data, expires=time.monotonic() + CACHE_TTL_SECONDS)
    return data


def clear_cache() -> None:
    with _cache_lock:
        _cache.update(key=None, data=None, expires=0.0)
