from __future__ import annotations

from functools import wraps
import threading
from typing import Callable, TypeVar


_MAIN_FILE_WRITE_LOCK = threading.RLock()
_T = TypeVar("_T")


def serialized_main_file_write(func: Callable[..., _T]) -> Callable[..., _T]:
    """讓同一程序內所有主檔寫入依序執行，避免 Excel 寫到一半被另一請求讀寫。"""

    @wraps(func)
    def wrapped(*args, **kwargs):
        with _MAIN_FILE_WRITE_LOCK:
            return func(*args, **kwargs)

    return wrapped
