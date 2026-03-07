"""
TTL 内存缓存模块

为高频读、低频写的 API 提供缓存层，减少 DolphinDB 查询压力。
写操作通过 invalidate() 清除对应前缀的缓存。
"""
import time
import threading
from typing import Any, Optional


class TTLCache:
    """线程安全的 TTL 内存缓存"""

    def __init__(self):
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expire_at = entry
            if time.time() < expire_at:
                return value
            del self._store[key]
            return None

    def set(self, key: str, value: Any, ttl: int = 60):
        with self._lock:
            self._store[key] = (value, time.time() + ttl)

    def invalidate(self, prefix: str = ""):
        """清除匹配前缀的缓存，空前缀清除全部"""
        with self._lock:
            if not prefix:
                self._store.clear()
            else:
                keys = [k for k in self._store if k.startswith(prefix)]
                for k in keys:
                    del self._store[k]


# 全局单例
api_cache = TTLCache()
