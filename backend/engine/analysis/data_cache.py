"""
分析数据缓存
TTL 30min + LRU 500MB + 手动释放
用于诊断警告后用户调整参数重跑时复用原始数据，避免重复查询 DB。
"""
import time
import sys
import threading
from typing import Any, Dict, Optional
from collections import OrderedDict

from app.core.logger import logger

CACHE_TTL_SECONDS = 30 * 60
CACHE_MAX_BYTES = 500 * 1024 * 1024  # 500 MB


def _estimate_size(obj: Any) -> int:
    """粗略估算对象内存占用（bytes）"""
    try:
        import polars as pl
        import pandas as pd
        if isinstance(obj, pl.DataFrame):
            return obj.estimated_size()
        if isinstance(obj, pd.DataFrame):
            return int(obj.memory_usage(deep=True).sum())
    except Exception:
        pass
    return sys.getsizeof(obj)


class AnalysisDataCache:
    """
    分析数据缓存单例。

    缓存 key: factor_id + start_date + end_date + index_pool（拼接字符串）
    缓存 value: {"factor_df": ..., "price_df": ..., "cached_at": timestamp}

    淘汰策略：
    1. TTL 过期（30min）
    2. LRU（总大小超过 500MB 时淘汰最久未访问的条目）
    3. 手动释放
    """

    _instance: Optional["AnalysisDataCache"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "AnalysisDataCache":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
                cls._instance._sizes: Dict[str, int] = {}
                cls._instance._total_bytes: int = 0
        return cls._instance

    @classmethod
    def get_instance(cls) -> "AnalysisDataCache":
        return cls()

    @staticmethod
    def make_key(
        factor_id: str,
        start_date: Optional[str],
        end_date: Optional[str],
        index_pool: Optional[str] = None,
    ) -> str:
        return f"{factor_id}|{start_date or ''}|{end_date or ''}|{index_pool or ''}"

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if key not in self._cache:
                return None
            entry = self._cache[key]
            if time.time() - entry["cached_at"] > CACHE_TTL_SECONDS:
                self._evict(key)
                logger.info(f"Cache TTL expired: {key}")
                return None
            self._cache.move_to_end(key)
            logger.info(f"Cache hit: {key}")
            return entry

    def set(self, key: str, data: Dict[str, Any]) -> None:
        with self._lock:
            size = sum(
                _estimate_size(v)
                for v in data.values()
                if not isinstance(v, (str, float, int))
            )
            if key in self._cache:
                self._evict(key)
            while self._total_bytes + size > CACHE_MAX_BYTES and self._cache:
                oldest_key = next(iter(self._cache))
                logger.info(f"Cache LRU evict: {oldest_key}")
                self._evict(oldest_key)
            self._cache[key] = {**data, "cached_at": time.time()}
            self._sizes[key] = size
            self._total_bytes += size
            logger.info(
                f"Cache set: {key}, size={size / 1024 / 1024:.1f}MB, "
                f"total={self._total_bytes / 1024 / 1024:.1f}MB"
            )

    def release(self, key: str) -> bool:
        """手动释放指定 key 的缓存"""
        with self._lock:
            if key in self._cache:
                self._evict(key)
                logger.info(f"Cache manually released: {key}")
                return True
            return False

    def release_all(self) -> int:
        """释放所有缓存，返回释放的条目数"""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._sizes.clear()
            self._total_bytes = 0
            logger.info(f"Cache cleared: {count} entries released")
            return count

    def stats(self) -> Dict[str, Any]:
        """返回缓存统计信息"""
        with self._lock:
            return {
                "entries": len(self._cache),
                "total_mb": round(self._total_bytes / 1024 / 1024, 1),
                "keys": list(self._cache.keys()),
            }

    def _evict(self, key: str) -> None:
        """内部：移除指定 key（不加锁，调用方负责加锁）"""
        if key in self._cache:
            del self._cache[key]
        if key in self._sizes:
            self._total_bytes -= self._sizes.pop(key)
