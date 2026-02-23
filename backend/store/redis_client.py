"""
Redis 缓存客户端
提供统一的缓存接口，支持 Polars DataFrame 序列化
"""
import json
import pickle
from typing import Optional, Any
import redis
from redis.connection import ConnectionPool
import polars as pl

from app.core.config import settings
from app.core.logger import logger


class RedisClient:
    """Redis 缓存客户端"""

    def __init__(self):
        self.config = {
            'host': settings.redis.redis_host,
            'port': settings.redis.redis_port,
            'db': settings.redis.redis_db,
            'password': settings.redis.redis_password if settings.redis.redis_password else None,
            'decode_responses': False,  # 使用二进制模式支持 pickle
            'max_connections': settings.redis.redis_max_connections,
        }

        # 创建连接池
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._init_client()

        logger.info(f"Redis client initialized: {self.config['host']}:{self.config['port']}/{self.config['db']}")

    def _init_client(self):
        """初始化 Redis 客户端"""
        try:
            self._pool = ConnectionPool(**self.config)
            self._client = redis.Redis(connection_pool=self._pool)

            # 测试连接
            self._client.ping()
            logger.info("Redis connection successful")
        except redis.ConnectionError as e:
            logger.warning(f"Redis connection failed: {e}. Cache will be disabled.")
            self._client = None
        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {e}")
            self._client = None

    def is_available(self) -> bool:
        """检查 Redis 是否可用"""
        if self._client is None:
            return False
        try:
            self._client.ping()
            return True
        except:
            return False

    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存值

        Args:
            key: 缓存键

        Returns:
            缓存值，如果不存在或出错返回 None
        """
        if not self.is_available():
            return None

        try:
            value = self._client.get(key)
            if value is None:
                return None

            # 尝试反序列化
            return pickle.loads(value)
        except Exception as e:
            logger.warning(f"Failed to get cache key '{key}': {e}")
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        设置缓存值

        Args:
            key: 缓存键
            value: 缓存值（支持任意可序列化对象）
            ttl: 过期时间（秒），None 表示永不过期

        Returns:
            是否设置成功
        """
        if not self.is_available():
            return False

        try:
            # 序列化值
            serialized = pickle.dumps(value)

            if ttl:
                self._client.setex(key, ttl, serialized)
            else:
                self._client.set(key, serialized)

            return True
        except Exception as e:
            logger.warning(f"Failed to set cache key '{key}': {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        删除缓存

        Args:
            key: 缓存键

        Returns:
            是否删除成功
        """
        if not self.is_available():
            return False

        try:
            self._client.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Failed to delete cache key '{key}': {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """
        删除匹配模式的所有键

        Args:
            pattern: 键模式（支持通配符 *）

        Returns:
            删除的键数量
        """
        if not self.is_available():
            return 0

        try:
            keys = self._client.keys(pattern)
            if keys:
                return self._client.delete(*keys)
            return 0
        except Exception as e:
            logger.warning(f"Failed to delete keys matching '{pattern}': {e}")
            return 0

    def exists(self, key: str) -> bool:
        """
        检查键是否存在

        Args:
            key: 缓存键

        Returns:
            是否存在
        """
        if not self.is_available():
            return False

        try:
            return self._client.exists(key) > 0
        except Exception as e:
            logger.warning(f"Failed to check key existence '{key}': {e}")
            return False

    def get_ttl(self, key: str) -> Optional[int]:
        """
        获取键的剩余过期时间

        Args:
            key: 缓存键

        Returns:
            剩余秒数，-1 表示永不过期，-2 表示不存在
        """
        if not self.is_available():
            return None

        try:
            return self._client.ttl(key)
        except Exception as e:
            logger.warning(f"Failed to get TTL for key '{key}': {e}")
            return None

    def cache_dataframe(self, key: str, df: pl.DataFrame, ttl: Optional[int] = None) -> bool:
        """
        缓存 Polars DataFrame

        Args:
            key: 缓存键
            df: Polars DataFrame
            ttl: 过期时间（秒）

        Returns:
            是否缓存成功
        """
        return self.set(key, df, ttl)

    def get_dataframe(self, key: str) -> Optional[pl.DataFrame]:
        """
        获取缓存的 Polars DataFrame

        Args:
            key: 缓存键

        Returns:
            Polars DataFrame 或 None
        """
        value = self.get(key)
        if value is not None and isinstance(value, pl.DataFrame):
            return value
        return None

    def get_stats(self) -> dict:
        """
        获取 Redis 统计信息

        Returns:
            统计信息字典
        """
        if not self.is_available():
            return {"available": False}

        try:
            info = self._client.info()
            return {
                "available": True,
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": self._calculate_hit_rate(info),
            }
        except Exception as e:
            logger.warning(f"Failed to get Redis stats: {e}")
            return {"available": False, "error": str(e)}

    def _calculate_hit_rate(self, info: dict) -> str:
        """计算缓存命中率"""
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses
        if total == 0:
            return "N/A"
        return f"{(hits / total * 100):.2f}%"

    def close(self):
        """关闭连接"""
        if self._pool:
            self._pool.disconnect()
            logger.info("Redis connection pool closed")


# 单例实例
redis_client = RedisClient()
