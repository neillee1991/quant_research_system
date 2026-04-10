"""
PostgreSQL 数据库连接池（Async）
使用 asyncpg 进行异步数据库操作
"""
import asyncpg
from typing import Optional
from contextlib import asynccontextmanager

from app.core.logger import logger
from app.core.config import settings


class DatabasePool:
    """异步 PostgreSQL 连接池管理"""

    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def init_pool(cls):
        """初始化连接池"""
        if cls._pool is not None:
            return

        try:
            cls._pool = await asyncpg.create_pool(
                host=settings.postgresql.postgres_host,
                port=settings.postgresql.postgres_port,
                database=settings.postgresql.postgres_db,
                user=settings.postgresql.postgres_user,
                password=settings.postgresql.postgres_password,
                min_size=5,
                max_size=20,
            )
            logger.info("PostgreSQL 连接池初始化成功")
        except Exception as e:
            logger.error(f"PostgreSQL 连接池初始化失败: {e}", exc_info=True)
            raise

    @classmethod
    async def close_pool(cls):
        """关闭连接池"""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None
            logger.info("PostgreSQL 连接池已关闭")

    @classmethod
    @asynccontextmanager
    async def get_connection(cls):
        """获取数据库连接（上下文管理器）"""
        if cls._pool is None:
            await cls.init_pool()

        conn = await cls._pool.acquire()
        try:
            yield conn
        finally:
            await cls._pool.release(conn)

    @classmethod
    async def execute(cls, query: str, *args):
        """执行 SQL 语句（无返回值）"""
        async with cls.get_connection() as conn:
            await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query: str, *args):
        """执行查询，返回多行"""
        async with cls.get_connection() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query: str, *args):
        """执行查询，返回单行"""
        async with cls.get_connection() as conn:
            return await conn.fetchrow(query, *args)

    @classmethod
    async def fetchval(cls, query: str, *args):
        """执行查询，返回单个值"""
        async with cls.get_connection() as conn:
            return await conn.fetchval(query, *args)


# 快捷函数
async def init_db():
    """初始化数据库"""
    await DatabasePool.init_pool()


async def close_db():
    """关闭数据库连接"""
    await DatabasePool.close_pool()
