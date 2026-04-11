"""
DolphinDB 数据库客户端（向后兼容层）
此文件保持向后兼容，重新导出新的模块化实现

新的实现位于: infrastructure/database/
- connection.py: 连接管理
- sql_adapter.py: SQL 适配器
- type_converter.py: 类型转换
- table_manager.py: 表管理
- data_operations.py: 数据操作
- metadata_manager.py: 元数据管理
- dolphindb_client.py: 门面模式客户端
"""
from typing import Any, Optional

from app.core.logger import logger

# 重新导出新的实现
from infrastructure.database.dolphindb_client import (
    DolphinDBClient as _NewDolphinDBClient,
    db_client as _new_db_client,
)


class DolphinDBClient(_NewDolphinDBClient):
    """
    DolphinDB 数据库客户端（向后兼容）

    继承新的模块化实现，种子数据方法委托给 infrastructure.seed
    """

    def __init__(self):
        """初始化客户端"""
        super().__init__()

    def seed_sync_task_config(self) -> None:
        """已废弃：seed 数据现由 app/main.py lifespan 通过 SeedDataManager 处理"""
        logger.warning("seed_sync_task_config is deprecated; seed is handled by app lifespan")

    def seed_etl_task_config(self) -> None:
        """已废弃：seed 数据现由 app/main.py lifespan 通过 SeedDataManager 处理"""
        logger.warning("seed_etl_task_config is deprecated; seed is handled by app lifespan")

    def seed_factor_data_config(self) -> None:
        """已废弃：seed 数据现由 app/main.py lifespan 通过 SeedDataManager 处理"""
        logger.warning("seed_factor_data_config is deprecated; seed is handled by app lifespan")

    def seed_factor_metadata(self) -> None:
        """已废弃：seed 数据现由 app/main.py lifespan 通过 SeedDataManager 处理"""
        logger.warning("seed_factor_metadata is deprecated; seed is handled by app lifespan")

    def seed_user_sync_preference(self) -> None:
        """已废弃：seed 数据现由 app/main.py lifespan 通过 SeedDataManager 处理"""
        logger.warning("seed_user_sync_preference is deprecated; seed is handled by app lifespan")

    def seed_flow_config(self) -> None:
        """已废弃：seed 数据现由 app/main.py lifespan 通过 SeedDataManager 处理"""
        logger.warning("seed_flow_config is deprecated; seed is handled by app lifespan")


# 单例实例（延迟初始化）
_db_client_instance: Optional["DolphinDBClient"] = None


def _get_db_client() -> "DolphinDBClient":
    """获取单例客户端实例"""
    global _db_client_instance
    if _db_client_instance is None:
        _db_client_instance = DolphinDBClient()
    return _db_client_instance


class _DBClientProxy:
    """Lazy proxy so existing `db_client.xxx` call sites continue to work."""

    def __getattr__(self, name):
        return getattr(_get_db_client(), name)


db_client = _DBClientProxy()


__all__ = ["DolphinDBClient", "db_client"]
