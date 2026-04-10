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
        """种子数据：同步任务配置"""
        try:
            from infrastructure.seed import SeedDataLoader, SeedDataManager

            loader = SeedDataLoader()
            manager = SeedDataManager(db_client=self, loader=loader)
            manager.seed_sync_task_config()
        except Exception as e:
            logger.error(f"seed_sync_task_config failed: {e}")

    def seed_etl_task_config(self) -> None:
        """种子数据：ETL 任务配置"""
        try:
            from infrastructure.seed import SeedDataLoader, SeedDataManager

            loader = SeedDataLoader()
            manager = SeedDataManager(db_client=self, loader=loader)
            manager.seed_etl_task_config()
        except Exception as e:
            logger.error(f"seed_etl_task_config failed: {e}")

    def seed_factor_data_config(self) -> None:
        """种子数据：因子数据配置"""
        try:
            from infrastructure.seed import SeedDataLoader, SeedDataManager

            loader = SeedDataLoader()
            manager = SeedDataManager(db_client=self, loader=loader)
            manager.seed_factor_data_config()
        except Exception as e:
            logger.error(f"seed_factor_data_config failed: {e}")

    def seed_factor_metadata(self) -> None:
        """种子数据：因子元数据"""
        try:
            from infrastructure.seed import SeedDataLoader, SeedDataManager

            loader = SeedDataLoader()
            manager = SeedDataManager(db_client=self, loader=loader)
            manager.seed_factor_metadata()
        except Exception as e:
            logger.error(f"seed_factor_metadata failed: {e}")

    def seed_user_sync_preference(self) -> None:
        """种子数据：用户同步偏好配置"""
        try:
            from infrastructure.seed import SeedDataLoader, SeedDataManager

            loader = SeedDataLoader()
            manager = SeedDataManager(db_client=self, loader=loader)
            manager.seed_user_sync_preference()
        except Exception as e:
            logger.error(f"seed_user_sync_preference failed: {e}")

    def seed_flow_config(self) -> None:
        """种子数据：flow 配置"""
        try:
            from infrastructure.seed import SeedDataLoader, SeedDataManager

            loader = SeedDataLoader()
            manager = SeedDataManager(db_client=self, loader=loader)
            manager.seed_flow_config()
        except Exception as e:
            logger.error(f"seed_flow_config failed: {e}")


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
