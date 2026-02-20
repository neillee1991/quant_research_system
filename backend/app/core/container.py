"""
依赖注入容器
管理应用程序的依赖关系
"""
from functools import lru_cache
from typing import Optional

from app.core.config import settings
from app.core.logger import logger
from app.services.data_service import DataService
from app.services.factor_service import FactorService
from app.services.backtest_service import BacktestService
from store.postgres_client import db_client
from data_manager.refactored_sync_engine import RefactoredSyncEngine


class Container:
    """依赖注入容器"""

    _instance: Optional['Container'] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            logger.info("Initializing dependency container")

    @lru_cache(maxsize=1)
    def get_data_repository(self):
        """获取数据仓库"""
        return db_client

    @lru_cache(maxsize=1)
    def get_data_service(self) -> DataService:
        """获取数据服务"""
        return DataService(repository=self.get_data_repository())

    @lru_cache(maxsize=1)
    def get_factor_service(self) -> FactorService:
        """获取因子服务"""
        return FactorService()

    @lru_cache(maxsize=1)
    def get_backtest_service(self) -> BacktestService:
        """获取回测服务"""
        return BacktestService()

    @lru_cache(maxsize=1)
    def get_sync_engine(self) -> RefactoredSyncEngine:
        """获取同步引擎"""
        return RefactoredSyncEngine()


# 全局容器实例
container = Container()


# FastAPI 依赖注入函数
def get_data_service() -> DataService:
    """FastAPI 依赖：数据服务"""
    return container.get_data_service()


def get_factor_service() -> FactorService:
    """FastAPI 依赖：因子服务"""
    return container.get_factor_service()


def get_backtest_service() -> BacktestService:
    """FastAPI 依赖：回测服务"""
    return container.get_backtest_service()


def get_sync_engine() -> RefactoredSyncEngine:
    """FastAPI 依赖：同步引擎"""
    return container.get_sync_engine()
