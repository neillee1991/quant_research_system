"""
核心接口定义
定义系统中各个组件的抽象接口，支持依赖注入和测试
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
import polars as pl


class IDataCollector(ABC):
    """数据采集器接口"""

    @abstractmethod
    def collect_daily(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pl.DataFrame:
        """采集日线数据"""
        pass

    @abstractmethod
    def collect_stock_basic(self) -> pl.DataFrame:
        """采集股票基础信息"""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """检查数据源是否可用"""
        pass


class IDataRepository(ABC):
    """数据仓库接口"""

    @abstractmethod
    def query(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """查询数据"""
        pass

    @abstractmethod
    def upsert(
        self,
        table: str,
        data: pl.DataFrame,
        primary_keys: List[str]
    ) -> None:
        """插入或更新数据"""
        pass

    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """检查表是否存在"""
        pass

    @abstractmethod
    def create_table(
        self,
        table: str,
        schema: Dict[str, Dict[str, Any]],
        primary_keys: List[str]
    ) -> None:
        """创建表"""
        pass

    @abstractmethod
    def get_table_columns(self, table: str) -> list:
        """获取表的列名列表"""
        pass


class IFactorEngine(ABC):
    """因子计算引擎接口"""

    @abstractmethod
    def compute_technical_factors(
        self,
        data: pl.DataFrame,
        factors: List[str]
    ) -> pl.DataFrame:
        """计算技术指标因子"""
        pass

    @abstractmethod
    def compute_cross_sectional_factors(
        self,
        data: pl.DataFrame,
        factors: List[str]
    ) -> pl.DataFrame:
        """计算截面因子"""
        pass


class IBacktestEngine(ABC):
    """回测引擎接口"""

    @abstractmethod
    def run_backtest(
        self,
        data: pl.DataFrame,
        signals: pl.DataFrame,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """执行回测"""
        pass


class ISyncTaskExecutor(ABC):
    """同步任务执行器接口"""

    @abstractmethod
    def execute_task(
        self,
        task_config: Dict[str, Any],
        target_date: Optional[str] = None
    ) -> bool:
        """执行同步任务"""
        pass


class IRateLimiter(ABC):
    """速率限制器接口"""

    @abstractmethod
    def wait(self) -> None:
        """等待以满足速率限制"""
        pass

    @abstractmethod
    def reset(self) -> None:
        """重置速率限制器"""
        pass


class IRetryPolicy(ABC):
    """重试策略接口"""

    @abstractmethod
    def execute(self, func: callable, *args, **kwargs) -> Any:
        """执行带重试的函数调用"""
        pass
