"""
Repository Pattern - 数据访问抽象层

Repository 模式将数据访问逻辑封装在统一的接口后面，使业务逻辑与数据存储细节解耦。

核心优势：
1. 统一的数据访问接口
2. 易于测试（可用 Mock 替换）
3. 易于切换数据源（数据库、API、文件等）
4. 集中管理数据访问逻辑

使用示例：
    # 创建 repository
    repo = MarketDataRepository(db_client)

    # 查询数据
    df = repo.find_by_date_range("20240101", "20240131")
    df = repo.find_by_codes(["000001.SZ"], "20240101", "20240131")

    # 保存数据
    count = repo.save(df)
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import polars as pl

from app.core.logger import logger
from infrastructure.database.query_builder import QueryBuilder


class IRepository(ABC):
    """Repository 接口定义"""

    @abstractmethod
    def find_by_date_range(
        self,
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        按日期范围查询数据

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            columns: 查询列，None 表示查询所有列

        Returns:
            Polars DataFrame
        """
        pass

    @abstractmethod
    def find_by_codes(
        self,
        ts_codes: List[str],
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        按股票代码和日期范围查询数据

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            columns: 查询列，None 表示查询所有列

        Returns:
            Polars DataFrame
        """
        pass

    @abstractmethod
    def save(self, data: pl.DataFrame) -> int:
        """
        保存数据

        Args:
            data: Polars DataFrame

        Returns:
            保存的行数
        """
        pass

    @abstractmethod
    def delete(self, conditions: Dict[str, Any]) -> int:
        """
        删除数据

        Args:
            conditions: 删除条件字典

        Returns:
            删除的行数
        """
        pass


class BaseRepository(IRepository):
    """Repository 基类实现"""

    def __init__(self, db_client, table_name: str):
        """
        初始化 Repository

        Args:
            db_client: DolphinDB 客户端
            table_name: 表名
        """
        self.db = db_client
        self.table_name = table_name
        logger.debug(f"Initialized {self.__class__.__name__} for table: {table_name}")

    def find_by_date_range(
        self,
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        按日期范围查询数据

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            columns: 查询列，None 表示查询所有列

        Returns:
            Polars DataFrame
        """
        query = QueryBuilder(self.table_name)

        if columns:
            query.select(columns)
        else:
            query.select_all()

        query.where_between("trade_date", start_date, end_date)
        query.order_by(["trade_date", "ts_code"])

        built_query = query.build()
        logger.debug(f"Executing query: {built_query.sql} with params: {built_query.params}")

        result = self.db.execute(built_query.sql, built_query.params)
        return result

    def find_by_codes(
        self,
        ts_codes: List[str],
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        按股票代码和日期范围查询数据

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            columns: 查询列，None 表示查询所有列

        Returns:
            Polars DataFrame
        """
        if not ts_codes:
            return pl.DataFrame()

        query = QueryBuilder(self.table_name)

        if columns:
            query.select(columns)
        else:
            query.select_all()

        query.where_in("ts_code", ts_codes)
        query.where_between("trade_date", start_date, end_date)
        query.order_by(["trade_date", "ts_code"])

        built_query = query.build()
        logger.debug(f"Executing query: {built_query.sql} with params: {built_query.params}")

        result = self.db.execute(built_query.sql, built_query.params)
        return result

    def save(self, data: pl.DataFrame) -> int:
        """
        保存数据（使用 upsert）

        Args:
            data: Polars DataFrame

        Returns:
            保存的行数
        """
        if data.is_empty():
            logger.warning(f"Attempted to save empty DataFrame to {self.table_name}")
            return 0

        count = self.db.upsert(self.table_name, data)
        logger.info(f"Saved {count} rows to {self.table_name}")
        return count

    def delete(self, conditions: Dict[str, Any]) -> int:
        """
        删除数据

        Args:
            conditions: 删除条件字典，例如 {"ts_code": "000001.SZ", "trade_date": "20240101"}

        Returns:
            删除的行数
        """
        if not conditions:
            logger.error("Delete conditions cannot be empty")
            raise ValueError("Delete conditions cannot be empty")

        query = QueryBuilder(self.table_name)

        for column, value in conditions.items():
            if isinstance(value, list):
                query.where_in(column, value)
            else:
                query.where(column, "=", value)

        built_query = query.build()

        # 构建 DELETE 语句
        delete_sql = built_query.sql.replace(f"SELECT * FROM {self.table_name}", f"DELETE FROM {self.table_name}")
        logger.debug(f"Executing delete: {delete_sql} with params: {built_query.params}")

        count = self.db.execute_delete(delete_sql, built_query.params)
        logger.info(f"Deleted {count} rows from {self.table_name}")
        return count

    def count(self, conditions: Optional[Dict[str, Any]] = None) -> int:
        """
        统计行数

        Args:
            conditions: 查询条件字典，None 表示统计所有行

        Returns:
            行数
        """
        query = QueryBuilder(self.table_name).select(["COUNT(*) as cnt"])

        if conditions:
            for column, value in conditions.items():
                if isinstance(value, list):
                    query.where_in(column, value)
                else:
                    query.where(column, "=", value)

        built_query = query.build()
        result = self.db.execute(built_query.sql, built_query.params)

        if result.is_empty():
            return 0

        return result["cnt"][0]

    def exists(self, conditions: Dict[str, Any]) -> bool:
        """
        检查数据是否存在

        Args:
            conditions: 查询条件字典

        Returns:
            是否存在
        """
        return self.count(conditions) > 0
