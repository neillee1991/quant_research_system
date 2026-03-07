"""
DolphinDB 客户端模块
提供统一的数据库访问接口，向后兼容原有的 dolphindb_client.py
"""
from typing import Any, Dict, List, Optional

import polars as pl

from app.core.logger import logger
from .connection import DolphinDBConnection
from .query_builder import QueryBuilder
from .meta_manager import MetadataManager
from .seed_data import SeedDataManager
from .data_operations import DataOperations


class DolphinDBClient:
    """
    DolphinDB 数据库客户端（线程安全单例）

    整合了连接管理、查询构建、元数据管理、数据初始化和数据操作功能
    """

    def __init__(self) -> None:
        """初始化 DolphinDB 客户端"""
        # 初始化各个组件
        self._connection = DolphinDBConnection()
        self._query_builder = QueryBuilder(self._connection)
        self._meta_manager = MetadataManager(self._connection)
        self._seed_manager = SeedDataManager(self._connection, self._query_builder)
        self._data_ops = DataOperations(self._connection, self._query_builder)

        logger.info("DolphinDB client components initialized")

    # ------------------------------------------------------------------
    #  连接管理（委托给 DolphinDBConnection）
    # ------------------------------------------------------------------

    def close(self) -> None:
        """关闭 DolphinDB 连接"""
        self._connection.close()

    # ------------------------------------------------------------------
    #  查询接口（委托给 QueryBuilder）
    # ------------------------------------------------------------------

    def query(
        self,
        sql: str,
        params: Optional[tuple] = None,
    ) -> pl.DataFrame:
        """
        执行查询并返回 Polars DataFrame

        Args:
            sql: SQL 查询语句（支持 %s 占位符）
            params: 查询参数

        Returns:
            pl.DataFrame
        """
        return self._query_builder.query(sql, params)

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """
        执行 SQL 语句（不返回结果）

        Args:
            sql: SQL 语句
            params: 参数
        """
        self._query_builder.execute(sql, params)

    # ------------------------------------------------------------------
    #  元数据表管理（委托给 MetadataManager）
    # ------------------------------------------------------------------

    def ensure_meta_tables(self) -> None:
        """
        检查并创建所有缺失的维度表
        对已存在的表，补加代码定义里有但实际表缺少的列
        """
        self._meta_manager.ensure_meta_tables()

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return self._meta_manager.table_exists(table_name)

    def create_table(
        self,
        table_name: str,
        schema: Dict[str, Dict[str, Any]],
        primary_keys: List[str],
    ) -> None:
        """
        在数据库中创建维度表

        Args:
            table_name: 表名
            schema: 列定义字典 {列名: {type, nullable, comment}}
            primary_keys: 主键列表
        """
        self._meta_manager.create_table(table_name, schema, primary_keys)
        # 注册到查询构建器的已知表集合
        self._query_builder.register_meta_table(table_name)

    def list_tables(self) -> List[Dict[str, Any]]:
        """
        列出数据库中所有已存在的表及其行数和列信息

        Returns:
            [{"table_name": str, "row_count": int, "columns": [str], "column_count": int}, ...]
        """
        return self._meta_manager.list_tables()

    def get_table_columns(self, table_name: str) -> List[str]:
        """获取指定表的列名列表"""
        return self._meta_manager.get_table_columns(table_name)

    def drop_table(self, table_name: str) -> None:
        """删除指定表"""
        self._meta_manager.drop_table(table_name)

    def register_meta_table(self, table_name: str) -> None:
        """将表名注册到元数据表集合（如果尚未注册）"""
        self._query_builder.register_meta_table(table_name)

    # ------------------------------------------------------------------
    #  任务版本管理（委托给 MetadataManager）
    # ------------------------------------------------------------------

    def create_task_version(
        self,
        task_id: str,
        task_name: str,
        description: str,
        script: str,
        sync_type: str,
        date_field: str,
        primary_keys_json: str,
        table_name: str,
        changed_by: str = "system",
        change_reason: str = "",
    ) -> int:
        """创建任务新版本（版本号自增）"""
        return self._meta_manager.create_task_version(
            task_id, task_name, description, script, sync_type,
            date_field, primary_keys_json, table_name, changed_by, change_reason
        )

    def get_task_versions(
        self,
        task_id: str,
        limit: int = 10,
    ) -> pl.DataFrame:
        """获取任务的版本历史"""
        return self._meta_manager.get_task_versions(task_id, limit)

    def get_task_version(
        self,
        task_id: str,
        version_number: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取任务的指定版本（默认当前版本）"""
        return self._meta_manager.get_task_version(task_id, version_number)

    def rollback_task_version(
        self,
        task_id: str,
        target_version: int,
        changed_by: str = "system",
        change_reason: str = "rollback",
    ) -> bool:
        """回滚任务到指定版本"""
        return self._meta_manager.rollback_task_version(
            task_id, target_version, changed_by, change_reason
        )

    def get_current_task_version(self, task_id: str) -> Optional[int]:
        """获取任务的当前版本号"""
        return self._meta_manager.get_current_task_version(task_id)

    # ------------------------------------------------------------------
    #  数据初始化（委托给 SeedDataManager）
    # ------------------------------------------------------------------

    def seed_sync_task_config(self) -> None:
        """如果 sync_task_config 表为空，则写入默认同步任务定义"""
        self._seed_manager.seed_sync_task_config()

    def seed_etl_task_config(self) -> None:
        """如果 etl_task_config 表为空，则写入默认 ETL 任务定义"""
        self._seed_manager.seed_etl_task_config()

    def seed_factor_data_config(self) -> None:
        """如果 factor_data_config 表为空，则写入默认字段映射"""
        self._seed_manager.seed_factor_data_config()

    def seed_factor_metadata(self) -> None:
        """如果 factor_metadata 表为空，则写入默认种子因子定义"""
        self._seed_manager.seed_factor_metadata()

    # ------------------------------------------------------------------
    #  数据操作（委托给 DataOperations）
    # ------------------------------------------------------------------

    def upsert(
        self,
        table_name: str,
        df: pl.DataFrame,
        key_columns: List[str],
        known_columns: Optional[List[str]] = None,
    ) -> None:
        """
        插入或更新数据

        Args:
            table_name: 表名
            df: Polars DataFrame
            key_columns: 主键列
            known_columns: 已知列顺序（跳过 schema 查询）
        """
        self._data_ops.upsert(table_name, df, key_columns, known_columns)

    def upsert_daily(self, df: pl.DataFrame) -> None:
        """插入或更新日线数据（兼容旧接口）"""
        self._data_ops.upsert_daily(df)

    def bulk_copy(
        self,
        table_name: str,
        df: pl.DataFrame,
        columns: List[str] = None,
        known_columns: Optional[List[str]] = None,
    ) -> int:
        """
        批量写入数据

        Args:
            table_name: 目标表名
            df: Polars DataFrame
            columns: 列名列表
            known_columns: 已知列顺序

        Returns:
            写入的行数
        """
        return self._data_ops.bulk_copy(table_name, df, columns, known_columns)

    def get_last_sync_date(self, source: str, data_type: str) -> Optional[str]:
        """获取最后同步日期"""
        return self._data_ops.get_last_sync_date(source, data_type)

    def update_sync_log(
        self,
        source: str,
        data_type: str,
        last_date: str,
    ) -> None:
        """更新同步日志"""
        self._data_ops.update_sync_log(source, data_type, last_date)


# ------------------------------------------------------------------
#  单例代理（向后兼容）
# ------------------------------------------------------------------

class _DBClientProxy:
    """延迟初始化的单例代理"""

    _instance: Optional[DolphinDBClient] = None

    def __getattr__(self, name):
        if self._instance is None:
            self._instance = DolphinDBClient()
        return getattr(self._instance, name)


# 全局单例实例（向后兼容原有的 `from store.dolphindb_client import db_client`）
db_client = _DBClientProxy()


__all__ = [
    "DolphinDBClient",
    "DolphinDBConnection",
    "QueryBuilder",
    "MetadataManager",
    "SeedDataManager",
    "DataOperations",
    "db_client",
]
