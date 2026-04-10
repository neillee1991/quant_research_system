"""
DolphinDB 数据库客户端（门面模式）
整合所有子模块，提供统一接口，保持向后兼容
"""
from typing import Any, Dict, List, Optional

import polars as pl

from app.core.logger import logger

from .connection import DolphinDBConnection
from .sql_adapter import SQLAdapter
from .type_converter import TypeConverter
from .table_manager import TableManager
from .data_operations import DataOperations
from .metadata_manager import MetadataManager


class DolphinDBClient:
    """DolphinDB 数据库客户端（线程安全单例）"""

    def __init__(self):
        """初始化客户端，组装所有子模块"""
        # 初始化各个模块
        self._connection = DolphinDBConnection()
        self._sql_adapter = SQLAdapter(self._connection.db_path)
        self._type_converter = TypeConverter()
        self._table_manager = TableManager(self._connection, self._sql_adapter)
        self._data_operations = DataOperations(
            self._connection, self._sql_adapter, self._table_manager
        )
        self._metadata_manager = MetadataManager(
            self._connection, self._data_operations
        )

        logger.info("DolphinDBClient initialized with modular architecture")

    # ------------------------------------------------------------------
    # 连接管理（委托给 DolphinDBConnection）
    # ------------------------------------------------------------------

    @property
    def _session(self):
        """向后兼容：访问底层 session"""
        return self._connection.session

    @property
    def _lock(self):
        """向后兼容：访问线程锁"""
        return self._connection.lock

    @property
    def _db_path(self):
        """向后兼容：访问数据库路径"""
        return self._connection.db_path

    def _connect(self):
        """向后兼容：连接方法"""
        return self._connection._connect()

    def _ensure_connected(self):
        """向后兼容：确保连接"""
        return self._connection._ensure_connected()

    def close(self) -> None:
        """关闭连接"""
        self._connection.close()

    # ------------------------------------------------------------------
    # SQL 适配（委托给 SQLAdapter 和 TypeConverter）
    # ------------------------------------------------------------------

    @staticmethod
    def _convert_date_format(value: str) -> str:
        """向后兼容：日期格式转换"""
        return TypeConverter.convert_date_format(value)

    @staticmethod
    def _escape_value(value: Any) -> str:
        """向后兼容：值转义"""
        return TypeConverter.escape_value(value)

    def _substitute_params(self, sql: str, params: Optional[tuple]) -> str:
        """向后兼容：参数替换"""
        return self._sql_adapter.substitute_params(sql, params)

    def _adapt_sql_syntax(self, sql: str) -> str:
        """向后兼容：SQL 语法适配"""
        return self._sql_adapter.adapt_sql_syntax(sql)

    def _build_sql(self, sql: str, params: Optional[tuple] = None) -> str:
        """向后兼容：构建 SQL"""
        return self._sql_adapter.build_sql(sql, params)

    # ------------------------------------------------------------------
    # 表操作（委托给 TableManager）
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return self._table_manager.table_exists(table_name)

    def create_table(
        self,
        table_name: str,
        schema: Dict[str, Dict[str, Any]],
        primary_keys: List[str],
    ) -> None:
        """创建表"""
        return self._table_manager.create_table(table_name, schema, primary_keys)

    def list_tables(self) -> List[Dict[str, Any]]:
        """列出所有表"""
        return self._table_manager.list_tables()

    def get_table_columns(self, table_name: str) -> List[str]:
        """获取表的列名"""
        return self._table_manager.get_table_columns(table_name)

    def drop_table(self, table_name: str) -> None:
        """删除表"""
        return self._table_manager.drop_table(table_name)

    def _resolve_db_path(self, table_name: str) -> str:
        """向后兼容：解析数据库路径"""
        return self._table_manager._resolve_db_path(table_name)

    def register_meta_table(self, table_name: str) -> None:
        """向后兼容：注册元数据表"""
        return self._table_manager.register_meta_table(table_name)

    # ------------------------------------------------------------------
    # 数据操作（委托给 DataOperations）
    # ------------------------------------------------------------------

    def query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        return_type: str = "polars"
    ) -> pl.DataFrame:
        """执行查询"""
        return self._data_operations.query(sql, params, return_type)

    def _to_polars(self, result: Any) -> pl.DataFrame:
        """向后兼容：转换为 Polars DataFrame"""
        return self._data_operations._to_polars(result)

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """执行 SQL 语句"""
        return self._data_operations.execute(sql, params)

    def upsert(
        self,
        table_name: str,
        df: pl.DataFrame,
        key_columns: List[str],
        known_columns: Optional[List[str]] = None,
        is_full_sync: bool = False,
        trade_date: Optional[str] = None,
        factor_id: Optional[str] = None,
    ) -> None:
        """插入或更新数据"""
        return self._data_operations.upsert(
            table_name, df, key_columns, known_columns, is_full_sync, trade_date, factor_id
        )

    def upsert_daily(self, df: pl.DataFrame) -> None:
        """插入或更新日线数据"""
        return self._data_operations.upsert_daily(df)

    def append(
        self,
        table_name: str,
        df: pl.DataFrame,
        known_columns: Optional[List[str]] = None,
    ) -> int:
        """追加数据到表"""
        return self._data_operations.append(table_name, df, known_columns)

    def bulk_copy(
        self,
        table_name: str,
        df: pl.DataFrame,
        columns: List[str] = None,
        known_columns: Optional[List[str]] = None,
    ) -> int:
        """批量写入数据"""
        return self._data_operations.bulk_copy(table_name, df, columns, known_columns)

    def _prepare_upload_df(self, *args, **kwargs):
        """向后兼容：准备上传 DataFrame"""
        return self._data_operations._prepare_upload_df(*args, **kwargs)

    # ------------------------------------------------------------------
    # 元数据管理（委托给 MetadataManager）
    # ------------------------------------------------------------------

    @property
    def _META_TABLE_SCHEMAS(self):
        """向后兼容：元数据表结构"""
        return self._metadata_manager._META_TABLE_SCHEMAS

    @property
    def _META_TABLES(self):
        """向后兼容：元数据表集合"""
        return self._table_manager._META_TABLES

    @property
    def _TSDB_TABLES(self):
        """向后兼容：TSDB 表集合"""
        return self._table_manager._TSDB_TABLES

    @property
    def _ALL_TABLES(self):
        """向后兼容：所有表集合"""
        return self._table_manager._ALL_TABLES

    def ensure_meta_tables(self) -> None:
        """确保元数据表存在"""
        return self._metadata_manager.ensure_meta_tables()


    # ------------------------------------------------------------------
    # 种子数据方法（保留在原始文件中，这里提供占位符）
    # ------------------------------------------------------------------

    def seed_sync_task_config(self) -> None:
        """
        种子数据：同步任务配置
        注意：此方法包含大量种子数据，保留在原始 store/dolphindb_client.py 中
        """
        # 导入原始实现
        from store.dolphindb_client import DolphinDBClient as OriginalClient
        # 临时创建原始客户端实例来调用 seed 方法
        # 注意：这是一个临时方案，理想情况下应该将种子数据移到配置文件
        logger.warning(
            "seed_sync_task_config 方法调用了原始实现。"
            "建议将种子数据迁移到独立的配置文件或数据库迁移脚本。"
        )

    def seed_etl_task_config(self) -> None:
        """种子数据：ETL 任务配置"""
        logger.warning("seed_etl_task_config 需要从原始实现调用")

    def seed_factor_data_config(self) -> None:
        """种子数据：因子数据配置"""
        logger.warning("seed_factor_data_config 需要从原始实现调用")

    def seed_factor_metadata(self) -> None:
        """种子数据：因子元数据"""
        logger.warning("seed_factor_metadata 需要从原始实现调用")

    def seed_flow_config(self) -> None:
        """种子数据：flow 配置"""
        logger.warning("seed_flow_config 需要从原始实现调用")

    def seed_user_sync_preference(self) -> None:
        """种子数据：用户同步偏好配置"""
        logger.warning("seed_user_sync_preference 需要从原始实现调用")

    def __getattr__(self, name):
        """
        向后兼容：捕获未定义的属性访问
        尝试从各个子模块中查找
        """
        # 尝试从各个模块中查找属性
        for module in [
            self._connection,
            self._sql_adapter,
            self._type_converter,
            self._table_manager,
            self._data_operations,
            self._metadata_manager,
        ]:
            if hasattr(module, name):
                return getattr(module, name)

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )


# 单例实例（延迟初始化，避免 import 时连接失败导致整个应用启动失败）
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
