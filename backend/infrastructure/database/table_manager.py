"""
DolphinDB 表管理模块
负责表的创建、删除、列举等操作
"""
import re
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.core.logger import logger


class TableManager:
    """DolphinDB 表管理器"""

    # 元数据表集合（维度表，存储在 meta 数据库）
    # 注意：配置表已迁移到 PostgreSQL，此集合仅保留仍在 DolphinDB 的维度表
    _META_TABLES = frozenset({
        "stock_basic",
        "trade_cal",
    })

    # TSDB 表集合（时间序列表，存储在 TSDB 数据库）
    _TSDB_TABLES = frozenset({
        "sync_daily_data", "sync_daily_basic", "sync_adj_factor",
        "sync_index_daily", "sync_moneyflow", "factor_values"
    })

    _ALL_TABLES = _META_TABLES | _TSDB_TABLES

    def __init__(self, connection, sql_adapter):
        """
        初始化表管理器

        Args:
            connection: DolphinDBConnection 实例
            sql_adapter: SQLAdapter 实例
        """
        self._conn = connection
        self._sql_adapter = sql_adapter

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        db_path = self._resolve_db_path(table_name)
        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                result = self._conn.session.run(
                    f"existsTable('{db_path}', '{table_name}')"
                )
            return bool(result)
        except Exception as e:
            logger.error(f"检查表是否存在失败 [{table_name}]: {e}")
            return False

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
        if not schema:
            logger.warning(f"schema 为空，跳过建表: {table_name}")
            return

        # 先注册到 _META_TABLES，确保后续 _resolve_db_path 路由正确
        self.register_meta_table(table_name)

        db_path = self._conn.db_path
        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                exists = self._conn.session.run(f"existsTable('{db_path}', '{table_name}')")
            if exists:
                logger.info(f"表 {table_name} 已存在，跳过建表")
                return

            # SQL 类型 → DolphinDB 类型映射
            _TYPE_MAP = {
                "VARCHAR": "SYMBOL", "TEXT": "STRING", "CHAR": "SYMBOL",
                "INTEGER": "INT", "INT": "INT", "BIGINT": "LONG",
                "DOUBLE PRECISION": "DOUBLE", "DOUBLE": "DOUBLE",
                "FLOAT": "FLOAT", "REAL": "FLOAT",
                "BOOLEAN": "BOOL", "DATE": "DATE",
                "TIMESTAMP": "TIMESTAMP", "DATETIME": "TIMESTAMP",
            }

            def _map_type(t: str) -> str:
                return _TYPE_MAP.get(t.upper(), t.upper())

            # 构建列定义
            col_defs = ",".join([
                f"array({_map_type(col_def.get('type', 'STRING'))},0) as {col_name}"
                for col_name, col_def in schema.items()
            ])

            # TSDB 引擎要求 primaryKey 最后一列为时间或整数类型
            # 确定 sort key 列：优先用 primary_keys，末尾追加一个时间/整数列
            _TEMPORAL_INT_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "INT", "LONG", "SHORT"}
            pk_list = list(primary_keys) if primary_keys else [list(schema.keys())[0]]
            last_pk_type = _map_type(schema.get(pk_list[-1], {}).get("type", "STRING"))
            if last_pk_type not in _TEMPORAL_INT_TYPES:
                # 找 schema 中可用的时间/整数列追加到末尾
                for col_name, col_def in schema.items():
                    if _map_type(col_def.get("type", "STRING")) in _TEMPORAL_INT_TYPES and col_name not in pk_list:
                        pk_list.append(col_name)
                        break
                else:
                    # 没有合适的列，补一个 created_at TIMESTAMP 列
                    col_defs += ",array(TIMESTAMP,0) as created_at"
                    pk_list.append("created_at")
            pk_str = "`" + "`".join(pk_list)
            script = (
                f"dbMeta = database('{db_path}');"
                f"schema_{table_name} = table({col_defs});"
                f"createTable(dbHandle=dbMeta, table=schema_{table_name}, tableName=`{table_name}, primaryKey={pk_str});"
            )
            with self._conn.lock:
                self._conn._ensure_connected()
                self._conn.session.run(script)
            logger.info(f"Created table {table_name} with {len(schema)} columns, primary_keys: {primary_keys}")
        except Exception as e:
            logger.error(f"建表失败 [{table_name}]: {e}")
            raise

    def list_tables(self) -> List[Dict[str, Any]]:
        """
        列出数据库中所有已存在的表及其行数和列信息
        动态查询数据库，不依赖硬编码的表名列表

        Returns:
            [{"table_name": str, "row_count": int, "columns": [str], "column_count": int}, ...]
        """
        results = []
        db_path = self._conn.db_path

        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                # 使用 getTables() 函数获取数据库中的所有表
                tables_result = self._conn.session.run(
                    f"getTables(database('{db_path}'))"
                )

            table_names = []
            if tables_result is not None:
                if isinstance(tables_result, np.ndarray):
                    table_names = tables_result.tolist()
                elif isinstance(tables_result, list):
                    table_names = tables_result
                elif isinstance(tables_result, pd.DataFrame):
                    if "tableName" in tables_result.columns:
                        table_names = tables_result["tableName"].tolist()
                    elif len(tables_result.columns) > 0:
                        table_names = tables_result.iloc[:, 0].tolist()

            for tbl in table_names:
                try:
                    with self._conn.lock:
                        self._conn._ensure_connected()
                        # 获取行数
                        count_result = self._conn.session.run(
                            f"exec count(*) from loadTable('{db_path}', '{tbl}')"
                        )
                        row_count = int(count_result) if count_result is not None else 0

                        # 获取列信息
                        schema_info = self._conn.session.run(
                            f"schema(loadTable('{db_path}', '{tbl}'))"
                        )

                    columns = []
                    if isinstance(schema_info, dict) and "colDefs" in schema_info:
                        col_defs_df = schema_info["colDefs"]
                        if isinstance(col_defs_df, pd.DataFrame) and "name" in col_defs_df.columns:
                            columns = col_defs_df["name"].tolist()

                    results.append({
                        "table_name": tbl,
                        "row_count": row_count,
                        "columns": columns,
                        "column_count": len(columns)
                    })
                except Exception as e:
                    logger.warning(f"获取表 {tbl} 信息失败: {e}")
                    results.append({
                        "table_name": tbl,
                        "row_count": 0,
                        "columns": [],
                        "column_count": 0
                    })

            return results
        except Exception as e:
            logger.error(f"列出表失败: {e}")
            return []

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        获取表的列名列表

        Args:
            table_name: 表名

        Returns:
            列名列表
        """
        db_path = self._resolve_db_path(table_name)
        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                schema_info = self._conn.session.run(
                    f"schema(loadTable('{db_path}', '{table_name}'))"
                )
            if isinstance(schema_info, dict) and "colDefs" in schema_info:
                col_defs_df = schema_info["colDefs"]
                if isinstance(col_defs_df, pd.DataFrame) and "name" in col_defs_df.columns:
                    return col_defs_df["name"].tolist()
            return []
        except Exception as e:
            logger.error(f"获取表列失败 [{table_name}]: {e}")
            return []

    def drop_table(self, table_name: str) -> None:
        """
        删除表

        Args:
            table_name: 表名
        """
        db_path = self._resolve_db_path(table_name)
        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                self._conn.session.run(
                    f"dropTable(database('{db_path}'), '{table_name}')"
                )
            logger.info(f"表 {table_name} 已删除")
        except Exception as e:
            logger.error(f"删除表失败 [{table_name}]: {e}")
            raise

    def _resolve_db_path(self, table_name: str) -> str:
        """
        根据表名返回所属数据库路径

        Args:
            table_name: 表名

        Returns:
            数据库路径
        """
        return self._conn.db_path

    def register_meta_table(self, table_name: str) -> None:
        """
        将表名注册到元数据表集合（如果尚未注册）

        Args:
            table_name: 表名
        """
        if table_name not in self._META_TABLES:
            # 使用类变量而不是实例变量
            TableManager._META_TABLES = TableManager._META_TABLES | frozenset({table_name})
            TableManager._ALL_TABLES = TableManager._META_TABLES | TableManager._TSDB_TABLES
