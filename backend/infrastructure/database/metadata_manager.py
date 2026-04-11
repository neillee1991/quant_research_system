"""
DolphinDB 元数据管理模块
负责元数据表的创建、版本管理和种子数据初始化
"""
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl

from app.core.logger import logger


class MetadataManager:
    """DolphinDB 元数据管理器"""

    # 元数据表结构定义 (table_name: (schema_expr, primary_keys))
    # 注意：配置表（sync_task_configs, etl_task_configs, factor_configs 等）已迁移到 PostgreSQL
    # DolphinDB 只保留时序业务数据表
    _META_TABLE_SCHEMAS = {
        "factor_values": (
            "table("
            "array(SYMBOL,0) as ts_code,"
            "array(SYMBOL,0) as factor_id,"
            "array(DATE,0) as trade_date,"
            "array(DOUBLE,0) as factor_value,"
            "array(INT,0) as quality_flag,"
            "array(INT,0) as task_version,"
            "array(STRING,0) as run_id,"
            "array(STRING,0) as data_version,"
            "array(TIMESTAMP,0) as created_at)",
            ["ts_code", "factor_id", "trade_date"],
        ),
    }

    def __init__(self, connection, data_operations):
        """
        初始化元数据管理器

        Args:
            connection: DolphinDBConnection 实例
            data_operations: DataOperations 实例
        """
        self._conn = connection
        self._data_ops = data_operations

    def ensure_meta_tables(self) -> None:
        """
        检查并创建所有缺失的维度表。
        对已存在的表，补加代码定义里有但实际表缺少的列。
        应在应用首次启动时调用一次。
        """
        db_path = self._conn.db_path
        created = []
        altered = []
        with self._conn.lock:
            self._conn._ensure_connected()
            for tbl, (schema_expr, pk_cols) in self._META_TABLE_SCHEMAS.items():
                try:
                    exists = self._conn.session.run(
                        f"existsTable('{db_path}', '{tbl}')"
                    )
                    if not exists:
                        pk_str = "`" + "`".join(pk_cols)
                        script = (
                            f"dbMeta = database('{db_path}');"
                            f"schema_{tbl} = {schema_expr};"
                            f"createTable(dbHandle=dbMeta, table=schema_{tbl}, tableName=`{tbl}, primaryKey={pk_str});"
                        )
                        self._conn.session.run(script)
                        created.append(tbl)
                    else:
                        # 补加代码定义里有但实际表缺少的列
                        schema_info = self._conn.session.run(
                            f"schema(loadTable('{db_path}', '{tbl}')).colDefs"
                        )
                        existing_cols = set(schema_info['name'].tolist()) if schema_info is not None else set()

                        # 从 schema_expr 解析期望的列定义
                        col_defs = re.findall(r'array\((\w+),0\)\s+as\s+(\w+)', schema_expr)
                        # col_defs: [(type, name), ...]
                        added = []
                        for dfs_type, col_name in col_defs:
                            if col_name not in existing_cols:
                                try:
                                    self._conn.session.run(
                                        f"tbl_handle = loadTable('{db_path}', '{tbl}');"
                                        f"addColumn(tbl_handle, `{col_name}, {dfs_type})"
                                    )
                                    added.append(col_name)
                                except Exception as add_err:
                                    logger.warning(f"给表 [{tbl}] 加列 [{col_name}] 失败: {add_err}")
                        if added:
                            altered.append(f"{tbl}({', '.join(added)})")
                except Exception as e:
                    logger.error(f"动态创建/更新维度表失败 [{tbl}]: {e}")
                    raise
        if created:
            logger.info(f"动态创建了 {len(created)} 张维度表: {', '.join(created)}")
        if altered:
            logger.info(f"补加了缺失列: {'; '.join(altered)}")
        if not created and not altered:
            logger.info("所有维度表已存在且列完整，无需变更")

