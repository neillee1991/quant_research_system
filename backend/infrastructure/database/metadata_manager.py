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
    _META_TABLE_SCHEMAS = {
        "factor_metadata": (
            "table("
            "array(SYMBOL,0) as factor_id,"
            "array(STRING,0) as description,"
            "array(STRING,0) as category,"
            "array(STRING,0) as compute_mode,"
            "array(STRING,0) as storage_target,"
            "array(STRING,0) as depends_on,"
            "array(STRING,0) as params,"
            "array(STRING,0) as code,"
            "array(BOOL,0) as enabled,"
            "array(BOOL,0) as align_calendar,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["factor_id"],
        ),
        "factor_analysis": (
            "table("
            "array(SYMBOL,0) as factor_id,"
            "array(TIMESTAMP,0) as analysis_date,"
            "array(STRING,0) as start_date,"
            "array(STRING,0) as end_date,"
            "array(STRING,0) as periods,"
            "array(DOUBLE,0) as ic_mean,"
            "array(DOUBLE,0) as ic_std,"
            "array(DOUBLE,0) as rank_ic_mean,"
            "array(DOUBLE,0) as rank_ic_std,"
            "array(DOUBLE,0) as ic_ir,"
            "array(DOUBLE,0) as turnover_mean,"
            "array(TIMESTAMP,0) as created_at)",
            ["factor_id", "analysis_date"],
        ),
        "factor_task_run": (
            "table("
            "array(SYMBOL,0) as factor_id,"
            "array(SYMBOL,0) as mode,"
            "array(SYMBOL,0) as status,"
            "array(STRING,0) as start_date,"
            "array(STRING,0) as end_date,"
            "array(INT,0) as rows_affected,"
            "array(DOUBLE,0) as duration_seconds,"
            "array(BOOL,0) as filter_st,"
            "array(BOOL,0) as filter_new_stock,"
            "array(INT,0) as new_stock_days,"
            "array(BOOL,0) as mark_limit,"
            "array(STRING,0) as adjust_price,"
            "array(STRING,0) as preprocess,"
            "array(STRING,0) as run_id,"
            "array(STRING,0) as error_message,"
            "array(TIMESTAMP,0) as created_at)",
            ["factor_id", "created_at"],
        ),
        "sync_task_config": (
            "table("
            "array(SYMBOL,0) as task_id,"
            "array(SYMBOL,0) as api_name,"
            "array(STRING,0) as description,"
            "array(SYMBOL,0) as sync_type,"
            "array(STRING,0) as params_json,"
            "array(SYMBOL,0) as date_field,"
            "array(STRING,0) as primary_keys_json,"
            "array(SYMBOL,0) as table_name,"
            "array(STRING,0) as schema_json,"
            "array(BOOL,0) as enabled,"
            "array(INT,0) as api_limit,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["task_id"],
        ),
        "etl_task_config": (
            "table("
            "array(SYMBOL,0) as task_id,"
            "array(STRING,0) as description,"
            "array(STRING,0) as script,"
            "array(SYMBOL,0) as sync_type,"
            "array(STRING,0) as date_field,"
            "array(STRING,0) as primary_keys_json,"
            "array(SYMBOL,0) as table_name,"
            "array(BOOL,0) as enabled,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["task_id"],
        ),
        "factor_data_config": (
            "table("
            "array(SYMBOL,0) as field_key,"
            "array(STRING,0) as description,"
            "array(SYMBOL,0) as table_name,"
            "array(SYMBOL,0) as column_name,"
            "array(STRING,0) as extra_config,"
            "array(TIMESTAMP,0) as updated_at)",
            ["field_key", "updated_at"],
        ),
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
        "trade_cal": (
            "table("
            "array(SYMBOL,0) as exchange,"
            "array(DATE,0) as cal_date,"
            "array(INT,0) as is_open,"
            "array(DATE,0) as pretrade_date)",
            ["exchange", "cal_date"],
        ),
        "stock_basic": (
            "table("
            "array(SYMBOL,0) as ts_code,"
            "array(STRING,0) as symbol,"
            "array(STRING,0) as name,"
            "array(STRING,0) as area,"
            "array(STRING,0) as industry,"
            "array(STRING,0) as market,"
            "array(DATE,0) as list_date,"
            "array(STRING,0) as list_status)",
            ["ts_code"],
        ),
        "index_metadata": (
            "table("
            "array(SYMBOL,0) as index_code,"
            "array(STRING,0) as index_name,"
            "array(STRING,0) as description,"
            "array(INT,0) as stock_count,"
            "array(DATE,0) as latest_date,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["index_code"],
        ),
        "user_sync_preference": (
            "table("
            "array(SYMBOL,0) as user_id,"
            "array(SYMBOL,0) as index_table,"
            "array(STRING,0) as filter_config,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["user_id"],
        ),
        "task_runs": (
            "table("
            "array(SYMBOL,0) as run_id,"
            "array(SYMBOL,0) as task_type,"
            "array(SYMBOL,0) as task_id,"
            "array(STRING,0) as task_name,"
            "array(SYMBOL,0) as status,"
            "array(TIMESTAMP,0) as started_at,"
            "array(TIMESTAMP,0) as finished_at,"
            "array(DOUBLE,0) as elapsed_sec,"
            "array(INT,0) as rows,"
            "array(STRING,0) as error,"
            "array(STRING,0) as params)",
            ["run_id"],
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

