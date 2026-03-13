"""
DolphinDB 元数据表管理模块
负责元数据表的创建、Schema 管理和版本控制
"""
import re
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import polars as pl

from app.core.logger import logger
from .connection import DolphinDBConnection


class MetadataManager:
    """元数据表管理器"""

    # 维度表 Schema 定义（用于动态建表）
    # 格式: { "table_name": ("DolphinDB table(...) 建表表达式", [primaryKey列]) }
    _META_TABLE_SCHEMAS: Dict[str, tuple] = {
        "sync_log": (
            "table("
            "array(SYMBOL,0) as source,"
            "array(SYMBOL,0) as data_type,"
            "array(STRING,0) as last_date,"
            "array(TIMESTAMP,0) as updated_at)",
            ["source", "data_type", "updated_at"],
        ),
        "sync_log_history": (
            "table("
            "array(SYMBOL,0) as source,"
            "array(SYMBOL,0) as data_type,"
            "array(STRING,0) as last_date,"
            "array(STRING,0) as sync_date,"
            "array(INT,0) as rows_synced,"
            "array(SYMBOL,0) as status,"
            "array(STRING,0) as error_message,"
            "array(STRING,0) as params,"
            "array(TIMESTAMP,0) as created_at)",
            ["source", "created_at"],
        ),
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
        "production_task_run": (
            "table("
            "array(SYMBOL,0) as factor_id,"
            "array(STRING,0) as run_id,"
            "array(STRING,0) as start_date,"
            "array(STRING,0) as end_date,"
            "array(SYMBOL,0) as status,"
            "array(STRING,0) as error_message,"
            "array(TIMESTAMP,0) as started_at,"
            "array(TIMESTAMP,0) as finished_at)",
            ["factor_id", "run_id"],
        ),
        "sync_task_config": (
            "table("
            "array(SYMBOL,0) as task_id,"
            "array(SYMBOL,0) as api_name,"
            "array(STRING,0) as description,"
            "array(SYMBOL,0) as sync_type,"
            "array(STRING,0) as params_json,"
            "array(STRING,0) as date_field,"
            "array(STRING,0) as primary_keys_json,"
            "array(SYMBOL,0) as table_name,"
            "array(STRING,0) as schema_json,"
            "array(INT,0) as api_limit,"
            "array(BOOL,0) as enabled,"
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
            "array(STRING,0) as schema_json,"
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
        # factor_values 表使用特殊的分区策略，不在此处创建
        # 由 ensure_factor_values_table() 方法单独处理
        # "factor_values": (...),  # 已移除，使用分区表创建逻辑
    }

    def __init__(self, connection: DolphinDBConnection) -> None:
        """
        初始化元数据管理器

        Args:
            connection: DolphinDB 连接管理器
        """
        self.conn = connection

    def ensure_meta_tables(self) -> None:
        """
        检查并创建所有缺失的维度表
        对已存在的表，补加代码定义里有但实际表缺少的列
        应在应用首次启动时调用一次

        注意：factor_values 表使用分区策略，由 ensure_factor_values_table() 单独创建
        """
        db_path = self.conn.db_path
        created: List[str] = []
        altered: List[str] = []

        with self.conn.lock:
            self.conn._ensure_connected()
            for tbl, (schema_expr, pk_cols) in self._META_TABLE_SCHEMAS.items():
                try:
                    exists = self.conn.session.run(
                        f"existsTable('{db_path}', '{tbl}')"
                    )
                    if not exists:
                        pk_str = "`" + "`".join(pk_cols)
                        script = (
                            f"dbMeta = database('{db_path}');"
                            f"schema_{tbl} = {schema_expr};"
                            f"createTable(dbHandle=dbMeta, table=schema_{tbl}, "
                            f"tableName=`{tbl}, primaryKey={pk_str});"
                        )
                        self.conn.session.run(script)
                        created.append(tbl)
                    else:
                        # 补加代码定义里有但实际表缺少的列
                        schema_info = self.conn.session.run(
                            f"schema(loadTable('{db_path}', '{tbl}')).colDefs"
                        )
                        existing_cols = (
                            set(schema_info['name'].tolist())
                            if schema_info is not None
                            else set()
                        )

                        # 从 schema_expr 解析期望的列定义
                        col_defs = re.findall(
                            r'array\((\w+),0\)\s+as\s+(\w+)', schema_expr
                        )
                        # col_defs: [(type, name), ...]
                        added: List[str] = []
                        for dfs_type, col_name in col_defs:
                            if col_name not in existing_cols:
                                try:
                                    self.conn.session.run(
                                        f"tbl_handle = loadTable('{db_path}', '{tbl}');"
                                        f"addColumn(tbl_handle, `{col_name}, {dfs_type})"
                                    )
                                    added.append(col_name)
                                except Exception as add_err:
                                    logger.warning(
                                        f"给表 [{tbl}] 加列 [{col_name}] 失败: {add_err}"
                                    )
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

        # 创建 factor_values 分区表
        self.ensure_factor_values_table()

    def ensure_factor_values_table(self) -> None:
        """
        创建 factor_values 分区表（三维组合分区）

        分区策略：
        - 第一层：HASH(factor_id, 20) - 20 个因子桶
        - 第二层：RANGE(trade_date) - 按季度分区（2010-2040，120个季度）
        - 第三层：HASH(ts_code, 10) - 10 个股票桶
        - 总分区数：20 × 120 × 10 = 24,000 个分区

        优化效果：
        - 按股票查询（时序）：裁剪到 ~10 个分区
        - 按日期查询（横截面）：裁剪到 ~200 个分区
        - 按因子查询（全量）：裁剪到 ~1200 个分区
        """
        db_path = self.conn.db_path

        try:
            with self.conn.lock:
                self.conn._ensure_connected()

                # 检查表是否已存在
                exists = self.conn.session.run(
                    f"existsTable('{db_path}', 'factor_values')"
                )

                if exists:
                    logger.info("factor_values 表已存在，跳过创建")
                    return

                # 创建三层组合分区表
                create_script = f"""
                // 使用现有的 dfs://quant 数据库
                dbPath = "{db_path}";
                db = database(dbPath);

                // 创建表结构
                schema = table(
                    array(SYMBOL, 0) as ts_code,
                    array(DATE, 0) as trade_date,
                    array(STRING, 0) as factor_id,
                    array(DOUBLE, 0) as factor_value,
                    array(INT, 0) as quality_flag,
                    array(INT, 0) as task_version,
                    array(STRING, 0) as run_id,
                    array(STRING, 0) as data_version,
                    array(TIMESTAMP, 0) as created_at
                );

                // 在现有数据库中创建分区表
                // 使用数据库已有的分区方案（VALUE(trade_date 按月) × HASH(ts_code, 50)）
                pt = createPartitionedTable(
                    dbHandle=db,
                    table=schema,
                    tableName=`factor_values,
                    partitionColumns=`trade_date`ts_code,
                    sortColumns=`factor_id`ts_code`trade_date
                );

                // 返回成功标志
                1;
                """

                self.conn.session.run(create_script)
                logger.info("✅ factor_values 分区表创建成功")
                logger.info("   分区策略：VALUE(trade_date 按月) × HASH(ts_code, 50)")
                logger.info("   排序键：factor_id, trade_date, ts_code")

        except Exception as e:
            logger.error(f"❌ 创建 factor_values 分区表失败: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            是否存在
        """
        db_path = self.conn.db_path
        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                result = self.conn.session.run(
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

        # 使用配置的数据库路径
        db_path = self.conn.db_path
        logger.info(f"Creating table {table_name} in {db_path}")

        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                exists = self.conn.session.run(
                    f"existsTable('{db_path}', '{table_name}')"
                )
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
            _TEMPORAL_INT_TYPES = {
                "DATE", "DATETIME", "TIMESTAMP", "INT", "LONG", "SHORT"
            }
            pk_list = list(primary_keys) if primary_keys else [list(schema.keys())[0]]
            last_pk_type = _map_type(schema.get(pk_list[-1], {}).get("type", "STRING"))

            if last_pk_type not in _TEMPORAL_INT_TYPES:
                # 找 schema 中可用的时间/整数列追加到末尾
                for col_name, col_def in schema.items():
                    mapped_type = _map_type(col_def.get("type", "STRING"))
                    if mapped_type in _TEMPORAL_INT_TYPES and col_name not in pk_list:
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
                f"createTable(dbHandle=dbMeta, table=schema_{table_name}, "
                f"tableName=`{table_name}, primaryKey={pk_str});"
            )
            with self.conn.lock:
                self.conn._ensure_connected()
                self.conn.session.run(script)
            logger.info(
                f"Created table {table_name} in {db_path} with {len(schema)} columns, "
                f"primary_keys: {primary_keys}"
            )
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
        results: List[Dict[str, Any]] = []
        db_path = self.conn.db_path

        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                # 使用 getTables() 函数获取数据库中的所有表
                tables_result = self.conn.session.run(
                    f"getTables(database('{db_path}'))"
                )

            table_names: List[str] = []
            if tables_result is not None:
                if isinstance(tables_result, np.ndarray):
                    # getTables() 返回 numpy 数组
                    table_names = tables_result.tolist()
                elif isinstance(tables_result, list):
                    table_names = tables_result
        except Exception as e:
            logger.error(f"查询数据库 {db_path} 的表列表失败: {e}")
            return []

        # 获取每个表的详细信息
        for table_name in sorted(table_names):
            try:
                with self.conn.lock:
                    self.conn._ensure_connected()
                    schema_info = self.conn.session.run(
                        f"schema(loadTable('{db_path}', '{table_name}'))"
                    )
                columns: List[str] = []
                if isinstance(schema_info, dict) and "colDefs" in schema_info:
                    col_defs = schema_info["colDefs"]
                    if isinstance(col_defs, pd.DataFrame) and "name" in col_defs.columns:
                        columns = col_defs["name"].tolist()

                # 获取行数
                with self.conn.lock:
                    self.conn._ensure_connected()
                    row_count = self.conn.session.run(
                        f"exec count(*) from loadTable('{db_path}', '{table_name}')"
                    )
                    row_count = int(row_count) if row_count is not None else 0

                results.append({
                    "table_name": table_name,
                    "row_count": row_count,
                    "columns": columns,
                    "column_count": len(columns),
                })
            except Exception as e:
                logger.warning(f"获取表信息失败 [{table_name}]: {e}")
                continue

        return results

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        获取指定表的列名列表

        Args:
            table_name: 表名

        Returns:
            列名列表
        """
        db_path = self.conn.db_path
        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                schema_info = self.conn.session.run(
                    f"schema(loadTable('{db_path}', '{table_name}'))"
                )
            if isinstance(schema_info, dict) and "colDefs" in schema_info:
                col_defs = schema_info["colDefs"]
                if isinstance(col_defs, pd.DataFrame) and "name" in col_defs.columns:
                    return col_defs["name"].tolist()
            return []
        except Exception as e:
            logger.error(f"获取表列信息失败 [{table_name}]: {e}")
            return []

    def drop_table(self, table_name: str) -> None:
        """
        删除指定表

        Args:
            table_name: 表名
        """
        db_path = self.conn.db_path
        with self.conn.lock:
            self.conn._ensure_connected()
            self.conn.session.run(
                f"dropTable(database('{db_path}'), '{table_name}')"
            )
        logger.info(f"Dropped table {table_name}")

