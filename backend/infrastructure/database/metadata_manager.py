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
            "array(INT,0) as version_number,"
            "array(BOOL,0) as is_current,"
            "array(STRING,0) as description,"
            "array(STRING,0) as category,"
            "array(STRING,0) as compute_mode,"
            "array(STRING,0) as storage_target,"
            "array(STRING,0) as depends_on,"
            "array(STRING,0) as params,"
            "array(STRING,0) as code,"
            "array(STRING,0) as changed_by,"
            "array(STRING,0) as change_reason,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["factor_id", "version_number"],
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
            "array(BOOL,0) as handle_suspension,"
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
            "array(INT,0) as version_number,"
            "array(BOOL,0) as is_current,"
            "array(SYMBOL,0) as api_name,"
            "array(STRING,0) as description,"
            "array(SYMBOL,0) as sync_type,"
            "array(STRING,0) as params_json,"
            "array(SYMBOL,0) as date_field,"
            "array(STRING,0) as primary_keys_json,"
            "array(SYMBOL,0) as table_name,"
            "array(STRING,0) as schema_json,"
            "array(INT,0) as api_limit,"
            "array(STRING,0) as changed_by,"
            "array(STRING,0) as change_reason,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["task_id", "version_number"],
        ),
        "etl_task_config": (
            "table("
            "array(SYMBOL,0) as task_id,"
            "array(INT,0) as version_number,"
            "array(BOOL,0) as is_current,"
            "array(STRING,0) as description,"
            "array(STRING,0) as script,"
            "array(SYMBOL,0) as sync_type,"
            "array(STRING,0) as date_field,"
            "array(STRING,0) as primary_keys_json,"
            "array(SYMBOL,0) as table_name,"
            "array(STRING,0) as changed_by,"
            "array(STRING,0) as change_reason,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["task_id", "version_number"],
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

    def create_task_version(
        self,
        task_type: str,
        task_id: str,
        config_data: Dict[str, Any],
        changed_by: str = "system",
        change_reason: str = ""
    ) -> int:
        """
        创建任务配置新版本

        Args:
            task_type: 任务类型 ("sync", "etl", "factor")
            task_id: 任务ID
            config_data: 配置数据字典
            changed_by: 修改人
            change_reason: 修改原因

        Returns:
            新版本号
        """
        table_map = {
            "sync": "sync_task_config",
            "etl": "etl_task_config",
            "factor": "factor_metadata"
        }

        if task_type not in table_map:
            raise ValueError(f"Invalid task_type: {task_type}")

        table_name = table_map[task_type]
        id_field = "task_id" if task_type in ["sync", "etl"] else "factor_id"

        # 获取当前最大版本号
        sql = f"SELECT max(version_number) as max_ver FROM {table_name} WHERE {id_field} = %s"
        result = self._data_ops.query(sql, params=(task_id,))

        max_ver = 0
        if not result.is_empty() and result["max_ver"][0] is not None:
            max_ver = int(result["max_ver"][0])

        new_version = max_ver + 1
        now = datetime.now()

        # 将旧版本的 is_current 设为 false (使用 upsert 而非 UPDATE)
        if max_ver > 0:
            # 读取所有旧版本
            old_versions = self._data_ops.query(
                f"SELECT * FROM {table_name} WHERE {id_field} = %s AND is_current = true",
                params=(task_id,)
            )
            if not old_versions.is_empty():
                # 更新 is_current 为 false
                old_versions = old_versions.with_columns(
                    pl.lit(False).alias("is_current"),
                    pl.lit(now).alias("updated_at")
                )
                self._data_ops.upsert(table_name, old_versions, [id_field, "version_number"])

        # 插入新版本
        config_data[id_field] = task_id
        config_data["version_number"] = new_version
        config_data["is_current"] = True
        config_data["changed_by"] = changed_by
        config_data["change_reason"] = change_reason
        config_data["created_at"] = now
        config_data["updated_at"] = now

        df = pl.DataFrame([config_data])
        self._data_ops.upsert(table_name, df, [id_field, "version_number"])

        logger.info(f"Created {task_type} task version: {task_id} v{new_version}")
        return new_version

    def get_task_versions(
        self,
        task_type: str,
        task_id: str
    ) -> pl.DataFrame:
        """
        获取任务的所有版本历史

        Args:
            task_type: 任务类型 ("sync", "etl", "factor")
            task_id: 任务ID

        Returns:
            版本历史 DataFrame
        """
        table_map = {
            "sync": "sync_task_config",
            "etl": "etl_task_config",
            "factor": "factor_metadata"
        }

        if task_type not in table_map:
            raise ValueError(f"Invalid task_type: {task_type}")

        table_name = table_map[task_type]
        id_field = "task_id" if task_type in ["sync", "etl"] else "factor_id"

        sql = f"SELECT * FROM {table_name} WHERE {id_field} = %s ORDER BY version_number DESC"
        return self._data_ops.query(sql, params=(task_id,))

    def get_task_version(
        self,
        task_type: str,
        task_id: str,
        version: int
    ) -> Optional[Dict[str, Any]]:
        """
        获取任务的特定版本

        Args:
            task_type: 任务类型 ("sync", "etl", "factor")
            task_id: 任务ID
            version: 版本号

        Returns:
            版本配置字典，不存在返回 None
        """
        table_map = {
            "sync": "sync_task_config",
            "etl": "etl_task_config",
            "factor": "factor_metadata"
        }

        if task_type not in table_map:
            raise ValueError(f"Invalid task_type: {task_type}")

        table_name = table_map[task_type]
        id_field = "task_id" if task_type in ["sync", "etl"] else "factor_id"

        sql = f"SELECT * FROM {table_name} WHERE {id_field} = %s AND version_number = %s"
        result = self._data_ops.query(sql, params=(task_id, version))

        if result.is_empty():
            return None

        return result.to_dicts()[0]

    def rollback_task_version(
        self,
        task_type: str,
        task_id: str,
        target_version: int,
        changed_by: str = "system",
        change_reason: str = "Rollback"
    ) -> int:
        """
        回滚任务到指定版本（创建新版本，内容复制自目标版本）

        Args:
            task_type: 任务类型 ("sync", "etl", "factor")
            task_id: 任务ID
            target_version: 目标版本号
            changed_by: 修改人
            change_reason: 修改原因

        Returns:
            新版本号
        """
        target_config = self.get_task_version(task_type, task_id, target_version)
        if not target_config:
            raise ValueError(f"Version {target_version} not found for {task_type} task {task_id}")

        # 移除版本管理字段
        target_config.pop("version_number", None)
        target_config.pop("is_current", None)
        target_config.pop("created_at", None)
        target_config.pop("updated_at", None)

        return self.create_task_version(
            task_type=task_type,
            task_id=task_id,
            config_data=target_config,
            changed_by=changed_by,
            change_reason=f"{change_reason} (from v{target_version})"
        )

    def get_current_task_version(
        self,
        task_type: str,
        task_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        获取任务的当前版本

        Args:
            task_type: 任务类型 ("sync", "etl", "factor")
            task_id: 任务ID

        Returns:
            当前版本配置字典，不存在返回 None
        """
        table_map = {
            "sync": "sync_task_config",
            "etl": "etl_task_config",
            "factor": "factor_metadata"
        }

        if task_type not in table_map:
            raise ValueError(f"Invalid task_type: {task_type}")

        table_name = table_map[task_type]
        id_field = "task_id" if task_type in ["sync", "etl"] else "factor_id"

        sql = f"SELECT * FROM {table_name} WHERE {id_field} = %s AND is_current = true"
        result = self._data_ops.query(sql, params=(task_id,))

        if result.is_empty():
            return None

        return result.to_dicts()[0]

    # Note: Seed methods (seed_sync_task_config, seed_etl_task_config,
    # seed_factor_data_config, seed_factor_metadata) are intentionally
    # omitted from this refactored module to keep it focused.
    # These methods contain large amounts of seed data and should be
    # moved to separate configuration files or database migration scripts.
    # For backward compatibility, they remain in the original DolphinDBClient.
