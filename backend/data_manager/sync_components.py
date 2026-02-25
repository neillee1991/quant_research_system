"""
数据同步核心组件
拆分原 ConfigBasedSyncEngine 的职责
"""
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
import json
import polars as pl
import tushare as ts

from app.core.interfaces import ISyncTaskExecutor, IDataRepository
from app.core.exceptions import (
    SyncConfigError,
    SyncTaskNotFoundError,
    DataCollectionError
)
from app.core.utils import RateLimiter, RetryPolicy, DateUtils
from app.core.logger import logger
from app.core.config import settings
from app.core.constants import DEFAULT_START_DATE


class SyncConfigManager:
    """同步配置管理器 — 从 DolphinDB sync_task_config 表读取"""

    def __init__(self, config_path: Optional[str] = None):
        # config_path 保留签名兼容，但不再使用
        from store.dolphindb_client import db_client
        self._db = db_client
        self._cache: Optional[List[Dict[str, Any]]] = None

    # ------ 内部 ------

    def _load_tasks(self) -> List[Dict[str, Any]]:
        """从数据库加载全部任务配置并缓存"""
        if self._cache is not None:
            return self._cache
        try:
            df = self._db.query("SELECT * FROM sync_task_config")
            if df.is_empty():
                self._cache = []
                return self._cache
            seen: Dict[str, Dict[str, Any]] = {}
            for row in df.iter_rows(named=True):
                task = {
                    "task_id": row["task_id"],
                    "api_name": row["api_name"],
                    "description": row.get("description", ""),
                    "sync_type": row["sync_type"],
                    "params": json.loads(row["params_json"]) if row.get("params_json") else {},
                    "date_field": row.get("date_field", ""),
                    "primary_keys": json.loads(row["primary_keys_json"]) if row.get("primary_keys_json") else [],
                    "table_name": row["table_name"],
                    "schema": json.loads(row["schema_json"]) if row.get("schema_json") else {},
                    "enabled": bool(row.get("enabled", True)),
                    "api_limit": row.get("api_limit", 5000),
                    "schedule": row.get("schedule", "daily"),
                }
                seen[task["task_id"]] = task
            self._cache = list(seen.values())
            logger.info(f"从数据库加载了 {len(self._cache)} 条同步任务配置")
            return self._cache
        except Exception as e:
            raise SyncConfigError(f"从数据库加载同步任务配置失败: {e}")

    def reload(self) -> None:
        """清除缓存，下次访问时重新加载"""
        self._cache = None

    # ------ 公开接口（保持不变） ------

    def get_task(self, task_id: str) -> Dict[str, Any]:
        """获取任务配置"""
        for task in self._load_tasks():
            if task["task_id"] == task_id:
                return task
        raise SyncTaskNotFoundError(task_id)

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """获取所有任务"""
        return self._load_tasks()

    def get_enabled_tasks(self) -> List[Dict[str, Any]]:
        """获取所有启用的任务"""
        return [t for t in self._load_tasks() if t.get("enabled", True)]

    def get_tasks_by_schedule(self, schedule: str) -> List[Dict[str, Any]]:
        """按调度类型获取任务"""
        return [
            t for t in self.get_enabled_tasks()
            if t.get("schedule") == schedule
        ]

    def get_global_config(self) -> Dict[str, Any]:
        """获取全局配置（现在从 settings 读取，不再依赖 JSON）"""
        return {
            "rate_limit": {
                "calls_per_minute": settings.collector.calls_per_minute,
                "retry_times": settings.collector.retry_times,
                "retry_delay": settings.collector.retry_delay,
            }
        }


class SyncLogManager:
    """同步日志管理器"""

    def __init__(self, repository: IDataRepository):
        self.repository = repository

    def get_last_sync_date(self, task_id: str) -> Optional[str]:
        """获取最后同步日期"""
        try:
            sql = """
                SELECT last_date
                FROM sync_log
                WHERE source = %s AND data_type = %s
                LIMIT 1
            """
            result = self.repository.query(sql, params=("tushare_config", task_id))
            if not result.is_empty():
                return result["last_date"][0]
        except Exception as e:
            logger.warning(f"Failed to get last sync date for {task_id}: {e}")
        return None

    def get_last_sync_info(self, task_id: str) -> Optional[dict]:
        """获取最后同步信息（包含同步时间和数据日期）"""
        try:
            sql = """
                SELECT last_date, updated_at
                FROM sync_log
                WHERE source = %s AND data_type = %s
                LIMIT 1
            """
            result = self.repository.query(sql, params=("tushare_config", task_id))
            if not result.is_empty():
                return {
                    "last_date": result["last_date"][0],
                    "updated_at": result["updated_at"][0].strftime("%Y-%m-%d %H:%M:%S") if result["updated_at"][0] else None
                }
        except Exception as e:
            logger.warning(f"Failed to get last sync info for {task_id}: {e}")
        return None

    def update_sync_log(self, task_id: str, sync_date: str, rows_synced: int = 0) -> None:
        """更新同步日志（同时记录历史）"""
        try:
            # 1. 更新 sync_log 表（保留最新状态）
            log_data = pl.DataFrame({
                "source": ["tushare_config"],
                "data_type": [task_id],
                "last_date": [sync_date],
                "updated_at": [datetime.now()]
            })
            self.repository.upsert(
                "sync_log",
                log_data,
                ["source", "data_type"]
            )

            # 2. 插入 sync_log_history 表（记录历史）
            history_data = pl.DataFrame({
                "source": ["tushare_config"],
                "data_type": [task_id],
                "last_date": [sync_date],
                "sync_date": [sync_date],
                "rows_synced": [rows_synced],
                "status": ["success"],
                "created_at": [datetime.now()]
            })
            # 使用 bulk_copy 直接追加到历史表
            self.repository.bulk_copy("sync_log_history", history_data)

            logger.debug(f"Updated sync log for {task_id}: {sync_date}, rows: {rows_synced}")
        except Exception as e:
            logger.error(f"Failed to update sync log: {e}")


class TableManager:
    """表结构管理器"""

    def __init__(self, repository: IDataRepository):
        self.repository = repository

    def ensure_table_exists(self, task: Dict[str, Any]) -> None:
        """确保表存在"""
        table_name = task["table_name"]
        primary_keys = task["primary_keys"]
        schema = task.get("schema", {})

        if self.repository.table_exists(table_name):
            logger.info(f"Table {table_name} already exists")
            self._add_missing_columns(table_name, schema)
        else:
            if schema:
                self.repository.create_table(table_name, schema, primary_keys)
                logger.info(f"Created table {table_name} with {len(schema)} columns")
            else:
                self._create_basic_table(table_name, primary_keys)
                logger.info(f"Created basic table {table_name}")

    def _add_missing_columns(
        self,
        table_name: str,
        schema: Dict[str, Dict[str, Any]]
    ) -> None:
        """添加缺失的列（DolphinDB）"""
        if not schema:
            return

        try:
            # 使用专用方法获取现有列
            existing_cols = self.repository.get_table_columns(table_name)

            # DolphinDB 类型映射
            type_map = {
                "VARCHAR": "STRING", "TEXT": "STRING", "CHAR": "STRING",
                "INTEGER": "INT", "INT": "INT", "BIGINT": "LONG",
                "DOUBLE PRECISION": "DOUBLE", "DOUBLE": "DOUBLE",
                "FLOAT": "FLOAT", "REAL": "FLOAT",
                "BOOLEAN": "BOOL", "DATE": "DATE",
                "TIMESTAMP": "TIMESTAMP", "DATETIME": "TIMESTAMP",
            }

            db_path = self.repository._resolve_db_path(table_name)
            for col_name, col_def in schema.items():
                if col_name not in existing_cols:
                    pg_type = col_def.get("type", "VARCHAR").upper()
                    ddb_type = type_map.get(pg_type, "STRING")
                    try:
                        self.repository.execute(
                            f'addColumn(loadTable("{db_path}", "{table_name}"), '
                            f'"{col_name}", {ddb_type})'
                        )
                        logger.info(f"Added column {col_name} ({ddb_type}) to {table_name}")
                    except Exception as e:
                        logger.warning(f"Failed to add column {col_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to check table columns: {e}")

    def _create_basic_table(
        self,
        table_name: str,
        primary_keys: List[str]
    ) -> None:
        """创建基础表结构（DolphinDB）
        注意：DolphinDB 的分区表应在 init_dolphindb.dos 中预先创建。
        此方法仅作为 fallback，创建维度表。
        """
        try:
            db_path = self.repository._resolve_db_path(table_name)
            # 检查表是否已存在
            exists = self.repository.table_exists(table_name)
            if exists:
                return

            # 创建一个基础维度表
            self.repository.execute(f"""
                t = table(1:0, `ts_code`trade_date, [SYMBOL, STRING]);
                db = database("{db_path}");
                db.createTable(t, `{table_name})
            """)
            logger.info(f"Created basic dimension table {table_name}")
        except Exception as e:
            logger.warning(f"Failed to create basic table {table_name}: {e}")


class TushareAPIClient:
    """Tushare API 客户端"""

    def __init__(
        self,
        token: str,
        rate_limiter: RateLimiter,
        retry_policy: RetryPolicy
    ):
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.rate_limiter = rate_limiter
        self.retry_policy = retry_policy

    def call_api(self, api_name: str, **kwargs) -> Optional[pl.DataFrame]:
        """调用 API"""
        api_func = getattr(self.pro, api_name, None)
        if api_func is None:
            raise DataCollectionError("tushare", f"API {api_name} not found")

        def _call():
            self.rate_limiter.wait()
            df = api_func(**kwargs)
            if df is not None and not df.empty:
                return pl.from_pandas(df)
            return None

        try:
            result = self.retry_policy.execute(_call)
            if result is None:
                logger.warning(f"Empty result for {api_name}")
            return result
        except Exception as e:
            raise DataCollectionError("tushare", f"API call failed: {e}")


class SyncTaskExecutor(ISyncTaskExecutor):
    """同步任务执行器"""

    def __init__(
        self,
        api_client: TushareAPIClient,
        repository: IDataRepository,
        table_manager: TableManager,
        log_manager: SyncLogManager
    ):
        self.api_client = api_client
        self.repository = repository
        self.table_manager = table_manager
        self.log_manager = log_manager

    def execute_task(
        self,
        task_config: Dict[str, Any],
        target_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> bool:
        """执行同步任务"""
        task_id = task_config["task_id"]
        api_name = task_config["api_name"]
        sync_type = task_config["sync_type"]

        if not task_config.get("enabled", True):
            logger.info(f"Task {task_id} is disabled")
            return True

        logger.info(f"Starting sync task: {task_id}")

        try:
            self.table_manager.ensure_table_exists(task_config)

            if sync_type == "full":
                return self._execute_full_sync(task_config)
            elif sync_type == "incremental":
                return self._execute_incremental_sync(task_config, target_date, end_date)
            else:
                raise SyncConfigError(f"Unknown sync type: {sync_type}")

        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            return False

    def _execute_full_sync(self, task: Dict[str, Any]) -> bool:
        """执行全量同步"""
        task_id = task["task_id"]
        api_name = task["api_name"]
        params = self._format_params(task["params"])

        df = self.api_client.call_api(api_name, **params)
        if df is None or df.is_empty():
            logger.warning(f"No data for {task_id}")
            self.log_manager.update_sync_log(task_id, DateUtils.today(), 0)
            return False

        rows_count = len(df)
        self.repository.upsert(task["table_name"], df, task["primary_keys"])
        self.log_manager.update_sync_log(task_id, DateUtils.today(), rows_count)
        logger.info(f"Full sync completed for {task_id}: {rows_count} rows")
        return True

    def _execute_incremental_sync(
        self,
        task: Dict[str, Any],
        target_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> bool:
        """执行增量同步"""
        task_id = task["task_id"]
        api_name = task["api_name"]

        # 确定日期范围
        if target_date is None:
            # 当没有指定 target_date 时，只同步最新一天
            last_date = self.log_manager.get_last_sync_date(task_id)
            if last_date:
                # 如果有上次同步日期，从下一天开始同步到今天
                start_date = DateUtils.add_days(last_date, 1)
            else:
                # 如果没有上次同步日期，只同步今天（不同步历史数据）
                start_date = DateUtils.today()

            # 如果指定了 end_date，使用它；否则使用今天
            target_date = end_date if end_date else DateUtils.today()
        else:
            # 如果指定了 target_date，使用它作为开始日期
            start_date = target_date
            # 如果指定了 end_date，使用它；否则使用 target_date
            target_date = end_date if end_date else target_date

        if start_date > target_date:
            logger.info(f"Task {task_id} already up to date")
            return True

        # 按日期循环同步
        dates = DateUtils.get_date_range(start_date, target_date)
        total_rows = 0

        for date_str in dates:
            params = self._format_params(task["params"], date_str)
            df = self.api_client.call_api(api_name, **params)

            if df is not None and not df.is_empty():
                self.repository.upsert(task["table_name"], df, task["primary_keys"])
                rows_count = len(df)
                total_rows += rows_count
                logger.info(f"Synced {task_id} for {date_str}: {rows_count} rows")

                # 记录每次同步的历史
                self.log_manager.update_sync_log(task_id, date_str, rows_count)
            else:
                # 即使没有数据也记录
                self.log_manager.update_sync_log(task_id, date_str, 0)

        logger.info(f"Incremental sync completed for {task_id}: {total_rows} total rows")
        return True

    def _format_params(
        self,
        params: Dict[str, Any],
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """格式化参数"""
        formatted = {}
        for key, value in params.items():
            if isinstance(value, str) and "{date}" in value:
                formatted[key] = date if date else DateUtils.today()
            else:
                formatted[key] = value
        return formatted
