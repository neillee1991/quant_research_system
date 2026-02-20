import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import polars as pl
import tushare as ts

from app.core.config import settings
from app.core.logger import logger
from store.postgres_client import db_client


class ConfigBasedSyncEngine:
    """基于 JSON 配置的 Tushare 数据同步引擎"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = Path(__file__).parent / "sync_config.json"

        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        self.global_config = self.config.get("global_config", {})
        self.rate_limit = self.global_config.get("rate_limit", {})
        self.calls_per_minute = self.rate_limit.get("calls_per_minute", 120)
        self.retry_times = self.rate_limit.get("retry_times", 3)
        self.retry_delay = self.rate_limit.get("retry_delay", 2)

        # 初始化 Tushare
        if not settings.TUSHARE_TOKEN:
            raise ValueError("TUSHARE_TOKEN not configured")
        ts.set_token(settings.TUSHARE_TOKEN)
        self.pro = ts.pro_api()

        # 调用间隔（秒）
        self.call_interval = 60.0 / self.calls_per_minute
        self.last_call_time = 0

    def _rate_limit_wait(self):
        """速率限制等待"""
        elapsed = time.time() - self.last_call_time
        if elapsed < self.call_interval:
            time.sleep(self.call_interval - elapsed)
        self.last_call_time = time.time()

    def _call_api_with_retry(self, api_name: str, **kwargs) -> Optional[pl.DataFrame]:
        """带重试的 API 调用"""
        api_func = getattr(self.pro, api_name, None)
        if api_func is None:
            logger.error(f"API {api_name} not found")
            return None

        for attempt in range(self.retry_times):
            try:
                self._rate_limit_wait()
                df = api_func(**kwargs)
                if df is not None and not df.empty:
                    return pl.from_pandas(df)
                logger.warning(f"Empty result for {api_name} on attempt {attempt + 1}")
            except Exception as e:
                logger.warning(f"API {api_name} attempt {attempt + 1} failed: {e}")
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay ** attempt)
        return None

    def _ensure_table_exists(self, task: Dict[str, Any]):
        """确保目标表存在，根据 schema 定义创建完整表结构"""
        table_name = task["table_name"]
        primary_keys = task["primary_keys"]
        schema = task.get("schema", {})

        conn = db_client.connect()
        try:
            conn.execute(f"SELECT * FROM {table_name} LIMIT 1")
            logger.info(f"Table {table_name} already exists")

            # 检查是否有新增字段需要添加
            existing_cols = set(conn.execute(f"PRAGMA table_info({table_name})").fetchdf()["name"].tolist())
            for col_name, col_def in schema.items():
                if col_name not in existing_cols:
                    col_type = col_def.get("type", "VARCHAR")
                    nullable = "" if col_def.get("nullable", True) else "NOT NULL"
                    try:
                        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {nullable}")
                        logger.info(f"Added column {col_name} to {table_name}")
                    except Exception as e:
                        logger.warning(f"Failed to add column {col_name}: {e}")
        except:
            # 表不存在，根据 schema 创建完整表结构
            if schema:
                # 使用 schema 定义创建表
                columns = []
                for col_name, col_def in schema.items():
                    col_type = col_def.get("type", "VARCHAR")
                    nullable = "" if col_def.get("nullable", True) else "NOT NULL"
                    comment = col_def.get("comment", "")
                    columns.append(f"{col_name} {col_type} {nullable}")

                pk_clause = f"PRIMARY KEY ({', '.join(primary_keys)})"
                columns_str = ",\n                    ".join(columns)

                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {columns_str},
                        {pk_clause}
                    )
                """
                conn.execute(create_sql)
                logger.info(f"Created table {table_name} with {len(schema)} columns from schema")
            else:
                # 没有 schema 定义，创建基础结构
                pk_clause = f"PRIMARY KEY ({', '.join(primary_keys)})"
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        ts_code VARCHAR,
                        trade_date VARCHAR,
                        {pk_clause}
                    )
                """)
                logger.info(f"Created basic table {table_name} (no schema defined)")


    def _get_last_sync_date(self, task_id: str) -> Optional[str]:
        """获取任务最后同步日期"""
        return db_client.get_last_sync_date("tushare_config", task_id)

    def _update_sync_log(self, task_id: str, sync_date: str):
        """更新同步日志"""
        db_client.update_sync_log("tushare_config", task_id, sync_date)

    def _format_params(self, params: Dict[str, Any], date: str = None) -> Dict[str, Any]:
        """格式化参数，替换占位符"""
        formatted = {}
        for key, value in params.items():
            if isinstance(value, str) and "{date}" in value:
                formatted[key] = date if date else datetime.today().strftime("%Y%m%d")
            else:
                formatted[key] = value
        return formatted

    def sync_task(self, task: Dict[str, Any], target_date: str = None) -> bool:
        """执行单个同步任务"""
        task_id = task["task_id"]
        api_name = task["api_name"]
        sync_type = task["sync_type"]

        if not task.get("enabled", True):
            logger.info(f"Task {task_id} is disabled, skipping")
            return True

        logger.info(f"Starting sync task: {task_id} ({task['description']})")

        try:
            self._ensure_table_exists(task)

            if sync_type == "full":
                # 全量同步
                params = self._format_params(task["params"])
                df = self._call_api_with_retry(api_name, **params)
                if df is None or df.is_empty():
                    logger.warning(f"No data returned for {task_id}")
                    return False

                # 写入数据库（全量替换）
                self._upsert_data(task["table_name"], df, task["primary_keys"])
                self._update_sync_log(task_id, datetime.today().strftime("%Y%m%d"))
                logger.info(f"Full sync completed for {task_id}: {len(df)} rows")
                return True

            elif sync_type == "incremental":
                # 增量同步
                if target_date is None:
                    last_date = self._get_last_sync_date(task_id)
                    if last_date:
                        start_date = (datetime.strptime(last_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                    else:
                        start_date = "20100101"  # 默认起始日期
                    target_date = datetime.today().strftime("%Y%m%d")
                else:
                    start_date = target_date

                if start_date > target_date:
                    logger.info(f"Task {task_id} already up to date")
                    return True

                # 按日期循环同步
                current_date = datetime.strptime(start_date, "%Y%m%d")
                end_date = datetime.strptime(target_date, "%Y%m%d")
                total_rows = 0

                while current_date <= end_date:
                    date_str = current_date.strftime("%Y%m%d")
                    params = self._format_params(task["params"], date_str)

                    df = self._call_api_with_retry(api_name, **params)
                    if df is not None and not df.is_empty():
                        self._upsert_data(task["table_name"], df, task["primary_keys"])
                        total_rows += len(df)
                        logger.info(f"Synced {task_id} for {date_str}: {len(df)} rows")

                    self._update_sync_log(task_id, date_str)
                    current_date += timedelta(days=1)

                logger.info(f"Incremental sync completed for {task_id}: {total_rows} total rows")
                return True

        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            return False

    def _upsert_data(self, table_name: str, df: pl.DataFrame, primary_keys: List[str]):
        """插入或更新数据（表结构已在初始化时创建）"""
        conn = db_client.connect()

        # 检查是否有新列需要添加（防御性编程，正常情况下不应该发生）
        existing_cols = set(conn.execute(f"PRAGMA table_info({table_name})").fetchdf()["name"].tolist())
        for col in df.columns:
            if col not in existing_cols:
                # 推断类型
                dtype = df[col].dtype
                if dtype in [pl.Int64, pl.Int32, pl.Int16]:
                    sql_type = "BIGINT"
                elif dtype in [pl.Float64, pl.Float32]:
                    sql_type = "DOUBLE"
                else:
                    sql_type = "VARCHAR"

                try:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {sql_type}")
                    logger.warning(f"Dynamically added column {col} to {table_name} (not in schema)")
                except:
                    pass

        # 使用 INSERT OR REPLACE
        conn.register("_tmp_sync", df.to_arrow())
        cols = ", ".join(df.columns)
        conn.execute(f"""
            INSERT OR REPLACE INTO {table_name}
            SELECT {cols} FROM _tmp_sync
        """)
        conn.unregister("_tmp_sync")

    def sync_all_enabled_tasks(self, target_date: str = None):
        """同步所有启用的任务"""
        tasks = self.config.get("sync_tasks", [])
        enabled_tasks = [t for t in tasks if t.get("enabled", True)]

        logger.info(f"Starting sync for {len(enabled_tasks)} enabled tasks")
        results = {}

        for task in enabled_tasks:
            success = self.sync_task(task, target_date)
            results[task["task_id"]] = "success" if success else "failed"

        logger.info(f"Sync completed. Results: {results}")
        return results

    def sync_by_schedule(self, schedule: str, target_date: str = None):
        """按调度类型同步（daily/weekly/monthly）"""
        tasks = [t for t in self.config["sync_tasks"]
                 if t.get("schedule") == schedule and t.get("enabled", True)]

        logger.info(f"Syncing {len(tasks)} tasks with schedule={schedule}")
        results = {}

        for task in tasks:
            success = self.sync_task(task, target_date)
            results[task["task_id"]] = "success" if success else "failed"

        return results


# 全局实例
sync_engine = ConfigBasedSyncEngine()
