"""
数据同步核心组件
拆分原 ConfigBasedSyncEngine 的职责
"""
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
import json
import warnings
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
    """
    同步配置管理器 - 兼容层

    ⚠️ 已废弃：此类仅用于向后兼容
    新代码请直接使用 app.services.task_service.sync_service

    使用 psycopg2（同步）直接查询 PostgreSQL，避免 asyncpg 事件循环问题
    """

    def __init__(self, config_path: Optional[str] = None):
        warnings.warn(
            "SyncConfigManager is deprecated. Use app.services.task_service.sync_service instead.",
            DeprecationWarning,
            stacklevel=2
        )

    def _query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """使用 psycopg2 同步查询 PostgreSQL"""
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(
            host=settings.postgresql.postgres_host,
            port=settings.postgresql.postgres_port,
            dbname=settings.postgresql.postgres_db,
            user=settings.postgresql.postgres_user,
            password=settings.postgresql.postgres_password,
        )
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def _row_to_task(self, row: Dict[str, Any]):
        from app.models.base_task import SyncTaskConfig
        return SyncTaskConfig.from_row(row)

    def get_task(self, task_id: str) -> Dict[str, Any]:
        rows = self._query("SELECT * FROM sync_task_configs WHERE task_id = %s", (task_id,))
        if not rows:
            raise SyncTaskNotFoundError(task_id)
        return self._row_to_task(rows[0]).model_dump()

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        rows = self._query("SELECT * FROM sync_task_configs")
        return [self._row_to_task(r).model_dump() for r in rows]

    def get_enabled_tasks(self) -> List[Dict[str, Any]]:
        rows = self._query("SELECT * FROM sync_task_configs WHERE enabled = true")
        return [self._row_to_task(r).model_dump() for r in rows]

    def reload(self) -> None:
        pass  # 无缓存，无需重载

    def get_global_config(self) -> Dict[str, Any]:
        """
        获取全局配置

        Returns:
            全局配置字典
        """
        return {
            "rate_limit": {
                "calls_per_minute": settings.collector.calls_per_minute,
                "retry_times": settings.collector.retry_times,
                "retry_delay": settings.collector.retry_delay,
            }
        }


class SyncLogManager:
    """同步日志管理器 - 重构版本

    注意:
    - get_last_sync_date() 现在从目标表实时查询
    - update_sync_log() 已删除
    - get_last_sync_info() 保留但标记为 deprecated（供 API 过渡）
    """

    def __init__(self, repository: IDataRepository):
        self.repository = repository

    def get_last_sync_date(self, task_id: str) -> Optional[str]:
        """获取最后同步日期（从目标表实时计算）

        Args:
            task_id: 任务ID

        Returns:
            最后同步日期 (YYYYMMDD)，无数据时返回 None
        """
        try:
            # 1. 从 sync_task_config 获取 table_name 和 date_field
            task_config = self._get_task_config(task_id)
            if not task_config:
                logger.warning(f"Task config not found for {task_id}")
                return None

            table_name = task_config.get("table_name")
            date_field = task_config.get("date_field", "trade_date")

            if not table_name:
                logger.warning(f"Task {task_id} missing table_name")
                return None

            # 2. 检查表是否存在
            self.repository.register_meta_table(table_name)
            if not self.repository.table_exists(table_name):
                logger.debug(f"Table {table_name} does not exist yet")
                return None

            # 3. 查询 MAX(date_field)
            sql = f'SELECT MAX({date_field}) as max_date FROM {table_name}'
            result = self.repository.query(sql)

            if result.is_empty() or result["max_date"][0] is None:
                return None

            max_date_val = result["max_date"][0]

            # 4. 格式化日期为 YYYYMMDD
            return self._format_date(max_date_val)

        except Exception as e:
            logger.warning(f"Failed to get last sync date for {task_id}: {e}")
            return None

    def _get_task_config(self, task_id: str) -> Optional[Dict[str, Any]]:
        """从 sync_task_configs 获取任务配置（psycopg2 同步查询）"""
        try:
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(
                host=settings.postgresql.postgres_host,
                port=settings.postgresql.postgres_port,
                dbname=settings.postgresql.postgres_db,
                user=settings.postgresql.postgres_user,
                password=settings.postgresql.postgres_password,
            )
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM sync_task_configs WHERE task_id = %s", (task_id,))
                    row = cur.fetchone()
                    if row:
                        from app.models.base_task import SyncTaskConfig
                        return SyncTaskConfig.from_row(dict(row)).model_dump()
                    return None
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"Failed to get task config for {task_id}: {e}")
            return None

    def _format_date(self, date_val: Any) -> Optional[str]:
        """格式化日期值为 YYYYMMDD 字符串"""
        if date_val is None:
            return None
        if isinstance(date_val, str):
            # 处理 YYYY-MM-DD 或 YYYYMMDD 格式
            s = date_val.replace("-", "").replace(" ", "")[:8]
            if len(s) == 8 and s.isdigit():
                return s
            return date_val
        elif isinstance(date_val, int):
            # 处理整数格式 20260329
            s = str(date_val)
            if len(s) == 8:
                return s
            return None
        elif hasattr(date_val, "strftime"):
            # datetime 对象
            return date_val.strftime("%Y%m%d")
        else:
            s = str(date_val).replace("-", "").replace(" ", "")[:8]
            if len(s) == 8 and s.isdigit():
                return s
            return None

    def get_last_sync_info(self, task_id: str) -> Optional[dict]:
        """获取最后同步信息（已废弃，保留用于兼容）

        注意: 现在只从 task_runs 表查询，不再查询 sync_log
        """
        import warnings
        warnings.warn(
            "get_last_sync_info is deprecated. Use task_runs table instead.",
            DeprecationWarning,
            stacklevel=2
        )
        try:
            # 从 task_runs 表查询最近一次成功的记录
            sql = """
                SELECT finished_at as updated_at
                FROM task_runs
                WHERE task_type = 'sync' AND task_id = %s AND status = 'success'
                ORDER BY finished_at DESC
                LIMIT 1
            """
            result = self.repository.query(sql, params=(task_id,))
            if not result.is_empty():
                row = result.to_dicts()[0]
                updated_at_val = row.get("updated_at")
                updated_at_str = None
                if updated_at_val:
                    if hasattr(updated_at_val, "strftime"):
                        updated_at_str = updated_at_val.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        updated_at_str = str(updated_at_val)
                # last_date 仍从目标表实时获取
                last_date = self.get_last_sync_date(task_id)
                return {
                    "last_date": last_date,
                    "updated_at": updated_at_str
                }
        except Exception as e:
            logger.warning(f"Failed to get last sync info for {task_id}: {e}")
        return None


class TableManager:
    """表结构管理器"""

    def __init__(self, repository: IDataRepository):
        self.repository = repository

    def ensure_table_exists(self, task: Dict[str, Any]) -> None:
        """确保表存在"""
        import json

        table_name = task.get("table_name")
        if not table_name:
            logger.warning("Task config missing table_name, skipping table creation")
            return

        primary_keys = task.get("primary_keys", [])
        schema = task.get("schema", {})

        # 先注册到元数据表集合，确保 table_exists / _resolve_db_path 路由到正确的库
        self.repository.register_meta_table(table_name)

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

            for col_name, col_def in schema.items():
                if col_name not in existing_cols:
                    pg_type = col_def.get("type", "VARCHAR").upper()
                    ddb_type = type_map.get(pg_type, "STRING")
                    try:
                        self.repository.execute(
                            f'addColumn({table_name}, "{col_name}", {ddb_type})'
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
        调用方 ensure_table_exists() 已确认表不存在。
        """
        try:
            # 创建一个基础维度表（TSDB 引擎要求 primaryKey 最后一列为时间/整数类型）
            self.repository.execute(f"""
                t = table(1:0, `ts_code`trade_date`created_at, [SYMBOL, STRING, TIMESTAMP]);
                db = database("dfs://quant");
                createTable(dbHandle=db, table=t, tableName=`{table_name}, primaryKey=`ts_code`created_at)
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
        # 设置临时环境变量，避免写入 root 权限的 ~/tk.csv
        import tempfile
        import os
        temp_dir = tempfile.gettempdir()
        os.environ['TUSHARE_TOKEN_FILE'] = os.path.join(temp_dir, 'tushare_token.csv')

        try:
            ts.set_token(token)
        except PermissionError:
            # 如果写入 token 文件失败，忽略（token 已通过环境变量传递）
            logger.warning("无法写入 Tushare token 文件，使用环境变量中的 token")

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
    ) -> int:
        """执行同步任务，返回同步行数，-1 表示失败"""
        task_id = task_config["task_id"]
        api_name = task_config["api_name"]
        sync_type = task_config["sync_type"]

        if not task_config.get("enabled", True):
            logger.info(f"Task {task_id} is disabled")
            return 0

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
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return -1

    def _fetch_with_pagination(
        self,
        task_id: str,
        api_name: str,
        params: Dict[str, Any],
        api_limit: int,
    ) -> Optional[pl.DataFrame]:
        """
        带 offset 分页的 API 调用，自动循环直到取完全部数据。

        Returns:
            合并后的完整 DataFrame，无数据时返回 None
        """
        all_frames: list[pl.DataFrame] = []
        offset = 0
        page = 0

        while True:
            page += 1
            page_params = {**params, "limit": api_limit, "offset": offset}
            df = self.api_client.call_api(api_name, **page_params)

            if df is None or df.is_empty():
                break

            rows = len(df)
            all_frames.append(df)
            logger.info(
                f"[{task_id}] page {page}: fetched {rows} rows "
                f"(offset={offset}, limit={api_limit})"
            )

            if rows < api_limit:
                # 返回行数不足 limit，说明已是最后一页
                break
            offset += rows

        if not all_frames:
            return None

        result = pl.concat(all_frames)
        if page > 1:
            logger.info(
                f"[{task_id}] pagination done: {page} pages, "
                f"{len(result)} total rows"
            )
        return result

    def _execute_full_sync(self, task: Dict[str, Any]) -> int:
        """执行全量同步，返回同步行数，-1 表示失败"""
        task_id = task["task_id"]
        api_name = task["api_name"]
        api_limit = task.get("api_limit", 5000)
        table_name = task["table_name"]
        primary_keys = task.get("primary_keys", [])
        params = self._format_params(task["params"])

        try:
            df = self._fetch_with_pagination(task_id, api_name, params, api_limit)
            if df is None or df.is_empty():
                logger.warning(f"No data for {task_id}")
                return 0

            rows_count = len(df)
            # 列重命名（如 con_code → ts_code）
            col_mapping = task.get("column_mapping")
            if col_mapping:
                df = df.rename({k: v for k, v in col_mapping.items() if k in df.columns})
            # 全量同步：清空整个表再写入
            self.repository.upsert(table_name, df, primary_keys, is_full_sync=True)
            logger.info(f"Full sync completed for {task_id}: {rows_count} rows")
            return rows_count
        except Exception as e:
            logger.error(f"Full sync failed for {task_id}: {e}")
            return -1

    def _execute_incremental_sync(
        self,
        task: Dict[str, Any],
        target_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> int:
        """执行增量同步，返回同步行数，-1 表示失败"""
        task_id = task["task_id"]
        api_name = task["api_name"]
        api_limit = task.get("api_limit", 5000)

        logger.info(f"Starting incremental sync for {task_id}: target_date={target_date}, end_date={end_date}")

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

        # 确保日期都是字符串格式（防止 datetime 对象比较错误）
        from datetime import datetime
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y%m%d")
        if isinstance(target_date, datetime):
            target_date = target_date.strftime("%Y%m%d")

        logger.info(f"[{task_id}] Date range: {start_date} to {target_date}")

        if start_date > target_date:
            logger.info(f"Task {task_id} already up to date")
            return 0

        # 按日期循环同步（优先使用交易日历过滤非交易日）
        from app.core.utils import TradingCalendar
        cal = TradingCalendar.get_instance(self.repository)
        if cal.is_loaded:
            dates = cal.get_trading_days(start_date, target_date)
        else:
            dates = DateUtils.get_date_range(start_date, target_date)

        logger.info(f"[{task_id}] Trading days to sync: {len(dates)}")
        total_rows = 0

        for date_str in dates:
            params_str = f"type=incremental, date={date_str}, range={start_date}~{target_date}"
            try:
                logger.info(f"[{task_id}] Syncing date: {date_str}")
                params = self._format_params(task["params"], date_str)
                df = self._fetch_with_pagination(task_id, api_name, params, api_limit)

                if df is not None and not df.is_empty():
                    # 列重命名（如 con_code → ts_code）
                    col_mapping = task.get("column_mapping")
                    if col_mapping:
                        df = df.rename({k: v for k, v in col_mapping.items() if k in df.columns})
                    # 增量同步：只清空当前 trade_date 的数据再写入
                    self.repository.upsert(
                        task["table_name"],
                        df,
                        task["primary_keys"],
                        is_full_sync=False,
                        trade_date=date_str
                    )
                    rows_count = len(df)
                    total_rows += rows_count
                    logger.info(f"Synced {task_id} for {date_str}: {rows_count} rows")

                else:
                    logger.info(f"[{task_id}] No data for date {date_str}")

            except Exception as e:
                logger.error(f"Sync {task_id} for {date_str} failed: {e}", exc_info=True)
                return -1

        logger.info(f"Incremental sync completed for {task_id}: {total_rows} total rows")
        return total_rows

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
