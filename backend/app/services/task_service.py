"""
通用任务服务层
提供统一的 CRUD 操作，使用 PostgreSQL（asyncpg）
"""
import json
from datetime import datetime
from typing import TypeVar, Generic, Type, List, Optional, Dict, Any

from app.models.base_task import BaseTaskConfig, SyncTaskConfig, ETLTaskConfig, FactorConfig
from app.core.logger import logger
from app.validators.schema_validator import SchemaValidator
from app.validators.shared_table_validator import shared_table_validator

T = TypeVar('T', bound=BaseTaskConfig)


class TaskService(Generic[T]):
    """通用任务服务 - 提供统一的 CRUD 接口（PostgreSQL）"""

    def __init__(
        self,
        task_type: str,
        table_name: str,
        id_field: str,
        model_class: Type[T]
    ):
        self.task_type = task_type
        self.table_name = table_name
        self.id_field = id_field
        self.model_class = model_class

    async def list_tasks(self, enabled_only: bool = False) -> List[T]:
        from scheduler.db import DatabasePool
        sql = f"SELECT * FROM {self.table_name}"
        if enabled_only:
            sql += " WHERE enabled = true"
        rows = await DatabasePool.fetch(sql)
        tasks = []
        for row in rows:
            try:
                tasks.append(self.model_class(**dict(row)))
            except Exception as e:
                logger.warning(f"Failed to parse task {row.get(self.id_field)}: {e}")
        return tasks

    async def get_task(self, task_id: str) -> Optional[T]:
        from scheduler.db import DatabasePool
        row = await DatabasePool.fetchrow(
            f"SELECT * FROM {self.table_name} WHERE {self.id_field} = $1",
            task_id,
        )
        if not row:
            return None
        return self.model_class(**dict(row))

    async def create_task(
        self,
        config_data: Dict[str, Any],
    ) -> T:
        task = self.model_class(**config_data)
        task_id = getattr(task, self.id_field)

        existing = await self.get_task(task_id)
        if existing:
            # 如果任务已存在但被软删除了，恢复它
            if not getattr(existing, "enabled", True):
                from scheduler.db import DatabasePool
                await DatabasePool.execute(
                    f"UPDATE {self.table_name} SET enabled = true WHERE {self.id_field} = $1",
                    task_id,
                )
                logger.info(f"Restored soft-deleted {self.task_type} task {task_id}")
                return await self.get_task(task_id)
            else:
                raise ValueError(f"Task {task_id} already exists")

        await self._validate_schema(config_data, task_id)

        config_dict = task.model_dump(exclude_none=True)
        await self._upsert(config_dict)
        logger.info(f"Created {self.task_type} task {task_id}")
        return await self.get_task(task_id)

    async def update_task(
        self,
        task_id: str,
        config_data: Dict[str, Any],
    ) -> T:
        existing = await self.get_task(task_id)
        if not existing:
            raise ValueError(f"Task {task_id} not found")

        current_dict = existing.model_dump(exclude_none=True)
        current_dict.update(config_data)
        current_dict[self.id_field] = task_id

        await self._validate_schema_evolution(config_data, existing, current_dict, task_id)

        updated_task = self.model_class(**current_dict)
        await self._upsert(updated_task.model_dump(exclude_none=True))
        logger.info(f"Updated {self.task_type} task {task_id}")
        return await self.get_task(task_id)

    async def delete_task(
        self,
        task_id: str,
        drop_table: bool = False,
        hard_delete: bool = False
    ) -> bool:
        from scheduler.db import DatabasePool
        existing = await self.get_task(task_id)
        if not existing:
            raise ValueError(f"Task {task_id} not found")

        if drop_table and self.task_type in ["sync", "etl"]:
            table_name = getattr(existing, "table_name", None)
            if table_name:
                is_shared = shared_table_validator.check_shared_table(
                    table_name=table_name,
                    exclude_task_id=task_id,
                    config_table=self.table_name
                )
                if is_shared:
                    sharing_tasks = shared_table_validator.get_sharing_tasks(
                        table_name=table_name, exclude_task_id=task_id
                    )
                    raise ValueError(
                        f"Cannot drop table '{table_name}' - shared by: {sharing_tasks}"
                    )
                try:
                    from infrastructure.database.dolphindb_client import db_client
                    if db_client.table_exists(table_name):
                        db_client.drop_table(table_name)
                        logger.info(f"Dropped table {table_name} for task {task_id}")
                except Exception as e:
                    raise ValueError(f"Failed to drop table {table_name}: {e}")

        if hard_delete:
            await DatabasePool.execute(
                f"DELETE FROM {self.table_name} WHERE {self.id_field} = $1", task_id
            )
            logger.info(f"Deleted (hard) {self.task_type} task {task_id}")
        else:
            await DatabasePool.execute(
                f"UPDATE {self.table_name} SET enabled = false WHERE {self.id_field} = $1",
                task_id,
            )
            logger.info(f"Deleted (soft) {self.task_type} task {task_id}")
        return True

    async def get_schema(self, task_id: str) -> Dict[str, Any]:
        """获取任务的表结构定义"""
        task = await self.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")

        task_data = task.model_dump()
        schema_json = task_data.get('schema_json')
        if not schema_json or not isinstance(schema_json, str):
            return {
                "status": "success",
                "data": {
                    "task_id": task_id,
                    "table_name": task_data.get('table_name'),
                    "columns": [],
                    "message": "No schema defined yet"
                }
            }

        try:
            schema = json.loads(schema_json)
            columns = schema.get("columns", []) if isinstance(schema, dict) else []
            return {
                "status": "success",
                "data": {
                    "task_id": task_id,
                    "table_name": task_data.get('table_name'),
                    "columns": columns,
                    "primary_keys": self._parse_primary_keys(task_data)
                }
            }
        except Exception as e:
            logger.error(f"Failed to parse schema for task {task_id}: {e}")
            raise ValueError(f"Failed to parse schema: {e}")

    async def test_script(self, script: str, date: Optional[str] = None) -> Dict[str, Any]:
        """测试 ETL 脚本（仅 ETL 任务）"""
        if self.task_type != "etl":
            raise ValueError("test_script is only available for ETL tasks")

        from infrastructure.database.dolphindb_client import db_client

        if not script or not script.strip():
            raise ValueError("Script cannot be empty")

        try:
            # 替换日期占位符，生成 DolphinDB 原生日期字面量（如 2026.04.09）
            raw_date = date or datetime.now().strftime("%Y%m%d")
            ddb_date = f"{raw_date[:4]}.{raw_date[4:6]}.{raw_date[6:8]}"
            from app.core.config import settings as _settings
            db_path = _settings.database.db_path
            resolved_script = (script
                               .replace("{date}", ddb_date)
                               .replace("{db_ts}", db_path)
                               .replace("{db_meta}", db_path))
            result = db_client.query(resolved_script)
            columns = []
            if not result.is_empty():
                for col_name in result.columns:
                    col_type = str(result[col_name].dtype)
                    columns.append({"name": col_name, "type": col_type})

            return {
                "status": "success",
                "data": {
                    "columns": columns,
                    "row_count": len(result) if not result.is_empty() else 0,
                    "sample_data": result.head(5).to_dicts() if not result.is_empty() else []
                }
            }
        except Exception as e:
            logger.error(f"Failed to test ETL script: {e}")
            raise ValueError(f"Script execution failed: {e}")

    async def backfill(self, task_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """回溯执行任务（仅 Sync/ETL 任务）"""
        if self.task_type not in ("sync", "etl"):
            raise ValueError("backfill is only available for sync/etl tasks")

        task = await self.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")

        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required")

        try:
            logger.info(f"Backfill {self.task_type} task {task_id} from {start_date} to {end_date}")
            return {
                "status": "success",
                "message": f"Backfill job submitted for {task_id}",
                "task_id": task_id,
                "start_date": start_date,
                "end_date": end_date
            }
        except Exception as e:
            logger.error(f"Failed to backfill task {task_id}: {e}")
            raise ValueError(f"Backfill failed: {e}")

    async def create_table(self, task_id: str, payload: Dict[str, Any] = None) -> Dict[str, Any]:
        """在 DolphinDB 中创建表"""
        from infrastructure.database.dolphindb_client import db_client

        task = await self.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")

        task_data = task.model_dump()
        table_name = task_data.get('table_name')
        if not table_name:
            raise ValueError(f"Task {task_id} does not have a table_name")

        schema_json = task_data.get('schema_json')
        if not schema_json or not isinstance(schema_json, str):
            raise ValueError(f"Task {task_id} does not have a schema defined")

        try:
            schema = json.loads(schema_json)
            primary_keys = self._parse_primary_keys(task_data)

            if db_client.table_exists(table_name):
                return {
                    "status": "success",
                    "message": f"Table {table_name} already exists",
                    "table_name": table_name
                }

            db_client.create_table(table_name, schema, primary_keys)
            logger.info(f"Created table {table_name} for task {task_id}")
            return {
                "status": "success",
                "message": f"Table {table_name} created successfully",
                "table_name": table_name
            }
        except Exception as e:
            logger.error(f"Failed to create table for task {task_id}: {e}")
            raise ValueError(f"Failed to create table: {e}")

    async def inspect_data(self, task_id: str) -> Dict[str, Any]:
        """数据探查：查询 DolphinDB 时序表（保持同步，不迁移）"""
        from infrastructure.database.dolphindb_client import db_client

        task = await self.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")

        table_name = getattr(task, 'table_name', None)
        if not table_name:
            if hasattr(task, 'factor_id'):
                table_name = 'factor_values'
            else:
                raise ValueError(f"Task {task_id} does not have a table_name")

        if not db_client.table_exists(table_name):
            return {"table_name": table_name, "exists": False,
                    "message": f"Table {table_name} does not exist yet"}

        date_field = getattr(task, 'date_field', None) or 'trade_date'
        where_clause = ""
        if hasattr(task, 'factor_id'):
            where_clause = f"WHERE factor_id = '{task.factor_id}'"

        try:
            count_sql = f"SELECT count(*) as total FROM loadTable('dfs://quant', '{table_name}') {where_clause} limit 1"
            count_result = db_client.query(count_sql)
            if count_result.is_empty() or count_result['total'][0] == 0:
                return {"table_name": table_name, "exists": True, "has_data": False,
                        "message": f"Table {table_name} exists but has no data"}

            result = db_client.query(f"""
                SELECT min({date_field}) as min_date, max({date_field}) as max_date
                FROM loadTable("dfs://quant", "{table_name}") {where_clause}
            """)
            if result.is_empty() or result['min_date'][0] is None:
                return {"table_name": table_name, "exists": True, "has_data": False,
                        "message": "No valid date data"}

            min_date, max_date = result['min_date'][0], result['max_date'][0]

            def to_date_str(d):
                if isinstance(d, (int, str)):
                    return str(d).replace('-', '')[:8]
                if hasattr(d, 'strftime'):
                    return d.strftime('%Y%m%d')
                return str(d).replace('-', '')[:8]

            min_date_str, max_date_str = to_date_str(min_date), to_date_str(max_date)
            min_int, max_int = int(min_date_str), int(max_date_str)

            actual_dates_result = db_client.query(f"""
                SELECT DISTINCT {date_field} FROM loadTable("dfs://quant", "{table_name}")
                {where_clause} ORDER BY {date_field}
            """)
            actual_dates = set()
            if not actual_dates_result.is_empty():
                for d in actual_dates_result[actual_dates_result.columns[0]].to_list():
                    di = int(to_date_str(d))
                    if min_int <= di <= max_int:
                        actual_dates.add(di)

            try:
                cal_result = db_client.query("""
                    SELECT cal_date FROM loadTable("dfs://quant", "sync_trade_cal")
                    WHERE exchange = 'SSE' AND is_open = 1 ORDER BY cal_date
                """)
                trading_days = set()
                for d in cal_result['cal_date'].to_list():
                    di = int(to_date_str(d))
                    if min_int <= di <= max_int:
                        trading_days.add(di)
                missing = sorted(trading_days - actual_dates)
                coverage = len(actual_dates) / len(trading_days) * 100 if trading_days else 0
                return {
                    "table_name": table_name, "exists": True, "has_data": True,
                    "date_field": date_field, "min_date": min_date_str, "max_date": max_date_str,
                    "actual_dates": len(actual_dates), "expected_dates": len(trading_days),
                    "missing_dates": [str(d) for d in missing], "missing_count": len(missing),
                    "coverage_percent": round(coverage, 2), "trading_calendar_available": True,
                }
            except Exception as e:
                logger.warning(f"Trading calendar unavailable: {e}")
                return {
                    "table_name": table_name, "exists": True, "has_data": True,
                    "date_field": date_field, "min_date": min_date_str, "max_date": max_date_str,
                    "actual_dates": len(actual_dates), "trading_calendar_available": False,
                    "message": "Trading calendar not available",
                }
        except Exception as e:
            logger.error(f"Failed to inspect data for task {task_id}: {e}")
            raise ValueError(f"Failed to inspect data: {e}")

    # ── private helpers ──────────────────────────────────────────────────────

    async def _upsert(self, config_dict: Dict[str, Any]) -> None:
        """Generic upsert via INSERT ... ON CONFLICT DO UPDATE."""
        from scheduler.db import DatabasePool
        cols = list(config_dict.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
        updates = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in cols if c != self.id_field
        )
        sql = (
            f"INSERT INTO {self.table_name} ({', '.join(cols)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({self.id_field}) DO UPDATE SET {updates}"
        )
        await DatabasePool.execute(sql, *[config_dict[c] for c in cols])

    async def _validate_schema(self, config_data: Dict[str, Any], task_id: str) -> None:
        if self.task_type not in ("sync", "etl"):
            return
        schema_json = config_data.get("schema_json")
        if not schema_json:
            return
        primary_keys = self._parse_primary_keys(config_data)
        schema = json.loads(schema_json) if isinstance(schema_json, str) else schema_json
        is_valid, errors = SchemaValidator.validate_schema(schema, primary_keys)
        if not is_valid:
            raise ValueError(f"Schema validation failed: {'; '.join(errors)}")
        table_name = config_data.get("table_name")
        if table_name:
            result = shared_table_validator.validate_shared_schema(
                table_name=table_name, schema=schema,
                primary_keys=primary_keys, exclude_task_id=task_id
            )
            if not result["valid"]:
                raise ValueError(
                    f"Shared table schema conflict on '{table_name}': "
                    f"{'; '.join(result['conflicts'])}"
                )

    async def _validate_schema_evolution(
        self, config_data: Dict[str, Any], existing: T,
        current_dict: Dict[str, Any], task_id: str
    ) -> None:
        if self.task_type not in ("sync", "etl"):
            return
        new_schema_json = config_data.get("schema_json")
        if not new_schema_json:
            return
        new_schema = json.loads(new_schema_json) if isinstance(new_schema_json, str) else new_schema_json
        old_schema_json = existing.model_dump().get("schema_json")
        if old_schema_json and isinstance(old_schema_json, str):
            old_schema = json.loads(old_schema_json)
            if old_schema:  # 旧 schema 为空时跳过 evolution 检查
                primary_keys = self._parse_primary_keys(current_dict)
                is_valid, errors = SchemaValidator.validate_schema_evolution(
                    old_schema=old_schema, new_schema=new_schema, primary_keys=primary_keys
                )
                if not is_valid:
                    raise ValueError(f"Schema evolution failed: {'; '.join(errors)}")
        table_name = current_dict.get("table_name")
        if table_name:
            result = shared_table_validator.validate_shared_schema(
                table_name=table_name, schema=new_schema,
                primary_keys=self._parse_primary_keys(current_dict), exclude_task_id=task_id
            )
            if not result["valid"]:
                raise ValueError(
                    f"Shared table schema conflict on '{table_name}': "
                    f"{'; '.join(result['conflicts'])}"
                )

    @staticmethod
    def _parse_primary_keys(data: Dict[str, Any]) -> list:
        raw = data.get("primary_keys_json") or data.get("primary_keys", [])
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return []
        return raw or []


# Service instances — table names updated to plural form
sync_service = TaskService[SyncTaskConfig](
    task_type="sync",
    table_name="sync_task_configs",
    id_field="task_id",
    model_class=SyncTaskConfig,
)

etl_service = TaskService[ETLTaskConfig](
    task_type="etl",
    table_name="etl_task_configs",
    id_field="task_id",
    model_class=ETLTaskConfig,
)

factor_service = TaskService[FactorConfig](
    task_type="factor",
    table_name="factor_configs",
    id_field="factor_id",
    model_class=FactorConfig,
)
