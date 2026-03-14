"""
通用任务服务层
提供统一的 CRUD 操作和版本控制
"""
import json
from typing import TypeVar, Generic, Type, List, Optional, Dict, Any

from app.models.base_task import BaseTaskConfig, SyncTaskConfig, ETLTaskConfig, FactorConfig
from store.dolphindb_client import db_client
from app.core.logger import logger
from app.validators.schema_validator import SchemaValidator
from app.validators.shared_table_validator import shared_table_validator

T = TypeVar('T', bound=BaseTaskConfig)


class TaskService(Generic[T]):
    """通用任务服务 - 提供统一的 CRUD 接口"""

    def __init__(
        self,
        task_type: str,
        table_name: str,
        id_field: str,
        model_class: Type[T]
    ):
        """
        初始化任务服务

        Args:
            task_type: 任务类型 (sync/etl/factor)
            table_name: 数据库表名
            id_field: 主键字段名 (task_id 或 factor_id)
            model_class: Pydantic 模型类
        """
        self.task_type = task_type
        self.table_name = table_name
        self.id_field = id_field
        self.model_class = model_class

    def list_tasks(self, enabled_only: bool = False) -> List[T]:
        """
        列出所有任务

        Args:
            enabled_only: 是否只返回启用的任务

        Returns:
            任务列表
        """
        sql = f"SELECT * FROM {self.table_name}"
        if enabled_only:
            sql += " WHERE enabled = true"

        df = db_client.query(sql)
        if df.is_empty():
            return []

        tasks = []
        for row in df.to_dicts():
            try:
                tasks.append(self.model_class(**row))
            except Exception as e:
                logger.warning(f"Failed to parse task {row.get(self.id_field)}: {e}")
                continue

        return tasks

    def get_task(self, task_id: str) -> Optional[T]:
        """
        获取单个任务（当前版本）

        Args:
            task_id: 任务ID

        Returns:
            任务配置，不存在返回 None
        """
        sql = f"SELECT * FROM {self.table_name} WHERE {self.id_field} = %s"
        df = db_client.query(sql, params=(task_id,))

        if df.is_empty():
            return None

        row = df.to_dicts()[0]
        return self.model_class(**row)

    def create_task(
        self,
        config_data: Dict[str, Any],
        changed_by: str = "api",
        change_reason: str = "Create new task"
    ) -> T:
        """
        创建新任务

        Args:
            config_data: 任务配置数据
            changed_by: 修改人
            change_reason: 修改原因

        Returns:
            创建的任务配置
        """
        # 验证数据
        task = self.model_class(**config_data)
        task_id = getattr(task, self.id_field)

        # 检查是否已存在
        existing = self.get_task(task_id)
        if existing:
            raise ValueError(f"Task {task_id} already exists")

        # Schema 验证（仅对 sync 和 etl 任务）
        if self.task_type in ["sync", "etl"]:
            schema_json = config_data.get("schema_json")
            primary_keys = config_data.get("primary_keys", [])
            table_name = config_data.get("table_name")

            if schema_json:
                # 解析 schema_json
                try:
                    if isinstance(schema_json, str):
                        schema = json.loads(schema_json)
                    else:
                        schema = schema_json
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid schema_json format: {e}")

                # 验证 schema 格式
                is_valid, errors = SchemaValidator.validate_schema(schema, primary_keys)
                if not is_valid:
                    raise ValueError(f"Schema validation failed: {'; '.join(errors)}")

                # 如果是共享表，验证 schema 一致性
                if table_name:
                    validation_result = shared_table_validator.validate_shared_schema(
                        table_name=table_name,
                        schema=schema,
                        primary_keys=primary_keys,
                        exclude_task_id=task_id
                    )

                    if not validation_result["valid"]:
                        conflicts = validation_result["conflicts"]
                        sharing_tasks = validation_result["sharing_tasks"]
                        raise ValueError(
                            f"Shared table schema conflict detected. "
                            f"Table '{table_name}' is used by tasks: {sharing_tasks}. "
                            f"Conflicts: {'; '.join(conflicts)}"
                        )

        # 插入任务配置
        config_dict = task.model_dump(exclude_none=True)

        # 使用 upsert 插入数据
        db_client.upsert(self.table_name, config_dict)

        logger.info(f"Created {self.task_type} task {task_id}")

        # 返回创建的任务
        return self.get_task(task_id)

    def update_task(
        self,
        task_id: str,
        config_data: Dict[str, Any],
        changed_by: str = "api",
        change_reason: str = "Update task"
    ) -> T:
        """
        更新任务（创建新版本）

        Args:
            task_id: 任务ID
            config_data: 更新的配置数据
            changed_by: 修改人
            change_reason: 修改原因

        Returns:
            更新后的任务配置
        """
        # 检查任务是否存在
        existing = self.get_task(task_id)
        if not existing:
            raise ValueError(f"Task {task_id} not found")

        # 合并现有配置和更新数据
        current_dict = existing.model_dump(exclude_none=True)
        current_dict.update(config_data)

        # 确保 ID 字段不变
        current_dict[self.id_field] = task_id

        # Schema 演化验证（仅对 sync 和 etl 任务）
        if self.task_type in ["sync", "etl"]:
            new_schema_json = config_data.get("schema_json")
            new_primary_keys = config_data.get("primary_keys")
            table_name = current_dict.get("table_name")

            # 如果更新了 schema_json，进行演化验证
            if new_schema_json:
                try:
                    if isinstance(new_schema_json, str):
                        new_schema = json.loads(new_schema_json)
                    else:
                        new_schema = new_schema_json
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid schema_json format: {e}")

                # 获取旧 schema
                old_schema_json = getattr(existing, "schema_json", None)
                if old_schema_json:
                    try:
                        if isinstance(old_schema_json, str):
                            old_schema = json.loads(old_schema_json)
                        else:
                            old_schema = old_schema_json
                    except json.JSONDecodeError:
                        old_schema = {}

                    # 验证 schema 演化（只允许新增字段）
                    primary_keys = new_primary_keys if new_primary_keys else current_dict.get("primary_keys", [])
                    is_valid, errors = SchemaValidator.validate_schema_evolution(
                        old_schema=old_schema,
                        new_schema=new_schema,
                        primary_keys=primary_keys
                    )

                    if not is_valid:
                        raise ValueError(
                            f"Schema evolution validation failed: {'; '.join(errors)}. "
                            f"Only adding new fields is allowed."
                        )
                else:
                    # 如果旧 schema 不存在，只验证新 schema 格式
                    primary_keys = new_primary_keys if new_primary_keys else current_dict.get("primary_keys", [])
                    is_valid, errors = SchemaValidator.validate_schema(new_schema, primary_keys)
                    if not is_valid:
                        raise ValueError(f"Schema validation failed: {'; '.join(errors)}")

                # 如果是共享表，验证 schema 一致性
                if table_name:
                    validation_result = shared_table_validator.validate_shared_schema(
                        table_name=table_name,
                        schema=new_schema,
                        primary_keys=new_primary_keys if new_primary_keys else current_dict.get("primary_keys", []),
                        exclude_task_id=task_id
                    )

                    if not validation_result["valid"]:
                        conflicts = validation_result["conflicts"]
                        sharing_tasks = validation_result["sharing_tasks"]
                        raise ValueError(
                            f"Shared table schema conflict detected. "
                            f"Table '{table_name}' is used by tasks: {sharing_tasks}. "
                            f"Conflicts: {'; '.join(conflicts)}"
                        )

        # 验证更新后的数据
        updated_task = self.model_class(**current_dict)

        # 更新任务配置
        config_dict = updated_task.model_dump(exclude_none=True)

        # 使用 upsert 更新数据
        db_client.upsert(self.table_name, config_dict)

        logger.info(f"Updated {self.task_type} task {task_id}")

        # 返回更新后的任务
        return self.get_task(task_id)

    def delete_task(
        self,
        task_id: str,
        changed_by: str = "api",
        change_reason: str = "Delete task",
        drop_table: bool = False
    ) -> bool:
        """
        删除任务（软删除，设置 enabled=false）

        Args:
            task_id: 任务ID
            changed_by: 修改人
            change_reason: 修改原因
            drop_table: 是否同时删除物理表（危险操作，默认 False）

        Returns:
            是否成功删除
        """
        # 检查任务是否存在
        existing = self.get_task(task_id)
        if not existing:
            raise ValueError(f"Task {task_id} not found")

        # 如果要删除物理表，进行共享表检查
        if drop_table and self.task_type in ["sync", "etl"]:
            table_name = getattr(existing, "table_name", None)
            if table_name:
                # 检查是否为共享表
                is_shared = shared_table_validator.check_shared_table(
                    table_name=table_name,
                    exclude_task_id=task_id,
                    config_table=self.table_name
                )

                if is_shared:
                    sharing_tasks = shared_table_validator.get_sharing_tasks(
                        table_name=table_name,
                        exclude_task_id=task_id
                    )
                    raise ValueError(
                        f"Cannot drop table '{table_name}' - it is shared by other tasks: {sharing_tasks}. "
                        f"Please delete those tasks first or use soft delete (drop_table=False)."
                    )

                # 如果不是共享表，删除物理表
                try:
                    if db_client.table_exists(table_name):
                        db_client.drop_table(table_name)
                        logger.info(f"Dropped table {table_name} for task {task_id}")
                except Exception as e:
                    logger.error(f"Failed to drop table {table_name}: {e}")
                    raise ValueError(f"Failed to drop table {table_name}: {e}")

        # 软删除：设置 enabled=false
        config_dict = existing.model_dump(exclude_none=True)
        config_dict["enabled"] = False

        # 使用 upsert 更新数据
        db_client.upsert(self.table_name, config_dict)

        logger.info(f"Deleted (soft) {self.task_type} task {task_id}")
        return True

    def inspect_data(self, task_id: str) -> Dict[str, Any]:
        """
        数据探查：检查表中的数据完整性

        Args:
            task_id: 任务 ID

        Returns:
            包含数据统计和缺失交易日的字典
        """
        # 获取任务配置
        task = self.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")

        # 获取表名
        # 对于因子任务，使用 factor_values 表
        table_name = getattr(task, 'table_name', None)
        if not table_name:
            # 如果没有 table_name，可能是因子任务
            if hasattr(task, 'factor_id'):
                table_name = 'factor_values'
            else:
                raise ValueError(f"Task {task_id} does not have a table_name")

        # 检查表是否存在
        if not db_client.table_exists(table_name):
            return {
                "table_name": table_name,
                "exists": False,
                "message": f"Table {table_name} does not exist yet"
            }

        # 获取日期字段名（优先使用 date_field，否则使用 trade_date）
        date_field = getattr(task, 'date_field', None)
        if not date_field or date_field == '':
            date_field = 'trade_date'

        logger.info(f"Task: {task_id}, date_field from task: {getattr(task, 'date_field', 'NOT_FOUND')}, using: {date_field}")

        # 构建 WHERE 子句（用于因子任务）
        where_clause = ""
        if hasattr(task, 'factor_id'):
            where_clause = f"WHERE factor_id = '{task.factor_id}'"

        # 查询表中的日期范围
        try:
            # 先检查表是否有数据
            count_sql = f"SELECT count(*) as total FROM loadTable('dfs://quant', '{table_name}') {where_clause} limit 1"
            count_result = db_client.query(count_sql)

            if count_result.is_empty() or count_result['total'][0] == 0:
                return {
                    "table_name": table_name,
                    "exists": True,
                    "has_data": False,
                    "message": f"Table {table_name} exists but has no data"
                }

            # 查询最小和最大日期
            date_range_sql = f"""
                SELECT
                    min({date_field}) as min_date,
                    max({date_field}) as max_date
                FROM loadTable("dfs://quant", "{table_name}")
                {where_clause}
            """
            result = db_client.query(date_range_sql)

            if result.is_empty() or result['min_date'][0] is None:
                return {
                    "table_name": table_name,
                    "exists": True,
                    "has_data": False,
                    "message": f"Table {table_name} exists but has no valid date data"
                }

            min_date = result['min_date'][0]
            max_date = result['max_date'][0]

            # 转换日期为字符串格式
            # DolphinDB 日期字段可能是 DATE 类型（整数 YYYYMMDD）或 DATETIME 类型
            if isinstance(min_date, (int, str)):
                # 如果是整数或字符串，直接使用
                min_date_str = str(min_date).replace('-', '').replace(' ', '').replace(':', '')[:8]
                max_date_str = str(max_date).replace('-', '').replace(' ', '').replace(':', '')[:8]
            elif hasattr(min_date, 'strftime'):
                # 如果是 datetime 对象
                min_date_str = min_date.strftime('%Y%m%d')
                max_date_str = max_date.strftime('%Y%m%d')
            else:
                # 其他情况，尝试转换为字符串
                min_date_str = str(min_date).replace('-', '')[:8]
                max_date_str = str(max_date).replace('-', '')[:8]

            logger.info(f"Date range: {min_date} to {max_date}, converted to {min_date_str} - {max_date_str}")

            # 获取表中实际存在的不同日期
            # 注意：DolphinDB 的 DATE 类型需要使用 date() 函数或者不加引号的整数
            actual_dates_sql = f"""
                SELECT DISTINCT {date_field}
                FROM loadTable("dfs://quant", "{table_name}")
                {where_clause}
                ORDER BY {date_field}
            """
            actual_dates_result = db_client.query(actual_dates_sql)
            logger.info(f"Actual dates query columns: {actual_dates_result.columns}")
            logger.info(f"Actual dates result shape: {actual_dates_result.shape}")

            # 使用第一列（无论列名是什么）
            if actual_dates_result.is_empty():
                actual_dates = set()
            else:
                all_dates = actual_dates_result[actual_dates_result.columns[0]].to_list()
                logger.info(f"Sample dates from DB: {all_dates[:5]}, types: {[type(d) for d in all_dates[:5]]}")

                # 过滤到指定日期范围内
                min_date_int = int(min_date_str)
                max_date_int = int(max_date_str)

                # 转换日期为整数进行比较
                actual_dates = set()
                for d in all_dates:
                    if isinstance(d, int):
                        date_int = d
                    elif isinstance(d, str):
                        date_int = int(d.replace('-', '').replace(' ', '').replace(':', '')[:8])
                    elif hasattr(d, 'strftime'):
                        date_int = int(d.strftime('%Y%m%d'))
                    else:
                        continue

                    if min_date_int <= date_int <= max_date_int:
                        actual_dates.add(date_int)

            date_count = len(actual_dates)
            logger.info(f"Filtered actual_dates count: {date_count}")

            # 获取交易日历（SSE 上交所）
            trading_days_sql = f"""
                SELECT cal_date
                FROM loadTable("dfs://quant", "sync_trade_cal")
                WHERE exchange = 'SSE'
                  AND is_open = 1
                ORDER BY cal_date
            """

            try:
                trading_days_result = db_client.query(trading_days_sql)
                all_trading_days = trading_days_result['cal_date'].to_list()
                logger.info(f"Sample trading days: {all_trading_days[:5]}, types: {[type(d) for d in all_trading_days[:5]]}")

                # 过滤到指定日期范围内并转换为整数
                min_date_int = int(min_date_str)
                max_date_int = int(max_date_str)

                trading_days = set()
                for d in all_trading_days:
                    if isinstance(d, int):
                        date_int = d
                    elif isinstance(d, str):
                        date_int = int(d.replace('-', '').replace(' ', '').replace(':', '')[:8])
                    elif hasattr(d, 'strftime'):
                        date_int = int(d.strftime('%Y%m%d'))
                    else:
                        continue

                    if min_date_int <= date_int <= max_date_int:
                        trading_days.add(date_int)

                expected_count = len(trading_days)
                logger.info(f"Filtered trading_days count: {expected_count}")
            except Exception as e:
                logger.warning(f"Failed to load trading calendar: {e}")
                # 如果交易日历表不存在，返回基本信息
                return {
                    "table_name": table_name,
                    "exists": True,
                    "has_data": True,
                    "date_field": date_field,
                    "min_date": str(min_date),
                    "max_date": str(max_date),
                    "actual_dates": int(date_count),
                    "trading_calendar_available": False,
                    "message": "Trading calendar not available, cannot check missing dates"
                }

            # 找出缺失的交易日（actual_dates 已经在前面查询过了）
            missing_dates = sorted(trading_days - actual_dates)

            # 计算覆盖率
            coverage = (len(actual_dates) / expected_count * 100) if expected_count > 0 else 0

            return {
                "table_name": table_name,
                "exists": True,
                "has_data": True,
                "date_field": date_field,
                "min_date": str(min_date),
                "max_date": str(max_date),
                "actual_dates": len(actual_dates),
                "expected_dates": expected_count,
                "missing_dates": [str(d) for d in missing_dates],
                "missing_count": len(missing_dates),
                "coverage_percent": round(coverage, 2),
                "trading_calendar_available": True
            }

        except Exception as e:
            logger.error(f"Failed to inspect data for task {task_id}: {e}")
            raise ValueError(f"Failed to inspect data: {e}")


# 创建三个服务实例
sync_service = TaskService[SyncTaskConfig](
    task_type="sync",
    table_name="sync_task_config",
    id_field="task_id",
    model_class=SyncTaskConfig
)

etl_service = TaskService[ETLTaskConfig](
    task_type="etl",
    table_name="etl_task_config",
    id_field="task_id",
    model_class=ETLTaskConfig
)

factor_service = TaskService[FactorConfig](
    task_type="factor",
    table_name="factor_metadata",
    id_field="factor_id",
    model_class=FactorConfig
)
