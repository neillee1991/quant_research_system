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
