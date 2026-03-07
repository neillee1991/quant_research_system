"""
通用任务服务层
提供统一的 CRUD 操作和版本控制
"""
from typing import TypeVar, Generic, Type, List, Optional, Dict, Any

from app.models.base_task import BaseTaskConfig, SyncTaskConfig, ETLTaskConfig, FactorConfig
from store.dolphindb_client import db_client
from app.core.logger import logger

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
        sql = f"SELECT * FROM {self.table_name} WHERE is_current = true"
        if enabled_only:
            sql += " AND enabled = true"

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
        sql = f"SELECT * FROM {self.table_name} WHERE {self.id_field} = %s AND is_current = true"
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

        # 创建版本
        config_dict = task.model_dump(exclude_none=True)
        config_dict["changed_by"] = changed_by
        config_dict["change_reason"] = change_reason

        version = db_client.create_task_version(
            task_type=self.task_type,
            task_id=task_id,
            config_data=config_dict,
            changed_by=changed_by,
            change_reason=change_reason
        )

        logger.info(f"Created {self.task_type} task {task_id} version {version}")

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

        # 验证更新后的数据
        updated_task = self.model_class(**current_dict)

        # 创建新版本
        config_dict = updated_task.model_dump(exclude_none=True)
        config_dict["changed_by"] = changed_by
        config_dict["change_reason"] = change_reason

        version = db_client.create_task_version(
            task_type=self.task_type,
            task_id=task_id,
            config_data=config_dict,
            changed_by=changed_by,
            change_reason=change_reason
        )

        logger.info(f"Updated {self.task_type} task {task_id} to version {version}")

        # 返回更新后的任务
        return self.get_task(task_id)

    def delete_task(
        self,
        task_id: str,
        changed_by: str = "api",
        change_reason: str = "Delete task"
    ) -> bool:
        """
        删除任务（软删除，设置 enabled=false）

        Args:
            task_id: 任务ID
            changed_by: 修改人
            change_reason: 修改原因

        Returns:
            是否成功删除
        """
        # 检查任务是否存在
        existing = self.get_task(task_id)
        if not existing:
            raise ValueError(f"Task {task_id} not found")

        # 软删除：设置 enabled=false
        config_dict = existing.model_dump(exclude_none=True)
        config_dict["enabled"] = False
        config_dict["changed_by"] = changed_by
        config_dict["change_reason"] = change_reason

        version = db_client.create_task_version(
            task_type=self.task_type,
            task_id=task_id,
            config_data=config_dict,
            changed_by=changed_by,
            change_reason=change_reason
        )

        logger.info(f"Deleted (soft) {self.task_type} task {task_id} version {version}")
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
