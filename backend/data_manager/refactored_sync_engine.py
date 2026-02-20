"""
重构后的同步引擎
使用组合模式，职责清晰分离
"""
from typing import Dict, List, Optional
from app.core.utils import RateLimiter, RetryPolicy
from app.core.config import settings
from app.core.logger import logger
from store.postgres_client import db_client
from data_manager.sync_components import (
    SyncConfigManager,
    SyncLogManager,
    TableManager,
    TushareAPIClient,
    SyncTaskExecutor
)


class RefactoredSyncEngine:
    """重构后的同步引擎"""

    def __init__(self, config_path: Optional[str] = None):
        # 初始化组件
        self.config_manager = SyncConfigManager(config_path)
        self.log_manager = SyncLogManager(db_client)
        self.table_manager = TableManager(db_client)

        # 获取全局配置
        global_config = self.config_manager.get_global_config()
        rate_limit_config = global_config.get("rate_limit", {})

        # 初始化速率限制器和重试策略
        self.rate_limiter = RateLimiter(
            calls_per_minute=rate_limit_config.get(
                "calls_per_minute",
                settings.collector.calls_per_minute
            )
        )

        self.retry_policy = RetryPolicy(
            max_attempts=rate_limit_config.get(
                "retry_times",
                settings.collector.retry_times
            ),
            base_delay=rate_limit_config.get(
                "retry_delay",
                settings.collector.retry_delay
            )
        )

        # 初始化 API 客户端
        self.api_client = TushareAPIClient(
            token=settings.TUSHARE_TOKEN,
            rate_limiter=self.rate_limiter,
            retry_policy=self.retry_policy
        )

        # 初始化任务执行器
        self.task_executor = SyncTaskExecutor(
            api_client=self.api_client,
            repository=db_client,
            table_manager=self.table_manager,
            log_manager=self.log_manager
        )

    def sync_task(self, task_id: str, target_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        """同步单个任务"""
        try:
            task = self.config_manager.get_task(task_id)
            return self.task_executor.execute_task(task, target_date, end_date)
        except Exception as e:
            logger.error(f"Failed to sync task {task_id}: {e}")
            return False

    def sync_all_enabled_tasks(self, target_date: Optional[str] = None) -> Dict[str, str]:
        """同步所有启用的任务"""
        tasks = self.config_manager.get_enabled_tasks()
        logger.info(f"Starting sync for {len(tasks)} enabled tasks")

        results = {}
        for task in tasks:
            task_id = task["task_id"]
            success = self.task_executor.execute_task(task, target_date)
            results[task_id] = "success" if success else "failed"

        logger.info(f"Sync completed. Results: {results}")
        return results

    def sync_by_schedule(
        self,
        schedule: str,
        target_date: Optional[str] = None
    ) -> Dict[str, str]:
        """按调度类型同步"""
        tasks = self.config_manager.get_tasks_by_schedule(schedule)
        logger.info(f"Syncing {len(tasks)} tasks with schedule={schedule}")

        results = {}
        for task in tasks:
            task_id = task["task_id"]
            success = self.task_executor.execute_task(task, target_date)
            results[task_id] = "success" if success else "failed"

        return results

    def get_all_tasks(self) -> List[Dict]:
        """获取所有任务配置"""
        return self.config_manager.get_all_tasks()

    def get_task_status(self, task_id: str) -> Dict:
        """获取任务状态"""
        try:
            task = self.config_manager.get_task(task_id)
            last_sync_date = self.log_manager.get_last_sync_date(task_id)

            return {
                "task_id": task_id,
                "description": task.get("description", ""),
                "enabled": task.get("enabled", True),
                "sync_type": task.get("sync_type", ""),
                "schedule": task.get("schedule", ""),
                "last_sync_date": last_sync_date,
                "table_name": task.get("table_name", ""),
                "date_field": task.get("date_field", "trade_date")
            }
        except Exception as e:
            logger.error(f"Failed to get task status: {e}")
            return {"error": str(e)}


# 全局实例（保持向后兼容）
sync_engine = RefactoredSyncEngine()
