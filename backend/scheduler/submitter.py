"""
任务提交器
通过 HTTP API 调用现有任务端点
"""
import asyncio
import httpx
from typing import Optional

from app.core.logger import logger
from .models import TaskRun, TaskStatus
from .repository import TaskRunRepository


class TaskSubmitter:
    """HTTP 任务提交器"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.max_retries = 3
        self.timeout = 300.0  # 5 分钟超时

    def _get_task_endpoint(self, task_type: str, task_id: str) -> str:
        """根据 task_type 获取 API 端点"""
        if task_type == "sync":
            return f"{self.base_url}/api/v1/sync/task/{task_id}/run"
        elif task_type == "etl":
            return f"{self.base_url}/api/v1/etl/task/{task_id}/run"
        elif task_type == "factor":
            return f"{self.base_url}/api/v1/factors/task/{task_id}/run"
        else:
            raise ValueError(f"Unknown task_type: {task_type}")

    async def submit_task(self, task_run: TaskRun) -> bool:
        """提交任务执行，带重试"""
        endpoint = self._get_task_endpoint(task_run.task_type, task_run.task_id)
        logger.info(f"提交任务: {task_run.task_type}/{task_run.task_id} -> {endpoint}")

        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(endpoint)
                    response.raise_for_status()
                    logger.info(f"任务执行成功: {task_run.task_type}/{task_run.task_id}")
                    return True
            except Exception as e:
                wait_time = 2 ** attempt  # 指数退避: 1s, 2s, 4s
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"任务执行失败 (尝试 {attempt + 1}/{self.max_retries}): {e}，"
                        f"{wait_time} 秒后重试"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"任务执行最终失败 (尝试 {self.max_retries} 次): {e}"
                    )
                    return False
        return False
