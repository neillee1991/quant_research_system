"""
任务提交器
通过 HTTP API 调用现有任务端点
"""
import asyncio
import httpx
from typing import Optional
from datetime import datetime

from app.core.logger import logger
from .models import TaskRun, TaskStatus
from .repository import TaskRunRepository


class TaskSubmitter:
    """HTTP 任务提交器"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.max_retries = 3
        self.timeout = 300.0  # 5 分钟超时
        self.poll_interval = 2.0  # 轮询间隔（秒）
        self.max_poll_time = 300.0  # 最大轮询时间（秒）

    def _get_task_endpoint(self, task_type: str, task_id: str) -> str:
        """根据 task_type 获取 API 端点"""
        return f"{self.base_url}/api/v1/tasks/{task_type}/{task_id}/execute"

    def _get_task_status_endpoint(self, task_type: str, run_id: str) -> str:
        """获取任务状态查询端点"""
        return f"{self.base_url}/api/v1/tasks/{task_type}/status/{run_id}"

    async def submit_task(self, task_run: TaskRun) -> bool:
        """提交任务执行，带重试和轮询等待"""
        endpoint = self._get_task_endpoint(task_run.task_type, task_run.task_id)
        logger.info(f"提交任务: {task_run.task_type}/{task_run.task_id} -> {endpoint}")

        # 步骤 1: 提交任务
        # 构建请求体，传递 target_date 和 flow_run_id
        request_body = {}
        if task_run.target_date:
            request_body["start_date"] = task_run.target_date
            request_body["end_date"] = task_run.target_date  # 明确单日范围，避免各任务类型兜底逻辑不一致
        if task_run.flow_run_id:
            request_body["flow_run_id"] = task_run.flow_run_id
        if task_run.run_id:
            request_body["run_id"] = task_run.run_id

        run_id = None
        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(endpoint, json=request_body)
                    response.raise_for_status()
                    result = response.json()
                    run_id = result.get("result", {}).get("run_id")
                    if run_id:
                        logger.info(f"任务提交成功: {task_run.task_type}/{task_run.task_id}, run_id={run_id}")
                        break
                    else:
                        logger.warning(f"任务提交成功但未返回 run_id: {result}")
            except Exception as e:
                wait_time = 2 ** attempt  # 指数退避: 1s, 2s, 4s
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"任务提交失败 (尝试 {attempt + 1}/{self.max_retries}): {e}，"
                        f"{wait_time} 秒后重试"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"任务提交最终失败 (尝试 {self.max_retries} 次): {e}"
                    )
                    return False

        if not run_id:
            logger.error(f"无法获取任务 run_id，放弃等待")
            return False

        # 步骤 2: 轮询等待任务完成
        logger.info(f"等待任务完成: {task_run.task_type}/{task_run.task_id}, run_id={run_id}")
        start_time = datetime.now()

        while True:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > self.max_poll_time:
                logger.error(f"任务等待超时 ({self.max_poll_time}秒): {task_run.task_type}/{task_run.task_id}")
                return False

            try:
                status_endpoint = self._get_task_status_endpoint(task_run.task_type, run_id)
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(status_endpoint)
                    if response.status_code == 200:
                        result = response.json()
                        task_data = result.get("data", {})
                        status = task_data.get("status")

                        if status == "success":
                            logger.info(f"任务执行成功: {task_run.task_type}/{task_run.task_id}")
                            return True
                        elif status == "failed":
                            error = task_data.get("error", "未知错误")
                            logger.error(f"任务执行失败: {task_run.task_type}/{task_run.task_id}, error={error}")
                            return False
                        elif status in ["running", "pending"]:
                            # 继续等待
                            await asyncio.sleep(self.poll_interval)
                        else:
                            logger.warning(f"未知任务状态: {status}, 继续等待")
                            await asyncio.sleep(self.poll_interval)
                    else:
                        logger.warning(f"查询任务状态失败: {response.status_code}, 继续等待")
                        await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.warning(f"查询任务状态异常: {e}, 继续等待")
                await asyncio.sleep(self.poll_interval)
