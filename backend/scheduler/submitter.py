"""
任务提交器
直接调用后台任务函数，绕过 HTTP 层
"""
import asyncio
import json
from typing import Optional

from app.core.logger import logger
from .models import TaskRun
from app.services.task_executors import (
    execute_sync_task,
    execute_etl_task,
    execute_factor_task,
)


class TaskSubmitter:
    """直接调用任务函数的提交器（替代原 HTTP 自调用方案）"""

    async def submit_task(self, task_run: TaskRun) -> bool:
        """提交任务执行，直接调用对应的后台函数"""
        task_type = task_run.task_type
        task_id = task_run.task_id
        run_id = task_run.run_id
        target_date = task_run.target_date

        logger.info(f"提交任务: {task_type}/{task_id}, run_id={run_id}, target_date={target_date}")

        try:
            from app.services.task_runner import TaskRunner
            from engine.factor.registry import get_factor, discover_factors

            # 构建与 API 层相同格式的参数
            params_dict = {
                "start_date": target_date,
                "end_date": target_date,
            }

            # 获取因子定义，添加预处理参数
            discover_factors(db_client=None)  # 使用默认配置
            definition = get_factor(task_id)
            if definition and definition.params:
                params_dict["preprocess"] = definition.params.get("preprocess", {})

            await TaskRunner.start(
                run_id=run_id,
                task_type=task_type,
                task_id=task_id,
                task_name=f"{task_type.upper()} 任务: {task_id}",
                params=json.dumps(params_dict),
                flow_run_id=task_run.flow_run_id,
            )

            task_kwargs = dict(
                task_id=task_id,
                start_date=target_date,
                end_date=target_date,
                run_id=run_id,
            )

            if task_type == "sync":
                await execute_sync_task(**task_kwargs)
            elif task_type == "etl":
                await execute_etl_task(**task_kwargs)
            elif task_type == "factor":
                await execute_factor_task(**task_kwargs)
            else:
                logger.error(f"未知任务类型: {task_type}")
                await TaskRunner.fail(run_id, error=f"未知任务类型: {task_type}")
                return False

            logger.info(f"任务执行成功: {task_type}/{task_id}")
            return True

        except Exception as e:
            logger.error(f"任务执行失败: {task_type}/{task_id}, error={e}", exc_info=True)
            return False
