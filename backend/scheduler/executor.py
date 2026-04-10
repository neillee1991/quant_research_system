"""
DAG 执行器
拓扑排序 + 层并行执行
"""
from typing import List, Dict, Any, Set, Optional
from collections import deque
import asyncio
from datetime import datetime

from app.core.logger import logger
from .models import FlowRun, TaskRun, FlowStatus, TaskStatus, TriggerType
from .repository import FlowRunRepository, TaskRunRepository
from .submitter import TaskSubmitter


class DAGExecutor:
    """DAG 执行器"""

    def __init__(self, submitter: Optional[TaskSubmitter] = None):
        self.submitter = submitter or TaskSubmitter()

    @staticmethod
    def topological_sort(tasks: List[Dict[str, Any]]) -> List[List[str]]:
        """
        拓扑排序，返回层列表
        每层的任务可以并行执行
        """
        # 构建任务图
        task_map: Dict[str, Dict[str, Any]] = {t["id"]: t for t in tasks}
        in_degree: Dict[str, int] = {t["id"]: 0 for t in tasks}
        adjacency: Dict[str, List[str]] = {t["id"]: [] for t in tasks}

        # 计算入度和邻接表
        for task in tasks:
            task_id = task["id"]
            for dep_id in task.get("depends_on", []):
                if dep_id in task_map:
                    adjacency[dep_id].append(task_id)
                    in_degree[task_id] += 1

        # Kahn 算法进行拓扑排序
        layers: List[List[str]] = []
        queue = deque([t_id for t_id, deg in in_degree.items() if deg == 0])

        while queue:
            level_size = len(queue)
            current_layer = []
            for _ in range(level_size):
                task_id = queue.popleft()
                current_layer.append(task_id)
                for neighbor in adjacency[task_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            layers.append(current_layer)

        # 检查是否有环
        total_tasks = sum(len(layer) for layer in layers)
        if total_tasks != len(tasks):
            raise ValueError("DAG 存在循环依赖")

        return layers

    async def execute_flow(self, flow_run: FlowRun, tasks: List[Dict[str, Any]]) -> bool:
        """
        执行 Flow
        """
        logger.info(f"开始执行 Flow: {flow_run.flow_name}, flow_run_id={flow_run.id}")

        # 更新 FlowRun 状态为 running
        await FlowRunRepository.update_status(flow_run.id, FlowStatus.RUNNING)

        try:
            # 拓扑排序
            layers = self.topological_sort(tasks)
            logger.info(f"DAG 拓扑排序完成，共 {len(layers)} 层")

            # 逐层执行
            task_map: Dict[str, Dict[str, Any]] = {t["id"]: t for t in tasks}
            failed = False

            for i, layer in enumerate(layers):
                if failed:
                    break

                logger.info(f"执行第 {i + 1}/{len(layers)} 层: {layer}")

                # 并行执行层内任务
                layer_tasks = [
                    self._execute_task(flow_run.id, task_map[task_id])
                    for task_id in layer
                ]
                results = await asyncio.gather(*layer_tasks, return_exceptions=True)

                # 检查结果
                for task_id, result in zip(layer, results):
                    if isinstance(result, Exception) or not result:
                        logger.error(f"任务执行失败: {task_id}")
                        failed = True
                        break

            # 更新最终状态
            if failed:
                await FlowRunRepository.update_status(
                    flow_run.id, FlowStatus.FAILED, "部分任务执行失败"
                )
                return False
            else:
                await FlowRunRepository.update_status(flow_run.id, FlowStatus.SUCCESS)
                logger.info(f"Flow 执行成功: {flow_run.flow_name}")
                return True

        except Exception as e:
            logger.error(f"Flow 执行异常: {e}", exc_info=True)
            await FlowRunRepository.update_status(
                flow_run.id, FlowStatus.FAILED, str(e)
            )
            return False

    async def _execute_task(self, flow_run_id: int, task: Dict[str, Any]) -> bool:
        """执行单个 Task"""
        task_id = task["id"]
        task_type = task["type"]

        logger.info(f"执行任务: {task_type}/{task_id}")

        # 创建 TaskRun
        task_run = TaskRun(
            flow_run_id=flow_run_id,
            task_id=task_id,
            task_type=task_type,
            status=TaskStatus.PENDING,
        )
        task_run_id = await TaskRunRepository.create(task_run)
        task_run.id = task_run_id

        # 更新状态为 running
        await TaskRunRepository.update_status(task_run_id, TaskStatus.RUNNING)

        try:
            if task_type == "flow":
                # 嵌套 Flow：递归执行子 Flow
                flow_name = task.get("flow_name")
                if not flow_name:
                    raise ValueError("flow 类型任务必须指定 flow_name")
                success = await self._execute_subflow(flow_run_id, flow_name)
            else:
                # 普通任务：通过 HTTP 提交
                success = await self.submitter.submit_task(task_run)

            # 更新最终状态
            if success:
                await TaskRunRepository.update_status(task_run_id, TaskStatus.SUCCESS)
                logger.info(f"任务执行成功: {task_type}/{task_id}")
            else:
                await TaskRunRepository.update_status(
                    task_run_id, TaskStatus.FAILED, "任务执行失败"
                )
                logger.error(f"任务执行失败: {task_type}/{task_id}")

            return success

        except Exception as e:
            logger.error(f"任务执行异常: {task_type}/{task_id}, {e}", exc_info=True)
            await TaskRunRepository.update_status(
                task_run_id, TaskStatus.FAILED, str(e)
            )
            return False

    async def _execute_subflow(self, parent_flow_run_id: int, flow_name: str) -> bool:
        """执行子 Flow（占位实现，需要与 core.py 配合）"""
        # 这里简化处理，实际应该通过 scheduler core 触发
        logger.info(f"执行子 Flow: {flow_name} (parent_flow_run_id={parent_flow_run_id})")
        # TODO: 完整实现需要调度器 core 支持
        await asyncio.sleep(1)
        return True
