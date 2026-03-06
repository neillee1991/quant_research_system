"""
动态 Flow 执行器
根据 YAML 配置动态构建和执行 Prefect Flow
"""
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import defaultdict

from app.core.logger import logger


def _topological_sort(tasks: List[dict]) -> List[List[dict]]:
    """
    拓扑排序，返回按层分组的任务列表
    同一层的任务可以并行执行
    """
    # 构建依赖图
    task_map = {t["id"]: t for t in tasks}
    in_degree = defaultdict(int)
    dependents = defaultdict(list)

    for task in tasks:
        task_id = task["id"]
        deps = task.get("depends_on", [])
        in_degree[task_id] = len(deps)
        for dep in deps:
            dependents[dep].append(task_id)

    # BFS 分层
    layers = []
    queue = [t["id"] for t in tasks if in_degree[t["id"]] == 0]

    while queue:
        layers.append([task_map[tid] for tid in queue])
        next_queue = []
        for tid in queue:
            for dep_id in dependents[tid]:
                in_degree[dep_id] -= 1
                if in_degree[dep_id] == 0:
                    next_queue.append(dep_id)
        queue = next_queue

    # 检查是否有循环依赖
    total_tasks = sum(len(layer) for layer in layers)
    if total_tasks != len(tasks):
        raise ValueError("检测到循环依赖")

    return layers


def _execute_sync_task_sync(task_id: str, target_date: Optional[str]) -> dict:
    """同步执行同步任务"""
    from data_manager.refactored_sync_engine import sync_engine

    logger.info(f"执行同步任务: {task_id}")
    try:
        success = sync_engine.sync_task(task_id, target_date)
        return {"task_id": task_id, "type": "sync", "success": success}
    except Exception as e:
        logger.error(f"同步任务 {task_id} 失败: {e}")
        return {"task_id": task_id, "type": "sync", "success": False, "error": str(e)}


def _execute_factor_task_sync(task_id: str, target_date: Optional[str]) -> dict:
    """同步执行因子计算任务"""
    from store.dolphindb_client import db_client
    from engine.production.engine import ProductionEngine

    logger.info(f"执行因子任务: {task_id}")
    try:
        engine = ProductionEngine(db_client)
        result = engine.run_task(task_id, target_date=target_date)
        return {"task_id": task_id, "type": "factor", "success": result}
    except Exception as e:
        logger.error(f"因子任务 {task_id} 失败: {e}")
        return {"task_id": task_id, "type": "factor", "success": False, "error": str(e)}


async def _execute_task(task: dict, target_date: Optional[str]) -> dict:
    """执行单个任务（在线程池中运行同步代码）"""
    task_id = task["id"]
    task_type = task["type"]

    loop = asyncio.get_event_loop()

    if task_type == "sync":
        return await loop.run_in_executor(
            None, _execute_sync_task_sync, task_id, target_date
        )
    elif task_type == "factor":
        return await loop.run_in_executor(
            None, _execute_factor_task_sync, task_id, target_date
        )
    else:
        return {"task_id": task_id, "type": task_type, "success": False, "error": f"未知任务类型: {task_type}"}


async def run_dynamic_flow(config: dict, target_date: Optional[str] = None) -> Dict[str, Any]:
    """
    执行动态 Flow

    Args:
        config: Flow 配置字典
        target_date: 目标日期 YYYYMMDD

    Returns:
        执行结果字典
    """
    flow_name = config.get("name", "unknown")
    tasks = config.get("tasks", [])

    if not tasks:
        return {"flow": flow_name, "status": "empty", "results": []}

    if target_date is None:
        target_date = datetime.now().strftime("%Y%m%d")

    logger.info(f"开始执行 Flow: {flow_name}, 目标日期: {target_date}")

    # 拓扑排序
    try:
        layers = _topological_sort(tasks)
    except ValueError as e:
        return {"flow": flow_name, "status": "error", "error": str(e)}

    # 按层执行
    all_results = []
    for i, layer in enumerate(layers):
        logger.info(f"执行第 {i+1} 层，共 {len(layer)} 个任务")

        # 并行执行同一层的任务
        layer_tasks = [_execute_task(task, target_date) for task in layer]
        layer_results = await asyncio.gather(*layer_tasks)
        all_results.extend(layer_results)

        # 检查是否有失败的任务
        failed = [r for r in layer_results if not r.get("success")]
        if failed:
            logger.warning(f"第 {i+1} 层有 {len(failed)} 个任务失败")

    success_count = sum(1 for r in all_results if r.get("success"))
    fail_count = len(all_results) - success_count

    logger.info(f"Flow {flow_name} 执行完成: {success_count} 成功, {fail_count} 失败")

    return {
        "flow": flow_name,
        "status": "completed",
        "target_date": target_date,
        "success_count": success_count,
        "fail_count": fail_count,
        "results": all_results
    }
