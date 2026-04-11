"""
数据同步 Prefect Flow

Prefect 负责调度和编排，实际执行通过 HTTP API 完成，
确保所有任务状态写入 task_runs 表，前端任务监控可见。
"""
from datetime import datetime
from typing import Optional
from prefect import flow, task, get_run_logger
import httpx
import os

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# 从环境变量读取 API 地址，默认 localhost:8000
_API_BASE = os.getenv("BACKEND_API_URL", "http://localhost:8000/api/v1")
_HTTP_TIMEOUT = 300  # 5 分钟，给后台任务足够时间启动


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def sync_task(task_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """触发单个数据同步任务（通过 HTTP API）"""
    logger = get_run_logger()
    logger.info(f"触发同步任务: {task_id}, start_date={start_date}, end_date={end_date}")

    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    resp = httpx.post(
        f"{_API_BASE}/tasks/sync/{task_id}/execute",
        json=params,
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    run_id = data.get("task_id")  # API 返回的 run_id 字段名为 task_id
    logger.info(f"同步任务 {task_id} 已提交, run_id={run_id}")
    return run_id


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def compute_factor(factor_id: str, target_date: Optional[str] = None, mode: str = "incremental"):
    """触发单个因子计算任务（通过 HTTP API）"""
    logger = get_run_logger()
    logger.info(f"触发因子计算: {factor_id}, target_date={target_date}, mode={mode}")

    payload = {"factor_id": factor_id, "mode": mode}
    if target_date:
        payload["target_date"] = target_date

    resp = httpx.post(
        f"{_API_BASE}/factor/run",
        json=payload,
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    run_id = data.get("data", {}).get("run_id")
    logger.info(f"因子计算 {factor_id} 已提交, run_id={run_id}")
    return run_id


@flow(name="daily-data-sync", log_prints=True)
def sync_daily_data(target_date: Optional[str] = None):
    """
    每日数据同步流水线
    对应原 daily_update DAG: 同步行情 → 计算因子
    """
    logger = get_run_logger()

    if target_date is None:
        target_date = datetime.now().strftime("%Y%m%d")

    logger.info(f"开始每日数据同步, 目标日期: {target_date}")

    # 第一层: 并行同步数据（无依赖）
    daily_future = sync_task.submit("sync_daily", target_date)
    daily_basic_future = sync_task.submit("sync_daily_basic", target_date)
    adj_factor_future = sync_task.submit("sync_adj_factor", target_date)
    moneyflow_future = sync_task.submit("sync_moneyflow", target_date)

    # 等待第一层完成
    daily_run_id = daily_future.result()
    daily_basic_run_id = daily_basic_future.result()
    adj_factor_future.result()
    moneyflow_future.result()

    # 第二层: 依赖 daily 的因子计算
    if daily_run_id:
        compute_factor.submit("factor_momentum_20", target_date)
        compute_factor.submit("factor_volatility_20", target_date)
        compute_factor.submit("factor_ma_20", target_date)

    # 依赖 sync_daily_basic 的因子计算
    if daily_basic_run_id:
        compute_factor.submit("factor_pe_rank", target_date)
        compute_factor.submit("factor_pb_rank", target_date)
        compute_factor.submit("factor_volatility_10", target_date)

    logger.info("每日数据同步流水线完成")


@flow(name="weekly-analysis", log_prints=True)
def weekly_analysis(target_date: Optional[str] = None):
    """
    每周因子分析流水线
    对应原 weekly_analysis DAG
    """
    logger = get_run_logger()

    if target_date is None:
        target_date = datetime.now().strftime("%Y%m%d")

    logger.info(f"开始每周分析, 目标日期: {target_date}")

    # 同步基础数据
    stock_basic_future = sync_task.submit("sync_stock_basic", target_date)
    daily_future = sync_task.submit("sync_daily", target_date)

    # 等待同步完成
    stock_basic_future.result()
    daily_run_id = daily_future.result()

    # 计算技术因子
    if daily_run_id:
        compute_factor.submit("factor_ma_5", target_date)
        compute_factor.submit("factor_ma_20", target_date)
        compute_factor.submit("factor_rsi_14", target_date)

    logger.info("每周分析流水线完成")


@flow(name="full-data-sync", log_prints=True)
def sync_all_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """全量数据同步（逐任务触发 API）"""
    logger = get_run_logger()
    logger.info(f"开始全量数据同步: {start_date} → {end_date}")

    # 获取所有启用的任务列表
    resp = httpx.get(f"{_API_BASE}/tasks/sync", timeout=30)
    resp.raise_for_status()
    tasks_data = resp.json()
    task_list = tasks_data.get("tasks", [])
    enabled_tasks = [t["task_id"] for t in task_list if t.get("enabled", True)]

    logger.info(f"共 {len(enabled_tasks)} 个启用任务")

    futures = [
        sync_task.submit(tid, start_date, end_date)
        for tid in enabled_tasks
    ]
    run_ids = [f.result() for f in futures]

    success_count = sum(1 for r in run_ids if r is not None)
    logger.info(f"全量同步提交完成: {success_count}/{len(enabled_tasks)} 成功提交")
    return run_ids


@flow(name="single-task-sync", log_prints=True)
def sync_single(task_id: str, target_date: Optional[str] = None, end_date: Optional[str] = None):
    """同步单个任务（供 API 调用）"""
    return sync_task(task_id, target_date, end_date)
