"""
数据同步 Prefect Flow
替代原 DAG 调度系统中的数据同步流水线
"""
from datetime import datetime
from typing import Optional
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def sync_task(task_id: str, target_date: Optional[str] = None, end_date: Optional[str] = None):
    """同步单个数据任务"""
    from data_manager.refactored_sync_engine import sync_engine

    logger = get_run_logger()
    logger.info(f"开始同步任务: {task_id}, 目标日期: {target_date}")

    success = sync_engine.sync_task(task_id, target_date, end_date)

    if success:
        logger.info(f"任务 {task_id} 同步成功")
    else:
        logger.error(f"任务 {task_id} 同步失败")
        raise RuntimeError(f"同步任务失败: {task_id}")

    return success


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def compute_factor(factor_id: str, target_date: Optional[str] = None):
    """计算单个因子"""
    from store.dolphindb_client import db_client

    logger = get_run_logger()
    logger.info(f"开始计算因子: {factor_id}, 目标日期: {target_date}")

    try:
        # 尝试使用 production engine
        from engine.production.engine import ProductionEngine
        engine = ProductionEngine(db_client)
        result = engine.compute_factor(factor_id, target_date)
        logger.info(f"因子 {factor_id} 计算完成")
        return result
    except ImportError:
        logger.warning(f"ProductionEngine 不可用，跳过因子 {factor_id}")
        return None
    except Exception as e:
        logger.error(f"因子 {factor_id} 计算失败: {e}")
        raise


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
    daily_future = sync_task.submit("daily", target_date)
    daily_basic_future = sync_task.submit("daily_basic", target_date)
    adj_factor_future = sync_task.submit("adj_factor", target_date)
    moneyflow_future = sync_task.submit("moneyflow", target_date)

    # 等待第一层完成
    daily_result = daily_future.result()
    daily_basic_result = daily_basic_future.result()
    adj_factor_future.result()
    moneyflow_future.result()

    # 第二层: 依赖 daily 的因子计算
    if daily_result:
        compute_factor.submit("factor_momentum_20", target_date)
        compute_factor.submit("factor_volatility_20", target_date)
        compute_factor.submit("factor_ma_20", target_date)

    # 依赖 daily_basic 的因子计算
    if daily_basic_result:
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
    stock_basic_future = sync_task.submit("stock_basic", target_date)
    daily_future = sync_task.submit("daily", target_date)

    # 等待同步完成
    stock_basic_future.result()
    daily_result = daily_future.result()

    # 计算技术因子
    if daily_result:
        compute_factor.submit("factor_ma_5", target_date)
        compute_factor.submit("factor_ma_20", target_date)
        compute_factor.submit("factor_rsi_14", target_date)

    logger.info("每周分析流水线完成")


@flow(name="full-data-sync", log_prints=True)
def sync_all_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """全量数据同步"""
    from data_manager.refactored_sync_engine import sync_engine

    logger = get_run_logger()
    logger.info(f"开始全量数据同步: {start_date} → {end_date}")

    results = sync_engine.sync_all_enabled_tasks(start_date)

    success_count = sum(1 for v in results.values() if v == "success")
    fail_count = sum(1 for v in results.values() if v == "failed")

    logger.info(f"全量同步完成: {success_count} 成功, {fail_count} 失败")
    return results


@flow(name="single-task-sync", log_prints=True)
def sync_single(task_id: str, target_date: Optional[str] = None, end_date: Optional[str] = None):
    """同步单个任务（供 API 调用）"""
    return sync_task(task_id, target_date, end_date)
