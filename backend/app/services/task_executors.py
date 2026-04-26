"""
后台任务执行函数 — sync / etl / factor
从 API 层分离，供 execute.py 端点和 scheduler/submitter 共同调用
"""
from typing import Optional
import polars as pl

from app.core.logger import logger
from app.services.task_runner import tracked_task


@tracked_task("sync", task_id_kwarg="task_id")
async def execute_sync_task(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    import asyncio
    from data_manager.refactored_sync_engine import sync_engine
    rows = await asyncio.get_event_loop().run_in_executor(
        None, lambda: sync_engine.sync_task(task_id=task_id, target_date=start_date, end_date=end_date)
    )
    if rows < 0:
        raise RuntimeError(f"Sync task {task_id} failed")
    logger.info(f"Sync task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows, "extra": {"start_date": start_date, "end_date": end_date}}


@tracked_task("etl", task_id_kwarg="task_id")
async def execute_etl_task(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    from scheduler.db import DatabasePool
    from infrastructure.database.dolphindb_client import db_client

    row = await DatabasePool.fetchrow("SELECT * FROM etl_task_configs WHERE task_id = $1", task_id)
    if row is None:
        raise ValueError(f"ETL task {task_id} not found")
    task = dict(row)

    table_name = task.get("table_name")
    schema = task.get("schema") or {}
    primary_keys = task.get("primary_keys") or []
    if table_name and schema:
        import asyncio
        from data_manager.sync_components import TableManager as SyncTableManager
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: SyncTableManager(db_client).ensure_table_exists({
                "table_name": table_name,
                "schema": schema,
                "primary_keys": primary_keys,
            })
        )

    script_template = task.get("script", "")
    if not script_template or not script_template.strip():
        raise ValueError("ETL script is empty")

    if start_date and len(start_date) == 8:
        date_str = f"{start_date[:4]}.{start_date[4:6]}.{start_date[6:]}"
    else:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y.%m.%d")

    from app.core.config import settings
    db_path = settings.database.db_path
    script = (script_template
              .replace("{date}", date_str)
              .replace("{db_ts}", db_path)
              .replace("{db_meta}", db_path))

    import asyncio
    def _run():
        result = db_client.query(script)
        # 检查查询结果是否为空
        if result is None:
            return 0
        # 检查是否是 Polars DataFrame 并调用 is_empty() 方法
        if hasattr(result, 'is_empty') and result.is_empty():
            return 0
        # 检查是否是字典或列表类型的空数据
        if isinstance(result, dict) and not result:
            return 0
        if isinstance(result, list) and not result:
            return 0
        # 检查是否是 Pandas DataFrame 且为空
        if hasattr(result, 'empty') and result.empty:
            return 0
        if table_name:
            _pks = primary_keys if isinstance(primary_keys, list) else []
            is_full = task.get("sync_type", "incremental") == "full"
            # 确保 upsert 方法的 df 参数是 Polars DataFrame
            if not isinstance(result, pl.DataFrame):
                try:
                    result = pl.DataFrame(result)
                except Exception as e:
                    logger.error(f"无法将查询结果转换为 Polars DataFrame: {e}")
                    return 0
            db_client.upsert(
                table_name=table_name,
                df=result,
                key_columns=_pks,
                is_full_sync=is_full,
                trade_date=date_str.replace(".", "") if not is_full else None,
            )
        # 计算返回的行数
        if hasattr(result, 'height'):  # Polars DataFrame
            return result.height
        elif hasattr(result, 'shape') and len(result.shape) > 0:  # Pandas DataFrame
            return result.shape[0]
        elif isinstance(result, list):  # 列表类型
            return len(result)
        elif isinstance(result, dict):  # 字典类型（键值对形式）
            # 假设字典的值是数组或列表
            if result:
                first_value = list(result.values())[0]
                if isinstance(first_value, list) or isinstance(first_value, pl.Series):
                    return len(first_value)
        return 0

    rows = await asyncio.get_event_loop().run_in_executor(None, _run)
    logger.info(f"ETL task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows if isinstance(rows, int) else 0, "extra": {"start_date": start_date, "end_date": end_date}}


@tracked_task("factor", task_id_kwarg="task_id")
async def execute_factor_task(
    task_id: str,
    start_date: Optional[str],
    end_date: Optional[str],
    run_id: str,
    target_date: Optional[str] = None,
    mode: Optional[str] = None,
    preprocess: Optional[dict] = None,
):
    import asyncio
    from app.services.factor_service import FactorComputeService
    from infrastructure.database.dolphindb_client import db_client
    from engine.factor.registry import get_factor, discover_factors

    service = FactorComputeService(db_client)
    discover_factors(db_client=db_client)
    definition = get_factor(task_id)

    # preprocess 优先用调用方传入的，其次从 definition.params 取
    if preprocess is None and definition and definition.params:
        preprocess = definition.params.get("preprocess")

    # mode 优先用调用方传入的，其次根据 start_date 推断
    resolved_mode = mode or ("full" if start_date else "incremental")

    compute_result = await asyncio.get_event_loop().run_in_executor(
        None, lambda: service.compute_factor(
            factor_id=task_id,
            target_date=target_date,
            start_date=start_date,
            end_date=end_date,
            mode=resolved_mode,
            preprocess=preprocess,
            run_id=run_id,
        )
    )
    rows = getattr(compute_result, "rows", 0)
    if not getattr(compute_result, "success", True):
        raise RuntimeError(getattr(compute_result, "message", f"Factor {task_id} failed"))
    logger.info(f"Factor task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows, "extra": {"start_date": start_date, "end_date": end_date}}
