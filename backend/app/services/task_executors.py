"""
后台任务执行函数 — sync / etl / factor
从 API 层分离，供 execute.py 端点和 scheduler/submitter 共同调用
"""
from typing import Optional

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
        if result is None or result.is_empty():
            return 0
        if table_name:
            _pks = primary_keys if isinstance(primary_keys, list) else []
            is_full = task.get("sync_type", "incremental") == "full"
            db_client.upsert(
                table_name=table_name,
                df=result,
                key_columns=_pks,
                is_full_sync=is_full,
                trade_date=date_str.replace(".", "") if not is_full else None,
            )
        return len(result)

    rows = await asyncio.get_event_loop().run_in_executor(None, _run)
    logger.info(f"ETL task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows if isinstance(rows, int) else 0, "extra": {"start_date": start_date, "end_date": end_date}}


@tracked_task("factor", task_id_kwarg="task_id")
async def execute_factor_task(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    import asyncio
    from app.services.factor_service import FactorComputeService
    from infrastructure.database.dolphindb_client import db_client
    from engine.factor.registry import get_factor, discover_factors

    service = FactorComputeService(db_client)
    discover_factors(db_client=db_client)
    definition = get_factor(task_id)

    preprocess_options = None
    if definition and definition.params:
        preprocess_options = definition.params.get("preprocess")

    compute_result = await asyncio.get_event_loop().run_in_executor(
        None, lambda: service.compute_factor(
            factor_id=task_id,
            start_date=start_date,
            end_date=end_date,
            mode="full" if start_date else "incremental",
            preprocess=preprocess_options,
        )
    )
    rows = getattr(compute_result, "rows", 0)
    logger.info(f"Factor task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows, "extra": {"start_date": start_date, "end_date": end_date}}
