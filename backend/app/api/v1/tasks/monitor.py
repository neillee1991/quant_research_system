"""
任务监控端点：running / history / cleanup / status
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from app.core.logger import logger
from scheduler.db import DatabasePool
from .models import RunningTaskResponse, TaskHistoryResponse, _format_task_row

router = APIRouter()


@router.get("/tasks/running", response_model=RunningTaskResponse)
async def get_running_tasks(
    task_type: Optional[str] = Query(default=None),
    task_id: Optional[str] = Query(default=None),
):
    try:
        conditions = ["status = 'running'"]
        params: list = []
        idx = 1
        if task_type:
            conditions.append(f"task_type = ${idx}"); params.append(task_type); idx += 1
        if task_id:
            conditions.append(f"task_id = ${idx}"); params.append(task_id); idx += 1
        where = " AND ".join(conditions)
        rows = await DatabasePool.fetch(
            f"SELECT * FROM task_runs WHERE {where} ORDER BY started_at DESC", *params
        )
        tasks = [_format_task_row(dict(r)) for r in rows]
        return RunningTaskResponse(tasks=tasks, total=len(tasks))
    except Exception as e:
        logger.error(f"Failed to get running tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/history", response_model=TaskHistoryResponse)
async def get_task_history(
    limit: int = Query(default=50, ge=1, le=200),
    task_type: Optional[str] = Query(default=None),
    task_id: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    try:
        conditions = ["status IN ('success', 'failed')"]
        params: list = []
        idx = 1
        if task_type:
            conditions.append(f"task_type = ${idx}"); params.append(task_type); idx += 1
        if task_id:
            conditions.append(f"task_id = ${idx}"); params.append(task_id); idx += 1
        if start_date:
            conditions.append(f"started_at >= TO_DATE(${idx}, 'YYYYMMDD')"); params.append(start_date); idx += 1
        if end_date:
            conditions.append(f"started_at < TO_DATE(${idx}, 'YYYYMMDD') + INTERVAL '1 day'"); params.append(end_date); idx += 1
        where = " AND ".join(conditions)
        from app.core.sql_security import validate_limit_value
        safe_limit = validate_limit_value(limit)
        params.append(safe_limit)
        rows = await DatabasePool.fetch(
            f"SELECT * FROM task_runs WHERE {where} ORDER BY COALESCE(finished_at, started_at) DESC LIMIT ${idx}",
            *params,
        )
        tasks = [_format_task_row(dict(r)) for r in rows]
        return TaskHistoryResponse(tasks=tasks, total=len(tasks))
    except Exception as e:
        logger.error(f"Failed to get task history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/cleanup")
async def cleanup_stale_tasks(
    timeout_minutes: int = Query(default=0, ge=0),
):
    try:
        from app.services.task_runner import TaskRunner
        cleaned = await TaskRunner.cleanup_stale(
            timeout_minutes=timeout_minutes,
            reason=f"manual cleanup (timeout={timeout_minutes}min)"
        )
        return {"status": "success", "data": {"cleaned": cleaned}}
    except Exception as e:
        logger.error(f"Failed to cleanup stale tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/status/{run_id}")
async def get_task_status(
    task_type: str,
    run_id: str,
):
    try:
        row = await DatabasePool.fetchrow(
            "SELECT * FROM task_runs WHERE run_id = $1 LIMIT 1", run_id
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"Task status not found for run_id: {run_id}")
        return {"status": "success", "data": _format_task_row(dict(row))}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
