"""
后台执行端点：sync / etl / factor
后台函数已迁移到 app/services/task_executors.py
"""
import json
from typing import Optional
from fastapi import APIRouter, HTTPException, Path, BackgroundTasks
import uuid

from app.core.logger import logger
from app.services.task_executors import (
    execute_sync_task as _execute_sync_task_background,
    execute_etl_task as _execute_etl_task_background,
    execute_factor_task as _execute_factor_task_background,
)
from .models import TaskExecuteRequest, TaskExecuteResponse, _get_service

router = APIRouter()


@router.post("/tasks/{task_type}/{task_id}/execute", response_model=TaskExecuteResponse)
async def execute_task(
    task_type: str = Path(...),
    task_id: str = Path(...),
    request: TaskExecuteRequest = None,
    background_tasks: BackgroundTasks = None,
):
    try:
        service = _get_service(task_type)
        task = await service.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in {task_type}")

        req = request or TaskExecuteRequest()
        run_id = req.run_id or str(uuid.uuid4())

        params_str = json.dumps(req.params) if req.params else ""
        if req.start_date or req.end_date:
            date_params = {k: v for k, v in {"start_date": req.start_date, "end_date": req.end_date}.items() if v}
            merged = {**date_params, **req.params}
            params_str = json.dumps(merged)

        from app.services.task_runner import TaskRunner
        await TaskRunner.start(
            run_id=run_id,
            task_type=task_type,
            task_id=task_id,
            task_name=f"{task_type.upper()} 任务: {task_id}",
            params=params_str,
            flow_run_id=req.flow_run_id,
        )

        task_kwargs = dict(
            task_id=task_id,
            start_date=req.start_date,
            end_date=req.end_date,
            run_id=run_id,
        )

        if task_type == "sync":
            background_tasks.add_task(_execute_sync_task_background, **task_kwargs)
            message = f"Sync task {task_id} started in background"
        elif task_type == "etl":
            background_tasks.add_task(_execute_etl_task_background, **task_kwargs)
            message = f"ETL task {task_id} started in background"
        elif task_type == "factor":
            background_tasks.add_task(_execute_factor_task_background, **task_kwargs)
            message = f"Factor task {task_id} started in background"
        else:
            raise HTTPException(status_code=400, detail=f"Unknown task_type: {task_type}")

        return TaskExecuteResponse(
            status="success",
            message=message,
            task_id=task_id,
            task_type=task_type,
            result={"run_id": run_id, "status": "pending"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Task execution failed: {str(e)}")
