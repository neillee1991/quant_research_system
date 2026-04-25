"""
CRUD 端点：list / get / create / update / delete / status / inspect
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Path

from app.core.logger import logger
from infrastructure.database.dolphindb_client import db_client
from scheduler.db import DatabasePool
from .models import (
    TaskCreateRequest, TaskUpdateRequest, DeleteResponse,
    TaskListResponse, TaskResponse, DataInspectionResponse,
    _get_service, _parse_task_config, _format_task_row,
)

router = APIRouter()


@router.get("/tasks/{task_type}", response_model=TaskListResponse)
async def list_tasks(
    task_type: str = Path(...),
    enabled_only: bool = Query(default=False),
):
    try:
        service = _get_service(task_type)
        tasks = await service.list_tasks(enabled_only=enabled_only)
        return TaskListResponse(
            tasks=[t.model_dump() for t in tasks],
            total=len(tasks),
            task_type=task_type,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list {task_type} tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}", response_model=TaskResponse)
async def get_task(
    task_type: str = Path(...),
    task_id: str = Path(...),
):
    try:
        service = _get_service(task_type)
        task = await service.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in {task_type}")
        return TaskResponse(task=task.model_dump(), task_type=task_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_type}", response_model=TaskResponse)
async def create_task(
    task_type: str = Path(...),
    request: TaskCreateRequest = None,
):
    try:
        service = _get_service(task_type)
        validated = _parse_task_config(task_type, request.config_data)
        task = await service.create_task(config_data=validated.model_dump(exclude_none=True))
        return TaskResponse(task=task.model_dump(), task_type=task_type)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create {task_type} task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_type}/{task_id}", response_model=TaskResponse)
async def update_task(
    task_type: str = Path(...),
    task_id: str = Path(...),
    request: TaskUpdateRequest = None,
):
    try:
        service = _get_service(task_type)
        validated = _parse_task_config(task_type, request.config_data)
        task = await service.update_task(
            task_id=task_id,
            config_data=validated.model_dump(exclude_none=True),
        )
        return TaskResponse(task=task.model_dump(), task_type=task_type)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        logger.error(f"Failed to update {task_type} task {task_id}: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_type}/{task_id}", response_model=DeleteResponse)
async def delete_task(
    task_type: str = Path(...),
    task_id: str = Path(...),
    drop_table: bool = Query(default=False),
):
    try:
        service = _get_service(task_type)
        if drop_table:
            task = await service.get_task(task_id)
            if task:
                table_name = None
                if task_type in ("sync", "etl"):
                    table_name = getattr(task, "table_name", None)
                elif task_type == "factor":
                    logger.warning(f"Factor tasks use shared table, skipping drop_table for {task_id}")
                if table_name:
                    try:
                        db_client.drop_table(table_name)
                    except Exception as e:
                        logger.warning(f"Failed to drop table {table_name}: {e}")
        success = await service.delete_task(task_id=task_id)
        return DeleteResponse(
            success=success,
            message=f"Task {task_id} deleted successfully" + (" (table dropped)" if drop_table else ""),
            task_id=task_id,
            task_type=task_type,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}/status")
async def get_task_data_status(
    task_type: str = Path(...),
    task_id: str = Path(...),
):
    try:
        service = _get_service(task_type)
        task = await service.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in {task_type}")

        task_dict = task.model_dump()
        table_name = task_dict.get("table_name")
        date_field = task_dict.get("date_field") or ""

        table_latest_date = None
        if table_name:
            try:
                db_client.register_meta_table(table_name)
                if date_field and db_client.table_exists(table_name):
                    result = db_client.query(f'SELECT MAX({date_field}) as max_date FROM {table_name}')
                    if not result.is_empty() and result["max_date"][0] is not None:
                        max_date_val = result["max_date"][0]
                        if isinstance(max_date_val, str):
                            s = max_date_val.replace("-", "").replace(" ", "")[:8]
                            if len(s) == 8 and s.isdigit():
                                table_latest_date = s
                        elif isinstance(max_date_val, int):
                            s = str(max_date_val)
                            if len(s) == 8:
                                table_latest_date = s
                        elif hasattr(max_date_val, "strftime"):
                            table_latest_date = max_date_val.strftime("%Y%m%d")
            except Exception as e:
                logger.warning(f"Failed to get latest date for {task_type} table {table_name}: {e}")

        last_sync_time = None
        try:
            last_run = await DatabasePool.fetchrow("""
                SELECT finished_at FROM task_runs
                WHERE task_type = $1 AND task_id = $2 AND status = 'success'
                ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1
            """, task_type, task_id)
            if last_run and last_run.get("finished_at"):
                finished_at = last_run["finished_at"]
                last_sync_time = finished_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(finished_at, "strftime") else str(finished_at)
        except Exception as e:
            logger.warning(f"Failed to get last sync time for {task_type} task {task_id}: {e}")

        return {"status": "success", "data": {
            "task_id": task_dict.get("task_id"),
            "description": task_dict.get("description", ""),
            "sync_type": task_dict.get("sync_type", ""),
            "table_name": table_name,
            "enabled": task_dict.get("enabled", True),
            "last_sync_date": table_latest_date,
            "last_sync_time": last_sync_time,
            "table_latest_date": table_latest_date,
        }}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get {task_type} task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}/inspect", response_model=DataInspectionResponse)
async def inspect_task_data(
    task_type: str = Path(...),
    task_id: str = Path(...),
):
    try:
        service = _get_service(task_type)
        result = await service.inspect_data(task_id)
        return DataInspectionResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to inspect data for {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Data inspection failed: {str(e)}")
