"""
ETL 专用端点：test / create-table / schema
"""
from fastapi import APIRouter, HTTPException, Path

from app.core.logger import logger
from app.services.task_service import etl_service

router = APIRouter()


@router.post("/tasks/etl/test")
async def test_etl_script(payload: dict):
    script = payload.get("script", "")
    date = payload.get("date")
    if not script or not script.strip():
        raise HTTPException(status_code=400, detail="Script cannot be empty")
    try:
        return await etl_service.test_script(script, date)
    except Exception as e:
        logger.error(f"Failed to test ETL script: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/tasks/etl/{task_id}/create-table")
async def create_etl_table(
    task_id: str = Path(...),
    payload: dict = None,
):
    try:
        return await etl_service.create_table(task_id, payload or {})
    except Exception as e:
        logger.error(f"Failed to create ETL table for {task_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tasks/etl/{task_id}/schema")
async def get_etl_table_schema(task_id: str = Path(...)):
    try:
        return await etl_service.get_schema(task_id)
    except Exception as e:
        logger.error(f"Failed to get ETL schema for {task_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
