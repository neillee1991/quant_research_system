"""
Flow Configuration Management API (PostgreSQL + Custom Scheduler)
"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from datetime import datetime, timedelta

from app.core.logger import logger
from app.models.flow_config import (
    FlowConfigCreate,
    FlowConfigUpdate,
    FlowConfigInDB,
    FlowConfigListItem,
    TaskInDAG,
)
from app.services.flow_service import flow_service
from scheduler.core import get_scheduler
from scheduler.repository import FlowRunRepository

router = APIRouter()


# ==================== API Endpoints ====================

@router.get("/flows", response_model=List[FlowConfigListItem])
def list_flows(
    enabled_only: bool = Query(default=False, description="Only return enabled flows"),
):
    """List all flow configurations"""
    try:
        return flow_service.list_flows(enabled_only=enabled_only)
    except Exception as e:
        logger.error(f"Failed to list flows: {e}")
        raise HTTPException(status_code=500, detail="Failed to list flows")


@router.get("/flows/{name}", response_model=FlowConfigInDB)
def get_flow(name: str):
    """Get a single flow configuration"""
    try:
        flow = flow_service.get_flow(name)
        if not flow:
            raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")
        return flow
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get flow")


@router.post("/flows", response_model=FlowConfigInDB)
def create_flow(config: FlowConfigCreate):
    """Create a new flow configuration"""
    try:
        return flow_service.create_flow(config)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create flow: {e}")
        raise HTTPException(status_code=500, detail="Failed to create flow")


@router.put("/flows/{name}", response_model=FlowConfigInDB)
def update_flow(name: str, config: FlowConfigUpdate):
    """Update a flow configuration"""
    try:
        return flow_service.update_flow(name, config)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update flow")


@router.delete("/flows/{name}")
def delete_flow(name: str, hard: bool = Query(default=False, description="Hard delete (permanently remove)")):
    """Delete a flow (disable by default, or hard delete)"""
    try:
        flow_service.delete_flow(name, soft_delete=not hard)
        if hard:
            return {"status": "success", "message": f"Flow '{name}' permanently deleted"}
        else:
            return {"status": "success", "message": f"Flow '{name}' disabled"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete flow")


@router.post("/flows/{name}/run")
async def run_flow(
    name: str,
    target_date: Optional[str] = Query(None, description="Target date YYYYMMDD (override date offset)"),
    background_tasks: BackgroundTasks = None,
):
    """Run a flow immediately (legacy endpoint, uses scheduler)"""
    try:
        scheduler = get_scheduler()
        flow_run_id = await scheduler.trigger_flow_manual(name, target_date)

        if not flow_run_id:
            raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")

        return {
            "status": "success",
            "message": f"Flow '{name}' triggered",
            "flow_run_id": flow_run_id,
            "target_date": target_date,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to run flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to run flow")


@router.post("/flows/{name}/trigger")
async def trigger_flow(
    name: str,
    target_date: Optional[str] = Query(None, description="Target date YYYYMMDD (override date offset)"),
):
    """Trigger a flow manually (returns flow_run_id)"""
    try:
        scheduler = get_scheduler()
        flow_run_id = await scheduler.trigger_flow_manual(name, target_date)

        if not flow_run_id:
            raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")

        return {
            "status": "success",
            "message": f"Flow '{name}' triggered",
            "flow_run_id": flow_run_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger flow")


@router.get("/flows/{name}/runs")
async def list_flow_runs(
    name: str,
    limit: int = Query(default=50, ge=1, le=200, description="Maximum number of runs to return"),
):
    """List flow execution history"""
    try:
        runs = await FlowRunRepository.list_by_flow_name(name, limit=limit)
        return {
            "status": "success",
            "data": runs,
        }
    except Exception as e:
        logger.error(f"Failed to list flow runs for {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to list flow runs")


# ==================== Dependency Inference ====================

@router.post("/flows/infer-dependencies")
def infer_dependencies(tasks: List[TaskInDAG]):
    """
    Infer dependencies for tasks automatically

    For ETL tasks: parses SQL to find source tables and maps to sync tasks
    For factor tasks: uses depends_on from factor config
    """
    try:
        from app.services.dependency_inference import dependency_inference_service

        inferred_tasks = dependency_inference_service.infer_dependencies(tasks)

        return {
            "status": "success",
            "tasks": inferred_tasks,
        }
    except Exception as e:
        logger.error(f"Failed to infer dependencies: {e}")
        raise HTTPException(status_code=500, detail="Failed to infer dependencies")
