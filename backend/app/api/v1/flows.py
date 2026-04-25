"""
Flow Configuration Management API (PostgreSQL + Custom Scheduler)
"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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

class BackfillRequest(BaseModel):
    start_date: str
    end_date: str

# ==================== API Endpoints ====================

@router.get("/flows", response_model=List[FlowConfigListItem])
async def list_flows(
    enabled_only: bool = Query(default=False, description="Only return enabled flows"),
):
    """List all flow configurations"""
    try:
        return await flow_service.list_flows(enabled_only=enabled_only)
    except Exception as e:
        logger.error(f"Failed to list flows: {e}")
        raise HTTPException(status_code=500, detail="Failed to list flows")

@router.get("/flows/{name}", response_model=FlowConfigInDB)
async def get_flow(name: str):
    """Get a single flow configuration"""
    try:
        flow = await flow_service.get_flow(name)
        if not flow:
            raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")
        return flow
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get flow")

@router.post("/flows", response_model=FlowConfigInDB)
async def create_flow(config: FlowConfigCreate):
    """Create a new flow configuration"""
    try:
        return await flow_service.create_flow(config)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create flow: {e}")
        raise HTTPException(status_code=500, detail="Failed to create flow")

@router.put("/flows/{name}", response_model=FlowConfigInDB)
async def update_flow(name: str, config: FlowConfigUpdate):
    """Update a flow configuration"""
    try:
        return await flow_service.update_flow(name, config)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update flow")

@router.delete("/flows/{name}")
async def delete_flow(name: str, hard: bool = Query(default=False, description="Hard delete (permanently remove)")):
    """Delete a flow (disable by default, or hard delete)"""
    try:
        await flow_service.delete_flow(name, soft_delete=not hard)
        if hard:
            return {"status": "success", "message": f"Flow '{name}' permanently deleted"}
        else:
            return {"status": "success", "message": f"Flow '{name}' disabled"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete flow")

@router.post("/flows/{name}/backfill")
async def backfill_flow(name: str, body: BackfillRequest, background_tasks: BackgroundTasks):
    """回溯模式：按交易日串行触发多个 FlowRun（后台执行）"""
    try:
        scheduler = get_scheduler()
        flow = await scheduler._get_flow_config(name)
        if not flow:
            raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")

        # 后台串行执行，立即返回
        background_tasks.add_task(scheduler.backfill_flow, name, body.start_date, body.end_date)

        return {
            "status": "success",
            "message": f"Flow '{name}' backfill started ({body.start_date} ~ {body.end_date})",
            "start_date": body.start_date,
            "end_date": body.end_date,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to backfill flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to start backfill")

@router.get("/flows/{name}/runs/{flow_run_id}")
async def get_flow_run_detail(name: str, flow_run_id: str):
    """获取单次 flow_run 的详情（flow_run_id 为随机 run_id 字符串）"""
    try:
        from scheduler.db import DatabasePool
        flow_run = await DatabasePool.fetchrow(
            "SELECT * FROM flow_runs WHERE run_id = $1", flow_run_id
        )
        flow_run = dict(flow_run) if flow_run else None
        if not flow_run or flow_run.get("flow_name") != name:
            raise HTTPException(status_code=404, detail=f"Flow run {flow_run_id} not found")

        task_rows = await DatabasePool.fetch("""
            SELECT run_id, task_id, task_type, status,
                   started_at, finished_at, elapsed_sec,
                   rows, params, extra, error_message
            FROM task_runs
            WHERE flow_run_id = $1 AND run_id IS NOT NULL
            ORDER BY COALESCE(started_at, created_at)
        """, flow_run["id"])

        def fmt_time(t):
            if not t:
                return None
            # 如果是 naive datetime，附加上海时区
            if t.tzinfo is None:
                t = t.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            return t.isoformat()

        tasks = []
        for r in task_rows:
            tasks.append({
                "run_id": r["run_id"],
                "task_id": r["task_id"],
                "task_type": r["task_type"],
                "status": r["status"],
                "started_at": fmt_time(r["started_at"]),
                "finished_at": fmt_time(r["finished_at"]),
                "elapsed_sec": r["elapsed_sec"],
                "rows": r["rows"],
                "params": r["params"],
                "extra": r["extra"],
                "error": r["error_message"],
            })

        duration_sec = None
        if flow_run.get("started_at") and flow_run.get("ended_at"):
            duration_sec = (flow_run["ended_at"] - flow_run["started_at"]).total_seconds()

        return {
            "flow_run": {
                "run_id": flow_run["run_id"],
                "flow_name": flow_run["flow_name"],
                "status": flow_run["status"],
                "trigger_type": flow_run["trigger_type"],
                "target_date": flow_run["target_date"],
                "started_at": fmt_time(flow_run.get("started_at")),
                "ended_at": fmt_time(flow_run.get("ended_at")),
                "duration_sec": duration_sec,
                "error_message": flow_run.get("error_message"),
            },
            "tasks": tasks,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get flow run detail {flow_run_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get flow run detail")

@router.get("/flows/{name}/runs")
async def list_flow_runs(
    name: str,
    limit: int = Query(default=50, ge=1, le=200, description="Maximum number of runs to return"),
):
    """List flow execution history"""
    try:
        runs = await FlowRunRepository.list_by_flow_name(name, limit=limit)
        # Convert to frontend expected format
        formatted_runs = []
        for run in runs:
            duration_sec = None
            if run.get("started_at") and run.get("ended_at"):
                duration_sec = (run["ended_at"] - run["started_at"]).total_seconds()

            formatted_runs.append({
                "flow_run_id": run.get("run_id") or str(run["id"]),  # 优先用随机 run_id
                "flow_name": run["flow_name"],
                "status": run["status"],
                "trigger_type": "scheduled" if run["trigger_type"] == "cron" else run["trigger_type"],
                "target_date": run["target_date"],
                "started_at": (run["started_at"].replace(tzinfo=ZoneInfo("Asia/Shanghai")) if run["started_at"].tzinfo is None else run["started_at"]).isoformat() if run.get("started_at") else None,
                "finished_at": (run["ended_at"].replace(tzinfo=ZoneInfo("Asia/Shanghai")) if run["ended_at"].tzinfo is None else run["ended_at"]).isoformat() if run.get("ended_at") else None,
                "duration_sec": duration_sec,
                "error": run.get("error_message"),
            })
        return {
            "status": "success",
            "data": formatted_runs,
        }
    except Exception as e:
        logger.error(f"Failed to list flow runs for {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to list flow runs")

# ==================== Dependency Inference ====================

@router.post("/flows/infer-dependencies")
async def infer_dependencies(tasks: List[TaskInDAG]):
    """
    Infer dependencies for tasks automatically

    For ETL tasks: parses SQL to find source tables and maps to sync tasks
    For factor tasks: uses depends_on from factor config
    """
    try:
        from app.services.dependency_inference import dependency_inference_service

        inferred_tasks = await dependency_inference_service.infer_dependencies(tasks)

        return {
            "status": "success",
            "tasks": inferred_tasks,
        }
    except Exception as e:
        logger.error(f"Failed to infer dependencies: {e}")
        raise HTTPException(status_code=500, detail="Failed to infer dependencies")
