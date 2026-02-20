from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from data_manager.config_sync_engine import sync_engine
from app.core.logger import logger

router = APIRouter()


@router.post("/sync/config/task/{task_id}")
def sync_single_task(task_id: str, target_date: Optional[str] = None):
    """同步指定任务"""
    try:
        tasks = sync_engine.config.get("sync_tasks", [])
        task = next((t for t in tasks if t["task_id"] == task_id), None)

        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        success = sync_engine.sync_task(task, target_date)
        return {
            "task_id": task_id,
            "status": "success" if success else "failed",
            "target_date": target_date
        }
    except Exception as e:
        logger.error(f"Sync task {task_id} error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync/config/all")
def sync_all_tasks(target_date: Optional[str] = None):
    """同步所有启用的任务"""
    try:
        import threading
        # 后台执行避免超时
        def run_sync():
            sync_engine.sync_all_enabled_tasks(target_date)

        t = threading.Thread(target=run_sync, daemon=True)
        t.start()

        return {
            "status": "started",
            "message": "Sync started in background",
            "target_date": target_date
        }
    except Exception as e:
        logger.error(f"Sync all tasks error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync/config/schedule/{schedule}")
def sync_by_schedule(schedule: str, target_date: Optional[str] = None):
    """按调度类型同步（daily/weekly/monthly）"""
    try:
        if schedule not in ["daily", "weekly", "monthly"]:
            raise HTTPException(status_code=400, detail="Invalid schedule type")

        import threading
        def run_sync():
            sync_engine.sync_by_schedule(schedule, target_date)

        t = threading.Thread(target=run_sync, daemon=True)
        t.start()

        return {
            "status": "started",
            "schedule": schedule,
            "target_date": target_date
        }
    except Exception as e:
        logger.error(f"Sync by schedule error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sync/config/tasks")
def list_sync_tasks():
    """列出所有同步任务配置"""
    try:
        tasks = sync_engine.config.get("sync_tasks", [])
        return {
            "tasks": [
                {
                    "task_id": t["task_id"],
                    "description": t["description"],
                    "sync_type": t["sync_type"],
                    "schedule": t["schedule"],
                    "enabled": t.get("enabled", True),
                    "table_name": t["table_name"]
                }
                for t in tasks
            ],
            "total": len(tasks)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sync/config/status/{task_id}")
def get_task_status(task_id: str):
    """获取任务同步状态"""
    try:
        last_date = sync_engine._get_last_sync_date(task_id)
        return {
            "task_id": task_id,
            "last_sync_date": last_date,
            "status": "synced" if last_date else "not_synced"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
