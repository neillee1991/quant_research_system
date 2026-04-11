from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import uuid
import time
import polars as pl
from store.dolphindb_client import db_client
from ml_module.pipeline import MLPipeline
from ml_module.optimizer import FactorOptimizer
from app.core.logger import logger

router = APIRouter()

# In-memory job status store (replace with Redis/DB for production)
_job_status: dict[str, dict] = {}
_JOB_TTL_SECONDS = 3600  # expire entries after 1 hour

def _evict_expired_jobs():
    now = time.time()
    expired = [jid for jid, v in list(_job_status.items()) if now - v.get("_ts", now) > _JOB_TTL_SECONDS]
    for jid in expired:
        del _job_status[jid]

class MLTrainRequest(BaseModel):
    ts_code: str
    start_date: str = "20200101"
    end_date: str = "20241231"
    feature_cols: Optional[list[str]] = None
    task: str = "full"  # "automl" | "optimize" | "full"

def _run_ml_job(job_id: str, ts_code: str, start: str, end: str,
                feature_cols: list[str] | None, task: str):
    _job_status[job_id] = {"status": "running", "result": None, "_ts": time.time()}
    try:
        df = db_client.query(
            "SELECT * FROM sync_daily_data WHERE ts_code=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
            (ts_code, start, end),
        )
        if df.is_empty():
            _job_status[job_id] = {"status": "failed", "result": f"No data for {ts_code}", "_ts": time.time()}
            return

        pipeline = MLPipeline(df)
        if task == "automl":
            result = pipeline.run_automl(feature_cols)
        elif task == "optimize":
            result = pipeline.run_optimization(feature_cols)
        else:
            result = pipeline.run_full()

        _job_status[job_id] = {"status": "done", "result": result, "_ts": time.time()}
    except Exception as e:
        logger.error(f"ML job {job_id} failed: {e}")
        _job_status[job_id] = {"status": "failed", "result": str(e), "_ts": time.time()}

@router.post("/ml/train")
def start_ml_train(req: MLTrainRequest, background_tasks: BackgroundTasks):
    """Start an ML training job in the background."""
    _evict_expired_jobs()
    job_id = str(uuid.uuid4())
    # Set status BEFORE scheduling to avoid race condition
    _job_status[job_id] = {"status": "queued", "result": None, "_ts": time.time()}
    background_tasks.add_task(
        _run_ml_job, job_id, req.ts_code, req.start_date, req.end_date,
        req.feature_cols, req.task
    )
    return {"job_id": job_id, "status": "queued"}

@router.get("/ml/weights")
def get_best_weights():
    """Return the latest optimized factor weights."""
    weights = FactorOptimizer.load_weights()
    return {"weights": weights}
