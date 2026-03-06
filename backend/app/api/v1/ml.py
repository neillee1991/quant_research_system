from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import polars as pl
from store.dolphindb_client import db_client
from ml_module.pipeline import MLPipeline
from ml_module.optimizer import FactorOptimizer
from app.core.logger import logger

router = APIRouter()

# In-memory job status store (replace with Redis/DB for production)
_job_status: dict[str, dict] = {}


class MLTrainRequest(BaseModel):
    ts_code: str
    start_date: str = "20200101"
    end_date: str = "20241231"
    feature_cols: Optional[list[str]] = None
    task: str = "full"  # "automl" | "optimize" | "full"


def _run_ml_job(job_id: str, ts_code: str, start: str, end: str,
                feature_cols: list[str] | None, task: str):
    _job_status[job_id] = {"status": "running", "result": None}
    try:
        df = db_client.query(
            "SELECT * FROM sync_daily_data WHERE ts_code=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
            [ts_code, start, end],
        )
        if df.is_empty():
            _job_status[job_id] = {"status": "failed", "result": f"No data for {ts_code}"}
            return

        pipeline = MLPipeline(df)
        if task == "automl":
            result = pipeline.run_automl(feature_cols)
        elif task == "optimize":
            result = pipeline.run_optimization(feature_cols)
        else:
            result = pipeline.run_full()

        _job_status[job_id] = {"status": "done", "result": result}
    except Exception as e:
        logger.error(f"ML job {job_id} failed: {e}")
        _job_status[job_id] = {"status": "failed", "result": str(e)}


@router.post("/ml/train")
def start_ml_train(req: MLTrainRequest, background_tasks: BackgroundTasks):
    """Start an ML training job in the background."""
    import uuid
    job_id = str(uuid.uuid4())[:8]
    background_tasks.add_task(
        _run_ml_job, job_id, req.ts_code, req.start_date, req.end_date,
        req.feature_cols, req.task
    )
    _job_status[job_id] = {"status": "queued", "result": None}
    return {"job_id": job_id, "status": "queued"}


@router.get("/ml/status/{job_id}")
def get_ml_status(job_id: str):
    """Poll ML job status."""
    if job_id not in _job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_status[job_id]


@router.get("/ml/weights")
def get_best_weights():
    """Return the latest optimized factor weights."""
    weights = FactorOptimizer.load_weights()
    return {"weights": weights}
