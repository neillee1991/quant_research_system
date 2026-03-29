"""统一任务管理服务

所有异步任务（sync/etl/factor/analysis）通过此服务写入 task_runs 表，
前端监控只需查询一张表。原有日志表保留不动（双写）。
"""
import time
from datetime import datetime
from functools import wraps
from typing import Callable, Optional

import polars as pl

from app.core.logger import logger


class TaskRunner:
    """统一任务状态管理，所有方法失败只 warning，不影响主业务"""

    @staticmethod
    def start(
        run_id: str,
        task_type: str,
        task_id: str,
        task_name: str,
        params: str = "",
    ) -> None:
        """写入 task_runs，status=running"""
        try:
            from store.dolphindb_client import db_client

            record = pl.DataFrame({
                "run_id": [run_id],
                "task_type": [task_type],
                "task_id": [task_id],
                "task_name": [task_name],
                "status": ["running"],
                "started_at": [datetime.now()],
                "finished_at": [None],
                "elapsed_sec": [0.0],
                "rows": [0],
                "error": [""],
                "params": [params],
            }).with_columns([
                pl.col("finished_at").cast(pl.Datetime),
            ])
            db_client.append("task_runs", record)
        except Exception as e:
            logger.warning(f"TaskRunner.start failed for {run_id}: {e}")

    @staticmethod
    def finish(run_id: str, rows: int = 0, elapsed_sec: float = 0.0) -> None:
        """更新 task_runs：status=success"""
        try:
            from store.dolphindb_client import db_client

            existing = db_client.query(
                "SELECT * FROM task_runs WHERE run_id = %s", (run_id,)
            )
            if existing.is_empty():
                logger.warning(f"TaskRunner.finish: run_id {run_id} not found in task_runs")
                return
            r = existing.to_dicts()[0]
            record = pl.DataFrame({
                "run_id": [r["run_id"]],
                "task_type": [r.get("task_type") or ""],
                "task_id": [r.get("task_id") or ""],
                "task_name": [r.get("task_name") or ""],
                "status": ["success"],
                "started_at": [r.get("started_at")],
                "finished_at": [datetime.now()],
                "elapsed_sec": [float(elapsed_sec)],
                "rows": [int(rows)],
                "error": [r.get("error") or ""],
                "params": [r.get("params") or ""],
            }).with_columns([
                pl.col("started_at").cast(pl.Datetime),
                pl.col("finished_at").cast(pl.Datetime),
            ])
            db_client.upsert("task_runs", record, key_columns=["run_id"])
        except Exception as e:
            logger.warning(f"TaskRunner.finish failed for {run_id}: {e}")

    @staticmethod
    def fail(run_id: str, error: str = "", elapsed_sec: float = 0.0) -> None:
        """更新 task_runs：status=failed"""
        try:
            from store.dolphindb_client import db_client

            existing = db_client.query(
                "SELECT * FROM task_runs WHERE run_id = %s", (run_id,)
            )
            if existing.is_empty():
                logger.warning(f"TaskRunner.fail: run_id {run_id} not found in task_runs")
                return
            r = existing.to_dicts()[0]
            record = pl.DataFrame({
                "run_id": [r["run_id"]],
                "task_type": [r.get("task_type") or ""],
                "task_id": [r.get("task_id") or ""],
                "task_name": [r.get("task_name") or ""],
                "status": ["failed"],
                "started_at": [r.get("started_at")],
                "finished_at": [datetime.now()],
                "elapsed_sec": [float(elapsed_sec)],
                "rows": [0],
                "error": [str(error)[:500]],
                "params": [r.get("params") or ""],
            }).with_columns([
                pl.col("started_at").cast(pl.Datetime),
                pl.col("finished_at").cast(pl.Datetime),
            ])
            db_client.upsert("task_runs", record, key_columns=["run_id"])
        except Exception as e:
            logger.warning(f"TaskRunner.fail failed for {run_id}: {e}")

    @staticmethod
    def cleanup_stale(timeout_minutes: int = 0, reason: str = "timeout") -> int:
        """将僵尸 running 记录标记为 failed。

        timeout_minutes=0 表示清理所有 running 记录（用于重启场景）。
        timeout_minutes>0 表示只清理超过指定时间的记录。
        返回清理的记录数。
        """
        try:
            from store.dolphindb_client import db_client

            df = db_client.query("SELECT * FROM task_runs WHERE status = 'running'")
            if df.is_empty():
                return 0

            now = datetime.now()
            to_clean = []
            for row in df.to_dicts():
                if timeout_minutes == 0:
                    to_clean.append(row)
                else:
                    started = row.get("started_at")
                    if started is None:
                        to_clean.append(row)
                    else:
                        age_minutes = (now - started).total_seconds() / 60
                        if age_minutes >= timeout_minutes:
                            to_clean.append(row)

            for row in to_clean:
                started = row.get("started_at")
                elapsed = (now - started).total_seconds() if started else 0.0
                record = pl.DataFrame({
                    "run_id": [row["run_id"]],
                    "task_type": [row.get("task_type") or ""],
                    "task_id": [row.get("task_id") or ""],
                    "task_name": [row.get("task_name") or ""],
                    "status": ["failed"],
                    "started_at": [row.get("started_at")],
                    "finished_at": [now],
                    "elapsed_sec": [float(elapsed)],
                    "rows": [row.get("rows") or 0],
                    "error": [f"Task interrupted: {reason}"],
                    "params": [row.get("params") or ""],
                }).with_columns([
                    pl.col("started_at").cast(pl.Datetime),
                    pl.col("finished_at").cast(pl.Datetime),
                ])
                db_client.upsert("task_runs", record, key_columns=["run_id"])

            return len(to_clean)
        except Exception as e:
            logger.warning(f"TaskRunner.cleanup_stale failed: {e}")
            return 0


def tracked_task(task_type: str, task_id_kwarg: str = "task_id") -> Callable:
    """装饰后台函数，自动调用 TaskRunner.finish/fail。

    被装饰函数需要接受 run_id 关键字参数。
    API 层负责调用 TaskRunner.start()，装饰器只负责结束状态。

    用法：
        @tracked_task("sync", task_id_kwarg="task_id")
        def _sync_task_background(task_id: str, ..., run_id: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            run_id: Optional[str] = kwargs.get("run_id")
            t0 = time.time()
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - t0
                rows = 0
                if isinstance(result, dict):
                    rows = result.get("rows", 0) or 0
                elif hasattr(result, "rows"):
                    rows = result.rows or 0
                if run_id:
                    TaskRunner.finish(run_id, rows=rows, elapsed_sec=elapsed)
                return result
            except Exception as e:
                elapsed = time.time() - t0
                if run_id:
                    TaskRunner.fail(run_id, error=str(e), elapsed_sec=elapsed)
                raise
        return wrapper
    return decorator
