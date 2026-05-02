"""统一任务状态管理

所有异步任务（sync/etl/factor/analysis/backtest）通过此模块写入 PostgreSQL task_runs 表。
"""
import asyncio
import json
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from functools import wraps
from typing import Callable, Optional

from app.core.logger import logger


_TZ = ZoneInfo("Asia/Shanghai")


def _now() -> datetime:
    """返回带时区的当前上海时间"""
    return datetime.now(_TZ)


class TaskRunner:
    """统一任务状态管理，所有方法失败只 warning，不影响主业务"""

    @staticmethod
    async def start(
        run_id: str,
        task_type: str,
        task_id: str,
        task_name: str,
        params: str = "",
        flow_run_id: Optional[int] = None,
    ) -> None:
        """写入 task_runs，status=running"""
        try:
            from scheduler.db import DatabasePool
            await DatabasePool.execute("""
                INSERT INTO task_runs
                  (run_id, task_type, task_id, task_name, status,
                   started_at, finished_at, elapsed_sec, rows, error_message, params, extra, flow_run_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            """,
                run_id, task_type, task_id, task_name, "running",
                _now(), None, 0.0, 0, "", params, "", flow_run_id,
            )
        except Exception as e:
            logger.warning(f"TaskRunner.start failed for {run_id}: {e}")

    @staticmethod
    def start_sync(
        run_id: str,
        task_type: str,
        task_id: str,
        task_name: str,
        params: str = "",
        flow_run_id: Optional[int] = None,
    ) -> None:
        """同步版本：写入 task_runs，status=running（用于同步上下文）"""
        try:
            asyncio.run(TaskRunner.start(run_id, task_type, task_id, task_name, params, flow_run_id))
        except Exception as e:
            logger.warning(f"TaskRunner.start_sync failed for {run_id}: {e}")

    @staticmethod
    async def finish(run_id: str, rows: int = 0, elapsed_sec: float = 0.0, extra: str = "") -> None:
        """更新 task_runs：status=success"""
        try:
            from scheduler.db import DatabasePool
            await DatabasePool.execute("""
                UPDATE task_runs
                SET status      = 'success',
                    finished_at = $2,
                    elapsed_sec = $3,
                    rows        = $4,
                    extra       = $5
                WHERE run_id = $1
            """, run_id, _now(), float(elapsed_sec), int(rows), extra or "")
        except Exception as e:
            logger.warning(f"TaskRunner.finish failed for {run_id}: {e}")

    @staticmethod
    async def fail(run_id: str, error: str = "", elapsed_sec: float = 0.0) -> None:
        """更新 task_runs：status=failed"""
        try:
            from scheduler.db import DatabasePool
            await DatabasePool.execute("""
                UPDATE task_runs
                SET status      = 'failed',
                    finished_at = $2,
                    elapsed_sec = $3,
                    error_message = $4
                WHERE run_id = $1
            """, run_id, _now(), float(elapsed_sec), str(error)[:500])
        except Exception as e:
            logger.warning(f"TaskRunner.fail failed for {run_id}: {e}")

    @staticmethod
    async def cleanup_stale(timeout_minutes: int = 0, reason: str = "timeout") -> int:
        """将僵尸 running 记录标记为 failed。

        timeout_minutes=0 清理所有 running 记录（重启场景）。
        timeout_minutes>0 只清理超过指定时间的记录。
        返回清理的记录数。
        """
        try:
            from scheduler.db import DatabasePool
            if timeout_minutes == 0:
                result = await DatabasePool.execute("""
                    UPDATE task_runs
                    SET status      = 'failed',
                        finished_at = $1,
                        error_message = $2
                    WHERE status = 'running'
                """, _now(), f"Task interrupted: {reason}")
            else:
                result = await DatabasePool.execute("""
                    UPDATE task_runs
                    SET status      = 'failed',
                        finished_at = $1,
                        elapsed_sec = EXTRACT(EPOCH FROM ($1 - started_at)),
                        error_message = $2
                    WHERE status = 'running'
                      AND started_at < $1 - ($3 * INTERVAL '1 minute')
                """, _now(), f"Task interrupted: {reason}", timeout_minutes)
            # asyncpg returns "UPDATE N" string
            cleaned = int(result.split()[-1]) if result and result.startswith("UPDATE") else 0
            return cleaned
        except Exception as e:
            logger.warning(f"TaskRunner.cleanup_stale failed: {e}")
            return 0


def tracked_task(task_type: str, task_id_kwarg: str = "task_id") -> Callable:
    """装饰异步后台函数，自动调用 TaskRunner.finish/fail。

    被装饰函数必须是 async def，且接受 run_id 关键字参数。
    API 层负责调用 TaskRunner.start()，装饰器只负责结束状态。

    用法：
        @tracked_task("sync", task_id_kwarg="task_id")
        async def _sync_task_background(task_id: str, ..., run_id: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            run_id: Optional[str] = kwargs.get("run_id")
            t0 = time.time()
            try:
                result = await func(*args, **kwargs)
                elapsed = time.time() - t0
                rows = 0
                extra = ""
                if isinstance(result, dict):
                    rows = result.get("rows", 0) or 0
                    extra_val = result.get("extra")
                    if isinstance(extra_val, dict):
                        extra = json.dumps(extra_val)
                    elif isinstance(extra_val, str):
                        extra = extra_val
                elif hasattr(result, "rows"):
                    rows = result.rows or 0
                    if hasattr(result, "extra"):
                        extra_val = result.extra
                        if isinstance(extra_val, dict):
                            extra = json.dumps(extra_val)
                        elif isinstance(extra_val, str):
                            extra = extra_val
                if run_id:
                    await TaskRunner.finish(run_id, rows=rows, elapsed_sec=elapsed, extra=extra)
                return result
            except Exception as e:
                elapsed = time.time() - t0
                if run_id:
                    await TaskRunner.fail(run_id, error=str(e), elapsed_sec=elapsed)
                raise
        return wrapper
    return decorator
