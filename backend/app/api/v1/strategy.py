import json
import time
import uuid
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel

from app.core.logger import logger
from app.services.task_runner import TaskRunner, tracked_task
from engine.backtester.vector_engine import BacktestConfig, VectorEngine
from engine.parser.flow_parser import FlowParser
from store.dolphindb_client import db_client

router = APIRouter()


class BacktestRequest(BaseModel):
    graph: dict[str, Any]


class SimpleBacktestRequest(BaseModel):
    ts_code: str
    start_date: str = "20200101"
    end_date: str = "20241231"
    signal_col: str = "signal"
    commission_rate: float = 0.0003
    slippage_rate: float = 0.0001
    initial_capital: float = 1_000_000.0


def _load_data(ts_code: str, start: str, end: str):
    return db_client.query(
        "SELECT * FROM sync_daily_data WHERE ts_code=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
        [ts_code, start, end],
    )


@tracked_task("backtest", task_id_kwarg="task_id")
async def _run_backtest_background(task_id: str, graph: dict, run_id: str):
    """后台执行回测，结果写入 PostgreSQL backtest_results"""
    from scheduler.db import DatabasePool

    parser = FlowParser(df_loader=_load_data)
    result = parser.parse_and_run(graph)

    metrics = result.get("metrics", {})
    equity_curve = result.get("equity_curve", [])
    trades = result.get("trades", result.get("trades_sample", []))

    await DatabasePool.execute("""
        INSERT INTO backtest_results
          (run_id, task_id, task_name, metrics_json, equity_curve_json, trades_json, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (run_id) DO UPDATE SET
          metrics_json      = EXCLUDED.metrics_json,
          equity_curve_json = EXCLUDED.equity_curve_json,
          trades_json       = EXCLUDED.trades_json
    """,
        run_id, task_id, task_id,
        json.dumps(metrics, default=str),
        json.dumps(equity_curve, default=str),
        json.dumps(trades, default=str),
        datetime.now(),
    )

    return {
        "rows": len(equity_curve),
        "extra": {"result": {"type": "table", "table": "backtest_results"}},
    }


@router.post("/strategy/backtest/async")
async def backtest_async(request: dict, background_tasks: BackgroundTasks):
    """异步回测 - 立即返回 run_id，后台执行，结果持久化到 backtest_results 表"""
    name = request.get("name", "backtest")
    graph = request.get("graph", {})

    task_id = f"{name}_{uuid.uuid4().hex[:8]}"
    run_id = f"{task_id}_{int(time.time() * 1000)}"

    await TaskRunner.start(run_id, "backtest", task_id, f"回测: {name}",
                           params=json.dumps({"name": name}))

    background_tasks.add_task(
        _run_backtest_background,
        task_id=task_id,
        graph=graph,
        run_id=run_id,
    )

    return {"run_id": run_id, "task_id": task_id, "status": "running"}


@router.get("/strategy/backtest/history")
async def get_backtest_history(limit: int = Query(default=20, le=100)):
    """查询回测历史（从 PostgreSQL task_runs 表）"""
    from scheduler.db import DatabasePool

    rows = await DatabasePool.fetch(
        "SELECT * FROM task_runs WHERE task_type = 'backtest' ORDER BY started_at DESC LIMIT $1",
        limit,
    )
    tasks = []
    for row in rows:
        r = dict(row)
        for field in ["started_at", "finished_at"]:
            if field in r and r[field]:
                r[field] = str(r[field])
        tasks.append(r)
    return {"tasks": tasks, "total": len(tasks)}


@router.get("/strategy/backtest/{run_id}/result")
async def get_backtest_result(run_id: str):
    """查询回测结果（从 PostgreSQL backtest_results 表）"""
    from scheduler.db import DatabasePool

    row = await DatabasePool.fetchrow(
        "SELECT * FROM backtest_results WHERE run_id = $1", run_id
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Result not found for run_id: {run_id}")

    r = dict(row)
    return {
        "run_id": r["run_id"],
        "task_id": r["task_id"],
        "task_name": r["task_name"],
        "metrics": json.loads(r.get("metrics_json") or "{}"),
        "equity_curve": json.loads(r.get("equity_curve_json") or "[]"),
        "trades_sample": json.loads(r.get("trades_json") or "[]"),
        "created_at": str(r.get("created_at", "")),
    }


@router.post("/strategy/backtest")
def run_backtest(req: BacktestRequest):
    """Run a backtest from a React Flow graph JSON."""
    try:
        parser = FlowParser(df_loader=_load_data)
        result = parser.parse_and_run(req.graph)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategy/operators")
def list_operators():
    """Return available operator definitions for the frontend node palette."""
    from engine.parser.flow_parser import OPERATOR_REGISTRY
    return {"operators": OPERATOR_REGISTRY}
