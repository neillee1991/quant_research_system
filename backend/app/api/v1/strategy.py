import json
import time
import uuid
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.core.logger import logger
from app.services.task_runner import TaskRunner, tracked_task
from engine.parser.flow_parser import FlowParser
from engine.script.validator import validate_script
from engine.script.compiler import compile_script
from engine.script.executor import execute_ir, ExecutionError
from store.dolphindb_client import db_client

router = APIRouter()


class BacktestRequest(BaseModel):
    graph: dict[str, Any]


class ScriptValidateRequest(BaseModel):
    script: str = Field(..., min_length=1)
    language: str = "python"


class ScriptCompileRequest(BaseModel):
    script: str = Field(..., min_length=1)
    language: str = "python"
    entry_point: str = "build_strategy"


class ScriptBacktestRequest(BaseModel):
    script: str = Field(..., min_length=1)
    name: str = "script_backtest"
    language: str = "python"
    entry_point: str = "build_strategy"
    params: dict[str, Any] = Field(default_factory=dict)


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


# ── 脚本校验（真实 AST 白名单） ────────────────────────────────

@router.post("/strategy/backtest/script/validate")
def validate_backtest_script(req: ScriptValidateRequest):
    result = validate_script(req.script, req.language)
    return {
        "valid": result.valid,
        "language": result.language,
        "script_hash": result.script_hash,
        "warnings": result.warnings,
        "errors": result.errors,
    }


# ── 脚本编译（受限执行 + IR 产出） ─────────────────────────────

@router.post("/strategy/backtest/script/compile")
def compile_backtest_script(req: ScriptCompileRequest):
    result = compile_script(req.script, req.language, req.entry_point)
    return {
        "status": "compiled" if result.success else "failed",
        "script_hash": result.script_hash,
        "ir": result.ir,
        "warnings": result.warnings,
        "errors": result.errors,
    }


# ── 脚本回测（异步提交） ───────────────────────────────────────

@router.post("/strategy/backtest/script", status_code=202)
async def run_script_backtest(req: ScriptBacktestRequest, background_tasks: BackgroundTasks):
    # 先校验
    val = validate_script(req.script, req.language)
    if not val.valid:
        raise HTTPException(status_code=400, detail="; ".join(val.errors))

    # 再编译
    comp = compile_script(req.script, req.language, req.entry_point)
    if not comp.success:
        raise HTTPException(status_code=400, detail="; ".join(comp.errors))

    script_hash = val.script_hash
    task_id = f"{req.name}_{uuid.uuid4().hex[:8]}"
    run_id = f"script_{task_id}_{int(time.time() * 1000)}"

    # 保存脚本版本到 script_versions 表（ON CONFLICT DO NOTHING 避免重复插入）
    from scheduler.db import DatabasePool
    await DatabasePool.execute("""
        INSERT INTO script_versions
          (script_hash, script_text, name, language, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (script_hash) DO NOTHING
    """, script_hash, req.script, req.name, req.language, datetime.now())

    await TaskRunner.start(
        run_id, "backtest", task_id, f"脚本回测: {req.name}",
        params=json.dumps({
            "mode": "script", "name": req.name,
            "script_hash": script_hash, "ir_version": comp.ir.get("version", ""),
        }),
    )

    background_tasks.add_task(
        _run_script_backtest_background,
        task_id=task_id,
        ir=comp.ir,
        run_id=run_id,
        script_hash=script_hash,
        warnings=comp.warnings,
    )

    return {
        "run_id": run_id,
        "task_id": task_id,
        "mode": "script",
        "status": "running",
        "message": "脚本回测任务已提交",
        "script_hash": script_hash,
    }


async def _run_script_backtest_background(
    task_id: str, ir: dict, run_id: str, script_hash: str, warnings: list[str],
):
    """后台执行脚本回测，结果写入 backtest_results"""
    from scheduler.db import DatabasePool

    try:
        result = execute_ir(ir, _load_data)

        metrics = result.get("metrics", {})
        equity_curve = result.get("equity_curve", [])
        trades = result.get("trades_sample", [])

        await DatabasePool.execute("""
            INSERT INTO backtest_results
              (run_id, task_id, task_name, metrics_json, equity_curve_json,
               trades_json, created_at, mode, script_hash, warnings_json)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (run_id) DO UPDATE SET
              metrics_json      = EXCLUDED.metrics_json,
              equity_curve_json = EXCLUDED.equity_curve_json,
              trades_json       = EXCLUDED.trades_json
        """,
            run_id, task_id, task_id,
            json.dumps(metrics, default=str),
            json.dumps(equity_curve, default=str),
            json.dumps(trades, default=str),
            datetime.now(), "script", script_hash,
            json.dumps(warnings),
        )

        await TaskRunner.finish(run_id, rows=len(equity_curve))
        return {"rows": len(equity_curve)}

    except Exception as e:
        logger.error(f"Script backtest failed for {run_id}: {e}")
        await TaskRunner.fail(run_id, error_message=str(e))
        raise


# ── 统一运行结果查询（task_runs + backtest_results 联合） ───────

@router.get("/strategy/backtest/runs/{run_id}")
async def get_unified_backtest_run(run_id: str):
    from scheduler.db import DatabasePool

    # 先查 task_runs 拿状态
    task_row = await DatabasePool.fetchrow(
        "SELECT status, mode, script_hash, error_message FROM task_runs WHERE run_id = $1",
        run_id,
    )

    if not task_row:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    task = dict(task_row)
    mode = task.get("mode") or "graph"

    if task["status"] == "running":
        return {
            "run_id": run_id,
            "mode": mode,
            "status": "running",
            "message": "回测执行中...",
            "script_hash": task.get("script_hash"),
        }

    if task["status"] == "failed":
        return {
            "run_id": run_id,
            "mode": mode,
            "status": "failed",
            "error": task.get("error_message", "未知错误"),
            "script_hash": task.get("script_hash"),
        }

    # 已完成，查 backtest_results 拿详情
    result_row = await DatabasePool.fetchrow(
        "SELECT * FROM backtest_results WHERE run_id = $1", run_id,
    )
    if not result_row:
        raise HTTPException(status_code=404, detail=f"Result not found: {run_id}")

    r = dict(result_row)
    return {
        "run_id": r["run_id"],
        "task_id": r["task_id"],
        "task_name": r["task_name"],
        "mode": r.get("mode", mode),
        "status": "completed",
        "metrics": json.loads(r.get("metrics_json") or "{}"),
        "equity_curve": json.loads(r.get("equity_curve_json") or "[]"),
        "trades_sample": json.loads(r.get("trades_json") or "[]"),
        "warnings": json.loads(r.get("warnings_json") or "[]"),
        "script_hash": r.get("script_hash"),
        "created_at": str(r.get("created_at", "")),
    }


# ── 图模式回测（保持不变） ──────────────────────────────────────

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
          (run_id, task_id, task_name, metrics_json, equity_curve_json,
           trades_json, created_at, mode)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (run_id) DO UPDATE SET
          metrics_json      = EXCLUDED.metrics_json,
          equity_curve_json = EXCLUDED.equity_curve_json,
          trades_json       = EXCLUDED.trades_json
    """,
        run_id, task_id, task_id,
        json.dumps(metrics, default=str),
        json.dumps(equity_curve, default=str),
        json.dumps(trades, default=str),
        datetime.now(), "graph",
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
                           params=json.dumps({"name": name, "mode": "graph"}))

    background_tasks.add_task(
        _run_backtest_background,
        task_id=task_id,
        graph=graph,
        run_id=run_id,
    )

    return {"run_id": run_id, "task_id": task_id, "status": "running"}


@router.get("/strategy/backtest/{run_id}/result")
async def get_backtest_result(run_id: str):
    """查询回测结果（从 PostgreSQL backtest_results 表）"""
    from scheduler.db import DatabasePool

    row = await DatabasePool.fetchrow(
        "SELECT * FROM backtest_results WHERE run_id = $1", run_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Result not found for run_id: {run_id}")

    r = dict(row)
    return {
        "run_id": r["run_id"],
        "task_id": r["task_id"],
        "task_name": r["task_name"],
        "mode": r.get("mode", "graph"),
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
