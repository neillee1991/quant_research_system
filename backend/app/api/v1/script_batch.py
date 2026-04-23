"""批量脚本回测 API

实现 Phase3 中的 3B-4 和 3B-5 任务：
- 批量回测任务创建（参数笛卡尔积）
- 批量结果查询与聚合
"""
import json
import time
import uuid
from itertools import product
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.core.logger import logger
from app.services.task_runner import TaskRunner
from engine.script.validator import validate_script
from engine.script.compiler import compile_script
from engine.script.executor import execute_ir, ExecutionError
from store.dolphindb_client import db_client

router = APIRouter()


class ScriptBatchBacktestRequest(BaseModel):
    """批量脚本回测请求模型"""
    script: str = Field(..., min_length=1, description="策略脚本代码")
    name: str = Field("script_batch_backtest", description="任务名称前缀")
    language: str = Field("python", description="脚本语言")
    entry_point: str = Field("build_strategy", description="入口函数名")
    param_grid: Dict[str, List[Any]] = Field(..., description="参数网格（用于生成笛卡尔积）")
    ts_codes: List[str] = Field(default_factory=list, description="可选：指定股票代码列表，默认使用脚本中定义的")


class ScriptBatchBacktestResponse(BaseModel):
    """批量回测响应模型"""
    batch_id: str
    total_runs: int
    run_ids: List[str]
    task_ids: List[str]
    mode: str = "script_batch"
    status: str = "running"
    message: str = "批量回测任务已提交"
    script_hash: str


class ScriptBatchResult(BaseModel):
    """单个回测结果摘要"""
    run_id: str
    task_id: str
    status: str
    params: Dict[str, Any]
    metrics: Optional[Dict[str, Any]] = None
    equity_curve: Optional[List[Dict[str, Any]]] = None
    trades_sample: Optional[List[Dict[str, Any]]] = None
    warnings: List[str] = Field(default_factory=list)
    error_message: Optional[str] = None
    created_at: Optional[str] = None


class ScriptBatchAggregatedResult(BaseModel):
    """批量回测聚合结果"""
    batch_id: str
    total_runs: int
    completed_runs: int
    failed_runs: int
    running_runs: int
    results: List[ScriptBatchResult]
    best_run: Optional[Dict[str, Any]] = None
    summary: Dict[str, Any]


def _load_data(ts_code: str, start: str, end: str):
    """数据加载函数，复用 strategy.py 中的实现"""
    return db_client.query(
        "SELECT * FROM sync_daily_data WHERE ts_code=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
        [ts_code, start, end],
    )


def _generate_param_combinations(param_grid: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    """生成参数网格的笛卡尔积组合"""
    if not param_grid:
        return [{}]

    # 转换为 (key, values) 列表
    items = list(param_grid.items())
    # 生成笛卡尔积
    keys, values = zip(*items)
    combinations = []
    for combination in product(*values):
        combinations.append(dict(zip(keys, combination)))
    return combinations


async def _run_single_batch_backtest(
    task_id: str,
    ir: dict,
    run_id: str,
    script_hash: str,
    params: Dict[str, Any],
    warnings: List[str],
):
    """执行单个回测任务（后台任务）"""
    from scheduler.db import DatabasePool

    try:
        # 执行回测
        result = execute_ir(ir, _load_data)

        metrics = result.get("metrics", {})
        equity_curve = result.get("equity_curve", [])
        trades = result.get("trades_sample", [])

        # 保存结果到 backtest_results 表（兼容现有表结构）
        try:
            # 尝试插入包含新字段
            await DatabasePool.execute("""
                INSERT INTO backtest_results
                  (run_id, task_id, task_name, metrics_json, equity_curve_json,
                   trades_json, created_at, mode, script_hash, warnings_json, params_json)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT (run_id) DO UPDATE SET
                  metrics_json      = EXCLUDED.metrics_json,
                  equity_curve_json = EXCLUDED.equity_curve_json,
                  trades_json       = EXCLUDED.trades_json
            """,
                run_id, task_id, task_id,
                json.dumps(metrics, default=str),
                json.dumps(equity_curve, default=str),
                json.dumps(trades, default=str),
                time.strftime("%Y-%m-%d %H:%M:%S"), "script_batch", script_hash,
                json.dumps(warnings),
                json.dumps(params),
            )
        except Exception as e:
            # 如果表结构不包含新字段，降级到原来的 schema
            logger.warning(f"Table may not have new fields, falling back: {e}")
            await DatabasePool.execute("""
                INSERT INTO backtest_results
                  (run_id, task_id, task_name, metrics_json, equity_curve_json,
                   trades_json, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (run_id) DO UPDATE SET
                  metrics_json      = EXCLUDED.metrics_json,
                  equity_curve_json = EXCLUDED.equity_curve_json,
                  trades_json       = EXCLUDED.trades_json
            """,
                run_id, task_id, task_id,
                json.dumps(metrics, default=str),
                json.dumps(equity_curve, default=str),
                json.dumps(trades, default=str),
                time.strftime("%Y-%m-%d %H:%M:%S"),
            )

        await TaskRunner.finish(run_id, rows=len(equity_curve))
        return {"rows": len(equity_curve)}

    except Exception as e:
        logger.error(f"Batch backtest failed for {run_id}: {e}")
        await TaskRunner.fail(run_id, error_message=str(e))
        raise


@router.post("/strategy/backtest/script/batch", status_code=202, response_model=ScriptBatchBacktestResponse)
async def create_script_batch_backtest(
    req: ScriptBatchBacktestRequest,
    background_tasks: BackgroundTasks,
):
    """创建批量脚本回测任务

    接收脚本、参数网格和股票代码列表，生成参数笛卡尔积，创建多个回测任务。
    使用 TaskRunner 管理任务状态，返回 batch_id 和任务统计信息。
    """
    # 1. 校验脚本
    val = validate_script(req.script, req.language)
    if not val.valid:
        raise HTTPException(status_code=400, detail="; ".join(val.errors))

    # 2. 编译脚本
    comp = compile_script(req.script, req.language, req.entry_point)
    if not comp.success:
        raise HTTPException(status_code=400, detail="; ".join(comp.errors))

    script_hash = val.script_hash
    batch_id = f"{req.name}_{uuid.uuid4().hex[:8]}"

    # 保存脚本版本到 script_versions 表（ON CONFLICT DO NOTHING 避免重复插入）
    from scheduler.db import DatabasePool
    await DatabasePool.execute("""
        INSERT INTO script_versions
          (script_hash, script_text, name, language, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (script_hash) DO NOTHING
    """, script_hash, req.script, req.name, req.language, time.strftime("%Y-%m-%d %H:%M:%S"))

    # 3. 生成参数组合
    param_combinations = _generate_param_combinations(req.param_grid)

    run_ids = []
    task_ids = []

    # 4. 创建回测任务
    for i, params in enumerate(param_combinations):
        task_id = f"{batch_id}_task_{i}"
        run_id = f"script_batch_{task_id}_{int(time.time() * 1000)}"

        # 启动任务状态追踪
        await TaskRunner.start(
            run_id, "backtest", task_id, f"脚本回测: {req.name} (参数组合 {i+1}/{len(param_combinations)})",
            params=json.dumps({
                "mode": "script_batch",
                "name": req.name,
                "script_hash": script_hash,
                "ir_version": comp.ir.get("version", ""),
                "batch_id": batch_id,
                "params": params,
            }),
        )

        # 添加到后台任务队列
        background_tasks.add_task(
            _run_single_batch_backtest,
            task_id=task_id,
            ir=comp.ir,
            run_id=run_id,
            script_hash=script_hash,
            params=params,
            warnings=comp.warnings,
        )

        run_ids.append(run_id)
        task_ids.append(task_id)

    return ScriptBatchBacktestResponse(
        batch_id=batch_id,
        total_runs=len(param_combinations),
        run_ids=run_ids,
        task_ids=task_ids,
        script_hash=script_hash,
    )


@router.get("/strategy/backtest/script/batch/{batch_id}", response_model=ScriptBatchAggregatedResult)
async def get_script_batch_backtest_result(batch_id: str):
    """查询批量回测结果

    查询指定 batch_id 的所有回测任务结果，聚合并找出最佳结果（按夏普比率排序）。
    """
    from scheduler.db import DatabasePool

    # 查询该批次的所有任务
    task_rows = await DatabasePool.fetch(
        """
        SELECT run_id, status, mode, script_hash, error_message, params
        FROM task_runs
        WHERE params LIKE $1 AND task_type = 'backtest'
        """,
        f"%{batch_id}%",
    )

    if not task_rows:
        raise HTTPException(status_code=404, detail=f"Batch not found: {batch_id}")

    results: List[ScriptBatchResult] = []
    completed_runs = 0
    failed_runs = 0
    running_runs = 0

    # 查询每个任务的详细结果
    for task_row in task_rows:
        run_id = task_row["run_id"]
        status = task_row["status"]
        error_message = task_row["error_message"]
        task_id = run_id.split("_task_")[0] + f"_task_{run_id.split('_task_')[1].split('_')[0]}"

        # 解析任务参数
        params = {}
        if task_row["params"]:
            try:
                params = json.loads(task_row["params"]).get("params", {})
            except Exception:
                pass

        # 查询回测结果
        if status == "running":
            running_runs += 1
            results.append(ScriptBatchResult(
                run_id=run_id,
                task_id=task_id,
                status="running",
                params=params,
                metrics=None,
                equity_curve=None,
                trades_sample=None,
                warnings=[],
                error_message=None,
            ))
            continue

        result_row = await DatabasePool.fetchrow(
            "SELECT * FROM backtest_results WHERE run_id = $1", run_id,
        )

        if status == "failed":
            failed_runs += 1
            results.append(ScriptBatchResult(
                run_id=run_id,
                task_id=task_id,
                status="failed",
                params=params,
                metrics=None,
                equity_curve=None,
                trades_sample=None,
                warnings=[],
                error_message=error_message,
            ))
            continue

        # 成功完成的任务
        completed_runs += 1
        r = dict(result_row)
        results.append(ScriptBatchResult(
            run_id=run_id,
            task_id=task_id,
            status="completed",
            params=params,
            metrics=json.loads(r.get("metrics_json", "{}")),
            equity_curve=json.loads(r.get("equity_curve_json", "[]")),
            trades_sample=json.loads(r.get("trades_json", "[]")),
            warnings=json.loads(r.get("warnings_json", "[]")),
            error_message=None,
            created_at=str(r.get("created_at", "")),
        ))

    # 找出最佳结果（按夏普比率排序）
    best_run = None
    if completed_runs > 0:
        completed_results = [r for r in results if r.status == "completed" and r.metrics]
        if completed_results:
            # 按夏普比率降序排序
            sorted_results = sorted(
                completed_results,
                key=lambda x: x.metrics.get("sharpe_ratio", -float("inf")),
                reverse=True,
            )
            best_run = {
                "run_id": sorted_results[0].run_id,
                "task_id": sorted_results[0].task_id,
                "params": sorted_results[0].params,
                "metrics": sorted_results[0].metrics,
                "sharpe_ratio": sorted_results[0].metrics.get("sharpe_ratio", 0),
            }

    # 生成摘要信息
    summary = {
        "total_runs": len(task_rows),
        "completed_runs": completed_runs,
        "failed_runs": failed_runs,
        "running_runs": running_runs,
        "success_rate": completed_runs / len(task_rows) if len(task_rows) > 0 else 0,
    }

    if completed_runs > 0:
        sharpe_ratios = [r.metrics.get("sharpe_ratio", 0) for r in results if r.status == "completed" and r.metrics]
        total_returns = [r.metrics.get("total_return", 0) for r in results if r.status == "completed" and r.metrics]
        max_drawdowns = [r.metrics.get("max_drawdown", 0) for r in results if r.status == "completed" and r.metrics]

        summary.update({
            "avg_sharpe_ratio": sum(sharpe_ratios) / len(sharpe_ratios),
            "max_sharpe_ratio": max(sharpe_ratios) if sharpe_ratios else 0,
            "min_sharpe_ratio": min(sharpe_ratios) if sharpe_ratios else 0,
            "avg_total_return": sum(total_returns) / len(total_returns),
            "max_total_return": max(total_returns) if total_returns else 0,
            "min_total_return": min(total_returns) if total_returns else 0,
            "avg_max_drawdown": sum(max_drawdowns) / len(max_drawdowns),
        })

    return ScriptBatchAggregatedResult(
        batch_id=batch_id,
        total_runs=len(task_rows),
        completed_runs=completed_runs,
        failed_runs=failed_runs,
        running_runs=running_runs,
        results=results,
        best_run=best_run,
        summary=summary,
    )
