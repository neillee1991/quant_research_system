"""
策略脚本与图模式对账 API (Phase3 3D-12)

提供对比 script 模式与 graph 模式回测结果的接口，
验证两种方式产生的回测指标是否一致。
"""
import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.core.logger import logger
from app.services.task_runner import TaskRunner
from engine.script.validator import validate_script
from engine.script.compiler import compile_script
from engine.script.executor import execute_ir
from engine.parser.flow_parser import FlowParser
from store.dolphindb_client import db_client


router = APIRouter()


class ScriptCrossValidateRequest(BaseModel):
    """图与脚本对账请求"""
    script: str = Field(..., description="策略脚本代码")
    graph: Dict[str, Any] = Field(..., description="React Flow 图定义")
    script_params: Dict[str, Any] = Field(default_factory=dict, description="脚本参数")
    language: str = Field("python", description="脚本语言")
    entry_point: str = Field("build_strategy", description="脚本入口函数")


class MetricsDiff(BaseModel):
    """指标差异"""
    sharpe_delta: float
    return_delta: float
    max_drawdown_delta: float
    win_rate_delta: float
    profit_factor_delta: float
    sharpe_pct_diff: float
    return_pct_diff: float
    match: bool


class ScriptCrossValidateResponse(BaseModel):
    """对账响应"""
    cross_validate_id: str
    status: str = "pending"
    script_metrics: Optional[Dict[str, Any]] = None
    graph_metrics: Optional[Dict[str, Any]] = None
    diff: Optional[MetricsDiff] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


def _load_data(ts_code: str, start: str, end: str):
    """数据加载函数"""
    return db_client.query(
        "SELECT * FROM sync_daily_data WHERE ts_code=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
        [ts_code, start, end],
    )


def _calculate_diff(metrics1: Dict[str, Any], metrics2: Dict[str, Any]) -> MetricsDiff:
    """计算两组指标的差异"""
    tol = 0.01  # 1% 容差

    sharpe_delta = abs(metrics1.get('sharpe_ratio', 0) - metrics2.get('sharpe_ratio', 0))
    return_delta = abs(metrics1.get('annualized_return', 0) - metrics2.get('annualized_return', 0))
    max_drawdown_delta = abs(metrics1.get('max_drawdown', 0) - metrics2.get('max_drawdown', 0))
    win_rate_delta = abs(metrics1.get('win_rate', 0) - metrics2.get('win_rate', 0))
    profit_factor_delta = abs(metrics1.get('profit_factor', 0) - metrics2.get('profit_factor', 0))

    sharpe_denom = max(abs(metrics1.get('sharpe_ratio', 1e-6)), abs(metrics2.get('sharpe_ratio', 1e-6)))
    return_denom = max(abs(metrics1.get('annualized_return', 1e-6)), abs(metrics2.get('annualized_return', 1e-6)))
    sharpe_pct_diff = sharpe_delta / sharpe_denom * 100 if sharpe_denom > 0 else 0
    return_pct_diff = return_delta / return_denom * 100 if return_denom > 0 else 0

    match = all([
        sharpe_pct_diff < tol * 100 or sharpe_delta < 0.05,
        return_pct_diff < tol * 100 or return_delta < 0.01,
        max_drawdown_delta < 0.02,
        win_rate_delta < 0.05,
    ])

    return MetricsDiff(
        sharpe_delta=sharpe_delta,
        return_delta=return_delta,
        max_drawdown_delta=max_drawdown_delta,
        win_rate_delta=win_rate_delta,
        profit_factor_delta=profit_factor_delta,
        sharpe_pct_diff=sharpe_pct_diff,
        return_pct_diff=return_pct_diff,
        match=match,
    )


async def _run_cross_validate_background(
    task_id: str,
    req: ScriptCrossValidateRequest,
    cross_id: str,
):
    """后台执行对账任务"""
    from scheduler.db import DatabasePool

    try:
        # 1. 执行脚本回测
        val_result = validate_script(req.script, req.language)
        if not val_result.valid:
            await TaskRunner.fail(cross_id, error_message=f"脚本校验失败: {';'.join(val_result.errors)}")
            return

        comp_result = compile_script(req.script, req.language, req.entry_point)
        if not comp_result.success:
            await TaskRunner.fail(cross_id, error_message=f"脚本编译失败: {';'.join(comp_result.errors)}")
            return

        script_res = execute_ir(comp_result.ir, _load_data)
        script_metrics = script_res.get('metrics', {})

        # 2. 执行图回测
        parser = FlowParser(df_loader=_load_data)
        graph_res = parser.parse_and_run(req.graph)
        graph_metrics = graph_res.get('metrics', {})

        # 3. 计算差异
        diff = _calculate_diff(script_metrics, graph_metrics)

        # 4. 保存结果（存到一个简单的表或者用 task_runs 的 extra 字段）
        # 这里为了简化，我们直接用 task_runs 的 extra 字段存储
        result_dict = {
            "script_metrics": script_metrics,
            "graph_metrics": graph_metrics,
            "diff": diff.dict(),
        }

        # 更新 task_runs 状态
        await TaskRunner.finish(cross_id, rows=1)
        await DatabasePool.execute(
            "UPDATE task_runs SET extra=$1 WHERE run_id=$2",
            json.dumps(result_dict),
            cross_id,
        )

    except Exception as e:
        logger.error(f"Cross validation failed: {e}", exc_info=True)
        await TaskRunner.fail(cross_id, error_message=str(e))
        raise


@router.post("/strategy/backtest/script/validate-cross", response_model=ScriptCrossValidateResponse)
async def validate_cross(
    req: ScriptCrossValidateRequest,
    background_tasks: BackgroundTasks,
):
    """
    图与脚本模式对账接口

    同时运行 script 和 graph 两种模式的回测，对比结果指标差异。
    返回包含双方指标及差异信息的完整报告。
    """
    # 1. 首先快速校验脚本
    val_result = validate_script(req.script, req.language)
    if not val_result.valid:
        raise HTTPException(status_code=400, detail=f"脚本校验失败: {';'.join(val_result.errors)}")

    # 2. 创建任务 ID
    cross_id = f"cross-validate-{uuid.uuid4().hex[:8]}"

    # 3. 启动任务
    await TaskRunner.start(
        cross_id, "backtest", cross_id, "图与脚本对账",
        params=json.dumps({"mode": "cross-validate", "source": "script-vs-graph"}),
    )

    # 4. 添加到后台任务
    background_tasks.add_task(
        _run_cross_validate_background,
        task_id=cross_id,
        req=req,
        cross_id=cross_id,
    )

    return ScriptCrossValidateResponse(
        cross_validate_id=cross_id,
        status="running",
        started_at=datetime.now().isoformat(),
    )


@router.get("/strategy/backtest/script/validate-cross/{cross_validate_id}", response_model=ScriptCrossValidateResponse)
async def get_cross_validate_result(cross_validate_id: str):
    """查询对账任务结果"""
    from scheduler.db import DatabasePool

    # 查询任务运行状态
    task_row = await DatabasePool.fetchrow(
        "SELECT status, error_message, extra, started_at, finished_at FROM task_runs WHERE run_id = $1",
        cross_validate_id,
    )

    if not task_row:
        raise HTTPException(status_code=404, detail=f"Cross validate task not found: {cross_validate_id}")

    status = task_row.get("status", "unknown")
    error = task_row.get("error_message")
    extra = task_row.get("extra")
    started_at = task_row.get("started_at")
    finished_at = task_row.get("finished_at")

    response = ScriptCrossValidateResponse(
        cross_validate_id=cross_validate_id,
        status=status,
        started_at=started_at.isoformat() if started_at else None,
        finished_at=finished_at.isoformat() if finished_at else None,
    )

    if status == "failed":
        response.error = error

    # 解析额外数据获取详细指标
    if extra and (status == "completed" or status == "success"):
        try:
            extra_data = json.loads(extra)
            response.script_metrics = extra_data.get("script_metrics")
            response.graph_metrics = extra_data.get("graph_metrics")
            if "diff" in extra_data:
                response.diff = MetricsDiff(**extra_data["diff"])
        except Exception as e:
            logger.warning(f"Failed to parse extra data: {e}")

    return response
