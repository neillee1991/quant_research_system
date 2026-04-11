"""因子分析 API 端点"""
import json
import time
import uuid
from datetime import datetime as dt
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel

from store.dolphindb_client import db_client
from engine.analysis.analyzer import FactorAnalyzer
from app.core.logger import logger
from app.core.utils import (
    safe_json_parse,
    DateUtils,
    load_json_from_file,
    parse_json_fields,
)
from app.services.task_runner import TaskRunner, tracked_task

router = APIRouter()
analyzer = FactorAnalyzer(db_client)

# ==================== Helper Functions ====================

def _enhance_analysis_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """增强分析记录：解析 JSON 字段、加载报告、提升行业分析字段"""
    result = dict(record)
    result = parse_json_fields(result, ['ic_summary', 'ic_by_period'])

    if 'config' in result and result['config']:
        config = safe_json_parse(result['config'])
        for key in ['winsorize', 'winsorize_lower', 'winsorize_upper']:
            if key in config and key not in result:
                result[key] = config[key]

    report_path = result.get('report_path')
    if report_path:
        report = load_json_from_file(report_path)
        if report:
            result.update(report)
            charts_data = result.get('charts_data') or {}
            if isinstance(charts_data, dict):
                result.setdefault('ic_by_industry', charts_data.get('ic_by_industry'))
                result.setdefault('returns_by_industry', charts_data.get('returns_by_industry'))

    return result

# ==================== Pydantic Models ====================

class AnalysisRequest(BaseModel):
    factor_id: str
    start_date: str
    end_date: str
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5
    index_pool: Optional[str] = None
    groupby_field: Optional[str] = None
    next_day_entry: bool = True
    entry_price: str = "open"
    neutralize: bool = False
    neutralize_controls: Optional[List[str]] = None
    industry_level: str = "industry_l1"
    winsorize: bool = False
    winsorize_lower: float = 0.01
    winsorize_upper: float = 0.99

# ==================== 后台任务辅助函数 ====================

async def _create_pending_task(task_id: str, req: AnalysisRequest) -> None:
    """在 PostgreSQL factor_analysis_results 写入 pending 占位记录"""
    from scheduler.db import DatabasePool

    config_json = json.dumps({
        "periods": req.periods, "quantiles": req.quantiles,
        "index_pool": req.index_pool, "groupby_field": req.groupby_field,
        "entry_price": req.entry_price, "neutralize": req.neutralize,
        "neutralize_controls": req.neutralize_controls,
        "industry_level": req.industry_level,
        "winsorize": req.winsorize,
        "winsorize_lower": req.winsorize_lower,
        "winsorize_upper": req.winsorize_upper,
    })
    await DatabasePool.execute("""
        INSERT INTO factor_analysis_results
          (factor_id, analysis_date, start_date, end_date, config,
           task_status, task_id, created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (factor_id, analysis_date) DO UPDATE SET
          task_status = EXCLUDED.task_status,
          task_id     = EXCLUDED.task_id
    """,
        req.factor_id, dt.now(),
        req.start_date, req.end_date,
        config_json, "pending", task_id, dt.now(),
    )

async def _update_task_status(task_id: str, status: str, error: Optional[str] = None) -> None:
    """更新 factor_analysis_results 任务状态"""
    from scheduler.db import DatabasePool

    if error:
        await DatabasePool.execute(
            "UPDATE factor_analysis_results SET task_status = $2, error_message = $3 WHERE task_id = $1",
            task_id, status, error,
        )
    else:
        await DatabasePool.execute(
            "UPDATE factor_analysis_results SET task_status = $2 WHERE task_id = $1",
            task_id, status,
        )

async def _save_analysis_result(task_id: str, factor_id: str, results: Dict[str, Any]) -> None:
    """保存分析结果到 PostgreSQL factor_analysis_results"""
    from scheduler.db import DatabasePool
    from app.core.config import settings

    actual_start = results.get("_actual_start")
    actual_end = results.get("_actual_end")
    config = results.get("_config", {})

    # 完整分析报告存文件
    report_fields = [
        'ic_ts', 'quantile_returns', 'cumulative_returns',
        'returns_by_group', 'turnover', 'charts_data',
        'ic_by_group', 'spread_ts', 'alpha_beta',
        'factor_cumulative_returns', 'ic_by_month', 'event_study',
        'ic_by_industry', 'returns_by_industry', 'diagnostics',
        'decay_analysis',
    ]
    report = {k: results.get(k) for k in report_fields if results.get(k) is not None}

    factor_analysis_dir = Path(settings.analysis_dir) / factor_id
    factor_analysis_dir.mkdir(parents=True, exist_ok=True)
    report_path = factor_analysis_dir / f"{task_id}.json"
    report_path.write_text(json.dumps(report), encoding='utf-8')
    logger.info(f"Analysis report saved to {report_path}")

    await DatabasePool.execute("""
        UPDATE factor_analysis_results SET
          start_date   = $2,
          end_date     = $3,
          config       = $4,
          ic_summary   = $5,
          ic_by_period = $6,
          report_path  = $7,
          task_status  = 'completed'
        WHERE task_id = $1
    """,
        task_id,
        DateUtils.normalize_date_to_str(actual_start),
        DateUtils.normalize_date_to_str(actual_end),
        json.dumps(config),
        json.dumps(results.get("ic_summary", {})),
        json.dumps(results.get("ic_by_period", [])),
        str(report_path),
    )
    logger.info(f"Analysis record saved: task_id={task_id}")

@tracked_task("analysis", task_id_kwarg="factor_id")
async def _run_analysis_background(task_id: str, req: AnalysisRequest, run_id: str = None, factor_id: str = None):
    """后台执行分析，更新 task_status"""
    await _update_task_status(task_id, "running")
    results = analyzer.analyze(
        factor_id=req.factor_id,
        start_date=req.start_date,
        end_date=req.end_date,
        periods=req.periods,
        quantiles=req.quantiles,
        index_pool=req.index_pool,
        groupby_field=req.groupby_field,
        next_day_entry=req.next_day_entry,
        entry_price=req.entry_price,
        neutralize=req.neutralize,
        neutralize_controls=req.neutralize_controls,
        industry_level=req.industry_level,
        winsorize=req.winsorize,
        winsorize_lower=req.winsorize_lower,
        winsorize_upper=req.winsorize_upper,
    )
    if results is None:
        raise RuntimeError("分析返回空结果")
    await _save_analysis_result(task_id, req.factor_id, results)
    return {"extra": {"result_id": task_id, "table": "factor_analysis_results"}}

# ==================== API Endpoints ====================

@router.post("/factor/analysis/alphalens", response_model=dict)
async def submit_analysis(req: AnalysisRequest, background_tasks: BackgroundTasks):
    """提交因子分析任务（异步）。立即返回 task_id，后台执行分析。"""
    task_id = str(uuid.uuid4())
    run_id = f"analysis_{task_id[:12]}"
    await TaskRunner.start(
        run_id, "analysis", req.factor_id, f"因子分析: {req.factor_id}",
        params=json.dumps({"start_date": req.start_date, "end_date": req.end_date, "periods": req.periods})
    )
    await _create_pending_task(task_id, req)
    background_tasks.add_task(
        _run_analysis_background,
        task_id=task_id, req=req, run_id=run_id, factor_id=req.factor_id
    )
    return {
        "status": "success",
        "data": {"task_id": task_id, "factor_id": req.factor_id, "status": "pending"}
    }

@router.get("/factor/analysis/status/{task_id}")
async def get_analysis_status(task_id: str):
    """查询分析任务状态"""
    status = analyzer.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
    return {"status": "success", "data": status}

@router.get("/factor/analysis/{factor_id}/latest")
async def get_latest_alphalens_analysis(factor_id: str):
    """获取指定因子的最新 Alphalens 分析结果"""
    from scheduler.db import DatabasePool

    try:
        row = await DatabasePool.fetchrow("""
            SELECT * FROM factor_analysis_results
            WHERE factor_id = $1
            ORDER BY analysis_date DESC
            LIMIT 1
        """, factor_id)

        if not row:
            raise HTTPException(status_code=404, detail=f"未找到因子 {factor_id} 的分析结果")

        record = _enhance_analysis_record(dict(row))
        for ts_field in ["analysis_date", "created_at"]:
            if record.get(ts_field):
                record[ts_field] = str(record[ts_field])
        return {"status": "success", "data": record}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get latest analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/factor/analysis/{factor_id}/detail/{analysis_id}")
async def get_alphalens_analysis_by_id(factor_id: str, analysis_id: str):
    """按 task_id 获取指定 Alphalens 分析结果"""
    from scheduler.db import DatabasePool

    try:
        row = await DatabasePool.fetchrow("""
            SELECT * FROM factor_analysis_results
            WHERE factor_id = $1 AND task_id = $2
            LIMIT 1
        """, factor_id, analysis_id)

        if not row:
            raise HTTPException(status_code=404, detail=f"未找到分析记录 {analysis_id}")

        record = _enhance_analysis_record(dict(row))
        for ts_field in ["analysis_date", "created_at"]:
            if record.get(ts_field):
                record[ts_field] = str(record[ts_field])
        return {"status": "success", "data": record}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get analysis by id: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/factor/analysis/{factor_id}/detail/{analysis_id}")
async def delete_alphalens_analysis_by_id(factor_id: str, analysis_id: str):
    """删除指定的 Alphalens 分析结果"""
    from scheduler.db import DatabasePool

    try:
        row = await DatabasePool.fetchrow("""
            SELECT id, factor_id, report_path FROM factor_analysis_results
            WHERE factor_id = $1 AND task_id = $2
            LIMIT 1
        """, factor_id, analysis_id)

        if not row:
            raise HTTPException(status_code=404, detail=f"未找到分析记录 {analysis_id}")

        report_path = row.get('report_path')
        if report_path:
            try:
                Path(report_path).unlink()
                logger.info(f"Deleted analysis report file: {report_path}")
            except Exception as e:
                logger.warning(f"Failed to delete report file: {e}")

        await DatabasePool.execute(
            "DELETE FROM factor_analysis_results WHERE task_id = $1", analysis_id
        )
        return {"status": "success", "data": {"message": "分析记录已删除"}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/factor/analysis/trading-days")
async def get_trading_days(start: str, end: str):
    """获取指定范围内的交易日列表（YYYYMMDD 格式）"""
    from app.core.utils import TradingCalendar
    cal = TradingCalendar.get_instance(db_client)
    days = cal.get_trading_days(start, end)
    return {"status": "success", "data": days}
