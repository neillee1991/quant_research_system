"""因子分析 API 端点"""
import json
import time
import uuid
from datetime import datetime as dt
from typing import Optional, List, Dict, Any
from pathlib import Path

import polars as pl
from fastapi import APIRouter, HTTPException, BackgroundTasks
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

router = APIRouter()
analyzer = FactorAnalyzer(db_client)


# ==================== Helper Functions ====================

def _enhance_analysis_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """增强分析记录：解析 JSON 字段、加载报告、提升行业分析字段"""
    result = dict(record)

    # 解析 JSON 字段
    result = parse_json_fields(result, ['ic_summary', 'ic_by_period'])

    # 从 config 中提取配置到顶层（用于历史列表展示）
    if 'config' in result and result['config']:
        config = safe_json_parse(result['config'])
        for key in ['winsorize', 'winsorize_lower', 'winsorize_upper']:
            if key in config and key not in result:
                result[key] = config[key]

    # 从文件加载完整报告
    report_path = result.get('report_path')
    if report_path:
        report = load_json_from_file(report_path)
        if report:
            result.update(report)
            # 提升行业分析字段到顶层
            charts_data = result.get('charts_data') or {}
            if isinstance(charts_data, dict):
                result.setdefault('ic_by_industry', charts_data.get('ic_by_industry'))
                result.setdefault('returns_by_industry', charts_data.get('returns_by_industry'))

    return result


# ==================== Pydantic Models ====================

class AnalysisRequest(BaseModel):
    """因子分析请求"""
    factor_id: str
    start_date: str
    end_date: str
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5
    index_pool: Optional[str] = None
    groupby_field: Optional[str] = None
    # 买入时点控制
    next_day_entry: bool = True
    entry_price: str = "open"   # "open" | "close" | "high" | "low"
    # 因子中性化
    neutralize: bool = False
    neutralize_controls: Optional[List[str]] = None  # ["market", "industry", "size"]
    industry_level: str = "industry_l1"  # "industry_l1" | "industry_l2"
    # 极端值处理
    winsorize: bool = False
    winsorize_lower: float = 0.01  # 下界分位数
    winsorize_upper: float = 0.99  # 上界分位数


# ==================== 后台任务辅助函数 ====================

def _create_pending_task(task_id: str, req: AnalysisRequest):
    """在 DB 写入 pending 占位记录"""
    record = {
        "id": task_id,
        "factor_id": req.factor_id,
        "analysis_date": dt.now(),
        "start_date": dt.strptime(req.start_date, "%Y%m%d").date(),
        "end_date": dt.strptime(req.end_date, "%Y%m%d").date(),
        "config": json.dumps({
            "periods": req.periods,
            "quantiles": req.quantiles,
            "index_pool": req.index_pool,
            "groupby_field": req.groupby_field,
            "entry_price": req.entry_price,
            "neutralize": req.neutralize,
            "neutralize_controls": req.neutralize_controls,
            "industry_level": req.industry_level,
            "winsorize": req.winsorize,
            "winsorize_lower": req.winsorize_lower,
            "winsorize_upper": req.winsorize_upper,
        }),
        "ic_summary": None,
        "ic_by_period": None,
        "decay_analysis": None,
        "report_path": None,
        "task_status": "pending",
        "task_id": task_id,
        "error_message": None,
    }
    df = pl.DataFrame([record])
    db_client.upsert("factor_analysis_extended", df, key_columns=["id"])


def _update_task_status(task_id: str, status: str, error: Optional[str] = None):
    """更新任务状态"""
    try:
        df = db_client.query(
            "SELECT id, factor_id, analysis_date, start_date, end_date, config, "
            "ic_summary, ic_by_period, decay_analysis, report_path, task_id "
            "FROM loadTable(\"dfs://quant\", \"factor_analysis_extended\") WHERE id = %s",
            (task_id,)
        )
        if df.is_empty():
            return
        row = df.to_dicts()[0]
        row["task_status"] = status
        row["error_message"] = error
        db_client.upsert("factor_analysis_extended", pl.DataFrame([row]), key_columns=["id"])
    except Exception as e:
        logger.error(f"Failed to update task status {task_id}: {e}")


def _save_analysis_result(
    task_id: str,
    factor_id: str,
    results: Dict[str, Any],
):
    """保存分析结果 - 使用 upsert，如果记录不存在则创建"""
    from app.core.config import settings

    actual_start = results.get("_actual_start")
    actual_end = results.get("_actual_end")
    config = results.get("_config", {})

    # 1. 完整分析报告存文件
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

    # 2. 直接使用 upsert 更新或创建记录
    try:
        # 先尝试查询现有记录
        df = db_client.query(
            "SELECT * FROM loadTable(\"dfs://quant\", \"factor_analysis_extended\") WHERE id = %s",
            (task_id,)
        )

        if not df.is_empty():
            # 更新现有记录
            row = df.to_dicts()[0]
            logger.info(f"Updating existing record: id={task_id}")
        else:
            # 创建新记录（兜底方案）
            logger.warning(f"Record {task_id} not found, creating new one")
            row = {
                "id": task_id,
                "factor_id": factor_id,
                "analysis_date": dt.now(),
                "decay_analysis": None,
                "error_message": None,
            }

        # 更新字段
        row["start_date"] = DateUtils.normalize_date_to_object(actual_start)
        row["end_date"] = DateUtils.normalize_date_to_object(actual_end)
        row["config"] = json.dumps(config)
        row["ic_summary"] = json.dumps(results.get("ic_summary", {}))
        row["ic_by_period"] = json.dumps(results.get("ic_by_period", []))
        row["report_path"] = str(report_path)
        row["task_status"] = "completed"
        row["task_id"] = task_id

        db_client.upsert("factor_analysis_extended", pl.DataFrame([row]), key_columns=["id"])
        logger.info(f"Analysis record saved: id={task_id}")
    except Exception as e:
        logger.error(f"Failed to save analysis record {task_id}: {e}")
        import traceback
        traceback.print_exc()


def _run_analysis_background(task_id: str, req: AnalysisRequest):
    """后台执行分析，更新 task_status"""
    try:
        _update_task_status(task_id, "running")
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
            _update_task_status(task_id, "failed", error="分析返回空结果")
        else:
            _save_analysis_result(task_id, req.factor_id, results)
    except Exception as e:
        logger.error(f"Background analysis failed for task {task_id}: {e}")
        import traceback
        traceback.print_exc()
        _update_task_status(task_id, "failed", error=str(e))


# ==================== API Endpoints ====================

@router.post("/analysis/alphalens", response_model=dict)
async def submit_analysis(req: AnalysisRequest, background_tasks: BackgroundTasks):
    """提交因子分析任务（异步）。立即返回 task_id，后台执行分析。"""
    # 使用 UUID 作为任务 ID，避免冲突
    task_id = str(uuid.uuid4())
    _create_pending_task(task_id, req)
    background_tasks.add_task(_run_analysis_background, task_id=task_id, req=req)
    return {
        "status": "success",
        "data": {"task_id": task_id, "factor_id": req.factor_id, "status": "pending"}
    }


@router.get("/analysis/alphalens/status/{task_id}")
async def get_analysis_status(task_id: str):
    """查询分析任务状态"""
    status = analyzer.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
    return {"status": "success", "data": status}


@router.get("/analysis/alphalens/{factor_id}/latest")
async def get_latest_alphalens_analysis(factor_id: str):
    """获取指定因子的最新 Alphalens 分析结果（完整详情）"""
    try:
        df = db_client.query("""
            SELECT *
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s
            ORDER BY analysis_date DESC
            LIMIT 1
        """, (factor_id,))

        if df.is_empty():
            raise HTTPException(
                status_code=404,
                detail=f"未找到因子 {factor_id} 的分析结果"
            )

        record = df.to_dicts()[0]
        record = _enhance_analysis_record(record)

        return {"status": "success", "data": record}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get latest analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/alphalens/{factor_id}/detail/{analysis_id}")
async def get_alphalens_analysis_by_id(factor_id: str, analysis_id: str):
    """按 id 获取指定 Alphalens 分析结果（完整详情）"""
    try:
        df = db_client.query("""
            SELECT *
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s AND id = %s
            LIMIT 1
        """, (factor_id, analysis_id))

        if df.is_empty():
            raise HTTPException(status_code=404, detail=f"未找到分析记录 {analysis_id}")

        record = df.to_dicts()[0]
        record = _enhance_analysis_record(record)

        return {"status": "success", "data": record}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get analysis by id: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/alphalens/{factor_id}/history")
async def get_alphalens_analysis_history(
    factor_id: str,
    limit: int = 20,
    offset: int = 0
):
    """获取指定因子的 Alphalens 分析历史记录

    Args:
        factor_id: 因子ID
        limit: 返回记录数量
        offset: 偏移量

    Returns:
        历史分析记录列表（不包含完整结果，仅元数据）
    """
    try:
        df = db_client.query("""
            SELECT
                id, factor_id, analysis_date, start_date, end_date,
                config, task_status, ic_summary, ic_by_period, decay_analysis
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s
            ORDER BY analysis_date DESC
            LIMIT %s, %s
        """, (factor_id, offset, limit))

        if df.is_empty():
            return {
                "status": "success",
                "data": {
                    "records": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset
                }
            }

        records = df.to_dicts()

        for record in records:
            if 'config' in record and record['config']:
                config = safe_json_parse(record['config'])
                record['periods'] = config.get('periods')
                record['quantiles'] = config.get('quantiles')
                record['index_pool'] = config.get('index_pool')
                record['groupby_field'] = config.get('groupby_field')
                record['entry_price'] = config.get('entry_price')
                record['neutralize'] = config.get('neutralize')
                record['neutralize_controls'] = config.get('neutralize_controls')
                record['industry_level'] = config.get('industry_level')
                record['winsorize'] = config.get('winsorize')
                record['winsorize_lower'] = config.get('winsorize_lower')
                record['winsorize_upper'] = config.get('winsorize_upper')

        count_df = db_client.query("""
            SELECT COUNT(*) as total
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s
        """, (factor_id,))
        total = count_df.to_dicts()[0]['total'] if not count_df.is_empty() else 0

        return {
            "status": "success",
            "data": {
                "records": records,
                "total": total,
                "limit": limit,
                "offset": offset
            }
        }

    except Exception as e:
        logger.error(f"Failed to get analysis history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/analysis/alphalens/{factor_id}/detail/{analysis_id}")
async def delete_alphalens_analysis_by_id(factor_id: str, analysis_id: str):
    """删除指定的 Alphalens 分析结果"""
    try:
        # 首先查询要删除的记录
        df = db_client.query("""
            SELECT id, factor_id, report_path
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s AND id = %s
            LIMIT 1
        """, (factor_id, analysis_id))

        if df.is_empty():
            raise HTTPException(status_code=404, detail=f"未找到分析记录 {analysis_id}")

        record = df.to_dicts()[0]

        # 删除文件（如果存在）
        report_path = record.get('report_path')
        if report_path:
            p = Path(report_path)
            try:
                p.unlink()
                logger.info(f"Deleted analysis report file: {report_path}")
            except Exception as e:
                logger.warning(f"Failed to delete report file: {e}")

        # 删除数据库记录
        db_client.query("""
            DELETE FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE id = %s
        """, (analysis_id,))

        return {"status": "success", "data": {"message": "分析记录已删除"}}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/trading-days")
async def get_trading_days(start: str, end: str):
    """获取指定范围内的交易日列表（YYYYMMDD 格式）"""
    from app.core.utils import TradingCalendar
    cal = TradingCalendar.get_instance(db_client)
    days = cal.get_trading_days(start, end)
    return {"status": "success", "data": days}
