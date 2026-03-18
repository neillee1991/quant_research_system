"""因子分析 API 端点"""
import json
import time
from datetime import datetime as dt
from typing import Optional, List

import polars as pl
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from store.dolphindb_client import db_client
from engine.analysis.analyzer import FactorAnalyzer
from app.core.logger import logger
from app.core.utils import safe_json_parse

router = APIRouter()
analyzer = FactorAnalyzer(db_client)


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


# ==================== 后台任务辅助函数 ====================

def _create_pending_task(task_id: int, req: AnalysisRequest):
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
        }),
        "task_status": "pending",
        "task_id": str(task_id),
        "error_message": None,
        "ic_summary": None, "ic_by_period": None, "ic_ts": None,
        "quantile_returns": None, "cumulative_returns": None,
        "ic_by_group": None, "returns_by_group": None,
        "turnover": None, "decay_analysis": None, "charts_data": None,
    }
    df = pl.DataFrame([record])
    db_client.upsert("factor_analysis_extended", df, key_columns=["id"])


def _update_task_status(task_id: int, status: str, error: Optional[str] = None):
    """更新任务状态（upsert 方式）"""
    try:
        df = db_client.query("""
            SELECT * FROM factor_analysis_extended WHERE id = %s
        """, (task_id,))
        if df.is_empty():
            return
        row = df.to_dicts()[0]
        row["task_status"] = status
        row["error_message"] = error
        updated_df = pl.DataFrame([row])
        db_client.upsert("factor_analysis_extended", updated_df, key_columns=["id"])
    except Exception as e:
        logger.error(f"Failed to update task status {task_id}: {e}")


def _run_analysis_background(task_id: int, req: AnalysisRequest):
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
        )
        if results is None:
            _update_task_status(task_id, "failed", error="分析返回空结果")
        else:
            _update_task_status(task_id, "completed")
    except Exception as e:
        logger.error(f"Background analysis failed for task {task_id}: {e}")
        _update_task_status(task_id, "failed", error=str(e))


# ==================== API Endpoints ====================

@router.post("/analysis/alphalens", response_model=dict)
async def submit_analysis(req: AnalysisRequest, background_tasks: BackgroundTasks):
    """提交因子分析任务（异步）。立即返回 task_id，后台执行分析。"""
    task_id = int(time.time() * 1000)
    _create_pending_task(task_id, req)
    background_tasks.add_task(_run_analysis_background, task_id=task_id, req=req)
    return {
        "status": "success",
        "data": {"task_id": task_id, "factor_id": req.factor_id, "status": "pending"}
    }


@router.get("/analysis/alphalens/status/{task_id}")
async def get_analysis_status(task_id: int):
    """查询分析任务状态"""
    status = analyzer.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
    return {"status": "success", "data": status}


@router.get("/analysis/alphalens/{factor_id}/latest")
async def get_latest_alphalens_analysis(factor_id: str):
    """获取指定因子的最新 Alphalens 分析结果

    Args:
        factor_id: 因子ID

    Returns:
        最新的分析结果，包含完整的 Alphalens 输出
    """
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

        json_fields = [
            'ic_summary', 'ic_by_period', 'ic_ts', 'quantile_returns',
            'cumulative_returns', 'ic_by_group', 'returns_by_group',
            'turnover', 'decay_analysis', 'charts_data'
        ]

        for field in json_fields:
            if field in record and record[field]:
                try:
                    record[field] = safe_json_parse(record[field])
                except Exception as e:
                    logger.warning(f"Failed to parse {field} for {factor_id}: {e}")
                    record[field] = None

        # 从 charts_data 中提取行业分析字段，提升为顶层字段
        charts_data = record.get('charts_data') or {}
        if isinstance(charts_data, dict):
            if 'ic_by_industry' in charts_data:
                record['ic_by_industry'] = charts_data['ic_by_industry']
            if 'returns_by_industry' in charts_data:
                record['returns_by_industry'] = charts_data['returns_by_industry']

        return {
            "status": "success",
            "data": record
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get latest analysis: {e}")
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
                config, task_status
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
                try:
                    config = safe_json_parse(record['config'])
                    record['periods'] = config.get('periods')
                    record['quantiles'] = config.get('quantiles')
                    record['index_pool'] = config.get('index_pool')
                    record['groupby_field'] = config.get('groupby_field')
                except Exception as e:
                    logger.debug(f"Failed to parse analysis config: {e}")
                    record['periods'] = None
                    record['quantiles'] = None
                    record['index_pool'] = None
                    record['groupby_field'] = None

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
