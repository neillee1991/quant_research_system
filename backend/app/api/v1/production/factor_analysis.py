"""因子分析 API 端点"""
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from store.dolphindb_client import db_client
from engine.analysis.analyzer import FactorAnalyzer
from app.core.logger import logger
from app.core.utils import safe_json_parse

router = APIRouter()
analyzer = FactorAnalyzer(db_client)


# ==================== Pydantic Models ====================

class AnalyzeRequest(BaseModel):
    """因子分析请求"""
    factor_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5


class AlphalensAnalysisRequest(BaseModel):
    """Alphalens 分析请求"""
    factor_id: str
    start_date: str
    end_date: str
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5
    index_pool: Optional[str] = None
    groupby_field: Optional[str] = None


# ==================== Helper Functions ====================

def _format_analysis_summary(summary: dict) -> dict:
    """将 analyzer.analyze() 返回的 summary 转换为前端期望的格式"""
    ic_summary = []
    layer_returns = []

    for period_str, pdata in summary.get("periods", {}).items():
        period = int(period_str)
        ic_summary.append({
            "period": period,
            "ic_mean": pdata.get("ic_mean", 0),
            "ic_std": pdata.get("ic_std", 0),
            "icir": pdata.get("ic_ir", 0),
            "ic_positive_ratio": pdata.get("ic_positive_ratio", 0),
            "long_short_return": pdata.get("long_short_return", 0),
        })
        for qr in pdata.get("quantile_returns", []):
            layer_returns.append({
                "period": period,
                "quantile": qr.get("quantile", ""),
                "mean_return": qr.get("avg_return", 0),
            })

    ic_summary.sort(key=lambda x: x["period"])
    return {
        "factor_id": summary.get("factor_id"),
        "ic_summary": ic_summary,
        "layer_returns": layer_returns,
        "turnover": summary.get("turnover"),
        "ic_mean": summary.get("ic_mean", 0),
        "ic_std": summary.get("ic_std", 0),
        "ic_ir": summary.get("ic_ir", 0),
    }


def _format_db_analysis(row: dict) -> dict:
    """将 DB 行记录转换为前端期望的格式"""
    ic_summary = []
    layer_returns = []

    periods = safe_json_parse(row.get("periods"), default=[])
    quantile_returns = safe_json_parse(row.get("quantile_returns"))

    for p in periods:
        ic_summary.append({
            "period": p,
            "ic_mean": row.get("ic_mean", 0),
            "ic_std": row.get("ic_std", 0),
            "icir": row.get("ic_ir", 0),
            "ic_positive_ratio": 0,
        })

    if quantile_returns:
        for qr in quantile_returns:
            layer_returns.append({
                "period": periods[0] if periods else 1,
                "quantile": qr.get("quantile", ""),
                "mean_return": qr.get("avg_return", 0),
            })

    return {
        "factor_id": row.get("factor_id"),
        "ic_summary": ic_summary,
        "layer_returns": layer_returns,
        "turnover_mean": row.get("turnover_mean", 0),
        "ic_mean": row.get("ic_mean", 0),
        "ic_std": row.get("ic_std", 0),
        "ic_ir": row.get("ic_ir", 0),
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
        "analysis_date": str(row.get("analysis_date", "")),
    }


def _clean_alphalens_results(results: dict) -> dict:
    """清理 Alphalens 结果中的 NaN/Inf 值以确保 JSON 序列化"""
    import math
    import numpy as np

    def clean_value(v):
        if isinstance(v, (float, np.floating)):
            if math.isnan(v) or math.isinf(v):
                return None
        elif isinstance(v, (np.integer, np.int64, np.int32)):
            return int(v)
        return v

    def clean_dict(d):
        if isinstance(d, dict):
            return {k: clean_dict(v) for k, v in d.items()}
        elif isinstance(d, (list, tuple)):
            return [clean_dict(item) for item in d]
        else:
            return clean_value(d)

    return clean_dict(results)


# ==================== API Endpoints ====================

@router.post("/analysis/run")
async def run_analysis(req: AnalyzeRequest):
    """运行因子分析"""
    try:
        result = analyzer.analyze(
            factor_id=req.factor_id,
            start_date=req.start_date,
            end_date=req.end_date,
            periods=req.periods,
            quantiles=req.quantiles,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"因子 {req.factor_id} 无数据或分析失败")
        return {"status": "success", "data": _format_analysis_summary(result)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/{factor_id}")
async def get_analysis(factor_id: str):
    """获取最新分析结果"""
    result = analyzer.get_latest_analysis(factor_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"因子 {factor_id} 无分析记录")
    return {"status": "success", "data": _format_db_analysis(result)}


@router.get("/analysis/{factor_id}/history")
async def get_analysis_history(factor_id: str, limit: int = 10):
    """获取分析历史"""
    records = analyzer.get_analysis_history(factor_id, limit)
    return {"status": "success", "data": [_format_db_analysis(r) for r in records]}


@router.post("/analysis/alphalens")
async def run_alphalens_analysis(req: AlphalensAnalysisRequest):
    """运行 Alphalens 因子分析

    Args:
        factor_id: 因子ID
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        periods: 持有周期列表，默认 [1, 5, 10]
        quantiles: 分位数，默认 5
        index_pool: 指数股票池代码（可选），如 '000300.SH'
        groupby_field: 分组字段（可选），如 'industry', 'market_cap'

    Returns:
        完整的 Alphalens 分析结果
    """
    try:
        logger.info(f"Starting Alphalens analysis: factor_id={req.factor_id}, "
                   f"date_range={req.start_date}~{req.end_date}, "
                   f"index_pool={req.index_pool}, groupby={req.groupby_field}")

        results = analyzer.analyze(
            factor_id=req.factor_id,
            start_date=req.start_date,
            end_date=req.end_date,
            periods=req.periods,
            quantiles=req.quantiles,
            use_alphalens=True,
            index_pool=req.index_pool,
            groupby_field=req.groupby_field
        )

        if not results:
            raise HTTPException(status_code=500, detail="分析失败，未返回结果")

        results = _clean_alphalens_results(results)
        logger.info(f"Alphalens analysis completed for {req.factor_id}")

        return {
            "status": "success",
            "message": f"成功完成 Alphalens 分析",
            "data": results
        }

    except Exception as e:
        logger.error(f"Alphalens analysis failed: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


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
            'turnover', 'decay_analysis', 'chart_data'
        ]

        for field in json_fields:
            if field in record and record[field]:
                try:
                    record[field] = safe_json_parse(record[field])
                except Exception as e:
                    logger.warning(f"Failed to parse {field} for {factor_id}: {e}")
                    record[field] = None

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
