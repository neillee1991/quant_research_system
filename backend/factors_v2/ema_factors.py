"""
因子: 指数移动平均线 (Exponential Moving Average)

迁移自旧架构
生成时间: 2026-03-07
"""
import polars as pl
from engine.production.registry import factor
from engine.factors.technical import TechnicalFactors


@factor(
    factor_id="factor_ema_12",
    description="12日指数移动平均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 12,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_ema_12(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    计算12日指数移动平均线

    Args:
        df: 包含 ts_code, trade_date, close 的 DataFrame
        params: 参数字典，包含 window

    Returns:
        包含 ts_code, trade_date, factor_value 的 DataFrame
    """
    window = params.get("window", 12)

    # 按股票分组计算指数移动平均
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.ema(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])

    return result.select(["ts_code", "trade_date", "factor_value"])


@factor(
    factor_id="factor_ema_26",
    description="26日指数移动平均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 26,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_ema_26(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """计算26日指数移动平均线"""
    window = params.get("window", 26)
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.ema(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])
