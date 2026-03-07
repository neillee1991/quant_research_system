"""
因子: 移动平均线 (Moving Average)

迁移自旧架构
生成时间: 2026-03-07
"""
import polars as pl
from engine.production.registry import factor
from engine.factors.technical import TechnicalFactors


@factor(
    factor_id="factor_ma_5",
    description="5日移动平均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 5,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_ma_5(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    计算5日移动平均线

    Args:
        df: 包含 ts_code, trade_date, close 的 DataFrame
        params: 参数字典，包含 window

    Returns:
        包含 ts_code, trade_date, factor_value 的 DataFrame
    """
    window = params.get("window", 5)

    # 按股票分组计算移动平均
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.sma(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])

    # 返回必需列
    return result.select(["ts_code", "trade_date", "factor_value"])


@factor(
    factor_id="factor_ma_10",
    description="10日移动平均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 10,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_ma_10(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """计算10日移动平均线"""
    window = params.get("window", 10)
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.sma(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])


@factor(
    factor_id="factor_ma_20",
    description="20日移动平均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 20,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_ma_20(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """计算20日移动平均线"""
    window = params.get("window", 20)
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.sma(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])


@factor(
    factor_id="factor_ma_60",
    description="60日移动平均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 60,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_ma_60(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """计算60日移动平均线"""
    window = params.get("window", 60)
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.sma(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])
