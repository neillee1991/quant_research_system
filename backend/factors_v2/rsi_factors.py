"""
因子: 相对强弱指标 (Relative Strength Index)

迁移自旧架构
生成时间: 2026-03-07
"""
import polars as pl
from engine.production.registry import factor
from engine.factors.technical import TechnicalFactors


@factor(
    factor_id="factor_rsi_6",
    description="6日相对强弱指标",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 6,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_rsi_6(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    计算6日RSI

    Args:
        df: 包含 ts_code, trade_date, close 的 DataFrame
        params: 参数字典，包含 window

    Returns:
        包含 ts_code, trade_date, factor_value 的 DataFrame
    """
    window = params.get("window", 6)

    # 按股票分组计算RSI
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.rsi(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])

    return result.select(["ts_code", "trade_date", "factor_value"])


@factor(
    factor_id="factor_rsi_14",
    description="14日相对强弱指标",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 14,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_rsi_14(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """计算14日RSI"""
    window = params.get("window", 14)
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.rsi(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])


@factor(
    factor_id="factor_rsi_24",
    description="24日相对强弱指标",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": 24,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_rsi_24(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """计算24日RSI"""
    window = params.get("window", 24)
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.rsi(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])
