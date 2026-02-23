"""自定义因子"""
import polars as pl
from engine.production.registry import factor
@factor(
    "factor_volatility_10",
    description="10日波动率",
    depends_on=["daily_data"],
    category="technical",
    params={"window": 10, "lookback_days": 30},
)
def compute_volatility_10(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = 10
    return (
        df.sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("pct_chg").rolling_std(window_size=w).over("ts_code").alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )