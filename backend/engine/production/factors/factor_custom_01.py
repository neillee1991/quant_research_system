"""自定义因子"""
import polars as pl
from engine.production.registry import factor


@factor(
    "factor_custom_01",
    description="自定义因子",
    depends_on=["factor_ma_20"],
    category="custom",
    params={"window": 20, "lookback_days": 40},
)
def compute_custom(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = params.get("window", 20)

    return (
        df.with_columns((1/(pl.col("factor_ma_20"))).alias("factor_value"))
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )
