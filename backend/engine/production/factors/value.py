"""
价值因子
"""
import polars as pl
from engine.production.registry import factor


@factor(
    "factor_pe_rank",
    description="PE行业内排名百分位",
    depends_on=["daily_basic"],
    category="value",
    params={"lookback_days": 5},
)
def compute_pe_rank(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    return (
        df.filter(pl.col("pe").is_not_null() & (pl.col("pe") > 0))
        .with_columns(
            pl.col("pe").rank().over("trade_date").alias("pe_rank"),
            pl.col("pe").count().over("trade_date").alias("pe_count"),
        )
        .with_columns(
            (pl.col("pe_rank") / pl.col("pe_count")).alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )


@factor(
    "factor_pb_rank",
    description="PB行业内排名百分位",
    depends_on=["daily_basic"],
    category="value",
    params={"lookback_days": 5},
)
def compute_pb_rank(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    return (
        df.filter(pl.col("pb").is_not_null() & (pl.col("pb") > 0))
        .with_columns(
            pl.col("pb").rank().over("trade_date").alias("pb_rank"),
            pl.col("pb").count().over("trade_date").alias("pb_count"),
        )
        .with_columns(
            (pl.col("pb_rank") / pl.col("pb_count")).alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )


@factor(
    "factor_turnover_rank",
    description="换手率排名百分位",
    depends_on=["daily_basic"],
    category="value",
    params={"lookback_days": 5},
)
def compute_turnover_rank(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    return (
        df.filter(pl.col("turnover_rate").is_not_null())
        .with_columns(
            pl.col("turnover_rate").rank().over("trade_date").alias("tr_rank"),
            pl.col("turnover_rate").count().over("trade_date").alias("tr_count"),
        )
        .with_columns(
            (pl.col("tr_rank") / pl.col("tr_count")).alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )
