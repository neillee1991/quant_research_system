"""
动量和技术因子
"""
import polars as pl
from engine.production.registry import factor


@factor(
    "factor_ma_5",
    description="5日均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={"window": 5, "lookback_days": 20},
)
def compute_ma_5(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = params.get("window", 5)
    return (
        df.sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close").rolling_mean(window_size=w).over("ts_code").alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )


@factor(
    "factor_ma_20",
    description="20日均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={"window": 20, "lookback_days": 40},
)
def compute_ma_20(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = params.get("window", 20)
    return (
        df.sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close").rolling_mean(window_size=w).over("ts_code").alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )


@factor(
    "factor_rsi_14",
    description="14日RSI",
    depends_on=["sync_daily_data"],
    category="technical",
    compute_mode="full",
    params={"window": 14, "lookback_days": 30},
)
def compute_rsi_14(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = params.get("window", 14)
    sorted_df = df.sort(["ts_code", "trade_date"])

    result = (
        sorted_df
        .with_columns(
            (pl.col("close") - pl.col("close").shift(1)).over("ts_code").alias("change")
        )
        .with_columns([
            pl.when(pl.col("change") > 0).then(pl.col("change")).otherwise(0.0).alias("gain"),
            pl.when(pl.col("change") < 0).then(-pl.col("change")).otherwise(0.0).alias("loss"),
        ])
        .with_columns([
            pl.col("gain").rolling_mean(window_size=w).over("ts_code").alias("avg_gain"),
            pl.col("loss").rolling_mean(window_size=w).over("ts_code").alias("avg_loss"),
        ])
        .with_columns(
            pl.when(pl.col("avg_loss") == 0)
            .then(100.0)
            .otherwise(100.0 - 100.0 / (1.0 + pl.col("avg_gain") / pl.col("avg_loss")))
            .alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )
    return result


@factor(
    "factor_momentum_20",
    description="20日动量（涨跌幅）",
    depends_on=["sync_daily_data"],
    category="momentum",
    params={"window": 20, "lookback_days": 40},
)
def compute_momentum_20(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = params.get("window", 20)
    return (
        df.sort(["ts_code", "trade_date"])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(w) - 1.0)
            .over("ts_code")
            .alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )


@factor(
    "factor_volatility_20",
    description="20日波动率",
    depends_on=["sync_daily_data"],
    category="technical",
    params={"window": 20, "lookback_days": 40},
)
def compute_volatility_20(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = 20
    return (
        df.sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("pct_chg").rolling_std(window_size=w).over("ts_code").alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )
