import polars as pl
from app.core.logger import logger


class DataProcessor:
    """Clean and validate raw market data."""

    @staticmethod
    def clean_daily(df: pl.DataFrame) -> pl.DataFrame:
        """Basic cleaning: drop nulls, sort, deduplicate."""
        df = df.drop_nulls(subset=["trade_date", "ts_code", "close"])
        df = df.unique(subset=["trade_date", "ts_code"])
        df = df.sort(["ts_code", "trade_date"])
        return df

    @staticmethod
    def validate_ohlcv(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
        """
        Validate OHLCV integrity.
        Returns (cleaned_df, n_dropped).
        """
        before = len(df)
        df = df.filter(
            (pl.col("high") >= pl.col("low")) &
            (pl.col("high") >= pl.col("open")) &
            (pl.col("high") >= pl.col("close")) &
            (pl.col("low") <= pl.col("open")) &
            (pl.col("low") <= pl.col("close")) &
            (pl.col("vol") >= 0)
        )
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} invalid OHLCV rows")
        return df, dropped

    @staticmethod
    def fill_missing_dates(df: pl.DataFrame, ts_code: str) -> pl.DataFrame:
        """Forward-fill missing trading dates (simple approach)."""
        df = df.filter(pl.col("ts_code") == ts_code).sort("trade_date")
        return df
