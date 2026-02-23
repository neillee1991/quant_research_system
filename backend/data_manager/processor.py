import polars as pl
from app.core.logger import logger
from typing import Optional


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

    @staticmethod
    def mark_suspension_gaps(df: pl.DataFrame,
                             trading_days: Optional[list[str]] = None,
                             gap_threshold: int = 1) -> pl.DataFrame:
        """检测停牌导致的交易日 gap，添加 _suspension_gap 列。

        Args:
            df: 必须包含 ts_code, trade_date 列，且已按 (ts_code, trade_date) 排序
            trading_days: 有序交易日列表。如果提供，gap 按交易日计数；否则按行间距估算。
            gap_threshold: 超过该交易日间隔视为停牌 gap（默认 1，即连续两个交易日之间不应有缺失）

        Returns:
            新增 _suspension_gap 列（int）：该行与前一行之间缺失的交易日数，0 表示连续。
        """
        if trading_days:
            # 构建交易日序号映射
            td_index = pl.DataFrame({
                "trade_date": trading_days,
                "_td_seq": list(range(len(trading_days))),
            })
            df = df.join(td_index, on="trade_date", how="left")
            df = df.with_columns(
                (pl.col("_td_seq") - pl.col("_td_seq").shift(1) - 1)
                .over("ts_code")
                .fill_null(0)
                .clip(lower_bound=0)
                .alias("_suspension_gap")
            )
            df = df.drop("_td_seq")
        else:
            # 无交易日历时，用行号差估算（假设数据只含交易日行）
            df = df.with_columns(
                pl.lit(0).alias("_suspension_gap")
            )

        gap_count = df.filter(pl.col("_suspension_gap") > gap_threshold).height
        if gap_count > 0:
            logger.info(f"检测到 {gap_count} 行存在停牌 gap")

        return df

    @staticmethod
    def nullify_post_suspension(df: pl.DataFrame, window: int) -> pl.DataFrame:
        """将停牌复牌后前 window 行的 factor_value 置为 null。

        Args:
            df: 必须包含 ts_code, trade_date, factor_value, _suspension_gap 列
            window: 复牌后需要置空的行数

        Returns:
            处理后的 DataFrame（_suspension_gap 列被移除）
        """
        if "_suspension_gap" not in df.columns or "factor_value" not in df.columns:
            return df

        # 对每只股票，找到 gap > 0 的行，以及其后 window-1 行，全部置 null
        # 策略：构建一个 "距最近 gap 的行数" 列，<= window 的置 null
        df = df.with_columns(
            # 标记 gap 行为 1，其余为 0
            pl.when(pl.col("_suspension_gap") > 0)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("_is_gap")
        )

        # 用 rolling sum 向后扩散 gap 标记：如果最近 window 行内有 gap，则标记为需要置空
        df = df.with_columns(
            pl.col("_is_gap")
            .rolling_sum(window_size=window, min_periods=1)
            .over("ts_code")
            .alias("_near_gap")
        )

        nullified = df.filter(pl.col("_near_gap") > 0).height
        if nullified > 0:
            logger.info(f"停牌复牌置空: {nullified} 行 factor_value 被设为 null")

        df = df.with_columns(
            pl.when(pl.col("_near_gap") > 0)
            .then(pl.lit(None, dtype=pl.Float64))
            .otherwise(pl.col("factor_value"))
            .alias("factor_value")
        )

        df = df.drop(["_is_gap", "_near_gap", "_suspension_gap"])
        return df

    @staticmethod
    def mark_limit_up_down(df: pl.DataFrame, vol_threshold: float = 100.0) -> pl.DataFrame:
        """标记一字涨跌停。

        判断逻辑：
        - 涨跌幅接近涨跌停限制（|pct_chg| >= 9.8%）且成交量极低（vol < vol_threshold 手）
        - 或者 open == close == high == low（一字板）

        Args:
            df: 必须包含 open, high, low, close 列，可选 pct_chg, vol 列
            vol_threshold: 成交量阈值（手），低于此值视为极低成交量

        Returns:
            新增 _limit_up_down 列：1=一字涨停, -1=一字跌停, 0=正常
        """
        has_pct = "pct_chg" in df.columns
        has_vol = "vol" in df.columns

        # 一字板：OHLC 全部相等
        is_flat = (
            (pl.col("open") == pl.col("close")) &
            (pl.col("high") == pl.col("low")) &
            (pl.col("open") == pl.col("high"))
        )

        if has_pct and has_vol:
            # 涨跌幅大且量极低
            is_limit = (pl.col("pct_chg").abs() >= 9.8) & (pl.col("vol") < vol_threshold)
            is_limit_combined = is_flat | is_limit

            df = df.with_columns(
                pl.when(is_limit_combined & (pl.col("pct_chg") > 0))
                .then(pl.lit(1))
                .when(is_limit_combined & (pl.col("pct_chg") < 0))
                .then(pl.lit(-1))
                .otherwise(pl.lit(0))
                .alias("_limit_up_down")
            )
        elif has_pct:
            df = df.with_columns(
                pl.when(is_flat & (pl.col("pct_chg") > 0))
                .then(pl.lit(1))
                .when(is_flat & (pl.col("pct_chg") < 0))
                .then(pl.lit(-1))
                .otherwise(pl.lit(0))
                .alias("_limit_up_down")
            )
        else:
            df = df.with_columns(pl.lit(0).alias("_limit_up_down"))

        marked = df.filter(pl.col("_limit_up_down") != 0).height
        if marked > 0:
            logger.info(f"涨跌停标记: {marked} 行被标记为一字涨跌停")

        return df
