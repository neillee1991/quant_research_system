import polars as pl
import numpy as np


class TechnicalFactors:
    """High-performance technical indicator library using Polars."""

    @staticmethod
    def sma(series: pl.Series, window: int) -> pl.Series:
        """Simple Moving Average."""
        return series.rolling_mean(window_size=window, min_periods=1)

    @staticmethod
    def ema(series: pl.Series, window: int) -> pl.Series:
        """Exponential Moving Average."""
        return series.ewm_mean(span=window, adjust=False)

    @staticmethod
    def rsi(series: pl.Series, window: int = 14) -> pl.Series:
        """Relative Strength Index."""
        delta = series.diff()
        gain = delta.clip(lower_bound=0)
        loss = (-delta).clip(lower_bound=0)
        avg_gain = gain.ewm_mean(span=window, adjust=False)
        avg_loss = loss.ewm_mean(span=window, adjust=False)
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def macd(series: pl.Series, fast: int = 12, slow: int = 26, signal: int = 9
             ) -> tuple[pl.Series, pl.Series, pl.Series]:
        """MACD, Signal, Histogram."""
        ema_fast = series.ewm_mean(span=fast, adjust=False)
        ema_slow = series.ewm_mean(span=slow, adjust=False)
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm_mean(span=signal, adjust=False)
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def kdj(high: pl.Series, low: pl.Series, close: pl.Series,
            n: int = 9, m1: int = 3, m2: int = 3
            ) -> tuple[pl.Series, pl.Series, pl.Series]:
        """KDJ stochastic oscillator."""
        low_n = low.rolling_min(window_size=n, min_periods=1)
        high_n = high.rolling_max(window_size=n, min_periods=1)
        rsv = (close - low_n) / (high_n - low_n + 1e-10) * 100
        k = rsv.ewm_mean(com=m1 - 1, adjust=False)
        d = k.ewm_mean(com=m2 - 1, adjust=False)
        j = 3 * k - 2 * d
        return k, d, j

    @staticmethod
    def bollinger_bands(series: pl.Series, window: int = 20, num_std: float = 2.0
                        ) -> tuple[pl.Series, pl.Series, pl.Series]:
        """Bollinger Bands: upper, middle, lower."""
        mid = series.rolling_mean(window_size=window, min_periods=1)
        std = series.rolling_std(window_size=window, min_periods=1)
        upper = mid + num_std * std
        lower = mid - num_std * std
        return upper, mid, lower

    @staticmethod
    def atr(high: pl.Series, low: pl.Series, close: pl.Series, window: int = 14) -> pl.Series:
        """Average True Range."""
        prev_close = close.shift(1)
        tr = pl.Series([max(h - l, abs(h - pc), abs(l - pc))
                        for h, l, pc in zip(high, low, prev_close.fill_null(close[0]))])
        return tr.ewm_mean(span=window, adjust=False)

    @staticmethod
    def rolling_std(series: pl.Series, window: int) -> pl.Series:
        return series.rolling_std(window_size=window, min_periods=1)

    @staticmethod
    def rolling_mean(series: pl.Series, window: int) -> pl.Series:
        return series.rolling_mean(window_size=window, min_periods=1)


class CrossSectionalFactors:
    """Cross-sectional operators applied across stocks on the same date."""

    @staticmethod
    def rank(df: pl.DataFrame, col: str) -> pl.DataFrame:
        """Cross-sectional rank (percentile) per date."""
        return df.with_columns(
            pl.col(col).rank("average").over("trade_date").alias(f"{col}_rank")
        )

    @staticmethod
    def zscore(df: pl.DataFrame, col: str) -> pl.DataFrame:
        """Cross-sectional Z-score per date."""
        return df.with_columns(
            ((pl.col(col) - pl.col(col).mean().over("trade_date")) /
             (pl.col(col).std().over("trade_date") + 1e-10)).alias(f"{col}_zscore")
        )

    @staticmethod
    def neutralize(df: pl.DataFrame, factor_col: str, group_col: str) -> pl.DataFrame:
        """Industry/group neutralization via demeaning within group."""
        return df.with_columns(
            (pl.col(factor_col) - pl.col(factor_col).mean().over(["trade_date", group_col])
             ).alias(f"{factor_col}_neutral")
        )
