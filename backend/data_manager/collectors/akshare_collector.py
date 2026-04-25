import time
import akshare as ak
import polars as pl
from app.core.logger import logger


class AkShareCollector:
    """AkShare data collector (no token required)."""

    _call_interval = 0.5

    def _call_with_retry(self, func, max_retries: int = 3, **kwargs):
        for attempt in range(max_retries):
            try:
                time.sleep(self._call_interval)
                result = func(**kwargs)
                if result is not None and not (hasattr(result, "empty") and result.empty):
                    return result
                logger.warning(f"AkShare empty result on attempt {attempt + 1}")
            except Exception as e:
                logger.warning(f"AkShare attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)
        return None

    def get_stock_list(self) -> pl.DataFrame | None:
        """Fetch A-share stock list from AkShare."""
        try:
            df = self._call_with_retry(ak.stock_info_a_code_name)
            if df is None:
                return None
            return pl.from_pandas(df)
        except Exception as e:
            logger.error(f"AkShare get_stock_list failed: {e}")
            return None

    def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pl.DataFrame | None:
        """
        Fetch daily OHLCV data.
        symbol: e.g. '000001' (without exchange suffix)
        start_date / end_date: 'YYYYMMDD'
        """
        try:
            df = self._call_with_retry(
                ak.stock_zh_a_hist,
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if df is None or df.empty:
                return None
            df = df.rename(columns={
                "日期": "trade_date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "vol",
                "成交额": "amount",
                "涨跌幅": "pct_chg",
            })
            df["ts_code"] = symbol
            df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "")
            cols = ["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount", "pct_chg"]
            return pl.from_pandas(df[cols])
        except Exception as e:
            logger.error(f"AkShare get_daily_data({symbol}) failed: {e}")
            return None
