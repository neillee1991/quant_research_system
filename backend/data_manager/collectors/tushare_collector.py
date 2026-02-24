import time
import polars as pl
import tushare as ts
from datetime import datetime, timedelta
from app.core.config import settings
from app.core.logger import logger
from store.dolphindb_client import db_client


class TushareCollector:
    """Tushare data collector with rate-limit handling."""

    def __init__(self):
        if not settings.collector.tushare_token:
            raise ValueError("TUSHARE_TOKEN 未设置，请在 .env 文件中配置")
        ts.set_token(settings.collector.tushare_token)
        self.pro = ts.pro_api()
        self._call_interval = 0.4  # seconds between API calls

    def _call_with_retry(self, func, max_retries: int = 3, **kwargs):
        for attempt in range(max_retries):
            try:
                time.sleep(self._call_interval)
                result = func(**kwargs)
                if result is not None and not result.empty:
                    return result
                logger.warning(f"Empty result on attempt {attempt + 1}")
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)
        return None

    def get_stock_list(self) -> pl.DataFrame | None:
        """Fetch all A-share stock list."""
        df = self._call_with_retry(self.pro.stock_basic, exchange="", list_status="L",
                                   fields="ts_code,symbol,name,area,industry,list_date")
        if df is None:
            return None
        return pl.from_pandas(df)

    def get_daily_data(self, ts_code: str, start_date: str, end_date: str) -> pl.DataFrame | None:
        """Fetch daily OHLCV data for a single stock."""
        df = self._call_with_retry(
            self.pro.daily,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="trade_date,ts_code,open,high,low,close,vol,amount,pct_chg",
        )
        if df is None:
            return None
        return pl.from_pandas(df)

    def get_index_daily(self, ts_code: str, start_date: str, end_date: str) -> pl.DataFrame | None:
        """Fetch daily data for an index (e.g. 000001.SH)."""
        df = self._call_with_retry(
            self.pro.index_daily,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None:
            return None
        return pl.from_pandas(df)

    def sync_stock_daily(self, ts_code: str, start_date: str | None = None):
        """Incremental sync for a single stock."""
        last_date = db_client.get_last_sync_date("tushare", ts_code)
        if last_date:
            start = (datetime.strptime(last_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        else:
            start = start_date or "20100101"

        end = datetime.today().strftime("%Y%m%d")
        if start > end:
            logger.info(f"{ts_code} already up to date")
            return

        logger.info(f"Syncing {ts_code} from {start} to {end}")
        df = self.get_daily_data(ts_code, start, end)
        if df is None or df.is_empty():
            logger.warning(f"No data returned for {ts_code}")
            return

        db_client.upsert_daily(df)
        db_client.update_sync_log("tushare", ts_code, end)
        logger.info(f"Synced {len(df)} rows for {ts_code}")
