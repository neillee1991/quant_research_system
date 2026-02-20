import polars as pl
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from app.core.config import settings
from app.core.logger import logger
from store.postgres_client import db_client
from data_manager.collectors.tushare_collector import TushareCollector
from data_manager.collectors.akshare_collector import AkShareCollector


class DataSyncTask:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self._tushare: TushareCollector | None = None
        self._akshare = AkShareCollector()

    @property
    def tushare(self) -> TushareCollector:
        if self._tushare is None:
            self._tushare = TushareCollector()
        return self._tushare

    def sync_all_stocks(self, source: str = "tushare"):
        """Full incremental sync for all stocks."""
        logger.info(f"Starting full sync via {source}")
        if source == "tushare":
            stock_df = self.tushare.get_stock_list()
            if stock_df is None:
                logger.error("Failed to fetch stock list")
                return
            codes = stock_df["ts_code"].to_list()
            for code in codes:
                try:
                    self.tushare.sync_stock_daily(code)
                except Exception as e:
                    logger.error(f"Sync failed for {code}: {e}")
        logger.info("Full sync completed")

    def sync_single_stock(self, ts_code: str, source: str = "tushare"):
        """Sync a single stock."""
        if source == "tushare":
            self.tushare.sync_stock_daily(ts_code)
        elif source == "akshare":
            symbol = ts_code.split(".")[0]
            df = self._akshare.get_daily_data(
                symbol,
                start_date="20100101",
                end_date=datetime.today().strftime("%Y%m%d"),
            )
            if df is not None and not df.is_empty():
                db_client.upsert_daily(df)

    def start_scheduler(self):
        """Register scheduled jobs and start the scheduler."""
        # Daily sync at 18:00 on weekdays
        self.scheduler.add_job(
            self.sync_all_stocks,
            trigger="cron",
            day_of_week="mon-fri",
            hour=18,
            minute=0,
            id="daily_sync",
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("Scheduler started")

    def stop_scheduler(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")


sync_task = DataSyncTask()
