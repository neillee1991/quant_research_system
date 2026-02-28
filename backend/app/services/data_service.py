"""
数据服务层
封装数据查询和处理逻辑
"""
from typing import Any, Dict, List, Optional
from datetime import datetime
import polars as pl

from app.core.interfaces import IDataRepository
from app.core.exceptions import DataNotFoundError, DataValidationError
from app.core.utils import QueryBuilder, DateUtils
from app.core.logger import logger
from app.core.constants import DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT


class DataService:
    """数据服务"""

    def __init__(self, repository: IDataRepository):
        self.repository = repository

    def get_stock_list(
        self,
        market: Optional[str] = None,
        industry: Optional[str] = None
    ) -> pl.DataFrame:
        """获取股票列表"""
        filters = {}
        if market:
            filters["market"] = market
        if industry:
            filters["industry"] = industry

        try:
            df = self.repository.query("sync_stock_basic", filters=filters)
            if df.is_empty():
                raise DataNotFoundError("sync_stock_basic", f"market={market}, industry={industry}")
            return df
        except Exception as e:
            logger.error(f"Failed to get stock list: {e}")
            raise

    def get_daily_data(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """获取日线数据"""
        filters = {}

        if ts_code:
            filters["ts_code"] = ts_code

        if start_date:
            self._validate_date(start_date)
            filters["trade_date"] = (">=", start_date)

        if end_date:
            self._validate_date(end_date)
            if "trade_date" in filters:
                # 需要范围查询，暂时简化处理
                pass
            else:
                filters["trade_date"] = ("<=", end_date)

        try:
            df = self.repository.query(
                "sync_daily_data",
                filters=filters,
                limit=min(limit or DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT)
            )

            if df.is_empty():
                raise DataNotFoundError(
                    "sync_daily_data",
                    f"ts_code={ts_code}, start={start_date}, end={end_date}"
                )

            return df
        except Exception as e:
            logger.error(f"Failed to get daily data: {e}")
            raise

    def get_factor_data(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        factors: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """获取因子数据"""
        filters = {}

        if ts_code:
            filters["ts_code"] = ts_code

        if start_date:
            self._validate_date(start_date)

        if end_date:
            self._validate_date(end_date)

        try:
            # 从 sync_daily_basic 表获取因子数据
            df = self.repository.query(
                "sync_daily_basic",
                columns=factors if factors else None,
                filters=filters
            )

            if df.is_empty():
                raise DataNotFoundError(
                    "sync_daily_basic",
                    f"ts_code={ts_code}, factors={factors}"
                )

            return df
        except Exception as e:
            logger.error(f"Failed to get factor data: {e}")
            raise

    def load_stock_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pl.DataFrame:
        """加载股票数据（包含价格和因子）"""
        self._validate_date(start_date)
        self._validate_date(end_date)

        try:
            # 使用参数化查询防止SQL注入
            query = """
                SELECT
                    d.ts_code,
                    d.trade_date,
                    d.open,
                    d.high,
                    d.low,
                    d.close,
                    d.vol,
                    d.amount,
                    b.pe,
                    b.pb,
                    b.turnover_rate
                FROM sync_daily_data d
                LEFT JOIN sync_daily_basic b
                    ON d.ts_code = b.ts_code AND d.trade_date = b.trade_date
                WHERE d.ts_code = %s
                    AND d.trade_date >= %s
                    AND d.trade_date <= %s
                ORDER BY d.trade_date
            """

            df = self.repository.query(query, (ts_code, start_date, end_date))

            if df.is_empty():
                raise DataNotFoundError(
                    "stock_data",
                    f"ts_code={ts_code}, date_range={start_date}~{end_date}"
                )

            logger.info(f"Loaded {len(df)} rows for {ts_code}")
            return df

        except Exception as e:
            logger.error(f"Failed to load stock data: {e}")
            raise

    def save_factor_data(
        self,
        table: str,
        data: pl.DataFrame,
        primary_keys: List[str]
    ) -> None:
        """保存因子数据"""
        if data.is_empty():
            raise DataValidationError("data", "Cannot save empty DataFrame")

        try:
            self.repository.upsert(table, data, primary_keys)
            logger.info(f"Saved {len(data)} rows to {table}")
        except Exception as e:
            logger.error(f"Failed to save factor data: {e}")
            raise

    def _validate_date(self, date_str: str) -> None:
        """验证日期格式"""
        try:
            DateUtils.parse_date(date_str)
        except ValueError as e:
            raise DataValidationError("date", f"Invalid date format: {date_str}") from e
