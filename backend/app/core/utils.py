"""
通用工具类
提供重试、速率限制、日期处理等通用功能
"""
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, TypeVar
from functools import wraps

from app.core.logger import logger
from app.core.exceptions import RateLimitExceededError
from app.core.constants import (
    DATE_FORMAT_YYYYMMDD,
    DATE_FORMAT_YYYY_MM_DD,
    RETRY_BACKOFF_BASE
)

T = TypeVar('T')


class RateLimiter:
    """速率限制器"""

    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.call_interval = 60.0 / calls_per_minute
        self.last_call_time = 0.0

    def wait(self) -> None:
        """等待以满足速率限制"""
        elapsed = time.time() - self.last_call_time
        if elapsed < self.call_interval:
            sleep_time = self.call_interval - elapsed
            time.sleep(sleep_time)
        self.last_call_time = time.time()

    def reset(self) -> None:
        """重置速率限制器"""
        self.last_call_time = 0.0


class RetryPolicy:
    """重试策略"""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: int = RETRY_BACKOFF_BASE,
        exceptions: tuple = (Exception,)
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.exceptions = exceptions

    def execute(self, func: Callable[..., T], *args, **kwargs) -> Optional[T]:
        """执行带重试的函数调用"""
        last_exception = None

        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs)
            except self.exceptions as e:
                last_exception = e
                if attempt < self.max_attempts - 1:
                    delay = self.base_delay ** attempt
                    logger.warning(
                        f"Attempt {attempt + 1}/{self.max_attempts} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"All {self.max_attempts} attempts failed. Last error: {e}"
                    )

        if last_exception:
            raise last_exception
        return None


def retry(
    max_attempts: int = 3,
    base_delay: int = RETRY_BACKOFF_BASE,
    exceptions: tuple = (Exception,)
):
    """重试装饰器"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            policy = RetryPolicy(max_attempts, base_delay, exceptions)
            return policy.execute(func, *args, **kwargs)
        return wrapper
    return decorator


class DateUtils:
    """日期工具类"""

    @staticmethod
    def format_date(date: datetime, format_str: str = DATE_FORMAT_YYYYMMDD) -> str:
        """格式化日期"""
        return date.strftime(format_str)

    @staticmethod
    def parse_date(date_str: str, format_str: str = DATE_FORMAT_YYYYMMDD) -> datetime:
        """解析日期字符串"""
        return datetime.strptime(date_str, format_str)

    @staticmethod
    def convert_date_format(
        date_str: str,
        from_format: str = DATE_FORMAT_YYYYMMDD,
        to_format: str = DATE_FORMAT_YYYY_MM_DD
    ) -> str:
        """转换日期格式"""
        date = datetime.strptime(date_str, from_format)
        return date.strftime(to_format)

    @staticmethod
    def get_date_range(
        start_date: str,
        end_date: str,
        format_str: str = DATE_FORMAT_YYYYMMDD
    ) -> list[str]:
        """获取日期范围"""
        start = datetime.strptime(start_date, format_str)
        end = datetime.strptime(end_date, format_str)

        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime(format_str))
            current += timedelta(days=1)

        return dates

    @staticmethod
    def add_days(date_str: str, days: int, format_str: str = DATE_FORMAT_YYYYMMDD) -> str:
        """日期加减天数"""
        date = datetime.strptime(date_str, format_str)
        new_date = date + timedelta(days=days)
        return new_date.strftime(format_str)

    @staticmethod
    def today(format_str: str = DATE_FORMAT_YYYYMMDD) -> str:
        """获取今天日期"""
        return datetime.today().strftime(format_str)

    @staticmethod
    def normalize_date(date_str: Optional[str], target_format: str = DATE_FORMAT_YYYYMMDD) -> Optional[str]:
        """将日期字符串统一转换为目标格式，自动识别 YYYY-MM-DD 和 YYYYMMDD"""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime(target_format)
            except ValueError:
                continue
        raise ValueError(f"Unrecognized date format: {date_str}")


class TradingCalendar:
    """交易日历服务

    基于 trade_cal 表提供交易日查询、偏移等能力。
    初始化时一次性加载到内存，后续查询无 DB 开销。
    """

    _instance: Optional["TradingCalendar"] = None

    def __init__(self, db_client=None):
        self._trading_days: list[str] = []
        self._trading_day_set: set[str] = set()
        if db_client is not None:
            self._load(db_client)

    # ---------- 单例 ----------
    @classmethod
    def get_instance(cls, db_client=None) -> "TradingCalendar":
        if cls._instance is None or not cls._instance._trading_days:
            cls._instance = cls(db_client)
        return cls._instance

    # ---------- 加载 ----------
    def _load(self, db_client) -> None:
        """从 trade_cal 表加载 SSE 交易日"""
        try:
            df = db_client.query(
                "SELECT cal_date FROM trade_cal "
                "WHERE exchange = 'SSE' AND is_open = 1 "
                "ORDER BY cal_date"
            )
            if df.is_empty():
                logger.warning("TradingCalendar: trade_cal 表为空，回退到自然日模式")
                return
            self._trading_days = df["cal_date"].to_list()
            self._trading_day_set = set(self._trading_days)
            logger.info(f"TradingCalendar loaded {len(self._trading_days)} trading days "
                        f"({self._trading_days[0]} ~ {self._trading_days[-1]})")
        except Exception as e:
            logger.warning(f"TradingCalendar: 加载失败 ({e})，回退到自然日模式")

    # ---------- 查询 ----------
    @property
    def is_loaded(self) -> bool:
        return len(self._trading_days) > 0

    def is_trading_day(self, date_str: str) -> bool:
        return date_str in self._trading_day_set

    def get_trading_days(self, start: str, end: str) -> list[str]:
        """返回 [start, end] 范围内的交易日列表"""
        import bisect
        lo = bisect.bisect_left(self._trading_days, start)
        hi = bisect.bisect_right(self._trading_days, end)
        return self._trading_days[lo:hi]

    def offset_trading_days(self, date_str: str, n: int) -> str:
        """从 date_str 向前(n<0)或向后(n>0)偏移 |n| 个交易日。

        如果 date_str 不是交易日，先定位到最近的交易日再偏移。
        如果日历未加载，回退到自然日 * 1.5 的粗略估算。
        """
        if not self.is_loaded:
            # 回退：自然日粗略估算
            factor = 1.5 if n < 0 else 1.5
            return DateUtils.add_days(date_str, int(n * factor))

        import bisect
        idx = bisect.bisect_left(self._trading_days, date_str)
        # 如果 date_str 不在日历中，idx 指向下一个交易日
        # 向前偏移时，应从前一个交易日开始
        if idx >= len(self._trading_days) or self._trading_days[idx] != date_str:
            if n < 0:
                idx = idx - 1
            # n >= 0 时 idx 已经指向下一个交易日，合理

        target = idx + n
        target = max(0, min(target, len(self._trading_days) - 1))
        return self._trading_days[target]

    def count_trading_days(self, start: str, end: str) -> int:
        """计算 [start, end] 之间的交易日数量"""
        return len(self.get_trading_days(start, end))


class QueryBuilder:
    """SQL 查询构建器"""

    @staticmethod
    def build_where_clause(filters: dict[str, Any]) -> str:
        """构建 WHERE 子句"""
        if not filters:
            return ""

        conditions = []
        for key, value in filters.items():
            if isinstance(value, str):
                conditions.append(f"{key} = '{value}'")
            elif isinstance(value, (list, tuple)):
                values_str = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in value)
                conditions.append(f"{key} IN ({values_str})")
            elif value is None:
                conditions.append(f"{key} IS NULL")
            else:
                conditions.append(f"{key} = {value}")

        return "WHERE " + " AND ".join(conditions) if conditions else ""

    @staticmethod
    def build_select_query(
        table: str,
        columns: Optional[list[str]] = None,
        filters: Optional[dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None
    ) -> str:
        """构建 SELECT 查询"""
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        if filters:
            query += " " + QueryBuilder.build_where_clause(filters)

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        return query
