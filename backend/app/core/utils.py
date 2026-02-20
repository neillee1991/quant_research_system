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
