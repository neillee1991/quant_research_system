"""
通用工具类
提供重试、速率限制、日期处理等通用功能
"""
import json
import gzip
import base64
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, Dict, List
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
        """获取日期范围（不可变模式：使用列表推导式）"""
        start = datetime.strptime(start_date, format_str)
        end = datetime.strptime(end_date, format_str)

        # 不可变模式：使用列表推导式替代 append
        total_days = (end - start).days + 1
        return [
            (start + timedelta(days=i)).strftime(format_str)
            for i in range(total_days)
        ]

    @staticmethod
    def add_days(date_str: str, days: int, format_str: str = DATE_FORMAT_YYYYMMDD) -> str:
        """日期加减天数"""
        # 如果输入是 datetime 对象，先转换为字符串
        if isinstance(date_str, datetime):
            date_str = date_str.strftime(format_str)
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

    @staticmethod
    def normalize_date_to_object(d: Any) -> Optional[date]:
        """将多种日期类型转换为 date 对象

        支持: date 对象, datetime 对象, YYYYMMDD 字符串, YYYY-MM-DD 字符串
        """
        if d is None:
            return None
        if hasattr(d, 'date'):
            return d.date()
        if isinstance(d, date):
            return d
        if isinstance(d, datetime):
            return d.date()
        return datetime.strptime(str(d), "%Y%m%d").date()

    @staticmethod
    def format_date_for_display(date_value: Any) -> Optional[str]:
        """将日期值格式化为 YYYY-MM-DD 显示格式

        自动处理: YYYYMMDD 字符串, date/datetime 对象
        """
        if date_value is None:
            return None
        date_str = str(date_value)
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        # 如果已经是带连字符的格式，直接返回前10个字符
        if len(date_str) >= 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str[:10]
        # 处理 ISO 格式带时间的情况
        if 'T' in date_str:
            return date_str.split('T')[0][:10]
        return date_str

    @staticmethod
    def validate_yyyymmdd(date_str: str) -> bool:
        """验证字符串是否为有效的 YYYYMMDD 格式"""
        if not date_str or not date_str.isdigit() or len(date_str) != 8:
            return False
        try:
            datetime.strptime(date_str, "%Y%m%d")
            return True
        except ValueError:
            return False


def safe_str_datetime(dt: Any) -> Optional[str]:
    """安全地将 datetime/date 转换为字符串，处理 None 值"""
    if dt is None:
        return None
    return str(dt)


def unify_record_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """统一数据库记录的字段名（旧字段名 -> 新字段名）"""
    field_mapping = {
        "rows_affected": "rows",
        "duration_seconds": "elapsed_seconds",
    }
    # 创建新字典而不是修改原字典
    result = dict(record)
    for old_key, new_key in field_mapping.items():
        if old_key in result:
            result[new_key] = result[old_key]
            del result[old_key]
    return result


def decompress_json(compressed_str: str) -> Any:
    """解压缩 JSON 数据"""
    try:
        compressed = base64.b64decode(compressed_str.encode('ascii'))
        json_str = gzip.decompress(compressed).decode('utf-8')
        return json.loads(json_str)
    except Exception:
        # 兼容未压缩的旧数据
        return json.loads(compressed_str)


def load_json_from_file(file_path: str, default: Any = None) -> Any:
    """从文件路径加载 JSON 数据，兼容旧的压缩格式

    Args:
        file_path: 文件路径，或者是压缩的 JSON 字符串（旧数据兼容）
        default: 加载失败时的默认值

    Returns:
        解析后的 JSON 数据
    """
    p = Path(file_path)
    # 首先尝试作为文件读取
    try:
        if p.exists():
            return json.loads(p.read_text(encoding='utf-8'))
    except Exception as e:
        logger.warning(f"Failed to read file {file_path}: {e}")

    # 兼容旧数据：尝试作为压缩 JSON 解析
    try:
        return decompress_json(file_path)
    except Exception:
        return default


def parse_json_fields(record: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    """解析记录中的多个 JSON 字段

    Args:
        record: 数据库记录字典
        fields: 需要解析的字段名列表

    Returns:
        更新后的记录（新字典，不修改原记录）
    """
    result = dict(record)
    for field in fields:
        if result.get(field):
            result[field] = safe_json_parse(result[field])
    return result


def normalize_trade_date_pl(df: Any, date_col: str = "trade_date") -> Any:
    """归一化 Polars DataFrame 中的 trade_date 列为 YYYYMMDD 字符串格式

    Args:
        df: Polars DataFrame
        date_col: 日期列名，默认为 "trade_date"

    Returns:
        新的 DataFrame，日期列已归一化
    """
    import polars as pl

    if date_col not in df.columns:
        return df

    dtype = df[date_col].dtype

    if dtype == pl.Utf8:
        # 已经是字符串，去掉连字符
        return df.with_columns(
            pl.col(date_col).str.replace_all("-", "").alias(date_col)
        )
    elif dtype in (pl.Date, pl.Datetime):
        # 日期类型，格式化为 YYYYMMDD
        return df.with_columns(
            pl.col(date_col).dt.strftime("%Y%m%d").alias(date_col)
        )
    else:
        # 其他类型，先转字符串再去掉连字符
        return df.with_columns(
            pl.col(date_col).cast(pl.Utf8).str.replace_all("-", "").alias(date_col)
        )


def safe_json_parse(raw: any, default: any = None) -> any:
    """安全解析 JSON 字符串、字典或 None

    Args:
        raw: 输入值，可以是 str/dict/None
        default: 解析失败时的默认值，默认为 {}

    Returns:
        解析后的值或默认值
    """
    import json
    if raw is None:
        return default if default is not None else {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return default if default is not None else {}
    return default if default is not None else {}


class TradingCalendar:
    """交易日历服务

    基于 sync_trade_cal 表提供交易日查询、偏移等能力。
    初始化时一次性加载到内存，后续查询无 DB 开销。
    """

    _instance: Optional["TradingCalendar"] = None

    def __init__(self, db_client=None):
        self._trading_days: list[str] = []
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
        """从 sync_trade_cal 表加载 SSE 交易日"""
        try:
            df = db_client.query(
                "SELECT cal_date FROM sync_trade_cal "
                "WHERE exchange = 'SSE' AND is_open = 1 "
                "ORDER BY cal_date"
            )
            if df.is_empty():
                logger.warning("TradingCalendar: sync_trade_cal 表为空，回退到自然日模式")
                return

            # 确保 cal_date 转换为字符串格式 YYYYMMDD
            cal_dates = df["cal_date"].to_list()
            self._trading_days = []
            for date in cal_dates:
                if isinstance(date, str):
                    self._trading_days.append(date)
                elif isinstance(date, datetime):
                    self._trading_days.append(date.strftime("%Y%m%d"))
                else:
                    self._trading_days.append(str(date))

            logger.info(f"TradingCalendar loaded {len(self._trading_days)} trading days "
                        f"({self._trading_days[0]} ~ {self._trading_days[-1]})")
        except Exception as e:
            logger.warning(f"TradingCalendar: 加载失败 ({e})，回退到自然日模式")

    # ---------- 查询 ----------
    @property
    def is_loaded(self) -> bool:
        return len(self._trading_days) > 0

    def is_trading_day(self, date_str: str) -> bool:
        """检查是否为交易日，使用二分查找 O(log n)"""
        import bisect
        idx = bisect.bisect_left(self._trading_days, date_str)
        return idx < len(self._trading_days) and self._trading_days[idx] == date_str

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
    def build_where_clause(filters: dict[str, Any]) -> tuple[str, list]:
        """构建 WHERE 子句，返回 (clause_str, params_list) 元组（不可变模式）

        使用 %s 占位符，避免 SQL 注入。
        不可变模式：使用推导式替代 append/extend
        """
        if not filters:
            return "", []

        # 不可变模式：使用生成器和列表推导式
        def process_filter(key: str, value: Any) -> tuple[str, list]:
            """处理单个过滤条件，返回 (condition_str, params_list)"""
            if isinstance(value, (list, tuple)):
                placeholders = ", ".join(["%s"] * len(value))
                return f"{key} IN ({placeholders})", list(value)
            elif value is None:
                return f"{key} IS NULL", []
            else:
                return f"{key} = %s", [value]

        # 使用列表推导式构建条件和参数
        results = [process_filter(k, v) for k, v in filters.items()]
        conditions = [cond for cond, _ in results]
        params = [p for _, params_list in results for p in params_list]

        clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        return clause, params

    @staticmethod
    def build_select_query(
        table: str,
        columns: Optional[list[str]] = None,
        filters: Optional[dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None
    ) -> tuple[str, list]:
        """构建 SELECT 查询，返回 (sql, params) 元组"""
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        params: list = []
        if filters:
            where_clause, where_params = QueryBuilder.build_where_clause(filters)
            if where_clause:
                query += " " + where_clause
                params.extend(where_params)

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        return query, params
