"""
系统常量定义
集中管理所有魔法数字和常量值
"""
from typing import Final

# 交易日相关
TRADING_DAYS_PER_YEAR: Final[int] = 252
TRADING_HOURS_PER_DAY: Final[int] = 4

# 因子计算默认参数
DEFAULT_MA_WINDOW: Final[int] = 20
DEFAULT_EMA_WINDOW: Final[int] = 12
DEFAULT_RSI_WINDOW: Final[int] = 14
DEFAULT_MACD_FAST: Final[int] = 12
DEFAULT_MACD_SLOW: Final[int] = 26
DEFAULT_MACD_SIGNAL: Final[int] = 9
DEFAULT_BOLLINGER_WINDOW: Final[int] = 20
DEFAULT_BOLLINGER_STD: Final[float] = 2.0
DEFAULT_ATR_WINDOW: Final[int] = 14
DEFAULT_KDJ_WINDOW: Final[int] = 9

# 回测默认参数
DEFAULT_INITIAL_CAPITAL: Final[float] = 1_000_000.0
DEFAULT_COMMISSION_RATE: Final[float] = 0.0003
DEFAULT_SLIPPAGE_RATE: Final[float] = 0.0001
DEFAULT_MIN_POSITION: Final[float] = 0.01
DEFAULT_MAX_POSITION: Final[float] = 0.2

# 数据同步
DEFAULT_START_DATE: Final[str] = "20100101"
DEFAULT_BATCH_SIZE: Final[int] = 5000
MAX_RETRY_ATTEMPTS: Final[int] = 3
RETRY_BACKOFF_BASE: Final[int] = 2

# 数据库
DEFAULT_QUERY_LIMIT: Final[int] = 10000
MAX_QUERY_LIMIT: Final[int] = 1_000_000

# 机器学习
DEFAULT_CV_FOLDS: Final[int] = 5
DEFAULT_TEST_SIZE: Final[float] = 0.2
DEFAULT_RANDOM_STATE: Final[int] = 42
DEFAULT_N_TRIALS: Final[int] = 100

# 日期格式
DATE_FORMAT_YYYYMMDD: Final[str] = "%Y%m%d"
DATE_FORMAT_YYYY_MM_DD: Final[str] = "%Y-%m-%d"
DATETIME_FORMAT_ISO: Final[str] = "%Y-%m-%d %H:%M:%S"

# 数据类型映射
PANDAS_TO_DUCKDB_TYPE_MAP: Final[dict] = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "float64": "DOUBLE",
    "float32": "FLOAT",
    "object": "VARCHAR",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
}

# API 限流
DEFAULT_RATE_LIMIT_CALLS: Final[int] = 120
DEFAULT_RATE_LIMIT_PERIOD: Final[int] = 60  # 秒

# 信号类型
SIGNAL_BUY: Final[int] = 1
SIGNAL_SELL: Final[int] = -1
SIGNAL_HOLD: Final[int] = 0
