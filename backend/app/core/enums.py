"""
核心枚举类型
定义系统中常用的字符串常量，避免魔法字符串
"""
from enum import Enum


class ComputeMode(str, Enum):
    """因子计算模式"""
    INCREMENTAL = "incremental"  # 增量计算
    FULL = "full"  # 全量计算


class AdjustPrice(str, Enum):
    """价格复权模式"""
    FORWARD = "forward"  # 前复权
    BACKWARD = "backward"  # 后复权
    NONE = "none"  # 不复权


class FactorCategory(str, Enum):
    """因子分类"""
    CUSTOM = "custom"  # 自定义
    MOMENTUM = "momentum"  # 动量
    VALUE = "value"  # 价值
    TECHNICAL = "technical"  # 技术
    QUALITY = "quality"  # 质量
    GROWTH = "growth"  # 成长
    VOLATILITY = "volatility"  # 波动率


# 表名常量
class TableName:
    """数据库表名常量"""
    FACTOR_VALUES = "factor_values"
    FACTOR_METADATA = "factor_metadata"  # 向后兼容旧值
    FACTOR_CONFIGS = "factor_configs"      # 新表名
    FACTOR_DATA_CONFIG = "factor_data_config"  # 向后兼容旧值
    FACTOR_FIELD_MAPPINGS = "factor_field_mappings"  # 新表名
    SYNC_TASK_CONFIG = "sync_task_config"  # 向后兼容旧值
    SYNC_TASK_CONFIGS = "sync_task_configs"  # 新表名
    ETL_TASK_CONFIG = "etl_task_config"  # 向后兼容旧值
    ETL_TASK_CONFIGS = "etl_task_configs"  # 新表名
    SYNC_STOCK_BASIC = "sync_stock_basic"
    SYNC_DAILY_DATA = "sync_daily_data"
    SYNC_DAILY_BASIC = "sync_daily_basic"
    SYNC_ADJ_FACTOR = "sync_adj_factor"
    SYNC_TRADE_CAL = "sync_trade_cal"
    STOCK_DAILY_STATUS = "stock_daily_status"


# 交易所代码常量
class Exchange:
    """交易所代码"""
    SSE = "SSE"  # 上海证券交易所
    SZSE = "SZSE"  # 深圳证券交易所
