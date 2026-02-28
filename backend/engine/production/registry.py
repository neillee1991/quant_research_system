"""
因子注册表
通过装饰器模式注册因子计算函数，框架自动处理数据加载和结果存储
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Any, Optional


@dataclass
class StorageConfig:
    """因子存储配置"""
    target: str = "factor_values"  # 默认存到统一因子表，也可指定自定义表名
    columns: Optional[Dict[str, str]] = None  # 自定义表的列定义 {列名: 类型}
    primary_keys: Optional[List[str]] = None  # 自定义表的主键


@dataclass
class FactorDefinition:
    """因子定义"""
    factor_id: str
    description: str
    func: Callable
    depends_on: List[str]
    category: str
    params: Dict[str, Any]
    compute_mode: str  # "incremental" 或 "full"
    storage: StorageConfig


# 全局因子注册表
_factor_registry: Dict[str, FactorDefinition] = {}


def factor(
    factor_id: str,
    description: str = "",
    depends_on: list = None,
    category: str = "custom",
    params: dict = None,
    compute_mode: str = "incremental",
    storage: dict = None
):
    """因子注册装饰器

    Args:
        factor_id: 因子唯一标识
        description: 因子描述
        depends_on: 依赖的数据源列表（表名或因子ID）
            - "sync_daily_data": 从日线行情表加载
            - "sync_daily_basic": 从每日指标表加载
            - "factor_xxx": 从已计算的因子加载
        category: 因子分类 ("momentum", "value", "technical", "custom")
        params: 因子参数（如窗口大小等）
        compute_mode: 计算模式
            - "incremental": 增量计算（只算最新数据，需要加载窗口期历史数据）
            - "full": 全量重算（加载所有历史数据）
        storage: 存储配置字典
            - None: 存到统一因子表 factor_values
            - {"target": "factor_values"}: 同上
            - {"target": "my_table", "columns": {"col": "TYPE"}, "primary_keys": ["col1"]}: 自定义表

    Usage:
        @factor("factor_ma_20", description="20日均线",
                depends_on=["sync_daily_data"], category="technical")
        def compute_ma_20(df, params):
            # df: 含 ts_code, trade_date, close 等列的 Polars DataFrame
            # 返回: 含 ts_code, trade_date, factor_value 的 DataFrame
            return df.with_columns(...)
    """
    def decorator(func):
        storage_config = StorageConfig(**(storage or {}))
        _factor_registry[factor_id] = FactorDefinition(
            factor_id=factor_id,
            description=description,
            func=func,
            depends_on=depends_on or [],
            category=category,
            params=params or {},
            compute_mode=compute_mode,
            storage=storage_config
        )
        return func
    return decorator


def get_registry() -> Dict[str, FactorDefinition]:
    """获取因子注册表"""
    return _factor_registry


def get_factor(factor_id: str) -> Optional[FactorDefinition]:
    """获取指定因子定义"""
    return _factor_registry.get(factor_id)


def list_factors() -> List[Dict[str, Any]]:
    """列出所有已注册因子的摘要信息"""
    return [
        {
            "factor_id": f.factor_id,
            "description": f.description,
            "category": f.category,
            "compute_mode": f.compute_mode,
            "depends_on": f.depends_on,
            "storage_target": f.storage.target,
            "params": f.params,
        }
        for f in _factor_registry.values()
    ]


def unregister_factor(factor_id: str):
    """从注册表中移除因子"""
    _factor_registry.pop(factor_id, None)


def discover_factors(factors_dir: str = None):
    """自动发现并导入 factors/ 目录下所有因子模块，触发 @factor 装饰器注册。

    可安全多次调用，Python import 缓存保证幂等。
    """
    import os
    import importlib
    import logging

    if factors_dir is None:
        factors_dir = os.path.join(os.path.dirname(__file__), "factors")

    if not os.path.isdir(factors_dir):
        return

    for fname in sorted(os.listdir(factors_dir)):
        if fname.endswith(".py") and not fname.startswith("__"):
            module_name = f"engine.production.factors.{fname[:-3]}"
            try:
                importlib.import_module(module_name)
            except Exception as e:
                logging.getLogger(__name__).warning(f"Failed to import factor module {module_name}: {e}")
