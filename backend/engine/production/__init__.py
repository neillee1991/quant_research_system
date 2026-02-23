"""
生产任务框架
提供因子注册、计算引擎和分析功能
"""
from engine.production.registry import factor, get_registry, get_factor, list_factors  # noqa: F401
from engine.production.engine import ProductionEngine  # noqa: F401
