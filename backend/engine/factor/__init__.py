"""
生产任务框架

提供因子注册、计算引擎和分析功能。
此框架负责：
- 数据库驱动的因子管理
- 增量计算支持
- 完整的质量控制流程
- 统一的因子注册表
- 完整的执行流水线
"""

from engine.factor.registry import get_registry, get_factor, list_factors
__all__ = ["get_registry", "get_factor", "list_factors"]