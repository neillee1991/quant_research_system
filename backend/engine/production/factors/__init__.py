"""
因子模块自动加载
导入此包时自动发现并注册所有因子
"""
from engine.production.registry import discover_factors

discover_factors()
