"""
回测引擎模块
包含多种回测引擎实现
"""
from .vector_engine import VectorEngine, BacktestConfig, BacktestResult

__all__ = [
    "VectorEngine",
    "BacktestConfig",
    "BacktestResult",
]
