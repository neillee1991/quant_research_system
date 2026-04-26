"""
回测引擎模块
实现 VectorBT + RQAlpha 双模回测架构
"""

from .core.base_strategy import BaseStrategy, StrategyConfig, StrategyMode, VectorBTStrategy, RQAlphaStrategy
from .core.context import BacktestContext, Portfolio, Position, Order, Transaction
from .core.interfaces import ISuspensionProvider, IAdjustmentProvider, ILimitPriceProvider, ICalendarProvider, IMarketDataProvider
from .core.strategy_loader import StrategyLoader, StrategyFactory, StrategyMetadata

__all__ = [
    "BaseStrategy",
    "StrategyConfig",
    "StrategyMode",
    "VectorBTStrategy",
    "RQAlphaStrategy",
    "BacktestContext",
    "Portfolio",
    "Position",
    "Order",
    "Transaction",
    "ISuspensionProvider",
    "IAdjustmentProvider",
    "ILimitPriceProvider",
    "ICalendarProvider",
    "IMarketDataProvider",
    "StrategyLoader",
    "StrategyFactory",
    "StrategyMetadata"
]
