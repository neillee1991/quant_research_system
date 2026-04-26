"""
回测引擎核心模块 - 提供策略基类、上下文管理、接口定义和策略加载器
"""
from backend.engine.backtest.core.base_strategy import (
    BaseStrategy,
    RQAlphaStrategy,
    StrategyConfig,
    StrategyMode,
    VectorBTStrategy,
)
from backend.engine.backtest.core.context import BacktestContext, Order, Portfolio, Position, Transaction
from backend.engine.backtest.core.interfaces import (
    IAdjustmentProvider,
    ICalendarProvider,
    ILimitPriceProvider,
    IMarketDataProvider,
    ISuspensionProvider,
)
from backend.engine.backtest.core.strategy_loader import StrategyFactory, StrategyLoader, StrategyMetadata

# 导出所有核心组件
__all__ = [
    # 策略基类
    "BaseStrategy",
    "VectorBTStrategy",
    "RQAlphaStrategy",
    "StrategyConfig",
    "StrategyMode",

    # 上下文和投资组合
    "BacktestContext",
    "Portfolio",
    "Position",
    "Order",
    "Transaction",

    # Provider接口
    "ISuspensionProvider",
    "IAdjustmentProvider",
    "ILimitPriceProvider",
    "ICalendarProvider",
    "IMarketDataProvider",

    # 策略加载
    "StrategyLoader",
    "StrategyFactory",
    "StrategyMetadata"
]
