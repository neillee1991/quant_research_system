#!/usr/bin/env python3
"""
测试回测引擎核心基础类
"""
import pandas as pd

from backend.engine.backtest.core.base_strategy import (
    BaseStrategy,
    VectorBTStrategy,
    RQAlphaStrategy,
    StrategyConfig,
    StrategyMode
)
from backend.engine.backtest.core.context import (
    BacktestContext,
    Portfolio,
    Position
)


class TestVectorBTStrategy(VectorBTStrategy):
    """测试用的 VectorBT 策略"""

    def __init__(self, config: StrategyConfig):
        super().__init__(config)

    def initialize(self, context):
        super().initialize(context)
        print("策略初始化完成")

    def on_bar(self, data):
        print(f"收到K线数据: {data}")

    def terminate(self):
        print("策略结束")


def test_strategy_config():
    """测试策略配置类"""
    print("\n=== 测试策略配置类 ===")

    config = StrategyConfig(
        strategy_id="test_strategy_001",
        mode=StrategyMode.VECTORBT,
        initial_capital=1000000.0
    )

    assert config.strategy_id == "test_strategy_001"
    assert config.mode == StrategyMode.VECTORBT
    assert config.initial_capital == 1000000.0

    print("✓ 策略配置类测试通过")


def test_position():
    """测试持仓类"""
    print("\n=== 测试持仓类 ===")

    position = Position(
        symbol="000001.SZ",
        quantity=1000,
        avg_cost=10.0,
        market_price=12.5
    )

    assert position.symbol == "000001.SZ"
    assert position.quantity == 1000
    assert position.avg_cost == 10.0
    assert position.market_price == 12.5
    assert position.market_value == 12500.0
    assert position.unrealized_pnl == 2500.0

    print("✓ 持仓类测试通过")


def test_portfolio():
    """测试投资组合类"""
    print("\n=== 测试投资组合类 ===")

    portfolio = Portfolio(
        total_assets=1000000.0,
        available_cash=1000000.0
    )

    position1 = Position(
        symbol="000001.SZ",
        quantity=1000,
        avg_cost=10.0,
        market_price=12.5
    )
    portfolio.add_position(position1)

    position2 = Position(
        symbol="600000.SH",
        quantity=500,
        avg_cost=20.0,
        market_price=22.0
    )
    portfolio.add_position(position2)

    total_mv = portfolio.calculate_total_market_value()
    assert total_mv == 12500.0 + 11000.0

    total_assets = portfolio.calculate_total_assets()
    assert total_assets == 1000000.0 + 23500.0

    print("✓ 投资组合类测试通过")


def test_backtest_context():
    """测试回测上下文类"""
    print("\n=== 测试回测上下文类 ===")

    context = BacktestContext(
        initial_capital=1000000.0,
        commission=0.0003,
        slippage=0.001
    )

    assert context.initial_capital == 1000000.0
    assert context.available_cash == 1000000.0

    context.set_current_datetime(pd.Timestamp("2024-01-01"))
    assert context.current_datetime == pd.Timestamp("2024-01-01")

    position = Position(
        symbol="000001.SZ",
        quantity=1000,
        avg_cost=10.0,
        market_price=12.5
    )
    context.portfolio.add_position(position)

    retrieved = context.get_position("000001.SZ")
    assert retrieved is not None
    assert retrieved.symbol == "000001.SZ"

    context.set_strategy_data("test_key", "test_value")
    assert context.get_strategy_data("test_key") == "test_value"

    print("✓ 回测上下文类测试通过")


def test_strategy_creation():
    """测试策略创建"""
    print("\n=== 测试策略创建 ===")

    config = StrategyConfig(
        strategy_id="test_strategy_001",
        mode=StrategyMode.VECTORBT,
        initial_capital=1000000.0
    )

    strategy = TestVectorBTStrategy(config)

    assert strategy.strategy_id == "test_strategy_001"
    assert strategy.mode == StrategyMode.VECTORBT
    assert not strategy.is_initialized()

    # 模拟初始化
    context = BacktestContext(1000000.0)
    strategy.initialize(context)
    assert strategy.is_initialized()

    print("✓ 策略创建和初始化测试通过")


if __name__ == "__main__":
    print("开始测试回测引擎核心基础类...")

    try:
        test_strategy_config()
        test_position()
        test_portfolio()
        test_backtest_context()
        test_strategy_creation()

        print("\n✅ 所有测试通过！")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
