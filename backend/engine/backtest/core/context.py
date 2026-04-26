"""
策略上下文和投资组合管理
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass(frozen=True)
class Position:
    """持仓信息"""
    symbol: str
    quantity: float
    avg_cost: float
    market_price: float
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    direction: str = "long"

    def __post_init__(self):
        """计算持仓市值和未实现盈亏"""
        # 使用 object.__setattr__ 因为 frozen=True 禁止直接修改
        object.__setattr__(self, 'market_value', self.quantity * self.market_price)
        object.__setattr__(self, 'unrealized_pnl', (self.market_price - self.avg_cost) * self.quantity)


@dataclass(frozen=True)
class Portfolio:
    """投资组合"""
    total_assets: float = 0.0
    available_cash: float = 0.0
    total_market_value: float = 0.0
    total_pnl: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)

    def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定标的持仓"""
        return self.positions.get(symbol)

    def add_position(self, position: Position) -> None:
        """添加持仓"""
        self.positions[position.symbol] = position

    def update_position(self, symbol: str, price: float) -> None:
        """更新持仓市值"""
        if symbol in self.positions:
            position = self.positions[symbol]
            position.market_price = price
            position.market_value = position.quantity * price
            position.unrealized_pnl = (price - position.avg_cost) * position.quantity

    def remove_position(self, symbol: str) -> None:
        """移除持仓"""
        self.positions.pop(symbol, None)

    def get_all_positions(self) -> List[Position]:
        """获取所有持仓"""
        return list(self.positions.values())

    def calculate_total_market_value(self) -> float:
        """计算总市值"""
        self.total_market_value = sum(
            pos.market_value for pos in self.positions.values()
        )
        return self.total_market_value

    def calculate_total_assets(self) -> float:
        """计算总资产"""
        self.calculate_total_market_value()
        self.total_assets = self.available_cash + self.total_market_value
        return self.total_assets

    def calculate_total_pnl(self) -> float:
        """计算总盈亏"""
        self.total_pnl = sum(
            pos.unrealized_pnl + pos.realized_pnl
            for pos in self.positions.values()
        )
        return self.total_pnl


@dataclass(frozen=True)
class Order:
    """订单信息"""
    order_id: str
    symbol: str
    direction: str
    order_type: str
    quantity: float
    price: float
    status: str = "pending"
    filled_quantity: float = 0.0
    filled_price: float = 0.0
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None


@dataclass(frozen=True)
class Transaction:
    """交易记录"""
    transaction_id: str
    order_id: str
    symbol: str
    direction: str
    quantity: float
    price: float
    fee: float
    timestamp: datetime


class BacktestContext:
    """
    回测上下文
    提供策略运行时的环境信息和操作接口
    """

    def __init__(
        self,
        initial_capital: float,
        commission: float = 0.0003,
        slippage: float = 0.001,
        start_date: Optional[pd.Timestamp] = None,
        end_date: Optional[pd.Timestamp] = None,
        benchmark: Optional[str] = None
    ):
        # 回测配置
        self.initial_capital = initial_capital
        self.commission = commission
        self.slippage = slippage
        self.start_date = start_date
        self.end_date = end_date
        self.benchmark = benchmark

        # 运行时状态
        self.current_datetime: Optional[pd.Timestamp] = None
        self.current_bar: Optional[pd.Series] = None
        self.current_idx: int = -1

        # 投资组合
        self.portfolio = Portfolio(
            total_assets=initial_capital,
            available_cash=initial_capital,
            total_market_value=0.0,
            total_pnl=0.0,
            positions={}
        )

        # 订单和交易记录
        self.orders: Dict[str, Order] = {}
        self.transactions: List[Transaction] = []
        self.order_counter = 0

        # 策略参数存储
        self.strategy_data: Dict[str, Any] = {}

    @property
    def available_cash(self) -> float:
        """获取可用资金"""
        return self.portfolio.available_cash

    @available_cash.setter
    def available_cash(self, value: float) -> None:
        """设置可用资金"""
        self.portfolio.available_cash = value

    @property
    def total_assets(self) -> float:
        """获取总资产"""
        return self.portfolio.total_assets

    def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定标的持仓"""
        return self.portfolio.get_position(symbol)

    def get_all_positions(self) -> List[Position]:
        """获取所有持仓"""
        return self.portfolio.get_all_positions()

    def set_current_datetime(self, dt: pd.Timestamp) -> None:
        """设置当前回测时间"""
        self.current_datetime = dt

    def set_current_bar(self, bar: pd.Series) -> None:
        """设置当前K线数据"""
        self.current_bar = bar

    def set_current_idx(self, idx: int) -> None:
        """设置当前回测索引"""
        self.current_idx = idx

    def record_order(self, order: Order) -> str:
        """记录订单"""
        if order.order_id is None:
            self.order_counter += 1
            order.order_id = f"order_{self.order_counter}"
        if order.create_time is None:
            order.create_time = self.current_datetime
        self.orders[order.order_id] = order
        return order.order_id

    def record_transaction(self, transaction: Transaction) -> None:
        """记录交易"""
        self.transactions.append(transaction)

    def update_portfolio_value(self) -> None:
        """更新投资组合价值"""
        self.portfolio.calculate_total_assets()
        self.portfolio.calculate_total_pnl()

    def set_strategy_data(self, key: str, value: Any) -> None:
        """存储策略数据"""
        self.strategy_data[key] = value

    def get_strategy_data(self, key: str, default: Any = None) -> Any:
        """获取策略数据"""
        return self.strategy_data.get(key, default)

    def get_context_snapshot(self) -> Dict[str, Any]:
        """获取上下文快照（用于分析）"""
        return {
            "datetime": self.current_datetime,
            "total_assets": self.portfolio.total_assets,
            "available_cash": self.portfolio.available_cash,
            "total_market_value": self.portfolio.total_market_value,
            "total_pnl": self.portfolio.total_pnl,
            "position_count": len(self.portfolio.positions),
            "position_symbols": list(self.portfolio.positions.keys())
        }

    def reset(self) -> None:
        """重置上下文状态"""
        self.current_datetime = None
        self.current_bar = None
        self.current_idx = -1
        self.portfolio = Portfolio(
            total_assets=self.initial_capital,
            available_cash=self.initial_capital,
            total_market_value=0.0,
            total_pnl=0.0,
            positions={}
        )
        self.orders.clear()
        self.transactions.clear()
        self.order_counter = 0
        self.strategy_data.clear()
