"""
统一策略基类 - 支持 VectorBT 和 RQAlpha 两种模式
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass(frozen=True)
class TradingSignal:
    """交易信号"""
    ts_code: str
    signal_type: str  # 'buy' 或 'sell'
    signal_strength: float
    price: float
    timestamp: str


class StrategyMode(Enum):
    """策略运行模式"""
    VECTORBT = "vectorbt"
    RQALPHA = "rqalpha"


@dataclass
class StrategyConfig:
    """策略配置"""
    strategy_id: str
    mode: StrategyMode
    initial_capital: float = 1000000.0
    commission: float = 0.0003
    slippage: float = 0.001
    benchmark: Optional[str] = None
    start_date: Optional[pd.Timestamp] = None
    end_date: Optional[pd.Timestamp] = None


class BaseStrategy(ABC):
    """
    策略基类 - 所有策略必须继承此类
    支持 VectorBT 和 RQAlpha 两种回测模式
    """

    def __init__(self, config: StrategyConfig):
        self.config = config
        self.context = None
        self._initialized = False

    @property
    def strategy_id(self) -> str:
        """策略ID"""
        return self.config.strategy_id

    @property
    def mode(self) -> StrategyMode:
        """策略运行模式"""
        return self.config.mode

    @abstractmethod
    def initialize(self, context: Any) -> None:
        """
        初始化策略
        在回测开始前调用一次

        Args:
            context: 回测上下文对象
        """
        pass

    @abstractmethod
    def generate_signals(self, prices: Any, factors: Dict[str, Any]) -> List[TradingSignal]:
        """
        生成交易信号（VectorBT 模式）

        Args:
            prices: 价格数据（pandas MultiIndex DataFrame）
            factors: 因子数据（字典格式，key为因子名，value为Series）

        Returns:
            交易信号列表
        """
        pass

    @abstractmethod
    def on_bar(self, data: Any) -> None:
        """
        K线级别的策略逻辑
        VectorBT模式：处理整个K线周期的数据
        RQAlpha模式：每日调用一次

        Args:
            data: 市场数据
        """
        pass

    @abstractmethod
    def on_tick(self, data: Any) -> None:
        """
         tick级别的策略逻辑（可选）
        仅在RQAlpha模式且支持tick级回测时调用

        Args:
            data: tick数据
        """
        pass

    @abstractmethod
    def on_order_book_update(self, data: Any) -> None:
        """
        订单簿更新事件（可选）

        Args:
            data: 订单簿数据
        """
        pass

    @abstractmethod
    def on_trade(self, data: Any) -> None:
        """
        成交事件（可选）

        Args:
            data: 成交数据
        """
        pass

    @abstractmethod
    def on_position_change(self, data: Any) -> None:
        """
        持仓变化事件

        Args:
            data: 持仓变化数据
        """
        pass

    @abstractmethod
    def on_order_change(self, data: Any) -> None:
        """
        订单状态变化事件

        Args:
            data: 订单状态数据
        """
        pass

    @abstractmethod
    def terminate(self) -> None:
        """策略结束清理"""
        pass

    def is_initialized(self) -> bool:
        """检查策略是否已初始化"""
        return self._initialized

    def get_params(self) -> Dict[str, Any]:
        """获取策略参数"""
        return {
            "strategy_id": self.strategy_id,
            "mode": self.mode.value,
            "initial_capital": self.config.initial_capital,
            "commission": self.config.commission,
            "slippage": self.config.slippage,
            "benchmark": self.config.benchmark
        }


class VectorBTStrategy(BaseStrategy):
    """
    VectorBT 模式策略基类
    基于向量化计算的回测策略
    """

    def on_tick(self, data: Any) -> None:
        """VectorBT模式不支持tick级回测"""
        pass

    def on_order_book_update(self, data: Any) -> None:
        """VectorBT模式不支持订单簿更新事件"""
        pass

    def on_trade(self, data: Any) -> None:
        """VectorBT模式不支持成交事件"""
        pass

    def on_position_change(self, data: Any) -> None:
        """VectorBT模式不支持持仓变化事件"""
        pass

    def on_order_change(self, data: Any) -> None:
        """VectorBT模式不支持订单状态变化事件"""
        pass

    def on_bar(self, data: Any) -> None:
        """VectorBT模式通过generate_signals处理数据，on_bar为空实现"""
        pass

    def terminate(self) -> None:
        """策略结束清理"""
        pass


class RQAlphaStrategy(BaseStrategy):
    """
    RQAlpha 模式策略基类
    基于事件驱动的回测策略
    """

    def on_order_book_update(self, data: Any) -> None:
        """RQAlpha模式默认不支持订单簿更新事件"""
        pass

    def on_trade(self, data: Any) -> None:
        """RQAlpha模式默认不支持成交事件"""
        pass

    def generate_signals(self, prices: Any, factors: Dict[str, Any]) -> List[TradingSignal]:
        """RQAlpha模式通过事件驱动处理，generate_signals为空实现"""
        return []

    def terminate(self) -> None:
        """策略结束清理"""
        pass
