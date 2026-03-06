"""
VectorBT 回测引擎
基于 VectorBT 框架的向量化回测，替代原自研引擎
"""
import vectorbt as vbt
import pandas as pd
import numpy as np
import polars as pl
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class BacktestConfig:
    initial_capital: float = 1_000_000.0
    commission_rate: float = 0.0003   # 0.03%
    slippage_rate: float = 0.0001     # 0.01%
    position_size: float = 1.0        # fraction of capital per trade
    hold_days: int = 1


@dataclass
class BacktestResult:
    equity_curve: pl.DataFrame
    trades: pl.DataFrame
    metrics: dict[str, Any]


class VectorEngine:
    """VectorBT 回测引擎封装"""

    def __init__(self, config: BacktestConfig | None = None):
        self.config = config or BacktestConfig()

    def run(self, df: pl.DataFrame, signal_col: str = "signal") -> BacktestResult:
        """
        执行回测

        Args:
            df: Polars DataFrame，包含 trade_date, ts_code, close, {signal_col}
                signal: 1=long, -1=short, 0=flat
            signal_col: 信号列名

        Returns:
            BacktestResult
        """
        if "ts_code" in df.columns and df["ts_code"].n_unique() > 1:
            return self._run_multi_asset(df, signal_col)
        else:
            return self._run_single_asset(df, signal_col)

    def _run_single_asset(self, df: pl.DataFrame, signal_col: str) -> BacktestResult:
        """单标的回测"""
        # Polars → Pandas（VectorBT 需要 pandas）
        pdf = df.sort("trade_date").to_pandas()
        pdf['trade_date'] = pd.to_datetime(pdf['trade_date'], format='%Y%m%d')
        pdf = pdf.set_index('trade_date')

        close = pdf['close']
        signals = pdf[signal_col]

        # 生成 entries/exits
        entries = (signals == 1) & (signals.shift(1) != 1)
        exits = (signals != 1) & (signals.shift(1) == 1)

        # 处理做空信号
        short_entries = (signals == -1) & (signals.shift(1) != -1)
        short_exits = (signals != -1) & (signals.shift(1) == -1)

        cfg = self.config

        # 运行 VectorBT 多头
        portfolio = vbt.Portfolio.from_signals(
            close=close,
            entries=entries,
            exits=exits,
            short_entries=short_entries,
            short_exits=short_exits,
            init_cash=cfg.initial_capital,
            fees=cfg.commission_rate,
            slippage=cfg.slippage_rate,
            freq='1D',
        )

        return self._build_result(portfolio)

    def _run_multi_asset(self, df: pl.DataFrame, signal_col: str) -> BacktestResult:
        """多标的组合回测"""
        pdf = df.sort(["ts_code", "trade_date"]).to_pandas()
        pdf['trade_date'] = pd.to_datetime(pdf['trade_date'], format='%Y%m%d')

        # Pivot 为宽表
        close_wide = pdf.pivot_table(index='trade_date', columns='ts_code', values='close')
        signal_wide = pdf.pivot_table(index='trade_date', columns='ts_code', values=signal_col, fill_value=0)

        entries = (signal_wide == 1) & (signal_wide.shift(1) != 1)
        exits = (signal_wide != 1) & (signal_wide.shift(1) == 1)
        short_entries = (signal_wide == -1) & (signal_wide.shift(1) != -1)
        short_exits = (signal_wide != -1) & (signal_wide.shift(1) == -1)

        cfg = self.config

        portfolio = vbt.Portfolio.from_signals(
            close=close_wide,
            entries=entries,
            exits=exits,
            short_entries=short_entries,
            short_exits=short_exits,
            init_cash=cfg.initial_capital,
            fees=cfg.commission_rate,
            slippage=cfg.slippage_rate,
            freq='1D',
            cash_sharing=True,
        )

        return self._build_result(portfolio)

    def _build_result(self, portfolio: vbt.Portfolio) -> BacktestResult:
        """从 VectorBT Portfolio 构建 BacktestResult"""
        # 提取指标
        stats = portfolio.stats()

        metrics = {
            "total_return": self._safe_float(portfolio.total_return()),
            "annualized_return": self._safe_float(stats.get("Total Return [%]", 0)) / 100,
            "sharpe_ratio": self._safe_float(portfolio.sharpe_ratio()),
            "sortino_ratio": self._safe_float(portfolio.sortino_ratio()),
            "calmar_ratio": self._safe_float(portfolio.calmar_ratio()),
            "omega_ratio": self._safe_float(portfolio.omega_ratio()),
            "max_drawdown": self._safe_float(portfolio.max_drawdown()),
            "max_dd_duration": str(stats.get("Max Drawdown Duration", "")),
            "win_rate": self._safe_float(stats.get("Win Rate [%]", 0)) / 100,
            "profit_factor": self._safe_float(stats.get("Profit Factor", 0)),
            "expectancy": self._safe_float(stats.get("Expectancy", 0)),
            "n_trades": int(stats.get("Total Trades", 0)),
            "avg_winning_trade": self._safe_float(stats.get("Avg Winning Trade [%]", 0)) / 100,
            "avg_losing_trade": self._safe_float(stats.get("Avg Losing Trade [%]", 0)) / 100,
            "best_trade": self._safe_float(stats.get("Best Trade [%]", 0)) / 100,
            "worst_trade": self._safe_float(stats.get("Worst Trade [%]", 0)) / 100,
            "initial_capital": self.config.initial_capital,
            "final_value": self._safe_float(stats.get("End Value", self.config.initial_capital)),
        }

        # 权益曲线
        equity_series = portfolio.value()
        if isinstance(equity_series, pd.DataFrame):
            equity_series = equity_series.sum(axis=1)
        equity_pdf = equity_series.reset_index()
        equity_pdf.columns = ['trade_date', 'equity']
        equity_curve = pl.from_pandas(equity_pdf)

        # 交易记录
        try:
            trades_pdf = portfolio.trades.records_readable
            trades_df = pl.from_pandas(trades_pdf) if not trades_pdf.empty else pl.DataFrame()
        except Exception:
            trades_df = pl.DataFrame()

        return BacktestResult(
            equity_curve=equity_curve,
            trades=trades_df,
            metrics=metrics,
        )

    @staticmethod
    def _safe_float(value) -> float:
        """安全转换为 float"""
        try:
            v = float(value)
            if np.isnan(v) or np.isinf(v):
                return 0.0
            return round(v, 6)
        except (TypeError, ValueError):
            return 0.0


