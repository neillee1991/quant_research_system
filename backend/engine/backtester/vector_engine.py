import polars as pl
import numpy as np
from dataclasses import dataclass, field
from typing import Any


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
    """Vectorized backtesting engine using Polars."""

    def __init__(self, config: BacktestConfig | None = None):
        self.config = config or BacktestConfig()

    def run(self, df: pl.DataFrame, signal_col: str = "signal") -> BacktestResult:
        """
        Run backtest on a DataFrame with columns:
          trade_date, ts_code, close, {signal_col}
        signal: 1=long, -1=short, 0=flat
        """
        df = df.sort(["ts_code", "trade_date"])

        # Forward return (next day close)
        df = df.with_columns(
            pl.col("close").shift(-1).over("ts_code").alias("next_close")
        )

        # Apply slippage to entry price
        cfg = self.config
        df = df.with_columns(
            (pl.col("close") * (1 + pl.col(signal_col) * cfg.slippage_rate)).alias("entry_price")
        )

        # Daily PnL
        df = df.with_columns(
            (
                pl.col(signal_col) *
                (pl.col("next_close") - pl.col("entry_price")) / pl.col("entry_price") -
                pl.col(signal_col).abs() * cfg.commission_rate
            ).alias("daily_return")
        ).drop_nulls(subset=["daily_return"])

        # Aggregate portfolio return per date (equal weight across positions)
        portfolio = (
            df.group_by("trade_date")
            .agg(pl.col("daily_return").mean().alias("port_return"))
            .sort("trade_date")
        )

        # Equity curve
        portfolio = portfolio.with_columns(
            (cfg.initial_capital * (1 + pl.col("port_return")).cum_prod()).alias("equity")
        )

        trades = df.filter(pl.col(signal_col) != 0).select(
            ["trade_date", "ts_code", signal_col, "entry_price", "next_close", "daily_return"]
        )

        metrics = self._evaluate(portfolio, trades)
        return BacktestResult(equity_curve=portfolio, trades=trades, metrics=metrics)

    def _evaluate(self, portfolio: pl.DataFrame, trades: pl.DataFrame) -> dict:
        returns = portfolio["port_return"]
        equity = portfolio["equity"]

        # Sharpe ratio (annualized, 252 trading days)
        mean_r = float(returns.mean())
        std_r = float(returns.std()) + 1e-10
        sharpe = mean_r / std_r * (252 ** 0.5)

        # Max drawdown
        peak = equity.cum_max()
        drawdown = (equity - peak) / peak
        max_dd = float(drawdown.min())

        # Win rate
        win_rate = float((trades["daily_return"] > 0).mean()) if len(trades) > 0 else 0.0

        # Profit factor
        gains = trades.filter(pl.col("daily_return") > 0)["daily_return"].sum()
        losses = abs(trades.filter(pl.col("daily_return") < 0)["daily_return"].sum())
        profit_factor = float(gains / (losses + 1e-10))

        # Annualized return
        n_days = len(portfolio)
        total_return = float(equity[-1] / equity[0] - 1) if n_days > 0 else 0.0
        ann_return = (1 + total_return) ** (252 / max(n_days, 1)) - 1

        # Turnover (avg daily trades / total stocks)
        turnover = len(trades) / max(n_days, 1)

        return {
            "sharpe_ratio": round(sharpe, 4),
            "max_drawdown": round(max_dd, 4),
            "annualized_return": round(ann_return, 4),
            "total_return": round(total_return, 4),
            "win_rate": round(win_rate, 4),
            "profit_factor": round(profit_factor, 4),
            "turnover": round(turnover, 4),
            "n_trades": len(trades),
        }
