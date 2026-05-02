"""
VectorBT 投资组合管理 - 计算绩效指标
"""
from typing import Dict, Any, List
import pandas as pd


class VectorBTPortfolio:
    """VectorBT 投资组合管理"""

    @staticmethod
    def calculate_performance_metrics(portfolio) -> Dict[str, Any]:
        """计算绩效指标"""
        stats = portfolio.stats()

        return {
            "total_return": float(stats['Total Return [%]']) if 'Total Return [%]' in stats else 0.0,
            "annual_return": float(stats['Annual Return [%]']) if 'Annual Return [%]' in stats else 0.0,
            "max_drawdown": float(stats['Max Drawdown [%]']) if 'Max Drawdown [%]' in stats else 0.0,
            "sharpe_ratio": float(stats['Sharpe Ratio']) if 'Sharpe Ratio' in stats else 0.0,
            "win_rate": float(stats['Win Rate [%]']) if 'Win Rate [%]' in stats else 0.0,
            "profit_factor": float(stats['Profit Factor']) if 'Profit Factor' in stats else 0.0,
        }

    @staticmethod
    def generate_equity_curve(portfolio) -> pd.Series:
        """生成净值曲线"""
        return portfolio.total_portfolio_value()

    @staticmethod
    def generate_trade_analysis(portfolio) -> List[Dict[str, Any]]:
        """生成交易分析"""
        trades = portfolio.trades.records

        if trades.empty:
            return []

        return [
            {
                "entry_date": str(trade['Entry Date']),
                "exit_date": str(trade['Exit Date']),
                "entry_price": float(trade['Entry Price']),
                "exit_price": float(trade['Exit Price']),
                "quantity": float(trade['Size']),
                "pnl": float(trade['Return [%]']),
            }
            for _, trade in trades.iterrows()
        ]
