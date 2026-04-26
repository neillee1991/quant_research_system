"""
报告生成器 - 统一绩效指标计算
"""
from typing import Dict, Any
import pandas as pd
import numpy as np
from app.core.logger import logger


class ReportGenerator:
    """报告生成器"""

    @staticmethod
    def generate_report(results: Dict[str, Any]) -> Dict[str, Any]:
        """生成综合报告"""
        report = {
            "summary": {},
            "performance": {},
            "risk": {},
            "trades": [],
            "equity_curve": None,
        }

        if "metrics" in results:
            report["performance"] = results["metrics"]

        if "equity_curve" in results:
            report["risk"] = ReportGenerator._calculate_risk_metrics(
                results["equity_curve"]
            )

        if "trades" in results:
            report["trades"] = ReportGenerator._analyze_trades(
                results["trades"]
            )

        if "equity_curve" in results:
            report["equity_curve"] = ReportGenerator._prepare_equity_curve(
                results["equity_curve"]
            )

        return report

    @staticmethod
    def _calculate_risk_metrics(equity_curve: pd.Series) -> Dict[str, float]:
        """计算风险指标"""
        returns = equity_curve.pct_change().dropna()

        return {
            "max_drawdown": float(ReportGenerator._calculate_max_drawdown(equity_curve)),
            "volatility": float(np.std(returns)),
            "sharpe_ratio": float(np.mean(returns) / np.std(returns) * np.sqrt(252)),
            "var_95": float(np.percentile(returns, 5)),
        }

    @staticmethod
    def _calculate_max_drawdown(equity_curve: pd.Series) -> float:
        """计算最大回撤"""
        rolling_max = equity_curve.expanding().max()
        drawdown = (equity_curve - rolling_max) / rolling_max
        return drawdown.min()

    @staticmethod
    def _analyze_trades(trades: pd.DataFrame) -> Dict[str, Any]:
        """分析交易"""
        if trades.empty:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0,
            }

        winning_trades = trades[trades['pnl'] > 0]
        losing_trades = trades[trades['pnl'] < 0]

        return {
            "total_trades": len(trades),
            "winning_trades": len(winning_trades),
            "losing_trades": len(losing_trades),
            "win_rate": len(winning_trades) / len(trades),
            "avg_win": float(winning_trades['pnl'].mean()),
            "avg_loss": float(losing_trades['pnl'].mean()),
        }

    @staticmethod
    def _prepare_equity_curve(equity_curve: pd.Series) -> Dict[str, Any]:
        """准备净值曲线数据"""
        return {
            "dates": equity_curve.index.astype(str).tolist(),
            "values": equity_curve.values.tolist(),
        }
