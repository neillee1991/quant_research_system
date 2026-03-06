"""
回测服务层
封装回测执行和结果分析逻辑
"""
from typing import Any, Dict, Optional
import polars as pl

from app.core.interfaces import IBacktestEngine
from app.core.exceptions import BacktestConfigError, InsufficientDataError
from app.core.logger import logger
from app.core.config import settings
from engine.backtester.vector_engine import VectorEngine, BacktestConfig, BacktestResult


class BacktestService:
    """回测服务"""

    def __init__(self, backtest_engine: Optional[IBacktestEngine] = None):
        self.backtest_engine = backtest_engine or VectorEngine()

    def run_backtest(
        self,
        price_data: pl.DataFrame,
        signals: pl.DataFrame,
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行回测"""
        # 验证数据
        self._validate_backtest_data(price_data, signals)

        # 构建配置
        backtest_config = self._build_backtest_config(config)

        try:
            # 合并价格数据和信号
            join_cols = ["trade_date"]
            if "ts_code" in price_data.columns and "ts_code" in signals.columns:
                join_cols.append("ts_code")
            merged_df = price_data.join(signals, on=join_cols, how="inner")

            # 执行回测（VectorEngine 接受合并后的 DataFrame）
            self.backtest_engine = VectorEngine(config=backtest_config)
            result = self.backtest_engine.run(merged_df, "signal")

            # 转换结果
            return self._format_backtest_result(result)

        except Exception as e:
            logger.error(f"Backtest execution failed: {e}")
            raise

    def run_strategy_backtest(
        self,
        data: pl.DataFrame,
        strategy_config: Dict[str, Any],
        backtest_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行策略回测（从策略配置生成信号）"""
        # 这里需要根据策略配置生成信号
        # 暂时简化处理
        signals = self._generate_signals_from_strategy(data, strategy_config)

        return self.run_backtest(data, signals, backtest_config)

    def analyze_backtest_result(
        self,
        result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """分析回测结果"""
        try:
            analysis = {
                "performance_summary": {
                    "total_return": result.get("total_return", 0),
                    "annual_return": result.get("annual_return", 0),
                    "sharpe_ratio": result.get("sharpe_ratio", 0),
                    "max_drawdown": result.get("max_drawdown", 0),
                },
                "trade_statistics": {
                    "total_trades": result.get("total_trades", 0),
                    "win_rate": result.get("win_rate", 0),
                    "profit_factor": result.get("profit_factor", 0),
                },
                "risk_metrics": {
                    "volatility": result.get("volatility", 0),
                    "downside_deviation": result.get("downside_deviation", 0),
                    "var_95": result.get("var_95", 0),
                }
            }

            logger.info("Backtest analysis completed")
            return analysis

        except Exception as e:
            logger.error(f"Failed to analyze backtest result: {e}")
            raise

    def _validate_backtest_data(
        self,
        price_data: pl.DataFrame,
        signals: pl.DataFrame
    ) -> None:
        """验证回测数据"""
        if price_data.is_empty():
            raise InsufficientDataError(1, 0, context={"data_type": "price_data"})

        if signals.is_empty():
            raise InsufficientDataError(1, 0, context={"data_type": "signals"})

        # 检查必需列
        required_price_cols = ["trade_date", "close"]
        missing_cols = [col for col in required_price_cols if col not in price_data.columns]
        if missing_cols:
            raise BacktestConfigError(
                f"Missing required columns in price_data: {missing_cols}"
            )

        required_signal_cols = ["trade_date", "signal"]
        missing_cols = [col for col in required_signal_cols if col not in signals.columns]
        if missing_cols:
            raise BacktestConfigError(
                f"Missing required columns in signals: {missing_cols}"
            )

        # 检查数据量
        min_data_points = 20
        if len(price_data) < min_data_points:
            raise InsufficientDataError(
                min_data_points,
                len(price_data),
                context={"data_type": "price_data"}
            )

    def _build_backtest_config(
        self,
        config: Optional[Dict[str, Any]] = None
    ) -> BacktestConfig:
        """构建回测配置"""
        config = config or {}

        return BacktestConfig(
            initial_capital=config.get(
                "initial_capital",
                settings.backtest.initial_capital
            ),
            commission_rate=config.get(
                "commission_rate",
                settings.backtest.commission_rate
            ),
            slippage_rate=config.get(
                "slippage_rate",
                settings.backtest.slippage_rate
            )
        )

    def _format_backtest_result(
        self,
        result: BacktestResult
    ) -> Dict[str, Any]:
        """格式化回测结果"""
        return {
            **result.metrics,
            "equity_curve": result.equity_curve.to_dicts() if not result.equity_curve.is_empty() else [],
            "trades": result.trades.to_dicts() if not result.trades.is_empty() else [],
        }

    def _generate_signals_from_strategy(
        self,
        data: pl.DataFrame,
        strategy_config: Dict[str, Any]
    ) -> pl.DataFrame:
        """从策略配置生成信号（简化实现）"""
        # 这里应该根据策略配置生成信号
        # 暂时返回简单的移动平均交叉信号
        logger.warning("Using simplified signal generation")

        # 计算短期和长期均线
        short_window = strategy_config.get("short_window", 5)
        long_window = strategy_config.get("long_window", 20)

        df = data.with_columns([
            pl.col("close").rolling_mean(short_window).alias("ma_short"),
            pl.col("close").rolling_mean(long_window).alias("ma_long")
        ])

        # 生成信号
        df = df.with_columns([
            pl.when(pl.col("ma_short") > pl.col("ma_long"))
            .then(1)
            .when(pl.col("ma_short") < pl.col("ma_long"))
            .then(-1)
            .otherwise(0)
            .alias("signal")
        ])

        return df.select(["trade_date", "signal"])
