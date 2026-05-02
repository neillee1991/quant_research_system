"""
VectorBT 引擎 - 向量化回测
"""
from typing import Dict, Any, List
import polars as pl
import pandas as pd
from app.core.logger import logger
from backend.engine.backtest.data_pipeline.prefetcher import DataPrefetcher
from backend.engine.backtest.core.base_strategy import BaseStrategy


class VectorBTEngine:
    """VectorBT 引擎"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.prefetcher = DataPrefetcher(config)

    async def run(
        self,
        strategy: BaseStrategy,
        start_date: str,
        end_date: str,
        factors: List[str],
        stocks: List[str] = None
    ) -> Dict[str, Any]:
        """执行回测"""
        logger.info(f"开始 VectorBT 回测: {start_date} 到 {end_date}")

        # 1. 获取宽表数据
        wide_table = await self.prefetcher.prefetch_data(
            start_date, end_date, factors, stocks
        )

        # 2. 转换为 VectorBT 可接受格式
        prices = self._extract_prices(wide_table)
        factor_data = self._extract_factors(wide_table, factors)

        # 3. 执行策略
        portfolio = self._execute_strategy(strategy, prices, factor_data)

        # 4. 生成报告
        report = self._generate_report(portfolio)

        logger.info("VectorBT 回测完成")
        return report

    def _extract_prices(self, wide_table: pl.DataFrame) -> pd.DataFrame:
        """提取价格数据（长表转宽表）"""
        price_fields = ["open", "high", "low", "close", "volume", "amount"]
        price_data = wide_table[["ts_code", "trade_date"] + price_fields]

        # 使用 Polars 进行 pivot 操作
        pivot_tables = {}
        for field in price_fields:
            # 创建单个字段的 pivot
            pivot = price_data.pivot(
                index="trade_date",
                columns="ts_code",
                values=field
            )
            pivot_tables[field] = pivot

        # 合并所有字段
        result_dfs = []
        for field, df in pivot_tables.items():
            df_renamed = df.with_columns([pl.lit(field).alias("field")])
            result_dfs.append(df_renamed)

        # 转换为 Pandas MultiIndex DataFrame 格式（VectorBT 需要）
        pandas_data = pd.concat({k: v.to_pandas() for k, v in pivot_tables.items()}, axis=1)

        return pandas_data

    def _extract_factors(self, wide_table: pl.DataFrame, factors: List[str]) -> Dict[str, pd.Series]:
        """提取因子数据"""
        factor_data = {}

        for factor_id in factors:
            factor_pivot = wide_table.pivot(
                index="trade_date",
                columns="ts_code",
                values=factor_id
            )

            factor_data[factor_id] = factor_pivot.to_pandas()

        return factor_data

    def _execute_strategy(
        self,
        strategy: BaseStrategy,
        prices: pd.DataFrame,
        factor_data: Dict[str, pd.Series]
    ):
        """执行 VectorBT 策略"""
        try:
            import vectorbt as vbt
            vbt.settings.set_theme("dark")
        except ImportError:
            logger.error("VectorBT 未安装")
            raise ImportError("VectorBT 未安装")

        # 生成信号
        entries, exits = self._generate_signals(strategy, prices, factor_data)

        # 创建回测器
        if "close" in prices.columns.get_level_values(0):
            close_prices = prices["close"]
            portfolio = vbt.Portfolio.from_signals(
                close_prices,
                entries,
                exits,
                init_cash=self.config.get('initial_capital', 1000000),
                fees=self.config.get('fees', 0.0003),
                slippage=self.config.get('slippage', 0.001)
            )

            return portfolio

        raise ValueError("价格数据中缺少 close 字段")

    def _generate_signals(
        self,
        strategy: BaseStrategy,
        prices: pd.DataFrame,
        factor_data: Dict[str, pd.Series]
    ):
        """生成信号"""
        try:
            signals = strategy.generate_signals(prices, factor_data)
            if signals:
                logger.warning("generate_signals 返回了信号，但当前实现使用简化逻辑")
        except NotImplementedError:
            logger.debug("策略未实现 generate_signals，使用默认空信号")

        # 默认返回空信号
        entries = pd.DataFrame(False, index=prices.index, columns=prices.columns.get_level_values(1))
        exits = pd.DataFrame(False, index=prices.index, columns=prices.columns.get_level_values(1))

        return entries, exits

    def _generate_report(self, portfolio):
        """生成报告"""
        if portfolio is None:
            return {
                "metrics": {},
                "equity_curve": None,
                "trades": []
            }

        stats = portfolio.stats()

        return {
            "metrics": stats.to_dict(),
            "equity_curve": portfolio.total_portfolio_value(),
            "trades": portfolio.trades.records
        }
