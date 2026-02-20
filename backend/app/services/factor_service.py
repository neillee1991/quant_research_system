"""
因子服务层
封装因子计算和分析逻辑
"""
from typing import Dict, List, Optional
import polars as pl

from app.core.interfaces import IFactorEngine
from app.core.exceptions import FactorComputationError, UnsupportedFactorError
from app.core.logger import logger
from engine.factors.technical import TechnicalFactors
from engine.factors.financial import CrossSectionalFactors, FactorAnalyzer


class FactorService:
    """因子服务"""

    def __init__(self, factor_engine: Optional[IFactorEngine] = None):
        self.factor_engine = factor_engine
        self.technical_factors = TechnicalFactors
        self.cross_sectional_factors = CrossSectionalFactors
        self.factor_analyzer = FactorAnalyzer

    def compute_technical_indicators(
        self,
        data: pl.DataFrame,
        indicators: List[str],
        params: Optional[Dict[str, int]] = None
    ) -> pl.DataFrame:
        """计算技术指标"""
        if data.is_empty():
            raise FactorComputationError("technical_indicators", "Empty input data")

        try:
            result = data.clone()

            for indicator in indicators:
                indicator_lower = indicator.lower()

                if indicator_lower == "ma":
                    window = params.get("ma_window", 20) if params else 20
                    result = self.technical_factors.moving_average(result, window)

                elif indicator_lower == "ema":
                    window = params.get("ema_window", 12) if params else 12
                    result = self.technical_factors.exponential_ma(result, window)

                elif indicator_lower == "rsi":
                    window = params.get("rsi_window", 14) if params else 14
                    result = self.technical_factors.rsi(result, window)

                elif indicator_lower == "macd":
                    result = self.technical_factors.macd(result)

                elif indicator_lower == "kdj":
                    result = self.technical_factors.kdj(result)

                elif indicator_lower == "bollinger":
                    window = params.get("bollinger_window", 20) if params else 20
                    result = self.technical_factors.bollinger_bands(result, window)

                elif indicator_lower == "atr":
                    window = params.get("atr_window", 14) if params else 14
                    result = self.technical_factors.atr(result, window)

                else:
                    raise UnsupportedFactorError(indicator)

            logger.info(f"Computed {len(indicators)} technical indicators")
            return result

        except UnsupportedFactorError:
            raise
        except Exception as e:
            logger.error(f"Failed to compute technical indicators: {e}")
            raise FactorComputationError("technical_indicators", str(e))

    def compute_cross_sectional_factors(
        self,
        data: pl.DataFrame,
        factors: List[str],
        date_col: str = "trade_date"
    ) -> pl.DataFrame:
        """计算截面因子"""
        if data.is_empty():
            raise FactorComputationError("cross_sectional_factors", "Empty input data")

        try:
            result = data.clone()

            for factor in factors:
                factor_lower = factor.lower()

                if factor_lower == "rank":
                    result = self.cross_sectional_factors.rank_factor(
                        result, "close", date_col
                    )

                elif factor_lower == "zscore":
                    result = self.cross_sectional_factors.zscore_factor(
                        result, "close", date_col
                    )

                elif factor_lower == "industry_neutral":
                    if "industry" in result.columns:
                        result = self.cross_sectional_factors.industry_neutral_factor(
                            result, "close", "industry", date_col
                        )
                    else:
                        logger.warning("Industry column not found, skipping industry_neutral")

                else:
                    raise UnsupportedFactorError(factor)

            logger.info(f"Computed {len(factors)} cross-sectional factors")
            return result

        except UnsupportedFactorError:
            raise
        except Exception as e:
            logger.error(f"Failed to compute cross-sectional factors: {e}")
            raise FactorComputationError("cross_sectional_factors", str(e))

    def analyze_factor_performance(
        self,
        factor_data: pl.DataFrame,
        return_data: pl.DataFrame,
        factor_col: str,
        return_col: str = "return"
    ) -> Dict[str, float]:
        """分析因子表现"""
        try:
            ic = self.factor_analyzer.information_coefficient(
                factor_data, factor_col, return_col
            )

            rank_ic = self.factor_analyzer.rank_ic(
                factor_data, factor_col, return_col
            )

            result = {
                "ic": ic,
                "rank_ic": rank_ic,
                "abs_ic": abs(ic),
                "abs_rank_ic": abs(rank_ic)
            }

            logger.info(f"Factor analysis: IC={ic:.4f}, Rank IC={rank_ic:.4f}")
            return result

        except Exception as e:
            logger.error(f"Failed to analyze factor performance: {e}")
            raise FactorComputationError("factor_analysis", str(e))

    def compute_factor_returns(
        self,
        data: pl.DataFrame,
        factor_col: str,
        n_quantiles: int = 5
    ) -> pl.DataFrame:
        """计算因子分层收益"""
        try:
            result = self.factor_analyzer.factor_quantile_returns(
                data, factor_col, n_quantiles
            )

            logger.info(f"Computed factor returns for {n_quantiles} quantiles")
            return result

        except Exception as e:
            logger.error(f"Failed to compute factor returns: {e}")
            raise FactorComputationError("factor_returns", str(e))
