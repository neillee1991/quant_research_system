"""
因子服务层
封装因子计算和分析逻辑
"""
from typing import Dict, List, Optional
import polars as pl

from app.core.interfaces import IFactorEngine
from app.core.exceptions import FactorComputationError, UnsupportedFactorError
from app.core.logger import logger
from engine.factors.technical import TechnicalFactors, CrossSectionalFactors
from engine.factors.financial import FactorAnalyzer


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
            tf = self.technical_factors

            for indicator in indicators:
                indicator_lower = indicator.lower()

                if indicator_lower == "ma":
                    window = params.get("ma_window", 20) if params else 20
                    result = result.with_columns(
                        tf.sma(result["close"], window).alias(f"ma{window}")
                    )

                elif indicator_lower == "ema":
                    window = params.get("ema_window", 12) if params else 12
                    result = result.with_columns(
                        tf.ema(result["close"], window).alias(f"ema{window}")
                    )

                elif indicator_lower == "rsi":
                    window = params.get("rsi_window", 14) if params else 14
                    result = result.with_columns(
                        tf.rsi(result["close"], window).alias("rsi")
                    )

                elif indicator_lower == "macd":
                    macd_line, signal_line, histogram = tf.macd(result["close"])
                    result = result.with_columns([
                        macd_line.alias("macd"),
                        signal_line.alias("macd_signal"),
                        histogram.alias("macd_hist"),
                    ])

                elif indicator_lower == "kdj":
                    k, d, j = tf.kdj(result["high"], result["low"], result["close"])
                    result = result.with_columns([
                        k.alias("k"), d.alias("d"), j.alias("j"),
                    ])

                elif indicator_lower == "bollinger":
                    window = params.get("bollinger_window", 20) if params else 20
                    upper, mid, lower = tf.bollinger_bands(result["close"], window)
                    result = result.with_columns([
                        upper.alias("bb_upper"),
                        mid.alias("bb_mid"),
                        lower.alias("bb_lower"),
                    ])

                elif indicator_lower == "atr":
                    window = params.get("atr_window", 14) if params else 14
                    result = result.with_columns(
                        tf.atr(result["high"], result["low"], result["close"], window).alias("atr")
                    )

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
                    result = self.cross_sectional_factors.rank(result, "close")

                elif factor_lower == "zscore":
                    result = self.cross_sectional_factors.zscore(result, "close")

                elif factor_lower == "industry_neutral":
                    if "industry" in result.columns:
                        result = self.cross_sectional_factors.neutralize(
                            result, "close", "industry"
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
            ic = self.factor_analyzer.ic(
                factor_data[factor_col], factor_data[return_col]
            )

            rank_ic = self.factor_analyzer.rank_ic(
                factor_data[factor_col], factor_data[return_col]
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
            result = self.factor_analyzer.layered_returns(
                data, factor_col, "return", n_quantiles
            )

            logger.info(f"Computed factor returns for {n_quantiles} quantiles")
            return result

        except Exception as e:
            logger.error(f"Failed to compute factor returns: {e}")
            raise FactorComputationError("factor_returns", str(e))


# ==================== 生产因子计算服务 ====================

import polars as pl
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

from app.core.utils import DateUtils, TradingCalendar
from engine.factor.registry import get_factor, discover_factors, FactorDefinition
from engine.factor.data_config import DataConfigLoader
from infrastructure.processor.pipeline import ProcessContext
from infrastructure.processor.pipeline_factory import PipelineFactory

DEFAULT_LOOKBACK_DAYS = 60

DEFAULT_PREPROCESS = {
    "adjust_price": "forward",
    "filter_st": True,
    "filter_new_stock": True,
    "new_stock_days": 60,
    "mark_limit": True,
}


@dataclass
class ComputeResult:
    """因子计算结果"""
    success: bool
    factor_id: str
    rows: int
    elapsed_seconds: float
    calc_start: str
    calc_end: str
    message: Optional[str] = None
    quality_metrics: Optional[Dict[str, Any]] = None


class FactorComputeService:
    """因子计算服务 - 服务编排模式"""

    def __init__(self, db_client):
        self.db = db_client
        self.trading_cal = TradingCalendar.get_instance(db_client)
        self.data_config = DataConfigLoader(db_client)
        self.pipeline_factory = PipelineFactory(db_client, self.data_config, self.trading_cal)
        self._register_config_tables()

    def _register_config_tables(self):
        try:
            config = self.data_config.load()
            for cfg in config.values():
                table_name = cfg.get("table_name", "")
                if table_name and table_name not in self.db._ALL_TABLES:
                    self.db.register_meta_table(table_name)
                extra = cfg.get("extra_config", {})
                if isinstance(extra, dict):
                    for key in ("price_table",):
                        ref_table = extra.get(key, "")
                        if ref_table and ref_table not in self.db._ALL_TABLES:
                            self.db.register_meta_table(ref_table)
        except Exception as e:
            logger.debug(f"注册 data_config 表名失败: {e}")

    def compute_factor(
        self,
        factor_id: str,
        target_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: Optional[str] = None,
        preprocess: Optional[Dict[str, Any]] = None,
        preprocess_profile: Optional[str] = None,
        save_results: bool = True,
        run_id: Optional[str] = None,
    ) -> ComputeResult:
        """执行因子计算"""
        started_at = datetime.now()
        calc_start = ""
        calc_end = ""

        try:
            discover_factors(db_client=self.db)
            definition = get_factor(factor_id)
            if not definition:
                return ComputeResult(
                    success=False, factor_id=factor_id, rows=0,
                    elapsed_seconds=0, calc_start="", calc_end="",
                    message=f"Factor not found: {factor_id}"
                )

            preprocess_options = self._resolve_preprocess_options(
                factor_id, definition, preprocess, preprocess_profile
            )
            compute_mode = mode or definition.compute_mode or "incremental"
            calc_start, calc_end, data_start = self._resolve_dates(
                factor_id, compute_mode, target_date, start_date, end_date, definition
            )

            if calc_start is None:
                return ComputeResult(
                    success=False, factor_id=factor_id, rows=0,
                    elapsed_seconds=0, calc_start="", calc_end="",
                    message="Invalid date range"
                )

            logger.info(
                f"Factor {factor_id}: computing [{calc_start}, {calc_end}], "
                f"loading data from {data_start}"
            )

            context = ProcessContext(
                factor_id=factor_id,
                factor_definition=definition,
                calc_start=calc_start,
                calc_end=calc_end,
                data_start=data_start,
                preprocess_options=preprocess_options,
                run_id=run_id,
                dataframe=pl.DataFrame()
            )
            pipeline = self.pipeline_factory.create_factor_pipeline(
                factor_id=factor_id,
                preprocess_options=preprocess_options,
                save_results=save_results
            )
            result_df = pipeline.execute(context)

            elapsed = (datetime.now() - started_at).total_seconds()
            rows = context.get_state("saved_rows") or (len(result_df) if result_df is not None else 0)
            quality_metrics = context.get_state("quality_metrics")

            if save_results and rows > 0:
                self._update_metadata(factor_id, definition, calc_end, rows)

            logger.info(f"Factor {factor_id} completed: {rows} rows in {elapsed:.1f}s")
            return ComputeResult(
                success=True, factor_id=factor_id, rows=rows,
                elapsed_seconds=elapsed, calc_start=calc_start, calc_end=calc_end,
                quality_metrics=quality_metrics
            )

        except Exception as e:
            elapsed = (datetime.now() - started_at).total_seconds()
            logger.error(f"Factor {factor_id} failed: {e}", exc_info=True)
            return ComputeResult(
                success=False, factor_id=factor_id, rows=0,
                elapsed_seconds=elapsed, calc_start=calc_start, calc_end=calc_end,
                message=str(e)
            )

    def _resolve_preprocess_options(
        self,
        factor_id: str,
        definition: FactorDefinition,
        explicit_options: Optional[Dict[str, Any]],
        profile_name: Optional[str]
    ) -> Dict[str, Any]:
        base_options = {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
            "new_stock_days": 60,
            "mark_limit": True,
        }
        factor_options = definition.params.get("preprocess", {}) if definition.params else {}
        merged = {**base_options, **factor_options}
        db_options = self._get_factor_preprocess(factor_id)
        merged = {**merged, **db_options}
        if explicit_options:
            merged = {**merged, **explicit_options}
        logger.debug(f"Resolved preprocess options for {factor_id}: {merged}")
        return merged

    def _resolve_dates(
        self,
        factor_id: str,
        compute_mode: str,
        target_date: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        definition: FactorDefinition
    ):
        today = datetime.now().strftime("%Y%m%d")

        if compute_mode == "full":
            calc_start = start_date or "20100101"
            calc_end = end_date or today
            lookback = definition.params.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
            data_start = self.trading_cal.offset_trading_days(calc_start, -lookback) if start_date else calc_start
            return calc_start, calc_end, data_start

        if target_date:
            calc_start = target_date
            calc_end = end_date or target_date
        elif start_date:
            calc_start = start_date
            calc_end = end_date or start_date
        else:
            last_date = self._get_last_computed_date(factor_id)
            calc_start = DateUtils.add_days(last_date, 1) if last_date else today
            calc_end = today

        if calc_start > calc_end:
            return None, None, None

        lookback = definition.params.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
        data_start = self.trading_cal.offset_trading_days(calc_start, -lookback)
        return calc_start, calc_end, data_start

    def _get_factor_preprocess(self, factor_id: str) -> Dict[str, Any]:
        import json
        import psycopg2
        import psycopg2.extras
        from app.core.config import settings
        try:
            conn = psycopg2.connect(
                host=settings.postgresql.postgres_host,
                port=settings.postgresql.postgres_port,
                dbname=settings.postgresql.postgres_db,
                user=settings.postgresql.postgres_user,
                password=settings.postgresql.postgres_password,
            )
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT params FROM factor_configs WHERE factor_id = %s", (factor_id,))
                    row = cur.fetchone()
            conn.close()
            if row:
                params = row["params"]
                if isinstance(params, dict):
                    return params.get("preprocess", {})
                elif isinstance(params, str):
                    try:
                        return json.loads(params).get("preprocess", {})
                    except json.JSONDecodeError:
                        return {}
        except Exception as e:
            logger.debug(f"Failed to get factor preprocess from DB: {e}")
        return {}

    def _get_last_computed_date(self, factor_id: str) -> Optional[str]:
        try:
            result = self.db.query(
                "SELECT max(trade_date) as last_date FROM factor_values WHERE factor_id = %s",
                (factor_id,)
            )
            if not result.is_empty() and result["last_date"][0]:
                last_date = str(result["last_date"][0])
                if " " in last_date:
                    last_date = last_date.split()[0].replace("-", "")
                return last_date
        except Exception as e:
            logger.debug(f"Failed to get last computed date: {e}")
        return None

    def _update_metadata(self, factor_id: str, definition: FactorDefinition, last_date: str, rows: int):
        import psycopg2
        from app.core.config import settings
        try:
            conn = psycopg2.connect(
                host=settings.postgresql.postgres_host,
                port=settings.postgresql.postgres_port,
                dbname=settings.postgresql.postgres_db,
                user=settings.postgresql.postgres_user,
                password=settings.postgresql.postgres_password,
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE factor_configs SET updated_at = NOW() WHERE factor_id = %s", (factor_id,))
            conn.close()
        except Exception as e:
            logger.warning(f"Failed to update metadata: {e}")
