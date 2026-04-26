"""
因子计算服务
提供基于数据库驱动的因子计算编排功能
"""
import polars as pl
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

from app.core.logger import logger
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


def get_preprocess_loader():
    """获取预处理配置加载器（供测试使用）"""
    class PreprocessLoader:
        def get_default_profile(self):
            return DEFAULT_PREPROCESS

        def get_profile(self, profile_name):
            return DEFAULT_PREPROCESS

    return PreprocessLoader()


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

    def _save_results(self, factor_id: str, df: pl.DataFrame):
        """保存计算结果"""
        self.db.upsert("factor_values", df, ["ts_code", "trade_date", "factor_id"])
        if not df.is_empty():
            logger.info(f"Saved {len(df)} rows for factor {factor_id}")

    def _create_run_record(self, factor_id: str, mode: str, start_date: str, end_date: str, preprocess_options: Dict):
        """创建运行记录"""
        run_id = f"{factor_id}_{mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        record = {
            "run_id": run_id,
            "factor_id": factor_id,
            "mode": mode,
            "start_date": start_date,
            "end_date": end_date,
            "preprocess": preprocess_options,
            "status": "running",
            "started_at": datetime.now().strftime("%Y%m%d %H:%M:%S")
        }
        self.db.upsert("production_task_run", pl.DataFrame([record]), ["run_id"])
        return run_id

    def _finish_run_record(self, run_id: str, status: str, rows: int, started_at: datetime, error_message: Optional[str] = None):
        """完成运行记录"""
        duration = (datetime.now() - started_at).total_seconds()
        record = {
            "run_id": run_id,
            "status": status,
            "finished_at": datetime.now().strftime("%Y%m%d %H:%M:%S"),
            "duration_seconds": duration,
            "rows_processed": rows
        }
        if error_message:
            record["error_message"] = error_message
        self.db.upsert("production_task_run", pl.DataFrame([record]), ["run_id"])
        logger.info(f"Run {run_id} completed with {rows} rows in {duration:.1f} seconds")

    def _update_metadata(self, factor_id: str, definition: FactorDefinition, last_date: str, rows: int):
        """更新因子配置元数据"""
        record = {
            "factor_id": factor_id,
            "last_date": last_date,
            "rows": rows,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.db.upsert("factor_configs", pl.DataFrame([record]), ["factor_id"])
