"""
FactorComputeService - 因子计算服务（重构后的 ProductionEngine）

职责：
1. 服务编排 - 协调各个组件完成因子计算
2. 日期解析 - 确定计算范围和数据加载范围
3. Pipeline 构建 - 根据配置构建数据处理管道
4. 结果管理 - 保存结果和更新元数据
"""
import polars as pl
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

from app.core.logger import logger
from app.core.utils import DateUtils, TradingCalendar
from engine.production.registry import get_factor, discover_factors, FactorDefinition
from engine.production.data_config import DataConfigLoader
from config.preprocess_loader import get_preprocess_loader
from infrastructure.processor.pipeline import ProcessContext
from infrastructure.processor.pipeline_factory import PipelineFactory


# 增量计算时，需要额外加载的历史窗口天数（用于滚动计算）
DEFAULT_LOOKBACK_DAYS = 60


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
        """初始化服务

        Args:
            db_client: DolphinDB 客户端
        """
        self.db = db_client
        self.trading_cal = TradingCalendar.get_instance(db_client)
        self.data_config = DataConfigLoader(db_client)
        self.preprocess_loader = get_preprocess_loader()
        self.pipeline_factory = PipelineFactory(db_client, self.data_config, self.trading_cal)

        # 注册配置表
        self._register_config_tables()

    def _register_config_tables(self):
        """将 data_config 中引用的表名注册到 db._ALL_TABLES"""
        try:
            config = self.data_config.load()
            for cfg in config.values():
                table_name = cfg.get("table_name", "")
                if table_name and table_name not in self.db._ALL_TABLES:
                    self.db.register_meta_table(table_name)
                # 注册 extra_config 中引用的表
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
    ) -> ComputeResult:
        """执行因子计算

        Args:
            factor_id: 因子ID
            target_date: 目标日期（增量模式下只算这一天）
            start_date: 开始日期（范围计算）
            end_date: 结束日期（范围计算）
            mode: 强制指定计算模式 ("incremental" / "full")
            preprocess: 预处理选项（优先级最高）
            preprocess_profile: 预处理配置名称（default, conservative, aggressive）
            save_results: 是否保存结果到数据库

        Returns:
            ComputeResult 计算结果
        """
        started_at = datetime.now()

        try:
            # 1. 获取因子定义
            discover_factors(db_client=self.db)
            definition = get_factor(factor_id)
            if not definition:
                return ComputeResult(
                    success=False,
                    factor_id=factor_id,
                    rows=0,
                    elapsed_seconds=0,
                    calc_start="",
                    calc_end="",
                    message=f"Factor not found: {factor_id}"
                )

            # 2. 解析预处理选项（优先级：显式传入 > profile > DB > 代码 > 默认）
            preprocess_options = self._resolve_preprocess_options(
                factor_id, definition, preprocess, preprocess_profile
            )

            # 3. 解析日期范围
            compute_mode = mode or definition.compute_mode or "incremental"
            calc_start, calc_end, data_start = self._resolve_dates(
                factor_id, compute_mode, target_date, start_date, end_date, definition
            )

            if calc_start is None:
                return ComputeResult(
                    success=False,
                    factor_id=factor_id,
                    rows=0,
                    elapsed_seconds=0,
                    calc_start="",
                    calc_end="",
                    message="Invalid date range"
                )

            logger.info(
                f"Factor {factor_id}: computing [{calc_start}, {calc_end}], "
                f"loading data from {data_start}"
            )

            # 4. 创建运行记录
            run_id = self._create_run_record(factor_id, calc_start, calc_end, compute_mode)

            # 5. 构建处理上下文
            context = ProcessContext(
                factor_id=factor_id,
                factor_definition=definition,
                calc_start=calc_start,
                calc_end=calc_end,
                data_start=data_start,
                preprocess_options=preprocess_options,
                run_id=run_id,
                dataframe=pl.DataFrame()  # 初始空 DataFrame
            )

            # 6. 构建数据处理管道
            pipeline = self.pipeline_factory.create_factor_pipeline(
                factor_id=factor_id,
                preprocess_options=preprocess_options,
                save_results=save_results
            )

            # 7. 执行管道
            result_df = pipeline.execute(context)

            # 8. 获取结果统计
            rows = context.get_state("saved_rows", len(result_df) if result_df is not None else 0)
            quality_metrics = context.get_state("quality_metrics")

            # 9. 更新因子元数据
            if save_results and rows > 0:
                self._update_metadata(factor_id, definition, calc_end, rows)

            # 10. 完成运行记录
            elapsed = (datetime.now() - started_at).total_seconds()
            self._finish_run_record(run_id, "success", rows, started_at)

            logger.info(f"Factor {factor_id} completed: {rows} rows in {elapsed:.1f}s")

            return ComputeResult(
                success=True,
                factor_id=factor_id,
                rows=rows,
                elapsed_seconds=elapsed,
                calc_start=calc_start,
                calc_end=calc_end,
                quality_metrics=quality_metrics
            )

        except Exception as e:
            elapsed = (datetime.now() - started_at).total_seconds()
            logger.error(f"Factor {factor_id} failed: {e}", exc_info=True)

            # 更新运行记录为失败
            if 'run_id' in locals():
                self._finish_run_record(run_id, "failed", 0, started_at, str(e))

            return ComputeResult(
                success=False,
                factor_id=factor_id,
                rows=0,
                elapsed_seconds=elapsed,
                calc_start=calc_start if 'calc_start' in locals() else "",
                calc_end=calc_end if 'calc_end' in locals() else "",
                message=str(e)
            )

    def _resolve_preprocess_options(
        self,
        factor_id: str,
        definition: FactorDefinition,
        explicit_options: Optional[Dict[str, Any]],
        profile_name: Optional[str]
    ) -> Dict[str, Any]:
        """解析预处理选项（多层优先级）

        优先级：显式传入 > profile > DB > 代码 > 默认配置

        Args:
            factor_id: 因子ID
            definition: 因子定义
            explicit_options: 显式传入的选项
            profile_name: 配置名称

        Returns:
            合并后的预处理选项
        """
        # 1. 加载基础配置（profile 或 default）
        if profile_name:
            base_options = self.preprocess_loader.get_profile(profile_name)
        else:
            base_options = self.preprocess_loader.get_default_profile()

        # 2. 合并代码中的因子定义
        factor_options = definition.params.get("preprocess", {}) if definition.params else {}
        merged = {**base_options, **factor_options}

        # 3. 合并数据库中的配置
        db_options = self._get_factor_preprocess(factor_id)
        merged = {**merged, **db_options}

        # 4. 合并显式传入的选项（优先级最高）
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
        """解析计算日期范围

        Returns:
            (calc_start, calc_end, data_start)
        """
        today = datetime.now().strftime("%Y%m%d")

        if compute_mode == "full":
            calc_start = start_date or "20100101"
            calc_end = end_date or today
            lookback = definition.params.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
            data_start = self.trading_cal.offset_trading_days(calc_start, -lookback) if start_date else calc_start
            return calc_start, calc_end, data_start

        # 增量模式
        if target_date:
            calc_start = target_date
            calc_end = end_date or target_date
        elif start_date:
            calc_start = start_date
            calc_end = end_date or today
        else:
            last_date = self._get_last_computed_date(factor_id)
            if last_date:
                calc_start = DateUtils.add_days(last_date, 1)
            else:
                calc_start = today
            calc_end = today

        if calc_start > calc_end:
            return None, None, None

        lookback = definition.params.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
        data_start = self.trading_cal.offset_trading_days(calc_start, -lookback)

        return calc_start, calc_end, data_start

    def _get_factor_preprocess(self, factor_id: str) -> Dict[str, Any]:
        """从数据库获取因子的预处理配置"""
        try:
            result = self.db.query(
                f"SELECT params FROM factor_metadata WHERE factor_id = '{factor_id}'"
            )
            if not result.is_empty():
                params = result["params"][0]
                if isinstance(params, dict):
                    return params.get("preprocess", {})
        except Exception as e:
            logger.debug(f"Failed to get factor preprocess from DB: {e}")
        return {}

    def _get_last_computed_date(self, factor_id: str) -> Optional[str]:
        """获取因子最后计算日期"""
        try:
            result = self.db.query(
                f"SELECT max(trade_date) as last_date FROM factor_values "
                f"WHERE factor_id = '{factor_id}'"
            )
            if not result.is_empty() and result["last_date"][0]:
                return str(result["last_date"][0])
        except Exception as e:
            logger.debug(f"Failed to get last computed date: {e}")
        return None

    def _create_run_record(
        self, factor_id: str, calc_start: str, calc_end: str, mode: str
    ) -> str:
        """创建运行记录"""
        run_id = f"{factor_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        try:
            record = pl.DataFrame({
                "run_id": [run_id],
                "factor_id": [factor_id],
                "start_date": [calc_start],
                "end_date": [calc_end],
                "mode": [mode],
                "status": ["running"],
                "created_at": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            })
            self.db.append("factor_run_log", record)
        except Exception as e:
            logger.warning(f"Failed to create run record: {e}")
        return run_id

    def _finish_run_record(
        self,
        run_id: str,
        status: str,
        rows: int,
        started_at: datetime,
        message: Optional[str] = None
    ):
        """完成运行记录"""
        try:
            elapsed = (datetime.now() - started_at).total_seconds()
            update_df = pl.DataFrame({
                "run_id": [run_id],
                "status": [status],
                "rows": [rows],
                "elapsed_seconds": [elapsed],
                "message": [message or ""],
                "finished_at": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            })
            self.db.upsert("factor_run_log", update_df)
        except Exception as e:
            logger.warning(f"Failed to finish run record: {e}")

    def _update_metadata(
        self, factor_id: str, definition: FactorDefinition, last_date: str, rows: int
    ):
        """更新因子元数据"""
        try:
            update_df = pl.DataFrame({
                "factor_id": [factor_id],
                "last_computed_date": [last_date],
                "total_rows": [rows],
                "updated_at": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            })
            self.db.upsert("factor_metadata", update_df)
        except Exception as e:
            logger.warning(f"Failed to update metadata: {e}")
