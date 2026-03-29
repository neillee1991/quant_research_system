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
# Removed: preprocess_config.yaml is no longer used
from infrastructure.processor.pipeline import ProcessContext
from infrastructure.processor.pipeline_factory import PipelineFactory


# 增量计算时，需要额外加载的历史窗口天数（用于滚动计算）
DEFAULT_LOOKBACK_DAYS = 60

# 默认预处理选项（从 ProductionEngine 迁移）
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
        """初始化服务

        Args:
            db_client: DolphinDB 客户端
        """
        self.db = db_client
        self.trading_cal = TradingCalendar.get_instance(db_client)
        self.data_config = DataConfigLoader(db_client)
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
        run_id: Optional[str] = None,
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

            # 4. 创建运行记录（如果外部已传入 run_id，更新已有记录；否则新建）
            if run_id:
                try:
                    self.db.execute("DELETE FROM factor_run_log WHERE run_id = %s", (run_id,))
                    record = pl.DataFrame({
                        "run_id": [run_id],
                        "factor_id": [factor_id],
                        "start_date": [calc_start],
                        "end_date": [calc_end],
                        "mode": [compute_mode],
                        "status": ["running"],
                        "rows": [0],
                        "elapsed_seconds": [0.0],
                        "message": [""],
                        "created_at": [started_at],
                    })
                    self.db.append("factor_run_log", record)
                except Exception as e:
                    logger.warning(f"Failed to update run record {run_id}: {e}")
            else:
                run_id = self._create_run_record(factor_id, calc_start, calc_end, compute_mode)

            # 保存运行上下文用于后续更新
            run_context = {
                "factor_id": factor_id,
                "calc_start": calc_start,
                "calc_end": calc_end,
                "compute_mode": compute_mode
            }

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
            self._finish_run_record(run_id, "success", rows, started_at, run_context)

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
            if 'run_id' in locals() and 'run_context' in locals():
                self._finish_run_record(run_id, "failed", 0, started_at, run_context, str(e))

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
        """解析预处理选项（简化版）

        优先级：显式传入 > DB配置 > 代码定义 > 系统默认

        Args:
            factor_id: 因子ID
            definition: 因子定义
            explicit_options: 显式传入的选项
            profile_name: 配置名称（已废弃，保留参数兼容性）

        Returns:
            合并后的预处理选项
        """
        # 1. 系统默认配置
        base_options = {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
            "new_stock_days": 60,
            "mark_limit": True,
        }

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
                "SELECT params FROM factor_metadata WHERE factor_id = %s",
                (factor_id,)
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
                "SELECT max(trade_date) as last_date FROM factor_values "
                "WHERE factor_id = %s",
                (factor_id,)
            )
            if not result.is_empty() and result["last_date"][0]:
                last_date = str(result["last_date"][0])
                # 转换日期格式：2026-03-13 00:00:00 -> 20260313
                if " " in last_date:
                    last_date = last_date.split()[0].replace("-", "")
                return last_date
        except Exception as e:
            logger.debug(f"Failed to get last computed date: {e}")
        return None

    def _create_run_record(
        self, factor_id: str, calc_start: str, calc_end: str, mode: str
    ) -> str:
        """创建运行记录"""
        run_id = f"{factor_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        try:
            now = datetime.now()
            # 只包含迁移脚本中定义的基本字段
            record = pl.DataFrame({
                "run_id": [run_id],
                "factor_id": [factor_id],
                "start_date": [calc_start],
                "end_date": [calc_end],
                "mode": [mode],
                "status": ["running"],
                "rows": [0],
                "elapsed_seconds": [0.0],
                "message": [""],
                "created_at": [now],
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
        run_context: Dict[str, Any],
        message: Optional[str] = None
    ):
        """完成运行记录"""
        try:
            elapsed = (datetime.now() - started_at).total_seconds()
            # 先删除旧记录
            self.db.execute(
                "DELETE FROM factor_run_log WHERE run_id = %s", (run_id,)
            )

            # 插入完整记录
            now = datetime.now()
            record = pl.DataFrame({
                "factor_id": [run_context["factor_id"]],
                "mode": [run_context["compute_mode"]],
                "status": [status],
                "start_date": [run_context["calc_start"]],
                "end_date": [run_context["calc_end"]],
                "rows": [rows],
                "elapsed_seconds": [elapsed],
                "message": [message or ""],
                "run_id": [run_id],
                "created_at": [started_at]
            })
            self.db.append("factor_run_log", record)
            logger.info(f"Finished run record: {run_id}, status={status}, rows={rows}, elapsed={elapsed:.1f}s")
        except Exception as e:
            logger.warning(f"Failed to finish run record: {e}", exc_info=True)

    def _update_metadata(
        self, factor_id: str, definition: FactorDefinition, last_date: str, rows: int
    ):
        """更新因子元数据的 updated_at 时间戳"""
        try:
            # 读出完整行，只改 updated_at，避免 DolphinDB 按列位置插入时错位
            existing = self.db.query(
                "SELECT * FROM factor_metadata WHERE factor_id = %s", (factor_id,)
            )
            if existing.is_empty():
                return
            row = existing.to_dicts()[0]
            update_df = pl.DataFrame({
                "factor_id": [row["factor_id"]],
                "description": [row.get("description", "")],
                "category": [row.get("category", "custom")],
                "compute_mode": [row.get("compute_mode", "incremental")],
                "storage_target": [row.get("storage_target", "factor_values")],
                "depends_on": [row.get("depends_on", "[]")],
                "params": [row.get("params", "{}")],
                "code": [row.get("code", "")],
                "enabled": [row.get("enabled", True)],
                "created_at": [row.get("created_at", datetime.now())],
                "updated_at": [datetime.now()],
            })
            self.db.upsert("factor_metadata", update_df, key_columns=["factor_id"])
        except Exception as e:
            logger.warning(f"Failed to update metadata: {e}")

    # ==================== 迁移的独有功能 ====================

    def _save_to_custom_table(
        self,
        df: pl.DataFrame,
        table_name: str,
        primary_keys: list,
        compute_mode: str = "incremental",
        factor_id: Optional[str] = None
    ) -> int:
        """保存结果到自定义表 (迁移自 ProductionEngine)

        Args:
            df: 要保存的数据
            table_name: 目标表名
            primary_keys: 主键列表
            compute_mode: 计算模式 ("full" 或 "incremental")
            factor_id: 因子ID（用于精确删除）

        Returns:
            写入的行数
        """
        try:
            # 自动建表
            if not self.db.table_exists(table_name):
                # 根据 DataFrame schema 自动推断列类型
                schema = {}
                for col in df.columns:
                    dtype = df[col].dtype
                    if dtype == pl.Utf8:
                        col_type = "STRING"
                    elif dtype == pl.Int64:
                        col_type = "LONG"
                    elif dtype == pl.Float64:
                        col_type = "DOUBLE"
                    elif dtype == pl.Date:
                        col_type = "DATE"
                    elif dtype == pl.Datetime:
                        col_type = "TIMESTAMP"
                    else:
                        col_type = "STRING"  # 默认

                    schema[col] = {
                        "type": col_type,
                        "nullable": col not in primary_keys
                    }

                self.db.create_table(table_name, schema, primary_keys)
                logger.info(f"Created custom factor table: {table_name}")

            # 保存数据
            if compute_mode == "full":
                # 全量模式：清空整个表
                self.db.upsert(table_name, df, primary_keys, is_full_sync=True)
            else:
                # 增量模式：按 trade_date 逐个清空并写入
                if "trade_date" in df.columns:
                    trade_dates = df["trade_date"].unique().to_list()
                    for trade_date in trade_dates:
                        date_df = df.filter(pl.col("trade_date") == trade_date)
                        self.db.upsert(
                            table_name, date_df, primary_keys,
                            is_full_sync=False, trade_date=trade_date, factor_id=factor_id
                        )
                else:
                    # 没有 trade_date 列，直接写入
                    self.db.upsert(table_name, df, primary_keys, is_full_sync=False)

            logger.info(f"Saved {len(df)} rows to custom table: {table_name}")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to save to custom table {table_name}: {e}", exc_info=True)
            raise

    def _filter_new_stock(
        self, df: pl.DataFrame, data_start: str, new_stock_days: int
    ) -> pl.DataFrame:
        """过滤新股（上市未满 N 个交易日）(迁移自 ProductionEngine)

        Args:
            df: 输入数据
            data_start: 数据起始日期
            new_stock_days: 新股排除天数

        Returns:
            过滤后的数据
        """
        try:
            ld_cfg = self.data_config.get("list_date")
            ld_table = ld_cfg.get("table_name") or "sync_stock_basic"
            ld_column = ld_cfg.get("column_name") or "list_date"

            stock_info = self.db.query(f'SELECT ts_code, {ld_column} FROM {ld_table}')
            if stock_info.is_empty():
                return df

            before = len(df)
            if self.trading_cal.is_loaded:
                # 用交易日历精确计算每只股票的 cutoff
                cutoff_map = {}
                for row in stock_info.to_dicts():
                    ld = row.get(ld_column)
                    if ld:
                        cutoff_map[row["ts_code"]] = self.trading_cal.offset_trading_days(
                            ld, new_stock_days
                        )
                if cutoff_map:
                    cutoff_df = pl.DataFrame({
                        "ts_code": list(cutoff_map.keys()),
                        "_ipo_cutoff": list(cutoff_map.values()),
                    })
                    df = df.join(cutoff_df, on="ts_code", how="left")
                    df = df.filter(
                        pl.col("_ipo_cutoff").is_null() |
                        (pl.col("trade_date") >= pl.col("_ipo_cutoff"))
                    )
                    df = df.drop("_ipo_cutoff")
            else:
                # 无交易日历：静态排除
                from datetime import timedelta
                dt = DateUtils.parse_date(data_start)
                ipo_cutoff = (dt - timedelta(days=int(new_stock_days * 1.5))).strftime("%Y%m%d")
                new_codes = stock_info.filter(
                    pl.col(ld_column).is_not_null() & (pl.col(ld_column) > ipo_cutoff)
                )["ts_code"].to_list()
                if new_codes:
                    df = df.filter(~pl.col("ts_code").is_in(new_codes))

            dropped = before - len(df)
            if dropped > 0:
                logger.info(f"新股过滤: 移除 {dropped} 行")
            return df

        except Exception as e:
            logger.warning(f"新股过滤失败 ({e})，跳过")
            return df

    def _get_factor_preprocess(self, factor_id: str) -> Dict[str, Any]:
        """从数据库加载因子的预处理配置 (迁移自 ProductionEngine)

        Args:
            factor_id: 因子ID

        Returns:
            预处理配置字典
        """
        try:
            result = self.db.query(
                "SELECT params FROM factor_metadata WHERE factor_id = %s",
                (factor_id,)
            )
            if result.is_empty():
                return {}

            params = result["params"][0]
            if isinstance(params, dict):
                return params.get("preprocess", {})
            elif isinstance(params, str):
                import json
                try:
                    params_dict = json.loads(params)
                    return params_dict.get("preprocess", {})
                except json.JSONDecodeError:
                    return {}
            return {}

        except Exception as e:
            logger.warning(f"Failed to load preprocess config for {factor_id}: {e}")
            return {}

    def _insert_run_record(
        self,
        factor_id: str,
        mode: str,
        start_date: Optional[str],
        end_date: Optional[str],
        opts: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """插入运行记录,包含所有预处理选项 (增强版,迁移自 ProductionEngine)

        Args:
            factor_id: 因子ID
            mode: 计算模式
            start_date: 开始日期
            end_date: 结束日期
            opts: 预处理选项

        Returns:
            run_id (时间戳字符串)
        """
        import json
        try:
            now = datetime.now()
            run_id = now.strftime("%Y%m%d%H%M%S%f")

            opts = opts or {}
            opts_str = json.dumps(opts)

            record = pl.DataFrame({
                "factor_id": [factor_id],
                "mode": [mode or ""],
                "status": ["running"],
                "start_date": [start_date or ""],
                "end_date": [end_date or ""],
                "rows_affected": [0],
                "duration_seconds": [0.0],
                "filter_st": [opts.get("filter_st", True)],
                "filter_new_stock": [opts.get("filter_new_stock", True)],
                "new_stock_days": [opts.get("new_stock_days", 60)],
                "mark_limit": [opts.get("mark_limit", True)],
                "adjust_price": [opts.get("adjust_price", "none")],
                "preprocess": [opts_str],
                "run_id": [run_id],
                "error_message": [""],
                "created_at": [now],
            })
            self.db.append("factor_task_run", record)
            logger.info(f"Inserted run record: {run_id} for factor {factor_id}")
            return run_id

        except Exception as e:
            import traceback
            logger.error(f"Failed to insert run record for {factor_id}: {e}\n{traceback.format_exc()}")
            return None
