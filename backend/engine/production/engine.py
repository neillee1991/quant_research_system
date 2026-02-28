"""
因子生产引擎
负责数据加载、因子计算调度、结果存储
"""
import pandas as pd
import polars as pl
from typing import Optional, Dict, Any, List
from datetime import datetime

from app.core.logger import logger
from app.core.utils import TradingCalendar
from data_manager.processor import DataProcessor
from engine.production.registry import (
    FactorDefinition, StorageConfig, get_factor, get_registry, list_factors, discover_factors
)


# 数据表到查询列的映射
TABLE_COLUMNS = {
    "sync_daily_data": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"],
    "sync_daily_basic": ["ts_code", "trade_date", "close", "turnover_rate", "volume_ratio", "pe", "pb"],
    "sync_adj_factor": ["ts_code", "trade_date", "adj_factor"],
    "sync_index_daily": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"],
}

# 增量计算时，需要额外加载的历史窗口天数（用于滚动计算）
DEFAULT_LOOKBACK_DAYS = 60

# 新股上市后需排除的最少交易日数
IPO_EXCLUDE_DAYS = 60


class ProductionEngine:
    """因子生产引擎"""

    def __init__(self, db_client):
        self.db = db_client
        self.trading_cal = TradingCalendar.get_instance(db_client)

    # 默认预处理选项
    DEFAULT_PREPROCESS = {
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True,
        "new_stock_days": 60,
        "handle_suspension": True,
        "mark_limit": True,
    }

    def run_task(
        self,
        factor_id: str,
        target_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: Optional[str] = None,
        preprocess: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """执行因子计算任务

        Args:
            factor_id: 因子ID
            target_date: 目标日期（增量模式下只算这一天）
            start_date: 开始日期（范围计算）
            end_date: 结束日期（范围计算）
            mode: 强制指定计算模式 ("incremental" / "full")，覆盖因子定义
            preprocess: 预处理选项，None 时从因子 params.preprocess 读取，仍无则使用默认值
        """
        discover_factors()  # 确保代码因子已注册
        definition = get_factor(factor_id)
        if not definition:
            logger.error(f"Factor not found: {factor_id}")
            return False

        # 优先级：显式传入 > DB factor_metadata.params.preprocess > 代码 params.preprocess > 全局默认
        factor_pp = definition.params.get("preprocess", {}) if definition.params else {}
        db_pp = self._get_factor_preprocess(factor_id)
        opts = {**self.DEFAULT_PREPROCESS, **factor_pp, **db_pp, **(preprocess or {})}

        compute_mode = mode or definition.compute_mode
        started_at = datetime.now()
        run_id = self._insert_run_record(factor_id, compute_mode, start_date, end_date)

        try:
            logger.info(f"Starting factor computation: {factor_id} (mode={compute_mode})")

            # 1. 确定日期范围
            calc_start, calc_end, data_start = self._resolve_dates(
                factor_id, compute_mode, target_date, start_date, end_date, definition
            )

            if calc_start is None:
                logger.info(f"Factor {factor_id} already up to date")
                self._finish_run_record(run_id, "success", 0, started_at)
                return True

            logger.info(f"Factor {factor_id}: computing {calc_start} ~ {calc_end}, loading data from {data_start}")

            # 2. 加载依赖数据（复权处理由 adjust_price 选项控制）
            df = self._load_data(definition, data_start, calc_end, adjust_price=opts["adjust_price"])
            if df is None or df.is_empty():
                logger.warning(f"No data loaded for factor {factor_id}")
                self._finish_run_record(run_id, "success", 0, started_at, "no data in date range")
                return False

            logger.info(f"Loaded {len(df)} rows for factor {factor_id}")

            # 2.5 过滤特殊股票（根据选项）
            if opts["filter_st"] or opts["filter_new_stock"]:
                df = self._filter_special_stocks(
                    df, data_start,
                    filter_st=opts["filter_st"],
                    filter_new_stock=opts["filter_new_stock"],
                    new_stock_days=opts["new_stock_days"],
                )

            # 2.6 标记一字涨跌停（根据选项）
            if opts["mark_limit"] and "sync_daily_data" in definition.depends_on and "open" in df.columns:
                df = DataProcessor.mark_limit_up_down(df)

            # 3. 执行因子计算
            result = definition.func(df, definition.params)
            if result is None or result.is_empty():
                logger.warning(f"Factor {factor_id} returned empty result")
                self._finish_run_record(run_id, "success", 0, started_at, "empty result")
                return False

            # 3.5 停牌复牌处理（根据选项）
            if opts["handle_suspension"] and "factor_value" in result.columns and self.trading_cal.is_loaded:
                window = definition.params.get("window", 20)
                trading_days = self.trading_cal.get_trading_days(data_start, calc_end)
                result = result.sort(["ts_code", "trade_date"])
                result = DataProcessor.mark_suspension_gaps(result, trading_days)
                result = DataProcessor.nullify_post_suspension(result, window)
                result = result.drop_nulls(subset=["factor_value"])

            # 4. 过滤到目标日期范围（增量模式下去掉 lookback 窗口的数据）
            if "trade_date" in result.columns:
                result = result.filter(
                    (pl.col("trade_date") >= calc_start) &
                    (pl.col("trade_date") <= calc_end)
                )

            # 4.5 生成因子质量标记
            result = self._build_quality_flag(result, df)

            logger.info(f"Factor {factor_id} computed {len(result)} rows")

            # 5. 存储结果
            rows = self._save_results(factor_id, result, definition.storage)

            # 6. 更新因子元数据
            self._update_metadata(factor_id, definition, calc_end, rows)

            elapsed = (datetime.now() - started_at).total_seconds()
            logger.info(f"Factor {factor_id} completed: {rows} rows in {elapsed:.1f}s")
            self._finish_run_record(run_id, "success", rows, started_at)
            return True

        except Exception as e:
            logger.error(f"Factor {factor_id} failed: {e}")
            self._finish_run_record(run_id, "failed", 0, started_at, str(e))
            return False

    # ==================== 日期解析 ====================

    def _resolve_dates(
        self, factor_id: str, compute_mode: str,
        target_date: Optional[str], start_date: Optional[str],
        end_date: Optional[str], definition: FactorDefinition
    ):
        """解析计算日期范围

        Returns:
            (calc_start, calc_end, data_start)
            - calc_start: 计算结果的起始日期
            - calc_end: 计算结果的结束日期
            - data_start: 数据加载的起始日期（含 lookback 窗口）
        """
        today = datetime.now().strftime("%Y%m%d")

        if compute_mode == "full":
            # 全量模式：加载所有数据
            calc_start = start_date or "20100101"
            calc_end = end_date or today
            # 全量模式也需要 lookback 窗口（用于滚动窗口因子的前 N 行计算）
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
            # 从上次计算日期的下一天开始
            last_date = self._get_last_computed_date(factor_id)
            if last_date:
                calc_start = self._add_days(last_date, 1)
            else:
                calc_start = today
            calc_end = today

        if calc_start > calc_end:
            return None, None, None

        # 加载额外的 lookback 窗口数据（用于滚动计算，按交易日偏移）
        lookback = definition.params.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
        data_start = self.trading_cal.offset_trading_days(calc_start, -lookback)

        return calc_start, calc_end, data_start

    # ==================== 数据加载 ====================

    def _load_data(self, definition: FactorDefinition,
                   start_date: str, end_date: str,
                   adjust_price: str = "forward") -> Optional[pl.DataFrame]:
        """根据 depends_on 加载数据

        Args:
            adjust_price: 复权方式 "none"=不复权, "forward"=前复权, "backward"=后复权
        """
        frames = []
        needs_adj = "sync_daily_data" in definition.depends_on and adjust_price != "none"

        for dep in definition.depends_on:
            if dep.startswith("factor_"):
                df = self._load_factor_data(dep, start_date, end_date)
            elif dep in TABLE_COLUMNS:
                df = self._load_table_data(dep, start_date, end_date)
            else:
                # 尝试作为普通表加载
                df = self._load_table_data(dep, start_date, end_date)

            if df is not None and not df.is_empty():
                frames.append(df)

        if not frames:
            return None

        # 合并多个数据源
        result = frames[0]
        for df in frames[1:]:
            join_cols = [c for c in ["ts_code", "trade_date"] if c in df.columns and c in result.columns]
            if join_cols:
                # 只取右表中不重复的列
                right_cols = [c for c in df.columns if c not in result.columns or c in join_cols]
                result = result.join(df.select(right_cols), on=join_cols, how="left")

        # 复权处理
        if needs_adj:
            result = self._apply_adjust(result, start_date, end_date, adjust_price)

        return result

    def _apply_adjust(self, df: pl.DataFrame,
                      start_date: str, end_date: str,
                      adjust_type: str = "forward") -> pl.DataFrame:
        """对 OHLC 价格做复权处理

        Args:
            adjust_type: "forward"=前复权, "backward"=后复权
        """
        try:
            adj_df = self._load_table_data("sync_adj_factor", start_date, end_date)
            if adj_df is None or adj_df.is_empty():
                logger.warning("adj_factor 数据为空，跳过复权处理")
                return df

            # 合并复权因子
            df = df.join(adj_df, on=["ts_code", "trade_date"], how="left")

            if "adj_factor" not in df.columns:
                return df

            # 计算基准复权因子
            if adjust_type == "forward":
                # 前复权：以最新日期的 adj_factor 为基准
                base_adj = (
                    df.sort(["ts_code", "trade_date"])
                    .group_by("ts_code")
                    .agg(pl.col("adj_factor").last().alias("_base_adj"))
                )
            else:
                # 后复权：以最早日期的 adj_factor 为基准
                base_adj = (
                    df.sort(["ts_code", "trade_date"])
                    .group_by("ts_code")
                    .agg(pl.col("adj_factor").first().alias("_base_adj"))
                )

            df = df.join(base_adj, on="ts_code", how="left")

            # 复权公式：adjusted_price = price * adj_factor / base_adj
            price_cols = [c for c in ["open", "high", "low", "close"] if c in df.columns]
            df = df.with_columns([
                (pl.col(c) * pl.col("adj_factor") / pl.col("_base_adj")).alias(c)
                for c in price_cols
            ])

            # 清理临时列
            df = df.drop(["adj_factor", "_base_adj"])
            logger.debug(f"{'前' if adjust_type == 'forward' else '后'}复权处理完成")
            return df
        except Exception as e:
            logger.warning(f"复权处理失败 ({e})，使用未复权价格")
            return df

    def _filter_special_stocks(self, df: pl.DataFrame, data_start: str,
                               filter_st: bool = True,
                               filter_new_stock: bool = True,
                               new_stock_days: int = IPO_EXCLUDE_DAYS) -> pl.DataFrame:
        """过滤特殊股票。

        Args:
            df: 含 ts_code, trade_date 的 DataFrame
            data_start: 数据加载起始日期，用于判断新股
            filter_st: 是否过滤 ST/*ST 股票
            filter_new_stock: 是否过滤新股
            new_stock_days: 新股排除天数
        """
        try:
            stock_info = self.db.query(
                f'SELECT ts_code, name, list_date FROM loadTable("{self.db._db_path}", "sync_stock_basic")'
            )
            if stock_info.is_empty():
                return df

            before = len(df)
            exclude_codes: set = set()
            st_count = 0
            new_count = 0

            # 1. 过滤 ST / *ST 股票
            if filter_st:
                st_codes = stock_info.filter(
                    pl.col("name").str.contains("ST")
                )["ts_code"].to_list()
                exclude_codes.update(st_codes)
                st_count = len(st_codes)

            # 2. 过滤新股
            if filter_new_stock:
                if self.trading_cal.is_loaded:
                    ipo_cutoff = self.trading_cal.offset_trading_days(data_start, -new_stock_days)
                else:
                    from datetime import timedelta
                    dt = datetime.strptime(data_start, "%Y%m%d")
                    ipo_cutoff = (dt - timedelta(days=int(new_stock_days * 1.5))).strftime("%Y%m%d")

                new_stock_codes = stock_info.filter(
                    pl.col("list_date").is_not_null() & (pl.col("list_date") > ipo_cutoff)
                )["ts_code"].to_list()
                exclude_codes.update(new_stock_codes)
                new_count = len(new_stock_codes)

            if exclude_codes:
                df = df.filter(~pl.col("ts_code").is_in(list(exclude_codes)))
                dropped = before - len(df)
                if dropped > 0:
                    logger.info(
                        f"特殊股票过滤: 排除 {len(exclude_codes)} 只股票 "
                        f"(ST: {st_count}, 新股: {new_count})，"
                        f"移除 {dropped} 行数据"
                    )

            return df
        except Exception as e:
            logger.warning(f"特殊股票过滤失败 ({e})，跳过过滤")
            return df

    def _load_table_data(self, table_name: str, start_date: str,
                         end_date: str) -> Optional[pl.DataFrame]:
        """从数据表加载数据"""
        try:
            columns = TABLE_COLUMNS.get(table_name, ["*"])
            col_str = ", ".join(columns) if columns != ["*"] else "*"

            # 检查表是否有 trade_date 列
            sql = f"SELECT {col_str} FROM {table_name} WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date"
            return self.db.query(sql, (start_date, end_date))
        except Exception as e:
            logger.error(f"Failed to load data from {table_name}: {e}")
            return None

    def _load_factor_data(self, factor_id: str, start_date: str,
                          end_date: str) -> Optional[pl.DataFrame]:
        """从 factor_values 表加载已计算的因子数据"""
        try:
            sql = """
                SELECT ts_code, trade_date, factor_value
                FROM factor_values
                WHERE factor_id = %s AND trade_date >= %s AND trade_date <= %s
                ORDER BY ts_code, trade_date
            """
            df = self.db.query(sql, (factor_id, start_date, end_date))
            if not df.is_empty():
                # 重命名 factor_value 为因子ID，方便合并
                df = df.rename({"factor_value": factor_id})
            return df
        except Exception as e:
            logger.error(f"Failed to load factor data {factor_id}: {e}")
            return None

    # ==================== 结果存储 ====================

    @staticmethod
    def _build_quality_flag(result: pl.DataFrame, source_df: pl.DataFrame) -> pl.DataFrame:
        """根据源数据中的标记列，为因子结果生成 quality_flag（位掩码）。"""
        from app.core.constants import QUALITY_NORMAL, QUALITY_LIMIT_UP, QUALITY_LIMIT_DOWN

        if "_limit_up_down" not in source_df.columns:
            # 无涨跌停标记，默认 quality_flag = 0
            return result.with_columns(pl.lit(QUALITY_NORMAL).alias("quality_flag"))

        # 从源数据提取涨跌停标记，join 到结果上
        limit_flags = source_df.select(["ts_code", "trade_date", "_limit_up_down"]).unique(
            subset=["ts_code", "trade_date"]
        )
        result = result.join(limit_flags, on=["ts_code", "trade_date"], how="left")

        result = result.with_columns(
            pl.when(pl.col("_limit_up_down") == 1)
            .then(pl.lit(QUALITY_LIMIT_UP))
            .when(pl.col("_limit_up_down") == -1)
            .then(pl.lit(QUALITY_LIMIT_DOWN))
            .otherwise(pl.lit(QUALITY_NORMAL))
            .alias("quality_flag")
        )

        if "_limit_up_down" in result.columns:
            result = result.drop("_limit_up_down")

        return result

    def _save_results(self, factor_id: str, df: pl.DataFrame,
                      storage: StorageConfig) -> int:
        """保存因子计算结果"""
        if storage.target == "factor_values":
            return self._save_to_unified_table(factor_id, df)
        else:
            return self._save_to_custom_table(df, storage)

    def _save_to_unified_table(self, factor_id: str, df: pl.DataFrame) -> int:
        """保存到统一因子表"""
        # DolphinDB 自动管理分区，无需手动确保

        # 构造写入数据：ts_code, trade_date, factor_id, factor_value, quality_flag
        select_cols = [
            pl.col("ts_code"),
            pl.col("trade_date"),
            pl.lit(factor_id).alias("factor_id"),
            pl.col("factor_value").cast(pl.Float64),
        ]
        if "quality_flag" in df.columns:
            select_cols.append(pl.col("quality_flag").cast(pl.Int32))

        write_df = df.select(select_cols)

        self.db.upsert("factor_values", write_df,
                       ["ts_code", "trade_date", "factor_id"])
        return len(write_df)

    def _save_to_custom_table(self, df: pl.DataFrame,
                              storage: StorageConfig) -> int:
        """保存到自定义表"""
        table_name = storage.target

        # 自动建表
        if not self.db.table_exists(table_name) and storage.columns:
            pk = storage.primary_keys or ["ts_code", "trade_date"]
            self.db.create_table(table_name, {
                col: {"type": col_type, "nullable": col not in pk}
                for col, col_type in storage.columns.items()
            }, pk)
            logger.info(f"Created custom factor table: {table_name}")

        pk = storage.primary_keys or ["ts_code", "trade_date"]
        self.db.upsert(table_name, df, pk)
        return len(df)

    # ==================== 元数据管理 ====================

    def _update_metadata(self, factor_id: str, definition: FactorDefinition,
                         last_date: str, rows: int):
        """更新因子元数据（保留用户设置的 preprocess 配置）"""
        import json
        try:
            # 合并 params：保留 DB 中用户设置的 preprocess，其余用代码定义覆盖
            db_pp = self._get_factor_preprocess(factor_id)
            merged_params = dict(definition.params) if definition.params else {}
            if db_pp:
                merged_params["preprocess"] = db_pp

            now = datetime.now()
            pdf = pd.DataFrame({
                "factor_id": [factor_id],
                "description": [definition.description or ""],
                "category": [definition.category or "custom"],
                "compute_mode": [definition.compute_mode or "incremental"],
                "storage_target": [definition.storage.target or "factor_values"],
                "params": [json.dumps(merged_params)],
                "last_computed_date": [last_date],
                "last_computed_at": [now],
                "created_at": [now],
                "updated_at": [now],
            })
            meta_db = self.db._db_path
            with self.db._lock:
                self.db._ensure_connected()
                # delete + insert 实现 upsert
                self.db._session.run(
                    f'fm = loadTable("{meta_db}", "factor_metadata");'
                    f'delete from fm where factor_id = "{factor_id}"'
                )
                tmp = f"_meta_upd_{factor_id}"
                self.db._session.upload({tmp: pdf})
                self.db._session.run(
                    f'fm = loadTable("{meta_db}", "factor_metadata");'
                    f'tableInsert(fm, {tmp});'
                    f"undef('{tmp}')"
                )
        except Exception as e:
            logger.warning(f"Failed to update factor metadata: {e}")

    def _get_last_computed_date(self, factor_id: str) -> Optional[str]:
        """获取因子最后计算日期"""
        try:
            df = self.db.query(
                "SELECT last_computed_date FROM factor_metadata WHERE factor_id = %s",
                (factor_id,)
            )
            if not df.is_empty() and df["last_computed_date"][0]:
                return df["last_computed_date"][0]
        except Exception:
            pass
        return None

    def _get_factor_preprocess(self, factor_id: str) -> dict:
        """从 factor_metadata 表读取因子的预处理配置"""
        import json
        try:
            df = self.db.query(
                "SELECT params FROM factor_metadata WHERE factor_id = %s",
                (factor_id,)
            )
            if not df.is_empty() and df["params"][0]:
                params = df["params"][0]
                if isinstance(params, str):
                    params = json.loads(params)
                return params.get("preprocess", {})
        except Exception:
            pass
        return {}

    # ==================== 工具方法 ====================

    @staticmethod
    def _add_days(date_str: str, days: int) -> str:
        """日期加减天数"""
        from datetime import timedelta
        dt = datetime.strptime(date_str, "%Y%m%d")
        result = dt + timedelta(days=days)
        return result.strftime("%Y%m%d")

    # ==================== 运行记录 ====================

    def _insert_run_record(self, factor_id: str, mode: str,
                           start_date: Optional[str], end_date: Optional[str]) -> Optional[str]:
        """插入运行记录，返回 run_id (时间戳字符串)"""
        try:
            now = datetime.now()
            run_id = now.strftime("%Y%m%d%H%M%S%f")
            pdf = pd.DataFrame({
                "factor_id": [factor_id],
                "mode": [mode or ""],
                "status": ["running"],
                "start_date": [start_date or ""],
                "end_date": [end_date or ""],
                "rows_affected": [0],
                "duration_seconds": [0.0],
                "error_message": [run_id],  # 借用 error_message 存 run_id 用于后续定位
                "created_at": [now],
            })
            meta_db = self.db._db_path
            with self.db._lock:
                self.db._ensure_connected()
                tmp = f"_run_{run_id}"
                self.db._session.upload({tmp: pdf})
                self.db._session.run(
                    f'ptr = loadTable("{meta_db}", "factor_task_run");'
                    f'tableInsert(ptr, {tmp});'
                    f"undef('{tmp}')"
                )
            return run_id
        except Exception as e:
            logger.debug(f"Failed to insert run record: {e}")
            return None

    def _finish_run_record(self, run_id: Optional[str], status: str,
                           rows: int, started_at: datetime,
                           error_msg: str = None):
        """更新运行记录的最终状态"""
        if run_id is None:
            return
        elapsed = (datetime.now() - started_at).total_seconds()
        try:
            meta_db = self.db._db_path
            err = (error_msg or "").replace('"', '\\"') if error_msg else ""
            self.db.execute(
                f'ptr = loadTable("{meta_db}", "factor_task_run");'
                f'update ptr set status = "{status}", rows_affected = {rows}, '
                f'duration_seconds = {elapsed}, error_message = "{err}" '
                f'where error_message = "{run_id}"'
            )
        except Exception as e:
            logger.debug(f"Failed to update run record: {e}")
