"""
因子生产引擎
负责数据加载、因子计算调度、结果存储
"""
import polars as pl
from app.core.utils import DateUtils
from typing import Optional, Dict, Any, List
from datetime import datetime

from app.core.logger import logger
from app.core.utils import TradingCalendar
from data_manager.processor import DataProcessor
from engine.production.registry import (
    FactorDefinition, StorageConfig, get_factor, get_registry, list_factors, discover_factors
)
from engine.production.data_config import DataConfigLoader


# 数据表到查询列的映射
TABLE_COLUMNS = {
    "sync_daily_data": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"],
    "sync_daily_basic": ["ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"],
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
        self.data_config = DataConfigLoader(db_client)
        # 确保 data_config 中引用的表都已注册到 _ALL_TABLES（用于 SQL 语法适配）
        self._register_config_tables()

    # 默认预处理选项
    DEFAULT_PREPROCESS = {
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True,
        "new_stock_days": 60,
        "handle_suspension": True,
        "mark_limit": True,
    }

    def _register_config_tables(self):
        """将 data_config 中引用的表名注册到 db._ALL_TABLES，确保 SQL 语法适配能识别"""
        try:
            config = self.data_config.load()
            for cfg in config.values():
                table_name = cfg.get("table_name", "")
                if table_name and table_name not in self.db._ALL_TABLES:
                    self.db.register_meta_table(table_name)
                # 也注册 extra_config 中引用的表（如 price_table）
                extra = cfg.get("extra_config", {})
                if isinstance(extra, dict):
                    for key in ("price_table",):
                        ref_table = extra.get(key, "")
                        if ref_table and ref_table not in self.db._ALL_TABLES:
                            self.db.register_meta_table(ref_table)
        except Exception as e:
            logger.debug(f"注册 data_config 表名失败: {e}")

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
        discover_factors(db_client=self.db)  # 确保因子已注册（优先从数据库加载）
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
        run_id = self._insert_run_record(factor_id, compute_mode, start_date, end_date, opts)

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

            # 2.5 应用股票状态（ST、涨跌停、停牌）- 统一从 stock_daily_status 表 join
            if opts["filter_st"] or opts["filter_new_stock"] or opts["mark_limit"]:
                df = self._apply_stock_status(
                    df, data_start, calc_end,
                    filter_st=opts["filter_st"],
                    filter_new_stock=opts["filter_new_stock"],
                    new_stock_days=opts["new_stock_days"],
                    mark_limit=opts["mark_limit"],
                )

            # 3. 执行因子计算
            result = definition.func(df, definition.params)
            if result is None or result.is_empty():
                logger.warning(f"Factor {factor_id} returned empty result")
                self._finish_run_record(run_id, "success", 0, started_at, "empty result")
                return False

            # 3.5 停牌处理（基于 stock_daily_status.is_suspend）
            if opts["handle_suspension"] and "factor_value" in result.columns:
                window = definition.params.get("window", 20)
                result = self._handle_suspension_from_status(result, data_start, calc_end, window)

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
            rows = self._save_results(factor_id, result, definition.storage, run_id)

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
                calc_start = DateUtils.add_days(last_date, 1)
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
        # 不可变模式：使用列表推导式收集所有数据帧
        frames = [
            df for dep in definition.depends_on
            if (df := (
                self._load_factor_data(dep, start_date, end_date) if dep.startswith("factor_")
                else self._load_table_data(dep, start_date, end_date)
            )) is not None and not df.is_empty()
        ]

        if not frames:
            return None

        needs_adj = "sync_daily_data" in definition.depends_on and adjust_price != "none"

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
            cfg = self.data_config.get("adj_factor")
            adj_table = cfg["table_name"] or "sync_adj_factor"
            adj_column = cfg["column_name"] or "adj_factor"

            adj_df = self._load_table_data(adj_table, start_date, end_date)
            if adj_df is None or adj_df.is_empty():
                logger.warning("adj_factor 数据为空，跳过复权处理")
                return df

            # 合并复权因子
            df = df.join(adj_df, on=["ts_code", "trade_date"], how="left")

            if adj_column not in df.columns:
                return df

            # 计算基准复权因子
            if adjust_type == "forward":
                base_adj = (
                    df.sort(["ts_code", "trade_date"])
                    .group_by("ts_code")
                    .agg(pl.col(adj_column).last().alias("_base_adj"))
                )
            else:
                base_adj = (
                    df.sort(["ts_code", "trade_date"])
                    .group_by("ts_code")
                    .agg(pl.col(adj_column).first().alias("_base_adj"))
                )

            df = df.join(base_adj, on="ts_code", how="left")

            # 复权公式：adjusted_price = price * adj_factor / base_adj
            price_cols = [c for c in ["open", "high", "low", "close"] if c in df.columns]
            df = df.with_columns([
                (pl.col(c) * pl.col(adj_column) / pl.col("_base_adj")).alias(c)
                for c in price_cols
            ])

            # 清理临时列（只删除确实存在的列）
            cols_to_drop = [c for c in [adj_column, "_base_adj"] if c in df.columns]
            if cols_to_drop:
                df = df.drop(cols_to_drop)
            logger.debug(f"{'前' if adjust_type == 'forward' else '后'}复权处理完成")
            return df
        except Exception as e:
            logger.warning(f"复权处理失败 ({e})，使用未复权价格")
            return df

    def _apply_stock_status(self, df: pl.DataFrame,
                            start_date: str, end_date: str,
                            filter_st: bool = True,
                            filter_new_stock: bool = True,
                            new_stock_days: int = IPO_EXCLUDE_DAYS,
                            mark_limit: bool = True) -> pl.DataFrame:
        """根据 data_config 配置加载股票状态并应用过滤/标记。

        完全从 factor_data_config 表读取配置，支持用户配置的任意数据源。
        如果未配置，则跳过对应过滤。
        """
        try:
            # 获取每个字段的配置
            fields = ["is_st", "is_suspend", "is_limit"]

            # 不可变模式：使用字典推导式和列表推导式
            def get_field_config(fk: str) -> tuple[str, Optional[Dict[str, str]]]:
                """获取字段配置，返回 (field_key, config_dict or None)"""
                cfg = self.data_config.get(fk)
                tbl = cfg.get("table_name", "")
                col = cfg.get("column_name", "")
                if tbl and col:
                    return fk, {"table": tbl, "column": col}
                return fk, None

            # 使用推导式构建配置和缺失列表
            all_configs = [get_field_config(fk) for fk in fields]
            field_configs = {fk: cfg for fk, cfg in all_configs if cfg is not None}
            missing_configs = [fk for fk, cfg in all_configs if cfg is None]

            # 如果有字段未配置，提示用户（但继续处理其他已配置的字段）
            if missing_configs:
                logger.info(
                    f"股票状态字段 {missing_configs} 未在'因子-数据配置'中配置，"
                    f"对应过滤/标记功能将跳过。请在配置界面设置数据源（table_name + column_name）"
                )

            if not field_configs:
                logger.info("股票状态配置为空，跳过所有状态过滤")
                # 仍然需要处理新股过滤（不依赖 stock_daily_status 表）
                if filter_new_stock:
                    df = self._filter_new_stock(df, start_date, new_stock_days)
                return df

            # 收集所有需要的表及其字段（不可变模式：使用字典推导式）
            # 先按表分组字段
            from collections import defaultdict
            table_cols_temp = defaultdict(set)
            for fk, cfg in field_configs.items():
                table_cols_temp[cfg["table"]].add(cfg["column"])

            # 转换为不可变的字典（列表去重）
            tables: Dict[str, List[str]] = {
                tbl: list(cols) for tbl, cols in table_cols_temp.items()
            }

            # 为每张表加载数据并合并（不可变模式：使用列表推导式）
            def load_status_table(tbl: str, cols: List[str]) -> Optional[pl.DataFrame]:
                """加载单个状态表数据"""
                if not self.db.table_exists(tbl):
                    logger.warning(f"配置的数据表 {tbl} 不存在，跳过状态过滤")
                    return None

                select_cols = ["ts_code", "trade_date"] + cols
                col_str = ", ".join(select_cols)
                ddb_start = f"{start_date[:4]}.{start_date[4:6]}.{start_date[6:]}"
                ddb_end = f"{end_date[:4]}.{end_date[4:6]}.{end_date[6:]}"
                status_df = self.db.query(
                    f"SELECT {col_str} FROM {tbl}"
                    f" WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
                )
                if status_df.is_empty():
                    return None

                # 重命名列：column_name -> field_key (is_st, is_suspend, is_limit)
                rename_map = {
                    cfg["column"]: fk
                    for fk, cfg in field_configs.items()
                    if cfg["table"] == tbl
                }
                return status_df.rename(rename_map) if rename_map else status_df

            # 使用列表推导式收集所有状态数据帧
            status_dfs = [
                sdf for tbl, cols in tables.items()
                if (sdf := load_status_table(tbl, cols)) is not None
            ]

            if not status_dfs:
                logger.warning("股票状态数据为空，跳过过滤")
                return df

            # 合并多个状态表
            status_df = status_dfs[0]
            for sdf in status_dfs[1:]:
                # 只取右表不重复的列
                right_cols = [c for c in sdf.columns if c not in status_df.columns or c in ["ts_code", "trade_date"]]
                status_df = status_df.join(sdf.select(right_cols), on=["ts_code", "trade_date"], how="left")

            # Join 到主数据
            before = len(df)
            df = df.join(status_df, on=["ts_code", "trade_date"], how="left")

            # 填充缺失值（没有状态记录的默认为正常）
            df = df.with_columns([
                pl.col(fk).fill_null(0) for fk in fields if fk in df.columns
            ])

            # 2. 过滤 ST（仅当 is_st 配置存在时）
            if filter_st and "is_st" in df.columns:
                before_st = len(df)
                df = df.filter(pl.col("is_st") == 0)
                st_dropped = before_st - len(df)
                if st_dropped > 0:
                    logger.info(f"ST 过滤: 移除 {st_dropped} 行")

            # 3. 标记涨跌停（仅当 is_limit 配置存在时）
            if mark_limit and "is_limit" in df.columns:
                df = df.with_columns(pl.col("is_limit").alias("_limit_up_down"))
                marked = df.filter(pl.col("_limit_up_down") != 0).height
                if marked > 0:
                    logger.info(f"涨跌停标记: {marked} 行")

            # 清理临时列（保留 is_suspend 用于后续停牌处理）
            df = df.drop([c for c in ["is_st", "is_limit"] if c in df.columns])

            total_dropped = before - len(df)
            if total_dropped > 0:
                logger.info(f"股票状态过滤: 总计移除 {total_dropped} 行")

            # 4. 过滤新股（仍需单独处理，因为需要交易日历计算）
            if filter_new_stock:
                df = self._filter_new_stock(df, start_date, new_stock_days)

            return df
        except Exception as e:
            logger.warning(f"应用股票状态失败 ({e})，跳过")
            return df

    def _filter_new_stock(self, df: pl.DataFrame, data_start: str, new_stock_days: int) -> pl.DataFrame:
        """过滤新股（上市未满 N 个交易日）"""
        try:
            ld_cfg = self.data_config.get("list_date")
            ld_table = ld_cfg["table_name"] or "sync_stock_basic"
            ld_column = ld_cfg["column_name"] or "list_date"

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
                        cutoff_map[row["ts_code"]] = self.trading_cal.offset_trading_days(ld, new_stock_days)
                if cutoff_map:
                    cutoff_df = pl.DataFrame({
                        "ts_code": list(cutoff_map.keys()),
                        "_ipo_cutoff": list(cutoff_map.values()),
                    })
                    df = df.join(cutoff_df, on="ts_code", how="left")
                    df = df.filter(
                        pl.col("_ipo_cutoff").is_null() | (pl.col("trade_date") >= pl.col("_ipo_cutoff"))
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

    def _handle_suspension_from_status(self, result: pl.DataFrame,
                                       start_date: str, end_date: str,
                                       window: int) -> pl.DataFrame:
        """基于 data_config 配置的 is_suspend 字段处理停牌。

        将 is_suspend > 0 的行及其后 window 行的 factor_value 置空。
        完全依赖 factor_data_config 配置，支持用户配置的任意数据源。
        """
        try:
            status_cfg = self.data_config.get("is_suspend")
            status_table = status_cfg.get("table_name", "")
            status_column = status_cfg.get("column_name", "is_suspend")

            if status_table and self.db.table_exists(status_table):
                # 将日期转为 DolphinDB DATE 字面量格式 (YYYY.MM.DD)，不加引号
                ddb_start = f"{start_date[:4]}.{start_date[4:6]}.{start_date[6:]}"
                ddb_end = f"{end_date[:4]}.{end_date[4:6]}.{end_date[6:]}"
                status_df = self.db.query(
                    f"SELECT ts_code, trade_date, {status_column} FROM {status_table}"
                    f" WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
                )

                if not status_df.is_empty():
                    # 重命名配置的列名为 is_suspend（后续逻辑统一使用）
                    if status_column != "is_suspend":
                        status_df = status_df.rename({status_column: "is_suspend"})

                    result = result.sort(["ts_code", "trade_date"])
                    result = result.join(status_df, on=["ts_code", "trade_date"], how="left")
                    result = result.with_columns(pl.col("is_suspend").fill_null(0))

                    # 向前扩散：停牌日及其后 window-1 行置空
                    # rolling_sum 是向后看的，所以先 reverse，rolling_sum，再 reverse 回来
                    result = result.with_columns(
                        pl.col("is_suspend")
                        .reverse()
                        .rolling_sum(window_size=window, min_periods=1)
                        .reverse()
                        .over("ts_code")
                        .alias("_near_susp")
                    )

                    nullified = result.filter(pl.col("_near_susp") > 0).height
                    if nullified > 0:
                        logger.info(f"停牌处理: {nullified} 行 factor_value 被置空")

                    result = result.with_columns(
                        pl.when(pl.col("_near_susp") > 0)
                        .then(pl.lit(None, dtype=pl.Float64))
                        .otherwise(pl.col("factor_value"))
                        .alias("factor_value")
                    )
                    result = result.drop(["is_suspend", "_near_susp"])
                    result = result.drop_nulls(subset=["factor_value"])
                    return result

            # 无配置数据源，回退到交易日 gap 推断
            logger.info("is_suspend 未配置，回退到交易日 gap 推断停牌")
            if self.trading_cal.is_loaded:
                trading_days = self.trading_cal.get_trading_days(start_date, end_date)
                result = result.sort(["ts_code", "trade_date"])
                result = DataProcessor.mark_suspension_gaps(result, trading_days)
                result = DataProcessor.nullify_post_suspension(result, window)
                result = result.drop_nulls(subset=["factor_value"])

            return result
        except Exception as e:
            logger.warning(f"停牌处理失败 ({e})，跳过")
            return result

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
                      storage: StorageConfig, run_id: str = "") -> int:
        """保存因子计算结果"""
        if storage.target == "factor_values":
            return self._save_to_unified_table(factor_id, df, run_id)
        else:
            return self._save_to_custom_table(df, storage)

    def _save_to_unified_table(self, factor_id: str, df: pl.DataFrame, run_id: str = "", task_version: int = 1) -> int:
        """保存到统一因子表"""
        from datetime import datetime

        # 获取当前因子版本
        current_version = self.db.get_current_task_version("factor", factor_id)
        if current_version:
            task_version = current_version.get("version_number", 1)

        # 构造写入数据：ts_code, trade_date, factor_id, factor_value, quality_flag, task_version, run_id, data_version, created_at
        # 不可变模式：使用列表推导式和条件表达式构建列列表
        base_cols = [
            pl.col("ts_code"),
            pl.col("trade_date"),
            pl.lit(factor_id).alias("factor_id"),
            pl.col("factor_value").cast(pl.Float64),
        ]

        quality_col = (
            pl.col("quality_flag").cast(pl.Int32)
            if "quality_flag" in df.columns
            else pl.lit(0).alias("quality_flag")
        )

        version_cols = [
            pl.lit(task_version).alias("task_version"),
            pl.lit(run_id).alias("run_id"),
            pl.lit(f"v{task_version}").alias("data_version"),
            pl.lit(datetime.now()).alias("created_at")
        ]

        # 使用不可变列表拼接（返回新列表）
        select_cols = [*base_cols, quality_col, *version_cols]

        write_df = df.select(select_cols)

        # 先删除旧数据再插入，确保被剔除的股票（如 ST 变更）不会留有残余值
        trade_dates = write_df["trade_date"].unique().to_list()
        if trade_dates:
            self._delete_factor_dates(factor_id, trade_dates)

        self.db.upsert("factor_values", write_df,
                       ["ts_code", "trade_date", "factor_id"])
        return len(write_df)

    def _delete_factor_dates(self, factor_id: str, trade_dates: list) -> None:
        """删除 factor_values 中指定因子在指定日期的所有旧数据"""
        try:
            db_path = self.db._db_path
            ddb_dates = [self.db._escape_value(d) for d in trade_dates]
            dates_vec = "[" + ", ".join(ddb_dates) + "]"
            self.db.execute(
                f'pt = loadTable("{db_path}", "factor_values");'
                f'delete from pt where factor_id = "{factor_id}" and trade_date in {dates_vec}'
            )
            logger.debug(f"已删除 {factor_id} 在 {len(trade_dates)} 个日期的旧数据")
        except Exception as e:
            logger.warning(f"删除 {factor_id} 旧数据失败: {e}")

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
        """更新因子元数据（保留 code/depends_on/preprocess 配置）"""
        import json
        try:
            # 先读取现有记录，保留 code / depends_on / created_at
            existing = {}
            try:
                df_existing = self.db.query(
                    "SELECT * FROM factor_metadata WHERE factor_id = %s", (factor_id,)
                )
                if not df_existing.is_empty():
                    existing = df_existing.to_dicts()[0]
            except Exception as e:
                logger.debug(f"查询现有因子元数据失败: {e}")

            # 合并 params：保留 DB 中用户设置的 preprocess，其余用代码定义覆盖
            db_pp = self._get_factor_preprocess(factor_id)
            merged_params = dict(definition.params) if definition.params else {}
            if db_pp:
                merged_params["preprocess"] = db_pp

            now = datetime.now()
            row = {
                "factor_id": factor_id,
                "description": definition.description or "",
                "category": definition.category or "custom",
                "compute_mode": definition.compute_mode or "incremental",
                "storage_target": definition.storage.target or "factor_values",
                "depends_on": existing.get("depends_on") or json.dumps(definition.depends_on or []),
                "params": json.dumps(merged_params),
                "code": existing.get("code") or "",
                "last_computed_date": last_date,
                "last_computed_at": now,
                "created_at": existing.get("created_at") or now,
                "updated_at": now,
            }

            update_df = pl.DataFrame([row], schema={
                "factor_id": pl.Utf8,
                "description": pl.Utf8,
                "category": pl.Utf8,
                "compute_mode": pl.Utf8,
                "storage_target": pl.Utf8,
                "depends_on": pl.Utf8,
                "params": pl.Utf8,
                "code": pl.Utf8,
                "last_computed_date": pl.Utf8,
                "last_computed_at": pl.Datetime("ns"),
                "created_at": pl.Datetime("ns"),
                "updated_at": pl.Datetime("ns"),
            })
            self.db.upsert("factor_metadata", update_df, ["factor_id"])
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
        except Exception as e:
            logger.debug(f"获取因子最后计算日期失败: {e}")
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
        except Exception as e:
            logger.debug(f"读取因子预处理配置失败: {e}")
        return {}

    # ==================== 工具方法 ====================

    # ==================== 运行记录 ====================

    def _insert_run_record(self, factor_id: str, mode: str,
                           start_date: Optional[str], end_date: Optional[str],
                           opts: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """插入运行记录，返回 run_id (时间戳字符串)"""
        import json
        try:
            now = datetime.now()
            run_id = now.strftime("%Y%m%d%H%M%S%f")

            # 格式化 timestamp 为 DolphinDB TIMESTAMP 格式（含毫秒）
            now_ts = now.strftime("%Y.%m.%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}"
            opts = opts or {}
            opts_str = json.dumps(opts).replace('"', '\\"')  # 转义双引号

            # 提取预处理参数
            filter_st = "true" if opts.get("filter_st", True) else "false"
            filter_new_stock = "true" if opts.get("filter_new_stock", True) else "false"
            new_stock_days = opts.get("new_stock_days", 60)
            handle_suspension = "true" if opts.get("handle_suspension", True) else "false"
            mark_limit = "true" if opts.get("mark_limit", True) else "false"
            adjust_price = opts.get("adjust_price", "none")

            # 列顺序必须与 factor_task_run 表定义一致：
            # factor_id, mode, status, start_date, end_date, rows_affected,
            # duration_seconds, filter_st, filter_new_stock, new_stock_days,
            # handle_suspension, mark_limit, adjust_price, preprocess, run_id, error_message, created_at
            meta_db = self.db._db_path
            with self.db._lock:
                self.db._ensure_connected()
                self.db._session.run(
                    f'ts_val = [temporalParse("{now_ts}", "yyyy.MM.ddTHH:mm:ss.SSS")];'
                    f'tmpRun = table('
                    f'["{factor_id}"] as factor_id, '
                    f'["{mode or ""}"] as mode, '
                    f'["running"] as status, '
                    f'["{start_date or ""}"] as start_date, '
                    f'["{end_date or ""}"] as end_date, '
                    f'[0] as rows_affected, '
                    f'[0.0] as duration_seconds, '
                    f'[{filter_st}] as filter_st, '
                    f'[{filter_new_stock}] as filter_new_stock, '
                    f'[{new_stock_days}] as new_stock_days, '
                    f'[{handle_suspension}] as handle_suspension, '
                    f'[{mark_limit}] as mark_limit, '
                    f'["{adjust_price}"] as adjust_price, '
                    f'["{opts_str}"] as preprocess, '
                    f'["{run_id}"] as run_id, '
                    f'[""] as error_message, '
                    f'ts_val as created_at'
                    f');'
                    f'ptr = loadTable("{meta_db}", "factor_task_run");'
                    f'ptr.append!(tmpRun);'
                )
            return run_id
        except Exception as e:
            import traceback
            logger.error(f"Failed to insert run record for {factor_id}: {e}\n{traceback.format_exc()}")
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
            if error_msg:
                err = (error_msg
                       .replace("\\", "\\\\")
                       .replace('"', '\\"')
                       .replace(";", "")
                       .replace("`", "")
                       [:500])
            else:
                err = ""
            self.db.execute(
                f'ptr = loadTable("{meta_db}", "factor_task_run");'
                f'update ptr set status = "{status}", rows_affected = {rows}, '
                f'duration_seconds = {elapsed}, error_message = "{err}" '
                f'where run_id = "{run_id}"'
            )
        except Exception as e:
            import traceback
            logger.error(f"Failed to update run record {run_id}: {e}\n{traceback.format_exc()}")
