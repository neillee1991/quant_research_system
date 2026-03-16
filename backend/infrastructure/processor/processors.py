"""
具体处理器实现 - 将 ProductionEngine 的8步流程拆解为独立处理器
"""
import polars as pl
from typing import Optional, Dict, Any
from datetime import datetime

from app.core.logger import logger
from app.core.utils import DateUtils
from data_manager.processor import DataProcessor
from infrastructure.processor.pipeline import IProcessor, ProcessContext


class DataLoaderProcessor(IProcessor):
    """数据加载处理器 - 从 DolphinDB 加载依赖数据"""

    def __init__(self, db_client, data_config):
        self.db = db_client
        self.data_config = data_config

    @property
    def name(self) -> str:
        return "DataLoader"

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """加载因子依赖的数据表"""
        definition = context.factor_definition
        data_start = context.data_start
        calc_end = context.calc_end

        # 获取依赖的数据表
        depends_on = definition.depends_on or []
        if not depends_on:
            logger.warning(f"Factor {context.factor_id} has no depends_on, returning empty DataFrame")
            return pl.DataFrame()

        # 转换日期格式
        ddb_start = f"{data_start[:4]}.{data_start[4:6]}.{data_start[6:]}"
        ddb_end = f"{calc_end[:4]}.{calc_end[4:6]}.{calc_end[6:]}"

        # 加载数据配置（优先使用用户配置）
        config = self.data_config.load()

        # 加载并合并所有依赖表
        merged_df = None
        for dep in depends_on:
            # 检查是否是因子依赖（以 factor: 开头）
            if dep.startswith("factor:"):
                factor_id = dep[7:]  # 移除 "factor:" 前缀
                logger.debug(f"Loading factor dependency: {factor_id}")

                query = (
                    f"SELECT ts_code, trade_date, factor_value AS {factor_id} "
                    f"FROM factor_values "
                    f"WHERE factor_id = '{factor_id}' "
                    f"AND trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
                )
                dep_df = self.db.query(query)

                if dep_df.is_empty():
                    logger.warning(f"No data loaded for factor {factor_id}")
                    continue

                logger.info(f"Loaded {len(dep_df)} rows from factor {factor_id}")

            else:
                # 1. 优先使用用户配置（factor_data_config 表）
                dep_config = config.get(dep)

                if dep_config and dep_config.get("table_name"):
                    # 用户已配置此数据源
                    table_name = dep_config["table_name"]
                    column_name = dep_config.get("column_name", "")

                    if column_name:
                        # 单列配置
                        query = (
                            f"SELECT ts_code, trade_date, {column_name} FROM {table_name} "
                            f"WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
                        )
                    else:
                        # 多列配置（需要从 extra_config 解析）
                        query = (
                            f"SELECT * FROM {table_name} "
                            f"WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
                        )

                    logger.debug(f"Loading {dep} from user config: {table_name}")
                    dep_df = self.db.query(query)

                    if dep_df.is_empty():
                        logger.warning(f"No data loaded from {table_name} (user config)")
                        continue

                    logger.info(f"Loaded {len(dep_df)} rows from {dep} (user config)")

                else:
                    # 2. 尝试直接从表名加载（适用于 sync/etl 任务表）
                    try:
                        logger.debug(f"Trying to load {dep} directly as table name")
                        query = (
                            f"SELECT * FROM {dep} "
                            f"WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
                        )
                        dep_df = self.db.query(query)

                        if dep_df.is_empty():
                            logger.warning(f"No data loaded from table {dep}")
                            continue

                        logger.info(f"Loaded {len(dep_df)} rows from table {dep}")
                    except Exception as e:
                        # 数据源未配置且表不存在
                        raise ValueError(
                            f"Data source '{dep}' not found. "
                            f"Please ensure the table exists or add configuration in factor_data_config table. "
                            f"Error: {e}"
                        )

            # 合并数据
            if merged_df is None:
                merged_df = dep_df
            else:
                # 检查是否有重复的列名（除了 ts_code 和 trade_date）
                common_cols = set(merged_df.columns) & set(dep_df.columns) - {"ts_code", "trade_date"}
                if common_cols:
                    logger.warning(f"Duplicate columns found when merging {dep}: {common_cols}")
                    # 重命名重复列
                    for col in common_cols:
                        dep_df = dep_df.rename({col: f"{col}_{dep}"})

                # 使用 inner join 避免产生过多的 null 值
                # 如果需要 outer join，可以在因子代码中手动处理
                merged_df = merged_df.join(
                    dep_df,
                    on=["ts_code", "trade_date"],
                    how="inner"  # 改为 inner join，只保留所有表都有的数据
                )
                logger.debug(f"After joining {dep}: {len(merged_df)} rows")

        if merged_df is None or merged_df.is_empty():
            logger.warning(f"No data loaded from any dependency for factor {context.factor_id}")
            return pl.DataFrame()

        logger.info(f"Loaded total {len(merged_df)} rows for factor {context.factor_id}")

        # 保存原始数据到共享状态（用于后续质量检查）
        context.set_state("raw_data", merged_df)

        return merged_df


class AdjustmentProcessor(IProcessor):
    """复权处理器 - 应用前复权/后复权"""

    def __init__(self, db_client, data_config):
        self.db = db_client
        self.data_config = data_config

    @property
    def name(self) -> str:
        return "AdjustmentProcessor"

    def should_run(self, context: ProcessContext) -> bool:
        """只有配置了复权选项才执行"""
        adjust_mode = context.get_option("adjust_price", "none")
        return adjust_mode in ("forward", "backward")

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """应用复权因子"""
        adjust_mode = context.get_option("adjust_price")
        data_start = context.data_start
        calc_end = context.calc_end

        # 转换日期格式
        ddb_start = f"{data_start[:4]}.{data_start[4:6]}.{data_start[6:]}"
        ddb_end = f"{calc_end[:4]}.{calc_end[4:6]}.{calc_end[6:]}"

        # 1. 优先使用用户配置的复权因子表
        config = self.data_config.load()
        adj_config = config.get("adj_factor")

        if adj_config and adj_config.get("table_name"):
            # 用户配置了复权因子表
            table_name = adj_config["table_name"]
            column_name = adj_config.get("column_name", "adj_factor")
            logger.debug(f"Loading adj_factor from user config: {table_name}")
        else:
            # 2. 使用系统默认配置
            table_name = "sync_adj_factor"
            column_name = "adj_factor"
            logger.debug(f"Loading adj_factor from builtin config: {table_name}")

        # 加载复权因子
        adj_df = self.db.query(
            f"SELECT ts_code, trade_date, {column_name} as adj_factor FROM {table_name} "
            f"WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
        )

        if adj_df.is_empty():
            logger.warning(f"No adj_factor data from {table_name}, skipping adjustment")
            return df

        # 合并复权因子
        df = df.join(adj_df, on=["ts_code", "trade_date"], how="left")

        # 应用复权
        price_cols = ["open", "high", "low", "close"]
        existing_price_cols = [col for col in price_cols if col in df.columns]

        if not existing_price_cols:
            logger.warning("No price columns to adjust")
            return df.drop("adj_factor")

        if adjust_mode == "forward":
            # 前复权：价格 * 复权因子
            for col in existing_price_cols:
                df = df.with_columns(
                    (pl.col(col) * pl.col("adj_factor")).alias(col)
                )
        elif adjust_mode == "backward":
            # 后复权：价格 / 复权因子
            for col in existing_price_cols:
                df = df.with_columns(
                    (pl.col(col) / pl.col("adj_factor")).alias(col)
                )

        # 移除复权因子列
        df = df.drop("adj_factor")

        logger.info(f"Applied {adjust_mode} adjustment to {existing_price_cols} (from {table_name})")
        return df


class StatusFilterProcessor(IProcessor):
    """状态过滤处理器 - 过滤 ST、新股、标记涨跌停"""

    def __init__(self, db_client, data_config, trading_cal):
        self.db = db_client
        self.data_config = data_config
        self.trading_cal = trading_cal

    @property
    def name(self) -> str:
        return "StatusFilter"

    def should_run(self, context: ProcessContext) -> bool:
        """只要有任一状态过滤选项就执行"""
        return (
            context.get_option("filter_st", False) or
            context.get_option("filter_new_stock", False) or
            context.get_option("mark_limit", False)
        )

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """应用股票状态过滤"""
        data_start = context.data_start
        calc_end = context.calc_end
        filter_st = context.get_option("filter_st", False)
        filter_new_stock = context.get_option("filter_new_stock", False)
        new_stock_days = context.get_option("new_stock_days", 60)
        mark_limit = context.get_option("mark_limit", False)

        # 加载状态数据配置
        config = self.data_config.load()
        field_configs = {
            "is_st": config.get("is_st"),
            "is_limit": config.get("is_limit"),
        }

        # 按表分组加载
        tables = {}
        for field_key, cfg in field_configs.items():
            if not cfg:
                continue
            tbl = cfg.get("table_name", "")
            col = cfg.get("column_name", "")
            if not tbl or not col:
                logger.debug(f"Skipping {field_key}: missing table_name or column_name")
                continue
            if tbl not in tables:
                tables[tbl] = []
            tables[tbl].append((field_key, col))

        # 加载状态数据
        status_dfs = []
        for tbl, field_cols in tables.items():
            if not self.db.table_exists(tbl):
                logger.warning(f"Status table {tbl} not exists, skipping")
                continue

            cols = [col for _, col in field_cols]
            select_cols = ["ts_code", "trade_date"] + cols
            col_str = ", ".join(select_cols)
            ddb_start = f"{data_start[:4]}.{data_start[4:6]}.{data_start[6:]}"
            ddb_end = f"{calc_end[:4]}.{calc_end[4:6]}.{calc_end[6:]}"

            status_df = self.db.query(
                f"SELECT {col_str} FROM {tbl} "
                f"WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
            )

            if status_df.is_empty():
                continue

            # 重命名列：column_name -> field_key
            rename_map = {col: fk for fk, col in field_cols}
            if rename_map:
                status_df = status_df.rename(rename_map)

            status_dfs.append(status_df)

        if not status_dfs:
            logger.warning("No status data loaded, skipping filter")
            return df

        # 合并所有状态数据
        status_df = status_dfs[0]
        for sdf in status_dfs[1:]:
            status_df = status_df.join(sdf, on=["ts_code", "trade_date"], how="outer")

        # Join 到主数据
        df = df.join(status_df, on=["ts_code", "trade_date"], how="left")

        # 过滤 ST
        if filter_st and "is_st" in df.columns:
            before = len(df)
            df = df.filter(pl.col("is_st") != 1)
            logger.info(f"Filtered ST stocks: {before} -> {len(df)}")

        # 过滤新股
        if filter_new_stock:
            df = self._filter_new_stock(df, data_start, calc_end, new_stock_days)

        # 标记涨跌停（保留列用于后续处理）
        if mark_limit and "is_limit" not in df.columns:
            logger.warning("is_limit column not found, skipping mark_limit")

        return df

    def _filter_new_stock(self, df: pl.DataFrame, start_date: str, end_date: str, days: int) -> pl.DataFrame:
        """过滤新股（上市不足 N 天）"""
        try:
            # 查询股票基本信息
            basic_df = self.db.query("SELECT ts_code, list_date FROM sync_stock_basic")
            if basic_df.is_empty():
                logger.warning("sync_stock_basic is empty, skipping new stock filter")
                return df

            # 计算每个股票在日期范围内的上市天数
            df = df.join(basic_df, on="ts_code", how="left")

            # 转换日期为 YYYYMMDD 整数格式
            # trade_date 可能是 datetime/date/string 类型
            if df["trade_date"].dtype in [pl.Datetime, pl.Date]:
                df = df.with_columns(
                    pl.col("trade_date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("trade_date_int")
                )
            elif df["trade_date"].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col("trade_date").str.replace_all("-", "").cast(pl.Int32).alias("trade_date_int")
                )
            else:
                df = df.with_columns(pl.col("trade_date").cast(pl.Int32).alias("trade_date_int"))

            # list_date 同样处理
            if df["list_date"].dtype in [pl.Datetime, pl.Date]:
                df = df.with_columns(
                    pl.col("list_date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("list_date_int")
                )
            elif df["list_date"].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col("list_date").str.replace_all("-", "").cast(pl.Int32).alias("list_date_int")
                )
            else:
                df = df.with_columns(pl.col("list_date").cast(pl.Int32).alias("list_date_int"))

            # 简化：使用日期差值估算交易天数（实际应该用交易日历）
            # 假设一年约250个交易日
            before = len(df)
            df = df.with_columns(
                ((pl.col("trade_date_int") - pl.col("list_date_int")) / 10000 * 250).cast(pl.Int32).alias("days_since_ipo")
            )
            df = df.filter(pl.col("days_since_ipo") >= days)
            df = df.drop(["list_date", "trade_date_int", "list_date_int", "days_since_ipo"])

            logger.info(f"Filtered new stocks (<{days} days): {before} -> {len(df)}")
            return df
        except Exception as e:
            logger.warning(f"Failed to filter new stocks: {e}, skipping filter")
            return df


class FactorComputeProcessor(IProcessor):
    """因子计算处理器 - 执行因子计算函数"""

    @property
    def name(self) -> str:
        return "FactorCompute"

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """执行因子计算"""
        definition = context.factor_definition

        # 调用因子计算函数
        result = definition.func(df, definition.params)

        if result is None or result.is_empty():
            logger.warning(f"Factor {context.factor_id} returned empty result")
            return pl.DataFrame()

        return result




class DateRangeFilterProcessor(IProcessor):
    """日期范围过滤器 - 过滤到目标计算范围"""

    @property
    def name(self) -> str:
        return "DateRangeFilter"

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """过滤到 calc_start ~ calc_end 范围"""
        if "trade_date" not in df.columns:
            logger.warning("trade_date column not found, skipping date filter")
            return df

        calc_start = context.calc_start
        calc_end = context.calc_end

        # 转换字符串日期为 date 类型进行比较
        if df["trade_date"].dtype in [pl.Date, pl.Datetime]:
            # trade_date 是日期类型，需要将字符串转换为日期
            start_date = pl.lit(calc_start).str.to_date("%Y%m%d")
            end_date = pl.lit(calc_end).str.to_date("%Y%m%d")

            before = len(df)
            df = df.filter(
                (pl.col("trade_date") >= start_date) &
                (pl.col("trade_date") <= end_date)
            )
        else:
            # trade_date 是字符串或整数类型
            before = len(df)
            df = df.filter(
                (pl.col("trade_date") >= calc_start) &
                (pl.col("trade_date") <= calc_end)
            )

        logger.info(f"Date range filter [{calc_start}, {calc_end}]: {before} -> {len(df)}")

        return df


class CalendarAlignProcessor(IProcessor):
    """交易日历对齐处理器 - 窗口内有停牌缺口时将 factor_value 置 null"""

    def __init__(self, db_client, trading_cal):
        self.db = db_client
        self.trading_cal = trading_cal

    @property
    def name(self) -> str:
        return "CalendarAlign"

    def should_run(self, context: ProcessContext) -> bool:
        return context.factor_definition.align_calendar

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        if "factor_value" not in df.columns or "trade_date" not in df.columns:
            return df

        definition = context.factor_definition
        lookback_days = definition.params.get("lookback_days", 60) if definition.params else 60
        # 优先从 params 中取 window，其次用 lookback_days
        window = definition.params.get("window", lookback_days) if definition.params else lookback_days

        # 加载交易日历（只取计算区间内的交易日）
        cal_df = self.db.query(
            "SELECT cal_date FROM sync_trade_cal WHERE is_open = 1 ORDER BY cal_date"
        )
        if cal_df.is_empty():
            logger.warning("CalendarAlign: trading calendar empty, skipping")
            return df

        # 将交易日历转为 YYYYMMDD 字符串 set，方便查找
        trading_days = set(
            cal_df["cal_date"].cast(pl.Utf8).str.replace_all("-", "").to_list()
        )

        # 对每只股票，统计每个计算日前 window 个交易日内实际有数据的天数
        # 方法：用 rolling_count 统计行数，再与交易日历中应有的天数比较
        # 交易日历中应有天数 = window（严格模式：窗口内每个交易日都必须有数据）

        # 先确保 trade_date 是字符串格式 YYYYMMDD
        if df["trade_date"].dtype != pl.Utf8:
            df = df.with_columns(
                pl.col("trade_date").cast(pl.Utf8).str.replace_all("-", "").alias("trade_date")
            )

        # 按股票分组，计算每个交易日前 window 行内的实际数据行数
        df = df.sort(["ts_code", "trade_date"])
        df = df.with_columns(
            pl.col("trade_date")
              .rolling_count(window_size=window)
              .over("ts_code")
              .alias("_actual_rows")
        )

        # 对每个计算日，查询交易日历中该窗口应有的天数
        # 简化实现：用 _actual_rows < window 作为判断条件
        # 这在无停牌时永远满足（行数=window），有停牌时行数<window
        # 但由于停牌日不在 DataFrame 里，_actual_rows 始终 = min(window, 已有行数)
        # 真正的缺口检测：需要比较窗口起始日期到当前日期之间的交易日历天数

        # 构建每只股票每个交易日的"窗口起始交易日"
        df = df.with_columns(
            pl.col("trade_date")
              .shift(window - 1)
              .over("ts_code")
              .alias("_window_start_date")
        )

        # 计算交易日历中 [_window_start_date, trade_date] 应有的天数
        # 用 Python UDF 实现（Polars 原生不支持外部查找）
        sorted_cal = sorted(trading_days)

        def count_trading_days(start: str, end: str) -> int:
            if start is None or end is None:
                return 0
            import bisect
            lo = bisect.bisect_left(sorted_cal, start)
            hi = bisect.bisect_right(sorted_cal, end)
            return hi - lo

        # 将计算结果作为新列
        records = df.select(["_window_start_date", "trade_date"]).to_dicts()
        expected_counts = [
            count_trading_days(r["_window_start_date"], r["trade_date"])
            for r in records
        ]
        df = df.with_columns(
            pl.Series("_expected_days", expected_counts)
        )

        # 严格模式：实际行数 < 应有交易日数 → 置 null
        before = df.filter(pl.col("factor_value").is_not_null()).height
        df = df.with_columns(
            pl.when(
                pl.col("_actual_rows").is_null() |
                (pl.col("_expected_days") > pl.col("_actual_rows"))
            )
            .then(None)
            .otherwise(pl.col("factor_value"))
            .alias("factor_value")
        )
        after = df.filter(pl.col("factor_value").is_not_null()).height
        logger.info(f"CalendarAlign (window={window}): {before} -> {after} non-null values")

        # 清理临时列
        df = df.drop(["_actual_rows", "_window_start_date", "_expected_days"])

        return df


class QualityCheckerProcessor(IProcessor):
    """质量检查处理器 - 生成因子质量标记"""

    @property
    def name(self) -> str:
        return "QualityChecker"

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """生成质量标记"""
        if "factor_value" not in df.columns:
            logger.warning("factor_value column not found, skipping quality check")
            return df

        # 计算质量指标
        total = len(df)
        null_count = df.filter(pl.col("factor_value").is_null()).height
        null_rate = null_count / total if total > 0 else 0

        # 检测极端值（±3σ）
        mean = df.select(pl.col("factor_value").mean()).item()
        std = df.select(pl.col("factor_value").std()).item()

        if mean is not None and std is not None and std > 0:
            outlier_count = df.filter(
                (pl.col("factor_value") < mean - 3 * std) |
                (pl.col("factor_value") > mean + 3 * std)
            ).height
            outlier_rate = outlier_count / total if total > 0 else 0
        else:
            outlier_rate = 0

        # 生成质量标记（使用整数：0=good, 1=warning, 2=poor）
        if null_rate > 0.5:
            quality_flag = 2  # poor
        elif null_rate > 0.2 or outlier_rate > 0.05:
            quality_flag = 1  # warning
        else:
            quality_flag = 0  # good

        # 添加质量标记列
        df = df.with_columns(pl.lit(quality_flag).alias("quality_flag"))

        # 保存质量指标到共享状态
        context.set_state("quality_metrics", {
            "null_rate": null_rate,
            "outlier_rate": outlier_rate,
            "quality_flag": quality_flag
        })

        logger.info(f"Quality check: null_rate={null_rate:.2%}, outlier_rate={outlier_rate:.2%}, flag={quality_flag}")

        return df


class ResultWriterProcessor(IProcessor):
    """结果写入处理器 - 保存到 DolphinDB"""

    def __init__(self, db_client):
        self.db = db_client

    @property
    def name(self) -> str:
        return "ResultWriter"

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """保存因子计算结果"""
        factor_id = context.factor_id
        definition = context.factor_definition
        run_id = context.run_id

        # 准备写入数据
        result_df = df.clone()

        # 添加元数据列
        if "factor_id" not in result_df.columns:
            result_df = result_df.with_columns(pl.lit(factor_id).alias("factor_id"))

        if "run_id" not in result_df.columns and run_id:
            result_df = result_df.with_columns(pl.lit(run_id).alias("run_id"))

        if "task_version" not in result_df.columns:
            result_df = result_df.with_columns(pl.lit(1).alias("task_version"))

        if "data_version" not in result_df.columns:
            result_df = result_df.with_columns(pl.lit("v1").alias("data_version"))

        if "created_at" not in result_df.columns:
            result_df = result_df.with_columns(
                pl.lit(datetime.now()).alias("created_at")
            )

        # 确定存储表
        storage_config = definition.storage
        table_name = storage_config.target if storage_config else "factor_values"
        primary_keys = ["ts_code", "trade_date", "factor_id"] if table_name == "factor_values" else ["ts_code", "trade_date"]

        # 获取计算模式
        compute_mode = context.compute_mode

        # 写入数据库（upsert）
        try:
            if compute_mode == "full":
                # 全量模式：清空整个因子的所有数据
                rows = self.db.upsert(table_name, result_df, primary_keys, is_full_sync=True)
            else:
                # 增量模式：按 trade_date 逐个清空并写入（精确到 factor_id）
                if "trade_date" in result_df.columns:
                    trade_dates = result_df["trade_date"].unique().to_list()
                    total_rows = 0
                    for trade_date in trade_dates:
                        date_df = result_df.filter(pl.col("trade_date") == trade_date)
                        # 传入 factor_id，确保只删除该因子在该日期的数据
                        self.db.upsert(
                            table_name, date_df, primary_keys,
                            is_full_sync=False,
                            trade_date=trade_date,
                            factor_id=factor_id
                        )
                        total_rows += len(date_df)
                    rows = total_rows
                else:
                    # 没有 trade_date 列，直接写入
                    rows = self.db.upsert(table_name, result_df, primary_keys, is_full_sync=False)

            logger.info(f"Saved {rows} rows to {table_name} (mode={compute_mode})")
            context.set_state("saved_rows", rows)
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            raise

        return result_df
