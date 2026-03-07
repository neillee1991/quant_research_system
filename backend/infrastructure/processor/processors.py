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

        # 加载数据配置
        config = self.data_config.load()

        # 加载并合并所有依赖表
        merged_df = None
        for dep in depends_on:
            dep_config = config.get(dep)
            if not dep_config:
                logger.warning(f"Data config not found for {dep}")
                continue

            table_name = dep_config["table_name"]
            field_mapping = dep_config["field_mapping"]

            # 构建查询列
            select_cols = ["ts_code", "trade_date"] + list(field_mapping.keys())
            col_str = ", ".join(select_cols)

            # 转换日期格式
            ddb_start = f"{data_start[:4]}.{data_start[4:6]}.{data_start[6:]}"
            ddb_end = f"{calc_end[:4]}.{calc_end[4:6]}.{calc_end[6:]}"

            # 查询数据
            query = (
                f"SELECT {col_str} FROM {table_name} "
                f"WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
            )
            dep_df = self.db.query(query)

            if dep_df.is_empty():
                logger.warning(f"No data loaded from {table_name}")
                continue

            # 应用字段映射
            dep_df = dep_df.rename(field_mapping)

            # 合并数据
            if merged_df is None:
                merged_df = dep_df
            else:
                merged_df = merged_df.join(
                    dep_df,
                    on=["ts_code", "trade_date"],
                    how="outer"
                )

        if merged_df is None or merged_df.is_empty():
            logger.warning("No data loaded from any dependency")
            return pl.DataFrame()

        # 保存原始数据到共享状态（用于后续质量检查）
        context.set_state("raw_data", merged_df)

        return merged_df


class AdjustmentProcessor(IProcessor):
    """复权处理器 - 应用前复权/后复权"""

    def __init__(self, db_client):
        self.db = db_client

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

        # 加载复权因子
        ddb_start = f"{data_start[:4]}.{data_start[4:6]}.{data_start[6:]}"
        ddb_end = f"{calc_end[:4]}.{calc_end[4:6]}.{calc_end[6:]}"

        adj_df = self.db.query(
            f"SELECT ts_code, trade_date, adj_factor FROM sync_adj_factor "
            f"WHERE trade_date >= {ddb_start} AND trade_date <= {ddb_end}"
        )

        if adj_df.is_empty():
            logger.warning("No adj_factor data, skipping adjustment")
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

        logger.info(f"Applied {adjust_mode} adjustment to {existing_price_cols}")
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
            "is_suspend": config.get("is_suspend"),
            "is_limit": config.get("is_limit"),
        }

        # 按表分组加载
        tables = {}
        for field_key, cfg in field_configs.items():
            if not cfg:
                continue
            tbl = cfg["table"]
            col = cfg["column"]
            if tbl not in tables:
                tables[tbl] = []
            tables[tbl].append(col)

        # 加载状态数据
        status_dfs = []
        for tbl, cols in tables.items():
            if not self.db.table_exists(tbl):
                logger.warning(f"Status table {tbl} not exists, skipping")
                continue

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

            # 重命名列
            rename_map = {
                cfg["column"]: fk
                for fk, cfg in field_configs.items()
                if cfg and cfg["table"] == tbl
            }
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
        # 查询股票基本信息
        basic_df = self.db.query("SELECT ts_code, list_date FROM sync_stock_basic")
        if basic_df.is_empty():
            logger.warning("sync_stock_basic is empty, skipping new stock filter")
            return df

        # 计算每个股票在日期范围内的上市天数
        df = df.join(basic_df, on="ts_code", how="left")

        # 过滤：trade_date - list_date >= days
        before = len(df)
        df = df.filter(
            (pl.col("trade_date").cast(pl.Int32) - pl.col("list_date").cast(pl.Int32)) >= days
        )
        df = df.drop("list_date")

        logger.info(f"Filtered new stocks (<{days} days): {before} -> {len(df)}")
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


class SuspensionHandlerProcessor(IProcessor):
    """停牌处理器 - 停牌期间因子值置空"""

    def __init__(self, db_client, data_config):
        self.db = db_client
        self.data_config = data_config

    @property
    def name(self) -> str:
        return "SuspensionHandler"

    def should_run(self, context: ProcessContext) -> bool:
        """只有配置了停牌处理才执行"""
        return context.get_option("handle_suspension", False)

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """停牌期间因子值置空"""
        if "factor_value" not in df.columns:
            logger.warning("factor_value column not found, skipping suspension handling")
            return df

        # 如果已经有 is_suspend 列（来自 StatusFilter）
        if "is_suspend" in df.columns:
            before = df.filter(pl.col("factor_value").is_not_null()).height
            df = df.with_columns(
                pl.when(pl.col("is_suspend") == 1)
                .then(None)
                .otherwise(pl.col("factor_value"))
                .alias("factor_value")
            )
            after = df.filter(pl.col("factor_value").is_not_null()).height
            logger.info(f"Suspension handling: {before} -> {after} non-null values")

        return df


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

        before = len(df)
        df = df.filter(
            (pl.col("trade_date") >= calc_start) &
            (pl.col("trade_date") <= calc_end)
        )
        logger.info(f"Date range filter [{calc_start}, {calc_end}]: {before} -> {len(df)}")

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

        # 生成质量标记
        quality_flag = "good"
        if null_rate > 0.5:
            quality_flag = "poor"
        elif null_rate > 0.2 or outlier_rate > 0.05:
            quality_flag = "warning"

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

        if "created_at" not in result_df.columns:
            result_df = result_df.with_columns(
                pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("created_at")
            )

        # 确定存储表
        storage_config = definition.storage
        table_name = storage_config.table if storage_config else "factor_values"

        # 写入数据库（upsert）
        try:
            rows = self.db.upsert(table_name, result_df)
            logger.info(f"Saved {rows} rows to {table_name}")
            context.set_state("saved_rows", rows)
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            raise

        return result_df
