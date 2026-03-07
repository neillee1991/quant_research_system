"""
FactorDataRepository - 因子数据访问层

封装 factor_values 表的访问，提供：
1. 因子值查询
2. 因子结果保存（upsert）
3. 因子计算历史查询
4. 因子质量统计

使用示例：
    repo = FactorDataRepository(db_client)

    # 查询因子值
    df = repo.get_factor_values(
        factor_id="momentum_20",
        ts_codes=["000001.SZ", "000002.SZ"],
        start_date="20240101",
        end_date="20240131"
    )

    # 保存因子结果
    count = repo.save_factor_results(
        factor_id="momentum_20",
        data=result_df,
        run_id=123
    )

    # 查询因子最新日期
    latest_date = repo.get_latest_date("momentum_20")

    # 查询因子质量统计
    stats = repo.get_quality_stats("momentum_20", "20240101", "20240131")
"""
from typing import List, Optional, Dict, Any
import polars as pl
from datetime import datetime

from app.core.logger import logger
from infrastructure.repository.base import BaseRepository
from infrastructure.database.query_builder import QueryBuilder


class FactorDataRepository(BaseRepository):
    """因子数据 Repository"""

    def __init__(self, db_client):
        """
        初始化 FactorDataRepository

        Args:
            db_client: DolphinDB 客户端
        """
        super().__init__(db_client, "factor_values")

    def get_factor_values(
        self,
        factor_id: str,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        查询因子值

        Args:
            factor_id: 因子ID
            ts_codes: 股票代码列表，None 表示所有股票
            start_date: 开始日期 (YYYYMMDD)，None 表示不限
            end_date: 结束日期 (YYYYMMDD)，None 表示不限
            columns: 查询列，None 表示查询所有列

        Returns:
            Polars DataFrame
        """
        query = QueryBuilder(self.table_name)

        if columns:
            query.select(columns)
        else:
            query.select_all()

        query.where("factor_id", "=", factor_id)

        if ts_codes:
            query.where_in("ts_code", ts_codes)

        if start_date and end_date:
            query.where_between("trade_date", start_date, end_date)
        elif start_date:
            query.where("trade_date", ">=", start_date)
        elif end_date:
            query.where("trade_date", "<=", end_date)

        query.order_by(["trade_date", "ts_code"])

        built_query = query.build()
        logger.debug(f"Querying factor values: {built_query.sql}")

        result = self.db.execute(built_query.sql, built_query.params)
        return result

    def save_factor_results(
        self,
        factor_id: str,
        data: pl.DataFrame,
        run_id: Optional[int] = None
    ) -> int:
        """
        保存因子计算结果

        Args:
            factor_id: 因子ID
            data: 因子结果 DataFrame，必须包含 ts_code, trade_date, factor_value
            run_id: 运行ID（可选）

        Returns:
            保存的行数
        """
        if data.is_empty():
            logger.warning(f"Attempted to save empty factor results for {factor_id}")
            return 0

        # 验证必需列
        required_cols = ["ts_code", "trade_date", "factor_value"]
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # 添加 factor_id 和 run_id
        data = data.with_columns([
            pl.lit(factor_id).alias("factor_id"),
        ])

        if run_id is not None:
            data = data.with_columns([
                pl.lit(run_id).alias("run_id"),
            ])

        # 添加时间戳
        data = data.with_columns([
            pl.lit(datetime.now()).alias("updated_at"),
        ])

        # 保存到数据库
        count = self.save(data)
        logger.info(f"Saved {count} factor values for {factor_id}")
        return count

    def get_latest_date(self, factor_id: str, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取因子的最新计算日期

        Args:
            factor_id: 因子ID
            ts_code: 股票代码，None 表示所有股票

        Returns:
            最新日期 (YYYYMMDD)
        """
        query = QueryBuilder(self.table_name) \
            .select(["MAX(trade_date) as max_date"]) \
            .where("factor_id", "=", factor_id)

        if ts_code:
            query.where("ts_code", "=", ts_code)

        result = self.db.execute(query.build().sql, query.build().params)

        if result.is_empty():
            return None

        return result["max_date"][0]

    def get_date_range(self, factor_id: str) -> tuple[Optional[str], Optional[str]]:
        """
        获取因子的日期范围

        Args:
            factor_id: 因子ID

        Returns:
            (最早日期, 最新日期)
        """
        query = QueryBuilder(self.table_name) \
            .select(["MIN(trade_date) as min_date", "MAX(trade_date) as max_date"]) \
            .where("factor_id", "=", factor_id) \
            .build()

        result = self.db.execute(query.sql, query.params)

        if result.is_empty():
            return None, None

        return result["min_date"][0], result["max_date"][0]

    def get_quality_stats(
        self,
        factor_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取因子质量统计

        Args:
            factor_id: 因子ID
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)

        Returns:
            质量统计字典，包含：
            - total_count: 总记录数
            - null_count: 空值数量
            - null_rate: 空值率
            - valid_count: 有效值数量
            - mean: 均值
            - std: 标准差
            - min: 最小值
            - max: 最大值
        """
        # 查询因子值
        df = self.get_factor_values(
            factor_id=factor_id,
            start_date=start_date,
            end_date=end_date,
            columns=["factor_value", "quality_flag"]
        )

        if df.is_empty():
            return {
                "total_count": 0,
                "null_count": 0,
                "null_rate": 0.0,
                "valid_count": 0,
                "mean": None,
                "std": None,
                "min": None,
                "max": None,
            }

        total_count = len(df)
        null_count = df["factor_value"].null_count()
        valid_count = total_count - null_count
        null_rate = null_count / total_count if total_count > 0 else 0.0

        # 计算统计量（仅对非空值）
        valid_df = df.filter(pl.col("factor_value").is_not_null())

        if valid_df.is_empty():
            return {
                "total_count": total_count,
                "null_count": null_count,
                "null_rate": null_rate,
                "valid_count": valid_count,
                "mean": None,
                "std": None,
                "min": None,
                "max": None,
            }

        stats = valid_df.select([
            pl.col("factor_value").mean().alias("mean"),
            pl.col("factor_value").std().alias("std"),
            pl.col("factor_value").min().alias("min"),
            pl.col("factor_value").max().alias("max"),
        ]).to_dicts()[0]

        return {
            "total_count": total_count,
            "null_count": null_count,
            "null_rate": null_rate,
            "valid_count": valid_count,
            **stats,
        }

    def delete_factor_values(
        self,
        factor_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_codes: Optional[List[str]] = None
    ) -> int:
        """
        删除因子值

        Args:
            factor_id: 因子ID
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            ts_codes: 股票代码列表

        Returns:
            删除的行数
        """
        conditions = {"factor_id": factor_id}

        if start_date and end_date:
            # 使用 WHERE BETWEEN 需要特殊处理
            query = QueryBuilder(self.table_name) \
                .where("factor_id", "=", factor_id) \
                .where_between("trade_date", start_date, end_date)

            if ts_codes:
                query.where_in("ts_code", ts_codes)

            built_query = query.build()
            delete_sql = built_query.sql.replace(
                f"SELECT * FROM {self.table_name}",
                f"DELETE FROM {self.table_name}"
            )

            count = self.db.execute_delete(delete_sql, built_query.params)
            logger.info(f"Deleted {count} factor values for {factor_id}")
            return count

        if ts_codes:
            conditions["ts_code"] = ts_codes

        return self.delete(conditions)

    def get_factor_coverage(
        self,
        factor_id: str,
        trade_date: str
    ) -> Dict[str, Any]:
        """
        获取因子在指定日期的覆盖率

        Args:
            factor_id: 因子ID
            trade_date: 交易日期 (YYYYMMDD)

        Returns:
            覆盖率统计，包含：
            - total_stocks: 总股票数（从 sync_daily_data 查询）
            - factor_stocks: 有因子值的股票数
            - coverage_rate: 覆盖率
        """
        # 查询当日所有股票
        market_query = QueryBuilder("sync_daily_data") \
            .select(["COUNT(DISTINCT ts_code) as total"]) \
            .where("trade_date", "=", trade_date) \
            .build()

        market_result = self.db.execute(market_query.sql, market_query.params)
        total_stocks = market_result["total"][0] if not market_result.is_empty() else 0

        # 查询当日有因子值的股票
        factor_query = QueryBuilder(self.table_name) \
            .select(["COUNT(DISTINCT ts_code) as factor_total"]) \
            .where("factor_id", "=", factor_id) \
            .where("trade_date", "=", trade_date) \
            .where_not_null("factor_value") \
            .build()

        factor_result = self.db.execute(factor_query.sql, factor_query.params)
        factor_stocks = factor_result["factor_total"][0] if not factor_result.is_empty() else 0

        coverage_rate = factor_stocks / total_stocks if total_stocks > 0 else 0.0

        return {
            "total_stocks": total_stocks,
            "factor_stocks": factor_stocks,
            "coverage_rate": coverage_rate,
        }

    def get_factors_by_date(
        self,
        trade_date: str,
        ts_codes: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        获取指定日期的所有因子值（宽表格式）

        Args:
            trade_date: 交易日期 (YYYYMMDD)
            ts_codes: 股票代码列表，None 表示所有股票

        Returns:
            Polars DataFrame，每个因子一列
        """
        query = QueryBuilder(self.table_name) \
            .select(["ts_code", "factor_id", "factor_value"]) \
            .where("trade_date", "=", trade_date)

        if ts_codes:
            query.where_in("ts_code", ts_codes)

        built_query = query.build()
        result = self.db.execute(built_query.sql, built_query.params)

        if result.is_empty():
            return pl.DataFrame()

        # 透视为宽表（每个因子一列）
        pivot_df = result.pivot(
            values="factor_value",
            index="ts_code",
            columns="factor_id"
        )

        return pivot_df
