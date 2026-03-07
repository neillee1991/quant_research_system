"""
MarketDataRepository - 市场行情数据访问层

封装 sync_daily_data 表的访问，提供：
1. 基础查询功能（继承自 BaseRepository）
2. 带复权的数据查询
3. 多表关联查询（行情 + 复权因子 + 股票状态）

使用示例：
    repo = MarketDataRepository(db_client)

    # 基础查询
    df = repo.find_by_date_range("20240101", "20240131")

    # 带复权查询
    df = repo.get_with_adjustment(
        ts_codes=["000001.SZ", "000002.SZ"],
        start_date="20240101",
        end_date="20240131",
        adjust_type="forward"  # forward/backward/none
    )

    # 带股票状态查询
    df = repo.get_with_status(
        ts_codes=["000001.SZ"],
        start_date="20240101",
        end_date="20240131",
        filter_st=True,
        filter_new_stock=True
    )
"""
from typing import List, Optional, Literal
import polars as pl

from app.core.logger import logger
from infrastructure.repository.base import BaseRepository
from infrastructure.database.query_builder import QueryBuilder


class MarketDataRepository(BaseRepository):
    """市场行情数据 Repository"""

    def __init__(self, db_client):
        """
        初始化 MarketDataRepository

        Args:
            db_client: DolphinDB 客户端
        """
        super().__init__(db_client, "sync_daily_data")

    def get_with_adjustment(
        self,
        ts_codes: Optional[List[str]] = None,
        start_date: str = "20100101",
        end_date: Optional[str] = None,
        adjust_type: Literal["forward", "backward", "none"] = "forward",
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        获取带复权的行情数据

        Args:
            ts_codes: 股票代码列表，None 表示所有股票
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)，None 表示今天
            adjust_type: 复权类型 (forward=前复权, backward=后复权, none=不复权)
            columns: 查询列，None 表示查询所有列

        Returns:
            Polars DataFrame，包含复权后的 OHLC 数据
        """
        if adjust_type == "none":
            # 不复权，直接查询原始数据
            if ts_codes:
                return self.find_by_codes(ts_codes, start_date, end_date or "20991231", columns)
            else:
                return self.find_by_date_range(start_date, end_date or "20991231", columns)

        # 需要复权，使用 DolphinDB 客户端的复权逻辑
        logger.debug(f"Loading market data with {adjust_type} adjustment")

        # 构建查询（使用 DolphinDB 的 SQL 语法）
        base_cols = columns or ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"]

        # 使用 DolphinDB 客户端的 load_table_data 方法（已包含复权逻辑）
        df = self.db.load_table_data(
            table_name="sync_daily_data",
            columns=base_cols,
            filters={
                "ts_code": ts_codes if ts_codes else None,
                "trade_date": (start_date, end_date or "20991231")
            }
        )

        if df.is_empty():
            return df

        # 应用复权
        if adjust_type in ("forward", "backward"):
            df = self._apply_adjustment(df, adjust_type)

        return df

    def _apply_adjustment(
        self,
        df: pl.DataFrame,
        adjust_type: Literal["forward", "backward"]
    ) -> pl.DataFrame:
        """
        应用复权因子

        Args:
            df: 原始行情数据
            adjust_type: 复权类型

        Returns:
            复权后的 DataFrame
        """
        if df.is_empty():
            return df

        # 加载复权因子
        ts_codes = df["ts_code"].unique().to_list()
        start_date = df["trade_date"].min()
        end_date = df["trade_date"].max()

        adj_query = QueryBuilder("sync_adj_factor") \
            .select(["ts_code", "trade_date", "adj_factor"]) \
            .where_in("ts_code", ts_codes) \
            .where_between("trade_date", start_date, end_date) \
            .build()

        adj_df = self.db.execute(adj_query.sql, adj_query.params)

        if adj_df.is_empty():
            logger.warning("No adjustment factors found, returning unadjusted data")
            return df

        # Join 复权因子
        df = df.join(adj_df, on=["ts_code", "trade_date"], how="left")

        # 填充缺失的复权因子（使用前向填充）
        df = df.with_columns(
            pl.col("adj_factor").fill_null(strategy="forward").over("ts_code")
        )

        # 应用复权
        if adjust_type == "forward":
            # 前复权：价格 * (最新复权因子 / 当日复权因子)
            latest_adj = df.group_by("ts_code").agg(pl.col("adj_factor").max().alias("latest_adj"))
            df = df.join(latest_adj, on="ts_code", how="left")

            df = df.with_columns([
                (pl.col("open") * pl.col("latest_adj") / pl.col("adj_factor")).alias("open"),
                (pl.col("high") * pl.col("latest_adj") / pl.col("adj_factor")).alias("high"),
                (pl.col("low") * pl.col("latest_adj") / pl.col("adj_factor")).alias("low"),
                (pl.col("close") * pl.col("latest_adj") / pl.col("adj_factor")).alias("close"),
            ])

            df = df.drop(["adj_factor", "latest_adj"])

        elif adjust_type == "backward":
            # 后复权：价格 * (当日复权因子 / 最早复权因子)
            earliest_adj = df.group_by("ts_code").agg(pl.col("adj_factor").min().alias("earliest_adj"))
            df = df.join(earliest_adj, on="ts_code", how="left")

            df = df.with_columns([
                (pl.col("open") * pl.col("adj_factor") / pl.col("earliest_adj")).alias("open"),
                (pl.col("high") * pl.col("adj_factor") / pl.col("earliest_adj")).alias("high"),
                (pl.col("low") * pl.col("adj_factor") / pl.col("earliest_adj")).alias("low"),
                (pl.col("close") * pl.col("adj_factor") / pl.col("earliest_adj")).alias("close"),
            ])

            df = df.drop(["adj_factor", "earliest_adj"])

        return df

    def get_with_status(
        self,
        ts_codes: Optional[List[str]] = None,
        start_date: str = "20100101",
        end_date: Optional[str] = None,
        filter_st: bool = False,
        filter_new_stock: bool = False,
        new_stock_days: int = 60,
        mark_limit: bool = False,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        获取带股票状态的行情数据

        Args:
            ts_codes: 股票代码列表，None 表示所有股票
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            filter_st: 是否过滤 ST 股票
            filter_new_stock: 是否过滤新股（上市 < new_stock_days 天）
            new_stock_days: 新股过滤天数
            mark_limit: 是否标记涨跌停
            columns: 查询列

        Returns:
            Polars DataFrame，包含股票状态字段
        """
        # 查询行情数据
        if ts_codes:
            df = self.find_by_codes(ts_codes, start_date, end_date or "20991231", columns)
        else:
            df = self.find_by_date_range(start_date, end_date or "20991231", columns)

        if df.is_empty():
            return df

        # 加载股票状态
        status_query = QueryBuilder("stock_daily_status") \
            .select(["ts_code", "trade_date", "is_st", "is_suspend", "is_limit_up", "is_limit_down", "list_days"]) \
            .where_between("trade_date", start_date, end_date or "20991231")

        if ts_codes:
            status_query.where_in("ts_code", ts_codes)

        status_df = self.db.execute(status_query.build().sql, status_query.build().params)

        if status_df.is_empty():
            logger.warning("No stock status data found")
            return df

        # Join 状态数据
        df = df.join(status_df, on=["ts_code", "trade_date"], how="left")

        # 应用过滤
        if filter_st:
            df = df.filter(pl.col("is_st") == False)

        if filter_new_stock:
            df = df.filter(pl.col("list_days") >= new_stock_days)

        # 标记涨跌停（可选）
        if mark_limit:
            # 涨跌停标记已在 stock_daily_status 表中
            pass

        return df

    def get_latest_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新交易日期

        Args:
            ts_code: 股票代码，None 表示所有股票

        Returns:
            最新交易日期 (YYYYMMDD)
        """
        query = QueryBuilder(self.table_name).select(["MAX(trade_date) as max_date"])

        if ts_code:
            query.where("ts_code", "=", ts_code)

        result = self.db.execute(query.build().sql, query.build().params)

        if result.is_empty():
            return None

        return result["max_date"][0]

    def get_codes_by_date(self, trade_date: str) -> List[str]:
        """
        获取指定日期的所有股票代码

        Args:
            trade_date: 交易日期 (YYYYMMDD)

        Returns:
            股票代码列表
        """
        query = QueryBuilder(self.table_name) \
            .select(["DISTINCT ts_code"]) \
            .where("trade_date", "=", trade_date) \
            .order_by(["ts_code"]) \
            .build()

        result = self.db.execute(query.sql, query.params)

        if result.is_empty():
            return []

        return result["ts_code"].to_list()
