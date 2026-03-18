"""
远期收益计算器
手动计算各持有期远期收益，绕过 Alphalens 内部价格计算，支持 T+1 买入和价格类型选择。
"""
import polars as pl
import pandas as pd
from typing import List

from app.core.logger import logger

VALID_PRICE_COLS = {"open", "high", "low", "close"}


class ForwardReturnCalculator:
    """
    手动计算远期收益，替代 Alphalens 的 get_clean_factor_and_forward_returns 价格计算部分。

    支持：
    - next_day_entry=True:  T+1 日 entry_price 买入，T+1+period 日 close 卖出
    - next_day_entry=False: T 日 close 买入，T+period 日 close 卖出
    """

    @staticmethod
    def calc(
        factor_df: pl.DataFrame,
        price_df: pl.DataFrame,
        periods: List[int],
        next_day_entry: bool = True,
        entry_price: str = "open",
    ) -> pd.DataFrame:
        """
        计算各持有期远期收益，返回 Alphalens 兼容的 MultiIndex DataFrame。

        Args:
            factor_df: columns=[ts_code, trade_date, factor_value]
            price_df:  columns=[ts_code, trade_date, open, high, low, close]
            periods:   持有期列表，如 [1, 5, 10]
            next_day_entry: True=T+1买入，False=T日收盘买入
            entry_price: 买入价格列名，仅 next_day_entry=True 时有效

        Returns:
            pd.DataFrame with MultiIndex (date, asset):
              - factor: 因子值
              - {period}D: 各持有期远期收益（如 1D, 5D, 10D）
        """
        if entry_price not in VALID_PRICE_COLS:
            raise ValueError(f"entry_price must be one of {VALID_PRICE_COLS}, got '{entry_price}'")

        # 1. 按 ts_code, trade_date 排序价格数据
        price_sorted = price_df.sort(["ts_code", "trade_date"])

        # 2. 计算买入价：next_day_entry=True 时 shift(-1) 取次日价格
        if next_day_entry:
            price_with_entry = price_sorted.with_columns(
                pl.col(entry_price).shift(-1).over("ts_code").alias("_entry_price"),
            )
        else:
            price_with_entry = price_sorted.with_columns(
                pl.col("close").alias("_entry_price"),
            )

        # 3. 过滤买入价为 0 或 null 的行（停牌）
        price_with_entry = price_with_entry.filter(pl.col("_entry_price") > 0)

        # 4. 对每个 period 计算卖出价（T+1+period 日 close，或 T+period 日 close）
        result_df = price_with_entry.select(["ts_code", "trade_date", "_entry_price"])
        for period in periods:
            shift_n = period + 1 if next_day_entry else period
            exit_close = price_sorted.with_columns(
                pl.col("close").shift(-shift_n).over("ts_code").alias(f"_exit_{period}")
            ).select(["ts_code", "trade_date", f"_exit_{period}"])
            result_df = result_df.join(exit_close, on=["ts_code", "trade_date"], how="left")

        # 5. 计算收益率
        for period in periods:
            result_df = result_df.with_columns(
                ((pl.col(f"_exit_{period}") / pl.col("_entry_price")) - 1.0)
                .alias(f"{period}D")
            ).drop(f"_exit_{period}")

        result_df = result_df.drop("_entry_price")

        # 6. 合并因子值
        merged = factor_df.join(result_df, on=["ts_code", "trade_date"], how="inner")
        period_cols = [f"{p}D" for p in periods]
        merged = merged.drop_nulls(subset=["factor_value"] + period_cols)

        if merged.is_empty():
            raise ValueError("No valid rows after joining factor and forward returns")

        logger.info(f"ForwardReturnCalculator: {len(merged)} valid rows after join and drop_nulls")

        # 7. 转换为 Alphalens MultiIndex 格式
        merged_pd = merged.to_pandas()
        merged_pd["trade_date"] = pd.to_datetime(merged_pd["trade_date"], format="%Y%m%d")
        merged_pd = merged_pd.set_index(["trade_date", "ts_code"])
        merged_pd.index.names = ["date", "asset"]
        merged_pd = merged_pd.rename(columns={"factor_value": "factor"})

        keep_cols = ["factor"] + period_cols
        return merged_pd[keep_cols]
