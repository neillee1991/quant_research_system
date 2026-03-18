"""
因子中性化模块
对因子值进行市场、行业、市值三因子 OLS 回归，取残差作为中性化后的因子值。
"""
import numpy as np
import polars as pl
from typing import Optional, List

from app.core.logger import logger


class Neutralizer:
    """
    因子中性化器。

    支持三种控制变量：
    - market: 市场因子（截距项，即全市场等权）
    - industry: 行业因子（行业哑变量，历史快照）
    - size: 市值因子（对数市值）

    方法：逐日截面 OLS 回归，取残差作为中性化因子值。
    """

    @staticmethod
    def neutralize(
        factor_df: pl.DataFrame,
        controls: List[str] = ["market", "industry", "size"],
        industry_df: Optional[pl.DataFrame] = None,
        size_df: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """
        对因子值进行中性化处理。

        Args:
            factor_df: columns=[ts_code, trade_date, factor_value]
            controls: 控制变量列表，可选 "market", "industry", "size"
            industry_df: 行业数据，columns=[ts_code, trade_date, industry]
            size_df: 市值数据，columns=[ts_code, trade_date, size_value]

        Returns:
            pl.DataFrame with columns=[ts_code, trade_date, factor_value]（中性化后）
        """
        if not controls:
            return factor_df

        df = factor_df.clone()

        if "industry" in controls and industry_df is not None and not industry_df.is_empty():
            df = df.join(
                industry_df.select(["ts_code", "trade_date", "industry"]),
                on=["ts_code", "trade_date"],
                how="left",
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("industry"))

        if "size" in controls and size_df is not None and not size_df.is_empty():
            df = df.join(
                size_df.select(["ts_code", "trade_date", "size_value"]),
                on=["ts_code", "trade_date"],
                how="left",
            ).with_columns(
                pl.col("size_value").log().alias("log_size")
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("log_size"))

        dates = df["trade_date"].unique().sort().to_list()
        result_rows = []

        for date in dates:
            day_df = df.filter(pl.col("trade_date") == date).drop_nulls(subset=["factor_value"])
            if len(day_df) < 10:
                for row in day_df.iter_rows(named=True):
                    result_rows.append({
                        "ts_code": row["ts_code"],
                        "trade_date": row["trade_date"],
                        "factor_value": row["factor_value"],
                    })
                continue

            y = day_df["factor_value"].to_numpy().astype(float)
            X_cols = []

            if "industry" in controls and "industry" in day_df.columns:
                industries = day_df["industry"].to_list()
                unique_inds = sorted(set(i for i in industries if i is not None))
                if len(unique_inds) > 1:
                    for ind in unique_inds[1:]:
                        X_cols.append(
                            np.array([1.0 if i == ind else 0.0 for i in industries])
                        )

            if "size" in controls and "log_size" in day_df.columns:
                log_size = day_df["log_size"].to_numpy().astype(float)
                valid_size = ~np.isnan(log_size)
                if valid_size.sum() > len(y) * 0.5:
                    log_size = np.where(np.isnan(log_size), np.nanmean(log_size), log_size)
                    X_cols.append(log_size)

            n = len(y)
            intercept = np.ones(n)
            X = np.column_stack([intercept] + X_cols) if X_cols else intercept.reshape(-1, 1)

            try:
                coeffs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
                residuals = y - X @ coeffs
            except Exception as e:
                logger.warning(f"OLS failed for date {date}: {e}, using original values")
                residuals = y

            ts_codes = day_df["ts_code"].to_list()
            trade_dates = day_df["trade_date"].to_list()
            for ts_code, trade_date, resid in zip(ts_codes, trade_dates, residuals):
                result_rows.append({
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    "factor_value": float(resid),
                })

        if not result_rows:
            return factor_df

        result = pl.DataFrame(result_rows).with_columns(
            pl.col("trade_date").cast(factor_df["trade_date"].dtype)
        )
        logger.info(f"Neutralization complete: {len(result)} rows, controls={controls}")
        return result
