import polars as pl
import numpy as np


class FactorAnalyzer:
    """IC/Rank IC analysis and factor evaluation."""

    @staticmethod
    def ic(factor: pl.Series, forward_return: pl.Series) -> float:
        """Pearson IC between factor and forward return."""
        df = pl.DataFrame({"f": factor, "r": forward_return}).drop_nulls()
        if len(df) < 5:
            return float("nan")
        return float(np.corrcoef(df["f"].to_numpy(), df["r"].to_numpy())[0, 1])

    @staticmethod
    def rank_ic(factor: pl.Series, forward_return: pl.Series) -> float:
        """Spearman Rank IC."""
        df = pl.DataFrame({"f": factor, "r": forward_return}).drop_nulls()
        if len(df) < 5:
            return float("nan")
        return float(np.corrcoef(df["f"].rank("average").to_numpy(), df["r"].rank("average").to_numpy())[0, 1])

    @staticmethod
    def ic_series(df: pl.DataFrame, factor_col: str, return_col: str) -> pl.DataFrame:
        """Compute IC per date."""
        results = []
        for date, group in df.group_by("trade_date"):
            ic_val = FactorAnalyzer.ic(group[factor_col], group[return_col])
            rank_ic_val = FactorAnalyzer.rank_ic(group[factor_col], group[return_col])
            results.append({"trade_date": date[0], "ic": ic_val, "rank_ic": rank_ic_val})
        return pl.DataFrame(results).sort("trade_date")

    @staticmethod
    def ic_summary(ic_df: pl.DataFrame) -> dict:
        """IC mean, std, IR, win rate."""
        ic = ic_df["ic"].drop_nulls()
        rank_ic = ic_df["rank_ic"].drop_nulls()
        return {
            "ic_mean": float(ic.mean()),
            "ic_std": float(ic.std()),
            "ic_ir": float(ic.mean() / (ic.std() + 1e-10)),
            "rank_ic_mean": float(rank_ic.mean()),
            "rank_ic_ir": float(rank_ic.mean() / (rank_ic.std() + 1e-10)),
            "ic_win_rate": float((ic > 0).mean()),
        }

    @staticmethod
    def layered_returns(df: pl.DataFrame, factor_col: str, return_col: str,
                        n_groups: int = 5) -> pl.DataFrame:
        """Stratified return analysis by factor quantile."""
        df = df.with_columns(
            pl.col(factor_col).rank("average").over("trade_date").alias("_rank"),
            pl.col(factor_col).count().over("trade_date").alias("_count"),
        ).with_columns(
            (pl.col("_rank") / pl.col("_count") * n_groups).ceil().cast(pl.Int32).clip(1, n_groups).alias("quantile")
        )
        return df.group_by(["trade_date", "quantile"]).agg(
            pl.col(return_col).mean().alias("avg_return")
        ).sort(["trade_date", "quantile"])

    @staticmethod
    def correlation_matrix(df: pl.DataFrame, factor_cols: list[str]) -> pl.DataFrame:
        """Factor correlation matrix."""
        sub = df.select(factor_cols).drop_nulls()
        corr_data = {}
        for col_a in factor_cols:
            row = {}
            for col_b in factor_cols:
                row[col_b] = float(np.corrcoef(sub[col_a].to_numpy(), sub[col_b].to_numpy())[0, 1])
            corr_data[col_a] = row
        return pl.DataFrame(corr_data)
