"""
因子分析引擎
纯 Polars 实现 IC 分析、分层收益、换手率等核心因子评价指标
"""
import polars as pl
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

from app.core.logger import logger


class FactorAnalyzer:
    """因子分析器"""

    def __init__(self, db_client):
        self.db = db_client

    def analyze(
        self,
        factor_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        periods: List[int] = None,
        quantiles: int = 5,
    ) -> Optional[Dict[str, Any]]:
        """执行完整因子分析

        Args:
            factor_id: 因子ID
            start_date: 分析起始日期
            end_date: 分析结束日期
            periods: 持有期列表，默认 [1, 5, 10, 20]
            quantiles: 分层数量，默认5层
        """
        if periods is None:
            periods = [1, 5, 10, 20]

        started_at = datetime.now()
        logger.info(f"Analyzing factor: {factor_id}")

        try:
            # 1. 加载因子数据和收益率数据
            factor_df = self._load_factor_data(factor_id, start_date, end_date)
            if factor_df is None or factor_df.is_empty():
                logger.warning(f"No factor data for {factor_id}")
                return None

            price_df = self._load_price_data(factor_df, start_date, end_date, max(periods))
            if price_df is None or price_df.is_empty():
                logger.warning(f"No price data for analysis")
                return None

            # 2. 合并因子和价格
            merged = factor_df.join(price_df, on=["ts_code", "trade_date"], how="inner")
            logger.info(f"Merged data: {len(merged)} rows, {merged['trade_date'].n_unique()} dates")

            # 3. 计算各持有期 IC
            ic_results = {}
            for period in periods:
                ic_series = self._calc_ic_series(merged, period)
                if ic_series is not None and not ic_series.is_empty():
                    ic_results[period] = ic_series

            # 4. 计算分层收益
            quantile_returns = self._calc_quantile_returns(merged, periods, quantiles)

            # 5. 计算换手率
            turnover = self._calc_turnover(merged, quantiles)

            # 6. 汇总统计
            summary = self._build_summary(factor_id, ic_results, quantile_returns, turnover, periods)

            # 7. 持久化
            actual_start = merged["trade_date"].min()
            actual_end = merged["trade_date"].max()
            self._save_analysis(factor_id, summary, actual_start, actual_end, periods)

            elapsed = (datetime.now() - started_at).total_seconds()
            logger.info(f"Factor {factor_id} analysis done in {elapsed:.1f}s")
            return summary

        except Exception as e:
            logger.error(f"Factor analysis failed for {factor_id}: {e}")
            import traceback
            traceback.print_exc()
            return None

    # ==================== 数据加载 ====================

    def _load_factor_data(self, factor_id: str, start_date: Optional[str],
                          end_date: Optional[str]) -> Optional[pl.DataFrame]:
        """加载因子数据（支持分批加载大数据集）"""
        conditions = ["factor_id = %s"]
        params = [factor_id]
        if start_date:
            conditions.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= %s")
            params.append(end_date)

        where = " AND ".join(conditions)
        sql = f"SELECT ts_code, trade_date, factor_value FROM factor_values WHERE {where} ORDER BY trade_date, ts_code"

        try:
            df = self.db.query(sql, tuple(params))
            return df if not df.is_empty() else None
        except Exception as e:
            logger.error(f"Failed to load factor data: {e}")
            return None

    def _load_price_data(self, factor_df: pl.DataFrame, start_date: Optional[str],
                         end_date: Optional[str], max_period: int) -> Optional[pl.DataFrame]:
        """加载价格数据（支持分批加载），需要额外加载 max_period 天用于计算远期收益"""
        min_date = factor_df["trade_date"].min()
        max_date = factor_df["trade_date"].max()

        extra_days = max_period * 2
        from datetime import timedelta
        end_dt = datetime.strptime(max_date, "%Y%m%d") + timedelta(days=extra_days)
        load_end = end_dt.strftime("%Y%m%d")

        sql = """
            SELECT ts_code, trade_date, close, pct_chg
            FROM sync_daily_data
            WHERE trade_date >= %s AND trade_date <= %s
            ORDER BY ts_code, trade_date
        """
        try:
            df = self.db.query(sql, (min_date, load_end))
            return df if not df.is_empty() else None
        except Exception as e:
            logger.error(f"Failed to load price data: {e}")
            return None

    # ==================== IC 分析 ====================

    def _calc_ic_series(self, merged: pl.DataFrame, period: int) -> Optional[pl.DataFrame]:
        """计算指定持有期的 IC 时间序列（Rank IC）"""
        sorted_df = merged.sort(["ts_code", "trade_date"])

        # 计算远期收益率：period 天后的收益
        with_fwd = (
            sorted_df
            .with_columns(
                pl.col("close").shift(-period).over("ts_code").alias("fwd_close")
            )
            .with_columns(
                (pl.col("fwd_close") / pl.col("close") - 1.0).alias("fwd_return")
            )
            .drop_nulls(subset=["factor_value", "fwd_return"])
        )

        if with_fwd.is_empty():
            return None

        # 按日期计算截面 Rank IC（Spearman 相关系数）
        ic_list = []
        dates = with_fwd["trade_date"].unique().sort()

        for dt in dates:
            cross = with_fwd.filter(pl.col("trade_date") == dt)
            if len(cross) < 30:
                continue

            rank_factor = cross["factor_value"].rank()
            rank_return = cross["fwd_return"].rank()

            n = len(cross)
            mean_f = rank_factor.mean()
            mean_r = rank_return.mean()
            cov = ((rank_factor - mean_f) * (rank_return - mean_r)).sum()
            std_f = ((rank_factor - mean_f) ** 2).sum() ** 0.5
            std_r = ((rank_return - mean_r) ** 2).sum() ** 0.5

            if std_f > 0 and std_r > 0:
                ic = cov / (std_f * std_r)
                ic_list.append({"trade_date": dt, "ic": float(ic)})

        if not ic_list:
            return None

        return pl.DataFrame(ic_list)

    # ==================== 分层收益 ====================

    def _calc_quantile_returns(self, merged: pl.DataFrame, periods: List[int],
                               quantiles: int) -> Dict[int, pl.DataFrame]:
        """计算各持有期的分层收益"""
        sorted_df = merged.sort(["ts_code", "trade_date"])
        results = {}

        for period in periods:
            with_fwd = (
                sorted_df
                .with_columns(
                    pl.col("close").shift(-period).over("ts_code").alias("fwd_close")
                )
                .with_columns(
                    (pl.col("fwd_close") / pl.col("close") - 1.0).alias("fwd_return")
                )
                .drop_nulls(subset=["factor_value", "fwd_return"])
            )

            if with_fwd.is_empty():
                continue

            # 按日期截面分层
            with_q = with_fwd.with_columns(
                (pl.col("factor_value").rank().over("trade_date")
                 / pl.col("factor_value").count().over("trade_date")
                 * quantiles).cast(pl.Int32).clip(0, quantiles - 1).alias("quantile")
            )

            # 各层各日平均收益
            group_ret = (
                with_q.group_by(["trade_date", "quantile"])
                .agg(pl.col("fwd_return").mean().alias("mean_return"))
                .sort(["trade_date", "quantile"])
            )

            results[period] = group_ret

        return results

    # ==================== 换手率 ====================

    def _calc_turnover(self, merged: pl.DataFrame, quantiles: int) -> Optional[Dict[str, float]]:
        """计算各层换手率"""
        sorted_df = merged.sort(["ts_code", "trade_date"])

        with_q = sorted_df.with_columns(
            (pl.col("factor_value").rank().over("trade_date")
             / pl.col("factor_value").count().over("trade_date")
             * quantiles).cast(pl.Int32).clip(0, quantiles - 1).alias("quantile")
        )

        dates = with_q["trade_date"].unique().sort()
        if len(dates) < 2:
            return None

        turnover_by_q = {q: [] for q in range(quantiles)}

        prev_groups = {}
        for dt in dates:
            cross = with_q.filter(pl.col("trade_date") == dt)
            curr_groups = {}
            for q in range(quantiles):
                stocks = set(cross.filter(pl.col("quantile") == q)["ts_code"].to_list())
                curr_groups[q] = stocks

                if q in prev_groups and prev_groups[q]:
                    prev = prev_groups[q]
                    if prev and stocks:
                        overlap = len(prev & stocks)
                        total = max(len(prev), len(stocks))
                        turnover_by_q[q].append(1.0 - overlap / total if total > 0 else 0.0)

            prev_groups = curr_groups

        result = {}
        for q in range(quantiles):
            vals = turnover_by_q[q]
            result[f"Q{q+1}"] = sum(vals) / len(vals) if vals else 0.0

        return result

    # ==================== 汇总统计 ====================

    def _build_summary(self, factor_id: str, ic_results: Dict[int, pl.DataFrame],
                       quantile_returns: Dict[int, pl.DataFrame],
                       turnover: Optional[Dict[str, float]],
                       periods: List[int]) -> Dict[str, Any]:
        """构建分析结果摘要"""
        summary = {"factor_id": factor_id, "periods": {}, "turnover": turnover}

        for period in periods:
            period_summary = {}

            # IC 统计
            if period in ic_results:
                ic_df = ic_results[period]
                ic_vals = ic_df["ic"]
                ic_mean = float(ic_vals.mean())
                ic_std = float(ic_vals.std())
                period_summary["ic_mean"] = round(ic_mean, 6)
                period_summary["ic_std"] = round(ic_std, 6)
                period_summary["ic_ir"] = round(ic_mean / ic_std, 4) if ic_std > 0 else 0.0
                period_summary["ic_positive_ratio"] = round(
                    float((ic_vals > 0).sum()) / len(ic_vals), 4
                )
                period_summary["ic_series"] = [
                    {"date": r["trade_date"], "ic": round(r["ic"], 6)}
                    for r in ic_df.to_dicts()
                ]

            # 分层收益统计
            if period in quantile_returns:
                qr = quantile_returns[period]
                q_summary = (
                    qr.group_by("quantile")
                    .agg([
                        pl.col("mean_return").mean().alias("avg_return"),
                        pl.col("mean_return").std().alias("std_return"),
                    ])
                    .sort("quantile")
                )
                period_summary["quantile_returns"] = [
                    {
                        "quantile": f"Q{int(r['quantile'])+1}",
                        "avg_return": round(float(r["avg_return"]), 6),
                        "std_return": round(float(r["std_return"]), 6),
                        "sharpe": round(
                            float(r["avg_return"]) / float(r["std_return"]), 4
                        ) if r["std_return"] and float(r["std_return"]) > 0 else 0.0,
                    }
                    for r in q_summary.to_dicts()
                ]

                # 多空收益
                returns_by_q = {int(r["quantile"]): float(r["avg_return"]) for r in q_summary.to_dicts()}
                max_q = max(returns_by_q.keys()) if returns_by_q else 0
                long_ret = returns_by_q.get(max_q, 0)
                short_ret = returns_by_q.get(0, 0)
                period_summary["long_short_return"] = round(long_ret - short_ret, 6)

            summary["periods"][str(period)] = period_summary

        # 主要指标（取 period=1 的 IC 作为默认）
        if "1" in summary["periods"]:
            p1 = summary["periods"]["1"]
            summary["ic_mean"] = p1.get("ic_mean", 0)
            summary["ic_std"] = p1.get("ic_std", 0)
            summary["ic_ir"] = p1.get("ic_ir", 0)
        elif periods and str(periods[0]) in summary["periods"]:
            p = summary["periods"][str(periods[0])]
            summary["ic_mean"] = p.get("ic_mean", 0)
            summary["ic_std"] = p.get("ic_std", 0)
            summary["ic_ir"] = p.get("ic_ir", 0)

        return summary

    # ==================== 持久化 ====================

    def _save_analysis(self, factor_id: str, summary: Dict, start_date: str,
                       end_date: str, periods: List[int]):
        """保存分析结果到数据库"""
        try:
            ic_series = None
            p_key = "1" if "1" in summary.get("periods", {}) else str(periods[0]) if periods else None
            if p_key and p_key in summary.get("periods", {}):
                ic_series = summary["periods"][p_key].get("ic_series")

            quantile_returns = None
            if p_key and p_key in summary.get("periods", {}):
                quantile_returns = summary["periods"][p_key].get("quantile_returns")

            self.db.execute("""
                INSERT INTO factor_analysis (
                    factor_id, start_date, end_date, periods,
                    ic_mean, ic_std, rank_ic_mean, rank_ic_std, ic_ir,
                    turnover_mean, quantile_returns, ic_series
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                factor_id, start_date, end_date,
                json.dumps(periods),
                summary.get("ic_mean", 0),
                summary.get("ic_std", 0),
                summary.get("ic_mean", 0),  # rank_ic = ic（我们用的就是 Rank IC）
                summary.get("ic_std", 0),
                summary.get("ic_ir", 0),
                sum(summary.get("turnover", {}).values()) / max(len(summary.get("turnover", {})), 1)
                if summary.get("turnover") else 0,
                json.dumps(quantile_returns) if quantile_returns else None,
                json.dumps(ic_series) if ic_series else None,
            ))
            logger.info(f"Saved analysis result for {factor_id}")
        except Exception as e:
            logger.error(f"Failed to save analysis: {e}")

    # ==================== 查询接口 ====================

    def get_latest_analysis(self, factor_id: str) -> Optional[Dict]:
        """获取最新分析结果"""
        try:
            df = self.db.query("""
                SELECT * FROM factor_analysis
                WHERE factor_id = %s
                ORDER BY analysis_date DESC LIMIT 1
            """, (factor_id,))
            if df.is_empty():
                return None
            row = df.to_dicts()[0]
            # 解析 JSON 字段
            for key in ["periods", "quantile_returns", "ic_series"]:
                if row.get(key) and isinstance(row[key], str):
                    row[key] = json.loads(row[key])
            return row
        except Exception as e:
            logger.error(f"Failed to get analysis: {e}")
            return None

    def get_analysis_history(self, factor_id: str, limit: int = 10) -> List[Dict]:
        """获取分析历史"""
        try:
            df = self.db.query("""
                SELECT id, factor_id, analysis_date, start_date, end_date,
                       ic_mean, ic_std, ic_ir, turnover_mean
                FROM factor_analysis
                WHERE factor_id = %s
                ORDER BY analysis_date DESC LIMIT %s
            """, (factor_id, limit))
            return df.to_dicts() if not df.is_empty() else []
        except Exception as e:
            logger.error(f"Failed to get analysis history: {e}")
            return []
