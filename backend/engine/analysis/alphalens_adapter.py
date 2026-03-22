"""
Alphalens 框架适配器
处理 Polars → Pandas 数据转换，调用 alphalens 核心函数，序列化结果
"""
import polars as pl
import pandas as pd
import numpy as np
from scipy import stats as scipy_stats
from typing import List, Dict, Any, Optional
import alphalens
from alphalens.performance import (
    factor_information_coefficient,
    mean_return_by_quantile,
    quantile_turnover,
    factor_rank_autocorrelation,
    factor_alpha_beta,
    factor_returns,
    compute_mean_returns_spread,
    mean_information_coefficient,
    average_cumulative_return_by_quantile,
)

from app.core.logger import logger


class AlphalensAdapter:
    """Alphalens 框架适配器，处理 Polars → Pandas 转换"""

    def __init__(self, db_client):
        self.db = db_client

    def prepare_factor_data(
        self,
        factor_df: pl.DataFrame,      # (ts_code, trade_date, factor_value)
        price_df: pl.DataFrame,        # (ts_code, trade_date, open, high, low, close)
        periods: List[int] = [1, 5, 10, 20],
        quantiles: int = 5,
        groupby_df: Optional[pl.DataFrame] = None,  # (ts_code, trade_date, group_value)
        next_day_entry: bool = True,
        entry_price: str = "open",
    ) -> pd.DataFrame:
        """
        转换为 alphalens 所需的格式

        Args:
            factor_df: 因子数据
            price_df: 价格数据（需含 open/high/low/close 列）
            periods: 持仓周期列表
            quantiles: 分组数量
            groupby_df: 分组数据（可选），如行业、市值等
            next_day_entry: True=T+1日买入，False=T日收盘买入
            entry_price: 买入价格列名（next_day_entry=True 时有效）

        Returns:
            pd.DataFrame with MultiIndex (date, asset) and columns:
            - factor: 因子值
            - 1D, 5D, 10D, 20D: 各周期远期收益
            - factor_quantile: 分位数标签
            - group (optional): 分组标签
        """
        from engine.analysis.forward_returns import ForwardReturnCalculator
        from alphalens.utils import quantize_factor

        logger.info(f"Preparing factor data: {len(factor_df)} rows, {factor_df['ts_code'].n_unique()} stocks")

        # 1. 用 ForwardReturnCalculator 手动计算远期收益
        factor_data_raw = ForwardReturnCalculator.calc(
            factor_df=factor_df,
            price_df=price_df,
            periods=periods,
            next_day_entry=next_day_entry,
            entry_price=entry_price,
        )

        # 去重，确保 MultiIndex 唯一（同一 date+asset 保留第一条）
        factor_data_raw = factor_data_raw[~factor_data_raw.index.duplicated(keep='first')]

        # 2. 添加分位数列（Alphalens 后续分析需要 factor_quantile 列）
        factor_data_raw["factor_quantile"] = quantize_factor(
            factor_data_raw,
            quantiles=quantiles,
            bins=None,
            by_group=False,
            no_raise=True,
        )

        # 3. 处理分组数据（如果有）
        if groupby_df is not None and not groupby_df.is_empty():
            groupby_series = self._prepare_groupby(groupby_df)
            factor_data_raw = factor_data_raw.join(
                groupby_series.rename("group"), how="left"
            )

        logger.info(f"Factor data prepared: {len(factor_data_raw)} rows")
        return factor_data_raw

    def _prepare_groupby(self, groupby_df: pl.DataFrame) -> pd.Series:
        """转换分组数据为 MultiIndex Series

        Args:
            groupby_df: Polars DataFrame with columns (ts_code, trade_date, group_value)

        Returns:
            pd.Series with MultiIndex (date, asset)
        """
        groupby_pd = groupby_df.to_pandas()
        groupby_pd['trade_date'] = pd.to_datetime(groupby_pd['trade_date'], format='%Y%m%d')

        # 重命名列以匹配 alphalens 期望的格式
        col_name = [c for c in groupby_pd.columns if c not in ['ts_code', 'trade_date']][0]
        series = groupby_pd.set_index(['trade_date', 'ts_code'])[col_name]
        series.index.names = ['date', 'asset']
        return series

    def run_full_analysis(
        self,
        factor_data: pd.DataFrame,
        periods: List[int],
        quantiles: int
    ) -> Dict[str, Any]:
        """
        运行完整 alphalens 分析

        Returns:
            {
                'ic_summary': {...},
                'ic_by_period': [...],
                'ic_ts': [...],
                'quantile_returns': [...],
                'cumulative_returns': [...],
                'ic_by_group': {...},      # 如果有分组
                'returns_by_group': {...},  # 如果有分组
                'turnover': {...},
                'decay_analysis': {...},
                'charts_data': {...}        # 预计算的图表数据
            }
        """
        results = {}
        has_group = 'group' in factor_data.columns

        logger.info("Running alphalens analysis...")

        # 1. IC 分析
        logger.info("Computing IC...")
        try:
            ic = factor_information_coefficient(factor_data)
            results['ic_summary'] = self._serialize_ic_summary(ic)
            results['ic_by_period'] = self._serialize_ic_by_period(ic)
            results['ic_ts'] = self._serialize_ic_ts(ic)
        except Exception as e:
            logger.error(f"Failed to compute IC: {e}")
            results['ic_summary'] = {}
            results['ic_by_period'] = []
            results['ic_ts'] = []

        # 1b. Rank IC（手动计算 Spearman 相关系数，逐日）
        logger.info("Computing Rank IC...")
        try:
            period_cols = [c for c in factor_data.columns if c.endswith("D") and c[:-1].isdigit()]
            rank_ic_rows = []
            # 质量控制：记录每个 period 因样本不足（<30）被跳过的切片数
            skipped_counts = {col: 0 for col in period_cols}
            total_dates = 0
            for date, group in factor_data.groupby(level="date"):
                total_dates += 1
                row = {"date": date}
                for col in period_cols:
                    tmp = group[["factor", col]].dropna()
                    if len(tmp) >= 30:
                        row[col] = tmp["factor"].rank().corr(tmp[col].rank())
                    else:
                        row[col] = float("nan")
                        skipped_counts[col] += 1
                rank_ic_rows.append(row)
            rank_ic = pd.DataFrame(rank_ic_rows).set_index("date")
            results['rank_ic_summary'] = self._serialize_ic_summary(rank_ic)
            results['rank_ic_by_period'] = self._serialize_ic_by_period(rank_ic)
            results['rank_ic_quality'] = {
                col: {
                    "total_dates": total_dates,
                    "skipped_dates": skipped_counts[col],
                    "skipped_pct": round(skipped_counts[col] / total_dates * 100, 1) if total_dates > 0 else 0.0,
                }
                for col in period_cols
            }
        except Exception as e:
            logger.warning(f"Failed to compute Rank IC: {e}")
            results['rank_ic_summary'] = {}
            results['rank_ic_by_period'] = []
            results['rank_ic_quality'] = {}

        # 1c. IC Decay：固定最短持有期收益，计算 lag=0..20 期前因子值的预测能力衰减
        # IC_decay(lag) = corr(factor(t-lag), forward_return_shortest(t))
        logger.info("Computing IC Decay...")
        try:
            period_cols = [c for c in factor_data.columns if c.endswith("D") and c[:-1].isdigit()]
            if period_cols:
                shortest_col = sorted(period_cols, key=lambda x: int(x[:-1]))[0]
                max_lag = 20
                ic_decay = []
                # 按日期排序，取出因子值和最短期收益
                fd_sorted = factor_data[["factor", shortest_col]].copy()
                fd_sorted = fd_sorted.sort_index(level="date")
                # 按 asset 分组，对因子值做 shift(lag)
                for lag in range(0, max_lag + 1):
                    if lag == 0:
                        lagged_factor = fd_sorted["factor"]
                    else:
                        lagged_factor = fd_sorted.groupby(level="asset")["factor"].shift(lag)
                    tmp = pd.concat([lagged_factor, fd_sorted[shortest_col]], axis=1).dropna()
                    tmp.columns = ["lagged_factor", "fwd_ret"]
                    if len(tmp) > 30:
                        ic_val = tmp["lagged_factor"].corr(tmp["fwd_ret"], method="pearson")
                        rank_ic_val = tmp["lagged_factor"].corr(tmp["fwd_ret"], method="spearman")
                        ic_decay.append({
                            "lag": lag,
                            "ic": round(float(ic_val), 4),
                            "rank_ic": round(float(rank_ic_val), 4),
                        })
                    else:
                        ic_decay.append({"lag": lag, "ic": None, "rank_ic": None})
                results['ic_decay'] = ic_decay
                results['ic_decay_period'] = shortest_col  # 记录使用的基准 period
            else:
                results['ic_decay'] = []
        except Exception as e:
            logger.warning(f"Failed to compute IC Decay: {e}")
            results['ic_decay'] = []

        # 1d. Monthly IC heatmap
        logger.info("Computing monthly IC...")
        try:
            monthly_ic = mean_information_coefficient(factor_data, by_time='M')
            results['ic_by_month'] = [
                {
                    "month": str(idx),
                    **{str(col): round(float(monthly_ic.loc[idx, col]), 4) if not pd.isna(monthly_ic.loc[idx, col]) else None
                       for col in monthly_ic.columns}
                }
                for idx in monthly_ic.index
            ]
        except Exception as e:
            logger.warning(f"Failed to compute ic_by_month: {e}")
            results['ic_by_month'] = []

        # 1e. Event study (average cumulative return by quantile)，对每个 period 分别计算
        # 使用各 period 自身的日收益做事件窗口，避免跨 period 混用收益序列
        logger.info("Computing event study...")
        try:
            period_cols = [c for c in factor_data.columns if c.endswith('D') and c[:-1].isdigit()]
            import time as _time
            event_study_by_period = {}
            for col in period_cols:
                try:
                    returns_wide = factor_data[col].unstack(level='asset')
                    _t0 = _time.time()
                    avg_cum_ret = average_cumulative_return_by_quantile(
                        factor_data,
                        returns=returns_wide,
                        periods_before=5,
                        periods_after=15,
                        demeaned=True,
                    )
                    logger.info(f"event_study [{col}] computed in {_time.time() - _t0:.1f}s")
                    event_study = {}
                    for q in avg_cum_ret.index.get_level_values(0).unique():
                        event_study[str(int(q))] = {
                            "mean": {
                                str(c): round(float(avg_cum_ret.loc[(q, "mean"), c]), 6)
                                if not pd.isna(avg_cum_ret.loc[(q, "mean"), c]) else None
                                for c in avg_cum_ret.columns
                            },
                            "std": {
                                str(c): round(float(avg_cum_ret.loc[(q, "std"), c]), 6)
                                if not pd.isna(avg_cum_ret.loc[(q, "std"), c]) else None
                                for c in avg_cum_ret.columns
                            }
                        }
                    event_study_by_period[col] = event_study
                except Exception as e:
                    logger.warning(f"event_study failed for period {col}: {e}")
            results['event_study'] = event_study_by_period
        except Exception as e:
            logger.warning(f"Failed to compute event_study: {e}")
            results['event_study'] = {}

        # 2. 分层收益（demeaned=True，市场中性，与多空组合口径一致）
        try:
            mean_ret_by_q, std_ret_by_q = mean_return_by_quantile(
                factor_data, by_date=False, by_group=False, demeaned=True
            )
            results['quantile_returns'] = self._serialize_quantile_returns(mean_ret_by_q, std_ret_by_q)
        except Exception as e:
            logger.error(f"Failed to compute quantile returns: {e}")
            results['quantile_returns'] = []

        # 3. 累计收益（按每个 period 分别计算）
        logger.info("Computing cumulative returns...")
        try:
            results['cumulative_returns'] = {
                f'{p}D': self._compute_cumulative_returns(factor_data, p)
                for p in periods
            }
        except Exception as e:
            logger.warning(f"Failed to compute cumulative returns: {e}")
            results['cumulative_returns'] = {}

        # 3a. Alpha/Beta per period
        logger.info("Computing alpha/beta...")
        try:
            ab = factor_alpha_beta(factor_data, demeaned=True)
            results['alpha_beta'] = [
                {
                    "period": col,
                    "ann_alpha": float(ab.loc["Ann. alpha", col]),
                    "beta": float(ab.loc["beta", col]),
                }
                for col in ab.columns
            ]
        except Exception as e:
            logger.warning(f"Failed to compute alpha_beta: {e}")
            results['alpha_beta'] = []

        # 3b. Factor-weighted cumulative returns (long/short portfolio)
        logger.info("Computing factor cumulative returns...")
        try:
            fr = factor_returns(factor_data, demeaned=True, equal_weight=False)
            # 除以 period 归一化为单日收益，与分层累计收益口径一致
            for col in fr.columns:
                try:
                    p = int(col.replace('D', ''))
                    fr[col] = fr[col] / p
                except Exception:
                    pass
            cum_fr = (1 + fr).cumprod()
            results['factor_cumulative_returns'] = [
                {
                    "date": date.strftime('%Y%m%d'),
                    **{col: float(cum_fr.loc[date, col]) if not pd.isna(cum_fr.loc[date, col]) else None
                       for col in cum_fr.columns}
                }
                for date in cum_fr.index
            ]
        except Exception as e:
            logger.warning(f"Failed to compute factor_cumulative_returns: {e}")
            results['factor_cumulative_returns'] = []

        # 3c. Q5-Q1 spread time series per period（demeaned=True，与分层收益口径一致）
        logger.info("Computing spread time series...")
        try:
            mean_ret_by_date, _ = mean_return_by_quantile(
                factor_data, by_date=True, by_group=False, demeaned=True
            )
            upper_q = factor_data['factor_quantile'].max()
            lower_q = factor_data['factor_quantile'].min()
            # pass full DataFrame; result is DataFrame with one row per date, one col per period
            spread_df, _ = compute_mean_returns_spread(
                mean_ret_by_date,
                upper_quant=upper_q,
                lower_quant=lower_q,
            )
            spread_ts = {}
            for col in spread_df.columns:
                series = spread_df[col]
                spread_ts[str(col)] = {
                    (date.strftime('%Y%m%d') if hasattr(date, 'strftime') else str(date)): float(v) if not pd.isna(v) else None
                    for date, v in series.items()
                }
            results['spread_ts'] = spread_ts
        except Exception as e:
            logger.warning(f"Failed to compute spread_ts: {e}")
            results['spread_ts'] = {}

        # 4. 分组分析（如果有 groupby）
        if has_group:
            logger.info("Computing group-wise analysis...")
            try:
                ic_by_group = factor_information_coefficient(factor_data, by_group=True)
                results['ic_by_group'] = self._serialize_ic_by_group(ic_by_group)
            except Exception as e:
                logger.warning(f"Failed to compute IC by group: {e}")
                results['ic_by_group'] = {}

            try:
                ret_by_group, _ = mean_return_by_quantile(
                    factor_data, by_date=False, by_group=True, demeaned=False
                )
                results['returns_by_group'] = self._serialize_returns_by_group(ret_by_group)
            except Exception as e:
                logger.warning(f"Failed to compute returns by group: {e}")
                results['returns_by_group'] = {}

        # 5. 换手率 (为每个 period × quantile 计算)
        # quantile_turnover 签名: (quantile_factor, quantile, period=1)
        #   - quantile_factor: factor_data 中的 factor_quantile 列 (Series)
        #   - quantile: 具体的分位数编号 (int)
        #   - period: 换手周期
        logger.info("Computing turnover...")
        try:
            turnover_data = {}
            quantile_factor = factor_data['factor_quantile']
            all_quantiles = sorted(quantile_factor.unique())

            for period in periods:
                for q in all_quantiles:
                    try:
                        to = quantile_turnover(quantile_factor, int(q), period=period)
                        q_key = f'quantile_{int(q)}'
                        if q_key not in turnover_data:
                            turnover_data[q_key] = {}
                        turnover_data[q_key][f'period_{period}'] = {
                            str(date): float(value)
                            for date, value in to.items()
                            if isinstance(value, (int, float)) and not (np.isnan(value) or np.isinf(value))
                        }
                    except Exception as e:
                        logger.debug(f"Failed to compute turnover for q={q}, period={period}: {e}")

            results['turnover'] = turnover_data
            logger.info(f"Turnover computed with quantiles: {list(turnover_data.keys())}")
        except Exception as e:
            logger.warning(f"Failed to compute turnover: {e}", exc_info=True)
            results['turnover'] = {}

        # 6. 衰减分析（按每个 period 计算 lag=period 的自相关）
        logger.info("Computing decay analysis...")
        try:
            decay_analysis = {}
            for p in periods:
                autocorr = factor_rank_autocorrelation(factor_data, period=p)
                decay_analysis[f'{p}D'] = {
                    (date.strftime('%Y%m%d') if hasattr(date, 'strftime') else str(date)): float(v) if not pd.isna(v) else None
                    for date, v in autocorr.items()
                }
            results['decay_analysis'] = decay_analysis
        except Exception as e:
            logger.warning(f"Failed to compute decay: {e}")
            results['decay_analysis'] = {}

        # 7. 预计算图表数据
        logger.info("Preparing chart data...")
        results['charts_data'] = self._prepare_charts_data(results)

        logger.info("Alphalens analysis completed")
        return results

    # ==================== 序列化方法 ====================

    def _serialize_ic_summary(self, ic: pd.DataFrame) -> Dict:
        """序列化 IC 汇总统计（按 period 分别输出，不跨 period 平均）"""
        result = {}
        for col in ic.columns:
            series = ic[col].dropna()
            if len(series) == 0:
                continue
            ic_mean = float(series.mean())
            ic_std = float(series.std())
            result[str(col)] = {
                'ic_mean': round(ic_mean, 4),
                'ic_std': round(ic_std, 4),
                'ic_ir': round(ic_mean / (ic_std + 1e-10), 4),
                'ic_win_rate': round(float((series > 0).mean()), 4),
            }
        return result

    def _serialize_ic_by_period(self, ic: pd.DataFrame) -> List[Dict]:
        """序列化各周期 IC 统计（含 t 统计量和 p 值）"""
        result = []
        for col in ic.columns:
            series = ic[col].dropna()
            n = len(series)
            ic_mean = float(series.mean())
            ic_std = float(series.std())
            ic_ir = ic_mean / (ic_std + 1e-10)
            # t 统计量：t = IC_mean / (IC_std / sqrt(n))
            t_stat = float(ic_mean / (ic_std / np.sqrt(n))) if n > 1 and ic_std > 0 else 0.0
            # 双尾 p 值
            p_value = float(2 * scipy_stats.t.sf(abs(t_stat), df=n - 1)) if n > 1 else 1.0
            result.append({
                'period': str(col),
                'ic_mean': ic_mean,
                'ic_std': ic_std,
                'ic_ir': ic_ir,
                'ic_win_rate': float((series > 0).mean()),
                't_stat': round(t_stat, 4),
                'p_value': round(p_value, 4),
                'n_obs': n,
            })
        return result

    def _serialize_ic_ts(self, ic: pd.DataFrame) -> List[Dict]:
        """序列化 IC 时间序列"""
        ic_reset = ic.reset_index()
        # The index column name may vary (e.g. 'date', 'Date', or the first column)
        date_col = ic_reset.columns[0]
        return [
            {
                'date': row[date_col].strftime('%Y%m%d') if hasattr(row[date_col], 'strftime') else str(row[date_col]),
                **{f'ic_{col}': float(row[col]) if not pd.isna(row[col]) else None
                   for col in ic.columns}
            }
            for _, row in ic_reset.iterrows()
        ]

    def _serialize_quantile_returns(self, mean_ret: pd.DataFrame, std_ret: pd.DataFrame) -> List[Dict]:
        """序列化分位数收益"""
        results = []
        for period in mean_ret.columns:
            for quantile in mean_ret.index:
                mean_val = float(mean_ret.loc[quantile, period])
                std_val = float(std_ret.loc[quantile, period])
                results.append({
                    'period': str(period),
                    'quantile': int(quantile),
                    'mean_return': mean_val,
                    'std_return': std_val,
                    'sharpe': mean_val / (std_val + 1e-10)
                })
        return results

    def _compute_cumulative_returns(self, factor_data: pd.DataFrame, period: int) -> List[Dict]:
        """计算分层累计收益曲线（每日，alphalens 已将 forward return 归一化为单日收益）"""
        period_col = f'{period}D'
        if period_col not in factor_data.columns:
            return []

        mean_ret_by_date, _ = mean_return_by_quantile(
            factor_data, by_date=True, by_group=False, demeaned=True
        )

        quantiles = sorted(mean_ret_by_date.index.get_level_values('factor_quantile').unique())
        cumulative = {}
        for quantile in quantiles:
            try:
                daily = mean_ret_by_date.xs(quantile, level='factor_quantile')[period_col].dropna().sort_index()
                if daily.empty:
                    continue
                cumulative[f'quantile_{int(quantile)}'] = (1 + daily).cumprod()
            except Exception:
                continue

        all_dates = sorted(set(
            date for series in cumulative.values() for date in series.index
        ))
        return [
            {
                'date': date.strftime('%Y%m%d'),
                **{key: float(series.loc[date]) if date in series.index else None
                   for key, series in cumulative.items()}
            }
            for date in all_dates
        ]

    def _serialize_ic_by_group(self, ic_by_group: pd.DataFrame) -> Dict:
        """序列化分组 IC（按 group 取均值）"""
        # factor_information_coefficient(by_group=True) 返回 MultiIndex (date, group)
        # 需要按 group 聚合取均值
        if isinstance(ic_by_group.index, pd.MultiIndex):
            ic_mean = ic_by_group.groupby(level='group').mean()
        else:
            ic_mean = ic_by_group
        return {
            str(group): {
                str(period): float(ic_mean.loc[group, period])
                for period in ic_mean.columns
            }
            for group in ic_mean.index
        }

    def _serialize_returns_by_group(self, ret_by_group: pd.DataFrame) -> Dict:
        """序列化分组收益"""
        result = {}
        for group in ret_by_group.index.get_level_values(0).unique():
            group_data = ret_by_group.loc[group]
            result[str(group)] = [
                {
                    'period': str(period),
                    'quantile': str(quantile),
                    'mean_return': float(group_data.loc[quantile, period])
                }
                for period in group_data.columns
                for quantile in group_data.index
            ]
        return result

    def _serialize_turnover(self, turnover: pd.DataFrame) -> Dict:
        """序列化换手率"""
        return {
            f'quantile_{int(q)+1}': {
                str(period): float(turnover.loc[q, period])
                for period in turnover.columns
            }
            for q in turnover.index
        }

    def _serialize_decay(self, autocorr: pd.Series) -> Dict:
        """序列化衰减分析"""
        return {
            str(period): float(autocorr[period])
            for period in autocorr.index
        }

    def compute_industry_analysis(
        self,
        factor_data: pd.DataFrame,
        industry_groupby_df: pl.DataFrame,
        periods: List[int],
    ) -> Dict[str, Any]:
        """计算分行业 IC 和收益率分析（始终执行，不依赖 groupby_field）"""
        try:
            industry_series = self._prepare_groupby(industry_groupby_df)

            # 创建副本，添加 group 列（避免修改原始数据）
            fd = factor_data.copy()
            fd = fd.join(industry_series.rename("group"), how="left")
            fd = fd.dropna(subset=["group"])

            if fd.empty:
                logger.warning("No data after joining industry groups")
                return {}

            logger.info(f"Industry analysis: {fd['group'].nunique()} industries, {len(fd)} rows")

            result: Dict[str, Any] = {}

            try:
                ic_by_ind = factor_information_coefficient(fd, by_group=True)
                result['ic_by_industry'] = self._serialize_ic_by_group(ic_by_ind)
            except Exception as e:
                logger.warning(f"IC by industry failed: {e}")
                result['ic_by_industry'] = {}

            try:
                ret_by_ind, _ = mean_return_by_quantile(
                    fd, by_date=False, by_group=True, demeaned=False
                )
                result['returns_by_industry'] = self._serialize_returns_by_group(ret_by_ind)
            except Exception as e:
                logger.warning(f"Returns by industry failed: {e}")
                result['returns_by_industry'] = {}

            return result
        except Exception as e:
            logger.warning(f"Industry analysis failed: {e}")
            return {}

    def _prepare_charts_data(self, results: Dict) -> Dict:
        """预计算图表数据（ECharts 配置）"""
        charts = {}

        # IC 时序图
        if results.get('ic_ts'):
            ic_ts = results['ic_ts']
            charts['ic_timeseries'] = {
                'dates': [item['date'] for item in ic_ts],
                'series': {
                    key.replace('ic_', ''): [item.get(key) for item in ic_ts]
                    for key in ic_ts[0].keys() if key.startswith('ic_')
                }
            }

        # 分位数收益柱状图
        if results.get('quantile_returns'):
            qr = results['quantile_returns']
            periods = list(set(item['period'] for item in qr))
            charts['quantile_returns_bar'] = {
                period: {
                    'quantiles': [item['quantile'] for item in qr if item['period'] == period],
                    'returns': [item['mean_return'] for item in qr if item['period'] == period]
                }
                for period in periods
            }

        # 累计收益曲线（新格式：按 period 分组）
        if results.get('cumulative_returns'):
            cum_ret = results['cumulative_returns']
            if isinstance(cum_ret, dict):
                charts['cumulative_returns_line'] = {
                    period_key: {
                        'dates': [item['date'] for item in period_data],
                        'series': {
                            key.replace('quantile_', 'Q'): [item.get(key) for item in period_data]
                            for key in (period_data[0].keys() if period_data else [])
                            if key.startswith('quantile_')
                        }
                    }
                    for period_key, period_data in cum_ret.items()
                    if period_data
                }

        # Alpha/Beta
        if results.get('alpha_beta'):
            charts['alpha_beta'] = results['alpha_beta']

        # Factor cumulative returns (long/short portfolio)
        if results.get('factor_cumulative_returns'):
            charts['factor_cumulative_returns'] = results['factor_cumulative_returns']

        # Spread time series
        if results.get('spread_ts'):
            charts['spread_ts'] = results['spread_ts']

        # Monthly IC heatmap
        if results.get('ic_by_month'):
            charts['ic_by_month'] = results['ic_by_month']

        # Event study
        if results.get('event_study'):
            charts['event_study'] = results['event_study']

        return charts
