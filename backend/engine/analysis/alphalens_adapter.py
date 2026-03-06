"""
Alphalens 框架适配器
处理 Polars → Pandas 数据转换，调用 alphalens 核心函数，序列化结果
"""
import polars as pl
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import alphalens
from alphalens.utils import get_clean_factor_and_forward_returns
from alphalens.performance import (
    factor_information_coefficient,
    mean_return_by_quantile,
    quantile_turnover,
    factor_rank_autocorrelation,
)

from app.core.logger import logger


class AlphalensAdapter:
    """Alphalens 框架适配器，处理 Polars → Pandas 转换"""

    def __init__(self, db_client):
        self.db = db_client

    def prepare_factor_data(
        self,
        factor_df: pl.DataFrame,      # (ts_code, trade_date, factor_value)
        price_df: pl.DataFrame,        # (ts_code, trade_date, close)
        periods: List[int] = [1, 5, 10, 20],
        quantiles: int = 5,
        groupby_df: Optional[pl.DataFrame] = None  # (ts_code, trade_date, group_value)
    ) -> pd.DataFrame:
        """
        转换为 alphalens 所需的格式

        Args:
            factor_df: 因子数据
            price_df: 价格数据
            periods: 持仓周期列表
            quantiles: 分组数量
            groupby_df: 分组数据（可选），如行业、市值等

        Returns:
            pd.DataFrame with MultiIndex (date, asset) and columns:
            - factor: 因子值
            - 1D, 5D, 10D, 20D: 各周期远期收益
            - group (optional): 分组标签
        """
        logger.info(f"Preparing factor data: {len(factor_df)} rows, {factor_df['ts_code'].n_unique()} stocks")

        # 1. Polars → Pandas
        factor_pd = factor_df.to_pandas()
        price_pd = price_df.to_pandas()

        # 2. 转换日期格式并创建 MultiIndex
        factor_pd['trade_date'] = pd.to_datetime(factor_pd['trade_date'], format='%Y%m%d')
        factor_pd = factor_pd.set_index(['trade_date', 'ts_code'])['factor_value']
        factor_pd.index.names = ['date', 'asset']

        price_pd['trade_date'] = pd.to_datetime(price_pd['trade_date'], format='%Y%m%d')
        price_pd = price_pd.pivot(index='trade_date', columns='ts_code', values='close')
        price_pd.index.name = 'date'

        # 3. 处理分组数据（如果有）
        groupby_series = None
        if groupby_df is not None and not groupby_df.is_empty():
            groupby_series = self._prepare_groupby(groupby_df)
            logger.info(f"Groupby data prepared: {len(groupby_series)} entries")

        # 4. 调用 alphalens 工具函数
        logger.info(f"Calling alphalens get_clean_factor_and_forward_returns...")
        try:
            factor_data = get_clean_factor_and_forward_returns(
                factor=factor_pd,
                prices=price_pd,
                periods=periods,
                quantiles=quantiles,
                groupby=groupby_series,
                max_loss=0.5,  # 允许 50% 的数据缺失（测试环境）
                bins=None,
                binning_by_group=False
            )
            logger.info(f"Factor data prepared: {len(factor_data)} rows")
            return factor_data
        except Exception as e:
            logger.error(f"Failed to prepare factor data: {e}")
            raise

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

        # 2. 分层收益
        logger.info("Computing quantile returns...")
        try:
            mean_ret_by_q, std_ret_by_q = mean_return_by_quantile(
                factor_data, by_date=False, by_group=False, demeaned=False
            )
            results['quantile_returns'] = self._serialize_quantile_returns(mean_ret_by_q, std_ret_by_q)
        except Exception as e:
            logger.error(f"Failed to compute quantile returns: {e}")
            results['quantile_returns'] = []

        # 3. 累计收益（简化版，使用第一个周期）
        logger.info("Computing cumulative returns...")
        try:
            # 计算各分位数的累计收益
            results['cumulative_returns'] = self._compute_cumulative_returns(factor_data, periods[0])
        except Exception as e:
            logger.warning(f"Failed to compute cumulative returns: {e}")
            results['cumulative_returns'] = []

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

        # 5. 换手率 (为每个 period 计算)
        logger.info("Computing turnover...")
        try:
            # quantile_turnover 接受 factor_data 和 period，返回所有 quantile 的换手率
            turnover_data = {}
            for period in periods:
                try:
                    to = quantile_turnover(factor_data, period)
                    # to 是一个 DataFrame，columns 是 quantile，index 是日期
                    if isinstance(to, pd.DataFrame):
                        for col in to.columns:
                            quantile_key = f'quantile_{col}'
                            if quantile_key not in turnover_data:
                                turnover_data[quantile_key] = {}
                            turnover_data[quantile_key][f'period_{period}'] = {
                                str(date): float(value)
                                for date, value in to[col].items()
                                if not (np.isnan(value) or np.isinf(value))
                            }
                    elif isinstance(to, pd.Series):
                        # 如果返回 Series，说明只有一个 quantile
                        turnover_data['quantile_1'] = {
                            f'period_{period}': {
                                str(date): float(value)
                                for date, value in to.items()
                                if not (np.isnan(value) or np.isinf(value))
                            }
                        }
                except Exception as e:
                    logger.debug(f"Failed to compute turnover for period {period}: {e}")
            results['turnover'] = turnover_data
        except Exception as e:
            logger.warning(f"Failed to compute turnover: {e}")
            results['turnover'] = {}

        # 6. 衰减分析
        logger.info("Computing decay analysis...")
        try:
            autocorr = factor_rank_autocorrelation(factor_data)
            results['decay_analysis'] = self._serialize_decay(autocorr)
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
        """序列化 IC 汇总统计"""
        return {
            'ic_mean': float(ic.mean().mean()),
            'ic_std': float(ic.std().mean()),
            'ic_ir': float(ic.mean().mean() / (ic.std().mean() + 1e-10)),
            'ic_win_rate': float((ic > 0).mean().mean())
        }

    def _serialize_ic_by_period(self, ic: pd.DataFrame) -> List[Dict]:
        """序列化各周期 IC 统计"""
        return [
            {
                'period': str(col),
                'ic_mean': float(ic[col].mean()),
                'ic_std': float(ic[col].std()),
                'ic_ir': float(ic[col].mean() / (ic[col].std() + 1e-10)),
                'ic_win_rate': float((ic[col] > 0).mean())
            }
            for col in ic.columns
        ]

    def _serialize_ic_ts(self, ic: pd.DataFrame) -> List[Dict]:
        """序列化 IC 时间序列"""
        ic_reset = ic.reset_index()
        return [
            {
                'date': row['date'].strftime('%Y%m%d'),
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
                    'quantile': int(quantile) + 1,  # 1-based
                    'mean_return': mean_val,
                    'std_return': std_val,
                    'sharpe': mean_val / (std_val + 1e-10)
                })
        return results

    def _compute_cumulative_returns(self, factor_data: pd.DataFrame, period: int) -> List[Dict]:
        """计算累计收益曲线"""
        # 按日期和分位数分组，计算平均收益
        period_col = f'{period}D'
        if period_col not in factor_data.columns:
            return []

        # 按日期和分位数分组
        grouped = factor_data.groupby([factor_data.index.get_level_values('date'), 'factor_quantile'])[period_col].mean()

        # 转换为累计收益
        cumulative = {}
        for quantile in factor_data['factor_quantile'].unique():
            quantile_returns = grouped.xs(quantile, level=1)
            cumulative[f'quantile_{int(quantile)}'] = (1 + quantile_returns).cumprod()

        # 序列化
        dates = sorted(set(factor_data.index.get_level_values('date')))
        return [
            {
                'date': date.strftime('%Y%m%d'),
                **{key: float(series.get(date, np.nan)) if date in series.index else None
                   for key, series in cumulative.items()}
            }
            for date in dates
        ]

    def _serialize_ic_by_group(self, ic_by_group: pd.DataFrame) -> Dict:
        """序列化分组 IC"""
        return {
            str(group): {
                str(period): float(ic_by_group.loc[group, period])
                for period in ic_by_group.columns
            }
            for group in ic_by_group.index
        }

    def _serialize_returns_by_group(self, ret_by_group: pd.DataFrame) -> Dict:
        """序列化分组收益"""
        result = {}
        for group in ret_by_group.index.get_level_values(0).unique():
            group_data = ret_by_group.loc[group]
            result[str(group)] = [
                {
                    'period': str(period),
                    'quantile': int(quantile) + 1,
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

        # 累计收益曲线
        if results.get('cumulative_returns'):
            cum_ret = results['cumulative_returns']
            if cum_ret:
                charts['cumulative_returns_line'] = {
                    'dates': [item['date'] for item in cum_ret],
                    'series': {
                        key.replace('quantile_', 'Q'): [item.get(key) for item in cum_ret]
                        for key in cum_ret[0].keys() if key.startswith('quantile_')
                    }
                }

        return charts
