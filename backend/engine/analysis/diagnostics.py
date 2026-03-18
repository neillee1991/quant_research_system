"""
因子诊断模块
检测因子值的极端值、分布特征、IC 显著性，生成带警告的诊断报告。
"""
import numpy as np
import polars as pl
import pandas as pd
from typing import Dict, Any, Optional
from scipy import stats

from app.core.logger import logger


class FactorDiagnostics:
    """
    因子诊断器，在分析完成后生成诊断报告和警告。

    警告级别：
    - WARNING: 建议用户关注，可选择调整参数
    - INFO: 信息性提示
    """

    EXTREME_VALUE_THRESHOLD = 3.0
    EXTREME_VALUE_WARN_PCT = 0.01  # 1%
    IC_SIGNIFICANCE_THRESHOLD = 0.05
    MIN_SAMPLE_SIZE = 30

    @classmethod
    def diagnose(
        cls,
        factor_df: pl.DataFrame,
        factor_data: pd.DataFrame,
        ic_series: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        """
        生成完整诊断报告。

        Args:
            factor_df: 原始因子值 DataFrame (ts_code, trade_date, factor_value)
            factor_data: Alphalens 处理后的 MultiIndex DataFrame
            ic_series: IC 时序 DataFrame（可选）

        Returns:
            {
                "warnings": [...],
                "distribution": {...},
                "extreme_values": {...},
                "ic_significance": {...},
            }
        """
        report: Dict[str, Any] = {
            "warnings": [],
            "distribution": {},
            "extreme_values": {},
            "ic_significance": {},
        }

        factor_values = factor_df["factor_value"].drop_nulls().to_numpy()

        if len(factor_values) < cls.MIN_SAMPLE_SIZE:
            report["warnings"].append({
                "level": "WARNING",
                "type": "insufficient_data",
                "message": f"有效因子值仅 {len(factor_values)} 个，建议扩大时间范围",
                "action": None,
            })
            return report

        report["distribution"] = cls._calc_distribution(factor_values)

        extreme_result = cls._detect_extreme_values(factor_values)
        report["extreme_values"] = extreme_result
        if extreme_result["extreme_pct"] > cls.EXTREME_VALUE_WARN_PCT * 100:
            report["warnings"].append({
                "level": "WARNING",
                "type": "extreme_values",
                "message": (
                    f"因子值存在 {extreme_result['extreme_pct']:.1f}% 的极端值"
                    f"（±{cls.EXTREME_VALUE_THRESHOLD}σ），可能影响 IC 计算"
                ),
                "action": "winsorize",
                "action_label": "Winsorize 处理（截尾至 1%-99% 分位数）",
                "params": {"method": "winsorize", "lower": 0.01, "upper": 0.99},
            })

        if len(factor_values) <= 5000:
            _, p_value = stats.shapiro(factor_values[:5000])
            if p_value < 0.05:
                report["warnings"].append({
                    "level": "INFO",
                    "type": "non_normal",
                    "message": f"因子值分布非正态（Shapiro-Wilk p={p_value:.4f}），建议使用 Rank IC",
                    "action": None,
                })

        if ic_series is not None:
            report["ic_significance"] = cls._calc_ic_significance(ic_series)

        return report

    @classmethod
    def _calc_distribution(cls, values: np.ndarray) -> Dict[str, Any]:
        return {
            "count": int(len(values)),
            "mean": round(float(np.mean(values)), 6),
            "std": round(float(np.std(values)), 6),
            "skew": round(float(stats.skew(values)), 4),
            "kurtosis": round(float(stats.kurtosis(values)), 4),
            "min": round(float(np.min(values)), 6),
            "p1": round(float(np.percentile(values, 1)), 6),
            "p25": round(float(np.percentile(values, 25)), 6),
            "median": round(float(np.median(values)), 6),
            "p75": round(float(np.percentile(values, 75)), 6),
            "p99": round(float(np.percentile(values, 99)), 6),
            "max": round(float(np.max(values)), 6),
        }

    @classmethod
    def _detect_extreme_values(cls, values: np.ndarray) -> Dict[str, Any]:
        mean = np.mean(values)
        std = np.std(values)
        lower = mean - cls.EXTREME_VALUE_THRESHOLD * std
        upper = mean + cls.EXTREME_VALUE_THRESHOLD * std
        extreme_mask = (values < lower) | (values > upper)
        extreme_count = int(np.sum(extreme_mask))
        return {
            "extreme_count": extreme_count,
            "extreme_pct": round(extreme_count / len(values) * 100, 2),
            "lower_bound": round(float(lower), 6),
            "upper_bound": round(float(upper), 6),
        }

    @classmethod
    def _calc_ic_significance(cls, ic_df: pd.DataFrame) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for col in ic_df.columns:
            ic_vals = ic_df[col].dropna().values
            if len(ic_vals) < cls.MIN_SAMPLE_SIZE:
                continue
            t_stat, p_value = stats.ttest_1samp(ic_vals, 0)
            result[col] = {
                "mean_ic": round(float(np.mean(ic_vals)), 4),
                "t_stat": round(float(t_stat), 4),
                "p_value": round(float(p_value), 4),
                "significant": bool(p_value < cls.IC_SIGNIFICANCE_THRESHOLD),
            }
        return result
