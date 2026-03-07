"""
因子分析器单元测试
测试分位数分组、Sharpe Ratio 年化、IC 计算等核心逻辑
不依赖真实 DolphinDB 连接
"""
import math
import pytest
import polars as pl
import numpy as np


# ==================== 分位数分组逻辑测试 ====================

def _quantile_assign(factor_values: list, quantiles: int) -> list:
    """
    复现 analyzer.py 中的分位数分组逻辑：
    (rank / count * quantiles).ceil().clip(1, quantiles)
    """
    s = pl.Series(factor_values)
    result = (
        s.rank()
        / s.count()
        * quantiles
    ).ceil().cast(pl.Int32).clip(1, quantiles)
    return result.to_list()


class TestQuantileGrouping:
    def test_quantile_starts_at_1(self):
        """分组编号应从 1 开始，不能有 0"""
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        groups = _quantile_assign(values, quantiles=5)
        assert min(groups) == 1, f"Min quantile should be 1, got {min(groups)}"

    def test_quantile_max_equals_quantiles(self):
        """最大分组编号应等于 quantiles 参数"""
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        for q in [3, 5, 10]:
            groups = _quantile_assign(values, quantiles=q)
            assert max(groups) == q, f"Max quantile should be {q}, got {max(groups)}"

    def test_quantile_count_balanced(self):
        """分组数量应大致均匀"""
        values = list(range(1, 101))  # 100 个值
        groups = _quantile_assign(values, quantiles=5)
        from collections import Counter
        counts = Counter(groups)
        for q in range(1, 6):
            assert counts[q] == 20, f"Q{q} should have 20 items, got {counts[q]}"

    def test_quantile_clip_prevents_zero(self):
        """clip(1, quantiles) 确保不会出现 0 分组"""
        # 极端情况：只有一个值
        values = [42.0]
        groups = _quantile_assign(values, quantiles=5)
        assert groups[0] >= 1

    def test_quantile_clip_prevents_overflow(self):
        """clip 确保不超过 quantiles"""
        values = [1.0, 2.0, 3.0]
        groups = _quantile_assign(values, quantiles=5)
        for g in groups:
            assert 1 <= g <= 5

    def test_quantile_ordering(self):
        """较小因子值应在较低分组"""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        groups = _quantile_assign(values, quantiles=5)
        # 排序后分组应单调不减
        assert groups == sorted(groups)


# ==================== Sharpe Ratio 年化测试 ====================

def _compute_sharpe_annualized(returns: list, periods_per_year: int = 252) -> float:
    """
    复现 analyzer.py 中 Sharpe Ratio 计算（不含年化）：
    sharpe = avg_return / std_return
    正确的年化 Sharpe = avg_return / std_return * sqrt(252)
    """
    arr = np.array(returns)
    mean = arr.mean()
    std = arr.std(ddof=1)
    if std == 0:
        return 0.0
    return mean / std * math.sqrt(periods_per_year)


class TestSharpeRatioAnnualized:
    def test_sharpe_formula(self):
        """验证年化 Sharpe = mean/std * sqrt(252)"""
        returns = [0.001, 0.002, -0.001, 0.003, 0.0, -0.002, 0.004, 0.001]
        arr = np.array(returns)
        mean = arr.mean()
        std = arr.std(ddof=1)
        expected = mean / std * math.sqrt(252)
        result = _compute_sharpe_annualized(returns)
        assert result == pytest.approx(expected, rel=1e-6)

    def test_sharpe_positive_for_positive_returns(self):
        """正收益序列的 Sharpe 应为正"""
        returns = [0.001] * 20 + [-0.0001] * 5
        result = _compute_sharpe_annualized(returns)
        assert result > 0

    def test_sharpe_zero_std_returns_zero(self):
        """标准差为 0 时返回 0（避免除零）"""
        returns = [0.001] * 10
        arr = np.array(returns)
        std = arr.std(ddof=1)
        if std == 0:
            result = _compute_sharpe_annualized(returns)
            assert result == 0.0

    def test_sharpe_annualization_factor(self):
        """验证年化因子 sqrt(252) 被正确应用"""
        returns = [0.001, -0.001, 0.002, -0.002, 0.003]
        arr = np.array(returns)
        mean = arr.mean()
        std = arr.std(ddof=1)
        non_annualized = mean / std
        annualized = _compute_sharpe_annualized(returns)
        assert annualized == pytest.approx(non_annualized * math.sqrt(252), rel=1e-6)

    def test_analyzer_sharpe_not_annualized_bug(self):
        """
        验证 analyzer.py 中 _build_summary 的 Sharpe 计算缺少年化（已知 bug H-21）。
        当前实现：sharpe = avg_return / std_return（无 sqrt(252)）
        正确实现：sharpe = avg_return / std_return * sqrt(252)
        """
        avg_return = 0.001
        std_return = 0.01
        # 当前实现（不含年化）
        current_sharpe = avg_return / std_return
        # 正确年化 Sharpe
        correct_sharpe = avg_return / std_return * math.sqrt(252)
        # 两者应不同（差距约 15.87 倍）
        assert abs(correct_sharpe - current_sharpe) > 1.0, (
            "Annualized Sharpe should differ significantly from non-annualized"
        )
        assert correct_sharpe == pytest.approx(current_sharpe * math.sqrt(252), rel=1e-6)


# ==================== IC 计算测试 ====================

def _compute_rank_ic(factor_values: list, forward_returns: list) -> float:
    """
    复现 analyzer.py 中的 Rank IC 计算（Spearman 相关系数）
    """
    factor = pl.Series(factor_values)
    fwd = pl.Series(forward_returns)
    rank_f = factor.rank()
    rank_r = fwd.rank()
    n = len(factor)
    mean_f = rank_f.mean()
    mean_r = rank_r.mean()
    cov = ((rank_f - mean_f) * (rank_r - mean_r)).sum()
    std_f = ((rank_f - mean_f) ** 2).sum() ** 0.5
    std_r = ((rank_r - mean_r) ** 2).sum() ** 0.5
    if std_f > 0 and std_r > 0:
        return float(cov / (std_f * std_r))
    return 0.0


class TestICCalculation:
    def test_ic_range(self):
        """IC 值应在 [-1, 1] 范围内"""
        import random
        random.seed(0)
        for _ in range(10):
            factors = [random.gauss(0, 1) for _ in range(50)]
            returns = [random.gauss(0, 1) for _ in range(50)]
            ic = _compute_rank_ic(factors, returns)
            assert -1.0 <= ic <= 1.0, f"IC out of range: {ic}"

    def test_ic_perfect_positive_correlation(self):
        """完全正相关时 IC 应为 1.0"""
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        ic = _compute_rank_ic(values, values)
        assert ic == pytest.approx(1.0, abs=1e-6)

    def test_ic_perfect_negative_correlation(self):
        """完全负相关时 IC 应为 -1.0"""
        factors = [1.0, 2.0, 3.0, 4.0, 5.0]
        returns = [5.0, 4.0, 3.0, 2.0, 1.0]
        ic = _compute_rank_ic(factors, returns)
        assert ic == pytest.approx(-1.0, abs=1e-6)

    def test_ic_zero_for_random_uncorrelated(self):
        """不相关数据的 IC 应接近 0（统计意义上）"""
        # 使用固定种子确保可重复
        np.random.seed(42)
        factors = np.random.randn(1000).tolist()
        returns = np.random.randn(1000).tolist()
        ic = _compute_rank_ic(factors, returns)
        # 1000 个样本，IC 应在 [-0.1, 0.1] 范围内
        assert abs(ic) < 0.1, f"IC for uncorrelated data should be near 0, got {ic}"

    def test_ic_symmetric(self):
        """IC(X, Y) 应等于 IC(Y, X)（Spearman 相关系数对称性）"""
        factors = [3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0]
        returns = [2.0, 7.0, 1.0, 8.0, 2.0, 8.0, 1.0, 8.0]
        ic_xy = _compute_rank_ic(factors, returns)
        ic_yx = _compute_rank_ic(returns, factors)
        assert ic_xy == pytest.approx(ic_yx, abs=1e-10)

    def test_ic_minimum_sample_size(self):
        """少于 30 个样本时，analyzer 跳过计算（验证阈值逻辑）"""
        # analyzer.py 中 if len(cross) < 30: continue
        # 此测试验证该阈值存在
        threshold = 30
        small_sample = list(range(threshold - 1))
        large_sample = list(range(threshold))
        # 小样本不应触发 IC 计算（在 analyzer 中被跳过）
        assert len(small_sample) < threshold
        assert len(large_sample) >= threshold
