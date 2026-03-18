# Factor Analysis P1: Distribution + Winsorize + Long-Short NAV + IC Significance

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 新增因子分布诊断（含极端值警告）、winsorize 支持、多空净值曲线、Pearson/Rank IC 双输出、IC 统计显著性分析，并在 API 返回中包含结构化 warnings 供前端交互式展示。

**Architecture:** 新建 `FactorDiagnostics` 类（`diagnostics.py`）承载所有诊断逻辑；在 `AlphalensAdapter.run_full_analysis()` 中新增多空净值和双 IC 计算；`AnalysisRequest` 新增 `winsorize` 相关参数；API 返回统一包含 `warnings` 数组。

**Tech Stack:** Polars, Pandas, NumPy, SciPy (t-test), Alphalens, FastAPI

**依赖:** P0 计划已完成（`pipeline_stats` 已在 `diagnostics` 字段中）

---

## Task 1: 新建 FactorDiagnostics — 因子分布分析

**Files:**
- Create: `backend/engine/analysis/diagnostics.py`

**Step 1: 创建文件，实现 `distribution()` 方法**

```python
"""
因子诊断分析模块
包含：因子分布、行业暴露、市值暴露、IC 显著性
"""
import polars as pl
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
from scipy import stats
from app.core.logger import logger


class FactorDiagnostics:
    """因子诊断分析，所有方法接受 Polars DataFrame，返回可序列化的 dict"""

    @staticmethod
    def distribution(factor_df: pl.DataFrame, factor_col: str = "factor_value") -> Dict[str, Any]:
        """
        计算因子分布统计（基于全量数据，非截面均值）。

        Args:
            factor_df: 含 factor_col 列的 DataFrame
            factor_col: 因子值列名

        Returns:
            {mean, std, skewness, kurtosis, extreme_ratio, extreme_ratio_warning, percentiles}
        """
        vals = factor_df[factor_col].drop_nulls()

        if len(vals) == 0:
            return {"error": "no_valid_values"}

        arr = vals.to_numpy()
        mean_val = float(np.mean(arr))
        std_val = float(np.std(arr))

        # 极端值：|z-score| > 3
        if std_val > 0:
            z_scores = np.abs((arr - mean_val) / std_val)
            extreme_ratio = float(np.mean(z_scores > 3))
        else:
            extreme_ratio = 0.0

        # 偏度和峰度
        skewness = float(stats.skew(arr))
        kurtosis = float(stats.kurtosis(arr))  # excess kurtosis

        # 分位数
        percentiles = {
            "p1":  float(np.percentile(arr, 1)),
            "p5":  float(np.percentile(arr, 5)),
            "p25": float(np.percentile(arr, 25)),
            "p50": float(np.percentile(arr, 50)),
            "p75": float(np.percentile(arr, 75)),
            "p95": float(np.percentile(arr, 95)),
            "p99": float(np.percentile(arr, 99)),
        }

        return {
            "mean": round(mean_val, 6),
            "std": round(std_val, 6),
            "skewness": round(skewness, 4),
            "kurtosis": round(kurtosis, 4),
            "extreme_ratio": round(extreme_ratio, 4),
            "extreme_ratio_warning": extreme_ratio > 0.05,
            "zero_variance": std_val < 1e-10,
            "percentiles": {k: round(v, 6) for k, v in percentiles.items()},
            "sample_size": len(arr),
        }

    @staticmethod
    def ic_significance(ic_series: List[float]) -> Dict[str, Any]:
        """
        对 IC 时序做单样本 t-test（H₀: IC_mean = 0）。

        Args:
            ic_series: IC 值列表（已去除 NaN）

        Returns:
            {t_stat, p_value, significant, sample_size, insufficient_warning}
        """
        arr = np.array([v for v in ic_series if v is not None and not np.isnan(v)])
        n = len(arr)

        if n < 3:
            return {
                "t_stat": None, "p_value": None,
                "significant": False, "sample_size": n,
                "insufficient_warning": True,
            }

        t_stat, p_value = stats.ttest_1samp(arr, popmean=0)

        return {
            "t_stat": round(float(t_stat), 4),
            "p_value": round(float(p_value), 6),
            "significant": float(p_value) < 0.05,
            "sample_size": n,
            "insufficient_warning": n < 20,
        }

    @staticmethod
    def build_warnings(
        distribution_result: Dict,
        ic_significance_result: Dict,
        ic_divergence: float,
        size_corr_mean: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        根据诊断结果生成结构化 warnings 列表。

        Returns:
            List of {key, severity, message, suggested_params}
        """
        warnings = []

        # 极端值警告
        if distribution_result.get("extreme_ratio_warning"):
            ratio = distribution_result["extreme_ratio"]
            warnings.append({
                "key": "extreme_values",
                "severity": "high",
                "message": f"因子值中有 {ratio*100:.1f}% 的极端值（|z-score| > 3），建议进行 winsorize 处理",
                "suggested_params": {
                    "winsorize": True,
                    "winsorize_lower": 0.01,
                    "winsorize_upper": 0.99,
                },
            })

        # 零方差警告
        if distribution_result.get("zero_variance"):
            warnings.append({
                "key": "zero_variance",
                "severity": "critical",
                "message": "因子值方差为零，无法进行有效分析",
                "suggested_params": {},
            })

        # IC 不显著
        if ic_significance_result.get("p_value") is not None:
            if not ic_significance_result["significant"]:
                warnings.append({
                    "key": "ic_not_significant",
                    "severity": "medium",
                    "message": f"IC 统计上不显著（p={ic_significance_result['p_value']:.3f}），因子预测能力可能较弱",
                    "suggested_params": {},
                })

        # IC 样本不足
        if ic_significance_result.get("insufficient_warning"):
            warnings.append({
                "key": "ic_insufficient_samples",
                "severity": "medium",
                "message": f"IC 序列样本数不足（{ic_significance_result['sample_size']} 个交易日），统计结论不可靠",
                "suggested_params": {},
            })

        # Pearson vs Rank IC 差异
        if abs(ic_divergence) > 0.3:
            warnings.append({
                "key": "ic_divergence",
                "severity": "low",
                "message": f"Pearson IC 与 Rank IC 差异较大（{ic_divergence:.3f}），因子分布可能非正态，建议以 Rank IC 为主",
                "suggested_params": {},
            })

        # 市值暴露
        if size_corr_mean is not None and abs(size_corr_mean) > 0.3:
            warnings.append({
                "key": "size_bias",
                "severity": "medium",
                "message": f"因子与市值相关性较高（均值={size_corr_mean:.3f}），建议做市值中性化",
                "suggested_params": {"neutralize_size": True},
            })

        return warnings
```

**Step 2: 验证语法**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.diagnostics import FactorDiagnostics; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/diagnostics.py
git commit -m "feat: 新增 FactorDiagnostics，包含分布分析、IC显著性、warnings 生成"
```

---

## Task 2: 新增 winsorize 支持

**Files:**
- Modify: `backend/engine/analysis/analyzer.py` — `_analyze_with_alphalens()` 方法
- Modify: `backend/app/api/v1/production/factor_analysis.py` — `AlphalensAnalysisRequest`

**Step 1: 在 `AlphalensAnalysisRequest` 中新增 winsorize 参数**

```python
class AlphalensAnalysisRequest(BaseModel):
    ...
    # winsorize（诊断后用户可调整）
    winsorize: bool = False
    winsorize_lower: float = 0.01
    winsorize_upper: float = 0.99
```

**Step 2: 在 `analyzer.py` 的 `analyze()` 和 `_analyze_with_alphalens()` 签名中新增参数**

```python
def analyze(self, ..., winsorize: bool = False,
            winsorize_lower: float = 0.01, winsorize_upper: float = 0.99, ...):
```

**Step 3: 在 `_analyze_with_alphalens()` 中，股票池过滤后、ForwardReturnCalculator 前，插入 winsorize 步骤**

```python
# winsorize（可选）
if winsorize:
    lower_q = factor_df["factor_value"].quantile(winsorize_lower)
    upper_q = factor_df["factor_value"].quantile(winsorize_upper)
    factor_df = factor_df.with_columns(
        pl.col("factor_value").clip(lower_q, upper_q)
    )
    prev = count
    count = len(factor_df)
    _record_step("winsorize", factor_df, prev)
    logger.info(f"Winsorized factor_value to [{lower_q:.4f}, {upper_q:.4f}]")
```

**Step 4: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.analyzer import FactorAnalyzer; print('OK')"
```

Expected: `OK`

**Step 5: Commit**

```bash
git add backend/engine/analysis/analyzer.py \
        backend/app/api/v1/production/factor_analysis.py
git commit -m "feat: 新增 winsorize 支持，pipeline_stats 记录 winsorize 步骤"
```

---

## Task 3: 双 IC 输出（Pearson + Rank IC）

**Files:**
- Modify: `backend/engine/analysis/alphalens_adapter.py` — `run_full_analysis()` 和序列化方法

**Step 1: 在 `run_full_analysis()` 中同时计算 Pearson IC 和 Rank IC**

Alphalens 的 `factor_information_coefficient` 默认计算 Spearman（Rank IC）。要同时得到 Pearson IC，需传 `method='pearson'`：

```python
# Rank IC（Spearman，默认）
rank_ic = factor_information_coefficient(factor_data, method='spearman')
# Pearson IC
pearson_ic = factor_information_coefficient(factor_data, method='pearson')

results['rank_ic_summary'] = self._serialize_ic_summary(rank_ic)
results['pearson_ic_summary'] = self._serialize_ic_summary(pearson_ic)
results['rank_ic_by_period'] = self._serialize_ic_by_period(rank_ic)
results['pearson_ic_by_period'] = self._serialize_ic_by_period(pearson_ic)
results['rank_ic_ts'] = self._serialize_ic_ts(rank_ic)
results['pearson_ic_ts'] = self._serialize_ic_ts(pearson_ic)

# 保留原有 ic_summary/ic_by_period/ic_ts 字段（指向 rank_ic，向后兼容）
results['ic_summary'] = results['rank_ic_summary']
results['ic_by_period'] = results['rank_ic_by_period']
results['ic_ts'] = results['rank_ic_ts']

# 计算 IC 差异（用于 divergence warning）
ic_divergence = float(
    abs(rank_ic.mean().mean() - pearson_ic.mean().mean())
)
results['ic_divergence'] = ic_divergence
```

**Step 2: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.alphalens_adapter import AlphalensAdapter; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/alphalens_adapter.py
git commit -m "feat: IC 分析同时输出 Pearson IC 和 Rank IC"
```

---

## Task 4: 多空净值曲线

**Files:**
- Modify: `backend/engine/analysis/alphalens_adapter.py` — `_compute_cumulative_returns()` 方法

**Step 1: 在 `_compute_cumulative_returns()` 中新增多空组合**

在现有各分位数累计收益计算后，新增：

```python
# 多空组合：最高分位 - 最低分位
quantile_nums = sorted(factor_data['factor_quantile'].unique())
if len(quantile_nums) >= 2:
    q_max = quantile_nums[-1]
    q_min = quantile_nums[0]

    long_returns = grouped.xs(q_max, level=1) if q_max in grouped.index.get_level_values(1) else pd.Series(dtype=float)
    short_returns = grouped.xs(q_min, level=1) if q_min in grouped.index.get_level_values(1) else pd.Series(dtype=float)

    # 对齐日期
    ls_returns = long_returns.subtract(short_returns, fill_value=0)
    ls_nav = (1 + ls_returns).cumprod()

    # 计算 Sharpe 和最大回撤
    ls_arr = ls_returns.dropna().values
    sharpe = float(np.mean(ls_arr) / (np.std(ls_arr) + 1e-10) * np.sqrt(252)) if len(ls_arr) > 1 else 0.0

    nav_arr = ls_nav.values
    peak = np.maximum.accumulate(nav_arr)
    drawdown = (nav_arr - peak) / (peak + 1e-10)
    max_drawdown = float(np.min(drawdown))

    # 异常日期（单日涨跌幅 > 20%）
    anomaly_dates = [
        date.strftime('%Y%m%d')
        for date, ret in ls_returns.items()
        if abs(ret) > 0.20
    ]

    cumulative['long_short'] = ls_nav
    long_short_meta = {
        "sharpe": round(sharpe, 4),
        "max_drawdown": round(max_drawdown, 4),
        "anomaly_dates": anomaly_dates,
    }
else:
    long_short_meta = {}
```

在序列化输出中，将 `long_short` 单独提取：

```python
result = [
    {
        'date': date.strftime('%Y%m%d'),
        **{key: float(series.get(date, np.nan)) if date in series.index else None
           for key, series in cumulative.items()}
    }
    for date in dates
]

return {
    "nav_series": result,
    "long_short_meta": long_short_meta,
}
```

注意：`_compute_cumulative_returns` 的返回类型从 `List[Dict]` 变为 `Dict`，需要同步更新 `run_full_analysis()` 中的赋值：

```python
cum_ret = self._compute_cumulative_returns(factor_data, periods[0])
results['cumulative_returns'] = cum_ret.get('nav_series', [])
results['long_short'] = cum_ret.get('long_short_meta', {})
```

**Step 2: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.alphalens_adapter import AlphalensAdapter; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/alphalens_adapter.py
git commit -m "feat: 新增多空净值曲线，含 Sharpe、最大回撤、异常日期"
```

---

## Task 5: 将诊断分析接入 analyzer.py

**Files:**
- Modify: `backend/engine/analysis/analyzer.py` — `_analyze_with_alphalens()` 末尾

**Step 1: 在 `_analyze_with_alphalens()` 中调用 FactorDiagnostics**

在 `run_full_analysis()` 调用之后，`_save_alphalens_analysis()` 之前，插入：

```python
from engine.analysis.diagnostics import FactorDiagnostics

# 因子分布诊断（用原始/winsorize 后的 factor_df）
dist_result = FactorDiagnostics.distribution(factor_df)

# IC 显著性（用 rank_ic_ts 的第一个 period）
rank_ic_ts = results.get('rank_ic_ts', [])
if rank_ic_ts:
    first_period_key = f"ic_{results.get('rank_ic_by_period', [{}])[0].get('period', '1D')}"
    ic_vals = [row.get(first_period_key) for row in rank_ic_ts if row.get(first_period_key) is not None]
else:
    ic_vals = []
ic_sig = FactorDiagnostics.ic_significance(ic_vals)

# IC 差异
ic_divergence = results.get('ic_divergence', 0.0)

# 生成 warnings
warnings = FactorDiagnostics.build_warnings(
    distribution_result=dist_result,
    ic_significance_result=ic_sig,
    ic_divergence=ic_divergence,
)

# 合并进 results
results['diagnostics'] = {
    **results.get('diagnostics', {}),  # 保留 pipeline_stats
    'distribution': dist_result,
    'ic_significance': ic_sig,
}
results['warnings'] = warnings
```

**Step 2: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "
from engine.analysis.analyzer import FactorAnalyzer
import inspect
src = inspect.getsource(FactorAnalyzer._analyze_with_alphalens)
assert 'FactorDiagnostics' in src
assert 'warnings' in src
print('diagnostics integration OK')
"
```

Expected: `diagnostics integration OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/analyzer.py
git commit -m "feat: 将 FactorDiagnostics 接入分析流程，输出 warnings"
```

---

## Task 6: 前端展示 warnings 高亮卡片 + winsorize 交互

**Files:**
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`
- Modify: `frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts`
- Modify: `frontend/src/api/index.ts`

**Step 1: 更新 `api/index.ts`，新增 winsorize 参数**

```typescript
runAlphalensAnalysis: (data: {
  ...
  winsorize?: boolean;
  winsorize_lower?: number;
  winsorize_upper?: number;
}) => longRunningApi.post('/analysis/alphalens', data),
```

**Step 2: 在 `useFactorAnalysis.ts` 中新增 winsorize 状态**

```typescript
const [winsorize, setWinsorize] = useState(false);
const [winsorizeRange, setWinsorizeRange] = useState<[number, number]>([0.01, 0.99]);
```

在 `runAnalysis` 中透传：

```typescript
winsorize,
winsorize_lower: winsorizeRange[0],
winsorize_upper: winsorizeRange[1],
```

在 return 中新增这些状态。

**Step 3: 在 `AnalysisPanel.tsx` 中新增 warnings 展示区域**

在分析结果顶部，新增 warnings 高亮卡片区域：

```tsx
{analysisResult?.warnings?.length > 0 && (
  <div style={{ marginBottom: 16 }}>
    {analysisResult.warnings.map((w: any) => (
      <Banner
        key={w.key}
        type={w.severity === 'critical' ? 'danger' : w.severity === 'high' ? 'warning' : 'info'}
        description={
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>{w.message}</span>
            {w.key === 'extreme_values' && (
              <Button
                size="small"
                onClick={() => {
                  setWinsorize(true);
                  Toast.info('已启用 winsorize，点击重新分析');
                }}
              >
                启用 Winsorize 并重跑
              </Button>
            )}
            {w.key === 'size_bias' && (
              <Button size="small" onClick={() => Toast.info('请在中性化选项中启用市值中性化')}>
                查看中性化选项
              </Button>
            )}
          </div>
        }
        style={{ marginBottom: 8 }}
      />
    ))}
  </div>
)}
```

**Step 4: 新增双 IC 图表**

在现有 IC 图表旁边，新增 Pearson IC vs Rank IC 对比图：

```tsx
// 在 getICChartOption 中，同时展示 rank_ic 和 pearson_ic
const icByPeriod = analysisResult?.rank_ic_by_period || analysisResult?.ic_by_period;
const pearsonByPeriod = analysisResult?.pearson_ic_by_period;
// 如果两者都有，在同一图表中用不同颜色展示
```

**Step 5: 验证前端编译**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
npm run build 2>&1 | grep -E "ERROR|error TS" | head -20
```

Expected: 无输出

**Step 6: Commit**

```bash
git add frontend/src/pages/FactorCenter/AnalysisPanel.tsx \
        frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts \
        frontend/src/api/index.ts
git commit -m "feat: 前端展示 warnings 高亮卡片，支持 winsorize 交互式重跑"
```

---

## 注意事项

1. **`factor_information_coefficient` 的 `method` 参数**：确认当前安装的 alphalens 版本是否支持 `method='pearson'`。如果不支持，手动用 `scipy.stats.pearsonr` 对每个截面日计算，汇总为时序。

2. **`_compute_cumulative_returns` 返回类型变更**：从 `List[Dict]` 变为 `Dict`，需要检查 `_prepare_charts_data()` 中对 `cumulative_returns` 的引用，同步更新为 `results.get('cumulative_returns', [])`。

3. **Banner 组件**：Semi UI 的 `Banner` 组件需要从 `@douyinfe/semi-ui` 导入，确认项目中已有此组件。
