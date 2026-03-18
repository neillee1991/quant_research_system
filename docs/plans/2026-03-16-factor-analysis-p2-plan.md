# Factor Analysis P2: Neutralizer + Industry/Size Exposure + IC Decay

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现通用因子中性化（市场/行业/市值三因素 OLS 回归），新增行业暴露和市值暴露诊断，实现基于 periods 的 IC 衰减分析。

**Architecture:** 新建 `Neutralizer` 类（`neutralizer.py`），每截面日做多元 OLS；`FactorDiagnostics` 新增 `industry_exposure`、`size_exposure`、`ic_decay` 方法；`AnalysisRequest` 新增中性化参数；数据全部走 `DataConfigLoader.load_field_data()`。

**Tech Stack:** Polars, NumPy (lstsq), Pandas, FastAPI

**依赖:** P0、P1 计划已完成

---

## Task 1: 新建 Neutralizer

**Files:**
- Create: `backend/engine/analysis/neutralizer.py`

**Step 1: 创建文件**

```python
"""
通用因子中性化模块
每截面日做多元 OLS 回归，剔除市场/行业/市值暴露，返回残差作为中性化因子值。
"""
import polars as pl
import numpy as np
from typing import Optional, List, Dict, Any
from app.core.logger import logger


class Neutralizer:
    """
    通用因子中性化。

    回归模型（每截面日）：
        factor = β₀ + β₁·mkt_return + β₂·log(mkt_cap) + Σβₖ·industry_dummy_k + ε

    用户可自由组合三个因素，残差 ε 作为中性化后的因子值。
    """

    @staticmethod
    def neutralize(
        factor_df: pl.DataFrame,
        market_df: Optional[pl.DataFrame] = None,    # (trade_date, mkt_return)
        industry_df: Optional[pl.DataFrame] = None,  # (ts_code, trade_date, industry_l1_value)
        size_df: Optional[pl.DataFrame] = None,      # (ts_code, trade_date, market_cap_value)
    ) -> Dict[str, Any]:
        """
        对因子值做截面中性化。

        Args:
            factor_df: columns=[ts_code, trade_date, factor_value]
            market_df: 市场基准收益，columns=[trade_date, benchmark_return_value]
            industry_df: 行业历史快照，columns=[ts_code, trade_date, industry_l1_value]
            size_df: 市值数据，columns=[ts_code, trade_date, market_cap_value]

        Returns:
            {
                "factor_df": pl.DataFrame,  # factor_value 列替换为残差
                "r2_by_date": List[float],
                "r2_mean": float,
                "r2_warning": bool,
                "skipped_dates": List[str],
                "merged_industries": Dict[str, List[str]],
            }
        """
        if market_df is None and industry_df is None and size_df is None:
            logger.warning("Neutralizer called with no factors, returning original")
            return {
                "factor_df": factor_df,
                "r2_by_date": [], "r2_mean": 0.0,
                "r2_warning": False, "skipped_dates": [], "merged_industries": {},
            }

        dates = factor_df["trade_date"].unique().sort().to_list()
        result_rows = []
        r2_list = []
        skipped_dates = []
        merged_industries: Dict[str, List[str]] = {}

        for date in dates:
            day_factor = factor_df.filter(pl.col("trade_date") == date)
            ts_codes = day_factor["ts_code"].to_list()
            y = day_factor["factor_value"].to_numpy()

            # 跳过全 null 截面
            if np.all(np.isnan(y.astype(float))):
                skipped_dates.append(date)
                result_rows.extend(day_factor.to_dicts())
                continue

            X_cols = []

            # 市场因子
            if market_df is not None:
                day_mkt = market_df.filter(pl.col("trade_date") == date)
                if day_mkt.is_empty():
                    skipped_dates.append(date)
                    result_rows.extend(day_factor.to_dicts())
                    continue
                mkt_val = float(day_mkt["benchmark_return_value"][0])
                X_cols.append(np.full(len(ts_codes), mkt_val))

            # 市值因子
            if size_df is not None:
                day_size = size_df.filter(pl.col("trade_date") == date)
                size_map = dict(zip(day_size["ts_code"].to_list(),
                                    day_size["market_cap_value"].to_list()))
                size_vals = np.array([size_map.get(c, np.nan) for c in ts_codes], dtype=float)
                # log 变换，过滤 <= 0
                with np.errstate(invalid='ignore'):
                    log_size = np.where(size_vals > 0, np.log(size_vals), np.nan)
                X_cols.append(log_size)

            # 行业哑变量
            industry_dummies = None
            if industry_df is not None:
                day_ind = industry_df.filter(pl.col("trade_date") == date)
                ind_map = dict(zip(day_ind["ts_code"].to_list(),
                                   day_ind["industry_l1_value"].to_list()))
                industries = [ind_map.get(c, "OTHER") for c in ts_codes]

                # 合并只有 1 只股票的行业到 OTHER
                from collections import Counter
                ind_counts = Counter(industries)
                for ind, cnt in ind_counts.items():
                    if cnt == 1 and ind != "OTHER":
                        if ind not in merged_industries:
                            merged_industries[ind] = []
                        merged_industries[ind].append(date)
                        industries = ["OTHER" if x == ind else x for x in industries]

                unique_inds = sorted(set(industries))
                if len(unique_inds) > 1:
                    # 去掉第一个行业（避免完全共线性）
                    for ind in unique_inds[1:]:
                        dummy = np.array([1.0 if x == ind else 0.0 for x in industries])
                        X_cols.append(dummy)

            if not X_cols:
                skipped_dates.append(date)
                result_rows.extend(day_factor.to_dicts())
                continue

            # 构建设计矩阵 [截距, X1, X2, ...]
            X = np.column_stack([np.ones(len(ts_codes))] + X_cols)

            # 过滤含 NaN 的行
            valid_mask = ~np.any(np.isnan(X), axis=1) & ~np.isnan(y.astype(float))
            if valid_mask.sum() < 5:
                skipped_dates.append(date)
                result_rows.extend(day_factor.to_dicts())
                continue

            X_valid = X[valid_mask]
            y_valid = y[valid_mask].astype(float)

            # OLS via lstsq（自动处理共线性）
            coeffs, _, _, _ = np.linalg.lstsq(X_valid, y_valid, rcond=None)
            residuals_valid = y_valid - X_valid @ coeffs

            # R²
            ss_res = np.sum(residuals_valid ** 2)
            ss_tot = np.sum((y_valid - np.mean(y_valid)) ** 2)
            r2 = 1 - ss_res / (ss_tot + 1e-10)
            r2_list.append(float(r2))

            # 将残差写回（无效行保留原始值）
            residuals_full = y.astype(float).copy()
            residuals_full[valid_mask] = residuals_valid

            for i, row in enumerate(day_factor.to_dicts()):
                row["factor_value"] = float(residuals_full[i]) if valid_mask[i] else None
                result_rows.append(row)

        neutralized_df = pl.DataFrame(result_rows).cast({"factor_value": pl.Float64})
        r2_mean = float(np.mean(r2_list)) if r2_list else 0.0

        return {
            "factor_df": neutralized_df,
            "r2_by_date": [round(r, 4) for r in r2_list],
            "r2_mean": round(r2_mean, 4),
            "r2_warning": r2_mean > 0.8,
            "skipped_dates": skipped_dates,
            "merged_industries": {k: len(v) for k, v in merged_industries.items()},
        }
```

**Step 2: 验证语法**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.neutralizer import Neutralizer; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/neutralizer.py
git commit -m "feat: 新增 Neutralizer，支持市场/行业/市值三因素截面 OLS 中性化"
```

---

## Task 2: 新增行业暴露和市值暴露诊断

**Files:**
- Modify: `backend/engine/analysis/diagnostics.py` — 新增两个静态方法

**Step 1: 在 `FactorDiagnostics` 中新增 `industry_exposure()`**

```python
@staticmethod
def industry_exposure(
    factor_df: pl.DataFrame,
    industry_df: pl.DataFrame,
    factor_col: str = "factor_value",
) -> Dict[str, Any]:
    """
    计算因子在各行业的平均值 vs 全市场均值。

    Args:
        factor_df: columns=[ts_code, trade_date, factor_value]
        industry_df: columns=[ts_code, trade_date, industry_l1_value]

    Returns:
        {by_industry: {行业名: {mean_factor, vs_market, stock_count}}, max_deviation, concentration_warning}
    """
    merged = factor_df.join(
        industry_df.rename({"industry_l1_value": "industry"}),
        on=["ts_code", "trade_date"],
        how="inner"
    ).drop_nulls(subset=[factor_col, "industry"])

    if merged.is_empty():
        return {"error": "no_data_after_join"}

    # 全市场均值（时序均值）
    market_mean = float(merged[factor_col].mean())

    # 各行业均值
    by_ind = (
        merged.group_by("industry")
        .agg([
            pl.col(factor_col).mean().alias("mean_factor"),
            pl.col("ts_code").n_unique().alias("stock_count"),
        ])
        .sort("industry")
    )

    result = {}
    max_dev = 0.0
    for row in by_ind.to_dicts():
        dev = float(row["mean_factor"]) - market_mean
        result[row["industry"]] = {
            "mean_factor": round(float(row["mean_factor"]), 6),
            "vs_market": round(dev, 6),
            "stock_count": int(row["stock_count"]),
        }
        max_dev = max(max_dev, abs(dev))

    # 用全市场 std 归一化判断是否集中
    market_std = float(merged[factor_col].std()) or 1.0
    concentration_warning = max_dev > 0.5 * market_std

    return {
        "by_industry": result,
        "max_deviation": round(max_dev, 6),
        "concentration_warning": concentration_warning,
    }
```

**Step 2: 新增 `size_exposure()`**

```python
@staticmethod
def size_exposure(
    factor_df: pl.DataFrame,
    size_df: pl.DataFrame,
    factor_col: str = "factor_value",
) -> Dict[str, Any]:
    """
    计算因子与 log(market_cap) 的截面 Spearman 相关系数时序。

    Returns:
        {corr_series: [{date, corr}], corr_mean, corr_std, size_bias_warning}
    """
    merged = factor_df.join(
        size_df.rename({"market_cap_value": "mkt_cap"}),
        on=["ts_code", "trade_date"],
        how="inner"
    ).drop_nulls(subset=[factor_col, "mkt_cap"])

    if merged.is_empty():
        return {"error": "no_data_after_join"}

    # log 市值
    merged = merged.with_columns(
        pl.col("mkt_cap").log().alias("log_mkt_cap")
    ).filter(pl.col("log_mkt_cap").is_finite())

    corr_series = []
    for date, group in merged.group_by("trade_date"):
        if len(group) < 5:
            continue
        f_vals = group[factor_col].to_numpy()
        s_vals = group["log_mkt_cap"].to_numpy()
        from scipy.stats import spearmanr
        corr, _ = spearmanr(f_vals, s_vals)
        if not np.isnan(corr):
            corr_series.append({"date": date[0], "corr": round(float(corr), 4)})

    corr_series.sort(key=lambda x: x["date"])
    corr_vals = [r["corr"] for r in corr_series]
    corr_mean = float(np.mean(corr_vals)) if corr_vals else 0.0
    corr_std = float(np.std(corr_vals)) if corr_vals else 0.0

    return {
        "corr_series": corr_series,
        "corr_mean": round(corr_mean, 4),
        "corr_std": round(corr_std, 4),
        "size_bias_warning": abs(corr_mean) > 0.3,
    }
```

**Step 3: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.diagnostics import FactorDiagnostics; print('OK')"
```

Expected: `OK`

**Step 4: Commit**

```bash
git add backend/engine/analysis/diagnostics.py
git commit -m "feat: FactorDiagnostics 新增行业暴露和市值暴露分析"
```

---

## Task 3: 新增 IC 衰减分析

**Files:**
- Modify: `backend/engine/analysis/diagnostics.py` — 新增 `ic_decay()` 方法

**Step 1: 新增 `ic_decay()` 方法**

```python
@staticmethod
def ic_decay(
    factor_df: pl.DataFrame,
    price_df: pl.DataFrame,
    periods: List[int],
    factor_col: str = "factor_value",
) -> Dict[str, Any]:
    """
    IC 衰减分析：固定 T 日因子值，计算对 T+lag 日收益的 Rank IC，lag 复用 periods。

    Args:
        factor_df: columns=[ts_code, trade_date, factor_value]
        price_df:  columns=[ts_code, trade_date, close]
        periods:   lag 列表，如 [1, 5, 10]

    Returns:
        {decay: [{lag, ic_mean, rank_ic_mean}], half_life}
    """
    from scipy.stats import spearmanr, pearsonr

    price_sorted = price_df.sort(["ts_code", "trade_date"])
    decay_results = []

    for lag in periods:
        # 计算 T+lag 日收益
        with_fwd = price_sorted.with_columns(
            pl.col("close").shift(-lag).over("ts_code").alias("fwd_close")
        ).with_columns(
            (pl.col("fwd_close") / pl.col("close") - 1.0).alias("fwd_return")
        )

        merged = factor_df.join(
            with_fwd.select(["ts_code", "trade_date", "fwd_return"]),
            on=["ts_code", "trade_date"],
            how="inner"
        ).drop_nulls(subset=[factor_col, "fwd_return"])

        if merged.is_empty():
            decay_results.append({"lag": lag, "ic_mean": None, "rank_ic_mean": None})
            continue

        # 按日期计算截面 IC
        pearson_ics = []
        spearman_ics = []
        for date, group in merged.group_by("trade_date"):
            if len(group) < 10:
                continue
            f = group[factor_col].to_numpy()
            r = group["fwd_return"].to_numpy()
            p_corr, _ = pearsonr(f, r)
            s_corr, _ = spearmanr(f, r)
            if not np.isnan(p_corr):
                pearson_ics.append(p_corr)
            if not np.isnan(s_corr):
                spearman_ics.append(s_corr)

        decay_results.append({
            "lag": lag,
            "ic_mean": round(float(np.mean(pearson_ics)), 6) if pearson_ics else None,
            "rank_ic_mean": round(float(np.mean(spearman_ics)), 6) if spearman_ics else None,
        })

    # 计算 half_life（IC 衰减到初始值一半时的 lag，线性插值）
    half_life = None
    valid = [(r["lag"], r["rank_ic_mean"]) for r in decay_results if r["rank_ic_mean"] is not None]
    if len(valid) >= 2:
        ic0 = valid[0][1]
        half_target = ic0 / 2.0
        for i in range(1, len(valid)):
            if (valid[i][1] <= half_target <= valid[i-1][1]) or \
               (valid[i][1] >= half_target >= valid[i-1][1]):
                lag0, ic_a = valid[i-1]
                lag1, ic_b = valid[i]
                if abs(ic_b - ic_a) > 1e-10:
                    half_life = round(lag0 + (half_target - ic_a) / (ic_b - ic_a) * (lag1 - lag0), 1)
                break

    return {
        "decay": decay_results,
        "half_life": half_life,
    }
```

**Step 2: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.diagnostics import FactorDiagnostics; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/diagnostics.py
git commit -m "feat: FactorDiagnostics 新增 IC 衰减分析，复用 periods 参数"
```

---

## Task 4: 将中性化和新诊断接入 analyzer.py

**Files:**
- Modify: `backend/engine/analysis/analyzer.py` — `_analyze_with_alphalens()` 和 `analyze()` 签名
- Modify: `backend/app/api/v1/production/factor_analysis.py` — `AlphalensAnalysisRequest`

**Step 1: 在 `AlphalensAnalysisRequest` 中新增中性化参数**

```python
# 因子中性化
neutralize_factor: bool = False
neutralize_market: bool = False
neutralize_industry: bool = False
neutralize_size: bool = False
# 收益中性化（分层收益减基准）
benchmark_excess_return: bool = False
```

**Step 2: 在 `analyze()` 和 `_analyze_with_alphalens()` 签名中新增这些参数**

**Step 3: 在 `_analyze_with_alphalens()` 中，winsorize 之后、ForwardReturnCalculator 之前，插入中性化步骤**

```python
from engine.analysis.neutralizer import Neutralizer

neutralization_meta = {}
if neutralize_factor and (neutralize_market or neutralize_industry or neutralize_size):
    # 加载中性化所需数据
    ts_codes = factor_df["ts_code"].unique().to_list()
    date_min = factor_df["trade_date"].min()
    date_max = factor_df["trade_date"].max()

    market_df = None
    if neutralize_market:
        market_df = self.data_config_loader.load_field_data(
            "benchmark_return", ts_codes, date_min, date_max
        )
        # benchmark_return 是指数级别数据，不按 ts_code 过滤
        # 需要单独查询（见注意事项）

    industry_df = None
    if neutralize_industry:
        industry_df = self.data_config_loader.load_field_data(
            "industry_l1", ts_codes, date_min, date_max
        )

    size_df = None
    if neutralize_size:
        size_df = self.data_config_loader.load_field_data(
            "market_cap", ts_codes, date_min, date_max
        )

    neut_result = Neutralizer.neutralize(
        factor_df=factor_df,
        market_df=market_df,
        industry_df=industry_df,
        size_df=size_df,
    )
    factor_df = neut_result["factor_df"]
    neutralization_meta = {k: v for k, v in neut_result.items() if k != "factor_df"}
    prev = count
    count = len(factor_df)
    _record_step("neutralization", factor_df, prev)

    # 中性化 R² 警告
    if neut_result.get("r2_warning"):
        warnings.append({
            "key": "neutralization_r2_high",
            "severity": "medium",
            "message": f"中性化回归 R² 均值为 {neut_result['r2_mean']:.2f}，因子可能主要由市场/行业/市值解释",
            "suggested_params": {},
        })
```

**Step 4: 在诊断部分加入行业暴露、市值暴露、IC 衰减**

```python
# 行业暴露（如果有行业数据）
industry_exposure = None
if industry_df is not None:
    industry_exposure = FactorDiagnostics.industry_exposure(factor_df, industry_df)
    if industry_exposure.get("concentration_warning"):
        warnings.append({
            "key": "industry_concentration",
            "severity": "medium",
            "message": "因子存在明显行业集中，建议做行业中性化",
            "suggested_params": {"neutralize_industry": True},
        })

# 市值暴露（如果有市值数据）
size_exposure = None
if size_df is not None:
    size_exposure = FactorDiagnostics.size_exposure(factor_df, size_df)

# IC 衰减
ic_decay = FactorDiagnostics.ic_decay(factor_df, price_df, periods)

# 合并进 diagnostics
results['diagnostics'].update({
    'neutralization': neutralization_meta,
    'industry_exposure': industry_exposure,
    'size_exposure': size_exposure,
    'ic_decay': ic_decay,
})

# 更新 warnings（size_bias 需要 size_exposure 数据）
if size_exposure and size_exposure.get("size_bias_warning"):
    warnings.append({
        "key": "size_bias",
        "severity": "medium",
        "message": f"因子与市值相关性较高（均值={size_exposure['corr_mean']:.3f}），建议做市值中性化",
        "suggested_params": {"neutralize_size": True},
    })
```

**Step 5: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.analyzer import FactorAnalyzer; print('OK')"
```

Expected: `OK`

**Step 6: Commit**

```bash
git add backend/engine/analysis/analyzer.py \
        backend/app/api/v1/production/factor_analysis.py
git commit -m "feat: 中性化、行业/市值暴露、IC衰减接入分析流程"
```

---

## Task 5: 前端新增中性化选项 UI

**Files:**
- Modify: `frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts`
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`
- Modify: `frontend/src/api/index.ts`

**Step 1: 更新 API 类型，新增中性化参数**

**Step 2: 在 `useFactorAnalysis.ts` 中新增中性化状态**

```typescript
const [neutralizeMarket, setNeutralizeMarket] = useState(false);
const [neutralizeIndustry, setNeutralizeIndustry] = useState(false);
const [neutralizeSize, setNeutralizeSize] = useState(false);
```

**Step 3: 在 `AnalysisPanel.tsx` 中新增中性化选项区域**

在股票池选择下方，新增折叠面板"高级选项"：

```tsx
<Collapse>
  <Collapse.Panel header="中性化选项" itemKey="neutralize">
    <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
      <Checkbox checked={neutralizeMarket} onChange={setNeutralizeMarket}>
        市场中性化
      </Checkbox>
      <Checkbox checked={neutralizeIndustry} onChange={setNeutralizeIndustry}>
        行业中性化
      </Checkbox>
      <Checkbox checked={neutralizeSize} onChange={setNeutralizeSize}>
        市值中性化
      </Checkbox>
    </div>
    {(neutralizeMarket || neutralizeIndustry || neutralizeSize) && (
      <div style={{ marginTop: 8, color: '#94A3B8', fontSize: 12 }}>
        中性化需要在因子配置中配置对应字段（benchmark_return / industry_l1 / market_cap）
      </div>
    )}
  </Collapse.Panel>
</Collapse>
```

**Step 4: 新增 IC 衰减图表**

在分析结果区域新增 IC 衰减折线图：

```tsx
{analysisResult?.diagnostics?.ic_decay?.decay && (
  <Card title="IC 衰减分析">
    <ReactECharts option={getICDecayChartOption()} style={{ height: 200 }} />
  </Card>
)}
```

`getICDecayChartOption()` 展示 lag vs rank_ic_mean 折线图，标注 half_life。

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
git commit -m "feat: 前端新增中性化选项和 IC 衰减图表"
```

---

## 注意事项

1. **`benchmark_return` 的数据结构**：市场基准收益是指数级别数据（不含 ts_code），`DataConfigLoader.load_field_data()` 目前按 ts_code 过滤，需要为 `benchmark_return` 字段单独实现一个不过滤 ts_code 的查询方法，或在 `extra_config` 中标记 `"no_ts_code_filter": true`。

2. **中性化后因子方差为零**：`Neutralizer.neutralize()` 返回后，需检查 `factor_df["factor_value"].std() < 1e-10`，若为零则终止分析并返回 `error: "neutralized_factor_zero_variance"`。

3. **IC 衰减计算量**：对每个 lag 都要做一次全量截面 IC 计算，`periods=[1,5,10,20]` 时需要 4 次遍历。如果数据量大（>50 万行），可以考虑只在 `diagnostics` 中做，不影响主分析路径。
