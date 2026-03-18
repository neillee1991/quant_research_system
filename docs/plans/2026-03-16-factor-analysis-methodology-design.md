# 因子分析方法论升级设计文档

**日期**: 2026-03-16
**状态**: 待实现

---

## 目标

将因子分析模块从当前的基础 Alphalens 封装，升级为一套完整的量化因子研究框架，支持：前视偏差控制、通用中性化、完整 IC 分析、多空净值曲线、衰减分析、诊断报告、交互式参数调整。

---

## 架构概览

```
AnalysisRequest（含所有参数）
    ↓
FactorAnalyzer.analyze()
    ├── DataCache（原始数据缓存，TTL 30min）
    ├── Neutralizer（因子中性化，可选）
    ├── ForwardReturnCalculator（手动计算远期收益，绕过 Alphalens 价格计算）
    ├── AlphalensAdapter（IC、分层收益、换手率）
    ├── FactorDiagnostics（覆盖率流水线、分布、暴露、显著性、衰减）
    └── AnalysisResult（含 diagnostics + warnings + main_results）
```

新增文件：
- `engine/analysis/neutralizer.py` — 通用中性化
- `engine/analysis/forward_returns.py` — 远期收益计算
- `engine/analysis/diagnostics.py` — 诊断分析
- `engine/analysis/data_cache.py` — 数据缓存管理

---

## Section 1：分析参数

### 新增参数（全部有默认值，向后兼容）

```python
class AnalysisRequest(BaseModel):
    factor_id: str
    start_date: str
    end_date: str
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5
    index_pool: Optional[str] = None
    groupby_field: Optional[str] = None

    # 买入时点控制
    next_day_entry: bool = True          # True=T+1买入，False=T日收盘买入
    entry_price: str = "open"           # "open" | "close" | "high" | "low"

    # 因子中性化（作用于因子值本身，回归残差）
    neutralize_factor: bool = False
    neutralize_market: bool = False      # 剔除市场 beta（需配置 benchmark_return 字段）
    neutralize_industry: bool = False    # 剔除行业（用 industry_l1 历史快照）
    neutralize_size: bool = False        # 剔除市值（用 market_cap 配置）

    # 收益中性化（分层收益减基准）
    benchmark_excess_return: bool = False

    # 数据清洗（诊断后用户可调整）
    winsorize: bool = False
    winsorize_lower: float = 0.01        # 下分位数截断
    winsorize_upper: float = 0.99        # 上分位数截断

    # 重跑时复用缓存
    cache_key: Optional[str] = None      # 传入上次的 task_id 复用数据缓存
```

---

## Section 2：远期收益计算（`forward_returns.py`）

**绕过 Alphalens 价格计算**，手动构造带 `{period}D` 列的 DataFrame。

### 计算逻辑

```
next_day_entry=True, entry_price="open":
  fwd_return(T, period) = price[T+1, open→T+1+period, close] / price[T+1, open] - 1

next_day_entry=False（当日收盘买入）:
  fwd_return(T, period) = price[T+period, close] / price[T, close] - 1
```

### 价格数据加载

`_load_price_data()` 需同时加载 open、high、low、close 四列（当前只加载 close）。

### 边界情况

| 场景 | 处理 |
|------|------|
| T+1 日停牌（open=0 或 null） | fwd_return 为 null，进入 pipeline_stats 的 `forward_return` 步骤 |
| 末尾 period 天无法计算 | drop_nulls，pipeline_stats 记录 |
| 价格为负或零 | 视为异常，fwd_return 置 null，`diagnostics.price_anomalies` 记录 |

---

## Section 3：通用中性化（`neutralizer.py`）

### 核心算法

每个截面日做一次多元 OLS 回归（`numpy.linalg.lstsq`，自动处理共线性）：

```
factor_value = β₀
             + β₁ · mkt_return          （若 neutralize_market=True）
             + β₂ · log(mkt_cap)        （若 neutralize_size=True）
             + Σ βₖ · industry_dummy_k  （若 neutralize_industry=True）
             + ε
```

残差 ε 作为中性化后的因子值，传入后续分析。

### 数据来源

全部走 `DataConfigLoader.load_field_data()`：
- 市场：`field_key="benchmark_return"`（新增配置项，指向指数日行情表的收益列）
- 行业：`field_key="industry_l1"`（历史快照，每股每日一条记录）
- 市值：`field_key="market_cap"`

### 边界情况

| 场景 | 处理 | 报告 |
|------|------|------|
| 某日行业/市值数据缺失 | 跳过该日中性化，用原始因子值 | `neutralization.skipped_dates` |
| 某行业当日只有 1 只股票 | 合并到 "OTHER" 行业 | `neutralization.merged_industries` |
| 回归 R² 均值 > 0.8 | 不阻断，高亮警告（因子可能就是市值/行业因子） | `neutralization.r2_warning: true` |
| 回归矩阵奇异 | lstsq 自动处理，静默 | 无 |
| 中性化后因子方差接近 0 | 终止分析，返回错误 | `error: "neutralized_factor_zero_variance"` |

### 输出

```python
{
  "neutralized_factor_df": pl.DataFrame,  # 替换 factor_value 列
  "r2_by_date": List[float],              # 每日回归 R²
  "r2_mean": float,
  "r2_warning": bool,
  "skipped_dates": List[str],
  "merged_industries": Dict[str, List[str]]
}
```

---

## Section 4：诊断分析（`diagnostics.py`）

### 4.1 覆盖率流水线（`pipeline_stats`）

在 `FactorAnalyzer._analyze_with_alphalens()` 每个处理步骤后插入计数点：

```python
pipeline_steps = [
    "raw_factor",         # 从 DB 加载后
    "date_filter",        # 日期范围过滤后
    "index_pool_filter",  # 股票池过滤后
    "winsorize",          # winsorize 后（若启用）
    "neutralization",     # 中性化后（若启用）
    "forward_return",     # 远期收益计算后（drop_nulls）
    "alphalens_clean",    # Alphalens 内部清洗后
    "min_section_size",   # 截面样本数 < 30 过滤后
]
```

每步记录：`total_rows`、`dropped`、`drop_pct`、`dates_remaining`。

输出示例：
```json
{
  "pipeline": [
    {"step": "raw_factor",        "total_rows": 120000, "dropped": 0,     "drop_pct": 0.0,  "dates": 600},
    {"step": "date_filter",       "total_rows": 80000,  "dropped": 40000, "drop_pct": 33.3, "dates": 400},
    {"step": "index_pool_filter", "total_rows": 30000,  "dropped": 50000, "drop_pct": 62.5, "dates": 400},
    {"step": "forward_return",    "total_rows": 28500,  "dropped": 1500,  "drop_pct": 5.0,  "dates": 380},
    {"step": "alphalens_clean",   "total_rows": 28200,  "dropped": 300,   "drop_pct": 1.1,  "dates": 380}
  ],
  "final_rows": 28200,
  "final_dates": 380,
  "avg_daily_coverage": 74.2,
  "coverage_pct": 0.82
}
```

### 4.2 因子分布（`distribution`）

每日截面统计，汇总为时序均值：

```python
{
  "mean": float,
  "std": float,
  "skewness": float,          # 偏度，|skew| > 1 建议 winsorize
  "kurtosis": float,          # 峰度
  "extreme_ratio": float,     # |z-score| > 3 的比例
  "extreme_ratio_warning": bool,  # extreme_ratio > 0.05 时为 True
  "percentiles": {            # 全局分位数
    "p1": float, "p5": float, "p25": float,
    "p50": float, "p75": float, "p95": float, "p99": float
  }
}
```

**边界情况**：
- 因子值全为同一常数（零方差）→ 终止分析，`error: "factor_has_zero_variance"`
- 极端值比例 > 5% → `extreme_ratio_warning: true`，前端高亮提示用户考虑 winsorize

### 4.3 行业暴露（`industry_exposure`）

每日截面：各行业因子均值 vs 全市场均值，汇总为时序均值。

```python
{
  "by_industry": {
    "银行": {"mean_factor": 0.32, "vs_market": +0.15, "stock_count": 42},
    "电子": {"mean_factor": -0.12, "vs_market": -0.25, "stock_count": 89},
    ...
  },
  "max_deviation": float,     # 最大行业偏离
  "concentration_warning": bool  # 某行业偏离 > 0.5σ 时警告
}
```

### 4.4 市值暴露（`size_exposure`）

每日截面：因子值与 log(market_cap) 的 Spearman 相关系数，输出时序。

```python
{
  "corr_series": [{"date": "20230101", "corr": 0.23}, ...],
  "corr_mean": float,
  "corr_std": float,
  "size_bias_warning": bool   # |corr_mean| > 0.3 时警告
}
```

**边界情况**：市值数据未配置 → 跳过，`size_exposure: null`，前端提示"未配置市值字段"。

### 4.5 IC 统计显著性（`ic_significance`）

对 IC 时序做单样本 t-test（H₀: IC_mean = 0）：

```python
{
  "t_stat": float,
  "p_value": float,
  "significant": bool,        # p_value < 0.05
  "sample_size": int,
  "insufficient_warning": bool  # 样本数 < 20 时警告
}
```

**边界情况**：
- IC 序列长度 < 20 → 计算但标记 `insufficient_warning: true`
- Pearson IC 与 Rank IC 差异 > 0.3 → `ic_divergence_warning: true`，建议使用 Rank IC

### 4.6 衰减分析（`ic_decay`）

复用 `periods` 参数，对每个 lag 计算 IC 均值（固定 T 日因子，对 T+lag 收益计算 IC）：

```python
{
  "decay": [
    {"lag": 1,  "ic_mean": 0.045, "rank_ic_mean": 0.052},
    {"lag": 5,  "ic_mean": 0.031, "rank_ic_mean": 0.038},
    {"lag": 10, "ic_mean": 0.018, "rank_ic_mean": 0.022},
  ],
  "half_life": float  # IC 衰减到初始值一半时的 lag（线性插值）
}
```

---

## Section 5：多空净值曲线

在 `AlphalensAdapter._compute_cumulative_returns()` 中新增：

```python
{
  "long_short": {
    "dates": [...],
    "raw": [...],           # Q5 - Q1 每日收益累计净值
    "excess": [...],        # (Q5 - Q1 - benchmark) 累计净值（若 benchmark_excess_return=True）
    "max_drawdown": float,
    "sharpe": float,        # 时序 Sharpe = mean(daily_ret) / std(daily_ret) * sqrt(252)
    "anomaly_dates": [...]  # 单日涨跌幅 > 20% 的日期，前端标注
  }
}
```

**边界情况**：
- 某层某日样本数为 0 → 该日收益为 null，净值曲线断点，`anomaly_dates` 记录
- 基准收益数据缺失某日 → 该日不做超额，`benchmark.missing_dates` 记录

---

## Section 6：交互式诊断与参数调整

### 工作流

```
第一次提交分析
    ↓
后台执行完整分析 + 诊断
    ↓
返回结果，diagnostics.warnings 包含所有高亮问题
    ↓
前端展示警告模块（高亮卡片）
    ↓
用户调整参数（winsorize、中性化选项等）
    ↓
提交重跑请求（携带 cache_key=上次 task_id）
    ↓
后端从缓存取原始数据，跳过数据加载，直接重跑计算
```

### 警告类型与前端交互

| 警告 key | 触发条件 | 前端展示 | 用户可调整参数 |
|----------|----------|----------|----------------|
| `extreme_values` | `extreme_ratio > 0.05` | 高亮卡片：显示极端值比例，建议 winsorize | `winsorize=true`，`winsorize_lower/upper` |
| `size_bias` | `|size_corr_mean| > 0.3` | 警告：因子与市值高度相关 | `neutralize_size=true` |
| `industry_concentration` | 某行业偏离 > 0.5σ | 警告：因子存在行业集中 | `neutralize_industry=true` |
| `neutralization_r2_high` | `r2_mean > 0.8` | 警告：中性化 R² 过高，因子可能是纯市值/行业因子 | 提示用户重新审视因子定义 |
| `ic_not_significant` | `p_value >= 0.05` | 提示：IC 统计上不显著 | 调整日期范围或因子参数 |
| `ic_divergence` | Pearson vs Rank IC 差 > 0.3 | 提示：建议使用 Rank IC | 无参数调整，仅信息提示 |
| `low_coverage` | `coverage_pct < 0.3` | 警告：因子覆盖率过低 | 调整股票池或日期范围 |

### 警告结构（API 返回）

```json
{
  "warnings": [
    {
      "key": "extreme_values",
      "severity": "high",
      "message": "因子值中有 8.3% 的极端值（|z-score| > 3），建议进行 winsorize 处理",
      "suggested_params": {
        "winsorize": true,
        "winsorize_lower": 0.01,
        "winsorize_upper": 0.99
      }
    }
  ]
}
```

---

## Section 7：数据缓存（`data_cache.py`）

### 缓存结构

```python
class AnalysisDataCache:
    _store: Dict[str, CacheEntry] = {}
    MAX_TOTAL_BYTES = 500 * 1024 * 1024  # 500MB
    TTL_SECONDS = 1800                    # 30 分钟

@dataclass
class CacheEntry:
    factor_df: pl.DataFrame
    price_df: pl.DataFrame
    created_at: datetime
    last_accessed: datetime
    size_bytes: int
```

### 释放策略（三重保障）

1. **TTL 自动过期**：后台线程每 5 分钟扫描，清除超过 30 分钟未访问的条目
2. **用户主动释放**：`DELETE /analysis/cache/{task_id}`，前端在关闭分析面板或切换因子时调用
3. **内存压力保护**：总缓存超过 500MB 时，按 LRU 淘汰最久未访问的条目

### API

```
DELETE /analysis/cache/{task_id}   — 主动释放
GET    /analysis/cache/stats       — 查看缓存状态（调试用）
```

---

## Section 8：API 变更

### 新增/修改端点

| 端点 | 变更 | 说明 |
|------|------|------|
| `POST /analysis/alphalens` | 修改请求体 | 新增所有分析参数 |
| `GET /analysis/alphalens/status/{task_id}` | 新增 | 任务状态查询 |
| `DELETE /analysis/cache/{task_id}` | 新增 | 主动释放缓存 |
| `GET /analysis/cache/stats` | 新增 | 缓存状态（调试） |

### 返回结构

```json
{
  "status": "success",
  "data": {
    "task_id": 1710000000000,
    "factor_id": "momentum_20d",
    "cache_key": "1710000000000",
    "main_results": {
      "ic_summary": {},
      "ic_by_period": [],
      "ic_ts": [],
      "quantile_returns": [],
      "cumulative_returns": [],
      "long_short": {},
      "turnover": {},
      "ic_decay": {}
    },
    "diagnostics": {
      "pipeline_stats": {},
      "distribution": {},
      "industry_exposure": {},
      "size_exposure": {},
      "ic_significance": {},
      "neutralization": {}
    },
    "warnings": []
  }
}
```

---

## 实现优先级

| 优先级 | 模块 | 原因 |
|--------|------|------|
| P0 | 远期收益计算（前视偏差修复） | 影响所有分析结果的正确性 |
| P0 | 覆盖率流水线 | 基础诊断，影响用户信任度 |
| P1 | 因子分布 + 极端值警告 + winsorize | 交互式调参的核心场景 |
| P1 | 多空净值曲线 | 最直观的因子评估指标 |
| P1 | IC 显著性 + Pearson/Rank IC 双输出 | 方法论完整性 |
| P2 | 通用中性化（Neutralizer） | 依赖数据配置，需用户先配置字段 |
| P2 | 行业/市值暴露 | 依赖中性化数据 |
| P2 | 衰减分析 | 计算量较大 |
| P3 | 数据缓存 + 交互式重跑 | 依赖前面所有模块完成 |
