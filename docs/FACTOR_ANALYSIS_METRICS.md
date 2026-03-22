# 因子分析模块指标计算文档

> 文档路径：`backend/engine/analysis/`
> 更新日期：2026-03-21

---

## 一、模块架构

```
engine/analysis/
├── analyzer.py            # 主入口，数据加载 + 流水线编排
├── alphalens_adapter.py   # 指标计算核心（Alphalens 封装）
├── forward_returns.py     # 远期收益计算
├── neutralizer.py         # 因子中性化
├── diagnostics.py         # 因子诊断与警告
└── data_cache.py          # TTL+LRU 缓存
```

### 数据流总览

```
factor_values (DolphinDB)
        │
        ▼
_load_factor_data()          → Polars DF [ts_code, trade_date, factor_value]
        │
        ├─ [可选] 股票池过滤   → inner join index_constituents
        ├─ [可选] 因子中性化   → Neutralizer.neutralize()
        │
        ▼
_load_price_data()           → Polars DF [ts_code, trade_date, open/high/low/close]
        │
        ▼
ForwardReturnCalculator.calc()  → 计算各持有期远期收益
        │
        ▼
quantize_factor()            → 添加 factor_quantile 列
        │
        ▼
AlphalensAdapter.run_full_analysis()  → 计算所有指标
        │
        ▼
factor_analysis_extended (DolphinDB)  → 压缩存储结果
```

---

## 二、远期收益计算（ForwardReturnCalculator）

**文件：** `forward_returns.py`

### 计算逻辑

支持两种买入模式：

| 模式 | 买入价 | 卖出价 |
|------|--------|--------|
| `next_day_entry=True`（默认） | T+1 日 `entry_price`（默认 open） | T+1+period 日 close |
| `next_day_entry=False` | T 日 close | T+period 日 close |

**公式：**
```
forward_return(period) = exit_close / entry_price - 1
```

**实现细节：**
- 用 Polars `shift(-n).over("ts_code")` 按股票分组取未来价格
- 过滤买入价为 0 或 null 的行（停牌处理）
- 输出列名：`1D`, `5D`, `10D`, `20D`（对应 periods 参数）
- 最终转为 Alphalens MultiIndex 格式：`(date, asset)` 双层索引

---

## 三、IC 系列指标

**文件：** `alphalens_adapter.py`，`run_full_analysis()` 方法

### 3.1 IC（Pearson 信息系数）

**调用：** `alphalens.performance.factor_information_coefficient(factor_data)`

**公式：**
```
IC(t) = Pearson_corr(factor_value(t), forward_return(t))
```
逐日计算因子值与远期收益的 Pearson 线性相关系数。

**输出字段（`ic_summary`）：**

| 字段 | 公式 | 含义 |
|------|------|------|
| `ic_mean` | `mean(IC_t)` | IC 均值，衡量因子预测能力方向和强度 |
| `ic_std` | `std(IC_t)` | IC 标准差，衡量预测稳定性 |
| `ic_ir` | `ic_mean / (ic_std + 1e-10)` | IC 信息比率，综合衡量因子质量 |
| `ic_win_rate` | `mean(IC_t > 0)` | IC 胜率，IC 为正的天数占比 |

**各周期详细统计（`ic_by_period`）：**

| 字段 | 公式 |
|------|------|
| `t_stat` | `ic_mean / (ic_std / sqrt(n))` |
| `p_value` | 双尾 t 检验：`2 * t.sf(|t_stat|, df=n-1)` |
| `n_obs` | 有效观测天数 |

> **判断标准：** `|IC_mean| > 0.03` 且 `p_value < 0.05` 认为因子有效。

### 3.2 Rank IC（Spearman 信息系数）

**计算方式：** 手动逐日计算（非 Alphalens 内置）

**公式：**
```
RankIC(t) = Spearman_corr(factor_value(t), forward_return(t))
           = Pearson_corr(rank(factor_value(t)), rank(forward_return(t)))
```

**实现：**
```python
row[col] = tmp["factor"].rank().corr(tmp[col].rank())
```
每日至少需要 5 个有效样本，否则记为 NaN。

**输出：** `rank_ic_summary`、`rank_ic_by_period`（字段结构同 IC）

> **与 IC 的区别：** Rank IC 对极端值不敏感，更适合因子值分布非正态的情况。

### 3.3 IC Decay（IC 衰减）

**含义：** 衡量因子在不同持有期的预测能力衰减情况。

**计算方式：** 对每个 period 列，用全样本（非逐日）计算整体 Pearson 和 Spearman 相关系数：
```python
ic_val = factor.corr(forward_return_period, method="pearson")
rank_ic_val = factor.corr(forward_return_period, method="spearman")
```

**输出（`ic_decay`）：**
```json
[
  {"lag": 1,  "ic": 0.045, "rank_ic": 0.052},
  {"lag": 5,  "ic": 0.031, "rank_ic": 0.038},
  {"lag": 10, "ic": 0.018, "rank_ic": 0.022},
  {"lag": 20, "ic": 0.009, "rank_ic": 0.011}
]
```

> **注意：** 这里的 IC Decay 是全样本汇总值，不是逐日滚动计算，与部分文献定义不同。

### 3.4 月度 IC 热力图（ic_by_month）

**调用：** `mean_information_coefficient(factor_data, by_time='M')`

按月聚合 IC 均值，用于识别因子的季节性规律。

---

## 四、分层收益（Quantile Returns）

**调用：** `mean_return_by_quantile(factor_data, by_date=False, by_group=False, demeaned=False)`

**逻辑：**
1. 每日将因子值分为 Q 个分位数组（默认 Q=5）
2. 计算每组的平均远期收益
3. 跨时间取均值

**输出（`quantile_returns`）：**
```json
[
  {"quantile": 1, "period": "1D", "mean_return": -0.0012, "std_return": 0.0089},
  {"quantile": 5, "period": "1D", "mean_return":  0.0021, "std_return": 0.0091}
]
```

> **判断标准：** Q5 收益 > Q1 收益（单调递增）说明因子有效。

---

## 五、累计收益（Cumulative Returns）

**文件：** `alphalens_adapter.py`，`_compute_cumulative_returns()` 方法

**逻辑：**
1. 按日期、分位数组计算平均收益
2. 对每个分位数组做累乘：`(1 + r_t).cumprod()`

**公式：**
```
CumReturn(t) = ∏(1 + mean_return_q(τ)) for τ = 1..t
```

**输出（`cumulative_returns`）：**
```json
{
  "1D": [
    {"date": "20240101", "quantile_1": 0.95, "quantile_5": 1.12, ...}
  ]
}
```

---

## 六、Alpha / Beta

**调用：** `factor_alpha_beta(factor_data, demeaned=True)`

**逻辑：** 对因子加权多空组合收益做市场回归：
```
portfolio_return(t) = alpha + beta * market_return(t) + ε(t)
```

**输出（`alpha_beta`）：**

| 字段 | 含义 |
|------|------|
| `ann_alpha` | 年化超额收益（截距项年化） |
| `beta` | 市场暴露系数 |

---

## 七、因子加权多空组合累计收益（Factor Cumulative Returns）

**调用：** `factor_returns(factor_data, demeaned=True, equal_weight=False)`

**逻辑：**
- 按因子值加权构建多空组合（`demeaned=True` 去除市场均值）
- 对每个 period 的收益除以 period 归一化为单日收益（消除重叠持仓的复利虚高）：
  ```python
  fr[col] = fr[col] / period
  ```
- 累乘得到净值曲线

**输出（`factor_cumulative_returns`）：**
```json
[{"date": "20240101", "1D": 1.05, "5D": 1.08, "10D": 1.12}]
```

---

## 八、Q5-Q1 价差时序（Spread Time Series）

**调用：** `compute_mean_returns_spread(mean_ret_by_date, upper_quant=5, lower_quant=1)`

**公式：**
```
spread(t) = mean_return_Q5(t) - mean_return_Q1(t)
```

逐日计算最高分位组与最低分位组的收益差，衡量因子多空价差的时序稳定性。

**输出（`spread_ts`）：**
```json
{"1D": {"20240101": 0.0033, "20240102": 0.0021, ...}}
```

---

## 九、换手率（Turnover）

**调用：** `quantile_turnover(factor_data, period)`

**公式：**
```
Turnover_q(t) = |stocks_in_q(t) - stocks_in_q(t-period)| / |stocks_in_q(t)|
```

衡量每个分位数组在相邻持有期之间的成分股变化比例。

**输出（`turnover`）：**
```json
{
  "quantile_1": {"period_1": {"20240101": 0.15, ...}},
  "quantile_5": {"period_1": {"20240101": 0.18, ...}}
}
```

> **判断标准：** 换手率过高（>50%）会导致实际交易成本侵蚀因子收益。

---

## 十、因子自相关衰减（Decay Analysis）

**调用：** `factor_rank_autocorrelation(factor_data, period=p)`

**公式：**
```
AutoCorr(t, p) = Spearman_corr(rank(factor(t)), rank(factor(t-p)))
```

逐日计算因子排名与 p 期前排名的 Spearman 相关系数，衡量因子信号的持续性。

**输出（`decay_analysis`）：**
```json
{
  "1D":  {"20240101": 0.92, ...},
  "5D":  {"20240101": 0.75, ...},
  "20D": {"20240101": 0.51, ...}
}
```

> **与 IC Decay 的区别：** 这里衡量的是因子值本身的持续性（自相关），而非预测能力的衰减。

---

## 十一、分组分析（Group Analysis）

当传入 `groupby_field`（如行业、市值分组）时触发。

### 11.1 分组 IC

**调用：** `factor_information_coefficient(factor_data, by_group=True)`

在每个分组内分别计算 IC，输出各组的 IC 均值。

### 11.2 分组分层收益

**调用：** `mean_return_by_quantile(factor_data, by_date=False, by_group=True, demeaned=False)`

在每个分组内分别计算各分位数的平均收益。

---

## 十二、事件研究（Event Study）

**调用：** `average_cumulative_return_by_quantile(factor_data, returns, periods_before=5, periods_after=15, demeaned=True)`

计算因子信号发出前后各分位数组的平均累计收益，类似事件窗口分析。

**输出（`event_study`）：**
```json
{
  "5": {
    "mean": {"-5": 0.001, "0": 0.0, "5": 0.008, "15": 0.021},
    "std":  {"-5": 0.003, "0": 0.0, "5": 0.005, "15": 0.009}
  }
}
```

---

## 十三、因子中性化（Neutralizer）

**文件：** `neutralizer.py`

**方法：** 逐日截面 OLS 回归，取残差作为中性化后的因子值。

**公式：**
```
factor_value(i,t) = α + β₁·industry_dummy(i,t) + β₂·log_size(i,t) + ε(i,t)
neutralized_factor(i,t) = ε(i,t)
```

**控制变量：**

| 变量 | 实现方式 |
|------|----------|
| `market` | 截距项（等权市场因子） |
| `industry` | 行业哑变量（去掉第一个行业作为基准） |
| `size` | `log(市值)`，缺失值用均值填充 |

**边界条件：**
- 每日样本 < 10 时跳过中性化，保留原始值
- OLS 失败时回退到原始值并记录警告
- 市值数据缺失超过 50% 时不加入 size 控制变量

---

## 十四、因子诊断（FactorDiagnostics）

**文件：** `diagnostics.py`

### 14.1 分布统计（distribution）

计算因子值的描述性统计：count、mean、std、skew、kurtosis、min、p1、p25、median、p75、p99、max。

### 14.2 极端值检测（extreme_values）

**判断标准：** `|factor_value - mean| > 3σ`

超过 1% 的样本为极端值时触发 WARNING，建议 Winsorize 处理（截尾至 1%-99% 分位数）。

### 14.3 正态性检验

**方法：** Shapiro-Wilk 检验（样本 ≤ 5000 时）

`p < 0.05` 时提示分布非正态，建议使用 Rank IC。

### 14.4 IC 显著性检验（ic_significance）

**方法：** 单样本 t 检验（H₀: IC_mean = 0）

```
t = IC_mean / (IC_std / sqrt(n))
p_value = 2 * t.sf(|t|, df=n-1)
```

---

## 十五、已知问题

### Bug：`_prepare_charts_data` 中的 TypeError

**错误日志：**
```
TypeError: string indices must be integers, not 'str'
File alphalens_adapter.py:623
'dates': [item['date'] for item in cum_ret]
```

**原因：** `cum_ret` 是 `dict`（`{period_key: [...]}`），但代码直接对其迭代，把 `period_key`（字符串）当成了列表元素来访问 `item['date']`。

**位置：** [alphalens_adapter.py:623](../backend/engine/analysis/alphalens_adapter.py#L623)

**修复方向：** 第 634 行已有正确的 `isinstance(cum_ret, dict)` 分支处理，但第 623 行的代码路径未走到该分支，需检查 `cum_ret` 的实际类型并统一处理逻辑。

### 其他注意事项

| 问题 | 位置 | 说明 |
|------|------|------|
| IC Decay 非逐日滚动 | `alphalens_adapter.py:178` | 全样本汇总相关系数，非标准滚动 IC Decay |
| `ic_series=None` | `analyzer.py:344` | TODO 标记，诊断模块未接收 IC 时序，`ic_significance` 不会被计算 |
| 因子加权收益归一化 | `alphalens_adapter.py:297` | 除以 period 是近似处理，对重叠持仓的处理不够严格 |
| 行业分析独立于 groupby | `analyzer.py:300` | 行业分析单独调用，与 `groupby_field` 分组分析并行存在，可能重复 |
