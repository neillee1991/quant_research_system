# Factor Analysis P0: Forward Returns + Pipeline Coverage

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 修复前视偏差（手动计算远期收益，支持 T+1 买入和价格类型选择），并在分析流程中插入覆盖率流水线追踪每步数据损失。

**Architecture:** 新建 `ForwardReturnCalculator` 类替代 Alphalens 内部价格计算；在 `_analyze_with_alphalens` 每个处理步骤后插入计数点，汇总为 `pipeline_stats`；`AnalysisRequest` 新增 `next_day_entry`、`entry_price` 参数。

**Tech Stack:** Polars, Pandas, Alphalens, FastAPI, Python 3.11

---

## 背景：当前代码的问题

1. `AlphalensAdapter.prepare_factor_data()` 把 `price_df`（只含 close）传给 `get_clean_factor_and_forward_returns()`，Alphalens 内部用 `prices.pct_change(period)` 计算远期收益，等价于 T 日收盘买入。
2. `_load_price_data()` 只加载 `close` 列，不含 open/high/low。
3. 没有任何数据损失追踪。

---

## Task 1: 扩展价格数据加载，支持 OHLC

**Files:**
- Modify: `backend/engine/analysis/analyzer.py` — `_load_price_data()` 方法（约 L160-182）

**Step 1: 修改 SQL，加载 open/high/low/close 四列**

找到 `_load_price_data` 方法，将 SQL 从：
```python
sql = """
    SELECT ts_code, trade_date, close, pct_chg
    FROM sync_daily_data
    WHERE trade_date >= %s AND trade_date <= %s
    ORDER BY ts_code, trade_date
"""
```
改为：
```python
sql = """
    SELECT ts_code, trade_date, open, high, low, close
    FROM sync_daily_data
    WHERE trade_date >= %s AND trade_date <= %s
    ORDER BY ts_code, trade_date
"""
```

**Step 2: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.analyzer import FactorAnalyzer; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/analyzer.py
git commit -m "feat: 价格数据加载扩展为 OHLC 四列"
```

---

## Task 2: 新建 ForwardReturnCalculator

**Files:**
- Create: `backend/engine/analysis/forward_returns.py`

**Step 1: 创建文件**

```python
"""
远期收益计算器
手动计算各持有期远期收益，绕过 Alphalens 内部价格计算，支持 T+1 买入和价格类型选择。
"""
import polars as pl
import pandas as pd
import numpy as np
from typing import List, Optional
from app.core.logger import logger

VALID_PRICE_COLS = {"open", "high", "low", "close"}


class ForwardReturnCalculator:
    """
    手动计算远期收益，替代 Alphalens 的 get_clean_factor_and_forward_returns 价格计算部分。

    支持：
    - next_day_entry=True:  T+1 日 entry_price 买入，T+1+period 日 close 卖出
    - next_day_entry=False: T 日 close 买入，T+period 日 close 卖出
    """

    @staticmethod
    def calc(
        factor_df: pl.DataFrame,
        price_df: pl.DataFrame,
        periods: List[int],
        next_day_entry: bool = True,
        entry_price: str = "open",
    ) -> pd.DataFrame:
        """
        计算各持有期远期收益，返回 Alphalens 兼容的 MultiIndex DataFrame。

        Args:
            factor_df: columns=[ts_code, trade_date, factor_value]
            price_df:  columns=[ts_code, trade_date, open, high, low, close]
            periods:   持有期列表，如 [1, 5, 10]
            next_day_entry: True=T+1买入，False=T日收盘买入
            entry_price: 买入价格列名，仅 next_day_entry=True 时有效

        Returns:
            pd.DataFrame with MultiIndex (date, asset):
              - factor: 因子值
              - {period}D: 各持有期远期收益（如 1D, 5D, 10D）
        """
        if entry_price not in VALID_PRICE_COLS:
            raise ValueError(f"entry_price must be one of {VALID_PRICE_COLS}, got '{entry_price}'")

        # 1. 按 ts_code, trade_date 排序价格数据
        price_sorted = price_df.sort(["ts_code", "trade_date"])

        # 2. 计算买入价：next_day_entry=True 时 shift(-1) 取次日价格
        if next_day_entry:
            price_with_entry = price_sorted.with_columns(
                pl.col(entry_price).shift(-1).over("ts_code").alias("_entry_price"),
            )
        else:
            price_with_entry = price_sorted.with_columns(
                pl.col("close").alias("_entry_price"),
            )

        # 3. 对每个 period 计算卖出价（T+1+period 日 close，或 T+period 日 close）
        result_df = price_with_entry.select(["ts_code", "trade_date", "_entry_price"])
        for period in periods:
            shift_n = period + 1 if next_day_entry else period
            result_df = result_df.with_columns(
                pl.col("_entry_price")
                .shift(-shift_n)
                .over("ts_code")
                .alias(f"_exit_{period}")
            )
            # 用 price_sorted 的 close 作为卖出价
            exit_close = price_sorted.with_columns(
                pl.col("close").shift(-shift_n).over("ts_code").alias(f"_exit_{period}")
            ).select(["ts_code", "trade_date", f"_exit_{period}"])
            result_df = result_df.drop(f"_exit_{period}").join(
                exit_close, on=["ts_code", "trade_date"], how="left"
            )

        # 4. 计算收益率
        for period in periods:
            result_df = result_df.with_columns(
                ((pl.col(f"_exit_{period}") / pl.col("_entry_price")) - 1.0)
                .alias(f"{period}D")
            ).drop(f"_exit_{period}")

        result_df = result_df.drop("_entry_price")

        # 5. 合并因子值
        merged = factor_df.join(result_df, on=["ts_code", "trade_date"], how="inner")
        merged = merged.drop_nulls(subset=["factor_value"] + [f"{p}D" for p in periods])

        if merged.is_empty():
            raise ValueError("No valid rows after joining factor and forward returns")

        # 6. 转换为 Alphalens MultiIndex 格式
        merged_pd = merged.to_pandas()
        merged_pd["trade_date"] = pd.to_datetime(merged_pd["trade_date"], format="%Y%m%d")
        merged_pd = merged_pd.set_index(["trade_date", "ts_code"])
        merged_pd.index.names = ["date", "asset"]
        merged_pd = merged_pd.rename(columns={"factor_value": "factor"})

        # 保留 factor + period 列
        keep_cols = ["factor"] + [f"{p}D" for p in periods]
        return merged_pd[keep_cols]
```

**Step 2: 验证语法**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.forward_returns import ForwardReturnCalculator; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/engine/analysis/forward_returns.py
git commit -m "feat: 新增 ForwardReturnCalculator，支持 T+1 买入和 OHLC 价格选择"
```

---

## Task 3: 扩展 AnalysisRequest，新增买入参数

**Files:**
- Modify: `backend/app/api/v1/production/factor_analysis.py` — `AlphalensAnalysisRequest` 模型

**Step 1: 在 `AlphalensAnalysisRequest` 中新增字段**

找到 `AlphalensAnalysisRequest` 类（约 L27-35），添加：

```python
class AlphalensAnalysisRequest(BaseModel):
    """Alphalens 分析请求"""
    factor_id: str
    start_date: str
    end_date: str
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5
    index_pool: Optional[str] = None
    groupby_field: Optional[str] = None
    # 新增：买入时点控制
    next_day_entry: bool = True
    entry_price: str = "open"   # "open" | "close" | "high" | "low"
```

**Step 2: 在 `run_alphalens_analysis` 端点中透传新参数**

找到 `run_alphalens_analysis` 函数（约 L177），将 `analyzer.analyze()` 调用改为：

```python
results = analyzer.analyze(
    factor_id=req.factor_id,
    start_date=req.start_date,
    end_date=req.end_date,
    periods=req.periods,
    quantiles=req.quantiles,
    use_alphalens=True,
    index_pool=req.index_pool,
    groupby_field=req.groupby_field,
    next_day_entry=req.next_day_entry,
    entry_price=req.entry_price,
)
```

**Step 3: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from app.api.v1.production.factor_analysis import router; print('OK')"
```

Expected: `OK`

**Step 4: Commit**

```bash
git add backend/app/api/v1/production/factor_analysis.py
git commit -m "feat: AnalysisRequest 新增 next_day_entry 和 entry_price 参数"
```

---

## Task 4: 将 ForwardReturnCalculator 接入 AnalyzerAdapter

**Files:**
- Modify: `backend/engine/analysis/alphalens_adapter.py` — `prepare_factor_data()` 方法
- Modify: `backend/engine/analysis/analyzer.py` — `_analyze_with_alphalens()` 和 `analyze()` 签名

**Step 1: 修改 `analyzer.py` 的 `analyze()` 签名，新增参数**

找到 `analyze()` 方法（约 L40-77），添加两个参数：

```python
def analyze(
    self,
    factor_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    periods: List[int] = None,
    quantiles: int = 5,
    use_alphalens: bool = True,
    index_pool: Optional[str] = None,
    groupby_field: Optional[str] = None,
    next_day_entry: bool = True,       # 新增
    entry_price: str = "open",         # 新增
) -> Optional[Dict[str, Any]]:
```

同样在 `_analyze_with_alphalens()` 签名中新增这两个参数，并透传给 `alphalens_adapter.prepare_factor_data()`。

**Step 2: 修改 `AlphalensAdapter.prepare_factor_data()` 签名**

```python
def prepare_factor_data(
    self,
    factor_df: pl.DataFrame,
    price_df: pl.DataFrame,
    periods: List[int] = [1, 5, 10, 20],
    quantiles: int = 5,
    groupby_df: Optional[pl.DataFrame] = None,
    next_day_entry: bool = True,    # 新增
    entry_price: str = "open",      # 新增
) -> pd.DataFrame:
```

**Step 3: 在 `prepare_factor_data()` 中用 ForwardReturnCalculator 替代 Alphalens 价格计算**

将原来调用 `get_clean_factor_and_forward_returns` 的逻辑替换为：

```python
from engine.analysis.forward_returns import ForwardReturnCalculator

# 1. 用 ForwardReturnCalculator 手动计算远期收益
factor_data_raw = ForwardReturnCalculator.calc(
    factor_df=factor_df,
    price_df=price_df,
    periods=periods,
    next_day_entry=next_day_entry,
    entry_price=entry_price,
)

# 2. 添加分位数列（Alphalens 后续分析需要 factor_quantile 列）
#    用 alphalens.utils.quantize_factor 对已有数据打分位标签
from alphalens.utils import quantize_factor
factor_data_raw["factor_quantile"] = quantize_factor(
    factor_data_raw["factor"],
    quantiles=quantiles,
    bins=None,
    by_group=False,
)

# 3. 处理分组数据（如果有）
if groupby_df is not None and not groupby_df.is_empty():
    groupby_series = self._prepare_groupby(groupby_df)
    factor_data_raw = factor_data_raw.join(
        groupby_series.rename("group"), how="left"
    )

logger.info(f"Factor data prepared: {len(factor_data_raw)} rows")
return factor_data_raw
```

**Step 4: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "
from engine.analysis.alphalens_adapter import AlphalensAdapter
from engine.analysis.analyzer import FactorAnalyzer
print('imports OK')
"
```

Expected: `imports OK`

**Step 5: Commit**

```bash
git add backend/engine/analysis/alphalens_adapter.py backend/engine/analysis/analyzer.py
git commit -m "feat: 用 ForwardReturnCalculator 替代 Alphalens 内部价格计算"
```

---

## Task 5: 新增覆盖率流水线追踪

**Files:**
- Modify: `backend/engine/analysis/analyzer.py` — `_analyze_with_alphalens()` 方法

**Step 1: 在 `_analyze_with_alphalens()` 中插入计数点**

在方法开头初始化 pipeline_stats：

```python
pipeline_stats = []

def _record_step(step: str, df_or_count, prev_count: int) -> int:
    """记录一个流水线步骤的数据量变化"""
    current = len(df_or_count) if hasattr(df_or_count, '__len__') else df_or_count
    dropped = prev_count - current
    pipeline_stats.append({
        "step": step,
        "total_rows": current,
        "dropped": dropped,
        "drop_pct": round(dropped / prev_count * 100, 2) if prev_count > 0 else 0.0,
    })
    return current
```

在每个处理步骤后调用 `_record_step`：

```python
# 步骤 1: 加载因子数据后
count = len(factor_df)
_record_step("raw_factor", factor_df, 0)  # 第一步 dropped=0

# 步骤 2: 日期过滤（已在 _load_factor_data 中完成，此处记录结果）
# 步骤 3: 股票池过滤后
prev = count
count = len(factor_df)
_record_step("index_pool_filter", factor_df, prev)

# 步骤 4: ForwardReturnCalculator 后（drop_nulls 发生在内部）
prev = count
count = len(factor_data_raw)
_record_step("forward_return", factor_data_raw, prev)

# 步骤 5: Alphalens quantize 后（drop_nulls 可能再次发生）
prev = count
count = len(factor_data)
_record_step("alphalens_clean", factor_data, prev)
```

**Step 2: 将 pipeline_stats 加入返回结果**

在 `results` 字典中添加：

```python
results["diagnostics"] = {
    "pipeline_stats": pipeline_stats,
    "final_rows": len(factor_data),
    "final_dates": factor_data.index.get_level_values("date").nunique(),
    "avg_daily_coverage": round(len(factor_data) / max(factor_data.index.get_level_values("date").nunique(), 1), 1),
}
```

**Step 3: 验证 pipeline_stats 出现在返回结果中**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "
from engine.analysis.analyzer import FactorAnalyzer
import inspect
src = inspect.getsource(FactorAnalyzer._analyze_with_alphalens)
assert 'pipeline_stats' in src, 'pipeline_stats not found'
print('pipeline_stats OK')
"
```

Expected: `pipeline_stats OK`

**Step 4: Commit**

```bash
git add backend/engine/analysis/analyzer.py
git commit -m "feat: 分析流水线新增覆盖率追踪 pipeline_stats"
```

---

## Task 6: 前端新增买入参数 UI

**Files:**
- Modify: `frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts`
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`
- Modify: `frontend/src/api/index.ts`

**Step 1: 更新 `api/index.ts` 的 `runAlphalensAnalysis` 类型**

在请求体类型中新增：

```typescript
runAlphalensAnalysis: (data: {
  factor_id: string;
  start_date: string;
  end_date: string;
  periods?: number[];
  quantiles?: number;
  index_pool?: string;
  groupby_field?: string;
  next_day_entry?: boolean;   // 新增
  entry_price?: string;       // 新增
}) => longRunningApi.post('/analysis/alphalens', data),
```

**Step 2: 在 `useFactorAnalysis.ts` 中新增状态**

```typescript
const [nextDayEntry, setNextDayEntry] = useState(true);
const [entryPrice, setEntryPrice] = useState<string>('open');
```

在 `runAnalysis` 的 API 调用中透传：

```typescript
res = await productionApi.runAlphalensAnalysis({
  ...
  next_day_entry: nextDayEntry,
  entry_price: entryPrice,
});
```

在 return 对象中新增 `nextDayEntry, setNextDayEntry, entryPrice, setEntryPrice`。

**Step 3: 在 `AnalysisPanel.tsx` 中新增 UI**

在日期选择器下方，新增两个控件（仅在 Alphalens 模式下显示）：

```tsx
<Checkbox
  checked={nextDayEntry}
  onChange={setNextDayEntry}
>
  次日买入（推荐，避免前视偏差）
</Checkbox>

{nextDayEntry && (
  <Select
    value={entryPrice}
    onChange={setEntryPrice}
    style={{ width: 120 }}
    optionList={[
      { label: '开盘价', value: 'open' },
      { label: '收盘价', value: 'close' },
      { label: '最高价', value: 'high' },
      { label: '最低价', value: 'low' },
    ]}
  />
)}
```

**Step 4: 验证前端编译**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
npm run build 2>&1 | grep -E "ERROR|error TS" | head -20
```

Expected: 无输出

**Step 5: Commit**

```bash
git add frontend/src/api/index.ts \
        frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts \
        frontend/src/pages/FactorCenter/AnalysisPanel.tsx
git commit -m "feat: 前端新增次日买入和买入价格选择 UI"
```

---

## Task 7: 前端展示 pipeline_stats

**Files:**
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`

**Step 1: 在分析结果区域新增流水线统计展示**

在 IC 图表上方，新增一个折叠面板（Collapsible）展示 pipeline_stats：

```tsx
{analysisResult?.diagnostics?.pipeline_stats && (
  <Collapse>
    <Collapse.Panel header="数据覆盖率流水线" itemKey="pipeline">
      <Table
        size="small"
        dataSource={analysisResult.diagnostics.pipeline_stats}
        columns={[
          { title: '步骤', dataIndex: 'step' },
          { title: '剩余行数', dataIndex: 'total_rows' },
          { title: '过滤行数', dataIndex: 'dropped' },
          {
            title: '过滤比例',
            dataIndex: 'drop_pct',
            render: (v: number) => (
              <Tag color={v > 20 ? 'orange' : 'green'}>{v.toFixed(1)}%</Tag>
            )
          },
        ]}
        pagination={false}
      />
      <div style={{ marginTop: 8, color: '#94A3B8', fontSize: 12 }}>
        最终有效数据：{analysisResult.diagnostics.final_rows} 行，
        {analysisResult.diagnostics.final_dates} 个交易日，
        日均覆盖 {analysisResult.diagnostics.avg_daily_coverage?.toFixed(0)} 只股票
      </div>
    </Collapse.Panel>
  </Collapse>
)}
```

**Step 2: 验证前端编译**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
npm run build 2>&1 | grep -E "ERROR|error TS" | head -20
```

Expected: 无输出

**Step 3: Commit**

```bash
git add frontend/src/pages/FactorCenter/AnalysisPanel.tsx
git commit -m "feat: 前端展示数据覆盖率流水线统计"
```

---

## 注意事项

1. **`quantize_factor` 的调用方式**：Alphalens 的 `quantize_factor` 接受 `pd.Series`（MultiIndex），不是 DataFrame 列。调用时传 `factor_data_raw["factor"]`。

2. **价格数据中 open=0 的情况**：A 股停牌时 open 可能为 0，`ForwardReturnCalculator` 中需要过滤：
   ```python
   price_with_entry = price_with_entry.filter(pl.col("_entry_price") > 0)
   ```
   这会在 `forward_return` 步骤中被 pipeline_stats 记录。

3. **`extra_days` 的调整**：`next_day_entry=True` 时，末尾需要多加载 1 天，`_load_price_data` 中 `extra_days = max_period * 2 + 1`。
