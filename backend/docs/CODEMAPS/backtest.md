# Backtest Engine Codemap

**Last Updated:** 2026-03-03
**Entry Points:** engine/backtester/backtester.py

## Architecture

```
Backtest Pipeline
├── Strategy Parser (engine/parser/)
│   ├─ Parse React Flow JSON
│   └─ Convert to computation graph
│
├── Backtest Engine (engine/backtester/backtester.py)
│   ├─ Load price data
│   ├─ Generate signals
│   ├─ Execute trades
│   ├─ Calculate P&L
│   └─ Compute metrics
│
└── Analysis (engine/analysis/)
    ├─ Performance metrics
    ├─ Risk analysis
    └─ Drawdown analysis
```

## Backtest Engine

### VectorizedBacktester (engine/backtester/backtester.py)

**Purpose:** Vectorized backtesting without loops

**Key Methods:**

| Method | Purpose |
|--------|---------|
| `run()` | Execute backtest |
| `_generate_signals()` | Create trading signals |
| `_execute_trades()` | Simulate trade execution |
| `_calculate_pnl()` | Compute profit/loss |
| `_calculate_metrics()` | Generate performance metrics |

### Backtest Workflow

```
1. Load Configuration
   ├─ Initial capital
   ├─ Commission rate
   ├─ Slippage rate
   └─ Position limits

2. Load Price Data
   ├─ Query from DolphinDB
   └─ Validate data quality

3. Generate Signals
   ├─ Apply factor thresholds
   ├─ Create buy/sell signals
   └─ Handle signal conflicts

4. Execute Trades (Vectorized)
   ├─ Calculate position sizes
   ├─ Apply commission
   ├─ Apply slippage
   └─ Update portfolio

5. Calculate P&L
   ├─ Daily returns
   ├─ Cumulative returns
   └─ Drawdown

6. Compute Metrics
   ├─ Sharpe ratio
   ├─ Max drawdown
   ├─ Win rate
   ├─ Profit factor
   └─ Calmar ratio
```

### Configuration

**BacktestConfig (app/core/config.py):**
```python
class BacktestConfig(BaseSettings):
    initial_capital: float = 1_000_000.0
    commission_rate: float = 0.0003  # 0.03%
    slippage_rate: float = 0.0001    # 0.01%
    min_position_size: float = 0.01  # 1%
    max_position_size: float = 0.2   # 20%
```

### Input Data Format

**Price DataFrame:**
```
Columns: ts_code, trade_date, open, high, low, close, vol
Index: trade_date (sorted)
```

**Signal DataFrame:**
```
Columns: ts_code, trade_date, signal
Values: 1 (buy), -1 (sell), 0 (hold)
```

### Output Metrics

**Portfolio Metrics:**
```python
{
    "total_return": 0.25,           # 25% total return
    "annual_return": 0.12,          # 12% annualized
    "sharpe_ratio": 1.5,            # Risk-adjusted return
    "max_drawdown": -0.15,          # -15% max drawdown
    "calmar_ratio": 0.8,            # Return / max drawdown
    "win_rate": 0.55,               # 55% winning trades
    "profit_factor": 1.8,           # Gross profit / gross loss
    "total_trades": 120,
    "winning_trades": 66,
    "losing_trades": 54,
}
```

**Daily Returns:**
```
Columns: trade_date, portfolio_value, daily_return, cumulative_return
```

## Strategy Parser

### ReactFlowParser (engine/parser/parser.py)

**Purpose:** Convert React Flow JSON to executable computation graph

**Input Format:**
```json
{
  "nodes": [
    {
      "id": "1",
      "type": "data",
      "data": {
        "label": "Load Data",
        "params": {
          "ts_code": "000001.SZ",
          "start_date": "20240101"
        }
      }
    },
    {
      "id": "2",
      "type": "factor",
      "data": {
        "label": "MA20",
        "params": {"window": 20}
      }
    }
  ],
  "edges": [
    {"source": "1", "target": "2"}
  ]
}
```

**Supported Node Types:**
- `data` - Load price data
- `factor` - Compute technical indicator
- `signal` - Generate trading signal
- `backtest` - Run backtest

**Execution:**
```python
parser = ReactFlowParser()
graph = parser.parse(flow_json)
result = graph.execute()
```

## Analysis Module

### PerformanceAnalyzer (engine/analysis/analyzer.py)

**Metrics Calculation:**

| Metric | Formula | Interpretation |
|--------|---------|-----------------|
| Total Return | (Final Value - Initial) / Initial | Overall profit |
| Annual Return | (1 + Total Return)^(252/days) - 1 | Annualized return |
| Sharpe Ratio | (Annual Return - Risk-Free) / Volatility | Risk-adjusted return |
| Max Drawdown | (Peak - Trough) / Peak | Worst peak-to-trough |
| Calmar Ratio | Annual Return / Max Drawdown | Return per unit risk |
| Win Rate | Winning Trades / Total Trades | Percentage of profitable trades |
| Profit Factor | Gross Profit / Gross Loss | Profitability ratio |

**Usage:**
```python
analyzer = PerformanceAnalyzer()
metrics = analyzer.calculate_metrics(
    portfolio_values=portfolio_values,
    returns=daily_returns,
    risk_free_rate=0.02
)
```

### DrawdownAnalyzer (engine/analysis/drawdown.py)

**Drawdown Analysis:**
```python
analyzer = DrawdownAnalyzer()
drawdowns = analyzer.calculate_drawdowns(portfolio_values)
# Returns: max_drawdown, current_drawdown, drawdown_duration
```

## Integration with API

### Strategy API (app/api/v1/strategy.py)

**Backtest Endpoint:**
```python
@router.post("/backtest")
async def run_backtest(request: BacktestRequest) -> BacktestResponse:
    """
    Execute backtest

    Request:
    {
        "strategy_json": {...},  # React Flow JSON
        "start_date": "20240101",
        "end_date": "20260101",
        "initial_capital": 1000000
    }

    Response:
    {
        "backtest_id": "bt_123",
        "status": "completed",
        "metrics": {...},
        "daily_returns": [...]
    }
    """
```

## Performance Considerations

### Vectorization
- All calculations use Polars (no loops)
- Batch operations on entire price series
- Typical performance: 1 year of daily data in < 100ms

### Memory Efficiency
- Stream large datasets
- Use chunked processing for multi-year backtests
- Cache intermediate results

### Optimization Tips
1. Use date ranges to limit data
2. Pre-filter stocks by liquidity
3. Batch multiple backtests
4. Use incremental backtesting for parameter optimization

## Example: Simple Moving Average Crossover

```python
# Strategy: Buy when MA20 > MA50, Sell when MA20 < MA50

from engine.factors.technical import TechnicalFactors
from engine.backtester.backtester import VectorizedBacktester

# Load data
df = db_client.query(
    "SELECT * FROM sync_daily_data WHERE ts_code = %s",
    ("000001.SZ",)
)

# Compute indicators
df = df.with_columns([
    TechnicalFactors.sma(df["close"], 20).alias("ma20"),
    TechnicalFactors.sma(df["close"], 50).alias("ma50"),
])

# Generate signals
df = df.with_columns(
    pl.when(pl.col("ma20") > pl.col("ma50"))
        .then(1)
        .when(pl.col("ma20") < pl.col("ma50"))
        .then(-1)
        .otherwise(0)
        .alias("signal")
)

# Run backtest
backtester = VectorizedBacktester(
    initial_capital=1_000_000,
    commission_rate=0.0003,
    slippage_rate=0.0001
)
results = backtester.run(df)
print(results.metrics)
```

## Related Codemaps

- [Factor Engine](./factors.md) - Signal generation
- [API Routes](./api.md) - Backtest endpoints
- [Data Layer](./data.md) - Price data
