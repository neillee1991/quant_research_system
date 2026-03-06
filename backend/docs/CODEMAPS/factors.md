# Factor Engine Codemap

**Last Updated:** 2026-03-03
**Entry Points:** engine/production/engine.py, engine/factors/

## Architecture

```
Factor Computation Pipeline
├── Factor Registry (engine/production/registry.py)
│   ├─ @factor decorator
│   ├─ FactorDefinition
│   └─ discover_factors()
│
├── Production Engine (engine/production/engine.py)
│   ├─ run_task()
│   ├─ _resolve_dates()
│   ├─ _load_data()
│   ├─ _apply_adjust()
│   ├─ _apply_stock_status()
│   ├─ _execute_factor()
│   ├─ _handle_suspension()
│   ├─ _build_quality_flag()
│   └─ _save_results()
│
├── Factor Library (engine/factors/)
│   ├─ TechnicalFactors (technical.py)
│   ├─ CrossSectionalFactors (technical.py)
│   └─ FactorAnalyzer (financial.py)
│
└── Data Configuration (engine/production/data_config.py)
    └─ DataConfigLoader
```

## Factor Registry

### FactorDefinition

**Structure:**
```python
@dataclass
class FactorDefinition:
    factor_id: str
    factor_name: str
    description: str
    func: Callable  # Factor computation function
    depends_on: List[str]  # Required data fields
    params: Dict[str, Any]  # Parameters
    storage_config: StorageConfig
    mode: str  # "incremental" or "full"
```

### Registration

**Via Decorator:**
```python
@factor(
    factor_id="ma20",
    factor_name="20-day Moving Average",
    depends_on=["close"],
    params={"window": 20},
    mode="incremental"
)
def compute_ma20(df: pl.DataFrame, params: Dict) -> pl.Series:
    return df["close"].rolling_mean(window_size=params["window"])
```

**Discovery:**
```python
from engine.production.registry import discover_factors, get_factor

# Load factors from code and database
discover_factors(db_client=db_client)

# Get specific factor
definition = get_factor("ma20")

# List all factors
factors = list_factors()
```

## Production Engine

### ProductionEngine.run_task()

**8-Step Computation Pipeline:**

```
1. _resolve_dates()
   ├─ Determine mode (incremental/full)
   ├─ Calculate data_start (lookback_days offset)
   └─ Set target_date for incremental mode

2. _load_data()
   ├─ Parse depends_on fields
   ├─ Query DolphinDB for each field
   └─ Merge into single DataFrame

3. _apply_adjust()
   ├─ Load adj_factor from DolphinDB
   ├─ Apply forward/backward 复权
   └─ Adjust OHLC prices

4. _apply_stock_status()
   ├─ Load stock_daily_status
   ├─ Filter ST stocks
   ├─ Filter new stocks (< 60 days)
   └─ Mark limit-up/limit-down

5. definition.func(df, params)
   ├─ Execute factor computation
   ├─ Vectorized Polars operations
   └─ Return factor values

6. _handle_suspension_from_status()
   ├─ Identify suspension periods
   └─ Set factor_value to NULL

7. _build_quality_flag()
   ├─ Calculate null rate
   ├─ Detect extreme values
   └─ Generate quality score

8. _save_results()
   ├─ Upsert to factor_values table
   └─ Update production_task_run log
```

### Key Methods

**run_task()**
```python
def run_task(
    factor_id: str,
    target_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    mode: Optional[str] = None,
    preprocess: Optional[Dict[str, Any]] = None,
) -> bool:
    """Execute factor computation task"""
```

**_resolve_dates()**
```python
def _resolve_dates(
    factor_id: str,
    target_date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    mode: Optional[str]
) -> Tuple[str, str, str, str]:
    """
    Returns: (mode, data_start, calc_start, calc_end)
    - mode: "incremental" or "full"
    - data_start: earliest date to load (includes lookback)
    - calc_start: first date to compute
    - calc_end: last date to compute
    """
```

**_load_data()**
```python
def _load_data(
    factor_id: str,
    data_start: str,
    calc_end: str
) -> pl.DataFrame:
    """
    Load all required fields from DolphinDB
    Returns DataFrame with columns: ts_code, trade_date, [field1, field2, ...]
    """
```

**_apply_adjust()**
```python
def _apply_adjust(
    df: pl.DataFrame,
    adjust_method: str  # "forward" or "backward"
) -> pl.DataFrame:
    """Apply price adjustment (复权)"""
```

**_apply_stock_status()**
```python
def _apply_stock_status(
    df: pl.DataFrame,
    filter_st: bool,
    filter_new_stock: bool,
    new_stock_days: int,
    mark_limit: bool
) -> pl.DataFrame:
    """Apply stock status filters and marks"""
```

**_build_quality_flag()**
```python
def _build_quality_flag(
    df: pl.DataFrame,
    factor_col: str
) -> pl.Series:
    """
    Generate quality flags:
    - null_rate: percentage of NULL values
    - extreme_rate: percentage of extreme values
    - quality_score: 0-100 score
    """
```

## Technical Factors

### TechnicalFactors (engine/factors/technical.py)

**Available Indicators:**

| Indicator | Method | Parameters |
|-----------|--------|------------|
| SMA | `sma(series, window)` | window: int |
| EMA | `ema(series, window)` | window: int |
| RSI | `rsi(series, window)` | window: 14 (default) |
| MACD | `macd(series, fast, slow, signal)` | fast: 12, slow: 26, signal: 9 |
| KDJ | `kdj(high, low, close, n, m1, m2)` | n: 9, m1: 3, m2: 3 |
| Bollinger Bands | `bollinger_bands(series, window, num_std)` | window: 20, num_std: 2.0 |
| ATR | `atr(high, low, close, window)` | window: 14 |

**Implementation:**
```python
class TechnicalFactors:
    @staticmethod
    def sma(series: pl.Series, window: int) -> pl.Series:
        """Simple Moving Average"""
        return series.rolling_mean(window_size=window, min_periods=1)

    @staticmethod
    def rsi(series: pl.Series, window: int = 14) -> pl.Series:
        """Relative Strength Index"""
        delta = series.diff()
        gain = delta.clip(lower_bound=0)
        loss = (-delta).clip(lower_bound=0)
        avg_gain = gain.ewm_mean(span=window, adjust=False)
        avg_loss = loss.ewm_mean(span=window, adjust=False)
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))
```

### CrossSectionalFactors (engine/factors/technical.py)

**Available Factors:**

| Factor | Method | Purpose |
|--------|--------|---------|
| Rank | `rank(series)` | Cross-sectional rank |
| Z-Score | `zscore(series)` | Standardized score |
| Industry Neutral | `industry_neutral(series, industry)` | Remove industry effect |

## Financial Factors

### FactorAnalyzer (engine/factors/financial.py)

**Analysis Methods:**

| Method | Purpose |
|--------|---------|
| `calculate_ic()` | Information Coefficient |
| `calculate_rank_ic()` | Rank IC |
| `calculate_turnover()` | Portfolio turnover |
| `calculate_sharpe()` | Sharpe ratio |

**Usage:**
```python
analyzer = FactorAnalyzer()
ic = analyzer.calculate_ic(factor_values, returns)
rank_ic = analyzer.calculate_rank_ic(factor_values, returns)
```

## Data Configuration

### DataConfigLoader (engine/production/data_config.py)

**Configuration Format:**
```json
{
    "close": {
        "table_name": "sync_daily_data",
        "column_name": "close",
        "extra_config": {
            "adjust_method": "forward"
        }
    },
    "volume": {
        "table_name": "sync_daily_data",
        "column_name": "vol"
    }
}
```

**Stored in:** `factor_data_config` table

**Usage:**
```python
loader = DataConfigLoader(db_client)
config = loader.load()

# Get table and column for a field
table = config["close"]["table_name"]
column = config["close"]["column_name"]
```

## Preprocessing Options

**Default Preprocessing:**
```python
DEFAULT_PREPROCESS = {
    "adjust_price": "forward",      # forward/backward 复权
    "filter_st": True,              # Filter ST stocks
    "filter_new_stock": True,       # Filter new stocks
    "new_stock_days": 60,           # IPO exclusion period
    "handle_suspension": True,      # Handle suspension
    "mark_limit": True,             # Mark limit moves
}
```

**Priority Order:**
1. Explicit parameter (highest)
2. DB factor_metadata.params.preprocess
3. Code params.preprocess
4. Global DEFAULT_PREPROCESS (lowest)

## Example: Creating a Custom Factor

```python
from engine.production.registry import factor
import polars as pl

@factor(
    factor_id="rsi_14",
    factor_name="RSI 14-day",
    depends_on=["close"],
    params={"window": 14},
    mode="incremental"
)
def compute_rsi_14(df: pl.DataFrame, params: dict) -> pl.Series:
    """Compute 14-day RSI"""
    from engine.factors.technical import TechnicalFactors
    return TechnicalFactors.rsi(df["close"], params["window"])
```

## Performance Optimization

### Vectorization
- All computations use Polars (not loops)
- Batch operations on entire DataFrame
- Typical performance: 1M rows in < 1 second

### Incremental Computation
- Only compute new dates
- Reuse historical data
- Lookback window for rolling calculations

### Caching
- Factor registry cached in memory
- Data config cached with TTL
- Trading calendar cached

## Related Codemaps

- [Data Layer](./data.md) - Data loading
- [API Routes](./api.md) - Production endpoints
- [Service Layer](./services.md) - Business logic
