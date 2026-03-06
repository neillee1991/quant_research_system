# Developer Guide

**Last Updated:** 2026-03-03
**Target Audience:** Backend developers

## Table of Contents

1. [Development Setup](#development-setup)
2. [Architecture Overview](#architecture-overview)
3. [Common Tasks](#common-tasks)
4. [Code Patterns](#code-patterns)
5. [Testing](#testing)
6. [Debugging](#debugging)
7. [Performance Tips](#performance-tips)

## Development Setup

### Initial Setup

```bash
# Clone and navigate
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env with your settings
```

### IDE Configuration

**VS Code:**
```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": true,
  "python.formatting.provider": "black",
  "[python]": {
    "editor.formatOnSave": true,
    "editor.defaultFormatter": "ms-python.python"
  }
}
```

### Running Development Server

```bash
# With auto-reload
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Or use main.py
python main.py
```

## Architecture Overview

### Layered Architecture

```
┌─────────────────────────────────────────┐
│         API Layer (FastAPI)             │
│  Routes: /api/v1/data, /factor, etc.   │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│      Service Layer (Business Logic)     │
│  DataService, FactorService, etc.      │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│    Engine Layer (Computation)           │
│  ProductionEngine, BacktestEngine       │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│      Data Layer (Storage)               │
│  DolphinDB, Polars DataFrames           │
└─────────────────────────────────────────┘
```

### Key Design Patterns

**Dependency Injection:**
```python
from app.core.container import container

# Get service instance
data_service = container.get_data_service()
factor_service = container.get_factor_service()
```

**Repository Pattern:**
```python
# Services depend on abstract interfaces, not concrete implementations
class DataService:
    def __init__(self, db_client):
        self.db = db_client

    def get_daily_data(self, ts_code, start_date, end_date):
        return self.db.query(
            "SELECT * FROM sync_daily_data WHERE ts_code = %s AND trade_date BETWEEN %s AND %s",
            (ts_code, start_date, end_date)
        )
```

**Factory Pattern:**
```python
# Factor registry creates factor instances
from engine.production.registry import get_factor

definition = get_factor("ma20")
result = definition.func(df, definition.params)
```

## Common Tasks

### Task 1: Add a New Technical Indicator

**Step 1:** Add to `engine/factors/technical.py`

```python
class TechnicalFactors:
    @staticmethod
    def my_indicator(series: pl.Series, param1: int) -> pl.Series:
        """My custom indicator"""
        # Implementation using Polars
        return series.rolling_mean(window_size=param1)
```

**Step 2:** Create factor wrapper in `engine/factors/`

```python
from engine.production.registry import factor
from engine.factors.technical import TechnicalFactors

@factor(
    factor_id="my_indicator",
    factor_name="My Indicator",
    depends_on=["close"],
    params={"param1": 20},
    mode="incremental"
)
def compute_my_indicator(df: pl.DataFrame, params: dict) -> pl.Series:
    return TechnicalFactors.my_indicator(df["close"], params["param1"])
```

**Step 3:** Test the factor

```python
# In tests/test_my_indicator.py
import polars as pl
from engine.factors.technical import TechnicalFactors

def test_my_indicator():
    data = pl.Series([1, 2, 3, 4, 5])
    result = TechnicalFactors.my_indicator(data, 2)
    assert len(result) == len(data)
    assert result[0] is None or result[0] == 1.0
```

### Task 2: Add a New API Endpoint

**Step 1:** Create route in `app/api/v1/my_module.py`

```python
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from app.core.container import container
from app.core.exceptions import DataNotFoundError

router = APIRouter()

class MyRequest(BaseModel):
    param1: str
    param2: int

class MyResponse(BaseModel):
    success: bool
    data: dict

@router.post("/my-endpoint")
async def my_endpoint(request: MyRequest) -> MyResponse:
    """My endpoint description"""
    try:
        service = container.get_data_service()
        result = service.do_something(request.param1, request.param2)
        return MyResponse(success=True, data=result)
    except Exception as e:
        raise DataNotFoundError(f"Error: {e}")
```

**Step 2:** Register in `app/main.py`

```python
from app.api.v1 import my_module

app.include_router(
    my_module.router,
    prefix=settings.api_v1_prefix,
    tags=["my-module"]
)
```

**Step 3:** Test the endpoint

```bash
curl -X POST http://localhost:8000/api/v1/my-endpoint \
  -H "Content-Type: application/json" \
  -d '{"param1": "value", "param2": 10}'
```

### Task 3: Query Data from DolphinDB

**Basic Query:**
```python
from store.dolphindb_client import db_client

# Query with parameters
df = db_client.query(
    "SELECT * FROM sync_daily_data WHERE ts_code = %s AND trade_date >= %s",
    ("000001.SZ", "20240101")
)
```

**Upsert Data:**
```python
import polars as pl

# Create DataFrame
data = pl.DataFrame({
    "ts_code": ["000001.SZ", "000001.SZ"],
    "trade_date": ["20260303", "20260304"],
    "factor_id": ["ma20", "ma20"],
    "factor_value": [100.5, 101.2]
})

# Upsert to database
db_client.upsert(
    "factor_values",
    data,
    ["ts_code", "trade_date", "factor_id"]
)
```

**Complex Query:**
```python
# Multi-table join
df = db_client.query("""
    SELECT
        d.ts_code,
        d.trade_date,
        d.close,
        a.adj_factor,
        s.is_st
    FROM sync_daily_data d
    LEFT JOIN sync_adj_factor a ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date
    LEFT JOIN stock_daily_status s ON d.ts_code = s.ts_code AND d.trade_date = s.trade_date
    WHERE d.ts_code = %s AND d.trade_date >= %s
    ORDER BY d.trade_date
""", ("000001.SZ", "20240101"))
```

### Task 4: Run Factor Computation

**Programmatically:**
```python
from engine.production.engine import ProductionEngine
from store.dolphindb_client import db_client

engine = ProductionEngine(db_client)
success = engine.run_task(
    factor_id="ma20",
    target_date="20260303",
    mode="incremental"
)
```

**Via API:**
```bash
curl -X POST http://localhost:8000/api/v1/production/run \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "ma20",
    "target_date": "20260303",
    "mode": "incremental"
  }'
```

### Task 5: Debug Data Issues

**Check Data Availability:**
```python
from store.dolphindb_client import db_client

# Check if data exists
df = db_client.query(
    "SELECT COUNT(*) as cnt FROM sync_daily_data WHERE ts_code = %s",
    ("000001.SZ",)
)
print(f"Total records: {df['cnt'][0]}")

# Check date range
df = db_client.query(
    "SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date FROM sync_daily_data WHERE ts_code = %s",
    ("000001.SZ",)
)
print(f"Date range: {df['min_date'][0]} to {df['max_date'][0]}")
```

**Check Sync Status:**
```python
# View sync logs
df = db_client.query("SELECT * FROM sync_log ORDER BY sync_time DESC LIMIT 10")
print(df)

# Check sync task config
df = db_client.query("SELECT * FROM sync_task_config WHERE enabled = true")
print(df)
```

## Code Patterns

### Pattern 1: Immutable Data Processing

**WRONG:**
```python
def process_data(df):
    df["new_col"] = df["col1"] + df["col2"]  # Mutates original
    return df
```

**CORRECT:**
```python
def process_data(df):
    return df.with_columns(
        (pl.col("col1") + pl.col("col2")).alias("new_col")
    )
```

### Pattern 2: Error Handling

**WRONG:**
```python
def get_data(ts_code):
    try:
        return db_client.query("SELECT * FROM sync_daily_data WHERE ts_code = %s", (ts_code,))
    except:
        return None  # Silently swallow error
```

**CORRECT:**
```python
from app.core.exceptions import DataNotFoundError

def get_data(ts_code):
    try:
        df = db_client.query(
            "SELECT * FROM sync_daily_data WHERE ts_code = %s",
            (ts_code,)
        )
        if df.is_empty():
            raise DataNotFoundError(f"No data found for {ts_code}")
        return df
    except Exception as e:
        logger.error(f"Failed to get data for {ts_code}: {e}")
        raise
```

### Pattern 3: Vectorized Computation

**WRONG:**
```python
def compute_ma(series, window):
    result = []
    for i in range(len(series)):
        if i < window:
            result.append(None)
        else:
            result.append(sum(series[i-window:i]) / window)
    return result
```

**CORRECT:**
```python
def compute_ma(series: pl.Series, window: int) -> pl.Series:
    return series.rolling_mean(window_size=window, min_periods=1)
```

### Pattern 4: Configuration Access

**WRONG:**
```python
import os
host = os.getenv("DOLPHINDB_HOST", "localhost")
port = int(os.getenv("DOLPHINDB_PORT", "8848"))
```

**CORRECT:**
```python
from app.core.config import settings
host = settings.database.dolphindb_host
port = settings.database.dolphindb_port
```

## Testing

### Unit Tests

```python
# tests/test_technical_factors.py
import polars as pl
from engine.factors.technical import TechnicalFactors

def test_sma():
    data = pl.Series([1, 2, 3, 4, 5])
    result = TechnicalFactors.sma(data, 2)
    assert len(result) == 5
    assert result[1] == 1.5  # (1+2)/2

def test_rsi():
    data = pl.Series([44, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08])
    result = TechnicalFactors.rsi(data, 14)
    assert len(result) == 10
    assert 0 <= result[-1] <= 100
```

### Integration Tests

```python
# tests/test_production_engine.py
from engine.production.engine import ProductionEngine
from store.dolphindb_client import db_client

def test_run_task():
    engine = ProductionEngine(db_client)
    success = engine.run_task(
        factor_id="ma20",
        target_date="20260303",
        mode="incremental"
    )
    assert success is True
```

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=app --cov=engine --cov=data_manager tests/

# Run specific test
pytest tests/test_technical_factors.py::test_sma -v

# Run with markers
pytest -m "not slow" tests/
```

## Debugging

### Using Logging

```python
from app.core.logger import logger

logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
logger.critical("Critical message")
```

### Using Debugger

**VS Code:**
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python: FastAPI",
      "type": "python",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:app", "--reload"],
      "jinja": true,
      "justMyCode": true
    }
  ]
}
```

### Inspecting Data

```python
import polars as pl

df = pl.DataFrame({
    "col1": [1, 2, 3],
    "col2": [4, 5, 6]
})

# Print shape
print(df.shape)  # (3, 2)

# Print schema
print(df.schema)

# Print first rows
print(df.head())

# Print statistics
print(df.describe())

# Check for nulls
print(df.null_count())
```

## Performance Tips

### 1. Use Polars Instead of Pandas

```python
# SLOW: Pandas
import pandas as pd
df = pd.read_csv("data.csv")
df["new_col"] = df["col1"] + df["col2"]

# FAST: Polars
import polars as pl
df = pl.read_csv("data.csv")
df = df.with_columns((pl.col("col1") + pl.col("col2")).alias("new_col"))
```

### 2. Vectorize Operations

```python
# SLOW: Loop
result = []
for i in range(len(data)):
    result.append(data[i] * 2)

# FAST: Vectorized
result = data * 2
```

### 3. Use Incremental Computation

```python
# Load only new data
last_date = db_client.query(
    "SELECT MAX(trade_date) as max_date FROM factor_values WHERE factor_id = %s",
    ("ma20",)
)["max_date"][0]

# Compute only from last_date onwards
engine.run_task(
    factor_id="ma20",
    start_date=last_date,
    mode="incremental"
)
```

### 4. Batch Database Operations

```python
# SLOW: Individual inserts
for row in data:
    db_client.execute(f"INSERT INTO table VALUES ({row})")

# FAST: Batch upsert
db_client.upsert("table", df, ["primary_key"])
```

### 5. Cache Expensive Computations

```python
from functools import lru_cache

@lru_cache(maxsize=128)
def get_trading_calendar():
    return db_client.query("SELECT * FROM trade_cal")
```

## Common Issues

### Issue 1: DolphinDB Connection Timeout

**Symptom:** `ConnectionError: 无法连接 DolphinDB`

**Solution:**
```bash
# Check if DolphinDB is running
docker ps | grep dolphindb

# Restart if needed
docker-compose restart dolphindb

# Check connection settings in .env
DOLPHINDB__HOST=localhost
DOLPHINDB__PORT=8848
```

### Issue 2: SQL Syntax Error

**Symptom:** `DolphinDB SQL error: syntax error`

**Solution:**
- Use `%s` placeholders (not `?`)
- Use DolphinDB functions (not PostgreSQL)
- Check table names are registered

### Issue 3: Out of Memory

**Symptom:** `MemoryError` or process killed

**Solution:**
- Use date ranges to limit data
- Process in chunks
- Use Polars lazy evaluation

### Issue 4: Slow Queries

**Symptom:** Query takes > 10 seconds

**Solution:**
- Add date range filter
- Filter by ts_code early
- Check if indexes exist
- Use LIMIT for testing

## Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Polars Documentation](https://docs.pola-rs.com/)
- [DolphinDB Documentation](https://www.dolphindb.com/docs/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Prefect Documentation](https://docs.prefect.io/)

## Getting Help

1. Check logs: `backend/logs/app.log`
2. Review API docs: http://localhost:8000/docs
3. Check codemaps: `docs/CODEMAPS/`
4. Ask in team chat or create issue
