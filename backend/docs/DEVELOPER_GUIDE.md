# Developer Guide

**Last Updated:** 2026-03-07
**Version:** v2.0 (重构后)
**Target Audience:** Backend developers

## Table of Contents

1. [Development Setup](#development-setup)
2. [Architecture Overview](#architecture-overview)
3. [Working with Refactored Modules](#working-with-refactored-modules)
4. [Common Tasks](#common-tasks)
5. [Code Patterns](#code-patterns)
6. [Testing](#testing)
7. [Debugging](#debugging)
8. [Performance Tips](#performance-tips)

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

详细架构请参考 [ARCHITECTURE.md](./ARCHITECTURE.md)

```
API Layer (FastAPI)
  ↓
Service Layer (业务逻辑)
  ↓
Engine Layer (计算引擎)
  ↓
Data Layer (数据访问)
  ↓
Storage Layer (DolphinDB)
```

### Key Design Patterns

**Repository Pattern:**
```python
# DolphinDBClient 作为统一的数据访问接口
from store.dolphindb import DolphinDBClient

client = DolphinDBClient()
df = client.query(
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

**Facade Pattern:**
```python
# DolphinDBClient 作为 Facade，隐藏内部复杂性
class DolphinDBClient:
    def __init__(self):
        self._connection = DolphinDBConnection()
        self._query_builder = QueryBuilder(self._connection)
        self._meta_manager = MetadataManager(self._connection)
        # ... 其他组件
```

---

## Working with Refactored Modules

### 1. DolphinDB Client (重构后)

**位置**: `store/dolphindb/`

**模块结构**:
```
dolphindb/
├── __init__.py           # 客户端入口 (Facade)
├── connection.py         # 连接管理 (Singleton)
├── query_builder.py      # 查询构建
├── meta_manager.py       # 元数据管理
├── seed_data.py          # 数据初始化
└── data_operations.py    # 数据操作
```

**使用示例**:
```python
from store.dolphindb import DolphinDBClient

# 创建客户端实例
client = DolphinDBClient()

# 查询数据
df = client.query(
    "SELECT * FROM sync_daily_data WHERE ts_code = %s LIMIT 100",
    ("000001.SZ",)
)

# 执行 SQL (无返回值)
client.execute(
    "UPDATE factor_metadata SET enabled = %s WHERE factor_id = %s",
    (True, "ma20")
)

# 检查表是否存在
if client.table_exists("factor_values"):
    print("Table exists")

# 创建表
client.create_table(
    table_name="my_table",
    schema={
        "id": {"type": "INT", "nullable": False},
        "value": {"type": "DOUBLE", "nullable": True}
    },
    primary_keys=["id"]
)
```

**关键改进**:
- ✅ 单一职责：每个模块职责清晰
- ✅ 线程安全：连接管理使用单例模式
- ✅ SQL 安全：参数化查询防止注入
- ✅ 易于测试：可以 Mock 各个组件

### 2. Data API (重构后)

**位置**: `app/api/v1/data/`

**模块结构**:
```
data/
├── __init__.py           # 路由聚合
├── query_api.py          # 数据查询 (6 个端点)
├── sync_api.py           # 数据同步 (18 个端点)
├── config_api.py         # 配置管理 (5 个端点)
└── etl_api.py            # ETL 任务 (10 个端点)
```

**添加新端点**:
```python
# 在对应的模块中添加
# 例如：query_api.py

from fastapi import APIRouter, HTTPException
from app.core.response import success_response, error_response

router = APIRouter()

@router.get("/my-endpoint")
async def my_endpoint(param: str):
    try:
        # 业务逻辑
        result = do_something(param)
        return success_response(result)
    except Exception as e:
        logger.error(f"Error: {e}")
        return error_response(str(e))
```

**端点分类指南**:
- `query_api.py`: 数据查询、表管理
- `sync_api.py`: 数据同步、调度器
- `config_api.py`: 任务配置 CRUD
- `etl_api.py`: ETL 任务管理

### 3. Production API (重构后)

**位置**: `app/api/v1/production/`

**模块结构**:
```
production/
├── __init__.py           # 路由聚合
├── factor_analysis.py    # 因子分析 (6 个端点)
├── factor_compute.py     # 因子计算 (4 个端点)
├── factor_registry.py    # 因子注册 (8 个端点)
└── factor_config.py      # 配置管理 (8 个端点)
```

**使用 ProductionEngine**:
```python
from engine.production.engine import ProductionEngine
from engine.production.registry import get_factor

# 获取因子定义
definition = get_factor("ma20")

# 创建引擎实例
engine = ProductionEngine()

# 运行因子计算
result = await engine.run_task(
    factor_id="ma20",
    start_date="20240101",
    end_date="20240131",
    mode="incremental",
    preprocess_options={
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True
    }
)
```

### 4. DataProcessor (预处理)

**位置**: `data_manager/processor.py`

**使用示例**:
```python
from data_manager.processor import DataProcessor

processor = DataProcessor()

# 应用预处理
df_processed = processor.preprocess(
    df=raw_df,
    options={
        "adjust_price": "forward",   # 前复权
        "filter_st": True,           # 过滤 ST
        "filter_new_stock": True,    # 过滤新股
        "handle_suspension": True,   # 停牌处理
        "mark_limit": True           # 标记涨跌停
    }
)
```

**自定义预处理器**:
```python
class MyProcessor(DataProcessor):
    def custom_filter(self, df: pl.DataFrame) -> pl.DataFrame:
        """自定义过滤逻辑"""
        return df.filter(pl.col("volume") > 1000000)

    def preprocess(self, df: pl.DataFrame, options: dict) -> pl.DataFrame:
        df = super().preprocess(df, options)
        if options.get("custom_filter"):
            df = self.custom_filter(df)
        return df
```

## Common Tasks

### Task 1: Add a New Technical Indicator

**Step 1:** Add to `engine/factors/technical.py`

```python
class TechnicalFactors:
    @staticmethod
    def my_indicator(series: pl.Series, param1: int) -> pl.Series:
        """My custom indicator

        Args:
            series: 价格序列
            param1: 参数1

        Returns:
            计算结果序列
        """
        # 使用 Polars 向量化操作
        return series.rolling_mean(window_size=param1)
```

**Step 2:** Create factor wrapper in `engine/factors/`

```python
from engine.production.registry import factor
from engine.factors.technical import TechnicalFactors

@factor(
    factor_id="my_indicator",
    factor_name="My Indicator",
    depends_on=["close"],  # 依赖的数据字段
    params={"param1": 20},  # 默认参数
    mode="incremental"  # 增量计算模式
)
def compute_my_indicator(df: pl.DataFrame, params: dict) -> pl.Series:
    """计算自定义指标

    Args:
        df: 包含 close 列的 DataFrame
        params: 参数字典

    Returns:
        因子值序列
    """
    return TechnicalFactors.my_indicator(df["close"], params["param1"])
```

**Step 3:** Test the factor

```python
# In tests/test_my_indicator.py
import polars as pl
from engine.factors.technical import TechnicalFactors

def test_my_indicator():
    data = pl.Series([1, 2, 3, 4, 5])
    result = TechnicalFactors.my_indicator(data, param1=2)
    assert len(result) == len(data)
    assert result[-1] == 4.5  # (4 + 5) / 2
```

**Step 4:** 通过 API 添加到数据库

```bash
curl -X POST "http://localhost:8000/api/v1/production/factors" \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "my_indicator",
    "factor_name": "My Indicator",
    "category": "technical",
    "description": "自定义技术指标",
    "depends_on": ["close"],
    "params": {"param1": 20},
    "enabled": true
  }'
```

### Task 2: Add a New Data Sync Task

**Step 1:** 在数据库添加任务配置

```python
from store.dolphindb import DolphinDBClient

client = DolphinDBClient()

# 插入任务配置
client.execute("""
    INSERT INTO sync_task_config VALUES (
        'my_data_source',
        'My Data Source',
        'tushare',
        'incremental',
        'sync_my_data',
        true,
        '{"api_name": "my_api", "fields": ["field1", "field2"]}',
        now(),
        now()
    )
""")
```

**Step 2:** 创建 Prefect Flow

```python
# In flows/my_data_sync_flow.py
from prefect import flow, task
from data_manager.refactored_sync_engine import SyncEngine

@task
def sync_my_data(start_date: str, end_date: str):
    """同步自定义数据"""
    engine = SyncEngine()
    return engine.run_task(
        task_id="my_data_source",
        start_date=start_date,
        end_date=end_date
    )

@flow(name="my-data-sync")
def my_data_sync_flow(start_date: str, end_date: str):
    """自定义数据同步流程"""
    result = sync_my_data(start_date, end_date)
    return result
```

**Step 3:** 注册和调度

```bash
# 部署 Flow
python -m flows.my_data_sync_flow

# 通过 API 触发
curl -X POST "http://localhost:8000/api/v1/data/sync/task/my_data_source"
```

### Task 3: Add a New API Endpoint

**Step 1:** 选择合适的模块

- 数据查询 → `data/query_api.py`
- 数据同步 → `data/sync_api.py`
- 因子分析 → `production/factor_analysis.py`
- 因子计算 → `production/factor_compute.py`

**Step 2:** 添加端点

```python
# 例如：在 data/query_api.py 添加
from fastapi import APIRouter, Query
from app.core.response import success_response, error_response
from app.core.logger import logger

router = APIRouter()

@router.get("/my-endpoint")
async def my_endpoint(
    param1: str = Query(..., description="参数1"),
    param2: int = Query(10, description="参数2")
):
    """
    我的自定义端点

    Args:
        param1: 必需参数
        param2: 可选参数，默认 10

    Returns:
        成功响应或错误响应
    """
    try:
        # 业务逻辑
        result = do_something(param1, param2)
        return success_response(result)
    except Exception as e:
        logger.error(f"Error in my_endpoint: {e}", exc_info=True)
        return error_response(str(e))
```

**Step 3:** 确保路由已注册

```python
# 在 data/__init__.py 中
from .query_api import router as query_router

# 路由应该已经包含在 router 中
```

### Task 4: 扩展 DataProcessor

**Step 1:** 创建自定义处理器

```python
# In data_manager/custom_processor.py
from data_manager.processor import DataProcessor
import polars as pl

class CustomProcessor(DataProcessor):
    """自定义数据处理器"""

    def filter_by_liquidity(self, df: pl.DataFrame, min_volume: int) -> pl.DataFrame:
        """按流动性过滤

        Args:
            df: 输入数据
            min_volume: 最小成交量

        Returns:
            过滤后的数据
        """
        return df.filter(pl.col("volume") >= min_volume)

    def normalize_price(self, df: pl.DataFrame) -> pl.DataFrame:
        """价格标准化

        Args:
            df: 输入数据

        Returns:
            标准化后的数据
        """
        return df.with_columns([
            ((pl.col("close") - pl.col("close").mean()) / pl.col("close").std()).alias("close_norm")
        ])

    def preprocess(self, df: pl.DataFrame, options: dict) -> pl.DataFrame:
        """扩展预处理流程

        Args:
            df: 输入数据
            options: 预处理选项

        Returns:
            处理后的数据
        """
        # 先执行基础预处理
        df = super().preprocess(df, options)

        # 执行自定义处理
        if options.get("filter_liquidity"):
            df = self.filter_by_liquidity(df, options["min_volume"])

        if options.get("normalize"):
            df = self.normalize_price(df)

        return df
```

**Step 2:** 使用自定义处理器

```python
from data_manager.custom_processor import CustomProcessor

processor = CustomProcessor()

df_processed = processor.preprocess(
    df=raw_df,
    options={
        "adjust_price": "forward",
        "filter_st": True,
        "filter_liquidity": True,
        "min_volume": 1000000,
        "normalize": True
    }
)
```

### Task 5: 配置字段映射

**场景**: 不同数据源的字段名不一致

**Step 1:** 更新字段映射配置

```python
from store.dolphindb import DolphinDBClient

client = DolphinDBClient()

# 更新配置
client.execute("""
    UPDATE factor_data_config
    SET field_mapping = '{"close": "close_price", "volume": "vol", "amount": "turnover"}'
    WHERE config_id = 'default'
""")
```

**Step 2:** 或通过 API 更新

```bash
curl -X PUT "http://localhost:8000/api/v1/production/data-config" \
  -H "Content-Type: application/json" \
  -d '{
    "field_mapping": {
      "close": "close_price",
      "volume": "vol",
      "amount": "turnover"
    }
  }'
```

**Step 3:** 在因子计算中使用

```python
from engine.production.data_config import DataConfigLoader

config_loader = DataConfigLoader()
config = config_loader.load_config()

# 获取映射后的字段名
close_field = config.get_mapped_field("close")  # 返回 "close_price"
```esult = TechnicalFactors.my_indicator(data, 2)
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
