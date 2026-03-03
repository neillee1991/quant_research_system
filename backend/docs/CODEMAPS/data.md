# Data Layer Codemap

**Last Updated:** 2026-03-03
**Entry Points:** store/dolphindb_client.py, data_manager/

## Architecture

```
DolphinDB (Time-Series Database)
├── dfs://quant_ts (TSDB - Time-Series Data)
│   ├─ sync_daily_data        (OHLCV data)
│   ├─ sync_daily_basic       (Close + indicators)
│   ├─ sync_adj_factor        (Price adjustment factors)
│   ├─ sync_index_daily       (Index data)
│   ├─ sync_moneyflow         (Money flow data)
│   └─ factor_values          (Computed factor results)
│
└── dfs://quant_meta (Metadata - Dimension Tables)
    ├─ stock_basic            (Stock master data)
    ├─ factor_metadata        (Factor definitions)
    ├─ factor_data_config     (Field mappings)
    ├─ sync_task_config       (Sync task definitions)
    ├─ sync_log               (Current sync status)
    ├─ sync_log_history       (Historical sync logs)
    ├─ trade_cal              (Trading calendar)
    ├─ production_task_run    (Factor computation logs)
    ├─ dag_run_log            (Prefect flow logs)
    └─ stock_daily_status     (ST, suspension, limit flags)
```

## Database Client

### DolphinDBClient (store/dolphindb_client.py)

**Singleton Pattern:** Thread-safe single connection instance

**Key Methods:**

| Method | Purpose |
|--------|---------|
| `query(sql, params)` | Execute SELECT query |
| `upsert(table, df, keys)` | Insert or update records |
| `execute(sql)` | Execute DML/DDL |
| `ensure_meta_tables()` | Create dimension tables |
| `seed_sync_task_config()` | Initialize default sync tasks |
| `seed_factor_data_config()` | Initialize field mappings |

**Connection Management:**
```python
from store.dolphindb_client import db_client

# Auto-reconnect on connection loss
df = db_client.query("SELECT * FROM sync_daily_data WHERE ts_code = %s", ("000001.SZ",))
```

**SQL Syntax Adaptation:**
- Converts PostgreSQL `%s` placeholders to DolphinDB literals
- Auto-converts YYYYMMDD dates to DolphinDB format (YYYY.MM.DD)
- Handles bare table names → `loadTable()` calls
- Lowercases SQL functions for DolphinDB compatibility

**Data Type Mapping:**
```python
Python → DolphinDB
str → STRING
int → INT/LONG
float → DOUBLE
datetime → TIMESTAMP
date → DATE
bool → BOOL
None → NULL
```

## Sync Engine

### SyncTaskExecutor (data_manager/sync_components.py)

**Purpose:** Orchestrate data synchronization from Tushare API

**Workflow:**
```
1. Load task config from DolphinDB
2. Determine sync type (incremental/full)
3. Fetch data from Tushare API
4. Transform & validate
5. Upsert to DolphinDB
6. Update sync log
```

**Key Components:**

| Component | Purpose |
|-----------|---------|
| `SyncConfigManager` | Load task configs with caching |
| `SyncLogManager` | Track last sync dates |
| `TableManager` | Auto-create tables with schemas |
| `TushareAPIClient` | Rate-limited API calls |
| `SyncTaskExecutor` | Orchestrate sync execution |

**Sync Types:**
- `incremental` - Continue from last sync date
- `full` - Complete replacement

**Rate Limiting:**
- Default: 120 calls/minute (Tushare limit)
- Configurable via `COLLECTOR__CALLS_PER_MINUTE`
- Automatic retry with exponential backoff

### DataProcessor (data_manager/processor.py)

**Purpose:** Data transformation and preprocessing

**Key Methods:**

| Method | Purpose |
|--------|---------|
| `adjust_prices()` | Apply 复权 (forward/backward) |
| `filter_st_stocks()` | Remove ST stocks |
| `filter_new_stocks()` | Remove stocks < 60 days old |
| `handle_suspension()` | Mark suspension periods |
| `mark_limit_moves()` | Flag limit-up/limit-down |

**Preprocessing Pipeline:**
```python
processor = DataProcessor(db_client)
df = processor.adjust_prices(df, method="forward")
df = processor.filter_st_stocks(df)
df = processor.filter_new_stocks(df, days=60)
df = processor.handle_suspension(df)
df = processor.mark_limit_moves(df)
```

## Data Configuration

### DataConfigLoader (engine/production/data_config.py)

**Purpose:** Manage field mappings and data source configuration

**Configuration Structure:**
```python
{
    "field_name": {
        "table_name": "sync_daily_data",
        "column_name": "close",
        "extra_config": {
            "adjust_method": "forward",
            "price_table": "sync_daily_data"
        }
    }
}
```

**Stored in:** `factor_data_config` table

**Usage:**
```python
config_loader = DataConfigLoader(db_client)
config = config_loader.load()
table_name = config["close"]["table_name"]
```

## Key Tables

### TSDB Tables (dfs://quant_ts)

#### sync_daily_data
```
Columns: ts_code, trade_date, open, high, low, close, vol, amount, pct_chg
Partition: ts_code (hash), trade_date (range)
Primary Key: (ts_code, trade_date)
```

#### sync_daily_basic
```
Columns: ts_code, trade_date, close, turnover_rate, pe, pb, ...
Partition: ts_code (hash), trade_date (range)
Primary Key: (ts_code, trade_date)
```

#### factor_values
```
Columns: ts_code, trade_date, factor_id, factor_value, quality_flag
Partition: factor_id (hash), trade_date (range)
Primary Key: (ts_code, trade_date, factor_id)
```

#### sync_adj_factor
```
Columns: ts_code, trade_date, adj_factor
Partition: ts_code (hash), trade_date (range)
Primary Key: (ts_code, trade_date)
```

### Metadata Tables (dfs://quant_meta)

#### factor_metadata
```
Columns: factor_id, factor_name, description, code, params, created_at
Primary Key: factor_id
```

#### sync_task_config
```
Columns: task_id, task_name, data_source, sync_type, table_name,
         primary_keys, enabled, created_at
Primary Key: task_id
```

#### stock_daily_status
```
Columns: ts_code, trade_date, is_st, is_suspend, is_limit_up, is_limit_down
Primary Key: (ts_code, trade_date)
```

#### trade_cal
```
Columns: trade_date, is_open
Primary Key: trade_date
```

## Query Examples

### Query Daily Data
```python
df = db_client.query(
    "SELECT * FROM sync_daily_data WHERE ts_code = %s AND trade_date >= %s",
    ("000001.SZ", "20240101")
)
```

### Upsert Factor Results
```python
db_client.upsert(
    "factor_values",
    factor_df,
    ["ts_code", "trade_date", "factor_id"]
)
```

### Query Factor Results
```python
df = db_client.query(
    "SELECT * FROM factor_values WHERE factor_id = %s AND trade_date = %s",
    ("ma20", "20260303")
)
```

### Get Last Sync Date
```python
df = db_client.query(
    "SELECT MAX(trade_date) as last_date FROM sync_daily_data WHERE ts_code = %s",
    ("000001.SZ",)
)
last_date = df["last_date"][0] if not df.is_empty() else None
```

## Performance Considerations

### Partitioning Strategy
- TSDB tables partitioned by stock code (hash) + date (range)
- Enables efficient time-range queries
- Supports incremental data loading

### Indexing
- Primary keys automatically indexed
- Trade date indexed for range queries
- Stock code indexed for filtering

### Query Optimization
- Use date ranges to limit data scans
- Filter by ts_code early in WHERE clause
- Avoid full table scans

## Related Codemaps

- [API Routes](./api.md) - Data endpoints
- [Factor Engine](./factors.md) - Data consumption
- [Production Engine](./production.md) - Computation orchestration
