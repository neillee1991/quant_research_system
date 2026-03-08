# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quant Research System is a full-stack quantitative trading platform with drag-and-drop strategy modeling, vectorized backtesting, and AutoML capabilities. The system uses DolphinDB for time-series data storage, Polars for data processing, Prefect 3.x for orchestration, VectorBT for backtesting, and React Flow for visual strategy design.

## Development Commands

### Quick Start (Recommended)

```bash
# Start all services (database, backend, frontend)
./start.sh

# Check service status
./check_status.sh

# Stop all services
./stop.sh
```

### Backend (Python 3.11 required)

```bash
cd backend
~/.pyenv/versions/3.11.9/bin/python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run development server
python main.py
# or:
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Frontend (React + TypeScript)

```bash
cd frontend
npm install
npm start  # Runs on http://localhost:3000
```

### Database (DolphinDB via Docker)

```bash
docker-compose up -d
cd backend
python database/init_dolphindb.py
# DolphinDB Web UI: http://localhost:8848
```

## Architecture & Key Concepts

### Database Layer: DolphinDB

**Critical**: This project uses DolphinDB as the sole data store.

- Two databases: `dfs://quant_ts` (TSDB partitioned tables) and `dfs://quant_meta` (dimension tables)
- TSDB tables: `daily_data`, `daily_basic`, `adj_factor`, `index_daily`, `moneyflow`, `factor_values`
- Dimension tables: `sync_log`, `sync_log_history`, `stock_basic`, `factor_metadata`, `factor_analysis`, `dag_run_log`, `dag_task_log`, `production_task_run`, `trade_cal`, `sync_task_config`, `factor_data_config`
- Bare table names are auto-resolved to `loadTable()` calls in `_adapt_sql_syntax()`

The database client is a singleton: `from store.dolphindb_client import db_client`

### Configuration System (Pydantic-based)

```python
from app.core.config import settings

settings.dolphindb.host
settings.collector.tushare_token
settings.backtest.initial_capital
```

Environment variables use double underscore for nesting:
- `DOLPHINDB__HOST=localhost`
- `COLLECTOR__CALLS_PER_MINUTE=120`

### Configuration Management

**Direct Update Mode**: Configuration updates directly overwrite existing values without version history.

**Best Practices**:
- Export configurations before updates for manual backup
- Use Git to version control important configuration files
- Schedule regular database backups
- Test configuration changes in development environment first

**Configuration Tables**:
- `sync_task_config` - Data sync task configurations
- `factor_data_config` - Factor field mapping configurations
- All configs include `updated_at` timestamp for tracking changes

### Data Sync Engine (Database-Driven)

The sync system reads task definitions from the DolphinDB `sync_task_config` dimension table:

- `SyncConfigManager`: Loads task configs from DolphinDB with in-memory cache
- `SyncLogManager`: Tracks last sync dates for incremental syncs
- `TableManager`: Auto-creates tables with schemas from config
- `TushareAPIClient`: Rate-limited API calls with retry logic
- `SyncTaskExecutor`: Orchestrates sync execution

### Service Layer Architecture

```python
from app.core.container import container

data_service = container.get_data_service()
factor_service = container.get_factor_service()
backtest_service = container.get_backtest_service()
```

### API Route Structure

All routes are under `/api/v1/`:
- `/data/*` - Data queries and sync operations
- `/factor/*` - Technical indicator calculations
- `/strategy/*` - Backtest execution
- `/ml/*` - AutoML model training
- `/production/*` - Production factor management

**Important**: The `/data/daily` endpoint queries `daily_basic` table (not `daily_data`), which contains close price + indicators (PE, PB, turnover_rate) but NOT full OHLC data.

**Configuration Updates**: All configuration update endpoints use direct overwrite mode. Previous versions are not retained. Always export configurations before making changes.

### Exception Handling

Custom exception hierarchy in `app/core/exceptions.py`:
- `QuantException` (base)
  - `DataException` → `DataNotFoundError`, `DataValidationError`
  - `SyncException` → `SyncTaskNotFoundError`, `RateLimitExceededError`
  - `BacktestException`, `FactorException`, `MLException`

### Factor Engine (Polars-based)

Technical indicators in `engine/factors/`:
- Time-series factors: MA, EMA, RSI, MACD, KDJ, Bollinger Bands, ATR
- Cross-sectional factors: Rank, Z-Score, Industry neutralization

### Production Factor Framework (8-step pipeline)

`ProductionEngine.run_task()` in `engine/production/engine.py`:
1. `_resolve_dates()` - full/incremental mode, compute data_start with lookback_days offset
2. `_load_data()` - load from DolphinDB based on `depends_on`
3. `_apply_adjust()` - forward/backward price adjustment
4. `_apply_stock_status()` - filter ST, new stocks (<60 days), mark limit-up/down
5. `definition.func(df, params)` - execute factor computation (Polars vectorized)
6. `_handle_suspension_from_status()` - null out factor_value after suspension
7. `_build_quality_flag()` - quality flags (null rate, extreme values)
8. `_save_results()` - upsert to `factor_values` table

### Backtest Engine

Vectorized backtesting in `engine/backtester/`:
- Processes entire price series at once (no loops)
- Calculates: Sharpe ratio, max drawdown, win rate, profit factor
- Uses Polars for efficient computation

### Strategy Parser (DSL)

React Flow JSON graphs are parsed into executable computation chains:
- Nodes represent operations (data load, factor compute, signal generation)
- Edges define data flow
- Parser in `engine/parser/` converts to executable Python

## Common Patterns

### Adding a New Sync Task

1. Insert a row into `sync_task_config` table via API: `POST /api/v1/data/sync/tasks`
2. Or add to `seed_sync_task_config()` in `dolphindb_client.py` for default tasks
3. Set `sync_type` to `incremental` or `full`
4. Specify `primary_keys` for upsert operations

### Querying Data

```python
from store.dolphindb_client import db_client

# Query with parameters (use %s placeholders)
df = db_client.query(
    "SELECT * FROM daily_basic WHERE ts_code = %s AND trade_date >= %s",
    ("000001.SZ", "20240101")
)

# Upsert data
db_client.upsert("table_name", polars_df, ["primary", "keys"])
```

### Adding a New Production Factor

1. Create file in `engine/production/factors/`
2. Use `@factor` decorator from `engine/production/registry.py`
3. Define `depends_on`, `params`, `lookback_days`
4. Implement Polars vectorized computation
5. Factor auto-discovered via `discover_factors()`

### Adding a New API Endpoint

1. Add route to appropriate file in `app/api/v1/`
2. Use Pydantic models for request/response validation
3. Inject services via `Depends()` if using service layer
4. Raise custom exceptions from `app.core.exceptions`

## Known Issues (from BUG_REPORT.md)

### Critical — Fix Before New Features

- **C-02**: SQL injection in `data_merged.py` — 18+ f-string SQL concatenations, use `%s` params
- **C-05**: Arbitrary Python code execution in `/production/factors/test` — needs auth + sandbox
- **C-07**: `annualized_return` uses total return instead of `"Annualized Return [%]"` from VectorBT
- **C-08/C-09**: FlowEditor node form changes not synced back to ReactFlow/Zustand store

### High Priority

- **H-01**: `_escape_value` auto-converts YYYYMMDD strings to date format — breaks STRING column queries
- **H-02**: RSI uses SMA instead of EWM (Wilder's method) — wrong values
- **H-03**: Factor analysis quantile grouping off-by-one — use `ceil().clip(1, quantiles)`
- **H-10**: `DataService.get_daily_data` silently ignores `end_date`
- **H-21**: Factor analysis Sharpe ratio not annualized (missing `sqrt(252)`)

## Important Notes

- **Python version**: Must use 3.11 (PyCaret compatibility requirement)
- **Database**: DolphinDB via Docker (TSDB engine for time-series, dimension tables for metadata)
- **Data processing**: Polars is preferred over Pandas for performance
- **Frontend**: Chinese language UI using Ant Design, React Flow, and ECharts
- **Frontend proxy**: React dev server proxies `/api` to `http://localhost:8000`
- **Tushare token**: Required for data sync, set in `backend/.env`
- **Rate limiting**: Tushare API calls are rate-limited (default 120/min)
- **Scheduler**: Prefect 3.x orchestrates sync/compute/backtest flows (UI at http://localhost:4200)
- **SQL params**: Always use `%s` placeholders, never f-string SQL concatenation
- **Immutability**: Use Polars expressions (lazy, immutable) — never mutate DataFrames in-place

## Troubleshooting

### SQL Syntax Errors
- Use `%s` not `?` for parameters; pass tuple not list
- Bare table names auto-resolved by `_adapt_sql_syntax()`; if not, use `loadTable("dfs://quant_ts", "table_name")`

### Connection Pool Exhausted
- DolphinDB uses a single persistent connection; check for session leaks

### Empty Tables
- Check if sync task exists: `GET /api/v1/data/sync/tasks`
- Verify task is `enabled: true`
- Run sync: `POST /api/v1/data/sync/task/{task_id}`

### Service Not Starting
- Use `./check_status.sh` to diagnose
- Check port availability (3000, 8000, 8848, 4200)
- Ensure Docker is running for DolphinDB and Prefect

## File Locations

| Purpose | Path |
|---------|------|
| Production engine | `backend/engine/production/engine.py` |
| Factor registry | `backend/engine/production/registry.py` |
| Data config | `backend/engine/production/data_config.py` |
| Technical factors | `backend/engine/factors/technical.py` |
| Factor analysis | `backend/engine/analysis/analyzer.py` |
| Data processor | `backend/data_manager/processor.py` |
| Sync engine | `backend/data_manager/refactored_sync_engine.py` |
| DolphinDB client | `backend/store/dolphindb_client.py` |
| Production API | `backend/app/api/v1/production.py` |
| Data API | `backend/app/api/v1/data_merged.py` |
| Prefect flows | `backend/flows/data_sync_flow.py` |
| Config | `.env` (project root) |
| Logs | `backend/logs/app.log` |
| PID files | `.pids/` |
