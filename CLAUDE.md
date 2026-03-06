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
# Setup environment
cd backend
~/.pyenv/versions/3.11.9/bin/python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run development server
python main.py
# or with uvicorn directly:
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# API documentation available at: http://localhost:8000/docs
```

### Frontend (React + TypeScript)

```bash
cd frontend
npm install
npm start  # Runs on http://localhost:3000
```

Note: Frontend is in Chinese and uses Ant Design components.

### Database (DolphinDB via Docker)

```bash
# Start DolphinDB and Prefect services
docker-compose up -d

# Initialize DolphinDB tables
cd backend
python database/init_dolphindb.py

# DolphinDB Web UI: http://localhost:8848
```

## Architecture & Key Concepts

### Database Layer: DolphinDB

**Critical**: This project uses DolphinDB as the sole data store (replacing PostgreSQL/Redis/DuckDB).

- Two databases: `dfs://quant_ts` (TSDB partitioned tables) and `dfs://quant_meta` (dimension tables)
- TSDB tables: `daily_data`, `daily_basic`, `adj_factor`, `index_daily`, `moneyflow`, `factor_values`
- Dimension tables (created dynamically at startup): `sync_log`, `sync_log_history`, `stock_basic`, `factor_metadata`, `factor_analysis`, `dag_run_log`, `dag_task_log`, `production_task_run`, `trade_cal`, `sync_task_config`
- Bare table names are auto-resolved to `loadTable()` calls in `_adapt_sql_syntax()`
- SQL functions are auto-lowercased for DolphinDB compatibility

The database client is a singleton: `from store.dolphindb_client import db_client`

### Configuration System (Pydantic-based)

Configuration uses nested Pydantic models with environment variable support:

```python
from app.core.config import settings

# Access nested configs
settings.dolphindb.host
settings.collector.tushare_token
settings.backtest.initial_capital
```

Environment variables use double underscore for nesting:
- `DOLPHINDB__HOST=localhost`
- `COLLECTOR__CALLS_PER_MINUTE=120`

### Data Sync Engine (Database-Driven)

The sync system reads task definitions from the DolphinDB `sync_task_config` dimension table (seeded with 8 default tasks on first startup):

- `SyncConfigManager`: Loads task configs from DolphinDB with in-memory cache
- `SyncLogManager`: Tracks last sync dates for incremental syncs
- `TableManager`: Auto-creates tables with schemas from config
- `TushareAPIClient`: Rate-limited API calls with retry logic
- `SyncTaskExecutor`: Orchestrates sync execution

**Key sync types**:
- `incremental`: Tracks last sync date, continues from checkpoint
- `full`: Complete replacement on each sync

### Service Layer Architecture

Business logic is separated into service classes (dependency injection pattern):

```python
from app.core.container import container

data_service = container.get_data_service()
factor_service = container.get_factor_service()
backtest_service = container.get_backtest_service()
```

Services abstract repository operations and provide domain-specific methods.

### API Route Structure

All routes are under `/api/v1/`:
- `/data/*` - Data queries and sync operations (merged from old `/sync/config/*`)
- `/factor/*` - Technical indicator calculations
- `/strategy/*` - Backtest execution
- `/ml/*` - AutoML model training

**Important**: The `/data/daily` endpoint queries `daily_basic` table (not `daily_data`), which contains close price + indicators (PE, PB, turnover_rate) but NOT full OHLC data.

### Exception Handling

Custom exception hierarchy in `app/core/exceptions.py`:
- `QuantException` (base)
  - `DataException` → `DataNotFoundError`, `DataValidationError`
  - `SyncException` → `SyncTaskNotFoundError`, `RateLimitExceededError`
  - `BacktestException`, `FactorException`, `MLException`

All exceptions carry context and are handled by FastAPI exception handlers.

### Factor Engine (Polars-based)

Technical indicators are computed using Polars expressions in `engine/factors/`:
- Time-series factors: MA, EMA, RSI, MACD, KDJ, Bollinger Bands, ATR
- Cross-sectional factors: Rank, Z-Score, Industry neutralization

All factor computations are vectorized for performance.

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
5. Task will auto-create table on first sync

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

### Adding a New API Endpoint

1. Add route to appropriate file in `app/api/v1/`
2. Use Pydantic models for request/response validation
3. Inject services via `Depends()` if using service layer
4. Raise custom exceptions from `app.core.exceptions`
5. FastAPI will auto-generate OpenAPI docs

## Important Notes

- **Python version**: Must use 3.11 (PyCaret compatibility requirement)
- **Database**: DolphinDB via Docker (TSDB engine for time-series, dimension tables for metadata)
- **Data processing**: Polars is preferred over Pandas for performance
- **Frontend**: Chinese language UI using Ant Design, React Flow, and ECharts
- **Frontend proxy**: React dev server proxies `/api` to `http://localhost:8000`
- **Tushare token**: Required for data sync, set in `backend/.env`
- **Rate limiting**: Tushare API calls are rate-limited (default 120/min)
- **Scheduler**: Prefect 3.x orchestrates sync/compute/backtest flows (UI at http://localhost:4200)

## Troubleshooting

### SQL Syntax Errors
If you see DolphinDB SQL errors, check for:
- Using `?` instead of `%s` placeholders
- Passing list instead of tuple for parameters
- PostgreSQL-specific syntax (ON CONFLICT, information_schema, etc.) — use DolphinDB equivalents
- Bare table names should be auto-resolved by `_adapt_sql_syntax()`; if not, use `loadTable("dfs://quant_ts", "table_name")`

### Connection Pool Exhausted
- DolphinDB uses a single persistent connection; check for session leaks
- Review long-running queries

### Empty Tables
- Check if sync task exists: `GET /api/v1/data/sync/tasks`
- Verify task is `enabled: true`
- Run sync: `POST /api/v1/data/sync/task/{task_id}`
- Check logs in `backend/logs/app.log`

### Service Not Starting
- Use `./check_status.sh` to diagnose issues
- Check port availability (3000, 8000, 8848, 4200)
- View logs: `tail -f /tmp/backend.log` or `tail -f /tmp/frontend.log`
- Ensure Docker is running for DolphinDB and Prefect

### Database Connection Issues
- Verify DolphinDB is running: `docker ps | grep dolphindb`
- Check connection settings in `.env` (project root)
- Default credentials: user=admin, password from `.env` DOLPHINDB_PASSWORD

## File Locations

- Config: `.env` (project root, environment); sync tasks stored in DolphinDB `sync_task_config` table
- Logs: `backend/logs/app.log` (or `/tmp/backend.log` and `/tmp/frontend.log` when using start.sh)
- PID files: `.pids/backend.pid` and `.pids/frontend.pid` (when using start.sh)
- Database client: `backend/store/dolphindb_client.py`
- Database init: `backend/database/init_dolphindb.py` and `init_dolphindb.dos`
- API routes: `backend/app/api/v1/` (data_merged.py, factor.py, strategy.py, ml.py)
- Services: `backend/app/services/` (data_service.py, factor_service.py, backtest_service.py)
- Factor engine: `backend/engine/factors/`
- Backtest engine: `backend/engine/backtester/`
- ML module: `backend/ml_module/` (trainer.py, optimizer.py, pipeline.py)
- Sync engine: `backend/data_manager/sync_components.py` and `refactored_sync_engine.py`
- Utility scripts: `start.sh`, `stop.sh`, `check_status.sh`
