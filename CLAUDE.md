# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quant Research System is a full-stack quantitative trading platform with drag-and-drop strategy modeling, vectorized backtesting, and AutoML capabilities. The system uses PostgreSQL for data storage, Polars for data processing, and React Flow for visual strategy design.

## Development Commands

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

### Database (PostgreSQL via Docker)

```bash
# Start PostgreSQL and pgAdmin
docker-compose up -d

# Connect to database
docker exec quant_postgres psql -U quant_user -d quant_research

# View logs
docker-compose logs -f postgres

# Backup database
docker exec quant_postgres pg_dump -U quant_user quant_research > backup.sql

# Restore database
docker exec -i quant_postgres psql -U quant_user quant_research < backup.sql
```

## Architecture & Key Concepts

### Database Layer: PostgreSQL Migration

**Critical**: This project recently migrated from DuckDB to PostgreSQL. All SQL queries MUST use PostgreSQL syntax:
- Use `%s` placeholders, NOT `?` (DuckDB style)
- Pass parameters as tuples: `db_client.query(sql, tuple(params))`
- Use `information_schema` for metadata queries, NOT `PRAGMA`
- Connection pooling is managed via `psycopg2.pool.ThreadedConnectionPool`

The database client is a singleton: `from store.postgres_client import db_client`

### Configuration System (Pydantic-based)

Configuration uses nested Pydantic models with environment variable support:

```python
from app.core.config import settings

# Access nested configs
settings.database.postgres_host
settings.collector.tushare_token
settings.backtest.initial_capital
```

Environment variables use double underscore for nesting:
- `DATABASE__POSTGRES_HOST=localhost`
- `COLLECTOR__CALLS_PER_MINUTE=120`

### Data Sync Engine (Config-Driven)

The sync system is JSON-configured (`backend/data_manager/sync_config.json`) with modular components:

- `SyncConfigManager`: Loads and validates sync task configurations
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

1. Add task definition to `backend/data_manager/sync_config.json`
2. Define schema with PostgreSQL types (use `DOUBLE PRECISION` not `DOUBLE`)
3. Set `sync_type` to `incremental` or `full`
4. Specify `primary_keys` for upsert operations
5. Task will auto-create table on first sync

### Querying Data

```python
from store.postgres_client import db_client

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
- **Database**: PostgreSQL 16 via Docker (connection pooling enabled)
- **Data processing**: Polars is preferred over Pandas for performance
- **Frontend proxy**: React dev server proxies `/api` to `http://localhost:8000`
- **Tushare token**: Required for data sync, set in `backend/.env`
- **Rate limiting**: Tushare API calls are rate-limited (default 120/min)

## Troubleshooting

### SQL Syntax Errors
If you see "syntax error at or near 'AND'" or similar, check for:
- Using `?` instead of `%s` placeholders
- Passing list instead of tuple for parameters
- DuckDB-specific syntax (PRAGMA, etc.)

### Connection Pool Exhausted
- Check for unclosed connections (use context managers)
- Increase pool size: `DATABASE__CONNECTION_POOL_SIZE=20`
- Review long-running queries

### Empty Tables
- Check if sync task exists in `sync_config.json`
- Verify task is `enabled: true`
- Run sync: `POST /api/v1/data/sync/task/{task_id}`
- Check logs in `backend/logs/app.log`

## File Locations

- Config: `backend/.env` (environment) and `backend/data_manager/sync_config.json` (sync tasks)
- Logs: `backend/logs/app.log`
- Database client: `backend/store/postgres_client.py`
- API routes: `backend/app/api/v1/`
- Services: `backend/app/services/`
- Factor engine: `backend/engine/factors/`
- Sync engine: `backend/data_manager/sync_components.py` and `refactored_sync_engine.py`
