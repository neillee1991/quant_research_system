# QuantSystem Backend - Architecture Index

**Last Updated:** 2026-03-03
**Version:** 2.0.0
**Framework:** FastAPI + DolphinDB + Polars + Prefect 3.x

## Overview

QuantSystem is a full-stack quantitative trading platform with vectorized factor computation, backtesting, and AutoML capabilities. The backend is organized into distinct layers: data ingestion, computation, storage, and API exposure.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    API Layer (FastAPI)                  │
│  /api/v1/data  /api/v1/factor  /api/v1/production      │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│              Service Layer (Business Logic)             │
│  DataService  FactorService  BacktestService            │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│           Computation Engines (Polars-based)            │
│  ProductionEngine  FactorEngine  BacktestEngine         │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│         Data Processing & Sync (Incremental)            │
│  DataProcessor  SyncEngine  TushareCollector            │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│        Storage Layer (DolphinDB Time-Series DB)         │
│  TSDB: daily_data, factor_values, adj_factor            │
│  Meta: factor_metadata, sync_task_config                │
└─────────────────────────────────────────────────────────┘
```

## Core Modules

| Module | Purpose | Key Files |
|--------|---------|-----------|
| **app/api/v1** | REST API endpoints | data_merged.py, factor.py, production.py, strategy.py, ml.py |
| **app/services** | Business logic layer | data_service.py, factor_service.py, backtest_service.py |
| **engine/production** | Factor computation orchestration | engine.py, registry.py, data_config.py |
| **engine/factors** | Technical & financial indicators | technical.py, financial.py |
| **engine/backtester** | Vectorized backtesting | backtester.py |
| **data_manager** | Data sync & preprocessing | sync_components.py, processor.py, collectors/ |
| **store** | Database abstraction | dolphindb_client.py |
| **app/core** | Configuration & utilities | config.py, exceptions.py, logger.py, container.py |

## Data Flow

### Factor Computation Pipeline
```
1. API Request (/api/v1/production/run)
   ↓
2. ProductionEngine.run_task()
   ├─ Resolve dates (incremental vs full)
   ├─ Load data from DolphinDB
   ├─ Apply adjustments (复权, ST filter, suspension)
   ├─ Execute factor calculation (Polars vectorized)
   ├─ Build quality flags
   └─ Upsert results to factor_values table
   ↓
3. Results stored in DolphinDB
```

### Data Sync Pipeline
```
1. Sync Task Config (DolphinDB table)
   ↓
2. SyncTaskExecutor
   ├─ Load task definition
   ├─ Determine sync type (incremental/full)
   ├─ Fetch data from Tushare API
   ├─ Transform & validate
   └─ Upsert to DolphinDB
   ↓
3. Sync Log updated
```

## Key Concepts

### Factor Registry
- Factors are registered via `@factor` decorator in code
- Metadata stored in `factor_metadata` table
- Dynamic discovery via `discover_factors()`
- Supports parameterized computation

### Data Configuration
- Field mappings defined in `factor_data_config` table
- Supports multiple data sources per factor
- Automatic table registration

### Preprocessing Pipeline
- Price adjustment (forward/backward 复权)
- ST stock filtering
- New stock filtering (< 60 days)
- Suspension handling
- Limit-up/limit-down marking

### Quality Flags
- Null rate detection
- Extreme value detection
- Data completeness scoring

## Related Codemaps

- [API Routes](./api.md) - Detailed endpoint documentation
- [Data Layer](./data.md) - Database schema and sync engine
- [Factor Engine](./factors.md) - Technical indicators and computation
- [Backtest Engine](./backtest.md) - Vectorized backtesting
- [ML Module](./ml.md) - AutoML and model training

## Quick Start

### Setup
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run Development Server
```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### API Documentation
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Database
- DolphinDB Web UI: http://localhost:8848
- Default credentials: admin / 123456

## Configuration

All configuration uses Pydantic with environment variable support:

```python
from app.core.config import settings

# Access nested configs
settings.database.dolphindb_host
settings.collector.tushare_token
settings.backtest.initial_capital
```

Environment variables use double underscore for nesting:
```bash
DOLPHINDB__HOST=localhost
COLLECTOR__CALLS_PER_MINUTE=120
```

## Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run specific test file
pytest tests/test_technical_factors.py
```

## Deployment

See [DEPLOYMENT.md](../DEPLOYMENT.md) for production deployment guidelines.

## Support

For issues or questions:
1. Check logs: `backend/logs/app.log`
2. Review [TROUBLESHOOTING.md](../TROUBLESHOOTING.md)
3. Check API docs: http://localhost:8000/docs
