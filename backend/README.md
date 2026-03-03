# QuantSystem Backend - README

**Version:** 2.0.0
**Last Updated:** 2026-03-03
**Python:** 3.11+
**Framework:** FastAPI + DolphinDB + Polars + Prefect 3.x

## Overview

QuantSystem Backend is a high-performance quantitative trading platform featuring:

- **Vectorized Factor Computation** - Polars-based factor engine with 60+ technical indicators
- **Time-Series Database** - DolphinDB for efficient OHLCV data storage and queries
- **Incremental Data Sync** - Automatic data synchronization from Tushare API
- **Vectorized Backtesting** - Fast portfolio simulation without loops
- **AutoML Integration** - PyCaret-based model training and optimization
- **Workflow Orchestration** - Prefect 3.x for scheduling and monitoring

## Quick Start

### Prerequisites

- Python 3.11+ (PyCaret requirement)
- Docker (for DolphinDB)
- 4GB+ RAM

### Installation

```bash
# Clone repository
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env with your Tushare token and DolphinDB credentials
```

### Start Services

```bash
# Start DolphinDB (Docker)
docker-compose up -d

# Initialize database
python database/init_dolphindb.py

# Run development server
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Access Points

- **API Documentation:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **DolphinDB Web UI:** http://localhost:8848
- **Prefect UI:** http://localhost:4200

## Project Structure

```
backend/
├── app/                          # FastAPI application
│   ├── api/v1/                   # API routes
│   │   ├── data_merged.py        # Data queries & sync
│   │   ├── factor.py             # Factor computation
│   │   ├── production.py         # Production engine
│   │   ├── strategy.py           # Backtest execution
│   │   ├── ml.py                 # ML training
│   │   └── flows.py              # Prefect workflows
│   ├── services/                 # Business logic
│   │   ├── data_service.py
│   │   ├── factor_service.py
│   │   └── backtest_service.py
│   ├── core/                     # Configuration & utilities
│   │   ├── config.py             # Pydantic settings
│   │   ├── exceptions.py         # Custom exceptions
│   │   ├── logger.py             # Logging setup
│   │   └── container.py          # Dependency injection
│   └── main.py                   # FastAPI app factory
│
├── engine/                       # Computation engines
│   ├── production/               # Factor production
│   │   ├── engine.py             # Main engine
│   │   ├── registry.py           # Factor registry
│   │   └── data_config.py        # Field mappings
│   ├── factors/                  # Indicator library
│   │   ├── technical.py          # Technical indicators
│   │   └── financial.py          # Financial analysis
│   ├── backtester/               # Backtesting
│   │   └── backtester.py
│   ├── parser/                   # Strategy parser
│   │   └── parser.py
│   └── analysis/                 # Performance analysis
│       ├── analyzer.py
│       └── drawdown.py
│
├── data_manager/                 # Data sync & processing
│   ├── sync_components.py        # Sync orchestration
│   ├── processor.py              # Data preprocessing
│   ├── refactored_sync_engine.py # Sync engine
│   └── collectors/               # Data collectors
│       └── tushare_client.py
│
├── store/                        # Database layer
│   └── dolphindb_client.py       # DolphinDB client
│
├── database/                     # Database initialization
│   ├── init_dolphindb.py
│   └── init_dolphindb.dos        # DolphinDB script
│
├── ml_module/                    # Machine learning
│   ├── trainer.py
│   ├── optimizer.py
│   └── pipeline.py
│
├── flows/                        # Prefect workflows
│   └── data_sync_flow.py
│
├── tests/                        # Test suite
│   ├── test_technical_factors.py
│   └── test_dolphindb_utils.py
│
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment template
└── README.md                     # This file
```

## Configuration

### Environment Variables

Create `.env` file in project root:

```bash
# Application
APP_NAME="Quant Research System"
ENVIRONMENT="development"
DEBUG=true
CORS_ORIGINS="*"

# DolphinDB
DOLPHINDB__HOST=localhost
DOLPHINDB__PORT=8848
DOLPHINDB__USER=admin
DOLPHINDB__PASSWORD=123456
DOLPHINDB__DB_PATH=dfs://quant

# Data Collection
COLLECTOR__TUSHARE_TOKEN=your_token_here
COLLECTOR__CALLS_PER_MINUTE=120
COLLECTOR__RETRY_TIMES=3
COLLECTOR__TIMEOUT=30

# Backtest
BACKTEST__INITIAL_CAPITAL=1000000
BACKTEST__COMMISSION_RATE=0.0003
BACKTEST__SLIPPAGE_RATE=0.0001

# ML
ML__N_TRIALS=100
ML__CV_FOLDS=5
ML__TEST_SIZE=0.2
```

### Accessing Configuration

```python
from app.core.config import settings

# Access nested configs
host = settings.database.dolphindb_host
token = settings.collector.tushare_token
capital = settings.backtest.initial_capital
```

## API Documentation

### Data Endpoints

**Query Daily Data**
```bash
GET /api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101
```

**List Sync Tasks**
```bash
GET /api/v1/data/sync/tasks
```

**Execute Sync Task**
```bash
POST /api/v1/data/sync/task/{task_id}
```

### Factor Endpoints

**Compute Technical Indicators**
```bash
POST /api/v1/factor/compute
{
    "data": [...],
    "indicators": ["ma", "rsi", "macd"],
    "params": {"ma_window": 20}
}
```

**List Available Factors**
```bash
GET /api/v1/factor/list
```

### Production Endpoints

**Run Factor Computation**
```bash
POST /api/v1/production/run
{
    "factor_id": "ma20",
    "target_date": "20260303",
    "mode": "incremental"
}
```

**Get Factor Results**
```bash
GET /api/v1/production/results?factor_id=ma20&start_date=20240101&end_date=20260101
```

### Strategy Endpoints

**Run Backtest**
```bash
POST /api/v1/strategy/backtest
{
    "strategy_json": {...},
    "start_date": "20240101",
    "end_date": "20260101",
    "initial_capital": 1000000
}
```

## Database Schema

### TSDB Tables (dfs://quant_ts)

- `sync_daily_data` - OHLCV data
- `sync_daily_basic` - Close + indicators
- `sync_adj_factor` - Price adjustment factors
- `sync_index_daily` - Index data
- `factor_values` - Computed factor results

### Metadata Tables (dfs://quant_meta)

- `factor_metadata` - Factor definitions
- `sync_task_config` - Sync task configurations
- `stock_daily_status` - ST, suspension, limit flags
- `trade_cal` - Trading calendar
- `stock_basic` - Stock master data

See [Data Layer Codemap](./docs/CODEMAPS/data.md) for detailed schema.

## Development Guide

### Adding a New Factor

1. Create factor function with `@factor` decorator:

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
    from engine.factors.technical import TechnicalFactors
    return TechnicalFactors.rsi(df["close"], params["window"])
```

2. Register factor in database:

```bash
POST /api/v1/production/register
{
    "factor_id": "rsi_14",
    "factor_name": "RSI 14-day",
    "code": "..."
}
```

3. Run factor computation:

```bash
POST /api/v1/production/run
{
    "factor_id": "rsi_14",
    "target_date": "20260303"
}
```

### Adding a New API Endpoint

1. Create route in `app/api/v1/`:

```python
from fastapi import APIRouter, Depends
from app.core.container import container

router = APIRouter()

@router.get("/my-endpoint")
async def my_endpoint(param: str):
    service = container.get_data_service()
    result = service.do_something(param)
    return {"success": True, "data": result}
```

2. Register in `app/main.py`:

```python
from app.api.v1 import my_module
app.include_router(my_module.router, prefix=settings.api_v1_prefix)
```

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run specific test
pytest tests/test_technical_factors.py::test_sma
```

### Code Quality

```bash
# Format code
black app/ engine/ data_manager/ store/

# Lint
flake8 app/ engine/ data_manager/ store/

# Type checking
mypy app/ engine/ data_manager/ store/
```

## Performance Optimization

### Vectorization
- All computations use Polars (not loops)
- Batch operations on entire DataFrame
- Typical: 1M rows in < 1 second

### Incremental Computation
- Only compute new dates
- Reuse historical data
- Lookback window for rolling calculations

### Caching
- Factor registry cached in memory
- Data config cached with TTL
- Trading calendar cached

## Troubleshooting

### DolphinDB Connection Issues

```bash
# Check if DolphinDB is running
docker ps | grep dolphindb

# View DolphinDB logs
docker logs dolphindb

# Restart DolphinDB
docker-compose restart dolphindb
```

### Empty Tables

```bash
# Check sync tasks
GET /api/v1/data/sync/tasks

# Run sync manually
POST /api/v1/data/sync/task/{task_id}

# Check sync logs
GET /api/v1/data/sync/logs
```

### API Errors

Check logs:
```bash
tail -f backend/logs/app.log
```

### SQL Syntax Errors

Common issues:
- Using `?` instead of `%s` placeholders
- PostgreSQL-specific syntax (use DolphinDB equivalents)
- Bare table names (auto-resolved by client)

## Dependencies

### Core
- **fastapi** - Web framework
- **uvicorn** - ASGI server
- **pydantic** - Data validation

### Data Processing
- **polars** - Vectorized computation
- **pandas** - Data manipulation
- **numpy** - Numerical computing

### Database
- **dolphindb** - Time-series database

### Data Collection
- **tushare** - Chinese stock data API
- **akshare** - Alternative data source

### Quantitative
- **vectorbt** - Backtesting
- **alphalens-reloaded** - Factor analysis

### Machine Learning
- **pycaret** - AutoML
- **optuna** - Hyperparameter optimization
- **scikit-learn** - ML algorithms
- **xgboost** - Gradient boosting
- **lightgbm** - Light gradient boosting

### Orchestration
- **prefect** - Workflow scheduling

## Documentation

- [Architecture Index](./docs/CODEMAPS/INDEX.md)
- [API Routes](./docs/CODEMAPS/api.md)
- [Data Layer](./docs/CODEMAPS/data.md)
- [Factor Engine](./docs/CODEMAPS/factors.md)
- [Backtest Engine](./docs/CODEMAPS/backtest.md)

## Contributing

1. Create feature branch: `git checkout -b feature/my-feature`
2. Write tests first (TDD)
3. Implement feature
4. Run tests: `pytest tests/`
5. Format code: `black .`
6. Commit: `git commit -m "feat: add my feature"`
7. Push: `git push origin feature/my-feature`
8. Create pull request

## License

Proprietary - QuantSystem

## Support

For issues or questions:
1. Check [Troubleshooting](./docs/TROUBLESHOOTING.md)
2. Review API docs: http://localhost:8000/docs
3. Check logs: `backend/logs/app.log`
