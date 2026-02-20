# Quant Research System

A full-stack quantitative research platform with drag-and-drop strategy modeling, vectorized backtesting, and AutoML.

**🆕 Now powered by PostgreSQL for better concurrency and enterprise-grade reliability!**

## Quick Start

### Prerequisites

- Python 3.11 (PyCaret requires <=3.11)
- Node.js 18+ (for frontend)
- Docker & Docker Compose (for PostgreSQL database)

### 1. Start PostgreSQL Database

```bash
cd /Users/bytedance/Claude/quant_research_system
docker-compose up -d
```

This will start:
- **PostgreSQL 16** on port 5432
- **pgAdmin** web interface on port 5050 (optional)

### 2. Backend Setup

```bash
# Install Python 3.11 with pyenv
pyenv install 3.11.9

# Create virtual environment
cd backend
~/.pyenv/versions/3.11.9/bin/python3 -m venv .venv
source .venv/bin/activate

# Install dependencies (includes PostgreSQL drivers)
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and set DATABASE__POSTGRES_* variables

# Run server
python main.py
# API docs: http://localhost:8000/docs
```

### 3. Frontend Setup

```bash
cd frontend
npm install
npm start
# App: http://localhost:3000
```

## Key Features

| Feature | Tech |
|---|---|
| Database | PostgreSQL 16 + connection pooling + Docker |
| Data sync | Tushare / AkShare + incremental PostgreSQL upsert |
| Factor library | Polars: MA, EMA, RSI, MACD, KDJ, Bollinger, ATR |
| Backtest engine | Vectorized (Polars): Sharpe, MaxDD, WinRate, ProfitFactor |
| Strategy modeling | React Flow drag-and-drop → JSON graph → DSL parser |
| AutoML | PyCaret model comparison + Optuna Bayesian optimization |
| Scheduling | APScheduler daily sync at 18:00 |

## Database Management

### Access pgAdmin
- URL: http://localhost:5050
- Email: `admin@quant.com`
- Password: `admin123`

### PostgreSQL Connection
- Host: `localhost`
- Port: `5432`
- Database: `quant_research`
- User: `quant_user`
- Password: `quant_pass_2024`

### Useful Commands

```bash
# Connect to database
docker exec -it quant_postgres psql -U quant_user -d quant_research

# Backup database
docker exec quant_postgres pg_dump -U quant_user quant_research > backup.sql

# Restore database
docker exec -i quant_postgres psql -U quant_user quant_research < backup.sql

# View logs
docker-compose logs -f postgres
```

## Documentation

For detailed documentation, see [DOCUMENTATION.md](DOCUMENTATION.md)

For PostgreSQL migration guide, see [POSTGRESQL_MIGRATION.md](POSTGRESQL_MIGRATION.md)

## Architecture

```
quant_research_system/
├── backend/
│   ├── app/
│   │   ├── api/v1/              # FastAPI routes
│   │   ├── core/                # Config, logger, exceptions
│   │   └── services/            # Business logic layer
│   ├── data_manager/            # Data sync engine
│   ├── engine/                  # Factor & backtest engines
│   ├── ml_module/               # AutoML
│   └── store/                   # PostgreSQL client
├── frontend/
│   ├── src/components/          # React components
│   └── src/pages/               # Main pages
└── docker-compose.yml           # PostgreSQL + pgAdmin
```

## License

MIT License
