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

- Two databases: `dfs://quant` (unified database with optimized partitioning)
- TSDB tables: `daily_data`, `daily_basic`, `adj_factor`, `index_daily`, `moneyflow`, `factor_values`
- Dimension tables: `sync_log`, `sync_log_history`, `stock_basic`, `factor_metadata`, `factor_analysis`, `dag_run_log`, `dag_task_log`, `production_task_run`, `trade_cal`, `sync_task_config`, `factor_data_config`
- Bare table names are auto-resolved to `loadTable()` calls in `_adapt_sql_syntax()`

**factor_values 表分区策略（已优化）**:
- 三维组合分区：HASH(factor_id, 20) + RANGE(trade_date, 季度) + HASH(ts_code, 10)
- 总分区数：20 × 120 × 10 = 24,000 个分区
- 查询优化效果：
  - 按股票查询（时序）：裁剪到 ~10 个分区
  - 按日期查询（横截面）：裁剪到 ~200 个分区
  - 按因子查询（全量）：裁剪到 ~1200 个分区
- 性能监控：查询和写入操作自动记录耗时和速度

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
- Bare table names auto-resolved by `_adapt_sql_syntax()`; if not, use `loadTable("dfs://quant", "table_name")`

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

### 分区性能测试
- 运行 `python scripts/test_partition_performance.py` 测试查询性能
- 预期速度：> 10,000 行/秒表示分区优化生效
- 如果性能不佳，检查查询是否命中分区键（factor_id, trade_date, ts_code）

### 重建分区表
- 如果需要重新优化分区，运行 `python scripts/optimize_factor_values_partition.py`
- 脚本会自动备份数据、删除旧表、创建优化分区表、恢复数据

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
| Factor API | `backend/app/api/v1/factor/` |
| Data API | `backend/app/api/v1/data/` |
| Tasks API (unified) | `backend/app/api/v1/tasks.py` |
| Factor compute service | `backend/app/services/factor_service.py` |
| Prefect flows | `backend/flows/data_sync_flow.py` |
| Config | `.env` (project root) |
| Logs | `backend/logs/app.log` |
| PID files | `.pids/` |



1.我希望你可以基于第一性的原则和我沟通，不用完全迎合我的判断，如果我的分析思路有风险不合理，我需要你提醒我；
2.每次进行大的系统架构调整和接口更新的时候，你要完整了解这个接口和架构当前的使用状况，要求在调整后系统不再使用旧的代码，每次进行重构的时候我都需要你组织前端、后端、数据、系统架构、UI 一起来讨论，找到最好的方案；
3.不要假设我清楚自己想要什么东西，动机或目的不清晰的时候，你要和我讨论清楚目的，否则不做下一步；
4.目标清晰但路径不是最短最合理的时候，直接告诉我最好的方法，给我建议；
5.遇到问题的时候找到根本原因，不打补丁，不写硬代码，每个觉得都要能够解释为什么
6.输出内容说重点，减少不会影响决策的内容的产出
7.当我提需求的时候，你要学会举一反三，询问我系统中类似的模块是否也要同步做调整。例如功能类似的模块、继承自同一父类的其他类等等
8.文件一定要分批次写入
9.拒绝硬代码，尽量使用环境变量配置，如果有硬代码，一定要提前和我确认

---

## 项目标准与规范

**重要**: 本项目遵循严格的代码质量和安全标准。详细规范请参考 [PROJECT_STANDARDS.md](PROJECT_STANDARDS.md)

### 代码质量要求

- **函数长度**: ≤ 50 行
- **文件长度**: ≤ 800 行
- **嵌套深度**: ≤ 4 层
- **类型注解覆盖率**: ≥ 80%
- **测试覆盖率**: ≥ 80% (核心模块 ≥ 90%)

### 安全要求 (CRITICAL)

**🔴 立即处理的安全问题:**
1. **代码执行端点** (`/api/v1/production/factor/test`) - 必须添加认证和授权
2. **SQL查询端点** (`/api/v1/data/query`) - 必须添加认证和速率限制
3. **默认密码** - 生产环境禁止使用默认密码 "123456"

**强制要求:**
- 所有 API 端点必须实现认证 (JWT/OAuth2)
- 敏感操作必须实现基于角色的授权 (RBAC)
- 所有用户输入必须验证 (使用 Pydantic)
- 表名/列名使用白名单验证
- 禁止硬编码敏感信息

### 文件组织规范

**✅ 正确的目录结构:**
```
backend/
├── app/              # FastAPI 应用层
├── engine/           # 因子计算引擎
├── data_manager/     # 数据管理
├── store/            # 数据存储
├── tests/            # 测试代码 (按类型组织)
│   ├── unit/        # 单元测试
│   ├── integration/ # 集成测试
│   ├── api/         # API 测试
│   └── performance/ # 性能测试
├── scripts/          # 运维脚本 (仅必要脚本)
│   ├── migrations/  # 数据库迁移
│   └── maintenance/ # 维护脚本
└── docs/            # 项目文档
```

**❌ 禁止:**
- backend 根目录放置临时脚本 (check_*.py, test_*.py, analyze_*.py)
- 代码库中保留 backups/ 目录
- 提交 __pycache__/ 和 *.pyc 文件
- tests/ 目录所有文件平铺 (必须按类型组织)

### 测试规范

**测试组织:**
- 单元测试: `tests/unit/` - 快速, 无外部依赖
- 集成测试: `tests/integration/` - 需要数据库
- API 测试: `tests/api/` - 需要启动服务
- 性能测试: `tests/performance/` - 性能基准

**测试依赖 (requirements-test.txt):**
```
pytest>=8.0.0
pytest-cov>=4.1.0
pytest-asyncio>=0.23.0
pytest-mock>=3.12.0
pytest-xdist>=3.5.0
```

### 代码风格工具

**必须使用:**
- `black` - 代码格式化 (line-length=100)
- `isort` - import 排序
- `flake8` - 代码检查
- `mypy` - 类型检查

**提交前检查:**
```bash
black backend/
isort backend/
flake8 backend/
mypy backend/
pytest tests/ --cov=. --cov-report=term-missing
```

### Git 提交规范

**提交信息格式:**
```
<type>: <subject>

<body>

<footer>
```

**Type:** feat, fix, refactor, docs, test, chore, perf, ci

**示例:**
```
feat: 添加因子计算缓存机制

- 实现 Redis 缓存层
- 添加缓存失效策略
- 性能提升 3x

Closes #123
```

### 代码审查要求

**强制要求:**
- 所有代码必须经过 Code Review
- 至少 1 人 approve 才能合并
- CI 测试必须通过
- 覆盖率不能下降

**审查清单:**
- [ ] 代码通过所有工具检查 (black, isort, flake8, mypy)
- [ ] 测试覆盖率 ≥ 80%
- [ ] 所有测试通过
- [ ] 添加了必要的文档和注释
- [ ] 没有硬编码的敏感信息
- [ ] 没有未解决的 TODO/FIXME
- [ ] 更新了 CHANGELOG.md

---