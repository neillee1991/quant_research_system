# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quant Research System is a full-stack quantitative trading platform with factor computation, strategy backtesting, and production deployment capabilities. The system uses DolphinDB for time-series data storage, Polars for data processing, a self-developed scheduler for orchestration, and React Flow for visual strategy design.

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

### Frontend Type Sync (MANDATORY after backend model changes)

**CRITICAL RULE**: After modifying ANY Pydantic model in `backend/app/models/`, you MUST regenerate frontend types:

```bash
# 确保后端服务运行中，然后执行：
cd frontend
npm run gen:types
```

- 生成文件: `src/types/generated.ts`
- 禁止手动编辑 `generated.ts`，所有类型从此文件引用：
  ```typescript
  import type { components } from '../types/generated'
  type FactorConfig = components['schemas']['FactorConfig']
  type SyncTaskConfig = components['schemas']['SyncTaskConfig']
  type ETLTaskConfig = components['schemas']['ETLTaskConfig']
  ```
- 手写类型文件 (`task.ts`, `factor.ts`, `data.ts`) 已过时，逐步迁移到 `generated.ts`

### Database (DolphinDB via Docker)

```bash
docker-compose up -d
cd backend
python database/init_dolphindb.py
# DolphinDB Web UI: http://localhost:8848
```

## Architecture & Key Concepts

### Database Layer: Dual Database Architecture

**Critical**: This project uses both DolphinDB and PostgreSQL:

- **DolphinDB** (`dfs://quant`): Time-series data storage with optimized partitioning
  - TSDB tables: `daily_data`, `daily_basic`, `adj_factor`, `index_daily`, `moneyflow`, `factor_values`
  - Dimension tables: `sync_log`, `sync_log_history`, `stock_basic`, `factor_metadata`, `factor_analysis`, `trade_cal`
  - Bare table names are auto-resolved to `loadTable()` calls in `_adapt_sql_syntax()`

- **PostgreSQL**: Metadata and configuration storage
  - Tables: `factor_configs`, `sync_task_configs`, `etl_task_configs`, `task_runs`, `flow_runs`

**Note**: Configuration tables have been migrated from DolphinDB to PostgreSQL

**factor_values 表分区策略（已优化）**:
- 三维组合分区：HASH(factor_id, 20) + RANGE(trade_date, 季度) + HASH(ts_code, 10)
- 总分区数：20 × 120 × 10 = 24,000 个分区
- 查询优化效果：
  - 按股票查询（时序）：裁剪到 ~10 个分区
  - 按日期查询（横截面）：裁剪到 ~200 个分区
  - 按因子查询（全量）：裁剪到 ~1200 个分区
- 性能监控：查询和写入操作自动记录耗时和速度

The database client is a singleton: `from infrastructure.database.dolphindb_client import db_client`

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

factor_service = container.get_factor_service()
backtest_service = container.get_backtest_service()
sync_engine = container.get_sync_engine()
```

**注意**: `DataService` 已删除（死代码），数据查询直接使用 `db_client`。

### API Route Structure

All routes are under `/api/v1/`:
- `/data/*` - Data queries and sync operations
- `/factor/*` - Factor calculation and management
- `/strategy/*` - Backtest execution
- `/tasks/*` - Unified task management
- `/flows/*` - Flow orchestration (self-developed scheduler)
- `/config/*` - Configuration management

**Important**: The `/data/daily` endpoint queries `daily_basic` table (not `daily_data`), which contains close price + indicators (PE, PB, turnover_rate) but NOT full OHLC data.

**Configuration Updates**: All configuration update endpoints use direct overwrite mode. Previous versions are not retained. Always export configurations before making changes.

### Exception Handling

Custom exception hierarchy in `app/core/exceptions.py`:
- `QuantException` (base)
  - `DataException` → `DataNotFoundError`, `DataValidationError`
  - `SyncException` → `SyncTaskNotFoundError`, `RateLimitExceededError`
  - `BacktestException`, `FactorException`, `MLException`

### 认证与安全

本系统为个人单机使用，**无 API 认证和限流**（`auth.py`、`rate_limit.py` 已删除）。

安全防护集中在：
- 因子代码执行：`app/core/sandbox.py` AST 静态分析 + 受限 exec（`engine/factor/registry.py` 已接入）
- SQL 注入防护：`app/core/sql_security.py` 表名白名单 + 参数化查询
- 输入验证：`app/validators/input_validators.py`

### Factor Engine (Polars-based)

Technical indicators and factor analysis in `engine/analysis/`:
- Time-series factors: MA, EMA, RSI, MACD, KDJ, Bollinger Bands, ATR
- Cross-sectional factors: Rank, Z-Score, Industry neutralization
- Factor analysis: IC (Information Coefficient), quantile analysis, Sharpe ratio

### Production Factor Framework (8-step pipeline)

`FactorComputeService` in `app/services/factor_service.py` orchestrates factor computation:
1. `_resolve_dates()` - full/incremental mode, compute data_start with lookback_days offset
2. `_load_data()` - load from DolphinDB based on `depends_on`
3. `_apply_adjust()` - forward/backward price adjustment
4. `_apply_stock_status()` - filter ST, new stocks (<60 days), mark limit-up/down
5. `definition.func(df, params)` - execute factor computation (Polars vectorized)
6. `_handle_suspension()` - null out factor_value after suspension
7. `_build_quality_flag()` - quality flags (null rate, extreme values)
8. `_save_results()` - upsert to `factor_values` table

Factors are registered in `engine/factor/registry.py` with `@factor` decorator and dynamically loaded from PostgreSQL `factor_configs` table.

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
from infrastructure.database.dolphindb_client import db_client

# Query with parameters (use %s placeholders)
df = db_client.query(
    "SELECT * FROM daily_basic WHERE ts_code = %s AND trade_date >= %s",
    ("000001.SZ", "20240101")
)

# Upsert data
db_client.upsert("table_name", polars_df, ["primary", "keys"])
```

### Adding a New Production Factor

1. Create factor configuration in PostgreSQL `factor_configs` table
2. Write factor computation code using Polars vectorized operations
3. Factors are dynamically loaded and discovered via `engine/factor/registry.py`
4. No code changes required for new factor definitions

### Adding a New API Endpoint

1. Add route to appropriate file in `app/api/v1/`
2. Use Pydantic models for request/response validation
3. Inject services via `Depends()` if using service layer
4. Raise custom exceptions from `app.core.exceptions`

## Known Issues

### High Priority

- **H-01**: `_escape_value` auto-converts YYYYMMDD strings to date format — breaks STRING column queries
- **H-02**: RSI uses SMA instead of EWM (Wilder's method) — wrong values
- **H-03**: Factor analysis quantile grouping off-by-one — use `ceil().clip(1, quantiles)`
- **H-21**: Factor analysis Sharpe ratio not annualized (missing `sqrt(252)`)
- **H-10**: `DataService.get_daily_data` silently ignores `end_date` — **已删除 DataService，此问题已消除**

## Important Notes

- **Python version**: Must use 3.11 (PyCaret compatibility requirement)
- **Database**: DolphinDB via Docker (TSDB engine for time-series, dimension tables for metadata)
- **Data processing**: Polars is preferred over Pandas for performance
- **Frontend**: Chinese language UI using Ant Design, React Flow, and ECharts
- **Frontend proxy**: React dev server proxies `/api` to `http://localhost:8000`
- **Tushare token**: Required for data sync, set in `.env`
- **Rate limiting**: Tushare API calls are rate-limited (default 120/min); AkShare calls rate-limited at 0.5s interval with 3-retry exponential backoff
- **Scheduler**: Self-developed scheduler orchestrates sync/compute/backtest flows (supports DAG execution and cron scheduling)
- **SQL params**: Always use `%s` placeholders, never f-string SQL concatenation
- **Immutability**: Use Polars expressions (lazy, immutable) — never mutate DataFrames in-place
- **进程守护**: `start.sh` 使用 while-true restart loop，后端/前端崩溃后5秒自动重启
- **密码配置**: 所有密码通过 `.env` 环境变量配置，参考 `.env.example`，禁止硬编码
- **Flow CRUD**: `FlowService` 使用 asyncpg（DatabasePool），与 `TaskService` 保持一致
- **因子代码安全**: 从数据库加载的因子代码执行前经过 `code_sandbox.check_security()` 沙箱检查
- **run_id 格式**: 统一使用 `str(uuid.uuid4())`，禁止时间戳或混合格式

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
| Factor compute service | `backend/app/services/factor_service.py` |
| Factor registry | `backend/engine/factor/registry.py` |
| Data config | `backend/engine/factor/data_config.py` |
| Factor analysis | `backend/engine/analysis/analyzer.py` |
| Data processor | `backend/infrastructure/processor/pipeline.py` |
| Sync engine | `backend/data_manager/refactored_sync_engine.py` |
| DolphinDB client | `backend/infrastructure/database/dolphindb_client.py` |
| Factor API | `backend/app/api/v1/factor/` |
| Data API | `backend/app/api/v1/data/` |
| Tasks API (unified) | `backend/app/api/v1/tasks.py` |
| Scheduler | `backend/scheduler/`
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
8.写文件的时候，改为分批次写入文件，每批次只写 400 行
9.拒绝硬代码，尽量使用环境变量配置，如果有硬代码，一定要提前和我确认
10.每次重点更新，都自动帮我写好 git 日志并提交
11.每次更新，都要同时更新系统架构、memory 和本文件。确保文档都是最新的。
12.写代码时，结合系统架构确定代码逻辑，如果产生了架构层面的变化，调整后要更新架构分析报告和文档
---

## 项目标准与规范

**重要**: 本项目遵循严格的代码质量和安全标准。详细规范请参考 [PROJECT_STANDARDS.md](PROJECT_STANDARDS.md)

### 代码质量要求

- **函数长度**: ≤ 50 行
- **文件长度**: ≤ 800 行
- **嵌套深度**: ≤ 4 层
- **类型注解覆盖率**: ≥ 80%
- **测试覆盖率**: ≥ 80% (核心模块 ≥ 90%)

### 安全要求

**本系统为个人单机使用，无需 API 认证和限流。**

安全防护重点：
- 因子代码沙箱：`app/core/sandbox.py`（已接入 `engine/factor/registry.py`）
- SQL 注入防护：表名白名单 + 参数化查询
- 输入验证：Pydantic 模型 + `app/validators/`
- 密码管理：所有密码通过 `.env` 环境变量配置，禁止硬编码默认值

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