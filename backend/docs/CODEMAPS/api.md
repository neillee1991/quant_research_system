# API Routes Codemap

**Last Updated:** 2026-03-03
**Entry Points:** app/api/v1/

## Architecture

```
FastAPI Application
├── /api/v1/data/*          (data_merged.py)
│   ├─ GET /daily           Query daily data
│   ├─ GET /sync/tasks      List sync tasks
│   ├─ POST /sync/task/{id} Execute sync
│   └─ GET /sync/logs       View sync history
│
├── /api/v1/factor/*        (factor.py)
│   ├─ POST /compute        Calculate indicators
│   └─ GET /list            List available factors
│
├── /api/v1/production/*    (production.py)
│   ├─ POST /run            Execute factor computation
│   ├─ GET /status/{id}     Check task status
│   └─ GET /results         Query factor results
│
├── /api/v1/strategy/*      (strategy.py)
│   ├─ POST /backtest       Run backtest
│   └─ GET /results/{id}    Get backtest results
│
├── /api/v1/ml/*            (ml.py)
│   ├─ POST /train          Train ML model
│   └─ GET /models          List trained models
│
└── /api/v1/flows/*         (flows.py)
    ├─ GET /list            List Prefect flows
    └─ POST /trigger        Trigger flow execution
```

## Key Modules

### data_merged.py (Data API)
**Purpose:** Data queries, sync task management, sync execution

**Key Endpoints:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/data/daily` | GET | Query daily OHLCV data |
| `/data/sync/tasks` | GET | List all sync tasks |
| `/data/sync/task/{task_id}` | POST | Execute sync task |
| `/data/sync/logs` | GET | View sync history |
| `/data/sync/config` | GET | Get sync configuration |

**Dependencies:**
- `DataService` - Business logic
- `DolphinDBClient` - Database access
- `SyncTaskExecutor` - Sync execution

**Request/Response Models:**
```python
class DailyDataQuery(BaseModel):
    ts_code: str
    start_date: str  # YYYYMMDD
    end_date: str    # YYYYMMDD
    limit: int = 100

class SyncTaskResponse(BaseModel):
    task_id: str
    task_name: str
    status: str  # pending, running, completed, failed
    last_sync_date: Optional[str]
    next_sync_date: Optional[str]
```

### factor.py (Factor API)
**Purpose:** Technical indicator computation

**Key Endpoints:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/factor/compute` | POST | Calculate indicators |
| `/factor/list` | GET | List available factors |

**Dependencies:**
- `FactorService` - Factor computation
- `TechnicalFactors` - Indicator library

### production.py (Production API)
**Purpose:** Factor production engine orchestration

**Key Endpoints:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/production/run` | POST | Execute factor computation |
| `/production/status/{task_id}` | GET | Check task status |
| `/production/results` | GET | Query factor results |
| `/production/factors` | GET | List registered factors |

**Request Model:**
```python
class ProductionTaskRequest(BaseModel):
    factor_id: str
    target_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    mode: Optional[str] = None  # incremental, full
    preprocess: Optional[Dict[str, Any]] = None
```

**Dependencies:**
- `ProductionEngine` - Factor computation
- `FactorRegistry` - Factor discovery

### strategy.py (Strategy API)
**Purpose:** Backtest execution

**Key Endpoints:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/strategy/backtest` | POST | Run backtest |
| `/strategy/results/{id}` | GET | Get backtest results |

### ml.py (ML API)
**Purpose:** AutoML model training

**Key Endpoints:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/ml/train` | POST | Train ML model |
| `/ml/models` | GET | List trained models |

### flows.py (Prefect Flows API)
**Purpose:** Workflow orchestration

**Key Endpoints:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/flows/list` | GET | List Prefect flows |
| `/flows/trigger` | POST | Trigger flow execution |

## Error Handling

All endpoints use custom exception hierarchy:

```python
QuantException (base)
├── DataException
│   ├── DataNotFoundError (404)
│   └── DataValidationError (422)
├── SyncException
│   ├── SyncTaskNotFoundError (404)
│   └── RateLimitExceededError (429)
├── FactorException (400)
├── BacktestException (400)
└── MLException (400)
```

Exception handlers in `app/core/exceptions.py` convert to HTTP responses.

## Request/Response Format

All responses follow standard envelope:

```json
{
  "success": true,
  "data": { /* payload */ },
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

Error response:
```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "DATA_NOT_FOUND",
    "message": "Stock data not found for ts_code=000001.SZ"
  },
  "timestamp": "2026-03-03T10:30:00Z"
}
```

## Authentication

Currently no authentication. For production, add:
- JWT token validation
- API key management
- Rate limiting per user

## CORS Configuration

Configured via `CORS_ORIGINS` environment variable:
```bash
CORS_ORIGINS="http://localhost:3000,http://localhost:3001"
```

## Middleware Stack

1. **GZipMiddleware** - Compress responses > 1KB
2. **CORSMiddleware** - Cross-origin requests
3. **Exception Handlers** - Custom error responses

## Related Codemaps

- [Data Layer](./data.md) - Database schema
- [Factor Engine](./factors.md) - Computation details
- [Service Layer](./services.md) - Business logic
