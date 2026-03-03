# API Documentation

**Last Updated:** 2026-03-03
**Base URL:** http://localhost:8000/api/v1
**Documentation:** http://localhost:8000/docs

## Table of Contents

1. [Data Endpoints](#data-endpoints)
2. [Factor Endpoints](#factor-endpoints)
3. [Production Endpoints](#production-endpoints)
4. [Strategy Endpoints](#strategy-endpoints)
5. [ML Endpoints](#ml-endpoints)
6. [Error Handling](#error-handling)
7. [Authentication](#authentication)

## Data Endpoints

### GET /data/daily

Query daily OHLCV data.

**Parameters:**
```
ts_code: string (required) - Stock code (e.g., "000001.SZ")
start_date: string (required) - Start date (YYYYMMDD format)
end_date: string (required) - End date (YYYYMMDD format)
limit: integer (optional) - Max records to return (default: 100)
```

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101&limit=50"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "ts_code": "000001.SZ",
      "trade_date": "20240101",
      "open": 100.5,
      "high": 102.3,
      "low": 99.8,
      "close": 101.2,
      "vol": 1000000,
      "amount": 101200000,
      "pct_chg": 0.5
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

**Status Codes:**
- 200: Success
- 404: Data not found
- 422: Invalid parameters

---

### GET /data/sync/tasks

List all sync tasks.

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/data/sync/tasks"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "task_id": "sync_daily_data",
      "task_name": "Daily OHLCV Data",
      "data_source": "tushare",
      "sync_type": "incremental",
      "table_name": "sync_daily_data",
      "enabled": true,
      "last_sync_date": "20260302",
      "next_sync_date": "20260303"
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### POST /data/sync/task/{task_id}

Execute a sync task.

**Parameters:**
```
task_id: string (path) - Task ID to execute
```

**Example Request:**
```bash
curl -X POST "http://localhost:8000/api/v1/data/sync/task/sync_daily_data"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "task_id": "sync_daily_data",
    "status": "completed",
    "records_synced": 5000,
    "duration_seconds": 45,
    "sync_time": "2026-03-03T10:30:00Z"
  },
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### GET /data/sync/logs

View sync history.

**Parameters:**
```
task_id: string (optional) - Filter by task ID
limit: integer (optional) - Max records (default: 100)
```

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/data/sync/logs?task_id=sync_daily_data&limit=10"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "task_id": "sync_daily_data",
      "sync_time": "2026-03-03T10:30:00Z",
      "status": "completed",
      "records_synced": 5000,
      "duration_seconds": 45,
      "error_message": null
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

## Factor Endpoints

### POST /factor/compute

Compute technical indicators.

**Request Body:**
```json
{
  "data": [
    {
      "ts_code": "000001.SZ",
      "trade_date": "20260303",
      "open": 100.5,
      "high": 102.3,
      "low": 99.8,
      "close": 101.2,
      "vol": 1000000
    }
  ],
  "indicators": ["ma", "rsi", "macd"],
  "params": {
    "ma_window": 20,
    "rsi_window": 14
  }
}
```

**Example Request:**
```bash
curl -X POST "http://localhost:8000/api/v1/factor/compute" \
  -H "Content-Type: application/json" \
  -d '{
    "data": [...],
    "indicators": ["ma", "rsi"],
    "params": {"ma_window": 20}
  }'
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "ts_code": "000001.SZ",
      "trade_date": "20260303",
      "close": 101.2,
      "ma20": 100.8,
      "rsi": 65.5
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### GET /factor/list

List available factors.

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/factor/list"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "factor_id": "ma20",
      "factor_name": "20-day Moving Average",
      "description": "Simple moving average of close price",
      "depends_on": ["close"],
      "params": {"window": 20},
      "mode": "incremental"
    },
    {
      "factor_id": "rsi_14",
      "factor_name": "RSI 14-day",
      "description": "Relative Strength Index",
      "depends_on": ["close"],
      "params": {"window": 14},
      "mode": "incremental"
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

## Production Endpoints

### POST /production/run

Execute factor computation task.

**Request Body:**
```json
{
  "factor_id": "ma20",
  "target_date": "20260303",
  "start_date": null,
  "end_date": null,
  "mode": "incremental",
  "preprocess": {
    "adjust_price": "forward",
    "filter_st": true,
    "filter_new_stock": true
  }
}
```

**Parameters:**
- `factor_id` (required): Factor ID to compute
- `target_date` (optional): Single date for incremental mode
- `start_date` (optional): Range start date
- `end_date` (optional): Range end date
- `mode` (optional): "incremental" or "full"
- `preprocess` (optional): Preprocessing options

**Example Request:**
```bash
curl -X POST "http://localhost:8000/api/v1/production/run" \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "ma20",
    "target_date": "20260303",
    "mode": "incremental"
  }'
```

**Response:**
```json
{
  "success": true,
  "data": {
    "task_id": "prod_123456",
    "factor_id": "ma20",
    "status": "completed",
    "records_computed": 5000,
    "duration_seconds": 30,
    "start_time": "2026-03-03T10:30:00Z",
    "end_time": "2026-03-03T10:30:30Z"
  },
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### GET /production/status/{task_id}

Check factor computation task status.

**Parameters:**
```
task_id: string (path) - Task ID
```

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/production/status/prod_123456"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "task_id": "prod_123456",
    "factor_id": "ma20",
    "status": "completed",
    "progress": 100,
    "records_computed": 5000,
    "start_time": "2026-03-03T10:30:00Z",
    "end_time": "2026-03-03T10:30:30Z"
  },
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### GET /production/results

Query factor computation results.

**Parameters:**
```
factor_id: string (required) - Factor ID
ts_code: string (optional) - Stock code
start_date: string (optional) - Start date (YYYYMMDD)
end_date: string (optional) - End date (YYYYMMDD)
limit: integer (optional) - Max records (default: 100)
```

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/production/results?factor_id=ma20&ts_code=000001.SZ&start_date=20240101&end_date=20260101"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "ts_code": "000001.SZ",
      "trade_date": "20260303",
      "factor_id": "ma20",
      "factor_value": 100.8,
      "quality_flag": {
        "null_rate": 0.0,
        "extreme_rate": 0.0,
        "quality_score": 100
      }
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### GET /production/factors

List registered factors.

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/production/factors"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "factor_id": "ma20",
      "factor_name": "20-day Moving Average",
      "description": "Simple moving average",
      "depends_on": ["close"],
      "mode": "incremental",
      "last_computed": "20260303"
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

## Strategy Endpoints

### POST /strategy/backtest

Run backtest.

**Request Body:**
```json
{
  "strategy_json": {
    "nodes": [...],
    "edges": [...]
  },
  "start_date": "20240101",
  "end_date": "20260101",
  "initial_capital": 1000000,
  "commission_rate": 0.0003,
  "slippage_rate": 0.0001
}
```

**Example Request:**
```bash
curl -X POST "http://localhost:8000/api/v1/strategy/backtest" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_json": {...},
    "start_date": "20240101",
    "end_date": "20260101",
    "initial_capital": 1000000
  }'
```

**Response:**
```json
{
  "success": true,
  "data": {
    "backtest_id": "bt_123456",
    "status": "completed",
    "metrics": {
      "total_return": 0.25,
      "annual_return": 0.12,
      "sharpe_ratio": 1.5,
      "max_drawdown": -0.15,
      "win_rate": 0.55,
      "profit_factor": 1.8,
      "total_trades": 120
    },
    "daily_returns": [...]
  },
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### GET /strategy/results/{backtest_id}

Get backtest results.

**Parameters:**
```
backtest_id: string (path) - Backtest ID
```

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/strategy/results/bt_123456"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "backtest_id": "bt_123456",
    "status": "completed",
    "metrics": {...},
    "daily_returns": [...]
  },
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

## ML Endpoints

### POST /ml/train

Train ML model.

**Request Body:**
```json
{
  "model_name": "my_model",
  "data_source": "factor_values",
  "features": ["ma20", "rsi_14", "macd"],
  "target": "returns",
  "model_type": "regression",
  "test_size": 0.2,
  "cv_folds": 5
}
```

**Example Request:**
```bash
curl -X POST "http://localhost:8000/api/v1/ml/train" \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "my_model",
    "features": ["ma20", "rsi_14"],
    "target": "returns"
  }'
```

**Response:**
```json
{
  "success": true,
  "data": {
    "model_id": "ml_123456",
    "model_name": "my_model",
    "status": "completed",
    "metrics": {
      "train_score": 0.85,
      "test_score": 0.82,
      "cv_score": 0.83
    },
    "training_time": 120
  },
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

### GET /ml/models

List trained models.

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/ml/models"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "model_id": "ml_123456",
      "model_name": "my_model",
      "model_type": "regression",
      "created_at": "2026-03-03T10:30:00Z",
      "metrics": {
        "train_score": 0.85,
        "test_score": 0.82
      }
    }
  ],
  "error": null,
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

## Error Handling

### Error Response Format

All errors follow standard format:

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "details": {}
  },
  "timestamp": "2026-03-03T10:30:00Z"
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `DATA_NOT_FOUND` | 404 | Requested data not found |
| `DATA_VALIDATION_ERROR` | 422 | Invalid input parameters |
| `SYNC_TASK_NOT_FOUND` | 404 | Sync task not found |
| `RATE_LIMIT_EXCEEDED` | 429 | API rate limit exceeded |
| `FACTOR_COMPUTATION_ERROR` | 400 | Factor computation failed |
| `BACKTEST_ERROR` | 400 | Backtest execution failed |
| `ML_ERROR` | 400 | ML training failed |
| `INTERNAL_ERROR` | 500 | Internal server error |

### Example Error Response

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "DATA_NOT_FOUND",
    "message": "Stock data not found for ts_code=000001.SZ",
    "details": {
      "ts_code": "000001.SZ",
      "start_date": "20240101",
      "end_date": "20260101"
    }
  },
  "timestamp": "2026-03-03T10:30:00Z"
}
```

---

## Authentication

Currently no authentication required. For production, implement:

1. **JWT Token Authentication**
   ```bash
   Authorization: Bearer <token>
   ```

2. **API Key Authentication**
   ```bash
   X-API-Key: <api_key>
   ```

3. **Rate Limiting**
   - Per user: 1000 requests/hour
   - Per IP: 10000 requests/hour

---

## Rate Limiting

Current limits (per minute):
- Data endpoints: 100 requests
- Factor endpoints: 50 requests
- Production endpoints: 20 requests
- Strategy endpoints: 10 requests
- ML endpoints: 5 requests

---

## Pagination

For list endpoints, use:
```
?page=1&page_size=50
```

Response includes:
```json
{
  "data": [...],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total": 1000,
    "total_pages": 20
  }
}
```

---

## Versioning

Current API version: **v1**

Future versions will be available at:
- `/api/v2/...`
- `/api/v3/...`

---

## Support

For API issues:
1. Check http://localhost:8000/docs for interactive documentation
2. Review error messages and error codes
3. Check logs: `backend/logs/app.log`
4. Contact development team
