# Troubleshooting Guide

**Last Updated:** 2026-03-03

## Common Issues and Solutions

### Database Issues

#### Issue: DolphinDB Connection Timeout

**Symptoms:**
```
ConnectionError: 无法连接 DolphinDB localhost:8848
```

**Causes:**
- DolphinDB service not running
- Incorrect host/port configuration
- Network connectivity issue
- Firewall blocking connection

**Solutions:**

1. Check if DolphinDB is running:
```bash
docker ps | grep dolphindb
```

2. If not running, start it:
```bash
docker-compose up -d dolphindb
```

3. Verify connection settings in `.env`:
```bash
DOLPHINDB__HOST=localhost
DOLPHINDB__PORT=8848
DOLPHINDB__USER=admin
DOLPHINDB__PASSWORD=123456
```

4. Test connection manually:
```python
from store.dolphindb_client import db_client
try:
    result = db_client.query("SELECT 1")
    print("Connection successful")
except Exception as e:
    print(f"Connection failed: {e}")
```

5. Check DolphinDB logs:
```bash
docker logs dolphindb
```

---

#### Issue: Empty Tables

**Symptoms:**
- Query returns no data
- Sync tasks show 0 records synced
- Factor computation returns empty results

**Causes:**
- Sync tasks not executed
- Sync tasks disabled
- Data not yet available for date range
- Incorrect table name

**Solutions:**

1. Check if sync tasks are enabled:
```bash
curl -X GET "http://localhost:8000/api/v1/data/sync/tasks"
```

2. Check sync logs:
```bash
curl -X GET "http://localhost:8000/api/v1/data/sync/logs?limit=10"
```

3. Run sync manually:
```bash
curl -X POST "http://localhost:8000/api/v1/data/sync/task/sync_daily_data"
```

4. Verify data exists in database:
```python
from store.dolphindb_client import db_client

df = db_client.query("SELECT COUNT(*) as cnt FROM sync_daily_data")
print(f"Total records: {df['cnt'][0]}")

df = db_client.query(
    "SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date FROM sync_daily_data"
)
print(f"Date range: {df['min_date'][0]} to {df['max_date'][0]}")
```

5. Check if date range is valid:
```python
# Query with specific date range
df = db_client.query(
    "SELECT * FROM sync_daily_data WHERE trade_date >= %s AND trade_date <= %s LIMIT 10",
    ("20240101", "20260101")
)
print(df)
```

---

#### Issue: SQL Syntax Error

**Symptoms:**
```
DolphinDB SQL error: syntax error
```

**Causes:**
- Using PostgreSQL syntax instead of DolphinDB
- Incorrect placeholder format
- Bare table names not registered
- Function name case mismatch

**Solutions:**

1. Check placeholder format (use `%s` not `?`):
```python
# WRONG
db_client.query("SELECT * FROM table WHERE id = ?", (1,))

# CORRECT
db_client.query("SELECT * FROM table WHERE id = %s", (1,))
```

2. Use DolphinDB functions (lowercase):
```python
# WRONG
db_client.query("SELECT MAX(trade_date) FROM table")

# CORRECT
db_client.query("SELECT max(trade_date) FROM table")
```

3. Register table names:
```python
# If getting "table not found" error
db_client.register_meta_table("my_table")
```

4. Use loadTable for bare table names:
```python
# WRONG
db_client.query("SELECT * FROM sync_daily_data")

# CORRECT (auto-converted by client)
db_client.query("SELECT * FROM loadTable('dfs://quant_ts', 'sync_daily_data')")
```

---

### API Issues

#### Issue: 404 Not Found

**Symptoms:**
```
404 Not Found: /api/v1/data/daily
```

**Causes:**
- Endpoint not implemented
- Typo in endpoint path
- API version mismatch
- Route not registered

**Solutions:**

1. Check available endpoints:
```bash
curl -X GET "http://localhost:8000/docs"
```

2. Verify endpoint path:
```bash
# Check if endpoint exists
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101"
```

3. Check if API is running:
```bash
curl -X GET "http://localhost:8000/docs"
```

4. Restart API server:
```bash
# Kill existing process
pkill -f "uvicorn app.main:app"

# Start new server
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

---

#### Issue: 422 Validation Error

**Symptoms:**
```json
{
  "detail": [
    {
      "loc": ["query", "ts_code"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

**Causes:**
- Missing required parameters
- Invalid parameter type
- Invalid parameter format

**Solutions:**

1. Check required parameters:
```bash
# WRONG - missing ts_code
curl -X GET "http://localhost:8000/api/v1/data/daily?start_date=20240101"

# CORRECT
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101"
```

2. Verify parameter types:
```bash
# WRONG - limit should be integer
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101&limit=abc"

# CORRECT
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101&limit=50"
```

3. Check date format (YYYYMMDD):
```bash
# WRONG - invalid date format
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=2024-01-01&end_date=2026-01-01"

# CORRECT
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101"
```

---

#### Issue: 500 Internal Server Error

**Symptoms:**
```
500 Internal Server Error
```

**Causes:**
- Unhandled exception in code
- Database connection lost
- Out of memory
- Timeout

**Solutions:**

1. Check server logs:
```bash
tail -f backend/logs/app.log
```

2. Check for specific error:
```bash
# Look for traceback in logs
grep -A 10 "Traceback" backend/logs/app.log
```

3. Restart server:
```bash
pkill -f "uvicorn app.main:app"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

4. Check database connection:
```python
from store.dolphindb_client import db_client
try:
    result = db_client.query("SELECT 1")
    print("Database OK")
except Exception as e:
    print(f"Database error: {e}")
```

5. Check memory usage:
```bash
# Check if process is using too much memory
ps aux | grep uvicorn
```

---

### Data Sync Issues

#### Issue: Sync Task Fails

**Symptoms:**
```
Sync task failed: Rate limit exceeded
```

**Causes:**
- Tushare API rate limit exceeded
- Network connectivity issue
- Invalid API token
- Data source unavailable

**Solutions:**

1. Check Tushare token:
```bash
# Verify token is set in .env
grep TUSHARE_TOKEN .env
```

2. Check rate limit settings:
```bash
# View current settings
grep COLLECTOR__CALLS_PER_MINUTE .env

# Reduce rate if needed
COLLECTOR__CALLS_PER_MINUTE=60
```

3. Check sync logs:
```bash
curl -X GET "http://localhost:8000/api/v1/data/sync/logs?limit=20"
```

4. Retry sync:
```bash
# Wait a few minutes, then retry
curl -X POST "http://localhost:8000/api/v1/data/sync/task/sync_daily_data"
```

5. Check network connectivity:
```bash
# Test connection to Tushare
curl -X GET "http://api.tushare.pro"
```

---

#### Issue: Sync Takes Too Long

**Symptoms:**
- Sync task running for > 1 hour
- Process appears stuck

**Causes:**
- Large date range
- Network latency
- Database write bottleneck
- Memory pressure

**Solutions:**

1. Check sync progress:
```bash
# View sync logs
curl -X GET "http://localhost:8000/api/v1/data/sync/logs?limit=5"
```

2. Reduce date range:
```python
# Instead of syncing all data, sync recent data only
# Modify sync task to use incremental mode
```

3. Check database performance:
```python
from store.dolphindb_client import db_client

# Check table size
df = db_client.query("SELECT COUNT(*) as cnt FROM sync_daily_data")
print(f"Table size: {df['cnt'][0]} records")
```

4. Increase timeout:
```bash
# In .env
COLLECTOR__TIMEOUT=60
```

5. Kill stuck process:
```bash
# Find process ID
ps aux | grep sync

# Kill process
kill -9 <pid>
```

---

### Factor Computation Issues

#### Issue: Factor Computation Fails

**Symptoms:**
```
Factor computation failed: KeyError: 'close'
```

**Causes:**
- Required field not available
- Data preprocessing failed
- Factor function error
- Insufficient data

**Solutions:**

1. Check factor dependencies:
```bash
# Get factor definition
curl -X GET "http://localhost:8000/api/v1/production/factors"
```

2. Verify required fields exist:
```python
from store.dolphindb_client import db_client

# Check if 'close' field exists
df = db_client.query("SELECT close FROM sync_daily_data LIMIT 1")
print(df.columns)
```

3. Check data quality:
```python
# Check for null values
df = db_client.query("SELECT * FROM sync_daily_data WHERE ts_code = %s LIMIT 100", ("000001.SZ",))
print(df.null_count())
```

4. Run factor with debug logging:
```python
from app.core.logger import logger
from engine.production.engine import ProductionEngine
from store.dolphindb_client import db_client

logger.setLevel("DEBUG")
engine = ProductionEngine(db_client)
engine.run_task(factor_id="ma20", target_date="20260303")
```

5. Check factor code:
```python
from engine.production.registry import get_factor

definition = get_factor("ma20")
print(definition.depends_on)  # Check dependencies
print(definition.params)      # Check parameters
```

---

#### Issue: Factor Results Are NULL

**Symptoms:**
- Factor computation completes but all values are NULL
- Quality flag shows high null rate

**Causes:**
- Data preprocessing filtered all records
- ST stock filtering too aggressive
- Suspension handling removed all data
- Insufficient lookback data

**Solutions:**

1. Check preprocessing options:
```python
# Run with minimal preprocessing
engine.run_task(
    factor_id="ma20",
    target_date="20260303",
    preprocess={
        "adjust_price": "forward",
        "filter_st": False,
        "filter_new_stock": False,
        "handle_suspension": False,
        "mark_limit": False,
    }
)
```

2. Check stock status:
```python
from store.dolphindb_client import db_client

# Check if stock is ST
df = db_client.query(
    "SELECT * FROM stock_daily_status WHERE ts_code = %s AND trade_date = %s",
    ("000001.SZ", "20260303")
)
print(df)
```

3. Check suspension status:
```python
# Check if stock is suspended
df = db_client.query(
    "SELECT * FROM stock_daily_status WHERE ts_code = %s AND is_suspend = true",
    ("000001.SZ",)
)
print(f"Suspension periods: {len(df)}")
```

4. Check lookback data:
```python
# Verify sufficient historical data
df = db_client.query(
    "SELECT COUNT(*) as cnt FROM sync_daily_data WHERE ts_code = %s AND trade_date < %s",
    ("000001.SZ", "20260303")
)
print(f"Historical records: {df['cnt'][0]}")
```

---

### Performance Issues

#### Issue: Slow Query

**Symptoms:**
- Query takes > 10 seconds
- API timeout

**Causes:**
- Large date range
- No date filter
- Full table scan
- Missing indexes

**Solutions:**

1. Add date range filter:
```bash
# SLOW - no date filter
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ"

# FAST - with date filter
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20260101&end_date=20260303"
```

2. Reduce result limit:
```bash
# SLOW - large limit
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101&limit=1000000"

# FAST - small limit
curl -X GET "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20260101&limit=100"
```

3. Check query plan:
```python
from store.dolphindb_client import db_client

# Analyze query
df = db_client.query("""
    SELECT * FROM sync_daily_data
    WHERE ts_code = %s AND trade_date >= %s
    LIMIT 10
""", ("000001.SZ", "20240101"))
```

4. Increase timeout:
```bash
# In .env
COLLECTOR__TIMEOUT=60
```

---

#### Issue: Out of Memory

**Symptoms:**
```
MemoryError: Unable to allocate memory
Process killed
```

**Causes:**
- Loading too much data
- Memory leak
- Insufficient system memory

**Solutions:**

1. Reduce data range:
```python
# Load smaller date range
df = db_client.query(
    "SELECT * FROM sync_daily_data WHERE ts_code = %s AND trade_date >= %s",
    ("000001.SZ", "20260101")  # Recent data only
)
```

2. Use chunked processing:
```python
# Process in chunks
dates = ["20240101", "20240201", "20240301", ...]
for date in dates:
    df = db_client.query(
        "SELECT * FROM sync_daily_data WHERE trade_date = %s",
        (date,)
    )
    # Process chunk
```

3. Check system memory:
```bash
# View memory usage
free -h

# View process memory
ps aux | grep uvicorn
```

4. Restart server:
```bash
pkill -f "uvicorn app.main:app"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

---

### Logging and Debugging

#### Enable Debug Logging

```python
from app.core.logger import logger
import logging

# Set to DEBUG level
logger.setLevel(logging.DEBUG)

# Now debug messages will appear
logger.debug("Debug message")
```

#### View Logs

```bash
# Real-time logs
tail -f backend/logs/app.log

# Last 100 lines
tail -100 backend/logs/app.log

# Search for errors
grep ERROR backend/logs/app.log

# Search for specific module
grep "production" backend/logs/app.log
```

#### Enable SQL Query Logging

```python
from store.dolphindb_client import db_client

# Enable query logging
db_client.enable_query_logging()

# Now all queries will be logged
df = db_client.query("SELECT * FROM sync_daily_data LIMIT 1")
```

---

## Getting Help

1. **Check Documentation**
   - README.md - Project overview
   - DEVELOPER_GUIDE.md - Development guide
   - API.md - API documentation
   - docs/CODEMAPS/ - Architecture documentation

2. **Check Logs**
   - backend/logs/app.log - Application logs
   - Docker logs: `docker logs dolphindb`

3. **Test Connectivity**
   - API: http://localhost:8000/docs
   - Database: http://localhost:8848
   - Prefect: http://localhost:4200

4. **Contact Support**
   - Create GitHub issue
   - Contact development team
   - Check team chat

---

## Quick Reference

### Common Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Restart API
pkill -f "uvicorn app.main:app"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Run tests
pytest tests/

# Format code
black app/ engine/ data_manager/ store/

# Check types
mypy app/ engine/ data_manager/ store/
```

### Useful URLs

- API Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
- DolphinDB: http://localhost:8848
- Prefect: http://localhost:4200

### Environment Variables

```bash
# Database
DOLPHINDB__HOST=localhost
DOLPHINDB__PORT=8848
DOLPHINDB__USER=admin
DOLPHINDB__PASSWORD=123456

# API
CORS_ORIGINS="*"
DEBUG=true

# Data Collection
COLLECTOR__TUSHARE_TOKEN=your_token
COLLECTOR__CALLS_PER_MINUTE=120
```
