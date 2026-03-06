# Duplicate Code Refactoring Guide

This document shows the 4 duplicate code patterns found and how to refactor them.

## Pattern 1: Date Range Query Builder

**Found in 3 locations:**
1. `app/api/v1/data_merged.py:127-134`
2. `app/api/v1/production.py:816-822`
3. `engine/analysis/analyzer.py:95-103`

### Before (Duplicated):
```python
conditions = ["ts_code = %s"]
params = [ts_code]
if start_date:
    conditions.append("trade_date >= %s")
    params.append(start_date)
if end_date:
    conditions.append("trade_date <= %s")
    params.append(end_date)
where = " AND ".join(conditions)
```

### After (Using Utility):
```python
from app.core.utils import build_date_range_query

where, params = build_date_range_query(
    ["ts_code = %s"], [ts_code],
    start_date=start_date,
    end_date=end_date
)
```

### Implementation:

Add to `app/core/utils.py`:

```python
def build_date_range_query(
    base_conditions: list[str],
    base_params: list,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_column: str = "trade_date"
) -> tuple[str, list]:
    """Build SQL WHERE clause with date range filtering."""
    conditions = base_conditions.copy()
    params = base_params.copy()

    if start_date:
        conditions.append(f"{date_column} >= %s")
        params.append(start_date)
    if end_date:
        conditions.append(f"{date_column} <= %s")
        params.append(end_date)

    where = " AND ".join(conditions)
    return where, params
```

---

## Pattern 2: Sync Log Entry Creation

**Found in 2 locations:**
1. `app/api/v1/data_merged.py:757-765`
2. `data_manager/sync_components.py:157-165`

### Before (Duplicated):
```python
log_df = pl.DataFrame({
    "data_type": [task_id],
    "last_date": [sync_date],
    "sync_date": [sync_date],
    "rows_synced": [rows_synced],
    "status": [status],
    "error_message": [error_message],
    "params": [params],
    "created_at": [datetime.now()]
})
```

### After (Using Utility):
```python
from data_manager.sync_components import create_sync_log_entry

log_df = create_sync_log_entry(
    task_id=task_id,
    sync_date=sync_date,
    rows_synced=rows_synced,
    status=status,
    error_message=error_message,
    params=params
)
```

### Implementation:

Add to `data_manager/sync_components.py`:

```python
import polars as pl
from datetime import datetime
from typing import Optional


def create_sync_log_entry(
    task_id: str,
    sync_date: str,
    rows_synced: int,
    status: str,
    error_message: Optional[str] = None,
    params: Optional[str] = None
) -> pl.DataFrame:
    """
    Create a sync log entry DataFrame.

    Args:
        task_id: Task identifier
        sync_date: Sync date in YYYYMMDD format
        rows_synced: Number of rows synced
        status: Status (success/failed)
        error_message: Error message if failed
        params: Additional parameters as JSON string

    Returns:
        Polars DataFrame ready for upsert to sync_log table
    """
    return pl.DataFrame({
        "data_type": [task_id],
        "last_date": [sync_date],
        "sync_date": [sync_date],
        "rows_synced": [rows_synced],
        "status": [status],
        "error_message": [error_message or ""],
        "params": [params or ""],
        "created_at": [datetime.now()]
    })
```

---

## Pattern 3: Factor Metadata DataFrame Schema

**Found in 2 locations:**
1. `app/api/v1/production.py:358-368`
2. `engine/production/engine.py:738-748`

### Before (Duplicated):
```python
update_df = pl.DataFrame([row], schema={
    "factor_id": pl.Utf8,
    "description": pl.Utf8,
    "category": pl.Utf8,
    "compute_mode": pl.Utf8,
    "storage_target": pl.Utf8,
    "depends_on": pl.Utf8,
    "params": pl.Utf8,
    "code": pl.Utf8,
    "last_computed_date": pl.Utf8,
})
```

### After (Using Utility):
```python
from engine.production.registry import create_factor_metadata_df

update_df = create_factor_metadata_df(row)
```

### Implementation:

Add to `engine/production/registry.py`:

```python
import polars as pl
from typing import Dict, Any


def create_factor_metadata_df(row: Dict[str, Any]) -> pl.DataFrame:
    """
    Create a factor metadata DataFrame with proper schema.

    Args:
        row: Dictionary with factor metadata fields

    Returns:
        Polars DataFrame ready for upsert to factor_metadata table
    """
    return pl.DataFrame([row], schema={
        "factor_id": pl.Utf8,
        "description": pl.Utf8,
        "category": pl.Utf8,
        "compute_mode": pl.Utf8,
        "storage_target": pl.Utf8,
        "depends_on": pl.Utf8,
        "params": pl.Utf8,
        "code": pl.Utf8,
        "last_computed_date": pl.Utf8,
    })
```

---

## Refactoring Checklist

For each pattern:

- [ ] Add utility function to appropriate module
- [ ] Add docstring with examples
- [ ] Add type hints
- [ ] Update first location to use utility
- [ ] Run tests
- [ ] Update second location to use utility
- [ ] Run tests
- [ ] Update third location (if exists)
- [ ] Run tests
- [ ] Commit changes

## Testing Strategy

After each refactoring:

```bash
# 1. Run unit tests
pytest tests/

# 2. Test specific functionality
pytest tests/test_technical_factors.py -v

# 3. Start server and check logs
python main.py
# Check for import errors or runtime issues

# 4. Test API endpoints
curl http://localhost:8000/api/v1/data/stocks
curl http://localhost:8000/api/v1/factor/compute -X POST -H "Content-Type: application/json" -d '{"ts_code": "000001.SZ", "factors": ["sma20"]}'
```

## Benefits

1. **Maintainability**: Change logic in one place
2. **Testability**: Test utility functions independently
3. **Readability**: Clear function names explain intent
4. **Consistency**: Same behavior across all usages
5. **Reusability**: Easy to use in new code

## Estimated Time

- Pattern 1 (Date Range): 15 minutes
- Pattern 2 (Sync Log): 15 minutes
- Pattern 3 (Factor Metadata): 15 minutes
- Testing: 30 minutes
- **Total**: ~1.5 hours

## Risk Level

**Low-Medium Risk**
- Changes are localized
- Behavior remains identical
- Easy to rollback
- Covered by existing tests

## When to Refactor

- ✅ After Phase 1 cleanup is verified
- ✅ When you have time for thorough testing
- ✅ Before adding new similar code
- ❌ Right before a production deployment
- ❌ During active debugging of other issues
