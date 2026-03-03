# Dead Code & Redundancy Cleanup Report

**Project**: QuantSystem Backend
**Analysis Date**: 2026-03-03
**Total Python Files**: 68
**Largest Files**: dolphindb_client.py (1325 lines), data_merged.py (1247 lines), production.py (954 lines)

---

## Executive Summary

This report identifies dead code, unused imports, duplicate code patterns, and unused dependencies in the QuantSystem backend. Items are categorized by risk level for safe removal.

### Key Findings
- **Unused Imports**: 15+ instances across 12 files
- **Unused Variables**: 8 instances
- **Duplicate Code Patterns**: 4 significant duplications
- **Unused Dependencies**: 2 packages (alphalens, xgboost, lightgbm)
- **Debug/Test Scripts**: 3 files in database/ folder not referenced in production code
- **API Endpoints**: All endpoints in data_merged.py, factor.py, flows.py, ml.py, production.py are registered and potentially used

---

## Category 1: SAFE TO REMOVE (High Confidence)

### 1.1 Unused Imports

| File | Line | Import | Confidence |
|------|------|--------|------------|
| `app/api/v1/production.py` | 4 | `timedelta` | 90% |
| `app/api/v1/production.py` | 10 | `get_registry` | 90% |
| `app/api/v1/production.py` | 14 | `settings` | 90% |
| `app/api/v1/production.py` | 16 | `DateUtils` | 90% |
| `app/core/utils.py` | 11 | `RateLimitExceededError` | 90% |
| `data_manager/sync_components.py` | 21 | `DEFAULT_START_DATE` | 90% |
| `flows/data_sync_flow.py` | 7 | `task_input_hash` | 90% |
| `ml_module/optimizer.py` | 2 | `pickle` | 90% |
| `ml_module/trainer.py` | 1 | `pickle` | 90% |
| `app/services/data_service.py` | 5 | `Any`, `Dict` | 90% |
| `app/services/data_service.py` | 11 | `QueryBuilder` | 90% |
| `engine/factors/technical.py` | 2 | `np` (numpy as np) | 90% |
| `app/api/v1/factor.py` | 6 | `CrossSectionalFactors` | 90% |

**Action**: Remove these imports using autoflake.

### 1.2 Unused Variables

| File | Line | Variable | Confidence |
|------|------|----------|------------|
| `app/services/factor_service.py` | 104 | `date_col` | 100% |
| `app/services/factor_service.py` | 145 | `return_data` | 100% |
| `ml_module/trainer.py` | 35 | `n_select` | 100% |

**Action**: Remove these variable assignments.

### 1.3 Unused Dependencies

Based on grep analysis of actual imports:

| Package | Used In Files | Status |
|---------|---------------|--------|
| `alphalens-reloaded` | 0 | **UNUSED** |
| `xgboost` | 0 | **UNUSED** |
| `lightgbm` | 0 | **UNUSED** |
| `vectorbt` | 1 (engine/backtester/vector_engine.py) | Used |
| `pycaret` | 2 (ml_module/) | Used |
| `optuna` | 1 (ml_module/optimizer.py) | Used |

**Action**: Remove alphalens-reloaded, xgboost, lightgbm from requirements.txt.

### 1.4 Debug/Test Scripts (Not in Production)

These files in `database/` folder are not imported anywhere in the codebase:

| File | Purpose | Lines | Status |
|------|---------|-------|--------|
| `database/debug_st_filter.py` | Diagnostic script for ST filter issue | 121 | **One-time debug script** |
| `database/test_factor_registration.py` | Test factor loading from DB | 79 | **Manual test script** |
| `database/migrate_factors_to_db.py` | One-time migration script | 132 | **Migration completed** |

**Action**: Move to `database/archive/` or `database/scripts/` folder for historical reference, or delete if migration is confirmed complete.

---

## Category 2: DUPLICATE CODE (Refactor Recommended)

### 2.1 Date Range Query Pattern (3 duplicates)

**Locations**:
- `app/api/v1/data_merged.py:127-134`
- `app/api/v1/production.py:816-822`
- `engine/analysis/analyzer.py:95-103`

**Pattern**:
```python
if start_date:
    conditions.append("trade_date >= %s")
    params.append(start_date)
if end_date:
    conditions.append("trade_date <= %s")
    params.append(end_date)
where = " AND ".join(conditions)
```

**Recommendation**: Extract to utility function in `app/core/utils.py`:
```python
def build_date_range_query(
    base_conditions: list[str],
    base_params: list,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> tuple[str, list]:
    """Build SQL WHERE clause with date range filtering."""
    conditions = base_conditions.copy()
    params = base_params.copy()

    if start_date:
        conditions.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= %s")
        params.append(end_date)

    where = " AND ".join(conditions)
    return where, params
```

### 2.2 Sync Log DataFrame Creation (2 duplicates)

**Locations**:
- `app/api/v1/data_merged.py:757-765`
- `data_manager/sync_components.py:157-165`

**Pattern**:
```python
"data_type": [task_id],
"last_date": [sync_date],
"sync_date": [sync_date],
"rows_synced": [rows_synced],
"status": [status],
"error_message": [error_message],
"params": [params],
"created_at": [datetime.now()]
```

**Recommendation**: Extract to `data_manager/sync_components.py` as `create_sync_log_entry()` function.

### 2.3 Factor Metadata Update DataFrame (2 duplicates)

**Locations**:
- `app/api/v1/production.py:358-368`
- `engine/production/engine.py:738-748`

**Pattern**:
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
```

**Recommendation**: Extract to `engine/production/registry.py` as `create_factor_metadata_df()` function.

---

## Category 3: CAREFUL REVIEW NEEDED

### 3.1 API Endpoints (60% confidence from vulture)

Vulture flagged many API endpoints as "unused" because they're registered via FastAPI decorators. **These are NOT dead code** - they're HTTP endpoints accessed by the frontend.

**Confirmed Active Routers**:
- `app/api/v1/data_merged.py` - 30 endpoints (data queries, sync tasks, ETL)
- `app/api/v1/factor.py` - 2 endpoints (compute_factors, compute_ic)
- `app/api/v1/flows.py` - 6 endpoints (flow CRUD operations)
- `app/api/v1/ml.py` - 3 endpoints (ML training)
- `app/api/v1/production.py` - Multiple endpoints (factor production)

**Action**: NO REMOVAL. These are registered in `app/main.py` via `include_router()`.

### 3.2 Pydantic Model Fields

Vulture flagged unused fields in Pydantic models (e.g., `FlowConfig.cron`, `FlowConfig.tags`). These are used for API request/response validation.

**Action**: NO REMOVAL. These are part of the API contract.

---

## Recommended Cleanup Steps

### Phase 1: Safe Removals (Low Risk)
1. Run autoflake to remove unused imports:
   ```bash
   autoflake --in-place --remove-all-unused-imports --remove-unused-variables \
     app/api/v1/production.py \
     app/core/utils.py \
     data_manager/sync_components.py \
     flows/data_sync_flow.py \
     ml_module/optimizer.py \
     ml_module/trainer.py \
     app/services/data_service.py \
     engine/factors/technical.py \
     app/api/v1/factor.py
   ```

2. Remove unused dependencies from requirements.txt:
   ```diff
   - alphalens-reloaded>=0.4.3
   - xgboost>=2.0.0
   - lightgbm>=4.0.0
   ```

3. Archive debug scripts:
   ```bash
   mkdir -p database/archive
   mv database/debug_st_filter.py database/archive/
   mv database/test_factor_registration.py database/archive/
   mv database/migrate_factors_to_db.py database/archive/
   ```

4. Run tests to verify no breakage:
   ```bash
   pytest tests/
   ```

### Phase 2: Refactor Duplicates (Medium Risk)
1. Extract `build_date_range_query()` utility function
2. Extract `create_sync_log_entry()` function
3. Extract `create_factor_metadata_df()` function
4. Update all call sites
5. Run tests after each refactor

### Phase 3: Verification
1. Run full test suite
2. Start backend server and verify API docs: http://localhost:8000/docs
3. Test critical endpoints manually
4. Check logs for any import errors

---

## Estimated Impact

### Before Cleanup
- Total lines: ~15,000
- Unused imports: 15+
- Duplicate code: ~40 lines
- Unused dependencies: 3

### After Cleanup
- Lines removed: ~200 (1.3%)
- Cleaner imports: 12 files
- Reduced duplication: 3 utility functions
- Faster pip install: -3 heavy ML packages

### Risk Assessment
- **Phase 1**: Very Low Risk (automated tools, unused code)
- **Phase 2**: Low-Medium Risk (requires testing)
- **Phase 3**: Low Risk (verification only)

---

## Notes

1. **API Endpoints**: Do NOT remove any `@router` decorated functions - they're all potentially used by the frontend.

2. **Polars vs Pandas**: The codebase uses Polars as primary data processing library. Pandas is kept for compatibility with some ML libraries (pycaret).

3. **numpy Import**: The `import numpy as np` in `engine/factors/technical.py` appears unused because the code uses Polars expressions. However, verify no numpy operations before removing.

4. **Test Coverage**: Run existing tests in `tests/` folder before and after cleanup to ensure no regressions.

5. **Database Scripts**: The three scripts in `database/` are one-time utilities. Archive them rather than delete for historical reference.

---

## Appendix: Full Vulture Output

See vulture_full_output.txt for complete analysis with lower confidence thresholds.
