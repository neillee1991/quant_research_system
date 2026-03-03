# Dead Code Cleanup - Quick Start Guide

## Summary

Analyzed 68 Python files in QuantSystem backend and identified:
- ✅ **15+ unused imports** (safe to remove)
- ✅ **3 unused variables** (safe to remove)
- ✅ **3 unused dependencies** (alphalens, xgboost, lightgbm)
- ✅ **3 debug scripts** (can be archived)
- ✅ **4 duplicate code patterns** (can be refactored)
- ⚠️ **All API endpoints are active** (do NOT remove)

## Quick Actions

### Option 1: Automated Cleanup (Recommended)

Run the automated cleanup script:

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
./cleanup_phase1.sh
```

This will:
1. Backup all files to `backup_YYYYMMDD_HHMMSS/`
2. Remove unused imports and variables using autoflake
3. Archive debug scripts to `database/archive/`
4. Remove unused dependencies from requirements.txt

### Option 2: Manual Review

1. **Read the full report**:
   ```bash
   cat CLEANUP_REPORT.md
   ```

2. **Review specific files**:
   - Unused imports: `app/api/v1/production.py`, `app/core/utils.py`, etc.
   - Duplicate code: See "Category 2" in CLEANUP_REPORT.md

3. **Apply changes selectively** using the commands in the report

## Verification Steps

After cleanup, verify everything works:

```bash
# 1. Run tests
pytest tests/

# 2. Start backend server
python main.py

# 3. Check API docs
# Open http://localhost:8000/docs in browser

# 4. Check for import errors in logs
tail -f logs/app.log
```

## Rollback

If issues occur, restore from backup:

```bash
cp backup_YYYYMMDD_HHMMSS/* .
```

## Files Created

1. **CLEANUP_REPORT.md** - Detailed analysis with all findings
2. **cleanup_phase1.sh** - Automated cleanup script
3. **refactor_utils.py** - Utility functions for duplicate code (Phase 2)
4. **CLEANUP_QUICKSTART.md** - This file

## Risk Assessment

- **Phase 1 (Automated)**: Very Low Risk
  - Removes only confirmed unused code
  - Creates automatic backups
  - Can be rolled back instantly

- **Phase 2 (Refactoring)**: Low-Medium Risk
  - Requires manual code changes
  - Should be done after Phase 1 verification
  - See refactor_utils.py for helper functions

## Expected Benefits

- **Cleaner codebase**: Remove ~200 lines of dead code
- **Faster installs**: Remove 3 heavy ML packages
- **Better maintainability**: Eliminate duplicate patterns
- **Reduced confusion**: Archive one-time scripts

## Important Notes

⚠️ **DO NOT REMOVE**:
- Any `@router` decorated functions (they're API endpoints)
- Pydantic model fields (used for validation)
- Any imports that appear in `__init__.py` files

✅ **SAFE TO REMOVE**:
- Imports flagged by autoflake
- Variables assigned but never read
- Dependencies not imported anywhere
- Debug scripts in database/ folder

## Next Steps

1. Run `./cleanup_phase1.sh`
2. Review changes with `git diff`
3. Run tests with `pytest tests/`
4. Commit changes if tests pass
5. Consider Phase 2 refactoring (optional)

## Questions?

Refer to CLEANUP_REPORT.md for detailed explanations of each finding.
