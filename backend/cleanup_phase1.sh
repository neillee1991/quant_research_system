#!/bin/bash
# Phase 1: Safe Cleanup - Automated removal of unused imports and variables
# Run this script from the backend directory

set -e  # Exit on error

echo "=========================================="
echo "QuantSystem Backend - Phase 1 Cleanup"
echo "=========================================="
echo ""

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "Error: Must run from backend directory"
    exit 1
fi

# Activate virtual environment
if [ ! -d ".venv" ]; then
    echo "Error: Virtual environment not found"
    exit 1
fi

source .venv/bin/activate

# Ensure autoflake is installed
echo "1. Checking autoflake installation..."
pip show autoflake > /dev/null 2>&1 || pip install autoflake

echo ""
echo "2. Backing up files before cleanup..."
BACKUP_DIR="backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

FILES_TO_CLEAN=(
    "app/api/v1/production.py"
    "app/core/utils.py"
    "data_manager/sync_components.py"
    "flows/data_sync_flow.py"
    "ml_module/optimizer.py"
    "ml_module/trainer.py"
    "app/services/data_service.py"
    "engine/factors/technical.py"
    "app/api/v1/factor.py"
    "app/services/factor_service.py"
)

for file in "${FILES_TO_CLEAN[@]}"; do
    if [ -f "$file" ]; then
        cp "$file" "$BACKUP_DIR/"
        echo "  ✓ Backed up $file"
    fi
done

echo ""
echo "3. Running autoflake to remove unused imports and variables..."
for file in "${FILES_TO_CLEAN[@]}"; do
    if [ -f "$file" ]; then
        echo "  Processing $file..."
        autoflake --in-place --remove-all-unused-imports --remove-unused-variables "$file"
    fi
done

echo ""
echo "4. Archiving debug scripts..."
mkdir -p database/archive
if [ -f "database/debug_st_filter.py" ]; then
    mv database/debug_st_filter.py database/archive/
    echo "  ✓ Archived debug_st_filter.py"
fi
if [ -f "database/test_factor_registration.py" ]; then
    mv database/test_factor_registration.py database/archive/
    echo "  ✓ Archived test_factor_registration.py"
fi
if [ -f "database/migrate_factors_to_db.py" ]; then
    mv database/migrate_factors_to_db.py database/archive/
    echo "  ✓ Archived migrate_factors_to_db.py"
fi

echo ""
echo "5. Updating requirements.txt..."
cp requirements.txt "$BACKUP_DIR/requirements.txt"
sed -i.bak '/alphalens-reloaded/d' requirements.txt
sed -i.bak '/xgboost/d' requirements.txt
sed -i.bak '/lightgbm/d' requirements.txt
rm requirements.txt.bak
echo "  ✓ Removed unused dependencies"

echo ""
echo "=========================================="
echo "Phase 1 Cleanup Complete!"
echo "=========================================="
echo ""
echo "Backup location: $BACKUP_DIR"
echo ""
echo "Next steps:"
echo "  1. Review changes: git diff"
echo "  2. Run tests: pytest tests/"
echo "  3. Start server: python main.py"
echo "  4. If issues occur, restore from: $BACKUP_DIR"
echo ""
echo "To rollback:"
echo "  cp $BACKUP_DIR/* ."
echo ""
