#!/bin/bash
# Version Control Test Runner
# Runs version control tests with proper environment setup

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(dirname "$SCRIPT_DIR")"

cd "$BACKEND_DIR"

echo "=========================================="
echo "Version Control Test Suite"
echo "=========================================="
echo ""

# Check if DolphinDB is running
echo "Checking DolphinDB connection..."
if ! nc -z localhost 8848 2>/dev/null; then
    echo "ERROR: DolphinDB is not running on localhost:8848"
    echo "Please start DolphinDB before running tests"
    exit 1
fi
echo "✓ DolphinDB is running"
echo ""

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
    echo "✓ Virtual environment activated"
    echo ""
fi

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "ERROR: pytest is not installed"
    echo "Install with: pip install pytest pytest-cov"
    exit 1
fi

# Run tests based on argument
case "${1:-all}" in
    schema)
        echo "Running Schema Validation Tests..."
        pytest tests/test_version_control.py::TestSchemaChanges -v
        ;;
    operations)
        echo "Running Version Operations Tests..."
        pytest tests/test_version_control.py::TestVersionOperations -v
        ;;
    integrity)
        echo "Running Data Integrity Tests..."
        pytest tests/test_version_control.py::TestDataIntegrity -v
        ;;
    integration)
        echo "Running Integration Tests..."
        pytest tests/test_version_control.py::TestIntegration -v
        ;;
    performance)
        echo "Running Performance Tests..."
        pytest tests/test_version_control.py::TestPerformance -v
        ;;
    coverage)
        echo "Running All Tests with Coverage..."
        pytest tests/test_version_control.py \
            --cov=store \
            --cov=app/services \
            --cov=app/api \
            --cov-report=html \
            --cov-report=term \
            -v
        echo ""
        echo "Coverage report generated in htmlcov/index.html"
        ;;
    all)
        echo "Running All Version Control Tests..."
        pytest tests/test_version_control.py -v
        ;;
    *)
        echo "Usage: $0 {schema|operations|integrity|integration|performance|coverage|all}"
        echo ""
        echo "Test Categories:"
        echo "  schema       - Schema validation tests"
        echo "  operations   - Version CRUD operations"
        echo "  integrity    - Data integrity tests"
        echo "  integration  - End-to-end integration tests"
        echo "  performance  - Performance tests"
        echo "  coverage     - All tests with coverage report"
        echo "  all          - All tests (default)"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Test run complete"
echo "=========================================="
