#!/bin/bash
# 测试文件清单和验证脚本

echo "=========================================="
echo "测试覆盖率提升工作交付清单"
echo "=========================================="
echo ""

# 定义颜色
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查文件是否存在
check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}✅${NC} $1"
        return 0
    else
        echo -e "${RED}❌${NC} $1 (缺失)"
        return 1
    fi
}

# 统计变量
total=0
passed=0

echo "📁 新增测试文件"
echo "----------------------------------------"
files=(
    "test_metadata_manager.py"
    "test_data_operations.py"
    "test_factor_compute_service.py"
)
for file in "${files[@]}"; do
    total=$((total + 1))
    if check_file "$file"; then
        passed=$((passed + 1))
    fi
done

echo ""
echo "📁 扩展测试文件"
echo "----------------------------------------"
files=(
    "test_pipeline_integration.py"
)
for file in "${files[@]}"; do
    total=$((total + 1))
    if check_file "$file"; then
        passed=$((passed + 1))
    fi
done

echo ""
echo "🛠️ 测试工具"
echo "----------------------------------------"
files=(
    "conftest.py"
    "run_coverage.sh"
    "generate_coverage_report.py"
)
for file in "${files[@]}"; do
    total=$((total + 1))
    if check_file "$file"; then
        passed=$((passed + 1))
    fi
done

echo ""
echo "📚 文档文件"
echo "----------------------------------------"
files=(
    "README.md"
    "QUICK_START.md"
    "TEST_EXECUTION.md"
    "COVERAGE_IMPROVEMENT_SUMMARY.md"
    "COMPLETION_REPORT.md"
    "FINAL_SUMMARY.md"
    "INDEX.md"
)
for file in "${files[@]}"; do
    total=$((total + 1))
    if check_file "$file"; then
        passed=$((passed + 1))
    fi
done

echo ""
echo "=========================================="
echo "交付清单统计"
echo "=========================================="
echo "总文件数: $total"
echo "已交付: $passed"
echo "缺失: $((total - passed))"

if [ $passed -eq $total ]; then
    echo -e "${GREEN}✅ 所有文件已交付！${NC}"
    exit_code=0
else
    echo -e "${RED}❌ 有文件缺失，请检查${NC}"
    exit_code=1
fi

echo ""
echo "=========================================="
echo "下一步操作"
echo "=========================================="
echo "1. 运行测试: ./run_coverage.sh"
echo "2. 查看报告: python generate_coverage_report.py"
echo "3. 阅读文档: cat QUICK_START.md"
echo ""

exit $exit_code
