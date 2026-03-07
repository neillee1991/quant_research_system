#!/bin/bash

###############################################################################
# 冒烟测试脚本 v2.0
# 用途: 快速验证核心功能是否正常
# 作者: DevOps Team
# 日期: 2026-03-07
###############################################################################

set -e
set -u
set -o pipefail

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 配置变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
API_HOST="${API_HOST:-localhost}"
API_PORT="${API_PORT:-8000}"
API_BASE_URL="http://${API_HOST}:${API_PORT}/api/v1"

# 测试结果
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

###############################################################################
# 工具函数
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $@"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $@"
    ((PASSED_TESTS++))
}

log_error() {
    echo -e "${RED}[✗]${NC} $@"
    ((FAILED_TESTS++))
}

print_banner() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║              冒烟测试 v2.0                                 ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

test_api() {
    local test_name="$1"
    local method="$2"
    local endpoint="$3"
    local expected_status="${4:-200}"
    local data="${5:-}"

    ((TOTAL_TESTS++))

    local url="${API_BASE_URL}${endpoint}"
    local response

    if [[ "${method}" == "GET" ]]; then
        response=$(curl -s -w "\n%{http_code}" -X GET "${url}")
    elif [[ "${method}" == "POST" ]]; then
        response=$(curl -s -w "\n%{http_code}" -X POST "${url}" \
            -H "Content-Type: application/json" \
            -d "${data}")
    fi

    local status_code=$(echo "${response}" | tail -n 1)
    local body=$(echo "${response}" | head -n -1)

    if [[ "${status_code}" == "${expected_status}" ]]; then
        log_success "${test_name} (${method} ${endpoint})"
        return 0
    else
        log_error "${test_name} - 期望状态码 ${expected_status}, 实际 ${status_code}"
        echo "  响应: ${body}"
        return 1
    fi
}

###############################################################################
# 测试用例
###############################################################################

# 1. 基础 API 测试
test_health_check() {
    test_api "健康检查" "GET" "/health" "200"
}

test_api_docs() {
    local url="http://${API_HOST}:${API_PORT}/docs"
    ((TOTAL_TESTS++))

    if curl -f -s -o /dev/null "${url}"; then
        log_success "API 文档访问"
        return 0
    else
        log_error "API 文档访问失败"
        return 1
    fi
}

# 2. 数据 API 测试
test_get_stock_list() {
    test_api "获取股票列表" "GET" "/data/stocks?limit=10" "200"
}

test_get_daily_data() {
    test_api "获取日线数据" "GET" "/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20240131" "200"
}

test_get_sync_tasks() {
    test_api "获取同步任务列表" "GET" "/data/sync/tasks" "200"
}

# 3. 因子 API 测试
test_get_factor_list() {
    test_api "获取因子列表" "GET" "/production/factors" "200"
}

test_get_factor_metadata() {
    test_api "获取因子元数据" "GET" "/production/factors/metadata" "200"
}

# 4. 生产任务测试
test_get_production_tasks() {
    test_api "获取生产任务列表" "GET" "/production/tasks" "200"
}

# 5. 数据库连接测试
test_database_connection() {
    ((TOTAL_TESTS++))

    cd "${PROJECT_ROOT}"
    source .venv/bin/activate

    local result=$(python -c "
from store.dolphindb_client import db_client
try:
    result = db_client.query('select count(*) as cnt from stock_basic')
    if len(result) > 0 and result['cnt'][0] > 0:
        print('OK')
    else:
        print('FAIL')
except Exception as e:
    print(f'ERROR: {e}')
" 2>&1)

    if [[ "${result}" == "OK" ]]; then
        log_success "数据库连接测试"
        return 0
    else
        log_error "数据库连接测试失败: ${result}"
        return 1
    fi
}

# 6. 因子计算测试
test_factor_calculation() {
    ((TOTAL_TESTS++))

    cd "${PROJECT_ROOT}"
    source .venv/bin/activate

    local result=$(python -c "
from engine.production.registry import discover_factors
try:
    factors = discover_factors()
    if len(factors) > 0:
        print('OK')
    else:
        print('FAIL')
except Exception as e:
    print(f'ERROR: {e}')
" 2>&1)

    if [[ "${result}" == "OK" ]]; then
        log_success "因子注册发现测试"
        return 0
    else
        log_error "因子注册发现测试失败: ${result}"
        return 1
    fi
}

# 7. 数据预处理测试
test_data_preprocessing() {
    ((TOTAL_TESTS++))

    cd "${PROJECT_ROOT}"
    source .venv/bin/activate

    local result=$(python -c "
from data_manager.processor import DataProcessor
try:
    processor = DataProcessor()
    print('OK')
except Exception as e:
    print(f'ERROR: {e}')
" 2>&1)

    if [[ "${result}" == "OK" ]]; then
        log_success "数据预处理模块加载"
        return 0
    else
        log_error "数据预处理模块加载失败: ${result}"
        return 1
    fi
}

# 8. 配置加载测试
test_config_loading() {
    ((TOTAL_TESTS++))

    cd "${PROJECT_ROOT}"
    source .venv/bin/activate

    local result=$(python -c "
from app.core.config import settings
try:
    assert settings.app_name is not None
    assert settings.database.dolphindb_host is not None
    print('OK')
except Exception as e:
    print(f'ERROR: {e}')
" 2>&1)

    if [[ "${result}" == "OK" ]]; then
        log_success "配置加载测试"
        return 0
    else
        log_error "配置加载测试失败: ${result}"
        return 1
    fi
}

# 9. 日志系统测试
test_logging_system() {
    ((TOTAL_TESTS++))

    local log_file="${PROJECT_ROOT}/logs/app.log"

    if [[ -f "${log_file}" ]] && [[ -s "${log_file}" ]]; then
        log_success "日志系统正常"
        return 0
    else
        log_error "日志文件不存在或为空"
        return 1
    fi
}

# 10. 性能基准测试
test_api_performance() {
    ((TOTAL_TESTS++))

    local url="${API_BASE_URL}/health"
    local total_time=0
    local iterations=10

    for i in $(seq 1 ${iterations}); do
        local start_time=$(date +%s%N)
        curl -f -s -o /dev/null "${url}"
        local end_time=$(date +%s%N)
        local response_time=$(( (end_time - start_time) / 1000000 ))
        total_time=$((total_time + response_time))
    done

    local avg_time=$((total_time / iterations))

    if [[ ${avg_time} -lt 500 ]]; then
        log_success "API 性能测试 (平均响应时间: ${avg_time}ms)"
        return 0
    else
        log_error "API 性能测试失败 (平均响应时间: ${avg_time}ms > 500ms)"
        return 1
    fi
}

###############################################################################
# 主测试流程
###############################################################################

main() {
    print_banner

    echo ""
    log_info "========== 1. 基础 API 测试 =========="
    test_health_check || true
    test_api_docs || true

    echo ""
    log_info "========== 2. 数据 API 测试 =========="
    test_get_stock_list || true
    test_get_daily_data || true
    test_get_sync_tasks || true

    echo ""
    log_info "========== 3. 因子 API 测试 =========="
    test_get_factor_list || true
    test_get_factor_metadata || true

    echo ""
    log_info "========== 4. 生产任务测试 =========="
    test_get_production_tasks || true

    echo ""
    log_info "========== 5. 核心功能测试 =========="
    test_database_connection || true
    test_factor_calculation || true
    test_data_preprocessing || true
    test_config_loading || true
    test_logging_system || true

    echo ""
    log_info "========== 6. 性能测试 =========="
    test_api_performance || true

    # 打印总结
    echo ""
    echo "=========================================="
    echo "冒烟测试总结:"
    echo "  总测试数: ${TOTAL_TESTS}"
    echo -e "  通过: ${GREEN}${PASSED_TESTS}${NC}"
    echo -e "  失败: ${RED}${FAILED_TESTS}${NC}"
    echo "  通过率: $(( PASSED_TESTS * 100 / TOTAL_TESTS ))%"
    echo "=========================================="

    if [[ ${FAILED_TESTS} -eq 0 ]]; then
        log_success "所有冒烟测试通过！"
        exit 0
    elif [[ ${FAILED_TESTS} -le 2 ]]; then
        log_error "部分测试失败，但可以继续部署"
        exit 0
    else
        log_error "多个测试失败，建议回滚"
        exit 1
    fi
}

# 执行主流程
main "$@"
