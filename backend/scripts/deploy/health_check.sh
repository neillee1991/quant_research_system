#!/bin/bash

###############################################################################
# 服务健康检查脚本 v2.0
# 用途: 检查所有服务的健康状态
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
PREFECT_HOST="${PREFECT_HOST:-localhost}"
PREFECT_PORT="${PREFECT_PORT:-4200}"
DOLPHINDB_HOST="${DOLPHINDB_HOST:-localhost}"
DOLPHINDB_PORT="${DOLPHINDB_PORT:-8848}"

# 健康检查配置
MAX_RETRIES=3
RETRY_DELAY=2
TIMEOUT=10

# 结果统计
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0

###############################################################################
# 工具函数
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $@"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $@"
    ((PASSED_CHECKS++))
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $@"
}

log_error() {
    echo -e "${RED}[✗]${NC} $@"
    ((FAILED_CHECKS++))
}

print_banner() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║              服务健康检查 v2.0                             ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

check_with_retry() {
    local check_name="$1"
    local check_func="$2"

    ((TOTAL_CHECKS++))

    for i in $(seq 1 ${MAX_RETRIES}); do
        if ${check_func}; then
            log_success "${check_name}"
            return 0
        fi

        if [[ ${i} -lt ${MAX_RETRIES} ]]; then
            log_warning "${check_name} - 重试 ${i}/${MAX_RETRIES}..."
            sleep ${RETRY_DELAY}
        fi
    done

    log_error "${check_name} - 失败"
    return 1
}

###############################################################################
# 基础检查
###############################################################################

check_port() {
    local port=$1
    lsof -Pi :${port} -sTCP:LISTEN -t >/dev/null 2>&1
}

check_http() {
    local url=$1
    curl -f -s -o /dev/null --max-time ${TIMEOUT} "${url}"
}

###############################################################################
# API 服务检查
###############################################################################

check_api_port() {
    check_port ${API_PORT}
}

check_api_health() {
    check_http "http://${API_HOST}:${API_PORT}/health"
}

check_api_docs() {
    check_http "http://${API_HOST}:${API_PORT}/docs"
}

check_api_response_time() {
    local start_time=$(date +%s%N)
    curl -f -s -o /dev/null --max-time ${TIMEOUT} "http://${API_HOST}:${API_PORT}/health"
    local end_time=$(date +%s%N)
    local response_time=$(( (end_time - start_time) / 1000000 ))

    if [[ ${response_time} -lt 1000 ]]; then
        log_success "API 响应时间: ${response_time}ms"
        return 0
    else
        log_warning "API 响应时间过长: ${response_time}ms"
        return 1
    fi
}

###############################################################################
# DolphinDB 检查
###############################################################################

check_dolphindb_port() {
    check_port ${DOLPHINDB_PORT}
}

check_dolphindb_connection() {
    cd "${PROJECT_ROOT}"
    source .venv/bin/activate
    python -c "
from store.dolphindb_client import db_client
try:
    result = db_client.query('select 1 as test')
    exit(0 if len(result) > 0 else 1)
except Exception as e:
    print(f'Error: {e}')
    exit(1)
" 2>/dev/null
}

check_dolphindb_tables() {
    cd "${PROJECT_ROOT}"
    source .venv/bin/activate
    python -c "
from store.dolphindb_client import db_client
try:
    # 检查关键表是否存在
    tables = ['daily_data', 'factor_values', 'factor_metadata']
    for table in tables:
        result = db_client.query(f'select count(*) from {table}')
        if result is None:
            exit(1)
    exit(0)
except Exception as e:
    print(f'Error: {e}')
    exit(1)
" 2>/dev/null
}

###############################################################################
# Prefect 检查
###############################################################################

check_prefect_port() {
    check_port ${PREFECT_PORT}
}

check_prefect_api() {
    check_http "http://${PREFECT_HOST}:${PREFECT_PORT}/api/health"
}

###############################################################################
# 系统资源检查
###############################################################################

check_disk_space() {
    local available=$(df -h "${PROJECT_ROOT}" | awk 'NR==2 {print $4}' | sed 's/G//')
    if (( $(echo "${available} > 10" | bc -l) )); then
        log_success "磁盘空间充足: ${available}GB 可用"
        return 0
    else
        log_warning "磁盘空间不足: ${available}GB 可用"
        return 1
    fi
}

check_memory() {
    local available=$(vm_stat | grep "Pages free" | awk '{print $3}' | sed 's/\.//')
    local available_mb=$((available * 4096 / 1024 / 1024))

    if [[ ${available_mb} -gt 1024 ]]; then
        log_success "内存充足: ${available_mb}MB 可用"
        return 0
    else
        log_warning "内存不足: ${available_mb}MB 可用"
        return 1
    fi
}

check_cpu() {
    local cpu_usage=$(top -l 1 | grep "CPU usage" | awk '{print $3}' | sed 's/%//')

    if (( $(echo "${cpu_usage} < 80" | bc -l) )); then
        log_success "CPU 使用率正常: ${cpu_usage}%"
        return 0
    else
        log_warning "CPU 使用率过高: ${cpu_usage}%"
        return 1
    fi
}

###############################################################################
# 日志检查
###############################################################################

check_error_logs() {
    local log_file="${PROJECT_ROOT}/logs/app.log"

    if [[ ! -f "${log_file}" ]]; then
        log_warning "日志文件不存在: ${log_file}"
        return 1
    fi

    # 检查最近5分钟的错误日志
    local error_count=$(tail -n 1000 "${log_file}" | grep -i "error\|exception\|critical" | wc -l)

    if [[ ${error_count} -eq 0 ]]; then
        log_success "无错误日志"
        return 0
    elif [[ ${error_count} -lt 10 ]]; then
        log_warning "发现 ${error_count} 条错误日志"
        return 0
    else
        log_error "发现大量错误日志: ${error_count} 条"
        return 1
    fi
}

###############################################################################
# 业务功能检查
###############################################################################

check_data_sync() {
    cd "${PROJECT_ROOT}"
    source .venv/bin/activate
    python -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/quant'))
    cur = conn.cursor()
    cur.execute(\"SELECT 1 FROM task_runs WHERE task_type = 'sync' AND status = 'success' ORDER BY finished_at DESC LIMIT 1\")
    row = cur.fetchone()
    conn.close()
    exit(0 if row else 1)
except Exception as e:
    print(f'Error: {e}')
    exit(1)
" 2>/dev/null
}

check_factor_calculation() {
    cd "${PROJECT_ROOT}"
    source .venv/bin/activate
    python -c "
from store.dolphindb_client import db_client
try:
    # 检查因子计算结果
    result = db_client.query('''
        select count(*) as cnt from factor_values
        where trade_date = (select max(trade_date) from factor_values)
    ''')
    if len(result) > 0 and result['cnt'][0] > 0:
        exit(0)
    exit(1)
except Exception as e:
    print(f'Error: {e}')
    exit(1)
" 2>/dev/null
}

###############################################################################
# 主检查流程
###############################################################################

main() {
    print_banner

    echo ""
    log_info "========== 1. API 服务检查 =========="
    check_with_retry "API 端口监听" check_api_port
    check_with_retry "API 健康检查" check_api_health
    check_with_retry "API 文档访问" check_api_docs
    ((TOTAL_CHECKS++))
    check_api_response_time || true

    echo ""
    log_info "========== 2. DolphinDB 检查 =========="
    check_with_retry "DolphinDB 端口监听" check_dolphindb_port
    check_with_retry "DolphinDB 连接" check_dolphindb_connection
    check_with_retry "DolphinDB 表结构" check_dolphindb_tables

    echo ""
    log_info "========== 3. Prefect 检查 =========="
    check_with_retry "Prefect 端口监听" check_prefect_port
    check_with_retry "Prefect API" check_prefect_api

    echo ""
    log_info "========== 4. 系统资源检查 =========="
    ((TOTAL_CHECKS++))
    check_disk_space || true
    ((TOTAL_CHECKS++))
    check_memory || true
    ((TOTAL_CHECKS++))
    check_cpu || true

    echo ""
    log_info "========== 5. 日志检查 =========="
    ((TOTAL_CHECKS++))
    check_error_logs || true

    echo ""
    log_info "========== 6. 业务功能检查 =========="
    check_with_retry "数据同步功能" check_data_sync
    check_with_retry "因子计算功能" check_factor_calculation

    # 打印总结
    echo ""
    echo "=========================================="
    echo "健康检查总结:"
    echo "  总检查项: ${TOTAL_CHECKS}"
    echo -e "  通过: ${GREEN}${PASSED_CHECKS}${NC}"
    echo -e "  失败: ${RED}${FAILED_CHECKS}${NC}"
    echo "=========================================="

    if [[ ${FAILED_CHECKS} -eq 0 ]]; then
        log_success "所有检查通过！"
        exit 0
    else
        log_error "部分检查失败，请检查日志"
        exit 1
    fi
}

# 执行主流程
main "$@"
