#!/bin/bash

###############################################################################
# 生产环境回滚脚本 v2.0
# 用途: 快速回滚到上一个稳定版本
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
BACKEND_DIR="${PROJECT_ROOT}"
ROLLBACK_TIME=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${PROJECT_ROOT}/logs/rollback_${ROLLBACK_TIME}.log"
BACKUP_DIR="${PROJECT_ROOT}/.backups"

# 回滚选项
ROLLBACK_TARGET="${1:-latest}"  # latest 或具体的备份时间戳
DRY_RUN="${DRY_RUN:-false}"

###############################################################################
# 工具函数
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $@" | tee -a "${LOG_FILE}"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $@" | tee -a "${LOG_FILE}"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $@" | tee -a "${LOG_FILE}"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $@" | tee -a "${LOG_FILE}"
}

print_banner() {
    echo -e "${RED}"
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║              紧急回滚脚本 v2.0                             ║"
    echo "║              时间: ${ROLLBACK_TIME}                        ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

confirm() {
    local message="$1"
    if [[ "${DRY_RUN}" == "true" ]]; then
        log_info "[DRY RUN] 跳过确认: ${message}"
        return 0
    fi

    read -p "${message} (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_warning "回滚已取消"
        exit 1
    fi
}

###############################################################################
# 查找备份
###############################################################################

find_backup() {
    log_info "========== 步骤 1/6: 查找备份 =========="

    if [[ ! -d "${BACKUP_DIR}" ]]; then
        log_error "备份目录不存在: ${BACKUP_DIR}"
        exit 1
    fi

    # 列出所有备份
    log_info "可用的备份:"
    BACKUPS=($(ls -1t "${BACKUP_DIR}" | head -10))

    if [[ ${#BACKUPS[@]} -eq 0 ]]; then
        log_error "没有找到可用的备份"
        exit 1
    fi

    for i in "${!BACKUPS[@]}"; do
        echo "  [$i] ${BACKUPS[$i]}"
    done

    # 选择备份
    if [[ "${ROLLBACK_TARGET}" == "latest" ]]; then
        SELECTED_BACKUP="${BACKUPS[0]}"
        log_info "自动选择最新备份: ${SELECTED_BACKUP}"
    else
        SELECTED_BACKUP="${ROLLBACK_TARGET}"
    fi

    BACKUP_PATH="${BACKUP_DIR}/${SELECTED_BACKUP}"

    if [[ ! -d "${BACKUP_PATH}" ]]; then
        log_error "备份不存在: ${BACKUP_PATH}"
        exit 1
    fi

    log_success "选择备份: ${SELECTED_BACKUP}"
    export BACKUP_PATH
}

###############################################################################
# 停止服务
###############################################################################

stop_services() {
    log_info "========== 步骤 2/6: 停止服务 =========="

    cd "${BACKEND_DIR}"

    if [[ "${DRY_RUN}" == "false" ]]; then
        # 停止 Backend
        if [[ -f ".pids/backend.pid" ]]; then
            BACKEND_PID=$(cat .pids/backend.pid)
            if ps -p ${BACKEND_PID} > /dev/null 2>&1; then
                log_info "停止 Backend 服务 (PID: ${BACKEND_PID})..."
                kill ${BACKEND_PID}
                sleep 2

                # 强制杀死
                if ps -p ${BACKEND_PID} > /dev/null 2>&1; then
                    kill -9 ${BACKEND_PID}
                fi
                log_success "Backend 服务已停止"
            fi
        fi

        # 停止 Prefect
        log_info "停止 Prefect 服务..."
        pkill -f "prefect server" || true

        log_success "所有服务已停止"
    else
        log_info "[DRY RUN] 跳过停止服务"
    fi
}

###############################################################################
# 回滚代码
###############################################################################

rollback_code() {
    log_info "========== 步骤 3/6: 回滚代码 =========="

    cd "${PROJECT_ROOT}"

    if [[ "${DRY_RUN}" == "false" ]]; then
        # 解压备份
        log_info "恢复代码备份..."
        if [[ -f "${BACKUP_PATH}/code_backup.tar.gz" ]]; then
            tar -xzf "${BACKUP_PATH}/code_backup.tar.gz" -C "${PROJECT_ROOT}"
            log_success "代码已恢复"
        else
            log_error "代码备份文件不存在"
            exit 1
        fi
    else
        log_info "[DRY RUN] 跳过代码回滚"
    fi
}

###############################################################################
# 回滚配置
###############################################################################

rollback_config() {
    log_info "========== 步骤 4/6: 回滚配置 =========="

    if [[ "${DRY_RUN}" == "false" ]]; then
        # 恢复配置文件
        log_info "恢复配置文件..."
        if [[ -d "${BACKUP_PATH}/config_backup" ]]; then
            rm -rf "${BACKEND_DIR}/config"
            cp -r "${BACKUP_PATH}/config_backup" "${BACKEND_DIR}/config"
            log_success "配置文件已恢复"
        fi

        # 恢复 .env
        if [[ -f "${BACKUP_PATH}/.env.backup" ]]; then
            cp "${BACKUP_PATH}/.env.backup" "${PROJECT_ROOT}/.env"
            log_success ".env 文件已恢复"
        fi
    else
        log_info "[DRY RUN] 跳过配置回滚"
    fi
}

###############################################################################
# 回滚数据库
###############################################################################

rollback_database() {
    log_info "========== 步骤 5/6: 回滚数据库 =========="

    cd "${BACKEND_DIR}"

    if [[ "${DRY_RUN}" == "false" ]]; then
        log_warning "数据库回滚需要手动操作，请直接在 PostgreSQL 中执行相应的 DROP/ALTER 语句"
        log_success "数据库回滚步骤已跳过"
    else
        log_info "[DRY RUN] 跳过数据库回滚"
    fi
}

###############################################################################
# 启动服务
###############################################################################

start_services() {
    log_info "========== 步骤 6/6: 启动服务 =========="

    cd "${BACKEND_DIR}"

    if [[ "${DRY_RUN}" == "false" ]]; then
        # 启动 Backend
        log_info "启动 Backend 服务..."
        source .venv/bin/activate
        nohup python main.py > logs/app.log 2>&1 &
        BACKEND_PID=$!
        mkdir -p .pids
        echo ${BACKEND_PID} > .pids/backend.pid
        log_success "Backend 服务已启动 (PID: ${BACKEND_PID})"

        # 等待服务启动
        log_info "等待服务启动..."
        sleep 5

        # 健康检查
        log_info "执行健康检查..."
        bash "${SCRIPT_DIR}/health_check.sh" || {
            log_error "健康检查失败"
            exit 1
        }
        log_success "健康检查通过"
    else
        log_info "[DRY RUN] 跳过启动服务"
    fi
}

###############################################################################
# 回滚总结
###############################################################################

rollback_summary() {
    echo ""
    log_success "=========================================="
    log_success "         回滚成功完成！"
    log_success "=========================================="
    echo ""
    log_info "回滚信息:"
    log_info "  - 时间: ${ROLLBACK_TIME}"
    log_info "  - 备份: ${SELECTED_BACKUP}"
    log_info "  - 日志: ${LOG_FILE}"
    echo ""
    log_warning "后续步骤:"
    log_warning "  1. 监控服务状态 (前30分钟)"
    log_warning "  2. 检查错误日志"
    log_warning "  3. 验证业务功能"
    log_warning "  4. 通知相关人员"
    log_warning "  5. 分析回滚原因"
    echo ""
}

###############################################################################
# 主流程
###############################################################################

main() {
    # 创建日志目录
    mkdir -p "${PROJECT_ROOT}/logs"

    # 打印横幅
    print_banner

    # 警告
    log_error "警告: 回滚操作将覆盖当前代码和配置！"
    confirm "确认执行回滚操作？"

    # 执行回滚步骤
    find_backup
    stop_services
    rollback_code
    rollback_config
    rollback_database
    start_services

    # 回滚总结
    rollback_summary
}

# 执行主流程
main "$@"
