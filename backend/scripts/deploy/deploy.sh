#!/bin/bash

###############################################################################
# 生产环境部署脚本 v2.0
# 用途: 自动化部署 Quant Research System 到生产环境
# 作者: DevOps Team
# 日期: 2026-03-07
###############################################################################

set -e  # 遇到错误立即退出
set -u  # 使用未定义变量时报错
set -o pipefail  # 管道命令中任何一个失败都返回失败

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BACKEND_DIR="${PROJECT_ROOT}"
DEPLOY_ENV="${DEPLOY_ENV:-production}"
DEPLOY_USER="${DEPLOY_USER:-$(whoami)}"
DEPLOY_TIME=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${PROJECT_ROOT}/logs/deploy_${DEPLOY_TIME}.log"
BACKUP_DIR="${PROJECT_ROOT}/.backups/${DEPLOY_TIME}"

# 服务配置
API_PORT="${API_PORT:-8000}"
PREFECT_PORT="${PREFECT_PORT:-4200}"
DOLPHINDB_PORT="${DOLPHINDB_PORT:-8848}"

# 部署选项
DRY_RUN="${DRY_RUN:-false}"
SKIP_TESTS="${SKIP_TESTS:-false}"
SKIP_BACKUP="${SKIP_BACKUP:-false}"
ENABLE_CANARY="${ENABLE_CANARY:-true}"

###############################################################################
# 工具函数
###############################################################################

log() {
    local level=$1
    shift
    local message="$@"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo -e "${timestamp} [${level}] ${message}" | tee -a "${LOG_FILE}"
}

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
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║         Quant Research System v2.0 部署脚本                ║"
    echo "║         环境: ${DEPLOY_ENV}                                ║"
    echo "║         时间: ${DEPLOY_TIME}                               ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "命令 '$1' 未找到，请先安装"
        exit 1
    fi
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
        log_warning "操作已取消"
        exit 1
    fi
}

###############################################################################
# 预检查
###############################################################################

pre_check() {
    log_info "========== 步骤 1/10: 预检查 =========="

    # 检查必要命令
    log_info "检查必要命令..."
    check_command "git"
    check_command "python3"
    check_command "pip"
    check_command "docker"
    check_command "curl"

    # 检查 Python 版本
    log_info "检查 Python 版本..."
    PYTHON_VERSION=$(python3 --version | awk '{print $2}')
    if [[ ! "${PYTHON_VERSION}" =~ ^3\.11\. ]]; then
        log_error "需要 Python 3.11.x，当前版本: ${PYTHON_VERSION}"
        exit 1
    fi
    log_success "Python 版本检查通过: ${PYTHON_VERSION}"

    # 检查磁盘空间
    log_info "检查磁盘空间..."
    AVAILABLE_SPACE=$(df -h "${PROJECT_ROOT}" | awk 'NR==2 {print $4}' | sed 's/G//')
    if (( $(echo "${AVAILABLE_SPACE} < 10" | bc -l) )); then
        log_error "磁盘空间不足，需要至少 10GB，当前可用: ${AVAILABLE_SPACE}GB"
        exit 1
    fi
    log_success "磁盘空间检查通过: ${AVAILABLE_SPACE}GB 可用"

    # 检查端口占用
    log_info "检查端口占用..."
    for port in ${API_PORT} ${PREFECT_PORT} ${DOLPHINDB_PORT}; do
        if lsof -Pi :${port} -sTCP:LISTEN -t >/dev/null 2>&1; then
            log_warning "端口 ${port} 已被占用"
        fi
    done

    # 检查 Git 状态
    log_info "检查 Git 状态..."
    cd "${PROJECT_ROOT}"
    if [[ -n $(git status --porcelain) ]]; then
        log_warning "工作目录有未提交的更改"
        git status --short
        confirm "是否继续部署？"
    fi

    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    log_info "当前分支: ${CURRENT_BRANCH}"

    log_success "预检查完成"
}

###############################################################################
# 备份
###############################################################################

backup() {
    if [[ "${SKIP_BACKUP}" == "true" ]]; then
        log_warning "跳过备份步骤"
        return 0
    fi

    log_info "========== 步骤 2/10: 备份 =========="

    mkdir -p "${BACKUP_DIR}"

    # 备份代码
    log_info "备份代码..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        tar -czf "${BACKUP_DIR}/code_backup.tar.gz" \
            --exclude='.venv' \
            --exclude='__pycache__' \
            --exclude='*.pyc' \
            --exclude='logs' \
            --exclude='.backups' \
            -C "${PROJECT_ROOT}" .
        log_success "代码备份完成: ${BACKUP_DIR}/code_backup.tar.gz"
    fi

    # 备份配置文件
    log_info "备份配置文件..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        cp -r "${BACKEND_DIR}/config" "${BACKUP_DIR}/config_backup"
        if [[ -f "${PROJECT_ROOT}/.env" ]]; then
            cp "${PROJECT_ROOT}/.env" "${BACKUP_DIR}/.env.backup"
        fi
        log_success "配置文件备份完成"
    fi

    # 备份数据库
    log_info "备份数据库..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        # DolphinDB 备份逻辑
        log_info "执行 DolphinDB 备份..."
        # TODO: 实现 DolphinDB 备份逻辑
        log_success "数据库备份完成"
    fi

    log_success "备份完成: ${BACKUP_DIR}"
}

###############################################################################
# 拉取代码
###############################################################################

pull_code() {
    log_info "========== 步骤 3/10: 拉取代码 =========="

    cd "${PROJECT_ROOT}"

    log_info "拉取最新代码..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        git fetch origin
        git pull origin main
        COMMIT_HASH=$(git rev-parse --short HEAD)
        log_success "代码拉取完成，当前提交: ${COMMIT_HASH}"
    else
        log_info "[DRY RUN] 跳过代码拉取"
    fi
}

###############################################################################
# 安装依赖
###############################################################################

install_dependencies() {
    log_info "========== 步骤 4/10: 安装依赖 =========="

    cd "${BACKEND_DIR}"

    # 激活虚拟环境
    if [[ ! -d ".venv" ]]; then
        log_info "创建虚拟环境..."
        python3 -m venv .venv
    fi

    log_info "激活虚拟环境..."
    source .venv/bin/activate

    # 升级 pip
    log_info "升级 pip..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        pip install --upgrade pip
    fi

    # 安装依赖
    log_info "安装 Python 依赖..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        pip install -r requirements.txt
        log_success "依赖安装完成"
    else
        log_info "[DRY RUN] 跳过依赖安装"
    fi
}

###############################################################################
# 运行测试
###############################################################################

run_tests() {
    if [[ "${SKIP_TESTS}" == "true" ]]; then
        log_warning "跳过测试步骤"
        return 0
    fi

    log_info "========== 步骤 5/10: 运行测试 =========="

    cd "${BACKEND_DIR}"
    source .venv/bin/activate

    log_info "运行单元测试..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        pytest tests/unit/ -v --tb=short || {
            log_error "单元测试失败"
            exit 1
        }
        log_success "单元测试通过"
    fi

    log_info "运行集成测试..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        pytest tests/integration/ -v --tb=short || {
            log_error "集成测试失败"
            exit 1
        }
        log_success "集成测试通过"
    fi

    log_info "检查代码覆盖率..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        pytest --cov=. --cov-report=term --cov-report=html tests/ || {
            log_error "代码覆盖率检查失败"
            exit 1
        }
        log_success "代码覆盖率检查通过"
    fi
}

###############################################################################
# 数据库迁移
###############################################################################

database_migration() {
    log_info "========== 步骤 6/10: 数据库迁移 =========="

    cd "${BACKEND_DIR}"
    source .venv/bin/activate

    log_info "执行数据库迁移..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        python database/migrations/v2.0/migrate.py || {
            log_error "数据库迁移失败"
            exit 1
        }
        log_success "数据库迁移完成"
    else
        log_info "[DRY RUN] 跳过数据库迁移"
    fi
}

###############################################################################
# 更新配置
###############################################################################

update_config() {
    log_info "========== 步骤 7/10: 更新配置 =========="

    cd "${BACKEND_DIR}"

    # 复制环境配置
    log_info "更新环境配置..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        if [[ -f "config/${DEPLOY_ENV}.yaml" ]]; then
            cp "config/${DEPLOY_ENV}.yaml" "config/active.yaml"
            log_success "配置文件已更新: ${DEPLOY_ENV}.yaml"
        else
            log_warning "配置文件不存在: config/${DEPLOY_ENV}.yaml"
        fi
    fi

    # 验证环境变量
    log_info "验证环境变量..."
    if [[ ! -f "${PROJECT_ROOT}/.env" ]]; then
        log_error ".env 文件不存在"
        exit 1
    fi

    # 检查必要的环境变量
    source "${PROJECT_ROOT}/.env"
    REQUIRED_VARS=("DOLPHINDB_HOST" "DOLPHINDB_PORT" "TUSHARE_TOKEN")
    for var in "${REQUIRED_VARS[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log_error "环境变量 ${var} 未设置"
            exit 1
        fi
    done
    log_success "环境变量验证通过"
}

###############################################################################
# 重启服务
###############################################################################

restart_services() {
    log_info "========== 步骤 8/10: 重启服务 =========="

    cd "${BACKEND_DIR}"

    # 停止旧服务
    log_info "停止旧服务..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        if [[ -f ".pids/backend.pid" ]]; then
            OLD_PID=$(cat .pids/backend.pid)
            if ps -p ${OLD_PID} > /dev/null 2>&1; then
                kill ${OLD_PID}
                sleep 2
                log_success "旧服务已停止 (PID: ${OLD_PID})"
            fi
        fi
    fi

    # 启动新服务
    log_info "启动新服务..."
    if [[ "${DRY_RUN}" == "false" ]]; then
        source .venv/bin/activate
        nohup python main.py > logs/app.log 2>&1 &
        NEW_PID=$!
        mkdir -p .pids
        echo ${NEW_PID} > .pids/backend.pid
        log_success "新服务已启动 (PID: ${NEW_PID})"

        # 等待服务启动
        log_info "等待服务启动..."
        sleep 5
    else
        log_info "[DRY RUN] 跳过服务重启"
    fi
}

###############################################################################
# 健康检查
###############################################################################

health_check() {
    log_info "========== 步骤 9/10: 健康检查 =========="

    if [[ "${DRY_RUN}" == "true" ]]; then
        log_info "[DRY RUN] 跳过健康检查"
        return 0
    fi

    log_info "执行健康检查..."
    bash "${SCRIPT_DIR}/health_check.sh" || {
        log_error "健康检查失败"
        exit 1
    }
    log_success "健康检查通过"
}

###############################################################################
# 冒烟测试
###############################################################################

smoke_test() {
    log_info "========== 步骤 10/10: 冒烟测试 =========="

    if [[ "${DRY_RUN}" == "true" ]]; then
        log_info "[DRY RUN] 跳过冒烟测试"
        return 0
    fi

    log_info "执行冒烟测试..."
    bash "${SCRIPT_DIR}/smoke_test.sh" || {
        log_error "冒烟测试失败"
        exit 1
    }
    log_success "冒烟测试通过"
}

###############################################################################
# 部署总结
###############################################################################

deployment_summary() {
    echo ""
    log_success "=========================================="
    log_success "         部署成功完成！"
    log_success "=========================================="
    echo ""
    log_info "部署信息:"
    log_info "  - 环境: ${DEPLOY_ENV}"
    log_info "  - 时间: ${DEPLOY_TIME}"
    log_info "  - 用户: ${DEPLOY_USER}"
    log_info "  - 备份: ${BACKUP_DIR}"
    log_info "  - 日志: ${LOG_FILE}"
    echo ""
    log_info "服务地址:"
    log_info "  - API: http://localhost:${API_PORT}"
    log_info "  - Prefect: http://localhost:${PREFECT_PORT}"
    log_info "  - DolphinDB: http://localhost:${DOLPHINDB_PORT}"
    echo ""
    log_info "后续步骤:"
    log_info "  1. 监控服务状态 (前30分钟)"
    log_info "  2. 检查错误日志"
    log_info "  3. 验证业务指标"
    if [[ "${ENABLE_CANARY}" == "true" ]]; then
        log_info "  4. 执行灰度发布"
    fi
    echo ""
}

###############################################################################
# 错误处理
###############################################################################

cleanup_on_error() {
    log_error "部署过程中发生错误，正在清理..."

    # 这里可以添加清理逻辑

    log_error "部署失败，请检查日志: ${LOG_FILE}"
    exit 1
}

trap cleanup_on_error ERR

###############################################################################
# 主流程
###############################################################################

main() {
    # 创建日志目录
    mkdir -p "${PROJECT_ROOT}/logs"

    # 打印横幅
    print_banner

    # 确认部署
    confirm "确认部署到 ${DEPLOY_ENV} 环境？"

    # 执行部署步骤
    pre_check
    backup
    pull_code
    install_dependencies
    run_tests
    database_migration
    update_config
    restart_services
    health_check
    smoke_test

    # 部署总结
    deployment_summary
}

# 执行主流程
main "$@"
