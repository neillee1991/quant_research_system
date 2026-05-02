#!/bin/bash
# ===================================================================
# 量化研究系统 - 简化的 Shell 脚本统一配置
# 个人使用版本，保留核心配置
# ===================================================================

# ==================== 项目路径配置（固定路径）====================
# 项目根目录
PROJECT_ROOT="/Volumes/LiSh/quantsystem/quant_research_system"

# 各个模块目录
BACKEND_DIR="$PROJECT_ROOT/backend"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
CONFIG_DIR="$PROJECT_ROOT/config"
LOG_DIR="$PROJECT_ROOT/logs"
PID_DIR="$PROJECT_ROOT/.pids"
DATA_DIR="$PROJECT_ROOT/data"

# 配置子目录
ENV_CONFIG_DIR="$CONFIG_DIR/environments"
APP_CONFIG_DIR="$CONFIG_DIR/app"
DOCKER_CONFIG_DIR="$CONFIG_DIR/docker"
TASK_CONFIG_DIR="$CONFIG_DIR/tasks"
SCRIPT_CONFIG_DIR="$CONFIG_DIR/scripts"

# ==================== 服务配置 ====================

# 后端服务
BACKEND_HOST="0.0.0.0"
BACKEND_PORT="8000"
BACKEND_RELOAD="--reload"  # 开发模式自动重载

# 前端服务
FRONTEND_PORT="3000"

# ==================== Docker服务配置 ====================

# DolphinDB
DOLPHINDB_CONTAINER="quant_dolphindb"
DOLPHINDB_PORT="8848"

# PostgreSQL
POSTGRES_CONTAINER="quant_postgres"

# ==================== 路径配置（个人使用固定路径）====================

# DolphinDB 数据目录（真实路径）
DOLPHINDB_DATA_DIR="/Volumes/LiSh/data/dolphin"

# PostgreSQL 数据目录（真实路径）
POSTGRES_DATA_DIR="/Volumes/LiSh/data/postgres"

# ==================== Python配置 ====================

# Python 版本要求
PYTHON_MIN_MAJOR=3
PYTHON_MIN_MINOR=11

# 虚拟环境目录
VENV_DIR="$BACKEND_DIR/.venv"

# ==================== DolphinDB 配置 ====================

# DolphinDB 健康检查
DB_INIT_WAIT=10
DB_MAX_ATTEMPTS=30
DB_CHECK_INTERVAL=3

# PostgreSQL 健康检查
POSTGRES_INIT_WAIT=5
POSTGRES_MAX_ATTEMPTS=10
POSTGRES_CHECK_INTERVAL=2

# ==================== 颜色配置 ====================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'  # No Color

# ==================== 功能开关 ====================

# 是否自动安装依赖（true/false）
AUTO_INSTALL_DEPS=true

# ==================== 配置验证 ====================

# 初始化配置
ensure_directories() {
    mkdir -p "$LOG_DIR" "$PID_DIR" "$DATA_DIR" 2>/dev/null || true
    chmod 750 "$LOG_DIR" "$PID_DIR" "$DATA_DIR" 2>/dev/null || true
}

# 验证配置
validate_config() {
    local errors=0

    # 检查项目目录
    if [ ! -d "$PROJECT_ROOT" ]; then
        log_error "项目根目录不存在: $PROJECT_ROOT"
        errors=1
    fi

    # 检查数据目录
    if [ ! -d "$DOLPHINDB_DATA_DIR" ]; then
        log_warning "DolphinDB 数据目录不存在，将创建: $DOLPHINDB_DATA_DIR"
        mkdir -p "$DOLPHINDB_DATA_DIR" 2>/dev/null || true
        chmod 777 "$DOLPHINDB_DATA_DIR" 2>/dev/null || true
    fi

    if [ ! -d "$POSTGRES_DATA_DIR" ]; then
        log_warning "PostgreSQL 数据目录不存在，将创建: $POSTGRES_DATA_DIR"
        mkdir -p "$POSTGRES_DATA_DIR" 2>/dev/null || true
        chmod 777 "$POSTGRES_DATA_DIR" 2>/dev/null || true
    fi

    return $errors
}

# ==================== 工具函数 ====================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    local debug="${DEBUG:-true}"
    if [ "$debug" = "true" ]; then
        echo -e "${CYAN}[DEBUG]${NC} $1"
    fi
}

# 导出配置供其他脚本使用
export PROJECT_ROOT
export BACKEND_DIR
export FRONTEND_DIR
export CONFIG_DIR
export LOG_DIR
export PID_DIR
export DATA_DIR
export ENV_CONFIG_DIR
export APP_CONFIG_DIR
export DOCKER_CONFIG_DIR
export TASK_CONFIG_DIR
export SCRIPT_CONFIG_DIR

export BACKEND_HOST
export BACKEND_PORT
export BACKEND_RELOAD
export FRONTEND_PORT

export DOLPHINDB_CONTAINER
export DOLPHINDB_PORT
export DOLPHINDB_DATA_DIR
export POSTGRES_CONTAINER
export POSTGRES_DATA_DIR

export PYTHON_MIN_MAJOR
export PYTHON_MIN_MINOR
export VENV_DIR

export DB_INIT_WAIT
export DB_MAX_ATTEMPTS
export DB_CHECK_INTERVAL
export POSTGRES_INIT_WAIT
export POSTGRES_MAX_ATTEMPTS
export POSTGRES_CHECK_INTERVAL

export AUTO_INSTALL_DEPS

# PID 和日志文件路径
BACKEND_PID="$PID_DIR/backend.pid"
FRONTEND_PID="$PID_DIR/frontend.pid"
BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"

export BACKEND_PID
export FRONTEND_PID
export BACKEND_LOG
export FRONTEND_LOG

# 确保目录存在
ensure_directories
