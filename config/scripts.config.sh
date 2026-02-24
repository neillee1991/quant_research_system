#!/bin/bash
# ===================================================================
# 量化研究系统 - 脚本配置文件
# 所有启动脚本的配置参数统一在此定义
# ===================================================================

# ==================== 服务配置 ====================

# 后端服务
BACKEND_HOST="0.0.0.0"
BACKEND_PORT="8000"
BACKEND_RELOAD="--reload"  # 开发模式自动重载，生产环境设为空

# 前端服务
FRONTEND_PORT="3000"

# Prefect 服务
PREFECT_PORT="4200"

# ==================== Docker 服务 ====================

# DolphinDB
DOLPHINDB_CONTAINER="quant_dolphindb"
DOLPHINDB_PORT="8848"

# Prefect
PREFECT_CONTAINER="quant_prefect"

# ==================== 路径配置 ====================

# 项目目录（自动检测）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
FRONTEND_DIR="$SCRIPT_DIR/frontend"

# 日志目录
LOG_DIR="$SCRIPT_DIR/logs"
BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"
PREFECT_WORKER_LOG="$LOG_DIR/prefect_worker.log"

# PID 文件目录
PID_DIR="$SCRIPT_DIR/.pids"
BACKEND_PID="$PID_DIR/backend.pid"
FRONTEND_PID="$PID_DIR/frontend.pid"
PREFECT_WORKER_PID="$PID_DIR/prefect_worker.pid"

# ==================== Python 配置 ====================

# Python 版本要求
PYTHON_MIN_MAJOR=3
PYTHON_MIN_MINOR=11

# 虚拟环境目录
VENV_DIR=".venv"

# ==================== DolphinDB 配置 ====================

# DolphinDB 数据目录（外部卷）
DOLPHINDB_DATA_DIR="/Volumes/QuantData/dolphindb"

# DolphinDB 健康检查
DB_INIT_WAIT=10
DB_MAX_ATTEMPTS=30
DB_CHECK_INTERVAL=3

# ==================== 颜色配置 ====================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

# ==================== 功能开关 ====================

# 是否自动安装依赖（true/false）
AUTO_INSTALL_DEPS=true

# 是否启动 Prefect Worker（true/false）
ENABLE_PREFECT_WORKER=true
