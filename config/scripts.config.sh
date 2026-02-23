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

# ==================== Docker 服务 ====================

# PostgreSQL
POSTGRES_CONTAINER="quant_postgres"
POSTGRES_USER="quant_user"
POSTGRES_DB="quant_research"

# Redis
REDIS_CONTAINER="quant_redis"

# ==================== 路径配置 ====================

# 项目目录（自动检测）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
FRONTEND_DIR="$SCRIPT_DIR/frontend"

# 日志目录
LOG_DIR="$SCRIPT_DIR/logs"
BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"

# PID 文件目录
PID_DIR="$SCRIPT_DIR/.pids"
BACKEND_PID="$PID_DIR/backend.pid"
FRONTEND_PID="$PID_DIR/frontend.pid"

# ==================== Python 配置 ====================

# Python 版本要求
PYTHON_MIN_MAJOR=3
PYTHON_MIN_MINOR=11

# 虚拟环境目录
VENV_DIR=".venv"

# ==================== 数据库配置 ====================

# 数据库初始化等待时间（秒）
DB_INIT_WAIT=5

# 数据库健康检查最大尝试次数
DB_MAX_ATTEMPTS=30

# 数据库健康检查间隔（秒）
DB_CHECK_INTERVAL=2

# 性能索引最小数量（低于此值会提示）
MIN_INDEX_COUNT=20

# ==================== 颜色配置 ====================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

# ==================== 功能开关 ====================

# 是否启动 Redis（true/false）
ENABLE_REDIS=true

# 是否启动 pgAdmin（true/false）
ENABLE_PGADMIN=false

# 是否检查性能索引（true/false）
CHECK_INDEXES=true

# 是否检查 Redis 缓存（true/false）
CHECK_REDIS=true

# 是否自动安装依赖（true/false）
AUTO_INSTALL_DEPS=true

# ==================== 清理配置 ====================

# macOS ExFAT 卷元数据文件清理
PG_DATA_DIR="/Volumes/QuantData/postgresql"
CLEAN_MACOS_METADATA=true
