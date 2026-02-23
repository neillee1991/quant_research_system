#!/bin/bash

# 量化研究系统一键启动脚本
# 自动启动数据库、后端和前端服务

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
FRONTEND_DIR="$SCRIPT_DIR/frontend"

# 日志文件
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"

# PID 文件
PID_DIR="$SCRIPT_DIR/.pids"
mkdir -p "$PID_DIR"
BACKEND_PID="$PID_DIR/backend.pid"
FRONTEND_PID="$PID_DIR/frontend.pid"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   量化研究系统 - 一键启动脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查是否已经在运行
check_running() {
    if [ -f "$BACKEND_PID" ] && kill -0 $(cat "$BACKEND_PID") 2>/dev/null; then
        echo -e "${YELLOW}⚠️  后端服务已在运行 (PID: $(cat "$BACKEND_PID"))${NC}"
        return 0
    fi
    if [ -f "$FRONTEND_PID" ] && kill -0 $(cat "$FRONTEND_PID") 2>/dev/null; then
        echo -e "${YELLOW}⚠️  前端服务已在运行 (PID: $(cat "$FRONTEND_PID"))${NC}"
        return 0
    fi
    return 1
}

# 停止已有服务
stop_services() {
    echo -e "${YELLOW}正在停止已有服务...${NC}"

    if [ -f "$BACKEND_PID" ]; then
        BACKEND_PID_NUM=$(cat "$BACKEND_PID")
        if kill -0 "$BACKEND_PID_NUM" 2>/dev/null; then
            kill "$BACKEND_PID_NUM"
            echo -e "${GREEN}✓ 后端服务已停止${NC}"
        fi
        rm -f "$BACKEND_PID"
    fi

    if [ -f "$FRONTEND_PID" ]; then
        FRONTEND_PID_NUM=$(cat "$FRONTEND_PID")
        if kill -0 "$FRONTEND_PID_NUM" 2>/dev/null; then
            kill "$FRONTEND_PID_NUM"
            echo -e "${GREEN}✓ 前端服务已停止${NC}"
        fi
        rm -f "$FRONTEND_PID"
    fi

    sleep 2
}

# 检查 Docker
check_docker() {
    echo -e "${BLUE}[1/5] 检查 Docker 环境...${NC}"

    if ! command -v docker &> /dev/null; then
        echo -e "${RED}✗ Docker 未安装${NC}"
        exit 1
    fi

    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}✗ Docker 未运行，请先启动 Docker${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Docker 已运行${NC}"
}

# 启动数据库
start_database() {
    echo -e "${BLUE}[2/5] 启动 PostgreSQL 数据库...${NC}"

    cd "$SCRIPT_DIR"

    # 清理 macOS 在 ExFAT 卷上生成的 ._ 文件，避免 PostgreSQL 启动失败
    PG_DATA_DIR="/Volumes/QuantData/postgresql"
    if [ -d "$PG_DATA_DIR" ]; then
        echo -e "${YELLOW}清理 macOS 元数据文件...${NC}"
        find "$PG_DATA_DIR" -name "._*" -delete 2>/dev/null || true
    fi

    docker-compose up -d

    echo -e "${YELLOW}等待数据库初始化...${NC}"
    sleep 5

    # 检查数据库健康状态
    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if docker exec quant_postgres pg_isready -U quant_user -d quant_research > /dev/null 2>&1; then
            echo -e "${GREEN}✓ 数据库已就绪${NC}"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    echo -e "${RED}✗ 数据库启动超时${NC}"
    exit 1
}

# 检查 Python 环境
check_python() {
    echo -e "${BLUE}[3/5] 检查 Python 环境...${NC}"

    # 优先使用 python3.11，回退到 python3
    if command -v python3.11 &> /dev/null; then
        PYTHON_CMD="python3.11"
    elif command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    else
        echo -e "${RED}✗ Python3 未安装${NC}"
        exit 1
    fi

    PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
    PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
    PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

    if [ "$PYTHON_MAJOR" -lt 3 ] || [ "$PYTHON_MINOR" -lt 11 ]; then
        echo -e "${RED}✗ 需要 Python 3.11+，当前: $PYTHON_VERSION${NC}"
        echo -e "${YELLOW}  请安装 Python 3.11: brew install python@3.11${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Python 已安装: $PYTHON_CMD ($PYTHON_VERSION)${NC}"
}

# 初始化数据库表
init_database() {
    echo -e "${BLUE}[4/6] 初始化数据库表...${NC}"

    cd "$BACKEND_DIR"

    # 检查虚拟环境
    if [ ! -d "venv" ] && [ ! -d ".venv" ]; then
        echo -e "${YELLOW}⚠️  虚拟环境不存在，正在创建...${NC}"
        $PYTHON_CMD -m venv .venv
        source .venv/bin/activate
        pip install -r requirements.txt
    else
        if [ -d ".venv" ]; then
            source .venv/bin/activate
        else
            source venv/bin/activate
        fi
    fi

    # 运行数据库初始化脚本
    echo -e "${YELLOW}检查并创建数据库表...${NC}"
    $PYTHON_CMD init_database.py

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 数据库初始化完成${NC}"
    else
        echo -e "${RED}✗ 数据库初始化失败${NC}"
        exit 1
    fi
}

# 启动后端
start_backend() {
    echo -e "${BLUE}[5/6] 启动后端服务...${NC}"

    cd "$BACKEND_DIR"

    # 激活虚拟环境
    if [ -d ".venv" ]; then
        source .venv/bin/activate
    else
        source venv/bin/activate
    fi

    # 启动后端服务
    echo -e "${YELLOW}启动 FastAPI 服务器...${NC}"
    nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload > "$BACKEND_LOG" 2>&1 &
    echo $! > "$BACKEND_PID"

    # 等待后端启动
    echo -e "${YELLOW}等待后端服务启动...${NC}"
    for i in {1..30}; do
        if curl -s http://localhost:8000/docs > /dev/null 2>&1; then
            echo -e "${GREEN}✓ 后端服务启动成功 (PID: $(cat "$BACKEND_PID"))${NC}"
            return 0
        fi
        sleep 1
    done

    echo -e "${RED}✗ 后端服务启动超时，请查看日志: $BACKEND_LOG${NC}"
    exit 1
}

# 启动前端
start_frontend() {
    echo -e "${BLUE}[6/6] 启动前端服务...${NC}"

    cd "$FRONTEND_DIR"

    # 检查 Node.js
    if ! command -v node &> /dev/null; then
        echo -e "${RED}✗ Node.js 未安装${NC}"
        exit 1
    fi

    # 检查 node_modules
    if [ ! -d "node_modules" ]; then
        echo -e "${YELLOW}⚠️  依赖未安装，正在安装...${NC}"
        npm install
    fi

    # 启动前端服务
    echo -e "${YELLOW}启动 React 开发服务器...${NC}"
    nohup npm start > "$FRONTEND_LOG" 2>&1 &
    echo $! > "$FRONTEND_PID"

    # 等待前端启动
    echo -e "${YELLOW}等待前端服务启动...${NC}"
    for i in {1..60}; do
        if curl -s http://localhost:3000 > /dev/null 2>&1; then
            echo -e "${GREEN}✓ 前端服务启动成功 (PID: $(cat "$FRONTEND_PID"))${NC}"
            return 0
        fi
        sleep 1
    done

    echo -e "${RED}✗ 前端服务启动超时，请查看日志: $FRONTEND_LOG${NC}"
    exit 1
}

# 显示服务状态
show_status() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}✓ 所有服务启动成功！${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo -e "${YELLOW}服务信息:${NC}"
    echo -e "  📊 PostgreSQL:  ${GREEN}localhost:5432${NC}"
    echo -e "  🔧 pgAdmin:     ${GREEN}http://localhost:5050${NC}"
    echo -e "  🚀 后端 API:    ${GREEN}http://localhost:8000${NC}"
    echo -e "  📖 API 文档:    ${GREEN}http://localhost:8000/docs${NC}"
    echo -e "  🎨 前端界面:    ${GREEN}http://localhost:3000${NC}"
    echo ""
    echo -e "${YELLOW}日志文件:${NC}"
    echo -e "  后端日志: ${BLUE}$BACKEND_LOG${NC}"
    echo -e "  前端日志: ${BLUE}$FRONTEND_LOG${NC}"
    echo ""
    echo -e "${YELLOW}管理命令:${NC}"
    echo -e "  查看后端日志: ${BLUE}tail -f $BACKEND_LOG${NC}"
    echo -e "  查看前端日志: ${BLUE}tail -f $FRONTEND_LOG${NC}"
    echo -e "  停止所有服务: ${BLUE}./stop.sh${NC}"
    echo ""
}

# 主流程
main() {
    # 检查是否已运行
    if check_running; then
        read -p "是否重启服务? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            stop_services
        else
            echo -e "${YELLOW}退出${NC}"
            exit 0
        fi
    fi

    # 执行启动流程
    check_docker
    start_database
    check_python
    init_database
    start_backend
    start_frontend
    show_status

    # 提示
    echo -e "${GREEN}按 Ctrl+C 退出，服务将继续在后台运行${NC}"
    echo ""

    # 可选：实时显示日志
    read -p "是否查看实时日志? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}显示后端日志 (Ctrl+C 退出):${NC}"
        tail -f "$BACKEND_LOG"
    fi
}

# 捕获 Ctrl+C
trap 'echo -e "\n${YELLOW}服务继续在后台运行，使用 ./stop.sh 停止服务${NC}"; exit 0' INT

# 运行主流程
main
