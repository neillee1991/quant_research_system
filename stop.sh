#!/bin/bash

# 量化研究系统停止脚本
# 停止所有服务（数据库、后端、前端）

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# PID 文件
PID_DIR="$SCRIPT_DIR/.pids"
BACKEND_PID="$PID_DIR/backend.pid"
FRONTEND_PID="$PID_DIR/frontend.pid"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   停止量化研究系统服务${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 停止后端服务
if [ -f "$BACKEND_PID" ]; then
    BACKEND_PID_NUM=$(cat "$BACKEND_PID")
    if kill -0 "$BACKEND_PID_NUM" 2>/dev/null; then
        echo -e "${YELLOW}停止后端服务 (PID: $BACKEND_PID_NUM)...${NC}"
        kill "$BACKEND_PID_NUM"
        echo -e "${GREEN}✓ 后端服务已停止${NC}"
    else
        echo -e "${YELLOW}后端服务未运行${NC}"
    fi
    rm -f "$BACKEND_PID"
else
    echo -e "${YELLOW}后端服务未运行${NC}"
fi

# 停止前端服务
if [ -f "$FRONTEND_PID" ]; then
    FRONTEND_PID_NUM=$(cat "$FRONTEND_PID")
    if kill -0 "$FRONTEND_PID_NUM" 2>/dev/null; then
        echo -e "${YELLOW}停止前端服务 (PID: $FRONTEND_PID_NUM)...${NC}"
        kill "$FRONTEND_PID_NUM"
        echo -e "${GREEN}✓ 前端服务已停止${NC}"
    else
        echo -e "${YELLOW}前端服务未运行${NC}"
    fi
    rm -f "$FRONTEND_PID"
else
    echo -e "${YELLOW}前端服务未运行${NC}"
fi

# 停止 Docker 容器
echo ""
echo -e "${YELLOW}停止 PostgreSQL 和 pgAdmin...${NC}"
cd "$SCRIPT_DIR"
docker-compose down

echo ""
echo -e "${GREEN}✓ 所有服务已停止${NC}"
echo ""
echo -e "${YELLOW}💡 提示:${NC}"
echo -e "   重新启动: ${BLUE}./start.sh${NC}"
echo -e "   删除所有数据: ${BLUE}docker-compose down -v${NC}"
echo ""
