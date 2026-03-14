#!/bin/bash
# ===================================================================
# 量化研究系统 - 停止脚本
# ===================================================================

# 加载配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config/scripts.config.sh"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   量化研究系统 - 停止服务${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 停止后端
if [ -f "$BACKEND_PID" ]; then
    BACKEND_PID_NUM=$(cat "$BACKEND_PID")
    if kill -0 "$BACKEND_PID_NUM" 2>/dev/null; then
        kill "$BACKEND_PID_NUM"
        echo -e "${GREEN}✓ 后端服务已停止 (PID: $BACKEND_PID_NUM)${NC}"
    else
        echo -e "${YELLOW}⚠️  后端服务未运行${NC}"
    fi
    rm -f "$BACKEND_PID"
else
    echo -e "${YELLOW}⚠️  后端服务未运行${NC}"
fi

# 停止前端
if [ -f "$FRONTEND_PID" ]; then
    FRONTEND_PID_NUM=$(cat "$FRONTEND_PID")
    if kill -0 "$FRONTEND_PID_NUM" 2>/dev/null; then
        # 使用 pkill 停止整个进程树（包括 npm 和 webpack-dev-server）
        pkill -P "$FRONTEND_PID_NUM" 2>/dev/null
        kill "$FRONTEND_PID_NUM" 2>/dev/null
        echo -e "${GREEN}✓ 前端服务已停止 (PID: $FRONTEND_PID_NUM)${NC}"
    else
        echo -e "${YELLOW}⚠️  前端服务未运行${NC}"
    fi
    rm -f "$FRONTEND_PID"
else
    echo -e "${YELLOW}⚠️  前端服务未运行${NC}"
fi

# 额外清理：强制停止所有 node 进程（如果 PID 文件不存在）
if pgrep -f "react-scripts start" > /dev/null 2>&1; then
    echo -e "${YELLOW}检测到残留的前端进程，正在清理...${NC}"
    pkill -f "react-scripts start"
    echo -e "${GREEN}✓ 前端残留进程已清理${NC}"
fi

# 询问是否停止 Docker 服务
echo ""
read -p "是否停止 Docker 服务 (DolphinDB/Prefect)? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd "$SCRIPT_DIR"
    docker-compose down
    echo -e "${GREEN}✓ Docker 服务已停止${NC}"
else
    echo -e "${YELLOW}Docker 服务继续运行${NC}"
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}   服务已停止${NC}"
echo -e "${GREEN}========================================${NC}"
