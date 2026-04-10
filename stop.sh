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

# 统一停止函数：先用 PID 文件，再按进程名兜底
kill_service() {
    local name="$1"
    local pid_file="$2"
    local pattern="$3"

    local killed=0

    # 1. 先用 PID 文件
    if [ -f "$pid_file" ]; then
        local pid_num
        pid_num=$(cat "$pid_file")
        if kill -0 "$pid_num" 2>/dev/null; then
            pkill -P "$pid_num" 2>/dev/null || true
            kill "$pid_num" 2>/dev/null && killed=1
        fi
        rm -f "$pid_file"
    fi

    # 2. 按进程名兜底（捕获 PID 文件之外的游离进程）
    local leftover
    leftover=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$leftover" ]; then
        pkill -f "$pattern" 2>/dev/null || true
        killed=1
    fi

    if [ "$killed" -eq 1 ]; then
        echo -e "${GREEN}✓ $name 已停止${NC}"
    else
        echo -e "${YELLOW}⚠️  $name 未运行${NC}"
    fi
}

# 停止后端（匹配 uvicorn app.main:app）
kill_service "后端服务" "$BACKEND_PID" "uvicorn app.main:app"

# 停止前端（匹配 react-scripts start）
kill_service "前端服务" "$FRONTEND_PID" "react-scripts start"

# 等待端口释放
sleep 1

# 验证端口已释放
for port in $BACKEND_PORT $FRONTEND_PORT; do
    if lsof -ti ":$port" > /dev/null 2>&1; then
        echo -e "${YELLOW}端口 $port 仍被占用，强制释放...${NC}"
        lsof -ti ":$port" | xargs kill -9 2>/dev/null || true
        echo -e "${GREEN}✓ 端口 $port 已释放${NC}"
    fi
done

# 询问是否停止 Docker 服务
echo ""
read -p "是否停止 Docker 服务 (DolphinDB/PostgreSQL)? (y/n): " -n 1 -r
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
