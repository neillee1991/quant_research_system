#!/bin/bash
# ===================================================================
# 量化研究系统 - 状态检查脚本
# ===================================================================

# 加载配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config/scripts.config.sh"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}量化研究系统 - 服务状态检查${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# 检查 DolphinDB
echo -e "${BLUE}1. DolphinDB 状态${NC}"
echo "-----------------------------------------"
if docker ps | grep -q $DOLPHINDB_CONTAINER; then
    echo -e "${GREEN}✅ DolphinDB 容器运行中${NC}"

    if curl -sf http://localhost:$DOLPHINDB_PORT/ > /dev/null 2>&1; then
        echo -e "${GREEN}✅ DolphinDB 连接正常${NC}"
    else
        echo -e "${RED}❌ DolphinDB 连接失败${NC}"
    fi
else
    echo -e "${RED}❌ DolphinDB 容器未运行${NC}"
fi
echo ""

# 检查 PostgreSQL
echo -e "${BLUE}2. PostgreSQL 状态${NC}"
echo "-----------------------------------------"
if docker ps | grep -q $POSTGRES_CONTAINER; then
    echo -e "${GREEN}✅ PostgreSQL 容器运行中${NC}"

    if docker exec $POSTGRES_CONTAINER pg_isready -U quant -d quantsystem -h localhost > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL 连接正常${NC}"
    else
        echo -e "${RED}❌ PostgreSQL 连接失败${NC}"
    fi
else
    echo -e "${RED}❌ PostgreSQL 容器未运行${NC}"
fi
echo ""

# 检查后端
echo -e "${BLUE}3. 后端服务状态${NC}"
echo "-----------------------------------------"
if lsof -ti:$BACKEND_PORT > /dev/null 2>&1; then
    echo -e "${GREEN}✅ 后端正在运行 (端口 $BACKEND_PORT)${NC}"
    echo "   进程: $(lsof -ti:$BACKEND_PORT | head -1)"

    # 测试 API
    if curl -sf http://localhost:$BACKEND_PORT/api/v1/tasks/running > /dev/null 2>&1; then
        echo -e "${GREEN}✅ API 响应正常${NC}"
    else
        echo -e "${YELLOW}⚠️  API 无响应（可能正在启动中）${NC}"
    fi
else
    echo -e "${RED}❌ 后端未运行${NC}"
fi
echo ""

# 检查前端
echo -e "${BLUE}4. 前端服务状态${NC}"
echo "-----------------------------------------"
if lsof -ti:$FRONTEND_PORT > /dev/null 2>&1; then
    echo -e "${GREEN}✅ 前端正在运行 (端口 $FRONTEND_PORT)${NC}"
    echo "   进程: $(lsof -ti:$FRONTEND_PORT | head -1)"

    # 测试前端
    if curl -sf http://localhost:$FRONTEND_PORT > /dev/null 2>&1; then
        echo -e "${GREEN}✅ 前端页面可访问${NC}"
    else
        echo -e "${YELLOW}⚠️  前端页面无响应（可能正在启动中）${NC}"
    fi
else
    echo -e "${RED}❌ 前端未运行${NC}"
fi
echo ""

# 检查调度器状态（通过后端 API）
echo -e "${BLUE}5. 调度器状态${NC}"
echo "-----------------------------------------"
if lsof -ti:$BACKEND_PORT > /dev/null 2>&1; then
    FLOW_COUNT=$(curl -sf http://localhost:$BACKEND_PORT/api/v1/flows 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "?")
    RUNNING_TASKS=$(curl -sf "http://localhost:$BACKEND_PORT/api/v1/tasks/running" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total',0))" 2>/dev/null || echo "?")
    echo -e "${GREEN}✅ 调度器运行中${NC}"
    echo "   Flow 配置数: $FLOW_COUNT"
    echo "   运行中任务: $RUNNING_TASKS"
else
    echo -e "${YELLOW}⚠️  后端未运行，无法检查调度器${NC}"
fi
echo ""

# 检查 PostgreSQL 配置数据
echo -e "${BLUE}6. 配置数据状态${NC}"
echo "-----------------------------------------"
if lsof -ti:$BACKEND_PORT > /dev/null 2>&1; then
    SYNC_COUNT=$(curl -sf "http://localhost:$BACKEND_PORT/api/v1/tasks/sync" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total',0))" 2>/dev/null || echo "?")
    ETL_COUNT=$(curl -sf "http://localhost:$BACKEND_PORT/api/v1/tasks/etl" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total',0))" 2>/dev/null || echo "?")
    FACTOR_COUNT=$(curl -sf "http://localhost:$BACKEND_PORT/api/v1/tasks/factor" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('total',0))" 2>/dev/null || echo "?")
    echo "   同步任务: $SYNC_COUNT 条"
    echo "   ETL 任务: $ETL_COUNT 条"
    echo "   因子配置: $FACTOR_COUNT 条"
else
    echo -e "${YELLOW}⚠️  后端未运行，无法检查配置数据${NC}"
fi
echo ""

# 检查日志
echo -e "${BLUE}7. 最近的日志${NC}"
echo "-----------------------------------------"
if [ -f "$BACKEND_LOG" ]; then
    echo "后端日志 (最后 5 行):"
    tail -5 "$BACKEND_LOG" | sed 's/^/  /'
else
    echo -e "${YELLOW}⚠️  后端日志文件不存在${NC}"
fi
echo ""

# 访问地址
echo -e "${BLUE}8. 访问地址${NC}"
echo "-----------------------------------------"
echo -e "📖 API 文档:    ${GREEN}http://localhost:$BACKEND_PORT/docs${NC}"
echo -e "🌐 前端应用:    ${GREEN}http://localhost:$FRONTEND_PORT${NC}"
echo -e "🗄️  DolphinDB:  ${GREEN}http://localhost:$DOLPHINDB_PORT${NC}"
echo ""

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}检查完成${NC}"
echo -e "${BLUE}=========================================${NC}"
