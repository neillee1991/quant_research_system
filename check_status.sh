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

# 检查后端
echo -e "${BLUE}1. 后端服务状态${NC}"
echo "-----------------------------------------"
if lsof -ti:$BACKEND_PORT > /dev/null 2>&1; then
    echo -e "${GREEN}✅ 后端正在运行 (端口 $BACKEND_PORT)${NC}"
    echo "   进程: $(lsof -ti:$BACKEND_PORT | head -1)"

    # 测试 API
    if curl -s http://localhost:$BACKEND_PORT/api/v1/data/stocks > /dev/null 2>&1; then
        echo -e "${GREEN}✅ API 响应正常${NC}"
    else
        echo -e "${RED}❌ API 无响应${NC}"
    fi
else
    echo -e "${RED}❌ 后端未运行${NC}"
fi
echo ""

# 检查前端
echo -e "${BLUE}2. 前端服务状态${NC}"
echo "-----------------------------------------"
if lsof -ti:$FRONTEND_PORT > /dev/null 2>&1; then
    echo -e "${GREEN}✅ 前端正在运行 (端口 $FRONTEND_PORT)${NC}"
    echo "   进程: $(lsof -ti:$FRONTEND_PORT | head -1)"

    # 测试前端
    if curl -s http://localhost:$FRONTEND_PORT > /dev/null 2>&1; then
        echo -e "${GREEN}✅ 前端页面可访问${NC}"
    else
        echo -e "${RED}❌ 前端页面无响应${NC}"
    fi
else
    echo -e "${RED}❌ 前端未运行${NC}"
fi
echo ""

# 检查 PostgreSQL
echo -e "${BLUE}3. PostgreSQL 状态${NC}"
echo "-----------------------------------------"
if docker ps | grep -q $POSTGRES_CONTAINER; then
    echo -e "${GREEN}✅ PostgreSQL 容器运行中${NC}"

    if docker exec $POSTGRES_CONTAINER pg_isready -U $POSTGRES_USER -d $POSTGRES_DB > /dev/null 2>&1; then
        echo -e "${GREEN}✅ 数据库连接正常${NC}"

        # 获取数据库大小
        DB_SIZE=$(docker exec $POSTGRES_CONTAINER psql -U $POSTGRES_USER -d $POSTGRES_DB -t -c "SELECT pg_size_pretty(pg_database_size('$POSTGRES_DB'));" 2>/dev/null | xargs)
        echo "   数据库大小: $DB_SIZE"
    else
        echo -e "${RED}❌ 数据库连接失败${NC}"
    fi
else
    echo -e "${RED}❌ PostgreSQL 容器未运行${NC}"
fi
echo ""

# 检查 Redis
echo -e "${BLUE}4. Redis 状态${NC}"
echo "-----------------------------------------"
if docker ps | grep -q $REDIS_CONTAINER; then
    echo -e "${GREEN}✅ Redis 容器运行中${NC}"

    if docker exec $REDIS_CONTAINER redis-cli ping > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis 连接正常${NC}"

        # 获取 Redis 信息
        REDIS_MEMORY=$(docker exec $REDIS_CONTAINER redis-cli info memory 2>/dev/null | grep "used_memory_human" | cut -d: -f2 | tr -d '\r')
        REDIS_KEYS=$(docker exec $REDIS_CONTAINER redis-cli dbsize 2>/dev/null | tr -d '\r')
        echo "   内存使用: $REDIS_MEMORY"
        echo "   缓存键数: $REDIS_KEYS"
    else
        echo -e "${RED}❌ Redis 连接失败${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Redis 容器未运行${NC}"
fi
echo ""

# 检查日志
echo -e "${BLUE}5. 最近的日志${NC}"
echo "-----------------------------------------"
if [ -f "$BACKEND_LOG" ]; then
    echo "后端日志 (最后 3 行):"
    tail -3 "$BACKEND_LOG" | sed 's/^/  /'
else
    echo -e "${YELLOW}⚠️  后端日志文件不存在${NC}"
fi
echo ""

# 访问地址
echo -e "${BLUE}6. 访问地址${NC}"
echo "-----------------------------------------"
echo -e "📖 API 文档: ${GREEN}http://localhost:$BACKEND_PORT/docs${NC}"
echo -e "🌐 前端应用: ${GREEN}http://localhost:$FRONTEND_PORT${NC}"
if [ "$ENABLE_PGADMIN" = true ]; then
    echo -e "🗄️  pgAdmin:  ${GREEN}http://localhost:5050${NC}"
fi
echo ""

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}检查完成${NC}"
echo -e "${BLUE}=========================================${NC}"
