#!/bin/bash
# ===================================================================
# 量化研究系统 - 一键启动脚本
# 自动启动数据库、后端和前端服务
# ===================================================================

set -e

# 加载配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config/scripts.config.sh"

# ==================== 工具函数 ====================

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   量化研究系统 - 一键启动${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

print_step() {
    echo -e "${BLUE}[$1] $2${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# 检查服务是否运行
check_running() {
    if [ -f "$BACKEND_PID" ] && kill -0 $(cat "$BACKEND_PID") 2>/dev/null; then
        print_warning "后端服务已在运行 (PID: $(cat "$BACKEND_PID"))"
        return 0
    fi
    if [ -f "$FRONTEND_PID" ] && kill -0 $(cat "$FRONTEND_PID") 2>/dev/null; then
        print_warning "前端服务已在运行 (PID: $(cat "$FRONTEND_PID"))"
        return 0
    fi
    return 1
}

# 停止已有服务
stop_services() {
    print_warning "正在停止已有服务..."

    if [ -f "$BACKEND_PID" ]; then
        BACKEND_PID_NUM=$(cat "$BACKEND_PID")
        if kill -0 "$BACKEND_PID_NUM" 2>/dev/null; then
            kill "$BACKEND_PID_NUM"
            print_success "后端服务已停止"
        fi
        rm -f "$BACKEND_PID"
    fi

    if [ -f "$FRONTEND_PID" ]; then
        FRONTEND_PID_NUM=$(cat "$FRONTEND_PID")
        if kill -0 "$FRONTEND_PID_NUM" 2>/dev/null; then
            kill "$FRONTEND_PID_NUM"
            print_success "前端服务已停止"
        fi
        rm -f "$FRONTEND_PID"
    fi

    sleep 2
}

# 检查 Docker
check_docker() {
    print_step "1/6" "检查 Docker 环境..."

    if ! command -v docker &> /dev/null; then
        print_error "Docker 未安装"
        exit 1
    fi

    if ! docker info > /dev/null 2>&1; then
        print_error "Docker 未运行，请先启动 Docker"
        exit 1
    fi

    print_success "Docker 已运行"
}

# 启动数据库
start_database() {
    print_step "2/6" "启动数据库服务..."

    cd "$SCRIPT_DIR"

    # 清理 macOS 元数据文件
    if [ "$CLEAN_MACOS_METADATA" = true ] && [ -d "$PG_DATA_DIR" ]; then
        print_warning "清理 macOS 元数据文件..."
        find "$PG_DATA_DIR" -name "._*" -delete 2>/dev/null || true
    fi

    # 启动服务
    if [ "$ENABLE_REDIS" = true ]; then
        docker-compose up -d postgres redis
    else
        docker-compose up -d postgres
    fi

    echo -e "${YELLOW}等待数据库初始化...${NC}"
    sleep $DB_INIT_WAIT

    # 健康检查
    attempt=0
    while [ $attempt -lt $DB_MAX_ATTEMPTS ]; do
        if docker exec $POSTGRES_CONTAINER pg_isready -U $POSTGRES_USER -d $POSTGRES_DB > /dev/null 2>&1; then
            print_success "PostgreSQL 已就绪"
            break
        fi
        attempt=$((attempt + 1))
        sleep $DB_CHECK_INTERVAL
    done

    if [ $attempt -eq $DB_MAX_ATTEMPTS ]; then
        print_error "数据库启动超时"
        exit 1
    fi

    # 检查 Redis
    if [ "$ENABLE_REDIS" = true ]; then
        if docker exec $REDIS_CONTAINER redis-cli ping > /dev/null 2>&1; then
            print_success "Redis 已就绪"
        else
            print_warning "Redis 启动失败（系统将继续运行）"
        fi
    fi
}

# 检查 Python 环境
check_python() {
    print_step "3/6" "检查 Python 环境..."

    if command -v python3.11 &> /dev/null; then
        PYTHON_CMD="python3.11"
    elif command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    else
        print_error "Python3 未安装"
        exit 1
    fi

    PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
    PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
    PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

    if [ "$PYTHON_MAJOR" -lt $PYTHON_MIN_MAJOR ] || [ "$PYTHON_MINOR" -lt $PYTHON_MIN_MINOR ]; then
        print_error "需要 Python ${PYTHON_MIN_MAJOR}.${PYTHON_MIN_MINOR}+，当前: $PYTHON_VERSION"
        exit 1
    fi

    print_success "Python $PYTHON_VERSION"
}

# 初始化后端
init_backend() {
    print_step "4/6" "初始化后端环境..."

    cd "$BACKEND_DIR"

    # 检查虚拟环境
    if [ ! -d "$VENV_DIR" ]; then
        print_warning "创建虚拟环境..."
        $PYTHON_CMD -m venv $VENV_DIR
    fi

    source $VENV_DIR/bin/activate

    # 安装依赖
    if [ "$AUTO_INSTALL_DEPS" = true ]; then
        if ! python -c "import fastapi, redis" 2>/dev/null; then
            print_warning "安装 Python 依赖..."
            pip install -q -r requirements.txt
        fi
    fi

    # 检查数据库连接
    if ! python -c "from store.postgres_client import db_client; db_client.query('SELECT 1')" 2>/dev/null; then
        print_error "数据库连接失败"
        exit 1
    fi

    print_success "后端环境就绪"

    # 检查索引
    if [ "$CHECK_INDEXES" = true ]; then
        INDEX_COUNT=$(python -c "
from store.postgres_client import db_client
df = db_client.query(\"SELECT COUNT(*) as cnt FROM pg_indexes WHERE schemaname = 'public' AND indexname LIKE 'idx_%'\")
print(df['cnt'][0])
" 2>/dev/null)

        if [ "$INDEX_COUNT" -ge $MIN_INDEX_COUNT ]; then
            print_success "性能索引: $INDEX_COUNT 个"
        else
            print_warning "索引数量: $INDEX_COUNT 个（建议 >= $MIN_INDEX_COUNT）"
        fi
    fi

    # 检查 Redis
    if [ "$CHECK_REDIS" = true ] && [ "$ENABLE_REDIS" = true ]; then
        REDIS_AVAILABLE=$(python -c "
from store.redis_client import redis_client
print('yes' if redis_client.is_available() else 'no')
" 2>/dev/null)

        if [ "$REDIS_AVAILABLE" = "yes" ]; then
            print_success "Redis 缓存可用"
        else
            print_warning "Redis 缓存不可用"
        fi
    fi
}

# 启动后端
start_backend() {
    print_step "5/6" "启动后端服务..."

    cd "$BACKEND_DIR"
    source $VENV_DIR/bin/activate

    mkdir -p "$LOG_DIR" "$PID_DIR"

    nohup uvicorn app.main:app --host $BACKEND_HOST --port $BACKEND_PORT $BACKEND_RELOAD > "$BACKEND_LOG" 2>&1 &
    echo $! > "$BACKEND_PID"

    sleep 3

    if kill -0 $(cat "$BACKEND_PID") 2>/dev/null; then
        print_success "后端服务已启动 (PID: $(cat "$BACKEND_PID"))"
    else
        print_error "后端服务启动失败"
        cat "$BACKEND_LOG"
        exit 1
    fi
}

# 启动前端
start_frontend() {
    print_step "6/6" "启动前端服务..."

    cd "$FRONTEND_DIR"

    if [ ! -d "node_modules" ]; then
        print_warning "安装前端依赖..."
        npm install
    fi

    mkdir -p "$LOG_DIR" "$PID_DIR"

    nohup npm start > "$FRONTEND_LOG" 2>&1 &
    echo $! > "$FRONTEND_PID"

    sleep 5

    if kill -0 $(cat "$FRONTEND_PID") 2>/dev/null; then
        print_success "前端服务已启动 (PID: $(cat "$FRONTEND_PID"))"
    else
        print_error "前端服务启动失败"
        cat "$FRONTEND_LOG"
        exit 1
    fi
}

# 显示状态
show_status() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}   🎉 所有服务启动成功！${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "${BLUE}访问地址:${NC}"
    echo -e "  前端界面: ${GREEN}http://localhost:$FRONTEND_PORT${NC}"
    echo -e "  API 文档: ${GREEN}http://localhost:$BACKEND_PORT/docs${NC}"
    if [ "$ENABLE_PGADMIN" = true ]; then
        echo -e "  pgAdmin:  ${GREEN}http://localhost:5050${NC}"
    fi
    echo ""
    echo -e "${BLUE}日志文件:${NC}"
    echo -e "  后端: $BACKEND_LOG"
    echo -e "  前端: $FRONTEND_LOG"
    echo ""
    echo -e "${BLUE}管理命令:${NC}"
    echo -e "  查看状态: ${YELLOW}./check_status.sh${NC}"
    echo -e "  停止服务: ${YELLOW}./stop.sh${NC}"
    echo ""
}

# ==================== 主流程 ====================

main() {
    print_header

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
    init_backend
    start_backend
    start_frontend
    show_status
}

# 捕获 Ctrl+C
trap 'echo -e "\n${YELLOW}服务继续在后台运行，使用 ./stop.sh 停止服务${NC}"; exit 0' INT

# 运行主流程
main
