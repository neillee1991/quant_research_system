#!/bin/bash
# ===================================================================
# 量化研究系统 - 一键启动脚本
# 自动启动 DolphinDB、PostgreSQL、后端和前端服务
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

# 检查服务是否运行（按端口和进程名，不依赖 PID 文件）
check_running() {
    local running=0
    if lsof -ti ":$BACKEND_PORT" > /dev/null 2>&1; then
        print_warning "后端端口 $BACKEND_PORT 已被占用"
        running=1
    fi
    if lsof -ti ":$FRONTEND_PORT" > /dev/null 2>&1; then
        print_warning "前端端口 $FRONTEND_PORT 已被占用"
        running=1
    fi
    return $running
}

# 停止已有服务（按进程名 + 端口双重清理）
stop_services() {
    print_warning "正在停止已有服务..."

    # 按 PID 文件停止
    for pid_file in "$BACKEND_PID" "$FRONTEND_PID"; do
        if [ -f "$pid_file" ]; then
            PID_NUM=$(cat "$pid_file")
            if kill -0 "$PID_NUM" 2>/dev/null; then
                pkill -P "$PID_NUM" 2>/dev/null || true
                kill "$PID_NUM" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        fi
    done

    # 按进程名兜底
    pkill -f "uvicorn app.main:app" 2>/dev/null || true
    pkill -f "react-scripts start" 2>/dev/null || true

    # 等待端口释放
    sleep 2

    # 强制释放端口（最后保障）
    for port in $BACKEND_PORT $FRONTEND_PORT; do
        local pids
        pids=$(lsof -ti ":$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            echo "$pids" | xargs kill -9 2>/dev/null || true
            print_warning "强制释放端口 $port"
        fi
    done

    print_success "已有服务已停止"
}

# 检查 Docker
check_docker() {
    print_step "1/5" "检查 Docker 环境..."

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

# 启动基础服务
start_infrastructure() {
    print_step "2/5" "启动基础服务 (DolphinDB + PostgreSQL)..."

    cd "$SCRIPT_DIR"
    DOLPHINDB_DATA_DIR="/Users/lisheng/Code/application/dolphin"

    # 确保 DolphinDB 顶层数据目录存在（子目录由容器自行初始化）
    mkdir -p "$DOLPHINDB_DATA_DIR"
    chmod 777 "$DOLPHINDB_DATA_DIR"

    # PostgreSQL 数据目录权限修复
    POSTGRES_DATA_DIR="${SCRIPT_DIR}/data/postgres"
    if [ -d "$POSTGRES_DATA_DIR" ] && [ ! -f "$POSTGRES_DATA_DIR/PG_VERSION" ]; then
        print_warning "PostgreSQL 数据目录不完整，清理重建..."
        rm -rf "$POSTGRES_DATA_DIR"
    fi
    mkdir -p "$POSTGRES_DATA_DIR"
    chmod 777 "$POSTGRES_DATA_DIR"

    export DOLPHINDB_DATA_DIR
    docker-compose up -d dolphindb postgres

    # 等待 DolphinDB 就绪
    echo -e "${YELLOW}等待 DolphinDB 初始化...${NC}"
    sleep $DB_INIT_WAIT

    attempt=0
    while [ $attempt -lt $DB_MAX_ATTEMPTS ]; do
        if curl -sf http://localhost:$DOLPHINDB_PORT/ > /dev/null 2>&1; then
            print_success "DolphinDB 已就绪"
            break
        fi
        attempt=$((attempt + 1))
        sleep $DB_CHECK_INTERVAL
    done

    if [ $attempt -eq $DB_MAX_ATTEMPTS ]; then
        print_error "DolphinDB 启动超时"
        exit 1
    fi

    # 等待 PostgreSQL 就绪
    attempt=0
    while [ $attempt -lt $POSTGRES_MAX_ATTEMPTS ]; do
        if docker exec $POSTGRES_CONTAINER pg_isready -U quant -d quantsystem -h localhost > /dev/null 2>&1; then
            print_success "PostgreSQL 已就绪"
            break
        fi
        attempt=$((attempt + 1))
        sleep $POSTGRES_CHECK_INTERVAL
    done

    if [ $attempt -eq $POSTGRES_MAX_ATTEMPTS ]; then
        print_warning "PostgreSQL 启动较慢，继续..."
    fi
}

# 检查 Python 环境
check_python() {
    print_step "3/5" "检查 Python 环境..."

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
    print_step "4/5" "初始化后端环境..."

    cd "$BACKEND_DIR"

    # 检查虚拟环境
    if [ ! -d "$VENV_DIR" ]; then
        print_warning "创建虚拟环境..."
        $PYTHON_CMD -m venv $VENV_DIR
    fi

    # 激活虚拟环境
    source "$VENV_DIR/bin/activate"

    # 安装依赖
    if [ "$AUTO_INSTALL_DEPS" = true ]; then
        pip install -r requirements.txt -q 2>/dev/null
        print_success "依赖已安装"
    fi

    print_success "后端环境就绪"
}

# 启动后端
start_backend() {
    print_step "5/5" "启动后端和前端服务..."

    cd "$BACKEND_DIR"
    source "$VENV_DIR/bin/activate"

    # 创建日志和PID目录
    mkdir -p "$LOG_DIR" "$PID_DIR"

    # 启动 uvicorn - 使用虚拟环境里的 python
    nohup python -m uvicorn app.main:app \
        --host $BACKEND_HOST \
        --port $BACKEND_PORT \
        $BACKEND_RELOAD \
        > "$BACKEND_LOG" 2>&1 &

    echo $! > "$BACKEND_PID"
    print_success "后端服务已启动 (PID: $(cat "$BACKEND_PID"))"
}

# 启动前端
start_frontend() {
    cd "$FRONTEND_DIR"

    # 检查 node_modules 是否存在且完整（验证关键包）
    if [ ! -d "node_modules" ] || [ ! -d "node_modules/react" ] || [ ! -d "node_modules/html-webpack-plugin/lib" ]; then
        print_warning "安装前端依赖..."
        npm install
    fi

    # 启动前端
    nohup npm start > "$FRONTEND_LOG" 2>&1 &
    echo $! > "$FRONTEND_PID"
    print_success "前端服务已启动 (PID: $(cat "$FRONTEND_PID"))"
}

# 显示状态
show_status() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}   所有服务启动成功！${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "${BLUE}访问地址:${NC}"
    echo -e "  前端界面:    ${GREEN}http://localhost:$FRONTEND_PORT${NC}"
    echo -e "  API 文档:    ${GREEN}http://localhost:$BACKEND_PORT/docs${NC}"
    echo -e "  DolphinDB:   ${GREEN}http://localhost:8848${NC} (Web管理)"
    echo ""
    echo -e "${BLUE}日志文件:${NC}"
    echo -e "  后端:          $BACKEND_LOG"
    echo -e "  前端:          $FRONTEND_LOG"
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
    start_infrastructure
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
