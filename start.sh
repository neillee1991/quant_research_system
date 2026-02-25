#!/bin/bash
# ===================================================================
# 量化研究系统 - 一键启动脚本
# 自动启动 DolphinDB、Prefect、后端和前端服务
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

    for pid_file in "$BACKEND_PID" "$FRONTEND_PID" "$PREFECT_WORKER_PID"; do
        if [ -f "$pid_file" ]; then
            PID_NUM=$(cat "$pid_file")
            if kill -0 "$PID_NUM" 2>/dev/null; then
                kill "$PID_NUM"
                print_success "已停止进程 $PID_NUM"
            fi
            rm -f "$pid_file"
        fi
    done

    sleep 2
}

# 检查 Docker
check_docker() {
    print_step "1/7" "检查 Docker 环境..."

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
    print_step "2/7" "启动基础服务 (DolphinDB + Prefect)..."

    cd "$SCRIPT_DIR"
    DOLPHINDB_DATA_DIR="/Users/lisheng/Code/application/dolphin"

    # 确保 DolphinDB 数据目录存在
    if [ ! -d "$DOLPHINDB_DATA_DIR" ]; then
        print_warning "创建 DolphinDB 数据目录: $DOLPHINDB_DATA_DIR"
        mkdir -p "$DOLPHINDB_DATA_DIR"
    fi

    docker-compose up -d dolphindb prefect-server

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

    # 等待 Prefect Server 就绪
    attempt=0
    while [ $attempt -lt 20 ]; do
        if curl -sf http://localhost:$PREFECT_PORT/api/health > /dev/null 2>&1; then
            print_success "Prefect Server 已就绪"
            break
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    if [ $attempt -eq 20 ]; then
        print_warning "Prefect Server 启动较慢，继续..."
    fi
}

# 检查 Python 环境
check_python() {
    print_step "3/7" "检查 Python 环境..."

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
    print_step "4/7" "初始化后端环境..."

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

    # 设置 Prefect API URL
    export PREFECT_API_URL="http://localhost:$PREFECT_PORT/api"

    print_success "后端环境就绪"
}

# 启动后端
start_backend() {
    print_step "5/7" "启动后端服务..."

    cd "$BACKEND_DIR"
    source "$VENV_DIR/bin/activate"

    # 创建日志和PID目录
    mkdir -p "$LOG_DIR" "$PID_DIR"

    # 启动 uvicorn
    nohup $PYTHON_CMD -m uvicorn app.main:app \
        --host $BACKEND_HOST \
        --port $BACKEND_PORT \
        $BACKEND_RELOAD \
        > "$BACKEND_LOG" 2>&1 &

    echo $! > "$BACKEND_PID"
    print_success "后端服务已启动 (PID: $(cat "$BACKEND_PID"))"
}

# 启动 Prefect Worker
start_prefect_worker() {
    print_step "6/7" "启动 Prefect Worker..."

    if [ "$ENABLE_PREFECT_WORKER" != true ]; then
        print_warning "Prefect Worker 已禁用"
        return
    fi

    cd "$BACKEND_DIR"
    source "$VENV_DIR/bin/activate"

    export PREFECT_API_URL="http://localhost:$PREFECT_PORT/api"
    export NO_PROXY="localhost,127.0.0.1"
    export no_proxy="localhost,127.0.0.1"

    # 注册并启动 flows
    nohup $PYTHON_CMD flows/serve.py > "$PREFECT_WORKER_LOG" 2>&1 &
    echo $! > "$PREFECT_WORKER_PID"
    print_success "Prefect Worker 已启动 (PID: $(cat "$PREFECT_WORKER_PID"))"
}

# 启动前端
start_frontend() {
    print_step "7/7" "启动前端服务..."

    cd "$FRONTEND_DIR"

    # 检查 node_modules
    if [ ! -d "node_modules" ]; then
        print_warning "安装前端依赖..."
        npm install --silent 2>/dev/null
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
    echo -e "  Prefect UI:  ${GREEN}http://localhost:$PREFECT_PORT${NC}"
    echo -e "  DolphinDB:   ${GREEN}http://localhost:8848${NC} (Web管理)"
    echo ""
    echo -e "${BLUE}日志文件:${NC}"
    echo -e "  后端:          $BACKEND_LOG"
    echo -e "  前端:          $FRONTEND_LOG"
    echo -e "  Prefect Worker: $PREFECT_WORKER_LOG"
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
    start_prefect_worker
    start_frontend
    show_status
}

# 捕获 Ctrl+C
trap 'echo -e "\n${YELLOW}服务继续在后台运行，使用 ./stop.sh 停止服务${NC}"; exit 0' INT

# 运行主流程
main
