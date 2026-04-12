#!/bin/bash
# ===================================================================
# 量化研究系统 - 首次部署脚本
# 一键完成环境检查、依赖安装、数据库初始化
# ===================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ==================== 从 .env 读取配置 ====================

load_config() {
    # 默认值（与 .env.example 一致）
    DOLPHINDB_HOST="localhost"
    DOLPHINDB_PORT="8848"
    DOLPHINDB_USER="admin"
    DOLPHINDB_PASSWORD="123456"
    REACT_APP_API_BASE_URL="http://localhost:8000"
    BACKEND_PORT="8000"
    FRONTEND_PORT="3000"

    # 默认 DolphinDB 数据目录
    DOLPHINDB_DATA_DIR="/Users/lisheng/Code/application/dolphin"

    # 如果 .env 存在，覆盖默认值
    if [ -f "$SCRIPT_DIR/.env" ]; then
        while IFS='=' read -r key value; do
            # 跳过注释和空行
            [[ "$key" =~ ^#.*$ || -z "$key" ]] && continue
            # 去除空格
            key=$(echo "$key" | xargs)
            value=$(echo "$value" | xargs)
            case "$key" in
                DOLPHINDB_HOST)     DOLPHINDB_HOST="$value" ;;
                DOLPHINDB_PORT)     DOLPHINDB_PORT="$value" ;;
                DOLPHINDB_USER)     DOLPHINDB_USER="$value" ;;
                DOLPHINDB_PASSWORD) DOLPHINDB_PASSWORD="$value" ;;
                DOLPHINDB_DATA_DIR) DOLPHINDB_DATA_DIR="$value" ;;
                REACT_APP_API_BASE_URL) REACT_APP_API_BASE_URL="$value" ;;
            esac
        done < "$SCRIPT_DIR/.env"
    fi

    # 从 REACT_APP_API_BASE_URL 提取端口
    BACKEND_PORT=$(echo "$REACT_APP_API_BASE_URL" | grep -oE '[0-9]+$' || echo "8000")
    # DolphinDB Web 管理端口 = 数据端口 + 1
    DOLPHINDB_WEB_PORT=$((DOLPHINDB_PORT + 1))

    # Export 环境变量，确保 docker-compose 可以访问
    export DOLPHINDB_DATA_DIR
}

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   量化研究系统 - 首次部署${NC}"
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

# ==================== 步骤 1: 系统依赖检查与自动安装 ====================

install_python() {
    echo ""
    echo -e "${YELLOW}  检测到系统缺少 Python 3.11+，尝试自动安装...${NC}"

    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS: 使用 Homebrew
        if command -v brew &> /dev/null; then
            echo "  使用 Homebrew 安装 Python 3.11..."
            brew install python@3.11
            # brew 安装后可能需要 link
            brew link --overwrite python@3.11 2>/dev/null || true
        else
            echo -e "${YELLOW}  Homebrew 未安装，先安装 Homebrew...${NC}"
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
            # Apple Silicon 需要手动添加到 PATH
            if [[ "$(uname -m)" == "arm64" ]]; then
                eval "$(/opt/homebrew/bin/brew shellenv)"
            fi
            brew install python@3.11
        fi
    elif [[ "$(uname)" == "Linux" ]]; then
        # Linux: 检测包管理器
        if command -v apt-get &> /dev/null; then
            echo "  使用 apt 安装 Python 3.11..."
            sudo apt-get update -qq
            sudo apt-get install -y -qq software-properties-common
            sudo add-apt-repository -y ppa:deadsnakes/ppa
            sudo apt-get update -qq
            sudo apt-get install -y -qq python3.11 python3.11-venv python3.11-dev
        elif command -v yum &> /dev/null; then
            echo "  使用 yum 安装 Python 3.11..."
            sudo yum install -y python3.11 python3.11-devel
        elif command -v dnf &> /dev/null; then
            echo "  使用 dnf 安装 Python 3.11..."
            sudo dnf install -y python3.11 python3.11-devel
        else
            print_error "无法识别的包管理器，请手动安装 Python 3.11+"
            echo "  安装: https://www.python.org/downloads/"
            return 1
        fi
    else
        print_error "不支持的操作系统: $(uname)"
        echo "  请手动安装 Python 3.11+: https://www.python.org/downloads/"
        return 1
    fi

    # 验证安装
    if command -v python3.11 &> /dev/null; then
        print_success "Python 3.11 安装成功"
        PYTHON_CMD="python3.11"
        return 0
    elif command -v python3 &> /dev/null; then
        PY_VER=$(python3 --version 2>&1 | awk '{print $2}')
        PY_MINOR=$(echo "$PY_VER" | cut -d. -f2)
        if [ "$PY_MINOR" -ge 11 ]; then
            print_success "Python $PY_VER 安装成功"
            PYTHON_CMD="python3"
            return 0
        fi
    fi

    print_error "Python 安装后仍未检测到 3.11+，请手动检查"
    return 1
}

install_node() {
    echo ""
    echo -e "${YELLOW}  检测到系统缺少 Node.js，尝试自动安装...${NC}"

    if [[ "$(uname)" == "Darwin" ]]; then
        if command -v brew &> /dev/null; then
            echo "  使用 Homebrew 安装 Node.js..."
            brew install node
        else
            print_error "Homebrew 未安装，无法自动安装 Node.js"
            echo "  安装: https://nodejs.org/"
            return 1
        fi
    elif [[ "$(uname)" == "Linux" ]]; then
        echo "  使用 NodeSource 安装 Node.js 20.x..."
        if command -v curl &> /dev/null; then
            curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
            sudo apt-get install -y -qq nodejs
        else
            print_error "curl 未安装，无法自动安装 Node.js"
            echo "  安装: https://nodejs.org/"
            return 1
        fi
    else
        print_error "请手动安装 Node.js: https://nodejs.org/"
        return 1
    fi

    if command -v node &> /dev/null; then
        print_success "Node.js $(node --version) 安装成功"
        return 0
    fi

    print_error "Node.js 安装失败"
    return 1
}

check_dependencies() {
    print_step "1/8" "检查系统依赖..."

    local missing=0

    # Docker
    if command -v docker &> /dev/null && docker info > /dev/null 2>&1; then
        print_success "Docker $(docker --version | awk '{print $3}' | tr -d ',')"
    else
        print_error "Docker 未安装或未运行"
        echo "  Docker 需要手动安装: https://docs.docker.com/get-docker/"
        missing=1
    fi

    # Docker Compose
    if docker compose version > /dev/null 2>&1 || docker-compose version > /dev/null 2>&1; then
        print_success "Docker Compose 已安装"
    else
        print_error "Docker Compose 未安装（通常随 Docker Desktop 一起安装）"
        missing=1
    fi

    # Docker 是必须手动安装的，先检查
    if [ $missing -eq 1 ]; then
        echo ""
        print_error "Docker 需要手动安装后重新运行此脚本"
        exit 1
    fi

    # Python（支持自动安装）
    PYTHON_CMD=""
    if command -v python3.11 &> /dev/null; then
        PYTHON_CMD="python3.11"
    elif command -v python3 &> /dev/null; then
        PY_VER=$(python3 --version 2>&1 | awk '{print $2}')
        PY_MINOR=$(echo "$PY_VER" | cut -d. -f2)
        if [ "$PY_MINOR" -ge 11 ]; then
            PYTHON_CMD="python3"
        fi
    fi

    if [ -n "$PYTHON_CMD" ]; then
        print_success "Python $($PYTHON_CMD --version 2>&1 | awk '{print $2}')"
    else
        print_warning "未检测到 Python 3.11+"
        read -p "  是否自动安装 Python 3.11? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            install_python || exit 1
        else
            print_error "请手动安装 Python 3.11+ 后重新运行"
            exit 1
        fi
    fi

    # Node.js（支持自动安装）
    if command -v node &> /dev/null; then
        print_success "Node.js $(node --version)"
    else
        print_warning "未检测到 Node.js"
        read -p "  是否自动安装 Node.js? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            install_node || exit 1
        else
            print_error "请手动安装 Node.js 后重新运行"
            exit 1
        fi
    fi
}

# ==================== 步骤 2: 环境配置 ====================

setup_env() {
    print_step "2/8" "配置环境变量..."

    cd "$SCRIPT_DIR"

    if [ -f ".env" ]; then
        print_warning ".env 文件已存在，跳过创建"
        # 检查关键配置
        if grep -q "your_tushare_token_here" .env; then
            print_warning "请编辑 .env 文件，填入 TUSHARE_TOKEN"
        fi
    else
        cp .env.example .env
        print_success "已创建 .env 文件"
        echo ""
        echo -e "${YELLOW}  重要: 请编辑 .env 文件，填入以下必要配置:${NC}"
        echo -e "  - TUSHARE_TOKEN: Tushare API Token (https://tushare.pro/register)"
        echo -e "  - DOLPHINDB_PASSWORD: DolphinDB 密码 (默认 123456)"
        echo ""
        read -p "  已编辑完成? 按 Enter 继续..." -r
    fi
}

# ==================== 步骤 3: 启动基础服务 ====================

start_docker_services() {
    print_step "3/7" "启动 Docker 服务 (DolphinDB + PostgreSQL)..."

    cd "$SCRIPT_DIR"

    # 确保 DolphinDB 顶层数据目录存在（子目录由容器自行初始化）
    mkdir -p "$DOLPHINDB_DATA_DIR"
    chmod 777 "$DOLPHINDB_DATA_DIR"

    # PostgreSQL 数据目录处理
    # postgres 容器内用户 uid=999，若目录权限不对会报 "Operation not permitted"
    POSTGRES_DATA_DIR="${SCRIPT_DIR}/data/postgres"
    if [ -d "$POSTGRES_DATA_DIR" ]; then
        # 检查目录是否为空或损坏（pg_filenode.map 不可读是典型症状）
        if [ ! -f "$POSTGRES_DATA_DIR/PG_VERSION" ]; then
            print_warning "PostgreSQL 数据目录存在但不完整，清理后重新初始化..."
            rm -rf "$POSTGRES_DATA_DIR"
            mkdir -p "$POSTGRES_DATA_DIR"
        else
            # 目录完整，修复权限
            chmod 700 "$POSTGRES_DATA_DIR"
        fi
    else
        mkdir -p "$POSTGRES_DATA_DIR"
    fi
    # 确保宿主机目录对容器内 postgres 用户(uid=999)可写
    chmod 777 "$POSTGRES_DATA_DIR"

    export DOLPHINDB_DATA_DIR
    docker-compose up -d
    print_success "Docker 服务已启动"
}

# ==================== 步骤 4: 等待服务就绪 ====================

wait_for_services() {
    print_step "4/7" "等待服务就绪..."

    # 等待 DolphinDB
    echo -n "  等待 DolphinDB ($DOLPHINDB_HOST:$DOLPHINDB_PORT)"
    for i in $(seq 1 60); do
        if curl -sf "http://${DOLPHINDB_HOST}:${DOLPHINDB_PORT}/" > /dev/null 2>&1; then
            echo ""
            print_success "DolphinDB 已就绪"
            break
        fi
        echo -n "."
        sleep 2
    done

    # 等待 PostgreSQL
    echo -n "  等待 PostgreSQL"
    for i in $(seq 1 30); do
        if docker exec quant_postgres pg_isready -U quant -d quantsystem -h localhost > /dev/null 2>&1; then
            echo ""
            print_success "PostgreSQL 已就绪"
            break
        fi
        echo -n "."
        sleep 2
    done
}

# ==================== 步骤 5: Python 环境 ====================

setup_python() {
    print_step "5/7" "配置 Python 环境..."

    cd "$SCRIPT_DIR/backend"

    # 创建虚拟环境
    if [ ! -d ".venv" ]; then
        $PYTHON_CMD -m venv .venv
        print_success "虚拟环境已创建"
    else
        print_success "虚拟环境已存在"
    fi

    # 激活并安装依赖
    source .venv/bin/activate
    pip install --upgrade pip -q 2>/dev/null
    echo "  安装 Python 依赖（可能需要几分钟）..."
    pip install -r requirements.txt -q 2>/dev/null
    print_success "Python 依赖已安装"
}

# ==================== 步骤 6: 初始化数据库 ====================

init_database() {
    print_step "6/7" "初始化数据库..."

    cd "$SCRIPT_DIR/backend"
    source .venv/bin/activate

    # 步骤 6a: 创建 DolphinDB 数据库（dfs://quant）
    python database/init_dolphindb.py

    # 步骤 6b: 建 PostgreSQL 表结构
    python database/init_postgres.py

    print_success "数据库初始化完成"
}

# ==================== 步骤 7: 前端依赖 ====================

setup_frontend() {
    print_step "7/7" "安装前端依赖..."

    cd "$SCRIPT_DIR/frontend"

    if [ ! -d "node_modules" ]; then
        npm install --silent 2>/dev/null
        print_success "前端依赖已安装"
    else
        print_success "前端依赖已存在"
    fi
}

# ==================== 完成 ====================

show_completion() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}   首次部署完成！${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "${BLUE}启动系统:${NC}"
    echo -e "  ${YELLOW}./start.sh${NC}"
    echo ""
    echo -e "${BLUE}访问地址（启动后）:${NC}"
    echo -e "  前端界面:    http://localhost:${FRONTEND_PORT}"
    echo -e "  API 文档:    ${REACT_APP_API_BASE_URL}/docs"
    echo -e "  DolphinDB:   http://${DOLPHINDB_HOST}:${DOLPHINDB_WEB_PORT}"
    echo ""
    echo -e "${BLUE}首次使用建议:${NC}"
    echo -e "  1. 启动系统: ./start.sh"
    echo -e "  2. 访问配置中心 (/config)，导入 backend/config/initial_config.json"
    echo -e "  3. 访问数据中心，执行全量数据同步"
    echo -e "  4. 在调度中心配置并启动 daily_task"
    echo ""
}

# ==================== 主流程 ====================

main() {
    print_header
    load_config
    check_dependencies
    setup_env
    load_config  # 重新加载（setup_env 可能刚创建了 .env）
    start_docker_services
    wait_for_services
    setup_python
    init_database
    setup_frontend
    show_completion
}

main
