#!/bin/bash
# 修改PostgreSQL默认密码脚本

set -e

# 配置
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-quant}"
POSTGRES_DB="${POSTGRES_DB:-quantsystem}"
OLD_PASSWORD="${OLD_PASSWORD:-quant123}"
NEW_PASSWORD="${NEW_PASSWORD:-}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# 检查参数
if [ -z "$NEW_PASSWORD" ]; then
    log_error "NEW_PASSWORD environment variable is required"
    echo "Usage: NEW_PASSWORD=your-new-password ./change_postgres_password.sh"
    exit 1
fi

# 检查密码长度
if [ ${#NEW_PASSWORD} -lt 8 ]; then
    log_error "Password must be at least 8 characters long"
    exit 1
fi

log_info "Changing PostgreSQL password for user '$POSTGRES_USER'..."
log_info "Host: $POSTGRES_HOST:$POSTGRES_PORT"
log_info "Database: $POSTGRES_DB"

# 设置环境变量
export PGPASSWORD="$OLD_PASSWORD"

# 修改密码
if psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "ALTER USER $POSTGRES_USER WITH PASSWORD '$NEW_PASSWORD';" 2>/dev/null; then
    log_info "✓ Password changed successfully"

    # 验证新密码
    export PGPASSWORD="$NEW_PASSWORD"
    if psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT 1;" >/dev/null 2>&1; then
        log_info "✓ Password verification successful"
        log_info "✓ New password is working correctly"

        # 提示更新.env文件
        log_warn "Remember to update .env file:"
        log_warn "POSTGRES_PASSWORD=$NEW_PASSWORD"

        exit 0
    else
        log_error "Password verification failed"
        exit 1
    fi
else
    log_error "Failed to change password"
    log_error "Please check your connection settings and old password"
    exit 1
fi
