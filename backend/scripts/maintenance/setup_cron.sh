#!/bin/bash
# 配置Cron定时备份任务脚本

set -e

# 配置
# 获取脚本所在目录，自动计算备份脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_SCRIPT="$SCRIPT_DIR/backup.sh"
LOG_FILE="/var/log/quant_backup.log"

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

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

# 检查备份脚本是否存在
if [ ! -f "$BACKUP_SCRIPT" ]; then
    log_error "Backup script not found: $BACKUP_SCRIPT"
    exit 1
fi

# 确保脚本有执行权限
chmod +x "$BACKUP_SCRIPT"
log_info "✓ Backup script has execute permission"

# 创建日志目录
mkdir -p "$(dirname "$LOG_FILE")"
log_info "✓ Log directory created: $(dirname "$LOG_FILE")"

# 生成crontab条目
CRON_DAILY="0 2 * * * $BACKUP_SCRIPT >> $LOG_FILE 2>&1"
CRON_WEEKLY="0 3 * * 0 $BACKUP_SCRIPT weekly >> $LOG_FILE 2>&1"
CRON_MONTHLY="0 4 1 * * $BACKUP_SCRIPT monthly >> $LOG_FILE 2>&1"

log_info "Cron entries to be added:"
echo "  Daily (2:00 AM):   $CRON_DAILY"
echo "  Weekly (3:00 AM):  $CRON_WEEKLY"
echo "  Monthly (4:00 AM): $CRON_MONTHLY"

# 检查是否已存在
EXISTING_CRON=$(crontab -l 2>/dev/null | grep "$BACKUP_SCRIPT" || true)

if [ -n "$EXISTING_CRON" ]; then
    log_warn "Cron entries already exist:"
    echo "$EXISTING_CRON"
    log_warn "Skipping cron configuration"
    exit 0
fi

# 添加到crontab
(crontab -l 2>/dev/null || true; echo ""; echo "# QuantSystem Backup Tasks"; echo "$CRON_DAILY"; echo "$CRON_WEEKLY"; echo "$CRON_MONTHLY") | crontab -

log_info "✓ Cron entries added successfully"

# 验证
log_info "Current crontab entries:"
crontab -l | grep "$BACKUP_SCRIPT" || true

log_info "✓ Cron configuration completed"
log_info ""
log_info "Backup schedule:"
log_info "  - Daily backup at 2:00 AM"
log_info "  - Weekly backup at 3:00 AM (Sunday)"
log_info "  - Monthly backup at 4:00 AM (1st of month)"
log_info ""
log_info "Logs will be written to: $LOG_FILE"
log_info "View logs with: tail -f $LOG_FILE"
