#!/bin/bash
# 数据库自动备份脚本
#
# 安装到 crontab:
#   0 2 * * * /path/to/backup.sh >> /var/log/quant_backup.log 2>&1
#   0 3 * * 0 /path/to/backup.sh weekly >> /var/log/quant_backup.log 2>&1
#   0 4 1 * * /path/to/backup.sh monthly >> /var/log/quant_backup.log 2>&1

set -e

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BACKUP_TYPE="${1:-full}"
LOG_FILE="${PROJECT_ROOT}/logs/backup.log"

# 确保日志目录存在
mkdir -p "$(dirname "$LOG_FILE")"

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "Starting backup (type: $BACKUP_TYPE)..."

# 激活虚拟环境
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# 运行备份
cd "$PROJECT_ROOT"
python scripts/maintenance/backup_manager.py create --type "$BACKUP_TYPE" >> "$LOG_FILE" 2>&1

if [ $? -eq 0 ]; then
    log "Backup completed successfully"

    # 清理旧备份（保留30天或10个备份）
    log "Cleaning up old backups..."
    python scripts/maintenance/backup_manager.py cleanup --keep-days 30 --keep-count 10 >> "$LOG_FILE" 2>&1

    log "Backup process finished"
else
    log "Backup failed!"
    exit 1
fi
