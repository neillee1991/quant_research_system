#!/bin/bash
# ===================================================================
# 配置管理工具
# 用于管理量化研究系统的环境配置
# ===================================================================

set -e

# 加载脚本配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/scripts.config.sh"

# 显示帮助信息
show_help() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  list                列出所有可用的环境配置"
    echo "  use [ENV]          切换到指定的环境配置"
    echo "  show [ENV]         显示指定环境的配置"
    echo "  validate           验证当前配置"
    echo "  backup             备份当前配置"
    echo "  restore [FILE]     从备份恢复配置"
    echo "  export             导出当前配置为环境变量"
    echo ""
    echo "Options:"
    echo "  -h, --help         显示此帮助信息"
    echo ""
    echo "Examples:"
    echo "  $0 list"
    echo "  $0 use development"
    echo "  $0 show production"
    echo "  $0 validate"
}

# 列出所有环境配置
list_environments() {
    echo "Available environments:"
    echo ""

    if [ ! -d "$ENV_CONFIG_DIR" ]; then
        log_error "Environment config directory not found: $ENV_CONFIG_DIR"
        return 1
    fi

    for env_file in "$ENV_CONFIG_DIR"/*.env; do
        if [ -f "$env_file" ]; then
            env_name=$(basename "$env_file" .env)

            # 检查是否是当前环境
            current_marker=""
            if [ -f "$PROJECT_ROOT/.env" ]; then
                current_env=$(grep '^ENVIRONMENT=' "$PROJECT_ROOT/.env" 2>/dev/null | cut -d= -f2 || true)
                if [ "$current_env" = "$env_name" ]; then
                    current_marker=" * (current)"
                fi
            fi

            echo "  - $env_name$current_marker"
        fi
    done
    echo ""
}

# 切换环境
use_environment() {
    local env_name="$1"
    if [ -z "$env_name" ]; then
        log_error "Please specify environment name"
        show_help
        return 1
    fi

    local env_file="$ENV_CONFIG_DIR/$env_name.env"
    if [ ! -f "$env_file" ]; then
        log_error "Environment config not found: $env_name"
        list_environments
        return 1
    fi

    # 备份当前配置
    if [ -f "$PROJECT_ROOT/.env" ]; then
        local backup_file="$PROJECT_ROOT/.env.backup.$(date '+%Y%m%d_%H%M%S')"
        log_info "Backing up current config to $backup_file"
        cp "$PROJECT_ROOT/.env" "$backup_file"
    fi

    # 复制新配置
    log_info "Switching to environment: $env_name"
    cp "$env_file" "$PROJECT_ROOT/.env"

    log_success "Switched to $env_name successfully!"
    echo ""
    echo "You may need to restart services for changes to take effect."
    echo "To verify the configuration, run: $0 validate"
}

# 显示环境配置
show_environment() {
    local env_name="$1"
    local env_file

    if [ -z "$env_name" ]; then
        if [ -f "$PROJECT_ROOT/.env" ]; then
            env_file="$PROJECT_ROOT/.env"
            echo "Current environment configuration:"
            echo ""
        else
            log_error "No active configuration. Please specify environment name."
            list_environments
            return 1
        fi
    else
        env_file="$ENV_CONFIG_DIR/$env_name.env"
        if [ ! -f "$env_file" ]; then
            log_error "Environment config not found: $env_name"
            list_environments
            return 1
        fi
        echo "Environment: $env_name"
        echo ""
    fi

    # 显示配置（隐藏敏感信息）
    while IFS= read -r line; do
        # 跳过注释和空行
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue

        local key
        local value
        key=$(echo "$line" | cut -d= -f1)
        value=$(echo "$line" | cut -d= -f2-)

        # 隐藏敏感信息
        if [[ "$key" =~ PASSWORD|TOKEN|SECRET|KEY ]]; then
            value="********"
        fi

        echo "  $key=$value"
    done < "$env_file"
    echo ""
}

# 验证当前配置
validate_config() {
    log_info "Validating current configuration..."

    local errors=0

    # 检查.env文件
    if [ ! -f "$PROJECT_ROOT/.env" ]; then
        log_error ".env file not found"
        errors=$((errors + 1))
    else
        log_info ".env file found"

        # 检查必要的配置项
        local required_vars=(
            "TUSHARE_TOKEN"
            "DOLPHINDB_PASSWORD"
            "POSTGRES_PASSWORD"
        )

        for var in "${required_vars[@]}"; do
            if ! grep -q "^$var=" "$PROJECT_ROOT/.env"; then
                log_warning "Missing required variable: $var"
            fi
        done
    fi

    # 检查目录结构
    if [ ! -d "$APP_CONFIG_DIR" ]; then
        log_error "App config directory not found: $APP_CONFIG_DIR"
        errors=$((errors + 1))
    else
        log_info "App config directory found"
    fi

    if [ ! -d "$TASK_CONFIG_DIR" ]; then
        log_warning "Task config directory not found: $TASK_CONFIG_DIR"
    fi

    # 检查Python环境
    if [ -d "$VENV_DIR" ]; then
        log_info "Python virtual environment found"
    else
        log_warning "Python virtual environment not found"
    fi

    # 检查Docker
    if command -v docker &>/dev/null; then
        log_info "Docker is available"
    else
        log_warning "Docker not found"
    fi

    echo ""
    if [ "$errors" -eq 0 ]; then
        log_success "Configuration validation completed with no errors!"
        return 0
    else
        log_error "Configuration validation completed with $errors errors"
        return $errors
    fi
}

# 备份配置
backup_config() {
    local timestamp
    timestamp=$(date '+%Y%m%d_%H%M%S')
    local backup_dir="$DATA_DIR/config_backups"
    mkdir -p "$backup_dir"

    local backup_file="$backup_dir/config_backup_$timestamp.tar.gz"

    log_info "Creating configuration backup: $backup_file"

    # 打包配置文件
    tar -czf "$backup_file" \
        -C "$PROJECT_ROOT" \
        --exclude=".venv" \
        --exclude="node_modules" \
        --exclude="__pycache__" \
        --exclude=".pytest_cache" \
        .env \
        config/ \
        backend/config/ 2>/dev/null || true

    if [ $? -eq 0 ]; then
        log_success "Backup created successfully: $backup_file"
        echo "Size: $(du -h "$backup_file" | cut -f1)"
        return 0
    else
        log_error "Backup failed"
        return 1
    fi
}

# 恢复配置
restore_config() {
    local backup_file="$1"

    if [ -z "$backup_file" ]; then
        log_error "Please specify backup file"
        echo "Available backups:"
        local backup_dir="$DATA_DIR/config_backups"
        if [ -d "$backup_dir" ]; then
            ls -lt "$backup_dir" | grep -v '^total'
        fi
        return 1
    fi

    if [ ! -f "$backup_file" ]; then
        log_error "Backup file not found: $backup_file"
        return 1
    fi

    log_warning "This will overwrite current configuration"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Restore cancelled"
        return 0
    fi

    # 先备份当前配置
    backup_config

    # 恢复配置
    log_info "Restoring configuration from: $backup_file"
    tar -xzf "$backup_file" -C "$PROJECT_ROOT"

    if [ $? -eq 0 ]; then
        log_success "Configuration restored successfully!"
        return 0
    else
        log_error "Restore failed"
        return 1
    fi
}

# 导出配置为环境变量
export_config() {
    if [ ! -f "$PROJECT_ROOT/.env" ]; then
        log_error ".env file not found"
        return 1
    fi

    echo "# Exported configuration"
    echo "# Source this file or copy to ~/.bashrc or ~/.zshrc"
    echo ""
    echo "export QUANT_ROOT=\"$PROJECT_ROOT\""
    echo ""

    while IFS= read -r line; do
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
        echo "export $line"
    done < "$PROJECT_ROOT/.env"

    echo ""
    echo "# To load this configuration, run:"
    echo "# source <($0 export)"
}

# 主函数
main() {
    local command="$1"
    shift

    case "$command" in
        list)
            list_environments
            ;;
        use)
            use_environment "$1"
            ;;
        show)
            show_environment "$1"
            ;;
        validate)
            validate_config
            ;;
        backup)
            backup_config
            ;;
        restore)
            restore_config "$1"
            ;;
        export)
            export_config
            ;;
        -h|--help|help)
            show_help
            ;;
        "")
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            echo ""
            show_help
            return 1
            ;;
    esac
}

# 运行主函数
main "$@"
