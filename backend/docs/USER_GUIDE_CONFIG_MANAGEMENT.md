# 配置管理用户指南

**版本:** v2.0.0
**更新日期:** 2026-03-08
**适用对象:** 系统管理员、量化研究员

## 目录

1. [概述](#概述)
2. [配置类型](#配置类型)
3. [创建配置](#创建配置)
4. [更新配置](#更新配置)
5. [查询配置](#查询配置)
6. [删除配置](#删除配置)
7. [配置备份与恢复](#配置备份与恢复)
8. [最佳实践](#最佳实践)
9. [常见问题](#常见问题)

## 概述

QuantSystem 的配置管理系统采用**直接更新模式**，配置变更会立即生效并覆盖原有配置。系统不保留历史版本，因此在更新配置前务必做好备份。

### 重要提示

- **不可撤销**: 配置更新后无法自动回滚到之前的版本
- **立即生效**: 配置更新后立即对新任务生效
- **需要备份**: 建议在更新前导出当前配置
- **审计日志**: 所有配置变更都会记录在审计日志中

## 配置类型

### 1. 数据同步任务配置 (Sync Task Config)

用于配置从 Tushare API 同步数据到 DolphinDB 的任务。

**存储位置**: `sync_task_config` 表

**配置示例**:
```json
{
  "task_id": "daily_data",
  "api_name": "daily",
  "table_name": "sync_daily_data",
  "fields": [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount"
  ],
  "date_field": "trade_date",
  "primary_keys": ["ts_code", "trade_date"],
  "api_limit": 5000,
  "enabled": true,
  "description": "日线行情数据同步"
}
```

### 2. 因子配置 (Factor Config)

用于配置因子计算的字段映射和预处理选项。

**存储位置**: `factor_data_config` 表

**配置示例**:
```json
{
  "factor_id": "momentum_20",
  "field_mappings": {
    "close": "close",
    "volume": "vol",
    "ts_code": "ts_code",
    "trade_date": "trade_date"
  },
  "preprocess_options": {
    "adjust_price": "forward",
    "filter_st": true,
    "filter_new_stock": true,
    "new_stock_days": 60,
    "handle_suspension": true,
    "mark_limit": true
  },
  "enabled": true
}
```

### 3. ETL 任务配置 (ETL Task Config)

用于配置数据转换和加工任务。

**存储位置**: `etl_task_config` 表

**配置示例**:
```json
{
  "task_id": "calculate_returns",
  "source_table": "sync_daily_data",
  "target_table": "processed_returns",
  "transform_script": "return_calculation.py",
  "schedule": "0 2 * * *",
  "enabled": true
}
```

## 创建配置

### 通过 API 创建

#### 创建数据同步任务

```bash
curl -X POST http://localhost:8000/api/v1/data/sync/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "new_sync_task",
    "api_name": "daily",
    "table_name": "sync_daily_data",
    "fields": ["ts_code", "trade_date", "close"],
    "date_field": "trade_date",
    "primary_keys": ["ts_code", "trade_date"],
    "enabled": true
  }'
```

#### 创建因子配置

```bash
curl -X POST http://localhost:8000/api/v1/production/factors \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "new_factor",
    "field_mappings": {
      "close": "close",
      "volume": "vol"
    },
    "enabled": true
  }'
```

### 通过前端界面创建

1. 登录系统前端 (http://localhost:3000)
2. 导航到对应的配置管理页面
   - 数据同步: **数据中心 > 同步任务**
   - 因子配置: **因子中心 > 因子管理**
3. 点击 **新建** 按钮
4. 填写配置表单
5. 点击 **保存** 完成创建

### 通过 Python 代码创建

```python
from app.services.task_service import sync_service

# 创建同步任务配置
task = sync_service.create_task(
    config_data={
        "task_id": "new_sync_task",
        "api_name": "daily",
        "table_name": "sync_daily_data",
        "fields": ["ts_code", "trade_date", "close"],
        "date_field": "trade_date",
        "primary_keys": ["ts_code", "trade_date"],
        "enabled": True
    }
)

print(f"Created task: {task['task_id']}")
```

## 更新配置

### 重要警告

配置更新采用**直接覆盖模式**，更新后无法自动恢复到之前的版本。请务必：

1. **更新前备份**: 导出当前配置到文件
2. **测试环境验证**: 先在测试环境验证配置正确性
3. **小心操作**: 仔细检查配置内容，避免误操作

### 通过 API 更新

```bash
# 1. 先导出当前配置（备份）
curl http://localhost:8000/api/v1/data/sync/tasks/daily_data \
  > backups/daily_data_$(date +%Y%m%d_%H%M%S).json

# 2. 更新配置
curl -X PUT http://localhost:8000/api/v1/data/sync/tasks/daily_data \
  -H "Content-Type: application/json" \
  -d '{
    "api_limit": 8000,
    "description": "Updated description"
  }'
```

### 通过前端界面更新

1. 导航到配置详情页
2. 点击 **导出配置** 按钮（备份）
3. 点击 **编辑** 按钮
4. 修改配置内容
5. 系统会显示确认对话框：
   ```
   ⚠️ 警告：更新配置将直接覆盖，无法撤销

   确认要更新配置吗？

   建议：已导出当前配置作为备份
   ```
6. 确认后点击 **保存**

### 通过 Python 代码更新

```python
from app.services.task_service import sync_service

# 1. 先获取当前配置（备份）
current_config = sync_service.get_task("daily_data")
import json
with open(f"backups/daily_data_backup.json", "w") as f:
    json.dump(current_config, f, indent=2)

# 2. 更新配置
updated = sync_service.update_task(
    task_id="daily_data",
    config_data={
        "api_limit": 8000,
        "description": "Updated description"
    }
)

print(f"Updated task: {updated['task_id']}")
```

### 部分更新 vs 完整更新

系统支持部分更新，只需提供要修改的字段：

```python
# 部分更新（推荐）- 只更新指定字段
sync_service.update_task(
    task_id="daily_data",
    config_data={"api_limit": 8000}  # 只更新 api_limit
)

# 完整更新 - 替换整个配置
sync_service.update_task(
    task_id="daily_data",
    config_data={
        "task_id": "daily_data",
        "api_name": "daily",
        "table_name": "sync_daily_data",
        # ... 所有字段
    }
)
```

## 查询配置

### 查询单个配置

```bash
# API 方式
curl http://localhost:8000/api/v1/data/sync/tasks/daily_data

# 响应示例
{
  "success": true,
  "data": {
    "task_id": "daily_data",
    "api_name": "daily",
    "table_name": "sync_daily_data",
    "fields": [...],
    "enabled": true,
    "updated_at": "2026-03-08T10:30:00"
  }
}
```

```python
# Python 方式
from app.services.task_service import sync_service

config = sync_service.get_task("daily_data")
print(config)
```

### 查询所有配置

```bash
# API 方式
curl http://localhost:8000/api/v1/data/sync/tasks

# 可选参数
curl "http://localhost:8000/api/v1/data/sync/tasks?enabled_only=true"
```

```python
# Python 方式
from app.services.task_service import sync_service

# 查询所有任务
all_tasks = sync_service.list_tasks()

# 只查询启用的任务
enabled_tasks = sync_service.list_tasks(enabled_only=True)
```

### 查询配置变更历史

虽然系统不保留配置版本，但可以通过审计日志查询变更历史：

```python
from store.dolphindb_client import db_client

# 查询配置变更记录
logs = db_client.query("""
    SELECT
        timestamp,
        operation,
        entity_id,
        changed_by,
        change_summary
    FROM audit_log
    WHERE entity_type = 'sync_task_config'
    AND entity_id = 'daily_data'
    ORDER BY timestamp DESC
    LIMIT 50
""")

print(logs)
```

## 删除配置

### 软删除（推荐）

软删除会将配置标记为禁用，但保留在数据库中：

```bash
# API 方式
curl -X DELETE http://localhost:8000/api/v1/data/sync/tasks/daily_data?soft=true
```

```python
# Python 方式
sync_service.delete_task("daily_data", soft=True)
```

### 硬删除（谨慎使用）

硬删除会从数据库中永久删除配置：

```bash
# API 方式
curl -X DELETE http://localhost:8000/api/v1/data/sync/tasks/daily_data?soft=false
```

```python
# Python 方式
sync_service.delete_task("daily_data", soft=False)
```

**警告**: 硬删除后无法恢复，除非从备份中恢复。

## 配置备份与恢复

### 手动备份

#### 导出单个配置

```bash
# 导出为 JSON 文件
curl http://localhost:8000/api/v1/data/sync/tasks/daily_data \
  > backups/daily_data_$(date +%Y%m%d).json
```

#### 批量导出所有配置

```python
import json
from datetime import datetime
from pathlib import Path
from store.dolphindb_client import db_client

def backup_all_configs():
    """备份所有配置到文件"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path(f"backups/config_backup_{timestamp}")
    backup_dir.mkdir(parents=True, exist_ok=True)

    # 备份同步任务配置
    sync_tasks = db_client.query("SELECT * FROM sync_task_config")
    with open(backup_dir / "sync_tasks.json", "w") as f:
        json.dump(sync_tasks.to_dict('records'), f, indent=2, ensure_ascii=False)

    # 备份因子配置
    factor_configs = db_client.query("SELECT * FROM factor_data_config")
    with open(backup_dir / "factor_configs.json", "w") as f:
        json.dump(factor_configs.to_dict('records'), f, indent=2, ensure_ascii=False)

    print(f"✅ 备份完成: {backup_dir}")
    return backup_dir

# 执行备份
backup_all_configs()
```

### 从备份恢复

#### 恢复单个配置

```bash
# 从 JSON 文件恢复
curl -X PUT http://localhost:8000/api/v1/data/sync/tasks/daily_data \
  -H "Content-Type: application/json" \
  -d @backups/daily_data_20260307.json
```

```python
import json

# 从文件读取配置
with open("backups/daily_data_20260307.json", "r") as f:
    config = json.load(f)

# 恢复配置
sync_service.update_task(
    task_id=config["task_id"],
    config_data=config
)
```

#### 批量恢复配置

```python
import json
from pathlib import Path

def restore_configs(backup_dir):
    """从备份目录恢复所有配置"""
    backup_path = Path(backup_dir)

    # 恢复同步任务配置
    sync_file = backup_path / "sync_tasks.json"
    if sync_file.exists():
        with open(sync_file, "r") as f:
            sync_tasks = json.load(f)

        for task in sync_tasks:
            try:
                sync_service.update_task(
                    task_id=task["task_id"],
                    config_data=task
                )
                print(f"✅ 恢复同步任务: {task['task_id']}")
            except Exception as e:
                print(f"❌ 恢复失败 {task['task_id']}: {e}")

    # 恢复因子配置
    factor_file = backup_path / "factor_configs.json"
    if factor_file.exists():
        with open(factor_file, "r") as f:
            factor_configs = json.load(f)

        for config in factor_configs:
            try:
                factor_service.update_factor(
                    factor_id=config["factor_id"],
                    config_data=config
                )
                print(f"✅ 恢复因子配置: {config['factor_id']}")
            except Exception as e:
                print(f"❌ 恢复失败 {config['factor_id']}: {e}")

# 执行恢复
restore_configs("backups/config_backup_20260307_100000")
```

### 自动备份

建议设置定期自动备份任务：

```python
# 使用 cron 或 Prefect 调度
from prefect import flow, task
from datetime import timedelta

@task
def backup_configs_task():
    """备份配置任务"""
    return backup_all_configs()

@flow(name="config-backup-flow")
def config_backup_flow():
    """配置备份工作流"""
    backup_dir = backup_configs_task()
    return backup_dir

# 部署为定时任务（每天凌晨 2 点）
if __name__ == "__main__":
    config_backup_flow.serve(
        name="daily-config-backup",
        cron="0 2 * * *"
    )
```

### 数据库级别备份

除了配置文件备份，还应定期备份整个 DolphinDB 数据库：

```bash
# 使用 DolphinDB 备份功能
docker exec dolphindb /opt/dolphindb/server/dolphindb \
  -script /path/to/backup_script.dos

# 或使用 Docker 卷备份
docker run --rm \
  --volumes-from dolphindb \
  -v $(pwd)/backups:/backup \
  ubuntu tar czf /backup/dolphindb_$(date +%Y%m%d).tar.gz /data
```

## 最佳实践

### 1. 配置变更流程

建议采用以下标准流程：

```
1. 规划变更
   ↓
2. 导出当前配置（备份）
   ↓
3. 在测试环境验证
   ↓
4. 在生产环境更新
   ↓
5. 验证更新结果
   ↓
6. 记录变更日志
```

### 2. 使用 Git 管理配置

将重要配置纳入 Git 版本控制：

```bash
# 项目结构
config/
├── sync_tasks/
│   ├── daily_data.json
│   ├── basic_data.json
│   └── adj_factor.json
├── factors/
│   ├── momentum_20.json
│   └── ma_cross.json
└── README.md

# Git 工作流
git checkout -b config/update-daily-data
vim config/sync_tasks/daily_data.json
git add config/sync_tasks/daily_data.json
git commit -m "feat: increase api_limit for daily_data"
git push origin config/update-daily-data

# 创建 PR 进行 Code Review
# 合并后应用到生产环境
```

### 3. 配置验证

更新前验证配置的正确性：

```python
from pydantic import BaseModel, ValidationError

class SyncTaskConfig(BaseModel):
    task_id: str
    api_name: str
    table_name: str
    fields: list[str]
    date_field: str
    primary_keys: list[str]
    enabled: bool = True

def validate_config(config_data):
    """验证配置格式"""
    try:
        config = SyncTaskConfig(**config_data)
        print("✅ 配置验证通过")
        return True
    except ValidationError as e:
        print(f"❌ 配置验证失败: {e}")
        return False

# 使用
new_config = {
    "task_id": "test_task",
    "api_name": "daily",
    # ...
}

if validate_config(new_config):
    sync_service.create_task(config_data=new_config)
```

### 4. 配置文档化

为每个配置添加详细的文档说明：

```json
{
  "task_id": "daily_data",
  "description": "日线行情数据同步 - 包含 OHLCV 和基本指标",
  "api_name": "daily",
  "table_name": "sync_daily_data",
  "fields": ["ts_code", "trade_date", "open", "high", "low", "close", "vol"],
  "date_field": "trade_date",
  "primary_keys": ["ts_code", "trade_date"],
  "api_limit": 5000,
  "enabled": true,
  "notes": "每日凌晨 1 点自动同步前一交易日数据",
  "owner": "data_team",
  "last_updated_by": "admin",
  "last_updated_reason": "增加 api_limit 以提高同步速度"
}
```

### 5. 监控配置变更

设置配置变更监控和告警：

```python
def monitor_config_changes():
    """监控配置变更"""
    # 查询最近 24 小时的配置变更
    recent_changes = db_client.query("""
        SELECT
            timestamp,
            entity_type,
            entity_id,
            operation,
            changed_by
        FROM audit_log
        WHERE entity_type IN ('sync_task_config', 'factor_data_config')
        AND timestamp >= now() - 24h
        ORDER BY timestamp DESC
    """)

    if len(recent_changes) > 0:
        # 发送通知（邮件、钉钉、Slack 等）
        send_notification(
            title="配置变更通知",
            content=f"过去 24 小时有 {len(recent_changes)} 个配置变更",
            details=recent_changes.to_dict('records')
        )
```

### 6. 环境隔离

为不同环境维护独立的配置：

```python
import os

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

CONFIG_PATHS = {
    "development": "config/dev/",
    "staging": "config/staging/",
    "production": "config/prod/"
}

config_path = CONFIG_PATHS[ENVIRONMENT]
```

## 常见问题

### Q1: 误操作更新了配置，如何恢复？

**A:** 有以下几种恢复方式：

1. **从备份文件恢复**（如果有导出）
   ```bash
   curl -X PUT http://localhost:8000/api/v1/data/sync/tasks/daily_data \
     -H "Content-Type: application/json" \
     -d @backups/daily_data_20260307.json
   ```

2. **从 Git 历史恢复**（如果配置在 Git 中）
   ```bash
   git checkout HEAD~1 config/sync_tasks/daily_data.json
   # 应用到系统
   ```

3. **从数据库备份恢复**
   ```bash
   # 恢复整个数据库到之前的时间点
   docker exec -i dolphindb dolphindb < backups/dolphindb_20260307.sql
   ```

4. **手动重建配置**
   - 根据文档或记忆重新配置
   - 参考其他类似任务的配置

### Q2: 如何知道配置何时被修改过？

**A:** 查看配置的 `updated_at` 字段和审计日志：

```python
# 查看配置更新时间
config = sync_service.get_task("daily_data")
print(f"最后更新时间: {config['updated_at']}")

# 查看详细变更历史
logs = db_client.query("""
    SELECT * FROM audit_log
    WHERE entity_type = 'sync_task_config'
    AND entity_id = 'daily_data'
    ORDER BY timestamp DESC
""")
print(logs)
```

### Q3: 多人同时修改配置会怎样？

**A:** 后提交的更新会覆盖先提交的更新。建议：

1. **团队协作规范**
   - 更新前在团队群中通知
   - 使用配置锁定机制（如果实现）
   - 通过 Git PR 流程进行 Code Review

2. **检查最新状态**
   ```python
   # 更新前先获取最新配置
   current = sync_service.get_task("daily_data")

   # 基于最新配置进行修改
   updated_config = {**current, "api_limit": 8000}

   # 更新
   sync_service.update_task("daily_data", updated_config)
   ```

### Q4: 配置更新后多久生效？

**A:** 立即生效，但具体影响取决于配置类型：

- **同步任务配置**: 下次任务执行时生效
- **因子配置**: 下次因子计算时生效
- **ETL 任务配置**: 下次 ETL 任务运行时生效

如需立即应用，可以手动触发相关任务。

### Q5: 如何批量更新多个配置？

**A:** 使用脚本批量更新：

```python
def batch_update_configs(updates):
    """批量更新配置"""
    results = []

    for task_id, config_data in updates.items():
        try:
            # 备份
            current = sync_service.get_task(task_id)
            backup_config(task_id, current)

            # 更新
            updated = sync_service.update_task(task_id, config_data)
            results.append({"task_id": task_id, "status": "success"})
            print(f"✅ 更新成功: {task_id}")
        except Exception as e:
            results.append({"task_id": task_id, "status": "failed", "error": str(e)})
            print(f"❌ 更新失败: {task_id} - {e}")

    return results

# 使用
updates = {
    "daily_data": {"api_limit": 8000},
    "basic_data": {"api_limit": 8000},
    "adj_factor": {"api_limit": 8000}
}

results = batch_update_configs(updates)
```

### Q6: 配置中的敏感信息如何处理？

**A:** 敏感信息（如 API Token）应使用环境变量：

```python
# 不要在配置中硬编码敏感信息
# ❌ 错误做法
{
    "api_token": "your_secret_token_here"
}

# ✅ 正确做法 - 使用环境变量引用
{
    "api_token": "${TUSHARE_TOKEN}"
}

# 在代码中解析
import os

def resolve_env_vars(config):
    """解析配置中的环境变量"""
    if isinstance(config, dict):
        return {k: resolve_env_vars(v) for k, v in config.items()}
    elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
        env_var = config[2:-1]
        return os.getenv(env_var)
    else:
        return config
```

## 相关文档

- [版本移除迁移指南](./MIGRATION_GUIDE_NO_VERSION.md)
- [开发者指南](./DEVELOPER_GUIDE.md)
- [API 文档](http://localhost:8000/docs)
- [CHANGELOG](./CHANGELOG.md)

## 支持与反馈

如有问题或建议：

1. 查看文档：`/docs/`
2. 检查日志：`backend/logs/app.log`
3. 提交 Issue 或联系开发团队
