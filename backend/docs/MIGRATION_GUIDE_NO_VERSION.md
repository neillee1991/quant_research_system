# 版本管理移除迁移指南

**版本:** v2.0.0
**日期:** 2026-03-08
**影响范围:** 配置管理系统（数据同步任务、因子配置等）

## 概述

从 v2.0.0 开始，系统移除了配置的版本管理功能。配置更新现在采用直接覆盖模式，不再保留历史版本或支持回滚操作。

## 为什么移除版本管理？

### 原因分析

1. **复杂度与收益不匹配**
   - 版本管理增加了数据库表结构复杂度（version 字段、版本历史表）
   - 实际使用中，配置回滚需求极少（<1%）
   - 维护成本高，但使用频率低

2. **简化系统架构**
   - 减少数据库表字段和索引
   - 简化 API 接口和响应格式
   - 降低前端状态管理复杂度

3. **更符合实际使用场景**
   - 配置变更通常经过充分测试后才上线
   - 出现问题时，通常是修改配置而非回滚
   - Git 仓库已提供代码级别的版本控制

### 替代方案

- **数据库备份**: 定期备份 DolphinDB 数据库
- **配置导出**: 通过 API 导出配置为 JSON 文件
- **Git 管理**: 将重要配置纳入 Git 版本控制
- **审计日志**: 系统仍保留操作日志记录

## 系统行为变化

### 数据库变更

#### 移除的字段
```sql
-- sync_task_config 表
ALTER TABLE sync_task_config DROP COLUMN version;
ALTER TABLE sync_task_config DROP COLUMN created_at;
ALTER TABLE sync_task_config DROP COLUMN created_by;

-- factor_data_config 表
ALTER TABLE factor_data_config DROP COLUMN version;
ALTER TABLE factor_data_config DROP COLUMN created_at;
ALTER TABLE factor_data_config DROP COLUMN created_by;
```

#### 移除的表
```sql
-- 版本历史表已删除
DROP TABLE IF EXISTS sync_task_config_history;
DROP TABLE IF EXISTS factor_data_config_history;
```

### API 变更

#### 移除的端点

以下端点已被移除：

```
DELETE /api/v1/data/sync/tasks/{task_id}/versions
DELETE /api/v1/data/sync/tasks/{task_id}/versions/{version}
DELETE /api/v1/data/sync/tasks/{task_id}/rollback/{version}

DELETE /api/v1/production/factors/{factor_id}/versions
DELETE /api/v1/production/factors/{factor_id}/versions/{version}
DELETE /api/v1/production/factors/{factor_id}/rollback/{version}
```

#### 修改的端点

**配置更新端点** - 不再返回 `version` 字段：

```json
// 旧响应格式 (v1.x)
{
  "success": true,
  "data": {
    "task_id": "daily_data",
    "version": 3,
    "config": {...}
  }
}

// 新响应格式 (v2.0+)
{
  "success": true,
  "data": {
    "task_id": "daily_data",
    "config": {...}
  }
}
```

**配置查询端点** - 不再返回版本信息：

```json
// 旧响应 (v1.x)
{
  "task_id": "daily_data",
  "version": 3,
  "created_at": "2026-03-01T10:00:00",
  "created_by": "admin",
  "config": {...}
}

// 新响应 (v2.0+)
{
  "task_id": "daily_data",
  "updated_at": "2026-03-08T10:00:00",
  "config": {...}
}
```

### 前端变更

#### 移除的功能

1. **版本历史查看器**
   - 移除版本历史列表组件
   - 移除版本对比功能

2. **回滚按钮**
   - 配置详情页不再显示回滚按钮
   - 移除回滚确认对话框

3. **版本号显示**
   - 配置卡片不再显示版本号
   - 表格列不再包含 version 字段

#### 新增的功能

1. **配置导出**
   - 支持导出配置为 JSON 文件
   - 可用于手动备份和恢复

2. **更新确认**
   - 更新配置前显示更明确的警告
   - 强调更新操作不可撤销

## 数据库迁移步骤

### 自动迁移（推荐）

运行迁移脚本：

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python database/migrations/remove_version_fields.py
```

迁移脚本会：
1. 备份现有数据到 `*_backup` 表
2. 删除 version 相关字段
3. 删除版本历史表
4. 验证数据完整性

### 手动迁移

如果需要手动执行：

```sql
-- 1. 备份数据
CREATE TABLE sync_task_config_backup AS SELECT * FROM sync_task_config;
CREATE TABLE factor_data_config_backup AS SELECT * FROM factor_data_config;

-- 2. 删除版本字段
ALTER TABLE sync_task_config DROP COLUMN version;
ALTER TABLE sync_task_config DROP COLUMN created_at;
ALTER TABLE sync_task_config DROP COLUMN created_by;

ALTER TABLE factor_data_config DROP COLUMN version;
ALTER TABLE factor_data_config DROP COLUMN created_at;
ALTER TABLE factor_data_config DROP COLUMN created_by;

-- 3. 删除历史表
DROP TABLE IF EXISTS sync_task_config_history;
DROP TABLE IF EXISTS factor_data_config_history;

-- 4. 验证
SELECT COUNT(*) FROM sync_task_config;
SELECT COUNT(*) FROM factor_data_config;
```

### 回滚迁移（如需恢复）

```sql
-- 从备份恢复
DROP TABLE sync_task_config;
DROP TABLE factor_data_config;

ALTER TABLE sync_task_config_backup RENAME TO sync_task_config;
ALTER TABLE factor_data_config_backup RENAME TO factor_data_config;
```

## 代码迁移指南

### 后端代码

#### 移除版本相关代码

**旧代码 (v1.x):**
```python
# 创建配置时指定版本
config = {
    "task_id": "daily_data",
    "version": 1,
    "config": {...}
}

# 查询特定版本
version_config = get_config_version(task_id, version=2)

# 回滚到指定版本
rollback_config(task_id, target_version=1)
```

**新代码 (v2.0+):**
```python
# 直接创建/更新配置
config = {
    "task_id": "daily_data",
    "config": {...}
}

# 查询当前配置（无版本参数）
current_config = get_config(task_id)

# 更新配置（直接覆盖）
update_config(task_id, new_config)
```

#### 更新数据模型

**旧模型 (v1.x):**
```python
class SyncTaskConfig(BaseModel):
    task_id: str
    version: int
    created_at: datetime
    created_by: str
    config: Dict[str, Any]
```

**新模型 (v2.0+):**
```python
class SyncTaskConfig(BaseModel):
    task_id: str
    updated_at: datetime  # 替代 created_at
    config: Dict[str, Any]
```

### 前端代码

#### 移除版本相关 UI

**旧代码 (v1.x):**
```typescript
// 显示版本号
<Tag>v{config.version}</Tag>

// 版本历史按钮
<Button onClick={() => showVersionHistory(taskId)}>
  版本历史
</Button>

// 回滚按钮
<Button onClick={() => rollback(taskId, version)}>
  回滚到此版本
</Button>
```

**新代码 (v2.0+):**
```typescript
// 显示更新时间
<Text type="secondary">
  更新于: {formatDate(config.updated_at)}
</Text>

// 导出配置按钮
<Button onClick={() => exportConfig(taskId)}>
  导出配置
</Button>

// 更新确认
<Popconfirm
  title="更新配置将直接覆盖，无法撤销，确认继续？"
  onConfirm={() => updateConfig(taskId, newConfig)}
>
  <Button type="primary">更新配置</Button>
</Popconfirm>
```

#### 更新 API 调用

**旧代码 (v1.x):**
```typescript
// 获取版本历史
const versions = await api.get(`/tasks/${taskId}/versions`);

// 回滚
await api.post(`/tasks/${taskId}/rollback/${version}`);
```

**新代码 (v2.0+):**
```typescript
// 直接更新（无版本参数）
await api.put(`/tasks/${taskId}`, { config: newConfig });

// 导出配置
const config = await api.get(`/tasks/${taskId}`);
downloadJSON(config, `${taskId}_backup.json`);
```

## 配置管理最佳实践

### 1. 更新前备份

```bash
# 导出配置到文件
curl http://localhost:8000/api/v1/data/sync/tasks/daily_data \
  > backups/daily_data_$(date +%Y%m%d).json
```

### 2. 使用 Git 管理重要配置

```bash
# 将配置文件纳入版本控制
cd config/
git add sync_tasks.json
git commit -m "feat: update daily_data sync config"
```

### 3. 定期数据库备份

```bash
# 备份 DolphinDB 数据库
docker exec dolphindb /opt/dolphindb/server/dolphindb \
  -script backup_database.dos
```

### 4. 测试环境验证

```python
# 在测试环境先验证配置
test_config = {
    "task_id": "daily_data_test",
    "config": {...}
}

# 验证通过后再更新生产环境
prod_config = {
    "task_id": "daily_data",
    "config": {...}
}
```

### 5. 审计日志监控

```python
# 查询配置变更日志
logs = db_client.query("""
    SELECT * FROM audit_log
    WHERE operation = 'UPDATE_CONFIG'
    AND entity_type = 'sync_task_config'
    ORDER BY timestamp DESC
    LIMIT 100
""")
```

## 常见问题

### Q1: 如果误操作更新了配置怎么办？

**A:** 有以下恢复方案：

1. **从备份文件恢复**（如果有导出）
   ```bash
   curl -X PUT http://localhost:8000/api/v1/data/sync/tasks/daily_data \
     -H "Content-Type: application/json" \
     -d @backups/daily_data_20260307.json
   ```

2. **从数据库备份恢复**
   ```sql
   -- 如果有定期备份
   RESTORE DATABASE FROM '/backup/dolphindb_20260307.bak';
   ```

3. **手动重建配置**
   - 根据文档或记忆重新配置
   - 参考其他类似任务的配置

### Q2: 如何追踪配置变更历史？

**A:** 虽然没有版本管理，但可以通过以下方式追踪：

1. **审计日志**
   ```python
   # 查询配置变更记录
   changes = get_audit_logs(
       entity_type="sync_task_config",
       entity_id="daily_data",
       operation="UPDATE"
   )
   ```

2. **Git 历史**（如果配置在 Git 中）
   ```bash
   git log --follow config/sync_tasks.json
   ```

3. **数据库备份对比**
   ```bash
   # 对比不同时间点的备份
   diff backup_20260301.json backup_20260308.json
   ```

### Q3: 多人协作时如何避免配置冲突？

**A:** 建议采用以下流程：

1. **更新前通知**
   - 在团队群中通知配置变更计划
   - 使用配置锁定机制（如果实现）

2. **使用配置文件 + Git**
   ```bash
   # 创建分支
   git checkout -b config/update-daily-data

   # 修改配置文件
   vim config/sync_tasks.json

   # 提交并创建 PR
   git commit -m "feat: update daily_data config"
   git push origin config/update-daily-data
   ```

3. **通过 API 检查最新状态**
   ```python
   # 更新前先获取最新配置
   current = get_config("daily_data")

   # 基于最新配置进行修改
   updated = {**current, "new_field": "value"}

   # 更新
   update_config("daily_data", updated)
   ```

### Q4: 如何批量备份所有配置？

**A:** 使用备份脚本：

```python
import json
from datetime import datetime
from pathlib import Path

def backup_all_configs():
    """备份所有配置到文件"""
    backup_dir = Path("backups") / datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir.mkdir(parents=True, exist_ok=True)

    # 备份同步任务配置
    sync_tasks = db_client.query("SELECT * FROM sync_task_config")
    with open(backup_dir / "sync_tasks.json", "w") as f:
        json.dump(sync_tasks.to_dict(), f, indent=2)

    # 备份因子配置
    factor_configs = db_client.query("SELECT * FROM factor_data_config")
    with open(backup_dir / "factor_configs.json", "w") as f:
        json.dump(factor_configs.to_dict(), f, indent=2)

    print(f"Backup completed: {backup_dir}")

# 定期执行（如每天凌晨）
backup_all_configs()
```

### Q5: 系统升级后如何验证配置正确性？

**A:** 执行验证检查：

```python
def validate_configs():
    """验证配置完整性"""
    issues = []

    # 检查必需字段
    configs = db_client.query("SELECT * FROM sync_task_config")
    for config in configs:
        if not config.get("task_id"):
            issues.append(f"Missing task_id in config")
        if not config.get("config"):
            issues.append(f"Missing config in {config['task_id']}")

    # 检查配置格式
    for config in configs:
        try:
            validate_sync_config(config["config"])
        except Exception as e:
            issues.append(f"Invalid config for {config['task_id']}: {e}")

    if issues:
        print("Validation issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("All configs are valid")

    return len(issues) == 0
```

## 升级检查清单

升级到 v2.0.0 前，请确认：

- [ ] 已备份 DolphinDB 数据库
- [ ] 已导出所有重要配置到文件
- [ ] 已更新后端代码（移除版本相关逻辑）
- [ ] 已更新前端代码（移除版本 UI）
- [ ] 已运行数据库迁移脚本
- [ ] 已验证配置完整性
- [ ] 已测试配置更新功能
- [ ] 已通知团队成员系统变更
- [ ] 已更新操作文档和培训材料

## 回滚计划

如果升级后遇到问题，可以回滚到 v1.x：

1. **停止服务**
   ```bash
   ./stop.sh
   ```

2. **恢复数据库**
   ```bash
   # 从备份恢复
   docker exec -i dolphindb dolphindb < backup_v1.sql
   ```

3. **切换代码版本**
   ```bash
   git checkout v1.9.0
   cd backend && source .venv/bin/activate
   pip install -r requirements.txt
   ```

4. **重启服务**
   ```bash
   ./start.sh
   ```

## 支持与反馈

如有问题或建议：

1. 查看文档：`/docs/`
2. 检查日志：`backend/logs/app.log`
3. 提交 Issue 或联系开发团队

## 相关文档

- [配置管理用户指南](./USER_GUIDE_CONFIG_MANAGEMENT.md)
- [开发者指南](./DEVELOPER_GUIDE.md)
- [API 文档](http://localhost:8000/docs)
- [CHANGELOG](./CHANGELOG.md)
