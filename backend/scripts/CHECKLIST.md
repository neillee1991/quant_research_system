# 数据库重新初始化检查清单

## 执行前检查

- [ ] 已阅读 `scripts/README.md` 文档
- [ ] 已备份重要数据（如有需要）
- [ ] DolphinDB 服务正在运行 (`docker-compose ps`)
- [ ] Python 虚拟环境已激活 (`source .venv/bin/activate`)
- [ ] 应用服务已停止（避免数据冲突）

## 执行步骤

### 方法 1: 自动化执行（推荐）

```bash
cd backend
python scripts/reinit_database.py
```

或使用 Shell 脚本:

```bash
cd backend/scripts
./run_reinit.sh
```

### 方法 2: 手动执行

```bash
cd backend

# 步骤 1: 备份配置数据
python scripts/backup_configs.py
# 检查: backend/backups/ 目录中应该有 3 个 JSON 文件

# 步骤 2: 删除旧表
python scripts/drop_old_tables.py
# 确认: 输入 'yes' 继续

# 步骤 3: 重新创建表
python database/init_dolphindb.py
# 检查: 应该显示 "初始化完成!"

# 步骤 4: 恢复配置数据
python scripts/restore_configs.py
# 确认: 输入 'yes' 继续

# 步骤 5: 验证数据完整性
python scripts/verify_integrity.py
# 检查: 应该显示 "✓ 所有验证通过!"
```

## 执行后验证

### 1. 检查表结构

```bash
cd backend
python -c "
from store.dolphindb_client import db_client
tables = ['sync_task_config', 'etl_task_config', 'factor_metadata']
for table in tables:
    schema = db_client.session.run(f'schema({table})')
    cols = schema['colDefs']['name']
    print(f'{table}: {len(cols)} 列')
    # 确认不包含版本字段
    version_fields = ['version_number', 'is_current', 'changed_by', 'change_reason']
    has_version = any(f in cols for f in version_fields)
    print(f'  包含版本字段: {has_version}')
"
```

预期输出:
```
sync_task_config: X 列
  包含版本字段: False
etl_task_config: X 列
  包含版本字段: False
factor_metadata: X 列
  包含版本字段: False
```

### 2. 检查数据记录数

```bash
cd backend
python -c "
from store.dolphindb_client import db_client
tables = ['sync_task_config', 'etl_task_config', 'factor_metadata']
for table in tables:
    df = db_client.query(f'SELECT count(*) as cnt FROM {table}')
    count = df['cnt'][0]
    print(f'{table}: {count} 条记录')
"
```

### 3. 启动应用测试

```bash
cd backend
python main.py
```

检查启动日志:
- [ ] 无数据库连接错误
- [ ] 无表结构错误
- [ ] 配置加载成功

### 4. 测试 API 接口

```bash
# 测试同步任务配置接口
curl http://localhost:8000/api/v1/data/sync/tasks

# 测试因子元数据接口
curl http://localhost:8000/api/v1/production/factors
```

预期结果:
- [ ] 返回 200 状态码
- [ ] 返回配置数据列表
- [ ] 数据结构正确（无版本字段）

## 常见问题排查

### 问题 1: DolphinDB 连接失败

**症状:**
```
✗ DolphinDB 连接失败
```

**解决方法:**
```bash
# 检查 Docker 容器状态
docker-compose ps

# 如果未运行，启动服务
docker-compose up -d

# 检查日志
docker-compose logs dolphindb
```

### 问题 2: 备份文件为空

**症状:**
```
○ sync_task_config: 0 条记录
```

**原因:** 表本身就是空的（首次安装或测试环境）

**解决方法:** 继续执行，恢复后会使用种子数据

### 问题 3: 恢复时主键冲突

**症状:**
```
✗ 恢复表 sync_task_config 失败: Duplicate key
```

**解决方法:**
```bash
# 清空表后重新恢复
cd backend
python -c "
from store.dolphindb_client import db_client
db_client.session.run('delete from sync_task_config')
"
python scripts/restore_configs.py
```

### 问题 4: 表结构不匹配

**症状:**
```
✗ 恢复表失败: Column not found
```

**解决方法:**
```bash
# 重新创建表
cd backend
python scripts/drop_old_tables.py
python database/init_dolphindb.py
python scripts/restore_configs.py
```

## 回滚步骤

如果重新初始化后出现问题，可以回滚:

```bash
cd backend

# 1. 停止应用
# Ctrl+C 或 kill 进程

# 2. 删除新表
python scripts/drop_old_tables.py

# 3. 从备份恢复
# 找到最新的备份文件
ls -lt backups/

# 4. 手动恢复数据
# 根据备份文件内容，使用旧的表结构重新导入
```

## 完成确认

全部完成后，确认以下内容:

- [ ] 所有脚本执行成功，无错误
- [ ] 验证脚本显示 "✓ 所有验证通过!"
- [ ] 应用可以正常启动
- [ ] API 接口正常响应
- [ ] 配置数据已正确恢复
- [ ] 表中不包含版本字段
- [ ] 备份文件已保存在 `backend/backups/`

## 清理工作

重新初始化成功后，可以选择性清理:

```bash
# 保留最近的备份，删除旧备份（可选）
cd backend/backups
ls -lt
# 手动删除不需要的旧备份文件
```

## 文档更新

如果表结构有变化，记得更新:

- [ ] `CLAUDE.md` - 项目文档
- [ ] `MEMORY.md` - 项目记忆
- [ ] API 文档
- [ ] 数据库设计文档

## 时间估算

- 备份: 1-2 分钟
- 删除旧表: < 1 分钟
- 重新创建表: 1-2 分钟
- 恢复数据: 1-2 分钟
- 验证: < 1 分钟

**总计: 约 5-10 分钟**

## 支持

如有问题，请检查:
1. `backend/logs/app.log` - 应用日志
2. `docker-compose logs dolphindb` - 数据库日志
3. `backend/backups/` - 备份文件
4. `scripts/README.md` - 详细文档
