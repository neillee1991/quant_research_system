# 数据库重新初始化脚本

此目录包含用于重新初始化数据库以适应新的无版本管理结构的脚本。

## 脚本说明

### 1. `backup_configs.py` - 备份配置数据
从 DolphinDB 导出当前配置到 JSON 文件。

**备份的表:**
- `sync_task_config` - 数据同步任务配置
- `etl_task_config` - ETL 任务配置
- `factor_metadata` - 因子元数据

**输出:**
- 备份文件保存在 `backend/backups/` 目录
- 文件名格式: `{table_name}_{timestamp}.json`
- 生成备份元数据文件: `backup_metadata_{timestamp}.json`

### 2. `drop_old_tables.py` - 删除旧表
删除包含版本管理字段的旧表。

**删除的表:**
- `sync_task_config`
- `etl_task_config`
- `factor_metadata`
- `task_version_history` (如果存在)

**注意:** 此脚本会要求确认操作。

### 3. `restore_configs.py` - 恢复配置数据
从 JSON 备份文件恢复配置到新表结构。

**功能:**
- 自动查找最新的备份文件
- 移除版本管理字段 (`version_number`, `is_current`, `changed_by`, `change_reason`)
- 使用 `upsert` 写入新表

### 4. `verify_integrity.py` - 验证数据完整性
检查表结构和数据是否正确。

**验证项:**
- 表是否存在
- 记录数统计
- 是否不包含版本字段
- 主键约束是否有效

### 5. `reinit_database.py` - 主控脚本
自动化执行完整的重新初始化流程。

**执行步骤:**
1. 检查 DolphinDB 状态
2. 备份现有配置数据
3. 删除旧表
4. 重新创建表（无版本字段）
5. 恢复配置数据
6. 验证数据完整性

## 使用方法

### 方法 1: 使用主控脚本（推荐）

```bash
cd backend
python scripts/reinit_database.py
```

此脚本会自动执行所有步骤，并在每个步骤后进行验证。

### 方法 2: 手动执行各个步骤

```bash
cd backend

# 1. 启动 DolphinDB（如果未运行）
docker-compose up -d

# 2. 备份配置数据
python scripts/backup_configs.py

# 3. 删除旧表
python scripts/drop_old_tables.py

# 4. 重新创建表
python database/init_dolphindb.py

# 5. 恢复配置数据
python scripts/restore_configs.py

# 6. 验证数据完整性
python scripts/verify_integrity.py
```

## 前置条件

1. **DolphinDB 必须正在运行**
   ```bash
   docker-compose up -d
   ```

2. **Python 环境已配置**
   ```bash
   source .venv/bin/activate  # 或使用你的虚拟环境
   pip install -r requirements.txt
   ```

3. **环境变量已设置**
   确保 `.env` 文件中包含 DolphinDB 连接配置:
   ```
   DOLPHINDB__HOST=127.0.0.1
   DOLPHINDB__PORT=8848
   DOLPHINDB__USER=admin
   DOLPHINDB__PASSWORD=123456
   ```

## 备份文件位置

所有备份文件保存在 `backend/backups/` 目录:

```
backend/backups/
├── sync_task_config_20260308_143022.json
├── etl_task_config_20260308_143022.json
├── factor_metadata_20260308_143022.json
└── backup_metadata_20260308_143022.json
```

## 错误处理

### 错误: DolphinDB 未运行
```
✗ DolphinDB 连接失败
```
**解决方法:** 运行 `docker-compose up -d` 启动 DolphinDB

### 错误: 未找到备份文件
```
✗ sync_task_config: 未找到表 sync_task_config 的备份文件
```
**解决方法:** 先运行 `backup_configs.py` 创建备份

### 错误: 表已存在
```
✗ 创建表失败: Table already exists
```
**解决方法:** 先运行 `drop_old_tables.py` 删除旧表

## 回滚操作

如果重新初始化后发现问题，可以从备份恢复:

```bash
# 1. 删除新表
python scripts/drop_old_tables.py

# 2. 恢复旧表结构（需要手动执行旧的 init_dolphindb.py）
# 或者从数据库备份恢复

# 3. 从 JSON 文件恢复数据
python scripts/restore_configs.py
```

## 注意事项

1. **数据安全**: 在执行删除操作前，务必确认备份已成功创建
2. **停机时间**: 重新初始化过程中，应用应该停止运行
3. **版本字段**: 恢复时会自动移除所有版本管理相关字段
4. **主键冲突**: 如果备份中有重复主键，恢复时会使用 upsert 覆盖
5. **日期时间**: 备份文件中的日期时间使用 ISO 格式存储

## 验证清单

重新初始化完成后，请验证以下内容:

- [ ] 所有配置表都已创建
- [ ] 配置数据已正确恢复
- [ ] 表中不包含版本字段
- [ ] 主键约束正常工作
- [ ] 应用可以正常启动
- [ ] API 接口正常响应
- [ ] 数据同步功能正常

## 日志位置

脚本执行日志保存在:
- 控制台输出
- `backend/logs/app.log` (应用日志)

## 联系支持

如果遇到问题，请检查:
1. DolphinDB 日志: `docker-compose logs dolphindb`
2. 应用日志: `backend/logs/app.log`
3. 备份文件是否完整: `backend/backups/`
