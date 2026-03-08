# 数据库重新初始化 - 快速开始

## 🎯 目标

重新初始化数据库以适应新的无版本管理结构，移除所有版本相关字段。

## 📋 已创建的文件

### 核心脚本 (backend/scripts/)
- ✅ `backup_configs.py` - 备份配置数据到 JSON
- ✅ `drop_old_tables.py` - 删除旧表
- ✅ `restore_configs.py` - 从 JSON 恢复配置
- ✅ `verify_integrity.py` - 验证数据完整性
- ✅ `reinit_database.py` - 主控脚本（自动化所有步骤）

### 辅助文件
- ✅ `run_reinit.sh` - Shell 快速启动脚本
- ✅ `GUIDE.sh` - 执行前检查脚本
- ✅ `README.md` - 详细使用文档
- ✅ `CHECKLIST.md` - 执行检查清单
- ✅ `SUMMARY.md` - 总结文档
- ✅ `QUICKSTART.md` - 本文件

## 🚀 立即开始

### 第一步：给 Shell 脚本添加执行权限

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts
chmod +x *.sh
```

### 第二步：运行检查脚本

```bash
./GUIDE.sh
```

这会检查：
- DolphinDB 是否运行
- Python 虚拟环境是否存在
- 备份目录状态

### 第三步：执行重新初始化

**方法 1：使用主控脚本（推荐）**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python scripts/reinit_database.py
```

**方法 2：使用 Shell 脚本**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts
./run_reinit.sh
```

## ⚡ 一键执行命令

```bash
# 完整命令（复制粘贴即可）
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts && \
chmod +x *.sh && \
./GUIDE.sh && \
cd .. && \
python scripts/reinit_database.py
```

## 📝 执行流程

```
1. 检查 DolphinDB 状态 ✓
   ↓
2. 备份配置数据 (sync_task_config, etl_task_config, factor_metadata)
   ↓
3. 删除旧表（需要确认）
   ↓
4. 重新创建表（无版本字段）
   ↓
5. 恢复配置数据（自动移除版本字段）
   ↓
6. 验证数据完整性 ✓
```

## ⏱️ 预计时间

- **自动化执行**: 5-10 分钟
- **手动执行**: 10-15 分钟

## ✅ 成功标志

执行成功后，你会看到：

```
✓ DolphinDB 正在运行
✓ 已备份 X 条记录
✓ 已删除表: sync_task_config
✓ 已删除表: etl_task_config
✓ 已删除表: factor_metadata
✓ 脚本执行完成
✓ 已恢复 X 条记录
✓ 数据完整性验证通过
✓ 数据库重新初始化完成!
```

## 🔍 验证步骤

### 1. 检查表结构（不包含版本字段）

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "
from store.dolphindb_client import db_client
tables = ['sync_task_config', 'etl_task_config', 'factor_metadata']
for table in tables:
    schema = db_client.session.run(f'schema({table})')
    cols = schema['colDefs']['name']
    version_fields = ['version_number', 'is_current', 'changed_by', 'change_reason']
    has_version = any(f in cols for f in version_fields)
    print(f'{table}: 包含版本字段={has_version}')
"
```

预期输出：所有表都应该显示 `包含版本字段=False`

### 2. 检查数据记录数

```bash
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
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python main.py
```

检查启动日志，确保无错误。

## 🆘 常见问题

### 问题 1: DolphinDB 未运行

```bash
# 启动 DolphinDB
cd /Users/lisheng/Code/quantsystem/quant_research_system
docker-compose up -d

# 检查状态
docker-compose ps
```

### 问题 2: 虚拟环境不存在

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 问题 3: 权限被拒绝

```bash
# 给脚本添加执行权限
chmod +x /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/*.sh
```

## 📚 详细文档

- **README.md** - 完整使用文档，包含所有脚本说明
- **CHECKLIST.md** - 详细的执行检查清单和故障排除
- **SUMMARY.md** - 项目总结和维护建议

## 🔄 回滚操作

如果需要回滚：

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 查看备份文件
ls -lt backups/

# 从备份恢复（使用最新的备份文件）
python scripts/restore_configs.py
```

## 📦 备份文件位置

```
backend/backups/
├── sync_task_config_20260308_HHMMSS.json
├── etl_task_config_20260308_HHMMSS.json
├── factor_metadata_20260308_HHMMSS.json
└── backup_metadata_20260308_HHMMSS.json
```

## 🎉 完成后

1. ✅ 验证所有表结构正确
2. ✅ 验证数据已恢复
3. ✅ 启动应用测试功能
4. ✅ 保留备份文件
5. ✅ 更新项目文档（如需要）

## 💡 提示

- 执行前建议先阅读 `README.md` 了解详细信息
- 使用 `CHECKLIST.md` 作为执行参考
- 保留备份文件至少一周
- 定期运行 `verify_integrity.py` 验证数据完整性

---

**准备好了吗？** 运行以下命令开始：

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python scripts/reinit_database.py
```
