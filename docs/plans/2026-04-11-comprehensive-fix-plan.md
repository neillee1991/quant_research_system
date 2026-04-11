# QuantSystem 全面修复计划

**日期**: 2026-04-11  
**基于**: 安全审计报告 + 代码质量评估 + 数据库审查报告

---

## 优先级 0 - 立即处理（严重安全问题）

### 0.1 SQL注入漏洞修复 🔴

**问题位置**:
- `app/api/v1/data/query_api.py:180` - `DELETE FROM {clean_table_name}`
- `app/api/v1/data/sync_api.py:125` - `SELECT MAX({date_field}) as max_date FROM {table_name}`
- `app/api/v1/data/etl_api.py:210` - `DELETE FROM {table_name} WHERE 1=1`
- `app/api/v1/data/etl_api.py:401` - `select top 1 * from {table_name}`

**修复方案**:
- 使用已有的 `sql_security.py` 模块验证表名和列名
- 使用参数化查询

### 0.2 API认证保护 🔴

**问题**: 大部分API端点没有认证保护

**需要保护的端点**:
- 所有 `/api/v1/data/*` 端点
- 所有 `/api/v1/factor/*` 端点  
- 所有 `/api/v1/tasks/*` 端点
- 所有 `/api/v1/flows/*` 端点

**例外（公开端点）**:
- `/api/v1/auth/token` - 登录
- `/api/v1/docs` - API文档
- `/api/v1/redoc` - API文档

**修复方案**:
- 在 `main.py` 中为路由器添加全局依赖 `Depends(get_current_active_user)`
- 或者在每个路由器文件中逐个添加

### 0.3 API速率限制 🔴

**问题**: 没有API级别的速率限制，容易被滥用

**修复方案**:
- 添加 `slowapi` + `limits` 库
- 在 `main.py` 中配置速率限制中间件
- 默认限制: 100次/分钟 per IP

---

## 优先级 1 - 本周处理（高优先级）

### 1.1 数据库自动化备份 🟠

**问题**: 没有自动化备份策略

**修复方案**:
- 创建 `scripts/maintenance/backup.sh` - 完整备份脚本
- 创建 `scripts/maintenance/backup.py` - Python备份管理器
- 配置 cron 定时任务
- 添加备份验证

### 1.2 修改默认数据库密码 🟠

**问题**: DolphinDB使用默认密码 `admin/123456`

**修复方案**:
- 修改 `init_dolphindb.dos` 中的默认密码
- 添加环境变量配置
- 更新文档

### 1.3 添加外键约束 🟠

**问题**: PostgreSQL表缺少外键约束

**需要添加的外键**:
- `flow_runs.flow_name → flow_configs.name`
- `factor_analysis_results.factor_id → factor_configs.factor_id`

### 1.4 添加CHECK约束 🟠

**问题**: 状态字段和类型字段缺少约束

**需要添加的约束**:
- `flow_runs.status IN ('pending','running','success','failed','cancelled')`
- `task_runs.status IN ('pending','running','success','failed')`
- `task_runs.task_type IN ('sync','etl','factor','flow')`
- `sync_task_configs.sync_type IN ('incremental','full')`

---

## 优先级 2 - 本月处理（中优先级）

### 2.1 拆分tasks.py 🟡

**问题**: `tasks.py` 789行，超过800行限制

**拆分方案**:
```
app/api/v1/tasks/
├── __init__.py
├── router.py          - 主路由注册
├── sync_tasks.py      - Sync任务CRUD
├── etl_tasks.py       - ETL任务CRUD
├── factor_tasks.py    - Factor任务CRUD
├── monitor.py         - 运行任务监控
└── common.py          - 共享逻辑
```

### 2.2 统一任务状态查询逻辑 🟡

**问题**: 任务状态查询在三处重复

**修复方案**:
- 提取到 `TaskService.get_task_status()`
- 所有端点使用统一方法

### 2.3 添加updated_at触发器 🟡

**问题**: updated_at字段需要自动更新

**修复方案**:
- 创建PostgreSQL触发器函数
- 应用到所有有updated_at的表

### 2.4 完成数据迁移 🟡

**问题**: stock_basic和trade_cal迁移未完成

**决策**: 
- 保留在DolphinDB（时序数据）
- 从PostgreSQL删除stocks和trading_calendar表
- 更新代码以消除混淆

---

## 优先级 3 - 规划中（低优先级）

### 3.1 重构JSON字段

**问题**: JSON字段滥用，违反第一范式

**建议**:
- 配置类JSON拆分为关联表
- 结果类JSON保留，但提取常用查询字段

### 3.2 统一主键设计

**问题**: 主键设计不一致

**建议**: 统一使用BIGSERIAL代理主键

### 3.3 清理task_runs冗余字段

**问题**: task_runs表有重复字段

**需要移除的字段**:
- `run_id` - 与id重复
- `task_name` - 可从task_id推断
- `finished_at` - 与ended_at重复

### 3.4 使用迁移工具

**建议**: 使用Alembic替代手动迁移脚本

---

## 执行顺序

```
Phase 1: 安全加固 (现在)
  ├─ 0.1 SQL注入修复
  ├─ 0.2 API认证保护
  └─ 0.3 API速率限制

Phase 2: 数据库改进 (本周)
  ├─ 1.1 自动化备份
  ├─ 1.2 修改默认密码
  ├─ 1.3 外键约束
  └─ 1.4 CHECK约束

Phase 3: 代码重构 (本月)
  ├─ 2.1 拆分tasks.py
  ├─ 2.2 统一状态查询
  ├─ 2.3 updated_at触发器
  └─ 2.4 完成数据迁移

Phase 4: 长期优化 (规划中)
  └─ 3.1-3.4 低优先级改进
```

---

## 验证清单

每个修复完成后验证:
- [ ] 所有测试通过
- [ ] 代码通过black/isort/flake8/mypy
- [ ] 手动测试关键功能
- [ ] 无新的安全警告
- [ ] 文档已更新
