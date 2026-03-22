# 项目重构实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 全面重构 QuantResearchSystem 项目，清理整合文档、种子数据配置化、统一 DolphinDB 客户端、清理脚本、检查 API 一致性

**Architecture:** 渐进式 5 阶段重构，每个阶段独立验证，风险可控

**Tech Stack:** Python 3.11, FastAPI, Polars, DolphinDB, React 18, TypeScript

---

## 前置条件

- ✅ 设计文档已完成: `docs/plans/2026-03-22-project-refactoring-design.md`
- ⚠️ 注意: 项目不是 git 仓库，没有版本控制。所有操作直接修改文件，请谨慎！

---

## 阶段 1: 文档整合与清理

### Task 1.1: 创建 docs/ 目录结构

**Files:**
- Create: `docs/ARCHITECTURE.md`
- Create: `docs/DEVELOPER_GUIDE.md`
- Create: `docs/API_REFERENCE.md`
- Create: `docs/DEPLOYMENT.md`
- Create: `docs/TROUBLESHOOTING.md`

**Step 1: 创建目录和空文档**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system
mkdir -p docs
touch docs/ARCHITECTURE.md
touch docs/DEVELOPER_GUIDE.md
touch docs/API_REFERENCE.md
touch docs/DEPLOYMENT.md
touch docs/TROUBLESHOOTING.md
```

**Step 2: 读取需要整合的源文档**

读取以下文件，提取有用信息：
- `PROJECT_STATUS.md`
- `SYSTEM_REFACTORING_STATUS.md`
- `PROJECT_STANDARDS.md`
- `CLAUDE.md` (项目架构部分)
- `DATA_INSPECTION_FRONTEND_GUIDE.md`
- `backend/README.md`
- `backend/TASK_ABSTRACTION_GUIDE.md`
- `frontend/TASK_ABSTRACTION_GUIDE.md`

**Step 3: 编写 ARCHITECTURE.md**

内容结构：
```markdown
# 系统架构

## 系统概览
- 项目简介
- 技术栈

## 架构分层
- 数据层 (DolphinDB)
- 计算层 (Polars + Prefect)
- API 层 (FastAPI)
- 前端层 (React)

## 核心模块说明
- 数据同步引擎
- 因子计算引擎
- 生产因子框架
- 回测引擎

## 数据库表设计
- TSDB 表列表
- 维度表列表

## 关键数据流
- 因子计算 8 步流程
- 数据同步流程
```

**Step 4: 编写 DEVELOPER_GUIDE.md**

内容结构：
```markdown
# 开发者指南

## 开发环境搭建
- Python 3.11 环境
- Node.js 环境
- Docker 环境

## 代码规范
- 命名规范（来自设计文档）
- 代码质量标准（来自 PROJECT_STANDARDS.md）
- Git 工作流

## 开发流程
- 后端开发
- 前端开发
- 数据库初始化

## 测试指南
- 测试组织
- 运行测试
- 覆盖率要求

## 调试技巧
- 日志查看
- 常见问题
```

**特别注意：把命名规范写入此文档：**
- 不使用 `new`、`old`、`v2`、`legacy` 这类版本号
- 类名 PascalCase：`DatabaseClient`、`SeedDataLoader`
- 函数名 snake_case：`load_sync_tasks()`
- 文件名 snake_case：`database_client.py`

**Step 5: 删除临时文档**

删除以下文件：
- `DATA_INSPECTION_FEATURE.md`
- `DATA_INSPECTION_FRONTEND_GUIDE.md`
- `DOLPHINDB_DATA_DIR_ROOT_CAUSE.md`
- `DOLPHINDB_PATH_FIX.md`
- `SCRIPTS_STATUS.md`
- `SETUP_FIXES.md`
- `FACTOR_UI_FIXES.md`
- `DATA_DEPENDENCY_THREE_CATEGORIES.md`
- `FACTOR_MODULE_CONSISTENCY_ANALYSIS.md`
- `PROJECT_STATUS.md`
- `SYSTEM_REFACTORING_STATUS.md`

**Step 6: 更新根目录 README.md**

精简根目录 README.md，只保留：
- 项目简介
- 快速开始（3步启动）
- 核心功能列表
- 链接到 docs/ 下的详细文档

---

## 阶段 2: 种子数据配置化

### Task 2.1: 创建 config/seed_data/ 目录

**Files:**
- Create: `config/seed_data/README.md`
- Create: `config/seed_data/sync_tasks.json`
- Create: `config/seed_data/etl_tasks.json`
- Create: `config/seed_data/factor_data_config.json`
- Create: `config/seed_data/factor_metadata.json`

**Step 1: 创建目录**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system
mkdir -p config/seed_data
```

**Step 2: 提取 sync_tasks.json**

从 `store/dolphindb/seed_data.py` 和 `store/dolphindb_client.py` 中提取所有同步任务配置：
- sync_stock_basic
- sync_trade_cal
- sync_daily_data
- sync_adj_factor
- sync_daily_basic
- sync_stk_limit
- sync_suspend_d
- sync_stock_st
- sync_sw_index_member_N
- sync_sw_index_member_Y
- sync_ci_index_member_N
- sync_ci_index_member_Y

格式参考设计文档。

**Step 3: 提取 etl_tasks.json**

提取 3 个 ETL 任务：
- etl_index_member
- etl_index_member_daily
- etl_stock_daily_info

**Step 4: 提取 factor_data_config.json**

提取 7 个字段映射配置。

**Step 5: 提取 factor_metadata.json**

提取 8 个默认因子定义。

**Step 6: 创建 config/seed_data/README.md**

说明配置文件的用途、格式、如何添加新配置。

---

### Task 2.2: 创建 SeedDataLoader 服务

**Files:**
- Create: `infrastructure/seed/__init__.py`
- Create: `infrastructure/seed/seed_loader.py`

**Step 1: 创建 infrastructure/seed/__init__.py**

```python
"""
种子数据加载模块
"""
from .seed_loader import SeedDataLoader, seed_data_loader

__all__ = ["SeedDataLoader", "seed_data_loader"]
```

**Step 2: 编写 SeedDataLoader 类**

位置：`infrastructure/seed/seed_loader.py`

完整实现，包括：
- `__init__(config_dir: Optional[Path] = None)`
- `load_sync_tasks() -> List[Dict]`
- `load_etl_tasks() -> List[Dict]`
- `load_factor_data_config() -> List[Dict]`
- `load_factor_metadata() -> List[Dict]`
- `seed_all_to_database(db_client)`
- `seed_sync_tasks_to_database(db_client)`
- `seed_etl_tasks_to_database(db_client)`
- `seed_factor_config_to_database(db_client)`

**Step 3: 创建单例实例**

在文件末尾添加：
```python
# 单例实例
seed_data_loader = SeedDataLoader()
```

---

### Task 2.3: 更新启动流程

**Files:**
- Modify: `app/main.py`

**Step 1: 修改 app/main.py 的导入**

```python
# 旧代码：
# from store.dolphindb_client import db_client

# 新代码：
from infrastructure.seed.seed_loader import seed_data_loader
```

**Step 2: 修改 lifespan 函数中的 seed 部分**

```python
# 旧代码：
# db_client.seed_sync_task_config()
# db_client.seed_etl_task_config()
# db_client.seed_factor_data_config()
# db_client.seed_factor_metadata()

# 新代码：
seed_data_loader.seed_all_to_database(db_client)
```

---

## 阶段 3: DolphinDB 客户端统一

### Task 3.1: 重命名 infrastructure/database/ 模块

**Files:**
- Rename: `infrastructure/database/dolphindb_client.py` → `infrastructure/database/database_client.py`
- Modify: `infrastructure/database/__init__.py`

**Step 1: 重命名文件**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
mv infrastructure/database/dolphindb_client.py infrastructure/database/database_client.py
```

**Step 2: 更新文件内部类名**

在 `infrastructure/database/database_client.py` 中：
- 类名 `DolphinDBClient` → `DatabaseClient`
- 单例 `_db_client_instance` → `_database_client_instance`
- 函数 `_get_db_client()` → `_get_database_client()`
- 单例 `db_client` → `database_client`

**Step 3: 更新 infrastructure/database/__init__.py**

```python
"""
数据库基础设施模块
"""
from .database_client import DatabaseClient, database_client

__all__ = ["DatabaseClient", "database_client"]
```

---

### Task 3.2: 更新 store/__init__.py 兼容层

**Files:**
- Modify: `store/__init__.py`
- Delete: `store/dolphindb_client.py`
- Delete: `store/dolphindb/` (整个目录)

**Step 1: 更新 store/__init__.py**

```python
"""
数据存储模块（向后兼容层）
"""
from infrastructure.database import DatabaseClient, database_client

# 保留旧名称的别名
DolphinDBClient = DatabaseClient
db_client = database_client

__all__ = ["DatabaseClient", "database_client", "DolphinDBClient", "db_client"]
```

**Step 2: 删除旧实现**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
rm -f store/dolphindb_client.py
rm -rf store/dolphindb/
```

**Step 3: 验证所有导入仍然工作**

检查以下文件的导入是否仍然工作：
- `app/main.py`
- `app/api/v1/*.py`
- `app/services/*.py`
- `data_manager/*.py`
- `engine/production/*.py`
- `flows/*.py`
- `services/*.py`

这些文件应该都使用 `from store.dolphindb_client import db_client`，通过兼容层仍然可以工作。

---

## 阶段 4: 脚本清理

### Task 4.1: 重新组织脚本目录

**Files:**
- Create: `scripts/maintenance/`
- Create: `scripts/migrations/`
- Create: `scripts/validation/`
- Move: `scripts/backup_configs.py` → `scripts/maintenance/`
- Move: `scripts/restore_configs.py` → `scripts/maintenance/`
- Move: `scripts/health_check.py` → `scripts/validation/`
- Delete: 其他所有脚本

**Step 1: 创建新目录**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
mkdir -p scripts/maintenance
mkdir -p scripts/migrations
mkdir -p scripts/validation
```

**Step 2: 移动保留的脚本**

```bash
mv scripts/backup_configs.py scripts/maintenance/
mv scripts/restore_configs.py scripts/maintenance/
mv scripts/health_check.py scripts/validation/
```

**Step 3: 删除所有其他脚本**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 删除根目录临时脚本
rm -f verify_lightweight.py
rm -f verify_refactor.py
rm -f verify_data_api.sh
rm -f main.py
rm -f init_database.py

# 删除 scripts/ 下的其他脚本
cd scripts
rm -f test_partition_performance.py
rm -f optimize_factor_values_partition.py
rm -f performance_dashboard.py
rm -f seed_factors.py
rm -f verify_integrity.py
rm -f verify_backup.py
rm -f verify_migration.py
rm -f verify_seed_tasks.py
rm -f test_new_api.py
rm -f test_task_execution.py
rm -f profile_code.py
rm -f run_benchmarks.py
rm -f migrate_factor.py
rm -f reinit_database.py
rm -f drop_old_tables.py
rm -f fix_factor_data_config.py
rm -f cleanup_duplicate_factor_data_config.py
rm -f cleanup_duplicate_sync_log.py

# 删除 migrations/completed/
rm -rf migrations/completed/
rm -f migrations/clean_depends_on_fields_from_config.py
```

**Step 4: 创建 scripts/README.md**

说明保留的脚本的用途。

---

## 阶段 5: 前后端 API 一致性检查

### Task 5.1: 比对前端 API 调用和后端路由

**Files:**
- Read: `frontend/src/api/index.ts`
- Read: `backend/app/api/v1/*.py`
- Read: `backend/app/api/v1/*/*.py`

**Step 1: 列出前端所有 API 调用**

从 `frontend/src/api/index.ts` 提取：
- `dataApi.*` 的所有端点
- `factorApi.*` 的所有端点
- `strategyApi.*` 的所有端点
- `mlApi.*` 的所有端点
- `productionApi.*` 的所有端点
- `flowApi.*` 的所有端点

**Step 2: 列出后端所有路由**

从 `backend/app/api/v1/` 提取所有路由：
- `data/` 模块
- `production/` 模块
- `factor.py`
- `strategy.py`
- `ml.py`
- `flows.py`
- `tasks.py`
- `generic_task.py`
- `schema_tools.py`
- `versions.py`

**Step 3: 一一比对**

创建对比表，确认：
- 每个前端调用的端点在后端存在
- HTTP 方法匹配（GET/POST/PUT/DELETE）
- 请求参数匹配
- 响应数据结构匹配

**Step 4: 修复发现的不一致**

根据比对结果：
- 如果后端实现正确，更新前端类型定义
- 如果前端调用正确，更新后端实现
- 删除双方都未使用的端点

---

## 验证与完成

### Task V1: 系统集成测试

**Step 1: 启动后端**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
source .venv/bin/activate
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

确认无错误，服务正常启动。

**Step 2: 启动前端**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
npm start
```

确认前端正常启动，可以访问 http://localhost:3000

**Step 3: 验证核心功能**

- 访问数据中心页面
- 访问因子中心页面
- 确认 API 调用正常

---

## 总结

完成以上所有任务后，项目将：
- ✅ 文档结构清晰，无冗余
- ✅ 所有种子数据在配置文件中
- ✅ DolphinDB 客户端统一，无重复代码
- ✅ 命名规范、科学、易读易理解
- ✅ 前后端 API 完全一致
- ✅ 系统可以正常启动和运行
