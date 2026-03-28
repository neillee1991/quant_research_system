# 开发者指南

## 开发环境搭建

### Python 3.11 环境

项目需要 Python 3.11 (PyCaret 兼容性要求)

```bash
cd backend

# 创建虚拟环境
python3.11 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 前端环境

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm start  # 访问 http://localhost:3000
```

### 数据库环境

```bash
# 启动 DolphinDB (Docker)
docker-compose up -d dolphindb

# 初始化数据库
cd backend
python database/init_dolphindb.py

# DolphinDB Web UI: http://localhost:8848
```

### 一键启动所有服务

```bash
# 在项目根目录
./setup.sh          # 首次运行（配置环境）
./start.sh          # 启动所有服务
./check_status.sh   # 检查服务状态
./stop.sh           # 停止所有服务
```

启动后访问：
- **前端界面**: http://localhost:3000
- **API 文档**: http://localhost:8000/docs
- **Prefect UI**: http://localhost:4200
- **DolphinDB**: http://localhost:8848

---

## 代码规范

### 命名规范 (强制执行)

**核心原则**:
- 不使用 `new`、`old`、`v2`、`legacy` 这类版本号在名称中
- 使用清晰、描述性的名称
- 使用领域术语

**类名 (PascalCase)**:
```python
# ✅ 正确
DatabaseClient
SeedDataLoader
FactorComputeService

# ❌ 错误
NewDolphinDBClient
OldFactorEngine
V2DataManager
```

**函数名 (snake_case)**:
```python
# ✅ 正确
load_sync_tasks()
calculate_factor_value()
get_database_connection()

# ❌ 错误
load_new_tasks()
calc_factor()
get_db_conn_new()
```

**文件名 (snake_case)**:
```python
# ✅ 正确
database_client.py
seed_loader.py
factor_compute_service.py

# ❌ 错误
dolphindb_client_v2.py
new_seed_loader.py
factor_service_new.py
```

**模块导出单例**:
```python
# ✅ 正确
database_client = DatabaseClient()

# ❌ 错误
db_client_new = NewDatabaseClient()
```

### 代码质量标准

| 指标 | 要求 |
|------|------|
| 函数长度 | ≤ 50 行 |
| 文件长度 | ≤ 800 行 |
| 嵌套深度 | ≤ 4 层 |
| 函数参数 | ≤ 5 个 |
| 类型注解覆盖率 | ≥ 80% |
| 测试覆盖率 | ≥ 80% |

### 代码风格工具

项目使用以下工具：
- `black` - 代码格式化 (line-length=100)
- `isort` - import 排序
- `flake8` - 代码检查
- `mypy` - 类型检查

```bash
# 格式化代码
black backend/
isort backend/

# 检查代码
flake8 backend/
mypy backend/
```

### 类型注解

**强制要求**:
```python
# ✅ 正确: 完整的类型注解
def calculate_factor(
    df: pl.DataFrame,
    params: Dict[str, Any],
    start_date: str,
    end_date: str
) -> pl.DataFrame:
    """计算因子值"""
    pass

# ❌ 错误: 缺少类型注解
def calculate_factor(df, params, start_date, end_date):
    pass
```

### 错误处理

```python
# ✅ 正确: 明确的异常处理
try:
    result = db_client.execute(sql)
except DolphinDBException as e:
    logger.error(f"Database query failed: {e}")
    raise DataException(f"Failed to query {table_name}") from e
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    raise

# ❌ 错误: 吞掉异常
try:
    result = db_client.execute(sql)
except:
    pass  # 静默失败
```

---

## 开发流程

### 后端开发

```bash
cd backend
source .venv/bin/activate

# 启动开发服务器
python main.py
# 或
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 前端开发

```bash
cd frontend

# 启动开发服务器
npm start  # 访问 http://localhost:3000
```

### 数据库初始化

```bash
cd backend

# 初始化 DolphinDB 表结构
python database/init_dolphindb.py
```

---

## 测试指南

### 测试组织

```
tests/
├── unit/                      # 单元测试 (快速, 无外部依赖)
├── integration/               # 集成测试 (需要数据库)
├── api/                       # API 测试 (需要启动服务)
└── performance/               # 性能测试
```

### 运行测试

```bash
cd backend

# 运行所有测试
pytest tests/

# 运行特定测试
pytest tests/test_technical_factors.py -v

# 运行并生成覆盖率报告
pytest tests/ --cov=. --cov-report=term-missing
```

### 测试覆盖率要求

- 最低覆盖率: 80%
- 核心模块覆盖率: ≥ 90% (engine/, data_manager/, store/)
- 新增代码覆盖率: 100%

---

## 调试技巧

### 日志查看

```bash
# 后端日志
tail -f backend/logs/app.log

# 前端日志
# 浏览器开发者工具 Console

# Docker 日志
docker-compose logs -f dolphindb
docker-compose logs -f prefect-server
```

### 常见问题

**SQL 语法错误**:
- 使用 `%s` 不是 `?` 作为参数占位符
- 传递 tuple 不是 list
- 裸表名会通过 `_adapt_sql_syntax()` 自动解析

**连接池耗尽**:
- DolphinDB 使用单个持久连接
- 检查是否有会话泄漏

**空表**:
- 检查同步任务是否存在: `GET /api/v1/data/sync/tasks`
- 确认任务 `enabled: true`
- 运行同步: `POST /api/v1/data/sync/task/{task_id}`

---

## 项目架构参考

### 核心文件路径

| 功能 | 文件路径 |
|------|---------|
| 生产引擎 | `backend/engine/production/engine.py` |
| 因子注册 | `backend/engine/production/registry.py` |
| 数据配置 | `backend/engine/production/data_config.py` |
| 技术因子库 | `backend/engine/factors/technical.py` |
| 因子分析 | `backend/engine/analysis/analyzer.py` |
| 数据处理器 | `backend/data_manager/processor.py` |
| 数据同步引擎 | `backend/data_manager/refactored_sync_engine.py` |
| DolphinDB 客户端 | `backend/infrastructure/database/dolphindb_client.py` |
| DolphinDB 客户端（兼容层） | `backend/store/dolphindb_client.py` |
| 种子数据管理器 | `backend/infrastructure/seed/manager.py` |
| 种子数据加载器 | `backend/infrastructure/seed/loader.py` |
| 种子数据配置 | `backend/config/seed_data/` |
| 生产 API | `backend/app/api/v1/production/` |
| 数据 API | `backend/app/api/v1/data/` |

### 数据库表

详见 [ARCHITECTURE.md](./ARCHITECTURE.md)
