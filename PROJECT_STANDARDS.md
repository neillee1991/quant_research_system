# 项目标准与规范

> 本文档定义 QuantSystem 项目的文件组织标准、代码规范和最佳实践

## 1. 项目文件组织标准

### 1.1 目录结构规范

```
quant_research_system/
├── backend/
│   ├── app/                    # FastAPI 应用层
│   │   ├── api/v1/            # API 路由 (按功能模块组织)
│   │   ├── core/              # 核心配置和工具
│   │   ├── models/            # Pydantic 模型
│   │   ├── services/          # 业务逻辑服务层
│   │   └── validators/        # 数据验证器
│   ├── engine/                # 因子计算引擎
│   │   ├── production/        # 生产环境引擎
│   │   ├── factors/           # 因子库
│   │   └── analysis/          # 因子分析
│   ├── data_manager/          # 数据管理层
│   ├── store/                 # 数据存储层 (DolphinDB)
│   ├── infrastructure/        # 基础设施层
│   ├── flows/                 # Prefect 工作流
│   ├── tests/                 # 测试代码 (按类型组织)
│   │   ├── unit/             # 单元测试
│   │   ├── integration/      # 集成测试
│   │   ├── api/              # API 测试
│   │   └── performance/      # 性能测试
│   ├── scripts/               # 运维脚本 (仅保留必要脚本)
│   │   ├── migrations/       # 数据库迁移脚本
│   │   └── maintenance/      # 维护脚本
│   ├── docs/                  # 项目文档
│   ├── config/                # 配置文件
│   └── database/              # 数据库初始化脚本
├── frontend/                  # React 前端
└── docker/                    # Docker 配置
```

### 1.2 文件命名规范

**Python 文件:**
- 模块文件: `snake_case.py` (例: `data_service.py`)
- 测试文件: `test_<module_name>.py` (例: `test_data_service.py`)
- 配置文件: `<purpose>_config.py` (例: `database_config.py`)

**文档文件:**
- README.md - 项目概览和快速开始
- DEVELOPER_GUIDE.md - 开发者指南
- API_REFERENCE.md - API 参考文档
- CHANGELOG.md - 变更日志
- 避免创建过多重复的 GUIDE、QUICKSTART 文档

### 1.3 禁止的文件位置

**❌ 不允许:**
- backend 根目录放置临时脚本 (check_*.py, test_*.py, analyze_*.py)
- 代码库中保留备份目录 (backups/)
- 提交 __pycache__/ 和 *.pyc 文件
- 在 scripts/ 中混杂测试代码

**✅ 正确做法:**
- 临时脚本放在 scripts/temp/ 并添加到 .gitignore
- 备份使用 Git 历史或外部存储
- 测试代码统一放在 tests/ 目录
- 使用 .gitignore 排除编译产物

## 2. 代码质量标准

### 2.1 代码复杂度限制

- **函数长度**: ≤ 50 行 (超过则拆分)
- **文件长度**: ≤ 800 行 (超过则拆分模块)
- **嵌套深度**: ≤ 4 层 (超过则提取函数)
- **函数参数**: ≤ 5 个 (超过则使用配置对象)
- **圈复杂度**: ≤ 10 (使用 radon 检查)

### 2.2 类型注解要求

**强制要求:**
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

**类型注解覆盖率目标: ≥ 80%**

### 2.3 错误处理规范

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

### 2.4 代码风格规范

**使用工具:**
- `black` - 代码格式化 (line-length=100)
- `isort` - import 排序
- `flake8` - 代码检查
- `mypy` - 类型检查

**配置文件 (pyproject.toml):**
```toml
[tool.black]
line-length = 100
target-version = ['py311']

[tool.isort]
profile = "black"
line_length = 100

[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
```

## 3. 安全规范

### 3.1 认证授权 (CRITICAL)

**强制要求:**
- 所有 API 端点必须实现认证
- 敏感操作 (代码执行、数据删除) 必须实现授权
- 使用 JWT 或 OAuth2 进行身份验证

**实现示例:**
```python
from fastapi import Depends, HTTPException
from app.core.auth import get_current_user, require_admin

@router.post("/factor/execute")
async def execute_factor_code(
    code: str,
    current_user: User = Depends(get_current_user),  # 认证
    _: None = Depends(require_admin)  # 授权
):
    # 仅管理员可执行代码
    pass
```

### 3.2 输入验证规范

**强制要求:**
- 所有用户输入必须验证
- 使用 Pydantic 模型进行数据验证
- 表名/列名使用白名单验证

```python
# ✅ 正确: 使用 Pydantic 验证
from pydantic import BaseModel, Field, validator

class FactorRequest(BaseModel):
    factor_id: str = Field(..., regex=r'^[a-zA-Z0-9_-]+$')
    start_date: str = Field(..., regex=r'^\d{8}$')
    end_date: str = Field(..., regex=r'^\d{8}$')

    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v < values['start_date']:
            raise ValueError('end_date must be >= start_date')
        return v
```

### 3.3 敏感信息管理

**强制要求:**
- 禁止硬编码密钥、token、密码
- 使用环境变量或密钥管理服务
- 生产环境禁用默认密码

```python
# ❌ 错误: 硬编码密码
dolphindb_password: str = Field(default="123456")

# ✅ 正确: 强制从环境变量读取
dolphindb_password: str = Field(..., env="DOLPHINDB_PASSWORD")

# ✅ 更好: 启动时检查弱密码
def validate_production_config():
    if settings.environment == "production":
        if settings.dolphindb_password == "123456":
            raise ValueError("Production environment cannot use default password")
```

### 3.4 SQL 注入防护

**强制要求:**
- 使用参数化查询 (即使 DolphinDB 需要手动转义)
- 表名/列名使用白名单验证
- 添加 SQL 注入测试用例

```python
# ✅ 正确: 参数化查询
sql = "SELECT * FROM table WHERE ts_code = %s AND trade_date = %s"
result = db_client.execute(sql, params=(ts_code, trade_date))

# ✅ 正确: 表名白名单验证
ALLOWED_TABLES = {
    "sync_daily_data", "sync_adj_factor", "factor_values"
}

def validate_table_name(table_name: str):
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table_name}")
```

## 4. 测试规范

### 4.1 测试覆盖率要求

- **最低覆盖率**: 80%
- **核心模块覆盖率**: ≥ 90% (engine/, data_manager/, store/)
- **新增代码覆盖率**: 100%

### 4.2 测试组织规范

```
tests/
├── unit/                      # 单元测试 (快速, 无外部依赖)
│   ├── test_technical_factors.py
│   ├── test_analyzer.py
│   └── test_data_service.py
├── integration/               # 集成测试 (需要数据库)
│   ├── test_dolphindb_client.py
│   ├── test_sync_engine.py
│   └── test_pipeline.py
├── api/                       # API 测试 (需要启动服务)
│   ├── test_factor_api.py
│   └── test_data_api.py
├── performance/               # 性能测试
│   └── test_partition_performance.py
├── conftest.py               # pytest 配置和 fixtures
└── __init__.py
```

### 4.3 测试命名规范

```python
# ✅ 正确: 清晰的测试命名
def test_calculate_ma_factor_with_valid_data():
    """测试使用有效数据计算 MA 因子"""
    pass

def test_calculate_ma_factor_with_empty_dataframe_raises_error():
    """测试空 DataFrame 抛出异常"""
    pass

# ❌ 错误: 模糊的测试命名
def test_ma():
    pass

def test_error():
    pass
```

### 4.4 测试依赖管理

**requirements-test.txt:**
```
pytest>=8.0.0
pytest-cov>=4.1.0
pytest-asyncio>=0.23.0
pytest-mock>=3.12.0
pytest-xdist>=3.5.0
```

## 5. 文档规范

### 5.1 文档结构

**核心文档 (必须):**
- `README.md` - 项目概览、快速开始、架构图
- `DEVELOPER_GUIDE.md` - 开发环境搭建、开发流程
- `API_REFERENCE.md` - API 端点文档
- `CHANGELOG.md` - 版本变更记录

**专题文档 (按需):**
- `DEPLOYMENT.md` - 部署指南
- `TROUBLESHOOTING.md` - 故障排查
- `MIGRATION_GUIDE.md` - 迁移指南 (仅在重大变更时)

**❌ 避免:**
- 创建多个重复的 GUIDE、QUICKSTART 文档
- 文档内容与代码不同步
- 过度详细的内部实现文档 (应该用代码注释)

### 5.2 代码注释规范

```python
# ✅ 正确: 清晰的 docstring
def calculate_factor(
    df: pl.DataFrame,
    params: Dict[str, Any]
) -> pl.DataFrame:
    """计算技术指标因子

    Args:
        df: 包含 OHLCV 数据的 DataFrame
        params: 因子参数字典, 例如 {"period": 20, "method": "sma"}

    Returns:
        包含因子值的 DataFrame, 新增 factor_value 列

    Raises:
        ValueError: 当 df 为空或缺少必需列时

    Example:
        >>> df = pl.DataFrame({"close": [100, 101, 102]})
        >>> result = calculate_factor(df, {"period": 2})
    """
    pass

# ❌ 错误: 无用的注释
def calculate_factor(df, params):
    # 计算因子
    pass
```

## 6. Git 工作流规范

### 6.1 分支命名

- `main` - 生产环境分支
- `develop` - 开发分支
- `feature/<name>` - 功能分支
- `bugfix/<name>` - 修复分支
- `hotfix/<name>` - 紧急修复分支

### 6.2 提交信息规范

```
<type>: <subject>

<body>

<footer>
```

**Type:**
- `feat`: 新功能
- `fix`: 修复 bug
- `refactor`: 重构代码
- `docs`: 文档更新
- `test`: 测试相关
- `chore`: 构建/工具相关

**示例:**
```
feat: 添加因子计算缓存机制

- 实现 Redis 缓存层
- 添加缓存失效策略
- 性能提升 3x

Closes #123
```

### 6.3 代码审查要求

**强制要求:**
- 所有代码必须经过 Code Review
- 至少 1 人 approve 才能合并
- CI 测试必须通过
- 覆盖率不能下降

## 7. 性能规范

### 7.1 数据库查询优化

- 使用分区裁剪 (查询时指定 factor_id, trade_date, ts_code)
- 避免 SELECT * (明确指定需要的列)
- 大批量数据使用批量插入 (batch_size=10000)
- 添加查询性能监控

### 7.2 代码性能要求

- 避免循环中的重复计算
- 使用 Polars 向量化操作替代 Python 循环
- 大数据集使用流式处理
- 添加性能基准测试

## 8. 依赖管理规范

### 8.1 依赖版本固定

```
# ❌ 错误: 使用 >= 可能导致意外更新
fastapi>=0.111.0

# ✅ 正确: 固定版本
fastapi==0.111.0
```

### 8.2 依赖安全扫描

**定期执行:**
```bash
pip install pip-audit
pip-audit -r requirements.txt
```

## 9. 持续集成规范

### 9.1 CI 流程

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run tests
        run: |
          pytest tests/ --cov=. --cov-report=xml
      - name: Check coverage
        run: |
          coverage report --fail-under=80
      - name: Security scan
        run: |
          pip-audit -r requirements.txt
```

## 10. 代码审查清单

**提交代码前自查:**
- [ ] 代码通过 black, isort, flake8, mypy 检查
- [ ] 测试覆盖率 ≥ 80%
- [ ] 所有测试通过
- [ ] 添加了必要的文档和注释
- [ ] 没有硬编码的敏感信息
- [ ] 没有 TODO/FIXME 注释 (或已创建 issue)
- [ ] 更新了 CHANGELOG.md

**Code Review 检查:**
- [ ] 代码逻辑清晰, 易于理解
- [ ] 没有明显的性能问题
- [ ] 错误处理完整
- [ ] 安全问题已处理
- [ ] 符合项目架构设计

---

## 附录: 工具配置

### pyproject.toml
```toml
[tool.black]
line-length = 100
target-version = ['py311']

[tool.isort]
profile = "black"
line_length = 100

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = "-v --cov=. --cov-report=term-missing"

[tool.coverage.run]
omit = [
    "*/tests/*",
    "*/migrations/*",
    "*/__pycache__/*",
    "*/venv/*",
    "*/.venv/*"
]

[tool.coverage.report]
fail_under = 80
```

### .gitignore
```
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
venv/

# IDE
.vscode/
.idea/
*.swp

# Logs
*.log
logs/

# Database
*.db
*.sqlite

# Temporary files
*.tmp
.DS_Store

# Backups
backups/
*.backup
*.bak

# Coverage
.coverage
htmlcov/
coverage.xml
```
