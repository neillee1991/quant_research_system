# 测试执行指南

**版本**: v1.0
**更新日期**: 2026-03-07
**适用项目**: QuantSystem Backend

---

## 📋 目录

1. [快速开始](#快速开始)
2. [环境准备](#环境准备)
3. [运行测试](#运行测试)
4. [测试类型](#测试类型)
5. [覆盖率报告](#覆盖率报告)
6. [测试结果解读](#测试结果解读)
7. [常见问题排查](#常见问题排查)
8. [最佳实践](#最佳实践)

---

## 🚀 快速开始

### 最简单的方式

```bash
# 1. 进入项目目录
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 2. 激活虚拟环境
source .venv/bin/activate

# 3. 运行所有测试
pytest tests/ -v

# 4. 查看测试报告
# 测试结果会直接显示在终端
```

### 运行特定测试

```bash
# 运行单个测试文件
pytest tests/test_query_builder.py -v

# 运行特定测试类
pytest tests/test_analyzer.py::TestFactorAnalyzer -v

# 运行特定测试方法
pytest tests/test_analyzer.py::TestFactorAnalyzer::test_calculate_ic -v
```

---

## 🔧 环境准备

### 1. 检查 Python 环境

```bash
# 检查 Python 版本 (需要 3.11+)
python --version

# 检查虚拟环境
which python
# 应该输出: /Users/lisheng/Code/quantsystem/quant_research_system/backend/.venv/bin/python
```

### 2. 安装测试依赖

```bash
# 激活虚拟环境
source .venv/bin/activate

# 安装测试依赖
pip install pytest pytest-cov pytest-asyncio pytest-mock pytest-xdist

# 验证安装
pytest --version
```

### 3. 检查 DolphinDB 连接

```bash
# 检查 DolphinDB 是否运行
docker ps | grep dolphindb

# 如果没有运行，启动 DolphinDB
docker-compose up -d dolphindb

# 测试连接
python -c "
from store.dolphindb_client import db_client
db_client.connect()
print('DolphinDB 连接成功')
db_client.close()
"
```

### 4. 准备测试数据

```bash
# 运行数据初始化脚本 (如果需要)
python scripts/init_test_data.py

# 或者使用 seed 数据
python -c "
from store.dolphindb_client import db_client
db_client.connect()
db_client.seed_sync_task_config()
db_client.seed_factor_data_config()
print('测试数据准备完成')
"
```

---

## 🧪 运行测试

### 基本命令

#### 运行所有测试

```bash
# 基本运行
pytest tests/

# 详细输出 (-v: verbose)
pytest tests/ -v

# 显示打印输出 (-s: no capture)
pytest tests/ -s

# 简短的错误信息 (--tb=short)
pytest tests/ --tb=short

# 完整的错误信息 (--tb=long)
pytest tests/ --tb=long
```

#### 运行特定目录的测试

```bash
# 运行单元测试
pytest tests/unit/ -v

# 运行集成测试
pytest tests/integration/ -v

# 运行 API 测试
pytest tests/api/ -v

# 运行基础设施测试
pytest tests/infrastructure/ -v
```

#### 运行特定文件的测试

```bash
# 运行 QueryBuilder 测试
pytest tests/test_query_builder.py -v

# 运行 Connection 测试
pytest tests/test_connection.py -v

# 运行分析器测试
pytest tests/test_analyzer.py -v

# 运行技术因子测试
pytest tests/test_technical_factors.py -v
```

#### 运行特定测试用例

```bash
# 运行特定测试类
pytest tests/test_query_builder.py::TestQueryBuilder -v

# 运行特定测试方法
pytest tests/test_query_builder.py::TestQueryBuilder::test_replace_params -v

# 使用模式匹配 (-k)
pytest tests/ -k "test_connection" -v
pytest tests/ -k "test_query or test_execute" -v
```

### 高级选项

#### 并行运行测试

```bash
# 安装 pytest-xdist
pip install pytest-xdist

# 自动检测 CPU 核心数并行运行
pytest tests/ -n auto

# 指定并行数量
pytest tests/ -n 4

# 并行运行并显示详细输出
pytest tests/ -n auto -v
```

#### 失败时停止

```bash
# 第一个失败后停止
pytest tests/ -x

# 失败 3 次后停止
pytest tests/ --maxfail=3
```

#### 重新运行失败的测试

```bash
# 第一次运行
pytest tests/ -v

# 只重新运行失败的测试
pytest tests/ --lf  # --last-failed

# 先运行失败的，再运行其他的
pytest tests/ --ff  # --failed-first
```

#### 跳过慢速测试

```bash
# 标记慢速测试
@pytest.mark.slow
def test_slow_operation():
    pass

# 跳过慢速测试
pytest tests/ -m "not slow"

# 只运行慢速测试
pytest tests/ -m "slow"
```

#### 设置超时

```bash
# 安装 pytest-timeout
pip install pytest-timeout

# 设置全局超时 (30 秒)
pytest tests/ --timeout=30

# 在测试中设置超时
@pytest.mark.timeout(60)
def test_long_operation():
    pass
```

---

## 📊 测试类型

### 1. 单元测试

**目的**: 测试单个函数或类的功能

**运行方式**:
```bash
# 运行所有单元测试
pytest tests/test_technical_factors.py -v
pytest tests/test_analyzer.py -v
pytest tests/test_data_service.py -v
pytest tests/test_security.py -v
```

**特点**:
- 快速 (< 1 秒)
- 独立 (不依赖外部服务)
- 使用 Mock 和 Stub

### 2. 集成测试

**目的**: 测试多个组件的协作

**运行方式**:
```bash
# 运行所有集成测试
pytest tests/test_pipeline_integration.py -v
pytest tests/test_analyzer_integration.py -v
pytest tests/test_alphalens_integration.py -v
```

**特点**:
- 较慢 (< 10 秒)
- 依赖外部服务 (DolphinDB)
- 测试真实交互

### 3. API 测试

**目的**: 测试 API 端点

**运行方式**:
```bash
# 运行所有 API 测试
pytest tests/test_alphalens_api.py -v
pytest tests/test_index_pool_api.py -v
pytest tests/test_generic_task_api.py -v
```

**特点**:
- 中等速度 (< 5 秒)
- 测试 HTTP 请求/响应
- 验证 API 契约

### 4. 基础设施测试

**目的**: 测试底层基础设施

**运行方式**:
```bash
# 运行基础设施测试
pytest tests/infrastructure_test_query_builder.py -v
pytest tests/infrastructure_test_repository.py -v
```

**特点**:
- 测试数据库连接
- 测试查询构建
- 测试数据操作

### 5. 回归测试

**目的**: 确保重构后功能不变

**运行方式**:
```bash
# 运行所有测试 (完整回归测试)
pytest tests/ -v

# 运行特定回归测试
pytest tests/test_connection.py -v
pytest tests/test_query_builder.py -v
```

**特点**:
- 覆盖所有功能
- 验证向后兼容性
- 对比重构前后

---

## 📈 覆盖率报告

### 生成覆盖率报告

#### 终端报告

```bash
# 基本覆盖率报告
pytest tests/ --cov=.

# 显示未覆盖的行
pytest tests/ --cov=. --cov-report=term-missing

# 只显示特定模块的覆盖率
pytest tests/ --cov=store/dolphindb --cov-report=term-missing

# 只显示覆盖率低于 80% 的文件
pytest tests/ --cov=. --cov-report=term --cov-fail-under=80
```

#### HTML 报告

```bash
# 生成 HTML 覆盖率报告
pytest tests/ --cov=. --cov-report=html

# 查看报告
open htmlcov/index.html

# 生成并自动打开
pytest tests/ --cov=. --cov-report=html && open htmlcov/index.html
```

#### XML 报告 (用于 CI/CD)

```bash
# 生成 XML 报告
pytest tests/ --cov=. --cov-report=xml

# 生成 JUnit XML 报告
pytest tests/ --junitxml=test-results.xml
```

### 覆盖率配置

创建 `.coveragerc` 文件:

```ini
[run]
source = .
omit =
    .venv/*
    tests/*
    */migrations/*
    */site-packages/*

[report]
exclude_lines =
    pragma: no cover
    def __repr__
    raise AssertionError
    raise NotImplementedError
    if __name__ == .__main__.:
    if TYPE_CHECKING:
```

### 查看覆盖率详情

```bash
# 生成覆盖率报告
pytest tests/ --cov=store/dolphindb --cov-report=term-missing

# 输出示例:
# Name                                    Stmts   Miss  Cover   Missing
# ---------------------------------------------------------------------
# store/dolphindb/__init__.py                10      0   100%
# store/dolphindb/connection.py             150     60    60%   45-67, 89-102
# store/dolphindb/query_builder.py          120     36    70%   78-92, 110-115
# store/dolphindb/meta_manager.py           180    108    40%   多行未覆盖
# store/dolphindb/data_operations.py        160     96    40%   多行未覆盖
# store/dolphindb/seed_data.py              140     98    30%   多行未覆盖
# ---------------------------------------------------------------------
# TOTAL                                     760    398    48%
```

---

## 📖 测试结果解读

### 测试输出格式

```bash
# 运行测试
pytest tests/test_query_builder.py -v

# 输出示例:
tests/test_query_builder.py::TestQueryBuilder::test_replace_params PASSED     [ 10%]
tests/test_query_builder.py::TestQueryBuilder::test_adapt_syntax PASSED       [ 20%]
tests/test_query_builder.py::TestQueryBuilder::test_where_in FAILED           [ 30%]
tests/test_query_builder.py::TestQueryBuilder::test_execute_query SKIPPED     [ 40%]

================================= FAILURES =================================
_______________________ TestQueryBuilder.test_where_in _______________________

    def test_where_in(self):
        sql = "SELECT * FROM table WHERE id IN (%s)"
        params = [[1, 2, 3]]
>       result = builder.replace_params(sql, params)
E       AssertionError: assert 'SELECT * FROM table WHERE id IN (1,2,3)' == 'SELECT * FROM table WHERE id IN ([1,2,3])'

tests/test_query_builder.py:45: AssertionError
========================= short test summary info ==========================
FAILED tests/test_query_builder.py::TestQueryBuilder::test_where_in
=================== 1 failed, 2 passed, 1 skipped in 0.50s ===================
```

### 测试状态说明

| 状态 | 符号 | 说明 |
|------|------|------|
| PASSED | `.` 或 `✓` | 测试通过 |
| FAILED | `F` 或 `✗` | 测试失败 |
| ERROR | `E` | 测试执行出错 |
| SKIPPED | `s` | 测试被跳过 |
| XFAIL | `x` | 预期失败 (已知问题) |
| XPASS | `X` | 预期失败但通过了 |

### 失败原因分析

#### 1. AssertionError

```python
# 断言失败
AssertionError: assert 'actual' == 'expected'
```

**原因**: 实际结果与预期不符
**解决**: 检查实现逻辑或调整测试断言

#### 2. AttributeError

```python
# 属性不存在
AttributeError: 'Mock' object has no attribute 'lock'
```

**原因**: Mock 对象配置不完整
**解决**: 添加缺失的属性或方法

#### 3. TypeError

```python
# 类型错误
TypeError: 'NoneType' object is not callable
```

**原因**: 对象类型不正确
**解决**: 检查对象初始化和类型转换

#### 4. ValueError

```python
# 值错误
ValueError: invalid literal for int() with base 10: 'abc'
```

**原因**: 输入值不符合预期
**解决**: 添加输入验证或调整测试数据

### 覆盖率指标解读

| 覆盖率 | 评级 | 说明 |
|--------|------|------|
| 90-100% | 优秀 | 测试非常充分 |
| 80-90% | 良好 | 测试较为充分 |
| 70-80% | 及格 | 测试基本覆盖 |
| 60-70% | 不足 | 需要补充测试 |
| < 60% | 严重不足 | 测试严重缺失 |

---

## 🔍 常见问题排查

### 问题 1: 无法连接 DolphinDB

**症状**:
```
ConnectionError: Failed to connect to DolphinDB at localhost:8848
```

**排查步骤**:
```bash
# 1. 检查 DolphinDB 是否运行
docker ps | grep dolphindb

# 2. 检查端口是否被占用
lsof -i :8848

# 3. 启动 DolphinDB
docker-compose up -d dolphindb

# 4. 查看 DolphinDB 日志
docker logs dolphindb

# 5. 测试连接
telnet localhost 8848
```

**解决方案**:
- 启动 DolphinDB 服务
- 检查防火墙设置
- 验证连接配置

### 问题 2: 测试超时

**症状**:
```
FAILED tests/test_slow.py::test_long_operation - Timeout >30.0s
```

**排查步骤**:
```bash
# 1. 增加超时时间
pytest tests/ --timeout=60

# 2. 检查是否有死锁
# 查看测试日志

# 3. 使用调试模式
pytest tests/test_slow.py -s --pdb
```

**解决方案**:
- 增加超时时间
- 优化测试逻辑
- 使用 Mock 替代慢速操作

### 问题 3: 测试数据污染

**症状**:
```
AssertionError: Expected 0 records, but found 5
```

**排查步骤**:
```bash
# 1. 检查测试隔离
# 确保每个测试都有独立的数据

# 2. 使用 fixture 清理数据
@pytest.fixture(autouse=True)
def cleanup():
    yield
    # 清理测试数据
    db_client.execute("DELETE FROM test_table")

# 3. 使用事务回滚
@pytest.fixture
def db_transaction():
    db_client.execute("BEGIN")
    yield
    db_client.execute("ROLLBACK")
```

**解决方案**:
- 使用 fixture 清理数据
- 使用事务回滚
- 使用独立的测试数据库

### 问题 4: Mock 对象配置错误

**症状**:
```
AttributeError: 'Mock' object has no attribute '__enter__'
```

**排查步骤**:
```python
# 1. 检查 Mock 配置
mock_obj = MagicMock()

# 2. 添加上下文管理器支持
mock_obj.__enter__ = MagicMock(return_value=None)
mock_obj.__exit__ = MagicMock(return_value=None)

# 3. 或使用 patch
with patch('module.Class') as mock_class:
    mock_class.return_value.__enter__.return_value = mock_obj
```

**解决方案**:
- 正确配置 Mock 对象
- 使用 MagicMock 自动支持魔术方法
- 使用 patch 装饰器

### 问题 5: 导入错误

**症状**:
```
ImportError: cannot import name 'DolphinDBClient' from 'store.dolphindb_client'
```

**排查步骤**:
```bash
# 1. 检查 Python 路径
python -c "import sys; print(sys.path)"

# 2. 检查模块是否存在
ls -la store/dolphindb_client.py

# 3. 检查 __init__.py
cat store/__init__.py

# 4. 重新安装包
pip install -e .
```

**解决方案**:
- 检查导入路径
- 确保 `__init__.py` 存在
- 重新安装包

### 问题 6: 覆盖率报告不准确

**症状**:
```
Coverage report shows 0% for all files
```

**排查步骤**:
```bash
# 1. 检查 .coveragerc 配置
cat .coveragerc

# 2. 指定源代码目录
pytest tests/ --cov=store/dolphindb

# 3. 清理旧的覆盖率数据
rm -rf .coverage htmlcov/

# 4. 重新生成报告
pytest tests/ --cov=. --cov-report=html
```

**解决方案**:
- 正确配置 `.coveragerc`
- 指定正确的源代码目录
- 清理旧的覆盖率数据

---

## 💡 最佳实践

### 1. 测试命名规范

```python
# 好的命名
def test_query_builder_replaces_single_param():
    pass

def test_connection_handles_timeout_error():
    pass

def test_analyzer_calculates_ic_correctly():
    pass

# 不好的命名
def test1():
    pass

def test_stuff():
    pass

def test_it_works():
    pass
```

### 2. 使用 Fixture

```python
# 共享的测试数据
@pytest.fixture
def sample_dataframe():
    return pl.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["2024-01-01", "2024-01-01"],
        "close": [10.0, 20.0]
    })

# 使用 fixture
def test_calculate_returns(sample_dataframe):
    result = calculate_returns(sample_dataframe)
    assert result is not None
```

### 3. 参数化测试

```python
# 测试多个输入
@pytest.mark.parametrize("input,expected", [
    (1, 2),
    (2, 4),
    (3, 6),
])
def test_double(input, expected):
    assert double(input) == expected
```

### 4. 测试隔离

```python
# 每个测试都独立
@pytest.fixture(autouse=True)
def reset_state():
    # 测试前准备
    setup_test_data()
    yield
    # 测试后清理
    cleanup_test_data()
```

### 5. 使用标记

```python
# 标记慢速测试
@pytest.mark.slow
def test_large_dataset():
    pass

# 标记集成测试
@pytest.mark.integration
def test_full_pipeline():
    pass

# 运行时过滤
# pytest tests/ -m "not slow"
# pytest tests/ -m "integration"
```

### 6. 测试文档

```python
def test_query_builder_handles_null_params():
    """
    测试 QueryBuilder 正确处理 NULL 参数

    场景:
    - 输入包含 None 值的参数
    - 应该转换为 DolphinDB 的 NULL

    预期:
    - SQL 中的 %s 被替换为 NULL
    - 不抛出异常
    """
    sql = "SELECT * FROM table WHERE value = %s"
    params = [None]
    result = builder.replace_params(sql, params)
    assert "NULL" in result
```

### 7. 持续集成

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.11
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      - name: Run tests
        run: |
          pytest tests/ --cov=. --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

---

## 📚 参考资料

### 官方文档

- [pytest 文档](https://docs.pytest.org/)
- [pytest-cov 文档](https://pytest-cov.readthedocs.io/)
- [pytest-asyncio 文档](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock 文档](https://pytest-mock.readthedocs.io/)

### 项目文档

- [测试计划](./TEST_PLAN.md)
- [测试报告](./TEST_REPORT.md)
- [QA 总结](./QA_SUMMARY.md)
- [回归测试总结](./REGRESSION_TEST_SUMMARY.md)

### 相关资源

- [测试驱动开发 (TDD)](https://en.wikipedia.org/wiki/Test-driven_development)
- [单元测试最佳实践](https://martinfowler.com/bliki/UnitTest.html)
- [测试金字塔](https://martinfowler.com/articles/practical-test-pyramid.html)

---

**最后更新**: 2026-03-07
**维护者**: 回归测试专员
**版本**: v1.0
