# 快速测试指南

## 🚀 快速开始

### 运行所有测试
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 方法 1: 使用测试脚本（推荐）
chmod +x tests/run_coverage.sh
./tests/run_coverage.sh

# 方法 2: 直接使用 pytest
pytest tests/ -v
```

### 运行特定测试文件
```bash
# 测试元数据管理器
pytest tests/test_metadata_manager.py -v

# 测试数据操作
pytest tests/test_data_operations.py -v

# 测试因子计算服务
pytest tests/test_factor_compute_service.py -v

# 测试 Pipeline
pytest tests/test_pipeline_integration.py -v
```

### 运行特定测试类
```bash
# 运行特定测试类
pytest tests/test_metadata_manager.py::TestVersionManagement -v

# 运行特定测试方法
pytest tests/test_metadata_manager.py::TestVersionManagement::test_create_task_version_first_version -v
```

---

## 📊 生成覆盖率报告

### 完整覆盖率报告
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 生成 HTML + JSON + 终端报告
pytest tests/ \
    --cov=infrastructure \
    --cov=services \
    --cov=config \
    --cov-report=html \
    --cov-report=json \
    --cov-report=term-missing \
    -v

# 查看 HTML 报告
open htmlcov/index.html

# 查看详细分析
python tests/generate_coverage_report.py
```

### 只看覆盖率摘要
```bash
pytest tests/ \
    --cov=infrastructure \
    --cov=services \
    --cov=config \
    --cov-report=term \
    --tb=no \
    -q
```

---

## 🔍 调试测试

### 显示详细输出
```bash
# 显示 print 输出
pytest tests/test_metadata_manager.py -v -s

# 显示完整错误堆栈
pytest tests/test_metadata_manager.py -v --tb=long

# 在第一个失败时停止
pytest tests/ -x
```

### 运行失败的测试
```bash
# 只运行上次失败的测试
pytest --lf

# 先运行失败的，再运行其他的
pytest --ff
```

---

## 🏷️ 使用测试标记

### 只运行单元测试
```bash
pytest tests/ -m unit -v
```

### 跳过慢速测试
```bash
pytest tests/ -m "not slow" -v
```

### 只运行集成测试
```bash
pytest tests/ -m integration -v
```

---

## 📈 检查覆盖率

### 检查特定模块
```bash
# 只检查 infrastructure
pytest tests/ --cov=infrastructure --cov-report=term-missing

# 只检查 services
pytest tests/ --cov=services --cov-report=term-missing
```

### 设置覆盖率失败阈值
```bash
# 如果覆盖率低于 80% 则失败
pytest tests/ \
    --cov=infrastructure \
    --cov=services \
    --cov-report=term \
    --cov-fail-under=80
```

---

## 🛠️ 常用命令组合

### 开发时快速测试
```bash
# 快速运行，不生成报告
pytest tests/ -q --tb=short
```

### 提交前完整检查
```bash
# 完整测试 + 覆盖率 + 详细报告
pytest tests/ \
    --cov=infrastructure \
    --cov=services \
    --cov=config \
    --cov-report=html \
    --cov-report=term-missing \
    --cov-fail-under=80 \
    -v
```

### CI/CD 模式
```bash
# 适合 CI 环境
pytest tests/ \
    --cov=infrastructure \
    --cov=services \
    --cov=config \
    --cov-report=xml \
    --cov-report=term \
    --junitxml=test-results.xml \
    --cov-fail-under=80 \
    -v
```

---

## 📝 编写新测试

### 测试文件命名
- 文件名: `test_<module_name>.py`
- 测试类: `Test<ClassName>`
- 测试方法: `test_<what_it_tests>`

### 使用 Fixtures
```python
def test_something(mock_db_connection, sample_daily_data):
    """使用 conftest.py 中定义的 fixtures"""
    # 测试代码
    pass
```

### Mock 外部依赖
```python
from unittest.mock import Mock, patch

def test_with_mock():
    mock_db = Mock()
    mock_db.query.return_value = pl.DataFrame()

    # 测试代码
    pass
```

---

## 🐛 常见问题

### 问题 1: 导入错误
```bash
# 确保在 backend 目录运行
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
pytest tests/
```

### 问题 2: Mock 不生效
```python
# 确保 patch 路径正确
# 错误: @patch('dolphindb.session')
# 正确: @patch('infrastructure.database.connection.ddb')
```

### 问题 3: 测试数据库连接
```python
# 使用 Mock 而不是真实连接
@pytest.fixture
def mock_db():
    return Mock()
```

---

## 📚 参考资源

### Pytest 文档
- [Pytest 官方文档](https://docs.pytest.org/)
- [Pytest-cov 文档](https://pytest-cov.readthedocs.io/)

### 项目测试文档
- `tests/COVERAGE_IMPROVEMENT_SUMMARY.md` - 覆盖率提升总结
- `tests/conftest.py` - Fixtures 定义
- `tests/README.md` - 测试说明

---

## ✅ 验收检查清单

运行测试前检查：
- [ ] 在 backend 目录
- [ ] 虚拟环境已激活
- [ ] 依赖已安装 (`pip install -r requirements.txt`)

测试通过标准：
- [ ] 所有测试通过
- [ ] 覆盖率 >= 80%
- [ ] 无测试警告
- [ ] 无 import 错误

---

## 💡 最佳实践

1. **测试隔离**: 每个测试独立，不依赖其他测试
2. **使用 Mock**: Mock 外部依赖，保持测试快速
3. **清晰命名**: 测试名称描述测试内容
4. **单一职责**: 每个测试只测试一个功能点
5. **边界测试**: 测试边界条件和异常情况
6. **持续运行**: 开发时频繁运行测试

---

## 🎯 下一步

1. 运行测试脚本: `./tests/run_coverage.sh`
2. 查看覆盖率报告: `open htmlcov/index.html`
3. 如果覆盖率不足，查看详细报告: `python tests/generate_coverage_report.py`
4. 根据建议补充测试用例
5. 重复直到覆盖率达标

---

**祝测试顺利！** 🎉
