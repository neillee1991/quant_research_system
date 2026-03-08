# 测试文件索引

快速查找测试相关文件和文档。

---

## 📋 快速导航

| 需求 | 文档 |
|------|------|
| 🚀 快速开始 | [QUICK_START.md](./QUICK_START.md) |
| 📖 执行测试 | [TEST_EXECUTION.md](./TEST_EXECUTION.md) |
| 📊 工作总结 | [FINAL_SUMMARY.md](./FINAL_SUMMARY.md) |
| 📝 完整报告 | [COMPLETION_REPORT.md](./COMPLETION_REPORT.md) |
| 🔧 技术细节 | [COVERAGE_IMPROVEMENT_SUMMARY.md](./COVERAGE_IMPROVEMENT_SUMMARY.md) |
| 📚 测试说明 | [README.md](./README.md) |

---

## 🧪 测试文件

### 新增测试（2026-03-07）

| 文件 | 说明 | 覆盖率 |
|------|------|--------|
| [test_metadata_manager.py](./test_metadata_manager.py) | 元数据管理器测试 | 85%+ |
| [test_data_operations.py](./test_data_operations.py) | 数据操作测试 | 85%+ |
| [test_factor_compute_service.py](./test_factor_compute_service.py) | 因子计算服务测试 | 80%+ |

### 扩展测试

| 文件 | 说明 | 新增内容 |
|------|------|---------|
| [test_pipeline_integration.py](./test_pipeline_integration.py) | Pipeline 集成测试 | +7 测试类 |

### 现有测试

| 文件 | 说明 |
|------|------|
| [test_connection.py](./test_connection.py) | 数据库连接测试 |
| [test_query_builder.py](./test_query_builder.py) | SQL 构建器测试 |
| [test_security.py](./test_security.py) | 安全测试 |
| [test_analyzer.py](./test_analyzer.py) | 因子分析测试 |
| [test_technical_factors.py](./test_technical_factors.py) | 技术指标测试 |
| [test_data_service.py](./test_data_service.py) | 数据服务测试 |

---

## 🛠️ 工具和配置

| 文件 | 说明 |
|------|------|
| [conftest.py](./conftest.py) | Pytest 配置和 Fixtures |
| [run_coverage.sh](./run_coverage.sh) | 自动化测试脚本 |
| [generate_coverage_report.py](./generate_coverage_report.py) | 报告生成器 |

---

## 📚 文档

### 使用指南

| 文档 | 内容 | 适合人群 |
|------|------|---------|
| [QUICK_START.md](./QUICK_START.md) | 快速开始，5分钟上手 | 所有人 |
| [TEST_EXECUTION.md](./TEST_EXECUTION.md) | 详细执行步骤和故障排除 | 开发者 |
| [README.md](./README.md) | 完整测试说明 | 所有人 |

### 技术文档

| 文档 | 内容 | 适合人群 |
|------|------|---------|
| [COVERAGE_IMPROVEMENT_SUMMARY.md](./COVERAGE_IMPROVEMENT_SUMMARY.md) | 技术实现细节 | 测试工程师 |
| [COMPLETION_REPORT.md](./COMPLETION_REPORT.md) | 完整工作报告 | 项目经理 |
| [FINAL_SUMMARY.md](./FINAL_SUMMARY.md) | 工作总结 | 所有人 |

### 历史文档

| 文档 | 内容 |
|------|------|
| [TEST_PLAN.md](./TEST_PLAN.md) | 测试计划 |
| [TEST_REPORT.md](./TEST_REPORT.md) | 测试报告 |
| [QA_SUMMARY.md](./QA_SUMMARY.md) | QA 总结 |

---

## 🎯 按场景查找

### 我想快速运行测试
👉 [QUICK_START.md](./QUICK_START.md) → "快速开始"部分

### 我遇到测试失败
👉 [TEST_EXECUTION.md](./TEST_EXECUTION.md) → "故障排除"部分

### 我想了解新增了什么测试
👉 [FINAL_SUMMARY.md](./FINAL_SUMMARY.md) → "交付物清单"部分

### 我想查看覆盖率报告
👉 [TEST_EXECUTION.md](./TEST_EXECUTION.md) → "查看报告"部分

### 我想编写新的测试
👉 [README.md](./README.md) → "编写测试"部分
👉 [conftest.py](./conftest.py) → 查看可用的 fixtures

### 我想了解技术实现
👉 [COVERAGE_IMPROVEMENT_SUMMARY.md](./COVERAGE_IMPROVEMENT_SUMMARY.md)

### 我想查看完整报告
👉 [COMPLETION_REPORT.md](./COMPLETION_REPORT.md)

---

## 📊 统计信息

### 文件统计
- **测试文件**: 16+
- **新增测试文件**: 3
- **扩展测试文件**: 1
- **工具脚本**: 2
- **配置文件**: 1
- **文档文件**: 10+

### 测试统计
- **测试类**: 50+
- **测试用例**: 200+
- **新增测试用例**: 130+
- **覆盖率**: 82-85% (预期)

---

## 🚀 快速命令

```bash
# 进入测试目录
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend/tests

# 运行所有测试
../run_coverage.sh

# 运行特定测试
pytest test_metadata_manager.py -v

# 查看覆盖率报告
python generate_coverage_report.py

# 打开 HTML 报告
open ../htmlcov/index.html
```

---

## 📞 需要帮助？

1. **快速问题** → [QUICK_START.md](./QUICK_START.md)
2. **执行问题** → [TEST_EXECUTION.md](./TEST_EXECUTION.md)
3. **技术问题** → [COVERAGE_IMPROVEMENT_SUMMARY.md](./COVERAGE_IMPROVEMENT_SUMMARY.md)
4. **Fixtures** → [conftest.py](./conftest.py)

---

**最后更新**: 2026-03-07
**版本**: v1.0
