# 测试问题跟踪清单

**版本**: v1.0
**更新日期**: 2026-03-07
**项目**: QuantSystem Backend - DolphinDB 重构

---

## 📊 问题统计

| 优先级 | 总数 | 已修复 | 进行中 | 待处理 | 完成率 |
|--------|------|--------|--------|--------|--------|
| P0 (阻塞性) | 0 | 0 | 0 | 0 | - |
| P1 (重要) | 2 | 0 | 0 | 2 | 0% |
| P2 (次要) | 13 | 0 | 0 | 13 | 0% |
| P3 (优化) | 2 | 0 | 0 | 2 | 0% |
| **总计** | **17** | **0** | **0** | **17** | **0%** |

---

## 🔴 P0 问题 (阻塞性问题)

**无** ✅

---

## 🟠 P1 问题 (重要问题)

### P1-1: Mock 对象配置问题

| 字段 | 内容 |
|------|------|
| **问题ID** | P1-1 |
| **优先级** | P1 (重要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 1 小时 |

**问题描述**:
Mock 对象不支持 `with conn.lock:` 上下文管理器协议，导致查询执行测试失败。

**影响范围**:
- 影响 4 个测试用例
- 无法测试查询执行逻辑
- 测试覆盖率降低

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_execute_query`
- `test_query_builder.py::TestQueryBuilder::test_execute_with_params`
- `test_query_builder.py::TestQueryBuilder::test_execute_error_handling`
- `test_query_builder.py::TestQueryBuilder::test_execute_with_lock`

**错误信息**:
```python
AttributeError: 'Mock' object has no attribute '__enter__'
```

**根本原因**:
测试中的 Mock 对象未配置上下文管理器协议，而实际代码使用了 `with self.conn.lock:`。

**修复建议**:
```python
# 在测试 fixture 中添加
@pytest.fixture
def mock_connection():
    mock_conn = MagicMock()
    mock_conn.lock = MagicMock()
    mock_conn.lock.__enter__ = MagicMock(return_value=None)
    mock_conn.lock.__exit__ = MagicMock(return_value=None)
    return mock_conn
```

**验证方法**:
```bash
pytest tests/test_query_builder.py::TestQueryBuilder::test_execute_query -v
```

**相关文件**:
- `tests/test_query_builder.py`
- `store/dolphindb/query_builder.py`

---

### P1-2: IN 子句列表参数处理

| 字段 | 内容 |
|------|------|
| **问题ID** | P1-2 |
| **优先级** | P1 (重要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 2 小时 |

**问题描述**:
列表参数在 IN 子句中的处理方式与预期不同，可能影响查询功能。

**影响范围**:
- 影响 1 个测试用例
- 可能影响实际查询功能
- 需要验证数据正确性

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_where_in_clause`

**错误信息**:
```python
AssertionError: assert 'WHERE id IN (1,2,3)' == 'WHERE id IN ([1,2,3])'
```

**根本原因**:
列表参数的序列化方式与预期不同，需要确认是实现问题还是测试断言问题。

**修复建议**:
1. 编写集成测试验证实际查询结果
2. 如果查询结果正确，调整测试断言
3. 如果查询结果错误，修复实现逻辑

**验证方法**:
```bash
# 单元测试
pytest tests/test_query_builder.py::TestQueryBuilder::test_where_in_clause -v

# 集成测试
python -c "
from store.dolphindb_client import db_client
db_client.connect()
result = db_client.query('SELECT * FROM table WHERE id IN (%s)', [[1,2,3]])
print(result)
"
```

**相关文件**:
- `tests/test_query_builder.py`
- `store/dolphindb/query_builder.py`

---

## 🟡 P2 问题 (次要问题)

### P2-1: 字符串引号不一致 (测试1)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-1 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 15 分钟 |

**问题描述**:
实际实现使用双引号 `"test"`，测试期望单引号 `'test'`。

**影响范围**:
- 影响 1 个测试用例
- 仅测试断言问题，功能正确

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_string_param_single_quote`

**修复建议**:
调整测试断言，使用双引号。

**相关文件**:
- `tests/test_query_builder.py`

---

### P2-2: 字符串引号不一致 (测试2)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-2 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 15 分钟 |

**问题描述**:
实际实现使用双引号 `"test"`，测试期望单引号 `'test'`。

**影响范围**:
- 影响 1 个测试用例
- 仅测试断言问题，功能正确

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_string_param_double_quote`

**修复建议**:
调整测试断言，使用双引号。

**相关文件**:
- `tests/test_query_builder.py`

---

### P2-3: 日期格式处理差异 (测试1)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-3 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 20 分钟 |

**问题描述**:
实际使用 `temporalParse("20240101", "yyyyMMdd")`，测试期望 `2024.01.01`。

**影响范围**:
- 影响 1 个测试用例
- 功能正确，格式不同

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_date_param_format1`

**修复建议**:
调整测试断言，使用 `temporalParse` 格式。

**相关文件**:
- `tests/test_query_builder.py`

---

### P2-4: 日期格式处理差异 (测试2)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-4 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 20 分钟 |

**问题描述**:
实际使用 `temporalParse("20240101", "yyyyMMdd")`，测试期望 `2024.01.01`。

**影响范围**:
- 影响 1 个测试用例
- 功能正确，格式不同

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_date_param_format2`

**修复建议**:
调整测试断言，使用 `temporalParse` 格式。

**相关文件**:
- `tests/test_query_builder.py`

---

### P2-5: 日期格式处理差异 (测试3)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-5 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 20 分钟 |

**问题描述**:
实际使用 `temporalParse("20240101", "yyyyMMdd")`，测试期望 `2024.01.01`。

**影响范围**:
- 影响 1 个测试用例
- 功能正确，格式不同

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_date_param_format3`

**修复建议**:
调整测试断言，使用 `temporalParse` 格式。

**相关文件**:
- `tests/test_query_builder.py`

---

### P2-6: 回归测试日期字符串失败

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-6 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 20 分钟 |

**问题描述**:
日期字符串转义格式不匹配，与 QueryBuilder 的日期处理相关。

**影响范围**:
- 影响 1 个回归测试
- 功能正确，格式不同

**失败的测试**:
- `test_dolphindb_utils.py::TestEscapeValue::test_date_string`

**错误信息**:
```python
AssertionError: assert 'temporalParse("20240101", "yyyyMMdd")' == '2024.01.01'
```

**修复建议**:
调整测试断言以匹配新的日期处理方式。

**相关文件**:
- `tests/test_dolphindb_utils.py`
- `store/dolphindb/query_builder.py`

---

### P2-7: 空 SQL 验证缺失 (测试1)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-7 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 30 分钟 |

**问题描述**:
实际实现未验证空 SQL，测试期望抛出 ValueError。

**影响范围**:
- 影响 1 个测试用例
- 边界条件处理

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_empty_sql`

**修复建议**:
在 QueryBuilder 中添加空 SQL 验证，或调整测试期望。

**相关文件**:
- `tests/test_query_builder.py`
- `store/dolphindb/query_builder.py`

---

### P2-8: 空 SQL 验证缺失 (测试2)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-8 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 30 分钟 |

**问题描述**:
实际实现未验证空白 SQL，测试期望抛出 ValueError。

**影响范围**:
- 影响 1 个测试用例
- 边界条件处理

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_whitespace_sql`

**修复建议**:
在 QueryBuilder 中添加空白 SQL 验证，或调整测试期望。

**相关文件**:
- `tests/test_query_builder.py`
- `store/dolphindb/query_builder.py`

---

### P2-9: ILIKE 操作符未转换

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-9 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 1 小时 |

**问题描述**:
实际实现未转换 ILIKE 操作符，测试期望转换为 LIKE。

**影响范围**:
- 影响 1 个测试用例
- 功能增强

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_ilike_operator`

**修复建议**:
1. 确认 DolphinDB 是否支持 ILIKE
2. 如果不支持，添加转换逻辑
3. 如果支持，调整测试期望

**相关文件**:
- `tests/test_query_builder.py`
- `store/dolphindb/query_builder.py`

---

### P2-10: SQL 注入防护 (测试1)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-10 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 30 分钟 |

**问题描述**:
实际使用双引号转义，测试期望单引号转义。

**影响范围**:
- 影响 1 个测试用例
- 需要验证安全性

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_sql_injection_single_quote`

**修复建议**:
验证双引号转义的安全性，调整测试断言。

**相关文件**:
- `tests/test_query_builder.py`
- `store/dolphindb/query_builder.py`

---

### P2-11: SQL 注入防护 (测试2)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-11 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 30 分钟 |

**问题描述**:
实际使用双引号转义，测试期望单引号转义。

**影响范围**:
- 影响 1 个测试用例
- 需要验证安全性

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_sql_injection_double_quote`

**修复建议**:
验证双引号转义的安全性，调整测试断言。

**相关文件**:
- `tests/test_query_builder.py`
- `store/dolphindb/query_builder.py`

---

### P2-12: LIMIT 语法 (测试1)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-12 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 15 分钟 |

**问题描述**:
实际保持 `LIMIT` 不变，测试期望转换为 `top`。

**影响范围**:
- 影响 1 个测试用例
- 无实际影响 (DolphinDB 支持两种语法)

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_limit_syntax1`

**修复建议**:
调整测试断言，接受 `LIMIT` 语法。

**相关文件**:
- `tests/test_query_builder.py`

---

### P2-13: LIMIT 语法 (测试2)

| 字段 | 内容 |
|------|------|
| **问题ID** | P2-13 |
| **优先级** | P2 (次要) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 15 分钟 |

**问题描述**:
实际保持 `LIMIT` 不变，测试期望转换为 `top`。

**影响范围**:
- 影响 1 个测试用例
- 无实际影响 (DolphinDB 支持两种语法)

**失败的测试**:
- `test_query_builder.py::TestQueryBuilder::test_limit_syntax2`

**修复建议**:
调整测试断言，接受 `LIMIT` 语法。

**相关文件**:
- `tests/test_query_builder.py`

---

## 🟢 P3 问题 (优化建议)

### P3-1: 测试覆盖率不足

| 字段 | 内容 |
|------|------|
| **问题ID** | P3-1 |
| **优先级** | P3 (优化) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 2-3 天 |

**问题描述**:
当前代码覆盖率约 48%，未达到 80% 目标。

**影响范围**:
- 整体测试质量
- 代码可维护性

**改进建议**:
1. 完成 Connection 测试 (+8%)
2. 创建 MetadataManager 测试 (+12%)
3. 创建 DataOperations 测试 (+10%)
4. 创建 SeedDataManager 测试 (+8%)

**相关文件**:
- `store/dolphindb/connection.py`
- `store/dolphindb/meta_manager.py`
- `store/dolphindb/data_operations.py`
- `store/dolphindb/seed_data.py`

---

### P3-2: 缺少性能和压力测试

| 字段 | 内容 |
|------|------|
| **问题ID** | P3-2 |
| **优先级** | P3 (优化) |
| **状态** | 待处理 |
| **发现日期** | 2026-03-07 |
| **负责人** | 待分配 |
| **预计修复时间** | 1 周 |

**问题描述**:
缺少性能基准测试、并发测试和压力测试。

**影响范围**:
- 性能监控
- 系统稳定性

**改进建议**:
1. 创建性能基准测试
2. 创建并发测试
3. 创建边界条件测试
4. 创建压力测试

**相关文件**:
- `tests/test_performance_benchmark.py` (待创建)
- `tests/test_concurrency.py` (待创建)
- `tests/test_edge_cases.py` (待创建)

---

## 📈 修复进度跟踪

### 本周计划 (2026-03-07 ~ 2026-03-13)

| 问题ID | 优先级 | 预计时间 | 负责人 | 状态 |
|--------|--------|----------|--------|------|
| P1-1 | P1 | 1 小时 | 待分配 | 待处理 |
| P1-2 | P1 | 2 小时 | 待分配 | 待处理 |
| P2-1 ~ P2-6 | P2 | 2 小时 | 待分配 | 待处理 |

**总计**: 5 小时

### 下周计划 (2026-03-14 ~ 2026-03-20)

| 问题ID | 优先级 | 预计时间 | 负责人 | 状态 |
|--------|--------|----------|--------|------|
| P2-7 ~ P2-13 | P2 | 4 小时 | 待分配 | 待处理 |
| P3-1 | P3 | 2-3 天 | 待分配 | 待处理 |

**总计**: 3 天

### 两周后计划 (2026-03-21 ~ 2026-03-27)

| 问题ID | 优先级 | 预计时间 | 负责人 | 状态 |
|--------|--------|----------|--------|------|
| P3-2 | P3 | 1 周 | 待分配 | 待处理 |

**总计**: 1 周

---

## 📝 更新日志

| 日期 | 更新内容 | 更新人 |
|------|----------|--------|
| 2026-03-07 | 创建问题跟踪清单 | 回归测试专员 |
| - | - | - |
| - | - | - |

---

## 📚 相关文档

- [测试计划](./TEST_PLAN.md)
- [测试报告](./TEST_REPORT.md)
- [QA 总结](./QA_SUMMARY.md)
- [回归测试总结](./REGRESSION_TEST_SUMMARY.md)
- [测试执行指南](./TEST_EXECUTION_GUIDE.md)

---

**最后更新**: 2026-03-07
**维护者**: 回归测试专员
**版本**: v1.0
