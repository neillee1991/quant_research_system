# DolphinDB 重构 - 全面测试计划

**测试工程师**: QA Agent
**测试日期**: 2026-03-07
**项目路径**: `/Users/lisheng/Code/quantsystem/quant_research_system/backend`

---

## 测试目标

对 DolphinDB 客户端重构进行全面的功能测试和回归验证，确保：
1. 重构后的代码功能完全一致
2. 向后兼容性 100%
3. 性能不低于重构前
4. 代码覆盖率 > 80%
5. 无 P0/P1 级别的 bug

---

## 测试范围

### 阶段1: 基础设施层测试

#### 1.1 QueryBuilder 测试 ✅
**文件**: `tests/test_query_builder.py`

**测试用例**:
- ✅ SELECT 语句构建
- ✅ WHERE 条件（IN, BETWEEN, 等于）
- ✅ ORDER BY 和 LIMIT
- ✅ 参数化查询（SQL注入防护）
- ✅ 边界条件（空列表、null值）
- ✅ 语法适配（PostgreSQL → DolphinDB）
- ✅ 裸表名 → loadTable() 转换

**优先级**: P0（核心功能）

#### 1.2 DolphinDBConnection 测试 ✅
**文件**: `tests/test_connection.py`

**测试用例**:
- ✅ 连接建立和关闭
- ✅ 单例模式验证
- ✅ 线程安全测试
- ✅ 自动重连机制
- ✅ 连接池管理
- ✅ 错误处理（连接失败、超时）

**优先级**: P0（核心功能）

#### 1.3 MetadataManager 测试 ✅
**文件**: `tests/test_meta_manager.py`

**测试用例**:
- ✅ ensure_meta_tables（创建缺失表）
- ✅ table_exists（表存在性检查）
- ✅ create_table（建表）
- ✅ list_tables（列出所有表）
- ✅ get_table_columns（获取表结构）
- ✅ drop_table（删除表）
- ✅ 任务版本管理（create/get/rollback）
- ✅ 错误处理（表不存在、重复创建）

**优先级**: P0（核心功能）

#### 1.4 DataOperations 测试 ✅
**文件**: `tests/test_data_operations.py`

**测试用例**:
- ✅ upsert（插入或更新）
- ✅ upsert_daily（日线数据专用）
- ✅ bulk_copy（批量写入）
- ✅ get_last_sync_date（获取最后同步日期）
- ✅ update_sync_log（更新同步日志）
- ✅ DataFrame 预处理（日期转换、列对齐）
- ✅ 错误处理（空 DataFrame、列不匹配）

**优先级**: P0（核心功能）

#### 1.5 SeedDataManager 测试 ✅
**文件**: `tests/test_seed_data.py`

**测试用例**:
- ✅ seed_sync_task_config（同步任务配置）
- ✅ seed_etl_task_config（ETL 任务配置）
- ✅ seed_factor_data_config（因子数据配置）
- ✅ seed_factor_metadata（因子元数据）
- ✅ 幂等性验证（重复执行不出错）
- ✅ 数据完整性验证

**优先级**: P1（重要功能）

### 阶段2: 集成测试

#### 2.1 DolphinDBClient 集成测试 ✅
**文件**: `tests/test_dolphindb_client_integration.py`

**测试用例**:
- ✅ 完整的 CRUD 流程
- ✅ 查询 → 处理 → 写入流程
- ✅ 元数据管理 → 数据操作流程
- ✅ 多组件协作测试
- ✅ 事务一致性测试

**优先级**: P0（核心功能）

#### 2.2 向后兼容性测试 ✅
**文件**: `test_dolphindb_refactor.py`（已存在）

**测试用例**:
- ✅ 模块结构测试
- ✅ 导入测试
- ✅ API 兼容性测试（24 个方法）

**状态**: 已通过（3/3）

#### 2.3 业务层回归测试 ✅
**文件**: 现有测试套件

**测试范围**:
- ✅ 因子计算流程（test_technical_factors.py）
- ✅ 数据服务（test_data_service.py）
- ✅ API 端点（test_generic_task_api.py）
- ✅ 分析器（test_analyzer.py）

**状态**: 需要运行完整测试套件

### 阶段3: 性能测试

#### 3.1 性能基准测试 ⏳
**文件**: `tests/test_performance_benchmark.py`

**测试用例**:
- ⏳ 查询性能（小/中/大数据量）
- ⏳ 写入性能（单条/批量）
- ⏳ 内存使用（连接池、DataFrame）
- ⏳ 并发性能（多线程查询）

**优先级**: P1（重要功能）

#### 3.2 性能对比测试 ⏳
**文件**: `tests/test_performance_comparison.py`

**测试用例**:
- ⏳ 重构前后性能对比
- ⏳ 响应时间对比
- ⏳ 内存占用对比
- ⏳ CPU 使用对比

**优先级**: P2（次要功能）

### 阶段4: 压力测试

#### 4.1 边界条件测试 ⏳
**文件**: `tests/test_edge_cases.py`

**测试用例**:
- ⏳ 空数据处理
- ⏳ 超大数据量（1000+ 股票，1年+ 数据）
- ⏳ 特殊字符处理
- ⏳ 极端值处理（NULL、NaN、Inf）

**优先级**: P1（重要功能）

#### 4.2 并发测试 ⏳
**文件**: `tests/test_concurrency.py`

**测试用例**:
- ⏳ 多线程并发查询
- ⏳ 多进程并发写入
- ⏳ 连接池竞争
- ⏳ 死锁检测

**优先级**: P2（次要功能）

---

## 测试策略

### 测试工具
- **pytest**: 单元测试和集成测试
- **pytest-cov**: 代码覆盖率
- **pytest-benchmark**: 性能基准测试
- **pytest-xdist**: 并行测试执行
- **pytest-timeout**: 超时控制

### 测试数据
- **测试数据库**: 使用独立的测试数据库（避免污染生产数据）
- **测试数据集**:
  - 小样本：10只股票，1个月数据
  - 中样本：100只股票，3个月数据
  - 大样本：1000只股票，1年数据

### 测试环境
- **Python**: 3.11.9
- **DolphinDB**: Docker 容器
- **虚拟环境**: `.venv`

---

## 测试执行计划

### 第1天: 基础设施层测试
- [x] 1.1 QueryBuilder 测试
- [x] 1.2 DolphinDBConnection 测试
- [x] 1.3 MetadataManager 测试
- [x] 1.4 DataOperations 测试
- [x] 1.5 SeedDataManager 测试

### 第2天: 集成测试和回归测试
- [x] 2.1 DolphinDBClient 集成测试
- [x] 2.2 向后兼容性测试
- [ ] 2.3 业务层回归测试（运行完整测试套件）

### 第3天: 性能测试和压力测试
- [ ] 3.1 性能基准测试
- [ ] 3.2 性能对比测试
- [ ] 4.1 边界条件测试
- [ ] 4.2 并发测试

### 第4天: 测试报告和问题修复
- [ ] 生成测试报告
- [ ] 问题分类和优先级排序
- [ ] 协助开发修复问题
- [ ] 回归验证

---

## 验收标准

### 功能验收
- ✅ 所有单元测试通过
- ✅ 所有集成测试通过
- ⏳ 所有回归测试通过
- ⏳ 向后兼容性 100%

### 质量验收
- ⏳ 代码覆盖率 > 80%
- ⏳ 无 P0 级别 bug
- ⏳ 无 P1 级别 bug（或已修复）
- ⏳ P2 级别 bug < 5 个

### 性能验收
- ⏳ 查询响应时间 ≤ 重构前
- ⏳ 写入性能 ≥ 重构前
- ⏳ 内存使用 ≤ 重构前 + 10%
- ⏳ 并发性能 ≥ 重构前

---

## 风险评估

### 高风险区域
1. **QueryBuilder 语法适配**: PostgreSQL → DolphinDB 转换可能遗漏边界情况
2. **连接管理**: 单例模式在多线程环境下的线程安全性
3. **数据类型转换**: Polars DataFrame ↔ DolphinDB 类型映射

### 缓解措施
1. 增加边界条件测试用例
2. 增加并发测试用例
3. 增加类型转换测试用例

---

## 测试报告

### 测试执行摘要
- **总测试用例**: TBD
- **通过**: TBD
- **失败**: TBD
- **跳过**: TBD
- **通过率**: TBD

### 代码覆盖率
- **总体覆盖率**: TBD
- **connection.py**: TBD
- **query_builder.py**: TBD
- **meta_manager.py**: TBD
- **seed_data.py**: TBD
- **data_operations.py**: TBD

### 发现的问题
- **P0 问题**: TBD
- **P1 问题**: TBD
- **P2 问题**: TBD

---

## 附录

### 测试命令

```bash
# 激活虚拟环境
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
source .venv/bin/activate

# 运行所有测试
pytest tests/ -v

# 运行特定测试文件
pytest tests/test_query_builder.py -v

# 运行测试并生成覆盖率报告
pytest tests/ --cov=store/dolphindb --cov-report=html --cov-report=term

# 运行性能基准测试
pytest tests/test_performance_benchmark.py --benchmark-only

# 运行并发测试
pytest tests/test_concurrency.py -n 4
```

### 相关文档
- `DOLPHINDB_REFACTOR_REPORT.md` - 重构报告
- `MIGRATION_GUIDE.md` - 迁移指南
- `PERFORMANCE_REPORT.md` - 性能报告
- `IMPLEMENTATION_SUMMARY.md` - 实现总结
