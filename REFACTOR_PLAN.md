# 项目重构执行计划

> 本文档记录完整的重构计划、进度和决策

## 决策记录

**日期**: 2026-03-14
**决策人**: 项目负责人
**执行人**: Claude Code

### 技术决策
- **DolphinDB客户端**: 保留 `/infrastructure/database/` (更模块化、可扩展)
- **因子引擎**: 保留 `FactorComputeService` (更解耦)
- **执行策略**: 保守更新,充分测试
- **预计工作量**: 2-3周

### 决策理由
保留 `/infrastructure/database/` 是因为:
1. 更好的模块化设计 (7个独立模块)
2. 更高的可扩展性 (门面模式 + 依赖注入)
3. 更清晰的职责分离 (连接、SQL适配、类型转换、表管理等)
4. 更适合长期维护和团队协作

---

## 阶段1: 安全修复 + 清理废弃代码 (1-2天)

### 1.1 安全修复

#### 🔴 CRITICAL: 代码执行端点
**文件**: `backend/app/api/v1/production/factor_compute.py`
**问题**: 无认证的代码执行端点
**修复方案**:
1. 临时方案: 添加环境变量开关,默认禁用
2. 长期方案: 实现 JWT 认证 + RBAC 授权

#### 🔴 CRITICAL: SQL查询端点
**文件**: `backend/app/api/v1/data/query_api.py`
**问题**: 无认证的SQL查询端点
**修复方案**:
1. 添加认证中间件
2. 添加速率限制
3. 增强SQL注入防护

#### 🔴 CRITICAL: 默认密码
**文件**: `backend/app/core/config.py`
**问题**: 默认密码 "123456"
**修复方案**:
1. 添加启动时密码强度检查
2. 生产环境强制要求设置密码

#### 🔴 HIGH: 性能瓶颈
**文件**: `backend/engine/production/engine.py:733-751`
**问题**: 循环中的数据库操作
**修复方案**:
1. 改为批量删除: `DELETE WHERE trade_date IN (...)`
2. 单次批量写入: `upsert(write_df)`
**预期收益**: 性能提升 10-50倍

### 1.2 删除废弃文件

#### 明确废弃的文件 (可以安全删除)
```bash
# DolphinDB客户端废弃文件
backend/store/dolphindb_client_new.py              # 仅作为重新导出,已被替代
backend/store/dolphindb/seed_data.py.backup        # 备份文件
backend/store/dolphindb/seed_data.py.backup2       # 备份文件

# API路由废弃文件 (已被拆分版本替代)
backend/app/api/v1/production.py                   # 1486行,已拆分到 production/ 目录
backend/app/api/v1/data_merged.py                  # 1613行,已拆分到 data/ 目录

# 临时脚本
backend/check_sync_logs.py                         # 临时调试脚本
backend/analyze_refactor.py                        # 一次性分析脚本

# 备份目录
backend/backups/cleanup-2026-03-08/                # 过时的备份 (18个文件)
```

**预期收益**: 删除约 5000 行废弃代码

### 1.3 清理编译缓存

```bash
# 清理 .pyc 文件和 __pycache__ 目录
find backend -type f -name "*.pyc" ! -path "*/.venv/*" -delete
find backend -type d -name "__pycache__" ! -path "*/.venv/*" -exec rm -rf {} +
```

**预期收益**: 清理 164 个 .pyc 文件和 28 个 __pycache__ 目录

### 1.4 移动临时脚本

```bash
# 移动到合适的位置
mv backend/health_check.py backend/scripts/health_check.py
mv backend/init_meta_tables.py backend/database/init_meta_tables.py
```

### 1.5 归档备份目录

```bash
# 压缩归档而不是直接删除
cd backend
tar -czf ../backups-2026-03-08.tar.gz backups/cleanup-2026-03-08/
rm -rf backups/cleanup-2026-03-08/
```

---

## 阶段2: 功能对比分析 (2-3天)

### 2.1 DolphinDB客户端功能对比

#### 需要对比的模块

**`/store/dolphindb/` (1888行)**:
- `connection.py` (200行) - 连接管理
- `query_builder.py` (300行) - 查询构建
- `meta_manager.py` (250行) - 元数据管理
- `seed_data.py` (500行) - 种子数据
- `data_operations.py` (400行) - 数据操作
- `__init__.py` (238行) - 门面模式

**`/infrastructure/database/` (2000行)**:
- `connection.py` - 连接管理
- `sql_adapter.py` - SQL适配
- `type_converter.py` - 类型转换
- `table_manager.py` - 表管理
- `data_operations.py` - 数据操作
- `metadata_manager.py` - 元数据管理
- `dolphindb_client.py` - 门面模式

#### 对比维度
1. **功能完整性**: 哪些功能只在一个实现中存在?
2. **API兼容性**: 接口是否兼容?
3. **性能差异**: 是否有性能优化?
4. **测试覆盖**: 哪个实现有更好的测试?

#### 输出文档
- `docs/DOLPHINDB_MIGRATION.md` - 详细的功能对比和迁移计划

### 2.2 因子引擎功能对比

#### 需要对比的实现

**`ProductionEngine` (engine/production/engine.py, 973行)**:
- 8步因子计算流程
- 数据加载和预处理
- 复权处理
- 状态过滤
- 质量标记
- 结果存储

**`FactorComputeService` (services/factor_compute_service.py, 385行)**:
- 重构后的因子计算流程
- 更好的解耦设计

#### 对比维度
1. **功能差异**: ProductionEngine 有哪些 FactorComputeService 没有的功能?
2. **性能差异**: 哪个实现更高效?
3. **可维护性**: 哪个更容易扩展?

#### 输出文档
- `docs/FACTOR_ENGINE_MIGRATION.md` - 详细的功能对比和迁移计划

### 2.3 引用分析

使用工具分析代码引用:

```bash
# 分析 store.dolphindb 的引用
grep -r "from store.dolphindb" backend/ --include="*.py" > refs_store_dolphindb.txt
grep -r "import store.dolphindb" backend/ --include="*.py" >> refs_store_dolphindb.txt

# 分析 ProductionEngine 的引用
grep -r "ProductionEngine" backend/ --include="*.py" > refs_production_engine.txt

# 统计引用数量
wc -l refs_*.txt
```

#### 输出文档
- `docs/REFERENCE_ANALYSIS.md` - 引用分析报告

---

## 阶段3: 迁移 DolphinDB 客户端 (1周)

### 3.1 迁移独有功能

#### 步骤
1. 识别 `/store/dolphindb/` 中的独有功能
2. 将独有功能迁移到 `/infrastructure/database/`
3. 为每个迁移的功能编写单元测试
4. 运行集成测试验证

#### 迁移清单
- [ ] 功能1: (待识别)
- [ ] 功能2: (待识别)
- [ ] ...

### 3.2 更新引用

#### 步骤
1. 创建兼容层 (临时)
2. 逐个文件更新引用
3. 每次更新后运行测试
4. 确认无问题后删除兼容层

#### 引用更新清单
- [ ] 文件1: (待识别)
- [ ] 文件2: (待识别)
- [ ] ...

### 3.3 删除旧实现

#### 步骤
1. 确认所有引用已更新
2. 运行完整测试套件
3. 删除 `/store/dolphindb/` 目录
4. 更新文档

---

## 阶段4: 迁移因子引擎 (1周)

### 4.1 迁移独有功能

#### 步骤
1. 识别 `ProductionEngine` 中的独有功能
2. 将独有功能迁移到 `FactorComputeService`
3. 为每个迁移的功能编写单元测试
4. 运行集成测试验证

#### 迁移清单
- [ ] 功能1: (待识别)
- [ ] 功能2: (待识别)
- [ ] ...

### 4.2 更新引用

#### 步骤
1. 创建兼容层 (临时)
2. 逐个文件更新引用
3. 每次更新后运行测试
4. 确认无问题后删除兼容层

#### 引用更新清单
- [ ] 文件1: (待识别)
- [ ] 文件2: (待识别)
- [ ] ...

### 4.3 废弃旧实现

#### 步骤
1. 标记 `ProductionEngine` 为废弃
2. 添加 DeprecationWarning
3. 更新文档说明迁移路径
4. 在下一个版本中删除

---

## 阶段5: 重组测试和文档 (3-5天)

### 5.1 重组测试目录

#### 当前状态
- 22 个测试文件平铺在 `tests/` 根目录

#### 目标结构
```
tests/
├── unit/                    # 单元测试
│   ├── test_dolphindb_client.py
│   ├── test_factor_compute_service.py
│   ├── test_technical_factors.py
│   └── ...
├── integration/             # 集成测试
│   ├── test_factor_pipeline.py
│   ├── test_data_sync.py
│   └── ...
├── api/                     # API 测试
│   ├── test_factor_api.py
│   ├── test_data_api.py
│   └── ...
├── performance/             # 性能测试
│   └── test_partition_performance.py
├── conftest.py             # 共享 fixtures
└── README.md               # 测试指南
```

#### 迁移清单
- [ ] 识别每个测试文件的类型
- [ ] 移动到对应目录
- [ ] 更新 import 路径
- [ ] 运行测试验证

### 5.2 合并冗余文档

#### 当前状态
- 13 个文档文件,总计 7066 行,内容高度重叠

#### 目标结构
```
docs/
├── README.md                # 项目概览
├── ARCHITECTURE.md          # 架构设计
├── API_REFERENCE.md         # API 文档
├── DEVELOPER_GUIDE.md       # 开发指南
├── OPERATIONS.md            # 运维手册
└── CHANGELOG.md             # 变更日志
```

#### 合并计划
- [ ] 合并 COMPREHENSIVE_GUIDE.md → DEVELOPER_GUIDE.md
- [ ] 合并 PIPELINE_* 系列 → ARCHITECTURE.md
- [ ] 合并 PERFORMANCE_* 系列 → OPERATIONS.md
- [ ] 删除冗余文档

### 5.3 添加测试依赖

创建 `requirements-test.txt`:
```
pytest>=8.0.0
pytest-cov>=4.1.0
pytest-asyncio>=0.23.0
pytest-mock>=3.12.0
pytest-xdist>=3.5.0
```

### 5.4 配置测试工具

创建 `pytest.ini` 和 `.coveragerc`

---

## 测试策略

### 单元测试
- 每个迁移的功能必须有单元测试
- 测试覆盖率 ≥ 80%

### 集成测试
- 测试关键业务流程
- 数据同步、因子计算、API 调用

### 回归测试
- 每次迁移后运行完整测试套件
- 确保没有破坏现有功能

### 性能测试
- 验证性能优化效果
- 确保没有性能退化

---

## 风险管理

### 回滚计划
1. 每个阶段完成后创建 Git 分支
2. 如果出现问题,可以快速回滚
3. 保留旧代码至少 30 天

### 监控指标
- 测试覆盖率
- 测试通过率
- 性能指标
- 错误日志

### 沟通机制
- 每个阶段完成后汇报进度
- 遇到问题及时沟通
- 重大决策需要确认

---

## 进度跟踪

### 阶段1: 安全修复 + 清理废弃代码
- [ ] 1.1 安全修复
- [ ] 1.2 删除废弃文件
- [ ] 1.3 清理编译缓存
- [ ] 1.4 移动临时脚本
- [ ] 1.5 归档备份目录

### 阶段2: 功能对比分析
- [ ] 2.1 DolphinDB客户端功能对比
- [ ] 2.2 因子引擎功能对比
- [ ] 2.3 引用分析

### 阶段3: 迁移 DolphinDB 客户端
- [ ] 3.1 迁移独有功能
- [ ] 3.2 更新引用
- [ ] 3.3 删除旧实现

### 阶段4: 迁移因子引擎
- [ ] 4.1 迁移独有功能
- [ ] 4.2 更新引用
- [ ] 4.3 废弃旧实现

### 阶段5: 重组测试和文档
- [ ] 5.1 重组测试目录
- [ ] 5.2 合并冗余文档
- [ ] 5.3 添加测试依赖
- [ ] 5.4 配置测试工具

---

## 完成标准

### 代码质量
- [ ] 所有测试通过
- [ ] 测试覆盖率 ≥ 80%
- [ ] 代码通过 black, isort, flake8, mypy 检查
- [ ] 没有 TODO/FIXME 注释

### 文档
- [ ] 所有文档已更新
- [ ] API 文档与代码一致
- [ ] 迁移指南完整

### 性能
- [ ] 性能测试通过
- [ ] 关键操作性能提升 10-50倍
- [ ] 没有性能退化

### 安全
- [ ] 所有安全问题已修复
- [ ] 通过安全扫描
- [ ] 敏感端点已添加认证

---

## 附录

### 相关文档
- [PROJECT_STANDARDS.md](PROJECT_STANDARDS.md) - 项目标准
- [CLAUDE.md](CLAUDE.md) - 项目概览
- 安全审查报告 - `/private/tmp/claude-501/.../a99723497b8231764.output`
- 架构审查报告 - `/private/tmp/claude-501/.../ac932d8a61a7f3f64.output`
- 代码质量审查报告 - `/private/tmp/claude-501/.../a2c3959aa54e5921f.output`
- 测试文档审查报告 - `/private/tmp/claude-501/.../a1fb85b8a00e03a37.output`

### 工具和命令
```bash
# 运行测试
pytest tests/ --cov=. --cov-report=term-missing

# 代码格式化
black backend/
isort backend/

# 代码检查
flake8 backend/
mypy backend/

# 安全扫描
pip-audit -r requirements.txt

# 性能测试
python scripts/test_partition_performance.py
```
