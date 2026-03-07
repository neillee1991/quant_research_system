# 因子迁移计划

## 1. 项目概览

### 1.1 迁移目标
将现有因子从旧架构（ProductionEngine）迁移到新架构（FactorComputeService + DataPipeline）

### 1.2 架构对比

| 维度 | 旧架构 (ProductionEngine) | 新架构 (FactorComputeService) |
|------|--------------------------|------------------------------|
| 核心类 | ProductionEngine | FactorComputeService |
| 数据处理 | 单体方法（8步流程） | Pipeline模式（可组合处理器） |
| 预处理配置 | 硬编码 DEFAULT_PREPROCESS | 配置文件 + PreprocessLoader |
| 扩展性 | 低（修改需改核心代码） | 高（添加新Processor即可） |
| 测试性 | 低（难以单独测试各步骤） | 高（每个Processor独立测试） |
| 代码行数 | ~800行 | ~400行（服务层） + 处理器 |

### 1.3 功能一致性保证
新架构完全复刻旧架构的8步流程：
1. DataLoaderProcessor → _load_data()
2. AdjustmentProcessor → _apply_adjust()
3. StatusFilterProcessor → _apply_stock_status()
4. FactorComputeProcessor → definition.func()
5. SuspensionHandlerProcessor → _handle_suspension_from_status()
6. DateRangeFilterProcessor → 日期过滤
7. QualityCheckerProcessor → _build_quality_flag()
8. ResultWriterProcessor → _save_results()

## 2. 现有因子盘点

### 2.1 因子来源
- **数据库存储**: factor_metadata 表（动态编译的 code 字段）
- **Python文件**: engine/production/factors/ 目录（示例因子）
- **技术指标库**: engine/factors/technical.py（TechnicalFactors类）

### 2.2 因子分类

#### A. 技术指标类（简单）
- MA (Simple Moving Average)
- EMA (Exponential Moving Average)
- RSI (Relative Strength Index)
- MACD
- KDJ
- Bollinger Bands
- ATR

**特点**:
- 无外部依赖
- 纯向量化计算
- 易于验证

#### B. 截面因子类（中等）
- Rank (截面排名)
- Z-Score (截面标准化)
- Neutralize (行业中性化)

**特点**:
- 需要分组计算
- 依赖 trade_date 分组
- 需要行业数据

#### C. 数据库因子（复杂）
- 从 factor_metadata 表加载
- 动态编译执行
- 可能有复杂依赖

**特点**:
- 需要从DB恢复
- 可能依赖其他因子
- 需要验证编译结果

### 2.3 因子统计
- **技术指标**: 7个基础指标
- **截面因子**: 3个操作符
- **数据库因子**: 待查询（需连接DB）

## 3. 迁移策略

### 3.1 迁移原则
1. **渐进式迁移**: 逐批迁移，保持系统稳定
2. **双轨运行**: 新旧架构并存，逐步切换
3. **100%验证**: 每个因子迁移后必须验证结果一致
4. **零停机**: 不影响生产环境运行

### 3.2 迁移优先级

#### 第一批（简单技术指标）- Week 1
- MA (20日均线)
- RSI (14日RSI)
- EMA (指数移动平均)

**理由**:
- 无依赖
- 计算简单
- 易于验证
- 可快速建立信心

#### 第二批（复杂技术指标）- Week 2
- MACD
- KDJ
- Bollinger Bands
- ATR

**理由**:
- 多输出值
- 需要多列数据
- 验证复杂度中等

#### 第三批（截面因子）- Week 3
- Rank
- Z-Score
- Neutralize

**理由**:
- 需要分组计算
- 依赖行业数据
- 需要特殊处理

#### 第四批（数据库因子）- Week 4+
- 从 factor_metadata 加载的所有因子

**理由**:
- 数量未知
- 可能有复杂依赖
- 需要逐个分析

### 3.3 风险评估

| 风险 | 等级 | 缓解措施 |
|------|------|---------|
| 计算结果不一致 | 高 | 自动化对比验证脚本 |
| 性能下降 | 中 | 性能基准测试 |
| 依赖关系遗漏 | 中 | 依赖图分析工具 |
| 数据库因子编译失败 | 高 | 沙箱测试环境 |
| 生产环境影响 | 低 | 双轨运行，灰度切换 |

### 3.4 回滚方案
1. **代码回滚**: Git revert 到迁移前版本
2. **数据回滚**: 保留旧表数据，新表标记为 _v2
3. **配置回滚**: 通过环境变量切换新旧引擎
4. **监控告警**: 实时监控计算结果差异

## 4. 迁移工具

### 4.1 自动化工具
- `scripts/migrate_factor.py`: 因子定义转换工具
- `scripts/verify_migration.py`: 结果验证工具
- `scripts/benchmark_performance.py`: 性能对比工具
- `scripts/analyze_dependencies.py`: 依赖分析工具

### 4.2 工具功能

#### migrate_factor.py
```python
# 功能：
# 1. 从旧架构提取因子定义
# 2. 生成新架构配置文件
# 3. 创建测试用例
# 4. 生成迁移报告

# 使用：
python scripts/migrate_factor.py --factor-id factor_ma_20 --dry-run
python scripts/migrate_factor.py --factor-id factor_ma_20 --execute
```

#### verify_migration.py
```python
# 功能：
# 1. 对比新旧架构计算结果
# 2. 生成差异报告
# 3. 统计一致性指标
# 4. 可视化对比图表

# 使用：
python scripts/verify_migration.py --factor-id factor_ma_20 --date-range 2024-01-01:2024-12-31
```

#### benchmark_performance.py
```python
# 功能：
# 1. 测试计算性能
# 2. 对比新旧架构耗时
# 3. 生成性能报告

# 使用：
python scripts/benchmark_performance.py --factor-id factor_ma_20 --iterations 10
```

## 5. 迁移步骤（详细）

### 5.1 准备阶段
- [ ] 创建迁移分支 `feature/factor-migration`
- [ ] 备份生产数据库
- [ ] 部署测试环境
- [ ] 安装迁移工具

### 5.2 第一批迁移（MA, RSI, EMA）

#### Step 1: 分析因子定义
```bash
# 查看因子注册信息
python -c "from engine.production.registry import list_factors; print(list_factors())"

# 分析依赖关系
python scripts/analyze_dependencies.py --factor-id factor_ma_20
```

#### Step 2: 创建新架构因子
```bash
# 生成迁移代码
python scripts/migrate_factor.py --factor-id factor_ma_20 --output factors_v2/
```

#### Step 3: 运行验证
```bash
# 计算旧架构结果
python scripts/compute_old.py --factor-id factor_ma_20 --date 2024-01-01

# 计算新架构结果
python scripts/compute_new.py --factor-id factor_ma_20 --date 2024-01-01

# 对比结果
python scripts/verify_migration.py --factor-id factor_ma_20 --date 2024-01-01
```

#### Step 4: 性能测试
```bash
python scripts/benchmark_performance.py --factor-id factor_ma_20
```

#### Step 5: 更新文档
- 记录迁移结果
- 更新因子文档
- 标记迁移状态

### 5.3 后续批次
重复 5.2 步骤，针对不同批次的因子

## 6. 验收标准

### 6.1 功能验收
- [ ] 计算结果 100% 一致（误差 < 1e-10）
- [ ] 所有依赖正确加载
- [ ] 预处理选项生效
- [ ] 质量标记正确

### 6.2 性能验收
- [ ] 计算耗时不超过旧架构 120%
- [ ] 内存占用不超过旧架构 150%
- [ ] 支持并发计算

### 6.3 代码质量
- [ ] 单元测试覆盖率 > 80%
- [ ] 集成测试通过
- [ ] 代码审查通过
- [ ] 文档完整

## 7. 时间表

| 阶段 | 时间 | 交付物 |
|------|------|--------|
| 准备阶段 | Day 1-2 | 迁移工具、测试环境 |
| 第一批迁移 | Day 3-5 | MA, RSI, EMA 迁移完成 |
| 第二批迁移 | Day 6-8 | MACD, KDJ, BB, ATR 迁移完成 |
| 第三批迁移 | Day 9-11 | 截面因子迁移完成 |
| 第四批迁移 | Day 12-20 | 数据库因子迁移完成 |
| 验收测试 | Day 21-23 | 完整验收报告 |
| 生产部署 | Day 24-25 | 灰度发布、监控 |

## 8. 成功指标

### 8.1 技术指标
- 迁移成功率: 100%
- 结果一致性: 100%
- 性能下降: < 20%
- 测试覆盖率: > 80%

### 8.2 业务指标
- 零生产事故
- 零数据丢失
- 用户无感知切换

## 9. 后续优化

### 9.1 短期优化（1个月内）
- 优化 Pipeline 性能
- 添加更多 Processor
- 完善监控告警

### 9.2 长期优化（3个月内）
- 支持分布式计算
- 因子依赖自动解析
- 智能缓存机制
- 可视化 Pipeline 编排

## 10. 附录

### 10.1 关键文件路径
- 旧架构: `/Users/lisheng/Code/quantsystem/quant_research_system/backend/engine/production/engine.py`
- 新架构: `/Users/lisheng/Code/quantsystem/quant_research_system/backend/services/factor_compute_service.py`
- Pipeline: `/Users/lisheng/Code/quantsystem/quant_research_system/backend/infrastructure/processor/pipeline.py`
- 工厂: `/Users/lisheng/Code/quantsystem/quant_research_system/backend/infrastructure/processor/pipeline_factory.py`

### 10.2 参考文档
- [新架构设计文档](./ARCHITECTURE.md)
- [Pipeline 使用指南](./PIPELINE_GUIDE.md)
- [因子开发规范](./FACTOR_DEVELOPMENT.md)
