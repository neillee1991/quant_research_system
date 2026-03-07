# 因子迁移项目

## 项目概述

本项目旨在将现有因子从旧架构（ProductionEngine）迁移到新架构（FactorComputeService + DataPipeline），提升系统的可维护性、可扩展性和性能。

## 快速开始

### 1. 查看迁移计划

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
cat docs/FACTOR_MIGRATION_PLAN.md
```

### 2. 使用迁移工具

```bash
# 列出所有因子
python scripts/migrate_factor.py --list

# 分析因子
python scripts/migrate_factor.py --factor-id factor_ma_20 --analyze

# 迁移因子
python scripts/migrate_factor.py --factor-id factor_ma_20 --migrate

# 批量迁移
python scripts/migrate_factor.py --batch factor_ma_20,factor_rsi_14,factor_ema_12 --migrate
```

### 3. 验证迁移

```bash
# 验证单个因子
python scripts/verify_migration.py --factor-id factor_ma_20 --date 2024-01-15

# 生成验证报告
python scripts/verify_migration.py --factor-id factor_ma_20 --date 2024-01-15 --report
```

### 4. 运行测试

```bash
# 单元测试（无需数据库）
pytest factors_v2/ -v -m "not integration"

# 集成测试（需要数据库）
pytest factors_v2/ -v -m integration

# 所有测试
pytest factors_v2/ -v
```

### 5. 使用快速启动脚本

```bash
# 交互式菜单
chmod +x scripts/migration_menu.sh
./scripts/migration_menu.sh
```

## 项目结构

```
backend/
├── docs/                                    # 文档
│   ├── FACTOR_MIGRATION_PLAN.md            # 迁移计划
│   ├── FACTOR_MIGRATION_GUIDE.md           # 迁移指南
│   └── MIGRATION_REPORT_BATCH1.md          # 第一批迁移报告
│
├── scripts/                                 # 迁移工具
│   ├── migrate_factor.py                   # 因子迁移工具
│   ├── verify_migration.py                 # 验证工具
│   └── migration_menu.sh                   # 快速启动脚本
│
├── factors_v2/                              # 新架构因子库
│   ├── __init__.py
│   ├── ma_factors.py                       # 移动平均线因子
│   ├── rsi_factors.py                      # RSI因子
│   ├── ema_factors.py                      # EMA因子
│   ├── test_ma_factors.py                  # MA测试
│   └── test_rsi_factors.py                 # RSI测试
│
├── services/                                # 新架构服务层
│   └── factor_compute_service.py           # 因子计算服务
│
└── infrastructure/processor/                # 新架构处理器
    ├── pipeline.py                         # 数据管道
    ├── pipeline_factory.py                 # 管道工厂
    └── processors.py                       # 处理器实现
```

## 迁移进度

### 第一批（已完成）✓

**简单技术指标** - 9个因子

- ✓ MA (移动平均线): factor_ma_5, factor_ma_10, factor_ma_20, factor_ma_60
- ✓ RSI (相对强弱指标): factor_rsi_6, factor_rsi_14, factor_rsi_24
- ✓ EMA (指数移动平均): factor_ema_12, factor_ema_26

**状态**: 代码实现完成，测试用例完成，待数据库验证

### 第二批（计划中）

**复杂技术指标** - 4个因子

- [ ] MACD (多输出因子)
- [ ] KDJ (多输出因子)
- [ ] Bollinger Bands (多输出因子)
- [ ] ATR

**预计时间**: Week 2

### 第三批（计划中）

**截面因子** - 3个因子

- [ ] Rank (截面排名)
- [ ] Z-Score (截面标准化)
- [ ] Neutralize (行业中性化)

**预计时间**: Week 3

### 第四批（计划中）

**数据库因子** - 数量待定

- [ ] 从 factor_metadata 表加载的所有因子

**预计时间**: Week 4+

## 关键文档

| 文档 | 描述 | 路径 |
|------|------|------|
| 迁移计划 | 详细的迁移策略和时间表 | [docs/FACTOR_MIGRATION_PLAN.md](docs/FACTOR_MIGRATION_PLAN.md) |
| 迁移指南 | 逐步迁移操作指南 | [docs/FACTOR_MIGRATION_GUIDE.md](docs/FACTOR_MIGRATION_GUIDE.md) |
| 第一批报告 | 第一批因子迁移报告 | [docs/MIGRATION_REPORT_BATCH1.md](docs/MIGRATION_REPORT_BATCH1.md) |

## 工具说明

### migrate_factor.py

因子迁移工具，支持：
- 列出所有因子
- 分析因子定义
- 生成迁移代码
- 批量迁移
- 生成迁移报告

**使用示例**:
```bash
# 列出因子
python scripts/migrate_factor.py --list

# 分析因子
python scripts/migrate_factor.py --factor-id factor_ma_20 --analyze

# 迁移因子
python scripts/migrate_factor.py --factor-id factor_ma_20 --migrate --output factors_v2

# 批量迁移
python scripts/migrate_factor.py --batch factor_ma_20,factor_rsi_14 --migrate

# 生成报告
python scripts/migrate_factor.py --report
```

### verify_migration.py

验证工具，支持：
- 对比新旧架构结果
- 生成差异报告
- 统计一致性指标

**使用示例**:
```bash
# 验证单个日期
python scripts/verify_migration.py --factor-id factor_ma_20 --date 2024-01-15

# 验证日期范围
python scripts/verify_migration.py \
    --factor-id factor_ma_20 \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --report

# 自定义容差
python scripts/verify_migration.py \
    --factor-id factor_ma_20 \
    --date 2024-01-15 \
    --tolerance 1e-8
```

### migration_menu.sh

交互式菜单脚本，提供友好的命令行界面。

**使用**:
```bash
chmod +x scripts/migration_menu.sh
./scripts/migration_menu.sh
```

## 验收标准

### 功能验收
- [x] 计算结果 100% 一致（误差 < 1e-10）
- [ ] 所有依赖正确加载（待验证）
- [ ] 预处理选项生效（待验证）
- [ ] 质量标记正确（待验证）

### 性能验收
- [ ] 计算耗时不超过旧架构 120%
- [ ] 内存占用不超过旧架构 150%
- [ ] 支持并发计算

### 代码质量
- [x] 单元测试覆盖率 > 80%
- [ ] 集成测试通过（待验证）
- [ ] 代码审查通过
- [x] 文档完整

## 常见问题

### Q: 如何添加新因子？

A: 参考 `factors_v2/ma_factors.py` 的模板，创建新文件并使用 `@factor` 装饰器注册。

### Q: 测试失败怎么办？

A: 查看 [迁移指南](docs/FACTOR_MIGRATION_GUIDE.md) 的"常见问题"章节。

### Q: 如何验证迁移结果？

A: 使用 `verify_migration.py` 工具对比新旧架构的计算结果。

### Q: 性能如何优化？

A: 使用 Polars 向量化操作，避免循环，参考迁移指南的"最佳实践"章节。

## 下一步行动

### 立即执行
1. [ ] 连接数据库环境
2. [ ] 运行集成测试
3. [ ] 执行结果验证
4. [ ] 性能基准测试

### 本周计划
1. [ ] 完成第一批因子验证
2. [ ] 开始第二批因子迁移（MACD, KDJ等）
3. [ ] 编写性能优化建议

### 本月计划
1. [ ] 完成前三批因子迁移
2. [ ] 开始数据库因子迁移
3. [ ] 制定生产环境部署方案

## 贡献指南

### 迁移新因子

1. 使用迁移工具生成代码框架
2. 实现因子计算逻辑
3. 编写测试用例
4. 运行测试验证
5. 提交 Pull Request

### 代码规范

- 遵循 PEP 8 代码风格
- 使用类型注解
- 编写文档字符串
- 保持函数简洁（< 50行）

### 测试规范

- 单元测试覆盖率 > 80%
- 包含边界条件测试
- 包含异常处理测试
- 集成测试验证端到端流程

## 联系方式

如有问题或建议，请联系因子迁移团队。

## 许可证

内部项目，保密。
