# 第一批因子迁移报告

## 执行概览

- **迁移日期**: 2026-03-07
- **迁移批次**: 第一批（简单技术指标）
- **迁移因子数**: 9个
- **迁移状态**: 已完成代码实现，待验证

## 迁移因子列表

### 1. 移动平均线 (MA)

| 因子ID | 描述 | 窗口期 | 文件 | 状态 |
|--------|------|--------|------|------|
| factor_ma_5 | 5日移动平均线 | 5 | factors_v2/ma_factors.py | ✓ 已迁移 |
| factor_ma_10 | 10日移动平均线 | 10 | factors_v2/ma_factors.py | ✓ 已迁移 |
| factor_ma_20 | 20日移动平均线 | 20 | factors_v2/ma_factors.py | ✓ 已迁移 |
| factor_ma_60 | 60日移动平均线 | 60 | factors_v2/ma_factors.py | ✓ 已迁移 |

**技术细节**:
- 使用 `TechnicalFactors.sma()` 进行向量化计算
- 按 `ts_code` 分组，避免跨股票计算
- 支持前复权价格
- 自动过滤ST股票和新股

### 2. 相对强弱指标 (RSI)

| 因子ID | 描述 | 窗口期 | 文件 | 状态 |
|--------|------|--------|------|------|
| factor_rsi_6 | 6日RSI | 6 | factors_v2/rsi_factors.py | ✓ 已迁移 |
| factor_rsi_14 | 14日RSI | 14 | factors_v2/rsi_factors.py | ✓ 已迁移 |
| factor_rsi_24 | 24日RSI | 24 | factors_v2/rsi_factors.py | ✓ 已迁移 |

**技术细节**:
- 使用 `TechnicalFactors.rsi()` 计算
- 基于EWM（指数加权移动平均）
- 输出范围: 0-100
- 处理除零异常（添加 epsilon）

### 3. 指数移动平均线 (EMA)

| 因子ID | 描述 | 窗口期 | 文件 | 状态 |
|--------|------|--------|------|------|
| factor_ema_12 | 12日EMA | 12 | factors_v2/ema_factors.py | ✓ 已迁移 |
| factor_ema_26 | 26日EMA | 26 | factors_v2/ema_factors.py | ✓ 已迁移 |

**技术细节**:
- 使用 `TechnicalFactors.ema()` 计算
- 基于 Polars 的 `ewm_mean()`
- 参数 `adjust=False` 保持一致性

## 迁移方法

### 代码结构

```
factors_v2/
├── __init__.py              # 包初始化
├── ma_factors.py            # 移动平均线因子
├── rsi_factors.py           # RSI因子
├── ema_factors.py           # EMA因子
├── test_ma_factors.py       # MA测试用例
└── test_rsi_factors.py      # RSI测试用例
```

### 因子模板

所有因子遵循统一模板：

```python
@factor(
    factor_id="factor_xxx",
    description="因子描述",
    depends_on=["sync_daily_data"],
    category="technical",
    params={
        "window": N,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    },
    compute_mode="incremental",
)
def compute_xxx(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    window = params.get("window", N)
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.xxx(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])
```

### 预处理配置

所有因子使用统一的预处理选项：

```python
"preprocess": {
    "adjust_price": "forward",      # 前复权
    "filter_st": True,              # 过滤ST股票
    "filter_new_stock": True,       # 过滤新股（上市<60天）
}
```

## 测试覆盖

### 单元测试

每个因子包含以下测试：

1. **基本计算测试**: 验证计算逻辑正确性
2. **多股票测试**: 验证分组计算
3. **空值处理测试**: 验证异常情况
4. **输出列测试**: 验证返回格式

### 集成测试

需要数据库连接的测试（标记为 `@pytest.mark.integration`）：

1. **新服务测试**: 验证新架构能正常计算
2. **一致性测试**: 对比新旧架构结果（标记为 `@pytest.mark.slow`）
3. **性能测试**: 验证计算耗时

### 运行测试

```bash
# 运行单元测试（无需数据库）
pytest factors_v2/test_ma_factors.py -v -m "not integration"
pytest factors_v2/test_rsi_factors.py -v -m "not integration"

# 运行集成测试（需要数据库）
pytest factors_v2/test_ma_factors.py -v -m integration

# 运行所有测试
pytest factors_v2/ -v
```

## 验证计划

### 验证步骤

1. **单元测试验证**
   ```bash
   pytest factors_v2/ -v -m "not integration"
   ```
   预期: 所有测试通过

2. **集成测试验证**（需要数据库环境）
   ```bash
   pytest factors_v2/ -v -m integration
   ```
   预期: 新服务能正常计算

3. **结果一致性验证**
   ```bash
   python scripts/verify_migration.py --factor-id factor_ma_20 --date 2024-01-15
   python scripts/verify_migration.py --factor-id factor_rsi_14 --date 2024-01-15
   python scripts/verify_migration.py --factor-id factor_ema_12 --date 2024-01-15
   ```
   预期: 最大误差 < 1e-10，一致率 = 100%

4. **性能验证**
   ```bash
   python scripts/benchmark_performance.py --factor-id factor_ma_20
   ```
   预期: 新架构耗时 ≤ 旧架构 × 1.2

### 验证标准

| 指标 | 标准 | 说明 |
|------|------|------|
| 行数一致性 | 100% | 新旧架构输出行数完全一致 |
| 值一致性 | 最大误差 < 1e-10 | 浮点数计算误差在可接受范围内 |
| 一致率 | 100% | 所有值都在误差范围内 |
| 性能 | ≤ 120% | 新架构耗时不超过旧架构的1.2倍 |
| 测试覆盖率 | > 80% | 代码测试覆盖率 |

## 技术亮点

### 1. 向量化计算

使用 Polars 的向量化操作，避免循环：

```python
# ❌ 慢：逐行计算
for row in df.iter_rows():
    ma = calculate_ma(row)

# ✓ 快：向量化计算
result = df.with_columns([
    pl.col("close").rolling_mean(20).over("ts_code").alias("ma_20")
])
```

### 2. 分组计算

使用 `.over("ts_code")` 确保按股票分组：

```python
result = df.sort(["ts_code", "trade_date"]).with_columns([
    TechnicalFactors.sma(pl.col("close"), window)
    .over("ts_code")  # 关键：按股票分组
    .alias("factor_value")
])
```

### 3. 统一接口

所有因子遵循统一的输入输出接口：

- **输入**: `pl.DataFrame` 包含 `ts_code`, `trade_date`, `close` 等列
- **输出**: `pl.DataFrame` 包含 `ts_code`, `trade_date`, `factor_value`

### 4. 可配置预处理

通过 `params.preprocess` 配置预处理选项，无需修改代码。

## 遇到的问题及解决方案

### 问题1: 跨股票计算

**问题**: 初始实现未使用 `.over("ts_code")`，导致跨股票计算错误。

**解决**: 在所有滚动计算中添加 `.over("ts_code")`。

### 问题2: 列名冲突

**问题**: 输入数据包含额外列，可能导致输出列过多。

**解决**: 使用 `.select(["ts_code", "trade_date", "factor_value"])` 明确返回列。

### 问题3: 空值处理

**问题**: RSI计算中可能出现除零错误。

**解决**: 在 `TechnicalFactors.rsi()` 中添加 epsilon（1e-10）。

## 下一步计划

### 第二批迁移（Week 2）

迁移复杂技术指标：

- [ ] MACD (多输出因子)
- [ ] KDJ (多输出因子)
- [ ] Bollinger Bands (多输出因子)
- [ ] ATR

**挑战**:
- 需要处理多输出值
- 需要自定义存储表
- 需要多列数据（high, low, close）

### 第三批迁移（Week 3）

迁移截面因子：

- [ ] Rank (截面排名)
- [ ] Z-Score (截面标准化)
- [ ] Neutralize (行业中性化)

**挑战**:
- 需要分组计算（按 trade_date）
- 需要行业数据
- 需要特殊的预处理

### 第四批迁移（Week 4+）

迁移数据库因子：

- [ ] 从 factor_metadata 表加载所有因子
- [ ] 动态编译执行
- [ ] 处理复杂依赖

**挑战**:
- 数量未知
- 可能有复杂依赖关系
- 需要逐个分析和测试

## 总结

### 完成情况

- ✓ 迁移工具开发完成
- ✓ 迁移计划制定完成
- ✓ 迁移指南编写完成
- ✓ 第一批因子代码实现完成
- ✓ 测试用例编写完成
- ⏳ 待验证（需要数据库环境）

### 关键成果

1. **迁移工具**: `migrate_factor.py`, `verify_migration.py`
2. **文档**: 迁移计划、迁移指南
3. **因子代码**: 9个技术指标因子
4. **测试用例**: 完整的单元测试和集成测试

### 经验总结

1. **自动化优先**: 使用工具生成代码，减少人工错误
2. **测试驱动**: 先写测试，再实现逻辑
3. **渐进式迁移**: 从简单到复杂，逐步推进
4. **文档同步**: 迁移的同时更新文档

### 风险提示

1. **需要数据库验证**: 当前只完成代码实现，需要连接数据库进行验证
2. **性能待测试**: 需要实际运行性能测试
3. **生产环境部署**: 需要制定详细的部署方案

## 附录

### 文件清单

```
backend/
├── docs/
│   ├── FACTOR_MIGRATION_PLAN.md          # 迁移计划
│   ├── FACTOR_MIGRATION_GUIDE.md         # 迁移指南
│   └── MIGRATION_REPORT_BATCH1.md        # 本报告
├── scripts/
│   ├── migrate_factor.py                 # 迁移工具
│   └── verify_migration.py               # 验证工具
└── factors_v2/
    ├── __init__.py
    ├── ma_factors.py                     # MA因子（4个）
    ├── rsi_factors.py                    # RSI因子（3个）
    ├── ema_factors.py                    # EMA因子（2个）
    ├── test_ma_factors.py                # MA测试
    └── test_rsi_factors.py               # RSI测试
```

### 代码统计

- 因子代码: ~300 行
- 测试代码: ~400 行
- 工具代码: ~600 行
- 文档: ~1500 行

### 联系方式

如有问题，请联系因子迁移团队。
