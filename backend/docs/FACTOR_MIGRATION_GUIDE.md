# 因子迁移指南

## 概述

本指南详细说明如何将因子从旧架构（ProductionEngine）迁移到新架构（FactorComputeService + DataPipeline）。

## 前置条件

- 已部署测试环境
- 已备份生产数据库
- 已安装迁移工具
- 熟悉新架构设计

## 迁移流程

### 1. 分析因子

使用迁移工具分析因子定义：

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 列出所有因子
python scripts/migrate_factor.py --list

# 分析单个因子
python scripts/migrate_factor.py --factor-id factor_ma_20 --analyze
```

输出示例：
```
因子分析: factor_ma_20

  描述: 20日移动平均线
  类别: technical
  复杂度: simple
  计算模式: incremental
  依赖: sync_daily_data
  参数: {'window': 20}
  存储: factor_values
```

### 2. 评估复杂度

根据分析结果评估迁移难度：

| 复杂度 | 特征 | 迁移难度 | 预计时间 |
|--------|------|---------|---------|
| simple | 单一依赖，无参数或简单参数 | 低 | 1-2小时 |
| medium | 多个依赖，复杂参数 | 中 | 2-4小时 |
| complex | 依赖其他因子，自定义存储 | 高 | 4-8小时 |

### 3. 生成迁移代码

```bash
# 生成单个因子代码
python scripts/migrate_factor.py --factor-id factor_ma_20 --migrate --output factors_v2

# 批量生成
python scripts/migrate_factor.py --batch factor_ma_20,factor_rsi_14,factor_ema_12 --migrate
```

生成的文件：
- `factors_v2/factor_ma_20.py` - 因子定义
- `factors_v2/test_factor_ma_20.py` - 测试用例

### 4. 实现因子逻辑

编辑生成的因子文件，实现计算逻辑：

```python
# factors_v2/factor_ma_20.py

import polars as pl
from engine.production.registry import factor
from engine.factors.technical import TechnicalFactors


@factor(
    factor_id="factor_ma_20",
    description="20日移动平均线",
    depends_on=["sync_daily_data"],
    category="technical",
    params={"window": 20},
    compute_mode="incremental",
)
def compute_ma_20(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    计算20日移动平均线

    Args:
        df: 包含 ts_code, trade_date, close 的 DataFrame
        params: 参数字典，包含 window

    Returns:
        包含 ts_code, trade_date, factor_value 的 DataFrame
    """
    window = params.get("window", 20)

    # 按股票分组计算移动平均
    result = df.sort(["ts_code", "trade_date"]).with_columns([
        TechnicalFactors.sma(pl.col("close"), window)
        .over("ts_code")
        .alias("factor_value")
    ])

    # 返回必需列
    return result.select(["ts_code", "trade_date", "factor_value"])
```

### 5. 编写测试用例

完善生成的测试文件：

```python
# factors_v2/test_factor_ma_20.py

import pytest
import polars as pl
from datetime import datetime


class TestFactorMa20:
    """测试 factor_ma_20"""

    def test_basic_calculation(self):
        """测试基本计算逻辑"""
        # 构造测试数据
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 30,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 31)],
            "close": list(range(100, 130))
        })

        # 执行计算
        from factors_v2.factor_ma_20 import compute_ma_20
        result = compute_ma_20(test_data, {"window": 20})

        # 验证结果
        assert not result.is_empty()
        assert "factor_value" in result.columns
        assert len(result) == 30

        # 验证第20个值（前19个值的平均）
        expected_ma20 = sum(range(100, 120)) / 20
        actual_ma20 = result.filter(pl.col("trade_date") == "2024-01-20")["factor_value"][0]
        assert abs(actual_ma20 - expected_ma20) < 1e-6

    def test_result_consistency(self, db_client):
        """测试与旧架构结果一致性"""
        from engine.production.engine import ProductionEngine
        from services.factor_compute_service import FactorComputeService

        old_engine = ProductionEngine(db_client)
        new_service = FactorComputeService(db_client)

        factor_id = "factor_ma_20"
        test_date = "2024-01-15"

        # 旧架构计算
        old_engine.run_task(factor_id=factor_id, target_date=test_date)

        # 新架构计算
        new_result = new_service.compute_factor(
            factor_id=factor_id,
            target_date=test_date,
            save_results=False
        )

        assert new_result.success

        # 加载并对比结果
        old_data = self._load_factor_data(db_client, factor_id, test_date)
        new_data = new_result.context.dataframe

        # 验证一致性
        assert len(old_data) == len(new_data)

        merged = old_data.join(new_data, on=["ts_code", "trade_date"], suffix="_new")
        max_diff = (merged["factor_value"] - merged["factor_value_new"]).abs().max()
        assert max_diff < 1e-10
```

### 6. 运行测试

```bash
# 运行单元测试
pytest factors_v2/test_factor_ma_20.py -v

# 运行集成测试（需要数据库）
pytest factors_v2/test_factor_ma_20.py::TestFactorMa20::test_result_consistency -v
```

### 7. 验证迁移

使用验证工具对比新旧架构结果：

```bash
# 验证单个日期
python scripts/verify_migration.py \
    --factor-id factor_ma_20 \
    --date 2024-01-15 \
    --tolerance 1e-10

# 验证日期范围
python scripts/verify_migration.py \
    --factor-id factor_ma_20 \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --report
```

验证通过标准：
- ✓ 行数完全一致
- ✓ 最大误差 < 1e-10
- ✓ 一致率 = 100%

### 8. 性能测试

```bash
# 对比性能
python scripts/benchmark_performance.py \
    --factor-id factor_ma_20 \
    --iterations 10
```

性能要求：
- 新架构耗时 ≤ 旧架构 × 1.2
- 内存占用 ≤ 旧架构 × 1.5

### 9. 更新文档

在迁移报告中记录：
- 迁移日期
- 验证结果
- 性能对比
- 遇到的问题及解决方案

### 10. 代码审查

提交 Pull Request，包含：
- 因子代码
- 测试用例
- 验证报告
- 性能报告

## 常见问题 FAQ

### Q1: 如何处理依赖其他因子的情况？

**A**: 确保被依赖的因子先迁移，然后在新因子中通过 `depends_on` 声明依赖：

```python
@factor(
    factor_id="factor_momentum",
    depends_on=["factor_ma_20", "factor_ma_60"],  # 依赖其他因子
    ...
)
def compute_momentum(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    # df 会自动包含 ma_20 和 ma_60 列
    result = df.with_columns([
        (pl.col("ma_20") / pl.col("ma_60") - 1).alias("factor_value")
    ])
    return result
```

### Q2: 如何处理多输出因子？

**A**: 使用自定义存储表：

```python
@factor(
    factor_id="factor_macd",
    storage={
        "target": "factor_macd_values",
        "columns": {
            "ts_code": "SYMBOL",
            "trade_date": "DATE",
            "macd": "DOUBLE",
            "signal": "DOUBLE",
            "histogram": "DOUBLE"
        },
        "primary_keys": ["ts_code", "trade_date"]
    }
)
def compute_macd(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    # 返回多列
    return df.select(["ts_code", "trade_date", "macd", "signal", "histogram"])
```

### Q3: 如何处理预处理选项？

**A**: 在因子参数中指定：

```python
@factor(
    factor_id="factor_custom",
    params={
        "window": 20,
        "preprocess": {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
            "handle_suspension": True
        }
    }
)
```

或使用预处理配置文件：

```python
# 在计算时指定
new_service.compute_factor(
    factor_id="factor_custom",
    preprocess_profile="conservative"  # 使用预定义配置
)
```

### Q4: 验证失败怎么办？

**A**: 按以下步骤排查：

1. **检查数据加载**
   ```python
   # 打印加载的数据
   print(df.head())
   print(df.describe())
   ```

2. **检查计算逻辑**
   ```python
   # 逐步验证中间结果
   step1 = df.with_columns(...)
   print(step1.head())
   ```

3. **检查预处理选项**
   ```python
   # 确保新旧架构使用相同的预处理
   preprocess = {
       "adjust_price": "forward",
       "filter_st": True,
       ...
   }
   ```

4. **检查日期范围**
   ```python
   # 确保加载了足够的历史数据
   lookback_days = 60  # 对于20日均线，至少需要20天
   ```

### Q5: 如何处理停牌数据？

**A**: 新架构自动处理，通过 `SuspensionHandlerProcessor`：

```python
# 停牌期间的因子值会被置为 null
# 复牌后重新计算

# 如果需要自定义处理，可以在因子函数中：
def compute_custom(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    result = df.with_columns([
        pl.when(pl.col("is_suspend"))
        .then(None)  # 停牌时置空
        .otherwise(pl.col("close").rolling_mean(20))
        .alias("factor_value")
    ])
    return result
```

### Q6: 如何优化性能？

**A**: 使用 Polars 向量化操作：

```python
# ❌ 慢：逐行计算
for row in df.iter_rows():
    value = calculate(row)

# ✓ 快：向量化计算
result = df.with_columns([
    pl.col("close").rolling_mean(20).alias("ma_20")
])

# ✓ 更快：使用 lazy evaluation
result = (
    df.lazy()
    .with_columns([...])
    .filter(...)
    .collect()
)
```

### Q7: 如何调试 Pipeline？

**A**: 启用详细日志：

```python
import logging
logging.getLogger("infrastructure.processor").setLevel(logging.DEBUG)

# 或在计算时查看中间结果
context = ProcessContext(...)
pipeline = factory.create_factor_pipeline(...)

# 逐阶段执行
for stage in pipeline.get_stages():
    print(f"Stage: {stage.name}")
    df = stage.process(df, context)
    print(df.head())
```

## 迁移检查清单

### 迁移前
- [ ] 分析因子定义和依赖
- [ ] 评估复杂度和风险
- [ ] 准备测试数据
- [ ] 备份相关数据

### 迁移中
- [ ] 生成迁移代码
- [ ] 实现因子逻辑
- [ ] 编写测试用例
- [ ] 运行单元测试
- [ ] 运行集成测试

### 迁移后
- [ ] 验证结果一致性（100%）
- [ ] 性能测试（≤ 120%）
- [ ] 更新文档
- [ ] 代码审查
- [ ] 合并到主分支

## 示例：完整迁移流程

以 `factor_ma_20` 为例：

```bash
# 1. 分析
python scripts/migrate_factor.py --factor-id factor_ma_20 --analyze

# 2. 生成代码
python scripts/migrate_factor.py --factor-id factor_ma_20 --migrate

# 3. 编辑代码（手动）
vim factors_v2/factor_ma_20.py

# 4. 运行测试
pytest factors_v2/test_factor_ma_20.py -v

# 5. 验证结果
python scripts/verify_migration.py \
    --factor-id factor_ma_20 \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --report

# 6. 性能测试
python scripts/benchmark_performance.py --factor-id factor_ma_20

# 7. 提交代码
git add factors_v2/factor_ma_20.py factors_v2/test_factor_ma_20.py
git commit -m "feat: migrate factor_ma_20 to new architecture"
git push origin feature/factor-migration
```

## 最佳实践

1. **小步快跑**: 每次迁移1-3个因子，及时验证
2. **自动化优先**: 使用工具生成代码，减少人工错误
3. **测试驱动**: 先写测试，再实现逻辑
4. **文档同步**: 迁移的同时更新文档
5. **代码审查**: 每个因子都要经过审查
6. **监控告警**: 生产环境部署后持续监控

## 参考资料

- [新架构设计文档](./ARCHITECTURE.md)
- [Pipeline 使用指南](./PIPELINE_GUIDE.md)
- [因子开发规范](./FACTOR_DEVELOPMENT.md)
- [迁移计划](./FACTOR_MIGRATION_PLAN.md)
