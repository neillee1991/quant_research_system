# DataPipeline 快速开始

## 基本使用

### 1. 使用 FactorComputeService

最简单的方式是使用 `FactorComputeService`：

```python
from services.factor_compute_service import FactorComputeService
from store.dolphindb_client import DolphinDBClient

# 初始化服务
db_client = DolphinDBClient.get_instance()
service = FactorComputeService(db_client)

# 计算因子（使用默认配置）
result = service.compute_factor(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131"
)

# 检查结果
if result.success:
    print(f"✓ 计算成功")
    print(f"  行数: {result.rows}")
    print(f"  耗时: {result.elapsed_seconds:.1f}s")
    print(f"  质量: {result.quality_metrics}")
else:
    print(f"✗ 计算失败: {result.message}")
```

### 2. 使用预处理配置

```python
# 使用保守配置（更严格的过滤）
result = service.compute_factor(
    factor_id="momentum_20",
    preprocess_profile="conservative"
)

# 使用激进配置（最少过滤）
result = service.compute_factor(
    factor_id="momentum_20",
    preprocess_profile="aggressive"
)

# 混合使用：基于配置 + 自定义选项
result = service.compute_factor(
    factor_id="momentum_20",
    preprocess_profile="default",
    preprocess={
        "new_stock_days": 90,  # 覆盖默认的 60 天
        "filter_st": False      # 不过滤 ST
    }
)
```

### 3. 自定义预处理选项

```python
result = service.compute_factor(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131",
    preprocess={
        "adjust_price": "forward",      # 前复权
        "filter_st": True,              # 过滤 ST
        "filter_new_stock": True,       # 过滤新股
        "new_stock_days": 60,           # 新股上市 60 天内过滤
        "handle_suspension": True,      # 处理停牌
        "mark_limit": True              # 标记涨跌停
    }
)
```

## 高级用法

### 1. 直接使用 Pipeline

如果需要更细粒度的控制，可以直接使用 Pipeline：

```python
from infrastructure.processor import (
    DataPipeline,
    ProcessContext,
    DataLoaderProcessor,
    AdjustmentProcessor,
    FactorComputeProcessor,
    QualityCheckerProcessor,
)
from engine.production.registry import get_factor

# 获取因子定义
definition = get_factor("momentum_20")

# 创建管道
pipeline = DataPipeline(name="CustomPipeline")
pipeline.add_stage(DataLoaderProcessor(db_client, data_config))
pipeline.add_stage(AdjustmentProcessor(db_client))
pipeline.add_stage(FactorComputeProcessor())
pipeline.add_stage(QualityCheckerProcessor())

# 创建上下文
context = ProcessContext(
    factor_id="momentum_20",
    factor_definition=definition,
    calc_start="20240101",
    calc_end="20240131",
    data_start="20231201",
    preprocess_options={"adjust_price": "forward"},
    dataframe=pl.DataFrame()
)

# 执行管道
result_df = pipeline.execute(context)

# 获取质量指标
quality_metrics = context.get_state("quality_metrics")
print(f"质量指标: {quality_metrics}")
```

### 2. 使用 PipelineFactory

```python
from infrastructure.processor.pipeline_factory import PipelineFactory

# 创建工厂
factory = PipelineFactory(db_client, data_config, trading_cal)

# 创建标准因子计算管道
pipeline = factory.create_factor_pipeline(
    factor_id="momentum_20",
    preprocess_options={
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True,
    },
    save_results=True
)

# 执行管道
result_df = pipeline.execute(context)
```

### 3. 创建自定义处理器

```python
from infrastructure.processor import IProcessor, ProcessContext
import polars as pl

class WinsorizeProcessor(IProcessor):
    """极值处理器 - Winsorize 方法"""

    def __init__(self, lower_pct=0.01, upper_pct=0.99):
        self.lower_pct = lower_pct
        self.upper_pct = upper_pct

    @property
    def name(self) -> str:
        return "WinsorizeProcessor"

    def should_run(self, context: ProcessContext) -> bool:
        # 只在有 factor_value 列时运行
        return "factor_value" in context.dataframe.columns

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        # 计算分位数
        lower = df["factor_value"].quantile(self.lower_pct)
        upper = df["factor_value"].quantile(self.upper_pct)

        # Winsorize
        df = df.with_columns(
            pl.when(pl.col("factor_value") < lower)
            .then(lower)
            .when(pl.col("factor_value") > upper)
            .then(upper)
            .otherwise(pl.col("factor_value"))
            .alias("factor_value")
        )

        return df

# 使用自定义处理器
pipeline = DataPipeline()
pipeline.add_stage(DataLoaderProcessor(db_client, data_config))
pipeline.add_stage(FactorComputeProcessor())
pipeline.add_stage(WinsorizeProcessor(lower_pct=0.01, upper_pct=0.99))
pipeline.add_stage(QualityCheckerProcessor())

result_df = pipeline.execute(context)
```

### 4. 批量计算多个因子

```python
factor_ids = ["momentum_20", "volatility_20", "volume_ratio"]

results = []
for factor_id in factor_ids:
    result = service.compute_factor(
        factor_id=factor_id,
        start_date="20240101",
        end_date="20240131",
        preprocess_profile="default"
    )
    results.append(result)

# 汇总结果
for result in results:
    status = "✓" if result.success else "✗"
    print(f"{status} {result.factor_id}: {result.rows} rows, {result.elapsed_seconds:.1f}s")
```

## 配置管理

### 1. 查看可用配置

```python
from config.preprocess_loader import get_preprocess_loader

loader = get_preprocess_loader()

# 列出所有配置
profiles = loader.list_profiles()
print(f"可用配置: {profiles}")

# 查看具体配置
default_config = loader.get_profile("default")
print(f"默认配置: {default_config}")
```

### 2. 添加自定义配置

编辑 `config/preprocess_config.yaml`：

```yaml
preprocess_profiles:
  my_custom:
    adjust_price: backward
    filter_st: true
    filter_new_stock: true
    new_stock_days: 90
    handle_suspension: true
    mark_limit: false
```

使用自定义配置：

```python
result = service.compute_factor(
    factor_id="momentum_20",
    preprocess_profile="my_custom"
)
```

### 3. 动态合并配置

```python
# 基于 conservative 配置，但覆盖部分选项
merged_options = loader.merge_options(
    profile_name="conservative",
    custom_options={
        "new_stock_days": 150,
        "mark_limit": False
    }
)

result = service.compute_factor(
    factor_id="momentum_20",
    preprocess=merged_options
)
```

## 调试技巧

### 1. 启用详细日志

```python
import logging

# 启用 DEBUG 日志
logging.getLogger("infrastructure.processor").setLevel(logging.DEBUG)
logging.getLogger("services.factor_compute_service").setLevel(logging.DEBUG)

# 执行计算
result = service.compute_factor("momentum_20")
```

### 2. 检查 Pipeline 阶段

```python
from infrastructure.processor.pipeline_factory import PipelineFactory

factory = PipelineFactory(db_client, data_config, trading_cal)
pipeline = factory.create_factor_pipeline(
    factor_id="momentum_20",
    preprocess_options={"adjust_price": "forward"},
    save_results=True
)

# 查看所有阶段
for idx, stage in enumerate(pipeline.get_stages(), 1):
    print(f"{idx}. {stage.name}")
```

### 3. 检查中间结果

```python
# 不保存结果，只计算
result = service.compute_factor(
    factor_id="momentum_20",
    save_results=False
)

# 检查质量指标
if result.quality_metrics:
    print(f"Null率: {result.quality_metrics['null_rate']:.2%}")
    print(f"异常值率: {result.quality_metrics['outlier_rate']:.2%}")
    print(f"质量标记: {result.quality_metrics['quality_flag']}")
```

### 4. 使用共享状态传递调试信息

```python
class DebugProcessor(IProcessor):
    @property
    def name(self):
        return "DebugProcessor"

    def process(self, df, context):
        # 记录中间状态
        context.set_state("debug_row_count", len(df))
        context.set_state("debug_columns", df.columns)
        print(f"[DEBUG] Rows: {len(df)}, Columns: {df.columns}")
        return df

# 添加到管道
pipeline.add_stage(DebugProcessor())
```

## 错误处理

### 1. 捕获计算错误

```python
result = service.compute_factor("momentum_20")

if not result.success:
    print(f"错误: {result.message}")
    # 根据错误类型处理
    if "not found" in result.message:
        print("因子不存在")
    elif "empty" in result.message:
        print("数据为空")
```

### 2. 自定义错误处理

```python
class SafeProcessor(IProcessor):
    @property
    def name(self):
        return "SafeProcessor"

    def process(self, df, context):
        try:
            # 处理逻辑
            return df
        except Exception as e:
            # 记录错误但不中断管道
            context.set_state("error", str(e))
            return df

    def on_error(self, error, context):
        # 自定义错误处理
        print(f"处理器失败: {error}")
        # 可以发送告警、记录日志等
```

## 性能优化

### 1. 跳过不必要的处理器

```python
# 如果不需要复权，设置 adjust_price = "none"
result = service.compute_factor(
    factor_id="momentum_20",
    preprocess={"adjust_price": "none"}  # 跳过 AdjustmentProcessor
)
```

### 2. 批量处理优化

```python
# 复用数据加载，处理多个因子
from infrastructure.processor import DataLoaderProcessor

# 加载一次数据
loader = DataLoaderProcessor(db_client, data_config)
# ... 然后用于多个因子计算
```

### 3. 使用 Polars lazy API

```python
class LazyProcessor(IProcessor):
    @property
    def name(self):
        return "LazyProcessor"

    def process(self, df, context):
        # 使用 lazy API
        lazy_df = df.lazy()
        lazy_df = lazy_df.with_columns(
            (pl.col("close") / pl.col("close").shift(1) - 1).alias("return")
        )
        # 延迟计算
        return lazy_df.collect()
```

## 测试

### 1. 单元测试处理器

```python
import pytest
from infrastructure.processor import ProcessContext

def test_my_processor():
    processor = MyProcessor()

    df = pl.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240101"],
        "close": [10.0]
    })

    context = ProcessContext(
        factor_id="test",
        factor_definition=Mock(),
        calc_start="20240101",
        calc_end="20240101",
        data_start="20231201"
    )

    result = processor.process(df, context)
    assert len(result) == 1
```

### 2. 集成测试

```python
def test_full_pipeline():
    service = FactorComputeService(db_client)

    result = service.compute_factor(
        factor_id="test_factor",
        start_date="20240101",
        end_date="20240101",
        save_results=False
    )

    assert result.success
    assert result.rows > 0
```

## 常见问题

### Q: 如何查看 Pipeline 执行了哪些阶段？

A: 启用 INFO 日志，会显示每个阶段的执行情况。

### Q: 如何在不保存结果的情况下测试因子？

A: 设置 `save_results=False`。

### Q: 如何处理大数据量？

A: 使用 Polars lazy API，分批处理，或使用 DolphinDB 的分布式计算。

### Q: 如何添加新的预处理步骤？

A: 创建新的处理器，然后在 PipelineFactory 中添加。

## 下一步

- 阅读 [架构设计文档](PIPELINE_ARCHITECTURE.md)
- 查看 [迁移指南](MIGRATION_GUIDE.md)
- 运行 [集成测试](../tests/test_pipeline_integration.py)
