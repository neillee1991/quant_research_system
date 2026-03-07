# DataPipeline 架构迁移指南

## 概述

本指南说明如何从旧的 `ProductionEngine` 迁移到新的 `FactorComputeService` + `DataPipeline` 架构。

## 架构对比

### 旧架构 (ProductionEngine)
```
ProductionEngine (911行)
├── run_task() - 单一方法包含所有逻辑
├── _load_data() - 数据加载
├── _apply_adjust() - 复权处理
├── _apply_stock_status() - 状态过滤
├── _handle_suspension() - 停牌处理
├── _build_quality_flag() - 质量检查
└── _save_results() - 结果保存
```

**问题：**
- 职责混杂，难以测试
- 预处理逻辑硬编码
- 扩展新处理步骤需要修改核心代码
- 无法灵活组合处理流程

### 新架构 (FactorComputeService + DataPipeline)
```
FactorComputeService (服务编排)
└── compute_factor()
    ├── 解析配置
    ├── 构建 Pipeline
    └── 执行计算

DataPipeline (可组合管道)
├── DataLoaderProcessor
├── AdjustmentProcessor
├── StatusFilterProcessor
├── FactorComputeProcessor
├── SuspensionHandlerProcessor
├── DateRangeFilterProcessor
├── QualityCheckerProcessor
└── ResultWriterProcessor
```

**优势：**
- 单一职责，每个处理器独立
- 配置驱动，支持多种预处理策略
- 易于扩展，添加新处理器无需修改核心代码
- 灵活组合，可自定义处理流程

## 迁移步骤

### 步骤 1: 安装依赖

确保已安装 PyYAML：
```bash
pip install pyyaml
```

### 步骤 2: 更新 API 调用

#### 旧代码
```python
from engine.production.engine import ProductionEngine

engine = ProductionEngine(db_client)
success = engine.run_task(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131",
    preprocess={
        "adjust_price": "forward",
        "filter_st": True,
    }
)
```

#### 新代码
```python
from services.factor_compute_service import FactorComputeService

service = FactorComputeService(db_client)
result = service.compute_factor(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131",
    preprocess={
        "adjust_price": "forward",
        "filter_st": True,
    }
)

# 检查结果
if result.success:
    print(f"计算成功: {result.rows} 行, 耗时 {result.elapsed_seconds:.1f}s")
    print(f"质量指标: {result.quality_metrics}")
else:
    print(f"计算失败: {result.message}")
```

### 步骤 3: 使用预处理配置

新架构支持预定义的预处理配置：

```python
# 使用默认配置
result = service.compute_factor(
    factor_id="momentum_20",
    preprocess_profile="default"
)

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
    preprocess={"new_stock_days": 90}  # 覆盖配置中的值
)
```

### 步骤 4: 自定义处理流程

如果需要自定义处理流程，可以直接使用 Pipeline：

```python
from infrastructure.processor import (
    DataPipeline,
    ProcessContext,
    DataLoaderProcessor,
    FactorComputeProcessor,
    QualityCheckerProcessor,
)

# 创建自定义管道（只加载数据、计算因子、检查质量）
pipeline = DataPipeline(name="CustomPipeline")
pipeline.add_stage(DataLoaderProcessor(db_client, data_config))
pipeline.add_stage(FactorComputeProcessor())
pipeline.add_stage(QualityCheckerProcessor())

# 创建上下文
context = ProcessContext(
    factor_id="test_factor",
    factor_definition=definition,
    calc_start="20240101",
    calc_end="20240131",
    data_start="20231201",
    preprocess_options={},
    dataframe=pl.DataFrame()
)

# 执行管道
result_df = pipeline.execute(context)
```

### 步骤 5: 添加自定义处理器

```python
from infrastructure.processor import IProcessor, ProcessContext
import polars as pl

class MyCustomProcessor(IProcessor):
    """自定义处理器示例"""

    @property
    def name(self) -> str:
        return "MyCustomProcessor"

    def should_run(self, context: ProcessContext) -> bool:
        # 只在特定条件下运行
        return context.get_option("enable_custom", False)

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        # 自定义处理逻辑
        df = df.with_columns(
            (pl.col("factor_value") * 2).alias("factor_value")
        )
        return df

# 使用自定义处理器
pipeline = DataPipeline()
pipeline.add_stage(DataLoaderProcessor(db_client, data_config))
pipeline.add_stage(FactorComputeProcessor())
pipeline.add_stage(MyCustomProcessor())  # 添加自定义处理器
pipeline.add_stage(QualityCheckerProcessor())
```

## API 兼容性

### ProductionEngine.run_task() 参数映射

| 旧参数 | 新参数 | 说明 |
|--------|--------|------|
| `factor_id` | `factor_id` | 相同 |
| `target_date` | `target_date` | 相同 |
| `start_date` | `start_date` | 相同 |
| `end_date` | `end_date` | 相同 |
| `mode` | `mode` | 相同 |
| `preprocess` | `preprocess` | 相同 |
| - | `preprocess_profile` | 新增：使用预定义配置 |
| - | `save_results` | 新增：控制是否保存结果 |

### 返回值变化

**旧版本：**
```python
success: bool  # True/False
```

**新版本：**
```python
result: ComputeResult
├── success: bool
├── factor_id: str
├── rows: int
├── elapsed_seconds: float
├── calc_start: str
├── calc_end: str
├── message: Optional[str]
└── quality_metrics: Optional[Dict]
```

## 预处理配置

### 配置文件位置
`backend/config/preprocess_config.yaml`

### 可用配置

| 配置名 | 说明 | 适用场景 |
|--------|------|----------|
| `default` | 默认配置，平衡的预处理策略 | 通用因子计算 |
| `conservative` | 保守配置，更严格的过滤 | 高质量因子研究 |
| `aggressive` | 激进配置，最少过滤 | 全市场覆盖 |
| `research` | 研究配置，后复权 | 学术研究 |
| `backtest` | 回测配置，标记涨跌停 | 策略回测 |
| `live` | 实盘配置，不复权 | 实盘交易 |

### 自定义配置

编辑 `config/preprocess_config.yaml` 添加新配置：

```yaml
preprocess_profiles:
  my_custom:
    adjust_price: forward
    filter_st: true
    filter_new_stock: true
    new_stock_days: 90
    handle_suspension: true
    mark_limit: true
```

使用自定义配置：
```python
result = service.compute_factor(
    factor_id="test_factor",
    preprocess_profile="my_custom"
)
```

## 测试

### 运行集成测试
```bash
cd backend
pytest tests/test_pipeline_integration.py -v
```

### 测试覆盖
- Pipeline 核心功能
- 处理器执行顺序
- 条件跳过处理器
- 具体处理器逻辑
- PipelineFactory
- FactorComputeService
- 预处理选项优先级

## 性能对比

新架构的性能与旧架构基本相同：
- 数据加载：相同（使用相同的 DolphinDB 查询）
- 数据处理：相同（使用相同的 Polars 操作）
- 额外开销：<1%（Pipeline 编排开销）

## 回滚方案

如果需要回滚到旧架构：

1. 保留旧代码：`ProductionEngine` 仍然存在于 `engine/production/engine.py`
2. 切换调用：将 `FactorComputeService` 替换回 `ProductionEngine`
3. 新旧架构可以共存，逐步迁移

## 常见问题

### Q1: 如何保持与现有 API 的兼容性？

A: 在 API 层添加适配器：

```python
# app/api/v1/production.py
from services.factor_compute_service import FactorComputeService

@router.post("/factor/compute")
def compute_factor(request: ComputeRequest):
    service = FactorComputeService(db_client)
    result = service.compute_factor(
        factor_id=request.factor_id,
        start_date=request.start_date,
        end_date=request.end_date,
        preprocess=request.preprocess
    )

    # 适配旧的返回格式
    return {
        "success": result.success,
        "message": result.message or "OK",
        "data": {
            "rows": result.rows,
            "elapsed": result.elapsed_seconds
        }
    }
```

### Q2: 如何调试 Pipeline 执行？

A: 启用详细日志：

```python
import logging
logging.getLogger("infrastructure.processor").setLevel(logging.DEBUG)
```

或者检查 Pipeline 阶段：

```python
pipeline = factory.create_factor_pipeline(...)
for stage in pipeline.get_stages():
    print(f"Stage: {stage.name}")
```

### Q3: 如何处理自定义数据源？

A: 实现自定义 DataLoader：

```python
class CustomDataLoader(IProcessor):
    @property
    def name(self):
        return "CustomDataLoader"

    def process(self, df, context):
        # 从自定义数据源加载
        custom_df = load_from_custom_source(...)
        return custom_df
```

### Q4: 如何优化性能？

A:
1. 减少不必要的处理器（通过 `should_run()` 跳过）
2. 使用 Polars lazy API（在处理器内部）
3. 批量处理多个因子（复用数据加载）

## 下一步

1. 逐步迁移现有因子计算任务
2. 监控新架构的性能和稳定性
3. 根据实际使用情况优化处理器
4. 添加更多预处理配置
5. 完全移除旧的 ProductionEngine（在充分验证后）

## 联系支持

如有问题，请查看：
- 代码文档：`infrastructure/processor/`
- 测试用例：`tests/test_pipeline_integration.py`
- 配置示例：`config/preprocess_config.yaml`
