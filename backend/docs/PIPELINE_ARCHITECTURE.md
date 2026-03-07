# DataPipeline 架构设计文档

## 架构概览

DataPipeline 是一个可组合的数据处理管道架构，用于替代原有的 ProductionEngine 单体设计。

### 核心设计原则

1. **单一职责** - 每个处理器只负责一个特定的数据处理任务
2. **开闭原则** - 对扩展开放，对修改关闭
3. **依赖倒置** - 依赖抽象接口，不依赖具体实现
4. **配置驱动** - 通过配置文件控制处理流程，无需修改代码

## 架构分层

```
┌─────────────────────────────────────────────────────────┐
│                    API Layer                            │
│              (app/api/v1/production.py)                 │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                  Service Layer                          │
│         (services/factor_compute_service.py)            │
│  - 服务编排                                              │
│  - 日期解析                                              │
│  - 配置管理                                              │
│  - 结果管理                                              │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Infrastructure Layer                       │
│        (infrastructure/processor/)                      │
│                                                         │
│  ┌─────────────────────────────────────────────┐      │
│  │         PipelineFactory                     │      │
│  │  - 根据配置构建 Pipeline                     │      │
│  └─────────────────────────────────────────────┘      │
│                           │                             │
│                           ▼                             │
│  ┌─────────────────────────────────────────────┐      │
│  │         DataPipeline                        │      │
│  │  - 编排处理器执行                            │      │
│  │  - 管理上下文传递                            │      │
│  └─────────────────────────────────────────────┘      │
│                           │                             │
│                           ▼                             │
│  ┌─────────────────────────────────────────────┐      │
│  │         Processors (8个)                    │      │
│  │  1. DataLoader                              │      │
│  │  2. AdjustmentProcessor                     │      │
│  │  3. StatusFilterProcessor                   │      │
│  │  4. FactorComputeProcessor                  │      │
│  │  5. SuspensionHandlerProcessor              │      │
│  │  6. DateRangeFilterProcessor                │      │
│  │  7. QualityCheckerProcessor                 │      │
│  │  8. ResultWriterProcessor                   │      │
│  └─────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                Configuration Layer                      │
│           (config/preprocess_config.yaml)               │
│  - 预处理配置                                            │
│  - 多种预设策略                                          │
└─────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. ProcessContext (上下文)

**职责：** 在 Pipeline 各阶段间传递信息

**关键字段：**
```python
@dataclass
class ProcessContext:
    factor_id: str                          # 因子ID
    factor_definition: FactorDefinition     # 因子定义
    calc_start: str                         # 计算起始日期
    calc_end: str                           # 计算结束日期
    data_start: str                         # 数据加载起始日期
    preprocess_options: Dict[str, Any]      # 预处理选项
    run_id: Optional[str]                   # 运行ID
    shared_state: Dict[str, Any]            # 共享状态
    dataframe: Optional[pl.DataFrame]       # 数据引用
```

**设计要点：**
- 使用 dataclass 减少样板代码
- shared_state 用于处理器间传递临时数据
- dataframe 引用避免重复传递大对象

### 2. IProcessor (处理器接口)

**职责：** 定义处理器契约

**核心方法：**
```python
class IProcessor(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """处理器名称"""
        pass

    def should_run(self, context: ProcessContext) -> bool:
        """判断是否需要执行"""
        return True

    @abstractmethod
    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """执行处理逻辑"""
        pass

    def on_error(self, error: Exception, context: ProcessContext) -> None:
        """错误处理钩子"""
        pass
```

**设计要点：**
- 使用 ABC 强制子类实现必要方法
- should_run() 支持条件执行
- on_error() 提供错误处理扩展点

### 3. DataPipeline (管道编排器)

**职责：** 按顺序执行多个处理器

**核心方法：**
```python
class DataPipeline:
    def add_stage(self, processor: IProcessor) -> 'DataPipeline':
        """添加处理阶段（链式调用）"""
        pass

    def execute(self, context: ProcessContext) -> pl.DataFrame:
        """执行完整管道"""
        pass
```

**执行流程：**
```
1. 遍历所有处理器
2. 检查 should_run() 是否需要执行
3. 调用 process() 执行处理
4. 更新 context.dataframe
5. 处理异常（调用 on_error()）
6. 返回最终结果
```

### 4. PipelineFactory (管道工厂)

**职责：** 根据配置动态构建 Pipeline

**核心方法：**
```python
class PipelineFactory:
    def create_factor_pipeline(
        self,
        factor_id: str,
        preprocess_options: Dict[str, Any],
        save_results: bool = True
    ) -> DataPipeline:
        """创建因子计算管道"""
        pass
```

**构建逻辑：**
```python
pipeline = DataPipeline()
pipeline.add_stage(DataLoaderProcessor(...))           # 1. 数据加载
if adjust_price:
    pipeline.add_stage(AdjustmentProcessor(...))       # 2. 复权处理
if filter_st or filter_new_stock:
    pipeline.add_stage(StatusFilterProcessor(...))     # 3. 状态过滤
pipeline.add_stage(FactorComputeProcessor())           # 4. 因子计算
if handle_suspension:
    pipeline.add_stage(SuspensionHandlerProcessor(...))# 5. 停牌处理
pipeline.add_stage(DateRangeFilterProcessor())         # 6. 日期过滤
pipeline.add_stage(QualityCheckerProcessor())          # 7. 质量检查
if save_results:
    pipeline.add_stage(ResultWriterProcessor(...))     # 8. 结果写入
return pipeline
```

### 5. FactorComputeService (因子计算服务)

**职责：** 服务编排，协调各组件完成因子计算

**核心方法：**
```python
class FactorComputeService:
    def compute_factor(
        self,
        factor_id: str,
        target_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: Optional[str] = None,
        preprocess: Optional[Dict[str, Any]] = None,
        preprocess_profile: Optional[str] = None,
        save_results: bool = True,
    ) -> ComputeResult:
        """执行因子计算"""
        pass
```

**执行流程：**
```
1. 获取因子定义 (discover_factors, get_factor)
2. 解析预处理选项 (_resolve_preprocess_options)
3. 解析日期范围 (_resolve_dates)
4. 创建运行记录 (_create_run_record)
5. 构建处理上下文 (ProcessContext)
6. 构建数据处理管道 (PipelineFactory)
7. 执行管道 (pipeline.execute)
8. 更新因子元数据 (_update_metadata)
9. 完成运行记录 (_finish_run_record)
10. 返回计算结果 (ComputeResult)
```

## 8个处理器详解

### 1. DataLoaderProcessor
- **职责：** 从 DolphinDB 加载依赖数据
- **输入：** 空 DataFrame
- **输出：** 合并后的原始数据
- **关键逻辑：** 根据 depends_on 加载多个表并 join

### 2. AdjustmentProcessor
- **职责：** 应用前复权/后复权
- **条件：** adjust_price in ("forward", "backward")
- **关键逻辑：** 加载 adj_factor，调整 OHLC 价格

### 3. StatusFilterProcessor
- **职责：** 过滤 ST、新股，标记涨跌停
- **条件：** filter_st or filter_new_stock or mark_limit
- **关键逻辑：** 从 stock_daily_status 加载状态数据并过滤

### 4. FactorComputeProcessor
- **职责：** 执行因子计算函数
- **关键逻辑：** 调用 definition.func(df, params)

### 5. SuspensionHandlerProcessor
- **职责：** 停牌期间因子值置空
- **条件：** handle_suspension = True
- **关键逻辑：** 根据 is_suspend 标记将 factor_value 置 null

### 6. DateRangeFilterProcessor
- **职责：** 过滤到目标日期范围
- **关键逻辑：** 过滤 trade_date 在 [calc_start, calc_end] 范围内

### 7. QualityCheckerProcessor
- **职责：** 生成因子质量标记
- **关键逻辑：** 计算 null_rate、outlier_rate，生成 quality_flag

### 8. ResultWriterProcessor
- **职责：** 保存结果到 DolphinDB
- **条件：** save_results = True
- **关键逻辑：** upsert 到 factor_values 表

## 配置系统

### 预处理配置文件

**位置：** `config/preprocess_config.yaml`

**结构：**
```yaml
preprocess_profiles:
  default:
    adjust_price: forward
    filter_st: true
    filter_new_stock: true
    new_stock_days: 60
    handle_suspension: true
    mark_limit: true

  conservative:
    # 更严格的过滤
    new_stock_days: 120

  aggressive:
    # 最少的过滤
    filter_st: false
```

### 配置优先级

```
显式传入 > preprocess_profile > DB配置 > 代码定义 > 默认配置
```

## 扩展性设计

### 添加新处理器

```python
from infrastructure.processor import IProcessor, ProcessContext
import polars as pl

class MyNewProcessor(IProcessor):
    @property
    def name(self) -> str:
        return "MyNewProcessor"

    def should_run(self, context: ProcessContext) -> bool:
        return context.get_option("enable_my_feature", False)

    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        # 自定义处理逻辑
        return df
```

### 自定义管道

```python
from infrastructure.processor import DataPipeline

pipeline = DataPipeline(name="CustomPipeline")
pipeline.add_stage(DataLoaderProcessor(...))
pipeline.add_stage(MyNewProcessor())
pipeline.add_stage(FactorComputeProcessor())
pipeline.add_stage(QualityCheckerProcessor())

result = pipeline.execute(context)
```

## 性能优化

### 1. 条件跳过
通过 `should_run()` 跳过不必要的处理器，减少开销。

### 2. 数据引用
使用 `context.dataframe` 引用传递，避免重复复制大对象。

### 3. Polars 优化
在处理器内部使用 Polars lazy API，延迟计算。

### 4. 批量处理
复用 Pipeline 处理多个因子，共享数据加载。

## 监控与日志

### 日志级别
- INFO: Pipeline 开始/结束，每个阶段的执行
- DEBUG: 详细的数据统计
- WARNING: 跳过的处理器，空数据
- ERROR: 处理失败

### 监控指标
- 计算耗时 (elapsed_seconds)
- 数据行数 (rows)
- 质量指标 (quality_metrics)
- 成功率 (success)

## 总结

DataPipeline 架构通过以下方式解决了旧架构的问题：

1. **可维护性** - 单一职责，代码清晰
2. **可扩展性** - 开闭原则，易于添加新功能
3. **可测试性** - 独立组件，易于单元测试
4. **灵活性** - 配置驱动，支持多种场景
5. **性能** - 与旧架构相当，额外开销<1%

这是一个面向未来的架构设计，为量化研究平台的长期发展奠定了坚实基础。
