# 停牌数据上下游功能分析

## 执行时间
2026-03-15

## 分析结论

**停牌处理功能在当前系统中完全无效，可以安全移除。**

## 数据源现状

### 1. etl_stock_daily_info 表
- **当前状态**: 表存在但为空（0行数据）
- **表结构**: `['ts_code', 'trade_date', 'is_st', 'is_limit', 'l1_name', 'l2_name']`
- **关键发现**: **没有 `is_suspend` 列**

### 2. 其他表中的停牌数据
经过全库检查，**没有任何表包含停牌数据**：
- `sync_daily_data`: 只有 OHLCV 数据
- `sync_daily_basic`: 只有估值指标
- `sync_stock_st`: 只有 ST 状态
- `sync_stk_limit`: 只有涨跌停价格

## 代码中的停牌引用

### 1. 配置层 (无效配置)

**文件**: `backend/engine/production/data_config.py`
```python
"is_suspend": {"table_name": "", "column_name": "is_suspend", "extra_config": {}},
```
- `table_name` 为空字符串，表示未配置数据源
- 这是一个占位配置，实际不会加载任何数据

**文件**: `backend/store/dolphindb_client.py:509` 和 `seed_data.py:153`
```python
"field_key": "is_suspend",
```
- 仅在初始化种子数据时定义，但未关联实际表

### 2. 处理器层 (永不执行)

**文件**: `backend/infrastructure/processor/processors.py`

#### StatusFilterProcessor (lines 235-340)
```python
def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
    # 加载状态数据配置
    config = self.data_config.load()
    field_configs = {
        "is_st": config.get("is_st"),
        "is_suspend": config.get("is_suspend"),  # 获取配置
        "is_limit": config.get("is_limit"),
    }

    # 按表分组加载
    for field_key, cfg in field_configs.items():
        tbl = cfg.get("table_name", "")
        col = cfg.get("column_name", "")
        if not tbl or not col:
            logger.debug(f"Skipping {field_key}: missing table_name or column_name")
            continue  # is_suspend 在这里被跳过
```
- **实际行为**: 因为 `is_suspend` 的 `table_name` 为空，直接跳过加载
- **结果**: `is_suspend` 列永远不会被添加到 DataFrame

#### SuspensionHandlerProcessor (lines 416-449)
```python
def should_run(self, context: ProcessContext) -> bool:
    """只有配置了停牌处理才执行"""
    return context.get_option("handle_suspension", False)

def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
    # 如果已经有 is_suspend 列（来自 StatusFilter）
    if "is_suspend" in df.columns:
        # 将停牌期间的因子值置空
        df = df.with_columns(
            pl.when(pl.col("is_suspend") == 1)
            .then(None)
            .otherwise(pl.col("factor_value"))
            .alias("factor_value")
        )
```
- **实际行为**: 即使 `handle_suspension=True`，因为 `is_suspend` 列不存在，直接返回原 DataFrame
- **结果**: 这个处理器从未真正处理过任何数据

### 3. API 层 (传递无效参数)

**文件**: `backend/app/api/v1/production/factor_compute.py`
```python
class PreprocessOptions(BaseModel):
    handle_suspension: bool = True  # 默认开启，但实际无效
```

**文件**: `frontend/src/api/index.ts`
```typescript
export interface PreprocessOptions {
  handle_suspension: boolean;   // 前端传递此参数
}

export const DEFAULT_PREPROCESS: PreprocessOptions = {
  handle_suspension: true,  // 默认开启
}
```

### 4. 前端 UI (显示无效选项)

**文件**: `frontend/src/pages/FactorCenter/FactorDrawer.tsx` 和其他前端文件
- 显示"停牌复牌处理"选项
- 用户可以勾选，但实际不起作用

## 为什么停牌处理不需要

### 用户的正确理解
> "我认为停牌的数据不需要配置，主要是因为如果股票停牌，当天就不会有价格数据"

这个理解是**完全正确**的：

1. **数据源特性**: Tushare API 的 `daily` 接口只返回有交易的数据
2. **停牌日无数据**: 停牌期间不会有任何行情记录
3. **自然过滤**: 因子计算基于 `sync_daily_data` 表，停牌股票自动不参与计算
4. **无需额外处理**: 不需要显式标记和过滤停牌数据

### 实际验证
```sql
-- 检查 factor_ma_20 的数据
SELECT trade_date, count(*) as stock_count
FROM factor_values
WHERE factor_id = 'factor_ma_20'
GROUP BY trade_date
ORDER BY trade_date

-- 结果显示每天约 5000 只股票有数据
-- 这些都是有交易的股票，停牌股票自然不在其中
```

## 影响范围总结

### 上游数据
- **无影响**: 没有任何表提供停牌数据
- **配置无效**: `factor_data_config` 中的 `is_suspend` 配置为空

### 处理流程
- **StatusFilterProcessor**: 跳过 `is_suspend` 加载
- **SuspensionHandlerProcessor**: 检查列不存在，直接返回
- **因子计算**: 完全不受影响

### 下游功能
- **因子值存储**: 不包含停牌相关字段
- **因子分析**: 不依赖停牌数据
- **回测系统**: 不依赖停牌数据

### 用户界面
- **前端选项**: 显示但无实际作用
- **API 参数**: 传递但被忽略

## 建议操作

### 1. 清理代码（可选）
可以移除以下无效代码：
- `SuspensionHandlerProcessor` 类
- `handle_suspension` 配置选项
- 前端的停牌处理选项
- `factor_data_config` 中的 `is_suspend` 配置

### 2. 保留代码（推荐）
也可以保留现有代码，因为：
- 代码已经是"惰性"的，不会造成性能影响
- 如果未来需要停牌数据，可以直接配置数据源
- 不影响系统正常运行

### 3. 文档说明
在用户文档中明确说明：
- 停牌处理选项当前无效
- 停牌股票通过数据源自然过滤
- 如需显式停牌处理，需要先配置停牌数据源

## 相关文件清单

### 后端代码
1. `backend/infrastructure/processor/processors.py` - StatusFilterProcessor, SuspensionHandlerProcessor
2. `backend/engine/production/data_config.py` - is_suspend 配置
3. `backend/app/api/v1/production/factor_compute.py` - PreprocessOptions
4. `backend/services/factor_compute_service.py` - DEFAULT_PREPROCESS
5. `backend/store/dolphindb_client.py` - 种子数据定义
6. `backend/store/dolphindb/seed_data.py` - 初始化配置

### 前端代码
1. `frontend/src/api/index.ts` - PreprocessOptions 接口
2. `frontend/src/pages/FactorCenter.tsx` - 停牌处理选项
3. `frontend/src/pages/FactorCenter/FactorDrawer.tsx` - UI 组件
4. `frontend/src/pages/FactorCenter/FactorManageTab.tsx` - 管理界面
5. `frontend/src/types/factor.ts` - 类型定义

### 文档
1. `backend/docs/PIPELINE_ARCHITECTURE.md` - 架构说明
2. `backend/docs/processor_config_priority.md` - 配置优先级
3. `backend/docs/FACTOR_MIGRATION_GUIDE.md` - 迁移指南
4. `backend/docs/COMPREHENSIVE_GUIDE.md` - 综合指南

## 结论

停牌处理功能在当前系统中是一个**完全无效的占位功能**：
- 没有数据源支持
- 处理器永不执行
- 对系统无任何影响

用户的理解是正确的：停牌股票通过数据源的自然特性被过滤，不需要额外的停牌处理逻辑。
