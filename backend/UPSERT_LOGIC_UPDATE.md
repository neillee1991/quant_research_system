# Upsert 逻辑更新说明

## 更新时间
2026-03-08

## 更新目标
统一所有数据写入操作（同步任务、ETL 任务、因子任务）的 upsert 逻辑，遵循以下规则：

1. **全量任务**：清空整个表，然后写入新数据
2. **增量任务**：只清空当前 trade_date 的数据，然后写入新数据

## 修改的文件

### 1. 核心数据操作层
**文件**: `infrastructure/database/data_operations.py`

**修改内容**:
- `upsert()` 方法新增参数：
  - `is_full_sync: bool = False` - 是否全量同步
  - `trade_date: Optional[str] = None` - 交易日期（增量同步时提供）
- 实现逻辑：
  - `is_full_sync=True`: 执行 `DELETE FROM table;` 清空整个表
  - `is_full_sync=False` 且 `trade_date` 有值: 执行 `DELETE FROM table WHERE trade_date = {date};` 清空指定日期
  - `is_full_sync=False` 且 `trade_date=None`: 直接插入（用于元数据表等无需清空的场景）

### 2. DolphinDB 客户端包装层
**文件**: `infrastructure/database/dolphindb_client.py`

**修改内容**:
- `upsert()` 方法签名更新，传递新参数到 `DataOperations.upsert()`

### 3. 数据同步引擎
**文件**: `data_manager/sync_components.py`

**修改内容**:
- `_execute_full_sync()`: 调用 `upsert(..., is_full_sync=True)`
- `_execute_incremental_sync()`: 调用 `upsert(..., is_full_sync=False, trade_date=date_str)`
- 移除了旧的手动 DELETE 逻辑

### 4. 生产引擎（因子计算）
**文件**: `engine/production/engine.py`

**修改内容**:
- `_save_results()` 新增 `compute_mode` 参数
- `_save_to_unified_table()` 根据 `compute_mode` 决定清空策略：
  - `compute_mode="full"`: 清空该因子的所有历史数据
  - `compute_mode="incremental"`: 按日期逐个清空并写入
- `_save_to_custom_table()` 同样支持全量/增量模式
- 删除了旧的 `_delete_factor_dates()` 方法

### 5. ETL 处理器
**文件**: `infrastructure/processor/processors.py`

**修改内容**:
- `ResultWriterProcessor.process()` 根据 `context.compute_mode` 调用相应的 upsert 模式

**文件**: `infrastructure/processor/pipeline.py`

**修改内容**:
- `ProcessContext` 新增 `compute_mode: str = "incremental"` 字段

## 测试验证

**测试文件**: `backend/test_upsert_logic.py`

**测试结果**:
- ✓ 全量同步测试通过：第二次全量同步清空了第一次的数据
- ✓ 增量同步测试通过：只清空了指定日期的数据，保留了其他日期的数据

## 向后兼容性

- 所有新增参数都有默认值，不影响现有代码
- 元数据表的 upsert 调用（如 `factor_metadata`, `factor_data_config`）无需修改，默认行为为直接插入

## 使用示例

### 同步任务
```python
# 全量同步
db_client.upsert(table_name, df, primary_keys, is_full_sync=True)

# 增量同步
db_client.upsert(table_name, df, primary_keys, is_full_sync=False, trade_date="20240101")
```

### 因子任务
```python
# 全量计算
engine.run_task(factor_id, mode="full")  # 内部会清空该因子的所有数据

# 增量计算
engine.run_task(factor_id, mode="incremental", target_date="20240101")  # 只清空该日期的数据
```

## 注意事项

1. **日期格式**: `trade_date` 参数使用字符串格式 "YYYYMMDD"，内部会自动转义为 DolphinDB 的日期类型
2. **事务性**: DELETE + INSERT 在同一个 DolphinDB session.run() 中执行，保证原子性
3. **性能**: 增量模式下按日期逐个清空，对于大量日期可能较慢，但保证了数据一致性
