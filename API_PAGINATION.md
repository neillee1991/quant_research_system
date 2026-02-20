# API 分页功能说明

## 功能概述

当 Tushare API 有数据量限制时，系统现在支持自动分页获取所有数据。通过在任务配置中设置 `api_limit` 参数，系统会自动循环调用 API，使用 `limit` 和 `offset` 参数获取完整数据。

## 工作原理

### 1. 分页逻辑

当配置了 `api_limit` 参数时：

```python
# 第一次调用: offset=0, limit=5000
# 返回 5000 行数据

# 第二次调用: offset=5000, limit=5000
# 返回 5000 行数据

# 第三次调用: offset=10000, limit=5000
# 返回 2000 行数据 (小于 limit，说明到达数据尾部)

# 停止循环，总共获取 12000 行数据
```

### 2. 终止条件

系统通过以下条件判断是否到达数据尾部：

1. **返回数据为空**: `df is None or df.is_empty()`
2. **返回行数小于 limit**: `rows_count < limit`

这两个条件确保了：
- 不会无限循环
- 能够准确获取所有数据
- 避免不必要的 API 调用

## 配置示例

### 示例 1: 全量同步股票基础信息

```json
{
  "task_id": "stock_basic",
  "api_name": "stock_basic",
  "sync_type": "full",
  "params": {
    "exchange": "",
    "list_status": "L"
  },
  "table_name": "stock_basic",
  "batch_size": 5000,
  "api_limit": 5000,
  "enabled": true
}
```

**说明**:
- 假设有 12000 只股票
- `api_limit=5000` 会触发 3 次 API 调用
- 第 1 次: offset=0, 获取 5000 行
- 第 2 次: offset=5000, 获取 5000 行
- 第 3 次: offset=10000, 获取 2000 行 (小于 5000，停止)

### 示例 2: 增量同步日线数据

```json
{
  "task_id": "daily_basic",
  "api_name": "daily_basic",
  "sync_type": "incremental",
  "params": {
    "trade_date": "{date}"
  },
  "table_name": "daily_basic",
  "batch_size": 1000,
  "api_limit": 5000,
  "enabled": true
}
```

**说明**:
- 每天同步一次
- 如果某天有 8000 只股票的数据
- `api_limit=5000` 会触发 2 次 API 调用
- 第 1 次: offset=0, 获取 5000 行
- 第 2 次: offset=5000, 获取 3000 行 (小于 5000，停止)

### 示例 3: 不使用分页

```json
{
  "task_id": "adj_factor",
  "api_name": "adj_factor",
  "sync_type": "incremental",
  "params": {
    "trade_date": "{date}"
  },
  "table_name": "adj_factor",
  "batch_size": 5000,
  "enabled": true
}
```

**说明**:
- 没有设置 `api_limit`
- 系统会直接调用 API，不进行分页
- 适用于数据量较小的接口

## 参数说明

### batch_size vs api_limit

这两个参数容易混淆，但作用不同：

| 参数 | 作用 | 默认值 | 示例 |
|------|------|--------|------|
| `batch_size` | 数据库写入批次大小 | 5000 | 从内存写入数据库时，每次写入 5000 行 |
| `api_limit` | API 单次调用的 limit 参数 | None (不分页) | API 调用时的 limit 参数，用于分页 |

**使用场景**:

1. **只设置 batch_size**:
   - API 一次性返回所有数据
   - 数据库分批写入
   - 适用于数据量小的接口

2. **同时设置 batch_size 和 api_limit**:
   - API 分页获取数据
   - 数据库分批写入
   - 适用于数据量大的接口

## 日志示例

### 成功的分页调用

```
2026-02-20 21:00:00 | INFO | Starting sync task: stock_basic (股票列表)
2026-02-20 21:00:01 | DEBUG | Calling stock_basic with limit=5000, offset=0
2026-02-20 21:00:02 | INFO | Fetched 5000 rows at offset=0
2026-02-20 21:00:03 | DEBUG | Calling stock_basic with limit=5000, offset=5000
2026-02-20 21:00:04 | INFO | Fetched 5000 rows at offset=5000
2026-02-20 21:00:05 | DEBUG | Calling stock_basic with limit=5000, offset=10000
2026-02-20 21:00:06 | INFO | Fetched 2000 rows at offset=10000
2026-02-20 21:00:06 | DEBUG | Reached end of data: 2000 < 5000
2026-02-20 21:00:07 | INFO | Full sync completed for stock_basic: 12000 rows
```

### 到达数据尾部

```
2026-02-20 21:00:10 | DEBUG | Calling daily_basic with limit=5000, offset=5000
2026-02-20 21:00:11 | DEBUG | No more data at offset=5000
2026-02-20 21:00:11 | INFO | Full sync completed for daily_basic: 5000 rows
```

## 性能考虑

### 1. API 调用频率

- 系统会自动进行速率限制 (`rate_limit_wait`)
- 默认每分钟 120 次调用
- 分页会增加 API 调用次数，需要注意配额

### 2. 内存使用

- 所有分页数据会在内存中合并
- 如果数据量非常大（如百万行），建议：
  - 增大 `api_limit` 减少调用次数
  - 或者使用增量同步，按日期分批处理

### 3. 推荐配置

| 数据量 | api_limit | batch_size | 说明 |
|--------|-----------|------------|------|
| < 5000 行 | 不设置 | 5000 | 一次性获取 |
| 5000-50000 行 | 5000 | 5000 | 标准分页 |
| > 50000 行 | 10000 | 5000 | 大批量分页 |

## 错误处理

### 1. API 调用失败

- 系统会自动重试（默认 3 次）
- 重试间隔递增（2^attempt 秒）
- 如果所有重试都失败，返回 None

### 2. 数据异常

- 如果某次分页返回空数据，会记录警告并停止
- 已获取的数据会正常写入数据库
- 不会影响后续任务的执行

## 最佳实践

1. **首次同步大量历史数据**:
   - 设置较大的 `api_limit` (如 10000)
   - 使用增量同步，按日期分批
   - 监控日志确保数据完整

2. **日常增量更新**:
   - 使用默认的 `api_limit` (5000)
   - 每天只同步最新一天的数据
   - 数据量小，速度快

3. **全量同步基础数据**:
   - 设置 `api_limit` 确保获取所有数据
   - 使用 `auto_add_trade_date` 追踪更新时间
   - 定期执行（如每周）

## 总结

API 分页功能通过自动处理 `limit` 和 `offset` 参数，解决了 Tushare API 单次调用数据量限制的问题。只需在任务配置中添加 `api_limit` 参数，系统就会自动：

- ✅ 循环调用 API 直到获取所有数据
- ✅ 智能判断数据尾部，避免无限循环
- ✅ 合并所有分页数据
- ✅ 分批写入数据库
- ✅ 记录详细日志便于监控

这使得同步大量数据变得简单可靠。
