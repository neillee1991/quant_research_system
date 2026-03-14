# 数据探查功能文档

> 日期: 2026-03-14
> 功能: 任务数据完整性检查

## 功能概述

在同步任务、ETL 任务、因子任务的详情页中，新增**数据探查**功能，用于检查表中数据的完整性：

- 显示表中数据的最早日期和最晚日期
- 对比该时间段内的交易日历
- 找出缺失的交易日
- 计算数据覆盖率

## API 端点

### 数据探查

```http
GET /api/v1/tasks/{task_type}/{task_id}/inspect
```

**路径参数**：
- `task_type`: 任务类型 (`sync` / `etl` / `factor`)
- `task_id`: 任务 ID

**响应示例 1**（表不存在）：
```json
{
  "table_name": "sync_trade_cal",
  "exists": false,
  "message": "Table sync_trade_cal does not exist yet"
}
```

**响应示例 2**（表存在但无数据）：
```json
{
  "table_name": "sync_daily_data",
  "exists": true,
  "has_data": false,
  "message": "Table sync_daily_data exists but has no data"
}
```

**响应示例 3**（有数据，无交易日历）：
```json
{
  "table_name": "sync_adj_factor",
  "exists": true,
  "has_data": true,
  "date_field": "trade_date",
  "min_date": "2020-01-02",
  "max_date": "2024-12-31",
  "actual_dates": 1205,
  "trading_calendar_available": false,
  "message": "Trading calendar not available, cannot check missing dates"
}
```

**响应示例 4**（完整数据探查）：
```json
{
  "table_name": "sync_daily_basic",
  "exists": true,
  "has_data": true,
  "date_field": "trade_date",
  "min_date": "2020-01-02",
  "max_date": "2024-12-31",
  "actual_dates": 1200,
  "expected_dates": 1220,
  "missing_dates": [
    "2020-03-15",
    "2020-06-20",
    "2021-01-10",
    ...
  ],
  "missing_count": 20,
  "coverage_percent": 98.36,
  "trading_calendar_available": true
}
```

## 响应字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `table_name` | string | 表名 |
| `exists` | boolean | 表是否存在 |
| `has_data` | boolean | 表是否有数据 |
| `date_field` | string | 日期字段名（如 `trade_date`） |
| `min_date` | string | 最早日期 (YYYY-MM-DD) |
| `max_date` | string | 最晚日期 (YYYY-MM-DD) |
| `actual_dates` | integer | 实际数据天数 |
| `expected_dates` | integer | 预期交易日天数 |
| `missing_dates` | array | 缺失的交易日列表 |
| `missing_count` | integer | 缺失天数 |
| `coverage_percent` | float | 数据覆盖率 (%) |
| `trading_calendar_available` | boolean | 交易日历是否可用 |
| `message` | string | 提示信息 |

## 实现逻辑

### 1. 检查表是否存在

```python
if not db_client.table_exists(table_name):
    return {"exists": False, "message": "Table does not exist yet"}
```

### 2. 检查表是否有数据

```sql
SELECT count(*) as total
FROM loadTable('dfs://quant', 'table_name')
LIMIT 1
```

### 3. 查询日期范围

```sql
SELECT
    min(date_field) as min_date,
    max(date_field) as max_date
FROM loadTable("dfs://quant", "table_name")
```

### 4. 获取实际存在的日期

```sql
SELECT DISTINCT date_field as date
FROM loadTable("dfs://quant", "table_name")
WHERE date_field >= min_date AND date_field <= max_date
ORDER BY date_field
```

### 5. 获取交易日历

```sql
SELECT cal_date
FROM loadTable("dfs://quant", "sync_trade_cal")
WHERE exchange = 'SSE'
  AND is_open = 1
  AND cal_date >= min_date
  AND cal_date <= max_date
ORDER BY cal_date
```

### 6. 计算缺失日期

```python
missing_dates = trading_days - actual_dates
coverage = (len(actual_dates) / len(trading_days)) * 100
```

## 前端集成建议

### 1. 在任务详情页添加"数据探查"按钮

```tsx
<Button onClick={handleInspectData}>
  数据探查
</Button>
```

### 2. 显示探查结果

```tsx
interface DataInspectionResult {
  table_name: string;
  exists: boolean;
  has_data?: boolean;
  date_field?: string;
  min_date?: string;
  max_date?: string;
  actual_dates?: number;
  expected_dates?: number;
  missing_dates?: string[];
  missing_count?: number;
  coverage_percent?: number;
  trading_calendar_available?: boolean;
  message?: string;
}

const DataInspectionPanel: React.FC<{data: DataInspectionResult}> = ({data}) => {
  if (!data.exists) {
    return <Alert type="warning">{data.message}</Alert>;
  }

  if (!data.has_data) {
    return <Alert type="info">{data.message}</Alert>;
  }

  return (
    <Card title="数据探查结果">
      <Descriptions column={2}>
        <Descriptions.Item label="表名">{data.table_name}</Descriptions.Item>
        <Descriptions.Item label="日期字段">{data.date_field}</Descriptions.Item>
        <Descriptions.Item label="最早日期">{data.min_date}</Descriptions.Item>
        <Descriptions.Item label="最晚日期">{data.max_date}</Descriptions.Item>
        <Descriptions.Item label="实际天数">{data.actual_dates}</Descriptions.Item>
        <Descriptions.Item label="预期天数">{data.expected_dates}</Descriptions.Item>
        <Descriptions.Item label="缺失天数">
          <Badge count={data.missing_count} showZero />
        </Descriptions.Item>
        <Descriptions.Item label="覆盖率">
          <Progress
            percent={data.coverage_percent}
            status={data.coverage_percent >= 95 ? 'success' : 'exception'}
          />
        </Descriptions.Item>
      </Descriptions>

      {data.missing_count > 0 && (
        <div style={{marginTop: 16}}>
          <h4>缺失的交易日：</h4>
          <Tag.Group>
            {data.missing_dates.slice(0, 20).map(date => (
              <Tag key={date} color="red">{date}</Tag>
            ))}
            {data.missing_count > 20 && (
              <Tag>... 还有 {data.missing_count - 20} 天</Tag>
            )}
          </Tag.Group>
        </div>
      )}
    </Card>
  );
};
```

### 3. API 调用示例

```typescript
const inspectTaskData = async (taskType: string, taskId: string) => {
  try {
    const response = await fetch(
      `http://localhost:8000/api/v1/tasks/${taskType}/${taskId}/inspect`
    );
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Data inspection failed:', error);
    throw error;
  }
};

// 使用
const handleInspectData = async () => {
  setLoading(true);
  try {
    const result = await inspectTaskData('sync', 'sync_daily_data');
    setInspectionResult(result);
    setModalVisible(true);
  } catch (error) {
    message.error('数据探查失败');
  } finally {
    setLoading(false);
  }
};
```

## 使用场景

### 1. 数据同步任务

检查数据同步是否完整，是否有遗漏的交易日：

```bash
curl http://localhost:8000/api/v1/tasks/sync/sync_daily_data/inspect
```

### 2. ETL 任务

检查 ETL 处理后的数据是否完整：

```bash
curl http://localhost:8000/api/v1/tasks/etl/etl_stock_daily_info/inspect
```

### 3. 因子任务

检查因子计算结果的覆盖范围：

```bash
curl http://localhost:8000/api/v1/tasks/factor/ma_5/inspect
```

## 性能优化

### 1. 查询优化

- 使用 `LIMIT 1` 快速检查表是否有数据
- 先查询日期范围，再查询具体日期列表
- 避免使用 `count(distinct field)` 嵌套聚合

### 2. 缓存策略

建议前端缓存探查结果，避免频繁查询：

```typescript
const cacheKey = `inspection_${taskType}_${taskId}`;
const cachedResult = localStorage.getItem(cacheKey);

if (cachedResult) {
  const {data, timestamp} = JSON.parse(cachedResult);
  // 缓存 5 分钟
  if (Date.now() - timestamp < 5 * 60 * 1000) {
    return data;
  }
}

// 查询并缓存
const result = await inspectTaskData(taskType, taskId);
localStorage.setItem(cacheKey, JSON.stringify({
  data: result,
  timestamp: Date.now()
}));
```

## 错误处理

### 1. 表不存在

```json
{
  "table_name": "sync_xxx",
  "exists": false,
  "message": "Table sync_xxx does not exist yet"
}
```

**处理建议**：提示用户先执行一次同步任务

### 2. 交易日历不可用

```json
{
  "trading_calendar_available": false,
  "message": "Trading calendar not available, cannot check missing dates"
}
```

**处理建议**：提示用户先同步交易日历数据（`sync_trade_cal` 任务）

### 3. 日期字段不存在

某些任务（如 `sync_stock_basic`）没有日期字段，会返回错误。

**处理建议**：只对有日期字段的任务显示"数据探查"按钮

## 后端代码位置

| 文件 | 说明 |
|------|------|
| `backend/app/services/task_service.py:338-450` | `TaskService.inspect_data()` 方法 |
| `backend/app/api/v1/tasks.py:80-98` | `DataInspectionResponse` 模型 |
| `backend/app/api/v1/tasks.py:382-410` | `/tasks/{task_type}/{task_id}/inspect` 端点 |

## 测试

### 1. 测试表不存在

```bash
curl http://localhost:8000/api/v1/tasks/sync/sync_trade_cal/inspect
```

### 2. 测试表存在但无数据

```bash
# 先创建表但不写入数据
curl http://localhost:8000/api/v1/tasks/sync/sync_daily_data/inspect
```

### 3. 测试完整功能

```bash
# 需要先同步一些数据
curl http://localhost:8000/api/v1/tasks/sync/sync_adj_factor/inspect
```

## 未来优化

### 1. 支持自定义日期范围

```http
GET /api/v1/tasks/{task_type}/{task_id}/inspect?start_date=20240101&end_date=20241231
```

### 2. 支持多个交易所

```http
GET /api/v1/tasks/{task_type}/{task_id}/inspect?exchange=SZSE
```

### 3. 导出缺失日期列表

```http
GET /api/v1/tasks/{task_type}/{task_id}/inspect/export
```

返回 CSV 文件，包含所有缺失的交易日。

### 4. 数据质量评分

基于覆盖率、缺失天数、数据新鲜度等指标，给出数据质量评分（0-100）。

---

**最后更新**: 2026-03-14
**状态**: ✅ 已实现并测试
