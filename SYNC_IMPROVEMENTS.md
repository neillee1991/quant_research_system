# 同步系统改进总结

## 1. 新增功能：日期范围同步

### 前端改进
- 在同步模态框中添加了 **Start Date** 和 **End Date** 输入框
- 用户可以指定日期范围进行同步，而不仅仅是单个目标日期
- 保留了原有的 Target Date 字段以保持向后兼容

### 后端改进
- API 端点 `/data/sync/task/{task_id}` 新增参数：
  - `start_date`: 开始日期 (YYYYMMDD)
  - `end_date`: 结束日期 (YYYYMMDD)
- 同步引擎支持日期范围：
  - 如果指定了 start_date 和 end_date，系统会从 start_date 同步到 end_date
  - 如果只指定 target_date，保持原有行为
  - 如果都不指定，同步最新一天的数据

### 使用示例
```
同步 2024年1月1日 到 2024年1月31日 的数据：
- Start Date: 20240101
- End Date: 20240131
```

## 2. Tushare API 结构分析与灵活配置模板

### API 调用模式分类

#### 模式 1: 按日期同步 (by_date)
- **适用场景**: 获取某一天所有股票的数据
- **API 示例**: daily_basic, moneyflow, daily
- **参数模式**: `trade_date=YYYYMMDD`
- **特点**: 一次调用返回多只股票的数据
- **优势**: 效率高，适合日常增量更新

#### 模式 2: 按股票同步 (by_stock)
- **适用场景**: 获取某只股票的完整历史数据
- **API 示例**: daily (支持 ts_code + start_date + end_date)
- **参数模式**: `ts_code=000001.SZ&start_date=YYYYMMDD&end_date=YYYYMMDD`
- **特点**: 需要遍历所有股票，每只股票调用一次
- **优势**: 适合补全历史数据、单只股票深度分析

#### 模式 3: 全量同步 (full)
- **适用场景**: 获取不随时间变化的基础数据
- **API 示例**: stock_basic, trade_cal
- **参数模式**: 无日期参数或固定参数
- **特点**: 一次性获取所有数据
- **优势**: 简单直接，适合基础数据维护

### 新的配置模板结构

```json
{
  "task_id": "task_name",
  "api_name": "tushare_api_name",
  "description": "任务描述",
  "sync_mode": "by_date | by_stock | full",
  "sync_type": "incremental | full",
  "schedule": "daily | weekly | monthly",
  "enabled": true,
  "params": {
    "trade_date": "{date}",
    "ts_code": "{stock_code}",
    "start_date": "{start_date}",
    "end_date": "{end_date}"
  },
  "date_field": "trade_date",
  "primary_keys": ["ts_code", "trade_date"],
  "table_name": "table_name",
  "batch_size": 5000,
  "stock_batch_size": 50,
  "offset": 0,
  "stock_source": {
    "type": "table | api | static",
    "table_name": "stock_basic",
    "code_column": "ts_code",
    "filter": "list_status = 'L'"
  },
  "auto_add_trade_date": true,
  "schema": {}
}
```

### 关键配置字段说明

#### sync_mode (新增)
- `by_date`: 按日期同步，遍历日期，每个日期调用一次 API
- `by_stock`: 按股票同步，遍历股票，每只股票调用一次 API
- `full`: 全量同步，不遍历，直接调用 API

#### batch_size
- 数据库写入的批次大小
- 默认 5000 行
- 防止大数据量导致内存溢出

#### stock_batch_size (新增)
- 按股票同步时，每批处理的股票数量
- 用于控制并发和 API 调用频率

#### stock_source (新增)
- 定义如何获取股票列表
- `type: "table"`: 从数据库表获取
- `table_name`: 股票列表表名
- `code_column`: 股票代码列名
- `filter`: SQL WHERE 条件

#### auto_add_trade_date (新增)
- 全量同步时是否自动添加 trade_date 字段
- 值为当前同步日期
- 用于追踪数据更新时间

### 参数占位符

系统支持以下动态占位符：
- `{date}`: 当前同步日期 (YYYYMMDD)
- `{start_date}`: 开始日期 (YYYYMMDD)
- `{end_date}`: 结束日期 (YYYYMMDD)
- `{stock_code}`: 股票代码 (如 000001.SZ)

## 3. 配置灵活性优势

### 优势 1: 统一的配置格式
- 所有任务使用相同的 JSON 结构
- 通过 `sync_mode` 字段区分不同的同步模式
- 易于理解和维护

### 优势 2: 动态参数生成
- 使用占位符自动替换参数
- 支持日期、股票代码等动态值
- 减少重复配置

### 优势 3: 批量处理控制
- `batch_size`: 控制数据库写入批次
- `stock_batch_size`: 控制股票遍历批次
- `offset`: 支持分页获取大数据

### 优势 4: 灵活的股票列表来源
- 从数据库表获取（最常用）
- 从 API 获取（实时）
- 静态列表（测试用）

### 优势 5: 启用/禁用控制
- `enabled`: 快速启用或禁用任务
- 不需要删除配置
- 便于临时调整

## 4. 使用场景示例

### 场景 1: 每日更新所有股票的基本指标
```json
{
  "task_id": "daily_basic",
  "sync_mode": "by_date",
  "params": {"trade_date": "{date}"}
}
```
- 每天运行一次
- 获取所有股票当天的 PE、PB 等指标

### 场景 2: 补全某只股票的历史数据
```json
{
  "task_id": "daily_history",
  "sync_mode": "by_stock",
  "params": {
    "ts_code": "{stock_code}",
    "start_date": "20200101",
    "end_date": "20241231"
  }
}
```
- 遍历所有股票
- 每只股票获取 2020-2024 年的完整历史

### 场景 3: 每周更新股票基础信息
```json
{
  "task_id": "stock_basic",
  "sync_mode": "full",
  "schedule": "weekly",
  "auto_add_trade_date": true
}
```
- 每周运行一次
- 获取最新的股票列表和基础信息

## 5. 实现要点

### 后端需要实现的功能

1. **sync_mode 路由逻辑**
   ```python
   if sync_mode == "by_date":
       # 遍历日期，每个日期调用一次 API
   elif sync_mode == "by_stock":
       # 遍历股票，每只股票调用一次 API
   elif sync_mode == "full":
       # 直接调用 API，不遍历
   ```

2. **股票列表获取**
   ```python
   def get_stock_list(stock_source):
       if stock_source["type"] == "table":
           return db.query(f"SELECT {code_column} FROM {table_name} WHERE {filter}")
       elif stock_source["type"] == "api":
           return api.stock_basic()
       elif stock_source["type"] == "static":
           return stock_source["codes"]
   ```

3. **批量处理**
   ```python
   for i in range(0, len(stocks), stock_batch_size):
       batch = stocks[i:i+stock_batch_size]
       for stock in batch:
           sync_stock_data(stock)
   ```

## 6. 配置文件位置

- **模板文件**: `backend/data_manager/task_config_template.json`
- **实际配置**: `backend/data_manager/sync_config.json`

## 7. 前端使用

用户可以通过以下方式管理任务：
1. 点击 "New Task" 按钮创建新任务
2. 点击 "Edit" 按钮编辑现有任务
3. 在 JSON 编辑器中修改配置
4. 保存后立即生效

## 8. 总结

新的配置模板提供了：
- ✅ 足够的灵活性：支持三种同步模式
- ✅ 动态参数生成：使用占位符自动替换
- ✅ 批量处理控制：batch_size 和 stock_batch_size
- ✅ 启用/禁用控制：enabled 字段
- ✅ 按日期同步：适合日常增量更新
- ✅ 按股票同步：适合历史数据补全
- ✅ 统一的配置格式：易于理解和维护
