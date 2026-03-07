# 任务配置抽屉 - 快速使用指南

## 功能概述

为数据中心添加了统一的任务配置抽屉，支持同步任务和 ETL 任务的新建、编辑和复制。

## 使用方式

### 同步任务

#### 新建同步任务
1. 进入"数据同步"标签页
2. 点击"新建任务"按钮
3. 填写表单：
   - 任务ID（必填，唯一标识）
   - 描述（必填）
   - 数据表名（必填）
   - 同步类型（增量/全量）
   - 数据源（Tushare/AKShare/自定义）
   - API名称（必填）
   - 字段映射（点击"添加字段"配置）
   - 调度配置（可选）
4. 点击"保存"

#### 编辑同步任务
1. 点击任务ID
2. 修改配置
3. 点击"保存"

#### 复制同步任务
1. 点击任务行的复制按钮
2. 系统自动填充源任务配置
3. 修改任务ID（必须唯一）
4. 调整其他配置
5. 点击"保存"

### ETL 任务

#### 新建 ETL 任务
1. 进入"ETL 任务"标签页
2. 点击"新建任务"按钮
3. 填写表单：
   - 任务ID（必填，唯一标识）
   - 描述（必填）
   - 数据表名（必填）
   - DolphinDB 脚本（必填）
   - 调度配置（可选）
4. 点击"保存"

#### 编辑 ETL 任务
1. 点击任务ID或编辑按钮
2. 修改配置
3. 点击"保存"

#### 复制 ETL 任务
1. 点击任务行的复制按钮
2. 系统自动填充源任务配置
3. 修改任务ID（必须唯一）
4. 调整其他配置
5. 点击"保存"

## 字段说明

### 基础信息

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| 任务ID | 文本 | 是 | 唯一标识，只能包含字母、数字、下划线 |
| 描述 | 文本 | 是 | 任务描述信息 |
| 数据表名 | 文本 | 是 | DolphinDB 表名，只能包含字母、数字、下划线 |

### 同步任务特有字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| 同步类型 | 选择 | 是 | 增量同步或全量同步 |
| 数据源 | 选择 | 是 | Tushare、AKShare 或自定义 |
| API名称 | 文本 | 是 | 数据源 API 接口名称 |
| 字段映射 | 列表 | 否 | 定义数据表字段结构 |

### ETL 任务特有字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| DolphinDB脚本 | 文本域 | 是 | ETL 转换脚本，支持 DolphinDB 语法 |

### 调度配置

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| 启用任务 | 开关 | 否 | 是否启用该任务 |
| 调度周期 | 文本 | 否 | 如：daily, weekly |
| Cron表达式 | 文本 | 否 | 仅同步任务，如：0 9 * * * |

## 字段映射配置

字段映射用于定义同步任务的数据表结构：

### 字段属性

- **字段名**: 数据表中的列名
- **字段类型**: 数据类型（STRING, INT, LONG, DOUBLE, DATE, TIMESTAMP）
- **源字段名**: API 返回数据中的字段名（可选，默认与字段名相同）
- **描述**: 字段说明（可选）

### 示例

```
字段 1:
  字段名: ts_code
  字段类型: STRING
  源字段名: ts_code
  描述: 股票代码

字段 2:
  字段名: trade_date
  字段类型: DATE
  源字段名: trade_date
  描述: 交易日期

字段 3:
  字段名: close
  字段类型: DOUBLE
  源字段名: close
  描述: 收盘价
```

## DolphinDB 脚本示例

### 简单查询
```dolphindb
t = loadTable("dfs://quant_ts", "sync_daily_data")
select * from t where trade_date >= 2024.01.01
```

### 数据转换
```dolphindb
t = loadTable("dfs://quant_ts", "sync_daily_data")
select
  ts_code,
  trade_date,
  close,
  volume,
  close * volume as amount
from t
where trade_date >= 2024.01.01
```

### 聚合计算
```dolphindb
t = loadTable("dfs://quant_ts", "sync_daily_data")
select
  ts_code,
  avg(close) as avg_close,
  sum(volume) as total_volume
from t
where trade_date >= 2024.01.01
group by ts_code
```

## 常见问题

### Q: 任务ID已存在怎么办？
A: 任务ID必须唯一，请使用不同的ID。建议使用有意义的命名，如：`daily_stock_basic`、`etl_ma_indicator`。

### Q: 字段映射是必须的吗？
A: 不是必须的，但建议配置。如果不配置，系统会根据 API 返回的数据自动推断字段类型。

### Q: 如何测试 ETL 脚本？
A: 保存任务后，可以在 ETL 任务列表中点击"回溯"按钮，选择日期范围进行测试。

### Q: 调度配置如何生效？
A: 保存任务后，需要在调度中心启动调度器，任务才会按照配置的周期自动执行。

### Q: 编辑任务会影响历史数据吗？
A: 编辑任务配置不会影响已同步的历史数据，只影响后续的同步行为。

### Q: 复制任务时需要注意什么？
A: 复制任务时，任务ID会被清空，必须输入新的唯一ID。数据表名也建议修改，避免与源任务冲突。

## 技术细节

### 组件位置
- 主组件: `src/pages/DataCenter/TaskDrawer.tsx`
- 集成位置: `src/pages/DataCenter/index.tsx`

### 数据流
```
用户输入 → 表单验证 → Hook处理 → API调用 → 后端保存 → 刷新列表
```

### 状态管理
- 使用 React Hooks 管理组件状态
- 使用自定义 Hooks (useSyncTasks, useETLTasks) 管理业务逻辑

### API 端点
- 创建同步任务: `POST /api/v1/data/sync/tasks`
- 更新同步任务: `PUT /api/v1/data/sync/task/{task_id}/config`
- 创建 ETL 任务: `POST /api/v1/data/etl/tasks`
- 更新 ETL 任务: `PUT /api/v1/data/etl/task/{task_id}`
