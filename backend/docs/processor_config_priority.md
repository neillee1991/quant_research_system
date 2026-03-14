# 因子计算处理器配置优先级说明

## 概述

因子计算管道中的三个核心处理器（DataLoaderProcessor、AdjustmentProcessor、StatusFilterProcessor）现在都支持**用户配置优先，系统默认兜底**的策略。

---

## 1. DataLoaderProcessor（数据加载处理器）

### 配置优先级

```
用户配置（factor_data_config 表） > 系统内置配置（BUILTIN_DATA_SOURCES）
```

### 逻辑流程

```python
for dep in definition.depends_on:
    # 1. 优先查找用户配置
    if factor_data_config 表中有 dep 的配置:
        使用用户配置的 table_name 和 column_name
        logger.info(f"Loaded from {dep} (user config)")

    # 2. Fallback 到系统内置配置
    elif dep in BUILTIN_DATA_SOURCES:
        使用内置配置的 table_name 和 columns
        logger.info(f"Loaded from {dep} (builtin config)")

    # 3. 都没有配置
    else:
        logger.warning(f"Data source '{dep}' not configured")
        跳过此依赖
```

### 配置示例

**用户配置（factor_data_config 表）：**
```sql
INSERT INTO factor_data_config VALUES (
    'sync_daily_data',
    '自定义日线数据',
    'my_custom_daily_table',  -- 用户自定义表名
    'open,high,low,close',
    '{}',
    now()
);
```

**系统内置配置（BUILTIN_DATA_SOURCES）：**
```python
BUILTIN_DATA_SOURCES = {
    "sync_daily_data": {
        "table_name": "sync_daily_data",
        "columns": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"],
    },
    ...
}
```

---

## 2. AdjustmentProcessor（复权处理器）

### 配置优先级

```
用户配置（factor_data_config 表的 adj_factor） > 系统默认（sync_adj_factor 表）
```

### 逻辑流程

```python
# 1. 优先使用用户配置
adj_config = factor_data_config.get("adj_factor")
if adj_config and adj_config.get("table_name"):
    table_name = adj_config["table_name"]
    column_name = adj_config.get("column_name", "adj_factor")
    logger.debug(f"Loading adj_factor from user config: {table_name}")

# 2. 使用系统默认
else:
    table_name = "sync_adj_factor"
    column_name = "adj_factor"
    logger.debug(f"Loading adj_factor from builtin config")
```

### 配置示例

**用户配置（factor_data_config 表）：**
```sql
INSERT INTO factor_data_config VALUES (
    'adj_factor',
    '自定义复权因子',
    'my_adj_factor_table',  -- 用户自定义表名
    'my_adj_column',        -- 用户自定义列名
    '{}',
    now()
);
```

**系统默认：**
- 表名：`sync_adj_factor`
- 列名：`adj_factor`

---

## 3. StatusFilterProcessor（状态过滤处理器）

### 配置优先级

```
用户配置（factor_data_config 表） > 无配置时跳过过滤
```

### 逻辑流程

```python
# 从 factor_data_config 加载状态字段配置
field_configs = {
    "is_st": config.get("is_st"),
    "is_suspend": config.get("is_suspend"),
    "is_limit": config.get("is_limit"),
}

# 按表分组加载
for field_key, cfg in field_configs.items():
    if cfg and cfg.get("table_name"):
        # 使用用户配置加载状态数据
        加载并过滤
    else:
        # 没有配置则跳过此字段的过滤
        logger.debug(f"Skipping {field_key}: not configured")
```

### 配置示例

**用户配置（factor_data_config 表）：**
```sql
-- ST 状态配置
INSERT INTO factor_data_config VALUES (
    'is_st',
    'ST 股票标记',
    'sync_stock_st',
    'ts_code',
    '{"mode":"exists_in_table","values":{"0":"正常","1":"ST"}}',
    now()
);

-- 停牌状态配置
INSERT INTO factor_data_config VALUES (
    'is_suspend',
    '停牌状态',
    'sync_suspend_d',
    'suspend_type',
    '{"mode":"exists_in_table","filter":{"suspend_type":"S"}}',
    now()
);

-- 涨跌停状态配置
INSERT INTO factor_data_config VALUES (
    'is_limit',
    '涨跌停状态',
    'sync_stk_limit',
    'up_limit,down_limit',
    '{"mode":"compare_with_price","price_table":"sync_daily_data","price_column":"close"}',
    now()
);
```

---

## 预处理选项的统一优先级

所有处理器的**行为控制**（是否执行、如何执行）都通过 `preprocess_options` 控制，遵循以下优先级：

```
1. API 显式传入 (最高优先级)
   ↓
2. Profile 配置 (preprocess_config.yaml)
   ↓
3. 数据库配置 (factor_metadata.params.preprocess)
   ↓
4. 代码定义 (@factor 装饰器的 params.preprocess)
   ↓
5. 默认配置 (preprocess_config.yaml 的 default profile)
```

### 示例

```python
# 1. API 调用时显式传入（优先级最高）
compute_factor(
    factor_id="ma_5",
    preprocess={
        "adjust_price": "forward",  # 覆盖所有其他配置
        "filter_st": True
    }
)

# 2. Profile 配置
preprocess_config.yaml:
  aggressive:
    adjust_price: "forward"
    filter_st: true
    filter_new_stock: true

# 3. 数据库配置
factor_metadata.params = {
    "preprocess": {
        "adjust_price": "backward"
    }
}

# 4. 代码定义
@factor(
    params={
        "preprocess": {
            "adjust_price": "forward"
        }
    }
)

# 5. 默认配置
preprocess_config.yaml:
  default:
    adjust_price: "none"
    filter_st: false
```

---

## 总结表

| 处理器 | 数据源配置 | 行为控制 | 用户可自定义 |
|--------|-----------|---------|-------------|
| **DataLoaderProcessor** | 用户配置 > 内置配置 | depends_on（因子定义） | ✅ 完全支持 |
| **AdjustmentProcessor** | 用户配置 > 系统默认 | preprocess_options.adjust_price | ✅ 完全支持 |
| **StatusFilterProcessor** | 用户配置（必需） | preprocess_options.filter_* | ✅ 完全支持 |

---

## 配置建议

### 1. 使用系统默认（推荐新手）

不需要配置 `factor_data_config` 表，系统会自动使用内置配置：
- 主数据源：`sync_daily_data`, `sync_daily_basic`, `sync_adj_factor`
- 复权因子：`sync_adj_factor`

### 2. 自定义数据源（高级用户）

在 `factor_data_config` 表中配置自定义数据源：
```sql
-- 自定义日线数据表
INSERT INTO factor_data_config VALUES (
    'sync_daily_data',
    '自定义日线数据',
    'my_daily_data',
    'open,high,low,close,volume',
    '{}',
    now()
);

-- 自定义复权因子表
INSERT INTO factor_data_config VALUES (
    'adj_factor',
    '自定义复权因子',
    'my_adj_factor',
    'adj_factor',
    '{}',
    now()
);
```

### 3. 混合使用

可以只配置部分数据源，其他使用系统默认：
```sql
-- 只自定义 ST 状态，其他使用默认
INSERT INTO factor_data_config VALUES (
    'is_st',
    '自定义 ST 标记',
    'my_st_table',
    'is_st_flag',
    '{}',
    now()
);
```

---

## 日志识别

修改后的代码会在日志中明确标识数据来源：

```
# 用户配置
INFO: Loaded 333182 rows from sync_daily_data (user config)
INFO: Loading adj_factor from user config: my_adj_factor

# 系统默认
INFO: Loaded 333182 rows from sync_daily_data (builtin config)
INFO: Loading adj_factor from builtin config: sync_adj_factor

# 未配置
WARNING: Data source 'custom_field' not configured (not in user config or builtin)
```

---

## 修改历史

- **2026-03-14**: 重构配置优先级，改为用户配置优先，系统默认兜底
- **之前**: 内置配置优先（已废弃）
