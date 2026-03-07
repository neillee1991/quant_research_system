# QA 报告

> 日期: 2026-03-07

## Bug 验证结果

| Bug ID | 文件 | 状态 | 说明 |
|--------|------|------|------|
| C-02 | `app/api/v1/data_merged.py` | **已确认** | 存在 11 处 f-string SQL 拼接，其中 3 处高危：第 680 行 `task_id` 直接插入 SELECT，第 735 行 `task_id` 直接插入 DELETE，第 1133 行 `task_id` 直接插入 DELETE。其余 8 处拼接表名/字段名（来自内部配置，风险较低但仍不规范）。 |
| C-07 | `engine/backtester/vector_engine.py` | **已确认** | 第 125 行 `annualized_return` 使用 `stats.get("Total Return [%]", 0)` 而非 `"Annualized Return [%]"`，导致年化收益率实为总收益率。 |
| H-01 | `store/dolphindb_client.py` | **已确认** | `_escape_value` 第 102-104 行：8 位纯数字字符串自动转换为 `YYYY.MM.DD` 日期格式（不加引号），导致 STRING 类型列的查询类型不匹配。 |
| H-02 | `engine/factors/technical.py` | **已修复** | RSI 第 24-25 行已使用 `ewm_mean(span=window, adjust=False)`，即 Wilder EWM 方法，非 SMA。 |
| H-03 | `engine/analysis/analyzer.py` | **已修复** | `_calc_quantile_returns` 第 258-261 行已使用 `(rank / count * quantiles).ceil().clip(1, quantiles)`，分组从 1 开始，最大值等于 quantiles。 |
| H-03-label | `engine/analysis/analyzer.py` | **已确认（新发现）** | `_build_summary` 第 358 行：`f"Q{int(r['quantile'])+1}"` 对已经是 1-based 的分组再 +1，导致标签偏移（Q1→Q2, Q5→Q6）。 |
| H-10 | `app/services/data_service.py` | **已修复** | `get_daily_data` 第 75-77 行已正确处理 `end_date`，生成 `trade_date <= %s` 条件并传入参数。 |
| H-21 | `engine/analysis/analyzer.py` | **已确认** | `_build_summary` 第 362 行 Sharpe 计算为 `avg_return / std_return`，缺少年化因子 `sqrt(252)`。 |
| flows.py 路径遍历 | `app/api/v1/flows.py` | **已修复** | 第 18-23 行已有 `_SAFE_NAME_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')` 正则验证，`_validate_flow_name` 在所有路径操作前调用。 |
| production.py 路径遍历 | `app/api/v1/production.py` | **已修复** | 第 26-31 行已有 `_SAFE_FACTOR_ID_RE` 正则验证，`_validate_factor_id` 在文件路径操作前调用。 |
| engine.py error_msg | `engine/production/engine.py` | **已修复** | `_finish_run_record` 第 865-871 行对 `error_msg` 做了 `replace("\\", "\\\\")`, `replace('"', '\\"')`, `replace(";", "")`, `replace("`", "")` 及 `[:500]` 截断处理。 |
| utils.py QueryBuilder | `app/core/utils.py` | **已确认（安全风险）** | `build_where_clause` 第 284 行直接插入字符串值 `f"{key} = '{value}'"` 不转义单引号，存在 SQL 注入风险（但该方法在当前代码中使用场景有限）。 |

## 测试覆盖

| 测试文件 | 新增测试数 | 覆盖的功能 |
|----------|-----------|-----------|
| `tests/test_technical_factors.py` | 10 | RSI EWM vs SMA 验证、RSI 已知值（全涨/全跌）、ATR 首根 K 线、ATR 跳空 gap、ATR 非负、Bollinger 中轨=SMA、Bollinger 带宽=2σ、Bollinger 正态分布覆盖率 |
| `tests/test_analyzer.py` | 17 | 分位数从 1 开始、最大值=quantiles、均匀分布、clip 防溢出、排序单调性；Sharpe 年化公式、正收益为正、零标准差、年化因子验证、H-21 bug 确认；IC 范围 [-1,1]、完全正相关=1、完全负相关=-1、不相关≈0、对称性、最小样本阈值 |
| `tests/test_security.py` | 16 | factor_id 路径遍历拒绝/接受、flow name 路径遍历拒绝/接受、HTTPException 验证；双引号转义、反斜杠转义、SQL 注入转义、None→NULL、H-01 YYYYMMDD 自动转换 bug 确认；QueryBuilder 字符串引号、NULL、IN 子句、特殊字符不转义（安全风险）、整数不引号、空过滤器、AND 连接 |
| `tests/test_data_service.py` | 10 | end_date 包含在 SQL 条件中、end_date 值传入参数、仅 start_date 无 end_date 条件、两个日期条件同时存在、无日期无过滤、无效 start_date 抛异常、无效 end_date 抛异常、limit 应用、空结果抛 DataNotFoundError、日期范围 SQL 构造、ts_code+日期组合过滤、YYYYMMDD 格式验证 |

## 发现的新问题

### 新问题 1：分位数标签 off-by-one（`analyzer.py` 第 358 行）

`_build_summary` 中对已经是 1-based（1~quantiles）的分组值再 `+1`：

```python
"quantile": f"Q{int(r['quantile'])+1}",  # 错误：Q1→Q2, Q5→Q6
```

正确应为：

```python
"quantile": f"Q{int(r['quantile'])}",
```

同时，多空收益计算（第 372 行）`returns_by_q.get(0, 0)` 取 Q0 的收益，但分组从 1 开始，Q0 永远不存在，导致 `short_ret` 始终为 0，多空收益等于最高分组收益。

### 新问题 2：`data_merged.py` 第 147 行 LIMIT 硬编码

```python
sql = f"SELECT * FROM sync_daily_data WHERE {where} ORDER BY trade_date DESC LIMIT {limit}"
```

`limit` 变量来自外部参数，虽然是整数类型，但未经过 `MAX_QUERY_LIMIT` 上限约束（与 `data_service.py` 中的处理不一致）。

### 新问题 3：`data_merged.py` 第 88/93 行 f-string 拼接表名

```python
f"SELECT task_id FROM {config_table} WHERE table_name = %s AND task_id != %s"
```

`config_table` 来自内部配置（非用户输入），风险较低，但不符合参数化查询规范。

### 新问题 4：`engine/production/engine.py` `_delete_factor_dates` 中 `factor_id` 直接插入 DolphinDB 脚本

第 679 行：

```python
f'delete from pt where factor_id = "{factor_id}" and trade_date in {dates_vec}'
```

`factor_id` 在调用前已经过 `_validate_factor_id` 正则验证（仅允许 `[a-zA-Z0-9_\-]`），风险已缓解，但建议统一使用参数化方式。
