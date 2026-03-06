# 量化研究系统 - Bug 报告

> 审查日期: 2026-02-28 | 审查范围: 全栈代码审查

## 概要统计

| 严重程度 | 数量 | 说明 |
|----------|------|------|
| **Critical** | 10 | 系统无法正常工作或存在严重安全漏洞 |
| **High** | 23 | 功能错误或重要安全问题 |
| **Medium** | 36 | 逻辑缺陷、性能问题或次要安全问题 |
| **Low** | 26 | 代码质量、维护性或边缘情况 |
| **总计** | **95** | |

---

## 一、Critical 级别 Bug

### C-01: `trade_cal` 和 `stock_basic` 表未在启动时创建 [已报错]

**文件**: `store/dolphindb_client.py:372-378`
**现象**: 用户日志中报错 `path '/quant_meta/trade_cal.tbl' failed, reason: path does not exist`

**原因**: `_META_TABLE_SCHEMAS` 字典中不包含 `trade_cal` 和 `stock_basic` 的 schema 定义，因此 `ensure_meta_tables()` 不会创建这两个表。但 `TradingCalendar._load()` 在应用启动时就会查询 `trade_cal`，此时表尚未通过数据同步创建。

**修复建议**: 在 `_META_TABLE_SCHEMAS` 中添加 `trade_cal` 和 `stock_basic` 的 schema：
```python
"trade_cal": (
    "table("
    "array(SYMBOL,0) as exchange,"
    "array(STRING,0) as cal_date,"
    "array(INT,0) as is_open,"
    "array(STRING,0) as pretrade_date)"
),
"stock_basic": (
    "table("
    "array(SYMBOL,0) as ts_code,"
    "array(STRING,0) as symbol,"
    "array(STRING,0) as name,"
    "array(STRING,0) as area,"
    "array(STRING,0) as industry,"
    "array(STRING,0) as market,"
    "array(STRING,0) as list_date)"
),
```

---

### C-02: 多处 SQL 注入漏洞 — `data_merged.py` 中 f-string 拼接用户输入

**文件**: `app/api/v1/data_merged.py:437, 489, 534, 544, 547, 566, 868, 902, 935, 944, 952, 955, 973, 993, 1015, 1108, 1192, 1230`

**原因**: 至少 18 处 SQL 查询通过 f-string 直接拼接用户提供的 `task_id`、`table_name` 等值，例如：
```python
f"SELECT * FROM sync_task_config WHERE task_id = '{task_id}'"
```
攻击者可通过构造 `task_id = "foo'; dropTable(...); '"` 注入任意 DolphinDB 脚本。

**修复建议**: 全部改用 `%s` 参数化查询：
```python
db_client.query("SELECT * FROM sync_task_config WHERE task_id = %s", (task_id,))
```

---

### C-03: `/data/query` 端点 SQL 安全检查可被绕过

**文件**: `app/api/v1/data_merged.py:690-712`

**原因**: 通过关键字黑名单检查 `DROP`, `DELETE` 等，但 DolphinDB 函数名 `dropTable(...)` 不在检查范围内，分号分隔的多语句也未被拦截。

**修复建议**: 移除此端点或添加严格的身份验证和白名单机制。

---

### C-04: ETL 脚本端点允许执行任意 DolphinDB 命令

**文件**: `app/api/v1/data_merged.py:1008-1037, 1040-1095`

**原因**: `run_etl_task` 和 `test_etl_script` 直接执行用户提供的 DolphinDB 脚本，无任何验证或沙箱。

**修复建议**: 添加身份验证和授权；限制可执行的操作类型。

---

### C-05: `/production/factors/test` 端点允许任意 Python 代码执行

**文件**: `app/api/v1/production.py:543-743`

**原因**: 使用 `exec(compiled, namespace)` 执行用户提交的 Python 代码，`__builtins__` 完全可用，可执行 `os.system()`、`subprocess` 等。

**修复建议**: 添加强身份验证；限制 `__builtins__` 为安全子集；考虑容器化沙箱。

---

### C-06: 路径遍历漏洞 — 因子/流程文件操作

**文件**: `app/api/v1/production.py:320-324, 400-404, 500-529` 和 `app/api/v1/flows.py:45, 55, 62, 102, 114, 130`

**原因**: `factor_id` 和流程 `name` 用于构建文件路径时未验证，`../../etc/passwd` 样式的路径可读取/写入/删除目标目录外的文件。

**修复建议**: 验证 `factor_id`/`name` 仅包含字母数字、下划线、连字符；检查解析后路径是否在目标目录内。

---

### C-07: 回测引擎 `annualized_return` 使用了错误的指标

**文件**: `engine/backtester/vector_engine.py:125`

**原因**: 使用 `stats.get("Total Return [%]")` 除以 100 作为年化收益率，但这是**总收益率**而非年化收益率。VectorBT 提供独立的 `"Annualized Return [%]"` 键。5年100%总收益的回测会错误报告年化收益为 1.0 而非 ~15%。

**修复建议**:
```python
"annualized_return": self._safe_float(stats.get("Annualized Return [%]", 0)) / 100,
```

---

### C-08: 前端 FlowEditor 节点表单修改不会同步到节点数据

**文件**: `frontend/src/components/FlowEditor/nodes/DataInputNode.tsx:12-34` (所有4个节点组件相同问题)

**原因**: 节点使用 Semi Design `<Form>` 的 `initValue` 但没有 `onChange` 回调将修改写回 ReactFlow 节点 `data`。用户在表单中的修改是纯视觉的，回测时发送的仍是原始数据。

**修复建议**: 每个节点添加 `onValueChange` 回调更新节点 data。

---

### C-09: FlowEditor 本地状态不同步回 Zustand Store

**文件**: `frontend/src/components/FlowEditor/index.tsx:32-33`

**原因**: `useNodesState` 和 `useEdgesState` 从 Zustand Store 获取初始值，但修改后的本地状态从不写回 Store。切换标签页后用户编辑丢失。

**修复建议**: 添加 `useEffect` 将本地状态变更同步回 Store。

---

### C-10: DAGEditor 不响应外部任务变更

**文件**: `frontend/src/components/SchedulerFlowEditor/DAGEditor.tsx:117-118`

**原因**: `useNodesState(initialNodes)` 只使用初始值，后续 `initialNodes` 变更不会更新编辑器节点。

**修复建议**: 添加 `useEffect` 监听 `initialNodes`/`initialEdges` 变更并调用 `setNodes`/`setEdges`。

---

## 二、High 级别 Bug

### H-01: `_escape_value` 自动转换 YYYYMMDD 字符串破坏 STRING 列查询

**文件**: `store/dolphindb_client.py:99-103`

**原因**: 8位数字字符串自动从 `"20240101"` 转换为 `"2024.01.01"`，但 `trade_cal.cal_date`、`sync_log.last_date` 等 STRING 列存储的是 `"20240101"` 格式。参数化查询结果不匹配。

**修复建议**: 移除 `_escape_value` 中的自动日期转换，仅对 Python `date` 对象进行日期格式化。

---

### H-02: RSI 生产因子使用了错误的平滑方法 (SMA 而非 EWM)

**文件**: `engine/production/factors/momentum.py:68-69`

**原因**: 使用 `rolling_mean`（简单移动平均）代替 `ewm_mean`（指数加权移动平均），产生的是 Cutler's RSI 而非标准 Wilder RSI，数值有显著差异。RSI 30/70 阈值策略会产生错误信号。

**修复建议**:
```python
pl.col("gain").ewm_mean(com=w-1, adjust=False).over("ts_code").alias("avg_gain"),
pl.col("loss").ewm_mean(com=w-1, adjust=False).over("ts_code").alias("avg_loss"),
```

---

### H-03: 因子分析分位数分组 off-by-one 错误

**文件**: `engine/analysis/analyzer.py:210-213, 233-236`

**原因**: 使用 `.cast(pl.Int32).clip(0, quantiles-1)` 产生 0-indexed 分位，最低分位组（quantile=0）股票数量远少于其他组，分层分析结果不对称。

**修复建议**:
```python
(... * quantiles).ceil().cast(pl.Int32).clip(1, quantiles).alias("quantile")
```

---

### H-04: `Polars.replace()` API 在 RSI 计算中可能不兼容

**文件**: `engine/factors/technical.py:26`

**原因**: `avg_loss.replace(0, 1e-10)` 在新版 Polars 中 API 签名已变化，且无法处理极小正浮点数。

**修复建议**: 使用 `avg_loss.clip(lower_bound=1e-10)`。

---

### H-05: 信号与收盘价 NaN 处理不一致导致回测异常

**文件**: `engine/backtester/vector_engine.py:93-94`

**原因**: `close_wide` 有 NaN（缺失数据），但 `signal_wide` 用 `fill_value=0` 填充。当某股票无收盘价但信号为有效值时，VectorBT 尝试在 NaN 价格上执行交易。

**修复建议**: `signal_wide = signal_wide.where(close_wide.notna(), other=np.nan)`

---

### H-06: 拓扑排序不检测循环 — 静默丢失节点

**文件**: `engine/parser/flow_parser.py:194-212`

**原因**: Kahn's 算法正确实现但未检查循环。如果图中存在环，环上节点静默跳过，导致部分计算缺失。

**修复建议**:
```python
if len(order) != len(nodes):
    raise ValueError(f"图中存在环: {set(nodes.keys()) - set(order)}")
```

---

### H-07: 生产因子引擎 `_finish_run_record` SQL 注入

**文件**: `engine/production/engine.py:611-618`

**原因**: `error_msg` 来自异常文本，仅转义双引号后直接拼入 DolphinDB 脚本。分号、反引号等特殊字符未处理。

---

### H-08: `sys.modules` 操作非线程安全

**文件**: `app/api/v1/production.py:614-627`

**原因**: 因子测试端点临时替换 `sys.modules` 中的模块引用。并发请求会互相破坏。

---

### H-09: `_job_status` 无限增长的内存泄漏

**文件**: `app/api/v1/ml.py:13`

**原因**: ML 任务状态存储在内存字典中，已完成任务永不清理。

**修复建议**: 添加 TTL 清理机制或使用 LRU 缓存。

---

### H-10: `DataService.get_daily_data` 静默丢弃 end_date 过滤

**文件**: `app/services/data_service.py:56-66`

**原因**: 当同时提供 start_date 和 end_date 时，end_date 被 `pass` 静默忽略。查询返回从 start_date 起的所有数据。

**修复建议**: 实现真正的范围查询支持。

---

### H-11: `QueryBuilder.build_where_clause` SQL 注入

**文件**: `app/core/utils.py:258-268`

**原因**: `f"{key} = '{value}'"` 直接拼接，单引号可注入。

---

### H-12: 模块级 `DolphinDBClient()` 实例化导致导入时失败

**文件**: `store/dolphindb_client.py:1018` 和 `data_manager/refactored_sync_engine.py:123`

**原因**: 模块导入时立即创建数据库连接。如果 DolphinDB 未就绪，整个应用无法启动。

**修复建议**: 使用延迟初始化模式（lazy initialization）。

---

### H-13: 前端 TaskSelector 获取因子列表的响应结构错误

**文件**: `frontend/src/components/SchedulerFlowEditor/TaskSelector.tsx:36`

**原因**: `setFactorTasks(factorRes.data || [])` 应为 `factorRes.data?.data || []`，否则因子复选框不渲染。

---

### H-14: 前端 TaskSelector 切换任务时丢失 depends_on 依赖关系

**文件**: `frontend/src/components/SchedulerFlowEditor/TaskSelector.tsx:49-58`

**原因**: 切换任务时重新构建 `TaskConfig[]`，丢弃已在 DAG 编辑器中设置的依赖关系。

---

### H-15: 前端 Toolbar idCounter 在 HMR 时重置导致重复节点 ID

**文件**: `frontend/src/components/FlowEditor/Toolbar.tsx:9-10`

**修复建议**: 使用 `crypto.randomUUID()` 生成唯一 ID。

---

### H-16: 前端 StrategyCenter 指标显示在 null 值时崩溃

**文件**: `frontend/src/pages/StrategyCenter.tsx:192-196`

**原因**: `(metrics.max_drawdown * 100).toFixed(2)` 在字段为 null 时抛出 TypeError。

**修复建议**: `((metrics.max_drawdown ?? 0) * 100).toFixed(2)`

---

### H-17: 前端 TopBar 缺少 `/market` 路由名称

**文件**: `frontend/src/components/Layout/TopBar.tsx:7-12`

**原因**: `routeNameMap` 未包含 `/market` 映射，行情页面显示错误的面包屑。

---

### H-18: 前端 loadDaily 无错误处理

**文件**: `frontend/src/pages/DataCenter.tsx:488-496`

**原因**: `try/finally` 无 `catch`，API 失败时用户无反馈。

---

### H-19: 前端 StrategyCenter `mlApi.getWeights()` 未处理 Promise 拒绝

**文件**: `frontend/src/pages/StrategyCenter.tsx:22-24`

---

### H-20: 前端 DataCenter `loadInitialData` 无并发限制

**文件**: `frontend/src/pages/DataCenter.tsx:254-258`

**原因**: 对每个任务并行发起 API 请求，无并发限制，可能导致请求风暴。

---

### H-21: 因子分析 Sharpe Ratio 未年化

**文件**: `engine/analysis/analyzer.py:314-316`

**原因**: `mean/std` 未乘以 `sqrt(252)`，日频 Sharpe 值过小。

---

### H-22: `_format_db_analysis` 对所有周期使用相同 IC 值

**文件**: `app/api/v1/production.py:82-89`

**原因**: 循环中对不同周期重复使用相同的 IC 均值/标准差/IR。

---

### H-23: `factor.py` 中 `compute_ic` 始终使用 SMA 而非请求的因子

**文件**: `app/api/v1/factor.py:76-77`

**原因**: 无论请求什么因子，IC 计算始终基于 20 日 SMA。

---

## 三、Medium 级别 Bug（摘要）

| ID | 文件 | 问题描述 |
|----|------|----------|
| M-01 | `main.py:42-48` | CORS 通配符 + credentials 配置冲突 |
| M-02 | `data_merged.py:444-457` | `update_task_config` 用空字符串覆盖缺失字段 |
| M-03 | `data_merged.py:222-227` | `get_sync_status` 日期过滤未转义 |
| M-04 | `data_merged.py:471,424,856` | 端点接受原始 dict 而非 Pydantic Model |
| M-05 | `config.py:92-97` | Pydantic validator 中创建目录的副作用 |
| M-06 | `exceptions.py:282` | loguru logger.level 检查方式错误，debug 详情永不暴露 |
| M-07 | `utils.py:65` | 指数退避首次重试延迟不正确 (1秒而非2秒) |
| M-08 | `data_service.py:136-158` | `load_stock_data` 违反 IDataRepository 接口 |
| M-09 | `ml.py:26,44,47,59` | `_job_status` 非线程安全 |
| M-10 | `flows.py:49` | yaml.safe_load 对空文件返回 None |
| M-11 | `production/engine.py:585` | 借用 `error_message` 列存 `run_id` |
| M-12 | `production/engine.py:304-305` | 无条件删除 `adj_factor` 列 |
| M-13 | `production/engine.py:516-517` | `_update_metadata` 未参数化 factor_id |
| M-14 | `sync_components.py:405-406` | 增量同步遍历所有日历日而非交易日 |
| M-15 | `sync_components.py:226-229` | `addColumn` DolphinDB 语法错误 |
| M-16 | `dolphindb_client.py:540` | `addColumn` 缺少数组语法 |
| M-17 | `dolphindb_client.py:138-140` | 函数名大小写转换不完整 |
| M-18 | `dolphindb_client.py:60-67` | `_ensure_connected` 部分代码路径缺少锁 |
| M-19 | `akshare_collector.py:44,46` | ts_code 格式缺少交易所后缀 |
| M-20 | `data_sync_flow.py:127-134` | `sync_all_data` 忽略 end_date 参数 |
| M-21 | `technical.py:67-68` | ATR 使用 Python 循环而非向量化操作 |
| M-22 | `base.py:10` | 类级别可变默认参数 |
| M-23 | `financial.py` | 文件内容为 FactorAnalyzer 副本而非财务因子 |
| M-24 | `flow_parser.py:162-191` | 信号解析器不支持复合条件 |
| M-25 | `flow_parser.py:183` | 未识别运算符静默变为"全买" |
| M-26 | `momentum.py:112` | 波动率因子硬编码窗口忽略 params |
| M-27 | `factor_custom_01.py:17` | 除零产生 inf 未过滤 |
| M-28 | `analyzer.py:246-260` | 换手率计算 O(N) Python 循环 |
| M-29 | `analyzer.py:160-183` | IC 序列计算 O(N) Python 循环 |
| M-30 | `analyzer.py:322-326` | 多空收益依赖错误的分位索引 |
| M-31 | `KLineChart.tsx:28` | 硬编码深色背景不适配浅色主题 |
| M-32 | `MarketCenter.tsx:17-36` | 错误不显示给用户 |
| M-33 | `indicators.ts:176-177` | RSI 在 avgGain=avgLoss=0 时返回 NaN |
| M-34 | `DataCenter.tsx:1295` | 用 JSON.stringify 生成 rowKey 不可靠 |
| M-35 | `FactorCenter.tsx:88-99` | 输入框每次按键触发 API 请求 |
| M-36 | `TradingViewChart.tsx:131-157` | 子图表清理逻辑不对称 |

---

## 四、Low 级别 Bug（摘要）

| ID | 文件 | 问题描述 |
|----|------|----------|
| L-01 | `main.py:23-24` | 关闭时未释放 DB 连接 |
| L-02 | `data_merged.py:765-767` | 修改 frozenset 非线程安全 |
| L-03 | `data_merged.py:624-629` | truncate_table 系统表保护不完整 |
| L-04 | `factor.py:44-58` | 未匹配因子类型静默跳过 |
| L-05 | `factor.py:39,71` | end_date 默认 "99991231" 无效日期 |
| L-06 | `ml.py:54` | UUID 截断至 8 字符有碰撞风险 |
| L-07 | `logger.py:11` | 日志文件使用相对路径 |
| L-08 | `utils.py:180-183` | TradingCalendar 单例非线程安全 |
| L-09 | `production/engine.py:202` | 增量计算用日历日而非交易日 |
| L-10 | `production/engine.py:327` | 访问 DB 客户端私有属性 |
| L-11 | `registry.py:37-41` | params 引用可被外部修改 |
| L-12 | `financial.py:63-72` | 相关性矩阵缺少行标签列 |
| L-13 | `vector_engine.py:63-68` | 空头信号从未由上游生成 |
| L-14 | `vector_engine.py:146-147` | 多组合 sum(axis=1) 不正确 |
| L-15 | `tushare_collector.py:23` | 首次 API 调用前不必要的 sleep |
| L-16 | `sync_components.py:357-359` | 空数据返回 False 可能为有效情况 |
| L-17 | `optimizer.py:36` | 每次 trial 创建新 VectorEngine |
| L-18 | `dolphindb_client.py:1004-1014` | close() 后调用方法无法重连 |
| L-19 | `dolphindb_client.py:142-156` | 不处理 DELETE/INSERT/UPDATE 的表名 |
| L-20 | `App.tsx:14-19` | 缺少 404 catch-all 路由 |
| L-21 | `index.tsx:11` | 未使用 React.StrictMode |
| L-22 | `App.tsx:9 + index.tsx:4` | CSS 重复导入 |
| L-23 | `DataCenter.tsx:82-157` | 30+ 个 useState 声明难以维护 |
| L-24 | `KLineChart.tsx:23` | 买卖点标记可能含 undefined 坐标 |
| L-25 | `api/index.ts:56-57` | API 方法大量使用 any 类型 |
| L-26 | `SchedulerCenter.tsx:241-244` | iframe 选择器过于脆弱 |

---

## 五、优先修复建议

### 立即修复 (P0)

1. **C-01**: 添加 `trade_cal`/`stock_basic` schema → 解决用户报告的启动错误
2. **C-02**: 全面改用参数化查询 → 消除 SQL 注入漏洞
3. **C-05/C-04**: 添加身份验证和沙箱 → 消除远程代码执行风险
4. **C-07**: 修正年化收益率指标 → 回测结果不可信

### 短期修复 (P1)

5. **H-02**: RSI 因子改用 EWM → 因子值计算错误
6. **H-03**: 分位数分组修正 → 因子分析结果不准确
7. **H-01**: 移除日期自动转换 → 查询结果不匹配
8. **H-10**: 实现 end_date 过滤 → 数据查询范围错误
9. **C-08/C-09/C-10**: 修复前端 FlowEditor 数据同步 → 策略编辑器不可用

### 中期改进 (P2)

10. **H-12**: 延迟初始化 DB 客户端 → 提高启动可靠性
11. **M-28/M-29**: 向量化 IC/换手率计算 → 性能优化
12. **H-06**: 拓扑排序添加环检测 → 防止静默数据丢失
13. 全面添加身份验证中间件 → 所有危险端点需要授权
