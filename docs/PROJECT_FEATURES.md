# 量化研究系统 - 功能文档

> 版本 2.0.0 | 更新日期: 2026-02-28

## 系统概述

量化研究系统是一个全栈量化交易研究平台，采用 Python 3.11 后端 (FastAPI) + React 18 前端 (TypeScript) 架构，集成了数据同步、因子计算、策略回测、自动机器学习和任务调度等核心功能。

## 系统架构

```
┌─────────────────────────────────────────┐
│  前端 (React 18 + TypeScript)           │
│  - 数据中心 / 因子中心 / 策略中心       │
│  - 调度中心 / 行情中心                  │
│  - 可视化编辑器 / 图表组件              │
└──────────────┬──────────────────────────┘
               │ HTTP API
┌──────────────▼──────────────────────────┐
│  FastAPI 后端 (Python 3.11)             │
│  ├─ API 路由 (/api/v1/*)               │
│  ├─ 服务层 (业务逻辑)                   │
│  ├─ 引擎模块 (因子/回测/分析/解析)      │
│  └─ 存储层 (DolphinDB 客户端)           │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  数据层 & 调度层                        │
│  ├─ DolphinDB 时序数据库 (TSDB)        │
│  ├─ Prefect 3.x (流程调度)              │
│  └─ 数据管理器 (同步引擎)               │
└─────────────────────────────────────────┘
```

---

## 一、数据管理模块

### 1.1 数据同步引擎

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| 数据库驱动的任务配置 | 同步任务定义存储在 DolphinDB `sync_task_config` 表中 | `data_manager/sync_components.py` |
| 增量同步 | 基于 `sync_log` 记录的最后同步日期，按日逐步拉取新数据 | `sync_components.py:SyncTaskExecutor` |
| 全量同步 | 一次性获取全部历史数据（如 `stock_basic`、`trade_cal`） | `sync_components.py:_execute_full_sync` |
| 自动表创建 | 根据 API 返回数据自动推断 schema 并创建 DolphinDB 表 | `sync_components.py:TableManager` |
| 频率限制 | Tushare API 调用频率控制（默认 120 次/分钟） | `sync_components.py:TushareAPIClient` |
| 同步日志追踪 | `sync_log` 记录当前状态，`sync_log_history` 记录历史审计 | `sync_components.py:SyncLogManager` |

### 1.2 数据源

| 数据源 | 用途 | 集成方式 |
|--------|------|----------|
| Tushare Pro | A 股行情、财务、基本面数据 | Python SDK + 频率限制 |
| AkShare | 替代数据源 | Python SDK |

### 1.3 ETL 管理

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| ETL 任务配置 | 自定义 DolphinDB 脚本任务 | `app/api/v1/data_merged.py` |
| 脚本测试 | 测试 ETL 脚本执行结果 | `data_merged.py:test_etl_script` |
| 执行记录 | ETL 任务执行日志和结果追踪 | `data_merged.py:run_etl_task` |

### 1.4 数据库表结构

**TSDB 表 (dfs://quant_research)**：按月 + 股票代码 COMPO 分区

| 表名 | 用途 | 关键字段 |
|------|------|----------|
| `daily_data` | 日线 OHLCV | ts_code, trade_date |
| `daily_basic` | 每日估值指标 | ts_code, trade_date |
| `adj_factor` | 复权因子 | ts_code, trade_date |
| `index_daily` | 指数日线 | ts_code, trade_date |
| `moneyflow` | 资金流向 | ts_code, trade_date |
| `factor_values` | 因子值存储 | factor_id, trade_date |

**元数据表 (dfs://quant_meta)**：维度表

| 表名 | 用途 |
|------|------|
| `stock_basic` | 股票基本信息 |
| `trade_cal` | 交易日历 |
| `sync_log` | 同步状态 |
| `sync_log_history` | 同步历史 |
| `sync_task_config` | 同步任务配置 |
| `etl_task_config` | ETL 任务配置 |
| `factor_metadata` | 因子元数据 |
| `factor_analysis` | 因子分析结果 |
| `production_task_run` | 生产因子运行记录 |

---

## 二、因子引擎模块

### 2.1 技术因子

| 因子 | 函数 | 参数 | 相关文件 |
|------|------|------|----------|
| 简单移动平均 (SMA) | `TechnicalFactors.sma()` | window | `engine/factors/technical.py` |
| 指数移动平均 (EMA) | `TechnicalFactors.ema()` | window, alpha | `technical.py` |
| 相对强弱指标 (RSI) | `TechnicalFactors.rsi()` | window (默认14) | `technical.py` |
| MACD | `TechnicalFactors.macd()` | fast, slow, signal | `technical.py` |
| KDJ 随机指标 | `TechnicalFactors.kdj()` | n, m1, m2 | `technical.py` |
| 布林带 | `TechnicalFactors.bollinger()` | window, num_std | `technical.py` |
| ATR 真实波幅 | `TechnicalFactors.atr()` | window | `technical.py` |

### 2.2 截面因子操作

| 操作 | 说明 | 相关文件 |
|------|------|----------|
| 截面排名 | `rank().over("trade_date")` | `technical.py` |
| Z-Score 标准化 | `(x - mean) / std` | `technical.py` |
| 中性化 | 行业/市值中性化处理 | `technical.py` |

### 2.3 生产因子框架

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| `@factor` 装饰器注册 | 声明式因子注册，定义依赖、参数、描述 | `engine/production/registry.py` |
| 增量/全量计算 | 支持增量更新和全量回算两种模式 | `engine/production/engine.py` |
| 股票过滤 | 自动过滤 ST、新上市、停牌股票 | `engine.py:_filter_special_stocks` |
| 复权处理 | 支持前复权/后复权 | `engine.py:_apply_adjust` |
| 运行追踪 | 每次计算记录 run_id、状态、耗时 | `engine.py:_finish_run_record` |

**已注册生产因子**：

| 因子 ID | 类型 | 描述 | 文件 |
|---------|------|------|------|
| `factor_ma_5/10/20/60` | 动量 | 移动平均线 | `production/factors/momentum.py` |
| `factor_rsi_14` | 动量 | RSI 指标 | `momentum.py` |
| `factor_volatility_20` | 波动率 | 20日波动率 | `momentum.py` |
| `factor_volatility_10` | 波动率 | 10日波动率 | `production/factors/factor_volatility_10.py` |
| `factor_ep` | 价值 | 盈利收益率 (1/PE) | `production/factors/value.py` |
| `factor_bp` | 价值 | 账面市值比 (1/PB) | `value.py` |
| `factor_custom_01` | 自定义 | 自定义因子 | `production/factors/factor_custom_01.py` |

### 2.4 因子分析

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| IC/Rank IC 计算 | 信息系数和排名信息系数 | `engine/analysis/analyzer.py` |
| IC 时间序列 | 每日 IC 值序列 | `analyzer.py:_calc_ic_series` |
| 分层收益分析 | N 分位数组合收益对比 | `analyzer.py:_calc_quantile_returns` |
| 换手率计算 | 分组持仓变化率 | `analyzer.py:_calc_turnover` |
| 多空组合 | 最高/最低分位组合收益差 | `analyzer.py:_build_summary` |
| 持久化存储 | 分析结果写入 `factor_analysis` 表 | `analyzer.py:analyze` |

---

## 三、策略回测模块

### 3.1 可视化策略构建

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| 拖拽式编辑器 | 基于 React Flow 的可视化策略编辑 | `components/FlowEditor/` |
| 数据输入节点 | 选择股票、日期范围 | `FlowEditor/nodes/DataInputNode.tsx` |
| 算子节点 | 技术指标、截面变换操作 | `FlowEditor/nodes/OperatorNode.tsx` |
| 信号节点 | 买卖条件判断 | `FlowEditor/nodes/SignalNode.tsx` |
| 回测输出节点 | 回测参数配置 | `FlowEditor/nodes/BacktestOutputNode.tsx` |

### 3.2 DSL 解析器

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| JSON → 计算链 | React Flow 图转可执行计算链 | `engine/parser/flow_parser.py` |
| 拓扑排序 | DAG 节点依赖排序 | `flow_parser.py:_topo_sort` |
| 算子映射 | 节点类型映射到计算函数 | `flow_parser.py:_apply_operator` |
| 信号生成 | 条件表达式 → 二值信号 | `flow_parser.py:_apply_signal` |

### 3.3 向量化回测引擎

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| VectorBT 引擎 | 基于 VectorBT 的向量化回测 | `engine/backtester/vector_engine.py` |
| 多资产组合 | 支持多股票组合回测 | `vector_engine.py` |
| 性能指标 | Sharpe、最大回撤、胜率、盈亏比 | `vector_engine.py:_extract_metrics` |
| 交易成本 | 佣金和滑点建模 | `vector_engine.py` |

---

## 四、机器学习模块

### 4.1 AutoML 训练

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| PyCaret 集成 | 自动模型选择和比较 | `ml_module/trainer.py` |
| 分类预测 | 信号方向预测 | `trainer.py` |
| 回归预测 | 收益率预测 | `trainer.py` |
| 后台训练 | 异步后台任务执行 | `app/api/v1/ml.py` |

### 4.2 超参优化

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| Optuna 集成 | 贝叶斯超参搜索 | `ml_module/optimizer.py` |
| 多目标优化 | 收益 + 风险联合优化 | `optimizer.py` |
| 模型持久化 | 训练模型保存和加载 | `ml_module/pipeline.py` |

---

## 五、任务调度模块

### 5.1 Prefect 工作流

| 流程 | 触发方式 | 说明 | 相关文件 |
|------|----------|------|----------|
| `daily-data-sync` | 每日 | 并行数据同步 → 因子计算 | `flows/data_sync_flow.py` |
| `weekly-analysis` | 每周 | 因子分析 + 数据更新 | `data_sync_flow.py` |
| `full-data-sync` | 手动 | 历史数据回补 | `data_sync_flow.py` |
| `single-task-sync` | 手动 | 单任务按需同步 | `data_sync_flow.py` |

### 5.2 DAG 可视化编辑

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| DAG 编辑器 | 拖拽式 Prefect DAG 构建 | `components/SchedulerFlowEditor/DAGEditor.tsx` |
| 任务选择器 | 选择同步/因子计算任务 | `SchedulerFlowEditor/TaskSelector.tsx` |
| Cron 调度 | Cron 表达式配置定时执行 | `pages/SchedulerCenter.tsx` |
| Prefect 仪表盘 | 嵌入 Prefect UI 监控 | `SchedulerCenter.tsx` |

---

## 六、前端页面模块

### 6.1 数据中心 (DataCenter)

| 功能 | 说明 |
|------|------|
| 同步任务管理 | 查看/创建/编辑/删除同步任务 |
| 手动同步触发 | 单任务或批量触发数据同步 |
| 历史数据回补 | 指定日期范围的数据补录 |
| 数据表查看 | 查看数据库表统计和数据预览 |
| SQL 查询 | 自定义 SQL 查询执行 |
| ETL 任务管理 | 创建和管理 ETL 脚本任务 |

### 6.2 因子中心 (FactorCenter)

| 功能 | 说明 |
|------|------|
| 因子计算 | 选择因子类型和参数进行计算 |
| 因子数据查看 | 查看计算后的因子值 |
| 因子分析 | IC/IR 分析、分层收益、换手率 |
| 生产因子管理 | 注册/编辑/删除生产因子 |
| 因子代码编辑 | 在线编辑因子 Python 代码 |

### 6.3 策略中心 (StrategyCenter)

| 功能 | 说明 |
|------|------|
| 可视化策略编辑 | React Flow 拖拽构建策略图 |
| 策略回测 | 提交策略图执行回测 |
| 权益曲线 | ECharts 展示回测权益曲线 |
| 回测指标 | Sharpe、最大回撤、胜率等 |

### 6.4 调度中心 (SchedulerCenter)

| 功能 | 说明 |
|------|------|
| 流程管理 | 创建/编辑/删除 Prefect 流程 |
| DAG 编辑 | 可视化编辑任务依赖关系 |
| 运行历史 | 查看流程执行记录和状态 |
| Prefect 仪表盘 | 嵌入式 Prefect UI |

### 6.5 行情中心 (MarketCenter)

| 功能 | 说明 |
|------|------|
| 股票选择 | 下拉选择股票查看行情 |
| K 线图表 | TradingView 风格 K 线图 |
| 技术指标 | MA/EMA/RSI/MACD/KDJ 叠加 |

---

## 七、基础设施模块

### 7.1 DolphinDB 客户端

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| 单例连接池 | 线程安全的单例模式 | `store/dolphindb_client.py` |
| SQL 语法适配 | 标准 SQL → DolphinDB 语法转换 | `dolphindb_client.py:_adapt_sql_syntax` |
| 参数化查询 | `%s` 占位符防 SQL 注入 | `dolphindb_client.py:_substitute_params` |
| 自动表名解析 | 裸表名 → `loadTable()` 调用 | `dolphindb_client.py:_resolve_table_names` |
| Upsert 操作 | 主键去重写入 | `dolphindb_client.py:upsert` |
| 元数据表管理 | 自动创建和维护系统表 | `dolphindb_client.py:ensure_meta_tables` |

### 7.2 配置管理

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| Pydantic Settings | 环境变量驱动的配置 | `app/core/config.py` |
| 嵌套配置 | 支持 `DOLPHINDB__PASSWORD` 嵌套命名 | `config.py` |
| 自动目录创建 | 启动时自动创建必要目录 | `config.py` |

### 7.3 日志和异常处理

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| Loguru 日志 | 结构化日志输出 | `app/core/logger.py` |
| 自定义异常 | 分层异常体系 | `app/core/exceptions.py` |
| 全局异常处理 | FastAPI 异常中间件 | `exceptions.py` |

### 7.4 工具类

| 功能 | 说明 | 相关文件 |
|------|------|----------|
| 频率限制器 | 通用 API 调用频率控制 | `app/core/utils.py:RateLimiter` |
| 重试策略 | 指数退避重试 | `utils.py:RetryPolicy` |
| 日期工具 | 日期范围生成和转换 | `utils.py:DateUtils` |
| 交易日历 | 交易日判断和偏移 | `utils.py:TradingCalendar` |
| SQL 构建器 | SQL 查询构建辅助 | `utils.py:QueryBuilder` |

---

## 八、部署架构

### 8.1 Docker 服务

| 服务 | 镜像 | 端口 |
|------|------|------|
| DolphinDB | dolphindb/dolphindb:v3.00.5 | 8848 |
| Prefect | prefecthq/prefect:3-latest | 4200 |

### 8.2 应用服务

| 服务 | 端口 | 启动方式 |
|------|------|----------|
| 后端 API | 8000 | `python main.py` (uvicorn) |
| 前端开发服务 | 3000 | `npm start` |
| API 文档 | 8000/docs | 自动 (Swagger) |

### 8.3 脚本

| 脚本 | 用途 |
|------|------|
| `setup.sh` | 一键环境初始化 |
| `start.sh` | 启动所有服务 |
| `stop.sh` | 停止所有服务 |
| `check_status.sh` | 健康检查 |

---

## 九、技术栈

### 后端

| 技术 | 版本 | 用途 |
|------|------|------|
| Python | 3.11+ | 运行时 |
| FastAPI | >= 0.111.0 | Web 框架 |
| Polars | >= 1.0.0 | 数据处理（高性能） |
| DolphinDB | >= 3.0.0.0 | 时序数据库 |
| VectorBT | >= 0.26.0 | 向量化回测 |
| PyCaret | >= 3.3.0 | AutoML |
| Prefect | >= 3.0.0 | 工作流调度 |
| Tushare | >= 1.4.19 | 数据源 |

### 前端

| 技术 | 版本 | 用途 |
|------|------|------|
| React | 18 | UI 框架 |
| TypeScript | - | 类型安全 |
| Semi Design | - | UI 组件库 |
| React Flow | 11.11.4 | 流程编辑器 |
| ECharts | 5 | 图表可视化 |
| lightweight-charts | - | K 线图表 |
| Zustand | 4.5.4 | 状态管理 |
| Axios | 1.7.2 | HTTP 客户端 |
